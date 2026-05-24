use regex::Regex;
use serde::{Deserialize, Deserializer};
use std::collections::{HashMap, HashSet};
use std::fs;
use std::path::{Path, PathBuf};
use std::sync::Mutex;
use std::thread::JoinHandle;
use std::time::Duration;

use anyhow::{Context, Result, bail};

use crate::cell::{Kind, parse_typed_cell};
use crate::utils::parse_duration;

/// Post-deserialize semantic checks for config structs (cross-field
/// invariants, value ranges, etc.) that serde can't express on its own.
/// Implementors `bail!` on failure.
trait Validate {
    fn validate(&self) -> Result<()>;
}

// Custom deserializer for an optional Regex. Used together with
// `#[serde(default, deserialize_with = ...)]` so absent keys produce `None`
// and present-but-invalid patterns fail config loading.
fn deserialize_optional_regex<'de, D>(deserializer: D) -> Result<Option<Regex>, D::Error>
where
    D: Deserializer<'de>,
{
    let pattern = String::deserialize(deserializer)?;
    Regex::new(&pattern)
        .map(Some)
        .map_err(serde::de::Error::custom)
}

// Custom deserializer for Kind: reads the field as a string and parses it
// via `Kind::from_config`, surfacing unknown types as deserialization
// errors so invalid `type` values fail config loading.
fn deserialize_kind<'de, D>(deserializer: D) -> Result<Kind, D::Error>
where
    D: Deserializer<'de>,
{
    let type_str = String::deserialize(deserializer)?;
    Kind::from_config(&type_str).map_err(serde::de::Error::custom)
}

// Custom deserializer for an optional Duration: reads the field as an
// optional string and parses it via `parse_duration`, surfacing parse errors
// as deserialization errors so an invalid duration fails config loading.
fn deserialize_duration<'de, D>(deserializer: D) -> Result<Option<Duration>, D::Error>
where
    D: Deserializer<'de>,
{
    let value: Option<String> = Option::deserialize(deserializer)?;
    value
        .map(|s| parse_duration(&s).map_err(serde::de::Error::custom))
        .transpose()
}

// Config file formats we accept.
enum ConfigFormat {
    Toml,
    Json,
}

/// Controls block cleanup / truncation of the block chain.
#[derive(Clone, Debug, Deserialize)]
#[serde(default, deny_unknown_fields)]
pub struct TruncateConfig {
    /// Keep at most this many blocks; older ones are removed. `None` disables the limit.
    #[serde(rename = "max-blocks")]
    pub max_blocks: Option<u32>,
    /// Drop blocks whose `created` timestamp is older than this duration (e.g. `"30d"`). `None` disables the limit.
    #[serde(rename = "max-age", deserialize_with = "deserialize_duration")]
    pub max_age: Option<Duration>,
    /// When true, also delete blocks no longer referenced by any retained block.
    #[serde(rename = "remove-orphans")]
    pub remove_orphans: bool,
    /// When true, blocks already reported to the consumer are eligible for removal.
    #[serde(rename = "truncate-reported")]
    pub truncate_reported: bool,
}

impl Default for TruncateConfig {
    fn default() -> Self {
        Self {
            max_blocks: None,
            max_age: None,
            remove_orphans: true,
            truncate_reported: true,
        }
    }
}

impl Validate for TruncateConfig {
    fn validate(&self) -> Result<()> {
        if let Some(max_blocks) = self.max_blocks
            && max_blocks < 1
        {
            bail!("truncate.max-blocks must be >= 1");
        }
        Ok(())
    }
}

/// Controls zstd compression of patch payloads.
#[derive(Debug, Deserialize)]
#[serde(default, deny_unknown_fields)]
pub struct CompressionConfig {
    /// When true, patch payloads are zstd-compressed before being written.
    pub enable: bool,
    /// Zstd compression level passed to `zstd::encode_all`. `0` selects the zstd default.
    pub level: i32,
}

impl Default for CompressionConfig {
    fn default() -> Self {
        Self {
            enable: true,
            level: 0,
        }
    }
}

impl Validate for CompressionConfig {
    fn validate(&self) -> Result<()> {
        let range = zstd::compression_level_range();
        if self.level != 0 && !range.contains(&self.level) {
            bail!(
                "compression.level {} is outside the supported zstd range {}..={}",
                self.level,
                range.start(),
                range.end()
            );
        }
        Ok(())
    }
}

/// A static field added to every generated SQL row (e.g. a `host` column
/// identifying which agent produced the data).
#[derive(Debug, Deserialize)]
#[serde(default, deny_unknown_fields)]
pub struct InjectedFieldConfig {
    /// Column name in the target database.
    pub name: String,
    /// Cell kind; one of `TEXT`, `NUMBER`, or `BOOLEAN`.
    #[serde(rename = "type", deserialize_with = "deserialize_kind")]
    pub kind: Kind,
    /// The static value written into the column for every row.
    pub value: String,
}

impl Default for InjectedFieldConfig {
    fn default() -> Self {
        Self {
            name: String::new(),
            kind: Kind::Text,
            value: String::new(),
        }
    }
}

impl Validate for InjectedFieldConfig {
    fn validate(&self) -> Result<()> {
        if self.name.is_empty() {
            bail!("name must not be empty");
        }
        if self.value.is_empty() {
            bail!("'{}': value must not be empty", self.name);
        }
        parse_typed_cell(&self.value, self.kind).with_context(|| format!("'{}'", self.name))?;
        Ok(())
    }
}

/// Per-table CSV-load filter. One optional block per table that decides
/// whether each loaded record is kept.
#[derive(Debug, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct FilterConfig {
    /// Fields whose values the include/exclude regexes are matched against.
    /// Must be non-empty; every name must appear in the parent table's `fields`.
    pub fields: Vec<String>,
    /// Whitelist regex. The record is kept only if at least one of the
    /// listed fields' values matches the pattern. Unanchored by default.
    #[serde(default, deserialize_with = "deserialize_optional_regex")]
    pub include: Option<Regex>,
    /// Blacklist regex. The record is dropped if any of the listed fields'
    /// values matches the pattern. Unanchored by default. Exclude wins on
    /// overlap with include.
    #[serde(default, deserialize_with = "deserialize_optional_regex")]
    pub exclude: Option<Regex>,
}

impl FilterConfig {
    fn validate(&self, table_field_names: &HashSet<&str>) -> Result<()> {
        if self.fields.is_empty() {
            bail!("filter.fields must not be empty");
        }
        for field in &self.fields {
            if !table_field_names.contains(field.as_str()) {
                bail!("filter.fields references unknown field '{}'", field);
            }
        }
        if self.include.is_none() && self.exclude.is_none() {
            bail!("filter must set 'include', 'exclude', or both");
        }
        Ok(())
    }
}

/// CSV-specific configuration for a table. The presence of this block on a
/// `TableConfig` marks the table as CSV-backed; its absence means the table
/// is callback-backed and rows come from the FFI cell callback.
#[derive(Debug, Default, Deserialize)]
#[serde(default, deny_unknown_fields)]
pub struct CsvConfig {
    /// CSV file path. Absolute paths are used as-is; relative paths are
    /// resolved against the work directory.
    pub source: String,
    /// When true, the first CSV row is a header used to match columns by name;
    /// when false, columns are matched by position.
    pub header: bool,
    /// Regex that, when matched against a cell's text, maps the cell to SQL
    /// `NULL`. Applies to every non-primary-key field in the table.
    /// Unanchored by default.
    #[serde(rename = "null", deserialize_with = "deserialize_optional_regex")]
    pub null_pattern: Option<Regex>,
    /// Regex matched against BOOLEAN cells to produce `true`. When unset, the
    /// strict default literal `"true"` is required. Unanchored by default.
    #[serde(rename = "true", deserialize_with = "deserialize_optional_regex")]
    pub true_pattern: Option<Regex>,
    /// Regex matched against BOOLEAN cells to produce `false`. When unset, the
    /// strict default literal `"false"` is required. Unanchored by default.
    #[serde(rename = "false", deserialize_with = "deserialize_optional_regex")]
    pub false_pattern: Option<Regex>,
    /// Drop records where any field value exceeds this character length.
    /// `None` disables the limit.
    #[serde(rename = "max-field-length")]
    pub max_field_length: Option<usize>,
    /// Optional include/exclude filter applied at CSV load time.
    pub filter: Option<FilterConfig>,
}

impl CsvConfig {
    fn validate(&self, table_field_names: &HashSet<&str>) -> Result<()> {
        if self.source.is_empty() {
            bail!("csv.source must not be empty");
        }
        if self.max_field_length == Some(0) {
            bail!("csv.max-field-length must be >= 1");
        }
        if let Some(filter) = &self.filter {
            filter.validate(table_field_names).context("csv.filter")?;
        }
        Ok(())
    }

    /// Returns `Some(reason)` if the record should be filtered out, `None` to keep.
    pub fn should_filter(&self, field_names: &[String], values: &[&str]) -> Option<String> {
        if let Some(max_length) = self.max_field_length {
            for (name, value) in field_names.iter().zip(values.iter()) {
                if value.len() > max_length {
                    return Some(format!(
                        "field '{}' length {} exceeds max-field-length {}",
                        name,
                        value.len(),
                        max_length
                    ));
                }
            }
        }

        if let Some(filter) = &self.filter {
            let candidate_values: Vec<&str> = filter
                .fields
                .iter()
                .filter_map(|name| find_field_value(field_names, values, name))
                .collect();

            if let Some(include) = &filter.include
                && !candidate_values.iter().any(|v| include.is_match(v))
            {
                return Some("no include rule matched".to_string());
            }

            if let Some(exclude) = &filter.exclude
                && let Some(value) = candidate_values.iter().find(|v| exclude.is_match(v))
            {
                return Some(format!("value '{}' matches exclude rule", value));
            }
        }

        None
    }
}

/// Look up the value whose field name is `target`. Returns `None` if
/// `target` isn't in `field_names` or if `values` is shorter than
/// `field_names`.
fn find_field_value<'a>(
    field_names: &[String],
    values: &'a [&str],
    target: &str,
) -> Option<&'a str> {
    let position = field_names.iter().position(|name| name == target)?;
    values.get(position).copied()
}

/// Top-level configuration loaded from `config.toml` or `config.json` in the
/// work directory.
#[derive(Debug, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct Config {
    /// Directory the config was loaded from; populated by `Config::load`, not
    /// deserialized.
    #[serde(skip)]
    pub work_dir: PathBuf,
    /// Static fields added to every generated SQL row.
    #[serde(default, rename = "injected-fields")]
    pub injected_fields: Vec<InjectedFieldConfig>,
    /// Zstd compression settings for patch payloads.
    #[serde(default)]
    pub compression: CompressionConfig,
    /// Per-table source-file and field schemas, keyed by table name.
    pub tables: HashMap<String, TableConfig>,
    /// Block chain truncation policy.
    #[serde(default)]
    pub truncate: TruncateConfig,
    /// Handle of the background truncation thread most recently spawned for
    /// this config (if any). `truncate::spawn_background` only spawns a new
    /// thread when this slot is empty or holds a finished handle, so at most
    /// one pass is in flight at a time for a given `Config`. `Drop` joins
    /// any unfinished handle so `lch_deinit` (and end-of-scope in tests and
    /// the CLI) cleanly waits for truncation before tearing down.
    #[serde(skip)]
    pub(crate) background_truncation: Mutex<Option<JoinHandle<()>>>,
}

impl Drop for Config {
    fn drop(&mut self) {
        let slot = self
            .background_truncation
            .get_mut()
            .unwrap_or_else(|e| e.into_inner());
        let handle = slot.take();
        if let Some(handle) = handle {
            let _ = handle.join();
        }
    }
}

/// One column in a table record.
#[derive(Debug, Deserialize)]
#[serde(default, deny_unknown_fields)]
pub struct FieldConfig {
    /// Column name. Matches a CSV header when `csv.header = true`; otherwise
    /// only used as the SQL column name.
    pub name: String,
    /// Cell kind; one of `TEXT`, `NUMBER`, or `BOOLEAN`.
    #[serde(rename = "type", deserialize_with = "deserialize_kind")]
    pub kind: Kind,
    /// When true, this field is part of the table's composite primary key.
    #[serde(rename = "primary-key")]
    pub primary_key: bool,
}

impl Default for FieldConfig {
    fn default() -> Self {
        Self {
            name: String::new(),
            kind: Kind::Text,
            primary_key: false,
        }
    }
}

/// Configure where the table data comes from and how its columns map to SQL.
#[derive(Debug, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct TableConfig {
    /// Column definitions.
    pub fields: Vec<FieldConfig>,
    /// CSV-specific configuration. When present, the table is CSV-backed and
    /// rows are loaded from `csv.source` at block creation time. When absent,
    /// the table is callback-backed and rows are pulled from the FFI cell
    /// callback.
    pub csv: Option<CsvConfig>,
}

impl Validate for FieldConfig {
    fn validate(&self) -> Result<()> {
        if self.name.is_empty() {
            bail!("field name must not be empty");
        }
        Ok(())
    }
}

impl Validate for TableConfig {
    fn validate(&self) -> Result<()> {
        let num_primary_keys = self.fields.iter().filter(|field| field.primary_key).count();
        if num_primary_keys == 0 {
            bail!("at least one field must be marked as primary-key");
        }

        let mut seen = HashSet::new();
        for field in &self.fields {
            field.validate()?;
            if !seen.insert(field.name.as_str()) {
                bail!("found duplicate field name '{}'", field.name);
            }
        }

        if let Some(csv) = &self.csv {
            csv.validate(&seen)?;
        }

        Ok(())
    }
}

impl TableConfig {
    pub fn field_names(&self) -> Vec<String> {
        self.fields.iter().map(|field| field.name.clone()).collect()
    }

    pub fn primary_key(&self) -> Vec<String> {
        self.fields
            .iter()
            .filter(|field| field.primary_key)
            .map(|field| field.name.clone())
            .collect()
    }
}

impl Validate for Config {
    fn validate(&self) -> Result<()> {
        if self.tables.is_empty() {
            bail!("at least one table must be declared under [tables]");
        }
        for (name, table) in &self.tables {
            table
                .validate()
                .with_context(|| format!("table '{}'", name))?;
        }

        let mut injected_names = HashSet::new();
        for (index, field) in self.injected_fields.iter().enumerate() {
            field
                .validate()
                .with_context(|| format!("injected-fields[{}]", index))?;
            if !injected_names.insert(&field.name) {
                bail!(
                    "injected-fields[{}]: duplicate field name '{}'",
                    index,
                    field.name
                );
            }
            for (table_name, table) in &self.tables {
                if table.fields.iter().any(|f| f.name == field.name) {
                    bail!(
                        "injected-fields[{}] '{}' collides with a column in table '{}'",
                        index,
                        field.name,
                        table_name
                    );
                }
            }
        }

        self.truncate.validate()?;
        self.compression.validate()?;

        Ok(())
    }
}

impl Config {
    pub fn load(work_dir: &Path) -> Result<Config> {
        let toml_path = work_dir.join("config.toml");
        let json_path = work_dir.join("config.json");

        let (path, format) = match (toml_path.exists(), json_path.exists()) {
            (true, true) => {
                bail!("found both config.toml and config.json (don't know which one to pick)")
            }
            (true, false) => (toml_path, ConfigFormat::Toml),
            (false, true) => (json_path, ConfigFormat::Json),
            (false, false) => bail!(
                "no config file found in '{}' (expected config.toml or config.json)",
                work_dir.display()
            ),
        };

        log::debug!("Parsing config from file '{}'...", path.display());
        let content = fs::read_to_string(&path)
            .with_context(|| format!("failed to read config file '{}'", path.display()))?;
        let mut config: Config = match format {
            ConfigFormat::Toml => toml::from_str(&content).with_context(|| {
                format!("failed to parse config TOML file '{}'", path.display())
            })?,
            ConfigFormat::Json => serde_json::from_str(&content).with_context(|| {
                format!("failed to parse config JSON file '{}'", path.display())
            })?,
        };
        config.work_dir = work_dir.to_path_buf();

        config.validate()?;

        log::debug!("Initialized config with {} tables", config.tables.len());
        Ok(config)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn make_csv(filter: Option<FilterConfig>) -> CsvConfig {
        CsvConfig {
            source: "x.csv".to_string(),
            filter,
            ..Default::default()
        }
    }

    fn make_filter(
        fields: Vec<&str>,
        include: Option<&str>,
        exclude: Option<&str>,
    ) -> FilterConfig {
        FilterConfig {
            fields: fields.into_iter().map(|s| s.to_string()).collect(),
            include: include.map(|s| Regex::new(s).unwrap()),
            exclude: exclude.map(|s| Regex::new(s).unwrap()),
        }
    }

    #[test]
    fn test_should_filter_max_field_length() {
        let csv = CsvConfig {
            source: "x.csv".to_string(),
            max_field_length: Some(5),
            ..Default::default()
        };
        let fields = vec!["id".to_string(), "name".to_string()];

        assert!(csv.should_filter(&fields, &["1", "Alice"]).is_none());
        assert!(csv.should_filter(&fields, &["1", "Roberto"]).is_some());
    }

    #[test]
    fn test_should_filter_exclude_anchored_regex() {
        let csv = make_csv(Some(make_filter(vec!["status"], None, Some("^inactive$"))));
        let fields = vec!["id".to_string(), "status".to_string()];

        assert!(csv.should_filter(&fields, &["1", "active"]).is_none());
        assert!(csv.should_filter(&fields, &["2", "inactive"]).is_some());
        // Anchored pattern does not match substrings.
        assert!(
            csv.should_filter(&fields, &["3", "inactive-user"])
                .is_none()
        );
    }

    #[test]
    fn test_should_filter_exclude_unanchored_regex() {
        let csv = make_csv(Some(make_filter(vec!["desc"], None, Some("DEPRECATED"))));
        let fields = vec!["id".to_string(), "desc".to_string()];

        assert!(csv.should_filter(&fields, &["1", "active item"]).is_none());
        assert!(
            csv.should_filter(&fields, &["2", "DEPRECATED old item"])
                .is_some()
        );
    }

    #[test]
    fn test_should_filter_default_csv() {
        let csv = make_csv(None);
        let fields = vec!["id".to_string(), "name".to_string()];

        assert!(csv.should_filter(&fields, &["1", "Alice"]).is_none());
    }

    #[test]
    fn test_should_filter_include_match_keeps_record() {
        let csv = make_csv(Some(make_filter(
            vec!["status"],
            Some("^(active|pending)$"),
            None,
        )));
        let fields = vec!["id".to_string(), "status".to_string()];

        assert!(csv.should_filter(&fields, &["1", "active"]).is_none());
        assert!(csv.should_filter(&fields, &["2", "pending"]).is_none());
    }

    #[test]
    fn test_should_filter_include_no_match_drops_record() {
        let csv = make_csv(Some(make_filter(
            vec!["status"],
            Some("^(active|pending)$"),
            None,
        )));
        let fields = vec!["id".to_string(), "status".to_string()];

        assert!(csv.should_filter(&fields, &["1", "inactive"]).is_some());
    }

    /// Multiple listed fields combine with OR for include: a record passes if
    /// at least one listed field matches the pattern.
    #[test]
    fn test_should_filter_include_or_across_fields() {
        let csv = make_csv(Some(make_filter(
            vec!["status", "label"],
            Some("^active$"),
            None,
        )));
        let fields = vec!["id".to_string(), "status".to_string(), "label".to_string()];

        // status matches.
        assert!(csv.should_filter(&fields, &["1", "active", "x"]).is_none());
        // label matches.
        assert!(csv.should_filter(&fields, &["2", "x", "active"]).is_none());
        // Neither matches.
        assert!(csv.should_filter(&fields, &["3", "x", "y"]).is_some());
    }

    /// Multiple listed fields combine with OR for exclude: a record drops if
    /// any listed field matches the pattern.
    #[test]
    fn test_should_filter_exclude_or_across_fields() {
        let csv = make_csv(Some(make_filter(
            vec!["status", "label"],
            None,
            Some("^drop$"),
        )));
        let fields = vec!["id".to_string(), "status".to_string(), "label".to_string()];

        // status matches exclude.
        assert!(csv.should_filter(&fields, &["1", "drop", "x"]).is_some());
        // label matches exclude.
        assert!(csv.should_filter(&fields, &["2", "x", "drop"]).is_some());
        // Neither matches.
        assert!(csv.should_filter(&fields, &["3", "x", "y"]).is_none());
    }

    /// When a record matches both an include rule and an exclude rule, the
    /// exclude rule wins. Reason strings are checked exactly so a regression
    /// where include short-circuits exclude is caught.
    #[test]
    fn test_should_filter_exclude_wins_over_include() {
        let csv = make_csv(Some(make_filter(
            vec!["status"],
            Some("^(active|pending)$"),
            Some("^pending$"),
        )));
        let fields = vec!["id".to_string(), "status".to_string()];

        // active: matches include, not exclude -> kept.
        assert!(csv.should_filter(&fields, &["1", "active"]).is_none());
        // pending: matches both -> dropped with the exclude reason.
        assert_eq!(
            csv.should_filter(&fields, &["2", "pending"]).as_deref(),
            Some("value 'pending' matches exclude rule"),
        );
        // archived: doesn't match include -> dropped with the include reason.
        assert_eq!(
            csv.should_filter(&fields, &["3", "archived"]).as_deref(),
            Some("no include rule matched"),
        );
    }

    #[test]
    fn test_invalid_filter_regex_fails_to_load() {
        let toml_input = r#"
[tables.users]
fields = [
    { name = "id", type = "NUMBER", primary-key = true },
    { name = "status", type = "TEXT" },
]

[tables.users.csv]
source = "users.csv"

[tables.users.csv.filter]
fields = ["status"]
exclude = "["
"#;
        let result: Result<Config, _> = toml::from_str(toml_input);
        let err = result.expect_err("invalid regex should fail to parse");
        assert!(
            err.to_string().contains("regex parse error") || err.to_string().contains("regex"),
            "expected error to mention 'regex', got: {err}"
        );
    }

    #[test]
    fn test_filter_references_unknown_field() {
        let dir = tempfile::tempdir().unwrap();
        let toml_input = r#"
[tables.users]
fields = [
    { name = "id", type = "NUMBER", primary-key = true },
    { name = "status", type = "TEXT" },
]

[tables.users.csv]
source = "users.csv"

[tables.users.csv.filter]
fields = ["nonexistent"]
include = "^x$"
"#;
        fs::write(dir.path().join("config.toml"), toml_input).unwrap();
        let err = Config::load(dir.path()).expect_err("expected unknown-field error");
        assert!(
            err.to_string().contains("unknown field 'nonexistent'")
                || format!("{:#}", err).contains("nonexistent"),
            "expected unknown-field error, got: {err:#}"
        );
    }

    #[test]
    fn test_filter_without_include_or_exclude_rejected() {
        let dir = tempfile::tempdir().unwrap();
        let toml_input = r#"
[tables.users]
fields = [
    { name = "id", type = "NUMBER", primary-key = true },
    { name = "status", type = "TEXT" },
]

[tables.users.csv]
source = "users.csv"

[tables.users.csv.filter]
fields = ["status"]
"#;
        fs::write(dir.path().join("config.toml"), toml_input).unwrap();
        let err = Config::load(dir.path()).expect_err("expected missing-pattern error");
        assert!(
            format!("{:#}", err).contains("include"),
            "expected error to mention 'include'/'exclude', got: {err:#}"
        );
    }

    #[test]
    fn test_filter_empty_fields_rejected() {
        let dir = tempfile::tempdir().unwrap();
        let toml_input = r#"
[tables.users]
fields = [
    { name = "id", type = "NUMBER", primary-key = true },
]

[tables.users.csv]
source = "users.csv"

[tables.users.csv.filter]
fields = []
include = "^x$"
"#;
        fs::write(dir.path().join("config.toml"), toml_input).unwrap();
        let err = Config::load(dir.path()).expect_err("expected empty-fields error");
        assert!(
            format!("{:#}", err).contains("filter.fields"),
            "expected error to mention 'filter.fields', got: {err:#}"
        );
    }

    #[test]
    fn test_empty_csv_source_rejected() {
        let dir = tempfile::tempdir().unwrap();
        let toml_input = r#"
[tables.users]
fields = [
    { name = "id", type = "NUMBER", primary-key = true },
]

[tables.users.csv]
source = ""
"#;
        fs::write(dir.path().join("config.toml"), toml_input).unwrap();
        let err = Config::load(dir.path()).expect_err("expected empty-source error");
        assert!(
            format!("{:#}", err).contains("csv.source must not be empty"),
            "expected error about empty csv.source, got: {err:#}"
        );
    }

    #[test]
    fn test_invalid_sentinel_regex_fails_to_load() {
        let toml_input = r#"
[tables.users]
fields = [
    { name = "id", type = "NUMBER", primary-key = true },
]

[tables.users.csv]
source = "users.csv"
null = "["
"#;
        let result: Result<Config, _> = toml::from_str(toml_input);
        let err = result.expect_err("invalid sentinel regex should fail to parse");
        assert!(
            err.to_string().contains("regex"),
            "expected error to mention 'regex', got: {err}"
        );
    }

    #[test]
    fn test_callback_backed_table_no_csv_block() {
        let dir = tempfile::tempdir().unwrap();
        let toml_input = r#"
[tables.users]
fields = [
    { name = "id", type = "NUMBER", primary-key = true },
    { name = "name", type = "TEXT" },
]
"#;
        fs::write(dir.path().join("config.toml"), toml_input).unwrap();
        let config = Config::load(dir.path()).unwrap();
        let table = config.tables.get("users").unwrap();
        assert!(table.csv.is_none());
    }

    #[test]
    fn test_load_fails_when_both_toml_and_json_present() {
        let dir = tempfile::tempdir().unwrap();
        let minimal_toml = r#"
[tables.users]
fields = [
    { name = "id", type = "NUMBER", primary-key = true },
]

[tables.users.csv]
source = "users.csv"
"#;
        let minimal_json = r#"{
  "tables": {
    "users": {
      "fields": [{ "name": "id", "type": "NUMBER", "primary-key": true }],
      "csv": { "source": "users.csv" }
    }
  }
}"#;
        fs::write(dir.path().join("config.toml"), minimal_toml).unwrap();
        fs::write(dir.path().join("config.json"), minimal_json).unwrap();

        let err = Config::load(dir.path()).expect_err("expected ambiguity error");
        assert!(
            err.to_string().contains("both config.toml and config.json"),
            "unexpected error: {err}"
        );
    }

    #[test]
    fn test_load_rejects_typo_in_field_key() {
        // `primery-key` is a misspelling of `primary-key`. Without
        // deny_unknown_fields it would deserialize silently, leaving
        // primary_key = false on every field.
        let dir = tempfile::tempdir().unwrap();
        let toml_input = r#"
[tables.users]
fields = [
    { name = "id", type = "NUMBER", primery-key = true },
]
"#;
        fs::write(dir.path().join("config.toml"), toml_input).unwrap();
        let err = Config::load(dir.path()).expect_err("expected unknown-key error");
        let msg = format!("{:#}", err);
        assert!(
            msg.contains("primery-key"),
            "expected error to mention the typo, got: {msg}"
        );
    }

    #[test]
    fn test_load_rejects_underscore_variant_of_kebab_key() {
        // `max_blocks` is the snake_case form; the real key is `max-blocks`.
        // Without deny_unknown_fields the wrong spelling would be silently
        // dropped and the documented limit ignored.
        let dir = tempfile::tempdir().unwrap();
        let toml_input = r#"
[tables.users]
fields = [
    { name = "id", type = "NUMBER", primary-key = true },
]

[truncate]
max_blocks = 5
"#;
        fs::write(dir.path().join("config.toml"), toml_input).unwrap();
        let err = Config::load(dir.path()).expect_err("expected unknown-key error");
        let msg = format!("{:#}", err);
        assert!(
            msg.contains("max_blocks"),
            "expected error to mention the wrong key, got: {msg}"
        );
    }

    #[test]
    fn test_load_rejects_stale_top_level_key() {
        let dir = tempfile::tempdir().unwrap();
        let toml_input = r#"
some-removed-feature = true

[tables.users]
fields = [
    { name = "id", type = "NUMBER", primary-key = true },
]
"#;
        fs::write(dir.path().join("config.toml"), toml_input).unwrap();
        let err = Config::load(dir.path()).expect_err("expected unknown-key error");
        let msg = format!("{:#}", err);
        assert!(
            msg.contains("some-removed-feature"),
            "expected error to mention the unknown top-level key, got: {msg}"
        );
    }
}
