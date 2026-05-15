use regex::Regex;
use serde::{Deserialize, Deserializer};
use std::collections::{HashMap, HashSet};
use std::fs;
use std::path::{Path, PathBuf};
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

// Custom deserializer used via `#[serde(deserialize_with = ...)]`: reads the
// field as a string and compiles it into a `Regex`, surfacing compile errors
// as deserialization errors so an invalid pattern fails config loading.
fn deserialize_regex<'de, D>(deserializer: D) -> Result<Regex, D::Error>
where
    D: Deserializer<'de>,
{
    let pattern = String::deserialize(deserializer)?;
    Regex::new(&pattern).map_err(serde::de::Error::custom)
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
#[derive(Debug, Deserialize)]
#[serde(default)]
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
#[serde(default)]
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
#[serde(default)]
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

/// Rules to include/exclude records in tables.
#[derive(Debug, Deserialize)]
pub struct FilterRule {
    /// Tables this rule applies to. Empty means all tables.
    #[serde(default)]
    pub tables: Vec<String>,
    /// Name of the field whose value the regex is matched against.
    pub field: String,
    /// Pattern matched against the field value. Unanchored by default.
    #[serde(deserialize_with = "deserialize_regex")]
    pub regex: Regex,
}

impl FilterRule {
    /// Returns true if this rule applies to the given table.
    /// An empty `table` list means the rule applies to all tables.
    fn applies_to(&self, table_name: &str) -> bool {
        self.tables.is_empty() || self.tables.iter().any(|name| name == table_name)
    }
}

/// Drops records at CSV load time so they never enter state, deltas, or SQL
/// output.
#[derive(Debug, Default, Deserialize)]
pub struct FilterConfig {
    /// Drop records where any field value exceeds this character length.
    /// `None` disables the limit.
    #[serde(rename = "max-field-length")]
    pub max_field_length: Option<usize>,
    /// Whitelist rules. When any include rule applies to a table, a record is
    /// kept only if at least one rule matches.
    #[serde(default)]
    pub include: Vec<FilterRule>,
    /// Blacklist rules. Records matching any applicable exclude rule are
    /// dropped. Exclude wins on overlap.
    #[serde(default)]
    pub exclude: Vec<FilterRule>,
}

impl Validate for FilterConfig {
    fn validate(&self) -> Result<()> {
        if self.max_field_length == Some(0) {
            bail!("filters.max-field-length must be >= 1");
        }
        Ok(())
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

impl FilterConfig {
    /// Returns `Some(reason)` if the record should be filtered out, `None` to keep.
    pub fn should_filter(
        &self,
        table_name: &str,
        field_names: &[String],
        values: &[&str],
    ) -> Option<String> {
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

        let mut has_applicable_include = false;
        let mut any_include_matched = false;
        for include in &self.include {
            if !include.applies_to(table_name) {
                // Rule does not apply for this table
                continue;
            }
            let Some(value) = find_field_value(field_names, values, &include.field) else {
                // Field does not exist in table
                continue;
            };
            has_applicable_include = true;
            if include.regex.is_match(value) {
                any_include_matched = true;
                break;
            }
        }

        if has_applicable_include && !any_include_matched {
            // An include rule applied to this table, but none matched
            return Some("no include rule matched".to_string());
        }

        for exclude in &self.exclude {
            if !exclude.applies_to(table_name) {
                // Rule does not apply for this table
                continue;
            }
            let Some(value) = find_field_value(field_names, values, &exclude.field) else {
                // Field does not exist in table
                continue;
            };
            if exclude.regex.is_match(value) {
                return Some(format!("field '{}' matches exclude rule", exclude.field));
            }
        }
        None
    }
}

/// Top-level configuration loaded from `config.toml` or `config.json` in the
/// work directory.
#[derive(Debug, Deserialize)]
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
    /// Include/exclude rules applied at CSV load time.
    #[serde(default)]
    pub filters: FilterConfig,
}

/// One column in a table record.
#[derive(Debug, Deserialize)]
#[serde(default)]
pub struct FieldConfig {
    /// Column name. Matches a CSV header when `header = true`; otherwise
    /// only used as the SQL column name.
    pub name: String,
    /// Cell kind; one of `TEXT`, `NUMBER`, or `BOOLEAN`.
    #[serde(rename = "type", deserialize_with = "deserialize_kind")]
    pub kind: Kind,
    /// When true, this field is part of the table's composite primary key.
    #[serde(rename = "primary-key")]
    pub primary_key: bool,
    /// CSV string treated as SQL `NULL`. Not allowed on primary-key fields.
    #[serde(rename = "null")]
    pub null_sentinel: Option<String>,
    /// CSV string treated as boolean true (BOOLEAN fields only). Disables the
    /// default `"true"`.
    #[serde(rename = "true")]
    pub true_sentinel: Option<String>,
    /// CSV string treated as boolean false (BOOLEAN fields only). Disables the
    /// default `"false"`.
    #[serde(rename = "false")]
    pub false_sentinel: Option<String>,
}

impl Default for FieldConfig {
    fn default() -> Self {
        Self {
            name: String::new(),
            kind: Kind::Text,
            primary_key: false,
            null_sentinel: None,
            true_sentinel: None,
            false_sentinel: None,
        }
    }
}

/// Configure where the table CSV lives and how its columns map to SQL.
#[derive(Debug, Deserialize)]
pub struct TableConfig {
    /// CSV file path. Absolute paths are used as-is; relative paths are
    /// resolved against the work directory.
    pub source: String,
    /// When true, the first CSV row is a header used to match columns by name;
    /// when false, columns are matched by position.
    #[serde(default)]
    pub header: bool,
    /// Column definitions.
    pub fields: Vec<FieldConfig>,
}

impl Validate for FieldConfig {
    fn validate(&self) -> Result<()> {
        if self.name.is_empty() {
            bail!("field name must not be empty");
        }

        if self.primary_key && self.null_sentinel.is_some() {
            bail!(
                "primary-key field '{}' must not have a null sentinel",
                self.name
            );
        }

        if (self.true_sentinel.is_some() || self.false_sentinel.is_some())
            && self.kind != Kind::Boolean
        {
            bail!(
                "field '{}': 'true' and 'false' sentinels are only valid on BOOLEAN fields",
                self.name
            );
        }

        if let (Some(t), Some(f)) = (&self.true_sentinel, &self.false_sentinel)
            && t == f
        {
            bail!(
                "field '{}': 'true' and 'false' sentinels must differ",
                self.name
            );
        }

        if let (Some(t), Some(n)) = (&self.true_sentinel, &self.null_sentinel)
            && t == n
        {
            bail!(
                "field '{}': 'true' and 'null' sentinels must differ",
                self.name
            );
        }

        if let (Some(f), Some(n)) = (&self.false_sentinel, &self.null_sentinel)
            && f == n
        {
            bail!(
                "field '{}': 'false' and 'null' sentinels must differ",
                self.name
            );
        }

        Ok(())
    }
}

impl Validate for TableConfig {
    fn validate(&self) -> Result<()> {
        if self.source.is_empty() {
            bail!("source must not be empty");
        }
        let num_primary_keys = self.fields.iter().filter(|field| field.primary_key).count();
        if num_primary_keys == 0 {
            bail!("at least one field must be marked as primary-key");
        }

        let mut seen = HashSet::new();
        for field in &self.fields {
            field.validate()?;
            if !seen.insert(&field.name) {
                bail!("found duplicate field name '{}'", field.name);
            }
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
        self.filters.validate()?;

        for (label, rules) in [
            ("filters.include", &self.filters.include),
            ("filters.exclude", &self.filters.exclude),
        ] {
            for (index, rule) in rules.iter().enumerate() {
                for table_name in &rule.tables {
                    if !self.tables.contains_key(table_name) {
                        bail!(
                            "{}[{}]: references unknown table '{}'",
                            label,
                            index,
                            table_name
                        );
                    }
                }
            }
        }

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

    #[test]
    fn test_validate_rejects_true_sentinel_on_non_boolean() {
        let field = FieldConfig {
            name: "name".to_string(),
            kind: Kind::Text,
            true_sentinel: Some("Y".to_string()),
            ..Default::default()
        };
        let msg = format!("{:#}", field.validate().unwrap_err());
        assert!(msg.contains("only valid on BOOLEAN"), "got: {msg}");
    }

    #[test]
    fn test_validate_rejects_false_sentinel_on_non_boolean() {
        let field = FieldConfig {
            name: "count".to_string(),
            kind: Kind::Number,
            false_sentinel: Some("none".to_string()),
            ..Default::default()
        };
        let msg = format!("{:#}", field.validate().unwrap_err());
        assert!(msg.contains("only valid on BOOLEAN"), "got: {msg}");
    }

    #[test]
    fn test_validate_rejects_equal_true_and_false_sentinels() {
        let field = FieldConfig {
            name: "flag".to_string(),
            kind: Kind::Boolean,
            true_sentinel: Some("X".to_string()),
            false_sentinel: Some("X".to_string()),
            ..Default::default()
        };
        let msg = format!("{:#}", field.validate().unwrap_err());
        assert!(msg.contains("'true' and 'false' sentinels"), "got: {msg}");
    }

    #[test]
    fn test_validate_rejects_true_sentinel_collision_with_null() {
        let field = FieldConfig {
            name: "flag".to_string(),
            kind: Kind::Boolean,
            null_sentinel: Some("X".to_string()),
            true_sentinel: Some("X".to_string()),
            ..Default::default()
        };
        let msg = format!("{:#}", field.validate().unwrap_err());
        assert!(msg.contains("'true' and 'null' sentinels"), "got: {msg}");
    }

    #[test]
    fn test_validate_rejects_false_sentinel_collision_with_null() {
        let field = FieldConfig {
            name: "flag".to_string(),
            kind: Kind::Boolean,
            null_sentinel: Some("X".to_string()),
            false_sentinel: Some("X".to_string()),
            ..Default::default()
        };
        let msg = format!("{:#}", field.validate().unwrap_err());
        assert!(msg.contains("'false' and 'null' sentinels"), "got: {msg}");
    }

    #[test]
    fn test_validate_accepts_distinct_sentinels_on_boolean() {
        let field = FieldConfig {
            name: "flag".to_string(),
            kind: Kind::Boolean,
            null_sentinel: Some("?".to_string()),
            true_sentinel: Some("Y".to_string()),
            false_sentinel: Some("N".to_string()),
            ..Default::default()
        };
        field.validate().unwrap();
    }

    fn make_rule(tables: Vec<&str>, field: &str, regex: &str) -> FilterRule {
        FilterRule {
            tables: tables.into_iter().map(|s| s.to_string()).collect(),
            field: field.to_string(),
            regex: Regex::new(regex).unwrap(),
        }
    }

    #[test]
    fn test_should_filter_max_field_length() {
        let filter = FilterConfig {
            max_field_length: Some(5),
            ..Default::default()
        };
        let fields = vec!["id".to_string(), "name".to_string()];

        assert!(
            filter
                .should_filter("t", &fields, &["1", "Alice"])
                .is_none()
        );
        assert!(
            filter
                .should_filter("t", &fields, &["1", "Roberto"])
                .is_some()
        );
    }

    #[test]
    fn test_should_filter_exclude_anchored_regex() {
        let filter = FilterConfig {
            exclude: vec![make_rule(vec![], "status", "^inactive$")],
            ..Default::default()
        };
        let fields = vec!["id".to_string(), "status".to_string()];

        assert!(
            filter
                .should_filter("t", &fields, &["1", "active"])
                .is_none()
        );
        assert!(
            filter
                .should_filter("t", &fields, &["2", "inactive"])
                .is_some()
        );
        // Anchored pattern does not match substrings
        assert!(
            filter
                .should_filter("t", &fields, &["3", "inactive-user"])
                .is_none()
        );
    }

    #[test]
    fn test_should_filter_exclude_unanchored_regex() {
        let filter = FilterConfig {
            exclude: vec![make_rule(vec![], "desc", "DEPRECATED")],
            ..Default::default()
        };
        let fields = vec!["id".to_string(), "desc".to_string()];

        assert!(
            filter
                .should_filter("t", &fields, &["1", "active item"])
                .is_none()
        );
        assert!(
            filter
                .should_filter("t", &fields, &["2", "DEPRECATED old item"])
                .is_some()
        );
    }

    #[test]
    fn test_should_filter_exclude_alternation_regex() {
        let filter = FilterConfig {
            exclude: vec![make_rule(vec![], "status", "^(inactive|archived)$")],
            ..Default::default()
        };
        let fields = vec!["id".to_string(), "status".to_string()];

        assert!(
            filter
                .should_filter("t", &fields, &["1", "inactive"])
                .is_some()
        );
        assert!(
            filter
                .should_filter("t", &fields, &["2", "archived"])
                .is_some()
        );
        assert!(
            filter
                .should_filter("t", &fields, &["3", "active"])
                .is_none()
        );
    }

    /// An exclude rule referencing a field that does not exist in the table's
    /// field list is silently skipped — no records are filtered.
    #[test]
    fn test_should_filter_exclude_skipped_when_field_not_in_table() {
        let filter = FilterConfig {
            exclude: vec![make_rule(vec![], "nonexistent", "^value$")],
            ..Default::default()
        };
        let fields = vec!["id".to_string(), "name".to_string()];

        assert!(
            filter
                .should_filter("t", &fields, &["1", "Alice"])
                .is_none()
        );
    }

    #[test]
    fn test_should_filter_exclude_table_scoped() {
        let filter = FilterConfig {
            exclude: vec![make_rule(vec!["users"], "status", "^inactive$")],
            ..Default::default()
        };
        let fields = vec!["id".to_string(), "status".to_string()];

        // Applies to "users" table
        assert!(
            filter
                .should_filter("users", &fields, &["1", "inactive"])
                .is_some()
        );
        // Does not apply to "orders" table
        assert!(
            filter
                .should_filter("orders", &fields, &["1", "inactive"])
                .is_none()
        );
    }

    #[test]
    fn test_should_filter_exclude_multiple_tables() {
        let filter = FilterConfig {
            exclude: vec![make_rule(vec!["users", "admins"], "status", "^inactive$")],
            ..Default::default()
        };
        let fields = vec!["id".to_string(), "status".to_string()];

        assert!(
            filter
                .should_filter("users", &fields, &["1", "inactive"])
                .is_some()
        );
        assert!(
            filter
                .should_filter("admins", &fields, &["1", "inactive"])
                .is_some()
        );
        assert!(
            filter
                .should_filter("orders", &fields, &["1", "inactive"])
                .is_none()
        );
    }

    #[test]
    fn test_invalid_regex_fails_to_load() {
        let toml_input = r#"
[[filters.exclude]]
field = "status"
regex = "["

[tables.users]
source = "users.csv"
fields = [
    { name = "id", type = "NUMBER", primary-key = true },
]
"#;
        let result: Result<Config, _> = toml::from_str(toml_input);
        let err = result.expect_err("invalid regex should fail to parse");
        assert!(
            err.to_string().contains("regex"),
            "expected error to mention 'regex', got: {err}"
        );
    }

    #[test]
    fn test_should_filter_default_config() {
        let filter = FilterConfig::default();
        let fields = vec!["id".to_string(), "name".to_string()];

        assert!(
            filter
                .should_filter("t", &fields, &["1", "Alice"])
                .is_none()
        );
    }

    #[test]
    fn test_should_filter_include_match_keeps_record() {
        let filter = FilterConfig {
            include: vec![make_rule(vec![], "status", "^(active|pending)$")],
            ..Default::default()
        };
        let fields = vec!["id".to_string(), "status".to_string()];

        assert!(
            filter
                .should_filter("t", &fields, &["1", "active"])
                .is_none()
        );
        assert!(
            filter
                .should_filter("t", &fields, &["2", "pending"])
                .is_none()
        );
    }

    #[test]
    fn test_should_filter_include_no_match_drops_record() {
        let filter = FilterConfig {
            include: vec![make_rule(vec![], "status", "^(active|pending)$")],
            ..Default::default()
        };
        let fields = vec!["id".to_string(), "status".to_string()];

        assert!(
            filter
                .should_filter("t", &fields, &["1", "inactive"])
                .is_some()
        );
    }

    #[test]
    fn test_should_filter_include_unanchored_regex() {
        let filter = FilterConfig {
            include: vec![make_rule(vec![], "desc", "PRODUCTION")],
            ..Default::default()
        };
        let fields = vec!["id".to_string(), "desc".to_string()];

        assert!(
            filter
                .should_filter("t", &fields, &["1", "PRODUCTION ready"])
                .is_none()
        );
        assert!(
            filter
                .should_filter("t", &fields, &["2", "draft item"])
                .is_some()
        );
    }

    /// Multiple include rules combine with OR semantics: a record passes if
    /// it matches at least one applicable rule.
    #[test]
    fn test_should_filter_include_or_semantics() {
        let filter = FilterConfig {
            include: vec![
                make_rule(vec![], "status", "^active$"),
                make_rule(vec![], "status", "^pending$"),
            ],
            ..Default::default()
        };
        let fields = vec!["id".to_string(), "status".to_string()];

        assert!(
            filter
                .should_filter("t", &fields, &["1", "active"])
                .is_none()
        );
        assert!(
            filter
                .should_filter("t", &fields, &["2", "pending"])
                .is_none()
        );
        assert!(
            filter
                .should_filter("t", &fields, &["3", "archived"])
                .is_some()
        );
    }

    /// An include rule whose `field` does not exist in the table is silently
    /// skipped. When that's the only include rule, the record is unconstrained.
    #[test]
    fn test_should_filter_include_skipped_when_field_not_in_table() {
        let filter = FilterConfig {
            include: vec![make_rule(vec![], "nonexistent", "^value$")],
            ..Default::default()
        };
        let fields = vec!["id".to_string(), "name".to_string()];

        assert!(
            filter
                .should_filter("t", &fields, &["1", "Alice"])
                .is_none()
        );
    }

    /// When include rules exist but all are scoped to other tables, the
    /// current table is unconstrained — records pass.
    #[test]
    fn test_should_filter_include_no_applicable_rule_keeps_record() {
        let filter = FilterConfig {
            include: vec![make_rule(vec!["users"], "status", "^active$")],
            ..Default::default()
        };
        let fields = vec!["id".to_string(), "status".to_string()];

        assert!(
            filter
                .should_filter("orders", &fields, &["1", "archived"])
                .is_none()
        );
    }

    #[test]
    fn test_should_filter_include_table_scoped() {
        let filter = FilterConfig {
            include: vec![make_rule(vec!["users"], "status", "^active$")],
            ..Default::default()
        };
        let fields = vec!["id".to_string(), "status".to_string()];

        assert!(
            filter
                .should_filter("users", &fields, &["1", "active"])
                .is_none()
        );
        assert!(
            filter
                .should_filter("users", &fields, &["2", "inactive"])
                .is_some()
        );
        // "orders" table is unconstrained (rule scoped to "users")
        assert!(
            filter
                .should_filter("orders", &fields, &["3", "anything"])
                .is_none()
        );
    }

    /// When a record matches both an include rule and an exclude rule, the
    /// exclude rule wins. Reason strings are checked exactly to disambiguate
    /// which rule fired — `.is_some()` would not catch a regression where
    /// include accidentally short-circuits exclude.
    #[test]
    fn test_should_filter_exclude_wins_over_include() {
        let filter = FilterConfig {
            include: vec![make_rule(vec![], "status", "^(active|pending)$")],
            exclude: vec![make_rule(vec![], "status", "^pending$")],
            ..Default::default()
        };
        let fields = vec!["id".to_string(), "status".to_string()];

        // active: matches include, no exclude → kept
        assert!(
            filter
                .should_filter("t", &fields, &["1", "active"])
                .is_none()
        );
        // pending: matches both → dropped with the exclude reason
        assert_eq!(
            filter
                .should_filter("t", &fields, &["2", "pending"])
                .as_deref(),
            Some("field 'status' matches exclude rule"),
        );
        // archived: doesn't match include → dropped with the include reason
        assert_eq!(
            filter
                .should_filter("t", &fields, &["3", "archived"])
                .as_deref(),
            Some("no include rule matched"),
        );
    }

    #[test]
    fn test_load_fails_when_both_toml_and_json_present() {
        let dir = tempfile::tempdir().unwrap();
        let minimal_toml = r#"
[tables.users]
source = "users.csv"
fields = [
    { name = "id", type = "NUMBER", primary-key = true },
]
"#;
        let minimal_json = r#"{
  "tables": {
    "users": {
      "source": "users.csv",
      "fields": [{ "name": "id", "type": "NUMBER", "primary-key": true }]
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
    fn test_invalid_include_regex_fails_to_load() {
        let toml_input = r#"
[[filters.include]]
field = "status"
regex = "["

[tables.users]
source = "users.csv"
fields = [
    { name = "id", type = "NUMBER", primary-key = true },
]
"#;
        let result: Result<Config, _> = toml::from_str(toml_input);
        let err = result.expect_err("invalid regex should fail to parse");
        assert!(
            err.to_string().contains("regex"),
            "expected error to mention 'regex', got: {err}"
        );
    }
}
