use regex::Regex;
use serde::{Deserialize, Deserializer};
use serde_json::Value;
use std::collections::{HashMap, HashSet};
use std::fs;
use std::path::{Path, PathBuf};
use std::sync::Mutex;
use std::time::Duration;

use anyhow::{Context, Result, bail};

use crate::cell::Kind;
use crate::utils::{parse_duration, parse_file_mode, validate_field_name};

/// Subdirectory of the work directory where state files live when `state-dir`
/// is not set in the config.
const STATE_SUBDIR: &str = "state";

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

/// Default Unix permission bits for files created in the work directory.
/// Secure-by-default: only the owner can read or write.
fn default_file_mode() -> u32 {
    0o600
}

/// Default Unix permission bits for the state directory when leech2 creates it.
/// Secure-by-default: only the owner can read, write, or traverse it.
fn default_dir_mode() -> u32 {
    0o700
}

/// Serde default for boolean options that are enabled unless turned off.
fn default_true() -> bool {
    true
}

// Custom deserializer for `file-mode`: reads the field as a string and parses it
// via `parse_file_mode`. The parsed value is range checked in `Config::validate`.
fn deserialize_file_mode<'de, D>(deserializer: D) -> Result<u32, D::Error>
where
    D: Deserializer<'de>,
{
    let raw = String::deserialize(deserializer)?;
    parse_file_mode(&raw).map_err(serde::de::Error::custom)
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

/// Controls the opt-in cumulative stats file written after patch creation.
#[derive(Debug, Default, Deserialize)]
#[serde(default, deny_unknown_fields)]
pub struct StatsConfig {
    /// When true, `patch create` appends a run record to the `STATS` JSON file
    /// in the state directory.
    pub enable: bool,
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
}

impl Validate for CsvConfig {
    fn validate(&self) -> Result<()> {
        if self.source.is_empty() {
            bail!("csv.source must not be empty");
        }
        Ok(())
    }
}

/// Validated configuration: the base `config.toml`/`config.json` in the work
/// directory deep-merged with any drop-in fragments it pulls in via `include`.
#[derive(Debug, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct Config {
    /// Directory the config was loaded from; populated by `Config::load`, not
    /// deserialized.
    #[serde(skip)]
    pub work_dir: PathBuf,
    /// Optional override for where state files (`HEAD`, `STATE`, `REPORTED`,
    /// block files) live. Relative paths resolve against `work_dir`; absolute
    /// paths are used as-is. When unset, state lives in a `state` subdirectory
    /// of `work_dir`. Consumers should read the resolved directory via
    /// `state_dir()`.
    #[serde(default, rename = "state-dir")]
    pub(crate) state_dir: Option<PathBuf>,
    /// Zstd compression settings for patch payloads.
    #[serde(default)]
    pub compression: CompressionConfig,
    /// Cumulative patch-creation stats file settings.
    #[serde(default)]
    pub stats: StatsConfig,
    /// Per-table source-file and field schemas, keyed by table name.
    #[serde(default)]
    pub tables: HashMap<String, TableConfig>,
    /// Event streams, keyed by name.
    #[serde(default)]
    pub events: HashMap<String, EventConfig>,
    /// Block chain truncation policy.
    #[serde(default)]
    pub truncate: TruncateConfig,
    /// Unix permission bits for files created in the work directory, written
    /// as an octal string (e.g. `"0600"`). Ignored on non-Unix platforms.
    #[serde(
        default = "default_file_mode",
        rename = "file-mode",
        deserialize_with = "deserialize_file_mode"
    )]
    pub file_mode: u32,
    /// Unix permission bits for the state directory when leech2 creates it,
    /// written as an octal string (e.g. `"0700"`). Ignored on non-Unix
    /// platforms.
    #[serde(
        default = "default_dir_mode",
        rename = "dir-mode",
        deserialize_with = "deserialize_file_mode"
    )]
    pub dir_mode: u32,
    /// Stage stats for the patch-creation run currently in flight. Operations
    /// record into this as they run; `stats::finalize_patch_create` drains it
    /// to the `STATS` file. Not deserialized.
    #[serde(skip)]
    pub(crate) pending_stats: Mutex<crate::stats::PendingStats>,
    /// When true, CLI create/mutate operations skip all disk writes and print
    /// "Would have ..." messages instead. CLI-only; set by `lch --dry-run`,
    /// never deserialized.
    #[serde(skip)]
    pub dry_run: bool,
}

impl Default for Config {
    fn default() -> Self {
        Config {
            work_dir: PathBuf::new(),
            state_dir: None,
            compression: CompressionConfig::default(),
            stats: StatsConfig::default(),
            tables: HashMap::new(),
            events: HashMap::new(),
            truncate: TruncateConfig::default(),
            file_mode: default_file_mode(),
            dir_mode: default_dir_mode(),
            pending_stats: Default::default(),
            dry_run: false,
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
    /// Free-form note describing what the field is for. Ignored by leech2;
    /// useful for documenting fields in JSON config, which has no comment
    /// syntax.
    #[serde(default)]
    pub comment: Option<String>,
}

impl Default for FieldConfig {
    fn default() -> Self {
        Self {
            name: String::new(),
            kind: Kind::Text,
            primary_key: false,
            comment: None,
        }
    }
}

/// Values written into a change log's operation column, one per operation.
#[derive(Debug, Deserialize)]
#[serde(default, deny_unknown_fields)]
pub struct LogOperations {
    pub insert: String,
    pub update: String,
    pub delete: String,
}

impl Default for LogOperations {
    fn default() -> Self {
        Self {
            insert: "INSERT".to_string(),
            update: "UPDATE".to_string(),
            delete: "DELETE".to_string(),
        }
    }
}

fn default_log_operation_field() -> String {
    "operation".to_string()
}

fn default_log_timestamp_field() -> String {
    "timestamp".to_string()
}

/// Opt-in change log for a table. Each insert, update and delete in a block is
/// recorded as an event, which the receiver writes as a row in the log table.
#[derive(Debug, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct LogConfig {
    /// Name of the log table.
    pub table: String,
    /// Log column that receives each row's event ID, built from the hash of
    /// the block that recorded the change and the change's position in that
    /// block.
    #[serde(rename = "id-field")]
    pub id_field: String,
    /// Log column that receives the operation (see `operations`).
    #[serde(default = "default_log_operation_field", rename = "operation-field")]
    pub operation_field: String,
    /// Log column that receives the record's change timestamp.
    #[serde(default = "default_log_timestamp_field", rename = "timestamp-field")]
    pub timestamp_field: String,
    /// Values written into `operation-field` for each operation.
    #[serde(default)]
    pub operations: LogOperations,
    /// Fields of the table that the log table does not have.
    #[serde(default)]
    pub exclude: Vec<String>,
}

impl LogConfig {
    fn validate(&self, fields: &[FieldConfig]) -> Result<()> {
        validate_field_name(&self.table).context("log.table")?;
        validate_field_name(&self.id_field).context("log.id-field")?;
        validate_field_name(&self.operation_field).context("log.operation-field")?;
        validate_field_name(&self.timestamp_field).context("log.timestamp-field")?;
        let columns = [
            ("id-field", &self.id_field),
            ("operation-field", &self.operation_field),
            ("timestamp-field", &self.timestamp_field),
        ];
        for (index, (name, column)) in columns.iter().enumerate() {
            for (other_name, other_column) in &columns[index + 1..] {
                if column == other_column {
                    bail!("log.{} and log.{} are both '{}'", name, other_name, column);
                }
            }
        }

        for column in [&self.id_field, &self.operation_field, &self.timestamp_field] {
            if fields.iter().any(|field| &field.name == column) {
                bail!("log column '{}' collides with a field", column);
            }
        }

        let mut excluded = HashSet::new();
        for name in &self.exclude {
            let Some(field) = fields.iter().find(|field| &field.name == name) else {
                bail!("log.exclude references unknown field '{}'", name);
            };
            if field.primary_key {
                bail!("log.exclude must not contain primary-key field '{}'", name);
            }
            if !excluded.insert(name.as_str()) {
                bail!("log.exclude contains '{}' more than once", name);
            }
        }
        Ok(())
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
    /// When true (the default), patch creation sends this table as a full
    /// state instead of a consolidated delta whenever the full state encodes
    /// smaller. When false, full state is only used when delta creation fails
    /// (e.g. due to new tables, layout changes, missing blocks).
    #[serde(rename = "use-full-state-if-smaller", default = "default_true")]
    pub use_full_state_if_smaller: bool,
    /// Name of a SQL column that receives the creation time of the last block
    /// that changed each record. Written on INSERT and UPDATE. Records sent as
    /// part of a full state get NULL. When absent, no such column is written.
    #[serde(default, rename = "change-timestamp")]
    pub change_timestamp: Option<String>,
    /// Opt-in change log. When absent, no log is written.
    #[serde(default)]
    pub log: Option<LogConfig>,
}

impl Validate for FieldConfig {
    fn validate(&self) -> Result<()> {
        validate_field_name(&self.name)
    }
}

impl Default for TableConfig {
    fn default() -> Self {
        Self {
            fields: Vec::new(),
            csv: None,
            use_full_state_if_smaller: default_true(),
            change_timestamp: None,
            log: None,
        }
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
            csv.validate()?;
        }

        if let Some(column) = &self.change_timestamp {
            validate_field_name(column).context("change-timestamp")?;
            if seen.contains(column.as_str()) {
                bail!("change-timestamp '{}' collides with a field", column);
            }
        }

        if let Some(log) = &self.log {
            log.validate(&self.fields)?;
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

    /// Whether patch creation tracks per-record change times for this table.
    pub fn tracks_changes(&self) -> bool {
        self.change_timestamp.is_some() || self.log.is_some()
    }
}

/// An event stream. Each block stores the events supplied since the previous
/// block, and the receiver only ever inserts them. Events are pulled from the
/// FFI cell callback.
#[derive(Debug, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct EventConfig {
    /// Column definitions. Events have no primary key.
    pub fields: Vec<FieldConfig>,
    /// Column that receives each event's ID, built from the hash of the block
    /// that stored the event and the event's position in that block.
    #[serde(rename = "id-field")]
    pub id_field: String,
}

impl Validate for EventConfig {
    fn validate(&self) -> Result<()> {
        if self.fields.is_empty() {
            bail!("at least one field must be declared");
        }

        let mut seen = HashSet::new();
        for field in &self.fields {
            field.validate()?;
            if field.primary_key {
                bail!("field '{}': events have no primary key", field.name);
            }
            if !seen.insert(field.name.as_str()) {
                bail!("found duplicate field name '{}'", field.name);
            }
        }

        validate_field_name(&self.id_field).context("id-field")?;
        if seen.contains(self.id_field.as_str()) {
            bail!("id-field '{}' collides with a field", self.id_field);
        }

        Ok(())
    }
}

impl Validate for Config {
    fn validate(&self) -> Result<()> {
        if self.tables.is_empty() && self.events.is_empty() {
            bail!("at least one table or event stream must be declared");
        }
        let mut log_tables = HashSet::new();
        for (name, table) in &self.tables {
            table
                .validate()
                .with_context(|| format!("table '{}'", name))?;
            let Some(log) = &table.log else {
                continue;
            };
            if self.tables.contains_key(&log.table) || self.events.contains_key(&log.table) {
                bail!(
                    "table '{}': log.table '{}' is also declared as a table or event stream",
                    name,
                    log.table
                );
            }
            if !log_tables.insert(log.table.as_str()) {
                bail!(
                    "table '{}': log.table '{}' is used by another table",
                    name,
                    log.table
                );
            }
        }
        for (name, event) in &self.events {
            event
                .validate()
                .with_context(|| format!("event stream '{}'", name))?;
            if self.tables.contains_key(name) {
                bail!("event stream '{}' is also declared as a table", name);
            }
        }

        if self.file_mode > 0o777 {
            bail!(
                "file-mode {:o} is out of range (must be <= 0o777)",
                self.file_mode
            );
        }

        if self.dir_mode > 0o777 {
            bail!(
                "dir-mode {:o} is out of range (must be <= 0o777)",
                self.dir_mode
            );
        }

        self.truncate.validate()?;
        self.compression.validate()?;

        Ok(())
    }
}

/// Parse a single config file into an untyped value tree, selecting the parser
/// by file extension (`.toml` or `.json`). Parsing into [`serde_json::Value`]
/// rather than [`Config`] gives a common representation that fragments of either
/// format can be deep-merged into before a single final deserialization.
fn parse_fragment(path: &Path) -> Result<Value> {
    let content = fs::read_to_string(path)
        .with_context(|| format!("failed to read config file '{}'", path.display()))?;
    let extension = path.extension().and_then(|extension| extension.to_str());
    match extension {
        Some("toml") => toml::from_str(&content)
            .with_context(|| format!("failed to parse config TOML file '{}'", path.display())),
        Some("json") => serde_json::from_str(&content)
            .with_context(|| format!("failed to parse config JSON file '{}'", path.display())),
        _ => bail!(
            "config file '{}' must have a '.toml' or '.json' extension",
            path.display()
        ),
    }
}

/// Deep-merge `overlay` into `base` (last writer wins). Objects are merged
/// key-by-key, recursing into shared keys, so the `tables` map unions by table
/// name and sections like `compression` merge field-by-field. Any other value
/// (array or scalar) replaces what's in `base`, so a later fragment's table
/// `fields` list overrides the earlier one wholesale.
fn deep_merge(base: &mut Value, overlay: Value) {
    match (base, overlay) {
        (Value::Object(base_map), Value::Object(overlay_map)) => {
            for (key, overlay_value) in overlay_map {
                match base_map.get_mut(&key) {
                    Some(base_value) => deep_merge(base_value, overlay_value),
                    None => {
                        base_map.insert(key, overlay_value);
                    }
                }
            }
        }
        (base_slot, overlay_value) => *base_slot = overlay_value,
    }
}

/// Take the base config's `include` glob patterns out of its value tree. Removing
/// the key keeps it out of the final `Config` deserialization, which would
/// otherwise reject it under `deny_unknown_fields`.
fn take_include_patterns(base: &mut Value, base_path: &Path) -> Result<Vec<String>> {
    let Some(map) = base.as_object_mut() else {
        return Ok(Vec::new());
    };
    let Some(value) = map.remove("include") else {
        return Ok(Vec::new());
    };
    serde_json::from_value(value).with_context(|| {
        format!(
            "'include' in '{}' must be a list of strings",
            base_path.display()
        )
    })
}

/// Expand the base config's `include` glob patterns into an ordered list of
/// fragment paths. Relative patterns resolve against `work_dir`. Patterns are
/// processed in order; each pattern's matches are sorted lexicographically.
/// The base file and already-seen paths are skipped so every fragment loads at
/// most once. A pattern that matches nothing is not an error.
fn resolve_includes(
    work_dir: &Path,
    patterns: &[String],
    base_path: &Path,
) -> Result<Vec<PathBuf>> {
    let mut paths = Vec::new();
    let mut seen = HashSet::new();
    seen.insert(base_path.to_path_buf());

    for pattern in patterns {
        let joined = if Path::new(pattern).is_absolute() {
            PathBuf::from(pattern)
        } else {
            work_dir.join(pattern)
        };
        let joined = joined
            .to_str()
            .ok_or_else(|| anyhow::anyhow!("include pattern '{}' is not valid UTF-8", pattern))?;

        let mut matches = Vec::new();
        for entry in
            glob::glob(joined).with_context(|| format!("invalid include pattern '{}'", pattern))?
        {
            matches.push(
                entry.with_context(|| format!("failed to read match for include '{}'", pattern))?,
            );
        }

        if matches.is_empty() {
            log::debug!("Include pattern '{}' matched no files", pattern);
            continue;
        }

        matches.sort();
        for path in matches {
            if seen.insert(path.clone()) {
                paths.push(path);
            }
        }
    }

    Ok(paths)
}

impl Config {
    /// Directory holding state files, resolved from the optional `state-dir`
    /// config value: relative to `work_dir`, absolute as-is, or the `state`
    /// subdirectory of `work_dir` when unset.
    pub fn state_dir(&self) -> PathBuf {
        match &self.state_dir {
            Some(dir) if dir.is_absolute() => dir.clone(),
            Some(dir) => self.work_dir.join(dir),
            None => self.work_dir.join(STATE_SUBDIR),
        }
    }

    /// Resolve the state directory (see [`Config::state_dir`]) and create it,
    /// and any missing parents, with the configured `dir-mode`. Idempotent, so
    /// callers can invoke it before any state I/O without checking first.
    pub fn ensure_state_dir(&self) -> Result<PathBuf> {
        let state_dir = self.state_dir();
        let mut builder = fs::DirBuilder::new();
        builder.recursive(true);
        #[cfg(unix)]
        {
            use std::os::unix::fs::DirBuilderExt;
            builder.mode(self.dir_mode);
        }
        builder.create(&state_dir).with_context(|| {
            format!("failed to create state directory '{}'", state_dir.display())
        })?;
        Ok(state_dir)
    }

    pub fn load(work_dir: &Path) -> Result<Config> {
        let toml_path = work_dir.join("config.toml");
        let json_path = work_dir.join("config.json");

        let base_path = match (toml_path.exists(), json_path.exists()) {
            (true, true) => {
                bail!("found both config.toml and config.json (don't know which one to pick)")
            }
            (true, false) => toml_path,
            (false, true) => json_path,
            (false, false) => bail!(
                "no config file found in '{}' (expected config.toml or config.json)",
                work_dir.display()
            ),
        };

        log::debug!("Parsing config from file '{}'...", base_path.display());
        let mut merged = parse_fragment(&base_path)?;
        let include_patterns = take_include_patterns(&mut merged, &base_path)?;

        for path in resolve_includes(work_dir, &include_patterns, &base_path)? {
            log::debug!("Merging config fragment '{}'...", path.display());
            let fragment = parse_fragment(&path)?;
            if fragment.get("include").is_some() {
                bail!(
                    "config fragment '{}' may not declare 'include' (nested includes are not supported)",
                    path.display()
                );
            }
            deep_merge(&mut merged, fragment);
        }

        // `serde_path_to_error` prefixes the offending key path (e.g.
        // `truncate.max-age`) onto deserialization errors, which a plain
        // `serde_json::from_value` would otherwise drop.
        let mut config: Config = serde_path_to_error::deserialize(merged)
            .context("failed to build config from merged files")?;
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
    fn test_field_comment_is_parsed() {
        let dir = tempfile::tempdir().unwrap();
        let json_input = r#"{
  "tables": {
    "users": {
      "fields": [
        { "name": "id", "type": "NUMBER", "primary-key": true, "comment": "user identifier" },
        { "name": "name", "type": "TEXT" }
      ]
    }
  }
}"#;
        fs::write(dir.path().join("config.json"), json_input).unwrap();
        let config = Config::load(dir.path()).unwrap();
        let fields = &config.tables.get("users").unwrap().fields;
        assert_eq!(fields[0].comment.as_deref(), Some("user identifier"));
        assert_eq!(fields[1].comment, None);
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

    #[test]
    fn test_load_rejects_control_character_in_field_name() {
        let dir = tempfile::tempdir().unwrap();
        let toml_input = "
[tables.users]
fields = [
    { name = \"id\\nevil\", type = \"NUMBER\", primary-key = true },
]
";
        fs::write(dir.path().join("config.toml"), toml_input).unwrap();
        let err = Config::load(dir.path()).expect_err("expected validation error");
        let msg = format!("{:#}", err);
        assert!(
            msg.contains("control character"),
            "expected error to mention the control character, got: {msg}"
        );
    }

    fn minimal_config_with(extra: &str) -> String {
        format!(
            "{}\n[tables.users]\nfields = [\n    {{ name = \"id\", type = \"NUMBER\", primary-key = true }},\n]\n",
            extra
        )
    }

    #[test]
    fn test_file_mode_defaults_to_0600() {
        let dir = tempfile::tempdir().unwrap();
        fs::write(dir.path().join("config.toml"), minimal_config_with("")).unwrap();
        let config = Config::load(dir.path()).unwrap();
        assert_eq!(config.file_mode, 0o600);
    }

    #[test]
    fn test_file_mode_parses_octal_string() {
        let dir = tempfile::tempdir().unwrap();
        fs::write(
            dir.path().join("config.toml"),
            minimal_config_with("file-mode = \"0640\""),
        )
        .unwrap();
        let config = Config::load(dir.path()).unwrap();
        assert_eq!(config.file_mode, 0o640);
    }

    #[test]
    fn test_file_mode_accepts_0o_prefix() {
        let dir = tempfile::tempdir().unwrap();
        fs::write(
            dir.path().join("config.toml"),
            minimal_config_with("file-mode = \"0o644\""),
        )
        .unwrap();
        let config = Config::load(dir.path()).unwrap();
        assert_eq!(config.file_mode, 0o644);
    }

    #[test]
    fn test_file_mode_out_of_range_rejected() {
        let dir = tempfile::tempdir().unwrap();
        fs::write(
            dir.path().join("config.toml"),
            minimal_config_with("file-mode = \"1000\""),
        )
        .unwrap();
        let err = Config::load(dir.path()).expect_err("expected out-of-range error");
        let msg = format!("{:#}", err);
        assert!(
            msg.contains("file-mode"),
            "expected error to mention 'file-mode', got: {msg}"
        );
    }

    #[test]
    fn test_file_mode_non_octal_rejected() {
        let dir = tempfile::tempdir().unwrap();
        fs::write(
            dir.path().join("config.toml"),
            minimal_config_with("file-mode = \"99\""),
        )
        .unwrap();
        let err = Config::load(dir.path()).expect_err("expected parse error");
        let msg = format!("{:#}", err);
        assert!(
            msg.contains("file-mode"),
            "expected error to mention 'file-mode', got: {msg}"
        );
    }

    #[test]
    fn test_dir_mode_defaults_to_0700() {
        let dir = tempfile::tempdir().unwrap();
        fs::write(dir.path().join("config.toml"), minimal_config_with("")).unwrap();
        let config = Config::load(dir.path()).unwrap();
        assert_eq!(config.dir_mode, 0o700);
    }

    #[test]
    fn test_dir_mode_parses_octal_string() {
        let dir = tempfile::tempdir().unwrap();
        fs::write(
            dir.path().join("config.toml"),
            minimal_config_with("dir-mode = \"0750\""),
        )
        .unwrap();
        let config = Config::load(dir.path()).unwrap();
        assert_eq!(config.dir_mode, 0o750);
    }

    #[test]
    fn test_dir_mode_out_of_range_rejected() {
        let dir = tempfile::tempdir().unwrap();
        fs::write(
            dir.path().join("config.toml"),
            minimal_config_with("dir-mode = \"1000\""),
        )
        .unwrap();
        let err = Config::load(dir.path()).expect_err("expected out-of-range error");
        let msg = format!("{:#}", err);
        assert!(
            msg.contains("dir-mode"),
            "expected error to mention 'dir-mode', got: {msg}"
        );
    }

    #[test]
    fn test_state_dir_defaults_to_subdir() {
        let dir = tempfile::tempdir().unwrap();
        fs::write(dir.path().join("config.toml"), minimal_config_with("")).unwrap();
        let config = Config::load(dir.path()).unwrap();
        assert_eq!(config.state_dir(), dir.path().join("state"));
    }

    #[test]
    fn test_state_dir_relative_resolves_against_work_dir() {
        let dir = tempfile::tempdir().unwrap();
        fs::write(
            dir.path().join("config.toml"),
            minimal_config_with("state-dir = \"db\""),
        )
        .unwrap();
        let config = Config::load(dir.path()).unwrap();
        assert_eq!(config.state_dir(), dir.path().join("db"));
    }

    #[test]
    fn test_state_dir_absolute_used_as_is() {
        let absolute = if cfg!(windows) {
            "C:/var/lib/leech2"
        } else {
            "/var/lib/leech2"
        };
        let dir = tempfile::tempdir().unwrap();
        fs::write(
            dir.path().join("config.toml"),
            minimal_config_with(&format!("state-dir = \"{absolute}\"")),
        )
        .unwrap();
        let config = Config::load(dir.path()).unwrap();
        assert_eq!(config.state_dir(), PathBuf::from(absolute));
    }

    #[test]
    fn test_include_merges_fragment_tables() {
        let dir = tempfile::tempdir().unwrap();
        let base = r#"
include = ["drop-in/*.toml"]

[tables.users]
fields = [
    { name = "id", type = "NUMBER", primary-key = true },
]
"#;
        let fragment = r#"
[tables.products]
fields = [
    { name = "sku", type = "TEXT", primary-key = true },
]
"#;
        fs::write(dir.path().join("config.toml"), base).unwrap();
        fs::create_dir(dir.path().join("drop-in")).unwrap();
        fs::write(dir.path().join("drop-in/extra.toml"), fragment).unwrap();

        let config = Config::load(dir.path()).unwrap();
        assert!(config.tables.contains_key("users"));
        assert!(config.tables.contains_key("products"));
    }

    #[test]
    fn test_include_duplicate_table_last_wins() {
        let dir = tempfile::tempdir().unwrap();
        let base = r#"
include = ["drop-in/*.toml"]

[tables.users]
fields = [
    { name = "id", type = "NUMBER", primary-key = true },
    { name = "name", type = "TEXT" },
]
"#;
        // The fragment redefines `users` with a single field; last-wins means
        // the merged table has only that field.
        let fragment = r#"
[tables.users]
fields = [
    { name = "uuid", type = "TEXT", primary-key = true },
]
"#;
        fs::write(dir.path().join("config.toml"), base).unwrap();
        fs::create_dir(dir.path().join("drop-in")).unwrap();
        fs::write(dir.path().join("drop-in/override.toml"), fragment).unwrap();

        let config = Config::load(dir.path()).unwrap();
        let fields = &config.tables.get("users").unwrap().fields;
        assert_eq!(fields.len(), 1);
        assert_eq!(fields[0].name, "uuid");
    }

    #[test]
    fn test_include_deep_merges_section_subkeys() {
        // The fragment sets only `csv.source`; deep-merge keeps the base table's
        // fields and the rest of its csv block.
        let dir = tempfile::tempdir().unwrap();
        let base = r#"
include = ["drop-in/*.toml"]

[tables.users]
fields = [
    { name = "id", type = "NUMBER", primary-key = true },
]

[tables.users.csv]
source = "base.csv"
header = true
"#;
        let fragment = r#"
[tables.users.csv]
source = "override.csv"
"#;
        fs::write(dir.path().join("config.toml"), base).unwrap();
        fs::create_dir(dir.path().join("drop-in")).unwrap();
        fs::write(dir.path().join("drop-in/source.toml"), fragment).unwrap();

        let config = Config::load(dir.path()).unwrap();
        let csv = config.tables.get("users").unwrap().csv.as_ref().unwrap();
        assert_eq!(csv.source, "override.csv");
        // Untouched sub-keys survive the merge.
        assert!(csv.header);
        assert_eq!(config.tables.get("users").unwrap().fields.len(), 1);
    }

    #[test]
    fn test_include_global_section_fragment_overrides_base() {
        let dir = tempfile::tempdir().unwrap();
        let base = r#"
include = ["drop-in/*.toml"]
file-mode = "0600"

[tables.users]
fields = [
    { name = "id", type = "NUMBER", primary-key = true },
]
"#;
        let fragment = "file-mode = \"0640\"\n";
        fs::write(dir.path().join("config.toml"), base).unwrap();
        fs::create_dir(dir.path().join("drop-in")).unwrap();
        fs::write(dir.path().join("drop-in/mode.toml"), fragment).unwrap();

        let config = Config::load(dir.path()).unwrap();
        assert_eq!(config.file_mode, 0o640);
    }

    #[test]
    fn test_fragment_declaring_include_rejected() {
        let dir = tempfile::tempdir().unwrap();
        let base = r#"
include = ["drop-in/*.toml"]

[tables.users]
fields = [
    { name = "id", type = "NUMBER", primary-key = true },
]
"#;
        let fragment = r#"
include = ["other/*.toml"]

[tables.products]
fields = [
    { name = "sku", type = "TEXT", primary-key = true },
]
"#;
        fs::write(dir.path().join("config.toml"), base).unwrap();
        fs::create_dir(dir.path().join("drop-in")).unwrap();
        fs::write(dir.path().join("drop-in/nested.toml"), fragment).unwrap();

        let err = Config::load(dir.path()).expect_err("expected nested-include error");
        let msg = format!("{:#}", err);
        assert!(
            msg.contains("may not declare 'include'"),
            "expected error about nested includes, got: {msg}"
        );
    }

    #[test]
    fn test_fragment_unknown_extension_rejected() {
        let dir = tempfile::tempdir().unwrap();
        let base = r#"
include = ["drop-in/*"]

[tables.users]
fields = [
    { name = "id", type = "NUMBER", primary-key = true },
]
"#;
        fs::write(dir.path().join("config.toml"), base).unwrap();
        fs::create_dir(dir.path().join("drop-in")).unwrap();
        fs::write(dir.path().join("drop-in/extra.yaml"), "tables: {}").unwrap();

        let err = Config::load(dir.path()).expect_err("expected unknown-extension error");
        let msg = format!("{:#}", err);
        assert!(
            msg.contains("'.toml' or '.json' extension"),
            "expected error about the extension, got: {msg}"
        );
    }

    #[test]
    fn test_include_mixes_toml_base_and_json_fragment() {
        let dir = tempfile::tempdir().unwrap();
        let base = r#"
include = ["drop-in/*.json"]

[tables.users]
fields = [
    { name = "id", type = "NUMBER", primary-key = true },
]
"#;
        let fragment = r#"{
  "tables": {
    "products": {
      "fields": [{ "name": "sku", "type": "TEXT", "primary-key": true }]
    }
  }
}"#;
        fs::write(dir.path().join("config.toml"), base).unwrap();
        fs::create_dir(dir.path().join("drop-in")).unwrap();
        fs::write(dir.path().join("drop-in/extra.json"), fragment).unwrap();

        let config = Config::load(dir.path()).unwrap();
        assert!(config.tables.contains_key("users"));
        assert!(config.tables.contains_key("products"));
    }

    #[test]
    fn test_include_pattern_matching_nothing_is_ok() {
        let dir = tempfile::tempdir().unwrap();
        let base = r#"
include = ["drop-in/*.toml"]

[tables.users]
fields = [
    { name = "id", type = "NUMBER", primary-key = true },
]
"#;
        fs::write(dir.path().join("config.toml"), base).unwrap();

        let config = Config::load(dir.path()).unwrap();
        assert!(config.tables.contains_key("users"));
    }

    fn load_with_change_timestamp(column: &str) -> Result<Config> {
        let dir = tempfile::tempdir().unwrap();
        let toml_input = format!(
            r#"
[tables.users]
change-timestamp = "{column}"
fields = [
    {{ name = "id", type = "NUMBER", primary-key = true }},
    {{ name = "name", type = "TEXT" }},
]
"#
        );
        fs::write(dir.path().join("config.toml"), toml_input).unwrap();
        Config::load(dir.path())
    }

    #[test]
    fn test_change_timestamp_accepted() {
        let config = load_with_change_timestamp("changetimestamp").unwrap();
        let table = &config.tables["users"];
        assert_eq!(table.change_timestamp.as_deref(), Some("changetimestamp"));
        assert!(table.tracks_changes());
    }

    #[test]
    fn test_change_timestamp_defaults_to_none() {
        let dir = tempfile::tempdir().unwrap();
        let toml_input = r#"
[tables.users]
fields = [{ name = "id", type = "NUMBER", primary-key = true }]
"#;
        fs::write(dir.path().join("config.toml"), toml_input).unwrap();
        let config = Config::load(dir.path()).unwrap();
        assert!(!config.tables["users"].tracks_changes());
    }

    #[test]
    fn test_change_timestamp_rejects_field_collision() {
        let err = load_with_change_timestamp("name").unwrap_err();
        let msg = format!("{:#}", err);
        assert!(msg.contains("collides with a field"), "got: {msg}");
    }

    #[test]
    fn test_change_timestamp_rejects_control_character() {
        let err = load_with_change_timestamp("changed\\u0001").unwrap_err();
        let msg = format!("{:#}", err);
        assert!(msg.contains("control character"), "got: {msg}");
    }

    fn load_with_log(log: &str) -> Result<Config> {
        let dir = tempfile::tempdir().unwrap();
        let toml_input = format!(
            r#"
[tables.users]
fields = [
    {{ name = "id", type = "NUMBER", primary-key = true }},
    {{ name = "name", type = "TEXT" }},
    {{ name = "email", type = "TEXT" }},
]

[tables.users.log]
{log}
"#
        );
        fs::write(dir.path().join("config.toml"), toml_input).unwrap();
        Config::load(dir.path())
    }

    const LOG_TABLE: &str = "table = \"userslog\"\nid-field = \"eventid\"\n";

    fn load_log_error(extra: &str) -> String {
        let err = load_with_log(&format!("{LOG_TABLE}{extra}")).unwrap_err();
        format!("{:#}", err)
    }

    #[test]
    fn test_log_accepted() {
        let log = format!(
            r#"{LOG_TABLE}
operation-field = "op"
timestamp-field = "ts"
operations = {{ insert = "ADD", update = "CHANGE", delete = "REMOVE" }}
exclude = ["email"]
"#
        );
        let config = load_with_log(&log).unwrap();
        let table = &config.tables["users"];
        let log = table.log.as_ref().unwrap();
        assert_eq!(log.table, "userslog");
        assert_eq!(log.id_field, "eventid");
        assert_eq!(log.operation_field, "op");
        assert_eq!(log.timestamp_field, "ts");
        assert_eq!(log.operations.insert, "ADD");
        assert_eq!(log.operations.update, "CHANGE");
        assert_eq!(log.operations.delete, "REMOVE");
        assert_eq!(log.exclude, vec!["email".to_string()]);
        assert!(table.tracks_changes());
    }

    #[test]
    fn test_log_defaults() {
        let config = load_with_log(LOG_TABLE).unwrap();
        let log = config.tables["users"].log.as_ref().unwrap();
        assert_eq!(log.operation_field, "operation");
        assert_eq!(log.timestamp_field, "timestamp");
        assert_eq!(log.operations.insert, "INSERT");
        assert_eq!(log.operations.update, "UPDATE");
        assert_eq!(log.operations.delete, "DELETE");
        assert!(log.exclude.is_empty());
    }

    #[test]
    fn test_log_operations_partial_override() {
        let log = format!("{LOG_TABLE}operations = {{ delete = \"REMOVE\" }}\n");
        let config = load_with_log(&log).unwrap();
        let log = config.tables["users"].log.as_ref().unwrap();
        assert_eq!(log.operations.insert, "INSERT");
        assert_eq!(log.operations.update, "UPDATE");
        assert_eq!(log.operations.delete, "REMOVE");
    }

    #[test]
    fn test_log_requires_table() {
        let msg = format!("{:#}", load_with_log("").unwrap_err());
        assert!(msg.contains("missing field `table`"), "got: {msg}");
    }

    #[test]
    fn test_log_requires_id_field() {
        let msg = format!("{:#}", load_with_log("table = \"userslog\"\n").unwrap_err());
        assert!(msg.contains("missing field `id-field`"), "got: {msg}");
    }

    #[test]
    fn test_log_rejects_id_field_collision() {
        let msg = load_log_error("operation-field = \"eventid\"\n");
        assert!(msg.contains("are both 'eventid'"), "got: {msg}");

        let msg = format!(
            "{:#}",
            load_with_log("table = \"userslog\"\nid-field = \"name\"\n").unwrap_err()
        );
        assert!(msg.contains("collides with a field"), "got: {msg}");
    }

    #[test]
    fn test_log_rejects_unknown_key() {
        let msg = load_log_error("full-state = \"insert\"\n");
        assert!(msg.contains("unknown field `full-state`"), "got: {msg}");
    }

    #[test]
    fn test_log_rejects_field_collision() {
        let msg = load_log_error("operation-field = \"name\"\n");
        assert!(msg.contains("collides with a field"), "got: {msg}");
    }

    #[test]
    fn test_log_rejects_same_operation_and_timestamp_field() {
        let msg = load_log_error("operation-field = \"timestamp\"\n");
        assert!(msg.contains("are both 'timestamp'"), "got: {msg}");
    }

    #[test]
    fn test_log_rejects_unknown_exclude() {
        let msg = load_log_error("exclude = [\"missing\"]\n");
        assert!(msg.contains("unknown field 'missing'"), "got: {msg}");
    }

    #[test]
    fn test_log_rejects_primary_key_exclude() {
        let msg = load_log_error("exclude = [\"id\"]\n");
        assert!(msg.contains("primary-key field 'id'"), "got: {msg}");
    }

    #[test]
    fn test_log_rejects_duplicate_exclude() {
        let msg = load_log_error("exclude = [\"email\", \"email\"]\n");
        assert!(msg.contains("more than once"), "got: {msg}");
    }

    #[test]
    fn test_log_rejects_table_name_of_declared_table() {
        let msg = format!(
            "{:#}",
            load_with_log("table = \"users\"\nid-field = \"eventid\"\n").unwrap_err()
        );
        assert!(msg.contains("also declared as a table"), "got: {msg}");
    }

    #[test]
    fn test_log_rejects_table_shared_by_two_tables() {
        let dir = tempfile::tempdir().unwrap();
        let toml_input = r#"
[tables.users]
fields = [{ name = "id", type = "NUMBER", primary-key = true }]

[tables.users.log]
table = "changes"
id-field = "eventid"

[tables.groups]
fields = [{ name = "id", type = "NUMBER", primary-key = true }]

[tables.groups.log]
table = "changes"
id-field = "eventid"
"#;
        fs::write(dir.path().join("config.toml"), toml_input).unwrap();
        let msg = format!("{:#}", Config::load(dir.path()).unwrap_err());
        assert!(msg.contains("used by another table"), "got: {msg}");
    }

    /// Load a config with an `alerts` event stream. `extra` is appended to the
    /// stream's section and `prefix` is placed before it.
    fn load_with_events(extra: &str, prefix: &str) -> Result<Config> {
        let dir = tempfile::tempdir().unwrap();
        let toml_input = format!(
            r#"
{prefix}
[events.alerts]
id-field = "eventid"
fields = [
    {{ name = "message", type = "TEXT" }},
    {{ name = "severity", type = "NUMBER" }},
]
{extra}
"#
        );
        fs::write(dir.path().join("config.toml"), toml_input).unwrap();
        Config::load(dir.path())
    }

    fn load_events_error(extra: &str, prefix: &str) -> String {
        format!("{:#}", load_with_events(extra, prefix).unwrap_err())
    }

    #[test]
    fn test_events_accepted_without_tables() {
        let config = load_with_events("", "").unwrap();
        assert!(config.tables.is_empty());
        let event = &config.events["alerts"];
        assert_eq!(event.id_field, "eventid");
        assert_eq!(event.fields.len(), 2);
    }

    #[test]
    fn test_events_default_to_none() {
        let dir = tempfile::tempdir().unwrap();
        let toml_input = r#"
[tables.users]
fields = [{ name = "id", type = "NUMBER", primary-key = true }]
"#;
        fs::write(dir.path().join("config.toml"), toml_input).unwrap();
        let config = Config::load(dir.path()).unwrap();
        assert!(config.events.is_empty());
    }

    #[test]
    fn test_events_require_id_field() {
        let dir = tempfile::tempdir().unwrap();
        let toml_input = r#"
[events.alerts]
fields = [{ name = "message", type = "TEXT" }]
"#;
        fs::write(dir.path().join("config.toml"), toml_input).unwrap();
        let msg = format!("{:#}", Config::load(dir.path()).unwrap_err());
        assert!(msg.contains("missing field `id-field`"), "got: {msg}");
    }

    #[test]
    fn test_events_reject_primary_key() {
        let dir = tempfile::tempdir().unwrap();
        let toml_input = r#"
[events.alerts]
id-field = "eventid"
fields = [{ name = "message", type = "TEXT", primary-key = true }]
"#;
        fs::write(dir.path().join("config.toml"), toml_input).unwrap();
        let msg = format!("{:#}", Config::load(dir.path()).unwrap_err());
        assert!(msg.contains("events have no primary key"), "got: {msg}");
    }

    #[test]
    fn test_events_reject_csv() {
        let msg = load_events_error("[events.alerts.csv]\nsource = \"alerts.csv\"\n", "");
        assert!(msg.contains("unknown field `csv`"), "got: {msg}");
    }

    #[test]
    fn test_events_reject_id_field_collision() {
        let dir = tempfile::tempdir().unwrap();
        let toml_input = r#"
[events.alerts]
id-field = "message"
fields = [{ name = "message", type = "TEXT" }]
"#;
        fs::write(dir.path().join("config.toml"), toml_input).unwrap();
        let msg = format!("{:#}", Config::load(dir.path()).unwrap_err());
        assert!(msg.contains("collides with a field"), "got: {msg}");
    }

    #[test]
    fn test_events_reject_table_name_collision() {
        let table = r#"
[tables.alerts]
fields = [{ name = "id", type = "NUMBER", primary-key = true }]
"#;
        let msg = load_events_error("", table);
        assert!(msg.contains("also declared as a table"), "got: {msg}");
    }
}
