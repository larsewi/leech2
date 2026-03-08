use serde::Deserialize;
use std::collections::{HashMap, HashSet};
use std::fs;
use std::path::{Path, PathBuf};
use std::time::Duration;

use anyhow::{Context, Result, bail};

enum ConfigFormat {
    Toml,
    Json,
}

#[derive(Debug, Deserialize)]
pub struct TruncateConfig {
    #[serde(rename = "max-blocks")]
    pub max_blocks: Option<u32>,
    #[serde(rename = "max-age")]
    pub max_age: Option<String>,
}

#[derive(Debug, Deserialize)]
#[serde(default)]
pub struct CompressionConfig {
    pub enable: bool,
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

#[derive(Debug, Deserialize)]
pub struct InjectedFieldConfig {
    pub name: String,
    #[serde(rename = "type", default = "default_sql_type")]
    pub sql_type: String,
    pub value: String,
}

impl InjectedFieldConfig {
    fn validate(&self) -> Result<()> {
        crate::sql::SqlType::from_config(&self.sql_type).context("invalid type")?;
        Ok(())
    }
}

#[derive(Debug, Deserialize)]
pub struct Config {
    #[serde(skip)]
    pub work_dir: PathBuf,
    #[serde(default, rename = "injected-fields")]
    pub injected_fields: Vec<InjectedFieldConfig>,
    #[serde(default)]
    pub compression: CompressionConfig,
    pub tables: HashMap<String, TableConfig>,
    pub truncate: Option<TruncateConfig>,
}

#[derive(Debug, Deserialize)]
pub struct FieldConfig {
    pub name: String,
    #[serde(rename = "type", default = "default_sql_type")]
    pub sql_type: String,
    #[serde(rename = "primary-key", default)]
    pub primary_key: bool,
    #[serde(default)]
    pub null: Option<String>,
}

fn default_sql_type() -> String {
    "TEXT".to_string()
}

#[derive(Debug, Deserialize)]
pub struct TableConfig {
    pub source: String,
    #[serde(default)]
    pub header: bool,
    pub fields: Vec<FieldConfig>,
}

impl TableConfig {
    fn validate(&self) -> Result<()> {
        let num_primary_keys = self.fields.iter().filter(|field| field.primary_key).count();
        if num_primary_keys == 0 {
            bail!("at least one field must be marked as primary-key");
        }

        let mut seen = HashSet::new();
        for field in &self.fields {
            if !seen.insert(&field.name) {
                bail!("found duplicate field name '{}'", field.name);
            }
            if field.primary_key && field.null.is_some() {
                bail!(
                    "primary-key field '{}' must not have a null sentinel",
                    field.name
                );
            }
        }

        Ok(())
    }

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

    /// Return field names in PK-first order: primary key fields first (in
    /// declaration order), then subsidiary fields (in declaration order).
    /// This matches the ordering used by `Table::load()` when building the
    /// in-memory table stored in the STATE file.
    pub fn ordered_field_names(&self) -> Vec<String> {
        let primary_key = self.primary_key();
        let mut names = primary_key.clone();
        for field in &self.fields {
            if !primary_key.contains(&field.name) {
                names.push(field.name.clone());
            }
        }
        names
    }

    /// Compute a SHA-1 hash over this table's SQL-affecting fields.
    /// Fields are sorted alphabetically by name for order independence.
    /// The hash covers: field name, sql_type, primary_key flag, and null sentinel.
    pub fn field_hash(&self) -> String {
        let mut sorted_fields: Vec<&FieldConfig> = self.fields.iter().collect();
        sorted_fields.sort_by(|a, b| a.name.cmp(&b.name));

        let mut data = Vec::new();
        for field in sorted_fields {
            data.extend_from_slice(field.name.as_bytes());
            data.push(0);
            data.extend_from_slice(field.sql_type.as_bytes());
            data.push(0);
            data.push(u8::from(field.primary_key));
            data.push(0);
            if let Some(ref sentinel) = field.null {
                data.push(1);
                data.extend_from_slice(sentinel.as_bytes());
            } else {
                data.push(0);
            }
            data.push(0);
        }

        crate::utils::compute_hash(&data)
    }
}

impl Config {
    pub fn load(work_dir: &Path) -> Result<Config> {
        let toml_path = work_dir.join("config.toml");
        let json_path = work_dir.join("config.json");

        let (path, format) = if toml_path.exists() {
            (toml_path, ConfigFormat::Toml)
        } else if json_path.exists() {
            (json_path, ConfigFormat::Json)
        } else {
            bail!("no config file found (expected config.toml or config.json)");
        };

        log::debug!("Parsing config from file '{}'...", path.display());
        let content = fs::read_to_string(&path).context("failed to read config file")?;
        let mut config: Config = match format {
            ConfigFormat::Toml => toml::from_str(&content).context("failed to parse config")?,
            ConfigFormat::Json => {
                serde_json::from_str(&content).context("failed to parse config")?
            }
        };
        config.work_dir = work_dir.to_path_buf();

        // Validate each table: at least one primary key, no duplicate field
        // names, and no null sentinels on primary-key fields.
        for (name, table) in &config.tables {
            table
                .validate()
                .with_context(|| format!("table '{}'", name))?;
        }

        // Validate injected fields: no duplicate names across the list,
        // and each field must have a valid SQL type.
        let mut injected_names = HashSet::new();
        for (index, field) in config.injected_fields.iter().enumerate() {
            if !injected_names.insert(&field.name) {
                bail!(
                    "injected-fields[{}]: duplicate field name '{}'",
                    index,
                    field.name
                );
            }
            field
                .validate()
                .with_context(|| format!("injected-fields[{}]", index))?;
        }

        // Validate truncation: max-blocks >= 1 and max-age is a valid
        // duration string (e.g. "30s", "12h", "7d").
        if let Some(ref truncate) = config.truncate {
            if let Some(max_blocks) = truncate.max_blocks
                && max_blocks < 1
            {
                bail!("truncate.max-blocks must be >= 1");
            }
            if let Some(ref max_age) = truncate.max_age {
                parse_duration(max_age).context("truncate.max-age")?;
            }
        }

        log::info!("Initialized config with {} tables", config.tables.len());
        Ok(config)
    }
}

const SECONDS_PER_MINUTE: u64 = 60;
const SECONDS_PER_HOUR: u64 = 60 * SECONDS_PER_MINUTE;
const SECONDS_PER_DAY: u64 = 24 * SECONDS_PER_HOUR;
const SECONDS_PER_WEEK: u64 = 7 * SECONDS_PER_DAY;

/// Parse a duration string like "30s", "12h", "7d", "2w" into a `Duration`.
/// Supported suffixes: `s` (seconds), `m` (minutes), `h` (hours), `d` (days), `w` (weeks).
pub fn parse_duration(s: &str) -> Result<Duration> {
    if s.is_empty() {
        bail!("empty duration string");
    }

    let (number, suffix) = s.split_at(s.len() - 1);
    let value: u64 = number
        .parse()
        .map_err(|_| anyhow::anyhow!("invalid duration '{}'", s))?;

    let seconds = match suffix {
        "s" => value,
        "m" => value * SECONDS_PER_MINUTE,
        "h" => value * SECONDS_PER_HOUR,
        "d" => value * SECONDS_PER_DAY,
        "w" => value * SECONDS_PER_WEEK,
        _ => bail!("invalid duration suffix '{}' in '{}'", suffix, s),
    };

    Ok(Duration::from_secs(seconds))
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_parse_duration_seconds() {
        assert_eq!(parse_duration("30s").unwrap(), Duration::from_secs(30));
    }

    #[test]
    fn test_parse_duration_minutes() {
        assert_eq!(parse_duration("5m").unwrap(), Duration::from_secs(300));
    }

    #[test]
    fn test_parse_duration_hours() {
        assert_eq!(parse_duration("12h").unwrap(), Duration::from_secs(43200));
    }

    #[test]
    fn test_parse_duration_days() {
        assert_eq!(parse_duration("7d").unwrap(), Duration::from_secs(604800));
    }

    #[test]
    fn test_parse_duration_weeks() {
        assert_eq!(parse_duration("2w").unwrap(), Duration::from_secs(1209600));
    }

    #[test]
    fn test_parse_duration_invalid_suffix() {
        assert!(parse_duration("10x").is_err());
    }

    #[test]
    fn test_parse_duration_invalid_number() {
        assert!(parse_duration("abcs").is_err());
    }

    #[test]
    fn test_parse_duration_empty() {
        assert!(parse_duration("").is_err());
    }

    fn make_field(
        name: &str,
        sql_type: &str,
        primary_key: bool,
        null: Option<&str>,
    ) -> FieldConfig {
        FieldConfig {
            name: name.to_string(),
            sql_type: sql_type.to_string(),
            primary_key,
            null: null.map(|s| s.to_string()),
        }
    }

    fn make_table_config(fields: Vec<FieldConfig>) -> TableConfig {
        TableConfig {
            source: "test.csv".to_string(),
            header: false,
            fields,
        }
    }

    #[test]
    fn test_ordered_field_names() {
        let config = make_table_config(vec![
            make_field("name", "TEXT", false, None),
            make_field("id", "NUMBER", true, None),
            make_field("email", "TEXT", false, None),
        ]);
        assert_eq!(config.ordered_field_names(), vec!["id", "name", "email"]);
    }

    #[test]
    fn test_ordered_field_names_multiple_primary_keys() {
        let config = make_table_config(vec![
            make_field("value", "TEXT", false, None),
            make_field("pk_b", "TEXT", true, None),
            make_field("pk_a", "TEXT", true, None),
        ]);
        // PKs in declaration order, then subsidiaries
        assert_eq!(config.ordered_field_names(), vec!["pk_b", "pk_a", "value"]);
    }

    #[test]
    fn test_field_hash_deterministic() {
        let config = make_table_config(vec![
            make_field("id", "NUMBER", true, None),
            make_field("name", "TEXT", false, Some("")),
        ]);
        assert_eq!(config.field_hash(), config.field_hash());
    }

    #[test]
    fn test_field_hash_order_independent() {
        let config_a = make_table_config(vec![
            make_field("id", "NUMBER", true, None),
            make_field("name", "TEXT", false, None),
        ]);
        let config_b = make_table_config(vec![
            make_field("name", "TEXT", false, None),
            make_field("id", "NUMBER", true, None),
        ]);
        assert_eq!(config_a.field_hash(), config_b.field_hash());
    }

    #[test]
    fn test_field_hash_changes_on_type() {
        let config_a = make_table_config(vec![make_field("id", "NUMBER", true, None)]);
        let config_b = make_table_config(vec![make_field("id", "TEXT", true, None)]);
        assert_ne!(config_a.field_hash(), config_b.field_hash());
    }

    #[test]
    fn test_field_hash_changes_on_null() {
        let config_a = make_table_config(vec![
            make_field("id", "TEXT", true, None),
            make_field("val", "TEXT", false, None),
        ]);
        let config_b = make_table_config(vec![
            make_field("id", "TEXT", true, None),
            make_field("val", "TEXT", false, Some("")),
        ]);
        assert_ne!(config_a.field_hash(), config_b.field_hash());
    }

    #[test]
    fn test_field_hash_changes_on_primary_key() {
        let config_a = make_table_config(vec![
            make_field("id", "TEXT", true, None),
            make_field("name", "TEXT", false, None),
        ]);
        let config_b = make_table_config(vec![
            make_field("id", "TEXT", true, None),
            make_field("name", "TEXT", true, None),
        ]);
        assert_ne!(config_a.field_hash(), config_b.field_hash());
    }
}
