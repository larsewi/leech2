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
pub struct Config {
    #[serde(skip)]
    pub work_dir: PathBuf,
    #[serde(default)]
    pub compression: CompressionConfig,
    pub tables: HashMap<String, TableConfig>,
    pub truncate: Option<TruncateConfig>,
}

#[derive(Debug, Deserialize)]
pub struct FieldConfig {
    pub name: String,
    #[serde(rename = "type", default = "default_field_type")]
    pub field_type: String,
    #[serde(rename = "primary-key", default)]
    pub primary_key: bool,
    #[serde(default)]
    pub format: Option<String>,
}

fn default_field_type() -> String {
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
    pub fn field_names(&self) -> Vec<String> {
        self.fields.iter().map(|f| f.name.clone()).collect()
    }

    pub fn primary_key(&self) -> Vec<String> {
        self.fields
            .iter()
            .filter(|f| f.primary_key)
            .map(|f| f.name.clone())
            .collect()
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

        for (name, table) in &config.tables {
            let num_primary_keys = table
                .fields
                .iter()
                .filter(|field| field.primary_key)
                .count();
            if num_primary_keys == 0 {
                bail!(
                    "table '{}': at least one field must be marked as primary-key",
                    name
                );
            }

            let mut seen = HashSet::new();
            for field in &table.fields {
                if !seen.insert(&field.name) {
                    bail!(
                        "table '{}': found duplicate field name '{}'",
                        name,
                        field.name
                    );
                }
            }
        }

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
}
