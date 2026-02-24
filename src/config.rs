use serde::Deserialize;
use std::collections::{HashMap, HashSet};
use std::fs;
use std::path::{Path, PathBuf};
use std::sync::OnceLock;

#[derive(Debug, Deserialize)]
pub struct Config {
    #[serde(skip)]
    pub work_dir: PathBuf,
    #[serde(default = "default_compression")]
    pub compression: bool,
    #[serde(rename = "compression-level", default)]
    pub compression_level: i32,
    pub tables: HashMap<String, TableConfig>,
}

fn default_compression() -> bool {
    true
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

    pub fn field_types(&self) -> Vec<String> {
        self.fields.iter().map(|f| f.field_type.clone()).collect()
    }

    pub fn field_formats(&self) -> Vec<Option<String>> {
        self.fields.iter().map(|f| f.format.clone()).collect()
    }
}

static CONFIG: OnceLock<Config> = OnceLock::new();

impl Config {
    pub fn get() -> Result<&'static Config, String> {
        CONFIG
            .get()
            .ok_or_else(|| "config not initialized".to_string())
    }

    pub fn init(work_dir: &Path) -> Result<(), String> {
        let path = work_dir.join("config.toml");
        log::debug!("Parsing config from file '{}'...", path.display());
        let content =
            fs::read_to_string(&path).map_err(|e| format!("failed to read config file: {}", e))?;
        let mut config: Config =
            toml::from_str(&content).map_err(|e| format!("failed to parse config: {}", e))?;
        config.work_dir = work_dir.to_path_buf();

        for (name, table) in &config.tables {
            let pk_count = table.fields.iter().filter(|f| f.primary_key).count();
            if pk_count == 0 {
                return Err(format!(
                    "table '{}': at least one field must be marked as primary-key",
                    name
                ));
            }

            let mut seen = HashSet::new();
            for field in &table.fields {
                if !seen.insert(&field.name) {
                    return Err(format!(
                        "table '{}': duplicate field name '{}'",
                        name, field.name
                    ));
                }
            }
        }

        log::info!("Initialized config with {} tables", config.tables.len());
        CONFIG
            .set(config)
            .map_err(|_| "config already initialized".to_string())
    }
}
