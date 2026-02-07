use serde::Deserialize;
use std::collections::HashMap;
use std::fs;
use std::path::{Path, PathBuf};
use std::sync::OnceLock;

#[derive(Debug, Deserialize)]
pub struct Config {
    #[serde(skip)]
    pub work_dir: PathBuf,
    pub tables: HashMap<String, TableConfig>,
}

#[derive(Debug, Deserialize)]
pub struct TableConfig {
    pub source: String,
    #[serde(rename = "field-names")]
    pub field_names: Vec<String>,
    #[serde(rename = "primary-key")]
    pub primary_key: Vec<String>,
}

static CONFIG: OnceLock<Config> = OnceLock::new();

impl Config {
    pub fn get_work_dir() -> Result<&'static Path, String> {
        CONFIG
            .get()
            .map(|c| c.work_dir.as_path())
            .ok_or_else(|| "config not initialized".to_string())
    }

    pub fn get_tables() -> Result<&'static HashMap<String, TableConfig>, String> {
        CONFIG
            .get()
            .map(|c| &c.tables)
            .ok_or_else(|| "config not initialized".to_string())
    }

    pub fn load(work_dir: &Path) -> Result<(), String> {
        let path = work_dir.join("config.toml");
        log::debug!("Parsing config from file '{}'...", path.display());
        let content =
            fs::read_to_string(&path).map_err(|e| format!("failed to read config file: {}", e))?;
        let mut config: Config =
            toml::from_str(&content).map_err(|e| format!("failed to parse config: {}", e))?;
        config.work_dir = work_dir.to_path_buf();
        log::debug!("{:#?}", config);
        log::info!("Initialized config with {} tables", config.tables.len());
        CONFIG
            .set(config)
            .map_err(|_| "config already initialized".to_string())
    }
}
