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
    pub keys: KeysConfig,
}

#[derive(Debug, Deserialize)]
pub struct KeysConfig {
    pub primary: Vec<String>,
    pub subsidiary: Vec<String>,
}

static CONFIG: OnceLock<Config> = OnceLock::new();

pub fn get_work_dir() -> Result<&'static Path, String> {
    CONFIG
        .get()
        .map(|c| c.work_dir.as_path())
        .ok_or_else(|| "config not initialized".to_string())
}

pub fn get_config() -> Result<&'static Config, String> {
    CONFIG
        .get()
        .ok_or_else(|| "config not initialized".to_string())
}

fn load_config(work_dir: &Path) -> Result<Config, String> {
    let config_path = work_dir.join("config.toml");
    let content = fs::read_to_string(&config_path)
        .map_err(|e| format!("failed to read config file: {}", e))?;
    let mut config: Config =
        toml::from_str(&content).map_err(|e| format!("failed to parse config: {}", e))?;
    config.work_dir = work_dir.to_path_buf();
    Ok(config)
}

pub fn init_impl(path: &str) -> Result<(), String> {
    env_logger::init();

    let config = load_config(Path::new(path))?;
    log::debug!("init: work directory '{}'", config.work_dir.display());
    log::info!("init: loaded config with {} tables", config.tables.len());
    CONFIG
        .set(config)
        .map_err(|_| "config already initialized".to_string())?;

    Ok(())
}
