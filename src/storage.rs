use std::fs;
use std::io::Write;

use crate::config;

pub fn read_head() -> Result<String, String> {
    let path = config::get_work_dir()?.join("HEAD");
    log::debug!("Reading head from file '{}'", path.display());
    let hash = fs::read_to_string(&path)
        .map(|s| s.trim().to_string())
        .unwrap_or_else(|_| "0".repeat(40));
    log::info!("Current head is '{:.7}...'", hash,);
    Ok(hash)
}

pub fn ensure_work_dir() -> Result<(), String> {
    fs::create_dir_all(config::get_work_dir()?).map_err(|e| e.to_string())
}

pub fn write_block(hash: &str, data: &[u8]) -> Result<(), String> {
    let path = config::get_work_dir()?.join(hash);
    log::debug!("Writing block to file '{}'...", path.display());
    let mut file = fs::File::create(&path).map_err(|e| e.to_string())?;
    let ret = file.write_all(data).map_err(|e| e.to_string());
    log::info!("Stored block '{:.7}...'", hash);
    ret
}

pub fn write_head(hash: &str) -> Result<(), String> {
    let path = config::get_work_dir()?.join("HEAD");
    log::debug!("Writing head to file '{}'...", path.display());
    let mut file = fs::File::create(&path).map_err(|e| e.to_string())?;
    let ret = file.write_all(hash.as_bytes()).map_err(|e| e.to_string());
    log::info!("Updated head to '{:.7}...'", hash);
    ret
}
