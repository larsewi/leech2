use std::fs;
use std::io::Write;

use crate::config;

pub fn read_head() -> Result<String, String> {
    let head_path = config::get_work_dir()?.join("HEAD");
    Ok(fs::read_to_string(&head_path)
        .map(|s| s.trim().to_string())
        .unwrap_or_else(|_| "0".repeat(40)))
}

pub fn ensure_work_dir() -> Result<(), String> {
    fs::create_dir_all(config::get_work_dir()?).map_err(|e| e.to_string())
}

pub fn write_block(hash: &str, data: &[u8]) -> Result<(), String> {
    let path = config::get_work_dir()?.join(hash);
    let mut file = fs::File::create(&path).map_err(|e| e.to_string())?;
    file.write_all(data).map_err(|e| e.to_string())
}

pub fn write_head(hash: &str) -> Result<(), String> {
    let head_path = config::get_work_dir()?.join("HEAD");
    let mut file = fs::File::create(&head_path).map_err(|e| e.to_string())?;
    file.write_all(hash.as_bytes()).map_err(|e| e.to_string())
}
