use std::fs;
use std::io::Write;

use prost::Message;

use crate::block::Block;
use crate::config;

pub fn read_block(hash: &str) -> Result<Block, Box<dyn std::error::Error>> {
    let path = config::get_work_dir()?.join(hash);
    log::debug!("Reading block from file '{}'", path.display());
    let data =
        fs::read(&path).map_err(|e| format!("Failed to read block '{}': {}", path.display(), e))?;
    let block = Block::decode(data.as_slice())
        .map_err(|e| format!("Failed to decode block '{:.7}...': {}", hash, e))?;
    log::info!("Loaded block '{:.7}...'", hash);
    Ok(block)
}

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
    let path = config::get_work_dir()?;
    fs::create_dir_all(&path).map_err(|e| {
        format!(
            "Failed to create work directory '{}': {}",
            path.display(),
            e
        )
    })
}

pub fn write_block(hash: &str, data: &[u8]) -> Result<(), String> {
    let path = config::get_work_dir()?.join(hash);
    log::debug!("Writing block to file '{}'...", path.display());
    let mut file = fs::File::create(&path)
        .map_err(|e| format!("Failed to create block file '{}': {}", path.display(), e))?;
    file.write_all(data)
        .map_err(|e| format!("Failed to write block '{}': {}", path.display(), e))?;
    log::info!("Stored block '{:.7}...'", hash);
    Ok(())
}

pub fn write_head(hash: &str) -> Result<(), String> {
    let path = config::get_work_dir()?.join("HEAD");
    log::debug!("Writing head to file '{}'...", path.display());
    let mut file = fs::File::create(&path)
        .map_err(|e| format!("Failed to create HEAD file '{}': {}", path.display(), e))?;
    file.write_all(hash.as_bytes())
        .map_err(|e| format!("Failed to write HEAD file '{}': {}", path.display(), e))?;
    log::info!("Updated head to '{:.7}...'", hash);
    Ok(())
}
