use std::fs::{self, File};
use std::io::{Read, Write};

use fs2::FileExt;
use prost::Message;

use crate::block::Block;
use crate::config;

/// Saves data to a file in the work directory with an exclusive lock.
pub fn save(name: &str, data: &[u8]) -> Result<(), Box<dyn std::error::Error>> {
    let work_dir = config::get_work_dir()?;
    fs::create_dir_all(&work_dir)
        .map_err(|e| format!("Failed to create work directory '{}': {}", work_dir.display(), e))?;

    let path = work_dir.join(name);
    log::debug!("Storing data to file '{}'...", path.display());

    let file = File::create(&path)
        .map_err(|e| format!("Failed to create file '{}': {}", path.display(), e))?;
    file.lock_exclusive()
        .map_err(|e| format!("Failed to acquire exclusive lock on '{}': {}", path.display(), e))?;

    (&file)
        .write_all(data)
        .map_err(|e| format!("Failed to write to '{}': {}", path.display(), e))?;

    file.unlock()
        .map_err(|e| format!("Failed to release lock on '{}': {}", path.display(), e))?;

    log::debug!("Stored {} bytes to '{}'", data.len(), path.display());
    Ok(())
}

/// Loads data from a file in the work directory with a shared lock.
pub fn load(name: &str) -> Result<Option<Vec<u8>>, Box<dyn std::error::Error>> {
    let path = config::get_work_dir()?.join(name);
    log::debug!("Loading data from file '{}'...", path.display());

    if !path.exists() {
        log::debug!("File '{}' does not exist", path.display());
        return Ok(None);
    }

    let file = File::open(&path)
        .map_err(|e| format!("Failed to open file '{}': {}", path.display(), e))?;
    file.lock_shared()
        .map_err(|e| format!("Failed to acquire shared lock on '{}': {}", path.display(), e))?;

    let mut data = Vec::new();
    (&file)
        .read_to_end(&mut data)
        .map_err(|e| format!("Failed to read from '{}': {}", path.display(), e))?;

    file.unlock()
        .map_err(|e| format!("Failed to release lock on '{}': {}", path.display(), e))?;

    log::debug!("Loaded {} bytes from '{}'", data.len(), path.display());
    Ok(Some(data))
}

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
