use std::fs::{self, File, OpenOptions};
use std::io::{Read, Write};

use fs2::FileExt;

use crate::config;

/// Saves data to a file in the work directory with an exclusive lock.
pub fn save(name: &str, data: &[u8]) -> Result<(), Box<dyn std::error::Error>> {
    let work_dir = &config::Config::get()?.work_dir;
    fs::create_dir_all(work_dir).map_err(|e| {
        format!(
            "Failed to create work directory '{}': {}",
            work_dir.display(),
            e
        )
    })?;

    let path = work_dir.join(name);
    #[allow(clippy::suspicious_open_options)]
    let file = OpenOptions::new()
        .write(true)
        .create(true)
        .open(&path)
        .map_err(|e| format!("Failed to open file '{}': {}", path.display(), e))?;
    file.lock_exclusive().map_err(|e| {
        format!(
            "Failed to acquire exclusive lock on '{}': {}",
            path.display(),
            e
        )
    })?;

    file.set_len(0)
        .map_err(|e| format!("Failed to truncate '{}': {}", path.display(), e))?;
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
    let path = config::Config::get()?.work_dir.join(name);
    if !path.exists() {
        log::debug!("File '{}' does not exist", path.display());
        return Ok(None);
    }

    let file = File::open(&path)
        .map_err(|e| format!("Failed to open file '{}': {}", path.display(), e))?;
    file.lock_shared().map_err(|e| {
        format!(
            "Failed to acquire shared lock on '{}': {}",
            path.display(),
            e
        )
    })?;

    let mut data = Vec::new();
    (&file)
        .read_to_end(&mut data)
        .map_err(|e| format!("Failed to read from '{}': {}", path.display(), e))?;

    file.unlock()
        .map_err(|e| format!("Failed to release lock on '{}': {}", path.display(), e))?;

    log::debug!("Loaded {} bytes from '{}'", data.len(), path.display());
    Ok(Some(data))
}
