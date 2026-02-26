use std::fs::{self, File};
use std::io::{Read, Write};
use std::path::Path;

use fs2::FileExt;

use crate::config;

/// Acquires a lock on a separate `.<name>.lock` file for inter-process synchronization.
/// Returns the lock file handle; the lock is released when the handle is dropped.
fn acquire_lock(
    dir: &Path,
    name: &str,
    exclusive: bool,
) -> Result<File, Box<dyn std::error::Error>> {
    let lock_path = dir.join(format!(".{}.lock", name));
    let lock_file = File::create(&lock_path)
        .map_err(|e| format!("Failed to open lock file '{}': {}", lock_path.display(), e))?;
    if exclusive {
        lock_file.lock_exclusive()
    } else {
        lock_file.lock_shared()
    }
    .map_err(|e| format!("Failed to acquire lock on '{}': {}", lock_path.display(), e))?;
    Ok(lock_file)
}

/// Saves data to a file in the work directory using a separate lock file and atomic rename.
pub fn save(name: &str, data: &[u8]) -> Result<(), Box<dyn std::error::Error>> {
    let work_dir = &config::Config::get()?.work_dir;
    fs::create_dir_all(work_dir).map_err(|e| {
        format!(
            "Failed to create work directory '{}': {}",
            work_dir.display(),
            e
        )
    })?;

    let _lock = acquire_lock(work_dir, name, true)?;

    // Write to temp file, then atomic rename for crash safety.
    let tmp_path = work_dir.join(format!("{}.tmp", name));
    let path = work_dir.join(name);

    File::create(&tmp_path)
        .map_err(|e| format!("Failed to create temp file '{}': {}", tmp_path.display(), e))?
        .write_all(data)
        .map_err(|e| format!("Failed to write to '{}': {}", tmp_path.display(), e))?;
    fs::rename(&tmp_path, &path).map_err(|e| {
        format!(
            "Failed to rename '{}' to '{}': {}",
            tmp_path.display(),
            path.display(),
            e
        )
    })?;

    // _lock dropped here, releasing exclusive lock.
    log::debug!("Stored {} bytes to '{}'", data.len(), path.display());
    Ok(())
}

/// Removes a file from the work directory using an exclusive lock.
pub fn remove(name: &str) -> Result<(), Box<dyn std::error::Error>> {
    let work_dir = &config::Config::get()?.work_dir;
    let path = work_dir.join(name);

    if !path.exists() {
        log::debug!(
            "File '{}' does not exist, nothing to remove",
            path.display()
        );
        return Ok(());
    }

    let _lock = acquire_lock(work_dir, name, true)?;

    fs::remove_file(&path)
        .map_err(|e| format!("Failed to remove file '{}': {}", path.display(), e))?;

    // Best-effort cleanup of the lock file after removing the data file.
    let lock_path = work_dir.join(format!(".{}.lock", name));
    // _lock is dropped here first, then we try to clean up
    drop(_lock);
    let _ = fs::remove_file(&lock_path);

    log::debug!("Removed '{}'", path.display());
    Ok(())
}

/// Loads data from a file in the work directory with a shared lock.
pub fn load(name: &str) -> Result<Option<Vec<u8>>, Box<dyn std::error::Error>> {
    let path = config::Config::get()?.work_dir.join(name);
    if !path.exists() {
        log::debug!("File '{}' does not exist", path.display());
        return Ok(None);
    }

    let work_dir = path.parent().unwrap();
    let _lock = acquire_lock(work_dir, name, false)?;

    let mut data = Vec::new();
    File::open(&path)
        .map_err(|e| format!("Failed to open file '{}': {}", path.display(), e))?
        .read_to_end(&mut data)
        .map_err(|e| format!("Failed to read from '{}': {}", path.display(), e))?;

    // _lock dropped here, releasing shared lock.
    log::debug!("Loaded {} bytes from '{}'", data.len(), path.display());
    Ok(Some(data))
}
