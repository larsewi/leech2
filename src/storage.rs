use std::fs::{self, File};
use std::io::{Read, Write};
use std::path::Path;

use anyhow::{Context, Result};
use fs2::FileExt;

/// Acquires a lock on a separate `.<name>.lock` file for inter-process synchronization.
/// Returns the lock file handle; the lock is released when the handle is dropped.
fn acquire_lock(dir: &Path, name: &str, exclusive: bool) -> Result<File> {
    let lock_path = dir.join(format!(".{}.lock", name));
    let lock_file = File::create(&lock_path)
        .with_context(|| format!("Failed to open lock file '{}'", lock_path.display()))?;
    if exclusive {
        lock_file.lock_exclusive()
    } else {
        lock_file.lock_shared()
    }
    .with_context(|| format!("Failed to acquire lock on '{}'", lock_path.display()))?;
    Ok(lock_file)
}

/// Saves data to a file in the work directory using a separate lock file and atomic rename.
pub fn store(work_dir: &Path, name: &str, data: &[u8]) -> Result<()> {
    fs::create_dir_all(work_dir)
        .with_context(|| format!("Failed to create work directory '{}'", work_dir.display()))?;

    let _lock = acquire_lock(work_dir, name, true)?;

    // Write to temp file, then atomic rename for crash safety.
    let tmp_path = work_dir.join(format!("{}.tmp", name));
    let path = work_dir.join(name);

    File::create(&tmp_path)
        .with_context(|| format!("Failed to create temp file '{}'", tmp_path.display()))?
        .write_all(data)
        .with_context(|| format!("Failed to write to '{}'", tmp_path.display()))?;
    fs::rename(&tmp_path, &path).with_context(|| {
        format!(
            "Failed to rename '{}' to '{}'",
            tmp_path.display(),
            path.display()
        )
    })?;

    // _lock dropped here, releasing exclusive lock.
    log::debug!("Stored {} bytes to '{}'", data.len(), path.display());
    Ok(())
}

/// Removes a file from the work directory using an exclusive lock.
pub fn remove(work_dir: &Path, name: &str) -> Result<()> {
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
        .with_context(|| format!("Failed to remove file '{}'", path.display()))?;

    // Best-effort cleanup of the lock file after removing the data file.
    let lock_path = work_dir.join(format!(".{}.lock", name));
    // _lock is dropped here first, then we try to clean up
    drop(_lock);
    let _ = fs::remove_file(&lock_path);

    log::debug!("Removed '{}'", path.display());
    Ok(())
}

/// Loads data from a file in the work directory with a shared lock.
pub fn load(work_dir: &Path, name: &str) -> Result<Option<Vec<u8>>> {
    let path = work_dir.join(name);
    if !path.exists() {
        log::debug!("File '{}' does not exist", path.display());
        return Ok(None);
    }

    let _lock = acquire_lock(work_dir, name, false)?;

    let mut data = Vec::new();
    File::open(&path)
        .with_context(|| format!("Failed to open file '{}'", path.display()))?
        .read_to_end(&mut data)
        .with_context(|| format!("Failed to read from '{}'", path.display()))?;

    // _lock dropped here, releasing shared lock.
    log::debug!("Loaded {} bytes from '{}'", data.len(), path.display());
    Ok(Some(data))
}
