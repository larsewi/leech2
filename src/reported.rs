use std::path::Path;

use anyhow::Result;

use crate::storage;

const REPORTED_FILE: &str = "REPORTED";

pub fn load(work_dir: &Path, mode: u32) -> Result<Option<String>> {
    match storage::load(work_dir, REPORTED_FILE, mode)? {
        Some(data) => {
            let hash = String::from_utf8(data)?.trim().to_string();
            log::info!("Reported hash is '{:.7}...'", hash);
            Ok(Some(hash))
        }
        None => {
            log::debug!("No REPORTED file found");
            Ok(None)
        }
    }
}

pub fn save(work_dir: &Path, hash: &str, mode: u32, dry_run: bool) -> Result<()> {
    storage::store(work_dir, REPORTED_FILE, hash.as_bytes(), mode, dry_run)?;
    log::info!("Updated reported to '{:.7}...'", hash);
    Ok(())
}

pub fn remove(work_dir: &Path, mode: u32, dry_run: bool) -> Result<()> {
    storage::remove(work_dir, REPORTED_FILE, mode, dry_run)?;
    log::info!("Removed REPORTED file");
    Ok(())
}
