use std::path::Path;

use anyhow::Result;

use crate::storage;
use crate::utils::GENESIS_HASH;

const REPORTED_FILE: &str = "REPORTED";

pub fn load(work_dir: &Path) -> Result<Option<String>> {
    match storage::load(work_dir, REPORTED_FILE)? {
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

/// The last reported block hash, or the genesis hash if nothing has been
/// reported yet. This is where a patch starts when no reference is given.
pub fn load_or_genesis(work_dir: &Path) -> Result<String> {
    let hash = load(work_dir)?.unwrap_or_else(|| GENESIS_HASH.to_string());
    Ok(hash)
}

pub fn save(work_dir: &Path, hash: &str, mode: u32, dry_run: bool) -> Result<()> {
    let contents = format!("{}\n", hash);
    storage::store(work_dir, REPORTED_FILE, contents.as_bytes(), mode, dry_run)?;
    log::info!("Updated reported to '{:.7}...'", hash);
    Ok(())
}

pub fn remove(work_dir: &Path, dry_run: bool) -> Result<()> {
    storage::remove(work_dir, REPORTED_FILE, dry_run)?;
    log::info!("Removed REPORTED file");
    Ok(())
}
