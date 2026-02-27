use std::path::Path;

use anyhow::Result;

use crate::storage;

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

pub fn save(work_dir: &Path, hash: &str) -> Result<()> {
    storage::store(work_dir, REPORTED_FILE, hash.as_bytes())?;
    log::info!("Updated reported to '{:.7}...'", hash);
    Ok(())
}
