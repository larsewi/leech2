use std::path::Path;

use anyhow::Result;

use crate::storage;
use crate::utils::GENESIS_HASH;

const HEAD_FILE: &str = "HEAD";

pub fn load(work_dir: &Path) -> Result<String> {
    let hash = match storage::load(work_dir, HEAD_FILE)? {
        Some(data) => String::from_utf8(data)?.trim().to_string(),
        None => GENESIS_HASH.to_string(),
    };
    log::info!("Current head is '{:.7}...'", hash);
    Ok(hash)
}

pub fn store(work_dir: &Path, hash: &str) -> Result<()> {
    storage::store(work_dir, HEAD_FILE, hash.as_bytes())?;
    log::info!("Updated head to '{:.7}...'", hash);
    Ok(())
}
