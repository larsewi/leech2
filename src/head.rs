use std::path::Path;

use anyhow::{Context, Result};

use crate::storage;
use crate::utils::GENESIS_HASH;

const HEAD_FILE: &str = "HEAD";

pub fn load(work_dir: &Path, mode: u32) -> Result<String> {
    let hash = match storage::load(work_dir, HEAD_FILE, mode)? {
        Some(data) => {
            let text = String::from_utf8(data).context("HEAD file contains non-UTF-8 data")?;
            // Tolerate trailing whitespace from manual edits or differing
            // line endings; `head::store` writes the bare hash with no
            // newline.
            text.trim().to_string()
        }
        None => GENESIS_HASH.to_string(),
    };
    log::debug!("Current head is '{:.7}...'", hash);
    Ok(hash)
}

pub fn store(work_dir: &Path, hash: &str, mode: u32, dry_run: bool) -> Result<()> {
    storage::store(work_dir, HEAD_FILE, hash.as_bytes(), mode, dry_run)?;
    log::debug!("Updated head to '{:.7}...'", hash);
    Ok(())
}
