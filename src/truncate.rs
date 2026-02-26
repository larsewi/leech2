use std::collections::HashSet;
use std::path::Path;
use std::time::SystemTime;

use crate::block::Block;
use crate::config::{Config, parse_duration};
use crate::head;
use crate::reported;
use crate::storage;
use crate::utils::GENESIS_HASH;

struct ChainEntry {
    hash: String,
    created: Option<SystemTime>,
}

/// Returns `(block_hashes, stale_lock_files)` by scanning the work directory.
/// Block hashes are 40-hex-char filenames. Stale lock files are `.<40-hex>.lock`
/// files whose corresponding block is not on disk.
fn scan_work_dir(
    work_dir: &Path,
) -> Result<(HashSet<String>, Vec<String>), Box<dyn std::error::Error>> {
    let mut blocks = HashSet::new();
    let mut lock_files = Vec::new();

    for entry in std::fs::read_dir(work_dir)? {
        let entry = entry?;
        let name = entry.file_name();
        let Some(name) = name.to_str() else {
            continue;
        };
        if name.len() == 40 && name.chars().all(|c| c.is_ascii_hexdigit()) {
            blocks.insert(name.to_string());
        } else if let Some(base) = name.strip_suffix(".lock")
            && let Some(base) = base.strip_prefix(".")
            && base.len() == 40
            && base.chars().all(|c| c.is_ascii_hexdigit())
        {
            lock_files.push(name.to_string());
        }
    }

    // Keep only lock files whose block is not on disk
    lock_files.retain(|name| {
        let base = name.strip_suffix(".lock").and_then(|s| s.strip_prefix("."));
        match base {
            Some(base) => !blocks.contains(base),
            None => false,
        }
    });

    Ok((blocks, lock_files))
}

pub fn run(config: &Config) -> Result<(), Box<dyn std::error::Error>> {
    let work_dir = &config.work_dir;
    let head_hash = head::load(work_dir)?;

    // Walk chain from HEAD → GENESIS, building ordered list and reachable set together
    let mut chain = Vec::new();
    let mut reachable = HashSet::new();

    let mut current_hash = head_hash.clone();
    while current_hash != GENESIS_HASH {
        let block = match Block::load(work_dir, &current_hash) {
            Ok(b) => b,
            Err(_) => {
                // Block was previously truncated — end of reachable chain
                log::debug!(
                    "Block '{:.7}...' not found (previously truncated), stopping chain walk",
                    current_hash
                );
                break;
            }
        };
        let created = block.created.map(|ts| {
            SystemTime::UNIX_EPOCH + std::time::Duration::new(ts.seconds as u64, ts.nanos as u32)
        });
        let parent = block.parent.clone();
        reachable.insert(current_hash.clone());
        chain.push(ChainEntry {
            hash: current_hash,
            created,
        });
        current_hash = parent;
    }

    // Orphan removal: delete block files on disk not in reachable set,
    // and stale lock files whose block no longer exists
    let (on_disk, stale_locks) = scan_work_dir(work_dir)?;
    for hash in &on_disk {
        if !reachable.contains(hash) {
            log::info!("Removing orphaned block '{:.7}...'", hash);
            storage::remove(work_dir, hash)?;
        }
    }

    for lock_file in &stale_locks {
        log::info!("Removing stale lock file '{}'", lock_file);
        let _ = std::fs::remove_file(work_dir.join(lock_file));
    }

    if chain.is_empty() {
        return Ok(());
    }

    // Precompute rule parameters
    let reported_pos = match reported::load(work_dir)? {
        Some(ref hash) => chain.iter().position(|e| e.hash == *hash),
        None => None,
    };

    let max_blocks = config
        .truncate
        .as_ref()
        .and_then(|t| t.max_blocks)
        .map(|m| m as usize);
    let max_age_cutoff = match config.truncate.as_ref().and_then(|t| t.max_age.as_ref()) {
        Some(s) => Some(SystemTime::now() - parse_duration(s)?),
        None => None,
    };

    // Single pass: check all removal rules for each block
    let mut removed = 0u32;
    for (i, entry) in chain.iter().enumerate() {
        if i == 0 {
            continue; // Never delete HEAD
        }

        let should_remove = reported_pos.is_some_and(|pos| i > pos)
            || max_blocks.is_some_and(|max| i >= max)
            || max_age_cutoff.is_some_and(|cutoff| entry.created.is_some_and(|c| c < cutoff));

        if should_remove {
            log::info!("Truncating block '{:.7}...'", entry.hash);
            storage::remove(work_dir, &entry.hash)?;
            removed += 1;
        }
    }

    if removed > 0 {
        log::info!("Truncated {} block(s)", removed);
    }

    Ok(())
}
