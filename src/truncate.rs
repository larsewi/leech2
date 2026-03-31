use std::collections::HashSet;
use std::path::Path;
use std::time::SystemTime;

use anyhow::Result;

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

/// Strips the leading `.` and trailing `.lock` from a lock file name,
/// returning the inner block hash (e.g. `".abc123.lock"` → `"abc123"`).
fn strip_lock_affixes(name: &str) -> Option<&str> {
    name.strip_prefix(".")?.strip_suffix(".lock")
}

/// Returns `true` if `s` is a 40-character hexadecimal string (i.e. a SHA-1 hash).
fn is_hex_hash(s: &str) -> bool {
    s.len() == 40 && s.chars().all(|c| c.is_ascii_hexdigit())
}

/// Returns `(block_hashes, stale_lock_files)` by scanning the work directory.
/// Block hashes are 40-hex-char filenames. Stale lock files are `.<40-hex>.lock`
/// files whose corresponding block is not on disk.
fn scan_work_dir(work_dir: &Path) -> Result<(HashSet<String>, Vec<String>)> {
    let mut blocks = HashSet::new();
    let mut lock_files = Vec::new();

    for entry in std::fs::read_dir(work_dir)? {
        let entry = entry?;
        let name = entry.file_name();
        let Some(name) = name.to_str() else {
            continue;
        };
        if is_hex_hash(name) {
            blocks.insert(name.to_string());
        } else if strip_lock_affixes(name).is_some_and(is_hex_hash) {
            lock_files.push(name.to_string());
        }
    }

    // Keep only lock files whose block is not on disk
    lock_files.retain(|name| {
        let base = strip_lock_affixes(name);
        match base {
            Some(base) => !blocks.contains(base),
            None => false,
        }
    });

    Ok((blocks, lock_files))
}

/// Walk the block chain from HEAD back towards GENESIS, returning an ordered
/// list of chain entries and the set of reachable block hashes.
fn walk_chain(work_dir: &Path, head_hash: &str) -> (Vec<ChainEntry>, HashSet<String>) {
    let mut chain = Vec::new();
    let mut reachable = HashSet::new();

    let mut current_hash = head_hash.to_string();
    while current_hash != GENESIS_HASH {
        let Ok(header) = Block::load_header(work_dir, &current_hash) else {
            // Reached end of chain
            log::trace!(
                "Block '{:.7}...' not found (previously truncated), stopping chain walk",
                current_hash
            );
            break;
        };
        let Some(created) = header.created else {
            log::warn!(
                "Block '{:.7}...' has no timestamp, stopping chain walk",
                current_hash
            );
            break;
        };
        let Ok(created) = SystemTime::try_from(created) else {
            log::warn!(
                "Block '{:.7}...' has invalid timestamp, stopping chain walk",
                current_hash
            );
            break;
        };
        reachable.insert(current_hash.clone());
        chain.push(ChainEntry {
            hash: current_hash,
            created: Some(created),
        });
        current_hash = header.parent;
    }

    (chain, reachable)
}

/// Remove orphaned blocks (not reachable from HEAD) and stale lock files
/// (whose corresponding block no longer exists on disk). This also cleans up
/// corrupt blocks, since `walk_chain` stops before adding them to the
/// reachable set.
fn remove_orphans(config: &Config, reachable: &HashSet<String>) -> Result<()> {
    let work_dir = &config.work_dir;
    let (on_disk, stale_locks) = scan_work_dir(work_dir)?;

    if config.truncate.remove_orphans {
        for hash in &on_disk {
            if !reachable.contains(hash) {
                log::info!("Removing orphaned block '{:.7}...'", hash);
                storage::remove(work_dir, hash)?;
            }
        }
    }

    for lock_file in &stale_locks {
        log::info!("Removing stale lock file '{}'", lock_file);
        if let Err(error) = std::fs::remove_file(work_dir.join(lock_file)) {
            log::warn!(
                "Failed to remove stale lock file '{}': {}",
                lock_file,
                error
            );
        }
    }

    Ok(())
}

/// Truncate blocks from the chain according to the configured rules
/// (max_blocks, max_age, truncate_reported). Never deletes HEAD.
fn truncate_chain(config: &Config, chain: &[ChainEntry]) -> Result<()> {
    let work_dir = &config.work_dir;

    let reported_pos = if config.truncate.truncate_reported {
        match reported::load(work_dir)? {
            Some(hash) => chain
                .iter()
                .position(|chain_entry| chain_entry.hash == hash),
            None => None,
        }
    } else {
        None
    };

    let max_blocks = config.truncate.max_blocks.map(|n| n as usize);
    let max_age_cutoff = match &config.truncate.max_age {
        Some(max_age) => Some(SystemTime::now() - parse_duration(max_age)?),
        None => None,
    };

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

pub fn run(config: &Config) -> Result<()> {
    let work_dir = &config.work_dir;
    let head_hash = head::load(work_dir)?;

    let (chain, reachable) = walk_chain(work_dir, &head_hash);
    remove_orphans(config, &reachable)?;
    truncate_chain(config, &chain)?;

    Ok(())
}
