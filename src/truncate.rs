use std::collections::HashSet;
use std::path::Path;
use std::time::SystemTime;

use anyhow::Result;

use crate::block::Block;
use crate::config::TruncateConfig;
use crate::head;
use crate::reported;
use crate::storage;
use crate::utils::GENESIS_HASH;

struct ChainEntry {
    hash: String,
    created: SystemTime,
}

/// Returns `true` if `s` is a 40-character hexadecimal string (i.e. a SHA-1 hash).
fn is_hex_hash(s: &str) -> bool {
    s.len() == 40 && s.chars().all(|c| c.is_ascii_hexdigit())
}

/// Returns `true` if `name` is a lock file left by an older version, which
/// locked each file separately (`.<name>.lock`) and the chain with
/// `.chain.lock`.
fn is_legacy_lock_file(name: &str) -> bool {
    name != storage::LOCK_FILE && name.starts_with('.') && name.ends_with(".lock")
}

/// Returns `(block_hashes, legacy_lock_files)` by scanning the work directory.
/// Block hashes are 40-hex-char filenames.
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
        } else if is_legacy_lock_file(name) {
            lock_files.push(name.to_string());
        }
    }

    Ok((blocks, lock_files))
}

/// Walk the block chain from HEAD back toward GENESIS, returning an ordered
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
            created,
        });
        current_hash = header.parent;
    }

    (chain, reachable)
}

/// Remove orphaned blocks (not reachable from HEAD) and lock files left by
/// older versions. This also cleans up corrupt blocks, since `walk_chain` stops
/// before adding them to the reachable set.
fn remove_orphans(
    work_dir: &Path,
    config: &TruncateConfig,
    reachable: &HashSet<String>,
    dry_run: bool,
) -> Result<()> {
    let (on_disk, legacy_locks) = scan_work_dir(work_dir)?;

    if config.remove_orphans {
        for hash in &on_disk {
            if !reachable.contains(hash) {
                if !dry_run {
                    log::info!("Removing orphaned block '{:.7}...'", hash);
                }
                storage::remove(work_dir, hash, dry_run)?;
            }
        }
    }

    for lock_file in &legacy_locks {
        if dry_run {
            eprintln!("Would have removed legacy lock file '{}'", lock_file);
            continue;
        }
        log::info!("Removing legacy lock file '{}'", lock_file);
        if let Err(error) = std::fs::remove_file(work_dir.join(lock_file)) {
            log::warn!(
                "Failed to remove legacy lock file '{}': {}",
                lock_file,
                error
            );
        }
    }

    Ok(())
}

/// Truncate blocks from the chain according to the configured rules
/// (max_blocks, max_age, truncate_reported). Never deletes HEAD.
fn truncate_chain(
    work_dir: &Path,
    config: &TruncateConfig,
    chain: &[ChainEntry],
    dry_run: bool,
) -> Result<()> {
    let reported_pos = if config.truncate_reported {
        match reported::load(work_dir)? {
            Some(hash) => chain
                .iter()
                .position(|chain_entry| chain_entry.hash == hash),
            None => None,
        }
    } else {
        None
    };

    let max_blocks = config.max_blocks.map(|n| n as usize);
    let max_age_cutoff = config.max_age.map(|max_age| SystemTime::now() - max_age);

    let mut removed = 0;
    for (i, entry) in chain.iter().enumerate() {
        if i == 0 {
            continue; // Never delete HEAD
        }

        let past_reported = reported_pos.is_some_and(|pos| i > pos);
        let past_max_blocks = max_blocks.is_some_and(|max| i >= max);
        let past_max_age = max_age_cutoff.is_some_and(|cutoff| entry.created < cutoff);
        let should_remove = past_reported || past_max_blocks || past_max_age;

        if should_remove {
            if !dry_run {
                log::info!("Truncating block '{:.7}...'", entry.hash);
            }
            storage::remove(work_dir, &entry.hash, dry_run)?;
            removed += 1;
        }
    }

    if removed > 0 {
        if dry_run {
            eprintln!("Would have truncated {} block(s)", removed);
        } else {
            log::info!("Truncated {} block(s)", removed);
        }
    }

    Ok(())
}

/// Run a single truncation pass under an exclusive lock on the state
/// directory. Waits until the lock is available; serializes against every
/// other operation on the state directory.
pub fn run(work_dir: &Path, config: &TruncateConfig, mode: u32, dry_run: bool) -> Result<()> {
    // Take the lock even in dry-run so the reported preview reflects a
    // consistent chain.
    let _lock = storage::lock(work_dir, true, mode)?;

    let head_hash = head::load(work_dir)?;
    let (chain, reachable) = walk_chain(work_dir, &head_hash);
    remove_orphans(work_dir, config, &reachable, dry_run)?;
    truncate_chain(work_dir, config, &chain, dry_run)?;

    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_is_legacy_lock_file() {
        assert!(is_legacy_lock_file(
            ".a1b2c3d4e5f6a1b2c3d4e5f6a1b2c3d4e5f6a1b2.lock"
        ));
        assert!(is_legacy_lock_file(".HEAD.lock"));
        assert!(is_legacy_lock_file(".chain.lock"));
    }

    #[test]
    fn test_is_legacy_lock_file_keeps_current_lock() {
        assert!(!is_legacy_lock_file(storage::LOCK_FILE));
    }

    #[test]
    fn test_is_legacy_lock_file_other_files() {
        assert!(!is_legacy_lock_file("HEAD"));
        assert!(!is_legacy_lock_file("HEAD.lock"));
        assert!(!is_legacy_lock_file(".HEAD"));
    }

    #[test]
    fn test_is_hex_hash() {
        assert!(is_hex_hash("a1b2c3d4e5f6a1b2c3d4e5f6a1b2c3d4e5f6a1b2"));
        assert!(is_hex_hash("0000000000000000000000000000000000000000"));
    }

    #[test]
    fn test_is_hex_hash_too_short() {
        assert!(!is_hex_hash("a1b2c3"));
    }

    #[test]
    fn test_is_hex_hash_too_long() {
        assert!(!is_hex_hash("a1b2c3d4e5f6a1b2c3d4e5f6a1b2c3d4e5f6a1b2a"));
    }

    #[test]
    fn test_is_hex_hash_non_hex() {
        assert!(!is_hex_hash("g1b2c3d4e5f6a1b2c3d4e5f6a1b2c3d4e5f6a1b2"));
    }

    #[test]
    fn test_is_hex_hash_empty() {
        assert!(!is_hex_hash(""));
    }
}
