use std::collections::HashSet;
use std::path::Path;
use std::time::SystemTime;

use anyhow::{Context, Result};

use crate::block::Block;
use crate::config::{Config, TruncateConfig};
use crate::head;
use crate::reported;
use crate::storage;
use crate::utils::{GENESIS_HASH, join_logging_panics};

/// Lock-file name used to serialize chain-mutating operations (block creation
/// advancing HEAD, and truncation walking the chain and removing orphans).
/// Held exclusively by `Block::create` and by `truncate::run`; held for no
/// other reason.
const CHAIN_LOCK_NAME: &str = "chain";

struct ChainEntry {
    hash: String,
    created: SystemTime,
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

/// Remove orphaned blocks (not reachable from HEAD) and stale lock files
/// (whose corresponding block no longer exists on disk). This also cleans up
/// corrupt blocks, since `walk_chain` stops before adding them to the
/// reachable set.
fn remove_orphans(
    work_dir: &Path,
    config: &TruncateConfig,
    reachable: &HashSet<String>,
) -> Result<()> {
    let (on_disk, stale_locks) = scan_work_dir(work_dir)?;

    if config.remove_orphans {
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
fn truncate_chain(work_dir: &Path, config: &TruncateConfig, chain: &[ChainEntry]) -> Result<()> {
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

/// Run a single truncation pass under the chain lock. Blocks until the
/// chain lock is available; serializes against `Block::create` and any
/// other in-progress truncation in the same work directory.
pub fn run(work_dir: &Path, config: &TruncateConfig) -> Result<()> {
    let _chain_lock = storage::acquire_lock(work_dir, CHAIN_LOCK_NAME, true)
        .context("failed to acquire chain lock for truncation")?;

    let head_hash = head::load(work_dir)?;
    let (chain, reachable) = walk_chain(work_dir, &head_hash);
    remove_orphans(work_dir, config, &reachable)?;
    truncate_chain(work_dir, config, &chain)?;

    Ok(())
}

/// Spawn `run` on a background thread, taking an owned snapshot of
/// `config.work_dir` and `config.truncate` so the thread is decoupled from
/// the `Config`'s lifetime. The `JoinHandle` is parked in
/// `config.background_truncation`.
///
/// If a previous background pass is still running, this is a no-op: that
/// pass is either holding or waiting on the chain lock and will observe
/// the latest `HEAD` when it runs, so a follow-up spawn would only queue
/// behind the same chain lock and repeat the cleanup work.
pub fn spawn_background(config: &Config) {
    let mut slot = config
        .background_truncation
        .lock()
        .unwrap_or_else(|e| e.into_inner());

    let previous = slot.take();
    if let Some(handle) = previous {
        if handle.is_finished() {
            join_logging_panics(handle, "Background truncation thread");
        } else {
            log::debug!(
                "Skipping background truncation for '{}': previous pass still in flight",
                config.work_dir.display()
            );
            *slot = Some(handle);
            return;
        }
    }

    let work_dir = config.work_dir.clone();
    let truncate_config = config.truncate.clone();
    let handle = std::thread::spawn(move || {
        if let Err(e) = run(&work_dir, &truncate_config) {
            log::warn!("Background truncation failed (non-fatal): {:#}", e);
        }
    });
    *slot = Some(handle);
}

/// Join the background truncation thread most recently spawned for
/// `config`, if any. Returns after it has exited.
pub fn wait_for_pending(config: &Config) {
    let mut slot = config
        .background_truncation
        .lock()
        .unwrap_or_else(|e| e.into_inner());
    let handle = slot.take();
    if let Some(handle) = handle {
        join_logging_panics(handle, "Background truncation thread");
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_strip_lock_affixes() {
        assert_eq!(strip_lock_affixes(".abc123.lock"), Some("abc123"));
        assert_eq!(strip_lock_affixes(".hello.lock"), Some("hello"));
    }

    #[test]
    fn test_strip_lock_affixes_missing_prefix() {
        assert_eq!(strip_lock_affixes("abc123.lock"), None);
    }

    #[test]
    fn test_strip_lock_affixes_missing_suffix() {
        assert_eq!(strip_lock_affixes(".abc123"), None);
    }

    #[test]
    fn test_strip_lock_affixes_empty() {
        assert_eq!(strip_lock_affixes(""), None);
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
