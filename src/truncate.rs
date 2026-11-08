use std::collections::HashSet;
use std::path::Path;
use std::time::SystemTime;

use anyhow::Result;

use crate::block::Block;
use crate::config::TruncateConfig;
use crate::head;
use crate::reported;
use crate::storage;
use crate::utils::{GENESIS_HASH, is_hex_hash};

struct ChainEntry {
    hash: String,
    created: SystemTime,
}

/// Returns `true` if `name` is a lock file left by an older version, which
/// locked each file separately (`.<name>.lock`) and the chain with
/// `.chain.lock`.
fn is_legacy_lock_file(name: &str) -> bool {
    name != storage::LOCK_FILE && name.starts_with('.') && name.ends_with(".lock")
}

/// Returns `true` if `name` is the temp file of a block write (`<hash>.tmp`).
/// Under the exclusive lock, one can only be left by a writer that crashed
/// before renaming it into place. Unlike the temp files of HEAD, STATE and the
/// like, which the next write of that file reuses, it is never written again.
fn is_block_temp_file(name: &str) -> bool {
    name.strip_suffix(".tmp").is_some_and(is_hex_hash)
}

/// Files found by scanning the work directory.
struct WorkDirScan {
    /// Block hashes (40-hex-char filenames).
    blocks: HashSet<String>,
    /// Lock files left by older versions.
    legacy_lock_files: Vec<String>,
    /// Temp files left by crashed block writes.
    block_temp_files: Vec<String>,
}

fn scan_work_dir(work_dir: &Path) -> Result<WorkDirScan> {
    let mut scan = WorkDirScan {
        blocks: HashSet::new(),
        legacy_lock_files: Vec::new(),
        block_temp_files: Vec::new(),
    };

    for entry in std::fs::read_dir(work_dir)? {
        let entry = entry?;
        let name = entry.file_name();
        let Some(name) = name.to_str() else {
            continue;
        };
        if is_hex_hash(name) {
            scan.blocks.insert(name.to_string());
        } else if is_legacy_lock_file(name) {
            scan.legacy_lock_files.push(name.to_string());
        } else if is_block_temp_file(name) {
            scan.block_temp_files.push(name.to_string());
        }
    }

    Ok(scan)
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

/// Remove a leftover file that is not a block. Failure is logged rather than
/// returned, since a stray file does no harm.
fn remove_stray_file(work_dir: &Path, name: &str, kind: &str, dry_run: bool) {
    if dry_run {
        eprintln!("Would have removed {} '{}'", kind, name);
        return;
    }
    log::info!("Removing {} '{}'", kind, name);
    if let Err(error) = std::fs::remove_file(work_dir.join(name)) {
        log::warn!("Failed to remove {} '{}': {}", kind, name, error);
    }
}

/// Remove orphaned blocks (not reachable from HEAD), lock files left by older
/// versions and temp files left by crashed block writes. This also cleans up
/// corrupt blocks, since `walk_chain` stops before adding them to the reachable
/// set.
fn remove_orphans(
    work_dir: &Path,
    config: &TruncateConfig,
    reachable: &HashSet<String>,
    dry_run: bool,
) -> Result<()> {
    let scan = scan_work_dir(work_dir)?;

    if config.remove_orphans {
        for hash in &scan.blocks {
            if !reachable.contains(hash) {
                if !dry_run {
                    log::info!("Removing orphaned block '{:.7}...'", hash);
                }
                storage::remove(work_dir, hash, dry_run)?;
            }
        }
    }

    for lock_file in &scan.legacy_lock_files {
        remove_stray_file(work_dir, lock_file, "legacy lock file", dry_run);
    }
    for temp_file in &scan.block_temp_files {
        remove_stray_file(work_dir, temp_file, "stale block temp file", dry_run);
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
    // A max age reaching back further than the clock can represent leaves no
    // cutoff: no block is old enough to remove.
    let max_age_cutoff = config
        .max_age
        .and_then(|max_age| SystemTime::now().checked_sub(max_age));

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
    fn test_is_block_temp_file() {
        assert!(is_block_temp_file(
            "a1b2c3d4e5f6a1b2c3d4e5f6a1b2c3d4e5f6a1b2.tmp"
        ));
        assert!(!is_block_temp_file("HEAD.tmp"));
        assert!(!is_block_temp_file("STATE.tmp"));
        assert!(!is_block_temp_file(
            "a1b2c3d4e5f6a1b2c3d4e5f6a1b2c3d4e5f6a1b2"
        ));
    }
}
