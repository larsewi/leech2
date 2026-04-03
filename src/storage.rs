use std::fs::{self, File};
use std::io::{Read, Write};
use std::path::Path;

use anyhow::{Context, Result, bail};

use crate::utils::GENESIS_HASH;
use fs2::FileExt;

/// Acquires a lock on a separate `.<name>.lock` file for inter-process synchronization.
/// Returns the lock file handle; the lock is released when the handle is dropped.
fn acquire_lock(dir: &Path, name: &str, exclusive: bool) -> Result<File> {
    let lock_path = dir.join(format!(".{}.lock", name));
    let lock_file = File::create(&lock_path)
        .with_context(|| format!("failed to open lock file '{}'", lock_path.display()))?;
    if exclusive {
        lock_file.lock_exclusive()
    } else {
        lock_file.lock_shared()
    }
    .with_context(|| format!("failed to acquire lock on '{}'", lock_path.display()))?;
    Ok(lock_file)
}

/// Saves data to a file in the work directory using a separate lock file and atomic rename.
pub fn store(work_dir: &Path, name: &str, data: &[u8]) -> Result<()> {
    fs::create_dir_all(work_dir)
        .with_context(|| format!("failed to create work directory '{}'", work_dir.display()))?;

    let _lock = acquire_lock(work_dir, name, true)?;

    // Write to temp file, then atomic rename for crash safety.
    let tmp_path = work_dir.join(format!("{}.tmp", name));
    let path = work_dir.join(name);

    File::create(&tmp_path)
        .with_context(|| format!("failed to create temp file '{}'", tmp_path.display()))?
        .write_all(data)
        .with_context(|| format!("failed to write to '{}'", tmp_path.display()))?;
    fs::rename(&tmp_path, &path).with_context(|| {
        format!(
            "failed to rename '{}' to '{}'",
            tmp_path.display(),
            path.display()
        )
    })?;

    // _lock dropped here, releasing exclusive lock.
    log::trace!("Stored {} bytes to '{}'", data.len(), path.display());
    Ok(())
}

/// Removes a file from the work directory using an exclusive lock.
pub fn remove(work_dir: &Path, name: &str) -> Result<()> {
    let path = work_dir.join(name);

    let _lock = acquire_lock(work_dir, name, true)?;

    match fs::remove_file(&path) {
        Ok(()) => {}
        Err(e) if e.kind() == std::io::ErrorKind::NotFound => {
            log::trace!(
                "File '{}' does not exist, nothing to remove",
                path.display()
            );
            return Ok(());
        }
        Err(e) => {
            return Err(anyhow::Error::new(e)
                .context(format!("failed to remove file '{}'", path.display())));
        }
    }

    // Best-effort cleanup of the lock file after removing the data file.
    let lock_path = work_dir.join(format!(".{}.lock", name));
    drop(_lock);
    let _ = fs::remove_file(&lock_path);

    log::trace!("Removed '{}'", path.display());
    Ok(())
}

/// Loads data from a file in the work directory with a shared lock.
pub fn load(work_dir: &Path, name: &str) -> Result<Option<Vec<u8>>> {
    let path = work_dir.join(name);

    let _lock = acquire_lock(work_dir, name, false)?;

    match File::open(&path) {
        Ok(mut file) => {
            let mut data = Vec::new();
            file.read_to_end(&mut data)
                .with_context(|| format!("failed to read from '{}'", path.display()))?;
            log::trace!("Loaded {} bytes from '{}'", data.len(), path.display());
            Ok(Some(data))
        }
        Err(e) if e.kind() == std::io::ErrorKind::NotFound => {
            log::trace!("File '{}' does not exist", path.display());
            Ok(None)
        }
        Err(e) => {
            Err(anyhow::Error::new(e).context(format!("failed to open file '{}'", path.display())))
        }
    }
}

pub fn resolve_hash_prefix(work_dir: &Path, prefix: &str) -> Result<String> {
    let mut matches: Vec<String> = Vec::new();

    if GENESIS_HASH.starts_with(prefix) {
        matches.push(GENESIS_HASH.to_string());
    }

    for entry in std::fs::read_dir(work_dir)? {
        let entry = entry?;
        let name = entry.file_name();
        let Some(name) = name.to_str() else {
            continue;
        };
        if name.starts_with(prefix)
            && name.len() == 40
            && name.chars().all(|c| c.is_ascii_hexdigit())
        {
            matches.push(name.to_string());
        }
    }

    match matches.as_slice() {
        [] => bail!("no block found matching prefix '{}'", prefix),
        [single] => Ok(single.clone()),
        [first, second, ..] => bail!(
            "ambiguous hash prefix '{}': matches {} and {}",
            prefix,
            first,
            second
        ),
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use fs2::FileExt;
    use tempfile::tempdir;

    #[test]
    fn test_acquire_lock_creates_lock_file() {
        let dir = tempdir().unwrap();
        let _lock = acquire_lock(dir.path(), "foo", true).unwrap();
        assert!(dir.path().join(".foo.lock").exists());
    }

    #[test]
    fn test_shared_locks_do_not_block_each_other() {
        let dir = tempdir().unwrap();
        let _lock1 = acquire_lock(dir.path(), "foo", false).unwrap();
        let _lock2 = acquire_lock(dir.path(), "foo", false).unwrap();
    }

    #[test]
    fn test_exclusive_lock_blocks_exclusive_lock() {
        let dir = tempdir().unwrap();
        let _lock = acquire_lock(dir.path(), "foo", true).unwrap();

        let lock_path = dir.path().join(".foo.lock");
        let file = File::create(&lock_path).unwrap();
        assert!(file.try_lock_exclusive().is_err());
    }

    #[test]
    fn test_exclusive_lock_blocks_shared_lock() {
        let dir = tempdir().unwrap();
        let _lock = acquire_lock(dir.path(), "foo", true).unwrap();

        let lock_path = dir.path().join(".foo.lock");
        let file = File::create(&lock_path).unwrap();
        assert!(file.try_lock_shared().is_err());
    }

    #[test]
    fn test_shared_lock_blocks_exclusive_lock() {
        let dir = tempdir().unwrap();
        let _lock = acquire_lock(dir.path(), "foo", false).unwrap();

        let lock_path = dir.path().join(".foo.lock");
        let file = File::create(&lock_path).unwrap();
        assert!(file.try_lock_exclusive().is_err());
    }

    #[test]
    fn test_lock_released_on_drop() {
        let dir = tempdir().unwrap();
        {
            let _lock = acquire_lock(dir.path(), "foo", true).unwrap();
        }
        let _lock = acquire_lock(dir.path(), "foo", true).unwrap();
    }

    #[test]
    fn test_acquire_lock_invalid_dir() {
        let result = acquire_lock(Path::new("/nonexistent/path"), "foo", true);
        assert!(result.is_err());
    }

    #[test]
    fn test_resolve_hash_prefix_exact_match() {
        let dir = tempdir().unwrap();
        let hash = "abcdef1234567890abcdef1234567890abcdef12";
        File::create(dir.path().join(hash)).unwrap();

        let result = resolve_hash_prefix(dir.path(), "abcdef").unwrap();
        assert_eq!(result, hash);
    }

    #[test]
    fn test_resolve_hash_prefix_full_hash() {
        let dir = tempdir().unwrap();
        let hash = "abcdef1234567890abcdef1234567890abcdef12";
        File::create(dir.path().join(hash)).unwrap();

        let result = resolve_hash_prefix(dir.path(), hash).unwrap();
        assert_eq!(result, hash);
    }

    #[test]
    fn test_resolve_hash_prefix_no_match() {
        let dir = tempdir().unwrap();
        let hash = "abcdef1234567890abcdef1234567890abcdef12";
        File::create(dir.path().join(hash)).unwrap();

        let result = resolve_hash_prefix(dir.path(), "ffffff");
        assert!(result.is_err());
    }

    #[test]
    fn test_resolve_hash_prefix_ambiguous() {
        let dir = tempdir().unwrap();
        File::create(dir.path().join("abcdef1234567890abcdef1234567890abcdef12")).unwrap();
        File::create(dir.path().join("abcdef5678901234567890abcdef1234567890ab")).unwrap();

        let result = resolve_hash_prefix(dir.path(), "abcdef");
        assert!(result.is_err());
    }

    #[test]
    fn test_resolve_hash_prefix_genesis_hash() {
        let dir = tempdir().unwrap();

        let result = resolve_hash_prefix(dir.path(), "00000").unwrap();
        assert_eq!(result, GENESIS_HASH);
    }

    #[test]
    fn test_resolve_hash_prefix_ignores_non_hash_files() {
        let dir = tempdir().unwrap();
        // Too short to be a hash
        File::create(dir.path().join("abcdef")).unwrap();
        // Right length but contains non-hex characters
        File::create(dir.path().join("abcdef1234567890abcdef1234567890abcdefGH")).unwrap();

        let result = resolve_hash_prefix(dir.path(), "abcdef");
        assert!(result.is_err());
    }
}
