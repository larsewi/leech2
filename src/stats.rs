use std::time::Duration;

use anyhow::Result;
use serde::Serialize;
use serde_json::Value;

use crate::config::Config;
use crate::patch;
use crate::storage;

/// Name of the cumulative stats file in the state directory.
pub const STATS_FILE: &str = "STATS";

/// Elapsed time and wire sizes for one size-reducing stage (delta merging or
/// compression). Only the primitive, non-derivable values are stored; the
/// reader computes bytes saved as `bytes_before - bytes_after`.
#[derive(Serialize)]
pub struct StageStats {
    /// Time the stage took, in milliseconds.
    pub duration_ms: f64,
    /// Wire size entering the stage.
    pub bytes_before: u64,
    /// Wire size leaving the stage.
    pub bytes_after: u64,
}

/// A single patch-creation run appended to the `STATS` file.
#[derive(Serialize)]
struct RunStats {
    /// RFC 3339 timestamp of when the run was recorded.
    timestamp: String,
    /// Delta-merging (consolidation) stage.
    delta_merging: StageStats,
    /// Compression stage.
    compression: StageStats,
}

/// Append a patch-creation run to the cumulative `STATS` JSON file when stats
/// are enabled. Best-effort: any failure is logged and swallowed so stats
/// collection never breaks patch creation.
pub fn record_patch_create(config: &Config, merge_duration: Duration, compression: StageStats) {
    if !config.stats.enable {
        return;
    }

    // Baseline for delta merging is the size of a full-state patch. If it can't
    // be computed (e.g. no STATE file), treat merging as saving nothing rather
    // than failing.
    let delta_before = match patch::full_state_size(config) {
        Ok(size) => size,
        Err(e) => {
            log::warn!(
                "Stats: could not compute full-state baseline, recording zero delta savings: {:#}",
                e
            );
            compression.bytes_before
        }
    };

    let run = RunStats {
        timestamp: chrono::Utc::now().to_rfc3339(),
        delta_merging: StageStats {
            duration_ms: merge_duration.as_secs_f64() * 1000.0,
            bytes_before: delta_before,
            // The consolidated patch is what compression receives.
            bytes_after: compression.bytes_before,
        },
        compression,
    };

    if let Err(e) = append(config, run) {
        log::warn!("Stats: failed to record patch creation: {:#}", e);
    }
}

/// Append `run` to the `STATS` JSON array in the state directory, creating the
/// file if absent and replacing it wholesale if the existing content is not a
/// valid JSON array.
fn append(config: &Config, run: RunStats) -> Result<()> {
    if config.dry_run {
        log::info!(
            "Would have appended stats to '{}'",
            config.state_dir().join(STATS_FILE).display()
        );
        return Ok(());
    }

    let state_dir = config.ensure_state_dir()?;

    let mut entries: Vec<Value> = match storage::load(&state_dir, STATS_FILE, config.file_mode)? {
        Some(bytes) => serde_json::from_slice(&bytes).unwrap_or_else(|e| {
            log::warn!(
                "STATS file is not a valid JSON array, starting fresh: {}",
                e
            );
            Vec::new()
        }),
        None => Vec::new(),
    };

    entries.push(serde_json::to_value(&run)?);
    let bytes = serde_json::to_vec_pretty(&entries)?;
    storage::store(&state_dir, STATS_FILE, &bytes, config.file_mode)?;
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;

    fn test_config(dir: &std::path::Path) -> Config {
        let mut config = Config::default();
        config.work_dir = dir.to_path_buf();
        config
    }

    fn sample_run() -> RunStats {
        RunStats {
            timestamp: "2026-07-14T10:32:05.123Z".to_string(),
            delta_merging: StageStats {
                duration_ms: 4.12,
                bytes_before: 48213,
                bytes_after: 3120,
            },
            compression: StageStats {
                duration_ms: 1.87,
                bytes_before: 3120,
                bytes_after: 1042,
            },
        }
    }

    fn load_entries(config: &Config) -> Vec<Value> {
        let state_dir = config.state_dir();
        let bytes = storage::load(&state_dir, STATS_FILE, config.file_mode)
            .unwrap()
            .unwrap();
        serde_json::from_slice(&bytes).unwrap()
    }

    #[test]
    fn test_append_creates_single_entry() {
        let dir = tempfile::tempdir().unwrap();
        let config = test_config(dir.path());

        append(&config, sample_run()).unwrap();

        let entries = load_entries(&config);
        assert_eq!(entries.len(), 1);
        assert_eq!(entries[0]["delta_merging"]["bytes_before"], 48213);
        assert_eq!(entries[0]["compression"]["bytes_after"], 1042);
    }

    #[test]
    fn test_append_is_cumulative() {
        let dir = tempfile::tempdir().unwrap();
        let config = test_config(dir.path());

        append(&config, sample_run()).unwrap();
        append(&config, sample_run()).unwrap();

        assert_eq!(load_entries(&config).len(), 2);
    }

    #[test]
    fn test_append_replaces_corrupt_file() {
        let dir = tempfile::tempdir().unwrap();
        let config = test_config(dir.path());
        let state_dir = config.ensure_state_dir().unwrap();
        storage::store(&state_dir, STATS_FILE, b"not json", config.file_mode).unwrap();

        append(&config, sample_run()).unwrap();

        assert_eq!(load_entries(&config).len(), 1);
    }

    #[test]
    fn test_append_dry_run_writes_nothing() {
        let dir = tempfile::tempdir().unwrap();
        let mut config = test_config(dir.path());
        config.dry_run = true;

        append(&config, sample_run()).unwrap();

        assert!(!config.state_dir().join(STATS_FILE).exists());
    }
}
