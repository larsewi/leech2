use std::fmt;

use anyhow::{Context, Result};
use serde::{Deserialize, Serialize};
use serde_json::Value;

use crate::config::Config;
use crate::storage;

/// Name of the cumulative stats file in the state directory.
pub const STATS_FILE: &str = "STATS";

/// Elapsed time and wire sizes for one size-reducing stage (delta merging or
/// compression). Only the primitive, non-derivable values are stored; the
/// reader computes bytes saved as `bytes_in - bytes_out`.
#[derive(Debug, Serialize, Deserialize)]
pub(crate) struct StageStats {
    /// Time the stage took, in milliseconds.
    pub duration_ms: f64,
    /// Wire size entering the stage.
    pub bytes_in: u64,
    /// Wire size leaving the stage.
    pub bytes_out: u64,
}

impl StageStats {
    /// Bytes saved on the wire by this stage. Signed because compression can
    /// grow a tiny payload.
    fn saved(&self) -> i64 {
        self.bytes_in as i64 - self.bytes_out as i64
    }
}

/// A single patch-creation run appended to the `STATS` file.
#[derive(Serialize, Deserialize)]
struct RunStats {
    /// RFC 3339 timestamp of when the run was recorded.
    timestamp: String,
    /// Delta-merging (consolidation) stage.
    delta_merging: StageStats,
    /// Compression stage.
    compression: StageStats,
}

/// A size-reducing stage whose stats can be recorded into a [`Config`]'s
/// in-flight run.
pub(crate) enum Stage {
    DeltaMerging,
    Compression,
}

/// The stages recorded for the patch-creation run currently in flight. Lives
/// behind a `Mutex` on [`Config`]; the operations that produce stats
/// ([`crate::patch::Patch::create`], [`crate::wire::encode_patch`]) record into
/// it as they run, and [`finalize_patch_create`] drains it into the `STATS`
/// file. This keeps the stats out of function return types and lets future
/// operations (e.g. block pruning) contribute without threading values around.
#[derive(Debug, Default)]
pub(crate) struct PendingStats {
    delta_merging: Option<StageStats>,
    compression: Option<StageStats>,
}

/// Record a stage of the in-flight patch-creation run. Callers should only
/// invoke this when `config.stats.enable` is set.
pub(crate) fn record_stage(config: &Config, stage: Stage, stats: StageStats) {
    let mut pending = config
        .pending_stats
        .lock()
        .unwrap_or_else(|e| e.into_inner());
    match stage {
        Stage::DeltaMerging => pending.delta_merging = Some(stats),
        Stage::Compression => pending.compression = Some(stats),
    }
}

/// Append the in-flight patch-creation run to the cumulative `STATS` JSON file
/// and clear the accumulator. No-op when stats are disabled. Best-effort: any
/// failure is logged and swallowed so stats collection never breaks patch
/// creation.
pub fn finalize_patch_create(config: &Config) {
    if !config.stats.enable {
        return;
    }

    let (delta_merging, compression) = {
        let mut pending = config
            .pending_stats
            .lock()
            .unwrap_or_else(|e| e.into_inner());
        (pending.delta_merging.take(), pending.compression.take())
    };

    let (Some(delta_merging), Some(compression)) = (delta_merging, compression) else {
        log::warn!("Stats: incomplete patch-create run, nothing recorded");
        return;
    };

    let run = RunStats {
        timestamp: chrono::Utc::now().to_rfc3339(),
        delta_merging,
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
    storage::store(
        &state_dir,
        STATS_FILE,
        &bytes,
        config.file_mode,
        config.dry_run,
    )?;
    Ok(())
}

/// Aggregated view of all runs in the stats file: per stage, the total and
/// most-recent value for time and bytes saved. Averages are derived in
/// `Display`.
pub struct Summary {
    /// Number of recorded runs.
    pub runs: usize,
    /// Total delta-merging time across all runs, in milliseconds.
    pub delta_total_ms: f64,
    /// Delta-merging time of the most recent run, in milliseconds.
    pub delta_last_ms: f64,
    /// Total bytes saved by delta merging across all runs.
    pub delta_saved_bytes: i64,
    /// Bytes saved by delta merging in the most recent run.
    pub delta_last_saved_bytes: i64,
    /// Total compression time across all runs, in milliseconds.
    pub compression_total_ms: f64,
    /// Compression time of the most recent run, in milliseconds.
    pub compression_last_ms: f64,
    /// Total bytes saved by compression across all runs.
    pub compression_saved_bytes: i64,
    /// Bytes saved by compression in the most recent run.
    pub compression_last_saved_bytes: i64,
}

/// Write a two-space-indented table: the first column is left-aligned, the rest
/// right-aligned, each column padded to its widest cell. Rows are separated by
/// newlines with no trailing newline.
fn write_table(f: &mut fmt::Formatter<'_>, rows: &[[String; 4]]) -> fmt::Result {
    let mut widths = [0usize; 4];
    for row in rows {
        for (i, cell) in row.iter().enumerate() {
            widths[i] = widths[i].max(cell.len());
        }
    }
    for (index, row) in rows.iter().enumerate() {
        if index > 0 {
            writeln!(f)?;
        }
        write!(f, "  {:<width$}", row[0], width = widths[0])?;
        for i in 1..4 {
            write!(f, "  {:>width$}", row[i], width = widths[i])?;
        }
    }
    Ok(())
}

impl fmt::Display for Summary {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        let runs = self.runs as f64;
        let ms = |value: f64| format!("{:.2} ms", value);
        let avg_ms = |total: f64| ms(total / runs);
        let bytes = |value: i64| format!("{} bytes", value);
        let avg_bytes = |total: i64| bytes((total as f64 / runs).round() as i64);

        let time_rows = [
            ["Time".into(), "Total".into(), "Avg".into(), "Last".into()],
            [
                "Delta merging".into(),
                ms(self.delta_total_ms),
                avg_ms(self.delta_total_ms),
                ms(self.delta_last_ms),
            ],
            [
                "Compression".into(),
                ms(self.compression_total_ms),
                avg_ms(self.compression_total_ms),
                ms(self.compression_last_ms),
            ],
        ];

        let bytes_rows = [
            [
                "Bytes saved".into(),
                "Total".into(),
                "Avg".into(),
                "Last".into(),
            ],
            [
                "Delta merging".into(),
                bytes(self.delta_saved_bytes),
                avg_bytes(self.delta_saved_bytes),
                bytes(self.delta_last_saved_bytes),
            ],
            [
                "Compression".into(),
                bytes(self.compression_saved_bytes),
                avg_bytes(self.compression_saved_bytes),
                bytes(self.compression_last_saved_bytes),
            ],
        ];

        write!(f, "Stats summary ({} runs)\n\n", self.runs)?;
        write_table(f, &time_rows)?;
        write!(f, "\n\n")?;
        write_table(f, &bytes_rows)
    }
}

/// Read and aggregate the `STATS` file into a [`Summary`]. Returns `Ok(None)`
/// when no stats have been recorded (missing or empty file).
pub fn summarize(config: &Config) -> Result<Option<Summary>> {
    let state_dir = config.state_dir();
    // Guard on existence so a missing state dir doesn't trip the lock-file
    // creation in `storage::load`.
    if !state_dir.join(STATS_FILE).exists() {
        return Ok(None);
    }
    let Some(bytes) = storage::load(&state_dir, STATS_FILE, config.file_mode)? else {
        return Ok(None);
    };
    let runs: Vec<RunStats> =
        serde_json::from_slice(&bytes).context("failed to parse STATS file")?;
    let Some(last) = runs.last() else {
        return Ok(None);
    };

    let mut summary = Summary {
        runs: runs.len(),
        delta_total_ms: 0.0,
        delta_last_ms: last.delta_merging.duration_ms,
        delta_saved_bytes: 0,
        delta_last_saved_bytes: last.delta_merging.saved(),
        compression_total_ms: 0.0,
        compression_last_ms: last.compression.duration_ms,
        compression_saved_bytes: 0,
        compression_last_saved_bytes: last.compression.saved(),
    };
    for run in &runs {
        summary.delta_total_ms += run.delta_merging.duration_ms;
        summary.delta_saved_bytes += run.delta_merging.saved();
        summary.compression_total_ms += run.compression.duration_ms;
        summary.compression_saved_bytes += run.compression.saved();
    }
    Ok(Some(summary))
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
                bytes_in: 48213,
                bytes_out: 3120,
            },
            compression: StageStats {
                duration_ms: 1.87,
                bytes_in: 3120,
                bytes_out: 1042,
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
        assert_eq!(entries[0]["delta_merging"]["bytes_in"], 48213);
        assert_eq!(entries[0]["compression"]["bytes_out"], 1042);
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
        storage::store(&state_dir, STATS_FILE, b"not json", config.file_mode, false).unwrap();

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
