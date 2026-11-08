//! Recording of the stage stats of an in-flight patch-creation run. The
//! operations that produce stats ([`crate::patch::Patch::create`],
//! [`crate::wire::encode_patch`]) record into it as they run, and
//! [`crate::stats::finalize_patch_create`] drains it into the `STATS` file.
//!
//! Nothing in this module is part of leech2's Rust public API; the module is
//! declared `mod recorder;` (private) at the crate root.

use std::cell::RefCell;

use serde::{Deserialize, Serialize};

/// Elapsed time and wire sizes for one size-reducing stage (delta merging or
/// compression). Only the primitive, non-derivable values are stored; the
/// reader computes bytes saved as `bytes_in - bytes_out`.
#[derive(Debug, Serialize, Deserialize)]
pub struct StageStats {
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
    pub fn saved(&self) -> i64 {
        self.bytes_in as i64 - self.bytes_out as i64
    }

    /// Percentage of the input saved by this stage. Zero when there was no
    /// input; negative when the stage grew the payload.
    pub fn percent_saved(&self) -> f64 {
        if self.bytes_in == 0 {
            0.0
        } else {
            self.saved() as f64 / self.bytes_in as f64 * 100.0
        }
    }
}

/// A size-reducing stage whose stats can be recorded into the in-flight run.
pub enum Stage {
    DeltaMerging,
    Compression,
}

/// The stages recorded for the patch-creation run currently in flight.
#[derive(Debug, Default)]
pub struct PendingStats {
    pub delta_merging: Option<StageStats>,
    pub compression: Option<StageStats>,
}

thread_local! {
    // A patch-creation run happens entirely on its caller's thread, so
    // concurrent runs on other threads cannot mix their stats into it.
    static PENDING_STATS: RefCell<PendingStats> = RefCell::default();
}

/// Record a stage of the in-flight patch-creation run. Callers should only
/// invoke this when `config.stats.enable` is set.
pub fn record_stage(stage: Stage, stats: StageStats) {
    PENDING_STATS.with_borrow_mut(|pending| match stage {
        Stage::DeltaMerging => pending.delta_merging = Some(stats),
        Stage::Compression => pending.compression = Some(stats),
    });
}

/// Take the stages recorded for the in-flight run, leaving none recorded.
pub fn take_pending() -> PendingStats {
    PENDING_STATS.take()
}
