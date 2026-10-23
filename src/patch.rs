pub use crate::proto::patch::Patch;

use std::collections::{HashMap, HashSet};
use std::fmt;
use std::path::Path;
use std::time::Instant;

use anyhow::{Context, Result, bail};
use prost::Message;
use prost_types::Timestamp;

use crate::block::Block;
use crate::config::{Config, TableConfig};
use crate::delta::Delta;
use crate::event;
use crate::head;
use crate::proto::patch::Delta as ProtoDelta;
use crate::proto::patch::EventBatches as ProtoEventBatches;
use crate::proto::state::State as ProtoState;
use crate::proto::table::Table as ProtoTable;
use crate::stats::{self, Stage, StageStats};
use crate::utils;
use crate::utils::GENESIS_HASH;

fn fmt_payload<T: fmt::Display>(
    payload: &HashMap<String, T>,
    label: &str,
    f: &mut fmt::Formatter<'_>,
) -> fmt::Result {
    if !payload.is_empty() {
        write!(f, "\n  {} ({}):", label, payload.len())?;
        for (name, value) in payload {
            write!(
                f,
                "\n    '{}' {}",
                name,
                utils::indent(&value.to_string(), "    ")
            )?;
        }
    }
    Ok(())
}

impl fmt::Display for Patch {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "Patch:")?;
        write!(f, "\n  Head: {}", self.head)?;
        match &self.created {
            Some(timestamp) => write!(f, "\n  Created: {}", utils::format_timestamp(timestamp))?,
            // Timestamp is None when the head points to genesis (no blocks exist yet).
            None => write!(f, "\n  Created: N/A")?,
        }
        write!(f, "\n  Blocks: {}", self.num_blocks)?;
        fmt_payload(&self.deltas, "Deltas", f)?;
        fmt_payload(&self.states, "States", f)?;
        fmt_payload(&self.events, "Events", f)?;
        if self.deltas.is_empty() && self.states.is_empty() && self.events.is_empty() {
            write!(f, "\n  Payload: None")?;
        }
        Ok(())
    }
}

/// Load the head block header and walk the chain back to (but not including)
/// `last_known`, collecting block hashes. Only the block header is decoded
/// per block, avoiding the heavier full-payload parse. Returns the head
/// block's timestamp and the hashes in newest-first order. If `head` matches
/// `last_known`, returns an empty hash list.
fn collect_block_hashes(
    work_dir: &Path,
    head: &str,
    last_known: &str,
    mode: u32,
) -> Result<(Option<Timestamp>, Vec<String>)> {
    let block = Block::load_header(work_dir, head, mode)?;
    let created = block.created;

    if head == last_known {
        return Ok((created, Vec::new()));
    }

    let mut hashes = vec![head.to_string()];
    let mut parent = block.parent;

    while parent != GENESIS_HASH && parent != last_known {
        hashes.push(parent.clone());
        parent = Block::load_header(work_dir, &parent, mode)?.parent;
    }

    if parent != last_known {
        bail!("block '{}' not found in chain", last_known);
    }

    Ok((created, hashes))
}

/// Running tally of how many entries of each kind a table contributed
/// across the blocks in a consolidation run, before the merge collapses
/// them. Used to log the pre -> post reduction.
#[derive(Clone, Copy, Default)]
struct DeltaCounts {
    inserts: usize,
    updates: usize,
    deletes: usize,
}

/// Merge a single block's deltas into per-table running results. Blocks are
/// fed in oldest-first, so the running results are the older (parent) side
/// and the incoming block is the newer (child) side: the merge direction is
/// `parent.merge(child)`. A table's first block is merged into an empty delta,
/// so every block passes through the merge rules.
///
/// For tables with a change-timestamp column, the block's creation time is
/// passed to the merge and becomes the change timestamp of the entries it
/// touches.
///
/// Tables that are new or whose layout changed (delta is `None`), or whose
/// merge failed, are added to `skipped_tables` and fall back to full state.
fn merge_block_deltas(
    block: Block,
    table_configs: &HashMap<String, TableConfig>,
    merged_deltas: &mut HashMap<String, Delta>,
    skipped_tables: &mut HashSet<String>,
    pre_counts: &mut HashMap<String, DeltaCounts>,
) {
    for (table_name, payload) in block.payload {
        if skipped_tables.contains(&table_name) {
            continue;
        }

        // A missing delta means the table is new or its field layout changed
        // between blocks; skip further merging and fall back to full state.
        let Some(proto_delta) = payload.delta else {
            log::info!(
                "Table '{}' is new or its layout changed, falling back to full state",
                table_name
            );
            merged_deltas.remove(&table_name);
            skipped_tables.insert(table_name);
            continue;
        };

        let counts = pre_counts.entry(table_name.clone()).or_default();
        counts.inserts += proto_delta.inserts.len();
        counts.updates += proto_delta.updates.len();
        counts.deletes += proto_delta.deletes.len();

        // A table removed from config has no change-timestamp column.
        let created = match table_configs.get(&table_name) {
            Some(table_config) if table_config.change_timestamp.is_some() => block.created,
            _ => None,
        };

        let result = Delta::try_from(proto_delta).and_then(|child| {
            let mut parent = match merged_deltas.remove(&table_name) {
                Some(parent) => parent,
                None => Delta {
                    primary_key_names: child.primary_key_names.clone(),
                    subsidiary_value_names: child.subsidiary_value_names.clone(),
                    ..Default::default()
                },
            };
            parent.merge(child, created)?;
            Ok(parent)
        });

        match result {
            Ok(delta) => {
                merged_deltas.insert(table_name, delta);
            }
            Err(e) => {
                log::warn!(
                    "Merge failed for table '{}', falling back to full state: {}",
                    table_name,
                    e
                );
                merged_deltas.remove(&table_name);
                skipped_tables.insert(table_name);
            }
        }
    }
}

type ConsolidateResult = (
    u32,
    HashMap<String, ProtoDelta>,
    HashMap<String, ProtoTable>,
);

/// Merge the blocks in `block_hashes` (newest first) into per-table deltas
/// and states. `events` receives the events of every block that loads, even
/// when consolidation fails, so a fallback patch can still carry them.
fn try_consolidate(
    work_dir: &Path,
    block_hashes: &[String],
    mode: u32,
    table_configs: &HashMap<String, TableConfig>,
    events: &mut HashMap<String, ProtoEventBatches>,
) -> Result<ConsolidateResult> {
    if block_hashes.is_empty() {
        return Ok((0, HashMap::new(), HashMap::new()));
    }

    let num_blocks = block_hashes.len() as u32;

    // Load blocks one at a time oldest-first, merging deltas incrementally.
    // Only one block's payload and the per-table running results are in
    // memory at a time.
    let mut merged_deltas: HashMap<String, Delta> = HashMap::new();
    let mut skipped_tables: HashSet<String> = HashSet::new();
    let mut pre_counts: HashMap<String, DeltaCounts> = HashMap::new();
    let mut load_error = None;

    for (index, hash) in block_hashes.iter().rev().enumerate() {
        log::trace!(
            "Merging block {}/{}: '{:.7}...'",
            index + 1,
            num_blocks,
            hash
        );
        let mut block = match Block::load(work_dir, hash, mode) {
            Ok(block) => block,
            Err(e) => {
                // Keep loading the remaining blocks for their events, but stop
                // merging: deltas cannot be merged across a missing block.
                load_error.get_or_insert(e);
                continue;
            }
        };
        event::append_batches(events, hash, std::mem::take(&mut block.events));
        if load_error.is_none() {
            merge_block_deltas(
                block,
                table_configs,
                &mut merged_deltas,
                &mut skipped_tables,
                &mut pre_counts,
            );
        }
    }
    if let Some(e) = load_error {
        return Err(e);
    }

    // Load state for per-table size comparison and fallback.
    let state_tables = match ProtoState::load(work_dir, mode)? {
        Some(state) => state.tables,
        None => HashMap::new(),
    };

    let mut result_deltas = HashMap::new();
    let mut result_states = HashMap::new();

    // Skipped tables fall back to full state. If the STATE file can't satisfy
    // one (e.g. STATE was deleted), bail so the caller falls back to a
    // full-state patch for the whole set, rather than emitting a patch that
    // silently omits a table that needs full state.
    for table_name in &skipped_tables {
        let state_table = state_tables.get(table_name).with_context(|| {
            format!(
                "table '{}' needs full state but is not in the STATE file",
                table_name
            )
        })?;
        log::info!("Table '{}': using full state", table_name);
        result_states.insert(table_name.clone(), state_table.clone());
    }

    for (table_name, merged) in merged_deltas {
        let merged_delta = ProtoDelta::from(merged);

        let pre = pre_counts.get(&table_name).copied().unwrap_or_default();
        log::info!(
            "Table '{}': consolidated {} block(s); inserts {}->{}, updates {}->{}, deletes {}->{}",
            table_name,
            num_blocks,
            pre.inserts,
            merged_delta.inserts.len(),
            pre.updates,
            merged_delta.updates.len(),
            pre.deletes,
            merged_delta.deletes.len(),
        );

        // A table removed from config has no setting; keep the default.
        let use_full_state_if_smaller = match table_configs.get(&table_name) {
            Some(table_config) => table_config.use_full_state_if_smaller,
            None => true,
        };

        // Per-table size comparison: use full state if it's smaller.
        if use_full_state_if_smaller
            && let Some(state_table) = state_tables.get(&table_name)
            && state_table.encoded_len() < merged_delta.encoded_len()
        {
            log::info!(
                "Table '{}': using full state (smaller than consolidated delta)",
                table_name
            );
            result_states.insert(table_name, state_table.clone());
            continue;
        }

        result_deltas.insert(table_name, merged_delta);
    }

    Ok((num_blocks, result_deltas, result_states))
}

/// Encoded protobuf size of a full-state patch for the current HEAD, carrying
/// `num_blocks` so it matches the framing of the actual patch. Used as the
/// baseline for measuring how many bytes delta merging saved on the wire; when
/// the actual patch is itself full state, this makes the saving exactly zero.
fn full_state_size(config: &Config, num_blocks: u32) -> Result<u64> {
    let state_dir = config.ensure_state_dir()?;
    let head = head::load(&state_dir, config.file_mode)?;
    let created = Block::load_header(&state_dir, &head, config.file_mode)
        .ok()
        .and_then(|header| header.created);
    let mut patch = full_state_patch(&state_dir, &head, created, config.file_mode)?;
    patch.num_blocks = num_blocks;
    Ok(patch.encoded_len() as u64)
}

/// Build a full-state patch from the STATE file. `created` is the HEAD
/// block's creation time, which callers have already read.
fn full_state_patch(
    work_dir: &Path,
    head: &str,
    created: Option<Timestamp>,
    mode: u32,
) -> Result<Patch> {
    let state =
        ProtoState::load(work_dir, mode)?.context("no STATE file found for full state patch")?;
    Ok(Patch {
        head: head.to_string(),
        created,
        states: state.tables,
        ..Default::default()
    })
}

/// Build a full-state patch for when there is no known reference to start
/// from. It carries no events: those of every stored block could make the
/// patch too big.
fn unknown_reference_patch(work_dir: &Path, head: &str, mode: u32) -> Result<Patch> {
    let created = Block::load_header(work_dir, head, mode)
        .ok()
        .and_then(|header| header.created);
    let patch = full_state_patch(work_dir, head, created, mode)?;
    log::info!("Consolidated patch:\n{}", patch);
    Ok(patch)
}

impl Patch {
    /// Consolidate the chain from `last_known` to HEAD into a patch. When stats
    /// are enabled, times the consolidation and records the delta-merging stage
    /// (full-state size vs consolidated size) into the config's in-flight run.
    pub fn create(config: &Config, last_known: &str) -> Result<Patch> {
        let start = Instant::now();
        let mut patch = Self::create_consolidated(config, last_known)?;

        if config.stats.enable {
            let duration_ms = start.elapsed().as_secs_f64() * 1000.0;
            // Delta merging only concerns tables, and the full-state baseline
            // carries no events, so leave events out of the measurement.
            let events = std::mem::take(&mut patch.events);
            let bytes_out = patch.encoded_len() as u64;
            patch.events = events;
            // Baseline is a full-state patch; if it can't be computed (e.g. no
            // STATE file), treat merging as saving nothing rather than failing.
            let bytes_in = full_state_size(config, patch.num_blocks).unwrap_or_else(|e| {
                log::warn!(
                    "Stats: could not compute full-state baseline, recording zero delta savings: {:#}",
                    e
                );
                bytes_out
            });
            stats::record_stage(
                config,
                Stage::DeltaMerging,
                StageStats {
                    duration_ms,
                    bytes_in,
                    bytes_out,
                },
            );
        }

        if config.dry_run {
            // `dry_run` is only ever set by the CLI, so this stdout print never
            // reaches FFI consumers. Show the patch that would have been created.
            println!("Would have created patch '{:.7}...'\n{}", patch.head, patch);
        }

        Ok(patch)
    }

    fn create_consolidated(config: &Config, last_known: &str) -> Result<Patch> {
        let state_dir = config.ensure_state_dir()?;
        let file_mode = config.file_mode;

        let resolved = crate::storage::resolve_hash_prefix(&state_dir, last_known);

        let head = head::load(&state_dir, file_mode)?;

        if head == GENESIS_HASH {
            let patch = Patch {
                head,
                ..Default::default()
            };
            log::info!("Consolidated patch:\n{}", patch);
            return Ok(patch);
        }

        // If the reference block can't be resolved or is genesis, produce a
        // full STATE payload (DELETE + INSERT) which is always safe to apply
        // regardless of current database contents.
        let last_known = match resolved {
            Ok(hash) if hash != GENESIS_HASH => hash,
            Ok(_) => {
                log::info!("Reference is genesis, producing full state patch");
                return unknown_reference_patch(&state_dir, &head, file_mode);
            }
            Err(e) => {
                log::warn!(
                    "Reference block not found, producing full state patch: {}",
                    e
                );
                return unknown_reference_patch(&state_dir, &head, file_mode);
            }
        };

        // A reference outside the chain (e.g. a broken chain) is as good as
        // unknown.
        let (created, block_hashes) =
            match collect_block_hashes(&state_dir, &head, &last_known, file_mode) {
                Ok(result) => result,
                Err(e) => {
                    log::warn!("Consolidation failed, falling back to full state: {}", e);
                    return unknown_reference_patch(&state_dir, &head, file_mode);
                }
            };

        let mut events = HashMap::new();
        let (num_blocks, deltas, states) = match try_consolidate(
            &state_dir,
            &block_hashes,
            file_mode,
            &config.tables,
            &mut events,
        ) {
            Ok(result) => result,
            Err(e) => {
                log::warn!("Consolidation failed, falling back to full state: {}", e);
                // The reference is known, so the events of the blocks
                // after it are all the receiver lacks.
                let mut patch = full_state_patch(&state_dir, &head, created, file_mode)?;
                patch.events = events;
                log::info!("Consolidated patch:\n{}", patch);
                return Ok(patch);
            }
        };

        let patch = Patch {
            head,
            created,
            num_blocks,
            deltas,
            states,
            events,
        };

        log::info!("Consolidated patch:\n{}", patch);
        Ok(patch)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    use crate::cell::{Cell, text_proto_cells};
    use crate::proto::block::Delta as ProtoBlockDelta;
    use crate::proto::block::TableChange;
    use crate::proto::block::Update as ProtoBlockUpdate;
    use crate::proto::record::Record as ProtoRecord;

    fn timestamp(seconds: i64) -> Timestamp {
        Timestamp { seconds, nanos: 0 }
    }

    fn insert(key: &str, value: &str) -> ProtoRecord {
        ProtoRecord {
            key: text_proto_cells(&[key]),
            value: text_proto_cells(&[value]),
        }
    }

    fn update(key: &str, old_value: &str, new_value: &str) -> ProtoBlockUpdate {
        ProtoBlockUpdate {
            key: text_proto_cells(&[key]),
            old_value: text_proto_cells(&[old_value]),
            new_value: text_proto_cells(&[new_value]),
        }
    }

    fn block(seconds: i64, deltas: Vec<(&str, Vec<ProtoRecord>, Vec<ProtoBlockUpdate>)>) -> Block {
        let mut payload = HashMap::new();
        for (table_name, inserts, updates) in deltas {
            let delta = ProtoBlockDelta {
                primary_key_names: vec!["id".to_string()],
                subsidiary_value_names: vec!["name".to_string()],
                inserts,
                updates,
                ..Default::default()
            };
            payload.insert(table_name.to_string(), TableChange { delta: Some(delta) });
        }
        Block {
            created: Some(timestamp(seconds)),
            payload,
            ..Default::default()
        }
    }

    #[test]
    fn test_merge_block_deltas_change_timestamp_from_last_block() {
        let table_configs = HashMap::from([
            (
                "tracked".to_string(),
                TableConfig {
                    change_timestamp: Some("changed".to_string()),
                    ..Default::default()
                },
            ),
            ("untracked".to_string(), TableConfig::default()),
        ]);
        let blocks = vec![
            block(
                100,
                vec![
                    ("tracked", vec![insert("1", "Alice")], vec![]),
                    ("untracked", vec![insert("1", "Alice")], vec![]),
                ],
            ),
            block(200, vec![("tracked", vec![insert("2", "Bob")], vec![])]),
            block(
                300,
                vec![
                    ("tracked", vec![], vec![update("1", "Alice", "Alicia")]),
                    ("untracked", vec![], vec![update("1", "Alice", "Alicia")]),
                ],
            ),
        ];

        let mut merged_deltas = HashMap::new();
        let mut skipped_tables = HashSet::new();
        let mut pre_counts = HashMap::new();
        for block in blocks {
            merge_block_deltas(
                block,
                &table_configs,
                &mut merged_deltas,
                &mut skipped_tables,
                &mut pre_counts,
            );
        }

        assert!(skipped_tables.is_empty());
        let tracked = &merged_deltas["tracked"];
        assert_eq!(
            tracked.inserts[&vec![Cell::from("1")]],
            (vec![Cell::from("Alicia")], Some(timestamp(300)))
        );
        assert_eq!(
            tracked.inserts[&vec![Cell::from("2")]],
            (vec![Cell::from("Bob")], Some(timestamp(200)))
        );
        let untracked = &merged_deltas["untracked"];
        assert_eq!(
            untracked.inserts[&vec![Cell::from("1")]],
            (vec![Cell::from("Alicia")], None)
        );
    }
}
