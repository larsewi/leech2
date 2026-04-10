pub use crate::proto::patch::Patch;

use std::collections::{HashMap, HashSet};
use std::fmt;
use std::path::Path;

use anyhow::{Context, Result, bail};
use prost::Message;
use prost_types::Timestamp;

use crate::block::Block;
use crate::config::{Config, InjectedFieldConfig};
use crate::delta::Delta;
use crate::head;
use crate::proto::delta::Delta as ProtoDelta;
use crate::proto::injected::Field;
use crate::proto::table::Table as ProtoTable;
use crate::state;
use crate::utils;
use crate::utils::GENESIS_HASH;

impl From<&InjectedFieldConfig> for Field {
    fn from(config: &InjectedFieldConfig) -> Self {
        Field {
            name: config.name.clone(),
            sql_type: config.sql_type.clone(),
            value: config.value.clone(),
        }
    }
}

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
        for field in &self.injected_fields {
            write!(f, "\n  Injected: {} = {}", field.name, field.value)?;
        }
        write!(f, "\n  Blocks: {}", self.num_blocks)?;
        fmt_payload(&self.deltas, "Deltas", f)?;
        fmt_payload(&self.states, "States", f)?;
        if self.deltas.is_empty() && self.states.is_empty() {
            write!(f, "\n  Payload: None")?;
        }
        Ok(())
    }
}

/// Walk the chain from `head` back to (but not including) `last_known`,
/// collecting only the block hashes. Uses `Block::load_parent_hash` which
/// decodes only the block header, avoiding the heavier full-payload parse.
/// Returns hashes in chain order (newest-first); callers that need
/// oldest-first should reverse.
fn collect_block_hashes(work_dir: &Path, head: &str, last_known: &str) -> Result<Vec<String>> {
    let mut hashes = vec![head.to_string()];
    let mut parent = Block::load_parent_hash(work_dir, head)?;

    while parent != GENESIS_HASH && parent != last_known {
        hashes.push(parent.clone());
        parent = Block::load_parent_hash(work_dir, &parent)?;
    }

    if parent != last_known {
        bail!("block starting with '{}' not found in chain", last_known);
    }

    Ok(hashes)
}

/// Merge a single block's deltas into per-table running results. The block is
/// the older (parent) side and the running results are the newer (child) side,
/// so the merge direction is `parent.merge(child)`. When `running_deltas` is
/// empty (first block), this simply extracts the block's deltas.
///
/// Tables whose layout changed (delta is `None`) or whose merge failed are
/// added to `reset_tables` so they fall back to full state.
fn merge_block_deltas(
    block: Block,
    running_deltas: &mut HashMap<String, Delta>,
    reset_tables: &mut HashSet<String>,
) {
    for (name, change) in block.payload {
        if reset_tables.contains(&name) {
            continue;
        }
        match change.delta {
            Some(proto_delta) => {
                let result = Delta::try_from(proto_delta).and_then(|mut parent| {
                    if let Some(child) = running_deltas.remove(&name) {
                        parent.merge(child)?;
                    }
                    Ok(parent)
                });
                match result {
                    Ok(delta) => {
                        running_deltas.insert(name, delta);
                    }
                    Err(e) => {
                        log::warn!(
                            "Merge failed for table '{}', falling back to full state: {}",
                            name,
                            e
                        );
                        running_deltas.remove(&name);
                        reset_tables.insert(name);
                    }
                }
            }
            None => {
                running_deltas.remove(&name);
                reset_tables.insert(name);
            }
        }
    }
}

type ConsolidateResult = (
    Option<Timestamp>,
    u32,
    HashMap<String, ProtoDelta>,
    HashMap<String, ProtoTable>,
);

fn try_consolidate(work_dir: &Path, head: &str, last_known: &str) -> Result<ConsolidateResult> {
    let head_block = Block::load(work_dir, head)?;
    let created = head_block.created;

    if head.starts_with(last_known) {
        return Ok((created, 0, HashMap::new(), HashMap::new()));
    }

    // Collect block hashes by walking the chain newest-to-oldest. Only the
    // block header is decoded per block (not the full payload).
    let mut block_hashes = collect_block_hashes(work_dir, head, last_known)?;
    let num_blocks = block_hashes.len() as u32;
    block_hashes.reverse();

    // Load blocks one at a time oldest-first, merging deltas incrementally.
    // Only one block's payload and the per-table running results are in
    // memory at a time.
    let mut running_deltas: HashMap<String, Delta> = HashMap::new();
    let mut reset_tables: HashSet<String> = HashSet::new();

    for (index, hash) in block_hashes.iter().enumerate() {
        log::trace!(
            "Merging block {}/{}: '{:.7}...'",
            index + 1,
            num_blocks,
            hash
        );
        let block = Block::load(work_dir, hash)?;
        merge_block_deltas(block, &mut running_deltas, &mut reset_tables);
    }

    // Load state for per-table size comparison and fallback.
    let state = state::State::load(work_dir)?;
    let state_tables: HashMap<String, ProtoTable> = state.map(|s| s.into()).unwrap_or_default();

    let mut result_deltas = HashMap::new();
    let mut result_states = HashMap::new();

    // Tables marked for reset go directly to full state.
    for table_name in &reset_tables {
        if let Some(state_table) = state_tables.get(table_name) {
            log::info!("Table '{}': using full state (layout changed)", table_name);
            result_states.insert(table_name.clone(), state_table.clone());
        } else {
            log::warn!("Table '{}' not in STATE file, skipping", table_name);
        }
    }

    for (table_name, merged) in running_deltas {
        let mut merged_delta = ProtoDelta::from(merged);

        // Strip data the receiver doesn't need.
        for delete in &mut merged_delta.deletes {
            delete.value.clear();
        }
        for update in &mut merged_delta.updates {
            update.sparse_encode();
        }

        // Per-table size comparison: use full state if it's smaller.
        if let Some(state_table) = state_tables.get(&table_name)
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

    Ok((created, num_blocks, result_deltas, result_states))
}

fn full_state_patch(
    work_dir: &Path,
    head: &str,
    injected_fields: Vec<Field>,
    field_hashes: HashMap<String, String>,
) -> Result<Patch> {
    let created = Block::load(work_dir, head)
        .ok()
        .and_then(|block| block.created);
    let state =
        state::State::load(work_dir)?.context("no STATE file found for full state patch")?;
    let patch = Patch {
        head: head.to_string(),
        created,
        injected_fields,
        num_blocks: 0,
        deltas: HashMap::new(),
        states: state.into(),
        field_hashes,
    };
    log::trace!("Built patch:\n{}", patch);
    Ok(patch)
}

impl Patch {
    pub fn create(config: &Config, last_known: &str) -> Result<Patch> {
        let work_dir = &config.work_dir;

        let resolved = crate::storage::resolve_hash_prefix(work_dir, last_known);

        let head = head::load(work_dir)?;

        let injected_fields: Vec<Field> = config.injected_fields.iter().map(Field::from).collect();

        let field_hashes: HashMap<String, String> = config
            .tables
            .iter()
            .map(|(name, table_config)| (name.clone(), table_config.field_hash()))
            .collect();

        if head == GENESIS_HASH {
            let patch = Patch {
                head,
                created: None,
                injected_fields,
                num_blocks: 0,
                deltas: HashMap::new(),
                states: HashMap::new(),
                field_hashes,
            };
            log::trace!("Built patch:\n{}", patch);
            return Ok(patch);
        }

        // If the reference block can't be resolved or is genesis, produce a
        // full STATE payload (TRUNCATE + INSERT) which is always safe to apply
        // regardless of current database contents.
        let last_known = match resolved {
            Ok(hash) if hash != GENESIS_HASH => hash,
            Ok(_) => {
                log::info!("Reference is genesis, producing full state patch");
                return full_state_patch(work_dir, &head, injected_fields, field_hashes);
            }
            Err(e) => {
                log::warn!(
                    "Reference block not found, producing full state patch: {}",
                    e
                );
                return full_state_patch(work_dir, &head, injected_fields, field_hashes);
            }
        };

        let (created, num_blocks, deltas, states) =
            match try_consolidate(work_dir, &head, &last_known) {
                Ok(result) => result,
                Err(e) => {
                    log::warn!("Consolidation failed, falling back to full state: {}", e);
                    return full_state_patch(work_dir, &head, injected_fields, field_hashes);
                }
            };

        let patch = Patch {
            head,
            created,
            injected_fields,
            num_blocks,
            deltas,
            states,
            field_hashes,
        };

        log::trace!("Built patch:\n{}", patch);
        Ok(patch)
    }
}
