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
use crate::proto::injected::Field;
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

impl fmt::Display for Patch {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "Patch:")?;
        write!(f, "\n  Head: {}", self.head)?;
        match &self.created {
            Some(timestamp) => write!(f, "\n  Created: {}", utils::format_timestamp(timestamp))?,
            None => write!(f, "\n  Created: N/A")?,
        }
        for field in &self.injected_fields {
            write!(f, "\n  Injected: {} = {}", field.name, field.value)?;
        }
        write!(f, "\n  Blocks: {}", self.num_blocks)?;
        if !self.deltas.is_empty() {
            write!(f, "\n  Deltas ({}):", self.deltas.len())?;
            for (name, delta) in &self.deltas {
                write!(
                    f,
                    "\n    '{}' {}",
                    name,
                    utils::indent(&delta.to_string(), "    ")
                )?;
            }
        }
        if !self.states.is_empty() {
            write!(f, "\n  States ({}):", self.states.len())?;
            for (name, table) in &self.states {
                write!(
                    f,
                    "\n    '{}' {}",
                    name,
                    utils::indent(&table.to_string(), "    ")
                )?;
            }
        }
        if self.deltas.is_empty() && self.states.is_empty() {
            write!(f, "\n  Payload: None")?;
        }
        Ok(())
    }
}

/// Walk the chain from `head_block` back to (but not including) `last_known`,
/// returning blocks oldest-first.
fn collect_blocks(work_dir: &Path, head_block: Block, last_known: &str) -> Result<Vec<Block>> {
    let mut blocks = vec![head_block];
    let mut current_hash = blocks[0].parent.clone();

    while current_hash != GENESIS_HASH && !current_hash.starts_with(last_known) {
        let block = Block::load(work_dir, &current_hash)?;
        current_hash = block.parent.clone();
        blocks.push(block);
    }

    if !current_hash.starts_with(last_known) {
        bail!("Block starting with '{}' not found in chain", last_known);
    }

    blocks.reverse(); // oldest first
    Ok(blocks)
}

/// Merge a single table's proto deltas (oldest-first) into one consolidated delta.
fn merge_table_deltas(
    table_name: &str,
    deltas: Vec<crate::proto::delta::Delta>,
) -> Result<crate::proto::delta::Delta> {
    let mut iter = deltas.into_iter();
    let first = iter.next().context("no deltas to merge")?;
    let mut merged = Delta::try_from(first)?;

    for proto_delta in iter {
        let child = Delta::try_from(proto_delta)?;
        merged.merge(child, table_name)?;
    }

    Ok(crate::proto::delta::Delta::from(merged))
}

type ConsolidateResult = (
    Option<Timestamp>,
    u32,
    HashMap<String, crate::proto::delta::Delta>,
    HashMap<String, crate::proto::table::Table>,
);

fn try_consolidate(work_dir: &Path, head: &str, last_known: &str) -> Result<ConsolidateResult> {
    let head_block = Block::load(work_dir, head)?;
    let created = head_block.created;

    if head.starts_with(last_known) {
        return Ok((created, 0, HashMap::new(), HashMap::new()));
    }

    let blocks = collect_blocks(work_dir, head_block, last_known)?;
    let num_blocks = blocks.len() as u32;

    // Collect deltas by table name and track tables that need full state.
    let mut table_deltas: HashMap<String, Vec<crate::proto::delta::Delta>> = HashMap::new();
    let mut reset_tables: HashSet<String> = HashSet::new();

    for block in blocks {
        for (name, change) in block.payload {
            match change.delta {
                Some(delta) => {
                    table_deltas.entry(name).or_default().push(delta);
                }
                None => {
                    reset_tables.insert(name);
                }
            }
        }
    }

    // Load state for per-table size comparison and fallback.
    let state = state::State::load(work_dir)?;
    let state_tables: HashMap<String, crate::proto::table::Table> = state
        .map(|s| crate::proto::state::State::from(s).tables)
        .unwrap_or_default();

    let mut result_deltas = HashMap::new();
    let mut result_states = HashMap::new();

    // Tables marked for reset go directly to full state.
    for table_name in &reset_tables {
        table_deltas.remove(table_name);
        if let Some(state_table) = state_tables.get(table_name) {
            log::info!("Table '{}': using full state (layout changed)", table_name);
            result_states.insert(table_name.clone(), state_table.clone());
        } else {
            log::warn!("Table '{}' not in STATE file, skipping", table_name);
        }
    }

    for (table_name, deltas) in table_deltas {
        match merge_table_deltas(&table_name, deltas) {
            Ok(mut merged_delta) => {
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
            Err(e) => {
                log::warn!(
                    "Merge failed for table '{}', falling back to full state: {}",
                    table_name,
                    e
                );
                if let Some(state_table) = state_tables.get(&table_name) {
                    result_states.insert(table_name, state_table.clone());
                } else {
                    log::warn!("Table '{}' not in STATE file, skipping", table_name);
                }
            }
        }
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
        state::State::load(work_dir)?.context("No STATE file found for full state patch")?;
    let proto_state = crate::proto::state::State::from(state);
    let patch = Patch {
        head: head.to_string(),
        created,
        injected_fields,
        num_blocks: 0,
        deltas: HashMap::new(),
        states: proto_state.tables,
        field_hashes,
    };
    log::debug!("Built patch:\n{}", patch);
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
            log::debug!("Built patch:\n{}", patch);
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

        log::debug!("Built patch:\n{}", patch);
        Ok(patch)
    }
}
