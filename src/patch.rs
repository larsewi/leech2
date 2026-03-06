pub use crate::proto::patch::Patch;

use std::fmt;
use std::path::Path;

use anyhow::{Context, Result, bail};
use prost::Message;
use prost_types::Timestamp;

use crate::block::Block;
use crate::config::{Config, InjectedFieldConfig};
use crate::head;
use crate::proto::delta::Deltas;
use crate::proto::injected::Field;
use crate::proto::patch::patch::Payload;
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

type ConsolidateResult = (Option<Timestamp>, u32, Option<Payload>);

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
        match &self.payload {
            Some(Payload::Deltas(deltas)) => {
                write!(f, "\n  Payload ({} deltas):", deltas.items.len())?;
                for delta in &deltas.items {
                    write!(f, "\n    {}", utils::indent(&delta.to_string(), "    "))?;
                }
            }
            Some(Payload::State(state)) => {
                write!(f, "\n  Payload (full state):")?;
                write!(f, "\n    {}", utils::indent(&state.to_string(), "    "))?;
            }
            None => {
                write!(f, "\n  Payload: None")?;
            }
        }
        Ok(())
    }
}

fn consolidate(
    work_dir: &Path,
    head_block: Block,
    last_known: &str,
) -> Result<(u32, Vec<crate::proto::delta::Delta>)> {
    let mut current_hash = head_block.parent.clone();
    let mut current_block = head_block;
    let mut num_blocks: u32 = 1;

    while current_hash != GENESIS_HASH && !current_hash.starts_with(last_known) {
        let block = Block::load(work_dir, &current_hash)?;
        let parent_hash = block.parent.clone();
        current_block = block.merge(current_block)?;
        num_blocks += 1;
        current_hash = parent_hash;
    }

    if !current_hash.starts_with(last_known) {
        bail!("Block starting with '{}' not found in chain", last_known);
    }

    Ok((num_blocks, current_block.payload))
}

fn try_consolidate(work_dir: &Path, head: &str, last_known: &str) -> Result<ConsolidateResult> {
    let block = Block::load(work_dir, head)?;
    let created = block.created;

    if head.starts_with(last_known) {
        return Ok((created, 0, None));
    }

    let (num_blocks, mut deltas) = consolidate(work_dir, block, last_known)?;

    // Strip data the receiver doesn't need — patches are fully consolidated
    // so the receiver only needs keys + changed values.
    for delta in &mut deltas {
        // Deletes: receiver only needs the primary key, not the old row values.
        for delete in &mut delta.deletes {
            delete.value.clear();
        }
        for update in &mut delta.updates {
            update.sparse_encode();
        }
    }

    let deltas_payload = Deltas { items: deltas };
    let state = state::State::load(work_dir)?;
    let proto_state = state.map(crate::proto::state::State::from);

    let payload = match proto_state {
        Some(state_payload) if state_payload.encoded_len() < deltas_payload.encoded_len() => {
            log::info!("Using full state (smaller than consolidated deltas)");
            Payload::State(state_payload)
        }
        _ => Payload::Deltas(deltas_payload),
    };

    Ok((created, num_blocks, Some(payload)))
}

fn full_state_patch(work_dir: &Path, head: &str, injected_fields: Vec<Field>) -> Result<Patch> {
    let created = Block::load(work_dir, head)
        .ok()
        .and_then(|block| block.created);
    let state =
        state::State::load(work_dir)?.context("No STATE file found for full state patch")?;
    let patch = Patch {
        head: head.to_string(),
        created,
        injected_fields,
        num_blocks: 0,
        payload: Some(Payload::State(crate::proto::state::State::from(state))),
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

        if head == GENESIS_HASH {
            let patch = Patch {
                head,
                created: None,
                injected_fields,
                num_blocks: 0,
                payload: None,
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
                return full_state_patch(work_dir, &head, injected_fields);
            }
            Err(e) => {
                log::warn!(
                    "Reference block not found, producing full state patch: {}",
                    e
                );
                return full_state_patch(work_dir, &head, injected_fields);
            }
        };

        let (created, num_blocks, payload) = match try_consolidate(work_dir, &head, &last_known) {
            Ok((head_created, num_blocks, payload)) => (head_created, num_blocks, payload),
            Err(e) => {
                log::warn!("Consolidation failed, falling back to full state: {}", e);
                return full_state_patch(work_dir, &head, injected_fields);
            }
        };

        let patch = Patch {
            head,
            created,
            injected_fields,
            num_blocks,
            payload,
        };

        log::debug!("Built patch:\n{}", patch);
        Ok(patch)
    }
}
