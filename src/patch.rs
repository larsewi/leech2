pub use crate::proto::patch::Patch;

use std::fmt;
use std::path::Path;

use anyhow::{Context, Result, bail};
use prost::Message;
use prost_types::Timestamp;

use crate::block::Block;
use crate::config::Config;
use crate::head;
use crate::proto::patch::Deltas;
use crate::proto::patch::patch::Payload;
use crate::state;
use crate::utils;
use crate::utils::GENESIS_HASH;

type ConsolidateResult = (Option<Timestamp>, u32, Option<Payload>);

impl fmt::Display for Patch {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "Patch:")?;
        write!(f, "\n  Head: {}", self.head_hash)?;
        match &self.head_created {
            Some(ts) => write!(f, "\n  Created: {}", utils::format_timestamp(ts))?,
            None => write!(f, "\n  Created: N/A")?,
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

fn consolidate(
    work_dir: &Path,
    head_block: Block,
    last_known_hash: &str,
) -> Result<(u32, Vec<crate::proto::delta::Delta>)> {
    let mut current_hash = head_block.parent.clone();
    let mut current_block = head_block;
    let mut num_blocks: u32 = 1;

    while current_hash != GENESIS_HASH && !current_hash.starts_with(last_known_hash) {
        let block = Block::load(work_dir, &current_hash)?;
        let parent_hash = block.parent.clone();
        current_block = Block::merge(block, current_block)?;
        num_blocks += 1;
        current_hash = parent_hash;
    }

    if !current_hash.starts_with(last_known_hash) {
        bail!(
            "Block starting with '{}' not found in chain",
            last_known_hash
        );
    }

    Ok((num_blocks, current_block.payload))
}

fn try_consolidate(
    work_dir: &Path,
    head_hash: &str,
    last_known_hash: &str,
) -> Result<ConsolidateResult> {
    let block = Block::load(work_dir, head_hash)?;
    let head_created = block.created;

    if head_hash.starts_with(last_known_hash) {
        return Ok((head_created, 0, None));
    }

    let (num_blocks, mut deltas) = consolidate(work_dir, block, last_known_hash)?;

    // Strip data the receiver doesn't need — patches are fully consolidated
    // so the receiver only needs keys + changed values.
    for delta in &mut deltas {
        // Deletes: receiver only needs the primary key, not the old row values.
        for delete in &mut delta.deletes {
            delete.value.clear();
        }
        // Updates: sparse-encode to changed_indices + new_value only.
        for update in &mut delta.updates {
            let mut changed_indices = Vec::new();
            let mut sparse_new = Vec::new();
            for (i, (o, n)) in update
                .old_value
                .iter()
                .zip(update.new_value.iter())
                .enumerate()
            {
                if o != n {
                    changed_indices.push(i as u32);
                    sparse_new.push(n.clone());
                }
            }
            update.changed_indices = changed_indices;
            update.old_value.clear();
            update.new_value = sparse_new;
        }
    }

    let deltas_payload = Deltas { items: deltas };
    let state = state::State::load(work_dir)?;
    let proto_state = state.map(crate::proto::state::State::from);

    let payload = match proto_state {
        Some(s) if s.encoded_len() < deltas_payload.encoded_len() => {
            log::info!("Using full state (smaller than consolidated deltas)");
            Payload::State(s)
        }
        _ => Payload::Deltas(deltas_payload),
    };

    Ok((head_created, num_blocks, Some(payload)))
}

impl Patch {
    pub fn create(config: &Config, last_known_hash: &str) -> Result<Patch> {
        let work_dir = &config.work_dir;
        resolve_hash_prefix(work_dir, last_known_hash)?;

        let head_hash = head::load(work_dir)?;

        if head_hash == GENESIS_HASH {
            let patch = Patch {
                head_hash,
                head_created: None,
                num_blocks: 0,
                payload: None,
            };
            log::debug!("Built patch:\n{}", patch);
            return Ok(patch);
        }

        let (head_created, num_blocks, payload) =
            match try_consolidate(work_dir, &head_hash, last_known_hash) {
                Ok((head_created, num_blocks, payload)) => (head_created, num_blocks, payload),
                Err(e) => {
                    log::warn!("Consolidation failed, falling back to full state: {}", e);
                    let state = state::State::load(work_dir)?
                        .context("Consolidation failed and no STATE file found for fallback")?;
                    (
                        None,
                        0,
                        Some(Payload::State(crate::proto::state::State::from(state))),
                    )
                }
            };

        let patch = Patch {
            head_hash,
            head_created,
            num_blocks,
            payload,
        };

        log::debug!("Built patch:\n{}", patch);
        Ok(patch)
    }
}
