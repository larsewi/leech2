pub use crate::proto::patch::Patch;

use std::fmt;
use std::path::Path;

use anyhow::{Context, Result, bail};
use prost::Message;
use prost_types::Timestamp;

use crate::block::Block;
use crate::config::{Config, HostConfig};
use crate::head;
use crate::proto::delta::Deltas;
use crate::proto::host::Host;
use crate::proto::patch::patch::Payload;
use crate::state;
use crate::utils;
use crate::utils::GENESIS_HASH;

impl From<&HostConfig> for Host {
    fn from(h: &HostConfig) -> Self {
        Host {
            name: h.name.clone(),
            r#type: h.field_type.clone(),
            value: h.value.clone(),
            format: h.format.clone().unwrap_or_default(),
        }
    }
}

type ConsolidateResult = (Option<Timestamp>, u32, Option<Payload>);

impl fmt::Display for Patch {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "Patch:")?;
        write!(f, "\n  Head: {}", self.head)?;
        match &self.created {
            Some(ts) => write!(f, "\n  Created: {}", utils::format_timestamp(ts))?,
            None => write!(f, "\n  Created: N/A")?,
        }
        if let Some(host) = &self.host {
            write!(f, "\n  Host: {} = {}", host.name, host.value)?;
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
    last_known_hash: &str,
) -> Result<(u32, Vec<crate::proto::delta::Delta>)> {
    let mut current_hash = head_block.parent.clone();
    let mut current_block = head_block;
    let mut num_blocks: u32 = 1;

    while current_hash != GENESIS_HASH && !current_hash.starts_with(last_known_hash) {
        let block = Block::load(work_dir, &current_hash)?;
        let parent_hash = block.parent.clone();
        current_block = block.merge(current_block)?;
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

    Ok((head_created, num_blocks, Some(payload)))
}

fn full_state_patch(work_dir: &Path, head_hash: &str, host: Option<Host>) -> Result<Patch> {
    let head_created = Block::load(work_dir, head_hash)
        .ok()
        .and_then(|b| b.created);
    let state =
        state::State::load(work_dir)?.context("No STATE file found for full state patch")?;
    let patch = Patch {
        head: head_hash.to_string(),
        created: head_created,
        host,
        num_blocks: 0,
        payload: Some(Payload::State(crate::proto::state::State::from(state))),
    };
    log::debug!("Built patch:\n{}", patch);
    Ok(patch)
}

impl Patch {
    pub fn create(config: &Config, last_known_hash: &str) -> Result<Patch> {
        let work_dir = &config.work_dir;

        let resolved = crate::storage::resolve_hash_prefix(work_dir, last_known_hash);

        let head_hash = head::load(work_dir)?;

        let host = config.host.as_ref().map(Host::from);

        if head_hash == GENESIS_HASH {
            let patch = Patch {
                head: head_hash,
                created: None,
                host,
                num_blocks: 0,
                payload: None,
            };
            log::debug!("Built patch:\n{}", patch);
            return Ok(patch);
        }

        // If the reference block can't be resolved or is genesis, produce a
        // full STATE payload (TRUNCATE + INSERT) which is always safe to apply
        // regardless of current database contents.
        let last_known_hash = match resolved {
            Ok(hash) if hash != GENESIS_HASH => hash,
            Ok(_) => {
                log::info!("Reference is genesis, producing full state patch");
                return full_state_patch(work_dir, &head_hash, host);
            }
            Err(e) => {
                log::warn!(
                    "Reference block not found, producing full state patch: {}",
                    e
                );
                return full_state_patch(work_dir, &head_hash, host);
            }
        };

        let (head_created, num_blocks, payload) =
            match try_consolidate(work_dir, &head_hash, &last_known_hash) {
                Ok((head_created, num_blocks, payload)) => (head_created, num_blocks, payload),
                Err(e) => {
                    log::warn!("Consolidation failed, falling back to full state: {}", e);
                    return full_state_patch(work_dir, &head_hash, host);
                }
            };

        let patch = Patch {
            head: head_hash,
            created: head_created,
            host,
            num_blocks,
            payload,
        };

        log::debug!("Built patch:\n{}", patch);
        Ok(patch)
    }
}
