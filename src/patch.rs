pub use crate::proto::patch::Patch;

use prost::Message;
use prost_types::Timestamp;

use crate::block::Block;
use crate::head;
use crate::proto::patch::patch::Payload;
use crate::proto::patch::Deltas;
use crate::state;
use crate::utils::GENESIS_HASH;

fn consolidate(
    head_block: Block,
    last_known_hash: &str,
) -> Result<(u32, Vec<crate::proto::delta::Delta>), Box<dyn std::error::Error>> {
    let mut current_hash = head_block.parent.clone();
    let mut current_block = head_block;
    let mut num_blocks: u32 = 1;

    while current_hash != GENESIS_HASH && !current_hash.starts_with(last_known_hash) {
        let block = Block::load(&current_hash)?;
        let parent_hash = block.parent.clone();
        current_block = block.merge(current_block)?;
        num_blocks += 1;
        current_hash = parent_hash;
    }

    if !current_hash.starts_with(last_known_hash) {
        return Err(format!(
            "Block starting with '{}' not found in chain",
            last_known_hash
        )
        .into());
    }

    Ok((num_blocks, current_block.payload))
}

fn load_state_payload() -> Result<Payload, Box<dyn std::error::Error>> {
    let state = state::State::load()?
        .ok_or("No STATE file found")?;
    Ok(Payload::State(crate::proto::state::State::from(state)))
}

fn try_consolidate(
    head_hash: &str,
    last_known_hash: &str,
) -> Result<(Option<Timestamp>, u32, Payload), Box<dyn std::error::Error>> {
    let block = Block::load(head_hash)?;
    let head_created = block.created.clone();
    let (num_blocks, deltas) = consolidate(block, last_known_hash)?;

    let deltas_payload = Deltas { items: deltas };
    let state = state::State::load()?;
    let proto_state = state.map(crate::proto::state::State::from);

    let payload = match proto_state {
        Some(s) if s.encoded_len() < deltas_payload.encoded_len() => {
            log::info!("Using full state (smaller than consolidated deltas)");
            Payload::State(s)
        }
        _ => Payload::Deltas(deltas_payload),
    };

    Ok((head_created, num_blocks, payload))
}

impl Patch {
    pub fn create(last_known_hash: &str) -> Result<Patch, Box<dyn std::error::Error>> {
        let head_hash = head::load()?;

        if head_hash == GENESIS_HASH || head_hash.starts_with(last_known_hash) {
            let patch = Patch {
                head_hash,
                head_created: None,
                num_blocks: 0,
                payload: None,
            };
            log::debug!("Built patch: {:#?}", patch);
            return Ok(patch);
        }

        let (head_created, num_blocks, payload) =
            match try_consolidate(&head_hash, last_known_hash) {
                Ok((head_created, num_blocks, payload)) => {
                    (head_created, num_blocks, Some(payload))
                }
                Err(e) => {
                    log::warn!("Consolidation failed, falling back to full state: {}", e);
                    (None, 0, Some(load_state_payload()?))
                }
            };

        let patch = Patch {
            head_hash,
            head_created,
            num_blocks,
            payload,
        };

        log::debug!("Built patch: {:#?}", patch);
        Ok(patch)
    }
}
