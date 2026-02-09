pub use crate::proto::patch::Patch;

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
            match Block::load(&head_hash).and_then(|block| {
                let head_created = block.created.clone();
                let (num_blocks, deltas) = consolidate(block, last_known_hash)?;
                Ok((head_created, num_blocks, deltas))
            }) {
                Ok((head_created, num_blocks, deltas)) => (
                    head_created,
                    num_blocks,
                    Some(Payload::Deltas(Deltas { items: deltas })),
                ),
                Err(e) => {
                    log::warn!("Consolidation failed, falling back to full state: {}", e);
                    let state = state::State::load()?
                        .ok_or("Consolidation failed and no STATE file found for fallback")?;
                    (None, 0, Some(Payload::State(crate::proto::state::State::from(state))))
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
