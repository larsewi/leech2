pub use crate::proto::patch::Patch;

use crate::block::Block;
use crate::head;
use crate::utils::GENESIS_HASH;

impl Patch {
    pub fn create(final_hash: &str) -> Result<Patch, Box<dyn std::error::Error>> {
        let head_hash = head::load()?;
        let mut current_hash = head_hash.clone();
        let mut current_block: Option<Block> = None;
        let mut head_created = None;
        let mut num_blocks: u32 = 0;

        while current_hash != GENESIS_HASH && !current_hash.starts_with(final_hash) {
            let block = Block::load(&current_hash)?;
            let parent_hash = block.parent.clone();

            if head_created.is_none() {
                head_created = block.created.clone();
            }

            current_block = Some(match current_block {
                Some(prev) => block.merge(prev)?,
                None => block,
            });

            num_blocks += 1;
            current_hash = parent_hash;
        }

        log::info!("Reached final block '{:.7}...'", current_hash);
        if let Some(ref block) = current_block {
            log::debug!("Final merged block: {:#?}", block);
        }

        if !current_hash.starts_with(final_hash) {
            return Err(format!("Block starting with '{}' not found in chain", final_hash).into());
        }

        let final_block = Block::load(&current_hash)?;

        if head_created.is_none() {
            head_created = final_block.created.clone();
        }

        let patch = Patch {
            head_hash,
            head_created,
            final_hash: current_hash,
            final_created: final_block.created,
            num_blocks,
            payload: current_block
                .map(|b| b.payload)
                .ok_or("No blocks were merged")?,
        };

        log::debug!("Built patch: {:#?}", patch);

        Ok(patch)
    }
}
