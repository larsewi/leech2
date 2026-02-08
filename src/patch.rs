pub use crate::proto::patch::Patch;

use crate::block::Block;
use crate::head;
use crate::utils::GENESIS_HASH;

impl Patch {
    pub fn create(last_known_hash: &str) -> Result<Patch, Box<dyn std::error::Error>> {
        let head_hash = head::load()?;
        let mut current_hash = head_hash.clone();
        let mut current_block: Option<Block> = None;
        let mut head_created = None;
        let mut num_blocks: u32 = 0;

        while current_hash != GENESIS_HASH && !current_hash.starts_with(last_known_hash) {
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

        if !current_hash.starts_with(last_known_hash) {
            return Err(format!("Block starting with '{}' not found in chain", last_known_hash).into());
        }

        let patch = Patch {
            head_hash,
            head_created,
            num_blocks,
            payload: current_block.map(|b| b.payload).unwrap_or_default(),
        };

        log::debug!("Built patch: {:#?}", patch);

        Ok(patch)
    }
}
