use crate::block::{self, Block};
use crate::storage;

pub fn diff(final_hash: &str) -> Result<(), Box<dyn std::error::Error>> {
    log::debug!("diff(block={})", final_hash);

    let genesis = "0".repeat(40);
    let mut current_hash = storage::read_head()?;
    let mut current_block: Option<Block> = None;

    while current_hash != genesis && !current_hash.starts_with(final_hash) {
        let block = storage::read_block(&current_hash)?;
        let parent_hash = block.parent.clone();

        current_block = Some(match current_block {
            Some(prev) => block::merge_blocks(block, prev)?,
            None => block,
        });

        current_hash = parent_hash;
    }

    log::info!("Reached final block '{:.7}...'", current_hash);

    if !current_hash.starts_with(final_hash) {
        return Err(format!("Block starting with '{}' not found in chain", final_hash).into());
    }

    Ok(())
}
