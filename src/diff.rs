use crate::block::Block;
use crate::head;
use crate::utils::GENESIS_HASH;

pub fn diff(final_hash: &str) -> Result<(), Box<dyn std::error::Error>> {
    let mut current_hash = head::load()?;
    let mut current_block: Option<Block> = None;

    while current_hash != GENESIS_HASH && !current_hash.starts_with(final_hash) {
        let block = Block::load(&current_hash)?;
        let parent_hash = block.parent.clone();

        current_block = Some(match current_block {
            Some(prev) => block.merge(prev)?,
            None => block,
        });

        current_hash = parent_hash;
    }

    log::info!("Reached final block '{:.7}...'", current_hash);
    if let Some(ref block) = current_block {
        log::debug!("Final merged block: {:#?}", block);
    }

    if !current_hash.starts_with(final_hash) {
        return Err(format!("Block starting with '{}' not found in chain", final_hash).into());
    }

    Ok(())
}
