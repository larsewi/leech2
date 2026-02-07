use std::time::{SystemTime, UNIX_EPOCH};

use prost::Message;
use sha1::{Digest, Sha1};

use crate::delta;
use crate::state;
use crate::storage;

pub use crate::proto::block::Block;

fn get_timestamp() -> Result<i32, &'static str> {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .map(|d| d.as_secs() as i32)
        .map_err(|_| "system time before UNIX epoch")
}

fn encode_block(block: &Block) -> Result<Vec<u8>, prost::EncodeError> {
    let mut buf = Vec::new();
    block.encode(&mut buf)?;
    Ok(buf)
}

fn compute_hash(data: &[u8]) -> String {
    let mut hasher = Sha1::new();
    hasher.update(data);
    format!("{:x}", hasher.finalize())
}

pub fn merge_blocks(
    mut parent: Block,
    mut current: Block,
) -> Result<Block, Box<dyn std::error::Error>> {
    log::debug!("merge_blocks()");

    for current_delta in current.payload.drain(..) {
        if let Some(parent_delta) = parent
            .payload
            .iter_mut()
            .find(|d| d.name == current_delta.name)
        {
            let mut parent_domain: delta::Delta = std::mem::take(parent_delta).into();
            let current_domain: delta::Delta = current_delta.into();
            parent_domain.merge(current_domain);
            *parent_delta = parent_domain.into();
        } else {
            parent.payload.push(current_delta);
        }
    }

    Ok(parent)
}

pub fn commit() -> Result<String, Box<dyn std::error::Error>> {
    log::debug!("commit()");

    let previous_state = state::load_previous_state()?;
    let current_state = state::load_current_state()?;
    let deltas = delta::Delta::compute(previous_state, &current_state);
    let payload = deltas
        .into_iter()
        .map(crate::proto::delta::Delta::from)
        .collect();

    let timestamp = get_timestamp()?;
    let parent = storage::read_head()?;

    let block = Block {
        parent,
        timestamp,
        payload,
    };
    log::debug!("{:#?}", block);

    let buf = encode_block(&block)?;
    let hash = compute_hash(&buf);

    log::info!("Created block '{:.7}...'", hash);

    storage::ensure_work_dir()?;
    storage::write_block(&hash, &buf)?;

    storage::write_head(&hash)?;
    state::save_state(&current_state)?;

    Ok(hash)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_get_timestamp() {
        let result = get_timestamp();
        assert!(result.is_ok());
        let timestamp = result.unwrap();
        assert!(timestamp > 1577836800, "timestamp should be after 2020");
    }

    #[test]
    fn test_encode_block() {
        let block = Block {
            timestamp: 1700000000,
            parent: "abc123".to_string(),
            payload: Vec::new(),
        };
        let result = encode_block(&block);
        assert!(result.is_ok());
        let encoded = result.unwrap();
        assert!(!encoded.is_empty());

        // Verify roundtrip: decode should produce the same block
        let decoded = Block::decode(encoded.as_slice()).unwrap();
        assert_eq!(decoded.timestamp, block.timestamp);
        assert_eq!(decoded.parent, block.parent);
    }
}
