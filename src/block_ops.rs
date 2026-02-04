use std::time::{SystemTime, UNIX_EPOCH};

use prost::Message;
use sha1::{Digest, Sha1};

use crate::block::Block;
use crate::storage;

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

pub fn commit_impl() -> Result<String, Box<dyn std::error::Error>> {
    let timestamp = get_timestamp()?;
    let parent = storage::read_head()?;

    let block = Block {
        version: 1,
        timestamp,
        parent,
    };

    let buf = encode_block(&block)?;
    let hash = compute_hash(&buf);

    storage::ensure_work_dir()?;
    storage::write_block(&hash, &buf)?;
    storage::write_head(&hash)?;

    log::info!(
        "commit: created block {} (version={}, timestamp={}, parent={})",
        hash,
        block.version,
        block.timestamp,
        block.parent
    );

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
            version: 1,
            timestamp: 1700000000,
            parent: "abc123".to_string(),
        };
        let result = encode_block(&block);
        assert!(result.is_ok());
        let encoded = result.unwrap();
        assert!(!encoded.is_empty());

        // Verify roundtrip: decode should produce the same block
        let decoded = Block::decode(encoded.as_slice()).unwrap();
        assert_eq!(decoded.version, block.version);
        assert_eq!(decoded.timestamp, block.timestamp);
        assert_eq!(decoded.parent, block.parent);
    }
}
