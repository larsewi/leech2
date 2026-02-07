use prost::Message;

use crate::delta;
use crate::head;
use crate::state;
use crate::storage;
use crate::utils;

pub use crate::proto::block::Block;

impl Block {
    pub fn new() -> Result<String, Box<dyn std::error::Error>> {
        log::debug!("Block::new()");

        let previous_state = state::State::load()?;
        let current_state = state::State::compute()?;
        let deltas = delta::Delta::compute(previous_state, &current_state);
        let payload = deltas
            .into_iter()
            .map(crate::proto::delta::Delta::from)
            .collect();

        let timestamp = utils::get_timestamp()?;
        let parent = head::load()?;

        let block = Block {
            parent,
            timestamp,
            payload,
        };
        log::debug!("{:#?}", block);

        let mut buf = Vec::new();
        block.encode(&mut buf)?;
        let hash = utils::compute_hash(&buf);

        log::info!("Created block '{:.7}...'", hash);

        storage::ensure_work_dir()?;
        storage::write_block(&hash, &buf)?;

        head::save(&hash)?;
        current_state.save()?;

        Ok(hash)
    }

    pub fn merge(
        mut self,
        mut other: Block,
    ) -> Result<Block, Box<dyn std::error::Error>> {
        log::debug!("Block::merge()");

        for current_delta in other.payload.drain(..) {
            if let Some(parent_delta) = self
                .payload
                .iter_mut()
                .find(|d| d.name == current_delta.name)
            {
                let mut parent_domain: delta::Delta = std::mem::take(parent_delta).into();
                let current_domain: delta::Delta = current_delta.into();
                parent_domain.merge(current_domain);
                *parent_delta = parent_domain.into();
            } else {
                self.payload.push(current_delta);
            }
        }

        Ok(self)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_get_timestamp() {
        let result = utils::get_timestamp();
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
        let mut buf = Vec::new();
        block.encode(&mut buf).unwrap();
        assert!(!buf.is_empty());

        // Verify roundtrip: decode should produce the same block
        let decoded = Block::decode(buf.as_slice()).unwrap();
        assert_eq!(decoded.timestamp, block.timestamp);
        assert_eq!(decoded.parent, block.parent);
    }
}
