use prost::Message;

use crate::delta;
use crate::head;
use crate::state;
use crate::storage;
use crate::utils;

pub use crate::proto::block::Block;

impl Block {
    pub fn load(hash: &str) -> Result<Block, Box<dyn std::error::Error>> {
        let data = storage::load(hash)?
            .ok_or_else(|| format!("Block '{:.7}...' not found", hash))?;
        let block = Block::decode(data.as_slice())
            .map_err(|e| format!("Failed to decode block '{:.7}...': {}", hash, e))?;
        log::info!("Loaded block '{:.7}...'", hash);
        Ok(block)
    }

    pub fn create() -> Result<String, Box<dyn std::error::Error>> {
        let previous_state = state::State::load()?;
        let current_state = state::State::compute()?;

        let parent = head::load()?;
        let timestamp = utils::get_timestamp()?;

        let deltas = delta::Delta::compute(previous_state, &current_state);
        let payload = deltas
            .into_iter()
            .map(crate::proto::delta::Delta::from)
            .collect();

        let block = Block {
            parent,
            timestamp,
            payload,
        };
        log::debug!("{:#?}", block);

        let mut buf = Vec::new();
        block.encode(&mut buf)?;
        let hash = utils::compute_hash(&buf);
        storage::save(&hash, &buf)?;

        log::info!("Created block '{:.7}...'", hash);

        head::save(&hash)?;
        current_state.save()?;

        Ok(hash)
    }

    pub fn merge(
        mut self,
        mut other: Block,
    ) -> Result<Block, Box<dyn std::error::Error>> {
        for other_delta in other.payload.drain(..) {
            if let Some(self_delta) = self
                .payload
                .iter_mut()
                .find(|d| d.name == other_delta.name)
            {
                let mut self_domain: delta::Delta = std::mem::take(self_delta).into();
                let other_domain: delta::Delta = other_delta.into();
                self_domain.merge(other_domain)?;
                *self_delta = self_domain.into();
            } else {
                self.payload.push(other_delta);
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
