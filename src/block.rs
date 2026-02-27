use std::fmt;
use std::path::Path;

use anyhow::{Context, Result};
use prost::Message;

use crate::config::Config;
use crate::delta;
use crate::head;
use crate::state;
use crate::storage;
use crate::truncate;
use crate::utils;

pub use crate::proto::block::Block;

impl fmt::Display for Block {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "Block:")?;
        write!(f, "\n  Parent: {}", self.parent)?;
        match &self.created {
            Some(ts) => write!(f, "\n  Created: {}", utils::format_timestamp(ts))?,
            None => write!(f, "\n  Created: N/A")?,
        }
        write!(f, "\n  Payload ({} deltas):", self.payload.len())?;
        for delta in &self.payload {
            write!(f, "\n    {}", utils::indent(&delta.to_string(), "    "))?;
        }
        Ok(())
    }
}

impl Block {
    pub fn load(work_dir: &Path, hash: &str) -> Result<Block> {
        let data = storage::load(work_dir, hash)?
            .with_context(|| format!("Block '{:.7}...' not found", hash))?;
        let block = Block::decode(data.as_slice())
            .with_context(|| format!("Failed to decode block '{:.7}...'", hash))?;
        log::info!("Loaded block '{:.7}...'", hash);
        Ok(block)
    }

    pub fn create(config: &Config) -> Result<String> {
        let work_dir = &config.work_dir;
        let previous_state = state::State::load(work_dir)?;
        let current_state = state::State::compute(config)?;

        let parent = head::load(work_dir)?;
        let created = Some(std::time::SystemTime::now().into());

        let deltas = delta::Delta::compute(previous_state, &current_state);
        let payload = deltas
            .into_iter()
            .map(crate::proto::delta::Delta::from)
            .collect();

        let block = Block {
            parent,
            created,
            payload,
        };
        log::debug!("{}", block);

        let mut buf = Vec::new();
        block.encode(&mut buf)?;
        let hash = utils::compute_hash(&buf);
        storage::save(work_dir, &hash, &buf)?;

        log::info!("Created block '{:.7}...'", hash);

        current_state.save(work_dir)?;
        head::save(work_dir, &hash)?;

        if let Err(e) = truncate::run(config) {
            log::warn!("Truncation failed (non-fatal): {}", e);
        }

        Ok(hash)
    }

    pub fn merge(mut self, mut other: Block) -> Result<Block> {
        for other_delta in other.payload.drain(..) {
            if let Some(self_delta) = self.payload.iter_mut().find(|d| d.name == other_delta.name) {
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
    fn test_encode_block() {
        let block = Block {
            created: Some(prost_types::Timestamp {
                seconds: 1700000000,
                nanos: 0,
            }),
            parent: "abc123".to_string(),
            payload: Vec::new(),
        };
        let mut buf = Vec::new();
        block.encode(&mut buf).unwrap();
        assert!(!buf.is_empty());

        // Verify roundtrip: decode should produce the same block
        let decoded = Block::decode(buf.as_slice()).unwrap();
        assert_eq!(decoded.created, block.created);
        assert_eq!(decoded.parent, block.parent);
    }
}
