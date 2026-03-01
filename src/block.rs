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
            .with_context(|| format!("Failed to load block '{:.7}...'", hash))?;
        let block = Block::decode(data.as_slice())
            .with_context(|| format!("Failed to decode block '{:.7}...'", hash))?;
        log::info!("Loaded block '{:.7}...'", hash);
        Ok(block)
    }

    pub fn create(config: &Config) -> Result<String> {
        let work_dir = &config.work_dir;
        let previous_state =
            state::State::load(work_dir).context("Failed to load previous state")?;
        let current_state =
            state::State::compute(config).context("Failed to compute current state")?;

        let parent_hash = head::load(work_dir).context("Failed to load head of chain")?;
        let created = Some(std::time::SystemTime::now().into());

        let deltas = delta::Delta::compute(previous_state, &current_state);
        let payload = deltas
            .into_iter()
            .map(crate::proto::delta::Delta::from)
            .collect();

        let block = Block {
            parent: parent_hash,
            created,
            payload,
        };
        log::debug!("{}", block);

        let mut encoded = Vec::new();
        block
            .encode(&mut encoded)
            .context("Failed to encode block")?;
        let hash = utils::compute_hash(&encoded);
        storage::store(work_dir, &hash, &encoded)
            .with_context(|| format!("Failed to store block {:.7}", hash))?;

        log::info!("Created block '{:.7}...'", hash);

        current_state
            .store(work_dir)
            .context("Failed to store current state")?;
        head::store(work_dir, &hash).context("Failed to update head of state")?;

        if let Err(e) = truncate::run(config) {
            log::warn!("Truncation failed (non-fatal): {}", e);
        }

        Ok(hash)
    }

    pub fn merge(mut self, mut child: Block) -> Result<Block> {
        for child_delta in child.payload.drain(..) {
            if let Some(parent_delta) = self
                .payload
                .iter_mut()
                .find(|d| d.table_name == child_delta.table_name)
            {
                let mut parent_domain: delta::Delta = std::mem::take(parent_delta).try_into()?;
                let child_domain: delta::Delta = child_delta.try_into()?;
                parent_domain
                    .merge(child_domain)
                    .context("Failed to merge deltas")?;
                *parent_delta = parent_domain.into();
            } else {
                self.payload.push(child_delta);
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
