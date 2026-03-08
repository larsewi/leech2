use std::collections::HashMap;
use std::fmt;
use std::path::Path;
use std::time::SystemTime;

use anyhow::{Context, Result};
use prost::Message;

use crate::config::Config;
use crate::delta;
use crate::head;
use crate::proto::block::TableChange;
use crate::state;
use crate::storage;
use crate::truncate;
use crate::utils;

pub use crate::proto::block::Block;

impl From<Option<delta::Delta>> for TableChange {
    fn from(delta: Option<delta::Delta>) -> Self {
        TableChange {
            delta: delta.map(crate::proto::delta::Delta::from),
        }
    }
}

impl fmt::Display for Block {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "Block:")?;
        write!(f, "\n  Parent: {}", self.parent)?;
        match &self.created {
            Some(ts) => write!(f, "\n  Created: {}", utils::format_timestamp(ts))?,
            None => write!(f, "\n  Created: N/A")?,
        }
        write!(f, "\n  Payload ({} tables):", self.payload.len())?;
        for (name, change) in &self.payload {
            match &change.delta {
                Some(delta) => write!(
                    f,
                    "\n    '{}' {}",
                    name,
                    utils::indent(&delta.to_string(), "    ")
                )?,
                None => write!(f, "\n    '{}' <layout changed>", name)?,
            }
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
        let current_state =
            state::State::compute(config).context("Failed to compute current state")?;

        let parent_hash = head::load(work_dir).context("Failed to load head of chain")?;

        let created = Some(SystemTime::now().into());

        // When starting a fresh chain (HEAD is genesis), store an empty payload.
        // The first block's deltas are never used during patch creation: a genesis
        // reference always produces a full state patch from the STATE file, and
        // non-genesis references exclude the first block from consolidation.
        // Any stale STATE file left from a previous run is also ignored.
        let payload = if parent_hash == utils::GENESIS_HASH {
            HashMap::new()
        } else {
            let previous_state =
                state::State::load(work_dir).context("Failed to load previous state")?;

            delta::Delta::compute(previous_state, &current_state)
                .into_iter()
                .map(|(name, delta)| (name, TableChange::from(delta)))
                .collect()
        };

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
            payload: HashMap::new(),
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
