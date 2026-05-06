use std::collections::HashMap;
use std::fmt;
use std::path::Path;
use std::time::SystemTime;

use anyhow::{Context, Result};
use prost::Message;

use crate::config::Config;
use crate::delta;
use crate::head;
use crate::proto::block::{BlockHeader, TableChange};
use crate::proto::delta::Delta as ProtoDelta;
use crate::state;
use crate::storage;
use crate::truncate;
use crate::utils;

pub use crate::proto::block::Block;

impl From<Option<delta::Delta>> for TableChange {
    fn from(delta: Option<delta::Delta>) -> Self {
        TableChange {
            delta: delta.map(ProtoDelta::from),
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

/// Read the block file at `hash` and decode it as `T`. `kind` names what is
/// being decoded (e.g. `"block"`, `"block header"`) and appears in log and
/// error messages.
fn load_decode<T: Message + Default>(work_dir: &Path, hash: &str, kind: &str) -> Result<T> {
    let data = storage::load(work_dir, hash)?
        .with_context(|| format!("failed to load block '{:.7}...'", hash))?;
    let value = T::decode(data.as_slice())
        .with_context(|| format!("failed to decode {} '{:.7}...'", kind, hash))?;
    log::info!("Loaded {} '{:.7}...'", kind, hash);
    Ok(value)
}

impl Block {
    pub fn load(work_dir: &Path, hash: &str) -> Result<Block> {
        load_decode(work_dir, hash, "block")
    }

    /// Load the block header (parent hash + created timestamp) without
    /// decoding the full payload. Reads the block file and decodes it as a
    /// [`BlockHeader`], which shares field tags with [`Block`] — prost skips
    /// the unknown payload field so only the parent hash and timestamp are
    /// deserialized.
    pub fn load_header(work_dir: &Path, hash: &str) -> Result<BlockHeader> {
        load_decode(work_dir, hash, "block header")
    }

    /// Load only the parent hash from a block by decoding just the header,
    /// avoiding the heavier full-payload parse.
    pub fn load_parent_hash(work_dir: &Path, hash: &str) -> Result<String> {
        Ok(Self::load_header(work_dir, hash)?.parent)
    }

    pub fn create(config: &Config) -> Result<String> {
        let work_dir = &config.work_dir;
        let current_state =
            state::State::compute(config).context("failed to compute current state")?;

        let parent_hash = head::load(work_dir).context("failed to load head of chain")?;

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
                state::State::load(work_dir).context("failed to load previous state")?;

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
        log::trace!("{}", block);

        let mut encoded = Vec::new();
        block
            .encode(&mut encoded)
            .context("failed to encode block")?;
        let hash = utils::compute_hash(&encoded);
        storage::store(work_dir, &hash, &encoded)
            .with_context(|| format!("failed to store block {:.7}", hash))?;

        log::info!("Created block '{:.7}...'", hash);

        current_state
            .store(work_dir)
            .context("failed to store current state")?;
        head::store(work_dir, &hash).context("failed to update head of state")?;

        if let Err(e) = truncate::run(config) {
            log::warn!("Truncation failed (non-fatal): {}", e);
        }

        Ok(hash)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn dummy_block() -> Block {
        Block {
            parent: "deadbeef".to_string(),
            created: Some(prost_types::Timestamp {
                seconds: 1700000000,
                nanos: 0,
            }),
            payload: HashMap::new(),
        }
    }

    #[test]
    fn test_block_encode_decode() {
        let block = dummy_block();
        let mut buf = Vec::new();
        block.encode(&mut buf).unwrap();
        assert!(!buf.is_empty());

        // Verify roundtrip: decode should produce the same block
        let decoded = Block::decode(buf.as_slice()).unwrap();
        assert_eq!(decoded.parent, block.parent);
        assert_eq!(decoded.created, block.created);
    }

    #[test]
    fn test_block_bytes_decode_as_header() {
        let block = dummy_block();
        let mut buf = Vec::new();
        block.encode(&mut buf).unwrap();

        let header = BlockHeader::decode(buf.as_slice()).unwrap();
        assert_eq!(header.parent, block.parent);
        assert_eq!(header.created, block.created);
    }

    #[test]
    fn test_block_display() {
        let block = dummy_block();
        let expected = "Block:
  Parent: deadbeef
  Created: 2023-11-14 22:13:20 UTC
  Payload (0 tables):";
        assert_eq!(block.to_string(), expected);
    }
}
