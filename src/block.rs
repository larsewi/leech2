use std::collections::HashMap;
use std::fmt;
use std::path::Path;
use std::time::SystemTime;

use anyhow::{Context, Result, bail};
use prost::Message;

use crate::callbacks::Callbacks;
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

impl Block {
    pub fn load(work_dir: &Path, hash: &str, mode: u32) -> Result<Block> {
        let Some(data) = storage::load(work_dir, hash, mode)? else {
            bail!("failed to load block '{:.7}...'", hash);
        };
        let block = Block::decode(data.as_slice())
            .with_context(|| format!("failed to decode block '{:.7}...'", hash))?;
        log::debug!("Loaded block '{:.7}...'", hash);
        Ok(block)
    }

    /// Load the block header (parent hash + created timestamp) without
    /// decoding the full payload. Reads the block file and decodes it as a
    /// [`BlockHeader`], which shares field tags with [`Block`] — prost skips
    /// the unknown payload field so only the parent hash and timestamp are
    /// deserialized.
    pub fn load_header(work_dir: &Path, hash: &str, mode: u32) -> Result<BlockHeader> {
        let Some(data) = storage::load(work_dir, hash, mode)? else {
            bail!("failed to load block '{:.7}...'", hash);
        };
        let header = BlockHeader::decode(data.as_slice())
            .with_context(|| format!("failed to decode block header '{:.7}...'", hash))?;
        log::debug!("Loaded block header '{:.7}...'", hash);
        Ok(header)
    }

    /// Build a new block from `config`. Callback-backed tables are pulled
    /// through `callbacks`. Pass `None` when every table in `config` is
    /// CSV-backed.
    ///
    /// The write window — store the new block file, then store STATE, then
    /// advance HEAD — is held under an exclusive lock on `.chain.lock` so a
    /// concurrent truncation cannot observe the new block file before HEAD
    /// points at it (which would orphan-mark and delete it). After HEAD
    /// advances, truncation is kicked off on a background thread; use
    /// [`truncate::wait_for_pending`] to observe its completion.
    pub fn create(config: &Config, callbacks: Option<&Callbacks>) -> Result<String> {
        let state_dir = config.ensure_state_dir()?;
        let file_mode = config.file_mode;
        let current_state =
            state::State::compute(config, callbacks).context("failed to compute current state")?;

        let parent_hash =
            head::load(&state_dir, file_mode).context("failed to load head of chain")?;

        let created = Some(SystemTime::now().into());

        // When starting a fresh chain (HEAD is genesis), store an empty payload.
        // The first block's deltas are never used during patch creation: a genesis
        // reference always produces a full state patch from the STATE file, and
        // non-genesis references exclude the first block from consolidation.
        // Any stale STATE file left from a previous run is also ignored.
        let payload = if parent_hash == utils::GENESIS_HASH {
            HashMap::new()
        } else {
            let previous_state = state::State::load(&state_dir, file_mode)
                .context("failed to load previous state")?;

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
        let mut encoded = Vec::new();
        block
            .encode(&mut encoded)
            .context("failed to encode block")?;
        let hash = utils::compute_hash(&encoded);

        let chain_lock = storage::acquire_lock(&state_dir, "chain", true, file_mode)
            .context("failed to acquire chain lock")?;

        storage::store(&state_dir, &hash, &encoded, file_mode, config.dry_run)
            .with_context(|| format!("failed to store block {:.7}", hash))?;

        current_state
            .store(&state_dir, file_mode, config.dry_run)
            .context("failed to store current state")?;
        head::store(&state_dir, &hash, file_mode, config.dry_run)
            .context("failed to update head of state")?;

        drop(chain_lock);

        if !config.dry_run {
            log::info!("Created block '{:.7}...': {}", hash, block);
        }

        // In dry-run this reports what truncation would remove; otherwise it
        // kicks off the real cleanup on a background thread.
        truncate::spawn_background(config);

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
