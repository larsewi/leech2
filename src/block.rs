use std::fmt;
use std::path::Path;

use anyhow::{Context, Result};
use prost::Message;

use crate::config::Config;
use crate::head;
use crate::proto::block::TableChange;
use crate::state;
use crate::state::State;
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

/// Check which tables had their field layout changed since the previous state
/// and return the set of table names that need a full state snapshot.
fn detect_layout_changes(previous: &State, config: &Config) -> Vec<String> {
    let mut changed = Vec::new();
    for (name, table) in &previous.tables {
        if let Some(table_config) = config.tables.get(name) {
            let expected_fields = table_config.ordered_field_names();
            if table.fields != expected_fields {
                log::warn!(
                    "Table '{}': field layout changed, will use full state",
                    name
                );
                changed.push(name.clone());
            }
        }
    }
    changed
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

        let created = Some(std::time::SystemTime::now().into());

        // When starting a fresh chain (HEAD is genesis), store an empty payload.
        // The first block's deltas are never used during patch creation: a genesis
        // reference always produces a full state patch from the STATE file, and
        // non-genesis references exclude the first block from consolidation.
        // Any stale STATE file left from a previous run is also ignored.
        let payload = if parent_hash == utils::GENESIS_HASH {
            std::collections::HashMap::new()
        } else {
            let previous_state =
                state::State::load(work_dir).context("Failed to load previous state")?;
            let layout_changed_tables = previous_state
                .as_ref()
                .map(|s| detect_layout_changes(s, config))
                .unwrap_or_default();

            let deltas = crate::delta::Delta::compute(previous_state, &current_state);
            let mut payload = deltas
                .into_iter()
                .map(|(name, delta)| {
                    (
                        name,
                        TableChange {
                            delta: Some(crate::proto::delta::Delta::from(delta)),
                        },
                    )
                })
                .collect::<std::collections::HashMap<_, _>>();

            // Mark layout-changed tables: replace their delta with None so that
            // patch consolidation uses full state instead of attempting to merge.
            for name in layout_changed_tables {
                payload.insert(name, TableChange { delta: None });
            }

            payload
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
            payload: std::collections::HashMap::new(),
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
