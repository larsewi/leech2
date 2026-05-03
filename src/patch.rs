pub use crate::proto::patch::Patch;

use std::collections::{HashMap, HashSet};
use std::fmt;
use std::path::Path;

use anyhow::{Context, Result, bail};
use prost::Message;
use prost_types::Timestamp;

use crate::block::Block;
use crate::config::{Config, InjectedFieldConfig};
use crate::delta::Delta;
use crate::head;
use crate::proto::delta::Delta as ProtoDelta;
use crate::proto::injected::Field;
use crate::proto::state::State as ProtoState;
use crate::proto::table::Table as ProtoTable;
use crate::sql::{SqlType, parse_typed_value};
use crate::utils;
use crate::utils::GENESIS_HASH;

impl TryFrom<&InjectedFieldConfig> for Field {
    type Error = anyhow::Error;

    fn try_from(config: &InjectedFieldConfig) -> Result<Self> {
        let sql_type = SqlType::from_config(&config.sql_type)
            .with_context(|| format!("injected field '{}'", config.name))?;
        let value = parse_typed_value(&config.value, &sql_type)
            .with_context(|| format!("injected field '{}'", config.name))?;
        Ok(Field {
            name: config.name.clone(),
            value: Some(value.into()),
        })
    }
}

fn fmt_payload<T: fmt::Display>(
    payload: &HashMap<String, T>,
    label: &str,
    f: &mut fmt::Formatter<'_>,
) -> fmt::Result {
    if !payload.is_empty() {
        write!(f, "\n  {} ({}):", label, payload.len())?;
        for (name, value) in payload {
            write!(
                f,
                "\n    '{}' {}",
                name,
                utils::indent(&value.to_string(), "    ")
            )?;
        }
    }
    Ok(())
}

impl fmt::Display for Patch {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "Patch:")?;
        write!(f, "\n  Head: {}", self.head)?;
        match &self.created {
            Some(timestamp) => write!(f, "\n  Created: {}", utils::format_timestamp(timestamp))?,
            // Timestamp is None when the head points to genesis (no blocks exist yet).
            None => write!(f, "\n  Created: N/A")?,
        }
        for field in &self.injected_fields {
            let value = match &field.value {
                Some(value) => value.to_string(),
                None => "<missing>".to_string(),
            };
            write!(f, "\n  Injected: {} = {}", field.name, value)?;
        }
        write!(f, "\n  Blocks: {}", self.num_blocks)?;
        fmt_payload(&self.deltas, "Deltas", f)?;
        fmt_payload(&self.states, "States", f)?;
        if self.deltas.is_empty() && self.states.is_empty() {
            write!(f, "\n  Payload: None")?;
        }
        Ok(())
    }
}

/// Load the head block header and walk the chain back to (but not including)
/// `last_known`, collecting block hashes. Only the block header is decoded
/// per block, avoiding the heavier full-payload parse. Returns the head
/// block's timestamp and the hashes in newest-first order. If `head` matches
/// `last_known`, returns an empty hash list.
fn collect_block_hashes(
    work_dir: &Path,
    head: &str,
    last_known: &str,
) -> Result<(Option<Timestamp>, Vec<String>)> {
    let block = Block::load_header(work_dir, head)?;
    let created = block.created;

    if head == last_known {
        return Ok((created, Vec::new()));
    }

    let mut hashes = vec![head.to_string()];
    let mut parent = block.parent;

    while parent != GENESIS_HASH && parent != last_known {
        hashes.push(parent.clone());
        parent = Block::load_parent_hash(work_dir, &parent)?;
    }

    if parent != last_known {
        bail!("block '{}' not found in chain", last_known);
    }

    Ok((created, hashes))
}

/// Merge a single block's deltas into per-table running results. The block is
/// the older (parent) side and the running results are the newer (child) side,
/// so the merge direction is `parent.merge(child)`. When `merged_deltas` is
/// empty (first block), this simply extracts the block's deltas.
///
/// Tables whose layout changed (delta is `None`) or whose merge failed are
/// added to `skipped_tables` and fall back to full state.
fn merge_block_deltas(
    block: Block,
    merged_deltas: &mut HashMap<String, Delta>,
    skipped_tables: &mut HashSet<String>,
) {
    for (table_name, payload) in block.payload {
        if skipped_tables.contains(&table_name) {
            continue;
        }

        // A missing delta means the table's field layout changed between
        // blocks; skip further merging and fall back to full state.
        let Some(proto_delta) = payload.delta else {
            log::warn!(
                "Layout changed for table '{}', falling back to full state",
                table_name
            );
            merged_deltas.remove(&table_name);
            skipped_tables.insert(table_name);
            continue;
        };

        let result = Delta::try_from(proto_delta).and_then(|mut parent| {
            if let Some(child) = merged_deltas.remove(&table_name) {
                parent.merge(child)?;
            }
            Ok(parent)
        });

        match result {
            Ok(delta) => {
                merged_deltas.insert(table_name, delta);
            }
            Err(e) => {
                log::warn!(
                    "Merge failed for table '{}', falling back to full state: {}",
                    table_name,
                    e
                );
                merged_deltas.remove(&table_name);
                skipped_tables.insert(table_name);
            }
        }
    }
}

type ConsolidateResult = (
    Option<Timestamp>,
    u32,
    HashMap<String, ProtoDelta>,
    HashMap<String, ProtoTable>,
);

fn try_consolidate(work_dir: &Path, head: &str, last_known: &str) -> Result<ConsolidateResult> {
    let (created, block_hashes) = collect_block_hashes(work_dir, head, last_known)?;

    if block_hashes.is_empty() {
        return Ok((created, 0, HashMap::new(), HashMap::new()));
    }

    let num_blocks = block_hashes.len() as u32;

    // Load blocks one at a time oldest-first, merging deltas incrementally.
    // Only one block's payload and the per-table running results are in
    // memory at a time.
    let mut merged_deltas: HashMap<String, Delta> = HashMap::new();
    let mut skipped_tables: HashSet<String> = HashSet::new();

    for (index, hash) in block_hashes.iter().rev().enumerate() {
        log::trace!(
            "Merging block {}/{}: '{:.7}...'",
            index + 1,
            num_blocks,
            hash
        );
        let block = Block::load(work_dir, hash)?;
        merge_block_deltas(block, &mut merged_deltas, &mut skipped_tables);
    }

    // Load state for per-table size comparison and fallback.
    let state_tables = match ProtoState::load(work_dir)? {
        Some(state) => state.tables,
        None => HashMap::new(),
    };

    let mut result_deltas = HashMap::new();
    let mut result_states = HashMap::new();

    // Skipped tables fall back to full state.
    for table_name in &skipped_tables {
        if let Some(state_table) = state_tables.get(table_name) {
            log::info!("Table '{}': using full state (layout changed)", table_name);
            result_states.insert(table_name.clone(), state_table.clone());
        } else {
            log::warn!("Table '{}' not in STATE file, skipping", table_name);
        }
    }

    for (table_name, merged) in merged_deltas {
        let mut merged_delta = ProtoDelta::from(merged);

        // Strip data the receiver doesn't need.
        for delete in &mut merged_delta.deletes {
            delete.value.clear();
        }
        for update in &mut merged_delta.updates {
            update.sparse_encode();
        }

        // Per-table size comparison: use full state if it's smaller.
        if let Some(state_table) = state_tables.get(&table_name)
            && state_table.encoded_len() < merged_delta.encoded_len()
        {
            log::info!(
                "Table '{}': using full state (smaller than consolidated delta)",
                table_name
            );
            result_states.insert(table_name, state_table.clone());
            continue;
        }

        result_deltas.insert(table_name, merged_delta);
    }

    Ok((created, num_blocks, result_deltas, result_states))
}

fn full_state_patch(
    work_dir: &Path,
    head: &str,
    injected_fields: Vec<Field>,
    field_hashes: HashMap<String, String>,
) -> Result<Patch> {
    let created = Block::load(work_dir, head)
        .ok()
        .and_then(|block| block.created);
    let state = ProtoState::load(work_dir)?.context("no STATE file found for full state patch")?;
    let patch = Patch {
        head: head.to_string(),
        created,
        injected_fields,
        num_blocks: 0,
        deltas: HashMap::new(),
        states: state.tables,
        field_hashes,
    };
    log::trace!("Built patch:\n{}", patch);
    Ok(patch)
}

impl Patch {
    pub fn create(config: &Config, last_known: &str) -> Result<Patch> {
        let work_dir = &config.work_dir;

        let resolved = crate::storage::resolve_hash_prefix(work_dir, last_known);

        let head = head::load(work_dir)?;

        let mut injected_fields: Vec<Field> = Vec::with_capacity(config.injected_fields.len());
        for field_config in &config.injected_fields {
            injected_fields.push(Field::try_from(field_config)?);
        }

        let field_hashes: HashMap<String, String> = config
            .tables
            .iter()
            .map(|(name, table_config)| (name.clone(), table_config.field_hash()))
            .collect();

        if head == GENESIS_HASH {
            let patch = Patch {
                head,
                created: None,
                injected_fields,
                num_blocks: 0,
                deltas: HashMap::new(),
                states: HashMap::new(),
                field_hashes,
            };
            log::trace!("Built patch:\n{}", patch);
            return Ok(patch);
        }

        // If the reference block can't be resolved or is genesis, produce a
        // full STATE payload (TRUNCATE + INSERT) which is always safe to apply
        // regardless of current database contents.
        let last_known = match resolved {
            Ok(hash) if hash != GENESIS_HASH => hash,
            Ok(_) => {
                log::info!("Reference is genesis, producing full state patch");
                return full_state_patch(work_dir, &head, injected_fields, field_hashes);
            }
            Err(e) => {
                log::warn!(
                    "Reference block not found, producing full state patch: {}",
                    e
                );
                return full_state_patch(work_dir, &head, injected_fields, field_hashes);
            }
        };

        let (created, num_blocks, deltas, states) =
            match try_consolidate(work_dir, &head, &last_known) {
                Ok(result) => result,
                Err(e) => {
                    log::warn!("Consolidation failed, falling back to full state: {}", e);
                    return full_state_patch(work_dir, &head, injected_fields, field_hashes);
                }
            };

        let patch = Patch {
            head,
            created,
            injected_fields,
            num_blocks,
            deltas,
            states,
            field_hashes,
        };

        log::trace!("Built patch:\n{}", patch);
        Ok(patch)
    }

    /// Add or overwrite an injected field on this patch. Validates that the
    /// name is non-empty and the sql_type is one of TEXT, NUMBER, or BOOLEAN.
    /// If a field with the same name already exists (whether from static
    /// config or a previous inject_field call), its value is replaced; a
    /// warning is logged when the replacement actually differs from the
    /// existing value.
    pub fn inject_field(&mut self, name: &str, value: &str, sql_type: &str) -> Result<()> {
        if name.is_empty() {
            bail!("inject_field: name must not be empty");
        }

        let sql_type = SqlType::from_config(sql_type).context("inject_field: invalid sql_type")?;
        let parsed = parse_typed_value(value, &sql_type).context("inject_field: invalid value")?;
        let new_value: crate::proto::cell::Value = parsed.into();

        if let Some(existing) = self.injected_fields.iter_mut().find(|f| f.name == name) {
            if existing.value.as_ref() != Some(&new_value) {
                let old_value = match &existing.value {
                    Some(value) => value.to_string(),
                    None => "<missing>".to_string(),
                };
                log::warn!(
                    "inject_field: overwriting '{}' (was {}, now {})",
                    name,
                    old_value,
                    new_value
                );
            }
            existing.value = Some(new_value);
        } else {
            self.injected_fields.push(Field {
                name: name.to_string(),
                value: Some(new_value),
            });
        }
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::value::Value;

    fn empty_patch() -> Patch {
        Patch {
            head: String::new(),
            created: None,
            injected_fields: Vec::new(),
            num_blocks: 0,
            deltas: HashMap::new(),
            states: HashMap::new(),
            field_hashes: HashMap::new(),
        }
    }

    fn injected_value(field: &Field) -> Value {
        Value::try_from(field.value.as_ref().unwrap()).unwrap()
    }

    #[test]
    fn test_inject_field_add_text() {
        let mut patch = empty_patch();
        patch.inject_field("hostkey", "abc", "TEXT").unwrap();
        assert_eq!(patch.injected_fields.len(), 1);
        assert_eq!(patch.injected_fields[0].name, "hostkey");
        assert_eq!(
            injected_value(&patch.injected_fields[0]),
            Value::from("abc")
        );
    }

    #[test]
    fn test_inject_field_add_number() {
        let mut patch = empty_patch();
        patch.inject_field("count", "42", "NUMBER").unwrap();
        assert_eq!(
            injected_value(&patch.injected_fields[0]),
            Value::Number(42.0)
        );
    }

    #[test]
    fn test_inject_field_add_boolean() {
        let mut patch = empty_patch();
        patch.inject_field("enabled", "true", "BOOLEAN").unwrap();
        assert_eq!(
            injected_value(&patch.injected_fields[0]),
            Value::Boolean(true)
        );
    }

    #[test]
    fn test_inject_field_overwrite_replaces_value() {
        let mut patch = empty_patch();
        patch.injected_fields.push(Field {
            name: "host".to_string(),
            value: Some(Value::Number(1.0).into()),
        });
        patch.inject_field("host", "new-value", "TEXT").unwrap();
        assert_eq!(patch.injected_fields.len(), 1);
        assert_eq!(patch.injected_fields[0].name, "host");
        assert_eq!(
            injected_value(&patch.injected_fields[0]),
            Value::from("new-value")
        );
    }

    #[test]
    fn test_inject_field_multiple_distinct_names_append() {
        let mut patch = empty_patch();
        patch.inject_field("a", "1", "TEXT").unwrap();
        patch.inject_field("b", "2", "TEXT").unwrap();
        assert_eq!(patch.injected_fields.len(), 2);
        assert_eq!(patch.injected_fields[0].name, "a");
        assert_eq!(patch.injected_fields[1].name, "b");
    }

    #[test]
    fn test_inject_field_rejects_empty_name() {
        let mut patch = empty_patch();
        let err = patch.inject_field("", "value", "TEXT").unwrap_err();
        assert!(err.to_string().contains("name must not be empty"));
    }

    #[test]
    fn test_inject_field_rejects_invalid_type() {
        let mut patch = empty_patch();
        let err = patch.inject_field("foo", "bar", "BOGUS").unwrap_err();
        assert!(err.to_string().contains("invalid sql_type"));
    }

    #[test]
    fn test_inject_field_rejects_invalid_value() {
        let mut patch = empty_patch();
        let err = patch
            .inject_field("count", "not_a_number", "NUMBER")
            .unwrap_err();
        assert!(err.to_string().contains("invalid value"));
    }

    #[test]
    fn test_inject_field_invalid_type_does_not_mutate() {
        let mut patch = empty_patch();
        let _ = patch.inject_field("foo", "bar", "BOGUS");
        assert!(patch.injected_fields.is_empty());
    }
}
