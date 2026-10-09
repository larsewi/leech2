use std::collections::{HashMap, HashSet};

use anyhow::Result;
use prost_types::Timestamp;

use crate::cell::{Cell, decode_proto_cells};
use crate::proto::block::Update as ProtoBlockUpdate;
use crate::proto::cell::Cell as ProtoCell;
use crate::proto::patch::Update as ProtoPatchUpdate;

pub type UpdateMap = HashMap<Vec<Cell>, (Vec<Cell>, Vec<Cell>, Option<Timestamp>)>;

impl From<(Vec<Cell>, (Vec<Cell>, Vec<Cell>, Option<Timestamp>))> for ProtoBlockUpdate {
    fn from(
        (key, (old_value, new_value, _)): (Vec<Cell>, (Vec<Cell>, Vec<Cell>, Option<Timestamp>)),
    ) -> Self {
        // Blocks carry no change timestamps.
        ProtoBlockUpdate {
            key: key.into_iter().map(Into::into).collect(),
            old_value: old_value.into_iter().map(Into::into).collect(),
            new_value: new_value.into_iter().map(Into::into).collect(),
        }
    }
}

impl From<(Vec<Cell>, (Vec<Cell>, Vec<Cell>, Option<Timestamp>))> for ProtoPatchUpdate {
    /// Sparse-encode the update: keep only the indices and new values of the
    /// columns that changed, and drop the old values.
    fn from(
        (key, (old_value, new_value, change_timestamp)): (
            Vec<Cell>,
            (Vec<Cell>, Vec<Cell>, Option<Timestamp>),
        ),
    ) -> Self {
        let mut changed_indices = Vec::new();
        let mut sparse_new = Vec::new();
        for (index, (old, new)) in old_value.iter().zip(&new_value).enumerate() {
            if old != new {
                changed_indices.push(index as u32);
                sparse_new.push(new.clone());
            }
        }

        // If all columns changed, the indices add overhead without saving any
        // values, so send the full new value instead.
        let new_value = if changed_indices.len() == new_value.len() {
            changed_indices.clear();
            new_value
        } else {
            sparse_new
        };

        ProtoPatchUpdate {
            key: key.into_iter().map(Into::into).collect(),
            changed_indices,
            new_value: new_value.into_iter().map(Into::into).collect(),
            change_timestamp,
        }
    }
}

impl ProtoBlockUpdate {
    /// Format each subsidiary column for display: `"old -> new"` when it
    /// changed, `"_"` when it didn't.
    pub fn format_columns(&self, num_subsidiary: usize) -> Vec<String> {
        let mut columns = Vec::with_capacity(num_subsidiary);
        for index in 0..num_subsidiary {
            let old = format_cell(self.old_value.get(index));
            let new = format_cell(self.new_value.get(index));
            if old == new {
                columns.push("_".to_string());
            } else {
                columns.push(format!("{} -> {}", old, new));
            }
        }
        columns
    }
}

impl ProtoPatchUpdate {
    /// Format each subsidiary column for display: the new value when it
    /// changed, `"_"` when it didn't. An update with no `changed_indices`
    /// changed every column.
    pub fn format_columns(&self, num_subsidiary: usize) -> Vec<String> {
        let mut columns = Vec::with_capacity(num_subsidiary);
        if self.changed_indices.is_empty() {
            for index in 0..num_subsidiary {
                columns.push(format_cell(self.new_value.get(index)));
            }
            return columns;
        }

        let changed: HashSet<u32> = self.changed_indices.iter().copied().collect();
        let mut new_values = self.new_value.iter();
        for index in 0..num_subsidiary as u32 {
            if changed.contains(&index) {
                columns.push(format_cell(new_values.next()));
            } else {
                columns.push("_".to_string());
            }
        }
        columns
    }
}

fn format_cell(cell: Option<&ProtoCell>) -> String {
    cell.map_or("<missing>".to_string(), ProtoCell::to_string)
}

/// Decode a block's updates into an [`UpdateMap`] keyed by each record's key.
pub fn decode_proto_updates(protos: Vec<ProtoBlockUpdate>) -> Result<UpdateMap> {
    let mut updates = HashMap::with_capacity(protos.len());
    for proto in protos {
        let key = decode_proto_cells(proto.key)?;
        let old_value = decode_proto_cells(proto.old_value)?;
        let new_value = decode_proto_cells(proto.new_value)?;
        updates.insert(key, (old_value, new_value, None));
    }
    Ok(updates)
}

#[cfg(test)]
mod tests {
    use super::*;

    use crate::cell::text_proto_cells;

    fn text_cells(values: &[&str]) -> Vec<Cell> {
        values.iter().map(|&value| value.into()).collect()
    }

    fn patch_update(old_value: &[&str], new_value: &[&str]) -> ProtoPatchUpdate {
        ProtoPatchUpdate::from((
            text_cells(&["k"]),
            (text_cells(old_value), text_cells(new_value), None),
        ))
    }

    #[test]
    fn test_patch_update_is_sparse() {
        let update = patch_update(&["a", "b", "c"], &["a", "x", "c"]);
        assert_eq!(update.changed_indices, vec![1]);
        assert_eq!(update.new_value, text_proto_cells(&["x"]));
    }

    #[test]
    fn test_patch_update_all_changed_is_full() {
        let update = patch_update(&["a", "b"], &["x", "y"]);
        assert!(update.changed_indices.is_empty());
        assert_eq!(update.new_value, text_proto_cells(&["x", "y"]));
    }

    #[test]
    fn test_block_update_format_columns() {
        let update = ProtoBlockUpdate {
            key: text_proto_cells(&["k"]),
            old_value: text_proto_cells(&["a", "b", "c"]),
            new_value: text_proto_cells(&["a", "x", "c"]),
        };
        assert_eq!(update.format_columns(3), vec!["_", r#""b" -> "x""#, "_"]);
    }

    #[test]
    fn test_patch_update_format_full_columns() {
        let update = patch_update(&["a", "b", "c"], &["x", "y", "z"]);
        assert_eq!(update.format_columns(3), vec![r#""x""#, r#""y""#, r#""z""#]);
    }

    #[test]
    fn test_patch_update_format_sparse_columns() {
        let update = patch_update(&["a", "b", "c"], &["a", "x", "c"]);
        assert_eq!(update.format_columns(3), vec!["_", r#""x""#, "_"]);
    }
}
