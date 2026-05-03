use std::collections::HashSet;
use std::fmt;

use anyhow::{Result, bail};

use crate::proto::cell::Value as ProtoValue;
use crate::proto::update::Update as ProtoUpdate;
use crate::value::{Value, decode_proto_values, display_proto_values};

/// An entry whose subsidiary (non-key) values changed between two states.
///
/// `Update` is the domain counterpart to `proto::update::Update`. The proto
/// representation carries `Vec<proto::cell::Value>`; the domain type unwraps
/// each proto value into a typed domain `Value`.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Update {
    pub key: Vec<Value>,
    pub changed_indices: Vec<u32>,
    pub old_value: Vec<Value>,
    pub new_value: Vec<Value>,
}

impl TryFrom<ProtoUpdate> for Update {
    type Error = anyhow::Error;

    fn try_from(proto: ProtoUpdate) -> Result<Self> {
        Ok(Update {
            key: decode_proto_values(proto.key)?,
            changed_indices: proto.changed_indices,
            old_value: decode_proto_values(proto.old_value)?,
            new_value: decode_proto_values(proto.new_value)?,
        })
    }
}

impl From<Update> for ProtoUpdate {
    fn from(update: Update) -> Self {
        ProtoUpdate {
            key: update.key.into_iter().map(Into::into).collect(),
            changed_indices: update.changed_indices,
            old_value: update.old_value.into_iter().map(Into::into).collect(),
            new_value: update.new_value.into_iter().map(Into::into).collect(),
        }
    }
}

impl From<(Vec<Value>, (Vec<Value>, Vec<Value>))> for ProtoUpdate {
    fn from((key, (old_value, new_value)): (Vec<Value>, (Vec<Value>, Vec<Value>))) -> Self {
        ProtoUpdate {
            key: key.into_iter().map(Into::into).collect(),
            changed_indices: Vec::new(),
            old_value: old_value.into_iter().map(Into::into).collect(),
            new_value: new_value.into_iter().map(Into::into).collect(),
        }
    }
}

impl ProtoUpdate {
    /// Expand a sparse `new_value` back to a full-length vector in place.
    /// Positions not in `changed_indices` are filled with `Value::Null`.
    ///
    /// Expects the shape produced by `sparse_encode`: `new_value` length
    /// equals `changed_indices` length, `old_value` is empty, and every
    /// `changed_indices` entry is a valid column position in `0..num_values`.
    /// Returns an error if the wire data violates any of these invariants.
    pub fn expand_sparse(&mut self, num_values: usize) -> Result<()> {
        if self.changed_indices.is_empty() {
            return Ok(());
        }

        let num_changed = self.changed_indices.len();
        if self.new_value.len() != num_changed {
            bail!(
                "update: new_value has {} entries, expected {}",
                self.new_value.len(),
                num_changed
            );
        }
        // sparse_encode always clears old_value, so a sparse update on the
        // wire should have it empty. A populated old_value means the proto
        // was corrupted in transit or produced by a buggy peer.
        if !self.old_value.is_empty() {
            bail!(
                "update: old_value has {} entries on a sparse update, expected 0",
                self.old_value.len()
            );
        }

        // Move each sparse value into its true column position. Unchanged
        // columns become `Value::Null`. Bounds-check column_index inside
        // the loop so we fail fast on the first bad index.
        let null_value: ProtoValue = Value::Null.into();
        let mut new_expanded = vec![null_value.clone(); num_values];
        for (sparse_index, &column_index) in self.changed_indices.iter().enumerate() {
            if (column_index as usize) >= num_values {
                bail!(
                    "update: changed_indices[{}] = {} is out of range (table has {} columns)",
                    sparse_index,
                    column_index,
                    num_values
                );
            }
            new_expanded[column_index as usize] =
                std::mem::replace(&mut self.new_value[sparse_index], null_value.clone());
        }
        self.new_value = new_expanded;
        self.changed_indices.clear();
        Ok(())
    }

    /// Format column values for display.
    ///
    /// Returns a vector of formatted column strings. Full updates (no
    /// `changed_indices`) compare old and new positionally. Sparse updates
    /// show only changed columns, with `"_"` for unchanged ones.
    pub fn format_columns(&self, num_subsidiary: usize) -> Vec<String> {
        let has_old = !self.old_value.is_empty();
        if self.changed_indices.is_empty() {
            return self.format_full_columns(num_subsidiary, has_old);
        }
        self.format_sparse_columns(num_subsidiary, has_old)
    }

    fn format_full_columns(&self, num_subsidiary: usize, has_old: bool) -> Vec<String> {
        let mut columns = Vec::with_capacity(num_subsidiary);
        for i in 0..num_subsidiary {
            let new = self.new_value.get(i);
            let old = if has_old { self.old_value.get(i) } else { None };
            columns.push(format_update_column(new, old, has_old));
        }
        columns
    }

    fn format_sparse_columns(&self, num_subsidiary: usize, has_old: bool) -> Vec<String> {
        let changed: HashSet<u32> = self.changed_indices.iter().copied().collect();
        let mut new_iter = self.new_value.iter();
        let mut old_iter = self.old_value.iter();
        let mut columns = Vec::with_capacity(num_subsidiary);
        for i in 0..num_subsidiary as u32 {
            if !changed.contains(&i) {
                columns.push("_".to_string());
                continue;
            }
            let new = new_iter.next();
            let old = if has_old { old_iter.next() } else { None };
            columns.push(format_update_column(new, old, has_old));
        }
        columns
    }

    /// Sparse-encode an update: keep only the indices and values of columns that
    /// actually changed, and discard the old values.
    pub fn sparse_encode(&mut self) {
        let mut changed_indices = Vec::new();
        let mut sparse_new = Vec::new();

        let pairs = self.old_value.iter().zip(self.new_value.iter());
        for (i, (old_value, new_value)) in pairs.enumerate() {
            if old_value != new_value {
                changed_indices.push(i as u32);
                sparse_new.push(new_value.clone());
            }
        }

        // If all columns changed, sparse encoding adds index overhead
        // without saving any values — just drop old_value and keep
        // new_value as-is.
        self.old_value.clear();
        if changed_indices.len() == self.new_value.len() {
            return;
        }

        self.changed_indices = changed_indices;
        self.new_value = sparse_new;
    }
}

/// Format a single column value for update display.
///
/// When `old` is provided and differs from `new`, shows `"old -> new"`.
/// When `old` equals `new`, shows `"_"` (unchanged).
/// When there is no old value (i.e. due to sparse encoding), shows just `new`.
fn format_update_column(
    new: Option<&ProtoValue>,
    old: Option<&ProtoValue>,
    has_old: bool,
) -> String {
    let new_str = new.map_or("<missing>".to_string(), ProtoValue::to_string);
    if !has_old {
        return new_str;
    }
    let old_str = old.map_or("<missing>".to_string(), ProtoValue::to_string);
    if old_str == new_str {
        "_".to_string()
    } else {
        format!("{} -> {}", old_str, new_str)
    }
}

impl fmt::Display for ProtoUpdate {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(
            f,
            "[{}] [cols {:?}]: [{}] -> [{}]",
            display_proto_values(&self.key),
            self.changed_indices,
            display_proto_values(&self.old_value),
            display_proto_values(&self.new_value)
        )
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    use crate::value::{decode_proto_values, text_proto_values};

    fn make_proto_update(
        key: &[&str],
        changed_indices: &[u32],
        old_value: &[&str],
        new_value: &[&str],
    ) -> ProtoUpdate {
        ProtoUpdate {
            key: text_proto_values(key),
            changed_indices: changed_indices.to_vec(),
            old_value: text_proto_values(old_value),
            new_value: text_proto_values(new_value),
        }
    }

    #[test]
    fn test_expand_sparse() {
        // Sparse-encoded shape: empty old_value, sparse new_value.
        let mut update = make_proto_update(&["k"], &[0, 2], &[], &["x", "y"]);
        update.expand_sparse(3).unwrap();
        assert!(update.old_value.is_empty());
        assert!(update.changed_indices.is_empty());
        let decoded = decode_proto_values(update.new_value).unwrap();
        assert_eq!(decoded, vec!["x".into(), Value::Null, "y".into()]);
    }

    #[test]
    fn test_expand_sparse_no_changed_indices() {
        let mut update = make_proto_update(&["k"], &[], &["a", "b"], &["a", "b"]);
        update.expand_sparse(2).unwrap();
        assert_eq!(update.old_value.len(), 2);
        assert_eq!(update.new_value.len(), 2);
    }

    #[test]
    fn test_expand_sparse_rejects_populated_old_value() {
        let mut update = make_proto_update(&["k"], &[0, 2], &["a", "b"], &["x", "y"]);
        let err = update.expand_sparse(3).unwrap_err();
        assert!(err.to_string().contains("old_value"));
    }

    #[test]
    fn test_expand_sparse_rejects_new_value_length_mismatch() {
        let mut update = make_proto_update(&["k"], &[0, 2], &[], &["x"]);
        let err = update.expand_sparse(3).unwrap_err();
        assert!(err.to_string().contains("new_value"));
    }

    #[test]
    fn test_expand_sparse_rejects_index_out_of_range() {
        let mut update = make_proto_update(&["k"], &[0, 5], &[], &["x", "y"]);
        let err = update.expand_sparse(3).unwrap_err();
        assert!(err.to_string().contains("out of range"));
    }

    #[test]
    fn test_sparse_encode() {
        let mut update = make_proto_update(&["k"], &[], &["a", "b", "c"], &["a", "x", "c"]);
        update.sparse_encode();
        assert_eq!(update.changed_indices, vec![1]);
        assert!(update.old_value.is_empty());
        let decoded = decode_proto_values(update.new_value).unwrap();
        assert_eq!(decoded, vec!["x".into()]);
    }

    #[test]
    fn test_sparse_encode_all_changed() {
        let mut update = make_proto_update(&["k"], &[], &["a", "b"], &["x", "y"]);
        update.sparse_encode();
        assert!(update.changed_indices.is_empty());
        assert!(update.old_value.is_empty());
        assert_eq!(update.new_value.len(), 2);
    }

    #[test]
    fn test_format_full_columns_with_old() {
        let update = make_proto_update(&["k"], &[], &["a", "b", "c"], &["a", "x", "c"]);
        let columns = update.format_columns(3);
        assert_eq!(columns, vec!["_", r#""b" -> "x""#, "_"]);
    }

    #[test]
    fn test_format_full_columns_without_old() {
        let update = make_proto_update(&["k"], &[], &[], &["a", "x", "c"]);
        let columns = update.format_columns(3);
        assert_eq!(columns, vec![r#""a""#, r#""x""#, r#""c""#]);
    }

    // Note: sparse_encode() always clears old_value, so leech2 itself never
    // produces this combination. The proto wire format allows it, however, so
    // we verify that the display logic handles it correctly.
    #[test]
    fn test_format_sparse_columns_with_old() {
        let update = make_proto_update(&["k"], &[1], &["b"], &["x"]);
        let columns = update.format_columns(3);
        assert_eq!(columns, vec!["_", r#""b" -> "x""#, "_"]);
    }

    #[test]
    fn test_format_sparse_columns_without_old() {
        let update = make_proto_update(&["k"], &[1], &[], &["x"]);
        let columns = update.format_columns(3);
        assert_eq!(columns, vec!["_", r#""x""#, "_"]);
    }

    #[test]
    fn test_proto_round_trip() {
        let domain = Update {
            key: vec!["k".into()],
            changed_indices: vec![0],
            old_value: vec![],
            new_value: vec!["x".into()],
        };
        let proto: ProtoUpdate = domain.clone().into();
        let back: Update = proto.try_into().unwrap();
        assert_eq!(domain, back);
    }
}
