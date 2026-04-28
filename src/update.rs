pub use crate::proto::update::Update;

use std::collections::HashSet;
use std::fmt;
use std::mem;

use anyhow::{Result, bail};

impl Update {
    /// Expand a sparse `new_value` back to a full-length vector in place.
    /// Positions not in `changed_indices` are filled with empty strings.
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
        // columns are left as empty strings. Bounds-check column_index
        // inside the loop so we fail fast on the first bad index.
        let mut new_expanded = vec![String::new(); num_values];
        for (sparse_index, &column_index) in self.changed_indices.iter().enumerate() {
            if (column_index as usize) >= num_values {
                bail!(
                    "update: changed_indices[{}] = {} is out of range (table has {} columns)",
                    sparse_index,
                    column_index,
                    num_values
                );
            }
            new_expanded[column_index as usize] = mem::take(&mut self.new_value[sparse_index]);
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
            let new = self.new_value.get(i).map_or("<missing>", String::as_str);
            let old = if has_old {
                Some(self.old_value.get(i).map_or("<missing>", String::as_str))
            } else {
                None
            };
            columns.push(format_update_column(new, old));
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
            let new = new_iter.next().map_or("<missing>", String::as_str);
            let old = if has_old {
                Some(old_iter.next().map_or("<missing>", String::as_str))
            } else {
                None
            };
            columns.push(format_update_column(new, old));
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

impl From<(Vec<String>, (Vec<String>, Vec<String>))> for Update {
    fn from((key, (old_value, new_value)): (Vec<String>, (Vec<String>, Vec<String>))) -> Self {
        Update {
            key,
            changed_indices: Vec::new(),
            old_value,
            new_value,
        }
    }
}

/// Format a single column value for update display.
///
/// When `old` is provided and differs from `new`, shows `"old -> new"`.
/// When `old` equals `new`, shows `"_"` (unchanged).
/// When there is no old value (i.e. due to sparse encoding), shows just `new`.
fn format_update_column(new: &str, old: Option<&str>) -> String {
    match old {
        Some(old) if old != new => format!("{} -> {}", old, new),
        Some(_) => "_".to_string(),
        None => new.to_string(),
    }
}

impl fmt::Display for Update {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(
            f,
            "{:?} [cols {:?}]: {:?} -> {:?}",
            self.key, self.changed_indices, self.old_value, self.new_value
        )
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn make_update(
        key: &[&str],
        changed_indices: &[u32],
        old_value: &[&str],
        new_value: &[&str],
    ) -> Update {
        Update {
            key: key.iter().map(|s| s.to_string()).collect(),
            changed_indices: changed_indices.to_vec(),
            old_value: old_value.iter().map(|s| s.to_string()).collect(),
            new_value: new_value.iter().map(|s| s.to_string()).collect(),
        }
    }

    #[test]
    fn test_expand_sparse() {
        // Sparse-encoded shape: empty old_value, sparse new_value.
        let mut update = make_update(&["k"], &[0, 2], &[], &["x", "y"]);
        update.expand_sparse(3).unwrap();
        assert!(update.old_value.is_empty());
        assert_eq!(update.new_value, vec!["x", "", "y"]);
        assert!(update.changed_indices.is_empty());
    }

    #[test]
    fn test_expand_sparse_no_changed_indices() {
        let mut update = make_update(&["k"], &[], &["a", "b"], &["a", "b"]);
        update.expand_sparse(2).unwrap();
        assert_eq!(update.old_value, vec!["a", "b"]);
        assert_eq!(update.new_value, vec!["a", "b"]);
    }

    #[test]
    fn test_expand_sparse_rejects_populated_old_value() {
        let mut update = make_update(&["k"], &[0, 2], &["a", "b"], &["x", "y"]);
        let err = update.expand_sparse(3).unwrap_err();
        assert!(err.to_string().contains("old_value"));
    }

    #[test]
    fn test_expand_sparse_rejects_new_value_length_mismatch() {
        let mut update = make_update(&["k"], &[0, 2], &[], &["x"]);
        let err = update.expand_sparse(3).unwrap_err();
        assert!(err.to_string().contains("new_value"));
    }

    #[test]
    fn test_expand_sparse_rejects_index_out_of_range() {
        let mut update = make_update(&["k"], &[0, 5], &[], &["x", "y"]);
        let err = update.expand_sparse(3).unwrap_err();
        assert!(err.to_string().contains("out of range"));
    }

    #[test]
    fn test_sparse_encode() {
        let mut update = make_update(&["k"], &[], &["a", "b", "c"], &["a", "x", "c"]);
        update.sparse_encode();
        assert_eq!(update.changed_indices, vec![1]);
        assert!(update.old_value.is_empty());
        assert_eq!(update.new_value, vec!["x"]);
    }

    #[test]
    fn test_sparse_encode_all_changed() {
        let mut update = make_update(&["k"], &[], &["a", "b"], &["x", "y"]);
        update.sparse_encode();
        assert!(update.changed_indices.is_empty());
        assert!(update.old_value.is_empty());
        assert_eq!(update.new_value, vec!["x", "y"]);
    }

    #[test]
    fn test_format_full_columns_with_old() {
        let update = make_update(&["k"], &[], &["a", "b", "c"], &["a", "x", "c"]);
        let columns = update.format_columns(3);
        assert_eq!(columns, vec!["_", "b -> x", "_"]);
    }

    #[test]
    fn test_format_full_columns_without_old() {
        let update = make_update(&["k"], &[], &[], &["a", "x", "c"]);
        let columns = update.format_columns(3);
        assert_eq!(columns, vec!["a", "x", "c"]);
    }

    // Note: sparse_encode() always clears old_value, so leech2 itself never
    // produces this combination. The proto wire format allows it, however, so
    // we verify that the display logic handles it correctly.
    #[test]
    fn test_format_sparse_columns_with_old() {
        let update = make_update(&["k"], &[1], &["b"], &["x"]);
        let columns = update.format_columns(3);
        assert_eq!(columns, vec!["_", "b -> x", "_"]);
    }

    #[test]
    fn test_format_sparse_columns_without_old() {
        let update = make_update(&["k"], &[1], &[], &["x"]);
        let columns = update.format_columns(3);
        assert_eq!(columns, vec!["_", "x", "_"]);
    }

    #[test]
    fn test_from_tuple() {
        let update: Update = (
            vec!["k".to_string()],
            (vec!["a".to_string()], vec!["b".to_string()]),
        )
            .into();
        assert_eq!(update.key, vec!["k"]);
        assert_eq!(update.old_value, vec!["a"]);
        assert_eq!(update.new_value, vec!["b"]);
        assert!(update.changed_indices.is_empty());
    }

    #[test]
    fn test_display() {
        let update = make_update(&["k"], &[1], &["a"], &["b"]);
        let display = format!("{}", update);
        assert_eq!(display, r#"["k"] [cols [1]]: ["a"] -> ["b"]"#);
    }
}
