pub use crate::proto::update::Update;

use std::collections::HashSet;
use std::fmt;
use std::mem;

impl Update {
    /// Expand sparse old and new values back to full-length vectors in place.
    /// Positions not in `changed_indices` are filled with empty strings.
    pub fn expand_sparse(&mut self, num_values: usize) {
        if self.changed_indices.is_empty() {
            return;
        }

        // Pre-allocate full-length vectors so we can index into arbitrary
        // column positions. Unchanged columns remain as empty strings.
        let mut old_expanded = vec![String::new(); num_values];
        let mut new_expanded = vec![String::new(); num_values];

        // Move each sparse value into its true column position, using
        // mem::take to avoid cloning (the source vectors are overwritten
        // below anyway).
        for (sparse_index, &column_index) in self.changed_indices.iter().enumerate() {
            old_expanded[column_index as usize] = mem::take(&mut self.old_value[sparse_index]);
            new_expanded[column_index as usize] = mem::take(&mut self.new_value[sparse_index]);
        }

        self.old_value = old_expanded;
        self.new_value = new_expanded;
        self.changed_indices.clear();
    }

    /// Format column values for display.
    ///
    /// Returns a vector of formatted column strings. Full updates (no
    /// `changed_indices`) compare old and new positionally. Sparse updates
    /// show only changed columns, with `"_"` for unchanged ones.
    pub fn format_columns(&self, num_subsidiary: usize) -> Vec<String> {
        let has_old = !self.old_value.is_empty();
        if self.changed_indices.is_empty() && !self.new_value.is_empty() {
            return self.format_full_columns(num_subsidiary, has_old);
        }
        self.format_sparse_columns(num_subsidiary, has_old)
    }

    fn format_full_columns(&self, num_subsidiary: usize, has_old: bool) -> Vec<String> {
        (0..num_subsidiary)
            .map(|i| {
                let new = self
                    .new_value
                    .get(i)
                    .map(|s| s.as_str())
                    .unwrap_or("<missing>");
                let old = has_old.then(|| {
                    self.old_value
                        .get(i)
                        .map(|s| s.as_str())
                        .unwrap_or("<missing>")
                });
                format_update_column(new, old)
            })
            .collect()
    }

    fn format_sparse_columns(&self, num_subsidiary: usize, has_old: bool) -> Vec<String> {
        let changed: HashSet<u32> = self.changed_indices.iter().copied().collect();
        let mut new_iter = self.new_value.iter();
        let mut old_iter = self.old_value.iter();
        (0..num_subsidiary as u32)
            .map(|i| {
                if changed.contains(&i) {
                    let new = new_iter.next().map(|s| s.as_str()).unwrap_or("<missing>");
                    let old =
                        has_old.then(|| old_iter.next().map(|s| s.as_str()).unwrap_or("<missing>"));
                    format_update_column(new, old)
                } else {
                    "_".to_string()
                }
            })
            .collect()
    }

    /// Sparse-encode an update: keep only the indices and values of columns that
    /// actually changed, and discard the old values.
    pub fn sparse_encode(&mut self) {
        let mut changed_indices = Vec::new();
        let mut sparse_new = Vec::new();
        for (i, (old_value, new_value)) in
            self.old_value.iter().zip(self.new_value.iter()).enumerate()
        {
            if old_value != new_value {
                changed_indices.push(i as u32);
                sparse_new.push(new_value.clone());
            }
        }
        self.changed_indices = changed_indices;
        self.old_value.clear();
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
/// When there is no old value, shows just `new`.
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
        let mut update = make_update(&["k"], &[0, 2], &["a", "b"], &["x", "y"]);
        update.expand_sparse(3);
        assert_eq!(update.old_value, vec!["a", "", "b"]);
        assert_eq!(update.new_value, vec!["x", "", "y"]);
        assert!(update.changed_indices.is_empty());
    }

    #[test]
    fn test_expand_sparse_no_changed_indices() {
        let mut update = make_update(&["k"], &[], &["a", "b"], &["x", "y"]);
        update.expand_sparse(2);
        assert_eq!(update.old_value, vec!["a", "b"]);
        assert_eq!(update.new_value, vec!["x", "y"]);
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
        assert_eq!(update.changed_indices, vec![0, 1]);
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
