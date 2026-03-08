pub use crate::proto::update::Update;

use std::fmt;
use std::mem;

impl Update {
    /// Expand sparse old and new values back to full-length vectors in place.
    /// Positions not in `changed_indices` are filled with empty strings.
    pub fn expand_sparse(&mut self, num_values: usize) {
        if self.changed_indices.is_empty() {
            return;
        }
        let mut old_expanded = vec![String::new(); num_values];
        let mut new_expanded = vec![String::new(); num_values];
        for (sparse_index, &column_index) in self.changed_indices.iter().enumerate() {
            old_expanded[column_index as usize] = mem::take(&mut self.old_value[sparse_index]);
            new_expanded[column_index as usize] = mem::take(&mut self.new_value[sparse_index]);
        }
        self.old_value = old_expanded;
        self.new_value = new_expanded;
        self.changed_indices.clear();
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

impl fmt::Display for Update {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(
            f,
            "{:?} [cols {:?}]: {:?} -> {:?}",
            self.key, self.changed_indices, self.old_value, self.new_value
        )
    }
}
