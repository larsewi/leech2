pub use crate::proto::update::Update;

use std::fmt;

impl Update {
    /// Expand sparse values back to a full-length vector.
    /// Positions not in `changed_indices` are filled with empty strings.
    pub fn expand_sparse(
        changed_indices: &[u32],
        sparse_values: &[String],
        num_values: usize,
    ) -> Vec<String> {
        if changed_indices.is_empty() {
            return sparse_values.to_vec();
        }
        let mut full = vec![String::new(); num_values];
        for (index, value) in changed_indices.iter().zip(sparse_values.iter()) {
            full[*index as usize] = value.clone();
        }
        full
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

impl fmt::Display for Update {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(
            f,
            "{:?} [cols {:?}]: {:?} -> {:?}",
            self.key, self.changed_indices, self.old_value, self.new_value
        )
    }
}
