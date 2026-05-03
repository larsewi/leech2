use std::collections::HashMap;
use std::fmt;

use anyhow::{Context, Result, bail};

use crate::entry::Entry;
use crate::proto::delta::Delta as ProtoDelta;
use crate::state::State;
use crate::table::Table;
use crate::update::Update;
use crate::value::Value;
use crate::value::display_proto_values;

type RecordMap = HashMap<Vec<Value>, Vec<Value>>;
type UpdateMap = HashMap<Vec<Value>, (Vec<Value>, Vec<Value>)>;

/// Delta represents the changes to a single table between two states.
#[derive(Debug, Clone, PartialEq)]
pub struct Delta {
    /// The names of all columns, primary key columns first.
    pub column_names: Vec<String>,
    /// Entries that were added (key -> value).
    pub inserts: RecordMap,
    /// Entries that were removed (key -> value).
    pub deletes: RecordMap,
    /// Entries that were modified (key -> (old_value, new_value)).
    pub updates: UpdateMap,
}

impl TryFrom<ProtoDelta> for Delta {
    type Error = anyhow::Error;

    fn try_from(proto: ProtoDelta) -> Result<Self> {
        let num_subsidiary = proto.num_subsidiary().context("corrupt delta")?;

        let mut inserts = HashMap::with_capacity(proto.inserts.len());
        for entry in proto.inserts {
            let entry = Entry::try_from(entry)?;
            inserts.insert(entry.key, entry.value);
        }

        let mut deletes = HashMap::with_capacity(proto.deletes.len());
        for entry in proto.deletes {
            let entry = Entry::try_from(entry)?;
            deletes.insert(entry.key, entry.value);
        }

        // Updates are stored sparsely on the wire: only changed column
        // indices and their values are included. Expand them back to
        // full-width value vectors (one element per subsidiary column).
        let mut updates = HashMap::with_capacity(proto.updates.len());
        for mut proto_update in proto.updates {
            proto_update.expand_sparse(num_subsidiary)?;
            let update = Update::try_from(proto_update)?;
            updates.insert(update.key, (update.old_value, update.new_value));
        }

        Ok(Delta {
            column_names: proto.column_names,
            inserts,
            deletes,
            updates,
        })
    }
}

impl From<Delta> for ProtoDelta {
    fn from(delta: Delta) -> Self {
        ProtoDelta {
            column_names: delta.column_names,
            inserts: delta.inserts.into_iter().map(Into::into).collect(),
            deletes: delta.deletes.into_iter().map(Into::into).collect(),
            updates: delta.updates.into_iter().map(Into::into).collect(),
        }
    }
}

impl ProtoDelta {
    /// Number of subsidiary (non-key) columns.
    ///
    /// The proto format stores keys and values separately, but `column_names`
    /// lists all columns together (PK first, then subsidiary). This method
    /// determines the PK count from the first available entry's key length
    /// (trying inserts, then deletes, then updates; defaulting to 0 if the
    /// delta is empty) and subtracts it from the total column count.
    fn num_subsidiary(&self) -> Result<usize> {
        let num_primary_keys = if let Some(entry) = self.inserts.first() {
            entry.key.len()
        } else if let Some(entry) = self.deletes.first() {
            entry.key.len()
        } else if let Some(update) = self.updates.first() {
            update.key.len()
        } else {
            0
        };
        if self.column_names.len() < num_primary_keys {
            bail!(
                "column_names has {} entries but primary key has {}",
                self.column_names.len(),
                num_primary_keys
            );
        }
        Ok(self.column_names.len() - num_primary_keys)
    }

    fn fmt_inserts(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        if self.inserts.is_empty() {
            return Ok(());
        }

        write!(f, "\n  Inserts ({}):", self.inserts.len())?;
        for entry in &self.inserts {
            write!(
                f,
                "\n    ({}) {}",
                display_proto_values(&entry.key),
                display_proto_values(&entry.value)
            )?;
        }
        Ok(())
    }

    fn fmt_deletes(&self, f: &mut fmt::Formatter<'_>, num_subsidiary: usize) -> fmt::Result {
        if self.deletes.is_empty() {
            return Ok(());
        }

        write!(f, "\n  Deletes ({}):", self.deletes.len())?;
        for entry in &self.deletes {
            let values = if entry.value.is_empty() {
                vec!["_"; num_subsidiary].join(", ")
            } else {
                display_proto_values(&entry.value)
            };
            write!(f, "\n    ({}) {}", display_proto_values(&entry.key), values)?;
        }
        Ok(())
    }

    /// Format update entries. Updates come in two wire formats:
    /// - **Full** (blocks): `changed_indices` is empty and all `num_subsidiary`
    ///   columns are present in `new_value`/`old_value` positionally.
    /// - **Sparse** (patches): only the columns listed in `changed_indices`
    ///   appear in `new_value`/`old_value`; unchanged columns show as `"_"`.
    fn fmt_updates(&self, f: &mut fmt::Formatter<'_>, num_subsidiary: usize) -> fmt::Result {
        if self.updates.is_empty() {
            return Ok(());
        }

        write!(f, "\n  Updates ({}):", self.updates.len())?;
        for update in &self.updates {
            let columns = update.format_columns(num_subsidiary);
            write!(
                f,
                "\n    ({}) {}",
                display_proto_values(&update.key),
                columns.join(", ")
            )?;
        }
        Ok(())
    }
}

impl fmt::Display for ProtoDelta {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "[{}]", self.column_names.join(", "))?;
        let num_subsidiary = match self.num_subsidiary() {
            Ok(num_subsidiary) => num_subsidiary,
            Err(e) => return write!(f, " <corrupt delta: {}>", e),
        };
        self.fmt_inserts(f)?;
        self.fmt_deletes(f, num_subsidiary)?;
        self.fmt_updates(f, num_subsidiary)?;
        Ok(())
    }
}

impl Delta {
    /// Merge child delta into parent delta, producing a single delta that
    /// represents the combined effect of both. See DELTA_MERGING_RULES.md for
    /// the full specification of the 15 rules.
    pub fn merge(&mut self, child: Delta) -> Result<()> {
        if self.column_names != child.column_names {
            bail!(
                "field mismatch ({:?} vs {:?})",
                self.column_names,
                child.column_names
            );
        }

        for (key, value) in child.inserts {
            self.merge_insert(key, value)
                .context("failed to merge inserts")?;
        }
        for (key, value) in child.deletes {
            self.merge_delete(key, value)
                .context("failed to merge deletes")?;
        }
        for (key, (child_old, child_new)) in child.updates {
            self.merge_update(key, child_old, child_new)
                .context("failed to merge updates")?;
        }
        Ok(())
    }

    fn merge_insert(&mut self, key: Vec<Value>, insert_value: Vec<Value>) -> Result<()> {
        if self.inserts.contains_key(&key) {
            // Rule 5: double insert → error
            bail!("rule 5: key {:?} inserted in both blocks", key);
        } else if let Some(delete_value) = self.deletes.remove(&key) {
            if delete_value == insert_value {
                // Rule 9a: delete then insert with same value → cancels out
                log::trace!("Rule 9a: delete + insert cancel out for key {:?}", key);
            } else {
                // Rule 9b: delete then insert with different value → update
                log::trace!("Rule 9b: delete + insert becomes update for key {:?}", key);
                self.updates.insert(key, (delete_value, insert_value));
            }
        } else if self.updates.contains_key(&key) {
            // Rule 13: insert after update → error
            bail!(
                "rule 13: key {:?} updated in parent, inserted in child",
                key
            );
        } else {
            // Rule 1: pass through
            log::trace!("Rule 1: insert passes through for key {:?}", key);
            self.inserts.insert(key, insert_value);
        }
        Ok(())
    }

    fn merge_delete(&mut self, key: Vec<Value>, delete_value: Vec<Value>) -> Result<()> {
        if self.inserts.remove(&key).is_some() {
            // Rule 6: insert then delete → cancels out
            log::trace!("Rule 6: insert + delete cancel out for key {:?}", key);
        } else if self.deletes.contains_key(&key) {
            // Rule 10: double delete → error
            bail!("rule 10: key {:?} deleted in both blocks", key);
        } else if let Some((old_value, new_value)) = self.updates.remove(&key) {
            if delete_value == new_value {
                // Rule 14a: update then delete, values match → delete(old)
                log::trace!("Rule 14a: update + delete becomes delete for key {:?}", key);
                self.deletes.insert(key, old_value);
            } else {
                // Rule 14b: update then delete, values mismatch → error
                bail!(
                    "rule 14b: key {:?} updated to {:?} in parent, but deleted with {:?}",
                    key,
                    new_value,
                    delete_value
                );
            }
        } else {
            // Rule 2: pass through
            log::trace!("Rule 2: delete passes through for key {:?}", key);
            self.deletes.insert(key, delete_value);
        }
        Ok(())
    }

    fn merge_update(
        &mut self,
        key: Vec<Value>,
        child_old: Vec<Value>,
        child_new: Vec<Value>,
    ) -> Result<()> {
        if let Some(insert_value) = self.inserts.get_mut(&key) {
            // Rule 7: insert then update → insert(new_value)
            log::trace!("Rule 7: insert + update becomes insert for key {:?}", key);
            *insert_value = child_new;
        } else if self.deletes.contains_key(&key) {
            // Rule 11: update after delete → error
            bail!("rule 11: key {:?} deleted in parent, updated in child", key);
        } else if let Some((merged_old, merged_new)) = self.updates.get_mut(&key) {
            // Rule 15: combine parent and child updates per column. Keep
            // the earliest "before" and the latest "after" for each
            // column.
            log::trace!("Rule 15: update + update merged for key {:?}", key);
            if merged_old.len() != merged_new.len()
                || merged_old.len() != child_old.len()
                || merged_old.len() != child_new.len()
            {
                bail!(
                    "rule 15: vector length mismatch for key {:?}: \
                     parent old/new = {}/{}, child old/new = {}/{}",
                    key,
                    merged_old.len(),
                    merged_new.len(),
                    child_old.len(),
                    child_new.len()
                );
            }
            for i in 0..merged_old.len() {
                let parent_changed = merged_old[i] != merged_new[i];
                let child_changed = child_old[i] != child_new[i];
                match (parent_changed, child_changed) {
                    // Both changed: keep parent's "before", take child's "after".
                    (true, true) => merged_new[i] = child_new[i].clone(),
                    // Only parent changed: existing pair is correct.
                    (true, false) => {}
                    // Only child changed: adopt child's pair.
                    (false, true) => {
                        merged_old[i] = child_old[i].clone();
                        merged_new[i] = child_new[i].clone();
                    }
                    // Neither changed: nothing to do.
                    (false, false) => {}
                }
            }
        } else {
            // Rule 3: pass through
            log::trace!("Rule 3: update passes through for key {:?}", key);
            self.updates.insert(key, (child_old, child_new));
        }
        Ok(())
    }

    /// Compute deltas between a previous and current state.
    ///
    /// Returns `None` for tables whose field layout changed (columns
    /// added/removed/reordered), since positional record values are
    /// not comparable across different layouts.  Callers should treat
    /// `None` as "use full state instead of a delta".
    pub fn compute(
        previous_state: Option<State>,
        current_state: &State,
    ) -> HashMap<String, Option<Delta>> {
        let mut deltas = HashMap::new();

        // Process tables in current state
        for (table_name, current_table) in &current_state.tables {
            let previous_table = previous_state
                .as_ref()
                .and_then(|state| state.tables.get(table_name));

            // If the field layout changed, a meaningful delta cannot be computed.
            if let Some(previous_table) = previous_table
                && previous_table.fields != current_table.fields
            {
                log::warn!(
                    "Table '{}': field layout changed, will use full state",
                    table_name
                );
                deltas.insert(table_name.clone(), None);
                continue;
            }

            let (inserts, deletes, updates) = Self::diff_table(previous_table, current_table);

            log::trace!(
                "Table '{}': {} inserts, {} deletes, {} updates",
                table_name,
                inserts.len(),
                deletes.len(),
                updates.len()
            );

            // Skip tables with no changes
            if inserts.is_empty() && deletes.is_empty() && updates.is_empty() {
                continue;
            }

            deltas.insert(
                table_name.clone(),
                Some(Delta {
                    column_names: current_table.fields.clone(),
                    inserts,
                    deletes,
                    updates,
                }),
            );
        }

        // Tables only in previous state: all records are deletes
        if let Some(ref previous_state) = previous_state {
            for (table_name, table) in &previous_state.tables {
                // Skip empty tables
                if table.records.is_empty() {
                    continue;
                }

                // Skip if table exists in current state (this is already handled above)
                if current_state.tables.contains_key(table_name) {
                    continue;
                }

                deltas.insert(
                    table_name.clone(),
                    Some(Delta {
                        column_names: table.fields.clone(),
                        inserts: HashMap::new(),
                        deletes: table.records.clone(),
                        updates: HashMap::new(),
                    }),
                );
            }
        }

        deltas
    }

    fn diff_table(
        previous_table: Option<&Table>,
        current_table: &Table,
    ) -> (RecordMap, RecordMap, UpdateMap) {
        let mut deletes = HashMap::new();
        let mut updates = HashMap::new();

        let Some(previous_table) = previous_table else {
            // No previous table: all records are inserts
            let inserts = current_table.records.clone();
            return (inserts, deletes, updates);
        };

        let mut inserts = HashMap::new();

        // Keys in previous but not current -> deletes
        for (key, value) in &previous_table.records {
            if !current_table.records.contains_key(key) {
                deletes.insert(key.clone(), value.clone());
            }
        }

        // Keys in current but not previous -> inserts
        // Keys in both with different values -> updates
        for (key, current_value) in &current_table.records {
            match previous_table.records.get(key) {
                None => {
                    inserts.insert(key.clone(), current_value.clone());
                }
                Some(previous_value) if previous_value != current_value => {
                    updates.insert(key.clone(), (previous_value.clone(), current_value.clone()));
                }
                _ => {} // Same value, skip
            }
        }

        (inserts, deletes, updates)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::value::text_values;

    fn make_table(rows: &[(&[&str], &[&str])]) -> Table {
        let records = rows
            .iter()
            .map(|(key, value)| (text_values(key), text_values(value)))
            .collect();
        Table {
            fields: vec![],
            records,
        }
    }

    #[test]
    fn test_no_previous_state_all_inserts() {
        let mut tables = HashMap::new();
        tables.insert(
            "users".to_string(),
            make_table(&[(&["1"], &["alice"]), (&["2"], &["bob"])]),
        );
        let current = State { tables };

        let deltas = Delta::compute(None, &current);

        assert_eq!(deltas.len(), 1);
        let delta = deltas.get("users").unwrap().as_ref().unwrap();
        assert_eq!(delta.inserts.len(), 2);
        assert_eq!(delta.deletes.len(), 0);
        assert_eq!(delta.updates.len(), 0);
    }

    #[test]
    fn test_table_only_in_previous_all_deletes() {
        let mut prev_tables = HashMap::new();
        prev_tables.insert(
            "old_table".to_string(),
            make_table(&[(&["1"], &["data1"]), (&["2"], &["data2"])]),
        );
        let previous = State {
            tables: prev_tables,
        };
        let current = State {
            tables: HashMap::new(),
        };

        let deltas = Delta::compute(Some(previous), &current);

        assert_eq!(deltas.len(), 1);
        let delta = deltas.get("old_table").unwrap().as_ref().unwrap();
        assert_eq!(delta.inserts.len(), 0);
        assert_eq!(delta.deletes.len(), 2);
        assert_eq!(delta.updates.len(), 0);
    }

    #[test]
    fn test_table_in_both_states_mixed_changes() {
        let mut previous_tables = HashMap::new();
        previous_tables.insert(
            "users".to_string(),
            make_table(&[
                (&["1"], &["alice"]),   // will be updated
                (&["2"], &["bob"]),     // will be deleted
                (&["3"], &["charlie"]), // unchanged
            ]),
        );
        let previous_state = State {
            tables: previous_tables,
        };

        let mut current_tables = HashMap::new();
        current_tables.insert(
            "users".to_string(),
            make_table(&[
                (&["1"], &["alice_updated"]), // update
                (&["3"], &["charlie"]),       // unchanged
                (&["4"], &["dave"]),          // insert
            ]),
        );
        let current_state = State {
            tables: current_tables,
        };

        let deltas = Delta::compute(Some(previous_state), &current_state);

        assert_eq!(deltas.len(), 1);
        let delta = deltas.get("users").unwrap().as_ref().unwrap();

        // Key "4" is new -> insert
        assert_eq!(delta.inserts.len(), 1);
        assert!(delta.inserts.contains_key(&text_values(&["4"])));

        // Key "2" removed -> delete
        assert_eq!(delta.deletes.len(), 1);
        assert!(delta.deletes.contains_key(&text_values(&["2"])));

        // Key "1" changed value -> update
        // Key "3" has same value -> skipped
        assert_eq!(delta.updates.len(), 1);
        assert!(delta.updates.contains_key(&text_values(&["1"])));
    }

    #[test]
    fn test_multiple_tables() {
        let mut previous_tables = HashMap::new();
        previous_tables.insert("table_a".to_string(), make_table(&[(&["1"], &["a"])]));
        previous_tables.insert("table_b".to_string(), make_table(&[(&["1"], &["b"])]));
        let previous_state = State {
            tables: previous_tables,
        };

        let mut current_tables = HashMap::new();
        current_tables.insert("table_b".to_string(), make_table(&[(&["2"], &["b2"])]));
        current_tables.insert("table_c".to_string(), make_table(&[(&["1"], &["c"])]));
        let current_state = State {
            tables: current_tables,
        };

        let deltas = Delta::compute(Some(previous_state), &current_state);

        assert_eq!(deltas.len(), 3);

        // table_a: only in previous -> all deletes
        let delta_a = deltas.get("table_a").unwrap().as_ref().unwrap();
        assert_eq!(delta_a.deletes.len(), 1);
        assert_eq!(delta_a.inserts.len(), 0);

        // table_b: in both -> key "1" deleted, key "2" inserted
        let delta_b = deltas.get("table_b").unwrap().as_ref().unwrap();
        assert_eq!(delta_b.deletes.len(), 1);
        assert!(delta_b.deletes.contains_key(&text_values(&["1"])));
        assert_eq!(delta_b.inserts.len(), 1);
        assert!(delta_b.inserts.contains_key(&text_values(&["2"])));

        // table_c: only in current -> all inserts
        let delta_c = deltas.get("table_c").unwrap().as_ref().unwrap();
        assert_eq!(delta_c.inserts.len(), 1);
        assert_eq!(delta_c.deletes.len(), 0);
    }

    #[test]
    fn test_empty_states() {
        let previous_state = State {
            tables: HashMap::new(),
        };
        let current_state = State {
            tables: HashMap::new(),
        };

        let deltas = Delta::compute(Some(previous_state), &current_state);
        assert_eq!(deltas.len(), 0);
    }

    #[test]
    fn test_unchanged_table_skipped() {
        let mut previous_tables = HashMap::new();
        previous_tables.insert(
            "unchanged".to_string(),
            make_table(&[(&["1"], &["alice"]), (&["2"], &["bob"])]),
        );
        previous_tables.insert(
            "changed".to_string(),
            make_table(&[(&["1"], &["old_value"])]),
        );
        let previous_state = State {
            tables: previous_tables,
        };

        let mut current_tables = HashMap::new();
        current_tables.insert(
            "unchanged".to_string(),
            make_table(&[(&["1"], &["alice"]), (&["2"], &["bob"])]),
        );
        current_tables.insert(
            "changed".to_string(),
            make_table(&[(&["1"], &["new_value"])]),
        );
        let current_state = State {
            tables: current_tables,
        };

        let deltas = Delta::compute(Some(previous_state), &current_state);

        // Only the changed table should have a delta
        assert_eq!(deltas.len(), 1);
        assert!(deltas.contains_key("changed"));
        assert!(!deltas.contains_key("unchanged"));
    }

    #[test]
    fn test_layout_change_returns_none() {
        let mut previous_tables = HashMap::new();
        previous_tables.insert(
            "users".to_string(),
            Table {
                fields: vec!["id".to_string(), "name".to_string()],
                records: HashMap::from([(text_values(&["1"]), text_values(&["alice"]))]),
            },
        );
        let previous_state = State {
            tables: previous_tables,
        };

        let mut current_tables = HashMap::new();
        current_tables.insert(
            "users".to_string(),
            Table {
                fields: vec!["id".to_string(), "name".to_string(), "email".to_string()],
                records: HashMap::from([(
                    text_values(&["1"]),
                    text_values(&["alice", "alice@example.com"]),
                )]),
            },
        );
        let current_state = State {
            tables: current_tables,
        };

        let deltas = Delta::compute(Some(previous_state), &current_state);

        assert_eq!(deltas.len(), 1);
        assert!(deltas.get("users").unwrap().is_none());
    }

    #[test]
    fn test_composite_key() {
        let mut previous_tables = HashMap::new();
        previous_tables.insert(
            "orders".to_string(),
            make_table(&[
                (&["user1", "order1"], &["100"]),
                (&["user1", "order2"], &["200"]),
            ]),
        );
        let previous_state = State {
            tables: previous_tables,
        };

        let mut current_tables = HashMap::new();
        current_tables.insert(
            "orders".to_string(),
            make_table(&[
                (&["user1", "order1"], &["150"]), // update
                (&["user2", "order1"], &["300"]), // insert (different user)
            ]),
        );
        let current_state = State {
            tables: current_tables,
        };

        let deltas = Delta::compute(Some(previous_state), &current_state);

        let delta = deltas.get("orders").unwrap().as_ref().unwrap();
        assert_eq!(delta.inserts.len(), 1);
        assert!(
            delta
                .inserts
                .contains_key(&text_values(&["user2", "order1"]))
        );
        assert_eq!(delta.deletes.len(), 1);
        assert!(
            delta
                .deletes
                .contains_key(&text_values(&["user1", "order2"]))
        );
        assert_eq!(delta.updates.len(), 1);
        assert!(
            delta
                .updates
                .contains_key(&text_values(&["user1", "order1"]))
        );
    }

    // ---- Merge tests ----

    fn empty_delta() -> Delta {
        Delta {
            column_names: vec![],
            inserts: HashMap::new(),
            deletes: HashMap::new(),
            updates: HashMap::new(),
        }
    }

    // Rule 1: child insert, no parent → insert passes through
    #[test]
    fn test_merge_rule1_child_insert_only() {
        let mut parent_delta = empty_delta();
        let mut child_delta = empty_delta();
        child_delta
            .inserts
            .insert(text_values(&["3"]), text_values(&["Charlie"]));

        parent_delta.merge(child_delta).unwrap();

        assert_eq!(parent_delta.inserts.len(), 1);
        assert_eq!(
            parent_delta.inserts[&text_values(&["3"])],
            text_values(&["Charlie"])
        );
        assert!(parent_delta.deletes.is_empty());
        assert!(parent_delta.updates.is_empty());
    }

    // Rule 2: child delete, no parent → delete passes through
    #[test]
    fn test_merge_rule2_child_delete_only() {
        let mut parent_delta = empty_delta();
        let mut child_delta = empty_delta();
        child_delta
            .deletes
            .insert(text_values(&["2"]), text_values(&["Bob"]));

        parent_delta.merge(child_delta).unwrap();

        assert_eq!(parent_delta.deletes.len(), 1);
        assert_eq!(
            parent_delta.deletes[&text_values(&["2"])],
            text_values(&["Bob"])
        );
        assert!(parent_delta.inserts.is_empty());
        assert!(parent_delta.updates.is_empty());
    }

    // Rule 3: child update, no parent → update passes through
    #[test]
    fn test_merge_rule3_child_update_only() {
        let mut parent_delta = empty_delta();
        let mut child_delta = empty_delta();
        child_delta.updates.insert(
            text_values(&["1"]),
            (text_values(&["Alice"]), text_values(&["Alicia"])),
        );

        parent_delta.merge(child_delta).unwrap();

        assert_eq!(parent_delta.updates.len(), 1);
        let (old_value, new_value) = &parent_delta.updates[&text_values(&["1"])];
        assert_eq!(old_value, &text_values(&["Alice"]));
        assert_eq!(new_value, &text_values(&["Alicia"]));
        assert!(parent_delta.inserts.is_empty());
        assert!(parent_delta.deletes.is_empty());
    }

    // Rule 4: parent insert, no child → insert stays
    #[test]
    fn test_merge_rule4_parent_insert_only() {
        let mut parent_delta = empty_delta();
        parent_delta
            .inserts
            .insert(text_values(&["3"]), text_values(&["Charlie"]));
        let child_delta = empty_delta();

        parent_delta.merge(child_delta).unwrap();

        assert_eq!(parent_delta.inserts.len(), 1);
        assert_eq!(
            parent_delta.inserts[&text_values(&["3"])],
            text_values(&["Charlie"])
        );
    }

    // Rule 5: insert in both → error
    #[test]
    fn test_merge_rule5_double_insert_error() {
        let mut parent_delta = empty_delta();
        parent_delta
            .inserts
            .insert(text_values(&["3"]), text_values(&["Charlie"]));
        let mut child_delta = empty_delta();
        child_delta
            .inserts
            .insert(text_values(&["3"]), text_values(&["Charles"]));

        let merged_delta = parent_delta.merge(child_delta);
        assert!(merged_delta.is_err());
    }

    // Rule 6: insert then delete → cancels out
    #[test]
    fn test_merge_rule6_insert_then_delete_cancels() {
        let mut parent_delta = empty_delta();
        parent_delta
            .inserts
            .insert(text_values(&["3"]), text_values(&["Charlie"]));
        let mut child_delta = empty_delta();
        child_delta
            .deletes
            .insert(text_values(&["3"]), text_values(&["Charles"]));

        parent_delta.merge(child_delta).unwrap();

        assert!(parent_delta.inserts.is_empty());
        assert!(parent_delta.deletes.is_empty());
        assert!(parent_delta.updates.is_empty());
    }

    // Rule 7: insert then update → insert with new value
    #[test]
    fn test_merge_rule7_insert_then_update() {
        let mut parent_delta = empty_delta();
        parent_delta
            .inserts
            .insert(text_values(&["3"]), text_values(&["Charlie"]));
        let mut child_delta = empty_delta();
        child_delta.updates.insert(
            text_values(&["3"]),
            (text_values(&["Charlie"]), text_values(&["Charles"])),
        );

        parent_delta.merge(child_delta).unwrap();

        assert_eq!(parent_delta.inserts.len(), 1);
        assert_eq!(
            parent_delta.inserts[&text_values(&["3"])],
            text_values(&["Charles"])
        );
        assert!(parent_delta.deletes.is_empty());
        assert!(parent_delta.updates.is_empty());
    }

    // Rule 8: parent delete, no child → delete stays
    #[test]
    fn test_merge_rule8_parent_delete_only() {
        let mut parent_delta = empty_delta();
        parent_delta
            .deletes
            .insert(text_values(&["2"]), text_values(&["Bob"]));
        let child_delta = empty_delta();

        parent_delta.merge(child_delta).unwrap();

        assert_eq!(parent_delta.deletes.len(), 1);
        assert_eq!(
            parent_delta.deletes[&text_values(&["2"])],
            text_values(&["Bob"])
        );
    }

    // Rule 9a: delete then insert with same value → cancels out
    #[test]
    fn test_merge_rule9a_delete_then_insert_same_cancels() {
        let mut parent_delta = empty_delta();
        parent_delta
            .deletes
            .insert(text_values(&["2"]), text_values(&["Bob"]));
        let mut child_delta = empty_delta();
        child_delta
            .inserts
            .insert(text_values(&["2"]), text_values(&["Bob"]));

        parent_delta.merge(child_delta).unwrap();

        assert!(parent_delta.inserts.is_empty());
        assert!(parent_delta.deletes.is_empty());
        assert!(parent_delta.updates.is_empty());
    }

    // Rule 9b: delete then insert with different value → update
    #[test]
    fn test_merge_rule9b_delete_then_insert_different_becomes_update() {
        let mut parent_delta = empty_delta();
        parent_delta
            .deletes
            .insert(text_values(&["2"]), text_values(&["Bob"]));
        let mut child_delta = empty_delta();
        child_delta
            .inserts
            .insert(text_values(&["2"]), text_values(&["Robert"]));

        parent_delta.merge(child_delta).unwrap();

        assert!(parent_delta.inserts.is_empty());
        assert!(parent_delta.deletes.is_empty());
        assert_eq!(parent_delta.updates.len(), 1);
        let (old_value, new_value) = &parent_delta.updates[&text_values(&["2"])];
        assert_eq!(old_value, &text_values(&["Bob"]));
        assert_eq!(new_value, &text_values(&["Robert"]));
    }

    // Rule 10: double delete → error
    #[test]
    fn test_merge_rule10_double_delete_error() {
        let mut parent_delta = empty_delta();
        parent_delta
            .deletes
            .insert(text_values(&["2"]), text_values(&["Bob"]));
        let mut child_delta = empty_delta();
        child_delta
            .deletes
            .insert(text_values(&["2"]), text_values(&["Bob"]));

        let merged_delta = parent_delta.merge(child_delta);
        assert!(merged_delta.is_err());
    }

    // Rule 11: delete then update → error
    #[test]
    fn test_merge_rule11_delete_then_update_error() {
        let mut parent_delta = empty_delta();
        parent_delta
            .deletes
            .insert(text_values(&["2"]), text_values(&["Bob"]));
        let mut child_delta = empty_delta();
        child_delta.updates.insert(
            text_values(&["2"]),
            (text_values(&["Bob"]), text_values(&["Robert"])),
        );

        let merged_delta = parent_delta.merge(child_delta);
        assert!(merged_delta.is_err());
    }

    // Rule 12: parent update, no child → update stays
    #[test]
    fn test_merge_rule12_parent_update_only() {
        let mut parent_delta = empty_delta();
        parent_delta.updates.insert(
            text_values(&["1"]),
            (text_values(&["Alice"]), text_values(&["Alicia"])),
        );
        let child_delta = empty_delta();

        parent_delta.merge(child_delta).unwrap();

        assert_eq!(parent_delta.updates.len(), 1);
        let (old_value, new_value) = &parent_delta.updates[&text_values(&["1"])];
        assert_eq!(old_value, &text_values(&["Alice"]));
        assert_eq!(new_value, &text_values(&["Alicia"]));
    }

    // Rule 13: update then insert → error
    #[test]
    fn test_merge_rule13_update_then_insert_error() {
        let mut parent_delta = empty_delta();
        parent_delta.updates.insert(
            text_values(&["1"]),
            (text_values(&["Alice"]), text_values(&["Alicia"])),
        );
        let mut child_delta = empty_delta();
        child_delta
            .inserts
            .insert(text_values(&["1"]), text_values(&["Alice"]));

        let merged_delta = parent_delta.merge(child_delta);
        assert!(merged_delta.is_err());
    }

    // Rule 14a: update then delete with matching value → delete(old)
    #[test]
    fn test_merge_rule14a_update_then_delete_matching() {
        let mut parent_delta = empty_delta();
        parent_delta.updates.insert(
            text_values(&["1"]),
            (text_values(&["Alice"]), text_values(&["Alicia"])),
        );
        let mut child_delta = empty_delta();
        child_delta
            .deletes
            .insert(text_values(&["1"]), text_values(&["Alicia"]));

        parent_delta.merge(child_delta).unwrap();

        assert!(parent_delta.inserts.is_empty());
        assert!(parent_delta.updates.is_empty());
        assert_eq!(parent_delta.deletes.len(), 1);
        assert_eq!(
            parent_delta.deletes[&text_values(&["1"])],
            text_values(&["Alice"])
        );
    }

    // Rule 14b: update then delete with mismatched value → error
    #[test]
    fn test_merge_rule14b_update_then_delete_mismatch_error() {
        let mut parent_delta = empty_delta();
        parent_delta.updates.insert(
            text_values(&["1"]),
            (text_values(&["Alice"]), text_values(&["Alicia"])),
        );
        let mut child_delta = empty_delta();
        child_delta
            .deletes
            .insert(text_values(&["1"]), text_values(&["Alice"]));

        let merged_delta = parent_delta.merge(child_delta);
        assert!(merged_delta.is_err());
    }

    // Rule 15: update then update → update(old1 → new2)
    #[test]
    fn test_merge_rule15_update_then_update() {
        let mut parent_delta = empty_delta();
        parent_delta.updates.insert(
            text_values(&["1"]),
            (text_values(&["Alice"]), text_values(&["Alicia"])),
        );
        let mut child_delta = empty_delta();
        child_delta.updates.insert(
            text_values(&["1"]),
            (text_values(&["Alicia"]), text_values(&["Ali"])),
        );

        parent_delta.merge(child_delta).unwrap();

        assert_eq!(parent_delta.updates.len(), 1);
        let (old_value, new_value) = &parent_delta.updates[&text_values(&["1"])];
        assert_eq!(old_value, &text_values(&["Alice"]));
        assert_eq!(new_value, &text_values(&["Ali"]));
        assert!(parent_delta.inserts.is_empty());
        assert!(parent_delta.deletes.is_empty());
    }

    // Test merging with multiple keys exercising different rules simultaneously
    #[test]
    fn test_merge_multiple_keys_mixed_rules() {
        let mut parent_delta = empty_delta();
        parent_delta
            .inserts
            .insert(text_values(&["3"]), text_values(&["Charlie"])); // will be updated (rule 7)
        parent_delta
            .deletes
            .insert(text_values(&["2"]), text_values(&["Bob"])); // will be re-inserted different (rule 9b)
        parent_delta.updates.insert(
            text_values(&["1"]),
            (text_values(&["Alice"]), text_values(&["Alicia"])),
        ); // will be updated again (rule 15)

        let mut child_delta = empty_delta();
        child_delta.updates.insert(
            text_values(&["3"]),
            (text_values(&["Charlie"]), text_values(&["Charles"])),
        ); // rule 7
        child_delta
            .inserts
            .insert(text_values(&["2"]), text_values(&["Robert"])); // rule 9b
        child_delta.updates.insert(
            text_values(&["1"]),
            (text_values(&["Alicia"]), text_values(&["Ali"])),
        ); // rule 15
        child_delta
            .inserts
            .insert(text_values(&["4"]), text_values(&["Dave"])); // rule 1

        parent_delta.merge(child_delta).unwrap();

        // Rule 7: insert(3, Charlie) + update(3, Charlie→Charles) = insert(3, Charles)
        assert_eq!(parent_delta.inserts.len(), 2);
        assert_eq!(
            parent_delta.inserts[&text_values(&["3"])],
            text_values(&["Charles"])
        );
        // Rule 1: insert(4, Dave) passes through
        assert_eq!(
            parent_delta.inserts[&text_values(&["4"])],
            text_values(&["Dave"])
        );

        // Rule 9b: delete(2, Bob) + insert(2, Robert) = update(2, Bob→Robert)
        // Rule 15: update(1, Alice→Alicia) + update(1, Alicia→Ali) = update(1, Alice→Ali)
        assert_eq!(parent_delta.updates.len(), 2);
        let (old_value, new_value) = &parent_delta.updates[&text_values(&["2"])];
        assert_eq!(old_value, &text_values(&["Bob"]));
        assert_eq!(new_value, &text_values(&["Robert"]));
        let (old_value, new_value) = &parent_delta.updates[&text_values(&["1"])];
        assert_eq!(old_value, &text_values(&["Alice"]));
        assert_eq!(new_value, &text_values(&["Ali"]));

        assert!(parent_delta.deletes.is_empty());
    }

    // Merge with mismatched field names → error
    #[test]
    fn test_merge_field_mismatch_error() {
        let mut parent_delta = Delta {
            column_names: vec!["id".to_string(), "name".to_string()],
            inserts: HashMap::new(),
            deletes: HashMap::new(),
            updates: HashMap::new(),
        };
        let child_delta = Delta {
            column_names: vec!["id".to_string(), "email".to_string()],
            inserts: HashMap::new(),
            deletes: HashMap::new(),
            updates: HashMap::new(),
        };

        let merged_delta = parent_delta.merge(child_delta);
        assert!(merged_delta.is_err());
        assert!(
            merged_delta
                .unwrap_err()
                .to_string()
                .contains("field mismatch"),
            "error should mention field mismatch"
        );
    }

    // Test merging with composite keys
    #[test]
    fn test_merge_composite_keys() {
        let mut parent_delta = empty_delta();
        parent_delta
            .inserts
            .insert(text_values(&["u1", "o1"]), text_values(&["100"]));
        let mut child_delta = empty_delta();
        child_delta.updates.insert(
            text_values(&["u1", "o1"]),
            (text_values(&["100"]), text_values(&["150"])),
        );

        parent_delta.merge(child_delta).unwrap();

        assert_eq!(parent_delta.inserts.len(), 1);
        assert_eq!(
            parent_delta.inserts[&text_values(&["u1", "o1"])],
            text_values(&["150"])
        );
        assert!(parent_delta.updates.is_empty());
    }
}
