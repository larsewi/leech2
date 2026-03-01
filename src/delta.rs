use std::collections::HashMap;
use std::fmt;

use anyhow::{Context, Result, bail};

use crate::entry::Entry;
use crate::state::State;
use crate::table::Table;
use crate::update::Update;

type RecordMap = HashMap<Vec<String>, Vec<String>>;
type UpdateMap = HashMap<Vec<String>, (Vec<String>, Vec<String>)>;

/// Expand sparse values back to a full-length vector.
/// Positions not in `changed_indices` are filled with empty strings.
fn expand_sparse(
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

/// Delta represents the changes to a single table between two states.
#[derive(Debug, Clone, PartialEq)]
pub struct Delta {
    /// The name of the table this delta applies to.
    pub table_name: String,
    /// The names of all columns, primary key columns first.
    pub column_names: Vec<String>,
    /// Entries that were added (key -> value).
    pub inserts: HashMap<Vec<String>, Vec<String>>,
    /// Entries that were removed (key -> value).
    pub deletes: HashMap<Vec<String>, Vec<String>>,
    /// Entries that were modified (key -> (old_value, new_value)).
    pub updates: HashMap<Vec<String>, (Vec<String>, Vec<String>)>,
}

impl TryFrom<crate::proto::delta::Delta> for Delta {
    type Error = anyhow::Error;

    fn try_from(proto: crate::proto::delta::Delta) -> Result<Self> {
        let num_sub = proto
            .num_sub()
            .map_err(|e| anyhow::anyhow!("corrupt delta '{}': {}", proto.table_name, e))?;

        let inserts = proto
            .inserts
            .into_iter()
            .map(|e| (e.key, e.value))
            .collect();
        let deletes = proto
            .deletes
            .into_iter()
            .map(|e| (e.key, e.value))
            .collect();
        let updates = proto
            .updates
            .into_iter()
            .map(|u| {
                let old_value = expand_sparse(&u.changed_indices, &u.old_value, num_sub);
                let new_value = expand_sparse(&u.changed_indices, &u.new_value, num_sub);
                (u.key, (old_value, new_value))
            })
            .collect();
        Ok(Delta {
            table_name: proto.table_name,
            column_names: proto.column_names,
            inserts,
            deletes,
            updates,
        })
    }
}

impl From<Delta> for crate::proto::delta::Delta {
    fn from(delta: Delta) -> Self {
        let inserts = delta
            .inserts
            .into_iter()
            .map(|(key, value)| Entry { key, value })
            .collect();
        let deletes = delta
            .deletes
            .into_iter()
            .map(|(key, value)| Entry { key, value })
            .collect();
        let updates = delta
            .updates
            .into_iter()
            .map(|(key, (old_value, new_value))| Update {
                key,
                changed_indices: Vec::new(),
                old_value,
                new_value,
            })
            .collect();
        crate::proto::delta::Delta {
            table_name: delta.table_name,
            column_names: delta.column_names,
            inserts,
            deletes,
            updates,
        }
    }
}

/// Format a single column value for update display.
///
/// When `old` is provided and differs from `new`, shows `"old -> new"`.
/// When `old` equals `new`, shows `"_"` (unchanged). When there is no
/// old value, shows just `new`.
fn fmt_update_col(new: &str, old: Option<&str>) -> String {
    match old {
        Some(old) if old != new => format!("{} -> {}", old, new),
        Some(_) => "_".to_string(),
        None => new.to_string(),
    }
}

impl crate::proto::delta::Delta {
    /// Number of subsidiary (non-key) columns.
    ///
    /// The proto format stores keys and values separately, but `column_names`
    /// lists all columns together (PK first, then subsidiary). This method
    /// determines the PK count from the first available entry's key length
    /// (trying inserts, then deletes, then updates; defaulting to 0 if the
    /// delta is empty) and subtracts it from the total column count.
    fn num_sub(&self) -> Result<usize, String> {
        let num_pk = if let Some(entry) = self.inserts.first() {
            entry.key.len()
        } else if let Some(entry) = self.deletes.first() {
            entry.key.len()
        } else if let Some(update) = self.updates.first() {
            update.key.len()
        } else {
            0
        };
        if self.column_names.len() < num_pk {
            return Err(format!(
                "column_names has {} entries but primary key has {}",
                self.column_names.len(),
                num_pk
            ));
        }
        Ok(self.column_names.len() - num_pk)
    }

    fn fmt_inserts(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        if !self.inserts.is_empty() {
            write!(f, "\n  Inserts ({}):", self.inserts.len())?;
            for entry in &self.inserts {
                write!(
                    f,
                    "\n    ({}) {}",
                    entry.key.join(", "),
                    entry.value.join(", ")
                )?;
            }
        }
        Ok(())
    }

    fn fmt_deletes(&self, f: &mut fmt::Formatter<'_>, num_sub: usize) -> fmt::Result {
        if !self.deletes.is_empty() {
            write!(f, "\n  Deletes ({}):", self.deletes.len())?;
            for entry in &self.deletes {
                let vals = if entry.value.is_empty() {
                    vec!["_"; num_sub].join(", ")
                } else {
                    entry.value.join(", ")
                };
                write!(f, "\n    ({}) {}", entry.key.join(", "), vals)?;
            }
        }
        Ok(())
    }

    /// Format update entries. Updates come in two wire formats:
    /// - **Full** (blocks): `changed_indices` is empty and all `num_sub`
    ///   columns are present in `new_value`/`old_value` positionally.
    /// - **Sparse** (patches): only the columns listed in `changed_indices`
    ///   appear in `new_value`/`old_value`; unchanged columns show as `"_"`.
    fn fmt_updates(&self, f: &mut fmt::Formatter<'_>, num_sub: usize) -> fmt::Result {
        if !self.updates.is_empty() {
            write!(f, "\n  Updates ({}):", self.updates.len())?;
            for update in &self.updates {
                let is_full = update.changed_indices.is_empty() && !update.new_value.is_empty();
                let has_old = !update.old_value.is_empty();

                let cols: Vec<String> = if is_full {
                    // Full format (blocks): compare old and new positionally.
                    (0..num_sub)
                        .map(|i| {
                            let new = update
                                .new_value
                                .get(i)
                                .map(|s| s.as_str())
                                .unwrap_or("<missing>");
                            let old = has_old.then(|| {
                                update
                                    .old_value
                                    .get(i)
                                    .map(|s| s.as_str())
                                    .unwrap_or("<missing>")
                            });
                            fmt_update_col(new, old)
                        })
                        .collect()
                } else {
                    // Sparse format (patches): use changed_indices.
                    let changed: std::collections::HashSet<u32> =
                        update.changed_indices.iter().copied().collect();
                    let mut new_iter = update.new_value.iter();
                    let mut old_iter = update.old_value.iter();
                    (0..num_sub as u32)
                        .map(|i| {
                            if changed.contains(&i) {
                                let new =
                                    new_iter.next().map(|s| s.as_str()).unwrap_or("<missing>");
                                let old = has_old.then(|| {
                                    old_iter.next().map(|s| s.as_str()).unwrap_or("<missing>")
                                });
                                fmt_update_col(new, old)
                            } else {
                                "_".to_string()
                            }
                        })
                        .collect()
                };

                write!(f, "\n    ({}) {}", update.key.join(", "), cols.join(", "))?;
            }
        }
        Ok(())
    }
}

impl fmt::Display for crate::proto::delta::Delta {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(
            f,
            "'{}' [{}]",
            self.table_name,
            self.column_names.join(", ")
        )?;
        let num_sub = match self.num_sub() {
            Ok(n) => n,
            Err(e) => return write!(f, " <corrupt delta: {}>", e),
        };
        self.fmt_inserts(f)?;
        self.fmt_deletes(f, num_sub)?;
        self.fmt_updates(f, num_sub)?;
        Ok(())
    }
}

impl Delta {
    /// Merge child delta into parent delta, producing a single delta that
    /// represents the combined effect of both. See DELTA_MERGING_RULES.md for
    /// the full specification of the 15 rules.
    pub fn merge(parent: &mut Self, child: Delta) -> Result<()> {
        if parent.column_names != child.column_names {
            bail!(
                "cannot merge deltas for table '{}': field mismatch ({:?} vs {:?})",
                parent.table_name,
                parent.column_names,
                child.column_names
            );
        }

        for (key, value) in child.inserts {
            Delta::merge_insert(parent, key, value).context("failed to merge inserts")?;
        }
        for (key, value) in child.deletes {
            Delta::merge_delete(parent, key, value).context("failed to merge deletes")?;
        }
        for (key, (old, new)) in child.updates {
            Delta::merge_update(parent, key, old, new).context("failed to merge updates")?;
        }
        Ok(())
    }

    fn merge_insert(parent: &mut Self, key: Vec<String>, insert_value: Vec<String>) -> Result<()> {
        if parent.inserts.contains_key(&key) {
            // Rule 5: double insert → error
            bail!("Rule 5: Key {:?} inserted in both blocks", key);
        } else if let Some(delete_value) = parent.deletes.remove(&key) {
            if delete_value == insert_value {
                // Rule 9a: delete then insert with same value → cancels out
                log::debug!("Rule 9a: delete + insert cancel out for key {:?}", key);
            } else {
                // Rule 9b: delete then insert with different value → update
                log::debug!("Rule 9b: delete + insert becomes update for key {:?}", key);
                parent.updates.insert(key, (delete_value, insert_value));
            }
        } else if parent.updates.contains_key(&key) {
            // Rule 13: insert after update → error
            bail!(
                "Rule 13: Key {:?} updated in parent, inserted in child",
                key
            );
        } else {
            // Rule 1: pass through
            log::debug!("Rule 1: insert passes through for key {:?}", key);
            parent.inserts.insert(key, insert_value);
        }
        Ok(())
    }

    fn merge_delete(parent: &mut Self, key: Vec<String>, delete_value: Vec<String>) -> Result<()> {
        if parent.inserts.remove(&key).is_some() {
            // Rule 6: insert then delete → cancels out
            log::debug!("Rule 6: insert + delete cancel out for key {:?}", key);
        } else if parent.deletes.contains_key(&key) {
            // Rule 10: double delete → error
            bail!("Rule 10: Key {:?} deleted in both blocks", key);
        } else if let Some((old_value, new_value)) = parent.updates.remove(&key) {
            if delete_value == new_value {
                // Rule 14a: update then delete, values match → delete(old)
                log::debug!("Rule 14a: update + delete becomes delete for key {:?}", key);
                parent.deletes.insert(key, old_value);
            } else {
                // Rule 14b: update then delete, values mismatch → error
                bail!(
                    "Rule 14b: Key {:?} updated to {:?} in parent, but deleted with {:?}",
                    key,
                    new_value,
                    delete_value
                );
            }
        } else {
            // Rule 2: pass through
            log::debug!("Rule 2: delete passes through for key {:?}", key);
            parent.deletes.insert(key, delete_value);
        }
        Ok(())
    }

    fn merge_update(
        parent: &mut Self,
        key: Vec<String>,
        old_value: Vec<String>,
        new_value: Vec<String>,
    ) -> Result<()> {
        if let Some(insert_val) = parent.inserts.get_mut(&key) {
            // Rule 7: insert then update → insert(new_val)
            log::debug!("Rule 7: insert + update becomes insert for key {:?}", key);
            *insert_val = new_value;
        } else if parent.deletes.contains_key(&key) {
            // Rule 11: update after delete → error
            bail!("Rule 11: Key {:?} deleted in parent, updated in child", key);
        } else if let Some(update) = parent.updates.get_mut(&key) {
            // Rule 15: update then update → update(old1 → new2)
            // Merge sparse-expanded updates: only touch positions that actually
            // changed in the current update.
            log::debug!("Rule 15: update + update merged for key {:?}", key);
            for i in 0..update.0.len() {
                let parent_changed = update.0[i] != update.1[i];
                let current_changed = old_value[i] != new_value[i];
                if current_changed {
                    update.1[i] = new_value[i].clone();
                    if !parent_changed {
                        update.0[i] = old_value[i].clone();
                    }
                }
            }
        } else {
            // Rule 3: pass through
            log::debug!("Rule 3: update passes through for key {:?}", key);
            parent.updates.insert(key, (old_value, new_value));
        }
        Ok(())
    }

    pub fn compute(previous_state: Option<State>, current_state: &State) -> Vec<Delta> {
        let mut deltas = Vec::new();

        // Process tables in current state
        for (table_name, current_table) in &current_state.tables {
            let previous_table = previous_state
                .as_ref()
                .and_then(|ps| ps.tables.get(table_name));

            let (inserts, deletes, updates) = Self::compute_table(previous_table, current_table);

            // Skip tables with no changes
            if inserts.is_empty() && deletes.is_empty() && updates.is_empty() {
                continue;
            }

            deltas.push(Delta {
                table_name: table_name.clone(),
                column_names: current_table.fields.clone(),
                inserts,
                deletes,
                updates,
            });
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

                deltas.push(Delta {
                    table_name: table_name.clone(),
                    column_names: table.fields.clone(),
                    inserts: HashMap::new(),
                    deletes: table.records.clone(),
                    updates: HashMap::new(),
                });
            }
        }

        deltas
    }

    fn compute_table(
        previous_table: Option<&Table>,
        current_table: &Table,
    ) -> (RecordMap, RecordMap, UpdateMap) {
        let mut inserts = HashMap::new();
        let mut deletes = HashMap::new();
        let mut updates = HashMap::new();

        let Some(previous_table) = previous_table else {
            // No previous table: all records are inserts
            let inserts = current_table.records.clone();
            return (inserts, deletes, updates);
        };

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

    fn make_key(key: &[&str]) -> Vec<String> {
        key.iter().map(|s| s.to_string()).collect()
    }

    fn make_table(rows: &[(&[&str], &[&str])]) -> Table {
        let records = rows
            .iter()
            .map(|(key, value)| {
                (
                    key.iter().map(|s| s.to_string()).collect(),
                    value.iter().map(|s| s.to_string()).collect(),
                )
            })
            .collect();
        Table {
            fields: vec![],
            records,
        }
    }

    fn find_delta<'a>(deltas: &'a [Delta], name: &str) -> Option<&'a Delta> {
        deltas.iter().find(|delta| delta.table_name == name)
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
        let delta = find_delta(&deltas, "users").unwrap();
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
        let delta = find_delta(&deltas, "old_table").unwrap();
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
        let delta = find_delta(&deltas, "users").unwrap();

        // Key "4" is new -> insert
        assert_eq!(delta.inserts.len(), 1);
        assert!(delta.inserts.contains_key(&make_key(&["4"])));

        // Key "2" removed -> delete
        assert_eq!(delta.deletes.len(), 1);
        assert!(delta.deletes.contains_key(&make_key(&["2"])));

        // Key "1" changed value -> update
        // Key "3" has same value -> skipped
        assert_eq!(delta.updates.len(), 1);
        assert!(delta.updates.contains_key(&make_key(&["1"])));
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
        let delta_a = find_delta(&deltas, "table_a").unwrap();
        assert_eq!(delta_a.deletes.len(), 1);
        assert_eq!(delta_a.inserts.len(), 0);

        // table_b: in both -> key "1" deleted, key "2" inserted
        let delta_b = find_delta(&deltas, "table_b").unwrap();
        assert_eq!(delta_b.deletes.len(), 1);
        assert!(delta_b.deletes.contains_key(&make_key(&["1"])));
        assert_eq!(delta_b.inserts.len(), 1);
        assert!(delta_b.inserts.contains_key(&make_key(&["2"])));

        // table_c: only in current -> all inserts
        let delta_c = find_delta(&deltas, "table_c").unwrap();
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
        assert!(find_delta(&deltas, "changed").is_some());
        assert!(find_delta(&deltas, "unchanged").is_none());
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

        let delta = find_delta(&deltas, "orders").unwrap();
        assert_eq!(delta.inserts.len(), 1);
        assert!(delta.inserts.contains_key(&make_key(&["user2", "order1"])));
        assert_eq!(delta.deletes.len(), 1);
        assert!(delta.deletes.contains_key(&make_key(&["user1", "order2"])));
        assert_eq!(delta.updates.len(), 1);
        assert!(delta.updates.contains_key(&make_key(&["user1", "order1"])));
    }

    // ---- Merge tests ----

    fn make_value(value: &[&str]) -> Vec<String> {
        value.iter().map(|s| s.to_string()).collect()
    }

    fn empty_delta() -> Delta {
        Delta {
            table_name: "t".to_string(),
            column_names: vec![],
            inserts: HashMap::new(),
            deletes: HashMap::new(),
            updates: HashMap::new(),
        }
    }

    // Rule 1: child insert, no parent → insert passes through
    #[test]
    fn test_merge_rule1_current_insert_only() {
        let mut parent_delta = empty_delta();
        let mut child_delta = empty_delta();
        child_delta
            .inserts
            .insert(make_key(&["3"]), make_value(&["Charlie"]));

        Delta::merge(&mut parent_delta, child_delta).unwrap();

        assert_eq!(parent_delta.inserts.len(), 1);
        assert_eq!(
            parent_delta.inserts[&make_key(&["3"])],
            make_value(&["Charlie"])
        );
        assert!(parent_delta.deletes.is_empty());
        assert!(parent_delta.updates.is_empty());
    }

    // Rule 2: child delete, no parent → delete passes through
    #[test]
    fn test_merge_rule2_current_delete_only() {
        let mut parent_delta = empty_delta();
        let mut child_delta = empty_delta();
        child_delta
            .deletes
            .insert(make_key(&["2"]), make_value(&["Bob"]));

        Delta::merge(&mut parent_delta, child_delta).unwrap();

        assert_eq!(parent_delta.deletes.len(), 1);
        assert_eq!(
            parent_delta.deletes[&make_key(&["2"])],
            make_value(&["Bob"])
        );
        assert!(parent_delta.inserts.is_empty());
        assert!(parent_delta.updates.is_empty());
    }

    // Rule 3: child update, no parent → update passes through
    #[test]
    fn test_merge_rule3_current_update_only() {
        let mut parent_delta = empty_delta();
        let mut child_delta = empty_delta();
        child_delta.updates.insert(
            make_key(&["1"]),
            (make_value(&["Alice"]), make_value(&["Alicia"])),
        );

        Delta::merge(&mut parent_delta, child_delta).unwrap();

        assert_eq!(parent_delta.updates.len(), 1);
        let (old_value, new_value) = &parent_delta.updates[&make_key(&["1"])];
        assert_eq!(old_value, &make_value(&["Alice"]));
        assert_eq!(new_value, &make_value(&["Alicia"]));
        assert!(parent_delta.inserts.is_empty());
        assert!(parent_delta.deletes.is_empty());
    }

    // Rule 4: parent insert, no child → insert stays
    #[test]
    fn test_merge_rule4_parent_insert_only() {
        let mut parent_delta = empty_delta();
        parent_delta
            .inserts
            .insert(make_key(&["3"]), make_value(&["Charlie"]));
        let child_delta = empty_delta();

        Delta::merge(&mut parent_delta, child_delta).unwrap();

        assert_eq!(parent_delta.inserts.len(), 1);
        assert_eq!(
            parent_delta.inserts[&make_key(&["3"])],
            make_value(&["Charlie"])
        );
    }

    // Rule 5: insert in both → error
    #[test]
    fn test_merge_rule5_double_insert_error() {
        let mut parent_delta = empty_delta();
        parent_delta
            .inserts
            .insert(make_key(&["3"]), make_value(&["Charlie"]));
        let mut child_delta = empty_delta();
        child_delta
            .inserts
            .insert(make_key(&["3"]), make_value(&["Charles"]));

        let merged_delta = Delta::merge(&mut parent_delta, child_delta);
        assert!(merged_delta.is_err());
    }

    // Rule 6: insert then delete → cancels out
    #[test]
    fn test_merge_rule6_insert_then_delete_cancels() {
        let mut parent_delta = empty_delta();
        parent_delta
            .inserts
            .insert(make_key(&["3"]), make_value(&["Charlie"]));
        let mut child_delta = empty_delta();
        child_delta
            .deletes
            .insert(make_key(&["3"]), make_value(&["Charles"]));

        Delta::merge(&mut parent_delta, child_delta).unwrap();

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
            .insert(make_key(&["3"]), make_value(&["Charlie"]));
        let mut child_delta = empty_delta();
        child_delta.updates.insert(
            make_key(&["3"]),
            (make_value(&["Charlie"]), make_value(&["Charles"])),
        );

        Delta::merge(&mut parent_delta, child_delta).unwrap();

        assert_eq!(parent_delta.inserts.len(), 1);
        assert_eq!(
            parent_delta.inserts[&make_key(&["3"])],
            make_value(&["Charles"])
        );
        assert!(parent_delta.deletes.is_empty());
        assert!(parent_delta.updates.is_empty());
    }

    // Rule 8: parent delete, no current → delete stays
    #[test]
    fn test_merge_rule8_parent_delete_only() {
        let mut parent_delta = empty_delta();
        parent_delta
            .deletes
            .insert(make_key(&["2"]), make_value(&["Bob"]));
        let child_delta = empty_delta();

        Delta::merge(&mut parent_delta, child_delta).unwrap();

        assert_eq!(parent_delta.deletes.len(), 1);
        assert_eq!(
            parent_delta.deletes[&make_key(&["2"])],
            make_value(&["Bob"])
        );
    }

    // Rule 9a: delete then insert with same value → cancels out
    #[test]
    fn test_merge_rule9a_delete_then_insert_same_cancels() {
        let mut parent_delta = empty_delta();
        parent_delta
            .deletes
            .insert(make_key(&["2"]), make_value(&["Bob"]));
        let mut child_delta = empty_delta();
        child_delta
            .inserts
            .insert(make_key(&["2"]), make_value(&["Bob"]));

        Delta::merge(&mut parent_delta, child_delta).unwrap();

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
            .insert(make_key(&["2"]), make_value(&["Bob"]));
        let mut child_delta = empty_delta();
        child_delta
            .inserts
            .insert(make_key(&["2"]), make_value(&["Robert"]));

        Delta::merge(&mut parent_delta, child_delta).unwrap();

        assert!(parent_delta.inserts.is_empty());
        assert!(parent_delta.deletes.is_empty());
        assert_eq!(parent_delta.updates.len(), 1);
        let (old_value, new_value) = &parent_delta.updates[&make_key(&["2"])];
        assert_eq!(old_value, &make_value(&["Bob"]));
        assert_eq!(new_value, &make_value(&["Robert"]));
    }

    // Rule 10: double delete → error
    #[test]
    fn test_merge_rule10_double_delete_error() {
        let mut parent_delta = empty_delta();
        parent_delta
            .deletes
            .insert(make_key(&["2"]), make_value(&["Bob"]));
        let mut current_child = empty_delta();
        current_child
            .deletes
            .insert(make_key(&["2"]), make_value(&["Bob"]));

        let merged_delta = Delta::merge(&mut parent_delta, current_child);
        assert!(merged_delta.is_err());
    }

    // Rule 11: delete then update → error
    #[test]
    fn test_merge_rule11_delete_then_update_error() {
        let mut parent_delta = empty_delta();
        parent_delta
            .deletes
            .insert(make_key(&["2"]), make_value(&["Bob"]));
        let mut child_delta = empty_delta();
        child_delta.updates.insert(
            make_key(&["2"]),
            (make_value(&["Bob"]), make_value(&["Robert"])),
        );

        let merged_delta = Delta::merge(&mut parent_delta, child_delta);
        assert!(merged_delta.is_err());
    }

    // Rule 12: parent update, no current → update stays
    #[test]
    fn test_merge_rule12_parent_update_only() {
        let mut parent_delta = empty_delta();
        parent_delta.updates.insert(
            make_key(&["1"]),
            (make_value(&["Alice"]), make_value(&["Alicia"])),
        );
        let child_delta = empty_delta();

        Delta::merge(&mut parent_delta, child_delta).unwrap();

        assert_eq!(parent_delta.updates.len(), 1);
        let (old_value, new_value) = &parent_delta.updates[&make_key(&["1"])];
        assert_eq!(old_value, &make_value(&["Alice"]));
        assert_eq!(new_value, &make_value(&["Alicia"]));
    }

    // Rule 13: update then insert → error
    #[test]
    fn test_merge_rule13_update_then_insert_error() {
        let mut parent_delta = empty_delta();
        parent_delta.updates.insert(
            make_key(&["1"]),
            (make_value(&["Alice"]), make_value(&["Alicia"])),
        );
        let mut child_delta = empty_delta();
        child_delta
            .inserts
            .insert(make_key(&["1"]), make_value(&["Alice"]));

        let merged_delta = Delta::merge(&mut parent_delta, child_delta);
        assert!(merged_delta.is_err());
    }

    // Rule 14a: update then delete with matching value → delete(old)
    #[test]
    fn test_merge_rule14a_update_then_delete_matching() {
        let mut parent_delta = empty_delta();
        parent_delta.updates.insert(
            make_key(&["1"]),
            (make_value(&["Alice"]), make_value(&["Alicia"])),
        );
        let mut child_delta = empty_delta();
        child_delta
            .deletes
            .insert(make_key(&["1"]), make_value(&["Alicia"]));

        Delta::merge(&mut parent_delta, child_delta).unwrap();

        assert!(parent_delta.inserts.is_empty());
        assert!(parent_delta.updates.is_empty());
        assert_eq!(parent_delta.deletes.len(), 1);
        assert_eq!(
            parent_delta.deletes[&make_key(&["1"])],
            make_value(&["Alice"])
        );
    }

    // Rule 14b: update then delete with mismatched value → error
    #[test]
    fn test_merge_rule14b_update_then_delete_mismatch_error() {
        let mut parent_delta = empty_delta();
        parent_delta.updates.insert(
            make_key(&["1"]),
            (make_value(&["Alice"]), make_value(&["Alicia"])),
        );
        let mut child_delta = empty_delta();
        child_delta
            .deletes
            .insert(make_key(&["1"]), make_value(&["Alice"]));

        let merged_delta = Delta::merge(&mut parent_delta, child_delta);
        assert!(merged_delta.is_err());
    }

    // Rule 15: update then update → update(old1 → new2)
    #[test]
    fn test_merge_rule15_update_then_update() {
        let mut parent_delta = empty_delta();
        parent_delta.updates.insert(
            make_key(&["1"]),
            (make_value(&["Alice"]), make_value(&["Alicia"])),
        );
        let mut child_delta = empty_delta();
        child_delta.updates.insert(
            make_key(&["1"]),
            (make_value(&["Alicia"]), make_value(&["Ali"])),
        );

        Delta::merge(&mut parent_delta, child_delta).unwrap();

        assert_eq!(parent_delta.updates.len(), 1);
        let (old_value, new_value) = &parent_delta.updates[&make_key(&["1"])];
        assert_eq!(old_value, &make_value(&["Alice"]));
        assert_eq!(new_value, &make_value(&["Ali"]));
        assert!(parent_delta.inserts.is_empty());
        assert!(parent_delta.deletes.is_empty());
    }

    // Test merging with multiple keys exercising different rules simultaneously
    #[test]
    fn test_merge_multiple_keys_mixed_rules() {
        let mut parent_delta = empty_delta();
        parent_delta
            .inserts
            .insert(make_key(&["3"]), make_value(&["Charlie"])); // will be updated (rule 7)
        parent_delta
            .deletes
            .insert(make_key(&["2"]), make_value(&["Bob"])); // will be re-inserted different (rule 9b)
        parent_delta.updates.insert(
            make_key(&["1"]),
            (make_value(&["Alice"]), make_value(&["Alicia"])),
        ); // will be updated again (rule 15)

        let mut current_delta = empty_delta();
        current_delta.updates.insert(
            make_key(&["3"]),
            (make_value(&["Charlie"]), make_value(&["Charles"])),
        ); // rule 7
        current_delta
            .inserts
            .insert(make_key(&["2"]), make_value(&["Robert"])); // rule 9b
        current_delta.updates.insert(
            make_key(&["1"]),
            (make_value(&["Alicia"]), make_value(&["Ali"])),
        ); // rule 15
        current_delta
            .inserts
            .insert(make_key(&["4"]), make_value(&["Dave"])); // rule 1

        Delta::merge(&mut parent_delta, current_delta).unwrap();

        // Rule 7: insert(3, Charlie) + update(3, Charlie→Charles) = insert(3, Charles)
        assert_eq!(parent_delta.inserts.len(), 2);
        assert_eq!(
            parent_delta.inserts[&make_key(&["3"])],
            make_value(&["Charles"])
        );
        // Rule 1: insert(4, Dave) passes through
        assert_eq!(
            parent_delta.inserts[&make_key(&["4"])],
            make_value(&["Dave"])
        );

        // Rule 9b: delete(2, Bob) + insert(2, Robert) = update(2, Bob→Robert)
        // Rule 15: update(1, Alice→Alicia) + update(1, Alicia→Ali) = update(1, Alice→Ali)
        assert_eq!(parent_delta.updates.len(), 2);
        let (old_value, new_value) = &parent_delta.updates[&make_key(&["2"])];
        assert_eq!(old_value, &make_value(&["Bob"]));
        assert_eq!(new_value, &make_value(&["Robert"]));
        let (old_value, new_value) = &parent_delta.updates[&make_key(&["1"])];
        assert_eq!(old_value, &make_value(&["Alice"]));
        assert_eq!(new_value, &make_value(&["Ali"]));

        assert!(parent_delta.deletes.is_empty());
    }

    // Merge with mismatched field names → error
    #[test]
    fn test_merge_field_mismatch_error() {
        let mut parent_delta = Delta {
            table_name: "t".to_string(),
            column_names: vec!["id".to_string(), "name".to_string()],
            inserts: HashMap::new(),
            deletes: HashMap::new(),
            updates: HashMap::new(),
        };
        let child_delta = Delta {
            table_name: "t".to_string(),
            column_names: vec!["id".to_string(), "email".to_string()],
            inserts: HashMap::new(),
            deletes: HashMap::new(),
            updates: HashMap::new(),
        };

        let merged_delta = Delta::merge(&mut parent_delta, child_delta);
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
            .insert(make_key(&["u1", "o1"]), make_value(&["100"]));
        let mut child_delta = empty_delta();
        child_delta.updates.insert(
            make_key(&["u1", "o1"]),
            (make_value(&["100"]), make_value(&["150"])),
        );

        Delta::merge(&mut parent_delta, child_delta).unwrap();

        assert_eq!(parent_delta.inserts.len(), 1);
        assert_eq!(
            parent_delta.inserts[&make_key(&["u1", "o1"])],
            make_value(&["150"])
        );
        assert!(parent_delta.updates.is_empty());
    }
}
