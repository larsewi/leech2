use std::collections::HashMap;
use std::fmt;

use crate::entry::Entry;
use crate::state::State;
use crate::table::Table;
use crate::update::Update;

type RecordMap = HashMap<Vec<String>, Vec<String>>;
type UpdateMap = HashMap<Vec<String>, (Vec<String>, Vec<String>)>;

/// Expand sparse values back to a full-length vector.
/// Positions not in `changed_indices` are filled with empty strings.
fn expand_sparse(changed_indices: &[u32], sparse_values: &[String], num_sub: usize) -> Vec<String> {
    if changed_indices.is_empty() {
        return sparse_values.to_vec();
    }
    let mut full = vec![String::new(); num_sub];
    for (idx, val) in changed_indices.iter().zip(sparse_values.iter()) {
        full[*idx as usize] = val.clone();
    }
    full
}

/// Delta represents the changes to a single table between two states.
#[derive(Debug, Clone, PartialEq)]
pub struct Delta {
    /// The name of the table this delta applies to.
    pub name: String,
    /// The names of all columns, primary key columns first.
    pub fields: Vec<String>,
    /// Entries that were added (key -> value).
    pub inserts: HashMap<Vec<String>, Vec<String>>,
    /// Entries that were removed (key -> value).
    pub deletes: HashMap<Vec<String>, Vec<String>>,
    /// Entries that were modified (key -> (old_value, new_value)).
    pub updates: HashMap<Vec<String>, (Vec<String>, Vec<String>)>,
}

impl From<crate::proto::delta::Delta> for Delta {
    fn from(proto: crate::proto::delta::Delta) -> Self {
        // Determine subsidiary column count from fields and key length.
        let num_pk = proto
            .inserts
            .first()
            .map(|e| e.key.len())
            .or_else(|| proto.deletes.first().map(|e| e.key.len()))
            .or_else(|| proto.updates.first().map(|u| u.key.len()))
            .unwrap_or(0);
        let num_sub = proto.fields.len().saturating_sub(num_pk);

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
        Delta {
            name: proto.name,
            fields: proto.fields,
            inserts,
            deletes,
            updates,
        }
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
            name: delta.name,
            fields: delta.fields,
            inserts,
            deletes,
            updates,
        }
    }
}

impl crate::proto::delta::Delta {
    /// Number of subsidiary (non-key) fields.
    fn num_sub(&self) -> usize {
        let num_pk = self
            .inserts
            .first()
            .map(|e| e.key.len())
            .or_else(|| self.deletes.first().map(|e| e.key.len()))
            .or_else(|| self.updates.first().map(|u| u.key.len()))
            .unwrap_or(0);
        self.fields.len().saturating_sub(num_pk)
    }
}

impl fmt::Display for crate::proto::delta::Delta {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        let num_sub = self.num_sub();

        write!(f, "'{}' [{}]", self.name, self.fields.join(", "))?;
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
        if !self.updates.is_empty() {
            write!(f, "\n  Updates ({}):", self.updates.len())?;
            for update in &self.updates {
                let is_full = update.changed_indices.is_empty() && !update.new_value.is_empty();
                let has_old = !update.old_value.is_empty();

                let cols: Vec<String> = if is_full {
                    // Full format (blocks): compare old and new positionally.
                    (0..num_sub)
                        .map(|i| {
                            let new = update.new_value.get(i).map(|s| s.as_str()).unwrap_or("?");
                            if has_old {
                                let old =
                                    update.old_value.get(i).map(|s| s.as_str()).unwrap_or("?");
                                if old != new {
                                    format!("{} -> {}", old, new)
                                } else {
                                    "_".to_string()
                                }
                            } else {
                                new.to_string()
                            }
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
                                let new = new_iter.next().map(|s| s.as_str()).unwrap_or("?");
                                if has_old {
                                    let old = old_iter.next().map(|s| s.as_str()).unwrap_or("?");
                                    format!("{} -> {}", old, new)
                                } else {
                                    new.to_string()
                                }
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

impl Delta {
    /// Merge another delta (Child) into this delta (Parent), producing a single
    /// delta that represents the combined effect of both. See
    /// DELTA_MERGING_RULES.md for the full specification of the 15 rules.
    pub fn merge(&mut self, other: Delta) -> Result<(), Box<dyn std::error::Error>> {
        if self.fields != other.fields {
            return Err(format!(
                "cannot merge deltas for table '{}': field mismatch ({:?} vs {:?})",
                self.name, self.fields, other.fields
            )
            .into());
        }

        for (key, val) in other.inserts {
            self.merge_insert(key, val)?;
        }
        for (key, val) in other.deletes {
            self.merge_delete(key, val)?;
        }
        for (key, (old, new)) in other.updates {
            self.merge_update(key, old, new)?;
        }
        Ok(())
    }

    fn merge_insert(
        &mut self,
        key: Vec<String>,
        val: Vec<String>,
    ) -> Result<(), Box<dyn std::error::Error>> {
        if self.inserts.contains_key(&key) {
            // Rule 5: double insert → error
            log::debug!("Rule 5: key {:?} inserted in both blocks", key);
            return Err(format!("Conflict: key {:?} inserted in both blocks", key).into());
        } else if let Some(del_val) = self.deletes.remove(&key) {
            if del_val == val {
                // Rule 9a: delete then insert with same value → cancels out
                log::debug!("Rule 9a: delete + insert cancel out for key {:?}", key);
            } else {
                // Rule 9b: delete then insert with different value → update
                log::debug!("Rule 9b: delete + insert becomes update for key {:?}", key);
                self.updates.insert(key, (del_val, val));
            }
        } else if self.updates.contains_key(&key) {
            // Rule 13: insert after update → error
            log::debug!(
                "Rule 13: key {:?} updated in parent, inserted in current",
                key
            );
            return Err(format!(
                "Conflict: key {:?} updated in parent, inserted in current",
                key
            )
            .into());
        } else {
            // Rule 1: pass through
            log::debug!("Rule 1: insert passes through for key {:?}", key);
            self.inserts.insert(key, val);
        }
        Ok(())
    }

    fn merge_delete(
        &mut self,
        key: Vec<String>,
        val: Vec<String>,
    ) -> Result<(), Box<dyn std::error::Error>> {
        if self.inserts.remove(&key).is_some() {
            // Rule 6: insert then delete → cancels out
            log::debug!("Rule 6: insert + delete cancel out for key {:?}", key);
        } else if self.deletes.contains_key(&key) {
            // Rule 10: double delete → error
            log::debug!("Rule 10: key {:?} deleted in both blocks", key);
            return Err(format!("Conflict: key {:?} deleted in both blocks", key).into());
        } else if let Some((old, new_val)) = self.updates.remove(&key) {
            if val == new_val {
                // Rule 14a: update then delete, values match → delete(old)
                log::debug!("Rule 14a: update + delete becomes delete for key {:?}", key);
                self.deletes.insert(key, old);
            } else {
                // Rule 14b: update then delete, values mismatch → error
                log::debug!(
                    "Rule 14b: key {:?} updated to {:?} in parent, but deleted with {:?}",
                    key,
                    new_val,
                    val
                );
                return Err(format!(
                    "Conflict: key {:?} updated to {:?} in parent, but deleted with {:?}",
                    key, new_val, val
                )
                .into());
            }
        } else {
            // Rule 2: pass through
            log::debug!("Rule 2: delete passes through for key {:?}", key);
            self.deletes.insert(key, val);
        }
        Ok(())
    }

    fn merge_update(
        &mut self,
        key: Vec<String>,
        other_old: Vec<String>,
        other_new: Vec<String>,
    ) -> Result<(), Box<dyn std::error::Error>> {
        if let Some(insert_val) = self.inserts.get_mut(&key) {
            // Rule 7: insert then update → insert(new_val)
            log::debug!("Rule 7: insert + update becomes insert for key {:?}", key);
            *insert_val = other_new;
        } else if self.deletes.contains_key(&key) {
            // Rule 11: update after delete → error
            log::debug!(
                "Rule 11: key {:?} deleted in parent, updated in current",
                key
            );
            return Err(format!(
                "Conflict: key {:?} deleted in parent, updated in current",
                key
            )
            .into());
        } else if let Some(update) = self.updates.get_mut(&key) {
            // Rule 15: update then update → update(old1 → new2)
            // Merge sparse-expanded updates: only touch positions that actually
            // changed in the current update.
            log::debug!("Rule 15: update + update merged for key {:?}", key);
            for i in 0..update.0.len() {
                let parent_changed = update.0[i] != update.1[i];
                let current_changed = other_old[i] != other_new[i];
                if current_changed {
                    update.1[i] = other_new[i].clone();
                    if !parent_changed {
                        update.0[i] = other_old[i].clone();
                    }
                }
            }
        } else {
            // Rule 3: pass through
            log::debug!("Rule 3: update passes through for key {:?}", key);
            self.updates.insert(key, (other_old, other_new));
        }
        Ok(())
    }

    pub fn compute(previous_state: Option<State>, current_state: &State) -> Vec<Delta> {
        let mut deltas = Vec::new();

        // Process tables in current state
        for (table_name, current_table) in &current_state.tables {
            let prev_table = previous_state
                .as_ref()
                .and_then(|ps| ps.tables.get(table_name));

            let (inserts, deletes, updates) = Self::compute_table(prev_table, current_table);

            // Skip tables with no changes
            if inserts.is_empty() && deletes.is_empty() && updates.is_empty() {
                continue;
            }

            deltas.push(Delta {
                name: table_name.clone(),
                fields: current_table.fields.clone(),
                inserts,
                deletes,
                updates,
            });
        }

        // Tables only in previous state: all records are deletes
        if let Some(ref previous) = previous_state {
            for (table_name, table) in &previous.tables {
                // Skip empty tables
                if table.records.is_empty() {
                    continue;
                }

                // Skip if table exists in current state (this is already handled above)
                if current_state.tables.contains_key(table_name) {
                    continue;
                }

                deltas.push(Delta {
                    name: table_name.clone(),
                    fields: table.fields.clone(),
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
            .map(|(k, v)| {
                (
                    k.iter().map(|s| s.to_string()).collect(),
                    v.iter().map(|s| s.to_string()).collect(),
                )
            })
            .collect();
        Table {
            fields: vec![],
            records,
        }
    }

    fn find_delta<'a>(deltas: &'a [Delta], name: &str) -> Option<&'a Delta> {
        deltas.iter().find(|d| d.name == name)
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
        let mut prev_tables = HashMap::new();
        prev_tables.insert(
            "users".to_string(),
            make_table(&[
                (&["1"], &["alice"]),   // will be updated
                (&["2"], &["bob"]),     // will be deleted
                (&["3"], &["charlie"]), // unchanged
            ]),
        );
        let previous = State {
            tables: prev_tables,
        };

        let mut curr_tables = HashMap::new();
        curr_tables.insert(
            "users".to_string(),
            make_table(&[
                (&["1"], &["alice_updated"]), // update
                (&["3"], &["charlie"]),       // unchanged
                (&["4"], &["dave"]),          // insert
            ]),
        );
        let current = State {
            tables: curr_tables,
        };

        let deltas = Delta::compute(Some(previous), &current);

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
        let mut prev_tables = HashMap::new();
        prev_tables.insert("table_a".to_string(), make_table(&[(&["1"], &["a"])]));
        prev_tables.insert("table_b".to_string(), make_table(&[(&["1"], &["b"])]));
        let previous = State {
            tables: prev_tables,
        };

        let mut curr_tables = HashMap::new();
        curr_tables.insert("table_b".to_string(), make_table(&[(&["2"], &["b2"])]));
        curr_tables.insert("table_c".to_string(), make_table(&[(&["1"], &["c"])]));
        let current = State {
            tables: curr_tables,
        };

        let deltas = Delta::compute(Some(previous), &current);

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
        let previous = State {
            tables: HashMap::new(),
        };
        let current = State {
            tables: HashMap::new(),
        };

        let deltas = Delta::compute(Some(previous), &current);
        assert_eq!(deltas.len(), 0);
    }

    #[test]
    fn test_unchanged_table_skipped() {
        let mut prev_tables = HashMap::new();
        prev_tables.insert(
            "unchanged".to_string(),
            make_table(&[(&["1"], &["alice"]), (&["2"], &["bob"])]),
        );
        prev_tables.insert(
            "changed".to_string(),
            make_table(&[(&["1"], &["old_value"])]),
        );
        let previous = State {
            tables: prev_tables,
        };

        let mut curr_tables = HashMap::new();
        curr_tables.insert(
            "unchanged".to_string(),
            make_table(&[(&["1"], &["alice"]), (&["2"], &["bob"])]),
        );
        curr_tables.insert(
            "changed".to_string(),
            make_table(&[(&["1"], &["new_value"])]),
        );
        let current = State {
            tables: curr_tables,
        };

        let deltas = Delta::compute(Some(previous), &current);

        // Only the changed table should have a delta
        assert_eq!(deltas.len(), 1);
        assert!(find_delta(&deltas, "changed").is_some());
        assert!(find_delta(&deltas, "unchanged").is_none());
    }

    #[test]
    fn test_composite_key() {
        let mut prev_tables = HashMap::new();
        prev_tables.insert(
            "orders".to_string(),
            make_table(&[
                (&["user1", "order1"], &["100"]),
                (&["user1", "order2"], &["200"]),
            ]),
        );
        let previous = State {
            tables: prev_tables,
        };

        let mut curr_tables = HashMap::new();
        curr_tables.insert(
            "orders".to_string(),
            make_table(&[
                (&["user1", "order1"], &["150"]), // update
                (&["user2", "order1"], &["300"]), // insert (different user)
            ]),
        );
        let current = State {
            tables: curr_tables,
        };

        let deltas = Delta::compute(Some(previous), &current);

        let delta = find_delta(&deltas, "orders").unwrap();
        assert_eq!(delta.inserts.len(), 1);
        assert!(delta.inserts.contains_key(&make_key(&["user2", "order1"])));
        assert_eq!(delta.deletes.len(), 1);
        assert!(delta.deletes.contains_key(&make_key(&["user1", "order2"])));
        assert_eq!(delta.updates.len(), 1);
        assert!(delta.updates.contains_key(&make_key(&["user1", "order1"])));
    }

    // ---- Merge tests ----

    fn make_val(val: &[&str]) -> Vec<String> {
        val.iter().map(|s| s.to_string()).collect()
    }

    fn empty_delta() -> Delta {
        Delta {
            name: "t".to_string(),
            fields: vec![],
            inserts: HashMap::new(),
            deletes: HashMap::new(),
            updates: HashMap::new(),
        }
    }

    // Rule 1: current insert, no parent → insert passes through
    #[test]
    fn test_merge_rule1_current_insert_only() {
        let mut parent = empty_delta();
        let mut current = empty_delta();
        current
            .inserts
            .insert(make_key(&["3"]), make_val(&["Charlie"]));

        parent.merge(current).unwrap();

        assert_eq!(parent.inserts.len(), 1);
        assert_eq!(parent.inserts[&make_key(&["3"])], make_val(&["Charlie"]));
        assert!(parent.deletes.is_empty());
        assert!(parent.updates.is_empty());
    }

    // Rule 2: current delete, no parent → delete passes through
    #[test]
    fn test_merge_rule2_current_delete_only() {
        let mut parent = empty_delta();
        let mut current = empty_delta();
        current.deletes.insert(make_key(&["2"]), make_val(&["Bob"]));

        parent.merge(current).unwrap();

        assert_eq!(parent.deletes.len(), 1);
        assert_eq!(parent.deletes[&make_key(&["2"])], make_val(&["Bob"]));
        assert!(parent.inserts.is_empty());
        assert!(parent.updates.is_empty());
    }

    // Rule 3: current update, no parent → update passes through
    #[test]
    fn test_merge_rule3_current_update_only() {
        let mut parent = empty_delta();
        let mut current = empty_delta();
        current.updates.insert(
            make_key(&["1"]),
            (make_val(&["Alice"]), make_val(&["Alicia"])),
        );

        parent.merge(current).unwrap();

        assert_eq!(parent.updates.len(), 1);
        let (old, new) = &parent.updates[&make_key(&["1"])];
        assert_eq!(old, &make_val(&["Alice"]));
        assert_eq!(new, &make_val(&["Alicia"]));
        assert!(parent.inserts.is_empty());
        assert!(parent.deletes.is_empty());
    }

    // Rule 4: parent insert, no current → insert stays
    #[test]
    fn test_merge_rule4_parent_insert_only() {
        let mut parent = empty_delta();
        parent
            .inserts
            .insert(make_key(&["3"]), make_val(&["Charlie"]));
        let current = empty_delta();

        parent.merge(current).unwrap();

        assert_eq!(parent.inserts.len(), 1);
        assert_eq!(parent.inserts[&make_key(&["3"])], make_val(&["Charlie"]));
    }

    // Rule 5: insert in both → error
    #[test]
    fn test_merge_rule5_double_insert_error() {
        let mut parent = empty_delta();
        parent
            .inserts
            .insert(make_key(&["3"]), make_val(&["Charlie"]));
        let mut current = empty_delta();
        current
            .inserts
            .insert(make_key(&["3"]), make_val(&["Charles"]));

        let result = parent.merge(current);
        assert!(result.is_err());
    }

    // Rule 6: insert then delete → cancels out
    #[test]
    fn test_merge_rule6_insert_then_delete_cancels() {
        let mut parent = empty_delta();
        parent
            .inserts
            .insert(make_key(&["3"]), make_val(&["Charlie"]));
        let mut current = empty_delta();
        current
            .deletes
            .insert(make_key(&["3"]), make_val(&["Charles"]));

        parent.merge(current).unwrap();

        assert!(parent.inserts.is_empty());
        assert!(parent.deletes.is_empty());
        assert!(parent.updates.is_empty());
    }

    // Rule 7: insert then update → insert with new value
    #[test]
    fn test_merge_rule7_insert_then_update() {
        let mut parent = empty_delta();
        parent
            .inserts
            .insert(make_key(&["3"]), make_val(&["Charlie"]));
        let mut current = empty_delta();
        current.updates.insert(
            make_key(&["3"]),
            (make_val(&["Charlie"]), make_val(&["Charles"])),
        );

        parent.merge(current).unwrap();

        assert_eq!(parent.inserts.len(), 1);
        assert_eq!(parent.inserts[&make_key(&["3"])], make_val(&["Charles"]));
        assert!(parent.deletes.is_empty());
        assert!(parent.updates.is_empty());
    }

    // Rule 8: parent delete, no current → delete stays
    #[test]
    fn test_merge_rule8_parent_delete_only() {
        let mut parent = empty_delta();
        parent.deletes.insert(make_key(&["2"]), make_val(&["Bob"]));
        let current = empty_delta();

        parent.merge(current).unwrap();

        assert_eq!(parent.deletes.len(), 1);
        assert_eq!(parent.deletes[&make_key(&["2"])], make_val(&["Bob"]));
    }

    // Rule 9a: delete then insert with same value → cancels out
    #[test]
    fn test_merge_rule9a_delete_then_insert_same_cancels() {
        let mut parent = empty_delta();
        parent.deletes.insert(make_key(&["2"]), make_val(&["Bob"]));
        let mut current = empty_delta();
        current.inserts.insert(make_key(&["2"]), make_val(&["Bob"]));

        parent.merge(current).unwrap();

        assert!(parent.inserts.is_empty());
        assert!(parent.deletes.is_empty());
        assert!(parent.updates.is_empty());
    }

    // Rule 9b: delete then insert with different value → update
    #[test]
    fn test_merge_rule9b_delete_then_insert_different_becomes_update() {
        let mut parent = empty_delta();
        parent.deletes.insert(make_key(&["2"]), make_val(&["Bob"]));
        let mut current = empty_delta();
        current
            .inserts
            .insert(make_key(&["2"]), make_val(&["Robert"]));

        parent.merge(current).unwrap();

        assert!(parent.inserts.is_empty());
        assert!(parent.deletes.is_empty());
        assert_eq!(parent.updates.len(), 1);
        let (old, new) = &parent.updates[&make_key(&["2"])];
        assert_eq!(old, &make_val(&["Bob"]));
        assert_eq!(new, &make_val(&["Robert"]));
    }

    // Rule 10: double delete → error
    #[test]
    fn test_merge_rule10_double_delete_error() {
        let mut parent = empty_delta();
        parent.deletes.insert(make_key(&["2"]), make_val(&["Bob"]));
        let mut current = empty_delta();
        current.deletes.insert(make_key(&["2"]), make_val(&["Bob"]));

        let result = parent.merge(current);
        assert!(result.is_err());
    }

    // Rule 11: delete then update → error
    #[test]
    fn test_merge_rule11_delete_then_update_error() {
        let mut parent = empty_delta();
        parent.deletes.insert(make_key(&["2"]), make_val(&["Bob"]));
        let mut current = empty_delta();
        current.updates.insert(
            make_key(&["2"]),
            (make_val(&["Bob"]), make_val(&["Robert"])),
        );

        let result = parent.merge(current);
        assert!(result.is_err());
    }

    // Rule 12: parent update, no current → update stays
    #[test]
    fn test_merge_rule12_parent_update_only() {
        let mut parent = empty_delta();
        parent.updates.insert(
            make_key(&["1"]),
            (make_val(&["Alice"]), make_val(&["Alicia"])),
        );
        let current = empty_delta();

        parent.merge(current).unwrap();

        assert_eq!(parent.updates.len(), 1);
        let (old, new) = &parent.updates[&make_key(&["1"])];
        assert_eq!(old, &make_val(&["Alice"]));
        assert_eq!(new, &make_val(&["Alicia"]));
    }

    // Rule 13: update then insert → error
    #[test]
    fn test_merge_rule13_update_then_insert_error() {
        let mut parent = empty_delta();
        parent.updates.insert(
            make_key(&["1"]),
            (make_val(&["Alice"]), make_val(&["Alicia"])),
        );
        let mut current = empty_delta();
        current
            .inserts
            .insert(make_key(&["1"]), make_val(&["Alice"]));

        let result = parent.merge(current);
        assert!(result.is_err());
    }

    // Rule 14a: update then delete with matching value → delete(old)
    #[test]
    fn test_merge_rule14a_update_then_delete_matching() {
        let mut parent = empty_delta();
        parent.updates.insert(
            make_key(&["1"]),
            (make_val(&["Alice"]), make_val(&["Alicia"])),
        );
        let mut current = empty_delta();
        current
            .deletes
            .insert(make_key(&["1"]), make_val(&["Alicia"]));

        parent.merge(current).unwrap();

        assert!(parent.inserts.is_empty());
        assert!(parent.updates.is_empty());
        assert_eq!(parent.deletes.len(), 1);
        assert_eq!(parent.deletes[&make_key(&["1"])], make_val(&["Alice"]));
    }

    // Rule 14b: update then delete with mismatched value → error
    #[test]
    fn test_merge_rule14b_update_then_delete_mismatch_error() {
        let mut parent = empty_delta();
        parent.updates.insert(
            make_key(&["1"]),
            (make_val(&["Alice"]), make_val(&["Alicia"])),
        );
        let mut current = empty_delta();
        current
            .deletes
            .insert(make_key(&["1"]), make_val(&["Alice"]));

        let result = parent.merge(current);
        assert!(result.is_err());
    }

    // Rule 15: update then update → update(old1 → new2)
    #[test]
    fn test_merge_rule15_update_then_update() {
        let mut parent = empty_delta();
        parent.updates.insert(
            make_key(&["1"]),
            (make_val(&["Alice"]), make_val(&["Alicia"])),
        );
        let mut current = empty_delta();
        current.updates.insert(
            make_key(&["1"]),
            (make_val(&["Alicia"]), make_val(&["Ali"])),
        );

        parent.merge(current).unwrap();

        assert_eq!(parent.updates.len(), 1);
        let (old, new) = &parent.updates[&make_key(&["1"])];
        assert_eq!(old, &make_val(&["Alice"]));
        assert_eq!(new, &make_val(&["Ali"]));
        assert!(parent.inserts.is_empty());
        assert!(parent.deletes.is_empty());
    }

    // Test merging with multiple keys exercising different rules simultaneously
    #[test]
    fn test_merge_multiple_keys_mixed_rules() {
        let mut parent = empty_delta();
        parent
            .inserts
            .insert(make_key(&["3"]), make_val(&["Charlie"])); // will be updated (rule 7)
        parent.deletes.insert(make_key(&["2"]), make_val(&["Bob"])); // will be re-inserted different (rule 9b)
        parent.updates.insert(
            make_key(&["1"]),
            (make_val(&["Alice"]), make_val(&["Alicia"])),
        ); // will be updated again (rule 15)

        let mut current = empty_delta();
        current.updates.insert(
            make_key(&["3"]),
            (make_val(&["Charlie"]), make_val(&["Charles"])),
        ); // rule 7
        current
            .inserts
            .insert(make_key(&["2"]), make_val(&["Robert"])); // rule 9b
        current.updates.insert(
            make_key(&["1"]),
            (make_val(&["Alicia"]), make_val(&["Ali"])),
        ); // rule 15
        current
            .inserts
            .insert(make_key(&["4"]), make_val(&["Dave"])); // rule 1

        parent.merge(current).unwrap();

        // Rule 7: insert(3, Charlie) + update(3, Charlie→Charles) = insert(3, Charles)
        assert_eq!(parent.inserts.len(), 2);
        assert_eq!(parent.inserts[&make_key(&["3"])], make_val(&["Charles"]));
        // Rule 1: insert(4, Dave) passes through
        assert_eq!(parent.inserts[&make_key(&["4"])], make_val(&["Dave"]));

        // Rule 9b: delete(2, Bob) + insert(2, Robert) = update(2, Bob→Robert)
        // Rule 15: update(1, Alice→Alicia) + update(1, Alicia→Ali) = update(1, Alice→Ali)
        assert_eq!(parent.updates.len(), 2);
        let (old, new) = &parent.updates[&make_key(&["2"])];
        assert_eq!(old, &make_val(&["Bob"]));
        assert_eq!(new, &make_val(&["Robert"]));
        let (old, new) = &parent.updates[&make_key(&["1"])];
        assert_eq!(old, &make_val(&["Alice"]));
        assert_eq!(new, &make_val(&["Ali"]));

        assert!(parent.deletes.is_empty());
    }

    // Merge with mismatched field names → error
    #[test]
    fn test_merge_field_mismatch_error() {
        let mut parent = Delta {
            name: "t".to_string(),
            fields: vec!["id".to_string(), "name".to_string()],
            inserts: HashMap::new(),
            deletes: HashMap::new(),
            updates: HashMap::new(),
        };
        let other = Delta {
            name: "t".to_string(),
            fields: vec!["id".to_string(), "email".to_string()],
            inserts: HashMap::new(),
            deletes: HashMap::new(),
            updates: HashMap::new(),
        };

        let result = parent.merge(other);
        assert!(result.is_err());
        assert!(
            result.unwrap_err().to_string().contains("field mismatch"),
            "error should mention field mismatch"
        );
    }

    // Test merging with composite keys
    #[test]
    fn test_merge_composite_keys() {
        let mut parent = empty_delta();
        parent
            .inserts
            .insert(make_key(&["u1", "o1"]), make_val(&["100"]));
        let mut current = empty_delta();
        current.updates.insert(
            make_key(&["u1", "o1"]),
            (make_val(&["100"]), make_val(&["150"])),
        );

        parent.merge(current).unwrap();

        assert_eq!(parent.inserts.len(), 1);
        assert_eq!(parent.inserts[&make_key(&["u1", "o1"])], make_val(&["150"]));
        assert!(parent.updates.is_empty());
    }
}
