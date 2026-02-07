use std::collections::HashMap;

use crate::entry::Entry;
use crate::state::State;
use crate::table::Table;
use crate::update::Update;

/// Delta represents the changes to a single table between two states.
#[derive(Debug, Clone, PartialEq)]
pub struct Delta {
    /// The name of the table this delta applies to.
    pub name: String,
    /// Entries that were added (key -> value).
    pub inserts: HashMap<Vec<String>, Vec<String>>,
    /// Entries that were removed (key -> value).
    pub deletes: HashMap<Vec<String>, Vec<String>>,
    /// Entries that were modified (key -> (old_value, new_value)).
    pub updates: HashMap<Vec<String>, (Vec<String>, Vec<String>)>,
}

impl From<crate::proto::delta::Delta> for Delta {
    fn from(proto: crate::proto::delta::Delta) -> Self {
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
            .map(|u| (u.key, (u.old_value, u.new_value)))
            .collect();
        Delta {
            name: proto.name,
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
                old_value,
                new_value,
            })
            .collect();
        crate::proto::delta::Delta {
            name: delta.name,
            inserts,
            deletes,
            updates,
        }
    }
}

pub fn merge_deltas(_parent: &mut Delta, mut _current: Delta) {
    // TODO: Implement merge logic
    log::debug!("merge_deltas()");
}

type Inserts = HashMap<Vec<String>, Vec<String>>;
type Deletes = HashMap<Vec<String>, Vec<String>>;
type Updates = HashMap<Vec<String>, (Vec<String>, Vec<String>)>;

fn compute_table_delta(
    prev_table: Option<&Table>,
    curr_table: &Table,
) -> (Inserts, Deletes, Updates) {
    let mut inserts = HashMap::new();
    let mut deletes = HashMap::new();
    let mut updates = HashMap::new();

    let Some(prev_table) = prev_table else {
        // No previous table: all records are inserts
        let inserts = curr_table.records.clone();
        return (inserts, deletes, updates);
    };

    // Keys in previous but not current -> deletes
    for (k, v) in &prev_table.records {
        if !curr_table.records.contains_key(k) {
            deletes.insert(k.clone(), v.clone());
        }
    }

    // Keys in current but not previous -> inserts
    // Keys in both with different values -> updates
    for (k, v) in &curr_table.records {
        match prev_table.records.get(k) {
            None => {
                inserts.insert(k.clone(), v.clone());
            }
            Some(prev_value) if prev_value != v => {
                updates.insert(k.clone(), (prev_value.clone(), v.clone()));
            }
            _ => {} // Same value, skip
        }
    }

    (inserts, deletes, updates)
}

pub fn compute_delta(previous_state: Option<State>, current_state: &State) -> Vec<Delta> {
    let mut deltas = Vec::new();

    // Process tables in current state
    for (table_name, current_table) in &current_state.tables {
        let prev_table = previous_state
            .as_ref()
            .and_then(|ps| ps.tables.get(table_name));

        let (inserts, deletes, updates) = compute_table_delta(prev_table, current_table);

        // Skip tables with no changes
        if inserts.is_empty() && deletes.is_empty() && updates.is_empty() {
            continue;
        }

        deltas.push(Delta {
            name: table_name.clone(),
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
                inserts: HashMap::new(),
                deletes: table.records.clone(),
                updates: HashMap::new(),
            });
        }
    }

    deltas
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
            primary_key: vec![],
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

        let deltas = compute_delta(None, &current);

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

        let deltas = compute_delta(Some(previous), &current);

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

        let deltas = compute_delta(Some(previous), &current);

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

        let deltas = compute_delta(Some(previous), &current);

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

        let deltas = compute_delta(Some(previous), &current);
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

        let deltas = compute_delta(Some(previous), &current);

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

        let deltas = compute_delta(Some(previous), &current);

        let delta = find_delta(&deltas, "orders").unwrap();
        assert_eq!(delta.inserts.len(), 1);
        assert!(delta.inserts.contains_key(&make_key(&["user2", "order1"])));
        assert_eq!(delta.deletes.len(), 1);
        assert!(delta.deletes.contains_key(&make_key(&["user1", "order2"])));
        assert_eq!(delta.updates.len(), 1);
        assert!(delta.updates.contains_key(&make_key(&["user1", "order1"])));
    }
}
