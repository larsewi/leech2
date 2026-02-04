use std::collections::HashMap;

use crate::block::{Delta, DeltaEntry};
use crate::state::{Row, State, Table};

fn row_to_entry(row: &Row) -> DeltaEntry {
    DeltaEntry {
        primary_key: row.primary_key.clone(),
        subsidiary_val: row.subsidiary_val.clone(),
    }
}

fn table_to_map(table: &Table) -> HashMap<&Vec<String>, &Vec<String>> {
    table
        .rows
        .iter()
        .map(|row| (&row.primary_key, &row.subsidiary_val))
        .collect()
}

fn compute_table_delta(
    prev_table: Option<&Table>,
    curr_table: &Table,
) -> (Vec<DeltaEntry>, Vec<DeltaEntry>, Vec<DeltaEntry>) {
    let mut inserts = Vec::new();
    let mut deletes = Vec::new();
    let mut updates = Vec::new();

    let Some(prev_table) = prev_table else {
        // No previous table: all rows are inserts
        let inserts = curr_table.rows.iter().map(row_to_entry).collect();
        return (inserts, deletes, updates);
    };

    let prev_map = table_to_map(prev_table);
    let curr_map = table_to_map(curr_table);

    // Keys in previous but not current -> deletes
    for (key, value) in &prev_map {
        if !curr_map.contains_key(key) {
            deletes.push(DeltaEntry {
                primary_key: (*key).clone(),
                subsidiary_val: (*value).clone(),
            });
        }
    }

    // Keys in current but not previous -> inserts
    // Keys in both with different values -> updates
    for (key, value) in &curr_map {
        match prev_map.get(key) {
            None => {
                inserts.push(DeltaEntry {
                    primary_key: (*key).clone(),
                    subsidiary_val: (*value).clone(),
                });
            }
            Some(prev_value) if prev_value != value => {
                updates.push(DeltaEntry {
                    primary_key: (*key).clone(),
                    subsidiary_val: (*value).clone(),
                });
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

        deltas.push(Delta {
            name: table_name.clone(),
            inserts,
            deletes,
            updates,
        });
    }

    // Tables only in previous state: all rows are deletes
    if let Some(ref previous) = previous_state {
        for (table_name, table) in &previous.tables {
            if !current_state.tables.contains_key(table_name) {
                let deletes = table.rows.iter().map(row_to_entry).collect();

                deltas.push(Delta {
                    name: table_name.clone(),
                    inserts: Vec::new(),
                    deletes,
                    updates: Vec::new(),
                });
            }
        }
    }

    deltas
}

#[cfg(test)]
mod tests {
    use super::*;

    fn make_row(key: &[&str], value: &[&str]) -> Row {
        Row {
            primary_key: key.iter().map(|s| s.to_string()).collect(),
            subsidiary_val: value.iter().map(|s| s.to_string()).collect(),
        }
    }

    fn make_table(rows: Vec<Row>) -> Table {
        Table {
            field_names: vec![],
            primary_key_names: vec![],
            rows,
        }
    }

    fn find_delta<'a>(deltas: &'a [Delta], name: &str) -> Option<&'a Delta> {
        deltas.iter().find(|d| d.name == name)
    }

    fn has_entry(entries: &[DeltaEntry], key: &[&str]) -> bool {
        let key_vec: Vec<String> = key.iter().map(|s| s.to_string()).collect();
        entries.iter().any(|e| e.primary_key == key_vec)
    }

    #[test]
    fn test_no_previous_state_all_inserts() {
        let mut tables = HashMap::new();
        tables.insert(
            "users".to_string(),
            make_table(vec![
                make_row(&["1"], &["alice"]),
                make_row(&["2"], &["bob"]),
            ]),
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
            make_table(vec![
                make_row(&["1"], &["data1"]),
                make_row(&["2"], &["data2"]),
            ]),
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
            make_table(vec![
                make_row(&["1"], &["alice"]),   // will be updated
                make_row(&["2"], &["bob"]),     // will be deleted
                make_row(&["3"], &["charlie"]), // will be updated
            ]),
        );
        let previous = State {
            tables: prev_tables,
        };

        let mut curr_tables = HashMap::new();
        curr_tables.insert(
            "users".to_string(),
            make_table(vec![
                make_row(&["1"], &["alice_updated"]), // update
                make_row(&["3"], &["charlie"]),       // update (same value)
                make_row(&["4"], &["dave"]),          // insert
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
        assert!(has_entry(&delta.inserts, &["4"]));

        // Key "2" removed -> delete
        assert_eq!(delta.deletes.len(), 1);
        assert!(has_entry(&delta.deletes, &["2"]));

        // Key "1" changed value -> update
        // Key "3" has same value -> skipped
        assert_eq!(delta.updates.len(), 1);
        assert!(has_entry(&delta.updates, &["1"]));
    }

    #[test]
    fn test_multiple_tables() {
        let mut prev_tables = HashMap::new();
        prev_tables.insert(
            "table_a".to_string(),
            make_table(vec![make_row(&["1"], &["a"])]),
        );
        prev_tables.insert(
            "table_b".to_string(),
            make_table(vec![make_row(&["1"], &["b"])]),
        );
        let previous = State {
            tables: prev_tables,
        };

        let mut curr_tables = HashMap::new();
        curr_tables.insert(
            "table_b".to_string(),
            make_table(vec![make_row(&["2"], &["b2"])]),
        );
        curr_tables.insert(
            "table_c".to_string(),
            make_table(vec![make_row(&["1"], &["c"])]),
        );
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
        assert!(has_entry(&delta_b.deletes, &["1"]));
        assert_eq!(delta_b.inserts.len(), 1);
        assert!(has_entry(&delta_b.inserts, &["2"]));

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
    fn test_composite_key() {
        let mut prev_tables = HashMap::new();
        prev_tables.insert(
            "orders".to_string(),
            make_table(vec![
                make_row(&["user1", "order1"], &["100"]),
                make_row(&["user1", "order2"], &["200"]),
            ]),
        );
        let previous = State {
            tables: prev_tables,
        };

        let mut curr_tables = HashMap::new();
        curr_tables.insert(
            "orders".to_string(),
            make_table(vec![
                make_row(&["user1", "order1"], &["150"]), // update
                make_row(&["user2", "order1"], &["300"]), // insert (different user)
            ]),
        );
        let current = State {
            tables: curr_tables,
        };

        let deltas = compute_delta(Some(previous), &current);

        let delta = find_delta(&deltas, "orders").unwrap();
        assert_eq!(delta.inserts.len(), 1);
        assert!(has_entry(&delta.inserts, &["user2", "order1"]));
        assert_eq!(delta.deletes.len(), 1);
        assert!(has_entry(&delta.deletes, &["user1", "order2"]));
        assert_eq!(delta.updates.len(), 1);
        assert!(has_entry(&delta.updates, &["user1", "order1"]));
    }
}
