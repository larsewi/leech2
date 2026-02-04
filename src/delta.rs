use std::collections::HashMap;

use crate::block::{Delta, DeltaEntry};
use crate::state::State;

pub fn compute_delta(
    previous_state: Option<State>,
    current_state: &State,
) -> Vec<Delta> {
    let mut deltas = Vec::new();

    for (table_name, current_table) in &current_state.tables {
        let previous_table = previous_state
            .as_ref()
            .and_then(|ps| ps.tables.get(table_name));

        match previous_table {
            None => {
                // Table only in current state: all rows are inserts
                let inserts: Vec<DeltaEntry> = current_table
                    .rows
                    .iter()
                    .map(|row| DeltaEntry {
                        key: row.primary_key.clone(),
                        value: row.subsidiary_val.clone(),
                    })
                    .collect();

                deltas.push(Delta {
                    name: table_name.clone(),
                    inserts,
                    deletes: Vec::new(),
                    updates: Vec::new(),
                });
            }
            Some(prev_table) => {
                // Table in both states: compare keys
                let prev_map: HashMap<&Vec<String>, &Vec<String>> = prev_table
                    .rows
                    .iter()
                    .map(|row| (&row.primary_key, &row.subsidiary_val))
                    .collect();

                let curr_map: HashMap<&Vec<String>, &Vec<String>> = current_table
                    .rows
                    .iter()
                    .map(|row| (&row.primary_key, &row.subsidiary_val))
                    .collect();

                let mut inserts = Vec::new();
                let mut deletes = Vec::new();
                let mut updates = Vec::new();

                // Keys in previous but not current -> deletes
                for (key, value) in &prev_map {
                    if !curr_map.contains_key(key) {
                        deletes.push(DeltaEntry {
                            key: (*key).clone(),
                            value: (*value).clone(),
                        });
                    }
                }

                // Keys in current but not previous -> inserts
                // Keys in both -> updates
                for (key, value) in &curr_map {
                    if !prev_map.contains_key(key) {
                        inserts.push(DeltaEntry {
                            key: (*key).clone(),
                            value: (*value).clone(),
                        });
                    } else {
                        updates.push(DeltaEntry {
                            key: (*key).clone(),
                            value: (*value).clone(),
                        });
                    }
                }

                deltas.push(Delta {
                    name: table_name.clone(),
                    inserts,
                    deletes,
                    updates,
                });
            }
        }
    }

    // Tables only in previous state: all rows are deletes
    if let Some(ref previous) = previous_state {
        for (table_name, table) in &previous.tables {
            if !current_state.tables.contains_key(table_name) {
                let deletes: Vec<DeltaEntry> = table
                    .rows
                    .iter()
                    .map(|row| DeltaEntry {
                        key: row.primary_key.clone(),
                        value: row.subsidiary_val.clone(),
                    })
                    .collect();

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
