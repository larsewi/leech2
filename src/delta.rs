use crate::block::{Delta, DeltaEntry};
use crate::state::State;

pub fn compute_delta(
    previous_state: Option<State>,
    current_state: &State,
) -> Vec<Delta> {
    let mut deltas = Vec::new();

    for (table_name, table) in &current_state.tables {
        let in_previous = previous_state
            .as_ref()
            .map(|ps| ps.tables.contains_key(table_name))
            .unwrap_or(false);

        if !in_previous {
            let inserts: Vec<DeltaEntry> = table
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
    }

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
