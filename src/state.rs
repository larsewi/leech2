use std::collections::HashMap;
use std::fs::File;

use prost::Message;

use crate::config::{self, TableConfig};
use crate::entry::Entry;
use crate::table::Table;

pub use crate::proto::state::State;

pub fn load_previous_state() -> Result<Option<State>, Box<dyn std::error::Error>> {
    let cfg = config::get_config()?;
    let state_path = cfg.work_dir.join("previous_state");
    if !state_path.exists() {
        log::info!("No previous state found");
        return Ok(None);
    }

    let data = std::fs::read(&state_path)?;
    let state = State::decode(data.as_slice())?;
    log::info!("Loaded previous state ({} tables)", state.tables.len());
    log::debug!("Previous state: {:#?}", state);
    Ok(Some(state))
}

fn parse_table(
    table: &TableConfig,
    reader: csv::Reader<File>,
) -> Result<HashMap<Vec<String>, Vec<String>>, Box<dyn std::error::Error>> {
    // Find indices for primary key fields and subsidiary fields
    let primary_indices: Vec<usize> = table
        .primary_key
        .iter()
        .filter_map(|pk_col| table.field_names.iter().position(|c| c == pk_col))
        .collect();

    let subsidiary_indices: Vec<usize> = table
        .field_names
        .iter()
        .enumerate()
        .filter(|(_, col)| !table.primary_key.contains(col))
        .map(|(i, _)| i)
        .collect();

    let mut result: HashMap<Vec<String>, Vec<String>> = HashMap::new();

    for record in reader.into_records() {
        let record = record?;

        let primary_key: Vec<String> = primary_indices
            .iter()
            .filter_map(|&i| record.get(i).map(String::from))
            .collect();

        let subsidiary: Vec<String> = subsidiary_indices
            .iter()
            .filter_map(|&i| record.get(i).map(String::from))
            .collect();

        result.insert(primary_key, subsidiary);
    }

    Ok(result)
}

pub fn load_current_state() -> Result<State, Box<dyn std::error::Error>> {
    let cfg = config::get_config()?;
    let mut all_tables: HashMap<String, Table> = HashMap::new();

    for (name, table) in &cfg.tables {
        let source_path = cfg.work_dir.join(&table.source);
        let file = File::open(&source_path)
            .map_err(|e| format!("failed to open '{}': {}", source_path.display(), e))?;
        let reader = csv::ReaderBuilder::new()
            .has_headers(false)
            .from_reader(file);

        let table_data = parse_table(table, reader)?;
        log::info!("Loaded table '{}' ({} records)", name, table_data.len());

        let rows: Vec<Entry> = table_data
            .into_iter()
            .map(|(pk, sub)| Entry {
                key: pk,
                value: sub,
            })
            .collect();

        all_tables.insert(
            name.clone(),
            Table {
                fields: table.field_names.clone(),
                primary_key: table.primary_key.clone(),
                rows,
            },
        );
    }

    let state = State { tables: all_tables };
    log::debug!("Current state: {:#?}", state);
    Ok(state)
}

pub fn save_state(state: &State) -> Result<(), Box<dyn std::error::Error>> {
    let cfg = config::get_config()?;
    let state_path = cfg.work_dir.join("previous_state");

    let mut buf = Vec::new();
    state.encode(&mut buf)?;
    std::fs::write(&state_path, &buf)?;

    log::info!("Stored current state as previous state");
    Ok(())
}
