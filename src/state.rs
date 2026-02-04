use std::collections::HashMap;

use prost::Message;

use crate::config;
use crate::table::{Table, load_table};

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

pub fn load_current_state() -> Result<State, Box<dyn std::error::Error>> {
    let cfg = config::get_config()?;
    let mut all_tables: HashMap<String, Table> = HashMap::new();

    for (name, table_config) in &cfg.tables {
        let table = load_table(name, table_config)?;
        all_tables.insert(name.clone(), table);
    }

    let state = State { tables: all_tables };
    log::info!("Loaded current state ({} tables)", state.tables.len());
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
