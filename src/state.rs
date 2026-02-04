use std::collections::HashMap;

use prost::Message;

use crate::config;
use crate::table::{Table, load_table};

pub use crate::proto::state::State;

pub fn load_previous_state() -> Result<Option<State>, Box<dyn std::error::Error>> {
    let config = config::get_config()?;
    let path = config.work_dir.join("previous_state");
    if !path.exists() {
        log::info!("No previous state found");
        return Ok(None);
    }

    log::debug!("Parsing previous state from file '{}'...", path.display());

    let data = std::fs::read(&path)?;
    let state = State::decode(data.as_slice())?;
    log::debug!("{:#?}", state);
    log::info!("Loaded previous state with {} tables", state.tables.len());
    Ok(Some(state))
}

pub fn load_current_state() -> Result<State, Box<dyn std::error::Error>> {
    let config = config::get_config()?;
    let mut tables: HashMap<String, Table> = HashMap::new();

    for (name, config) in &config.tables {
        let table = load_table(name, config)?;
        tables.insert(name.clone(), table);
    }

    let state = State { tables };
    log::info!("Computed current state from {} tables", state.tables.len());
    log::debug!("{:#?}", state);
    Ok(state)
}

pub fn save_state(state: &State) -> Result<(), Box<dyn std::error::Error>> {
    let config = config::get_config()?;
    let path = config.work_dir.join("previous_state");
    log::debug!("Storing current state in file '{}'...", path.display());

    let mut buf = Vec::new();
    state.encode(&mut buf)?;
    std::fs::write(&path, &buf)?;
    log::info!(
        "Updated previous state to current state with {} tables",
        state.tables.len()
    );
    Ok(())
}
