use std::collections::HashMap;

use prost::Message;

use crate::config;
use crate::table::Table;

/// State represents a snapshot of all tables at a point in time.
#[derive(Debug, Clone, PartialEq)]
pub struct State {
    /// Map from table name to table contents.
    pub tables: HashMap<String, Table>,
}

impl From<crate::proto::state::State> for State {
    fn from(proto: crate::proto::state::State) -> Self {
        let tables = proto
            .tables
            .into_iter()
            .map(|(name, table)| (name, Table::from(table)))
            .collect();
        State { tables }
    }
}

impl From<State> for crate::proto::state::State {
    fn from(state: State) -> Self {
        let tables = state
            .tables
            .into_iter()
            .map(|(name, table)| (name, crate::proto::table::Table::from(table)))
            .collect();
        crate::proto::state::State { tables }
    }
}

impl State {
    pub fn load_previous() -> Result<Option<Self>, Box<dyn std::error::Error>> {
        let config = config::get_config()?;
        let path = config.work_dir.join("previous_state");
        if !path.exists() {
            log::info!("No previous state found");
            return Ok(None);
        }

        log::debug!("Parsing previous state from file '{}'...", path.display());

        let data = std::fs::read(&path)?;
        let proto_state = crate::proto::state::State::decode(data.as_slice())?;
        let state = State::from(proto_state);
        log::debug!("{:#?}", state);
        log::info!("Loaded previous state with {} tables", state.tables.len());
        Ok(Some(state))
    }

    pub fn load_current() -> Result<Self, Box<dyn std::error::Error>> {
        let config = config::get_config()?;
        let mut tables: HashMap<String, Table> = HashMap::new();

        for (name, config) in &config.tables {
            let table = Table::load(name, config)?;
            tables.insert(name.clone(), table);
        }

        let state = State { tables };
        log::info!("Computed current state from {} tables", state.tables.len());
        log::debug!("{:#?}", state);
        Ok(state)
    }
}

pub fn save_state(state: &State) -> Result<(), Box<dyn std::error::Error>> {
    let config = config::get_config()?;
    let path = config.work_dir.join("previous_state");
    log::debug!("Storing current state in file '{}'...", path.display());

    let proto_state = crate::proto::state::State::from(state.clone());
    let mut buf = Vec::new();
    proto_state.encode(&mut buf)?;
    std::fs::write(&path, &buf)?;
    log::info!(
        "Updated previous state to current state with {} tables",
        state.tables.len()
    );
    Ok(())
}
