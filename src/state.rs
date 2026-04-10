use std::collections::HashMap;
use std::fmt;
use std::path::Path;

use anyhow::Result;
use prost::Message;

use crate::config::Config;
use crate::storage;
use crate::table::Table;
use crate::utils::indent;

type ProtoState = crate::proto::state::State;
type ProtoTable = crate::proto::table::Table;

const STATE_FILE: &str = "STATE";

/// State represents a snapshot of all tables at a point in time.
#[derive(Debug, Clone, PartialEq)]
pub struct State {
    /// Map from table name to table contents.
    pub tables: HashMap<String, Table>,
}

impl From<ProtoState> for State {
    fn from(proto: ProtoState) -> Self {
        let tables = proto
            .tables
            .into_iter()
            .map(|(name, proto_table)| (name, Table::from(proto_table)))
            .collect();
        State { tables }
    }
}

impl From<State> for ProtoState {
    fn from(state: State) -> Self {
        let tables = state
            .tables
            .into_iter()
            .map(|(name, table)| (name, ProtoTable::from(table)))
            .collect();
        ProtoState { tables }
    }
}

impl fmt::Display for ProtoState {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "State ({} tables):", self.tables.len())?;
        for (name, table) in &self.tables {
            write!(f, "\n  '{}' {}", name, indent(&table.to_string(), "  "))?;
        }
        Ok(())
    }
}

impl ProtoState {
    pub fn load(work_dir: &Path) -> Result<Option<Self>> {
        let Some(data) = storage::load(work_dir, STATE_FILE)? else {
            log::info!("No previous state found");
            return Ok(None);
        };

        let proto_state = ProtoState::decode(data.as_slice())?;
        log::info!(
            "Loaded previous state with {} tables",
            proto_state.tables.len()
        );
        log::trace!("{}", proto_state);
        Ok(Some(proto_state))
    }
}

impl State {
    pub fn load(work_dir: &Path) -> Result<Option<Self>> {
        let Some(proto) = ProtoState::load(work_dir)? else {
            return Ok(None);
        };
        Ok(Some(State::from(proto)))
    }

    pub fn compute(config: &Config) -> Result<Self> {
        let mut tables: HashMap<String, Table> = HashMap::new();

        for (name, table_config) in &config.tables {
            let table = Table::load(&config.work_dir, name, table_config, &config.filters)?;
            tables.insert(name.clone(), table);
        }

        let state = State { tables };
        log::info!("Computed current state from {} tables", state.tables.len());
        log::trace!("{}", ProtoState::from(state.clone()));
        Ok(state)
    }

    pub fn store(&self, work_dir: &Path) -> Result<()> {
        let proto_state = ProtoState::from(self.clone());
        let mut buf = Vec::new();
        proto_state.encode(&mut buf)?;
        storage::store(work_dir, STATE_FILE, &buf)?;
        log::info!(
            "Updated previous state to current state with {} tables",
            self.tables.len()
        );
        Ok(())
    }
}
