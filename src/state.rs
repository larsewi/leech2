use std::collections::HashMap;
use std::fmt;
use std::path::Path;

use anyhow::Result;
use prost::Message;

use crate::config::Config;
use crate::storage;
use crate::table::Table;
use crate::utils::indent;

const STATE_FILE: &str = "STATE";

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
            .map(|proto_table| (proto_table.table_name.clone(), Table::from(proto_table)))
            .collect();
        State { tables }
    }
}

impl From<State> for crate::proto::state::State {
    fn from(state: State) -> Self {
        let tables = state
            .tables
            .into_iter()
            .map(|(name, table)| {
                let mut proto_table = crate::proto::table::Table::from(table);
                proto_table.table_name = name;
                proto_table
            })
            .collect();
        crate::proto::state::State { tables }
    }
}

impl fmt::Display for crate::proto::state::State {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "State ({} tables):", self.tables.len())?;
        let mut tables: Vec<_> = self.tables.iter().collect();
        tables.sort_by_key(|t| &t.table_name);
        for table in tables {
            write!(f, "\n  {}", indent(&table.to_string(), "  "))?;
        }
        Ok(())
    }
}

impl State {
    pub fn load(work_dir: &Path) -> Result<Option<Self>> {
        let Some(data) = storage::load(work_dir, STATE_FILE)? else {
            log::info!("No previous state found");
            return Ok(None);
        };

        let proto_state = crate::proto::state::State::decode(data.as_slice())?;
        log::info!(
            "Loaded previous state with {} tables",
            proto_state.tables.len()
        );
        log::debug!("{}", proto_state);
        let state = State::from(proto_state);
        Ok(Some(state))
    }

    pub fn compute(config: &Config) -> Result<Self> {
        let mut tables: HashMap<String, Table> = HashMap::new();

        for (name, table_config) in &config.tables {
            let table = Table::load(&config.work_dir, name, table_config)?;
            tables.insert(name.clone(), table);
        }

        let state = State { tables };
        log::info!("Computed current state from {} tables", state.tables.len());
        if log::log_enabled!(log::Level::Debug) {
            log::debug!("{}", crate::proto::state::State::from(state.clone()));
        }
        Ok(state)
    }

    pub fn store(&self, work_dir: &Path) -> Result<()> {
        let proto_state = crate::proto::state::State::from(self.clone());
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
