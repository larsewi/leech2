use std::collections::HashMap;
use std::fmt;
use std::path::Path;

use anyhow::Result;
use prost::Message;

use crate::callbacks::Callbacks;
use crate::config::Config;
use crate::storage;
use crate::table::Table;
use crate::utils::indent;

pub use crate::proto::state::State as ProtoState;

type ProtoTable = crate::proto::table::Table;

const STATE_FILE: &str = "STATE";

/// State represents a snapshot of all tables at a point in time.
#[derive(Debug, Clone, PartialEq)]
pub struct State {
    /// Map from table name to table contents.
    pub tables: HashMap<String, Table>,
}

impl TryFrom<ProtoState> for State {
    type Error = anyhow::Error;

    fn try_from(proto: ProtoState) -> Result<Self> {
        let mut tables = HashMap::with_capacity(proto.tables.len());
        for (name, proto_table) in proto.tables {
            tables.insert(name, Table::try_from(proto_table)?);
        }
        Ok(State { tables })
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
        write!(f, "State ({} tables)", self.tables.len())?;
        if !self.tables.is_empty() {
            write!(f, ":")?;
        }
        for (name, table) in &self.tables {
            write!(f, "\n  '{}' {}", name, indent(&table.to_string(), "  "))?;
        }
        Ok(())
    }
}

impl ProtoState {
    pub fn load(work_dir: &Path, mode: u32) -> Result<Option<Self>> {
        let Some(data) = storage::load(work_dir, STATE_FILE, mode)? else {
            log::info!("No previous state found");
            return Ok(None);
        };

        let proto_state = ProtoState::decode(data.as_slice())?;
        log::debug!(
            "Loaded previous state with {} tables",
            proto_state.tables.len()
        );
        log::trace!("{}", proto_state);
        Ok(Some(proto_state))
    }
}

impl State {
    pub fn load(work_dir: &Path, mode: u32) -> Result<Option<Self>> {
        let Some(proto) = ProtoState::load(work_dir, mode)? else {
            return Ok(None);
        };
        Ok(Some(State::try_from(proto)?))
    }

    /// Build a fresh snapshot of every table declared in `config`.
    ///
    /// Tables with a `[csv]` block are loaded from CSV exactly as before.
    /// Tables without a `[csv]` block are pulled through `callbacks`;
    /// reaching such a table with `callbacks == None` is an error.
    pub fn compute(config: &Config, callbacks: Option<&Callbacks>) -> Result<Self> {
        let mut tables: HashMap<String, Table> = HashMap::new();

        for (name, table_config) in &config.tables {
            let table = if table_config.csv.is_some() {
                Table::load_from_csv(&config.work_dir, name, table_config)?
            } else {
                let Some(cbs) = callbacks else {
                    anyhow::bail!(
                        "table '{}' is callback-backed but no callbacks were provided",
                        name
                    );
                };
                let field_names: Vec<&str> = table_config
                    .fields
                    .iter()
                    .map(|f| f.name.as_str())
                    .collect();
                cbs.with_table(name, &field_names, |bound| {
                    Table::load_from_callbacks(name, table_config, bound)
                })?
            };
            tables.insert(name.clone(), table);
        }

        let state = State { tables };
        log::debug!("Computed current state from {} tables", state.tables.len());
        log::trace!("{}", ProtoState::from(state.clone()));
        Ok(state)
    }

    pub fn store(&self, work_dir: &Path, mode: u32, dry_run: bool) -> Result<()> {
        let proto_state = ProtoState::from(self.clone());
        let mut buf = Vec::new();
        proto_state.encode(&mut buf)?;
        storage::store(work_dir, STATE_FILE, &buf, mode, dry_run)?;
        log::debug!(
            "Updated previous state to current state with {} tables",
            self.tables.len()
        );
        Ok(())
    }
}
