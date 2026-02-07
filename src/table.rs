use std::collections::HashMap;
use std::fs::File;

use crate::config::{self, TableConfig};
use crate::entry::Entry;

/// A table with records stored in a hash map for efficient lookup.
#[derive(Debug, Clone, PartialEq)]
pub struct Table {
    /// The names of all columns in the table.
    pub fields: Vec<String>,
    /// The names of columns that form the primary key.
    pub primary_key: Vec<String>,
    /// Map from primary key values to subsidiary values.
    pub records: HashMap<Vec<String>, Vec<String>>,
}

impl From<crate::proto::table::Table> for Table {
    fn from(proto: crate::proto::table::Table) -> Self {
        let records = proto
            .rows
            .into_iter()
            .map(|entry| (entry.key, entry.value))
            .collect();
        Table {
            fields: proto.fields,
            primary_key: proto.primary_key,
            records,
        }
    }
}

impl From<Table> for crate::proto::table::Table {
    fn from(table: Table) -> Self {
        let rows = table
            .records
            .into_iter()
            .map(|(key, value)| Entry { key, value })
            .collect();
        crate::proto::table::Table {
            fields: table.fields,
            primary_key: table.primary_key,
            rows,
        }
    }
}

impl Table {
    /// Loads a table from a CSV file.
    pub fn load(name: &str, config: &TableConfig) -> Result<Self, Box<dyn std::error::Error>> {
        let path = config::get_work_dir()?.join(&config.source);
        let file =
            File::open(&path).map_err(|e| format!("failed to open '{}': {}", path.display(), e))?;
        let reader = csv::ReaderBuilder::new()
            .has_headers(false)
            .from_reader(file);

        log::debug!("Parsing csv file '{}'...", path.display());
        let table = Self::parse_csv(config, reader)?;

        log::info!("Loaded table '{}' with {} records", name, table.records.len());

        Ok(table)
    }

    fn parse_csv(
        config: &TableConfig,
        reader: csv::Reader<File>,
    ) -> Result<Self, Box<dyn std::error::Error>> {
        let primary_indices: Vec<usize> = config
            .primary_key
            .iter()
            .filter_map(|pk_col| config.field_names.iter().position(|c| c == pk_col))
            .collect();

        let subsidiary_indices: Vec<usize> = config
            .field_names
            .iter()
            .enumerate()
            .filter(|(_, col)| !config.primary_key.contains(col))
            .map(|(i, _)| i)
            .collect();

        let mut records: HashMap<Vec<String>, Vec<String>> = HashMap::new();

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

            records.insert(primary_key, subsidiary);
        }

        Ok(Table {
            fields: config.field_names.clone(),
            primary_key: config.primary_key.clone(),
            records,
        })
    }
}
