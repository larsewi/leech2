use std::collections::HashMap;
use std::fmt;
use std::fs::File;

use crate::config::{self, TableConfig};
use crate::entry::Entry;


/// A table with records stored in a hash map for efficient lookup.
/// Fields are ordered with primary key columns first, followed by subsidiary columns.
#[derive(Debug, Clone, PartialEq)]
pub struct Table {
    /// The names of all columns in the table, primary key columns first.
    pub fields: Vec<String>,
    /// Map from primary key values to subsidiary values.
    pub records: HashMap<Vec<Vec<u8>>, Vec<Vec<u8>>>,
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
            rows,
        }
    }
}

impl fmt::Display for crate::proto::table::Table {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "[{}]", self.fields.join(", "))?;
        for row in &self.rows {
            write!(f, "\n  {}", row)?;
        }
        Ok(())
    }
}

impl Table {
    /// Loads a table from a CSV file.
    pub fn load(name: &str, config: &TableConfig) -> Result<Self, Box<dyn std::error::Error>> {
        let path = config::Config::get()?.work_dir.join(&config.source);
        let file =
            File::open(&path).map_err(|e| format!("failed to open '{}': {}", path.display(), e))?;
        let reader = csv::ReaderBuilder::new()
            .has_headers(false)
            .from_reader(file);

        log::debug!("Parsing csv file '{}'...", path.display());
        let table = Self::parse_csv(config, reader)?;

        log::info!(
            "Loaded table '{}' with {} records",
            name,
            table.records.len()
        );

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

        // Order fields with primary key columns first, then subsidiary columns.
        let fields: Vec<String> = primary_indices
            .iter()
            .chain(subsidiary_indices.iter())
            .map(|&i| config.field_names[i].clone())
            .collect();

        let mut records: HashMap<Vec<Vec<u8>>, Vec<Vec<u8>>> = HashMap::new();

        for record in reader.into_byte_records() {
            let record = record?;

            let primary_key: Vec<Vec<u8>> = primary_indices
                .iter()
                .filter_map(|&i| record.get(i).map(|b| b.to_vec()))
                .collect();

            let subsidiary: Vec<Vec<u8>> = subsidiary_indices
                .iter()
                .filter_map(|&i| record.get(i).map(|b| b.to_vec()))
                .collect();

            records.insert(primary_key, subsidiary);
        }

        Ok(Table { fields, records })
    }
}
