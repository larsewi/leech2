use std::collections::HashMap;
use std::fmt;
use std::fs::File;
use std::path::Path;

use crate::config::TableConfig;
use crate::entry::Entry;

/// A table with records stored in a hash map for efficient lookup.
/// Fields are ordered with primary key columns first, followed by subsidiary columns.
#[derive(Debug, Clone, PartialEq)]
pub struct Table {
    /// The names of all columns in the table, primary key columns first.
    pub fields: Vec<String>,
    /// Map from primary key values to subsidiary values.
    pub records: HashMap<Vec<String>, Vec<String>>,
}

impl From<crate::proto::table::Table> for Table {
    fn from(proto: crate::proto::table::Table) -> Self {
        let records = proto
            .entries
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
        let entries = table
            .records
            .into_iter()
            .map(|(key, value)| Entry { key, value })
            .collect();
        crate::proto::table::Table {
            fields: table.fields,
            entries,
        }
    }
}

impl fmt::Display for crate::proto::table::Table {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "[{}]", self.fields.join(", "))?;
        for entry in &self.entries {
            write!(
                f,
                "\n  ({}) {}",
                entry.key.join(", "),
                entry.value.join(", ")
            )?;
        }
        Ok(())
    }
}

impl Table {
    /// Loads a table from a CSV file.
    pub fn load(
        work_dir: &Path,
        name: &str,
        config: &TableConfig,
    ) -> Result<Self, Box<dyn std::error::Error>> {
        let path = work_dir.join(&config.source);
        let file =
            File::open(&path).map_err(|e| format!("failed to open '{}': {}", path.display(), e))?;
        let reader = csv::ReaderBuilder::new()
            .has_headers(config.header)
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
        let field_names = config.field_names();
        let primary_key = config.primary_key();

        let primary_indices: Vec<usize> = primary_key
            .iter()
            .filter_map(|pk_col| field_names.iter().position(|c| c == pk_col))
            .collect();

        let subsidiary_indices: Vec<usize> = field_names
            .iter()
            .enumerate()
            .filter(|(_, col)| !primary_key.contains(col))
            .map(|(i, _)| i)
            .collect();

        // Order fields with primary key columns first, then subsidiary columns.
        let fields: Vec<String> = primary_indices
            .iter()
            .chain(subsidiary_indices.iter())
            .map(|&i| field_names[i].clone())
            .collect();

        let expected_len = field_names.len();
        let mut records: HashMap<Vec<String>, Vec<String>> = HashMap::new();

        for (row_num, record) in reader.into_records().enumerate() {
            let record = record?;

            if record.len() != expected_len {
                return Err(format!(
                    "row {}: expected {} fields but got {}",
                    row_num + 1,
                    expected_len,
                    record.len()
                )
                .into());
            }

            let primary_key: Vec<String> = primary_indices
                .iter()
                .map(|&i| record[i].to_string())
                .collect();

            let subsidiary: Vec<String> = subsidiary_indices
                .iter()
                .map(|&i| record[i].to_string())
                .collect();

            records.insert(primary_key, subsidiary);
        }

        Ok(Table { fields, records })
    }
}
