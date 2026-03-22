use std::collections::HashMap;
use std::fmt;
use std::fs::File;
use std::path::Path;

use anyhow::{Context, Result};

use crate::config::TableConfig;

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
        let records = proto.entries.into_iter().map(Into::into).collect();
        Table {
            fields: proto.fields,
            records,
        }
    }
}

impl From<Table> for crate::proto::table::Table {
    fn from(table: Table) -> Self {
        let entries = table.records.into_iter().map(Into::into).collect();
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
    pub fn load(work_dir: &Path, name: &str, config: &TableConfig) -> Result<Self> {
        let path = work_dir.join(&config.source);
        let file =
            File::open(&path).with_context(|| format!("failed to open '{}'", path.display()))?;
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

    fn parse_csv(config: &TableConfig, mut reader: csv::Reader<File>) -> Result<Self> {
        let field_names = config.field_names();
        let primary_key = config.primary_key();

        // Map each config field to its CSV column index.
        // When header=true, match by name; otherwise, use positional order.
        let mut field_indices = Vec::with_capacity(field_names.len());
        if config.header {
            let headers = reader.headers().context("failed to read CSV header")?;
            for name in &field_names {
                let index = headers
                    .iter()
                    .position(|h| h == name)
                    .ok_or_else(|| anyhow::anyhow!("field '{}' not found in CSV header", name))?;
                field_indices.push(index);
            }
        } else {
            for i in 0..field_names.len() {
                field_indices.push(i);
            }
        }

        let primary_indices: Vec<usize> = primary_key
            .iter()
            .filter_map(|primary_key_column| {
                field_names
                    .iter()
                    .position(|name| name == primary_key_column)
            })
            .map(|config_index| field_indices[config_index])
            .collect();

        let subsidiary_indices: Vec<usize> = field_names
            .iter()
            .enumerate()
            .filter(|(_, column)| !primary_key.contains(column))
            .map(|(config_index, _)| field_indices[config_index])
            .collect();

        // Order fields with primary key columns first, then subsidiary columns.
        let fields: Vec<String> = primary_key
            .iter()
            .chain(
                field_names
                    .iter()
                    .filter(|name| !primary_key.contains(name)),
            )
            .cloned()
            .collect();

        let mut records: HashMap<Vec<String>, Vec<String>> = HashMap::new();

        for (row_num, record) in reader.into_records().enumerate() {
            let record = record?;

            if !config.header && record.len() != field_names.len() {
                anyhow::bail!(
                    "row {}: expected {} fields but got {}",
                    row_num + 1,
                    field_names.len(),
                    record.len()
                );
            }

            let primary_key: Vec<String> = primary_indices
                .iter()
                .map(|&i| record[i].to_string())
                .collect();

            let subsidiary: Vec<String> = subsidiary_indices
                .iter()
                .map(|&i| record[i].to_string())
                .collect();

            if records.insert(primary_key.clone(), subsidiary).is_some() {
                anyhow::bail!("duplicate primary key {:?}", primary_key);
            }
        }

        Ok(Table { fields, records })
    }
}
