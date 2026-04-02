use std::collections::HashMap;
use std::fmt;
use std::fs::File;
use std::path::Path;

use anyhow::{Context, Result};

use crate::config::{FilterConfig, TableConfig};

type ProtoTable = crate::proto::table::Table;

/// A table with records stored in a hash map for efficient lookup.
/// Fields are ordered with primary key columns first, followed by subsidiary columns.
#[derive(Debug, Clone, PartialEq)]
pub struct Table {
    /// The names of all columns in the table, primary key columns first.
    pub fields: Vec<String>,
    /// Map from primary key values to subsidiary values.
    pub records: HashMap<Vec<String>, Vec<String>>,
}

impl From<ProtoTable> for Table {
    fn from(proto: ProtoTable) -> Self {
        let records = proto.entries.into_iter().map(Into::into).collect();
        Table {
            fields: proto.fields,
            records,
        }
    }
}

impl From<Table> for ProtoTable {
    fn from(table: Table) -> Self {
        let entries = table.records.into_iter().map(Into::into).collect();
        ProtoTable {
            fields: table.fields,
            entries,
        }
    }
}

impl fmt::Display for ProtoTable {
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
        filters: &FilterConfig,
    ) -> Result<Self> {
        let path = work_dir.join(&config.source);
        let file =
            File::open(&path).with_context(|| format!("failed to open '{}'", path.display()))?;
        let reader = csv::ReaderBuilder::new()
            .has_headers(config.header)
            .from_reader(file);

        log::debug!("Parsing csv file '{}'...", path.display());
        let table = Self::parse_csv(name, config, filters, reader)?;

        log::info!(
            "Loaded table '{}' with {} records",
            name,
            table.records.len()
        );

        Ok(table)
    }

    fn parse_csv(
        table_name: &str,
        config: &TableConfig,
        filters: &FilterConfig,
        mut reader: csv::Reader<File>,
    ) -> Result<Self> {
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

        let fields = config.ordered_field_names();

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

            let values: Vec<&str> = field_indices.iter().map(|&i| &record[i]).collect();
            let reason = filters.should_filter(table_name, &field_names, &values);
            if let Some(reason) = reason {
                log::debug!("Filtered record at row {}: {}", row_num + 1, reason);
                continue;
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
