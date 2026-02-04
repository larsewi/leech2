use std::collections::HashMap;
use std::fs::File;

use crate::config::{self, TableConfig};
use crate::entry::Entry;

pub use crate::proto::table::Table;

/// Builds a map from primary key to subsidiary value for all rows in a table.
pub fn table_to_map(table: &Table) -> HashMap<&Vec<String>, &Vec<String>> {
    table
        .rows
        .iter()
        .map(|row| (&row.key, &row.value))
        .collect()
}

fn parse_csv(
    config: &TableConfig,
    reader: csv::Reader<File>,
) -> Result<HashMap<Vec<String>, Vec<String>>, Box<dyn std::error::Error>> {
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

    let mut result: HashMap<Vec<String>, Vec<String>> = HashMap::new();

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

        result.insert(primary_key, subsidiary);
    }

    Ok(result)
}

/// Loads a table from a CSV file.
pub fn load_table(
    name: &str,
    config: &TableConfig,
) -> Result<Table, Box<dyn std::error::Error>> {
    let source_path = config::get_work_dir()?.join(&config.source);
    let file = File::open(&source_path)
        .map_err(|e| format!("failed to open '{}': {}", source_path.display(), e))?;
    let reader = csv::ReaderBuilder::new()
        .has_headers(false)
        .from_reader(file);

    let table_data = parse_csv(config, reader)?;

    let rows: Vec<Entry> = table_data
        .into_iter()
        .map(|(pk, sub)| Entry { key: pk, value: sub })
        .collect();

    log::info!("Loaded table '{}' ({} records)", name, rows.len());

    Ok(Table {
        fields: config.field_names.clone(),
        primary_key: config.primary_key.clone(),
        rows,
    })
}
