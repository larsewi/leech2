use std::collections::HashMap;
use std::fmt;
use std::fs::File;
use std::path::Path;

use anyhow::{Context, Result};

use crate::config::{FieldConfig, FilterConfig, TableConfig};
use crate::entry::Entry;
use crate::sql::{SqlType, parse_typed_value};
use crate::value::Value;
use crate::value::display_proto_values;

type ProtoTable = crate::proto::table::Table;

/// A table with records stored in a hash map for efficient lookup.
/// Fields are ordered with primary key columns first, followed by subsidiary columns.
#[derive(Debug, Clone, PartialEq)]
pub struct Table {
    /// The names of all columns in the table, primary key columns first.
    pub fields: Vec<String>,
    /// Map from primary key values to subsidiary values.
    pub records: HashMap<Vec<Value>, Vec<Value>>,
}

impl TryFrom<ProtoTable> for Table {
    type Error = anyhow::Error;

    fn try_from(proto: ProtoTable) -> Result<Self> {
        let mut records = HashMap::with_capacity(proto.entries.len());
        for proto_entry in proto.entries {
            let entry = Entry::try_from(proto_entry)?;
            records.insert(entry.key, entry.value);
        }
        Ok(Table {
            fields: proto.fields,
            records,
        })
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
                display_proto_values(&entry.key),
                display_proto_values(&entry.value)
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
        // Shared advisory lock: defence-in-depth against a cooperating producer
        // that takes an exclusive lock while rewriting the CSV in place. The
        // lock is released when `file` (moved into the reader) is dropped.
        file.lock_shared()
            .with_context(|| format!("failed to acquire shared lock on '{}'", path.display()))?;
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

    /// Map each config field to its CSV column index.
    /// When header=true, match by name; otherwise, use positional order.
    fn resolve_field_indices(
        config: &TableConfig,
        reader: &mut csv::Reader<File>,
    ) -> Result<Vec<usize>> {
        let field_names = config.field_names();
        let mut indices = Vec::with_capacity(field_names.len());
        if config.header {
            let headers = reader.headers().context("failed to read CSV header")?;
            for name in &field_names {
                let index = headers
                    .iter()
                    .position(|h| h == name)
                    .ok_or_else(|| anyhow::anyhow!("field '{}' not found in CSV header", name))?;
                indices.push(index);
            }
        } else {
            indices = Vec::from_iter(0..field_names.len());
        }
        Ok(indices)
    }

    /// Split field indices into primary-key and subsidiary groups based on
    /// which fields are marked as primary keys in the config.
    fn compute_key_indices(
        field_names: &[String],
        primary_key: &[String],
        field_indices: &[usize],
    ) -> (Vec<usize>, Vec<usize>) {
        let mut primary_indices = Vec::new();
        let mut subsidiary_indices = Vec::new();
        for (config_index, name) in field_names.iter().enumerate() {
            if primary_key.contains(name) {
                primary_indices.push(field_indices[config_index]);
            } else {
                subsidiary_indices.push(field_indices[config_index]);
            }
        }
        (primary_indices, subsidiary_indices)
    }

    #[cfg(test)]
    fn test_reader(csv_content: &str, has_headers: bool) -> csv::Reader<File> {
        use std::io::Write;
        let mut tmp = tempfile::NamedTempFile::new().unwrap();
        tmp.write_all(csv_content.as_bytes()).unwrap();
        tmp.flush().unwrap();
        csv::ReaderBuilder::new()
            .has_headers(has_headers)
            .from_reader(File::open(tmp.path()).unwrap())
    }

    fn parse_csv(
        table_name: &str,
        config: &TableConfig,
        filters: &FilterConfig,
        mut reader: csv::Reader<File>,
    ) -> Result<Self> {
        let field_names = config.field_names();
        let primary_key = config.primary_key();
        let field_indices = Self::resolve_field_indices(config, &mut reader)?;
        let (primary_indices, subsidiary_indices) =
            Self::compute_key_indices(&field_names, &primary_key, &field_indices);

        // Field configs split by primary-key flag in declaration order. This
        // matches the ordering of `primary_indices` / `subsidiary_indices` —
        // both are derived from `config.fields` in declaration order — so
        // zipping the two pairs lines up the right config with each value.
        let primary_field_configs: Vec<&FieldConfig> =
            config.fields.iter().filter(|f| f.primary_key).collect();
        let subsidiary_field_configs: Vec<&FieldConfig> =
            config.fields.iter().filter(|f| !f.primary_key).collect();

        let fields = config.ordered_field_names();

        let mut records: HashMap<Vec<Value>, Vec<Value>> = HashMap::new();

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

            let primary_key = parse_columns(&record, &primary_indices, &primary_field_configs)
                .with_context(|| format!("row {}", row_num + 1))?;
            let subsidiary = parse_columns(&record, &subsidiary_indices, &subsidiary_field_configs)
                .with_context(|| format!("row {}", row_num + 1))?;

            if records.insert(primary_key.clone(), subsidiary).is_some() {
                anyhow::bail!("duplicate primary key {:?}", primary_key);
            }
        }

        Ok(Table { fields, records })
    }
}

/// Pull values at `csv_indices` out of `record` and parse each one into a
/// typed `Value` according to its corresponding `FieldConfig`. The two
/// slices must be the same length and aligned 1-to-1.
fn parse_columns(
    record: &csv::StringRecord,
    csv_indices: &[usize],
    field_configs: &[&FieldConfig],
) -> Result<Vec<Value>> {
    let mut out = Vec::with_capacity(csv_indices.len());
    for (&csv_index, &field) in csv_indices.iter().zip(field_configs.iter()) {
        out.push(parse_field_value(&record[csv_index], field)?);
    }
    Ok(out)
}

/// Parse a single CSV value into a `Value` based on its field config.
/// Values matching the `null` sentinel become `Value::Null`; otherwise the
/// value is parsed by `SqlType` (`TEXT`/`NUMBER`/`BOOLEAN`).
fn parse_field_value(value: &str, field: &FieldConfig) -> Result<Value> {
    if let Some(sentinel) = &field.null
        && value == sentinel
    {
        return Ok(Value::Null);
    }
    let sql_type =
        SqlType::from_config(&field.sql_type).with_context(|| format!("field '{}'", field.name))?;
    parse_typed_value(value, &sql_type).with_context(|| format!("field '{}'", field.name))
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::config::FieldConfig;

    fn make_field(name: &str, primary_key: bool) -> FieldConfig {
        FieldConfig {
            name: name.to_string(),
            sql_type: "TEXT".to_string(),
            primary_key,
            null: None,
        }
    }

    fn make_config(fields: Vec<FieldConfig>, header: bool) -> TableConfig {
        TableConfig {
            source: "test.csv".to_string(),
            header,
            fields,
        }
    }

    // -- compute_key_indices tests --

    #[test]
    fn test_compute_key_indices_single_primary_key() {
        let field_names = ["id", "name", "email"].map(String::from).to_vec();
        let primary_key = ["id"].map(String::from).to_vec();
        let field_indices = vec![0, 1, 2];

        let (primary_indices, subsidiary_indices) =
            Table::compute_key_indices(&field_names, &primary_key, &field_indices);

        assert_eq!(primary_indices, vec![0]);
        assert_eq!(subsidiary_indices, vec![1, 2]);
    }

    #[test]
    fn test_compute_key_indices_composite_primary_key() {
        let field_names = ["region", "id", "name"].map(String::from).to_vec();
        let primary_key = ["region", "id"].map(String::from).to_vec();
        let field_indices = vec![0, 1, 2];

        let (primary_indices, subsidiary_indices) =
            Table::compute_key_indices(&field_names, &primary_key, &field_indices);

        assert_eq!(primary_indices, vec![0, 1]);
        assert_eq!(subsidiary_indices, vec![2]);
    }

    #[test]
    fn test_compute_key_indices_reordered_columns() {
        // CSV columns are in a different order than the config fields.
        let field_names = ["id", "name", "email"].map(String::from).to_vec();
        let primary_key = ["id"].map(String::from).to_vec();
        let field_indices = vec![2, 0, 1]; // id->col2, name->col0, email->col1

        let (primary_indices, subsidiary_indices) =
            Table::compute_key_indices(&field_names, &primary_key, &field_indices);

        assert_eq!(primary_indices, vec![2]);
        assert_eq!(subsidiary_indices, vec![0, 1]);
    }

    #[test]
    fn test_compute_key_indices_all_primary_keys() {
        let field_names = ["a", "b"].map(String::from).to_vec();
        let primary_key = ["a", "b"].map(String::from).to_vec();
        let field_indices = vec![0, 1];

        let (primary_indices, subsidiary_indices) =
            Table::compute_key_indices(&field_names, &primary_key, &field_indices);

        assert_eq!(primary_indices, vec![0, 1]);
        assert!(subsidiary_indices.is_empty());
    }

    // -- resolve_field_indices tests --

    #[test]
    fn test_resolve_field_indices_no_header() {
        let config = make_config(
            vec![
                make_field("id", true),
                make_field("name", false),
                make_field("email", false),
            ],
            false,
        );
        let mut reader = Table::test_reader("1,Alice,alice@example.com\n", false);

        let indices = Table::resolve_field_indices(&config, &mut reader).unwrap();
        assert_eq!(indices, vec![0, 1, 2]);
    }

    #[test]
    fn test_resolve_field_indices_with_header() {
        let config = make_config(
            vec![
                make_field("id", true),
                make_field("name", false),
                make_field("email", false),
            ],
            true,
        );
        let mut reader = Table::test_reader("id,name,email\n1,Alice,alice@example.com\n", true);

        let indices = Table::resolve_field_indices(&config, &mut reader).unwrap();
        assert_eq!(indices, vec![0, 1, 2]);
    }

    #[test]
    fn test_resolve_field_indices_reordered_header() {
        let config = make_config(
            vec![
                make_field("id", true),
                make_field("name", false),
                make_field("email", false),
            ],
            true,
        );
        // CSV columns in different order than config
        let mut reader = Table::test_reader("email,name,id\na@b.com,Alice,1\n", true);

        let indices = Table::resolve_field_indices(&config, &mut reader).unwrap();
        assert_eq!(indices, vec![2, 1, 0]);
    }

    #[test]
    fn test_resolve_field_indices_missing_field() {
        let config = make_config(
            vec![make_field("id", true), make_field("missing", false)],
            true,
        );
        let mut reader = Table::test_reader("id,name\n1,Alice\n", true);

        let err = Table::resolve_field_indices(&config, &mut reader).unwrap_err();
        assert!(
            err.to_string().contains("field 'missing' not found"),
            "unexpected error: {err}"
        );
    }

    // -- numeric normalization on load --

    fn make_typed_field(
        name: &str,
        sql_type: &str,
        primary_key: bool,
        null: Option<&str>,
    ) -> FieldConfig {
        FieldConfig {
            name: name.to_string(),
            sql_type: sql_type.to_string(),
            primary_key,
            null: null.map(str::to_string),
        }
    }

    #[test]
    fn test_parse_csv_parses_numbers() {
        let config = make_config(
            vec![
                make_typed_field("id", "NUMBER", true, None),
                make_typed_field("count", "NUMBER", false, None),
                make_typed_field("name", "TEXT", false, None),
            ],
            true,
        );
        let reader = Table::test_reader("id,count,name\n0.0,1e2,Alice\n+5,1.10,Bob\n", true);
        let table = Table::parse_csv("t", &config, &FilterConfig::default(), reader).unwrap();

        // "0.0" parses to 0.0; "1e2" parses to 100.0
        assert_eq!(
            table.records.get(&vec![Value::Number(0.0)]),
            Some(&vec![Value::Number(100.0), "Alice".into()])
        );
        // "+5" parses to 5.0; "1.10" parses to 1.1
        assert_eq!(
            table.records.get(&vec![Value::Number(5.0)]),
            Some(&vec![Value::Number(1.1), "Bob".into()])
        );
    }

    #[test]
    fn test_parse_csv_respects_null_sentinel_on_number() {
        let config = make_config(
            vec![
                make_typed_field("id", "NUMBER", true, None),
                make_typed_field("count", "NUMBER", false, Some("N/A")),
            ],
            true,
        );
        let reader = Table::test_reader("id,count\n1,N/A\n2,3.0\n", true);
        let table = Table::parse_csv("t", &config, &FilterConfig::default(), reader).unwrap();

        // Sentinel becomes Value::Null, even though "N/A" is not a number.
        assert_eq!(
            table.records.get(&vec![Value::Number(1.0)]),
            Some(&vec![Value::Null])
        );
        // Non-sentinel parses as a number.
        assert_eq!(
            table.records.get(&vec![Value::Number(2.0)]),
            Some(&vec![Value::Number(3.0)])
        );
    }

    #[test]
    fn test_parse_csv_parses_booleans() {
        let config = make_config(
            vec![
                make_typed_field("id", "NUMBER", true, None),
                make_typed_field("active", "BOOLEAN", false, None),
            ],
            true,
        );
        let reader = Table::test_reader("id,active\n1,True\n2,1\n3,YES\n4,False\n5,no\n", true);
        let table = Table::parse_csv("t", &config, &FilterConfig::default(), reader).unwrap();

        // Truthy variants all parse to Value::Boolean(true).
        for id in [1.0, 2.0, 3.0] {
            assert_eq!(
                table.records.get(&vec![Value::Number(id)]),
                Some(&vec![Value::Boolean(true)]),
                "id={id}"
            );
        }
        for id in [4.0, 5.0] {
            assert_eq!(
                table.records.get(&vec![Value::Number(id)]),
                Some(&vec![Value::Boolean(false)]),
                "id={id}"
            );
        }
    }

    #[test]
    fn test_parse_csv_rejects_invalid_number() {
        let config = make_config(
            vec![
                make_typed_field("id", "NUMBER", true, None),
                make_typed_field("count", "NUMBER", false, None),
            ],
            true,
        );
        let reader = Table::test_reader("id,count\n1,abc\n", true);
        let err = Table::parse_csv("t", &config, &FilterConfig::default(), reader).unwrap_err();
        let msg = format!("{:#}", err);
        assert!(msg.contains("row 1"), "expected row context: {msg}");
        assert!(
            msg.contains("field 'count'"),
            "expected field context: {msg}"
        );
        assert!(
            msg.contains("invalid number"),
            "expected invalid-number cause: {msg}"
        );
    }
}
