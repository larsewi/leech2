use std::collections::HashMap;
use std::fmt;
use std::fs::File;
use std::path::Path;

use anyhow::{Context, Result};

use crate::cell::{
    Cell, DEFAULT_FALSE_SENTINEL, DEFAULT_TRUE_SENTINEL, Kind, display_proto_cells, parse_boolean,
    parse_typed_cell,
};
use crate::config::{FieldConfig, FilterConfig, TableConfig};
use crate::record::decode_proto_records;

type ProtoTable = crate::proto::table::Table;

/// Tuple positions (column index, field config) for one half of a row,
/// either the primary-key columns or the subsidiaries. Sorted
/// lexicographically by field name to keep tuple identity stable across
/// config field reorderings.
type CanonicalColumns<'a> = Vec<(usize, &'a FieldConfig)>;

/// A table with records stored in a hash map for efficient lookup.
/// Fields are ordered with primary key columns first, followed by subsidiary columns.
#[derive(Debug, Clone, PartialEq)]
pub struct Table {
    /// The names of all columns in the table, primary key columns first.
    pub fields: Vec<String>,
    /// Map from primary key values to subsidiary values.
    pub records: HashMap<Vec<Cell>, Vec<Cell>>,
}

impl TryFrom<ProtoTable> for Table {
    type Error = anyhow::Error;

    fn try_from(proto: ProtoTable) -> Result<Self> {
        let records = decode_proto_records(proto.records)?;
        Ok(Table {
            fields: proto.fields,
            records,
        })
    }
}

impl From<Table> for ProtoTable {
    fn from(table: Table) -> Self {
        let records = table.records.into_iter().map(Into::into).collect();
        ProtoTable {
            fields: table.fields,
            records,
        }
    }
}

impl fmt::Display for ProtoTable {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "[{}]", self.fields.join(", "))?;
        for record in &self.records {
            write!(
                f,
                "\n  ({}) {}",
                display_proto_cells(&record.key),
                display_proto_cells(&record.value)
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
        // Shared advisory lock: defense-in-depth against a cooperating producer
        // that takes an exclusive lock while rewriting the CSV in place. The
        // lock is released when `file` (moved into the reader) is dropped.
        file.lock_shared()
            .with_context(|| format!("failed to acquire shared lock on '{}'", path.display()))?;
        let reader = csv::ReaderBuilder::new()
            .has_headers(config.header)
            .from_reader(file);

        log::debug!("Parsing csv file '{}'...", path.display());
        let table = Self::parse_csv(name, config, filters, reader)?;

        log::debug!(
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

    /// Split columns into primary-key and subsidiary groups, each sorted
    /// lexicographically by field name. Each entry pairs a column index
    /// with the `FieldConfig` used to parse the value at that column. The
    /// canonical ordering makes tuple identity independent of the order
    /// fields are declared in the config, so reordering fields in
    /// `tables.toml` does not change the on-disk or on-the-wire
    /// representation.
    fn compute_canonical_columns<'a>(
        config: &'a TableConfig,
        field_indices: &[usize],
    ) -> (CanonicalColumns<'a>, CanonicalColumns<'a>) {
        let mut entries: Vec<(&str, usize, &FieldConfig)> = config
            .fields
            .iter()
            .zip(field_indices.iter())
            .map(|(field, &idx)| (field.name.as_str(), idx, field))
            .collect();
        entries.sort_by_key(|(name, _, _)| *name);

        let mut primary_columns: CanonicalColumns = Vec::new();
        let mut subsidiary_columns: CanonicalColumns = Vec::new();
        for (_, idx, field) in entries {
            if field.primary_key {
                primary_columns.push((idx, field));
            } else {
                subsidiary_columns.push((idx, field));
            }
        }
        (primary_columns, subsidiary_columns)
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
        let field_indices = Self::resolve_field_indices(config, &mut reader)?;
        let (primary_columns, subsidiary_columns) =
            Self::compute_canonical_columns(config, &field_indices);

        let fields: Vec<String> = primary_columns
            .iter()
            .chain(subsidiary_columns.iter())
            .map(|(_, field)| field.name.clone())
            .collect();

        let mut records: HashMap<Vec<Cell>, Vec<Cell>> = HashMap::new();

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

            let primary_key = parse_columns(&record, &primary_columns)
                .with_context(|| format!("row {}", row_num + 1))?;
            let subsidiary = parse_columns(&record, &subsidiary_columns)
                .with_context(|| format!("row {}", row_num + 1))?;

            if records.insert(primary_key.clone(), subsidiary).is_some() {
                anyhow::bail!("duplicate primary key {:?}", primary_key);
            }
        }

        Ok(Table { fields, records })
    }
}

/// For each `(column_index, field_config)` entry, pull the value at
/// `column_index` out of `record` and parse it into a typed `Cell`
/// according to `field_config`.
fn parse_columns(
    record: &csv::StringRecord,
    columns: &[(usize, &FieldConfig)],
) -> Result<Vec<Cell>> {
    let mut out = Vec::with_capacity(columns.len());
    for &(column_index, field) in columns {
        out.push(parse_field_value(&record[column_index], field)?);
    }
    Ok(out)
}

/// Parse a single CSV value into a `Cell` based on its field config.
/// Values matching the `null` sentinel become `Cell::Null`; otherwise the
/// value is parsed by its declared kind (`TEXT`/`NUMBER`/`BOOLEAN`). For
/// BOOLEAN fields the per-field `true` / `false` sentinels are honoured,
/// falling back to the strict defaults `"true"` / `"false"`.
fn parse_field_value(value: &str, field: &FieldConfig) -> Result<Cell> {
    if let Some(sentinel) = &field.null_sentinel
        && value == sentinel
    {
        return Ok(Cell::Null);
    }
    if let Kind::Boolean = field.kind {
        let true_sentinel = field
            .true_sentinel
            .as_deref()
            .unwrap_or(DEFAULT_TRUE_SENTINEL);
        let false_sentinel = field
            .false_sentinel
            .as_deref()
            .unwrap_or(DEFAULT_FALSE_SENTINEL);
        return parse_boolean(value, true_sentinel, false_sentinel)
            .map(Cell::Boolean)
            .with_context(|| format!("field '{}'", field.name));
    }
    parse_typed_cell(value, field.kind).with_context(|| format!("field '{}'", field.name))
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::config::FieldConfig;

    fn make_field(name: &str, primary_key: bool) -> FieldConfig {
        FieldConfig {
            name: name.to_string(),
            primary_key,
            ..Default::default()
        }
    }

    fn make_config(fields: Vec<FieldConfig>, header: bool) -> TableConfig {
        TableConfig {
            source: "test.csv".to_string(),
            header,
            fields,
        }
    }

    // -- compute_canonical_columns tests --

    /// Project `compute_canonical_columns` output into `(column_index, name)`
    /// pairs for easy assertion.
    fn extract<'a>(columns: &[(usize, &'a FieldConfig)]) -> Vec<(usize, &'a str)> {
        columns
            .iter()
            .map(|(idx, field)| (*idx, field.name.as_str()))
            .collect()
    }

    #[test]
    fn test_compute_canonical_columns_single_primary_key() {
        let config = make_config(
            vec![
                make_field("id", true),
                make_field("name", false),
                make_field("email", false),
            ],
            false,
        );
        let field_indices = vec![0, 1, 2];

        let (primary, subsidiary) = Table::compute_canonical_columns(&config, &field_indices);

        assert_eq!(extract(&primary), vec![(0, "id")]);
        // Subsidiaries sorted lexicographically: email before name.
        assert_eq!(extract(&subsidiary), vec![(2, "email"), (1, "name")]);
    }

    #[test]
    fn test_compute_canonical_columns_composite_primary_key() {
        let config = make_config(
            vec![
                make_field("region", true),
                make_field("id", true),
                make_field("name", false),
            ],
            false,
        );
        let field_indices = vec![0, 1, 2];

        let (primary, subsidiary) = Table::compute_canonical_columns(&config, &field_indices);

        // Primary keys sorted lexicographically: id before region.
        assert_eq!(extract(&primary), vec![(1, "id"), (0, "region")]);
        assert_eq!(extract(&subsidiary), vec![(2, "name")]);
    }

    #[test]
    fn test_compute_canonical_columns_reordered_columns() {
        // Source columns are in a different order than the declared fields.
        let config = make_config(
            vec![
                make_field("id", true),
                make_field("name", false),
                make_field("email", false),
            ],
            true,
        );
        let field_indices = vec![2, 0, 1]; // id->col2, name->col0, email->col1

        let (primary, subsidiary) = Table::compute_canonical_columns(&config, &field_indices);

        assert_eq!(extract(&primary), vec![(2, "id")]);
        assert_eq!(extract(&subsidiary), vec![(1, "email"), (0, "name")]);
    }

    #[test]
    fn test_compute_canonical_columns_all_primary_keys() {
        let config = make_config(vec![make_field("b", true), make_field("a", true)], false);
        let field_indices = vec![0, 1];

        let (primary, subsidiary) = Table::compute_canonical_columns(&config, &field_indices);

        // Both PKs, sorted lexicographically: a before b. The column index
        // at each tuple position follows the field's source position.
        assert_eq!(extract(&primary), vec![(1, "a"), (0, "b")]);
        assert!(subsidiary.is_empty());
    }

    #[test]
    fn test_parse_csv_tuple_identity_invariant_under_field_reorder() {
        // Two configs with the same fields in different declaration orders
        // produce identical tables when fed the same source data.
        let config_a = make_config(
            vec![
                make_field("id", true),
                make_field("name", false),
                make_field("email", false),
            ],
            true,
        );
        let config_b = make_config(
            vec![
                make_field("email", false),
                make_field("id", true),
                make_field("name", false),
            ],
            true,
        );
        let csv = "id,name,email\n1,Alice,alice@example.com\n";

        let reader_a = Table::test_reader(csv, true);
        let table_a = Table::parse_csv("t", &config_a, &FilterConfig::default(), reader_a).unwrap();
        let reader_b = Table::test_reader(csv, true);
        let table_b = Table::parse_csv("t", &config_b, &FilterConfig::default(), reader_b).unwrap();

        assert_eq!(table_a.fields, vec!["id", "email", "name"]);
        assert_eq!(table_a.fields, table_b.fields);
        assert_eq!(table_a.records, table_b.records);
    }

    #[test]
    fn test_parse_csv_no_header_uses_canonical_tuple_layout() {
        // header=false: source columns are matched positionally against the
        // declaration order, but the resulting tuple is still canonical.
        let config = make_config(
            vec![
                make_field("name", false),
                make_field("id", true),
                make_field("email", false),
            ],
            false,
        );
        let reader = Table::test_reader("Alice,1,a@b.com\n", false);
        let table = Table::parse_csv("t", &config, &FilterConfig::default(), reader).unwrap();

        // Canonical layout: id (PK), then subsidiaries sorted lex.
        assert_eq!(table.fields, vec!["id", "email", "name"]);
        assert_eq!(
            table.records.get(&vec!["1".into()]),
            Some(&vec!["a@b.com".into(), "Alice".into()])
        );
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
        kind: Kind,
        primary_key: bool,
        null_sentinel: Option<&str>,
    ) -> FieldConfig {
        FieldConfig {
            name: name.to_string(),
            kind,
            primary_key,
            null_sentinel: null_sentinel.map(str::to_string),
            ..Default::default()
        }
    }

    #[test]
    fn test_parse_csv_parses_numbers() {
        let config = make_config(
            vec![
                make_typed_field("id", Kind::Number, true, None),
                make_typed_field("count", Kind::Number, false, None),
                make_typed_field("name", Kind::Text, false, None),
            ],
            true,
        );
        let reader = Table::test_reader("id,count,name\n0.0,1e2,Alice\n+5,1.10,Bob\n", true);
        let table = Table::parse_csv("t", &config, &FilterConfig::default(), reader).unwrap();

        // "0.0" parses to 0.0; "1e2" parses to 100.0
        assert_eq!(
            table.records.get(&vec![Cell::Number(0.0)]),
            Some(&vec![Cell::Number(100.0), "Alice".into()])
        );
        // "+5" parses to 5.0; "1.10" parses to 1.1
        assert_eq!(
            table.records.get(&vec![Cell::Number(5.0)]),
            Some(&vec![Cell::Number(1.1), "Bob".into()])
        );
    }

    #[test]
    fn test_parse_csv_respects_null_sentinel_on_number() {
        let config = make_config(
            vec![
                make_typed_field("id", Kind::Number, true, None),
                make_typed_field("count", Kind::Number, false, Some("N/A")),
            ],
            true,
        );
        let reader = Table::test_reader("id,count\n1,N/A\n2,3.0\n", true);
        let table = Table::parse_csv("t", &config, &FilterConfig::default(), reader).unwrap();

        // Sentinel becomes Cell::Null, even though "N/A" is not a number.
        assert_eq!(
            table.records.get(&vec![Cell::Number(1.0)]),
            Some(&vec![Cell::Null])
        );
        // Non-sentinel parses as a number.
        assert_eq!(
            table.records.get(&vec![Cell::Number(2.0)]),
            Some(&vec![Cell::Number(3.0)])
        );
    }

    #[test]
    fn test_parse_csv_parses_booleans_with_default_sentinels() {
        let config = make_config(
            vec![
                make_typed_field("id", Kind::Number, true, None),
                make_typed_field("active", Kind::Boolean, false, None),
            ],
            true,
        );
        let reader = Table::test_reader("id,active\n1,true\n2,false\n", true);
        let table = Table::parse_csv("t", &config, &FilterConfig::default(), reader).unwrap();

        assert_eq!(
            table.records.get(&vec![Cell::Number(1.0)]),
            Some(&vec![Cell::Boolean(true)])
        );
        assert_eq!(
            table.records.get(&vec![Cell::Number(2.0)]),
            Some(&vec![Cell::Boolean(false)])
        );
    }

    #[test]
    fn test_parse_csv_default_boolean_sentinels_are_strict() {
        let config = make_config(
            vec![
                make_typed_field("id", Kind::Number, true, None),
                make_typed_field("active", Kind::Boolean, false, None),
            ],
            true,
        );
        let reader = Table::test_reader("id,active\n1,True\n", true);
        let err = Table::parse_csv("t", &config, &FilterConfig::default(), reader).unwrap_err();
        let msg = format!("{:#}", err);
        assert!(msg.contains("invalid boolean value"), "got: {msg}");
    }

    #[test]
    fn test_parse_csv_respects_custom_boolean_sentinels() {
        let mut field = make_typed_field("active", Kind::Boolean, false, None);
        field.true_sentinel = Some("Y".to_string());
        field.false_sentinel = Some("N".to_string());
        let config = make_config(
            vec![make_typed_field("id", Kind::Number, true, None), field],
            true,
        );
        let reader = Table::test_reader("id,active\n1,Y\n2,N\n", true);
        let table = Table::parse_csv("t", &config, &FilterConfig::default(), reader).unwrap();

        assert_eq!(
            table.records.get(&vec![Cell::Number(1.0)]),
            Some(&vec![Cell::Boolean(true)])
        );
        assert_eq!(
            table.records.get(&vec![Cell::Number(2.0)]),
            Some(&vec![Cell::Boolean(false)])
        );
    }

    #[test]
    fn test_parse_csv_custom_boolean_sentinels_reject_defaults() {
        // When per-field sentinels are configured, the strict defaults are no
        // longer accepted — only the configured strings.
        let mut field = make_typed_field("active", Kind::Boolean, false, None);
        field.true_sentinel = Some("Y".to_string());
        field.false_sentinel = Some("N".to_string());
        let config = make_config(
            vec![make_typed_field("id", Kind::Number, true, None), field],
            true,
        );
        let reader = Table::test_reader("id,active\n1,true\n", true);
        let err = Table::parse_csv("t", &config, &FilterConfig::default(), reader).unwrap_err();
        let msg = format!("{:#}", err);
        assert!(msg.contains("invalid boolean value"), "got: {msg}");
    }

    #[test]
    fn test_parse_csv_rejects_invalid_number() {
        let config = make_config(
            vec![
                make_typed_field("id", Kind::Number, true, None),
                make_typed_field("count", Kind::Number, false, None),
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
