use std::collections::HashMap;
use std::fmt;
use std::fs::File;
use std::path::Path;

use anyhow::{Context, Result};

use crate::callbacks::{CellResult, TableCallbacks};
use crate::cell::{Cell, Kind, display_proto_cells, parse_boolean, parse_typed_cell};
use crate::config::{CsvConfig, FieldConfig, TableConfig};
use crate::record::decode_proto_records;

type ProtoTable = crate::proto::table::Table;

/// Tuple positions (column index, field config) for one half of a row,
/// either the primary-key columns or the subsidiaries. Sorted
/// lexicographically by field name to keep tuple identity stable across
/// config field reorderings.
type CanonicalColumns<'a> = Vec<(usize, &'a FieldConfig)>;

/// A table with records stored in a hash map for efficient lookup.
#[derive(Debug, Clone, PartialEq)]
pub struct Table {
    /// The primary-key field names, in tuple order.
    pub primary_key_names: Vec<String>,
    /// The subsidiary (non-key) field names, in tuple order.
    pub subsidiary_value_names: Vec<String>,
    /// Map from primary key values to subsidiary values.
    pub records: HashMap<Vec<Cell>, Vec<Cell>>,
}

impl TryFrom<ProtoTable> for Table {
    type Error = anyhow::Error;

    fn try_from(proto: ProtoTable) -> Result<Self> {
        let records = decode_proto_records(proto.records)?;
        Ok(Table {
            primary_key_names: proto.primary_key_names,
            subsidiary_value_names: proto.subsidiary_value_names,
            records,
        })
    }
}

impl From<Table> for ProtoTable {
    fn from(table: Table) -> Self {
        let records = table.records.into_iter().map(Into::into).collect();
        ProtoTable {
            primary_key_names: table.primary_key_names,
            subsidiary_value_names: table.subsidiary_value_names,
            records,
        }
    }
}

impl fmt::Display for ProtoTable {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        let mut field_names = self.primary_key_names.clone();
        field_names.extend_from_slice(&self.subsidiary_value_names);
        write!(f, "[{}]", field_names.join(", "))?;
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
    /// Loads a table from a CSV file. The table's `csv` block must be
    /// `Some`; callers (currently `State::compute`) check this before
    /// dispatching here.
    pub fn load_from_csv(work_dir: &Path, name: &str, config: &TableConfig) -> Result<Self> {
        let Some(csv) = config.csv.as_ref() else {
            anyhow::bail!(
                "table '{}' is callback-backed; load_from_csv does not apply",
                name
            );
        };
        let path = work_dir.join(&csv.source);
        let file =
            File::open(&path).with_context(|| format!("failed to open '{}'", path.display()))?;
        // Shared advisory lock: defense-in-depth against a cooperating producer
        // that takes an exclusive lock while rewriting the CSV in place. The
        // lock is released when `file` (moved into the reader) is dropped.
        file.lock_shared()
            .with_context(|| format!("failed to acquire shared lock on '{}'", path.display()))?;
        let reader = csv::ReaderBuilder::new()
            .has_headers(csv.header)
            .from_reader(file);

        log::debug!("Parsing csv file '{}'...", path.display());
        let table = Self::parse_csv(config, reader)?;

        log::debug!(
            "Loaded table '{}' with {} records",
            name,
            table.records.len()
        );

        Ok(table)
    }

    /// Loads a table by pulling rows from a caller-supplied cell callback.
    ///
    /// Rows are requested in ascending order from `row = 0` until the callback
    /// returns `LCH_END_OF_TABLE`. Within a row, leech2 requests cells in
    /// canonical order (primary keys lex-sorted, then subsidiaries lex-sorted)
    /// and the `col` it passes is the field's 0-based position in
    /// `config.fields` (declaration order).
    pub fn load_from_callbacks(
        name: &str,
        config: &TableConfig,
        callbacks: &TableCallbacks<'_>,
    ) -> Result<Self> {
        // The "field indices" passed into compute_canonical_columns are the
        // 0-based declaration-order positions in config.fields. That matches
        // the index leech promises to pass to the callback as `col`, so we
        // can synthesize it as the identity mapping.
        let positions: Vec<usize> = (0..config.fields.len()).collect();
        let (primary_columns, subsidiary_columns) =
            Self::compute_canonical_columns(config, &positions);

        let primary_key_names: Vec<String> = primary_columns
            .iter()
            .map(|(_, field)| field.name.clone())
            .collect();
        let subsidiary_value_names: Vec<String> = subsidiary_columns
            .iter()
            .map(|(_, field)| field.name.clone())
            .collect();

        let mut records: HashMap<Vec<Cell>, Vec<Cell>> = HashMap::new();
        let mut row: usize = 0;

        loop {
            let outcome =
                fetch_callback_row(name, callbacks, row, &primary_columns, &subsidiary_columns)?;
            match outcome {
                RowOutcome::Row {
                    primary_key,
                    subsidiary,
                } => {
                    if records.insert(primary_key.clone(), subsidiary).is_some() {
                        anyhow::bail!("duplicate primary key {:?}", primary_key);
                    }
                    row += 1;
                }
                RowOutcome::Filtered => {
                    row += 1;
                }
                RowOutcome::EndOfTable => break,
            }
        }

        log::debug!(
            "Loaded table '{}' with {} records from callback",
            name,
            records.len()
        );

        Ok(Table {
            primary_key_names,
            subsidiary_value_names,
            records,
        })
    }

    /// Map each config field to its CSV column index.
    /// When `csv.header` is true, match by name; otherwise, use positional order.
    fn resolve_field_indices(
        config: &TableConfig,
        reader: &mut csv::Reader<File>,
    ) -> Result<Vec<usize>> {
        let field_names = config.field_names();
        let mut indices = Vec::with_capacity(field_names.len());
        if config.csv.as_ref().is_some_and(|csv| csv.header) {
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

    fn parse_csv(config: &TableConfig, mut reader: csv::Reader<File>) -> Result<Self> {
        let Some(csv) = config.csv.as_ref() else {
            anyhow::bail!("parse_csv requires a configured [csv] block");
        };
        let field_names = config.field_names();
        let field_indices = Self::resolve_field_indices(config, &mut reader)?;
        let (primary_columns, subsidiary_columns) =
            Self::compute_canonical_columns(config, &field_indices);

        let primary_key_names: Vec<String> = primary_columns
            .iter()
            .map(|(_, field)| field.name.clone())
            .collect();
        let subsidiary_value_names: Vec<String> = subsidiary_columns
            .iter()
            .map(|(_, field)| field.name.clone())
            .collect();

        let mut records: HashMap<Vec<Cell>, Vec<Cell>> = HashMap::new();

        for (row_num, record) in reader.into_records().enumerate() {
            let record = record?;

            if !csv.header && record.len() != field_names.len() {
                anyhow::bail!(
                    "row {}: expected {} fields but got {}",
                    row_num + 1,
                    field_names.len(),
                    record.len()
                );
            }

            let values: Vec<&str> = field_indices.iter().map(|&i| &record[i]).collect();
            let reason = csv.should_filter(&field_names, &values);
            if let Some(reason) = reason {
                log::debug!("Filtered record at row {}: {}", row_num + 1, reason);
                continue;
            }

            let primary_key = parse_columns(&record, &primary_columns, csv)
                .with_context(|| format!("row {}", row_num + 1))?;
            let subsidiary = parse_columns(&record, &subsidiary_columns, csv)
                .with_context(|| format!("row {}", row_num + 1))?;

            if records.insert(primary_key.clone(), subsidiary).is_some() {
                anyhow::bail!("duplicate primary key {:?}", primary_key);
            }
        }

        Ok(Table {
            primary_key_names,
            subsidiary_value_names,
            records,
        })
    }
}

/// For each `(column_index, field_config)` entry, pull the value at
/// `column_index` out of `record` and parse it into a typed `Cell`
/// according to `field_config` and the table's CSV sentinels.
fn parse_columns(
    record: &csv::StringRecord,
    columns: &[(usize, &FieldConfig)],
    csv: &CsvConfig,
) -> Result<Vec<Cell>> {
    let mut out = Vec::with_capacity(columns.len());
    for &(column_index, field) in columns {
        out.push(parse_field_value(&record[column_index], field, csv)?);
    }
    Ok(out)
}

/// Outcome of asking the caller's `read_cell` hook for every cell of one row.
enum RowOutcome {
    Row {
        primary_key: Vec<Cell>,
        subsidiary: Vec<Cell>,
    },
    Filtered,
    EndOfTable,
}

/// Walk all canonical columns of a single row, returning what the caller said
/// about that row: a populated row, a filtered row, or end-of-table.
fn fetch_callback_row(
    name: &str,
    callbacks: &TableCallbacks<'_>,
    row: usize,
    primary_columns: &[(usize, &FieldConfig)],
    subsidiary_columns: &[(usize, &FieldConfig)],
) -> Result<RowOutcome> {
    let mut primary_key: Vec<Cell> = Vec::with_capacity(primary_columns.len());
    let mut subsidiary: Vec<Cell> = Vec::with_capacity(subsidiary_columns.len());

    for (group_out, group_cols) in [
        (&mut primary_key, primary_columns),
        (&mut subsidiary, subsidiary_columns),
    ] {
        for &(decl_idx, field_cfg) in group_cols {
            match callbacks.read_cell(row, decl_idx)? {
                CellResult::Cell(cell) => {
                    validate_cell(&cell, field_cfg)
                        .with_context(|| format!("row {} field '{}'", row + 1, field_cfg.name))?;
                    group_out.push(cell);
                }
                CellResult::EndOfTable => return Ok(RowOutcome::EndOfTable),
                CellResult::SkipRecord => {
                    log::trace!(
                        "Callback skipped row {} of table '{}' at field '{}'",
                        row + 1,
                        name,
                        field_cfg.name,
                    );
                    return Ok(RowOutcome::Filtered);
                }
            }
        }
    }

    Ok(RowOutcome::Row {
        primary_key,
        subsidiary,
    })
}

/// Validate a cell pulled from a callback against its field configuration.
/// Enforces:
/// - `Cell::Null` is rejected on primary-key fields.
/// - The cell's kind matches the field's declared kind (TEXT / NUMBER /
///   BOOLEAN); `Null` is accepted for any non-primary-key field regardless
///   of the declared kind.
fn validate_cell(cell: &Cell, field: &FieldConfig) -> Result<()> {
    if let Cell::Null = cell {
        if field.primary_key {
            anyhow::bail!("primary-key field must not be NULL");
        }
        return Ok(());
    }
    if cell.kind() != field.kind {
        anyhow::bail!(
            "cell kind {:?} does not match field kind {:?}",
            cell.kind(),
            field.kind,
        );
    }
    Ok(())
}

/// Parse a single CSV value into a `Cell` based on its field config and the
/// table-wide CSV sentinels. Values matching `csv.null` become `Cell::Null`
/// (rejected on primary-key fields); BOOLEAN values match against
/// `csv.true` / `csv.false` (falling back to the strict defaults `"true"` /
/// `"false"` when the pattern is unset); other values parse by the field's
/// declared kind.
fn parse_field_value(value: &str, field: &FieldConfig, csv: &CsvConfig) -> Result<Cell> {
    if let Some(pattern) = &csv.null_pattern
        && pattern.is_match(value)
    {
        if field.primary_key {
            anyhow::bail!(
                "primary-key field '{}' value '{}' matches the null pattern",
                field.name,
                value,
            );
        }
        return Ok(Cell::Null);
    }
    if let Kind::Boolean = field.kind {
        return parse_boolean(value, csv.true_pattern.as_ref(), csv.false_pattern.as_ref())
            .map(Cell::Boolean)
            .with_context(|| format!("field '{}'", field.name));
    }
    parse_typed_cell(value, field.kind).with_context(|| format!("field '{}'", field.name))
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::config::FieldConfig;
    use regex::Regex;

    fn make_field(name: &str, primary_key: bool) -> FieldConfig {
        FieldConfig {
            name: name.to_string(),
            primary_key,
            ..Default::default()
        }
    }

    fn make_csv(header: bool) -> CsvConfig {
        CsvConfig {
            source: "test.csv".to_string(),
            header,
            ..Default::default()
        }
    }

    fn make_config(fields: Vec<FieldConfig>, header: bool) -> TableConfig {
        TableConfig {
            fields,
            csv: Some(make_csv(header)),
        }
    }

    fn make_config_with_csv(fields: Vec<FieldConfig>, csv: CsvConfig) -> TableConfig {
        TableConfig {
            fields,
            csv: Some(csv),
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
        let table_a = Table::parse_csv(&config_a, reader_a).unwrap();
        let reader_b = Table::test_reader(csv, true);
        let table_b = Table::parse_csv(&config_b, reader_b).unwrap();

        assert_eq!(table_a.primary_key_names, vec!["id"]);
        assert_eq!(table_a.subsidiary_value_names, vec!["email", "name"]);
        assert_eq!(table_a.primary_key_names, table_b.primary_key_names);
        assert_eq!(
            table_a.subsidiary_value_names,
            table_b.subsidiary_value_names
        );
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
        let table = Table::parse_csv(&config, reader).unwrap();

        // Canonical layout: id (PK), then subsidiaries sorted lex.
        assert_eq!(table.primary_key_names, vec!["id"]);
        assert_eq!(table.subsidiary_value_names, vec!["email", "name"]);
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

    fn make_typed_field(name: &str, kind: Kind, primary_key: bool) -> FieldConfig {
        FieldConfig {
            name: name.to_string(),
            kind,
            primary_key,
        }
    }

    #[test]
    fn test_parse_csv_parses_numbers() {
        let config = make_config(
            vec![
                make_typed_field("id", Kind::Number, true),
                make_typed_field("count", Kind::Number, false),
                make_typed_field("name", Kind::Text, false),
            ],
            true,
        );
        let reader = Table::test_reader("id,count,name\n0.0,1e2,Alice\n+5,1.10,Bob\n", true);
        let table = Table::parse_csv(&config, reader).unwrap();

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
    fn test_parse_csv_respects_null_pattern_on_number() {
        let csv = CsvConfig {
            source: "test.csv".to_string(),
            header: true,
            null_pattern: Some(Regex::new("^N/A$").unwrap()),
            ..Default::default()
        };
        let config = make_config_with_csv(
            vec![
                make_typed_field("id", Kind::Number, true),
                make_typed_field("count", Kind::Number, false),
            ],
            csv,
        );
        let reader = Table::test_reader("id,count\n1,N/A\n2,3.0\n", true);
        let table = Table::parse_csv(&config, reader).unwrap();

        // The null pattern produces Cell::Null even though "N/A" is not a number.
        assert_eq!(
            table.records.get(&vec![Cell::Number(1.0)]),
            Some(&vec![Cell::Null])
        );
        // Non-matching value parses as a number.
        assert_eq!(
            table.records.get(&vec![Cell::Number(2.0)]),
            Some(&vec![Cell::Number(3.0)])
        );
    }

    #[test]
    fn test_parse_csv_parses_booleans_with_default_sentinels() {
        let config = make_config(
            vec![
                make_typed_field("id", Kind::Number, true),
                make_typed_field("active", Kind::Boolean, false),
            ],
            true,
        );
        let reader = Table::test_reader("id,active\n1,true\n2,false\n", true);
        let table = Table::parse_csv(&config, reader).unwrap();

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
                make_typed_field("id", Kind::Number, true),
                make_typed_field("active", Kind::Boolean, false),
            ],
            true,
        );
        let reader = Table::test_reader("id,active\n1,True\n", true);
        let err = Table::parse_csv(&config, reader).unwrap_err();
        let msg = format!("{:#}", err);
        assert!(msg.contains("invalid boolean value"), "got: {msg}");
    }

    #[test]
    fn test_parse_csv_respects_custom_boolean_patterns() {
        let csv = CsvConfig {
            source: "test.csv".to_string(),
            header: true,
            true_pattern: Some(Regex::new("^Y$").unwrap()),
            false_pattern: Some(Regex::new("^N$").unwrap()),
            ..Default::default()
        };
        let config = make_config_with_csv(
            vec![
                make_typed_field("id", Kind::Number, true),
                make_typed_field("active", Kind::Boolean, false),
            ],
            csv,
        );
        let reader = Table::test_reader("id,active\n1,Y\n2,N\n", true);
        let table = Table::parse_csv(&config, reader).unwrap();

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
    fn test_parse_csv_custom_boolean_patterns_reject_defaults() {
        // When per-table boolean patterns are configured, the strict defaults
        // are no longer accepted -- only values matching the configured patterns.
        let csv = CsvConfig {
            source: "test.csv".to_string(),
            header: true,
            true_pattern: Some(Regex::new("^Y$").unwrap()),
            false_pattern: Some(Regex::new("^N$").unwrap()),
            ..Default::default()
        };
        let config = make_config_with_csv(
            vec![
                make_typed_field("id", Kind::Number, true),
                make_typed_field("active", Kind::Boolean, false),
            ],
            csv,
        );
        let reader = Table::test_reader("id,active\n1,true\n", true);
        let err = Table::parse_csv(&config, reader).unwrap_err();
        let msg = format!("{:#}", err);
        assert!(msg.contains("invalid boolean value"), "got: {msg}");
    }

    #[test]
    fn test_parse_csv_rejects_invalid_number() {
        let config = make_config(
            vec![
                make_typed_field("id", Kind::Number, true),
                make_typed_field("count", Kind::Number, false),
            ],
            true,
        );
        let reader = Table::test_reader("id,count\n1,abc\n", true);
        let err = Table::parse_csv(&config, reader).unwrap_err();
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

    // -- validate_cell tests --

    #[test]
    fn test_validate_cell_rejects_null_on_primary_key() {
        let field = make_typed_field("id", Kind::Number, true);
        let err = validate_cell(&Cell::Null, &field).unwrap_err();
        assert!(format!("{:#}", err).contains("primary-key"), "got: {err:#}");
    }

    #[test]
    fn test_validate_cell_accepts_null_on_subsidiary() {
        let field = make_typed_field("count", Kind::Number, false);
        validate_cell(&Cell::Null, &field).unwrap();
    }

    #[test]
    fn test_validate_cell_rejects_kind_mismatch() {
        let field = make_typed_field("count", Kind::Number, false);
        let err = validate_cell(&Cell::Text("oops".to_string()), &field).unwrap_err();
        assert!(format!("{:#}", err).contains("kind"), "got: {err:#}");
    }

    #[test]
    fn test_validate_cell_accepts_matching_kind() {
        let field = make_typed_field("name", Kind::Text, true);
        validate_cell(&Cell::Text("Alice".to_string()), &field).unwrap();
    }

    // -- load_from_callbacks tests --
    //
    // Tests use a thread-local script that maps (row, field_name) -> action;
    // the test callback walks the script and forwards the result back to leech2
    // through an `FfiCell`. The script owns the CStrings backing TEXT cells so
    // their pointers stay valid for the duration of the call.

    use crate::callbacks::{Callbacks, FfiCallbacks};
    use crate::ffi::{
        END_OF_TABLE, FfiCell, FfiCellPayload, SKIP_RECORD, SUCCESS as FFI_SUCCESS, VALUE_NULL,
    };
    use std::cell::RefCell;
    use std::collections::HashMap;
    use std::ffi::{CStr, CString, c_char, c_void};

    enum CellAction {
        Cell(CellValue),
        Skip,
    }

    enum CellValue {
        Null,
        Text(CString),
        Number(f64),
    }

    /// One row's worth of canned answers, keyed by field name. A field that
    /// is not present in the map is treated as `EndOfTable` (so a test can
    /// model "row past the end" by omitting it entirely).
    type RowScript = HashMap<&'static str, CellAction>;

    /// Sequence of row scripts. Row index `r` consults `rows[r]`; once `r`
    /// exceeds `rows.len()`, every cell returns `EndOfTable`.
    struct Script {
        rows: Vec<RowScript>,
    }

    thread_local! {
        static SCRIPT: RefCell<Option<Script>> = const { RefCell::new(None) };
    }

    fn install_script(script: Script) {
        SCRIPT.with(|s| *s.borrow_mut() = Some(script));
    }

    fn clear_script() {
        SCRIPT.with(|s| *s.borrow_mut() = None);
    }

    unsafe extern "C" fn test_read_cell(
        _table: *const c_char,
        row: usize,
        _col: usize,
        field: *const c_char,
        out_cell: *mut FfiCell,
        _usr_data: *mut c_void,
    ) -> i32 {
        let field_name = unsafe { CStr::from_ptr(field) }.to_str().unwrap();
        SCRIPT.with(|s| {
            let s = s.borrow();
            let script = s.as_ref().expect("SCRIPT not installed");
            if row >= script.rows.len() {
                return END_OF_TABLE;
            }
            let Some(action) = script.rows[row].get(field_name) else {
                return END_OF_TABLE;
            };
            match action {
                CellAction::Skip => SKIP_RECORD,
                CellAction::Cell(value) => {
                    let cell = match value {
                        CellValue::Null => FfiCell {
                            kind: VALUE_NULL,
                            payload: FfiCellPayload { number: 0.0 },
                        },
                        CellValue::Text(s) => FfiCell {
                            kind: 1, // LCH_VALUE_TEXT
                            payload: FfiCellPayload { text: s.as_ptr() },
                        },
                        CellValue::Number(n) => FfiCell {
                            kind: 2, // LCH_VALUE_NUMBER
                            payload: FfiCellPayload { number: *n },
                        },
                    };
                    unsafe { *out_cell = cell };
                    FFI_SUCCESS
                }
            }
        })
    }

    fn make_callbacks() -> Callbacks {
        Callbacks::from(&FfiCallbacks {
            table_begin: None,
            read_cell: Some(test_read_cell),
            table_end: None,
            usr_data: std::ptr::null_mut(),
        })
    }

    fn load_table(name: &str, config: &TableConfig) -> Result<Table> {
        let callbacks = make_callbacks();
        let field_names: Vec<&str> = config.fields.iter().map(|f| f.name.as_str()).collect();
        let bound = callbacks.for_table(name, &field_names).unwrap();
        Table::load_from_callbacks(name, config, &bound)
    }

    fn typed_config(fields: Vec<FieldConfig>) -> TableConfig {
        TableConfig { fields, csv: None }
    }

    fn cell_text(s: &str) -> CellAction {
        CellAction::Cell(CellValue::Text(CString::new(s).unwrap()))
    }

    fn cell_number(n: f64) -> CellAction {
        CellAction::Cell(CellValue::Number(n))
    }

    #[test]
    fn test_load_from_callbacks_happy_path() {
        let config = typed_config(vec![
            make_typed_field("id", Kind::Number, true),
            make_typed_field("name", Kind::Text, false),
        ]);
        install_script(Script {
            rows: vec![
                HashMap::from([("id", cell_number(1.0)), ("name", cell_text("Alice"))]),
                HashMap::from([("id", cell_number(2.0)), ("name", cell_text("Bob"))]),
            ],
        });

        let table = load_table("t", &config).unwrap();

        assert_eq!(table.primary_key_names, vec!["id".to_string()]);
        assert_eq!(table.subsidiary_value_names, vec!["name".to_string()]);
        assert_eq!(table.records.len(), 2);
        assert_eq!(
            table.records.get(&vec![Cell::Number(1.0)]),
            Some(&vec!["Alice".into()])
        );
        clear_script();
    }

    #[test]
    fn test_load_from_callbacks_empty_table() {
        let config = typed_config(vec![
            make_typed_field("id", Kind::Number, true),
            make_typed_field("name", Kind::Text, false),
        ]);
        install_script(Script { rows: vec![] });

        let table = load_table("t", &config).unwrap();

        assert!(table.records.is_empty());
        clear_script();
    }

    #[test]
    fn test_load_from_callbacks_skip_record_drops_row() {
        let config = typed_config(vec![
            make_typed_field("id", Kind::Number, true),
            make_typed_field("name", Kind::Text, false),
        ]);
        install_script(Script {
            rows: vec![
                HashMap::from([("id", cell_number(1.0)), ("name", cell_text("Alice"))]),
                // Row 2 is skipped when leech asks for its primary key.
                HashMap::from([("id", CellAction::Skip), ("name", cell_text("Bob"))]),
                HashMap::from([("id", cell_number(3.0)), ("name", cell_text("Carol"))]),
            ],
        });

        let table = load_table("t", &config).unwrap();

        assert_eq!(table.records.len(), 2);
        assert!(table.records.contains_key(&vec![Cell::Number(1.0)]));
        assert!(table.records.contains_key(&vec![Cell::Number(3.0)]));
        assert!(!table.records.contains_key(&vec![Cell::Number(2.0)]));
        clear_script();
    }

    #[test]
    fn test_load_from_callbacks_duplicate_primary_key_errors() {
        let config = typed_config(vec![
            make_typed_field("id", Kind::Number, true),
            make_typed_field("name", Kind::Text, false),
        ]);
        install_script(Script {
            rows: vec![
                HashMap::from([("id", cell_number(1.0)), ("name", cell_text("Alice"))]),
                HashMap::from([("id", cell_number(1.0)), ("name", cell_text("Alice2"))]),
            ],
        });

        let err = load_table("t", &config).unwrap_err();
        assert!(
            format!("{:#}", err).contains("duplicate primary key"),
            "got: {err:#}"
        );
        clear_script();
    }

    #[test]
    fn test_load_from_callbacks_rejects_null_primary_key() {
        let config = typed_config(vec![
            make_typed_field("id", Kind::Number, true),
            make_typed_field("name", Kind::Text, false),
        ]);
        install_script(Script {
            rows: vec![HashMap::from([
                ("id", CellAction::Cell(CellValue::Null)),
                ("name", cell_text("Alice")),
            ])],
        });

        let err = load_table("t", &config).unwrap_err();
        assert!(format!("{:#}", err).contains("primary-key"), "got: {err:#}");
        clear_script();
    }

    #[test]
    fn test_load_from_callbacks_rejects_kind_mismatch() {
        let config = typed_config(vec![
            make_typed_field("id", Kind::Number, true),
            make_typed_field("count", Kind::Number, false),
        ]);
        install_script(Script {
            rows: vec![HashMap::from([
                ("id", cell_number(1.0)),
                ("count", cell_text("not a number")),
            ])],
        });

        let err = load_table("t", &config).unwrap_err();
        assert!(format!("{:#}", err).contains("kind"), "got: {err:#}");
        clear_script();
    }
}
