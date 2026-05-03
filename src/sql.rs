use std::collections::HashMap;

use anyhow::{Context, Result, anyhow, bail};

use crate::config::{Config, FieldConfig};
use crate::entry::Entry;
use crate::proto::delta::Delta as ProtoDelta;
use crate::proto::injected::Field as ProtoInjectedField;
use crate::proto::patch::Patch as ProtoPatch;
use crate::proto::table::Table as ProtoTable;

/// Controls how a CSV field value is formatted as a SQL literal.
///
/// These are not database column types — they determine quoting and
/// validation when embedding a value into a SQL string (e.g. `Text`
/// wraps in single quotes, `Number` validates and emits unquoted).
#[derive(Debug, Clone, PartialEq)]
pub enum SqlType {
    Text,
    Number,
    Boolean,
}

impl SqlType {
    pub fn from_config(type_str: &str) -> Result<Self> {
        match type_str.to_uppercase().as_str() {
            "TEXT" => Ok(SqlType::Text),
            "NUMBER" => Ok(SqlType::Number),
            "BOOLEAN" => Ok(SqlType::Boolean),
            other => bail!(
                "unknown field type '{}'; valid types are: TEXT, NUMBER, BOOLEAN",
                other
            ),
        }
    }
}

/// Per-field metadata resolved from config.
struct FieldMeta {
    name: String,
    sql_type: SqlType,
    /// When a CSV field value equals this sentinel string, it is emitted as SQL
    /// `NULL` instead of a typed literal.
    null: Option<String>,
}

impl TryFrom<&FieldConfig> for FieldMeta {
    type Error = anyhow::Error;

    fn try_from(field_config: &FieldConfig) -> Result<Self> {
        let sql_type = SqlType::from_config(&field_config.sql_type)
            .with_context(|| format!("field '{}'", field_config.name))?;
        Ok(FieldMeta {
            name: field_config.name.clone(),
            sql_type,
            null: field_config.null.clone(),
        })
    }
}

/// Schema information for a single table, resolved from config.
struct TableSchema {
    /// All fields in order: primary keys first, then subsidiary.
    fields: Vec<FieldMeta>,
    /// Number of primary key fields (the first `num_primary_keys` entries in `fields`).
    num_primary_keys: usize,
}

impl TableSchema {
    /// Resolve a table's schema from config, partitioning fields into
    /// primary-key fields followed by subsidiary fields while preserving
    /// declaration order within each group.
    fn resolve(config: &Config, table_name: &str) -> Result<Self> {
        let table_config = config
            .tables
            .get(table_name)
            .with_context(|| format!("table '{}' not found in config", table_name))?;

        // Build a name→config lookup so we can resolve type/null for each field.
        let field_config_by_name: HashMap<&str, &FieldConfig> = table_config
            .fields
            .iter()
            .map(|field| (field.name.as_str(), field))
            .collect();

        let primary_key_names = table_config.primary_key();
        let all_field_names = table_config.field_names();

        let mut fields = Vec::new();

        // Primary-key fields first.
        for name in &primary_key_names {
            let field_config = field_config_by_name.get(name.as_str()).with_context(|| {
                format!(
                    "primary key field '{}' not found in table '{}'",
                    name, table_name
                )
            })?;
            let field_meta = (*field_config)
                .try_into()
                .with_context(|| format!("table '{}'", table_name))?;
            fields.push(field_meta);
        }

        // Then subsidiary fields.
        for name in &all_field_names {
            if !primary_key_names.contains(name) {
                let field_config = field_config_by_name.get(name.as_str()).with_context(|| {
                    format!("field '{}' not found in table '{}'", name, table_name)
                })?;
                let field_meta = (*field_config)
                    .try_into()
                    .with_context(|| format!("table '{}'", table_name))?;
                fields.push(field_meta);
            }
        }

        Ok(TableSchema {
            num_primary_keys: primary_key_names.len(),
            fields,
        })
    }

    fn primary_key_fields(&self) -> &[FieldMeta] {
        &self.fields[..self.num_primary_keys]
    }

    fn subsidiary_fields(&self) -> &[FieldMeta] {
        &self.fields[self.num_primary_keys..]
    }
}

/// A static field injected into all SQL output (resolved from proto).
struct InjectedField {
    name: String,
    sql_type: SqlType,
    value: String,
}

impl TryFrom<&ProtoInjectedField> for InjectedField {
    type Error = anyhow::Error;

    fn try_from(proto: &ProtoInjectedField) -> Result<Self> {
        let sql_type = SqlType::from_config(&proto.sql_type)
            .with_context(|| format!("injected field '{}'", proto.name))?;
        Ok(InjectedField {
            name: proto.name.clone(),
            sql_type,
            value: proto.value.clone(),
        })
    }
}

impl InjectedField {
    fn where_clause(&self) -> Result<String> {
        let literal = quote_literal(&self.value, &self.sql_type)
            .with_context(|| format!("injected field '{}' value", self.name))?;
        Ok(format!("{} = {}", quote_identifier(&self.name), literal))
    }

    fn quoted_column(&self) -> String {
        quote_identifier(&self.name)
    }

    fn quoted_value(&self) -> Result<String> {
        quote_literal(&self.value, &self.sql_type)
            .with_context(|| format!("injected field '{}' value", self.name))
    }
}

/// Double-quote a SQL identifier, escaping embedded double quotes.
fn quote_identifier(name: &str) -> String {
    format!("\"{}\"", name.replace('"', "\"\""))
}

/// Canonicalize a numeric string so that mathematically equal values
/// produce the same representation (e.g. `"0"` and `"0.0"` both become
/// `"0"`, and `"1e2"` becomes `"100"`).
///
/// Precision is preserved: digit shifting and trimming are done by string
/// manipulation, never by round-tripping through `f64`. The only role of
/// `f64` parsing is to reject non-numeric input and non-finite values
/// (`NaN`, `inf`, exponents that overflow to infinity).
pub fn normalize_number(value: &str) -> Result<String> {
    let parsed: f64 = value
        .parse()
        .with_context(|| format!("invalid number: '{}'", value))?;
    if !parsed.is_finite() {
        bail!("invalid number: '{}'", value);
    }

    // Tokenize: optional sign, then mantissa, then optional exponent.
    let (sign, rest) = match value.as_bytes().first() {
        Some(b'-') => ("-", &value[1..]),
        Some(b'+') => ("", &value[1..]),
        _ => ("", value),
    };
    let (mantissa, exponent) = match rest.find(['e', 'E']) {
        Some(pos) => {
            let exp: i64 = rest[pos + 1..]
                .parse()
                .with_context(|| format!("invalid number: '{}'", value))?;
            (&rest[..pos], exp)
        }
        None => (rest, 0),
    };
    let (int_digits, frac_digits) = mantissa.split_once('.').unwrap_or((mantissa, ""));

    // Compute fixed-point form by placing the decimal at position
    // `int_digits.len() + exponent` within the concatenated digit string.
    let combined: String = format!("{}{}", int_digits, frac_digits);
    let decimal_pos = int_digits.len() as i64 + exponent;

    let (int_part, frac_part) = if decimal_pos <= 0 {
        let leading_zeros = "0".repeat((-decimal_pos) as usize);
        ("0".to_string(), format!("{}{}", leading_zeros, combined))
    } else if (decimal_pos as usize) >= combined.len() {
        let trailing_zeros = "0".repeat(decimal_pos as usize - combined.len());
        (format!("{}{}", combined, trailing_zeros), String::new())
    } else {
        let (int_chars, frac_chars) = combined.split_at(decimal_pos as usize);
        (int_chars.to_string(), frac_chars.to_string())
    };

    // Strip insignificant zeros and reassemble.
    let int_trimmed = int_part.trim_start_matches('0');
    let int_canonical = if int_trimmed.is_empty() {
        "0"
    } else {
        int_trimmed
    };
    let frac_trimmed = frac_part.trim_end_matches('0');

    let magnitude = if frac_trimmed.is_empty() {
        int_canonical.to_string()
    } else {
        format!("{}.{}", int_canonical, frac_trimmed)
    };

    // Don't render a sign on canonical zero.
    if magnitude == "0" {
        Ok(magnitude)
    } else {
        Ok(format!("{}{}", sign, magnitude))
    }
}

/// Canonicalize a boolean string to lowercase `"true"` or `"false"`, so
/// that `"True"`, `"1"`, `"yes"`, `"t"` (and their false counterparts)
/// don't compare unequal in the diff pipeline.
pub fn normalize_boolean(value: &str) -> Result<String> {
    match value.to_lowercase().as_str() {
        "true" | "1" | "t" | "yes" => Ok("true".to_string()),
        "false" | "0" | "f" | "no" => Ok("false".to_string()),
        _ => bail!("invalid boolean value: '{}'", value),
    }
}

/// Format a value as a SQL literal based on its type.
pub fn quote_literal(value: &str, sql_type: &SqlType) -> Result<String> {
    match sql_type {
        SqlType::Text => Ok(format!("'{}'", value.replace('\'', "''"))),
        SqlType::Number => {
            let number: f64 = value.parse()?;
            if !number.is_finite() {
                bail!("invalid number: '{}'", value);
            }
            Ok(value.to_string())
        }
        SqlType::Boolean => match value.to_lowercase().as_str() {
            "true" | "1" | "t" | "yes" => Ok("TRUE".to_string()),
            "false" | "0" | "f" | "no" => Ok("FALSE".to_string()),
            _ => bail!("invalid boolean value: '{}'", value),
        },
    }
}

/// Format a value as a SQL literal, emitting `NULL` if it matches the sentinel.
fn format_value(value: &str, field: &FieldMeta) -> Result<String> {
    if let Some(ref sentinel) = field.null
        && value == sentinel
    {
        return Ok("NULL".to_string());
    }
    quote_literal(value, &field.sql_type)
}

/// Convert key + value slices into a list of SQL literal strings.
fn format_row(key: &[String], value: &[String], schema: &TableSchema) -> Result<Vec<String>> {
    let primary_key_fields = schema.primary_key_fields();
    let subsidiary_fields = schema.subsidiary_fields();

    if key.len() != primary_key_fields.len() {
        bail!(
            "primary key field count mismatch: got {} values, expected {}",
            key.len(),
            primary_key_fields.len()
        );
    }
    if value.len() != subsidiary_fields.len() {
        bail!(
            "subsidiary field count mismatch: got {} values, expected {}",
            value.len(),
            subsidiary_fields.len()
        );
    }

    let mut literals = Vec::with_capacity(key.len() + value.len());
    for (value, field) in key.iter().zip(primary_key_fields) {
        let literal =
            format_value(value, field).with_context(|| format!("field '{}'", field.name))?;
        literals.push(literal);
    }
    for (value, field) in value.iter().zip(subsidiary_fields) {
        let literal =
            format_value(value, field).with_context(|| format!("field '{}'", field.name))?;
        literals.push(literal);
    }
    Ok(literals)
}

/// Generate DELETE statements for a list of entries.
fn emit_deletes(
    entries: &[Entry],
    schema: &TableSchema,
    injected_fields: &[InjectedField],
    quoted_table: &str,
    out: &mut String,
) -> Result<()> {
    for entry in entries {
        let where_clause = primary_key_where_clause(&entry.key, schema, injected_fields)
            .with_context(|| format!("key {:?}", entry.key))?;
        out.push_str(&format!(
            "DELETE FROM {} WHERE {};\n",
            quoted_table, where_clause
        ));
    }
    Ok(())
}

/// Generate INSERT statements for a list of entries.
fn emit_inserts(
    entries: &[Entry],
    schema: &TableSchema,
    injected_fields: &[InjectedField],
    quoted_table: &str,
    out: &mut String,
) -> Result<()> {
    if entries.is_empty() {
        return Ok(());
    }

    let mut column_parts: Vec<String> = schema
        .fields
        .iter()
        .map(|field| quote_identifier(&field.name))
        .collect();

    let injected_columns: Vec<String> = injected_fields.iter().map(|f| f.quoted_column()).collect();
    column_parts.splice(..0, injected_columns);
    let columns = column_parts.join(", ");

    for entry in entries {
        let mut literals = format_row(&entry.key, &entry.value, schema)
            .with_context(|| format!("key {:?}", entry.key))?;
        let injected_values: Result<Vec<String>> =
            injected_fields.iter().map(|f| f.quoted_value()).collect();
        literals.splice(..0, injected_values?);
        out.push_str(&format!(
            "INSERT INTO {} ({}) VALUES ({});\n",
            quoted_table,
            columns,
            literals.join(", ")
        ));
    }

    Ok(())
}

/// Format a single UPDATE statement.
fn format_update(
    update: &crate::update::Update,
    subsidiary_fields: &[FieldMeta],
    schema: &TableSchema,
    injected_fields: &[InjectedField],
    quoted_table: &str,
) -> Result<String> {
    // Sparse updates list changed column indices explicitly; full
    // updates (empty changed_indices) include all subsidiary columns.
    let indices = if update.changed_indices.is_empty() {
        (0..subsidiary_fields.len() as u32).collect()
    } else {
        update.changed_indices.clone()
    };

    let mut set_parts = Vec::new();
    for (&index, value) in indices.iter().zip(update.new_value.iter()) {
        let field = subsidiary_fields.get(index as usize).ok_or_else(|| {
            anyhow!(
                "changed_indices entry {} is out of range (table has {} subsidiary columns)",
                index,
                subsidiary_fields.len()
            )
        })?;
        let literal =
            format_value(value, field).with_context(|| format!("field '{}'", field.name))?;
        set_parts.push(format!("{} = {}", quote_identifier(&field.name), literal));
    }

    let where_clause = primary_key_where_clause(&update.key, schema, injected_fields)?;

    Ok(format!(
        "UPDATE {} SET {} WHERE {};\n",
        quoted_table,
        set_parts.join(", "),
        where_clause
    ))
}

/// Generate UPDATE statements for a list of updates.
fn emit_updates(
    updates: &[crate::update::Update],
    schema: &TableSchema,
    injected_fields: &[InjectedField],
    quoted_table: &str,
    out: &mut String,
) -> Result<()> {
    let subsidiary_fields = schema.subsidiary_fields();

    for update in updates {
        let stmt = format_update(
            update,
            subsidiary_fields,
            schema,
            injected_fields,
            quoted_table,
        )
        .with_context(|| format!("key {:?}", update.key))?;
        out.push_str(&stmt);
    }

    Ok(())
}

/// Build a WHERE clause from primary key values and injected fields.
fn primary_key_where_clause(
    key: &[String],
    schema: &TableSchema,
    injected_fields: &[InjectedField],
) -> Result<String> {
    let mut where_parts = Vec::new();
    for (value, field) in key.iter().zip(schema.primary_key_fields()) {
        let literal =
            format_value(value, field).with_context(|| format!("field '{}'", field.name))?;
        where_parts.push(format!("{} = {}", quote_identifier(&field.name), literal));
    }
    for injected in injected_fields {
        where_parts.push(injected.where_clause()?);
    }

    Ok(where_parts.join(" AND "))
}

/// Generate SQL statements for a delta (DELETE/INSERT/UPDATE).
fn delta_to_sql(
    config: &Config,
    table_name: &str,
    delta: &ProtoDelta,
    injected_fields: &[InjectedField],
    out: &mut String,
) -> Result<()> {
    let schema = TableSchema::resolve(config, table_name)?;
    let table = quote_identifier(table_name);

    emit_deletes(&delta.deletes, &schema, injected_fields, &table, out)
        .with_context(|| format!("table '{table_name}'"))?;
    emit_inserts(&delta.inserts, &schema, injected_fields, &table, out)
        .with_context(|| format!("table '{table_name}'"))?;
    emit_updates(&delta.updates, &schema, injected_fields, &table, out)
        .with_context(|| format!("table '{table_name}'"))?;

    Ok(())
}

/// Generate SQL statements for a single table's full state (TRUNCATE/DELETE + INSERT).
fn state_table_to_sql(
    config: &Config,
    table_name: &str,
    table: &ProtoTable,
    injected_fields: &[InjectedField],
    out: &mut String,
) -> Result<()> {
    let schema = TableSchema::resolve(config, table_name)?;
    let quoted_table = quote_identifier(table_name);

    if injected_fields.is_empty() {
        out.push_str(&format!("TRUNCATE {};\n", quoted_table));
    } else {
        let mut conditions = Vec::new();
        for injected in injected_fields {
            conditions.push(injected.where_clause()?);
        }
        out.push_str(&format!(
            "DELETE FROM {} WHERE {};\n",
            quoted_table,
            conditions.join(" AND ")
        ));
    }

    emit_inserts(&table.entries, &schema, injected_fields, &quoted_table, out)
        .with_context(|| format!("table '{table_name}'"))?;

    Ok(())
}

/// Verify that a table's field hash in the patch matches the hub's config.
fn check_field_hash(
    config: &Config,
    table_name: &str,
    field_hashes: &HashMap<String, String>,
) -> Result<()> {
    let agent_hash = field_hashes
        .get(table_name)
        .with_context(|| format!("table '{}': missing field hash in patch", table_name))?;
    let table_config = config
        .tables
        .get(table_name)
        .with_context(|| format!("table '{}': not found in config", table_name))?;
    let hub_hash = table_config.field_hash();
    if agent_hash != &hub_hash {
        bail!(
            "table '{}': field hash mismatch (agent={}, hub={})",
            table_name,
            agent_hash,
            hub_hash
        );
    }
    Ok(())
}

/// Convert a decoded patch to SQL statements.
///
/// Returns a SQL string wrapped in BEGIN/COMMIT.
pub fn patch_to_sql(config: &Config, patch: &ProtoPatch) -> Result<Option<String>> {
    log::info!("Converting patch to SQL: {}", patch);

    if patch.deltas.is_empty() && patch.states.is_empty() {
        log::info!("Patch has no payload, nothing to convert");
        return Ok(None);
    }

    let mut injected_fields = Vec::new();
    for proto_field in &patch.injected_fields {
        injected_fields.push(InjectedField::try_from(proto_field)?);
    }

    let mut sql = String::from("BEGIN;\n");

    for (table_name, delta) in &patch.deltas {
        check_field_hash(config, table_name, &patch.field_hashes)?;
        delta_to_sql(config, table_name, delta, &injected_fields, &mut sql)?;
    }

    for (table_name, table) in &patch.states {
        check_field_hash(config, table_name, &patch.field_hashes)?;
        state_table_to_sql(config, table_name, table, &injected_fields, &mut sql)?;
    }

    sql.push_str("COMMIT;\n");
    Ok(Some(sql))
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::config::TruncateConfig;

    fn dummy_config(tables: HashMap<String, crate::config::TableConfig>) -> Config {
        Config {
            work_dir: std::path::PathBuf::from("/tmp"),
            injected_fields: Vec::new(),
            compression: crate::config::CompressionConfig::default(),
            tables,
            truncate: TruncateConfig::default(),
            filters: crate::config::FilterConfig::default(),
        }
    }

    /// Build a TableConfig for tests. Each entry is `(field_name, is_primary_key)`;
    /// all fields are TEXT with no NULL sentinel.
    fn dummy_table(fields: &[(&str, bool)]) -> crate::config::TableConfig {
        crate::config::TableConfig {
            source: "test.csv".to_string(),
            header: false,
            fields: fields
                .iter()
                .map(|(name, primary_key)| FieldConfig {
                    name: name.to_string(),
                    sql_type: "TEXT".to_string(),
                    primary_key: *primary_key,
                    null: None,
                })
                .collect(),
        }
    }

    /// Build a ProtoPatch for tests. Defaults `head`, `created`,
    /// `injected_fields`, `num_blocks`, and `states`; the caller supplies
    /// the deltas and field hashes that distinguish the test case.
    fn dummy_patch(
        deltas: HashMap<String, ProtoDelta>,
        field_hashes: HashMap<String, String>,
    ) -> ProtoPatch {
        ProtoPatch {
            head: "abc123".to_string(),
            created: None,
            injected_fields: Vec::new(),
            num_blocks: 1,
            deltas,
            states: HashMap::new(),
            field_hashes,
        }
    }

    /// Build an empty ProtoDelta with the given column names. Tests push
    /// inserts, deletes, or updates onto the returned delta as needed.
    fn dummy_delta(column_names: &[&str]) -> ProtoDelta {
        ProtoDelta {
            column_names: column_names.iter().map(|s| s.to_string()).collect(),
            inserts: vec![],
            deletes: vec![],
            updates: vec![],
        }
    }

    #[test]
    fn test_sql_type_from_config() {
        assert_eq!(SqlType::from_config("TEXT").unwrap(), SqlType::Text);
        assert_eq!(SqlType::from_config("NUMBER").unwrap(), SqlType::Number);
        assert_eq!(SqlType::from_config("BOOLEAN").unwrap(), SqlType::Boolean);
        // Case insensitive
        assert_eq!(SqlType::from_config("text").unwrap(), SqlType::Text);
        assert_eq!(SqlType::from_config("number").unwrap(), SqlType::Number);
        assert_eq!(SqlType::from_config("Boolean").unwrap(), SqlType::Boolean);
        // Unknown types are rejected
        assert!(SqlType::from_config("unknown").is_err());
    }

    #[test]
    fn test_quote_identifier() {
        assert_eq!(quote_identifier("simple"), "\"simple\"");
        assert_eq!(quote_identifier("has\"quote"), "\"has\"\"quote\"");
        assert_eq!(quote_identifier(""), "\"\"");
    }

    #[test]
    fn test_quote_literal_text() {
        assert_eq!(quote_literal("hello", &SqlType::Text).unwrap(), "'hello'");
        assert_eq!(quote_literal("", &SqlType::Text).unwrap(), "''");
    }

    #[test]
    fn test_quote_literal_text_with_quotes() {
        assert_eq!(
            quote_literal("it's a test", &SqlType::Text).unwrap(),
            "'it''s a test'"
        );
        assert_eq!(quote_literal("a''b", &SqlType::Text).unwrap(), "'a''''b'");
    }

    #[test]
    fn test_normalize_number_zero_forms() {
        for input in [
            "0", "0.0", "-0", "+0", "00", "0.00", "-0.000", "0e10", "0.0e-5",
        ] {
            assert_eq!(normalize_number(input).unwrap(), "0", "input: {}", input);
        }
    }

    #[test]
    fn test_normalize_number_integers() {
        assert_eq!(normalize_number("42").unwrap(), "42");
        assert_eq!(normalize_number("-42").unwrap(), "-42");
        assert_eq!(normalize_number("+5").unwrap(), "5");
        assert_eq!(normalize_number("007").unwrap(), "7");
        assert_eq!(normalize_number("100").unwrap(), "100");
        assert_eq!(normalize_number("-100").unwrap(), "-100");
    }

    #[test]
    fn test_normalize_number_trailing_zero_stripping() {
        assert_eq!(normalize_number("100.0").unwrap(), "100");
        assert_eq!(normalize_number("100.00").unwrap(), "100");
        assert_eq!(normalize_number("1.10").unwrap(), "1.1");
        assert_eq!(normalize_number("3.14").unwrap(), "3.14");
        assert_eq!(normalize_number("-1.50").unwrap(), "-1.5");
    }

    #[test]
    fn test_normalize_number_leading_decimal() {
        assert_eq!(normalize_number(".5").unwrap(), "0.5");
        assert_eq!(normalize_number("-.5").unwrap(), "-0.5");
    }

    #[test]
    fn test_normalize_number_scientific() {
        assert_eq!(normalize_number("1e2").unwrap(), "100");
        assert_eq!(normalize_number("1E2").unwrap(), "100");
        assert_eq!(normalize_number("1.0e2").unwrap(), "100");
        assert_eq!(normalize_number("1.5e2").unwrap(), "150");
        assert_eq!(normalize_number("1.5e-2").unwrap(), "0.015");
        assert_eq!(normalize_number("0.01e4").unwrap(), "100");
        assert_eq!(normalize_number("-1.5e2").unwrap(), "-150");
        assert_eq!(normalize_number("1e0").unwrap(), "1");
    }

    #[test]
    fn test_normalize_number_preserves_precision() {
        // Above f64's 2^53 integer precision but well within finite range —
        // must round-trip digit-for-digit, not via f64.
        assert_eq!(
            normalize_number("99999999999999999999").unwrap(),
            "99999999999999999999"
        );
        assert_eq!(
            normalize_number("12345678901234567890.10").unwrap(),
            "12345678901234567890.1"
        );
    }

    #[test]
    fn test_normalize_number_rejects_non_numeric() {
        assert!(normalize_number("abc").is_err());
        assert!(normalize_number("").is_err());
        assert!(normalize_number("NaN").is_err());
        assert!(normalize_number("inf").is_err());
        assert!(normalize_number("-inf").is_err());
        // Exponent overflows f64 to infinity → rejected.
        assert!(normalize_number("1e1000").is_err());
    }

    #[test]
    fn test_normalize_boolean() {
        for input in ["true", "True", "TRUE", "1", "t", "T", "yes", "YES"] {
            assert_eq!(
                normalize_boolean(input).unwrap(),
                "true",
                "input: {}",
                input
            );
        }
        for input in ["false", "False", "FALSE", "0", "f", "F", "no", "NO"] {
            assert_eq!(
                normalize_boolean(input).unwrap(),
                "false",
                "input: {}",
                input
            );
        }
        assert!(normalize_boolean("maybe").is_err());
        assert!(normalize_boolean("").is_err());
    }

    #[test]
    fn test_quote_literal_number() {
        assert_eq!(quote_literal("42", &SqlType::Number).unwrap(), "42");
        assert_eq!(quote_literal("-100", &SqlType::Number).unwrap(), "-100");
        assert_eq!(quote_literal("3.14", &SqlType::Number).unwrap(), "3.14");
        assert_eq!(quote_literal("-0.5", &SqlType::Number).unwrap(), "-0.5");
        assert!(quote_literal("not_a_number", &SqlType::Number).is_err());
        assert!(quote_literal("NaN", &SqlType::Number).is_err());
        assert!(quote_literal("inf", &SqlType::Number).is_err());
        assert!(quote_literal("-inf", &SqlType::Number).is_err());
    }

    #[test]
    fn test_quote_literal_boolean() {
        assert_eq!(quote_literal("true", &SqlType::Boolean).unwrap(), "TRUE");
        assert_eq!(quote_literal("True", &SqlType::Boolean).unwrap(), "TRUE");
        assert_eq!(quote_literal("1", &SqlType::Boolean).unwrap(), "TRUE");
        assert_eq!(quote_literal("t", &SqlType::Boolean).unwrap(), "TRUE");
        assert_eq!(quote_literal("yes", &SqlType::Boolean).unwrap(), "TRUE");
        assert_eq!(quote_literal("false", &SqlType::Boolean).unwrap(), "FALSE");
        assert_eq!(quote_literal("False", &SqlType::Boolean).unwrap(), "FALSE");
        assert_eq!(quote_literal("0", &SqlType::Boolean).unwrap(), "FALSE");
        assert_eq!(quote_literal("f", &SqlType::Boolean).unwrap(), "FALSE");
        assert_eq!(quote_literal("no", &SqlType::Boolean).unwrap(), "FALSE");
        assert!(quote_literal("maybe", &SqlType::Boolean).is_err());
    }

    #[test]
    fn test_format_value_null_sentinel() {
        let field_with_null = FieldMeta {
            name: "notes".to_string(),
            sql_type: SqlType::Text,
            null: Some("".to_string()),
        };
        // Empty string matches sentinel → NULL
        assert_eq!(format_value("", &field_with_null).unwrap(), "NULL");
        // Non-empty string → normal quoting
        assert_eq!(format_value("hello", &field_with_null).unwrap(), "'hello'");

        let field_no_null = FieldMeta {
            name: "label".to_string(),
            sql_type: SqlType::Text,
            null: None,
        };
        // No sentinel → empty string is quoted normally
        assert_eq!(format_value("", &field_no_null).unwrap(), "''");

        let number_field = FieldMeta {
            name: "count".to_string(),
            sql_type: SqlType::Number,
            null: Some("N/A".to_string()),
        };
        // "N/A" matches sentinel → NULL
        assert_eq!(format_value("N/A", &number_field).unwrap(), "NULL");
        // "42" does not match → normal number
        assert_eq!(format_value("42", &number_field).unwrap(), "42");
    }

    #[test]
    fn test_patch_to_sql_rejects_mismatched_field_hash() {
        let table_config = dummy_table(&[("id", true)]);
        let config = dummy_config(HashMap::from([("test_table".to_string(), table_config)]));

        let mut delta = dummy_delta(&["id"]);
        delta.inserts.push(Entry {
            key: vec!["1".to_string()],
            value: vec![],
        });
        let patch = dummy_patch(
            HashMap::from([("test_table".to_string(), delta)]),
            HashMap::from([("test_table".to_string(), "wrong_hash".to_string())]),
        );

        let err = patch_to_sql(&config, &patch).unwrap_err();
        let msg = format!("{:#}", err);
        assert!(msg.contains("field hash mismatch"), "got: {}", msg);
    }

    #[test]
    fn test_patch_to_sql_rejects_missing_field_hash() {
        let table_config = dummy_table(&[("id", true)]);
        let config = dummy_config(HashMap::from([("test_table".to_string(), table_config)]));

        let mut delta = dummy_delta(&["id"]);
        delta.inserts.push(Entry {
            key: vec!["1".to_string()],
            value: vec![],
        });
        let patch = dummy_patch(
            HashMap::from([("test_table".to_string(), delta)]),
            HashMap::new(),
        );

        let err = patch_to_sql(&config, &patch).unwrap_err();
        let msg = format!("{:#}", err);
        assert!(msg.contains("missing field hash"), "got: {}", msg);
    }

    #[test]
    fn test_patch_to_sql_accepts_matching_field_hash() {
        let table_config = dummy_table(&[("id", true)]);
        let correct_hash = table_config.field_hash();
        let config = dummy_config(HashMap::from([("test_table".to_string(), table_config)]));

        let mut delta = dummy_delta(&["id"]);
        delta.inserts.push(Entry {
            key: vec!["1".to_string()],
            value: vec![],
        });
        let patch = dummy_patch(
            HashMap::from([("test_table".to_string(), delta)]),
            HashMap::from([("test_table".to_string(), correct_hash)]),
        );

        let result = patch_to_sql(&config, &patch).unwrap().unwrap();
        assert!(result.contains("INSERT INTO"));
    }

    #[test]
    fn test_patch_to_sql_rejects_out_of_range_changed_index() {
        // Two-column table: id (PK) + name (subsidiary). An update whose
        // changed_indices points at column 5 must bail rather than panic.
        let table_config = dummy_table(&[("id", true), ("name", false)]);
        let correct_hash = table_config.field_hash();
        let config = dummy_config(HashMap::from([("test_table".to_string(), table_config)]));

        let mut delta = dummy_delta(&["id", "name"]);
        delta.updates.push(crate::update::Update {
            key: vec!["1".to_string()],
            changed_indices: vec![5],
            old_value: vec![],
            new_value: vec!["x".to_string()],
        });
        let patch = dummy_patch(
            HashMap::from([("test_table".to_string(), delta)]),
            HashMap::from([("test_table".to_string(), correct_hash)]),
        );

        let err = patch_to_sql(&config, &patch).unwrap_err();
        let msg = format!("{:#}", err);
        assert!(msg.contains("out of range"), "got: {}", msg);
    }
}
