use std::collections::HashMap;

use anyhow::{Context, Result, anyhow, bail};

use crate::config::{Config, FieldConfig};
use crate::proto::cell::Value as ProtoValue;
use crate::proto::delta::Delta as ProtoDelta;
use crate::proto::entry::Entry as ProtoEntry;
use crate::proto::injected::Field as ProtoInjectedField;
use crate::proto::patch::Patch as ProtoPatch;
use crate::proto::table::Table as ProtoTable;
use crate::proto::update::Update as ProtoUpdate;
use crate::value::Value;

/// Controls how a CSV field value is parsed into a `Value`.
///
/// These are not database column types — they determine quoting and
/// validation when embedding a value into a SQL string (e.g. `Text`
/// wraps in single quotes, `Number` emits unquoted).
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

/// Parse a boolean string, accepting any of `true`/`1`/`t`/`yes` (and their
/// false counterparts) case-insensitively.
pub fn parse_boolean(value: &str) -> Result<bool> {
    const TRUE_VALUES: &[&str] = &["true", "1", "t", "yes"];
    const FALSE_VALUES: &[&str] = &["false", "0", "f", "no"];

    if TRUE_VALUES.iter().any(|v| value.eq_ignore_ascii_case(v)) {
        Ok(true)
    } else if FALSE_VALUES.iter().any(|v| value.eq_ignore_ascii_case(v)) {
        Ok(false)
    } else {
        bail!("invalid boolean value: '{}'", value);
    }
}

/// Parse a string into a typed `Value` according to the SQL type tag.
pub fn parse_typed_value(value: &str, sql_type: &SqlType) -> Result<Value> {
    match sql_type {
        SqlType::Text => Ok(Value::Text(value.to_string())),
        SqlType::Number => {
            let parsed: f64 = value
                .parse()
                .with_context(|| format!("invalid number: '{}'", value))?;
            Value::number(parsed)
        }
        SqlType::Boolean => Ok(Value::Boolean(parse_boolean(value)?)),
    }
}

/// Schema information for a single table, resolved from config.
struct TableSchema {
    /// All field names in order: primary keys first, then subsidiary.
    field_names: Vec<String>,
    /// Number of primary key fields (the first `num_primary_keys` entries).
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

        let field_config_by_name: HashMap<&str, &FieldConfig> = table_config
            .fields
            .iter()
            .map(|field| (field.name.as_str(), field))
            .collect();

        let primary_key_names = table_config.primary_key();
        let all_field_names = table_config.field_names();

        let mut field_names = Vec::new();

        // Primary-key fields first.
        for name in &primary_key_names {
            if !field_config_by_name.contains_key(name.as_str()) {
                bail!(
                    "primary key field '{}' not found in table '{}'",
                    name,
                    table_name
                );
            }
            field_names.push(name.clone());
        }

        // Then subsidiary fields, preserving declaration order.
        for name in &all_field_names {
            if !primary_key_names.contains(name) {
                if !field_config_by_name.contains_key(name.as_str()) {
                    bail!("field '{}' not found in table '{}'", name, table_name);
                }
                field_names.push(name.clone());
            }
        }

        Ok(TableSchema {
            num_primary_keys: primary_key_names.len(),
            field_names,
        })
    }

    fn primary_key_names(&self) -> &[String] {
        &self.field_names[..self.num_primary_keys]
    }

    fn subsidiary_names(&self) -> &[String] {
        &self.field_names[self.num_primary_keys..]
    }
}

/// A static field injected into all SQL output (resolved from proto).
struct InjectedField {
    name: String,
    value: Value,
}

impl TryFrom<&ProtoInjectedField> for InjectedField {
    type Error = anyhow::Error;

    fn try_from(proto: &ProtoInjectedField) -> Result<Self> {
        let proto_value = proto
            .value
            .as_ref()
            .with_context(|| format!("injected field '{}': missing value", proto.name))?;
        let value = Value::try_from(proto_value)
            .with_context(|| format!("injected field '{}'", proto.name))?;
        Ok(InjectedField {
            name: proto.name.clone(),
            value,
        })
    }
}

impl InjectedField {
    fn where_clause(&self) -> String {
        format!(
            "{} = {}",
            quote_identifier(&self.name),
            quote_literal(&self.value)
        )
    }

    fn quoted_column(&self) -> String {
        quote_identifier(&self.name)
    }

    fn quoted_value(&self) -> String {
        quote_literal(&self.value)
    }
}

/// Double-quote a SQL identifier, escaping embedded double quotes.
fn quote_identifier(name: &str) -> String {
    format!("\"{}\"", name.replace('"', "\"\""))
}

/// Format a `Value` as a SQL literal.
pub fn quote_literal(value: &Value) -> String {
    match value {
        Value::Null => "NULL".to_string(),
        Value::Text(s) => format!("'{}'", s.replace('\'', "''")),
        Value::Boolean(true) => "TRUE".to_string(),
        Value::Boolean(false) => "FALSE".to_string(),
        Value::Number(n) => n.to_string(),
    }
}

/// Convert key + value proto-value slices into a list of SQL literal strings.
fn format_row(
    key: &[ProtoValue],
    value: &[ProtoValue],
    schema: &TableSchema,
) -> Result<Vec<String>> {
    let primary_keys = schema.primary_key_names();
    let subsidiary_fields = schema.subsidiary_names();

    if key.len() != primary_keys.len() {
        bail!(
            "primary key field count mismatch: got {} values, expected {}",
            key.len(),
            primary_keys.len()
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
    for (proto_value, name) in key.iter().zip(primary_keys) {
        let v = Value::try_from(proto_value).with_context(|| format!("field '{}'", name))?;
        literals.push(quote_literal(&v));
    }
    for (proto_value, name) in value.iter().zip(subsidiary_fields) {
        let v = Value::try_from(proto_value).with_context(|| format!("field '{}'", name))?;
        literals.push(quote_literal(&v));
    }
    Ok(literals)
}

/// Generate DELETE statements for a list of entries.
fn emit_deletes(
    entries: &[ProtoEntry],
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
    entries: &[ProtoEntry],
    schema: &TableSchema,
    injected_fields: &[InjectedField],
    quoted_table: &str,
    out: &mut String,
) -> Result<()> {
    if entries.is_empty() {
        return Ok(());
    }

    let mut column_parts: Vec<String> = schema
        .field_names
        .iter()
        .map(|name| quote_identifier(name))
        .collect();

    let injected_columns: Vec<String> = injected_fields.iter().map(|f| f.quoted_column()).collect();
    column_parts.splice(..0, injected_columns);
    let columns = column_parts.join(", ");

    // Injected values are static across the entire patch, so compute once.
    let injected_values: Vec<String> = injected_fields.iter().map(|f| f.quoted_value()).collect();

    for entry in entries {
        let mut literals = format_row(&entry.key, &entry.value, schema)
            .with_context(|| format!("key {:?}", entry.key))?;
        literals.splice(..0, injected_values.iter().cloned());
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
    update: &ProtoUpdate,
    subsidiary_names: &[String],
    schema: &TableSchema,
    injected_fields: &[InjectedField],
    quoted_table: &str,
) -> Result<String> {
    // Sparse updates list changed column indices explicitly; full
    // updates (empty changed_indices) include all subsidiary columns.
    let indices: Vec<u32> = if update.changed_indices.is_empty() {
        (0..subsidiary_names.len() as u32).collect()
    } else {
        update.changed_indices.clone()
    };

    let mut set_parts = Vec::new();
    for (&index, proto_value) in indices.iter().zip(update.new_value.iter()) {
        let name = subsidiary_names.get(index as usize).ok_or_else(|| {
            anyhow!(
                "changed_indices entry {} is out of range (table has {} subsidiary columns)",
                index,
                subsidiary_names.len()
            )
        })?;
        let value = Value::try_from(proto_value).with_context(|| format!("field '{}'", name))?;
        set_parts.push(format!(
            "{} = {}",
            quote_identifier(name),
            quote_literal(&value)
        ));
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
    updates: &[ProtoUpdate],
    schema: &TableSchema,
    injected_fields: &[InjectedField],
    quoted_table: &str,
    out: &mut String,
) -> Result<()> {
    let subsidiary_names = schema.subsidiary_names();

    for update in updates {
        let stmt = format_update(
            update,
            subsidiary_names,
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
    key: &[ProtoValue],
    schema: &TableSchema,
    injected_fields: &[InjectedField],
) -> Result<String> {
    let mut where_parts = Vec::new();
    for (proto_value, name) in key.iter().zip(schema.primary_key_names()) {
        let value = Value::try_from(proto_value).with_context(|| format!("field '{}'", name))?;
        where_parts.push(format!(
            "{} = {}",
            quote_identifier(name),
            quote_literal(&value)
        ));
    }
    for injected in injected_fields {
        where_parts.push(injected.where_clause());
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
            conditions.push(injected.where_clause());
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
    use crate::value::text_proto_values;

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
        assert_eq!(quote_literal(&"hello".into()), "'hello'");
        assert_eq!(quote_literal(&"".into()), "''");
    }

    #[test]
    fn test_quote_literal_text_with_quotes() {
        assert_eq!(quote_literal(&"it's a test".into()), "'it''s a test'");
        assert_eq!(quote_literal(&"a''b".into()), "'a''''b'");
    }

    #[test]
    fn test_quote_literal_null() {
        assert_eq!(quote_literal(&Value::Null), "NULL");
    }

    #[test]
    fn test_quote_literal_number() {
        assert_eq!(quote_literal(&Value::from(42.0)), "42");
        assert_eq!(quote_literal(&Value::from(-100.0)), "-100");
        assert_eq!(quote_literal(&Value::from(2.5)), "2.5");
        assert_eq!(quote_literal(&Value::from(-0.5)), "-0.5");
    }

    #[test]
    fn test_quote_literal_boolean() {
        assert_eq!(quote_literal(&Value::from(true)), "TRUE");
        assert_eq!(quote_literal(&Value::from(false)), "FALSE");
    }

    #[test]
    fn test_parse_boolean_truthy() {
        for input in ["true", "True", "TRUE", "1", "t", "T", "yes", "YES"] {
            assert!(parse_boolean(input).unwrap(), "input: {}", input);
        }
    }

    #[test]
    fn test_parse_boolean_falsy() {
        for input in ["false", "False", "FALSE", "0", "f", "F", "no", "NO"] {
            assert!(!parse_boolean(input).unwrap(), "input: {}", input);
        }
    }

    #[test]
    fn test_parse_boolean_rejects_invalid() {
        assert!(parse_boolean("maybe").is_err());
        assert!(parse_boolean("").is_err());
    }

    #[test]
    fn test_patch_to_sql_rejects_mismatched_field_hash() {
        let table_config = dummy_table(&[("id", true)]);
        let config = dummy_config(HashMap::from([("test_table".to_string(), table_config)]));

        let mut delta = dummy_delta(&["id"]);
        delta.inserts.push(ProtoEntry {
            key: text_proto_values(&["1"]),
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
        delta.inserts.push(ProtoEntry {
            key: text_proto_values(&["1"]),
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
        delta.inserts.push(ProtoEntry {
            key: text_proto_values(&["1"]),
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
        delta.updates.push(ProtoUpdate {
            key: text_proto_values(&["1"]),
            changed_indices: vec![5],
            old_value: vec![],
            new_value: text_proto_values(&["x"]),
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
