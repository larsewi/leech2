use std::collections::HashMap;

use anyhow::{Context, Result, bail};

use crate::config::{Config, FieldConfig};
use crate::proto::patch::Patch;

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
    /// Resolve a table's schema from config, producing an ordered field list.
    ///
    /// The config stores fields in an unordered flat list. This function
    /// partitions them into primary-key fields followed by subsidiary fields
    /// so that callers can split the `fields` vec at `num_primary_keys` (see
    /// `primary_key_fields()` and `subsidiary_fields()`).
    ///
    fn resolve(config: &Config, table_name: &str) -> Result<Self> {
        let table_config = config
            .tables
            .get(table_name)
            .with_context(|| format!("table '{}' not found in config", table_name))?;

        // Build a name→config lookup so we can resolve type/null for each field.
        let field_configs: HashMap<&str, &FieldConfig> = table_config
            .fields
            .iter()
            .map(|field| (field.name.as_str(), field))
            .collect();

        let primary_key = table_config.primary_key();
        let field_names = table_config.field_names();

        let mut fields = Vec::new();
        for name in &primary_key {
            fields.push(field_configs[name.as_str()].try_into()?);
        }
        for name in &field_names {
            if !primary_key.contains(name) {
                fields.push(field_configs[name.as_str()].try_into()?);
            }
        }

        Ok(TableSchema {
            num_primary_keys: primary_key.len(),
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

impl InjectedField {
    fn resolve(proto: &crate::proto::injected::Field) -> Result<Self> {
        let sql_type = SqlType::from_config(&proto.sql_type)
            .with_context(|| format!("injected field '{}'", proto.name))?;
        Ok(InjectedField {
            name: proto.name.clone(),
            sql_type,
            value: proto.value.clone(),
        })
    }

    fn where_clause(&self) -> Result<String> {
        let literal = quote_literal(&self.value, &self.sql_type)
            .with_context(|| format!("injected field '{}' value", self.name))?;
        Ok(format!("{} = {}", quote_ident(&self.name), literal))
    }

    fn quoted_column(&self) -> String {
        quote_ident(&self.name)
    }

    fn quoted_value(&self) -> Result<String> {
        quote_literal(&self.value, &self.sql_type)
            .with_context(|| format!("injected field '{}' value", self.name))
    }
}

/// Double-quote a SQL identifier, escaping embedded double quotes.
fn quote_ident(name: &str) -> String {
    format!("\"{}\"", name.replace('"', "\"\""))
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

/// Generate INSERT statements for a list of entries.
fn emit_inserts(
    entries: &[crate::entry::Entry],
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
        .map(|field| quote_ident(&field.name))
        .collect();

    for (index, injected) in injected_fields.iter().enumerate() {
        column_parts.insert(index, injected.quoted_column());
    }
    let columns = column_parts.join(", ");

    for entry in entries {
        let mut literals = format_row(&entry.key, &entry.value, schema)?;
        for (index, injected) in injected_fields.iter().enumerate() {
            literals.insert(index, injected.quoted_value()?);
        }
        out.push_str(&format!(
            "INSERT INTO {} ({}) VALUES ({});\n",
            quoted_table,
            columns,
            literals.join(", ")
        ));
    }

    Ok(())
}

/// Build a WHERE clause from primary key values and injected fields.
fn primary_key_where_clause(
    key: &[String],
    schema: &TableSchema,
    injected_fields: &[InjectedField],
) -> Result<String> {
    let mut where_parts: Vec<String> = key
        .iter()
        .zip(schema.primary_key_fields())
        .map(|(value, field)| {
            let literal =
                format_value(value, field).with_context(|| format!("field '{}'", field.name))?;
            Ok(format!("{} = {}", quote_ident(&field.name), literal))
        })
        .collect::<Result<Vec<_>>>()?;

    for injected in injected_fields {
        where_parts.push(injected.where_clause()?);
    }

    Ok(where_parts.join(" AND "))
}

/// Generate SQL statements for a delta (DELETE/INSERT/UPDATE).
fn delta_to_sql(
    config: &Config,
    table_name: &str,
    delta: &crate::proto::delta::Delta,
    injected_fields: &[InjectedField],
    out: &mut String,
) -> Result<()> {
    let schema = TableSchema::resolve(config, table_name)?;
    let table = quote_ident(table_name);

    // DELETEs
    for entry in &delta.deletes {
        let where_clause = primary_key_where_clause(&entry.key, &schema, injected_fields)?;
        out.push_str(&format!("DELETE FROM {} WHERE {};\n", table, where_clause));
    }

    // INSERTs
    emit_inserts(&delta.inserts, &schema, injected_fields, &table, out)?;

    // UPDATEs
    for update in &delta.updates {
        let subsidiary_fields = schema.subsidiary_fields();
        // Sparse updates list changed column indices explicitly; full
        // updates (empty changed_indices) include all subsidiary columns.
        let indices: Vec<u32> = if update.changed_indices.is_empty() {
            (0..subsidiary_fields.len() as u32).collect()
        } else {
            update.changed_indices.clone()
        };

        let set_parts: Vec<String> = indices
            .iter()
            .zip(update.new_value.iter())
            .map(|(index, value)| {
                let field = &subsidiary_fields[*index as usize];
                let literal = format_value(value, field)
                    .with_context(|| format!("field '{}'", field.name))?;
                Ok(format!("{} = {}", quote_ident(&field.name), literal))
            })
            .collect::<Result<Vec<_>>>()?;

        let where_clause = primary_key_where_clause(&update.key, &schema, injected_fields)?;

        out.push_str(&format!(
            "UPDATE {} SET {} WHERE {};\n",
            table,
            set_parts.join(", "),
            where_clause
        ));
    }

    Ok(())
}

/// Generate SQL statements for a single table's full state (TRUNCATE/DELETE + INSERT).
fn state_table_to_sql(
    config: &Config,
    table_name: &str,
    table: &crate::proto::table::Table,
    injected_fields: &[InjectedField],
    out: &mut String,
) -> Result<()> {
    let schema = TableSchema::resolve(config, table_name)?;
    let quoted_table = quote_ident(table_name);

    if injected_fields.is_empty() {
        out.push_str(&format!("TRUNCATE {};\n", quoted_table));
    } else {
        let conditions: Vec<String> = injected_fields
            .iter()
            .map(|injected| injected.where_clause())
            .collect::<Result<Vec<_>>>()?;
        out.push_str(&format!(
            "DELETE FROM {} WHERE {};\n",
            quoted_table,
            conditions.join(" AND ")
        ));
    }

    emit_inserts(&table.entries, &schema, injected_fields, &quoted_table, out)?;

    Ok(())
}

/// Check whether a table's field hash in the patch matches the hub's config.
/// Returns true if the hash matches, false (with a warning) if it doesn't.
fn check_field_hash(
    config: &Config,
    table_name: &str,
    field_hashes: &HashMap<String, String>,
) -> bool {
    let Some(agent_hash) = field_hashes.get(table_name) else {
        log::warn!(
            "Table '{}': missing field hash in patch, skipping",
            table_name
        );
        return false;
    };
    let Some(table_config) = config.tables.get(table_name) else {
        log::warn!("Table '{}': not found in config, skipping", table_name);
        return false;
    };
    let hub_hash = table_config.field_hash();
    if agent_hash != &hub_hash {
        log::warn!(
            "Table '{}': field hash mismatch (agent={}, hub={}), skipping",
            table_name,
            agent_hash,
            hub_hash
        );
        return false;
    }
    true
}

/// Convert a decoded patch to SQL statements.
///
/// Returns a SQL string wrapped in BEGIN/COMMIT.
pub fn patch_to_sql(config: &Config, patch: &Patch) -> Result<Option<String>> {
    log::info!("Converting patch to SQL: {}", patch);

    if patch.deltas.is_empty() && patch.states.is_empty() {
        log::info!("Patch has no payload, nothing to convert");
        return Ok(None);
    }

    let injected_fields: Vec<InjectedField> = patch
        .injected_fields
        .iter()
        .map(InjectedField::resolve)
        .collect::<Result<Vec<_>>>()?;

    let mut sql = String::from("BEGIN;\n");

    for (table_name, delta) in &patch.deltas {
        if !check_field_hash(config, table_name, &patch.field_hashes) {
            continue;
        }
        delta_to_sql(config, table_name, delta, &injected_fields, &mut sql)?;
    }

    for (table_name, table) in &patch.states {
        if !check_field_hash(config, table_name, &patch.field_hashes) {
            continue;
        }
        state_table_to_sql(config, table_name, table, &injected_fields, &mut sql)?;
    }

    sql.push_str("COMMIT;\n");
    Ok(Some(sql))
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::config::TruncateConfig;

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
        assert!(SqlType::from_config("VARCHAR").is_err());
        assert!(SqlType::from_config("INTEGER").is_err());
        assert!(SqlType::from_config("FLOAT").is_err());
        assert!(SqlType::from_config("BINARY").is_err());
        assert!(SqlType::from_config("DATE").is_err());
        assert!(SqlType::from_config("unknown").is_err());
    }

    #[test]
    fn test_quote_ident() {
        assert_eq!(quote_ident("simple"), "\"simple\"");
        assert_eq!(quote_ident("has\"quote"), "\"has\"\"quote\"");
        assert_eq!(quote_ident(""), "\"\"");
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
    fn test_patch_to_sql_skips_mismatched_field_hash() {
        let table_config = crate::config::TableConfig {
            source: "test.csv".to_string(),
            header: false,
            fields: vec![FieldConfig {
                name: "id".to_string(),
                sql_type: "TEXT".to_string(),
                primary_key: true,
                null: None,
            }],
        };

        let config = Config {
            work_dir: std::path::PathBuf::from("/tmp"),
            injected_fields: Vec::new(),
            compression: crate::config::CompressionConfig::default(),
            tables: HashMap::from([("test_table".to_string(), table_config)]),
            truncate: TruncateConfig::default(),
            filters: crate::config::FilterConfig::default(),
        };

        let patch = Patch {
            head: "abc123".to_string(),
            created: None,
            injected_fields: Vec::new(),
            num_blocks: 1,
            deltas: HashMap::from([(
                "test_table".to_string(),
                crate::proto::delta::Delta {
                    column_names: vec!["id".to_string()],
                    inserts: vec![crate::proto::entry::Entry {
                        key: vec!["1".to_string()],
                        value: vec![],
                    }],
                    deletes: vec![],
                    updates: vec![],
                },
            )]),
            states: HashMap::new(),
            field_hashes: HashMap::from([("test_table".to_string(), "wrong_hash".to_string())]),
        };

        // Should succeed but produce no statements for the mismatched table
        let result = patch_to_sql(&config, &patch).unwrap().unwrap();
        assert!(!result.contains("INSERT INTO"));
        assert!(result.contains("BEGIN;"));
        assert!(result.contains("COMMIT;"));
    }

    #[test]
    fn test_patch_to_sql_skips_missing_field_hash() {
        let table_config = crate::config::TableConfig {
            source: "test.csv".to_string(),
            header: false,
            fields: vec![FieldConfig {
                name: "id".to_string(),
                sql_type: "TEXT".to_string(),
                primary_key: true,
                null: None,
            }],
        };

        let config = Config {
            work_dir: std::path::PathBuf::from("/tmp"),
            injected_fields: Vec::new(),
            compression: crate::config::CompressionConfig::default(),
            tables: HashMap::from([("test_table".to_string(), table_config)]),
            truncate: TruncateConfig::default(),
            filters: crate::config::FilterConfig::default(),
        };

        let patch = Patch {
            head: "abc123".to_string(),
            created: None,
            injected_fields: Vec::new(),
            num_blocks: 1,
            deltas: HashMap::from([(
                "test_table".to_string(),
                crate::proto::delta::Delta {
                    column_names: vec!["id".to_string()],
                    inserts: vec![crate::proto::entry::Entry {
                        key: vec!["1".to_string()],
                        value: vec![],
                    }],
                    deletes: vec![],
                    updates: vec![],
                },
            )]),
            states: HashMap::new(),
            field_hashes: HashMap::new(),
        };

        let result = patch_to_sql(&config, &patch).unwrap().unwrap();
        assert!(!result.contains("INSERT INTO"));
    }

    #[test]
    fn test_patch_to_sql_accepts_matching_field_hash() {
        let table_config = crate::config::TableConfig {
            source: "test.csv".to_string(),
            header: false,
            fields: vec![FieldConfig {
                name: "id".to_string(),
                sql_type: "TEXT".to_string(),
                primary_key: true,
                null: None,
            }],
        };
        let correct_hash = table_config.field_hash();

        let config = Config {
            work_dir: std::path::PathBuf::from("/tmp"),
            injected_fields: Vec::new(),
            compression: crate::config::CompressionConfig::default(),
            tables: HashMap::from([("test_table".to_string(), table_config)]),
            truncate: TruncateConfig::default(),
            filters: crate::config::FilterConfig::default(),
        };

        let patch = Patch {
            head: "abc123".to_string(),
            created: None,
            injected_fields: Vec::new(),
            num_blocks: 1,
            deltas: HashMap::from([(
                "test_table".to_string(),
                crate::proto::delta::Delta {
                    column_names: vec!["id".to_string()],
                    inserts: vec![crate::proto::entry::Entry {
                        key: vec!["1".to_string()],
                        value: vec![],
                    }],
                    deletes: vec![],
                    updates: vec![],
                },
            )]),
            states: HashMap::new(),
            field_hashes: HashMap::from([("test_table".to_string(), correct_hash)]),
        };

        let result = patch_to_sql(&config, &patch).unwrap().unwrap();
        assert!(result.contains("INSERT INTO"));
    }
}
