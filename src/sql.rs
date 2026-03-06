use anyhow::{Context, Result, bail};

use crate::config::Config;
use crate::proto::patch::Patch;
use crate::proto::patch::patch::Payload;

/// SQL type mapping for converting CSV byte values to SQL literals.
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
    /// If set, this CSV value is emitted as SQL `NULL` instead of a typed literal.
    null: Option<String>,
}

/// Schema information for a single table, resolved from config.
struct TableSchema {
    table_name: String,
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
    /// Fields that appear in the config get their declared type and null
    /// sentinel; fields referenced only by the primary key (not explicitly
    /// configured) default to `TEXT` with no null sentinel.
    fn resolve(config: &Config, table_name: &str) -> Result<Self> {
        let table_config = config
            .tables
            .get(table_name)
            .with_context(|| format!("table '{}' not found in config", table_name))?;

        // Build a name→config lookup so we can resolve type/null for each field.
        let field_configs: std::collections::HashMap<&str, &crate::config::FieldConfig> =
            table_config
                .fields
                .iter()
                .map(|field| (field.name.as_str(), field))
                .collect();

        let primary_key = table_config.primary_key();
        let field_names = table_config.field_names();

        // First pass: add primary key fields in their declared order.
        let mut fields = Vec::new();
        for name in &primary_key {
            let field_config = field_configs.get(name.as_str());
            let type_str = match field_config {
                Some(field_config) => field_config.sql_type.as_str(),
                None => "TEXT",
            };
            let null = match field_config {
                Some(field_config) => field_config.null.clone(),
                None => None,
            };
            let sql_type =
                SqlType::from_config(type_str).with_context(|| format!("field '{}'", name))?;
            fields.push(FieldMeta {
                name: name.clone(),
                sql_type,
                null,
            });
        }
        // Second pass: append subsidiary (non-PK) fields in their declared order.
        for name in &field_names {
            if !primary_key.contains(name) {
                let field_config = field_configs.get(name.as_str());
                let type_str = match field_config {
                    Some(field_config) => field_config.sql_type.as_str(),
                    None => "TEXT",
                };
                let null = match field_config {
                    Some(field_config) => field_config.null.clone(),
                    None => None,
                };
                let sql_type =
                    SqlType::from_config(type_str).with_context(|| format!("field '{}'", name))?;
                fields.push(FieldMeta {
                    name: name.clone(),
                    sql_type,
                    null,
                });
            }
        }

        Ok(TableSchema {
            table_name: table_name.to_string(),
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
            "PK field count mismatch: got {} values, expected {}",
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

/// Generate SQL statements for a delta (DELETE/INSERT/UPDATE).
fn delta_to_sql(
    config: &Config,
    delta: &crate::proto::delta::Delta,
    injected_fields: &[InjectedField],
    out: &mut String,
) -> Result<()> {
    let schema = TableSchema::resolve(config, &delta.table_name)?;
    let table = quote_ident(&schema.table_name);

    // DELETEs
    for entry in &delta.deletes {
        let mut where_parts: Vec<String> = entry
            .key
            .iter()
            .zip(schema.primary_key_fields())
            .map(|(value, field)| {
                let literal = format_value(value, field)
                    .with_context(|| format!("field '{}'", field.name))?;
                Ok(format!("{} = {}", quote_ident(&field.name), literal))
            })
            .collect::<Result<Vec<_>>>()?;

        for injected in injected_fields {
            where_parts.push(injected.where_clause()?);
        }

        out.push_str(&format!(
            "DELETE FROM {} WHERE {};\n",
            table,
            where_parts.join(" AND ")
        ));
    }

    // INSERTs
    if !delta.inserts.is_empty() {
        let mut column_parts: Vec<String> = schema
            .fields
            .iter()
            .map(|field| quote_ident(&field.name))
            .collect();

        for (index, injected) in injected_fields.iter().enumerate() {
            column_parts.insert(index, injected.quoted_column());
        }
        let columns = column_parts.join(", ");

        for entry in &delta.inserts {
            let mut literals = format_row(&entry.key, &entry.value, &schema)?;
            for (index, injected) in injected_fields.iter().enumerate() {
                literals.insert(index, injected.quoted_value()?);
            }
            out.push_str(&format!(
                "INSERT INTO {} ({}) VALUES ({});\n",
                table,
                columns,
                literals.join(", ")
            ));
        }
    }

    // UPDATEs
    for update in &delta.updates {
        let subsidiary_fields = schema.subsidiary_fields();
        let set_parts: Vec<String> = update
            .changed_indices
            .iter()
            .zip(update.new_value.iter())
            .map(|(index, value)| {
                let field = &subsidiary_fields[*index as usize];
                let literal = format_value(value, field)
                    .with_context(|| format!("field '{}'", field.name))?;
                Ok(format!("{} = {}", quote_ident(&field.name), literal))
            })
            .collect::<Result<Vec<_>>>()?;

        let mut where_parts: Vec<String> = update
            .key
            .iter()
            .zip(schema.primary_key_fields())
            .map(|(value, field)| {
                let literal = format_value(value, field)
                    .with_context(|| format!("field '{}'", field.name))?;
                Ok(format!("{} = {}", quote_ident(&field.name), literal))
            })
            .collect::<Result<Vec<_>>>()?;

        for injected in injected_fields {
            where_parts.push(injected.where_clause()?);
        }

        out.push_str(&format!(
            "UPDATE {} SET {} WHERE {};\n",
            table,
            set_parts.join(", "),
            where_parts.join(" AND ")
        ));
    }

    Ok(())
}

/// Generate SQL statements for a full state (TRUNCATE/DELETE + INSERT per table).
fn state_to_sql(
    config: &Config,
    state: &crate::proto::state::State,
    injected_fields: &[InjectedField],
    out: &mut String,
) -> Result<()> {
    for (table_name, table) in &state.tables {
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

        if !table.entries.is_empty() {
            let mut column_parts: Vec<String> = schema
                .fields
                .iter()
                .map(|field| quote_ident(&field.name))
                .collect();

            for (index, injected) in injected_fields.iter().enumerate() {
                column_parts.insert(index, injected.quoted_column());
            }
            let columns = column_parts.join(", ");

            for entry in &table.entries {
                let mut literals = format_row(&entry.key, &entry.value, &schema)?;
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
        }
    }

    Ok(())
}

/// Convert a decoded patch to SQL statements.
///
/// Returns a SQL string wrapped in BEGIN/COMMIT.
pub fn patch_to_sql(config: &Config, patch: &Patch) -> Result<Option<String>> {
    log::info!("Converting patch to SQL: {}", patch);

    let injected_fields: Vec<InjectedField> = patch
        .injected_fields
        .iter()
        .map(InjectedField::resolve)
        .collect::<Result<Vec<_>>>()?;

    match &patch.payload {
        Some(Payload::Deltas(deltas)) => {
            log::info!("Converting {} deltas to SQL", deltas.items.len());
            let mut sql = String::from("BEGIN;\n");
            for delta in &deltas.items {
                delta_to_sql(config, delta, &injected_fields, &mut sql)?;
            }
            sql.push_str("COMMIT;\n");
            Ok(Some(sql))
        }
        Some(Payload::State(state)) => {
            log::info!(
                "Converting full state ({} tables) to SQL",
                state.tables.len()
            );
            let mut sql = String::from("BEGIN;\n");
            state_to_sql(config, state, &injected_fields, &mut sql)?;
            sql.push_str("COMMIT;\n");
            Ok(Some(sql))
        }
        None => {
            log::info!("Patch has no payload, nothing to convert");
            Ok(None)
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

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
}
