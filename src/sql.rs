use std::collections::HashSet;

use chrono::{DateTime, NaiveDate, NaiveDateTime, NaiveTime};

use crate::config;
use crate::proto::patch::Patch;
use crate::proto::patch::patch::Payload;

/// SQL type mapping for converting CSV byte values to SQL literals.
#[derive(Debug, Clone, PartialEq)]
pub enum SqlType {
    Text,
    Integer,
    Float,
    Boolean,
    Binary,
    Date(String),
    Time(String),
    DateTime(String),
}

impl SqlType {
    pub fn from_config(type_str: &str, format: Option<&str>) -> Result<Self, String> {
        match type_str.to_uppercase().as_str() {
            "TEXT" => Ok(SqlType::Text),
            "INTEGER" => Ok(SqlType::Integer),
            "FLOAT" => Ok(SqlType::Float),
            "BOOLEAN" => Ok(SqlType::Boolean),
            "BINARY" => Ok(SqlType::Binary),
            "DATE" => Ok(SqlType::Date(format.unwrap_or("%Y-%m-%d").to_string())),
            "TIME" => Ok(SqlType::Time(format.unwrap_or("%H:%M:%S").to_string())),
            "DATETIME" => Ok(SqlType::DateTime(
                format.unwrap_or("%Y-%m-%d %H:%M:%S").to_string(),
            )),
            other => Err(format!(
                "unknown field type '{}'; valid types are: TEXT, INTEGER, FLOAT, BOOLEAN, BINARY, DATE, TIME, DATETIME",
                other
            )),
        }
    }
}

/// Schema information for a single table, resolved from config.
struct TableSchema {
    table_name: String,
    /// All fields in order: PK first, then subsidiary. Each with its SQL type.
    fields: Vec<(String, SqlType)>,
    /// Number of primary key fields (the first `num_pk` entries in `fields`).
    num_pk: usize,
}

impl TableSchema {
    fn resolve(table_name: &str) -> Result<Self, Box<dyn std::error::Error>> {
        let config = config::Config::get()?;
        let tc = config
            .tables
            .get(table_name)
            .ok_or_else(|| format!("table '{}' not found in config", table_name))?;

        let type_map: std::collections::HashMap<&str, (&str, Option<&str>)> = tc
            .fields
            .iter()
            .map(|f| {
                (
                    f.name.as_str(),
                    (f.field_type.as_str(), f.format.as_deref()),
                )
            })
            .collect();

        let pk = tc.primary_key();
        let field_names = tc.field_names();
        let pk_set: HashSet<&str> = pk.iter().map(|s| s.as_str()).collect();

        let mut fields = Vec::new();
        for name in &pk {
            let (type_str, fmt) = type_map
                .get(name.as_str())
                .copied()
                .unwrap_or(("TEXT", None));
            let sql_type = SqlType::from_config(type_str, fmt)
                .map_err(|e| format!("field '{}': {}", name, e))?;
            fields.push((name.clone(), sql_type));
        }
        for name in &field_names {
            if !pk_set.contains(name.as_str()) {
                let (type_str, fmt) = type_map
                    .get(name.as_str())
                    .copied()
                    .unwrap_or(("TEXT", None));
                let sql_type = SqlType::from_config(type_str, fmt)
                    .map_err(|e| format!("field '{}': {}", name, e))?;
                fields.push((name.clone(), sql_type));
            }
        }

        Ok(TableSchema {
            table_name: table_name.to_string(),
            num_pk: pk.len(),
            fields,
        })
    }

    fn pk_types(&self) -> &[(String, SqlType)] {
        &self.fields[..self.num_pk]
    }

    fn sub_types(&self) -> &[(String, SqlType)] {
        &self.fields[self.num_pk..]
    }
}

/// Double-quote a SQL identifier, escaping embedded double quotes.
fn quote_ident(name: &str) -> String {
    format!("\"{}\"", name.replace('"', "\"\""))
}

/// Format a value as a SQL literal based on its type.
pub fn quote_literal(s: &str, sql_type: &SqlType) -> Result<String, Box<dyn std::error::Error>> {
    match sql_type {
        SqlType::Text => Ok(format!("'{}'", s.replace('\'', "''"))),
        SqlType::Integer => {
            s.parse::<i64>()?;
            Ok(s.to_string())
        }
        SqlType::Float => {
            s.parse::<f64>()?;
            Ok(s.to_string())
        }
        SqlType::Boolean => match s.to_lowercase().as_str() {
            "true" | "1" | "t" | "yes" => Ok("TRUE".to_string()),
            "false" | "0" | "f" | "no" => Ok("FALSE".to_string()),
            _ => Err(format!("invalid boolean value: '{}'", s).into()),
        },
        SqlType::Binary => {
            if !s.len().is_multiple_of(2) {
                return Err(format!("invalid hex: odd length ({})", s.len()).into());
            }
            if !s.bytes().all(|b| b.is_ascii_hexdigit()) {
                return Err("invalid hex: contains non-hex characters".into());
            }
            Ok(format!("'\\x{}'", s))
        }
        SqlType::Date(fmt) => {
            NaiveDate::parse_from_str(s, fmt)
                .map_err(|e| format!("invalid date '{}' for format '{}': {}", s, fmt, e))?;
            Ok(format!("'{}'", s.replace('\'', "''")))
        }
        SqlType::Time(fmt) => {
            NaiveTime::parse_from_str(s, fmt)
                .map_err(|e| format!("invalid time '{}' for format '{}': {}", s, fmt, e))?;
            Ok(format!("'{}'", s.replace('\'', "''")))
        }
        SqlType::DateTime(fmt) => {
            if NaiveDateTime::parse_from_str(s, fmt).is_ok() {
                return Ok(format!("'{}'", s.replace('\'', "''")));
            }
            if let Ok(epoch) = s.parse::<i64>()
                && DateTime::from_timestamp(epoch, 0).is_some()
            {
                return Ok(format!("'{}'", s.replace('\'', "''")));
            }
            Err(format!(
                "invalid datetime '{}' for format '{}': could not parse as datetime or unix epoch",
                s, fmt
            )
            .into())
        }
    }
}

/// Convert key + value slices into a list of SQL literal strings.
fn format_row(
    key: &[String],
    value: &[String],
    schema: &TableSchema,
) -> Result<Vec<String>, Box<dyn std::error::Error>> {
    let pk_types = schema.pk_types();
    let sub_types = schema.sub_types();

    if key.len() != pk_types.len() {
        return Err(format!(
            "PK field count mismatch: got {} values, expected {}",
            key.len(),
            pk_types.len()
        )
        .into());
    }
    if value.len() != sub_types.len() {
        return Err(format!(
            "subsidiary field count mismatch: got {} values, expected {}",
            value.len(),
            sub_types.len()
        )
        .into());
    }

    let mut literals = Vec::with_capacity(key.len() + value.len());
    for (val, (name, sql_type)) in key.iter().zip(pk_types) {
        let lit = quote_literal(val, sql_type).map_err(|e| format!("field '{}': {}", name, e))?;
        literals.push(lit);
    }
    for (val, (name, sql_type)) in value.iter().zip(sub_types) {
        let lit = quote_literal(val, sql_type).map_err(|e| format!("field '{}': {}", name, e))?;
        literals.push(lit);
    }
    Ok(literals)
}

/// Generate SQL statements for a delta (DELETE/INSERT/UPDATE).
fn delta_to_sql(
    delta: &crate::proto::delta::Delta,
    out: &mut String,
) -> Result<(), Box<dyn std::error::Error>> {
    let schema = TableSchema::resolve(&delta.name)?;
    let table = quote_ident(&schema.table_name);

    // DELETEs
    for entry in &delta.deletes {
        let pk_literals: Vec<String> = entry
            .key
            .iter()
            .zip(schema.pk_types())
            .map(|(val, (name, sql_type))| {
                let lit =
                    quote_literal(val, sql_type).map_err(|e| format!("field '{}': {}", name, e))?;
                Ok(format!("{} = {}", quote_ident(name), lit))
            })
            .collect::<Result<Vec<_>, Box<dyn std::error::Error>>>()?;

        out.push_str(&format!(
            "DELETE FROM {} WHERE {};\n",
            table,
            pk_literals.join(" AND ")
        ));
    }

    // INSERTs
    if !delta.inserts.is_empty() {
        let columns: String = schema
            .fields
            .iter()
            .map(|(name, _)| quote_ident(name))
            .collect::<Vec<_>>()
            .join(", ");

        for entry in &delta.inserts {
            let literals = format_row(&entry.key, &entry.value, &schema)?;
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
        let sub_types = schema.sub_types();
        let set_parts: Vec<String> = update
            .changed_indices
            .iter()
            .zip(update.new_value.iter())
            .map(|(idx, val)| {
                let (name, sql_type) = &sub_types[*idx as usize];
                let lit =
                    quote_literal(val, sql_type).map_err(|e| format!("field '{}': {}", name, e))?;
                Ok(format!("{} = {}", quote_ident(name), lit))
            })
            .collect::<Result<Vec<_>, Box<dyn std::error::Error>>>()?;

        let where_parts: Vec<String> = update
            .key
            .iter()
            .zip(schema.pk_types())
            .map(|(val, (name, sql_type))| {
                let lit =
                    quote_literal(val, sql_type).map_err(|e| format!("field '{}': {}", name, e))?;
                Ok(format!("{} = {}", quote_ident(name), lit))
            })
            .collect::<Result<Vec<_>, Box<dyn std::error::Error>>>()?;

        out.push_str(&format!(
            "UPDATE {} SET {} WHERE {};\n",
            table,
            set_parts.join(", "),
            where_parts.join(" AND ")
        ));
    }

    Ok(())
}

/// Generate SQL statements for a full state (TRUNCATE + INSERT per table).
fn state_to_sql(
    state: &crate::proto::state::State,
    out: &mut String,
) -> Result<(), Box<dyn std::error::Error>> {
    for (table_name, table) in &state.tables {
        let schema = TableSchema::resolve(table_name)?;
        let quoted_table = quote_ident(table_name);

        out.push_str(&format!("TRUNCATE {};\n", quoted_table));

        if !table.entries.is_empty() {
            let columns: String = schema
                .fields
                .iter()
                .map(|(name, _)| quote_ident(name))
                .collect::<Vec<_>>()
                .join(", ");

            for entry in &table.entries {
                let literals = format_row(&entry.key, &entry.value, &schema)?;
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
pub fn patch_to_sql(patch: &Patch) -> Result<Option<String>, Box<dyn std::error::Error>> {
    log::info!("Converting patch to SQL: {}", patch);

    match &patch.payload {
        Some(Payload::Deltas(deltas)) => {
            log::info!("Converting {} deltas to SQL", deltas.items.len());
            let mut sql = String::from("BEGIN;\n");
            for delta in &deltas.items {
                delta_to_sql(delta, &mut sql)?;
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
            state_to_sql(state, &mut sql)?;
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
        // Canonical types
        assert_eq!(SqlType::from_config("TEXT", None).unwrap(), SqlType::Text);
        assert_eq!(
            SqlType::from_config("INTEGER", None).unwrap(),
            SqlType::Integer
        );
        assert_eq!(SqlType::from_config("FLOAT", None).unwrap(), SqlType::Float);
        assert_eq!(
            SqlType::from_config("BOOLEAN", None).unwrap(),
            SqlType::Boolean
        );
        assert_eq!(
            SqlType::from_config("BINARY", None).unwrap(),
            SqlType::Binary
        );
        // Case insensitive
        assert_eq!(
            SqlType::from_config("integer", None).unwrap(),
            SqlType::Integer
        );
        assert_eq!(
            SqlType::from_config("Boolean", None).unwrap(),
            SqlType::Boolean
        );
        assert_eq!(
            SqlType::from_config("binary", None).unwrap(),
            SqlType::Binary
        );
        // Date/time types with defaults
        assert_eq!(
            SqlType::from_config("DATE", None).unwrap(),
            SqlType::Date("%Y-%m-%d".to_string())
        );
        assert_eq!(
            SqlType::from_config("TIME", None).unwrap(),
            SqlType::Time("%H:%M:%S".to_string())
        );
        assert_eq!(
            SqlType::from_config("DATETIME", None).unwrap(),
            SqlType::DateTime("%Y-%m-%d %H:%M:%S".to_string())
        );
        // Case insensitive date/time
        assert_eq!(
            SqlType::from_config("date", None).unwrap(),
            SqlType::Date("%Y-%m-%d".to_string())
        );
        assert_eq!(
            SqlType::from_config("datetime", None).unwrap(),
            SqlType::DateTime("%Y-%m-%d %H:%M:%S".to_string())
        );
        // Custom format
        assert_eq!(
            SqlType::from_config("DATE", Some("%d/%m/%Y")).unwrap(),
            SqlType::Date("%d/%m/%Y".to_string())
        );
        assert_eq!(
            SqlType::from_config("TIME", Some("%H:%M")).unwrap(),
            SqlType::Time("%H:%M".to_string())
        );
        assert_eq!(
            SqlType::from_config("DATETIME", Some("%Y-%m-%dT%H:%M:%S")).unwrap(),
            SqlType::DateTime("%Y-%m-%dT%H:%M:%S".to_string())
        );
        // Unknown types are rejected
        assert!(SqlType::from_config("VARCHAR", None).is_err());
        assert!(SqlType::from_config("INT", None).is_err());
        assert!(SqlType::from_config("BLOB", None).is_err());
        assert!(SqlType::from_config("unknown", None).is_err());
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
    fn test_quote_literal_integer() {
        assert_eq!(quote_literal("42", &SqlType::Integer).unwrap(), "42");
        assert_eq!(quote_literal("-100", &SqlType::Integer).unwrap(), "-100");
        assert!(quote_literal("not_a_number", &SqlType::Integer).is_err());
    }

    #[test]
    fn test_quote_literal_float() {
        assert_eq!(quote_literal("3.14", &SqlType::Float).unwrap(), "3.14");
        assert_eq!(quote_literal("-0.5", &SqlType::Float).unwrap(), "-0.5");
        assert!(quote_literal("not_a_float", &SqlType::Float).is_err());
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
    fn test_quote_literal_binary() {
        assert_eq!(
            quote_literal("48656C6C6F", &SqlType::Binary).unwrap(),
            "'\\x48656C6C6F'"
        );
        assert_eq!(
            quote_literal("DEADBEEF", &SqlType::Binary).unwrap(),
            "'\\xDEADBEEF'"
        );
        assert_eq!(
            quote_literal("deadbeef", &SqlType::Binary).unwrap(),
            "'\\xdeadbeef'"
        );
        // Empty is valid
        assert_eq!(quote_literal("", &SqlType::Binary).unwrap(), "'\\x'");
        // Odd length
        assert!(quote_literal("ABC", &SqlType::Binary).is_err());
        // Non-hex characters
        assert!(quote_literal("GHIJ", &SqlType::Binary).is_err());
    }

    #[test]
    fn test_quote_literal_date() {
        let ty = SqlType::Date("%Y-%m-%d".to_string());
        assert_eq!(quote_literal("2024-01-15", &ty).unwrap(), "'2024-01-15'");
        assert_eq!(quote_literal("1970-01-01", &ty).unwrap(), "'1970-01-01'");
        assert!(quote_literal("not-a-date", &ty).is_err());
        assert!(quote_literal("2024-13-01", &ty).is_err());
        assert!(quote_literal("15/01/2024", &ty).is_err());
        // Custom format
        let ty_custom = SqlType::Date("%d/%m/%Y".to_string());
        assert_eq!(
            quote_literal("15/01/2024", &ty_custom).unwrap(),
            "'15/01/2024'"
        );
        assert!(quote_literal("2024-01-15", &ty_custom).is_err());
    }

    #[test]
    fn test_quote_literal_time() {
        let ty = SqlType::Time("%H:%M:%S".to_string());
        assert_eq!(quote_literal("10:30:00", &ty).unwrap(), "'10:30:00'");
        assert_eq!(quote_literal("23:59:59", &ty).unwrap(), "'23:59:59'");
        assert!(quote_literal("not-a-time", &ty).is_err());
        assert!(quote_literal("25:00:00", &ty).is_err());
        // Custom format
        let ty_custom = SqlType::Time("%H:%M".to_string());
        assert_eq!(quote_literal("10:30", &ty_custom).unwrap(), "'10:30'");
        assert!(quote_literal("10:30:00", &ty_custom).is_err());
    }

    #[test]
    fn test_quote_literal_datetime() {
        let ty = SqlType::DateTime("%Y-%m-%d %H:%M:%S".to_string());
        assert_eq!(
            quote_literal("2024-01-15 10:30:00", &ty).unwrap(),
            "'2024-01-15 10:30:00'"
        );
        // Unix epoch
        assert_eq!(quote_literal("1705312200", &ty).unwrap(), "'1705312200'");
        assert_eq!(quote_literal("0", &ty).unwrap(), "'0'");
        // Invalid
        assert!(quote_literal("not-a-datetime", &ty).is_err());
        assert!(quote_literal("2024-13-01 10:30:00", &ty).is_err());
        // Custom format
        let ty_custom = SqlType::DateTime("%Y-%m-%dT%H:%M:%S".to_string());
        assert_eq!(
            quote_literal("2024-01-15T10:30:00", &ty_custom).unwrap(),
            "'2024-01-15T10:30:00'"
        );
        assert!(quote_literal("2024-01-15 10:30:00", &ty_custom).is_err());
    }
}
