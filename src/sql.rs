use std::collections::HashSet;

use crate::config;
use crate::proto::patch::patch::Payload;
use crate::proto::patch::Patch;

/// SQL type mapping for converting CSV byte values to SQL literals.
#[derive(Debug, Clone, PartialEq)]
pub enum SqlType {
    Text,
    Integer,
    Float,
    Boolean,
    Binary,
}

impl SqlType {
    pub fn from_config(type_str: &str) -> Self {
        match type_str.to_uppercase().as_str() {
            "INTEGER" | "INT" | "BIGINT" | "SMALLINT" => SqlType::Integer,
            "FLOAT" | "DOUBLE" | "REAL" | "NUMERIC" | "DECIMAL" => SqlType::Float,
            "BOOLEAN" | "BOOL" => SqlType::Boolean,
            "BINARY" | "BYTEA" | "BLOB" => SqlType::Binary,
            _ => SqlType::Text,
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

        let type_map: std::collections::HashMap<&str, &str> = tc
            .fields
            .iter()
            .map(|f| (f.name.as_str(), f.field_type.as_str()))
            .collect();

        let pk = tc.primary_key();
        let field_names = tc.field_names();
        let pk_set: HashSet<&str> = pk.iter().map(|s| s.as_str()).collect();

        let mut fields = Vec::new();
        for name in &pk {
            let type_str = type_map.get(name.as_str()).copied().unwrap_or("TEXT");
            fields.push((name.clone(), SqlType::from_config(type_str)));
        }
        for name in &field_names {
            if !pk_set.contains(name.as_str()) {
                let type_str = type_map.get(name.as_str()).copied().unwrap_or("TEXT");
                fields.push((name.clone(), SqlType::from_config(type_str)));
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
            if s.len() % 2 != 0 {
                return Err(format!("invalid hex: odd length ({})", s.len()).into());
            }
            if !s.bytes().all(|b| b.is_ascii_hexdigit()) {
                return Err("invalid hex: contains non-hex characters".into());
            }
            Ok(format!("'\\x{}'", s))
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
        let lit = quote_literal(val, sql_type)
            .map_err(|e| format!("field '{}': {}", name, e))?;
        literals.push(lit);
    }
    for (val, (name, sql_type)) in value.iter().zip(sub_types) {
        let lit = quote_literal(val, sql_type)
            .map_err(|e| format!("field '{}': {}", name, e))?;
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
                let lit = quote_literal(val, sql_type)
                    .map_err(|e| format!("field '{}': {}", name, e))?;
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
                let lit = quote_literal(val, sql_type)
                    .map_err(|e| format!("field '{}': {}", name, e))?;
                Ok(format!("{} = {}", quote_ident(name), lit))
            })
            .collect::<Result<Vec<_>, Box<dyn std::error::Error>>>()?;

        let where_parts: Vec<String> = update
            .key
            .iter()
            .zip(schema.pk_types())
            .map(|(val, (name, sql_type))| {
                let lit = quote_literal(val, sql_type)
                    .map_err(|e| format!("field '{}': {}", name, e))?;
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
        assert_eq!(SqlType::from_config("INTEGER"), SqlType::Integer);
        assert_eq!(SqlType::from_config("INT"), SqlType::Integer);
        assert_eq!(SqlType::from_config("BIGINT"), SqlType::Integer);
        assert_eq!(SqlType::from_config("SMALLINT"), SqlType::Integer);
        assert_eq!(SqlType::from_config("FLOAT"), SqlType::Float);
        assert_eq!(SqlType::from_config("DOUBLE"), SqlType::Float);
        assert_eq!(SqlType::from_config("REAL"), SqlType::Float);
        assert_eq!(SqlType::from_config("NUMERIC"), SqlType::Float);
        assert_eq!(SqlType::from_config("DECIMAL"), SqlType::Float);
        assert_eq!(SqlType::from_config("BOOLEAN"), SqlType::Boolean);
        assert_eq!(SqlType::from_config("BOOL"), SqlType::Boolean);
        assert_eq!(SqlType::from_config("TEXT"), SqlType::Text);
        assert_eq!(SqlType::from_config("VARCHAR"), SqlType::Text);
        assert_eq!(SqlType::from_config("unknown"), SqlType::Text);
        assert_eq!(SqlType::from_config("BINARY"), SqlType::Binary);
        assert_eq!(SqlType::from_config("BYTEA"), SqlType::Binary);
        assert_eq!(SqlType::from_config("BLOB"), SqlType::Binary);
        // Case insensitive
        assert_eq!(SqlType::from_config("integer"), SqlType::Integer);
        assert_eq!(SqlType::from_config("Boolean"), SqlType::Boolean);
        assert_eq!(SqlType::from_config("bytea"), SqlType::Binary);
    }

    #[test]
    fn test_quote_ident() {
        assert_eq!(quote_ident("simple"), "\"simple\"");
        assert_eq!(quote_ident("has\"quote"), "\"has\"\"quote\"");
        assert_eq!(quote_ident(""), "\"\"");
    }

    #[test]
    fn test_quote_literal_text() {
        assert_eq!(
            quote_literal("hello", &SqlType::Text).unwrap(),
            "'hello'"
        );
        assert_eq!(
            quote_literal("", &SqlType::Text).unwrap(),
            "''"
        );
    }

    #[test]
    fn test_quote_literal_text_with_quotes() {
        assert_eq!(
            quote_literal("it's a test", &SqlType::Text).unwrap(),
            "'it''s a test'"
        );
        assert_eq!(
            quote_literal("a''b", &SqlType::Text).unwrap(),
            "'a''''b'"
        );
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
        assert_eq!(
            quote_literal("", &SqlType::Binary).unwrap(),
            "'\\x'"
        );
        // Odd length
        assert!(quote_literal("ABC", &SqlType::Binary).is_err());
        // Non-hex characters
        assert!(quote_literal("GHIJ", &SqlType::Binary).is_err());
    }
}
