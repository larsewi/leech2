use std::collections::{HashMap, HashSet};

use anyhow::{Context, Result, anyhow, bail};

use crate::config::{Config, FieldConfig};
use crate::proto::cell::Value as ProtoValue;
use crate::proto::delta::Delta as ProtoDelta;
use crate::proto::entry::Entry as ProtoEntry;
use crate::proto::injected::Field as ProtoInjectedField;
use crate::proto::patch::Patch as ProtoPatch;
use crate::proto::table::Table as ProtoTable;
use crate::proto::update::Update as ProtoUpdate;
use crate::value::{Value, ValueKind};

/// Schema information for a single table, derived from the wire-declared
/// field list. Column ordering follows the wire (i.e. the agent's
/// declaration order): primary-key columns first, then subsidiary columns.
/// The hub honors that order when generating SQL so values land in the
/// columns the agent intended, regardless of how the hub config declares
/// them.
struct TableSchema<'a> {
    /// Field names in wire order: primary keys first, then subsidiary.
    field_names: &'a [String],
    /// Number of primary key fields (the first `num_primary_keys` entries).
    num_primary_keys: usize,
    /// Hub-config field metadata keyed by field name. Used at SQL-rendering
    /// time to validate that each wire `Value`'s variant agrees with the
    /// hub's declared type and that nulls only appear in nullable columns.
    field_configs: HashMap<&'a str, &'a FieldConfig>,
}

impl<'a> TableSchema<'a> {
    /// Resolve a table's schema from a wire-declared field list (e.g.
    /// `Delta.fields` or `Table.fields`), validating that the wire's view
    /// of the schema agrees with the hub config.
    ///
    /// - Field count must match — a wire that omits a column would
    ///   silently leave it at the DB's default value.
    /// - Every wire name must be declared in the hub config — otherwise an
    ///   agent could target columns the operator never authorized leech2
    ///   to write to.
    /// - The wire's primary-key prefix must equal the hub's primary-key
    ///   set — otherwise an agent could choose which column scopes the
    ///   WHERE clause on UPDATE/DELETE, allowing arbitrary-row targeting.
    ///
    /// Type and nullability drift is caught later, per cell, by
    /// [`check_value_matches_field`].
    fn resolve(wire_fields: &'a [String], config: &'a Config, table_name: &str) -> Result<Self> {
        let table_config = config
            .tables
            .get(table_name)
            .with_context(|| format!("table '{}' not found in config", table_name))?;

        let field_configs: HashMap<&str, &FieldConfig> = table_config
            .fields
            .iter()
            .map(|field| (field.name.as_str(), field))
            .collect();
        let hub_pk_set: HashSet<&str> = table_config
            .fields
            .iter()
            .filter(|field| field.primary_key)
            .map(|field| field.name.as_str())
            .collect();

        if wire_fields.len() != table_config.fields.len() {
            bail!(
                "wire field count {} disagrees with hub config field count {} for table '{}'",
                wire_fields.len(),
                table_config.fields.len(),
                table_name
            );
        }

        for name in wire_fields {
            if !field_configs.contains_key(name.as_str()) {
                bail!(
                    "wire field '{}' is not declared in hub config for table '{}'",
                    name,
                    table_name
                );
            }
        }

        let num_primary_keys = hub_pk_set.len();
        let wire_pk_prefix: HashSet<&str> = wire_fields
            .iter()
            .take(num_primary_keys)
            .map(String::as_str)
            .collect();
        if wire_pk_prefix != hub_pk_set {
            bail!(
                "wire primary-key prefix {:?} disagrees with hub primary-key set {:?} for table '{}'",
                wire_pk_prefix,
                hub_pk_set,
                table_name
            );
        }

        Ok(TableSchema {
            field_names: wire_fields,
            num_primary_keys,
            field_configs,
        })
    }

    fn primary_key_names(&self) -> &[String] {
        &self.field_names[..self.num_primary_keys]
    }

    fn subsidiary_names(&self) -> &[String] {
        &self.field_names[self.num_primary_keys..]
    }

    /// Look up the hub `FieldConfig` for a wire field name. The wire-field
    /// validation in `resolve` guarantees every name in `field_names` has
    /// a hub config entry, so a missing entry here is an internal bug.
    fn field_config(&self, name: &str) -> Result<&FieldConfig> {
        self.field_configs
            .get(name)
            .copied()
            .with_context(|| format!("internal error: no hub field config for '{}'", name))
    }
}

/// Validate that a wire `Value`'s variant agrees with the field's declared
/// type, and that `Null` only appears in nullable fields.
fn check_value_matches_field(value: &Value, field: &FieldConfig) -> Result<()> {
    if value.kind() == ValueKind::Null {
        if field.null_sentinel.is_none() {
            bail!(
                "field '{}' is not nullable but wire value is NULL",
                field.name
            );
        }
        return Ok(());
    }

    if value.kind() != field.value_kind {
        bail!(
            "field '{}': wire value {} does not match declared type {:?}",
            field.name,
            value,
            field.value_kind
        );
    }
    Ok(())
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
        check_value_matches_field(&v, schema.field_config(name)?)?;
        literals.push(quote_literal(&v));
    }
    for (proto_value, name) in value.iter().zip(subsidiary_fields) {
        let v = Value::try_from(proto_value).with_context(|| format!("field '{}'", name))?;
        check_value_matches_field(&v, schema.field_config(name)?)?;
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
        check_value_matches_field(&value, schema.field_config(name)?)?;
        set_parts.push(format!(
            "{} = {}",
            quote_identifier(name),
            quote_literal(&value)
        ));
    }

    if set_parts.is_empty() {
        bail!("update has no SET assignments — would emit an empty SET clause");
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
        check_value_matches_field(&value, schema.field_config(name)?)?;
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
    let schema = TableSchema::resolve(&delta.fields, config, table_name)?;
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
    let schema = TableSchema::resolve(&table.fields, config, table_name)?;
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
        delta_to_sql(config, table_name, delta, &injected_fields, &mut sql)?;
    }

    for (table_name, table) in &patch.states {
        state_table_to_sql(config, table_name, table, &injected_fields, &mut sql)?;
    }

    sql.push_str("COMMIT;\n");
    Ok(Some(sql))
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::config::{FieldConfig, TruncateConfig};
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
                    primary_key: *primary_key,
                    ..Default::default()
                })
                .collect(),
        }
    }

    /// Build a ProtoPatch for tests. Defaults `head`, `created`,
    /// `injected_fields`, `num_blocks`, and `states`; the caller supplies
    /// the deltas that distinguish the test case.
    fn dummy_patch(deltas: HashMap<String, ProtoDelta>) -> ProtoPatch {
        ProtoPatch {
            head: "abc123".to_string(),
            created: None,
            injected_fields: Vec::new(),
            num_blocks: 1,
            deltas,
            states: HashMap::new(),
        }
    }

    /// Build an empty ProtoDelta with the given column names. Tests push
    /// inserts, deletes, or updates onto the returned delta as needed.
    fn dummy_delta(fields: &[&str]) -> ProtoDelta {
        ProtoDelta {
            fields: fields.iter().map(|s| s.to_string()).collect(),
            inserts: vec![],
            deletes: vec![],
            updates: vec![],
        }
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
    fn test_patch_to_sql_accepts_well_formed_patch() {
        let table_config = dummy_table(&[("id", true)]);
        let config = dummy_config(HashMap::from([("test_table".to_string(), table_config)]));

        let mut delta = dummy_delta(&["id"]);
        delta.inserts.push(ProtoEntry {
            key: text_proto_values(&["1"]),
            value: vec![],
        });
        let patch = dummy_patch(HashMap::from([("test_table".to_string(), delta)]));

        let result = patch_to_sql(&config, &patch).unwrap().unwrap();
        assert!(result.contains("INSERT INTO"));
    }

    #[test]
    fn test_patch_to_sql_rejects_update_with_no_set_assignments() {
        // A sparse update with empty `changed_indices` and empty `new_value`
        // would render as `UPDATE "t" SET  WHERE ...;` with an empty SET
        // clause. Reject it instead of emitting malformed SQL.
        let table_config = dummy_table(&[("id", true), ("name", false)]);
        let config = dummy_config(HashMap::from([("test_table".to_string(), table_config)]));

        let mut delta = dummy_delta(&["id", "name"]);
        delta.updates.push(ProtoUpdate {
            key: text_proto_values(&["1"]),
            changed_indices: vec![],
            old_value: vec![],
            new_value: vec![],
        });
        let patch = dummy_patch(HashMap::from([("test_table".to_string(), delta)]));

        let err = patch_to_sql(&config, &patch).unwrap_err();
        let msg = format!("{:#}", err);
        assert!(msg.contains("empty SET clause"), "got: {}", msg);
    }

    #[test]
    fn test_patch_to_sql_rejects_out_of_range_changed_index() {
        // Two-column table: id (PK) + name (subsidiary). An update whose
        // changed_indices points at column 5 must bail rather than panic.
        let table_config = dummy_table(&[("id", true), ("name", false)]);
        let config = dummy_config(HashMap::from([("test_table".to_string(), table_config)]));

        let mut delta = dummy_delta(&["id", "name"]);
        delta.updates.push(ProtoUpdate {
            key: text_proto_values(&["1"]),
            changed_indices: vec![5],
            old_value: vec![],
            new_value: text_proto_values(&["x"]),
        });
        let patch = dummy_patch(HashMap::from([("test_table".to_string(), delta)]));

        let err = patch_to_sql(&config, &patch).unwrap_err();
        let msg = format!("{:#}", err);
        assert!(msg.contains("out of range"), "got: {}", msg);
    }

    /// When the agent and hub agree on field set, types, PK assignment, and
    /// nullability but disagree on subsidiary declaration order, the hub
    /// honours the wire's `delta.fields` order so each value lands in the
    /// column the agent intended.
    #[test]
    fn test_subsidiary_order_drift_uses_wire_order_for_columns() {
        // Hub config: subsidiary declaration order is [email, name].
        let hub_config_table = dummy_table(&[("id", true), ("email", false), ("name", false)]);
        let hub_config = dummy_config(HashMap::from([("users".to_string(), hub_config_table)]));

        // Wire entry as the agent would have serialized it: subsidiary values
        // laid out in the agent's declaration order, i.e. [name, email].
        let mut delta = dummy_delta(&["id", "name", "email"]);
        delta.inserts.push(ProtoEntry {
            key: text_proto_values(&["1"]),
            value: text_proto_values(&["Alice", "alice@example.com"]),
        });

        let patch = dummy_patch(HashMap::from([("users".to_string(), delta)]));

        let sql = patch_to_sql(&hub_config, &patch).unwrap().unwrap();

        // The hub emits columns in the wire's order, so 'Alice' lands in
        // the name column and the email address lands in the email column.
        assert!(
            sql.contains("INSERT INTO \"users\" (\"id\", \"name\", \"email\") VALUES ('1', 'Alice', 'alice@example.com');"),
            "expected wire-order SQL, got:\n{sql}"
        );
    }

    #[test]
    fn test_resolve_rejects_wire_field_not_in_config() {
        // A malicious agent that passes the field-hash check could still
        // try to target a column outside the configured schema set by
        // putting an unknown name in `delta.fields`.
        let hub_config_table = dummy_table(&[("id", true), ("name", false)]);
        let hub_config = dummy_config(HashMap::from([("users".to_string(), hub_config_table)]));

        let wire_fields = vec!["id".to_string(), "password_hash".to_string()];
        let result = TableSchema::resolve(&wire_fields, &hub_config, "users");
        let msg = format!("{:#}", result.err().unwrap());
        assert!(msg.contains("not declared in hub config"), "got: {msg}");
    }

    #[test]
    fn test_resolve_rejects_wire_pk_disagreement() {
        // Hub: id is the sole PK. A malicious agent claims `email` is the
        // PK so its UPDATE/DELETE WHERE clauses scope on email instead.
        let hub_config_table = dummy_table(&[("id", true), ("email", false)]);
        let hub_config = dummy_config(HashMap::from([("users".to_string(), hub_config_table)]));

        let wire_fields = vec!["email".to_string(), "id".to_string()];
        let result = TableSchema::resolve(&wire_fields, &hub_config, "users");
        let msg = format!("{:#}", result.err().unwrap());
        assert!(msg.contains("primary-key prefix"), "got: {msg}");
    }

    fn make_field(name: &str, value_kind: ValueKind, nullable: bool) -> FieldConfig {
        FieldConfig {
            name: name.to_string(),
            value_kind,
            null_sentinel: if nullable { Some("".to_string()) } else { None },
            ..Default::default()
        }
    }

    #[test]
    fn test_check_value_matches_field_accepts_correct_types() {
        check_value_matches_field(
            &Value::Text("hello".into()),
            &make_field("name", ValueKind::Text, false),
        )
        .unwrap();
        check_value_matches_field(
            &Value::Number(2.5),
            &make_field("price", ValueKind::Number, false),
        )
        .unwrap();
        check_value_matches_field(
            &Value::Boolean(true),
            &make_field("flag", ValueKind::Boolean, false),
        )
        .unwrap();
    }

    #[test]
    fn test_check_value_matches_field_rejects_type_drift() {
        // Wire sends a Number into a column the hub config declared TEXT.
        let err = check_value_matches_field(
            &Value::Number(42.0),
            &make_field("note", ValueKind::Text, false),
        )
        .unwrap_err();
        let msg = format!("{:#}", err);
        assert!(msg.contains("does not match declared type"), "got: {msg}");
    }

    #[test]
    fn test_check_value_matches_field_rejects_null_in_non_nullable() {
        let err =
            check_value_matches_field(&Value::Null, &make_field("name", ValueKind::Text, false))
                .unwrap_err();
        let msg = format!("{:#}", err);
        assert!(msg.contains("not nullable"), "got: {msg}");
    }

    #[test]
    fn test_check_value_matches_field_accepts_null_in_nullable() {
        check_value_matches_field(&Value::Null, &make_field("name", ValueKind::Text, true))
            .unwrap();
    }

    #[test]
    fn test_patch_to_sql_rejects_wire_value_with_wrong_type() {
        // Hub declares the subsidiary column as NUMBER. The wire passes the
        // resolve checks, but the inserted value is a Text.
        let mut table = dummy_table(&[("id", true), ("score", false)]);
        table.fields[1].value_kind = ValueKind::Number;
        let config = dummy_config(HashMap::from([("t".to_string(), table)]));

        let mut delta = dummy_delta(&["id", "score"]);
        delta.inserts.push(ProtoEntry {
            key: text_proto_values(&["1"]),
            value: text_proto_values(&["not-a-number"]),
        });
        let patch = dummy_patch(HashMap::from([("t".to_string(), delta)]));

        let err = patch_to_sql(&config, &patch).unwrap_err();
        let msg = format!("{:#}", err);
        assert!(msg.contains("does not match declared type"), "got: {msg}");
    }
}
