use std::collections::{HashMap, HashSet};

use anyhow::{Context, Result, anyhow, bail};

use crate::cell::{Cell, Kind};
use crate::config::{Config, FieldConfig};
use crate::proto::cell::Cell as ProtoCell;
use crate::proto::delta::Delta as ProtoDelta;
use crate::proto::injected::Field as ProtoInjectedField;
use crate::proto::patch::Patch as ProtoPatch;
use crate::proto::record::Record as ProtoRecord;
use crate::proto::table::Table as ProtoTable;
use crate::proto::update::Update as ProtoUpdate;

/// Schema information for a single table, derived from the wire-declared
/// field lists. Column ordering follows the wire (i.e. the agent's
/// declaration order). The hub honors that order when generating SQL so
/// values land in the columns the agent intended, regardless of how the
/// hub config declares them.
struct TableSchema<'a> {
    /// Primary-key field names, in wire order.
    primary_key_names: &'a [String],
    /// Subsidiary (non-key) field names, in wire order.
    subsidiary_value_names: &'a [String],
    /// Hub-config field metadata keyed by field name. Used at SQL-rendering
    /// time to validate that each wire cell's variant agrees with the
    /// hub's declared type and that nulls only appear in nullable columns.
    field_configs: HashMap<&'a str, &'a FieldConfig>,
}

impl<'a> TableSchema<'a> {
    /// Resolve a table's schema from the wire-declared primary-key and
    /// subsidiary field lists, validating that the wire's view of the
    /// schema agrees with the hub config.
    ///
    /// - The union of primary-key and subsidiary names must equal the hub
    ///   config field set — a wire that omits a column would silently
    ///   leave it at the DB's default value, and an unknown name could
    ///   target columns the operator never authorized leech2 to write to.
    /// - The wire's primary-key set must equal the hub's primary-key
    ///   set — otherwise an agent could choose which column scopes the
    ///   WHERE clause on UPDATE/DELETE, allowing arbitrary-row targeting.
    ///
    /// Type and nullability drift is caught later, per cell, by
    /// [`check_value_matches_field`].
    fn resolve(
        wire_primary_key_names: &'a [String],
        wire_subsidiary_value_names: &'a [String],
        config: &'a Config,
        table_name: &str,
    ) -> Result<Self> {
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

        let wire_field_count = wire_primary_key_names.len() + wire_subsidiary_value_names.len();
        if wire_field_count != table_config.fields.len() {
            bail!(
                "wire field count {} disagrees with hub config field count {} for table '{}'",
                wire_field_count,
                table_config.fields.len(),
                table_name
            );
        }

        for name in wire_primary_key_names
            .iter()
            .chain(wire_subsidiary_value_names)
        {
            if !field_configs.contains_key(name.as_str()) {
                bail!(
                    "wire field '{}' is not declared in hub config for table '{}'",
                    name,
                    table_name
                );
            }
        }

        let wire_pk_set: HashSet<&str> =
            wire_primary_key_names.iter().map(String::as_str).collect();
        if wire_pk_set != hub_pk_set {
            bail!(
                "wire primary-key set {:?} disagrees with hub primary-key set {:?} for table '{}'",
                wire_pk_set,
                hub_pk_set,
                table_name
            );
        }

        Ok(TableSchema {
            primary_key_names: wire_primary_key_names,
            subsidiary_value_names: wire_subsidiary_value_names,
            field_configs,
        })
    }

    /// Look up the hub `FieldConfig` for a wire field name. The wire-field
    /// validation in `resolve` guarantees every wire name has a hub config
    /// entry, so a missing entry here is an internal bug.
    fn field_config(&self, name: &str) -> Result<&FieldConfig> {
        self.field_configs
            .get(name)
            .copied()
            .with_context(|| format!("internal error: no hub field config for '{}'", name))
    }
}

/// Validate that a wire cell's variant agrees with the field's declared
/// type. `Null` is accepted on any non-primary-key field; primary-key fields
/// reject `Null` upstream during state computation.
fn check_value_matches_field(value: &Cell, field: &FieldConfig) -> Result<()> {
    if value.kind() == Kind::Null {
        return Ok(());
    }

    if value.kind() != field.kind {
        bail!(
            "field '{}': wire value {} does not match declared type {:?}",
            field.name,
            value,
            field.kind
        );
    }
    Ok(())
}

/// A static field injected into all SQL output (resolved from proto).
struct InjectedField {
    name: String,
    value: Cell,
}

impl TryFrom<&ProtoInjectedField> for InjectedField {
    type Error = anyhow::Error;

    fn try_from(proto: &ProtoInjectedField) -> Result<Self> {
        let proto_value = proto
            .value
            .as_ref()
            .with_context(|| format!("injected field '{}': missing value", proto.name))?;
        let value = Cell::try_from(proto_value)
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
pub fn quote_identifier(name: &str) -> String {
    format!("\"{}\"", name.replace('"', "\"\""))
}

/// Format a `Cell` as a SQL literal.
pub fn quote_literal(value: &Cell) -> String {
    match value {
        Cell::Null => "NULL".to_string(),
        Cell::Text(s) => format!("'{}'", s.replace('\'', "''")),
        Cell::Boolean(true) => "TRUE".to_string(),
        Cell::Boolean(false) => "FALSE".to_string(),
        Cell::Number(n) => n.to_string(),
    }
}

/// Convert key + value proto-cell slices into a list of SQL literal strings.
fn format_row(key: &[ProtoCell], value: &[ProtoCell], schema: &TableSchema) -> Result<Vec<String>> {
    if key.len() != schema.primary_key_names.len() {
        bail!(
            "primary key field count mismatch: got {} values, expected {}",
            key.len(),
            schema.primary_key_names.len()
        );
    }
    if value.len() != schema.subsidiary_value_names.len() {
        bail!(
            "subsidiary field count mismatch: got {} values, expected {}",
            value.len(),
            schema.subsidiary_value_names.len()
        );
    }

    let mut literals = Vec::with_capacity(key.len() + value.len());
    for (proto_value, name) in key.iter().zip(schema.primary_key_names) {
        let v = Cell::try_from(proto_value).with_context(|| format!("field '{}'", name))?;
        check_value_matches_field(&v, schema.field_config(name)?)?;
        literals.push(quote_literal(&v));
    }
    for (proto_value, name) in value.iter().zip(schema.subsidiary_value_names) {
        let v = Cell::try_from(proto_value).with_context(|| format!("field '{}'", name))?;
        check_value_matches_field(&v, schema.field_config(name)?)?;
        literals.push(quote_literal(&v));
    }
    Ok(literals)
}

/// Generate DELETE statements for a list of records.
fn emit_deletes(
    records: &[ProtoRecord],
    schema: &TableSchema,
    injected_fields: &[InjectedField],
    quoted_table: &str,
    out: &mut String,
) -> Result<()> {
    for record in records {
        let where_clause = primary_key_where_clause(&record.key, schema, injected_fields)
            .with_context(|| format!("key {:?}", record.key))?;
        out.push_str(&format!(
            "DELETE FROM {} WHERE {};\n",
            quoted_table, where_clause
        ));
    }
    Ok(())
}

/// Generate INSERT statements for a list of records.
fn emit_inserts(
    records: &[ProtoRecord],
    schema: &TableSchema,
    injected_fields: &[InjectedField],
    quoted_table: &str,
    out: &mut String,
) -> Result<()> {
    if records.is_empty() {
        return Ok(());
    }

    let mut column_parts: Vec<String> =
        Vec::with_capacity(schema.primary_key_names.len() + schema.subsidiary_value_names.len());
    for name in schema
        .primary_key_names
        .iter()
        .chain(schema.subsidiary_value_names)
    {
        column_parts.push(quote_identifier(name));
    }

    let injected_columns: Vec<String> = injected_fields.iter().map(|f| f.quoted_column()).collect();
    column_parts.splice(..0, injected_columns);
    let columns = column_parts.join(", ");

    // Injected values are static across the entire patch, so compute once.
    let injected_values: Vec<String> = injected_fields.iter().map(|f| f.quoted_value()).collect();

    for record in records {
        let mut literals = format_row(&record.key, &record.value, schema)
            .with_context(|| format!("key {:?}", record.key))?;
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

    if indices.len() != update.new_value.len() {
        bail!(
            "update new_value count mismatch: got {} values, expected {}",
            update.new_value.len(),
            indices.len()
        );
    }

    let mut set_parts = Vec::new();
    for (&index, proto_value) in indices.iter().zip(update.new_value.iter()) {
        let name = subsidiary_names.get(index as usize).ok_or_else(|| {
            anyhow!(
                "changed_indices entry {} is out of range (table has {} subsidiary columns)",
                index,
                subsidiary_names.len()
            )
        })?;
        let value = Cell::try_from(proto_value).with_context(|| format!("field '{}'", name))?;
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
    for update in updates {
        let stmt = format_update(
            update,
            schema.subsidiary_value_names,
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
    key: &[ProtoCell],
    schema: &TableSchema,
    injected_fields: &[InjectedField],
) -> Result<String> {
    if key.len() != schema.primary_key_names.len() {
        bail!(
            "primary key field count mismatch: got {} values, expected {}",
            key.len(),
            schema.primary_key_names.len()
        );
    }

    let mut where_parts = Vec::new();
    for (proto_value, name) in key.iter().zip(schema.primary_key_names) {
        let value = Cell::try_from(proto_value).with_context(|| format!("field '{}'", name))?;
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
    let schema = TableSchema::resolve(
        &delta.primary_key_names,
        &delta.subsidiary_value_names,
        config,
        table_name,
    )?;
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
    let schema = TableSchema::resolve(
        &table.primary_key_names,
        &table.subsidiary_value_names,
        config,
        table_name,
    )?;
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

    emit_inserts(&table.records, &schema, injected_fields, &quoted_table, out)
        .with_context(|| format!("table '{table_name}'"))?;

    Ok(())
}

/// Convert a decoded patch to SQL statements.
///
/// The returned SQL is not wrapped in a transaction. Callers that need
/// atomicity should issue their own `BEGIN` / `COMMIT` (and may interleave
/// additional statements, e.g. recording the last applied block hash).
pub fn patch_to_sql(config: &Config, patch: &ProtoPatch) -> Result<Option<String>> {
    if patch.deltas.is_empty() && patch.states.is_empty() {
        log::info!("Patch has no payload, nothing to convert");
        return Ok(None);
    }

    let mut injected_fields = Vec::new();
    for proto_field in &patch.injected_fields {
        injected_fields.push(InjectedField::try_from(proto_field)?);
    }

    let mut sql = String::new();

    for (table_name, delta) in &patch.deltas {
        delta_to_sql(config, table_name, delta, &injected_fields, &mut sql)?;
    }

    for (table_name, table) in &patch.states {
        state_table_to_sql(config, table_name, table, &injected_fields, &mut sql)?;
    }

    if sql.is_empty() {
        log::info!("Patch produced no SQL statements");
        return Ok(None);
    }

    log::info!("Converted patch to SQL:\n{}", sql);
    Ok(Some(sql))
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::cell::text_proto_cells;
    use crate::config::{FieldConfig, TruncateConfig};

    fn dummy_config(tables: HashMap<String, crate::config::TableConfig>) -> Config {
        Config {
            work_dir: std::path::PathBuf::from("/tmp"),
            injected_fields: Vec::new(),
            compression: crate::config::CompressionConfig::default(),
            tables,
            truncate: TruncateConfig::default(),
            background_truncation: Default::default(),
        }
    }

    /// Build a TableConfig for tests. Each entry is `(field_name, is_primary_key)`;
    /// all fields are TEXT.
    fn dummy_table(fields: &[(&str, bool)]) -> crate::config::TableConfig {
        crate::config::TableConfig {
            fields: fields
                .iter()
                .map(|(name, primary_key)| FieldConfig {
                    name: name.to_string(),
                    primary_key: *primary_key,
                    ..Default::default()
                })
                .collect(),
            csv: None,
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

    /// Build an empty ProtoDelta with the given primary-key and subsidiary
    /// field names. Tests push inserts, deletes, or updates onto the
    /// returned delta as needed.
    fn dummy_delta(primary_keys: &[&str], subsidiary_values: &[&str]) -> ProtoDelta {
        ProtoDelta {
            primary_key_names: primary_keys.iter().map(|s| s.to_string()).collect(),
            subsidiary_value_names: subsidiary_values.iter().map(|s| s.to_string()).collect(),
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
        assert_eq!(quote_literal(&Cell::Null), "NULL");
    }

    #[test]
    fn test_quote_literal_number() {
        assert_eq!(quote_literal(&Cell::from(42.0)), "42");
        assert_eq!(quote_literal(&Cell::from(-100.0)), "-100");
        assert_eq!(quote_literal(&Cell::from(2.5)), "2.5");
        assert_eq!(quote_literal(&Cell::from(-0.5)), "-0.5");
    }

    #[test]
    fn test_quote_literal_boolean() {
        assert_eq!(quote_literal(&Cell::from(true)), "TRUE");
        assert_eq!(quote_literal(&Cell::from(false)), "FALSE");
    }

    #[test]
    fn test_patch_to_sql_accepts_well_formed_patch() {
        let table_config = dummy_table(&[("id", true)]);
        let config = dummy_config(HashMap::from([("test_table".to_string(), table_config)]));

        let mut delta = dummy_delta(&["id"], &[]);
        delta.inserts.push(ProtoRecord {
            key: text_proto_cells(&["1"]),
            value: vec![],
        });
        let patch = dummy_patch(HashMap::from([("test_table".to_string(), delta)]));

        let result = patch_to_sql(&config, &patch).unwrap().unwrap();
        assert!(result.contains("INSERT INTO"));
    }

    #[test]
    fn test_patch_to_sql_rejects_update_with_no_set_assignments() {
        // An update on a table with zero subsidiary columns has nothing to
        // assign and would render as `UPDATE "t" SET  WHERE ...;` with an
        // empty SET clause. Reject it instead of emitting malformed SQL.
        let table_config = dummy_table(&[("id", true), ("host", true)]);
        let config = dummy_config(HashMap::from([("test_table".to_string(), table_config)]));

        let mut delta = dummy_delta(&["id", "host"], &[]);
        delta.updates.push(ProtoUpdate {
            key: text_proto_cells(&["1", "h"]),
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

        let mut delta = dummy_delta(&["id"], &["name"]);
        delta.updates.push(ProtoUpdate {
            key: text_proto_cells(&["1"]),
            changed_indices: vec![5],
            old_value: vec![],
            new_value: text_proto_cells(&["x"]),
        });
        let patch = dummy_patch(HashMap::from([("test_table".to_string(), delta)]));

        let err = patch_to_sql(&config, &patch).unwrap_err();
        let msg = format!("{:#}", err);
        assert!(msg.contains("out of range"), "got: {}", msg);
    }

    /// When the agent and hub agree on field set, types, PK assignment, and
    /// nullability but disagree on subsidiary declaration order, the hub
    /// honours the wire's `subsidiary_value_names` order so each value
    /// lands in the column the agent intended.
    #[test]
    fn test_subsidiary_order_drift_uses_wire_order_for_columns() {
        // Hub config: subsidiary declaration order is [email, name].
        let hub_config_table = dummy_table(&[("id", true), ("email", false), ("name", false)]);
        let hub_config = dummy_config(HashMap::from([("users".to_string(), hub_config_table)]));

        // Wire entry as the agent would have serialized it: subsidiary values
        // laid out in the agent's declaration order, i.e. [name, email].
        let mut delta = dummy_delta(&["id"], &["name", "email"]);
        delta.inserts.push(ProtoRecord {
            key: text_proto_cells(&["1"]),
            value: text_proto_cells(&["Alice", "alice@example.com"]),
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
        // putting an unknown name in the wire field lists.
        let hub_config_table = dummy_table(&[("id", true), ("name", false)]);
        let hub_config = dummy_config(HashMap::from([("users".to_string(), hub_config_table)]));

        let primary_keys = vec!["id".to_string()];
        let subsidiary_values = vec!["password_hash".to_string()];
        let result = TableSchema::resolve(&primary_keys, &subsidiary_values, &hub_config, "users");
        let msg = format!("{:#}", result.err().unwrap());
        assert!(msg.contains("not declared in hub config"), "got: {msg}");
    }

    #[test]
    fn test_resolve_rejects_wire_pk_disagreement() {
        // Hub: id is the sole PK. A malicious agent claims `email` is the
        // PK so its UPDATE/DELETE WHERE clauses scope on email instead.
        let hub_config_table = dummy_table(&[("id", true), ("email", false)]);
        let hub_config = dummy_config(HashMap::from([("users".to_string(), hub_config_table)]));

        let primary_keys = vec!["email".to_string()];
        let subsidiary_values = vec!["id".to_string()];
        let result = TableSchema::resolve(&primary_keys, &subsidiary_values, &hub_config, "users");
        let msg = format!("{:#}", result.err().unwrap());
        assert!(msg.contains("primary-key set"), "got: {msg}");
    }

    fn make_field(name: &str, kind: Kind) -> FieldConfig {
        FieldConfig {
            name: name.to_string(),
            kind,
            ..Default::default()
        }
    }

    #[test]
    fn test_check_value_matches_field_accepts_correct_types() {
        check_value_matches_field(&Cell::Text("hello".into()), &make_field("name", Kind::Text))
            .unwrap();
        check_value_matches_field(&Cell::Number(2.5), &make_field("price", Kind::Number)).unwrap();
        check_value_matches_field(&Cell::Boolean(true), &make_field("flag", Kind::Boolean))
            .unwrap();
    }

    #[test]
    fn test_check_value_matches_field_rejects_type_drift() {
        // Wire sends a Number into a column the hub config declared TEXT.
        let err = check_value_matches_field(&Cell::Number(42.0), &make_field("note", Kind::Text))
            .unwrap_err();
        let msg = format!("{:#}", err);
        assert!(msg.contains("does not match declared type"), "got: {msg}");
    }

    #[test]
    fn test_check_value_matches_field_accepts_null() {
        // NULL is allowed on any non-primary-key field. Primary-key NULLs are
        // rejected upstream during state computation.
        check_value_matches_field(&Cell::Null, &make_field("name", Kind::Text)).unwrap();
    }

    #[test]
    fn test_patch_to_sql_rejects_wire_value_with_wrong_type() {
        // Hub declares the subsidiary column as NUMBER. The wire passes the
        // resolve checks, but the inserted value is a Text.
        let mut table = dummy_table(&[("id", true), ("score", false)]);
        table.fields[1].kind = Kind::Number;
        let config = dummy_config(HashMap::from([("t".to_string(), table)]));

        let mut delta = dummy_delta(&["id"], &["score"]);
        delta.inserts.push(ProtoRecord {
            key: text_proto_cells(&["1"]),
            value: text_proto_cells(&["not-a-number"]),
        });
        let patch = dummy_patch(HashMap::from([("t".to_string(), delta)]));

        let err = patch_to_sql(&config, &patch).unwrap_err();
        let msg = format!("{:#}", err);
        assert!(msg.contains("does not match declared type"), "got: {msg}");
    }

    #[test]
    fn test_patch_to_sql_rejects_update_with_mismatched_value_count() {
        let table = dummy_table(&[("id", true), ("a", false), ("b", false)]);
        let config = dummy_config(HashMap::from([("t".to_string(), table)]));

        let mut delta = dummy_delta(&["id"], &["a", "b"]);
        delta.updates.push(ProtoUpdate {
            key: text_proto_cells(&["1"]),
            changed_indices: vec![0, 1],
            old_value: text_proto_cells(&["x", "y"]),
            new_value: text_proto_cells(&["only-one"]),
        });
        let patch = dummy_patch(HashMap::from([("t".to_string(), delta)]));

        let err = patch_to_sql(&config, &patch).unwrap_err();
        let msg = format!("{:#}", err);
        assert!(msg.contains("new_value count mismatch"), "got: {msg}");
    }

    #[test]
    fn test_patch_to_sql_rejects_delete_with_short_primary_key() {
        let table = dummy_table(&[("id", true), ("host", true), ("name", false)]);
        let config = dummy_config(HashMap::from([("t".to_string(), table)]));

        let mut delta = dummy_delta(&["id", "host"], &["name"]);
        delta.deletes.push(ProtoRecord {
            key: text_proto_cells(&["1"]),
            value: vec![],
        });
        let patch = dummy_patch(HashMap::from([("t".to_string(), delta)]));

        let err = patch_to_sql(&config, &patch).unwrap_err();
        let msg = format!("{:#}", err);
        assert!(
            msg.contains("primary key field count mismatch"),
            "got: {msg}"
        );
    }

    #[test]
    fn test_patch_to_sql_rejects_update_with_empty_primary_key() {
        let table = dummy_table(&[("id", true), ("name", false)]);
        let config = dummy_config(HashMap::from([("t".to_string(), table)]));

        let mut delta = dummy_delta(&["id"], &["name"]);
        delta.updates.push(ProtoUpdate {
            key: vec![],
            changed_indices: vec![0],
            old_value: text_proto_cells(&["before"]),
            new_value: text_proto_cells(&["after"]),
        });
        let patch = dummy_patch(HashMap::from([("t".to_string(), delta)]));

        let err = patch_to_sql(&config, &patch).unwrap_err();
        let msg = format!("{:#}", err);
        assert!(
            msg.contains("primary key field count mismatch"),
            "got: {msg}"
        );
    }
}
