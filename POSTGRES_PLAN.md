# Plan: Apply Patches to PostgreSQL

## Context

leech2 can create patches (consolidated deltas or full state snapshots) but has no way to apply them to a database. The `lch_patch_to_sql()` function in `src/lib.rs:109` is a stub. We need a function that takes a protobuf-encoded patch and a PostgreSQL connection string, then executes the appropriate SQL against the database.

This also requires adding type information to the config, since CSV values are raw bytes and PostgreSQL needs typed parameters.

## Step 1: Refactor config format

**File: `src/config.rs`**

Replace `field_names: Vec<String>` and `primary_key: Vec<String>` with a structured `fields` array:

```rust
#[derive(Debug, Deserialize)]
pub struct FieldConfig {
    pub name: String,
    #[serde(rename = "type", default = "default_field_type")]
    pub field_type: String,
    #[serde(rename = "primary-key", default)]
    pub primary_key: bool,
}

fn default_field_type() -> String { "TEXT".to_string() }

#[derive(Debug, Deserialize)]
pub struct TableConfig {
    pub source: String,
    pub fields: Vec<FieldConfig>,
}
```

Add convenience methods to `TableConfig` for backwards compatibility with existing callers:
- `field_names() -> Vec<String>`
- `primary_key() -> Vec<String>`
- `field_types() -> Vec<String>`

Add validation in `Config::init()`: at least one primary key field per table, unique field names.

## Step 2: Update table.rs to use new config accessors

**File: `src/table.rs`** (lines 83-101)

Change direct field access to method calls:
- `config.primary_key` -> `config.primary_key()`
- `config.field_names` -> `config.field_names()`

Compute these once at the top of `parse_csv()` to avoid repeated allocations.

## Step 3: Update test config

**File: `tests/config.toml`**

```toml
[tables.foo]
source = "foo.csv"
fields = [
    { name = "foo", type = "INTEGER", primary-key = true },
    { name = "bar", type = "TEXT", primary-key = true },
    { name = "baz", type = "BOOLEAN" },
    { name = "qux", type = "FLOAT" },
]
```

## Step 4: Add postgres feature flag

**File: `Cargo.toml`**

```toml
[features]
default = []
postgres = ["dep:postgres"]

[dependencies]
postgres = { version = "0.19", optional = true }
```

## Step 5: Create `src/pg.rs` (new file, gated behind `#[cfg(feature = "postgres")]`)

Core components:

1. **Type mapping** - `SqlType` enum (Text, Integer, Float, Boolean) with `from_config()` parser
2. **Value conversion** - `convert_value(raw: &[u8], sql_type: &SqlType) -> Box<dyn ToSql + Sync>` - parses UTF-8 string bytes into typed Rust values (String, i64, f64, bool)
3. **Schema resolution** - `TableSchema` struct mapping delta/state field names to their SQL types using the config
4. **Delta application** - For each delta: DELETE (by PK), INSERT (all columns), UPDATE (SET values WHERE PK). All within a transaction.
5. **State application** - TRUNCATE table + INSERT all rows. Within same transaction.
6. **Top-level `apply_patch(patch_data: &[u8], conn_str: &str) -> Result<()>`** - Decodes patch, connects to PostgreSQL, runs everything in a single transaction.

SQL identifiers (table/column names) will be double-quoted for safety.

## Step 6: Add `lch_patch_apply` FFI function

**File: `src/lib.rs`**

- Register `#[cfg(feature = "postgres")] mod pg;`
- Add new FFI function:

```c
// in include/leech2.h
extern int lch_patch_apply(const uint8_t *patch, size_t patch_len, const char *conn_str);
```

Takes the protobuf-encoded patch buffer and a PostgreSQL connection string (e.g. `"host=localhost dbname=mydb user=postgres"`). Returns 0 on success, -1 on error (logged).

Keep the existing `lch_patch_to_sql` stub as-is for now.

## Step 7: Tests

- **Config parsing tests** in `src/config.rs` - new format, defaults, validation
- **Value conversion tests** in `src/pg.rs` - all types, error cases
- **Integration tests** gated behind `postgres` feature + `LEECH2_TEST_PG_CONN` env var

## Verification

1. `cargo build` - builds without postgres feature (no new dependencies)
2. `cargo build --features postgres` - builds with postgres support
3. `cargo test` - all existing tests pass with refactored config
4. `cargo test --features postgres` - unit tests for type conversion and SQL generation
5. Integration test with a real PostgreSQL instance (manual or CI with postgres service)
