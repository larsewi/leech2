mod common;

use leech2::block::Block;
use leech2::config::Config;
use leech2::patch::Patch;
use leech2::sql;
use leech2::utils::GENESIS_HASH;

#[test]
fn test_csv_with_header_row() {
    common::init_logging();
    let tmp = tempfile::tempdir().unwrap();
    let work_dir = tmp.path();

    common::write_config(
        work_dir,
        "config.toml",
        r#"
[tables.users]
source = "users.csv"
header = true
fields = [
    { name = "id", type = "NUMBER", primary-key = true },
    { name = "name", type = "TEXT" },
]
"#,
    );

    // CSV with header row — the header should be skipped
    common::write_csv(work_dir, "users.csv", "id,name\n1,Alice\n2,Bob\n");
    let config = Config::load(work_dir).unwrap();
    Block::create(&config).unwrap();

    let patch = Patch::create(&config, GENESIS_HASH).unwrap();
    let sql = sql::patch_to_sql(&config, &patch).unwrap().unwrap();

    // Should have 2 inserts (the header row is not treated as data)
    assert_eq!(common::count_sql(&sql, "INSERT INTO"), 2);

    // Verify that header values ("id", "name") are not in the SQL as data values
    assert!(
        !sql.contains("VALUES ('id',"),
        "header row should be skipped"
    );

    common::assert_wire_roundtrip(&config, &patch);
}

#[test]
fn test_csv_header_reordered_columns() {
    common::init_logging();
    let tmp = tempfile::tempdir().unwrap();
    let work_dir = tmp.path();

    // Config declares fields in order: id, name, email
    common::write_config(
        work_dir,
        "config.toml",
        r#"
[tables.users]
source = "users.csv"
header = true
fields = [
    { name = "id", type = "NUMBER", primary-key = true },
    { name = "name", type = "TEXT" },
    { name = "email", type = "TEXT" },
]
"#,
    );

    // CSV columns are in a different order: name, email, id
    common::write_csv(
        work_dir,
        "users.csv",
        "name,email,id\nAlice,alice@example.com,1\nBob,bob@example.com,2\n",
    );
    let config = Config::load(work_dir).unwrap();
    Block::create(&config).unwrap();

    let patch = Patch::create(&config, GENESIS_HASH).unwrap();
    let sql = sql::patch_to_sql(&config, &patch).unwrap().unwrap();

    // Verify values are correctly mapped despite reordered CSV columns
    assert_eq!(common::count_sql(&sql, "INSERT INTO"), 2);
    assert!(sql.contains("'Alice'"));
    assert!(sql.contains("'alice@example.com'"));
    assert!(sql.contains("'Bob'"));
    assert!(sql.contains("'bob@example.com'"));

    // Verify header values are not treated as data
    assert!(
        !sql.contains("VALUES ('name',"),
        "header row should be skipped"
    );

    common::assert_wire_roundtrip(&config, &patch);
}

#[test]
fn test_empty_csv_table() {
    common::init_logging();
    let tmp = tempfile::tempdir().unwrap();
    let work_dir = tmp.path();

    common::write_config(
        work_dir,
        "config.toml",
        r#"
[tables.users]
source = "users.csv"
fields = [
    { name = "id", type = "NUMBER", primary-key = true },
    { name = "name", type = "TEXT" },
]
"#,
    );

    // Block 1: empty CSV (0 rows)
    common::write_csv(work_dir, "users.csv", "");
    let config = Config::load(work_dir).unwrap();
    let hash1 = Block::create(&config).unwrap();

    // Patch from genesis with empty table: no data to insert
    let patch = Patch::create(&config, GENESIS_HASH).unwrap();
    let sql = sql::patch_to_sql(&config, &patch).unwrap();
    match sql {
        Some(s) => {
            assert_eq!(common::count_sql(&s, "INSERT INTO"), 0);
            assert_eq!(common::count_sql(&s, "DELETE FROM"), 0);
        }
        None => {} // Also acceptable: no payload at all
    }

    // Block 2: add rows to previously empty table
    common::write_csv(work_dir, "users.csv", "1,Alice\n2,Bob\n");
    let _hash2 = Block::create(&config).unwrap();

    // Patch from hash1: should show 2 inserts
    let patch2 = Patch::create(&config, &hash1).unwrap();
    let sql2 = sql::patch_to_sql(&config, &patch2).unwrap().unwrap();
    assert_eq!(common::count_sql(&sql2, "INSERT INTO"), 2);

    common::assert_wire_roundtrip(&config, &patch2);
}

#[test]
fn test_field_type_sql_quoting() {
    common::init_logging();
    let tmp = tempfile::tempdir().unwrap();
    let work_dir = tmp.path();

    common::write_config(
        work_dir,
        "config.toml",
        r#"
[tables.records]
source = "records.csv"
fields = [
    { name = "id", type = "NUMBER", primary-key = true },
    { name = "label", type = "TEXT" },
    { name = "count", type = "NUMBER" },
    { name = "active", type = "BOOLEAN" },
    { name = "temperature", type = "NUMBER" },
    { name = "notes", type = "TEXT" },
]
"#,
    );

    // Block 1: initial data with all field types
    common::write_csv(work_dir, "records.csv", "1,hello,42,true,36.6,first note\n");
    let config = Config::load(work_dir).unwrap();
    let hash1 = Block::create(&config).unwrap();

    // Block 2: update all subsidiary fields
    common::write_csv(
        work_dir,
        "records.csv",
        "1,it's a test,99,false,-3.14,second note\n",
    );
    let _hash2 = Block::create(&config).unwrap();

    // Patch from genesis: consolidated insert with v2 values (rule 7)
    let patch_genesis = Patch::create(&config, GENESIS_HASH).unwrap();
    let sql_genesis = sql::patch_to_sql(&config, &patch_genesis).unwrap().unwrap();

    // TEXT: escaped single quote
    assert!(sql_genesis.contains("'it''s a test'"));
    // NUMBER: unquoted
    assert!(sql_genesis.contains("99"));
    // BOOLEAN: normalized
    assert!(sql_genesis.contains("FALSE"));
    // NUMBER (float): unquoted
    assert!(sql_genesis.contains("-3.14"));
    // TEXT: single-quoted
    assert!(sql_genesis.contains("'second note'"));

    // Patch from hash1: verify type quoting regardless of payload type.
    // With 1 row and all fields changed, the patch may choose State (TRUNCATE+INSERT)
    // or Deltas (UPDATE). Either way, the SQL literals must be correctly formatted.
    let patch_partial = Patch::create(&config, &hash1).unwrap();
    let sql_partial = sql::patch_to_sql(&config, &patch_partial).unwrap().unwrap();

    assert!(sql_partial.contains("'it''s a test'"));
    assert!(sql_partial.contains("99"));
    assert!(sql_partial.contains("FALSE"));
    assert!(sql_partial.contains("-3.14"));
    assert!(sql_partial.contains("'second note'"));

    common::assert_wire_roundtrip(&config, &patch_genesis);
    common::assert_wire_roundtrip(&config, &patch_partial);
}

#[test]
fn test_null_sentinel() {
    common::init_logging();
    let tmp = tempfile::tempdir().unwrap();
    let work_dir = tmp.path();

    common::write_config(
        work_dir,
        "config.toml",
        r#"
[tables.items]
source = "items.csv"
fields = [
    { name = "id", type = "NUMBER", primary-key = true },
    { name = "name", type = "TEXT" },
    { name = "notes", type = "TEXT", null = "" },
    { name = "score", type = "NUMBER", null = "N/A" },
]
"#,
    );

    // Block 1: row with non-null values
    common::write_csv(work_dir, "items.csv", "1,Alice,some notes,42\n");
    let config = Config::load(work_dir).unwrap();
    let hash1 = Block::create(&config).unwrap();

    // Block 2: update to null sentinels
    common::write_csv(work_dir, "items.csv", "1,Alice,,N/A\n");
    let _hash2 = Block::create(&config).unwrap();

    // Patch from genesis: consolidated insert should have NULLs
    let patch = Patch::create(&config, GENESIS_HASH).unwrap();
    let sql = sql::patch_to_sql(&config, &patch).unwrap().unwrap();
    assert!(
        sql.contains("NULL"),
        "should contain NULL for sentinel values"
    );
    // name should not be NULL (no sentinel configured)
    assert!(sql.contains("'Alice'"));

    // Patch from hash1: delta should also have NULLs
    let patch2 = Patch::create(&config, &hash1).unwrap();
    let sql2 = sql::patch_to_sql(&config, &patch2).unwrap().unwrap();
    assert!(sql2.contains("NULL"), "delta should contain NULL");

    common::assert_wire_roundtrip(&config, &patch);
    common::assert_wire_roundtrip(&config, &patch2);
}

#[test]
fn test_null_on_primary_key_rejected() {
    common::init_logging();
    let tmp = tempfile::tempdir().unwrap();

    common::write_config(
        tmp.path(),
        "config.toml",
        r#"
[tables.items]
source = "items.csv"
fields = [
    { name = "id", type = "NUMBER", primary-key = true, null = "" },
    { name = "name", type = "TEXT" },
]
"#,
    );

    let result = Config::load(tmp.path());
    assert!(result.is_err());
    let error = format!("{:#}", result.unwrap_err());
    assert!(
        error.contains("null sentinel"),
        "should reject null on primary key"
    );
}
