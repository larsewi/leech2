mod common;

use leech2::block::Block;
use leech2::config::Config;
use leech2::patch::Patch;
use leech2::sql;
use leech2::utils::GENESIS_HASH;

#[test]
fn test_csv_with_header_row() {
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
    { name = "id", type = "INTEGER", primary-key = true },
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
fn test_empty_csv_table() {
    let tmp = tempfile::tempdir().unwrap();
    let work_dir = tmp.path();

    common::write_config(
        work_dir,
        "config.toml",
        r#"
[tables.users]
source = "users.csv"
fields = [
    { name = "id", type = "INTEGER", primary-key = true },
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
    let tmp = tempfile::tempdir().unwrap();
    let work_dir = tmp.path();

    common::write_config(
        work_dir,
        "config.toml",
        r#"
[tables.records]
source = "records.csv"
fields = [
    { name = "id", type = "INTEGER", primary-key = true },
    { name = "label", type = "TEXT" },
    { name = "count", type = "INTEGER" },
    { name = "active", type = "BOOLEAN" },
    { name = "created", type = "DATE" },
    { name = "temperature", type = "FLOAT" },
    { name = "sampled_at", type = "TIME" },
    { name = "recorded_at", type = "DATETIME" },
    { name = "payload", type = "BINARY" },
]
"#,
    );

    // Block 1: initial data with all field types
    common::write_csv(
        work_dir,
        "records.csv",
        "1,hello,42,true,2024-01-15,36.6,08:30:00,2024-01-15 10:30:00,48656C6C6F\n",
    );
    let config = Config::load(work_dir).unwrap();
    let hash1 = Block::create(&config).unwrap();

    // Block 2: update all subsidiary fields
    common::write_csv(
        work_dir,
        "records.csv",
        "1,it's a test,99,false,2024-06-30,-3.14,23:59:59,2024-06-30 18:00:00,DEADBEEF\n",
    );
    let _hash2 = Block::create(&config).unwrap();

    // Patch from genesis: consolidated insert with v2 values (rule 7)
    let patch_genesis = Patch::create(&config, GENESIS_HASH).unwrap();
    let sql_genesis = sql::patch_to_sql(&config, &patch_genesis).unwrap().unwrap();

    // TEXT: escaped single quote
    assert!(sql_genesis.contains("'it''s a test'"));
    // INTEGER: unquoted
    assert!(sql_genesis.contains("99"));
    // BOOLEAN: normalized
    assert!(sql_genesis.contains("FALSE"));
    // DATE: single-quoted
    assert!(sql_genesis.contains("'2024-06-30'"));
    // FLOAT: unquoted
    assert!(sql_genesis.contains("-3.14"));
    // TIME: single-quoted
    assert!(sql_genesis.contains("'23:59:59'"));
    // DATETIME: single-quoted
    assert!(sql_genesis.contains("'2024-06-30 18:00:00'"));
    // BINARY: hex-prefixed, quoted
    assert!(sql_genesis.contains(r"'\xDEADBEEF'"));

    // Patch from hash1: verify type quoting regardless of payload type.
    // With 1 row and all fields changed, the patch may choose State (TRUNCATE+INSERT)
    // or Deltas (UPDATE). Either way, the SQL literals must be correctly formatted.
    let patch_partial = Patch::create(&config, &hash1).unwrap();
    let sql_partial = sql::patch_to_sql(&config, &patch_partial).unwrap().unwrap();

    assert!(sql_partial.contains("'it''s a test'"));
    assert!(sql_partial.contains("99"));
    assert!(sql_partial.contains("FALSE"));
    assert!(sql_partial.contains("'2024-06-30'"));
    assert!(sql_partial.contains("-3.14"));
    assert!(sql_partial.contains("'23:59:59'"));
    assert!(sql_partial.contains("'2024-06-30 18:00:00'"));
    assert!(sql_partial.contains(r"'\xDEADBEEF'"));

    common::assert_wire_roundtrip(&config, &patch_genesis);
    common::assert_wire_roundtrip(&config, &patch_partial);
}
