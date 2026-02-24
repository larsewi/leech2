mod common;

use leech2::block::Block;
use leech2::config::Config;
use leech2::patch::Patch;
use leech2::sql;
use leech2::utils::GENESIS_HASH;

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
]
"#,
    );

    // Block 1: initial data
    common::write_csv(
        work_dir,
        "records.csv",
        "1,hello,42,true,2024-01-15\n",
    );
    Config::init(work_dir).unwrap();
    let hash1 = Block::create().unwrap();

    // Block 2: update all subsidiary fields, including single-quote in text
    common::write_csv(
        work_dir,
        "records.csv",
        "1,it's a test,99,false,2024-06-30\n",
    );
    let _hash2 = Block::create().unwrap();

    // Patch from genesis: verify INSERT quoting
    let patch_genesis = Patch::create(GENESIS_HASH).unwrap();
    let sql_genesis = sql::patch_to_sql(&patch_genesis).unwrap().unwrap();

    // Final state from genesis is a single insert with the v2 values
    // (insert + update = insert with new value, rule 7)
    assert!(sql_genesis.contains("99")); // INTEGER: no quotes
    assert!(sql_genesis.contains("FALSE")); // BOOLEAN: normalized
    assert!(sql_genesis.contains("'2024-06-30'")); // DATE: single-quoted
    assert!(sql_genesis.contains("'it''s a test'")); // TEXT: escaped single quote

    // Patch from hash1: verify type quoting regardless of payload type.
    // With 1 row and all fields changed, the patch may choose State (TRUNCATE+INSERT)
    // or Deltas (UPDATE). Either way, the SQL literals must be correctly formatted.
    let patch_partial = Patch::create(&hash1).unwrap();
    let sql_partial = sql::patch_to_sql(&patch_partial).unwrap().unwrap();

    // Verify type-specific formatting in the SQL output
    assert!(sql_partial.contains("'it''s a test'")); // TEXT: escaped quote
    assert!(sql_partial.contains("99")); // INTEGER: unquoted
    assert!(sql_partial.contains("FALSE")); // BOOLEAN: normalized
    assert!(sql_partial.contains("'2024-06-30'")); // DATE: quoted

    common::assert_wire_roundtrip(&patch_genesis);
    common::assert_wire_roundtrip(&patch_partial);
}
