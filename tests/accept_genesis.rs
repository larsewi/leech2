mod common;

use leech2::block::Block;
use leech2::config::Config;
use leech2::head;
use leech2::patch::Patch;
use leech2::sql;
use leech2::utils::GENESIS_HASH;

#[test]
fn test_genesis_block_all_inserts() {
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
    common::write_csv(work_dir, "users.csv", "1,Alice\n2,Bob\n");

    Config::init(work_dir).unwrap();

    // Before any blocks: HEAD is genesis, patch should be empty
    assert_eq!(head::load().unwrap(), GENESIS_HASH);
    let patch_empty = Patch::create(GENESIS_HASH).unwrap();
    assert_eq!(patch_empty.num_blocks, 0);
    assert!(patch_empty.payload.is_none());
    assert_eq!(sql::patch_to_sql(&patch_empty).unwrap(), None);

    // Create genesis block
    let hash = Block::create().unwrap();
    assert_ne!(hash, GENESIS_HASH);
    assert_eq!(head::load().unwrap(), hash);

    // Create patch from genesis
    let patch = Patch::create(GENESIS_HASH).unwrap();
    assert_eq!(patch.num_blocks, 1);
    assert_eq!(patch.head_hash, hash);
    assert!(patch.head_created.is_some());

    // Verify SQL output
    let sql = sql::patch_to_sql(&patch).unwrap().unwrap();
    assert!(sql.starts_with("BEGIN;\n"));
    assert!(sql.ends_with("COMMIT;\n"));

    // Regardless of Deltas vs State payload, the SQL should insert both rows
    assert_eq!(common::count_sql(&sql, "INSERT INTO"), 2);
    common::assert_sql_statements(
        &sql,
        &[
            r#"INSERT INTO "users" ("id", "name") VALUES (1, 'Alice');"#,
            r#"INSERT INTO "users" ("id", "name") VALUES (2, 'Bob');"#,
        ],
    );

    // Wire roundtrip
    common::assert_wire_roundtrip(&patch);

    // No-op patch: last_known == HEAD, should return empty payload
    let patch_noop = Patch::create(&hash).unwrap();
    assert_eq!(patch_noop.num_blocks, 0);
    assert!(patch_noop.payload.is_none());
    assert_eq!(sql::patch_to_sql(&patch_noop).unwrap(), None);
}
