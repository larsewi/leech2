mod common;

use leech2::block::Block;
use leech2::config::Config;
use leech2::patch::Patch;
use leech2::sql;

#[test]
fn test_hash_prefix_resolution() {
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

    common::write_csv(work_dir, "users.csv", "1,Alice\n");
    Config::init(work_dir).unwrap();
    let hash1 = Block::create().unwrap();

    common::write_csv(work_dir, "users.csv", "1,Alice\n2,Bob\n");
    let hash2 = Block::create().unwrap();

    // Use a short prefix of hash1 (first 8 chars)
    let prefix = &hash1[..8];
    let patch = Patch::create(prefix).unwrap();
    assert_eq!(patch.num_blocks, 1);
    assert_eq!(patch.head_hash, hash2);

    let sql = sql::patch_to_sql(&patch).unwrap().unwrap();
    assert!(sql.contains(r#"INSERT INTO "users" ("id", "name") VALUES (2, 'Bob');"#));

    // Invalid prefix should error
    let result = Patch::create("deadbeefdeadbeef");
    assert!(result.is_err(), "unknown hash prefix should return error");

    common::assert_wire_roundtrip(&patch);
}
