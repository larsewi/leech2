mod common;

use leech2::block::Block;
use leech2::config::Config;
use leech2::patch::Patch;
use leech2::sql;

#[test]
fn test_unchanged_csv_between_blocks() {
    common::init_logging();
    let tmp = tempfile::tempdir().unwrap();
    let work_dir = tmp.path();

    common::write_config(
        work_dir,
        "config.toml",
        r#"
[tables.users]
fields = [
    { name = "id", type = "NUMBER", primary-key = true },
    { name = "name", type = "TEXT" },
]

[tables.users.csv]
source = "users.csv"
"#,
    );

    common::write_csv(work_dir, "users.csv", "1,Alice\n2,Bob\n");
    let config = Config::load(work_dir).unwrap();

    let hash1 = Block::create(&config, None).unwrap();

    // Create second block without modifying CSV
    let hash2 = Block::create(&config, None).unwrap();
    assert_ne!(hash1, hash2, "blocks should differ (different timestamps)");

    // Patch from hash1: no changes
    let patch = Patch::create(&config, &hash1).unwrap();
    assert_eq!(patch.num_blocks, 1);

    // A no-change patch produces no SQL statements.
    let sql = sql::patch_to_sql(&config, &patch).unwrap();
    assert!(sql.is_none(), "expected no SQL, got: {:?}", sql);
}
