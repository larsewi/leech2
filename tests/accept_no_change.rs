mod common;

use leech2::block::Block;
use leech2::config::Config;
use leech2::patch::Patch;
use leech2::sql;

#[test]
fn test_unchanged_csv_between_blocks() {
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

    let hash1 = Block::create().unwrap();

    // Create second block without modifying CSV
    let hash2 = Block::create().unwrap();
    assert_ne!(hash1, hash2, "blocks should differ (different timestamps)");

    // Patch from hash1: no changes
    let patch = Patch::create(&hash1).unwrap();
    assert_eq!(patch.num_blocks, 1);

    let sql = sql::patch_to_sql(&patch).unwrap();
    match sql {
        Some(s) => {
            // Empty transaction (no operations)
            assert_eq!(s.trim(), "BEGIN;\nCOMMIT;");
        }
        None => {
            // Also acceptable if the patch has no payload
        }
    }
}
