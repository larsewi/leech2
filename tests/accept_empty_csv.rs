mod common;

use leech2::block::Block;
use leech2::config::Config;
use leech2::patch::Patch;
use leech2::sql;
use leech2::utils::GENESIS_HASH;

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
