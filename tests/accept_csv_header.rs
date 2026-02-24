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
    Config::init(work_dir).unwrap();
    Block::create().unwrap();

    let patch = Patch::create(GENESIS_HASH).unwrap();
    let sql = sql::patch_to_sql(&patch).unwrap().unwrap();

    // Should have 2 inserts (the header row is not treated as data)
    assert_eq!(common::count_sql(&sql, "INSERT INTO"), 2);

    // Verify that header values ("id", "name") are not in the SQL as data values
    assert!(!sql.contains("VALUES ('id',"), "header row should be skipped");

    common::assert_wire_roundtrip(&patch);
}
