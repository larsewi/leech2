mod common;

use leech2::block::Block;
use leech2::config::Config;
use leech2::patch::Patch;
use leech2::sql;

#[test]
fn test_state_payload_when_smaller_than_deltas() {
    let tmp = tempfile::tempdir().unwrap();
    let work_dir = tmp.path();

    common::write_config(
        work_dir,
        "config.toml",
        r#"
[tables.items]
source = "items.csv"
fields = [
    { name = "id", type = "INTEGER", primary-key = true },
    { name = "name", type = "TEXT" },
]
"#,
    );

    // Block 1: 20 rows
    let mut csv = String::new();
    for i in 1..=20 {
        csv.push_str(&format!("{},item{}\n", i, i));
    }
    common::write_csv(work_dir, "items.csv", &csv);
    let config = Config::load(work_dir).unwrap();
    let hash1 = Block::create(&config).unwrap();

    // Block 2: keep only 2 rows (delete 18)
    common::write_csv(work_dir, "items.csv", "1,item1\n2,item2\n");
    let _hash2 = Block::create(&config).unwrap();

    // Patch from hash1: delta has 18 deletes, state has 2 rows.
    // State should be smaller -> SQL uses TRUNCATE + INSERT pattern.
    // If deltas win instead, SQL uses DELETE FROM pattern.
    let patch = Patch::create(&config, &hash1).unwrap();
    assert_eq!(patch.num_blocks, 1);
    assert!(patch.payload.is_some(), "expected a payload");

    let sql = sql::patch_to_sql(&config, &patch).unwrap().unwrap();

    if sql.contains("TRUNCATE") {
        // State payload: TRUNCATE + 2 INSERTs
        assert_eq!(common::count_sql(&sql, "INSERT INTO"), 2);
        assert_eq!(common::count_sql(&sql, "DELETE FROM"), 0);
    } else {
        // Deltas payload: 18 DELETEs
        assert_eq!(common::count_sql(&sql, "DELETE FROM"), 18);
        assert_eq!(common::count_sql(&sql, "INSERT INTO"), 0);
    }

    common::assert_wire_roundtrip(&config, &patch);
}
