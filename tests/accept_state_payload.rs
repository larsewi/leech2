mod common;

use leech2::block::Block;
use leech2::config::Config;
use leech2::patch::Patch;
use leech2::sql;
use leech2::utils::GENESIS_HASH;

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
    { name = "id", type = "NUMBER", primary-key = true },
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
    assert!(
        !patch.deltas.is_empty() || !patch.states.is_empty(),
        "expected a payload"
    );

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

/// When one table has many deletes (state is smaller) and another has a small
/// change (delta is smaller), the patch should contain both deltas and states.
#[test]
fn test_mixed_payload_deltas_and_states() {
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
]

[tables.logs]
source = "logs.csv"
fields = [
    { name = "seq", type = "NUMBER", primary-key = true },
    { name = "message", type = "TEXT" },
]
"#,
    );

    // Block 1: items has 20 rows, logs has 20 rows.
    let mut items_csv = String::new();
    let mut logs_csv = String::new();
    for i in 1..=20 {
        items_csv.push_str(&format!("{},item{}\n", i, i));
        logs_csv.push_str(&format!("{},log message number {}\n", i, i));
    }
    common::write_csv(work_dir, "items.csv", &items_csv);
    common::write_csv(work_dir, "logs.csv", &logs_csv);
    let config = Config::load(work_dir).unwrap();
    let hash1 = Block::create(&config).unwrap();

    // Block 2: items drops to 2 rows (18 deletes → state wins),
    //          logs adds 1 row (1 insert → delta wins).
    common::write_csv(work_dir, "items.csv", "1,item1\n2,item2\n");
    logs_csv.push_str("21,log message number 21\n");
    common::write_csv(work_dir, "logs.csv", &logs_csv);
    let _hash2 = Block::create(&config).unwrap();

    let patch = Patch::create(&config, &hash1).unwrap();
    assert_eq!(patch.num_blocks, 1);

    // Verify both fields are populated (mixed payload).
    assert!(
        !patch.deltas.is_empty(),
        "expected deltas for the logs table"
    );
    assert!(
        !patch.states.is_empty(),
        "expected state for the items table"
    );

    // Verify which table ended up where.
    let delta_tables: Vec<&str> = patch.deltas.keys().map(|n| n.as_str()).collect();
    let state_tables: Vec<&str> = patch.states.keys().map(|n| n.as_str()).collect();
    assert!(
        delta_tables.contains(&"logs"),
        "logs should use delta, got deltas={:?} states={:?}",
        delta_tables,
        state_tables
    );
    assert!(
        state_tables.contains(&"items"),
        "items should use state, got deltas={:?} states={:?}",
        delta_tables,
        state_tables
    );

    // Verify SQL contains both delta and state patterns.
    let sql = sql::patch_to_sql(&config, &patch).unwrap().unwrap();
    assert!(sql.starts_with("BEGIN;\n"));
    assert!(sql.ends_with("COMMIT;\n"));

    // logs: delta path → 1 INSERT, no TRUNCATE for logs
    assert!(sql.contains(r#"INSERT INTO "logs""#));

    // items: state path → TRUNCATE + 2 INSERTs
    assert!(sql.contains(r#"TRUNCATE "items";"#));
    assert_eq!(common::count_sql(&sql, r#"INSERT INTO "items""#), 2);

    common::assert_wire_roundtrip(&config, &patch);

    // Full state from genesis should put all tables in states.
    let patch_genesis = Patch::create(&config, GENESIS_HASH).unwrap();
    assert!(patch_genesis.deltas.is_empty());
    assert_eq!(patch_genesis.states.len(), 2);

    common::assert_wire_roundtrip(&config, &patch_genesis);
}
