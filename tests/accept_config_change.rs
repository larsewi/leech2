mod common;

use leech2::block::Block;
use leech2::config::Config;
use leech2::patch::Patch;
use leech2::sql;

/// When a table's field layout changes between blocks, the patch should use
/// full state for that table while keeping deltas for unchanged tables.
#[test]
fn test_config_change_produces_mixed_patch() {
    let tmp = tempfile::tempdir().unwrap();
    let work_dir = tmp.path();

    // Initial config: items (id, name) and logs (seq, message).
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

    common::write_csv(work_dir, "items.csv", "1,apple\n2,banana\n");
    common::write_csv(work_dir, "logs.csv", "1,hello\n2,world\n");
    let config = Config::load(work_dir).unwrap();
    let hash1 = Block::create(&config).unwrap();

    // Change items config: add a "price" field.
    // logs stays the same but gets a new row.
    common::write_config(
        work_dir,
        "config.toml",
        r#"
[tables.items]
source = "items.csv"
fields = [
    { name = "id", type = "NUMBER", primary-key = true },
    { name = "name", type = "TEXT" },
    { name = "price", type = "NUMBER" },
]

[tables.logs]
source = "logs.csv"
fields = [
    { name = "seq", type = "NUMBER", primary-key = true },
    { name = "message", type = "TEXT" },
]
"#,
    );

    common::write_csv(
        work_dir,
        "items.csv",
        "1,apple,1.50\n2,banana,0.75\n3,cherry,2.00\n",
    );
    common::write_csv(work_dir, "logs.csv", "1,hello\n2,world\n3,new entry\n");
    let config = Config::load(work_dir).unwrap();
    let _hash2 = Block::create(&config).unwrap();

    // Patch from hash1: items had a layout change, logs did not.
    let patch = Patch::create(&config, &hash1).unwrap();
    assert_eq!(patch.num_blocks, 1);

    // items should be in states (layout changed → full state).
    assert!(
        patch.states.contains_key("items"),
        "items should use full state, got deltas={:?} states={:?}",
        patch.deltas.keys().collect::<Vec<_>>(),
        patch.states.keys().collect::<Vec<_>>()
    );

    // logs should be in deltas (unchanged layout → incremental).
    assert!(
        patch.deltas.contains_key("logs"),
        "logs should use delta, got deltas={:?} states={:?}",
        patch.deltas.keys().collect::<Vec<_>>(),
        patch.states.keys().collect::<Vec<_>>()
    );

    // Verify field hashes are present for both tables.
    assert!(patch.field_hashes.contains_key("items"));
    assert!(patch.field_hashes.contains_key("logs"));

    // Verify SQL generation.
    let sql = sql::patch_to_sql(&config, &patch).unwrap().unwrap();
    assert!(sql.starts_with("BEGIN;\n"));
    assert!(sql.ends_with("COMMIT;\n"));

    // items: state path → TRUNCATE + 3 INSERTs
    assert!(sql.contains(r#"TRUNCATE "items";"#));
    assert_eq!(common::count_sql(&sql, r#"INSERT INTO "items""#), 3);

    // logs: delta path → 1 INSERT
    assert!(sql.contains(r#"INSERT INTO "logs""#));
    assert_eq!(common::count_sql(&sql, r#"INSERT INTO "logs""#), 1);

    common::assert_wire_roundtrip(&config, &patch);
}
