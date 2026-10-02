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
fields = [
    { name = "id", type = "NUMBER", primary-key = true },
    { name = "name", type = "TEXT" },
]

[tables.items.csv]
source = "items.csv"

[tables.logs]
fields = [
    { name = "seq", type = "NUMBER", primary-key = true },
    { name = "message", type = "TEXT" },
]

[tables.logs.csv]
source = "logs.csv"
"#,
    );

    common::write_csv(work_dir, "items.csv", "1,apple\n2,banana\n");
    common::write_csv(work_dir, "logs.csv", "1,hello\n2,world\n");
    let config = Config::load(work_dir).unwrap();
    let hash1 = Block::create(&config, None).unwrap();

    // Change items config: add a "price" field.
    // logs stays the same but gets a new row.
    common::write_config(
        work_dir,
        "config.toml",
        r#"
[tables.items]
fields = [
    { name = "id", type = "NUMBER", primary-key = true },
    { name = "name", type = "TEXT" },
    { name = "price", type = "NUMBER" },
]

[tables.items.csv]
source = "items.csv"

[tables.logs]
fields = [
    { name = "seq", type = "NUMBER", primary-key = true },
    { name = "message", type = "TEXT" },
]

[tables.logs.csv]
source = "logs.csv"
"#,
    );

    common::write_csv(
        work_dir,
        "items.csv",
        "1,apple,1.50\n2,banana,0.75\n3,cherry,2.00\n",
    );
    common::write_csv(work_dir, "logs.csv", "1,hello\n2,world\n3,new entry\n");
    let config = Config::load(work_dir).unwrap();
    let _hash2 = Block::create(&config, None).unwrap();

    // Patch from hash1: items had a layout change, logs did not.
    let patch = Patch::create(&config, &hash1).unwrap();
    assert_eq!(patch.num_blocks, 1);

    // items should be in states (layout changed -> full state).
    assert!(
        patch.states.contains_key("items"),
        "items should use full state, got deltas={:?} states={:?}",
        patch.deltas.keys().collect::<Vec<_>>(),
        patch.states.keys().collect::<Vec<_>>()
    );

    // logs should be in deltas (unchanged layout -> incremental).
    assert!(
        patch.deltas.contains_key("logs"),
        "logs should use delta, got deltas={:?} states={:?}",
        patch.deltas.keys().collect::<Vec<_>>(),
        patch.states.keys().collect::<Vec<_>>()
    );

    // Verify SQL generation.
    let sql = sql::patch_to_sql(&config, &patch).unwrap().unwrap();

    // items: state path -> TRUNCATE + 3 INSERTs
    assert!(sql.contains(r#"TRUNCATE "items";"#));
    assert_eq!(common::count_sql(&sql, r#"INSERT INTO "items""#), 3);

    // logs: delta path -> 1 INSERT
    assert!(sql.contains(r#"INSERT INTO "logs""#));
    assert_eq!(common::count_sql(&sql, r#"INSERT INTO "logs""#), 1);

    common::assert_wire_roundtrip(&config, &patch);
}

/// A table added to the config should replace whatever the receiver has for
/// it, so the patch should use full state for it even if later blocks carry
/// deltas for it.
#[test]
fn test_new_table_produces_full_state() {
    let tmp = tempfile::tempdir().unwrap();
    let work_dir = tmp.path();

    let items_config = r#"
[tables.items]
fields = [
    { name = "id", type = "NUMBER", primary-key = true },
    { name = "name", type = "TEXT" },
]

[tables.items.csv]
source = "items.csv"
"#;
    let logs_config = r#"
[tables.logs]
fields = [
    { name = "seq", type = "NUMBER", primary-key = true },
    { name = "message", type = "TEXT" },
]

[tables.logs.csv]
source = "logs.csv"
"#;

    // Initial config: items only.
    common::write_config(work_dir, "config.toml", items_config);
    common::write_csv(work_dir, "items.csv", "1,apple\n");
    let config = Config::load(work_dir).unwrap();
    let hash1 = Block::create(&config, None).unwrap();

    // Add logs to the config. items gets a new row.
    common::write_config(
        work_dir,
        "config.toml",
        &format!("{items_config}{logs_config}"),
    );
    common::write_csv(work_dir, "items.csv", "1,apple\n2,banana\n");
    common::write_csv(work_dir, "logs.csv", "1,hello\n2,world\n");
    let config = Config::load(work_dir).unwrap();
    Block::create(&config, None).unwrap();

    // A later block carries a plain delta for logs.
    common::write_csv(work_dir, "logs.csv", "1,hello\n2,world\n3,again\n");
    Block::create(&config, None).unwrap();

    let patch = Patch::create(&config, &hash1).unwrap();
    assert_eq!(patch.num_blocks, 2);
    assert!(patch.states.contains_key("logs"));
    assert!(patch.deltas.contains_key("items"));

    let sql = sql::patch_to_sql(&config, &patch).unwrap().unwrap();

    // logs: state path -> TRUNCATE + 3 INSERTs
    assert!(sql.contains(r#"TRUNCATE "logs";"#));
    assert_eq!(common::count_sql(&sql, r#"INSERT INTO "logs""#), 3);

    // items: delta path -> 1 INSERT
    assert_eq!(common::count_sql(&sql, r#"INSERT INTO "items""#), 1);

    common::assert_wire_roundtrip(&config, &patch);
}
