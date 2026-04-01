mod common;

use leech2::block::Block;
use leech2::config::Config;
use leech2::patch::Patch;
use leech2::sql;
use leech2::utils::GENESIS_HASH;

#[test]
fn test_filter_max_field_length() {
    common::init_logging();
    let tmp = tempfile::tempdir().unwrap();
    let work_dir = tmp.path();

    common::write_config(
        work_dir,
        "config.toml",
        r#"
[filters]
max-field-length = 5

[tables.users]
source = "users.csv"
fields = [
    { name = "id", type = "NUMBER", primary-key = true },
    { name = "name", type = "TEXT" },
]
"#,
    );

    // "Roberto" (7 chars) exceeds max-field-length of 5
    common::write_csv(work_dir, "users.csv", "1,Alice\n2,Roberto\n3,Bob\n");
    let config = Config::load(work_dir).unwrap();
    Block::create(&config).unwrap();

    let patch = Patch::create(&config, GENESIS_HASH).unwrap();
    let sql = sql::patch_to_sql(&config, &patch).unwrap().unwrap();

    common::assert_sql_statements(
        &sql,
        &[
            r#"TRUNCATE "users";"#,
            r#"INSERT INTO "users" ("id", "name") VALUES (1, 'Alice');"#,
            r#"INSERT INTO "users" ("id", "name") VALUES (3, 'Bob');"#,
        ],
    );
}

#[test]
fn test_filter_exclude_equals() {
    common::init_logging();
    let tmp = tempfile::tempdir().unwrap();
    let work_dir = tmp.path();

    common::write_config(
        work_dir,
        "config.toml",
        r#"
[[filters.exclude]]
field = "status"
equals = "inactive"

[tables.users]
source = "users.csv"
fields = [
    { name = "id", type = "NUMBER", primary-key = true },
    { name = "name", type = "TEXT" },
    { name = "status", type = "TEXT" },
]
"#,
    );

    common::write_csv(
        work_dir,
        "users.csv",
        "1,Alice,active\n2,Bob,inactive\n3,Charlie,active\n",
    );
    let config = Config::load(work_dir).unwrap();
    Block::create(&config).unwrap();

    let patch = Patch::create(&config, GENESIS_HASH).unwrap();
    let sql = sql::patch_to_sql(&config, &patch).unwrap().unwrap();

    common::assert_sql_statements(
        &sql,
        &[
            r#"TRUNCATE "users";"#,
            r#"INSERT INTO "users" ("id", "name", "status") VALUES (1, 'Alice', 'active');"#,
            r#"INSERT INTO "users" ("id", "name", "status") VALUES (3, 'Charlie', 'active');"#,
        ],
    );
}

#[test]
fn test_filter_exclude_contains() {
    common::init_logging();
    let tmp = tempfile::tempdir().unwrap();
    let work_dir = tmp.path();

    common::write_config(
        work_dir,
        "config.toml",
        r#"
[[filters.exclude]]
field = "description"
contains = "DEPRECATED"

[tables.items]
source = "items.csv"
fields = [
    { name = "id", type = "NUMBER", primary-key = true },
    { name = "description", type = "TEXT" },
]
"#,
    );

    common::write_csv(
        work_dir,
        "items.csv",
        "1,Active item\n2,DEPRECATED old item\n3,Another item\n",
    );
    let config = Config::load(work_dir).unwrap();
    Block::create(&config).unwrap();

    let patch = Patch::create(&config, GENESIS_HASH).unwrap();
    let sql = sql::patch_to_sql(&config, &patch).unwrap().unwrap();

    common::assert_sql_statements(
        &sql,
        &[
            r#"TRUNCATE "items";"#,
            r#"INSERT INTO "items" ("id", "description") VALUES (1, 'Active item');"#,
            r#"INSERT INTO "items" ("id", "description") VALUES (3, 'Another item');"#,
        ],
    );
}

#[test]
fn test_filter_exclude_scoped_to_table() {
    common::init_logging();
    let tmp = tempfile::tempdir().unwrap();
    let work_dir = tmp.path();

    common::write_config(
        work_dir,
        "config.toml",
        r#"
[[filters.exclude]]
tables = ["users"]
field = "status"
equals = "inactive"

[tables.users]
source = "users.csv"
fields = [
    { name = "id", type = "NUMBER", primary-key = true },
    { name = "status", type = "TEXT" },
]

[tables.orders]
source = "orders.csv"
fields = [
    { name = "id", type = "NUMBER", primary-key = true },
    { name = "status", type = "TEXT" },
]
"#,
    );

    common::write_csv(work_dir, "users.csv", "1,inactive\n2,active\n");
    common::write_csv(work_dir, "orders.csv", "10,inactive\n20,active\n");
    let config = Config::load(work_dir).unwrap();
    Block::create(&config).unwrap();

    let patch = Patch::create(&config, GENESIS_HASH).unwrap();
    let sql = sql::patch_to_sql(&config, &patch).unwrap().unwrap();

    // User id=1 should be filtered, but order id=10 should NOT (rule scoped to "users")
    assert!(!sql.contains("VALUES (1, 'inactive')") || sql.contains(r#""orders""#));
    assert!(sql.contains(r#"INSERT INTO "users" ("id", "status") VALUES (2, 'active');"#));
    assert!(sql.contains(r#"INSERT INTO "orders" ("id", "status") VALUES (10, 'inactive');"#));
    assert!(sql.contains(r#"INSERT INTO "orders" ("id", "status") VALUES (20, 'active');"#));
}

#[test]
fn test_filter_produces_delete_when_record_starts_matching() {
    common::init_logging();
    let tmp = tempfile::tempdir().unwrap();
    let work_dir = tmp.path();

    common::write_config(
        work_dir,
        "config.toml",
        r#"
[[filters.exclude]]
field = "status"
equals = "inactive"

[tables.users]
source = "users.csv"
fields = [
    { name = "id", type = "NUMBER", primary-key = true },
    { name = "status", type = "TEXT" },
]
"#,
    );

    // Block 1: both records pass the filter
    common::write_csv(work_dir, "users.csv", "1,active\n2,active\n");
    let config = Config::load(work_dir).unwrap();
    let hash1 = Block::create(&config).unwrap();

    // Block 2: user 2 becomes inactive (now filtered out)
    common::write_csv(work_dir, "users.csv", "1,active\n2,inactive\n");
    Block::create(&config).unwrap();

    let patch = Patch::create(&config, &hash1).unwrap();
    let sql = sql::patch_to_sql(&config, &patch).unwrap().unwrap();

    // User 2 should appear as a DELETE (was in state, now filtered out)
    assert!(sql.contains(r#"DELETE FROM "users" WHERE "id" = 2;"#));
}

#[test]
fn test_filter_produces_insert_when_record_stops_matching() {
    common::init_logging();
    let tmp = tempfile::tempdir().unwrap();
    let work_dir = tmp.path();

    common::write_config(
        work_dir,
        "config.toml",
        r#"
[[filters.exclude]]
field = "status"
equals = "inactive"

[tables.users]
source = "users.csv"
fields = [
    { name = "id", type = "NUMBER", primary-key = true },
    { name = "status", type = "TEXT" },
]
"#,
    );

    // Block 1: user 2 is inactive (filtered out)
    common::write_csv(work_dir, "users.csv", "1,active\n2,inactive\n");
    let config = Config::load(work_dir).unwrap();
    let hash1 = Block::create(&config).unwrap();

    // Block 2: user 2 becomes active (passes filter)
    common::write_csv(work_dir, "users.csv", "1,active\n2,active\n");
    Block::create(&config).unwrap();

    let patch = Patch::create(&config, &hash1).unwrap();
    let sql = sql::patch_to_sql(&config, &patch).unwrap().unwrap();

    // User 2 should appear as an INSERT (was not in state, now included)
    assert!(sql.contains(r#"INSERT INTO "users" ("id", "status") VALUES (2, 'active');"#));
}
