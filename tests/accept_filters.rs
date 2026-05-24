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
[tables.users]
fields = [
    { name = "id", type = "NUMBER", primary-key = true },
    { name = "name", type = "TEXT" },
]

[tables.users.csv]
source = "users.csv"
max-field-length = 5
"#,
    );

    // "Roberto" (7 chars) exceeds max-field-length of 5
    common::write_csv(work_dir, "users.csv", "1,Alice\n2,Roberto\n3,Bob\n");
    let config = Config::load(work_dir).unwrap();
    Block::create(&config, None).unwrap();

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
fn test_filter_exclude_anchored_regex() {
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
    { name = "status", type = "TEXT" },
]

[tables.users.csv]
source = "users.csv"

[tables.users.csv.filter]
fields = ["status"]
exclude = "^inactive$"
"#,
    );

    common::write_csv(
        work_dir,
        "users.csv",
        "1,Alice,active\n2,Bob,inactive\n3,Charlie,active\n",
    );
    let config = Config::load(work_dir).unwrap();
    Block::create(&config, None).unwrap();

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
fn test_filter_exclude_unanchored_regex() {
    common::init_logging();
    let tmp = tempfile::tempdir().unwrap();
    let work_dir = tmp.path();

    common::write_config(
        work_dir,
        "config.toml",
        r#"
[tables.items]
fields = [
    { name = "id", type = "NUMBER", primary-key = true },
    { name = "description", type = "TEXT" },
]

[tables.items.csv]
source = "items.csv"

[tables.items.csv.filter]
fields = ["description"]
exclude = "DEPRECATED"
"#,
    );

    common::write_csv(
        work_dir,
        "items.csv",
        "1,Active item\n2,DEPRECATED old item\n3,Another item\n",
    );
    let config = Config::load(work_dir).unwrap();
    Block::create(&config, None).unwrap();

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

/// Each table's filter is structurally scoped to that table, so one table's
/// exclude has no effect on a sibling table that lacks its own filter.
#[test]
fn test_filter_only_applies_to_owning_table() {
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
    { name = "status", type = "TEXT" },
]

[tables.users.csv]
source = "users.csv"

[tables.users.csv.filter]
fields = ["status"]
exclude = "^inactive$"

[tables.orders]
fields = [
    { name = "id", type = "NUMBER", primary-key = true },
    { name = "status", type = "TEXT" },
]

[tables.orders.csv]
source = "orders.csv"
"#,
    );

    common::write_csv(work_dir, "users.csv", "1,inactive\n2,active\n");
    common::write_csv(work_dir, "orders.csv", "10,inactive\n20,active\n");
    let config = Config::load(work_dir).unwrap();
    Block::create(&config, None).unwrap();

    let patch = Patch::create(&config, GENESIS_HASH).unwrap();
    let sql = sql::patch_to_sql(&config, &patch).unwrap().unwrap();

    // users id=1 is filtered out; orders is unaffected.
    assert!(!sql.contains(r#"INSERT INTO "users" ("id", "status") VALUES (1, 'inactive');"#));
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
[tables.users]
fields = [
    { name = "id", type = "NUMBER", primary-key = true },
    { name = "status", type = "TEXT" },
]

[tables.users.csv]
source = "users.csv"

[tables.users.csv.filter]
fields = ["status"]
exclude = "^inactive$"
"#,
    );

    // Block 1: both records pass the filter
    common::write_csv(work_dir, "users.csv", "1,active\n2,active\n");
    let config = Config::load(work_dir).unwrap();
    let hash1 = Block::create(&config, None).unwrap();

    // Block 2: user 2 becomes inactive (now filtered out)
    common::write_csv(work_dir, "users.csv", "1,active\n2,inactive\n");
    Block::create(&config, None).unwrap();

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
[tables.users]
fields = [
    { name = "id", type = "NUMBER", primary-key = true },
    { name = "status", type = "TEXT" },
]

[tables.users.csv]
source = "users.csv"

[tables.users.csv.filter]
fields = ["status"]
exclude = "^inactive$"
"#,
    );

    // Block 1: user 2 is inactive (filtered out)
    common::write_csv(work_dir, "users.csv", "1,active\n2,inactive\n");
    let config = Config::load(work_dir).unwrap();
    let hash1 = Block::create(&config, None).unwrap();

    // Block 2: user 2 becomes active (passes filter)
    common::write_csv(work_dir, "users.csv", "1,active\n2,active\n");
    Block::create(&config, None).unwrap();

    let patch = Patch::create(&config, &hash1).unwrap();
    let sql = sql::patch_to_sql(&config, &patch).unwrap().unwrap();

    // User 2 should appear as an INSERT (was not in state, now included)
    assert!(sql.contains(r#"INSERT INTO "users" ("id", "status") VALUES (2, 'active');"#));
}

#[test]
fn test_filter_include_keeps_only_matching_records() {
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
    { name = "status", type = "TEXT" },
]

[tables.users.csv]
source = "users.csv"

[tables.users.csv.filter]
fields = ["status"]
include = "^(active|pending)$"
"#,
    );

    common::write_csv(
        work_dir,
        "users.csv",
        "1,Alice,active\n2,Bob,inactive\n3,Charlie,pending\n4,Dave,archived\n",
    );
    let config = Config::load(work_dir).unwrap();
    Block::create(&config, None).unwrap();

    let patch = Patch::create(&config, GENESIS_HASH).unwrap();
    let sql = sql::patch_to_sql(&config, &patch).unwrap().unwrap();

    common::assert_sql_statements(
        &sql,
        &[
            r#"TRUNCATE "users";"#,
            r#"INSERT INTO "users" ("id", "name", "status") VALUES (1, 'Alice', 'active');"#,
            r#"INSERT INTO "users" ("id", "name", "status") VALUES (3, 'Charlie', 'pending');"#,
        ],
    );
}

#[test]
fn test_filter_include_unanchored_regex() {
    common::init_logging();
    let tmp = tempfile::tempdir().unwrap();
    let work_dir = tmp.path();

    common::write_config(
        work_dir,
        "config.toml",
        r#"
[tables.items]
fields = [
    { name = "id", type = "NUMBER", primary-key = true },
    { name = "description", type = "TEXT" },
]

[tables.items.csv]
source = "items.csv"

[tables.items.csv.filter]
fields = ["description"]
include = "PRODUCTION"
"#,
    );

    common::write_csv(
        work_dir,
        "items.csv",
        "1,PRODUCTION ready\n2,draft item\n3,PRODUCTION-grade hardware\n",
    );
    let config = Config::load(work_dir).unwrap();
    Block::create(&config, None).unwrap();

    let patch = Patch::create(&config, GENESIS_HASH).unwrap();
    let sql = sql::patch_to_sql(&config, &patch).unwrap().unwrap();

    common::assert_sql_statements(
        &sql,
        &[
            r#"TRUNCATE "items";"#,
            r#"INSERT INTO "items" ("id", "description") VALUES (1, 'PRODUCTION ready');"#,
            r#"INSERT INTO "items" ("id", "description") VALUES (3, 'PRODUCTION-grade hardware');"#,
        ],
    );
}

/// Multiple fields in a filter combine with OR: a record passes the include
/// check if at least one listed field matches the pattern.
#[test]
fn test_filter_include_or_across_fields() {
    common::init_logging();
    let tmp = tempfile::tempdir().unwrap();
    let work_dir = tmp.path();

    common::write_config(
        work_dir,
        "config.toml",
        r#"
[tables.items]
fields = [
    { name = "id", type = "NUMBER", primary-key = true },
    { name = "primary_tag", type = "TEXT" },
    { name = "fallback_tag", type = "TEXT" },
]

[tables.items.csv]
source = "items.csv"

[tables.items.csv.filter]
fields = ["primary_tag", "fallback_tag"]
include = "^active$"
"#,
    );

    // CSV columns are positional: col 0 = id, col 1 = primary_tag, col 2 = fallback_tag.
    // Row 1: primary_tag matches; row 2: fallback_tag matches; row 3: neither matches.
    common::write_csv(
        work_dir,
        "items.csv",
        "1,active,old\n\
         2,old,active\n\
         3,old,old\n",
    );
    let config = Config::load(work_dir).unwrap();
    Block::create(&config, None).unwrap();

    let patch = Patch::create(&config, GENESIS_HASH).unwrap();
    let sql = sql::patch_to_sql(&config, &patch).unwrap().unwrap();

    // SQL emits columns in canonical (alphabetical) order:
    // id (PK), fallback_tag, primary_tag.
    assert!(sql.contains(r#"VALUES (1, 'old', 'active');"#));
    assert!(sql.contains(r#"VALUES (2, 'active', 'old');"#));
    assert!(!sql.contains(r#"VALUES (3, 'old', 'old');"#));
}

#[test]
fn test_filter_exclude_wins_over_include() {
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
    { name = "status", type = "TEXT" },
]

[tables.users.csv]
source = "users.csv"

[tables.users.csv.filter]
fields = ["status"]
include = "^(active|pending)$"
exclude = "^pending$"
"#,
    );

    common::write_csv(work_dir, "users.csv", "1,active\n2,pending\n3,inactive\n");
    let config = Config::load(work_dir).unwrap();
    Block::create(&config, None).unwrap();

    let patch = Patch::create(&config, GENESIS_HASH).unwrap();
    let sql = sql::patch_to_sql(&config, &patch).unwrap().unwrap();

    // Only "active" survives: "pending" is dropped by exclude (overlap), "inactive" by include.
    common::assert_sql_statements(
        &sql,
        &[
            r#"TRUNCATE "users";"#,
            r#"INSERT INTO "users" ("id", "status") VALUES (1, 'active');"#,
        ],
    );
}

#[test]
fn test_filter_produces_delete_when_record_stops_matching_include() {
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
    { name = "status", type = "TEXT" },
]

[tables.users.csv]
source = "users.csv"

[tables.users.csv.filter]
fields = ["status"]
include = "^active$"
"#,
    );

    // Block 1: both records match the include rule
    common::write_csv(work_dir, "users.csv", "1,active\n2,active\n");
    let config = Config::load(work_dir).unwrap();
    let hash1 = Block::create(&config, None).unwrap();

    // Block 2: user 2 stops matching the include rule (now filtered out)
    common::write_csv(work_dir, "users.csv", "1,active\n2,inactive\n");
    Block::create(&config, None).unwrap();

    let patch = Patch::create(&config, &hash1).unwrap();
    let sql = sql::patch_to_sql(&config, &patch).unwrap().unwrap();

    // User 2 should appear as a DELETE (was in state, now filtered out)
    assert!(sql.contains(r#"DELETE FROM "users" WHERE "id" = 2;"#));
}

#[test]
fn test_filter_produces_insert_when_record_starts_matching_include() {
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
    { name = "status", type = "TEXT" },
]

[tables.users.csv]
source = "users.csv"

[tables.users.csv.filter]
fields = ["status"]
include = "^active$"
"#,
    );

    // Block 1: user 2 doesn't match the include rule (filtered out)
    common::write_csv(work_dir, "users.csv", "1,active\n2,inactive\n");
    let config = Config::load(work_dir).unwrap();
    let hash1 = Block::create(&config, None).unwrap();

    // Block 2: user 2 starts matching the include rule (now passes)
    common::write_csv(work_dir, "users.csv", "1,active\n2,active\n");
    Block::create(&config, None).unwrap();

    let patch = Patch::create(&config, &hash1).unwrap();
    let sql = sql::patch_to_sql(&config, &patch).unwrap().unwrap();

    // User 2 should appear as an INSERT (was not in state, now included)
    assert!(sql.contains(r#"INSERT INTO "users" ("id", "status") VALUES (2, 'active');"#));
}
