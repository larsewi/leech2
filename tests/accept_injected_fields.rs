mod common;

use leech2::block::Block;
use leech2::config::Config;
use leech2::patch::Patch;
use leech2::sql;
use leech2::utils::GENESIS_HASH;

#[test]
fn test_injected_field_delta_sql() {
    common::init_logging();
    let tmp = tempfile::tempdir().unwrap();
    let work_dir = tmp.path();

    common::write_config(
        work_dir,
        "config.toml",
        r#"
[[injected-fields]]
name = "host"
type = "TEXT"
value = "agent-1"

[tables.users]
source = "users.csv"
fields = [
    { name = "id", type = "NUMBER", primary-key = true },
    { name = "name", type = "TEXT" },
]
"#,
    );

    // Block 1: initial data (many rows so delta is smaller than state)
    common::write_csv(
        work_dir,
        "users.csv",
        "1,Alice\n2,Bob\n3,Charlie\n4,Dave\n5,Eve\n6,Frank\n7,Grace\n8,Heidi\n",
    );
    let config = Config::load(work_dir).unwrap();
    let hash1 = Block::create(&config).unwrap();

    // Block 2: update Alice->Alicia, delete Bob, insert Ivan
    common::write_csv(
        work_dir,
        "users.csv",
        "1,Alicia\n3,Charlie\n4,Dave\n5,Eve\n6,Frank\n7,Grace\n8,Heidi\n9,Ivan\n",
    );
    Block::create(&config).unwrap();

    // Patch from hash1: 1 insert, 1 delete, 1 update — all with injected field
    let patch = Patch::create(&config, &hash1).unwrap();
    let sql = sql::patch_to_sql(&config, &patch).unwrap().unwrap();

    assert!(
        sql.contains(
            r#"INSERT INTO "users" ("host", "id", "name") VALUES ('agent-1', 9, 'Ivan');"#
        )
    );
    assert!(sql.contains(r#"DELETE FROM "users" WHERE "id" = 2 AND "host" = 'agent-1';"#));
    assert!(sql.contains(
        r#"UPDATE "users" SET "name" = 'Alicia' WHERE "id" = 1 AND "host" = 'agent-1';"#
    ));

    common::assert_wire_roundtrip(&config, &patch);
}

#[test]
fn test_injected_field_state_sql() {
    common::init_logging();
    let tmp = tempfile::tempdir().unwrap();
    let work_dir = tmp.path();

    common::write_config(
        work_dir,
        "config.toml",
        r#"
[[injected-fields]]
name = "host"
type = "TEXT"
value = "agent-1"

[tables.users]
source = "users.csv"
fields = [
    { name = "id", type = "NUMBER", primary-key = true },
    { name = "name", type = "TEXT" },
]
"#,
    );

    // Create a single row so the state is small enough to be used as payload
    common::write_csv(work_dir, "users.csv", "1,Alice\n");
    let config = Config::load(work_dir).unwrap();
    Block::create(&config).unwrap();

    // With only one row, the state snapshot is smaller than the delta, so the
    // patch will use the state payload path.
    let patch = Patch::create(&config, GENESIS_HASH).unwrap();
    let sql = sql::patch_to_sql(&config, &patch).unwrap().unwrap();

    // Regardless of delta vs state payload, injected field should be present
    assert!(sql.contains(r#""host""#), "SQL should contain host column");
    assert!(sql.contains("'agent-1'"), "SQL should contain host value");

    // With injected fields, should use DELETE WHERE instead of TRUNCATE
    assert!(
        !sql.contains("TRUNCATE"),
        "With injected fields, state payload should use DELETE WHERE, not TRUNCATE"
    );

    common::assert_wire_roundtrip(&config, &patch);
}

#[test]
fn test_no_injected_fields_unchanged_sql() {
    common::init_logging();
    let tmp = tempfile::tempdir().unwrap();
    let work_dir = tmp.path();

    // No [[injected-fields]] section
    common::write_config(
        work_dir,
        "config.toml",
        r#"
[tables.users]
source = "users.csv"
fields = [
    { name = "id", type = "NUMBER", primary-key = true },
    { name = "name", type = "TEXT" },
]
"#,
    );

    common::write_csv(work_dir, "users.csv", "1,Alice\n");
    let config = Config::load(work_dir).unwrap();
    Block::create(&config).unwrap();

    let patch = Patch::create(&config, GENESIS_HASH).unwrap();
    let sql = sql::patch_to_sql(&config, &patch).unwrap().unwrap();

    // Without injected fields, SQL should not contain any host column
    assert!(
        !sql.contains(r#""host""#),
        "SQL should not contain host column when not configured"
    );
}

#[test]
fn test_injected_field_integer_type() {
    common::init_logging();
    let tmp = tempfile::tempdir().unwrap();
    let work_dir = tmp.path();

    common::write_config(
        work_dir,
        "config.toml",
        r#"
[[injected-fields]]
name = "agent_id"
type = "NUMBER"
value = "42"

[tables.users]
source = "users.csv"
fields = [
    { name = "id", type = "NUMBER", primary-key = true },
    { name = "name", type = "TEXT" },
]
"#,
    );

    common::write_csv(work_dir, "users.csv", "1,Alice\n");
    let config = Config::load(work_dir).unwrap();
    Block::create(&config).unwrap();

    let patch = Patch::create(&config, GENESIS_HASH).unwrap();
    let sql = sql::patch_to_sql(&config, &patch).unwrap().unwrap();

    // Integer injected field should not be quoted
    assert!(
        sql.contains(r#""agent_id""#),
        "SQL should contain agent_id column"
    );
    assert!(sql.contains("42"), "SQL should contain integer value");
}

#[test]
fn test_multiple_injected_fields() {
    common::init_logging();
    let tmp = tempfile::tempdir().unwrap();
    let work_dir = tmp.path();

    common::write_config(
        work_dir,
        "config.toml",
        r#"
[[injected-fields]]
name = "host"
type = "TEXT"
value = "agent-1"

[[injected-fields]]
name = "environment"
type = "TEXT"
value = "production"

[tables.users]
source = "users.csv"
fields = [
    { name = "id", type = "NUMBER", primary-key = true },
    { name = "name", type = "TEXT" },
]
"#,
    );

    // Block 1: initial data (many rows so delta is smaller than state)
    common::write_csv(
        work_dir,
        "users.csv",
        "1,Alice\n2,Bob\n3,Charlie\n4,Dave\n5,Eve\n6,Frank\n7,Grace\n8,Heidi\n",
    );
    let config = Config::load(work_dir).unwrap();
    let hash1 = Block::create(&config).unwrap();

    // Block 2: update Alice->Alicia, delete Bob, insert Ivan
    common::write_csv(
        work_dir,
        "users.csv",
        "1,Alicia\n3,Charlie\n4,Dave\n5,Eve\n6,Frank\n7,Grace\n8,Heidi\n9,Ivan\n",
    );
    Block::create(&config).unwrap();

    let patch = Patch::create(&config, &hash1).unwrap();
    let sql = sql::patch_to_sql(&config, &patch).unwrap().unwrap();

    // INSERT should have both injected columns prepended
    assert!(sql.contains(
        r#"INSERT INTO "users" ("host", "environment", "id", "name") VALUES ('agent-1', 'production', 9, 'Ivan');"#
    ));

    // DELETE should have both injected fields in WHERE
    assert!(sql.contains(
        r#"DELETE FROM "users" WHERE "id" = 2 AND "host" = 'agent-1' AND "environment" = 'production';"#
    ));

    // UPDATE should have both injected fields in WHERE
    assert!(sql.contains(
        r#"UPDATE "users" SET "name" = 'Alicia' WHERE "id" = 1 AND "host" = 'agent-1' AND "environment" = 'production';"#
    ));

    common::assert_wire_roundtrip(&config, &patch);
}

#[test]
fn test_runtime_inject_without_static_fields() {
    common::init_logging();
    let tmp = tempfile::tempdir().unwrap();
    let work_dir = tmp.path();

    common::write_config(
        work_dir,
        "config.toml",
        r#"
[tables.users]
source = "users.csv"
fields = [
    { name = "id", type = "NUMBER", primary-key = true },
    { name = "name", type = "TEXT" },
]
"#,
    );

    common::write_csv(work_dir, "users.csv", "1,Alice\n");
    let config = Config::load(work_dir).unwrap();
    Block::create(&config).unwrap();

    let mut patch = Patch::create(&config, GENESIS_HASH).unwrap();
    patch.inject_field("hostkey", "abc123", "TEXT").unwrap();

    let sql = sql::patch_to_sql(&config, &patch).unwrap().unwrap();

    assert!(
        sql.contains(r#""hostkey""#),
        "SQL should contain hostkey column"
    );
    assert!(sql.contains("'abc123'"), "SQL should contain hostkey value");
    // Runtime injection should trigger the same state-payload partitioning as
    // static injection: DELETE WHERE instead of TRUNCATE.
    assert!(
        !sql.contains("TRUNCATE"),
        "With a runtime-injected field, state payload should use DELETE WHERE"
    );

    common::assert_wire_roundtrip(&config, &patch);
}

#[test]
fn test_runtime_inject_appends_alongside_static() {
    common::init_logging();
    let tmp = tempfile::tempdir().unwrap();
    let work_dir = tmp.path();

    common::write_config(
        work_dir,
        "config.toml",
        r#"
[[injected-fields]]
name = "host"
type = "TEXT"
value = "agent-1"

[tables.users]
source = "users.csv"
fields = [
    { name = "id", type = "NUMBER", primary-key = true },
    { name = "name", type = "TEXT" },
]
"#,
    );

    common::write_csv(work_dir, "users.csv", "1,Alice\n");
    let config = Config::load(work_dir).unwrap();
    Block::create(&config).unwrap();

    let mut patch = Patch::create(&config, GENESIS_HASH).unwrap();
    patch.inject_field("hub_id", "hub-1", "TEXT").unwrap();

    let sql = sql::patch_to_sql(&config, &patch).unwrap().unwrap();

    // Static field remains present.
    assert!(
        sql.contains(r#""host""#) && sql.contains("'agent-1'"),
        "SQL should contain the statically declared host field"
    );
    // Runtime-injected field is also present.
    assert!(
        sql.contains(r#""hub_id""#) && sql.contains("'hub-1'"),
        "SQL should contain the runtime-injected hub_id field"
    );

    common::assert_wire_roundtrip(&config, &patch);
}

#[test]
fn test_runtime_inject_overrides_static_value() {
    common::init_logging();
    let tmp = tempfile::tempdir().unwrap();
    let work_dir = tmp.path();

    common::write_config(
        work_dir,
        "config.toml",
        r#"
[[injected-fields]]
name = "host"
type = "TEXT"
value = "agent-claimed"

[tables.users]
source = "users.csv"
fields = [
    { name = "id", type = "NUMBER", primary-key = true },
    { name = "name", type = "TEXT" },
]
"#,
    );

    common::write_csv(work_dir, "users.csv", "1,Alice\n");
    let config = Config::load(work_dir).unwrap();
    Block::create(&config).unwrap();

    let mut patch = Patch::create(&config, GENESIS_HASH).unwrap();
    patch.inject_field("host", "hub-verified", "TEXT").unwrap();

    let sql = sql::patch_to_sql(&config, &patch).unwrap().unwrap();

    assert!(
        sql.contains("'hub-verified'"),
        "SQL should contain the runtime-injected host value"
    );
    assert!(
        !sql.contains("'agent-claimed'"),
        "SQL should not contain the overridden static host value"
    );

    common::assert_wire_roundtrip(&config, &patch);
}

#[test]
fn test_multiple_injected_fields_state_sql() {
    common::init_logging();
    let tmp = tempfile::tempdir().unwrap();
    let work_dir = tmp.path();

    common::write_config(
        work_dir,
        "config.toml",
        r#"
[[injected-fields]]
name = "host"
type = "TEXT"
value = "agent-1"

[[injected-fields]]
name = "environment"
type = "TEXT"
value = "production"

[tables.users]
source = "users.csv"
fields = [
    { name = "id", type = "NUMBER", primary-key = true },
    { name = "name", type = "TEXT" },
]
"#,
    );

    common::write_csv(work_dir, "users.csv", "1,Alice\n");
    let config = Config::load(work_dir).unwrap();
    Block::create(&config).unwrap();

    let patch = Patch::create(&config, GENESIS_HASH).unwrap();
    let sql = sql::patch_to_sql(&config, &patch).unwrap().unwrap();

    // Should use DELETE WHERE with both conditions instead of TRUNCATE
    assert!(
        !sql.contains("TRUNCATE"),
        "With injected fields, should use DELETE WHERE, not TRUNCATE"
    );
    assert!(sql.contains(
        r#"DELETE FROM "users" WHERE "host" = 'agent-1' AND "environment" = 'production';"#
    ));

    common::assert_wire_roundtrip(&config, &patch);
}
