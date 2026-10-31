mod common;

use leech2::block::Block;
use leech2::cell::Cell;
use leech2::config::Config;
use leech2::patch::Patch;
use leech2::sql::{self, InjectedField};
use leech2::utils::GENESIS_HASH;

const CONFIG: &str = r#"
[tables.users]
fields = [
    { name = "id", type = "NUMBER", primary-key = true },
    { name = "name", type = "TEXT" },
]

[tables.users.csv]
source = "users.csv"
"#;

#[test]
fn test_injected_field_delta_sql() {
    let tmp = tempfile::tempdir().unwrap();
    let work_dir = tmp.path();

    common::write_file(work_dir, "config.toml", CONFIG);

    // Block 1: initial data (many rows so delta is smaller than state)
    common::write_csv!(
        work_dir,
        "users.csv",
        "1,Alice",
        "2,Bob",
        "3,Charlie",
        "4,Dave",
        "5,Eve",
        "6,Frank",
        "7,Grace",
        "8,Heidi",
    );
    let config = Config::load(work_dir).unwrap();
    let hash1 = Block::create(&config, None).unwrap();

    // Block 2: update Alice->Alicia, delete Bob, insert Ivan
    common::write_csv!(
        work_dir,
        "users.csv",
        "1,Alicia",
        "3,Charlie",
        "4,Dave",
        "5,Eve",
        "6,Frank",
        "7,Grace",
        "8,Heidi",
        "9,Ivan",
    );
    Block::create(&config, None).unwrap();

    // Patch from hash1: 1 insert, 1 delete, 1 update -- all with injected field
    let patch = Patch::create(&config, &hash1).unwrap();
    let injected = [InjectedField::new("host", Cell::from("agent-1")).unwrap()];
    let sql = sql::patch_to_sql(&config, &patch, &injected)
        .unwrap()
        .unwrap();

    assert!(
        sql.contains(
            &[
                r#"INSERT INTO "users" ("host", "id", "name")"#,
                r#"VALUES ('agent-1', 9, 'Ivan');"#,
            ]
            .join("\n")
        )
    );
    assert!(
        sql.contains(
            &[
                r#"DELETE FROM "users""#,
                r#"WHERE "id" = 2 AND "host" = 'agent-1';"#,
            ]
            .join("\n")
        )
    );
    assert!(
        sql.contains(
            &[
                r#"UPDATE "users""#,
                r#"SET "name" = 'Alicia'"#,
                r#"WHERE "id" = 1 AND "host" = 'agent-1';"#,
            ]
            .join("\n")
        )
    );
}

#[test]
fn test_injected_field_state_sql() {
    let tmp = tempfile::tempdir().unwrap();
    let work_dir = tmp.path();

    common::write_file(work_dir, "config.toml", CONFIG);

    // Create a single row so the state is small enough to be used as payload
    common::write_csv!(work_dir, "users.csv", "1,Alice");
    let config = Config::load(work_dir).unwrap();
    Block::create(&config, None).unwrap();

    // With only one row, the state snapshot is smaller than the delta, so the
    // patch will use the state payload path.
    let patch = Patch::create(&config, GENESIS_HASH).unwrap();
    let injected = [InjectedField::new("host", Cell::from("agent-1")).unwrap()];
    let sql = sql::patch_to_sql(&config, &patch, &injected)
        .unwrap()
        .unwrap();

    // Regardless of delta vs state payload, injected field should be present
    assert!(sql.contains(r#""host""#), "SQL should contain host column");
    assert!(sql.contains("'agent-1'"), "SQL should contain host value");

    // With injected fields, the DELETE is scoped to them
    assert!(sql.contains(&[r#"DELETE FROM "users""#, r#"WHERE "host" = 'agent-1';"#].join("\n")));
}

#[test]
fn test_no_injected_fields_unchanged_sql() {
    let tmp = tempfile::tempdir().unwrap();
    let work_dir = tmp.path();

    common::write_file(work_dir, "config.toml", CONFIG);

    common::write_csv!(work_dir, "users.csv", "1,Alice");
    let config = Config::load(work_dir).unwrap();
    Block::create(&config, None).unwrap();

    let patch = Patch::create(&config, GENESIS_HASH).unwrap();
    let sql = sql::patch_to_sql(&config, &patch, &[]).unwrap().unwrap();

    // Without injected fields, SQL should not contain any host column
    assert!(
        !sql.contains(r#""host""#),
        "SQL should not contain host column when not injected"
    );
}

#[test]
fn test_injected_field_integer_type() {
    let tmp = tempfile::tempdir().unwrap();
    let work_dir = tmp.path();

    common::write_file(work_dir, "config.toml", CONFIG);

    common::write_csv!(work_dir, "users.csv", "1,Alice");
    let config = Config::load(work_dir).unwrap();
    Block::create(&config, None).unwrap();

    let patch = Patch::create(&config, GENESIS_HASH).unwrap();
    let injected = [InjectedField::new("agent_id", Cell::Number(42.0)).unwrap()];
    let sql = sql::patch_to_sql(&config, &patch, &injected)
        .unwrap()
        .unwrap();

    // Integer injected field should not be quoted
    assert!(
        sql.contains(r#""agent_id""#),
        "SQL should contain agent_id column"
    );
    assert!(sql.contains("42"), "SQL should contain integer value");
}

#[test]
fn test_multiple_injected_fields() {
    let tmp = tempfile::tempdir().unwrap();
    let work_dir = tmp.path();

    common::write_file(work_dir, "config.toml", CONFIG);

    // Block 1: initial data (many rows so delta is smaller than state)
    common::write_csv!(
        work_dir,
        "users.csv",
        "1,Alice",
        "2,Bob",
        "3,Charlie",
        "4,Dave",
        "5,Eve",
        "6,Frank",
        "7,Grace",
        "8,Heidi",
    );
    let config = Config::load(work_dir).unwrap();
    let hash1 = Block::create(&config, None).unwrap();

    // Block 2: update Alice->Alicia, delete Bob, insert Ivan
    common::write_csv!(
        work_dir,
        "users.csv",
        "1,Alicia",
        "3,Charlie",
        "4,Dave",
        "5,Eve",
        "6,Frank",
        "7,Grace",
        "8,Heidi",
        "9,Ivan",
    );
    Block::create(&config, None).unwrap();

    let patch = Patch::create(&config, &hash1).unwrap();
    let injected = [
        InjectedField::new("host", Cell::from("agent-1")).unwrap(),
        InjectedField::new("environment", Cell::from("production")).unwrap(),
    ];
    let sql = sql::patch_to_sql(&config, &patch, &injected)
        .unwrap()
        .unwrap();

    // INSERT should have both injected columns prepended
    assert!(
        sql.contains(
            &[
                r#"INSERT INTO "users" ("host", "environment", "id", "name")"#,
                r#"VALUES ('agent-1', 'production', 9, 'Ivan');"#,
            ]
            .join("\n")
        )
    );

    // DELETE should have both injected fields in WHERE
    assert!(
        sql.contains(
            &[
                r#"DELETE FROM "users""#,
                r#"WHERE "id" = 2 AND "host" = 'agent-1' AND "environment" = 'production';"#,
            ]
            .join("\n")
        )
    );

    // UPDATE should have both injected fields in WHERE
    assert!(
        sql.contains(
            &[
                r#"UPDATE "users""#,
                r#"SET "name" = 'Alicia'"#,
                r#"WHERE "id" = 1 AND "host" = 'agent-1' AND "environment" = 'production';"#,
            ]
            .join("\n")
        )
    );
}

#[test]
fn test_multiple_injected_fields_state_sql() {
    let tmp = tempfile::tempdir().unwrap();
    let work_dir = tmp.path();

    common::write_file(work_dir, "config.toml", CONFIG);

    common::write_csv!(work_dir, "users.csv", "1,Alice");
    let config = Config::load(work_dir).unwrap();
    Block::create(&config, None).unwrap();

    let patch = Patch::create(&config, GENESIS_HASH).unwrap();
    let injected = [
        InjectedField::new("host", Cell::from("agent-1")).unwrap(),
        InjectedField::new("environment", Cell::from("production")).unwrap(),
    ];
    let sql = sql::patch_to_sql(&config, &patch, &injected)
        .unwrap()
        .unwrap();

    // Should use DELETE WHERE with both conditions
    assert!(
        sql.contains(
            &[
                r#"DELETE FROM "users""#,
                r#"WHERE "host" = 'agent-1' AND "environment" = 'production';"#,
            ]
            .join("\n")
        )
    );
}
