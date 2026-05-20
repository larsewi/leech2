mod common;

use leech2::block::Block;
use leech2::config::Config;
use leech2::patch::Patch;
use leech2::sql;
use leech2::utils::GENESIS_HASH;

#[test]
fn test_missing_config_file() {
    common::init_logging();
    let tmp = tempfile::tempdir().unwrap();
    let result = Config::load(tmp.path());
    assert!(result.is_err());
    assert!(
        result
            .unwrap_err()
            .to_string()
            .contains("no config file found"),
        "should report missing config"
    );
}

#[test]
fn test_config_no_primary_key() {
    common::init_logging();
    let tmp = tempfile::tempdir().unwrap();
    common::write_config(
        tmp.path(),
        "config.toml",
        r#"
[tables.users]
source = "users.csv"
fields = [
    { name = "id", type = "NUMBER" },
    { name = "name", type = "TEXT" },
]
"#,
    );
    let result = Config::load(tmp.path());
    assert!(result.is_err());
    let error = format!("{:#}", result.unwrap_err());
    assert!(
        error.contains("primary-key"),
        "should report missing primary key"
    );
}

#[test]
fn test_config_duplicate_field_names() {
    common::init_logging();
    let tmp = tempfile::tempdir().unwrap();
    common::write_config(
        tmp.path(),
        "config.toml",
        r#"
[tables.users]
source = "users.csv"
fields = [
    { name = "id", type = "NUMBER", primary-key = true },
    { name = "id", type = "TEXT" },
]
"#,
    );
    let result = Config::load(tmp.path());
    assert!(result.is_err());
    let error = format!("{:#}", result.unwrap_err());
    assert!(
        error.contains("duplicate field name"),
        "should report duplicate field"
    );
}

#[test]
fn test_config_invalid_toml_syntax() {
    common::init_logging();
    let tmp = tempfile::tempdir().unwrap();
    common::write_config(tmp.path(), "config.toml", "this is not valid toml [[[");
    let result = Config::load(tmp.path());
    assert!(result.is_err());
    assert!(
        result
            .unwrap_err()
            .to_string()
            .contains("failed to parse config"),
        "should report parse failure"
    );
}

#[test]
fn test_config_invalid_json_syntax() {
    common::init_logging();
    let tmp = tempfile::tempdir().unwrap();
    common::write_config(tmp.path(), "config.json", "{not valid json}");
    let result = Config::load(tmp.path());
    assert!(result.is_err());
    assert!(
        result
            .unwrap_err()
            .to_string()
            .contains("failed to parse config"),
        "should report parse failure"
    );
}

#[test]
fn test_truncate_config_max_blocks_invalid() {
    common::init_logging();
    let tmp = tempfile::tempdir().unwrap();
    common::write_config(
        tmp.path(),
        "config.toml",
        r#"
[truncate]
max-blocks = 0

[tables.users]
source = "users.csv"
fields = [
    { name = "id", type = "NUMBER", primary-key = true },
    { name = "name", type = "TEXT" },
]
"#,
    );

    let result = Config::load(tmp.path());
    assert!(result.is_err());
    assert!(
        result.unwrap_err().to_string().contains("max-blocks"),
        "should report invalid max-blocks"
    );
}

#[test]
fn test_truncate_config_max_age_invalid() {
    common::init_logging();
    let tmp = tempfile::tempdir().unwrap();
    common::write_config(
        tmp.path(),
        "config.toml",
        r#"
[truncate]
max-age = "abc"

[tables.users]
source = "users.csv"
fields = [
    { name = "id", type = "NUMBER", primary-key = true },
    { name = "name", type = "TEXT" },
]
"#,
    );

    let result = Config::load(tmp.path());
    let err = format!("{:#}", result.unwrap_err());
    assert!(
        err.contains("max-age"),
        "should report invalid max-age: {err}"
    );
}

#[test]
fn test_truncate_config_no_truncate_section() {
    common::init_logging();
    let tmp = tempfile::tempdir().unwrap();
    common::write_config(
        tmp.path(),
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

    let result = Config::load(tmp.path());
    assert!(result.is_ok(), "Config without [truncate] should succeed");
}

#[test]
fn test_json_config_file() {
    common::init_logging();
    let tmp = tempfile::tempdir().unwrap();
    let work_dir = tmp.path();

    // Use config.json instead of config.toml
    common::write_config(
        work_dir,
        "config.json",
        r#"{
  "tables": {
    "users": {
      "source": "users.csv",
      "fields": [
        { "name": "id", "type": "NUMBER", "primary-key": true },
        { "name": "name", "type": "TEXT" }
      ]
    }
  }
}"#,
    );

    common::write_csv(work_dir, "users.csv", "1,Alice\n2,Bob\n");
    let config = Config::load(work_dir).unwrap();

    let hash = Block::create(&config, None).unwrap();

    let patch = Patch::create(&config, GENESIS_HASH).unwrap();
    assert_eq!(patch.num_blocks, 0);
    assert_eq!(patch.head, hash);

    let sql = sql::patch_to_sql(&config, &patch).unwrap().unwrap();
    assert_eq!(common::count_sql(&sql, "INSERT INTO"), 2);

    common::assert_wire_roundtrip(&config, &patch);
}

#[test]
fn test_empty_tables_map_rejected() {
    common::init_logging();
    let tmp = tempfile::tempdir().unwrap();
    common::write_config(tmp.path(), "config.toml", "[tables]\n");

    let err = format!("{:#}", Config::load(tmp.path()).unwrap_err());
    assert!(
        err.contains("at least one table"),
        "should report empty tables: {err}"
    );
}

#[test]
fn test_injected_field_collides_with_table_column() {
    common::init_logging();
    let tmp = tempfile::tempdir().unwrap();
    common::write_config(
        tmp.path(),
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
    { name = "host", type = "TEXT" },
]
"#,
    );

    let err = format!("{:#}", Config::load(tmp.path()).unwrap_err());
    assert!(
        err.contains("collides"),
        "should report collision with table column: {err}"
    );
}

#[test]
fn test_filter_rule_references_unknown_table() {
    common::init_logging();
    let tmp = tempfile::tempdir().unwrap();
    common::write_config(
        tmp.path(),
        "config.toml",
        r#"
[[filters.exclude]]
tables = ["nonexistent"]
field = "name"
regex = "^x$"

[tables.users]
source = "users.csv"
fields = [
    { name = "id", type = "NUMBER", primary-key = true },
    { name = "name", type = "TEXT" },
]
"#,
    );

    let err = format!("{:#}", Config::load(tmp.path()).unwrap_err());
    assert!(
        err.contains("unknown table 'nonexistent'"),
        "should report unknown table: {err}"
    );
}

#[test]
fn test_empty_table_source_rejected() {
    common::init_logging();
    let tmp = tempfile::tempdir().unwrap();
    common::write_config(
        tmp.path(),
        "config.toml",
        r#"
[tables.users]
source = ""
fields = [
    { name = "id", type = "NUMBER", primary-key = true },
    { name = "name", type = "TEXT" },
]
"#,
    );

    let err = format!("{:#}", Config::load(tmp.path()).unwrap_err());
    assert!(
        err.contains("source must not be empty"),
        "should report empty source: {err}"
    );
}

#[test]
fn test_injected_field_empty_name_rejected() {
    common::init_logging();
    let tmp = tempfile::tempdir().unwrap();
    common::write_config(
        tmp.path(),
        "config.toml",
        r#"
[[injected-fields]]
name = ""
value = "x"

[tables.users]
source = "users.csv"
fields = [
    { name = "id", type = "NUMBER", primary-key = true },
    { name = "name", type = "TEXT" },
]
"#,
    );

    let err = format!("{:#}", Config::load(tmp.path()).unwrap_err());
    assert!(
        err.contains("name must not be empty"),
        "should report empty name: {err}"
    );
}

#[test]
fn test_injected_field_empty_value_rejected() {
    common::init_logging();
    let tmp = tempfile::tempdir().unwrap();
    common::write_config(
        tmp.path(),
        "config.toml",
        r#"
[[injected-fields]]
name = "host"
value = ""

[tables.users]
source = "users.csv"
fields = [
    { name = "id", type = "NUMBER", primary-key = true },
    { name = "name", type = "TEXT" },
]
"#,
    );

    let err = format!("{:#}", Config::load(tmp.path()).unwrap_err());
    assert!(
        err.contains("value must not be empty"),
        "should report empty value: {err}"
    );
}

#[test]
fn test_injected_field_value_does_not_parse() {
    common::init_logging();
    let tmp = tempfile::tempdir().unwrap();
    common::write_config(
        tmp.path(),
        "config.toml",
        r#"
[[injected-fields]]
name = "count"
type = "NUMBER"
value = "abc"

[tables.users]
source = "users.csv"
fields = [
    { name = "id", type = "NUMBER", primary-key = true },
    { name = "name", type = "TEXT" },
]
"#,
    );

    let err = format!("{:#}", Config::load(tmp.path()).unwrap_err());
    assert!(
        err.contains("invalid number"),
        "should report unparseable value: {err}"
    );
}

#[test]
fn test_field_empty_name_rejected() {
    common::init_logging();
    let tmp = tempfile::tempdir().unwrap();
    common::write_config(
        tmp.path(),
        "config.toml",
        r#"
[tables.users]
source = "users.csv"
fields = [
    { name = "id", type = "NUMBER", primary-key = true },
    { type = "TEXT" },
]
"#,
    );

    let err = format!("{:#}", Config::load(tmp.path()).unwrap_err());
    assert!(
        err.contains("field name must not be empty"),
        "should report empty field name: {err}"
    );
}

#[test]
fn test_field_unknown_type_rejected() {
    common::init_logging();
    let tmp = tempfile::tempdir().unwrap();
    common::write_config(
        tmp.path(),
        "config.toml",
        r#"
[tables.users]
source = "users.csv"
fields = [
    { name = "id", type = "NUMBER", primary-key = true },
    { name = "name", type = "FLOAT" },
]
"#,
    );

    let err = format!("{:#}", Config::load(tmp.path()).unwrap_err());
    assert!(
        err.contains("unknown field type 'FLOAT'"),
        "should report unknown type: {err}"
    );
}

#[test]
fn test_compression_level_out_of_range() {
    common::init_logging();
    let tmp = tempfile::tempdir().unwrap();
    common::write_config(
        tmp.path(),
        "config.toml",
        r#"
[compression]
level = 999

[tables.users]
source = "users.csv"
fields = [
    { name = "id", type = "NUMBER", primary-key = true },
    { name = "name", type = "TEXT" },
]
"#,
    );

    let err = format!("{:#}", Config::load(tmp.path()).unwrap_err());
    assert!(
        err.contains("compression.level"),
        "should report out-of-range compression.level: {err}"
    );
}
