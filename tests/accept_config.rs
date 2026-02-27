mod common;

use leech2::block::Block;
use leech2::config::Config;
use leech2::patch::Patch;
use leech2::sql;
use leech2::utils::GENESIS_HASH;

#[test]
fn test_missing_config_file() {
    let tmp = tempfile::tempdir().unwrap();
    let result = Config::load(tmp.path());
    assert!(result.is_err());
    assert!(
        result.unwrap_err().to_string().contains("no config file found"),
        "should report missing config"
    );
}

#[test]
fn test_config_no_primary_key() {
    let tmp = tempfile::tempdir().unwrap();
    common::write_config(
        tmp.path(),
        "config.toml",
        r#"
[tables.users]
source = "users.csv"
fields = [
    { name = "id", type = "INTEGER" },
    { name = "name", type = "TEXT" },
]
"#,
    );
    let result = Config::load(tmp.path());
    assert!(result.is_err());
    assert!(
        result.unwrap_err().to_string().contains("primary-key"),
        "should report missing primary key"
    );
}

#[test]
fn test_config_duplicate_field_names() {
    let tmp = tempfile::tempdir().unwrap();
    common::write_config(
        tmp.path(),
        "config.toml",
        r#"
[tables.users]
source = "users.csv"
fields = [
    { name = "id", type = "INTEGER", primary-key = true },
    { name = "id", type = "TEXT" },
]
"#,
    );
    let result = Config::load(tmp.path());
    assert!(result.is_err());
    assert!(
        result.unwrap_err().to_string().contains("duplicate field name"),
        "should report duplicate field"
    );
}

#[test]
fn test_config_invalid_toml_syntax() {
    let tmp = tempfile::tempdir().unwrap();
    common::write_config(tmp.path(), "config.toml", "this is not valid toml [[[");
    let result = Config::load(tmp.path());
    assert!(result.is_err());
    assert!(
        result.unwrap_err().to_string().contains("failed to parse config"),
        "should report parse failure"
    );
}

#[test]
fn test_config_invalid_json_syntax() {
    let tmp = tempfile::tempdir().unwrap();
    common::write_config(tmp.path(), "config.json", "{not valid json}");
    let result = Config::load(tmp.path());
    assert!(result.is_err());
    assert!(
        result.unwrap_err().to_string().contains("failed to parse config"),
        "should report parse failure"
    );
}

#[test]
fn test_truncate_config_max_blocks_invalid() {
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
    { name = "id", type = "INTEGER", primary-key = true },
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
    { name = "id", type = "INTEGER", primary-key = true },
    { name = "name", type = "TEXT" },
]
"#,
    );

    let result = Config::load(tmp.path());
    assert!(result.is_err());
    assert!(
        result.unwrap_err().to_string().contains("max-age"),
        "should report invalid max-age"
    );
}

#[test]
fn test_truncate_config_no_truncate_section() {
    let tmp = tempfile::tempdir().unwrap();
    common::write_config(
        tmp.path(),
        "config.toml",
        r#"
[tables.users]
source = "users.csv"
fields = [
    { name = "id", type = "INTEGER", primary-key = true },
    { name = "name", type = "TEXT" },
]
"#,
    );

    let result = Config::load(tmp.path());
    assert!(result.is_ok(), "Config without [truncate] should succeed");
}

#[test]
fn test_json_config_file() {
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
        { "name": "id", "type": "INTEGER", "primary-key": true },
        { "name": "name", "type": "TEXT" }
      ]
    }
  }
}"#,
    );

    common::write_csv(work_dir, "users.csv", "1,Alice\n2,Bob\n");
    let config = Config::load(work_dir).unwrap();

    let hash = Block::create(&config).unwrap();

    let patch = Patch::create(&config, GENESIS_HASH).unwrap();
    assert_eq!(patch.num_blocks, 1);
    assert_eq!(patch.head_hash, hash);

    let sql = sql::patch_to_sql(&config, &patch).unwrap().unwrap();
    assert_eq!(common::count_sql(&sql, "INSERT INTO"), 2);

    common::assert_wire_roundtrip(&config, &patch);
}
