mod common;

use leech2::config::Config;

#[test]
fn test_config_validation_errors() {
    // --- Missing config file ---
    let tmp = tempfile::tempdir().unwrap();
    let result = Config::load(tmp.path());
    assert!(result.is_err());
    assert!(
        result.unwrap_err().contains("no config file found"),
        "should report missing config"
    );

    // --- No primary key ---
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
        result.unwrap_err().contains("primary-key"),
        "should report missing primary key"
    );

    // --- Duplicate field names ---
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
        result.unwrap_err().contains("duplicate field name"),
        "should report duplicate field"
    );

    // --- Invalid TOML syntax ---
    let tmp = tempfile::tempdir().unwrap();
    common::write_config(tmp.path(), "config.toml", "this is not valid toml [[[");
    let result = Config::load(tmp.path());
    assert!(result.is_err());
    assert!(
        result.unwrap_err().contains("failed to parse config"),
        "should report parse failure"
    );

    // --- Invalid JSON syntax ---
    let tmp = tempfile::tempdir().unwrap();
    common::write_config(tmp.path(), "config.json", "{not valid json}");
    let result = Config::load(tmp.path());
    assert!(result.is_err());
    assert!(
        result.unwrap_err().contains("failed to parse config"),
        "should report parse failure"
    );
}
