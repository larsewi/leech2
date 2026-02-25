mod common;

use leech2::config::Config;

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

    let result = Config::init(tmp.path());
    assert!(result.is_err());
    assert!(
        result.unwrap_err().contains("max-blocks"),
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

    let result = Config::init(tmp.path());
    assert!(result.is_err());
    assert!(
        result.unwrap_err().contains("max-age"),
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

    let result = Config::init(tmp.path());
    assert!(result.is_ok(), "Config without [truncate] should succeed");
}
