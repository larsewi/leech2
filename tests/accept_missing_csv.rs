mod common;

use leech2::block::Block;
use leech2::config::Config;

#[test]
fn test_block_create_with_missing_csv() {
    common::init_logging();
    let tmp = tempfile::tempdir().unwrap();
    let work_dir = tmp.path();

    // Config references a CSV that doesn't exist
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
"#,
    );
    // Deliberately NOT creating users.csv

    let config = Config::load(work_dir).unwrap();

    let result = Block::create(&config, None);
    assert!(
        result.is_err(),
        "block creation should fail with missing CSV"
    );
}
