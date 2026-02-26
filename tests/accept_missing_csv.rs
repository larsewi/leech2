mod common;

use leech2::block::Block;
use leech2::config::Config;

#[test]
fn test_block_create_with_missing_csv() {
    let tmp = tempfile::tempdir().unwrap();
    let work_dir = tmp.path();

    // Config references a CSV that doesn't exist
    common::write_config(
        work_dir,
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
    // Deliberately NOT creating users.csv

    let config = Config::load(work_dir).unwrap();

    let result = Block::create(&config);
    assert!(
        result.is_err(),
        "block creation should fail with missing CSV"
    );
}
