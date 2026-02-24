mod common;

use leech2::block::Block;
use leech2::config::Config;
use leech2::patch::Patch;
use leech2::sql;
use leech2::utils::GENESIS_HASH;

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
    Config::init(work_dir).unwrap();

    let hash = Block::create().unwrap();

    let patch = Patch::create(GENESIS_HASH).unwrap();
    assert_eq!(patch.num_blocks, 1);
    assert_eq!(patch.head_hash, hash);

    let sql = sql::patch_to_sql(&patch).unwrap().unwrap();
    assert_eq!(common::count_sql(&sql, "INSERT INTO"), 2);

    common::assert_wire_roundtrip(&patch);
}
