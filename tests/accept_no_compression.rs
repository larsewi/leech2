mod common;

use leech2::block::Block;
use leech2::config::Config;
use leech2::patch::Patch;
use leech2::sql;
use leech2::utils::GENESIS_HASH;
use leech2::wire;

/// Zstd frame magic number.
const ZSTD_MAGIC: [u8; 4] = [0x28, 0xB5, 0x2F, 0xFD];

#[test]
fn test_compression_disabled() {
    let tmp = tempfile::tempdir().unwrap();
    let work_dir = tmp.path();

    common::write_config(
        work_dir,
        "config.toml",
        r#"
[compression]
enable = false

[tables.users]
source = "users.csv"
fields = [
    { name = "id", type = "INTEGER", primary-key = true },
    { name = "name", type = "TEXT" },
]
"#,
    );

    common::write_csv(work_dir, "users.csv", "1,Alice\n2,Bob\n");
    let config = Config::load(work_dir).unwrap();
    Block::create(&config).unwrap();

    let patch = Patch::create(&config, GENESIS_HASH).unwrap();
    let encoded = wire::encode_patch(&config, &patch).unwrap();

    // Encoded bytes should NOT start with zstd magic (compression disabled)
    assert!(
        !encoded.starts_with(&ZSTD_MAGIC),
        "expected raw protobuf, but got zstd-compressed data"
    );

    // Decode should still work (auto-detects uncompressed)
    let decoded = wire::decode_patch(&encoded).unwrap();
    let sql_before = sql::patch_to_sql(&config, &patch).unwrap();
    let sql_after = sql::patch_to_sql(&config, &decoded).unwrap();
    assert_eq!(sql_before, sql_after);
}
