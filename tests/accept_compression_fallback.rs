mod common;

use leech2::block::Block;
use leech2::config::Config;
use leech2::patch::Patch;
use leech2::utils::GENESIS_HASH;
use leech2::{sql, wire};

/// Zstd frame magic number.
const ZSTD_MAGIC: [u8; 4] = [0x28, 0xB5, 0x2F, 0xFD];

#[test]
fn test_small_payload_kept_raw_when_compression_inflates() {
    common::init_logging();
    let tmp = tempfile::tempdir().unwrap();
    let work_dir = tmp.path();

    common::write_config(
        work_dir,
        "config.toml",
        r#"
[compression]
enable = true

[tables.users]
fields = [
    { name = "id", type = "NUMBER", primary-key = true },
    { name = "name", type = "TEXT" },
]

[tables.users.csv]
source = "users.csv"
"#,
    );

    // A single tiny row: the protobuf is small enough that zstd's frame
    // overhead makes the "compressed" form larger.
    common::write_csv(work_dir, "users.csv", "1,A\n");
    let config = Config::load(work_dir).unwrap();
    Block::create(&config, None).unwrap();

    let patch = Patch::create(&config, GENESIS_HASH).unwrap();
    let encoded = wire::encode_patch(&config, &patch).unwrap();

    // Compression did not shrink the payload, so the raw protobuf is shipped;
    // it must not start with the zstd magic.
    assert!(
        !encoded.starts_with(&ZSTD_MAGIC),
        "small payload should be shipped raw, not zstd-inflated"
    );

    // The receiver auto-detects the missing magic and decodes it identically.
    let decoded = wire::decode_patch(&encoded).unwrap();
    assert_eq!(
        sql::patch_to_sql(&config, &patch).unwrap(),
        sql::patch_to_sql(&config, &decoded).unwrap()
    );
}
