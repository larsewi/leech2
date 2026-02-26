mod common;

use leech2::block::Block;
use leech2::config::Config;
use leech2::patch::Patch;
use leech2::utils::GENESIS_HASH;

#[test]
fn test_truncate_max_blocks() {
    let tmp = tempfile::tempdir().unwrap();
    let work_dir = tmp.path();

    common::write_config(
        work_dir,
        "config.toml",
        r#"
[truncate]
max-blocks = 2

[tables.users]
source = "users.csv"
fields = [
    { name = "id", type = "INTEGER", primary-key = true },
    { name = "name", type = "TEXT" },
]
"#,
    );

    common::write_csv(work_dir, "users.csv", "1,Alice\n");
    let config = Config::load(work_dir).unwrap();
    let hash1 = Block::create(&config).unwrap();

    common::write_csv(work_dir, "users.csv", "1,Alice\n2,Bob\n");
    let hash2 = Block::create(&config).unwrap();

    // After 2 blocks, both should still exist (within limit)
    assert!(work_dir.join(&hash1).exists(), "within limit, should exist");
    assert!(work_dir.join(&hash2).exists(), "within limit, should exist");

    // Block 3 pushes hash1 past the limit
    common::write_csv(work_dir, "users.csv", "1,Alice\n2,Bob\n3,Charlie\n");
    let hash3 = Block::create(&config).unwrap();

    // hash1 should be removed (oldest, beyond max-blocks=2)
    assert!(
        !work_dir.join(&hash1).exists(),
        "oldest block should be truncated"
    );
    assert!(work_dir.join(&hash2).exists());
    assert!(work_dir.join(&hash3).exists());

    // Patch from genesis should fall back to state payload since hash1 is gone
    let patch = Patch::create(&config, GENESIS_HASH).unwrap();
    assert_eq!(patch.head_hash, hash3);

    // --- Under limit: create with max-blocks=2, only 2 blocks exist → both preserved ---
    // (We already have hash2 and hash3 remaining, which is exactly max-blocks=2)
    common::write_csv(work_dir, "users.csv", "1,Alice\n2,Bob\n3,Charlie\n4,Dave\n");
    let hash4 = Block::create(&config).unwrap();

    // hash2 should now be truncated (3 blocks, limit is 2)
    assert!(!work_dir.join(&hash2).exists());
    assert!(work_dir.join(&hash3).exists());
    assert!(work_dir.join(&hash4).exists());
}
