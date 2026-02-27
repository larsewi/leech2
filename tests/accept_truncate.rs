mod common;

use leech2::block::Block;
use leech2::config::Config;
use leech2::head;
use leech2::patch::Patch;
use leech2::reported;
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

/// Uses max-age = "1s" so we can test age-based truncation with a short sleep,
/// then also verify that blocks created within the age window are preserved.
#[test]
fn test_truncate_max_age() {
    let tmp = tempfile::tempdir().unwrap();
    let work_dir = tmp.path();

    common::write_config(
        work_dir,
        "config.toml",
        r#"
[truncate]
max-age = "1s"

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

    // Wait for the first block to become older than 1s
    std::thread::sleep(std::time::Duration::from_secs(2));

    // Create second block — truncation should remove hash1 (older than 1s)
    common::write_csv(work_dir, "users.csv", "1,Alice\n2,Bob\n");
    let hash2 = Block::create(&config).unwrap();

    assert!(
        !work_dir.join(&hash1).exists(),
        "old block should be removed"
    );
    assert!(work_dir.join(&hash2).exists(), "HEAD should be preserved");

    // Create two more blocks quickly — both should be preserved (within 1s window)
    common::write_csv(work_dir, "users.csv", "1,Alice\n2,Bob\n3,Charlie\n");
    let hash3 = Block::create(&config).unwrap();

    assert!(
        work_dir.join(&hash2).exists(),
        "recent block should be preserved"
    );
    assert!(work_dir.join(&hash3).exists(), "HEAD should be preserved");
}

#[test]
fn test_orphaned_blocks_removed() {
    let tmp = tempfile::tempdir().unwrap();
    let work_dir = tmp.path();

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

    common::write_csv(work_dir, "users.csv", "1,Alice\n");
    let config = Config::load(work_dir).unwrap();
    let hash1 = Block::create(&config).unwrap();

    // Add a fake orphaned 40-hex file and a stale lock file
    let orphan_hash = "aa00000000000000000000000000000000000000";
    let stale_lock = format!(".{}.lock", orphan_hash);
    std::fs::write(work_dir.join(orphan_hash), b"fake").unwrap();
    std::fs::write(work_dir.join(&stale_lock), b"").unwrap();
    assert!(work_dir.join(orphan_hash).exists());
    assert!(work_dir.join(&stale_lock).exists());

    // Create another block — truncation runs and should remove the orphan
    common::write_csv(work_dir, "users.csv", "1,Alice\n2,Bob\n");
    let hash2 = Block::create(&config).unwrap();

    // Orphan and its stale lock file should be gone
    assert!(
        !work_dir.join(orphan_hash).exists(),
        "orphaned block should be removed"
    );
    assert!(
        !work_dir.join(&stale_lock).exists(),
        "stale lock file should be removed"
    );

    // Chain blocks should still be on disk
    assert!(work_dir.join(&hash1).exists());
    assert!(work_dir.join(&hash2).exists());

    // --- Test orphan from old HEAD ---
    // Manually reset HEAD to GENESIS, making all current blocks orphans
    head::store(work_dir, GENESIS_HASH).unwrap();

    // Create a new block — truncation should remove the now-orphaned blocks
    common::write_csv(work_dir, "users.csv", "1,Alice\n2,Bob\n3,Charlie\n");
    let hash3 = Block::create(&config).unwrap();

    // Old blocks should be removed (orphaned)
    assert!(
        !work_dir.join(&hash1).exists(),
        "old block should be orphaned and removed"
    );
    assert!(
        !work_dir.join(&hash2).exists(),
        "old block should be orphaned and removed"
    );

    // New block should exist
    assert!(work_dir.join(&hash3).exists());
}

#[test]
fn test_truncate_reported() {
    let tmp = tempfile::tempdir().unwrap();
    let work_dir = tmp.path();

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

    common::write_csv(work_dir, "users.csv", "1,Alice\n");
    let config = Config::load(work_dir).unwrap();
    let hash1 = Block::create(&config).unwrap();

    common::write_csv(work_dir, "users.csv", "1,Alice\n2,Bob\n");
    let hash2 = Block::create(&config).unwrap();

    common::write_csv(work_dir, "users.csv", "1,Alice\n2,Bob\n3,Charlie\n");
    let hash3 = Block::create(&config).unwrap();

    // No REPORTED file yet — all blocks should be preserved
    assert!(work_dir.join(&hash1).exists());
    assert!(work_dir.join(&hash2).exists());
    assert!(work_dir.join(&hash3).exists());

    // Mark B2 as reported — blocks older than B2 should be removed on next create
    reported::save(work_dir, &hash2).unwrap();

    common::write_csv(work_dir, "users.csv", "1,Alice\n2,Bob\n3,Charlie\n4,Dave\n");
    let hash4 = Block::create(&config).unwrap();

    // B1 should be removed (older than REPORTED=B2)
    assert!(
        !work_dir.join(&hash1).exists(),
        "block older than REPORTED should be removed"
    );

    // B2, B3, B4 should be preserved
    assert!(work_dir.join(&hash2).exists());
    assert!(work_dir.join(&hash3).exists());
    assert!(work_dir.join(&hash4).exists());
}
