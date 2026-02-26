mod common;

use leech2::block::Block;
use leech2::config::Config;
use leech2::head;
use leech2::utils::GENESIS_HASH;

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
    head::save(work_dir, GENESIS_HASH).unwrap();

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
