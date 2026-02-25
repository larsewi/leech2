mod common;

use leech2::block::Block;
use leech2::config::Config;

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
    Config::init(work_dir).unwrap();
    let hash1 = Block::create().unwrap();

    // Wait for the first block to become older than 1s
    std::thread::sleep(std::time::Duration::from_secs(2));

    // Create second block — truncation should remove hash1 (older than 1s)
    common::write_csv(work_dir, "users.csv", "1,Alice\n2,Bob\n");
    let hash2 = Block::create().unwrap();

    assert!(
        !work_dir.join(&hash1).exists(),
        "old block should be removed"
    );
    assert!(work_dir.join(&hash2).exists(), "HEAD should be preserved");

    // Create two more blocks quickly — both should be preserved (within 1s window)
    common::write_csv(work_dir, "users.csv", "1,Alice\n2,Bob\n3,Charlie\n");
    let hash3 = Block::create().unwrap();

    assert!(
        work_dir.join(&hash2).exists(),
        "recent block should be preserved"
    );
    assert!(work_dir.join(&hash3).exists(), "HEAD should be preserved");
}
