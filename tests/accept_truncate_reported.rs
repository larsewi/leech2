mod common;

use leech2::block::Block;
use leech2::config::Config;
use leech2::reported;

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
