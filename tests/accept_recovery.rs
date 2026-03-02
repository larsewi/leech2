mod common;

use leech2::block::Block;
use leech2::config::Config;
use leech2::head;
use leech2::patch::Patch;
use leech2::reported;
use leech2::sql;
use leech2::storage;
use leech2::utils::GENESIS_HASH;

/// Helper: write a two-field users table config.
fn setup_users(work_dir: &std::path::Path) -> Config {
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
    Config::load(work_dir).unwrap()
}

/// When REPORTED points to a block that was truncated (deleted from disk),
/// patch creation should produce a full state (TRUNCATE + INSERT) instead
/// of failing or producing unsafe delta INSERTs.
#[test]
fn test_reported_block_truncated() {
    let tmp = tempfile::tempdir().unwrap();
    let work_dir = tmp.path();
    let config = setup_users(work_dir);

    common::write_csv(work_dir, "users.csv", "1,Alice\n");
    let hash1 = Block::create(&config).unwrap();

    common::write_csv(work_dir, "users.csv", "1,Alice\n2,Bob\n");
    let hash2 = Block::create(&config).unwrap();

    // Mark block 1 as reported (simulates: database has data up to hash1)
    reported::save(work_dir, &hash1).unwrap();

    // Delete block 1 from disk (simulates truncation)
    storage::remove(work_dir, &hash1).unwrap();
    assert!(!work_dir.join(&hash1).exists());

    // Patch from REPORTED should fall back to STATE (TRUNCATE + INSERT)
    let patch = Patch::create(&config, &hash1).unwrap();
    assert_eq!(patch.head, hash2);
    assert_eq!(patch.num_blocks, 0);

    let sql = sql::patch_to_sql(&config, &patch).unwrap().unwrap();
    assert_eq!(common::count_sql(&sql, "TRUNCATE"), 1);
    assert_eq!(common::count_sql(&sql, "INSERT INTO"), 2);
    assert_eq!(common::count_sql(&sql, "DELETE FROM"), 0);
    assert_eq!(common::count_sql(&sql, "UPDATE "), 0);

    common::assert_wire_roundtrip(&config, &patch);
}

/// When the REPORTED file is deleted, the CLI/FFI falls back to GENESIS.
/// The patch should produce TRUNCATE + INSERT (safe for a database that
/// may already contain rows).
#[test]
fn test_reported_file_deleted() {
    let tmp = tempfile::tempdir().unwrap();
    let work_dir = tmp.path();
    let config = setup_users(work_dir);

    common::write_csv(work_dir, "users.csv", "1,Alice\n2,Bob\n");
    let hash1 = Block::create(&config).unwrap();

    // Mark as reported, then delete the REPORTED file
    reported::save(work_dir, &hash1).unwrap();
    storage::remove(work_dir, "REPORTED").unwrap();
    assert!(reported::load(work_dir).unwrap().is_none());

    // CLI/FFI would resolve to GENESIS when REPORTED is missing
    let patch = Patch::create(&config, GENESIS_HASH).unwrap();
    assert_eq!(patch.head, hash1);
    assert_eq!(patch.num_blocks, 0);

    let sql = sql::patch_to_sql(&config, &patch).unwrap().unwrap();
    assert_eq!(common::count_sql(&sql, "TRUNCATE"), 1);
    assert_eq!(common::count_sql(&sql, "INSERT INTO"), 2);

    common::assert_wire_roundtrip(&config, &patch);
}

/// When HEAD is deleted, it defaults to GENESIS. Patch creation should
/// produce an empty patch (no blocks). A subsequent Block::create should
/// ignore the stale STATE file and capture all rows as inserts.
#[test]
fn test_head_file_deleted() {
    let tmp = tempfile::tempdir().unwrap();
    let work_dir = tmp.path();
    let config = setup_users(work_dir);

    common::write_csv(work_dir, "users.csv", "1,Alice\n2,Bob\n");
    Block::create(&config).unwrap();

    // Delete HEAD — load should return GENESIS
    storage::remove(work_dir, "HEAD").unwrap();
    assert_eq!(head::load(work_dir).unwrap(), GENESIS_HASH);

    // Patch from GENESIS with HEAD=GENESIS → empty patch (no blocks exist)
    let patch = Patch::create(&config, GENESIS_HASH).unwrap();
    assert!(patch.payload.is_none());
    assert_eq!(patch.num_blocks, 0);

    // Block::create should ignore stale STATE and capture full CSV as inserts
    let new_hash = Block::create(&config).unwrap();
    let patch = Patch::create(&config, GENESIS_HASH).unwrap();
    assert_eq!(patch.head, new_hash);

    let sql = sql::patch_to_sql(&config, &patch).unwrap().unwrap();
    assert_eq!(common::count_sql(&sql, "TRUNCATE"), 1);
    assert_eq!(common::count_sql(&sql, "INSERT INTO"), 2);

    common::assert_wire_roundtrip(&config, &patch);
}

/// When a block in the middle of the chain is missing, consolidation fails.
/// Patch creation should fall back to STATE (TRUNCATE + INSERT).
#[test]
fn test_block_chain_broken() {
    let tmp = tempfile::tempdir().unwrap();
    let work_dir = tmp.path();
    let config = setup_users(work_dir);

    common::write_csv(work_dir, "users.csv", "1,Alice\n");
    let hash1 = Block::create(&config).unwrap();

    common::write_csv(work_dir, "users.csv", "1,Alice\n2,Bob\n");
    let hash2 = Block::create(&config).unwrap();

    common::write_csv(work_dir, "users.csv", "1,Alice\n2,Bob\n3,Charlie\n");
    let hash3 = Block::create(&config).unwrap();

    // Delete the middle block — chain is: hash3 -> hash2 (missing) -> hash1
    storage::remove(work_dir, &hash2).unwrap();
    assert!(!work_dir.join(&hash2).exists());

    // Patch from hash1: consolidation walks hash3 -> tries hash2 -> fails -> STATE
    let patch = Patch::create(&config, &hash1).unwrap();
    assert_eq!(patch.head, hash3);
    assert_eq!(patch.num_blocks, 0);

    let sql = sql::patch_to_sql(&config, &patch).unwrap().unwrap();
    assert_eq!(common::count_sql(&sql, "TRUNCATE"), 1);
    assert_eq!(common::count_sql(&sql, "INSERT INTO"), 3);
    assert_eq!(common::count_sql(&sql, "DELETE FROM"), 0);
    assert_eq!(common::count_sql(&sql, "UPDATE "), 0);

    common::assert_wire_roundtrip(&config, &patch);
}

/// When the STATE file is deleted but the block chain is intact,
/// delta consolidation should still succeed (STATE is only needed
/// for the fallback path).
#[test]
fn test_state_file_deleted_with_valid_chain() {
    let tmp = tempfile::tempdir().unwrap();
    let work_dir = tmp.path();
    let config = setup_users(work_dir);

    common::write_csv(work_dir, "users.csv", "1,Alice\n");
    let hash1 = Block::create(&config).unwrap();

    common::write_csv(work_dir, "users.csv", "1,Alice\n2,Bob\n");
    let hash2 = Block::create(&config).unwrap();

    // Delete STATE — consolidation should still work via block chain
    storage::remove(work_dir, "STATE").unwrap();

    let patch = Patch::create(&config, &hash1).unwrap();
    assert_eq!(patch.head, hash2);
    assert_eq!(patch.num_blocks, 1);

    let sql = sql::patch_to_sql(&config, &patch).unwrap().unwrap();
    // Delta path: should have INSERT for Bob, no TRUNCATE
    assert_eq!(common::count_sql(&sql, "TRUNCATE"), 0);
    assert_eq!(common::count_sql(&sql, "INSERT INTO"), 1);
    assert!(sql.contains(r#"INSERT INTO "users" ("id", "name") VALUES (2, 'Bob');"#));

    common::assert_wire_roundtrip(&config, &patch);
}
