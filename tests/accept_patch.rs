mod common;

use leech2::block::Block;
use leech2::config::Config;
use leech2::patch::Patch;
use leech2::sql;
use leech2::utils::GENESIS_HASH;

#[test]
fn test_two_blocks_insert_delete_update() {
    common::init_logging();
    let tmp = tempfile::tempdir().unwrap();
    let work_dir = tmp.path();

    common::write_config(
        work_dir,
        "config.toml",
        r#"
[tables.users]
source = "users.csv"
fields = [
    { name = "id", type = "NUMBER", primary-key = true },
    { name = "name", type = "TEXT" },
]
"#,
    );

    // Block 1: initial data
    common::write_csv(work_dir, "users.csv", "1,Alice\n2,Bob\n3,Charlie\n");
    let config = Config::load(work_dir).unwrap();
    let hash1 = Block::create(&config, None).unwrap();

    // Block 2: update Alice->Alicia, delete Bob, insert Dave
    common::write_csv(work_dir, "users.csv", "1,Alicia\n3,Charlie\n4,Dave\n");
    let hash2 = Block::create(&config, None).unwrap();
    assert_ne!(hash1, hash2);

    // Patch from genesis: full state (TRUNCATE + INSERT), always safe
    // Current state: Alicia, Charlie, Dave
    let patch_full = Patch::create(&config, GENESIS_HASH).unwrap();
    assert_eq!(patch_full.num_blocks, 0);
    assert_eq!(patch_full.head, hash2);

    let sql_full = sql::patch_to_sql(&config, &patch_full).unwrap().unwrap();
    assert_eq!(common::count_sql(&sql_full, "TRUNCATE"), 1);
    assert_eq!(common::count_sql(&sql_full, "INSERT INTO"), 3);
    assert_eq!(common::count_sql(&sql_full, "DELETE FROM"), 0);
    assert_eq!(common::count_sql(&sql_full, "UPDATE "), 0);

    common::assert_wire_roundtrip(&config, &patch_full);

    // Patch from hash1 (just block 2's changes)
    // 1 insert (Dave), 1 delete (Bob), 1 update (Alice->Alicia)
    let patch_partial = Patch::create(&config, &hash1).unwrap();
    assert_eq!(patch_partial.num_blocks, 1);
    assert_eq!(patch_partial.head, hash2);

    let sql_partial = sql::patch_to_sql(&config, &patch_partial).unwrap().unwrap();
    assert_eq!(common::count_sql(&sql_partial, "INSERT INTO"), 1);
    assert_eq!(common::count_sql(&sql_partial, "DELETE FROM"), 1);
    assert_eq!(common::count_sql(&sql_partial, "UPDATE "), 1);

    // Verify specific SQL content
    assert!(sql_partial.contains(r#"INSERT INTO "users" ("id", "name") VALUES (4, 'Dave');"#));
    assert!(sql_partial.contains(r#"DELETE FROM "users" WHERE "id" = 2;"#));
    assert!(sql_partial.contains(r#"UPDATE "users" SET "name" = 'Alicia' WHERE "id" = 1;"#));

    common::assert_wire_roundtrip(&config, &patch_partial);
}

#[test]
fn test_three_blocks_chain_consolidation() {
    common::init_logging();
    let tmp = tempfile::tempdir().unwrap();
    let work_dir = tmp.path();

    common::write_config(
        work_dir,
        "config.toml",
        r#"
[tables.users]
source = "users.csv"
fields = [
    { name = "id", type = "NUMBER", primary-key = true },
    { name = "name", type = "TEXT" },
    { name = "email", type = "TEXT" },
]
"#,
    );

    // Block 1: initial data
    common::write_csv(work_dir, "users.csv", "1,Alice,a@ex.com\n2,Bob,b@ex.com\n");
    let config = Config::load(work_dir).unwrap();
    let hash1 = Block::create(&config, None).unwrap();

    // Block 2: update Alice's email, insert Charlie
    common::write_csv(
        work_dir,
        "users.csv",
        "1,Alice,a@new.com\n2,Bob,b@ex.com\n3,Charlie,c@ex.com\n",
    );
    let hash2 = Block::create(&config, None).unwrap();

    // Block 3: delete Bob, update Charlie -> Charles with new email
    common::write_csv(
        work_dir,
        "users.csv",
        "1,Alice,a@new.com\n3,Charles,ch@ex.com\n",
    );
    let hash3 = Block::create(&config, None).unwrap();

    // -- Patch from genesis: full state (TRUNCATE + INSERT), always safe --
    // Final state: 2 rows (Alice, Charles).
    let patch_genesis = Patch::create(&config, GENESIS_HASH).unwrap();
    assert_eq!(patch_genesis.num_blocks, 0);
    assert_eq!(patch_genesis.head, hash3);

    let sql_genesis = sql::patch_to_sql(&config, &patch_genesis).unwrap().unwrap();
    assert_eq!(common::count_sql(&sql_genesis, "TRUNCATE"), 1);
    assert_eq!(common::count_sql(&sql_genesis, "INSERT INTO"), 2);
    assert_eq!(common::count_sql(&sql_genesis, "DELETE FROM"), 0);
    assert_eq!(common::count_sql(&sql_genesis, "UPDATE "), 0);

    common::assert_wire_roundtrip(&config, &patch_genesis);

    // -- Patch from hash1 (consolidated blocks 2 and 3) --
    // Block 2: insert(3,Charlie,c@ex.com), update(1,email: a@ex.com->a@new.com)
    // Block 3: delete(2,Bob,b@ex.com), update(3, Charlie,c@ex.com -> Charles,ch@ex.com)
    // Merge: insert(3)+update(3) = insert(3,Charles,ch@ex.com) (rule 7)
    //        update(1) passes through (rule 3)
    //        delete(2) passes through (rule 2)
    // Per-table size comparison may choose state over delta if state is smaller.
    let patch_from1 = Patch::create(&config, &hash1).unwrap();
    assert_eq!(patch_from1.num_blocks, 2);

    let sql_from1 = sql::patch_to_sql(&config, &patch_from1).unwrap().unwrap();

    if sql_from1.contains("TRUNCATE") {
        // State path: TRUNCATE + 2 INSERTs (Alice, Charles)
        assert_eq!(common::count_sql(&sql_from1, "TRUNCATE"), 1);
        assert_eq!(common::count_sql(&sql_from1, "INSERT INTO"), 2);
    } else {
        // Delta path: 1 INSERT (Charles), 1 DELETE (Bob), 1 UPDATE (Alice email)
        assert_eq!(common::count_sql(&sql_from1, "INSERT INTO"), 1);
        assert_eq!(common::count_sql(&sql_from1, "DELETE FROM"), 1);
        assert_eq!(common::count_sql(&sql_from1, "UPDATE "), 1);

        assert!(sql_from1.contains(r#"DELETE FROM "users" WHERE "id" = 2;"#));
        assert!(sql_from1.contains(
            r#"INSERT INTO "users" ("id", "name", "email") VALUES (3, 'Charles', 'ch@ex.com');"#
        ));
        // Update should only set the changed column (email)
        assert!(sql_from1.contains(r#"UPDATE "users" SET "email" = 'a@new.com' WHERE "id" = 1;"#));
    }

    common::assert_wire_roundtrip(&config, &patch_from1);

    // -- Patch from hash2 (just block 3) --
    // Per-table size comparison may choose state over delta.
    let patch_from2 = Patch::create(&config, &hash2).unwrap();
    assert_eq!(patch_from2.num_blocks, 1);

    let sql_from2 = sql::patch_to_sql(&config, &patch_from2).unwrap().unwrap();

    if sql_from2.contains("TRUNCATE") {
        // State path: TRUNCATE + 2 INSERTs (Alice, Charles)
        assert_eq!(common::count_sql(&sql_from2, "TRUNCATE"), 1);
        assert_eq!(common::count_sql(&sql_from2, "INSERT INTO"), 2);
    } else {
        // Delta path: 1 DELETE (Bob), 1 UPDATE (Charlie -> Charles)
        assert_eq!(common::count_sql(&sql_from2, "DELETE FROM"), 1);
        assert_eq!(common::count_sql(&sql_from2, "INSERT INTO"), 0);
        assert_eq!(common::count_sql(&sql_from2, "UPDATE "), 1);
    }

    common::assert_wire_roundtrip(&config, &patch_from2);
}

#[test]
fn test_consecutive_updates_same_column_consolidate() {
    common::init_logging();
    let tmp = tempfile::tempdir().unwrap();
    let work_dir = tmp.path();

    common::write_config(
        work_dir,
        "config.toml",
        r#"
[tables.products]
source = "products.csv"
fields = [
    { name = "id", type = "NUMBER", primary-key = true },
    { name = "name", type = "TEXT" },
    { name = "price", type = "NUMBER" },
]
"#,
    );

    // Block 1: initial data
    common::write_csv(work_dir, "products.csv", "3,Widget,249.95\n");
    let config = Config::load(work_dir).unwrap();
    let hash1 = Block::create(&config, None).unwrap();

    // Block 2: bump price 249.95 -> 249.96
    common::write_csv(work_dir, "products.csv", "3,Widget,249.96\n");
    let _hash2 = Block::create(&config, None).unwrap();

    // Block 3: bump price 249.96 -> 249.97
    common::write_csv(work_dir, "products.csv", "3,Widget,249.97\n");
    let hash3 = Block::create(&config, None).unwrap();

    // Patch from hash1 should consolidate the two consecutive price updates
    // into a single update from 249.95 to 249.97. The merge must not collapse
    // into a degenerate update with old == new (which previously fell out of
    // a swapped parent/child order in try_consolidate).
    let patch = Patch::create(&config, &hash1).unwrap();
    assert_eq!(patch.num_blocks, 2);
    assert_eq!(patch.head, hash3);

    let sql = sql::patch_to_sql(&config, &patch).unwrap().unwrap();

    // The merge should produce a delta-path patch (not a state fallback).
    assert!(
        !sql.contains("TRUNCATE"),
        "expected delta path, got state fallback:\n{sql}"
    );
    assert_eq!(common::count_sql(&sql, "UPDATE "), 1);
    assert_eq!(common::count_sql(&sql, "INSERT INTO"), 0);
    assert_eq!(common::count_sql(&sql, "DELETE FROM"), 0);
    assert!(
        sql.contains(r#"UPDATE "products" SET "price" = 249.97 WHERE "id" = 3;"#),
        "expected price=249.97, got:\n{sql}"
    );

    common::assert_wire_roundtrip(&config, &patch);
}
