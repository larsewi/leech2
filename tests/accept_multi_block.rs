mod common;

use leech2::block::Block;
use leech2::config::Config;
use leech2::patch::Patch;
use leech2::sql;
use leech2::utils::GENESIS_HASH;

#[test]
fn test_three_blocks_chain_consolidation() {
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
    { name = "email", type = "TEXT" },
]
"#,
    );

    // Block 1: initial data
    common::write_csv(work_dir, "users.csv", "1,Alice,a@ex.com\n2,Bob,b@ex.com\n");
    let config = Config::load(work_dir).unwrap();
    let hash1 = Block::create(&config).unwrap();

    // Block 2: update Alice's email, insert Charlie
    common::write_csv(
        work_dir,
        "users.csv",
        "1,Alice,a@new.com\n2,Bob,b@ex.com\n3,Charlie,c@ex.com\n",
    );
    let hash2 = Block::create(&config).unwrap();

    // Block 3: delete Bob, update Charlie -> Charles with new email
    common::write_csv(
        work_dir,
        "users.csv",
        "1,Alice,a@new.com\n3,Charles,ch@ex.com\n",
    );
    let hash3 = Block::create(&config).unwrap();

    // -- Patch from genesis (consolidated 3 blocks) --
    // Final state: 2 rows. From genesis everything is inserts.
    let patch_genesis = Patch::create(&config, GENESIS_HASH).unwrap();
    assert_eq!(patch_genesis.num_blocks, 3);
    assert_eq!(patch_genesis.head_hash, hash3);

    let sql_genesis = sql::patch_to_sql(&config, &patch_genesis).unwrap().unwrap();
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
    let patch_from1 = Patch::create(&config, &hash1).unwrap();
    assert_eq!(patch_from1.num_blocks, 2);

    let sql_from1 = sql::patch_to_sql(&config, &patch_from1).unwrap().unwrap();
    assert_eq!(common::count_sql(&sql_from1, "INSERT INTO"), 1); // Charles
    assert_eq!(common::count_sql(&sql_from1, "DELETE FROM"), 1); // Bob
    assert_eq!(common::count_sql(&sql_from1, "UPDATE "), 1); // Alice email

    assert!(sql_from1.contains(r#"DELETE FROM "users" WHERE "id" = 2;"#));
    assert!(sql_from1.contains(
        r#"INSERT INTO "users" ("id", "name", "email") VALUES (3, 'Charles', 'ch@ex.com');"#
    ));
    // Update should only set the changed column (email)
    assert!(sql_from1.contains(r#"UPDATE "users" SET "email" = 'a@new.com' WHERE "id" = 1;"#));

    common::assert_wire_roundtrip(&config, &patch_from1);

    // -- Patch from hash2 (just block 3) --
    let patch_from2 = Patch::create(&config, &hash2).unwrap();
    assert_eq!(patch_from2.num_blocks, 1);

    let sql_from2 = sql::patch_to_sql(&config, &patch_from2).unwrap().unwrap();
    assert_eq!(common::count_sql(&sql_from2, "DELETE FROM"), 1); // Bob
    assert_eq!(common::count_sql(&sql_from2, "INSERT INTO"), 0);
    assert_eq!(common::count_sql(&sql_from2, "UPDATE "), 1); // Charlie -> Charles

    common::assert_wire_roundtrip(&config, &patch_from2);
}
