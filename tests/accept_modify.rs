mod common;

use leech2::block::Block;
use leech2::config::Config;
use leech2::patch::Patch;
use leech2::sql;
use leech2::utils::GENESIS_HASH;

#[test]
fn test_two_blocks_insert_delete_update() {
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

    // Block 1: initial data
    common::write_csv(work_dir, "users.csv", "1,Alice\n2,Bob\n3,Charlie\n");
    Config::init(work_dir).unwrap();
    let hash1 = Block::create().unwrap();

    // Block 2: update Alice->Alicia, delete Bob, insert Dave
    common::write_csv(work_dir, "users.csv", "1,Alicia\n3,Charlie\n4,Dave\n");
    let hash2 = Block::create().unwrap();
    assert_ne!(hash1, hash2);

    // Patch from genesis (consolidated 2 blocks)
    // Merge rules: insert(Bob)+delete(Bob) = cancel (rule 6)
    //              insert(Alice)+update(Alice->Alicia) = insert(Alicia) (rule 7)
    // Net result: 3 inserts (Alicia, Charlie, Dave), 0 deletes, 0 updates
    let patch_full = Patch::create(GENESIS_HASH).unwrap();
    assert_eq!(patch_full.num_blocks, 2);
    assert_eq!(patch_full.head_hash, hash2);

    let sql_full = sql::patch_to_sql(&patch_full).unwrap().unwrap();
    assert_eq!(common::count_sql(&sql_full, "INSERT INTO"), 3);
    assert_eq!(common::count_sql(&sql_full, "DELETE FROM"), 0);
    assert_eq!(common::count_sql(&sql_full, "UPDATE "), 0);

    common::assert_wire_roundtrip(&patch_full);

    // Patch from hash1 (just block 2's changes)
    // 1 insert (Dave), 1 delete (Bob), 1 update (Alice->Alicia)
    let patch_partial = Patch::create(&hash1).unwrap();
    assert_eq!(patch_partial.num_blocks, 1);
    assert_eq!(patch_partial.head_hash, hash2);

    let sql_partial = sql::patch_to_sql(&patch_partial).unwrap().unwrap();
    assert_eq!(common::count_sql(&sql_partial, "INSERT INTO"), 1);
    assert_eq!(common::count_sql(&sql_partial, "DELETE FROM"), 1);
    assert_eq!(common::count_sql(&sql_partial, "UPDATE "), 1);

    // Verify specific SQL content
    assert!(sql_partial.contains(r#"INSERT INTO "users" ("id", "name") VALUES (4, 'Dave');"#));
    assert!(sql_partial.contains(r#"DELETE FROM "users" WHERE "id" = 2;"#));
    assert!(sql_partial.contains(r#"UPDATE "users" SET "name" = 'Alicia' WHERE "id" = 1;"#));

    common::assert_wire_roundtrip(&patch_partial);
}
