mod common;

use leech2::block::Block;
use leech2::config::Config;
use leech2::patch::Patch;
use leech2::sql;
use leech2::utils::GENESIS_HASH;

#[test]
fn test_multiple_tables() {
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

[tables.products]
source = "products.csv"
fields = [
    { name = "sku", type = "TEXT", primary-key = true },
    { name = "price", type = "INTEGER" },
]
"#,
    );

    // Block 1: initial data for both tables
    common::write_csv(work_dir, "users.csv", "1,Alice\n");
    common::write_csv(work_dir, "products.csv", "ABC,100\n");
    let config = Config::load(work_dir).unwrap();
    let hash1 = Block::create(&config).unwrap();

    // Block 2: insert user, update product price
    common::write_csv(work_dir, "users.csv", "1,Alice\n2,Bob\n");
    common::write_csv(work_dir, "products.csv", "ABC,150\n");
    let _hash2 = Block::create(&config).unwrap();

    // Patch from hash1: should have changes for both tables
    let patch = Patch::create(&config, &hash1).unwrap();
    assert_eq!(patch.num_blocks, 1);

    let sql = sql::patch_to_sql(&config, &patch).unwrap().unwrap();

    // Users table: 1 insert (Bob)
    assert!(sql.contains(r#"INSERT INTO "users" ("id", "name") VALUES (2, 'Bob');"#));

    // Products table: 1 update (price 100->150)
    assert!(sql.contains(r#"UPDATE "products" SET "price" = 150 WHERE "sku" = 'ABC';"#));

    // Patch from genesis: should have inserts for both tables
    let patch_genesis = Patch::create(&config, GENESIS_HASH).unwrap();
    let sql_genesis = sql::patch_to_sql(&config, &patch_genesis).unwrap().unwrap();

    // Should reference both tables
    assert!(sql_genesis.contains(r#""users""#));
    assert!(sql_genesis.contains(r#""products""#));

    common::assert_wire_roundtrip(&config, &patch);
    common::assert_wire_roundtrip(&config, &patch_genesis);
}
