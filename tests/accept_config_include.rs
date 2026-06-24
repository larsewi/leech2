mod common;

use leech2::block::Block;
use leech2::config::Config;
use leech2::patch::Patch;
use leech2::sql;
use leech2::utils::GENESIS_HASH;

/// A table defined in a drop-in fragment pulled in via `include` participates in
/// block creation and patch generation end to end, alongside the base table.
#[test]
fn test_include_fragment_table_round_trip() {
    common::init_logging();
    let tmp = tempfile::tempdir().unwrap();
    let work_dir = tmp.path();

    common::write_config(
        work_dir,
        "config.toml",
        r#"
include = ["conf.d/*.toml"]

[tables.users]
fields = [
    { name = "id", type = "NUMBER", primary-key = true },
    { name = "name", type = "TEXT" },
]

[tables.users.csv]
source = "users.csv"
"#,
    );

    std::fs::create_dir(work_dir.join("conf.d")).unwrap();
    common::write_config(
        &work_dir.join("conf.d"),
        "products.toml",
        r#"
[tables.products]
fields = [
    { name = "sku", type = "TEXT", primary-key = true },
    { name = "price", type = "NUMBER" },
]

[tables.products.csv]
source = "products.csv"
"#,
    );

    common::write_csv(work_dir, "users.csv", "1,Alice\n");
    common::write_csv(work_dir, "products.csv", "ABC,100\n");

    let config = Config::load(work_dir).unwrap();
    assert!(config.tables.contains_key("users"));
    assert!(config.tables.contains_key("products"));

    let _hash = Block::create(&config, None).unwrap();

    let patch = Patch::create(&config, GENESIS_HASH).unwrap();
    let sql = sql::patch_to_sql(&config, &patch).unwrap().unwrap();

    assert!(sql.contains(r#""users""#));
    assert!(sql.contains(r#""products""#));

    common::assert_wire_roundtrip(&config, &patch);
}
