mod common;

use leech2::block::Block;
use leech2::config::Config;
use leech2::patch::Patch;
use leech2::sql;

/// Reordering fields in `tables.toml` between two blocks must not produce a
/// spurious delta when the underlying CSV data is unchanged. Tuple identity
/// is canonical (lexicographic by field name) so that the field declaration
/// order in the config is cosmetic.
#[test]
fn test_field_reorder_in_config_produces_no_delta() {
    common::init_logging();
    let tmp = tempfile::tempdir().unwrap();
    let work_dir = tmp.path();

    common::write_csv(
        work_dir,
        "users.csv",
        "id,name,email\n1,Alice,a@example.com\n2,Bob,b@example.com\n",
    );

    common::write_config(
        work_dir,
        "config.toml",
        r#"
[tables.users]
source = "users.csv"
header = true
fields = [
    { name = "id", type = "NUMBER", primary-key = true },
    { name = "name", type = "TEXT" },
    { name = "email", type = "TEXT" },
]
"#,
    );
    let hash1 = Block::create(&Config::load(work_dir).unwrap(), None).unwrap();

    // Reorder fields (id is still the only primary key, but its position in
    // the declared field list moves; subsidiaries are also reordered).
    common::write_config(
        work_dir,
        "config.toml",
        r#"
[tables.users]
source = "users.csv"
header = true
fields = [
    { name = "email", type = "TEXT" },
    { name = "id", type = "NUMBER", primary-key = true },
    { name = "name", type = "TEXT" },
]
"#,
    );
    let config = Config::load(work_dir).unwrap();
    let _hash2 = Block::create(&config, None).unwrap();

    // Patch from hash1 should be empty: same data, just a different
    // declaration order.
    let patch = Patch::create(&config, &hash1).unwrap();
    if let Some(s) = sql::patch_to_sql(&config, &patch).unwrap() {
        assert_eq!(s.trim(), "BEGIN;\nCOMMIT;");
    }

    common::assert_wire_roundtrip(&config, &patch);
}
