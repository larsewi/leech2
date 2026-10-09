mod common;

use std::thread::sleep;
use std::time::Duration;

use leech2::block::Block;
use leech2::config::Config;
use leech2::patch::Patch;
use leech2::sql;

/// Return the change timestamp literal on the SQL line containing `needle`.
/// The timestamp is the last quoted value on INSERT VALUES and UPDATE SET
/// lines.
fn change_timestamp_on_line(sql: &str, needle: &str) -> String {
    let line = sql
        .lines()
        .find(|line| line.contains(needle))
        .unwrap_or_else(|| panic!("no line containing {needle:?} in:\n{sql}"));
    line.rsplit('\'').nth(1).unwrap().to_string()
}

#[test]
fn test_change_timestamp_per_block() {
    let tmp = tempfile::tempdir().unwrap();
    let work_dir = tmp.path();

    common::write_config(
        work_dir,
        "config.toml",
        r#"
[tables.users]
change-timestamp = "changed"
use-full-state-if-smaller = false
fields = [
    { name = "id", type = "NUMBER", primary-key = true },
    { name = "name", type = "TEXT" },
]

[tables.users.csv]
source = "users.csv"
"#,
    );
    let config = Config::load(work_dir).unwrap();

    common::write_csv(work_dir, "users.csv", "1,Alice\n2,Bob\n3,Charlie\n");
    let hash1 = Block::create(&config, None).unwrap();

    // Block 2: update Alice, insert Dave.
    common::write_csv(
        work_dir,
        "users.csv",
        "1,Alicia\n2,Bob\n3,Charlie\n4,Dave\n",
    );
    Block::create(&config, None).unwrap();

    // Timestamps have second precision, so make block 3 land in a later second.
    sleep(Duration::from_millis(1100));

    // Block 3: update Bob, insert Eve.
    common::write_csv(
        work_dir,
        "users.csv",
        "1,Alicia\n2,Robert\n3,Charlie\n4,Dave\n5,Eve\n",
    );
    Block::create(&config, None).unwrap();

    let patch = Patch::create(&config, &hash1).unwrap();
    let sql = sql::patch_to_sql(&config, &patch).unwrap().unwrap();

    let block2_insert = change_timestamp_on_line(&sql, "VALUES (4, 'Dave'");
    let block2_update = change_timestamp_on_line(&sql, "SET \"name\" = 'Alicia'");
    let block3_insert = change_timestamp_on_line(&sql, "VALUES (5, 'Eve'");
    let block3_update = change_timestamp_on_line(&sql, "SET \"name\" = 'Robert'");

    assert_eq!(block2_insert, block2_update);
    assert_eq!(block3_insert, block3_update);
    assert_ne!(block2_insert, block3_insert);

    // Block 3 is HEAD, so its time is the patch's creation time.
    let head_created = patch.created.unwrap();
    let expected = chrono::DateTime::from_timestamp(head_created.seconds, 0)
        .unwrap()
        .format("%Y-%m-%dT%H:%M:%SZ")
        .to_string();
    assert_eq!(block3_insert, expected);
}
