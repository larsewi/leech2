mod common;

use std::path::Path;

use leech2::block::Block;
use leech2::config::Config;
use leech2::patch::Patch;
use leech2::sql;
use leech2::utils::format_iso_timestamp;

/// Write and load a config with a CSV-backed `users` table that logs its
/// changes in `users_log`.
fn load_config(work_dir: &Path) -> Config {
    common::write_file(
        work_dir,
        "config.toml",
        r#"
[tables.users]
fields = [
    { name = "id", type = "NUMBER", primary-key = true },
    { name = "name", type = "TEXT" },
]

[tables.users.csv]
source = "users.csv"

[tables.users.log]
table = "users_log"
id-field = "eventid"
"#,
    );
    Config::load(work_dir).unwrap()
}

/// Lines of `sql` that select a log row, in order.
fn log_rows(sql: &str) -> Vec<&str> {
    let mut rows = Vec::new();
    let mut lines = sql.lines();
    while let Some(line) = lines.next() {
        if line.starts_with("INSERT INTO \"users_log\"")
            && let Some(row) = lines.next()
        {
            rows.push(row);
        }
    }
    rows
}

/// The creation time of block `hash`, as the quoted SQL literal of a log row.
fn timestamp(config: &Config, hash: &str) -> String {
    let header = Block::load_header(&config.state_dir(), hash, config.file_mode).unwrap();
    format!(
        "'{}'",
        format_iso_timestamp(&header.created.unwrap()).unwrap()
    )
}

#[test]
fn test_change_log_records_every_block() {
    let tmp = tempfile::tempdir().unwrap();
    let work_dir = tmp.path();
    let config = load_config(work_dir);

    common::write_csv!(work_dir, "users.csv", "1,Alice");
    let hash1 = Block::create(&config, None).unwrap();

    // Block 2: update Alice.
    common::write_csv!(work_dir, "users.csv", "1,Alicia");
    let hash2 = Block::create(&config, None).unwrap();

    // Block 3: update Alicia again and insert Bob.
    common::write_csv!(work_dir, "users.csv", "1,Ali", "2,Bob");
    let hash3 = Block::create(&config, None).unwrap();

    // Block 4: delete Ali.
    common::write_csv!(work_dir, "users.csv", "2,Bob");
    let hash4 = Block::create(&config, None).unwrap();

    let patch = Patch::create(&config, &hash1).unwrap();
    let sql = sql::patch_to_sql(&config, &patch, &[]).unwrap().unwrap();

    // Block 3's events are in delete, insert, update order.
    let (time2, time3, time4) = (
        timestamp(&config, &hash2),
        timestamp(&config, &hash3),
        timestamp(&config, &hash4),
    );
    let expected = [
        format!("SELECT 1, 'Alicia', 'UPDATE', {time2}, '{hash2}:0'"),
        format!("SELECT 2, 'Bob', 'INSERT', {time3}, '{hash3}:0'"),
        format!("SELECT 1, 'Ali', 'UPDATE', {time3}, '{hash3}:1'"),
        format!("SELECT 1, 'Ali', 'DELETE', {time4}, '{hash4}:0'"),
    ];
    assert_eq!(log_rows(&sql), expected, "got:\n{sql}");
}

#[test]
fn test_change_log_skips_first_block() {
    let tmp = tempfile::tempdir().unwrap();
    let work_dir = tmp.path();
    let config = load_config(work_dir);

    common::write_csv!(work_dir, "users.csv", "1,Alice");
    let hash = Block::create(&config, None).unwrap();

    let block = Block::load(&config.state_dir(), &hash, config.file_mode).unwrap();
    assert!(block.events.is_empty());
}
