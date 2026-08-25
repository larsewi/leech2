#![allow(dead_code)]

use std::collections::HashSet;
use std::path::Path;

use leech2::config::Config;
use leech2::patch::Patch;
use leech2::sql;
use leech2::wire;

/// Install a logger for this test binary, before any test runs. Reads the
/// `LEECH2_LOG` env var, same as the CLI. Run `cargo test -- --nocapture` to
/// see the output; libtest only replays captured output for failing tests.
///
/// Runs before the Rust runtime is fully initialized, hence `unsafe`; the body
/// stays limited to installing the logger.
#[ctor::ctor(unsafe)]
fn init_logging() {
    let _ = env_logger::Builder::from_env(env_logger::Env::new().filter("LEECH2_LOG"))
        .is_test(true)
        .try_init();
}

/// Write a config file to the work directory.
pub fn write_config(work_dir: &Path, filename: &str, content: &str) {
    std::fs::write(work_dir.join(filename), content).unwrap();
}

/// Write a CSV file to the work directory.
pub fn write_csv(work_dir: &Path, filename: &str, content: &str) {
    std::fs::write(work_dir.join(filename), content).unwrap();
}

/// Parse SQL output into a set of individual statements. Handles
/// non-deterministic ordering from HashMap iteration. A statement spans
/// several lines (one clause per line) and ends with a semicolon; the clauses
/// are folded onto one line so callers can pass single-line expectations.
fn parse_sql_statements(sql: &str) -> HashSet<String> {
    let mut statements = HashSet::new();
    let mut current = String::new();

    for line in sql.lines() {
        let line = line.trim();
        if line.is_empty() {
            continue;
        }
        if !current.is_empty() {
            current.push(' ');
        }
        current.push_str(line);
        if line.ends_with(';') {
            statements.insert(std::mem::take(&mut current));
        }
    }
    if !current.is_empty() {
        statements.insert(current);
    }

    statements
}

/// Assert that the SQL output contains exactly the expected set of statements
/// (ignoring order). Each expected string should be a complete statement
/// including the trailing semicolon.
pub fn assert_sql_statements(sql: &str, expected: &[&str]) {
    let actual = parse_sql_statements(sql);
    let expected_set: HashSet<String> = expected.iter().map(|s| s.to_string()).collect();

    let missing: Vec<_> = expected_set.difference(&actual).collect();
    let extra: Vec<_> = actual.difference(&expected_set).collect();

    assert!(
        missing.is_empty() && extra.is_empty(),
        "SQL mismatch:\n  Missing: {:#?}\n  Extra: {:#?}\n  Full SQL:\n{}",
        missing,
        extra,
        sql
    );
}

/// Count occurrences of a keyword (e.g. "INSERT INTO", "DELETE FROM") in SQL.
pub fn count_sql(sql: &str, keyword: &str) -> usize {
    sql.matches(keyword).count()
}

/// Assert that a patch survives wire encoding/decoding (protobuf + optional
/// zstd compression) and produces identical SQL output.
pub fn assert_wire_roundtrip(config: &Config, patch: &Patch) {
    let encoded = wire::encode_patch(config, patch).unwrap();
    let decoded = wire::decode_patch(&encoded).unwrap();

    assert_eq!(patch.head, decoded.head);
    assert_eq!(patch.num_blocks, decoded.num_blocks);

    let sql_before = sql::patch_to_sql(config, patch).unwrap();
    let sql_after = sql::patch_to_sql(config, &decoded).unwrap();

    match (&sql_before, &sql_after) {
        (Some(before), Some(after)) => {
            let stmts_before = parse_sql_statements(before);
            let stmts_after = parse_sql_statements(after);
            assert_eq!(
                stmts_before, stmts_after,
                "SQL mismatch after wire roundtrip"
            );
        }
        _ => assert_eq!(sql_before, sql_after, "SQL mismatch after wire roundtrip"),
    }
}
