#![allow(dead_code)]

use std::collections::HashSet;
use std::path::Path;

use leech2::patch::Patch;
use leech2::sql;
use leech2::wire;

/// Write a config file to the work directory.
pub fn write_config(work_dir: &Path, filename: &str, content: &str) {
    std::fs::write(work_dir.join(filename), content).unwrap();
}

/// Write a CSV file to the work directory.
pub fn write_csv(work_dir: &Path, filename: &str, content: &str) {
    std::fs::write(work_dir.join(filename), content).unwrap();
}

/// Parse SQL output into a set of individual statements, stripping the
/// BEGIN/COMMIT wrapper. Handles non-deterministic ordering from HashMap
/// iteration.
fn parse_sql_statements(sql: &str) -> HashSet<String> {
    sql.lines()
        .map(|l| l.trim().to_string())
        .filter(|l| !l.is_empty() && l != "BEGIN;" && l != "COMMIT;")
        .collect()
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
pub fn assert_wire_roundtrip(patch: &Patch) {
    let encoded = wire::encode_patch(patch).unwrap();
    let decoded = wire::decode_patch(&encoded).unwrap();

    assert_eq!(patch.head_hash, decoded.head_hash);
    assert_eq!(patch.num_blocks, decoded.num_blocks);

    let sql_before = sql::patch_to_sql(patch).unwrap();
    let sql_after = sql::patch_to_sql(&decoded).unwrap();
    assert_eq!(sql_before, sql_after, "SQL mismatch after wire roundtrip");
}
