mod common;

use leech2::block::Block;
use leech2::config::Config;
use leech2::patch::Patch;
use leech2::sql;

#[test]
fn test_composite_primary_keys() {
    common::init_logging();
    let tmp = tempfile::tempdir().unwrap();
    let work_dir = tmp.path();

    common::write_config(
        work_dir,
        "config.toml",
        r#"
[tables.enrollments]
source = "enrollments.csv"
fields = [
    { name = "student_id", type = "NUMBER", primary-key = true },
    { name = "course_id", type = "NUMBER", primary-key = true },
    { name = "grade", type = "TEXT" },
]
"#,
    );

    // Block 1: initial enrollments
    common::write_csv(work_dir, "enrollments.csv", "1,101,A\n1,102,B\n2,101,C\n");
    let config = Config::load(work_dir).unwrap();
    let hash1 = Block::create(&config, None).unwrap();

    // Block 2: update (1,101) grade A->A+, delete (1,102), insert (2,103)
    common::write_csv(work_dir, "enrollments.csv", "1,101,A+\n2,101,C\n2,103,B\n");
    let _hash2 = Block::create(&config, None).unwrap();

    // Patch from hash1
    let patch = Patch::create(&config, &hash1).unwrap();
    let sql = sql::patch_to_sql(&config, &patch).unwrap().unwrap();

    // Composite-key columns appear in canonical (lex-sorted) order:
    // course_id before student_id.
    assert!(
        sql.contains(r#"DELETE FROM "enrollments" WHERE "course_id" = 102 AND "student_id" = 1;"#)
    );

    assert!(sql.contains(
        r#"INSERT INTO "enrollments" ("course_id", "student_id", "grade") VALUES (103, 2, 'B');"#
    ));

    assert!(sql.contains(r#"WHERE "course_id" = 101 AND "student_id" = 1;"#));
    assert!(sql.contains(r#"SET "grade" = 'A+'"#));

    common::assert_wire_roundtrip(&config, &patch);
}
