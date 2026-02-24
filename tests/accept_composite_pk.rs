mod common;

use leech2::block::Block;
use leech2::config::Config;
use leech2::patch::Patch;
use leech2::sql;

#[test]
fn test_composite_primary_keys() {
    let tmp = tempfile::tempdir().unwrap();
    let work_dir = tmp.path();

    common::write_config(
        work_dir,
        "config.toml",
        r#"
[tables.enrollments]
source = "enrollments.csv"
fields = [
    { name = "student_id", type = "INTEGER", primary-key = true },
    { name = "course_id", type = "INTEGER", primary-key = true },
    { name = "grade", type = "TEXT" },
]
"#,
    );

    // Block 1: initial enrollments
    common::write_csv(
        work_dir,
        "enrollments.csv",
        "1,101,A\n1,102,B\n2,101,C\n",
    );
    Config::init(work_dir).unwrap();
    let hash1 = Block::create().unwrap();

    // Block 2: update (1,101) grade A->A+, delete (1,102), insert (2,103)
    common::write_csv(
        work_dir,
        "enrollments.csv",
        "1,101,A+\n2,101,C\n2,103,B\n",
    );
    let _hash2 = Block::create().unwrap();

    // Patch from hash1
    let patch = Patch::create(&hash1).unwrap();
    let sql = sql::patch_to_sql(&patch).unwrap().unwrap();

    // DELETE should use composite key with AND
    assert!(
        sql.contains(r#"DELETE FROM "enrollments" WHERE "student_id" = 1 AND "course_id" = 102;"#)
    );

    // INSERT should include all columns
    assert!(sql.contains(
        r#"INSERT INTO "enrollments" ("student_id", "course_id", "grade") VALUES (2, 103, 'B');"#
    ));

    // UPDATE should use composite key in WHERE
    assert!(sql.contains(r#"WHERE "student_id" = 1 AND "course_id" = 101;"#));
    assert!(sql.contains(r#"SET "grade" = 'A+'"#));

    common::assert_wire_roundtrip(&patch);
}
