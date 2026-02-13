use std::path::PathBuf;
use std::process::Command;

fn project_root() -> PathBuf {
    PathBuf::from(env!("CARGO_MANIFEST_DIR"))
}

#[test]
fn test_c_ffi() {
    let root = project_root();

    // Build the C test binary via make
    let output = Command::new("make")
        .current_dir(root.join("tests"))
        .output()
        .expect("failed to invoke make");
    assert!(
        output.status.success(),
        "make failed:\n{}",
        String::from_utf8_lossy(&output.stderr)
    );

    // Set up work directory with test fixtures
    let tmp = tempfile::tempdir().unwrap();
    let work_dir = tmp.path().join("workdir");
    std::fs::create_dir_all(&work_dir).unwrap();
    std::fs::write(
        work_dir.join("config.toml"),
        "\
[tables.t]
source = \"t.csv\"
fields = [
    { name = \"id\", type = \"INTEGER\", primary-key = true },
    { name = \"val\", type = \"TEXT\" },
]
",
    )
    .unwrap();
    std::fs::write(work_dir.join("t.csv"), "1,hello\n2,world\n").unwrap();

    // Run the C test binary
    let bin = root.join("tests/test_c_ffi");
    let output = Command::new(&bin)
        .arg(work_dir.to_str().unwrap())
        .output()
        .expect("failed to run C test binary");
    assert!(
        output.status.success(),
        "C FFI test failed (exit code {:?}):\nstdout: {}\nstderr: {}",
        output.status.code(),
        String::from_utf8_lossy(&output.stdout),
        String::from_utf8_lossy(&output.stderr),
    );
}
