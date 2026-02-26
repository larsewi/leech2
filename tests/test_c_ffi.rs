use std::path::PathBuf;
use std::process::Command;

fn project_root() -> PathBuf {
    PathBuf::from(env!("CARGO_MANIFEST_DIR"))
}

fn build_c_test() -> PathBuf {
    let root = project_root();
    let output = Command::new("make")
        .current_dir(root.join("tests"))
        .output()
        .expect("failed to invoke make");
    assert!(
        output.status.success(),
        "make failed:\n{}",
        String::from_utf8_lossy(&output.stderr)
    );
    root.join("tests/test_c_ffi")
}

fn setup_workdir() -> tempfile::TempDir {
    let tmp = tempfile::tempdir().unwrap();
    std::fs::write(
        tmp.path().join("config.toml"),
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
    std::fs::write(tmp.path().join("t.csv"), "1,hello\n2,world\n").unwrap();
    tmp
}

#[test]
fn test_c_ffi() {
    let bin = build_c_test();
    let tmp = setup_workdir();

    let output = Command::new(&bin)
        .arg(tmp.path().to_str().unwrap())
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

#[test]
fn test_c_ffi_valgrind() {
    // Skip if valgrind is not installed
    let valgrind_check = Command::new("valgrind").arg("--version").output();
    if valgrind_check.is_err() || !valgrind_check.unwrap().status.success() {
        eprintln!("skipping valgrind test: valgrind not found");
        return;
    }

    let bin = build_c_test();
    let tmp = setup_workdir();

    let output = Command::new("valgrind")
        .args([
            "--leak-check=full",
            "--errors-for-leak-kinds=definite,indirect",
            "--error-exitcode=1",
        ])
        .arg(&bin)
        .arg(tmp.path().to_str().unwrap())
        .output()
        .expect("failed to run valgrind");

    let stderr = String::from_utf8_lossy(&output.stderr);
    assert!(
        output.status.success(),
        "valgrind detected memory errors (exit code {:?}):\nstdout: {}\nstderr: {}",
        output.status.code(),
        String::from_utf8_lossy(&output.stdout),
        stderr,
    );
}
