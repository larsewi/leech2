//! End-to-end tests for the `--dry-run` flag: create commands must compute and
//! report ("Would have ...") without touching the work directory on disk.

use std::path::Path;
use std::process::{Command, Output};

/// Run the `lch` binary with the work directory rooted at `base` and return
/// its output.
fn lch(base: &Path, args: &[&str]) -> Output {
    let mut command = Command::new(env!("CARGO_BIN_EXE_lch"));
    command.arg("-C").arg(base);
    command.args(args);
    command.output().expect("failed to run lch")
}

/// `-C <base>` rebases the work directory onto `<base>/.leech2`, and state lives
/// in a `state` subdirectory of it by default.
fn state_dir(base: &Path) -> std::path::PathBuf {
    base.join(".leech2").join("state")
}

fn assert_success(output: &Output) {
    assert!(
        output.status.success(),
        "lch failed: {}",
        String::from_utf8_lossy(&output.stderr)
    );
}

#[test]
fn block_create_dry_run_writes_nothing() {
    let tmp = tempfile::tempdir().unwrap();
    let base = tmp.path();

    assert_success(&lch(base, &["init"]));

    let output = lch(base, &["block", "create", "--dry-run"]);
    assert_success(&output);

    let stderr = String::from_utf8_lossy(&output.stderr);
    assert!(
        stderr.contains("Would have written") && stderr.contains("HEAD"),
        "stderr was: {stderr}"
    );

    // No chain state was advanced: HEAD must not exist.
    assert!(!state_dir(base).join("HEAD").exists());
}

#[test]
fn patch_create_dry_run_writes_no_patch_file() {
    let tmp = tempfile::tempdir().unwrap();
    let base = tmp.path();

    assert_success(&lch(base, &["init"]));
    assert_success(&lch(base, &["block", "create"]));

    let output = lch(base, &["patch", "create", "--dry-run"]);
    assert_success(&output);

    let stderr = String::from_utf8_lossy(&output.stderr);
    assert!(
        stderr.contains("Would have written") && stderr.contains("PATCH"),
        "stderr was: {stderr}"
    );

    // The PATCH file must not have been written.
    assert!(!state_dir(base).join("PATCH").exists());
}
