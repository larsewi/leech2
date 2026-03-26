use std::path::{Path, PathBuf};
use std::process::Command;
use std::sync::OnceLock;

fn project_root() -> PathBuf {
    PathBuf::from(env!("CARGO_MANIFEST_DIR"))
}

fn lib_dir() -> PathBuf {
    project_root().join("target").join(env!("LEECH2_PROFILE"))
}

fn build_c_test() -> PathBuf {
    let root = project_root();
    let tests_dir = root.join("tests");
    let include_dir = root.join("include");
    let lib_dir = lib_dir();
    let source = tests_dir.join("test_c_ffi.c");

    // opt_level is required because the cc crate reads OPT_LEVEL from the
    // environment, which is only set by cargo for build scripts.
    let compiler = cc::Build::new()
        .target(env!("LEECH2_TARGET"))
        .host(env!("LEECH2_HOST"))
        .opt_level(0)
        .get_compiler();

    if compiler.is_like_msvc() {
        let obj = tests_dir.join("test_c_ffi.obj");
        let bin = tests_dir.join("test_c_ffi.exe");

        // Compile
        let output = compiler
            .to_command()
            .args(["/nologo", "/W4"])
            .arg(format!("/I{}", include_dir.display()))
            .args(["/c", source.to_str().unwrap()])
            .arg(format!("/Fo:{}", obj.display()))
            .output()
            .expect("failed to invoke MSVC compiler");
        assert!(
            output.status.success(),
            "MSVC compilation failed:\n{}",
            String::from_utf8_lossy(&output.stderr)
        );

        // Link (use cl.exe as the driver so we don't need link.exe on PATH)
        let output = compiler
            .to_command()
            .args(["/nologo", obj.to_str().unwrap()])
            .arg(format!("/Fe:{}", bin.display()))
            .arg("/link")
            .arg(format!("/LIBPATH:{}", lib_dir.display()))
            .arg("leech2.dll.lib")
            .output()
            .expect("failed to invoke MSVC linker");
        assert!(
            output.status.success(),
            "MSVC linking failed:\n{}",
            String::from_utf8_lossy(&output.stderr)
        );

        bin
    } else {
        let obj = tests_dir.join("test_c_ffi.o");
        let bin = tests_dir.join(if cfg!(target_os = "windows") {
            "test_c_ffi.exe"
        } else {
            "test_c_ffi"
        });

        // Compile
        let output = compiler
            .to_command()
            .args(["-g", "-Wall", "-Wextra", "-Wconversion"])
            .arg(format!("-I{}", include_dir.display()))
            .args(["-c", source.to_str().unwrap(), "-o", obj.to_str().unwrap()])
            .output()
            .expect("failed to invoke C compiler");
        assert!(
            output.status.success(),
            "C compilation failed:\n{}",
            String::from_utf8_lossy(&output.stderr)
        );

        // Link
        let mut link_cmd = compiler.to_command();
        link_cmd
            .args([obj.to_str().unwrap(), "-o", bin.to_str().unwrap()])
            .arg(format!("-L{}", lib_dir.display()))
            .arg("-lleech2");
        if !cfg!(target_os = "windows") {
            link_cmd.arg(format!("-Wl,-rpath,{}", lib_dir.display()));
        }
        let output = link_cmd.output().expect("failed to invoke C linker");
        assert!(
            output.status.success(),
            "C linking failed:\n{}",
            String::from_utf8_lossy(&output.stderr)
        );

        bin
    }
}

/// Get the compiled C test binary, building it at most once.
fn get_c_test_binary() -> &'static PathBuf {
    static BIN: OnceLock<PathBuf> = OnceLock::new();
    BIN.get_or_init(build_c_test)
}

/// Create a [`Command`] for the C test binary.
///
/// On Windows, prepends the library directory to `PATH` so the dynamic linker
/// can find `leech2.dll` at runtime (there is no rpath on Windows).
fn c_test_cmd(bin: &Path) -> Command {
    let mut cmd = Command::new(bin);
    if cfg!(target_os = "windows") {
        let path = std::env::var_os("PATH").unwrap_or_default();
        let mut paths = std::env::split_paths(&path).collect::<Vec<_>>();
        paths.insert(0, lib_dir());
        cmd.env("PATH", std::env::join_paths(paths).unwrap());
    }
    cmd
}

fn setup_workdir() -> tempfile::TempDir {
    let tmp = tempfile::tempdir().unwrap();
    std::fs::write(
        tmp.path().join("config.toml"),
        "\
[tables.t]
source = \"t.csv\"
fields = [
    { name = \"id\", type = \"NUMBER\", primary-key = true },
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
    let bin = get_c_test_binary();
    let tmp = setup_workdir();

    let output = c_test_cmd(bin)
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
#[cfg_attr(target_os = "windows", ignore)]
fn test_c_ffi_valgrind() {
    // Skip if valgrind is not installed
    let valgrind_check = Command::new("valgrind").arg("--version").output();
    if valgrind_check.is_err() || !valgrind_check.unwrap().status.success() {
        eprintln!("skipping valgrind test: valgrind not found");
        return;
    }

    let bin = get_c_test_binary();
    let tmp = setup_workdir();

    let output = Command::new("valgrind")
        .args([
            "--leak-check=full",
            "--errors-for-leak-kinds=definite,indirect",
            "--error-exitcode=1",
        ])
        .arg(bin.as_path())
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
