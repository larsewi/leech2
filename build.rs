use std::path::{Path, PathBuf};
use std::process::Command;

fn main() {
    let proto_files = [
        "proto/block.proto",
        "proto/delta.proto",
        "proto/record.proto",
        "proto/injected.proto",
        "proto/patch.proto",
        "proto/state.proto",
        "proto/table.proto",
        "proto/update.proto",
        "proto/cell.proto",
    ];
    prost_build::compile_protos(&proto_files, &["proto/"])
        .expect("prost_build failed to compile .proto files; check protoc is installed");
    for proto in &proto_files {
        println!("cargo:rerun-if-changed={proto}");
    }

    // Forward build metadata so integration tests can compile C code and find
    // the cdylib without hard-coding paths or profiles.
    for var in ["TARGET", "HOST", "PROFILE"] {
        let value = std::env::var(var)
            .unwrap_or_else(|_| panic!("cargo must set environment variable {var}"));
        println!("cargo:rustc-env=LEECH2_{var}={value}");
    }

    let manifest_dir = PathBuf::from(
        std::env::var("CARGO_MANIFEST_DIR")
            .expect("cargo must set CARGO_MANIFEST_DIR for build scripts"),
    );
    let version = std::env::var("CARGO_PKG_VERSION").expect("cargo must set CARGO_PKG_VERSION");
    let profile_dir = profile_dir();

    generate_pkg_config(&manifest_dir, &profile_dir, &version);
    generate_man_pages(&manifest_dir, &profile_dir, &version);
}

// target/<profile>/ (or target/<triple>/<profile>/ when --target is set)
fn profile_dir() -> PathBuf {
    let out_dir =
        PathBuf::from(std::env::var("OUT_DIR").expect("cargo must set OUT_DIR for build scripts"));
    // OUT_DIR is target/<profile>/build/<crate>-<hash>/out; walk up three
    // levels to reach target/<profile>/.
    out_dir
        .parent()
        .and_then(|p| p.parent())
        .and_then(|p| p.parent())
        .unwrap_or_else(|| panic!("OUT_DIR {:?} has fewer than three ancestors", out_dir))
        .to_path_buf()
}

fn generate_pkg_config(manifest_dir: &Path, profile_dir: &Path, version: &str) {
    let template_path = manifest_dir.join("leech2.pc.in");
    let template = std::fs::read_to_string(&template_path).unwrap_or_else(|e| {
        panic!(
            "failed to read pkg-config template '{}': {}",
            template_path.display(),
            e
        )
    });

    for (name, libdir) in [("leech2-deb.pc", "lib"), ("leech2-rpm.pc", "lib64")] {
        let content = template
            .replace("@VERSION@", version)
            .replace("@LIBDIR@", libdir);
        let out_path = profile_dir.join(name);
        std::fs::write(&out_path, content).unwrap_or_else(|e| {
            panic!(
                "failed to write pkg-config file '{}': {}",
                out_path.display(),
                e
            )
        });
    }

    println!("cargo:rerun-if-changed=leech2.pc.in");
}

fn generate_man_pages(manifest_dir: &Path, profile_dir: &Path, version: &str) {
    let out_dir = profile_dir.join("man");
    std::fs::create_dir_all(&out_dir).unwrap_or_else(|e| {
        panic!(
            "failed to create man output directory '{}': {}",
            out_dir.display(),
            e
        )
    });

    let date = last_commit_date(manifest_dir);

    for name in ["lch.1", "libleech2.3"] {
        let template_path = manifest_dir.join("man").join(format!("{name}.in"));
        let content = std::fs::read_to_string(&template_path)
            .unwrap_or_else(|e| {
                panic!(
                    "failed to read man template '{}': {}",
                    template_path.display(),
                    e
                )
            })
            .replace("@VERSION@", version)
            .replace("@DATE@", &date);
        let out_path = out_dir.join(name);
        std::fs::write(&out_path, content)
            .unwrap_or_else(|e| panic!("failed to write man page '{}': {}", out_path.display(), e));
        println!("cargo:rerun-if-changed=man/{name}.in");
    }
}

// Resolve the source date stamp for generated man pages. Prefers the last
// commit's author date so it matches the release tag rather than whenever
// `cargo build` happened to run.
fn last_commit_date(manifest_dir: &Path) -> String {
    Command::new("git")
        .args(["log", "-1", "--format=%cs"])
        .current_dir(manifest_dir)
        .output()
        .ok()
        .filter(|output| output.status.success())
        .and_then(|output| String::from_utf8(output.stdout).ok())
        .map(|s| s.trim().to_string())
        .filter(|s| !s.is_empty())
        .unwrap_or_else(|| "unknown".to_string())
}
