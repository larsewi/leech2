use std::path::{Path, PathBuf};
use std::process::Command;

fn main() {
    prost_build::compile_protos(
        &[
            "proto/block.proto",
            "proto/delta.proto",
            "proto/entry.proto",
            "proto/injected.proto",
            "proto/patch.proto",
            "proto/state.proto",
            "proto/table.proto",
            "proto/update.proto",
            "proto/cell.proto",
        ],
        &["proto/"],
    )
    .unwrap();

    // Forward build metadata so integration tests can compile C code and find
    // the cdylib without hard-coding paths or profiles.
    for var in ["TARGET", "HOST", "PROFILE"] {
        println!(
            "cargo:rustc-env=LEECH2_{var}={}",
            std::env::var(var).unwrap()
        );
    }

    let manifest_dir = PathBuf::from(std::env::var("CARGO_MANIFEST_DIR").unwrap());
    let version = std::env::var("CARGO_PKG_VERSION").unwrap();
    let profile_dir = profile_dir();

    generate_pkg_config(&manifest_dir, &profile_dir, &version);
    generate_man_pages(&manifest_dir, &profile_dir, &version);
}

// target/<profile>/ (or target/<triple>/<profile>/ when --target is set)
fn profile_dir() -> PathBuf {
    let out_dir = PathBuf::from(std::env::var("OUT_DIR").unwrap());
    out_dir
        .parent()
        .and_then(|p| p.parent())
        .and_then(|p| p.parent())
        .unwrap()
        .to_path_buf()
}

fn generate_pkg_config(manifest_dir: &Path, profile_dir: &Path, version: &str) {
    let template = std::fs::read_to_string(manifest_dir.join("leech2.pc.in")).unwrap();

    for (name, libdir) in [("leech2-deb.pc", "lib"), ("leech2-rpm.pc", "lib64")] {
        let content = template
            .replace("@VERSION@", version)
            .replace("@LIBDIR@", libdir);
        std::fs::write(profile_dir.join(name), content).unwrap();
    }

    println!("cargo:rerun-if-changed=leech2.pc.in");
}

fn generate_man_pages(manifest_dir: &Path, profile_dir: &Path, version: &str) {
    let out_dir = profile_dir.join("man");
    std::fs::create_dir_all(&out_dir).unwrap();

    let date = last_commit_date(manifest_dir);

    for name in ["lch.1", "libleech2.3"] {
        let template_path = manifest_dir.join("man").join(format!("{name}.in"));
        let content = std::fs::read_to_string(&template_path)
            .unwrap()
            .replace("@VERSION@", version)
            .replace("@DATE@", &date);
        std::fs::write(out_dir.join(name), content).unwrap();
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
