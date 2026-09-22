use std::path::{Path, PathBuf};

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
    set_macos_install_name();
}

// Without this, the dylib records the absolute path it was built at as its
// install name, so programs linked against a shipped libleech2.dylib look for
// it under the build machine's target directory and fail to load. @rpath makes
// the library relocatable: consumers resolve it through their own rpath.
fn set_macos_install_name() {
    let target_os = std::env::var("CARGO_CFG_TARGET_OS")
        .expect("cargo must set CARGO_CFG_TARGET_OS for build scripts");
    if target_os == "macos" {
        println!("cargo:rustc-cdylib-link-arg=-Wl,-install_name,@rpath/libleech2.dylib");
    }
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
            .replace("@PREFIX@", "/usr")
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
