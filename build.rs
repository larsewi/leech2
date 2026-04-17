use std::path::PathBuf;

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

    generate_pkg_config();
}

fn generate_pkg_config() {
    let manifest_dir = PathBuf::from(std::env::var("CARGO_MANIFEST_DIR").unwrap());
    let template = std::fs::read_to_string(manifest_dir.join("leech2.pc.in")).unwrap();
    let version = std::env::var("CARGO_PKG_VERSION").unwrap();

    // target/<profile>/ (or target/<triple>/<profile>/ when --target is set)
    let out_dir = PathBuf::from(std::env::var("OUT_DIR").unwrap());
    let profile_dir = out_dir
        .parent()
        .and_then(|p| p.parent())
        .and_then(|p| p.parent())
        .unwrap();

    for (name, libdir) in [("leech2-deb.pc", "lib"), ("leech2-rpm.pc", "lib64")] {
        let content = template
            .replace("@VERSION@", &version)
            .replace("@LIBDIR@", libdir);
        std::fs::write(profile_dir.join(name), content).unwrap();
    }

    println!("cargo:rerun-if-changed=leech2.pc.in");
}
