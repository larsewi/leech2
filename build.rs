fn main() {
    let proto_files = [
        "proto/block.proto",
        "proto/record.proto",
        "proto/patch.proto",
        "proto/state.proto",
        "proto/table.proto",
        "proto/cell.proto",
        "proto/event.proto",
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
