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
}
