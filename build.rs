fn main() {
    prost_build::compile_protos(
        &[
            "proto/block.proto",
            "proto/delta.proto",
            "proto/entry.proto",
            "proto/patch.proto",
            "proto/state.proto",
            "proto/table.proto",
            "proto/update.proto",
        ],
        &["proto/"],
    )
    .unwrap();
}
