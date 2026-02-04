fn main() {
    prost_build::compile_protos(
        &[
            "proto/block.proto",
            "proto/delta.proto",
            "proto/state.proto",
            "proto/table.proto",
        ],
        &["proto/"],
    )
    .unwrap();
}
