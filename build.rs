fn main() {
    prost_build::compile_protos(&["proto/person.proto"], &["proto/"]).unwrap();
}
