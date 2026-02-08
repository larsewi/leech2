pub mod entry {
    include!(concat!(env!("OUT_DIR"), "/entry.rs"));
}
pub mod table {
    include!(concat!(env!("OUT_DIR"), "/table.rs"));
}
pub mod delta {
    include!(concat!(env!("OUT_DIR"), "/delta.rs"));
}
pub mod patch {
    include!(concat!(env!("OUT_DIR"), "/patch.rs"));
}
pub mod state {
    include!(concat!(env!("OUT_DIR"), "/state.rs"));
}
pub mod update {
    include!(concat!(env!("OUT_DIR"), "/update.rs"));
}
pub mod block {
    include!(concat!(env!("OUT_DIR"), "/block.rs"));
}
