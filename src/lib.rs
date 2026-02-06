use std::ffi::{CStr, c_char};
use std::path::PathBuf;

pub mod block;
mod config;
pub mod delta;
pub mod entry;
mod proto;
pub mod state;
mod storage;
pub mod table;

#[unsafe(no_mangle)]
pub extern "C" fn lch_init(work_dir: *const c_char) -> i32 {
    if work_dir.is_null() {
        log::error!("lch_commit(): Bad argument: work directory cannot be NULL");
        return -1;
    }

    let path = match unsafe { CStr::from_ptr(work_dir) }.to_str() {
        Ok(path) => path,
        Err(e) => {
            log::error!("lch_commit(): Bad argument: {e}");
            return -1;
        }
    };

    match config::init(&PathBuf::from(path)) {
        Ok(_) => 0,
        Err(e) => {
            log::error!("lch_commit(): {}", e);
            -1
        }
    }
}

#[unsafe(no_mangle)]
pub extern "C" fn lch_commit() -> i32 {
    match block::commit() {
        Ok(_) => 0,
        Err(e) => {
            log::error!("lch_commit(): {}", e);
            -1
        }
    }
}

#[unsafe(no_mangle)]
pub extern "C" fn lch_diff(block: *const c_char, flags: i32) -> i32 {
    if block.is_null() {
        log::error!("lch_diff(): Bad argument: block hash cannot be NULL");
        return -1;
    }

    let _hash = match unsafe { CStr::from_ptr(block) }.to_str() {
        Ok(hash) => hash,
        Err(e) => {
            log::error!("lch_diff(): Bad argument: {e}");
            return -1;
        }
    };

    let _squash = flags & 1;

    // TODO: Implement diff logic
    0
}
