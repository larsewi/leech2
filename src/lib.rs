use std::ffi::{c_char, CStr};

mod block_ops;
mod config;
mod delta;
pub mod state;
mod storage;

pub mod block {
    include!(concat!(env!("OUT_DIR"), "/block.rs"));
}

#[unsafe(no_mangle)]
pub extern "C" fn init(work_dir: *const c_char) -> i32 {
    if work_dir.is_null() {
        log::error!("init: bad argument: work directory cannot be NULL");
        return -1;
    }

    let path = match unsafe { CStr::from_ptr(work_dir) }.to_str() {
        Ok(path) => path,
        Err(e) => {
            log::error!("init: bad argument: {e}");
            return -1;
        }
    };

    match config::init_impl(path) {
        Ok(_) => 0,
        Err(e) => {
            log::error!("init: {}", e);
            -1
        }
    }
}

#[unsafe(no_mangle)]
pub extern "C" fn commit() -> i32 {
    match block_ops::commit_impl() {
        Ok(_) => 0,
        Err(e) => {
            log::error!("commit: {}", e);
            -1
        }
    }
}
