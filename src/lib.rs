use std::ffi::{c_char, CStr};

pub mod block;
mod config;
pub mod delta;
pub mod entry;
mod proto;
pub mod state;
mod storage;
pub mod table;

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

    match config::init(path) {
        Ok(_) => 0,
        Err(e) => {
            log::error!("init: {}", e);
            -1
        }
    }
}

#[unsafe(no_mangle)]
pub extern "C" fn commit() -> i32 {
    match block::commit() {
        Ok(_) => 0,
        Err(e) => {
            log::error!("commit: {}", e);
            -1
        }
    }
}
