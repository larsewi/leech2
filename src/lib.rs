use std::ffi::{CStr, c_char};
use std::path::PathBuf;

pub mod block;
mod config;
pub mod delta;
mod diff;
pub mod entry;
mod head;
mod proto;
pub mod state;
mod storage;
pub mod table;
pub mod update;
mod utils;

#[unsafe(no_mangle)]
pub extern "C" fn lch_init(work_dir: *const c_char) -> i32 {
    env_logger::init();

    if work_dir.is_null() {
        log::error!("lch_init(): Bad argument: work directory cannot be NULL");
        return -1;
    }

    let path = match unsafe { CStr::from_ptr(work_dir) }.to_str() {
        Ok(path) => PathBuf::from(path),
        Err(e) => {
            log::error!("lch_init(): Bad argument: {e}");
            return -1;
        }
    };

    log::debug!("lch_init(work_dir={})", path.display());

    if let Err(e) = config::Config::init(&path) {
        log::error!("lch_init(): {}", e);
        return -1;
    }

    0
}

#[unsafe(no_mangle)]
pub extern "C" fn lch_commit() -> i32 {
    match block::Block::create() {
        Ok(_) => 0,
        Err(e) => {
            log::error!("lch_commit(): {}", e);
            -1
        }
    }
}

#[unsafe(no_mangle)]
pub extern "C" fn lch_diff(block: *const c_char) -> i32 {
    if block.is_null() {
        log::error!("lch_diff(): Bad argument: block hash cannot be NULL");
        return -1;
    }

    let hash = match unsafe { CStr::from_ptr(block) }.to_str() {
        Ok(hash) => hash,
        Err(e) => {
            log::error!("lch_diff(): Bad argument: {e}");
            return -1;
        }
    };

    match diff::diff(hash) {
        Ok(_) => 0,
        Err(e) => {
            log::error!("lch_diff(): {}", e);
            -1
        }
    }
}
