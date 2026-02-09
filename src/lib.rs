use std::ffi::{CStr, c_char};
use std::mem;
use std::path::PathBuf;

use prost::Message;

pub mod block;
mod config;
pub mod delta;
pub mod entry;
mod head;
pub mod patch;
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
pub extern "C" fn lch_block_create() -> i32 {
    match block::Block::create() {
        Ok(_) => 0,
        Err(e) => {
            log::error!("lch_block_create(): {}", e);
            -1
        }
    }
}

#[unsafe(no_mangle)]
pub extern "C" fn lch_patch_create(
    block: *const c_char,
    out: *mut *mut u8,
    out_len: *mut usize,
) -> i32 {
    if block.is_null() {
        log::error!("lch_patch_create(): Bad argument: block hash cannot be NULL");
        return -1;
    }

    if out.is_null() || out_len.is_null() {
        log::error!("lch_patch_create(): Bad argument: out and out_len cannot be NULL");
        return -1;
    }

    let hash = match unsafe { CStr::from_ptr(block) }.to_str() {
        Ok(hash) => hash,
        Err(e) => {
            log::error!("lch_patch_create(): Bad argument: {e}");
            return -1;
        }
    };

    let p = match patch::Patch::create(hash) {
        Ok(p) => p,
        Err(e) => {
            log::error!("lch_patch_create(): {}", e);
            return -1;
        }
    };

    let mut buf = Vec::new();
    if let Err(e) = p.encode(&mut buf) {
        log::error!("lch_patch_create(): Failed to encode patch: {}", e);
        return -1;
    }

    let len = buf.len();
    let ptr = buf.as_mut_ptr();
    mem::forget(buf);

    unsafe {
        *out = ptr;
        *out_len = len;
    }

    0
}

#[unsafe(no_mangle)]
pub extern "C" fn lch_free(ptr: *mut u8, len: usize) {
    if !ptr.is_null() {
        unsafe {
            drop(Vec::from_raw_parts(ptr, len, len));
        }
    }
}
