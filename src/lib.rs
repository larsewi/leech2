use std::ffi::{CStr, CString, c_char};
use std::path::PathBuf;

use prost::Message;

pub mod block;
mod config;
pub mod delta;
pub mod entry;
mod head;
pub mod patch;
mod proto;
pub mod sql;
pub mod state;
mod storage;
pub mod table;
pub mod update;
mod utils;

#[unsafe(no_mangle)]
pub extern "C" fn lch_init(work_dir: *const c_char) -> i32 {
    if env_logger::try_init().is_err() {
        eprintln!("lch_init(): Failed to initialize logger (already initialized?)");
        return -1;
    }

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
    last_known: *const c_char,
    out: *mut *mut u8,
    len: *mut usize,
) -> i32 {
    if last_known.is_null() {
        log::error!("lch_patch_create(): Bad argument: block hash cannot be NULL");
        return -1;
    }

    if out.is_null() || len.is_null() {
        log::error!("lch_patch_create(): Bad argument: out and out_len cannot be NULL");
        return -1;
    }

    let hash = match unsafe { CStr::from_ptr(last_known) }.to_str() {
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

    let buf = buf.into_boxed_slice();
    let buf_len = buf.len();
    let ptr = Box::into_raw(buf) as *mut u8;

    unsafe {
        *out = ptr;
        *len = buf_len;
    }

    0
}

#[unsafe(no_mangle)]
pub extern "C" fn lch_patch_to_sql(buf: *const u8, len: usize, out: *mut *mut c_char) -> i32 {
    if buf.is_null() {
        log::error!("lch_patch_to_sql(): Bad argument: buf cannot be NULL");
        return -1;
    }

    if out.is_null() {
        log::error!("lch_patch_to_sql(): Bad argument: out cannot be NULL");
        return -1;
    }

    let data = unsafe { std::slice::from_raw_parts(buf, len) };

    let sql = match sql::patch_to_sql(data) {
        Ok(Some(s)) => s,
        Ok(None) => {
            unsafe { *out = std::ptr::null_mut() };
            return 0;
        }
        Err(e) => {
            log::error!("lch_patch_to_sql(): {}", e);
            return -1;
        }
    };

    let cstr = match CString::new(sql) {
        Ok(s) => s,
        Err(e) => {
            log::error!("lch_patch_to_sql(): Failed to create CString: {}", e);
            return -1;
        }
    };

    unsafe {
        *out = cstr.into_raw();
    }

    0
}

#[unsafe(no_mangle)]
pub extern "C" fn lch_free_buf(ptr: *mut u8, len: usize) {
    if !ptr.is_null() {
        unsafe {
            drop(Box::from_raw(std::slice::from_raw_parts_mut(ptr, len)));
        }
    }
}

#[unsafe(no_mangle)]
pub extern "C" fn lch_free_str(ptr: *mut c_char) {
    if !ptr.is_null() {
        unsafe {
            drop(CString::from_raw(ptr));
        }
    }
}
