use std::ffi::{CStr, CString, c_char};
use std::path::PathBuf;

pub mod block;
pub mod config;
pub mod delta;
pub mod entry;
pub mod head;
pub mod patch;
mod proto;
pub mod reported;
pub mod sql;
pub mod state;
pub mod storage;
pub mod table;
pub mod truncate;
pub mod update;
pub mod utils;
pub mod wire;

/// # Safety
/// `work_dir` must be a valid, non-null, null-terminated C string.
/// Returns a config handle on success, or NULL on failure.
/// The caller must free the returned handle with `lch_deinit`.
#[unsafe(no_mangle)]
pub unsafe extern "C" fn lch_init(work_dir: *const c_char) -> *mut config::Config {
    let _ = env_logger::Builder::from_env(env_logger::Env::new().filter("LEECH2_LOG")).try_init();

    if work_dir.is_null() {
        log::error!("lch_init(): Bad argument: work directory cannot be NULL");
        return std::ptr::null_mut();
    }

    let path = match unsafe { CStr::from_ptr(work_dir) }.to_str() {
        Ok(path) => PathBuf::from(path),
        Err(e) => {
            log::error!("lch_init(): Bad argument: {e}");
            return std::ptr::null_mut();
        }
    };

    log::debug!("lch_init(work_dir={})", path.display());

    match config::Config::load(&path) {
        Ok(cfg) => Box::into_raw(Box::new(cfg)),
        Err(e) => {
            log::error!("lch_init(): {}", e);
            std::ptr::null_mut()
        }
    }
}

/// # Safety
/// `config` must be a valid pointer returned by `lch_init`, or NULL (no-op).
/// After calling this function, the config pointer is invalid and must not be used.
#[unsafe(no_mangle)]
pub unsafe extern "C" fn lch_deinit(config: *mut config::Config) {
    if !config.is_null() {
        unsafe {
            drop(Box::from_raw(config));
        }
    }
}

/// # Safety
/// `config` must be a valid, non-null pointer returned by `lch_init`.
#[unsafe(no_mangle)]
pub unsafe extern "C" fn lch_block_create(config: *const config::Config) -> i32 {
    if config.is_null() {
        log::error!("lch_block_create(): Bad argument: config cannot be NULL");
        return -1;
    }

    let config = unsafe { &*config };
    match block::Block::create(config) {
        Ok(_) => 0,
        Err(e) => {
            log::error!("lch_block_create(): {:#}", e);
            -1
        }
    }
}

/// # Safety
/// `config` must be a valid, non-null pointer returned by `lch_init`.
/// `last_known` must be a valid, null-terminated C string, or NULL.
/// If NULL, the REPORTED hash is used; if REPORTED does not exist, genesis is used.
/// `out` and `len` must be valid, non-null pointers.
#[unsafe(no_mangle)]
pub unsafe extern "C" fn lch_patch_create(
    config: *const config::Config,
    last_known: *const c_char,
    out: *mut *mut u8,
    len: *mut usize,
) -> i32 {
    if config.is_null() {
        log::error!("lch_patch_create(): Bad argument: config cannot be NULL");
        return -1;
    }

    if out.is_null() || len.is_null() {
        log::error!("lch_patch_create(): Bad argument: out and out_len cannot be NULL");
        return -1;
    }

    let config = unsafe { &*config };

    let hash = if last_known.is_null() {
        match reported::load(&config.work_dir) {
            Ok(Some(hash)) => hash,
            Ok(None) => utils::GENESIS_HASH.to_string(),
            Err(e) => {
                log::error!("lch_patch_create(): Failed to load REPORTED: {:#}", e);
                return -1;
            }
        }
    } else {
        match unsafe { CStr::from_ptr(last_known) }.to_str() {
            Ok(hash) => hash.to_string(),
            Err(e) => {
                log::error!("lch_patch_create(): Bad argument: {e}");
                return -1;
            }
        }
    };

    let p = match patch::Patch::create(config, &hash) {
        Ok(p) => p,
        Err(e) => {
            log::error!("lch_patch_create(): {:#}", e);
            return -1;
        }
    };

    let buf = match wire::encode_patch(config, &p) {
        Ok(buf) => buf,
        Err(e) => {
            log::error!("lch_patch_create(): Failed to encode patch: {:#}", e);
            return -1;
        }
    };

    let buf = buf.into_boxed_slice();
    let buf_len = buf.len();
    let ptr = Box::into_raw(buf) as *mut u8;

    unsafe {
        *out = ptr;
        *len = buf_len;
    }

    0
}

/// # Safety
/// `config` must be a valid, non-null pointer returned by `lch_init`.
/// `buf` must be a valid, non-null pointer to `len` bytes.
/// `out` must be a valid, non-null pointer.
#[unsafe(no_mangle)]
pub unsafe extern "C" fn lch_patch_to_sql(
    config: *const config::Config,
    buf: *const u8,
    len: usize,
    out: *mut *mut c_char,
) -> i32 {
    if config.is_null() {
        log::error!("lch_patch_to_sql(): Bad argument: config cannot be NULL");
        return -1;
    }

    if buf.is_null() {
        log::error!("lch_patch_to_sql(): Bad argument: buf cannot be NULL");
        return -1;
    }

    if out.is_null() {
        log::error!("lch_patch_to_sql(): Bad argument: out cannot be NULL");
        return -1;
    }

    let config = unsafe { &*config };
    let data = unsafe { std::slice::from_raw_parts(buf, len) };

    let patch = match wire::decode_patch(data) {
        Ok(p) => p,
        Err(e) => {
            log::error!("lch_patch_to_sql(): Failed to decode patch: {:#}", e);
            return -1;
        }
    };

    let sql = match sql::patch_to_sql(config, &patch) {
        Ok(Some(s)) => s,
        Ok(None) => {
            unsafe { *out = std::ptr::null_mut() };
            return 0;
        }
        Err(e) => {
            log::error!("lch_patch_to_sql(): {:#}", e);
            return -1;
        }
    };

    let cstr = match CString::new(sql) {
        Ok(s) => s,
        Err(e) => {
            log::error!("lch_patch_to_sql(): Failed to create CString: {:#}", e);
            return -1;
        }
    };

    unsafe {
        *out = cstr.into_raw();
    }

    0
}

/// # Safety
/// `ptr` must be null or a pointer previously returned by `lch_patch_to_sql`.
#[unsafe(no_mangle)]
pub unsafe extern "C" fn lch_free_sql(ptr: *mut c_char) {
    if !ptr.is_null() {
        unsafe {
            drop(CString::from_raw(ptr));
        }
    }
}

/// # Safety
/// `config` must be a valid, non-null pointer returned by `lch_init`.
/// `buf` must be a valid pointer to `len` bytes, previously returned by `lch_patch_create`.
/// The buffer is always freed regardless of the `flags` value or any errors.
#[unsafe(no_mangle)]
pub unsafe extern "C" fn lch_patch_free(
    config: *const config::Config,
    buf: *mut u8,
    len: usize,
    flags: i32,
) -> i32 {
    const LCH_PATCH_APPLIED: i32 = 1;

    if config.is_null() {
        log::error!("lch_patch_free(): Bad argument: config cannot be NULL");
        // Still free the buffer even on argument errors
        if !buf.is_null() {
            unsafe {
                drop(Box::from_raw(std::ptr::slice_from_raw_parts_mut(buf, len)));
            }
        }
        return -1;
    }

    let config = unsafe { &*config };

    // Reconstruct the Box<[u8]> to reclaim the allocation. Converting to Vec
    // reuses the same allocation without copying, and the Vec is dropped (freed)
    // when this function returns — regardless of early returns below.
    let data = if buf.is_null() {
        Vec::new()
    } else {
        unsafe { Box::from_raw(std::ptr::slice_from_raw_parts_mut(buf, len)) }.into_vec()
    };

    if flags & LCH_PATCH_APPLIED != 0 {
        let patch = match wire::decode_patch(&data) {
            Ok(p) => p,
            Err(e) => {
                log::error!("lch_patch_free(): Failed to decode patch: {:#}", e);
                return -1; // data is dropped here, freeing the buffer
            }
        };

        if let Err(e) = self::reported::save(&config.work_dir, &patch.head_hash) {
            log::error!("lch_patch_free(): Failed to save REPORTED: {:#}", e);
            return -1; // data is dropped here, freeing the buffer
        }
    }

    // data is dropped here, freeing the buffer
    0
}
