use std::ffi::{CStr, CString, c_char, c_void};
use std::path::PathBuf;

pub mod block;
pub mod config;
pub mod delta;
pub mod entry;
pub mod head;
mod logger;
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
pub mod value;
pub mod wire;

const SUCCESS: i32 = 0;
const FAILURE: i32 = -1;

/// Run an FFI body inside `catch_unwind`, returning `default` if a panic is caught.
/// Panicking across an `extern "C"` boundary is undefined behavior, so every FFI
/// entry point routes its body through this guard as a last line of defense.
fn ffi_guard<T>(name: &str, default: T, body: impl FnOnce() -> T) -> T {
    match std::panic::catch_unwind(std::panic::AssertUnwindSafe(body)) {
        Ok(value) => value,
        Err(_) => {
            log::error!("{}: internal panic, returning failure", name);
            default
        }
    }
}

/// # Safety
/// `callback` must be a valid function pointer; passing NULL returns `LCH_FAILURE`.
/// `user_data` must be valid for the lifetime of the callback and safe to
/// access from any thread.
#[unsafe(no_mangle)]
pub unsafe extern "C" fn lch_log_init(
    callback: Option<unsafe extern "C" fn(i32, *const c_char, *mut c_void)>,
    user_data: *mut c_void,
) -> i32 {
    ffi_guard("lch_log_init", FAILURE, || {
        let Some(callback) = callback else {
            return FAILURE;
        };
        logger::init(callback, user_data);
        SUCCESS
    })
}

/// # Safety
/// `work_dir` must be a valid, non-null, null-terminated C string.
/// Returns a config handle on success, or NULL on failure.
/// The caller must free the returned handle with `lch_deinit`.
#[unsafe(no_mangle)]
pub unsafe extern "C" fn lch_init(work_dir: *const c_char) -> *mut config::Config {
    ffi_guard("lch_init", std::ptr::null_mut(), || {
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

        match crate::config::Config::load(&path) {
            Ok(config) => Box::into_raw(Box::new(config)),
            Err(e) => {
                log::error!("lch_init(): {}", e);
                std::ptr::null_mut()
            }
        }
    })
}

/// # Safety
/// `config` must be a valid pointer returned by `lch_init`, or NULL (no-op).
/// After calling this function, the config pointer is invalid and must not be used.
#[unsafe(no_mangle)]
pub unsafe extern "C" fn lch_deinit(config: *mut config::Config) {
    ffi_guard("lch_deinit", (), || {
        if !config.is_null() {
            unsafe {
                drop(Box::from_raw(config));
            }
        }
    })
}

/// # Safety
/// `config` must be a valid, non-null pointer returned by `lch_init`.
#[unsafe(no_mangle)]
pub unsafe extern "C" fn lch_block_create(config: *const config::Config) -> i32 {
    ffi_guard("lch_block_create", FAILURE, || {
        if config.is_null() {
            log::error!("lch_block_create(): Bad argument: config cannot be NULL");
            return FAILURE;
        }

        let config = unsafe { &*config };
        match block::Block::create(config) {
            Ok(_) => SUCCESS,
            Err(e) => {
                log::error!("lch_block_create(): {:#}", e);
                FAILURE
            }
        }
    })
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
    ffi_guard("lch_patch_create", FAILURE, || {
        if config.is_null() {
            log::error!("lch_patch_create(): Bad argument: config cannot be NULL");
            return FAILURE;
        }

        if out.is_null() || len.is_null() {
            log::error!("lch_patch_create(): Bad argument: out and out_len cannot be NULL");
            return FAILURE;
        }

        let config = unsafe { &*config };

        let hash = if last_known.is_null() {
            match reported::load(&config.work_dir) {
                Ok(Some(hash)) => hash,
                Ok(None) => utils::GENESIS_HASH.to_string(),
                Err(e) => {
                    log::error!("lch_patch_create(): Failed to load REPORTED: {:#}", e);
                    return FAILURE;
                }
            }
        } else {
            match unsafe { CStr::from_ptr(last_known) }.to_str() {
                Ok(hash) => hash.to_string(),
                Err(e) => {
                    log::error!("lch_patch_create(): Bad argument: {e}");
                    return FAILURE;
                }
            }
        };

        let patch = match patch::Patch::create(config, &hash) {
            Ok(patch) => patch,
            Err(e) => {
                log::error!("lch_patch_create(): {:#}", e);
                return FAILURE;
            }
        };

        let buf = match wire::encode_patch(config, &patch) {
            Ok(buf) => buf,
            Err(e) => {
                log::error!("lch_patch_create(): Failed to encode patch: {:#}", e);
                return FAILURE;
            }
        };

        let buf = buf.into_boxed_slice();
        let buf_len = buf.len();
        let ptr = Box::into_raw(buf) as *mut u8;

        unsafe {
            *out = ptr;
            *len = buf_len;
        }

        SUCCESS
    })
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
    ffi_guard("lch_patch_to_sql", FAILURE, || {
        if config.is_null() {
            log::error!("lch_patch_to_sql(): Bad argument: config cannot be NULL");
            return FAILURE;
        }

        if buf.is_null() {
            log::error!("lch_patch_to_sql(): Bad argument: buf cannot be NULL");
            return FAILURE;
        }

        if out.is_null() {
            log::error!("lch_patch_to_sql(): Bad argument: out cannot be NULL");
            return FAILURE;
        }

        let config = unsafe { &*config };
        let data = unsafe { std::slice::from_raw_parts(buf, len) };

        let patch = match wire::decode_patch(data) {
            Ok(patch) => patch,
            Err(e) => {
                log::error!("lch_patch_to_sql(): Failed to decode patch: {:#}", e);
                return FAILURE;
            }
        };

        let sql = match sql::patch_to_sql(config, &patch) {
            Ok(Some(sql)) => sql,
            Ok(None) => {
                unsafe { *out = std::ptr::null_mut() };
                return SUCCESS;
            }
            Err(e) => {
                log::error!("lch_patch_to_sql(): {:#}", e);
                return FAILURE;
            }
        };

        let cstr = match CString::new(sql) {
            Ok(cstr) => cstr,
            Err(e) => {
                log::error!("lch_patch_to_sql(): Failed to create CString: {:#}", e);
                return FAILURE;
            }
        };

        unsafe {
            *out = cstr.into_raw();
        }

        SUCCESS
    })
}

/// # Safety
/// `config` must be a valid, non-null pointer returned by `lch_init`.
/// `in_buf` must be a valid, non-null pointer to `in_len` bytes.
/// `name`, `value`, and `sql_type` must be valid, non-null, null-terminated C strings.
/// `out_buf` and `out_len` must be valid, non-null pointers.
#[unsafe(no_mangle)]
pub unsafe extern "C" fn lch_patch_inject(
    config: *const config::Config,
    in_buf: *const u8,
    in_len: usize,
    name: *const c_char,
    value: *const c_char,
    sql_type: *const c_char,
    out_buf: *mut *mut u8,
    out_len: *mut usize,
) -> i32 {
    ffi_guard("lch_patch_inject", FAILURE, || {
        if config.is_null() {
            log::error!("lch_patch_inject(): Bad argument: config cannot be NULL");
            return FAILURE;
        }

        if in_buf.is_null() {
            log::error!("lch_patch_inject(): Bad argument: in_buf cannot be NULL");
            return FAILURE;
        }

        if name.is_null() || value.is_null() || sql_type.is_null() {
            log::error!(
                "lch_patch_inject(): Bad argument: name, value, and sql_type cannot be NULL"
            );
            return FAILURE;
        }

        if out_buf.is_null() || out_len.is_null() {
            log::error!("lch_patch_inject(): Bad argument: out_buf and out_len cannot be NULL");
            return FAILURE;
        }

        let config = unsafe { &*config };
        let data = unsafe { std::slice::from_raw_parts(in_buf, in_len) };

        let name = match unsafe { CStr::from_ptr(name) }.to_str() {
            Ok(s) => s,
            Err(e) => {
                log::error!("lch_patch_inject(): Bad argument: name: {e}");
                return FAILURE;
            }
        };
        let value = match unsafe { CStr::from_ptr(value) }.to_str() {
            Ok(s) => s,
            Err(e) => {
                log::error!("lch_patch_inject(): Bad argument: value: {e}");
                return FAILURE;
            }
        };
        let sql_type = match unsafe { CStr::from_ptr(sql_type) }.to_str() {
            Ok(s) => s,
            Err(e) => {
                log::error!("lch_patch_inject(): Bad argument: sql_type: {e}");
                return FAILURE;
            }
        };

        let mut patch = match wire::decode_patch(data) {
            Ok(patch) => patch,
            Err(e) => {
                log::error!("lch_patch_inject(): Failed to decode patch: {:#}", e);
                return FAILURE;
            }
        };

        if let Err(e) = patch.inject_field(name, value, sql_type) {
            log::error!("lch_patch_inject(): {:#}", e);
            return FAILURE;
        }

        let buf = match wire::encode_patch(config, &patch) {
            Ok(buf) => buf,
            Err(e) => {
                log::error!("lch_patch_inject(): Failed to encode patch: {:#}", e);
                return FAILURE;
            }
        };

        let buf = buf.into_boxed_slice();
        let buf_len = buf.len();
        let ptr = Box::into_raw(buf) as *mut u8;

        unsafe {
            *out_buf = ptr;
            *out_len = buf_len;
        }

        SUCCESS
    })
}

/// # Safety
/// `ptr` must be null or a pointer previously returned by `lch_patch_to_sql`.
#[unsafe(no_mangle)]
pub unsafe extern "C" fn lch_sql_free(ptr: *mut c_char) {
    ffi_guard("lch_sql_free", (), || {
        if !ptr.is_null() {
            unsafe {
                drop(CString::from_raw(ptr));
            }
        }
    })
}

/// # Safety
/// `config` must be a valid, non-null pointer returned by `lch_init`.
/// `buf` must be a valid pointer to `len` bytes, previously returned by `lch_patch_create`.
#[unsafe(no_mangle)]
pub unsafe extern "C" fn lch_patch_applied(
    config: *const config::Config,
    buf: *const u8,
    len: usize,
) -> i32 {
    ffi_guard("lch_patch_applied", FAILURE, || {
        if config.is_null() {
            log::error!("lch_patch_applied(): Bad argument: config cannot be NULL");
            return FAILURE;
        }

        if buf.is_null() {
            log::error!("lch_patch_applied(): Bad argument: buf cannot be NULL");
            return FAILURE;
        }

        let config = unsafe { &*config };
        let data = unsafe { std::slice::from_raw_parts(buf, len) };

        let patch = match wire::decode_patch(data) {
            Ok(p) => p,
            Err(e) => {
                log::error!("lch_patch_applied(): Failed to decode patch: {:#}", e);
                return FAILURE;
            }
        };

        if let Err(e) = self::reported::save(&config.work_dir, &patch.head) {
            log::error!("lch_patch_applied(): Failed to save REPORTED: {:#}", e);
            return FAILURE;
        }

        SUCCESS
    })
}

/// # Safety
/// `config` must be a valid, non-null pointer returned by `lch_init`.
#[unsafe(no_mangle)]
pub unsafe extern "C" fn lch_patch_failed(config: *const config::Config) -> i32 {
    ffi_guard("lch_patch_failed", FAILURE, || {
        if config.is_null() {
            log::error!("lch_patch_failed(): Bad argument: config cannot be NULL");
            return FAILURE;
        }

        let config = unsafe { &*config };

        if let Err(e) = reported::remove(&config.work_dir) {
            log::error!("lch_patch_failed(): Failed to remove REPORTED: {:#}", e);
            return FAILURE;
        }

        SUCCESS
    })
}

/// # Safety
/// `buf` must be a valid pointer to `len` bytes, previously returned by `lch_patch_create`,
/// or NULL (no-op).
#[unsafe(no_mangle)]
pub unsafe extern "C" fn lch_patch_free(buf: *mut u8, len: usize) {
    ffi_guard("lch_patch_free", (), || {
        if !buf.is_null() {
            unsafe {
                drop(Box::from_raw(std::ptr::slice_from_raw_parts_mut(buf, len)));
            }
        }
    })
}

#[cfg(test)]
mod tests {
    use super::{FAILURE, ffi_guard, lch_log_init};

    #[test]
    fn ffi_guard_passes_through_normal_returns() {
        assert_eq!(ffi_guard("test", FAILURE, || 42), 42);
    }

    #[test]
    fn ffi_guard_catches_panics_and_returns_default() {
        let result = ffi_guard("test", FAILURE, || -> i32 { panic!("intentional") });
        assert_eq!(result, FAILURE);
    }

    #[test]
    fn lch_log_init_rejects_null_callback() {
        let result = unsafe { lch_log_init(None, std::ptr::null_mut()) };
        assert_eq!(result, FAILURE);
    }
}
