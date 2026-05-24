use std::ffi::{CStr, CString, c_char, c_void};
use std::path::PathBuf;

use crate::ffi::{
    FAILURE, FfiBuffer, FfiCell, SUCCESS, cell_from_ffi, cstr_arg, ffi_guard, null_arg,
};

pub mod block;
mod callbacks;
pub mod cell;
pub mod config;
pub mod delta;
mod ffi;
pub mod head;
mod logger;
pub mod patch;
mod proto;
pub mod record;
pub mod reported;
pub mod sql;
pub mod state;
pub mod storage;
pub mod table;
pub mod truncate;
pub mod update;
pub mod utils;
pub mod wire;

/// Install or replace the log callback.
///
/// The first call installs the global logger; subsequent calls atomically swap
/// the callback and `user_data`. After a swap, the old callback is no longer
/// invoked, but the library does not free or otherwise touch the previous
/// `user_data` — the caller owns its lifetime and must release it if needed.
///
/// Safe to call concurrently from multiple threads. Once installed, the
/// callback itself may be invoked from any thread (including in parallel),
/// so both `callback` and `user_data` must be thread-safe.
///
/// # Safety
/// `callback` must be a valid function pointer; passing NULL returns `LCH_FAILURE`.
/// `user_data` must remain valid until either the callback is replaced by a
/// later `lch_log_init` call or the process exits.
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

/// Return a pointer to a static, null-terminated string containing the
/// library version (e.g. "4.1.3"). The pointer is valid for the lifetime
/// of the process and must not be freed.
#[unsafe(no_mangle)]
pub extern "C" fn lch_version() -> *const c_char {
    static VERSION: &str = concat!(env!("CARGO_PKG_VERSION"), "\0");
    VERSION.as_ptr() as *const c_char
}

/// # Safety
/// `work_dir` must be a valid, non-null, null-terminated C string.
/// Returns a config handle on success, or NULL on failure.
/// The caller must free the returned handle with `lch_deinit`.
#[unsafe(no_mangle)]
pub unsafe extern "C" fn lch_init(work_dir: *const c_char) -> *mut config::Config {
    ffi_guard("lch_init", std::ptr::null_mut(), || {
        let Some(work_dir) = (unsafe { cstr_arg("lch_init", "work_dir", work_dir) }) else {
            return std::ptr::null_mut();
        };
        let path = PathBuf::from(work_dir);

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
            // `Drop for Config` joins any background truncation thread, so
            // this call blocks until truncation has finished.
            unsafe {
                drop(Box::from_raw(config));
            }
        }
    })
}

/// # Safety
/// `config` must be a valid, non-null pointer returned by `lch_init`.
/// `callbacks` may be NULL, or a valid pointer to an `lch_callbacks_t`
/// whose function pointers (if non-NULL) are valid `extern "C"` functions
/// and whose `usr_data` pointer remains valid for the duration of the call.
#[unsafe(no_mangle)]
pub unsafe extern "C" fn lch_block_create(
    config: *const config::Config,
    callbacks: *const callbacks::FfiCallbacks,
) -> i32 {
    ffi_guard("lch_block_create", FAILURE, || {
        if null_arg("lch_block_create", "config", config) {
            return FAILURE;
        }

        let rust_callbacks =
            (!callbacks.is_null()).then(|| callbacks::Callbacks::from(unsafe { &*callbacks }));

        let config = unsafe { &*config };
        match block::Block::create(config, rust_callbacks.as_ref()) {
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
/// `out` must be a valid, non-null pointer to an `lch_buffer_t`.
#[unsafe(no_mangle)]
pub unsafe extern "C" fn lch_patch_create(
    config: *const config::Config,
    last_known: *const c_char,
    out: *mut FfiBuffer,
) -> i32 {
    ffi_guard("lch_patch_create", FAILURE, || {
        if null_arg("lch_patch_create", "config", config) {
            return FAILURE;
        }
        if null_arg("lch_patch_create", "out", out) {
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

        unsafe { *out = buf.into() };

        SUCCESS
    })
}

/// # Safety
/// `config` must be a valid, non-null pointer returned by `lch_init`.
/// `patch` must be a valid, non-null pointer to an `lch_buffer_t` whose `data`
/// field points to `len` bytes previously returned by `lch_patch_create` or
/// `lch_patch_inject`.
/// `out` must be a valid, non-null pointer.
#[unsafe(no_mangle)]
pub unsafe extern "C" fn lch_patch_to_sql(
    config: *const config::Config,
    patch: *const FfiBuffer,
    out: *mut *mut c_char,
) -> i32 {
    ffi_guard("lch_patch_to_sql", FAILURE, || {
        if null_arg("lch_patch_to_sql", "config", config) {
            return FAILURE;
        }
        if null_arg("lch_patch_to_sql", "patch", patch) {
            return FAILURE;
        }
        if null_arg("lch_patch_to_sql", "out", out) {
            return FAILURE;
        }

        let config = unsafe { &*config };
        let patch_buf = unsafe { &*patch };
        if null_arg("lch_patch_to_sql", "patch->data", patch_buf.data) {
            return FAILURE;
        }
        let data = unsafe { std::slice::from_raw_parts(patch_buf.data, patch_buf.len) };

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
/// `r#in` must be a valid, non-null pointer to an `lch_buffer_t` whose `data`
/// field points to `len` bytes.
/// `name` must be a valid, non-null, null-terminated C string.
/// `cell` must be a valid, non-null pointer to an `lch_cell_t`; if its
/// kind is TEXT, the embedded text pointer must be a valid, null-terminated
/// C string.
/// `out` must be a valid, non-null pointer to an `lch_buffer_t`.
#[unsafe(no_mangle)]
pub unsafe extern "C" fn lch_patch_inject(
    config: *const config::Config,
    r#in: *const FfiBuffer,
    name: *const c_char,
    cell: *const FfiCell,
    out: *mut FfiBuffer,
) -> i32 {
    ffi_guard("lch_patch_inject", FAILURE, || {
        if null_arg("lch_patch_inject", "config", config) {
            return FAILURE;
        }
        if null_arg("lch_patch_inject", "in", r#in) {
            return FAILURE;
        }
        if null_arg("lch_patch_inject", "cell", cell) {
            return FAILURE;
        }
        if null_arg("lch_patch_inject", "out", out) {
            return FAILURE;
        }

        let Some(name) = (unsafe { cstr_arg("lch_patch_inject", "name", name) }) else {
            return FAILURE;
        };

        let Some(cell) = (unsafe { cell_from_ffi("lch_patch_inject", &*cell) }) else {
            return FAILURE;
        };

        let config = unsafe { &*config };
        let in_buf = unsafe { &*r#in };
        if null_arg("lch_patch_inject", "in->data", in_buf.data) {
            return FAILURE;
        }
        let data = unsafe { std::slice::from_raw_parts(in_buf.data, in_buf.len) };

        let mut patch = match wire::decode_patch(data) {
            Ok(patch) => patch,
            Err(e) => {
                log::error!("lch_patch_inject(): Failed to decode patch: {:#}", e);
                return FAILURE;
            }
        };

        if let Err(e) = patch.inject_field(name, cell) {
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

        unsafe { *out = buf.into() };

        SUCCESS
    })
}

/// # Safety
/// `ptr` must be null or a pointer to a null-terminated C string previously
/// returned by the library (e.g. from `lch_patch_to_sql` or `lch_patch_hash`).
#[unsafe(no_mangle)]
pub unsafe extern "C" fn lch_string_free(ptr: *mut c_char) {
    ffi_guard("lch_string_free", (), || {
        if !ptr.is_null() {
            unsafe {
                drop(CString::from_raw(ptr));
            }
        }
    })
}

/// # Safety
/// `patch` must be a valid, non-null pointer to an `lch_buffer_t` whose `data`
/// field points to `len` bytes previously returned by `lch_patch_create` or
/// `lch_patch_inject`.
/// `out` must be a valid, non-null pointer to a `*mut c_char`. On success it
/// receives a newly allocated, null-terminated string that the caller must
/// release with `lch_string_free`.
#[unsafe(no_mangle)]
pub unsafe extern "C" fn lch_patch_hash(patch: *const FfiBuffer, out: *mut *mut c_char) -> i32 {
    ffi_guard("lch_patch_hash", FAILURE, || {
        if null_arg("lch_patch_hash", "patch", patch) {
            return FAILURE;
        }
        if null_arg("lch_patch_hash", "out", out) {
            return FAILURE;
        }

        let patch_buf = unsafe { &*patch };
        if null_arg("lch_patch_hash", "patch->data", patch_buf.data) {
            return FAILURE;
        }
        let data = unsafe { std::slice::from_raw_parts(patch_buf.data, patch_buf.len) };

        let patch = match wire::decode_patch(data) {
            Ok(patch) => patch,
            Err(e) => {
                log::error!("lch_patch_hash(): Failed to decode patch: {:#}", e);
                return FAILURE;
            }
        };

        let cstr = match CString::new(patch.head) {
            Ok(cstr) => cstr,
            Err(e) => {
                log::error!("lch_patch_hash(): Failed to create CString: {:#}", e);
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
/// `patch` must be a valid, non-null pointer to an `lch_buffer_t` whose `data`
/// field points to `len` bytes previously returned by `lch_patch_create` or
/// `lch_patch_inject`.
#[unsafe(no_mangle)]
pub unsafe extern "C" fn lch_patch_applied(
    config: *const config::Config,
    patch: *const FfiBuffer,
) -> i32 {
    ffi_guard("lch_patch_applied", FAILURE, || {
        if null_arg("lch_patch_applied", "config", config) {
            return FAILURE;
        }
        if null_arg("lch_patch_applied", "patch", patch) {
            return FAILURE;
        }

        let config = unsafe { &*config };
        let patch_buf = unsafe { &*patch };
        if null_arg("lch_patch_applied", "patch->data", patch_buf.data) {
            return FAILURE;
        }
        let data = unsafe { std::slice::from_raw_parts(patch_buf.data, patch_buf.len) };

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
        if null_arg("lch_patch_failed", "config", config) {
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
/// `buf` must be NULL (no-op) or a valid pointer to an `lch_buffer_t` whose
/// `data` field was previously filled in by the library. A buffer with
/// `data == NULL` is a no-op.
#[unsafe(no_mangle)]
pub unsafe extern "C" fn lch_buffer_free(buf: *mut FfiBuffer) {
    ffi_guard("lch_buffer_free", (), || {
        if buf.is_null() {
            return;
        }
        let buf = unsafe { &mut *buf };
        if buf.data.is_null() {
            return;
        }
        unsafe {
            drop(Box::from_raw(std::ptr::slice_from_raw_parts_mut(
                buf.data, buf.len,
            )));
        }
        buf.data = std::ptr::null_mut();
        buf.len = 0;
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
