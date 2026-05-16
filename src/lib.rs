use std::ffi::{CStr, CString, c_char, c_int, c_void};
use std::path::PathBuf;

use crate::cell::Cell;

pub mod block;
pub mod cell;
pub mod config;
pub mod delta;
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

/// Logs and reports a null pointer FFI argument. Returns `true` if `ptr` is
/// null. Callers translate `true` into the function's failure sentinel.
///
/// `*mut T` coerces to `*const T` automatically, so this works for both
/// pointer kinds without casts at the call site.
fn null_arg<T>(fn_name: &str, arg_name: &str, ptr: *const T) -> bool {
    if ptr.is_null() {
        log::error!("{}(): Bad argument: {} cannot be NULL", fn_name, arg_name);
        return true;
    }
    false
}

/// Validate a required C string FFI argument and convert it to `&str`.
///
/// Logs an error and returns `None` if `ptr` is null or the bytes are not UTF-8.
/// Callers translate `None` into the function's failure sentinel.
///
/// # Safety
/// If `ptr` is non-null, it must point to a valid, null-terminated C string.
unsafe fn cstr_arg<'a>(fn_name: &str, arg_name: &str, ptr: *const c_char) -> Option<&'a str> {
    if ptr.is_null() {
        log::error!("{}(): Bad argument: {} cannot be NULL", fn_name, arg_name);
        return None;
    }
    match unsafe { CStr::from_ptr(ptr) }.to_str() {
        Ok(s) => Some(s),
        Err(e) => {
            log::error!("{}(): Bad argument: {}: {}", fn_name, arg_name, e);
            None
        }
    }
}

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
        if null_arg("lch_block_create", "config", config) {
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

/// ABI-compatible mirror of `lch_buffer_t` from `leech2.h`. An owned byte
/// buffer handed across the FFI boundary; freed with `lch_buffer_free`.
#[repr(C)]
pub struct LchBuffer {
    data: *mut u8,
    len: usize,
}

/// Encode a Rust byte vector into an `LchBuffer` whose `data` pointer is
/// owned by the caller and must be released with `lch_buffer_free`.
fn buffer_from_vec(buf: Vec<u8>) -> LchBuffer {
    let boxed = buf.into_boxed_slice();
    let len = boxed.len();
    let data = Box::into_raw(boxed) as *mut u8;
    LchBuffer { data, len }
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
    out: *mut LchBuffer,
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

        unsafe { *out = buffer_from_vec(buf) };

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
    patch: *const LchBuffer,
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

const LCH_VALUE_NULL: c_int = 0;
const LCH_VALUE_TEXT: c_int = 1;
const LCH_VALUE_NUMBER: c_int = 2;
const LCH_VALUE_BOOLEAN: c_int = 3;

/// ABI-compatible mirror of `lch_cell_t` from `leech2.h`. Only used to type
/// FFI parameters; the Rust side reads it via [`cell_from_ffi`].
#[repr(C)]
pub union LchCellPayload {
    text: *const c_char,
    number: f64,
    boolean: bool,
}

#[repr(C)]
pub struct LchCell {
    kind: c_int,
    payload: LchCellPayload,
}

/// Convert an FFI `lch_cell_t` into a domain [`Cell`]. Validates the kind
/// tag, rejects non-finite numbers, and (for TEXT) verifies the pointer is
/// non-null and UTF-8. Logs an error and returns `None` on failure; callers
/// translate `None` into the function's failure sentinel.
///
/// # Safety
/// When `cell.kind == LCH_VALUE_TEXT`, `cell.payload.text` must point to a
/// valid, null-terminated C string. A null pointer is rejected with an
/// error; use `LCH_VALUE_NULL` to represent a null value.
unsafe fn cell_from_ffi(fn_name: &str, cell: &LchCell) -> Option<Cell> {
    match cell.kind {
        LCH_VALUE_NULL => Some(Cell::Null),
        LCH_VALUE_TEXT => {
            let ptr = unsafe { cell.payload.text };
            let s = unsafe { cstr_arg(fn_name, "cell.text", ptr) }?;
            Some(Cell::Text(s.to_string()))
        }
        LCH_VALUE_NUMBER => match Cell::number(unsafe { cell.payload.number }) {
            Ok(cell) => Some(cell),
            Err(e) => {
                log::error!("{}(): Bad argument: cell.number: {:#}", fn_name, e);
                None
            }
        },
        LCH_VALUE_BOOLEAN => Some(Cell::Boolean(unsafe { cell.payload.boolean })),
        other => {
            log::error!(
                "{}(): Bad argument: cell.kind: unknown kind tag {}",
                fn_name,
                other
            );
            None
        }
    }
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
    r#in: *const LchBuffer,
    name: *const c_char,
    cell: *const LchCell,
    out: *mut LchBuffer,
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

        unsafe { *out = buffer_from_vec(buf) };

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
/// `patch` must be a valid, non-null pointer to an `lch_buffer_t` whose `data`
/// field points to `len` bytes previously returned by `lch_patch_create` or
/// `lch_patch_inject`.
#[unsafe(no_mangle)]
pub unsafe extern "C" fn lch_patch_applied(
    config: *const config::Config,
    patch: *const LchBuffer,
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
pub unsafe extern "C" fn lch_buffer_free(buf: *mut LchBuffer) {
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
