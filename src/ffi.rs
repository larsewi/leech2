//! Internal FFI plumbing: error sentinels, panic guards, repr-C type mirrors,
//! and the conversion helpers used by every `extern "C"` entry point in
//! `lib.rs` and by the callback adapter in `callbacks.rs`.
//!
//! Nothing in this module is part of leech2's Rust public API; the module is
//! declared `mod ffi;` (private) at the crate root.

use std::ffi::{CStr, c_char, c_int};

use crate::cell::Cell;

/// `LCH_SUCCESS` from `leech2.h`.
pub const SUCCESS: i32 = 0;
/// `LCH_FAILURE` from `leech2.h`.
pub const FAILURE: i32 = -1;
/// `LCH_END_OF_TABLE` from `leech2.h`. `lch_read_cell_cb_t` return code: the
/// row at this index does not exist; iteration for this table stops.
pub const END_OF_TABLE: i32 = 1;
/// `LCH_SKIP_RECORD` from `leech2.h`. `lch_read_cell_cb_t` return code:
/// drop the current row; advance to the next row without consulting any
/// further fields.
pub const SKIP_RECORD: i32 = 2;

/// `LCH_VALUE_NULL` from `leech2.h`. Cell kind tag.
pub const VALUE_NULL: c_int = 0;
/// `LCH_VALUE_TEXT` from `leech2.h`. Cell kind tag.
const VALUE_TEXT: c_int = 1;
/// `LCH_VALUE_NUMBER` from `leech2.h`. Cell kind tag.
const VALUE_NUMBER: c_int = 2;
/// `LCH_VALUE_BOOLEAN` from `leech2.h`. Cell kind tag.
const VALUE_BOOLEAN: c_int = 3;

/// Run an FFI body inside `catch_unwind`, returning `default` if a panic is caught.
/// Panicking across an `extern "C"` boundary is undefined behavior, so every FFI
/// entry point routes its body through this guard as a last line of defense.
pub fn ffi_guard<T>(name: &str, default: T, body: impl FnOnce() -> T) -> T {
    match std::panic::catch_unwind(std::panic::AssertUnwindSafe(body)) {
        Ok(value) => value,
        Err(_) => {
            log::error!("{}: internal panic, returning failure", name);
            default
        }
    }
}

/// Logs and reports a null pointer FFI argument. Returns `true` if `ptr` is
/// null.
///
/// `*mut T` coerces to `*const T` automatically, so this works for both
/// pointer kinds without casts at the call site.
pub fn null_arg<T>(fn_name: &str, arg_name: &str, ptr: *const T) -> bool {
    if ptr.is_null() {
        log::error!("{}(): Bad argument: {} cannot be NULL", fn_name, arg_name);
        return true;
    }
    false
}

/// Validate a required C string FFI argument and convert it to `&str`.
///
/// Logs an error and returns `None` if `ptr` is null or the bytes are not UTF-8.
///
/// # Safety
/// If `ptr` is non-null, it must point to a valid, null-terminated C string.
pub unsafe fn cstr_arg<'a>(fn_name: &str, arg_name: &str, ptr: *const c_char) -> Option<&'a str> {
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

/// ABI-compatible mirror of `lch_buffer_t` from `leech2.h`. An owned byte
/// buffer handed across the FFI boundary; freed with `lch_buffer_free`.
#[repr(C)]
pub struct FfiBuffer {
    pub data: *mut u8,
    pub len: usize,
}

impl From<Vec<u8>> for FfiBuffer {
    fn from(buf: Vec<u8>) -> Self {
        let boxed = buf.into_boxed_slice();
        let len = boxed.len();
        let data = Box::into_raw(boxed) as *mut u8;
        FfiBuffer { data, len }
    }
}

/// ABI-compatible mirror of `lch_cell_t` from `leech2.h`. Only used to type
/// FFI parameters; the Rust side reads it via [`cell_from_ffi`].
#[repr(C)]
pub union FfiCellPayload {
    pub text: *const c_char,
    pub number: f64,
    pub boolean: bool,
}

#[repr(C)]
pub struct FfiCell {
    pub kind: c_int,
    pub payload: FfiCellPayload,
}

/// Convert an FFI `lch_cell_t` into a domain [`Cell`]. Validates the kind
/// tag, rejects non-finite numbers, and (for TEXT) verifies the pointer is
/// non-null and UTF-8. Logs an error and returns `None` on failure.
///
/// # Safety
/// When `cell.kind == LCH_VALUE_TEXT`, `cell.payload.text` must point to a
/// valid, null-terminated C string. A null pointer is rejected with an
/// error; use `LCH_VALUE_NULL` to represent a null value.
pub unsafe fn cell_from_ffi(fn_name: &str, cell: &FfiCell) -> Option<Cell> {
    match cell.kind {
        VALUE_NULL => Some(Cell::Null),
        VALUE_TEXT => {
            let ptr = unsafe { cell.payload.text };
            let s = unsafe { cstr_arg(fn_name, "cell.text", ptr) }?;
            Some(Cell::Text(s.to_string()))
        }
        VALUE_NUMBER => match Cell::number(unsafe { cell.payload.number }) {
            Ok(cell) => Some(cell),
            Err(e) => {
                log::error!("{}(): Bad argument: cell.number: {:#}", fn_name, e);
                None
            }
        },
        VALUE_BOOLEAN => Some(Cell::Boolean(unsafe { cell.payload.boolean })),
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
