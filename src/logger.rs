use std::ffi::{CString, c_char, c_void};
use std::sync::{Once, RwLock};

use log::{Level, LevelFilter, Log, Metadata, Record};

use crate::ffi::{LOG_DEBUG, LOG_ERROR, LOG_INFO, LOG_TRACE, LOG_WARN};

type LogCallback = unsafe extern "C" fn(i32, *const c_char, *mut c_void);

struct CallbackState {
    callback: LogCallback,
    user_data: *mut c_void,
}

// SAFETY: C consumer guarantees callback and user_data are thread-safe.
unsafe impl Send for CallbackState {}
unsafe impl Sync for CallbackState {}

static CALLBACK: RwLock<Option<CallbackState>> = RwLock::new(None);
static INIT: Once = Once::new();

struct CallbackLogger;

impl Log for CallbackLogger {
    fn enabled(&self, _metadata: &Metadata) -> bool {
        match CALLBACK.read() {
            Ok(guard) => guard.is_some(),
            Err(_) => false,
        }
    }

    fn log(&self, record: &Record) {
        let Ok(guard) = CALLBACK.read() else { return };
        if let Some(ref state) = *guard {
            let message = format!("{}", record.args());
            if let Ok(cstr) = CString::new(message) {
                // Map explicitly to the LCH_LOG_* values rather than casting
                // the log crate's Level discriminant, which is not a stable
                // ABI contract.
                let level = match record.level() {
                    Level::Error => LOG_ERROR,
                    Level::Warn => LOG_WARN,
                    Level::Info => LOG_INFO,
                    Level::Debug => LOG_DEBUG,
                    Level::Trace => LOG_TRACE,
                };
                unsafe {
                    (state.callback)(level, cstr.as_ptr(), state.user_data);
                }
            }
        }
    }

    fn flush(&self) {}
}

/// Install or replace the log callback.
/// First call installs the global logger; subsequent calls swap the callback.
pub(crate) fn init(callback: LogCallback, user_data: *mut c_void) {
    // Install global logger exactly once.
    INIT.call_once(|| {
        let _ = log::set_boxed_logger(Box::new(CallbackLogger));
        log::set_max_level(LevelFilter::Trace);
    });

    // Set or replace the callback. The protected state is plain data, so
    // recovering from a poisoned lock is safe.
    let mut guard = match CALLBACK.write() {
        Ok(guard) => guard,
        Err(poisoned) => poisoned.into_inner(),
    };
    *guard = Some(CallbackState {
        callback,
        user_data,
    });
}
