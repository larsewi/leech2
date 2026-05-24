//! Safe-Rust adapter for the FFI callback bundle that drives the
//! callback-based path of `lch_block_create`. The repr-C mirror
//! ([`LchCallbacks`]) is decoded once at the FFI boundary into a [`Callbacks`]
//! value, then bound to one table at a time via [`Callbacks::for_table`].
//!
//! Not `Send`/`Sync`: callbacks are invoked exclusively on the thread that
//! called `lch_block_create`, and the raw `usr_data` pointer is the C
//! caller's responsibility.

use std::ffi::{CString, c_char, c_void};

use anyhow::{Context, Result, bail};

use crate::cell::Cell;
use crate::ffi::{
    END_OF_TABLE, LchCell, LchCellPayload, SKIP_RECORD, SUCCESS, VALUE_NULL, cell_from_ffi,
};

type TableBeginFn = unsafe extern "C" fn(*const c_char, *mut c_void) -> i32;
type TableEndFn = unsafe extern "C" fn(*const c_char, *mut c_void) -> i32;
type ReadCellFn = unsafe extern "C" fn(
    *const c_char,
    usize,
    usize,
    *const c_char,
    *mut LchCell,
    *mut c_void,
) -> i32;

/// ABI-compatible mirror of `lch_callbacks_t` from `leech2.h`. Function fields
/// use `Option<unsafe extern "C" fn ...>` so a NULL function pointer on the C
/// side deserializes to `None`.
#[repr(C)]
pub struct LchCallbacks {
    pub table_begin: Option<TableBeginFn>,
    pub read_cell: Option<ReadCellFn>,
    pub table_end: Option<TableEndFn>,
    pub usr_data: *mut c_void,
}

/// Outcome of a single `lch_read_cell_cb_t` invocation, after translating
/// the C-side return code into a Rust enum.
pub enum CellResult {
    /// `LCH_SUCCESS`: `out_cell` was populated; this is the decoded value.
    Cell(Cell),
    /// `LCH_END_OF_TABLE`: no row exists at this index; iteration stops.
    EndOfTable,
    /// `LCH_SKIP_RECORD`: drop the current row.
    SkipRecord,
}

/// Rust-side view of the callback bundle. Owned by `lch_block_create` for
/// the duration of one call and forwarded down to the block-creation
/// pipeline.
pub struct Callbacks {
    table_begin: Option<TableBeginFn>,
    read_cell: Option<ReadCellFn>,
    table_end: Option<TableEndFn>,
    usr_data: *mut c_void,
}

impl From<&LchCallbacks> for Callbacks {
    fn from(raw: &LchCallbacks) -> Self {
        Callbacks {
            table_begin: raw.table_begin,
            read_cell: raw.read_cell,
            table_end: raw.table_end,
            usr_data: raw.usr_data,
        }
    }
}

impl Callbacks {
    /// Bind this callback bundle to one table. The returned handle owns the
    /// pre-encoded C strings for the table name and every field, so the inner
    /// row/cell loop never has to touch a `CString` itself.
    pub fn for_table(&self, name: &str, field_names: &[&str]) -> Result<TableCallbacks<'_>> {
        let table_c = CString::new(name)
            .with_context(|| format!("table name '{}' contains a NUL byte", name))?;
        let mut field_cstrings = Vec::with_capacity(field_names.len());
        for field in field_names {
            field_cstrings.push(CString::new(*field).with_context(|| {
                format!(
                    "field name '{}' in table '{}' contains a NUL byte",
                    field, name
                )
            })?);
        }
        Ok(TableCallbacks {
            inner: self,
            table_c,
            field_cstrings,
        })
    }
}

/// A [`Callbacks`] bundle bound to one specific table. Holds the table name
/// and every field name pre-encoded as `CString` so the inner cell loop reuses
/// the same pointers across every callback invocation.
pub struct TableCallbacks<'a> {
    inner: &'a Callbacks,
    table_c: CString,
    field_cstrings: Vec<CString>,
}

impl TableCallbacks<'_> {
    /// Invoke the optional `table_begin` hook. A `None` hook is a successful
    /// no-op.
    pub fn table_begin(&self) -> Result<()> {
        let Some(cb) = self.inner.table_begin else {
            return Ok(());
        };
        let rc = unsafe { cb(self.table_c.as_ptr(), self.inner.usr_data) };
        if rc == SUCCESS {
            Ok(())
        } else {
            bail!(
                "table_begin callback returned failure for table '{}'",
                self.table_c.to_string_lossy()
            );
        }
    }

    /// Invoke the optional `table_end` hook. Fires for every table whose
    /// `table_begin` returned successfully, including on the error path.
    pub fn table_end(&self) -> Result<()> {
        let Some(cb) = self.inner.table_end else {
            return Ok(());
        };
        let rc = unsafe { cb(self.table_c.as_ptr(), self.inner.usr_data) };
        if rc == SUCCESS {
            Ok(())
        } else {
            bail!(
                "table_end callback returned failure for table '{}'",
                self.table_c.to_string_lossy()
            );
        }
    }

    /// Invoke the required `read_cell` hook for one (row, column) pair. `col`
    /// indexes the field-name list this handle was bound with.
    pub fn read_cell(&self, row: usize, col: usize) -> Result<CellResult> {
        let Some(cb) = self.inner.read_cell else {
            bail!(
                "table '{}' is callback-backed but no read_cell callback was provided",
                self.table_c.to_string_lossy()
            );
        };
        let field = &self.field_cstrings[col];
        let mut out = LchCell {
            kind: VALUE_NULL,
            payload: LchCellPayload { number: 0.0 },
        };
        let rc = unsafe {
            cb(
                self.table_c.as_ptr(),
                row,
                col,
                field.as_ptr(),
                &mut out,
                self.inner.usr_data,
            )
        };
        match rc {
            SUCCESS => {
                let Some(cell) = (unsafe { cell_from_ffi("lch_block_create", &out) }) else {
                    bail!(
                        "invalid cell from callback for table '{}' row {} field '{}'",
                        self.table_c.to_string_lossy(),
                        row + 1,
                        field.to_string_lossy(),
                    );
                };
                Ok(CellResult::Cell(cell))
            }
            END_OF_TABLE => Ok(CellResult::EndOfTable),
            SKIP_RECORD => Ok(CellResult::SkipRecord),
            _ => bail!(
                "read_cell callback returned failure for table '{}' row {} field '{}'",
                self.table_c.to_string_lossy(),
                row + 1,
                field.to_string_lossy(),
            ),
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::ffi::FAILURE;

    unsafe extern "C" fn fail_table_begin(_table: *const c_char, _usr_data: *mut c_void) -> i32 {
        FAILURE
    }

    unsafe extern "C" fn fail_table_end(_table: *const c_char, _usr_data: *mut c_void) -> i32 {
        FAILURE
    }

    fn callbacks_with_failing_begin() -> Callbacks {
        Callbacks::from(&LchCallbacks {
            table_begin: Some(fail_table_begin),
            read_cell: None,
            table_end: None,
            usr_data: std::ptr::null_mut(),
        })
    }

    fn callbacks_with_failing_end() -> Callbacks {
        Callbacks::from(&LchCallbacks {
            table_begin: None,
            read_cell: None,
            table_end: Some(fail_table_end),
            usr_data: std::ptr::null_mut(),
        })
    }

    #[test]
    fn test_table_begin_failure_propagates() {
        let callbacks = callbacks_with_failing_begin();
        let bound = callbacks.for_table("t", &["id"]).unwrap();
        let err = bound.table_begin().unwrap_err();
        let msg = format!("{:#}", err);
        assert!(
            msg.contains("table_begin callback returned failure"),
            "got: {msg}"
        );
        assert!(msg.contains("table 't'"), "got: {msg}");
    }

    #[test]
    fn test_table_end_failure_propagates() {
        let callbacks = callbacks_with_failing_end();
        let bound = callbacks.for_table("t", &["id"]).unwrap();
        let err = bound.table_end().unwrap_err();
        let msg = format!("{:#}", err);
        assert!(
            msg.contains("table_end callback returned failure"),
            "got: {msg}"
        );
        assert!(msg.contains("table 't'"), "got: {msg}");
    }

    fn empty_callbacks() -> Callbacks {
        Callbacks::from(&LchCallbacks {
            table_begin: None,
            read_cell: None,
            table_end: None,
            usr_data: std::ptr::null_mut(),
        })
    }

    fn expect_for_table_err(callbacks: &Callbacks, name: &str, fields: &[&str]) -> anyhow::Error {
        match callbacks.for_table(name, fields) {
            Ok(_) => panic!("expected for_table({name:?}, {fields:?}) to fail"),
            Err(e) => e,
        }
    }

    #[test]
    fn test_nul_byte_in_table_name_rejected() {
        let callbacks = empty_callbacks();
        let err = expect_for_table_err(&callbacks, "t\0bad", &["id"]);
        let msg = format!("{:#}", err);
        assert!(msg.contains("table name"), "got: {msg}");
        assert!(msg.contains("NUL byte"), "got: {msg}");
    }

    #[test]
    fn test_nul_byte_in_field_name_rejected() {
        let callbacks = empty_callbacks();
        let err = expect_for_table_err(&callbacks, "t", &["id", "bad\0col"]);
        let msg = format!("{:#}", err);
        assert!(msg.contains("field name 'bad\0col'"), "got: {msg}");
        assert!(msg.contains("table 't'"), "got: {msg}");
        assert!(msg.contains("NUL byte"), "got: {msg}");
    }

    #[test]
    fn test_read_cell_missing_is_an_error() {
        // A callback-backed table with no read_cell hook is a configuration
        // error: the cell-pull contract is unsatisfiable.
        let callbacks = Callbacks::from(&LchCallbacks {
            table_begin: None,
            read_cell: None,
            table_end: None,
            usr_data: std::ptr::null_mut(),
        });
        let bound = callbacks.for_table("t", &["id"]).unwrap();
        let err = match bound.read_cell(0, 0) {
            Ok(_) => panic!("expected read_cell to fail without a read_cell hook"),
            Err(e) => e,
        };
        let msg = format!("{:#}", err);
        assert!(
            msg.contains("no read_cell callback was provided"),
            "got: {msg}"
        );
        assert!(msg.contains("table 't'"), "got: {msg}");
    }
}
