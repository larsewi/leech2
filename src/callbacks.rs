//! Safe-Rust adapter for the FFI callback bundle that drives the
//! callback-based path of `lch_block_create`. The repr-C mirror
//! ([`LchCallbacks`]) is decoded once at the FFI boundary into a [`Callbacks`]
//! value that the block-creation pipeline consults from then on.
//!
//! Not `Send`/`Sync`: callbacks are invoked exclusively on the thread that
//! called `lch_block_create`, and the raw `usr_data` pointer is the C
//! caller's responsibility.

use std::ffi::{CStr, c_char, c_void};

use anyhow::{Result, bail};

use crate::cell::Cell;
use crate::ffi::{LCH_VALUE_NULL, LchCell, LchCellPayload, SUCCESS, cell_from_ffi};

/// `lch_read_cell_cb_t` return code: the row at this index does not exist;
/// iteration for this table stops.
pub const LCH_END_OF_TABLE: i32 = 1;

/// `lch_read_cell_cb_t` return code: drop the current row; advance to the
/// next row without consulting any further fields.
pub const LCH_FILTER_RECORD: i32 = 2;

type TableBeginFn = unsafe extern "C" fn(*const c_char, *mut c_void) -> i32;
type TableEndFn = unsafe extern "C" fn(*const c_char, i32, *mut c_void) -> i32;
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
    /// `LCH_FILTER_RECORD`: drop the current row.
    FilterRecord,
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

impl Callbacks {
    pub fn from_ffi(raw: &LchCallbacks) -> Self {
        Callbacks {
            table_begin: raw.table_begin,
            read_cell: raw.read_cell,
            table_end: raw.table_end,
            usr_data: raw.usr_data,
        }
    }

    /// Invoke the optional `table_begin` hook for one callback-backed table.
    /// A `None` hook is a successful no-op.
    pub fn table_begin(&self, table: &CStr) -> Result<()> {
        let Some(cb) = self.table_begin else {
            return Ok(());
        };
        let rc = unsafe { cb(table.as_ptr(), self.usr_data) };
        if rc == SUCCESS {
            Ok(())
        } else {
            bail!(
                "table_begin callback returned failure for table '{}'",
                table.to_string_lossy()
            );
        }
    }

    /// Invoke the optional `table_end` hook. Fires for every table whose
    /// `table_begin` returned successfully, including on the error path;
    /// `status` mirrors the C-side `LCH_SUCCESS` / `LCH_FAILURE` distinction.
    pub fn table_end(&self, table: &CStr, status: i32) -> Result<()> {
        let Some(cb) = self.table_end else {
            return Ok(());
        };
        let rc = unsafe { cb(table.as_ptr(), status, self.usr_data) };
        if rc == SUCCESS {
            Ok(())
        } else {
            bail!(
                "table_end callback returned failure for table '{}'",
                table.to_string_lossy()
            );
        }
    }

    /// Invoke the required `read_cell` hook for one (row, column) pair.
    /// `table` and `field` are pre-built `&CStr` so the row loop reuses the
    /// same pointers across every cell call.
    pub fn read_cell(
        &self,
        table: &CStr,
        row: usize,
        col: usize,
        field: &CStr,
    ) -> Result<CellResult> {
        let Some(cb) = self.read_cell else {
            bail!(
                "table '{}' is callback-backed but no read_cell callback was provided",
                table.to_string_lossy()
            );
        };
        let mut out = LchCell {
            kind: LCH_VALUE_NULL,
            payload: LchCellPayload { number: 0.0 },
        };
        let rc = unsafe {
            cb(
                table.as_ptr(),
                row,
                col,
                field.as_ptr(),
                &mut out,
                self.usr_data,
            )
        };
        match rc {
            SUCCESS => {
                let Some(cell) = (unsafe { cell_from_ffi("lch_block_create", &out) }) else {
                    bail!(
                        "invalid cell from callback for table '{}' row {} field '{}'",
                        table.to_string_lossy(),
                        row + 1,
                        field.to_string_lossy(),
                    );
                };
                Ok(CellResult::Cell(cell))
            }
            LCH_END_OF_TABLE => Ok(CellResult::EndOfTable),
            LCH_FILTER_RECORD => Ok(CellResult::FilterRecord),
            _ => bail!(
                "read_cell callback returned failure for table '{}' row {} field '{}'",
                table.to_string_lossy(),
                row + 1,
                field.to_string_lossy(),
            ),
        }
    }
}
