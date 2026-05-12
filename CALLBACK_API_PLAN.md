# Plan: Callback-Based Block Creation API

## Context

Today, `lch_block_create()` is the only way to build a block: it reads CSV files declared in `config.toml` via `Table::load()` → `parse_csv()`. Many C applications already have the data in memory (live DB cursors, in-process state, generated rows) and have to first materialize it to a CSV file just to hand it back. That's awkward, doubles I/O, and requires temp-file plumbing on the C side.

This change extends `lch_block_create` to accept an optional `lch_callbacks_t` bundle so a C application can supply table contents directly. The bundle carries three hooks — `table_begin`, `read_cell`, `table_end` — plus a `usr_data` pointer. leech2 calls `table_begin` once per callback-backed table (e.g. for the caller to open a DB cursor), reads each cell on demand via `read_cell`, then calls `table_end` once the table is drained or aborted (e.g. for the caller to close the cursor and commit or roll back). The hooks fire only for callback-backed tables — CSV-backed tables go through `Table::load` exactly as today and do not trigger any callbacks. Everything downstream — filters, type parsing, canonical column ordering, delta computation, block storage, truncation — stays identical to the CSV path.

Outcome: C apps can produce blocks without staging data to disk and without holding every table in memory simultaneously. CSV-backed tables continue to work unchanged, individual tables can be migrated from CSV to callback-backed one at a time, and callers that don't need callbacks pass `NULL` for the bundle.

## Design decisions (already validated)

- **Single entry point**: `lch_block_create()` gains one new parameter — a pointer to a heap- or stack-allocated `lch_callbacks_t` struct. The struct bundles three function pointers (`table_begin`, `read_cell`, `table_end`) plus a single `usr_data`. Passing `NULL` for the bundle keeps the CSV-only behavior. Bundling in a struct keeps the function signature stable if we add more lifecycle hooks later (e.g. `block_begin` / `block_end`). Since the upcoming release is already a major bump for other reasons, breaking the FFI signature is acceptable.
- **Lifecycle hooks (`table_begin` / `table_end`)** let the caller scope per-table resources (a DB cursor, a query result set, a malloc'd buffer) so only one table's worth of data lives in memory at a time. Both are optional; callers that don't need them set the corresponding field to `NULL`. `table_end` always pairs with a successful `table_begin` — including on the error path — so it's a safe place for teardown that mirrors RAII. A `status` parameter on `table_end` distinguishes "drained cleanly" from "aborted mid-iteration" so callers can commit-or-rollback against their backing store.
- **Granularity**: cell-pull `(table, row, col, field_name)`. The callback receives **both** an integer `col` (the field's 0-based position in `config.fields` as declared in the table's TOML/JSON, matching the order shown in error messages) **and** the field's name. Callers pick whichever is more convenient for their data layout: `col` for positional dispatch (e.g. a flat array of pointers indexed by column), `field_name` for name-based dispatch (e.g. a `strcmp` chain or a hash table keyed by name).
- **Iteration contract**: leech asks for rows in **ascending order** starting from `row=0`. Within a row, leech may ask for columns in **any order** and the order may vary across rows; callers must support random access by `col` / `field_name`. Tables are processed one at a time (one fully drained before the next), and the callback is invoked exclusively on the thread that called `lch_block_create`.
- **End of table**: no separate row-count callback. The cell callback signals "cell populated" by returning `LCH_SUCCESS` (0) — matching how every other entry point in `leech2.h` uses the constant — and signals end-of-table by returning the new `LCH_END_OF_TABLE` (1) for any cell of a row whose index is past the caller's data. Because column order within a row is unspecified, `LCH_END_OF_TABLE` is valid at **any column**; the natural caller implementation is `if (row >= my_data.len()) return LCH_END_OF_TABLE;` and never mixes responses for the same row index. Picking `LCH_END_OF_TABLE = 1` (rather than overloading `LCH_SUCCESS`) also gives safer wrong-answer behavior: a caller that mistakenly returns `0` without populating `*out_cell` causes leech to read uninitialized memory only with the overloaded convention; with the dedicated constant, returning `0` says "I delivered a cell" only when the caller has actually written one.
- **Schema**: reused from `config.toml`. No new schema-registration FFI surface.
- **Per-table mode**: callback-backed tables coexist with CSV-backed tables. Mode is determined by **presence of `source`** in `TableConfig` — `source` omitted ⇒ callback-backed. One source of truth; no redundant discriminator field.
- **Value representation**: reuse the existing `lch_cell_t` already declared in `leech2.h` for `lch_patch_inject`. The callback writes a typed value directly — no CSV-style string parsing across the FFI boundary. The cell's `kind` must match the field's declared `kind` (NULL is always accepted unless the field is a primary key).
- **Sentinels are CSV-only**: the field-config `null` / `true` / `false` sentinels exist to coerce CSV strings into typed values. They have no effect on the callback path — callers send `Cell::Null` / `Cell::Boolean(true|false)` directly via the union — and no per-field validation against them is performed on callback input.
- **Filters are CSV-only**: `FilterConfig` (`max-field-length`, `include`, `exclude`) is not applied to callback-backed tables. The callback owns its own data and is the natural place to drop rows — it does so by returning `LCH_FILTER_RECORD` (see below) from any field of a row. This avoids both the semantic mismatch of running CSV-text regexes against typed `Cell`s and the complexity of a typed-filter refactor. If the user has filters configured _and_ any table is callback-backed, leech2 logs a one-time warning at block creation naming the affected table(s) and the workaround.

## Files to modify

| File                                         | Change                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                       |
| -------------------------------------------- | ------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------- |
| `src/config.rs`                              | `TableConfig::source: String` → `Option<String>`. Update `Validate for TableConfig`: drop the "source must not be empty" check; the existing "≥1 primary-key field" + duplicate-name checks are sufficient. `FilterConfig::should_filter` is **not** changed; filters remain a CSV-only concept.                                                                                                                                                                                                                                                                                                                                                              |
| `src/table.rs`                               | Rename `Table::load` → `Table::load_from_csv` (current body; takes `&str source_path` instead of computing it from `TableConfig`, or keeps the same shape and treats `config.source` as `Some`). Add new `Table::load_from_callbacks`. Extract the parse-row-into-(pk, subsidiary) logic into a helper shared by both paths. Update the test fixture `make_config` helper (~line 285) so `source` is wrapped in `Some(...)`.                                                                                                                                                                                                                                       |
| `src/state.rs`                               | `State::compute(config)` → `State::compute(config, callbacks: Option<&Callbacks>)`. Dispatch per-table: `table_config.source.is_some()` → CSV path; `None` → callback path (error if `callbacks` is `None`). Pass `&config.filters` only on the CSV branch.                                                                                                                                                                                                                                                                                                                                                                                                   |
| `src/block.rs`                               | Keep `Block::create(config)` unchanged for the existing in-tree callers (about 30 call sites across `tests/` + `src/main.rs`). Add a sibling `Block::create_with_callbacks(config, callbacks: Option<&Callbacks>)` that does the real work; `Block::create` becomes a one-line delegator passing `None`. Emit a one-time warning at the top of `create_with_callbacks` when `config.filters` is non-default and at least one table is callback-backed.                                                                                                                                                                                                          |
| `src/lib.rs` _(or new `src/callbacks.rs`)_   | New `pub(crate) Callbacks` struct holding the three C function pointers (`table_begin`, `read_cell`, `table_end` — begin/end optional, `read_cell` required when any table is callback-backed) + `usr_data`. Safe-Rust wrappers `table_begin(&CStr) -> Result<()>`, `read_cell(&CStr, usize, usize, &CStr) -> Result<CellResult>`, `table_end(&CStr, i32) -> Result<()>`. Reuse the existing `LchCell` repr-C struct and `cell_from_ffi` helper (`src/lib.rs:314`, `src/lib.rs:336`) to decode the typed cell. Add a `pub LchCallbacks` repr-C mirror of `lch_callbacks_t`. Update the `lch_block_create` FFI entry to take `*const LchCallbacks` and forward an `Option<&Callbacks>` to `Block::create_with_callbacks`. |
| `include/leech2.h`                           | Add `LCH_END_OF_TABLE` and `LCH_FILTER_RECORD` sentinels, the `lch_table_begin_cb_t`, `lch_read_cell_cb_t`, `lch_table_end_cb_t` typedefs, and the `lch_callbacks_t` struct. Rewrite the `lch_block_create` declaration to take `const lch_callbacks_t *callbacks`. Document NULL semantics, lifetime of returned strings, the new return-code convention, the per-table lifecycle, and the thread model.                                                                                                                                                                                                                                                                                       |
| `tests/test_c_ffi.c` / `tests/test_c_ffi.rs` | Existing CSV-only call sites become `lch_block_create(cfg, NULL)`. Add a callback-backed scenario: define a `lch_callbacks_t` whose `cell` synthesizes the same rows as the existing CSV fixture (and whose `table_begin`/`table_end` increment counters in `usr_data`), drive `lch_block_create(cfg, &callbacks)`, and assert the resulting block bytes are identical to the CSV-equivalent block and that the counters reflect one begin + one end per callback-backed table.                                                                                                                                                                                                                |
| `man/libleech2.3.in`                         | Document the new parameter, return codes, the lifecycle ordering, and all three callback typedefs on the existing `lch_block_create` entry.                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                   |
| `man/lch.1.in`                               | No change (CLI uses CSV exclusively).                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                        |
| `README.md`, `CONTRIBUTING.md`               | Brief mention of callback mode and minimal C example.                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                        |

## C API shape

```c
#include <stdbool.h>   /* already included by leech2.h for lch_cell_t */

#define LCH_SUCCESS        0   /* existing — for the cell callback: cell populated */
#define LCH_FAILURE       -1   /* existing */
#define LCH_END_OF_TABLE   1   /* new — no row at this index; iteration stops */
#define LCH_FILTER_RECORD  2   /* new — drop the current row */

/* lch_cell_t and lch_kind_t are already declared in this header (used by
 * lch_patch_inject). The cell callback reuses them as-is — no new value type. */

/* Per-table setup. Invoked once, before the first cell callback for `table`.
 * The caller typically uses this hook to open a cursor, execute a query, or
 * allocate per-table buffers, stashing any handle through `usr_data`.
 *
 * Return values:
 *   LCH_SUCCESS  — proceed to pulling cells from this table.
 *   LCH_FAILURE  — unrecoverable error; block creation aborts immediately.
 *                  table_end is NOT invoked (begin did not succeed).
 *
 * Not invoked for CSV-backed tables. */
typedef int (*lch_table_begin_cb_t)(
    const char *table,
    void *usr_data);

/* Per-table teardown. Invoked once for every table for which `table_begin`
 * returned LCH_SUCCESS, including on the failure path. The caller typically
 * uses this hook to close a cursor, free per-table buffers, or commit /
 * rollback against the backing store based on `status`.
 *
 * `status` is:
 *   LCH_SUCCESS  — the table was drained cleanly (the cell callback returned
 *                  LCH_SUCCESS for some row, or the table had zero rows).
 *   LCH_FAILURE  — iteration was aborted (the cell callback returned
 *                  LCH_FAILURE, leech2 detected a duplicate primary key, an
 *                  invalid cell kind, etc.). Any partial data the caller
 *                  has staged for this table should be discarded.
 *
 * Return values:
 *   LCH_SUCCESS  — teardown completed.
 *   LCH_FAILURE  — teardown failed; block creation returns LCH_FAILURE
 *                  even if iteration up to this point succeeded.
 *
 * Not invoked for CSV-backed tables. */
typedef int (*lch_table_end_cb_t)(
    const char *table,
    int status,
    void *usr_data);

/* Cell callback. On entry, *out_cell is zero-initialised.
 *
 * Iteration contract:
 *   - Rows are requested in ascending order, starting from row == 0.
 *   - The order in which leech2 asks for columns within a row is unspecified
 *     and may vary across rows. The caller must support random access by
 *     `col` or `field_name`.
 *   - A table is fully drained before any other table is processed, and the
 *     callback is invoked exclusively on the thread that called
 *     lch_block_create().
 *
 * Return values:
 *   LCH_SUCCESS        — *out_cell has been populated. leech2 will ask for
 *                        the remaining fields of this row (in some order),
 *                        and after the row is complete will advance to
 *                        row + 1.
 *   LCH_END_OF_TABLE   — there is no row at this index; iteration for this
 *                        table stops. May be returned from ANY column of a
 *                        row; the natural caller implementation is
 *                        `if (row >= my_data.len()) return LCH_END_OF_TABLE;`.
 *                        Once returned, leech2 will not ask for any further
 *                        cell of this table in the current lch_block_create
 *                        call. The contents of *out_cell are ignored.
 *   LCH_FILTER_RECORD  — drop the current row. Any cell values already
 *                        received for this row are discarded; leech2 does
 *                        not ask for any remaining fields of this row and
 *                        advances to row + 1. May be returned from any
 *                        column. The contents of *out_cell are ignored.
 *   LCH_FAILURE        — unrecoverable error; block creation aborts.
 *
 * For a given row, the caller must return consistent answers across all the
 * cells leech2 asks for: either every cell returns LCH_SUCCESS (the row
 * exists), or every cell returns LCH_END_OF_TABLE / LCH_FILTER_RECORD. The
 * "row index past the end" check makes this automatic for the typical caller.
 *
 * The kind of *out_cell must match the field's declared kind:
 *   - TEXT    field → TEXT or NULL
 *   - NUMBER  field → NUMBER or NULL
 *   - BOOLEAN field → BOOLEAN or NULL
 * NULL is rejected for primary-key fields.
 *
 * Filters configured in config.toml (`max-field-length`, `include`, `exclude`)
 * do NOT apply to callback-backed tables; the callback is the sole authority
 * for which rows are included.
 */
typedef int (*lch_read_cell_cb_t)(
    const char *table,
    size_t row,
    size_t col,                /* 0-based index into config.fields (declaration order) */
    const char *field_name,    /* same field as `col`, looked up for convenience */
    lch_cell_t *out_cell,
    void *usr_data);

/* Callback bundle. Any callback pointer may be NULL if the caller does not
 * need that hook. `usr_data` is forwarded verbatim to every invoked
 * callback. */
typedef struct {
    lch_table_begin_cb_t  table_begin;    /* may be NULL */
    lch_read_cell_cb_t    read_cell;      /* required if any table is callback-backed */
    lch_table_end_cb_t    table_end;      /* may be NULL */
    void                 *usr_data;        /* opaque pointer passed verbatim */
} lch_callbacks_t;

/* Updated entry — accepts an optional callback bundle.
 *
 * `callbacks` may be NULL for configs whose tables are all CSV-backed. A
 * config containing any callback-backed table requires `callbacks` to be
 * non-NULL with `read_cell` set; otherwise the call returns LCH_FAILURE with
 * a log message naming the offending table. */
extern int lch_block_create(const lch_config_t *cfg,
                            const lch_callbacks_t *callbacks);  /* may be NULL */
```

Semantics:

- A config containing only CSV-backed tables works with `lch_block_create(cfg, NULL)` (the original behavior) and also with a non-`NULL` bundle (none of its hooks are invoked) — useful for callers that always pass the same bundle.
- A config containing any callback-backed table requires a non-`NULL` `callbacks` with `callbacks->read_cell` set; otherwise the call returns `LCH_FAILURE` with a log message naming the offending table. `table_begin` and `table_end` remain optional.
- For each callback-backed table, lifecycle order is `table_begin` (if set, exactly once) → some number of `read_cell` invocations → `table_end` (if set and `table_begin` returned `LCH_SUCCESS`, exactly once). If `table_begin` returns `LCH_FAILURE`, no `read_cell` calls are made for that table and `table_end` is not invoked. CSV-backed tables do not trigger any hooks. Tables are processed one at a time; lifecycles do not overlap.
- All callbacks are invoked exclusively from the thread that called `lch_block_create`. No re-entrancy is supported (calling back into leech2 from inside any callback is undefined).
- A kind-mismatched cell, a `LCH_VALUE_NULL` cell on a primary-key field, or a non-finite `LCH_VALUE_NUMBER` all surface as `LCH_FAILURE` with a log message identifying the table, row, and field. The current table's `table_end` is then invoked with `status = LCH_FAILURE` before `lch_block_create` returns.
- Filters configured in `config.toml` apply to CSV-backed tables only. If filters are configured and at least one table is callback-backed, leech2 logs a warning at block creation naming the affected callback-backed table(s) and noting that filtering is the callback's responsibility via `LCH_FILTER_RECORD`.

## Rust internals

### `Callbacks` adapter (in `lib.rs` or new `src/callbacks.rs`)

The Rust-side adapter wraps three C function pointers (any of which may be absent) plus `usr_data`. The cell wrapper translates the three-valued return code into a Rust enum and reuses the existing `LchCell` / `cell_from_ffi` infrastructure (`src/lib.rs:314`, `src/lib.rs:336`) used by `lch_patch_inject`. No new C-side cell type or decode helper is needed — `Cell::Null` / `Cell::Number` / `Cell::Text` / `Cell::Boolean` all come back through the same path.

```rust
const END_OF_TABLE: i32 = 1;
const FILTER_RECORD: i32 = 2;

pub(crate) enum CellResult {
    Cell(Cell),                 // includes Cell::Null
    EndOfTable,
    FilterRecord,
}

type TableBeginFn = unsafe extern "C" fn(*const c_char, *mut c_void) -> i32;
type TableEndFn   = unsafe extern "C" fn(*const c_char, i32, *mut c_void) -> i32;
type ReadCellFn   = unsafe extern "C" fn(*const c_char, usize, usize, *const c_char,
                                          *mut LchCell, *mut c_void) -> i32;

pub(crate) struct Callbacks {
    table_begin: Option<TableBeginFn>,
    read_cell:   Option<ReadCellFn>,    // required only when a table is callback-backed
    table_end:   Option<TableEndFn>,
    usr_data:    *mut c_void,
}

impl Callbacks {
    fn table_begin(&self, table: &CStr) -> Result<()> {
        let Some(cb) = self.table_begin else { return Ok(()); };
        let rc = unsafe { cb(table.as_ptr(), self.usr_data) };
        if rc == SUCCESS {
            Ok(())
        } else {
            bail!("table_begin callback failed for table '{}'",
                  table.to_string_lossy())
        }
    }

    /// Invoked after iteration completes for a table whose `table_begin`
    /// returned successfully — including on the error path. `status` mirrors
    /// the C-side `LCH_SUCCESS` / `LCH_FAILURE` distinction.
    fn table_end(&self, table: &CStr, status: i32) -> Result<()> {
        let Some(cb) = self.table_end else { return Ok(()); };
        let rc = unsafe { cb(table.as_ptr(), status, self.usr_data) };
        if rc == SUCCESS {
            Ok(())
        } else {
            bail!("table_end callback failed for table '{}'",
                  table.to_string_lossy())
        }
    }

    /// `table` and `field` are passed in as `&CStr` so the row loop can build
    /// them once per table / field and reuse the same pointers across every
    /// cell — avoids `CString::new` allocations on every call (millions of
    /// cells × two allocations adds up).
    fn read_cell(&self, table: &CStr, row: usize, col: usize, field: &CStr) -> Result<CellResult> {
        let Some(cb) = self.read_cell else {
            bail!("table '{}' is callback-backed but no read_cell callback was provided",
                  table.to_string_lossy());
        };
        let mut out = LchCell {
            kind: LCH_VALUE_NULL,
            payload: LchCellPayload { number: 0.0 },
        };
        let rc = unsafe {
            cb(table.as_ptr(), row, col, field.as_ptr(),
               &mut out, self.usr_data)
        };
        match rc {
            SUCCESS => {
                let cell = unsafe { cell_from_ffi("lch_block_create", &out) }
                    .ok_or_else(|| anyhow::anyhow!(
                        "invalid cell from callback for table '{}' row {} field '{}'",
                        table.to_string_lossy(), row, field.to_string_lossy()))?;
                Ok(CellResult::Cell(cell))
            }
            END_OF_TABLE => Ok(CellResult::EndOfTable),
            FILTER_RECORD => Ok(CellResult::FilterRecord),
            _ => bail!("read_cell callback failed for table '{}' row {} field '{}'",
                       table.to_string_lossy(), row, field.to_string_lossy()),
        }
    }
}
```

`cell_from_ffi` already rejects non-finite numbers (via `Cell::number`) and null `text` pointers on TEXT kinds, so those failure paths come for free.

The begin/end lifecycle is enforced by `State::compute`, not by `Callbacks` itself — the adapter exposes the hooks but does not track per-table state.

### `Table::load_from_callbacks` (parallel to `parse_csv` in `src/table.rs`)

End-of-table is discovered, not declared. Rows are walked in ascending order; for each row we ask for the cells in canonical order (PKs first, lex-sorted, then subsidiaries lex-sorted) — the same order `parse_csv` uses to populate its tuples, so the PK and subsidiary vectors come out canonical without a projection step. The "column order within a row is unspecified" contract in the C API gives us this freedom; canonical order happens to be the most convenient one for leech.

```rust
pub fn load_from_callbacks(
    name: &str,
    config: &TableConfig,
    callbacks: &Callbacks,
) -> Result<Self> {
    // Synthesize identity indices so `col` and `field_name` passed to the
    // callback always correspond to a field's declaration-order position in
    // config.fields (matching the API contract), independently of the order
    // in which we walk the canonical columns below.
    let positions: Vec<usize> = (0..config.fields.len()).collect();
    let (primary_columns, subsidiary_columns) =
        Self::compute_canonical_columns(config, &positions);

    let primary_key_names: Vec<String> = primary_columns
        .iter().map(|(_, f)| f.name.clone()).collect();
    let subsidiary_value_names: Vec<String> = subsidiary_columns
        .iter().map(|(_, f)| f.name.clone()).collect();

    // Build the per-table CStrings once so the inner loop reuses the same
    // pointers across every cell call.
    let table_c = CString::new(name)?;
    let field_cstrings: Vec<CString> = config.fields
        .iter()
        .map(|f| CString::new(f.name.as_str()))
        .collect::<Result<_, _>>()?;

    let mut records = HashMap::new();
    let mut row = 0usize;

    'rows: loop {
        let mut primary_key: Vec<Cell> = Vec::with_capacity(primary_columns.len());
        let mut subsidiary: Vec<Cell> = Vec::with_capacity(subsidiary_columns.len());

        // Pull cells in canonical order: PKs first, then subsidiaries.
        // `col` passed to the callback is the field's declaration-order index.
        for (group_out, group_cols) in [
            (&mut primary_key, primary_columns.as_slice()),
            (&mut subsidiary,  subsidiary_columns.as_slice()),
        ] {
            for &(decl_idx, field_cfg) in group_cols {
                let result = callbacks.read_cell(
                    &table_c, row, decl_idx, &field_cstrings[decl_idx],
                )?;
                match result {
                    CellResult::Cell(c) => {
                        // Reject Cell::Null on PK fields; require the cell's
                        // kind to match the field's declared kind.
                        validate_cell(&c, field_cfg).with_context(|| {
                            format!("row {} field '{}'", row + 1, field_cfg.name)
                        })?;
                        group_out.push(c);
                    }
                    CellResult::EndOfTable => break 'rows,
                    CellResult::FilterRecord => {
                        log::trace!(
                            "Callback filtered row {} of table '{}' at field '{}'",
                            row + 1, name, field_cfg.name,
                        );
                        row += 1;
                        continue 'rows;
                    }
                }
            }
        }

        if records.insert(primary_key.clone(), subsidiary).is_some() {
            anyhow::bail!("duplicate primary key {:?}", primary_key);
        }
        row += 1;
    }

    Ok(Table { primary_key_names, subsidiary_value_names, records })
}
```

`load_from_callbacks` does not take a `FilterConfig` — filters are CSV-only. `State::compute` passes filters only to `load_from_csv`, and at block creation time logs a warning if any callback-backed table coexists with a non-empty `FilterConfig` so the user knows their rules aren't being applied to those tables.

One new local helper:

- `validate_cell(cell: &Cell, field: &FieldConfig) -> Result<()>` — enforces (a) `Null` is rejected on primary-key fields, (b) `Text`/`Number`/`Boolean` matches `field.kind`.

Note: leech walks cells in canonical order here, but this is an implementation choice — the C API contract only promises ascending rows and gives no guarantee about column order within a row. A future refactor (e.g. asking for all cells of a row in parallel, or pulling subsidiaries first to short-circuit unchanged rows) is free to reorder cell requests without breaking callers.

### `State::compute` dispatch

```rust
pub fn compute(config: &Config, callbacks: Option<&Callbacks>) -> Result<Self> {
    let mut tables = HashMap::new();
    for (name, table_config) in &config.tables {
        let table = match &table_config.source {
            Some(_) => Table::load_from_csv(&config.work_dir, name, table_config, &config.filters)?,
            None => {
                let cbs = callbacks.ok_or_else(|| anyhow::anyhow!(
                    "table '{}' is callback-backed but no callbacks were provided", name))?;
                load_callback_table_with_lifecycle(name, table_config, cbs)?
            }
        };
        tables.insert(name.clone(), table);
    }
    Ok(State { tables })
}

/// Wraps `Table::load_from_callbacks` with the begin/end lifecycle so that
/// `table_end` always fires when `table_begin` succeeded, including on the
/// error path. The begin/end hooks are no-ops when the corresponding C
/// pointer is NULL.
fn load_callback_table_with_lifecycle(
    name: &str,
    table_config: &TableConfig,
    callbacks: &Callbacks,
) -> Result<Table> {
    let table_c = CString::new(name)?;
    callbacks.table_begin(&table_c)
        .with_context(|| format!("table '{}'", name))?;

    let result = Table::load_from_callbacks(name, table_config, callbacks, &table_c);

    // table_end fires regardless of outcome; status reflects whether
    // iteration drained cleanly.
    let end_status = if result.is_ok() { SUCCESS } else { FAILURE };
    let end_result = callbacks.table_end(&table_c, end_status);

    let table = result.with_context(|| format!("table '{}'", name))?;
    end_result.with_context(|| format!("table '{}'", name))?;
    Ok(table)
}
```

`Table::load_from_callbacks` takes the pre-built `&CStr` for the table name as a parameter (so it doesn't re-allocate it). It no longer calls `table_begin`/`table_end` itself — that's the lifecycle wrapper's job.

`&config.filters` is passed only on the CSV branch — the callback branch is responsible for filtering via `LCH_FILTER_RECORD`. A one-time warning is emitted earlier in `Block::create_with_callbacks` when both filters and callback-backed tables are configured together.

### `Block::create` keeps its existing signature

```rust
impl Block {
    /// Existing in-tree call shape — used by `main.rs` and the ~30 test sites
    /// that call `Block::create(&config)`. Equivalent to
    /// `Block::create_with_callbacks(config, None)`.
    pub fn create(config: &Config) -> Result<String> {
        Self::create_with_callbacks(config, None)
    }

    pub fn create_with_callbacks(
        config: &Config,
        callbacks: Option<&Callbacks>,
    ) -> Result<String> {
        // current body of Block::create, but calls
        // state::State::compute(config, callbacks) instead of (config).
        // Also: if callbacks.is_some() and any table is callback-backed and
        // config.filters is non-default, emit a one-time warning.
    }
}
```

Keeping `Block::create` callable with a single argument avoids editing ~30 call sites across `tests/` and `src/main.rs`. Only the FFI entry point calls `create_with_callbacks` directly.

### FFI entry point

`lch_block_create` is updated to take a single `*const LchCallbacks` parameter. The existing call site `block::Block::create(config)` becomes a forward through `Block::create_with_callbacks`.

```rust
/// ABI-compatible mirror of `lch_callbacks_t` from `leech2.h`. Each callback
/// field uses `Option<unsafe extern "C" fn ...>` so the layout matches a NULL
/// function pointer on the C side.
#[repr(C)]
pub struct LchCallbacks {
    table_begin: Option<TableBeginFn>,
    read_cell:   Option<ReadCellFn>,
    table_end:   Option<TableEndFn>,
    usr_data:    *mut c_void,
}

#[unsafe(no_mangle)]
pub unsafe extern "C" fn lch_block_create(
    config: *const config::Config,
    callbacks: *const LchCallbacks,
) -> i32 {
    ffi_guard("lch_block_create", FAILURE, || {
        if null_arg("lch_block_create", "config", config) {
            return FAILURE;
        }

        let rust_callbacks = (!callbacks.is_null()).then(|| {
            let c = unsafe { &*callbacks };
            Callbacks {
                table_begin: c.table_begin,
                read_cell:   c.read_cell,
                table_end:   c.table_end,
                usr_data:    c.usr_data,
            }
        });

        match block::Block::create_with_callbacks(unsafe { &*config }, rust_callbacks.as_ref()) {
            Ok(_)  => SUCCESS,
            Err(e) => { log::error!("lch_block_create(): {:#}", e); FAILURE }
        }
    })
}
```

The FFI entry does not pre-validate which callback pointers are set; each hook is invoked only where needed. When `callbacks` is NULL from C, `Block::create_with_callbacks` receives `None` and `State::compute` errors on any callback-backed table. When `callbacks` is non-NULL but `read_cell` is NULL, the error surfaces as soon as a callback-backed table is reached — both routes produce a message naming the offending table. The function pointers are typed `unsafe extern "C" fn` to match the convention used by the existing `lch_log_init` callback (`src/lib.rs:90`).

## Existing functions / types to reuse

- `src/table.rs`: `compute_canonical_columns` (line 137). The CSV-specific helpers `parse_field_value` and `parse_columns` are untouched — the callback path skips text parsing entirely.
- `src/config.rs`: `TableConfig::field_names` (line 446), `FilterConfig::should_filter` (line 234), `FieldConfig` validation (line 370).
- `src/cell.rs`: `Cell::number()` constructor (rejects NaN/Inf), `Kind` for the kind-match check in `validate_cell`.
- `src/lib.rs`: `ffi_guard` (line 29), `null_arg` (line 44), `cstr_arg` (line 59), `LchCell` (line 322), `LchCellPayload` (line 315), `cell_from_ffi` (line 336), and the `LCH_VALUE_*` constants (lines 307–310).
- `src/state.rs`: `State::store` (line 97) and the rest of the block pipeline stays untouched.

## Verification

End-to-end (run from repo root):

1. `cargo build && cargo test` — Rust unit tests cover `load_from_callbacks` with a Rust mock implementation that returns canned rows; assert resulting `Table` equals the CSV-equivalent. A second test exercises `LCH_FILTER_RECORD` (returned from the first cell of one row and from a later cell of another row) and asserts both rows are absent from the resulting `Table` while the surrounding rows remain. A third test returns `LCH_END_OF_TABLE` immediately for `row = 0` and asserts the resulting `Table` is empty. A fourth test verifies the callback only sees `row = R, R, R, R+1, R+1, …` and never sees a row index regress. A fifth test verifies lifecycle order: `table_begin` fires exactly once per callback-backed table before any `cell` call, and `table_end` fires exactly once after the last `cell` call with `status = LCH_SUCCESS`. A sixth test makes the cell callback return `LCH_FAILURE` mid-table and asserts `table_end` is still invoked with `status = LCH_FAILURE`. A seventh test makes `table_begin` return `LCH_FAILURE` and asserts no `cell` calls happen and `table_end` is not invoked.
2. `cargo test --test test_c_ffi` — extended C-FFI test:
   - Sets up `config.toml` where `users.source = "users.csv"` (CSV-backed) and `events` has no `source` (callback-backed).
   - Defines a `lch_callbacks_t` whose `cell` synthesizes rows for `events` and whose `table_begin` / `table_end` increment per-table counters stashed in `usr_data`.
   - Calls `lch_block_create(cfg, &callbacks)` and asserts return code, presence of HEAD file, that decoding the produced block back to a `State` shows expected `events` rows, and that `usr_data` shows exactly one begin + one end for `events` (and none for `users`, since it's CSV-backed).
   - Additional case: a CSV-only config driven through `lch_block_create(cfg, NULL)` produces a block byte-identical to the pre-change baseline fixture.
   - Additional case: calling `lch_block_create(cfg, NULL)` on a config with a callback-backed table returns `LCH_FAILURE` and logs a message naming the offending table.
   - Additional case: calling `lch_block_create(cfg, &callbacks)` with `callbacks.read_cell == NULL` on a config that has any callback-backed table returns `LCH_FAILURE` (with a message naming the offending table).
   - Additional case: calling `lch_block_create(cfg, &callbacks)` on an all-CSV config with every field of `callbacks` set to NULL succeeds and produces the same block as `lch_block_create(cfg, NULL)`.
3. `cargo clippy -- -D warnings`, `cargo fmt --check`, `clang-format -i include/leech2.h tests/test_c_ffi.c`, `cppcheck --error-exitcode=1 --enable=warning,style,performance,portability tests/test_c_ffi.c include/leech2.h`.
4. Sanity check: invoking `lch_block_create(cfg, NULL)` on a config with any callback-backed table returns `LCH_FAILURE` and logs a clear message naming the offending table.

## PR label

`breaking` — `lch_block_create()` gains a new `callbacks` parameter, so existing C callers must update their call sites (typically `lch_block_create(cfg)` → `lch_block_create(cfg, NULL)`). The behavior of a CSV-only config with `NULL` `callbacks` is unchanged, and the on-disk block format is unchanged.

## Non-goals (deliberately out of scope)

- Schema declaration via FFI (still uses `config.toml`).
- Multi-threaded callbacks.
- Re-entrancy from inside a callback into another leech2 FFI function.
- Custom string/number coercion in the callback path. The cell kind must match the field's declared kind; mixed-mode coercion (e.g. TEXT into a NUMBER field) is reserved for the CSV path where it's unavoidable.
- Applying `FilterConfig` rules (`max-field-length`, `include`, `exclude`) to callback-backed tables. The callback owns row inclusion via `LCH_FILTER_RECORD`.
