/**
 * @file leech2.h
 * @brief C API for the leech2 library.
 *
 * leech2 tracks CSV data sources, computes deltas between snapshots, and
 * produces SQL patches that can be applied to a downstream database.
 */

#ifndef __LEECH2_H__
#define __LEECH2_H__

#include <stdbool.h>
#include <stddef.h>
#include <stdint.h>

#ifdef __cplusplus
extern "C" {
#endif

#define LCH_SUCCESS 0
#define LCH_FAILURE -1

/* Cell-callback return codes (see lch_read_cell_cb_t). */
#define LCH_END_OF_TABLE 1
#define LCH_SKIP_RECORD 2

/**
 * Log severity levels.
 *
 * @note LCH_LOG_TRACE messages are only emitted in debug builds. Release
 *       builds strip trace-level logging at compile time.
 */
typedef enum {
  LCH_LOG_ERROR = 1,
  LCH_LOG_WARN = 2,
  LCH_LOG_INFO = 3,
  LCH_LOG_DEBUG = 4,
  LCH_LOG_TRACE = 5,
} lch_log_level_t;

typedef enum {
  LCH_VALUE_NULL = 0,
  LCH_VALUE_TEXT = 1,
  LCH_VALUE_NUMBER = 2,
  LCH_VALUE_BOOLEAN = 3,
} lch_kind_t;

typedef struct {
  /* Must match the declared kind of the field this cell represents:
   *   TEXT field    -> LCH_VALUE_TEXT or LCH_VALUE_NULL
   *   NUMBER field  -> LCH_VALUE_NUMBER or LCH_VALUE_NULL
   *   BOOLEAN field -> LCH_VALUE_BOOLEAN or LCH_VALUE_NULL
   * LCH_VALUE_NULL is rejected on primary-key fields. */
  lch_kind_t kind;
  union {
    /* Valid when kind == LCH_VALUE_TEXT. Null-terminated, must not be NULL;
     * use LCH_VALUE_NULL to represent a null value. */
    const char *text;
    /* Valid when kind == LCH_VALUE_NUMBER. Must be finite (not NaN/Inf). */
    double number;
    /* Valid when kind == LCH_VALUE_BOOLEAN. */
    bool boolean;
  };
} lch_cell_t;

/**
 * Owned byte buffer returned by the library.
 *
 * Functions that allocate a buffer fill in @p data and @p len. The buffer must
 * eventually be released with the matching free routine (see each function's
 * documentation). On failure, the fields are left untouched.
 */
typedef struct {
  uint8_t *data;
  size_t len;
} lch_buffer_t;

/**
 * Callback type for receiving log messages.
 *
 * @param level     Severity level of the message.
 * @param msg       Null-terminated log message string. Only valid for the
 *                  duration of the callback invocation.
 * @param usr_data  Opaque pointer passed to lch_log_init().
 */
typedef void (*lch_log_callback_t)(lch_log_level_t level, const char *msg,
                                   void *usr_data);

/**
 * Initialize logging with a callback.
 *
 * Installs a custom logger that delivers all log messages through @p callback.
 * Must be called before lch_init() for the callback to receive messages from
 * initialization.
 *
 * May be called again to atomically replace @p callback and @p usr_data. After
 * a replacement, the previous callback is no longer invoked; the library does
 * not free the previous @p usr_data, so the caller is responsible for its
 * lifetime.
 *
 * Safe to call concurrently from multiple threads. Once installed, @p callback
 * itself may be invoked from any thread, possibly in parallel, so both
 * @p callback and @p usr_data must be thread-safe.
 *
 * @param callback  Function to receive log messages (must not be NULL).
 * @param usr_data  Opaque pointer forwarded to every callback invocation. Must
 *                  remain valid until the callback is replaced by a later
 *                  lch_log_init() call or the process exits.
 * @return LCH_SUCCESS on success, LCH_FAILURE on error.
 */
extern int lch_log_init(lch_log_callback_t callback, void *usr_data);

/**
 * Opaque configuration handle.
 *
 * Created by lch_init() and freed by lch_deinit(). All other API functions
 * require a valid handle obtained from lch_init().
 */
typedef struct LchConfig lch_config_t;

/**
 * Initialize the library and load configuration.
 *
 * Parses the configuration found in @p work_dir and returns an opaque handle
 * used by all subsequent API calls.
 *
 * @param work_dir  Path to the leech2 working directory (must not be NULL).
 * @return An opaque config handle on success, or NULL on failure.
 *         The caller must free the handle with lch_deinit().
 */
extern lch_config_t *lch_init(const char *work_dir);

/**
 * Free a configuration handle.
 *
 * Releases all resources associated with the handle. Passing NULL is a safe
 * no-op. After this call the handle is invalid and must not be used.
 *
 * @param cfg  Handle previously returned by lch_init(), or NULL.
 */
extern void lch_deinit(lch_config_t *cfg);

/**
 * Per-table setup hook for callback-backed tables.
 *
 * Invoked once, before the first cell callback for @p table.
 *
 * @param table     Null-terminated table name. Borrowed; valid only for the
 *                  duration of the call.
 * @param usr_data  Opaque pointer from lch_callbacks_t::usr_data.
 * @return LCH_SUCCESS to proceed to pulling cells from this table.
 *         LCH_FAILURE to abort block creation immediately. table_end is NOT
 *         invoked when begin returns failure.
 */
typedef int (*lch_table_begin_cb_t)(const char *table, void *usr_data);

/**
 * Per-table teardown hook for callback-backed tables.
 *
 * Invoked once for every table whose lch_table_begin_cb_t returned
 * LCH_SUCCESS, including on the failure path.
 *
 * @param table     Null-terminated table name. Borrowed; valid only for the
 *                  duration of the call.
 * @param usr_data  Opaque pointer from lch_callbacks_t::usr_data. If teardown
 *                  needs to distinguish a clean drain from aborted iteration,
 *                  the callback implementation must track that state itself
 *                  via this pointer (for example by setting a flag).
 * @return LCH_SUCCESS to indicate teardown completed.
 *         LCH_FAILURE makes lch_block_create return LCH_FAILURE even if
 *         iteration up to this point succeeded.
 */
typedef int (*lch_table_end_cb_t)(const char *table, void *usr_data);

/**
 * Cell callback for callback-backed tables.
 *
 * Iteration contract:
 *   - Rows are requested in ascending order, starting from row == 0.
 *   - The order in which leech2 asks for columns within a row is unspecified
 *     and may vary across rows. The caller must support random access by
 *     @p col or @p field_name.
 *   - A table is fully drained before any other table is processed, and the
 *     callback is invoked exclusively on the thread that called
 *     lch_block_create().
 *
 * LCH_END_OF_TABLE / LCH_SKIP_RECORD on any cell short-circuits the rest
 * of the row: leech2 won't ask for the remaining cells, and any cells
 * already accepted for the row are discarded.
 *
 * @param table       Null-terminated table name. Borrowed.
 * @param row         0-based row index
 * @param col         0-based index of the field in config.toml declaration
 *                    order.
 * @param field_name  Null-terminated name of the field at @p col. Borrowed.
 * @param out_cell    On entry, zero-initialised. On LCH_SUCCESS return,
 *                    populate with the typed cell value. The kind tag must
 *                    match the field's declared kind. On LCH_END_OF_TABLE,
 *                    LCH_SKIP_RECORD, or LCH_FAILURE, the contents are
 *                    ignored.
 * @param usr_data    Opaque pointer from lch_callbacks_t::usr_data.
 * @return LCH_SUCCESS         out_cell populated; leech2 will ask for the
 *                             remaining fields of this row and then
 *                             advance to row + 1.
 *         LCH_END_OF_TABLE    No row exists at this index; iteration for
 *                             this table stops. May be returned from any
 *                             column.
 *         LCH_SKIP_RECORD     Drop the current row; leech2 does not ask for
 *                             any remaining fields of this row and advances
 *                             to row + 1. May be returned from any column.
 *         LCH_FAILURE         Unrecoverable error; block creation aborts.
 */
typedef int (*lch_read_cell_cb_t)(const char *table, size_t row, size_t col,
                                  const char *field_name, lch_cell_t *out_cell,
                                  void *usr_data);

/**
 * Callback bundle passed to lch_block_create() for callback-backed tables.
 */
typedef struct {
  /** May be NULL if no per-table setup is needed. */
  lch_table_begin_cb_t table_begin;
  /** Required when any table in the config is callback-backed. */
  lch_read_cell_cb_t read_cell;
  /** May be NULL if no per-table teardown is needed. */
  lch_table_end_cb_t table_end;
  /** Opaque pointer forwarded verbatim to every invoked callback. May be
   *  NULL if the callbacks do not need shared state. */
  void *usr_data;
} lch_callbacks_t;

/**
 * Create a new block from the current snapshot of every configured table.
 *
 * Reads each table's contents (from its configured CSV source, or via the
 * callback bundle for tables that have no source), computes the new state
 * and the delta against the previous state, and writes a new block together
 * with updated STATE and HEAD files. History truncation is performed
 * afterwards.
 *
 * @param cfg        Valid config handle (must not be NULL).
 * @param callbacks  Optional callback bundle. May be NULL when every table
 *                   in @p cfg is CSV-backed.
 * @return LCH_SUCCESS on success, LCH_FAILURE on error.
 */
extern int lch_block_create(const lch_config_t *cfg,
                            const lch_callbacks_t *callbacks);

/**
 * Create a patch from HEAD back to a known hash.
 *
 * Walks the block chain from HEAD to @p hash, merging deltas along the way.
 * On success, @p out receives the encoded patch buffer.
 *
 * If @p hash is NULL the REPORTED hash is used as the starting point; if
 * REPORTED does not exist, genesis (the very beginning of the chain) is used.
 *
 * Passing an explicit @p hash allows callers to bypass the built-in REPORTED
 * mechanism (lch_patch_applied / lch_patch_failed) and implement their own
 * system for tracking which blocks have been reported.
 *
 * The buffer written to @p out must eventually be freed with
 * lch_buffer_free().
 *
 * @param cfg       Valid config handle (must not be NULL).
 * @param hash      Last-known block hash (null-terminated string), or NULL.
 * @param[out] out  Receives the encoded patch buffer (must not be NULL).
 * @return LCH_SUCCESS on success, LCH_FAILURE on error.
 */
extern int lch_patch_create(const lch_config_t *cfg, const char *hash,
                            lch_buffer_t *out);

/**
 * Convert an encoded patch to SQL statements.
 *
 * Decodes the patch in @p patch and produces SQL that, when executed, applies
 * the patch to a downstream database:
 * - Delta payloads generate DELETE, INSERT, and UPDATE statements.
 * - State payloads generate TRUNCATE followed by INSERT statements.
 * - All statements are wrapped in BEGIN / COMMIT.
 *
 * If the patch contains no actionable changes, @p sql is set to NULL and the
 * function returns LCH_SUCCESS.
 *
 * @param cfg       Valid config handle (must not be NULL).
 * @param patch     Encoded patch buffer (must not be NULL).
 * @param[out] sql  Receives a pointer to the SQL string, or NULL if the patch
 *                  is empty. Free with lch_sql_free().
 * @return LCH_SUCCESS on success, LCH_FAILURE on error.
 */
extern int lch_patch_to_sql(const lch_config_t *cfg, const lch_buffer_t *patch,
                            char **sql);

/**
 * Inject a field into an encoded patch.
 *
 * Decodes the patch in @p in, adds or overwrites an injected field with the
 * given @p name and @p cell, and encodes the result into a new caller-owned
 * buffer written to @p out. The input buffer is not modified; the caller
 * manages its lifetime independently.
 *
 * The kind tag on @p cell determines how the value is formatted as a SQL
 * literal (TEXT becomes single-quoted, NUMBER is emitted as a numeric
 * literal, BOOLEAN is emitted as TRUE/FALSE). LCH_VALUE_NULL is not
 * accepted.
 *
 * If a field with the same @p name is already present on the patch -- whether
 * from static configuration or a prior injection -- both its value and kind
 * are replaced.
 *
 * The buffer written to @p out must eventually be freed with
 * lch_buffer_free().
 *
 * @param cfg       Valid config handle (must not be NULL).
 * @param in        Encoded input patch (must not be NULL).
 * @param name      Column name (non-empty, null-terminated).
 * @param cell      Typed value to inject (must not be NULL).
 * @param[out] out  Receives the encoded output patch (must not be NULL).
 * @return LCH_SUCCESS on success, LCH_FAILURE on error.
 */
extern int lch_patch_inject(const lch_config_t *cfg, const lch_buffer_t *in,
                            const char *name, const lch_cell_t *cell,
                            lch_buffer_t *out);

/**
 * Mark a patch as applied.
 *
 * Updates the REPORTED file with the patch's head hash so that future
 * truncation knows which blocks are safe to remove.
 *
 * @param cfg    Valid config handle (must not be NULL).
 * @param patch  Encoded patch buffer (must not be NULL).
 * @return LCH_SUCCESS on success, LCH_FAILURE on error.
 */
extern int lch_patch_applied(const lch_config_t *cfg,
                             const lch_buffer_t *patch);

/**
 * Mark a patch as failed.
 *
 * Removes the REPORTED file so that the next lch_patch_create() produces a
 * full state patch (TRUNCATE + INSERT for all tables). This is safe to call
 * regardless of whether a REPORTED file exists.
 *
 * @param cfg  Valid config handle (must not be NULL).
 * @return LCH_SUCCESS on success, LCH_FAILURE on error.
 */
extern int lch_patch_failed(const lch_config_t *cfg);

/**
 * Free a library-owned buffer.
 *
 * Passing NULL is a safe no-op, as is passing a buffer with @c data set to
 * NULL. After this call, the buffer's @c data pointer is invalid and must not
 * be used; the caller may reset the struct or let it go out of scope.
 *
 * @param buf  Buffer to free, or NULL.
 */
extern void lch_buffer_free(lch_buffer_t *buf);

/**
 * Free an SQL string returned by lch_patch_to_sql().
 *
 * Passing NULL is a safe no-op.
 *
 * @param sql  SQL string to free, or NULL.
 */
extern void lch_sql_free(char *sql);

#ifdef __cplusplus
}
#endif

#endif /* __LEECH2_H__ */
