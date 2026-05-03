/**
 * @file leech2.h
 * @brief C API for the leech2 library.
 *
 * leech2 tracks CSV data sources, computes diffs between snapshots, and
 * produces SQL patches that can be applied to a downstream database.
 */

#ifndef __LEECH2_H__
#define __LEECH2_H__

#include <stddef.h>
#include <stdint.h>

#ifdef __cplusplus
extern "C" {
#endif

#define LCH_SUCCESS 0
#define LCH_FAILURE -1

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
 * Create a new block from the current CSV data.
 *
 * Reads the configured CSV sources, computes the new state, diffs it against
 * the previous state, and writes a new block together with updated STATE and
 * HEAD files. History truncation is performed afterwards.
 *
 * @param cfg  Valid config handle (must not be NULL).
 * @return LCH_SUCCESS on success, LCH_FAILURE on error.
 */
extern int lch_block_create(const lch_config_t *cfg);

/**
 * Create a patch from HEAD back to a known hash.
 *
 * Walks the block chain from HEAD to @p hash, merging deltas along the way.
 * The resulting patch is encoded into a caller-owned buffer written to
 * @p buf and @p len.
 *
 * If @p hash is NULL the REPORTED hash is used as the starting point; if
 * REPORTED does not exist, genesis (the very beginning of the chain) is used.
 *
 * Passing an explicit @p hash allows callers to bypass the built-in REPORTED
 * mechanism (lch_patch_applied / lch_patch_failed) and implement their own
 * system for tracking which blocks have been reported.
 *
 * The buffer written to @p buf must eventually be freed with lch_patch_free().
 *
 * @param cfg       Valid config handle (must not be NULL).
 * @param hash      Last-known block hash (null-terminated string), or NULL.
 * @param[out] buf  Receives a pointer to the encoded patch buffer.
 * @param[out] len  Receives the length of the patch buffer in bytes.
 * @return LCH_SUCCESS on success, LCH_FAILURE on error.
 */
extern int lch_patch_create(const lch_config_t *cfg, const char *hash,
                            uint8_t **buf, size_t *len);

/**
 * Convert an encoded patch to SQL statements.
 *
 * Decodes the patch in @p buf and produces SQL that, when executed, applies the
 * patch to a downstream database:
 * - Delta payloads generate DELETE, INSERT, and UPDATE statements.
 * - State payloads generate TRUNCATE followed by INSERT statements.
 * - All statements are wrapped in BEGIN / COMMIT.
 *
 * If the patch contains no actionable changes, @p sql is set to NULL and the
 * function returns LCH_SUCCESS.
 *
 * @param cfg       Valid config handle (must not be NULL).
 * @param buf       Pointer to the encoded patch (must not be NULL).
 * @param len       Length of @p buf in bytes.
 * @param[out] sql  Receives a pointer to the SQL string, or NULL if the patch
 *                  is empty. Free with lch_sql_free().
 * @return LCH_SUCCESS on success, LCH_FAILURE on error.
 */
extern int lch_patch_to_sql(const lch_config_t *cfg, const uint8_t *buf,
                            size_t len, char **sql);

/**
 * Inject a field into an encoded patch.
 *
 * Decodes the patch in @p in_buf, adds or overwrites an injected field with
 * the given @p name, @p value, and @p type, and encodes the result into a
 * new caller-owned buffer written to @p out_buf and @p out_len. The input
 * buffer is not modified; the caller manages its lifetime independently.
 *
 * @p type controls how @p value is formatted as a SQL literal. It must be
 * one of "TEXT" (single-quoted), "NUMBER" (numeric, unquoted), or "BOOLEAN"
 * (emitted as TRUE/FALSE). Matching is case-insensitive.
 *
 * If a field with the same @p name is already present on the patch — whether
 * from static configuration or a prior injection — both its value and type
 * are replaced.
 *
 * The buffer written to @p out_buf must eventually be freed with
 * lch_patch_free().
 *
 * @param cfg           Valid config handle (must not be NULL).
 * @param in_buf        Pointer to the encoded input patch (must not be NULL).
 * @param in_len        Length of @p in_buf in bytes.
 * @param name          Column name (non-empty, null-terminated).
 * @param value         Value to inject (null-terminated).
 * @param type          "TEXT", "NUMBER", or "BOOLEAN" (null-terminated).
 * @param[out] out_buf  Receives a pointer to the encoded output patch.
 * @param[out] out_len  Receives the length of @p out_buf in bytes.
 * @return LCH_SUCCESS on success, LCH_FAILURE on error.
 */
extern int lch_patch_inject(const lch_config_t *cfg, const uint8_t *in_buf,
                            size_t in_len, const char *name, const char *value,
                            const char *type, uint8_t **out_buf,
                            size_t *out_len);

/**
 * Mark a patch as applied.
 *
 * Updates the REPORTED file with the patch's head hash so that future
 * truncation knows which blocks are safe to remove.
 *
 * @param cfg  Valid config handle (must not be NULL).
 * @param buf  Pointer to the encoded patch (must not be NULL).
 * @param len  Length of @p buf in bytes.
 * @return LCH_SUCCESS on success, LCH_FAILURE on error.
 */
extern int lch_patch_applied(const lch_config_t *cfg, const uint8_t *buf,
                             size_t len);

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
 * Free a patch buffer without marking it as applied.
 *
 * Passing NULL is a safe no-op. After this call, @p buf is invalid and must
 * not be used.
 *
 * @param buf  Patch buffer previously returned by lch_patch_create(), or NULL.
 * @param len  Length of @p buf in bytes.
 */
extern void lch_patch_free(uint8_t *buf, size_t len);

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
