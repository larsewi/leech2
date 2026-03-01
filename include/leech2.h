/**
 * @file leech2.h
 * @brief C API for the leech2 library.
 *
 * leech2 tracks CSV data sources, computes diffs between snapshots, and
 * produces SQL patches that can be applied to a downstream database.
 *
 * All functions (except lch_init, lch_deinit, and lch_free_sql) return
 * LCH_SUCCESS on success and LCH_FAILURE on error. Errors are logged via
 * env_logger; set the RUST_LOG environment variable (e.g. RUST_LOG=debug)
 * for detailed output.
 */

#ifndef __LEECH2_H__
#define __LEECH2_H__

#include <stddef.h>
#include <stdint.h>

#define LCH_SUCCESS  0
#define LCH_FAILURE -1

/**
 * Opaque configuration handle.
 *
 * Created by lch_init() and freed by lch_deinit(). All other API functions
 * require a valid handle obtained from lch_init().
 */
typedef struct Config lch_config_t;

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
 * @param config  Handle previously returned by lch_init(), or NULL.
 */
extern void lch_deinit(lch_config_t *config);

/**
 * Create a new block from the current CSV data.
 *
 * Reads the configured CSV sources, computes the new state, diffs it against
 * the previous state, and writes a new block together with updated STATE and
 * HEAD files. History truncation is performed afterwards.
 *
 * @param config  Valid config handle (must not be NULL).
 * @return LCH_SUCCESS on success, LCH_FAILURE on error.
 */
extern int lch_block_create(const lch_config_t *config);

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
 * The buffer written to @p buf must eventually be passed to lch_patch_applied()
 * which frees it.
 *
 * @param config  Valid config handle (must not be NULL).
 * @param hash    Last-known block hash (null-terminated string), or NULL.
 * @param[out] buf  Receives a pointer to the encoded patch buffer.
 * @param[out] len  Receives the length of the patch buffer in bytes.
 * @return LCH_SUCCESS on success, LCH_FAILURE on error.
 */
extern int lch_patch_create(const lch_config_t *config, const char *hash, uint8_t **buf, size_t *len);

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
 * @param config  Valid config handle (must not be NULL).
 * @param buf     Pointer to the encoded patch (must not be NULL).
 * @param len     Length of @p buf in bytes.
 * @param[out] sql  Receives a pointer to the SQL string, or NULL if the patch
 *                  is empty. Free with lch_free_sql().
 * @return LCH_SUCCESS on success, LCH_FAILURE on error.
 */
extern int lch_patch_to_sql(const lch_config_t *config, const uint8_t *buf, size_t len, char **sql);

/**
 * Mark a patch as applied and free its buffer.
 *
 * Always frees the buffer pointed to by @p buf, regardless of errors or the
 * value of @p reported. After this call, @p buf is invalid and must not be
 * used.
 *
 * If @p reported is non-zero, the REPORTED file is updated with the patch's
 * head hash so that future truncation knows which blocks are safe to remove.
 *
 * @param config    Valid config handle (must not be NULL).
 * @param buf       Patch buffer previously returned by lch_patch_create(),
 *                  or NULL.
 * @param len       Length of @p buf in bytes.
 * @param reported  Non-zero if the patch was successfully sent to the hub;
 *                  zero otherwise.
 * @return LCH_SUCCESS on success, LCH_FAILURE on error (the buffer is still freed).
 */
extern int lch_patch_applied(const lch_config_t *config, uint8_t *buf, size_t len, int reported);

/**
 * Free an SQL string returned by lch_patch_to_sql().
 *
 * Passing NULL is a safe no-op.
 *
 * @param sql  SQL string to free, or NULL.
 */
extern void lch_free_sql(char *sql);

#endif /* __LEECH2_H__ */
