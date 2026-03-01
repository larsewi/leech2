# leech2

<p align="center">
  <img src="logo.svg" alt="Leech Logo" width="120"/>
</p>

leech2 tracks changes to tables using a git-like content-addressable block
chain. It computes deltas between table snapshots, stores them as linked blocks,
and can produce consolidated patches that convert into SQL statements for
replaying changes on a target database.

leech2 ships as both a Rust library with a C-compatible FFI (`libleech2.so`)
and a CLI tool (`lch`).

## Build dependencies

```
# On macOS
brew install protobuf

# On Linux
sudo apt install protobuf-compiler

# On Windows
choco install protoc
```

## Building

```sh
cargo build            # build the library and CLI
cargo test             # run all tests
```

## Quick start

```sh
# Initialize a work directory with an example table
lch init

# Edit the CSV, then create a block to record the changes
lch block create

# Make more edits and create another block
lch block create

# Generate a patch (from REPORTED, or GENESIS on first run)
lch patch create

# Convert the patch to SQL
lch patch sql

# Mark the patch as applied so next patch starts from here
lch patch applied
```

## Configuration

Config can be `config.toml` or `config.json`.

### Tables

- Each table must have at least one field marked `primary-key = true`
- Field names within a table must be unique
- The type field maps table enties to the correct SQL database types
- Some type fields require a format specifier

```toml
[tables.employees]
source = "employees.csv"    # relative to work dir, or absolute
header = true

[[tables.employees.fields]]
name = "employee_id"
type = "INTEGER"
primary-key = true

[[tables.employees.fields]]
name = "first_name"
type = "TEXT"

[[tables.employees.fields]]
name = "hire_date"
type = "DATE"
format = "%Y-%m-%d"
```

| Type       | SQL literal             | Notes                                        |
|------------|-------------------------|----------------------------------------------|
| `TEXT`     | `'value'`               | Single quotes, escaped                       |
| `INTEGER`  | `42`                    | Validated as `i64`                           |
| `FLOAT`    | `3.14`                  | Validated as `f64`                           |
| `BOOLEAN`  | `TRUE`/`FALSE`          | Accepts `true/false`, `1/0`, `t/f`, `yes/no` |
| `BINARY`   | `'\xDEADBEEF'`          | Hex-encoded input                            |
| `DATE`     | `'2024-01-15'`          | Parsed with `format` (default `%Y-%m-%d`)    |
| `TIME`     | `'10:30:00'`            | Parsed with `format` (default `%H:%M:%S`)    |
| `DATETIME` | `'2024-01-15 10:30:00'` | Parsed with `format` or as unix epoch        |

### Compression

Patches are compressed with zstd by default. An optional `[compression]` section
controls this:

```toml
[compression]
enable = true  # enable zstd compression (default: true)
level = 3      # zstd level (default: 0)
```

### History truncation

An optional `[truncate]` section controls automatic pruning of old block files
after every `lch_block_create()` / `lch block create`:

```toml
[truncate]
max-blocks = 100  # keep at most 100 blocks in the chain (>= 1)
max-age = "7d"    # remove blocks older than this duration
```

Both fields are optional and independent. Supported duration suffixes: `s`
(seconds), `m` (minutes), `h` (hours), `d` (days), `w` (weeks).

Truncation always removes orphaned blocks (on disk but not reachable from HEAD)
and blocks older than the last reported position (see `lch_patch_applied`).

## C API

```c
#include "leech2.h"

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
 * Parses the configuration found in work_dir and returns an opaque handle
 * used by all subsequent API calls.
 *
 * @param work_dir  Path to the leech2 working directory (must not be NULL).
 * @return An opaque config handle on success, or NULL on failure.
 *         The caller must free the handle with lch_deinit().
 */
lch_config_t *lch_init(const char *work_dir);

/**
 * Free a configuration handle.
 *
 * Releases all resources associated with the handle. Passing NULL is a safe
 * no-op. After this call the handle is invalid and must not be used.
 *
 * @param config  Handle previously returned by lch_init(), or NULL.
 */
void lch_deinit(lch_config_t *config);

/**
 * Create a new block from the current CSV data.
 *
 * Reads the configured CSV sources, computes the new state, diffs it against
 * the previous state, and writes a new block together with updated STATE and
 * HEAD files. History truncation is performed afterwards.
 *
 * @param config  Valid config handle (must not be NULL).
 * @return 0 on success, -1 on error.
 */
int lch_block_create(const lch_config_t *config);

/**
 * Create a patch from HEAD back to a known hash.
 *
 * Walks the block chain from HEAD to hash, merging deltas along the way.
 * The resulting patch is encoded into a caller-owned buffer written to
 * buf and len.
 *
 * If hash is NULL the REPORTED hash is used as the starting point; if
 * REPORTED does not exist, genesis (the very beginning of the chain) is used.
 *
 * The buffer written to buf must eventually be passed to lch_patch_applied()
 * which frees it.
 *
 * @param config    Valid config handle (must not be NULL).
 * @param hash      Last-known block hash (null-terminated string), or NULL.
 * @param[out] buf  Receives a pointer to the encoded patch buffer.
 * @param[out] len  Receives the length of the patch buffer in bytes.
 * @return 0 on success, -1 on error.
 */
int lch_patch_create(const lch_config_t *config, const char *hash, uint8_t **buf, size_t *len);

/**
 * Convert an encoded patch to SQL statements.
 *
 * Decodes the patch in buf and produces SQL that, when executed, applies the
 * patch to a downstream database:
 * - Delta payloads generate DELETE, INSERT, and UPDATE statements.
 * - State payloads generate TRUNCATE followed by INSERT statements.
 * - All statements are wrapped in BEGIN / COMMIT.
 *
 * If the patch contains no actionable changes, sql is set to NULL and the
 * function returns 0.
 *
 * @param config    Valid config handle (must not be NULL).
 * @param buf       Pointer to the encoded patch (must not be NULL).
 * @param len       Length of buf in bytes.
 * @param[out] sql  Receives a pointer to the SQL string, or NULL if the patch
 *                  is empty. Free with lch_free_sql().
 * @return 0 on success, -1 on error.
 */
int lch_patch_to_sql(const lch_config_t *config, const uint8_t *buf, size_t len, char **sql);

/**
 * Mark a patch as applied and free its buffer.
 *
 * Always frees the buffer pointed to by buf, regardless of errors or the
 * value of reported. After this call, buf is invalid and must not be used.
 *
 * If reported is non-zero, the REPORTED file is updated with the patch's
 * head hash so that future truncation knows which blocks are safe to remove.
 *
 * @param config    Valid config handle (must not be NULL).
 * @param buf       Patch buffer previously returned by lch_patch_create(),
 *                  or NULL.
 * @param len       Length of buf in bytes.
 * @param reported  Non-zero if the patch was successfully sent to the hub;
 *                  zero otherwise.
 * @return 0 on success, -1 on error (the buffer is still freed).
 */
int lch_patch_applied(const lch_config_t *config, uint8_t *buf, size_t len, int reported);

/**
 * Free an SQL string returned by lch_patch_to_sql().
 *
 * Passing NULL is a safe no-op.
 *
 * @param sql  SQL string to free, or NULL.
 */
void lch_free_sql(char *sql);
```

All functions (except `lch_init`, `lch_deinit`, and `lch_free_sql`) return `0`
on success and `-1` on error. Errors are logged via `env_logger` (set
`RUST_LOG=debug` for detailed output).

### Example

```c
lch_config_t *config = lch_init("/path/to/.leech2");

lch_block_create(config);

uint8_t *buf;
size_t len;
lch_patch_create(config, NULL, &buf, &len);

char *sql;
lch_patch_to_sql(config, buf, len, &sql);
printf("%s", sql);
lch_free_sql(sql);

int ok = hub_send(buf, len);
lch_patch_applied(config, buf, len, ok);

lch_deinit(config);
```

## Contributing

See [CONTRIBUTING.md](CONTRIBUTING.md) for architecture details, data flow, and
development guidelines.
