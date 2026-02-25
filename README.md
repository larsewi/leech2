# leech2

leech2 tracks changes to CSV-backed database tables using a git-like
content-addressable block chain. It computes deltas between CSV snapshots,
stores them as linked blocks, and can produce consolidated patches that convert
into SQL statements for replaying changes on a target database.

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

# Generate a patch covering the last 2 blocks
lch patch create -n 2

# Convert the patch to SQL
lch patch sql
```

## Configuration

Config can be `config.toml` or `config.json`. TOML example:

```toml
compression = true          # enable zstd compression (default: true)
compression-level = 3       # zstd level (default: 0)

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

### Field types

| Type | SQL literal | Notes |
|------|-------------|-------|
| `TEXT` | `'value'` | Single quotes, escaped |
| `INTEGER` | `42` | Validated as `i64` |
| `FLOAT` | `3.14` | Validated as `f64` |
| `BOOLEAN` | `TRUE`/`FALSE` | Accepts `true/false`, `1/0`, `t/f`, `yes/no` |
| `BINARY` | `'\xDEADBEEF'` | Hex-encoded input |
| `DATE` | `'2024-01-15'` | Parsed with `format` (default `%Y-%m-%d`) |
| `TIME` | `'10:30:00'` | Parsed with `format` (default `%H:%M:%S`) |
| `DATETIME` | `'2024-01-15 10:30:00'` | Parsed with `format` or as unix epoch |

### History truncation

An optional `[truncate]` section controls automatic pruning of old block files
after every `lch_block_create()` / `lch block create`:

```toml
[truncate]
max-blocks = 100    # keep at most 100 blocks in the chain (>= 1)
max-age = "7d"      # remove blocks older than this duration
```

Both fields are optional and independent. Supported duration suffixes: `s`
(seconds), `m` (minutes), `h` (hours), `d` (days), `w` (weeks).

Truncation always removes orphaned blocks (on disk but not reachable from HEAD)
and blocks older than the last reported position (see `lch_patch_applied`).

### Validation

- Each table must have at least one field marked `primary-key = true`
- Field names within a table must be unique

## CLI

```
lch [-C <dir>] <command>

Commands:
  init                        Initialize .leech2/ with example config and CSV
  block create                Snapshot CSVs and create a new block
  block show [REF|-n N]       Display a block's contents
  patch create <REF|-n N>     Build a patch from REF (or N blocks back) to HEAD
  patch show                  Display the last generated patch
  patch sql                   Convert the last patch to SQL
  log                         List all blocks from HEAD to genesis
```

`REF` can be a full SHA-1 hash or an unambiguous prefix. Output is paged
through `$PAGER` (defaults to `less`).

## C API

```c
#include "leech2.h"

int   lch_init(const char *work_dir);
int   lch_block_create(void);
int   lch_patch_create(const char *hash, uint8_t **buf, size_t *len);
int   lch_patch_applied(uint8_t *buf, size_t len, int reported);
int   lch_patch_to_sql(const uint8_t *buf, size_t len, char **sql);
void  lch_free_buf(uint8_t *buf, size_t len);
void  lch_free_str(char *str);
```

All functions return `0` on success, `-1` on error. Errors are logged via
`env_logger` (set `RUST_LOG=debug` for detailed output).

### Lifecycle

```
 1. Initialize      lch_init(work_dir)
                     Parses config, stores it globally.

 2. Create block    lch_block_create()
                     Reads CSVs -> computes new state -> diffs against
                     previous state -> writes block + STATE + HEAD.
                     Runs history truncation afterwards.

 3. Create patch    lch_patch_create(last_known_hash)
                     Walks the chain from HEAD to the given hash,
                     merging deltas. Returns the encoded patch.

 4. Generate SQL    lch_patch_to_sql(buf, len)
                     Decodes the patch and produces SQL:
                     - Delta payload: DELETE + INSERT + UPDATE statements
                     - State payload: TRUNCATE + INSERT statements
                     All wrapped in BEGIN/COMMIT.

 5. Report patch    lch_patch_applied(buf, len, reported)
                     Frees the patch buffer. If reported=1, also
                     updates the REPORTED file so truncation knows
                     which blocks are safe to remove.
```

### Example

```c
lch_init("/path/to/.leech2");

// ... modify CSVs ...

lch_block_create();

uint8_t *buf;
size_t len;
lch_patch_create("0000000000000000000000000000000000000000", &buf, &len);

char *sql;
lch_patch_to_sql(buf, len, &sql);
printf("%s", sql);
lch_free_str(sql);

// Send patch to hub, then free buffer + update REPORTED
int ok = hub_send(buf, len);
lch_patch_applied(buf, len, ok);
```

## Contributing

See [CONTRIBUTING.md](CONTRIBUTING.md) for architecture details, data flow, and
development guidelines.
