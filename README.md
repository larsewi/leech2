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

## Concepts

### Work directory

All leech2 state lives in a single directory (`.leech2/` when using the CLI,
or any path passed to `lch_init`). It contains:

| File | Description |
|------|-------------|
| `config.toml` or `config.json` | Table definitions and field schemas |
| `HEAD` | Current block hash (40-character hex string) |
| `STATE` | Protobuf-encoded snapshot of all tables |
| `PATCH` | Last generated patch (CLI only) |
| `<sha1>` | Protobuf-encoded block files, named by their hash |

CSV source files are referenced by the config's `source` field. The path is
resolved relative to the work directory but can also be an absolute path, so
CSVs can live anywhere on the filesystem.

### Blocks

A block is a content-addressable unit containing:
- A **parent hash** pointing to the previous block (or the genesis hash
  `0000000000000000000000000000000000000000`)
- A **timestamp**
- A list of **deltas**, one per table that changed

Blocks form a singly-linked chain from HEAD back to genesis. Each block is
SHA-1 hashed from its protobuf encoding and stored under that hash in the work
directory.

### Deltas

A delta describes the difference between two states of a single table:
- **Inserts** -- new rows (primary key + values)
- **Deletes** -- removed rows (primary key + values)
- **Updates** -- changed rows (primary key + old values + new values)

When multiple blocks are consolidated into a patch, their deltas are merged
pairwise using 15 rules documented in
[DELTA_MERGING_RULES.md](DELTA_MERGING_RULES.md).

### Patches

A patch consolidates all blocks between a given reference point and HEAD. It
walks the chain backward, merging deltas at each step. The final payload is
whichever is smaller:
- The **consolidated deltas** (stripped down: delete values removed, updates
  sparse-encoded to only changed indices and new values)
- The **full current state** (a complete snapshot of all tables)

If delta consolidation fails due to conflicts, the patch falls back to the full
state automatically.

Patches are serialized as protobuf, optionally compressed with zstd.

### State

A state is a snapshot of all tables at a point in time. It is recomputed from
the CSV source files on each `block create` and persisted as the `STATE` file.

## Lifecycle

```
 1. Initialize      lch_init(work_dir)
                     Parses config, stores it globally.

 2. Create block    lch_block_create()
                     Reads CSVs -> computes new state -> diffs against
                     previous state -> writes block + STATE + HEAD.

 3. Create patch    lch_patch_create(last_known_hash)
                     Walks the chain from HEAD to the given hash,
                     merging deltas. Returns the encoded patch.

 4. Generate SQL    lch_patch_to_sql(buf, len)
                     Decodes the patch and produces SQL:
                     - Delta payload: DELETE + INSERT + UPDATE statements
                     - State payload: TRUNCATE + INSERT statements
                     All wrapped in BEGIN/COMMIT.
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

### Validation

- Each table must have at least one field marked `primary-key = true`
- Field names within a table must be unique

## C API

```c
#include "leech2.h"

int   lch_init(const char *work_dir);
int   lch_block_create(void);
int   lch_patch_create(const char *hash, uint8_t **buf, size_t *len);
int   lch_patch_to_sql(const uint8_t *buf, size_t len, char **sql);
void  lch_free_buf(uint8_t *buf, size_t len);
void  lch_free_str(char *str);
```

All functions return `0` on success, `-1` on error. Errors are logged via
`env_logger` (set `RUST_LOG=debug` for detailed output).

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
lch_free_buf(buf, len);
```

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

## Architecture

```
src/
  lib.rs        C FFI entry points
  main.rs       CLI (lch binary)
  config.rs     TOML/JSON config parsing, global OnceLock
  table.rs      CSV loading, in-memory table (HashMap<pk, values>)
  state.rs      Snapshot of all tables, protobuf persistence
  delta.rs      Diff computation + merge logic (see DELTA_MERGING_RULES.md)
  block.rs      Content-addressable block creation and loading
  patch.rs      Patch consolidation, payload selection
  head.rs       HEAD file read/write
  storage.rs    File I/O with fs2 locking
  wire.rs       Protobuf encode/decode + zstd compression
  sql.rs        Patch-to-SQL conversion with type mapping
  proto.rs      Generated protobuf code (via build.rs)
  utils.rs      SHA-1 hashing, timestamp formatting

proto/          Protobuf definitions (compiled at build time by prost-build)
include/        C header (leech2.h)
tests/          Acceptance tests
```

### Data flow

```
CSV files
    |
    v
Table::load()          Parse CSV into HashMap<primary_key, subsidiary_values>
    |
    v
State::compute()       Collect all tables into a State
    |
    +---> Delta::compute(prev_state, new_state)
    |         |
    |         v
    |     Block { parent, timestamp, deltas }
    |         |
    |         +--> SHA-1 hash --> stored as file
    |         +--> HEAD updated
    |
    +--> STATE file updated
```

```
Patch::create(last_known_hash)
    |
    v
Walk chain: HEAD -> ... -> last_known
    |
    v
Block::merge() at each step (see DELTA_MERGING_RULES.md)
    |
    v
Strip: key-only deletes, sparse updates
    |
    v
Compare encoded sizes: deltas vs full state
    |
    v
Patch { head_hash, timestamp, num_blocks, payload }
    |
    v
wire::encode_patch()  -->  protobuf + optional zstd
    |
    v
sql::patch_to_sql()
    |
    +--> Deltas payload: BEGIN; DELETE...; INSERT...; UPDATE...; COMMIT;
    +--> State payload:  BEGIN; TRUNCATE...; INSERT...; COMMIT;
```
