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
and blocks older than the last reported position (see `lch_patch_free`).

## C API

See [`include/leech2.h`](include/leech2.h) for the full API reference.

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

int flags = hub_send(buf, len) ? LCH_PATCH_APPLIED : 0;
lch_patch_free(config, buf, len, flags);

lch_deinit(config);
```

## Contributing

See [CONTRIBUTING.md](CONTRIBUTING.md) for architecture details, data flow, and
development guidelines.
