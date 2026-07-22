# leech2

<p align="center">
  <img src="logo.svg" alt="Leech Logo" width="100"/>
</p>

leech2 tracks changes to tables. It computes deltas between table snapshots,
stores them as linked blocks, and can produce consolidated patches that convert
into SQL statements for replaying changes on a target database.

leech2 ships as both a Rust library with a C-compatible FFI (`libleech2.so`) and
a CLI tool (`lch`).

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
cargo build  # build the library and CLI
cargo test   # run all tests
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

# If SQL application fails, force full state on next patch
lch patch failed
```

Pass `--dry-run` to any command to compute the changes and print what it `Would
have ...` done without changing anything on the disk:

## Configuration

The configuration lives in either `config.toml` or `config.json`. The CLI tool
currently expects this file to be inside a `.leech/` directory in the current
working directory (this may change). The C API does not care.

### State directory

State files (`HEAD`, `STATE`, `REPORTED`, `PATCH`, and block files) live in a
`state` subdirectory next to the config (in the work directory) by default. The
optional top-level `state-dir` option allows you to point it elsewhere:

```toml
state-dir = "/var/lib/leech2"
```

Use can either use an absolute path or path relative to the work directory.

### Drop-in fragments

The base config may pull in additional config files via a top-level `include`
key holding a list of glob patterns. This lets a package that bundles leech2
ship a read-only base config while still letting users extend / overwrite the
reporting system by dropping fragment files into an included directory. This way
users can extend or overwrite config options without having to edit the package
bundled file.

```toml
include = ["conf.d/*.toml", "conf.d/*.json"]
```

- Relative patterns resolve against the work directory; absolute patterns are
  used as-is. A pattern that matches nothing is not an error.
- Fragments use the same schema as the base config and may be `.toml` or `.json`
  regardless of the base file's format. Every section is optional.
- Fragments are deep-merged in order: the base first, then each `include`
  pattern in the order listed, with each pattern's matches sorted by filename.
- Merging is **last-wins** and recurses into sections.
- A base `config.toml`/`config.json` is required, and only the base may declare
  `include` (nested includes are not supported).

### Tables

- Each table must have at least one field marked `primary-key = true`
- Field names within a table must be unique
- A table is **CSV-backed** when it has a `[tables.X.csv]` block declaring a
  `source`; otherwise it is **callback-backed** and its rows are pulled from
  the FFI cell callback at block creation time.
- Inside a `[csv]` block, when `header = false` (the default), CSV columns are
  mapped to config fields by position.
- When `header = true`, the first row of the CSV is treated as a header. Each
  config field is matched to a CSV column by name. Hence, columns may appear in
  any order. Every config field name must be present in the header; extra CSV
  columns are ignored.
- The type field controls how values are quoted in generated SQL string. These
  are not database column types. Your database may use any compatible type (e.g.
  `INTEGER`, `FLOAT`, `TIMESTAMP`). It is your responsibility to ensure the
  quoted literals are valid for your target database type.
- A field may carry an optional `comment` describing what it is for. leech2
  ignores it. It exists only to document fields in `config.json`, which has no
  comment syntax of its own.

```toml
[tables.products]
fields = [
    { name = "id",    type = "NUMBER", primary-key = true },
    { name = "name",  type = "TEXT" },
    { name = "price", type = "NUMBER" },
]

[tables.products.csv]
source = "products.csv"  # where to find the CSV (relative to work dir, or absolute)
header = true            # CSV has a header row (defaults to false)
```

| Type      | SQL literal    | Notes                                                                  |
| --------- | -------------- | ---------------------------------------------------------------------- |
| `TEXT`    | `'value'`      | Single quotes, escaped                                                 |
| `NUMBER`  | `42` / `3.14`  | Stored as `f64`; integers above 2^53 lose precision                    |
| `BOOLEAN` | `TRUE`/`FALSE` | Accepts the exact strings `true` / `false` (case-sensitive); see below |

The `[csv]` block can declare per-table regex sentinels. `null` maps matching
cell values to SQL `NULL`. `true` / `false` override the strings recognized as
boolean true/false (only meaningful for BOOLEAN fields; ignored elsewhere).
Patterns are unanchored — use `^...$` for exact matches. A primary-key cell
matching the `null` pattern is rejected at load time.

When a `true` or `false` regex is set, the strict defaults (`"true"` /
`"false"`) are no longer accepted unless the regex matches them. Setting just
one of the two leaves the other on its default literal.

```toml
[tables.flags]
fields = [
    { name = "id",     type = "NUMBER",  primary-key = true },
    { name = "notes",  type = "TEXT" },
    { name = "active", type = "BOOLEAN" },
]

[tables.flags.csv]
source = "flags.csv"
null  = "^(N/A)?$"   # empty string OR "N/A" -> NULL
true  = "^Y$"
false = "^N$"
```

### Injected fields

Optional `[[injected-fields]]` entries add static columns to all generated SQL.
Each entry becomes an extra column in INSERT statements and an extra condition
in DELETE/UPDATE WHERE clauses. When any injected fields are configured, state
payload patches use `DELETE FROM ... WHERE ...` instead of `TRUNCATE` so that
other agents' data is preserved.

```toml
[[injected-fields]]
name = "host"      # column name in the target database
type = "TEXT"      # SQL type (default: TEXT)
value = "agent-1"  # the static value

[[injected-fields]]
name = "environment"
type = "TEXT"
value = "production"
```

Fields can also be injected at runtime via `lch patch inject` or the
`lch_patch_inject` C API function. Runtime injection is useful when the
authoritative value is only known to the receiver (e.g. a hub that derives it
from an authenticated connection); values provided at runtime overwrite any
statically declared field with the same name.

```sh
lch patch inject hostkey abc123  # defaults to TEXT
lch patch inject count 42 NUMBER
```

### Filters

The `[csv]` block can declare per-table filtering that drops records at CSV load
time. Filtered records never enter state, deltas, or SQL output.

```toml
[tables.users.csv]
source = "users.csv"
max-field-length = 1024

[tables.users.csv.filter]
fields  = ["status", "label"]
include = "^(active|pending)$"
exclude = '^DROP$'
```

- `max-field-length`: Optional. Any record where any field value exceeds this
  length in bytes is dropped.
- `csv.filter` is an optional single-block-per-table section with three keys:
  - `fields`: list of field names this filter examines. Every name must appear
    in the table's `fields`.
  - `include`: optional regex (whitelist). The record is kept only if at least
    one listed field matches the pattern.
  - `exclude`: optional regex (blacklist). The record is dropped if any listed
    field matches the pattern. Exclude is evaluated after include.

Both regexes follow the Rust [`regex`](https://docs.rs/regex/) crate and are
unanchored by default — use `^...$` for exact matches.

When a record that previously passed the filters stops passing, it appears as a
DELETE in the next delta. Similarly, when a previously-filtered record starts
passing, it appears as an INSERT.

### Compression

Patches are compressed with zstd by default. An optional `[compression]` section
controls this:

```toml
[compression]
enable = true  # enable zstd compression (default: true)
level = 3      # compression level (defaults to zstd default)
```

If compression would enlarge a small payload, the raw protobuf is sent instead;
the receiver auto-detects which form it received.

### Stats

An optional `[stats]` section makes each `patch create` append a run record to a
cumulative `STATS` JSON file in the state directory. Disabled by default:

```toml
[stats]
enable = true  # record stats (default: false)
```

Each entry stores performance related information about the different
compression stages. Run `lch stats show` to print an aggregated summary.

### History truncation

An optional `[truncate]` section controls automatic pruning of old block files
after every `lch_block_create()` / `lch block create`:

```toml
[truncate]
max-blocks = 100          # keep at most 100 blocks in the chain (>= 1)
max-age = "7d"            # remove blocks older than this duration
remove-orphans = true     # remove blocks not reachable from HEAD (default: true, recommended)
truncate-reported = true  # remove blocks older than last reported (default: true)
```

All fields are optional and independent.

By default, truncation removes orphaned blocks (i.e., on disk but not reachable
from HEAD), as well as blocks older than the last reported position (see
`lch_patch_applied`).

### File permissions

Files created in the work directory are given Unix permission bits taken from
the optional top-level `file-mode` option:

```toml
file-mode = "0600"  # owner read/write only (default)
```

The option is ignored on non-Unix platforms.

The state directory itself, when leech2 creates it, is given the permission bits
from the optional top-level `dir-mode` option:

```toml
dir-mode = "0700"  # owner read/write/traverse only (default)
```

## C API

See [`include/leech2.h`](include/leech2.h) for the full API reference.

The `.deb` and `.rpm` packages install a `leech2.pc` file, so consumers can
discover compile and link flags with `pkg-config --cflags --libs leech2`.

```c
lch_config_t *cfg = lch_init("/path/to/workdir");

lch_block_create(cfg, NULL);

lch_buffer_t patch = {0};
lch_patch_create(cfg, NULL, &patch);

char *sql;
lch_patch_to_sql(cfg, &patch, &sql);
printf("%s", sql);
lch_string_free(sql);

if (hub_send(patch.data, patch.len)) {
  lch_patch_applied(cfg, &patch);
} else {
  lch_patch_failed(cfg);
}
lch_buffer_free(&patch);

lch_deinit(cfg);
```

## Logging

**CLI:** Logs are written to stderr. Set the `LEECH2_LOG` environment variable
to control the log level (e.g. `LEECH2_LOG=debug`).

**FFI:** Call `lch_log_init()` first to receive log messages through a callback.
See [`include/leech2.h`](include/leech2.h) for the full API. Available levels:
`LCH_LOG_ERROR` (1), `LCH_LOG_WARN` (2), `LCH_LOG_INFO` (3),
`LCH_LOG_DEBUG` (4), `LCH_LOG_TRACE` (5). Trace messages are only emitted in
debug builds; release builds strip them at compile time.

## Man pages

Man pages are included in `.deb` and `.rpm` packages and in release tarballs.
After installing, run `man lch` or `man libleech2` to see them.

## Contributing

See [CONTRIBUTING.md](CONTRIBUTING.md) for architecture details, data flow, and
development guidelines.
