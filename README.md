# leech2

<p align="center">
  <img src="logo.svg" alt="Leech Logo" width="100"/>
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

Pass `--dry-run` to any create or mutate command (`block create`, `patch
create`, `patch inject`, `patch applied`, `patch failed`) to compute the change
and print what it `Would have ...` done without writing anything to disk:

```sh
lch block create --dry-run
lch patch create --dry-run
```

## Configuration

Config can be `config.toml` or `config.json`.

### State directory

State files (`HEAD`, `STATE`, `REPORTED`, the `PATCH` file, and block files)
live in a directory separate from the config and CSV inputs. By default this is
a `state` subdirectory of the work directory; the optional top-level `state-dir`
option points it elsewhere:

```toml
state-dir = "/var/lib/leech2"  # absolute path
# state-dir = "db"             # relative paths resolve against the work directory
```

leech2 creates the state directory on demand. CSV `source` paths and `include`
globs are unaffected -- they remain inputs resolved relative to the work
directory.

### Drop-in fragments

The base config may pull in additional config files via a top-level `include`
key holding a list of glob patterns. This lets a package that bundles leech2
ship a read-only base config while still letting users extend the reporting
system -- adding tables or injected fields -- by dropping fragment files into an
included directory, without editing the bundled file.

```toml
include = ["conf.d/*.toml", "conf.d/*.json"]
```

- Relative patterns resolve against the work directory; absolute patterns are
  used as-is. A pattern that matches nothing is not an error.
- Fragments use the same schema as the base config and may be `.toml` or `.json`
  regardless of the base file's format. Every section is optional, so a fragment
  can contribute just the tables (or injected fields) it adds.
- Fragments are deep-merged in order: the base first, then each `include`
  pattern in the order listed, with each pattern's matches sorted by filename.
- Merging is **last-wins** and recurses into sections: the `tables` map unions by
  table name, and sections like `[compression]` and `[truncate]` merge field by
  field, so a fragment can override just the keys it sets. Lists are replaced
  wholesale, so a later fragment that sets a table's `fields` or the
  `injected-fields` list overrides the earlier one entirely. A drop-in fragment
  can therefore override values from the bundled base config.
- A base `config.toml`/`config.json` is required, and only the base may declare
  `include`; a fragment that sets `include` is rejected (nested includes are not
  supported).

### Tables

- Each table must have at least one field marked `primary-key = true`
- Field names within a table must be unique
- A table is **CSV-backed** when it has a `[tables.X.csv]` block declaring a
  `source`; otherwise it is **callback-backed** and its rows are pulled from
  the FFI cell callback at block creation time.
- Inside a `[csv]` block, when `header = false` (the default), CSV columns are
  mapped to config fields by position — the first column maps to the first
  field, etc.
- When `header = true`, the first row of the CSV is treated as a header. Each
  config field is matched to a CSV column by name, so columns may appear in any
  order. Every config field name must be present in the header; extra CSV columns
  are ignored. In this mode, the order in which fields are declared under the
  table is cosmetic — reordering them does not invalidate existing state.
- The type field controls how values are quoted in generated SQL. These are not
  database column types — your database may use any compatible type (e.g.
  `INTEGER`, `FLOAT`, `TIMESTAMP`). It is your responsibility to ensure the
  quoted literals are valid for your target database type.
- A field may carry an optional `comment` describing what it is for. leech2
  ignores it; it exists to document fields in `config.json`, which has no
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
Each entry becomes an extra column in INSERT statements and an extra condition in
DELETE/UPDATE WHERE clauses. When any injected fields are configured,
state payload patches use `DELETE FROM ... WHERE ...` instead of `TRUNCATE` so
that other agents' data is preserved.

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

The `type` field accepts the same values as table field types (`TEXT`, `NUMBER`,
`BOOLEAN`).

Fields can also be injected at runtime via `lch patch inject` or the
`lch_patch_inject` C API. Runtime injection is useful when the authoritative
value is only known to the receiver (e.g. a hub that derives it from an
authenticated connection); values provided at runtime overwrite any
statically declared field with the same name.

```sh
lch patch inject hostkey abc123  # defaults to TEXT
lch patch inject count 42 NUMBER
```

### Filters

The `[csv]` block can declare per-table filtering that drops records at CSV
load time. Filtered records never enter state, deltas, or SQL output.

```toml
[tables.users.csv]
source = "users.csv"
max-field-length = 1024      # drop records with any field longer than this

[tables.users.csv.filter]
fields  = ["status", "label"]   # which fields the patterns are matched against
include = "^(active|pending)$"  # keep only records whose listed fields match
exclude = '^DROP$'              # then drop records whose listed fields match
```

- `max-field-length`: Optional. Any record where any field value exceeds this
  length in bytes (UTF-8 encoded) is dropped.
- `csv.filter` is an optional single-block-per-table section with three keys:
  - `fields`: list of field names this filter examines. Every name must appear
    in the table's `fields` (validated at config-load time).
  - `include`: optional regex (whitelist). The record is kept only if at least
    one listed field matches the pattern. Use `|` for alternation when several
    values should pass.
  - `exclude`: optional regex (blacklist). The record is dropped if any listed
    field matches the pattern. Exclude is evaluated after include, so on
    overlap exclude wins.
- Filters are per-table by structure — there's no cross-table filter scope.
  Callback-backed tables (no `[csv]` block) own their own row inclusion via
  `LCH_SKIP_RECORD`.

Both regexes follow the Rust [`regex`](https://docs.rs/regex/) crate and are
unanchored by default — use `^...$` for exact matches.

**Escaping regex patterns:** In JSON, backslashes in a regex must be
doubled: `"\\d+"` means `\d+`. In TOML, use single-quoted literal strings
to write regexes verbatim: `'\d+'`.

Filtering happens before state computation. When a record that previously
passed the filters stops passing, it appears as a DELETE in the next delta.
When a previously-filtered record starts passing, it appears as an INSERT.

### Compression

Patches are compressed with zstd by default. An optional `[compression]` section
controls this:

```toml
[compression]
enable = true  # enable zstd compression (default: true)
level = 3      # compression level (defaults to zstd default)
```

### Stats

An optional `[stats]` section makes each `patch create` append a run record to a
cumulative `STATS` JSON file in the state directory. Disabled by default:

```toml
[stats]
enable = true  # record stats (default: false)
```

Each entry stores the `duration_ms`, `bytes_before`, and `bytes_after` of the
delta-merging and compression stages.

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

All fields are optional and independent. Supported duration suffixes: `s`
(seconds), `m` (minutes), `h` (hours), `d` (days), `w` (weeks).

By default, truncation removes orphaned blocks (on disk but not reachable from
HEAD) and blocks older than the last reported position (see `lch_patch_applied`).
Set `remove-orphans = false` or `truncate-reported = false` to disable these
behaviors. Disabling orphan removal is not recommended — corrupt blocks are
detected during the chain walk and left unreachable so that orphan removal can
clean them up.

### File permissions

Files created in the work directory (`HEAD`, `STATE`, `REPORTED`, block files,
and their lock files) are given Unix permission bits taken from the optional
top-level `file-mode` option:

```toml
file-mode = "0600"  # owner read/write only (default)
```

The value is an octal string (an optional `0o` prefix is accepted) and must be
`<= 0o777`. It defaults to `"0600"`, so only the owner can read or write the
work directory's files. The option is ignored on non-Unix platforms.

The state directory itself, when leech2 creates it, is given the permission bits
from the optional top-level `dir-mode` option:

```toml
dir-mode = "0700"  # owner read/write/traverse only (default)
```

It follows the same octal-string rules as `file-mode` and defaults to `"0700"`.

## C API

See [`include/leech2.h`](include/leech2.h) for the full API reference.

The `.deb` and `.rpm` packages install a `leech2.pc` file, so consumers can
discover compile and link flags with `pkg-config --cflags --libs leech2`.

```c
lch_config_t *cfg = lch_init("/path/to/workdir");

/* Every table in the config has a `source` key, so no callback bundle is
 * needed -- pass NULL. See "Callback-backed tables" below for the case where
 * a table's rows come from the application instead of a CSV file. */
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
After installing, run `man lch` or `man libleech2` for full documentation.

## Contributing

See [CONTRIBUTING.md](CONTRIBUTING.md) for architecture details, data flow, and
development guidelines.
