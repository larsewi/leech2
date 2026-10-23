# leech2

<p align="center">
  <img src="logo.svg" alt="Leech Logo" width="100"/>
</p>

leech2 tracks changes to tables. It computes deltas between table snapshots,
stores them as linked blocks, and can produce consolidated patches that convert
into SQL statements for replaying changes on a target database.

leech2 ships as both a Rust library with a C-compatible FFI (`libleech2.so`) and
a CLI tool (`lch`).

## Installing

```sh
# On macOS
brew install larsewi/tap/leech2
```

On Linux and Windows, grab a package from the
[releases](https://github.com/larsewi/leech2/releases): a `.deb` or `.rpm` for
Linux, an `.msi` for Windows. Each release also carries a plain tarball or zip
per platform. Nothing below is needed to install a package; the build
dependencies only matter if you build from source.

### Upgrading

When upgrading, upgrade the hub before its agents. A hub reads patches from
older agents, but an older hub may not read patches from newer agents.

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
currently expects this file to be inside a `.leech2/` directory in the current
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
- By default, a patch carries a table as a full state instead of a delta
  whenever the full state is smaller. Set `use-full-state-if-smaller = false`
  on a table to always send its delta. Full state is then only used when a delta
  cannot be produced (e.g. a new table, a layout change, or missing blocks).
- Set `change-timestamp = "<column>"` on a table to record when each record last
  changed. Generated INSERT and UPDATE statements then write the creation time
  of the last block that changed the record into that column, as an ISO 8601
  UTC literal (e.g. `'2026-10-09T08:30:00Z'`). The column must not be a field or
  an injected field, and it must be nullable: records sent as part of a full
  state get `NULL`. INSERT and UPDATE also write `NULL` when the agent does not
  track changes (e.g. an older agent, or one without this option), replacing
  earlier timestamps. To keep timestamps across patches, consider also setting
  `use-full-state-if-smaller = false`. Removing the option leaves the column's
  existing values in place.

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
Patterns are unanchored -- use `^...$` for exact matches. A primary-key cell
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

### Events

Event streams are for data that is only ever added, such as audit logs. The
source keeps recent events and may prune old ones. The receiver keeps the full
history. Declare an event stream under `[events.X]`:

```toml
[events.alerts]
id-field = "eventid"  # column that receives each event's ID (required)
fields = [
    { name = "message",  type = "TEXT" },
    { name = "severity", type = "NUMBER" },
]
```

- Events are pulled through the callbacks of `lch_block_create()`, like
  callback-backed tables. The callback must only return events it has not
  returned before. Only mark events as returned once `lch_block_create()`
  succeeds, or they are lost.
- Each block stores the events supplied since the previous block. Events have
  no primary key and are never updated or deleted, so pruning the source has no
  effect on the receiver.
- A patch carries the events of the blocks it covers. The receiver only inserts
  them.
- Each event gets an ID: the hash of its block and its position in that block
  (e.g. `'3f2a...c1:17'`). The receiver skips an event whose ID it already has.
  Index `id-field` on the receiver, together with any injected fields.
- A patch without a known reference (genesis, or a reference removed by
  truncation) carries no events. The events of every stored block could make it
  too big. The receiver never gets the events of the blocks it missed.
- Events stay on the source until truncation removes their block. With the
  default `[truncate]` settings, that happens once they are reported.
  `max-blocks` and `max-age` can remove events that were never reported.
- A stream's fields can change while unreported blocks still hold events with
  the old fields. The hub skips fields it does not know, and leaves fields an
  event lacks at their column default. So add a new field on the hub first,
  and give its column a default, then add it on the agents.
- Retention on the receiver is up to you.
- Fields must not be primary keys. `id-field` must not be a field or an
  injected field. Event streams cannot be CSV-backed.

### Change logs

A change log is an event stream that records the changes to a table. Add a
`[tables.X.log]` section, and every insert, update and delete in a block
becomes an event in the stream named by `table`. The events are shipped and
written like those of any other event stream (see [Events](#events)). Tables
without the section are not affected. `table` and `id-field` are required.

```toml
[tables.users.log]
table           = "users_log"  # name of the log table (the event stream)
id-field        = "eventid"    # column that receives each event's ID
operation-field = "operation"  # column that receives the operation (default)
timestamp-field = "timestamp"  # column that receives the block's creation time (default)
exclude         = ["email"]    # fields the log table does not have (default: [])
operations      = { insert = "INSERT", update = "UPDATE", delete = "DELETE" } # (default)
```

- An event's fields are the table's fields minus `exclude`, then
  `operation-field` and `timestamp-field`.
- Inserts and updates record the values after the change. Deletes record them
  before the change. An update that only changes excluded fields is still
  recorded.
- Changes are recorded even when the hub gets the table as a full state. A
  block that stores a full state for the table (a new table, or a layout
  change) records nothing. Neither does the first block.
- Agents apply `exclude` and `operations`, so give agents and the hub the same
  `[tables.X.log]` section. Agents older than leech2 7.0.0 record nothing.
- `exclude` must name existing non-primary-key fields. `id-field`,
  `operation-field` and `timestamp-field` must not be fields or injected
  fields. Two tables cannot share a log table.

### Injected fields

Injected fields add an extra field to each table. They are useful when multiple
agents report to the same hub. They let the hub tell where the data came from,
and prevent one agent from touching another agent's data.

The hub passes them to `lch_patch_to_sql()` or `lch patch sql --inject`.

```sh
lch patch sql --inject hostkey abc123 TEXT --inject count 42 NUMBER
```

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

Files and directories created by leech2 are given Unix permission bits taken
from the optional top-level `file-mode` and `dir-mode` options:

```toml
file-mode = "0600"  # owner read/write only (default)
dir-mode = "0700"   # owner read/write/traverse only (default)
```

The options are ignored on non-Unix platforms.

## C API

See [`include/leech2.h`](include/leech2.h) for the full API reference.

The `.deb` and `.rpm` packages and the Homebrew formula install a `leech2.pc`
file, so consumers can discover compile and link flags with
`pkg-config --cflags --libs leech2`. The Linux and macOS tarballs ship it as a
`leech2.pc.in` template instead, since the install prefix is only known once you
unpack them. Substitute `@PREFIX@`, `@LIBDIR@` and `@VERSION@` to get a usable
`leech2.pc`.

```c
lch_config_t *cfg = lch_init("/path/to/workdir");

lch_block_create(cfg, NULL);

lch_buffer_t patch = {0};
lch_patch_create(cfg, NULL, &patch);

char *sql;
lch_patch_to_sql(cfg, &patch, NULL, 0, &sql);
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

Man pages are included in the `.deb` and `.rpm` packages and the Homebrew
formula, and under `man/` in the source tarball and the Linux and macOS
tarballs. After installing a package, run `man 1 lch` or `man 3 leech2.h` to see
them.

## Contributing

See [CONTRIBUTING.md](CONTRIBUTING.md) for architecture details, data flow, and
development guidelines.
