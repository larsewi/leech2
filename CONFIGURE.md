# Configuration

The configuration lives in either `config.{toml,json}`. The CLI tool currently
expects this file to be inside a `.leech2/` directory in the current working
directory (this may change). The C API does not care.

## Tables

- Each table must have at least one field marked `primary-key = true`
- Field names within a table must be unique
- A table is **CSV-backed** when it has a `[tables.X.csv]` block declaring a
  `source` (see [CSV parsing](#csv-parsing)); otherwise it is
  **callback-backed** and its rows are pulled from the FFI cell callback at
  block creation time (see [C API](README.md#c-api)).
- A field may carry an optional `comment` describing what it is for. leech2
  ignores it.

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

### Field types

The type field controls how values are quoted in generated SQL string. These are
not database column types. Your database may use any compatible type. It is your
responsibility to ensure the quoted literals are valid for your target database
type.

| Type      | SQL literal    | Notes                                                       |
| --------- | -------------- | ----------------------------------------------------------- |
| `TEXT`    | `'value'`      | Single quotes, escaped; NUL characters are rejected         |
| `NUMBER`  | `42` / `3.14`  | Stored as `f64`; integers above 2^53 lose precision         |
| `BOOLEAN` | `TRUE`/`FALSE` | Accepts the exact strings `true` / `false` (case-sensitive) |

### CSV parsing

Inside a `[csv]` block, when `header = false` (the default), CSV columns are
mapped to config fields by position.

When `header = true`, the first row of the CSV is treated as a header. Each
config field is matched to a CSV column by name. Hence, columns may appear in
any order. Every config field name must be present in the header; extra CSV
columns are ignored.

leech2 takes a shared advisory lock on each CSV source while reading it. To keep
leech2 from reading a half-written file, the producer should either replace the
file atomically (write a temporary file in the same directory, then rename it
into place), or hold an exclusive lock on it while rewriting it in place. leech2
waits for that lock before reading.

The `[csv]` block can declare per-table regex sentinels. `null` maps matching
cell values to SQL `NULL`. `true` / `false` override the strings recognized as
boolean true/false (only meaningful for BOOLEAN fields). Patterns are unanchored
(use `^...$` for exact matches).

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

### Full state or delta

By default, a patch carries a table as a full state instead of a delta whenever
the full state is smaller. Set `use-full-state-if-smaller = false` on a table to
always send its delta. Full state is then only used when a delta cannot be
produced (e.g. a new table, a layout change, or missing blocks).

```toml
[tables.products]
use-full-state-if-smaller = false  # always send a delta when possible (defaults to true)
fields = [
    { name = "id",    type = "NUMBER", primary-key = true },
    { name = "name",  type = "TEXT" },
    { name = "price", type = "NUMBER" },
]
```

### Change timestamps

Set `change-timestamp = "<column>"` on a table to record when each record last
changed. Generated INSERT and UPDATE statements then write the creation time of
the last block that changed the record into that column, as an ISO 8601 UTC
literal (e.g. `'2026-10-09T08:30:00Z'`).

The column must not be a field or an injected field. It must be nullable.
Records get `NULL` when sent as a full state, or when the agent does not track
change timestamps.

To avoid `NULL` timestamps from full states, consider disabling the full state
optimization with `use-full-state-if-smaller = false` (see [Full state or
delta](#full-state-or-delta)).

```toml
[tables.products]
change-timestamp = "updated_at"    # nullable column that receives the timestamp
use-full-state-if-smaller = false  # avoid full states, which write NULL
fields = [
    { name = "id",    type = "NUMBER", primary-key = true },
    { name = "name",  type = "TEXT" },
    { name = "price", type = "NUMBER" },
]
```

## Events

Event streams are for data that is only ever added, such as audit logs. Declare
an event stream under `[events.X]`:

```toml
[events.alerts]
id-field = "eventid"  # column that receives each event's ID (required)
fields = [
    { name = "message",  type = "TEXT" },
    { name = "severity", type = "NUMBER" },
]
```

- Events are pulled through the callbacks of `lch_block_create()` The callback
  must only return events it has not returned before. Only mark events as
  recorded once `lch_block_create()` succeeds.
- Each block stores the events supplied since the previous block. Events have
  no primary key and are never updated or deleted.
- A patch carries the events of the blocks it covers. The receiver only inserts
  them.
- Each event gets an ID: the hash of its block and its position in that block
  (e.g. `'3f2a...c1:17'`). The receiver skips an event whose ID it already has.
- A patch without a known reference carries no events, so the hub never gets
  the events of the blocks it missed.
- Events stay on the agent until their block is truncated (by default, once
  reported; see [History truncation](#history-truncation)).
- Retention on the receiver is up to you.
- Event streams cannot be CSV-backed.

## Change logs

A change log is an event stream that records the changes to a table. Add a
`[tables.X.log]` section, and every insert, update and delete in a block becomes
an event in the stream named by `table`. The events are shipped and written like
those of any other event stream (see [Events](#events)).

```toml
[tables.users.log]
table           = "users_log"  # name of the log table (the event stream)
id-field        = "eventid"    # column that receives each event's ID
operation-field = "operation"  # column that receives the operation (default)
timestamp-field = "timestamp"  # column that receives the block's creation time (default)
exclude         = ["email"]    # fields the log table does not have (default: [])
operations      = { insert = "INSERT", update = "UPDATE", delete = "DELETE" } # (default)
```

- A change log's record contains the table's fields minus `exclude`, then
  `operation-field` and `timestamp-field`.
- Inserts and updates record the values after the change. Deletes record them
  before the change.

## Compression

Patches are compressed with zstd by default. An optional `[compression]` section
controls this:

```toml
[compression]
enable = true  # enable zstd compression (default: true)
level = 3      # compression level (defaults to zstd default)
```

If compression would enlarge a small payload, the raw protobuf is sent instead;
the receiver auto-detects which form it received.

## Stats

An optional `[stats]` section makes each `patch create` append a run record to a
cumulative `STATS` JSON file in the state directory. Disabled by default:

```toml
[stats]
enable = true  # record stats (default: false)
```

Each entry stores performance related information about the different
compression stages. Run `lch stats show` to print an aggregated summary.

## History truncation

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

## File permissions

Files and directories created by leech2 are given Unix permission bits taken
from the optional top-level `file-mode` and `dir-mode` options:

```toml
file-mode = "0600"  # owner read/write only (default)
dir-mode = "0700"   # owner read/write/traverse only (default)
```

The options are ignored on non-Unix platforms.

## State directory

State files (`HEAD`, `STATE`, `REPORTED`, `PATCH`, and block files) live in a
`state` subdirectory next to the config (in the work directory) by default. The
optional top-level `state-dir` option allows you to point it elsewhere:

```toml
state-dir = "/var/lib/leech2"
```

You can either use an absolute path or path relative to the work directory.

## Drop-in fragments

The base config may pull in additional config files via a top-level `include`
key holding a list of glob patterns.

```toml
include = ["conf.d/*.toml", "conf.d/*.json"]
```

- Relative patterns resolve against the work directory; absolute patterns are
  used as-is. A pattern that matches nothing is not an error.
- Fragments use the same schema as the base config and may be `.toml` or `.json`
  regardless of the base file's format.
- Fragments are deep-merged in order: the base first, then each `include` the
  order they are listed.
- Merging is **last-wins** and recurses into sections.
- Only the base may declare `include` (nested includes are not supported).
