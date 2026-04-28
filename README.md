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

## Configuration

Config can be `config.toml` or `config.json`.

### Tables

- Each table must have at least one field marked `primary-key = true`
- Field names within a table must be unique
- When `header = true`, the first row of the CSV is treated as a header. Each
  config field is matched to a CSV column by name, so columns may appear in any
  order. Every config field name must be present in the header; extra CSV columns
  are ignored. When `header = false` (the default), CSV columns are mapped to
  config fields by position — the first column maps to the first field, etc.
- The type field controls how values are quoted in generated SQL. These are not
  database column types — your database may use any compatible type (e.g.
  `INTEGER`, `FLOAT`, `TIMESTAMP`). It is your responsibility to ensure the
  quoted literals are valid for your target database type.

```toml
[tables.products]
source = "products.csv"  # where to find the CSV (relative to work dir, or absolute)
header = true            # CSV has a header row (defaults to false)

[[tables.products.fields]]
name = "id"
type = "NUMBER"
primary-key = true

[[tables.products.fields]]
name = "name"
type = "TEXT"

[[tables.products.fields]]
name = "price"
type = "NUMBER"
```

| Type      | SQL literal    | Notes                                        |
| --------- | -------------- | -------------------------------------------- |
| `TEXT`    | `'value'`      | Single quotes, escaped                       |
| `NUMBER`  | `42` / `3.14`  | Validated as finite `f64`                    |
| `BOOLEAN` | `TRUE`/`FALSE` | Accepts `true/false`, `1/0`, `t/f`, `yes/no` |

Fields can have an optional `null` sentinel that specifies which CSV value
should be emitted as SQL `NULL` instead of a typed literal. This is not allowed
on primary-key fields.

```toml
[tables.example]
source = "example.csv"
fields = [
    { name = "id", type = "NUMBER", primary-key = true },
    { name = "notes", type = "TEXT", null = "" },       # empty string -> NULL
    { name = "score", type = "NUMBER", null = "N/A" },  # "N/A" -> NULL
]
```

### Injected fields

Optional `[[injected-fields]]` entries add static columns to all generated SQL.
Each entry becomes an extra column in INSERT statements and an extra condition in
DELETE/UPDATE WHERE clauses. When any injected fields are configured,
state-payload patches use `DELETE FROM ... WHERE ...` instead of `TRUNCATE` so
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

An optional `[filters]` section drops records at CSV load time. Filtered
records never enter state, deltas, or SQL output.

```toml
[filters]
max-field-length = 1024      # drop records where a field exceeds this length

[[filters.include]]
field = "status"             # only keep records whose status matches the regex
regex = "^(active|pending)$"

[[filters.exclude]]
field = "status"             # field name to check
regex = "^inactive$"         # records whose field matches the regex are dropped

[[filters.exclude]]
field = "description"
regex = "DEPRECATED"         # unanchored — matches any substring

[[filters.exclude]]
tables = ["staging_orders"]  # only apply to specific tables (default: all)
field = "region"
regex = "^test$"

[[filters.exclude]]
field = "serial"
regex = '^\d{6}$'            # TOML literal string — no escaping needed
```

- `max-field-length`: Optional. Any record where any field value exceeds this
  character length is dropped.
- `[[filters.include]]` (whitelist): Optional list of inclusion rules. Each
  rule specifies a `field` and a `regex`. When one or more include rules
  apply to a table, a record is kept only if its field value matches at
  least one applicable rule (rules combine with OR). When a table has no
  applicable include rules, every record passes the include check.
- `[[filters.exclude]]` (blacklist): Optional list of exclusion rules. Each
  rule specifies a `field` and a `regex`. Records whose field value matches
  the regex are dropped. Exclude is evaluated after include, so on overlap
  exclude wins.
- `tables`: Optional list of table names a rule applies to. When omitted, the
  rule applies to all tables. If the named field doesn't exist in a table, the
  rule is silently skipped.

Include and exclude regexes share the same syntax: patterns are unanchored
by default — use `^` and `$` for exact matches — and follow the Rust
[`regex`](https://docs.rs/regex/) crate.

**Escaping regex patterns:** In JSON, backslashes in a regex must be
doubled: `"\\d+"` means `\d+`. In TOML, use single-quoted literal strings
to write regexes verbatim: `'\d+'`.

Filtering happens before state computation. When a record that previously
passed the filters stops passing — by matching an exclude rule, or by no
longer matching the include rules — it appears as a DELETE in the next
delta. When a previously-filtered record starts passing, it appears as an
INSERT.

### Compression

Patches are compressed with zstd by default. An optional `[compression]` section
controls this:

```toml
[compression]
enable = true  # enable zstd compression (default: true)
level = 3      # compression level (defaults to zstd default)
```

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

## C API

See [`include/leech2.h`](include/leech2.h) for the full API reference.

The `.deb` and `.rpm` packages install a `leech2.pc` file, so consumers can
discover compile and link flags with `pkg-config --cflags --libs leech2`.

```c
lch_config_t *cfg = lch_init("/path/to/workdir");

lch_block_create(cfg);

uint8_t *buf;
size_t len;
lch_patch_create(cfg, NULL, &buf, &len);

char *sql;
lch_patch_to_sql(cfg, buf, len, &sql);
printf("%s", sql);
lch_sql_free(sql);

if (hub_send(buf, len)) {
  lch_patch_applied(cfg, buf, len);
} else {
  lch_patch_failed(cfg);
}
lch_patch_free(buf, len);

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
