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
- The type field controls how values are quoted in generated SQL. These are not
  database column types — your database may use any compatible type (e.g.
  `INTEGER`, `FLOAT`, `TIMESTAMP`). It is your responsibility to ensure the
  quoted literals are valid for your target database type.

```toml
[tables.products]
source = "products.csv"  # relative to work dir, or absolute
header = true

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
|-----------|----------------|----------------------------------------------|
| `TEXT`    | `'value'`      | Single quotes, escaped                       |
| `NUMBER`  | `42` / `3.14`  | Validated as finite `f64`                    |
| `BOOLEAN` | `TRUE`/`FALSE` | Accepts `true/false`, `1/0`, `t/f`, `yes/no` |

Fields can have an optional `null` attribute that specifies which CSV value
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
