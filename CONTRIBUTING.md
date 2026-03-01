# Contributing to leech2

## Building & testing

See [README.md](README.md) for build dependencies and basic build commands.

To run a single test: `cargo test <test_name>` (e.g. `cargo test test_merge_rule5`).

## Core architecture

leech2 is a Rust `cdylib` that exposes a C-compatible API for tracking changes
to CSV-backed database tables. It implements a git-like content-addressable
block chain for change history. Changes flow through four primary operations:
`Block::create()`, `Patch::create()`, `patch_to_sql()`, and
`Patch::applied()`.

### Block::create()

`Block::create()` captures changes by comparing the current CSV table state
against the previous state stored on disk. The library loads each CSV file into
a hash map keyed by composite primary key, then computes a delta against the
previous snapshot. Each delta records three operation types: inserts (new keys),
deletes (removed keys), and updates (changed values). All deltas are bundled
into a block together with a parent hash and a timestamp, SHA-1 hashed, and
stored as a file named by its hash. The `HEAD` pointer is then advanced to point
at the new block.

Printing the block shows its structure:

```
Block:
  Parent: 7a3f1b2e...
  Created: 2025-06-15 08:30:00 UTC
  Payload (1 deltas):
    'employees' [employee_id, first_name, hire_date]
      Inserts (1):
        (3) Charlie, 2025-06-15
      Updates (1):
        (1) _, Alice -> Alicia, _
```

### Patch::create()

`Patch::create()` consolidates multiple blocks into a single patch by walking
the chain from `HEAD` back to a last-known hash (typically the hash stored in
`REPORTED`, or genesis on first run). At each step it merges the current block
with its parent using 15 conflict-resolution rules (see
[DELTA_MERGING_RULES.md](DELTA_MERGING_RULES.md)). Some rules handle
non-conflicting scenarios seamlessly, while others detect unresolvable conflicts
(e.g. double insert). If consolidation fails, the library falls back to sending
a full state snapshot.

After merging, the patch is optimized: deletes are stripped down to keys only,
and updates are sparse-encoded to include only changed columns. The library then
compares the encoded size of the consolidated deltas against the full state and
picks whichever is smaller.

Printing the patch shows either deltas or full state:

```
Patch:
  Head: 9c4d2e8f...
  Created: 2025-06-15 08:30:00 UTC
  Blocks: 3
  Payload (1 deltas):
    'employees' [employee_id, first_name, hire_date]
      Inserts (1):
        (3) Charlie, 2025-06-15
      Deletes (1):
        (2) _, _, _
      Updates (1):
        (1) _, Alice -> Alicia, _
```

### patch_to_sql()

`patch_to_sql()` converts an encoded patch into SQL statements suitable for
replaying changes on a target database. For deltas payloads it generates a
transaction with `DELETE`, `INSERT`, and `UPDATE` statements (in that order).
For full-state payloads it generates `TRUNCATE` followed by `INSERT` statements.
Column types defined in the config control how values are formatted in the SQL
output (quoting for `TEXT`, bare numbers for `INTEGER`, etc.).

### Patch::applied()

`Patch::applied()` marks a patch as successfully applied by writing its head
hash to the `REPORTED` file. The next call to `Patch::create()` will start from
this hash instead of genesis, so only new changes are included. The `REPORTED`
hash also serves as a truncation boundary: blocks older than the last reported
position can be safely pruned.

### Truncation

After every `Block::create()`, optional truncation runs to reclaim disk space.
It removes orphaned blocks (not reachable from `HEAD`), blocks older than the
`REPORTED` position, and blocks exceeding configured `max-blocks` or `max-age`
limits.

## Source layout

```
src/
  lib.rs        C FFI entry points
  main.rs       CLI (lch binary)
  config.rs     TOML/JSON config parsing
  table.rs      CSV loading, in-memory table (HashMap<pk, values>)
  state.rs      Snapshot of all tables, protobuf persistence
  entry.rs      Entry type (primary key + value) Display impl
  update.rs     Update type (key, changed indices, old/new values) Display impl
  delta.rs      Diff computation + merge logic (see DELTA_MERGING_RULES.md)
  block.rs      Content-addressable block creation and loading
  patch.rs      Patch consolidation, payload selection
  head.rs       HEAD file read/write
  reported.rs   REPORTED file read/write (last reported patch hash)
  truncate.rs   History truncation (orphan, reported, max-blocks, max-age)
  storage.rs    File I/O with fs2 locking
  wire.rs       Protobuf encode/decode + zstd compression
  sql.rs        Patch-to-SQL conversion with type mapping
  proto.rs      Generated protobuf code (via build.rs)
  utils.rs      SHA-1 hashing, timestamp formatting

proto/          Protobuf definitions (compiled at build time by prost-build)
include/        C header (leech2.h)
tests/          Acceptance tests
```

## Core data model

- **Config** (`src/config.rs`) -- TOML/JSON config defining tables, their CSV source files, field names, and primary keys. Returned by `Config::load()` and passed by reference to functions that need it.
- **Table** (`src/table.rs`) -- In-memory representation of a CSV table. Records stored as `HashMap<Vec<String>, Vec<String>>` (primary key -> subsidiary columns). Fields are reordered so primary key columns come first.
- **State** (`src/state.rs`) -- Snapshot of all tables at a point in time. Serialized to protobuf and persisted as `STATE` file.
- **Delta** (`src/delta.rs`) -- Diff between two states for a single table: inserts, deletes, and updates. Contains the merge logic implementing 15 rules (see [DELTA_MERGING_RULES.md](DELTA_MERGING_RULES.md)).
- **Block** (`src/block.rs`) -- A content-addressable unit containing a timestamp, parent hash, and a list of deltas. Blocks form a linked chain. SHA-1 hashed and stored by hash.
- **Patch** (`src/patch.rs`) -- Consolidates multiple blocks from HEAD back to a `last_known` hash by merging deltas. Chooses between sending consolidated deltas or full state based on encoded size.
- **Head** (`src/head.rs`) -- Reads/writes the `HEAD` file tracking the current block hash.
- **Storage** (`src/storage.rs`) -- File I/O with `fs2` file locking (exclusive for writes, shared for reads).

## Work directory layout

All leech2 state lives in a single directory (`.leech2/` when using the CLI,
or any path passed to `lch_init()`). It contains:

| File                           | Description                                                          |
|--------------------------------|----------------------------------------------------------------------|
| `config.toml` or `config.json` | Table definitions and field schemas                                  |
| `HEAD`                         | Current block hash (40-character hex string)                         |
| `REPORTED`                     | Hash of last successfully reported patch head (used by truncation)   |
| `STATE`                        | Protobuf-encoded snapshot of all tables                              |
| `PATCH`                        | Last generated patch (CLI only)                                      |
| `<sha1>`                       | Protobuf-encoded block files, named by their hash                    |
| `*.lock`                       | Lock files for inter-process synchronization (created automatically) |
| `*.tmp`                        | Temporary files used during atomic writes (should not persist)       |

CSV source files are referenced by the config's `source` field. The path is
resolved relative to the work directory but can also be an absolute path.

## Protobuf

Proto definitions are in `proto/`. Code is generated at build time via
`prost-build` (`build.rs`) into `OUT_DIR` and included via `src/proto.rs`.
Domain types have `From` impls to convert to/from their proto counterparts.
All protobuf types implement `Display`, so you can print them directly to
inspect their contents (e.g. `println!("{}", block)`, `println!("{}", patch)`).

**Note:** leech2 has not been released yet, so there are no
backwards-compatibility constraints on the proto specs. Reusing or renumbering
wire fields is fine.

## Data flow

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

## Delta merging rules

The 15 merge rules in `src/delta.rs` are fully specified in
[DELTA_MERGING_RULES.md](DELTA_MERGING_RULES.md). When modifying merge logic,
refer to that document and ensure all rule tests pass.
