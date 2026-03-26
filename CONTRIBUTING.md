# Contributing to leech2

## Building & testing

See [README.md](README.md) for build dependencies and basic build commands.

To run a single test: `cargo test <test_name>` (e.g. `cargo test test_merge_rule5`).

## Terminology

- **Agent** — an application that creates blocks and produces patches. Agents run
  on the data-source side, reading CSV files and building the block chain.
- **Hub** — an application that converts patches to SQL and applies them to the
  target database.
- **Genesis** — the zero hash representing the start of the chain before any
  blocks exist. When the reference point is genesis, a full state snapshot is
  produced for all tables.

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
deletes (removed keys), and updates (changed values).

When starting a fresh chain (HEAD is genesis), the block is stored with an empty
payload — delta computation and STATE file loading are skipped entirely. The
first block's deltas would never be used: a genesis reference always produces a
full state patch from the STATE file, and non-genesis references exclude the
first block from consolidation. This also avoids reading any stale STATE file
left over from a previous run.

Before computing deltas, the library detects field layout changes by comparing
each table's stored fields in the STATE file against the current config's
`ordered_field_names()`. Tables whose layout changed are recorded in the block
as a `TableChange` with no delta (`delta: None`), signaling that patch
consolidation should use a full state snapshot for that table instead of
attempting to merge incompatible deltas.

All table changes are bundled into a block together with a parent hash and a
timestamp, SHA-1 hashed, and stored as a file named by its hash. The `HEAD`
pointer is then advanced to point at the new block.

Printing the block shows its structure:

```
Block:
  Parent: 7a3f1b2e...
  Created: 2025-06-15 08:30:00 UTC
  Payload (2 tables):
    'employees' [employee_id, first_name, hire_date]
      Inserts (1):
        (3) Charlie, 2025-06-15
      Updates (1):
        (1) _, Alice -> Alicia, _
    'departments' <layout changed>
```

### Patch::create()

`Patch::create()` consolidates multiple blocks into a single patch by walking
the chain from `HEAD` back to a last-known hash (typically the hash stored in
`REPORTED`, or genesis on first run). To keep memory usage low, consolidation
proceeds in two phases: first, block hashes are collected by decoding each block
file as a lightweight `BlockHeader` (which shares field tags with `Block` so
prost skips the payload). Then, blocks are loaded one at a
time in oldest-first order and their deltas are merged incrementally into
per-table running results using 15 conflict-resolution rules (see
[DELTA_MERGING_RULES.md](DELTA_MERGING_RULES.md)). Each block is dropped after
its deltas are merged, so only one block's payload and the per-table running
results are in memory at a time. Some rules handle non-conflicting scenarios
seamlessly, while others detect unresolvable conflicts (e.g. double insert).

When the reference hash is genesis or can't be resolved (e.g. the block was
truncated), the library skips consolidation entirely and produces a full state
snapshot for all tables. This guarantees TRUNCATE + INSERT SQL that is safe to
apply regardless of what the target database currently contains. The same
fallback applies when the block chain is broken (e.g. a block is missing).

During consolidation, tables whose blocks contain a `TableChange` with no delta
(indicating a layout change) go directly to full state without attempting to
merge. If merging fails for a single table (e.g. an unresolvable conflict),
only that table falls back to full state — other tables keep their consolidated
deltas. After merging, each table's delta is optimized: deletes are stripped
down to keys only, and updates are sparse-encoded to include only changed
columns. The library then compares each table's consolidated delta encoded size
against its full state and picks whichever is smaller. This means a single patch
can contain a mix of delta tables and full-state tables.

Each patch also carries per-table field hashes computed from the config
(`field_hashes`). These allow the hub to validate that its config matches the
agent's before generating SQL.

Printing the patch shows any combination of deltas and states:

```
Patch:
  Head: 9c4d2e8f...
  Created: 2025-06-15 08:30:00 UTC
  Injected: host = agent-1
  Blocks: 3
  Deltas (1):
    'employees' [employee_id, first_name, hire_date]
      Inserts (1):
        (3) Charlie, 2025-06-15
      Deletes (1):
        (2) _, _, _
      Updates (1):
        (1) _, Alice -> Alicia, _
  States (1):
    'departments' [dept_id, dept_name]
      (HR) Human Resources
```

### patch_to_sql()

`patch_to_sql()` converts an encoded patch into SQL statements suitable for
replaying changes on a target database. Before generating SQL for each table,
it validates the table's field hash from the patch against the hub's config. If
the hashes don't match (or are missing), the table is skipped with a warning —
other tables are still processed. For delta tables it generates `DELETE`,
`INSERT`, and `UPDATE` statements. For full-state tables it generates `TRUNCATE`
followed by `INSERT` statements. A single patch may contain both delta and state
tables, and all statements are wrapped in a single transaction.
Column types defined in the config control how values are formatted in the SQL
output (quoting for `TEXT`, bare numbers for `NUMBER`, etc.).

When a patch carries injected fields (see `[[injected-fields]]` config section in
[README.md](README.md)), those columns are injected into all SQL output:
`INSERT` values include them, `DELETE`/`UPDATE` WHERE clauses are scoped by them,
and state payloads use `DELETE FROM ... WHERE ...` instead of `TRUNCATE` so that
other agents' data is preserved.

### Patch::applied()

`Patch::applied()` marks a patch as successfully applied by writing its head
hash to the `REPORTED` file. The next call to `Patch::create()` will start from
this hash instead of genesis, so only new changes are included. The `REPORTED`
hash also serves as a truncation boundary: blocks older than the last reported
position can be safely pruned.

### Truncation

After every `Block::create()`, optional truncation runs to reclaim disk space.
It walks the chain using `Block::load_header()` (decoding only the parent hash
and timestamp, skipping the payload) to determine reachability and creation
timestamps, then removes orphaned
blocks (not reachable from `HEAD`), blocks older than the `REPORTED` position,
and blocks exceeding configured `max-blocks` or `max-age` limits.

### Recovery from missing files

Work directory files can go missing due to truncation, manual deletion, or disk
errors. The library is designed to always produce SQL that is safe to apply,
even when the block chain or metadata is incomplete.

**Scenario/behavior:**
* **REPORTED block truncated:** `Patch::create` can't resolve the hash → falls
  back to full state (TRUNCATE + INSERT)
* **REPORTED file deleted:** CLI/FFI falls back to genesis → `Patch::create`
  produces full state
* **HEAD file deleted:** `head::load` returns genesis → empty patch. Next
  `Block::create` stores a block with an empty payload (stale STATE file is
  ignored), and the STATE file is overwritten with the current snapshot
* **Block chain broken:** (middle block deleted) | Delta consolidation fails →
  falls back to full state
* **STATE file deleted:** (chain intact) | Delta consolidation still succeeds
  via block chain; STATE is not needed

The key invariant: when the reference point is unknown or unreliable (genesis,
unresolvable hash, broken chain), the patch always uses **full state** for all
tables, which generates `TRUNCATE + INSERT` SQL. This avoids duplicate-key
violations that would occur if bare `INSERT` statements were applied to a
database that already contains rows. When injected fields are configured, state
tables use `DELETE FROM ... WHERE ...` instead of `TRUNCATE`, so only the matching
rows are replaced and other agents' data is preserved.

See `tests/accept_recovery.rs` for acceptance tests covering these scenarios.

## Source layout

```
src/
  lib.rs        C FFI entry points
  logger.rs     Callback-based log dispatch for FFI consumers
  main.rs       CLI (lch binary)
  config.rs     TOML/JSON config parsing
  table.rs      CSV loading, in-memory table (HashMap<pk, values>)
  state.rs      Snapshot of all tables, protobuf persistence
  entry.rs      Entry type (primary key + value) Display impl
  update.rs     Update type (key, changed indices, old/new values) Display impl
  delta.rs      Diff computation + merge logic (see DELTA_MERGING_RULES.md)
  block.rs      Content-addressable block creation and loading
  patch.rs      Patch consolidation, per-table payload selection
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

## Work directory layout

All leech2 state lives in a single directory (`.leech2/` when using the CLI,
or any path passed to `lch_init()`). It contains:

| File                 | Description                                                          |
|----------------------|----------------------------------------------------------------------|
| `config.{toml,json}` | Table definitions and field schemas                                  |
| `HEAD`               | Current block hash (40-character hex string)                         |
| `REPORTED`           | Hash of last successfully reported patch head (used by truncation)   |
| `STATE`              | Protobuf-encoded snapshot of all tables                              |
| `PATCH`              | Last generated patch (CLI only)                                      |
| `<sha1>`             | Protobuf-encoded block files, named by their hash                    |
| `*.lock`             | Lock files for inter-process synchronization (created automatically) |
| `*.tmp`              | Temporary files used during atomic writes (should not persist)       |

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
    +---> [if genesis: empty payload, skip to Block]
    |
    +---> detect_layout_changes(prev_state, config)
    |         |
    |         v
    |     Layout-changed tables marked as TableChange { delta: None }
    |
    +---> Delta::compute(prev_state, new_state)
    |         |
    |         v
    |     Normal tables wrapped as TableChange { delta: Some(...) }
    |
    +---> Block { parent, timestamp, payload: map<name, TableChange> }
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
Collect block hashes: HEAD -> ... -> last_known
    (decodes each block as BlockHeader, skipping payload)
    |
    v
Load blocks one at a time oldest-first, merging deltas incrementally
    |   (one block's payload + per-table running results in memory)
    |
    +-> Layout-changed tables -> full state directly (skip merge)
    |
    +-> Per table: Delta::merge() into running result (see DELTA_MERGING_RULES.md)
    |         (merge failure -> fall back to state for that table)
    v
Per table: strip (key-only deletes, sparse updates)
    |
    v
Per table: compare encoded sizes (delta vs state), pick smaller
    |
    v
Patch { head, created, num_blocks, deltas: {...}, states: {...}, field_hashes: {...} }
    |
    v
wire::encode_patch()  -->  protobuf + optional zstd
    |
    v
sql::patch_to_sql()
    |
    +--> Per table: check_field_hash() (skip+warn on mismatch)
    +--> Delta tables: DELETE...; INSERT...; UPDATE...;
    +--> State tables: TRUNCATE...; INSERT...;
    +--> All wrapped in: BEGIN; ... COMMIT;
    (with injected fields: DELETE/UPDATE WHERE scoped by them, state uses DELETE WHERE instead of TRUNCATE)
```

## Delta merging rules

The 15 merge rules in `src/delta.rs` are fully specified in
[DELTA_MERGING_RULES.md](DELTA_MERGING_RULES.md). When modifying merge logic,
refer to that document and ensure all rule tests pass.
