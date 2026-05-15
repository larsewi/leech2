# Contributing to leech2

## Building & testing

See [README.md](README.md) for build dependencies and basic build commands.

To run a single test: `cargo test <test_name>` (e.g. `cargo test test_merge_rule5`).
Prefix with `LEECH2_LOG=<level>` to enable logging (`error`, `warn`, `info`,
`debug`, `trace`).

## Formatting

| File type  | Tool           | Command                  |
| ---------- | -------------- | ------------------------ |
| `.rs`      | `cargo fmt`    | `cargo fmt`              |
| `.c`, `.h` | `clang-format` | `clang-format -i <file>` |
| `.sh`      | `shfmt`        | `shfmt -w -i 4 <file>`   |

## Terminology

See [TERMINOLOGY.md](TERMINOLOGY.md) for the project's vocabulary.

## Core architecture

leech2 is a Rust `cdylib` that exposes a C-compatible API for tracking changes
to CSV-backed database tables. It implements a git-like content-addressable
block chain for change history. Changes flow through four primary operations:
`Block::create()`, `Patch::create()`, `patch_to_sql()`,
`Patch::applied()`, and `Patch::failed()`.

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
canonical field list (primary keys first, then subsidiaries; each group sorted
lexicographically by name).

Because tuple identity is canonical, reordering fields in `tables.toml` does not
register as a layout change. Adding, removing, or renaming a field does. Tables
whose layout changed are recorded in the block as a `TableChange` with no delta
(`delta: None`), signaling that patch consolidation should use a full state
snapshot for that table instead of attempting to merge incompatible deltas.

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
`REPORTED`, or genesis on first run). Callers may also pass an explicit hash to
bypass the built-in REPORTED mechanism (`lch_patch_applied` /
`lch_patch_failed`) and implement their own system for tracking which blocks
have been reported.

To keep memory usage low, consolidation proceeds in two phases: first, block
hashes are collected by decoding each block file as a lightweight `BlockHeader`
(which shares field tags with `Block` so prost skips the payload). Then, blocks
are loaded one at a time in oldest-first order and their deltas are merged
incrementally into per-table running results using 15 conflict-resolution rules
(see [DELTA_MERGING_RULES.md](DELTA_MERGING_RULES.md)). Each block is dropped
after its deltas are merged, so only one block's payload and the per-table
running results are in memory at a time. Some rules handle non-conflicting
scenarios seamlessly, while others detect unresolvable conflicts (e.g. double
insert).

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
can contain a mix of delta tables and full state tables.

The hub validates each patch against its own config at SQL-generation
time. `delta.fields`/`table.fields` (carried per-table on the wire) must
match the hub's field set in count and names, and the wire's primary-key
prefix must equal the hub's primary-key set. Each cell's `Cell` variant
is then checked against the hub's declared `sql_type`, and `NULL` is only
accepted on fields with a configured null sentinel. Together these
defend against agents that misrepresent the schema or emit values of the
wrong type. Sentinel strings themselves are agent-local CSV parsing rules
and never need to agree between agent and hub.

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
replaying changes on a target database. For delta tables it generates `DELETE`,
`INSERT`, and `UPDATE` statements. For full state tables it generates `TRUNCATE`
followed by `INSERT` statements. Column ordering follows the wire's
`Delta.fields`/`Table.fields` rather than the hub config's declaration
order, so values land in the columns the agent intended even if the hub
config declares the same fields in a different order. Schema disagreements
between the wire and the hub config (unknown field, mismatched PK, wrong
type, illegal NULL) are rejected before any SQL is emitted. A single patch may contain both delta and state
tables, and all statements are wrapped in a single transaction.
Column types defined in the config control how values are formatted in the SQL
output (quoting for `TEXT`, bare numbers for `NUMBER`, etc.).

When a patch carries injected fields (see `[[injected-fields]]` config section in
[README.md](README.md)), those columns are injected into all SQL output:
`INSERT` values include them, `DELETE`/`UPDATE` WHERE clauses are scoped by them,
and state payloads use `DELETE FROM ... WHERE ...` instead of `TRUNCATE` so that
other agents' data is preserved. Injected fields can also be added or
overwritten after the fact via `Patch::inject_field()` (and its CLI /
C FFI counterparts), which the receiving side of a connection can use to
attach authoritative values derived from the authenticated peer.

### Patch::applied()

`Patch::applied()` marks a patch as successfully applied by writing its head
hash to the `REPORTED` file. The next call to `Patch::create()` will start from
this hash instead of genesis, so only new changes are included. The `REPORTED`
hash also serves as a truncation boundary: blocks older than the last reported
position can be safely pruned.

### Patch::failed()

`Patch::failed()` handles the case where a patch could not be applied to the
target database. It removes the `REPORTED` file, which forces the next
`Patch::create()` to start from genesis and produce a full state patch
(TRUNCATE + INSERT for all tables). This is idempotent and safe regardless of
the current database state — the full state patch will bring the database to the
correct state even if a previous partial application left it inconsistent.

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

- **REPORTED block truncated:** `Patch::create` can't resolve the hash → falls
  back to full state (TRUNCATE + INSERT)
- **REPORTED file deleted:** CLI/FFI falls back to genesis → `Patch::create`
  produces full state
- **HEAD file deleted:** `head::load` returns genesis → empty patch. Next
  `Block::create` stores a block with an empty payload (stale STATE file is
  ignored), and the STATE file is overwritten with the current snapshot
- **Block chain broken:** (middle block deleted) | Delta consolidation fails →
  falls back to full state
- **STATE file deleted:** (chain intact) | Delta consolidation still succeeds
  via block chain; STATE is not needed

The key invariant: when the reference point is unknown or unreliable (genesis,
unresolvable hash, broken chain), the patch always uses **full state** for all
tables, which generates `TRUNCATE + INSERT` SQL. This avoids duplicate-key
violations that would occur if bare `INSERT` statements were applied to a
database that already contains rows. When injected fields are configured, state
tables use `DELETE FROM ... WHERE ...` instead of `TRUNCATE`, so only the matching
rows are replaced and other agents' data is preserved.

See `tests/accept_recovery.rs` for acceptance tests covering these scenarios.

## Round-trip test

`tests/round_trip.rs` is an end-to-end property test that drives leech2 against
a real PostgreSQL instance. The acceptance tests under `tests/accept_*.rs`
verify SQL **shape** (counts of `INSERT` / `UPDATE` / `DELETE`); the round-trip
test additionally verifies SQL **semantics** by applying the generated SQL
through `psql` and asserting that the hub's row state matches the agent's
in-memory model after every ship.

### Topology

Three simulated agents run in parallel, each driven by a seeded RNG. Each
ship is applied to two targets:

1. **Per-agent schema** — one Postgres schema per agent (`rt_<seed>_agent_a`,
   etc.), holding raw rows. The agent's patch is applied as-is.
2. **Shared hub schema** — `rt_<seed>_hub`, with a composite primary key
   `(host, id)`. The same patch is taken, `inject_field("host", agent_name,
   "TEXT")` is called on it, and the resulting SQL is applied. leech2 rewrites
   `INSERT` / `UPDATE` / `DELETE` / `TRUNCATE` to scope by `host`, so multiple
   agents writing through the same target schema do not trample each other.

After every ship both targets are queried and the row sets are compared to
the agent's model. The hub query filters by `host`.

### What it catches

- Merge logic errors that produce a wrong final state regardless of which
  rule misfires (caught by row mismatch).
- Syntactically invalid SQL (caught by `psql` exit code, gated by
  `--variable=ON_ERROR_STOP=1`).
- Bugs in the layout-fallback path: each agent toggles its `email` column on
  and off at two random rounds, so consolidations crossing those boundaries
  exercise full-state replays.
- Bugs in injected-field handling: the hub schema only verifies if `host`
  scoping in `INSERT` columns and `WHERE` clauses is correct end-to-end.

### Running it

The test is `#[ignore]`d so `cargo test` skips it locally. CI runs it via the
`Round-trip` workflow with a Postgres 16 service container. To run locally:

```sh
PGHOST=localhost PGUSER=leech2 PGPASSWORD=leech2 PGDATABASE=leech2 \
  cargo test --release --test round_trip -- --include-ignored --nocapture
```

The seed defaults to a fixed constant. Override with `ROUND_TRIP_SEED=<u64>` to
reproduce a specific failure; the workflow exposes the same input via
`workflow_dispatch`.

## Source layout

```
src/
  lib.rs        C FFI entry points
  logger.rs     Callback-based log dispatch for FFI consumers
  main.rs       CLI (lch binary)
  config.rs     TOML/JSON config parsing
  table.rs      CSV loading, in-memory table (HashMap<Vec<Cell>, Vec<Cell>>)
  state.rs      Snapshot of all tables, protobuf persistence
  cell.rs       Domain Cell type + conversions to/from proto::cell::Cell
  record.rs     Record type (Vec<Cell> key + value)
  update.rs     Update type (key, changed indices, old/new values)
  delta.rs      Diff computation + merge logic (see DELTA_MERGING_RULES.md)
  block.rs      Content-addressable block creation and loading
  patch.rs      Patch consolidation, per-table payload selection
  head.rs       HEAD file read/write
  reported.rs   REPORTED file read/write/remove (last reported patch hash)
  truncate.rs   History truncation (orphan, reported, max-blocks, max-age)
  storage.rs    File I/O with advisory locking
  wire.rs       Protobuf encode/decode + zstd compression
  sql.rs        Patch-to-SQL conversion (consumes typed Values directly)
  proto.rs      Generated protobuf code (via build.rs)
  utils.rs      SHA-1 hashing, timestamp formatting

proto/          Protobuf definitions (compiled at build time by prost-build)
include/        C header (leech2.h)
leech2.pc.in    pkg-config template (version and libdir filled in by build.rs)
man/            Man page templates (*.in, version and date filled in by build.rs)
tests/          Acceptance tests (`accept_*.rs`), the round-trip
                property test (`round_trip.rs`, gated on `PGHOST`),
                and the C FFI test (`test_c_ffi.rs` + `test_c_ffi.c`)
```

## Work directory layout

All leech2 state lives in a single directory (`.leech2/` when using the CLI,
or any path passed to `lch_init()`). It contains:

| File                 | Description                                                          |
| -------------------- | -------------------------------------------------------------------- |
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

Each table cell on the wire is a `proto::cell::Cell` — a oneof of
`null` / `text` / `boolean` / `number` (`f64`). The type travels with the
data via the oneof tag, so the receiver doesn't re-parse strings to know
the type. CSV ingest produces a typed domain `Cell` per the config's
`SqlType`; SQL emission consumes those `Cell`s directly.

## Delta merging rules

The 15 merge rules in `src/delta.rs` are fully specified in
[DELTA_MERGING_RULES.md](DELTA_MERGING_RULES.md). When modifying merge logic,
refer to that document and ensure all rule tests pass.
