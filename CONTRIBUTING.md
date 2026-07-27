# Contributing to leech2

## Building & testing

See [README.md](README.md) for build dependencies and basic build commands.

To run a single test: `cargo test <test_name>` (e.g. `cargo test
test_merge_rule5`). Prefix with `LEECH2_LOG=<level>` and suffix with `--
--nocapture` to enable logging (`error`, `warn`, `info`, `debug`, `trace`).

## Man pages

Man pages are generated, not hand-written, so they never drift from the CLI or
the C API:

- The CLI man pages are rendered from the clap command definition in
  `src/cli.rs`.
- The C API man pages come from the doc comments in `include/leech2.h`, via
  doxygen (configured by `Doxyfile`).

Generation lives in the release-only `xtask` crate, so doxygen and `clap_mangen`
stay out of the everyday `cargo build`. The release workflow runs it
automatically. To regenerate locally you need [doxygen](https://www.doxygen.nl/)
installed, then:

```sh
cargo xtask generate-man-pages target/release/man
```

## Tooling (`xtask`)

Repo automation lives in the `xtask` crate rather than in `build.rs` or ad-hoc
scripts. It is a separate workspace member kept out of `default-members`, so
plain `cargo build`, `cargo test`, and `cargo clippy` never build it and its
dependencies (such as `clap_mangen`) stay out of the shipped `leech2` dependency
tree. It runs only when invoked explicitly, which is why release-only work like
man-page generation belongs here.

Run a task through the cargo alias defined in `.cargo/config.toml`:

```sh
cargo xtask <task> [args...]
```

Arguments are parsed with clap, so `cargo xtask --help` lists the tasks and
`cargo xtask <task> --help` documents one.

The only task today is `generate-man-pages` (see [Man pages](#man-pages)). To
add another, add a variant to the `Task` enum in `xtask/src/main.rs` and a match
arm in `main`. Tasks can reuse code from the `lch` binary by including a source
file directly, as the man-page task does with `src/cli.rs` (`#[path =
"../../src/cli.rs"]`).

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
`Block::create()`, `Patch::create()`, `patch_to_sql()`, `Patch::applied()`, and
`Patch::failed()`.

### Block::create()

`Block::create()` captures changes by comparing the current table state against
the previous state stored on disk. The library loads each table into a hash map
keyed by composite primary key, then computes a delta against the previous
snapshot. Each delta records three operation types: inserts (new keys), deletes
(removed keys), and updates (changed values).

Tables can be sourced two ways: from a CSV file declared via a `[tables.X.csv]`
block in the config, or from a caller-supplied callback bundle when no `[csv]`
block is present. Both paths produce the same in-memory table representation;
everything downstream — delta computation, layout change detection, block
storage, truncation — is identical. Sentinels (`null` / `true` / `false`) and
`filter` rules apply only on the CSV backed tables. Callback-backed tables skip
rows by returning `LCH_SKIP_RECORD`.

The callback bundle has four hooks:
- `table_begin` (option): per-table setup
- `read_cell` (required): produces one typed cell per call
- `destroy_cell` (optional) invoked once after each successful `read_cell` so
  the implementation can free memory it allocated for the cell
- `table_end` (optional) per-table teardown

Initialize `lch_callbacks_t` with designated initializers (e.g. `.read_cell =
my_read_cell`) so optional fields added in later releases default to NULL
without breaking the initializer.

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
register as a layout change. However, adding, removing, or renaming a field
does. Tables whose layout changed are recorded in the block as a `TableChange`
with no delta (`delta: None`), signaling that patch consolidation should use a
full state snapshot for that table instead of attempting to merge incompatible
deltas.

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
hashes are collected by decoding each block file as a lightweight `BlockHeader`.
Then, blocks are loaded one at a time in oldest-first order and their deltas are
merged incrementally into per-table running results using 15 conflict-resolution
rules (see [DELTA_MERGING_RULES.md](DELTA_MERGING_RULES.md)). Each block is
dropped after its deltas are merged, so only one block's payload and the
per-table running results are in memory at a time. Some rules handle
non-conflicting scenarios seamlessly, while others detect unresolvable conflicts
(e.g. double insert).

When the reference hash is genesis or can't be resolved (e.g. the block chain
was truncated or corrupted), the library skips consolidation entirely and
produces a full state snapshot for all tables. This guarantees TRUNCATE + INSERT
SQL that is safe to apply regardless of what the target database currently
contains. The same fallback applies when the block chain is broken (e.g. a block
is missing).

During consolidation, tables whose blocks contain a `TableChange` with no delta
(indicating a layout change) go directly to full state without attempting to
merge. If merging fails for a single table (e.g. an unresolvable conflict), only
that table falls back to full state — other tables keep their consolidated
deltas.

After merging, each table's delta is optimized: deletes are stripped down to
keys only, and updates are sparse-encoded to include only changed columns. The
library then compares each table's consolidated delta encoded size against its
full state and picks whichever is smaller. This means a single patch can contain
a mix of delta tables and full state tables.

The hub validates each patch against its own config at SQL-generation time. The
wire's primary-key names and subsidiary-value names must match the hub's field
set. Furthermore, the wire's primary-key set must equal the hub's primary-key
set. Each cell's `Cell` variant is then checked against the hub's declared
`kind`. `NULL` is accepted on any non-primary-key field (primary-key cells with
the value `NULL` are rejected upstream at load time). Together these defend
against agents that misrepresent the schema or emit values of the wrong type.

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

`patch_to_sql()` converts an encoded patch into SQL statements. For delta tables
it generates `DELETE`, `INSERT`, and `UPDATE` statements. For full state tables
it generates `TRUNCATE` followed by `INSERT` statements. Column ordering follows
the wire's `Delta.fields`/`Table.fields` rather than the hub config's
declaration order, so values land in the columns the agent intended even if the
hub config declares the same fields in a different order. Schema disagreements
between the wire and the hub config are rejected before any SQL is emitted.

Column types defined in the config control how values are formatted in the SQL
output (quoting for `TEXT`, bare numbers for `NUMBER`, etc.).

When a patch carries injected fields (see `[[injected-fields]]` config section
in [README.md](README.md)), those columns are injected into all SQL output:
`INSERT` values include them, `DELETE`/`UPDATE`- `WHERE` clauses are scoped by
them, and state payloads use `DELETE FROM ... WHERE ...` instead of `TRUNCATE`
to preserve other agents data. Injected fields can also be added or overwritten
after the fact via `Patch::inject_field()` (and its CLI / C FFI counterparts),
which the receiving side of a connection can use to attach authoritative values
derived from the authenticated peer.

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
(`TRUNCATE` + `INSERT` for all tables). This is idempotent and safe regardless
of the current database state — the full state patch will bring the database to
the correct state even if a previous partial application left it inconsistent.

### Truncation

After every `Block::create()`, optional truncation runs to reclaim disk space.
It walks the chain using `Block::load_header()` (decoding only the parent hash
and timestamp, skipping the payload) to determine reachability and creation
timestamps, then removes orphaned blocks (not reachable from `HEAD`), blocks
older than the `REPORTED` position, and blocks exceeding configured `max-blocks`
or `max-age` limits.

Truncation runs on a background thread spawned after `Block::create()` advances
`HEAD`, so the call returns without waiting for it. Concurrent block creation
and truncation in the same work directory serialize on an exclusive lock on
`.chain.lock`.

### Recovery from missing files

Work directory files can go missing due to truncation, manual deletion, or disk
errors. The library is designed to always produce SQL that is safe to apply,
even when the block chain or metadata is incomplete.

See `tests/accept_recovery.rs` for acceptance tests covering these scenarios.

## Round-trip test

`tests/round_trip.rs` is an end-to-end property test that drives leech2 against
a real PostgreSQL instance. The acceptance tests under `tests/accept_*.rs`
verify SQL **shape** (counts of `INSERT` / `UPDATE` / `DELETE`); the round-trip
test additionally verifies SQL **semantics** by applying the generated SQL
through `psql` and asserting that the hub's row state matches the agent's
in-memory model after every ship.

The test is `#[ignore]`d so `cargo test` skips it locally. CI runs it via the
`Round-trip` workflow with a Postgres 16 service container. To run locally:

```sh
PGHOST=localhost PGUSER=leech2 PGPASSWORD=leech2 PGDATABASE=leech2 \
  cargo test --release --test round_trip -- --include-ignored --nocapture
```

The seed randomised. Override the seed with `ROUND_TRIP_SEED=<u64>` to reproduce
a specific failure; the workflow exposes the same input via `workflow_dispatch`.

## Source layout

```
src/
  lib.rs        C FFI entry points
  ffi.rs        Shared FFI plumbing (panic guard, arg checks, repr-C buffer/
                cell types, cell decode helper)
  callbacks.rs  Rust-side adapter for the lch_callbacks_t bundle used by
                callback-backed tables
  logger.rs     Callback-based log dispatch for FFI consumers
  main.rs       CLI (lch binary)
  cli.rs        clap CLI definition (shared with the xtask man-page generator)
  config.rs     TOML/JSON config parsing, drop-in fragment merging (include)
  table.rs      Table loading (CSV path + callback path) and the in-memory
                table type (HashMap<Vec<Cell>, Vec<Cell>>)
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
Doxyfile        Doxygen config for the libleech2 man pages (run via xtask)
xtask/          Release-only tooling; `cargo xtask generate-man-pages <dir>`
                renders lch.1 (+ a page per subcommand) from the clap CLI and
                the libleech2 pages from include/leech2.h via doxygen
tests/          Acceptance tests (`accept_*.rs`), the round-trip
                property test (`round_trip.rs`, gated on `PGHOST`),
                and the C FFI test (`test_c_ffi.rs` + `test_c_ffi.c`)
```

## Work directory layout

The work directory (`.leech2/` when using the CLI, or any path passed to
`lch_init()`) holds the config and CSV inputs:

| File                 | Description                                                                       |
| -------------------- | --------------------------------------------------------------------------------- |
| `config.{toml,json}` | Table definitions and field schemas (may pull in drop-in fragments via `include`) |
| CSV sources          | Referenced by each table's `source` field (relative to the work dir, or absolute) |

State files live in a separate state directory, by default a `state`
subdirectory of the work directory (configurable via the `state-dir` config
option):

| File       | Description                                                          |
| ---------- | -------------------------------------------------------------------- |
| `HEAD`     | Current block hash (40-character hex string)                         |
| `REPORTED` | Hash of last successfully reported patch head (used by truncation)   |
| `STATE`    | Protobuf-encoded snapshot of all tables                              |
| `PATCH`    | Last generated patch (CLI only)                                      |
| `STATS`    | Cumulative JSON patch-creation stats (opt-in via `[stats]`)          |
| `<sha1>`   | Protobuf-encoded block files, named by their hash                    |
| `*.lock`   | Lock files for inter-process synchronization (created automatically) |
| `*.tmp`    | Temporary files used during atomic writes (should not persist)       |

leech2 creates the state directory on demand, with permission bits from the
`dir-mode` config option (default `0700`).

## Protobuf

Proto definitions are in `proto/`. Code is generated at build time via
`prost-build` (`build.rs`) into `OUT_DIR` and included via `src/proto.rs`.
Domain types have `From` impls to convert to/from their proto counterparts. All
protobuf types implement `Display`, so you can print them directly to inspect
their contents (e.g. `println!("{}", block)`, `println!("{}", patch)`).

Each table cell on the wire is a `proto::cell::Cell` — a oneof of `null` /
`text` / `boolean` / `number` (`f64`). The type travels with the data via the
oneof tag, so the receiver doesn't have to re-parse any strings to know the
type.

## Delta merging rules

The 15 merge rules in `src/delta.rs` are fully specified in
[DELTA_MERGING_RULES.md](DELTA_MERGING_RULES.md). When modifying merge logic,
refer to that document and ensure all rule tests pass.
