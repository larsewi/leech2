# Round-Trip Property Test Plan

## Motivation

The parent/child swap fixed in #153 went unnoticed for two months because
existing tests only assert _shape_ of the SQL output (counts of `INSERT`,
`UPDATE`, `DELETE`) and tolerate the silent full-state fallback. A more
end-to-end check — apply the generated SQL to a real database and compare
the result to the source-of-truth — would catch this class of bug
regardless of which merge rule it trips, and would also catch syntactically
broken SQL (e.g. the empty-`SET` clause that surfaced this bug).

## Goals

1. Catch merge logic errors regardless of which rule they trip.
2. Verify generated SQL is syntactically valid and executable against
   PostgreSQL.
3. Verify generated SQL is _semantically_ correct: applying every patch
   produced by an agent leaves the hub's database matching the agent's
   CSV state row-for-row.
4. Catch silent full-state fallbacks where the test expects a delta path.

## Topology

Single machine, multiple work directories simulating distinct agents and
one hub:

```
┌────────────┐    patches    ┌─────────────────┐
│ agents/A   │ ────────────▶ │                 │
│  .leech2/  │               │  hub/           │
│  *.csv     │               │   psql apply    │
├────────────┤               │   ▼             │
│ agents/B   │ ────────────▶ │  postgres:5432  │
│  ...       │               │                 │
├────────────┤               │                 │
│ agents/C   │ ────────────▶ │                 │
└────────────┘               └─────────────────┘
```

Each agent has its own `tempdir` with `config.toml`, CSV sources, and
`.leech2/` state. Agents are independent — they do not see each other's
data. The hub maintains one Postgres schema per agent (or one shared
schema with an injected `host` field — both modes worth testing).

## Per-round workflow

For N rounds (e.g. N = 100):

1. **Mutate**: pick an agent, apply a random mutation to its CSVs
   (insert/update/delete a small set of rows; rare schema change).
2. **Block**: run `lch block create` in the agent's work dir.
3. **Ship**: with probability p (e.g. 0.7), the hub records the agent's
   current head as `last_known`, runs `lch patch create <last_known>` in
   the agent's work dir, and pipes the resulting SQL through `psql`.
4. **Verify** (after every ship and at the end):
   - `psql` exit code is zero (syntactic validity).
   - Hub table contents match the agent's current CSV state (semantic
     correctness).
   - No `WARN ... falling back to full state` log line appeared in the
     agent's output (unless this round was a deliberate schema change).

## Mutation generator

Random walk over operation kinds, weighted:

| Op                     | Weight | Notes                                    |
| ---------------------- | ------ | ---------------------------------------- |
| Insert fresh PK        | 4      | new id, random subsidiary values         |
| Update random subset   | 4      | pick existing PK, randomize 1+ columns   |
| Delete random PK       | 2      | drop existing PK                         |
| No-op (touch CSV only) | 1      | exercises "no change" path               |
| Schema change          | rare   | add or remove a subsidiary column        |

Generation is seeded; the seed is printed on failure for reproducibility.

## Multi-agent without multi-master

leech2 is single-master per agent — agents do not merge each other's
changes. To run several agents on one hub without conflicts, two modes:

- **Per-agent schema**: each agent's tables live in a Postgres schema
  named after the agent. Cleanest isolation; verifies the SQL works in
  multiple schemas.
- **Shared schema with `host` injection**: each agent injects its own
  `host` field; rows are scoped by host. Verifies the injected-field
  flow end-to-end.

Phase 1 should pick one (per-agent schema is simpler); the other mode
can come later.

## Phasing

1. **Phase 1**: single-agent, fixed seed, no schema changes. Reproduces
   #153 and establishes the harness.
2. **Phase 2**: random mutations, seeded; still single-agent. Adds
   schema changes to exercise the layout-fallback path deliberately.
3. **Phase 3**: multiple agents (per-agent schema mode).
4. **Phase 4**: integrate into CI and gate merges on it.

## Implementation sketch

A new integration test, `tests/round_trip.rs`, behind `#[ignore]` so dev
runs do not require Postgres. CI runs with `--include-ignored`.

```rust
#[test]
#[ignore = "requires PGHOST; run via CI or `cargo test -- --include-ignored`"]
fn round_trip_single_agent() {
    let pg = pg_connect_from_env();           // skips if PGHOST unset
    let seed = env_seed_or_random();
    let mut rng = rng_from_seed(seed);

    let agent = AgentSim::new(&tempdir(), config_with_random_tables(&mut rng));
    let hub = HubSim::new(pg, agent.schema_name());
    hub.bootstrap(&agent.config());

    let mut last_known = GENESIS_HASH.to_string();
    for round in 0..100 {
        agent.mutate(&mut rng);
        agent.create_block();

        if rng.gen_bool(0.7) {
            let sql = agent.create_patch_sql(&last_known);
            hub.apply(&sql).unwrap_or_else(|e| panic!("seed={seed} round={round}: {e}"));
            hub.assert_matches(agent.csv_state())
                .unwrap_or_else(|e| panic!("seed={seed} round={round}: {e}"));
            last_known = agent.head();
        }
    }
}
```

Helpers (rough shape):

- `AgentSim`
  - `new(dir, config)` — write `config.toml`, write empty CSVs, init `.leech2`.
  - `mutate(rng)` — apply random ops to in-memory model, rewrite CSVs.
  - `create_block()` — invoke `Block::create` (library API).
  - `create_patch_sql(last_known)` — `Patch::create` + `sql::patch_to_sql`.
  - `csv_state()` — return current logical row sets per table.

- `HubSim`
  - `new(pg, schema)` — create the schema.
  - `bootstrap(config)` — `CREATE TABLE` per table from config.
  - `apply(sql)` — pipe through `psql --set=ON_ERROR_STOP=1`; surface
    Postgres errors verbatim.
  - `assert_matches(expected)` — `SELECT *` per table, compare to
    expected row sets.

`HubSim::apply` shells out to `psql` via the `PGHOST/PGUSER/PGPASSWORD`
env vars set by the GitHub Action. No new Rust dependency on a
Postgres driver is required — the SQL is the contract being tested,
and `psql` is the reference parser.

## CI integration

`.github/workflows/round-trip.yml`:

```yaml
name: round-trip
on:
  pull_request:
  push:
    branches: [master]
  workflow_dispatch:
    inputs:
      seed:
        description: 'Seed for the mutation generator (leave blank for random)'
        required: false
        type: string

jobs:
  round-trip:
    runs-on: ubuntu-latest
    services:
      postgres:
        image: postgres:16
        env:
          POSTGRES_USER: leech2
          POSTGRES_PASSWORD: leech2
          POSTGRES_DB: leech2
        ports: ['5432:5432']
        options: >-
          --health-cmd pg_isready
          --health-interval 5s
          --health-timeout 5s
          --health-retries 10
    env:
      PGHOST: localhost
      PGUSER: leech2
      PGPASSWORD: leech2
      PGDATABASE: leech2
      ROUND_TRIP_SEED: ${{ inputs.seed }}
    steps:
      - uses: actions/checkout@v4
      - run: sudo apt-get update && sudo apt-get install -y protobuf-compiler postgresql-client
      - run: cargo build --release
      - run: cargo test --release --test round_trip -- --include-ignored --nocapture
```

Per the project preference for built-in tooling over third-party
actions, this workflow uses only `actions/checkout`, the runner's
package manager, and the official Postgres service container.

## Failure-mode mapping

| Bug class                       | Caught by                                   |
| ------------------------------- | ------------------------------------------- |
| Swapped parent/child (#153)     | row mismatch after apply                    |
| Empty SET clause (#154)         | `psql` syntax error                         |
| Missing column in INSERT        | `psql` constraint error                     |
| Wrong PK in WHERE               | row mismatch after apply                    |
| Incorrect rule 15 column merge  | row mismatch after apply                    |
| Silent full-state fallback      | log scan (with explicit allow-list)         |
| Layout-change handling          | exercised by phase-2 schema mutations       |
| Injected-field misuse           | row mismatch in shared-schema mode          |

## Open questions

- Should we use `proptest` or `quickcheck` for shrinking? Manual seeding
  is simpler for phase 1; shrinking pays off when failures get hard
  to diagnose.
- How long should a CI run be? 100 rounds × 3 agents finishes in
  seconds; we can scale up once stable.
- Should we also assert wire round-trip equality (encode → decode →
  re-encode equals encode) inside the loop? Currently asserted by
  `common::assert_wire_roundtrip` in unit-style tests; cheap to repeat.
- Do we want a separate "stress" mode (10 000 rounds, nightly) versus
  the per-PR fast mode?
