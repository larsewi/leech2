//! Round-trip property test.
//!
//! Drives a single agent through a seeded sequence of mutations, ships
//! patches to a real Postgres instance via `psql`, and asserts that the
//! hub's row state matches the agent's CSV state after every ship.
//! Mutations include rare schema changes that exercise the
//! layout-fallback path.
//!
//! Gated on `PGHOST`. Locally the test no-ops; CI sets the env vars.

mod common;

use std::collections::BTreeMap;
use std::env;
use std::io::Write;
use std::path::{Path, PathBuf};
use std::process::{Command, Stdio};

use anyhow::{Context, Result, bail};
use leech2::block::Block;
use leech2::config::Config;
use leech2::patch::Patch;
use leech2::sql::{self, quote_identifier};
use leech2::utils::GENESIS_HASH;
use rand::rngs::StdRng;
use rand::seq::{IndexedRandom, IteratorRandom};
use rand::{Rng, SeedableRng};

const ROUNDS: usize = 50;
const MUTATIONS_PER_BLOCK_MAX: usize = 10;
const SHIP_PROBABILITY: f64 = 0.3;
const DEFAULT_SEED: u64 = 0xdead_beef_cafe_f00d;

/// CSV value treated as SQL NULL by the `email` field's `null` sentinel.
/// When the agent emits this string in the email column, leech2 maps it
/// to `Value::Null` and the resulting SQL writes `NULL` to the hub.
const EMAIL_NULL_SENTINEL: &str = "N/A";

/// The hub schema is the *superset* of every column the agent might ever
/// declare — leech2's generated INSERTs only mention currently-active
/// columns, so any column the agent has dropped just stays NULL on the hub.
const HUB_SUPERSET_SQL: &str = r#"
CREATE TABLE "users" (
    "id" DOUBLE PRECISION,
    "name" TEXT,
    "email" TEXT,
    "active" BOOLEAN,
    PRIMARY KEY ("id")
);
"#;

/// Build the agent's `config.toml` for a given schema state. `email` is the
/// only toggleable column for now; `id`, `name`, and `active` are always
/// present. The email field declares a `null` sentinel so the agent can
/// emit `EMAIL_NULL_SENTINEL` in the CSV to mean "no value".
fn config_toml(email_active: bool) -> String {
    let mut s = String::from(
        r#"[tables.users]
source = "users.csv"
fields = [
    { name = "id", type = "NUMBER", primary-key = true },
    { name = "name", type = "TEXT" },
"#,
    );
    if email_active {
        s.push_str(&format!(
            "    {{ name = \"email\", type = \"TEXT\", null = \"{EMAIL_NULL_SENTINEL}\" }},\n"
        ));
    }
    s.push_str("    { name = \"active\", type = \"BOOLEAN\" },\n");
    s.push_str("]\n");
    s
}

#[derive(Clone, Debug)]
struct Row {
    name: String,
    /// Either a real email or `EMAIL_NULL_SENTINEL` to mean "no value".
    email: String,
    active: bool,
}

/// One simulated agent: a tempdir with `config.toml` and `users.csv` plus an
/// in-memory mirror of what the CSV should contain. The mirror is the
/// source-of-truth that the hub's row state is checked against.
struct AgentSim {
    work_dir: PathBuf,
    // BTreeMap so CSV output and the expected-row list both walk keys in id
    // order, matching the hub's `ORDER BY id` query without an explicit sort.
    model: BTreeMap<i64, Row>,
    /// Whether the `email` column is currently part of the schema. Toggled
    /// by `MutationKind::SchemaChange` to exercise the layout-fallback path.
    email_active: bool,
}

impl AgentSim {
    /// Initialize the agent's work directory: write the initial config and
    /// an empty CSV so `Block::create` has something to read on the first
    /// round.
    fn new(work_dir: &Path) -> Result<Self> {
        let agent = Self {
            work_dir: work_dir.to_path_buf(),
            model: BTreeMap::new(),
            email_active: true,
        };
        agent.write_config()?;
        std::fs::write(work_dir.join("users.csv"), "").context("failed to write users.csv")?;
        Ok(agent)
    }

    /// Rewrite `config.toml` to reflect the current `email_active` flag.
    /// Called on initialization and on every schema-change mutation.
    fn write_config(&self) -> Result<()> {
        std::fs::write(
            self.work_dir.join("config.toml"),
            config_toml(self.email_active),
        )
        .context("failed to write config.toml")?;
        Ok(())
    }

    /// Serialize the in-memory model to `users.csv` so the next
    /// `Block::create` call observes the post-mutation state. The CSV
    /// column set tracks `email_active`; `active` is always present.
    fn write_csv(&self) -> Result<()> {
        let mut content = String::new();
        for (id, row) in &self.model {
            if self.email_active {
                content.push_str(&format!(
                    "{},{},{},{}\n",
                    id, row.name, row.email, row.active
                ));
            } else {
                content.push_str(&format!("{},{},{}\n", id, row.name, row.active));
            }
        }
        std::fs::write(self.work_dir.join("users.csv"), content)
            .context("failed to write users.csv")?;
        Ok(())
    }

    /// Apply one weighted random mutation (insert/update/delete/no-op) to the
    /// in-memory model. Update/delete on an empty model degrades to insert so
    /// every round produces a useful change when one is possible.
    fn mutate(&mut self, rng: &mut StdRng) {
        let mut kind = pick_mutation(rng);
        if self.model.is_empty() && matches!(kind, MutationKind::Update | MutationKind::Delete) {
            kind = MutationKind::Insert;
        }

        match kind {
            MutationKind::Insert => {
                if let Some(id) = self.fresh_id(rng) {
                    self.model.insert(
                        id,
                        Row {
                            name: random_name(rng),
                            email: random_email(rng),
                            active: rng.random_bool(0.5),
                        },
                    );
                }
            }
            MutationKind::Update => {
                let id = match self.model.keys().copied().choose(rng) {
                    Some(id) => id,
                    None => return,
                };
                let row = match self.model.get_mut(&id) {
                    Some(row) => row,
                    None => return,
                };
                let mut changed = false;
                if rng.random_bool(0.5) {
                    row.name = random_name(rng);
                    changed = true;
                }
                if rng.random_bool(0.5) {
                    row.email = random_email(rng);
                    changed = true;
                }
                if rng.random_bool(0.5) || !changed {
                    row.active = rng.random_bool(0.5);
                }
            }
            MutationKind::Delete => {
                if let Some(id) = self.model.keys().copied().choose(rng) {
                    self.model.remove(&id);
                }
            }
            MutationKind::NoOp => {}
        }
    }

    /// Toggle `email_active` and rewrite `config.toml`. When the column is
    /// being re-added, backfill fresh email values for every existing row
    /// so the next CSV write has something to put in the new column.
    /// Triggers leech2's layout-fallback path on the next consolidation
    /// that crosses this boundary.
    fn toggle_email_active(&mut self, rng: &mut StdRng) {
        self.email_active = !self.email_active;
        if self.email_active {
            for row in self.model.values_mut() {
                row.email = random_email(rng);
            }
        }
        self.write_config()
            .expect("rewrite config.toml after schema change");
        log::info!(
            "Schema change: email is now {}",
            if self.email_active {
                "active"
            } else {
                "inactive"
            }
        );
    }

    /// Pick an id from `1..1000` that is not currently in the model, or
    /// `None` if the id space is fully occupied. Sampling from the
    /// complement guarantees termination regardless of model size.
    fn fresh_id(&self, rng: &mut StdRng) -> Option<i64> {
        (1..1000)
            .filter(|id| !self.model.contains_key(id))
            .choose(rng)
    }
}

#[derive(Copy, Clone, PartialEq, Eq, Debug)]
enum MutationKind {
    Insert,
    Update,
    Delete,
    NoOp,
}

/// Mutation weights matching the plan's table. Inserts and updates dominate
/// so the model grows and consecutive same-row changes are likely.
const MUTATION_WEIGHTS: &[(MutationKind, u32)] = &[
    (MutationKind::Insert, 4),
    (MutationKind::Update, 4),
    (MutationKind::Delete, 2),
    (MutationKind::NoOp, 1),
];

/// Sample a mutation kind from `MUTATION_WEIGHTS`.
fn pick_mutation(rng: &mut StdRng) -> MutationKind {
    MUTATION_WEIGHTS
        .choose_weighted(rng, |(_, weight)| *weight)
        .expect("MUTATION_WEIGHTS is non-empty")
        .0
}

/// Pick a name from a small alphabet. The pool is intentionally tiny so
/// repeated updates frequently land on the same value — this exercises the
/// "update with no real change" merge paths.
fn random_name(rng: &mut StdRng) -> String {
    const NAMES: &[&str] = &["alice", "bob", "carol", "dave", "eve", "frank"];
    NAMES
        .iter()
        .copied()
        .choose(rng)
        .expect("NAMES is non-empty")
        .to_string()
}

/// Pick an email from a small pool, for the same reason as `random_name`.
/// Occasionally emit the null sentinel instead of a real email so the
/// CSV exercises the leech2 null-sentinel path; leech2 maps it to
/// `Value::Null` and the resulting SQL writes NULL to the hub.
/// No commas or quotes appear in the output, keeping CSV comparisons literal.
fn random_email(rng: &mut StdRng) -> String {
    if rng.random_bool(0.2) {
        return EMAIL_NULL_SENTINEL.to_string();
    }
    const DOMAINS: &[&str] = &["example.com", "test.org", "leech2.dev"];
    let user = random_name(rng);
    let domain = DOMAINS
        .iter()
        .copied()
        .choose(rng)
        .expect("DOMAINS is non-empty");
    format!("{user}@{domain}")
}

/// One simulated hub: an isolated Postgres schema reached by shelling out to
/// `psql`. All SQL the agent produces is applied here, and the resulting row
/// state is queried back for comparison.
struct HubSim {
    schema: String,
}

impl HubSim {
    fn new(schema: String) -> Self {
        Self { schema }
    }

    /// Run a SQL script through `psql`. The connection's `search_path` is
    /// set to the per-run schema via `PGOPTIONS` (rather than an in-band
    /// `SET`) so the command tag from that statement doesn't appear in
    /// stdout and pollute CSV parsing. Bails on any non-zero exit,
    /// surfacing stderr and the offending SQL.
    fn psql(&self, sql: &str) -> Result<String> {
        let mut child = Command::new("psql")
            // Ignore local configs.
            .arg("--no-psqlrc")
            // Drop column-alignment padding.
            .arg("--no-align")
            // Strip the column header row and the "(N rows)" footer.
            .arg("--tuples-only")
            // Emit rows as RFC-4180 CSV so parsing is unambiguous.
            .arg("--csv")
            // Abort on the first SQL error and exit non-zero.
            .arg("--variable=ON_ERROR_STOP=1")
            // Resolve unqualified identifiers in leech2's generated SQL
            // into the per-run schema.
            .env("PGOPTIONS", format!("-c search_path={}", self.schema))
            .stdin(Stdio::piped())
            .stdout(Stdio::piped())
            .stderr(Stdio::piped())
            .spawn()
            .context("failed to spawn psql")?;

        let mut stdin = child.stdin.take().context("missing psql stdin")?;
        stdin.write_all(sql.as_bytes())?;
        drop(stdin);

        let output = child.wait_with_output().context("failed to wait on psql")?;
        if !output.status.success() {
            bail!(
                "psql failed (status={}):\nstderr: {}\nsql:\n{}",
                output.status,
                String::from_utf8_lossy(&output.stderr),
                sql,
            );
        }
        String::from_utf8(output.stdout).context("psql produced non-UTF-8 output")
    }

    /// Drop the per-run schema if a previous run left one behind, recreate
    /// it, and create the superset table. Run once at the start of the
    /// test. The table holds every column the agent might ever declare —
    /// see `HUB_SUPERSET_SQL` for why.
    fn bootstrap(&self) -> Result<()> {
        let sql = format!(
            "DROP SCHEMA IF EXISTS {schema} CASCADE;\nCREATE SCHEMA {schema};\n{ddl}",
            schema = quote_identifier(&self.schema),
            ddl = HUB_SUPERSET_SQL,
        );
        self.psql(&sql).context("bootstrap failed")?;
        Ok(())
    }

    /// Pipe a leech2-generated SQL patch through `psql`. Failures here mean
    /// the SQL is syntactically invalid or violates a constraint — both are
    /// bugs the test is designed to catch.
    fn apply(&self, sql: &str) -> Result<()> {
        self.psql(sql).map(|_| ())
    }

    /// Query every row in the hub and assert it equals the agent's model
    /// row-for-row. This is the semantic check that catches merge-logic bugs:
    /// syntactically valid SQL that produces the wrong final state still
    /// mismatches here.
    ///
    /// `active::text` casts the boolean to "true"/"false" (psql's default
    /// CSV format would render "t"/"f", which doesn't match what the agent
    /// wrote in the source CSV).
    ///
    /// When `email_active` is false, the hub's email column should be NULL
    /// for every row (the most recent ship was a TRUNCATE+INSERT that did
    /// not name the column). When the agent emitted `EMAIL_NULL_SENTINEL`
    /// for a particular row, leech2 wrote NULL for that cell. psql renders
    /// NULL as the empty string in CSV mode, so the expected row formats
    /// with an empty email field in both cases.
    fn assert_matches(&self, agent: &AgentSim) -> Result<()> {
        let csv =
            self.psql("SELECT id, name, email, active::text FROM \"users\" ORDER BY id;\n")?;
        let hub_rows: Vec<String> = csv.lines().map(|s| s.to_string()).collect();
        let want_rows: Vec<String> = agent
            .model
            .iter()
            .map(|(id, r)| {
                let email = if agent.email_active && r.email != EMAIL_NULL_SENTINEL {
                    r.email.as_str()
                } else {
                    ""
                };
                format!("{},{},{},{}", id, r.name, email, r.active)
            })
            .collect();
        if hub_rows != want_rows {
            bail!(
                "row mismatch:\n  hub:  {:#?}\n  want: {:#?}",
                hub_rows,
                want_rows
            );
        }
        Ok(())
    }

    /// Best-effort schema teardown. Skipped on panic; CI runs in a fresh
    /// container per job so leftover schemas don't accumulate.
    fn cleanup(&self) -> Result<()> {
        self.psql(&format!(
            "DROP SCHEMA IF EXISTS {} CASCADE;",
            quote_identifier(&self.schema),
        ))?;
        Ok(())
    }
}

/// Pick the RNG seed: parse `ROUND_TRIP_SEED` if it's set to a valid `u64`,
/// otherwise use `DEFAULT_SEED`. The CI workflow forwards its optional
/// `seed` input through this env var; an unset or blank value is treated
/// as "use the default". A non-empty but unparsable value panics so a
/// typo doesn't silently run a different seed than the user asked for.
fn read_seed() -> u64 {
    let Ok(raw) = env::var("ROUND_TRIP_SEED") else {
        return DEFAULT_SEED;
    };
    let trimmed = raw.trim();
    if trimmed.is_empty() {
        return DEFAULT_SEED;
    }
    trimmed
        .parse::<u64>()
        .unwrap_or_else(|e| panic!("ROUND_TRIP_SEED={raw:?} is not a valid u64: {e}"))
}

#[test]
#[ignore = "requires PGHOST; run via `cargo test -- --include-ignored`"]
fn round_trip_single_agent() {
    common::init_logging();

    if env::var("PGHOST").is_err() {
        eprintln!("round_trip: PGHOST not set, skipping");
        return;
    }

    let seed = read_seed();
    eprintln!("round_trip: seed = {seed}");
    let mut rng = StdRng::seed_from_u64(seed);

    let tmp = tempfile::tempdir().unwrap();
    let work_dir = tmp.path();
    let mut agent = AgentSim::new(work_dir).unwrap();

    let hub = HubSim::new(format!("rt_{seed}"));
    hub.bootstrap().unwrap();

    // Pick two distinct rounds for schema changes so the test exercises
    // both removing the column and re-adding it (since email starts
    // active, the first toggle removes and the second re-adds).
    let schema_change_rounds: std::collections::HashSet<usize> = (0..ROUNDS)
        .choose_multiple(&mut rng, 2)
        .into_iter()
        .collect();
    log::info!("Scheduled schema changes at rounds {schema_change_rounds:?}");

    let mut last_known = GENESIS_HASH.to_string();
    for round in 0..ROUNDS {
        if schema_change_rounds.contains(&round) {
            agent.toggle_email_active(&mut rng);
        }
        let mutations = rng.random_range(0..=MUTATIONS_PER_BLOCK_MAX);
        for _ in 0..mutations {
            agent.mutate(&mut rng);
        }
        // Reload Config after mutations so a schema-change mutation's new
        // config.toml is observed by Block::create and Patch::create.
        let config = Config::load(work_dir).unwrap();
        log::info!(
            "Round {}/{}: applied {} mutation(s), model has {} row(s)",
            round + 1,
            ROUNDS,
            mutations,
            agent.model.len(),
        );
        agent.write_csv().unwrap();
        let head = Block::create(&config).unwrap();

        let force_ship = round + 1 == ROUNDS;
        if !force_ship && !rng.random_bool(SHIP_PROBABILITY) {
            log::info!("Round {}/{}: not shipping this round", round + 1, ROUNDS);
            continue;
        }

        log::info!(
            "Round {}/{}: shipping patch from '{:.7}...' to '{:.7}...'",
            round + 1,
            ROUNDS,
            last_known,
            head,
        );
        let patch = Patch::create(&config, &last_known).unwrap();
        if let Some(sql) = sql::patch_to_sql(&config, &patch).unwrap() {
            hub.apply(&sql)
                .unwrap_or_else(|e| panic!("seed={seed} round={round}: psql apply failed:\n{e:#}"));
        }
        hub.assert_matches(&agent)
            .unwrap_or_else(|e| panic!("seed={seed} round={round}: {e:#}"));
        log::info!(
            "Round {}/{}: hub state matches agent model",
            round + 1,
            ROUNDS
        );
        last_known = head;
    }

    let _ = hub.cleanup();
}
