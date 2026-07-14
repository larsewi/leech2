mod common;

use leech2::block::Block;
use leech2::config::Config;
use leech2::patch::Patch;
use leech2::stats::{self, STATS_FILE};
use leech2::utils::GENESIS_HASH;
use leech2::wire;
use serde_json::Value;

fn config_toml(enable: bool) -> String {
    format!(
        r#"
[stats]
enable = {enable}

[tables.users]
fields = [
    {{ name = "id", type = "NUMBER", primary-key = true }},
    {{ name = "name", type = "TEXT" }},
]

[tables.users.csv]
source = "users.csv"
"#
    )
}

/// Mirror the front-end patch-create sequence: create, encode, finalize. The
/// stages record into the config as they run; finalize drains them to STATS.
fn create_patch_with_stats(config: &Config, reference: &str) {
    let patch = Patch::create(config, reference).unwrap();
    let _encoded = wire::encode_patch(config, &patch).unwrap();
    stats::finalize_patch_create(config);
}

fn stats_path(config: &Config) -> std::path::PathBuf {
    config.state_dir().join(STATS_FILE)
}

fn read_stats(config: &Config) -> Vec<Value> {
    let bytes = std::fs::read(stats_path(config)).unwrap();
    serde_json::from_slice(&bytes).unwrap()
}

#[test]
fn test_stats_enabled_writes_cumulative_records() {
    common::init_logging();
    let tmp = tempfile::tempdir().unwrap();
    let work_dir = tmp.path();

    common::write_config(work_dir, "config.toml", &config_toml(true));
    common::write_csv(work_dir, "users.csv", "1,Alice\n2,Bob\n");
    let config = Config::load(work_dir).unwrap();
    Block::create(&config, None).unwrap();

    create_patch_with_stats(&config, GENESIS_HASH);

    let entries = read_stats(&config);
    assert_eq!(entries.len(), 1);

    let run = &entries[0];
    assert!(run["timestamp"].is_string());
    for stage in ["delta_merging", "compression"] {
        assert!(run[stage]["duration_ms"].as_f64().unwrap() >= 0.0);
        assert!(run[stage]["bytes_in"].is_u64());
        assert!(run[stage]["bytes_out"].is_u64());
    }
    // Pipeline invariant: the consolidated patch is what compression receives.
    assert_eq!(
        run["delta_merging"]["bytes_out"],
        run["compression"]["bytes_in"]
    );

    // A second run appends rather than overwriting.
    create_patch_with_stats(&config, GENESIS_HASH);
    assert_eq!(read_stats(&config).len(), 2);
}

#[test]
fn test_stats_disabled_writes_nothing() {
    common::init_logging();
    let tmp = tempfile::tempdir().unwrap();
    let work_dir = tmp.path();

    common::write_config(work_dir, "config.toml", &config_toml(false));
    common::write_csv(work_dir, "users.csv", "1,Alice\n2,Bob\n");
    let config = Config::load(work_dir).unwrap();
    Block::create(&config, None).unwrap();

    create_patch_with_stats(&config, GENESIS_HASH);

    assert!(!stats_path(&config).exists());
}

#[test]
fn test_stats_dry_run_writes_nothing() {
    common::init_logging();
    let tmp = tempfile::tempdir().unwrap();
    let work_dir = tmp.path();

    common::write_config(work_dir, "config.toml", &config_toml(true));
    common::write_csv(work_dir, "users.csv", "1,Alice\n2,Bob\n");
    let mut config = Config::load(work_dir).unwrap();
    Block::create(&config, None).unwrap();
    config.dry_run = true;

    create_patch_with_stats(&config, GENESIS_HASH);

    assert!(!stats_path(&config).exists());
}

/// Sum a stage field across the raw entries.
fn sum_saved(entries: &[Value], stage: &str) -> i64 {
    entries
        .iter()
        .map(|run| {
            run[stage]["bytes_in"].as_i64().unwrap()
                - run[stage]["bytes_out"].as_i64().unwrap()
        })
        .sum()
}

#[test]
fn test_summarize_aggregates_runs() {
    common::init_logging();
    let tmp = tempfile::tempdir().unwrap();
    let work_dir = tmp.path();

    common::write_config(work_dir, "config.toml", &config_toml(true));
    common::write_csv(work_dir, "users.csv", "1,Alice\n2,Bob\n");
    let config = Config::load(work_dir).unwrap();
    Block::create(&config, None).unwrap();

    create_patch_with_stats(&config, GENESIS_HASH);
    create_patch_with_stats(&config, GENESIS_HASH);

    let entries = read_stats(&config);
    let summary = stats::summarize(&config).unwrap().expect("summary");

    assert_eq!(summary.runs, 2);
    assert_eq!(
        summary.delta_saved_bytes,
        sum_saved(&entries, "delta_merging")
    );
    assert_eq!(
        summary.compression_saved_bytes,
        sum_saved(&entries, "compression")
    );
    // The `last` fields reflect the final recorded run.
    let last = entries.last().unwrap();
    assert_eq!(
        summary.compression_last_saved_bytes,
        last["compression"]["bytes_in"].as_i64().unwrap()
            - last["compression"]["bytes_out"].as_i64().unwrap()
    );
}

#[test]
fn test_summarize_returns_none_without_stats_file() {
    common::init_logging();
    let tmp = tempfile::tempdir().unwrap();
    let work_dir = tmp.path();

    common::write_config(work_dir, "config.toml", &config_toml(true));
    common::write_csv(work_dir, "users.csv", "1,Alice\n2,Bob\n");
    let config = Config::load(work_dir).unwrap();

    assert!(stats::summarize(&config).unwrap().is_none());
}
