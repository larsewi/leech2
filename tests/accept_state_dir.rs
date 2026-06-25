mod common;

use leech2::block::Block;
use leech2::config::Config;
use leech2::head;

const TABLE: &str = r#"
[tables.users]
fields = [
    { name = "id", type = "NUMBER", primary-key = true },
    { name = "name", type = "TEXT" },
]

[tables.users.csv]
source = "users.csv"
"#;

/// Without `state-dir`, state lives in a `state` subdirectory of the work
/// directory, separate from the config and CSV inputs that sit at the root.
#[test]
fn test_state_dir_defaults_to_subdir() {
    common::init_logging();
    let tmp = tempfile::tempdir().unwrap();
    let work_dir = tmp.path();

    common::write_config(work_dir, "config.toml", TABLE);
    common::write_csv(work_dir, "users.csv", "1,Alice\n2,Bob\n");

    let config = Config::load(work_dir).unwrap();
    assert_eq!(config.state_dir(), work_dir.join("state"));

    let hash = Block::create(&config, None).unwrap();

    // State landed in the subdirectory, not next to the config.
    assert!(work_dir.join("state").join("HEAD").exists());
    assert!(!work_dir.join("HEAD").exists());
    assert_eq!(
        head::load(&config.state_dir(), config.file_mode).unwrap(),
        hash
    );
}

/// An absolute `state-dir` puts all state files there, while CSV inputs are
/// still read relative to the work directory.
#[test]
fn test_state_dir_absolute_redirect() {
    common::init_logging();
    let work = tempfile::tempdir().unwrap();
    let state = tempfile::tempdir().unwrap();
    let work_dir = work.path();
    let state_dir = state.path();

    common::write_config(
        work_dir,
        "config.toml",
        &format!("state-dir = {:?}\n{}", state_dir, TABLE),
    );
    common::write_csv(work_dir, "users.csv", "1,Alice\n2,Bob\n");

    let config = Config::load(work_dir).unwrap();
    assert_eq!(config.state_dir(), state_dir);

    let hash = Block::create(&config, None).unwrap();

    // HEAD, STATE, and the block file live under the configured state dir.
    assert!(state_dir.join("HEAD").exists());
    assert!(state_dir.join("STATE").exists());
    assert!(state_dir.join(&hash).exists());

    // Nothing state-related leaked into the work directory.
    assert!(!work_dir.join("HEAD").exists());
    assert!(!work_dir.join("STATE").exists());
    assert!(!work_dir.join("state").exists());
}

/// A relative `state-dir` resolves against the work directory.
#[test]
fn test_state_dir_relative_to_work_dir() {
    common::init_logging();
    let tmp = tempfile::tempdir().unwrap();
    let work_dir = tmp.path();

    common::write_config(
        work_dir,
        "config.toml",
        &format!("state-dir = \"db\"\n{}", TABLE),
    );
    common::write_csv(work_dir, "users.csv", "1,Alice\n");

    let config = Config::load(work_dir).unwrap();
    assert_eq!(config.state_dir(), work_dir.join("db"));

    Block::create(&config, None).unwrap();
    assert!(work_dir.join("db").join("HEAD").exists());
}
