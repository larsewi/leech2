use std::io::{ErrorKind, IsTerminal, Write};
use std::path::{Path, PathBuf};
use std::process::{Command as ProcessCommand, ExitCode, Stdio};

use anyhow::{Context, Result, bail};
use clap::Parser;
use leech2::block::Block;
use leech2::cell::{Kind, parse_typed_cell};
use leech2::config::Config;
use leech2::sql::InjectedField;
use leech2::state::ProtoState;
use leech2::utils::{GENESIS_HASH, format_timestamp};

mod cli;
use cli::{BlockCmd, Cli, Cmd, PatchCmd, StateCmd, StatsCmd};

const LEECH2_DIR: &str = ".leech2";
const PATCH_FILE: &str = "PATCH";

const INIT_CONFIG_TEMPLATE: &str = r#"[stats]
enable = true

[tables.products.csv]
source = "products.csv"
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
"#;

const INIT_CSV_TEMPLATE: &str = "id,name,price\n\
                                 1,Keyboard,79.99\n\
                                 2,Mouse,34.50\n\
                                 3,Monitor,249.95\n";

fn work_dir(cli: &Cli) -> PathBuf {
    let base = cli.directory.clone().unwrap_or_else(|| PathBuf::from("."));
    base.join(LEECH2_DIR)
}

fn resolve_ref(
    config: &Config,
    reference: Option<&str>,
    num_blocks: Option<u32>,
) -> Result<String> {
    let state_dir = config.ensure_state_dir()?;
    let _lock = leech2::storage::lock(&state_dir, false, config.file_mode)?;
    match (reference, num_blocks) {
        (Some(reference), _) => leech2::storage::resolve_hash_prefix(&state_dir, reference),
        (None, Some(num_blocks)) => walk_back(&state_dir, num_blocks),
        (None, None) => leech2::head::load(&state_dir),
    }
}

fn walk_back(state_dir: &Path, num_blocks: u32) -> Result<String> {
    let mut hash = leech2::head::load(state_dir)?;
    for i in 0..num_blocks {
        if hash == GENESIS_HASH {
            bail!(
                "cannot walk back {} block(s); only {} reachable from HEAD",
                num_blocks,
                i
            );
        }
        hash = Block::load_header(state_dir, &hash)?.parent;
    }
    Ok(hash)
}

fn cmd_init(work_dir: &Path, dry_run: bool) -> Result<()> {
    // Check every file up front, so that init never overwrites or half-writes.
    for name in ["config.toml", "config.json", "products.csv"] {
        let path = work_dir.join(name);
        if path.exists() {
            bail!("already initialized: {} exists", path.display());
        }
    }

    let files = [
        ("config.toml", INIT_CONFIG_TEMPLATE),
        ("products.csv", INIT_CSV_TEMPLATE),
    ];

    if dry_run {
        if !work_dir.exists() {
            eprintln!("Would have created directory '{}'", work_dir.display());
        }
        for (name, content) in files {
            eprintln!(
                "Would have written {} bytes to '{}'",
                content.len(),
                work_dir.join(name).display()
            );
        }
        return Ok(());
    }

    std::fs::create_dir_all(work_dir)?;
    for (name, content) in files {
        std::fs::write(work_dir.join(name), content)?;
    }

    print_stdout(&format!("Initialized {}", work_dir.display()))?;
    Ok(())
}

fn cmd_block_create(config: &Config) -> Result<()> {
    let hash = Block::create(config, None)?;
    // In a dry run, `Block::create` prints the block that would have been
    // created; otherwise report the new block's hash.
    if !config.dry_run {
        print_stdout(&hash)?;
    }
    Ok(())
}

fn cmd_patch_create(
    config: &Config,
    reference: Option<&str>,
    num_blocks: Option<u32>,
) -> Result<()> {
    // When no explicit reference is given, default to the last reported hash
    // (i.e. the hash the server already knows about) so the patch only contains
    // new blocks. Fall back to the genesis hash if nothing has been reported yet.
    let hash = if reference.is_none() && num_blocks.is_none() {
        let state_dir = config.ensure_state_dir()?;
        let _lock = leech2::storage::lock(&state_dir, false, config.file_mode)?;
        leech2::reported::load_or_genesis(&state_dir)?
    } else {
        resolve_ref(config, reference, num_blocks)?
    };
    let patch = leech2::patch::Patch::create(config, &hash)?;

    let encoded = leech2::wire::encode_patch(config, &patch)?;
    let state_dir = config.ensure_state_dir()?;
    {
        let _lock = leech2::storage::lock(&state_dir, true, config.file_mode)?;
        leech2::storage::store(
            &state_dir,
            PATCH_FILE,
            &encoded,
            config.file_mode,
            config.dry_run,
        )?;
    }

    leech2::stats::finalize_patch_create(config);

    // In a dry run, `Patch::create` prints the patch that would have been
    // created; otherwise report the head the patch was built against.
    if !config.dry_run {
        print_stdout(&patch.head)?;
    }
    Ok(())
}

fn cmd_block_log(config: &Config) -> Result<String> {
    let state_dir = config.ensure_state_dir()?;
    let _lock = leech2::storage::lock(&state_dir, false, config.file_mode)?;
    let mut hash = leech2::head::load(&state_dir)?;

    if hash == GENESIS_HASH {
        bail!("no blocks exist yet");
    }

    let mut output = String::new();
    loop {
        let block = match Block::load(&state_dir, &hash) {
            Ok(block) => block,
            Err(_) => break, // block was truncated, end of reachable chain
        };

        let timestamp = block
            .created
            .as_ref()
            .map(format_timestamp)
            .unwrap_or_else(|| "N/A".to_string());

        let table_names: Vec<&str> = block.payload.keys().map(|name| name.as_str()).collect();
        let tables_str = if table_names.is_empty() {
            "no changes".to_string()
        } else {
            table_names.join(", ")
        };

        output.push_str(&format!(
            "block {}  {}  ({} tables: {})\n",
            hash,
            timestamp,
            block.payload.len(),
            tables_str
        ));

        hash = block.parent.clone();
        if hash == GENESIS_HASH {
            break;
        }
    }

    Ok(output)
}

fn cmd_block_show(config: &Config, reference: Option<&str>, n: Option<u32>) -> Result<String> {
    let hash = resolve_ref(config, reference, n)?;
    if hash == GENESIS_HASH {
        bail!("cannot show the genesis block");
    }
    let state_dir = config.ensure_state_dir()?;
    let _lock = leech2::storage::lock(&state_dir, false, config.file_mode)?;
    let block = Block::load(&state_dir, &hash)?;
    Ok(block.to_string())
}

fn load_patch(config: &Config) -> Result<leech2::patch::Patch> {
    let state_dir = config.ensure_state_dir()?;
    let _lock = leech2::storage::lock(&state_dir, false, config.file_mode)?;
    let data = leech2::storage::load(&state_dir, PATCH_FILE)?
        .context("no patch file found, run `lch patch create` first")?;
    leech2::wire::decode_patch(&data).context("failed to decode patch")
}

fn cmd_patch_show(config: &Config) -> Result<String> {
    let patch = load_patch(config)?;
    Ok(format!("{}", patch))
}

/// Parse the values of one `--inject NAME VALUE KIND` option.
fn parse_injected_field(args: &[String]) -> Result<InjectedField> {
    let [name, value, kind] = args else {
        bail!("--inject expects NAME VALUE KIND");
    };
    let kind = Kind::from_config(kind)?;
    let value =
        parse_typed_cell(value, kind).with_context(|| format!("injected field '{}'", name))?;
    InjectedField::new(name, value)
}

fn cmd_patch_sql(config: &Config, inject: &[String]) -> Result<String> {
    let mut injected_fields = Vec::new();
    for args in inject.chunks(3) {
        injected_fields.push(parse_injected_field(args)?);
    }
    let patch = load_patch(config)?;
    match leech2::sql::patch_to_sql(config, &patch, &injected_fields)? {
        Some(sql) => Ok(sql),
        None => Ok("-- no changes\n".to_string()),
    }
}

fn cmd_patch_applied(config: &Config) -> Result<()> {
    let patch = load_patch(config)?;
    let state_dir = config.ensure_state_dir()?;
    {
        let _lock = leech2::storage::lock(&state_dir, true, config.file_mode)?;
        leech2::reported::save(&state_dir, &patch.head, config.file_mode, config.dry_run)?;
    }

    print_stdout(&patch.head)?;
    Ok(())
}

fn cmd_state_show(config: &Config) -> Result<String> {
    let state_dir = config.ensure_state_dir()?;
    let _lock = leech2::storage::lock(&state_dir, false, config.file_mode)?;
    let state = ProtoState::load(&state_dir)?
        .context("no STATE file found, run `lch block create` first")?;
    Ok(state.to_string())
}

fn cmd_stats_show(config: &Config) -> Result<()> {
    match leech2::stats::summarize(config)? {
        Some(summary) => print_stdout(&summary.to_string()),
        None => print_stdout("No stats recorded yet"),
    }
}

fn cmd_patch_failed(config: &Config) -> Result<()> {
    let state_dir = config.ensure_state_dir()?;
    {
        let _lock = leech2::storage::lock(&state_dir, true, config.file_mode)?;
        leech2::reported::remove(&state_dir, config.dry_run)?;
    }

    // Without REPORTED, the next patch is built from genesis.
    print_stdout(GENESIS_HASH)
}

/// Print `content` to stdout, followed by a newline unless it already ends
/// with one. A reader that closed the pipe early (e.g. `lch block log | head
/// -1`) already has all the output it wanted, so a broken pipe is not an error.
fn print_stdout(content: &str) -> Result<()> {
    let mut stdout = std::io::stdout().lock();
    let mut result = stdout.write_all(content.as_bytes());
    if result.is_ok() && !content.ends_with('\n') {
        result = stdout.write_all(b"\n");
    }
    if result.is_ok() {
        result = stdout.flush();
    }
    match result {
        Err(e) if e.kind() == ErrorKind::BrokenPipe => Ok(()),
        result => result.context("failed to write to stdout"),
    }
}

/// Print `content` to stdout, piping through a pager (e.g. `less`) when the
/// output exceeds the terminal height. Falls back to plain printing when
/// stdout is not a TTY, the terminal size is unavailable, or the pager fails
/// to launch. Honors the `PAGER` environment variable, which may include
/// arguments on Unix. A trailing newline is added only when `content` does not
/// already end with one.
/// Count the terminal rows `content` takes up when lines wider than `width`
/// wrap. Assumes every character is one column wide.
fn count_rows(content: &str, width: usize) -> usize {
    let mut rows = 0;
    for line in content.lines() {
        let columns = line.chars().count();
        rows += columns.div_ceil(width.max(1)).max(1);
    }
    rows
}

fn print_with_pager(content: &str) -> Result<()> {
    let is_tty = std::io::stdout().is_terminal();
    let exceeds_height = terminal_size::terminal_size()
        .is_some_and(|(width, height)| count_rows(content, width.0 as usize) > height.0 as usize);
    let use_pager = is_tty && exceeds_height;

    if !use_pager {
        return print_stdout(content);
    }

    let default_pager = if cfg!(windows) { "more" } else { "less" };
    let pager_cmd = std::env::var("PAGER").unwrap_or_else(|_| default_pager.to_string());

    // On Unix, run the pager through the shell, as git does with `$PAGER` (see
    // `core.pager` in git-config(1)), so that a `PAGER` with arguments works.
    let mut command = if cfg!(unix) {
        let mut command = ProcessCommand::new("sh");
        command.arg("-c").arg(&pager_cmd);
        command
    } else {
        ProcessCommand::new(&pager_cmd)
    };

    let mut child = match command.stdin(Stdio::piped()).spawn() {
        Ok(child) => child,
        Err(_) => return print_stdout(content),
    };

    if let Some(mut stdin) = child.stdin.take() {
        let _ = stdin.write_all(content.as_bytes());
    }

    // The shell exits with 127 when the pager is not found, and 126 when it
    // can't be executed. The output was not shown then, so print it instead.
    if let Ok(status) = child.wait()
        && matches!(status.code(), Some(126 | 127))
    {
        return print_stdout(content);
    }
    Ok(())
}

fn run(cli: Cli) -> Result<()> {
    let work_dir = work_dir(&cli);

    match &cli.command {
        Cmd::Init => cmd_init(&work_dir, cli.dry_run)?,
        Cmd::Block { command } => {
            let mut config = Config::load(&work_dir)?;
            config.dry_run = cli.dry_run;
            match command {
                BlockCmd::Create => cmd_block_create(&config)?,
                BlockCmd::Show { reference, n } => {
                    let output = cmd_block_show(&config, reference.as_deref(), *n)?;
                    print_with_pager(&output)?;
                }
                BlockCmd::Log => {
                    let output = cmd_block_log(&config)?;
                    print_with_pager(&output)?;
                }
            }
        }
        Cmd::Patch { command } => {
            let mut config = Config::load(&work_dir)?;
            config.dry_run = cli.dry_run;
            match command {
                PatchCmd::Create { reference, n } => {
                    cmd_patch_create(&config, reference.as_deref(), *n)?;
                }
                PatchCmd::Show => {
                    let output = cmd_patch_show(&config)?;
                    print_with_pager(&output)?;
                }
                PatchCmd::Sql { inject } => {
                    let output = cmd_patch_sql(&config, inject)?;
                    print_with_pager(&output)?;
                }
                PatchCmd::Applied => {
                    cmd_patch_applied(&config)?;
                }
                PatchCmd::Failed => {
                    cmd_patch_failed(&config)?;
                }
            }
        }
        Cmd::State { command } => {
            let config = Config::load(&work_dir)?;
            match command {
                StateCmd::Show => {
                    let output = cmd_state_show(&config)?;
                    print_with_pager(&output)?;
                }
            }
        }
        Cmd::Stats { command } => {
            let config = Config::load(&work_dir)?;
            match command {
                StatsCmd::Show => cmd_stats_show(&config)?,
            }
        }
    }

    Ok(())
}

fn main() -> ExitCode {
    env_logger::Builder::from_env(env_logger::Env::new().filter("LEECH2_LOG")).init();

    let cli = Cli::parse();

    if let Err(e) = run(cli) {
        eprintln!("error: {:#}", e);
        return ExitCode::FAILURE;
    }

    ExitCode::SUCCESS
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn init_template_parses_as_csv_backed_table() {
        let work_dir = tempfile::tempdir().unwrap();
        std::fs::write(work_dir.path().join("config.toml"), INIT_CONFIG_TEMPLATE).unwrap();

        let config = Config::load(work_dir.path()).unwrap();
        let products = config
            .tables
            .get("products")
            .expect("init template must define a 'products' table");
        let csv = products
            .csv
            .as_ref()
            .expect("init template must place 'source'/'header' under [tables.products.csv]");
        assert_eq!(csv.source, "products.csv");
        assert!(csv.header);
        assert!(config.stats.enable, "init template must enable stats");
    }

    #[test]
    fn init_refuses_existing_json_config() {
        let work_dir = tempfile::tempdir().unwrap();
        std::fs::write(work_dir.path().join("config.json"), "{}").unwrap();

        let err = cmd_init(work_dir.path(), false).unwrap_err();
        assert!(err.to_string().contains("config.json exists"), "got: {err}");
        assert!(!work_dir.path().join("config.toml").exists());
    }

    #[test]
    fn count_rows_counts_wrapped_lines() {
        // An empty line takes one row, and a line of 25 columns takes 3 rows
        // in a terminal 10 columns wide.
        let content = format!("short\n\n{}\n{}\n", "x".repeat(10), "x".repeat(25));
        assert_eq!(count_rows(&content, 10), 1 + 1 + 1 + 3);
    }

    #[test]
    fn init_dry_run_writes_nothing() {
        let parent = tempfile::tempdir().unwrap();
        let work_dir = parent.path().join(LEECH2_DIR);

        cmd_init(&work_dir, true).unwrap();
        assert!(!work_dir.exists());
    }

    #[test]
    fn init_refuses_existing_csv() {
        let work_dir = tempfile::tempdir().unwrap();
        std::fs::write(work_dir.path().join("products.csv"), "mine").unwrap();

        let err = cmd_init(work_dir.path(), false).unwrap_err();
        assert!(
            err.to_string().contains("products.csv exists"),
            "got: {err}"
        );
        assert!(!work_dir.path().join("config.toml").exists());
        let csv = std::fs::read_to_string(work_dir.path().join("products.csv")).unwrap();
        assert_eq!(csv, "mine");
    }
}
