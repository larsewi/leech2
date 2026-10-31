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
use prost::Message;

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

fn work_dir(cli: &Cli) -> PathBuf {
    let base = cli.directory.clone().unwrap_or_else(|| PathBuf::from("."));
    base.join(LEECH2_DIR)
}

fn resolve_ref(
    config: &Config,
    reference: Option<&str>,
    num_blocks: Option<u32>,
) -> Result<String> {
    if reference.is_some() && num_blocks.is_some() {
        bail!("cannot specify both a hash prefix and -n");
    }
    let state_dir = config.ensure_state_dir()?;
    let _lock = leech2::storage::lock(&state_dir, false, config.file_mode)?;
    match (reference, num_blocks) {
        (Some(reference), _) => leech2::storage::resolve_hash_prefix(&state_dir, reference),
        (None, Some(num_blocks)) => walk_back(&state_dir, num_blocks),
        (None, None) => leech2::head::load(&state_dir),
    }
}

fn walk_back(work_dir: &std::path::Path, num_blocks: u32) -> Result<String> {
    let mut hash = leech2::head::load(work_dir)?;
    for i in 0..num_blocks {
        if hash == GENESIS_HASH {
            bail!(
                "cannot walk back {} block(s); only {} reachable from HEAD",
                num_blocks,
                i
            );
        }
        hash = Block::load_header(work_dir, &hash)?.parent;
    }
    Ok(hash)
}

fn cmd_init(work_dir: &std::path::Path) -> Result<()> {
    if work_dir.join("config.toml").exists() {
        bail!(
            "already initialized: {} exists",
            work_dir.join("config.toml").display()
        );
    }

    std::fs::create_dir_all(work_dir)?;

    std::fs::write(work_dir.join("config.toml"), INIT_CONFIG_TEMPLATE)?;

    std::fs::write(
        work_dir.join("products.csv"),
        "id,name,price\n\
         1,Keyboard,79.99\n\
         2,Mouse,34.50\n\
         3,Monitor,249.95\n",
    )?;

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
        leech2::reported::load(&state_dir)?
            .unwrap_or_else(|| leech2::utils::GENESIS_HASH.to_string())
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

/// Load block `hash` for the log. `None` means the block is not on disk: it was
/// truncated, so the reachable chain ends there.
fn load_logged_block(state_dir: &Path, hash: &str) -> Result<Option<Block>> {
    let Some(data) = leech2::storage::load(state_dir, hash)? else {
        return Ok(None);
    };
    let block = Block::decode(data.as_slice())
        .with_context(|| format!("failed to decode block '{}'", hash))?;
    Ok(Some(block))
}

/// Print the blocks reachable from HEAD, newest first. If a block can't be
/// read, the blocks before it are still printed before the error is returned.
fn cmd_block_log(config: &Config) -> Result<()> {
    let state_dir = config.ensure_state_dir()?;
    let _lock = leech2::storage::lock(&state_dir, false, config.file_mode)?;
    let mut hash = leech2::head::load(&state_dir)?;

    if hash == GENESIS_HASH {
        bail!("no blocks exist yet");
    }

    let mut output = String::new();
    let error = loop {
        let block = match load_logged_block(&state_dir, &hash) {
            Ok(Some(block)) => block,
            Ok(None) => break None, // block was truncated, end of reachable chain
            Err(e) => break Some(e),
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
            break None;
        }
    };

    if !output.is_empty() {
        print_with_pager(&output)?;
    }
    match error {
        Some(e) => Err(e),
        None => Ok(()),
    }
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
/// to launch. Honors the `PAGER` environment variable. A trailing newline is
/// added only when `content` does not already end with one.
fn print_with_pager(content: &str) -> Result<()> {
    let is_tty = std::io::stdout().is_terminal();
    let exceeds_height =
        terminal_size::terminal_size().is_some_and(|(_, h)| content.lines().count() > h.0 as usize);
    let use_pager = is_tty && exceeds_height;

    if !use_pager {
        return print_stdout(content);
    }

    let default_pager = if cfg!(windows) { "more" } else { "less" };
    let pager_cmd = std::env::var("PAGER").unwrap_or_else(|_| default_pager.to_string());

    let mut child = match ProcessCommand::new(&pager_cmd)
        .stdin(Stdio::piped())
        .spawn()
    {
        Ok(child) => child,
        Err(_) => return print_stdout(content),
    };

    if let Some(mut stdin) = child.stdin.take() {
        let _ = stdin.write_all(content.as_bytes());
    }

    let _ = child.wait();
    Ok(())
}

fn run(cli: Cli) -> Result<()> {
    let work_dir = work_dir(&cli);

    match &cli.command {
        Cmd::Init => cmd_init(&work_dir)?,
        Cmd::Block { command } => {
            let mut config = Config::load(&work_dir)?;
            config.dry_run = cli.dry_run;
            match command {
                BlockCmd::Create => cmd_block_create(&config)?,
                BlockCmd::Show { reference, n } => {
                    let output = cmd_block_show(&config, reference.as_deref(), *n)?;
                    print_with_pager(&output)?;
                }
                BlockCmd::Log => cmd_block_log(&config)?,
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
}
