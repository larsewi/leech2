use std::io::{IsTerminal, Write};
use std::path::PathBuf;
use std::process::{Command as ProcessCommand, ExitCode, Stdio};

use anyhow::{Context, Result, bail};
use clap::{Parser, Subcommand};
use leech2::block::Block;
use leech2::config::Config;
use leech2::utils::{GENESIS_HASH, format_timestamp};

const LEECH2_DIR: &str = ".leech2";
const PATCH_FILE: &str = "PATCH";

#[derive(Parser)]
#[command(name = "lch", about = "leech2 CLI - track CSV changes", version)]
struct Cli {
    /// Run as if started in <path> instead of the current directory
    #[arg(short = 'C', global = true)]
    directory: Option<PathBuf>,

    #[command(subcommand)]
    command: Cmd,
}

#[derive(Subcommand)]
enum Cmd {
    /// Initialize a new .leech2 work directory with an example table
    Init,
    /// Create or show blocks
    Block {
        #[command(subcommand)]
        command: BlockCmd,
    },
    /// Create, show, or convert patches
    Patch {
        #[command(subcommand)]
        command: PatchCmd,
    },
    /// Alias for `block log`
    Log,
    /// Alias for `patch sql`
    Sql,
    /// Alias for `patch applied`
    Applied,
    /// Alias for `patch failed`
    Failed,
}

#[derive(Subcommand)]
enum BlockCmd {
    /// Create a new block from current CSV state
    Create,
    /// Show the full contents of a block
    Show {
        /// Block hash prefix [default: HEAD]
        #[arg(name = "REF")]
        reference: Option<String>,
        /// Show the block N steps back from HEAD
        #[arg(short)]
        n: Option<u32>,
    },
    /// List all blocks from HEAD to genesis
    Log,
}

#[derive(Subcommand)]
enum PatchCmd {
    /// Create a patch from REF to HEAD and write to .leech2/PATCH
    Create {
        /// Block hash prefix [default: REPORTED or GENESIS]
        #[arg(name = "REF")]
        reference: Option<String>,
        /// Create a patch covering the last N blocks
        #[arg(short)]
        n: Option<u32>,
    },
    /// Show the contents of the .leech2/PATCH file
    Show,
    /// Convert the .leech2/PATCH file to SQL
    Sql,
    /// Inject a field into the .leech2/PATCH file
    Inject {
        /// Column name
        name: String,
        /// Value
        value: String,
        /// SQL type: TEXT, NUMBER, or BOOLEAN
        #[arg(default_value = "TEXT")]
        sql_type: String,
    },
    /// Mark the current patch as applied (saves head hash to REPORTED)
    Applied,
    /// Mark the current patch as failed (removes REPORTED to force full state)
    Failed,
}

fn work_dir(cli: &Cli) -> PathBuf {
    let base = cli.directory.clone().unwrap_or_else(|| PathBuf::from("."));
    base.join(LEECH2_DIR)
}

fn resolve_ref(
    config: &Config,
    reference: Option<&str>,
    num_blocks: Option<u32>,
) -> Result<String> {
    match (reference, num_blocks) {
        (Some(_), Some(_)) => bail!("cannot specify both a hash prefix and -n"),
        (Some(reference), None) => {
            leech2::storage::resolve_hash_prefix(&config.work_dir, reference)
        }
        (None, Some(num_blocks)) => walk_back(&config.work_dir, num_blocks),
        (None, None) => leech2::head::load(&config.work_dir),
    }
}

fn walk_back(work_dir: &std::path::Path, num_blocks: u32) -> Result<String> {
    let mut hash = leech2::head::load(work_dir)?;
    for i in 0..num_blocks {
        if hash == GENESIS_HASH {
            bail!(
                "only {} block(s) in chain, cannot go back {}",
                i,
                num_blocks
            );
        }
        hash = Block::load_parent_hash(work_dir, &hash)?;
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

    std::fs::write(
        work_dir.join("config.toml"),
        r#"[tables.products]
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
"#,
    )?;

    std::fs::write(
        work_dir.join("products.csv"),
        "id,name,price\n\
         1,Keyboard,79.99\n\
         2,Mouse,34.50\n\
         3,Monitor,249.95\n",
    )?;

    println!("Initialized {}", work_dir.display());
    Ok(())
}

fn cmd_block_create(config: &Config) -> Result<()> {
    let hash = Block::create(config)?;
    println!("{}", hash);
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
        leech2::reported::load(&config.work_dir)?
            .unwrap_or_else(|| leech2::utils::GENESIS_HASH.to_string())
    } else {
        resolve_ref(config, reference, num_blocks)?
    };
    let patch = leech2::patch::Patch::create(config, &hash)?;

    let encoded = leech2::wire::encode_patch(config, &patch)?;
    leech2::storage::store(&config.work_dir, PATCH_FILE, &encoded)?;

    println!("{}", patch);
    Ok(())
}

fn cmd_block_log(config: &Config) -> Result<String> {
    let work_dir = &config.work_dir;
    let mut hash = leech2::head::load(work_dir)?;

    if hash == GENESIS_HASH {
        bail!("no blocks exist yet");
    }

    let mut output = String::new();
    loop {
        let block = match Block::load(work_dir, &hash) {
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
    let block = Block::load(&config.work_dir, &hash)?;
    Ok(format!("block {}\n{}", hash, block))
}

fn load_patch(config: &Config) -> Result<leech2::patch::Patch> {
    let data = leech2::storage::load(&config.work_dir, PATCH_FILE)?
        .context("no patch file found, run `lch patch create` first")?;
    leech2::wire::decode_patch(&data).context("failed to decode patch")
}

fn cmd_patch_show(config: &Config) -> Result<String> {
    let patch = load_patch(config)?;
    Ok(format!("{}", patch))
}

fn cmd_patch_sql(config: &Config) -> Result<String> {
    let patch = load_patch(config)?;
    match leech2::sql::patch_to_sql(config, &patch)? {
        Some(sql) => Ok(sql),
        None => Ok("-- no changes\n".to_string()),
    }
}

fn cmd_patch_inject(config: &Config, name: &str, value: &str, sql_type: &str) -> Result<()> {
    let mut patch = load_patch(config)?;
    patch.inject_field(name, value, sql_type)?;

    let encoded = leech2::wire::encode_patch(config, &patch)?;
    leech2::storage::store(&config.work_dir, PATCH_FILE, &encoded)?;

    println!("{}", patch);
    Ok(())
}

fn cmd_patch_applied(config: &Config) -> Result<()> {
    let patch = load_patch(config)?;
    leech2::reported::save(&config.work_dir, &patch.head)?;

    println!("{}", patch.head);
    Ok(())
}

fn cmd_patch_failed(config: &Config) -> Result<()> {
    leech2::reported::remove(&config.work_dir)?;
    println!("REPORTED removed; next patch will be a full state");
    Ok(())
}

/// Print `content` to stdout, piping through a pager (e.g. `less`) when the
/// output exceeds the terminal height. Falls back to plain `println!` when
/// stdout is not a TTY, the terminal size is unavailable, or the pager fails
/// to launch. Honors the `PAGER` environment variable.
fn print_with_pager(content: &str) {
    let is_tty = std::io::stdout().is_terminal();
    let exceeds_height =
        terminal_size::terminal_size().is_some_and(|(_, h)| content.lines().count() > h.0 as usize);
    let use_pager = is_tty && exceeds_height;

    if !use_pager {
        println!("{}", content);
        return;
    }

    let default_pager = if cfg!(windows) { "more" } else { "less" };
    let pager_cmd = std::env::var("PAGER").unwrap_or_else(|_| default_pager.to_string());

    let mut child = match ProcessCommand::new(&pager_cmd)
        .stdin(Stdio::piped())
        .spawn()
    {
        Ok(child) => child,
        Err(_) => {
            print!("{}", content);
            return;
        }
    };

    if let Some(mut stdin) = child.stdin.take() {
        let _ = stdin.write_all(content.as_bytes());
    }

    let _ = child.wait();
}

fn run(cli: Cli) -> Result<()> {
    let work_dir = work_dir(&cli);

    if let Cmd::Init = &cli.command {
        return cmd_init(&work_dir);
    }

    let config = Config::load(&work_dir)?;

    match &cli.command {
        Cmd::Init => unreachable!(),
        Cmd::Block { command } => match command {
            BlockCmd::Create => cmd_block_create(&config)?,
            BlockCmd::Show { reference, n } => {
                let output = cmd_block_show(&config, reference.as_deref(), *n)?;
                print_with_pager(&output);
            }
            BlockCmd::Log => {
                let output = cmd_block_log(&config)?;
                print_with_pager(&output);
            }
        },
        Cmd::Patch { command } => match command {
            PatchCmd::Create { reference, n } => {
                cmd_patch_create(&config, reference.as_deref(), *n)?;
            }
            PatchCmd::Show => {
                let output = cmd_patch_show(&config)?;
                print_with_pager(&output);
            }
            PatchCmd::Sql => {
                let output = cmd_patch_sql(&config)?;
                print_with_pager(&output);
            }
            PatchCmd::Inject {
                name,
                value,
                sql_type,
            } => {
                cmd_patch_inject(&config, name, value, sql_type)?;
            }
            PatchCmd::Applied => {
                cmd_patch_applied(&config)?;
            }
            PatchCmd::Failed => {
                cmd_patch_failed(&config)?;
            }
        },
        Cmd::Log => {
            eprintln!(
                "warning: `lch log` is an alias that may change; use `lch block log` in scripts"
            );
            let output = cmd_block_log(&config)?;
            print_with_pager(&output);
        }
        Cmd::Sql => {
            eprintln!(
                "warning: `lch sql` is an alias that may change; use `lch patch sql` in scripts"
            );
            let output = cmd_patch_sql(&config)?;
            print_with_pager(&output);
        }
        Cmd::Applied => {
            eprintln!(
                "warning: `lch applied` is an alias that may change; use `lch patch applied` in scripts"
            );
            cmd_patch_applied(&config)?;
        }
        Cmd::Failed => {
            eprintln!(
                "warning: `lch failed` is an alias that may change; use `lch patch failed` in scripts"
            );
            cmd_patch_failed(&config)?;
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
