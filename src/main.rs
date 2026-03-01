use std::io::Write;
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
#[command(name = "lch", about = "leech2 CLI - track CSV changes")]
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
    /// List all blocks from HEAD to genesis
    Log,
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
    /// Mark the current patch as applied (saves head hash to REPORTED)
    Applied,
}

fn work_dir(cli: &Cli) -> PathBuf {
    let base = cli.directory.clone().unwrap_or_else(|| PathBuf::from("."));
    base.join(LEECH2_DIR)
}

fn resolve_ref(config: &Config, reference: Option<&str>, n: Option<u32>) -> Result<String> {
    match (reference, n) {
        (Some(_), Some(_)) => bail!("cannot specify both a hash prefix and -n"),
        (Some(r), None) => leech2::patch::resolve_hash_prefix(&config.work_dir, r),
        (None, Some(n)) => walk_back(&config.work_dir, n),
        (None, None) => leech2::head::load(&config.work_dir),
    }
}

fn walk_back(work_dir: &std::path::Path, n: u32) -> Result<String> {
    let mut hash = leech2::head::load(work_dir)?;
    for i in 0..n {
        if hash == GENESIS_HASH {
            bail!("only {} block(s) in chain, cannot go back {}", i, n);
        }
        let block = Block::load(work_dir, &hash)?;
        hash = block.parent;
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
        r#"[tables.employees]
source = "employees.csv"
header = true

[[tables.employees.fields]]
name = "employee_id"
type = "INTEGER"
primary-key = true

[[tables.employees.fields]]
name = "first_name"
type = "TEXT"

[[tables.employees.fields]]
name = "last_name"
type = "TEXT"

[[tables.employees.fields]]
name = "email"
type = "TEXT"

[[tables.employees.fields]]
name = "department"
type = "TEXT"

[[tables.employees.fields]]
name = "salary"
type = "INTEGER"

[[tables.employees.fields]]
name = "hire_date"
type = "TEXT"
"#,
    )?;

    std::fs::write(
        work_dir.join("employees.csv"),
        "employee_id,first_name,last_name,email,department,salary,hire_date\n\
         1,Alice,Johnson,alice.johnson@example.com,Engineering,92000,2021-03-15\n\
         2,Bob,Smith,bob.smith@example.com,Sales,67000,2019-07-01\n\
         3,Carol,Williams,carol.williams@example.com,Engineering,98000,2020-01-10\n\
         4,Dan,Brown,dan.brown@example.com,Marketing,71000,2022-06-20\n\
         5,Eve,Davis,eve.davis@example.com,Engineering,105000,2018-11-05\n",
    )?;

    println!("Initialized {}", work_dir.display());
    Ok(())
}

fn cmd_block_create(config: &Config) -> Result<()> {
    let hash = Block::create(config)?;
    println!("{}", hash);
    Ok(())
}

fn cmd_patch_create(config: &Config, reference: Option<&str>, n: Option<u32>) -> Result<()> {
    let hash = match (reference, n) {
        (None, None) => leech2::reported::load(&config.work_dir)?
            .unwrap_or_else(|| leech2::utils::GENESIS_HASH.to_string()),
        _ => resolve_ref(config, reference, n)?,
    };
    let patch = leech2::patch::Patch::create(config, &hash)?;

    let buf = leech2::wire::encode_patch(config, &patch)?;
    leech2::storage::store(&config.work_dir, PATCH_FILE, &buf)?;

    println!("{}", patch);
    Ok(())
}

fn cmd_log(config: &Config) -> Result<String> {
    let work_dir = &config.work_dir;
    let mut hash = leech2::head::load(work_dir)?;

    if hash == GENESIS_HASH {
        bail!("no blocks exist yet");
    }

    let mut output = String::new();
    loop {
        let block = Block::load(work_dir, &hash)?;

        let timestamp = block
            .created
            .as_ref()
            .map(format_timestamp)
            .unwrap_or_else(|| "N/A".to_string());

        let table_names: Vec<&str> = block
            .payload
            .iter()
            .map(|d| d.table_name.as_str())
            .collect();
        let tables_str = if table_names.is_empty() {
            "no changes".to_string()
        } else {
            table_names.join(", ")
        };

        output.push_str(&format!(
            "block {}  {}  ({} deltas: {})\n",
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

fn cmd_patch_show(config: &Config) -> Result<String> {
    let data = leech2::storage::load(&config.work_dir, PATCH_FILE)?
        .context("no patch file found, run `lch patch create` first")?;

    let patch = leech2::wire::decode_patch(&data)?;
    Ok(format!("{}", patch))
}

fn cmd_patch_sql(config: &Config) -> Result<String> {
    let data = leech2::storage::load(&config.work_dir, PATCH_FILE)?
        .context("no patch file found, run `lch patch create` first")?;

    let patch = leech2::wire::decode_patch(&data)?;
    match leech2::sql::patch_to_sql(config, &patch)? {
        Some(sql) => Ok(sql),
        None => Ok("-- no changes\n".to_string()),
    }
}

fn cmd_patch_applied(config: &Config) -> Result<()> {
    let data = leech2::storage::load(&config.work_dir, PATCH_FILE)?
        .context("no patch file found, run `lch patch create` first")?;

    let patch = leech2::wire::decode_patch(&data)?;
    leech2::reported::save(&config.work_dir, &patch.head_hash)?;

    println!("{}", patch.head_hash);
    Ok(())
}

fn print_with_pager(content: &str) {
    let pager_cmd = std::env::var("PAGER").unwrap_or_else(|_| "less".to_string());

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
            PatchCmd::Applied => {
                cmd_patch_applied(&config)?;
            }
        },
        Cmd::Log => {
            let output = cmd_log(&config)?;
            print_with_pager(&output);
        }
    }

    Ok(())
}

fn main() -> ExitCode {
    env_logger::init();

    let cli = Cli::parse();

    if let Err(e) = run(cli) {
        eprintln!("error: {:#}", e);
        return ExitCode::FAILURE;
    }

    ExitCode::SUCCESS
}
