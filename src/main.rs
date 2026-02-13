use std::io::Write;
use std::path::PathBuf;
use std::process::{Command as ProcessCommand, ExitCode, Stdio};

use clap::{Parser, Subcommand};
use prost::Message;

use leech2::block::Block;
use leech2::utils::{format_timestamp, GENESIS_HASH};

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
    /// Create a block or patch
    Create {
        #[command(subcommand)]
        command: CreateCmd,
    },
    /// Show a block, patch, or SQL
    Show {
        #[command(subcommand)]
        command: ShowCmd,
    },
    /// List all blocks from HEAD to genesis
    Log,
}

#[derive(Subcommand)]
enum CreateCmd {
    /// Create a new block from current CSV state
    Block,
    /// Create a patch from REF to HEAD and write to .leech2/PATCH
    Patch {
        /// Block hash prefix
        #[arg(name = "REF", required_unless_present = "n")]
        reference: Option<String>,
        /// Create a patch covering the last N blocks
        #[arg(short)]
        n: Option<u32>,
    },
}

#[derive(Subcommand)]
enum ShowCmd {
    /// Show the full contents of a block
    Block {
        /// Block hash prefix [default: HEAD]
        #[arg(name = "REF")]
        reference: Option<String>,
        /// Show the block N steps back from HEAD
        #[arg(short)]
        n: Option<u32>,
    },
    /// Show the contents of the .leech2/PATCH file
    Patch,
    /// Convert the .leech2/PATCH file to SQL
    Sql,
}

fn work_dir(cli: &Cli) -> PathBuf {
    let base = cli.directory.clone().unwrap_or_else(|| PathBuf::from("."));
    base.join(LEECH2_DIR)
}

fn resolve_ref(reference: Option<&str>, n: Option<u32>) -> Result<String, Box<dyn std::error::Error>> {
    match (reference, n) {
        (Some(_), Some(_)) => Err("cannot specify both a hash prefix and -n".into()),
        (Some(r), None) => leech2::patch::resolve_hash_prefix(r),
        (None, Some(n)) => walk_back(n),
        (None, None) => leech2::head::load(),
    }
}

fn walk_back(n: u32) -> Result<String, Box<dyn std::error::Error>> {
    let mut hash = leech2::head::load()?;
    for i in 0..n {
        if hash == GENESIS_HASH {
            return Err(format!(
                "only {} block(s) in chain, cannot go back {}",
                i, n
            )
            .into());
        }
        let block = Block::load(&hash)?;
        hash = block.parent;
    }
    Ok(hash)
}

fn cmd_init(work_dir: &std::path::Path) -> Result<(), Box<dyn std::error::Error>> {
    if work_dir.join("config.toml").exists() {
        return Err(format!(
            "already initialized: {} exists",
            work_dir.join("config.toml").display()
        )
        .into());
    }

    std::fs::create_dir_all(work_dir)?;

    std::fs::write(
        work_dir.join("config.toml"),
        r#"[tables.example]
source = "example.csv"
header = true

[[tables.example.fields]]
name = "id"
type = "INTEGER"
primary-key = true

[[tables.example.fields]]
name = "name"
type = "TEXT"
"#,
    )?;

    std::fs::write(work_dir.join("example.csv"), "id,name\n1,Alice\n2,Bob\n")?;

    println!("Initialized {}", work_dir.display());
    Ok(())
}

fn cmd_create_block() -> Result<(), Box<dyn std::error::Error>> {
    let hash = Block::create()?;
    println!("{}", hash);
    Ok(())
}

fn cmd_create_patch(reference: Option<&str>, n: Option<u32>) -> Result<(), Box<dyn std::error::Error>> {
    let hash = resolve_ref(reference, n)?;
    let patch = leech2::patch::Patch::create(&hash)?;

    let mut buf = Vec::new();
    patch.encode(&mut buf)?;
    leech2::storage::save(PATCH_FILE, &buf)?;

    println!("{}", patch);
    Ok(())
}

fn cmd_log() -> Result<String, Box<dyn std::error::Error>> {
    let mut hash = leech2::head::load()?;

    if hash == GENESIS_HASH {
        return Err("no blocks exist yet".into());
    }

    let mut output = String::new();
    loop {
        let block = Block::load(&hash)?;

        let timestamp = block
            .created
            .as_ref()
            .map(|ts| format_timestamp(ts))
            .unwrap_or_else(|| "N/A".to_string());

        let table_names: Vec<&str> = block.payload.iter().map(|d| d.name.as_str()).collect();
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

fn cmd_show_block(reference: Option<&str>, n: Option<u32>) -> Result<String, Box<dyn std::error::Error>> {
    let hash = resolve_ref(reference, n)?;
    if hash == GENESIS_HASH {
        return Err("cannot show the genesis block".into());
    }
    let block = Block::load(&hash)?;
    Ok(format!("block {}\n{}", hash, block))
}

fn cmd_show_patch() -> Result<String, Box<dyn std::error::Error>> {
    let data = leech2::storage::load(PATCH_FILE)?
        .ok_or("no patch file found, run `lch create patch` first")?;

    let patch = leech2::patch::Patch::decode(data.as_slice())?;
    Ok(format!("{}", patch))
}

fn cmd_show_sql() -> Result<String, Box<dyn std::error::Error>> {
    let data = leech2::storage::load(PATCH_FILE)?
        .ok_or("no patch file found, run `lch create patch` first")?;

    match leech2::sql::patch_to_sql(&data)? {
        Some(sql) => Ok(sql),
        None => Ok("-- no changes\n".to_string()),
    }
}

fn print_with_pager(content: &str) {
    let pager_cmd = std::env::var("PAGER").unwrap_or_else(|_| "less".to_string());

    let mut child = match ProcessCommand::new(&pager_cmd).stdin(Stdio::piped()).spawn() {
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

fn run(cli: Cli) -> Result<(), Box<dyn std::error::Error>> {
    let work_dir = work_dir(&cli);

    if let Cmd::Init = &cli.command {
        return cmd_init(&work_dir);
    }

    leech2::config::Config::init(&work_dir)?;

    match &cli.command {
        Cmd::Init => unreachable!(),
        Cmd::Create { command } => match command {
            CreateCmd::Block => cmd_create_block()?,
            CreateCmd::Patch { reference, n } => {
                cmd_create_patch(reference.as_deref(), *n)?;
            }
        },
        Cmd::Show { command } => {
            let output = match command {
                ShowCmd::Block { reference, n } => cmd_show_block(reference.as_deref(), *n)?,
                ShowCmd::Patch => cmd_show_patch()?,
                ShowCmd::Sql => cmd_show_sql()?,
            };
            print_with_pager(&output);
        }
        Cmd::Log => {
            let output = cmd_log()?;
            print_with_pager(&output);
        }
    }

    Ok(())
}

fn main() -> ExitCode {
    env_logger::init();

    let cli = Cli::parse();

    if let Err(e) = run(cli) {
        eprintln!("error: {}", e);
        return ExitCode::FAILURE;
    }

    ExitCode::SUCCESS
}
