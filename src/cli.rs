//! Command-line interface definition for the `lch` binary.

use std::path::PathBuf;

use clap::{Parser, Subcommand};

#[derive(Parser)]
#[command(name = "lch", about = "leech2 CLI - track changes to tables", version)]
pub struct Cli {
    /// Run as if started in <path> instead of the current directory
    #[arg(short = 'C', global = true)]
    pub directory: Option<PathBuf>,

    /// Skip all disk writes; log "Would have ..." instead
    #[arg(long, global = true)]
    pub dry_run: bool,

    #[command(subcommand)]
    pub command: Cmd,
}

#[derive(Subcommand)]
pub enum Cmd {
    /// Initialize a new .leech2 work directory with an example table
    Init,
    /// Operate on blocks
    Block {
        #[command(subcommand)]
        command: BlockCmd,
    },
    /// Operate on patches
    Patch {
        #[command(subcommand)]
        command: PatchCmd,
    },
    /// Operate on the stats file
    Stats {
        #[command(subcommand)]
        command: StatsCmd,
    },
}

#[derive(Subcommand)]
pub enum BlockCmd {
    /// Create a new block from current state
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
pub enum PatchCmd {
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
        /// Kind: TEXT, NUMBER, or BOOLEAN
        #[arg(default_value = "TEXT")]
        kind: String,
    },
    /// Mark the current patch as applied (saves head hash to REPORTED)
    Applied,
    /// Mark the current patch as failed (removes REPORTED to force full state)
    Failed,
}

#[derive(Subcommand)]
pub enum StatsCmd {
    /// Summarize the stats file
    Show,
}
