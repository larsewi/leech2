//! Repo automation for leech2. Run via the cargo alias (see
//! `.cargo/config.toml`), and pass `--help` for the full usage:
//!
//! ```text
//! cargo xtask generate-man-pages target/release/man
//! cargo xtask changelog-dependencies --since v5.4.3
//! ```
//!
//! This lives in a release-only crate so tooling dependencies such as
//! `clap_mangen` stay out of the everyday `cargo build` loop. Each task owns a
//! module; this file only parses arguments and dispatches.

use std::path::{Path, PathBuf};

use anyhow::{Context, Result};
use clap::{Parser, Subcommand};

mod dependencies;
mod man_pages;

// Share the exact clap definition the `lch` binary parses.
#[path = "../../src/cli.rs"]
mod cli;

/// Repo automation for leech2, invoked through the `cargo xtask` alias.
#[derive(Parser)]
// The only entry point is the `cargo xtask` alias, so spell that in the usage
// line rather than letting clap infer the bare binary name from argv[0].
#[command(name = "cargo xtask", bin_name = "cargo xtask", about, long_about = None)]
struct Xtask {
    #[command(subcommand)]
    task: Task,
}

#[derive(Subcommand)]
enum Task {
    /// Regenerate the `lch` and `libleech2` man pages.
    GenerateManPages {
        /// Directory to write the man pages to; created if missing.
        output_dir: PathBuf,
    },
    /// Print release-notes lines for the direct dependencies updated since a
    /// release tag.
    ChangelogDependencies {
        /// Release tag to compare against, e.g. `v5.4.3`.
        #[arg(long)]
        since: String,
    },
}

fn main() -> Result<()> {
    match Xtask::parse().task {
        Task::GenerateManPages { output_dir } => man_pages::generate(repo_root()?, &output_dir),
        Task::ChangelogDependencies { since } => {
            dependencies::changelog_dependencies(repo_root()?, &since)
        }
    }
}

/// The repo root, which is the parent of the xtask crate `CARGO_MANIFEST_DIR`
/// points at.
fn repo_root() -> Result<&'static Path> {
    Path::new(env!("CARGO_MANIFEST_DIR"))
        .parent()
        .context("xtask has no parent directory")
}

#[cfg(test)]
mod tests {
    use clap::CommandFactory;

    use super::*;

    // clap's own consistency check for a derived command tree (conflicting
    // names, bad defaults, ...), which otherwise only trips at runtime.
    #[test]
    fn xtask_cli_is_well_formed() {
        Xtask::command().debug_assert();
    }
}
