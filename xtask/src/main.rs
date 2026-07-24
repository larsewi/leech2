//! Repo automation for leech2.
//!
//! Currently a single task, `generate-man-pages`, which regenerates the man
//! pages. Run via the cargo alias (see `.cargo/config.toml`):
//!
//! ```text
//! cargo xtask generate-man-pages target/release/man
//! ```
//!
//! `lch.1` is rendered from the clap CLI definition so it never drifts from
//! the binary; the `libleech2` pages are produced by doxygen from the doc
//! comments in `include/leech2.h` so they never drift from the C API. This
//! lives in a release-only crate so doxygen and `clap_mangen` stay out of the
//! everyday `cargo build` loop while every release still ships up-to-date man
//! pages.

use std::path::{Path, PathBuf};
use std::process::Command;

use anyhow::{Context, Result, bail};
use clap::CommandFactory;

// Share the exact clap definition the `lch` binary parses.
#[path = "../../src/cli.rs"]
mod cli;

fn main() -> Result<()> {
    let mut arguments = std::env::args().skip(1);
    match arguments.next().as_deref() {
        Some("generate-man-pages") => {
            let output_dir: PathBuf = arguments
                .next()
                .context("usage: cargo xtask generate-man-pages <output-dir>")?
                .into();
            generate_man_pages(&output_dir)
        }
        Some(other) => bail!("unknown task '{other}'; available tasks: generate-man-pages"),
        None => bail!("usage: cargo xtask <task>; available tasks: generate-man-pages"),
    }
}

fn generate_man_pages(output_dir: &Path) -> Result<()> {
    std::fs::create_dir_all(output_dir)
        .with_context(|| format!("failed to create '{}'", output_dir.display()))?;

    // CARGO_MANIFEST_DIR points at the xtask crate; the repo root is its parent.
    let repo_root = Path::new(env!("CARGO_MANIFEST_DIR"))
        .parent()
        .context("xtask has no parent directory")?;
    let version = leech2_version(repo_root)?;
    let date = last_commit_date(repo_root);

    generate_cli_man_pages(output_dir, &version, &date)?;
    generate_c_api_man_pages(repo_root, output_dir, &version)?;

    Ok(())
}

/// Render `lch.1` and one page per subcommand (`lch-block-create.1`, ...) from
/// the clap command tree, so `man lch-block-create` works after install.
fn generate_cli_man_pages(output_dir: &Path, version: &str, date: &str) -> Result<()> {
    // cli.rs is compiled into this crate, so `#[command(version)]` would report
    // xtask's placeholder version; override it with leech2's, and propagate it
    // so the subcommand pages agree.
    // clap's `.version()` wants a `&'static str`; leaking is fine in this
    // short-lived one-shot process.
    let version_static: &'static str = version.to_string().leak();
    let command = cli::Cli::command()
        .version(version_static)
        .propagate_version(true);

    let temp_dir = output_dir.join(".clap-tmp");
    if temp_dir.exists() {
        std::fs::remove_dir_all(&temp_dir)?;
    }
    std::fs::create_dir_all(&temp_dir)?;
    clap_mangen::generate_to(command, &temp_dir).context("failed to render lch man pages")?;

    let pattern = temp_dir.join("*.1");
    let pattern = pattern.to_str().context("non-UTF-8 clap output path")?;
    let mut count = 0;
    for entry in glob::glob(pattern)? {
        let source = entry?;
        let name = source
            .file_name()
            .context("clap man page has no file name")?;
        let stem = name.to_string_lossy();
        let stem = stem.strip_suffix(".1").unwrap_or(&stem);

        // clap_mangen leaves the date/source/manual blank; give every page a
        // consistent .TH derived from its file name (e.g. lch-block-create).
        let title = format!(
            ".TH \"{}\" \"1\" \"{date}\" \"leech2 {version}\" \"User Commands\"",
            stem.to_uppercase()
        );
        let content = set_title(&std::fs::read_to_string(&source)?, &title);

        std::fs::write(output_dir.join(name), content)
            .with_context(|| format!("failed to write '{}'", output_dir.join(name).display()))?;
        count += 1;
    }

    std::fs::remove_dir_all(&temp_dir)?;
    println!("Wrote {count} lch man page(s) to {}", output_dir.display());
    Ok(())
}

/// Produce the `libleech2` man pages by running doxygen over the public
/// header, then copying the generated pages into `output_dir`. Doxygen emits
/// the header's file page (documenting every function) plus one page per
/// documented struct. Its directory-reference page is skipped: it carries no
/// API documentation and is named after the local build path.
fn generate_c_api_man_pages(repo_root: &Path, output_dir: &Path, version: &str) -> Result<()> {
    let temp_dir = output_dir.join(".doxygen-tmp");
    if temp_dir.exists() {
        std::fs::remove_dir_all(&temp_dir)?;
    }
    std::fs::create_dir_all(&temp_dir)?;

    let status = Command::new("doxygen")
        .arg("Doxyfile")
        .current_dir(repo_root)
        .env("DOXYGEN_OUTPUT_DIR", &temp_dir)
        .env("LEECH2_VERSION", version)
        .status()
        .context("failed to run doxygen; is it installed?")?;
    if !status.success() {
        bail!("doxygen exited with status {status}");
    }

    let pattern = temp_dir.join("**").join("*.3");
    let pattern = pattern.to_str().context("non-UTF-8 doxygen output path")?;
    let mut count = 0;
    for entry in glob::glob(pattern)? {
        let source = entry?;
        let content = std::fs::read_to_string(&source)
            .with_context(|| format!("failed to read '{}'", source.display()))?;
        // Doxygen labels its directory-reference page's .TH title accordingly;
        // it documents no API, so drop it.
        if content
            .lines()
            .any(|line| line.starts_with(".TH") && line.contains("Directory Reference"))
        {
            continue;
        }
        let name = source
            .file_name()
            .context("doxygen man page has no file name")?;
        std::fs::write(output_dir.join(name), &content)
            .with_context(|| format!("failed to write '{}'", output_dir.join(name).display()))?;
        count += 1;
    }
    if count == 0 {
        bail!("doxygen produced no man pages");
    }

    std::fs::remove_dir_all(&temp_dir)?;
    println!(
        "Wrote {count} libleech2 man page(s) to {}",
        output_dir.display()
    );
    Ok(())
}

/// Replace the `.TH` line of a roff document with `title`, leaving the rest
/// (including any preamble before `.TH`) untouched.
fn set_title(roff: &str, title: &str) -> String {
    let mut out = String::with_capacity(roff.len());
    for line in roff.lines() {
        if line.starts_with(".TH") {
            out.push_str(title);
        } else {
            out.push_str(line);
        }
        out.push('\n');
    }
    out
}

/// Read the `leech2` package version from the workspace `Cargo.toml`. xtask's
/// own `CARGO_PKG_VERSION` is a placeholder, so the shipped version has to come
/// from the root manifest.
fn leech2_version(repo_root: &Path) -> Result<String> {
    let manifest = repo_root.join("Cargo.toml");
    let text = std::fs::read_to_string(&manifest)
        .with_context(|| format!("failed to read '{}'", manifest.display()))?;
    for line in text.lines() {
        if let Some(rest) = line.strip_prefix("version = \"")
            && let Some(version) = rest.strip_suffix('"')
        {
            return Ok(version.to_string());
        }
    }
    bail!("no version found in {}", manifest.display())
}

/// The last commit's author date, so the man page date matches the release
/// rather than whenever the generator happened to run.
fn last_commit_date(repo_root: &Path) -> String {
    Command::new("git")
        .args(["log", "-1", "--format=%cs"])
        .current_dir(repo_root)
        .output()
        .ok()
        .filter(|output| output.status.success())
        .and_then(|output| String::from_utf8(output.stdout).ok())
        .map(|text| text.trim().to_string())
        .filter(|text| !text.is_empty())
        .unwrap_or_else(|| "unknown".to_string())
}

#[cfg(test)]
mod tests {
    use super::*;

    fn doxygen_available() -> bool {
        Command::new("doxygen")
            .arg("--version")
            .output()
            .is_ok_and(|output| output.status.success())
    }

    // The clap-derived pages need no external tools, so this always runs.
    #[test]
    fn renders_cli_man_pages() -> Result<()> {
        let out = tempfile::tempdir()?;
        generate_cli_man_pages(out.path(), "9.9.9", "2020-01-01")?;

        let root = std::fs::read_to_string(out.path().join("lch.1"))?;
        assert!(root.contains(".TH \"LCH\" \"1\""), "unexpected .TH: {root}");
        assert!(root.contains("leech2 9.9.9"), "version not stamped");
        assert!(
            out.path().join("lch-block-create.1").exists(),
            "per-subcommand page not generated"
        );
        Ok(())
    }

    // Runs the full pipeline (needs doxygen; skipped when absent, as CI installs
    // it -- see build.yml). Because the Doxyfile sets WARN_AS_ERROR alongside
    // WARN_IF_UNDOCUMENTED, generation fails if any symbol in leech2.h is
    // undocumented, so this guards documentation completeness too.
    #[test]
    fn generates_all_man_pages() -> Result<()> {
        if !doxygen_available() {
            eprintln!("skipping generates_all_man_pages: doxygen not installed");
            return Ok(());
        }
        let out = tempfile::tempdir()?;
        generate_man_pages(out.path())?;
        for page in ["lch.1", "lch-patch-create.1", "leech2.h.3", "lch_cell_t.3"] {
            assert!(
                out.path().join(page).exists(),
                "man page not generated: {page}"
            );
        }
        Ok(())
    }
}
