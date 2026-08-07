//! Release-notes lines for the direct dependencies updated since the previous
//! release.
//!
//! GitHub's generated notes carry one line per pull request, so a dependency
//! bumped twice in a release window shows up twice, each time under whatever
//! the pull request happened to be titled. Diffing the lockfile at the previous
//! tag against the one being released sidesteps that: one line per dependency,
//! from the version that shipped last time to the version shipping now, with
//! no pull request to name.

use std::collections::{BTreeMap, BTreeSet};
use std::path::Path;
use std::process::Command;

use anyhow::{Context, Result, bail};
use semver::{Version, VersionReq};
use serde::Deserialize;

/// The `[[package]]` entries of a `Cargo.lock`.
#[derive(Deserialize)]
struct Lockfile {
    /// Every package in the resolved graph, direct and transitive alike.
    #[serde(default)]
    package: Vec<LockedPackage>,
}

/// One resolved package in a `Cargo.lock`.
#[derive(Deserialize)]
struct LockedPackage {
    /// Crate name as published.
    name: String,
    /// Exact resolved version.
    version: String,
}

/// The `Cargo.toml` tables holding the dependencies that ship with the crate.
/// `dev-dependencies` are deliberately absent: they never reach a consumer.
#[derive(Deserialize)]
struct Manifest {
    /// The `[dependencies]` table, keyed by dependency name.
    #[serde(default)]
    dependencies: BTreeMap<String, toml::Value>,
    /// The `[build-dependencies]` table, keyed by dependency name.
    #[serde(default, rename = "build-dependencies")]
    build_dependencies: BTreeMap<String, toml::Value>,
}

/// Direct dependencies of the shipped crate, mapping published crate name to
/// the version requirement declared for it. The requirement is `None` for a
/// dependency declared without one, such as a path or git dependency.
type DirectDependencies = BTreeMap<String, Option<VersionReq>>;

/// Print one release-notes line per direct dependency whose resolved version
/// changed since `previous_tag`. Prints nothing when none of them moved.
pub fn changelog_dependencies(repo_root: &Path, previous_tag: &str) -> Result<()> {
    let previous_manifest = git_show(repo_root, &format!("{previous_tag}:Cargo.toml"))?;
    let previous_lockfile = git_show(repo_root, &format!("{previous_tag}:Cargo.lock"))?;

    let current_manifest_path = repo_root.join("Cargo.toml");
    let current_manifest = std::fs::read_to_string(&current_manifest_path)
        .with_context(|| format!("failed to read '{}'", current_manifest_path.display()))?;
    let current_lockfile_path = repo_root.join("Cargo.lock");
    let current_lockfile = std::fs::read_to_string(&current_lockfile_path)
        .with_context(|| format!("failed to read '{}'", current_lockfile_path.display()))?;

    let lines = summarize(
        &previous_manifest,
        &previous_lockfile,
        &current_manifest,
        &current_lockfile,
    )?;
    for line in lines {
        println!("{line}");
    }

    Ok(())
}

/// Build one release-notes line per direct dependency whose resolved version
/// changed between the two releases. Each side is read through its own
/// manifest, so a dependency added or dropped along the way is reported as
/// such even when it lingers in the lockfile as a transitive dependency.
fn summarize(
    previous_manifest: &str,
    previous_lockfile: &str,
    current_manifest: &str,
    current_lockfile: &str,
) -> Result<Vec<String>> {
    let previous = locked_versions(previous_lockfile, &direct_dependencies(previous_manifest)?)?;
    let current = locked_versions(current_lockfile, &direct_dependencies(current_manifest)?)?;

    let names: BTreeSet<&String> = previous.keys().chain(current.keys()).collect();
    let mut lines = Vec::new();
    for name in names {
        match (previous.get(name), current.get(name)) {
            (Some(before), Some(after)) if before != after => lines.push(format!(
                "- Updated dependency {name} from {} to {}",
                format_versions(before),
                format_versions(after)
            )),
            (None, Some(after)) => lines.push(format!(
                "- Added dependency {name} {}",
                format_versions(after)
            )),
            (Some(_), None) => lines.push(format!("- Removed dependency {name}")),
            _ => {}
        }
    }

    Ok(lines)
}

/// Read the direct dependencies out of a `Cargo.toml`.
fn direct_dependencies(manifest: &str) -> Result<DirectDependencies> {
    let manifest: Manifest = toml::from_str(manifest).context("failed to parse Cargo.toml")?;

    let mut direct = DirectDependencies::new();
    for (key, spec) in manifest
        .dependencies
        .iter()
        .chain(&manifest.build_dependencies)
    {
        // A renamed dependency keys the table by its local name and carries the
        // published name in `package`; the lockfile only knows the latter.
        let name = spec
            .get("package")
            .and_then(toml::Value::as_str)
            .unwrap_or(key);
        // Either `anyhow = "1"` or `anyhow = { version = "1", ... }`.
        let raw = spec
            .get("version")
            .and_then(toml::Value::as_str)
            .or_else(|| spec.as_str());
        let requirement = match raw {
            Some(raw) => Some(VersionReq::parse(raw).with_context(|| {
                format!("invalid version requirement for dependency '{name}': {raw}")
            })?),
            None => None,
        };
        direct.insert(name.to_string(), requirement);
    }

    Ok(direct)
}

/// Resolve each direct dependency to the version(s) the lockfile pins it at,
/// dropping the transitive packages that make up the rest of the lockfile.
///
/// A crate can appear in a lockfile at several incompatible versions, because
/// something else in the graph asks for an older major. Only the versions
/// satisfying the declared requirement are ours, so the others are skipped.
fn locked_versions(
    lockfile: &str,
    direct: &DirectDependencies,
) -> Result<BTreeMap<String, BTreeSet<String>>> {
    let lockfile: Lockfile = toml::from_str(lockfile).context("failed to parse Cargo.lock")?;

    let mut versions: BTreeMap<String, BTreeSet<String>> = BTreeMap::new();
    for package in lockfile.package {
        let Some(requirement) = direct.get(&package.name) else {
            continue;
        };
        if let Some(requirement) = requirement {
            let version = Version::parse(&package.version).with_context(|| {
                format!(
                    "invalid version for package '{}': {}",
                    package.name, package.version
                )
            })?;
            if !requirement.matches(&version) {
                continue;
            }
        }
        versions
            .entry(package.name)
            .or_default()
            .insert(package.version);
    }

    Ok(versions)
}

/// Render a dependency's resolved versions, comma separated in the rare case
/// where it is locked at more than one.
fn format_versions(versions: &BTreeSet<String>) -> String {
    let mut formatted = String::new();
    for version in versions {
        if !formatted.is_empty() {
            formatted.push_str(", ");
        }
        formatted.push_str(version);
    }
    formatted
}

/// Run `git show <revision>` in `repo_root`, e.g. `v5.4.3:Cargo.lock`.
fn git_show(repo_root: &Path, revision: &str) -> Result<String> {
    let output = Command::new("git")
        .args(["show", revision])
        .current_dir(repo_root)
        .output()
        .with_context(|| format!("failed to run 'git show {revision}'"))?;
    if !output.status.success() {
        bail!(
            "'git show {revision}' failed: {}",
            String::from_utf8_lossy(&output.stderr).trim()
        );
    }
    String::from_utf8(output.stdout)
        .with_context(|| format!("'git show {revision}' produced non-UTF-8 output"))
}

#[cfg(test)]
mod tests {
    use super::*;

    const MANIFEST: &str = r#"
[package]
name = "leech2"

[dependencies]
anyhow = "1.0.102"
clap = { version = "4", features = ["derive"] }

[dev-dependencies]
tempfile = "3"

[build-dependencies]
prost-build = "0.14"
"#;

    fn lockfile(packages: &[(&str, &str)]) -> String {
        let mut lockfile = String::from("version = 4\n");
        for (name, version) in packages {
            lockfile.push_str(&format!(
                "\n[[package]]\nname = \"{name}\"\nversion = \"{version}\"\n"
            ));
        }
        lockfile
    }

    #[test]
    fn direct_dependencies_covers_shipped_tables_only() {
        let direct = direct_dependencies(MANIFEST).unwrap();
        let names: Vec<&String> = direct.keys().collect();
        assert_eq!(names, ["anyhow", "clap", "prost-build"]);
    }

    #[test]
    fn direct_dependencies_resolves_renamed_dependency() {
        let manifest = r#"
[dependencies]
renamed = { package = "anyhow", version = "1" }
"#;
        let direct = direct_dependencies(manifest).unwrap();
        assert!(direct.contains_key("anyhow"), "got: {:?}", direct.keys());
    }

    #[test]
    fn direct_dependencies_accepts_dependency_without_requirement() {
        let manifest = r#"
[dependencies]
local = { path = "../local" }
"#;
        let direct = direct_dependencies(manifest).unwrap();
        assert_eq!(direct.get("local"), Some(&None));
    }

    #[test]
    fn summarize_reports_updated_dependency() {
        let previous = lockfile(&[("anyhow", "1.0.102"), ("clap", "4.6.5")]);
        let current = lockfile(&[("anyhow", "1.0.103"), ("clap", "4.6.5")]);

        let lines = summarize(MANIFEST, &previous, MANIFEST, &current).unwrap();
        assert_eq!(
            lines,
            ["- Updated dependency anyhow from 1.0.102 to 1.0.103"]
        );
    }

    #[test]
    fn summarize_ignores_transitive_dependency() {
        let previous = lockfile(&[("anyhow", "1.0.102"), ("winnow", "1.0.3")]);
        let current = lockfile(&[("anyhow", "1.0.102"), ("winnow", "1.0.4")]);

        let lines = summarize(MANIFEST, &previous, MANIFEST, &current).unwrap();
        assert!(lines.is_empty(), "got: {lines:?}");
    }

    #[test]
    fn summarize_ignores_dependency_locked_at_another_major() {
        // `toml` is required at 0.8, and something else in the graph pulls 1.1;
        // only the 0.8 line is ours, and it did not move.
        let manifest = r#"
[dependencies]
toml = "0.8"
"#;
        let previous = lockfile(&[("toml", "0.8.23"), ("toml", "1.1.3")]);
        let current = lockfile(&[("toml", "0.8.23"), ("toml", "1.1.4")]);

        let lines = summarize(manifest, &previous, manifest, &current).unwrap();
        assert!(lines.is_empty(), "got: {lines:?}");
    }

    #[test]
    fn summarize_collapses_repeated_updates_of_one_dependency() {
        // Two bumps in the same release window are a single line, oldest to
        // newest, because only the tag boundaries are compared.
        let manifest = r#"
[dependencies]
anyhow = "1"
"#;
        let previous = lockfile(&[("anyhow", "1.0.100")]);
        let current = lockfile(&[("anyhow", "1.0.103")]);

        let lines = summarize(manifest, &previous, manifest, &current).unwrap();
        assert_eq!(
            lines,
            ["- Updated dependency anyhow from 1.0.100 to 1.0.103"]
        );
    }

    #[test]
    fn summarize_reports_added_dependency() {
        let previous_manifest = r#"
[dependencies]
anyhow = "1"
"#;
        let previous = lockfile(&[("anyhow", "1.0.102")]);
        let current = lockfile(&[("anyhow", "1.0.102"), ("clap", "4.6.5")]);

        let lines = summarize(previous_manifest, &previous, MANIFEST, &current).unwrap();
        assert_eq!(lines, ["- Added dependency clap 4.6.5"]);
    }

    #[test]
    fn summarize_reports_dependency_dropped_to_transitive() {
        // Dropped from the manifest but still in the lockfile, because another
        // crate depends on it. It stopped being ours, so it reads as removed.
        let current_manifest = r#"
[dependencies]
anyhow = "1"
"#;
        let previous = lockfile(&[("anyhow", "1.0.102"), ("clap", "4.6.5")]);
        let current = lockfile(&[("anyhow", "1.0.102"), ("clap", "4.6.5")]);

        let lines = summarize(MANIFEST, &previous, current_manifest, &current).unwrap();
        assert_eq!(lines, ["- Removed dependency clap"]);
    }

    #[test]
    fn summarize_reports_major_version_change() {
        let previous_manifest = r#"
[dependencies]
rand = "0.8"
"#;
        let current_manifest = r#"
[dependencies]
rand = "0.9"
"#;
        let previous = lockfile(&[("rand", "0.8.5")]);
        let current = lockfile(&[("rand", "0.9.2")]);

        let lines = summarize(previous_manifest, &previous, current_manifest, &current).unwrap();
        assert_eq!(lines, ["- Updated dependency rand from 0.8.5 to 0.9.2"]);
    }

    #[test]
    fn format_versions_joins_multiple_versions() {
        let versions = BTreeSet::from(["1.0.0".to_string(), "2.0.0".to_string()]);
        assert_eq!(format_versions(&versions), "1.0.0, 2.0.0");
    }
}
