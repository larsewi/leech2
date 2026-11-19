//! The `leech2.pc` pkg-config files shipped in the `.deb` and `.rpm` packages.
//!
//! Both are rendered from `leech2.pc.in`. They differ only in the library
//! directory: Debian installs into `/usr/lib`, RPM into `/usr/lib64`.

use std::path::Path;

use anyhow::{Context, Result};

use crate::leech2_version;

/// Write `leech2-deb.pc` and `leech2-rpm.pc` into `output_dir`, which is
/// created if missing.
pub fn generate(repo_root: &Path, output_dir: &Path) -> Result<()> {
    std::fs::create_dir_all(output_dir)
        .with_context(|| format!("failed to create '{}'", output_dir.display()))?;

    let template_path = repo_root.join("leech2.pc.in");
    let template = std::fs::read_to_string(&template_path)
        .with_context(|| format!("failed to read '{}'", template_path.display()))?;
    let version = leech2_version(repo_root)?;

    for (name, libdir) in [("leech2-deb.pc", "lib"), ("leech2-rpm.pc", "lib64")] {
        let content = template
            .replace("@VERSION@", &version)
            .replace("@PREFIX@", "/usr")
            .replace("@LIBDIR@", libdir);
        let output_path = output_dir.join(name);
        std::fs::write(&output_path, content)
            .with_context(|| format!("failed to write '{}'", output_path.display()))?;
        println!("Wrote {}", output_path.display());
    }
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn generates_pkg_config_files() -> Result<()> {
        let out = tempfile::tempdir()?;
        generate(crate::repo_root()?, out.path())?;

        let deb = std::fs::read_to_string(out.path().join("leech2-deb.pc"))?;
        assert!(deb.contains("libdir=${exec_prefix}/lib\n"), "{deb}");
        let rpm = std::fs::read_to_string(out.path().join("leech2-rpm.pc"))?;
        assert!(rpm.contains("libdir=${exec_prefix}/lib64\n"), "{rpm}");
        for content in [&deb, &rpm] {
            assert!(!content.contains('@'), "unfilled placeholder: {content}");
        }
        Ok(())
    }
}
