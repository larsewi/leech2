# Releasing leech2

This project follows [Semantic Versioning](https://semver.org/). Given a
version **MAJOR.MINOR.PATCH**, increment the:

- **MAJOR** version for incompatible API changes
- **MINOR** version for new features
- **PATCH** version for bug fixes

## Pull request labels

Every pull request should carry at most one of these labels. They drive both
the auto-generated release notes (see `.github/release.yml`) and the automatic
version bump (see `.github/scripts/bump-version.sh`):

- **`breaking`** — incompatible API change; triggers a **major** bump.
  Changes that the system self-heals from at runtime are not considered breaking.
- **`feature`** — new user-facing feature; triggers a **minor** bump
- **`bug`** — bug fix; triggers a **patch** bump
- **`chore`** — internal change (tooling, refactor, docs, CI); excluded from
  release notes and counted as a **patch** bump
- **no label** — user-visible change that is neither a new feature nor a bug
  fix, such as changed or removed behavior; listed under **Other Changes** in
  the release notes and counted as a **patch** bump

When the **Version** workflow runs in `auto` mode, it inspects the labels of
every pull request merged since the previous `v*` tag. The highest-priority
label wins: `breaking` > `feature` > anything else.

## Dependency updates

Dependency bumps get their own **Dependencies** section in the release
notes, one line per direct dependency:

```markdown
## Dependencies

- Updated dependency anyhow from 1.0.102 to 1.0.104
- Added dependency glob 0.3.4
- Removed dependency chrono
```

The section is not built from pull requests. The **Release** workflow runs

```sh
cargo xtask changelog-dependencies --since v5.4.3
```

which diffs `Cargo.lock` at the previous release tag against the one being
released, keeping the packages that `Cargo.toml` declares under `[dependencies]`
or `[build-dependencies]`. Comparing the two tag boundaries means a dependency
bumped several times in one release window collapses into a single line, from
the version that shipped last time to the version shipping now. Transitive
packages and `dev-dependencies` are left out, as is a crate the lockfile pins at
a major other than the one declared.

## Steps

1. **Bump the version**

   Go to **Actions** → **Version** → **Run workflow** in the GitHub UI. Leave
   the component on `auto` to derive the bump from PR labels merged since the
   last tag, or pick `major`/`minor`/`patch` explicitly. Click **Run workflow**.
   This will create a pull request with the version bump in `Cargo.toml` and
   `Cargo.lock`. Review and merge the pull request.

2. **Trigger the release workflow**

   Go to **Actions** → **Release** → **Run workflow** in the GitHub UI. Select
   the branch to release from and click **Run workflow**.

3. **Wait for the workflow to complete**

   The workflow will:
   - Read the version from `Cargo.toml`
   - Generate the man pages (`cargo xtask generate-man-pages`) for the `.deb`
     and `.rpm` packages and the source tarball
   - Build release binaries for six targets:
     - Linux x86_64
     - Linux aarch64
     - macOS x86_64
     - macOS aarch64
     - Windows x86_64
     - Windows aarch64
   - Package `.deb`, `.rpm` and `.msi` files for Linux and Windows targets
   - Create `.tar.gz` or `.zip` archives for all targets
   - Create a source tarball (`cargo package`, with the man pages added under
     `man/`) for building from source
   - Run virus scan on all build artifacts
   - Generate a `checksums.txt` with the SHA-256 sum of every artifact
   - Create a GitHub Release with all artifacts attached
   - Add a **Dependencies** section to the release notes (see
     [Dependency updates](#dependency-updates))

4. **Verify the release**

   Check the [Releases](https://github.com/larsewi/leech2/releases) page and
   confirm that the release contains the expected artifacts:
   - `leech2-X.Y.Z-1.aarch64.rpm`
   - `leech2-X.Y.Z-1.x86_64.rpm`
   - `leech2-X.Y.Z-linux-aarch64.tar.gz`
   - `leech2-X.Y.Z-linux-x86_64.tar.gz`
   - `leech2-X.Y.Z-macos-aarch64.tar.gz`
   - `leech2-X.Y.Z-macos-x86_64.tar.gz`
   - `leech2-X.Y.Z-windows-aarch64.zip`
   - `leech2-X.Y.Z-windows-x86_64.zip`
   - `leech2-X.Y.Z-x86_64.msi`
   - `leech2-X.Y.Z.tar.gz` (source)
   - `leech2_X.Y.Z-1_amd64.deb`
   - `leech2_X.Y.Z-1_arm64.deb`
   - `checksums.txt`

   Update changelog.
