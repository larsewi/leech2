# Releasing leech2

This project follows [Semantic Versioning](https://semver.org/). Given a
version **MAJOR.MINOR.PATCH**, increment the:

- **MAJOR** version for incompatible API changes
- **MINOR** version for new features
- **PATCH** version for bug fixes

## Pull request labels

Every pull request should carry exactly one of these labels. They drive both
the auto-generated release notes (see `.github/release.yml`) and the automatic
version bump (see `.github/scripts/bump-version.sh`):

- **`breaking`** — incompatible API change; triggers a **major** bump.
  Changes that the system self-heals from at runtime are not considered breaking.
- **`feature`** — new user-facing feature; triggers a **minor** bump
- **`bug`** — bug fix; triggers a **patch** bump
- **`chore`** — internal change (tooling, refactor, docs, CI); excluded from
  release notes and counted as a **patch** bump

When the **Version** workflow runs in `auto` mode, it inspects the labels of
every pull request merged since the previous `v*` tag. The highest-priority
label wins: `breaking` > `feature` > anything else.

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
   - Build release binaries for six targets:
     - Linux x86_64
     - Linux aarch64
     - macOS x86_64
     - macOS aarch64
     - Windows x86_64
     - Windows aarch64
   - Package `.deb`, `.rpm` and `.msi` files for Linux and Windows targets
   - Create `.tar.gz` or `.zip` archives for all targets
   - Create a source tarball (`cargo package`) for building from source
   - Run virus scan on all build artifacts
   - Create a GitHub Release with all artifacts attached

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

   Update changelog.
