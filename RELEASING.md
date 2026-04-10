# Releasing leech2

This project follows [Semantic Versioning](https://semver.org/). Given a
version **MAJOR.MINOR.PATCH**, increment the:

- **MAJOR** version for incompatible API changes
- **MINOR** version for new features
- **PATCH** version for bug fixes

## Steps

1. **Bump the version**

   Go to **Actions** → **Version** → **Run workflow** in the GitHub UI. Select
   which component to bump (major, minor, or patch) and click **Run workflow**.
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
   - `leech2_X.Y.Z-1_amd64.deb`
   - `leech2_X.Y.Z-1_arm64.deb`

   Update changelog.

## Retrying a failed release

If the workflow fails before creating the tag, fix the issue and re-run the
workflow with the same version.

If the workflow fails after the tag has been pushed, delete the tag before
retrying.
