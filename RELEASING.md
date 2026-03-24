# Releasing leech2

## Prerequisites

- Push access to the repository
- The `version` field in `Cargo.toml` must match the version you intend to release

## Steps

1. **Update the version in `Cargo.toml`**

   Edit the `version` field under `[package]` to the new version number:

   ```toml
   [package]
   name = "leech2"
   version = "X.Y.Z"
   ```

2. **Commit and merge the version bump**

   Create a branch, commit the version change, open a pull request, and merge
   it to `master`.

3. **Trigger the release workflow**

   Go to **Actions** → **Release** → **Run workflow** in the GitHub UI. Enter
   the version number (e.g., `0.2.0`) and click **Run workflow**.

4. **Wait for the workflow to complete**

   The workflow will:
   - Verify that the input version matches `Cargo.toml`
   - Build release binaries for four targets:
     - Linux x86_64
     - Linux aarch64
     - macOS x86_64
     - macOS aarch64
   - Package `.deb` and `.rpm` files for both Linux targets
   - Create `.tar.gz` archives for all targets
   - Tag the commit as `vX.Y.Z` and push the tag
   - Create a GitHub Release with all artifacts attached

5. **Verify the release**

   Check the [Releases](https://github.com/larsewi/leech2/releases) page and
   confirm that the release contains the expected artifacts:
   - `leech2-X.Y.Z-linux-x86_64.tar.gz`
   - `leech2-X.Y.Z-linux-aarch64.tar.gz`
   - `leech2-X.Y.Z-macos-x86_64.tar.gz`
   - `leech2-X.Y.Z-macos-aarch64.tar.gz`
   - `leech2_X.Y.Z-1_amd64.deb`
   - `leech2_X.Y.Z-1_arm64.deb`
   - `leech2-X.Y.Z-1.x86_64.rpm`
   - `leech2-X.Y.Z-1.aarch64.rpm`

## Retrying a failed release

If the workflow fails before creating the tag, fix the issue and re-run the
workflow with the same version.

If the workflow fails after the tag has been pushed, delete the tag before
retrying:

```bash
git tag -d vX.Y.Z
git push origin :refs/tags/vX.Y.Z
```

Then re-run the workflow.
