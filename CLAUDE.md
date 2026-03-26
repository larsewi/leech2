# CLAUDE.md

This file provides guidance to Claude Code (claude.ai/code) when working with code in this repository.

## Build & Test Commands

**Prerequisite:** `protobuf-compiler` must be installed (`sudo apt install protobuf-compiler` / `brew install protobuf`).

- **Build the Rust library:** `cargo build`
- **Run Rust tests:** `cargo build && cargo test` (the C FFI test links against the cdylib built by `cargo build`; without it, the linker may fail with undefined references)
- **Run a single test:** `cargo test <test_name>` (e.g. `cargo test test_merge_rule5`)
- **Format code:** `cargo fmt`, `clang-format -i`
- **Lint:** `cargo clippy`

## Workflow

- **Never commit directly to `master`.** When starting a new piece of work, first run `git fetch`, then create and check out a new branch that tracks `origin/master` (e.g. `git checkout -b <branch-name> origin/master`).
- Always run `cargo fmt` and `cargo clippy` after changing Rust code.
- Always run `clang-format -i` changing C code.
- Update documentation ([README.md](README.md), [CONTRIBUTING.md](CONTRIBUTING.md), [DELTA_MERGING_RULES.md](DELTA_MERGING_RULES.md), [RELEASING.md](RELEASING.md)) when changing or adding features.
- Avoid `unwrap()`, `expect()`, and other panicking functions in production code. Use proper error handling (`?`, `ok_or_else`, pattern matching, etc.) instead. Panicking in tests is acceptable.
- Use `anyhow` for error handling: `anyhow::Result<T>` for return types, `bail!()` for early error returns, `.context()` / `.with_context()` to add context to errors. Do not use `Box<dyn std::error::Error>`.
- Prefer imports over fully-qualified paths. Add `use` items for types and functions that are used in a file rather than repeating `crate::module::Type` or `std::collections::HashMap` inline.
- Avoid abbreviations in variable names. Prefer descriptive names (e.g., `table_config` over `tc`).
- Prefer `From`/`Into` (or `TryFrom`/`TryInto` for fallible conversions) over manual construction when converting between types, especially domain-to-proto conversions.
- After implementing new features, look for opportunities to refactor the code to improve readability and reduce duplication.
- Never include a "Test plan" section in pull request descriptions unless specificly asked.
- Commit often, but ensure each commit leaves leech2 in a working state (builds, tests pass, clippy clean).
- Every commit message must include a `Signed-off-by` line. Example:
  ```
  Short summary of the change

  Signed-off-by: Lars Erik Wik <lars.erik.wik@northern.tech>
  ```

## Architecture

See [CONTRIBUTING.md](CONTRIBUTING.md) for full architecture details, source layout, and data flow.

Key notes for development:

- Proto definitions have no backwards-compatibility constraints yet. Reusing or renumbering wire fields is fine.
- The 15 delta merge rules in `src/delta.rs` are specified in [DELTA_MERGING_RULES.md](DELTA_MERGING_RULES.md).
