# CLAUDE.md

This file provides guidance to Claude Code (claude.ai/code) when working with code in this repository.

## Build & Test Commands

**Prerequisite:** `protobuf-compiler` must be installed (`sudo apt install protobuf-compiler` / `brew install protobuf`).

- **Build the Rust library:** `cargo build`
- **Run Rust tests:** `cargo test`
- **Run a single test:** `cargo test <test_name>` (e.g. `cargo test test_merge_rule5`)
- **Format code:** `cargo fmt`
- **Lint:** `cargo clippy`

## Workflow

- Always run `cargo fmt` and `cargo clippy` after changing Rust code.
- Update documentation ([README.md](README.md), [CONTRIBUTING.md](CONTRIBUTING.md), [DELTA_MERGING_RULES.md](DELTA_MERGING_RULES.md)) when changing or adding features.

## Architecture

See [CONTRIBUTING.md](CONTRIBUTING.md) for full architecture details, source layout, and data flow.

Key notes for development:

- Proto definitions have no backwards-compatibility constraints yet. Reusing or renumbering wire fields is fine.
- The 15 delta merge rules in `src/delta.rs` are specified in [DELTA_MERGING_RULES.md](DELTA_MERGING_RULES.md). When modifying merge logic, refer to that document and ensure all rule tests pass.
