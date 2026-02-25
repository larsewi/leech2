# History Truncation Feature

## Context

Block files accumulate indefinitely in the `.leech2/` directory. This feature adds automatic pruning after every commit, configurable via `config.toml`. Three pruning rules are supported (all additive), plus unconditional removal of orphaned blocks.

## Config Format

New optional `[truncate]` section in `config.toml`:

```toml
[truncate]
max-blocks = 100    # keep at most 100 blocks in the chain (>= 1)
max-age = "7d"      # remove blocks older than this (e.g. "30s", "12h", "7d", "2w")
```

Both fields are optional and independent. The presence of each field enables that pruning rule. Both can be present at the same time (additive).

**Truncation behavior (always active, in order):**
1. Orphaned blocks are always removed (blocks on disk not reachable from HEAD)
2. Blocks older than REPORTED are always removed (if a REPORTED file exists)
3. If `max-blocks` is set: remove oldest blocks beyond the limit
4. If `max-age` is set: remove blocks older than the duration

All rules are additive — the union of blocks to remove from all active rules.

If `[truncate]` is absent, only orphan cleanup and reported-based cleanup run.

## `lch_patch_applied` — Usage Pattern

After creating a patch with `lch_patch_create`, the caller **must** call `lch_patch_applied` to release the buffer. This replaces `lch_free_buf` for patch buffers. The `reported` boolean indicates whether the patch was successfully delivered to the hub.

**Typical flow from a C application using the leech2 library:**

```c
// 1. Initialize leech2
lch_init("/path/to/.leech2");

// 2. Create blocks over time (CSV data changes)
lch_block_create();
//    → internally: creates block, updates HEAD, runs truncation

// 3. When ready to sync with hub, create a patch
uint8_t *buf; size_t len;
lch_patch_create("0000...0000", &buf, &len);

// 4. Send patch to hub (application-specific, may take time)
int ok = hub_send(buf, len);

// 5. Release the buffer. If reported=true, also updates the REPORTED file
//    with the patch's head_hash so truncation knows those blocks are safe
//    to remove.
lch_patch_applied(buf, len, ok);

// 6. Next time lch_block_create() runs, truncation will:
//    - Remove orphaned blocks (always)
//    - Remove blocks older than REPORTED (always, if REPORTED exists)
//    - Apply max-blocks and/or max-age rules (if configured)
```

**Key points:**
- `lch_patch_applied(buf, len, reported)` is mandatory after `lch_patch_create` — it replaces `lch_free_buf` for patches
- When `reported = true`: decodes the patch to extract `head_hash`, writes it to the `REPORTED` file, then frees the buffer
- When `reported = false`: just frees the buffer (no REPORTED update)
- The buffer is always freed, regardless of the `reported` flag or any errors during REPORTED file write
- If `REPORTED` file doesn't exist yet, reported-based truncation is a no-op (safe default)

## Implementation

### 1. `src/config.rs` — Add truncation config

- Add `TruncateConfig` struct with `max_blocks: Option<u32>`, `max_age: Option<String>`
- Add `truncate: Option<TruncateConfig>` field to `Config`
- Add `parse_duration(s: &str) -> Result<Duration>` helper (supports `s`, `m`, `h`, `d`, `w`)
- Validate in `Config::init()`: `max-blocks` must be >= 1 if present, `max-age` must be a valid duration if present

### 2. `src/storage.rs` — Add `remove()` function

- `pub fn remove(name: &str)` — acquires exclusive lock, deletes file, best-effort cleanup of `.lock` file
- Follows same locking pattern as `save()`

### 3. `src/reported.rs` — New module for REPORTED file

- `pub fn load() -> Result<Option<String>>` — returns hash of last reported block
- `pub fn save(hash: &str) -> Result<()>` — writes hash to `REPORTED` file
- Same pattern as `head.rs`

### 4. `src/truncate.rs` — New module (core logic)

**Algorithm:**
1. Walk chain from HEAD → GENESIS, collecting reachable hashes + timestamps into ordered vec (index 0 = HEAD)
2. Scan work directory for all 40-hex-char filenames
3. Delete orphaned blocks (on disk but not in reachable set) — **always runs**
4. If REPORTED file exists, remove all blocks older than REPORTED — **always runs**
5. If `max-blocks` is set and `chain.len() > max_blocks`, mark oldest blocks beyond the limit for removal
6. If `max-age` is set, mark blocks with `created` timestamp older than `now - max_age` for removal
7. Call `storage::remove()` for each block to delete (union of steps 4-6)

**Key function:** `pub fn run() -> Result<()>`

### 5. `src/block.rs` — Integrate truncation

Call `truncate::run()` at the end of `Block::create()`, after `head::save()`. Truncation failure is non-fatal (logged as warning) so block creation still succeeds.

### 6. `src/lib.rs` — Register modules + FFI

- Add `pub mod reported;` and `pub mod truncate;`
- Add `lch_patch_applied(buf: *mut u8, len: usize, reported: bool) -> i32` C FFI export that:
  1. Always frees the buffer (regardless of `reported` or errors)
  2. If `reported = true`: decodes the patch to extract `head_hash`, writes it to `REPORTED` file
  3. If `reported = false`: just frees the buffer
  4. Returns 0 on success, -1 on error (buffer is still freed on error)
- This replaces `lch_free_buf` for patch buffers returned by `lch_patch_create`

### 7. Documentation

- Update `README.md` with `[truncate]` config docs and `lch_patch_applied` C API
- Update `CONTRIBUTING.md` with new modules and `REPORTED` file in work dir layout

## Acceptance Tests

### `tests/accept_truncate_orphan.rs`
- **test_orphaned_blocks_removed**: Create blocks, add fake 40-hex file to work dir, create another block → orphan removed, chain blocks preserved
- **test_orphaned_blocks_from_old_head**: Create block, manually reset HEAD to GENESIS, create new block → old block (now orphaned) removed

### `tests/accept_truncate_max_blocks.rs`
- **test_truncate_max_blocks**: Config with `max-blocks = 2`, create 3 blocks → oldest removed, patch fallback works
- **test_truncate_max_blocks_under_limit**: Config with `max-blocks = 5`, create 3 blocks → all preserved

### `tests/accept_truncate_max_age.rs`
- **test_truncate_max_age**: Config with `max-age = "1s"`, create block, sleep 2s, create second block → first removed
- **test_truncate_max_age_keeps_recent**: Config with `max-age = "1d"`, create 3 blocks quickly → all preserved

### `tests/accept_truncate_reported.rs`
- **test_truncate_reported**: Create B1, B2, B3. Call `reported::save(&B2_hash)`. Create B4 → B1 removed (older than REPORTED), B2-B4 preserved
- **test_truncate_no_reported_file**: No REPORTED file ever written → all blocks preserved (reported-based cleanup is no-op)

### `tests/accept_truncate_config.rs`
- **test_truncate_config_max_blocks_invalid**: `max-blocks = 0` → Config::init() error
- **test_truncate_config_max_age_invalid**: `max-age = "abc"` → Config::init() error
- **test_truncate_config_no_truncate_section**: No [truncate] section → init succeeds

### Unit tests
- `parse_duration` tests in `config.rs` (seconds, days, weeks, invalid inputs)

## Verification

1. `cargo fmt && cargo clippy` — no warnings
2. `cargo test` — all existing + new tests pass
3. Manual test: `lch init`, create several blocks with `lch block create`, verify `lch log` shows truncated history after adding `[truncate]` config
