---
description: "Use when starting new features, fixing bugs, or extending the codebase. Describes the spec-driven and test-driven development workflow adopted across the mediapm workspace."
name: "Spec-Driven and Test-Driven Development Workflow"
applyTo: "**/*"
---

# SDD/TDD Workflow

This file defines the **spec-first, test-first** development workflow adopted
across the mediapm workspace.

## When Adding a Feature

1. **Write the spec** — Update the relevant `AGENTS.md` with:
   - Invariants, contracts, and edge cases for the new functionality
   - Cross-crate integration boundaries if applicable

2. **Write tests** — In this order:
   - **Unit tests** (`#[cfg(test)]` in the same file) for internal logic
   - **Integration tests** (`tests/int/` or `tests/e2e/`) for public API contracts
   - **Property tests** (`#[cfg(feature = "proptest")]`) for determinism,
     idempotency, and round-trip behavior
   - **Demo examples** (for mediapm) validate the full pipeline

2a. **Enforce exact-output matching for terminal-rendering tests** — When
writing tests that validate progress bar, spinner, or any
terminal-rendered output, you **MUST** use
`assert_eq!(actual, expected)` with `concat!(...)` string matching.
Substring or count-only assertions are **not acceptable** except in
the narrow exceptions documented in Rust Conventions
(`rust-conventions.instructions.md`, "Terminal output matching").

3. **Implement** — Code against the spec and tests. Verify all tests pass
   before moving to the next step.

4. **Update the coverage matrix** — Mark spec items as covered
   (`[covered]`), partial (`[partial]`), or uncovered (`[missing]`) in
   the "Coverage matrix" section below.

## When Fixing a Bug

1. **Write a failing test** that reproduces the bug — this test goes into the
   relevant `tests/` directory or `#[cfg(test)]` block
2. **Fix the implementation** — run the test suite to confirm the fix
3. **Verify no regressions** — run `cargo test --no-fail-fast` and compare
   against the baseline in `.config/pre-existing-test-failures.json`
4. **Add a spec entry** if the bug revealed a gap in `AGENTS.md`

## When Adding a New Managed Tool

Follow the step-by-step guide in `src/mediapm/AGENTS.md` (section:
"Adding a New Managed Tool"). The TL;DR is:

1. Spec first — document the contract
2. Test first — write provider/preset/workflow tests
3. Implement provider → preset → workflow
4. Register in all dispatchers
5. Integration test end-to-end

## Coverage Tracking

The "Coverage matrix" section below maps each spec item to its test status. Update it when:

- A new spec item is added
- A new test is written that covers a spec item
- A spec item becomes stale or is removed

## Unicode emoji prohibition

**Do not use unicode emoji in this file or in any coverage matrix entries.**
Unicode emoji in agent-edited files can cause agent harness crashes.
Use only ASCII markers in the Status column:

- `[covered]` — spec item is fully tested
- `[partial]` — spec item is partially tested (approximation or incomplete)
- `[missing]` — spec item is not yet tested

## Validation Gates

| Gate                | What it validates                                 | Frequency          |
| ------------------- | ------------------------------------------------- | ------------------ |
| Pre-commit (`prek`) | `cargo fmt`, linting, basic checks                | Every commit       |
| Selective tests     | `cargo test -p <crate>` for iterating             | During development |
| Full workspace      | `cargo test --no-fail-fast`                       | Before push        |
| Demos               | `cargo run --example mediapm_demo` (and \_online) | Before push        |
| Coverage review     | Compare spec items vs test status                 | Per-release        |

## Coverage matrix

### MultiItemBudget architecture

| Spec item                                                                                            | Test(s)                                                                                                                                                                                                                                                                                                                                                                                                                    | Status    |
| ---------------------------------------------------------------------------------------------------- | -------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------- | --------- |
| MultiItemBudget struct (new, with_capacity, add_item, item_count, set_total, advance, set_pos, snap) | `multi_item_budget_new`, `multi_item_budget_with_capacity`, `multi_item_budget_add_item`, `multi_item_budget_item_count`, `multi_item_budget_set_total`, `multi_item_budget_advance`, `multi_item_budget_set_pos`, `multi_item_budget_snap`                                                                                                                                                                                | [covered] |
| MultiItemBudget aggregate() for progress bars                                                        | `multi_item_budget_aggregate`                                                                                                                                                                                                                                                                                                                                                                                              | [covered] |
| MultiItemBudget concurrent safety (Send + Sync)                                                      | `multi_item_budget_concurrent_read_write`, `multi_item_budget_send_sync`                                                                                                                                                                                                                                                                                                                                                   | [covered] |
| MultiItemBudget hard-invariant assertions (pos ≤ total per item)                                     | `multi_item_budget_invariant_panics` (set_pos, advance, etc.)                                                                                                                                                                                                                                                                                                                                                              | [covered] |
| ByteBudget (legacy — still exists, unused in provider pipeline)                                      | `byte_budget_tests` module (14 tests: new, advance, set_pos, adjust_positive, adjust_negative, reconcile_increases_total, reconcile_decreases_total, advance_panics_on_overflow, set_pos_panics_on_exceed_total, set_pos_panics_on_decrease, adjust_negative_panics_below_pos, concurrent_read_write, send_sync)                                                                                                           | [covered] |
| ProcessedSource struct                                                                               | `process_zip_archive_linux_label`, `process_tar_gz_archive_macos_label`, `process_tar_xz_archive_windows_label`, `process_binary_format_produces_file_entry`, `process_binary_with_url_derived_filename_cas_roundtrip`                                                                                                                                                                                                     | [covered] |
| Extraction helper local callback protocol                                                            | `extract_zip_fires_per_entry_progress`, `extract_tar_gz_fires_per_entry_progress`, `extract_tar_xz_fires_per_entry_progress`, `extract_zip_large_entry_fires_multiple_sub_entry_callbacks`, `extract_tar_gz_large_entry_fires_sub_entry_progress`, `extract_zip_progress_position_non_decreasing_and_total_constant`, `extract_tar_gz_progress_position_non_decreasing`, `extract_tar_xz_progress_position_non_decreasing` | [covered] |
| process_single_source MultiItemBudget integration (Phase 5 migration)                                | Updated `process_single_source` tests — uses MultiItemBudget internally, no more local_cb/SourceProgressCallback                                                                                                                                                                                                                                                                                                           | [covered] |
| Process MultiItemBudget ownership                                                                    | `process_position_never_exceeds_total_with_archive_entries`, `process_mixed_archive_binary_progress`                                                                                                                                                                                                                                                                                                                       | [covered] |
| Fetch MultiItemBudget ownership                                                                      | `fetch_progress_uses_size_hint_bytes_when_expected_size_none`, `fetch_progress_monotonic_with_known_sizes`                                                                                                                                                                                                                                                                                                                 | [covered] |
| CountingReader plain-u64 cleanup                                                                     | Updated CountingReader tests                                                                                                                                                                                                                                                                                                                                                                                               | [covered] |
| Regression test suite                                                                                | `process_budget_pos_never_exceeds_total`, `fetch_budget_pos_never_exceeds_total`, `process_fires_progress_per_source_entry`, `full_pipeline_progress_monotonic`                                                                                                                                                                                                                                                            | [covered] |
| Provider pipeline (Phases 2–8)                                                                       | All unit + integration tests                                                                                                                                                                                                                                                                                                                                                                                               | [covered] |
| ProgressGroup spinner: advances without dirty state                                                  | `spinner_advances_without_dirty`, `regression_spinner_dirty_independence`                                                                                                                                                                                                                                                                                                                                                  | [covered] |
| ProgressGroup spinner: frozen on finished/abandoned/failed                                           | `spinner_does_not_advance_on_finished_bar`, `spinner_stops_on_abandoned_bar`, `spinner_stops_on_failed_bar`                                                                                                                                                                                                                                                                                                                | [covered] |
| ProgressGroup spinner: active among finished                                                         | `spinner_active_among_finished`                                                                                                                                                                                                                                                                                                                                                                                            | [covered] |

### Progress output exact-output matching

Integration tests in `tests/progress_output/` converted from substring/contains/count assertions to `assert_eq!(term.contents(), concat!(...))`.

| Test module                                                                                                                                      | Tests                                                                             | Status    |
| ------------------------------------------------------------------------------------------------------------------------------------------------ | --------------------------------------------------------------------------------- | --------- |
| `terminal.rs` — overflow behavior, dimension edge cases                                                                                          | 18 tests, all exact `concat!()`                                                   | [covered] |
| `consumer.rs` — bar retention, parallel worker output                                                                                            | 2 tests, exact                                                                    | [covered] |
| `transition.rs` — bar state transitions                                                                                                          | subset, exact                                                                     | [covered] |
| `progress_group.rs` — gap conversions: child visibility, lifecycle, join-and-clear                                                               | 5 tests, exact `concat!()`                                                        | [covered] |
| `spinner.rs` — deterministic spinner animation with `TestTimeSource`                                                                             | 3 tests (8 contains → exact), also covers `regression_spinner_dirty_independence` | [covered] |
| `regression.rs` — concurrent set-and-sync (deterministic), child order, swap-slot, finish-and-clear, overall stability, masked-spinner ends_with | 6 tests, all exact                                                                | [covered] |
| `single_bar.rs` — first/last/only-bar lines exact                                                                                                | 1 structural `.len()` remaining                                                   | [covered] |

### CasApi: `get()` delegates to `get_to_writer()`

| Spec item                                                                                         | Test(s)                                                                                                               | Status    |
| ------------------------------------------------------------------------------------------------- | --------------------------------------------------------------------------------------------------------------------- | --------- |
| `InMemoryCas::get()` works above `WAL_INLINE_LIMIT` (2 MiB)                                       | `in_memory_get_succeeds_above_wal_inline_limit`                                                                       | [covered] |
| `FileSystemCas::get()` works above `WAL_INLINE_LIMIT` (65 MiB, `#[cfg(feature = "large-tests")]`) | `filesystem_get_succeeds_above_wal_inline_limit`                                                                      | [covered] |
| `InMemoryCas::get()` delegates to `get_to_writer()` internally                                    | Already verified by `in_memory_get_succeeds_above_wal_inline_limit` (no separate unit test for delegation mechanics)  | [covered] |
| `FileSystemCas::get()` delegates to `get_to_writer()` internally                                  | Already verified by `filesystem_get_succeeds_above_wal_inline_limit` (no separate unit test for delegation mechanics) | [covered] |
| `CasApi` section in `mediapm-cas/AGENTS.md` already documents the delegation                      | Section 10 of `src/mediapm-cas/AGENTS.md` — verified accurate, no change needed                                       | [covered] |

### Cache::lookup_bytes error handling

| Spec item                                                                             | Test(s)                                                                                | Status    |
| ------------------------------------------------------------------------------------- | -------------------------------------------------------------------------------------- | --------- |
| `lookup_bytes` returns `None` on transient CAS error (non-`NotFound`)                 | `lookup_bytes_keeps_entry_on_transient_cas_error`                                      | [covered] |
| `lookup_bytes` leaves index entry intact on transient error                           | `lookup_bytes_keeps_entry_on_transient_cas_error` (asserts `get_entry_hash` is `Some`) | [covered] |
| `lookup_bytes` removes index entry on `NotFound` error                                | `lookup_bytes_nonexistent_key_returns_none` (existing, checks `None` return)           | [covered] |
| `Cache::open` accepts verify strategies via `open_with_verify_strategies` (test-only) | `lookup_bytes_keeps_entry_on_transient_cas_error` (uses `Always` verify)               | [covered] |
| Transient error test uses large payload (>1 MiB) to force blob-store path             | `lookup_bytes_keeps_entry_on_transient_cas_error` (1025 × 1024 = 1 048 577 bytes)      | [covered] |

| Spec item                                                            | Test(s)                                                                                                     | Status    |
| -------------------------------------------------------------------- | ----------------------------------------------------------------------------------------------------------- | --------- |
| Pre-roll scrolls existing terminal content into scrollback (bug fix) | `pre_roll_with_existing_content_scrolls_it_away` — exact `concat!()` body matching + no-substring assertion | [covered] |

### Single push point: `sync_snapshot_to_bar`

| Spec item                                                                                     | Test(s)                                        | Status    |
| --------------------------------------------------------------------------------------------- | ---------------------------------------------- | --------- |
| `sync_snapshot_to_bar` is single authoritative push point for SharedState → indicatif         | `sync_slot_preserves_custom_message_on_attach` | [covered] |
| Custom message set via `set_message` survives `add_bar` of another bar (sync_slot delegation) | `sync_slot_preserves_custom_message_on_attach` | [covered] |
| Cache guard is updated by delegate path (no stale-cache skip on next tick)                    | `sync_slot_preserves_custom_message_on_attach` | [covered] |

### Metadata cache awareness on resolve bar

| Spec item                                                                                   | Test(s)                                                                                                                                                                                                                     | Status    |
| ------------------------------------------------------------------------------------------- | --------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------- | --------- |
| `resolve_tool_fetch` returns `metadata_cached` (bool) and `metadata_fetch_count` (u32)      | Production dispatch: all per-tool resolvers updated (`resolve_latest_github_tag`, `resolve_latest_autobuild_tag`, per-tool `resolve_tag` functions return tuples with new fields)                                           | [covered] |
| ffmpeg uses metadata_fetch_count=2 (btbn + evermeet), `metadata_cached=btbn\|\|evermeet`    | `resolve_tool_fetch` ffmpeg arm destructures both resolvers                                                                                                                                                                 | [covered] |
| `MetadataCacheTracker` auto-derives `metadata_fetch_count` from actual `lookup_bytes` calls | Auto-derived via `tracker.lookup_count()` after match in `resolve_tool_fetch` — no per-tool hardcoded values; `media-tagger` count=0, bar is indeterminate                                                                  | [covered] |
| `PreResolveOutcome::Resolved` carries `(bool, u32)` for metadata cache state                | `resolve_bar_shows_cached_when_metadata_cached`, `resolve_bar_shows_total_two_when_metadata_fetch_count_two`, `resolve_bar_no_cached_message_when_not_cached`, `resolve_bar_zero_metadata_fetch_count_uses_min_one`         | [covered] |
| `PreResolveOutcome::Skip` carries `metadata_cached: bool, metadata_fetch_count: u32`        | `skip_bar_shows_skipped_cached_when_metadata_cached`, `skip_bar_shows_skipped_when_metadata_not_cached`, `skip_bar_shows_skipped_cached_two`, `skip_bar_zero_metadata_fetch_count_uses_min_one`                             | [covered] |
| Resolve bar shows `"cached (N)"` message with correct N for various counts                  | `resolve_bar_shows_cached_when_metadata_cached` (N=1), `resolve_bar_cached_two_shows_cached_two` (N=2 + bare "cached" absent)                                                                                               | [covered] |
| Resolve bar bar total = `metadata_fetch_count`                                              | `resolve_bar_shows_total_two_when_metadata_fetch_count_two` (N=2→2), `resolve_bar_zero_metadata_fetch_count_uses_min_one` (N=0→0)                                                                                           | [covered] |
| Skip bar shows `"skipped cached (N)"` vs `"skipped"` depending on metadata_cached           | `skip_bar_shows_skipped_cached_when_metadata_cached` (N=1), `skip_bar_shows_skipped_when_metadata_not_cached`, `skip_bar_shows_skipped_cached_two` (N=2), `skip_bar_zero_metadata_fetch_count_uses_min_one` (N=0→"skipped") | [covered] |
| Skip bar position equals total for non-trivial metadata_fetch_count                         | `skip_bar_shows_skipped_cached_two` (pos=2, total=2)                                                                                                                                                                        | [covered] |
| Skip bar uses raw `metadata_fetch_count` for bar total (0 = indeterminate)                  | `skip_bar_zero_metadata_fetch_count_uses_min_one` (total=0, pos=0, no cached message)                                                                                                                                       | [covered] |
| Bare `"cached"` (without count) never appears in any resolve bar message                    | `resolve_bar_no_cached_message_when_not_cached`, `resolve_bar_cached_two_shows_cached_two` (also asserts bare absent)                                                                                                       | [covered] |

### Content cache key: actual download URL

| Spec item                                                              | Test(s)                                         | Status    |
| ---------------------------------------------------------------------- | ----------------------------------------------- | --------- |
| Cache key is actual URL used for download, not blindly `urls[0]`       | `fetch_cache_key_uses_actual_url_not_first_url` | [covered] |
| Cache key survives first-URL cache miss — iterates all URLs for lookup | `fetch_cache_key_uses_actual_url_not_first_url` | [covered] |

### DirectoryLockGuard

| Spec item                                                             | Test(s)                                                                                                                    | Status    |
| --------------------------------------------------------------------- | -------------------------------------------------------------------------------------------------------------------------- | --------- |
| DirectoryLockGuard two-layer architecture: DashMap + flock            | `directory_lock_new_releases_on_drop`, `directory_lock_same_process_contention`, `directory_lock_cross_process_contention` | [covered] |
| DirectoryLockGuard fail-fast (non-blocking) contract                  | `directory_lock_fail_fast_no_blocking`                                                                                     | [covered] |
| FileSystemCas same-process contention (`LockContention` on dual open) | `file_system_cas_same_process_contention`                                                                                  | [covered] |
| FileSystemCas cross-process contention (flock barrier detection)      | `file_system_cas_contention_with_flock_barrier`                                                                            | [covered] |
| FileSystemCas concurrent clones share lock (no contention)            | `file_system_cas_concurrent_clones_no_contention`                                                                          | [covered] |
| FileSystemCas symlink canonicalization (symlink → same dir detected)  | `file_system_cas_contention_with_canonical_symlink`                                                                        | [covered] |

### Counting mechanism

| Spec item                                                                                     | Test(s)                                                                          | Status    |
| --------------------------------------------------------------------------------------------- | -------------------------------------------------------------------------------- | --------- |
| Monotonic non-decreasing position within a phase                                              | `extract_zip_progress_position_non_decreasing_and_total_constant`                | [covered] |
|                                                                                               | `extract_tar_gz_progress_position_non_decreasing`                                | [covered] |
|                                                                                               | `extract_tar_xz_progress_position_non_decreasing`                                | [covered] |
| Position never exceeds total per item (hard assert)                                           | `multi_item_budget_invariant_panics`                                             | [covered] |
| Position equals total at endpoint of each phase                                               | `process_budget_pos_never_exceeds_total` (pos=total at end)                      | [covered] |
| ZIP proportional estimation: endpoint exact                                                   | `process_budget_pos_never_exceeds_total`                                         | [covered] |
| ZIP proportional estimation: mid-entry approximate                                            | (no exactness test — accepted approximation)                                     | [partial] |
| GzDecoder read-ahead (~32 KB jumps) mitigated by per-entry callbacks                          | `extract_tar_gz_large_entry_fires_sub_entry_progress`                            | [covered] |
| XzDecoder total_in() vs CountingReader responsiveness design decision                         | `extract_tar_xz_progress_position_non_decreasing`                                | [covered] |
| Compress ZIP metadata overhead (~KB) vs payload (MB–GB) — negligible undercount               | (no dedicated test — accepted approximation)                                     | [partial] |
| Compress sub-entry chunking: callback fires per SUB_ENTRY_CHUNK                               | `compress_budget_total_matches_output_size`, `compress_monotonic_non_decreasing` | [covered] |
| Fidelity over precision: smooth visual updates prioritized over byte-exact accuracy           | (architectural invariant — verified by all monotonicity tests)                   | [covered] |
| CountingReader sub-entry callback fires every SUB_ENTRY_CHUNK bytes                           | `extract_zip_large_entry_fires_multiple_sub_entry_callbacks`                     | [covered] |
|                                                                                               | `counting_reader_tracks_exact_compressed_bytes`                                  | [covered] |
| CountingReader plain-u64 cleanup                                                              | Updated `CountingReader` tests (Cell<u64> → correct field access)                | [covered] |
| per-entry callback fires after every tar entry (fills gaps where no sub-entry callback fires) | `extract_tar_gz_fires_per_entry_progress`                                        | [covered] |
|                                                                                               | `extract_tar_xz_fires_per_entry_progress`                                        | [covered] |
| GzDecoder + CountingReader integration                                                        | `gzdecoder_with_counting_reader_tracks_consumption`                              | [covered] |
| ZIP extraction end-position equals entry compressed total                                     | `zip_extraction_end_position_equals_entry_compressed`                            | [covered] |
| ZIP extraction: all snapshots have position ≤ total, non-decreasing                           | `zip_position_never_exceeds_entry_total`                                         | [covered] |
| Unified sub-entry chunk policy: SUB_ENTRY_CHUNK = 65536                                       | All sub-entry tests pass at 64 KB threshold                                      | [covered] |

### Compress estimate improvement (Phase 1)

| Spec item                                                           | Test(s)                                                                                  | Status    |
| ------------------------------------------------------------------- | ---------------------------------------------------------------------------------------- | --------- |
| Compress estimate never starts at 0 (moved before add_item)         | `process_single_source_archive_two_items_completed` (budget starts with estimate, not 0) | [covered] |
| gzip ISIZE parsing for tar.gz exact uncompressed size               | `estimate_uncompressed_size_tar_gz_uses_isize`                                           | [covered] |
| xz Index parsing for tar.xz exact uncompressed size                 | `estimate_uncompressed_size_tar_xz_uses_index`                                           | [covered] |
| `resolve_format_and_filename` helper extracted from inline matching | (compiles — verified by existing tests)                                                  | [covered] |
| Redundant `set_total` removed from `process_single_source`          | (compiles — verified by snapshot tests)                                                  | [covered] |

### Progress callback threading (Phase 2)

| Spec item                                                                   | Test(s)                                                                                         | Status    |
| --------------------------------------------------------------------------- | ----------------------------------------------------------------------------------------------- | --------- |
| `fire_progress` helper deduplicates aggregate+snapshot pattern              | (manual review — used in fetch_tool_sources, fetch_bytes_from_candidates, process_tool_sources) | [covered] |
| `progress_cb` threads through `process_single_source` for per-chunk updates | `process_progress_cb_fires_during_extraction`                                                   | [covered] |
| Decompress per-chunk callbacks fire during tar.gz/xz extraction             | `process_progress_cb_fires_during_extraction` (callback_count > item_count)                     | [covered] |
| Compress per-chunk callbacks fire during repack to CAS                      | `process_progress_cb_fires_during_extraction`                                                   | [covered] |
| Binary/launcher progress_cb fires after completion                          | `process_position_never_exceeds_total_with_archive_entries`                                     | [covered] |
| Initial progress_cb fire before processing loop starts                      | `process_position_never_exceeds_total_with_archive_entries` (first snapshot exists)             | [covered] |
| Fetch side callbacks deduplicated via `fire_progress`                       | `fetch_progress_uses_size_hint_bytes_when_expected_size_none`                                   | [covered] |
| Progress snapsnots count >> source count (per-chunk rather than per-source) | `process_position_never_exceeds_total_with_archive_entries` (snapshot_count > entries count)    | [covered] |

### Process-phase documentation

| Spec item                                                   | Test(s)                                                          | Status    |
| ----------------------------------------------------------- | ---------------------------------------------------------------- | --------- |
| Initial bar total is item count (intentional) documented    | Doc comment in `process_tool_sources`, comment in `provision.rs` | [covered] |
| Total refining across sources (expected) documented         | Doc comment in `process_tool_sources`                            | [covered] |
| Callback architecture docs updated with per-chunk threading | Doc comment in `process_tool_sources`                            | [covered] |
| Coverage matrix updated                                     | This file                                                        | [covered] |

### `.env.generated` env var names and paths

| Spec item                                                                            | Test(s)                                                                                                                      | Status    |
| ------------------------------------------------------------------------------------ | ---------------------------------------------------------------------------------------------------------------------------- | --------- |
| `.env.generated` operates at mediapm layer, not conductor layer                      | `tool-sync-tool-config.instructions.md` — spec section                                                                       | [covered] |
| Only one tool active at a time → env var names use plain tool id (no `@hash` suffix) | `sync_env_has_no_hash_in_names` (integration), `content_key_to_env_name_strips_hash` (unit)                                  | [covered] |
| Env var values point to `ProvisionCache` payload layout (`<tool_id>/payload/<key>`)  | `sync_env_paths_contain_payload_segment` (integration)                                                                       | [covered] |
| `content_key_to_env_name` is a pure function for binary-entry name derivation        | `content_key_to_env_name_binary` (unit)                                                                                      | [covered] |
| Binary entry produces both `_DIR` and binary env var per content-map key             | `write_runtime_env_binary_produces_dir_and_binary` (unit)                                                                    | [covered] |
| Dir-only entry produces only `_DIR` env var                                          | `write_runtime_env_dir_produces_dir_only` (unit)                                                                             | [covered] |
| No duplicate `_DIR` entries when processing multiple keys per OS                     | `write_runtime_env_mixed_os_produces_no_duplicate_dirs` (unit)                                                               | [covered] |
| Skipped tools get env var entries in `.env.generated`                                | `sync_twice_env_generated_persists` (integration)                                                                            | [covered] |
| `.env.generated` paths are always absolute                                           | `write_runtime_env_uses_absolute_paths` (unit), absolute assertion in `sync_env_paths_contain_payload_segment` (integration) | [covered] |
| Regression: existing path structure, env names, dedup unaffected                     | All existing unit + integration tests                                                                                        | [covered] |

### Dual-write strategy (state.json always-write vs NCL change-detected)

| Spec item                                                                                        | Test(s)                                                                                 | Status    |
| ------------------------------------------------------------------------------------------------ | --------------------------------------------------------------------------------------- | --------- |
| state.json always-write policy (metadata-driven, updates mtime even when content is identical)   | `state_json_always_writes_to_disk` (integration)                                        | [covered] |
| conductor.generated.ncl skip-if-unchanged (artifact-driven, uses `write_bytes_if_changed`)       | `conductor_ncl_skips_write_when_unchanged` (integration)                                | [covered] |
| State-only churn (canonical_version change without payload change) does not touch conductor file | `regression_state_only_churn_does_not_touch_conductor_file` (integration)               | [covered] |
| `write_bytes_if_changed` writes when content differs (unit)                                      | `write_bytes_if_changed_writes_when_content_differs` (unit)                             | [covered] |
| `write_bytes_if_changed` skips write when content identical, verified via mtime (unit)           | `write_bytes_if_changed_skips_write_when_content_identical` (unit)                      | [covered] |
| `write_bytes_if_changed` creates parent directories (unit)                                       | `write_bytes_if_changed_creates_parent_directories` (unit)                              | [covered] |
| Dual-write strategy documented in coordinator spec                                               | `tool-sync-coordinator-and-identity.instructions.md` — "Dual-write strategy" subsection | [covered] |
| State write policy documented in state persistence spec                                          | `state-persistence.instructions.md` — "State write policy" section                      | [covered] |
| `write_bytes_if_changed` as artifact gate documented in document I/O spec                        | `document-io-lifecycle.instructions.md` — `write_bytes_if_changed` bullet               | [covered] |

### Provisioning pruning (generated doc + filesystem)

| Spec item                                                                       | Test(s)                                                                                     | Status    |
| ------------------------------------------------------------------------------- | ------------------------------------------------------------------------------------------- | --------- |
| Old `tool_id@hash` keys pruned from generated doc when content_map_hash changes | `reconcile_prunes_old_tool_version_from_generated_doc`                                      | [covered] |
| `pruned_tools` field in `ToolSyncReport` tracks pruned key count                | `reconcile_prunes_old_tool_version_from_generated_doc` (asserts `report.pruned_tools >= 1`) | [covered] |
| `pruned_tools` is wired through to `ToolsSyncSummary`                           | compilation check (service.rs uses `report.pruned_tools` instead of `0` stub)               | [covered] |
| `retain_only_tool_dirs` called after save to prune filesystem tool directories  | compilation check (import + call in reconcile)                                              | [covered] |
| Pruning preserves keys for remaining tools (no false positives)                 | `reconcile_prunes_old_tool_version_from_generated_doc` (asserts new key exists after prune) | [covered] |

### tool_runtimes keyed by plain tool_id (Phase 9)

| Spec item                                                  | Test(s)                                                                 | Status    |
| ---------------------------------------------------------- | ----------------------------------------------------------------------- | --------- |
| tool_runtimes uses plain `tool_id` as keys (no `@hash`)    | All existing env integration tests (verified by 333-passing test suite) | [covered] |
| Skipped tools use `entry().or_insert()` to avoid overwrite | All skip-related env tests                                              | [covered] |
| `Ok(None)` path uses plain `tool_id` key                   | All env generation tests                                                | [covered] |

### VersionSpec split (ConfigVersionSpec + VersionSpec)

| Spec item                                                                                     | Test(s)                                                                                                                                                                                                                                              | Status    |
| --------------------------------------------------------------------------------------------- | ---------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------- | --------- |
| `ConfigVersionSpec` enum (Latest, Inherit, Exact) + custom serde                              | `config_version_spec_serde_latest`, `config_version_spec_serde_inherit`, `config_version_spec_serde_exact_vcs_hash`, `config_version_spec_serde_exact_version`, `config_version_spec_serde_exact_tag`, `config_version_spec_serde_exact_multi_field` | [covered] |
| `ConfigVersionSpec` serde validation (empty object, deny-unknown-fields)                      | `config_version_spec_serde_empty_object_error`, `config_version_spec_serde_deny_unknown_fields`                                                                                                                                                      | [covered] |
| `VersionSpec` enum (Latest, Exact) — no Inherit, no serde                                     | Compilation check (all internal functions use clean `VersionSpec`)                                                                                                                                                                                   | [covered] |
| `spec_matches_entry` takes `&VersionSpec` (clean, no Inherit arm)                             | All `spec_matches_*` tests (compilation — `Inherit` arm removed)                                                                                                                                                                                     | [covered] |
| `compute_used_tool_ids` function (DFS, transitive deps, visited-set termination)              | `compute_used_tool_ids_empty_desired`, `compute_used_tool_ids_single_no_deps`, `compute_used_tool_ids_with_transitive_deps`, `compute_used_tool_ids_circular_deps_terminates`                                                                        | [covered] |
| `resolve_dep_version_spec` takes `&ConfigVersionSpec`, returns `VersionSpec`                  | `resolve_dep_version_spec_inherit_resolves`, `resolve_dep_version_spec_exact_passthrough`, `resolve_dep_version_spec_latest_passthrough`, `resolve_dep_version_spec_inherit_missing_tool_error`, `resolve_dep_version_spec_circular_inherit_error`   | [covered] |
| Pruning uses computed active set (`compute_used_tool_ids`)                                    | Verified by existing `reconcile_prunes_old_tool_version_from_generated_doc` (uses step_tool_ids → active set)                                                                                                                                        | [covered] |
| `known_dependency_type` registry lookup function                                              | Compilation check (defined in `src/mediapm/src/tools/dependency.rs`)                                                                                                                                                                                 | [covered] |
| Per-tool `dependency_types()` functions (yt-dlp, media-tagger, rsgain)                        | Compilation check (each preset module returns `BTreeMap<&'static str, DependencyType>`)                                                                                                                                                              | [covered] |
| All 5 edges correctly classified (2 SameStep, 3 CrossStep)                                    | Compiled verification of `known_dependency_type` mapping                                                                                                                                                                                             | [covered] |
| `dependencies` flattened to `BTreeMap<String, ConfigVersionSpec>` — serde round-trip          | `tool_requirement_serde_dependencies_flat`, `tool_requirement_serde_dependencies_empty`                                                                                                                                                              | [covered] |
| Conversion at config boundary: `ConfigVersionSpec::Inherit` → error for tool's own spec       | `build_provisioning_entries` self-spec match (compilation — Inherit returns Err for explicit tools)                                                                                                                                                  | [covered] |
| Conversion at config boundary: `ConfigVersionSpec::Inherit` → resolved `VersionSpec` for deps | `resolve_dep_version_spec` Inherit path (returns `VersionSpec::Latest` or `VersionSpec::Exact`)                                                                                                                                                      | [covered] |
| `ToolRequirement` uses `ConfigVersionSpec` for `version_spec` and `dependencies`              | ToolRequirement serde round-trip tests (serialization unchanged, type changed)                                                                                                                                                                       | [covered] |
| `lib.rs` re-exports `VersionSpec` (clean, no Inherit) for public API                          | Compilation check (examples import from `mediapm::VersionSpec`)                                                                                                                                                                                      | [covered] |

### Error code catalog with crate-prefixed codes

| Spec item                                                                                        | Test(s)                                                                                                                                                                  | Status    |
| ------------------------------------------------------------------------------------------------ | ------------------------------------------------------------------------------------------------------------------------------------------------------------------------ | --------- |
| MPM-E001 displayed on unknown dependency key with "did you mean" suggestion                      | `validate_unknown_dep_key_error`, `validate_version_suffix_suggests_bare_id`, `validate_close_match_via_similar` (unit), `sync_rejects_bad_dependency_key` (integration) | [covered] |
| MPM-E001 exact constraint: keys outside `dependency_types()` rejected even if configured as tool | `validate_exactly_no_more` (unit), `sync_rejects_dep_key_not_in_known_types` (integration)                                                                               | [covered] |
| MPM-E001 unknown tool with any dependency key yields clear message                               | `validate_unknown_tool_rejects_all_deps` (unit)                                                                                                                          | [covered] |
| MPM-E001 `_version` suffix heuristic suggests bare tool ID                                       | `validate_version_suffix_suggests_bare_id`                                                                                                                               | [covered] |
| MPM-E002 displayed on `inherit` with unconfigured tool, includes parent tool context             | `resolve_dep_version_spec_inherit_missing_tool_error`                                                                                                                    | [covered] |
| MPM-E003 displayed on circular `inherit` resolution                                              | `resolve_dep_version_spec_circular_inherit_error`                                                                                                                        | [covered] |
| MPM-E004 displayed on serde config parse failure                                                 | (manual — all 4 sites wired with MPM-E004 wrapper)                                                                                                                       | [covered] |
| MPM-W001 warning on `ToolRequirement` serde drop in `compute_used_tool_ids`                      | `compute_used_tool_ids_*` tests (warning emitted, no test assertion on warning content)                                                                                  | [covered] |
| `error.rs` module-level docs contain error code catalog table (MPM-E001–MPM-E008, MPM-W001)      | Manual review of `src/mediapm/src/error.rs`                                                                                                                              | [covered] |
| `.agents/instructions/error-codes.instructions.md` as centralized catalog reference              | Manual review                                                                                                                                                            | [covered] |
| CAS error code catalog in `mediapm-cas/src/error.rs` module docs                                 | Manual review                                                                                                                                                            | [covered] |
| CND error code catalog in `mediapm-conductor/src/error.rs` module docs                           | Manual review                                                                                                                                                            | [covered] |

### Composite canonical_version

| Spec item                                                                                      | Test(s)                                                                                                                                                                             | Status    |
| ---------------------------------------------------------------------------------------------- | ----------------------------------------------------------------------------------------------------------------------------------------------------------------------------------- | --------- |
| `compute_composite_canonical_version` helper in sync/mod.rs — single source of truth           | 3 injection points + 1 comparison point all use the helper (compilation check — all call sites updated)                                                                             | [covered] |
| Format: `<bare>;dep_id:dep_ver;...` with deterministic sort by dep_id                          | `compute_composite_canonical_version_with_same_step_deps` (unit)                                                                                                                    | [covered] |
| Only SameStep deps included (CrossStep/Both excluded)                                          | `compute_composite_canonical_version_no_deps` (unit — CrossStep deps produce no `;` segments)                                                                                       | [covered] |
| Stored `canonical_version` in state.json is composite after sync                               | `sync_stores_composite_canonical_version` (integration)                                                                                                                             | [covered] |
| Skip check compares stored composite vs computed composite (not bare version)                  | `sync_skip_triggers_on_unchanged_composite` (integration — re-sync with identical state skips)                                                                                      | [covered] |
| `logical_tool_requires_sync` uses composite comparison                                         | `sync_logical_requires_sync_composite_comparison` (integration — matching composite → false), `sync_logical_requires_sync_on_composite_mismatch` (integration — mismatching → true) | [covered] |
| `index_managed_tools` groups entries by tool_id for skip/lookup                                | `index_managed_tools` produces correct grouping (unit — verified by existing `compute_used_tool_ids` tests that use same grouping)                                                  | [covered] |
| Spec doc in `tool-sync-coordinator-and-identity.instructions.md` — composite canonical section | This file                                                                                                                                                                           | [covered] |
| Coverage matrix updated                                                                        | This file                                                                                                                                                                           | [covered] |
