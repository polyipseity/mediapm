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
   (🟢), partial (🟡), or uncovered (🔴) in
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

| Spec item                                                                                            | Test(s)                                                                                                                                                                                                                                                                                                                                                                                                                    | Status |
| ---------------------------------------------------------------------------------------------------- | -------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------- | ------ |
| MultiItemBudget struct (new, with_capacity, add_item, item_count, set_total, advance, set_pos, snap) | `multi_item_budget_new`, `multi_item_budget_with_capacity`, `multi_item_budget_add_item`, `multi_item_budget_item_count`, `multi_item_budget_set_total`, `multi_item_budget_advance`, `multi_item_budget_set_pos`, `multi_item_budget_snap`                                                                                                                                                                                | 🟢     |
| MultiItemBudget aggregate() for progress bars                                                        | `multi_item_budget_aggregate`                                                                                                                                                                                                                                                                                                                                                                                              | 🟢     |
| MultiItemBudget concurrent safety (Send + Sync)                                                      | `multi_item_budget_concurrent_read_write`, `multi_item_budget_send_sync`                                                                                                                                                                                                                                                                                                                                                   | 🟢     |
| MultiItemBudget hard-invariant assertions (pos ≤ total per item)                                     | `multi_item_budget_invariant_panics` (set_pos, advance, etc.)                                                                                                                                                                                                                                                                                                                                                              | 🟢     |
| ByteBudget (legacy — still exists, unused in provider pipeline)                                      | `byte_budget_tests` module (14 tests: new, advance, set_pos, adjust_positive, adjust_negative, reconcile_increases_total, reconcile_decreases_total, advance_panics_on_overflow, set_pos_panics_on_exceed_total, set_pos_panics_on_decrease, adjust_negative_panics_below_pos, concurrent_read_write, send_sync)                                                                                                           | 🟢     |
| ProcessedSource struct                                                                               | `process_zip_archive_linux_label`, `process_tar_gz_archive_macos_label`, `process_tar_xz_archive_windows_label`, `process_binary_format_produces_file_entry`, `process_binary_with_url_derived_filename_cas_roundtrip`                                                                                                                                                                                                     | 🟢     |
| Extraction helper local callback protocol                                                            | `extract_zip_fires_per_entry_progress`, `extract_tar_gz_fires_per_entry_progress`, `extract_tar_xz_fires_per_entry_progress`, `extract_zip_large_entry_fires_multiple_sub_entry_callbacks`, `extract_tar_gz_large_entry_fires_sub_entry_progress`, `extract_zip_progress_position_non_decreasing_and_total_constant`, `extract_tar_gz_progress_position_non_decreasing`, `extract_tar_xz_progress_position_non_decreasing` | 🟢     |
| process_single_source MultiItemBudget integration (Phase 5 migration)                                | Updated `process_single_source` tests — uses MultiItemBudget internally, no more local_cb/SourceProgressCallback                                                                                                                                                                                                                                                                                                           | 🟢     |
| Process MultiItemBudget ownership                                                                | `process_position_never_exceeds_total_with_archive_entries`, `process_mixed_archive_binary_progress`                                                                                                                                                                                                                                                                                                               | 🟢     |
| Fetch MultiItemBudget ownership                                                                      | `fetch_progress_uses_size_hint_bytes_when_expected_size_none`, `fetch_progress_monotonic_with_known_sizes`                                                                                                                                                                                                                                                                                                                 | 🟢     |
| CountingReader plain-u64 cleanup                                                                     | Updated CountingReader tests                                                                                                                                                                                                                                                                                                                                                                                               | 🟢     |
| Regression test suite                                                                                | `process_budget_pos_never_exceeds_total`, `fetch_budget_pos_never_exceeds_total`, `process_fires_progress_per_source_entry`, `full_pipeline_progress_monotonic`                                                                                                                                                                                                                                                    | 🟢     |
| Provider pipeline (Phases 2–8)                                                                       | All unit + integration tests                                                                                                                                                                                                                                                                                                                                                                                               | 🟢     |
| ProgressGroup spinner: advances without dirty state                                                  | `spinner_advances_without_dirty`, `regression_spinner_dirty_independence`                                                                                                                                                                                                                                                                                                                                                  | 🟢     |
| ProgressGroup spinner: frozen on finished/abandoned/failed                                           | `spinner_does_not_advance_on_finished_bar`, `spinner_stops_on_abandoned_bar`, `spinner_stops_on_failed_bar`                                                                                                                                                                                                                                                                                                                | 🟢     |
| ProgressGroup spinner: active among finished                                                         | `spinner_active_among_finished`                                                                                                                                                                                                                                                                                                                                                                                            | 🟢     |

### Progress output exact-output matching

Integration tests in `tests/progress_output/` converted from substring/contains/count assertions to `assert_eq!(term.contents(), concat!(...))`.

| Test module                                                                                                                                      | Tests                                                                             | Status |
| ------------------------------------------------------------------------------------------------------------------------------------------------ | --------------------------------------------------------------------------------- | ------ |
| `terminal.rs` — overflow behavior, dimension edge cases                                                                                          | 18 tests, all exact `concat!()`                                                   | 🟢     |
| `consumer.rs` — bar retention, parallel worker output                                                                                            | 2 tests, exact                                                                    | 🟢     |
| `transition.rs` — bar state transitions                                                                                                          | subset, exact                                                                     | 🟢     |
| `progress_group.rs` — gap conversions: child visibility, lifecycle, join-and-clear                                                               | 5 tests, exact `concat!()`                                                        | 🟢     |
| `spinner.rs` — deterministic spinner animation with `TestTimeSource`                                                                             | 3 tests (8 contains → exact), also covers `regression_spinner_dirty_independence` | 🟢     |
| `regression.rs` — concurrent set-and-sync (deterministic), child order, swap-slot, finish-and-clear, overall stability, masked-spinner ends_with | 6 tests, all exact                                                                | 🟢     |
| `single_bar.rs` — first/last/only-bar lines exact                                                                                                | 1 structural `.len()` remaining                                                   | 🟢     |

### CasApi: `get()` delegates to `get_to_writer()`

| Spec item                                                                                         | Test(s)                                                                                                               | Status |
| ------------------------------------------------------------------------------------------------- | --------------------------------------------------------------------------------------------------------------------- | ------ |
| `InMemoryCas::get()` works above `WAL_INLINE_LIMIT` (2 MiB)                                       | `in_memory_get_succeeds_above_wal_inline_limit`                                                                       | 🟢     |
| `FileSystemCas::get()` works above `WAL_INLINE_LIMIT` (65 MiB, `#[cfg(feature = "large-tests")]`) | `filesystem_get_succeeds_above_wal_inline_limit`                                                                      | 🟢     |
| `InMemoryCas::get()` delegates to `get_to_writer()` internally                                    | Already verified by `in_memory_get_succeeds_above_wal_inline_limit` (no separate unit test for delegation mechanics)  | 🟢     |
| `FileSystemCas::get()` delegates to `get_to_writer()` internally                                  | Already verified by `filesystem_get_succeeds_above_wal_inline_limit` (no separate unit test for delegation mechanics) | 🟢     |
| `CasApi` section in `mediapm-cas/AGENTS.md` already documents the delegation                      | Section 10 of `src/mediapm-cas/AGENTS.md` — verified accurate, no change needed                                       | 🟢     |

### Cache::lookup_bytes error handling

| Spec item                                                                             | Test(s)                                                                                | Status |
| ------------------------------------------------------------------------------------- | -------------------------------------------------------------------------------------- | ------ |
| `lookup_bytes` returns `None` on transient CAS error (non-`NotFound`)                 | `lookup_bytes_keeps_entry_on_transient_cas_error`                                      | 🟢     |
| `lookup_bytes` leaves index entry intact on transient error                           | `lookup_bytes_keeps_entry_on_transient_cas_error` (asserts `get_entry_hash` is `Some`) | 🟢     |
| `lookup_bytes` removes index entry on `NotFound` error                                | `lookup_bytes_nonexistent_key_returns_none` (existing, checks `None` return)           | 🟢     |
| `Cache::open` accepts verify strategies via `open_with_verify_strategies` (test-only) | `lookup_bytes_keeps_entry_on_transient_cas_error` (uses `Always` verify)               | 🟢     |
| Transient error test uses large payload (>1 MiB) to force blob-store path             | `lookup_bytes_keeps_entry_on_transient_cas_error` (1025 × 1024 = 1 048 577 bytes)      | 🟢     |

| Spec item                                                            | Test(s)                                                                                                     | Status |
| -------------------------------------------------------------------- | ----------------------------------------------------------------------------------------------------------- | ------ |
| Pre-roll scrolls existing terminal content into scrollback (bug fix) | `pre_roll_with_existing_content_scrolls_it_away` — exact `concat!()` body matching + no-substring assertion | 🟢     |

### Single push point: `sync_snapshot_to_bar`

| Spec item                                                                                     | Test(s)                                        | Status |
| --------------------------------------------------------------------------------------------- | ---------------------------------------------- | ------ |
| `sync_snapshot_to_bar` is single authoritative push point for SharedState → indicatif         | `sync_slot_preserves_custom_message_on_attach` | �      |
| Custom message set via `set_message` survives `add_bar` of another bar (sync_slot delegation) | `sync_slot_preserves_custom_message_on_attach` | �      |
| Cache guard is updated by delegate path (no stale-cache skip on next tick)                    | `sync_slot_preserves_custom_message_on_attach` | �      |

### Metadata cache awareness on resolve bar

| Spec item                                                                                   | Test(s)                                                                                                                                                                                                                     | Status |
| ------------------------------------------------------------------------------------------- | --------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------- | ------ |
| `resolve_tool_fetch` returns `metadata_cached` (bool) and `metadata_fetch_count` (u32)      | Production dispatch: all per-tool resolvers updated (`resolve_latest_github_tag`, `resolve_latest_autobuild_tag`, per-tool `resolve_tag` functions return tuples with new fields)                                           | 🟢     |
| ffmpeg uses metadata_fetch_count=2 (btbn + evermeet), `metadata_cached=btbn\|\|evermeet`    | `resolve_tool_fetch` ffmpeg arm destructures both resolvers                                                                                                                                                                 | 🟢     |
| `MetadataCacheTracker` auto-derives `metadata_fetch_count` from actual `lookup_bytes` calls | Auto-derived via `tracker.lookup_count()` after match in `resolve_tool_fetch` — no per-tool hardcoded values; `media-tagger` count=0, bar is indeterminate                                                                  | 🟢     |
| `PreResolveOutcome::Resolved` carries `(bool, u32)` for metadata cache state                | `resolve_bar_shows_cached_when_metadata_cached`, `resolve_bar_shows_total_two_when_metadata_fetch_count_two`, `resolve_bar_no_cached_message_when_not_cached`, `resolve_bar_zero_metadata_fetch_count_uses_min_one`         | 🟢     |
| `PreResolveOutcome::Skip` carries `metadata_cached: bool, metadata_fetch_count: u32`        | `skip_bar_shows_skipped_cached_when_metadata_cached`, `skip_bar_shows_skipped_when_metadata_not_cached`, `skip_bar_shows_skipped_cached_two`, `skip_bar_zero_metadata_fetch_count_uses_min_one`                             | 🟢     |
| Resolve bar shows `"cached (N)"` message with correct N for various counts                  | `resolve_bar_shows_cached_when_metadata_cached` (N=1), `resolve_bar_cached_two_shows_cached_two` (N=2 + bare "cached" absent)                                                                                               | 🟢     |
| Resolve bar bar total = `metadata_fetch_count`                                              | `resolve_bar_shows_total_two_when_metadata_fetch_count_two` (N=2→2), `resolve_bar_zero_metadata_fetch_count_uses_min_one` (N=0→0)                                                                                           | 🟢     |
| Skip bar shows `"skipped cached (N)"` vs `"skipped"` depending on metadata_cached           | `skip_bar_shows_skipped_cached_when_metadata_cached` (N=1), `skip_bar_shows_skipped_when_metadata_not_cached`, `skip_bar_shows_skipped_cached_two` (N=2), `skip_bar_zero_metadata_fetch_count_uses_min_one` (N=0→"skipped") | 🟢     |
| Skip bar position equals total for non-trivial metadata_fetch_count                         | `skip_bar_shows_skipped_cached_two` (pos=2, total=2)                                                                                                                                                                        | 🟢     |
| Skip bar uses raw `metadata_fetch_count` for bar total (0 = indeterminate)                  | `skip_bar_zero_metadata_fetch_count_uses_min_one` (total=0, pos=0, no cached message)                                                                                                                                       | 🟢     |
| Bare `"cached"` (without count) never appears in any resolve bar message                    | `resolve_bar_no_cached_message_when_not_cached`, `resolve_bar_cached_two_shows_cached_two` (also asserts bare absent)                                                                                                       | 🟢     |

### Content cache key: actual download URL

| Spec item                                                              | Test(s)                                         | Status |
| ---------------------------------------------------------------------- | ----------------------------------------------- | ------ |
| Cache key is actual URL used for download, not blindly `urls[0]`       | `fetch_cache_key_uses_actual_url_not_first_url` | 🟢     |
| Cache key survives first-URL cache miss — iterates all URLs for lookup | `fetch_cache_key_uses_actual_url_not_first_url` | 🟢     |

### DirectoryLockGuard

| Spec item                                                             | Test(s)                                                                                                                    | Status |
| --------------------------------------------------------------------- | -------------------------------------------------------------------------------------------------------------------------- | ------ |
| DirectoryLockGuard two-layer architecture: DashMap + flock            | `directory_lock_new_releases_on_drop`, `directory_lock_same_process_contention`, `directory_lock_cross_process_contention` | 🟢     |
| DirectoryLockGuard fail-fast (non-blocking) contract                  | `directory_lock_fail_fast_no_blocking`                                                                                     | 🟢     |
| FileSystemCas same-process contention (`LockContention` on dual open) | `file_system_cas_same_process_contention`                                                                                  | 🟢     |
| FileSystemCas cross-process contention (flock barrier detection)      | `file_system_cas_contention_with_flock_barrier`                                                                            | 🟢     |
| FileSystemCas concurrent clones share lock (no contention)            | `file_system_cas_concurrent_clones_no_contention`                                                                          | 🟢     |
| FileSystemCas symlink canonicalization (symlink → same dir detected)  | `file_system_cas_contention_with_canonical_symlink`                                                                        | 🟢     |

### Counting mechanism

| Spec item                                                                                           | Test(s)                                                                                  | Status |
| --------------------------------------------------------------------------------------------------- | ---------------------------------------------------------------------------------------- | ------ |
| Monotonic non-decreasing position within a phase                                                    | `extract_zip_progress_position_non_decreasing_and_total_constant`                        | 🟢     |
|                                                                                                     | `extract_tar_gz_progress_position_non_decreasing`                                        | 🟢     |
|                                                                                                     | `extract_tar_xz_progress_position_non_decreasing`                                        | 🟢     |
| Position never exceeds total per item (hard assert)                                                 | `multi_item_budget_invariant_panics`                                                     | 🟢     |
| Position equals total at endpoint of each phase                                                     | `process_budget_pos_never_exceeds_total` (pos=total at end)                          | 🟢     |
| ZIP proportional estimation: endpoint exact                                                         | `process_budget_pos_never_exceeds_total`                                             | 🟢     |
| ZIP proportional estimation: mid-entry approximate                                                  | (no exactness test — accepted approximation)                                             | 🟡     |
| GzDecoder read-ahead (~32 KB jumps) mitigated by per-entry callbacks                                | `extract_tar_gz_large_entry_fires_sub_entry_progress`                                    | 🟢     |
| XzDecoder total_in() vs CountingReader responsiveness design decision                               | `extract_tar_xz_progress_position_non_decreasing`                                        | 🟢     |
| Compress ZIP metadata overhead (~KB) vs payload (MB–GB) — negligible undercount                     | (no dedicated test — accepted approximation)                                             | 🟡     |
| Compress sub-entry chunking: callback fires per SUB_ENTRY_CHUNK                                      | `compress_budget_total_matches_output_size`, `compress_monotonic_non_decreasing`          | 🟢     |
| Fidelity over precision: smooth visual updates prioritized over byte-exact accuracy                 | (architectural invariant — verified by all monotonicity tests)                           | 🟢     |
| CountingReader sub-entry callback fires every SUB_ENTRY_CHUNK bytes                                 | `extract_zip_large_entry_fires_multiple_sub_entry_callbacks`                             | 🟢     |
|                                                                                                     | `counting_reader_tracks_exact_compressed_bytes`                                          | 🟢     |
| CountingReader plain-u64 cleanup                                                                     | Updated `CountingReader` tests (Cell<u64> → correct field access)                        | 🟢     |
| per-entry callback fires after every tar entry (fills gaps where no sub-entry callback fires)       | `extract_tar_gz_fires_per_entry_progress`                                                | 🟢     |
|                                                                                                     | `extract_tar_xz_fires_per_entry_progress`                                                | 🟢     |
| GzDecoder + CountingReader integration                                                              | `gzdecoder_with_counting_reader_tracks_consumption`                                      | 🟢     |
| ZIP extraction end-position equals entry compressed total                                           | `zip_extraction_end_position_equals_entry_compressed`                                    | 🟢     |
| ZIP extraction: all snapshots have position ≤ total, non-decreasing                                 | `zip_position_never_exceeds_entry_total`                                                 | 🟢     |
| Unified sub-entry chunk policy: SUB_ENTRY_CHUNK = 65536                                              | All sub-entry tests pass at 64 KB threshold                                               | 🟢     |

### Compress estimate improvement (Phase 1)

| Spec item                                                                     | Test(s)                                                                                      | Status |
| ----------------------------------------------------------------------------- | --------------------------------------------------------------------------------------------- | ------ |
| Compress estimate never starts at 0 (moved before add_item)                   | `process_single_source_archive_two_items_completed` (budget starts with estimate, not 0)      | 🟢     |
| gzip ISIZE parsing for tar.gz exact uncompressed size                         | `estimate_uncompressed_size_tar_gz_uses_isize`                                                | 🟢     |
| xz Index parsing for tar.xz exact uncompressed size                           | `estimate_uncompressed_size_tar_xz_uses_index`                                                | 🟢     |
| `resolve_format_and_filename` helper extracted from inline matching           | (compiles — verified by existing tests)                                                       | 🟢     |
| Redundant `set_total` removed from `process_single_source`                    | (compiles — verified by snapshot tests)                                                       | 🟢     |

### Progress callback threading (Phase 2)

| Spec item                                                                     | Test(s)                                                                                      | Status |
| ----------------------------------------------------------------------------- | --------------------------------------------------------------------------------------------- | ------ |
| `fire_progress` helper deduplicates aggregate+snapshot pattern                | (manual review — used in fetch_tool_sources, fetch_bytes_from_candidates, process_tool_sources) | 🟢     |
| `progress_cb` threads through `process_single_source` for per-chunk updates   | `process_progress_cb_fires_during_extraction`                                                  | 🟢     |
| Decompress per-chunk callbacks fire during tar.gz/xz extraction               | `process_progress_cb_fires_during_extraction` (callback_count > item_count)                    | 🟢     |
| Compress per-chunk callbacks fire during repack to CAS                        | `process_progress_cb_fires_during_extraction`                                                  | 🟢     |
| Binary/launcher progress_cb fires after completion                            | `process_position_never_exceeds_total_with_archive_entries`                                    | 🟢     |
| Initial progress_cb fire before processing loop starts                        | `process_position_never_exceeds_total_with_archive_entries` (first snapshot exists)            | 🟢     |
| Fetch side callbacks deduplicated via `fire_progress`                         | `fetch_progress_uses_size_hint_bytes_when_expected_size_none`                                  | 🟢     |
| Progress snapsnots count >> source count (per-chunk rather than per-source)   | `process_position_never_exceeds_total_with_archive_entries` (snapshot_count > entries count)    | 🟢     |

### Process-phase documentation

| Spec item                                                                     | Test(s)                                                                                      | Status |
| ----------------------------------------------------------------------------- | --------------------------------------------------------------------------------------------- | ------ |
| Initial bar total is item count (intentional) documented                      | Doc comment in `process_tool_sources`, comment in `provision.rs`                              | 🟢     |
| Total refining across sources (expected) documented                           | Doc comment in `process_tool_sources`                                                         | 🟢     |
| Callback architecture docs updated with per-chunk threading                   | Doc comment in `process_tool_sources`                                                         | 🟢     |
| Coverage matrix updated                                                       | This file                                                                                     | 🟢     |
