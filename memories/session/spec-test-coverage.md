# Spec-to-test coverage matrix

## MultiItemBudget architecture

| Spec item | Test(s) | Status |
|---|---|---|
| MultiItemBudget struct (new, with_capacity, add_item, item_count, set_total, advance, set_pos, snap) | `multi_item_budget_new`, `multi_item_budget_with_capacity`, `multi_item_budget_add_item`, `multi_item_budget_item_count`, `multi_item_budget_set_total`, `multi_item_budget_advance`, `multi_item_budget_set_pos`, `multi_item_budget_snap` | 🟢 |
| MultiItemBudget aggregate() for progress bars | `multi_item_budget_aggregate` | 🟢 |
| MultiItemBudget concurrent safety (Send + Sync) | `multi_item_budget_concurrent_read_write`, `multi_item_budget_send_sync` | 🟢 |
| MultiItemBudget hard-invariant assertions (pos ≤ total per item) | `multi_item_budget_invariant_panics` (set_pos, advance, etc.) | 🟢 |
| ByteBudget (legacy — still exists, unused in provider pipeline) | `byte_budget_tests` module (14 tests: new, advance, set_pos, adjust_positive, adjust_negative, reconcile_increases_total, reconcile_decreases_total, advance_panics_on_overflow, set_pos_panics_on_exceed_total, set_pos_panics_on_decrease, adjust_negative_panics_below_pos, concurrent_read_write, send_sync) | 🟢 |
| ProcessedSource struct | `process_zip_archive_linux_label`, `process_tar_gz_archive_macos_label`, `process_tar_xz_archive_windows_label`, `process_binary_format_produces_file_entry`, `process_binary_with_url_derived_filename_cas_roundtrip` | 🟢 |
| Extraction helper local callback protocol | `extract_zip_fires_per_entry_progress`, `extract_tar_gz_fires_per_entry_progress`, `extract_tar_xz_fires_per_entry_progress`, `extract_zip_large_entry_fires_multiple_sub_entry_callbacks`, `extract_tar_gz_large_entry_fires_sub_entry_progress`, `extract_zip_progress_position_non_decreasing_and_total_constant`, `extract_tar_gz_progress_position_non_decreasing`, `extract_tar_xz_progress_position_non_decreasing` | 🟢 |
| process_single_source MultiItemBudget integration (Phase 5 migration) | Updated `process_single_source` tests — uses MultiItemBudget internally, no more local_cb/SourceProgressCallback | 🟢 |
| Postprocess MultiItemBudget ownership | `postprocess_position_never_exceeds_total_with_archive_entries`, `postprocess_mixed_archive_binary_progress` | 🟢 |
| Fetch MultiItemBudget ownership | `fetch_progress_uses_size_hint_bytes_when_expected_size_none`, `fetch_progress_monotonic_with_known_sizes` | 🟢 |
| CountingReader plain-u64 cleanup | Updated CountingReader tests | 🟢 |
| Regression test suite | `postprocess_budget_pos_never_exceeds_total`, `fetch_budget_pos_never_exceeds_total`, `postprocess_fires_progress_per_source_entry`, `full_pipeline_progress_monotonic` | 🟢 |
| Provider pipeline (Phases 2–8) | All unit + integration tests | 🟢 |
| ProgressGroup spinner: advances without dirty state | `spinner_advances_without_dirty`, `regression_spinner_dirty_independence` | 🟢 |
| ProgressGroup spinner: frozen on finished/abandoned/failed | `spinner_does_not_advance_on_finished_bar`, `spinner_stops_on_abandoned_bar`, `spinner_stops_on_failed_bar` | 🟢 |
| ProgressGroup spinner: active among finished | `spinner_active_among_finished` | 🟢 |

## Progress output exact-output matching

Integration tests in `tests/progress_output/` converted from substring/contains/count assertions to `assert_eq!(term.contents(), concat!(...))`.

| Test module | Tests | Status |
|---|---|---|
| `terminal.rs` — overflow behavior, dimension edge cases | 18 tests, all exact `concat!()` | 🟢 |
| `consumer.rs` — bar retention, parallel worker output | 2 tests, exact | 🟢 |
| `transition.rs` — bar state transitions | subset, exact | 🟢 |
| `progress_group.rs` — gap conversions: child visibility, lifecycle, join-and-clear | 5 tests, exact `concat!()` | 🟢 |
| `spinner.rs` — deterministic spinner animation with `TestTimeSource` | 3 tests (8 contains → exact), also covers `regression_spinner_dirty_independence` | 🟢 |
| `regression.rs` — concurrent set-and-sync (deterministic), child order, swap-slot, finish-and-clear, overall stability, masked-spinner ends_with | 6 tests, all exact | 🟢 |
| `single_bar.rs` — first/last/only-bar lines exact | 1 structural `.len()` remaining | 🟢 |
