# Spec-to-test coverage matrix: MultiItemBudget architecture

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
