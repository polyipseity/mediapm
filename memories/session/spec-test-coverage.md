# Spec-to-test coverage matrix: ByteBudget architecture

| Spec item | Test(s) | Status |
|---|---|---|
| ByteBudget struct (new, advance, set_pos, adjust, reconcile, snap) | `bytebudget_new`, `bytebudget_advance`, `bytebudget_set_pos`, `bytebudget_adjust_positive`, `bytebudget_adjust_negative`, `bytebudget_reconcile_increases_total`, `bytebudget_reconcile_decreases_total` | � |
| ByteBudget hard-invariant assertions (pos ≤ total) | `bytebudget_advance_panics_on_overflow`, `bytebudget_set_pos_panics_on_exceed_total`, `bytebudget_set_pos_panics_on_decrease`, `bytebudget_adjust_negative_panics_below_pos` | 🟢 |
| ByteBudget concurrent safety (Send + Sync) | `bytebudget_concurrent_read_write`, `bytebudget_send_sync` | 🟢 |
| SourceProgressCallback type | `source_progress_callback_compiles`, `source_progress_callback_send_sync` | 🟢 |
| ProcessedSource struct | `process_zip_archive_linux_label`, `process_tar_gz_archive_macos_label`, `process_tar_xz_archive_windows_label`, `process_binary_format_produces_file_entry`, `process_binary_with_url_derived_filename_cas_roundtrip`, `process_single_source_binary_input_cost_equals_byte_length`, `process_single_source_archive_input_cost_exceeds_compressed_size` | 🟢 |
| Extraction helper local callback protocol | `extract_zip_fires_per_entry_progress`, `extract_tar_gz_fires_per_entry_progress`, `extract_tar_xz_fires_per_entry_progress`, `extract_zip_large_entry_fires_multiple_sub_entry_callbacks`, `extract_tar_gz_large_entry_fires_sub_entry_progress`, `extract_zip_progress_position_non_decreasing_and_total_constant`, `extract_tar_gz_progress_position_non_decreasing`, `extract_tar_xz_progress_position_non_decreasing` | 🟢 |
| process_single_source local callback signature | Updated `process_single_source` tests | 🟢 |
| Postprocess ByteBudget ownership | `postprocess_position_never_exceeds_total_with_archive_entries`, `postprocess_mixed_archive_binary_progress` | 🟢 |
| Fetch ByteBudget ownership | `fetch_progress_uses_size_hint_bytes_when_expected_size_none`, `fetch_progress_monotonic_with_known_sizes` | 🟢 |
| CountingReader plain-u64 cleanup | Updated CountingReader tests | 🟢 |
| Regression test suite | `postprocess_budget_pos_never_exceeds_total`, `fetch_budget_pos_never_exceeds_total`, `postprocess_fires_progress_per_source_entry`, `full_pipeline_progress_monotonic` | 🟢 |
| Provider pipeline (Phases 2–8) | All unit + integration tests | 🟢 |
