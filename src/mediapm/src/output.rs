//! CLI-friendly output formatting for sync summaries and diagnostic messages.

use crate::SyncSummary;

/// Prints a human-readable sync summary to stdout.
pub fn print_sync_summary(summary: &SyncSummary) {
    println!(
        "sync complete: executed={}, cached={}, rematerialized={}, materialized={}, removed={}, removed_empty_dirs={}",
        summary.executed_instances,
        summary.cached_instances,
        summary.rematerialized_instances,
        summary.materialized_paths,
        summary.removed_paths,
        summary.removed_empty_dirs,
    );
    for warning in &summary.warnings {
        eprintln!("warning: {warning}");
    }
}
