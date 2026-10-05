//! CLI-friendly output formatting for sync summaries, progress bars, and
//! diagnostic messages.
//!
//! # Submodules
//!
//! * [`progress`] — progress bars with [`ProgressScreen`]
//!
//! Result-line primitives (`StatusIcon`, `print_result`, `format_result_line`, etc.)
//! are centralized in `mediapm_utils::report` and re-exported here for backward
//! compatibility with existing `crate::output::*` import paths.

pub mod observer;
pub mod progress;

pub use mediapm_utils::report::{
    StatusIcon, format_duration, format_result_line, print_error, print_heading, print_hint,
    print_result, print_status_report, print_warning,
};
pub use progress::{
    DimensionSource, ProgressBarApi, ProgressBarHandle, ProgressScreen, ProgressScreenApi,
    ProgressTerminal, TestDimensionSource, TestTimeSource,
};

use crate::SyncSummary;

/// True when a sync left the library incomplete.
///
/// A skipped hierarchy path resolved no content hash, so nothing was written
/// for that entry. The whole-run icon and the `mediapm sync` exit status both
/// read this, so a caller cannot get a green summary line from a library that
/// is missing entries, nor a zero exit status from one.
#[must_use]
pub fn sync_summary_is_incomplete(summary: &SyncSummary) -> bool {
    summary.skipped_paths > 0
}

/// Icon for the whole-sync summary line.
///
/// An incomplete library outranks the counts, so it is [`StatusIcon::Error`]
/// even when every entry the run did handle was written. A workflow step that
/// failed is [`StatusIcon::Warning`] and no more: the paths that step owns may
/// still have materialized, which is what separates it from a skip.
#[must_use]
pub fn sync_summary_icon(summary: &SyncSummary) -> StatusIcon {
    if sync_summary_is_incomplete(summary) {
        StatusIcon::Error
    } else if summary.workflow_failed_steps > 0 {
        StatusIcon::Warning
    } else if summary.executed_instances > 0 || summary.materialized_paths > 0 {
        StatusIcon::Success
    } else {
        StatusIcon::NoChange
    }
}

/// Print a sync summary line with status icon and duration tracking.
///
/// Constructs the field list from [`SyncSummary`] and calls [`print_result`].
pub fn print_sync_summary(summary: &SyncSummary) {
    let icon = sync_summary_icon(summary);

    let mut fields: Vec<(&str, Box<dyn std::fmt::Display>)> = Vec::new();
    fields.push(("executed", Box::new(summary.executed_instances)));

    if summary.cached_instances > 0 {
        fields.push(("cached", Box::new(summary.cached_instances)));
    }
    if summary.materialized_paths > 0 {
        fields.push(("materialized", Box::new(summary.materialized_paths)));
    }
    if summary.skipped_paths > 0 {
        fields.push(("skipped", Box::new(summary.skipped_paths)));
    }
    if summary.removed_paths > 0 {
        fields.push(("removed", Box::new(summary.removed_paths)));
    }
    if summary.removed_empty_dirs > 0 {
        fields.push(("removed_empty", Box::new(summary.removed_empty_dirs)));
    }
    if summary.added_tools > 0 {
        fields.push(("added_tools", Box::new(summary.added_tools)));
    }
    if summary.updated_tools > 0 {
        fields.push(("updated_tools", Box::new(summary.updated_tools)));
    }
    if summary.pruned_tools > 0 {
        fields.push(("pruned_tools", Box::new(summary.pruned_tools)));
    }
    if summary.removed_tools > 0 {
        fields.push(("removed_tools", Box::new(summary.removed_tools)));
    }
    if summary.skipped_tools > 0 {
        fields.push(("skipped_tools", Box::new(summary.skipped_tools)));
    }
    if summary.workflow_failed_steps > 0 {
        fields.push(("failed", Box::new(summary.workflow_failed_steps)));
    }

    let ref_fields: Vec<(&str, &dyn std::fmt::Display)> =
        fields.iter().map(|(k, v)| (*k, v.as_ref())).collect();

    print_result(icon, "sync complete", &ref_fields, None);

    for warning in &summary.warnings {
        print_warning(warning);
    }
}
