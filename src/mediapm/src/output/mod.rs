//! CLI-friendly output formatting for sync summaries, progress bars, and
//! diagnostic messages.
//!
//! # Submodules
//!
//! * [`progress`] — progress bars with [`ProgressGroup`] and [`ProgressHandle`]
//! * [`report`] — result lines with status icons via [`print_result`] and friends

pub mod progress;
pub mod report;

// No imports needed at this level — submodules handle their own.

pub use progress::{ProgressGroup, ProgressHandle};
pub use report::{
    StatusIcon, print_error, print_heading, print_hint, print_result, print_status_report,
    print_warning,
};

use crate::SyncSummary;

/// Print a sync summary line with status icon and duration tracking.
///
/// This is the original public API kept for backward compatibility. It
/// computes duration from an optional start time, constructs the field
/// list from [`SyncSummary`], and calls [`print_result`].
///
/// Pass `elapsed` = `None` to omit the duration suffix.
pub fn print_sync_summary(summary: &SyncSummary) {
    let icon = if summary.executed_instances > 0 || summary.materialized_paths > 0 {
        StatusIcon::Success
    } else {
        StatusIcon::NoChange
    };

    let mut fields: Vec<(&str, Box<dyn std::fmt::Display>)> = Vec::new();
    let e = summary.executed_instances;
    fields.push(("executed", Box::new(e)));

    if summary.cached_instances > 0 {
        fields.push(("cached", Box::new(summary.cached_instances)));
    }
    if summary.rematerialized_instances > 0 {
        fields.push(("rematerialized", Box::new(summary.rematerialized_instances)));
    }
    if summary.materialized_paths > 0 {
        fields.push(("materialized", Box::new(summary.materialized_paths)));
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

    let ref_fields: Vec<(&str, &dyn std::fmt::Display)> =
        fields.iter().map(|(k, v)| (*k, v.as_ref())).collect();

    print_result(icon, "sync complete", &ref_fields, None);

    for warning in &summary.warnings {
        print_warning(warning);
    }
}
