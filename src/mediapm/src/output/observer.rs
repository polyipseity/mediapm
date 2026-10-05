//! CLI-side observer that renders [`SyncPhaseReport`]s as structured
//! result lines on stdout.
//!
//! This module lives in the `mediapm` crate (not in the library layer)
//! because it calls [`print_result`] which writes to stdout — a
//! side-effect that belongs at the CLI boundary.

use std::sync::Arc;

use mediapm_utils::report::{StatusIcon, print_result};

use crate::{
    MaterializationSyncSummary, SyncPhaseObserver, SyncPhaseReport, ToolsSyncSummary,
    WorkflowSyncSummary,
};

/// Renders [`SyncPhaseReport`]s as structured result lines.
///
/// Intended to be wrapped in `Arc` and passed as the observer in
/// [`SyncLibraryOptions`](crate::SyncLibraryOptions).
pub struct CliSyncObserver;

/// True when a tool sync changed the registry in any way.
fn has_tool_changes(s: &ToolsSyncSummary) -> bool {
    s.added_tools > 0 || s.updated_tools > 0 || s.pruned_tools > 0 || s.removed_tools > 0
}

/// Icon for the materialization phase line.
///
/// A missing path is a hierarchy entry the run could not produce, so the
/// library is short of it and the icon is [`StatusIcon::Error`] however many
/// other paths landed. The screen behind this line already ends red on a
/// missing path, and the line agrees with it.
///
/// A skipped path is not a missing path. It means the file was already there
/// with the bytes the run resolved, so it says nothing about how the run went
/// and is left out of the icon.
#[must_use]
pub fn materialization_icon(summary: &MaterializationSyncSummary) -> StatusIcon {
    if summary.missing_paths > 0 {
        StatusIcon::Error
    } else if summary.materialized_paths > 0 || summary.removed_paths > 0 {
        StatusIcon::Success
    } else {
        StatusIcon::NoChange
    }
}

/// Icon for the workflow phase line.
///
/// A failed step is [`StatusIcon::Error`], not a warning: the media that step
/// was to produce is missing from the library, and the whole-run summary and
/// the exit status agree. The same reasoning is spelled out in
/// [`crate::output::sync_outcome`], which is what this line answers to.
#[must_use]
pub fn workflow_icon(summary: &WorkflowSyncSummary) -> StatusIcon {
    if summary.failed_steps > 0 {
        StatusIcon::Error
    } else if summary.executed_instances == 0 && summary.cached_instances == 0 {
        StatusIcon::NoChange
    } else {
        StatusIcon::Success
    }
}

impl SyncPhaseObserver for CliSyncObserver {
    fn on_phase(&self, report: SyncPhaseReport) {
        match report {
            SyncPhaseReport::Tools(s) => {
                let icon =
                    if has_tool_changes(&s) { StatusIcon::Success } else { StatusIcon::NoChange };
                print_result(
                    icon,
                    "tools synced",
                    &[
                        ("added", &s.added_tools as &dyn std::fmt::Display),
                        ("updated", &s.updated_tools),
                        ("pruned", &s.pruned_tools),
                        ("removed", &s.removed_tools),
                    ],
                    None,
                );
                for warning in &s.warnings {
                    mediapm_utils::report::print_warning(warning);
                }
            }
            SyncPhaseReport::Workflow(s) => {
                print_result(
                    workflow_icon(&s),
                    "workflow",
                    &[
                        ("executed", &s.executed_instances as &dyn std::fmt::Display),
                        ("cached", &s.cached_instances),
                        ("failed", &s.failed_steps),
                    ],
                    None,
                );
            }
            SyncPhaseReport::Materialization(s) => {
                // `missing` is named only when it is above zero, which keeps a
                // clean run's line as short as it was.
                let mut fields: Vec<(&str, &dyn std::fmt::Display)> = vec![
                    ("paths", &s.materialized_paths as &dyn std::fmt::Display),
                    ("skipped", &s.skipped_paths),
                    ("removed", &s.removed_paths),
                ];
                if s.missing_paths > 0 {
                    fields.push(("missing", &s.missing_paths));
                }
                print_result(materialization_icon(&s), "materialized", &fields, None);
            }
        }
    }
}

/// Create a boxed `CliSyncObserver` suitable for passing as an observer.
#[must_use]
pub fn cli_observer() -> Arc<CliSyncObserver> {
    Arc::new(CliSyncObserver)
}
