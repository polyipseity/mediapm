//! CLI-side observer that renders [`SyncPhaseReport`]s as structured
//! result lines on stdout.
//!
//! This module lives in the `mediapm` crate (not in the library layer)
//! because it calls [`print_result`] which writes to stdout — a
//! side-effect that belongs at the CLI boundary.

use std::sync::Arc;

use mediapm_utils::report::{StatusIcon, print_result};

use crate::{SyncPhaseObserver, SyncPhaseReport, ToolsSyncSummary};

/// Renders [`SyncPhaseReport`]s as structured result lines.
///
/// Intended to be wrapped in `Arc` and passed as the observer in
/// [`SyncLibraryOptions`](crate::SyncLibraryOptions).
pub struct CliSyncObserver;

fn has_tool_changes(s: &ToolsSyncSummary) -> bool {
    s.added_tools > 0 || s.updated_tools > 0 || s.pruned_tools > 0 || s.removed_tools > 0
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
                let icon = if s.failed_steps > 0 {
                    StatusIcon::Warning
                } else if s.executed_instances == 0 && s.cached_instances == 0 {
                    StatusIcon::NoChange
                } else {
                    StatusIcon::Success
                };
                print_result(
                    icon,
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
                let icon = if s.materialized_paths > 0 || s.removed_paths > 0 {
                    StatusIcon::Success
                } else {
                    StatusIcon::NoChange
                };
                print_result(
                    icon,
                    "materialized",
                    &[
                        ("paths", &s.materialized_paths as &dyn std::fmt::Display),
                        ("skipped", &s.skipped_paths),
                        ("removed", &s.removed_paths),
                    ],
                    None,
                );
            }
        }
    }
}

/// Create a boxed `CliSyncObserver` suitable for passing as an observer.
#[must_use]
pub fn cli_observer() -> Arc<CliSyncObserver> {
    Arc::new(CliSyncObserver)
}
