//! CLI-side observer that renders [`SyncPhaseReport`]s as structured
//! result lines on stdout.
//!
//! This module lives in the `mediapm` crate (not in the library layer)
//! because it calls [`print_result`] which writes to stdout — a
//! side-effect that belongs at the CLI boundary.

use std::sync::Arc;

use mediapm_utils::report::{StatusIcon, print_result};

use crate::{MaterializationSyncSummary, SyncPhaseObserver, SyncPhaseReport, ToolsSyncSummary};

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
/// A skipped path is a hierarchy entry whose variant resolved no content hash,
/// so nothing was written for it and the entry is absent from the library. A
/// run that skipped anything has not materialized the library, so the icon is
/// [`StatusIcon::Error`] regardless of how many other paths landed. The screen
/// behind this line already ends red on a skip, and the line agrees with it.
#[must_use]
pub fn materialization_icon(summary: &MaterializationSyncSummary) -> StatusIcon {
    if summary.skipped_paths > 0 {
        StatusIcon::Error
    } else if summary.materialized_paths > 0 || summary.removed_paths > 0 {
        StatusIcon::Success
    } else {
        StatusIcon::NoChange
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
                print_result(
                    materialization_icon(&s),
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
