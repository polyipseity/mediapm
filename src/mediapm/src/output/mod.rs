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
//!
//! The whole-run summary rules live here too, in [`sync_outcome`] and
//! [`sync_summary_icon`] for a library [`crate::SyncSummary`] and in
//! [`tool_sync_outcome`] and [`tool_sync_icon`] for a
//! [`crate::ToolsSyncSummary`], because the CLI has to read a summary and
//! library code is the only place both of them can reach.

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

use crate::{SyncSummary, ToolsSyncSummary};

/// How a finished sync amounts to something for a caller.
///
/// Three states, because the counters say three different things. A binary
/// that cannot tell them apart cannot be scripted against.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum SyncOutcome {
    /// The library holds what the config asked for. An entry that was already
    /// correct lands here: there was nothing to write, so there is nothing to
    /// report either.
    Clean,
    /// The run left the library correct and said something worth reading.
    Warning,
    /// The library is short of what the config asked for.
    Error,
}

/// The one rule that reads a [`SyncSummary`] and says how the run went.
///
/// Everything downstream reads this: [`sync_summary_icon`] turns it into the
/// summary line's icon, and `sync_exit_status` in `src/mediapm/src/main.rs`
/// turns it into the process status. Neither derives its own answer from the
/// counters, because two rules over the same fields drift apart.
///
/// A failed workflow step is [`SyncOutcome::Error`]. It used to be a warning on
/// the grounds that the paths the step owns may still have materialized. That
/// is the claim that does not hold: those paths are materialized from what the
/// failing step was supposed to produce, so a step that failed and paths that
/// materialized cannot both be true of the same entry. The media is not on
/// disk, and a green run tells a pipeline the opposite.
///
/// A normal skip is [`SyncOutcome::Clean`]. The recorded hash matched the
/// resolved one and the target was already the right length, so the run had
/// nothing to write and nothing to apologize for.
#[must_use]
pub fn sync_outcome(summary: &SyncSummary) -> SyncOutcome {
    if summary.missing_paths > 0 || summary.workflow_failed_steps > 0 {
        SyncOutcome::Error
    } else if !summary.warnings.is_empty() {
        SyncOutcome::Warning
    } else {
        SyncOutcome::Clean
    }
}

/// True when a sync left the library incomplete.
///
/// An incomplete library outranks every count, so this is what both the
/// summary line and the exit status read. It is [`SyncOutcome::Error`] spelled
/// as a predicate, kept for callers that want the question without the
/// three-way answer.
#[must_use]
pub fn sync_summary_is_incomplete(summary: &SyncSummary) -> bool {
    sync_outcome(summary) == SyncOutcome::Error
}

/// Icon for the whole-sync summary line.
///
/// An incomplete library outranks the counts, so it is [`StatusIcon::Error`]
/// even when every entry the run did handle was written. A warning-only run is
/// [`StatusIcon::Warning`]. A clean run is [`StatusIcon::Success`] when it did
/// some work and [`StatusIcon::NoChange`] when it did not, which is the
/// normal skip: the library was already right and still is.
#[must_use]
pub fn sync_summary_icon(summary: &SyncSummary) -> StatusIcon {
    match sync_outcome(summary) {
        SyncOutcome::Error => StatusIcon::Error,
        SyncOutcome::Warning => StatusIcon::Warning,
        SyncOutcome::Clean if summary.executed_instances > 0 || summary.materialized_paths > 0 => {
            StatusIcon::Success
        }
        SyncOutcome::Clean => StatusIcon::NoChange,
    }
}

/// How a finished tool sync amounts to something for a caller.
///
/// Two states, and the missing third is the rule. A tool that failed to
/// provision leaves every other tool in the run registered, and running the
/// command again retries the failed one, so the run broke nothing and there is
/// no error state to reach for. A tool sync that cannot run at all returns a
/// [`MediaPmError`](crate::MediaPmError) instead, and the CLI leaves through
/// the error path without consulting this answer.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum ToolSyncOutcome {
    /// Every desired tool resolved and provisioned.
    Clean,
    /// At least one tool failed to provision. The rest are registered.
    Warning,
}

/// The rule that reads a [`ToolsSyncSummary`] and says how the tool sync went.
///
/// It sits beside [`sync_outcome`] so the two answers stay in one place. A
/// [`SyncSummary`] can be short a path and so has an error state; a
/// [`ToolsSyncSummary`] cannot, and saying so here is cheaper than letting a
/// caller discover it from an exit status.
///
/// The CLI's tool-sync exit status reads this function rather than
/// `summary.warnings`, so the summary line's icon and the process status cannot
/// come to different answers about the same run.
#[must_use]
pub fn tool_sync_outcome(summary: &ToolsSyncSummary) -> ToolSyncOutcome {
    if summary.warnings.is_empty() { ToolSyncOutcome::Clean } else { ToolSyncOutcome::Warning }
}

/// Icon for the `mediapm tool sync` summary line.
///
/// A tool that failed to provision turns the line yellow. The rest of the run
/// is registered, so this is not an error, and a green line would hide the one
/// tool the user still has to deal with.
#[must_use]
pub fn tool_sync_icon(summary: &ToolsSyncSummary) -> StatusIcon {
    match tool_sync_outcome(summary) {
        ToolSyncOutcome::Clean => StatusIcon::Success,
        ToolSyncOutcome::Warning => StatusIcon::Warning,
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
    if summary.missing_paths > 0 {
        fields.push(("missing", Box::new(summary.missing_paths)));
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
