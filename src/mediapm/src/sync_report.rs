//! Per-phase sync reporting: observer trait, phase reports, and options.
//!
//! The service layer owns all three progress screens and joins them
//! internally. A [`SyncPhaseObserver`] receives one [`SyncPhaseReport`]
//! immediately after each screen's `group.join()` returns, giving the
//! CLI layer control over *when* and *how* to render each result line.
//!
//! The service never prints — it only reports data. This keeps
//! `println!`-based output at the CLI boundary (where TTY-gating,
//! quiet-mode, and test isolation are handled), and makes the phase
//! reports directly assertable in tests.

use std::sync::Arc;

use mediapm_utils::progress::ProgressTerminal;

use crate::{MaterializationSyncSummary, ToolsSyncSummary, WorkflowSyncSummary};

/// Receives one [`SyncPhaseReport`] immediately after each sync phase's
/// progress screen finishes.
///
/// Implementations must be cheap and must not block; they are invoked on
/// the sync task between phases. `Arc<dyn ...>` mirrors the existing
/// `Arc<dyn ProgressScreenApi + Send + Sync>` plumbing used for the
/// progress screens and lets callers share one observer across phases.
///
/// Phase order is guaranteed: `Tools` → `Workflow` → `Materialization`.
pub trait SyncPhaseObserver: Send + Sync {
    /// Called once per completed phase, in execution order.
    fn on_phase(&self, report: SyncPhaseReport);
}

/// Outcome of one sync phase, delivered to a [`SyncPhaseObserver`].
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum SyncPhaseReport {
    /// Tool provisioning and removal finished.
    Tools(ToolsSyncSummary),
    /// Managed-workflow execution finished.
    Workflow(WorkflowSyncSummary),
    /// Hierarchy materialization finished.
    Materialization(MaterializationSyncSummary),
}

/// Options for [`MediaPmService::sync_library`](crate::MediaPmService::sync_library).
///
/// Uses `Default` to reproduce the behavior of the legacy
/// `sync_library(verify)` signature with no observer.
#[derive(Default)]
pub struct SyncLibraryOptions {
    /// Verify materialized content by re-hashing committed files.
    pub verify_materialization: bool,
    /// Force tag re-resolution instead of using cached metadata.
    pub check_tag_updates: bool,
    /// Suppress the three progress screens. Does NOT suppress result
    /// lines — result lines are governed by the observer (see
    /// [`SyncPhaseObserver`]).
    pub no_progress: bool,
    /// Receives one report per completed phase, in execution order.
    pub observer: Option<Arc<dyn SyncPhaseObserver + Send + Sync>>,
}

/// Progress plumbing overrides for a library sync.
///
/// [`MediaPmService::sync_library_with_progress_overrides`](crate::MediaPmService::sync_library_with_progress_overrides)
/// is the only entry point that accepts these, and it exists for one reason:
/// `indicatif` is a dev-dependency of this crate, so production code cannot
/// construct a `MultiProgress` itself and a test that must observe the frames a
/// sync draws has no other way to hand it a terminal.
///
/// `terminal: None` means "the sync builds its own" — the production shape. A
/// supplied terminal is used exactly as given, including when it is inert, and
/// it takes precedence over both suppression flags.
#[derive(Default)]
pub struct SyncProgressOverrides {
    /// The terminal every phase of the sync renders through.
    pub terminal: Option<ProgressTerminal>,
    /// Whether the terminal the sync would build itself must be inert.
    ///
    /// Unioned with [`SyncLibraryOptions::no_progress`]: either flag suppresses
    /// progress. Suppression is expressed by *not opening a live terminal*, not
    /// by opening one and ignoring it.
    pub no_progress: bool,
}
