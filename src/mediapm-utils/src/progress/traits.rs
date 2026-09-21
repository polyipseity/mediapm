//! Shared API traits and the [`BarStyle`] marker for progress-bar dependency
//! injection.
//!
//! These types are feature-gated behind `progress`. Both [`TrackedHandle`] and
//! [`recording::RecordingTrackedHandle`] implement [`ProgressBarApi`]; both
//! [`ProgressGroup`] and [`recording::RecordingProgressTracker`] implement
//! [`ProgressGroupApi`].

use crate::progress::BarLabelTruncation;
use crate::progress::inner::{
    PrefixComponents, ProgressGroup, SuffixComponents, TrackSnapshot, TrackedHandle,
};
use std::sync::Arc;

/// Visual style for a child progress bar.
///
/// A marker stored on [`SharedState`] and read by the renderer's single push
/// point ([`ProgressRenderer::sync_snapshot_to_bar`]); it does **not** change
/// color or overall-bar semantics.
///
/// - [`StepCount`](BarStyle::StepCount) — the default. Shows a `count/total`
///   ratio driven by `advance`/`set_position`/`set_total`, with the standard
///   `tool_name [version] [phase] [count/total]` prefix and the
///   `count/total elapsed rate [eta] custom` suffix. Used by the sync,
///   materialization, and legacy per-step workflow screens.
/// - [`WorkerSpinner`](BarStyle::WorkerSpinner) — a fixed worker-slot bar
///   driven by **per-worker** state rather than a per-step or global total.
///   The coordinator populates `prefix_components`/`suffix_components`
///   directly (no `version`/`phase`; `custom` = `` `<status>` `` `[ <F> failed][ <R>
///   retry]`), and the renderer applies a `0/0` div-by-zero guard (renders
///   `total = 1, pos = 0` when the worker's assigned count is `0`) so an
///   idle worker shows an empty all-░ bar. The same `wide_bar` child template
///   as `StepCount` is used; only the field population differs.
///
/// Set via [`ProgressBarApi::set_style`] (or [`TrackedHandle::set_style`]) after
/// [`ProgressGroup::add_bar`]. Defaults to [`StepCount`](BarStyle::StepCount).
#[cfg(feature = "progress")]
#[derive(Debug, Clone, Copy, PartialEq, Eq, Default)]
pub enum BarStyle {
    /// Standard `count/total` child bar (default).
    #[default]
    StepCount,
    /// Fixed worker-slot bar driven by per-worker state.
    WorkerSpinner,
}

/// Minimum progress-bar handle API for dependency injection.
///
/// Both [`TrackedHandle`] and
/// [`RecordingTrackedHandle`](crate::progress::recording::RecordingTrackedHandle) implement
/// this trait, allowing consumer functions to accept either a real display
/// bar or a recording bar for testing.
#[cfg(feature = "progress")]
pub trait ProgressBarApi: Send + Sync {
    /// Advance the bar by `delta` work units.
    fn advance(&self, delta: u64);
    /// Mark the bar as finished successfully.
    fn finish_success(&self);
    /// Mark the bar as finished with an error.
    fn finish_error(&self);
    /// Mark the bar as finished with a non-fatal warning.
    fn finish_warning(&self);
    /// Return a data-copy snapshot of the tracking state.
    fn snapshot(&self) -> TrackSnapshot;
    /// Returns `true` if the handle has been finished/abandoned.
    fn is_finished(&self) -> bool;
    /// Jump to an absolute position.
    fn set_position(&self, pos: u64);
    /// Change the total mid-flight for dynamic workloads.
    fn set_total(&self, total: u64);
    /// Set prefix components for source-data-based truncation.
    fn set_prefix_components(&self, components: PrefixComponents);
    /// Set suffix components for source-data-based truncation.
    fn set_suffix_components(&self, components: SuffixComponents);
    /// Install client-supplied truncation logic (see [`BarLabelTruncation`]).
    ///
    /// When set, the renderer's single push point calls the trait directly
    /// to obtain the final prefix/suffix display strings, instead of the
    /// built-in component rendering. The client owns the field layout and
    /// truncation order. When unset, the built-in rendering remains the
    /// fallback.
    fn set_truncation(&self, truncation: Arc<dyn BarLabelTruncation>);
    /// Re-activate a finished bar. Clears the terminal state marker,
    /// resets elapsed tracking, and marks the bar dirty so the next
    /// tick redraws it as active.
    fn restart(&self);
    /// Set the visual style for the bar (see [`BarStyle`]).
    ///
    /// Defaults to [`StepCount`](BarStyle::StepCount). Callers that own a
    /// worker-slot bar set [`WorkerSpinner`](BarStyle::WorkerSpinner) after
    /// [`add_bar`](crate::progress::ProgressGroup::add_bar) so the renderer
    /// applies the style-specific `0/0` div-by-zero guard.
    fn set_style(&self, style: BarStyle);
}

#[cfg(feature = "progress")]
impl ProgressBarApi for TrackedHandle {
    fn advance(&self, delta: u64) {
        TrackedHandle::advance(self, delta);
    }
    fn finish_success(&self) {
        TrackedHandle::finish_success(self);
    }
    fn finish_error(&self) {
        TrackedHandle::finish_error(self);
    }
    fn finish_warning(&self) {
        TrackedHandle::finish_warning(self);
    }
    fn snapshot(&self) -> TrackSnapshot {
        TrackedHandle::snapshot(self)
    }
    fn is_finished(&self) -> bool {
        TrackedHandle::is_finished(self)
    }
    fn set_position(&self, pos: u64) {
        TrackedHandle::set_position(self, pos);
    }
    fn set_total(&self, total: u64) {
        TrackedHandle::set_total(self, total);
    }
    fn set_prefix_components(&self, components: PrefixComponents) {
        TrackedHandle::set_prefix_components(self, components);
    }
    fn set_suffix_components(&self, components: SuffixComponents) {
        TrackedHandle::set_suffix_components(self, components);
    }
    fn set_truncation(&self, truncation: Arc<dyn BarLabelTruncation>) {
        TrackedHandle::set_truncation(self, truncation);
    }
    fn restart(&self) {
        TrackedHandle::restart(self);
    }
    fn set_style(&self, style: BarStyle) {
        TrackedHandle::set_style(self, style);
    }
}

/// Minimum progress-group API for dependency injection.
///
/// Both [`ProgressGroup`] and
/// [`RecordingProgressTracker`](crate::progress::recording::RecordingProgressTracker) implement
/// this trait, allowing consumer functions to accept either a real display
/// group or a recording group for testing.
#[cfg(feature = "progress")]
pub trait ProgressGroupApi {
    /// Add a child bar and return an [`Arc`]-wrapped handle.
    fn add_bar(&self, total: u64, label: &str) -> Arc<dyn ProgressBarApi>;
    /// Block until all bars in the group reach a finished state.
    fn join(&self);
}

#[cfg(feature = "progress")]
impl ProgressGroupApi for ProgressGroup {
    fn add_bar(&self, total: u64, label: &str) -> Arc<dyn ProgressBarApi> {
        Arc::new(ProgressGroup::add_bar(self, total, label))
    }
    fn join(&self) {
        ProgressGroup::join(self);
    }
}
