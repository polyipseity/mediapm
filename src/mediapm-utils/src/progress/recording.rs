//! Recording progress tracker for testing progress-bar behavior without a
//! terminal.
//!
//! [`RecordingProgressTracker::ops`] to retrieve the recorded sequence for
//! verification.
//!
//! Only available when the `progress` feature is enabled.

#![allow(clippy::missing_panics_doc)]

use crate::progress::BarStyle;
use std::sync::{Arc, Mutex};
use std::time::{Duration, Instant};

/// Recorded progress operation for test assertions.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum ProgressOp {
    /// A bar was added to a group.
    AddBar {
        /// Total work units for the bar.
        total: u64,
        /// Display label for the bar.
        label: String,
    },
    /// `advance(delta)` was called.
    Advance {
        /// Number of work units advanced.
        delta: u64,
    },
    /// `set_total(total)` was called.
    SetTotal {
        /// New total work units.
        total: u64,
    },
    /// `set_position(pos)` was called.
    SetPosition {
        /// Absolute position to jump to.
        pos: u64,
    },
    /// `set_prefix_components(components)` was called.
    SetPrefixComponents {
        /// Marker component.
        marker: String,
        /// Tool name component.
        tool_name: String,
        /// Version component.
        version: String,
        /// Phase component.
        phase: String,
        /// Count (numerator) component.
        count: String,
        /// Total (denominator) component.
        total: String,
    },
    /// `set_suffix_components(components)` was called.
    SetSuffixComponents {
        /// Full suffix component set.
        components: crate::progress::SuffixComponents,
    },
    /// `set_truncation(truncation)` was called. Records the rendered
    /// prefix/suffix from the installed [`BarLabelTruncation`] at the
    /// recorder's configured width.
    SetTruncation {
        /// Rendered prefix string (full width, no truncation applied).
        prefix: String,
        /// Rendered suffix string (full width, no truncation applied).
        suffix: String,
    },
    /// `finish_success()` was called.
    FinishSuccess,
    /// `finish_error()` was called.
    FinishError,
    /// `finish_warning()` was called.
    FinishWarning,
    /// `finish_and_clear()` was called.
    FinishAndClear,
}

/// A recording progress tracker that records operations into a shared
/// [`Vec<ProgressOp>`] for test assertions.
///
/// Does not display anything. All handles added via
/// [`add_bar`](RecordingProgressTracker::add_bar) share the same
/// operation log.
#[derive(Clone)]
pub struct RecordingProgressTracker {
    ops: Arc<Mutex<Vec<ProgressOp>>>,
}

impl RecordingProgressTracker {
    /// Create a new empty recording tracker.
    #[must_use]
    pub fn new() -> Self {
        Self { ops: Arc::new(Mutex::new(Vec::new())) }
    }

    /// Create a tracker with a pre-recorded overall bar.
    ///
    /// The overall bar is recorded as an [`AddBar`](ProgressOp::AddBar) operation
    /// immediately. The returned [`RecordingTrackedHandle`] represents the overall bar
    /// for direct manipulation in tests.
    ///
    /// This mirrors the type-state builder pattern: callers who need an overall bar
    /// call `with_overall()` instead of `new()`, receiving the overall handle
    /// alongside the tracker.
    #[must_use]
    pub fn with_overall(label: &str, total: u64) -> (Self, RecordingTrackedHandle) {
        let tracker = Self::new();
        let overall = tracker.add_bar(total, label);
        (tracker, overall)
    }

    /// Record adding a bar with the given `total` and `label`.
    ///
    /// Returns a [`RecordingTrackedHandle`] that shares this tracker's
    /// operation log.
    #[must_use]
    pub fn add_bar(&self, total: u64, label: &str) -> RecordingTrackedHandle {
        self.ops
            .lock()
            .expect("recording lock")
            .push(ProgressOp::AddBar { total, label: label.to_string() });
        RecordingTrackedHandle {
            ops: self.ops.clone(),
            total: Some(total),
            start_time: Instant::now(),
            finished_elapsed: Arc::new(Mutex::new(None)),
        }
    }

    /// Return a snapshot of all recorded operations.
    #[must_use]
    pub fn ops(&self) -> Vec<ProgressOp> {
        self.ops.lock().expect("recording lock").clone()
    }

    /// Clear all recorded operations.
    pub fn clear(&self) {
        self.ops.lock().expect("recording lock").clear();
    }
}

impl Default for RecordingProgressTracker {
    fn default() -> Self {
        Self::new()
    }
}

/// A recording tracked handle that records operations into the shared log
/// of its parent [`RecordingProgressTracker`].
#[derive(Clone)]
pub struct RecordingTrackedHandle {
    ops: Arc<Mutex<Vec<ProgressOp>>>,
    total: Option<u64>,
    start_time: Instant,
    finished_elapsed: Arc<Mutex<Option<Duration>>>,
}

impl RecordingTrackedHandle {
    /// Create a standalone recording handle (not managed by a tracker).
    ///
    /// The handle has its own private operation log.
    #[must_use]
    pub fn new(total: u64) -> Self {
        Self {
            ops: Arc::new(Mutex::new(Vec::new())),
            total: Some(total),
            start_time: Instant::now(),
            finished_elapsed: Arc::new(Mutex::new(None)),
        }
    }

    /// Create a disabled (no-op) recording handle.
    ///
    /// All methods are no-ops; the handle logs nothing and reports
    /// [`total`](RecordingTrackedHandle::total) as 0.
    #[must_use]
    pub fn disabled() -> Self {
        Self {
            ops: Arc::new(Mutex::new(Vec::new())),
            total: None,
            start_time: Instant::now(),
            finished_elapsed: Arc::new(Mutex::new(None)),
        }
    }

    /// Return the total number of work units (0 = indeterminate/disabled).
    #[must_use]
    pub fn total(&self) -> u64 {
        self.total.unwrap_or(0)
    }

    /// Change the total mid-flight (recorded but not reflected in
    /// [`total()`](RecordingTrackedHandle::total) — use
    /// [`ops()`](RecordingTrackedHandle::ops) to verify).
    pub fn set_total(&self, total: u64) {
        self.ops.lock().expect("recording lock").push(ProgressOp::SetTotal { total });
    }

    /// Advance the handle by `delta` work units.
    pub fn advance(&self, delta: u64) {
        self.ops.lock().expect("recording lock").push(ProgressOp::Advance { delta });
    }

    /// Jump to an absolute position.
    pub fn set_position(&self, pos: u64) {
        self.ops.lock().expect("recording lock").push(ProgressOp::SetPosition { pos });
    }

    /// Set prefix components.
    pub fn set_prefix_components(&self, components: crate::progress::PrefixComponents) {
        self.ops.lock().expect("recording lock").push(ProgressOp::SetPrefixComponents {
            marker: components.marker,
            tool_name: components.tool_name,
            version: components.version,
            phase: components.phase,
            count: components.count,
            total: components.total,
        });
    }

    /// Set suffix components.
    pub fn set_suffix_components(&self, components: crate::progress::SuffixComponents) {
        self.ops
            .lock()
            .expect("recording lock")
            .push(ProgressOp::SetSuffixComponents { components });
    }

    /// Install a client-defined truncation implementation.
    ///
    /// Records the rendered prefix/suffix at a fixed recorder width so
    /// tests can assert the exact label content the coordinator produced.
    pub fn set_truncation(&self, truncation: &Arc<dyn crate::progress::BarLabelTruncation>) {
        let prefix = truncation.truncate_prefix(usize::MAX);
        let suffix = truncation.truncate_suffix(usize::MAX);
        self.ops.lock().expect("recording lock").push(ProgressOp::SetTruncation { prefix, suffix });
    }

    /// Set the visual style for the bar (see [`BarStyle`]).
    ///
    /// Recorded as a no-op marker today: the recording harness asserts
    /// worker-slot behavior via `tool_name`/`idle`
    /// [`SetPrefixComponents`](ProgressOp::SetPrefixComponents) transitions
    /// and `total`/`pos` deltas, not a style op. The method exists so
    /// callers can set the style uniformly through the
    /// [`ProgressBarApi`](crate::progress::ProgressBarApi) surface.
    pub fn set_style(&self, _style: BarStyle) {
        // No ProgressOp variant for style today; the recording harness
        // asserts worker-slot behavior via prefix/total deltas.
    }

    /// Mark as finished with success.
    pub fn finish_success(&self) {
        self.ops.lock().expect("recording lock").push(ProgressOp::FinishSuccess);
        self.mark_finished();
    }

    /// Mark as finished with an error.
    pub fn finish_error(&self) {
        self.ops.lock().expect("recording lock").push(ProgressOp::FinishError);
        self.mark_finished();
    }

    /// Mark as finished with a non-fatal warning.
    pub fn finish_warning(&self) {
        self.ops.lock().expect("recording lock").push(ProgressOp::FinishWarning);
        self.mark_finished();
    }

    /// Finish and clear from display.
    pub fn finish_and_clear(&self) {
        self.ops.lock().expect("recording lock").push(ProgressOp::FinishAndClear);
        self.mark_finished();
    }

    /// Return a snapshot of recorded operations for this handle.
    ///
    /// When created via [`RecordingProgressTracker::add_bar`], this
    /// returns the same shared log as all handles from that tracker.
    #[must_use]
    pub fn ops(&self) -> Vec<ProgressOp> {
        self.ops.lock().expect("recording lock").clone()
    }

    /// Return the elapsed duration (frozen after first finish method call).
    #[must_use]
    pub(crate) fn snapshot_elapsed(&self) -> Duration {
        if let Some(frozen) =
            *self.finished_elapsed.lock().expect("recording finished_elapsed lock")
        {
            frozen
        } else {
            self.start_time.elapsed()
        }
    }

    /// Capture the elapsed time if not already captured (idempotent).
    fn mark_finished(&self) {
        let mut elapsed = self.finished_elapsed.lock().expect("recording finished_elapsed lock");
        if elapsed.is_none() {
            *elapsed = Some(self.start_time.elapsed());
        }
    }
}
