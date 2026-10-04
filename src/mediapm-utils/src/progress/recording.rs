//! Recording progress tracker for testing progress-bar behavior without a
//! terminal.
//!
//! [`RecordingProgressTracker::recorded`] returns the log with the bar each
//! operation came from, so a test can count what one row emitted without also
//! counting another row's. [`RecordingProgressTracker::ops`] drops the bar
//! identity and is for tests that only care about the sequence.
//!
//! Only available when the `progress` feature is enabled.

#![allow(clippy::missing_panics_doc)]

use crate::progress::BarStyle;
use std::sync::atomic::{AtomicUsize, Ordering};
use std::sync::{Arc, Mutex, RwLock};
use std::time::{Duration, Instant};

/// Which bar a recorded operation came from.
///
/// Indices are handed out in the order bars were added, so the first bar a
/// tracker adds is `Index(0)` for every screen that opens with an overall row.
/// An index rather than a label because the coordinator relabels a worker slot
/// on every dispatch and every outcome: a label is only as stable as the code
/// under test, so a test keyed on one fails or passes for the wrong reason when
/// the label text moves.
#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Hash)]
pub enum BarId {
    /// The `n`th bar added to the tracker, counting from 0.
    Index(usize),
    /// A standalone handle that owns its log and belongs to no group, so it
    /// has no index among a tracker's bars.
    Standalone,
}

impl BarId {
    /// Return the bar's index, or `None` for a standalone handle.
    ///
    /// A test that wants "every bar except the overall row" compares against
    /// [`BarId::Index`] values and lets [`BarId::Standalone`] fall out, which
    /// is what a standalone handle deserves since it is not one of the group's
    /// rows.
    #[must_use]
    pub fn index(self) -> Option<usize> {
        match self {
            Self::Index(index) => Some(index),
            Self::Standalone => None,
        }
    }
}

/// A recorded operation together with the bar it was recorded from.
///
/// A screen draws several rows at once and any of them can emit a status
/// marker, so an op on its own cannot say which row it came from.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct RecordedProgressOp {
    /// Bar the operation belongs to.
    pub bar: BarId,
    /// The operation as the bar received it.
    pub op: ProgressOp,
}

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
    /// prefix/suffix from the installed
    /// [`BarLabelTruncation`](crate::progress::BarLabelTruncation) at the
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
    /// `restart()` was called.
    Restart,
}

/// A recording progress tracker that records operations into a shared
/// [`Vec<ProgressOp>`] for test assertions.
///
/// Does not display anything. All handles added via
/// [`add_bar`](RecordingProgressTracker::add_bar) share the same
/// operation log.
#[derive(Clone)]
pub struct RecordingProgressTracker {
    ops: Arc<Mutex<Vec<RecordedProgressOp>>>,
    next_bar_index: Arc<AtomicUsize>,
}

impl RecordingProgressTracker {
    /// Create a new empty recording tracker.
    #[must_use]
    pub fn new() -> Self {
        Self {
            ops: Arc::new(Mutex::new(Vec::new())),
            next_bar_index: Arc::new(AtomicUsize::new(0)),
        }
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
        let bar = BarId::Index(self.next_bar_index.fetch_add(1, Ordering::AcqRel));
        self.push(bar, ProgressOp::AddBar { total, label: label.to_string() });
        RecordingTrackedHandle {
            ops: self.ops.clone(),
            bar,
            total: Some(total),
            start_time: RwLock::new(Instant::now()),
            finished_elapsed: Arc::new(Mutex::new(None)),
        }
    }

    /// Append one operation for `bar`.
    fn push(&self, bar: BarId, op: ProgressOp) {
        self.ops.lock().expect("recording lock").push(RecordedProgressOp { bar, op });
    }

    /// Record adding a bar whose prefix fields are given as components.
    ///
    /// The log keeps the same [`AddBar`](ProgressOp::AddBar) shape as the label
    /// path, with the label the components render to, so a test asserting on
    /// the recorded bar cannot tell the two routes apart.
    #[must_use]
    pub fn add_bar_with_prefix(
        &self,
        total: u64,
        prefix: &crate::progress::PrefixComponents,
    ) -> RecordingTrackedHandle {
        self.add_bar(total, &prefix.display_label())
    }

    /// Return a snapshot of all recorded operations with their bar identity.
    ///
    /// This is what a per-row assertion needs: filter on
    /// [`RecordedProgressOp::bar`] before counting, so an overall row's
    /// terminal marker cannot land in a worker slot's count.
    #[must_use]
    pub fn recorded(&self) -> Vec<RecordedProgressOp> {
        self.ops.lock().expect("recording lock").clone()
    }

    /// Return a snapshot of the recorded operations with the bar identity
    /// dropped.
    ///
    /// Every bar lands in one sequence, so this cannot answer "how many did
    /// this row do". Use [`recorded`](Self::recorded) when the row matters.
    #[must_use]
    pub fn ops(&self) -> Vec<ProgressOp> {
        self.ops.lock().expect("recording lock").iter().map(|entry| entry.op.clone()).collect()
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
pub struct RecordingTrackedHandle {
    ops: Arc<Mutex<Vec<RecordedProgressOp>>>,
    bar: BarId,
    total: Option<u64>,
    start_time: RwLock<Instant>,
    finished_elapsed: Arc<Mutex<Option<Duration>>>,
}

impl Clone for RecordingTrackedHandle {
    fn clone(&self) -> Self {
        Self {
            ops: Arc::clone(&self.ops),
            bar: self.bar,
            total: self.total,
            start_time: RwLock::new(*self.start_time.read().expect("recording start_time lock")),
            finished_elapsed: Arc::clone(&self.finished_elapsed),
        }
    }
}

impl RecordingTrackedHandle {
    /// Create a standalone recording handle (not managed by a tracker).
    ///
    /// The handle has its own private operation log, so its operations carry
    /// [`BarId::Standalone`] rather than an index into some group's bars.
    #[must_use]
    pub fn new(total: u64) -> Self {
        Self {
            ops: Arc::new(Mutex::new(Vec::new())),
            bar: BarId::Standalone,
            total: Some(total),
            start_time: RwLock::new(Instant::now()),
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
            bar: BarId::Standalone,
            total: None,
            start_time: RwLock::new(Instant::now()),
            finished_elapsed: Arc::new(Mutex::new(None)),
        }
    }

    /// Return the bar this handle records for.
    ///
    /// Set once when the handle is created, from the tracker's add order, and
    /// never changed afterwards, so a filter on it stays valid for the whole
    /// run even though the row's label text moves.
    #[must_use]
    pub fn bar(&self) -> BarId {
        self.bar
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
        self.push(ProgressOp::SetTotal { total });
    }

    /// Advance the handle by `delta` work units.
    pub fn advance(&self, delta: u64) {
        self.push(ProgressOp::Advance { delta });
    }

    /// Jump to an absolute position.
    pub fn set_position(&self, pos: u64) {
        self.push(ProgressOp::SetPosition { pos });
    }

    /// Set prefix components.
    pub fn set_prefix_components(&self, components: crate::progress::PrefixComponents) {
        self.push(ProgressOp::SetPrefixComponents {
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
        self.push(ProgressOp::SetSuffixComponents { components });
    }

    /// Install a client-defined truncation implementation.
    ///
    /// Records the rendered prefix/suffix at a fixed recorder width so
    /// tests can assert the exact label content the coordinator produced.
    pub fn set_truncation(&self, truncation: &Arc<dyn crate::progress::BarLabelTruncation>) {
        let prefix = truncation.truncate_prefix(usize::MAX);
        let suffix =
            truncation.truncate_suffix(usize::MAX, &crate::progress::SuffixComponents::default());
        self.push(ProgressOp::SetTruncation { prefix, suffix });
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
        self.push(ProgressOp::FinishSuccess);
        self.mark_finished();
    }

    /// Mark as finished with an error.
    pub fn finish_error(&self) {
        self.push(ProgressOp::FinishError);
        self.mark_finished();
    }

    /// Mark as finished with a non-fatal warning.
    pub fn finish_warning(&self) {
        self.push(ProgressOp::FinishWarning);
        self.mark_finished();
    }

    /// Finish and clear from display.
    pub fn finish_and_clear(&self) {
        self.push(ProgressOp::FinishAndClear);
        self.mark_finished();
    }

    /// Re-activate a finished bar. Clears the terminal state marker,
    /// resets elapsed tracking, and marks the bar dirty so the next
    /// tick redraws it as active.
    pub fn restart(&self) {
        self.push(ProgressOp::Restart);
        *self.finished_elapsed.lock().expect("recording finished_elapsed lock") = None;
        *self.start_time.write().expect("recording start_time lock") = Instant::now();
    }

    /// Return a snapshot of recorded operations for this handle with the bar
    /// identity dropped.
    ///
    /// When created via [`RecordingProgressTracker::add_bar`], this
    /// returns the same shared log as all handles from that tracker.
    #[must_use]
    pub fn ops(&self) -> Vec<ProgressOp> {
        self.ops.lock().expect("recording lock").iter().map(|entry| entry.op.clone()).collect()
    }

    /// Return a snapshot of recorded operations for this handle, each tagged
    /// with this handle's [`BarId`].
    #[must_use]
    pub fn recorded(&self) -> Vec<RecordedProgressOp> {
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
            self.start_time.read().expect("recording start_time lock").elapsed()
        }
    }

    /// Capture the elapsed time if not already captured (idempotent).
    fn mark_finished(&self) {
        let mut elapsed = self.finished_elapsed.lock().expect("recording finished_elapsed lock");
        if elapsed.is_none() {
            *elapsed = Some(self.start_time.read().expect("recording start_time lock").elapsed());
        }
    }

    /// Append one operation tagged with this handle's bar identity.
    fn push(&self, op: ProgressOp) {
        self.ops.lock().expect("recording lock").push(RecordedProgressOp { bar: self.bar, op });
    }
}
