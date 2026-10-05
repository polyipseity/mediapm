//! Tracked progress state: what a [`ProgressBarHandle`] advances and what the
//! renderer reads at the top of a frame.
//!
//! Nothing here touches indicatif. A track is an atomic position, an atomic
//! total, and the fields that decide how a row is drawn, so a screen can be
//! driven, snapshotted and asserted on without a terminal in the picture. The
//! one place a track and the terminal meet is the renderer's push point, which
//! reads a [`TrackSnapshot`] rather than the state behind it.
//!
//! The module is the pure half of the renderer: [`TrackStatus`] names the
//! terminal states a row can be in, [`SharedState`] holds one track's fields,
//! [`TrackSnapshot`] is the frozen read the renderer draws from, and
//! [`ProgressBarHandle`] is the public surface a producer writes through.

use std::sync::atomic::{AtomicBool, AtomicU8, AtomicU64, Ordering};
use std::sync::{Arc, RwLock};
use std::time::{Duration, Instant};

use super::super::{PrefixComponents, RealTimeSource, SuffixComponents, TimeSource};
use super::super::{prefix_components_from_str, render_prefix_components};
use crate::progress::BarStyle;

// ---- SharedState (pure tracking, no indicatif dependency) -------------

/// Status of a tracked progress bar.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum TrackStatus {
    /// Bar is still active (work in progress).
    Active,
    /// Bar finished successfully.
    Success,
    /// Bar finished with an error.
    Failed,
    /// Bar finished with a non-fatal warning (kept visible).
    Warning,
}

impl TrackStatus {
    /// Compact numeric encoding for dedup comparison (Active=0, Success=1,
    /// Failed=2, Warning=3).
    ///
    /// `pub(super)` because the parent's per-slot style cache stores this value
    /// to skip a redundant `set_style`.
    pub(super) fn code(self) -> u8 {
        match self {
            Self::Active => 0,
            Self::Success => 1,
            Self::Failed => 2,
            Self::Warning => 3,
        }
    }
}

/// Shared mutable state for a tracked progress handle.
///
/// Interior mutability via atomics for numeric fields and [`RwLock`] for
/// string fields. [`Send`] + [`Sync`] when wrapped in [`Arc`].
pub(crate) struct SharedState {
    position: AtomicU64,
    pub(super) total: AtomicU64,
    pub(super) label: RwLock<String>,
    prefix_components: RwLock<PrefixComponents>,
    suffix_components: RwLock<SuffixComponents>,
    /// Client-supplied truncation logic. When `Some`, the renderer calls
    /// it directly at the single push point to obtain the final
    /// prefix/suffix display strings. When `None`, the renderer falls
    /// back to its built-in component rendering. mediapm-utils owns no
    /// field layout — the client does.
    pub(super) truncation: RwLock<Option<Arc<dyn crate::progress::BarLabelTruncation>>>,
    status: AtomicU8,
    /// `true` when this handle's rendered state changed since the last push,
    /// so the renderer must draw the slot again. Cleared by the renderer's
    /// single push point after it has copied the values onto the bar.
    pub(super) dirty: AtomicBool,
    disabled: AtomicBool,
    /// Visual style for the bar (see [`BarStyle`]). Defaults to
    /// [`StepCount`](BarStyle::StepCount); set to
    /// [`WorkerSpinner`](BarStyle::WorkerSpinner) for fixed worker-slot
    /// bars. Read by the renderer's single push point to apply the
    /// style-specific `0/0` div-by-zero guard.
    style: AtomicU8,
    start_time: RwLock<Instant>,
    finished_elapsed: RwLock<Option<Duration>>,
    time_source: Arc<dyn TimeSource>,
}

impl SharedState {
    pub(crate) fn new(total: u64, label: &str) -> Self {
        Self::with_time_source(total, label, Arc::new(RealTimeSource))
    }

    /// Create shared state, parsing `label` into [`PrefixComponents`]
    /// at construction time.
    ///
    /// The **single canonical construction path** for prefix data: the
    /// `add_bar`/`with_overall` label is parsed once here via
    /// [`prefix_components_from_str`], so even bars whose label was never
    /// touched by [`ProgressBarHandle::set_prefix_components`] carry
    /// structured components (tool name, version, phase, count/total) for
    /// [`semantic_truncate_prefix`](super::super::semantic_truncate_prefix)
    /// to truncate field-by-field. The legacy `set_prefix(String)` API has
    /// been removed — this construction-time parse plus
    /// [`ProgressBarHandle::set_prefix_components`] is the only prefix
    /// mechanism.
    pub(crate) fn with_time_source(
        total: u64,
        label: &str,
        time_source: Arc<dyn TimeSource>,
    ) -> Self {
        Self::with_time_source_and_style(total, label, BarStyle::StepCount, time_source)
    }

    /// Create shared state with an explicit [`BarStyle`].
    ///
    /// See [`with_time_source`](Self::with_time_source) for the canonical
    /// construction contract; this variant additionally seeds the style
    /// marker so the renderer can apply style-specific rendering (e.g. the
    /// `WorkerSpinner` `0/0` guard).
    pub(crate) fn with_time_source_and_style(
        total: u64,
        label: &str,
        style: BarStyle,
        time_source: Arc<dyn TimeSource>,
    ) -> Self {
        Self::build(total, label.to_string(), prefix_components_from_str(label), style, time_source)
    }

    /// Create shared state from prefix components rather than a label.
    ///
    /// The components are kept as given and the `label` is derived from them,
    /// so a field the caller owns survives whatever shape it has. The label
    /// the string path seeds can only report what the render draws, which is
    /// why a bar carrying structure is built here.
    pub(crate) fn with_prefix(
        total: u64,
        prefix: &PrefixComponents,
        style: BarStyle,
        time_source: Arc<dyn TimeSource>,
    ) -> Self {
        Self::build(total, prefix.display_label(), prefix.clone(), style, time_source)
    }

    /// Shared construction body for the label and component entry points.
    fn build(
        total: u64,
        label: String,
        prefix_components: PrefixComponents,
        style: BarStyle,
        time_source: Arc<dyn TimeSource>,
    ) -> Self {
        Self {
            position: AtomicU64::new(0),
            total: AtomicU64::new(total),
            label: RwLock::new(label),
            prefix_components: RwLock::new(prefix_components),
            suffix_components: RwLock::new(SuffixComponents::default()),
            truncation: RwLock::new(None),
            status: AtomicU8::new(0),
            dirty: AtomicBool::new(true),
            disabled: AtomicBool::new(false),
            style: AtomicU8::new(style as u8),
            start_time: RwLock::new(time_source.now()),
            finished_elapsed: RwLock::new(None),
            time_source,
        }
    }

    pub(super) fn snapshot(&self) -> TrackSnapshot {
        let pc = self.prefix_components.read().expect("shared_state prefix_components lock");
        let sc = self.suffix_components.read().expect("shared_state suffix_components lock");
        let status = match self.status.load(Ordering::Relaxed) {
            0 => TrackStatus::Active,
            // Code 1 is the canonical Success encoding; the wildcard
            // fallback also maps to Success for forward-compatibility
            // with unknown status codes (see snapshot_status_code_round_trip).
            #[allow(clippy::match_same_arms)]
            1 => TrackStatus::Success,
            2 => TrackStatus::Failed,
            3 => TrackStatus::Warning,
            _ => TrackStatus::Success,
        };
        // Fold the status marker into the prefix components so the marker
        // is truncatable data rather than fixed overhead, then render the
        // full display string (reset + colored marker + components).
        let mut pc = pc.clone();
        pc.marker = match status {
            TrackStatus::Failed => "F".to_string(),
            TrackStatus::Warning => "W".to_string(),
            _ => String::new(),
        };
        TrackSnapshot {
            position: self.position.load(Ordering::Relaxed),
            total: self.total.load(Ordering::Relaxed),
            label: self.label.read().expect("shared_state label lock").clone(),
            prefix: render_prefix_components(&pc, status),
            prefix_components: pc,
            suffix: sc.custom.clone(),
            suffix_components: sc.clone(),
            status,
            elapsed: self.elapsed(),
        }
    }

    pub(crate) fn elapsed(&self) -> Duration {
        if let Some(frozen) =
            *self.finished_elapsed.read().expect("shared_state finished_elapsed lock")
        {
            frozen
        } else {
            self.time_source.now() - *self.start_time.read().expect("shared_state start_time lock")
        }
    }

    /// Re-activate a finished bar. Clears the terminal state marker,
    /// resets elapsed tracking, and marks the bar dirty so the next
    /// tick redraws it as active.
    pub(crate) fn restart(&self) {
        self.status.store(0, Ordering::Relaxed); // Active
        *self.finished_elapsed.write().expect("shared_state finished_elapsed lock") = None;
        *self.start_time.write().expect("shared_state start_time lock") = self.time_source.now();
        self.dirty.store(true, Ordering::Release);
    }

    pub(crate) fn mark_finished(&self) {
        self.dirty.store(true, Ordering::Release);
        let elapsed =
            self.time_source.now() - *self.start_time.read().expect("shared_state start_time lock");
        *self.finished_elapsed.write().expect("shared_state finished_elapsed lock") = Some(elapsed);
    }

    /// Whether this handle has reached a terminal state (`Success`,
    /// `Warning`, `Failed`). Read by the renderer's grid bookkeeping, which
    /// recycles finished slots and keeps active ones; not part of the public
    /// handle API, whose `is_finished` lives on `ProgressBarHandle`.
    pub(super) fn is_finished(&self) -> bool {
        self.status.load(Ordering::Relaxed) != 0
    }

    pub(super) fn is_cleared(&self) -> bool {
        self.status.load(Ordering::Relaxed) == 5
    }

    pub(super) fn style(&self) -> BarStyle {
        match self.style.load(Ordering::Relaxed) {
            1 => BarStyle::WorkerSpinner,
            _ => BarStyle::StepCount,
        }
    }

    fn set_style(&self, style: BarStyle) {
        self.style.store(style as u8, Ordering::Relaxed);
        self.dirty.store(true, Ordering::Release);
    }
}

/// Data-copy snapshot of a tracked handle's state at one point in time.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct TrackSnapshot {
    /// Current position (work completed).
    pub position: u64,
    /// Total work units.
    pub total: u64,
    /// Display label.
    pub label: String,
    /// Rendered prefix (built from [`prefix_components`](Self::prefix_components)).
    pub prefix: String,
    /// Source components for the prefix.
    pub prefix_components: PrefixComponents,
    /// Custom suffix text appended to the auto-computed RHS (empty = none).
    pub suffix: String,
    /// Source components for the suffix.
    pub suffix_components: SuffixComponents,
    /// Current status.
    pub status: TrackStatus,
    /// Elapsed time since the handle was created (frozen on finish).
    pub elapsed: Duration,
}

// ---- ProgressBarHandle ----------------------------------------------------

/// Handle to a progress bar with optional display.
///
/// Cloning creates another reference to the same underlying tracking
/// state — all clones share state and advancing any one of them updates
/// the shared state that both clones reference.
///
/// To create a handle that never draws, use [`ProgressBarHandle::disabled`]. A
/// disabled handle performs no terminal writes and owns no render slot, so a
/// screen with no draw target can hand one out and a caller may drive it
/// harmlessly. It is not inert, though: position and status mutations still land
/// in its shared state, so what a disabled run reported stays inspectable. Only
/// `set_suffix_components`, `set_truncation`, and `set_style` consult the disabled
/// flag and discard their input; every other mutator, `set_prefix_components`
/// included, writes straight through.
///
/// [`ProgressBarHandle`] manages **tracking state only** (`Arc<SharedState>`);
/// the display bar is managed separately by [`ProgressRenderer`](super::ProgressRenderer), which
/// reads the same `Arc<SharedState>` and picks up changes asynchronously.
#[derive(Clone)]
pub struct ProgressBarHandle {
    pub(crate) state: Arc<SharedState>,
}

impl ProgressBarHandle {
    /// Create a handle that never draws.
    ///
    /// See [`ProgressBarHandle`] for what "disabled" suppresses: no terminal
    /// writes and no render slot, while position and status are still recorded
    /// for inspection.
    #[must_use]
    pub fn disabled() -> Self {
        let state = Arc::new(SharedState::new(0, ""));
        state.disabled.store(true, Ordering::Release);
        Self { state }
    }

    /// Create a standalone progress handle (not managed by a
    /// [`ProgressScreen`](super::super::ProgressScreen)) with no display backend.
    ///
    #[must_use]
    pub fn new(total: u64) -> Self {
        let state = Arc::new(SharedState::new(total, ""));
        Self { state }
    }

    /// Create a standalone progress handle with a label (no display
    /// backend).
    ///
    /// This is a convenience wrapper over [`new`](Self::new) that sets
    /// the initial label.
    #[must_use]
    pub fn with_label(total: u64, label: &str) -> Self {
        let state = Arc::new(SharedState::new(total, label));
        Self { state }
    }

    /// Return the total number of work units (0 = indeterminate).
    #[must_use]
    pub fn total(&self) -> u64 {
        self.state.total.load(Ordering::Relaxed)
    }

    /// Change the total mid-flight for dynamic workloads.
    pub fn set_total(&self, total: u64) {
        self.state.total.store(total, Ordering::Relaxed);
        self.state.dirty.store(true, Ordering::Release);
    }

    /// Advance the bar by `delta` work units.
    pub fn advance(&self, delta: u64) {
        self.state.position.fetch_add(delta, Ordering::Relaxed);
        self.state.dirty.store(true, Ordering::Release);
    }

    /// Jump to an absolute position.
    pub fn set_position(&self, pos: u64) {
        self.state.position.store(pos, Ordering::Relaxed);
        self.state.dirty.store(true, Ordering::Release);
    }

    /// Set prefix components directly (source-data API).
    ///
    /// The **single runtime prefix mutation API**. The initial value always
    /// comes from parsing the `add_bar`/`with_overall` label at construction
    /// (see `SharedState::with_time_source`); this method overrides those
    /// parsed components with fully structured source data. The legacy
    /// `set_prefix(String)` API has been removed — there is no string mutation
    /// path left.
    ///
    /// # Panics
    ///
    /// Panics if the shared-state `RwLock` is poisoned.
    pub fn set_prefix_components(&self, components: PrefixComponents) {
        {
            let mut pc =
                self.state.prefix_components.write().expect("shared_state prefix_components lock");
            *pc = components;
        }
        self.state.dirty.store(true, Ordering::Release);
    }

    /// Set suffix components directly (source-data API).
    ///
    /// The **single suffix mutation API** — the legacy `set_suffix(String)`
    /// API has been removed, and the suffix always flows through this
    /// structured path.
    ///
    /// Merge semantics (applied at `sync_snapshot_to_bar` time): user-set
    /// fields override the auto-derived fields composed from ticker data;
    /// empty user fields fall back to fresh ticker data, so callers may set
    /// just the fields they care about (typically `custom`) and leave the
    /// rest defaulted.
    ///
    /// # Panics
    ///
    /// Panics if the shared-state `RwLock` is poisoned.
    pub fn set_suffix_components(&self, components: SuffixComponents) {
        if self.state.disabled.load(Ordering::Relaxed) {
            return; // disabled handle
        }
        {
            let mut sc =
                self.state.suffix_components.write().expect("shared_state suffix_components lock");
            *sc = components;
        }
        self.state.dirty.store(true, Ordering::Release);
    }

    /// Install client-supplied truncation logic.
    ///
    /// Once set, the renderer's single push point calls
    /// [`truncate_prefix`](crate::progress::BarLabelTruncation::truncate_prefix)
    /// / [`truncate_suffix`](crate::progress::BarLabelTruncation::truncate_suffix)
    /// to obtain the final display strings directly, instead of the built-in
    /// component rendering. The client owns the field layout and order.
    ///
    /// # Panics
    ///
    /// Panics if the shared-state `RwLock` is poisoned.
    pub fn set_truncation(&self, truncation: Arc<dyn crate::progress::BarLabelTruncation>) {
        if self.state.disabled.load(Ordering::Relaxed) {
            return; // disabled handle
        }
        {
            let mut t = self.state.truncation.write().expect("shared_state truncation lock");
            *t = Some(truncation);
        }
        self.state.dirty.store(true, Ordering::Release);
    }

    /// Set the visual style for the bar (see [`BarStyle`]).
    ///
    /// Defaults to [`StepCount`](BarStyle::StepCount). Worker-slot bars set
    /// [`WorkerSpinner`](BarStyle::WorkerSpinner) so the renderer applies the
    /// style-specific `0/0` div-by-zero guard (renders `total = 1, pos = 0`
    /// when the worker's assigned count is `0`).
    ///
    /// # Panics
    ///
    /// Panics if the shared-state `RwLock` is poisoned.
    pub fn set_style(&self, style: BarStyle) {
        if self.state.disabled.load(Ordering::Relaxed) {
            return; // disabled handle
        }
        self.state.set_style(style);
    }

    /// Return the current visual style for the bar (see [`BarStyle`]).
    ///
    /// Defaults to [`StepCount`](BarStyle::StepCount).
    #[must_use]
    pub fn style(&self) -> BarStyle {
        self.state.style()
    }

    /// Mark the bar as finished successfully (keeps it visible).
    pub fn finish_success(&self) {
        self.state.status.store(1, Ordering::Relaxed); // Success
        self.state.mark_finished();
    }

    /// Mark the bar as finished with an error (keeps it visible).
    pub fn finish_error(&self) {
        self.state.status.store(2, Ordering::Relaxed); // Failed
        self.state.mark_finished();
    }

    /// Mark the bar as finished with a non-fatal warning (keeps it visible).
    pub fn finish_warning(&self) {
        self.state.status.store(3, Ordering::Relaxed); // Warning
        self.state.mark_finished();
    }

    /// Finish and clear the bar from the display.
    ///
    /// Stops the ticker and marks the bar as hidden. Call this instead of
    /// [`finish_success`](Self::finish_success) when the bar should
    /// disappear immediately.
    pub fn finish_and_clear(&self) {
        self.state.status.store(5, Ordering::Relaxed); // FinishedAndCleared
        self.state.mark_finished();
    }

    /// Return a data-copy snapshot of the current tracking state.
    #[must_use]
    pub fn snapshot(&self) -> TrackSnapshot {
        self.state.snapshot()
    }

    /// Returns `true` if the handle has been finished/abandoned.
    #[must_use]
    pub fn is_finished(&self) -> bool {
        self.state.is_finished()
    }

    /// Re-activate a finished bar. Clears the terminal state marker,
    /// resets elapsed tracking, and marks the bar dirty so the next
    /// tick redraws it as active.
    pub fn restart(&self) {
        self.state.restart();
    }
}
