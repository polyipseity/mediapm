//! `TrackedHandle`, `SharedState`, and `ProgressRenderer` (pure tracking + indicatif rendering).

use std::cell::{Cell, RefCell};
use std::collections::VecDeque;
use std::sync::atomic::{AtomicBool, AtomicU8, AtomicU64, Ordering};
use std::sync::{Arc, RwLock};
use std::time::{Duration, Instant};

use indicatif::{MultiProgress, ProgressBar, TermLike};

use super::{
    DebugSlotState, DebugTickSnapshot, DimensionSource, MAX_SLOTS, MIN_PREFIX_WIDTH,
    MIN_SUFFIX_WIDTH, PrefixComponents, ProgressDebugSink, RealTimeSource, SuffixComponents,
    TimeSource, WriteGate, apply_bar_style, apply_done_bar_style, apply_failed_bar_style,
    apply_overall_bar_style, bar_color_code, blank_bar_style, format_count, format_elapsed,
    format_eta, format_rate, max_prefix_width, max_suffix_width, prefix_components_from_str,
    render_prefix_components, render_suffix_components, semantic_truncate_prefix,
    semantic_truncate_suffix, visible_width,
};
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
    fn code(self) -> u8 {
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
    total: AtomicU64,
    label: RwLock<String>,
    prefix_components: RwLock<PrefixComponents>,
    suffix_components: RwLock<SuffixComponents>,
    /// Client-supplied truncation logic. When `Some`, the renderer calls
    /// it directly at the single push point to obtain the final
    /// prefix/suffix display strings. When `None`, the renderer falls
    /// back to its built-in component rendering. mediapm-utils owns no
    /// field layout — the client does.
    truncation: RwLock<Option<Arc<dyn crate::progress::BarLabelTruncation>>>,
    status: AtomicU8,
    dirty: AtomicBool,
    disabled: AtomicBool,
    /// Visual style for the bar (see [`BarStyle`]). Defaults to
    /// [`StepCount`](BarStyle::StepCount); set to
    /// [`WorkerSpinner`](BarStyle::WorkerSpinner) for fixed worker-slot
    /// bars. Read by the renderer's single push point to apply the
    /// style-specific `0/0` div-by-zero guard.
    style: AtomicU8,
    start_time: Instant,
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
    /// touched by [`set_prefix_components`] carry structured components
    /// (tool name, version, phase, count/total) for [`semantic_truncate_prefix`]
    /// to truncate field-by-field. The legacy `set_prefix(String)` API has
    /// been removed — this construction-time parse plus [`set_prefix_components`]
    /// is the only prefix mechanism.
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
        Self {
            position: AtomicU64::new(0),
            total: AtomicU64::new(total),
            label: RwLock::new(label.to_string()),
            prefix_components: RwLock::new(prefix_components_from_str(label)),
            suffix_components: RwLock::new(SuffixComponents::default()),
            truncation: RwLock::new(None),
            status: AtomicU8::new(0),
            dirty: AtomicBool::new(true),
            disabled: AtomicBool::new(false),
            style: AtomicU8::new(style as u8),
            start_time: time_source.now(),
            finished_elapsed: RwLock::new(None),
            time_source,
        }
    }

    fn snapshot(&self) -> TrackSnapshot {
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
            self.time_source.now() - self.start_time
        }
    }

    pub(crate) fn mark_finished(&self) {
        self.dirty.store(true, Ordering::Release);
        let elapsed = self.time_source.now() - self.start_time;
        *self.finished_elapsed.write().expect("shared_state finished_elapsed lock") = Some(elapsed);
    }

    fn is_finished(&self) -> bool {
        self.status.load(Ordering::Relaxed) != 0
    }

    fn is_cleared(&self) -> bool {
        self.status.load(Ordering::Relaxed) == 5
    }

    fn style(&self) -> BarStyle {
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

// ---- TrackedHandle ----------------------------------------------------

/// Handle to a progress bar with optional display.
///
/// Cloning creates another reference to the same underlying tracking
/// state — all clones share state and advancing any one of them updates
/// the shared state that both clones reference.
///
/// To create a no-op handle, use [`TrackedHandle::disabled`]. All mutating
/// methods on a disabled handle are zero-cost and do nothing.
///
/// [`TrackedHandle`] manages **tracking state only** (`Arc<SharedState>`);
/// the display bar is managed separately by [`ProgressRenderer`], which
/// reads the same `Arc<SharedState>` and picks up changes asynchronously.
#[derive(Clone)]
pub struct TrackedHandle {
    pub(crate) state: Arc<SharedState>,
}

impl TrackedHandle {
    /// Create a no-op handle (all methods are zero-cost).
    #[must_use]
    pub fn disabled() -> Self {
        let state = Arc::new(SharedState::new(0, ""));
        state.disabled.store(true, Ordering::Release);
        Self { state }
    }

    /// Create a standalone progress handle (not managed by a
    /// [`ProgressGroup`]) with no display backend.
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
    /// (see [`SharedState::with_time_source`]); this method overrides those
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
    /// Merge semantics (applied at [`sync_snapshot_to_bar`] time): user-set
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
    /// [`BarLabelTruncation::truncate_prefix`] /
    /// [`BarLabelTruncation::truncate_suffix`] to obtain the final display
    /// strings directly, instead of the built-in component rendering. The
    /// client owns the field layout and order.
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
}

// ---- (ProgressTracker removed: use TrackedHandle::with_label) -----

// ---- ProgressRenderer + ProgressGroup (rendering + combined) ----------

/// A single slot in the renderer's fixed-size grid.
struct RenderedSlot {
    /// The indicatif [`ProgressBar`] that draws to the terminal.
    bar: ProgressBar,
    /// Optional tracking state this slot is currently bound to.
    /// `None` means the slot is blank (unused).
    source: RefCell<Option<Arc<SharedState>>>,
    /// Cached last values pushed to the bar, used to skip redundant
    /// indicatif calls and reduce terminal flicker.
    cache: SlotCache,
}

/// Manages a fixed-size grid of [`ProgressBar`] slots in [`MultiProgress`]
/// with shift-based allocation and automatic recycling of finished slots.
///
/// All slots are pre-allocated at construction so the draw height never
/// changes — eliminating the root cause of terminal ghosting.
///
/// # Allocation strategy
///
/// 1. [`attach`](Self::attach) places new children into the **bottom** of the
///    active band (just above the overall bar if one exists) and shifts all
///    existing active children up by one slot, preserving chronological order
///    top-to-bottom.
/// 2. When all slots are occupied by active handles, finished slots are
///    recycled (scanning from the bottom upward).
/// 3. When no finished slot can be recycled, the new handle is pushed into
///    [`orphaned_states`](Self::orphaned_states) — tracked but with no render
///    slot until the terminal grows.
/// 4. Finished bars stay visible — their slots are only recycled when new
///    handles need display space.
pub struct ProgressRenderer {
    inner: MultiProgress,
    slots: Vec<RenderedSlot>,
    has_overall: bool,
    dim_source: Arc<dyn DimensionSource>,
    last_width: Option<u16>,
    /// When `true`, the slot count may be adjusted on terminal height
    /// changes.  `false` when the caller specified an explicit capacity
    /// (e.g. via [`from_mp`](Self::from_mp)).
    pub(crate) dynamic_height: bool,
    /// Queue of [`SharedState`] handles evicted from render slots during
    /// height shrink.  Reattached (FIFO) when the terminal grows back.
    orphaned_states: RefCell<VecDeque<Arc<SharedState>>>,
    /// Guard against double-`finalize` from both
    /// [`join_and_clear`](Self::join_and_clear) and
    /// [`Drop`](Drop).
    finalized: Cell<bool>,
    /// Injectable time source (real or synthetic for testing).
    pub(crate) time_source: Arc<dyn TimeSource>,

    /// EMA-smoothed rate tracking, one entry per slot.
    slots_timing: Vec<SlotTiming>,
    /// Write gate controlling terminal-write suppression during frames.
    /// Owned exclusively by the renderer — the only way to open/close
    /// the write window is through `gate.open()` (private to `gate.rs`).
    gate: WriteGate,

    /// One-shot flag: has the first-draw pre-roll (newline scroll) been
    /// performed?  Used to push intervening stderr content into scrollback
    /// before indicatif's first draw.
    pre_rolled: AtomicBool,

    /// Terminal to write pre-roll newlines to.  `None` in test mode
    /// (user-provided `MultiProgress` via `with_multi_progress`).
    pre_roll_term: Option<Box<dyn TermLike>>,

    /// Optional JSONL debug sink — emits bar-state snapshots on every tick.
    debug_sink: Option<ProgressDebugSink>,

    /// Current uniform prefix width applied to every visible bar this frame.
    /// Recomputed each tick from the max measured prefix across bound slots,
    /// clamped between [`MIN_PREFIX_WIDTH`] and [`max_prefix_width`].
    prefix_w: Cell<usize>,
    /// Current uniform suffix width applied to every visible bar this frame.
    /// See [`Self::prefix_w`] — same contract with [`MIN_SUFFIX_WIDTH`] /
    /// [`max_suffix_width`].
    suffix_w: Cell<usize>,
}

/// EMA-smoothed rate tracking for a render slot.
struct SlotTiming {
    prev_position: u64,
    prev_instant: Instant,
    rate: f64,
}

impl SlotTiming {
    fn new(time_source: &dyn TimeSource) -> Self {
        Self { prev_position: 0, prev_instant: time_source.now(), rate: 0.0 }
    }
}

/// Cached last values pushed to a bar, used to skip redundant indicatif
/// setter calls and reduce terminal flicker.
struct SlotCache {
    /// Last position sent to `set_position`.
    position: Cell<u64>,
    /// Last total sent to `set_length`.
    total: Cell<u64>,
    /// Last display suffix sent to the bar.
    suffix: RefCell<String>,
    /// Last prefix sent to `set_prefix`.
    prefix: RefCell<String>,
    /// Cached prefix_w at last set_style call (style dedup).
    style_prefix_w: Cell<usize>,
    /// Cached suffix_w at last set_style call (style dedup).
    style_suffix_w: Cell<usize>,
    /// Cached is_overall flag at last set_style call (style dedup).
    style_is_overall: Cell<bool>,
    /// Last status code at last set_style call (style dedup).
    style_status_code: Cell<u8>,
}

impl SlotCache {
    fn new() -> Self {
        Self {
            position: Cell::new(u64::MAX),
            total: Cell::new(u64::MAX),
            suffix: RefCell::new(String::new()),
            prefix: RefCell::new(String::new()),
            style_prefix_w: Cell::new(usize::MAX),
            style_suffix_w: Cell::new(usize::MAX),
            style_is_overall: Cell::new(false),
            style_status_code: Cell::new(u8::MAX),
        }
    }
}

impl RenderedSlot {}

impl ProgressRenderer {
    /// Pre-allocate `capacity` blank bars in an existing [`MultiProgress`].
    pub(crate) fn from_mp(
        mp: MultiProgress,
        capacity: usize,
        dim_source: Arc<dyn DimensionSource>,
        gate: WriteGate,
        time_source: Arc<dyn TimeSource>,
        pre_roll_term: Option<Box<dyn TermLike>>,
        debug_sink: Option<ProgressDebugSink>,
    ) -> Self {
        let mut slots = Vec::with_capacity(capacity);
        for _ in 0..capacity {
            let pb = ProgressBar::new(0);
            // IMPORTANT: add to MultiProgress FIRST, then configure.
            // Configuring before mp.add() prevents InMemoryTerm from
            // capturing blank bar output in tests.
            let bar = mp.add(pb);
            bar.set_style(blank_bar_style());
            bar.set_message(" ");
            bar.set_prefix("");
            slots.push(RenderedSlot { bar, source: RefCell::new(None), cache: SlotCache::new() });
        }
        // Trigger a final draw so all bars are captured by InMemoryTerm
        // even when capacity == terminal height.
        if let Some(slot) = slots.last() {
            slot.bar.tick();
        }
        let slots_timing = (0..capacity).map(|_| SlotTiming::new(&*time_source)).collect();
        Self {
            inner: mp,
            slots,
            has_overall: false,
            dim_source,
            last_width: None,
            dynamic_height: false,
            orphaned_states: RefCell::new(VecDeque::new()),
            finalized: Cell::new(false),
            time_source,
            slots_timing,
            gate,
            pre_rolled: AtomicBool::new(false),
            pre_roll_term,
            debug_sink,
            prefix_w: Cell::new(MIN_PREFIX_WIDTH),
            suffix_w: Cell::new(MIN_SUFFIX_WIDTH),
        }
    }

    /// Pre-allocate `capacity` bars with an overall bar at the bottom,
    /// using an existing [`MultiProgress`].  Returns `(renderer, overall_state)`.
    #[allow(clippy::too_many_arguments)]
    pub(crate) fn from_mp_with_overall(
        mp: MultiProgress,
        capacity: usize,
        total: u64,
        label: &str,
        dim_source: Arc<dyn DimensionSource>,
        gate: WriteGate,
        time_source: Arc<dyn TimeSource>,
        pre_roll_term: Option<Box<dyn TermLike>>,
        debug_sink: Option<ProgressDebugSink>,
    ) -> (Self, Arc<SharedState>) {
        let mut slots = Vec::with_capacity(capacity);
        for _ in 0..capacity.saturating_sub(1) {
            let pb = ProgressBar::new(0);
            let bar = mp.add(pb);
            bar.set_style(blank_bar_style());
            bar.set_message(" ");
            bar.set_prefix("");
            slots.push(RenderedSlot { bar, source: RefCell::new(None), cache: SlotCache::new() });
        }
        // Last slot = overall bar.
        let overall_state =
            Arc::new(SharedState::with_time_source(total, label, Arc::clone(&time_source)));
        let inner = ProgressBar::new(total);
        let overall_bar = mp.add(inner);
        apply_overall_bar_style(&overall_bar, MIN_PREFIX_WIDTH, MIN_SUFFIX_WIDTH);
        overall_bar.set_prefix(label.to_string());
        slots.push(RenderedSlot {
            bar: overall_bar,
            source: RefCell::new(Some(overall_state.clone())),
            cache: SlotCache::new(),
        });
        let slots_timing = (0..capacity).map(|_| SlotTiming::new(&*time_source)).collect();
        (
            Self {
                inner: mp,
                slots,
                has_overall: true,
                dim_source,
                last_width: None,
                dynamic_height: false,
                orphaned_states: RefCell::new(VecDeque::new()),
                finalized: Cell::new(false),
                time_source,
                slots_timing,
                gate,
                pre_rolled: AtomicBool::new(false),
                pre_roll_term,
                debug_sink,
                prefix_w: Cell::new(MIN_PREFIX_WIDTH),
                suffix_w: Cell::new(MIN_SUFFIX_WIDTH),
            },
            overall_state,
        )
    }

    /// Re-configure the bar at slot index `i` to reflect its current
    /// tracked source (or blank state if unbound).
    fn sync_slot(&self, i: usize) {
        let slot = &self.slots[i];
        if let Some(ref source) = *slot.source.borrow() {
            let snap = source.snapshot();
            let is_overall = self.has_overall && i == self.slots.len() - 1;
            let prefix_w = self.prefix_w.get();
            let suffix_w = self.suffix_w.get();
            let status_code = snap.status.code();

            // Style dedup: only call set_style when dimensions or status changed.
            let style_changed = prefix_w != slot.cache.style_prefix_w.get()
                || suffix_w != slot.cache.style_suffix_w.get()
                || is_overall != slot.cache.style_is_overall.get()
                || status_code != slot.cache.style_status_code.get();
            if style_changed {
                if is_overall {
                    apply_overall_bar_style(&slot.bar, prefix_w, suffix_w);
                } else if snap.status == TrackStatus::Failed {
                    apply_failed_bar_style(&slot.bar, prefix_w, suffix_w);
                } else if snap.status != TrackStatus::Active {
                    apply_done_bar_style(&slot.bar, prefix_w, suffix_w);
                } else {
                    // Slot recycling may leave the indicatif bar with
                    // Status::DoneVisible from the previous phase.  Reset
                    // it to InProgress so the spinner cycles again.
                    if slot.bar.is_finished() {
                        slot.bar.reset();
                    }
                    apply_bar_style(&slot.bar, prefix_w, suffix_w);
                }
                slot.cache.style_prefix_w.set(prefix_w);
                slot.cache.style_suffix_w.set(suffix_w);
                slot.cache.style_is_overall.set(is_overall);
                slot.cache.style_status_code.set(status_code);
            }
            let rate_str: Option<String> = if snap.status == TrackStatus::Active {
                if self.slots_timing[i].rate > 0.0 {
                    Some(format_rate(self.slots_timing[i].rate))
                } else {
                    Some("0/d".into())
                }
            } else {
                None
            };
            self.sync_snapshot_to_bar(i, &snap, rate_str.as_deref(), None);
        } else {
            slot.bar.set_style(blank_bar_style());
            slot.bar.set_message(" ");
            slot.bar.set_prefix("");
        }
    }

    /// Attach a tracked state to the next available render slot.
    ///
    /// Places the new child at the **bottom** of the active band (just
    /// above the overall bar when one exists).  Existing active children
    /// are shifted up by one slot, preserving chronological order:
    /// first-created child at the top of the band, last-created adjacent
    /// to the overall bar.
    ///
    /// When all slots are occupied by active handles, recycles the
    /// oldest finished slot from the top of the band and shifts all
    /// remaining bars up, keeping the newest bars contiguous at the
    /// bottom.  When no finished slot is available, the handle is
    /// pushed to [`orphaned_states`] — it remains tracked but has no
    /// render slot until the terminal grows back.
    pub(crate) fn attach(&mut self, state: &Arc<SharedState>) {
        // Buffer all draws during attach — slot shifts + sync_slot + recompute_layout
        // produce many intermediate state changes that should appear atomically.
        let _attach_guard = self.gate.open();
        let child_cap = self.slots.len() - usize::from(self.has_overall);
        let bottom = child_cap.saturating_sub(1);

        // Phase 1: shift active band up, place new child at bottom
        let active = self.slots[..=bottom].iter().filter(|s| s.source.borrow().is_some()).count();

        if active < child_cap {
            // Shift existing active children up by one slot (ascending
            // order preserves relative positions).
            for i in (bottom + 1 - active)..=bottom {
                let (left, right) = self.slots.split_at_mut(i);
                std::mem::swap(&mut left[left.len() - 1].source, &mut right[0].source);
                self.slots_timing.swap(i, i - 1);
            }
            // Sync shifted slots (sources moved to different bars).
            for i in (bottom.saturating_sub(active))..=bottom {
                self.sync_slot(i);
            }
            // Place new child at the freed bottom slot.
            self.slots[bottom].source.replace(Some(Arc::clone(state)));
            self.slots_timing[bottom] = SlotTiming::new(&*self.time_source);
            self.slots[bottom].cache = SlotCache::new();
            self.sync_slot(bottom);
            self.recompute_layout();
            return;
        }

        // Phase 2: compact — recycle the oldest finished slot and shift
        // all bars below it up by one slot, placing the new bar at the
        // bottom.  This keeps the most recent bars visible and contiguous.
        for old_i in 0..=bottom {
            if self.slots[old_i].source.borrow().as_ref().is_some_and(|s| s.is_finished()) {
                // Bubble the source at old_i rightward through bottom,
                // shifting all sources up by one slot.
                for j in old_i..bottom {
                    let (left, right) = self.slots.split_at_mut(j + 1);
                    std::mem::swap(&mut left[left.len() - 1].source, &mut right[0].source);
                    self.slots_timing.swap(j + 1, j);
                }
                // Sync shifted slots (sources moved to different bars).
                for j in old_i..bottom {
                    self.sync_slot(j);
                }
                // Place new child at the freed bottom slot.
                self.slots[bottom].source.replace(Some(Arc::clone(state)));
                self.slots_timing[bottom] = SlotTiming::new(&*self.time_source);
                self.slots[bottom].cache = SlotCache::new();
                self.sync_slot(bottom);
                self.recompute_layout();
                return;
            }
        }
        // Phase 3: no free slot — push to orphaned queue.
        self.orphaned_states.borrow_mut().push_back(Arc::clone(state));
    }

    /// Returns `true` when at least one tracked slot still has an active
    /// (non-terminal) source.  When this returns `false`, the daemon ticker
    /// can sleep longer since no spinner animation or progress updates are
    /// needed.
    pub(crate) fn has_active_slots(&self) -> bool {
        self.slots
            .iter()
            .any(|slot| slot.source.borrow().as_ref().is_some_and(|s| !s.is_finished()))
    }

    /// Defensive sync: refresh all render slots from their tracked sources.
    ///
    /// Includes resize reactivity and full style re-application.
    ///
    /// When the write gate is active (production),
    /// property-setter terminal writes are suppressed during the update
    /// loop, then exactly one draw is released at the end.  This ensures
    /// the 50 ms daemon ticker is the sole draw authority and eliminates
    /// flicker from burst writes.
    /// Recompute the uniform `prefix_w`/`suffix_w` applied to every visible
    /// bar this frame.
    ///
    /// Measures the rendered prefix/suffix width of each bound slot (via
    /// [`SharedState::snapshot`]), takes the max across all bound slots, and
    /// clamps it into `[MIN_PREFIX_WIDTH, max_prefix_width()]` (prefix)
    /// and `[MIN_SUFFIX_WIDTH, max_suffix_width()]` (suffix). The result
    /// is stored in the `prefix_w`/`suffix_w` cells and every bound slot is
    /// re-synced so all bars share the same alignment width — short labels
    /// no longer waste space and long labels no longer overflow the bar.
    fn recompute_layout(&self) {
        let mut max_prefix = 0usize;
        let mut max_suffix = 0usize;
        for (i, slot) in self.slots.iter().enumerate() {
            if let Some(ref source) = *slot.source.borrow() {
                let snap = source.snapshot();
                // `snap.prefix` is rendered WITH its leading `\x1b[0m`
                // escape (see `snapshot`), and indicatif counts those
                // escape bytes as visible characters in the
                // `{prefix:N.N}` template field. So the template field
                // width must be the *visible* label width PLUS the per-status
                // ANSI overhead (4 for normal, 13 for failed/warning). The
                // suffix side measures `render_suffix_components(&full_suffix,
                // "")` (no color) which is already ANSI-free, so it stays
                // `visible_width`. `sync_snapshot_to_bar` subtracts the same
                // per-status `ansi_overhead` to recover the visible budget
                // for `semantic_truncate_prefix`, keeping both sides in the
                // same coordinate system.
                // Client-truncated bars only carry the 4-byte `\x1b[0m`
                // reset, never the per-status marker bytes (13).  The
                // built-in path adds those extra bytes, but when a client
                // truncation is installed we use the client path instead.
                // The `snap.prefix` is always the built-in rendered prefix;
                // to size the layout correctly we check for client
                // truncation and use the matching overhead.
                let has_client_truncation = slot.source.borrow().as_ref().is_some_and(|s| {
                    s.truncation.read().expect("shared_state truncation lock").is_some()
                });
                let status_overhead: usize = if has_client_truncation {
                    4
                } else {
                    match snap.status {
                        TrackStatus::Failed | TrackStatus::Warning => 13,
                        _ => 4,
                    }
                };
                max_prefix = max_prefix.max(visible_width(snap.prefix.as_str()) + status_overhead);
                // Measure the full rendered suffix (auto fields + custom),
                // not just the stored custom text — the rendered RHS also
                // carries count/total/elapsed/rate/eta which consume the
                // width budget. Replicate the rate/eta computation from the
                // tick loop so the estimate matches what will actually draw.
                let count_str = format_count(snap.position);
                let total_str = format_count(snap.total);
                let elapsed_str = format_elapsed(snap.elapsed);
                let rate_str: Option<String> = if snap.status == TrackStatus::Active {
                    // Prospective rate: replicate the tick-loop EMA update
                    // read-only so the measured width matches what will draw.
                    let mut rate = self.slots_timing[i].rate;
                    if snap.position != self.slots_timing[i].prev_position {
                        let now = self.time_source.now();
                        let dt =
                            now.duration_since(self.slots_timing[i].prev_instant).as_secs_f64();
                        if dt > 0.001 {
                            #[allow(clippy::cast_precision_loss)]
                            let current =
                                (snap.position - self.slots_timing[i].prev_position) as f64 / dt;
                            rate = rate * 0.9 + current * 0.1;
                        }
                    }
                    Some(format_rate(rate))
                } else {
                    None
                };
                // Reserve eta width for any in-progress bar.  At measure
                // time `slots_timing[i].rate` may still be 0 (the bar was
                // just attached and has not ticked yet), but the draw path
                // computes eta whenever rate > 0 — which it will be once
                // the bar progresses.  Under-reserving here would let the
                // later wider draw overflow `suffix_w` and truncate the
                // custom suffix.  Use a nominal rate floor so the budget
                // covers the eta segment that will appear on the next tick.
                let eta_str = if snap.status == TrackStatus::Active && snap.total > snap.position {
                    let rate = if self.slots_timing[i].rate > 0.0 {
                        self.slots_timing[i].rate
                    } else {
                        1.0
                    };
                    #[allow(clippy::cast_precision_loss)]
                    let remaining = (snap.total - snap.position) as f64 / rate;
                    Some(format_eta(remaining))
                } else {
                    None
                };
                // Measure the MERGED suffix (auto fields + user-set
                // overrides), not just the auto-derived fields.  A wider
                // user-set `rate`/`eta`/`custom` must widen `suffix_w` or
                // it would overflow at draw and get truncated away.
                let auto_suffix = SuffixComponents {
                    count: count_str,
                    total: total_str,
                    elapsed: elapsed_str,
                    rate: rate_str,
                    eta: eta_str,
                    custom: snap.suffix.clone(),
                };
                let full_suffix = SuffixComponents::merge(&auto_suffix, &snap.suffix_components);
                let suffix_width = if has_client_truncation {
                    // Client-truncated bars: call the client's suffix
                    // truncation with a generous budget to measure the
                    // actual width of the final output string.
                    if let Some(ref source) = *slot.source.borrow() {
                        if let Some(ref t) =
                            *source.truncation.read().expect("shared_state truncation lock")
                        {
                            let rendered = t.truncate_suffix(max_suffix_width(), &full_suffix);
                            visible_width(rendered.as_str())
                        } else {
                            0
                        }
                    } else {
                        0
                    }
                } else {
                    let rendered = render_suffix_components(&full_suffix, "");
                    visible_width(rendered.as_str())
                };
                max_suffix = max_suffix.max(suffix_width);
            }
        }
        let prefix_w = max_prefix.clamp(MIN_PREFIX_WIDTH, max_prefix_width());
        let suffix_w = max_suffix.clamp(MIN_SUFFIX_WIDTH, max_suffix_width());
        self.prefix_w.set(prefix_w);
        self.suffix_w.set(suffix_w);
        for (i, slot) in self.slots.iter().enumerate() {
            if slot.source.borrow().is_some() {
                self.sync_slot(i);
            }
        }
    }

    #[expect(
        clippy::too_many_lines,
        reason = "tick orchestrates many progress-bar state updates that are clearer inline"
    )]
    /// Advance all progress bars by one frame.
    ///
    /// Recomputes the uniform alignment width, then redraws every visible
    /// bar from its tracked source state.
    pub fn tick(&mut self) {
        // Step 0: Enable buffering BEFORE recomputing layout so all
        // style changes from recompute_layout → sync_slot → apply_*_bar_style
        // are suppressed until the WriteGate draw release.
        self.gate.suppress();

        // Step 1: Recompute uniform alignment width (styles now buffered).
        self.recompute_layout();

        // Step 2: Existing update logic with dirty tracking.
        let resized = self.maybe_adjust_for_resize();

        // When resize happened, mark all bound slots as dirty so they
        // get re-synced even if no other mutation occurred.
        if resized {
            for slot in &self.slots {
                if let Some(ref source) = *slot.source.borrow() {
                    source.dirty.store(true, Ordering::Release);
                }
            }
        }

        for (i, slot) in self.slots.iter().enumerate() {
            if let Some(ref source) = *slot.source.borrow() {
                // Skip clean slots — nothing changed since last tick.
                let dirty = resized || source.dirty.swap(false, Ordering::AcqRel);
                if !dirty {
                    continue;
                }
                let snap = source.snapshot();

                // Compute EMA-smoothed rate for display in active bars only.
                // Rate is only recomputed when position actually changes.
                let rate_str: Option<String> = if snap.status == TrackStatus::Active {
                    if snap.position != self.slots_timing[i].prev_position {
                        let now = self.time_source.now();
                        let dt =
                            now.duration_since(self.slots_timing[i].prev_instant).as_secs_f64();
                        if dt > 0.001 {
                            #[allow(clippy::cast_precision_loss)]
                            let current =
                                (snap.position.saturating_sub(self.slots_timing[i].prev_position))
                                    as f64
                                    / dt;
                            self.slots_timing[i].rate =
                                self.slots_timing[i].rate * 0.9 + current * 0.1;
                            self.slots_timing[i].prev_position = snap.position;
                            self.slots_timing[i].prev_instant = now;
                        }
                    }
                    Some(format_rate(self.slots_timing[i].rate))
                } else {
                    None
                };

                // Compute ETA for active bars with known total and
                // non-zero rate.
                let eta_str = if snap.status == TrackStatus::Active
                    && snap.total > snap.position
                    && self.slots_timing[i].rate > 0.0
                {
                    #[allow(clippy::cast_precision_loss)]
                    let remaining = (snap.total - snap.position) as f64 / self.slots_timing[i].rate;
                    Some(format_eta(remaining))
                } else {
                    None
                };

                self.sync_snapshot_to_bar(i, &snap, rate_str.as_deref(), eta_str.as_deref());
                if snap.status == TrackStatus::Active {
                    // bar.tick() called after buffer disable below.
                } else if source.is_cleared() {
                    slot.bar.set_style(blank_bar_style());
                    slot.bar.set_message(" ");
                    slot.bar.set_prefix("");
                } else {
                    self.finish_slot(i, snap.status);
                }
            }
        }

        // Emit debug snapshot (if enabled) — all bar states are fresh from sync.
        if let Some(ref sink) = self.debug_sink {
            let bars: Vec<DebugSlotState> = self
                .slots
                .iter()
                .enumerate()
                .map(|(i, slot)| {
                    let (bound, snap) = match slot.source.borrow().as_ref() {
                        Some(s) => (true, s.snapshot()),
                        None => (
                            false,
                            TrackSnapshot {
                                position: 0,
                                total: 0,
                                label: String::new(),
                                prefix: String::new(),
                                prefix_components: PrefixComponents::default(),
                                suffix: String::new(),
                                suffix_components: SuffixComponents::default(),
                                status: TrackStatus::Active,
                                elapsed: Duration::ZERO,
                            },
                        ),
                    };
                    let rate = if bound && snap.status == TrackStatus::Active {
                        self.slots_timing[i].rate
                    } else {
                        0.0
                    };
                    #[expect(
                        clippy::cast_precision_loss,
                        reason = "progress ETA math tolerates u64 to f64 precision loss"
                    )]
                    let eta = if bound
                        && snap.status == TrackStatus::Active
                        && snap.total > snap.position
                        && self.slots_timing[i].rate > 0.0
                    {
                        Some((snap.total - snap.position) as f64 / self.slots_timing[i].rate)
                    } else {
                        None
                    };
                    DebugSlotState {
                        slot: i,
                        bound,
                        label: snap.label.clone(),
                        prefix: snap.prefix.clone(),
                        position: snap.position,
                        total: snap.total,
                        status: format!("{:?}", snap.status),
                        elapsed_secs: snap.elapsed.as_secs_f64(),
                        rate_bytes_per_sec: rate,
                        eta_secs: eta,
                        suffix: snap.suffix.clone(),
                        dirty: slot
                            .source
                            .borrow()
                            .as_ref()
                            .is_some_and(|s| s.dirty.load(Ordering::Acquire)),
                    }
                })
                .collect();
            let snapshot = DebugTickSnapshot {
                r#type: "tick".to_string(),
                tick: sink.tick_count.load(Ordering::Relaxed),
                elapsed_secs: self.time_source.now().duration_since(sink.start).as_secs_f64(),
                bars,
            };
            sink.emit(&snapshot);
        }

        // Step 3: Pre-roll newlines before first draw (bypasses buffer).
        self.pre_roll_if_needed();

        // Steps 4-6: RAII guard — draws go through while guard is alive,
        // buffer re-enabled automatically when guard drops.
        let _guard = self.gate.open();

        // Always tick active bars
        // Skip finished/abandoned/failed bars — their spinner is frozen on
        // the final frame set by `finish_slot`.
        for slot in &self.slots {
            if let Some(ref source) = *slot.source.borrow()
                && !source.is_finished()
            {
                slot.bar.tick();
            }
        }
    }

    /// Execute a frame: suppress writes, run `mutate`, advance spinners,
    /// draw once, then unsuppress. Guarantees exactly one terminal write.
    ///
    /// `mutate` is called while the write gate is suppressed, so any
    /// state changes it makes are accumulated without intermediate draws.
    /// After `mutate` returns, spinners are advanced and a single draw
    /// is triggered by opening the gate (indicatif draws on the next bar
    /// operation while the gate is open).
    #[expect(dead_code, reason = "C5 will wire run_frame into tick() and ProgressGroup::tick()")]
    pub(crate) fn run_frame(&mut self, mutate: impl FnOnce(&mut Self)) {
        // Step 1: Suppress writes.
        self.gate.suppress();

        // Step 2: Recompute layout (styles buffered).
        self.recompute_layout();

        // Step 3: Resize handling.
        let resized = self.maybe_adjust_for_resize();
        if resized {
            for slot in &self.slots {
                if let Some(ref source) = *slot.source.borrow() {
                    source.dirty.store(true, Ordering::Release);
                }
            }
        }

        // Step 4: Run caller's mutation (state changes, still suppressed).
        mutate(self);

        // Step 5: Sync all dirty slots to bars (still suppressed).
        for (i, slot) in self.slots.iter().enumerate() {
            if let Some(ref source) = *slot.source.borrow() {
                let dirty = resized || source.dirty.swap(false, Ordering::AcqRel);
                if !dirty {
                    continue;
                }
                let snap = source.snapshot();

                let rate_str: Option<String> = if snap.status == TrackStatus::Active {
                    if snap.position != self.slots_timing[i].prev_position {
                        let now = self.time_source.now();
                        let dt =
                            now.duration_since(self.slots_timing[i].prev_instant).as_secs_f64();
                        if dt > 0.001 {
                            #[allow(clippy::cast_precision_loss)]
                            let current =
                                (snap.position.saturating_sub(self.slots_timing[i].prev_position))
                                    as f64
                                    / dt;
                            self.slots_timing[i].rate =
                                self.slots_timing[i].rate * 0.9 + current * 0.1;
                            self.slots_timing[i].prev_position = snap.position;
                            self.slots_timing[i].prev_instant = now;
                        }
                    }
                    Some(format_rate(self.slots_timing[i].rate))
                } else {
                    None
                };

                let eta_str = if snap.status == TrackStatus::Active
                    && snap.total > snap.position
                    && self.slots_timing[i].rate > 0.0
                {
                    #[allow(clippy::cast_precision_loss)]
                    let remaining = (snap.total - snap.position) as f64 / self.slots_timing[i].rate;
                    Some(format_eta(remaining))
                } else {
                    None
                };

                self.sync_snapshot_to_bar(i, &snap, rate_str.as_deref(), eta_str.as_deref());
                if snap.status != TrackStatus::Active && !source.is_finished() {
                    self.finish_slot(i, snap.status);
                }
            }
        }

        // Step 6: Pre-roll (bypasses buffer).
        self.pre_roll_if_needed();

        // Step 7: Advance spinners and draw once.
        // Open the gate — indicatif draws on the next bar operation.
        let _guard = self.gate.open();
        for slot in &self.slots {
            if let Some(ref source) = *slot.source.borrow()
                && !source.is_finished()
            {
                slot.bar.tick();
            }
        }
        // _guard drops → gate re-suppressed.
    }

    /// Apply a snapshot's position/length/suffix/prefix to the indicatif bar
    /// at slot `i`. **This is the single authoritative push point for
    /// `SharedState` → indicatif.** All code paths that reflect `SharedState`
    /// (position, total, suffix, prefix) on the terminal bar must call
    /// through here — both the daemon ticker and [`finalize`](Self::finalize)
    /// do.
    ///
    /// Does **not** change the bar's style — callers manage style
    /// independently via [`finish_slot`](Self::finish_slot) or explicit
    /// `set_style` calls during attach/resize.
    fn sync_snapshot_to_bar(
        &self,
        i: usize,
        snap: &TrackSnapshot,
        rate_str: Option<&str>,
        eta_str: Option<&str>,
    ) {
        let slot = &self.slots[i];
        let is_overall = self.has_overall && i == self.slots.len() - 1;
        // Style-specific div-by-zero guard: a `WorkerSpinner` slot whose
        // assigned count is `0` (idle worker) would otherwise render
        // `total = 0`, which indicatif treats as indeterminate and hides
        // the bar. Pin it to `total = 1, pos = 0` so the idle worker shows
        // a fully-dimmed empty bar (all `░`). `StepCount` bars keep their
        // real total/position.
        let style = slot.source.borrow().as_ref().map_or(BarStyle::StepCount, |s| s.style());
        let (render_total, render_pos) = if style == BarStyle::WorkerSpinner && snap.total == 0 {
            (1, 0)
        } else {
            (snap.total, snap.position)
        };
        let count_str = format_count(render_pos);
        let total_str = format_count(render_total);
        let elapsed_str = format_elapsed(snap.elapsed);
        let color_code = bar_color_code(snap.status, is_overall);
        // Compose a fresh suffix component set each tick: auto fields from
        // snapshot + ticker timing, user-set fields from stored components.
        // Stored non-empty fields override the auto-derived ones; stored
        // rate/eta override when Some; empty fields auto-fill.  Use the
        // shared merge helper so the draw path matches the layout estimate.
        let auto_suffix = SuffixComponents {
            count: count_str,
            total: total_str,
            elapsed: elapsed_str,
            rate: rate_str.map(str::to_owned),
            eta: eta_str.map(str::to_owned),
            custom: String::new(),
        };
        let fresh_suffix = SuffixComponents::merge(&auto_suffix, &snap.suffix_components);

        // Truncate prefix to fit template width, accounting for ANSI
        // escapes added by render_prefix_components (which indicatif counts
        // as visible chars). Normal: \x1b[0m = 4; failed/warning:
        // \x1b[0m\x1b[3Xm\x1b[0m = 13. The marker brackets are visible data
        // and consume the width budget via semantic_truncate_prefix.
        let ansi_overhead: usize = match snap.status {
            TrackStatus::Failed | TrackStatus::Warning => 13,
            _ => 4,
        };
        // Client-defined truncation takes precedence when installed. The
        // renderer only *calls* the trait; it owns no field layout. The
        // `None` branch keeps the built-in component rendering as the
        // fallback so existing callers and tests stay green.
        let truncation = slot
            .source
            .borrow()
            .as_ref()
            .and_then(|s| s.truncation.read().expect("shared_state truncation lock").clone());
        let new_prefix = if let Some(t) = truncation.as_ref() {
            // Client-truncated bars only add the 4-byte `\x1b[0m` reset;
            // the 13-byte overhead for failed/warning status markers is
            // only used by the built-in component rendering.
            let client_overhead: usize = 4;
            let raw = t.truncate_prefix(self.prefix_w.get().saturating_sub(client_overhead));
            format!("\x1b[0m{raw}")
        } else {
            let truncated_prefix = semantic_truncate_prefix(
                &snap.prefix_components,
                self.prefix_w.get().saturating_sub(ansi_overhead),
            );
            render_prefix_components(&truncated_prefix, snap.status)
        };
        if new_prefix != *slot.cache.prefix.borrow() {
            slot.bar.set_prefix(new_prefix.clone());
            *slot.cache.prefix.borrow_mut() = new_prefix;
        }
        // Build display suffix: client truncation when installed, else
        // truncate the fresh component set then render.
        let display_suffix = if let Some(t) = truncation.as_ref() {
            // Client-truncated bars: pass the full merged suffix component
            // set so the client can render auto-derived fields alongside its
            // own fields.  The client also computes suffix width using these
            // same components.
            t.truncate_suffix(self.suffix_w.get(), &fresh_suffix)
        } else {
            let truncated_suffix = semantic_truncate_suffix(&fresh_suffix, self.suffix_w.get());
            render_suffix_components(&truncated_suffix, color_code)
        };
        if display_suffix != *slot.cache.suffix.borrow() {
            slot.bar.set_message(display_suffix.clone());
            *slot.cache.suffix.borrow_mut() = display_suffix;
        }
        if render_total != slot.cache.total.get() {
            slot.bar.set_length(render_total);
            slot.cache.total.set(render_total);
        }
        if render_pos != slot.cache.position.get() {
            slot.bar.set_position(render_pos);
            slot.cache.position.set(render_pos);
        }
    }

    /// Apply finish/abandon visual state to a completed slot.
    ///
    /// Sets the correct style for the slot's terminal status, calls
    /// `bar.finish()` or `bar.abandon()`, disables steady tick, and
    /// forces a final render.
    fn finish_slot(&self, i: usize, status: TrackStatus) {
        let slot = &self.slots[i];
        let prefix_w = self.prefix_w.get();
        let suffix_w = self.suffix_w.get();
        if self.has_overall && i == self.slots.len() - 1 {
            apply_overall_bar_style(&slot.bar, prefix_w, suffix_w);
        } else if status == TrackStatus::Failed {
            apply_failed_bar_style(&slot.bar, prefix_w, suffix_w);
        } else {
            apply_done_bar_style(&slot.bar, prefix_w, suffix_w);
        }
        match status {
            TrackStatus::Failed | TrackStatus::Warning => slot.bar.abandon(),
            _ => slot.bar.finish(),
        }
        slot.bar.tick();
    }

    /// Reserve the full terminal height before the first indicatif draw.
    ///
    /// Writes `rows` newlines to bypass [`BufferedTerm`] so they go directly
    /// to the terminal, then moves the cursor back up `rows` lines. This
    /// reserves the entire terminal screen for progress bar content,
    /// preventing intervening stderr content from being overwritten during
    /// bar draws.
    ///
    /// One-shot: only the first call writes; subsequent calls are no-ops. In
    /// test mode (`pre_roll_term` is `None`) this is always a no-op.
    ///
    /// # Scroll guarantee
    ///
    /// Moves the cursor to the absolute bottom of the terminal *before*
    /// writing blank lines, so every blank `write_line` triggers a scroll —
    /// newlines from a cursor partway down the screen would only fill the
    /// remaining rows below it, leaving visible content above exposed. After
    /// the blank lines the cursor returns to the top so indicatif can
    /// overwrite the now-empty visible area.
    fn pre_roll_if_needed(&self) {
        let Some(ref term) = self.pre_roll_term else {
            return;
        };
        if self.pre_rolled.swap(true, Ordering::AcqRel) {
            return;
        }
        let rows = self.dim_source.dimensions().0 as usize;
        // Move to the bottom first so every write_line causes a scroll.
        let _ = term.move_cursor_down(rows);
        for _ in 0..rows {
            let _ = term.write_line("");
        }
        let _ = term.move_cursor_up(rows);
    }

    /// Respond to terminal dimension changes since the last tick.
    ///
    /// Adjusts the slot capacity when height changes (prepending or
    /// draining blank slots) and re-applies bar styles when width
    /// crosses the 60-column compact/full template boundary.
    ///
    /// Returns `true` if any dimension actually changed.
    fn maybe_adjust_for_resize(&mut self) -> bool {
        let (rows, cols) = self.dim_source.dimensions();
        let mut changed = false;

        // --- Width reactivity ---
        if self.last_width != Some(cols) {
            self.last_width = Some(cols);
            changed = true;
            for i in 0..self.slots.len() {
                if self.slots[i].source.borrow().is_some() {
                    self.sync_slot(i);
                }
            }
        }

        // --- Height reactivity ---
        if self.dynamic_height {
            let desired_cap = (rows as usize).clamp(1, MAX_SLOTS);
            let current_cap = self.slots.len();
            if desired_cap > current_cap {
                changed = true;
                // Grow: append blank slots before the overall bar (or at
                // end when no overall bar exists).  New terminal space
                // appears at the bottom, so extending downward fills it
                // naturally instead of shifting existing bars.
                let insert_pos = self.slots.len() - usize::from(self.has_overall);
                for _ in 0..(desired_cap - current_cap) {
                    let pb = ProgressBar::new(0);
                    let bar = self.inner.insert(insert_pos, pb);
                    bar.set_style(blank_bar_style());
                    bar.set_message(" ");
                    bar.set_prefix("");
                    let slot =
                        RenderedSlot { bar, source: RefCell::new(None), cache: SlotCache::new() };
                    if let Some(orphan) = self.orphaned_states.borrow_mut().pop_back() {
                        slot.source.replace(Some(orphan));
                    }
                    self.slots.insert(insert_pos, slot);
                    self.slots_timing.insert(insert_pos, SlotTiming::new(&*self.time_source));
                }
                // Sync slots that may have been reattached.
                for i in 0..self.slots.len() {
                    self.sync_slot(i);
                }
            } else if desired_cap < current_cap {
                changed = true;
                // Shrink: evict from top until desired capacity is met.
                while self.slots.len() > desired_cap
                    && self.slots.len().saturating_sub(usize::from(self.has_overall)) > 0
                {
                    if let Some(source) = self.slots[0].source.borrow_mut().take() {
                        self.orphaned_states.borrow_mut().push_back(source);
                    }
                    self.inner.remove(&self.slots[0].bar);
                    self.slots.remove(0);
                    self.slots_timing.remove(0);
                }
            }
        }
        changed
    }

    /// Remove blank (unbound) reserved slots from [`MultiProgress`] and
    /// trigger a final draw so that only the non-blank finished bars
    /// remain visible in the terminal and in scrollback.
    ///
    /// This is intended as a replacement for [`clear()`](Self::clear)
    /// when the caller wants the final state of progress bars to
    /// persist in scrollback without empty reserved lines.
    ///
    /// Safe to call multiple times — only the first call has any effect.
    pub(crate) fn finalize(&self) {
        if self.finalized.replace(true) {
            return;
        }
        // Ensure pre_roll fires before the final draw.  When all bars
        // finish before the first ticker tick (≈50 ms), the ticker
        // never calls pre_roll_if_needed(), so bars would draw at the
        // current cursor position and overwrite existing terminal
        // content instead of scrolling it into scrollback.
        self.pre_roll_if_needed();
        // RAII guard: buffer OFF during final draw, re-enabled on drop.
        let _guard = self.gate.open();
        // Finish all bound bars that have reached a terminal state:
        // sync their final state FIRST (so position/total/elapsed/suffix
        // is up-to-date), then call finish_slot which applies the done
        // visual style.
        for (i, slot) in self.slots.iter().enumerate() {
            let snap = slot.source.borrow().as_ref().map(|s| s.snapshot());
            if let Some(ref snap) = snap
                && snap.status != TrackStatus::Active
            {
                self.sync_snapshot_to_bar(i, snap, None, None);
                self.finish_slot(i, snap.status);
            }
        }
        // Remove all blank (unbound) slots from MultiProgress.
        for slot in &self.slots {
            if slot.source.borrow().is_none() {
                self.inner.remove(&slot.bar);
            }
        }

        // Trigger one final draw with the reduced bar set.
        for slot in &self.slots {
            if slot.source.borrow().is_some() {
                slot.bar.tick();
                break;
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::progress::TestDimensionSource;
    use crate::progress::TestTimeSource;
    use crate::progress::inner::components::MAX_PREFIX_WIDTH;
    use crate::progress::inner::components::MAX_SUFFIX_WIDTH;
    use indicatif::MultiProgress;
    use indicatif::ProgressDrawTarget;
    use std::sync::Arc;

    #[test]
    fn recompute_layout_uniform_widths() {
        // Two bars with different prefix widths must converge to a single
        // uniform prefix_w equal to the max measured width (clamped to the
        // [MIN_PREFIX_WIDTH, MAX_PREFIX_WIDTH] band), and suffix_w must stay
        // within its own band.  This guards the dynamic cross-bar alignment.
        let term = indicatif::InMemoryTerm::new(10, 80);
        let target = ProgressDrawTarget::term_like(Box::new(term.clone()));
        let mp = MultiProgress::with_draw_target(target);
        let dims = Arc::new(TestDimensionSource::new((10, 80)));
        let ts = Arc::new(TestTimeSource::new());

        let mut renderer = ProgressRenderer::from_mp(
            mp,
            4,
            dims,
            WriteGate::new_noop(),
            ts as Arc<dyn TimeSource>,
            None,
            None,
        );

        // Short label ("a") and a 20-char label ("aaaaaaaaaaaaaaaaaaaa").
        let short =
            Arc::new(SharedState::with_time_source(100, "a", Arc::clone(&renderer.time_source)));
        let long = Arc::new(SharedState::with_time_source(
            100,
            "aaaaaaaaaaaaaaaaaaaa",
            Arc::clone(&renderer.time_source),
        ));
        renderer.attach(&short);
        renderer.attach(&long);

        renderer.recompute_layout();

        // The long label is 20 visible columns, but `snap.prefix` is rendered
        // WITH its leading `\x1b[0m` status escape (4 bytes) and indicatif
        // counts those escape bytes as visible characters in the
        // `{prefix:N.N}` field. So the uniform `prefix_w` must reserve
        // 20 + 4 = 24 columns to fit it without truncation.
        assert_eq!(
            renderer.prefix_w.get(),
            24,
            "prefix_w must equal max measured width + status ANSI overhead"
        );
        assert!(
            (MIN_PREFIX_WIDTH..=MAX_PREFIX_WIDTH).contains(&renderer.prefix_w.get()),
            "prefix_w must stay within [MIN_PREFIX_WIDTH, MAX_PREFIX_WIDTH]"
        );
        assert!(
            (MIN_SUFFIX_WIDTH..=MAX_SUFFIX_WIDTH).contains(&renderer.suffix_w.get()),
            "suffix_w must stay within [MIN_SUFFIX_WIDTH, MAX_SUFFIX_WIDTH]"
        );
    }
}
