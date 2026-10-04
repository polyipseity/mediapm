//! `ProgressBarHandle`, `SharedState`, and `ProgressRenderer` (pure tracking + indicatif rendering).
//!
//! # Known coverage gap: the client-truncation ANSI reset
//!
//! When a bar carries a client-installed [`BarLabelTruncation`], the push
//! point prefixes the client's string with `\x1b[0m` so colour from a
//! preceding bar cannot bleed into it. **No test asserts that this reset is
//! present.** Every route to the raw string is closed: `InMemoryTerm` strips
//! ANSI from what it reports, the debug sink records no `prefix` field,
//! `SlotCache::prefix` is private to this module, and [`visible_width`] cannot
//! see a zero-width escape.
//!
//! The gap is deliberate. A missing reset causes a colour bleed, not a layout
//! violation, and exposing the raw string purely so a test could observe four
//! bytes of ANSI would permanently widen the internal API for a cosmetic
//! guarantee. Anyone changing the wrapping below should know the property is
//! unguarded, and weigh adding a seam if the consequence of a regression grows.

use std::cell::{Cell, RefCell};
use std::collections::VecDeque;
use std::sync::atomic::{AtomicBool, AtomicU8, AtomicU64, Ordering};
use std::sync::{Arc, RwLock};
use std::time::{Duration, Instant};

use indicatif::{MultiProgress, ProgressBar, ProgressFinish};

use super::{
    DebugSlotState, DebugTickSnapshot, DimensionSource, FRAME_OVERHEAD_COLUMNS, MAX_SLOTS,
    MIN_BAR_FILL, MIN_PREFIX_WIDTH, MIN_SUFFIX_WIDTH, PrefixComponents, ProgressDebugSink,
    RealTimeSource, SuffixComponents, TimeSource, WriteGate, apply_bar_style, apply_done_bar_style,
    apply_failed_bar_style, apply_overall_bar_style, bar_color_code, blank_bar_style, format_count,
    format_elapsed, format_eta, format_rate, max_prefix_width, max_suffix_width,
    prefix_components_from_str, render_prefix_components, render_suffix_components,
    semantic_truncate_prefix, semantic_truncate_suffix, visible_width,
};
use crate::progress::BarStyle;

/// Compute the ANSI byte overhead for the prefix template field.
///
/// The `{prefix:N.N}` template in indicatif counts ANSI escape bytes as
/// visible characters. Client-truncated bars add only a 4-byte reset
/// (`\x1b[0m`); built-in rendering adds 13 bytes for failed/warning
/// status markers (`\x1b[0m\x1b[3Xm\x1b[0m`). This helper centralizes
/// the calculation so both `sync_snapshot_to_bar` and `recompute_layout`
/// use the same value.
fn compute_ansi_overhead(status: TrackStatus, has_client_truncation: bool) -> usize {
    if has_client_truncation {
        4
    } else {
        match status {
            TrackStatus::Failed | TrackStatus::Warning => 13,
            _ => 4,
        }
    }
}

/// Compose the full suffix component set from snapshot data and timing.
///
/// `keep_timing` decides whether the row draws its timing columns. See
/// [`keeps_timing`] for why a row that has dropped its fill sometimes keeps
/// them anyway. The strip itself lives in [`without_timing`], so the draw path
/// and the measurement pass in [`ProgressRenderer::recompute_layout`] cannot
/// disagree about what a row without its timing would hold.
fn compose_suffix(
    snap: &TrackSnapshot,
    rate_str: Option<&str>,
    eta_str: Option<&str>,
    keep_timing: bool,
) -> SuffixComponents {
    let auto_suffix = SuffixComponents {
        count: format_count(snap.position),
        total: format_count(snap.total),
        elapsed: format_elapsed(snap.elapsed),
        rate: rate_str.map(str::to_owned),
        eta: eta_str.map(str::to_owned),
        custom: String::new(),
    };
    let merged = SuffixComponents::merge(&auto_suffix, &snap.suffix_components);
    if keep_timing { merged } else { without_timing(&merged) }
}

/// Drop the timing columns from a merged suffix, keeping the tally and any
/// caller-set text.
///
/// `elapsed`, `rate` and `eta` answer "how long", and they are the first thing
/// a frame that has given up its fill gives up too, because the columns they
/// want are the ones the label and the tally want. `count`/`total` answer "how
/// far along" and `custom` carries the caller's own status words, so both stay.
fn without_timing(suffix: &SuffixComponents) -> SuffixComponents {
    SuffixComponents { elapsed: String::new(), rate: None, eta: None, ..suffix.clone() }
}

/// Whether a row draws its timing columns on a frame that has dropped its fill.
///
/// A frame whose fill is at [`MIN_BAR_FILL`] draws no bar at all, so its
/// columns go to the label and the tally, and the timing is what gives way
/// first. That trade is only worth making while the row still says something
/// without it, so the condition is about the row rather than about the frame:
/// `prefix` and `timingless_suffix` are what this row would draw with the
/// timing stripped, at the widths [`ProgressRenderer::recompute_layout`]
/// settled.
///
/// A row with nothing left keeps its timing instead. A worker slot on the
/// workflow screen has no tally of its own and a label that does not fit a
/// narrow line, so a stripped worker row renders `⠙` and nothing else, which
/// is a worse frame than the four-cell bar the fill would have drawn. Reading
/// this the other way round looks wrong until the bare-spinner case is on
/// screen; the timing is what is left, not what is spent first.
///
/// A frame that draws its fill is never bare, since four cells of `░░░░` are
/// the row's own report of how far along it is.
fn keeps_timing(draw_fill: bool, prefix: &str, timingless_suffix: &str) -> bool {
    draw_fill || (visible_width(prefix) == 0 && visible_width(timingless_suffix) == 0)
}

/// Render the suffix a slot draws into a `suffix_w`-column slot.
///
/// A client label decides which of its fields render, so it is asked; a bar
/// with no client label truncates the built-in components and renders those.
/// `sync_snapshot_to_bar` draws through this and
/// [`ProgressRenderer::recompute_layout`] measures through it, so what the
/// budget reserves and what the row shows are the same string.
fn render_slot_suffix(
    suffix: &SuffixComponents,
    truncation: Option<&Arc<dyn crate::progress::BarLabelTruncation>>,
    suffix_w: usize,
    color_code: &str,
) -> String {
    match truncation {
        Some(t) => t.truncate_suffix(suffix_w, suffix),
        None => render_suffix_components(&semantic_truncate_suffix(suffix, suffix_w), color_code),
    }
}

/// Measure the columns a merged suffix occupies once truncated to `ceiling`.
///
/// The measurement is what the budget reserves, so it goes through the same
/// [`render_slot_suffix`] the draw path uses.
fn measure_suffix_width(
    ceiling: usize,
    suffix: &SuffixComponents,
    truncation: Option<&Arc<dyn crate::progress::BarLabelTruncation>>,
) -> usize {
    visible_width(render_slot_suffix(suffix, truncation, ceiling, "").as_str())
}

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
    /// `true` when this handle's rendered state changed since the last push,
    /// so the renderer must draw the slot again. Cleared by the renderer's
    /// single push point after it has copied the values onto the bar.
    dirty: AtomicBool,
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
/// the display bar is managed separately by [`ProgressRenderer`], which
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
    /// [`ProgressScreen`](super::ProgressScreen)) with no display backend.
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

// ---- (ProgressTracker removed: use ProgressBarHandle::with_label) -----

// ---- ProgressRenderer + ProgressScreen (rendering + combined) ----------

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
/// 1. `attach` places new children into the **bottom** of the
///    active band (just above the overall bar if one exists) and shifts all
///    existing active children up by one slot, preserving chronological order
///    top-to-bottom.
/// 2. When all slots are occupied by active handles, finished slots are
///    recycled (scanning from the bottom upward).
/// 3. When no finished slot can be recycled, the new handle is pushed into
///    `orphaned_states` — tracked but with no render slot until the terminal
///    grows.
/// 4. Finished bars stay visible — their slots are only recycled when new
///    handles need display space.
pub struct ProgressRenderer {
    inner: MultiProgress,
    /// Render slots in draw order: children first, the overall bar last when
    /// one exists. Slot count is fixed per terminal height (see
    /// `dynamic_height`), and slots are recycled rather than removed.
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

    /// Nesting guard: `true` while inside [`run_frame`].  Panics in debug
    /// builds if `run_frame` or `tick` is called re-entrantly.
    in_frame: Cell<bool>,

    /// Optional JSONL debug sink — emits bar-state snapshots on every tick.
    /// Shared (`Arc`) because one sink belongs to the terminal and is used by
    /// every screen's renderer, so tick numbering stays monotonic across a
    /// sync instead of restarting per phase.
    debug_sink: Option<Arc<ProgressDebugSink>>,

    /// Current uniform prefix width applied to every visible bar this frame.
    /// Recomputed each tick from the widest measured prefix among bound slots,
    /// then held to what the terminal has left after the suffix and the bar
    /// floor, and never past [`MIN_PREFIX_WIDTH`] or [`max_prefix_width`].
    prefix_w: Cell<usize>,
    /// Current uniform suffix width applied to every visible bar this frame.
    /// See [`Self::prefix_w`] — same contract with [`MIN_SUFFIX_WIDTH`] /
    /// [`max_suffix_width`].
    suffix_w: Cell<usize>,
    /// Whether this frame's bars draw their fill.
    ///
    /// `false` once the fill the budget affords is at [`MIN_BAR_FILL`], which
    /// is the width a frame's labels leave the bar at every terminal narrower
    /// than the one where the labels stop overflowing the line. A fill that
    /// size carries a fixed quarter-resolution fraction the count beside it
    /// already states, and it is paid for out of a label clipped down to its
    /// tail, so below that point the frame drops it and the columns go to the
    /// label and the count. See [`Self::recompute_layout`].
    draw_fill: Cell<bool>,
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

/// Message a blanked slot carries.
///
/// One space rather than the empty string, because an empty message writes no
/// line at all and the rows above would lose their anchor.
const BLANK_MESSAGE: &str = " ";

/// Cached last values pushed to a bar, used to skip redundant indicatif
/// setter calls and reduce terminal flicker.
///
/// The cache describes **the bar**, not the source bound to it. A slot's
/// source moves between bars as the band shifts, but the bar's own state does
/// not, so a rebind keeps the cache and the next sync can still tell whether
/// the value it is about to push is already on screen. Rebuilding the cache on
/// a rebind throws that away and makes every rebind push a value the bar
/// already shows.
struct SlotCache {
    /// Last position sent to `set_position`.
    position: Cell<u64>,
    /// Last total sent to `set_length`.
    total: Cell<u64>,
    /// Last display suffix sent to the bar.
    suffix: RefCell<String>,
    /// Last prefix sent to `set_prefix`.
    prefix: RefCell<String>,
    /// Cached `prefix_w` at last `set_style` call (style dedup).
    style_prefix_w: Cell<usize>,
    /// Cached `suffix_w` at last `set_style` call (style dedup).
    style_suffix_w: Cell<usize>,
    /// Cached `is_overall` flag at last `set_style` call (style dedup).
    style_is_overall: Cell<bool>,
    /// Cached `draw_fill` flag at last `set_style` call (style dedup).
    style_draw_fill: Cell<bool>,
    /// Last status code at last `set_style` call (style dedup).
    style_status_code: Cell<u8>,
}

impl SlotCache {
    /// A cache that has never pushed onto its bar.
    ///
    /// `position` and `total` hold the `u64::MAX` never-pushed sentinel, and
    /// the style cells hold their own, so nothing a fresh bar already shows can
    /// be mistaken for a repeat of an earlier push.
    fn new() -> Self {
        Self {
            position: Cell::new(u64::MAX),
            total: Cell::new(u64::MAX),
            suffix: RefCell::new(String::new()),
            prefix: RefCell::new(String::new()),
            style_prefix_w: Cell::new(usize::MAX),
            style_suffix_w: Cell::new(usize::MAX),
            style_is_overall: Cell::new(false),
            style_draw_fill: Cell::new(false),
            style_status_code: Cell::new(u8::MAX),
        }
    }

    /// Forget the position last pushed, because the bar no longer holds it.
    ///
    /// [`indicatif::ProgressBar::reset`] returns the bar's position to zero and
    /// leaves its length alone, so only the position has to be invalidated:
    /// [`total`](Self::total) still describes the bar.
    fn invalidate_position(&self) {
        self.position.set(u64::MAX);
    }

    /// Record the blank state [`blank_new_bar`] just pushed onto a fresh bar.
    ///
    /// The style status code takes the same `u8::MAX` sentinel a fresh cache
    /// carries, so the next bind always re-applies the style. That sentinel on
    /// its own is ambiguous, which is what [`is_blanked`](Self::is_blanked)
    /// settles.
    fn mark_blanked(&self) {
        self.prefix.borrow_mut().clear();
        *self.suffix.borrow_mut() = BLANK_MESSAGE.to_string();
        self.style_status_code.set(u8::MAX);
    }

    /// Whether this slot was blanked and nothing has touched its bar since.
    ///
    /// A fresh cache carries the same `u8::MAX` style sentinel but an empty
    /// suffix, and a blanked slot carries a suffix of exactly
    /// [`BLANK_MESSAGE`], so the two cannot be confused. A real bind pushes
    /// both a prefix and a real status code, which clears the answer here, so a
    /// slot that was rebound and released again is never reported as blank.
    fn is_blanked(&self) -> bool {
        self.style_status_code.get() == u8::MAX
            && self.prefix.borrow().is_empty()
            && self.suffix.borrow().as_str() == BLANK_MESSAGE
    }
}

/// Put a freshly created bar into the blank state a new slot starts in.
///
/// Shared by slot construction and height growth so the state a fresh slot's
/// cache records is the state its bar is really in, and so the first
/// [`ProgressRenderer::blank_bar`] on it has nothing to do.
fn blank_new_bar(bar: &ProgressBar, cache: &SlotCache) {
    bar.set_style(blank_bar_style());
    bar.set_message(BLANK_MESSAGE);
    bar.set_prefix("");
    cache.mark_blanked();
}

/// Apply the screen's slot finish policy to a bar that has just been added to a [`MultiProgress`].
///
/// Slot bars use [`ProgressFinish::AndLeave`] so that a bar which is still **unfinished** when its screen is committed keeps the last line it drew, just like a finished one: indicatif's default [`ProgressFinish::AndClear`] makes `BarState::drop` run `finish_using_style`, which sets `Status::DoneHidden` and clears the line, so a partially-filled line — exactly what a `?` early return leaves behind — would be cleared instead of committed. That clearing draw only reaches a target that writes it: an **ungated** [`MultiProgress`] does, while the production target is a `BufferedTerm` write gate that suppresses every write outside an open window, leaving the frame [`ProgressRenderer::finalize`] drew as the last one the terminal sees.
///
/// The policy is therefore **defense-in-depth**, not a fix for an observed production defect: it keeps the retention contract from depending on the gate's window timing. See [`ProgressScreen::join`](super::terminal::ProgressScreen::join) for the full per-configuration statement.
///
/// `AndLeave` cannot change the finished-bar path: `BarState::drop` short-circuits on `is_finished()` (`indicatif/src/state.rs`), returns before `finish_using_style` is reached, and therefore never consults this policy.
///
/// Callers must add the bar to the [`MultiProgress`] first (as `from_mp` documents); this only returns the same bar with the policy applied.
fn with_slot_finish_policy(bar: ProgressBar) -> ProgressBar {
    bar.with_finish(ProgressFinish::AndLeave)
}

impl ProgressRenderer {
    /// Pre-allocate `capacity` blank bars in an existing [`MultiProgress`].
    pub(crate) fn from_mp(
        mp: MultiProgress,
        capacity: usize,
        dim_source: Arc<dyn DimensionSource>,
        gate: WriteGate,
        time_source: Arc<dyn TimeSource>,
        debug_sink: Option<Arc<ProgressDebugSink>>,
    ) -> Self {
        let mut slots = Vec::with_capacity(capacity);
        for _ in 0..capacity {
            let pb = ProgressBar::new(0);
            // IMPORTANT: add to MultiProgress FIRST, then configure.
            // Configuring before mp.add() prevents InMemoryTerm from
            // capturing blank bar output in tests.
            let bar = with_slot_finish_policy(mp.add(pb));
            let cache = SlotCache::new();
            blank_new_bar(&bar, &cache);
            slots.push(RenderedSlot { bar, source: RefCell::new(None), cache });
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
            in_frame: Cell::new(false),
            debug_sink,
            prefix_w: Cell::new(MIN_PREFIX_WIDTH),
            suffix_w: Cell::new(MIN_SUFFIX_WIDTH),
            draw_fill: Cell::new(true),
        }
    }

    /// Add an overall aggregate bar pinned at the bottom slot, drawing from
    /// `state`.
    ///
    /// `state` is the caller's own handle state, and the new slot adopts that
    /// very `Arc` as its render source: the caller's handle and the drawn
    /// overall bar are two views of one state, so every mutation made through
    /// the handle reaches the next frame. The bar's total and prefix are read
    /// from `state`, which stays the single source of truth for the overall bar
    /// — this method never fabricates a second state of its own.
    pub(crate) fn add_overall(&mut self, state: Arc<SharedState>) {
        let total = state.total.load(Ordering::Acquire);
        let label = {
            let guard = state.label.read().unwrap_or_else(std::sync::PoisonError::into_inner);
            guard.clone()
        };
        let inner = ProgressBar::new(total);
        let overall_bar = with_slot_finish_policy(self.inner.add(inner));
        // The style set here is the first frame's, before any
        // `recompute_layout` has run, so it carries the initial `draw_fill` and
        // is replaced on that first layout pass.
        apply_overall_bar_style(
            &overall_bar,
            MIN_PREFIX_WIDTH,
            MIN_SUFFIX_WIDTH,
            self.draw_fill.get(),
        );
        overall_bar.set_prefix(label);
        self.slots.push(RenderedSlot {
            bar: overall_bar,
            source: RefCell::new(Some(state)),
            cache: SlotCache::new(),
        });
        self.has_overall = true;
        self.slots_timing.push(SlotTiming::new(&*self.time_source));
    }

    /// Re-configure the bar at slot index `i` to reflect its current
    /// tracked source (or blank state if unbound).
    pub(crate) fn sync_slot(&self, i: usize) {
        let slot = &self.slots[i];
        if let Some(ref source) = *slot.source.borrow() {
            let snap = source.snapshot();
            let is_overall = self.has_overall && i == self.slots.len() - 1;
            let prefix_w = self.prefix_w.get();
            let suffix_w = self.suffix_w.get();
            let draw_fill = self.draw_fill.get();
            let status_code = snap.status.code();

            // Style dedup: only call set_style when dimensions or status changed.
            let style_changed = prefix_w != slot.cache.style_prefix_w.get()
                || suffix_w != slot.cache.style_suffix_w.get()
                || is_overall != slot.cache.style_is_overall.get()
                || draw_fill != slot.cache.style_draw_fill.get()
                || status_code != slot.cache.style_status_code.get();
            if style_changed {
                if is_overall {
                    apply_overall_bar_style(&slot.bar, prefix_w, suffix_w, draw_fill);
                } else if snap.status == TrackStatus::Failed {
                    apply_failed_bar_style(&slot.bar, prefix_w, suffix_w, draw_fill);
                } else if snap.status != TrackStatus::Active {
                    apply_done_bar_style(&slot.bar, prefix_w, suffix_w, draw_fill);
                } else {
                    // Slot recycling may leave the indicatif bar with
                    // Status::DoneVisible from the previous phase.  Reset
                    // it to InProgress so the spinner cycles again.
                    if slot.bar.is_finished() {
                        slot.bar.reset();
                        slot.cache.invalidate_position();
                    }
                    apply_bar_style(&slot.bar, prefix_w, suffix_w, draw_fill);
                }
                slot.cache.style_prefix_w.set(prefix_w);
                slot.cache.style_suffix_w.set(suffix_w);
                slot.cache.style_is_overall.set(is_overall);
                slot.cache.style_draw_fill.set(draw_fill);
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
            self.blank_bar(i);
        }
    }

    /// Return slot `i` to a blank row and record that it is blank.
    ///
    /// All three pushes are real `update_estimate_and_draw` calls, so blanking
    /// a slot that is already blank spends three draws to change nothing. The
    /// skip is what makes this safe to call from the paths that walk every slot
    /// on a height change, and it holds because a rebind clears the marker: see
    /// [`SlotCache::is_blanked`].
    fn blank_bar(&self, i: usize) {
        let slot = &self.slots[i];
        if slot.cache.is_blanked() {
            return;
        }
        blank_new_bar(&slot.bar, &slot.cache);
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
    ///
    /// Every slot keeps its [`SlotCache`]. The shift moves sources between
    /// slots, never bars between slots, so a slot's cache still describes
    /// what its own bar shows, and the sync that follows a rebind can skip
    /// a value that bar already holds. Rebuilding the cache here is what
    /// made every added bar cost a second `set_position` on the slot it
    /// landed in.
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
    /// spends the terminal's columns on them: what is left after the spinner,
    /// the separators and [`MIN_BAR_FILL`] is split between the two, the
    /// suffix taking what it measured first and the prefix the remainder.
    ///
    /// The result is stored in the `prefix_w`/`suffix_w` cells and every bound
    /// slot is re-synced so all bars share the same alignment width — short
    /// labels no longer waste space and long labels no longer overflow the bar.
    ///
    /// This is the budget and nothing else: it says how many columns a label
    /// may use, not which of its fields survive. That is
    /// [`semantic_truncate_prefix`] for the built-in components and the
    /// client's own [`BarLabelTruncation`] for a client label, and each ranks
    /// its own fields. Moving a ranking in here is tempting and wrong: it puts
    /// one cut in two places, and the two disagree the first time a screen
    /// gains a field.
    ///
    /// # The fill threshold
    ///
    /// A fill at [`MIN_BAR_FILL`] is worth nothing: four cells of `░░░░` show a
    /// quarter-resolution fraction the count beside it already states. So this
    /// function asks what the fill would actually be, and when the answer is
    /// the floor it spends those columns on the label and the count instead and
    /// draws the frame from the no-fill template.
    ///
    /// The question is asked against the budget that still reserves the floor,
    /// so it cannot answer differently next frame and oscillate: reserving the
    /// floor is what pins the fill at the floor. Only once the answer says no
    /// fill does the line drop the reservation, which frees four columns for
    /// the label.
    ///
    /// What that costs a screen is its own label widths, so the width at which
    /// the fill comes back differs per screen rather than being one number the
    /// renderer could hard-code. Measured over the three example screens at
    /// every width from 8 to 120, the fill sits at the floor at every width up
    /// to 58 on tool sync and materialization and up to 65 on the workflow
    /// screen, whose labels are seven columns wider.
    ///
    /// # The budget a row that kept its timing is drawn into
    ///
    /// A row with nothing left on it keeps its timing (see [`keeps_timing`]),
    /// and that timing is rendered into the suffix slot this function settled,
    /// truncated to that slot, rather than widening it. Reserving it here
    /// would let one row's clock take columns from the next row's label,
    /// which is the failure this budget exists to prevent, and it would make
    /// the width depend on which rows happen to be bare this frame.
    ///
    /// The timing a bare row keeps is therefore not measured here, so nothing
    /// this pass settles depends on which rows are bare this frame. The draw
    /// path reads the same two cells, asks [`keeps_timing`] the same question
    /// against them, and strips the timing through the same [`without_timing`]
    /// the measurement below uses, so the budget and the message cannot answer
    /// differently about the same row.
    pub(crate) fn recompute_layout(&self) {
        let (_, cols) = self.dim_source.dimensions();
        let prefix_ceiling = max_prefix_width(cols);
        let suffix_ceiling = max_suffix_width(cols);
        let mut max_prefix = 0usize;
        let mut max_suffix = 0usize;
        // The composed suffix with the frame's timing stripped, and the
        // client's truncation, for every bound slot in slot order. Filled on
        // the first pass so the fill-threshold second pass can measure it
        // without re-snapshotting.
        let mut measured: Vec<(
            SuffixComponents,
            Option<Arc<dyn crate::progress::BarLabelTruncation>>,
        )> = Vec::with_capacity(self.slots.len());
        for (i, slot) in self.slots.iter().enumerate() {
            if let Some(ref source) = *slot.source.borrow() {
                let snap = source.snapshot();
                let truncation =
                    source.truncation.read().expect("shared_state truncation lock").clone();
                let has_client_truncation = truncation.is_some();
                let status_overhead = compute_ansi_overhead(snap.status, has_client_truncation);
                // Measure what this bar will actually draw.  A bar with a
                // client label draws that label, so measuring the seed it was
                // constructed with would cap the slot at the placeholder text
                // every screen seeds its slots with, and a client label can
                // only ever be shortened.  Ask the client for its own output
                // at the ceiling, exactly as the suffix is measured below.
                let prefix_width = if let Some(ref t) = truncation {
                    let rendered =
                        t.truncate_prefix(prefix_ceiling.saturating_sub(status_overhead));
                    visible_width(rendered.as_str()) + status_overhead
                } else {
                    visible_width(snap.prefix.as_str()) + status_overhead
                };
                max_prefix = max_prefix.max(prefix_width);
                // Measure the full rendered suffix (auto fields + custom),
                // not just the stored custom text — the rendered RHS also
                // carries count/total/elapsed/rate/eta which consume the
                // width budget. Replicate the rate/eta computation from the
                // tick loop so the estimate matches what will actually draw.
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
                //
                // On a frame that keeps its fill, that is the whole suffix.
                // On a frame that drops the fill, what the budget reserves is
                // the suffix with the timing stripped, because that is the
                // narrower of the two and a row that keeps its timing renders
                // it into the slot this reserves. The timingless suffix is
                // kept so the no-fill pass below can measure it without
                // timing on a frame that turns out to draw no fill, which
                // saves re-snapshotting every slot.
                let suffix_with_timing =
                    compose_suffix(&snap, rate_str.as_deref(), eta_str.as_deref(), true);
                let suffix_without_timing =
                    compose_suffix(&snap, rate_str.as_deref(), eta_str.as_deref(), false);
                measured.push((suffix_without_timing, truncation.clone()));
                let suffix_width =
                    measure_suffix_width(suffix_ceiling, &suffix_with_timing, truncation.as_ref());
                max_suffix = max_suffix.max(suffix_width);
            }
        }
        // What is left of the line once the spinner, the separators and a
        // floor under the fill are paid for. Both label fields come out of it.
        // The suffix is settled first because it is the field that must not
        // wrap: a suffix past the end of the line spills onto the row below,
        // where it reads as a second bar.
        //
        // Settling a width is pure arithmetic over the two measured maxima, so
        // the fill-threshold second pass below reuses it rather than repeating
        // the suffix and prefix caps by hand.
        // The prefix cap carries the terminal term on purpose: without it the
        // slot is the widest seed label on screen, and a bar seeded with a short
        // placeholder never grows past it however much room the line has.
        let settle = |label_columns: usize, max_suffix: usize| {
            let suffix_w = max_suffix.min(suffix_ceiling).min(label_columns);
            let prefix_w = max_prefix.min(prefix_ceiling).min(label_columns - suffix_w);
            (prefix_w, suffix_w)
        };
        let reserved_columns =
            usize::from(cols).saturating_sub(FRAME_OVERHEAD_COLUMNS + MIN_BAR_FILL);
        let (prefix_w, suffix_w) = settle(reserved_columns, max_suffix);
        // A fill at the floor is four cells of `░░░░` or `████`, which says
        // nothing the count beside it does not. Once the labels take the rest
        // of the line the fill is pinned there at every narrower width, so this
        // is the widest terminal at which the bar has still stopped growing.
        let fill = usize::from(cols).saturating_sub(FRAME_OVERHEAD_COLUMNS + prefix_w + suffix_w);
        let draw_fill = fill > MIN_BAR_FILL;
        let (prefix_w, suffix_w) = if draw_fill {
            (prefix_w, suffix_w)
        } else {
            // The bar is gone, so the floor it was holding is not: give those
            // columns to the label, and re-measure the suffix without the
            // timing the frame no longer draws, or the slot it reserves would
            // be wider than the message that fills it. The measurement uses
            // the same `compose_suffix` the draw path composes with, so the
            // two are the same string rather than two rules that agree today.
            max_suffix = 0;
            for (suffix, truncation) in &measured {
                max_suffix = max_suffix.max(measure_suffix_width(
                    suffix_ceiling,
                    suffix,
                    truncation.as_ref(),
                ));
            }
            settle(usize::from(cols).saturating_sub(FRAME_OVERHEAD_COLUMNS), max_suffix)
        };
        self.prefix_w.set(prefix_w);
        self.suffix_w.set(suffix_w);
        self.draw_fill.set(draw_fill);
        for (i, slot) in self.slots.iter().enumerate() {
            if slot.source.borrow().is_some() {
                self.sync_slot(i);
            }
        }
    }

    /// Advance all progress bars by one frame.
    ///
    /// Recomputes the uniform alignment width, then redraws every visible
    /// bar from its tracked source state. Delegates entirely to `run_frame`
    /// to guarantee exactly one draw per tick.
    pub fn tick(&mut self) {
        self.run_frame();
    }

    /// Emit a JSONL snapshot of every slot's state to the configured debug sink.
    ///
    /// No-op when no sink is configured.  Called by
    /// [`run_frame`](Self::run_frame) **after** the dirty slots are synced, so
    /// `rate_bytes_per_sec` and `eta_secs` report the frame just computed
    /// rather than the previous one.
    fn emit_debug_snapshot(&self) {
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
    }

    /// Execute one frame: suppress writes, recompute layout, handle resize,
    /// sync every dirty slot (rate/ETA), emit the debug snapshot, advance
    /// spinners and draw once, then unsuppress.
    ///
    /// Every write happens while the gate is suppressed except the final
    /// draw, which is triggered by opening the gate (indicatif draws on the
    /// next bar operation while the gate is open).
    ///
    /// This is the terminal path's single frame entry point; pre-roll is not
    /// part of it (see [`finalize`](Self::finalize)).
    ///
    /// Returns immediately once [`finalize`](Self::finalize) has run: a
    /// finalized screen is committed, and repainting it would both rewrite the
    /// committed frame and re-draw bars the terminal has already released.
    pub(crate) fn run_frame(&mut self) {
        if self.finalized.get() {
            return;
        }
        // Nesting guard: panic in debug builds if called re-entrantly.
        debug_assert!(!self.in_frame.get(), "run_frame called while already in a frame");
        self.in_frame.set(true);

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

        // Step 4: Sync all dirty slots to bars (still suppressed).
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
                if snap.status == TrackStatus::Active {
                    // bar.tick() called in the spinner loop below.
                } else if source.is_cleared() {
                    self.blank_bar(i);
                } else {
                    self.finish_slot(i, snap.status);
                }
            }
        }

        // Step 5: Emit the debug snapshot now that rate/ETA are current.
        //
        // This must come after Step 4, not before it: the snapshot reports
        // `slots_timing[i].rate`, which Step 4 recomputes for every dirty
        // slot, so emitting earlier would make `rate_bytes_per_sec` and
        // `eta_secs` one frame stale on terminal screens.
        self.emit_debug_snapshot();

        // Step 6: Advance spinners and draw once.
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
        self.in_frame.set(false);
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
        let color_code = bar_color_code(snap.status, is_overall);

        // Client-defined truncation takes precedence when installed. The
        // renderer only *calls* the trait; it owns no field layout. The
        // `None` branch keeps the built-in component rendering as the
        // fallback so existing callers and tests stay green.
        let truncation = slot
            .source
            .borrow()
            .as_ref()
            .and_then(|s| s.truncation.read().expect("shared_state truncation lock").clone());
        let has_client_truncation = truncation.is_some();
        let ansi_overhead = compute_ansi_overhead(snap.status, has_client_truncation);
        let new_prefix = if let Some(t) = truncation.as_ref() {
            let raw = t.truncate_prefix(self.prefix_w.get().saturating_sub(ansi_overhead));
            format!("\x1b[0m{raw}")
        } else {
            let truncated_prefix = semantic_truncate_prefix(
                &snap.prefix_components,
                self.prefix_w.get().saturating_sub(ansi_overhead),
            );
            render_prefix_components(&truncated_prefix, snap.status)
        };
        // Whether this row draws its timing depends on what the row holds
        // without it, which takes both of this frame's settled widths to
        // render. The two are composed either way; the one that loses is the
        // one the row draws. Decided before the prefix is pushed so the
        // prefix is still in hand to answer it.
        let timingless_suffix = compose_suffix(snap, rate_str, eta_str, false);
        let timingless_rendered = render_slot_suffix(
            &timingless_suffix,
            truncation.as_ref(),
            self.suffix_w.get(),
            color_code,
        );
        let fresh_suffix = if keeps_timing(
            self.draw_fill.get(),
            new_prefix.as_str(),
            timingless_rendered.as_str(),
        ) {
            compose_suffix(snap, rate_str, eta_str, true)
        } else {
            timingless_suffix
        };
        if new_prefix != *slot.cache.prefix.borrow() {
            slot.bar.set_prefix(new_prefix.clone());
            *slot.cache.prefix.borrow_mut() = new_prefix;
        }
        // Build display suffix: client truncation when installed, else
        // truncate the fresh component set then render.
        let display_suffix =
            render_slot_suffix(&fresh_suffix, truncation.as_ref(), self.suffix_w.get(), color_code);
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
        let draw_fill = self.draw_fill.get();
        if self.has_overall && i == self.slots.len() - 1 {
            apply_overall_bar_style(&slot.bar, prefix_w, suffix_w, draw_fill);
        } else if status == TrackStatus::Failed {
            apply_failed_bar_style(&slot.bar, prefix_w, suffix_w, draw_fill);
        } else {
            apply_done_bar_style(&slot.bar, prefix_w, suffix_w, draw_fill);
        }
        match status {
            TrackStatus::Failed | TrackStatus::Warning => slot.bar.abandon(),
            _ => slot.bar.finish(),
        }
        slot.bar.tick();
    }

    /// Respond to terminal dimension changes since the last tick.
    ///
    /// Adjusts the slot capacity when height changes (prepending or
    /// draining blank slots) and re-applies bar styles so every slot
    /// picks up the new width.
    ///
    /// No template is selected by width. The four styles in `components.rs`
    /// are built per frame from the live `prefix_w`/`suffix_w` cells, and those
    /// cells are already paid for out of the terminal width, so a narrower
    /// terminal takes columns from the label first and only then from the
    /// fill. The width at which a screen runs out of fill is therefore a
    /// property of that screen's own labels and suffix, not a threshold in
    /// this function: `MIN_BAR_FILL` is what every screen keeps in common.
    ///
    /// Returns `true` if any dimension actually changed.
    pub(crate) fn maybe_adjust_for_resize(&mut self) -> bool {
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
                    let bar = with_slot_finish_policy(self.inner.insert(insert_pos, pb));
                    let cache = SlotCache::new();
                    blank_new_bar(&bar, &cache);
                    let slot = RenderedSlot { bar, source: RefCell::new(None), cache };
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
    /// The commit is completed by advancing the cursor past the frame
    /// ([`WriteGate::commit_frame`]): the draw protocol alone leaves the
    /// cursor on the frame's last row, which would leave the committed frame
    /// inside the next screen's band instead of below it.
    ///
    /// Safe to call multiple times — only the first call has any effect.
    pub(crate) fn finalize(&self) {
        if self.finalized.replace(true) {
            return;
        }
        // Pre-roll is deliberately absent here: the renderer does not own it.
        //
        // Pre-roll fires once per `ProgressTerminal`, from `build_screen`,
        // before the first bar of that terminal's first screen draws — and a
        // `ProgressTerminal` is the only way to build a screen, so every
        // on-screen bar gets the scroll of existing terminal content it needs
        // before its first frame. Do not add a renderer-side pre-roll fallback:
        // one pre-roll owner is the point of the split.
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

        // Trigger one final draw with the reduced bar set, and record whether
        // that draw happened: it is what the cursor advance below is
        // conditional on.
        let mut drew_frame = false;
        for slot in &self.slots {
            if slot.source.borrow().is_some() {
                slot.bar.tick();
                drew_frame = true;
                break;
            }
        }

        // Hand the frame to the terminal for good: the final draw leaves the
        // cursor on the frame's last row, so without this the frame is still
        // the live region and the next screen's frame claims its rows (see
        // `WriteGate::commit_frame`). Its caller contract puts it inside an
        // open write window — the `_guard` opened above holds one for the whole
        // of this method — because the write bypasses the gate and is the one
        // write of the commit that must not be suppressed.
        //
        // The advance is conditional on this screen having drawn a frame, and
        // that is not the obvious reading of "nothing was committed, so commit
        // nothing": the advance is not part of committing, it is compensation
        // for the draws the gate discarded while the frame was in progress (the
        // released draw walks the cursor to the frame's last row; a discarded
        // one does not).
        //
        // The condition is "a slot is bound NOW", which is what `drew_frame`
        // measures above, and a bound slot does imply at least one released
        // draw: `add_bar` writes the bar's first frame. It is not equivalent to
        // "this screen left no frame on the terminal". The two coincide only
        // for a screen that never bound a bar, where no draw reached the gate,
        // so no frame of this screen is on the terminal and the cursor is not
        // resting inside one; advancing there would write a blank row the
        // screen never drew.
        //
        // Known, accepted gap (public API only, cosmetic): a screen that binds
        // a bar, draws it, then has it evicted by a height shrink leaves that
        // frame on the terminal while `drew_frame` reads false at `finalize`,
        // so the advance is skipped and the next screen reclaims the row.
        // Reachable through the public builder with no overall bar,
        // `dynamic_height(true)`, and a dimension source whose row count grows
        // past the drawn slot and then shrinks back — the shrink evicts from
        // the top of the band, so the blank reserved slots above the drawn bar
        // go first and the drawn bar is orphaned only once they are gone. No
        // in-repo screen is measured to reach it: every production screen built
        // from a dynamic-height terminal registers an overall bar
        // (`mediapm-conductor/src/cli.rs:291`,
        // `mediapm/src/service.rs:866`/`1221`/`1301`/`1423`), whose slot is
        // still bound here, and the no-overall test screens that shrink their
        // injected source never join with an empty band. The consequence is a
        // stale row of a bar this screen has already orphaned being reclaimed,
        // which is why it is accepted rather than fixed. Any future fix must
        // keep the never-bound case, which
        // `gated_screen_without_bars_commits_nothing` pins.
        if drew_frame {
            self.gate.commit_frame();
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
    use indicatif::TermLike;
    use std::sync::Arc;
    use std::sync::atomic::AtomicUsize;

    /// A [`TermLike`] that counts the lines written to a wrapped
    /// [`indicatif::InMemoryTerm`].
    ///
    /// indicatif ends every draw with one `write_line`, so the count is the
    /// number of draws that reached the target. Re-blanking a blank slot
    /// changes nothing a frame can show, so a test that pins it has to count
    /// draws rather than read one.
    #[derive(Debug)]
    struct CountingTerm {
        /// The grid the draws land in, kept so a test can read the frame back.
        grid: indicatif::InMemoryTerm,
        /// Number of `write_line` calls since the counter was last read.
        lines: Arc<AtomicUsize>,
    }

    impl CountingTerm {
        fn new(rows: u16, cols: u16) -> Self {
            Self {
                grid: indicatif::InMemoryTerm::new(rows, cols),
                lines: Arc::new(AtomicUsize::new(0)),
            }
        }
    }

    impl TermLike for CountingTerm {
        fn width(&self) -> u16 {
            self.grid.width()
        }
        fn height(&self) -> u16 {
            self.grid.height()
        }
        fn move_cursor_up(&self, n: usize) -> std::io::Result<()> {
            self.grid.move_cursor_up(n)
        }
        fn move_cursor_down(&self, n: usize) -> std::io::Result<()> {
            self.grid.move_cursor_down(n)
        }
        fn move_cursor_right(&self, n: usize) -> std::io::Result<()> {
            self.grid.move_cursor_right(n)
        }
        fn move_cursor_left(&self, n: usize) -> std::io::Result<()> {
            self.grid.move_cursor_left(n)
        }
        fn write_line(&self, s: &str) -> std::io::Result<()> {
            self.lines.fetch_add(1, Ordering::Relaxed);
            self.grid.write_line(s)
        }
        fn write_str(&self, s: &str) -> std::io::Result<()> {
            self.grid.write_str(s)
        }
        fn clear_line(&self) -> std::io::Result<()> {
            self.grid.clear_line()
        }
        fn flush(&self) -> std::io::Result<()> {
            self.grid.flush()
        }
    }

    /// A renderer over a [`CountingTerm`], plus the draw counter it writes to.
    fn counting_renderer(
        rows: u16,
        cols: u16,
        capacity: usize,
    ) -> (ProgressRenderer, Arc<AtomicUsize>) {
        let term = CountingTerm::new(rows, cols);
        let lines = Arc::clone(&term.lines);
        let mp = MultiProgress::with_draw_target(ProgressDrawTarget::term_like(Box::new(term)));
        let dims = Arc::new(TestDimensionSource::new((rows, cols)));
        let ts = Arc::new(TestTimeSource::new());
        let renderer = ProgressRenderer::from_mp(
            mp,
            capacity,
            dims,
            WriteGate::new_noop(),
            ts as Arc<dyn TimeSource>,
            None,
        );
        (renderer, lines)
    }

    /// Blanking a slot that is already blank spends nothing.
    ///
    /// All three pushes `blank_bar` makes are real `update_estimate_and_draw`
    /// calls, and `maybe_adjust_for_resize` walks every slot on a height
    /// change, so a terminal that grows twice blanks the slots it added on the
    /// second pass too. The skip is safe because a rebind clears the marker:
    /// the second half of this test blanks a slot that has since been bound and
    /// does draw.
    #[test]
    fn blanking_an_already_blank_slot_draws_nothing() {
        let (mut renderer, lines) = counting_renderer(10, 80, 4);
        // Construction ends with one tick of the last slot, so measure from
        // here rather than from zero.
        let baseline = lines.load(Ordering::Relaxed);

        // Slot 0 is blank from construction.
        renderer.blank_bar(0);
        renderer.blank_bar(0);
        assert_eq!(
            lines.load(Ordering::Relaxed),
            baseline,
            "a slot that is already blank must not be drawn again"
        );

        let bar =
            Arc::new(SharedState::with_time_source(10, "tool", Arc::clone(&renderer.time_source)));
        renderer.attach(&bar);
        // attach binds the bottom slot, the only slot that is no longer blank.
        let bottom = renderer.slots.len() - 1;
        let after_attach = lines.load(Ordering::Relaxed);
        renderer.blank_bar(bottom);
        assert!(
            lines.load(Ordering::Relaxed) > after_attach,
            "a slot that has been bound and released is no longer blank, so blanking it draws"
        );
    }
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
