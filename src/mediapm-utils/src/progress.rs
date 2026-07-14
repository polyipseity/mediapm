//! Shared progress bar and download-progress types for mediapm CLIs.
//!
//! Crate consumers that want graphical progress bars enable the `progress`
//! feature (which pulls in `indicatif`).  The conductor library itself avoids
//! this dependency — it receives progress via [`ProgressCallback`] closures.
//!
//! # Architecture
//!
//! Progress tracking and rendering are separated into independent layers:
//!
//! | Layer | Types | Dependencies |
//! |---|---|---|
//! | **Tracking** (unlimited) | [`TrackedHandle`] | None (pure state) |
//! | **Rendering** (terminal-limited) | [`ProgressGroup`] | `indicatif` (behind feature) |
//! | **Recording** (testing) | [`recording::RecordingTrackedHandle`], [`recording::RecordingProgressTracker`] | None behind feature |
//!
//! # Types across feature boundaries
//!
//! | Type / fn | Available without `progress` | Available with `progress` |
//! |---|---|---|
//! | [`DownloadProgressSnapshot`] | ✅ | ✅ |
//! | [`ProgressCallback`] | ✅ | ✅ |
//! | [`TrackedHandle`] | ❌ | ✅ |
//! | [`ProgressGroup`] | ❌ | ✅ |
//! | [`ProgressRenderer`] | ❌ | ✅ |
//! | (no global toggle) | — | — |
//! | [`recording::RecordingProgressTracker`] | ❌ | ✅ |
//! | [`recording::RecordingTrackedHandle`] | ❌ | ✅ |
//! | [`recording::ProgressOp`] | ❌ | ✅ |

use std::sync::Arc;

// ---------------------------------------------------------------------------
// Download-progress types (always available)
// ---------------------------------------------------------------------------

/// Snapshot of download progress at one point in time.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct DownloadProgressSnapshot {
    /// Bytes downloaded so far.
    pub downloaded_bytes: u64,
    /// Total expected bytes, if known.
    pub total_bytes: Option<u64>,
}

/// Callback invoked with progress snapshots during a transfer.
pub type ProgressCallback = Arc<dyn Fn(DownloadProgressSnapshot) + Send + Sync>;

// ---------------------------------------------------------------------------
// Provider progress types (always available)
// ---------------------------------------------------------------------------

/// Which provider phase is currently active.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum ProviderPhase {
    /// Phase 1: resolving metadata and sources.
    Resolve,
    /// Phase 2: fetching or generating bytes.
    Fetch,
    /// Phase 3: postprocessing (extract, repack, CAS import).
    Postprocess,
}

/// Snapshot of provider progress at one point in time across all three phases.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct ProviderProgressSnapshot {
    /// Current phase.
    pub phase: ProviderPhase,
    /// Items completed vs total `(completed, total)`.
    /// Phase 1: sources resolved; Phase 2: files fetched; Phase 3: entries postprocessed.
    pub items: (u64, u64),
    /// Bytes completed vs total `(completed, total)`.
    /// Phase 1: `(0, 0)`; Phase 2: downloaded bytes; Phase 3: CAS-imported bytes.
    pub bytes: (u64, u64),
}

/// Callback invoked with provider progress snapshots during tool provisioning.
pub type ProviderProgressCallback = Arc<dyn Fn(ProviderProgressSnapshot) + Send + Sync>;

// ---------------------------------------------------------------------------
// Graphical progress bar types (only with `progress` feature)
// ---------------------------------------------------------------------------

#[cfg(feature = "progress")]
mod inner {
    use std::cell::{Cell, RefCell};
    use std::collections::VecDeque;
    use std::sync::atomic::{AtomicU8, AtomicU64, Ordering};
    use std::sync::{Arc, Mutex, RwLock};
    use std::thread;
    use std::time::{Duration, Instant};

    use indicatif::{MultiProgress, ProgressBar, ProgressStyle};

    // ---- dimension source (injectable for tests) -------------------------

    /// Source of terminal dimensions for responsive progress rendering.
    pub trait DimensionSource: Send + Sync {
        /// Returns `(rows, columns)` — the current terminal dimensions.
        fn dimensions(&self) -> (u16, u16);
    }

    /// Real terminal dimensions via [`console::Term::stderr`].
    pub struct RealTerminalSource;

    impl DimensionSource for RealTerminalSource {
        fn dimensions(&self) -> (u16, u16) {
            console::Term::stderr().size()
        }
    }

    /// Injectable dimensions for testing.
    ///
    /// Use [`set`](TestDimensionSource::set) to change dimensions mid-test
    /// so resize reactivity can be exercised without a real terminal.
    #[allow(dead_code)]
    pub struct TestDimensionSource {
        dims: Mutex<(u16, u16)>,
    }

    #[allow(dead_code)]
    impl TestDimensionSource {
        /// Create a source with the given initial dimensions.
        #[must_use]
        pub fn new(dims: (u16, u16)) -> Self {
            Self { dims: Mutex::new(dims) }
        }

        /// Override the dimensions returned by [`DimensionSource::dimensions`].
        ///
        /// # Panics
        ///
        /// Panics if the internal mutex is poisoned.
        pub fn set(&self, dims: (u16, u16)) {
            *self.dims.lock().unwrap() = dims;
        }
    }

    impl DimensionSource for TestDimensionSource {
        /// Returns the current dimensions.
        ///
        /// # Panics
        ///
        /// Panics if the internal mutex is poisoned.
        fn dimensions(&self) -> (u16, u16) {
            *self.dims.lock().unwrap()
        }
    }

    // ---- style constants --------------------------------------------------

    /// Format a duration compactly: `0s`, `3s`, `42s`, `1m35s`, `12m4s`, `2h15m`, `1d8h`, `30d`.
    pub(crate) fn format_elapsed(d: Duration) -> String {
        let total_secs = d.as_secs();
        if total_secs == 0 {
            return "0s".into();
        }
        let secs = total_secs % 60;
        let total_mins = total_secs / 60;
        if total_mins == 0 {
            return format!("{secs}s");
        }
        let mins = total_mins % 60;
        let total_hours = total_mins / 60;
        if total_hours == 0 {
            if secs > 0 {
                return format!("{total_mins}m{secs}s");
            }
            return format!("{total_mins}m");
        }
        let hours = total_hours % 24;
        let days = total_hours / 24;
        if days == 0 {
            if mins > 0 {
                return format!("{total_hours}h{mins}m");
            }
            return format!("{total_hours}h");
        }
        if hours > 0 {
            return format!("{days}d{hours}h");
        }
        format!("{days}d")
    }

    /// Format an ETA (seconds remaining) compactly: `5s`, `42s`, `1m35s`, `2h15m`, `1d8h`.
    /// Returns `"?"` when rate is zero or negative.
    pub(crate) fn format_eta(eta_secs: f64) -> String {
        if !eta_secs.is_finite() || eta_secs <= 0.0 {
            return "?".into();
        }
        format_elapsed(Duration::from_secs_f64(eta_secs))
    }

    /// Format a rate (units/second) compactly: `3.5/s`, `42/s`, `1.2k/s`, `123k/s`, `3.5M/s`.
    pub(crate) fn format_rate(rate: f64) -> String {
        if rate >= 1_000_000.0 {
            let v = rate / 1_000_000.0;
            if v < 10.0 { format!("{v:.1}M/s") } else { format!("{v:.0}M/s") }
        } else if rate >= 1_000.0 {
            let v = rate / 1_000.0;
            if v < 10.0 { format!("{v:.1}k/s") } else { format!("{v:.0}k/s") }
        } else if rate >= 1.0 {
            if rate < 10.0 { format!("{rate:.1}/s") } else { format!("{rate:.0}/s") }
        } else if rate * 60.0 >= 1.0 {
            format!("{:.0}/m", rate * 60.0)
        } else if rate * 3600.0 >= 1.0 {
            format!("{:.0}/h", rate * 3600.0)
        } else {
            format!("{:.0}/d", rate * 86400.0)
        }
    }

    /// Format a count with SI suffix: `0`, `999`, `1.2k`, `12.3k`, `123k`, `1.2M`, `12.3M`.
    #[allow(clippy::cast_precision_loss)]
    pub(crate) fn format_count(n: u64) -> String {
        if n >= 1_000_000_000 {
            format!("{:.1}G", n as f64 / 1_000_000_000.0)
        } else if n >= 1_000_000 {
            let v = n as f64 / 1_000_000.0;
            if v < 10.0 { format!("{v:.1}M") } else { format!("{v:.0}M") }
        } else if n >= 1_000 {
            let v = n as f64 / 1_000.0;
            if v < 10.0 { format!("{v:.1}k") } else { format!("{v:.0}k") }
        } else {
            n.to_string()
        }
    }

    const CHILD_BAR_TEMPLATE: &str =
        "{spinner:.green} {prefix:>20.20} {wide_bar:.yellow/dim} {msg:<25.50}";

    const OVERALL_BAR_TEMPLATE: &str =
        "{spinner:.green} {prefix:>20.20} {wide_bar:.magenta/dim} {msg:<25.50}";

    const COMPACT_BAR_TEMPLATE: &str = "{spinner:.green} {prefix:>20.20} {msg:<8.30}";

    const COMPACT_OVERALL_BAR_TEMPLATE: &str = "{spinner:.green} {prefix:>20.20} {msg:<8.30}";

    const DONE_BAR_TEMPLATE: &str =
        "{spinner:.white/.dim} {prefix:>20.20} {wide_bar:.green/dim} {msg:<25.50}";

    const COMPACT_DONE_BAR_TEMPLATE: &str = "{spinner:.white/.dim} {prefix:>20.20} {msg:<8.30}";

    const FAILED_BAR_TEMPLATE: &str =
        "{spinner:.red} {prefix:>20.20} {wide_bar:.red/dim} {msg:<25.50}";

    const COMPACT_FAILED_BAR_TEMPLATE: &str = "{spinner:.red} {prefix:>20.20} {msg:<8.30}";

    /// Maximum number of pre-allocated slot bars (safety cap).
    const MAX_SLOTS: usize = 200;

    /// ANSI SGR foreground color code matching the `{wide_bar}` template color.
    pub(super) fn bar_color_code(status: TrackStatus, is_overall: bool) -> &'static str {
        match status {
            TrackStatus::Failed => "31",
            TrackStatus::Abandoned | TrackStatus::Success | TrackStatus::Finished => "32",
            TrackStatus::Active if is_overall => "35",
            TrackStatus::Active => "33",
        }
    }

    /// Build the `{msg}` string: colored count/total + uncolored elapsed +
    /// uncolored rate + optional uncolored eta.
    ///
    /// When running:    `" {color}{count}/{total}{reset} {elapsed} {rate} [{eta}]"`
    /// When not running: `" {color}{count}/{total}{reset} {elapsed}"`
    pub(super) fn build_right_msg(
        color_code: &str,
        count_str: &str,
        total_str: &str,
        elapsed_str: &str,
        rate_str: Option<&str>,
        eta_str: Option<&str>,
    ) -> String {
        let mut s = format!(" \x1b[{color_code}m{count_str}/{total_str}\x1b[0m");
        s.push(' ');
        s.push_str(elapsed_str);
        if let Some(rate) = rate_str {
            s.push(' ');
            s.push_str(rate);
            if let Some(eta) = eta_str {
                s.push(' ');
                s.push_str(eta);
            }
        }
        s
    }

    /// Build prefix: always starts with ANSI reset to clear any SGR state
    /// from preceding template fields (e.g. `{spinner:.green}`).
    /// Normal states return just the prefix; failed/abandoned add a colored
    /// bracket before the prefix.
    pub(super) fn build_prefix(status: TrackStatus, prefix: &str) -> String {
        let reset = "\x1b[0m";
        match status {
            TrackStatus::Failed => format!("{reset}\x1b[31m[F]\x1b[0m {prefix}"),
            TrackStatus::Abandoned => format!("{reset}\x1b[33m[A]\x1b[0m {prefix}"),
            _ => format!("{reset}{prefix}"),
        }
    }

    fn child_bar_style() -> ProgressStyle {
        ProgressStyle::with_template(CHILD_BAR_TEMPLATE)
            .expect("invalid child bar template")
            .progress_chars("█░")
            .tick_chars("⠋⠙⠹⠸⠼⠴⠦⠧⠇⠏")
    }

    fn overall_bar_style() -> ProgressStyle {
        ProgressStyle::with_template(OVERALL_BAR_TEMPLATE)
            .expect("invalid overall bar template")
            .progress_chars("█░")
            .tick_chars("⠋⠙⠹⠸⠼⠴⠦⠧⠇⠏")
    }

    fn compact_overall_bar_style() -> ProgressStyle {
        ProgressStyle::with_template(COMPACT_OVERALL_BAR_TEMPLATE)
            .expect("invalid compact overall bar template")
            .tick_chars("⠋⠙⠹⠸⠼⠴⠦⠧⠇⠏")
    }

    fn apply_overall_bar_style(pb: &ProgressBar, width: u16) {
        if width < 60 {
            pb.set_style(compact_overall_bar_style());
        } else {
            pb.set_style(overall_bar_style());
        }
    }

    fn compact_bar_style() -> ProgressStyle {
        ProgressStyle::with_template(COMPACT_BAR_TEMPLATE)
            .expect("invalid compact bar template")
            .progress_chars("█░")
            .tick_chars("⠋⠙⠹⠸⠼⠴⠦⠧⠇⠏")
    }

    fn apply_bar_style(pb: &ProgressBar, width: u16) {
        if width < 60 {
            pb.set_style(compact_bar_style());
        } else {
            pb.set_style(child_bar_style());
        }
    }

    fn done_bar_style() -> ProgressStyle {
        ProgressStyle::with_template(DONE_BAR_TEMPLATE)
            .expect("invalid done bar template")
            .progress_chars("█░")
            .tick_chars("⠋⠙⠹⠸⠼⠴⠦⠧⠇⠏")
    }

    fn compact_done_bar_style() -> ProgressStyle {
        ProgressStyle::with_template(COMPACT_DONE_BAR_TEMPLATE)
            .expect("invalid compact done bar template")
            .tick_chars("⠋⠙⠹⠸⠼⠴⠦⠧⠇⠏")
    }

    fn failed_bar_style() -> ProgressStyle {
        ProgressStyle::with_template(FAILED_BAR_TEMPLATE)
            .expect("invalid failed bar template")
            .progress_chars("█░")
            .tick_chars("⠋⠙⠹⠸⠼⠴⠦⠧⠇⠏")
    }

    fn compact_failed_bar_style() -> ProgressStyle {
        ProgressStyle::with_template(COMPACT_FAILED_BAR_TEMPLATE)
            .expect("invalid compact failed bar template")
            .tick_chars("⠋⠙⠹⠸⠼⠴⠦⠧⠇⠏")
    }

    fn apply_done_bar_style(pb: &ProgressBar, width: u16) {
        if width < 60 {
            pb.set_style(compact_done_bar_style());
        } else {
            pb.set_style(done_bar_style());
        }
    }

    fn apply_failed_bar_style(pb: &ProgressBar, width: u16) {
        if width < 60 {
            pb.set_style(compact_failed_bar_style());
        } else {
            pb.set_style(failed_bar_style());
        }
    }

    fn blank_bar_style() -> ProgressStyle {
        ProgressStyle::with_template("{wide_msg}").expect("invalid blank bar template")
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
        /// Bar was abandoned.
        Abandoned,
        /// Bar finished (generic, no specific status).
        Finished,
    }

    /// Shared mutable state for a tracked progress handle.
    ///
    /// Interior mutability via atomics for numeric fields and [`RwLock`] for
    /// string fields.  [`Send`] + [`Sync`] when wrapped in [`Arc`].
    pub(crate) struct SharedState {
        position: AtomicU64,
        total: AtomicU64,
        label: RwLock<String>,
        prefix: RwLock<String>,
        status: AtomicU8,
        start_time: Instant,
        finished_elapsed: RwLock<Option<Duration>>,
    }

    impl SharedState {
        pub(crate) fn new(total: u64, label: &str) -> Self {
            Self {
                position: AtomicU64::new(0),
                total: AtomicU64::new(total),
                label: RwLock::new(label.to_string()),
                prefix: RwLock::new(label.to_string()),
                status: AtomicU8::new(0),
                start_time: Instant::now(),
                finished_elapsed: RwLock::new(None),
            }
        }

        fn snapshot(&self) -> TrackSnapshot {
            TrackSnapshot {
                position: self.position.load(Ordering::Relaxed),
                total: self.total.load(Ordering::Relaxed),
                label: self.label.read().expect("shared_state label lock").clone(),
                prefix: self.prefix.read().expect("shared_state prefix lock").clone(),
                status: match self.status.load(Ordering::Relaxed) {
                    0 => TrackStatus::Active,
                    1 => TrackStatus::Success,
                    2 => TrackStatus::Failed,
                    3 => TrackStatus::Abandoned,
                    _ => TrackStatus::Finished,
                },
                elapsed: self.elapsed(),
            }
        }

        pub(crate) fn elapsed(&self) -> Duration {
            if let Some(frozen) =
                *self.finished_elapsed.read().expect("shared_state finished_elapsed lock")
            {
                frozen
            } else {
                self.start_time.elapsed()
            }
        }

        pub(crate) fn mark_finished(&self) {
            let elapsed = self.start_time.elapsed();
            *self.finished_elapsed.write().expect("shared_state finished_elapsed lock") =
                Some(elapsed);
        }

        fn is_finished(&self) -> bool {
            self.status.load(Ordering::Relaxed) != 0
        }

        fn is_cleared(&self) -> bool {
            self.status.load(Ordering::Relaxed) == 5
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
        /// Prefix (shown before the bar).
        pub prefix: String,
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
    /// To create a no-op handle, use [`TrackedHandle::disabled`].
    /// All mutating methods on a disabled handle are zero-cost and do nothing.
    ///
    /// # Separation of concerns
    ///
    /// [`TrackedHandle`] manages **tracking state only** (`Arc<SharedState>`).
    /// The display bar is managed separately by [`ProgressRenderer`], which
    /// reads tracking state from the same `Arc<SharedState>` — mutating
    /// methods update state once and the renderer picks up changes
    /// asynchronously.
    #[derive(Clone)]
    pub struct TrackedHandle {
        pub(crate) state: Arc<SharedState>,
    }

    impl TrackedHandle {
        /// Create a no-op handle (all methods are zero-cost).
        #[must_use]
        pub fn disabled() -> Self {
            Self { state: Arc::new(SharedState::new(0, "")) }
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
        }

        /// Advance the bar by `delta` work units.
        pub fn advance(&self, delta: u64) {
            self.state.position.fetch_add(delta, Ordering::Relaxed);
        }

        /// Jump to an absolute position.
        pub fn set_position(&self, pos: u64) {
            self.state.position.store(pos, Ordering::Relaxed);
        }

        /// Set the prefix shown before the bar.
        ///
        /// # Panics
        ///
        /// Panics if the shared-state `RwLock` is poisoned.
        pub fn set_prefix(&self, prefix: impl Into<String>) {
            let prefix: String = prefix.into();
            (*self.state.prefix.write().expect("shared_state prefix lock")).clone_from(&prefix);
        }

        /// Mark the bar as finished (keeps it visible).
        pub fn finish(&self) {
            self.state.status.store(4, Ordering::Relaxed); // Finished
            self.state.mark_finished();
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

        /// Finish and clear the bar from the display.
        ///
        /// Stops the ticker and marks the bar as hidden. Call this instead of
        /// [`finish`](Self::finish) when the bar should disappear immediately.
        pub fn finish_and_clear(&self) {
            self.state.status.store(5, Ordering::Relaxed); // FinishedAndCleared
            self.state.mark_finished();
        }

        /// Abandon the bar — leaves it visible but stops all updates.
        pub fn abandon(&self) {
            self.state.status.store(3, Ordering::Relaxed); // Abandoned
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

    /// Manages a fixed-size grid of [`ProgressBar`] slots in
    /// [`MultiProgress`] with shift-based allocation and automatic
    /// recycling of finished slots.
    ///
    /// All slots are pre-allocated at construction so the draw height never
    /// changes — eliminating the root cause of terminal ghosting.
    ///
    /// # Allocation strategy
    ///
    /// 1. [`attach`](Self::attach) places new children into the **bottom** of
    ///    the active band (just above the overall bar if one exists) and
    ///    shifts all existing active children up by one slot.  This preserves
    ///    chronological order top-to-bottom (first-created child at the top
    ///    of the active band, last-created adjacent to the overall bar).
    /// 2. When all slots are occupied by active handles, finished slots are
    ///    recycled (scanning from the bottom upward).
    /// 3. When no finished slot can be recycled, the new handle is pushed
    ///    into [`orphaned_states`](Self::orphaned_states) — it is tracked but
    ///    has no render slot until the terminal grows.
    /// 4. Finished bars stay visible — their slots are only recycled when
    ///    new handles need display space.
    pub struct ProgressRenderer {
        inner: MultiProgress,
        slots: Vec<RenderedSlot>,
        has_overall: bool,
        dim_source: Arc<dyn DimensionSource>,
        last_width: Option<u16>,
        /// When `true`, the slot count may be adjusted on terminal height
        /// changes.  `false` when the caller specified an explicit capacity
        /// (e.g. via [`from_mp`](Self::from_mp)).
        dynamic_height: bool,
        /// Queue of [`SharedState`] handles evicted from render slots during
        /// height shrink.  Reattached (FIFO) when the terminal grows back.
        orphaned_states: RefCell<VecDeque<Arc<SharedState>>>,
        /// Guard against double-`finalize` from both
        /// [`join_and_clear`](Self::join_and_clear) and
        /// [`Drop`](Drop).
        finalized: Cell<bool>,
        /// EMA-smoothed rate tracking, one entry per slot.
        slots_timing: Vec<SlotTiming>,
    }

    /// EMA-smoothed rate tracking for a render slot.
    struct SlotTiming {
        prev_position: u64,
        prev_instant: Instant,
        rate: f64,
    }

    impl SlotTiming {
        fn new() -> Self {
            Self { prev_position: 0, prev_instant: Instant::now(), rate: 0.0 }
        }
    }

    /// Cached last values pushed to a bar, used to skip redundant indicatif
    /// setter calls and reduce terminal flicker.
    struct SlotCache {
        /// Last position sent to `set_position`.
        position: Cell<u64>,
        /// Last total sent to `set_length`.
        total: Cell<u64>,
        /// Last message sent to `set_message`.
        msg: RefCell<String>,
        /// Last prefix sent to `set_prefix`.
        prefix: RefCell<String>,
    }

    impl SlotCache {
        fn new() -> Self {
            Self {
                position: Cell::new(u64::MAX),
                total: Cell::new(u64::MAX),
                msg: RefCell::new(String::new()),
                prefix: RefCell::new(String::new()),
            }
        }
    }

    impl RenderedSlot {}

    impl ProgressRenderer {
        /// Pre-allocate `capacity` blank bars in an existing [`MultiProgress`].
        fn from_mp(
            mp: MultiProgress,
            capacity: usize,
            dim_source: Arc<dyn DimensionSource>,
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
                slots.push(RenderedSlot {
                    bar,
                    source: RefCell::new(None),
                    cache: SlotCache::new(),
                });
            }
            // Trigger a final draw so all bars are captured by InMemoryTerm
            // even when capacity == terminal height.
            if let Some(slot) = slots.last() {
                slot.bar.tick();
            }
            let slots_timing = (0..capacity).map(|_| SlotTiming::new()).collect();
            Self {
                inner: mp,
                slots,
                has_overall: false,
                dim_source,
                last_width: None,
                dynamic_height: false,
                orphaned_states: RefCell::new(VecDeque::new()),
                finalized: Cell::new(false),
                slots_timing,
            }
        }

        /// Pre-allocate `capacity` bars with an overall bar at the bottom,
        /// using an existing [`MultiProgress`].  Returns `(renderer, overall_state)`.
        fn from_mp_with_overall(
            mp: MultiProgress,
            capacity: usize,
            total: u64,
            label: &str,
            dim_source: Arc<dyn DimensionSource>,
        ) -> (Self, Arc<SharedState>) {
            let mut slots = Vec::with_capacity(capacity);
            for _ in 0..capacity.saturating_sub(1) {
                let pb = ProgressBar::new(0);
                let bar = mp.add(pb);
                bar.set_style(blank_bar_style());
                bar.set_message(" ");
                bar.set_prefix("");
                slots.push(RenderedSlot {
                    bar,
                    source: RefCell::new(None),
                    cache: SlotCache::new(),
                });
            }
            // Last slot = overall bar.
            let overall_state = Arc::new(SharedState::new(total, label));
            let inner = ProgressBar::new(total);
            let overall_bar = mp.add(inner);
            let (_, cols) = dim_source.dimensions();
            apply_overall_bar_style(&overall_bar, cols);
            overall_bar.set_prefix(label.to_string());
            slots.push(RenderedSlot {
                bar: overall_bar,
                source: RefCell::new(Some(overall_state.clone())),
                cache: SlotCache::new(),
            });
            let slots_timing = (0..capacity).map(|_| SlotTiming::new()).collect();
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
                    slots_timing,
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
                let (_, cols) = self.dim_source.dimensions();
                let is_overall = self.has_overall && i == self.slots.len() - 1;
                if is_overall {
                    apply_overall_bar_style(&slot.bar, cols);
                } else if snap.status == TrackStatus::Failed {
                    apply_failed_bar_style(&slot.bar, cols);
                } else if snap.status != TrackStatus::Active {
                    apply_done_bar_style(&slot.bar, cols);
                } else {
                    apply_bar_style(&slot.bar, cols);
                }
                slot.bar.set_prefix(build_prefix(snap.status, &snap.prefix));
                let elapsed_str = format_elapsed(snap.elapsed);
                let count_str = format_count(snap.position);
                let total_str = format_count(snap.total);
                let color_code = bar_color_code(snap.status, is_overall);
                let rate_str: Option<String> = if snap.status == TrackStatus::Active {
                    if self.slots_timing[i].rate > 0.0 {
                        Some(format_rate(self.slots_timing[i].rate))
                    } else {
                        Some("0/d".into())
                    }
                } else {
                    None
                };
                let msg = build_right_msg(
                    color_code,
                    &count_str,
                    &total_str,
                    &elapsed_str,
                    rate_str.as_deref(),
                    None,
                );
                slot.bar.set_message(msg);
                slot.bar.set_length(snap.total);
                slot.bar.set_position(snap.position);
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
        /// Attach a tracked state to the next available render slot.
        ///
        /// Places the new child at the **bottom** of the active band (just
        /// above the overall bar when one exists).  Existing active children
        /// are shifted up by one slot, preserving chronological order:
        /// first-created child at the top of the band, last-created adjacent
        /// to the overall bar.
        ///
        /// When all slots are occupied by active handles, scans for a
        /// finished slot to recycle.  When none is available, the handle is
        /// pushed to [`orphaned_states`] — it remains tracked but has no
        /// render slot until the terminal grows back.
        fn attach(&mut self, state: &Arc<SharedState>) {
            let child_cap = self.slots.len() - usize::from(self.has_overall);
            let bottom = child_cap.saturating_sub(1);

            // Phase 1: shift active band up, place new child at bottom
            let active =
                self.slots[..=bottom].iter().filter(|s| s.source.borrow().is_some()).count();

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
                self.slots_timing[bottom] = SlotTiming::new();
                self.slots[bottom].cache = SlotCache::new();
                self.sync_slot(bottom);
                return;
            }

            // Phase 2: recycle a finished slot (all slots occupied).
            for i in (0..=bottom).rev() {
                if self.slots[i].source.borrow().as_ref().is_none_or(|s| s.is_finished()) {
                    self.slots[i].source.replace(Some(Arc::clone(state)));
                    self.slots_timing[i] = SlotTiming::new();
                    self.slots[i].cache = SlotCache::new();
                    self.sync_slot(i);
                    return;
                }
            }
            // Phase 3: no free slot — push to orphaned queue.
            self.orphaned_states.borrow_mut().push_back(Arc::clone(state));
        }

        /// Defensive sync: refresh all render slots from their tracked sources.
        ///
        /// Includes resize reactivity and full style re-application.
        pub fn tick(&mut self) {
            self.maybe_adjust_for_resize();
            for (i, slot) in self.slots.iter().enumerate() {
                if let Some(ref source) = *slot.source.borrow() {
                    let snap = source.snapshot();

                    // Compute EMA-smoothed rate for display in active bars only.
                    // Rate is only recomputed when position actually changes.
                    let rate_str: Option<String> = if snap.status == TrackStatus::Active {
                        if snap.position != self.slots_timing[i].prev_position {
                            let now = Instant::now();
                            let dt =
                                now.duration_since(self.slots_timing[i].prev_instant).as_secs_f64();
                            if dt > 0.001 {
                                #[allow(clippy::cast_precision_loss)]
                                let current = (snap
                                    .position
                                    .saturating_sub(self.slots_timing[i].prev_position))
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
                        let remaining =
                            (snap.total - snap.position) as f64 / self.slots_timing[i].rate;
                        Some(format_eta(remaining))
                    } else {
                        None
                    };

                    self.sync_snapshot_to_bar(i, &snap, rate_str.as_deref(), eta_str.as_deref());
                    if snap.status == TrackStatus::Active {
                        slot.bar.tick();
                    } else if source.is_cleared() {
                        slot.bar.set_style(blank_bar_style());
                        slot.bar.set_message(" ");
                        slot.bar.set_prefix("");
                    } else {
                        self.finish_slot(i, snap.status);
                    }
                }
            }
        }

        /// Apply a snapshot's position/length/message/prefix to the
        /// indicatif bar at slot `i`.  This is the single place where
        /// `SharedState` is pushed to indicatif — both the daemon ticker
        /// and [`finalize`](Self::finalize) call through here.
        ///
        /// Does **not** change the bar's style — callers manage style
        /// independently via [`finish_slot`](Self::finish_slot) or
        /// explicit `set_style` calls during attach/resize.
        fn sync_snapshot_to_bar(
            &self,
            i: usize,
            snap: &TrackSnapshot,
            rate_str: Option<&str>,
            eta_str: Option<&str>,
        ) {
            let slot = &self.slots[i];
            let is_overall = self.has_overall && i == self.slots.len() - 1;
            let count_str = format_count(snap.position);
            let total_str = format_count(snap.total);
            let elapsed_str = format_elapsed(snap.elapsed);
            let color_code = bar_color_code(snap.status, is_overall);
            let msg = build_right_msg(
                color_code,
                &count_str,
                &total_str,
                &elapsed_str,
                rate_str,
                eta_str,
            );

            let new_prefix = build_prefix(snap.status, &snap.prefix);
            if new_prefix != *slot.cache.prefix.borrow() {
                slot.bar.set_prefix(new_prefix.clone());
                *slot.cache.prefix.borrow_mut() = new_prefix;
            }
            if msg != *slot.cache.msg.borrow() {
                slot.bar.set_message(msg.clone());
                *slot.cache.msg.borrow_mut() = msg;
            }
            if snap.total != slot.cache.total.get() {
                slot.bar.set_length(snap.total);
                slot.cache.total.set(snap.total);
            }
            if snap.position != slot.cache.position.get() {
                slot.bar.set_position(snap.position);
                slot.cache.position.set(snap.position);
            }
        }

        /// Apply finish/abandon visual state to a completed slot.
        ///
        /// Sets the correct style for the slot's terminal status, calls
        /// `bar.finish()` or `bar.abandon()`, disables steady tick, and
        /// forces a final render.
        fn finish_slot(&self, i: usize, status: TrackStatus) {
            let slot = &self.slots[i];
            if self.has_overall && i == self.slots.len() - 1 {
                slot.bar.set_style(overall_bar_style());
            } else if status == TrackStatus::Failed {
                slot.bar.set_style(failed_bar_style());
            } else {
                slot.bar.set_style(done_bar_style());
            }
            match status {
                TrackStatus::Failed | TrackStatus::Abandoned => slot.bar.abandon(),
                _ => slot.bar.finish(),
            }
            slot.bar.tick();
        }

        /// Respond to terminal dimension changes since the last tick.
        ///
        /// Adjusts the slot capacity when height changes (prepending or
        /// draining blank slots) and re-applies bar styles when width
        /// crosses the 60-column compact/full template boundary.
        fn maybe_adjust_for_resize(&mut self) {
            let (rows, cols) = self.dim_source.dimensions();

            // --- Width reactivity ---
            if self.last_width != Some(cols) {
                self.last_width = Some(cols);
                for i in 0..self.slots.len() {
                    let slot = &self.slots[i];
                    if slot.source.borrow().is_some() {
                        let is_overall = self.has_overall && i == self.slots.len() - 1;
                        if is_overall {
                            apply_overall_bar_style(&slot.bar, cols);
                        } else {
                            apply_bar_style(&slot.bar, cols);
                        }
                    }
                }
            }

            // --- Height reactivity ---
            if self.dynamic_height {
                let desired_cap = (rows as usize).clamp(1, MAX_SLOTS);
                let current_cap = self.slots.len();
                if desired_cap > current_cap {
                    // Grow: prepend blank slots at the top, reattaching orphans.
                    for _ in 0..(desired_cap - current_cap) {
                        let pb = ProgressBar::new(0);
                        let bar = self.inner.insert(0, pb);
                        bar.set_style(blank_bar_style());
                        bar.set_message(" ");
                        bar.set_prefix("");
                        let slot = RenderedSlot {
                            bar,
                            source: RefCell::new(None),
                            cache: SlotCache::new(),
                        };
                        if let Some(orphan) = self.orphaned_states.borrow_mut().pop_back() {
                            slot.source.replace(Some(orphan));
                        }
                        self.slots.insert(0, slot);
                        self.slots_timing.insert(0, SlotTiming::new());
                    }
                    // Sync slots that may have been reattached.
                    for i in 0..self.slots.len() {
                        self.sync_slot(i);
                    }
                } else if desired_cap < current_cap {
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
        fn finalize(&self) {
            if self.finalized.replace(true) {
                return;
            }
            // Finish all bound bars that have reached a terminal state:
            // sync their final state FIRST (so position/total/elapsed/message
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

    // ---- ProgressGroup (combined tracking + rendering) --------------------

    /// A vertical stack of progress bars.
    ///
    /// Bars are drawn in a fixed-height grid determined by the terminal height
    /// at construction time.  The draw height never changes, which eliminates
    /// ghosting from bar-count changes.
    ///
    /// To create a no-op group, use [`ProgressGroup::disabled`].
    pub struct ProgressGroup {
        /// `None` when progress is disabled.
        renderer: Option<Arc<Mutex<ProgressRenderer>>>,
        /// Daemon ticker thread driving renders at 50 ms intervals.
        /// Holds a `Weak` reference to the renderer — exits cleanly
        /// when the renderer is dropped.
        _ticker: Option<thread::JoinHandle<()>>,
    }

    // ---- ProgressGroupBuilder -------------------------------------------------

    /// Builder for [`ProgressGroup`] with optional configuration.
    ///
    /// # Defaults
    ///
    /// | Field | Default |
    /// |---|---|
    /// | `mp` | `None` (creates a fresh [`MultiProgress`]) |
    /// | `dim_source` | [`RealTerminalSource`] |
    /// | `overall` | `None` (no overall bar) |
    /// | `capacity` | `None` (derived from terminal height via `dim_source`) |
    /// | `dynamic_height` | `true` |
    ///
    /// # Examples
    ///
    /// ```ignore
    /// let group = ProgressGroup::builder().build();
    /// let (group, overall) = ProgressGroup::builder().with_overall("sync", 10).build_with_overall();
    /// ```
    #[derive(Clone)]
    pub struct ProgressGroupBuilder {
        mp: Option<MultiProgress>,
        dim_source: Arc<dyn DimensionSource>,
        overall: Option<(String, u64)>,
        capacity: Option<usize>,
        dynamic_height: bool,
    }

    impl Default for ProgressGroupBuilder {
        fn default() -> Self {
            Self {
                mp: None,
                dim_source: Arc::new(RealTerminalSource),
                overall: None,
                capacity: None,
                dynamic_height: false,
            }
        }
    }

    impl ProgressGroupBuilder {
        /// Use an existing [`MultiProgress`] instead of creating a fresh one.
        #[must_use]
        pub fn with_multi_progress(mut self, mp: MultiProgress) -> Self {
            self.mp = Some(mp);
            self
        }

        /// Use an injectable dimension source (for tests).
        #[must_use]
        pub fn with_dim_source(mut self, dim_source: Arc<dyn DimensionSource>) -> Self {
            self.dim_source = dim_source;
            self
        }

        /// Add an overall aggregate bar pinned at the bottom.
        #[must_use]
        pub fn with_overall(mut self, label: &str, total: u64) -> Self {
            self.overall = Some((label.to_string(), total));
            self
        }

        /// Set the exact slot capacity (clamped to `[1, MAX_SLOTS]`).
        /// When `None` (default), capacity is derived from terminal height.
        #[must_use]
        pub fn capacity(mut self, n: usize) -> Self {
            self.capacity = Some(n);
            self
        }

        /// Enable or disable dynamic height adaptation (default: `true`).
        #[must_use]
        pub fn dynamic_height(mut self, enabled: bool) -> Self {
            self.dynamic_height = enabled;
            self
        }

        /// Build a group without an overall bar.
        ///
        /// # Panics
        ///
        /// Panics if [`with_overall`](Self::with_overall) was called — use
        /// [`build_with_overall`](Self::build_with_overall) instead.
        /// Panics when the internal `Mutex` is poisoned.
        #[must_use]
        pub fn build(self) -> ProgressGroup {
            assert!(
                self.overall.is_none(),
                "use build_with_overall() when with_overall() was called"
            );
            let cap = self.capacity.unwrap_or_else(|| {
                let (rows, _) = self.dim_source.dimensions();
                (rows as usize).clamp(1, MAX_SLOTS)
            });
            let mp = self.mp.unwrap_or_default();
            let mut renderer = ProgressRenderer::from_mp(mp, cap, self.dim_source);
            renderer.dynamic_height = self.dynamic_height;
            let renderer = Some(Arc::new(Mutex::new(renderer)));
            let ticker = Some(ProgressGroup::spawn_ticker(renderer.as_ref().unwrap()));
            ProgressGroup { renderer, _ticker: ticker }
        }

        /// Build a group with an overall aggregate bar.
        ///
        /// # Panics
        ///
        /// Panics if [`with_overall`](Self::with_overall) was not called.
        /// Panics when the internal `Mutex` is poisoned.
        #[must_use]
        pub fn build_with_overall(self) -> (ProgressGroup, TrackedHandle) {
            let (label, total) =
                self.overall.expect("with_overall() must be called before build_with_overall()");
            let cap = self.capacity.unwrap_or_else(|| {
                let (rows, _) = self.dim_source.dimensions();
                (rows as usize).clamp(1, MAX_SLOTS)
            });
            let mp = self.mp.unwrap_or_default();
            let (mut renderer, state) =
                ProgressRenderer::from_mp_with_overall(mp, cap, total, &label, self.dim_source);
            renderer.dynamic_height = self.dynamic_height;
            let renderer = Arc::new(Mutex::new(renderer));
            let ticker = Some(ProgressGroup::spawn_ticker(&renderer));
            let handle = TrackedHandle { state };
            (ProgressGroup { renderer: Some(renderer), _ticker: ticker }, handle)
        }
    }

    impl ProgressGroup {
        /// Create a builder for configuring a [`ProgressGroup`].
        #[must_use]
        pub fn builder() -> ProgressGroupBuilder {
            ProgressGroupBuilder::default()
        }

        /// Create a no-op group that produces no terminal output.
        ///
        /// All bars added via [`add_bar`] return [`TrackedHandle::disabled`].
        /// Useful in tests where progress is not needed.
        #[must_use]
        pub fn disabled() -> Self {
            Self { renderer: None, _ticker: None }
        }

        /// Add a child bar to the group.
        ///
        /// Creates a tracking handle and (when a renderer is available)
        /// allocates a render slot for display.  When all slots are occupied
        /// by active handles, the bar is still tracked but has no display.
        ///
        /// # Panics
        ///
        /// Panics when the internal `Mutex` is poisoned (another thread
        /// panicked while holding the lock).
        #[must_use]
        pub fn add_bar(&self, total: u64, label: &str) -> TrackedHandle {
            let Some(ref renderer) = self.renderer else {
                return TrackedHandle::disabled();
            };
            let state = Arc::new(SharedState::new(total, label));
            {
                let mut locked = renderer.lock().unwrap_or_else(|e| e.into_inner());
                locked.attach(&state);
            }
            TrackedHandle { state }
        }

        /// Block until all bars in the group reach a finished state.
        ///
        /// In indicatif 0.17 `MultiProgress` has no blocking join, so this is
        /// effectively a no-op.  Bars remain visible in the terminal after
        /// this call.
        pub fn join(&self) {}

        /// Clear the terminal display after all bars are done.
        ///
        /// Remove blank reserved slots and keep only the non-blank finished
        /// bars visible.  Unlike the name suggests, this does **not** clear
        /// the terminal display — it collapses blank reserved slots so that
        /// scrollback shows only meaningful progress bars.
        ///
        /// Prefer [`join()`](Self::join) to keep bars fully visible without
        /// the collapsing step.
        ///
        /// # Panics
        ///
        /// Panics when the internal `Mutex` is poisoned (another thread
        /// panicked while holding the lock).
        pub fn join_and_clear(&self) {
            if let Some(ref renderer) = self.renderer {
                renderer.lock().unwrap_or_else(|e| e.into_inner()).finalize();
            }
        }

        /// Force a render sync (used in tests with
        /// [`InMemoryTerm`](indicatif::InMemoryTerm) where the timer
        /// thread does not run).
        ///
        /// # Panics
        ///
        /// Panics when the internal `Mutex` is poisoned (another thread
        /// panicked while holding the lock).
        pub fn tick(&self) {
            if let Some(ref renderer) = self.renderer {
                renderer.lock().unwrap_or_else(|e| e.into_inner()).tick();
            }
        }

        /// Spawn a daemon thread that drives render updates at 50 ms
        /// intervals.  Holds a `Weak` reference so the thread exits
        /// cleanly when the renderer is dropped.
        ///
        /// # Panics
        ///
        /// Panics when the internal `Mutex` is poisoned (another thread
        /// panicked while holding the lock).
        fn spawn_ticker(renderer: &Arc<Mutex<ProgressRenderer>>) -> thread::JoinHandle<()> {
            let weak = Arc::downgrade(renderer);
            thread::spawn(move || {
                while let Some(r) = weak.upgrade() {
                    let Ok(mut guard) = r.lock() else {
                        break;
                    };
                    guard.tick();
                    thread::sleep(Duration::from_millis(50));
                }
            })
        }
    }

    impl Default for ProgressGroup {
        fn default() -> Self {
            Self::builder().build()
        }
    }

    impl Drop for ProgressGroup {
        fn drop(&mut self) {
            if let Some(ref renderer) = self.renderer {
                renderer.lock().unwrap_or_else(|e| e.into_inner()).finalize();
            }
        }
    }
}

#[cfg(feature = "progress")]
pub use inner::{
    DimensionSource, ProgressGroup, ProgressRenderer, RealTerminalSource, TestDimensionSource,
    TrackSnapshot, TrackStatus, TrackedHandle,
};

#[cfg(feature = "progress")]
#[allow(unused_imports)]
pub(crate) use inner::{SharedState, format_elapsed, format_rate};

// ---- Shared API traits for dependency injection (feature-gated) -------

/// Minimum progress-bar handle API for dependency injection.
///
/// Both [`TrackedHandle`] and
/// [`RecordingTrackedHandle`](recording::RecordingTrackedHandle) implement
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
    /// Return a data-copy snapshot of the tracking state.
    fn snapshot(&self) -> TrackSnapshot;
    /// Returns `true` if the handle has been finished/abandoned.
    fn is_finished(&self) -> bool;
    /// Mark the bar as finished (keeps it visible).
    fn finish(&self);
    /// Jump to an absolute position.
    fn set_position(&self, pos: u64);
    /// Change the total mid-flight for dynamic workloads.
    fn set_total(&self, total: u64);
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
    fn snapshot(&self) -> TrackSnapshot {
        TrackedHandle::snapshot(self)
    }
    fn is_finished(&self) -> bool {
        TrackedHandle::is_finished(self)
    }
    fn finish(&self) {
        TrackedHandle::finish(self);
    }
    fn set_position(&self, pos: u64) {
        TrackedHandle::set_position(self, pos);
    }
    fn set_total(&self, total: u64) {
        TrackedHandle::set_total(self, total);
    }
}

/// Minimum progress-group API for dependency injection.
///
/// Both [`ProgressGroup`] and
/// [`RecordingProgressTracker`](recording::RecordingProgressTracker) implement
/// this trait, allowing consumer functions to accept either a real display
/// group or a recording group for testing.
#[cfg(feature = "progress")]
pub trait ProgressGroupApi {
    /// Add a child bar and return an [`Arc`]-wrapped handle.
    fn add_bar(&self, total: u64, label: &str) -> Arc<dyn ProgressBarApi>;
}

#[cfg(feature = "progress")]
impl ProgressGroupApi for ProgressGroup {
    fn add_bar(&self, total: u64, label: &str) -> Arc<dyn ProgressBarApi> {
        Arc::new(ProgressGroup::add_bar(self, total, label))
    }
}

// ---- Recording types for test assertions (feature-gated) ---------------

/// Recording progress operations for test assertions.
///
/// This module provides [`RecordingProgressTracker`] and
/// [`RecordingTrackedHandle`] that record all operations into a shared
/// operation log without any visual output. Use
/// [`RecordingProgressTracker::ops`] to retrieve the recorded sequence for
/// verification.
///
/// Only available when the `progress` feature is enabled.
#[cfg(feature = "progress")]
#[allow(clippy::missing_panics_doc)]
pub mod recording {
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
        /// `set_prefix(prefix)` was called.
        SetPrefix {
            /// Prefix text.
            prefix: String,
        },
        /// `finish()` was called.
        Finish,
        /// `finish_success()` was called.
        FinishSuccess,
        /// `finish_error()` was called.
        FinishError,
        /// `finish_and_clear()` was called.
        FinishAndClear,
        /// `abandon()` was called.
        Abandon,
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

        /// Set the prefix.
        pub fn set_prefix(&self, prefix: impl Into<String>) {
            self.ops
                .lock()
                .expect("recording lock")
                .push(ProgressOp::SetPrefix { prefix: prefix.into() });
        }

        /// Mark the handle as finished.
        pub fn finish(&self) {
            self.ops.lock().expect("recording lock").push(ProgressOp::Finish);
            self.mark_finished();
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

        /// Finish and clear from display.
        pub fn finish_and_clear(&self) {
            self.ops.lock().expect("recording lock").push(ProgressOp::FinishAndClear);
            self.mark_finished();
        }

        /// Abandon — leaves visible but stops updates.
        pub fn abandon(&self) {
            self.ops.lock().expect("recording lock").push(ProgressOp::Abandon);
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
            let mut elapsed =
                self.finished_elapsed.lock().expect("recording finished_elapsed lock");
            if elapsed.is_none() {
                *elapsed = Some(self.start_time.elapsed());
            }
        }
    }
}

// ---- Trait impls for recording types -------------------------------------

#[cfg(feature = "progress")]
impl ProgressBarApi for recording::RecordingTrackedHandle {
    fn advance(&self, delta: u64) {
        recording::RecordingTrackedHandle::advance(self, delta);
    }
    fn finish_success(&self) {
        recording::RecordingTrackedHandle::finish_success(self);
    }
    fn finish_error(&self) {
        recording::RecordingTrackedHandle::finish_error(self);
    }
    fn snapshot(&self) -> TrackSnapshot {
        TrackSnapshot {
            position: 0,
            total: self.total(),
            label: String::new(),
            prefix: String::new(),
            status: TrackStatus::Active,
            elapsed: recording::RecordingTrackedHandle::snapshot_elapsed(self),
        }
    }
    fn is_finished(&self) -> bool {
        let ops = self.ops();
        ops.iter().any(|op| {
            matches!(
                op,
                recording::ProgressOp::Finish
                    | recording::ProgressOp::FinishSuccess
                    | recording::ProgressOp::FinishError
                    | recording::ProgressOp::FinishAndClear
                    | recording::ProgressOp::Abandon
            )
        })
    }
    fn finish(&self) {
        recording::RecordingTrackedHandle::finish(self);
    }
    fn set_position(&self, pos: u64) {
        recording::RecordingTrackedHandle::set_position(self, pos);
    }
    fn set_total(&self, total: u64) {
        recording::RecordingTrackedHandle::set_total(self, total);
    }
}

#[cfg(feature = "progress")]
impl ProgressGroupApi for recording::RecordingProgressTracker {
    fn add_bar(&self, total: u64, label: &str) -> Arc<dyn ProgressBarApi> {
        Arc::new(recording::RecordingProgressTracker::add_bar(self, total, label))
    }
}

// ---- Tests ---------------------------------------------------------------

#[cfg(test)]
#[cfg(feature = "progress")]
mod tests {
    //! # Defense-in-depth
    //!
    //! Tests in this module are organized by layer:
    //!
    //! * **Recording** — [`RecordingProgressTracker`] tests verify the op-log
    //!   produced by each method call (correct sequence of [`ProgressOp`]
    //!   entries).
    //! * **State-mutation** — [`TrackedHandle::new`] / [`TrackedHandle::with_label`]
    //!   tests verify that underlying [`SharedState`] is updated correctly
    //!   (positions, totals, status, elapsed).
    //! * **Renderer integration** — [`ProgressGroup`] tests verify that the
    //!   full tracking-to-terminal path produces correct visual output.
    //!
    //! Each layer covers the same behavioral surface through different
    //! observation points, providing redundant coverage against regressions
    //! even when the observation mechanism itself has a bug.

    use std::sync::Arc;

    use super::recording::{ProgressOp, RecordingProgressTracker, RecordingTrackedHandle};
    use super::{ProgressGroup, TrackStatus, TrackedHandle};
    use indicatif::MultiProgress;

    #[test]
    fn progress_enabled_no_global_toggle() {
        // Constructors always produce enabled handles.
        let h = TrackedHandle::new(100);
        assert_eq!(h.total(), 100, "enabled handle reports initial total");
        let g = ProgressGroup::builder().build();
        let ch = g.add_bar(50, "child");
        assert_eq!(ch.total(), 50);
        let (_og, oh) = ProgressGroup::builder().with_overall("all", 300).build_with_overall();
        assert_eq!(oh.total(), 300);
        g.join_and_clear();

        h.set_total(200);
        assert_eq!(h.total(), 200);
        h.advance(10);
        h.set_position(20);
        h.set_prefix("pfx");
        h.finish();

        // Disabled handles can still be created explicitly.
        let dh = TrackedHandle::disabled();
        assert_eq!(dh.total(), 0, "disabled handle reports 0 total");
        let dg = ProgressGroup::disabled();
        let dch = dg.add_bar(50, "child");
        assert_eq!(dch.total(), 0);
        // All mutation methods are no-ops on a disabled handle
        dh.advance(10);
        dh.set_total(50);
        dh.set_position(5);
        dh.set_prefix("pfx");
        dh.finish();
        dh.finish_success();
        dh.finish_error();
        dh.finish_and_clear();
        dh.abandon();
    }

    #[test]
    fn handle_disabled_is_noop() {
        let h = TrackedHandle::disabled();
        assert_eq!(h.total(), 0);
        h.advance(10);
        h.set_total(50);
        h.set_position(5);
        h.set_prefix("pfx");
        h.finish();
        h.finish_success();
        h.finish_error();
        h.finish_and_clear();
        h.abandon();
    }

    // ---- RecordingProgressTracker ---------------------------------------

    #[test]
    fn recording_tracker_add_bar_creates_op() {
        let rt = RecordingProgressTracker::new();
        let _h = rt.add_bar(100, "test-bar");
        assert_eq!(rt.ops(), vec![ProgressOp::AddBar { total: 100, label: "test-bar".into() }]);
    }

    #[test]
    fn recording_tracker_clear_resets_ops() {
        let rt = RecordingProgressTracker::new();
        let _ = rt.add_bar(10, "a");
        let _ = rt.add_bar(20, "b");
        assert_eq!(rt.ops().len(), 2);
        rt.clear();
        assert!(rt.ops().is_empty());
    }

    #[test]
    fn recording_handle_records_all_ops() {
        let h = RecordingTrackedHandle::new(100);
        assert_eq!(h.total(), 100);

        h.set_total(200);
        h.advance(5);
        h.set_position(10);
        h.set_prefix("pfx");
        h.finish();

        assert_eq!(
            h.ops(),
            vec![
                ProgressOp::SetTotal { total: 200 },
                ProgressOp::Advance { delta: 5 },
                ProgressOp::SetPosition { pos: 10 },
                ProgressOp::SetPrefix { prefix: "pfx".into() },
                ProgressOp::Finish,
            ]
        );
    }

    #[test]
    fn recording_handle_shared_log() {
        let rt = RecordingProgressTracker::new();
        let h1 = rt.add_bar(50, "first");
        let h2 = rt.add_bar(100, "second");

        h1.advance(1);
        h2.advance(2);
        h1.finish();

        assert_eq!(
            rt.ops(),
            vec![
                ProgressOp::AddBar { total: 50, label: "first".into() },
                ProgressOp::AddBar { total: 100, label: "second".into() },
                ProgressOp::Advance { delta: 1 },
                ProgressOp::Advance { delta: 2 },
                ProgressOp::Finish,
            ]
        );
    }

    #[test]
    fn recording_handle_disabled_has_zero_total() {
        let h = RecordingTrackedHandle::disabled();
        assert_eq!(h.total(), 0);
        // Even a disabled handle records ops (it uses a fresh log).
        assert!(h.ops().is_empty());
        h.advance(1);
        assert_eq!(h.ops(), vec![ProgressOp::Advance { delta: 1 }]);
    }

    #[test]
    fn recording_handle_finish_success_and_error() {
        let h = RecordingTrackedHandle::new(10);
        h.finish_success();
        h.finish_error();
        assert_eq!(h.ops(), vec![ProgressOp::FinishSuccess, ProgressOp::FinishError,]);
    }

    #[test]
    fn recording_handle_finish_and_clear_abandon() {
        let h = RecordingTrackedHandle::new(1);
        h.finish_and_clear();
        h.abandon();
        assert_eq!(h.ops(), vec![ProgressOp::FinishAndClear, ProgressOp::Abandon]);
    }

    // ---- RecordingTrackedHandle elapsed ----------------------------------

    #[test]
    fn recording_handle_elapsed_starts_near_zero() {
        let h = RecordingTrackedHandle::new(100);
        let elapsed = h.snapshot_elapsed();
        assert!(elapsed.as_millis() < 100, "elapsed should start near zero, got {elapsed:?}");
    }

    #[test]
    fn recording_handle_elapsed_frozen_after_finish() {
        let h = RecordingTrackedHandle::new(100);
        std::thread::sleep(std::time::Duration::from_millis(10));
        h.finish();
        let frozen = h.snapshot_elapsed();
        assert!(
            frozen.as_millis() >= 5,
            "elapsed should capture time until finish, got {frozen:?}"
        );
        // Verify the value stays frozen on subsequent reads.
        let frozen2 = h.snapshot_elapsed();
        assert_eq!(frozen, frozen2, "elapsed should be frozen after finish");
    }

    #[test]
    fn recording_handle_elapsed_frozen_after_finish_success() {
        let h = RecordingTrackedHandle::new(100);
        std::thread::sleep(std::time::Duration::from_millis(10));
        h.finish_success();
        let frozen = h.snapshot_elapsed();
        assert!(
            frozen.as_millis() >= 5,
            "elapsed should capture time until finish_success, got {frozen:?}"
        );
        let frozen2 = h.snapshot_elapsed();
        assert_eq!(frozen, frozen2, "elapsed should be frozen after finish_success");
    }

    #[test]
    fn recording_handle_elapsed_frozen_after_finish_error() {
        let h = RecordingTrackedHandle::new(100);
        std::thread::sleep(std::time::Duration::from_millis(10));
        h.finish_error();
        let frozen = h.snapshot_elapsed();
        assert!(
            frozen.as_millis() >= 5,
            "elapsed should capture time until finish_error, got {frozen:?}"
        );
        let frozen2 = h.snapshot_elapsed();
        assert_eq!(frozen, frozen2, "elapsed should be frozen after finish_error");
    }

    #[test]
    fn recording_handle_elapsed_frozen_after_abandon() {
        let h = RecordingTrackedHandle::new(100);
        std::thread::sleep(std::time::Duration::from_millis(10));
        h.abandon();
        let frozen = h.snapshot_elapsed();
        assert!(
            frozen.as_millis() >= 5,
            "elapsed should capture time until abandon, got {frozen:?}"
        );
        let frozen2 = h.snapshot_elapsed();
        assert_eq!(frozen, frozen2, "elapsed should be frozen after abandon");
    }

    // ---- format_elapsed (pure formatting) --------------------------------

    #[test]
    fn format_elapsed_zero() {
        assert_eq!(super::format_elapsed(std::time::Duration::ZERO), "0s");
    }

    #[test]
    fn format_elapsed_seconds_only() {
        assert_eq!(super::format_elapsed(std::time::Duration::from_secs(42)), "42s");
    }

    #[test]
    fn format_elapsed_minutes_and_seconds() {
        assert_eq!(super::format_elapsed(std::time::Duration::from_secs(5 * 60 + 3)), "5m3s");
    }

    #[test]
    fn format_elapsed_hours() {
        assert_eq!(
            super::format_elapsed(std::time::Duration::from_secs(2 * 3600 + 15 * 60 + 30)),
            "2h15m"
        );
    }

    #[test]
    fn format_elapsed_large_hours() {
        assert_eq!(super::format_elapsed(std::time::Duration::from_secs(100 * 3600)), "4d4h");
    }

    // ---- format_rate (pure formatting) -----------------------------------

    #[test]
    fn format_rate_zero() {
        assert_eq!(super::format_rate(0.0), "0/d");
    }

    #[test]
    fn format_rate_slow() {
        assert_eq!(super::format_rate(0.000_1), "9/d");
    }

    #[test]
    fn format_rate_per_minute() {
        // 0.02/s = 1.2/m
        assert_eq!(super::format_rate(0.02), "1/m");
    }

    #[test]
    fn format_rate_per_hour() {
        // 0.000_5/s = 1.8/h
        assert_eq!(super::format_rate(0.000_5), "2/h");
    }

    #[test]
    fn format_rate_single_digit() {
        assert_eq!(super::format_rate(3.5), "3.5/s");
    }

    #[test]
    fn format_rate_double_digit() {
        assert_eq!(super::format_rate(42.0), "42/s");
    }

    #[test]
    fn format_rate_thousands_single() {
        assert_eq!(super::format_rate(1_200.0), "1.2k/s");
    }

    #[test]
    fn format_rate_thousands_double() {
        assert_eq!(super::format_rate(123_000.0), "123k/s");
    }

    #[test]
    fn format_rate_millions() {
        assert_eq!(super::format_rate(3_500_000.0), "3.5M/s");
    }

    // ---- SharedState elapsed --------------------------------------------

    #[test]
    fn shared_state_elapsed_starts_near_zero() {
        let s = super::SharedState::new(100, "test");
        let elapsed = s.elapsed();
        assert!(elapsed.as_millis() < 100, "elapsed should start near zero, got {elapsed:?}");
    }

    #[test]
    fn shared_state_elapsed_advances() {
        let s = super::SharedState::new(100, "test");
        std::thread::sleep(std::time::Duration::from_millis(10));
        let elapsed = s.elapsed();
        assert!(elapsed.as_millis() >= 5, "elapsed should advance after sleep, got {elapsed:?}");
    }

    #[test]
    fn shared_state_elapsed_frozen_after_mark_finished() {
        let s = super::SharedState::new(100, "test");
        std::thread::sleep(std::time::Duration::from_millis(10));
        s.mark_finished();
        let frozen = s.elapsed();
        assert!(
            frozen.as_millis() >= 5,
            "elapsed should capture time until mark_finished, got {frozen:?}"
        );
        // Small extra wait to confirm frozen.
        std::thread::sleep(std::time::Duration::from_millis(10));
        let frozen2 = s.elapsed();
        assert_eq!(frozen, frozen2, "elapsed should be frozen after mark_finished");
    }

    #[test]
    fn shared_state_elapsed_not_frozen_before_finish() {
        let s = super::SharedState::new(100, "test");
        let t0 = s.elapsed();
        // Without calling mark_finished, repeated reads should climb.
        let t1 = s.elapsed();
        assert!(t1 >= t0, "elapsed should not decrease before finish: {t0:?} >= {t1:?}");
    }

    #[test]
    fn shared_state_elapsed_monotonic() {
        let s = super::SharedState::new(100, "test");
        let t0 = s.elapsed();
        std::thread::sleep(std::time::Duration::from_millis(5));
        let t1 = s.elapsed();
        std::thread::sleep(std::time::Duration::from_millis(5));
        let t2 = s.elapsed();
        assert!(t1 >= t0, "t1 ({t1:?}) should be >= t0 ({t0:?})");
        assert!(t2 >= t1, "t2 ({t2:?}) should be >= t1 ({t1:?})");
    }

    // ---- TrackedHandle elapsed (integration) -----------------------------

    #[test]
    fn tracked_handle_elapsed_frozen_after_all_finish_methods() {
        // finish, finish_success, finish_error, finish_and_clear, abandon
        // must all freeze the elapsed.
        for (name, finish_fn) in [
            ("finish", Box::new(|h: &TrackedHandle| h.finish()) as Box<dyn Fn(&TrackedHandle)>),
            ("finish_success", Box::new(|h: &TrackedHandle| h.finish_success())),
            ("finish_error", Box::new(|h: &TrackedHandle| h.finish_error())),
            ("finish_and_clear", Box::new(|h: &TrackedHandle| h.finish_and_clear())),
            ("abandon", Box::new(|h: &TrackedHandle| h.abandon())),
        ] {
            let g = ProgressGroup::builder().build();
            let h = g.add_bar(100, &format!("{name}-bar"));
            std::thread::sleep(std::time::Duration::from_millis(10));
            finish_fn(&h);
            // We can't directly access SharedState::elapsed() from the handle,
            // but we verify the handle doesn't panic and is usable afterward.
            assert_eq!(h.total(), 100, "{name}: total preserved");
            g.join_and_clear();
        }
    }

    #[test]
    fn progress_group_new_creates_handle() {
        let g = ProgressGroup::builder().build();
        let h = g.add_bar(42, "child");
        assert!(h.total() > 0, "enabled handle must have total > 0");
        assert_eq!(h.total(), 42);
    }

    #[test]
    fn progress_group_with_overall_creates_both() {
        let (g, overall) = ProgressGroup::builder().with_overall("all", 100).build_with_overall();
        assert_eq!(overall.total(), 100, "overall bar must have total == 100");
        let child = g.add_bar(50, "child");
        assert_eq!(child.total(), 50, "child bar must have total == 50");
    }

    #[test]
    fn recording_handle_set_total_updates_position() {
        let h = RecordingTrackedHandle::new(100);
        h.set_position(5);
        h.set_total(20);
        assert_eq!(
            h.ops(),
            vec![ProgressOp::SetPosition { pos: 5 }, ProgressOp::SetTotal { total: 20 },]
        );
    }

    #[test]
    fn recording_handle_multiple_advances_sum() {
        let h = RecordingTrackedHandle::new(10);
        h.advance(1);
        h.advance(2);
        h.advance(3);
        let ops = h.ops();
        assert_eq!(ops.len(), 3, "expected 3 separate Advance ops");
        assert_eq!(ops[0], ProgressOp::Advance { delta: 1 });
        assert_eq!(ops[1], ProgressOp::Advance { delta: 2 });
        assert_eq!(ops[2], ProgressOp::Advance { delta: 3 });
    }

    #[test]
    fn progress_group_join_and_clear_does_not_panic() {
        // Non-empty group
        let g = ProgressGroup::builder().build();
        let _h = g.add_bar(10, "a");
        g.join();
        g.join_and_clear();

        // Empty group
        let g = ProgressGroup::builder().build();
        g.join();
        g.join_and_clear();
    }

    #[test]
    fn progress_group_disabled_construction() {
        let g1 = ProgressGroup::disabled();
        let h1 = g1.add_bar(50, "c1");
        assert_eq!(h1.total(), 0);

        // disabled + explicit disabled handle pair for with-overall patterns.
        let (_g2, h2) = (ProgressGroup::disabled(), TrackedHandle::disabled());
        assert_eq!(h2.total(), 0);
    }

    #[test]
    fn recording_handle_finish_does_not_generate_clear() {
        // Verify that finish_success / finish_error don't produce
        // FinishAndClear or Abandon operations (which would clear the bar).
        let h = RecordingTrackedHandle::new(10);
        h.finish_success();
        h.finish_error();
        for op in h.ops() {
            match op {
                ProgressOp::FinishSuccess | ProgressOp::FinishError => {}
                other => panic!("unexpected op: {other:?}"),
            }
        }
    }

    #[test]
    fn progress_group_join_leaves_handles_intact() {
        // join() is a no-op — handles must still be usable afterward.
        let g = ProgressGroup::builder().build();
        let h = g.add_bar(42, "child");
        h.advance(10);
        h.set_total(50);
        h.finish_success();
        g.join();
        assert_eq!(h.total(), 50, "handle total preserved after join");
    }

    #[test]
    fn progress_group_finish_success_and_error_preserve_group() {
        // Finish calls on a handle must preserve the total and the group must
        // remain functional (join() must not panic).
        let g = ProgressGroup::builder().build();
        let h = g.add_bar(10, "test");
        h.finish_success();
        assert_eq!(h.total(), 10, "handle total preserved after finish_success");
        // Second finish on the same slot must not corrupt state.
        h.finish_error();
        assert_eq!(h.total(), 10, "handle total preserved after finish_error");
        g.join(); // join must not panic on any state
    }

    // ── recording_group_add_bar_multiple_groups_independent ──

    #[test]
    fn recording_group_add_bar_multiple_groups_independent() {
        let g1 = RecordingProgressTracker::new();
        let g2 = RecordingProgressTracker::new();

        let h1 = g1.add_bar(10, "group1-bar");
        h1.advance(1);

        let h2 = g2.add_bar(20, "group2-bar");
        h2.advance(2);
        h2.finish();

        assert_eq!(
            g1.ops(),
            vec![
                ProgressOp::AddBar { total: 10, label: "group1-bar".to_string() },
                ProgressOp::Advance { delta: 1 },
            ],
            "group1 must have its own ops, unaffected by group2"
        );

        assert_eq!(
            g2.ops(),
            vec![
                ProgressOp::AddBar { total: 20, label: "group2-bar".to_string() },
                ProgressOp::Advance { delta: 2 },
                ProgressOp::Finish,
            ],
            "group2 must have its own ops, unaffected by group1"
        );
    }

    // ── recording_handle_finish_ops_sequence ──

    #[test]
    fn recording_handle_finish_ops_sequence() {
        let h = RecordingTrackedHandle::new(5);
        assert_eq!(h.ops(), vec![], "no ops yet");

        h.finish();
        assert_eq!(h.ops(), vec![ProgressOp::Finish], "finish() records Finish");

        let h2 = RecordingTrackedHandle::new(5);
        h2.finish_success();
        assert_eq!(
            h2.ops(),
            vec![ProgressOp::FinishSuccess],
            "finish_success records FinishSuccess"
        );

        let h3 = RecordingTrackedHandle::new(5);
        h3.finish_error();
        assert_eq!(h3.ops(), vec![ProgressOp::FinishError], "finish_error records FinishError");
    }

    // ── TrackedHandle::new (with bar) ──────────────────────────────────

    #[test]
    fn tracked_handle_new_creates_handle_with_total() {
        let h = TrackedHandle::new(50);
        assert_eq!(h.total(), 50);
        assert_eq!(h.snapshot().position, 0);
        assert!(!h.is_finished());
    }

    #[test]
    fn tracked_handle_new_advance_and_snapshot() {
        let h = TrackedHandle::new(100);
        h.advance(42);
        let snap = h.snapshot();
        assert_eq!(snap.position, 42);
        assert_eq!(snap.total, 100);
    }

    #[test]
    fn tracked_handle_is_finished_after_finish_success() {
        let h = TrackedHandle::new(10);
        assert!(!h.is_finished());
        h.finish_success();
        assert!(h.is_finished());
    }

    #[test]
    fn tracked_handle_is_finished_after_finish_error() {
        let h = TrackedHandle::new(10);
        h.finish_error();
        assert!(h.is_finished());
    }

    #[test]
    fn tracked_handle_is_finished_after_abandon() {
        let h = TrackedHandle::new(10);
        h.abandon();
        assert!(h.is_finished());
    }

    #[test]
    fn tracked_handle_snapshot_fields_match() {
        let h = TrackedHandle::new(100);
        h.set_prefix("pfx");
        h.advance(7);
        let snap = h.snapshot();
        assert_eq!(snap.prefix, "pfx");
        assert_eq!(snap.position, 7);
        assert_eq!(snap.total, 100);
        assert!(matches!(snap.status, TrackStatus::Active));
    }

    #[test]
    fn progress_group_excess_bars_return_active_handles() {
        // Fill slots beyond capacity, verify excess handle still tracks.

        // ProgressGroup::with_overall allocates terminal_height() slots
        // (clamped to 4-200).  Use a MultiProgress with small term to force
        // small capacity.
        let term = indicatif::InMemoryTerm::new(4, 40);
        let target = indicatif::ProgressDrawTarget::term_like(Box::new(term));
        let mp = MultiProgress::with_draw_target(target);
        let (group, _overall) = ProgressGroup::builder()
            .with_multi_progress(mp)
            .capacity(4)
            .with_overall("overall", 10)
            .build_with_overall();

        // 4 slots total → 3 child slots + 1 overall.
        // Add 5 children → first 3 get slots, last 2 have no display slot.
        let handles: Vec<_> = (0..5).map(|i| group.add_bar(10, &format!("t{i}"))).collect();

        // All handles must be active (not disabled).
        for (i, h) in handles.iter().enumerate() {
            assert_eq!(h.total(), 10, "handle {i} total");
        }

        // Mutate each — verify state tracking works even without display.
        for (i, h) in handles.iter().enumerate() {
            h.advance((i + 1) as u64);
        }
        for (i, h) in handles.iter().enumerate() {
            let snap = h.snapshot();
            assert_eq!(snap.position, (i + 1) as u64, "handle {i} position");
        }
    }

    #[test]
    fn progress_group_manager_finish_and_clear_via_tick_fn() {
        // finish_and_clear on a ProgressGroup-managed handle (bar=None,
        // tick_fn=Some) must still mark state as finished.

        let (_group, overall) =
            ProgressGroup::builder().with_overall("all", 10).build_with_overall();
        overall.finish_and_clear();
        let snap = overall.snapshot();
        assert!(
            matches!(snap.status, TrackStatus::Finished),
            "finish_and_clear → Finished, got {:?}",
            snap.status
        );
        assert_eq!(snap.position, 0, "position unchanged before advance");

        // Advance after finish_and_clear is harmless (no crash) but
        // does update position since advance() does not gate on status.
        overall.advance(5);
        assert_eq!(overall.snapshot().position, 5, "advance still works after finish_and_clear");
    }

    #[test]
    fn tracked_handle_finish_and_clear_disabled_is_noop() {
        // disabled() handle with finish_and_clear must not panic and
        // must leave state unchanged.
        let h = TrackedHandle::disabled();
        assert_eq!(h.total(), 0);
        h.finish_and_clear();
        assert_eq!(h.total(), 0);
    }

    #[test]
    fn progress_group_disabled_add_bar_returns_disabled() {
        let g = ProgressGroup::disabled();
        let child = g.add_bar(42, "child");
        assert_eq!(child.total(), 0, "child disabled");
    }

    #[test]
    fn progress_group_api_trait_via_recording() {
        // Verify RecordingProgressTracker implements ProgressGroupApi
        // and can be used via the trait.
        use super::ProgressGroupApi;
        let tracker: Arc<dyn ProgressGroupApi> = Arc::new(RecordingProgressTracker::new());
        let bar: Arc<dyn super::ProgressBarApi> = tracker.add_bar(100, "test");
        assert!(!bar.is_finished(), "recording bar starts unfinished");
        bar.advance(5);
        bar.finish_success();
        assert!(bar.is_finished(), "recording bar is finished");
    }

    #[test]
    fn rate_computation_handles_non_monotonic_position() {
        // When a bar's position regresses between ticks, the EMA rate
        // computation must not panic (saturating_sub guard).
        let term = indicatif::InMemoryTerm::new(10, 80);
        let target = indicatif::ProgressDrawTarget::term_like(Box::new(term.clone()));
        let mp = MultiProgress::with_draw_target(target);
        let group = ProgressGroup::builder().with_multi_progress(mp).capacity(4).build();
        let h = group.add_bar(100, "test");
        h.advance(80); // position grows to 80
        group.tick(); // tick captures prev_position = 80
        h.set_position(20); // position drops to 20 (non-monotonic)
        group.tick(); // must not panic (saturating_sub saves it)
        let snap = h.snapshot();
        assert_eq!(snap.position, 20);
        assert!(matches!(snap.status, TrackStatus::Active));
    }

    // ── Color helpers (ANSI escape code generation) ─────────────────────

    #[test]
    fn bar_color_code_active_child() {
        assert_eq!(super::inner::bar_color_code(super::TrackStatus::Active, false), "33");
    }

    #[test]
    fn bar_color_code_active_overall() {
        assert_eq!(super::inner::bar_color_code(super::TrackStatus::Active, true), "35");
    }

    #[test]
    fn bar_color_code_failed() {
        assert_eq!(super::inner::bar_color_code(super::TrackStatus::Failed, false), "31");
        assert_eq!(super::inner::bar_color_code(super::TrackStatus::Failed, true), "31");
    }

    #[test]
    fn bar_color_code_abandoned() {
        assert_eq!(super::inner::bar_color_code(super::TrackStatus::Abandoned, false), "32");
    }

    #[test]
    fn bar_color_code_success_and_finished() {
        for status in [super::TrackStatus::Success, super::TrackStatus::Finished] {
            assert_eq!(super::inner::bar_color_code(status, false), "32");
        }
    }

    #[test]
    fn build_prefix_failed() {
        let result = super::inner::build_prefix(super::TrackStatus::Failed, "wget");
        assert_eq!(result, "\x1b[0m\x1b[31m[F]\x1b[0m wget");
    }

    #[test]
    fn build_prefix_abandoned() {
        let result = super::inner::build_prefix(super::TrackStatus::Abandoned, "wget");
        assert_eq!(result, "\x1b[0m\x1b[33m[A]\x1b[0m wget");
    }

    #[test]
    fn build_prefix_normal_states() {
        for status in
            [super::TrackStatus::Active, super::TrackStatus::Success, super::TrackStatus::Finished]
        {
            let result = super::inner::build_prefix(status, "child");
            assert_eq!(result, "\x1b[0mchild");
        }
    }

    #[test]
    fn build_prefix_always_starts_with_reset() {
        for status in [
            super::TrackStatus::Active,
            super::TrackStatus::Failed,
            super::TrackStatus::Abandoned,
            super::TrackStatus::Success,
            super::TrackStatus::Finished,
        ] {
            let result = super::inner::build_prefix(status, "foo");
            assert!(
                result.starts_with("\x1b[0m"),
                "{status:?}: expected \\x1b[0m prefix, got {result:?}"
            );
        }
    }

    #[test]
    fn build_right_msg_with_rate() {
        // rate_str present, no eta
        let result = super::inner::build_right_msg("33", "0", "5", "0s", Some("0/d"), None);
        assert_eq!(result, " \x1b[33m0/5\x1b[0m 0s 0/d");
        assert!(result.ends_with("0s 0/d"), "expected elapsed then rate at end: {result:?}");
    }

    #[test]
    fn build_right_msg_with_rate_and_eta() {
        // rate_str + eta_str
        let result = super::inner::build_right_msg("33", "0", "5", "0s", Some("0/d"), Some("5s"));
        assert_eq!(result, " \x1b[33m0/5\x1b[0m 0s 0/d 5s");
        assert!(result.ends_with("0s 0/d 5s"), "expected elapsed rate eta at end: {result:?}");
    }

    #[test]
    fn build_right_msg_different_color_codes() {
        for (code, status_name) in [("31", "failed"), ("33", "child"), ("35", "overall")] {
            let result = super::inner::build_right_msg(code, "1", "2", "3s", Some("0/d"), None);
            assert!(
                result.contains(&format!("\x1b[{code}m")),
                "{status_name} should use code {code}: {result:?}"
            );
            assert!(result.contains("1/2"), "{status_name} count/total absent: {result:?}");
        }
    }
}
