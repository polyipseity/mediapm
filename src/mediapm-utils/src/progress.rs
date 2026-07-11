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
//! | **Tracking** (unlimited) | [`TrackedHandle`], [`ProgressTracker`] | None (pure state) |
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
//! | [`ProgressTracker`] | ❌ | ✅ |
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
// Graphical progress bar types (only with `progress` feature)
// ---------------------------------------------------------------------------

#[cfg(feature = "progress")]
mod inner {
    use std::cell::{Cell, RefCell};
    use std::collections::VecDeque;
    use std::sync::atomic::{AtomicU8, AtomicU64, Ordering};
    use std::sync::{Arc, Mutex, RwLock};
    use std::time::Duration;

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

    const CHILD_BAR_TEMPLATE: &str = "{spinner:.green} {prefix:>16.16} [{elapsed_precise}] {wide_bar:.cyan/blue} {pos}/{len} {msg} ({eta})";

    const OVERALL_BAR_TEMPLATE: &str = "{spinner:.green} {prefix:>16.16} [{elapsed_precise}] {wide_bar:.green/dim} {pos}/{len} {msg}";

    const COMPACT_BAR_TEMPLATE: &str =
        "{spinner:.green} {prefix:>16.16} [{elapsed_precise}] {pos}/{len} {msg}";

    const COMPACT_OVERALL_BAR_TEMPLATE: &str =
        "{spinner:.green} {prefix:>16.16} [{elapsed_precise}] {pos}/{len} {msg}";

    const DONE_BAR_TEMPLATE: &str = "{spinner:.dim} {prefix:>16.16} [{elapsed_precise}] {wide_bar:.white/dim} {pos}/{len} {msg}";

    const COMPACT_DONE_BAR_TEMPLATE: &str =
        "{spinner:.dim} {prefix:>16.16} [{elapsed_precise}] {pos}/{len} {msg}";

    /// Maximum number of pre-allocated slot bars (safety cap).
    const MAX_SLOTS: usize = 200;

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

    fn apply_done_bar_style(pb: &ProgressBar, width: u16) {
        if width < 60 {
            pb.set_style(compact_done_bar_style());
        } else {
            pb.set_style(done_bar_style());
        }
    }

    fn terminal_width() -> u16 {
        console::Term::stderr().size().1
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
        message: RwLock<String>,
        prefix: RwLock<String>,
        status: AtomicU8,
    }

    impl SharedState {
        fn new(total: u64, label: &str) -> Self {
            Self {
                position: AtomicU64::new(0),
                total: AtomicU64::new(total),
                label: RwLock::new(label.to_string()),
                message: RwLock::new(String::new()),
                prefix: RwLock::new(label.to_string()),
                status: AtomicU8::new(0),
            }
        }

        fn snapshot(&self) -> TrackSnapshot {
            TrackSnapshot {
                position: self.position.load(Ordering::Relaxed),
                total: self.total.load(Ordering::Relaxed),
                label: self.label.read().expect("shared_state label lock").clone(),
                message: self.message.read().expect("shared_state message lock").clone(),
                prefix: self.prefix.read().expect("shared_state prefix lock").clone(),
                status: match self.status.load(Ordering::Relaxed) {
                    0 => TrackStatus::Active,
                    1 => TrackStatus::Success,
                    2 => TrackStatus::Failed,
                    3 => TrackStatus::Abandoned,
                    _ => TrackStatus::Finished,
                },
            }
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
        /// Status message (shown after the bar).
        pub message: String,
        /// Prefix (shown before the bar).
        pub prefix: String,
        /// Current status.
        pub status: TrackStatus,
    }

    // ---- TrackedHandle ----------------------------------------------------

    /// Handle to a progress bar with optional display.
    ///
    /// Cloning creates another reference to the same underlying tracking
    /// state — all clones share state and advancing any one of them updates
    /// both the tracking state and the display (when attached to a render
    /// slot).
    ///
    /// To create a no-op handle, use [`TrackedHandle::disabled`].
    /// All mutating methods on a disabled handle are zero-cost and do nothing.
    ///
    /// # Separation of concerns
    ///
    /// [`TrackedHandle`] bundles two independent concerns:
    /// - **Tracking state** (`Arc<SharedState>`): always present, no display
    ///   dependency.  Read via [`snapshot()`](Self::snapshot).
    /// - **Display bar** (`Option<ProgressBar>`): present only when attached
    ///   to a render slot.  All mutating methods perform dual-write: they
    ///   update tracking state unconditionally and forward to the display
    ///   bar when one exists.
    #[derive(Clone)]
    pub struct TrackedHandle {
        pub(crate) state: Arc<SharedState>,
        pub(crate) bar: Option<ProgressBar>,
        /// Tick callback for renderer-managed bars.
        ///
        /// When `bar` is `None` and this is `Some`, mutating methods call
        /// `tick()` on the owning renderer instead of writing directly to a
        /// bar.  This keeps the handle functional after slot swaps that
        /// would invalidate a cached bar reference.
        pub(crate) tick_fn: Option<Arc<dyn Fn() + Send + Sync>>,
    }

    impl TrackedHandle {
        /// Create a no-op handle (all methods are zero-cost).
        #[must_use]
        pub fn disabled() -> Self {
            Self { state: Arc::new(SharedState::new(0, "")), bar: None, tick_fn: None }
        }

        /// Create a standalone progress bar (not managed by a [`ProgressGroup`]).
        #[must_use]
        pub fn new(total: u64) -> Self {
            let state = Arc::new(SharedState::new(total, ""));
            let pb = ProgressBar::new(total);
            apply_bar_style(&pb, terminal_width());
            pb.enable_steady_tick(Duration::from_millis(100));
            Self { state, bar: Some(pb), tick_fn: None }
        }

        /// Return the total number of work units (0 = indeterminate).
        #[must_use]
        pub fn total(&self) -> u64 {
            self.state.total.load(Ordering::Relaxed)
        }

        /// Change the total mid-flight for dynamic workloads.
        pub fn set_total(&self, total: u64) {
            self.state.total.store(total, Ordering::Relaxed);
            if let Some(ref bar) = self.bar {
                bar.set_length(total);
            } else if let Some(ref tick) = self.tick_fn {
                tick();
            }
        }

        /// Advance the bar by `delta` work units.
        pub fn advance(&self, delta: u64) {
            self.state.position.fetch_add(delta, Ordering::Relaxed);
            if let Some(ref bar) = self.bar {
                bar.inc(delta);
            } else if let Some(ref tick) = self.tick_fn {
                tick();
            }
        }

        /// Jump to an absolute position.
        pub fn set_position(&self, pos: u64) {
            self.state.position.store(pos, Ordering::Relaxed);
            if let Some(ref bar) = self.bar {
                bar.set_position(pos);
            } else if let Some(ref tick) = self.tick_fn {
                tick();
            }
        }

        /// Set the message shown after the bar (e.g. "materializing").
        ///
        /// # Panics
        ///
        /// Panics if the shared-state `RwLock` is poisoned.
        pub fn set_message(&self, msg: impl Into<String>) {
            let msg: String = msg.into();
            (*self.state.message.write().expect("shared_state message lock")).clone_from(&msg);
            if let Some(ref bar) = self.bar {
                bar.set_message(msg);
            } else if let Some(ref tick) = self.tick_fn {
                tick();
            }
        }

        /// Set the prefix shown before the bar.
        ///
        /// # Panics
        ///
        /// Panics if the shared-state `RwLock` is poisoned.
        pub fn set_prefix(&self, prefix: impl Into<String>) {
            let prefix: String = prefix.into();
            (*self.state.prefix.write().expect("shared_state prefix lock")).clone_from(&prefix);
            if let Some(ref bar) = self.bar {
                bar.set_prefix(prefix);
            } else if let Some(ref tick) = self.tick_fn {
                tick();
            }
        }

        /// Mark the bar as finished (keeps it visible).
        pub fn finish(&self) {
            self.state.status.store(4, Ordering::Relaxed); // Finished
            if let Some(ref bar) = self.bar {
                bar.disable_steady_tick();
                bar.finish();
            } else if let Some(ref tick) = self.tick_fn {
                tick();
            }
        }

        /// Mark as finished with a success message (keeps it visible).
        ///
        /// # Panics
        ///
        /// Panics if the shared-state `RwLock` is poisoned.
        pub fn finish_success(&self, msg: impl Into<String>) {
            self.state.status.store(1, Ordering::Relaxed); // Success
            let msg: String = msg.into();
            (*self.state.message.write().expect("shared_state message lock")).clone_from(&msg);
            if let Some(ref bar) = self.bar {
                bar.disable_steady_tick();
                bar.finish_with_message(msg);
            } else if let Some(ref tick) = self.tick_fn {
                tick();
            }
        }

        /// Mark as finished with an error message (keeps it visible).
        ///
        /// # Panics
        ///
        /// Panics if the shared-state `RwLock` is poisoned.
        pub fn finish_error(&self, msg: impl Into<String>) {
            self.state.status.store(2, Ordering::Relaxed); // Failed
            let msg: String = msg.into();
            (*self.state.message.write().expect("shared_state message lock")).clone_from(&msg);
            if let Some(ref bar) = self.bar {
                bar.disable_steady_tick();
                bar.abandon_with_message(msg);
            } else if let Some(ref tick) = self.tick_fn {
                tick();
            }
        }

        /// Finish and clear the bar from the display.
        ///
        /// Stops the ticker and marks the bar as hidden. Call this instead of
        /// [`finish`](Self::finish) when the bar should disappear immediately.
        pub fn finish_and_clear(&self) {
            self.state.status.store(5, Ordering::Relaxed); // FinishedAndCleared
            if let Some(ref bar) = self.bar {
                bar.disable_steady_tick();
                bar.finish_and_clear();
            } else if let Some(ref tick) = self.tick_fn {
                tick();
            }
        }

        /// Abandon the bar — leaves it visible but stops all updates.
        pub fn abandon(&self) {
            self.state.status.store(3, Ordering::Relaxed); // Abandoned
            if let Some(ref bar) = self.bar {
                bar.disable_steady_tick();
                bar.abandon();
            } else if let Some(ref tick) = self.tick_fn {
                tick();
            }
        }

        /// Force a redraw (useful in test environments with
        /// [`InMemoryTerm`](indicatif::InMemoryTerm) where steady tick
        /// timers don't fire).
        #[doc(hidden)]
        pub fn tick(&self) {
            if let Some(ref bar) = self.bar {
                let snap = self.state.snapshot();
                bar.set_prefix(snap.prefix);
                bar.set_message(snap.message);
                bar.set_position(snap.position);
                if snap.total > 0 {
                    bar.set_length(snap.total);
                }
                bar.tick();
            } else if let Some(ref tick) = self.tick_fn {
                tick();
            }
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

    // ---- ProgressTracker (pure tracking factory) --------------------------

    /// Pure tracking factory that creates [`TrackedHandle`]s with no display
    /// dependency.
    ///
    /// Every handle returned by [`add_bar`](Self::add_bar) has `bar: None` —
    /// it tracks state but has no visual output.  Use [`ProgressGroup`] when
    /// you need both tracking and display.
    pub struct ProgressTracker;

    impl ProgressTracker {
        /// Create a new tracking factory.
        #[must_use]
        pub fn new() -> Self {
            Self
        }

        /// Add a tracked bar with no display.
        ///
        /// Returns a [`TrackedHandle`] whose methods update shared state but
        /// produce no terminal output.  Use [`snapshot()`](TrackedHandle::snapshot)
        /// to read the state at any point.
        #[must_use]
        pub fn add_bar(&self, total: u64, label: &str) -> TrackedHandle {
            TrackedHandle {
                state: Arc::new(SharedState::new(total, label)),
                bar: None,
                tick_fn: None,
            }
        }
    }

    impl Default for ProgressTracker {
        fn default() -> Self {
            Self::new()
        }
    }

    // ---- ProgressHandle (deprecated alias) --------------------------------

    /// Handle to one progress bar.
    ///
    /// This type is retained as a deprecated alias for backward compatibility.
    /// New code should use [`TrackedHandle`] instead.
    #[derive(Clone)]
    #[deprecated(since = "0.1.0", note = "renamed to TrackedHandle")]
    pub struct ProgressHandle {
        pub(crate) inner: Option<ProgressBar>,
    }

    #[allow(deprecated)]
    impl ProgressHandle {
        /// Create a no-op handle (all methods are zero-cost).
        #[must_use]
        pub fn disabled() -> Self {
            Self { inner: None }
        }

        /// Create a standalone progress bar (not managed by a [`ProgressGroup`]).
        #[must_use]
        pub fn new(total: u64) -> Self {
            let pb = ProgressBar::new(total);
            apply_bar_style(&pb, terminal_width());
            pb.enable_steady_tick(Duration::from_millis(100));
            Self { inner: Some(pb) }
        }

        /// Return the total number of work units (0 = indeterminate).
        #[must_use]
        pub fn total(&self) -> u64 {
            self.inner.as_ref().and_then(ProgressBar::length).unwrap_or(0)
        }

        /// Change the total mid-flight for dynamic workloads.
        pub fn set_total(&self, total: u64) {
            if let Some(ref inner) = self.inner {
                inner.set_length(total);
            }
        }

        /// Advance the bar by `delta` work units.
        pub fn advance(&self, delta: u64) {
            if let Some(ref inner) = self.inner {
                inner.inc(delta);
            }
        }

        /// Jump to an absolute position.
        pub fn set_position(&self, pos: u64) {
            if let Some(ref inner) = self.inner {
                inner.set_position(pos);
            }
        }

        /// Set the message shown after the bar (e.g. "materializing").
        pub fn set_message(&self, msg: impl Into<String>) {
            if let Some(ref inner) = self.inner {
                inner.set_message(msg.into());
            }
        }

        /// Set the prefix shown before the bar.
        pub fn set_prefix(&self, prefix: impl Into<String>) {
            if let Some(ref inner) = self.inner {
                inner.set_prefix(prefix.into());
            }
        }

        /// Mark the bar as finished (keeps it visible).
        pub fn finish(&self) {
            if let Some(ref inner) = self.inner {
                inner.disable_steady_tick();
                inner.finish();
            }
        }

        /// Mark as finished with a success message (keeps it visible).
        pub fn finish_success(&self, msg: impl Into<String>) {
            if let Some(ref inner) = self.inner {
                inner.disable_steady_tick();
                inner.finish_with_message(msg.into());
            }
        }

        /// Mark as finished with an error message (keeps it visible).
        pub fn finish_error(&self, msg: impl Into<String>) {
            if let Some(ref inner) = self.inner {
                inner.disable_steady_tick();
                inner.abandon_with_message(msg.into());
            }
        }

        /// Finish and clear the bar from the display.
        ///
        /// Stops the ticker and marks the bar as hidden. Call this instead of
        /// [`finish`](Self::finish) when the bar should disappear immediately.
        pub fn finish_and_clear(&self) {
            if let Some(ref inner) = self.inner {
                inner.disable_steady_tick();
                inner.finish_and_clear();
            }
        }

        /// Abandon the bar — leaves it visible but stops all updates.
        pub fn abandon(&self) {
            if let Some(ref inner) = self.inner {
                inner.disable_steady_tick();
                inner.abandon();
            }
        }

        /// Force a redraw (useful in test environments with
        /// [`InMemoryTerm`](indicatif::InMemoryTerm) where steady tick
        /// timers don't fire).
        #[doc(hidden)]
        pub fn tick(&self) {
            if let Some(ref inner) = self.inner {
                inner.tick();
            }
        }
    }

    // ---- ProgressRenderer + ProgressGroup (rendering + combined) ----------

    /// A single slot in the renderer's fixed-size grid.
    struct RenderedSlot {
        /// The indicatif [`ProgressBar`] that draws to the terminal.
        bar: ProgressBar,
        /// Optional tracking state this slot is currently bound to.
        /// `None` means the slot is blank (unused).
        source: RefCell<Option<Arc<SharedState>>>,
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
    }

    impl ProgressRenderer {
        /// Pre-allocate blank bars in a fresh [`MultiProgress`] with capacity
        /// derived from terminal height.
        fn new(dim_source: Arc<dyn DimensionSource>) -> Self {
            let (rows, _) = dim_source.dimensions();
            let cap = (rows as usize).clamp(1, MAX_SLOTS);
            let mut renderer = Self::from_mp(MultiProgress::new(), cap, dim_source);
            renderer.dynamic_height = true;
            renderer
        }

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
                slots.push(RenderedSlot { bar, source: RefCell::new(None) });
            }
            // Trigger a final draw so all bars are captured by InMemoryTerm
            // even when capacity == terminal height.
            if let Some(slot) = slots.last() {
                slot.bar.tick();
            }
            Self {
                inner: mp,
                slots,
                has_overall: false,
                dim_source,
                last_width: None,
                dynamic_height: false,
                orphaned_states: RefCell::new(VecDeque::new()),
                finalized: Cell::new(false),
            }
        }

        /// Pre-allocate bars with an overall bar at the bottom, using a fresh
        /// [`MultiProgress`] with capacity derived from terminal height.
        /// Returns `(renderer, overall_state)`.
        fn with_overall(
            total: u64,
            label: &str,
            dim_source: Arc<dyn DimensionSource>,
        ) -> (Self, Arc<SharedState>) {
            let (rows, _) = dim_source.dimensions();
            let cap = (rows as usize).clamp(1, MAX_SLOTS);
            let (mut renderer, state) =
                Self::from_mp_with_overall(MultiProgress::new(), cap, total, label, dim_source);
            renderer.dynamic_height = true;
            (renderer, state)
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
                slots.push(RenderedSlot { bar, source: RefCell::new(None) });
            }
            // Last slot = overall bar.
            let overall_state = Arc::new(SharedState::new(total, label));
            let inner = ProgressBar::new(total);
            let overall_bar = mp.add(inner);
            let (_, cols) = dim_source.dimensions();
            apply_overall_bar_style(&overall_bar, cols);
            overall_bar.set_prefix(label.to_string());
            overall_bar.enable_steady_tick(Duration::from_millis(100));
            slots.push(RenderedSlot {
                bar: overall_bar,
                source: RefCell::new(Some(overall_state.clone())),
            });
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
                },
                overall_state,
            )
        }

        /// Swap the tracked sources of two render slots without
        /// simultaneous `RefCell` borrows.
        #[inline]
        fn swap_slot_sources(slots: &[RenderedSlot], i: usize, j: usize) {
            if i == j {
                return;
            }
            let mut tmp = None;
            {
                let mut cell = slots[i].source.borrow_mut();
                std::mem::swap(&mut tmp, &mut *cell);
            }
            {
                let mut cell = slots[j].source.borrow_mut();
                std::mem::swap(&mut tmp, &mut *cell);
            }
            {
                let mut cell = slots[i].source.borrow_mut();
                std::mem::swap(&mut tmp, &mut *cell);
            }
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
                } else if snap.status != TrackStatus::Active {
                    apply_done_bar_style(&slot.bar, cols);
                } else {
                    apply_bar_style(&slot.bar, cols);
                }
                slot.bar.set_prefix(snap.prefix);
                slot.bar.set_message(snap.message);
                slot.bar.set_length(snap.total);
                slot.bar.enable_steady_tick(Duration::from_millis(100));
            } else {
                slot.bar.set_style(blank_bar_style());
                slot.bar.set_message(" ");
                slot.bar.set_prefix("");
                slot.bar.disable_steady_tick();
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
        fn attach(&self, state: &Arc<SharedState>) {
            let child_cap = self.slots.len() - usize::from(self.has_overall);
            let bottom = child_cap.saturating_sub(1);

            // Phase 1: shift active band up, place new child at bottom
            let active =
                self.slots[..=bottom].iter().filter(|s| s.source.borrow().is_some()).count();

            if active < child_cap {
                // Shift existing active children up by one slot (ascending
                // order preserves relative positions).
                for i in (bottom + 1 - active)..=bottom {
                    Self::swap_slot_sources(&self.slots, i, i - 1);
                }
                // Sync shifted slots (sources moved to different bars).
                for i in (bottom.saturating_sub(active))..=bottom {
                    self.sync_slot(i);
                    self.slots[i].bar.reset_elapsed();
                }
                // Place new child at the freed bottom slot.
                self.slots[bottom].source.replace(Some(Arc::clone(state)));
                self.sync_slot(bottom);
                self.slots[bottom].bar.reset_elapsed();
                return;
            }

            // Phase 2: recycle a finished slot (all slots occupied).
            for i in (0..=bottom).rev() {
                if self.slots[i].source.borrow().as_ref().is_none_or(|s| s.is_finished()) {
                    self.slots[i].source.replace(Some(Arc::clone(state)));
                    self.sync_slot(i);
                    self.slots[i].bar.reset_elapsed();
                    return;
                }
            }
            // Phase 3: no free slot — push to orphaned queue.
            self.orphaned_states.borrow_mut().push_back(Arc::clone(state));
        }

        /// Fast tick for child bars — no extra redraw for active bars.
        ///
        /// When this renderer has an overall bar, resize reactivity is
        /// skipped since the overall bar's [`tick`](Self::tick) handles it.
        /// Without an overall bar, resize is checked so that child-driven
        /// groups still respond to terminal dimension changes.
        fn tick_light(&mut self) {
            if !self.has_overall {
                // Child-driven groups (no overall bar) still need resize
                // reactivity since no other tick path triggers it.
                self.maybe_adjust_for_resize();
            }
            let (_, cols) = self.dim_source.dimensions();
            for slot in &self.slots {
                if let Some(ref source) = *slot.source.borrow() {
                    let snap = source.snapshot();
                    slot.bar.set_position(snap.position);
                    slot.bar.set_length(snap.total);
                    slot.bar.set_message(snap.message.clone());
                    slot.bar.set_prefix(snap.prefix.clone());
                    if snap.status == TrackStatus::Active {
                        // Setters above already triggered a redraw via
                        // update_estimate_and_draw; no explicit tick() needed.
                    } else if source.is_cleared() {
                        slot.bar.set_style(blank_bar_style());
                        slot.bar.set_message(" ");
                        slot.bar.set_prefix("");
                        slot.bar.disable_steady_tick();
                    } else {
                        slot.bar.disable_steady_tick();
                        apply_done_bar_style(&slot.bar, cols);
                        match snap.status {
                            TrackStatus::Success => {
                                slot.bar.finish_with_message(snap.message.clone());
                            }
                            TrackStatus::Failed => {
                                slot.bar.abandon_with_message(snap.message.clone());
                            }
                            TrackStatus::Abandoned => {
                                slot.bar.abandon();
                            }
                            _ => {
                                slot.bar.finish();
                            }
                        }
                    }
                }
            }
        }

        /// Defensive sync: refresh all render slots from their tracked sources.
        ///
        /// Called on the overall bar's tick.  Includes resize reactivity and
        /// full style re-application.  For lighter per-child updates see
        /// [`tick_light`](Self::tick_light).
        pub fn tick(&mut self) {
            self.maybe_adjust_for_resize();
            let (_, cols) = self.dim_source.dimensions();
            for (i, slot) in self.slots.iter().enumerate() {
                if let Some(ref source) = *slot.source.borrow() {
                    let snap = source.snapshot();
                    slot.bar.set_position(snap.position);
                    slot.bar.set_length(snap.total);
                    slot.bar.set_message(snap.message.clone());
                    slot.bar.set_prefix(snap.prefix.clone());
                    if snap.status == TrackStatus::Active {
                        // Setters above already triggered a redraw; no tick() needed.
                    } else if source.is_cleared() {
                        slot.bar.set_style(blank_bar_style());
                        slot.bar.set_message(" ");
                        slot.bar.set_prefix("");
                        slot.bar.disable_steady_tick();
                    } else {
                        slot.bar.disable_steady_tick();
                        if self.has_overall && i == self.slots.len() - 1 {
                            apply_overall_bar_style(&slot.bar, cols);
                        } else {
                            apply_done_bar_style(&slot.bar, cols);
                        }
                        match snap.status {
                            TrackStatus::Success => {
                                slot.bar.finish_with_message(snap.message.clone());
                            }
                            TrackStatus::Failed => {
                                slot.bar.abandon_with_message(snap.message.clone());
                            }
                            TrackStatus::Abandoned => {
                                slot.bar.abandon();
                            }
                            _ => {
                                slot.bar.finish();
                            }
                        }
                    }
                }
            }
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
                        let slot = RenderedSlot { bar, source: RefCell::new(None) };
                        if let Some(orphan) = self.orphaned_states.borrow_mut().pop_back() {
                            slot.source.replace(Some(orphan));
                        }
                        self.slots.insert(0, slot);
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
    }

    impl ProgressGroup {
        /// Create a no-op group that produces no terminal output.
        ///
        /// All bars added via [`add_bar`] return [`TrackedHandle::disabled`].
        /// Useful in tests where progress is not needed.
        #[must_use]
        pub fn disabled() -> Self {
            Self { renderer: None }
        }

        /// Create a new group with no overall bar.
        ///
        /// Pre-allocates `max(terminal_height(), 1)` bars so the draw height
        /// matches the terminal.
        #[must_use]
        pub fn new() -> Self {
            Self {
                renderer: Some(Arc::new(Mutex::new(ProgressRenderer::new(Arc::new(
                    RealTerminalSource,
                ))))),
            }
        }

        /// Create a new group with an injectable dimension source
        /// (for tests using [`InMemoryTerm`](indicatif::InMemoryTerm)).
        #[must_use]
        pub fn new_with_dim(dim_source: Arc<dyn DimensionSource>) -> Self {
            Self { renderer: Some(Arc::new(Mutex::new(ProgressRenderer::new(dim_source)))) }
        }

        /// Create a group from an existing [`MultiProgress`] with a fixed
        /// number of pre-allocated slots.
        ///
        /// The `capacity` controls how many [`ProgressBar`] slots are
        /// pre-allocated.  All slots are initially blank.  Callers that want
        /// terminal-aware sizing should use [`new()`](Self::new) instead.
        #[must_use]
        pub fn with_mp(mp: MultiProgress, capacity: usize) -> Self {
            let cap = capacity.clamp(1, MAX_SLOTS);
            Self {
                renderer: Some(Arc::new(Mutex::new(ProgressRenderer::from_mp(
                    mp,
                    cap,
                    Arc::new(RealTerminalSource),
                )))),
            }
        }

        /// Create a group with an overall aggregate bar pinned at the bottom.
        ///
        /// The overall bar shares the same [`{spinner}`] prefix as child bars
        /// so all bars are visually aligned horizontally.
        ///
        /// Pre-allocates `max(terminal_height(), 1)` bars.  The overall bar
        /// occupies the bottom slot; children fill slots sequentially from
        /// the first upward when [`add_bar`] is called.
        ///
        /// # Panics
        ///
        /// Panics when the internal `Mutex` is poisoned (another thread
        /// panicked while holding the lock).
        #[must_use]
        pub fn with_overall(label: &str, total: u64) -> (Self, TrackedHandle) {
            let (renderer, state) =
                ProgressRenderer::with_overall(total, label, Arc::new(RealTerminalSource));
            let renderer = Arc::new(Mutex::new(renderer));
            let weak = Arc::downgrade(&renderer);
            let tick_fn: Option<Arc<dyn Fn() + Send + Sync>> = Some(Arc::new(move || {
                if let Some(r) = weak.upgrade() {
                    r.lock().unwrap().tick();
                }
            }));
            let handle = TrackedHandle { state, bar: None, tick_fn };
            (Self { renderer: Some(renderer) }, handle)
        }

        /// Create a group with an overall bar and an injectable dimension
        /// source (for tests using [`InMemoryTerm`](indicatif::InMemoryTerm)).
        ///
        /// # Panics
        ///
        /// Panics when the internal `Mutex` is poisoned (another thread
        /// panicked while holding the lock).
        #[must_use]
        pub fn with_overall_and_dim(
            label: &str,
            total: u64,
            dim_source: Arc<dyn DimensionSource>,
        ) -> (Self, TrackedHandle) {
            let (renderer, state) = ProgressRenderer::with_overall(total, label, dim_source);
            let renderer = Arc::new(Mutex::new(renderer));
            let weak = Arc::downgrade(&renderer);
            let tick_fn: Option<Arc<dyn Fn() + Send + Sync>> = Some(Arc::new(move || {
                if let Some(r) = weak.upgrade() {
                    r.lock().unwrap().tick();
                }
            }));
            let handle = TrackedHandle { state, bar: None, tick_fn };
            (Self { renderer: Some(renderer) }, handle)
        }

        /// Create a group from an existing [`MultiProgress`] with an overall
        /// aggregate bar pinned at the bottom and a fixed number of
        /// pre-allocated slots.
        ///
        /// The `capacity` controls how many [`ProgressBar`] slots are
        /// pre-allocated (including the overall bar itself).  See
        /// [`with_overall`](Self::with_overall) for semantics.
        ///
        /// # Panics
        ///
        /// Panics when the internal `Mutex` is poisoned (another thread
        /// panicked while holding the lock).
        #[must_use]
        pub fn with_mp_and_overall(
            mp: MultiProgress,
            capacity: usize,
            label: &str,
            total: u64,
        ) -> (Self, TrackedHandle) {
            let cap = capacity.clamp(1, MAX_SLOTS);
            let (renderer, state) = ProgressRenderer::from_mp_with_overall(
                mp,
                cap,
                total,
                label,
                Arc::new(RealTerminalSource),
            );
            let renderer = Arc::new(Mutex::new(renderer));
            let weak = Arc::downgrade(&renderer);
            let tick_fn: Option<Arc<dyn Fn() + Send + Sync>> = Some(Arc::new(move || {
                if let Some(r) = weak.upgrade() {
                    r.lock().unwrap().tick();
                }
            }));
            let handle = TrackedHandle { state, bar: None, tick_fn };
            (Self { renderer: Some(renderer) }, handle)
        }

        /// Create a group from an existing [`MultiProgress`] with a fixed
        /// number of pre-allocated slots and an injectable dimension source.
        ///
        /// Useful in tests that use [`InMemoryTerm`](indicatif::InMemoryTerm)
        /// — pass a [`TestDimensionSource`] configured to match the virtual
        /// terminal size so resize reactivity uses the right dimensions.
        ///
        /// Set `dynamic_height` to `true` when the slot count should be
        /// adjusted on terminal height changes (used by auto-sized groups).
        #[must_use]
        pub fn with_mp_and_dim(
            mp: MultiProgress,
            capacity: usize,
            dim_source: Arc<dyn DimensionSource>,
            dynamic_height: bool,
        ) -> Self {
            let cap = capacity.clamp(1, MAX_SLOTS);
            let mut renderer = ProgressRenderer::from_mp(mp, cap, dim_source);
            renderer.dynamic_height = dynamic_height;
            Self { renderer: Some(Arc::new(Mutex::new(renderer))) }
        }

        /// Create a group with an overall aggregate bar pinned at the bottom,
        /// from an existing [`MultiProgress`] with a fixed number of
        /// pre-allocated slots and an injectable dimension source.
        ///
        /// See [`with_mp_and_dim`](Self::with_mp_and_dim) for motivation.
        ///
        /// # Panics
        ///
        /// Panics when the internal `Mutex` is poisoned (another thread
        /// panicked while holding the lock).
        #[must_use]
        pub fn with_mp_and_overall_and_dim(
            mp: MultiProgress,
            capacity: usize,
            label: &str,
            total: u64,
            dim_source: Arc<dyn DimensionSource>,
            dynamic_height: bool,
        ) -> (Self, TrackedHandle) {
            let cap = capacity.clamp(1, MAX_SLOTS);
            let (mut renderer, state) =
                ProgressRenderer::from_mp_with_overall(mp, cap, total, label, dim_source);
            renderer.dynamic_height = dynamic_height;
            let renderer = Arc::new(Mutex::new(renderer));
            let weak = Arc::downgrade(&renderer);
            let tick_fn: Option<Arc<dyn Fn() + Send + Sync>> = Some(Arc::new(move || {
                if let Some(r) = weak.upgrade() {
                    r.lock().unwrap().tick();
                }
            }));
            let handle = TrackedHandle { state, bar: None, tick_fn };
            (Self { renderer: Some(renderer) }, handle)
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
                let locked = renderer.lock().unwrap();
                locked.attach(&state);
            }
            let weak = Arc::downgrade(renderer);
            let tick_fn: Option<Arc<dyn Fn() + Send + Sync>> = Some(Arc::new(move || {
                if let Some(r) = weak.upgrade() {
                    r.lock().unwrap().tick_light();
                }
            }));
            TrackedHandle { state, bar: None, tick_fn }
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
                renderer.lock().unwrap().finalize();
            }
        }
    }

    impl Default for ProgressGroup {
        fn default() -> Self {
            Self::new()
        }
    }

    impl Drop for ProgressGroup {
        fn drop(&mut self) {
            if let Some(ref renderer) = self.renderer {
                renderer.lock().unwrap().finalize();
            }
        }
    }
}

#[cfg(feature = "progress")]
#[allow(deprecated)]
pub use inner::{
    DimensionSource, ProgressGroup, ProgressHandle, ProgressRenderer, ProgressTracker,
    RealTerminalSource, TestDimensionSource, TrackSnapshot, TrackStatus, TrackedHandle,
};

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
    /// Mark as finished with a success message.
    fn finish_success(&self, msg: &str);
    /// Mark as finished with an error message.
    fn finish_error(&self, msg: &str);
    /// Return a data-copy snapshot of the tracking state.
    fn snapshot(&self) -> TrackSnapshot;
    /// Returns `true` if the handle has been finished/abandoned.
    fn is_finished(&self) -> bool;
    /// Mark the bar as finished (keeps it visible).
    fn finish(&self);
    /// Set the message shown after the bar.
    fn set_message(&self, msg: &str);
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
    fn finish_success(&self, msg: &str) {
        TrackedHandle::finish_success(self, msg);
    }
    fn finish_error(&self, msg: &str) {
        TrackedHandle::finish_error(self, msg);
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
    fn set_message(&self, msg: &str) {
        TrackedHandle::set_message(self, msg);
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
        /// `set_message(msg)` was called.
        SetMessage {
            /// Message text.
            msg: String,
        },
        /// `set_prefix(prefix)` was called.
        SetPrefix {
            /// Prefix text.
            prefix: String,
        },
        /// `finish()` was called.
        Finish,
        /// `finish_success(msg)` was called.
        FinishSuccess {
            /// Success message.
            msg: String,
        },
        /// `finish_error(msg)` was called.
        FinishError {
            /// Error message.
            msg: String,
        },
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
            RecordingTrackedHandle { ops: self.ops.clone(), total: Some(total) }
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
    }

    impl RecordingTrackedHandle {
        /// Create a standalone recording handle (not managed by a tracker).
        ///
        /// The handle has its own private operation log.
        #[must_use]
        pub fn new(total: u64) -> Self {
            Self { ops: Arc::new(Mutex::new(Vec::new())), total: Some(total) }
        }

        /// Create a disabled (no-op) recording handle.
        ///
        /// All methods are no-ops; the handle logs nothing and reports
        /// [`total`](RecordingTrackedHandle::total) as 0.
        #[must_use]
        pub fn disabled() -> Self {
            Self { ops: Arc::new(Mutex::new(Vec::new())), total: None }
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

        /// Set the message.
        pub fn set_message(&self, msg: impl Into<String>) {
            self.ops
                .lock()
                .expect("recording lock")
                .push(ProgressOp::SetMessage { msg: msg.into() });
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
        }

        /// Mark as finished with a success message.
        pub fn finish_success(&self, msg: impl Into<String>) {
            self.ops
                .lock()
                .expect("recording lock")
                .push(ProgressOp::FinishSuccess { msg: msg.into() });
        }

        /// Mark as finished with an error message.
        pub fn finish_error(&self, msg: impl Into<String>) {
            self.ops
                .lock()
                .expect("recording lock")
                .push(ProgressOp::FinishError { msg: msg.into() });
        }

        /// Finish and clear from display.
        pub fn finish_and_clear(&self) {
            self.ops.lock().expect("recording lock").push(ProgressOp::FinishAndClear);
        }

        /// Abandon — leaves visible but stops updates.
        pub fn abandon(&self) {
            self.ops.lock().expect("recording lock").push(ProgressOp::Abandon);
        }

        /// Return a snapshot of recorded operations for this handle.
        ///
        /// When created via [`RecordingProgressTracker::add_bar`], this
        /// returns the same shared log as all handles from that tracker.
        #[must_use]
        pub fn ops(&self) -> Vec<ProgressOp> {
            self.ops.lock().expect("recording lock").clone()
        }
    }
}

// ---- Trait impls for recording types -------------------------------------

#[cfg(feature = "progress")]
impl ProgressBarApi for recording::RecordingTrackedHandle {
    fn advance(&self, delta: u64) {
        recording::RecordingTrackedHandle::advance(self, delta);
    }
    fn finish_success(&self, msg: &str) {
        recording::RecordingTrackedHandle::finish_success(self, msg);
    }
    fn finish_error(&self, msg: &str) {
        recording::RecordingTrackedHandle::finish_error(self, msg);
    }
    fn snapshot(&self) -> TrackSnapshot {
        // Recording handles don't maintain SharedState, return basic snapshot
        TrackSnapshot {
            position: 0,
            total: self.total(),
            label: String::new(),
            message: String::new(),
            prefix: String::new(),
            status: TrackStatus::Active,
        }
    }
    fn is_finished(&self) -> bool {
        // Check the ops log for any finish-type operation
        let ops = self.ops();
        ops.iter().any(|op| {
            matches!(
                op,
                recording::ProgressOp::Finish
                    | recording::ProgressOp::FinishSuccess { .. }
                    | recording::ProgressOp::FinishError { .. }
                    | recording::ProgressOp::FinishAndClear
                    | recording::ProgressOp::Abandon
            )
        })
    }
    fn finish(&self) {
        recording::RecordingTrackedHandle::finish(self);
    }
    fn set_message(&self, msg: &str) {
        recording::RecordingTrackedHandle::set_message(self, msg);
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
    use std::sync::Arc;

    use super::recording::{ProgressOp, RecordingProgressTracker, RecordingTrackedHandle};
    use super::{ProgressGroup, ProgressTracker, TrackStatus, TrackedHandle};
    use indicatif::MultiProgress;

    #[test]
    fn progress_enabled_no_global_toggle() {
        // Constructors always produce enabled handles.
        let h = TrackedHandle::new(100);
        assert_eq!(h.total(), 100, "enabled handle reports initial total");
        let g = ProgressGroup::new();
        let ch = g.add_bar(50, "child");
        assert_eq!(ch.total(), 50);
        let (_og, oh) = ProgressGroup::with_overall("all", 300);
        assert_eq!(oh.total(), 300);
        g.join_and_clear();

        h.set_total(200);
        assert_eq!(h.total(), 200);
        h.advance(10);
        h.set_position(20);
        h.set_message("msg");
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
        dh.set_message("hi");
        dh.set_prefix("pfx");
        dh.finish();
        dh.finish_success("ok");
        dh.finish_error("err");
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
        h.set_message("hi");
        h.set_prefix("pfx");
        h.finish();
        h.finish_success("ok");
        h.finish_error("err");
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
        h.set_message("hello");
        h.set_prefix("pfx");
        h.finish();

        assert_eq!(
            h.ops(),
            vec![
                ProgressOp::SetTotal { total: 200 },
                ProgressOp::Advance { delta: 5 },
                ProgressOp::SetPosition { pos: 10 },
                ProgressOp::SetMessage { msg: "hello".into() },
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
        h.finish_success("ok!");
        h.finish_error("oh no");
        assert_eq!(
            h.ops(),
            vec![
                ProgressOp::FinishSuccess { msg: "ok!".into() },
                ProgressOp::FinishError { msg: "oh no".into() },
            ]
        );
    }

    #[test]
    fn recording_handle_finish_and_clear_abandon() {
        let h = RecordingTrackedHandle::new(1);
        h.finish_and_clear();
        h.abandon();
        assert_eq!(h.ops(), vec![ProgressOp::FinishAndClear, ProgressOp::Abandon]);
    }

    #[test]
    fn progress_group_new_creates_handle() {
        let g = ProgressGroup::new();
        let h = g.add_bar(42, "child");
        assert!(h.total() > 0, "enabled handle must have total > 0");
        assert_eq!(h.total(), 42);
    }

    #[test]
    fn progress_group_with_overall_creates_both() {
        let (g, overall) = ProgressGroup::with_overall("all", 100);
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
        let g = ProgressGroup::new();
        let _h = g.add_bar(10, "a");
        g.join();
        g.join_and_clear();

        // Empty group
        let g = ProgressGroup::new();
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
        h.finish_success("done");
        h.finish_error("fail");
        for op in h.ops() {
            match op {
                ProgressOp::FinishSuccess { .. } | ProgressOp::FinishError { .. } => {}
                other => panic!("unexpected op: {other:?}"),
            }
        }
    }

    #[test]
    fn progress_group_join_leaves_handles_intact() {
        // join() is a no-op — handles must still be usable afterward.
        let g = ProgressGroup::new();
        let h = g.add_bar(42, "child");
        h.advance(10);
        h.set_total(50);
        h.finish_success("done");
        g.join();
        assert_eq!(h.total(), 50, "handle total preserved after join");
    }

    #[test]
    fn progress_group_finish_success_and_error_preserve_group() {
        // Finish calls on a handle must preserve the total and the group must
        // remain functional (join() must not panic).
        let g = ProgressGroup::new();
        let h = g.add_bar(10, "test");
        h.finish_success("completed");
        assert_eq!(h.total(), 10, "handle total preserved after finish_success");
        // Second finish on the same slot must not corrupt state.
        h.finish_error("timed out");
        assert_eq!(h.total(), 10, "handle total preserved after finish_error");
        g.join(); // join must not panic on any state
    }

    // ── recording_handle_set_message_and_prefix_ops ──

    #[test]
    fn recording_handle_set_message_and_prefix_ops() {
        let h = RecordingTrackedHandle::new(10);
        h.set_message("hello");
        h.set_prefix("pfx");
        assert_eq!(
            h.ops(),
            vec![
                ProgressOp::SetMessage { msg: "hello".to_string() },
                ProgressOp::SetPrefix { prefix: "pfx".to_string() },
            ]
        );
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
        h2.finish_success("ok");
        assert_eq!(
            h2.ops(),
            vec![ProgressOp::FinishSuccess { msg: "ok".to_string() }],
            "finish_success records FinishSuccess"
        );

        let h3 = RecordingTrackedHandle::new(5);
        h3.finish_error("fail");
        assert_eq!(
            h3.ops(),
            vec![ProgressOp::FinishError { msg: "fail".to_string() }],
            "finish_error records FinishError"
        );
    }

    // ── Pure tracking: ProgressTracker ─────────────────────────────────

    #[test]
    fn tracker_add_bar_returns_handle_with_total_and_label() {
        let t = ProgressTracker::new();
        let h = t.add_bar(100, "test");
        assert_eq!(h.total(), 100);
        assert_eq!(h.snapshot().label, "test");
        assert_eq!(h.snapshot().total, 100);
        assert_eq!(h.snapshot().position, 0);
        assert!(!h.is_finished());
    }

    #[test]
    fn tracker_handle_advance_updates_position() {
        let t = ProgressTracker::new();
        let h = t.add_bar(100, "test");
        h.advance(10);
        assert_eq!(h.snapshot().position, 10);
        assert!(!h.is_finished());
        h.advance(20);
        assert_eq!(h.snapshot().position, 30);
    }

    #[test]
    fn tracker_handle_finish_success_reflected_in_snapshot() {
        let t = ProgressTracker::new();
        let h = t.add_bar(50, "test");
        h.advance(25);
        h.finish_success("done");
        let snap = h.snapshot();
        assert_eq!(snap.position, 25);
        assert_eq!(snap.total, 50);
        assert!(matches!(snap.status, TrackStatus::Success));
        assert_eq!(snap.message, "done");
        assert!(h.is_finished());
    }

    #[test]
    fn tracker_handle_finish_error_reflected_in_snapshot() {
        let t = ProgressTracker::new();
        let h = t.add_bar(30, "test");
        h.finish_error("error");
        let snap = h.snapshot();
        assert!(matches!(snap.status, TrackStatus::Failed));
        assert_eq!(snap.message, "error");
        assert!(h.is_finished());
    }

    #[test]
    fn tracker_handle_abandon_reflected_in_snapshot() {
        let t = ProgressTracker::new();
        let h = t.add_bar(20, "test");
        h.abandon();
        let snap = h.snapshot();
        assert!(matches!(snap.status, TrackStatus::Abandoned));
        assert!(h.is_finished());
    }

    #[test]
    fn tracker_handle_finish_reflected_in_snapshot() {
        let t = ProgressTracker::new();
        let h = t.add_bar(15, "test");
        h.finish();
        let snap = h.snapshot();
        assert!(matches!(snap.status, TrackStatus::Finished));
        assert!(h.is_finished());
    }

    #[test]
    fn tracker_handle_set_total_dynamic() {
        let t = ProgressTracker::new();
        let h = t.add_bar(100, "test");
        h.set_total(200);
        assert_eq!(h.snapshot().total, 200);
    }

    #[test]
    fn tracker_handle_shared_state_between_clones() {
        let t = ProgressTracker::new();
        let a = t.add_bar(100, "test");
        let b = a.clone();
        a.advance(10);
        assert_eq!(b.snapshot().position, 10, "clone sees original's advance");
        b.set_total(200);
        assert_eq!(a.snapshot().total, 200, "original sees clone's set_total");
        a.finish_success("ok");
        assert!(b.is_finished(), "clone sees original's finish");
    }

    #[test]
    fn tracker_handle_unlimited_bars() {
        let t = ProgressTracker::new();
        let mut handles = Vec::new();
        for i in 0..10_000 {
            let h = t.add_bar(1, &format!("h{i}"));
            handles.push(h);
        }
        // Verify a sample of handles
        assert_eq!(handles[0].total(), 1);
        assert_eq!(handles[5000].total(), 1);
        assert_eq!(handles[9999].total(), 1);
        handles[0].advance(1);
        assert_eq!(handles[0].snapshot().position, 1);
    }

    #[test]
    fn tracker_no_bar_all_methods_work() {
        // ProgressTracker produces handles with bar: None; all methods must
        // work without a display backend.
        let t = ProgressTracker::new();
        let h = t.add_bar(100, "test");
        h.advance(10);
        h.set_total(50);
        h.set_position(5);
        h.set_message("hello");
        h.set_prefix("pfx");
        h.finish_success("done");
        assert_eq!(h.total(), 50);
        assert_eq!(h.snapshot().message, "done");
        assert!(h.is_finished());
        h.tick(); // should not panic
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
        h.finish_success("ok");
        assert!(h.is_finished());
    }

    #[test]
    fn tracked_handle_is_finished_after_finish_error() {
        let h = TrackedHandle::new(10);
        h.finish_error("err");
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
        h.set_message("msg");
        h.advance(7);
        let snap = h.snapshot();
        assert_eq!(snap.prefix, "pfx");
        assert_eq!(snap.message, "msg");
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
        let (group, _overall) = ProgressGroup::with_mp_and_overall(mp, 4, "overall", 10);

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
            h.set_message(&format!("done {i}"));
        }
        for (i, h) in handles.iter().enumerate() {
            let snap = h.snapshot();
            assert_eq!(snap.position, (i + 1) as u64, "handle {i} position");
            assert_eq!(snap.message, format!("done {i}"), "handle {i} message");
        }
    }

    #[test]
    fn progress_group_manager_finish_and_clear_via_tick_fn() {
        // finish_and_clear on a ProgressGroup-managed handle (bar=None,
        // tick_fn=Some) must still mark state as finished.

        let (_group, overall) = ProgressGroup::with_overall("all", 10);
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
        bar.finish_success("ok");
        assert!(bar.is_finished(), "recording bar is finished");
    }
}
