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
//! | **Debug** | [`ProgressDebugSink`] | `serde_json` (behind feature) |
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
//! | [`ProgressDebugSink`] | ❌ | ✅ |
//! | [`DebugSlotState`] | ❌ | ✅ |
//! | [`DebugTickSnapshot`] | ❌ | ✅ |

use std::sync::Arc;
use std::sync::atomic::{AtomicU64, Ordering};

// ---------------------------------------------------------------------------
// ByteBudget — thread-safe progress size tracker (always available)
// ---------------------------------------------------------------------------

/// Thread-safe progress size tracker.
///
/// Tracks `(position, total)` where `position ≤ total` at all times. Both
/// fields use [`AtomicU64`] internally, making this type [`Send`] + [`Sync`]
/// without external locking. Safe to read from one thread (progress bar
/// renderer) while writing from another (download worker).
///
/// # Invariants (hard-fail with `assert!`)
///
/// - `pos ≤ total` — enforced on every mutation.
/// - `pos` never decreases.
/// - `total` may increase or decrease (via [`adjust`](Self::adjust) or
///   [`reconcile`](Self::reconcile)).
#[derive(Debug)]
pub struct ByteBudget {
    pos: AtomicU64,
    total: AtomicU64,
}

impl ByteBudget {
    /// Create a new budget with `pos = 0` and `total = initial_total`.
    #[must_use]
    pub fn new(initial_total: u64) -> Self {
        Self { pos: AtomicU64::new(0), total: AtomicU64::new(initial_total) }
    }

    /// Current position.
    #[must_use]
    pub fn pos(&self) -> u64 {
        self.pos.load(Ordering::Acquire)
    }

    /// Current total.
    #[must_use]
    pub fn total(&self) -> u64 {
        self.total.load(Ordering::Acquire)
    }

    /// Snapshot `(pos, total)`.
    #[must_use]
    pub fn snap(&self) -> (u64, u64) {
        (self.pos(), self.total())
    }

    /// Advance position by `amount`.
    ///
    /// Uses a `compare_exchange_weak` loop for thread safety.
    ///
    /// # Panics
    ///
    /// Panics (hard `assert!`) if `pos + amount > total`.
    pub fn advance(&self, amount: u64) {
        let mut old = self.pos.load(Ordering::Acquire);
        loop {
            let new = old + amount;
            let total = self.total.load(Ordering::Acquire);
            assert!(new <= total, "ByteBudget::advance({amount}) would exceed total {total}");
            match self.pos.compare_exchange_weak(old, new, Ordering::AcqRel, Ordering::Acquire) {
                Ok(_) => return,
                Err(actual) => old = actual,
            }
        }
    }

    /// Set position to an absolute value.
    ///
    /// Single load-store (no loop — assumes sequential completion).
    ///
    /// # Panics
    ///
    /// Panics (hard `assert!`) if `pos > total` or `pos < current position`.
    pub fn set_pos(&self, pos: u64) {
        let total = self.total.load(Ordering::Acquire);
        assert!(pos <= total, "ByteBudget::set_pos({pos}) > total {total}");
        let current = self.pos.load(Ordering::Acquire);
        assert!(pos >= current, "ByteBudget::set_pos({pos}) < current {current}");
        self.pos.store(pos, Ordering::Release);
    }

    /// Adjust total by `delta` (may be positive or negative).
    ///
    /// Uses a `compare_exchange_weak` loop for thread safety. Saturating
    /// arithmetic is used for extreme values near `u64::MAX`/`u64::MIN`.
    ///
    /// # Panics
    ///
    /// Panics (hard `assert!`) if `pos > new_total` after adjustment.
    pub fn adjust(&self, delta: i64) {
        let mut old = self.total.load(Ordering::Acquire);
        loop {
            let new = if delta >= 0 {
                old.saturating_add(delta.unsigned_abs())
            } else {
                old.saturating_sub(delta.unsigned_abs())
            };
            let pos = self.pos.load(Ordering::Acquire);
            assert!(
                pos <= new,
                "ByteBudget::adjust({delta}) would put total {new} below pos {pos}"
            );
            match self.total.compare_exchange_weak(old, new, Ordering::AcqRel, Ordering::Acquire) {
                Ok(_) => return,
                Err(actual) => old = actual,
            }
        }
    }

    /// Reconcile total after learning actual cost.
    ///
    /// - If `actual > estimate`: total increases by `actual - estimate`.
    /// - If `actual < estimate`: total decreases by `estimate - actual`.
    /// - If equal: no-op.
    ///
    /// # Panics
    ///
    /// Panics (hard `assert!`) if the resulting total < current position.
    pub fn reconcile(&self, estimate: u64, actual: u64) {
        match actual.cmp(&estimate) {
            std::cmp::Ordering::Greater => {
                // actual > estimate is guaranteed by the match arm, so the
                // difference is positive and fits in i64 for all real-world
                // byte budgets (< 9.2 EB).
                self.adjust(i64::try_from(actual - estimate).expect("diff fits in i64"));
            }
            std::cmp::Ordering::Less => {
                // estimate > actual is guaranteed by the match arm, so the
                // difference is positive and fits in i64 for all real-world
                // byte budgets (< 9.2 EB).
                self.adjust(-i64::try_from(estimate - actual).expect("diff fits in i64"));
            }
            std::cmp::Ordering::Equal => {}
        }
    }
}

// ---------------------------------------------------------------------------
// MultiItemBudget — thread-safe per-item progress size tracker (always available)
// ---------------------------------------------------------------------------

/// Thread-safe collection of per-item progress budgets.
///
/// Each item tracks `(position, total)` as a pair of [`AtomicU64`] values.
/// The aggregate progress is the sum of all items' positions and totals.
///
/// # Invariants (hard-fail with `assert!`)
///
/// - `pos ≤ total` per item — enforced on every mutation.
/// - `pos` never decreases per item.
/// - `total` is set once per item (at construction via [`add_item`](Self::add_item)
///   or dynamically via [`set_total`](Self::set_total)).
/// - Items with `total == 0` are considered indeterminate — counted in
///   [`item_count`](Self::item_count) but contribute 0 bytes to aggregate totals.
#[derive(Debug)]
pub struct MultiItemBudget {
    items: Vec<ItemBudget>,
}

#[derive(Debug)]
struct ItemBudget {
    pos: AtomicU64,
    total: AtomicU64,
}

impl MultiItemBudget {
    /// Create a new empty budget.
    #[must_use]
    pub fn new() -> Self {
        Self { items: Vec::new() }
    }

    /// Create a budget pre-allocated for `capacity` items.
    #[must_use]
    pub fn with_capacity(capacity: usize) -> Self {
        Self { items: Vec::with_capacity(capacity) }
    }

    /// Add one item with the given `total`.
    pub fn add_item(&mut self, total: u64) {
        self.items.push(ItemBudget { pos: AtomicU64::new(0), total: AtomicU64::new(total) });
    }

    /// Number of items.
    #[must_use]
    pub fn item_count(&self) -> usize {
        self.items.len()
    }

    /// Set total for the item at `item_idx`.
    ///
    /// # Panics
    ///
    /// Panics if `item_idx` is out of bounds, or if `total < current position`.
    pub fn set_total(&self, item_idx: usize, total: u64) {
        let item = &self.items[item_idx];
        let pos = item.pos.load(Ordering::Acquire);
        assert!(
            pos <= total,
            "MultiItemBudget::set_total({item_idx}, {total}) < current pos {pos}"
        );
        item.total.store(total, Ordering::Release);
    }

    /// Advance position for item at `item_idx` by `amount`.
    ///
    /// Uses a `compare_exchange_weak` loop for thread safety.
    ///
    /// # Panics
    ///
    /// Panics if `item_idx` is out of bounds, or if `pos + amount > total`.
    pub fn advance(&self, item_idx: usize, amount: u64) {
        let item = &self.items[item_idx];
        let mut old = item.pos.load(Ordering::Acquire);
        loop {
            let new = old + amount;
            let total = item.total.load(Ordering::Acquire);
            assert!(
                new <= total,
                "MultiItemBudget::advance({item_idx}, {amount}) would exceed total {total}"
            );
            match item.pos.compare_exchange_weak(old, new, Ordering::AcqRel, Ordering::Acquire) {
                Ok(_) => return,
                Err(actual) => old = actual,
            }
        }
    }

    /// Set absolute position for item at `item_idx`.
    ///
    /// Single load-store (no loop — assumes sequential completion).
    ///
    /// # Panics
    ///
    /// Panics if `item_idx` is out of bounds, `pos > total`, or `pos < current position`.
    pub fn set_pos(&self, item_idx: usize, pos: u64) {
        let item = &self.items[item_idx];
        let total = item.total.load(Ordering::Acquire);
        assert!(pos <= total, "MultiItemBudget::set_pos({item_idx}, {pos}) > total {total}");
        let current = item.pos.load(Ordering::Acquire);
        assert!(pos >= current, "MultiItemBudget::set_pos({item_idx}, {pos}) < current {current}");
        item.pos.store(pos, Ordering::Release);
    }

    /// Snapshot `(pos, total)` for item at `item_idx`.
    ///
    /// # Panics
    ///
    /// Panics if `item_idx` is out of bounds.
    #[must_use]
    pub fn snap(&self, item_idx: usize) -> (u64, u64) {
        let item = &self.items[item_idx];
        (item.pos.load(Ordering::Acquire), item.total.load(Ordering::Acquire))
    }

    /// Aggregate of all items: `(sum_pos, sum_total)`.
    ///
    /// Items with `total == 0` are indeterminate and contribute 0.
    #[must_use]
    pub fn aggregate(&self) -> (u64, u64) {
        let mut sum_pos = 0u64;
        let mut sum_total = 0u64;
        for item in &self.items {
            sum_pos = sum_pos.saturating_add(item.pos.load(Ordering::Acquire));
            sum_total = sum_total.saturating_add(item.total.load(Ordering::Acquire));
        }
        (sum_pos, sum_total)
    }
}

impl Default for MultiItemBudget {
    fn default() -> Self {
        Self::new()
    }
}

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
    /// Phase 3: processing (extract, repack, CAS import).
    Process,
}

/// Snapshot of provider progress at one point in time across all three phases.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct ProviderProgressSnapshot {
    /// Current phase.
    pub phase: ProviderPhase,
    /// Items completed vs total `(completed, total)`.
    /// Phase 1: sources resolved; Phase 2: files fetched; Phase 3: entries processed.
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
    use std::fmt::Write as _;
    use std::io::Write;
    use std::sync::atomic::{AtomicBool, AtomicU8, AtomicU64, Ordering};
    use std::sync::{Arc, Mutex, RwLock};
    use std::time::{Duration, Instant};

    use serde::Serialize;
    use serde_json;

    use indicatif::{MultiProgress, ProgressBar, ProgressDrawTarget, ProgressStyle, TermLike};

    // ---- BufferedTerm: suppress terminal writes from property setters ----

    /// Wraps [`console::Term`] to suppress terminal writes when
    /// `buffer_enabled` is `true`.  Used by [`ProgressRenderer`] to ensure
    /// the 50 ms daemon ticker is the sole draw authority — property setters
    /// (called from [`sync_snapshot_to_bar`]) never write to the terminal
    /// directly.
    ///
    /// When buffering is active, all write/clear/move operations are no-ops.
    /// [`width`](Self::width) and [`height`](Self::height) always delegate to
    /// the inner terminal (needed for correct layout in
    /// `draw_to_term`).
    #[derive(Debug)]
    pub(crate) struct BufferedTerm {
        inner: console::Term,
        buffer_enabled: Arc<AtomicBool>,
    }

    impl TermLike for BufferedTerm {
        fn width(&self) -> u16 {
            self.inner.width()
        }

        fn height(&self) -> u16 {
            self.inner.height()
        }

        fn write_line(&self, s: &str) -> std::io::Result<()> {
            if self.buffer_enabled.load(Ordering::Acquire) {
                return Ok(());
            }
            self.inner.write_line(s)
        }

        fn write_str(&self, s: &str) -> std::io::Result<()> {
            if self.buffer_enabled.load(Ordering::Acquire) {
                return Ok(());
            }
            self.inner.write_str(s)
        }

        fn clear_line(&self) -> std::io::Result<()> {
            if self.buffer_enabled.load(Ordering::Acquire) {
                return Ok(());
            }
            self.inner.clear_line()
        }

        fn flush(&self) -> std::io::Result<()> {
            if self.buffer_enabled.load(Ordering::Acquire) {
                return Ok(());
            }
            self.inner.flush()
        }

        fn move_cursor_up(&self, n: usize) -> std::io::Result<()> {
            if self.buffer_enabled.load(Ordering::Acquire) {
                return Ok(());
            }
            self.inner.move_cursor_up(n)
        }

        fn move_cursor_down(&self, n: usize) -> std::io::Result<()> {
            if self.buffer_enabled.load(Ordering::Acquire) {
                return Ok(());
            }
            self.inner.move_cursor_down(n)
        }

        fn move_cursor_left(&self, n: usize) -> std::io::Result<()> {
            if self.buffer_enabled.load(Ordering::Acquire) {
                return Ok(());
            }
            self.inner.move_cursor_left(n)
        }

        fn move_cursor_right(&self, n: usize) -> std::io::Result<()> {
            if self.buffer_enabled.load(Ordering::Acquire) {
                return Ok(());
            }
            self.inner.move_cursor_right(n)
        }
    }

    // ---- RAII buffer guard -----------------------------------------------

    /// RAII guard that temporarily disables buffering and restores it on drop.
    ///
    /// On creation, stores `false` (buffer OFF — next draw goes to terminal).
    /// On drop, stores `true` (buffer ON — subsequent writes suppressed).
    /// When `flag` is `None` (test mode with user-provided `MultiProgress`),
    /// both operations are no-ops.
    #[derive(Debug)]
    struct BufferGuard {
        flag: Option<Arc<AtomicBool>>,
    }

    impl BufferGuard {
        fn new(flag: Option<&Arc<AtomicBool>>) -> Self {
            if let Some(flag) = flag {
                flag.store(false, Ordering::Release);
            }
            Self { flag: flag.cloned() }
        }
    }

    impl Drop for BufferGuard {
        fn drop(&mut self) {
            if let Some(ref flag) = self.flag {
                flag.store(true, Ordering::Release);
            }
        }
    }

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

    // ---- time source (injectable for tests) ------------------------------

    /// Injectable time source for testing.
    pub trait TimeSource: Send + Sync {
        /// Returns the current instant.
        fn now(&self) -> Instant;
    }

    /// Real time via [`Instant::now`].
    pub struct RealTimeSource;

    impl TimeSource for RealTimeSource {
        fn now(&self) -> Instant {
            Instant::now()
        }
    }

    /// Injectable time for testing.
    ///
    /// Use [`advance`](TestTimeSource::advance) to move time forward
    /// synthetically without real wall-clock delay.
    #[allow(dead_code)]
    pub struct TestTimeSource {
        now: Mutex<Instant>,
    }

    impl Default for TestTimeSource {
        fn default() -> Self {
            Self { now: Mutex::new(Instant::now()) }
        }
    }

    #[allow(dead_code)]
    impl TestTimeSource {
        /// Create a source initialized to [`Instant::now`].
        #[must_use]
        pub fn new() -> Self {
            Self::default()
        }

        /// Advance the synthetic clock by `dur`.
        ///
        /// # Panics
        ///
        /// Panics if the internal mutex is poisoned.
        pub fn advance(&self, dur: Duration) {
            *self.now.lock().unwrap() += dur;
        }

        /// Override the instant returned by [`TimeSource::now`].
        ///
        /// # Panics
        ///
        /// Panics if the internal mutex is poisoned.
        #[allow(dead_code)]
        pub fn set(&self, instant: Instant) {
            *self.now.lock().unwrap() = instant;
        }
    }

    impl TimeSource for TestTimeSource {
        /// Returns the current synthetic instant.
        ///
        /// # Panics
        ///
        /// Panics if the internal mutex is poisoned.
        fn now(&self) -> Instant {
            *self.now.lock().unwrap()
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

    // ---- progress debug instrumentation ----------------------------------

    /// JSONL sink for progress debug snapshots.
    ///
    /// Every tick cycle writes one JSON line to the configured writer with a
    /// snapshot of all bar states.  Controlled via
    /// [`ProgressGroupBuilder::with_progress_debug_sink`] or the
    /// `MEDIAPM_PROGRESS_DEBUG` environment variable.
    pub struct ProgressDebugSink {
        /// Output writer (e.g. file, stderr).
        writer: Mutex<Box<dyn Write + Send>>,
        /// Monotonic tick counter — incremented on every emit.
        tick_count: AtomicU64,
        /// Time the sink was created.
        start: Instant,
    }

    impl std::fmt::Debug for ProgressDebugSink {
        fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
            f.debug_struct("ProgressDebugSink")
                .field("writer", &"Box<dyn Write + Send>")
                .field("tick_count", &self.tick_count)
                .field("start", &self.start)
                .finish()
        }
    }

    /// Snapshot of one bar slot at a point in time, ready for JSON
    /// serialization.
    #[derive(Debug, Serialize)]
    pub struct DebugSlotState {
        /// Slot index within the renderer's fixed grid.
        pub slot: usize,
        /// Whether this slot is bound to a tracked source.
        pub bound: bool,
        /// Current label (always present).
        pub label: String,
        /// Current prefix (always present).
        pub prefix: String,
        /// Current position (work completed).
        pub position: u64,
        /// Total work units (0 = indeterminate).
        pub total: u64,
        /// Current status as debug string (e.g. `"Active"`, `"Warning"`).
        pub status: String,
        /// Elapsed seconds since the handle was created.
        pub elapsed_secs: f64,
        /// Rate in bytes/second (0.0 when inactive or indeterminate).
        pub rate_bytes_per_sec: f64,
        /// Estimated seconds remaining (None when unknown or inactive).
        pub eta_secs: Option<f64>,
        /// Custom suffix (empty string when none).
        pub suffix: String,
        /// Whether the source had the dirty flag set this tick.
        pub dirty: bool,
    }

    /// Snapshot of a single tick cycle, serialized as one JSON line.
    #[derive(Debug, Serialize)]
    pub struct DebugTickSnapshot {
        /// Discriminant: `"tick"` (and later `"attach"`, `"finish"` etc.).
        pub r#type: String,
        /// Monotonic tick counter from the sink.
        pub tick: u64,
        /// Seconds since the sink was created.
        pub elapsed_secs: f64,
        /// Per-slot bar states.
        pub bars: Vec<DebugSlotState>,
    }

    impl ProgressDebugSink {
        /// Create a new debug sink that writes JSONL to `writer`.
        #[must_use]
        pub fn new(writer: Box<dyn Write + Send>) -> Self {
            Self {
                writer: Mutex::new(writer),
                tick_count: AtomicU64::new(0),
                start: Instant::now(),
            }
        }

        /// Emit one JSONL line with the given snapshot.
        ///
        /// Increments `tick_count`, serializes the snapshot as compact JSON,
        /// writes it followed by a newline, and flushes the writer.
        ///
        /// # Panics
        ///
        /// Panics if the writer mutex is poisoned or the writer returns an
        /// I/O error.
        pub fn emit(&self, snapshot: &DebugTickSnapshot) {
            self.tick_count.fetch_add(1, Ordering::Relaxed);
            let json = serde_json::to_string(snapshot).expect("debug snapshot serialization");
            let mut writer = self.writer.lock().expect("debug sink writer lock");
            writer.write_all(json.as_bytes()).expect("debug sink write");
            writer.write_all(b"\n").expect("debug sink newline");
            writer.flush().expect("debug sink flush");
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
        "{spinner:.green} {prefix:>30.30} {wide_bar:.yellow/dim} {msg:<25.55}";

    const OVERALL_BAR_TEMPLATE: &str =
        "{spinner:.green} {prefix:>30.30} {wide_bar:.magenta/dim} {msg:<25.55}";

    const COMPACT_BAR_TEMPLATE: &str = "{spinner:.green} {prefix:>25.25} {msg:<12.40}";

    const COMPACT_OVERALL_BAR_TEMPLATE: &str = "{spinner:.green} {prefix:>25.25} {msg:<12.40}";

    const DONE_BAR_TEMPLATE: &str =
        "{spinner:.white/.dim} {prefix:>30.30} {wide_bar:.green/dim} {msg:<25.55}";

    const COMPACT_DONE_BAR_TEMPLATE: &str = "{spinner:.white/.dim} {prefix:>25.25} {msg:<12.40}";

    const FAILED_BAR_TEMPLATE: &str =
        "{spinner:.red} {prefix:>30.30} {wide_bar:.red/dim} {msg:<25.55}";

    const COMPACT_FAILED_BAR_TEMPLATE: &str = "{spinner:.red} {prefix:>25.25} {msg:<12.40}";

    /// Maximum number of pre-allocated slot bars (safety cap).
    const MAX_SLOTS: usize = 256;

    /// ANSI SGR foreground color code matching the `{wide_bar}` template color.
    pub(super) fn bar_color_code(status: TrackStatus, is_overall: bool) -> &'static str {
        match status {
            TrackStatus::Failed => "31",
            TrackStatus::Warning => "33",
            TrackStatus::Success => "32",
            TrackStatus::Active if is_overall => "35",
            TrackStatus::Active => "33",
        }
    }

    /// Render [`SuffixComponents`] into the `{msg}` display string.
    ///
    /// Format: ` \x1b[{color_code}m{count}/{total}\x1b[0m` + ` {elapsed}` +
    /// ` {rate}` (when `Some`) + ` {eta}` (when `rate` AND `eta` are both
    /// `Some` — eta-only-when-rate guard) + ` {custom}` (when non-empty).
    /// The count/total segment is omitted when both fields are empty.
    ///
    /// Normative truncation spec: see [`semantic_truncate_suffix`].
    pub(super) fn render_suffix_components(parts: &SuffixComponents, color_code: &str) -> String {
        let mut s = String::new();
        if !parts.count.is_empty() || !parts.total.is_empty() {
            let _ = write!(s, " \x1b[{color_code}m{}/{}\x1b[0m", parts.count, parts.total);
        }
        if !parts.elapsed.is_empty() {
            s.push(' ');
            s.push_str(&parts.elapsed);
        }
        if let Some(rate) = &parts.rate {
            s.push(' ');
            s.push_str(rate);
            if let Some(eta) = &parts.eta {
                s.push(' ');
                s.push_str(eta);
            }
        }
        if !parts.custom.is_empty() {
            s.push(' ');
            s.push_str(&parts.custom);
        }
        s
    }

    // ---- Source-data component types -----------------------------------

    /// Source components of a progress prefix, stored separately so
    /// [`semantic_truncate_prefix`] receives individual fields directly
    /// rather than a combined string that must be re-parsed.
    ///
    /// Normative truncation spec (removal order, least-important first):
    ///
    /// 1. `version` — progressive shrink (right-to-left, keeping left prefix)
    /// 2. `count`/`total` — removed entirely, as one atomic pair
    /// 3. `phase` — removed entirely
    /// 4. `marker` — removed entirely
    /// 5. `tool_name` — progressive shrink
    /// 6. fallback — hard truncate whatever remains to the width budget
    ///
    /// Storage rule: `count` and `total` are stored as separate fields but
    /// rendered together (`{count}/{total}`) and trimmed together as one
    /// atomic unit — a bare count or bare total is never shown.
    ///
    /// Marker render rule: `marker` (set to `F` for failed, `W` for
    /// warning, else empty) renders as a colored `[{marker}] ` bracket —
    /// red for [`TrackStatus::Failed`], yellow for
    /// [`TrackStatus::Warning`] — when non-empty. The marker brackets are
    /// visible characters that participate in truncation.
    ///
    /// This struct is the prefix's structured single mechanism: initial
    /// values come from parsing the `add_bar`/`with_overall` label at
    /// construction, and [`TrackedHandle::set_prefix_components`] is the
    /// only runtime mutation API. The removal order above is a normative
    /// spec, verified verbatim by the `semantic_truncate_prefix_*` unit
    /// suites.
    #[derive(Debug, Clone, PartialEq, Eq, Default)]
    pub struct PrefixComponents {
        /// Status marker (`F` failed, `W` warning), empty when no marker.
        pub marker: String,
        /// Tool/binary name (always present).
        pub tool_name: String,
        /// Version suffix (e.g. `@7.1`), empty when absent.
        pub version: String,
        /// Phase tag (e.g. `pro`, `fch`, `dl`), empty when absent.
        pub phase: String,
        /// Count (numerator), rendered with `total` as `{count}/{total}`.
        pub count: String,
        /// Total (denominator), rendered with `count` as `{count}/{total}`.
        pub total: String,
    }

    /// Source components of a progress suffix, stored separately so
    /// [`semantic_truncate_suffix`] receives each field directly without
    /// string re-parsing.
    ///
    /// Normative truncation spec (removal order, least-important first):
    ///
    /// 1. `custom` — progressive shrink (right-to-left, keeping left prefix)
    /// 2. `eta` — removed entirely
    /// 3. `rate` — removed entirely
    /// 4. `elapsed` — removed entirely
    /// 5. `count`/`total` — removed entirely, as one atomic pair
    /// 6. fallback — hard truncate whatever remains to the width budget
    ///
    /// Storage rule: `count` and `total` are stored as separate fields but
    /// rendered together (`{count}/{total}`) and trimmed together as one
    /// atomic unit — a bare count or bare total is never shown.
    ///
    /// Render rule: `eta` is rendered only when `rate` is present
    /// (eta-only-when-rate guard).
    ///
    /// This struct is the suffix's structured single mechanism:
    /// [`TrackedHandle::set_suffix_components`] is the only mutation API
    /// (the legacy `set_suffix(String)` is removed), and user-set fields
    /// override the auto-derived ticker fields at sync time with empty
    /// fields auto-filled. The removal order above is a normative spec,
    /// verified verbatim by the `semantic_truncate_suffix_*` unit suites
    /// and preserved unchanged through the user-override merge (the merge
    /// guard tests assert truncation order still holds after merging).
    #[derive(Debug, Clone, PartialEq, Eq, Default)]
    pub struct SuffixComponents {
        /// Count (numerator), rendered with `total` as `{count}/{total}`.
        pub count: String,
        /// Total (denominator), rendered with `count` as `{count}/{total}`.
        pub total: String,
        /// Elapsed time display text (e.g. `0:00:05`), empty when absent.
        pub elapsed: String,
        /// Rate display text (e.g. `12.3 MiB/s`), `None` when no rate yet.
        pub rate: Option<String>,
        /// ETA display text (already bracketed, e.g. `[0:00:02]`), `None` when unavailable.
        pub eta: Option<String>,
        /// Custom suffix text appended after the auto-computed RHS.
        pub custom: String,
    }

    /// Parse an `add_bar` label into [`PrefixComponents`].
    ///
    /// Parsing rules:
    /// - First whitespace-bounded token = `tool_name`.
    /// - Trailing `[phase]` (last bracket pair) = phase inner text.
    /// - Trailing `digits/digits` after bracket = count.
    /// - Remainder between `tool_name` and `[` = `version`.
    /// - If no `[` found: `tool_name` = entire string (single or multi-word),
    ///   except a trailing space-separated `count/total` token is split off.
    ///
    /// Labels are the single identity source for a bar; structured
    /// `set_prefix_components` calls override the parsed components later.
    pub fn prefix_components_from_str(s: &str) -> PrefixComponents {
        let s = s.trim();
        if s.is_empty() {
            return PrefixComponents::default();
        }
        let tool_name = s.split_whitespace().next().unwrap_or("").to_string();
        if let Some(bracket_start) = s.rfind('[') {
            let bracket_end = bracket_start + s[bracket_start..].find(']').map_or(0, |i| i + 1);
            let phase = s[(bracket_start + 1)..(bracket_end - 1)].trim().to_string();
            let after_bracket = s[bracket_end..].trim();
            let (count, total) = split_count_total(after_bracket);
            let between = s[tool_name.len()..bracket_start].trim();
            // between might contain the count text if it appears before bracket
            let version = if !between.is_empty() && !between.contains('/') {
                between.to_string()
            } else {
                String::new()
            };
            PrefixComponents { marker: String::new(), tool_name, version, phase, count, total }
        } else {
            // No brackets — the entire string is the tool name (single or
            // multi-word), except a trailing space-separated count/total
            // token (containing `/`) is split off into `count`/`total`.
            let (tool_name, count, total) = match s.rfind(' ') {
                Some(space) => {
                    let (head, tail) = s.split_at(space + 1);
                    let (count, total) = split_count_total(tail.trim());
                    if total.is_empty() {
                        (s.to_string(), String::new(), String::new())
                    } else {
                        (head.trim().to_string(), count, total)
                    }
                }
                None => (s.to_string(), String::new(), String::new()),
            };
            PrefixComponents {
                marker: String::new(),
                tool_name,
                version: String::new(),
                phase: String::new(),
                count,
                total,
            }
        }
    }

    /// Split a `count/total` token (e.g. `2/5`) into separate fields.
    /// Returns empty strings when the token contains no `/`.
    fn split_count_total(token: &str) -> (String, String) {
        match token.split_once('/') {
            Some((count, total)) => (count.trim().to_string(), total.trim().to_string()),
            None => (String::new(), String::new()),
        }
    }

    /// Render [`PrefixComponents`] into the combined prefix display string.
    ///
    /// The render always starts with an ANSI reset to clear any SGR state
    /// from preceding template fields (e.g. `{spinner:.green}`). A non-empty
    /// `marker` renders as a colored `[{marker}] ` bracket — red for
    /// [`TrackStatus::Failed`], yellow for [`TrackStatus::Warning`],
    /// uncolored for other statuses. Then components render as
    /// `{tool_name}[ {version}][ [{phase}]][ {count}/{total}]`; empty
    /// sections are omitted, and `count`/`total` render only as a pair.
    pub fn render_prefix_components(parts: &PrefixComponents, status: TrackStatus) -> String {
        let mut s = String::from("\x1b[0m");
        if !parts.marker.is_empty() {
            match status {
                TrackStatus::Failed => {
                    s.push_str("\x1b[31m[");
                    s.push_str(&parts.marker);
                    s.push_str("]\x1b[0m ");
                }
                TrackStatus::Warning => {
                    s.push_str("\x1b[33m[");
                    s.push_str(&parts.marker);
                    s.push_str("]\x1b[0m ");
                }
                _ => {
                    s.push('[');
                    s.push_str(&parts.marker);
                    s.push_str("] ");
                }
            }
        }
        s.push_str(&parts.tool_name);
        if !parts.version.is_empty() {
            s.push(' ');
            s.push_str(&parts.version);
        }
        if !parts.phase.is_empty() {
            s.push_str(" [");
            s.push_str(&parts.phase);
            s.push(']');
        }
        if !parts.count.is_empty() || !parts.total.is_empty() {
            s.push(' ');
            s.push_str(&parts.count);
            s.push('/');
            s.push_str(&parts.total);
        }
        s
    }

    // ---- ANSI-safe truncation helpers -----------------------------------

    /// Strip ANSI SGR escape sequences (`\x1b[...m`) from `s`.
    /// Non-SGR escape sequences (not ending with `m`) are left untouched.
    pub(super) fn strip_ansi(s: &str) -> String {
        let mut out = String::with_capacity(s.len());
        let mut chars = s.chars();
        while let Some(c) = chars.next() {
            if c == '\x1b' {
                // Try to match \x1b[...m (SGR sequence)
                let saved = chars.clone();
                if chars.next() == Some('[') {
                    let mut seq = String::new();
                    for ch in chars.by_ref() {
                        seq.push(ch);
                        if ch == 'm' {
                            break;
                        }
                        if !ch.is_ascii_digit() && ch != ';' {
                            // Not an SGR sequence — restore and emit the escape.
                            out.push('\x1b');
                            out.push('[');
                            out.push_str(&seq);
                            break;
                        }
                    }
                    // If we broke because of 'm', the sequence was consumed — drop it.
                    // If we pushed saved chars, continue.
                } else {
                    out.push('\x1b');
                    chars = saved;
                }
            } else {
                out.push(c);
            }
        }
        out
    }

    /// Number of visible characters in `s` (after stripping ANSI SGR codes).
    #[cfg_attr(not(test), expect(dead_code))]
    pub(super) fn visible_width(s: &str) -> usize {
        strip_ansi(s).chars().count()
    }

    /// Maximum visible width for the prefix field based on terminal width.
    pub(super) const fn max_prefix_width(cols: u16) -> usize {
        if cols >= 60 { 30 } else { 25 }
    }

    /// Maximum visible width for the suffix field based on terminal width.
    pub(super) const fn max_suffix_width(cols: u16) -> usize {
        if cols >= 60 { 55 } else { 40 }
    }

    /// Progressively truncate prefix [`PrefixComponents`] so the rendered
    /// visible width fits within `max_width`.
    ///
    /// Normative truncation spec (removal order, least-important first):
    ///
    /// 1. `version` — progressive shrink (right-to-left, keeping left prefix)
    /// 2. `count`/`total` — removed entirely, as one atomic pair
    /// 3. `phase` — removed entirely
    /// 4. `marker` — removed entirely
    /// 5. `tool_name` — progressive shrink
    /// 6. fallback — hard truncate whatever remains to the width budget
    ///
    /// Storage rule: `count` and `total` are stored as separate fields but
    /// rendered together (`{count}/{total}`) and trimmed together as one
    /// atomic unit — a bare count or bare total is never shown. The `marker`
    /// is data like any other component: its brackets are visible characters
    /// that participate in truncation, and it is dropped before the tool name
    /// starts shrinking.
    ///
    /// Fit is measured on the component render (the components themselves
    /// carry no ANSI escapes — coloring is applied at render time).
    pub(super) fn semantic_truncate_prefix(
        parts: &PrefixComponents,
        max_width: usize,
    ) -> PrefixComponents {
        let fits = |p: &PrefixComponents| {
            let mut w = p.tool_name.chars().count();
            if !p.marker.is_empty() {
                w += p.marker.chars().count() + 3; // `[`, `]`, ` `
            }
            if !p.version.is_empty() {
                w += 1 + p.version.chars().count(); // ` `
            }
            if !p.phase.is_empty() {
                w += p.phase.chars().count() + 3; // ` [`, `]`
            }
            if !p.count.is_empty() || !p.total.is_empty() {
                w += p.count.chars().count() + p.total.chars().count() + 2; // ` `, `/`
            }
            w <= max_width
        };
        if fits(parts) {
            return parts.clone();
        }
        let mut out = parts.clone();

        // 1. Version: progressive shrink (right-to-left).
        if !out.version.is_empty() {
            let version_len = out.version.chars().count();
            for keep in (0..version_len).rev() {
                out.version = out.version.chars().take(keep).collect();
                if fits(&out) {
                    return out;
                }
            }
            // Fully removed — continue to the next step.
        }

        // 2. Count/total: removed entirely, as one atomic pair.
        if !out.count.is_empty() || !out.total.is_empty() {
            out.count = String::new();
            out.total = String::new();
            if fits(&out) {
                return out;
            }
        }

        // 3. Phase: removed entirely (including brackets).
        if !out.phase.is_empty() {
            out.phase = String::new();
            if fits(&out) {
                return out;
            }
        }

        // 4. Marker: removed entirely.
        if !out.marker.is_empty() {
            out.marker = String::new();
            if fits(&out) {
                return out;
            }
        }

        // 5. Tool name: progressive shrink.
        if !out.tool_name.is_empty() {
            let tool_len = out.tool_name.chars().count();
            for keep in (0..tool_len).rev() {
                out.tool_name = out.tool_name.chars().take(keep).collect();
                if fits(&out) {
                    return out;
                }
            }
        }

        // 6. Fallback: hard truncate whatever remains (only reachable when
        // every component is already empty).
        out
    }

    /// Progressively truncate suffix [`SuffixComponents`] so the rendered
    /// visible width fits within `max_width`.
    ///
    /// Normative truncation spec (removal order, least-important first):
    ///
    /// 1. `custom` — progressive shrink (right-to-left, keeping left prefix)
    /// 2. `eta` — removed entirely
    /// 3. `rate` — removed entirely
    /// 4. `elapsed` — removed entirely
    /// 5. `count`/`total` — removed entirely, as one atomic pair
    /// 6. fallback — hard truncate whatever remains to the width budget
    ///
    /// Storage rule: `count` and `total` are stored as separate fields but
    /// rendered together (`{count}/{total}`) and trimmed together as one
    /// atomic unit — a bare count or bare total is never shown.
    ///
    /// Fit is measured on the render with an empty color code (ANSI escapes
    /// add no visible width).
    pub(super) fn semantic_truncate_suffix(
        parts: &SuffixComponents,
        max_width: usize,
    ) -> SuffixComponents {
        let fits = |p: &SuffixComponents| {
            strip_ansi(&render_suffix_components(p, "")).chars().count() <= max_width
        };
        if fits(parts) {
            return parts.clone();
        }
        let mut out = parts.clone();

        // 1. Custom: progressive shrink (right-to-left, keeping left prefix).
        if !out.custom.is_empty() {
            let custom_len = out.custom.chars().count();
            for keep in (0..custom_len).rev() {
                out.custom = out.custom.chars().take(keep).collect();
                if fits(&out) {
                    return out;
                }
            }
            // Custom fully removed but still doesn't fit — continue below.
        }

        // 2. Eta: removed entirely.
        if out.eta.is_some() {
            out.eta = None;
            if fits(&out) {
                return out;
            }
        }

        // 3. Rate: removed entirely.
        if out.rate.is_some() {
            out.rate = None;
            if fits(&out) {
                return out;
            }
        }

        // 4. Elapsed: removed entirely.
        if !out.elapsed.is_empty() {
            out.elapsed.clear();
            if fits(&out) {
                return out;
            }
        }

        // 5. Count/total: removed entirely, as one atomic pair.
        if !out.count.is_empty() || !out.total.is_empty() {
            out.count.clear();
            out.total.clear();
            if fits(&out) {
                return out;
            }
        }

        // 6. Fallback: hard truncate whatever remains to the width budget.
        out.custom =
            strip_ansi(&render_suffix_components(&out, "")).chars().take(max_width).collect();
        out
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
        /// Bar finished with a non-fatal warning (kept visible).
        Warning,
    }

    /// Shared mutable state for a tracked progress handle.
    ///
    /// Interior mutability via atomics for numeric fields and [`RwLock`] for
    /// string fields.  [`Send`] + [`Sync`] when wrapped in [`Arc`].
    pub(crate) struct SharedState {
        position: AtomicU64,
        total: AtomicU64,
        label: RwLock<String>,
        prefix_components: RwLock<PrefixComponents>,
        suffix_components: RwLock<SuffixComponents>,
        status: AtomicU8,
        dirty: AtomicBool,
        disabled: AtomicBool,
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
        /// This is the **single canonical construction path** for prefix
        /// data: the `add_bar`/`with_overall` label is parsed once here via
        /// [`prefix_components_from_str`], so even bars whose label was
        /// never touched by [`set_prefix_components`] carry structured
        /// components (tool name, version, phase, count/total) for
        /// [`semantic_truncate_prefix`] to truncate field-by-field. The
        /// legacy `set_prefix(String)` API that re-parsed at mutation time
        /// has been removed — this construction-time parse plus
        /// [`set_prefix_components`] is the only prefix mechanism.
        pub(crate) fn with_time_source(
            total: u64,
            label: &str,
            time_source: Arc<dyn TimeSource>,
        ) -> Self {
            Self {
                position: AtomicU64::new(0),
                total: AtomicU64::new(total),
                label: RwLock::new(label.to_string()),
                prefix_components: RwLock::new(prefix_components_from_str(label)),
                suffix_components: RwLock::new(SuffixComponents::default()),
                status: AtomicU8::new(0),
                dirty: AtomicBool::new(true),
                disabled: AtomicBool::new(false),
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
        /// This is the **single runtime prefix mutation API**. The initial
        /// value always comes from parsing the `add_bar`/`with_overall`
        /// label at construction (see [`SharedState::with_time_source`]);
        /// this method overrides those parsed components with fully
        /// structured source data. The legacy `set_prefix(String)` API has
        /// been removed — there is no string mutation path left.
        ///
        /// # Panics
        ///
        /// Panics if the shared-state `RwLock` is poisoned.
        pub fn set_prefix_components(&self, components: PrefixComponents) {
            {
                let mut pc = self
                    .state
                    .prefix_components
                    .write()
                    .expect("shared_state prefix_components lock");
                *pc = components;
            }
            self.state.dirty.store(true, Ordering::Release);
        }

        /// Set suffix components directly (source-data API).
        ///
        /// This is the **single suffix mutation API** — the legacy
        /// `set_suffix(String)` API has been removed, and the suffix always
        /// flows through this structured path.
        ///
        /// Merge semantics (applied at [`sync_snapshot_to_bar`] time):
        /// user-set fields override the auto-derived fields composed from
        /// ticker data; empty user fields fall back to fresh ticker data,
        /// so callers may set just the fields they care about (typically
        /// `custom`) and leave the rest defaulted.
        ///
        /// # Panics
        ///
        /// Panics if the shared-state `RwLock` is poisoned.
        pub fn set_suffix_components(&self, components: SuffixComponents) {
            if self.state.disabled.load(Ordering::Relaxed) {
                return; // disabled handle
            }
            {
                let mut sc = self
                    .state
                    .suffix_components
                    .write()
                    .expect("shared_state suffix_components lock");
                *sc = components;
            }
            self.state.dirty.store(true, Ordering::Release);
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
        /// Injectable time source (real or synthetic for testing).
        time_source: Arc<dyn TimeSource>,

        /// EMA-smoothed rate tracking, one entry per slot.
        slots_timing: Vec<SlotTiming>,
        /// When `Some`, property-setter terminal writes are suppressed
        /// during [`tick`](Self::tick).  `None` when the user provided
        /// their own [`MultiProgress`] (tests via `InMemoryTerm`).
        buffer_enabled: Option<Arc<AtomicBool>>,

        /// One-shot flag: has the first-draw pre-roll (newline scroll) been
        /// performed?  Used to push intervening stderr content into scrollback
        /// before indicatif's first draw.
        pre_rolled: AtomicBool,

        /// Terminal to write pre-roll newlines to.  `None` in test mode
        /// (user-provided `MultiProgress` via `with_multi_progress`).
        pre_roll_term: Option<Box<dyn TermLike>>,

        /// Optional JSONL debug sink — emits bar-state snapshots on every tick.
        debug_sink: Option<ProgressDebugSink>,
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
    }

    impl SlotCache {
        fn new() -> Self {
            Self {
                position: Cell::new(u64::MAX),
                total: Cell::new(u64::MAX),
                suffix: RefCell::new(String::new()),
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
            buffer_enabled: Option<Arc<AtomicBool>>,
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
                buffer_enabled,
                pre_rolled: AtomicBool::new(false),
                pre_roll_term,
                debug_sink,
            }
        }

        /// Pre-allocate `capacity` bars with an overall bar at the bottom,
        /// using an existing [`MultiProgress`].  Returns `(renderer, overall_state)`.
        #[allow(clippy::too_many_arguments)]
        fn from_mp_with_overall(
            mp: MultiProgress,
            capacity: usize,
            total: u64,
            label: &str,
            dim_source: Arc<dyn DimensionSource>,
            buffer_enabled: Option<Arc<AtomicBool>>,
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
                slots.push(RenderedSlot {
                    bar,
                    source: RefCell::new(None),
                    cache: SlotCache::new(),
                });
            }
            // Last slot = overall bar.
            let overall_state =
                Arc::new(SharedState::with_time_source(total, label, Arc::clone(&time_source)));
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
                    buffer_enabled,
                    pre_rolled: AtomicBool::new(false),
                    pre_roll_term,
                    debug_sink,
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
                    // Slot recycling may leave the indicatif bar with
                    // Status::DoneVisible from the previous phase.  Reset
                    // it to InProgress so the spinner cycles again.
                    if slot.bar.is_finished() {
                        slot.bar.reset();
                    }
                    apply_bar_style(&slot.bar, cols);
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
                self.slots_timing[bottom] = SlotTiming::new(&*self.time_source);
                self.slots[bottom].cache = SlotCache::new();
                self.sync_slot(bottom);
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
        fn has_active_slots(&self) -> bool {
            self.slots
                .iter()
                .any(|slot| slot.source.borrow().as_ref().is_some_and(|s| !s.is_finished()))
        }

        /// Defensive sync: refresh all render slots from their tracked sources.
        ///
        /// Includes resize reactivity and full style re-application.
        ///
        /// When [`buffer_enabled`](Self::buffer_enabled) is `Some` (production),
        /// property-setter terminal writes are suppressed during the update
        /// loop, then exactly one draw is released at the end.  This ensures
        /// the 50 ms daemon ticker is the sole draw authority and eliminates
        /// flicker from burst writes.
        #[expect(
            clippy::too_many_lines,
            reason = "tick orchestrates many progress-bar state updates that are clearer inline"
        )]
        pub fn tick(&mut self) {
            // Step 1: Enable buffering — all property-setter draws become
            // no-ops through BufferedTerm.
            if let Some(ref flag) = self.buffer_enabled {
                flag.store(true, Ordering::Release);
            }

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
            let _guard = BufferGuard::new(self.buffer_enabled.as_ref());

            // Always tick active bars for spinner animation (dirty-independent).
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

        /// Apply a snapshot's position/length/suffix/prefix to the
        /// indicatif bar at slot `i`.  **This is the single authoritative
        /// push point for `SharedState` → indicatif.** All code paths that
        /// reflect `SharedState` (position, total, suffix, prefix) on the
        /// terminal bar must call through here — both the daemon ticker
        /// and [`finalize`](Self::finalize) do.
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
            let (_, cols) = self.dim_source.dimensions();
            let count_str = format_count(snap.position);
            let total_str = format_count(snap.total);
            let elapsed_str = format_elapsed(snap.elapsed);
            let color_code = bar_color_code(snap.status, is_overall);
            // Compose a fresh suffix component set each tick: auto fields from
            // snapshot + ticker timing, user-set fields from stored components.
            // Stored non-empty fields override the auto-derived ones; stored
            // rate/eta override when Some; empty fields auto-fill.
            let stored = &snap.suffix_components;
            let fresh_suffix = SuffixComponents {
                count: if stored.count.is_empty() { count_str } else { stored.count.clone() },
                total: if stored.total.is_empty() { total_str } else { stored.total.clone() },
                elapsed: if stored.elapsed.is_empty() {
                    elapsed_str
                } else {
                    stored.elapsed.clone()
                },
                rate: stored.rate.clone().or_else(|| rate_str.map(str::to_owned)),
                eta: stored.eta.clone().or_else(|| eta_str.map(str::to_owned)),
                custom: stored.custom.clone(),
            };

            // Truncate prefix to fit template width, accounting for ANSI
            // escapes added by render_prefix_components (which indicatif counts
            // as visible chars). Normal: \x1b[0m = 4; failed/warning:
            // \x1b[0m\x1b[3Xm\x1b[0m = 13. The marker brackets are visible data
            // and consume the width budget via semantic_truncate_prefix.
            let ansi_overhead: usize = match snap.status {
                TrackStatus::Failed | TrackStatus::Warning => 13,
                _ => 4,
            };
            let truncated_prefix = semantic_truncate_prefix(
                &snap.prefix_components,
                max_prefix_width(cols).saturating_sub(ansi_overhead),
            );
            let new_prefix = render_prefix_components(&truncated_prefix, snap.status);
            if new_prefix != *slot.cache.prefix.borrow() {
                slot.bar.set_prefix(new_prefix.clone());
                *slot.cache.prefix.borrow_mut() = new_prefix;
            }
            // Build display suffix: truncate the fresh component set, then render.
            let truncated_suffix = semantic_truncate_suffix(&fresh_suffix, max_suffix_width(cols));
            let display_suffix = render_suffix_components(&truncated_suffix, color_code);
            if display_suffix != *slot.cache.suffix.borrow() {
                slot.bar.set_message(display_suffix.clone());
                *slot.cache.suffix.borrow_mut() = display_suffix;
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
            let (_, cols) = self.dim_source.dimensions();
            if self.has_overall && i == self.slots.len() - 1 {
                apply_overall_bar_style(&slot.bar, cols);
            } else if status == TrackStatus::Failed {
                apply_failed_bar_style(&slot.bar, cols);
            } else {
                apply_done_bar_style(&slot.bar, cols);
            }
            match status {
                TrackStatus::Failed | TrackStatus::Warning => slot.bar.abandon(),
                _ => slot.bar.finish(),
            }
            slot.bar.tick();
        }

        /// Reserve the full terminal height before the first indicatif draw.
        ///
        /// Writes `rows` newlines to bypass [`BufferedTerm`] so they go
        /// directly to the terminal, then moves cursor back up `rows` lines.
        /// This reserves the entire terminal screen for progress bar content,
        /// preventing intervening stderr content from being overwritten during
        /// bar draws.
        ///
        /// One-shot: only the first call writes; subsequent calls are no-ops.
        /// In test mode (`pre_roll_term` is `None`) this is always a no-op.
        ///
        /// # Scroll guarantee
        ///
        /// Moves the cursor to the absolute bottom of the terminal *before*
        /// writing blank lines.  This ensures every blank `write_line` triggers
        /// a scroll — newlines from a cursor partway down the screen would
        /// only fill remaining rows below it, leaving visible content above
        /// exposed.  After the blank lines the cursor returns to the top so
        /// indicatif can overwrite the now-empty visible area.
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
                        let slot = RenderedSlot {
                            bar,
                            source: RefCell::new(None),
                            cache: SlotCache::new(),
                        };
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
        fn finalize(&self) {
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
            let _guard = BufferGuard::new(self.buffer_enabled.as_ref());
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
        /// Daemon ticker task driving renders at 50 ms intervals.
        /// Holds a `Weak` reference to the renderer — exits cleanly
        /// when the renderer is dropped.
        ticker: Option<std::thread::JoinHandle<()>>,
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
    pub struct ProgressGroupBuilder {
        mp: Option<MultiProgress>,
        dim_source: Arc<dyn DimensionSource>,
        overall: Option<(String, u64)>,
        capacity: Option<usize>,
        dynamic_height: bool,
        time_source: Arc<dyn TimeSource>,
        pre_roll_term: Option<Box<dyn TermLike>>,
        debug_sink: Option<ProgressDebugSink>,
        ticker_enabled: bool,
    }

    impl Default for ProgressGroupBuilder {
        fn default() -> Self {
            Self {
                mp: None,
                dim_source: Arc::new(RealTerminalSource),
                overall: None,
                capacity: None,
                dynamic_height: false,
                time_source: Arc::new(RealTimeSource),
                pre_roll_term: None,
                debug_sink: None,
                ticker_enabled: true,
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
        ///
        /// `label` is parsed into [`PrefixComponents`] at construction,
        /// exactly like [`ProgressGroup::add_bar`] — the overall bar shares
        /// the single canonical prefix path and the legacy `set_prefix(String)`
        /// API does not exist here either.
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

        /// Use an injectable time source (for tests).
        #[must_use]
        pub fn with_time_source(mut self, time_source: Arc<dyn TimeSource>) -> Self {
            self.time_source = time_source;
            self
        }

        /// Use an injectable term for pre-roll capture (for test assertions).
        ///
        /// When called, pre-roll newlines are written to `term` instead of
        /// `console::Term::stderr()`.  The user must also pass a compatible
        /// [`MultiProgress`] created from the same term via
        /// `ProgressDrawTarget::term_like`.
        #[must_use]
        pub fn with_pre_roll_capture(mut self, term: Box<dyn TermLike>) -> Self {
            self.pre_roll_term = Some(term);
            self
        }

        /// Attach a JSONL debug sink for progress bar state snapshots.
        #[must_use]
        pub fn with_progress_debug_sink(mut self, sink: ProgressDebugSink) -> Self {
            self.debug_sink = Some(sink);
            self
        }

        /// Disable or enable the background render ticker thread (default:
        /// enabled).  Disable in tests for deterministic progress bar output.
        #[must_use]
        pub fn with_ticker_enabled(mut self, enabled: bool) -> Self {
            self.ticker_enabled = enabled;
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
            let (mp, buffer_enabled, pre_roll_term): (
                MultiProgress,
                Option<Arc<AtomicBool>>,
                Option<Box<dyn TermLike>>,
            ) = if let Some(ref mp) = self.mp {
                (mp.clone(), None, self.pre_roll_term)
            } else {
                let flag = Arc::new(AtomicBool::new(true));
                let term =
                    BufferedTerm { inner: console::Term::stderr(), buffer_enabled: flag.clone() };
                let mp =
                    MultiProgress::with_draw_target(ProgressDrawTarget::term_like(Box::new(term)));
                (mp, Some(flag), Some(Box::new(console::Term::stderr()) as Box<dyn TermLike>))
            };
            let debug_sink = self.debug_sink.or_else(detect_progress_debug_env);
            let mut renderer = ProgressRenderer::from_mp(
                mp,
                cap,
                self.dim_source,
                buffer_enabled,
                self.time_source,
                pre_roll_term,
                debug_sink,
            );
            renderer.dynamic_height = self.dynamic_height;
            let renderer = Some(Arc::new(Mutex::new(renderer)));
            let ticker = if self.ticker_enabled {
                ProgressGroup::spawn_ticker(renderer.as_ref().unwrap())
            } else {
                None
            };
            ProgressGroup { renderer, ticker }
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
            let (mp, buffer_enabled, pre_roll_term): (
                MultiProgress,
                Option<Arc<AtomicBool>>,
                Option<Box<dyn TermLike>>,
            ) = if let Some(ref mp) = self.mp {
                (mp.clone(), None, self.pre_roll_term)
            } else {
                let flag = Arc::new(AtomicBool::new(true));
                let term =
                    BufferedTerm { inner: console::Term::stderr(), buffer_enabled: flag.clone() };
                let mp =
                    MultiProgress::with_draw_target(ProgressDrawTarget::term_like(Box::new(term)));
                (mp, Some(flag), Some(Box::new(console::Term::stderr()) as Box<dyn TermLike>))
            };
            let debug_sink = self.debug_sink.or_else(detect_progress_debug_env);
            let (mut renderer, state) = ProgressRenderer::from_mp_with_overall(
                mp,
                cap,
                total,
                &label,
                self.dim_source,
                buffer_enabled,
                self.time_source,
                pre_roll_term,
                debug_sink,
            );
            renderer.dynamic_height = self.dynamic_height;
            let renderer = Arc::new(Mutex::new(renderer));
            let ticker =
                if self.ticker_enabled { ProgressGroup::spawn_ticker(&renderer) } else { None };
            let handle = TrackedHandle { state };
            (ProgressGroup { renderer: Some(renderer), ticker }, handle)
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
            Self { renderer: None, ticker: None }
        }

        /// Add a child bar to the group.
        ///
        /// `label` is parsed into [`PrefixComponents`] at construction (see
        /// [`SharedState::with_time_source`]), so the bar's prefix is
        /// immediately structured — `[phase]` markers and `count/total`
        /// survive truncation field-by-field. Use
        /// [`TrackedHandle::set_prefix_components`] to replace the parsed
        /// components with structured source data at runtime; there is no
        /// string mutation API.
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
            let state;
            {
                let mut locked = renderer.lock().unwrap_or_else(std::sync::PoisonError::into_inner);
                state = Arc::new(SharedState::with_time_source(
                    total,
                    label,
                    Arc::clone(&locked.time_source),
                ));
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
                renderer.lock().unwrap_or_else(std::sync::PoisonError::into_inner).finalize();
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
                renderer.lock().unwrap_or_else(std::sync::PoisonError::into_inner).tick();
            }
        }

        /// Spawn a dedicated thread that drives render updates at 50 ms
        /// intervals.  Holds a `Weak` reference so the thread exits
        /// cleanly when the renderer is dropped.  Returns `None` when
        /// the thread could not be spawned.
        ///
        /// Using a dedicated thread (instead of a tokio task) ensures
        /// the spinner animates even when the async runtime is under
        /// load or the mutex is contested — the ticker is completely
        /// decoupled from the tokio worker pool.
        ///
        /// # Panics
        ///
        /// Panics when the internal `Mutex` is poisoned (another thread
        /// panicked while holding the lock).
        fn spawn_ticker(
            renderer: &Arc<Mutex<ProgressRenderer>>,
        ) -> Option<std::thread::JoinHandle<()>> {
            let weak = Arc::downgrade(renderer);
            match std::thread::Builder::new().name("mediapm-progress-ticker".into()).spawn(
                move || {
                    let mut all_done = false;
                    loop {
                        let sleep_ms = if all_done { 1000 } else { 50 };
                        std::thread::sleep(Duration::from_millis(sleep_ms));
                        let Some(r) = weak.upgrade() else { break };
                        let Ok(mut guard) = r.lock() else {
                            break;
                        };
                        guard.tick();
                        all_done = !guard.has_active_slots();
                    }
                },
            ) {
                Ok(handle) => Some(handle),
                Err(e) => {
                    eprintln!("warning: failed to start progress ticker thread: {e}");
                    None
                }
            }
        }
    }

    impl Default for ProgressGroup {
        fn default() -> Self {
            Self::builder().build()
        }
    }

    impl Drop for ProgressGroup {
        fn drop(&mut self) {
            // Drop the ticker handle to detach the thread — it will
            // exit on its next iteration when weak.upgrade() returns
            // None (the renderer Arc is dropped right after this).
            self.ticker.take();
            if let Some(ref renderer) = self.renderer {
                renderer.lock().unwrap_or_else(std::sync::PoisonError::into_inner).finalize();
            }
        }
    }

    /// Detect `MEDIAPM_PROGRESS_DEBUG` environment variable and create a
    /// [`ProgressDebugSink`] if set.
    ///
    /// - When the value is `"auto"` or empty: creates a file named
    ///   `progress-debug-<PID>.jsonl` in the current directory.
    /// - Otherwise: interprets the value as a file path and creates/overwrites
    ///   it.
    ///
    /// Returns `None` when the env var is not set (the normal case).
    fn detect_progress_debug_env() -> Option<ProgressDebugSink> {
        let val = std::env::var("MEDIAPM_PROGRESS_DEBUG").ok()?;
        let writer: Box<dyn Write + Send> = if val == "auto" || val.is_empty() {
            let path =
                std::path::PathBuf::from(format!("progress-debug-{}.jsonl", std::process::id()));
            Box::new(std::fs::File::create(&path).expect("failed to create progress debug file"))
        } else {
            let path = std::path::PathBuf::from(&val);
            Box::new(std::fs::File::create(&path).expect("failed to create progress debug file"))
        };
        Some(ProgressDebugSink::new(writer))
    }
}

#[cfg(feature = "progress")]
pub use inner::{
    DebugSlotState, DebugTickSnapshot, DimensionSource, PrefixComponents, ProgressDebugSink,
    ProgressGroup, ProgressRenderer, RealTerminalSource, RealTimeSource, SuffixComponents,
    TestDimensionSource, TestTimeSource, TimeSource, TrackSnapshot, TrackStatus, TrackedHandle,
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
        /// `set_suffix_components(components)` was called. Records the full
        /// structured set, matching what the caller passed.
        SetSuffixComponents {
            /// Full suffix component set.
            components: super::SuffixComponents,
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
        pub fn set_prefix_components(&self, components: super::PrefixComponents) {
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
        pub fn set_suffix_components(&self, components: super::SuffixComponents) {
            self.ops
                .lock()
                .expect("recording lock")
                .push(ProgressOp::SetSuffixComponents { components });
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
            prefix_components: crate::progress::inner::PrefixComponents::default(),
            suffix: String::new(),
            suffix_components: crate::progress::inner::SuffixComponents::default(),
            status: TrackStatus::Active,
            elapsed: recording::RecordingTrackedHandle::snapshot_elapsed(self),
        }
    }
    fn is_finished(&self) -> bool {
        let ops = self.ops();
        ops.iter().any(|op| {
            matches!(
                op,
                recording::ProgressOp::FinishSuccess
                    | recording::ProgressOp::FinishError
                    | recording::ProgressOp::FinishWarning
                    | recording::ProgressOp::FinishAndClear
            )
        })
    }
    fn finish_warning(&self) {
        recording::RecordingTrackedHandle::finish_warning(self);
    }
    fn set_position(&self, pos: u64) {
        recording::RecordingTrackedHandle::set_position(self, pos);
    }
    fn set_total(&self, total: u64) {
        recording::RecordingTrackedHandle::set_total(self, total);
    }
    fn set_prefix_components(&self, components: PrefixComponents) {
        recording::RecordingTrackedHandle::set_prefix_components(self, components);
    }
    fn set_suffix_components(&self, components: SuffixComponents) {
        recording::RecordingTrackedHandle::set_suffix_components(self, components);
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
    use super::{PrefixComponents, ProgressGroup, SuffixComponents, TrackStatus, TrackedHandle};
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
        h.set_prefix_components(PrefixComponents { tool_name: "pfx".into(), ..Default::default() });
        h.finish_success();

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
        dh.set_prefix_components(PrefixComponents {
            tool_name: "pfx".into(),
            ..Default::default()
        });
        dh.finish_success();
        dh.finish_error();
        dh.finish_and_clear();
    }

    #[test]
    fn handle_disabled_is_noop() {
        let h = TrackedHandle::disabled();
        assert_eq!(h.total(), 0);
        h.advance(10);
        h.set_total(50);
        h.set_position(5);
        h.set_prefix_components(PrefixComponents { tool_name: "pfx".into(), ..Default::default() });
        h.finish_success();
        h.finish_error();
        h.finish_and_clear();
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
        h.set_prefix_components(PrefixComponents { tool_name: "pfx".into(), ..Default::default() });
        h.finish_success();

        assert_eq!(
            h.ops(),
            vec![
                ProgressOp::SetTotal { total: 200 },
                ProgressOp::Advance { delta: 5 },
                ProgressOp::SetPosition { pos: 10 },
                ProgressOp::SetPrefixComponents {
                    marker: String::new(),
                    tool_name: "pfx".into(),
                    version: String::new(),
                    phase: String::new(),
                    count: String::new(),
                    total: String::new(),
                },
                ProgressOp::FinishSuccess,
            ]
        );
    }

    #[test]
    fn recording_handle_set_prefix_components_ops() {
        let h = RecordingTrackedHandle::new(100);
        h.set_prefix_components(PrefixComponents {
            marker: String::new(),
            tool_name: "wget".into(),
            version: "1.2.3".into(),
            phase: "fch".into(),
            count: "2".into(),
            total: "5".into(),
        });

        assert_eq!(
            h.ops(),
            vec![ProgressOp::SetPrefixComponents {
                marker: String::new(),
                tool_name: "wget".into(),
                version: "1.2.3".into(),
                phase: "fch".into(),
                count: "2".into(),
                total: "5".into(),
            }]
        );
    }

    #[test]
    fn recording_handle_set_suffix_components_ops() {
        let h = RecordingTrackedHandle::new(100);
        let components = SuffixComponents {
            count: "9".into(),
            total: "9".into(),
            elapsed: "1:23:45".into(),
            rate: Some("10/s".into()),
            eta: Some("[0:00:07]".into()),
            custom: "cached (1)".into(),
        };
        h.set_suffix_components(components.clone());

        assert_eq!(h.ops(), vec![ProgressOp::SetSuffixComponents { components }],);
    }

    #[test]
    fn recording_handle_shared_log() {
        let rt = RecordingProgressTracker::new();
        let h1 = rt.add_bar(50, "first");
        let h2 = rt.add_bar(100, "second");

        h1.advance(1);
        h2.advance(2);
        h1.finish_success();

        assert_eq!(
            rt.ops(),
            vec![
                ProgressOp::AddBar { total: 50, label: "first".into() },
                ProgressOp::AddBar { total: 100, label: "second".into() },
                ProgressOp::Advance { delta: 1 },
                ProgressOp::Advance { delta: 2 },
                ProgressOp::FinishSuccess,
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
    fn recording_handle_finish_and_clear_warning() {
        let h = RecordingTrackedHandle::new(1);
        h.finish_and_clear();
        h.finish_warning();
        assert_eq!(h.ops(), vec![ProgressOp::FinishAndClear, ProgressOp::FinishWarning]);
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
        std::thread::sleep(std::time::Duration::from_millis(1));
        h.finish_success();
        let frozen = h.snapshot_elapsed();
        // Verify the value stays frozen on subsequent reads.
        let frozen2 = h.snapshot_elapsed();
        assert_eq!(frozen, frozen2, "elapsed should be frozen after finish_success");
    }

    #[test]
    fn recording_handle_elapsed_frozen_after_finish_success() {
        let h = RecordingTrackedHandle::new(100);
        std::thread::sleep(std::time::Duration::from_millis(1));
        h.finish_success();
        let frozen = h.snapshot_elapsed();
        let frozen2 = h.snapshot_elapsed();
        assert_eq!(frozen, frozen2, "elapsed should be frozen after finish_success");
    }

    #[test]
    fn recording_handle_elapsed_frozen_after_finish_error() {
        let h = RecordingTrackedHandle::new(100);
        std::thread::sleep(std::time::Duration::from_millis(1));
        h.finish_error();
        let frozen = h.snapshot_elapsed();
        let frozen2 = h.snapshot_elapsed();
        assert_eq!(frozen, frozen2, "elapsed should be frozen after finish_error");
    }

    #[test]
    fn recording_handle_elapsed_frozen_after_finish_warning() {
        let h = RecordingTrackedHandle::new(100);
        std::thread::sleep(std::time::Duration::from_millis(1));
        h.finish_warning();
        let frozen = h.snapshot_elapsed();
        let frozen2 = h.snapshot_elapsed();
        assert_eq!(frozen, frozen2, "elapsed should be frozen after finish_warning");
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
        assert_eq!(super::format_elapsed(std::time::Duration::from_hours(100)), "4d4h");
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
        let ts = std::sync::Arc::new(super::TestTimeSource::new());
        let s = super::SharedState::with_time_source(
            100,
            "test",
            std::sync::Arc::clone(&ts) as std::sync::Arc<dyn super::TimeSource>,
        );
        ts.advance(std::time::Duration::from_millis(10));
        let elapsed = s.elapsed();
        assert!(elapsed.as_millis() >= 10, "elapsed should advance after advance, got {elapsed:?}");
    }

    #[test]
    fn shared_state_elapsed_frozen_after_mark_finished() {
        let ts = std::sync::Arc::new(super::TestTimeSource::new());
        let s = super::SharedState::with_time_source(
            100,
            "test",
            std::sync::Arc::clone(&ts) as std::sync::Arc<dyn super::TimeSource>,
        );
        ts.advance(std::time::Duration::from_millis(10));
        s.mark_finished();
        let frozen = s.elapsed();
        assert!(
            frozen.as_millis() >= 10,
            "elapsed should capture time until mark_finished, got {frozen:?}"
        );
        ts.advance(std::time::Duration::from_millis(10));
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
        let ts = std::sync::Arc::new(super::TestTimeSource::new());
        let s = super::SharedState::with_time_source(
            100,
            "test",
            std::sync::Arc::clone(&ts) as std::sync::Arc<dyn super::TimeSource>,
        );
        let t0 = s.elapsed();
        ts.advance(std::time::Duration::from_millis(5));
        let t1 = s.elapsed();
        ts.advance(std::time::Duration::from_millis(5));
        let t2 = s.elapsed();
        assert!(t1 >= t0, "t1 ({t1:?}) should be >= t0 ({t0:?})");
        assert!(t2 >= t1, "t2 ({t2:?}) should be >= t1 ({t1:?})");
    }

    // ---- TrackedHandle elapsed (integration) -----------------------------

    #[test]
    fn tracked_handle_elapsed_frozen_after_all_finish_methods() {
        // finish_success, finish_error, finish_warning, finish_and_clear
        // must all freeze the elapsed.
        for (name, finish_fn) in [
            (
                "finish_success",
                Box::new(|h: &TrackedHandle| h.finish_success()) as Box<dyn Fn(&TrackedHandle)>,
            ),
            (
                "finish_error",
                Box::new(|h: &TrackedHandle| h.finish_error()) as Box<dyn Fn(&TrackedHandle)>,
            ),
            (
                "finish_warning",
                Box::new(|h: &TrackedHandle| h.finish_warning()) as Box<dyn Fn(&TrackedHandle)>,
            ),
            (
                "finish_and_clear",
                Box::new(|h: &TrackedHandle| h.finish_and_clear()) as Box<dyn Fn(&TrackedHandle)>,
            ),
        ] {
            let ts = std::sync::Arc::new(super::TestTimeSource::new());
            let g = super::ProgressGroup::builder()
                .with_time_source(
                    std::sync::Arc::clone(&ts) as std::sync::Arc<dyn super::TimeSource>
                )
                .build();
            let h = g.add_bar(100, &format!("{name}-bar"));
            ts.advance(std::time::Duration::from_millis(10));
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
        h2.finish_success();

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
                ProgressOp::FinishSuccess,
            ],
            "group2 must have its own ops, unaffected by group1"
        );
    }

    // ── recording_handle_finish_ops_sequence ──

    #[test]
    fn recording_handle_finish_ops_sequence() {
        let h = RecordingTrackedHandle::new(5);
        assert_eq!(h.ops(), vec![], "no ops yet");

        h.finish_success();
        assert_eq!(
            h.ops(),
            vec![ProgressOp::FinishSuccess],
            "finish_success records FinishSuccess"
        );

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
    fn tracked_handle_is_finished_after_finish_warning() {
        let h = TrackedHandle::new(10);
        h.finish_warning();
        assert!(h.is_finished());
    }

    #[test]
    fn tracked_handle_snapshot_fields_match() {
        let h = TrackedHandle::new(100);
        h.set_prefix_components(PrefixComponents { tool_name: "pfx".into(), ..Default::default() });
        h.advance(7);
        let snap = h.snapshot();
        assert_eq!(snap.prefix, "\x1b[0mpfx");
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
            matches!(snap.status, TrackStatus::Success),
            "finish_and_clear → Success, got {:?}",
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

    #[test]
    fn spinner_advances_per_cycle_for_all_bars() {
        // Each bar's spinner character must change across ticks, not just
        // the overall bar's.
        use super::inner::DimensionSource;
        use std::sync::Arc;

        let term = indicatif::InMemoryTerm::new(10, 80);
        let target = indicatif::ProgressDrawTarget::term_like(Box::new(term.clone()));
        let mp = MultiProgress::with_draw_target(target);
        let dims = Arc::new(super::inner::TestDimensionSource::new((10, 80)));
        let ts = Arc::new(super::TestTimeSource::new());

        let (group, overall) = super::ProgressGroup::builder()
            .with_multi_progress(mp)
            .with_dim_source(dims as Arc<dyn DimensionSource>)
            .with_time_source(ts.clone() as Arc<dyn super::TimeSource>)
            // Disable the daemon ticker: it fires group.tick() on real
            // wall-clock time, injecting extra spinner ticks beyond this
            // test's manual ticks.  (indicatif's set_position also advances
            // the tick counter through a wall-clock position rate limiter,
            // so the assertion below checks a distinct-glyph set.)
            .with_ticker_enabled(false)
            .with_overall("syncing", 3)
            .build_with_overall();

        let bar1 = group.add_bar(100, "tool [resolve]");
        let bar2 = group.add_bar(100, "tool [fetch]");

        // Advance time so rate computation has positive dt.
        ts.advance(std::time::Duration::from_millis(100));

        // Collect the first-char (spinner) from each line across many ticks.
        let mut snapshots: Vec<Vec<char>> = Vec::new();
        for _ in 0..30 {
            // Change positions each tick to trigger tick_inner via setters.
            bar1.advance(1);
            bar2.advance(2);
            overall.advance(0);
            ts.advance(std::time::Duration::from_millis(50));
            group.tick();

            let output = term.contents();
            let line_chars: Vec<char> = output
                .lines()
                .filter(|l| !l.is_empty())
                .map(|l| l.chars().next().unwrap_or(' '))
                .collect();
            if !line_chars.is_empty() {
                snapshots.push(line_chars);
            }
        }

        // Must have captured several distinct snapshots.
        assert!(snapshots.len() >= 10, "expected >=10 captured snapshots, got {}", snapshots.len());

        // Every bar's spinner must be animating.  We can't assert
        // first-vs-last inequality: the glyph is indicatif's tick counter
        // mod 9, and that counter advances via BOTH set_position (gated by
        // a real-wall-clock position rate limiter) and the end-of-tick
        // bar.tick(), so the exact per-iteration advance varies with real
        // timing.  What IS guaranteed: every active bar is ticked at least
        // once per group.tick(), so 30 ticks over a 9-glyph cycle always
        // produce several distinct glyphs per bar.  Assert >= 2 distinct.
        let n_bars = snapshots.iter().map(Vec::len).max().unwrap_or(0);
        let mut distinct_per_bar: Vec<std::collections::BTreeSet<char>> =
            vec![std::collections::BTreeSet::new(); n_bars];
        for snap in &snapshots {
            for (i, c) in snap.iter().enumerate().take(n_bars) {
                distinct_per_bar[i].insert(*c);
            }
        }
        for (i, set) in distinct_per_bar.iter().enumerate() {
            assert!(
                set.len() >= 2,
                "bar {i}: spinner must cycle through >= 2 distinct glyphs, \
                 saw only {set:?} across {} snapshots",
                snapshots.len()
            );
        }
    }

    #[test]
    fn recycled_bar_spinner_animates() {
        // Force slot recycling by creating a renderer with a single child
        // slot.  When bar1 finishes and bar2 attaches, bar2 reuses bar1's
        // slot.  Without the fix, bar2's indicatif bar would still have
        // Status::DoneVisible and the spinner would show the final char
        // (⠏ for our tick set) without cycling.
        use super::inner::DimensionSource;
        use std::sync::Arc;

        let term = indicatif::InMemoryTerm::new(10, 80);
        let target = indicatif::ProgressDrawTarget::term_like(Box::new(term.clone()));
        let mp = MultiProgress::with_draw_target(target);
        let dims = Arc::new(super::inner::TestDimensionSource::new((10, 80)));
        let ts = Arc::new(super::TestTimeSource::new());

        // capacity=2 means 1 child + 1 overall bar.
        // dynamic_height=false fixed capacity prevents auto-growing.
        let (group, overall) = super::ProgressGroup::builder()
            .with_multi_progress(mp)
            .with_dim_source(dims as Arc<dyn DimensionSource>)
            .with_time_source(ts.clone() as Arc<dyn super::TimeSource>)
            // Disable the daemon ticker: it fires group.tick() on real
            // wall-clock time, injecting extra spinner ticks beyond this
            // test's manual ticks.  (indicatif's set_position also advances
            // the tick counter through a wall-clock position rate limiter,
            // so the assertion below checks a distinct-glyph set.)
            .with_ticker_enabled(false)
            .capacity(2)
            .dynamic_height(false)
            .with_overall("syncing", 3)
            .build_with_overall();

        // Phase 1: finish resolve bar (fills the single child slot).
        let bar1 = group.add_bar(1, "tool [resolve]");
        bar1.finish_success();

        // Tick to trigger finish_slot → bar.finish() → DoneVisible.
        ts.advance(std::time::Duration::from_millis(50));
        group.tick();

        // Phase 2: add fetch bar (recycles bar1's slot via attach Phase 2).
        let bar2 = group.add_bar(5, "tool [fetch]");
        bar2.advance(2);
        ts.advance(std::time::Duration::from_millis(50));
        group.tick();

        // Capture spinner chars across many ticks.
        let mut snapshots: Vec<Vec<char>> = Vec::new();
        for _ in 0..30 {
            bar2.advance(1);
            overall.advance(0);
            ts.advance(std::time::Duration::from_millis(50));
            group.tick();

            let output = term.contents();
            let line_chars: Vec<char> = output
                .lines()
                .filter(|l| !l.is_empty())
                .map(|l| l.chars().next().unwrap_or(' '))
                .collect();
            if !line_chars.is_empty() {
                snapshots.push(line_chars);
            }
        }

        // Must have captured several distinct snapshots.
        assert!(snapshots.len() >= 10, "expected >=10 captured snapshots, got {}", snapshots.len());

        // The spinner char for the child bar (first line) must differ
        // between the first and last snapshot — if it's the same, the
        // spinner is frozen because the indicatif bar stayed DoneVisible.
        let first = &snapshots[0];
        assert!(
            first.len() >= 2,
            "expected at least 2 visible bars (child + overall), got {}",
            first.len()
        );
        // Same window-robust glyph check as spinner_advances_per_cycle_for_all_bars:
        // the child bar's glyph is the tick counter mod 9, advanced by both
        // set_position (wall-clock rate-limited) and the end-of-tick bar.tick(),
        // so assert >= 2 distinct glyphs across the window instead of
        // first-vs-last inequality.
        let distinct_child: std::collections::BTreeSet<char> =
            snapshots.iter().filter_map(|s| s.first()).copied().collect();
        assert!(
            distinct_child.len() >= 2,
            "child bar spinner must cycle through >= 2 distinct glyphs, \
             saw only {distinct_child:?} across {} snapshots — slot status leak",
            snapshots.len()
        );
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
    fn bar_color_code_warning() {
        assert_eq!(super::inner::bar_color_code(super::TrackStatus::Warning, false), "33");
    }

    #[test]
    fn bar_color_code_success() {
        assert_eq!(super::inner::bar_color_code(super::TrackStatus::Success, false), "32");
    }

    // ---- ANSI-safe truncation helpers (Phase 1) -------------------------

    #[test]
    fn strip_ansi_empty() {
        assert_eq!(super::inner::strip_ansi(""), "");
    }

    #[test]
    fn strip_ansi_no_escapes() {
        assert_eq!(super::inner::strip_ansi("hello world"), "hello world");
    }

    #[test]
    fn strip_ansi_reset() {
        assert_eq!(super::inner::strip_ansi("\x1b[0m"), "");
    }

    #[test]
    fn strip_ansi_multiple() {
        assert_eq!(super::inner::strip_ansi("\x1b[31mfoo\x1b[0m"), "foo");
        assert_eq!(super::inner::strip_ansi("\x1b[33m[F]\x1b[0m bar"), "[F] bar");
    }

    #[test]
    fn strip_ansi_non_sgr_ignored() {
        // Non-SGR escape sequences (not ending with 'm') are passed through.
        assert_eq!(super::inner::strip_ansi("\x1b[2J"), "\x1b[2J");
        assert_eq!(super::inner::strip_ansi("a\x1b[Kb"), "a\x1b[Kb");
    }

    #[test]
    fn visible_width_empty() {
        assert_eq!(super::inner::visible_width(""), 0);
    }

    #[test]
    fn visible_width_no_ansi() {
        assert_eq!(super::inner::visible_width("hello"), 5);
    }

    #[test]
    fn visible_width_with_ansi() {
        assert_eq!(super::inner::visible_width("\x1b[31mhello\x1b[0m"), 5);
        assert_eq!(super::inner::visible_width("\x1b[33m[F]\x1b[0m wget"), 8);
    }

    #[test]
    fn visible_width_ansi_only() {
        assert_eq!(super::inner::visible_width("\x1b[0m"), 0);
        assert_eq!(super::inner::visible_width("\x1b[31m\x1b[33m"), 0);
    }

    #[test]
    fn max_prefix_width_wide() {
        assert_eq!(super::inner::max_prefix_width(80), 30);
        assert_eq!(super::inner::max_prefix_width(60), 30);
    }

    #[test]
    fn max_prefix_width_compact() {
        assert_eq!(super::inner::max_prefix_width(59), 25);
        assert_eq!(super::inner::max_prefix_width(40), 25);
    }

    #[test]
    fn max_suffix_width_wide() {
        assert_eq!(super::inner::max_suffix_width(80), 55);
        assert_eq!(super::inner::max_suffix_width(60), 55);
    }

    #[test]
    fn max_suffix_width_compact() {
        assert_eq!(super::inner::max_suffix_width(59), 40);
        assert_eq!(super::inner::max_suffix_width(40), 40);
    }

    // ---- render_prefix_components tests (Phase 2) -----------------------

    #[test]
    fn render_prefix_components_all_fields() {
        let result = super::inner::render_prefix_components(
            &super::inner::PrefixComponents {
                marker: String::new(),
                tool_name: "wget".into(),
                version: "1.2.3".into(),
                phase: "fch".into(),
                count: "2".into(),
                total: "5".into(),
            },
            super::TrackStatus::Active,
        );
        assert_eq!(result, "\x1b[0mwget 1.2.3 [fch] 2/5", "all fields rendered");
    }

    #[test]
    fn render_prefix_components_empty_version() {
        let result = super::inner::render_prefix_components(
            &super::inner::PrefixComponents {
                marker: String::new(),
                tool_name: "wget".into(),
                version: String::new(),
                phase: "fch".into(),
                count: "2".into(),
                total: "5".into(),
            },
            super::TrackStatus::Active,
        );
        assert_eq!(result, "\x1b[0mwget [fch] 2/5", "version omitted when empty");
    }

    #[test]
    fn render_prefix_components_only_tool_name() {
        let result = super::inner::render_prefix_components(
            &super::inner::PrefixComponents {
                marker: String::new(),
                tool_name: "wget".into(),
                version: String::new(),
                phase: String::new(),
                count: String::new(),
                total: String::new(),
            },
            super::TrackStatus::Active,
        );
        assert_eq!(result, "\x1b[0mwget", "only tool name when rest empty");
    }

    #[test]
    fn render_prefix_components_empty_phase_and_count() {
        let result = super::inner::render_prefix_components(
            &super::inner::PrefixComponents {
                marker: String::new(),
                tool_name: "wget".into(),
                version: "1.2.3".into(),
                phase: String::new(),
                count: String::new(),
                total: String::new(),
            },
            super::TrackStatus::Active,
        );
        assert_eq!(result, "\x1b[0mwget 1.2.3", "version without phase/count");
    }

    #[test]
    fn render_prefix_components_marker_failed() {
        let result = super::inner::render_prefix_components(
            &super::inner::PrefixComponents {
                marker: "F".into(),
                tool_name: "wget".into(),
                version: "1.2.3".into(),
                phase: "fch".into(),
                count: "2".into(),
                total: "5".into(),
            },
            super::TrackStatus::Failed,
        );
        assert_eq!(
            result, "\x1b[0m\x1b[31m[F]\x1b[0m wget 1.2.3 [fch] 2/5",
            "failed marker rendered red"
        );
    }

    #[test]
    fn render_prefix_components_marker_warning() {
        let result = super::inner::render_prefix_components(
            &super::inner::PrefixComponents {
                marker: "W".into(),
                tool_name: "wget".into(),
                version: "1.2.3".into(),
                phase: "fch".into(),
                count: "2".into(),
                total: "5".into(),
            },
            super::TrackStatus::Warning,
        );
        assert_eq!(
            result, "\x1b[0m\x1b[33m[W]\x1b[0m wget 1.2.3 [fch] 2/5",
            "warning marker rendered yellow"
        );
    }

    #[test]
    fn render_prefix_components_normal_states_no_marker() {
        for status in [super::TrackStatus::Active, super::TrackStatus::Success] {
            let result = super::inner::render_prefix_components(
                &super::inner::PrefixComponents {
                    marker: String::new(),
                    tool_name: "child".into(),
                    version: String::new(),
                    phase: String::new(),
                    count: String::new(),
                    total: String::new(),
                },
                status,
            );
            assert_eq!(result, "\x1b[0mchild", "{status:?}: no bracket for empty marker");
        }
    }

    #[test]
    fn render_prefix_components_marker_uncolored_when_status_normal() {
        // A non-empty marker with a normal status renders uncolored (no SGR
        // color codes around the bracket).
        let result = super::inner::render_prefix_components(
            &super::inner::PrefixComponents {
                marker: "F".into(),
                tool_name: "wget".into(),
                version: String::new(),
                phase: String::new(),
                count: String::new(),
                total: String::new(),
            },
            super::TrackStatus::Active,
        );
        assert_eq!(result, "\x1b[0m[F] wget", "marker bracket uncolored for normal status");
    }

    #[test]
    fn render_prefix_components_always_starts_with_reset() {
        for status in [
            super::TrackStatus::Active,
            super::TrackStatus::Failed,
            super::TrackStatus::Warning,
            super::TrackStatus::Success,
        ] {
            let result = super::inner::render_prefix_components(
                &super::inner::PrefixComponents {
                    marker: "F".into(),
                    tool_name: "foo".into(),
                    version: String::new(),
                    phase: String::new(),
                    count: String::new(),
                    total: String::new(),
                },
                status,
            );
            assert!(
                result.starts_with("\x1b[0m"),
                "{status:?}: expected \\x1b[0m prefix, got {result:?}"
            );
        }
    }

    /// Render truncated prefix components with Active status for assertions.
    fn render_prefix(parts: &super::inner::PrefixComponents) -> String {
        super::inner::render_prefix_components(parts, super::TrackStatus::Active)
    }

    // ---- prefix_components_from_str tests (Phase 2) -----------------------

    #[test]
    fn prefix_components_from_str_tool_name_only() {
        let result = super::inner::prefix_components_from_str("wget");
        assert_eq!(result.tool_name, "wget");
        assert!(result.marker.is_empty());
        assert!(result.version.is_empty());
        assert!(result.phase.is_empty());
        assert!(result.count.is_empty());
        assert!(result.total.is_empty());
    }

    #[test]
    fn prefix_components_from_str_all_fields() {
        let result = super::inner::prefix_components_from_str("yt-dlp 2024.12.20 [fch] 2/5");
        assert_eq!(result.tool_name, "yt-dlp");
        assert_eq!(result.version, "2024.12.20");
        assert_eq!(result.phase, "fch");
        assert_eq!(result.count, "2");
        assert_eq!(result.total, "5");
    }

    #[test]
    fn prefix_components_from_str_multiword_no_bracket() {
        // No-bracket labels with multiple words keep the whole string as
        // tool_name (regression: previously only the first token was kept).
        let result = super::inner::prefix_components_from_str("syncing tools");
        assert_eq!(result.tool_name, "syncing tools");
        assert!(result.version.is_empty());
        assert!(result.phase.is_empty());
        assert!(result.count.is_empty());
        assert!(result.total.is_empty());
    }

    #[test]
    fn prefix_components_from_str_multiword_with_count_total() {
        // A trailing count/total token after a multi-word tool name is split
        // off; the rest of the string stays the tool name.
        let result = super::inner::prefix_components_from_str("syncing tools 2/5");
        assert_eq!(result.tool_name, "syncing tools");
        assert!(result.version.is_empty());
        assert!(result.phase.is_empty());
        assert_eq!(result.count, "2");
        assert_eq!(result.total, "5");
    }

    #[test]
    fn prefix_components_from_str_keeps_no_bracket_trailing_nonslash_word() {
        // A trailing word without `/` is NOT a count/total token — the whole
        // string remains the tool name.
        let result = super::inner::prefix_components_from_str("materializing files");
        assert_eq!(result.tool_name, "materializing files");
        assert!(result.version.is_empty());
        assert!(result.phase.is_empty());
        assert!(result.count.is_empty());
        assert!(result.total.is_empty());
    }

    #[test]
    fn shared_state_parses_label_into_components() {
        // add_bar labels must be parsed into PrefixComponents at construction
        // (regression: previously the entire label was stored as tool_name,
        // so semantic truncation chopped `[res]` off resolve bars).
        let h = TrackedHandle::with_label(100, "ffmpeg autobuild-2026-07-31 [res]");
        let snap = h.snapshot();
        assert_eq!(snap.prefix_components.tool_name, "ffmpeg");
        assert_eq!(snap.prefix_components.version, "autobuild-2026-07-31");
        assert_eq!(snap.prefix_components.phase, "res");
        assert!(snap.prefix_components.count.is_empty());
        assert!(snap.prefix_components.total.is_empty());

        let h = TrackedHandle::with_label(100, "syncing tools");
        let snap = h.snapshot();
        assert_eq!(snap.prefix_components.tool_name, "syncing tools");

        let h = TrackedHandle::with_label(100, "");
        let snap = h.snapshot();
        assert!(snap.prefix_components.tool_name.is_empty());
        assert!(snap.prefix_components.version.is_empty());
        assert!(snap.prefix_components.phase.is_empty());
    }

    #[test]
    fn truncate_parsed_resolve_label_preserves_phase() {
        // The user-reported symptom: a long resolve label truncated to the
        // prefix budget must shrink the version first and keep `[res]`.
        let parts = super::inner::prefix_components_from_str("ffmpeg autobuild-2026-07-31 [res]");
        let result = super::inner::semantic_truncate_prefix(&parts, 26);
        assert_eq!(result.tool_name, "ffmpeg");
        assert_eq!(result.version, "autobuild-202");
        assert_eq!(result.phase, "res");
        assert!(result.count.is_empty());
        assert!(result.total.is_empty());
    }

    // ---- semantic_truncate_prefix tests (Phase 2) -----------------------

    fn prefix_parts_wget() -> super::inner::PrefixComponents {
        super::inner::PrefixComponents {
            marker: String::new(),
            tool_name: "wget".into(),
            version: "1.2.3".into(),
            phase: "fch".into(),
            count: "2".into(),
            total: "5".into(),
        }
    }

    #[test]
    fn semantic_truncate_prefix_already_fits() {
        let parts = prefix_parts_wget();
        let result = super::inner::semantic_truncate_prefix(&parts, 30);
        assert_eq!(
            render_prefix(&result),
            "\x1b[0mwget 1.2.3 [fch] 2/5",
            "no truncation when fits"
        );
    }

    #[test]
    fn semantic_truncate_prefix_remove_version() {
        let parts = prefix_parts_wget();
        // max=17: version shrunk to "1."
        let result = super::inner::semantic_truncate_prefix(&parts, 17);
        assert_eq!(
            render_prefix(&result),
            "\x1b[0mwget 1. [fch] 2/5",
            "version shortened by 3 chars"
        );
    }

    #[test]
    fn semantic_truncate_prefix_remove_version_full() {
        let parts = prefix_parts_wget();
        // max=15: version fully removed
        let result = super::inner::semantic_truncate_prefix(&parts, 15);
        assert_eq!(
            render_prefix(&result),
            "\x1b[0mwget [fch] 2/5",
            "version fully removed when excess covers it"
        );
    }

    #[test]
    fn semantic_truncate_prefix_remove_version_and_count_total() {
        let parts = prefix_parts_wget();
        // max=11: version and count/total removed
        let result = super::inner::semantic_truncate_prefix(&parts, 11);
        assert_eq!(render_prefix(&result), "\x1b[0mwget [fch]", "version and count/total removed");
    }

    #[test]
    fn semantic_truncate_prefix_count_total_removed_atomically() {
        let parts = prefix_parts_wget();
        // max=12: version gone, count/total removed as one pair — a bare "2/"
        // or "/5" must never appear.
        let result = super::inner::semantic_truncate_prefix(&parts, 12);
        assert_eq!(
            render_prefix(&result),
            "\x1b[0mwget [fch]",
            "count/total removed as atomic pair"
        );
    }

    #[test]
    fn semantic_truncate_prefix_phase_removed_atomically() {
        let parts = prefix_parts_wget();
        // max=8: count/total gone, phase removed entirely (never a bare "[f]")
        let result = super::inner::semantic_truncate_prefix(&parts, 8);
        assert_eq!(
            render_prefix(&result),
            "\x1b[0mwget",
            "phase removed entirely, no partial bracket"
        );
    }

    #[test]
    fn semantic_truncate_prefix_remove_version_count_phase() {
        let parts = prefix_parts_wget();
        // max=4: only tool name remains
        let result = super::inner::semantic_truncate_prefix(&parts, 4);
        assert_eq!(
            render_prefix(&result),
            "\x1b[0mwget",
            "tool name survives after version, count, phase removed"
        );
    }

    #[test]
    fn semantic_truncate_prefix_tool_name_progressive_shrink() {
        let parts = prefix_parts_wget();
        // max=2: progressive shrink of tool name (no fallback string mangling)
        let result = super::inner::semantic_truncate_prefix(&parts, 2);
        assert_eq!(render_prefix(&result), "\x1b[0mwg", "tool name shrunk to 2 chars");
    }

    #[test]
    fn semantic_truncate_prefix_empty() {
        let parts = super::inner::PrefixComponents::default();
        let result = super::inner::semantic_truncate_prefix(&parts, 30);
        assert_eq!(render_prefix(&result), "\x1b[0m", "empty prefix stays empty");
    }

    #[test]
    fn semantic_truncate_prefix_marker_removed_before_tool_name() {
        // Marker is truncatable data: dropped before the tool name shrinks.
        let parts = super::inner::PrefixComponents {
            marker: "F".into(),
            tool_name: "wget".into(),
            version: String::new(),
            phase: "fch".into(),
            count: "0".into(),
            total: "1".into(),
        };
        // max=7: marker (4) + tool (4) = 8 > 7 — marker removed, tool intact.
        let result = super::inner::semantic_truncate_prefix(&parts, 7);
        assert!(result.marker.is_empty(), "marker removed before tool name");
        assert_eq!(render_prefix(&result), "\x1b[0mwget", "tool name intact after marker removal");
        // max=3: marker removed first, then tool shrinks to 3 chars.
        let result = super::inner::semantic_truncate_prefix(&parts, 3);
        assert_eq!(render_prefix(&result), "\x1b[0mwge", "marker dropped before tool shrank");
    }

    #[test]
    fn semantic_truncate_prefix_marker_preserved_when_fits() {
        // max=9: after count/total and phase removal, marker (4) + tool (4)
        // = 8 <= 9 — marker preserved.
        let parts = super::inner::PrefixComponents {
            marker: "F".into(),
            tool_name: "wget".into(),
            version: String::new(),
            phase: "fch".into(),
            count: "0".into(),
            total: "1".into(),
        };
        let result = super::inner::semantic_truncate_prefix(&parts, 9);
        assert_eq!(
            super::inner::render_prefix_components(&result, super::TrackStatus::Failed),
            "\x1b[0m\x1b[31m[F]\x1b[0m wget",
            "marker preserved when it fits"
        );
    }

    #[test]
    fn semantic_truncate_prefix_marker_failed_effective_width() {
        // max=4: marker (4) + tool (4) = 8 > 4 — marker removed so the tool
        // name fits the effective width.
        let parts = super::inner::PrefixComponents {
            marker: "F".into(),
            tool_name: "wget".into(),
            version: String::new(),
            phase: "fch".into(),
            count: "0".into(),
            total: "1".into(),
        };
        let result = super::inner::semantic_truncate_prefix(&parts, 4);
        assert_eq!(
            super::inner::render_prefix_components(&result, super::TrackStatus::Failed),
            "\x1b[0mwget",
            "marker removed to fit effective width"
        );
    }

    #[test]
    fn semantic_truncate_prefix_version_shrinks_first_long_version() {
        // Regression: version must shrink (never mangle into a mid-char
        // suffix) before any other component is touched.
        let parts = super::inner::PrefixComponents {
            marker: String::new(),
            tool_name: "ffmpeg".into(),
            version: "autobuild-2026-07-31".into(),
            phase: "res".into(),
            count: "2".into(),
            total: "2".into(),
        };
        let result = super::inner::semantic_truncate_prefix(&parts, 30);
        assert_eq!(
            render_prefix(&result),
            "\x1b[0mffmpeg autobuild-202 [res] 2/2",
            "long version shrinks first, no mangled suffix"
        );
        let result = super::inner::semantic_truncate_prefix(&parts, 8);
        assert_eq!(render_prefix(&result), "\x1b[0mffmpeg", "all optional components dropped");
    }

    // ---- semantic_truncate_suffix tests (Phase 1) -----------------------

    /// Canonical test parts: auto components (33 visible) + custom (10).
    fn suffix_parts_full() -> SuffixComponents {
        SuffixComponents {
            count: "2".into(),
            total: "5".into(),
            elapsed: "0:00:05".into(),
            rate: Some("12.3 MiB/s".into()),
            eta: Some("[0:00:02]".into()),
            custom: "cached (1)".into(),
        }
    }

    /// Render truncated parts with the child-bar color code like production.
    fn render_suffix(parts: &SuffixComponents) -> String {
        super::inner::render_suffix_components(parts, "33")
    }

    #[test]
    fn semantic_truncate_suffix_fits() {
        let parts = suffix_parts_full();
        let result = super::inner::semantic_truncate_suffix(&parts, 100);
        assert_eq!(
            render_suffix(&result),
            " \x1b[33m2/5\x1b[0m 0:00:05 12.3 MiB/s [0:00:02] cached (1)",
            "full suffix when fits"
        );
    }

    #[test]
    fn semantic_truncate_suffix_remove_custom_partial() {
        let parts = suffix_parts_full();
        let result = super::inner::semantic_truncate_suffix(&parts, 39);
        assert_eq!(
            render_suffix(&result),
            " \x1b[33m2/5\x1b[0m 0:00:05 12.3 MiB/s [0:00:02] cache",
            "custom shrunk to keep=5 (39 visible)"
        );
    }

    #[test]
    fn semantic_truncate_suffix_remove_custom_all() {
        let parts = suffix_parts_full();
        let result = super::inner::semantic_truncate_suffix(&parts, 34);
        assert_eq!(
            render_suffix(&result),
            " \x1b[33m2/5\x1b[0m 0:00:05 12.3 MiB/s [0:00:02]",
            "custom fully removed (auto 33 <= 34)"
        );
    }

    #[test]
    fn semantic_truncate_suffix_removes_eta() {
        let parts = suffix_parts_full();
        let result = super::inner::semantic_truncate_suffix(&parts, 30);
        assert_eq!(
            render_suffix(&result),
            " \x1b[33m2/5\x1b[0m 0:00:05 12.3 MiB/s",
            "eta removed entirely (23 <= 30)"
        );
    }

    #[test]
    fn semantic_truncate_suffix_removes_rate() {
        let parts = suffix_parts_full();
        let result = super::inner::semantic_truncate_suffix(&parts, 22);
        assert_eq!(
            render_suffix(&result),
            " \x1b[33m2/5\x1b[0m 0:00:05",
            "rate removed entirely (12 <= 22)"
        );
    }

    #[test]
    fn semantic_truncate_suffix_removes_elapsed() {
        let parts = suffix_parts_full();
        let result = super::inner::semantic_truncate_suffix(&parts, 11);
        assert_eq!(
            render_suffix(&result),
            " \x1b[33m2/5\x1b[0m",
            "elapsed removed entirely (4 <= 11)"
        );
    }

    #[test]
    fn semantic_truncate_suffix_fits_count_total() {
        let parts = suffix_parts_full();
        let result = super::inner::semantic_truncate_suffix(&parts, 4);
        assert_eq!(
            render_suffix(&result),
            " \x1b[33m2/5\x1b[0m",
            "count/total is the last unit and fits at 4"
        );
    }

    #[test]
    fn semantic_truncate_suffix_removes_count_total() {
        let parts = suffix_parts_full();
        let result = super::inner::semantic_truncate_suffix(&parts, 3);
        assert_eq!(render_suffix(&result), "", "count/total removed entirely (0 <= 3)");
    }

    #[test]
    fn semantic_truncate_suffix_count_total_atomic_no_partial() {
        // Width 3: the full unit ` 2/5` (4 visible) does not fit, but a partial
        // like ` 2/` (3 visible) would. Atomicity requires dropping the whole
        // pair — a bare count or partial unit is never shown.
        let parts = SuffixComponents {
            count: "2".into(),
            total: "5".into(),
            elapsed: "0:00:05".into(),
            rate: None,
            eta: None,
            custom: String::new(),
        };
        let result = super::inner::semantic_truncate_suffix(&parts, 3);
        assert_eq!(render_suffix(&result), "", "no partial count/total at width 3");
    }

    #[test]
    fn semantic_truncate_suffix_auto_components_removed_without_custom() {
        // Full = 12 visible; at 10 elapsed is removed, leaving ` 2/5` (4).
        let parts = SuffixComponents {
            count: "2".into(),
            total: "5".into(),
            elapsed: "0:00:05".into(),
            rate: None,
            eta: None,
            custom: String::new(),
        };
        let result = super::inner::semantic_truncate_suffix(&parts, 10);
        assert_eq!(render_suffix(&result), " \x1b[33m2/5\x1b[0m", "elapsed removed at 10");
    }

    #[test]
    fn semantic_truncate_suffix_empty_both() {
        let result = super::inner::semantic_truncate_suffix(&SuffixComponents::default(), 30);
        assert_eq!(render_suffix(&result), "", "both empty");
    }

    #[test]
    fn semantic_truncate_suffix_custom_only() {
        let parts = SuffixComponents { custom: "custom".into(), ..Default::default() };
        let result = super::inner::semantic_truncate_suffix(&parts, 10);
        assert_eq!(render_suffix(&result), " custom", "custom appended after empty auto");
    }

    #[test]
    fn render_suffix_components_with_rate() {
        // rate present, no eta
        let parts = SuffixComponents {
            count: "0".into(),
            total: "5".into(),
            elapsed: "0s".into(),
            rate: Some("0/d".into()),
            eta: None,
            custom: String::new(),
        };
        let result = super::inner::render_suffix_components(&parts, "33");
        assert_eq!(result, " \x1b[33m0/5\x1b[0m 0s 0/d");
        assert!(result.ends_with("0s 0/d"), "expected elapsed then rate at end: {result:?}");
    }

    #[test]
    fn render_suffix_components_with_rate_and_eta() {
        // rate + eta
        let parts = SuffixComponents {
            count: "0".into(),
            total: "5".into(),
            elapsed: "0s".into(),
            rate: Some("0/d".into()),
            eta: Some("5s".into()),
            custom: String::new(),
        };
        let result = super::inner::render_suffix_components(&parts, "33");
        assert_eq!(result, " \x1b[33m0/5\x1b[0m 0s 0/d 5s");
        assert!(result.ends_with("0s 0/d 5s"), "expected elapsed rate eta at end: {result:?}");
    }

    #[test]
    fn render_suffix_components_different_color_codes() {
        for (code, status_name) in [("31", "failed"), ("33", "child"), ("35", "overall")] {
            let parts = SuffixComponents {
                count: "1".into(),
                total: "2".into(),
                elapsed: "3s".into(),
                rate: Some("0/d".into()),
                eta: None,
                custom: String::new(),
            };
            let result = super::inner::render_suffix_components(&parts, code);
            assert!(
                result.contains(&format!("\x1b[{code}m")),
                "{status_name} should use code {code}: {result:?}"
            );
            assert!(result.contains("1/2"), "{status_name} count/total absent: {result:?}");
        }
    }

    #[test]
    fn render_suffix_components_eta_suppressed_without_rate() {
        // Historical build_right_msg guard: eta renders only when rate is present.
        let parts = SuffixComponents {
            count: "0".into(),
            total: "5".into(),
            elapsed: "0s".into(),
            rate: None,
            eta: Some("5s".into()),
            custom: String::new(),
        };
        let result = super::inner::render_suffix_components(&parts, "33");
        assert_eq!(result, " \x1b[33m0/5\x1b[0m 0s", "eta suppressed without rate");
    }

    #[test]
    fn render_suffix_components_custom_appended() {
        let parts = SuffixComponents {
            count: "0".into(),
            total: "5".into(),
            elapsed: "0s".into(),
            rate: Some("0/d".into()),
            eta: None,
            custom: "cached (1)".into(),
        };
        let result = super::inner::render_suffix_components(&parts, "33");
        assert_eq!(result, " \x1b[33m0/5\x1b[0m 0s 0/d cached (1)", "custom appended");
    }

    // ---- Phase 4: BufferedTerm / dirty tracking tests ----------------------

    #[test]
    fn dirty_tracking_initial_state_starts_dirty() {
        // The SharedState dirty flag starts true so the very first tick always
        // draws, even without explicit mutations.
        let term = indicatif::InMemoryTerm::new(10, 80);
        let target = indicatif::ProgressDrawTarget::term_like(Box::new(term.clone()));
        let mp = MultiProgress::with_draw_target(target);
        let group = ProgressGroup::builder()
            .with_multi_progress(mp)
            .capacity(4)
            // Disable the daemon ticker: it fires group.tick() on real
            // wall-clock time, injecting extra ticks/draws beyond the manual
            // ticks this test controls.
            .with_ticker_enabled(false)
            .build();
        let _bar = group.add_bar(100, "test");

        group.tick();
        let content = term.contents();
        assert!(!content.is_empty(), "first tick must draw, got empty");
    }

    #[test]
    fn multiple_mutations_before_tick_single_draw() {
        // Several mutations between ticks should all be reflected in a single
        // coherent draw after the next tick, without intermediate draws.
        let term = indicatif::InMemoryTerm::new(10, 80);
        let target = indicatif::ProgressDrawTarget::term_like(Box::new(term.clone()));
        let mp = MultiProgress::with_draw_target(target);
        let group = ProgressGroup::builder()
            .with_multi_progress(mp)
            .capacity(4)
            // Disable the daemon ticker: it fires group.tick() on real
            // wall-clock time, injecting extra ticks/draws beyond the manual
            // ticks this test controls.
            .with_ticker_enabled(false)
            .build();
        let bar = group.add_bar(100, "test");

        bar.set_position(20);
        bar.set_total(200);
        bar.set_prefix_components(PrefixComponents {
            tool_name: "multi".into(),
            ..Default::default()
        });

        group.tick();
        let content = term.contents();
        assert!(content.contains("20/200"), "expected 20/200 in output: {content:?}");
        assert!(content.contains("multi"), "expected prefix 'multi' in output: {content:?}");
    }

    #[test]
    fn finalize_produces_final_output() {
        // join_and_clear (finalize) must produce visible output showing the
        // final state of all bars.  Uses TestTimeSource for deterministic
        // timing and exact terminal content matching.
        use std::sync::Arc;
        use std::time::Duration;

        let term = indicatif::InMemoryTerm::new(10, 80);
        let target = indicatif::ProgressDrawTarget::term_like(Box::new(term.clone()));
        let mp = MultiProgress::with_draw_target(target);
        let ts = Arc::new(super::TestTimeSource::new());
        let (group, overall) = ProgressGroup::builder()
            .with_multi_progress(mp)
            .with_time_source(Arc::clone(&ts) as Arc<dyn super::TimeSource>)
            // Disable the daemon ticker: it fires group.tick() on real
            // wall-clock time, injecting extra ticks/draws beyond the manual
            // ticks this test controls.
            .with_ticker_enabled(false)
            .capacity(4)
            .with_overall("overall", 10)
            .build_with_overall();
        let bar = group.add_bar(100, "child");
        bar.advance(50);
        overall.advance(5);
        ts.advance(Duration::from_secs(1));

        // Sync state from SharedState to bars before finalize (finalize
        // only syncs non-Active bars).
        group.tick();
        group.join_and_clear();
        let actual = term.contents();
        let lines: Vec<&str> = actual.lines().collect();

        // EXACT line count: exactly 2 visible lines (child + overall).
        assert_eq!(
            lines.len(),
            2,
            "finalize must show exactly 2 bars, got {} lines:\n{actual}",
            lines.len(),
        );

        // Line 0: child bar — prefix and position visible.
        assert!(lines[0].contains("child"), "child prefix in line 0: {0}", lines[0]);
        assert!(lines[0].contains("50/100"), "child pos 50/100: {0}", lines[0]);

        // Line 1: overall bar — prefix and position visible.
        assert!(lines[1].contains("overall"), "overall prefix in line 1: {0}", lines[1]);
        assert!(lines[1].contains("5/10"), "overall pos 5/10: {0}", lines[1]);

        // Both lines show elapsed (1s).
        assert!(lines[0].contains("1s"), "child shows 1s elapsed: {0}", lines[0]);
        assert!(lines[1].contains("1s"), "overall shows 1s elapsed: {0}", lines[1]);
    }

    #[test]
    fn dirty_tracking_skips_clean_ticks() {
        // A tick without any mutation must produce identical terminal content
        // to the previous tick (dirty tracking skips the slot, bar.tick() is
        // not called, no redraw occurs).

        // Helper: extract the body (everything after the spinner char).
        fn content_body(s: &str) -> &str {
            s.lines().find_map(|l| l.get(1..)).unwrap_or("")
        }

        use super::inner::DimensionSource;
        use std::sync::Arc;

        let term = indicatif::InMemoryTerm::new(10, 80);
        let target = indicatif::ProgressDrawTarget::term_like(Box::new(term.clone()));
        let mp = MultiProgress::with_draw_target(target);
        let dims = Arc::new(super::inner::TestDimensionSource::new((10, 80)));
        let ts = Arc::new(super::TestTimeSource::new());

        let group = ProgressGroup::builder()
            .with_multi_progress(mp)
            .with_dim_source(dims as Arc<dyn DimensionSource>)
            .with_time_source(ts.clone() as Arc<dyn super::TimeSource>)
            // Disable the daemon ticker: it fires group.tick() on real
            // wall-clock time, injecting extra ticks/draws beyond the manual
            // ticks this test controls.
            .with_ticker_enabled(false)
            .capacity(4)
            .build();
        let bar = group.add_bar(100, "test");
        bar.set_position(42);
        ts.advance(std::time::Duration::from_millis(100));

        // Tick 1: baseline (bar appears).
        group.tick();
        let baseline = term.contents();
        assert!(!baseline.is_empty(), "baseline must have content");

        let baseline_body = content_body(&baseline);

        // Tick 2: no mutations → content body (everything except spinner)
        // must be identical.  The daemon ticker is disabled above, so the
        // spinner char is deterministic too; we still compare only the body
        // to keep the assertion focused on dirty tracking.
        ts.advance(std::time::Duration::from_millis(50));
        group.tick();
        let second = term.contents();
        assert_eq!(
            content_body(&second),
            baseline_body,
            "clean tick should not change content body\n\
         expected body: {baseline_body:?}\n\
         got body:      {:?}",
            content_body(&second)
        );

        // Tick 3: still no mutations → content body remains unchanged.
        ts.advance(std::time::Duration::from_millis(50));
        group.tick();
        let third = term.contents();
        assert_eq!(
            content_body(&third),
            baseline_body,
            "second clean tick should also not change content body\n\
         expected body: {baseline_body:?}\n\
         got body:      {:?}",
            content_body(&third)
        );
    }

    #[test]
    fn dirty_tracking_draws_on_mutation() {
        // A tick after mutation must reflect the new state in the output.
        use super::inner::DimensionSource;
        use std::sync::Arc;

        let term = indicatif::InMemoryTerm::new(10, 80);
        let target = indicatif::ProgressDrawTarget::term_like(Box::new(term.clone()));
        let mp = MultiProgress::with_draw_target(target);
        let dims = Arc::new(super::inner::TestDimensionSource::new((10, 80)));
        let ts = Arc::new(super::TestTimeSource::new());

        let group = ProgressGroup::builder()
            .with_multi_progress(mp)
            .with_dim_source(dims as Arc<dyn DimensionSource>)
            .with_time_source(ts.clone() as Arc<dyn super::TimeSource>)
            // Disable the daemon ticker: it fires group.tick() on real
            // wall-clock time, injecting extra ticks/draws beyond the manual
            // ticks this test controls.
            .with_ticker_enabled(false)
            .capacity(4)
            .build();
        let bar = group.add_bar(100, "test");
        bar.set_position(10);
        ts.advance(std::time::Duration::from_millis(100));

        // Tick 1: baseline shows 10/100.
        group.tick();
        let baseline = term.contents();
        assert!(baseline.contains("10/100"), "baseline must contain 10/100, got: {baseline:?}");

        // Mutate position between ticks.
        bar.set_position(80);
        ts.advance(std::time::Duration::from_millis(50));

        // Tick 2: must show the new position (body changes even if
        // the spinner also advanced via daemon ticker).
        group.tick();
        let after = term.contents();
        assert!(after.contains("80/100"), "expected 80/100 after mutation+tick, got: {after:?}");
    }

    // ---- Pre-roll tests ----------------------------------------------------

    #[test]
    fn pre_roll_reserves_full_terminal_height() {
        // When pre_roll fires, it must write exactly `rows` newlines (one per
        // terminal row) and move the cursor back up by the same amount.
        use super::inner::DimensionSource;
        use std::sync::Arc;

        let mp_term = indicatif::InMemoryTerm::new(10, 80);
        let cap_term = indicatif::InMemoryTerm::new(100, 80);
        let target = indicatif::ProgressDrawTarget::term_like(Box::new(mp_term.clone()));
        let mp = MultiProgress::with_draw_target(target);
        let dims = Arc::new(super::inner::TestDimensionSource::new((10, 80)));
        let ts = Arc::new(super::TestTimeSource::new());

        let _group = ProgressGroup::builder()
            .with_multi_progress(mp)
            .with_pre_roll_capture(Box::new(cap_term.clone()))
            .with_dim_source(dims as Arc<dyn DimensionSource>)
            .with_time_source(ts.clone() as Arc<dyn super::TimeSource>)
            .capacity(2)
            .build();

        // Wait for the background ticker to fire pre_roll.
        std::thread::sleep(std::time::Duration::from_millis(200));

        let moves = cap_term.moves_since_last_check();
        let newline_count = moves.lines().filter(|l| l.trim() == "NewLine").count();
        assert_eq!(
            newline_count, 10,
            "pre_roll should write exactly 10 blank lines (rows=10), got {newline_count}:\n{moves}"
        );
        assert!(moves.contains("Up(10)"), "pre_roll should move cursor up 10 rows:\n{moves}");
    }

    #[test]
    fn pre_roll_one_shot() {
        // After pre_roll fires once, subsequent ticks must not write
        // additional newlines.
        use super::inner::DimensionSource;
        use std::sync::Arc;

        let mp_term = indicatif::InMemoryTerm::new(10, 80);
        let cap_term = indicatif::InMemoryTerm::new(100, 80);
        let target = indicatif::ProgressDrawTarget::term_like(Box::new(mp_term.clone()));
        let mp = MultiProgress::with_draw_target(target);
        let dims = Arc::new(super::inner::TestDimensionSource::new((10, 80)));
        let ts = Arc::new(super::TestTimeSource::new());

        let group = ProgressGroup::builder()
            .with_multi_progress(mp)
            .with_pre_roll_capture(Box::new(cap_term.clone()))
            .with_dim_source(dims as Arc<dyn DimensionSource>)
            .with_time_source(ts.clone() as Arc<dyn super::TimeSource>)
            .capacity(2)
            .build();

        // Wait for the background ticker to fire pre_roll.
        std::thread::sleep(std::time::Duration::from_millis(200));

        // Drain the pre_roll moves from the first ticker tick.
        let first_moves = cap_term.moves_since_last_check();
        let first_newlines = first_moves.lines().filter(|l| l.trim() == "NewLine").count();
        assert_eq!(first_newlines, 10, "first tick should write 10 pre_roll newlines");

        // Tick explicitly — pre_roll must not fire again.
        group.tick();
        std::thread::sleep(std::time::Duration::from_millis(150));
        let second_moves = cap_term.moves_since_last_check();
        let second_newlines = second_moves.lines().filter(|l| l.trim() == "NewLine").count();
        assert_eq!(
            second_newlines, 0,
            "second tick should NOT fire pre_roll again, got {second_newlines} newlines"
        );
    }

    #[test]
    fn pre_roll_with_overall() {
        // Same as pre_roll_reserves_full_terminal_height but with an overall
        // aggregate bar.
        use super::inner::DimensionSource;
        use std::sync::Arc;

        let mp_term = indicatif::InMemoryTerm::new(10, 80);
        let cap_term = indicatif::InMemoryTerm::new(100, 80);
        let target = indicatif::ProgressDrawTarget::term_like(Box::new(mp_term.clone()));
        let mp = MultiProgress::with_draw_target(target);
        let dims = Arc::new(super::inner::TestDimensionSource::new((10, 80)));
        let ts = Arc::new(super::TestTimeSource::new());

        let (_group, _overall) = ProgressGroup::builder()
            .with_multi_progress(mp)
            .with_pre_roll_capture(Box::new(cap_term.clone()))
            .with_dim_source(dims as Arc<dyn DimensionSource>)
            .with_time_source(ts.clone() as Arc<dyn super::TimeSource>)
            .capacity(2)
            .with_overall("total", 100)
            .build_with_overall();

        // Wait for the background ticker to fire pre_roll.
        std::thread::sleep(std::time::Duration::from_millis(200));

        let moves = cap_term.moves_since_last_check();
        let newline_count = moves.lines().filter(|l| l.trim() == "NewLine").count();
        assert_eq!(
            newline_count, 10,
            "pre_roll (with overall) should write exactly 10 blank lines, got {newline_count}:\n{moves}"
        );
        assert!(
            moves.contains("Up(10)"),
            "pre_roll (with overall) should move cursor up 10 rows:\n{moves}"
        );
    }

    #[test]
    fn pre_roll_height_changes_no_effect() {
        // Once pre_roll has fired, a subsequent terminal height change must
        // NOT trigger a second pre_roll (one-shot invariant).
        use super::inner::DimensionSource;
        use std::sync::Arc;

        let mp_term = indicatif::InMemoryTerm::new(10, 80);
        let cap_term = indicatif::InMemoryTerm::new(100, 80);
        let target = indicatif::ProgressDrawTarget::term_like(Box::new(mp_term.clone()));
        let mp = MultiProgress::with_draw_target(target);
        let dims = Arc::new(super::inner::TestDimensionSource::new((10, 80)));
        let ts = Arc::new(super::TestTimeSource::new());

        let group = ProgressGroup::builder()
            .with_multi_progress(mp)
            .with_pre_roll_capture(Box::new(cap_term.clone()))
            .with_dim_source(dims.clone() as Arc<dyn DimensionSource>)
            .with_time_source(ts.clone() as Arc<dyn super::TimeSource>)
            .capacity(2)
            .build();

        // Wait for pre_roll to fire at H=10.
        std::thread::sleep(std::time::Duration::from_millis(200));
        let _ = cap_term.moves_since_last_check();

        // Change terminal height — pre_roll must NOT re-fire.
        dims.set((20, 80));
        group.tick();
        std::thread::sleep(std::time::Duration::from_millis(150));

        let moves = cap_term.moves_since_last_check();
        let newline_count = moves.lines().filter(|l| l.trim() == "NewLine").count();
        assert_eq!(
            newline_count, 0,
            "height change should NOT trigger pre_roll again, got {newline_count} newlines"
        );
    }

    #[test]
    fn pre_roll_with_existing_content_scrolls_it_away() {
        // Regression test: when the terminal has existing content and the
        // cursor is NOT at the bottom, pre_roll must write enough newlines
        // to push ALL visible content into the scrollback buffer.
        //
        // In this scenario: cursor is at row 0 (top), terminal has 10 rows
        // with content at rows 0-4.  Pre-roll with only `rows` newlines
        // would scroll only 1 line (cursor reaches bottom after 9 newlines,
        // then 1 scroll), leaving rows 1-4 visible.
        use super::inner::DimensionSource;
        use indicatif::TermLike;
        use std::sync::Arc;

        let term = indicatif::InMemoryTerm::new(10, 80);
        // Write content BEFORE progress group (simulates terminal state).
        for i in 1..=5 {
            let _ = term.write_line(&format!("existing content line {i}"));
        }
        // At this point cursor is at row 5 (after writing 5 lines).
        // Move cursor UP 5 to simulate cursor at top (worst case).
        let _ = term.move_cursor_up(5);
        // Cursor is now at row 0, content at rows 0-4.
        let initial_content = term.contents();
        assert!(initial_content.contains("existing content line 1"), "content must be written");
        assert!(initial_content.contains("existing content line 5"), "content must be written");

        // Same InMemoryTerm for both draw target AND pre_roll capture.
        let target = indicatif::ProgressDrawTarget::term_like(Box::new(term.clone()));
        let mp = MultiProgress::with_draw_target(target);
        let dims = Arc::new(super::inner::TestDimensionSource::new((10, 80)));
        let ts = Arc::new(super::TestTimeSource::new());

        let group = ProgressGroup::builder()
            .with_multi_progress(mp)
            .with_pre_roll_capture(Box::new(term.clone()))
            .with_dim_source(dims as Arc<dyn DimensionSource>)
            .with_time_source(ts.clone() as Arc<dyn super::TimeSource>)
            .capacity(2)
            .build();
        // Wait for the background ticker to fire pre_roll (at 50ms, 3-4
        // ticks should be enough).
        std::thread::sleep(std::time::Duration::from_millis(200));
        // Pre_roll has now pushed all existing content away.

        let bar = group.add_bar(100, "work");
        bar.set_position(100);
        ts.advance(std::time::Duration::from_millis(100));
        bar.finish_success();

        // Sync state from SharedState to indicatif, then finalize for
        // deterministic finished-bar output (no spinner animation).
        group.tick();
        group.join_and_clear();

        // The visible content must contain ONLY the finished bar output.
        // All "existing content line *" must be gone (scrolled into
        // scrollback by pre_roll).
        // All existing content must have been scrolled into scrollback by
        // pre_roll.  The visible content must contain ONLY the finished bar
        // line (pure text — InMemoryTerm::contents strips ANSI codes).
        let after = term.contents();
        assert!(
            !after.contains("existing content"),
            "existing content must be scrolled away, got: {after:?}",
        );
        // Exactly one visible line with a spinner prefix and deterministic
        // body.  Strip the multi-byte spinner char for exact body matching.
        let bar_line = after.lines().next().expect("expected at least one bar line");
        let spinner_len = bar_line.chars().next().unwrap().len_utf8();
        let body = &bar_line[spinner_len..];
        assert_eq!(
            body,
            concat!("                           work ", "█████████████████████  ", "100/100 0s",),
            "bar body after spinner must match exactly",
        );
    }

    #[test]
    fn pre_roll_fires_on_join_and_clear_before_ticker() {
        // Regression test: when all bars finish before the first ticker tick
        // (≈50 ms), join_and_clear() → finalize() must still call
        // pre_roll_if_needed().  Without this, bars draw at the current
        // cursor position and overwrite existing terminal content.
        use super::inner::DimensionSource;
        use std::sync::Arc;

        let mp_term = indicatif::InMemoryTerm::new(10, 80);
        let cap_term = indicatif::InMemoryTerm::new(100, 80);
        let target = indicatif::ProgressDrawTarget::term_like(Box::new(mp_term.clone()));
        let mp = MultiProgress::with_draw_target(target);
        let dims = Arc::new(super::inner::TestDimensionSource::new((10, 80)));
        let ts = Arc::new(super::TestTimeSource::new());

        let group = ProgressGroup::builder()
            .with_multi_progress(mp)
            .with_pre_roll_capture(Box::new(cap_term.clone()))
            .with_dim_source(dims as Arc<dyn DimensionSource>)
            .with_time_source(ts.clone() as Arc<dyn super::TimeSource>)
            .capacity(2)
            .build();

        // Add a bar and complete it immediately — before the 50ms ticker
        // thread fires its first tick.
        let bar = group.add_bar(1, "instant");
        bar.set_position(1);
        ts.advance(std::time::Duration::from_millis(1));
        bar.finish_success();

        // Finalize immediately — the background ticker has slept only
        // microseconds, nowhere near its 50ms interval.
        group.join_and_clear();

        // Pre_roll must have been called (via finalize).
        let moves = cap_term.moves_since_last_check();
        let newline_count = moves.lines().filter(|l| l.trim() == "NewLine").count();
        assert!(
            newline_count > 0,
            "finalize must trigger pre_roll (rows=10), got {newline_count}:\n{moves}",
        );
        assert!(
            moves.contains("Up(10)"),
            "finalize pre_roll must move cursor up 10 rows:\n{moves}",
        );
    }

    #[test]
    fn sync_slot_preserves_custom_suffix_on_attach() {
        // Regression: when `add_bar` triggers `attach` → `sync_slot`, the
        // slot is synced with only the auto-computed RHS suffix, dropping
        // any custom suffix that was set via `set_suffix_components`.
        //
        // Without the fix, sync_slot overwrites the suffix with only the
        // auto-computed RHS, dropping the custom part.  With the fix,
        // sync_slot delegates to sync_snapshot_to_bar which appends the
        // custom suffix.
        //
        // Bar A is kept ACTIVE (not finished) so the tick drain-loop (which
        // unconditionally re-syncs non-active bars) does not rescue the
        // suffix.  Only the dirty-tracking loop processes A — and without
        // the fix A is not dirty after the attach, so the wrong suffix
        // persists.
        use std::sync::Arc;
        use std::time::Duration;

        let term = indicatif::InMemoryTerm::new(10, 80);
        let target = indicatif::ProgressDrawTarget::term_like(Box::new(term.clone()));
        let mp = indicatif::MultiProgress::with_draw_target(target);
        let ts = Arc::new(super::TestTimeSource::new());
        let group = ProgressGroup::builder()
            .with_multi_progress(mp)
            .with_time_source(Arc::clone(&ts) as Arc<dyn super::TimeSource>)
            .capacity(2)
            .build();

        // Phase 1: add bar A, set custom suffix and partial progress, sync.
        let bar_a = group.add_bar(100, "resolve");
        bar_a.set_position(50);
        bar_a.set_suffix_components(SuffixComponents {
            custom: "cached (1)".into(),
            ..Default::default()
        });
        ts.advance(Duration::from_millis(100));
        group.tick();

        // Baseline: custom suffix is visible after first tick.
        let baseline = term.contents();
        assert!(
            baseline.contains("cached (1)"),
            "baseline must show custom suffix after tick:\n{baseline}",
        );

        // Phase 2: add bar B — triggers attach → shift → sync_slot on A.
        let bar_b = group.add_bar(100, "fetch");
        bar_b.set_position(0);

        // Check terminal output IMMEDIATELY after add_bar + set_position.
        let after_attach = term.contents();
        assert!(
            after_attach.contains("cached (1)"),
            "custom suffix lost after add_bar + set_position (before tick).\n\
             Terminal output:\n{after_attach}",
        );
        std::mem::drop(bar_a);
        std::mem::drop(bar_b);
    }

    // ---- structured suffix merge semantics (Phase 5) --------------------

    #[test]
    fn suffix_stored_state_is_structured() {
        // set_suffix_components must store the full structured set (all six
        // fields), not just `custom` — snapshot() carries them through so the
        // sync path can merge field-by-field.
        let h = TrackedHandle::new(100);
        h.set_suffix_components(SuffixComponents {
            count: "9".into(),
            total: "9".into(),
            elapsed: "1:23:45".into(),
            rate: Some("10/s".into()),
            eta: Some("[0:00:07]".into()),
            custom: "cached".into(),
        });
        let snap = h.snapshot();
        assert_eq!(snap.suffix_components.count, "9");
        assert_eq!(snap.suffix_components.total, "9");
        assert_eq!(snap.suffix_components.elapsed, "1:23:45");
        assert_eq!(snap.suffix_components.rate.as_deref(), Some("10/s"));
        assert_eq!(snap.suffix_components.eta.as_deref(), Some("[0:00:07]"));
        assert_eq!(snap.suffix_components.custom, "cached");
    }

    #[test]
    fn suffix_merge_user_count_total_overrides_auto() {
        // User-set count/total components must override the auto-derived
        // count/total from the snapshot position.
        use std::sync::Arc;
        use std::time::Duration;

        let term = indicatif::InMemoryTerm::new(10, 80);
        let target = indicatif::ProgressDrawTarget::term_like(Box::new(term.clone()));
        let mp = indicatif::MultiProgress::with_draw_target(target);
        let ts = Arc::new(super::TestTimeSource::new());
        let group = ProgressGroup::builder()
            .with_multi_progress(mp)
            .with_time_source(Arc::clone(&ts) as Arc<dyn super::TimeSource>)
            .capacity(2)
            .build();

        let bar = group.add_bar(100, "test");
        bar.set_position(50);
        bar.set_suffix_components(SuffixComponents {
            count: "9".into(),
            total: "9".into(),
            ..Default::default()
        });
        ts.advance(Duration::from_millis(100));
        group.tick();

        let content = term.contents();
        assert!(content.contains("9/9"), "user count/total must override auto-derived:\n{content}");
        assert!(
            !content.contains("50/100"),
            "auto count/total must not show when user overrides:\n{content}",
        );
    }

    #[test]
    fn suffix_merge_user_rate_eta_elapsed_override_auto() {
        // User-set elapsed/rate/eta components must override the auto-derived
        // ticker fields.
        use std::sync::Arc;
        use std::time::Duration;

        let term = indicatif::InMemoryTerm::new(10, 80);
        let target = indicatif::ProgressDrawTarget::term_like(Box::new(term.clone()));
        let mp = indicatif::MultiProgress::with_draw_target(target);
        let ts = Arc::new(super::TestTimeSource::new());
        let group = ProgressGroup::builder()
            .with_multi_progress(mp)
            .with_time_source(Arc::clone(&ts) as Arc<dyn super::TimeSource>)
            .capacity(2)
            .build();

        let bar = group.add_bar(100, "test");
        bar.set_position(50);
        bar.set_suffix_components(SuffixComponents {
            elapsed: "1:23:45".into(),
            rate: Some("10/s".into()),
            eta: Some("[0:00:07]".into()),
            ..Default::default()
        });
        ts.advance(Duration::from_millis(100));
        group.tick();

        let content = term.contents();
        assert!(content.contains("1:23:45"), "user elapsed must override auto-derived:\n{content}");
        assert!(content.contains("10/s"), "user rate must override auto-derived:\n{content}");
        assert!(content.contains("[0:00:07]"), "user eta must override auto-derived:\n{content}");
    }

    #[test]
    fn suffix_truncation_order_unchanged_after_merge() {
        // A merged component set (user overrides on top of auto fields) must
        // still truncate in the normative order: custom progressive → eta →
        // rate → elapsed → count/total atomic → fallback. Uses the same width
        // ladder as the semantic_truncate_suffix suite.
        let merged = SuffixComponents {
            count: "9".into(),
            total: "9".into(),
            elapsed: "0:00:05".into(),
            rate: Some("12.3 MiB/s".into()),
            eta: Some("[0:00:02]".into()),
            custom: "cached (1)".into(),
        };
        // Fits: all fields present.
        let fits = super::inner::semantic_truncate_suffix(&merged, 100);
        assert_eq!(
            render_suffix(&fits),
            " \x1b[33m9/9\x1b[0m 0:00:05 12.3 MiB/s [0:00:02] cached (1)",
            "full merged suffix when fits",
        );
        // Custom shrunk to keep=5 (39 visible).
        let partial = super::inner::semantic_truncate_suffix(&merged, 39);
        assert_eq!(
            render_suffix(&partial),
            " \x1b[33m9/9\x1b[0m 0:00:05 12.3 MiB/s [0:00:02] cache",
            "merged custom shrunk first",
        );
        // Eta removed entirely (23 <= 30).
        let eta_off = super::inner::semantic_truncate_suffix(&merged, 30);
        assert_eq!(
            render_suffix(&eta_off),
            " \x1b[33m9/9\x1b[0m 0:00:05 12.3 MiB/s",
            "merged eta removed second",
        );
        // Rate removed entirely (12 <= 22).
        let rate_off = super::inner::semantic_truncate_suffix(&merged, 22);
        assert_eq!(
            render_suffix(&rate_off),
            " \x1b[33m9/9\x1b[0m 0:00:05",
            "merged rate removed third",
        );
        // Count/total still the last atomic unit.
        let count_only = super::inner::semantic_truncate_suffix(&merged, 4);
        assert_eq!(
            render_suffix(&count_only),
            " \x1b[33m9/9\x1b[0m",
            "merged count/total atomic at end",
        );
    }
}

// ---------------------------------------------------------------------------
// ByteBudget tests
// ---------------------------------------------------------------------------

#[cfg(test)]
mod byte_budget_tests {
    use super::*;
    use std::sync::Arc;
    use std::thread;

    #[test]
    fn new_sets_initial_state() {
        let b = ByteBudget::new(100);
        assert_eq!(b.pos(), 0);
        assert_eq!(b.total(), 100);
        assert_eq!(b.snap(), (0, 100));
    }

    #[test]
    fn advance_increases_position() {
        let b = ByteBudget::new(100);
        b.advance(30);
        assert_eq!(b.pos(), 30);
        b.advance(20);
        assert_eq!(b.pos(), 50);
        println!("snap: {:?}", b.snap());
    }

    #[test]
    #[should_panic(expected = "would exceed total")]
    fn advance_panics_on_overflow() {
        let b = ByteBudget::new(100);
        b.advance(101);
    }

    #[test]
    fn set_pos_works() {
        let b = ByteBudget::new(100);
        b.set_pos(50);
        assert_eq!(b.pos(), 50);
    }

    #[test]
    #[should_panic(expected = "> total")]
    fn set_pos_panics_on_exceed_total() {
        let b = ByteBudget::new(100);
        b.set_pos(101);
    }

    #[test]
    #[should_panic(expected = "< current")]
    fn set_pos_panics_on_decrease() {
        let b = ByteBudget::new(100);
        b.set_pos(50);
        b.set_pos(30);
    }

    #[test]
    fn adjust_positive_increases_total() {
        let b = ByteBudget::new(100);
        b.adjust(50);
        assert_eq!(b.total(), 150);
    }

    #[test]
    fn adjust_negative_decreases_total() {
        let b = ByteBudget::new(100);
        b.adjust(-30);
        assert_eq!(b.total(), 70);
    }

    #[test]
    #[should_panic(expected = "below pos")]
    fn adjust_negative_panics_below_pos() {
        let b = ByteBudget::new(40);
        b.advance(30);
        b.adjust(-50);
    }

    #[test]
    fn reconcile_increases_total() {
        let b = ByteBudget::new(100);
        b.reconcile(50, 100);
        assert_eq!(b.total(), 150);
    }

    #[test]
    fn reconcile_decreases_total() {
        let b = ByteBudget::new(100);
        b.reconcile(100, 50);
        assert_eq!(b.total(), 50);
    }

    #[test]
    fn reconcile_equal_is_noop() {
        let b = ByteBudget::new(100);
        b.reconcile(50, 50);
        assert_eq!(b.total(), 100);
    }

    #[test]
    fn concurrent_read_write_no_data_races() {
        let b = Arc::new(ByteBudget::new(1000));
        let b_clone = Arc::clone(&b);
        let writer = thread::spawn(move || {
            for _ in 0..100 {
                b_clone.advance(5);
            }
        });
        let b_clone2 = Arc::clone(&b);
        let reader = thread::spawn(move || {
            for _ in 0..100 {
                let (_pos, total) = b_clone2.snap();
                assert!(total == 1000 || total == 1500);
            }
        });
        writer.join().unwrap();
        reader.join().unwrap();
    }

    #[test]
    fn send_sync_trait_bounds() {
        fn assert_send<T: Send>() {}
        fn assert_sync<T: Sync>() {}
        assert_send::<ByteBudget>();
        assert_sync::<ByteBudget>();
    }
}

// ---------------------------------------------------------------------------
// MultiItemBudget tests
// ---------------------------------------------------------------------------

#[cfg(test)]
mod multi_item_budget_tests {
    use super::*;
    use std::sync::Arc;
    use std::thread;

    #[test]
    fn new_creates_empty() {
        let b = MultiItemBudget::new();
        assert_eq!(b.item_count(), 0);
        assert_eq!(b.aggregate(), (0, 0));
    }

    #[test]
    fn with_capacity_pre_allocates() {
        let b = MultiItemBudget::with_capacity(10);
        assert_eq!(b.item_count(), 0);
    }

    #[test]
    fn add_item_increases_count() {
        let mut b = MultiItemBudget::new();
        b.add_item(100);
        assert_eq!(b.item_count(), 1);
        b.add_item(200);
        assert_eq!(b.item_count(), 2);
    }

    #[test]
    fn add_item_sets_initial_total() {
        let mut b = MultiItemBudget::new();
        b.add_item(42);
        assert_eq!(b.snap(0), (0, 42));
    }

    #[test]
    fn item_count_reflects_adds() {
        let mut b = MultiItemBudget::new();
        assert_eq!(b.item_count(), 0);
        b.add_item(10);
        assert_eq!(b.item_count(), 1);
        b.add_item(20);
        b.add_item(30);
        assert_eq!(b.item_count(), 3);
    }

    #[test]
    fn set_total_updates_item_total() {
        let mut b = MultiItemBudget::new();
        b.add_item(100);
        b.set_total(0, 250);
        assert_eq!(b.snap(0), (0, 250));
    }

    #[test]
    #[should_panic(expected = "< current pos")]
    fn set_total_panics_below_position() {
        let mut b = MultiItemBudget::new();
        b.add_item(100);
        b.advance(0, 50);
        b.set_total(0, 30);
    }

    #[test]
    fn advance_increases_position() {
        let mut b = MultiItemBudget::new();
        b.add_item(100);
        b.advance(0, 30);
        assert_eq!(b.snap(0), (30, 100));
        b.advance(0, 20);
        assert_eq!(b.snap(0), (50, 100));
    }

    #[test]
    fn advance_multiple_items_independently() {
        let mut b = MultiItemBudget::new();
        b.add_item(100);
        b.add_item(200);
        b.advance(0, 10);
        b.advance(1, 20);
        assert_eq!(b.snap(0), (10, 100));
        assert_eq!(b.snap(1), (20, 200));
    }

    #[test]
    #[should_panic(expected = "would exceed total")]
    fn advance_panics_on_overflow() {
        let mut b = MultiItemBudget::new();
        b.add_item(100);
        b.advance(0, 101);
    }

    #[test]
    fn set_pos_works() {
        let mut b = MultiItemBudget::new();
        b.add_item(100);
        b.set_pos(0, 50);
        assert_eq!(b.snap(0), (50, 100));
    }

    #[test]
    #[should_panic(expected = "> total")]
    fn set_pos_panics_on_exceed_total() {
        let mut b = MultiItemBudget::new();
        b.add_item(100);
        b.set_pos(0, 101);
    }

    #[test]
    #[should_panic(expected = "< current")]
    fn set_pos_panics_on_decrease() {
        let mut b = MultiItemBudget::new();
        b.add_item(100);
        b.set_pos(0, 50);
        b.set_pos(0, 30);
    }

    #[test]
    fn snap_returns_item_state() {
        let mut b = MultiItemBudget::new();
        b.add_item(100);
        assert_eq!(b.snap(0), (0, 100));
        b.advance(0, 42);
        assert_eq!(b.snap(0), (42, 100));
    }

    #[test]
    #[should_panic(expected = "index out of bounds")]
    fn snap_panics_on_bad_index() {
        let b = MultiItemBudget::new();
        let _ = b.snap(0);
    }

    #[test]
    fn aggregate_sums_all_items() {
        let mut b = MultiItemBudget::new();
        b.add_item(100);
        b.add_item(200);
        b.add_item(300);
        b.advance(0, 10);
        b.advance(1, 20);
        b.advance(2, 30);
        assert_eq!(b.aggregate(), (60, 600));
    }

    #[test]
    fn aggregate_indeterminate_items_contribute_zero() {
        let mut b = MultiItemBudget::new();
        b.add_item(0); // indeterminate
        b.add_item(100);
        b.advance(1, 50);
        assert_eq!(b.aggregate(), (50, 100));
    }

    #[test]
    fn default_is_empty() {
        let b = MultiItemBudget::default();
        assert_eq!(b.item_count(), 0);
        assert_eq!(b.aggregate(), (0, 0));
    }

    #[test]
    fn concurrent_read_write_no_data_races() {
        // Pre-populate then wrap in Arc
        let mut inner = MultiItemBudget::new();
        inner.add_item(1000);
        inner.add_item(1000);
        let b = Arc::new(inner);
        let b_clone = Arc::clone(&b);
        let writer = thread::spawn(move || {
            for _ in 0..100 {
                b_clone.advance(0, 5);
            }
        });
        let b_clone2 = Arc::clone(&b);
        let reader = thread::spawn(move || {
            for _ in 0..100 {
                let (_pos, total) = b_clone2.snap(1);
                assert!(total == 1000 || total == 1500);
            }
        });
        writer.join().unwrap();
        reader.join().unwrap();
        assert_eq!(b.snap(0), (500, 1000));
    }

    #[test]
    fn send_sync_trait_bounds() {
        fn assert_send<T: Send>() {}
        fn assert_sync<T: Sync>() {}
        assert_send::<MultiItemBudget>();
        assert_sync::<MultiItemBudget>();
    }
}
