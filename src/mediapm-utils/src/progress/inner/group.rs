//! `ProgressGroup`: combined tracking + rendering with optional overall bar.

use std::marker::PhantomData;
use std::sync::atomic::AtomicBool;
use std::sync::{Arc, Mutex};
use std::time::Duration;

use indicatif::{MultiProgress, ProgressDrawTarget, TermLike};

use super::{
    BufferedTerm, DimensionSource, MAX_SLOTS, ProgressDebugSink, ProgressRenderer,
    RealTerminalSource, RealTimeSource, SharedState, TimeSource, TrackedHandle,
    detect_progress_debug_env,
};
use crate::progress::BarStyle;

// ---- ProgressGroup (combined tracking + rendering) --------------------

/// A vertical stack of progress bars.
///
/// Bars are drawn in a fixed-height grid determined by the terminal height at
/// construction time, which eliminates ghosting from bar-count changes. To
/// create a no-op group, use [`ProgressGroup::disabled`].
pub struct ProgressGroup {
    /// `None` when progress is disabled.
    renderer: Option<Arc<Mutex<ProgressRenderer>>>,
    /// Daemon ticker task driving renders at 50 ms intervals.
    /// Holds a `Weak` reference to the renderer — exits cleanly
    /// when the renderer is dropped.
    ticker: Option<std::thread::JoinHandle<()>>,
}

// ---- ProgressGroupBuilder -------------------------------------------------

/// Marker type indicating the builder has no overall bar yet.
pub struct NoOverall;

/// Marker type indicating the builder has an overall bar configured.
pub struct HasOverall;

/// Builder for [`ProgressGroup`] with compile-time overall-bar enforcement.
///
/// The phantom type parameter `S` tracks whether an overall bar has been
/// configured: [`ProgressGroupBuilder<NoOverall>`] has no `build()` (call
/// [`with_overall()`](Self::with_overall) first); [`ProgressGroupBuilder<HasOverall>`]
/// exposes `build()` returning `(ProgressGroup, TrackedHandle)`. This makes it
/// impossible to construct a `ProgressGroup` without an overall bar.
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
pub struct ProgressGroupBuilder<S = NoOverall> {
    mp: Option<MultiProgress>,
    dim_source: Arc<dyn DimensionSource>,
    overall: Option<(String, u64)>,
    capacity: Option<usize>,
    dynamic_height: bool,
    time_source: Arc<dyn TimeSource>,
    pre_roll_term: Option<Box<dyn TermLike>>,
    debug_sink: Option<ProgressDebugSink>,
    ticker_enabled: bool,
    _state: PhantomData<S>,
}

impl Default for ProgressGroupBuilder<NoOverall> {
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
            _state: PhantomData,
        }
    }
}

/// Configuration methods shared by both `NoOverall` and `HasOverall` states.
macro_rules! impl_builder_config {
    ($ty:ty) => {
        impl ProgressGroupBuilder<$ty> {
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
            /// Pre-roll newlines are written to `term` instead of
            /// `console::Term::stderr()`. The user must also pass a compatible
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
            /// enabled). Disable in tests for deterministic progress bar output.
            #[must_use]
            pub fn with_ticker_enabled(mut self, enabled: bool) -> Self {
                self.ticker_enabled = enabled;
                self
            }
        }
    };
}

impl_builder_config!(NoOverall);
impl_builder_config!(HasOverall);

impl ProgressGroupBuilder<NoOverall> {
    /// Build a group without an overall bar.
    ///
    /// Use when no overall aggregate bar is needed (e.g., the standalone
    /// conductor CLI). For screens that require an overall bar, call
    /// [`with_overall()`](Self::with_overall) first, then `build()`.
    ///
    /// # Panics
    ///
    /// Panics when the internal `Mutex` is poisoned.
    #[must_use]
    pub fn build(self) -> ProgressGroup {
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
            let mp = MultiProgress::with_draw_target(ProgressDrawTarget::term_like(Box::new(term)));
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

    /// Add an overall aggregate bar pinned at the bottom slot.
    ///
    /// Transitions the builder from [`NoOverall`] to [`HasOverall`],
    /// enabling [`build()`](ProgressGroupBuilder::build).
    ///
    /// `label` is parsed into [`PrefixComponents`] at construction,
    /// exactly like [`ProgressGroup::add_bar`] — the overall bar shares
    /// the single canonical prefix path and the legacy `set_prefix(String)`
    /// API does not exist here either.
    #[must_use]
    pub fn with_overall(self, label: &str, total: u64) -> ProgressGroupBuilder<HasOverall> {
        ProgressGroupBuilder {
            mp: self.mp,
            dim_source: self.dim_source,
            overall: Some((label.to_string(), total)),
            capacity: self.capacity,
            dynamic_height: self.dynamic_height,
            time_source: self.time_source,
            pre_roll_term: self.pre_roll_term,
            debug_sink: self.debug_sink,
            ticker_enabled: self.ticker_enabled,
            _state: PhantomData,
        }
    }
}

impl ProgressGroupBuilder<HasOverall> {
    /// Build a group with the overall bar pinned at the bottom slot.
    ///
    /// Returns both the [`ProgressGroup`] and a [`TrackedHandle`] for the
    /// overall bar. The caller owns the overall handle and must advance/
    /// finish it when the tracked operation completes.
    ///
    /// # Panics
    ///
    /// Panics when the internal `Mutex` is poisoned.
    #[must_use]
    pub fn build(self) -> (ProgressGroup, TrackedHandle) {
        let (label, total) = self.overall.expect("HasOverall builder must have overall set");
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
            let mp = MultiProgress::with_draw_target(ProgressDrawTarget::term_like(Box::new(term)));
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
        self.add_bar_with_style(total, label, BarStyle::StepCount)
    }

    /// Add a child bar with an explicit [`BarStyle`].
    ///
    /// See [`add_bar`](Self::add_bar) for the default-style variant. This
    /// seeds the style marker at construction so the renderer applies
    /// style-specific rendering (e.g. the `WorkerSpinner` `0/0` guard)
    /// from the first tick.
    pub fn add_bar_with_style(&self, total: u64, label: &str, style: BarStyle) -> TrackedHandle {
        let Some(ref renderer) = self.renderer else {
            return TrackedHandle::disabled();
        };
        let state;
        {
            let mut locked = renderer.lock().unwrap_or_else(std::sync::PoisonError::into_inner);
            state = Arc::new(SharedState::with_time_source_and_style(
                total,
                label,
                style,
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
        match std::thread::Builder::new().name("mediapm-progress-ticker".into()).spawn(move || {
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
        }) {
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
