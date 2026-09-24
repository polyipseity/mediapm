//! `ProgressTerminal`: the single owner of a draw target, write gate,
//! ticker thread, and pre-roll state for one sync operation.
//!
//! Exactly one [`ManagedScreen`] may be live inside a terminal at any time.
//! Creating a second screen while one is already live panics — the caller
//! must [`join`](ManagedScreen::join) or drop the current screen first.

use std::marker::PhantomData;
use std::sync::{Arc, Mutex, Weak};
use std::time::Duration;

use indicatif::{MultiProgress, ProgressDrawTarget, TermLike};

use super::HasOverall;
use super::NoOverall;
use super::{
    DimensionSource, MAX_SLOTS, ProgressBarHandle, ProgressDebugSink, ProgressRenderer,
    RealTerminalSource, RealTimeSource, SharedState, TimeSource, detect_progress_debug_env,
    gate::{BufferedTerm, WriteGate},
};
use crate::progress::BarStyle;

// ---- ScreenId -----------------------------------------------------------

/// Unique identifier for a live screen inside a [`ProgressTerminal`].
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
struct ScreenId(u64);

// ---- TerminalInner ------------------------------------------------------

/// Shared inner state of a [`ProgressTerminal`].
///
/// Holds the [`MultiProgress`], write gate, injectable sources, pre-roll
/// state, and mutable screen tracking. The ticker thread holds a [`Weak`]
/// reference to the current renderer and exits cleanly when the terminal
/// is dropped or a new screen is created.
#[expect(dead_code, reason = "fields reserved for Task 2 renderer demotion")]
struct TerminalInner {
    /// Draw target for all bars created inside this terminal.
    mp: MultiProgress,
    /// Write gate controlling terminal-write suppression during frames.
    gate: WriteGate,
    /// Mutable state protected by a mutex — the ticker and screen
    /// operations contend on this.
    state: Mutex<TerminalState>,
    /// Injectable dimension source (real terminal or test double).
    dim_source: Arc<dyn DimensionSource>,
    /// Injectable time source (real wall clock or test double).
    time_source: Arc<dyn TimeSource>,
    /// Optional JSONL debug sink for bar-state snapshots.
    debug_sink: Option<ProgressDebugSink>,
    /// One-shot flag: has the first-draw pre-roll (newline scroll) been
    /// performed?  True after the first frame renders.
    pre_rolled: std::sync::atomic::AtomicBool,
    /// Terminal to write pre-roll newlines to.  `None` in test mode
    /// (user-provided [`MultiProgress`] via [`with_multi_progress`]).
    pre_roll_term: Option<Box<dyn TermLike>>,
}

/// Mutable state owned by [`TerminalInner`].
struct TerminalState {
    /// The currently live screen, if any.
    screen: Option<ScreenState>,
    /// Monotonically increasing counter for [`ScreenId`] generation.
    next_id: u64,
    /// The currently live renderer (`None` when no screen is active).
    renderer: Option<Arc<Mutex<ProgressRenderer>>>,
    /// The ticker thread handle for the current renderer.
    ticker: Option<std::thread::JoinHandle<()>>,
}

/// Per-screen state stored in [`TerminalState`].
struct ScreenState {
    /// Unique identifier for this screen.
    id: ScreenId,
}

// ---- ProgressTerminalBuilder --------------------------------------------

/// Builder for [`ProgressTerminal`] with compile-time overall-bar enforcement.
///
/// Created by [`ProgressTerminal::builder`]. The builder configures the
/// terminal's draw target, dimension source, time source, pre-roll, debug
/// sink, and ticker settings.
pub struct ProgressTerminalBuilder<S = NoOverall> {
    mp_and_gate: Option<(MultiProgress, WriteGate)>,
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

impl Default for ProgressTerminalBuilder<NoOverall> {
    fn default() -> Self {
        Self {
            mp_and_gate: None,
            dim_source: Arc::new(RealTerminalSource),
            overall: None,
            capacity: None,
            dynamic_height: false,
            time_source: Arc::new(RealTimeSource),
            pre_roll_term: Some(Box::new(console::Term::stderr())),
            debug_sink: None,
            ticker_enabled: true,
            _state: PhantomData,
        }
    }
}

/// Configuration methods shared by both `NoOverall` and `HasOverall` states.
macro_rules! impl_terminal_builder_config {
    ($ty:ty) => {
        impl ProgressTerminalBuilder<$ty> {
            /// Use an injectable [`TermLike`] instead of creating a
            /// [`BufferedTerm`](super::gate::BufferedTerm) over
            /// [`console::Term::stderr`]. Always wraps the term in
            /// [`BufferedTerm`](super::gate::BufferedTerm) so the
            /// write-gate protocol is exercised in tests.
            #[must_use]
            pub fn with_term_like(mut self, term: Box<dyn TermLike>) -> Self {
                let (buffered, gate) = super::gate::BufferedTerm::new(term);
                let mp = MultiProgress::with_draw_target(ProgressDrawTarget::term_like(Box::new(
                    buffered,
                )));
                self.mp_and_gate = Some((mp, gate));
                self
            }

            /// Use an existing [`MultiProgress`] directly.
            ///
            /// Prefer [`with_term_like()`](Self::with_term_like) for new code;
            /// this method exists for incremental test migration. The write gate
            /// is a noop — tests using this path do not exercise the
            /// draw-per-frame guarantee.
            #[must_use]
            pub fn with_multi_progress(mut self, mp: MultiProgress) -> Self {
                self.mp_and_gate = Some((mp, super::gate::WriteGate::new_noop()));
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

            /// Enable or disable dynamic height adaptation (default: `false`).
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

impl_terminal_builder_config!(NoOverall);
impl_terminal_builder_config!(HasOverall);

impl ProgressTerminalBuilder<NoOverall> {
    /// Build a terminal without an overall bar.
    ///
    /// For screens that require an overall bar, call
    /// [`with_overall()`](Self::with_overall) first, then `build()`.
    #[must_use]
    pub fn build(self) -> ProgressTerminal {
        let cap = self.capacity.unwrap_or_else(|| {
            let (rows, _) = self.dim_source.dimensions();
            (rows as usize).clamp(1, MAX_SLOTS)
        });
        let (mp, gate) = self.mp_and_gate.unwrap_or_else(|| {
            let (buffered, gate) = BufferedTerm::new(Box::new(console::Term::stderr()));
            let mp =
                MultiProgress::with_draw_target(ProgressDrawTarget::term_like(Box::new(buffered)));
            (mp, gate)
        });
        let debug_sink = self.debug_sink.or_else(detect_progress_debug_env);
        ProgressTerminal {
            inner: Arc::new(TerminalInner {
                mp,
                gate,
                state: Mutex::new(TerminalState {
                    screen: None,
                    next_id: 0,
                    renderer: None,
                    ticker: None,
                }),
                dim_source: self.dim_source,
                time_source: self.time_source,
                debug_sink,
                pre_rolled: std::sync::atomic::AtomicBool::new(false),
                pre_roll_term: self.pre_roll_term,
            }),
            capacity: cap,
            dynamic_height: self.dynamic_height,
            ticker_enabled: self.ticker_enabled,
        }
    }

    /// Add an overall aggregate bar pinned at the bottom slot.
    ///
    /// Transitions the builder from [`NoOverall`] to [`HasOverall`],
    /// enabling [`build()`](ProgressTerminalBuilder::build).
    #[must_use]
    pub fn with_overall(self, label: &str, total: u64) -> ProgressTerminalBuilder<HasOverall> {
        ProgressTerminalBuilder {
            mp_and_gate: self.mp_and_gate,
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

impl ProgressTerminalBuilder<HasOverall> {
    /// Build a terminal with the overall bar pinned at the bottom slot.
    ///
    /// Returns both the [`ProgressTerminal`] and a [`ProgressBarHandle`] for the
    /// overall bar.
    #[must_use]
    pub fn build(self) -> (ProgressTerminal, ProgressBarHandle) {
        let (label, total) = self.overall.expect("HasOverall builder must have overall set");
        let cap = self.capacity.unwrap_or_else(|| {
            let (rows, _) = self.dim_source.dimensions();
            (rows as usize).clamp(1, MAX_SLOTS)
        });
        let (mp, gate) = self.mp_and_gate.unwrap_or_else(|| {
            let (buffered, gate) = BufferedTerm::new(Box::new(console::Term::stderr()));
            let mp =
                MultiProgress::with_draw_target(ProgressDrawTarget::term_like(Box::new(buffered)));
            (mp, gate)
        });
        let debug_sink = self.debug_sink.or_else(detect_progress_debug_env);
        let terminal = ProgressTerminal {
            inner: Arc::new(TerminalInner {
                mp,
                gate,
                state: Mutex::new(TerminalState {
                    screen: None,
                    next_id: 0,
                    renderer: None,
                    ticker: None,
                }),
                dim_source: self.dim_source,
                time_source: self.time_source,
                debug_sink,
                pre_rolled: std::sync::atomic::AtomicBool::new(false),
                pre_roll_term: self.pre_roll_term,
            }),
            capacity: cap,
            dynamic_height: self.dynamic_height,
            ticker_enabled: self.ticker_enabled,
        };
        let overall_state = Arc::new(SharedState::with_time_source(
            total,
            &label,
            Arc::clone(&terminal.inner.time_source),
        ));
        let handle = ProgressBarHandle { state: overall_state };
        (terminal, handle)
    }
}

// ---- ProgressTerminal ---------------------------------------------------

/// The single owner of a draw target, write gate, ticker thread, and
/// pre-roll state for one sync operation.
///
/// Create via [`ProgressTerminal::builder`] or [`ProgressTerminal::disabled`].
/// Exactly one [`ManagedScreen`] may be live at any time — creating a
/// second screen while one is already live panics.
pub struct ProgressTerminal {
    /// Shared inner state.
    inner: Arc<TerminalInner>,
    /// Slot capacity for renderers created by this terminal.
    capacity: usize,
    /// Dynamic height adaptation flag for renderers created by this terminal.
    dynamic_height: bool,
    /// Whether to spawn a ticker thread for each screen.
    ticker_enabled: bool,
}

impl ProgressTerminal {
    /// Create a builder for configuring a [`ProgressTerminal`].
    #[must_use]
    pub fn builder() -> ProgressTerminalBuilder {
        ProgressTerminalBuilder::default()
    }

    /// Create a no-op terminal that produces no terminal output.
    ///
    /// All screens added via [`screen`](Self::screen) are disabled no-ops.
    /// Useful in tests where progress is not needed.
    #[must_use]
    pub fn disabled() -> Self {
        let (mp, gate) = {
            let (buffered, gate) = BufferedTerm::new(Box::new(console::Term::stderr()));
            let mp =
                MultiProgress::with_draw_target(ProgressDrawTarget::term_like(Box::new(buffered)));
            (mp, gate)
        };
        Self {
            inner: Arc::new(TerminalInner {
                mp,
                gate,
                state: Mutex::new(TerminalState {
                    screen: None,
                    next_id: 0,
                    renderer: None,
                    ticker: None,
                }),
                dim_source: Arc::new(RealTerminalSource),
                time_source: Arc::new(RealTimeSource),
                debug_sink: None,
                pre_rolled: std::sync::atomic::AtomicBool::new(false),
                pre_roll_term: None,
            }),
            capacity: 1,
            dynamic_height: false,
            ticker_enabled: false,
        }
    }

    /// Begin building a new screen on this terminal.
    ///
    /// Returns a [`TerminalScreenBuilder`] that can configure dynamic height,
    /// capacity, and an optional overall bar before calling `.build()`.
    ///
    /// # Panics
    ///
    /// Panics if a screen is already live — the caller must
    /// [`join`](ManagedScreen::join) or drop the current screen first.
    pub fn screen(&self) -> TerminalScreenBuilder<'_, NoOverall> {
        TerminalScreenBuilder { terminal: self, overall: None, _state: PhantomData }
    }

    /// Force a render sync (used in tests with
    /// [`InMemoryTerm`](indicatif::InMemoryTerm) where the timer
    /// thread does not run).
    ///
    /// When no screen is live, this is a no-op.
    pub fn tick(&self) {
        let state = self.inner.state.lock().unwrap_or_else(std::sync::PoisonError::into_inner);
        if let Some(ref renderer) = state.renderer {
            renderer.lock().unwrap_or_else(std::sync::PoisonError::into_inner).tick();
        }
    }
}

impl Default for ProgressTerminal {
    fn default() -> Self {
        Self::builder().build()
    }
}

impl Drop for ProgressTerminal {
    fn drop(&mut self) {
        // Drop the ticker handle to detach the thread — it will
        // exit on its next iteration when weak.upgrade() returns
        // None (the renderer Arc is dropped right after this).
        let mut state = self.inner.state.lock().unwrap_or_else(std::sync::PoisonError::into_inner);
        state.ticker.take();
        // Finalize any remaining renderer (safety net).
        if let Some(ref renderer) = state.renderer {
            renderer.lock().unwrap_or_else(std::sync::PoisonError::into_inner).finalize();
        }
    }
}

// ---- TerminalScreenBuilder ----------------------------------------------

/// Builder for [`ManagedScreen`] with compile-time overall-bar enforcement.
///
/// Created by [`ProgressTerminal::screen`]. Use
/// [`with_overall`](Self::with_overall) to add an overall bar, then
/// `.build()` to obtain the screen (and overall handle when overall is set).
pub struct TerminalScreenBuilder<'a, S = NoOverall> {
    terminal: &'a ProgressTerminal,
    overall: Option<(String, u64)>,
    _state: PhantomData<S>,
}

impl<'a> TerminalScreenBuilder<'a, NoOverall> {
    /// Build a screen without an overall bar.
    ///
    /// # Panics
    ///
    /// Panics when a screen is already live on the terminal.
    #[must_use]
    pub fn build(self) -> ManagedScreen {
        build_screen(self.terminal, self.overall)
    }

    /// Add an overall aggregate bar pinned at the bottom slot.
    ///
    /// Transitions the builder from [`NoOverall`] to [`HasOverall`],
    /// enabling [`build()`](TerminalScreenBuilder::build).
    #[must_use]
    pub fn with_overall(self, label: &str, total: u64) -> TerminalScreenBuilder<'a, HasOverall> {
        TerminalScreenBuilder {
            terminal: self.terminal,
            overall: Some((label.to_string(), total)),
            _state: PhantomData,
        }
    }
}

impl<'a> TerminalScreenBuilder<'a, HasOverall> {
    /// Build a screen with the overall bar pinned at the bottom slot.
    ///
    /// Returns both the [`ManagedScreen`] and a [`ProgressBarHandle`] for the
    /// overall bar.
    ///
    /// # Panics
    ///
    /// Panics when a screen is already live on the terminal.
    #[must_use]
    pub fn build(self) -> (ManagedScreen, ProgressBarHandle) {
        let overall = self.overall.expect("HasOverall builder must have overall set");
        let screen = build_screen(self.terminal, Some(overall.clone()));
        let handle = ProgressBarHandle {
            state: Arc::new(SharedState::with_time_source(
                overall.1,
                &overall.0,
                Arc::clone(&self.terminal.inner.time_source),
            )),
        };
        (screen, handle)
    }
}

/// Internal: create a [`ManagedScreen`] on the given terminal.
fn build_screen(terminal: &ProgressTerminal, overall: Option<(String, u64)>) -> ManagedScreen {
    // Check that no screen is live.
    {
        let mut state =
            terminal.inner.state.lock().unwrap_or_else(std::sync::PoisonError::into_inner);
        if state.screen.is_some() {
            panic!(
                "ProgressTerminal already has a live screen; \
                 join() or drop the current screen before creating a new one"
            );
        }
        let id = ScreenId(state.next_id);
        state.next_id += 1;

        // Create a fresh renderer for this screen.
        let renderer = if let Some((ref label, total)) = overall {
            let mut r = ProgressRenderer::from_mp(
                MultiProgress::with_draw_target(ProgressDrawTarget::hidden()),
                terminal.capacity,
                Arc::clone(&terminal.inner.dim_source),
                WriteGate::new_noop(),
                Arc::clone(&terminal.inner.time_source),
                None,
                None,
            );
            r.dynamic_height = terminal.dynamic_height;
            r.add_overall(label, total);
            Arc::new(Mutex::new(r))
        } else {
            let mut r = ProgressRenderer::from_mp(
                MultiProgress::with_draw_target(ProgressDrawTarget::hidden()),
                terminal.capacity,
                Arc::clone(&terminal.inner.dim_source),
                WriteGate::new_noop(),
                Arc::clone(&terminal.inner.time_source),
                None,
                None,
            );
            r.dynamic_height = terminal.dynamic_height;
            Arc::new(Mutex::new(r))
        };

        // Stop any existing ticker.
        state.ticker.take();
        state.renderer = Some(Arc::clone(&renderer));

        // Spawn a new ticker if enabled.
        let ticker =
            if terminal.ticker_enabled { spawn_ticker(Arc::downgrade(&renderer)) } else { None };
        state.ticker = ticker;
        state.screen = Some(ScreenState { id });

        ManagedScreen {
            inner: Some(Arc::clone(&terminal.inner)),
            renderer: Some(renderer),
            screen_id: id,
            joined: std::sync::atomic::AtomicBool::new(false),
        }
    }
}

// ---- ManagedScreen -----------------------------------------------------

/// A handle for one live screen inside a [`ProgressTerminal`].
///
/// Bars are added via [`add_bar`](Self::add_bar) and the screen is
/// committed into scrollback via [`join`](Self::join).  Dropping a screen
/// without an explicit `join` also commits it (the `Drop` impl calls
/// `join`).
///
/// To create a no-op screen, use [`ManagedScreen::disabled`].
pub struct ManagedScreen {
    /// Shared terminal inner state (`None` when disabled).
    inner: Option<Arc<TerminalInner>>,
    /// Per-screen renderer (`None` when disabled).
    renderer: Option<Arc<Mutex<ProgressRenderer>>>,
    /// Unique identifier for this screen.
    screen_id: ScreenId,
    /// Whether this screen has been joined (idempotent flag).
    joined: std::sync::atomic::AtomicBool,
}

impl ManagedScreen {
    /// Create a disabled (no-op) screen.
    ///
    /// All bars added are no-ops.  Joining is a no-op.
    #[must_use]
    pub fn disabled() -> Self {
        Self {
            inner: None,
            renderer: None,
            screen_id: ScreenId(0),
            joined: std::sync::atomic::AtomicBool::new(true),
        }
    }

    /// Whether this screen is the currently live screen on the terminal.
    fn is_live(&self) -> bool {
        let Some(ref inner) = self.inner else {
            return false;
        };
        let state = inner.state.lock().unwrap_or_else(std::sync::PoisonError::into_inner);
        state.screen.as_ref().is_some_and(|s| s.id == self.screen_id)
    }

    /// Add a child bar to the screen.
    ///
    /// `label` is parsed into [`PrefixComponents`](super::PrefixComponents) at construction.
    ///
    /// # Panics
    ///
    /// Panics if this screen is not the live screen (already joined or dropped).
    #[must_use]
    pub fn add_bar(&self, total: u64, label: &str) -> ProgressBarHandle {
        self.add_bar_with_style(total, label, BarStyle::StepCount)
    }

    /// Add a child bar with an explicit [`BarStyle`].
    ///
    /// See [`add_bar`](Self::add_bar) for the default-style variant.
    ///
    /// # Panics
    ///
    /// Panics if this screen is not the live screen (already joined or dropped).
    pub fn add_bar_with_style(
        &self,
        total: u64,
        label: &str,
        style: BarStyle,
    ) -> ProgressBarHandle {
        let Some(ref renderer) = self.renderer else {
            return ProgressBarHandle::disabled();
        };
        // Check live status before acquiring the renderer lock.
        if !self.is_live() {
            panic!(
                "ManagedScreen is not the live screen \
                 (already joined or dropped)"
            );
        }
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
        ProgressBarHandle { state }
    }

    /// Commit this screen's bars into scrollback.
    ///
    /// Renders a final frame, removes the screen from the terminal, and
    /// marks all bars as committed.  After this call, the bars are
    /// immutable — subsequent terminal ticks will never repaint them.
    ///
    /// Idempotent — calling `join` on an already-joined screen is a no-op.
    pub fn join(&self) {
        if self.joined.swap(true, std::sync::atomic::Ordering::AcqRel) {
            return; // already joined
        }
        let Some(ref inner) = self.inner else { return };

        // Render the final frame and unregister the screen.
        let mut state = inner.state.lock().unwrap_or_else(std::sync::PoisonError::into_inner);
        // Finalize the renderer (render final frame, remove blank slots).
        if let Some(ref renderer) = state.renderer {
            renderer.lock().unwrap_or_else(std::sync::PoisonError::into_inner).finalize();
        }
        // Unregister the screen (allows the next screen to be created).
        if state.screen.as_ref().is_some_and(|s| s.id == self.screen_id) {
            state.screen = None;
            state.renderer = None;
            state.ticker.take();
        }
    }

    /// Alias of [`join()`](Self::join) for call-site compatibility.
    pub fn join_and_clear(&self) {
        self.join();
    }

    /// Force a render sync (used in tests with
    /// [`InMemoryTerm`](indicatif::InMemoryTerm) where the timer
    /// thread does not run).
    pub fn tick(&self) {
        let Some(ref renderer) = self.renderer else { return };
        super::terminal::run_terminal_frame(renderer);
    }
}

impl Drop for ManagedScreen {
    fn drop(&mut self) {
        self.join();
    }
}

/// Execute one frame on the live screen's renderer.
///
/// Suppresses the write gate, recomputes the per-screen layout, handles
/// terminal resize, syncs all dirty slots (including rate/ETA computation
/// and debug emission), performs the one-shot pre-roll, then draws once
/// by opening the gate and ticking active bars.
///
/// This is the single frame entry point for [`ManagedScreen::tick`].
/// The renderer's own [`tick`](ProgressRenderer::tick) method is used
/// only by the ticker thread for autonomous animation.
pub(crate) fn run_terminal_frame(renderer: &std::sync::Mutex<ProgressRenderer>) {
    // Recompute per-screen layout.
    {
        let r = renderer.lock().unwrap_or_else(std::sync::PoisonError::into_inner);
        r.recompute_layout();
    }

    // Handle terminal resize.
    let resized = {
        let mut r = renderer.lock().unwrap_or_else(std::sync::PoisonError::into_inner);
        r.maybe_adjust_for_resize()
    };
    if resized {
        let r = renderer.lock().unwrap_or_else(std::sync::PoisonError::into_inner);
        for slot in &r.slots {
            if let Some(ref source) = *slot.source.borrow() {
                source.dirty.store(true, std::sync::atomic::Ordering::Release);
            }
        }
    }

    // Sync all dirty slots (rate/ETA, debug emission).
    {
        let mut r = renderer.lock().unwrap_or_else(std::sync::PoisonError::into_inner);
        r.sync_all_dirty_slots(resized);
    }

    // Pre-roll (one-shot, bypasses buffer).
    {
        let r = renderer.lock().unwrap_or_else(std::sync::PoisonError::into_inner);
        r.pre_roll_if_needed();
    }

    // Draw: tick active bars (gate opened implicitly by indicatif).
    {
        let r = renderer.lock().unwrap_or_else(std::sync::PoisonError::into_inner);
        for slot in &r.slots {
            if let Some(ref source) = *slot.source.borrow()
                && !source.is_finished()
            {
                slot.bar.tick();
            }
        }
    }
}

// ---- Ticker -------------------------------------------------------------

/// Spawn a dedicated thread that drives render updates at 50 ms intervals.
/// Holds a [`Weak`] reference so the thread exits cleanly when the renderer
/// is dropped.  Returns `None` when the thread could not be spawned.
fn spawn_ticker(renderer: Weak<Mutex<ProgressRenderer>>) -> Option<std::thread::JoinHandle<()>> {
    match std::thread::Builder::new().name("mediapm-progress-ticker".into()).spawn(move || {
        let mut all_done = false;
        loop {
            let sleep_ms = if all_done { 1000 } else { 50 };
            std::thread::sleep(Duration::from_millis(sleep_ms));
            let Some(r) = renderer.upgrade() else { break };
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
