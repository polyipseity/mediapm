//! `ProgressTerminal`: the single owner of a draw target, write gate,
//! ticker thread, and pre-roll state for one sync operation.
//!
//! Exactly one [`ProgressScreen`] may be live inside a terminal at any time.
//! Creating a second screen while one is already live panics — the caller
//! must [`join`](ProgressScreen::join) or drop the current screen first.

use std::marker::PhantomData;
use std::sync::{Arc, Mutex, Weak};
use std::time::Duration;

use indicatif::{MultiProgress, ProgressDrawTarget, TermLike};

use super::HasOverall;
use super::NoOverall;
use super::PrefixComponents;
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
    /// Shared (`Arc`) because one sink belongs to the terminal and is used by
    /// every screen's renderer, so tick numbering stays monotonic across a
    /// sync instead of restarting per phase.
    debug_sink: Option<Arc<ProgressDebugSink>>,
    /// One-shot flag: has this terminal's pre-roll (newline scroll) already
    /// been written?
    ///
    /// Set by `TerminalInner::pre_roll_if_needed`, which `build_screen` calls
    /// while creating the first screen — so it is `true` as soon as that screen
    /// exists, not `true` only after the first frame draws. Later screens of
    /// the same sync observe `true` and skip the scroll.
    pre_rolled: std::sync::atomic::AtomicBool,
    /// Terminal to write pre-roll newlines to.
    ///
    /// Defaults to [`console::Term::stderr`]. `with_pre_roll_capture` replaces
    /// it so a test can assert on the scroll instead of writing to fd 2 —
    /// `with_multi_progress` does **not** change it, so a test that sets up a
    /// capture [`MultiProgress`] must also install a pre-roll capture unless it
    /// wants real stderr writes. `None` only for `ProgressTerminal::disabled`,
    /// where `TerminalInner::pre_roll_if_needed` is a no-op.
    pre_roll_term: Option<Box<dyn TermLike>>,
}

impl TerminalInner {
    /// Write the one-shot pre-roll the first time it is called.
    ///
    /// Pre-roll emits blank lines so existing terminal content scrolls into
    /// scrollback before the first bar draws, instead of being overwritten.
    /// It belongs to the terminal rather than a screen: a sync with three
    /// phase screens must scroll once.  A `None` `pre_roll_term` — set only by
    /// `ProgressTerminal::disabled` — makes this a no-op; a caller-supplied
    /// [`MultiProgress`] does not produce `None`.
    fn pre_roll_if_needed(&self) {
        if self.pre_rolled.swap(true, std::sync::atomic::Ordering::AcqRel) {
            return;
        }
        let Some(ref term) = self.pre_roll_term else { return };
        let rows = self.dim_source.dimensions().0 as usize;
        let _ = term.move_cursor_down(rows);
        for _ in 0..rows {
            let _ = term.write_line("");
        }
        let _ = term.move_cursor_up(rows);
    }
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

/// Builder for [`ProgressTerminal`].
///
/// Created by [`ProgressTerminal::builder`]. The builder configures the
/// terminal's draw target, dimension source, time source, pre-roll, debug
/// sink, and ticker settings. An overall bar is not a terminal concern: it
/// belongs to a screen, added with
/// [`ProgressTerminal::screen`] + `with_overall()`.
pub struct ProgressTerminalBuilder {
    mp_and_gate: Option<(MultiProgress, WriteGate)>,
    dim_source: Arc<dyn DimensionSource>,
    capacity: Option<usize>,
    dynamic_height: bool,
    time_source: Arc<dyn TimeSource>,
    pre_roll_term: Option<Box<dyn TermLike>>,
    debug_sink: Option<ProgressDebugSink>,
    ticker_enabled: bool,
}

impl Default for ProgressTerminalBuilder {
    fn default() -> Self {
        Self {
            mp_and_gate: None,
            dim_source: Arc::new(RealTerminalSource),
            capacity: None,
            dynamic_height: false,
            time_source: Arc::new(RealTimeSource),
            pre_roll_term: Some(Box::new(console::Term::stderr())),
            debug_sink: None,
            ticker_enabled: true,
        }
    }
}

/// Configuration methods for [`ProgressTerminalBuilder`].
impl ProgressTerminalBuilder {
    /// Use an injectable [`TermLike`] instead of creating a `BufferedTerm`
    /// over [`console::Term::stderr`]. Always wraps the term in a
    /// `BufferedTerm` so the write-gate protocol is exercised in tests.
    #[must_use]
    pub fn with_term_like(mut self, term: Box<dyn TermLike>) -> Self {
        let (buffered, gate) = super::gate::BufferedTerm::new(term);
        let mp = MultiProgress::with_draw_target(ProgressDrawTarget::term_like(Box::new(buffered)));
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

impl ProgressTerminalBuilder {
    /// Build a terminal.
    ///
    /// An overall bar belongs to a screen, not to the terminal:
    /// `terminal.screen().with_overall(label, total).build()` registers it as
    /// the pinned bottom slot of that screen.
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
        let debug_sink = self.debug_sink.or_else(detect_progress_debug_env).map(Arc::new);
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
            enabled: true,
        }
    }
}

// ---- ProgressTerminal ---------------------------------------------------

/// The single owner of a draw target, write gate, ticker thread, and
/// pre-roll state for one sync operation.
///
/// Create via [`ProgressTerminal::builder`] or [`ProgressTerminal::disabled`].
/// Exactly one [`ProgressScreen`] may be live at any time — creating a
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
    /// Whether this terminal may create live screens.
    ///
    /// `false` only for [`ProgressTerminal::disabled`], whose draw target is
    /// hidden and whose [`screen`](Self::screen) hands out
    /// [`ProgressScreen::disabled`] instead of building a renderer. A disabled
    /// terminal therefore allocates no bars, starts no ticker, writes no
    /// pre-roll and draws nothing, which is what makes it a `--no-progress`
    /// building block rather than a live terminal that merely skips pre-roll.
    enabled: bool,
}

impl ProgressTerminal {
    /// Create a builder for configuring a [`ProgressTerminal`].
    #[must_use]
    pub fn builder() -> ProgressTerminalBuilder {
        ProgressTerminalBuilder::default()
    }

    /// Create a no-op terminal that produces no terminal output.
    ///
    /// The terminal is inert end to end: its draw target is hidden, its
    /// [`screen`](Self::screen) builder returns
    /// [`ProgressScreen::disabled`] (so it allocates no bars and its handles
    /// are no-ops), and neither pre-roll nor a render ticker ever starts.
    /// Useful in tests where progress is not needed, and as the `--no-progress`
    /// building block for callers that must keep adding bars to the screen they
    /// were handed.
    #[must_use]
    pub fn disabled() -> Self {
        Self {
            inner: Arc::new(TerminalInner {
                // A hidden draw target: even a bar that somehow reached this
                // terminal's `MultiProgress` would render nowhere.
                mp: MultiProgress::with_draw_target(ProgressDrawTarget::hidden()),
                gate: super::gate::WriteGate::new_noop(),
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
            enabled: false,
        }
    }

    /// Begin building a new screen on this terminal.
    ///
    /// Returns a `TerminalScreenBuilder` that can configure dynamic height,
    /// capacity, and an optional overall bar before calling `.build()`.
    ///
    /// # Panics
    ///
    /// Panics if a screen is already live — the caller must
    /// [`join`](ProgressScreen::join) or drop the current screen first.
    #[must_use]
    pub fn screen(&self) -> TerminalScreenBuilder<'_, NoOverall> {
        TerminalScreenBuilder { terminal: self, overall: None, _state: PhantomData }
    }

    /// Force a render sync. Tests use this with the in-memory terminal from
    /// `indicatif`'s `in_memory` feature, where the timer thread does not run.
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

/// Builder for [`ProgressScreen`] with compile-time overall-bar enforcement.
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
    /// A screen built on [`ProgressTerminal::disabled`] is itself disabled: it
    /// allocates no bar and draws nothing, while still accepting every
    /// `add_bar` call a `--no-progress` run makes.
    ///
    /// # Panics
    ///
    /// Panics when a screen is already live on the terminal.
    #[must_use]
    pub fn build(self) -> ProgressScreen {
        if !self.terminal.enabled {
            return ProgressScreen::disabled();
        }
        // A `NoOverall` builder never carries an overall spec, so no overall
        // bar is registered and no handle is returned.
        build_screen(self.terminal, None)
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

impl TerminalScreenBuilder<'_, HasOverall> {
    /// Build a screen with the overall bar pinned at the bottom slot.
    ///
    /// Returns both the [`ProgressScreen`] and a [`ProgressBarHandle`] for the
    /// overall bar. The handle wraps the very [`SharedState`] the renderer's
    /// bottom slot draws from, so driving the handle moves the rendered overall
    /// bar; a handle whose state the renderer never read would render a
    /// permanently idle bar. On [`ProgressTerminal::disabled`] both are no-ops:
    /// the screen is [`ProgressScreen::disabled`] and the handle is
    /// [`ProgressBarHandle::disabled`].
    ///
    /// # Panics
    ///
    /// Panics when a screen is already live on the terminal.
    #[must_use]
    pub fn build(self) -> (ProgressScreen, ProgressBarHandle) {
        // A disabled terminal has no renderer to register the bottom slot on,
        // so it hands back the no-op handle rather than a state nothing reads.
        if !self.terminal.enabled {
            return (ProgressScreen::disabled(), ProgressBarHandle::disabled());
        }
        let (label, total) = self.overall.expect("HasOverall builder must have overall set");
        let state = Arc::new(SharedState::with_time_source(
            total,
            &label,
            Arc::clone(&self.terminal.inner.time_source),
        ));
        let handle = ProgressBarHandle { state: Arc::clone(&state) };
        let screen = build_screen(self.terminal, Some(state));
        (screen, handle)
    }
}

/// Internal: create the one live [`ProgressScreen`] on the given terminal.
///
/// The screen's renderer draws through the terminal's single [`MultiProgress`]
/// and honours the terminal's [`WriteGate`], so every screen of a sync renders
/// into the same draw target and frames are buffered as a unit. The screen also
/// inherits the terminal's dimension source, time source and JSONL debug sink.
///
/// `overall` is the caller's overall handle state; when `Some`, the renderer
/// registers it as the pinned bottom slot, adopting that exact `Arc` so the
/// caller's handle drives the drawn bar. `None` leaves the slot grid without an
/// overall bar.
///
/// # Panics
///
/// Panics when a screen is already live on this terminal: two live screens
/// would mean two renderers driving one [`MultiProgress`].
fn build_screen(terminal: &ProgressTerminal, overall: Option<Arc<SharedState>>) -> ProgressScreen {
    // Check that no screen is live.
    {
        let mut state =
            terminal.inner.state.lock().unwrap_or_else(std::sync::PoisonError::into_inner);
        assert!(
            state.screen.is_none(),
            "ProgressTerminal already has a live screen; \
             join() or drop the current screen before creating a new one"
        );
        let id = ScreenId(state.next_id);
        state.next_id += 1;

        // One pre-roll per terminal, before any bar of the first screen draws:
        // the bars must appear below the scrolled content, not over it.
        terminal.inner.pre_roll_if_needed();

        let mut r = ProgressRenderer::from_mp(
            terminal.inner.mp.clone(),
            terminal.capacity,
            Arc::clone(&terminal.inner.dim_source),
            terminal.inner.gate.clone(),
            Arc::clone(&terminal.inner.time_source),
            terminal.inner.debug_sink.clone(),
        );
        r.dynamic_height = terminal.dynamic_height;
        if let Some(state) = overall {
            r.add_overall(state);
        }
        let renderer = Arc::new(Mutex::new(r));

        // Stop any existing ticker.
        state.ticker.take();
        state.renderer = Some(Arc::clone(&renderer));

        // Spawn a new ticker if enabled.
        let ticker =
            if terminal.ticker_enabled { spawn_ticker(Arc::downgrade(&renderer)) } else { None };
        state.ticker = ticker;
        state.screen = Some(ScreenState { id });

        ProgressScreen {
            inner: Some(Arc::clone(&terminal.inner)),
            renderer: Some(Arc::downgrade(&renderer)),
            screen_id: id,
            joined: std::sync::atomic::AtomicBool::new(false),
        }
    }
}

// ---- ProgressScreen -----------------------------------------------------

/// A handle for one live screen inside a [`ProgressTerminal`].
///
/// Bars are added via [`add_bar`](Self::add_bar) and the screen is
/// committed into scrollback via [`join`](Self::join).  Dropping a screen
/// without an explicit `join` also commits it (the `Drop` impl calls
/// `join`).
///
/// To create a no-op screen, use [`ProgressScreen::disabled`]: it is not the
/// live screen and hands out no-op handles instead of drawing.
pub struct ProgressScreen {
    /// Shared terminal inner state (`None` when disabled).
    inner: Option<Arc<TerminalInner>>,
    /// Per-screen renderer (`None` when disabled).
    ///
    /// Deliberately [`Weak`]: the terminal's `TerminalState.renderer` is the
    /// only strong owner, so [`join`](Self::join) can drop the renderer (and
    /// with it every [`ProgressBar`](indicatif::ProgressBar) the screen
    /// reserved) by clearing its own field.  A strong clone here would keep
    /// the bars in the shared [`MultiProgress`] forever, which is the
    /// accumulated-reservation defect this handle exists to avoid.
    renderer: Option<Weak<Mutex<ProgressRenderer>>>,
    /// Unique identifier for this screen.
    screen_id: ScreenId,
    /// Whether this screen has been joined (idempotent flag).
    joined: std::sync::atomic::AtomicBool,
}

impl ProgressScreen {
    /// Create a disabled (no-op) screen.
    ///
    /// Every bar this screen hands out is a no-op handle, so the `--no-progress`
    /// path can keep adding child bars to it without allocating slots or drawing.
    /// Joining is a no-op.
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

    /// Add a child bar whose prefix fields are given as components.
    ///
    /// This is the constructor to reach for when the bar carries structure: a
    /// name, a version, a phase. [`add_bar`](Self::add_bar) fits a label the
    /// renderer can read back out of (a single word), because the display
    /// string it takes is parsed into components at construction and any field
    /// the string cannot express is lost there.
    ///
    /// # Panics
    ///
    /// Panics if this screen is not the live screen (already joined or dropped).
    #[must_use]
    pub fn add_bar_with_prefix(&self, total: u64, prefix: &PrefixComponents) -> ProgressBarHandle {
        self.attach_state(|time_source| {
            SharedState::with_prefix(total, prefix, BarStyle::StepCount, time_source)
        })
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
        self.attach_state(|time_source| {
            SharedState::with_time_source_and_style(total, label, style, time_source)
        })
    }

    /// Build the bar state from `make` and attach it to the live renderer.
    ///
    /// `make` runs while the renderer is locked because it reads the shared
    /// clock, so the two entry points share the disabled/live checks and the
    /// attach rather than repeating them.
    fn attach_state(
        &self,
        make: impl FnOnce(Arc<dyn TimeSource>) -> SharedState,
    ) -> ProgressBarHandle {
        // A disabled screen has no live status to check: it is the `--no-progress`
        // screen, and callers such as the materializer add bars to whatever group
        // they were handed, so it must hand back no-op handles rather than panic.
        if self.inner.is_none() {
            return ProgressBarHandle::disabled();
        }
        // Check live status first: a committed screen must panic rather than
        // fall through to the disabled path when its renderer is already gone.
        assert!(
            self.is_live(),
            "ProgressScreen is not the live screen (already joined or dropped)"
        );
        let Some(ref weak) = self.renderer else {
            return ProgressBarHandle::disabled();
        };
        let Some(renderer) = weak.upgrade() else {
            return ProgressBarHandle::disabled();
        };
        let state;
        {
            let mut locked = renderer.lock().unwrap_or_else(std::sync::PoisonError::into_inner);
            state = Arc::new(make(Arc::clone(&locked.time_source)));
            locked.attach(&state);
        }
        ProgressBarHandle { state }
    }

    /// Commit this screen's bars into scrollback.
    ///
    /// Renders the final frame, clears the terminal's live-screen slot and
    /// releases the terminal's strong reference to the per-screen renderer.
    /// That drop cascades to every [`ProgressBar`](indicatif::ProgressBar) the
    /// screen reserved, which is what commits their lines: indicatif reaps a
    /// dropped bar as a zombie and retains its drawn lines, while returning the
    /// bar's slot index to the [`MultiProgress`] free set for the next screen.
    ///
    /// What retains the line of a bar that is still **unfinished** here depends on the draw target, so it is stated per configuration:
    ///
    /// - **Write-gated target** (the `BufferedTerm` a terminal wraps around its
    ///   term, which is what production draws through): the **write gate**
    ///   retains it. indicatif's `BarState::drop` finishes an unfinished bar and
    ///   draws it, but that draw lands after the window `finalize` closes, so
    ///   the gate suppresses it and the frame `finalize` drew is the last one
    ///   the terminal ever sees. The gate's role here is real: production
    ///   retention rests on it, and deleting this reasoning is not safe.
    /// - **Ungated target** (a bare [`MultiProgress`] whose gate is a no-op):
    ///   the **finish policy** retains it. Every slot bar carries
    ///   `ProgressFinish::AndLeave` (see `with_slot_finish_policy` in
    ///   [`ProgressRenderer`]); without it, indicatif's default
    ///   `ProgressFinish::AndClear` sets `Status::DoneHidden` in
    ///   `BarState::drop` and clears the bar's line on the way out.
    ///
    /// `with_slot_finish_policy` is therefore **defense-in-depth**, not a fix
    /// for an observed production defect: it keeps the retention contract from
    /// depending on the gate's window timing, so an ungated target, or a future
    /// drop path that draws inside an open window, still keeps the line. No
    /// production configuration was measured to lose an unfinished bar's line
    /// with the policy absent. The gated behavior is pinned by
    /// `progress::tests::screen::gated_terminal_retains_an_unfinished_bar`; the
    /// ungated half by `join_commits_an_unfinished_bar` and
    /// `drop_without_join_keeps_an_unfinished_bar`.
    ///
    /// One measured limit of that retention, and one derived bound:
    ///
    /// - On either configuration, a bar that was **finished** before the drop
    ///   keeps its line through `BarState::drop`'s `is_finished()` short-circuit,
    ///   which returns before the finish policy is ever consulted. Exercised by
    ///   `progress::tests::screen`.
    /// - Derived, **not** measured: the ticker's `Weak::upgrade()` fails here,
    ///   but a tick already in flight holds a strong clone of the renderer until
    ///   it returns, so the retired reservation can outlive `join` by up to one
    ///   tick. That bound follows from `ProgressRenderer::run_frame` returning
    ///   immediately once finalized (the guard `finalize` sets): the window is
    ///   a single no-op tick that drops the reservation when it ends, so a
    ///   screen built and drawn inside the window sees the retired reservation
    ///   for one frame. The overlap itself is unmeasured and cannot be forced
    ///   from a test, because it needs a join to land inside another thread's
    ///   upgrade.
    ///
    /// Idempotent: joining an already-joined (or never-live) screen is a no-op,
    /// which is what makes `Drop` safe to route through here.
    ///
    /// The terminal's state mutex is used unpoisoned: a poisoned lock is
    /// recovered with `PoisonError::into_inner` instead of being propagated, so
    /// this method has no panic path.
    pub fn join(&self) {
        if self.joined.swap(true, std::sync::atomic::Ordering::AcqRel) {
            return; // already joined
        }
        let Some(ref inner) = self.inner else { return };

        let mut state = inner.state.lock().unwrap_or_else(std::sync::PoisonError::into_inner);
        if !state.screen.as_ref().is_some_and(|s| s.id == self.screen_id) {
            return; // another screen is live; this one was already retired
        }
        // Finalize the renderer (render final frame, remove blank slots).
        if let Some(ref renderer) = state.renderer {
            renderer.lock().unwrap_or_else(std::sync::PoisonError::into_inner).finalize();
        }
        // Dropping the terminal's `Arc` drops the renderer, its slots and
        // therefore its bars.  The screen's own handle is a `Weak`, so it does
        // not keep any of them alive.
        state.renderer = None;
        state.screen = None;
        state.ticker.take();
    }

    /// Alias of [`join()`](Self::join) for call-site compatibility.
    pub fn join_and_clear(&self) {
        self.join();
    }

    /// Force a render sync. Tests use this with the in-memory terminal from
    /// `indicatif`'s `in_memory` feature, where the timer thread does not run.
    ///
    /// A no-op once the screen is committed: its renderer is gone (the
    /// terminal dropped the last strong reference) or already finalized.
    pub fn tick(&self) {
        let Some(ref weak) = self.renderer else { return };
        let Some(renderer) = weak.upgrade() else { return };
        renderer.lock().unwrap_or_else(std::sync::PoisonError::into_inner).tick();
    }
}

impl Drop for ProgressScreen {
    fn drop(&mut self) {
        self.join();
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
