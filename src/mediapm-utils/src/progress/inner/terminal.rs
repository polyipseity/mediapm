//! `ProgressTerminal`: the single owner of the frame writer's terminal
//! handle, ticker thread, and pre-roll state for one sync operation.
//!
//! Exactly one [`ProgressScreen`] may be live inside a terminal at any time.
//! Creating a second screen while one is already live panics — the caller
//! must [`join`](ProgressScreen::join) or drop the current screen first.

use std::marker::PhantomData;
use std::sync::{Arc, Mutex, Weak};
use std::time::Duration;

use indicatif::{MultiProgress, TermLike};

use super::HasOverall;
use super::NoOverall;
use super::PrefixComponents;
use super::{
    DimensionSource, MAX_SLOTS, ProgressBarHandle, ProgressDebugSink, ProgressRenderer,
    RealTerminalSource, RealTimeSource, SharedState, TimeSource, detect_progress_debug_env,
    gate::WriteGate,
};
use crate::progress::BarStyle;

// ---- ScreenId -----------------------------------------------------------

/// Unique identifier for a live screen inside a [`ProgressTerminal`].
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
struct ScreenId(u64);

// ---- TerminalInner ------------------------------------------------------

/// Shared inner state of a [`ProgressTerminal`].
///
/// Holds the frame writer's gate, injectable sources, pre-roll
/// state, and mutable screen tracking. The ticker thread holds a [`Weak`]
/// reference to the current renderer and exits cleanly when the terminal
/// is dropped or a new screen is created.
struct TerminalInner {
    /// The frame writer's terminal handle plus the screen-commit cursor
    /// advance. Every frame of every screen is written through this gate.
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
    /// The frame writer's terminal handle. `None` until a builder method
    /// installs one; [`build`](Self::build) defaults it to a gate over
    /// [`console::Term::stderr`].
    gate: Option<WriteGate>,
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
            gate: None,
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
    /// Use an injectable [`TermLike`] as the frame writer's terminal.
    ///
    /// The gate is created here rather than taken from a caller, so a screen
    /// is always built against the term its frames are written to.
    #[must_use]
    pub fn with_term_like(mut self, term: Box<dyn TermLike>) -> Self {
        self.gate = Some(WriteGate::new_noop_with_term(Arc::from(term)));
        self
    }

    /// Ignore `mp`: build the band's own frame writer instead.
    ///
    /// Screens are built against the frame writer (see
    /// [`with_term_like`](Self::with_term_like)); a band drawn through an
    /// indicatif `MultiProgress` is the retired path, and its tests are
    /// migrating onto the frame writer. The parameter is kept so those call
    /// sites compile unchanged while they migrate; the gate handed out here
    /// has no term, so a screen built through this seam discards its frames.
    /// [`MultiProgress`] no longer draws anything — the frame writer is the
    /// screen's single draw path. This legacy method therefore installs a
    /// gate with no terminal (frames are built and discarded) so legacy
    /// call sites keep compiling through the migration; a test that wants to
    /// read rendered output must pair its target with
    /// [`with_multi_progress_and_term`](Self::with_multi_progress_and_term) or
    /// [`with_term_like`](Self::with_term_like).
    #[must_use]
    pub fn with_multi_progress(self, _mp: MultiProgress) -> Self {
        Self { gate: Some(WriteGate::new_noop()), ..self }
    }

    /// Use an existing [`MultiProgress`] **and** give the frame writer a
    /// terminal to write its plans through.
    ///
    /// The ungated test harnesses build their draw target over an injectable
    /// term (the `InMemoryTerm` a fixture asserts on) while managing their
    /// own bars; under the single writer, `run_frame` renders one frame plan
    /// and writes it through this term — without it the frame would be built
    /// and discarded. The gate is a no-op beyond that pairing: there are no
    /// buffered draws to suppress or compensate for. The `MultiProgress`
    /// parameter is deliberately unused; the harness keeps ownership of the
    /// same term instance it passes as the draw target.
    #[must_use]
    pub fn with_multi_progress_and_term(
        mut self,
        _mp: MultiProgress,
        term: Box<dyn TermLike>,
    ) -> Self {
        self.gate = Some(WriteGate::new_noop_with_term(Arc::from(term)));
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
        let gate = self
            .gate
            .unwrap_or_else(|| WriteGate::new_noop_with_term(Arc::new(console::Term::stderr())));
        let debug_sink = self.debug_sink.or_else(detect_progress_debug_env).map(Arc::new);
        ProgressTerminal {
            inner: Arc::new(TerminalInner {
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
                // A termless gate: even a frame that somehow reached this
                // terminal is built and discarded.
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
    overall: Option<(OverallPrefix, u64)>,
    _state: PhantomData<S>,
}

/// What seeds the overall bar's prefix.
///
/// `Label` hands its string to [`SharedState::with_time_source`], which parses
/// it into components once at construction. `Components` keeps the fields the
/// caller already has, so a name or version the parser cannot express reaches
/// the frame instead of being read back out of a rendered string.
enum OverallPrefix {
    Label(String),
    Components(PrefixComponents),
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
            overall: Some((OverallPrefix::Label(label.to_string()), total)),
            _state: PhantomData,
        }
    }

    /// Add an overall aggregate bar whose prefix fields are given as components.
    ///
    /// Transitions the builder from [`NoOverall`] to [`HasOverall`], the same
    /// way [`with_overall`](Self::with_overall) does.
    ///
    /// Reach for this when the row carries a name, a version or a phase that
    /// the caller holds as fields. [`with_overall`](Self::with_overall) takes a
    /// display string and parses it, so a field that string cannot express (a
    /// tool name carrying a space, a version carrying a space) is reshaped on
    /// the way in.
    #[must_use]
    pub fn with_overall_prefix(
        self,
        prefix: &PrefixComponents,
        total: u64,
    ) -> TerminalScreenBuilder<'a, HasOverall> {
        TerminalScreenBuilder {
            terminal: self.terminal,
            overall: Some((OverallPrefix::Components(prefix.clone()), total)),
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
        let (prefix, total) = self.overall.expect("HasOverall builder must have overall set");
        let time_source = Arc::clone(&self.terminal.inner.time_source);
        let state = Arc::new(match prefix {
            OverallPrefix::Label(label) => {
                SharedState::with_time_source(total, &label, time_source)
            }
            OverallPrefix::Components(components) => {
                SharedState::with_prefix(total, &components, BarStyle::StepCount, time_source)
            }
        });
        let handle = ProgressBarHandle { state: Arc::clone(&state) };
        let screen = build_screen(self.terminal, Some(state));
        (screen, handle)
    }
}

/// Internal: create the one live [`ProgressScreen`] on the given terminal.
///
/// The screen's renderer writes through the terminal's frame writer and
/// honours the terminal's [`WriteGate`], so every screen of a sync renders
/// into the same terminal and frames are committed one at a time. The screen
/// also inherits the terminal's dimension source, time source and JSONL
/// debug sink.
///
/// `overall` is the caller's overall handle state; when `Some`, the renderer
/// registers it as the pinned bottom slot, adopting that exact `Arc` so the
/// caller's handle drives the drawn bar. `None` leaves the slot grid without an
/// overall bar.
///
/// # Panics
///
/// Panics when a screen is already live on this terminal: two live screens
/// would mean two renderers driving one terminal.
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

        let mut r = ProgressRenderer::new(
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
    /// with it every slot the screen reserved) by clearing its own field. A
    /// strong clone here would keep the retired grid alive forever, which is
    /// the accumulated-reservation defect this handle exists to avoid.
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
    /// `label` is parsed into [`PrefixComponents`] at construction.
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

    /// Commit this screen's frame into scrollback.
    ///
    /// Renders the final frame, advances the cursor past it, clears the
    /// terminal's live-screen slot and releases the terminal's strong
    /// reference to the per-screen renderer, which drops the slot grid.
    ///
    /// What retains the screen's last image is the final frame itself:
    /// `ProgressRenderer::finalize` renders the plan one
    /// more time with the blank rows gone and the live rows full, writes it,
    /// and advances the cursor past it (`WriteGate::commit_frame`), so the
    /// committed frame is no longer the live region the next output would
    /// overwrite. Nothing else needs to hold the renderer or its slots open
    /// for the lines to stay.
    ///
    /// One bound worth stating: a tick already in flight holds a strong
    /// clone of the renderer when the join lands, so the retired reservation
    /// can outlive `join` by up to one tick. That bound follows from
    /// `ProgressRenderer::run_frame` returning immediately once finalized
    /// (the guard `finalize` sets): the overlap is a single no-op tick that
    /// drops the reservation when it ends. The overlap itself is unmeasured
    /// and cannot be forced from a test, because it needs a join to land
    /// inside another thread's upgrade.
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
        // Finalize the renderer (render the final plan frame, remove blank
        // slots, advance the cursor past the committed frame).
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
