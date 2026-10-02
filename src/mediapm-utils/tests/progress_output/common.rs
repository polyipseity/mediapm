//! Shared scaffolding for the exact-output progress suite.
//!
//! Every constructor in this module builds a [`ProgressTerminal`] whose draw
//! target is the [`InMemoryTerm`] it returns, with the daemon ticker disabled
//! so frames are produced only by explicit `tick()` calls. The screen type is
//! deliberately never named here: tests call `terminal.screen().build()`
//! themselves, so this module keeps compiling when the screen type is renamed.
//!
//! Two properties of the harness are load-bearing for every exact assertion:
//!
//! - The renderer's [`DimensionSource`] is pinned to the same `(rows, cols)`
//!   pair the [`InMemoryTerm`] was created with. Left at the default it would
//!   read the real terminal (24 rows here), so height adaptation would move
//!   every drawn frame off the captured grid and exact assertions would compare
//!   `""` with `""`.
//! - Slot capacity is independent of terminal height. It is a parameter because
//!   a screen's frame is exactly one line per reserved slot: a frame of `rows`
//!   lines fills the captured grid completely, so a test that must keep two
//!   screens' output visible needs `capacity < rows`.
//!
//! `with_multi_progress` is used rather than `with_term_like` because it is the
//! path the existing exact strings were captured on: it installs a no-op write
//! gate, so frames are drawn by indicatif's own bar operations rather than by
//! the terminal's buffered gate protocol.
//!
//! The two paths are **not** interchangeable, and the number of writes is not
//! the only difference. The gate changes what indicatif *erases*: a draw it
//! discards never reached the terminal, so the release that follows it erases
//! from the position the terminal is really at rather than from the one the
//! discarded draw's own accounting assumed. It also changes what is *rendered*:
//! a bar finished before that last released draw shows the done style (full bar,
//! elapsed only) on the ungated path but the active style (empty bar, elapsed
//! and rate) on the gated one, because the discarded draw is the one that would
//! have applied the done style. A committed frame therefore survives the next
//! screen on the gated path for a reason of its own — the cursor advance the
//! commit issues — and not because the ungated path's behaviour carries over. A
//! test that must pin what production does builds through
//! [`mk_with_capacity_gated`] instead of assuming that survival is a property
//! of the frame content.

use std::sync::{Arc, Mutex};

use indicatif::{InMemoryTerm, MultiProgress, ProgressDrawTarget, TermLike};
use mediapm_utils::progress::{
    DimensionSource, ProgressDebugSink, ProgressTerminal, TestDimensionSource, TestTimeSource,
    TimeSource,
};

/// Default terminal height. `mk_with_size` pins both the grid and the capacity
/// to it, so `mk_with_size(H, W)` is the canonical full-size harness.
pub const H: u16 = 24;
/// Default terminal width. Bar geometry is a function of the draw target's
/// width, so this is what makes a captured line a fixed length.
pub const W: u16 = 40;

/// Build a terminal at an explicit size, capacity pinned to `rows`.
///
/// The capacity equals the height so a frame exactly fills the captured grid;
/// use [`mk_with_capacity`] when a test needs a spare row.
pub fn mk_with_size(rows: u16, cols: u16) -> (ProgressTerminal, InMemoryTerm) {
    mk_with_capacity(rows, cols, rows as usize)
}

/// Build a terminal whose slot reservation is smaller than the terminal height.
///
/// A screen's frame is one line per reserved slot, so `capacity < rows` is the
/// only configuration in which an earlier screen's committed line stays inside
/// [`InMemoryTerm`]'s visible grid after a later screen draws a full frame.
pub fn mk_with_capacity(rows: u16, cols: u16, capacity: usize) -> (ProgressTerminal, InMemoryTerm) {
    build(rows, cols, capacity, Arc::new(TestDimensionSource::new((rows, cols))), None, false, None)
}

/// Build a terminal on the **gated** draw path, with the same slot reservation
/// as [`mk_with_capacity`].
///
/// [`mk_with_capacity`] installs a no-op write gate; this helper goes through
/// `with_term_like`, the path `ProgressTerminal` builds outside tests, so every
/// frame draw happens under the buffered write gate. A test needs it when what
/// it asserts depends on *which* draws reached the terminal rather than on the
/// frame content alone — a discarded draw changes what the next released draw
/// erases, not only how many writes happen (see the module docs).
pub fn mk_with_capacity_gated(
    rows: u16,
    cols: u16,
    capacity: usize,
) -> (ProgressTerminal, InMemoryTerm) {
    let grid = InMemoryTerm::new(rows, cols);
    let ts = Arc::new(TestTimeSource::new());
    let builder = ProgressTerminal::builder()
        .with_term_like(Box::new(grid.clone()))
        .with_dim_source(
            Arc::new(TestDimensionSource::new((rows, cols))) as Arc<dyn DimensionSource>
        )
        .capacity(capacity)
        .with_pre_roll_capture(Box::new(InMemoryTerm::new(rows, cols)))
        .with_time_source(Arc::clone(&ts) as Arc<dyn TimeSource>)
        .with_ticker_enabled(false);
    (builder.build(), grid)
}

/// Build a terminal with an explicit capacity and an injectable time source.
///
/// [`TestTimeSource`] makes elapsed and rate values deterministic; every exact
/// assertion that involves `s` or `/d` fields needs it.
pub fn mk_with_capacity_and_ts(
    rows: u16,
    cols: u16,
    capacity: usize,
    ts: &Arc<TestTimeSource>,
) -> (ProgressTerminal, InMemoryTerm) {
    build(
        rows,
        cols,
        capacity,
        Arc::new(TestDimensionSource::new((rows, cols))),
        Some(Arc::clone(ts)),
        false,
        None,
    )
}

/// Build a terminal driven by a caller-owned dimension source.
///
/// The caller keeps its own clone so it can change dimensions mid-test; the
/// grid is sized `rows`×`cols` independently of the dimension source, which is
/// what lets a test render at a narrow layout width inside a wide grid.
/// `dynamic_height` enables the renderer's height adaptation, and `ts` supplies
/// deterministic timing when the assertion needs it.
pub fn mk_with_dims(
    rows: u16,
    cols: u16,
    capacity: usize,
    dims: &Arc<TestDimensionSource>,
    ts: Option<&Arc<TestTimeSource>>,
    dynamic_height: bool,
) -> (ProgressTerminal, InMemoryTerm) {
    build(rows, cols, capacity, Arc::clone(dims), ts.map(Arc::clone), dynamic_height, None)
}

/// Build a terminal whose frames are written to `sink` as JSONL.
pub fn mk_with_debug_sink(
    rows: u16,
    cols: u16,
    capacity: usize,
    sink: ProgressDebugSink,
) -> (ProgressTerminal, InMemoryTerm) {
    build(
        rows,
        cols,
        capacity,
        Arc::new(TestDimensionSource::new((rows, cols))),
        None,
        false,
        Some(sink),
    )
}

/// Build a terminal whose pre-roll writes go to a caller-supplied term.
///
/// Pre-roll scrolls the existing terminal content away once per terminal, so
/// what it writes must be observable without landing on fd 2. It must be
/// recorded through a [`TermLike`], not through an [`InMemoryTerm`]: an
/// `InMemoryTerm` drops trailing blank rows from `contents()`, and pre-roll
/// writes blank lines only, so a capture term of that type records nothing.
pub fn mk_with_pre_roll_term(
    rows: u16,
    cols: u16,
    capacity: usize,
    term: Box<dyn TermLike>,
) -> (ProgressTerminal, InMemoryTerm) {
    let grid = InMemoryTerm::new(rows, cols);
    let target = ProgressDrawTarget::term_like(Box::new(grid.clone()));
    let builder = ProgressTerminal::builder()
        .with_multi_progress(MultiProgress::with_draw_target(target))
        .with_dim_source(
            Arc::new(TestDimensionSource::new((rows, cols))) as Arc<dyn DimensionSource>
        )
        .capacity(capacity)
        .with_pre_roll_capture(term)
        .with_ticker_enabled(false);
    (builder.build(), grid)
}

/// The single construction path behind the `mk*` helpers that do not need a
/// custom pre-roll term.
///
/// Every terminal gets an injectable clock: the caller's when it has one, a
/// fresh [`TestTimeSource`] otherwise. A bar's rate and eta are derived from its
/// clock, so a terminal left on the real clock renders different digits on every
/// run (a bar can even alternate between a rate and the `0/d` placeholder) and a
/// frame containing one is not assertable exactly. A fresh clock is frozen at its
/// creation instant, which renders as the zero elapsed and the placeholder rate.
///
/// Pre-roll gets a throwaway [`InMemoryTerm`] so it never writes to fd 2: it
/// emits blank lines and cursor moves, and letting it default to
/// `console::Term::stderr()` would spray escape sequences over the test
/// runner's own output.
fn build(
    rows: u16,
    cols: u16,
    capacity: usize,
    dims: Arc<TestDimensionSource>,
    ts: Option<Arc<TestTimeSource>>,
    dynamic_height: bool,
    sink: Option<ProgressDebugSink>,
) -> (ProgressTerminal, InMemoryTerm) {
    let grid = InMemoryTerm::new(rows, cols);
    let target = ProgressDrawTarget::term_like(Box::new(grid.clone()));
    let ts = ts.unwrap_or_else(|| Arc::new(TestTimeSource::new()));
    let mut builder = ProgressTerminal::builder()
        .with_multi_progress(MultiProgress::with_draw_target(target))
        .with_dim_source(dims as Arc<dyn DimensionSource>)
        .capacity(capacity)
        .dynamic_height(dynamic_height)
        .with_pre_roll_capture(Box::new(InMemoryTerm::new(rows, cols)))
        .with_time_source(Arc::clone(&ts) as Arc<dyn TimeSource>)
        .with_ticker_enabled(false);
    if let Some(sink) = sink {
        builder = builder.with_progress_debug_sink(sink);
    }
    (builder.build(), grid)
}

// ---- Frame inspection helpers -------------------------------------------

/// Number of cells a rendered frame spends on bar fills and blanks.
///
/// This is the rendered bar width, independent of how much of it is filled, so
/// it is the observable that responds to the draw target's width.
pub fn bar_cells(contents: &str) -> usize {
    contents.chars().filter(|&cell| cell == '█' || cell == '░').count()
}

/// Columns one captured line occupies.
///
/// [`InMemoryTerm`] strips the escapes the renderer writes, so counting the
/// characters of a line is its drawn width. A line longer than the terminal it
/// was captured at is a row that wrapped.
pub fn drawn_width(line: &str) -> usize {
    line.chars().count()
}

/// The first line of `contents` that contains `label`.
///
/// Panics when no line matches: for a test that has just asserted the bar was
/// bound, a missing line is the failure being reported, not an expected state.
pub fn line_with<'a>(contents: &'a str, label: &str) -> &'a str {
    contents
        .lines()
        .find(|line| line.contains(label))
        .unwrap_or_else(|| panic!("no line containing {label:?} in {contents:?}"))
}

/// `contents` with the spinner glyph blanked out of every drawn line.
///
/// The spinner advances on **every** draw, including draws where no tracked
/// value changed, so "the frame is unchanged" is only meaningful modulo the
/// spinner. Blanking the first character of each non-empty line (the spinner is
/// always the first character of a bar line) leaves the prefix, the bar, and the
/// whole suffix comparable byte for byte.
pub fn without_spinners(contents: &str) -> String {
    contents
        .lines()
        .map(|line| {
            let mut chars = line.chars();
            match chars.next() {
                Some(first) if first != ' ' => format!(" {}", chars.as_str()),
                _ => line.to_string(),
            }
        })
        .collect::<Vec<_>>()
        .join("\n")
}

// ---- Process-env serialization ------------------------------------------

/// Process-wide lock for tests that mutate `MEDIAPM_PROGRESS_DEBUG`.
///
/// The variable is process-global while cargo runs the tests of one binary
/// concurrently, so a test that sets it must hold this lock for the whole
/// window in which the sink is created and used.
pub static ENV_LOCK: Mutex<()> = Mutex::new(());

/// RAII guard that saves and restores a process environment variable.
pub struct EnvVarGuard {
    /// The variable being managed.
    key: &'static str,
    /// Its value before the guard was created; `None` when it was unset.
    previous: Option<String>,
}

impl EnvVarGuard {
    /// Set `key` to `value` for the guard's lifetime and restore it on drop.
    ///
    /// # Safety
    ///
    /// The caller must hold [`ENV_LOCK`] for the guard's whole lifetime: the
    /// process environment is shared by every concurrently running test in the
    /// same binary, and `std::env::set_var` is only sound when no other thread
    /// reads or writes the environment.
    pub unsafe fn set(key: &'static str, value: &str) -> Self {
        // SAFETY: the caller holds ENV_LOCK, so no other thread touches the environment.
        let previous = std::env::var(key).ok();
        // SAFETY: the caller holds ENV_LOCK, so no other thread touches the environment.
        unsafe { std::env::set_var(key, value) };
        Self { key, previous }
    }
}

impl Drop for EnvVarGuard {
    fn drop(&mut self) {
        match &self.previous {
            // SAFETY: the guard is dropped while its creator still holds ENV_LOCK.
            Some(value) => unsafe { std::env::set_var(self.key, value) },
            // SAFETY: the guard is dropped while its creator still holds ENV_LOCK.
            None => unsafe { std::env::remove_var(self.key) },
        }
    }
}
