//! Tests for the terminal-owned draw target (Task 1).
//!
//! A screen created from a [`ProgressTerminal`] must draw through that
//! terminal's own draw target, gate, and debug sink, and pre-roll must fire
//! exactly once per terminal no matter how many screens a sync creates.

use std::io::Write;
use std::sync::{Arc, Mutex};
use std::time::Duration;

use indicatif::{InMemoryTerm, MultiProgress, ProgressDrawTarget};

use super::super::{
    DimensionSource, ProgressDebugSink, ProgressTerminal, TestDimensionSource, TestTimeSource,
    TimeSource,
};

/// Terminal dimensions used by every test in this module.  `capacity` is
/// pinned to the same value: `InMemoryTerm::contents()` only reflects frames
/// drawn at or above the terminal's own height, so a larger capacity makes
/// every assertion below read `""` even though the frame was drawn.
const ROWS: u16 = 10;
const COLS: u16 = 80;

/// Slot capacity for tests that draw **two** screens through one draw target.
///
/// indicatif stops printing bars once their cumulative height would exceed the
/// terminal height (`draw_target.rs`, "Stop here if printing this bar would
/// exceed the terminal height").  Every screen reserves its own `capacity`
/// slots on the shared [`MultiProgress`], and the first screen's committed bars
/// remain there, so two sequential screens need one row of headroom beyond the
/// reservation or the second screen's bars are clipped away entirely.
const SEQUENTIAL_CAPACITY: usize = ROWS as usize - 1;

/// A `Write` sink that keeps everything written to it, so a test can read the
/// JSONL frames a debug sink emitted.
#[derive(Clone, Default)]
struct SharedVec(Arc<Mutex<Vec<u8>>>);

impl SharedVec {
    /// Return everything written so far as UTF-8 lossily decoded text.
    fn as_string(&self) -> String {
        let bytes = self.0.lock().expect("capture lock").clone();
        String::from_utf8_lossy(&bytes).into_owned()
    }
}

impl Write for SharedVec {
    fn write(&mut self, buf: &[u8]) -> std::io::Result<usize> {
        self.0.lock().expect("capture lock").extend_from_slice(buf);
        Ok(buf.len())
    }

    fn flush(&mut self) -> std::io::Result<()> {
        Ok(())
    }
}

/// Build a terminal over an `InMemoryTerm` the test can read back.
///
/// The dimension source is pinned to the terminal size so layout and resize
/// decisions match the captured frames, and `capacity` is the number of slots
/// the screen reserves.  `capacity` must not exceed [`ROWS`] (see the module
/// constants); a test that draws two sequential screens passes
/// [`SEQUENTIAL_CAPACITY`].
fn terminal_with_term(term: &InMemoryTerm, capacity: usize) -> ProgressTerminal {
    let target = ProgressDrawTarget::term_like(Box::new(term.clone()));
    let dims = Arc::new(TestDimensionSource::new((ROWS, COLS)));
    // Pre-roll must be captured, not written to fd 2: `with_multi_progress`
    // leaves the builder default (`console::Term::stderr()`), so without this
    // every `screen().build()` below would emit ten newlines and cursor moves
    // straight past libtest's capture.  A dedicated terminal also keeps those
    // newlines out of the `contents()` assertions, which read `term` — the
    // draw target — not this capture.
    let pre_roll_capture = InMemoryTerm::new(ROWS, COLS);
    ProgressTerminal::builder()
        .with_multi_progress(MultiProgress::with_draw_target(target))
        .with_pre_roll_capture(Box::new(pre_roll_capture))
        .with_dim_source(dims as Arc<dyn DimensionSource>)
        .capacity(capacity)
        .with_ticker_enabled(false)
        .build()
}

/// A fresh terminal plus a [`ProgressTerminal`] drawing into it with a full
/// slot reservation — the shape the single-screen tests need.
fn term_terminal() -> (ProgressTerminal, InMemoryTerm) {
    let term = InMemoryTerm::new(ROWS, COLS);
    let terminal = terminal_with_term(&term, ROWS as usize);
    (terminal, term)
}

/// A terminal with no live screen draws nothing: this is the post-commit
/// steady state and must never repaint a retired screen.
#[test]
fn terminal_without_screen_draws_nothing() {
    let (t, term) = term_terminal();
    t.tick();
    assert_eq!(term.contents(), "");
}

/// Exactly one live screen per terminal: a second build panics loudly.
#[test]
#[should_panic(expected = "already has a live screen")]
fn second_live_screen_panics() {
    let (t, _term) = term_terminal();
    let _a = t.screen().build();
    let _b = t.screen().build();
}

/// Disabled terminal yields no-op screens that produce no output.
#[test]
fn disabled_terminal_is_inert() {
    let t = ProgressTerminal::disabled();
    let s = t.screen().build();
    s.add_bar(3, "x").advance(3);
    s.join();
}

/// A bar added to a terminal screen must reach the terminal's draw target.
///
/// Without this assertion every commit-on-join test passes vacuously by
/// comparing `""` to `""`.
#[test]
fn screen_bars_reach_the_terminal_draw_target() {
    let term = InMemoryTerm::new(ROWS, COLS);
    let terminal = terminal_with_term(&term, ROWS as usize);
    let screen = terminal.screen().build();
    let bar = screen.add_bar(2, "alpha");
    bar.advance(1);
    screen.tick();
    let contents = term.contents();
    assert!(contents.contains("alpha"), "bar never reached the terminal: {contents:?}");
}

/// Two sequential screens share one draw target, so the first screen's
/// committed line survives the second screen's frame.
#[test]
fn sequential_screens_share_one_draw_target() {
    let term = InMemoryTerm::new(ROWS, COLS);
    let terminal = terminal_with_term(&term, SEQUENTIAL_CAPACITY);
    let first = terminal.screen().build();
    first.add_bar(1, "alpha").finish_success();
    first.tick();
    first.join();
    let second = terminal.screen().build();
    second.add_bar(1, "beta").finish_success();
    second.tick();
    let contents = term.contents();
    assert!(contents.contains("alpha"), "first screen never rendered: {contents:?}");
    assert!(contents.contains("beta"), "second screen never rendered: {contents:?}");
}

/// The terminal's debug sink is shared with its screens, so a screen frame
/// emits a JSONL record.  Passing `None` into `build_screen` regresses
/// `MEDIAPM_PROGRESS_DEBUG`.
#[test]
fn terminal_debug_sink_receives_screen_frames() {
    let term = InMemoryTerm::new(ROWS, COLS);
    let target = ProgressDrawTarget::term_like(Box::new(term.clone()));
    let dims = Arc::new(TestDimensionSource::new((ROWS, COLS)));
    let capture = SharedVec::default();
    let sink = ProgressDebugSink::new(Box::new(capture.clone()));
    let terminal = ProgressTerminal::builder()
        .with_multi_progress(MultiProgress::with_draw_target(target))
        .with_pre_roll_capture(Box::new(InMemoryTerm::new(ROWS, COLS)))
        .with_dim_source(dims as Arc<dyn DimensionSource>)
        .capacity(ROWS as usize)
        .with_progress_debug_sink(sink)
        .with_ticker_enabled(false)
        .build();
    let screen = terminal.screen().build();
    screen.add_bar(1, "alpha").finish_success();
    screen.tick();
    assert!(!capture.as_string().is_empty(), "terminal debug sink received no frame records");
}

/// The debug snapshot must describe the frame that was just computed, not the
/// previous one.
///
/// The snapshot reports `slots_timing[i].rate`, which the frame loop recomputes
/// while syncing dirty slots.  Emitting the snapshot before that sync leaves
/// `rate_bytes_per_sec` at `0` on the very first tick that moves a bar, so the
/// JSONL stream reports no rate at all for that frame.
#[test]
fn terminal_debug_snapshot_reports_the_current_frame_rate() {
    let term = InMemoryTerm::new(ROWS, COLS);
    let target = ProgressDrawTarget::term_like(Box::new(term.clone()));
    let dims = Arc::new(TestDimensionSource::new((ROWS, COLS)));
    let ts = Arc::new(TestTimeSource::new());
    let capture = SharedVec::default();
    let sink = ProgressDebugSink::new(Box::new(capture.clone()));
    let terminal = ProgressTerminal::builder()
        .with_multi_progress(MultiProgress::with_draw_target(target))
        .with_pre_roll_capture(Box::new(InMemoryTerm::new(ROWS, COLS)))
        .with_dim_source(dims as Arc<dyn DimensionSource>)
        .with_time_source(Arc::clone(&ts) as Arc<dyn TimeSource>)
        .capacity(ROWS as usize)
        .with_progress_debug_sink(sink)
        .with_ticker_enabled(false)
        .build();

    let screen = terminal.screen().build();
    let bar = screen.add_bar(100, "alpha");
    // Give the slot a measurable interval, then move it by a known amount:
    // 10 bytes over 0.1 s is an instantaneous rate of 100 B/s.
    ts.advance(Duration::from_millis(100));
    bar.advance(10);
    screen.tick();

    let records = capture.as_string();
    let last = records.lines().last().expect("at least one tick record");

    // Every reserved slot reports a `rate_bytes_per_sec`, and only the bound one
    // carries the rate this frame computed.  Take the maximum of the numeric
    // fields so the assertion does not depend on slot ordering or on how many
    // reserved slots the frame has; `null` does not parse and is skipped.
    let max_of = |field: &str| -> f64 {
        last.split(field)
            .skip(1)
            .filter_map(|rest| rest.split([',', '}']).next())
            .filter_map(|raw| raw.parse::<f64>().ok())
            .fold(0.0_f64, f64::max)
    };

    let rate = max_of("\"rate_bytes_per_sec\":");
    assert!(
        rate > 0.0,
        "snapshot must report the rate the frame just computed, got {rate} in {last}"
    );

    // The same ordering drives ETA: with a live rate and a remaining total, the
    // bound slot's `eta_secs` must be populated rather than `null`.
    let eta = max_of("\"eta_secs\":");
    assert!(eta > 0.0, "snapshot must report the ETA derived from that rate, got {eta} in {last}");
}
