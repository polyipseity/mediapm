//! Regression tests for commit-on-join semantics (Spec S2/S3).
//!
//! Joining a screen must both *retain* the lines it drew (they become
//! committed output the terminal never touches again) and *release* the slots
//! that screen reserved on the shared draw target, so the next screen of a
//! sync renders in full below the committed frame.  Both halves are asserted
//! here: the retention tests below compare `contents()` across a later tick,
//! and `second_screen_renders_at_full_capacity` proves release by drawing a
//! second screen at the terminal's own height.

use std::sync::Arc;

use super::super::{DimensionSource, ProgressTerminal, TestDimensionSource};
use indicatif::{InMemoryTerm, MultiProgress, ProgressDrawTarget};

/// Terminal dimensions used by this module, matching the other terminal tests.
///
/// Both the dimension source and the slot capacity are pinned to the
/// `InMemoryTerm` size: without them `capacity` is derived from the real
/// terminal (24 rows here), so every drawn frame lands off the captured screen
/// and the `assert_eq!(term.contents(), after)` below would compare `""` to
/// `""` and pass no matter what commit-on-join did.
const ROWS: u16 = 10;
const COLS: u16 = 80;

/// Build a terminal over `term` that reserves `capacity` slots per screen.
///
/// The dimension source is pinned to the [`ROWS`]/[`COLS`] size so layout and
/// resize decisions match the captured frames, and pre-roll writes into a
/// dedicated capture: its newlines and cursor moves must not land in `term`,
/// which the assertions read, nor on fd 2.
fn terminal_with_term(term: &InMemoryTerm, capacity: usize) -> ProgressTerminal {
    let target = ProgressDrawTarget::term_like(Box::new(term.clone()));
    let dims = Arc::new(TestDimensionSource::new((ROWS, COLS)));
    ProgressTerminal::builder()
        .with_multi_progress(MultiProgress::with_draw_target(target))
        .with_dim_source(dims as Arc<dyn DimensionSource>)
        .capacity(capacity)
        .with_ticker_enabled(false)
        .with_pre_roll_capture(Box::new(InMemoryTerm::new(ROWS, COLS)))
        .build()
}

/// A fresh terminal plus a [`ProgressTerminal`] drawing into it with a full
/// slot reservation — the shape the single-screen tests need.
fn term_terminal() -> (ProgressTerminal, InMemoryTerm) {
    let term = InMemoryTerm::new(ROWS, COLS);
    let terminal = terminal_with_term(&term, ROWS as usize);
    (terminal, term)
}

/// join() commits the screen: contents are identical before and after a
/// later terminal tick, proving the retired bars are never repainted.
#[test]
fn join_commits_and_no_later_tick_repaints() {
    let (t, term) = term_terminal();
    let s = t.screen().build();
    let bar = s.add_bar(3, "fetch");
    bar.advance(3);
    bar.finish_success();
    t.tick();
    assert!(term.contents().contains("fetch"), "precondition: bar rendered");
    s.join();
    let after_join = term.contents();
    t.tick();
    assert_eq!(term.contents(), after_join, "committed screen must be immutable");
}

/// Dropping a screen without an explicit join still commits it.
#[test]
fn drop_without_join_commits() {
    let (t, term) = term_terminal();
    {
        let s = t.screen().build();
        s.add_bar(1, "gone").finish_success();
        t.tick();
        assert!(term.contents().contains("gone"), "precondition: bar rendered");
    }
    let after = term.contents();
    t.tick();
    assert_eq!(term.contents(), after);
}

/// Adding a bar after join is a programming error — the screen is no longer live.
#[test]
#[should_panic(expected = "not the live screen")]
fn add_bar_after_join_panics() {
    let (t, _term) = term_terminal();
    let s = t.screen().build();
    s.join();
    let _ = s.add_bar(1, "late");
}

/// A joined screen's slots are released and its lines kept, so the next screen
/// on the same terminal draws strictly below the committed frame.
///
/// This is the load-bearing half of the pair.  The *release* half cannot
/// distinguish this task's drop-based commit from the erasing
/// `MultiProgress::remove` mechanism — `remove` frees the slot indices just as
/// well, so a second-screen-only assertion passes either way.  What rules out
/// erasure is that `alpha` is still drawn, and drawn *above* `beta`.
///
/// `capacity == ROWS - 1` (9 + 1 = 10) is the exact fit, not a workaround: a
/// screen's frame is one line per reserved slot, so at `capacity == ROWS` the
/// second screen's frame needs every row and the committed frame is scrolled
/// out of the visible grid by *any* mechanism (see
/// [`second_screen_renders_at_full_capacity`]).  One spare row is what makes
/// retention observable at all, which is the measured reason two screens
/// sharing a draw target need `capacity < ROWS`.
#[test]
fn next_screen_draws_below_the_committed_lines() {
    let term = InMemoryTerm::new(ROWS, COLS);
    let terminal = terminal_with_term(&term, ROWS as usize - 1);
    let first = terminal.screen().build();
    first.add_bar(1, "alpha").finish_success();
    first.tick();
    assert!(term.contents().contains("alpha"), "precondition: first bar rendered");
    first.join();
    assert!(term.contents().contains("alpha"), "join erased the committed frame");
    let second = terminal.screen().build();
    second.add_bar(1, "beta").finish_success();
    second.tick();
    let contents = term.contents();
    let lines: Vec<&str> = contents.lines().collect();
    let alpha = lines.iter().position(|l| l.contains("alpha")).expect("alpha committed");
    let beta = lines.iter().position(|l| l.contains("beta")).expect("beta drawn");
    assert!(alpha < beta, "committed screen must stay above the next screen: {lines:?}");
}

/// A joined screen's slots are released, so the next screen on the same
/// terminal renders in full at `capacity == ROWS` — the configuration F18
/// measured as failing.
///
/// Do not lower this capacity to make the test pass: a smaller capacity hides
/// exactly the defect this test exists to catch.  Before the fix the joined
/// screen's `capacity` reserved bars were never released, so the second screen
/// presented `1 + capacity` bars on a `ROWS`-tall terminal and indicatif stopped
/// printing at the terminal height, leaving `beta` invisible.
///
/// This test pins the release half only; [`next_screen_draws_below_the_committed_lines`]
/// pins retention, and it must run at `capacity == ROWS - 1`.  A screen's frame
/// is exactly `capacity` lines (every reserved slot draws one), so at full
/// capacity that frame occupies every row of the terminal and the committed
/// frame is scrolled out of `InMemoryTerm`'s visible grid
/// (`InMemoryTerm::contents` reads the screen, not its scrollback) whichever
/// mechanism commits it.
#[test]
fn second_screen_renders_at_full_capacity() {
    let term = InMemoryTerm::new(ROWS, COLS);
    let terminal = terminal_with_term(&term, ROWS as usize);
    let first = terminal.screen().build();
    first.add_bar(1, "alpha").finish_success();
    first.tick();
    assert!(term.contents().contains("alpha"), "precondition: first bar rendered");
    first.join();
    let second = terminal.screen().build();
    second.add_bar(1, "beta").finish_success();
    second.tick();
    let contents = term.contents();
    assert!(contents.contains("beta"), "second screen never rendered: {contents:?}");
}
