//! Regression tests for commit-on-join semantics (Spec S2/S3).
//!
//! Joining a screen must both *retain* the lines it drew (they become
//! committed output the terminal never touches again) and *release* the slots
//! that screen reserved on the shared draw target, so the next screen of a
//! sync renders in full below the committed frame.
//!
//! Retention is asserted by [`retired_screen_cannot_repaint_the_committed_frame`] and [`join_commits_an_unfinished_bar`]: a committed bar must be unreachable through the handle that used to own it, so mutating that handle cannot repaint the committed line, and a bar still unfinished when the screen is committed must keep its line exactly like a finished one. [`drop_without_join_keeps_an_unfinished_bar`] repeats the second half on the drop path, which is what a `?` early return takes. [`next_screen_draws_below_the_committed_lines`] is deliberately *weaker* — at `capacity == ROWS - 1` a leaked bar and a committed one produce byte-identical `contents()`, so it passes with and without the fix (see its own doc).
//!
//! Release is asserted twice: [`second_screen_renders_at_full_capacity`] draws a second screen that needs every row (a leaked reservation clips it), and [`release_holds_with_the_ticker_running`] repeats that with the production daemon ticker running instead of a manually driven `tick`.
//!
//! Reverting the release fix makes three of these tests fail together — [`second_screen_renders_at_full_capacity`], [`retired_screen_cannot_repaint_the_committed_frame`], and [`release_holds_with_the_ticker_running`] — so none of them is "the" failing assertion. [`join_commits_an_unfinished_bar`] discriminates the other half of the contract: it is red at `bfa740bc`, where release is fixed but the line of an unfinished bar is still cleared on the way out.

use std::sync::Arc;
use std::time::Duration;

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
///
/// `ticker_enabled` starts the daemon ticker thread, which is the production
/// frame driver.  Tests that call `tick()` directly leave it off so frames are
/// deterministic, and [`release_holds_with_the_ticker_running`] turns it on
/// because a manual `tick` cannot exercise the ticker's own strong clone of the
/// renderer.
fn terminal_with(term: &InMemoryTerm, capacity: usize, ticker_enabled: bool) -> ProgressTerminal {
    let target = ProgressDrawTarget::term_like(Box::new(term.clone()));
    let dims = Arc::new(TestDimensionSource::new((ROWS, COLS)));
    ProgressTerminal::builder()
        .with_multi_progress(MultiProgress::with_draw_target(target))
        .with_dim_source(dims as Arc<dyn DimensionSource>)
        .capacity(capacity)
        .with_ticker_enabled(ticker_enabled)
        .with_pre_roll_capture(Box::new(InMemoryTerm::new(ROWS, COLS)))
        .build()
}

/// A [`terminal_with`] with the ticker off — the shape every test that drives
/// frames itself needs.
fn terminal_with_term(term: &InMemoryTerm, capacity: usize) -> ProgressTerminal {
    terminal_with(term, capacity, false)
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

/// A join commits the line of a bar that is still **unfinished**, not only the lines of bars that were finished first.
///
/// The retention contract is total: whatever the screen last drew becomes committed output.  indicatif's default `ProgressFinish::AndClear` breaks that for an unfinished bar — `BarState::drop` runs `finish_using_style`, which sets `Status::DoneHidden` and clears the line on the way out — so the screen sets [`ProgressFinish::AndLeave`](indicatif::ProgressFinish::AndLeave) on every slot bar.  This is the failure path that matters: a `?` early return drops a screen mid-flight, and the partially-filled line is the information a user most wants to keep.
///
/// Measured red at `bfa740bc`, where the release half is already fixed but the finish policy is still indicatif's default: the join leaves `contents()` with no `alpha` at all.  The retired handle is probed afterwards so the test shows a *commit* rather than a re-leaked reservation.
#[test]
fn join_commits_an_unfinished_bar() {
    let term = InMemoryTerm::new(ROWS, COLS);
    let terminal = terminal_with_term(&term, ROWS as usize - 1);
    let first = terminal.screen().build();
    let bar = first.add_bar(60, "alpha");
    bar.advance(1);
    first.tick();
    let running = term.contents();
    assert!(running.contains("alpha"), "precondition: the bar rendered: {running:?}");

    first.join();
    let committed = term.contents();
    assert!(
        committed.contains("alpha"),
        "join dropped the unfinished bar's line instead of committing it: {committed:?}"
    );

    // The probe: a commit puts the line out of the retired handle's reach, so
    // neither the handle nor the retired screen may repaint it.  A re-leaked
    // reservation would move here.
    bar.advance(10);
    first.tick();
    assert_eq!(
        term.contents(),
        committed,
        "a committed frame must never be repainted by the screen that committed it"
    );
}

/// Dropping a screen without an explicit `join` commits an unfinished bar too.
///
/// `ManagedScreen::drop` (in `progress/inner/terminal.rs`) routes through `join`, so this is the same contract on the path an early `?` return takes: the line a screen drew before it was dropped survives the drop.
#[test]
fn drop_without_join_keeps_an_unfinished_bar() {
    let (t, term) = term_terminal();
    {
        let s = t.screen().build();
        let bar = s.add_bar(60, "gone");
        bar.advance(1);
        t.tick();
        assert!(term.contents().contains("gone"), "precondition: bar rendered");
    }
    let after = term.contents();
    assert!(after.contains("gone"), "the drop cleared the unfinished bar's line: {after:?}");
    t.tick();
    assert_eq!(term.contents(), after, "a committed frame must never be repainted");
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
/// **This test is green with the fix reverted** (measured at `da358265`, the
/// pre-fix revision), so it does not separate a commit from a leaked
/// reservation.  At `capacity == ROWS - 1` the un-released bar stays in the
/// shared [`MultiProgress`] and is redrawn as the head of the second screen's
/// frame with byte-identical content: one leaked line plus the second screen's
/// nine is still [`ROWS`], so nothing is clipped and both mechanisms produce
/// the same grid.
///
/// What it does establish is that the commit *erases* nothing: `alpha` survives both the join and the second screen's frame, which rules out a commit built on `MultiProgress::clear`.  It is also the test that catches an erasing commit built on `MultiProgress::remove` alone — the `join erased the committed frame` assertion still passes there (the removal only marks the member, and the clearing draw comes later), and the erasure surfaces at the `expect("alpha committed")` below once the second screen draws.  The assertions that fail with the release fix reverted are [`retired_screen_cannot_repaint_the_committed_frame`], [`second_screen_renders_at_full_capacity`], and [`release_holds_with_the_ticker_running`].
///
/// `capacity == ROWS - 1` is the exact fit, not a workaround: a screen's frame
/// is one line per reserved slot, so at `capacity == ROWS` the second screen's
/// frame needs every row and the committed frame is scrolled out of the visible
/// grid by *any* mechanism (see
/// [`second_screen_renders_at_full_capacity`]).  One spare row is what keeps
/// both screens' lines inside [`InMemoryTerm`]'s visible grid, which is the
/// measured reason two screens sharing a draw target need `capacity < ROWS`.
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
/// This test pins the release half only; retention is pinned by [`retired_screen_cannot_repaint_the_committed_frame`] and [`join_commits_an_unfinished_bar`], and it must run at `capacity == ROWS - 1`.  A screen's frame is exactly `capacity` lines (every reserved slot draws one), so at full capacity that frame occupies every row of the terminal and the committed frame is scrolled out of `InMemoryTerm`'s visible grid (`InMemoryTerm::contents` reads the screen, not its scrollback) whichever mechanism commits it.
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

/// A committed frame is frozen: the bar that drew it is unreachable, so
/// neither the handle that owned it nor the retired screen can change a
/// character of it.
///
/// This is one of the assertions that fail with the release fix reverted.  At `da358265` the screen handle kept the renderer — and with it every [`ProgressBar`] the screen reserved — alive behind a strong `Arc`, so `ManagedScreen::tick` still ran a frame: the surviving bar was re-rendered from its still-live `SharedState`, and the `advance` below moved its rendered `count/total` from `1/60` to `11/60` on the already-committed line.  After the fix the handle holds a `Weak`, `join` drops the last strong reference, and `tick` is a no-op.
///
/// The retained handle is the probe, not the subject: `ProgressBarHandle` is the public way to mutate a bar, and once the screen is committed the bar must no longer be reachable through it.  The `join erased the committed frame` assertion is a precondition here, not the erasure detector: an erasing commit built on `MultiProgress::remove` passes it (see [`next_screen_draws_below_the_committed_lines`], which catches that shape).
///
/// `alpha` is finished before the join on purpose: this test probes a *finished* bar, whose reaping indicatif never routes through the finish policy at all.  The unfinished case is the separate assertion of [`join_commits_an_unfinished_bar`], which is what pins the screen's `ProgressFinish::AndLeave` policy.
#[test]
fn retired_screen_cannot_repaint_the_committed_frame() {
    let term = InMemoryTerm::new(ROWS, COLS);
    let terminal = terminal_with_term(&term, ROWS as usize - 1);
    let first = terminal.screen().build();
    let bar = first.add_bar(60, "alpha");
    bar.advance(1);
    bar.finish_success();
    first.tick();
    let running = term.contents();
    assert!(running.contains("1/60"), "precondition: the bar rendered its position: {running:?}");

    first.join();
    let committed = term.contents();
    assert!(committed.contains("alpha"), "join erased the committed frame: {committed:?}");
    assert!(
        committed.contains("1/60"),
        "the committed frame must keep what it was committed with: {committed:?}"
    );

    let second = terminal.screen().build();
    second.add_bar(1, "beta").finish_success();
    second.tick();
    let after_second = term.contents();
    assert!(after_second.contains("beta"), "second screen never rendered: {after_second:?}");

    // Both of these reached the committed line before the fix: the handle kept
    // the bar alive, and `tick` drove one more frame on the retired screen.
    bar.advance(10);
    first.tick();
    assert_eq!(
        term.contents(),
        after_second,
        "a committed frame must never be repainted by the screen that committed it"
    );
}

/// Release holds on the production path: the second screen renders in full
/// while the daemon ticker — not a manual `tick` — drives the frames.
///
/// Every other test here disables the ticker, so nothing else exercises the one
/// path that takes a *transient* strong clone of the renderer: the ticker
/// upgrades its `Weak` for the duration of a tick, so a screen joined inside
/// that window can keep its reservation alive past `join`.  This test lets the
/// ticker run at least one frame of the first screen, joins, and then builds and
/// draws the second screen at `capacity == ROWS`, where a surviving reservation
/// is clipped away.
///
/// Measured: red at `da358265` (the leaked reservation pushes the second
/// screen's frame past the terminal height and its bar never appears), green at
/// the fixed revision.  What it does *not* prove: it cannot force the join to
/// land inside the ticker's upgrade window, so it is evidence that the window
/// does not normally bite, not a proof that it cannot.  `ManagedScreen::join`
/// documents what bounds that window.
#[test]
fn release_holds_with_the_ticker_running() {
    let term = InMemoryTerm::new(ROWS, COLS);
    let terminal = terminal_with(&term, ROWS as usize, true);
    let first = terminal.screen().build();
    first.add_bar(1, "alpha").finish_success();
    // The ticker's interval is 50 ms; sleep past it so the first screen has been
    // driven by the thread rather than by an explicit tick.
    std::thread::sleep(Duration::from_millis(60));
    let contents = term.contents();
    assert!(
        contents.contains("alpha"),
        "precondition: the ticker drew the first screen: {contents:?}"
    );
    first.join();
    let second = terminal.screen().build();
    second.add_bar(1, "beta").finish_success();
    // Three more ticker intervals for the thread to notice the new screen, draw
    // it, and observe the retired renderer's `Weak` fail.
    std::thread::sleep(Duration::from_millis(150));
    let contents = term.contents();
    assert!(
        contents.contains("beta"),
        "second screen never rendered with the ticker running: {contents:?}"
    );
}
