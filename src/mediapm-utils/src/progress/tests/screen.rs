//! Regression tests for commit-on-join semantics (Spec S2/S3).
//!
//! Joining a screen must both *retain* the lines it drew (they become
//! committed output the terminal never touches again) and *release* the slots
//! that screen reserved on the shared draw target, so the next screen of a
//! sync renders in full below the committed frame.
//!
//! Retention is asserted by [`retired_screen_cannot_repaint_the_committed_frame`] and [`join_commits_an_unfinished_bar`]: a committed bar must be unreachable through the handle that used to own it, so mutating that handle cannot repaint the committed line, and a bar still unfinished when the screen is committed must keep its line exactly like a finished one. [`gated_terminal_retains_an_unfinished_bar`] repeats that second half on the write-gated configuration production draws through, where the gate — not the finish policy — is what retains the line. [`drop_without_join_keeps_an_unfinished_bar`] repeats the second half on the drop path, which is what a `?` early return takes. [`next_screen_draws_below_the_committed_lines`] is deliberately *weaker* — at `capacity == ROWS - 1` a leaked bar and a committed one produce byte-identical `contents()`, so it passes with and without the fix (see its own doc).
//!
//! Where the committed frame *lands* is asserted by [`gated_second_screen_keeps_the_committed_frame`], [`gated_third_screen_keeps_every_committed_frame`], and [`gated_second_screen_keeps_the_committed_frame_across_capacities`]: on the gated configuration the frame outlives the next screen, on the ungated one the intermediate draws already walk the cursor down it and `next_screen_draws_below_the_committed_lines` covers the same ground.
//!
//! Release is asserted twice: [`second_screen_renders_at_full_capacity`] draws a second screen that needs every row (a leaked reservation clips it), and [`release_holds_with_the_ticker_running`] repeats that with the production daemon ticker running instead of a manually driven `tick`.
//!
//! Reverting the release fix makes three of these tests fail together — [`second_screen_renders_at_full_capacity`], [`retired_screen_cannot_repaint_the_committed_frame`], and [`release_holds_with_the_ticker_running`] — so none of them is "the" failing assertion. [`join_commits_an_unfinished_bar`] discriminates the other half of the contract, but only in this module's ungated configuration (`with_multi_progress`, a no-op gate): it is red at `bfa740bc`, where release is fixed but the line of an unfinished bar is still cleared on the way out. On the write-gated configuration production uses, that line is retained with and without the finish policy, which [`gated_terminal_retains_an_unfinished_bar`] pins.

use std::sync::Arc;
use std::time::Duration;

use super::super::{DimensionSource, ProgressScreenApi, ProgressTerminal, TestDimensionSource};
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
///
/// The draw target here is **ungated**: `with_multi_progress` installs a no-op
/// gate, so the drop-time finishing draw reaches `term`.  Production wraps its
/// term in a write gate instead — see [`terminal_with_gate`].
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

/// A [`terminal_with_term`] on the **production draw path**: `with_term_like` wraps
/// `term` in a `BufferedTerm` write gate — the configuration `ProgressTerminal`
/// builds outside tests — instead of the no-op gate `with_multi_progress` installs.
///
/// Use this for tests that pin what production actually does with the gate in
/// place; keep [`terminal_with_term`] for tests that need the drop-time draw to
/// reach `term` so a finish-policy defect stays observable.
fn terminal_with_gate(term: &InMemoryTerm, capacity: usize) -> ProgressTerminal {
    let dims = Arc::new(TestDimensionSource::new((ROWS, COLS)));
    ProgressTerminal::builder()
        .with_term_like(Box::new(term.clone()))
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

/// A join commits the line of a bar that is still **unfinished**, not only the lines of bars that were finished first.
///
/// The retention contract is total: whatever the screen last drew becomes committed output.  indicatif's default `ProgressFinish::AndClear` breaks that for an unfinished bar — `BarState::drop` runs `finish_using_style`, which sets `Status::DoneHidden` and clears the line on the way out — so the screen sets [`ProgressFinish::AndLeave`](indicatif::ProgressFinish::AndLeave) on every slot bar.  This is the failure path that matters: a `?` early return drops a screen mid-flight, and the partially-filled line is the information a user most wants to keep.
///
/// Measured red at `bfa740bc` in this test's configuration: [`terminal_with_term`] builds the terminal with `with_multi_progress`, whose gate is a no-op, so the drop-time `AndClear` draw reaches `term`.  At `bfa740bc` release is already fixed but the finish policy is still indicatif's default, and the join leaves `contents()` with no `alpha` at all.  The same assertions pass with *and* without the policy on the write-gated configuration production uses, where the gate suppresses that draw; that half of the contract is pinned by [`gated_terminal_retains_an_unfinished_bar`].
///
/// The retired handle is probed at the end for reachability, not for release: the probe cannot detect a leaked reservation, because the post-join `tick()` is a no-op once `finalize` has set the finalized flag.  The release direction is covered by [`second_screen_renders_at_full_capacity`].
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
    // neither the handle nor the retired screen may repaint it.  It is a
    // reachability probe only — it cannot detect a leaked reservation.  The
    // `tick()` below is a no-op once `finalize` set the finalized flag
    // (`ProgressRenderer::run_frame` returns early), so a reservation that
    // survived `join` leaves `contents()` unchanged too: deleting only
    // `state.renderer = None;` from `ProgressScreen::join` keeps this test and
    // `drop_without_join_keeps_an_unfinished_bar` passing.  Release is covered
    // by `second_screen_renders_at_full_capacity`.
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
/// `ProgressScreen::drop` (in `progress/inner/terminal.rs`) routes through `join`, so this is the same contract on the path an early `?` return takes: the line a screen drew before it was dropped survives the drop.
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

/// The production draw configuration retains an unfinished bar's line: this is what production actually does when a screen is committed mid-flight, measured on the write-gated target instead of on the no-op gate the other tests here use.
///
/// [`terminal_with_gate`] goes through `with_term_like`, the builder path whose `BufferedTerm` suppresses every write outside an open window.  `BarState::drop` finishes the unfinished bar and draws it after `finalize`'s window has closed, so that draw is suppressed and the frame `finalize` drew — both the bar's label and its `1/60` position — is the last one the terminal sees.
///
/// This is a **characterization/regression pin, not a red/green test**: it passes at both `bfa740bc` and `960fdc60`, because production retention is gate-provided and `with_slot_finish_policy`'s `AndLeave` policy is inert there.  Its value is that the configuration production depends on is measured rather than inferred, and that a change which routes the drop-time draw through an open window — or drops the gate from this path — fails a test instead of silently invalidating the reasoning in `ProgressScreen::join`.
///
/// Not vacuous, and measured: running these assertions on the **ungated** configuration ([`terminal_with_term`]) fails as soon as the finish policy is also removed, which is the pre-`960fdc60` behavior of the ungated tests (see the `task-2` report for the command and the failing output).  With the policy in place the ungated configuration passes too, which is exactly why this pin is needed to tell the two mechanisms apart.
#[test]
fn gated_terminal_retains_an_unfinished_bar() {
    let term = InMemoryTerm::new(ROWS, COLS);
    let terminal = terminal_with_gate(&term, ROWS as usize - 1);
    let screen = terminal.screen().build();
    let bar = screen.add_bar(60, "alpha");
    bar.advance(1);
    terminal.tick();
    let running = term.contents();
    assert!(running.contains("alpha"), "precondition: the bar rendered: {running:?}");
    assert!(running.contains("1/60"), "precondition: the bar rendered its position: {running:?}");

    screen.join();
    let committed = term.contents();
    assert!(
        committed.contains("alpha"),
        "the gated terminal dropped the unfinished bar's line: {committed:?}"
    );
    assert!(
        committed.contains("1/60"),
        "the gated terminal must keep the last frame it drew: {committed:?}"
    );
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
/// What it does establish is that the commit *erases* nothing: `alpha` survives both the join and the second screen's frame, which rules out a commit built on `MultiProgress::clear`.  It is also the test that catches an erasing commit built on `MultiProgress::remove` alone — the `join erased the committed frame` assertion still passes there (removal alone erases nothing: `MultiState::remove_idx` replaces the member with `MultiStateMember::default()` and drops it from the ordering — `indicatif-0.17.11/src/multi.rs:449-463` — so the clearing draw comes later), and the erasure surfaces at the `expect("alpha committed")` below once the second screen draws.  The assertions that fail with the release fix reverted are [`retired_screen_cannot_repaint_the_committed_frame`], [`second_screen_renders_at_full_capacity`], and [`release_holds_with_the_ticker_running`].
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
/// This is one of the assertions that fail with the release fix reverted.  At `da358265` the screen handle kept the renderer — and with it every [`ProgressBar`] the screen reserved — alive behind a strong `Arc`, so `ProgressScreen::tick` still ran a frame: the surviving bar was re-rendered from its still-live `SharedState`, and the `advance` below moved its rendered `count/total` from `1/60` to `11/60` on the already-committed line.  After the fix the handle holds a `Weak`, `join` drops the last strong reference, and `tick` is a no-op.
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
/// does not normally bite, not a proof that it cannot.  `ProgressScreen::join`
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

/// The surviving screen type is reachable only through a terminal and it
/// implements the dependency-injection trait the conductor consumes.
///
/// The coercion below is the compile-time proof of the collapse: exactly one
/// `ProgressScreen` exists, the terminal's screen builder produces it, and
/// `ProgressScreenApi` is implemented for it, so `mediapm-conductor` can drive
/// it through the trait without an `indicatif` dependency of its own.
#[test]
fn screen_implements_the_screen_api() {
    let term = InMemoryTerm::new(ROWS, COLS);
    let terminal = terminal_with_term(&term, ROWS as usize);
    let screen: Arc<dyn ProgressScreenApi + Send + Sync> = Arc::new(terminal.screen().build());
    let bar = screen.add_bar(2, "alpha");
    bar.advance(1);
    assert_eq!(bar.snapshot().position, 1, "the trait hands back a real handle for a live screen");
    screen.join();
}

/// A draw released after the gate discarded a frame must not unwind rows the terminal never received.
///
/// This is the write gate's success-reporting contract, measured on the production draw path ([`terminal_with_gate`]).  `indicatif` writes the height it drew back into the draw target at the very end of `DrawState::draw_to_term` (`*bar_count = real_height + shift`, `indicatif-0.17.11/src/draw_target.rs:572`), so a discarded draw that reported success left the target believing the terminal held a frame it never saw.  The first draw released afterwards consumed those phantom rows — `move_cursor_up(n - 1)` followed by one `clear_line` per row — which lands on whatever is really on them, here the previous screen's committed frame.
///
/// The assertion is on the operations the gate released, not on the grid: `InMemoryTerm` keeps no scrollback and has no public setter, so from `contents()` alone a row erased here and a row pushed off the top are indistinguishable.  Only the ops the gate lets through can tell them apart — a phantom unwind emits clears no frame of this screen drew, and the first released draw must therefore contain none.
///
/// `ROWS - 1` capacity is the two-screen fit used by the rest of this module: a screen's frame is one line per reserved slot.
#[test]
fn gated_second_screen_does_not_unwind_the_discarded_screens_rows() {
    let term = InMemoryTerm::new(ROWS, COLS);
    let terminal = terminal_with_gate(&term, ROWS as usize - 1);
    let first = terminal.screen().build();
    first.add_bar(1, "alpha").finish_success();
    terminal.tick();
    first.join();
    // Everything up to the commit is the first screen's own business; the ops
    // under test are the ones the second screen's first release emits.
    let _ = term.moves_since_last_check();

    let second = terminal.screen().build();
    second.add_bar(1, "beta").finish_success();
    terminal.tick();

    let ops = term.moves_since_last_check();
    // A draw is one op block ending at its `Flush`; the first one belongs to the
    // window `add_bar` opens, which is the earliest release after the commit.
    let first_draw = ops.split("Flush").next().unwrap_or_default();
    assert!(
        !first_draw.contains("Up("),
        "the first released draw moved the cursor up over rows it never drew: {ops}"
    );
    assert!(
        !first_draw.contains("Clear"),
        "the first released draw cleared rows it never drew: {ops}"
    );
}

/// A committed screen's frame stays on the terminal once the next screen renders.
///
/// This is the visible half of the commit contract ([`ProgressScreenApi::join`]): the frame is handed over permanently, so the next screen's band must start *below* it. `next_screen_draws_below_the_committed_lines` cannot see the failure because it uses the ungated configuration, where every intermediate draw really writes and walks the cursor down the frame. Production draws through the write gate ([`terminal_with_gate`]), where the discarded draws leave the cursor resting on the committed frame's last row, the next frame's band is written starting there, and the following in-place redraw clears the committed row.
///
/// Position is asserted, not mere presence: `alpha` still appearing somewhere could be a redraw in the wrong place, which is why the test pins its row relative to `beta`'s.
#[test]
fn gated_second_screen_keeps_the_committed_frame() {
    let term = InMemoryTerm::new(ROWS, COLS);
    let terminal = terminal_with_gate(&term, 4);
    let first = terminal.screen().build();
    first.add_bar(1, "alpha").finish_success();
    terminal.tick();
    first.join();

    let second = terminal.screen().build();
    second.add_bar(1, "beta").finish_success();
    terminal.tick();

    assert_eq!(
        (row_of(&term, "alpha"), row_of(&term, "beta")),
        (Some(0), Some(4)),
        "the committed frame must keep its rows above the next screen's:\n{}",
        term.contents()
    );
}

/// The advance keeps working for every later screen: committed frames survive until they are the oldest.
///
/// The erasure this guards is total and saturates at exactly one frame: with two screens the defect removes the first frame, and with three it removes both earlier ones, so a two-screen assertion alone cannot distinguish "the advance works" from "the advance works once". Every earlier label is therefore re-checked after every commit, not just the newest one. A commit also collapses a frame whose slots are mostly blank down to its bound bars, so the ordering assertion is "strictly below the previous frame", not a fixed offset.
#[test]
fn gated_three_screens_keep_every_committed_frame() {
    let term = InMemoryTerm::new(ROWS, COLS);
    let terminal = terminal_with_gate(&term, 4);
    let mut committed: Vec<&str> = Vec::new();
    for label in ["alpha", "beta", "gamma"] {
        let screen = terminal.screen().build();
        screen.add_bar(1, label).finish_success();
        terminal.tick();
        screen.join();
        committed.push(label);
        let rows: Vec<Option<usize>> = committed.iter().map(|l| row_of(&term, l)).collect();
        assert!(
            rows.iter().all(Option::is_some),
            "every committed frame must survive, got {rows:?} for {committed:?}:\n{}",
            term.contents()
        );
        let present: Vec<usize> = rows.into_iter().flatten().collect();
        assert!(
            present.windows(2).all(|pair| pair[0] < pair[1]),
            "each screen must sit below the one it followed, got {present:?}:\n{}",
            term.contents()
        );
    }
}

/// The advance survives the capacities the erasure first showed up at.
///
/// A sweep rather than one geometry: the defect is invisible at `capacity == 1` (the frame is a single line and the next band's first line is written below it) and shows up from `capacity == 2` through the full-height case. Pinning only one capacity would leave the others latent.
#[test]
fn gated_second_screen_keeps_the_committed_frame_across_capacities() {
    // `capacity == ROWS - 1` is deliberately absent: two frames that size plus
    // the separation row need more rows than the terminal has, and `InMemoryTerm`
    // keeps no scrollback, so the oldest frame leaves the grid for physical
    // reasons rather than because of this contract.
    for capacity in [1usize, 2, 3, 4, 8] {
        let term = InMemoryTerm::new(ROWS, COLS);
        let terminal = terminal_with_gate(&term, capacity);
        let first = terminal.screen().build();
        first.add_bar(1, "alpha").finish_success();
        terminal.tick();
        first.join();

        let second = terminal.screen().build();
        second.add_bar(1, "beta").finish_success();
        terminal.tick();

        assert_eq!(
            (row_of(&term, "alpha"), row_of(&term, "beta")),
            (Some(0), Some(capacity)),
            "capacity {capacity}: the committed frame must stay above the next screen's:\n{}",
            term.contents()
        );
    }
}

/// Row of the first line containing `needle`, or `None` when no line does.
fn row_of(term: &InMemoryTerm, needle: &str) -> Option<usize> {
    term.contents().lines().position(|line| line.contains(needle))
}
