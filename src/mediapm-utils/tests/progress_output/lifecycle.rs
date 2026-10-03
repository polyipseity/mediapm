//! Screen lifecycle: commit-on-join, finalize, slot recycling, and consumers.
//!
//! A screen owns one live renderer. `join()` commits the frame — the lines the
//! screen drew stay on the terminal and the slots it reserved are released so
//! the next screen renders in full below them. `ProgressScreen::join_and_clear`
//! is an alias of `join`; neither of them clears a bound bar, they only collapse
//! the blank reserved slots. The tests here pin those semantics together with
//! the two consumer patterns that drive them (sequential tool sync and
//! per-entry materialization).

use std::sync::{Arc, Mutex};
use std::time::Duration;

use indicatif::TermLike;
use mediapm_utils::progress::{BarStyle, ProgressScreen, TestTimeSource};

use super::common::{
    H, W, line_with, mk_with_capacity, mk_with_capacity_and_ts, mk_with_capacity_gated,
    mk_with_pre_roll_term,
};

/// An unfinished bar's line is committed by `join`, and the committed frame can
/// never be repainted: the retired screen's renderer is gone, so a later tick is
/// a no-op.
#[test]
fn active_bar_survives_join_and_its_line_is_frozen() {
    let (terminal, term) = mk_with_capacity(5, 80, 3);
    let (screen, _overall) = terminal.screen().with_overall("overall", 3).build();

    let _bar = screen.add_bar(5, "alpha");
    screen.tick();
    screen.join();

    let committed = term.contents();
    assert!(committed.contains("alpha"), "join dropped the unfinished bar: {committed:?}");
    screen.tick();
    assert_eq!(term.contents(), committed, "a committed frame must never be repainted");
    assert_eq!(
        committed,
        concat!(
            "⠏       alpha ███████████████████████████████████████████████████  0/5 0s 0/d\n",
            "⠏     overall ███████████████████████████████████████████████████  0/3 0s 0/d"
        ),
        "active_bar_survives_join_and_its_line_is_frozen"
    );
}

/// `join_and_clear` collapses the blank reserved slots and keeps every bound
/// bar; it does not clear bars despite the name.
#[test]
fn join_and_clear_collapses_blank_slots_only() {
    let (terminal, term) = mk_with_capacity(6, 80, 5);
    let (screen, _overall) = terminal.screen().with_overall("overall", 3).build();

    let child = screen.add_bar(5, "fetch");
    child.finish_success();
    screen.tick();
    screen.join_and_clear();
    assert_eq!(
        &term.contents(),
        concat!(
            "⠏       fetch ███████████████████████████████████████████████████  0/5 0s\n",
            "⠏     overall ███████████████████████████████████████████████████  0/3 0s 0/d"
        ),
        "join_and_clear_collapses_blank_slots_only"
    );
}

/// An overall bar survives the finalize that `join_and_clear` runs, and the
/// finished child above it does too.
#[test]
fn finalized_overall_bar_persists() {
    let (terminal, term) = mk_with_capacity(5, 80, 3);
    let (screen, _overall) = terminal.screen().with_overall("overall", 3).build();

    let child = screen.add_bar(5, "fetch");
    child.advance(5);
    screen.tick();
    child.finish_success();
    screen.tick();
    screen.join_and_clear();
    assert_eq!(
        &term.contents(),
        concat!(
            "⠏       fetch ███████████████████████████████████████████████████  5/5 0s\n",
            "⠏     overall ███████████████████████████████████████████████████  0/3 0s 0/d"
        ),
        "finalized_overall_bar_persists"
    );
}

/// `join` keeps every bound bar, not only the finished ones, and leaves no
/// blank filler line behind.
#[test]
fn join_keeps_every_bound_bar() {
    let (terminal, term) = mk_with_capacity(6, 80, 5);
    let (screen, _overall) = terminal.screen().with_overall("overall", 5).build();

    let child = screen.add_bar(3, "fetch");
    child.advance(3);
    screen.tick();
    screen.join();
    assert_eq!(
        &term.contents(),
        concat!(
            "⠏       fetch ███████████████████████████████████████████████████  3/3 0s 0/d\n",
            "⠏     overall ███████████████████████████████████████████████████  0/5 0s 0/d"
        ),
        "join_keeps_every_bound_bar"
    );
}

/// Finalizing a screen that has no children leaves only the overall bar.
#[test]
fn finalized_screen_without_children_draws_only_the_overall() {
    let (terminal, term) = mk_with_capacity(4, 80, 3);
    let (screen, _overall) = terminal.screen().with_overall("overall", 1).build();

    screen.tick();
    screen.join_and_clear();
    assert_eq!(
        &term.contents(),
        concat!(
            "⠏     overall ███████████████████████████████████████████████████",
            "  0/1 0s 0/d"
        ),
        "finalized_screen_without_children_draws_only_the_overall"
    );
}

/// The disabled screen the `--no-progress` path builds is inert: every handle it
/// hands out carries no total however it is driven, so a run with progress
/// disabled cannot allocate bars or draw.
///
/// The handle's `total` is the discriminator: a live screen hands back a handle
/// carrying the requested total, so a disabled screen that began allocating bars
/// fails here. The structured prefix/suffix setters are the only mutators that
/// check the disabled flag, so the position a disabled handle reports is not part
/// of this contract and is not asserted.
///
/// `ProgressScreen` rather than `ProgressTerminal` is the type under test: the
/// `no_progress` flag in `src/mediapm/src/service.rs` builds
/// `ProgressTerminal::disabled()`, which hands out screens of this same
/// type, while that terminal's disabled constructor is asserted by
/// `progress::tests::terminal::disabled_terminal_is_inert`.
#[test]
fn disabled_screen_handles_report_no_total() {
    let screen = ProgressScreen::disabled();
    let child = screen.add_bar(5, "child");
    assert_eq!(child.total(), 0, "a disabled screen hands out no-op handles");

    let styled = screen.add_bar_with_style(5, "worker", BarStyle::WorkerSpinner);
    assert_eq!(styled.total(), 0, "the styled variant is a no-op handle too");

    child.advance(3);
    child.set_position(4);
    child.finish_error();
    assert_eq!(child.total(), 0, "no mutator turns a no-op handle into a tracked bar");

    screen.tick();
    screen.join();
    screen.join_and_clear();
}

/// A finished child keeps its line instead of being cleared on finish.
#[test]
fn finished_child_stays_visible() {
    let (terminal, term) = mk_with_capacity(6, 80, 5);
    let (screen, _overall) = terminal.screen().with_overall("overall", 3).build();

    let child = screen.add_bar(5, "fetch");
    screen.tick();
    child.finish_success();
    screen.tick();
    assert_eq!(
        &term.contents(),
        concat!(
            "\n",
            "\n",
            "\n",
            "\n",
            "⠏       fetch ███████████████████████████████████████████████████  0/5 0s\n",
            "⠸     overall ░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░  0/3 0s 0/d"
        ),
        "finished_child_stays_visible"
    );
}

/// Every child of a screen that finished successfully persists in the frame.
#[test]
fn all_finished_bars_persist() {
    let (terminal, term) = mk_with_capacity(6, 80, 5);
    let (screen, _overall) = terminal.screen().with_overall("overall", 2).build();

    let c1 = screen.add_bar(3, "alpha");
    let c2 = screen.add_bar(5, "beta");
    c1.advance(3);
    c2.advance(5);
    screen.tick();
    c1.finish_success();
    c2.finish_success();
    screen.tick();
    assert_eq!(
        &term.contents(),
        concat!(
            "\n",
            "\n",
            "\n",
            "⠏       alpha ███████████████████████████████████████████████████  3/3 0s\n",
            "⠏        beta ███████████████████████████████████████████████████  5/5 0s\n",
            "⠸     overall ░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░  0/2 0s 0/d"
        ),
        "all_finished_bars_persist"
    );
}

/// A second tick after a slot finished leaves that slot's line byte-identical:
/// a finished bar's spinner stops advancing, unlike an active bar's.
#[test]
fn finishing_a_slot_twice_is_idempotent() {
    let (terminal, term) = mk_with_capacity(5, 80, 3);
    let (screen, _overall) = terminal.screen().with_overall("overall", 3).build();

    let child = screen.add_bar(5, "fetch");
    child.advance(5);
    screen.tick();
    child.finish_success();
    screen.tick();
    let first = term.contents();
    assert!(first.contains("5/5"), "the finished bar keeps its final position: {first:?}");

    screen.tick();
    assert_eq!(
        line_with(&term.contents(), "fetch"),
        line_with(&first, "fetch"),
        "a second tick on a finished slot must not change its line"
    );
    assert_eq!(
        first,
        concat!(
            "\n",
            "\n",
            "⠏       fetch ███████████████████████████████████████████████████  5/5 0s\n",
            "⠸     overall ░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░  0/3 0s 0/d"
        ),
        "finishing_a_slot_twice_is_idempotent"
    );
}

/// Clearing a child blanks its line, and the next child takes the bottom slot.
///
/// The cleared bar's tracked state is not forgotten, though: once the next
/// `add_bar` re-shifts the band, the cleared bar is drawn again as a zeroed
/// `0/5 0s` bar above the new child (see the `reused` frame). That is a
/// surprising result for a "clear" operation, and it is pinned here as-is rather
/// than encoded in the test name as a release.
#[test]
fn finish_and_clear_zeroes_the_line_and_reuses_the_slot() {
    let (terminal, term) = mk_with_capacity(6, 80, 5);
    let (screen, _overall) = terminal.screen().with_overall("overall", 3).build();

    let _c1 = screen.add_bar(5, "keep");
    let c2 = screen.add_bar(5, "clear");
    screen.tick();
    assert_eq!(
        &term.contents(),
        concat!(
            "\n",
            "\n",
            "\n",
            "⠹        keep ░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░  0/5 0s 0/d\n",
            "⠸       clear ░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░  0/5 0s 0/d\n",
            "⠹     overall ░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░  0/3 0s 0/d",
        ),
        "finish_and_clear_zeroes_the_line_and_reuses_the_slot/cleared"
    );

    c2.finish_and_clear();
    screen.tick();
    assert_eq!(
        &term.contents(),
        concat!(
            "\n",
            "\n",
            "\n",
            "⠸        keep ░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░  0/5 0s 0/d\n",
            "\n",
            "⠸     overall ░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░  0/3 0s 0/d"
        ),
        "finish_and_clear_zeroes_the_line_and_reuses_the_slot/before_reuse"
    );

    let _c3 = screen.add_bar(5, "new");
    screen.tick();
    assert_eq!(
        &term.contents(),
        concat!(
            "\n",
            "\n",
            "⠹        keep ░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░  0/5 0s 0/d\n",
            "⠸       clear ░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░  0/5 0s\n",
            "⠼         new ░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░  0/5 0s 0/d\n",
            "⠼     overall ░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░  0/3 0s 0/d",
        ),
        "finish_and_clear_zeroes_the_line_and_reuses_the_slot/reused"
    );
}

/// Clearing one child removes its own line and leaves the others untouched.
///
/// The screen reserves five slots, so the frame after the clear (two bars plus
/// the overall) is shorter than the grid. At `capacity == rows` a clear followed
/// by a redraw leaves [`indicatif::InMemoryTerm::contents`] empty, which would
/// make the assertions below compare `""` with `""`.
#[test]
fn finish_and_clear_keeps_other_bars() {
    let (terminal, term) = mk_with_capacity(6, 80, 5);
    let (screen, _overall) = terminal.screen().with_overall("overall", 5).build();

    let keep = screen.add_bar(3, "keep");
    let clear = screen.add_bar(3, "clear");
    keep.advance(3);
    clear.advance(3);
    screen.tick();
    assert!(term.contents().contains("clear"), "precondition: both children are drawn");

    clear.finish_and_clear();
    let _new = screen.add_bar(3, "new");
    screen.tick();

    let contents = term.contents();
    assert!(!contents.contains("clear"), "the cleared bar must be gone: {contents:?}");
    assert!(contents.contains("keep"), "the other child must survive: {contents:?}");
    assert!(contents.contains("overall"), "the overall bar must survive: {contents:?}");
    assert_eq!(
        &contents,
        concat!(
            "\n",
            "\n",
            "⠹        keep ███████████████████████████████████████████████████  3/3 0s 0/d\n",
            "\n",
            "⠦         new ░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░  0/3 0s 0/d\n",
            "⠸     overall ░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░  0/5 0s 0/d",
        ),
        "finish_and_clear_keeps_other_bars"
    );
}

/// A failed bar keeps its progress and stays on screen with the error marker.
#[test]
fn error_bar_keeps_progress_and_visibility() {
    let (terminal, term) = mk_with_capacity(6, 80, 5);
    let screen = terminal.screen().build();

    let child = screen.add_bar(5, "worker");
    child.advance(2);
    screen.tick();
    child.finish_error();
    let contents = term.contents();
    assert!(contents.contains("worker"), "the failed bar must stay visible: {contents:?}");
    assert_eq!(
        &contents,
        concat!(
            "\n",
            "\n",
            "\n",
            "\n",
            "⠼     worker ████████████████████░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░  2/5 0s 0/d"
        ),
        "error_bar_keeps_progress_and_visibility"
    );
}

/// When every slot is full, clearing a finished bar frees it for the next
/// child, which takes the bottom child slot without disturbing the others.
#[test]
fn recycle_finished_slot_after_full() {
    let ts = Arc::new(TestTimeSource::new());
    let (terminal, term) = mk_with_capacity_and_ts(6, 80, 5, &ts);
    let (screen, _overall) = terminal.screen().with_overall("overall", 5).build();

    let children: Vec<_> = (0..4).map(|i| screen.add_bar(2, &format!("task{i}"))).collect();
    ts.advance(Duration::from_secs(1));
    screen.tick();
    assert_eq!(
        &term.contents(),
        concat!(
            "\n",
            "⠹       task0 ░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░  0/2 1s 0/d\n",
            "⠹       task1 ░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░  0/2 1s 0/d\n",
            "⠹       task2 ░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░  0/2 1s 0/d\n",
            "⠸       task3 ░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░  0/2 1s 0/d\n",
            "⠹     overall ░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░  0/5 1s 0/d",
        ),
        "recycle_finished_slot_after_full/full"
    );

    children[0].finish_and_clear();
    ts.advance(Duration::from_secs(1));
    screen.tick();
    assert_eq!(
        &term.contents(),
        concat!(
            "\n",
            "\n",
            "⠸       task1 ░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░  0/2 2s 0/d\n",
            "⠸       task2 ░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░  0/2 2s 0/d\n",
            "⠼       task3 ░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░  0/2 2s 0/d\n",
            "⠸     overall ░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░  0/5 2s 0/d",
        ),
        "recycle_finished_slot_after_full/cleared"
    );

    let _c4 = screen.add_bar(2, "task4");
    ts.advance(Duration::from_secs(1));
    screen.tick();
    assert_eq!(
        &term.contents(),
        concat!(
            "⠙       task0 ░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░  0/2 1s\n",
            "⠸       task1 ░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░  0/2 3s 0/d\n",
            "⠼       task2 ░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░  0/2 3s 0/d\n",
            "⠼       task3 ░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░  0/2 3s 0/d\n",
            "⠴       task4 ░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░  0/2 1s 0/d\n",
            "⠼     overall ░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░  0/5 3s 0/d",
        ),
        "recycle_finished_slot_after_full/recycled"
    );
}

/// With no free slot, the oldest finished bar is recycled so the newest bars
/// (the ones a caller still cares about) keep their lines.
#[test]
fn recycle_oldest_finished_slot() {
    let ts = Arc::new(TestTimeSource::new());
    let (terminal, term) = mk_with_capacity_and_ts(10, 80, 6, &ts);
    let (screen, _overall) = terminal.screen().with_overall("overall", 5).build();

    let a1 = screen.add_bar(1, "a [resolve]");
    let a2 = screen.add_bar(1, "a [fetch]");
    let a3 = screen.add_bar(1, "a [process]");
    let b1 = screen.add_bar(1, "b [resolve]");
    let b2 = screen.add_bar(1, "b [fetch]");
    a1.finish_success();
    a2.finish_success();
    a3.finish_success();
    b1.finish_success();
    b2.finish_success();
    ts.advance(Duration::from_secs(1));
    screen.tick();
    assert_eq!(
        &term.contents(),
        concat!(
            "\n",
            "⠏     a [resolve] ███████████████████████████████████████████████  0/1 0s\n",
            "⠏       a [fetch] ███████████████████████████████████████████████  0/1 0s\n",
            "⠏     a [process] ███████████████████████████████████████████████  0/1 0s\n",
            "⠏     b [resolve] ███████████████████████████████████████████████  0/1 0s\n",
            "⠏       b [fetch] ███████████████████████████████████████████████  0/1 0s\n",
            "⠹         overall ░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░  0/5 1s 0/d"
        ),
        "recycle_oldest_finished_slot/before"
    );

    let b3 = screen.add_bar(1, "b [process]");
    b3.finish_success();
    screen.tick();
    assert_eq!(
        &term.contents(),
        concat!(
            "⠏     a [resolve] ███████████████████████████████████████████████  0/1 0s\n",
            "⠏       a [fetch] ███████████████████████████████████████████████  0/1 0s\n",
            "⠏     a [process] ███████████████████████████████████████████████  0/1 0s\n",
            "⠏     b [resolve] ███████████████████████████████████████████████  0/1 0s\n",
            "⠏       b [fetch] ███████████████████████████████████████████████  0/1 0s\n",
            "⠏     b [process] ███████████████████████████████████████████████  0/1 0s\n",
            "⠸         overall ░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░  0/5 1s 0/d"
        ),
        "recycle_oldest_finished_slot/after"
    );
}

/// The whole add → progress → finish → finalize lifecycle is exact at every
/// stage, so a missing bar, a ghost blank line, or a swapped slot is caught.
#[test]
fn finalize_full_lifecycle_exact() {
    let (terminal, term) = mk_with_capacity(8, 80, 5);
    let (screen, _overall) = terminal.screen().with_overall("overall", 10).build();

    let c1 = screen.add_bar(5, "alpha");
    let c2 = screen.add_bar(3, "beta");
    c1.advance(5);
    c1.finish_success();
    c2.advance(3);
    c2.finish_success();
    screen.tick();
    assert_eq!(
        &term.contents(),
        concat!(
            "\n",
            "\n",
            "\n",
            "⠏       alpha █████████████████████████████████████████████████  5/5 0s\n",
            "⠏        beta █████████████████████████████████████████████████  3/3 0s\n",
            "⠹     overall ░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░  0/10 0s 0/d"
        ),
        "finalize_full_lifecycle_exact/before"
    );

    screen.join_and_clear();
    assert_eq!(
        &term.contents(),
        concat!(
            "⠏       alpha █████████████████████████████████████████████████  5/5 0s\n",
            "⠏        beta █████████████████████████████████████████████████  3/3 0s\n",
            "⠏     overall █████████████████████████████████████████████████  0/10 0s 0/d"
        ),
        "finalize_full_lifecycle_exact/after"
    );
}

/// Finalizing a screen whose slots were all occupied leaves no blank lines:
/// padding newlines would push bar content into scrollback.
#[test]
fn finalize_leaves_no_blank_lines() {
    let (terminal, term) = mk_with_capacity(6, 80, 4);
    let (screen, _overall) = terminal.screen().with_overall("overall", 3).build();

    let c1 = screen.add_bar(5, "alpha");
    let c2 = screen.add_bar(5, "beta");
    let c3 = screen.add_bar(5, "gamma");
    for child in [&c1, &c2, &c3] {
        child.advance(5);
        child.finish_success();
    }
    screen.tick();
    assert_eq!(
        &term.contents(),
        concat!(
            "\n",
            "⠏       alpha ███████████████████████████████████████████████████  5/5 0s\n",
            "⠏        beta ███████████████████████████████████████████████████  5/5 0s\n",
            "⠏       gamma ███████████████████████████████████████████████████  5/5 0s\n",
            "⠹     overall ░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░  0/3 0s 0/d"
        ),
        "finalize_leaves_no_blank_lines/before"
    );

    screen.join_and_clear();
    assert_eq!(
        &term.contents(),
        concat!(
            "⠏       alpha ███████████████████████████████████████████████████  5/5 0s\n",
            "⠏        beta ███████████████████████████████████████████████████  5/5 0s\n",
            "⠏       gamma ███████████████████████████████████████████████████  5/5 0s\n",
            "⠏     overall ███████████████████████████████████████████████████  0/3 0s 0/d"
        ),
        "finalize_leaves_no_blank_lines/after"
    );
}

/// The overall bar keeps the bottom slot while its children complete, and the
/// completed children keep the slots above it.
#[test]
fn overall_stays_bottom_with_finished_children() {
    let (terminal, term) = mk_with_capacity(5, 80, 4);
    let (screen, _overall) = terminal.screen().with_overall("overall", 10).build();

    for i in 0..3 {
        let child = screen.add_bar(2, &format!("child{i}"));
        child.advance(2);
        child.finish_success();
    }
    screen.tick();
    let contents = term.contents();
    assert_eq!(contents.lines().last().map(|line| line.contains("overall")), Some(true));
    assert_eq!(
        contents,
        concat!(
            "\n",
            "⠏      child0 █████████████████████████████████████████████████  2/2 0s\n",
            "⠏      child1 █████████████████████████████████████████████████  2/2 0s\n",
            "⠏      child2 █████████████████████████████████████████████████  2/2 0s\n",
            "⠹     overall ░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░  0/10 0s 0/d"
        ),
        "overall_stays_bottom_with_finished_children"
    );
}

/// A second screen draws below the first screen's committed line.
///
/// `capacity == H - 1` is required, not a convenience: a screen's frame is one
/// line per reserved slot, so at full capacity the second screen's own frame
/// fills the terminal and the committed row leaves [`indicatif::InMemoryTerm`]'s
/// visible grid. The one spare row is what makes the ordering observable. The
/// release half (a second screen still rendering in full at `capacity == H`) is
/// covered by `progress::tests::screen::second_screen_renders_at_full_capacity`.
#[test]
fn second_screen_draws_below_the_first() {
    let (terminal, term) = mk_with_capacity(H, W, H as usize - 1);
    let first = terminal.screen().build();
    first.add_bar(1, "one").finish_success();
    first.tick();
    assert!(term.contents().contains("one"), "precondition: the first bar rendered");
    first.join();
    assert!(term.contents().contains("one"), "join must commit the first frame");

    let second = terminal.screen().build();
    second.add_bar(1, "two").finish_success();
    second.tick();

    let contents = term.contents();
    let lines: Vec<&str> = contents.lines().collect();
    let one = lines.iter().position(|l| l.contains("one")).expect("the committed line:");
    let two = lines.iter().position(|l| l.contains("two")).expect("the second screen's line:");
    assert!(one < two, "a committed screen must stay above the next one: {lines:?}");
}

/// Pre-roll scrolls exactly one terminal height of blank lines, and only once
/// per terminal: the second screen of one sync must not scroll again.
///
/// The scroll is recorded through this module's own [`TermLike`], not through an
/// [`indicatif::InMemoryTerm`]: an `InMemoryTerm` drops trailing blank rows from
/// `contents()`, and pre-roll writes blank lines only, so such a capture records
/// nothing and every assertion on it would be vacuous.
#[test]
fn pre_roll_writes_one_terminal_height_once() {
    let recorder = PreRollRecorder::default();
    let (terminal, term) = mk_with_pre_roll_term(4, 80, 4, Box::new(recorder.clone()));
    let screen = terminal.screen().build();
    screen.add_bar(1, "one").finish_success();
    screen.tick();
    assert!(term.contents().contains("one"), "precondition: the bar reached the grid");

    assert_eq!(
        recorder.writes(),
        vec![String::new(); 4],
        "pre-roll writes exactly one blank line per terminal row"
    );
    assert_eq!(
        recorder.moves(),
        vec![4, -4],
        "pre-roll moves the cursor down past the existing content and back up"
    );

    screen.join();
    let second = terminal.screen().build();
    second.add_bar(1, "two").finish_success();
    second.tick();
    assert_eq!(recorder.moves(), vec![4, -4], "pre-roll must fire exactly once per terminal");
    assert_eq!(recorder.writes().len(), 4, "the second screen must not scroll again");
}

/// Records the pre-roll scroll: one entry per written line and one signed entry
/// per cursor move (positive = down, negative = up).
#[derive(Clone, Debug, Default)]
struct PreRollRecorder {
    /// Lines passed to `write_line`, in order.
    writes: Arc<Mutex<Vec<String>>>,
    /// Cursor moves, in order, signed by direction.
    moves: Arc<Mutex<Vec<i32>>>,
}

impl PreRollRecorder {
    /// The lines written so far.
    fn writes(&self) -> Vec<String> {
        self.writes.lock().expect("recorder lock").clone()
    }

    /// The cursor moves so far.
    fn moves(&self) -> Vec<i32> {
        self.moves.lock().expect("recorder lock").clone()
    }
}

impl TermLike for PreRollRecorder {
    fn width(&self) -> u16 {
        80
    }

    fn height(&self) -> u16 {
        24
    }

    /// Records an upward cursor move of `n` rows as a negative entry.
    fn move_cursor_up(&self, n: usize) -> std::io::Result<()> {
        let rows = i32::try_from(n).map_err(std::io::Error::other)?;
        self.moves.lock().expect("recorder lock").push(-rows);
        Ok(())
    }

    /// Records a downward cursor move of `n` rows as a positive entry.
    fn move_cursor_down(&self, n: usize) -> std::io::Result<()> {
        let rows = i32::try_from(n).map_err(std::io::Error::other)?;
        self.moves.lock().expect("recorder lock").push(rows);
        Ok(())
    }

    fn move_cursor_right(&self, _n: usize) -> std::io::Result<()> {
        Ok(())
    }

    fn move_cursor_left(&self, _n: usize) -> std::io::Result<()> {
        Ok(())
    }

    fn write_line(&self, s: &str) -> std::io::Result<()> {
        self.writes.lock().expect("recorder lock").push(s.to_string());
        Ok(())
    }

    fn write_str(&self, _s: &str) -> std::io::Result<()> {
        Ok(())
    }

    fn clear_line(&self) -> std::io::Result<()> {
        Ok(())
    }

    fn flush(&self) -> std::io::Result<()> {
        Ok(())
    }
}

/// Driving the overall handle returned by the screen builder moves the rendered overall bar: the handle and the renderer's bottom slot share one state.
///
/// Regression guard. `TerminalScreenBuilder<HasOverall>::build()` used to return a handle built from a fresh state while the renderer's overall slot kept the state `add_overall` had created inside `build_screen`, so `advance`, `set_position`, `set_total` and every `finish_*` on the handle were invisible and the overall bar stayed at zero for the whole run.
#[test]
fn overall_handle_progress_reaches_the_renderer() {
    let (terminal, term) = mk_with_capacity(4, 80, 3);
    let (screen, overall) = terminal.screen().with_overall("overall", 4).build();
    let child = screen.add_bar(2, "child");
    child.advance(2);
    screen.tick();

    let before = line_with(&term.contents(), "overall").to_string();
    assert!(before.contains("0/4"), "the overall is still undriven: {before}");

    overall.set_position(4);
    overall.finish_success();
    screen.tick();

    let contents = term.contents();
    let overall_line = line_with(&contents, "overall").to_string();
    assert!(
        overall_line.contains("4/4"),
        "the overall reports the driven position: {overall_line}"
    );
    assert!(!overall_line.contains("0/d"), "and is finished, not still active: {overall_line}");
    assert!(contents.contains("2/2"), "while the child's own progress still renders: {contents:?}");
    assert_eq!(
        &contents,
        concat!(
            "\n",
            "\n",
            "⠴       child ██████████████████████████████████████████████████████  2/2 0s 0/d\n",
            "⠏     overall ██████████████████████████████████████████████████████  4/4 0s"
        ),
        "overall_handle_progress_reaches_the_renderer"
    );
}

/// A two-screen sequence on the **gated** draw path keeps the first screen's
/// committed frame below the second screen's, exactly as it was committed.
///
/// Every other exact-frame assertion in this suite runs on the ungated path
/// ([`mk_with_capacity`]'s no-op gate), where the intermediate draws really
/// write and walk the cursor down the committed frame. Production draws through
/// the gate, so what the frame's survival rests on is decided there — see the
/// harness module docs for what the gate changes.
///
/// This is a **gated-only discriminator** at its assertion point, not merely
/// coverage of the production configuration. Running the identical sequence on
/// the ungated harness ([`mk_with_capacity`]) and asserting the grid below
/// fails: the two diverge after the second `tick`, where the ungated path lets
/// `finish_success`'s done-style draw reach the terminal (`⠏     beta
/// █████████████████████  0/1 0s`) while the gate discards it and the frame
/// keeps its active style (`⠹     beta ░░░░░░░░░░░░░░  0/1 0s 0/d`). What the
/// geometry fixes — two screens at capacity 2, so both rows stay inside
/// `InMemoryTerm`'s visible grid — is the exact rows asserted, not the
/// discrimination.
///
/// An earlier revision of this doc claimed the ungated path captured the same
/// grid here and called the test non-discriminating. That erratum is corrected
/// rather than deleted because it is the dangerous direction: an understated
/// test invites a future deletion as redundant, and this is the only
/// integration-level exact-grid assertion on the production draw path.
#[test]
fn gated_two_screens_keep_the_committed_frame() {
    let (terminal, term) = mk_with_capacity_gated(H, W, 2);
    let first = terminal.screen().build();
    first.add_bar(1, "alpha").finish_success();
    terminal.tick();
    first.join();

    let second = terminal.screen().build();
    second.add_bar(1, "beta").finish_success();
    terminal.tick();

    assert_eq!(
        term.contents(),
        concat!(
            "⠏     alpha ████████████████████  0/1 0s\n",
            "\n",
            "⠹     beta ░░░░░░░░░░░░░░  0/1 0s 0/d"
        ),
        "gated_two_screens_keep_the_committed_frame"
    );
}
