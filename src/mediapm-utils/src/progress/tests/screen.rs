//! Regression tests for commit-on-join semantics (Spec S2/S3).

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

fn term_terminal() -> (ProgressTerminal, InMemoryTerm) {
    let term = InMemoryTerm::new(ROWS, COLS);
    let target = ProgressDrawTarget::term_like(Box::new(term.clone()));
    let dims = Arc::new(TestDimensionSource::new((ROWS, COLS)));
    let mp = MultiProgress::with_draw_target(target);
    let t = ProgressTerminal::builder()
        .with_multi_progress(mp)
        .with_dim_source(dims as Arc<dyn DimensionSource>)
        .capacity(ROWS as usize)
        .with_ticker_enabled(false)
        // A dedicated capture keeps pre-roll's newlines and cursor moves out of
        // `term`, which the assertions below read, and out of fd 2, where
        // `with_multi_progress` would otherwise send them.
        .with_pre_roll_capture(Box::new(InMemoryTerm::new(ROWS, COLS)))
        .build();
    (t, term)
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
