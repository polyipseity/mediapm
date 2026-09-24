//! Regression tests for commit-on-join semantics (Spec S2/S3).

use super::super::ProgressTerminal;
use indicatif::{InMemoryTerm, MultiProgress, ProgressDrawTarget};

fn term_terminal() -> (ProgressTerminal, InMemoryTerm) {
    let term = InMemoryTerm::new(10, 80);
    let target = ProgressDrawTarget::term_like(Box::new(term.clone()));
    let mp = MultiProgress::with_draw_target(target);
    let t = ProgressTerminal::builder()
        .with_multi_progress(mp)
        .with_ticker_enabled(false)
        .with_pre_roll_capture(Box::new(term.clone()))
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
