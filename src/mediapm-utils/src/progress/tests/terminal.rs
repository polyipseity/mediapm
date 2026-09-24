//! Tests for [`ProgressTerminal`] and [`ProgressScreen`] (Task 1).

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
