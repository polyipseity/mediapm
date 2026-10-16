//! Animation states: an `Active` row animates only when its source opted in.
//!
//! Spec AC2/R3: only in-flight rows animate; a row nobody declares live parks
//! on the static `·` — the safe default when a state is forgotten.

use super::ProgressBarHandle;
use super::ProgressRenderer;
use super::SharedState;
use super::WriteGate;
use crate::progress::{TestDimensionSource, TestTimeSource, TimeSource};
use indicatif::{InMemoryTerm, MultiProgress, ProgressDrawTarget, TermLike};
use std::sync::Arc;

/// A renderer whose single writer draws into `term`, plus one attached child
/// bar in its initial state (`Active`, not opted in).
///
/// Same construction the blank-slot tests use, except the gate carries the
/// term (`new_noop_with_term`) so `run_frame`'s plan actually reaches the
/// grid a test reads back.
fn renderer_with_active_child(term: &InMemoryTerm) -> (ProgressRenderer, ProgressBarHandle) {
    let mp = MultiProgress::with_draw_target(ProgressDrawTarget::term_like(Box::new(term.clone())));
    let dims = Arc::new(TestDimensionSource::new((term.height(), term.width())));
    let ts = Arc::new(TestTimeSource::new());
    let gate = WriteGate::new_noop_with_term(Arc::new(term.clone()) as Arc<dyn TermLike>);
    let mut renderer =
        ProgressRenderer::from_mp(mp, 4, dims, gate, ts.clone() as Arc<dyn TimeSource>, None);
    let handle =
        ProgressBarHandle { state: Arc::new(SharedState::with_time_source(10, "tool", ts)) };
    renderer.attach(&handle.state);
    (renderer, handle)
}

/// AC2: an `Active` row nobody opted in renders a parked dot and never
/// moves, across as many frames as you care to draw.
#[test]
fn active_without_opt_in_is_parked() {
    let term = InMemoryTerm::new(10, 80);
    let (mut renderer, bar) = renderer_with_active_child(&term);
    bar.set_position(0); // still `Active`
    renderer.run_frame();
    let first = term.contents();
    renderer.run_frame();
    let second = term.contents();
    assert!(
        first.lines().any(|line| line.starts_with('·')),
        "parked glyph missing from the active-but-not-opted-in row: {first:?}"
    );
    assert_eq!(
        first, second,
        "a parked row must not change between frames: {first:?} vs {second:?}"
    );
}

/// AC2: an opted-in `Active` row cycles the spinner column between frames.
#[test]
fn active_with_opt_in_spins() {
    let term = InMemoryTerm::new(10, 80);
    let (mut renderer, bar) = renderer_with_active_child(&term);
    bar.set_animating(true);
    bar.set_position(1);
    renderer.run_frame();
    let first = term.contents();
    renderer.run_frame();
    let second = term.contents();
    assert_ne!(
        first, second,
        "an opted-in active row must advance its spinner between frames: {first:?}"
    );
}
