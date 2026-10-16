//! Draw suppression: blanking a slot that is already blank reaches the terminal no more than once.

use super::super::tests_single_writer::counting_term;
use super::*;
use crate::progress::TestDimensionSource;
use crate::progress::TestTimeSource;
use indicatif::MultiProgress;
use indicatif::ProgressDrawTarget;
use std::sync::Arc;
use std::sync::atomic::AtomicUsize;

/// A renderer over the shared [`counting_term`] terminal, plus the draw
/// counter it writes to.
fn counting_renderer(
    rows: u16,
    cols: u16,
    capacity: usize,
) -> (ProgressRenderer, Arc<AtomicUsize>) {
    let (term, lines, _flushes) = counting_term(rows, cols);
    let mp = MultiProgress::with_draw_target(ProgressDrawTarget::term_like(Box::new(term)));
    let dims = Arc::new(TestDimensionSource::new((rows, cols)));
    let ts = Arc::new(TestTimeSource::new());
    let renderer = ProgressRenderer::from_mp(
        mp,
        capacity,
        dims,
        WriteGate::new_noop(),
        ts as Arc<dyn TimeSource>,
        None,
    );
    (renderer, lines)
}

/// Blanking a slot that is already blank spends nothing.
///
/// All three pushes `blank_bar` makes are real `update_estimate_and_draw`
/// calls, and `maybe_adjust_for_resize` walks every slot on a height
/// change, so a terminal that grows twice blanks the slots it added on the
/// second pass too. The skip is safe because a rebind clears the marker:
/// the second half of this test blanks a slot that has since been bound and
/// does draw.
#[test]
fn blanking_an_already_blank_slot_draws_nothing() {
    let (mut renderer, lines) = counting_renderer(10, 80, 4);
    // Construction ends with one tick of the last slot, so measure from
    // here rather than from zero.
    let baseline = lines.load(Ordering::Relaxed);

    // Slot 0 is blank from construction.
    renderer.blank_bar(0);
    renderer.blank_bar(0);
    assert_eq!(
        lines.load(Ordering::Relaxed),
        baseline,
        "a slot that is already blank must not be drawn again"
    );

    let bar =
        Arc::new(SharedState::with_time_source(10, "tool", Arc::clone(&renderer.time_source)));
    renderer.attach(&bar);
    // attach binds the bottom slot, the only slot that is no longer blank.
    let bottom = renderer.slots.len() - 1;
    let after_attach = lines.load(Ordering::Relaxed);
    renderer.blank_bar(bottom);
    assert!(
        lines.load(Ordering::Relaxed) > after_attach,
        "a slot that has been bound and released is no longer blank, so blanking it draws"
    );
}
