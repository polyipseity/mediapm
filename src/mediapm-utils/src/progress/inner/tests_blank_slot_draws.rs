//! Draw suppression: blanking a slot that is already blank reaches the terminal no more than once.

use super::super::tests_single_writer::counting_term;
use super::*;
use crate::progress::TestDimensionSource;
use crate::progress::TestTimeSource;
use indicatif::TermLike;
use std::sync::Arc;
use std::sync::atomic::AtomicUsize;
use std::sync::atomic::Ordering;

/// A renderer over the shared [`counting_term`] terminal, plus the draw
/// counter it writes to.
fn counting_renderer(
    rows: u16,
    cols: u16,
    capacity: usize,
) -> (ProgressRenderer, Arc<AtomicUsize>) {
    let (term, lines, _flushes) = counting_term(rows, cols);
    let dims = Arc::new(TestDimensionSource::new((rows, cols)));
    let ts = Arc::new(TestTimeSource::new());
    let gate = WriteGate::new_noop_with_term(Arc::new(term) as Arc<dyn TermLike>);
    let renderer = ProgressRenderer::new(capacity, dims, gate, ts as Arc<dyn TimeSource>, None);
    (renderer, lines)
}

/// Blanking a slot that is already blank spends nothing.
///
/// `blank_bar` records the blank state without writing, and
/// `maybe_adjust_for_resize` walks every slot on a height change, so a
/// terminal that grows twice blanks the slots it added on the second pass
/// too. The skip is safe because a rebind clears the marker: the second
/// half of this test blanks a slot that has since been bound.
#[test]
fn blanking_an_already_blank_slot_draws_nothing() {
    let (mut renderer, lines) = counting_renderer(10, 80, 4);
    // Construction used to end with one tick of the last slot; measure from
    // here rather than from zero either way.
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
