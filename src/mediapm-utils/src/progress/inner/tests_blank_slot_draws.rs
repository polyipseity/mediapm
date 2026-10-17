//! Blank slots: blanking is a cache marker — it costs no terminal write, and it pre-empts the retention a released slot would otherwise draw.

use super::super::tests_single_writer::counting_term;
use super::*;
use crate::progress::TestDimensionSource;
use crate::progress::TestTimeSource;
use indicatif::TermLike;
use std::sync::Arc;
use std::sync::atomic::AtomicUsize;
use std::sync::atomic::Ordering;

/// A renderer over the shared [`counting_term`] terminal, plus its flush
/// counter.  One `flush` is one completed repaint, so "flushes unchanged" is
/// the exact statement "the writer spent no terminal output".
fn counting_renderer(
    rows: u16,
    cols: u16,
    capacity: usize,
) -> (ProgressRenderer, Arc<AtomicUsize>) {
    let (term, _lines, flushes) = counting_term(rows, cols);
    let dims = Arc::new(TestDimensionSource::new((rows, cols)));
    let ts = Arc::new(TestTimeSource::new());
    let gate = WriteGate::new_noop_with_term(Arc::new(term) as Arc<dyn TermLike>);
    let renderer = ProgressRenderer::new(capacity, dims, gate, ts as Arc<dyn TimeSource>, None);
    (renderer, flushes)
}

/// Blank markers are free: blanking every slot — as a height change does when
/// it grows the grid — repaints nothing, and a screen that has never shown
/// content owes no frame at all.
///
/// The height-change walk re-blanks each slot it adds, so the marker must
/// cost no output per call; and because an all-blank plan is the no-bars
/// contract, running frames over a never-drawn screen must not scroll a real
/// terminal for no pixels (pinned for the writer by
/// `tests_single_writer::force_writes_an_unchanged_frame`'s neighbours and
/// for the screen by `screen::gated_screen_without_bars_commits_nothing`).
#[test]
fn blanking_an_already_blank_slot_costs_no_write() {
    let (mut renderer, flushes) = counting_renderer(10, 80, 4);
    // Every reserved slot is blank from construction; measure from here
    // rather than from zero either way.
    let baseline = flushes.load(Ordering::Relaxed);
    for i in 0..renderer.slots.len() {
        renderer.blank_bar(i);
    }
    assert_eq!(
        flushes.load(Ordering::Relaxed),
        baseline,
        "blanking already-blank slots must not repaint"
    );

    renderer.run_frame();
    renderer.run_frame();
    assert_eq!(
        flushes.load(Ordering::Relaxed),
        baseline,
        "all-blank plans are never painted: a never-drawn screen owes no frame"
    );
}

/// Blanking a bound slot pre-empts its retention: once the slot's source
/// releases, the row draws blank instead of retaining its last content.
///
/// `blank_bar` records the marker in the slot cache without writing — the
/// writer repaints only when the frame it builds differs from the last one
/// it wrote.  The released slot is then unbound, and the unbound branch of
/// `build_plan` reads the marker: blanked draws a blank row, unblanked
/// retains (the retention contract, pinned by the `progress/tests::screen`
/// release tests).  The frame write here is the discriminator between the
/// two outcomes: a retained row re-settles to the same strings and costs no
/// repaint, while the blanked row changes the frame and must repaint.
#[test]
fn blanking_a_bound_slot_preempts_its_retention_on_release() {
    let (mut renderer, flushes) = counting_renderer(10, 80, 4);
    let bar =
        Arc::new(SharedState::with_time_source(10, "tool", Arc::clone(&renderer.time_source)));
    renderer.attach(&bar);
    // attach binds the bottom slot and settles without writing; the first
    // frame draws it.
    let bottom = renderer.slots.len() - 1;
    let after_attach = flushes.load(Ordering::Relaxed);
    renderer.run_frame();
    let after_draw = flushes.load(Ordering::Relaxed);
    assert!(after_draw > after_attach, "the bound row must reach the terminal");

    renderer.blank_bar(bottom);
    assert_eq!(
        flushes.load(Ordering::Relaxed),
        after_draw,
        "blanking a bound slot is a cache marker: no write at the call"
    );

    // Release the slot's source the way `finalize` unbinds a row, then run
    // the next frame: the marker must win over retention.
    renderer.slots[bottom].source.replace(None);
    renderer.run_frame();
    assert!(
        flushes.load(Ordering::Relaxed) > after_draw,
        "the released slot repaints because its row went blank instead of retaining"
    );
    let frame = renderer.build_plan().render();
    assert!(
        frame[bottom].trim().is_empty(),
        "the released slot must draw the blank row — full-width spaces, no label, no spinner: {:?}",
        frame[bottom]
    );
}
