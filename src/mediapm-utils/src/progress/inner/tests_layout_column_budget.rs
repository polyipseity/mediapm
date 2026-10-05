//! How `recompute_layout` spends terminal columns: the cross-bar prefix width, and the ceiling that clamps it.

use super::*;
use crate::progress::TestDimensionSource;
use crate::progress::TestTimeSource;
use crate::progress::inner::components::MAX_PREFIX_WIDTH;
use crate::progress::inner::components::MAX_SUFFIX_WIDTH;
use indicatif::MultiProgress;
use indicatif::ProgressDrawTarget;
use std::sync::Arc;

#[test]
fn recompute_layout_uniform_widths() {
    // Two bars with different prefix widths must converge to a single
    // uniform prefix_w equal to the max measured width (clamped to the
    // [MIN_PREFIX_WIDTH, MAX_PREFIX_WIDTH] band), and suffix_w must stay
    // within its own band.  This guards the dynamic cross-bar alignment.
    let term = indicatif::InMemoryTerm::new(10, 80);
    let target = ProgressDrawTarget::term_like(Box::new(term.clone()));
    let mp = MultiProgress::with_draw_target(target);
    let dims = Arc::new(TestDimensionSource::new((10, 80)));
    let ts = Arc::new(TestTimeSource::new());

    let mut renderer = ProgressRenderer::from_mp(
        mp,
        4,
        dims,
        WriteGate::new_noop(),
        ts as Arc<dyn TimeSource>,
        None,
    );

    // Short label ("a") and a 20-char label ("aaaaaaaaaaaaaaaaaaaa").
    let short =
        Arc::new(SharedState::with_time_source(100, "a", Arc::clone(&renderer.time_source)));
    let long = Arc::new(SharedState::with_time_source(
        100,
        "aaaaaaaaaaaaaaaaaaaa",
        Arc::clone(&renderer.time_source),
    ));
    renderer.attach(&short);
    renderer.attach(&long);

    renderer.recompute_layout();

    // The long label is 20 visible columns, but `snap.prefix` is rendered
    // WITH its leading `\x1b[0m` status escape (4 bytes) and indicatif
    // counts those escape bytes as visible characters in the
    // `{prefix:N.N}` field. So the uniform `prefix_w` must reserve
    // 20 + 4 = 24 columns to fit it without truncation.
    assert_eq!(
        renderer.prefix_w.get(),
        24,
        "prefix_w must equal max measured width + status ANSI overhead"
    );
    assert!(
        (MIN_PREFIX_WIDTH..=MAX_PREFIX_WIDTH).contains(&renderer.prefix_w.get()),
        "prefix_w must stay within [MIN_PREFIX_WIDTH, MAX_PREFIX_WIDTH]"
    );
    assert!(
        (MIN_SUFFIX_WIDTH..=MAX_SUFFIX_WIDTH).contains(&renderer.suffix_w.get()),
        "suffix_w must stay within [MIN_SUFFIX_WIDTH, MAX_SUFFIX_WIDTH]"
    );
}

/// Settle the layout for one bar whose built-in label is `label`, drawn on
/// a terminal `cols` wide.
///
/// Returns the settled prefix width and whether the frame kept its fill.
/// Both are read out before the renderer drops, so the caller asserts on
/// the two cells the budget writes rather than on a frame.
fn settle_one_built_in_bar(label: &str, cols: u16) -> (usize, bool) {
    let term = indicatif::InMemoryTerm::new(10, cols);
    let target = ProgressDrawTarget::term_like(Box::new(term));
    let mp = MultiProgress::with_draw_target(target);
    let dims = Arc::new(TestDimensionSource::new((10, cols)));
    let ts = Arc::new(TestTimeSource::new());
    let mut renderer = ProgressRenderer::from_mp(
        mp,
        4,
        dims,
        WriteGate::new_noop(),
        ts as Arc<dyn TimeSource>,
        None,
    );
    let bar =
        Arc::new(SharedState::with_time_source(100, label, Arc::clone(&renderer.time_source)));
    renderer.attach(&bar);
    renderer.recompute_layout();
    (renderer.prefix_w.get(), renderer.draw_fill.get())
}

/// The prefix ceiling caps a built-in label that outgrows it.
///
/// A bar carrying a client label is capped at the source, because the
/// client is handed the ceiling less the reset reserve and returns no more
/// than it was given. A bar without one has no cap at the source: its width
/// is measured from the rendered built-in string, which is as wide as the
/// caller made it. The ceiling in `settle` is the only cap on that path, and
/// the tool-sync screen is built from it, where a `{tool} {version} [{phase}]`
/// label crosses forty columns routinely.
///
/// The settled width is the cell the clamp writes, so the first two
/// assertions read it. The third reads the fill, which is what the clamped
/// columns are for: without a bar beside the label, a wider slot buys the
/// label nothing.
#[test]
fn the_prefix_ceiling_caps_a_built_in_label_that_outgrows_it() {
    // Inside the ceiling the label sets the slot at its own measured width,
    // so the assertions below are about a ceiling rather than about a slot
    // that is always the ceiling.
    let short = "a".repeat(MAX_PREFIX_WIDTH - 10);
    let (measured, _) = settle_one_built_in_bar(&short, 120);
    assert_eq!(
        measured,
        short.chars().count() + compute_ansi_overhead(TrackStatus::Active, false),
        "a label this far inside the ceiling settles at its measured width, \
         so the ceiling is a cap and not a fixed slot"
    );

    // Past the ceiling the label stops growing the slot: a label that just
    // crosses it and one well over it take the same columns off the line.
    for width in [MAX_PREFIX_WIDTH, MAX_PREFIX_WIDTH + 30] {
        let (settled, _) = settle_one_built_in_bar(&"a".repeat(width), 120);
        assert_eq!(
            settled, MAX_PREFIX_WIDTH,
            "a {width}-column label must not claim more than the ceiling off the line"
        );
    }

    // The columns the ceiling gave back reach the fill, so a label this
    // long still leaves a bar beside it. Without the clamp the same label
    // takes the columns the fill needs and the frame drops the bar.
    let (_, drew_fill) = settle_one_built_in_bar(&"a".repeat(MAX_PREFIX_WIDTH + 30), 80);
    assert!(
        drew_fill,
        "at eighty columns the clamp is what leaves the long label a bar to sit next to"
    );
}
