//! Unit tests for the shared fitting ladder (`fit_segments`).
//!
//! These pin the properties the Phase 2 design promises at the
//! pure-function level, with no terminal and no label type involved. A
//! failure here means the fitting policy itself is broken, which the
//! per-label tests would not localise.

use crate::progress::{Segment, Shrink, fit_segments, front_ellipsis};

/// Head: phase, status, tally. Elastic: tool. Keep: version, ids.
fn step_segments() -> Vec<Segment> {
    vec![
        Segment::keep("[wf]"),
        Segment::keep("[F]"),
        Segment::keep("1/4"),
        Segment::keep("9.9.9"),
        Segment::keep("default"),
        Segment::keep("s5"),
        Segment::elastic("(mediapm-conductor-builtin-archive)"),
    ]
}

/// Rendered width of the leading three-segment head, `[wf] [F] 1/4`. Below
/// this the group cannot fit, so dropping from the tail reduces the output to
/// `[wf]` alone; at or above it the whole group fits and the trailing
/// segments are the ones that drop.
const HEAD_WIDTH: usize = 12;

/// A width strictly below [`HEAD_WIDTH`], so the head alone fills the budget.
const BELOW_HEAD: usize = 6;

/// Verifies the leading head survives across the widths where it alone fills
/// the budget and the widths where it fits alongside other segments.
#[test]
fn head_survives_below_and_above_its_own_width() {
    // The head group is exactly 12 columns wide, so the two regimes are
    // geometric: below its own width the tail drops away until only the
    // phase tag remains, and at or above it the whole group fits with room
    // left. Survival follows from the head's leading position in the
    // segment list.
    for width in BELOW_HEAD..HEAD_WIDTH {
        let out = fit_segments(&step_segments(), width);
        assert!(out.contains("[wf]"), "phase lost at width {width}: {out:?}");
        assert!(out.chars().count() <= width, "overflowed at width {width}: {out:?}");
    }

    for width in HEAD_WIDTH..60 {
        let out = fit_segments(&step_segments(), width);
        assert!(out.contains("[wf]"), "phase lost at width {width}: {out:?}");
        assert!(out.contains("[F]"), "status lost at width {width}: {out:?}");
        assert!(out.contains("1/4"), "tally lost at width {width}: {out:?}");
    }
}

#[test]
fn output_length_is_monotonic_in_width() {
    // The spec's property is "a narrower width never yields more text", so
    // the budget is walked from widest to narrowest and each step must be no
    // longer than the one before. Seeded from the real widest output rather
    // than a magic number, with a content assertion on that seed: an
    // implementation returning "" at every width would fail here instead of
    // passing the loop vacuously.
    const WIDEST: usize = 60;
    let segments = step_segments();
    let widest = fit_segments(&segments, WIDEST);
    assert!(
        widest.contains("[wf]") && widest.contains("1/4"),
        "widest probe lost head content: {widest:?}"
    );

    let mut previous_len = widest.chars().count();
    for width in (0..WIDEST).rev() {
        let out = fit_segments(&segments, width);
        let len = out.chars().count();
        assert!(len <= previous_len, "width {width} grew the line to {len} chars");
        previous_len = len;
    }
}

#[test]
fn output_is_never_a_fragment() {
    // The property that separates this ladder from the prefix cut it
    // replaces: no segment is ever cut at a character boundary. Every
    // token in the output is either a whole segment, or an elastic segment
    // shortened from the front. This is what spec rule 3 asks for, and it
    // is the assertion that fails if a shave is reintroduced.
    let segments = step_segments();
    for width in 0..60 {
        let out = fit_segments(&segments, width);
        for token in out.split(' ').filter(|t| !t.is_empty()) {
            let whole = segments.iter().any(|s| {
                if s.shrink != Shrink::FrontEllipsis {
                    return token == s.text;
                }
                // A whole elastic segment, or a front-ellipsised one:
                // the token must be '…' plus a suffix of the segment.
                // `strip_prefix` is used rather than a byte slice
                // because '…' is three bytes wide.
                token == s.text
                    || token.strip_prefix('…').is_some_and(|tail| s.text.ends_with(tail))
            });
            assert!(whole, "fragment {token:?} in output at width {width}: {out:?}");
        }
    }
}

#[test]
fn result_never_exceeds_the_budget() {
    for width in 0..60 {
        let out = fit_segments(&step_segments(), width);
        assert!(out.chars().count() <= width, "width {width} overflowed: {out:?}");
    }
}

#[test]
fn elastic_tool_name_shrinks_before_anything_is_dropped() {
    let segments = step_segments();
    let full = fit_segments(&segments, 200);
    assert!(full.contains("(mediapm-conductor-builtin-archive)"));

    let shrunk = fit_segments(&segments, 40);
    assert!(shrunk.contains('…'), "elastic segment not front-ellipsised: {shrunk:?}");
    assert!(!shrunk.contains("mediapm-conductor"), "tool not shortened: {shrunk:?}");
    assert!(shrunk.contains("[wf]"), "phase lost while shrinking: {shrunk:?}");
    assert!(shrunk.contains("1/4"), "tally lost while shrinking: {shrunk:?}");
    assert_eq!(shrunk.chars().count(), 40, "should fit exactly: {shrunk:?}");
}

#[test]
fn narrow_width_keeps_the_leading_head_whole() {
    let segments = step_segments();
    let at_head_width = fit_segments(&segments, HEAD_WIDTH);
    assert!(at_head_width.contains("[wf]"), "phase lost at the head's width: {at_head_width:?}");

    let below_head_width = fit_segments(&segments, 4);
    // With no shaving, a width too small for the whole head keeps the
    // highest-priority segment whole rather than clipping it. `[wf]` is
    // exactly four columns, so it is the one segment that fits here; the
    // result follows from its width and its leading position.
    assert_eq!(below_head_width, "[wf]");
}

#[test]
fn front_ellipsis_keeps_the_tail() {
    assert_eq!(front_ellipsis("abcdefghij", 6), "…fghij");
    assert_eq!(front_ellipsis("abc", 10), "abc");
    assert_eq!(front_ellipsis("abcdef", 1), "…");
    assert_eq!(front_ellipsis("abcdef", 0), "");
}

#[test]
fn front_ellipsis_preserves_a_path_tail() {
    let short = front_ellipsis("Music/Artist/Album/song.mkv", 15);
    assert!(short.starts_with('…'), "not front-ellipsised: {short:?}");
    assert!(short.ends_with("Album/song.mkv"), "tail not preserved: {short:?}");
}

#[test]
fn empty_input_yields_empty_output() {
    assert_eq!(fit_segments(&[], 40), "");
    assert_eq!(fit_segments(&[Segment::keep("[wf]")], 0), "");
}
