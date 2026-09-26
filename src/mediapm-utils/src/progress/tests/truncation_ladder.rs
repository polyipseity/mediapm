//! Unit tests for the shared fitting ladder (`fit_segments`).
//!
//! These pin the properties the Phase 2 design promises at the
//! pure-function level, with no terminal and no label type involved. A
//! failure here means the fitting policy itself is broken, which the
//! per-label tests would not localise.

use crate::progress::{Segment, fit_segments, front_ellipsis};

/// Protected: phase, status, tally. Elastic: tool. Keep: version, ids.
fn step_segments() -> Vec<Segment> {
    vec![
        Segment::protected("[wf]"),
        Segment::protected("[F]"),
        Segment::protected("1/4"),
        Segment::new_keep("9.9.9"),
        Segment::new_keep("default"),
        Segment::new_keep("s5"),
        Segment::elastic("(mediapm-conductor-builtin-archive)"),
    ]
}

const STEP_FLOOR: usize = 12;

#[test]
fn protected_fields_survive_at_and_above_the_floor() {
    // Two regimes, because a floor set equal to the protected head's own
    // width never binds: at and above it the head fits anyway, so protection
    // is untestable there. TIGHT_FLOOR sits below the head's 12-column width,
    // which is where protection actually decides the outcome.
    const TIGHT_FLOOR: usize = 6;

    for width in TIGHT_FLOOR..STEP_FLOOR {
        let out = fit_segments(&step_segments(), width, TIGHT_FLOOR);
        assert!(out.starts_with("[wf]"), "phase not retained at width {width}: {out:?}");
        // The head cannot fit whole, so it must be kept and clipped rather
        // than dropped in favour of a narrower line that would have fitted.
        assert_eq!(
            out.chars().count(),
            width,
            "protected head dropped instead of clipped at width {width}: {out:?}"
        );
    }

    for width in STEP_FLOOR..60 {
        let out = fit_segments(&step_segments(), width, STEP_FLOOR);
        assert!(out.contains("[wf]"), "phase lost at width {width}: {out:?}");
        assert!(out.contains("[F]"), "status lost at width {width}: {out:?}");
        assert!(out.contains("1/4"), "tally lost at width {width}: {out:?}");
    }
}

#[test]
fn output_length_is_monotonic_in_width() {
    // The spec's property is "a narrower width never yields more text",
    // i.e. length is non-decreasing as width grows. Two upward steps are
    // designed, not incidental: at the floor protection switches on (the
    // protected head returns), and above each dropped segment's threshold
    // that segment re-enters whole before the next one is shortened.
    let mut previous_len = 0usize;
    for width in 0..60 {
        let out = fit_segments(&step_segments(), width, STEP_FLOOR);
        let len = out.chars().count();
        assert!(len >= previous_len, "width {width} shrank the line from {previous_len} to {len}");
        previous_len = len;
    }
}

#[test]
fn result_never_exceeds_the_budget() {
    for width in 0..60 {
        let out = fit_segments(&step_segments(), width, STEP_FLOOR);
        assert!(out.chars().count() <= width, "width {width} overflowed: {out:?}");
    }
}

#[test]
fn elastic_tool_name_shrinks_before_anything_is_dropped() {
    let segments = step_segments();
    let full = fit_segments(&segments, 200, STEP_FLOOR);
    assert!(full.contains("(mediapm-conductor-builtin-archive)"));

    let shrunk = fit_segments(&segments, 40, STEP_FLOOR);
    assert!(shrunk.contains('…'), "elastic segment not front-ellipsised: {shrunk:?}");
    assert!(!shrunk.contains("mediapm-conductor"), "tool not shortened: {shrunk:?}");
    assert!(shrunk.contains("[wf]"), "phase lost while shrinking: {shrunk:?}");
    assert!(shrunk.contains("1/4"), "tally lost while shrinking: {shrunk:?}");
    assert_eq!(shrunk.chars().count(), 40, "should fit exactly: {shrunk:?}");
}

#[test]
fn below_the_floor_protection_lifts() {
    let segments = step_segments();
    let at_floor = fit_segments(&segments, STEP_FLOOR, STEP_FLOOR);
    assert!(at_floor.contains("[wf]"), "phase lost at the floor: {at_floor:?}");

    let below = fit_segments(&segments, 4, STEP_FLOOR);
    assert!(below.chars().count() <= 4, "below-floor output overflowed: {below:?}");
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
    assert_eq!(fit_segments(&[], 40, 12), "");
    assert_eq!(fit_segments(&[Segment::protected("[wf]")], 0, 12), "");
}
