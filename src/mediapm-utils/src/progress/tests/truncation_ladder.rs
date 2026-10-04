//! Unit tests for the shared fitting ladder (`fit_segments`).
//!
//! These pin the properties the fitting policy promises at the
//! pure-function level, with no terminal and no label type involved. A
//! failure here means the fitting policy itself is broken, which the
//! per-label tests would not localise.

use crate::progress::{Brackets, Segment, fit_segments, front_tail};

/// Head: phase, status, tally. Elastic: tool. Keep: version, ids.
fn step_segments() -> Vec<Segment> {
    vec![
        Segment::keep("wf").brackets(Brackets::Square),
        Segment::keep("F").brackets(Brackets::Square),
        Segment::keep("1/4"),
        Segment::keep("9.9.9"),
        Segment::keep("default"),
        Segment::keep("s5"),
        Segment::elastic("mediapm-conductor-builtin-archive").brackets(Brackets::Round),
    ]
}

/// A segment set whose elastic pieces are paths and a hyphenated name, the
/// shapes the three progress screens actually clip.
fn path_segments() -> Vec<Segment> {
    vec![
        Segment::keep("cmt").brackets(Brackets::Square),
        Segment::elastic("Music/Artist/Album/song.mkv"),
        Segment::elastic("mediapm-conductor-builtin-archive"),
    ]
}

/// A head-keeping segment gives columns back from its end, so a version
/// walks down one column at a time: `7.1`, `7.`, `7`, then nothing. The
/// clip lands between characters, and a version carries none to cut after.
#[test]
fn head_keeping_clip_takes_columns_from_the_end_of_a_version() {
    let version = vec![Segment::elastic_head("7.1")];
    assert_eq!(fit_segments(&version, 3), "7.1");
    assert_eq!(fit_segments(&version, 2), "7.");
    assert_eq!(fit_segments(&version, 1), "7");
    assert_eq!(fit_segments(&version, 0), "");
}

/// The two elastic modes cut opposite ends of the same value, so they have
/// to disagree about its shape at the same width.
#[test]
fn the_two_elastic_modes_are_not_the_same_clip() {
    // Both clips land between characters, so at two columns a version reads
    // from the left under one mode and from the right under the other.
    let version_head = vec![Segment::elastic_head("7.1")];
    let version_tail = vec![Segment::elastic("7.1")];
    assert_eq!(fit_segments(&version_tail, 2), ".1");
    assert_eq!(fit_segments(&version_head, 2), "7.");
    assert_eq!(fit_segments(&version_head, 1), "7");

    // A value with both ends worth naming shows the same split at one width.
    let path_head = vec![Segment::elastic_head("Music/song.mkv")];
    let path_tail = vec![Segment::elastic("Music/song.mkv")];
    assert_eq!(fit_segments(&path_head, 8), "Music/so");
    assert_eq!(fit_segments(&path_tail, 8), "song.mkv");
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

/// Rendered length never rises as the slot narrows.
///
/// This is the property the boundary-snapped clip could not hold. Snapping
/// moved the tail start to the next boundary, and where that boundary sits is
/// a step function of the budget, so the row was 38 columns at width 45, 46
/// columns at width 46, and 38 again at width 47. A row that grows as the
/// window narrows reads as jitter rather than as a label. The blind clip has
/// no step in it, so the ladder is checked over the whole range the
/// transcript fixtures are captured at.
#[test]
fn rendered_length_is_monotone_in_width() {
    for segments in [step_segments(), path_segments()] {
        let mut previous = fit_segments(&segments, 121).chars().count();
        for width in (8..=120).rev() {
            let out = fit_segments(&segments, width);
            let len = out.chars().count();
            assert!(
                len <= previous,
                "width {width} rendered {len} columns, more than the {} below it: {out:?}",
                previous + 1,
            );
            assert!(len <= width, "width {width} overflowed: {out:?}");
            previous = len;
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
    assert_eq!(
        shrunk, "[wf] [F] 1/4 9.9.9 default s5 in-archive",
        "elastic segment not shortened to its tail"
    );
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

/// A clipped tool name never leaves its closing parenthesis behind.
///
/// The parentheses used to be formatted into the segment's text, so they
/// were part of what the clip cut: one column short of the whole name, a row
/// read `)`. The name is the segment's content and the parentheses are its
/// decoration now, and a decoration is rendered only when the content is
/// rendered whole.
#[test]
fn a_clipped_tool_name_leaves_no_stray_parenthesis() {
    let segs = vec![
        Segment::keep("wf").brackets(Brackets::Square),
        Segment::elastic("ffmpeg").brackets(Brackets::Round),
    ];
    assert_eq!(fit_segments(&segs, 13), "[wf] (ffmpeg)");
    // One column less than the bracketed name is wide, so the name is cut
    // and takes its decoration with it.
    assert_eq!(fit_segments(&segs, 12), "[wf] ffmpeg");
    for width in 4..=13 {
        let out = fit_segments(&segs, width);
        assert!(
            !out.contains(')') || out.contains("(ffmpeg)"),
            "stray parenthesis at width {width}: {out:?}"
        );
    }
}

/// The bracket id on a materialization row is rendered whole or not at all.
///
/// The id used to sit inside the folder name, so a clip of the name could
/// leave `]` on the row with the `[` cut away. It is a segment of its own
/// now, held by `Keep`, so no width can produce half of its brackets.
#[test]
fn a_clipped_bracket_id_leaves_no_stray_bracket() {
    let segs = vec![
        Segment::elastic("Never Gonna Give You Up"),
        Segment::keep("youtube.dQw4w9WgXcQ").brackets(Brackets::Square),
    ];
    assert_eq!(fit_segments(&segs, 60), "Never Gonna Give You Up [youtube.dQw4w9WgXcQ]");
    for width in 5..=60 {
        let out = fit_segments(&segs, width);
        assert!(
            !out.contains(']') || out.contains("[youtube.dQw4w9WgXcQ]"),
            "stray bracket at width {width}: {out:?}"
        );
    }
}

/// A bracket group spanning two segments renders whole or not at all.
///
/// The step row wraps its tool name and the version beside it in one pair of
/// parentheses, so the opening bracket belongs to the name and the closing one
/// to the version. A member that shortens takes the pair down with it: a row
/// showing `(ffmpeg` or `ffmpeg v7.` has a bracket whose partner is missing,
/// and the partner is exactly what makes a bracket readable.
#[test]
fn a_bracket_group_over_two_segments_is_all_or_nothing() {
    let segs = vec![
        Segment::keep("wf").brackets(Brackets::Square),
        Segment::elastic("ffmpeg").spanning_brackets(Brackets::Round, 2),
        Segment::elastic_head("v7.1"),
    ];
    assert_eq!(fit_segments(&segs, 20), "[wf] (ffmpeg v7.1)");
    // Two columns short of the bracketed group, the version has given two
    // of its four columns back and the pair goes with it.
    assert_eq!(fit_segments(&segs, 15), "[wf] ffmpeg v7");
    // A group that lost a member to the drop phase has lost it the same way:
    // at 13 the version is gone whole and the name keeps no parentheses.
    assert_eq!(fit_segments(&segs, 13), "[wf] ffmpeg");
    for width in 4..=20 {
        let out = fit_segments(&segs, width);
        let opens = out.matches('(').count();
        assert_eq!(
            opens,
            out.matches(')').count(),
            "half a bracket group at width {width}: {out:?}"
        );
        assert!(opens <= 1, "two bracket groups at width {width}: {out:?}");
        assert!(out.chars().count() <= width, "overflowed at width {width}: {out:?}");
    }
}

/// The one-member group is the shape every other bracketed segment draws.
///
/// `brackets` is `spanning_brackets` with a single member, so the pair lands on
/// one segment whether the caller says so or not. Anything that renders the
/// same at one member has to keep rendering the same at two.
#[test]
fn a_one_member_group_renders_the_single_segment_shape() {
    let names = ["ffmpeg", "media-conductor-builtin-archive", "a"];
    for brackets in [Brackets::Round, Brackets::Square] {
        for name in names {
            let single = vec![Segment::keep("wf"), Segment::elastic(name).brackets(brackets)];
            let group =
                vec![Segment::keep("wf"), Segment::elastic(name).spanning_brackets(brackets, 1)];
            for width in 0..=40 {
                assert_eq!(
                    fit_segments(&single, width),
                    fit_segments(&group, width),
                    "{name:?} at width {width}"
                );
            }
        }
    }
}

/// A group that names fewer members than it has wraps the members it has.
///
/// A `members` below one is a caller mistake rather than a rendering choice,
/// and the narrow reading of it is the single-segment group every other call
/// site already draws.
#[test]
fn a_group_below_one_member_is_a_single_segment() {
    let segs =
        vec![Segment::keep("wf"), Segment::elastic("ffmpeg").spanning_brackets(Brackets::Round, 0)];
    assert_eq!(fit_segments(&segs, 12), "wf (ffmpeg)");
}

#[test]
fn front_tail_keeps_the_tail() {
    assert_eq!(front_tail("abcdef ghij", 5), " ghij");
    assert_eq!(front_tail("abcdef ghij", 4), "ghij");
    assert_eq!(front_tail("abcdefghij", 6), "efghij");
    assert_eq!(front_tail("abc", 10), "abc");
    assert_eq!(front_tail("abcdef", 0), "");
    assert_eq!(front_tail("", 0), "");
}

#[test]
fn front_tail_preserves_a_path_tail() {
    let short = front_tail("Music/Artist/Album/song.mkv", 15);
    assert_eq!(short, "/Album/song.mkv");
    assert!(short.ends_with("song.mkv"), "filename lost: {short:?}");
}

#[test]
fn empty_input_yields_empty_output() {
    assert_eq!(fit_segments(&[], 40), "");
    assert_eq!(fit_segments(&[Segment::keep("wf").brackets(Brackets::Square)], 0), "");
}
