//! Unit tests for the shared fitting ladder (`fit_segments`).
//!
//! These pin the properties the Phase 2 design promises at the
//! pure-function level, with no terminal and no label type involved. A
//! failure here means the fitting policy itself is broken, which the
//! per-label tests would not localise.

use crate::progress::{Segment, Shrink, fit_segments, front_tail};

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

/// A segment set whose elastic pieces are paths and a hyphenated name, the
/// shapes the three progress screens actually clip. Neither carries a space,
/// so each one reaches the output as a single token and a token can be traced
/// back to the segment it came from.
fn path_segments() -> Vec<Segment> {
    vec![
        Segment::keep("[cmt]"),
        Segment::elastic("Music/Artist/Album/song.mkv"),
        Segment::elastic("mediapm-conductor-builtin-archive"),
    ]
}

/// Whether `token` is a tail [`front_tail`] could have produced from `seg`.
///
/// The clip cuts back to a boundary, so the budget that produced a tail is
/// not its own width: `front_tail` on the tail's width cuts further. The
/// tail is legitimate when some budget at or above that width returns it
/// unchanged, which is what a whole segment and a clipped one both do.
fn is_a_tail_of(token: &str, seg: &Segment) -> bool {
    (token.chars().count()..=seg.text.chars().count())
        .any(|budget| front_tail(&seg.text, budget) == token)
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

/// Every width stays inside the budget, and no width renders a token that
/// could only have come from cutting a word in half.
///
/// This replaces `output_length_is_monotonic_in_width`. Monotonicity was
/// dropped because the clip snaps the tail start to a boundary, and where the
/// next boundary sits is a step function of the budget: on `step_segments` the
/// row is 38 columns at width 45, 46 columns at width 46, and 38 again at
/// width 47, because only width 46 lands the tail right after a hyphen. A row
/// that gains text as the window narrows is jitter, but it never overflows and
/// it never shows half a word, which is the defect that mattered.
#[test]
fn a_row_never_overflows_and_never_splits_a_word() {
    let segments = path_segments();
    // The widest probe has to carry real content, or an implementation that
    // returns "" at every width passes the loop below vacuously.
    let widest = fit_segments(&segments, 80);
    assert!(
        widest.contains("[cmt]") && widest.contains("song.mkv") && widest.contains("archive"),
        "widest probe lost content: {widest:?}"
    );

    for width in 0..80 {
        let out = fit_segments(&segments, width);
        assert!(out.chars().count() <= width, "width {width} overflowed: {out:?}");
        for token in out.split(' ').filter(|t| !t.is_empty()) {
            let legitimate = segments
                .iter()
                .any(|s| token == s.text || (s.shrink == Shrink::Front && is_a_tail_of(token, s)));
            assert!(
                legitimate,
                "fragment {token:?} that no clip of a segment produces at width {width}: {out:?}"
            );
        }
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
                if s.shrink != Shrink::Front {
                    return token == s.text;
                }
                // A whole elastic segment, or a clipped one: the token must
                // be a non-empty suffix of the segment, because the cut takes
                // the head and leaves the tail.
                token == s.text || (!token.is_empty() && s.text.ends_with(token))
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
    assert_eq!(
        shrunk, "[wf] [F] 1/4 9.9.9 default s5 archive)",
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

/// A tail that starts inside a word names nothing, so the clip yields
/// nothing and the drop phase takes the segment whole. This is the case the
/// one-column floor used to paper over: `(ffmpeg)` cut to `fmpeg)` spends six
/// columns of a row on a tool the reader cannot name, where the same six
/// columns buy back the whole segment or give way to the head beside it.
#[test]
fn a_tail_that_starts_mid_word_yields_nothing() {
    let segs = vec![Segment::keep("[wf]"), Segment::keep("1/4"), Segment::elastic("(ffmpeg)")];
    // `[wf] 1/4 (ffmpeg)` is 17 columns, so these three widths hand the tool
    // name 8, 7 and 6 columns in turn.
    assert_eq!(fit_segments(&segs, 17), "[wf] 1/4 (ffmpeg)");
    // Seven columns starts right after the bracket, which is a boundary, so
    // the whole name survives minus the bracket that opened it.
    assert_eq!(fit_segments(&segs, 16), "[wf] 1/4 ffmpeg)");
    // Six columns start inside the tool's name and carry no boundary to cut
    // after, so the name is dropped rather than rendered as a fragment.
    assert_eq!(fit_segments(&segs, 15), "[wf] 1/4");
}

/// The clip cuts after the first boundary in the tail it was given, so the
/// character the cut lands on stays and everything behind the boundary is
/// kept. A window that already begins at a boundary is kept whole, which is
/// what keeps `ffmpeg)` and `Astley` from being trimmed a second time.
#[test]
fn a_tail_is_cut_after_the_first_boundary_it_contains() {
    // `he Wall` and ` Wall` both carry a space, so both lose the partial word
    // in front of it and keep `Wall`.
    assert_eq!(front_tail("Music/Pink Floyd/The Wall", 7), "Wall");
    assert_eq!(front_tail("Music/Pink Floyd/The Wall", 6), "Wall");
    // A window with no boundary in it yields nothing rather than the partial
    // word it happens to cover.
    assert_eq!(front_tail("Music/Has the Right to Children", 6), "");
    // A window that begins at a boundary is already a whole word or a whole
    // bracket group, at exactly the budget and at more than it.
    assert_eq!(front_tail("Music/Has the Right to Children", 8), "Children");
    // The leading space of a nine-column tail is itself a boundary, so the
    // cut lands after it and the row shows no double space.
    assert_eq!(front_tail("Music/Has the Right to Children", 9), "Children");
    // The bracket is a boundary, so a tool name keeps everything after it.
    assert_eq!(front_tail("(ffmpeg)", 7), "ffmpeg)");
    assert_eq!(front_tail("(ffmpeg)", 8), "(ffmpeg)");
    // A path separator is a boundary, so the last element survives whole.
    assert_eq!(front_tail("music videos/Rick Astley", 6), "Astley");
    // A hyphen separates the words of a hyphenated name the way a space
    // separates the words of a phrase.
    assert_eq!(front_tail("mediapm-conductor-builtin-archive)", 10), "archive)");
    assert_eq!(front_tail("mediapm-conductor-builtin-archive)", 16), "builtin-archive)");
    // A dot is not a boundary, so the stem of a filename cannot be cut away
    // to leave its extension.
    assert_eq!(front_tail("01 - Telepathy.flac", 14), "Telepathy.flac");
    assert_eq!(front_tail("01 - Telepathy.flac", 13), "");
    // A tail may not begin on a space, a `/`, or a `-`. A window that opens
    // on one has been cut at the joiner rather than after it.
    assert_eq!(front_tail("01 - Telepathy.flac", 16), "Telepathy.flac");
    assert_eq!(front_tail("Music/Artist/Album/song.mkv", 8), "song.mkv");
    // A tail of nothing but a space is the double space a row used to show
    // between two segments, so it yields nothing.
    assert_eq!(front_tail("Never Gonna Give You Up ", 1), "");
}

#[test]
fn front_tail_keeps_the_tail() {
    // A window that starts at a boundary is kept whole, which is what a tail
    // cut back to a space looks like.
    assert_eq!(front_tail("abcdef ghij", 5), "ghij");
    assert_eq!(front_tail("abcdef ghij", 4), "ghij");
    // A window with no boundary in it yields nothing rather than the letters
    // it happens to cover.
    assert_eq!(front_tail("abcdefghij", 6), "");
    assert_eq!(front_tail("abc", 10), "abc");
    assert_eq!(front_tail("abcdef", 0), "");
    assert_eq!(front_tail("", 0), "");
}

#[test]
fn front_tail_preserves_a_path_tail() {
    let short = front_tail("Music/Artist/Album/song.mkv", 15);
    assert_eq!(short, "Album/song.mkv");
    assert!(short.ends_with("song.mkv"), "filename lost: {short:?}");
}

#[test]
fn empty_input_yields_empty_output() {
    assert_eq!(fit_segments(&[], 40), "");
    assert_eq!(fit_segments(&[Segment::keep("[wf]")], 0), "");
}
