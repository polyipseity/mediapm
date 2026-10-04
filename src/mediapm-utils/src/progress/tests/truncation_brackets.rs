//! The bracket-balance property of [`fit_segments`], swept over every
//! decoration shape the fitter can be handed.
//!
//! The fault this guards is a bracket reaching a row without its partner. It
//! was found twice, both times in a caller: a tool name whose parentheses were
//! formatted into the text, and a media id split out of a basename that then
//! clipped beside it. Both times the repair moved the brackets out of the text
//! and into [`Segment::brackets`], so the fitter owns the question of whether a
//! pair is drawn, and the question is worth asking of the fitter alone.
//!
//! Testing it per label needs a list of labels, and a list is what let a fourth
//! label ship unswept. Nothing here names a label: the cases are the segment
//! shapes, so a caller that builds a shape not in this list cannot reach an
//! untested branch of [`render_pieces`] through it.
//!
//! The sweep runs the whole width range the screen harnesses accept, 0 to 120,
//! because the interesting widths are in the middle of it. A row wide enough
//! for every segment renders its pair, a row that clips drops it, and the
//! transition between them is one column.

use crate::progress::{Brackets, Join, Segment, fit_segments};

/// Widest slot any case is swept at, matching the screen harness range.
const MAX_WIDTH: usize = 120;

/// First bracket or paren on `row` with no partner, in either direction.
///
/// The scan carries the partner each open expects rather than counting the two
/// kinds separately, so a `)` closing a `[` counts as matched and only a
/// mismatch or an unclosed open is a fault.
fn orphan_decoration(row: &str) -> Option<char> {
    let mut expected: Vec<char> = Vec::new();
    for ch in row.chars() {
        match ch {
            '[' => expected.push(']'),
            '(' => expected.push(')'),
            ']' | ')' if expected.pop() != Some(ch) => return Some(ch),
            _ => {}
        }
    }
    expected.pop()
}

/// A one-member group in round brackets, the shape a tool name wears on a
/// workflow screen. The name is elastic, so the widths where it shortens are
/// the widths where the pair has to go with it.
fn one_member_round_group() -> Vec<Segment> {
    vec![
        Segment::keep("wf").brackets(Brackets::Square),
        Segment::elastic("mediapm-conductor-builtin-archive").brackets(Brackets::Round),
    ]
}

/// The same one-member group in square brackets, the shape a phase tag and a
/// status marker wear. Round and square reach the same branch and are swept
/// both ways because the two are separate enum arms in the fitter.
fn one_member_square_group() -> Vec<Segment> {
    vec![
        Segment::keep("wf").brackets(Brackets::Square),
        Segment::elastic("mediapm-conductor-builtin-archive").brackets(Brackets::Square),
    ]
}

/// A two-member group whose members can both shorten, so the pair renders at
/// the widths where neither had to give anything back.
fn group_with_every_member_whole() -> Vec<Segment> {
    vec![
        Segment::keep("wf").brackets(Brackets::Square),
        Segment::elastic("ffmpeg").spanning_brackets(Brackets::Round, 2),
        Segment::elastic_head("v7.1"),
    ]
}

/// A two-member group whose first member is held whole and whose second is
/// elastic. Only the second can shorten, so every width where the pair is
/// missing is a width where the version clipped and took the pair down.
fn group_with_a_shortened_member() -> Vec<Segment> {
    vec![
        Segment::keep("wf").brackets(Brackets::Square),
        Segment::keep("ffmpeg").spanning_brackets(Brackets::Round, 2),
        Segment::elastic_head("v7.1.100"),
    ]
}

/// A three-member group whose members are all held whole. Nothing can shrink,
/// so the only way a member leaves the row is the drop phase, and a dropped
/// member has to take the pair with it exactly as a shortened one does.
fn group_with_a_dropped_member() -> Vec<Segment> {
    vec![
        Segment::keep("wf"),
        Segment::keep("ffmpeg").spanning_brackets(Brackets::Round, 3),
        Segment::keep("v7.1"),
        Segment::keep("s5"),
    ]
}

/// A group that names more members than the row holds at any width. The
/// declared span runs off the end of the segment list itself, so the fitter's
/// end-of-row guard is what keeps the pair off the row, at every width rather
/// than only the narrow ones.
fn group_running_off_the_end() -> Vec<Segment> {
    vec![
        Segment::keep("wf").brackets(Brackets::Square),
        Segment::keep("ffmpeg").spanning_brackets(Brackets::Round, 3),
        Segment::keep("v7.1"),
    ]
}

/// A row with no decoration anywhere, so the sweep covers the widths where the
/// clip lands mid-value on undecorated text.
fn undecorated_row() -> Vec<Segment> {
    vec![
        Segment::keep("wf"),
        Segment::elastic("Music/Artist/Album/song.mkv"),
        Segment::keep("9.9.9"),
    ]
}

/// A value the caller split in two, with the bracket group around the middle
/// piece and the tail piece rejoining it directly. This is the shape the
/// materialization id takes when it is lifted out of a basename, and it is the
/// only one where a bracket is not at either end of the segment list.
fn split_value_with_a_direct_join() -> Vec<Segment> {
    vec![
        Segment::elastic("01 - Telepathy"),
        Segment::keep("youtube.dQw4w9WgXcQ").brackets(Brackets::Square),
        Segment::keep(".link.mkv").joined(Join::Direct),
    ]
}

/// Every case the sweep runs, named so a failure names the shape that broke.
fn decoration_cases() -> Vec<(&'static str, Vec<Segment>)> {
    vec![
        ("one-member round group", one_member_round_group()),
        ("one-member square group", one_member_square_group()),
        ("group with every member whole", group_with_every_member_whole()),
        ("group with a shortened member", group_with_a_shortened_member()),
        ("group with a dropped member", group_with_a_dropped_member()),
        ("group running off the end", group_running_off_the_end()),
        ("undecorated row", undecorated_row()),
        ("split value with a direct join", split_value_with_a_direct_join()),
    ]
}

/// Assert that every width from nothing to [`MAX_WIDTH`] renders a row for
/// `segments` that fits its slot and carries no bracket without a partner.
fn assert_balanced_at_every_width(name: &str, segments: &[Segment]) {
    for width in 0..=MAX_WIDTH {
        let row = fit_segments(segments, width);
        assert!(row.chars().count() <= width, "{name} overflowed at width {width}: {row:?}");
        if let Some(orphan) = orphan_decoration(&row) {
            panic!("{name} rendered an orphan {orphan:?} at width {width}: {row:?}");
        }
    }
}

/// No fitted row, whatever it was handed, shows half of a bracket pair.
///
/// The scan tracks the partner each open expects rather than counting opens
/// against closes, so a row that pairs the wrong kinds is caught as well as one
/// that leaves an end unpaired. The width assertion rides along because a
/// balanced row that overflowed its slot would push the partner onto the next
/// terminal row, which is the same fault one column further out.
#[test]
fn no_fitted_row_carries_an_unmatched_bracket() {
    for (name, segments) in decoration_cases() {
        assert_balanced_at_every_width(name, &segments);
    }
}

/// Each case reaches the shape it is swept for, so the sweep is not vacuous.
///
/// A property that holds because nothing ever renders would pass just as
/// quietly as the fault it is meant to catch. Every case here asserts a width
/// where the pair is drawn and, where the case is about losing one, a width
/// where it is gone.
#[test]
fn each_case_renders_the_shape_the_sweep_relies_on() {
    let cases = decoration_cases();

    assert_eq!(
        fit_segments(&one_member_round_group(), 40),
        "[wf] (mediapm-conductor-builtin-archive)"
    );
    assert_eq!(fit_segments(&one_member_round_group(), 12), "[wf] archive");

    assert_eq!(
        fit_segments(&one_member_square_group(), 40),
        "[wf] [mediapm-conductor-builtin-archive]"
    );
    assert_eq!(fit_segments(&one_member_square_group(), 12), "[wf] archive");

    assert_eq!(fit_segments(&group_with_every_member_whole(), 20), "[wf] (ffmpeg v7.1)");
    assert_eq!(fit_segments(&group_with_a_shortened_member(), 30), "[wf] (ffmpeg v7.1.100)");
    assert_eq!(fit_segments(&group_with_a_shortened_member(), 21), "[wf] ffmpeg v7.1.100");

    assert_eq!(fit_segments(&group_with_a_dropped_member(), 20), "wf (ffmpeg v7.1 s5)");
    assert_eq!(fit_segments(&group_with_a_dropped_member(), 15), "wf ffmpeg v7.1");

    assert_eq!(fit_segments(&group_running_off_the_end(), 40), "[wf] ffmpeg v7.1");

    assert_eq!(fit_segments(&undecorated_row(), 40), "wf Music/Artist/Album/song.mkv 9.9.9");
    assert_eq!(fit_segments(&undecorated_row(), 13), "wf .mkv 9.9.9");

    assert_eq!(
        fit_segments(&split_value_with_a_direct_join(), 60),
        "01 - Telepathy [youtube.dQw4w9WgXcQ].link.mkv"
    );
    assert_eq!(
        fit_segments(&split_value_with_a_direct_join(), 30),
        "[youtube.dQw4w9WgXcQ].link.mkv"
    );

    // Every case renders something at a width no screen hands out, so a case
    // that quietly stopped fitting is noticed here rather than in the sweep.
    for (name, segments) in cases {
        assert!(!fit_segments(&segments, MAX_WIDTH).is_empty(), "{name} rendered nothing");
    }
}

/// A caller that formats its own brackets into the text loses them to the
/// clipper, and the fitter hands that row through unchanged.
///
/// The decoration API is the reason this is worth pinning: the fitter cannot
/// repair brackets it was never given, so the property it holds is narrower
/// than the property a row needs. This case says where the boundary is.
#[test]
fn brackets_formatted_into_the_text_are_the_callers_to_lose() {
    let segs = vec![Segment::keep("wf").brackets(Brackets::Square), Segment::elastic("(ffmpeg")];
    assert_eq!(fit_segments(&segs, 14), "[wf] (ffmpeg");
    assert!(orphan_decoration(&fit_segments(&segs, 14)).is_some());
}
