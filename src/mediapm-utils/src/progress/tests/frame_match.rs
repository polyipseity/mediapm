//! Tests for [`assert_frames_match`](super::super::frame_match::assert_frames_match).
//!
//! The comparison gives up one thing on purpose, which is the spinner glyph at
//! the spinner column. Each test below pins one side of that trade: the
//! differences it accepts, and the differences next to them that it still
//! catches.

use super::super::frame_match::{assert_frames_match, spinner_glyphs};

/// Run the comparison and hand back the message it refused with.
///
/// The comparison fails by panicking, which is what a caller sees. Reading the
/// panic back rather than marking each test `#[should_panic]` lets a test check
/// the message, so a refusal that names the wrong row or the wrong reason does
/// not pass as a refusal at all.
fn difference(expected: &str, actual: &str) -> Option<String> {
    let refused = std::panic::catch_unwind(|| assert_frames_match(expected, actual, "a fixture"));
    refused.err().map(|payload| match payload.downcast::<String>() {
        Ok(message) => *message,
        Err(payload) => match payload.downcast::<&str>() {
            Ok(message) => (*message).to_string(),
            Err(_) => "the comparison panicked without a message".to_string(),
        },
    })
}

/// Assert that the two frames are told apart, and hand the message back so the
/// caller can go on reading what the refusal named.
fn assert_differs(expected: &str, actual: &str) -> String {
    difference(expected, actual)
        .unwrap_or_else(|| panic!("the frames matched, so the difference went unread:\n{expected}"))
}

/// Assert that the two frames match, which means there is no refusal to read.
fn assert_matches(expected: &str, actual: &str) {
    assert_eq!(
        difference(expected, actual),
        None,
        "the frames were told apart, so a difference went unexempted"
    );
}

/// A row as a progress bar draws it: a spinner, a label, a fill and a tally.
const ROW: &str = "⠹ default s1 (ffmpeg) [active] ███░░ 2/5 28s";

/// The glyph set is the configured tick string, the cycle and the final string.
///
/// `indicatif` indexes the configured strings with `tick % (len - 1)`, so a
/// finished bar draws the last character and a running bar cycles the rest. A
/// set sized to the cycle alone would refuse every finished row on a real
/// screen, which is most of them, so the count is pinned at the full list.
#[test]
fn the_glyph_set_covers_the_finished_string_as_well_as_the_cycle() {
    let glyphs: Vec<char> = spinner_glyphs().collect();
    assert_eq!(glyphs.len(), 10);
    assert_eq!(glyphs.first(), Some(&'⠋'));
    assert_eq!(glyphs.last(), Some(&'⠏'));
}

/// Two frames that differ only in the spinner glyph match.
///
/// This is the reason the comparison exists: the same bar drawn at a different
/// point in its animation is the same layout.
#[test]
fn a_different_glyph_in_the_spinner_column_matches() {
    assert_matches(ROW, &ROW.replacen('⠹', "⠧", 1));
}

/// Every glyph in the set is accepted at the spinner column, not only the ones a
/// running bar cycles through.
#[test]
fn every_configured_glyph_is_accepted_in_the_spinner_column() {
    for glyph in spinner_glyphs() {
        let respun: String = glyph.to_string() + &ROW['⠹'.len_utf8()..];
        assert_matches(ROW, &respun);
    }
}

/// The exemption is the spinner column and nothing else.
///
/// A swap between the spinner and another tick glyph further along the row is a
/// difference, because a tick glyph that a row carries as text is label
/// content and has to compare exactly.
#[test]
fn a_glyph_outside_the_spinner_column_still_differs() {
    let moved = ROW.replacen('⠹', " ", 1).replacen("active", "⠹", 1);
    assert_differs(ROW, &moved);
}

/// A changed label is caught, and the refusal names the row, the column and
/// both characters, because a message that said only "differs" would leave a
/// reader hunting through a screen.
#[test]
fn a_changed_label_is_caught_and_named() {
    let refusal = assert_differs(ROW, &ROW.replace("(ffmpeg)", "(ffmpeq)"));
    assert!(refusal.contains("row 1 column 19"), "{refusal}");
    assert!(refusal.contains("expected 'g', got 'q'"), "{refusal}");
}

/// A changed bracket is caught, because a bracket pair is part of the label.
#[test]
fn a_changed_bracket_is_caught() {
    assert_differs(ROW, &ROW.replace("[active]", "(active)"));
}

/// A changed fraction is caught, because the tally is a label field.
#[test]
fn a_changed_fraction_is_caught() {
    assert_differs(ROW, &ROW.replace("2/5", "3/5"));
}

/// A changed timing is caught, because the elapsed column is compared too.
#[test]
fn a_changed_timing_is_caught() {
    assert_differs(ROW, &ROW.replace("28s", "29s"));
}

/// A fill one cell wider is caught.
///
/// The fill says how far along a bar is, so its width is the regression this
/// gate most has to keep catching.
#[test]
fn a_changed_fill_width_is_caught() {
    assert_differs(ROW, &ROW.replace("███░░", "████░"));
}

/// A fill one cell narrower is caught as well, so the check is a width and not
/// a count of the cells that are done.
#[test]
fn a_narrower_fill_is_caught() {
    assert_differs(ROW, &ROW.replace("███░░", "██░░░"));
}

/// A row that overflowed its slot and wrapped shows up as an extra row, and the
/// refusal says so through the row count rather than through a column.
#[test]
fn a_row_that_wrapped_is_caught_as_an_extra_row() {
    let wrapped = format!("{ROW}\n  spilled onto the line below");
    let refusal = assert_differs(ROW, &wrapped);
    assert!(refusal.contains("1 rows and 2 rows"), "{refusal}");
}

/// A row that vanished is caught on the row count.
#[test]
fn a_missing_row_is_caught() {
    assert_differs(ROW, "");
}

/// Colour codes do not decide whether two frames match.
///
/// Both frames carry the escapes a real grid carries, one on a side only, and
/// they still match, because the comparison strips them with the stripper the
/// renderer measures a label with rather than with one of its own.
#[test]
fn escape_sequences_on_one_side_do_not_change_the_result() {
    let coloured = format!("\x1b[32m{ROW}\x1b[0m");
    assert_matches(&coloured, ROW);
    assert_matches(ROW, &coloured);
}

/// A character in the spinner column that is not a configured glyph is caught.
///
/// The exemption covers the configured set, so a frame whose first column lost
/// its spinner and gained text is refused rather than waved through.
#[test]
fn a_non_glyph_in_the_spinner_column_is_caught() {
    assert_differs(ROW, &ROW.replacen('⠹', "x", 1));
}

/// A blank row matches a blank row, and a row that appeared on one side only is
/// caught.
///
/// A finished screen leaves rows nothing was drawn on, so the comparison has to
/// read an empty line as content rather than as a row that went missing.
#[test]
fn a_blank_row_matches_another_blank_row() {
    assert_matches("⠏ idle\n\n⠏ idle", "⠋ idle\n\n⠹ idle");
    assert_differs("⠏ idle\n\n⠏ idle", "⠋ idle\n⠹ idle");
}

/// Rows are compared by position, so a blank line that moved is caught.
#[test]
fn a_blank_line_that_moved_is_caught() {
    assert_matches("⠏ idle\n\n⠏ idle\n", "⠋ idle\n\n⠸ idle\n");
    assert_differs("⠏ idle\n⠋ idle", "⠏ idle\n\n⠋ idle");
}
