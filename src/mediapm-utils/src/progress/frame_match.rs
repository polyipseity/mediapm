//! Comparing two rendered frames without pinning the spinner's phase.
//!
//! A frame is the text a progress bar drew on one line of a terminal. Comparing
//! two of them byte for byte also compares which animation glyph the spinner
//! column holds, and that glyph is not a property of the layout: `indicatif`
//! advances a bar's tick through a wall-clock token bucket, so the glyph a row
//! shows at the end of a run depends on how much real time passed between that
//! row's draws and on what every other row on screen drew in between. Two runs
//! that draw the same bars in the same order can therefore end with different
//! glyphs, and a fixture that records them fails on a machine whose timing
//! differs from the one that captured it.
//!
//! [`assert_frames_match`] compares everything else exactly and accepts any
//! configured spinner glyph in the spinner column.
//!
//! # What this comparison no longer catches
//!
//! A frame whose only difference is the spinner's glyph passes, where an exact
//! comparison would have failed it. So does a swap between the spinner and a
//! tick glyph that appears as text further along the same row: the spinner
//! column accepts any configured glyph, and the other column is compared to the
//! character the frame draws there, so a swap changes that one and is caught.
//! What the comparison cannot catch is a glyph that changes to a glyph, because
//! that is a difference it cannot see from the text alone.
//!
//! Every other difference still fails. A row that gained or lost a column, a
//! label that reads differently, a bracket pair that does not match, a fraction
//! whose numbers moved, a fill that is the wrong width, a row that overflowed
//! its slot and wrapped onto the line below, and a row that appeared or
//! vanished all differ at a column this comparison does not exempt.

use crate::progress::inner::{SPINNER_COLUMN, TICK_CHARS, strip_ansi};

/// Every character the spinner column of a rendered frame can hold.
///
/// These are the characters every production bar style configures. The first
/// `len - 1` of them cycle while a bar runs and the last is the string a
/// finished bar draws, so the returned sequence is longer than the cycle a
/// running bar goes through.
#[must_use = "the glyph set is the input to a comparison, not a value to draw"]
pub fn spinner_glyphs() -> std::str::Chars<'static> {
    TICK_CHARS.chars()
}

/// Compare two rendered frames, treating the spinner column as animation phase.
///
/// Both frames are compared as text with ANSI escape sequences removed, through
/// the same stripper the renderer measures a label with, so a frame that
/// carries colour codes matches one that does not. Lines are compared in order
/// and characters within a line are compared in order. At the first column two
/// spinner glyphs match each other, and nothing else does.
///
/// # Panics
///
/// Panics when the frames have a different number of lines, or when any line
/// differs at a column this comparison compares exactly. The message names the
/// one-based row and the zero-based column, shows both characters, prints both
/// lines, and carries `context`, which the caller uses to say which frame was
/// compared with which.
pub fn assert_frames_match(expected: &str, actual: &str, context: &str) {
    let expected = strip_ansi(expected);
    let actual = strip_ansi(actual);
    let expected_lines: Vec<&str> = expected.lines().collect();
    let actual_lines: Vec<&str> = actual.lines().collect();
    assert!(
        expected_lines.len() == actual_lines.len(),
        "{context}: the frames have {} rows and {} rows\n  expected: {expected:?}\n  actual:   \
         {actual:?}",
        expected_lines.len(),
        actual_lines.len()
    );
    for (index, (expected_line, actual_line)) in
        expected_lines.iter().zip(actual_lines.iter()).enumerate()
    {
        if let Some(column) = first_difference(expected_line, actual_line) {
            panic!(
                "{context}: row {} column {column} differs: expected {}, got {}\n  expected: \
                 {expected_line:?}\n  actual:   {actual_line:?}",
                index + 1,
                describe(char_at(expected_line, column)),
                describe(char_at(actual_line, column))
            );
        }
    }
}

/// Column of the first character that differs, or `None` when the lines match.
///
/// The spinner column is compared as a set: two characters that are both
/// configured spinner glyphs are the same cell whatever phase each was drawn
/// at, and a character that is not one of them is still a difference there.
fn first_difference(expected: &str, actual: &str) -> Option<usize> {
    let mut expected_chars = expected.chars().enumerate();
    let mut actual_chars = actual.chars().enumerate();
    loop {
        match (expected_chars.next(), actual_chars.next()) {
            (None, None) => return None,
            (Some((column, expected_char)), Some((_, actual_char))) => {
                let spinner_is_phase = column == SPINNER_COLUMN
                    && is_spinner(expected_char)
                    && is_spinner(actual_char);
                if expected_char != actual_char && !spinner_is_phase {
                    return Some(column);
                }
            }
            // One line ran out before the other, so the shorter one ends here.
            _ => return Some(expected.chars().count().min(actual.chars().count())),
        }
    }
}

/// Whether `character` is one of the configured spinner glyphs.
fn is_spinner(character: char) -> bool {
    TICK_CHARS.contains(character)
}

/// The character `line` holds at `column`, or `None` when the line is shorter.
///
/// Only reachable in a failure report, where a row that ended early is itself
/// the difference and reads better as absent than as a guess.
fn char_at(line: &str, column: usize) -> Option<char> {
    line.chars().nth(column)
}

/// Render one cell of a failure report, so a row that ended early says so.
///
/// `{:?}` on an `Option` would print `Some('g')` where a reader wants `'g'`,
/// and `None` where they want to know the row ran out.
fn describe(character: Option<char>) -> String {
    match character {
        Some(character) => format!("{character:?}"),
        None => "nothing, the row ended".to_string(),
    }
}
