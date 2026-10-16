//! Single-writer frame emission: one repaint per changed frame, zero writes per unchanged one.
//!
//! [`CountingTerm`] is the shared instrument, moved here from
//! `tests_blank_slot_draws` so both inner test modules measure terminal
//! writes the same way: it wraps an [`indicatif::InMemoryTerm`] grid and
//! counts the `write_line` and `flush` calls that reach it.

use super::writer::write_frame;
use indicatif::TermLike;
use std::sync::Arc;
use std::sync::atomic::{AtomicUsize, Ordering};

/// A [`TermLike`] that counts the lines written to a wrapped
/// [`indicatif::InMemoryTerm`].
///
/// The line count is one `write_line` per draw on the indicatif path and
/// one per written row (including vacated-row clears) under
/// [`write_frame`]; `flushes` is the repaint unit on both paths.
/// indicatif ends every draw with one `write_line`, so on that path the
/// count is the number of draws that reached the target. Re-blanking a
/// blank slot changes nothing a frame can show, so a test that pins it
/// has to count draws rather than read one.
#[derive(Debug)]
pub(crate) struct CountingTerm {
    /// The grid the draws land in, kept so a test can read the frame back.
    grid: indicatif::InMemoryTerm,
    /// Number of `write_line` calls since the counter was last read.
    lines: Arc<AtomicUsize>,
    /// One `flush` is one completed repaint: the unit AC1 counts.
    flushes: Arc<AtomicUsize>,
}

/// `(term, line counter, flush counter)` for a `rows x cols` grid.
pub(crate) fn counting_term(
    rows: u16,
    cols: u16,
) -> (CountingTerm, Arc<AtomicUsize>, Arc<AtomicUsize>) {
    let term = CountingTerm {
        grid: indicatif::InMemoryTerm::new(rows, cols),
        lines: Arc::new(AtomicUsize::new(0)),
        flushes: Arc::new(AtomicUsize::new(0)),
    };
    let lines = Arc::clone(&term.lines);
    let flushes = Arc::clone(&term.flushes);
    (term, lines, flushes)
}

impl TermLike for CountingTerm {
    fn width(&self) -> u16 {
        self.grid.width()
    }
    fn height(&self) -> u16 {
        self.grid.height()
    }
    fn move_cursor_up(&self, n: usize) -> std::io::Result<()> {
        self.grid.move_cursor_up(n)
    }
    fn move_cursor_down(&self, n: usize) -> std::io::Result<()> {
        self.grid.move_cursor_down(n)
    }
    fn move_cursor_right(&self, n: usize) -> std::io::Result<()> {
        self.grid.move_cursor_right(n)
    }
    fn move_cursor_left(&self, n: usize) -> std::io::Result<()> {
        self.grid.move_cursor_left(n)
    }
    fn write_line(&self, s: &str) -> std::io::Result<()> {
        self.lines.fetch_add(1, Ordering::Relaxed);
        self.grid.write_line(s)
    }
    fn write_str(&self, s: &str) -> std::io::Result<()> {
        self.grid.write_str(s)
    }
    fn clear_line(&self) -> std::io::Result<()> {
        self.grid.clear_line()
    }
    fn flush(&self) -> std::io::Result<()> {
        self.flushes.fetch_add(1, Ordering::Relaxed);
        self.grid.flush()
    }
}

/// An unchanged frame costs zero terminal writes (spec AC1).
#[test]
fn identical_frame_writes_nothing() {
    let (term, lines, flushes) = counting_term(10, 40);
    let frame = vec!["a".to_string(), "b".to_string()];
    assert!(write_frame(&term, None, &frame, false).expect("first draw"), "first draw is a write");
    let after_first = flushes.load(Ordering::Relaxed);
    let lines_after_first = lines.load(Ordering::Relaxed);
    assert!(!write_frame(&term, Some(&frame), &frame, false).expect("second draw"), "unchanged");
    assert_eq!(flushes.load(Ordering::Relaxed), after_first, "no flush for no change");
    assert_eq!(lines.load(Ordering::Relaxed), lines_after_first, "no line write for no change");
}

/// A changed frame repaints in exactly one flush (spec AC1).
#[test]
fn changed_frame_repaints_once() {
    let (term, lines, flushes) = counting_term(10, 40);
    let before = ["a".to_string(), "b".to_string()];
    let after = ["a".to_string(), "c".to_string()];
    write_frame(&term, None, &before, false).expect("first draw");
    write_frame(&term, Some(&before), &after, false).expect("repaint");
    assert_eq!(flushes.load(Ordering::Relaxed), 2, "one flush per changed frame");
    assert_eq!(
        lines.load(Ordering::Relaxed),
        2,
        "one write_line per frame: the last line is the rest-on-last-row write_str"
    );
}

/// Shrinking the frame clears the rows it used to occupy (spec AC6).
#[test]
fn shrink_clears_the_vacated_rows() {
    let (term, _lines, _flushes) = counting_term(10, 40);
    let before = ["a".to_string(), "b".to_string(), "c".to_string()];
    let after = ["a".to_string()];
    write_frame(&term, None, &before, false).expect("first draw");
    write_frame(&term, Some(&before), &after, false).expect("shrink");
    let contents = term.grid.contents();
    assert!(!contents.contains('c'), "vacated row must be cleared: {contents:?}");
}

/// The writer's cursor anchor survives a repaint: nothing from the
/// previous frame may remain above the new frame (regression for the
/// off-by-one where the second repaint landed one row low).
#[test]
fn repaint_replaces_the_frame_in_place() {
    let (term, _lines, _flushes) = counting_term(10, 40);
    let first = vec!["first-a".to_string(), "first-b".to_string()];
    let second = vec!["second-a".to_string(), "second-b".to_string()];
    write_frame(&term, None, &first, false).expect("first draw");
    write_frame(&term, Some(&first), &second, false).expect("repaint");
    let contents = term.grid.contents();
    assert!(!contents.contains("first-a"), "stale first row survived: {contents:?}");
    assert!(contents.contains("second-a"), "new first row missing: {contents:?}");
    assert!(contents.contains("second-b"), "new second row missing: {contents:?}");
}

/// One row in, one row replaced: a repaint overwrites the row it entered
/// on, never the row below it.
#[test]
fn one_row_repaint_replaces_the_row_in_place() {
    let (term, _lines, _flushes) = counting_term(10, 40);
    let before = ["a".to_string()];
    let after = ["b".to_string()];
    write_frame(&term, None, &before, false).expect("first draw");
    write_frame(&term, Some(&before), &after, false).expect("repaint");
    let contents = term.grid.contents();
    assert!(!contents.contains('a'), "stale row survived: {contents:?}");
    assert!(contents.contains('b'), "new row missing: {contents:?}");
}

/// A shrink followed by a change still repaints in place: the shrink's
/// vacated rows are cleared and the exit anchor rests ON the new frame's
/// last row (regression for the drift where the next
/// back-up landed inside the frame).
#[test]
fn shrink_then_change_repaints_in_place() {
    let (term, _lines, _flushes) = counting_term(10, 40);
    let tall = vec!["tall-a".to_string(), "tall-b".to_string(), "tall-c".to_string()];
    let short = vec!["short-a".to_string()];
    let next = vec!["next-a".to_string()];
    write_frame(&term, None, &tall, false).expect("first draw");
    write_frame(&term, Some(&tall), &short, false).expect("shrink");
    write_frame(&term, Some(&short), &next, false).expect("change after shrink");
    let contents = term.grid.contents();
    assert!(!contents.contains("tall-a"), "stale tall row survived: {contents:?}");
    assert!(!contents.contains("short-a"), "previous row survived: {contents:?}");
    assert!(contents.contains("next-a"), "new row missing: {contents:?}");
}

/// `force` bypasses only the equality short-circuit: a resize owes a
/// write even when the new frame's strings match the last one, and the
/// cursor back-up still applies.
#[test]
fn force_writes_an_unchanged_frame() {
    let (term, _lines, flushes) = counting_term(10, 40);
    let frame = vec!["forced".to_string()];
    assert!(write_frame(&term, None, &frame, false).expect("first draw"), "first draw is a write");
    let flushes_after_first = flushes.load(Ordering::Relaxed);
    assert!(
        write_frame(&term, Some(&frame), &frame, true).expect("forced repaint"),
        "force must write an identical frame"
    );
    assert_eq!(
        flushes.load(Ordering::Relaxed),
        flushes_after_first + 1,
        "one flush for the forced repaint"
    );
}

/// A frame that fills the grid keeps every row: the final newline would
/// scroll the top row off a full-height grid, so the writer ends on the
/// bottom row instead, and the next entry's back-up shrinks by one to
/// match.
#[test]
fn full_height_frame_keeps_every_row() {
    let (term, _lines, _flushes) = counting_term(4, 40);
    let frame = vec!["a".to_string(), "b".to_string(), "c".to_string(), "d".to_string()];
    assert!(write_frame(&term, None, &frame, false).expect("first draw"), "first draw is a write");
    let contents = term.grid.contents();
    assert_eq!(
        contents, "a\nb\nc\nd",
        "a full-height frame must not scroll its top row: {contents:?}"
    );
    let changed = vec!["A".to_string(), "b".to_string(), "c".to_string(), "d2".to_string()];
    assert!(write_frame(&term, Some(&frame), &changed, false).expect("repaint"), "changed repaint");
    let contents = term.grid.contents();
    assert_eq!(contents, "A\nb\nc\nd2", "repaint must cover the frame in place: {contents:?}");
}

/// Shrinking away from a full-height frame clears the vacated rows without
/// scrolling and leaves the exit anchor resting on the new frame's last
/// row, so the
/// repaint after the shrink starts at the top row again.
#[test]
fn shrink_from_full_height_keeps_the_anchor() {
    let (term, _lines, _flushes) = counting_term(4, 40);
    let tall = vec![
        "tall-a".to_string(),
        "tall-b".to_string(),
        "tall-c".to_string(),
        "tall-d".to_string(),
    ];
    let short = vec!["short-a".to_string(), "short-b".to_string()];
    write_frame(&term, None, &tall, false).expect("first draw");
    write_frame(&term, Some(&tall), &short, false).expect("shrink");
    let contents = term.grid.contents();
    assert!(!contents.contains("tall-"), "stale row survived the shrink: {contents:?}");
    assert!(contents.starts_with("short-a\nshort-b"), "shrunk frame missing: {contents:?}");
    let next = vec!["next-1".to_string(), "next-2".to_string()];
    write_frame(&term, Some(&short), &next, false).expect("change after shrink");
    let contents = term.grid.contents();
    assert_eq!(contents, "next-1\nnext-2", "post-shrink repaint drifted: {contents:?}");
}
