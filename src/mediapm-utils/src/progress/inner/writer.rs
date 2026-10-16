//! Single-write frame emission.
//!
//! [`write_frame`] is the only place a frame reaches the terminal: it
//! diffs the incoming frame against the last one emitted and repaints in
//! a single cursor-protocol pass. An unchanged frame writes nothing,
//! which is what keeps a busy screen from shimmering between identical
//! states (spec AC1).

// Task 5 swaps `run_frame` onto this writer; until that wiring lands, only
// the `#[cfg(test)]` consumers call it, and the plain-lib build would flag
// every item as dead. Mirrors the module-level allowance in `plan.rs`;
// remove together with the Task 5 wiring.
#![allow(dead_code)]

use indicatif::TermLike;

/// Emit one frame as one repaint.
///
/// Returns `true` when bytes were written. An unchanged frame returns
/// `false` without touching the terminal, which is what keeps a busy
/// screen from shimmering between identical states.
///
/// The cursor contract: the writer enters with the cursor one row past
/// the previous frame — where a `write_line`-based pass leaves it — backs
/// up over exactly the rows that frame occupied, clears and rewrites each
/// of them, and exits one row past the new frame. There is no separate
/// anchor restoration: the exit position *is* the next entry position.
///
/// # Errors
///
/// Propagates the first failing terminal operation (`move_cursor_up`,
/// `clear_line`, `write_line`, or `flush`); the frame is whatever the
/// terminal accepted before the failure.
pub(crate) fn write_frame<T: TermLike + ?Sized>(
    term: &T,
    previous: Option<&[String]>,
    next: &[String],
) -> std::io::Result<bool> {
    if previous == Some(next) {
        return Ok(false);
    }
    let old_rows = previous.map_or(0, <[String]>::len);
    if old_rows > 0 {
        term.move_cursor_up(old_rows)?;
    }
    for line in next {
        term.clear_line()?;
        term.write_line(line)?;
    }
    // Rows the previous frame held that this one does not: clear them so a
    // shrink cannot leave stale content below the frame.
    for _ in next.len()..old_rows {
        term.clear_line()?;
        term.write_line("")?;
    }
    // Clearing walked to the OLD extent's end; restore the exit anchor to one
    // row past the NEW frame so the next entry's back-up is exact.
    if old_rows > next.len() {
        term.move_cursor_up(old_rows - next.len())?;
    }
    term.flush()?;
    Ok(true)
}
