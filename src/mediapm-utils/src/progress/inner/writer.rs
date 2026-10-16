//! Single-write frame emission.
//!
//! [`write_frame`] is the only place a frame reaches the terminal: it
//! diffs the incoming frame against the last one emitted and repaints in
//! a single cursor-protocol pass. An unchanged frame writes nothing,
//! which is what keeps a busy screen from shimmering between identical
//! states (spec AC1).

use indicatif::TermLike;

/// Emit one frame as one repaint.
///
/// Returns `true` when bytes were written. An unchanged frame returns
/// `false` without touching the terminal, which is what keeps a busy
/// screen from shimmering between identical states. `force` bypasses only
/// that equality short-circuit — a resize owes a write even when the new
/// frame's strings match the old ones — and never the cursor arithmetic.
///
/// The cursor contract (legacy-compatible): the writer's last line is
/// written **without** its trailing newline, so the cursor rests *on* the
/// frame's last row — never one row past it, which on a bottom-anchored
/// frame would scroll the grid and drop the top row. The next entry
/// therefore backs up `old_rows - 1` (from the last row to the frame's
/// first), clears and rewrites every row, and exits on the new last row
/// again: the exit position *is* the next entry position, with no
/// separate anchor state. Between frames nothing advances the cursor;
/// only a screen commit does ( [`super::gate::WriteGate::commit_frame`],
/// issued by `finalize`), and that CR+LF is what makes the next screen's
/// band start below the committed frame.
///
/// # Errors
///
/// Propagates the first failing terminal operation (`move_cursor_up`,
/// `clear_line`, `write_line`, `write_str`, or `flush`); the frame is
/// whatever the terminal accepted before the failure.
pub(crate) fn write_frame<T: TermLike + ?Sized>(
    term: &T,
    previous: Option<&[String]>,
    next: &[String],
    force: bool,
) -> std::io::Result<bool> {
    if !force && previous == Some(next) {
        return Ok(false);
    }
    let height = usize::from(term.height()).max(1);
    // A frame taller than the terminal writes only its first `height`
    // rows: the rows below the grid are clipped, never scrolled in — the
    // legacy draw wrote its block the same way (last visible line without
    // a trailing newline), pinned by `an_unbound_slot_draws_an_empty_line`,
    // whose 5-slot frame on a 4-row grid must show its top four rows.
    let old_rows = previous.map_or(0, <[String]>::len).min(height);
    let next_rows = next.len().min(height);
    if old_rows > 0 {
        // The previous write exited *on* its last row: back up one row
        // fewer than its length to reach its first row.
        term.move_cursor_up(old_rows - 1)?;
    }
    for (i, line) in next.iter().take(height).enumerate() {
        term.clear_line()?;
        if i + 1 == next_rows {
            // Rest on the last row: a newline here would scroll a frame
            // that reaches the terminal's bottom row (and would break the
            // entry anchor above for every frame).
            term.write_str(line)?;
        } else {
            term.write_line(line)?;
        }
    }
    // Rows the previous frame held that this one does not: clear them so a
    // shrink cannot leave stale content below the frame. The write above
    // exited *on* the new last row, so step down to the first vacated row
    // before walking them (an empty `next` already starts at row 0, where
    // the entry back-up landed). The last clear omits its newline for the
    // same rest-on-last-row reason.
    if old_rows > next_rows && next_rows > 0 {
        term.move_cursor_down(1)?;
    }
    for i in next_rows..old_rows {
        term.clear_line()?;
        if i + 1 == old_rows {
            term.write_str("")?;
        } else {
            term.write_line("")?;
        }
    }
    // Clearing walked to the OLD extent's last row; back up to the new
    // frame's last row (or row 0 for an empty frame) so the exit anchor
    // matches the contract.
    if old_rows > next_rows {
        let cursor = old_rows - 1;
        let exit = next_rows.saturating_sub(1);
        term.move_cursor_up(cursor - exit)?;
    }
    term.flush()?;
    Ok(true)
}
