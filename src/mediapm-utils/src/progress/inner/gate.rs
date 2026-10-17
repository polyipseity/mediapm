//! The frame writer's terminal handle and the screen-commit cursor advance.
//!
//! [`WriteGate`] is what the renderer holds so its frame writer can reach
//! the terminal ([`WriteGate::term`]) and so a committed screen can advance
//! the cursor past its frame ([`WriteGate::commit_frame`]). There is no
//! write suppression here: since the plan became the single writer, nothing
//! else in this crate pushes to the terminal, so the gate degenerates to a
//! term handle plus the one cursor advance a screen owes.
//!
//! # Committing a frame also advances the cursor past it
//!
//! A screen commit hands its frame over to the terminal (`LineAdjust::Keep`,
//! deliberately not a clear) so later output cannot touch it. The draw
//! protocol on its own does not deliver that: it leaves the cursor resting on
//! the frame's last row, so the frame is still the live region — the next
//! screen's frame is written starting at the cursor, claims the committed row
//! as one of its own, and the following in-place redraw clears it. The
//! advance issued by [`WriteGate::commit_frame`] is what makes "committed"
//! true for the terminal rather than nominal.

use std::sync::Arc;

use indicatif::TermLike;

/// The renderer's terminal handle plus the screen-commit cursor advance.
///
/// Obtained from the terminal builder (`with_term_like` /
/// `with_multi_progress_and_term`), from the production default over
/// `console::Term::stderr()`, or from [`WriteGate::new_noop`] for the inert
/// terminal that has no term to write through.
#[derive(Debug, Clone)]
pub(crate) struct WriteGate {
    /// The terminal frames are written through, when there is one.
    ///
    /// `None` only for an inert terminal ([`WriteGate::new_noop`]):
    /// `run_frame` writes its rendered frame through this handle (one
    /// `write_frame` pass) and skips the write when the gate has no term.
    committed_frame_term: Option<Arc<dyn TermLike>>,
}

impl WriteGate {
    /// Create a gate with no terminal.
    ///
    /// Frames through such a gate are built and discarded, and
    /// [`Self::commit_frame`] is a no-op. Only
    /// [`ProgressTerminal::disabled`](super::terminal::ProgressTerminal::disabled)
    /// and the legacy `with_multi_progress` test seam hand one out.
    pub(crate) fn new_noop() -> Self {
        Self { committed_frame_term: None }
    }

    /// Create a gate whose frame writer draws through `term`.
    ///
    /// This is the real gate: `run_frame` writes its rendered frame through
    /// this handle (one `write_frame` pass), and [`Self::commit_frame`]'s
    /// CR+LF is what moves the cursor past the frame `finalize` wrote. The
    /// ungated test harnesses pair their own `InMemoryTerm` with it so the
    /// plan reaches the grid a test reads back.
    pub(crate) fn new_noop_with_term(term: Arc<dyn TermLike>) -> Self {
        Self { committed_frame_term: Some(term) }
    }

    /// The terminal this gate hands its frames to, when there is one.
    ///
    /// `None` only for a gate with no term ([`WriteGate::new_noop`]):
    /// `run_frame` writes its rendered frame through this handle (one
    /// `write_frame` pass) and skips the write when the gate has no term.
    pub(crate) fn term(&self) -> Option<&dyn TermLike> {
        self.committed_frame_term.as_deref()
    }

    /// Advance the terminal cursor past a frame that has just been committed.
    ///
    /// The commit contract (see the module docs) hands the frame over to the
    /// terminal permanently, but the writer leaves the cursor on the frame's
    /// last row. Writing one CR+LF moves the cursor to the row below the
    /// frame, so the next screen's band starts there instead of claiming the
    /// committed row. CR is not optional: an LF alone keeps the column on a
    /// terminal that does not translate it, which would start the next
    /// frame's first line in the middle of a row.
    ///
    /// # Caller contract
    ///
    /// Call this after the frame's final write. The only caller is
    /// `ProgressRenderer::finalize`, which issues it right after the forced
    /// final-frame pass.
    ///
    /// No-op on a gate with no term ([`WriteGate::new_noop`]). A failed
    /// write is not actionable — the frame stays committed either way, and
    /// the commit path has no error channel (`ProgressBar::tick` discards
    /// its own draw error for the same reason).
    pub(crate) fn commit_frame(&self) {
        let Some(term) = &self.committed_frame_term else { return };
        // Best effort by contract: the frame is committed whether or not the
        // terminal accepted the move, and there is no error channel to report
        // it through (see the method docs).
        let _ = term.write_str("\r\n");
    }
}
