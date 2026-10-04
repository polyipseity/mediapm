//! # Defense-in-depth
//!
//! Tests in this module are organized by layer:
//!
//! * **Recording** — [`RecordingProgressTracker`] tests verify the op-log
//!   produced by each method call (correct sequence of [`ProgressOp`]
//!   entries) and that each entry names the [`BarId`] it came from.
//! * **State-mutation** — [`ProgressBarHandle::new`] / [`ProgressBarHandle::with_label`]
//!   tests verify that underlying [`SharedState`] is updated correctly
//!   (positions, totals, status, elapsed).
//! * **Renderer integration** — [`ProgressScreen`] tests verify that the
//!   full tracking-to-terminal path produces correct visual output.
//!
//! Each layer covers the same behavioral surface through different
//! observation points, providing redundant coverage against regressions
//! even when the observation mechanism itself has a bug.

#[allow(unused_imports)]
pub use super::recording::{
    BarId, ProgressOp, RecordedProgressOp, RecordingProgressTracker, RecordingTrackedHandle,
};
#[allow(unused_imports)]
pub use super::{
    BarStyle, PrefixComponents, ProgressBarHandle, ProgressScreen, SuffixComponents, TrackStatus,
};
pub use std::sync::Arc;

mod ansi;
mod components;
mod recording;
mod renderer;
mod screen;
mod state;
mod terminal;
mod truncation_brackets;
mod truncation_ladder;

/// A throwaway term for a terminal's one-shot pre-roll.
///
/// Unit tests build a `ProgressTerminal` and create their screen from it. The
/// terminal builder leaves `pre_roll_term` at its `console::Term::stderr`
/// default, and pre-roll writes one blank line per terminal row, so without a
/// capture every `screen().build()` would push those newlines past libtest's
/// capture and straight to fd 2.  The capture only needs somewhere to write:
/// the tests read the (separate) term they passed as the draw target, never
/// this one.
pub(crate) fn pre_roll_capture() -> Box<dyn indicatif::TermLike> {
    Box::new(indicatif::InMemoryTerm::new(200, 80))
}
