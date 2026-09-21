//! # Defense-in-depth
//!
//! Tests in this module are organized by layer:
//!
//! * **Recording** — [`RecordingProgressTracker`] tests verify the op-log
//!   produced by each method call (correct sequence of [`ProgressOp`]
//!   entries).
//! * **State-mutation** — [`TrackedHandle::new`] / [`TrackedHandle::with_label`]
//!   tests verify that underlying [`SharedState`] is updated correctly
//!   (positions, totals, status, elapsed).
//! * **Renderer integration** — [`ProgressGroup`] tests verify that the
//!   full tracking-to-terminal path produces correct visual output.
//!
//! Each layer covers the same behavioral surface through different
//! observation points, providing redundant coverage against regressions
//! even when the observation mechanism itself has a bug.

#[allow(unused_imports)]
pub use super::recording::{ProgressOp, RecordingProgressTracker, RecordingTrackedHandle};
#[allow(unused_imports)]
pub use super::{
    BarStyle, PrefixComponents, ProgressGroup, SuffixComponents, TrackStatus, TrackedHandle,
};
pub use std::sync::Arc;

mod ansi;
mod components;
mod recording;
mod renderer;
mod state;
