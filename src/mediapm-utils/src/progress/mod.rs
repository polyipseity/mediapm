//! Shared progress bar and download-progress types for mediapm CLIs.
//!
//! Crate consumers that want graphical progress bars enable the `progress`
//! feature (which pulls in `indicatif`). The conductor library avoids this
//! dependency — it receives progress via [`ProgressCallback`] closures.

mod byte_budget;
mod download;
mod multi_item_budget;

pub use byte_budget::ByteBudget;
pub use download::{
    DownloadProgressSnapshot, ProgressCallback, ProviderPhase, ProviderProgressCallback,
    ProviderProgressSnapshot,
};
pub use multi_item_budget::MultiItemBudget;

#[cfg(feature = "progress")]
use std::sync::Arc;

// Graphical progress bar types (only with `progress` feature)

#[cfg(feature = "progress")]
mod inner;

#[cfg(feature = "progress")]
pub use inner::{
    DebugSlotState, DebugTickSnapshot, DimensionSource, HasOverall, NoOverall, PrefixComponents,
    ProgressDebugSink, ProgressGroup, ProgressGroupBuilder, ProgressRenderer, RealTerminalSource,
    RealTimeSource, StatusCount, SuffixComponents, TestDimensionSource, TestTimeSource, TimeSource,
    TrackSnapshot, TrackStatus, TrackedHandle,
};

#[cfg(feature = "progress")]
#[allow(unused_imports)]
pub(crate) use inner::{SharedState, format_elapsed, format_rate};

// Shared API traits for dependency injection (feature-gated)

#[cfg(feature = "progress")]
mod traits;
#[cfg(feature = "progress")]
pub use traits::{BarStyle, ProgressBarApi, ProgressGroupApi};

// Client-defined truncation contract (feature-gated)

#[cfg(feature = "progress")]
mod truncation;
#[cfg(feature = "progress")]
pub use truncation::BarLabelTruncation;

// Recording types for test assertions (feature-gated)

/// Recording progress operations for test assertions.
///
/// Provides [`RecordingProgressTracker`] and [`RecordingTrackedHandle`], which
/// record all operations into a shared log without any visual output.
#[cfg(feature = "progress")]
pub mod recording;
#[cfg(feature = "progress")]
pub use recording::{ProgressOp, RecordingProgressTracker, RecordingTrackedHandle};

// Trait impls for recording types

#[cfg(feature = "progress")]
impl ProgressBarApi for recording::RecordingTrackedHandle {
    fn advance(&self, delta: u64) {
        recording::RecordingTrackedHandle::advance(self, delta);
    }
    fn finish_success(&self) {
        recording::RecordingTrackedHandle::finish_success(self);
    }
    fn finish_error(&self) {
        recording::RecordingTrackedHandle::finish_error(self);
    }
    fn snapshot(&self) -> TrackSnapshot {
        TrackSnapshot {
            position: 0,
            total: self.total(),
            label: String::new(),
            prefix: String::new(),
            prefix_components: crate::progress::inner::PrefixComponents::default(),
            suffix: String::new(),
            suffix_components: crate::progress::inner::SuffixComponents::default(),
            status: TrackStatus::Active,
            elapsed: recording::RecordingTrackedHandle::snapshot_elapsed(self),
        }
    }
    fn is_finished(&self) -> bool {
        let ops = self.ops();
        ops.iter().any(|op| {
            matches!(
                op,
                recording::ProgressOp::FinishSuccess
                    | recording::ProgressOp::FinishError
                    | recording::ProgressOp::FinishWarning
                    | recording::ProgressOp::FinishAndClear
            )
        })
    }
    fn finish_warning(&self) {
        recording::RecordingTrackedHandle::finish_warning(self);
    }
    fn set_position(&self, pos: u64) {
        recording::RecordingTrackedHandle::set_position(self, pos);
    }
    fn set_total(&self, total: u64) {
        recording::RecordingTrackedHandle::set_total(self, total);
    }
    fn set_prefix_components(&self, components: PrefixComponents) {
        recording::RecordingTrackedHandle::set_prefix_components(self, components);
    }
    fn set_suffix_components(&self, components: SuffixComponents) {
        recording::RecordingTrackedHandle::set_suffix_components(self, components);
    }
    fn set_truncation(&self, truncation: Arc<dyn BarLabelTruncation>) {
        recording::RecordingTrackedHandle::set_truncation(self, &truncation);
    }
    fn set_style(&self, style: BarStyle) {
        recording::RecordingTrackedHandle::set_style(self, style);
    }
}

#[cfg(feature = "progress")]
impl ProgressGroupApi for recording::RecordingProgressTracker {
    fn add_bar(&self, total: u64, label: &str) -> Arc<dyn ProgressBarApi> {
        Arc::new(recording::RecordingProgressTracker::add_bar(self, total, label))
    }
    fn join(&self) {
        // Recording tracker has no display to block on.
    }
}

// Tests

#[cfg(test)]
#[cfg(feature = "progress")]
mod tests;
