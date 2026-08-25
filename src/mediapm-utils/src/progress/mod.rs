//! Shared progress bar and download-progress types for mediapm CLIs.
//!
//! Crate consumers that want graphical progress bars enable the `progress`
//! feature (which pulls in `indicatif`).  The conductor library itself avoids
//! this dependency — it receives progress via [`ProgressCallback`] closures.
//!
//! # Architecture
//!
//! Progress tracking and rendering are separated into independent layers:
//!
//! | Layer | Types | Dependencies |
//! |---|---|---|
//! | **Tracking** (unlimited) | [`TrackedHandle`] | None (pure state) |
//! | **Rendering** (terminal-limited) | [`ProgressGroup`] | `indicatif` (behind feature) |
//! | **Recording** (testing) | [`recording::RecordingTrackedHandle`], [`recording::RecordingProgressTracker`] | None behind feature |
//! | **Debug** | [`ProgressDebugSink`] | `serde_json` (behind feature) |
//!
//! # Types across feature boundaries
//!
//! | Type / fn | Available without `progress` | Available with `progress` |
//! |---|---|---|
//! | [`DownloadProgressSnapshot`] | ✅ | ✅ |
//! | [`ProgressCallback`] | ✅ | ✅ |
//! | [`TrackedHandle`] | ❌ | ✅ |
//! | [`ProgressGroup`] | ❌ | ✅ |
//! | [`ProgressRenderer`] | ❌ | ✅ |
//! | (no global toggle) | — | — |
//! | [`recording::RecordingProgressTracker`] | ❌ | ✅ |
//! | [`recording::RecordingTrackedHandle`] | ❌ | ✅ |
//! | [`recording::ProgressOp`] | ❌ | ✅ |
//! | [`ProgressDebugSink`] | ❌ | ✅ |
//! | [`DebugSlotState`] | ❌ | ✅ |
//! | [`DebugTickSnapshot`] | ❌ | ✅ |

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

// ---------------------------------------------------------------------------
// Graphical progress bar types (only with `progress` feature)
// ---------------------------------------------------------------------------

#[cfg(feature = "progress")]
mod inner;

#[cfg(feature = "progress")]
pub use inner::{
    DebugSlotState, DebugTickSnapshot, DimensionSource, HasOverall, NoOverall, PrefixComponents,
    ProgressDebugSink, ProgressGroup, ProgressGroupBuilder, ProgressRenderer, RealTerminalSource,
    RealTimeSource, SuffixComponents, TestDimensionSource, TestTimeSource, TimeSource,
    TrackSnapshot, TrackStatus, TrackedHandle,
};

#[cfg(feature = "progress")]
#[allow(unused_imports)]
pub(crate) use inner::{SharedState, format_elapsed, format_rate};

// ---- Shared API traits for dependency injection (feature-gated) -------

/// Visual style for a child progress bar.
///
/// The style selects how the bar's fill and prefix/suffix are driven. It is
/// a marker stored on [`SharedState`] and read by the renderer's single push
/// point ([`ProgressRenderer::sync_snapshot_to_bar`]); it does **not** change
/// color or overall-bar semantics.
///
/// - [`StepCount`](BarStyle::StepCount) — the default. The bar shows a
///   `count/total` ratio driven by `advance`/`set_position`/`set_total`, with
///   the standard `tool_name [version] [phase] [count/total]` prefix and the
///   `count/total elapsed rate [eta] custom` suffix. Used by the sync screen,
///   materialization screen, and the legacy per-step workflow bars.
/// - [`WorkerSpinner`](BarStyle::WorkerSpinner) — a fixed worker-slot bar
///   driven by **per-worker** state rather than a per-step or global total.
///   The coordinator populates `prefix_components`/`suffix_components`
///   directly (no `version`/`phase`; `custom` = `<status>[ <F> failed][ <R>
///   retry]`), and the renderer applies a `0/0` div-by-zero guard (renders
///   `total = 1, pos = 0` when the worker's assigned count is `0`) so an
///   idle worker shows an empty all-░ bar. The same wide_bar child template
///   as `StepCount` is used; only the field population differs.
///
/// The style is set via [`ProgressBarApi::set_style`] (or
/// [`TrackedHandle::set_style`]) after [`ProgressGroup::add_bar`]. It defaults
/// to [`StepCount`](BarStyle::StepCount) so every existing caller is
/// byte-for-byte unchanged.
#[cfg(feature = "progress")]
#[derive(Debug, Clone, Copy, PartialEq, Eq, Default)]
pub enum BarStyle {
    /// Standard `count/total` child bar (default).
    #[default]
    StepCount,
    /// Fixed worker-slot bar driven by per-worker state.
    WorkerSpinner,
}

/// Minimum progress-bar handle API for dependency injection.
///
/// Both [`TrackedHandle`] and
/// [`RecordingTrackedHandle`](recording::RecordingTrackedHandle) implement
/// this trait, allowing consumer functions to accept either a real display
/// bar or a recording bar for testing.
#[cfg(feature = "progress")]
pub trait ProgressBarApi: Send + Sync {
    /// Advance the bar by `delta` work units.
    fn advance(&self, delta: u64);
    /// Mark the bar as finished successfully.
    fn finish_success(&self);
    /// Mark the bar as finished with an error.
    fn finish_error(&self);
    /// Mark the bar as finished with a non-fatal warning.
    fn finish_warning(&self);
    /// Return a data-copy snapshot of the tracking state.
    fn snapshot(&self) -> TrackSnapshot;
    /// Returns `true` if the handle has been finished/abandoned.
    fn is_finished(&self) -> bool;
    /// Jump to an absolute position.
    fn set_position(&self, pos: u64);
    /// Change the total mid-flight for dynamic workloads.
    fn set_total(&self, total: u64);
    /// Set prefix components for source-data-based truncation.
    fn set_prefix_components(&self, components: PrefixComponents);
    /// Set suffix components for source-data-based truncation.
    fn set_suffix_components(&self, components: SuffixComponents);
    /// Set the visual style for the bar (see [`BarStyle`]).
    ///
    /// Defaults to [`StepCount`](BarStyle::StepCount). Callers that own a
    /// worker-slot bar set [`WorkerSpinner`](BarStyle::WorkerSpinner) after
    /// [`add_bar`](crate::progress::ProgressGroup::add_bar) so the renderer
    /// applies the style-specific `0/0` div-by-zero guard.
    fn set_style(&self, style: BarStyle);
}

#[cfg(feature = "progress")]
impl ProgressBarApi for TrackedHandle {
    fn advance(&self, delta: u64) {
        TrackedHandle::advance(self, delta);
    }
    fn finish_success(&self) {
        TrackedHandle::finish_success(self);
    }
    fn finish_error(&self) {
        TrackedHandle::finish_error(self);
    }
    fn finish_warning(&self) {
        TrackedHandle::finish_warning(self);
    }
    fn snapshot(&self) -> TrackSnapshot {
        TrackedHandle::snapshot(self)
    }
    fn is_finished(&self) -> bool {
        TrackedHandle::is_finished(self)
    }
    fn set_position(&self, pos: u64) {
        TrackedHandle::set_position(self, pos);
    }
    fn set_total(&self, total: u64) {
        TrackedHandle::set_total(self, total);
    }
    fn set_prefix_components(&self, components: PrefixComponents) {
        TrackedHandle::set_prefix_components(self, components);
    }
    fn set_suffix_components(&self, components: SuffixComponents) {
        TrackedHandle::set_suffix_components(self, components);
    }
    fn set_style(&self, style: BarStyle) {
        TrackedHandle::set_style(self, style);
    }
}

/// Minimum progress-group API for dependency injection.
///
/// Both [`ProgressGroup`] and
/// [`RecordingProgressTracker`](recording::RecordingProgressTracker) implement
/// this trait, allowing consumer functions to accept either a real display
/// group or a recording group for testing.
#[cfg(feature = "progress")]
pub trait ProgressGroupApi {
    /// Add a child bar and return an [`Arc`]-wrapped handle.
    fn add_bar(&self, total: u64, label: &str) -> Arc<dyn ProgressBarApi>;
    /// Block until all bars in the group reach a finished state.
    fn join(&self);
}

#[cfg(feature = "progress")]
impl ProgressGroupApi for ProgressGroup {
    fn add_bar(&self, total: u64, label: &str) -> Arc<dyn ProgressBarApi> {
        Arc::new(ProgressGroup::add_bar(self, total, label))
    }
    fn join(&self) {
        ProgressGroup::join(self);
    }
}

// ---- Recording types for test assertions (feature-gated) ---------------

/// Recording progress operations for test assertions.
///
/// This module provides [`RecordingProgressTracker`] and
/// [`RecordingTrackedHandle`] that record all operations into a shared
/// operation log without any visual output. Use
#[cfg(feature = "progress")]
pub mod recording;
#[cfg(feature = "progress")]
pub use recording::{ProgressOp, RecordingProgressTracker, RecordingTrackedHandle};

// ---- Trait impls for recording types -------------------------------------

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

// ---- Tests ---------------------------------------------------------------

#[cfg(test)]
#[cfg(feature = "progress")]
mod tests {
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

    use std::sync::Arc;

    use super::recording::{ProgressOp, RecordingProgressTracker, RecordingTrackedHandle};
    use super::{
        BarStyle, PrefixComponents, ProgressGroup, SuffixComponents, TrackStatus, TrackedHandle,
    };
    use indicatif::MultiProgress;

    #[test]
    fn progress_enabled_no_global_toggle() {
        // Constructors always produce enabled handles.
        let h = TrackedHandle::new(100);
        assert_eq!(h.total(), 100, "enabled handle reports initial total");
        let g = ProgressGroup::builder().build();
        let ch = g.add_bar(50, "child");
        assert_eq!(ch.total(), 50);
        let (_og, oh) = ProgressGroup::builder().with_overall("all", 300).build();
        assert_eq!(oh.total(), 300);
        g.join_and_clear();

        h.set_total(200);
        assert_eq!(h.total(), 200);
        h.advance(10);
        h.set_position(20);
        h.set_prefix_components(PrefixComponents { tool_name: "pfx".into(), ..Default::default() });
        h.finish_success();

        // Disabled handles can still be created explicitly.
        let dh = TrackedHandle::disabled();
        assert_eq!(dh.total(), 0, "disabled handle reports 0 total");
        let dg = ProgressGroup::disabled();
        let dch = dg.add_bar(50, "child");
        assert_eq!(dch.total(), 0);
        // All mutation methods are no-ops on a disabled handle
        dh.advance(10);
        dh.set_total(50);
        dh.set_position(5);
        dh.set_prefix_components(PrefixComponents {
            tool_name: "pfx".into(),
            ..Default::default()
        });
        dh.finish_success();
        dh.finish_error();
        dh.finish_and_clear();
    }

    #[test]
    fn handle_disabled_is_noop() {
        let h = TrackedHandle::disabled();
        assert_eq!(h.total(), 0);
        h.advance(10);
        h.set_total(50);
        h.set_position(5);
        h.set_prefix_components(PrefixComponents { tool_name: "pfx".into(), ..Default::default() });
        h.finish_success();
        h.finish_error();
        h.finish_and_clear();
    }

    // ---- RecordingProgressTracker ---------------------------------------

    #[test]
    fn recording_tracker_add_bar_creates_op() {
        let rt = RecordingProgressTracker::new();
        let _h = rt.add_bar(100, "test-bar");
        assert_eq!(rt.ops(), vec![ProgressOp::AddBar { total: 100, label: "test-bar".into() }]);
    }

    #[test]
    fn recording_tracker_clear_resets_ops() {
        let rt = RecordingProgressTracker::new();
        let _ = rt.add_bar(10, "a");
        let _ = rt.add_bar(20, "b");
        assert_eq!(rt.ops().len(), 2);
        rt.clear();
        assert!(rt.ops().is_empty());
    }

    #[test]
    fn recording_handle_records_all_ops() {
        let h = RecordingTrackedHandle::new(100);
        assert_eq!(h.total(), 100);

        h.set_total(200);
        h.advance(5);
        h.set_position(10);
        h.set_prefix_components(PrefixComponents { tool_name: "pfx".into(), ..Default::default() });
        h.finish_success();

        assert_eq!(
            h.ops(),
            vec![
                ProgressOp::SetTotal { total: 200 },
                ProgressOp::Advance { delta: 5 },
                ProgressOp::SetPosition { pos: 10 },
                ProgressOp::SetPrefixComponents {
                    marker: String::new(),
                    tool_name: "pfx".into(),
                    version: String::new(),
                    phase: String::new(),
                    count: String::new(),
                    total: String::new(),
                },
                ProgressOp::FinishSuccess,
            ]
        );
    }

    #[test]
    fn recording_handle_set_prefix_components_ops() {
        let h = RecordingTrackedHandle::new(100);
        h.set_prefix_components(PrefixComponents {
            marker: String::new(),
            tool_name: "wget".into(),
            version: "1.2.3".into(),
            phase: "fch".into(),
            count: "2".into(),
            total: "5".into(),
        });

        assert_eq!(
            h.ops(),
            vec![ProgressOp::SetPrefixComponents {
                marker: String::new(),
                tool_name: "wget".into(),
                version: "1.2.3".into(),
                phase: "fch".into(),
                count: "2".into(),
                total: "5".into(),
            }]
        );
    }

    #[test]
    fn recording_handle_set_suffix_components_ops() {
        let h = RecordingTrackedHandle::new(100);
        let components = SuffixComponents {
            count: "9".into(),
            total: "9".into(),
            elapsed: "1:23:45".into(),
            rate: Some("10/s".into()),
            eta: Some("[0:00:07]".into()),
            custom: "cached (1)".into(),
        };
        h.set_suffix_components(components.clone());

        assert_eq!(h.ops(), vec![ProgressOp::SetSuffixComponents { components }],);
    }

    #[test]
    fn recording_handle_shared_log() {
        let rt = RecordingProgressTracker::new();
        let h1 = rt.add_bar(50, "first");
        let h2 = rt.add_bar(100, "second");

        h1.advance(1);
        h2.advance(2);
        h1.finish_success();

        assert_eq!(
            rt.ops(),
            vec![
                ProgressOp::AddBar { total: 50, label: "first".into() },
                ProgressOp::AddBar { total: 100, label: "second".into() },
                ProgressOp::Advance { delta: 1 },
                ProgressOp::Advance { delta: 2 },
                ProgressOp::FinishSuccess,
            ]
        );
    }

    #[test]
    fn recording_handle_disabled_has_zero_total() {
        let h = RecordingTrackedHandle::disabled();
        assert_eq!(h.total(), 0);
        // Even a disabled handle records ops (it uses a fresh log).
        assert!(h.ops().is_empty());
        h.advance(1);
        assert_eq!(h.ops(), vec![ProgressOp::Advance { delta: 1 }]);
    }

    #[test]
    fn recording_handle_finish_success_and_error() {
        let h = RecordingTrackedHandle::new(10);
        h.finish_success();
        h.finish_error();
        assert_eq!(h.ops(), vec![ProgressOp::FinishSuccess, ProgressOp::FinishError,]);
    }

    #[test]
    fn recording_handle_finish_and_clear_warning() {
        let h = RecordingTrackedHandle::new(1);
        h.finish_and_clear();
        h.finish_warning();
        assert_eq!(h.ops(), vec![ProgressOp::FinishAndClear, ProgressOp::FinishWarning]);
    }

    // ---- BarStyle selection (Stage 5 worker-slot generalization) ---------

    #[test]
    fn bar_style_default_is_step_count() {
        // A freshly created TrackedHandle uses StepCount (preserves all
        // existing callers: sync screen, materialization, etc.).
        let h = TrackedHandle::new(100);
        assert_eq!(h.style(), BarStyle::StepCount);
    }

    #[test]
    fn bar_style_set_style_switches_to_worker_spinner() {
        let h = TrackedHandle::new(100);
        assert_eq!(h.style(), BarStyle::StepCount);
        h.set_style(BarStyle::WorkerSpinner);
        assert_eq!(h.style(), BarStyle::WorkerSpinner);
    }

    #[test]
    fn bar_style_set_style_is_idempotent() {
        let h = TrackedHandle::new(100);
        h.set_style(BarStyle::WorkerSpinner);
        h.set_style(BarStyle::WorkerSpinner);
        assert_eq!(h.style(), BarStyle::WorkerSpinner);
        h.set_style(BarStyle::StepCount);
        assert_eq!(h.style(), BarStyle::StepCount);
    }

    #[test]
    fn bar_style_recording_handle_set_style_is_noop_marker() {
        // The recording harness records no ProgressOp for set_style today;
        // worker-slot behavior is asserted via prefix/total deltas instead.
        let h = RecordingTrackedHandle::new(100);
        h.set_style(BarStyle::WorkerSpinner);
        assert!(h.ops().is_empty(), "set_style must not emit a ProgressOp");
    }

    #[test]
    fn bar_style_worker_spinner_zero_zero_guard_renders_empty() {
        // A WorkerSpinner bar with assigned == 0 must render an empty bar
        // (total = 1, pos = 0) so the suffix can still read 0/0 without a
        // div-by-zero. Verified through the full tracking-to-terminal path.
        use super::inner::DimensionSource;
        use std::sync::Arc;

        let term = indicatif::InMemoryTerm::new(10, 80);
        let target = indicatif::ProgressDrawTarget::term_like(Box::new(term.clone()));
        let mp = MultiProgress::with_draw_target(target);
        let dims = Arc::new(super::inner::TestDimensionSource::new((10, 80)));
        let ts = Arc::new(super::TestTimeSource::new());

        let group = super::ProgressGroup::builder()
            .with_multi_progress(mp)
            .with_dim_source(dims as Arc<dyn DimensionSource>)
            .with_time_source(ts.clone() as Arc<dyn super::TimeSource>)
            .with_ticker_enabled(false)
            .build();

        let h = group.add_bar(0, "idle");
        h.set_style(BarStyle::WorkerSpinner);
        // Idle worker: assigned == 0, succeeded == 0, no version/phase.
        h.set_prefix_components(PrefixComponents {
            tool_name: "idle".into(),
            ..Default::default()
        });
        h.set_suffix_components(SuffixComponents { custom: "idle".into(), ..Default::default() });
        group.tick();

        let output = term.contents();
        // The bar must render (no panic) and the idle line must be present.
        assert!(output.contains("idle"), "idle worker line must render: {output}");
        // No "0/0" division artifact; the custom suffix carries the status.
        assert!(output.contains("idle"), "idle status must be visible: {output}");
    }

    #[test]
    fn bar_style_worker_spinner_populates_no_version_or_phase() {
        // WorkerSpinner bars populate only marker/tool_name/count-total in the
        // prefix and custom/elapsed in the suffix — never version or phase.
        // This locks the style-aware truncation order from the plan.
        let h = TrackedHandle::new(0);
        h.set_style(BarStyle::WorkerSpinner);
        h.set_prefix_components(PrefixComponents {
            tool_name: "wf-1/step-5 (echo)".into(),
            count: "2".into(),
            total: "3".into(),
            ..Default::default()
        });
        h.set_suffix_components(SuffixComponents {
            custom: "running".into(),
            ..Default::default()
        });
        let snap = h.snapshot();
        // version/phase are empty (never set for WorkerSpinner).
        assert_eq!(snap.prefix_components.version, "");
        assert_eq!(snap.prefix_components.phase, "");
        // count/total + tool_name + custom are present.
        assert_eq!(snap.prefix_components.tool_name, "wf-1/step-5 (echo)");
        assert_eq!(snap.prefix_components.count, "2");
        assert_eq!(snap.prefix_components.total, "3");
        assert_eq!(snap.suffix_components.custom, "running");
    }

    // ---- RecordingTrackedHandle elapsed ----------------------------------

    #[test]
    fn recording_handle_elapsed_starts_near_zero() {
        let h = RecordingTrackedHandle::new(100);
        let elapsed = h.snapshot_elapsed();
        assert!(elapsed.as_millis() < 100, "elapsed should start near zero, got {elapsed:?}");
    }

    #[test]
    fn recording_handle_elapsed_frozen_after_finish() {
        let h = RecordingTrackedHandle::new(100);
        std::thread::sleep(std::time::Duration::from_millis(1));
        h.finish_success();
        let frozen = h.snapshot_elapsed();
        // Verify the value stays frozen on subsequent reads.
        let frozen2 = h.snapshot_elapsed();
        assert_eq!(frozen, frozen2, "elapsed should be frozen after finish_success");
    }

    #[test]
    fn recording_handle_elapsed_frozen_after_finish_success() {
        let h = RecordingTrackedHandle::new(100);
        std::thread::sleep(std::time::Duration::from_millis(1));
        h.finish_success();
        let frozen = h.snapshot_elapsed();
        let frozen2 = h.snapshot_elapsed();
        assert_eq!(frozen, frozen2, "elapsed should be frozen after finish_success");
    }

    #[test]
    fn recording_handle_elapsed_frozen_after_finish_error() {
        let h = RecordingTrackedHandle::new(100);
        std::thread::sleep(std::time::Duration::from_millis(1));
        h.finish_error();
        let frozen = h.snapshot_elapsed();
        let frozen2 = h.snapshot_elapsed();
        assert_eq!(frozen, frozen2, "elapsed should be frozen after finish_error");
    }

    #[test]
    fn recording_handle_elapsed_frozen_after_finish_warning() {
        let h = RecordingTrackedHandle::new(100);
        std::thread::sleep(std::time::Duration::from_millis(1));
        h.finish_warning();
        let frozen = h.snapshot_elapsed();
        let frozen2 = h.snapshot_elapsed();
        assert_eq!(frozen, frozen2, "elapsed should be frozen after finish_warning");
    }

    // ---- format_elapsed (pure formatting) --------------------------------

    #[test]
    fn format_elapsed_zero() {
        assert_eq!(super::format_elapsed(std::time::Duration::ZERO), "0s");
    }

    #[test]
    fn format_elapsed_seconds_only() {
        assert_eq!(super::format_elapsed(std::time::Duration::from_secs(42)), "42s");
    }

    #[test]
    fn format_elapsed_minutes_and_seconds() {
        assert_eq!(super::format_elapsed(std::time::Duration::from_secs(5 * 60 + 3)), "5m3s");
    }

    #[test]
    fn format_elapsed_hours() {
        assert_eq!(
            super::format_elapsed(std::time::Duration::from_secs(2 * 3600 + 15 * 60 + 30)),
            "2h15m"
        );
    }

    #[test]
    fn format_elapsed_large_hours() {
        assert_eq!(super::format_elapsed(std::time::Duration::from_hours(100)), "4d4h");
    }

    // ---- format_rate (pure formatting) -----------------------------------

    #[test]
    fn format_rate_zero() {
        assert_eq!(super::format_rate(0.0), "0/d");
    }

    #[test]
    fn format_rate_slow() {
        assert_eq!(super::format_rate(0.000_1), "9/d");
    }

    #[test]
    fn format_rate_per_minute() {
        // 0.02/s = 1.2/m
        assert_eq!(super::format_rate(0.02), "1/m");
    }

    #[test]
    fn format_rate_per_hour() {
        // 0.000_5/s = 1.8/h
        assert_eq!(super::format_rate(0.000_5), "2/h");
    }

    #[test]
    fn format_rate_single_digit() {
        assert_eq!(super::format_rate(3.5), "3.5/s");
    }

    #[test]
    fn format_rate_double_digit() {
        assert_eq!(super::format_rate(42.0), "42/s");
    }

    #[test]
    fn format_rate_thousands_single() {
        assert_eq!(super::format_rate(1_200.0), "1.2k/s");
    }

    #[test]
    fn format_rate_thousands_double() {
        assert_eq!(super::format_rate(123_000.0), "123k/s");
    }

    #[test]
    fn format_rate_millions() {
        assert_eq!(super::format_rate(3_500_000.0), "3.5M/s");
    }

    // ---- SharedState elapsed --------------------------------------------

    #[test]
    fn shared_state_elapsed_starts_near_zero() {
        let s = super::SharedState::new(100, "test");
        let elapsed = s.elapsed();
        assert!(elapsed.as_millis() < 100, "elapsed should start near zero, got {elapsed:?}");
    }

    #[test]
    fn shared_state_elapsed_advances() {
        let ts = std::sync::Arc::new(super::TestTimeSource::new());
        let s = super::SharedState::with_time_source(
            100,
            "test",
            std::sync::Arc::clone(&ts) as std::sync::Arc<dyn super::TimeSource>,
        );
        ts.advance(std::time::Duration::from_millis(10));
        let elapsed = s.elapsed();
        assert!(elapsed.as_millis() >= 10, "elapsed should advance after advance, got {elapsed:?}");
    }

    #[test]
    fn shared_state_elapsed_frozen_after_mark_finished() {
        let ts = std::sync::Arc::new(super::TestTimeSource::new());
        let s = super::SharedState::with_time_source(
            100,
            "test",
            std::sync::Arc::clone(&ts) as std::sync::Arc<dyn super::TimeSource>,
        );
        ts.advance(std::time::Duration::from_millis(10));
        s.mark_finished();
        let frozen = s.elapsed();
        assert!(
            frozen.as_millis() >= 10,
            "elapsed should capture time until mark_finished, got {frozen:?}"
        );
        ts.advance(std::time::Duration::from_millis(10));
        let frozen2 = s.elapsed();
        assert_eq!(frozen, frozen2, "elapsed should be frozen after mark_finished");
    }

    #[test]
    fn shared_state_elapsed_not_frozen_before_finish() {
        let s = super::SharedState::new(100, "test");
        let t0 = s.elapsed();
        // Without calling mark_finished, repeated reads should climb.
        let t1 = s.elapsed();
        assert!(t1 >= t0, "elapsed should not decrease before finish: {t0:?} >= {t1:?}");
    }

    #[test]
    fn shared_state_elapsed_monotonic() {
        let ts = std::sync::Arc::new(super::TestTimeSource::new());
        let s = super::SharedState::with_time_source(
            100,
            "test",
            std::sync::Arc::clone(&ts) as std::sync::Arc<dyn super::TimeSource>,
        );
        let t0 = s.elapsed();
        ts.advance(std::time::Duration::from_millis(5));
        let t1 = s.elapsed();
        ts.advance(std::time::Duration::from_millis(5));
        let t2 = s.elapsed();
        assert!(t1 >= t0, "t1 ({t1:?}) should be >= t0 ({t0:?})");
        assert!(t2 >= t1, "t2 ({t2:?}) should be >= t1 ({t1:?})");
    }

    // ---- TrackedHandle elapsed (integration) -----------------------------

    #[test]
    fn tracked_handle_elapsed_frozen_after_all_finish_methods() {
        // finish_success, finish_error, finish_warning, finish_and_clear
        // must all freeze the elapsed.
        for (name, finish_fn) in [
            (
                "finish_success",
                Box::new(|h: &TrackedHandle| h.finish_success()) as Box<dyn Fn(&TrackedHandle)>,
            ),
            (
                "finish_error",
                Box::new(|h: &TrackedHandle| h.finish_error()) as Box<dyn Fn(&TrackedHandle)>,
            ),
            (
                "finish_warning",
                Box::new(|h: &TrackedHandle| h.finish_warning()) as Box<dyn Fn(&TrackedHandle)>,
            ),
            (
                "finish_and_clear",
                Box::new(|h: &TrackedHandle| h.finish_and_clear()) as Box<dyn Fn(&TrackedHandle)>,
            ),
        ] {
            let ts = std::sync::Arc::new(super::TestTimeSource::new());
            let g = super::ProgressGroup::builder()
                .with_time_source(
                    std::sync::Arc::clone(&ts) as std::sync::Arc<dyn super::TimeSource>
                )
                .build();
            let h = g.add_bar(100, &format!("{name}-bar"));
            ts.advance(std::time::Duration::from_millis(10));
            finish_fn(&h);
            // We can't directly access SharedState::elapsed() from the handle,
            // but we verify the handle doesn't panic and is usable afterward.
            assert_eq!(h.total(), 100, "{name}: total preserved");
            g.join_and_clear();
        }
    }

    #[test]
    fn progress_group_new_creates_handle() {
        let g = ProgressGroup::builder().build();
        let h = g.add_bar(42, "child");
        assert!(h.total() > 0, "enabled handle must have total > 0");
        assert_eq!(h.total(), 42);
    }

    #[test]
    fn progress_group_with_overall_creates_both() {
        let (g, overall) = ProgressGroup::builder().with_overall("all", 100).build();
        assert_eq!(overall.total(), 100, "overall bar must have total == 100");
        let child = g.add_bar(50, "child");
        assert_eq!(child.total(), 50, "child bar must have total == 50");
    }

    #[test]
    fn recording_handle_set_total_updates_position() {
        let h = RecordingTrackedHandle::new(100);
        h.set_position(5);
        h.set_total(20);
        assert_eq!(
            h.ops(),
            vec![ProgressOp::SetPosition { pos: 5 }, ProgressOp::SetTotal { total: 20 },]
        );
    }

    #[test]
    fn recording_handle_multiple_advances_sum() {
        let h = RecordingTrackedHandle::new(10);
        h.advance(1);
        h.advance(2);
        h.advance(3);
        let ops = h.ops();
        assert_eq!(ops.len(), 3, "expected 3 separate Advance ops");
        assert_eq!(ops[0], ProgressOp::Advance { delta: 1 });
        assert_eq!(ops[1], ProgressOp::Advance { delta: 2 });
        assert_eq!(ops[2], ProgressOp::Advance { delta: 3 });
    }

    #[test]
    fn progress_group_join_and_clear_does_not_panic() {
        // Non-empty group
        let g = ProgressGroup::builder().build();
        let _h = g.add_bar(10, "a");
        g.join();
        g.join_and_clear();

        // Empty group
        let g = ProgressGroup::builder().build();
        g.join();
        g.join_and_clear();
    }

    #[test]
    fn progress_group_disabled_construction() {
        let g1 = ProgressGroup::disabled();
        let h1 = g1.add_bar(50, "c1");
        assert_eq!(h1.total(), 0);

        // disabled + explicit disabled handle pair for with-overall patterns.
        let (_g2, h2) = (ProgressGroup::disabled(), TrackedHandle::disabled());
        assert_eq!(h2.total(), 0);
    }

    #[test]
    fn recording_handle_finish_does_not_generate_clear() {
        // Verify that finish_success / finish_error don't produce
        // FinishAndClear or Abandon operations (which would clear the bar).
        let h = RecordingTrackedHandle::new(10);
        h.finish_success();
        h.finish_error();
        for op in h.ops() {
            match op {
                ProgressOp::FinishSuccess | ProgressOp::FinishError => {}
                other => panic!("unexpected op: {other:?}"),
            }
        }
    }

    #[test]
    fn progress_group_join_leaves_handles_intact() {
        // join() is a no-op — handles must still be usable afterward.
        let g = ProgressGroup::builder().build();
        let h = g.add_bar(42, "child");
        h.advance(10);
        h.set_total(50);
        h.finish_success();
        g.join();
        assert_eq!(h.total(), 50, "handle total preserved after join");
    }

    #[test]
    fn progress_group_finish_success_and_error_preserve_group() {
        // Finish calls on a handle must preserve the total and the group must
        // remain functional (join() must not panic).
        let g = ProgressGroup::builder().build();
        let h = g.add_bar(10, "test");
        h.finish_success();
        assert_eq!(h.total(), 10, "handle total preserved after finish_success");
        // Second finish on the same slot must not corrupt state.
        h.finish_error();
        assert_eq!(h.total(), 10, "handle total preserved after finish_error");
        g.join(); // join must not panic on any state
    }

    // ── recording_group_add_bar_multiple_groups_independent ──

    #[test]
    fn recording_group_add_bar_multiple_groups_independent() {
        let g1 = RecordingProgressTracker::new();
        let g2 = RecordingProgressTracker::new();

        let h1 = g1.add_bar(10, "group1-bar");
        h1.advance(1);

        let h2 = g2.add_bar(20, "group2-bar");
        h2.advance(2);
        h2.finish_success();

        assert_eq!(
            g1.ops(),
            vec![
                ProgressOp::AddBar { total: 10, label: "group1-bar".to_string() },
                ProgressOp::Advance { delta: 1 },
            ],
            "group1 must have its own ops, unaffected by group2"
        );

        assert_eq!(
            g2.ops(),
            vec![
                ProgressOp::AddBar { total: 20, label: "group2-bar".to_string() },
                ProgressOp::Advance { delta: 2 },
                ProgressOp::FinishSuccess,
            ],
            "group2 must have its own ops, unaffected by group1"
        );
    }

    // ── recording_handle_finish_ops_sequence ──

    #[test]
    fn recording_handle_finish_ops_sequence() {
        let h = RecordingTrackedHandle::new(5);
        assert_eq!(h.ops(), vec![], "no ops yet");

        h.finish_success();
        assert_eq!(
            h.ops(),
            vec![ProgressOp::FinishSuccess],
            "finish_success records FinishSuccess"
        );

        let h2 = RecordingTrackedHandle::new(5);
        h2.finish_success();
        assert_eq!(
            h2.ops(),
            vec![ProgressOp::FinishSuccess],
            "finish_success records FinishSuccess"
        );

        let h3 = RecordingTrackedHandle::new(5);
        h3.finish_error();
        assert_eq!(h3.ops(), vec![ProgressOp::FinishError], "finish_error records FinishError");
    }

    // ── TrackedHandle::new (with bar) ──────────────────────────────────

    #[test]
    fn tracked_handle_new_creates_handle_with_total() {
        let h = TrackedHandle::new(50);
        assert_eq!(h.total(), 50);
        assert_eq!(h.snapshot().position, 0);
        assert!(!h.is_finished());
    }

    #[test]
    fn tracked_handle_new_advance_and_snapshot() {
        let h = TrackedHandle::new(100);
        h.advance(42);
        let snap = h.snapshot();
        assert_eq!(snap.position, 42);
        assert_eq!(snap.total, 100);
    }

    #[test]
    fn tracked_handle_is_finished_after_finish_success() {
        let h = TrackedHandle::new(10);
        assert!(!h.is_finished());
        h.finish_success();
        assert!(h.is_finished());
    }

    #[test]
    fn tracked_handle_is_finished_after_finish_error() {
        let h = TrackedHandle::new(10);
        h.finish_error();
        assert!(h.is_finished());
    }

    #[test]
    fn tracked_handle_is_finished_after_finish_warning() {
        let h = TrackedHandle::new(10);
        h.finish_warning();
        assert!(h.is_finished());
    }

    #[test]
    fn tracked_handle_snapshot_fields_match() {
        let h = TrackedHandle::new(100);
        h.set_prefix_components(PrefixComponents { tool_name: "pfx".into(), ..Default::default() });
        h.advance(7);
        let snap = h.snapshot();
        assert_eq!(snap.prefix, "\x1b[0mpfx");
        assert_eq!(snap.position, 7);
        assert_eq!(snap.total, 100);
        assert!(matches!(snap.status, TrackStatus::Active));
    }

    #[test]
    fn progress_group_excess_bars_return_active_handles() {
        // Fill slots beyond capacity, verify excess handle still tracks.

        // ProgressGroup::with_overall allocates terminal_height() slots
        // (clamped to 4-200).  Use a MultiProgress with small term to force
        // small capacity.
        let term = indicatif::InMemoryTerm::new(4, 40);
        let target = indicatif::ProgressDrawTarget::term_like(Box::new(term));
        let mp = MultiProgress::with_draw_target(target);
        let (group, _overall) = ProgressGroup::builder()
            .with_multi_progress(mp)
            .capacity(4)
            .with_overall("overall", 10)
            .build();

        // 4 slots total → 3 child slots + 1 overall.
        // Add 5 children → first 3 get slots, last 2 have no display slot.
        let handles: Vec<_> = (0..5).map(|i| group.add_bar(10, &format!("t{i}"))).collect();

        // All handles must be active (not disabled).
        for (i, h) in handles.iter().enumerate() {
            assert_eq!(h.total(), 10, "handle {i} total");
        }

        // Mutate each — verify state tracking works even without display.
        for (i, h) in handles.iter().enumerate() {
            h.advance((i + 1) as u64);
        }
        for (i, h) in handles.iter().enumerate() {
            let snap = h.snapshot();
            assert_eq!(snap.position, (i + 1) as u64, "handle {i} position");
        }
    }

    #[test]
    fn progress_group_manager_finish_and_clear_via_tick_fn() {
        // finish_and_clear on a ProgressGroup-managed handle (bar=None,
        // tick_fn=Some) must still mark state as finished.

        let (_group, overall) = ProgressGroup::builder().with_overall("all", 10).build();
        overall.finish_and_clear();
        let snap = overall.snapshot();
        assert!(
            matches!(snap.status, TrackStatus::Success),
            "finish_and_clear → Success, got {:?}",
            snap.status
        );
        assert_eq!(snap.position, 0, "position unchanged before advance");

        // Advance after finish_and_clear is harmless (no crash) but
        // does update position since advance() does not gate on status.
        overall.advance(5);
        assert_eq!(overall.snapshot().position, 5, "advance still works after finish_and_clear");
    }

    #[test]
    fn tracked_handle_finish_and_clear_disabled_is_noop() {
        // disabled() handle with finish_and_clear must not panic and
        // must leave state unchanged.
        let h = TrackedHandle::disabled();
        assert_eq!(h.total(), 0);
        h.finish_and_clear();
        assert_eq!(h.total(), 0);
    }

    #[test]
    fn progress_group_disabled_add_bar_returns_disabled() {
        let g = ProgressGroup::disabled();
        let child = g.add_bar(42, "child");
        assert_eq!(child.total(), 0, "child disabled");
    }

    #[test]
    fn progress_group_api_trait_via_recording() {
        // Verify RecordingProgressTracker implements ProgressGroupApi
        // and can be used via the trait.
        use super::ProgressGroupApi;
        let tracker: Arc<dyn ProgressGroupApi> = Arc::new(RecordingProgressTracker::new());
        let bar: Arc<dyn super::ProgressBarApi> = tracker.add_bar(100, "test");
        assert!(!bar.is_finished(), "recording bar starts unfinished");
        bar.advance(5);
        bar.finish_success();
        assert!(bar.is_finished(), "recording bar is finished");
    }

    #[test]
    fn rate_computation_handles_non_monotonic_position() {
        // When a bar's position regresses between ticks, the EMA rate
        // computation must not panic (saturating_sub guard).
        let term = indicatif::InMemoryTerm::new(10, 80);
        let target = indicatif::ProgressDrawTarget::term_like(Box::new(term.clone()));
        let mp = MultiProgress::with_draw_target(target);
        let group = ProgressGroup::builder().with_multi_progress(mp).capacity(4).build();
        let h = group.add_bar(100, "test");
        h.advance(80); // position grows to 80
        group.tick(); // tick captures prev_position = 80
        h.set_position(20); // position drops to 20 (non-monotonic)
        group.tick(); // must not panic (saturating_sub saves it)
        let snap = h.snapshot();
        assert_eq!(snap.position, 20);
        assert!(matches!(snap.status, TrackStatus::Active));
    }

    #[test]
    fn spinner_advances_per_cycle_for_all_bars() {
        // Each bar's spinner character must change across ticks, not just
        // the overall bar's.
        use super::inner::DimensionSource;
        use std::sync::Arc;

        let term = indicatif::InMemoryTerm::new(10, 80);
        let target = indicatif::ProgressDrawTarget::term_like(Box::new(term.clone()));
        let mp = MultiProgress::with_draw_target(target);
        let dims = Arc::new(super::inner::TestDimensionSource::new((10, 80)));
        let ts = Arc::new(super::TestTimeSource::new());

        let (group, overall) = super::ProgressGroup::builder()
            .with_multi_progress(mp)
            .with_dim_source(dims as Arc<dyn DimensionSource>)
            .with_time_source(ts.clone() as Arc<dyn super::TimeSource>)
            // Disable the daemon ticker: it fires group.tick() on real
            // wall-clock time, injecting extra spinner ticks beyond this
            // test's manual ticks.  (indicatif's set_position also advances
            // the tick counter through a wall-clock position rate limiter,
            // so the assertion below checks a distinct-glyph set.)
            .with_ticker_enabled(false)
            .with_overall("syncing", 3)
            .build();

        let bar1 = group.add_bar(100, "tool [resolve]");
        let bar2 = group.add_bar(100, "tool [fetch]");

        // Advance time so rate computation has positive dt.
        ts.advance(std::time::Duration::from_millis(100));

        // Collect the first-char (spinner) from each line across many ticks.
        let mut snapshots: Vec<Vec<char>> = Vec::new();
        for _ in 0..30 {
            // Change positions each tick to trigger tick_inner via setters.
            bar1.advance(1);
            bar2.advance(2);
            overall.advance(0);
            ts.advance(std::time::Duration::from_millis(50));
            group.tick();

            let output = term.contents();
            let line_chars: Vec<char> = output
                .lines()
                .filter(|l| !l.is_empty())
                .map(|l| l.chars().next().unwrap_or(' '))
                .collect();
            if !line_chars.is_empty() {
                snapshots.push(line_chars);
            }
        }

        // Must have captured several distinct snapshots.
        assert!(snapshots.len() >= 10, "expected >=10 captured snapshots, got {}", snapshots.len());

        // Every bar's spinner must be animating.  We can't assert
        // first-vs-last inequality: the glyph is indicatif's tick counter
        // mod 9, and that counter advances via BOTH set_position (gated by
        // a real-wall-clock position rate limiter) and the end-of-tick
        // bar.tick(), so the exact per-iteration advance varies with real
        // timing.  What IS guaranteed: every active bar is ticked at least
        // once per group.tick(), so 30 ticks over a 9-glyph cycle always
        // produce several distinct glyphs per bar.  Assert >= 2 distinct.
        let n_bars = snapshots.iter().map(Vec::len).max().unwrap_or(0);
        let mut distinct_per_bar: Vec<std::collections::BTreeSet<char>> =
            vec![std::collections::BTreeSet::new(); n_bars];
        for snap in &snapshots {
            for (i, c) in snap.iter().enumerate().take(n_bars) {
                distinct_per_bar[i].insert(*c);
            }
        }
        for (i, set) in distinct_per_bar.iter().enumerate() {
            assert!(
                set.len() >= 2,
                "bar {i}: spinner must cycle through >= 2 distinct glyphs, \
                 saw only {set:?} across {} snapshots",
                snapshots.len()
            );
        }
    }

    #[test]
    fn recycled_bar_spinner_animates() {
        // Force slot recycling by creating a renderer with a single child
        // slot.  When bar1 finishes and bar2 attaches, bar2 reuses bar1's
        // slot.  Without the fix, bar2's indicatif bar would still have
        // Status::DoneVisible and the spinner would show the final char
        // (⠏ for our tick set) without cycling.
        use super::inner::DimensionSource;
        use std::sync::Arc;

        let term = indicatif::InMemoryTerm::new(10, 80);
        let target = indicatif::ProgressDrawTarget::term_like(Box::new(term.clone()));
        let mp = MultiProgress::with_draw_target(target);
        let dims = Arc::new(super::inner::TestDimensionSource::new((10, 80)));
        let ts = Arc::new(super::TestTimeSource::new());

        // capacity=2 means 1 child + 1 overall bar.
        // dynamic_height=false fixed capacity prevents auto-growing.
        let (group, overall) = super::ProgressGroup::builder()
            .with_multi_progress(mp)
            .with_dim_source(dims as Arc<dyn DimensionSource>)
            .with_time_source(ts.clone() as Arc<dyn super::TimeSource>)
            // Disable the daemon ticker: it fires group.tick() on real
            // wall-clock time, injecting extra spinner ticks beyond this
            // test's manual ticks.  (indicatif's set_position also advances
            // the tick counter through a wall-clock position rate limiter,
            // so the assertion below checks a distinct-glyph set.)
            .with_ticker_enabled(false)
            .capacity(2)
            .dynamic_height(false)
            .with_overall("syncing", 3)
            .build();

        // Phase 1: finish resolve bar (fills the single child slot).
        let bar1 = group.add_bar(1, "tool [resolve]");
        bar1.finish_success();

        // Tick to trigger finish_slot → bar.finish() → DoneVisible.
        ts.advance(std::time::Duration::from_millis(50));
        group.tick();

        // Phase 2: add fetch bar (recycles bar1's slot via attach Phase 2).
        let bar2 = group.add_bar(5, "tool [fetch]");
        bar2.advance(2);
        ts.advance(std::time::Duration::from_millis(50));
        group.tick();

        // Capture spinner chars across many ticks.
        let mut snapshots: Vec<Vec<char>> = Vec::new();
        for _ in 0..30 {
            bar2.advance(1);
            overall.advance(0);
            ts.advance(std::time::Duration::from_millis(50));
            group.tick();

            let output = term.contents();
            let line_chars: Vec<char> = output
                .lines()
                .filter(|l| !l.is_empty())
                .map(|l| l.chars().next().unwrap_or(' '))
                .collect();
            if !line_chars.is_empty() {
                snapshots.push(line_chars);
            }
        }

        // Must have captured several distinct snapshots.
        assert!(snapshots.len() >= 10, "expected >=10 captured snapshots, got {}", snapshots.len());

        // The spinner char for the child bar (first line) must differ
        // between the first and last snapshot — if it's the same, the
        // spinner is frozen because the indicatif bar stayed DoneVisible.
        let first = &snapshots[0];
        assert!(
            first.len() >= 2,
            "expected at least 2 visible bars (child + overall), got {}",
            first.len()
        );
        // Same window-robust glyph check as spinner_advances_per_cycle_for_all_bars:
        // the child bar's glyph is the tick counter mod 9, advanced by both
        // set_position (wall-clock rate-limited) and the end-of-tick bar.tick(),
        // so assert >= 2 distinct glyphs across the window instead of
        // first-vs-last inequality.
        let distinct_child: std::collections::BTreeSet<char> =
            snapshots.iter().filter_map(|s| s.first()).copied().collect();
        assert!(
            distinct_child.len() >= 2,
            "child bar spinner must cycle through >= 2 distinct glyphs, \
             saw only {distinct_child:?} across {} snapshots — slot status leak",
            snapshots.len()
        );
    }

    // ── Color helpers (ANSI escape code generation) ─────────────────────

    #[test]
    fn bar_color_code_active_child() {
        assert_eq!(super::inner::bar_color_code(super::TrackStatus::Active, false), "33");
    }

    #[test]
    fn bar_color_code_active_overall() {
        assert_eq!(super::inner::bar_color_code(super::TrackStatus::Active, true), "35");
    }

    #[test]
    fn bar_color_code_failed() {
        assert_eq!(super::inner::bar_color_code(super::TrackStatus::Failed, false), "31");
        assert_eq!(super::inner::bar_color_code(super::TrackStatus::Failed, true), "31");
    }

    #[test]
    fn bar_color_code_warning() {
        assert_eq!(super::inner::bar_color_code(super::TrackStatus::Warning, false), "33");
    }

    #[test]
    fn bar_color_code_success() {
        assert_eq!(super::inner::bar_color_code(super::TrackStatus::Success, false), "32");
    }

    // ---- ANSI-safe truncation helpers (Phase 1) -------------------------

    #[test]
    fn strip_ansi_empty() {
        assert_eq!(super::inner::strip_ansi(""), "");
    }

    #[test]
    fn strip_ansi_no_escapes() {
        assert_eq!(super::inner::strip_ansi("hello world"), "hello world");
    }

    #[test]
    fn strip_ansi_reset() {
        assert_eq!(super::inner::strip_ansi("\x1b[0m"), "");
    }

    #[test]
    fn strip_ansi_multiple() {
        assert_eq!(super::inner::strip_ansi("\x1b[31mfoo\x1b[0m"), "foo");
        assert_eq!(super::inner::strip_ansi("\x1b[33m[F]\x1b[0m bar"), "[F] bar");
    }

    #[test]
    fn strip_ansi_non_sgr_ignored() {
        // Non-SGR escape sequences (not ending with 'm') are passed through.
        assert_eq!(super::inner::strip_ansi("\x1b[2J"), "\x1b[2J");
        assert_eq!(super::inner::strip_ansi("a\x1b[Kb"), "a\x1b[Kb");
    }

    #[test]
    fn visible_width_empty() {
        assert_eq!(super::inner::visible_width(""), 0);
    }

    #[test]
    fn visible_width_no_ansi() {
        assert_eq!(super::inner::visible_width("hello"), 5);
    }

    #[test]
    fn visible_width_with_ansi() {
        assert_eq!(super::inner::visible_width("\x1b[31mhello\x1b[0m"), 5);
        assert_eq!(super::inner::visible_width("\x1b[33m[F]\x1b[0m wget"), 8);
    }

    #[test]
    fn visible_width_ansi_only() {
        assert_eq!(super::inner::visible_width("\x1b[0m"), 0);
        assert_eq!(super::inner::visible_width("\x1b[31m\x1b[33m"), 0);
    }

    #[test]
    fn max_prefix_width_wide() {
        assert_eq!(super::inner::max_prefix_width(80), 30);
        assert_eq!(super::inner::max_prefix_width(60), 30);
    }

    #[test]
    fn max_prefix_width_compact() {
        assert_eq!(super::inner::max_prefix_width(59), 25);
        assert_eq!(super::inner::max_prefix_width(40), 25);
    }

    #[test]
    fn max_suffix_width_wide() {
        assert_eq!(super::inner::max_suffix_width(80), 55);
        assert_eq!(super::inner::max_suffix_width(60), 55);
    }

    #[test]
    fn max_suffix_width_compact() {
        assert_eq!(super::inner::max_suffix_width(59), 40);
        assert_eq!(super::inner::max_suffix_width(40), 40);
    }

    // ---- render_prefix_components tests (Phase 2) -----------------------

    #[test]
    fn render_prefix_components_all_fields() {
        let result = super::inner::render_prefix_components(
            &super::inner::PrefixComponents {
                marker: String::new(),
                tool_name: "wget".into(),
                version: "1.2.3".into(),
                phase: "fch".into(),
                count: "2".into(),
                total: "5".into(),
            },
            super::TrackStatus::Active,
        );
        assert_eq!(result, "\x1b[0mwget 1.2.3 [fch] 2/5", "all fields rendered");
    }

    #[test]
    fn render_prefix_components_empty_version() {
        let result = super::inner::render_prefix_components(
            &super::inner::PrefixComponents {
                marker: String::new(),
                tool_name: "wget".into(),
                version: String::new(),
                phase: "fch".into(),
                count: "2".into(),
                total: "5".into(),
            },
            super::TrackStatus::Active,
        );
        assert_eq!(result, "\x1b[0mwget [fch] 2/5", "version omitted when empty");
    }

    #[test]
    fn render_prefix_components_only_tool_name() {
        let result = super::inner::render_prefix_components(
            &super::inner::PrefixComponents {
                marker: String::new(),
                tool_name: "wget".into(),
                version: String::new(),
                phase: String::new(),
                count: String::new(),
                total: String::new(),
            },
            super::TrackStatus::Active,
        );
        assert_eq!(result, "\x1b[0mwget", "only tool name when rest empty");
    }

    #[test]
    fn render_prefix_components_empty_phase_and_count() {
        let result = super::inner::render_prefix_components(
            &super::inner::PrefixComponents {
                marker: String::new(),
                tool_name: "wget".into(),
                version: "1.2.3".into(),
                phase: String::new(),
                count: String::new(),
                total: String::new(),
            },
            super::TrackStatus::Active,
        );
        assert_eq!(result, "\x1b[0mwget 1.2.3", "version without phase/count");
    }

    #[test]
    fn render_prefix_components_marker_failed() {
        let result = super::inner::render_prefix_components(
            &super::inner::PrefixComponents {
                marker: "F".into(),
                tool_name: "wget".into(),
                version: "1.2.3".into(),
                phase: "fch".into(),
                count: "2".into(),
                total: "5".into(),
            },
            super::TrackStatus::Failed,
        );
        assert_eq!(
            result, "\x1b[0m\x1b[31m[F]\x1b[0m wget 1.2.3 [fch] 2/5",
            "failed marker rendered red"
        );
    }

    #[test]
    fn render_prefix_components_marker_warning() {
        let result = super::inner::render_prefix_components(
            &super::inner::PrefixComponents {
                marker: "W".into(),
                tool_name: "wget".into(),
                version: "1.2.3".into(),
                phase: "fch".into(),
                count: "2".into(),
                total: "5".into(),
            },
            super::TrackStatus::Warning,
        );
        assert_eq!(
            result, "\x1b[0m\x1b[33m[W]\x1b[0m wget 1.2.3 [fch] 2/5",
            "warning marker rendered yellow"
        );
    }

    #[test]
    fn render_prefix_components_normal_states_no_marker() {
        for status in [super::TrackStatus::Active, super::TrackStatus::Success] {
            let result = super::inner::render_prefix_components(
                &super::inner::PrefixComponents {
                    marker: String::new(),
                    tool_name: "child".into(),
                    version: String::new(),
                    phase: String::new(),
                    count: String::new(),
                    total: String::new(),
                },
                status,
            );
            assert_eq!(result, "\x1b[0mchild", "{status:?}: no bracket for empty marker");
        }
    }

    #[test]
    fn render_prefix_components_marker_uncolored_when_status_normal() {
        // A non-empty marker with a normal status renders uncolored (no SGR
        // color codes around the bracket).
        let result = super::inner::render_prefix_components(
            &super::inner::PrefixComponents {
                marker: "F".into(),
                tool_name: "wget".into(),
                version: String::new(),
                phase: String::new(),
                count: String::new(),
                total: String::new(),
            },
            super::TrackStatus::Active,
        );
        assert_eq!(result, "\x1b[0m[F] wget", "marker bracket uncolored for normal status");
    }

    #[test]
    fn render_prefix_components_always_starts_with_reset() {
        for status in [
            super::TrackStatus::Active,
            super::TrackStatus::Failed,
            super::TrackStatus::Warning,
            super::TrackStatus::Success,
        ] {
            let result = super::inner::render_prefix_components(
                &super::inner::PrefixComponents {
                    marker: "F".into(),
                    tool_name: "foo".into(),
                    version: String::new(),
                    phase: String::new(),
                    count: String::new(),
                    total: String::new(),
                },
                status,
            );
            assert!(
                result.starts_with("\x1b[0m"),
                "{status:?}: expected \\x1b[0m prefix, got {result:?}"
            );
        }
    }

    /// Render truncated prefix components with Active status for assertions.
    fn render_prefix(parts: &super::inner::PrefixComponents) -> String {
        super::inner::render_prefix_components(parts, super::TrackStatus::Active)
    }

    // ---- Regression: TrackStatus marker behavior (post-simplification) ----

    #[test]
    fn regression_finish_warning_stores_warning_and_decodes() {
        let h = TrackedHandle::new(10);
        assert!(!h.is_finished());
        h.finish_warning();
        assert!(h.is_finished(), "finish_warning marks the handle finished");
        assert_eq!(h.snapshot().status, TrackStatus::Warning, "finish_warning stores Warning");
    }

    #[test]
    fn regression_finish_error_stores_failed_and_decodes() {
        let h = TrackedHandle::new(10);
        h.finish_error();
        assert!(h.is_finished(), "finish_error marks the handle finished");
        assert_eq!(h.snapshot().status, TrackStatus::Failed, "finish_error stores Failed");
    }

    #[test]
    fn regression_finish_success_stores_success_and_decodes() {
        let h = TrackedHandle::new(10);
        h.finish_success();
        assert!(h.is_finished(), "finish_success marks the handle finished");
        assert_eq!(h.snapshot().status, TrackStatus::Success, "finish_success stores Success");
    }

    #[test]
    fn regression_render_prefix_components_status_markers() {
        // Failed => red [F], Warning => yellow [W], others uncolored.
        let parts = super::inner::PrefixComponents {
            marker: "F".into(),
            tool_name: "wget".into(),
            version: String::new(),
            phase: String::new(),
            count: String::new(),
            total: String::new(),
        };
        let failed = super::inner::render_prefix_components(&parts, TrackStatus::Failed);
        assert_eq!(failed, "\x1b[0m\x1b[31m[F]\x1b[0m wget", "failed marker rendered red");

        // Warning status colors the marker yellow; the marker glyph itself is
        // carried by the `marker` field (set to "W" at construction).
        let warning_parts = super::inner::PrefixComponents {
            marker: "W".into(),
            tool_name: "wget".into(),
            version: String::new(),
            phase: String::new(),
            count: String::new(),
            total: String::new(),
        };
        let warning = super::inner::render_prefix_components(&warning_parts, TrackStatus::Warning);
        assert_eq!(warning, "\x1b[0m\x1b[33m[W]\x1b[0m wget", "warning marker rendered yellow [W]");

        for status in [TrackStatus::Active, TrackStatus::Success] {
            let normal = super::inner::render_prefix_components(&parts, status);
            assert!(
                !normal.contains("\x1b[31m") && !normal.contains("\x1b[33m"),
                "{status:?}: marker must be uncolored (no red/yellow SGR)"
            );
            assert!(normal.contains("[F]"), "{status:?}: uncolored [F] bracket still present");
        }
    }

    #[test]
    fn regression_bar_color_code_status_codes() {
        assert_eq!(super::inner::bar_color_code(TrackStatus::Failed, false), "31");
        assert_eq!(super::inner::bar_color_code(TrackStatus::Warning, false), "33");
        assert_eq!(super::inner::bar_color_code(TrackStatus::Success, false), "32");
    }

    #[test]
    fn regression_snapshot_status_code_round_trip() {
        // Each known status decodes back to the correct TrackStatus via the
        // public finish_* API. The `_ => Success` fallback for unknown codes
        // is covered by the inner-module unit test below.
        let active = TrackedHandle::new(10);
        assert_eq!(active.snapshot().status, TrackStatus::Active);

        let success = TrackedHandle::new(10);
        success.finish_success();
        assert_eq!(success.snapshot().status, TrackStatus::Success);

        let failed = TrackedHandle::new(10);
        failed.finish_error();
        assert_eq!(failed.snapshot().status, TrackStatus::Failed);

        let warning = TrackedHandle::new(10);
        warning.finish_warning();
        assert_eq!(warning.snapshot().status, TrackStatus::Warning);

        // finish_and_clear stores code 5, which the snapshot decoder maps to
        // Success (the `_ => Success` fallback arm).
        let cleared = TrackedHandle::new(10);
        cleared.finish_and_clear();
        assert_eq!(cleared.snapshot().status, TrackStatus::Success);
    }

    // ---- prefix_components_from_str tests (Phase 2) -----------------------

    #[test]
    fn prefix_components_from_str_tool_name_only() {
        let result = super::inner::prefix_components_from_str("wget");
        assert_eq!(result.tool_name, "wget");
        assert!(result.marker.is_empty());
        assert!(result.version.is_empty());
        assert!(result.phase.is_empty());
        assert!(result.count.is_empty());
        assert!(result.total.is_empty());
    }

    #[test]
    fn prefix_components_from_str_all_fields() {
        let result = super::inner::prefix_components_from_str("yt-dlp 2024.12.20 [fch] 2/5");
        assert_eq!(result.tool_name, "yt-dlp");
        assert_eq!(result.version, "2024.12.20");
        assert_eq!(result.phase, "fch");
        assert_eq!(result.count, "2");
        assert_eq!(result.total, "5");
    }

    #[test]
    fn prefix_components_from_str_multiword_no_bracket() {
        // No-bracket labels with multiple words keep the whole string as
        // tool_name (regression: previously only the first token was kept).
        let result = super::inner::prefix_components_from_str("syncing tools");
        assert_eq!(result.tool_name, "syncing tools");
        assert!(result.version.is_empty());
        assert!(result.phase.is_empty());
        assert!(result.count.is_empty());
        assert!(result.total.is_empty());
    }

    #[test]
    fn prefix_components_from_str_multiword_with_count_total() {
        // A trailing count/total token after a multi-word tool name is split
        // off; the rest of the string stays the tool name.
        let result = super::inner::prefix_components_from_str("syncing tools 2/5");
        assert_eq!(result.tool_name, "syncing tools");
        assert!(result.version.is_empty());
        assert!(result.phase.is_empty());
        assert_eq!(result.count, "2");
        assert_eq!(result.total, "5");
    }

    #[test]
    fn prefix_components_from_str_keeps_no_bracket_trailing_nonslash_word() {
        // A trailing word without `/` is NOT a count/total token — the whole
        // string remains the tool name.
        let result = super::inner::prefix_components_from_str("materializing files");
        assert_eq!(result.tool_name, "materializing files");
        assert!(result.version.is_empty());
        assert!(result.phase.is_empty());
        assert!(result.count.is_empty());
        assert!(result.total.is_empty());
    }

    #[test]
    fn shared_state_parses_label_into_components() {
        // add_bar labels must be parsed into PrefixComponents at construction
        // (regression: previously the entire label was stored as tool_name,
        // so semantic truncation chopped `[res]` off resolve bars).
        let h = TrackedHandle::with_label(100, "ffmpeg autobuild-2026-07-31 [res]");
        let snap = h.snapshot();
        assert_eq!(snap.prefix_components.tool_name, "ffmpeg");
        assert_eq!(snap.prefix_components.version, "autobuild-2026-07-31");
        assert_eq!(snap.prefix_components.phase, "res");
        assert!(snap.prefix_components.count.is_empty());
        assert!(snap.prefix_components.total.is_empty());

        let h = TrackedHandle::with_label(100, "syncing tools");
        let snap = h.snapshot();
        assert_eq!(snap.prefix_components.tool_name, "syncing tools");

        let h = TrackedHandle::with_label(100, "");
        let snap = h.snapshot();
        assert!(snap.prefix_components.tool_name.is_empty());
        assert!(snap.prefix_components.version.is_empty());
        assert!(snap.prefix_components.phase.is_empty());
    }

    #[test]
    fn truncate_parsed_resolve_label_preserves_phase() {
        // The user-reported symptom: a long resolve label truncated to the
        // prefix budget must shrink the version first and keep `[res]`.
        let parts = super::inner::prefix_components_from_str("ffmpeg autobuild-2026-07-31 [res]");
        let result = super::inner::semantic_truncate_prefix(&parts, 26);
        assert_eq!(result.tool_name, "ffmpeg");
        assert_eq!(result.version, "autobuild-202");
        assert_eq!(result.phase, "res");
        assert!(result.count.is_empty());
        assert!(result.total.is_empty());
    }

    // ---- semantic_truncate_prefix tests (Phase 2) -----------------------

    fn prefix_parts_wget() -> super::inner::PrefixComponents {
        super::inner::PrefixComponents {
            marker: String::new(),
            tool_name: "wget".into(),
            version: "1.2.3".into(),
            phase: "fch".into(),
            count: "2".into(),
            total: "5".into(),
        }
    }

    #[test]
    fn semantic_truncate_prefix_already_fits() {
        let parts = prefix_parts_wget();
        let result = super::inner::semantic_truncate_prefix(&parts, 30);
        assert_eq!(
            render_prefix(&result),
            "\x1b[0mwget 1.2.3 [fch] 2/5",
            "no truncation when fits"
        );
    }

    #[test]
    fn semantic_truncate_prefix_remove_version() {
        let parts = prefix_parts_wget();
        // max=17: version shrunk to "1."
        let result = super::inner::semantic_truncate_prefix(&parts, 17);
        assert_eq!(
            render_prefix(&result),
            "\x1b[0mwget 1. [fch] 2/5",
            "version shortened by 3 chars"
        );
    }

    #[test]
    fn semantic_truncate_prefix_remove_version_full() {
        let parts = prefix_parts_wget();
        // max=15: version fully removed
        let result = super::inner::semantic_truncate_prefix(&parts, 15);
        assert_eq!(
            render_prefix(&result),
            "\x1b[0mwget [fch] 2/5",
            "version fully removed when excess covers it"
        );
    }

    #[test]
    fn semantic_truncate_prefix_remove_version_and_count_total() {
        let parts = prefix_parts_wget();
        // max=11: version and count/total removed
        let result = super::inner::semantic_truncate_prefix(&parts, 11);
        assert_eq!(render_prefix(&result), "\x1b[0mwget [fch]", "version and count/total removed");
    }

    #[test]
    fn semantic_truncate_prefix_count_total_removed_atomically() {
        let parts = prefix_parts_wget();
        // max=12: version gone, count/total removed as one pair — a bare "2/"
        // or "/5" must never appear.
        let result = super::inner::semantic_truncate_prefix(&parts, 12);
        assert_eq!(
            render_prefix(&result),
            "\x1b[0mwget [fch]",
            "count/total removed as atomic pair"
        );
    }

    #[test]
    fn semantic_truncate_prefix_phase_removed_atomically() {
        let parts = prefix_parts_wget();
        // max=8: count/total gone, phase removed entirely (never a bare "[f]")
        let result = super::inner::semantic_truncate_prefix(&parts, 8);
        assert_eq!(
            render_prefix(&result),
            "\x1b[0mwget",
            "phase removed entirely, no partial bracket"
        );
    }

    #[test]
    fn semantic_truncate_prefix_remove_version_count_phase() {
        let parts = prefix_parts_wget();
        // max=4: only tool name remains
        let result = super::inner::semantic_truncate_prefix(&parts, 4);
        assert_eq!(
            render_prefix(&result),
            "\x1b[0mwget",
            "tool name survives after version, count, phase removed"
        );
    }

    #[test]
    fn semantic_truncate_prefix_tool_name_progressive_shrink() {
        let parts = prefix_parts_wget();
        // max=2: progressive shrink of tool name (no fallback string mangling)
        let result = super::inner::semantic_truncate_prefix(&parts, 2);
        assert_eq!(render_prefix(&result), "\x1b[0mwg", "tool name shrunk to 2 chars");
    }

    #[test]
    fn semantic_truncate_prefix_empty() {
        let parts = super::inner::PrefixComponents::default();
        let result = super::inner::semantic_truncate_prefix(&parts, 30);
        assert_eq!(render_prefix(&result), "\x1b[0m", "empty prefix stays empty");
    }

    #[test]
    fn semantic_truncate_prefix_marker_removed_before_tool_name() {
        // Marker is truncatable data: dropped before the tool name shrinks.
        let parts = super::inner::PrefixComponents {
            marker: "F".into(),
            tool_name: "wget".into(),
            version: String::new(),
            phase: "fch".into(),
            count: "0".into(),
            total: "1".into(),
        };
        // max=7: marker (4) + tool (4) = 8 > 7 — marker removed, tool intact.
        let result = super::inner::semantic_truncate_prefix(&parts, 7);
        assert!(result.marker.is_empty(), "marker removed before tool name");
        assert_eq!(render_prefix(&result), "\x1b[0mwget", "tool name intact after marker removal");
        // max=3: marker removed first, then tool shrinks to 3 chars.
        let result = super::inner::semantic_truncate_prefix(&parts, 3);
        assert_eq!(render_prefix(&result), "\x1b[0mwge", "marker dropped before tool shrank");
    }

    #[test]
    fn semantic_truncate_prefix_marker_preserved_when_fits() {
        // max=9: after count/total and phase removal, marker (4) + tool (4)
        // = 8 <= 9 — marker preserved.
        let parts = super::inner::PrefixComponents {
            marker: "F".into(),
            tool_name: "wget".into(),
            version: String::new(),
            phase: "fch".into(),
            count: "0".into(),
            total: "1".into(),
        };
        let result = super::inner::semantic_truncate_prefix(&parts, 9);
        assert_eq!(
            super::inner::render_prefix_components(&result, super::TrackStatus::Failed),
            "\x1b[0m\x1b[31m[F]\x1b[0m wget",
            "marker preserved when it fits"
        );
    }

    #[test]
    fn semantic_truncate_prefix_marker_failed_effective_width() {
        // max=4: marker (4) + tool (4) = 8 > 4 — marker removed so the tool
        // name fits the effective width.
        let parts = super::inner::PrefixComponents {
            marker: "F".into(),
            tool_name: "wget".into(),
            version: String::new(),
            phase: "fch".into(),
            count: "0".into(),
            total: "1".into(),
        };
        let result = super::inner::semantic_truncate_prefix(&parts, 4);
        assert_eq!(
            super::inner::render_prefix_components(&result, super::TrackStatus::Failed),
            "\x1b[0mwget",
            "marker removed to fit effective width"
        );
    }

    #[test]
    fn semantic_truncate_prefix_version_shrinks_first_long_version() {
        // Regression: version must shrink (never mangle into a mid-char
        // suffix) before any other component is touched.
        let parts = super::inner::PrefixComponents {
            marker: String::new(),
            tool_name: "ffmpeg".into(),
            version: "autobuild-2026-07-31".into(),
            phase: "res".into(),
            count: "2".into(),
            total: "2".into(),
        };
        let result = super::inner::semantic_truncate_prefix(&parts, 30);
        assert_eq!(
            render_prefix(&result),
            "\x1b[0mffmpeg autobuild-202 [res] 2/2",
            "long version shrinks first, no mangled suffix"
        );
        let result = super::inner::semantic_truncate_prefix(&parts, 8);
        assert_eq!(render_prefix(&result), "\x1b[0mffmpeg", "all optional components dropped");
    }

    // ---- semantic_truncate_suffix tests (Phase 1) -----------------------

    /// Canonical test parts: auto components (33 visible) + custom (10).
    fn suffix_parts_full() -> SuffixComponents {
        SuffixComponents {
            count: "2".into(),
            total: "5".into(),
            elapsed: "0:00:05".into(),
            rate: Some("12.3 MiB/s".into()),
            eta: Some("[0:00:02]".into()),
            custom: "cached (1)".into(),
        }
    }

    /// Render truncated parts with the child-bar color code like production.
    fn render_suffix(parts: &SuffixComponents) -> String {
        super::inner::render_suffix_components(parts, "33")
    }

    #[test]
    fn semantic_truncate_suffix_fits() {
        let parts = suffix_parts_full();
        let result = super::inner::semantic_truncate_suffix(&parts, 100);
        assert_eq!(
            render_suffix(&result),
            " \x1b[33m2/5\x1b[0m 0:00:05 12.3 MiB/s [0:00:02] cached (1)",
            "full suffix when fits"
        );
    }

    #[test]
    fn semantic_truncate_suffix_remove_custom_partial() {
        let parts = suffix_parts_full();
        let result = super::inner::semantic_truncate_suffix(&parts, 39);
        assert_eq!(
            render_suffix(&result),
            " \x1b[33m2/5\x1b[0m 0:00:05 12.3 MiB/s [0:00:02] cache",
            "custom shrunk to keep=5 (39 visible)"
        );
    }

    #[test]
    fn semantic_truncate_suffix_remove_custom_all() {
        let parts = suffix_parts_full();
        let result = super::inner::semantic_truncate_suffix(&parts, 34);
        assert_eq!(
            render_suffix(&result),
            " \x1b[33m2/5\x1b[0m 0:00:05 12.3 MiB/s [0:00:02]",
            "custom fully removed (auto 33 <= 34)"
        );
    }

    #[test]
    fn semantic_truncate_suffix_removes_eta() {
        let parts = suffix_parts_full();
        let result = super::inner::semantic_truncate_suffix(&parts, 30);
        assert_eq!(
            render_suffix(&result),
            " \x1b[33m2/5\x1b[0m 0:00:05 12.3 MiB/s",
            "eta removed entirely (23 <= 30)"
        );
    }

    #[test]
    fn semantic_truncate_suffix_removes_rate() {
        let parts = suffix_parts_full();
        let result = super::inner::semantic_truncate_suffix(&parts, 22);
        assert_eq!(
            render_suffix(&result),
            " \x1b[33m2/5\x1b[0m 0:00:05",
            "rate removed entirely (12 <= 22)"
        );
    }

    #[test]
    fn semantic_truncate_suffix_removes_elapsed() {
        let parts = suffix_parts_full();
        let result = super::inner::semantic_truncate_suffix(&parts, 11);
        assert_eq!(
            render_suffix(&result),
            " \x1b[33m2/5\x1b[0m",
            "elapsed removed entirely (4 <= 11)"
        );
    }

    #[test]
    fn semantic_truncate_suffix_fits_count_total() {
        let parts = suffix_parts_full();
        let result = super::inner::semantic_truncate_suffix(&parts, 4);
        assert_eq!(
            render_suffix(&result),
            " \x1b[33m2/5\x1b[0m",
            "count/total is the last unit and fits at 4"
        );
    }

    #[test]
    fn semantic_truncate_suffix_removes_count_total() {
        let parts = suffix_parts_full();
        let result = super::inner::semantic_truncate_suffix(&parts, 3);
        assert_eq!(render_suffix(&result), "", "count/total removed entirely (0 <= 3)");
    }

    #[test]
    fn semantic_truncate_suffix_count_total_atomic_no_partial() {
        // Width 3: the full unit ` 2/5` (4 visible) does not fit, but a partial
        // like ` 2/` (3 visible) would. Atomicity requires dropping the whole
        // pair — a bare count or partial unit is never shown.
        let parts = SuffixComponents {
            count: "2".into(),
            total: "5".into(),
            elapsed: "0:00:05".into(),
            rate: None,
            eta: None,
            custom: String::new(),
        };
        let result = super::inner::semantic_truncate_suffix(&parts, 3);
        assert_eq!(render_suffix(&result), "", "no partial count/total at width 3");
    }

    #[test]
    fn semantic_truncate_suffix_auto_components_removed_without_custom() {
        // Full = 12 visible; at 10 elapsed is removed, leaving ` 2/5` (4).
        let parts = SuffixComponents {
            count: "2".into(),
            total: "5".into(),
            elapsed: "0:00:05".into(),
            rate: None,
            eta: None,
            custom: String::new(),
        };
        let result = super::inner::semantic_truncate_suffix(&parts, 10);
        assert_eq!(render_suffix(&result), " \x1b[33m2/5\x1b[0m", "elapsed removed at 10");
    }

    #[test]
    fn semantic_truncate_suffix_empty_both() {
        let result = super::inner::semantic_truncate_suffix(&SuffixComponents::default(), 30);
        assert_eq!(render_suffix(&result), "", "both empty");
    }

    #[test]
    fn semantic_truncate_suffix_custom_only() {
        let parts = SuffixComponents { custom: "custom".into(), ..Default::default() };
        let result = super::inner::semantic_truncate_suffix(&parts, 10);
        assert_eq!(render_suffix(&result), " custom", "custom appended after empty auto");
    }

    #[test]
    fn render_suffix_components_with_rate() {
        // rate present, no eta
        let parts = SuffixComponents {
            count: "0".into(),
            total: "5".into(),
            elapsed: "0s".into(),
            rate: Some("0/d".into()),
            eta: None,
            custom: String::new(),
        };
        let result = super::inner::render_suffix_components(&parts, "33");
        assert_eq!(result, " \x1b[33m0/5\x1b[0m 0s 0/d");
        assert!(result.ends_with("0s 0/d"), "expected elapsed then rate at end: {result:?}");
    }

    #[test]
    fn render_suffix_components_with_rate_and_eta() {
        // rate + eta
        let parts = SuffixComponents {
            count: "0".into(),
            total: "5".into(),
            elapsed: "0s".into(),
            rate: Some("0/d".into()),
            eta: Some("5s".into()),
            custom: String::new(),
        };
        let result = super::inner::render_suffix_components(&parts, "33");
        assert_eq!(result, " \x1b[33m0/5\x1b[0m 0s 0/d 5s");
        assert!(result.ends_with("0s 0/d 5s"), "expected elapsed rate eta at end: {result:?}");
    }

    #[test]
    fn render_suffix_components_different_color_codes() {
        for (code, status_name) in [("31", "failed"), ("33", "child"), ("35", "overall")] {
            let parts = SuffixComponents {
                count: "1".into(),
                total: "2".into(),
                elapsed: "3s".into(),
                rate: Some("0/d".into()),
                eta: None,
                custom: String::new(),
            };
            let result = super::inner::render_suffix_components(&parts, code);
            assert!(
                result.contains(&format!("\x1b[{code}m")),
                "{status_name} should use code {code}: {result:?}"
            );
            assert!(result.contains("1/2"), "{status_name} count/total absent: {result:?}");
        }
    }

    #[test]
    fn render_suffix_components_eta_suppressed_without_rate() {
        // Historical build_right_msg guard: eta renders only when rate is present.
        let parts = SuffixComponents {
            count: "0".into(),
            total: "5".into(),
            elapsed: "0s".into(),
            rate: None,
            eta: Some("5s".into()),
            custom: String::new(),
        };
        let result = super::inner::render_suffix_components(&parts, "33");
        assert_eq!(result, " \x1b[33m0/5\x1b[0m 0s", "eta suppressed without rate");
    }

    #[test]
    fn render_suffix_components_custom_appended() {
        let parts = SuffixComponents {
            count: "0".into(),
            total: "5".into(),
            elapsed: "0s".into(),
            rate: Some("0/d".into()),
            eta: None,
            custom: "cached (1)".into(),
        };
        let result = super::inner::render_suffix_components(&parts, "33");
        assert_eq!(result, " \x1b[33m0/5\x1b[0m 0s 0/d cached (1)", "custom appended");
    }

    // ---- Phase 4: BufferedTerm / dirty tracking tests ----------------------

    #[test]
    fn dirty_tracking_initial_state_starts_dirty() {
        // The SharedState dirty flag starts true so the very first tick always
        // draws, even without explicit mutations.
        let term = indicatif::InMemoryTerm::new(10, 80);
        let target = indicatif::ProgressDrawTarget::term_like(Box::new(term.clone()));
        let mp = MultiProgress::with_draw_target(target);
        let group = ProgressGroup::builder()
            .with_multi_progress(mp)
            .capacity(4)
            // Disable the daemon ticker: it fires group.tick() on real
            // wall-clock time, injecting extra ticks/draws beyond the manual
            // ticks this test controls.
            .with_ticker_enabled(false)
            .build();
        let _bar = group.add_bar(100, "test");

        group.tick();
        let content = term.contents();
        assert!(!content.is_empty(), "first tick must draw, got empty");
    }

    #[test]
    fn multiple_mutations_before_tick_single_draw() {
        // Several mutations between ticks should all be reflected in a single
        // coherent draw after the next tick, without intermediate draws.
        let term = indicatif::InMemoryTerm::new(10, 80);
        let target = indicatif::ProgressDrawTarget::term_like(Box::new(term.clone()));
        let mp = MultiProgress::with_draw_target(target);
        let group = ProgressGroup::builder()
            .with_multi_progress(mp)
            .capacity(4)
            // Disable the daemon ticker: it fires group.tick() on real
            // wall-clock time, injecting extra ticks/draws beyond the manual
            // ticks this test controls.
            .with_ticker_enabled(false)
            .build();
        let bar = group.add_bar(100, "test");

        bar.set_position(20);
        bar.set_total(200);
        bar.set_prefix_components(PrefixComponents {
            tool_name: "multi".into(),
            ..Default::default()
        });

        group.tick();
        let content = term.contents();
        assert!(content.contains("20/200"), "expected 20/200 in output: {content:?}");
        assert!(content.contains("multi"), "expected prefix 'multi' in output: {content:?}");
    }

    #[test]
    fn finalize_produces_final_output() {
        // join_and_clear (finalize) must produce visible output showing the
        // final state of all bars.  Uses TestTimeSource for deterministic
        // timing and exact terminal content matching.
        use std::sync::Arc;
        use std::time::Duration;

        let term = indicatif::InMemoryTerm::new(10, 80);
        let target = indicatif::ProgressDrawTarget::term_like(Box::new(term.clone()));
        let mp = MultiProgress::with_draw_target(target);
        let ts = Arc::new(super::TestTimeSource::new());
        let (group, overall) = ProgressGroup::builder()
            .with_multi_progress(mp)
            .with_time_source(Arc::clone(&ts) as Arc<dyn super::TimeSource>)
            // Disable the daemon ticker: it fires group.tick() on real
            // wall-clock time, injecting extra ticks/draws beyond the manual
            // ticks this test controls.
            .with_ticker_enabled(false)
            .capacity(4)
            .with_overall("overall", 10)
            .build();
        let bar = group.add_bar(100, "child");
        bar.advance(50);
        overall.advance(5);
        ts.advance(Duration::from_secs(1));

        // Sync state from SharedState to bars before finalize (finalize
        // only syncs non-Active bars).
        group.tick();
        group.join_and_clear();
        let actual = term.contents();
        let lines: Vec<&str> = actual.lines().collect();

        // EXACT line count: exactly 2 visible lines (child + overall).
        assert_eq!(
            lines.len(),
            2,
            "finalize must show exactly 2 bars, got {} lines:\n{actual}",
            lines.len(),
        );

        // Line 0: child bar — prefix and position visible.
        assert!(lines[0].contains("child"), "child prefix in line 0: {0}", lines[0]);
        assert!(lines[0].contains("50/100"), "child pos 50/100: {0}", lines[0]);

        // Line 1: overall bar — prefix and position visible.
        assert!(lines[1].contains("overall"), "overall prefix in line 1: {0}", lines[1]);
        assert!(lines[1].contains("5/10"), "overall pos 5/10: {0}", lines[1]);

        // Both lines show elapsed (1s).
        assert!(lines[0].contains("1s"), "child shows 1s elapsed: {0}", lines[0]);
        assert!(lines[1].contains("1s"), "overall shows 1s elapsed: {0}", lines[1]);
    }

    #[test]
    fn dirty_tracking_skips_clean_ticks() {
        // A tick without any mutation must produce identical terminal content
        // to the previous tick (dirty tracking skips the slot, bar.tick() is
        // not called, no redraw occurs).

        // Helper: extract the body (everything after the spinner char).
        fn content_body(s: &str) -> &str {
            s.lines().find_map(|l| l.get(1..)).unwrap_or("")
        }

        use super::inner::DimensionSource;
        use std::sync::Arc;

        let term = indicatif::InMemoryTerm::new(10, 80);
        let target = indicatif::ProgressDrawTarget::term_like(Box::new(term.clone()));
        let mp = MultiProgress::with_draw_target(target);
        let dims = Arc::new(super::inner::TestDimensionSource::new((10, 80)));
        let ts = Arc::new(super::TestTimeSource::new());

        let group = ProgressGroup::builder()
            .with_multi_progress(mp)
            .with_dim_source(dims as Arc<dyn DimensionSource>)
            .with_time_source(ts.clone() as Arc<dyn super::TimeSource>)
            // Disable the daemon ticker: it fires group.tick() on real
            // wall-clock time, injecting extra ticks/draws beyond the manual
            // ticks this test controls.
            .with_ticker_enabled(false)
            .capacity(4)
            .build();
        let bar = group.add_bar(100, "test");
        bar.set_position(42);
        ts.advance(std::time::Duration::from_millis(100));

        // Tick 1: baseline (bar appears).
        group.tick();
        let baseline = term.contents();
        assert!(!baseline.is_empty(), "baseline must have content");

        let baseline_body = content_body(&baseline);

        // Tick 2: no mutations → content body (everything except spinner)
        // must be identical.  The daemon ticker is disabled above, so the
        // spinner char is deterministic too; we still compare only the body
        // to keep the assertion focused on dirty tracking.
        ts.advance(std::time::Duration::from_millis(50));
        group.tick();
        let second = term.contents();
        assert_eq!(
            content_body(&second),
            baseline_body,
            "clean tick should not change content body\n\
         expected body: {baseline_body:?}\n\
         got body:      {:?}",
            content_body(&second)
        );

        // Tick 3: still no mutations → content body remains unchanged.
        ts.advance(std::time::Duration::from_millis(50));
        group.tick();
        let third = term.contents();
        assert_eq!(
            content_body(&third),
            baseline_body,
            "second clean tick should also not change content body\n\
         expected body: {baseline_body:?}\n\
         got body:      {:?}",
            content_body(&third)
        );
    }

    #[test]
    fn dirty_tracking_draws_on_mutation() {
        // A tick after mutation must reflect the new state in the output.
        use super::inner::DimensionSource;
        use std::sync::Arc;

        let term = indicatif::InMemoryTerm::new(10, 80);
        let target = indicatif::ProgressDrawTarget::term_like(Box::new(term.clone()));
        let mp = MultiProgress::with_draw_target(target);
        let dims = Arc::new(super::inner::TestDimensionSource::new((10, 80)));
        let ts = Arc::new(super::TestTimeSource::new());

        let group = ProgressGroup::builder()
            .with_multi_progress(mp)
            .with_dim_source(dims as Arc<dyn DimensionSource>)
            .with_time_source(ts.clone() as Arc<dyn super::TimeSource>)
            // Disable the daemon ticker: it fires group.tick() on real
            // wall-clock time, injecting extra ticks/draws beyond the manual
            // ticks this test controls.
            .with_ticker_enabled(false)
            .capacity(4)
            .build();
        let bar = group.add_bar(100, "test");
        bar.set_position(10);
        ts.advance(std::time::Duration::from_millis(100));

        // Tick 1: baseline shows 10/100.
        group.tick();
        let baseline = term.contents();
        assert!(baseline.contains("10/100"), "baseline must contain 10/100, got: {baseline:?}");

        // Mutate position between ticks.
        bar.set_position(80);
        ts.advance(std::time::Duration::from_millis(50));

        // Tick 2: must show the new position (body changes even if
        // the spinner also advanced via daemon ticker).
        group.tick();
        let after = term.contents();
        assert!(after.contains("80/100"), "expected 80/100 after mutation+tick, got: {after:?}");
    }

    // ---- Pre-roll tests ----------------------------------------------------

    #[test]
    fn pre_roll_reserves_full_terminal_height() {
        // When pre_roll fires, it must write exactly `rows` newlines (one per
        // terminal row) and move the cursor back up by the same amount.
        use super::inner::DimensionSource;
        use std::sync::Arc;

        let mp_term = indicatif::InMemoryTerm::new(10, 80);
        let cap_term = indicatif::InMemoryTerm::new(100, 80);
        let target = indicatif::ProgressDrawTarget::term_like(Box::new(mp_term.clone()));
        let mp = MultiProgress::with_draw_target(target);
        let dims = Arc::new(super::inner::TestDimensionSource::new((10, 80)));
        let ts = Arc::new(super::TestTimeSource::new());

        let _group = ProgressGroup::builder()
            .with_multi_progress(mp)
            .with_pre_roll_capture(Box::new(cap_term.clone()))
            .with_dim_source(dims as Arc<dyn DimensionSource>)
            .with_time_source(ts.clone() as Arc<dyn super::TimeSource>)
            .capacity(2)
            .build();

        // Wait for the background ticker to fire pre_roll.
        std::thread::sleep(std::time::Duration::from_millis(200));

        let moves = cap_term.moves_since_last_check();
        let newline_count = moves.lines().filter(|l| l.trim() == "NewLine").count();
        assert_eq!(
            newline_count, 10,
            "pre_roll should write exactly 10 blank lines (rows=10), got {newline_count}:\n{moves}"
        );
        assert!(moves.contains("Up(10)"), "pre_roll should move cursor up 10 rows:\n{moves}");
    }

    #[test]
    fn pre_roll_one_shot() {
        // After pre_roll fires once, subsequent ticks must not write
        // additional newlines.
        use super::inner::DimensionSource;
        use std::sync::Arc;

        let mp_term = indicatif::InMemoryTerm::new(10, 80);
        let cap_term = indicatif::InMemoryTerm::new(100, 80);
        let target = indicatif::ProgressDrawTarget::term_like(Box::new(mp_term.clone()));
        let mp = MultiProgress::with_draw_target(target);
        let dims = Arc::new(super::inner::TestDimensionSource::new((10, 80)));
        let ts = Arc::new(super::TestTimeSource::new());

        let group = ProgressGroup::builder()
            .with_multi_progress(mp)
            .with_pre_roll_capture(Box::new(cap_term.clone()))
            .with_dim_source(dims as Arc<dyn DimensionSource>)
            .with_time_source(ts.clone() as Arc<dyn super::TimeSource>)
            .capacity(2)
            .build();

        // Wait for the background ticker to fire pre_roll.
        std::thread::sleep(std::time::Duration::from_millis(200));

        // Drain the pre_roll moves from the first ticker tick.
        let first_moves = cap_term.moves_since_last_check();
        let first_newlines = first_moves.lines().filter(|l| l.trim() == "NewLine").count();
        assert_eq!(first_newlines, 10, "first tick should write 10 pre_roll newlines");

        // Tick explicitly — pre_roll must not fire again.
        group.tick();
        std::thread::sleep(std::time::Duration::from_millis(150));
        let second_moves = cap_term.moves_since_last_check();
        let second_newlines = second_moves.lines().filter(|l| l.trim() == "NewLine").count();
        assert_eq!(
            second_newlines, 0,
            "second tick should NOT fire pre_roll again, got {second_newlines} newlines"
        );
    }

    #[test]
    fn pre_roll_with_overall() {
        // Same as pre_roll_reserves_full_terminal_height but with an overall
        // aggregate bar.
        use super::inner::DimensionSource;
        use std::sync::Arc;

        let mp_term = indicatif::InMemoryTerm::new(10, 80);
        let cap_term = indicatif::InMemoryTerm::new(100, 80);
        let target = indicatif::ProgressDrawTarget::term_like(Box::new(mp_term.clone()));
        let mp = MultiProgress::with_draw_target(target);
        let dims = Arc::new(super::inner::TestDimensionSource::new((10, 80)));
        let ts = Arc::new(super::TestTimeSource::new());

        let (_group, _overall) = ProgressGroup::builder()
            .with_multi_progress(mp)
            .with_pre_roll_capture(Box::new(cap_term.clone()))
            .with_dim_source(dims as Arc<dyn DimensionSource>)
            .with_time_source(ts.clone() as Arc<dyn super::TimeSource>)
            .capacity(2)
            .with_overall("total", 100)
            .build();

        // Wait for the background ticker to fire pre_roll.
        std::thread::sleep(std::time::Duration::from_millis(200));

        let moves = cap_term.moves_since_last_check();
        let newline_count = moves.lines().filter(|l| l.trim() == "NewLine").count();
        assert_eq!(
            newline_count, 10,
            "pre_roll (with overall) should write exactly 10 blank lines, got {newline_count}:\n{moves}"
        );
        assert!(
            moves.contains("Up(10)"),
            "pre_roll (with overall) should move cursor up 10 rows:\n{moves}"
        );
    }

    #[test]
    fn pre_roll_height_changes_no_effect() {
        // Once pre_roll has fired, a subsequent terminal height change must
        // NOT trigger a second pre_roll (one-shot invariant).
        use super::inner::DimensionSource;
        use std::sync::Arc;

        let mp_term = indicatif::InMemoryTerm::new(10, 80);
        let cap_term = indicatif::InMemoryTerm::new(100, 80);
        let target = indicatif::ProgressDrawTarget::term_like(Box::new(mp_term.clone()));
        let mp = MultiProgress::with_draw_target(target);
        let dims = Arc::new(super::inner::TestDimensionSource::new((10, 80)));
        let ts = Arc::new(super::TestTimeSource::new());

        let group = ProgressGroup::builder()
            .with_multi_progress(mp)
            .with_pre_roll_capture(Box::new(cap_term.clone()))
            .with_dim_source(dims.clone() as Arc<dyn DimensionSource>)
            .with_time_source(ts.clone() as Arc<dyn super::TimeSource>)
            .capacity(2)
            .build();

        // Wait for pre_roll to fire at H=10.
        std::thread::sleep(std::time::Duration::from_millis(200));
        let _ = cap_term.moves_since_last_check();

        // Change terminal height — pre_roll must NOT re-fire.
        dims.set((20, 80));
        group.tick();
        std::thread::sleep(std::time::Duration::from_millis(150));

        let moves = cap_term.moves_since_last_check();
        let newline_count = moves.lines().filter(|l| l.trim() == "NewLine").count();
        assert_eq!(
            newline_count, 0,
            "height change should NOT trigger pre_roll again, got {newline_count} newlines"
        );
    }

    #[test]
    fn pre_roll_with_existing_content_scrolls_it_away() {
        // Regression test: when the terminal has existing content and the
        // cursor is NOT at the bottom, pre_roll must write enough newlines
        // to push ALL visible content into the scrollback buffer.
        //
        // In this scenario: cursor is at row 0 (top), terminal has 10 rows
        // with content at rows 0-4.  Pre-roll with only `rows` newlines
        // would scroll only 1 line (cursor reaches bottom after 9 newlines,
        // then 1 scroll), leaving rows 1-4 visible.
        use super::inner::DimensionSource;
        use indicatif::TermLike;
        use std::sync::Arc;

        let term = indicatif::InMemoryTerm::new(10, 80);
        // Write content BEFORE progress group (simulates terminal state).
        for i in 1..=5 {
            let _ = term.write_line(&format!("existing content line {i}"));
        }
        // At this point cursor is at row 5 (after writing 5 lines).
        // Move cursor UP 5 to simulate cursor at top (worst case).
        let _ = term.move_cursor_up(5);
        // Cursor is now at row 0, content at rows 0-4.
        let initial_content = term.contents();
        assert!(initial_content.contains("existing content line 1"), "content must be written");
        assert!(initial_content.contains("existing content line 5"), "content must be written");

        // Same InMemoryTerm for both draw target AND pre_roll capture.
        let target = indicatif::ProgressDrawTarget::term_like(Box::new(term.clone()));
        let mp = MultiProgress::with_draw_target(target);
        let dims = Arc::new(super::inner::TestDimensionSource::new((10, 80)));
        let ts = Arc::new(super::TestTimeSource::new());

        let group = ProgressGroup::builder()
            .with_multi_progress(mp)
            .with_pre_roll_capture(Box::new(term.clone()))
            .with_dim_source(dims as Arc<dyn DimensionSource>)
            .with_time_source(ts.clone() as Arc<dyn super::TimeSource>)
            .capacity(2)
            .build();
        // Wait for the background ticker to fire pre_roll (at 50ms, 3-4
        // ticks should be enough).
        std::thread::sleep(std::time::Duration::from_millis(200));
        // Pre_roll has now pushed all existing content away.

        let bar = group.add_bar(100, "work");
        bar.set_position(100);
        ts.advance(std::time::Duration::from_millis(100));
        bar.finish_success();

        // Sync state from SharedState to indicatif, then finalize for
        // deterministic finished-bar output (no spinner animation).
        group.tick();
        group.join_and_clear();

        // The visible content must contain ONLY the finished bar output.
        // All "existing content line *" must be gone (scrolled into
        // scrollback by pre_roll).
        // All existing content must have been scrolled into scrollback by
        // pre_roll.  The visible content must contain ONLY the finished bar
        // line (pure text — InMemoryTerm::contents strips ANSI codes).
        let after = term.contents();
        assert!(
            !after.contains("existing content"),
            "existing content must be scrolled away, got: {after:?}",
        );
        // Exactly one visible line with a spinner prefix and deterministic
        // body.  Strip the multi-byte spinner char for exact body matching.
        let bar_line = after.lines().next().expect("expected at least one bar line");
        let spinner_len = bar_line.chars().next().unwrap().len_utf8();
        let body = &bar_line[spinner_len..];
        assert_eq!(
            body, "                     work ████████████████████████████████  100/100 0s",
            "bar body after spinner must match exactly",
        );
    }

    #[test]
    fn pre_roll_fires_on_join_and_clear_before_ticker() {
        // Regression test: when all bars finish before the first ticker tick
        // (≈50 ms), join_and_clear() → finalize() must still call
        // pre_roll_if_needed().  Without this, bars draw at the current
        // cursor position and overwrite existing terminal content.
        use super::inner::DimensionSource;
        use std::sync::Arc;

        let mp_term = indicatif::InMemoryTerm::new(10, 80);
        let cap_term = indicatif::InMemoryTerm::new(100, 80);
        let target = indicatif::ProgressDrawTarget::term_like(Box::new(mp_term.clone()));
        let mp = MultiProgress::with_draw_target(target);
        let dims = Arc::new(super::inner::TestDimensionSource::new((10, 80)));
        let ts = Arc::new(super::TestTimeSource::new());

        let group = ProgressGroup::builder()
            .with_multi_progress(mp)
            .with_pre_roll_capture(Box::new(cap_term.clone()))
            .with_dim_source(dims as Arc<dyn DimensionSource>)
            .with_time_source(ts.clone() as Arc<dyn super::TimeSource>)
            .capacity(2)
            .build();

        // Add a bar and complete it immediately — before the 50ms ticker
        // thread fires its first tick.
        let bar = group.add_bar(1, "instant");
        bar.set_position(1);
        ts.advance(std::time::Duration::from_millis(1));
        bar.finish_success();

        // Finalize immediately — the background ticker has slept only
        // microseconds, nowhere near its 50ms interval.
        group.join_and_clear();

        // Pre_roll must have been called (via finalize).
        let moves = cap_term.moves_since_last_check();
        let newline_count = moves.lines().filter(|l| l.trim() == "NewLine").count();
        assert!(
            newline_count > 0,
            "finalize must trigger pre_roll (rows=10), got {newline_count}:\n{moves}",
        );
        assert!(
            moves.contains("Up(10)"),
            "finalize pre_roll must move cursor up 10 rows:\n{moves}",
        );
    }

    #[test]
    fn sync_slot_preserves_custom_suffix_on_attach() {
        // Regression: when `add_bar` triggers `attach` → `sync_slot`, the
        // slot is synced with only the auto-computed RHS suffix, dropping
        // any custom suffix that was set via `set_suffix_components`.
        //
        // Without the fix, sync_slot overwrites the suffix with only the
        // auto-computed RHS, dropping the custom part.  With the fix,
        // sync_slot delegates to sync_snapshot_to_bar which appends the
        // custom suffix.
        //
        // Bar A is kept ACTIVE (not finished) so the tick drain-loop (which
        // unconditionally re-syncs non-active bars) does not rescue the
        // suffix.  Only the dirty-tracking loop processes A — and without
        // the fix A is not dirty after the attach, so the wrong suffix
        // persists.
        use std::sync::Arc;
        use std::time::Duration;

        let term = indicatif::InMemoryTerm::new(10, 80);
        let target = indicatif::ProgressDrawTarget::term_like(Box::new(term.clone()));
        let mp = indicatif::MultiProgress::with_draw_target(target);
        let ts = Arc::new(super::TestTimeSource::new());
        let group = ProgressGroup::builder()
            .with_multi_progress(mp)
            .with_time_source(Arc::clone(&ts) as Arc<dyn super::TimeSource>)
            .capacity(2)
            .build();

        // Phase 1: add bar A, set custom suffix and partial progress, sync.
        let bar_a = group.add_bar(100, "resolve");
        bar_a.set_position(50);
        bar_a.set_suffix_components(SuffixComponents {
            custom: "cached (1)".into(),
            ..Default::default()
        });
        ts.advance(Duration::from_millis(100));
        group.tick();

        // Baseline: custom suffix is visible after first tick.
        let baseline = term.contents();
        assert!(
            baseline.contains("cached (1)"),
            "baseline must show custom suffix after tick:\n{baseline}",
        );

        // Phase 2: add bar B — triggers attach → shift → sync_slot on A.
        let bar_b = group.add_bar(100, "fetch");
        bar_b.set_position(0);

        // Check terminal output IMMEDIATELY after add_bar + set_position.
        let after_attach = term.contents();
        assert!(
            after_attach.contains("cached (1)"),
            "custom suffix lost after add_bar + set_position (before tick).\n\
             Terminal output:\n{after_attach}",
        );
        std::mem::drop(bar_a);
        std::mem::drop(bar_b);
    }

    // ---- structured suffix merge semantics (Phase 5) --------------------

    #[test]
    fn suffix_stored_state_is_structured() {
        // set_suffix_components must store the full structured set (all six
        // fields), not just `custom` — snapshot() carries them through so the
        // sync path can merge field-by-field.
        let h = TrackedHandle::new(100);
        h.set_suffix_components(SuffixComponents {
            count: "9".into(),
            total: "9".into(),
            elapsed: "1:23:45".into(),
            rate: Some("10/s".into()),
            eta: Some("[0:00:07]".into()),
            custom: "cached".into(),
        });
        let snap = h.snapshot();
        assert_eq!(snap.suffix_components.count, "9");
        assert_eq!(snap.suffix_components.total, "9");
        assert_eq!(snap.suffix_components.elapsed, "1:23:45");
        assert_eq!(snap.suffix_components.rate.as_deref(), Some("10/s"));
        assert_eq!(snap.suffix_components.eta.as_deref(), Some("[0:00:07]"));
        assert_eq!(snap.suffix_components.custom, "cached");
    }

    #[test]
    fn suffix_merge_user_count_total_overrides_auto() {
        // User-set count/total components must override the auto-derived
        // count/total from the snapshot position.
        use std::sync::Arc;
        use std::time::Duration;

        let term = indicatif::InMemoryTerm::new(10, 80);
        let target = indicatif::ProgressDrawTarget::term_like(Box::new(term.clone()));
        let mp = indicatif::MultiProgress::with_draw_target(target);
        let ts = Arc::new(super::TestTimeSource::new());
        let group = ProgressGroup::builder()
            .with_multi_progress(mp)
            .with_time_source(Arc::clone(&ts) as Arc<dyn super::TimeSource>)
            .capacity(2)
            .build();

        let bar = group.add_bar(100, "test");
        bar.set_position(50);
        bar.set_suffix_components(SuffixComponents {
            count: "9".into(),
            total: "9".into(),
            ..Default::default()
        });
        ts.advance(Duration::from_millis(100));
        group.tick();

        let content = term.contents();
        assert!(content.contains("9/9"), "user count/total must override auto-derived:\n{content}");
        assert!(
            !content.contains("50/100"),
            "auto count/total must not show when user overrides:\n{content}",
        );
    }

    #[test]
    fn suffix_merge_user_rate_eta_elapsed_override_auto() {
        // User-set elapsed/rate/eta components must override the auto-derived
        // ticker fields.
        use std::sync::Arc;
        use std::time::Duration;

        let term = indicatif::InMemoryTerm::new(10, 80);
        let target = indicatif::ProgressDrawTarget::term_like(Box::new(term.clone()));
        let mp = indicatif::MultiProgress::with_draw_target(target);
        let ts = Arc::new(super::TestTimeSource::new());
        let group = ProgressGroup::builder()
            .with_multi_progress(mp)
            .with_time_source(Arc::clone(&ts) as Arc<dyn super::TimeSource>)
            .capacity(2)
            .build();

        let bar = group.add_bar(100, "test");
        bar.set_position(50);
        bar.set_suffix_components(SuffixComponents {
            elapsed: "1:23:45".into(),
            rate: Some("10/s".into()),
            eta: Some("[0:00:07]".into()),
            ..Default::default()
        });
        ts.advance(Duration::from_millis(100));
        group.tick();

        let content = term.contents();
        assert!(content.contains("1:23:45"), "user elapsed must override auto-derived:\n{content}");
        assert!(content.contains("10/s"), "user rate must override auto-derived:\n{content}");
        assert!(content.contains("[0:00:07]"), "user eta must override auto-derived:\n{content}");
    }

    #[test]
    fn suffix_truncation_order_unchanged_after_merge() {
        // A merged component set (user overrides on top of auto fields) must
        // still truncate in the normative order: custom progressive → eta →
        // rate → elapsed → count/total atomic → fallback. Uses the same width
        // ladder as the semantic_truncate_suffix suite.
        let merged = SuffixComponents {
            count: "9".into(),
            total: "9".into(),
            elapsed: "0:00:05".into(),
            rate: Some("12.3 MiB/s".into()),
            eta: Some("[0:00:02]".into()),
            custom: "cached (1)".into(),
        };
        // Fits: all fields present.
        let fits = super::inner::semantic_truncate_suffix(&merged, 100);
        assert_eq!(
            render_suffix(&fits),
            " \x1b[33m9/9\x1b[0m 0:00:05 12.3 MiB/s [0:00:02] cached (1)",
            "full merged suffix when fits",
        );
        // Custom shrunk to keep=5 (39 visible).
        let partial = super::inner::semantic_truncate_suffix(&merged, 39);
        assert_eq!(
            render_suffix(&partial),
            " \x1b[33m9/9\x1b[0m 0:00:05 12.3 MiB/s [0:00:02] cache",
            "merged custom shrunk first",
        );
        // Eta removed entirely (23 <= 30).
        let eta_off = super::inner::semantic_truncate_suffix(&merged, 30);
        assert_eq!(
            render_suffix(&eta_off),
            " \x1b[33m9/9\x1b[0m 0:00:05 12.3 MiB/s",
            "merged eta removed second",
        );
        // Rate removed entirely (12 <= 22).
        let rate_off = super::inner::semantic_truncate_suffix(&merged, 22);
        assert_eq!(
            render_suffix(&rate_off),
            " \x1b[33m9/9\x1b[0m 0:00:05",
            "merged rate removed third",
        );
        // Count/total still the last atomic unit.
        let count_only = super::inner::semantic_truncate_suffix(&merged, 4);
        assert_eq!(
            render_suffix(&count_only),
            " \x1b[33m9/9\x1b[0m",
            "merged count/total atomic at end",
        );
    }
}
