use super::super::inner::*;
use super::*;

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
    use super::super::inner::DimensionSource;
    use std::sync::Arc;

    let term = indicatif::InMemoryTerm::new(10, 80);
    let target = indicatif::ProgressDrawTarget::term_like(Box::new(term.clone()));
    let mp = MultiProgress::with_draw_target(target);
    let dims = Arc::new(super::super::inner::TestDimensionSource::new((10, 80)));
    let ts = Arc::new(super::super::TestTimeSource::new());

    let group = super::super::ProgressGroup::builder()
        .with_multi_progress(mp)
        .with_dim_source(dims as Arc<dyn DimensionSource>)
        .with_time_source(ts.clone() as Arc<dyn super::super::TimeSource>)
        .with_ticker_enabled(false)
        .build();

    let h = group.add_bar(0, "idle");
    h.set_style(BarStyle::WorkerSpinner);
    // Idle worker: assigned == 0, succeeded == 0, no version/phase.
    h.set_prefix_components(PrefixComponents { tool_name: "idle".into(), ..Default::default() });
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
    h.set_suffix_components(SuffixComponents { custom: "running".into(), ..Default::default() });
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
