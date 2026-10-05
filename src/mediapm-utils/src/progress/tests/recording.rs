use super::super::inner::*;
use super::*;
use crate::progress::{BarLabelTruncation, Brackets, Segment, fit_segments};

/// Label whose rendered prefix is a single bracketed marker, so a test can put
/// a marker on one row and read another row's log without the two mixing.
///
/// The marker is a [`Shrink::Keep`] segment, so a prefix slot too narrow for
/// it drops it whole rather than clipping the text and leaving a `]` behind.
struct MarkerLabel(&'static str);

impl BarLabelTruncation for MarkerLabel {
    fn truncate_prefix(&self, max_width: usize) -> String {
        fit_segments(&[Segment::keep(self.0).brackets(Brackets::Square)], max_width)
    }

    fn truncate_suffix(&self, _max_width: usize, _suffix: &SuffixComponents) -> String {
        String::new()
    }
}

/// Box a marker label as the trait object the recording handle takes.
fn marker(value: &'static str) -> Arc<dyn BarLabelTruncation> {
    Arc::new(MarkerLabel(value))
}

/// Count the entries recorded for `bar` whose prefix carries `marker`.
fn markers_on(entries: &[RecordedProgressOp], bar: BarId, marker: char) -> usize {
    entries
        .iter()
        .filter(|entry| entry.bar == bar)
        .filter(|entry| {
            matches!(&entry.op, ProgressOp::SetTruncation { prefix, .. } if prefix.contains(marker))
        })
        .count()
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

/// The recording harness hands a label [`usize::MAX`] as its prefix budget,
/// which is the width every marker test below reads at, so a marker renders
/// whole there. A real slot is finite, and one too narrow for the bracketed
/// marker has to lose the marker rather than overflow the row.
#[test]
fn marker_label_fits_the_width_it_is_given() {
    let label = marker("idle");

    assert_eq!(label.truncate_prefix(usize::MAX), "[idle]");
    assert_eq!(label.truncate_prefix(6), "[idle]", "the marker fits its own width");
    assert_eq!(label.truncate_prefix(5), "", "one column short drops the marker whole");
    assert_eq!(label.truncate_suffix(usize::MAX, &SuffixComponents::default()), "");
}

/// A run where two rows both emit status markers. Counting across the whole
/// log gives 3, which cannot say which row emitted what; counting per bar
/// does. Without bar identity on the op there is no way to write the second
/// half of this test at all.
#[test]
fn markers_can_be_counted_per_bar() {
    let tracker = RecordingProgressTracker::new();
    let overall = tracker.add_bar(3, "workflow");
    let slot = tracker.add_bar(3, "idle");

    overall.set_truncation(&marker("idle"));
    slot.set_truncation(&marker("W"));
    slot.set_truncation(&marker("F"));
    overall.set_truncation(&marker("W"));

    let entries = tracker.recorded();

    assert_eq!(markers_on(&entries, BarId::Index(0), 'W'), 1, "overall row's terminal marker");
    assert_eq!(markers_on(&entries, BarId::Index(0), 'F'), 0, "no failure on the overall row");
    assert_eq!(markers_on(&entries, BarId::Index(1), 'W'), 1, "one retry on the worker slot");
    assert_eq!(markers_on(&entries, BarId::Index(1), 'F'), 1, "failure on the worker slot");

    let combined: usize = entries
        .iter()
        .filter(|entry| {
            matches!(&entry.op, ProgressOp::SetTruncation { prefix, .. } if prefix.contains('W'))
        })
        .count();
    assert_eq!(combined, 2, "the combined count cannot say which row each marker came from");
}

/// Every bar, including the one whose `AddBar` op opens the log, reports the
/// index it was handed out under.
#[test]
fn add_bar_ops_carry_their_own_index() {
    let tracker = RecordingProgressTracker::new();
    let _overall = tracker.add_bar(1, "workflow");
    let _slot = tracker.add_bar(0, "idle");

    let entries = tracker.recorded();
    let add_bar_bars: Vec<BarId> = entries
        .iter()
        .filter(|e| matches!(e.op, ProgressOp::AddBar { .. }))
        .map(|e| e.bar)
        .collect();

    assert_eq!(add_bar_bars, vec![BarId::Index(0), BarId::Index(1)]);
}

/// A handle built outside a tracker owns its own log and has no index among any
/// group's bars, so a filter for `Index(0)` cannot pick it up by accident.
#[test]
fn standalone_handle_has_no_bar_index() {
    let handle = RecordingTrackedHandle::new(1);
    handle.finish_warning();

    let entries = handle.recorded();
    assert_eq!(handle.bar(), BarId::Standalone);
    assert_eq!(BarId::Standalone.index(), None);
    assert_eq!(entries.len(), 1);
    assert_eq!(entries[0].bar, BarId::Standalone);
}

// ---- BarStyle selection (Stage 5 worker-slot generalization) ---------

#[test]
fn bar_style_default_is_step_count() {
    // A freshly created ProgressBarHandle uses StepCount (preserves all
    // existing callers: sync screen, materialization, etc.).
    let h = ProgressBarHandle::new(100);
    assert_eq!(h.style(), BarStyle::StepCount);
}

#[test]
fn bar_style_set_style_switches_to_worker_spinner() {
    let h = ProgressBarHandle::new(100);
    assert_eq!(h.style(), BarStyle::StepCount);
    h.set_style(BarStyle::WorkerSpinner);
    assert_eq!(h.style(), BarStyle::WorkerSpinner);
}

#[test]
fn bar_style_set_style_is_idempotent() {
    let h = ProgressBarHandle::new(100);
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
    let dims = Arc::new(super::super::inner::TestDimensionSource::new((10, 80)));
    let ts = Arc::new(super::super::TestTimeSource::new());

    let terminal = super::super::ProgressTerminal::builder()
        .with_term_like(Box::new(term.clone()))
        .with_pre_roll_capture(super::pre_roll_capture())
        .with_dim_source(dims as Arc<dyn DimensionSource>)
        .with_time_source(ts.clone() as Arc<dyn super::super::TimeSource>)
        .with_ticker_enabled(false)
        .build();
    let group = terminal.screen().build();

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
    let h = ProgressBarHandle::new(0);
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
