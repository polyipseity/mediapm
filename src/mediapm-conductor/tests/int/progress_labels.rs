//! Client-defined bar-label truncation contract tests.
//!
//! Validates that the conductor-owned label structs implement
//! [`BarLabelTruncation`] with the correct field order: a worker bar never
//! carries a `completed`/`total` progress tally, while a step bar carries that
//! tally in the suffix only, so the prefix holds names and the suffix holds
//! counts. Both labels lead with the status marker and end with the phase or
//! activity marker, which makes that trailing marker the first thing width
//! pressure sheds. The tool name is the elastic segment, so it shortens before
//! anything is dropped whole.

use mediapm_conductor::orchestration::progress_labels::{StepBarLabel, WorkerBarLabel};
use mediapm_utils::progress::{BarLabelTruncation, SuffixComponents};

#[test]
fn worker_label_ignores_a_populated_tally_in_the_suffix() {
    let label = WorkerBarLabel {
        status_marker: String::new(),
        workflow_id: "wf".into(),
        step_id: "s1".into(),
        tool: "echo@v1".into(),
        activity: "active".into(),
    };
    // A worker slot has no tally of its own. The renderer still offers one in
    // the suffix components, so the label has to drop it deliberately rather
    // than merely lack the field: this fails if `suffix_segments` ever starts
    // emitting count/total.
    let suffix = SuffixComponents {
        count: "3".into(),
        total: "10".into(),
        elapsed: "2m 05s".into(),
        ..Default::default()
    };
    let out = label.truncate_suffix(80, &suffix);
    assert!(!out.contains("3/10"), "worker suffix must not carry a tally: {out:?}");
    assert!(out.contains("2m 05s"), "worker suffix lost elapsed: {out:?}");

    // The prefix names the workflow, the step, and the tool, then closes with
    // the activity marker.
    let wide = label.truncate_prefix(80);
    assert_eq!(wide, "wf s1 (echo@v1) [active]");
}

#[test]
fn worker_label_tight_prefix_keeps_the_activity_marker() {
    let label = WorkerBarLabel {
        status_marker: String::new(),
        workflow_id: "wf".into(),
        step_id: "s1".into(),
        tool: "mediapm-conductor-builtin-archive".into(),
        activity: "active".into(),
    };
    // Narrow enough that the tool name has to go. Its tail holds no boundary
    // this narrow, so `fit_segments` drops it whole rather than rendering a
    // fragment, and what is left is the identifiers and the marker.
    let tight = label.truncate_prefix(14);
    assert!(tight.contains("[active]"), "activity lost under pressure: {tight:?}");
    assert!(!tight.contains("archive"), "tool should be shed at width 14: {tight:?}");
}

#[test]
fn worker_label_idle_renders_its_activity_marker() {
    let label = WorkerBarLabel {
        status_marker: String::new(),
        workflow_id: String::new(),
        step_id: String::new(),
        tool: "archive".into(),
        activity: "idle".into(),
    };
    // The tool name is deliberately not "idle", so it cannot stand in for the
    // marker; the assertion is on the bracketed form the label renders.
    let prefix = label.truncate_prefix(40);
    assert_eq!(prefix, "(archive) [idle]");
}

#[test]
fn step_label_truncate_keeps_version() {
    let label = StepBarLabel {
        status_marker: String::new(),
        workflow_id: "wf".into(),
        step_id: "s1".into(),
        tool: "echo@v1".into(),
        version: "1.2.3".into(),
        phase: "wf".into(),
        completed: "1".into(),
        total: "4".into(),
    };
    let prefix = label.truncate_prefix(80);
    assert_eq!(prefix, "wf s1 (echo@v1) [1.2.3] [wf]");

    // The width-80 case above fits whole, so on its own it would still pass if
    // truncation were removed. At 20 the tool name is shed and the version and
    // phase have to outrank the tool name to survive.
    let tight = label.truncate_prefix(20);
    assert!(tight.contains("[1.2.3]"), "version lost under truncation: {tight:?}");
    assert!(tight.contains("[wf]"), "phase lost under truncation: {tight:?}");
    assert!(!tight.contains("echo"), "tool should be shed at width 20: {tight:?}");
}

/// Visible width of the leading `[F] wf s1` head a step bar shares with a
/// worker bar.
const STEP_HEAD_WIDTH: usize = 9;
/// Visible width of the leading `[F] wf s1` head on a worker bar.
const WORKER_HEAD_WIDTH: usize = 9;

#[test]
fn step_label_head_survives_across_narrow_widths() {
    let label = StepBarLabel {
        status_marker: "F".into(),
        workflow_id: "wf".into(),
        step_id: "s1".into(),
        tool: "echo@v1".into(),
        version: "9.9.9".into(),
        phase: "wf".into(),
        completed: "1".into(),
        total: "4".into(),
    };
    for width in STEP_HEAD_WIDTH..=20 {
        let out = label.truncate_prefix(width);
        assert!(out.contains("[F]"), "status lost at width {width}: {out:?}");
        assert!(out.contains("wf"), "workflow lost at width {width}: {out:?}");
        assert!(out.contains("s1"), "step lost at width {width}: {out:?}");
        assert!(out.chars().count() <= width, "overflowed at width {width}: {out:?}");
    }
}

#[test]
fn step_label_sheds_the_phase_before_the_version() {
    // The phase tag trails the version, and `fit_segments` drops from the
    // tail, so the phase is what goes when the two no longer both fit. This is
    // the order tool-sync already uses for its own phase tag.
    let label = StepBarLabel {
        status_marker: String::new(),
        workflow_id: "wf".into(),
        step_id: "s1".into(),
        tool: "echo@v1".into(),
        version: "9.9.9".into(),
        phase: "wf".into(),
        completed: "1".into(),
        total: "4".into(),
    };
    assert_eq!(label.truncate_prefix(18), "wf s1 [9.9.9] [wf]");
    assert_eq!(label.truncate_prefix(17), "wf s1 [9.9.9]");
}

#[test]
fn worker_label_head_survives_across_narrow_widths() {
    let label = WorkerBarLabel {
        status_marker: "F".into(),
        workflow_id: "wf".into(),
        step_id: "s1".into(),
        tool: "echo@v1".into(),
        activity: "active".into(),
    };
    for width in WORKER_HEAD_WIDTH..=20 {
        let out = label.truncate_prefix(width);
        assert!(out.contains("[F]"), "status lost at width {width}: {out:?}");
        assert!(out.contains("wf"), "workflow lost at width {width}: {out:?}");
        assert!(out.contains("s1"), "step lost at width {width}: {out:?}");
        assert!(out.chars().count() <= width, "overflowed at width {width}: {out:?}");
    }
}

#[test]
fn worker_label_sheds_the_activity_marker_before_the_identifiers() {
    // The activity marker trails the identifiers, so it is the first segment
    // to go once the row narrows past the head.
    let label = WorkerBarLabel {
        status_marker: "F".into(),
        workflow_id: "wf".into(),
        step_id: "s1".into(),
        tool: "echo@v1".into(),
        activity: "active".into(),
    };
    assert_eq!(label.truncate_prefix(18), "[F] wf s1 [active]");
    assert_eq!(label.truncate_prefix(17), "[F] wf s1");
}

#[test]
fn step_label_renders_the_tally_once_and_only_in_the_suffix() {
    // Names live in the prefix and counts live in the suffix, so the tally
    // leaves the prefix entirely. The renderer offers its own count and total
    // in the suffix components, and the label renders its own tally from its
    // own fields, so a label that echoed the renderer's count as well would
    // put the same figure on the row twice.
    let label = StepBarLabel {
        status_marker: String::new(),
        workflow_id: "wf".into(),
        step_id: "s1".into(),
        tool: "echo@v1".into(),
        version: "7.1".into(),
        phase: "wf".into(),
        completed: "1".into(),
        total: "3".into(),
    };
    let suffix = SuffixComponents {
        count: "1".into(),
        total: "3".into(),
        elapsed: "23s".into(),
        ..Default::default()
    };
    let prefix = label.truncate_prefix(80);
    assert!(!prefix.contains("1/3"), "tally must not render in the prefix: {prefix:?}");
    let out = label.truncate_suffix(80, &suffix);
    assert_eq!(out.matches("1/3").count(), 1, "tally rendered more than once: {out:?}");
    assert!(out.ends_with("1/3 23s"), "tally should lead the suffix: {out:?}");
}

#[test]
fn step_label_shrinks_tool_before_dropping_version() {
    // Under width pressure the elastic tool name is shortened first; only
    // once it cannot shrink further is it dropped whole. The version and the
    // phase both outrank the tool name, so both are still here after the tool
    // has given back columns.
    let label = StepBarLabel {
        status_marker: String::new(),
        workflow_id: "wf".into(),
        step_id: "s1".into(),
        tool: "a-very-long-tool-name@v1".into(),
        version: "9.9.9".into(),
        phase: "wf".into(),
        completed: "1".into(),
        total: "4".into(),
    };
    let tight = label.truncate_prefix(30);
    assert!(!tight.contains("a-very-long-tool-name"), "tool not shortened: {tight:?}");
    assert_eq!(tight, "wf s1 name@v1) [9.9.9] [wf]");
}

#[test]
fn worker_label_clips_a_very_long_tool_name_from_the_front() {
    // A tool name longer than the budget is clipped to its tail, so the row
    // keeps the informative end of the name and still closes with the activity
    // marker rather than dropping it.
    let label = WorkerBarLabel {
        status_marker: String::new(),
        workflow_id: String::new(),
        step_id: String::new(),
        tool: "a-very-long-tool-name-that-exceeds-the-maximum-width-limit".into(),
        activity: "active".into(),
    };
    let tight = label.truncate_prefix(20);
    assert_eq!(tight, "limit) [active]");
}

#[test]
fn worker_suffix_keeps_auto_derived_timing_fields() {
    // A worker slot has no progress tally of its own, but the renderer
    // still supplies auto-derived elapsed, rate, and ETA in the suffix
    // argument it passes to `truncate_suffix`. Those fields must survive:
    // dropping them would silently remove timing information from every
    // worker bar, which no other test in this file would catch.
    let label = WorkerBarLabel {
        status_marker: String::new(),
        workflow_id: String::new(),
        step_id: String::new(),
        tool: String::new(),
        activity: "active".into(),
    };
    let suffix = SuffixComponents {
        elapsed: "1m 30s".into(),
        rate: Some("2.1 MiB/s".into()),
        eta: Some("00:45".into()),
        custom: String::new(),
        ..Default::default()
    };
    let out = label.truncate_suffix(80, &suffix);
    assert!(out.contains("1m 30s"), "elapsed lost: {out:?}");
    assert!(out.contains("2.1 MiB/s"), "rate lost: {out:?}");
    assert!(out.contains("00:45"), "eta lost: {out:?}");
}

#[test]
fn step_label_clips_a_very_long_tool_name_from_the_front() {
    // With every other field empty the tool name is the only segment, so the
    // clip has nothing to fall back on and the result is the bare tail of the
    // name at exactly the budget.
    let label = StepBarLabel {
        status_marker: String::new(),
        workflow_id: String::new(),
        step_id: String::new(),
        tool: "extremely-long-tool-name-that-exceeds-the-maximum-width-limit".into(),
        version: String::new(),
        phase: String::new(),
        completed: String::new(),
        total: String::new(),
    };
    let tight = label.truncate_prefix(20);
    assert_eq!(tight, "maximum-width-limit)");
    assert_eq!(tight.chars().count(), 20);
}

#[test]
fn step_suffix_keeps_the_leading_tally_and_timing_fields() {
    // The tally is the step bar's only count and renders in the suffix, and the
    // renderer separately supplies auto-derived elapsed, rate and ETA in the
    // suffix argument. Both must reach the rendered suffix: the timing fields
    // reach it only through that argument, so a `suffix_segments` that stopped
    // reading them would silently strip timing from every step bar, and no
    // other test in this file would catch it.
    let label = StepBarLabel {
        status_marker: String::new(),
        workflow_id: String::new(),
        step_id: String::new(),
        tool: String::new(),
        version: String::new(),
        phase: String::new(),
        completed: "3".into(),
        total: "10".into(),
    };
    let suffix = SuffixComponents {
        elapsed: "2m 05s".into(),
        rate: Some("1.4 MiB/s".into()),
        eta: Some("01:12".into()),
        ..Default::default()
    };

    // Wide enough for every field: all four must be present.
    let wide = label.truncate_suffix(80, &suffix);
    assert!(wide.contains("3/10"), "tally lost: {wide:?}");
    assert!(wide.contains("2m 05s"), "elapsed lost: {wide:?}");
    assert!(wide.contains("1.4 MiB/s"), "rate lost: {wide:?}");
    assert!(wide.contains("01:12"), "eta lost: {wide:?}");

    // Too narrow for all four. Segments yield from the tail, so the tally
    // leads the order and is the last one standing; the timing fields are
    // surrendered whole rather than clipped, because none of them is elastic.
    let tight = label.truncate_suffix(10, &suffix);
    assert_eq!(tight, "3/10", "tight case must keep the leading tally whole: {tight:?}");
}
