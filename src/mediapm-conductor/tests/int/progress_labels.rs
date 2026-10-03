//! Client-defined bar-label truncation contract tests.
//!
//! Validates that the conductor-owned label structs implement
//! [`BarLabelTruncation`] with the correct field order: a worker bar never
//! carries a `completed/total` progress tally, while a step bar places its
//! phase, status marker, and progress tally ahead of the tool name in the
//! segment list, so they are the last to yield; the tool name is the elastic
//! segment that shrinks first.

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

    // The prefix carries the activity marker and the parenthesized tool name.
    let wide = label.truncate_prefix(80);
    assert!(wide.contains("[active]"), "worker prefix missing activity: {wide:?}");
    assert!(wide.contains("(echo@v1)"), "worker prefix missing tool: {wide:?}");
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
    // Narrow enough that the tail has to go. The activity marker leads the
    // order, so it survives and the tool name is shed.
    let tight = label.truncate_prefix(10);
    assert!(tight.contains("[active]"), "activity lost under pressure: {tight:?}");
    assert!(!tight.contains("archive"), "tool should be shed at width 10: {tight:?}");
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
    assert!(prefix.contains("[idle]"), "idle worker prefix missing activity marker: {prefix:?}");
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
    assert!(prefix.contains("[1.2.3]"), "step prefix missing version: {prefix:?}");
    assert!(prefix.contains("1/4"), "step prefix missing progress tally: {prefix:?}");
    assert!(prefix.contains("[wf]"), "step prefix missing phase: {prefix:?}");

    // The width-80 case above fits whole, so on its own it would still pass if
    // truncation were removed. At 20 the tail is dropped and the version has
    // to outrank the identifiers and the tool name to survive.
    let tight = label.truncate_prefix(20);
    assert!(tight.contains("[1.2.3]"), "version lost under truncation: {tight:?}");
    assert!(!tight.contains("echo"), "tool should be shed at width 20: {tight:?}");
}

/// Visible width of the leading `[wf] [F] 1/4` head.
const STEP_HEAD_WIDTH: usize = 12;
/// Visible width of the leading `[F] [active]` head.
const WORKER_HEAD_WIDTH: usize = 12;

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
        assert!(out.contains("[wf]"), "phase lost at width {width}: {out:?}");
        assert!(out.contains("[F]"), "status lost at width {width}: {out:?}");
        assert!(out.contains("1/4"), "tally lost at width {width}: {out:?}");
        assert!(out.chars().count() <= width, "overflowed at width {width}: {out:?}");
    }
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
        assert!(out.contains("[active]"), "activity lost at width {width}: {out:?}");
        assert!(out.chars().count() <= width, "overflowed at width {width}: {out:?}");
    }
}

#[test]
fn step_label_shrinks_tool_before_dropping_version() {
    // Under width pressure the elastic tool name is shortened first; only
    // once it cannot shrink further is the version dropped. The leading
    // head never yields.
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
    let tight = label.truncate_prefix(20);
    assert!(tight.contains("[wf]"), "phase lost: {tight:?}");
    assert!(tight.contains("1/4"), "tally lost: {tight:?}");
    assert!(tight.contains("[9.9.9]"), "version dropped too early: {tight:?}");
    assert!(!tight.contains("a-very-long-tool-name"), "tool not shortened: {tight:?}");
    assert!(tight.chars().count() <= 20, "overflowed: {tight:?}");
}

#[test]
fn worker_label_front_ellipsises_very_long_tool_name() {
    // A tool name longer than the budget is clipped to its tail, so the
    // leading activity marker survives and the row ends in `limit)`, the end
    // of the tool name. This is the behaviour change from the old prefix
    // cut, which kept the head `(a-very-long-t` and discarded the whole
    // activity marker.
    let label = WorkerBarLabel {
        status_marker: String::new(),
        workflow_id: String::new(),
        step_id: String::new(),
        tool: "a-very-long-tool-name-that-exceeds-the-maximum-width-limit".into(),
        activity: "active".into(),
    };
    let tight = label.truncate_prefix(15);
    assert_eq!(tight, "[active] limit)");
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
fn step_label_front_ellipsises_very_long_tool_name() {
    // With every head field empty the tool name is the only segment.
    // It must be clipped from the front, keeping the tail, not cut from
    // the front as the old implementation did.
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
    // A step bar carries its own progress tally in the label struct, and the
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
