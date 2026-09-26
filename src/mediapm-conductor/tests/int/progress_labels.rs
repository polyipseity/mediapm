//! Client-defined bar-label truncation contract tests.
//!
//! Validates that the conductor-owned label structs implement
//! [`BarLabelTruncation`] with the correct field order: a worker bar never
//! carries a `completed/total` progress tally, while a step bar protects
//! its phase, status marker, and progress tally ahead of the tool name,
//! which is the elastic segment that shrinks first.

use mediapm_conductor::orchestration::progress_labels::{StepBarLabel, WorkerBarLabel};
use mediapm_utils::progress::BarLabelTruncation;

#[test]
fn worker_label_truncate_drops_completed_total_first() {
    let label = WorkerBarLabel {
        status_marker: String::new(),
        workflow_id: "wf".into(),
        step_id: "s1".into(),
        tool: "echo@v1".into(),
        activity: "active".into(),
    };
    // A worker bar has no progress tally by construction; the prefix must
    // never contain a `c/t` segment even at a tight width.
    let tight = label.truncate_prefix(8);
    assert!(!tight.contains('/'), "worker prefix must not contain c/t: {tight:?}");
    // At a comfortable width the activity marker is present and the tool
    // name is parenthesized.
    let wide = label.truncate_prefix(80);
    assert!(wide.contains("[active]"), "worker prefix missing activity: {wide:?}");
    assert!(wide.contains("(echo@v1)"), "worker prefix missing tool: {wide:?}");
}

#[test]
fn worker_label_idle_has_no_activity_marker() {
    let label = WorkerBarLabel {
        status_marker: String::new(),
        workflow_id: String::new(),
        step_id: String::new(),
        tool: "idle".into(),
        activity: "idle".into(),
    };
    let prefix = label.truncate_prefix(40);
    assert!(prefix.contains("idle"), "idle worker prefix missing label: {prefix:?}");
    assert!(!prefix.contains('/'), "idle worker prefix must not contain c/t: {prefix:?}");
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
}

#[test]
fn step_label_protected_head_survives_at_the_floor() {
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
    for width in StepBarLabel::PREFIX_FLOOR..=20 {
        let out = label.truncate_prefix(width);
        assert!(out.contains("[wf]"), "phase lost at width {width}: {out:?}");
        assert!(out.contains("[F]"), "status lost at width {width}: {out:?}");
        assert!(out.contains("1/4"), "tally lost at width {width}: {out:?}");
        assert!(out.chars().count() <= width, "overflowed at width {width}: {out:?}");
    }
}

#[test]
fn worker_label_protected_head_survives_at_the_floor() {
    let label = WorkerBarLabel {
        status_marker: "F".into(),
        workflow_id: "wf".into(),
        step_id: "s1".into(),
        tool: "echo@v1".into(),
        activity: "active".into(),
    };
    for width in WorkerBarLabel::PREFIX_FLOOR..=20 {
        let out = label.truncate_prefix(width);
        assert!(out.contains("[F]"), "status lost at width {width}: {out:?}");
        assert!(out.contains("[active]"), "activity lost at width {width}: {out:?}");
        assert!(out.chars().count() <= width, "overflowed at width {width}: {out:?}");
    }
}

#[test]
fn step_label_shrinks_tool_before_dropping_version() {
    // Under width pressure the elastic tool name is shortened first; only
    // once it cannot shrink further is the version dropped. The protected
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
    // A tool name longer than the budget is shortened from the front, so
    // the protected activity marker survives and the tool's tail is kept.
    // This is the behaviour change from the old prefix cut, which kept the
    // head `(a-very-long-t` and discarded the whole protected head.
    let label = WorkerBarLabel {
        status_marker: String::new(),
        workflow_id: String::new(),
        step_id: String::new(),
        tool: "a-very-long-tool-name-that-exceeds-the-maximum-width-limit".into(),
        activity: "active".into(),
    };
    let tight = label.truncate_prefix(15);
    assert_eq!(tight, "[active] …imit)");
}

#[test]
fn step_label_front_ellipsises_very_long_tool_name() {
    // With every protected field empty the tool name is the only segment.
    // It must be shortened from the front, keeping the tail, not cut from
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
    assert!(tight.starts_with('…'), "not front-ellipsised: {tight:?}");
    assert!(tight.ends_with("limit)"), "tail not preserved: {tight:?}");
    assert_eq!(tight.chars().count(), 20);
}
