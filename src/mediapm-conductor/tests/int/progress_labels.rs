//! Client-defined bar-label truncation contract tests.
//!
//! Validates that the conductor-owned label structs implement
//! [`BarLabelTruncation`] with the correct field order: a worker bar never
//! carries a `completed/total` progress tally, while a step bar keeps its
//! `version` segment longer than the tool name under width pressure.

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
fn step_label_truncate_keeps_version_over_tool() {
    // Under width pressure the version segment survives after the tool name
    // is dropped (version precedes tool in the step order).
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
    assert!(tight.contains("[9.9.9]"), "version dropped before tool: {tight:?}");
    assert!(!tight.contains("a-very-long-tool-name"), "tool name should drop first: {tight:?}");
}

#[test]
fn worker_label_very_long_tool_name_hard_truncates() {
    // When the first (tool) part alone exceeds max_width, it must be
    // hard-truncated to fit rather than producing an empty prefix.
    let label = WorkerBarLabel {
        status_marker: String::new(),
        workflow_id: String::new(),
        step_id: String::new(),
        tool: "a-very-long-tool-name-that-exceeds-the-maximum-width-limit".into(),
        activity: "active".into(),
    };
    let tight = label.truncate_prefix(15);
    assert!(!tight.is_empty(), "prefix must not be empty: {tight:?}");
    assert!(tight.len() <= 15, "prefix must fit: {:?} (len={})", tight, tight.len());
    assert!(
        tight.starts_with("(a-very-long-t"),
        "prefix should start with truncated first part: {tight:?}"
    );
}

#[test]
fn step_label_very_long_tool_name_hard_truncates() {
    // The step prefix order is version → completed/total → phase → marker
    // → workflow → step → tool.  When tool is the first part AND exceeds
    // max_width (e.g. when version/completed/phase are all empty), it must
    // be hard-truncated.
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
    assert!(!tight.is_empty(), "prefix must not be empty: {tight:?}");
    assert!(tight.len() <= 20, "prefix must fit: {:?} (len={})", tight, tight.len());
    assert!(
        tight.starts_with("(extremely-long-tool"),
        "prefix should start with truncated first part: {tight:?}"
    );
}
