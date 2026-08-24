//! Workflow screen progress tests — validates the `[wf]` progress bars
//! emitted by the conductor coordinator via `RecordingProgressTracker`.
//!
//! Every assertion uses exact `assert_eq!` on recorded `ProgressOp` sequences.
//! Tests are gated on `#[cfg(feature = "progress")]` for parity with the
//! feature-gated production code path.

use std::collections::BTreeMap;
use std::sync::Arc;

use crate::{TestConductor, doc_with_workflows, echo_tool};
use mediapm_conductor::api::RunWorkflowOptions;
use mediapm_conductor::{ToolKindSpec, ToolRuntime, WorkflowSpec, WorkflowStepSpec};
use mediapm_utils::progress::recording::{ProgressOp, RecordingProgressTracker};

// ---------------------------------------------------------------------------
// Helpers
// ---------------------------------------------------------------------------

/// Pins the worker pool size so progress-bar assertions are deterministic.
///
/// # Safety
///
/// Sets a process-wide env var; only safe when all concurrent tests agree on
/// the same value.
fn fix_worker_pool_size() {
    // SAFETY: all tests in this module expect pool_size = 2.
    unsafe {
        std::env::set_var("MEDIAPM_CONDUCTOR_WORKER_POOL_SIZE", "2");
    }
}

/// Creates a step with the given id (overrides `echo_step`'s fixed `"s1"`).
fn step(id: &str, tool: &str, text: &str) -> WorkflowStepSpec {
    WorkflowStepSpec {
        id: id.into(),
        tool: tool.into(),
        inputs: BTreeMap::from([("text".into(), text.into())]),
        outputs: BTreeMap::new(),
        max_retries: 0,
        depends_on: Vec::new(),
    }
}

/// Creates a step with a dependency on another step.
fn step_depends(id: &str, tool: &str, text: &str, depends_on: &[&str]) -> WorkflowStepSpec {
    WorkflowStepSpec {
        id: id.into(),
        tool: tool.into(),
        inputs: BTreeMap::from([("text".into(), text.into())]),
        outputs: BTreeMap::new(),
        max_retries: 0,
        depends_on: depends_on.iter().map(ToString::to_string).collect(),
    }
}

/// Creates a `ToolSpec` with a `builtin_id` that is not registered in the
/// step worker — the worker will return an error during dispatch.
fn broken_tool(name: &str) -> mediapm_conductor::ToolSpec {
    mediapm_conductor::ToolSpec {
        kind: ToolKindSpec::Builtin { builtin_id: "nonexistent-builtin@v1".to_string() },
        name: name.into(),
        inputs: BTreeMap::new(),
        default_inputs: BTreeMap::new(),
        outputs: BTreeMap::new(),
        runtime: ToolRuntime::default(),
    }
}

/// Idle worker-slot prefix components (no step assigned yet).
fn idle_pc() -> ProgressOp {
    ProgressOp::SetPrefixComponents {
        marker: String::new(),
        tool_name: "idle".into(),
        version: String::new(),
        phase: String::new(),
        count: String::new(),
        total: String::new(),
    }
}

/// Idle worker-slot prefix components after `succeeded`/`assigned` steps.
fn idle_pc_count(succeeded: usize, assigned: usize) -> ProgressOp {
    ProgressOp::SetPrefixComponents {
        marker: String::new(),
        tool_name: "idle".into(),
        version: String::new(),
        phase: String::new(),
        count: succeeded.to_string(),
        total: assigned.to_string(),
    }
}

/// Dispatch prefix components for a step assigned to a worker slot.
fn dispatch_pc(tool_name: &str, assigned: usize) -> ProgressOp {
    ProgressOp::SetPrefixComponents {
        marker: String::new(),
        tool_name: tool_name.into(),
        version: String::new(),
        phase: String::new(),
        count: assigned.to_string(),
        total: assigned.to_string(),
    }
}

/// Runs `workflow_name` with a recording progress group and returns the
/// tracker plus the run summary.
async fn run_with_progress(
    tc: &TestConductor,
    workflow_name: &str,
) -> (RecordingProgressTracker, mediapm_conductor::RunSummary) {
    let (tracker, overall) = RecordingProgressTracker::with_overall("workflow [wf]", 1);
    let summary = tc
        .conductor()
        .run_workflow(
            workflow_name,
            RunWorkflowOptions {
                progress_group: Some(Arc::new(tracker.clone())),
                overall_bar: Some(Arc::new(overall)),
                ..Default::default()
            },
        )
        .await
        .expect("workflow");
    (tracker, summary)
}

// ---------------------------------------------------------------------------
// Tests
// ---------------------------------------------------------------------------

/// Single-step echo workflow emits the expected `[wf]` progress sequence:
/// overall bar (total set by coordinator) → per-worker idle bars → step bar
/// via worker-0 slot → advance → finish → overall advance → finish.
#[tokio::test]
async fn single_step_success_progress_ops() {
    fix_worker_pool_size();
    let tc = TestConductor::new();
    tc.write_config(doc_with_workflows(
        BTreeMap::from([("echo@v1".into(), echo_tool("echo@v1"))]),
        vec![crate::echo_workflow("default", "echo@v1", "hello")],
    ));

    let (tracker, summary) = run_with_progress(&tc, "default").await;
    assert_eq!(summary.total_steps, 1);
    assert_eq!(summary.executed_steps, 1);
    assert_eq!(summary.failed_steps, 0);

    assert_eq!(
        tracker.ops(),
        vec![
            // Overall bar created by with_overall(), total set to real step count.
            ProgressOp::AddBar { total: 1, label: "workflow [wf]".into() },
            ProgressOp::SetTotal { total: 1 },
            // Per-worker idle bars (2 workers).
            ProgressOp::AddBar { total: 0, label: "idle [wf]".into() },
            idle_pc(),
            ProgressOp::AddBar { total: 0, label: "idle [wf]".into() },
            idle_pc(),
            // Dispatch s1 to worker-0 (consume idle bar).
            dispatch_pc("default/s1 (echo@v1)", 1),
            ProgressOp::SetTotal { total: 1 },
            // Step completes — idle count1/total1, advance, finish success.
            idle_pc_count(1, 1),
            ProgressOp::SetTotal { total: 1 },
            ProgressOp::Advance { delta: 1 },
            ProgressOp::FinishSuccess,
            // Overall bar advances once per terminal step.
            ProgressOp::Advance { delta: 1 },
            // Overall bar finished.
            ProgressOp::FinishSuccess,
        ],
    );
}

/// Two independent steps at the same level — each worker's idle bar is
/// consumed by its assigned step, then both complete.
#[tokio::test]
async fn two_step_same_level_success_progress_ops() {
    fix_worker_pool_size();
    let tc = TestConductor::new();
    tc.write_config(doc_with_workflows(
        BTreeMap::from([("echo@v1".into(), echo_tool("echo@v1"))]),
        vec![WorkflowSpec {
            name: "default".into(),
            display_name: None,
            description: None,
            impure: false,
            steps: vec![step("s1", "echo@v1", "a"), step("s2", "echo@v1", "b")],
        }],
    ));

    let (tracker, summary) = run_with_progress(&tc, "default").await;
    assert_eq!(summary.total_steps, 2);
    assert_eq!(summary.executed_steps, 2);
    assert_eq!(summary.failed_steps, 0);

    assert_eq!(
        tracker.ops(),
        vec![
            // Overall bar created by with_overall(), total set by coordinator.
            ProgressOp::AddBar { total: 1, label: "workflow [wf]".into() },
            ProgressOp::SetTotal { total: 2 },
            // Per-worker idle bars (2 workers).
            ProgressOp::AddBar { total: 0, label: "idle [wf]".into() },
            idle_pc(),
            ProgressOp::AddBar { total: 0, label: "idle [wf]".into() },
            idle_pc(),
            // Dispatch s1 to worker-0 (consume idle bar).
            dispatch_pc("default/s1 (echo@v1)", 1),
            ProgressOp::SetTotal { total: 1 },
            // Dispatch s2 to worker-1 (consume idle bar).
            dispatch_pc("default/s2 (echo@v1)", 1),
            ProgressOp::SetTotal { total: 1 },
            // Step 1 completes.
            idle_pc_count(1, 1),
            ProgressOp::SetTotal { total: 1 },
            ProgressOp::Advance { delta: 1 },
            ProgressOp::FinishSuccess,
            ProgressOp::Advance { delta: 1 },
            // Step 2 completes.
            idle_pc_count(1, 1),
            ProgressOp::SetTotal { total: 1 },
            ProgressOp::Advance { delta: 1 },
            ProgressOp::FinishSuccess,
            ProgressOp::Advance { delta: 1 },
            // Overall bar finished.
            ProgressOp::FinishSuccess,
        ],
    );
}

/// A workflow with three independent steps at the same level — verifies
/// correct `count`/`total` progression and that all steps finish before
/// the overall bar.  With `pool_size=2`, the third step reuses worker-0's
/// slot, so its dispatch/terminal counts advance to 2.
#[tokio::test]
async fn three_step_same_level_progress_ops() {
    fix_worker_pool_size();
    let tc = TestConductor::new();
    tc.write_config(doc_with_workflows(
        BTreeMap::from([("echo@v1".into(), echo_tool("echo@v1"))]),
        vec![WorkflowSpec {
            name: "default".into(),
            display_name: None,
            description: None,
            impure: false,
            steps: vec![
                step("s1", "echo@v1", "a"),
                step("s2", "echo@v1", "b"),
                step("s3", "echo@v1", "c"),
            ],
        }],
    ));

    let (tracker, summary) = run_with_progress(&tc, "default").await;
    assert_eq!(summary.total_steps, 3);
    assert_eq!(summary.executed_steps, 3);
    assert_eq!(summary.failed_steps, 0);

    assert_eq!(
        tracker.ops(),
        vec![
            // Overall bar created by with_overall(), total set by coordinator.
            ProgressOp::AddBar { total: 1, label: "workflow [wf]".into() },
            ProgressOp::SetTotal { total: 3 },
            // Per-worker idle bars (2 workers).
            ProgressOp::AddBar { total: 0, label: "idle [wf]".into() },
            idle_pc(),
            ProgressOp::AddBar { total: 0, label: "idle [wf]".into() },
            idle_pc(),
            // Dispatch s1 to worker-0 (assigned 1).
            dispatch_pc("default/s1 (echo@v1)", 1),
            ProgressOp::SetTotal { total: 1 },
            // Dispatch s2 to worker-1 (assigned 1).
            dispatch_pc("default/s2 (echo@v1)", 1),
            ProgressOp::SetTotal { total: 1 },
            // Dispatch s3 to worker-0 (assigned 2).
            dispatch_pc("default/s3 (echo@v1)", 2),
            ProgressOp::SetTotal { total: 2 },
            // Step 1 completes (worker-0: succeeded 1 / assigned 2 — s3 already
            // dispatched on the same slot before s1 terminated).
            idle_pc_count(1, 2),
            ProgressOp::SetTotal { total: 2 },
            ProgressOp::Advance { delta: 1 },
            ProgressOp::FinishSuccess,
            ProgressOp::Advance { delta: 1 },
            // Step 2 completes (worker-1: succeeded 1 / assigned 1).
            idle_pc_count(1, 1),
            ProgressOp::SetTotal { total: 1 },
            ProgressOp::Advance { delta: 1 },
            ProgressOp::FinishSuccess,
            ProgressOp::Advance { delta: 1 },
            // Step 3 completes (worker-0: succeeded 2 / assigned 2).
            idle_pc_count(2, 2),
            ProgressOp::SetTotal { total: 2 },
            ProgressOp::Advance { delta: 1 },
            ProgressOp::FinishSuccess,
            ProgressOp::Advance { delta: 1 },
            // Overall bar finished.
            ProgressOp::FinishSuccess,
        ],
    );
}

/// A step referencing a tool with an unregistered `builtin_id` fails during
/// execution — the step bar receives `finish_warning` and the overall bar
/// also receives `finish_warning` (`failed_steps > 0`).
#[tokio::test]
async fn step_failure_emits_finish_warning() {
    fix_worker_pool_size();
    let tc = TestConductor::new();
    tc.write_config(doc_with_workflows(
        BTreeMap::from([("broken".into(), broken_tool("broken"))]),
        vec![WorkflowSpec {
            name: "default".into(),
            display_name: None,
            description: None,
            impure: false,
            steps: vec![step("s1", "broken", "")],
        }],
    ));

    let (tracker, summary) = run_with_progress(&tc, "default").await;
    assert_eq!(summary.total_steps, 1);
    assert_eq!(summary.executed_steps, 0);
    assert_eq!(summary.failed_steps, 1);

    assert_eq!(
        tracker.ops(),
        vec![
            // Overall bar created by with_overall(), total set by coordinator.
            ProgressOp::AddBar { total: 1, label: "workflow [wf]".into() },
            ProgressOp::SetTotal { total: 1 },
            // Per-worker idle bars (2 workers).
            ProgressOp::AddBar { total: 0, label: "idle [wf]".into() },
            idle_pc(),
            ProgressOp::AddBar { total: 0, label: "idle [wf]".into() },
            idle_pc(),
            // Dispatch s1 to worker-0 (consume idle bar).
            dispatch_pc("default/s1 (broken)", 1),
            ProgressOp::SetTotal { total: 1 },
            // Step fails — marker "F", count 0, advance + finish_warning.
            ProgressOp::SetPrefixComponents {
                marker: "F".into(),
                tool_name: "idle".into(),
                version: String::new(),
                phase: String::new(),
                count: "0".into(),
                total: "1".into(),
            },
            ProgressOp::SetTotal { total: 1 },
            ProgressOp::Advance { delta: 1 },
            ProgressOp::FinishWarning,
            // Overall bar advances once per terminal step.
            ProgressOp::Advance { delta: 1 },
            // Overall bar finished with warning (failed_steps > 0).
            ProgressOp::FinishWarning,
        ],
    );
}

/// When `progress_group` is `None`, no progress ops are recorded and the
/// workflow still succeeds.
#[tokio::test]
async fn no_progress_group_succeeds_silently() {
    let tc = TestConductor::new();
    tc.write_config(doc_with_workflows(
        BTreeMap::from([("echo@v1".into(), echo_tool("echo@v1"))]),
        vec![crate::echo_workflow("default", "echo@v1", "hello")],
    ));

    let tracker = RecordingProgressTracker::new();
    let summary = tc
        .conductor()
        .run_workflow("default", RunWorkflowOptions { progress_group: None, ..Default::default() })
        .await
        .expect("workflow");
    assert_eq!(summary.total_steps, 1);
    assert_eq!(summary.executed_steps, 1);
    assert_eq!(summary.failed_steps, 0);
    assert!(tracker.ops().is_empty(), "no progress ops when progress_group is None");
}

/// Two steps with a dependency — step2 depends on step1 (two levels).
/// The coordinator awaits level 0 before starting level 1, guaranteeing
/// sequential progress ops.  Both levels reuse worker-0's fixed slot, so
/// the second dispatch/terminal counts advance to 2.
#[tokio::test]
async fn two_step_sequential_levels_progress_ops() {
    fix_worker_pool_size();
    let tc = TestConductor::new();
    tc.write_config(doc_with_workflows(
        BTreeMap::from([("echo@v1".into(), echo_tool("echo@v1"))]),
        vec![WorkflowSpec {
            name: "default".into(),
            display_name: None,
            description: None,
            impure: false,
            steps: vec![
                step("s1", "echo@v1", "first"),
                step_depends("s2", "echo@v1", "second", &["s1"]),
            ],
        }],
    ));

    let (tracker, summary) = run_with_progress(&tc, "default").await;
    assert_eq!(summary.total_steps, 2);
    assert_eq!(summary.executed_steps, 2);
    assert_eq!(summary.failed_steps, 0);

    assert_eq!(
        tracker.ops(),
        vec![
            // Overall bar created by with_overall(), total set by coordinator.
            ProgressOp::AddBar { total: 1, label: "workflow [wf]".into() },
            ProgressOp::SetTotal { total: 2 },
            // Per-worker idle bars (2 workers).
            ProgressOp::AddBar { total: 0, label: "idle [wf]".into() },
            idle_pc(),
            ProgressOp::AddBar { total: 0, label: "idle [wf]".into() },
            idle_pc(),
            // Level 0: dispatch s1 to worker-0 (assigned 1).
            dispatch_pc("default/s1 (echo@v1)", 1),
            ProgressOp::SetTotal { total: 1 },
            // Level 0 await — step 1 completes.
            idle_pc_count(1, 1),
            ProgressOp::SetTotal { total: 1 },
            ProgressOp::Advance { delta: 1 },
            ProgressOp::FinishSuccess,
            ProgressOp::Advance { delta: 1 },
            // Level 1: dispatch s2 to worker-0 (assigned 2).
            dispatch_pc("default/s2 (echo@v1)", 2),
            ProgressOp::SetTotal { total: 2 },
            // Level 1 await — step 2 completes.
            idle_pc_count(2, 2),
            ProgressOp::SetTotal { total: 2 },
            ProgressOp::Advance { delta: 1 },
            ProgressOp::FinishSuccess,
            ProgressOp::Advance { delta: 1 },
            // Overall bar finished.
            ProgressOp::FinishSuccess,
        ],
    );
}

/// Regression: the fixed worker-slot grid emits exactly `pool_size` idle bars
/// and never creates a fresh bar per step (no per-step flicker).
#[tokio::test]
async fn regression_no_per_step_flicker() {
    fix_worker_pool_size();
    let tc = TestConductor::new();
    tc.write_config(doc_with_workflows(
        BTreeMap::from([("echo@v1".into(), echo_tool("echo@v1"))]),
        vec![WorkflowSpec {
            name: "default".into(),
            display_name: None,
            description: None,
            impure: false,
            steps: vec![
                step("s1", "echo@v1", "a"),
                step("s2", "echo@v1", "b"),
                step("s3", "echo@v1", "c"),
            ],
        }],
    ));

    let (tracker, summary) = run_with_progress(&tc, "default").await;
    assert_eq!(summary.total_steps, 3);

    let ops = tracker.ops();
    let idle_bars = ops
        .iter()
        .filter(|op| matches!(op, ProgressOp::AddBar { label, .. } if label == "idle [wf]"))
        .count();
    assert_eq!(idle_bars, 2, "exactly pool_size idle bars");

    let step_bars = ops
        .iter()
        .filter(|op| {
            matches!(op, ProgressOp::AddBar { label, .. } if label != "idle [wf]" && label != "workflow [wf]")
        })
        .count();
    assert_eq!(step_bars, 0, "no per-step AddBar (no flicker)");
}

/// Regression: the overall bar advances exactly once per terminal step
/// (2 advances per step: step bar + overall bar) and never uses `SetPosition`.
#[tokio::test]
async fn regression_overall_advances_once_per_step() {
    fix_worker_pool_size();
    let tc = TestConductor::new();
    tc.write_config(doc_with_workflows(
        BTreeMap::from([("echo@v1".into(), echo_tool("echo@v1"))]),
        vec![WorkflowSpec {
            name: "default".into(),
            display_name: None,
            description: None,
            impure: false,
            steps: vec![step("s1", "echo@v1", "a"), step("s2", "echo@v1", "b")],
        }],
    ));

    let (tracker, summary) = run_with_progress(&tc, "default").await;
    let n = summary.total_steps;
    assert_eq!(n, 2);

    let ops = tracker.ops();
    let advances = ops.iter().filter(|op| matches!(op, ProgressOp::Advance { .. })).count();
    assert_eq!(advances, 2 * n, "overall advances once per step (2 per step total)");

    let positions = ops.iter().filter(|op| matches!(op, ProgressOp::SetPosition { .. })).count();
    assert_eq!(positions, 0, "no SetPosition ops on the overall bar");
}

/// Regression: every step is dispatched exactly once (dispatch count equals
/// total steps) and each dispatch terminates exactly once.
#[tokio::test]
async fn regression_worker_invariant_holds() {
    fix_worker_pool_size();
    let tc = TestConductor::new();
    tc.write_config(doc_with_workflows(
        BTreeMap::from([("echo@v1".into(), echo_tool("echo@v1"))]),
        vec![WorkflowSpec {
            name: "default".into(),
            display_name: None,
            description: None,
            impure: false,
            steps: vec![
                step("s1", "echo@v1", "a"),
                step("s2", "echo@v1", "b"),
                step("s3", "echo@v1", "c"),
            ],
        }],
    ));

    let (tracker, summary) = run_with_progress(&tc, "default").await;
    let n = summary.total_steps;
    assert_eq!(n, 3);

    let ops = tracker.ops();
    let dispatches = ops
        .iter()
        .filter(|op| {
            matches!(op, ProgressOp::SetPrefixComponents { tool_name, .. } if tool_name.contains('/'))
        })
        .count();
    assert_eq!(dispatches, n, "each step dispatched exactly once");

    // Each terminal step emits exactly one Advance on its own bar; the overall
    // bar also advances once per step, so total Advances == 2 * n. Counting
    // Advances avoids double-counting the overall bar's final FinishSuccess.
    let advances = ops.iter().filter(|op| matches!(op, ProgressOp::Advance { .. })).count();
    assert_eq!(advances, 2 * n, "each dispatch terminates exactly once");
}

/// Regression: no pending-retry marker (`"W"`) is ever emitted — retries are
/// disabled (`max_retries` always 0), so only `""` (idle) or `"F"` (failure)
/// markers appear.
#[tokio::test]
async fn regression_no_pending_retry_marker() {
    fix_worker_pool_size();
    let tc = TestConductor::new();
    tc.write_config(doc_with_workflows(
        BTreeMap::from([("broken".into(), broken_tool("broken"))]),
        vec![WorkflowSpec {
            name: "default".into(),
            display_name: None,
            description: None,
            impure: false,
            steps: vec![step("s1", "broken", "")],
        }],
    ));

    let (tracker, summary) = run_with_progress(&tc, "default").await;
    assert_eq!(summary.failed_steps, 1);

    let pending_retry = tracker
        .ops()
        .iter()
        .any(|op| matches!(op, ProgressOp::SetPrefixComponents { marker, .. } if marker == "W"));
    assert!(!pending_retry, "no pending-retry marker emitted");
}
