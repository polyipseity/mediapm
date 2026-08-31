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
use mediapm_utils::progress::BarLabelTruncation;
use mediapm_utils::progress::SuffixComponents;
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

/// Creates a flaky@v1 `WorkflowStepSpec` (test-only builtin that fails its
/// first `failures` invocations then succeeds). `max_retries` controls how
/// many times the coordinator may re-dispatch the step after a failure.
#[cfg(any(test, feature = "progress"))]
fn flaky_step(
    id: &str,
    tool: &str,
    failures: usize,
    max_retries: usize,
    key: &str,
) -> WorkflowStepSpec {
    WorkflowStepSpec {
        id: id.into(),
        tool: tool.into(),
        inputs: BTreeMap::from([
            ("failures".into(), failures.to_string()),
            ("key".into(), key.into()),
        ]),
        outputs: BTreeMap::new(),
        max_retries,
        depends_on: Vec::new(),
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
    use mediapm_conductor::orchestration::progress_labels::WorkerBarLabel;
    let label = WorkerBarLabel {
        status_marker: String::new(),
        workflow_id: String::new(),
        step_id: String::new(),
        tool: "idle".into(),
        activity: "idle".into(),
    };
    ProgressOp::SetTruncation { prefix: label.truncate_prefix(usize::MAX), suffix: String::new() }
}

/// Idle worker-slot label after `succeeded`/`assigned` steps.
///
/// Worker-slot bars omit the `count/total` text (no real item+size progress),
/// so this matches [`idle_pc`] exactly — the counts are tracked internally
/// for the bar fill but never rendered.
fn idle_pc_count(_succeeded: usize, _assigned: usize) -> ProgressOp {
    idle_pc()
}

/// Dispatch label for a step assigned to a worker slot.
///
/// After the Phase 2 fix, `workflow_id`, `step_id`, and `tool` are populated
/// as separate fields so the client-side truncation order can drop trailing
/// parts independently.
fn dispatch_pc(workflow_id: &str, step_id: &str, tool: &str, _assigned: usize) -> ProgressOp {
    use mediapm_conductor::orchestration::progress_labels::WorkerBarLabel;
    let label = WorkerBarLabel {
        status_marker: String::new(),
        workflow_id: workflow_id.into(),
        step_id: step_id.into(),
        tool: tool.into(),
        activity: "active".into(),
    };
    ProgressOp::SetTruncation { prefix: label.truncate_prefix(usize::MAX), suffix: String::new() }
}

/// Counts dispatch operations (truncation prefixes whose activity marker
/// contains `[active]`).
fn count_dispatches(ops: &[ProgressOp]) -> usize {
    ops.iter()
        .filter(|op| matches!(op, ProgressOp::SetTruncation { prefix, .. } if prefix.contains("[active]")))
        .count()
}

/// Counts pending-retry markers (`"W"`).
fn count_markers_w(ops: &[ProgressOp]) -> usize {
    ops.iter()
        .filter(|op| matches!(op, ProgressOp::SetTruncation { prefix, .. } if prefix.contains('W')))
        .count()
}

/// Counts final-failure markers (`"F"`).
fn count_markers_f(ops: &[ProgressOp]) -> usize {
    ops.iter()
        .filter(|op| matches!(op, ProgressOp::SetTruncation { prefix, .. } if prefix.contains('F')))
        .count()
}

/// Counts all `advance` operations (step bars + overall bar).
fn count_advances(ops: &[ProgressOp]) -> usize {
    ops.iter().filter(|op| matches!(op, ProgressOp::Advance { .. })).count()
}

/// Runs `workflow_name` with a recording progress group and custom options.
async fn run_with_progress_opts(
    tc: &TestConductor,
    workflow_name: &str,
    extra: RunWorkflowOptions,
) -> (RecordingProgressTracker, mediapm_conductor::RunSummary) {
    let (tracker, overall) = RecordingProgressTracker::with_overall("workflow [wf]", 1);
    let summary = tc
        .conductor()
        .run_workflow(
            workflow_name,
            RunWorkflowOptions {
                progress_group: Some(Arc::new(tracker.clone())),
                overall_bar: Some(Arc::new(overall)),
                ..extra
            },
        )
        .await
        .expect("workflow");
    (tracker, summary)
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
            dispatch_pc("default", "s1", "echo@v1", 1),
            ProgressOp::SetTotal { total: 1 },
            // Step completes — idle count1/total1, advance, finish success.
            idle_pc_count(1, 1),
            ProgressOp::SetTotal { total: 1 },
            ProgressOp::Advance { delta: 1 },
            ProgressOp::FinishSuccess,
            // Overall bar advances once per terminal step.
            ProgressOp::Advance { delta: 1 },
            // Overall bar finished with a status-list suffix.
            ProgressOp::SetSuffixComponents {
                components: SuffixComponents { custom: String::new(), ..Default::default() },
            },
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
            dispatch_pc("default", "s1", "echo@v1", 1),
            ProgressOp::SetTotal { total: 1 },
            // Dispatch s2 to worker-1 (consume idle bar).
            dispatch_pc("default", "s2", "echo@v1", 1),
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
            // Overall bar finished with a status-list suffix.
            ProgressOp::SetSuffixComponents {
                components: SuffixComponents { custom: String::new(), ..Default::default() },
            },
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
            dispatch_pc("default", "s1", "echo@v1", 1),
            ProgressOp::SetTotal { total: 1 },
            // Dispatch s2 to worker-1 (assigned 1).
            dispatch_pc("default", "s2", "echo@v1", 1),
            ProgressOp::SetTotal { total: 1 },
            // Dispatch s3 to worker-0 (assigned 2).
            dispatch_pc("default", "s3", "echo@v1", 2),
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
            // Overall bar finished with a status-list suffix.
            ProgressOp::SetSuffixComponents {
                components: SuffixComponents { custom: String::new(), ..Default::default() },
            },
            ProgressOp::FinishSuccess,
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
            dispatch_pc("default", "s1", "echo@v1", 1),
            ProgressOp::SetTotal { total: 1 },
            // Level 0 await — step 1 completes.
            idle_pc_count(1, 1),
            ProgressOp::SetTotal { total: 1 },
            ProgressOp::Advance { delta: 1 },
            ProgressOp::FinishSuccess,
            ProgressOp::Advance { delta: 1 },
            // Level 1: dispatch s2 to worker-0 (assigned 2).
            dispatch_pc("default", "s2", "echo@v1", 2),
            ProgressOp::SetTotal { total: 2 },
            // Level 1 await — step 2 completes.
            idle_pc_count(2, 2),
            ProgressOp::SetTotal { total: 2 },
            ProgressOp::Advance { delta: 1 },
            ProgressOp::FinishSuccess,
            ProgressOp::Advance { delta: 1 },
            // Overall bar finished with a status-list suffix.
            ProgressOp::SetSuffixComponents {
                components: SuffixComponents { custom: String::new(), ..Default::default() },
            },
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

/// Regression: a step that exhausts its retries is dispatched exactly
/// `max_retries + 1` times (once per attempt), emits a pending-retry marker
/// (`"W"`) for each non-final attempt, and a final-failure marker (`"F"`)
/// only on the last attempt. The overall bar advances exactly once (on the
/// terminal step).
#[tokio::test]
async fn regression_worker_invariant_holds() {
    fix_worker_pool_size();
    let tc = TestConductor::new();
    tc.write_config(doc_with_workflows(
        BTreeMap::from([("broken".into(), broken_tool("broken"))]),
        vec![WorkflowSpec {
            name: "default".into(),
            display_name: None,
            description: None,
            impure: false,
            steps: vec![WorkflowStepSpec { max_retries: 2, ..step("s1", "broken", "") }],
        }],
    ));

    let (tracker, summary) = run_with_progress(&tc, "default").await;
    assert_eq!(summary.total_steps, 1);
    assert_eq!(summary.executed_steps, 0);
    assert_eq!(summary.failed_steps, 1);

    let ops = tracker.ops();
    assert_eq!(count_dispatches(&ops), 3, "step dispatched once per attempt (max_retries + 1)");
    assert_eq!(count_markers_w(&ops), 2, "pending-retry marker per non-final attempt");
    assert_eq!(count_markers_f(&ops), 1, "final-failure marker on last attempt");
    assert_eq!(count_advances(&ops), 4, "3 step-bar advances + 1 overall advance");
}

/// Regression: a step that retries then succeeds emits a pending-retry marker
/// on the failed attempt but never a final-failure marker, and the overall bar
/// advances exactly once (on the terminal success).
#[tokio::test]
async fn regression_overall_advances_only_on_final_terminal() {
    fix_worker_pool_size();
    let tc = TestConductor::new();
    tc.write_config(doc_with_workflows(
        BTreeMap::from([("flaky@v1".into(), crate::flaky_tool("flaky@v1"))]),
        vec![WorkflowSpec {
            name: "default".into(),
            display_name: None,
            description: None,
            impure: false,
            steps: vec![flaky_step("s1", "flaky@v1", 1, 1, "overall_terminal")],
        }],
    ));

    let (tracker, summary) = run_with_progress(&tc, "default").await;
    assert_eq!(summary.total_steps, 1);
    assert_eq!(summary.executed_steps, 1);
    assert_eq!(summary.failed_steps, 0);

    let ops = tracker.ops();
    assert!(count_markers_w(&ops) >= 1, "pending-retry marker on failed attempt");
    assert_eq!(count_markers_f(&ops), 0, "no final-failure marker on success");
    assert_eq!(count_advances(&ops), 3, "2 step-bar advances + 1 overall advance");
}

/// Regression: after a retry succeeds, no pending-retry marker lingers — the
/// step terminates cleanly (success, no final-failure marker).
#[tokio::test]
async fn regression_pending_retry_trends_to_zero() {
    fix_worker_pool_size();
    let tc = TestConductor::new();
    tc.write_config(doc_with_workflows(
        BTreeMap::from([("flaky@v1".into(), crate::flaky_tool("flaky@v1"))]),
        vec![WorkflowSpec {
            name: "default".into(),
            display_name: None,
            description: None,
            impure: false,
            steps: vec![flaky_step("s1", "flaky@v1", 1, 1, "pending_zero")],
        }],
    ));

    let (tracker, summary) = run_with_progress(&tc, "default").await;
    assert_eq!(summary.total_steps, 1);
    assert_eq!(summary.executed_steps, 1);
    assert_eq!(summary.failed_steps, 0);

    let ops = tracker.ops();
    assert!(count_markers_w(&ops) >= 1, "pending-retry marker on failed attempt");
    assert_eq!(count_markers_f(&ops), 0, "no final-failure marker after success");
}

/// A flaky step that fails once then succeeds is dispatched twice (initial +
/// one retry) and terminates as a success.
#[tokio::test]
async fn retry_then_succeed_progress_ops() {
    fix_worker_pool_size();
    let tc = TestConductor::new();
    tc.write_config(doc_with_workflows(
        BTreeMap::from([("flaky@v1".into(), crate::flaky_tool("flaky@v1"))]),
        vec![WorkflowSpec {
            name: "default".into(),
            display_name: None,
            description: None,
            impure: false,
            steps: vec![flaky_step("s1", "flaky@v1", 1, 1, "retry_ok")],
        }],
    ));

    let (tracker, summary) = run_with_progress(&tc, "default").await;
    assert_eq!(summary.total_steps, 1);
    assert_eq!(summary.executed_steps, 1);
    assert_eq!(summary.failed_steps, 0);

    let ops = tracker.ops();
    assert!(count_markers_w(&ops) >= 1, "pending-retry marker on failed attempt");
    assert_eq!(count_markers_f(&ops), 0, "no final-failure marker on success");
    assert_eq!(count_dispatches(&ops), 2, "dispatched initial + one retry");
}

/// A broken step with `max_retries: 2` is dispatched three times (initial +
/// two retries) and terminates as a failure with no success.
#[tokio::test]
async fn retry_exhausted_progress_ops() {
    fix_worker_pool_size();
    let tc = TestConductor::new();
    tc.write_config(doc_with_workflows(
        BTreeMap::from([("broken".into(), broken_tool("broken"))]),
        vec![WorkflowSpec {
            name: "default".into(),
            display_name: None,
            description: None,
            impure: false,
            steps: vec![WorkflowStepSpec { max_retries: 2, ..step("s1", "broken", "") }],
        }],
    ));

    let (tracker, summary) = run_with_progress(&tc, "default").await;
    assert_eq!(summary.total_steps, 1);
    assert_eq!(summary.executed_steps, 0);
    assert_eq!(summary.failed_steps, 1);

    let ops = tracker.ops();
    assert_eq!(count_dispatches(&ops), 3, "dispatched initial + two retries");
    assert_eq!(count_markers_w(&ops), 2, "pending-retry marker per non-final attempt");
    assert_eq!(count_markers_f(&ops), 1, "final-failure marker on last attempt");
}

/// A broken step with `max_retries: 0` is dispatched exactly once and fails
/// with a final-failure marker (no pending-retry marker).
#[tokio::test]
async fn no_retry_when_max_retries_zero() {
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

    let ops = tracker.ops();
    assert_eq!(count_dispatches(&ops), 1, "no retry when max_retries is 0");
    assert_eq!(count_markers_w(&ops), 0, "no pending-retry marker");
    assert_eq!(count_markers_f(&ops), 1, "final-failure marker on the only attempt");
}

/// An impure broken step with `max_retries: 2` is NOT retried when
/// `retry_impure` is false (the default) — it fails on the first attempt.
#[tokio::test]
async fn impure_no_retry_without_flag() {
    fix_worker_pool_size();
    let tc = TestConductor::new();
    let impure_broken = {
        let mut t = broken_tool("broken");
        t.runtime = ToolRuntime { impure: true, max_retries: 2, ..Default::default() };
        t
    };
    tc.write_config(doc_with_workflows(
        BTreeMap::from([("broken".into(), impure_broken)]),
        vec![WorkflowSpec {
            name: "default".into(),
            display_name: None,
            description: None,
            impure: true,
            steps: vec![WorkflowStepSpec { max_retries: 2, ..step("s1", "broken", "") }],
        }],
    ));

    let (tracker, summary) = run_with_progress_opts(
        &tc,
        "default",
        RunWorkflowOptions { retry_impure: false, ..Default::default() },
    )
    .await;
    assert_eq!(summary.total_steps, 1);
    assert_eq!(summary.executed_steps, 0);
    assert_eq!(summary.failed_steps, 1);

    let ops = tracker.ops();
    assert_eq!(count_dispatches(&ops), 1, "impure step not retried without flag");
    assert_eq!(count_markers_w(&ops), 0, "no pending-retry marker");
    assert_eq!(count_markers_f(&ops), 1, "final-failure marker on the only attempt");
}

/// The overall workflow bar's suffix is a comma-joined status list of
/// `cached`/`failed`/`retried` counts. When a step fails, the rendered
/// suffix contains the `failed` word; when all steps succeed, the `failed`
/// word is absent (zero-count entries are dropped).
#[tokio::test]
async fn overall_bar_suffix_status_list() {
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
    assert_eq!(summary.failed_steps, 1);

    // Find the overall bar's final SetSuffixComponents op.
    let suffix = tracker
        .ops()
        .iter()
        .rev()
        .find_map(|op| match op {
            ProgressOp::SetSuffixComponents { components } => Some(components.custom.clone()),
            _ => None,
        })
        .expect("overall bar sets a status-list suffix");
    assert!(suffix.contains("failed"), "failed step appears in status list: {suffix:?}");
    assert!(!suffix.contains("cached"), "zero-count cached dropped: {suffix:?}");
    assert!(!suffix.contains("retried"), "zero-count retried dropped: {suffix:?}");
}

/// When all steps succeed, the overall bar's status-list suffix contains no
/// `failed` word (the zero-count entry is dropped) and no `cached`/`retried`
/// words either.
#[tokio::test]
async fn overall_bar_suffix_no_failed_on_success() {
    fix_worker_pool_size();
    let tc = TestConductor::new();
    tc.write_config(doc_with_workflows(
        BTreeMap::from([("echo@v1".into(), echo_tool("echo@v1"))]),
        vec![crate::echo_workflow("default", "echo@v1", "hello")],
    ));

    let (tracker, summary) = run_with_progress(&tc, "default").await;
    assert_eq!(summary.total_steps, 1);
    assert_eq!(summary.failed_steps, 0);

    let suffix = tracker
        .ops()
        .iter()
        .rev()
        .find_map(|op| match op {
            ProgressOp::SetSuffixComponents { components } => Some(components.custom.clone()),
            _ => None,
        })
        .expect("overall bar sets a status-list suffix");
    assert!(!suffix.contains("failed"), "no failed word on success: {suffix:?}");
    assert!(!suffix.contains("cached"), "zero-count cached dropped: {suffix:?}");
    assert!(!suffix.contains("retried"), "zero-count retried dropped: {suffix:?}");
}
