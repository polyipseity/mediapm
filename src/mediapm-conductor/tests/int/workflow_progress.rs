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

// ---------------------------------------------------------------------------
// Tests
// ---------------------------------------------------------------------------

/// Single-step echo workflow emits the expected `[wf]` progress sequence:
/// overall bar (total set by coordinator) → per-worker idle bars → step bar
/// via worker-0 slot → advance → finish → overall finish.
#[tokio::test]
async fn single_step_success_progress_ops() {
    fix_worker_pool_size();
    let tc = TestConductor::new();
    tc.write_config(doc_with_workflows(
        BTreeMap::from([("echo@v1".into(), echo_tool("echo@v1"))]),
        vec![crate::echo_workflow("default", "echo@v1", "hello")],
    ));

    let (tracker, overall) = RecordingProgressTracker::with_overall("workflow [wf]", 1);
    let summary = tc
        .conductor()
        .run_workflow(
            "default",
            RunWorkflowOptions {
                progress_group: Some(Arc::new(tracker.clone())),
                overall_bar: Some(Arc::new(overall)),
                ..Default::default()
            },
        )
        .await
        .expect("workflow");
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
            ProgressOp::AddBar { total: 1, label: "idle [wf]".into() },
            ProgressOp::SetPrefixComponents {
                marker: String::new(),
                tool_name: "worker-0".into(),
                version: String::new(),
                phase: "wf".into(),
                count: String::new(),
                total: String::new(),
            },
            ProgressOp::AddBar { total: 1, label: "idle [wf]".into() },
            ProgressOp::SetPrefixComponents {
                marker: String::new(),
                tool_name: "worker-1".into(),
                version: String::new(),
                phase: "wf".into(),
                count: String::new(),
                total: String::new(),
            },
            // Dispatch s1 to worker-0 (consume idle bar).
            ProgressOp::SetPrefixComponents {
                marker: String::new(),
                tool_name: "s1".into(),
                version: String::new(),
                phase: "wf".into(),
                count: "1".into(),
                total: "1".into(),
            },
            ProgressOp::SetTotal { total: 1 },
            // Step completes — advance and finish success.
            ProgressOp::Advance { delta: 1 },
            ProgressOp::FinishSuccess,
            // Overall bar finished.
            ProgressOp::SetPosition { pos: 1 },
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

    let (tracker, overall) = RecordingProgressTracker::with_overall("workflow [wf]", 1);
    let summary = tc
        .conductor()
        .run_workflow(
            "default",
            RunWorkflowOptions {
                progress_group: Some(Arc::new(tracker.clone())),
                overall_bar: Some(Arc::new(overall)),
                ..Default::default()
            },
        )
        .await
        .expect("workflow");
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
            ProgressOp::AddBar { total: 2, label: "idle [wf]".into() },
            ProgressOp::SetPrefixComponents {
                marker: String::new(),
                tool_name: "worker-0".into(),
                version: String::new(),
                phase: "wf".into(),
                count: String::new(),
                total: String::new(),
            },
            ProgressOp::AddBar { total: 2, label: "idle [wf]".into() },
            ProgressOp::SetPrefixComponents {
                marker: String::new(),
                tool_name: "worker-1".into(),
                version: String::new(),
                phase: "wf".into(),
                count: String::new(),
                total: String::new(),
            },
            // Dispatch s1 to worker-0 (consume idle bar).
            ProgressOp::SetPrefixComponents {
                marker: String::new(),
                tool_name: "s1".into(),
                version: String::new(),
                phase: "wf".into(),
                count: "1".into(),
                total: "2".into(),
            },
            ProgressOp::SetTotal { total: 1 },
            // Dispatch s2 to worker-1 (consume idle bar).
            ProgressOp::SetPrefixComponents {
                marker: String::new(),
                tool_name: "s2".into(),
                version: String::new(),
                phase: "wf".into(),
                count: "2".into(),
                total: "2".into(),
            },
            ProgressOp::SetTotal { total: 1 },
            // Step 1 completes.
            ProgressOp::Advance { delta: 1 },
            ProgressOp::FinishSuccess,
            // Step 2 completes.
            ProgressOp::Advance { delta: 1 },
            ProgressOp::FinishSuccess,
            // Overall bar finished.
            ProgressOp::SetPosition { pos: 2 },
            ProgressOp::FinishSuccess,
        ],
    );
}

/// Two steps with a dependency — step2 depends on step1 (two levels).
/// The coordinator awaits level 0 before starting level 1, guaranteeing
/// sequential progress ops.  Worker-0's idle bar is consumed at level 0;
/// level 1 creates a fresh bar for step2.
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

    let (tracker, overall) = RecordingProgressTracker::with_overall("workflow [wf]", 1);
    let summary = tc
        .conductor()
        .run_workflow(
            "default",
            RunWorkflowOptions {
                progress_group: Some(Arc::new(tracker.clone())),
                overall_bar: Some(Arc::new(overall)),
                ..Default::default()
            },
        )
        .await
        .expect("workflow");
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
            ProgressOp::AddBar { total: 2, label: "idle [wf]".into() },
            ProgressOp::SetPrefixComponents {
                marker: String::new(),
                tool_name: "worker-0".into(),
                version: String::new(),
                phase: "wf".into(),
                count: String::new(),
                total: String::new(),
            },
            ProgressOp::AddBar { total: 2, label: "idle [wf]".into() },
            ProgressOp::SetPrefixComponents {
                marker: String::new(),
                tool_name: "worker-1".into(),
                version: String::new(),
                phase: "wf".into(),
                count: String::new(),
                total: String::new(),
            },
            // Level 0: dispatch s1 to worker-0 (consume idle bar).
            ProgressOp::SetPrefixComponents {
                marker: String::new(),
                tool_name: "s1".into(),
                version: String::new(),
                phase: "wf".into(),
                count: "1".into(),
                total: "2".into(),
            },
            ProgressOp::SetTotal { total: 1 },
            // Level 0 await — step 1 completes.
            ProgressOp::Advance { delta: 1 },
            ProgressOp::FinishSuccess,
            // Level 1: dispatch s2 to worker-0 (idle consumed, fresh bar).
            ProgressOp::AddBar { total: 1, label: "s2 [wf]".into() },
            ProgressOp::SetPrefixComponents {
                marker: String::new(),
                tool_name: "s2".into(),
                version: String::new(),
                phase: "wf".into(),
                count: "1".into(),
                total: "2".into(),
            },
            // Level 1 await — step 2 completes.
            ProgressOp::Advance { delta: 1 },
            ProgressOp::FinishSuccess,
            // Overall bar finished.
            ProgressOp::SetPosition { pos: 2 },
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

    let (tracker, overall) = RecordingProgressTracker::with_overall("workflow [wf]", 1);
    let summary = tc
        .conductor()
        .run_workflow(
            "default",
            RunWorkflowOptions {
                progress_group: Some(Arc::new(tracker.clone())),
                overall_bar: Some(Arc::new(overall)),
                ..Default::default()
            },
        )
        .await
        .expect("workflow should complete even with failed steps");
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
            ProgressOp::AddBar { total: 1, label: "idle [wf]".into() },
            ProgressOp::SetPrefixComponents {
                marker: String::new(),
                tool_name: "worker-0".into(),
                version: String::new(),
                phase: "wf".into(),
                count: String::new(),
                total: String::new(),
            },
            ProgressOp::AddBar { total: 1, label: "idle [wf]".into() },
            ProgressOp::SetPrefixComponents {
                marker: String::new(),
                tool_name: "worker-1".into(),
                version: String::new(),
                phase: "wf".into(),
                count: String::new(),
                total: String::new(),
            },
            // Dispatch s1 to worker-0 (consume idle bar).
            ProgressOp::SetPrefixComponents {
                marker: String::new(),
                tool_name: "s1".into(),
                version: String::new(),
                phase: "wf".into(),
                count: "1".into(),
                total: "1".into(),
            },
            ProgressOp::SetTotal { total: 1 },
            // Step fails — advance + finish_warning (not finish_error).
            ProgressOp::Advance { delta: 1 },
            ProgressOp::FinishWarning,
            // Overall bar finished with warning (failed_steps > 0).
            ProgressOp::SetPosition { pos: 1 },
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

/// A workflow with three independent steps at the same level — verifies
/// correct `count`/`total` progression and that all steps finish before
/// the overall bar.  With `pool_size=2`, the third step creates a fresh bar.
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

    let (tracker, overall) = RecordingProgressTracker::with_overall("workflow [wf]", 1);
    let summary = tc
        .conductor()
        .run_workflow(
            "default",
            RunWorkflowOptions {
                progress_group: Some(Arc::new(tracker.clone())),
                overall_bar: Some(Arc::new(overall)),
                ..Default::default()
            },
        )
        .await
        .expect("workflow");
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
            ProgressOp::AddBar { total: 3, label: "idle [wf]".into() },
            ProgressOp::SetPrefixComponents {
                marker: String::new(),
                tool_name: "worker-0".into(),
                version: String::new(),
                phase: "wf".into(),
                count: String::new(),
                total: String::new(),
            },
            ProgressOp::AddBar { total: 3, label: "idle [wf]".into() },
            ProgressOp::SetPrefixComponents {
                marker: String::new(),
                tool_name: "worker-1".into(),
                version: String::new(),
                phase: "wf".into(),
                count: String::new(),
                total: String::new(),
            },
            // Dispatch s1 to worker-0 (consume idle bar).
            ProgressOp::SetPrefixComponents {
                marker: String::new(),
                tool_name: "s1".into(),
                version: String::new(),
                phase: "wf".into(),
                count: "1".into(),
                total: "3".into(),
            },
            ProgressOp::SetTotal { total: 1 },
            // Dispatch s2 to worker-1 (consume idle bar).
            ProgressOp::SetPrefixComponents {
                marker: String::new(),
                tool_name: "s2".into(),
                version: String::new(),
                phase: "wf".into(),
                count: "2".into(),
                total: "3".into(),
            },
            ProgressOp::SetTotal { total: 1 },
            // Dispatch s3 to worker-0 (idle consumed, fresh bar).
            ProgressOp::AddBar { total: 1, label: "s3 [wf]".into() },
            ProgressOp::SetPrefixComponents {
                marker: String::new(),
                tool_name: "s3".into(),
                version: String::new(),
                phase: "wf".into(),
                count: "3".into(),
                total: "3".into(),
            },
            // All three steps complete.
            ProgressOp::Advance { delta: 1 },
            ProgressOp::FinishSuccess,
            ProgressOp::Advance { delta: 1 },
            ProgressOp::FinishSuccess,
            ProgressOp::Advance { delta: 1 },
            ProgressOp::FinishSuccess,
            // Overall bar finished.
            ProgressOp::SetPosition { pos: 3 },
            ProgressOp::FinishSuccess,
        ],
    );
}
