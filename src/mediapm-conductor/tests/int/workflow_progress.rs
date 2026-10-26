//! Workflow screen progress tests, validates the bars the conductor
//! coordinator emits via `RecordingProgressTracker`.
//!
//! Every assertion uses exact `assert_eq!` on recorded `ProgressOp` sequences.
//! Tests are gated on `#[cfg(feature = "progress")]` for parity with the
//! feature-gated production code path.

use std::collections::{BTreeMap, BTreeSet};
use std::sync::Arc;

use crate::{TestConductor, doc_with_workflows, echo_tool};
use mediapm_conductor::api::RunWorkflowOptions;
use mediapm_conductor::{ToolKindSpec, ToolRuntime, WorkflowSpec, WorkflowStepSpec};
use mediapm_utils::progress::BarLabelTruncation;
use mediapm_utils::progress::SuffixComponents;
use mediapm_utils::progress::recording::{
    BarId, ProgressOp, RecordedProgressOp, RecordingProgressTracker,
};

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

/// Creates a `ToolSpec` whose command cannot be executed — the worker fails
/// the step during dispatch rather than at document load.
///
/// The failure is deliberately a spawn failure, not an unresolvable builtin id:
/// a `builtin` spec must name an id the build registers, so a document carrying
/// an invented id is rejected when it loads and never reaches a worker.
fn broken_tool(name: &str) -> mediapm_conductor::ToolSpec {
    mediapm_conductor::ToolSpec {
        version: None,
        kind: ToolKindSpec::Executable {
            command: vec!["mediapm-tool-that-does-not-exist".to_string()],
            env_vars: BTreeMap::new(),
            success_codes: vec![0],
        },
        name: name.into(),
        inputs: BTreeMap::new(),
        default_inputs: BTreeMap::new(),
        outputs: BTreeMap::new(),
        runtime: ToolRuntime::default(),
    }
}

/// Builds the `SetTruncation` op a worker-slot bar records, with the slot's
/// live tally filled in.
///
/// Mirrors `worker_slot_label` in the coordinator: the tally renders in the
/// suffix only when `total` is above zero (a slot nothing has been assigned
/// to yet renders timing only), and the suffix is measured through the
/// label's own `truncate_suffix` so the assertion follows the same rendering
/// path the coordinator's label goes through.
fn slot_pc(
    status_marker: &str,
    workflow_id: &str,
    step_id: &str,
    tool: &str,
    activity: &str,
    completed: usize,
    total: usize,
) -> ProgressOp {
    use mediapm_conductor::orchestration::progress_labels::WorkerBarLabel;
    let label = WorkerBarLabel {
        status_marker: status_marker.into(),
        workflow_id: workflow_id.into(),
        step_id: step_id.into(),
        tool: tool.into(),
        activity: activity.into(),
        completed: if total == 0 { String::new() } else { completed.to_string() },
        total: if total == 0 { String::new() } else { total.to_string() },
    };
    ProgressOp::SetTruncation {
        prefix: label.truncate_prefix(usize::MAX),
        suffix: label.truncate_suffix(usize::MAX, &SuffixComponents::default()),
    }
}

/// Idle worker-slot label for a slot that has not been assigned any work yet
/// (the pre-creation op): no tally, so the suffix stays empty.
fn idle_pc() -> ProgressOp {
    slot_pc("", "", "", "", "idle", 0, 0)
}

/// Idle worker-slot label after `completed` terminal outcomes on `total`
/// assignments.
fn idle_pc_count(completed: usize, total: usize) -> ProgressOp {
    slot_pc("", "", "", "", "idle", completed, total)
}

/// Dispatch label for a step assigned to a worker slot, carrying the slot's
/// tally: `completed` terminal outcomes over `total` assignments. The
/// just-dispatched step is in flight, so it counts toward `total` only.
fn dispatch_pc(
    workflow_id: &str,
    step_id: &str,
    tool: &str,
    completed: usize,
    total: usize,
) -> ProgressOp {
    slot_pc("", workflow_id, step_id, tool, "active", completed, total)
}

/// Pending-retry worker-slot label: the failed attempt counts toward `total`
/// (it consumed an assignment) but not toward `completed` — a retryable
/// failure is not a terminal outcome.
fn pending_retry_pc(completed: usize, total: usize) -> ProgressOp {
    slot_pc("W", "", "", "", "idle", completed, total)
}

/// Final-failure worker-slot label: the step reached its terminal outcome,
/// so it counts toward both `completed` and `total`.
fn final_failure_pc(completed: usize, total: usize) -> ProgressOp {
    slot_pc("F", "", "", "", "idle", completed, total)
}

/// Counts dispatch operations (truncation prefixes whose activity marker
/// contains `[active]`).
fn count_dispatches(ops: &[ProgressOp]) -> usize {
    ops.iter()
        .filter(|op| matches!(op, ProgressOp::SetTruncation { prefix, .. } if prefix.contains("[active]")))
        .count()
}

/// Counts `W` markers among the ops of a single row.
///
/// A slot carries one per non-final attempt. The overall row records no
/// label ops at all — its prefix is the static seed — so a marker count on
/// [`ops_on_overall`] is zero by construction; take the slice from
/// [`ops_on_worker_slots`] to count a slot's markers.
fn count_markers_w(ops: &[ProgressOp]) -> usize {
    ops.iter()
        .filter(|op| matches!(op, ProgressOp::SetTruncation { prefix, .. } if prefix.contains('W')))
        .count()
}

/// Counts final-failure markers (`"F"`).
///
/// As with [`count_markers_w`], the count on the overall row is zero: its
/// `[F]` renders from the built-in status marker, not from a label op.
fn count_markers_f(ops: &[ProgressOp]) -> usize {
    ops.iter()
        .filter(|op| matches!(op, ProgressOp::SetTruncation { prefix, .. } if prefix.contains('F')))
        .count()
}

/// Identity of the pinned overall row.
///
/// The screen opens with `RecordingProgressTracker::with_overall("workflow", 1)` —
/// a harness seed chosen for this test double, not the production
/// `running workflows` literal — so the overall row is the one bar whose
/// `AddBar` op names `workflow`.
/// Reading that op instead of hardcoding an index is what keeps the per-row
/// helpers pointing at the same row after a bar is added ahead of it.
fn overall_bar(recorded: &[RecordedProgressOp]) -> BarId {
    recorded
        .iter()
        .find(|entry| matches!(&entry.op, ProgressOp::AddBar { label, .. } if label == "workflow"))
        .map(|entry| entry.bar)
        .expect("tracker opens every screen with an overall row")
}

/// Ops recorded on the pinned overall row.
fn ops_on_overall(recorded: &[RecordedProgressOp]) -> Vec<ProgressOp> {
    let bar = overall_bar(recorded);
    recorded.iter().filter(|entry| entry.bar == bar).map(|entry| entry.op.clone()).collect()
}

/// Ops recorded on the worker slots, with the pinned overall row left out.
///
/// A slot carries a `[W]` per non-final attempt and the overall row carries the
/// single terminal `[W]` the finish arm installs, so a count taken over both
/// rows cannot say which one produced it. Splitting them here is what lets a
/// test assert the slot's retry count on its own.
fn ops_on_worker_slots(recorded: &[RecordedProgressOp]) -> Vec<ProgressOp> {
    let overall = overall_bar(recorded);
    recorded.iter().filter(|entry| entry.bar != overall).map(|entry| entry.op.clone()).collect()
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
    let (tracker, overall) = RecordingProgressTracker::with_overall("workflow", 1);
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
    let (tracker, overall) = RecordingProgressTracker::with_overall("workflow", 1);
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

/// The pinned overall row carries no label at all: it shows the static seed
/// and the run's progress, so the only ops it records are its creation, one
/// `set_total` per workflow, its advances, and its terminal finish.
#[tokio::test]
async fn overall_row_installs_no_label_and_sets_total_once() {
    fix_worker_pool_size();
    let tc = TestConductor::new();
    tc.write_config(doc_with_workflows(
        BTreeMap::from([("echo@v1".into(), echo_tool("echo@v1"))]),
        vec![crate::echo_workflow("default", "echo@v1", "hello")],
    ));

    let (tracker, summary) = run_with_progress(&tc, "default").await;
    assert_eq!(summary.total_steps, 1);

    let overall = ops_on_overall(&tracker.recorded());
    let truncations =
        overall.iter().filter(|op| matches!(op, ProgressOp::SetTruncation { .. })).count();
    assert_eq!(truncations, 0, "the overall row never installs a label: {overall:?}");
    let totals = overall.iter().filter(|op| matches!(op, ProgressOp::SetTotal { .. })).count();
    assert_eq!(totals, 1, "the overall row's total is set exactly once per workflow: {overall:?}");
    tc.shutdown().await;
}

/// Worker-slot bars suppress their auto rate and eta; the overall row keeps
/// its timing.
///
/// A worker slot counts steps, so a rate and eta derived from that count say
/// nothing — the coordinator drops both on every slot it pre-creates. The
/// overall row counts the run's steps against a byte-free total too, but it
/// is the row a user reads the run's pace from, so its timing stays.
#[tokio::test]
async fn worker_slots_suppress_rate_and_eta_but_the_overall_row_keeps_them() {
    fix_worker_pool_size();
    let tc = TestConductor::new();
    tc.write_config(doc_with_workflows(
        BTreeMap::from([("echo@v1".into(), echo_tool("echo@v1"))]),
        vec![crate::echo_workflow("default", "echo@v1", "hello")],
    ));

    let (tracker, summary) = run_with_progress(&tc, "default").await;
    assert_eq!(summary.total_steps, 1);
    let recorded = tracker.recorded();

    let worker_ops = ops_on_worker_slots(&recorded);
    let suppressions: Vec<&ProgressOp> = worker_ops
        .iter()
        .filter(|op| matches!(op, ProgressOp::SetSuppressRateEta { .. }))
        .collect();
    // Pool size is pinned to 2, and each pre-created slot sets the flag once.
    assert_eq!(
        suppressions,
        vec![
            &ProgressOp::SetSuppressRateEta { suppress: true },
            &ProgressOp::SetSuppressRateEta { suppress: true },
        ],
        "each pre-created worker slot suppresses rate and eta exactly once: {worker_ops:?}"
    );

    let overall = ops_on_overall(&recorded);
    let overall_suppressions =
        overall.iter().filter(|op| matches!(op, ProgressOp::SetSuppressRateEta { .. })).count();
    assert_eq!(overall_suppressions, 0, "the overall row keeps its rate and eta: {overall:?}");
    tc.shutdown().await;
}

/// Single-step echo workflow emits the expected progress sequence:
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
            ProgressOp::AddBar { total: 1, label: "workflow".into() },
            ProgressOp::SetTotal { total: 1 },
            // The run declares the overall bar live (Task 6 opt-in).
            ProgressOp::SetAnimating { animating: true },
            // Per-worker idle bars (2 workers), created finished.
            ProgressOp::AddBar { total: 0, label: "idle".into() },
            ProgressOp::SetSuppressRateEta { suppress: true },
            idle_pc(),
            ProgressOp::FinishSuccess,
            ProgressOp::AddBar { total: 0, label: "idle".into() },
            ProgressOp::SetSuppressRateEta { suppress: true },
            idle_pc(),
            ProgressOp::FinishSuccess,
            // Dispatch s1 to worker-0 (re-activate idle bar); 0 terminal
            // outcomes over 1 assignment (the step is in flight).
            ProgressOp::Restart,
            dispatch_pc("default", "s1", "echo@v1", 0, 1),
            ProgressOp::SetTotal { total: 1 },
            // The dispatch declares in-flight work (Task 6 opt-in).
            ProgressOp::SetAnimating { animating: true },
            // Step completes — worker-0 reaches 1 terminal outcome on 1
            // assignment, advance, finish success.
            idle_pc_count(1, 1),
            ProgressOp::SetTotal { total: 1 },
            ProgressOp::Advance { delta: 1 },
            ProgressOp::FinishSuccess,
            ProgressOp::Advance { delta: 1 },
            // Overall bar finished with the run's status list: one executed
            // step, everything else zero-count and dropped.
            ProgressOp::SetSuffixComponents {
                components: SuffixComponents { custom: "1 executed".into(), ..Default::default() },
            },
            ProgressOp::FinishSuccess,
        ],
    );
    tc.shutdown().await;
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
            ProgressOp::AddBar { total: 1, label: "workflow".into() },
            ProgressOp::SetTotal { total: 2 },
            // The run declares the overall bar live (Task 6 opt-in).
            ProgressOp::SetAnimating { animating: true },
            // Per-worker idle bars (2 workers), created finished.
            ProgressOp::AddBar { total: 0, label: "idle".into() },
            ProgressOp::SetSuppressRateEta { suppress: true },
            idle_pc(),
            ProgressOp::FinishSuccess,
            ProgressOp::AddBar { total: 0, label: "idle".into() },
            ProgressOp::SetSuppressRateEta { suppress: true },
            idle_pc(),
            ProgressOp::FinishSuccess,
            // Dispatch s1 to worker-0 (re-activate idle bar): 0 terminal
            // outcomes over 1 assignment.
            ProgressOp::Restart,
            dispatch_pc("default", "s1", "echo@v1", 0, 1),
            ProgressOp::SetTotal { total: 1 },
            // The dispatch declares in-flight work (Task 6 opt-in).
            ProgressOp::SetAnimating { animating: true },
            // Dispatch s2 to worker-1 (re-activate idle bar): 0/1.
            ProgressOp::Restart,
            dispatch_pc("default", "s2", "echo@v1", 0, 1),
            ProgressOp::SetTotal { total: 1 },
            // The dispatch declares in-flight work (Task 6 opt-in).
            ProgressOp::SetAnimating { animating: true },
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
            // Overall bar finished with the run's status list: two executed
            // steps.
            ProgressOp::SetSuffixComponents {
                components: SuffixComponents { custom: "2 executed".into(), ..Default::default() },
            },
            ProgressOp::FinishSuccess,
        ],
    );
    tc.shutdown().await;
}

/// A workflow with three independent steps — verifies the ready-queue
/// scheduler's `count`/`total` progression and that all steps settle before
/// the overall bar. With `pool_size=2`, the third step cannot dispatch until
/// a worker frees; it takes worker-0's slot the moment s1 settles, so its
/// dispatch/terminal counts advance to 2.
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
            ProgressOp::AddBar { total: 1, label: "workflow".into() },
            ProgressOp::SetTotal { total: 3 },
            // The run declares the overall bar live (Task 6 opt-in).
            ProgressOp::SetAnimating { animating: true },
            // Per-worker idle bars (2 workers), created finished.
            ProgressOp::AddBar { total: 0, label: "idle".into() },
            ProgressOp::SetSuppressRateEta { suppress: true },
            idle_pc(),
            ProgressOp::FinishSuccess,
            ProgressOp::AddBar { total: 0, label: "idle".into() },
            ProgressOp::SetSuppressRateEta { suppress: true },
            idle_pc(),
            ProgressOp::FinishSuccess,
            // Dispatch s1 to worker-0 (re-activate, 0/1).
            ProgressOp::Restart,
            dispatch_pc("default", "s1", "echo@v1", 0, 1),
            ProgressOp::SetTotal { total: 1 },
            // The dispatch declares in-flight work (Task 6 opt-in).
            ProgressOp::SetAnimating { animating: true },
            // Dispatch s2 to worker-1 (re-activate, 0/1). Both workers
            // are now busy, so s3 waits in the ready queue.
            ProgressOp::Restart,
            dispatch_pc("default", "s2", "echo@v1", 0, 1),
            ProgressOp::SetTotal { total: 1 },
            // The dispatch declares in-flight work (Task 6 opt-in).
            ProgressOp::SetAnimating { animating: true },
            // Step 1 completes and frees worker-0 (succeeded 1 / assigned 1).
            idle_pc_count(1, 1),
            ProgressOp::SetTotal { total: 1 },
            ProgressOp::Advance { delta: 1 },
            ProgressOp::FinishSuccess,
            ProgressOp::Advance { delta: 1 },
            // The freed worker immediately takes s3 (worker-0: 1 terminal
            // outcome over 2 assignments).
            ProgressOp::Restart,
            dispatch_pc("default", "s3", "echo@v1", 1, 2),
            ProgressOp::SetTotal { total: 2 },
            // The dispatch declares in-flight work (Task 6 opt-in).
            ProgressOp::SetAnimating { animating: true },
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
            // Overall bar finished with the run's status list: three
            // executed steps.
            ProgressOp::SetSuffixComponents {
                components: SuffixComponents { custom: "3 executed".into(), ..Default::default() },
            },
            ProgressOp::FinishSuccess,
        ],
    );
    tc.shutdown().await;
}

/// Regression: a slot that is not running a step stops at its activity
/// marker. `WorkerBarLabel` renders `tool` parenthesized, so the `tool`
/// placeholder the non-active states used to carry drew `(idle)` next to the
/// `[idle]` that `activity` had already said. The active state keeps its
/// tool name, so `default s1 (broken) [active]` has to survive the change.
#[tokio::test]
async fn regression_idle_slot_prefix_stops_at_the_activity_marker() {
    fix_worker_pool_size();
    let tc = TestConductor::new();
    tc.write_config(doc_with_workflows(
        BTreeMap::from([("broken".into(), broken_tool("broken"))]),
        vec![WorkflowSpec {
            name: "default".into(),
            display_name: None,
            description: None,
            impure: false,
            // Two retries, so the slot passes through PendingRetry before it
            // fails for good and all three non-active states are recorded.
            steps: vec![WorkflowStepSpec { max_retries: 2, ..step("s1", "broken", "") }],
        }],
    ));

    let (tracker, summary) = run_with_progress(&tc, "default").await;
    assert_eq!(summary.failed_steps, 1);

    let mut idle = BTreeSet::new();
    let mut active = Vec::new();
    for op in tracker.ops() {
        let ProgressOp::SetTruncation { prefix, .. } = op else { continue };
        if prefix.contains("[active]") {
            active.push(prefix);
        } else {
            idle.insert(prefix);
        }
    }
    assert_eq!(
        idle,
        BTreeSet::from(["[idle]".to_string(), "[W] [idle]".to_string(), "[F] [idle]".to_string()]),
        "a slot with no step running names no tool"
    );
    assert_eq!(
        active,
        vec!["default s1 (broken) [active]".to_string(); 3],
        "a slot running a step still names workflow, step and tool"
    );
    tc.shutdown().await;
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
    tc.shutdown().await;
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
            ProgressOp::AddBar { total: 1, label: "workflow".into() },
            ProgressOp::SetTotal { total: 2 },
            // The run declares the overall bar live (Task 6 opt-in).
            ProgressOp::SetAnimating { animating: true },
            // Per-worker idle bars (2 workers), created finished.
            ProgressOp::AddBar { total: 0, label: "idle".into() },
            ProgressOp::SetSuppressRateEta { suppress: true },
            idle_pc(),
            ProgressOp::FinishSuccess,
            ProgressOp::AddBar { total: 0, label: "idle".into() },
            ProgressOp::SetSuppressRateEta { suppress: true },
            idle_pc(),
            ProgressOp::FinishSuccess,
            // Dispatch s1 to worker-0 (re-activate, 0/1).
            ProgressOp::Restart,
            dispatch_pc("default", "s1", "echo@v1", 0, 1),
            ProgressOp::SetTotal { total: 1 },
            // The dispatch declares in-flight work (Task 6 opt-in).
            ProgressOp::SetAnimating { animating: true },
            // Step 1 completes and frees worker-0 (succeeded 1 / assigned 1).
            idle_pc_count(1, 1),
            ProgressOp::SetTotal { total: 1 },
            ProgressOp::Advance { delta: 1 },
            ProgressOp::FinishSuccess,
            ProgressOp::Advance { delta: 1 },
            // s2 becomes ready and takes the first free worker. Worker-1 sat
            // idle the whole run and fronts the FIFO queue, so s2 dispatches
            // there (0/1), not on worker-0.
            ProgressOp::Restart,
            dispatch_pc("default", "s2", "echo@v1", 0, 1),
            ProgressOp::SetTotal { total: 1 },
            // The dispatch declares in-flight work (Task 6 opt-in).
            ProgressOp::SetAnimating { animating: true },
            // Step 2 completes (worker-1: succeeded 1 / assigned 1).
            idle_pc_count(1, 1),
            ProgressOp::SetTotal { total: 1 },
            ProgressOp::Advance { delta: 1 },
            ProgressOp::FinishSuccess,
            ProgressOp::Advance { delta: 1 },
            // Overall bar finished with the run's status list: two
            // executed steps.
            ProgressOp::SetSuffixComponents {
                components: SuffixComponents { custom: "2 executed".into(), ..Default::default() },
            },
            ProgressOp::FinishSuccess,
        ],
    );
    tc.shutdown().await;
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
        .filter(|op| matches!(op, ProgressOp::AddBar { label, .. } if label == "idle"))
        .count();
    assert_eq!(idle_bars, 2, "exactly pool_size idle bars");

    let step_bars = ops
        .iter()
        .filter(|op| {
            matches!(op, ProgressOp::AddBar { label, .. } if label != "idle" && label != "workflow")
        })
        .count();
    assert_eq!(step_bars, 0, "no per-step AddBar (no flicker)");
    tc.shutdown().await;
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
    let recorded = tracker.recorded();
    assert_eq!(count_dispatches(&ops), 3, "step dispatched once per attempt (max_retries + 1)");
    assert_eq!(
        count_markers_w(&ops_on_worker_slots(&recorded)),
        2,
        "one pending-retry marker per non-final attempt on the worker slot"
    );
    assert_eq!(
        count_markers_w(&ops_on_overall(&recorded)),
        0,
        "the overall row records no label ops since its prefix is the static seed"
    );
    assert_eq!(
        count_markers_f(&ops_on_worker_slots(&recorded)),
        1,
        "final-failure marker on the last attempt"
    );
    assert_eq!(
        count_markers_f(&ops_on_overall(&recorded)),
        0,
        "the overall row does not carry the failure marker"
    );
    assert_eq!(count_advances(&ops), 4, "3 step-bar advances + 1 overall advance");
    tc.shutdown().await;
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
    let recorded = tracker.recorded();
    assert_eq!(
        count_markers_w(&ops_on_worker_slots(&recorded)),
        1,
        "one pending-retry marker on the worker slot"
    );
    assert_eq!(
        count_markers_w(&ops_on_overall(&recorded)),
        0,
        "the run succeeds, so the overall row ends clean"
    );
    assert_eq!(count_markers_f(&ops), 0, "no final-failure marker on success");
    assert_eq!(count_advances(&ops), 3, "2 step-bar advances + 1 overall advance");
    tc.shutdown().await;
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
    let recorded = tracker.recorded();
    assert_eq!(
        count_markers_w(&ops_on_worker_slots(&recorded)),
        1,
        "one pending-retry marker on the worker slot"
    );
    assert_eq!(
        count_markers_w(&ops_on_overall(&recorded)),
        0,
        "the overall row ends clean once the retry succeeds"
    );
    assert_eq!(count_markers_f(&ops), 0, "no final-failure marker after success");
    tc.shutdown().await;
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
    let recorded = tracker.recorded();
    assert_eq!(
        count_markers_w(&ops_on_worker_slots(&recorded)),
        1,
        "one pending-retry marker on the worker slot"
    );
    assert_eq!(
        count_markers_w(&ops_on_overall(&recorded)),
        0,
        "the run succeeds, so the overall row ends clean"
    );
    assert_eq!(count_markers_f(&ops), 0, "no final-failure marker on success");
    assert_eq!(count_dispatches(&ops), 2, "dispatched initial + one retry");
    tc.shutdown().await;
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
    let recorded = tracker.recorded();
    assert_eq!(count_dispatches(&ops), 3, "dispatched initial + two retries");
    assert_eq!(
        count_markers_w(&ops_on_worker_slots(&recorded)),
        2,
        "one pending-retry marker per non-final attempt on the worker slot"
    );
    assert_eq!(
        count_markers_w(&ops_on_overall(&recorded)),
        0,
        "the overall row records no label ops since its prefix is the static seed"
    );
    assert_eq!(
        count_markers_f(&ops_on_worker_slots(&recorded)),
        1,
        "final-failure marker on the last attempt"
    );
    assert_eq!(
        count_markers_f(&ops_on_overall(&recorded)),
        0,
        "the overall row does not carry the failure marker"
    );
    tc.shutdown().await;
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
    let recorded = tracker.recorded();
    assert_eq!(count_dispatches(&ops), 1, "no retry when max_retries is 0");
    assert_eq!(
        count_markers_w(&ops_on_worker_slots(&recorded)),
        0,
        "the only attempt is final, so the slot carries no pending-retry marker"
    );
    assert_eq!(
        count_markers_w(&ops_on_overall(&recorded)),
        0,
        "the overall row records no label ops since its prefix is the static seed"
    );
    assert_eq!(
        count_markers_f(&ops_on_worker_slots(&recorded)),
        1,
        "final-failure marker on the only attempt"
    );
    assert_eq!(
        count_markers_f(&ops_on_overall(&recorded)),
        0,
        "the overall row does not carry the failure marker"
    );
    tc.shutdown().await;
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
    let recorded = tracker.recorded();
    assert_eq!(count_dispatches(&ops), 1, "impure step not retried without flag");
    assert_eq!(
        count_markers_w(&ops_on_worker_slots(&recorded)),
        0,
        "the only attempt is final, so the slot carries no pending-retry marker"
    );
    assert_eq!(
        count_markers_w(&ops_on_overall(&recorded)),
        0,
        "the overall row records no label ops since its prefix is the static seed"
    );
    assert_eq!(
        count_markers_f(&ops_on_worker_slots(&recorded)),
        1,
        "final-failure marker on the only attempt"
    );
    assert_eq!(
        count_markers_f(&ops_on_overall(&recorded)),
        0,
        "the overall row does not carry the failure marker"
    );
    tc.shutdown().await;
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
    tc.shutdown().await;
}

/// When all steps succeed, the overall bar's status-list suffix contains no
/// `failed` word (the zero-count entry is dropped) and no `cached`/`retried`
/// words either — and it carries the executed count, so a clean run no
/// longer renders an empty suffix.
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
    assert_eq!(
        suffix, "1 executed",
        "a clean run reports its executed count instead of an empty suffix: {suffix:?}"
    );
    tc.shutdown().await;
}

/// The pinned overall row finishes as a warning when the run ends in
/// failure, and finishes as a success when it ends clean.
///
/// The row installs no label at all — its prefix is the static seed — so the
/// verdict is what the row records, not what a label carries: the terminal
/// finish op is the marker, and `[W]` and `[F]` must not collapse into one
/// another or into nothing. The `failed` count still lands in the suffix.
#[tokio::test]
async fn overall_row_marks_a_warned_run_and_leaves_a_clean_run_unmarked() {
    fix_worker_pool_size();
    let warned = TestConductor::new();
    warned.write_config(doc_with_workflows(
        BTreeMap::from([("broken".into(), broken_tool("broken"))]),
        vec![WorkflowSpec {
            name: "default".into(),
            display_name: None,
            description: None,
            impure: false,
            steps: vec![step("s1", "broken", "")],
        }],
    ));
    let (tracker, summary) = run_with_progress(&warned, "default").await;
    assert_eq!(summary.failed_steps, 1);
    let ops = ops_on_overall(&tracker.recorded());
    assert_eq!(
        ops.last(),
        Some(&ProgressOp::FinishWarning),
        "a run that lost a step ends the overall row with a warning: {ops:?}"
    );

    let clean = TestConductor::new();
    clean.write_config(doc_with_workflows(
        BTreeMap::from([("echo@v1".into(), echo_tool("echo@v1"))]),
        vec![crate::echo_workflow("default", "echo@v1", "hello")],
    ));
    let (tracker, summary) = run_with_progress(&clean, "default").await;
    assert_eq!(summary.failed_steps, 0);
    let ops = ops_on_overall(&tracker.recorded());
    assert_eq!(
        ops.last(),
        Some(&ProgressOp::FinishSuccess),
        "a clean run ends the overall row with a success: {ops:?}"
    );
    warned.shutdown().await;
    clean.shutdown().await;
}

/// A run with a failed step finishes the pinned overall row as a warning.
///
/// The overall row carries the run's verdict, so a run that lost a step cannot
/// end the same way a clean run does. Two things say that on the row: the
/// terminal finish is `FinishWarning` rather than `FinishSuccess`. The row's
/// prefix is the static seed, so `[W]` renders from the bar's built-in status
/// marker rather than from a label op. The suffix reports the tally, so
/// it is asserted too: a row that warned without saying how many steps failed
/// would leave the reader guessing which end of the run to look at.
#[tokio::test]
async fn overall_bar_finishes_warning_when_a_step_failed() {
    fix_worker_pool_size();
    let tc = TestConductor::new();
    tc.write_config(doc_with_workflows(
        BTreeMap::from([("broken".into(), broken_tool("broken"))]),
        vec![WorkflowSpec {
            name: "default".into(),
            display_name: None,
            description: None,
            impure: false,
            steps: vec![WorkflowStepSpec { max_retries: 0, ..step("s1", "broken", "") }],
        }],
    ));

    let (tracker, summary) = run_with_progress(&tc, "default").await;
    assert_eq!(summary.failed_steps, 1, "the fixture must fail its one step");

    let recorded = tracker.recorded();
    let ops = ops_on_overall(&recorded);
    assert!(
        ops.contains(&ProgressOp::FinishWarning),
        "a run that lost a step must not finish the overall row as a success: {ops:?}"
    );
    assert!(
        !ops.contains(&ProgressOp::FinishSuccess),
        "the overall row must not claim success after a failed step: {ops:?}"
    );
    assert_eq!(
        ops.last(),
        Some(&ProgressOp::FinishWarning),
        "the terminal finish is the last thing the overall row records: {ops:?}"
    );
    assert_eq!(
        count_markers_w(&ops),
        0,
        "the overall row records no label ops, so no marker can come from a label: {ops:?}"
    );
    let suffix = ops.iter().rev().find_map(|op| match op {
        ProgressOp::SetSuffixComponents { components } => Some(components.custom.clone()),
        _ => None,
    });
    assert_eq!(
        suffix,
        Some("1 failed".to_string()),
        "the warning is reported with the tally that caused it; a count of zero is \
         not rendered, so the two phases this run did not enter leave no word behind"
    );
    tc.shutdown().await;
}

/// Regression: the worker-bar tally counts a retryable failed attempt in
/// `total` only, never in `completed` — a step that fails once and then
/// succeeds shows `0/1` through the pending-retry settlement, `0/1` on the
/// re-dispatch, and only the terminal success moves the tally to `1/1`.
///
/// The re-dispatch takes the other free worker (the FIFO hands worker-1 the
/// retry), so the step's two assignments land on different slots: worker-0
/// ends at `0/1` with its `[W]` marker because its one assignment never
/// terminated there, and worker-1 closes at `1/1`. Both fractions stay at
/// or below 1 at every recorded frame.
#[tokio::test]
async fn retry_tally_counts_the_attempt_in_total_only() {
    fix_worker_pool_size();
    let tc = TestConductor::new();
    tc.write_config(doc_with_workflows(
        BTreeMap::from([("flaky@v1".into(), crate::flaky_tool("flaky@v1"))]),
        vec![WorkflowSpec {
            name: "default".into(),
            display_name: None,
            description: None,
            impure: false,
            steps: vec![flaky_step("s1", "flaky@v1", 1, 1, "tally_retry")],
        }],
    ));

    let (tracker, summary) = run_with_progress(&tc, "default").await;
    assert_eq!(summary.total_steps, 1);
    assert_eq!(summary.executed_steps, 1);
    assert_eq!(summary.retried_steps, 1);
    assert_eq!(summary.failed_steps, 0);

    assert_eq!(
        tracker.ops(),
        vec![
            ProgressOp::AddBar { total: 1, label: "workflow".into() },
            ProgressOp::SetTotal { total: 1 },
            ProgressOp::SetAnimating { animating: true },
            ProgressOp::AddBar { total: 0, label: "idle".into() },
            ProgressOp::SetSuppressRateEta { suppress: true },
            idle_pc(),
            ProgressOp::FinishSuccess,
            ProgressOp::AddBar { total: 0, label: "idle".into() },
            ProgressOp::SetSuppressRateEta { suppress: true },
            idle_pc(),
            ProgressOp::FinishSuccess,
            // Attempt 1 dispatches to worker-0: the assignment counts toward
            // total immediately, before any outcome exists.
            ProgressOp::Restart,
            dispatch_pc("default", "s1", "flaky@v1", 0, 1),
            ProgressOp::SetTotal { total: 1 },
            ProgressOp::SetAnimating { animating: true },
            // Attempt 1 fails retryably: the assignment stays in total, and
            // completed does not move — a retryable failure is not terminal.
            pending_retry_pc(0, 1),
            ProgressOp::SetTotal { total: 1 },
            ProgressOp::Advance { delta: 1 },
            ProgressOp::FinishWarning,
            // Attempt 2 re-dispatches to worker-1 (FIFO): 0/1 there.
            ProgressOp::Restart,
            dispatch_pc("default", "s1", "flaky@v1", 0, 1),
            ProgressOp::SetTotal { total: 1 },
            ProgressOp::SetAnimating { animating: true },
            // Attempt 2 succeeds: the terminal outcome closes worker-1's
            // tally at 1/1.
            idle_pc_count(1, 1),
            ProgressOp::SetTotal { total: 1 },
            ProgressOp::Advance { delta: 1 },
            ProgressOp::FinishSuccess,
            ProgressOp::Advance { delta: 1 },
            // The status list reports the executed step and the retry.
            ProgressOp::SetSuffixComponents {
                components: SuffixComponents {
                    custom: "1 executed, 1 retried".into(),
                    ..Default::default()
                },
            },
            ProgressOp::FinishSuccess,
        ],
    );
    tc.shutdown().await;
}

/// Regression: a final failure is a terminal outcome, so the worker-bar
/// tally counts it in `completed` as well as `total` — the exhausted-retry
/// slot ends at `1/1` behind its `[F]` marker, not at `0/1`.
#[tokio::test]
async fn final_failure_tally_counts_the_terminal_outcome() {
    fix_worker_pool_size();
    let tc = TestConductor::new();
    tc.write_config(doc_with_workflows(
        BTreeMap::from([("broken".into(), broken_tool("broken"))]),
        vec![WorkflowSpec {
            name: "default".into(),
            display_name: None,
            description: None,
            impure: false,
            steps: vec![WorkflowStepSpec { max_retries: 1, ..step("s1", "broken", "") }],
        }],
    ));

    let (tracker, summary) = run_with_progress(&tc, "default").await;
    assert_eq!(summary.total_steps, 1);
    assert_eq!(summary.executed_steps, 0);
    assert_eq!(summary.failed_steps, 1);

    assert_eq!(
        tracker.ops(),
        vec![
            ProgressOp::AddBar { total: 1, label: "workflow".into() },
            ProgressOp::SetTotal { total: 1 },
            ProgressOp::SetAnimating { animating: true },
            ProgressOp::AddBar { total: 0, label: "idle".into() },
            ProgressOp::SetSuppressRateEta { suppress: true },
            idle_pc(),
            ProgressOp::FinishSuccess,
            ProgressOp::AddBar { total: 0, label: "idle".into() },
            ProgressOp::SetSuppressRateEta { suppress: true },
            idle_pc(),
            ProgressOp::FinishSuccess,
            // Attempt 1 dispatches to worker-0 (0/1) and fails retryably:
            // total counts the assignment, completed does not move.
            ProgressOp::Restart,
            dispatch_pc("default", "s1", "broken", 0, 1),
            ProgressOp::SetTotal { total: 1 },
            ProgressOp::SetAnimating { animating: true },
            pending_retry_pc(0, 1),
            ProgressOp::SetTotal { total: 1 },
            ProgressOp::Advance { delta: 1 },
            ProgressOp::FinishWarning,
            // Attempt 2 re-dispatches to worker-1 and fails for good: the
            // terminal failure moves completed, closing the tally at 1/1.
            ProgressOp::Restart,
            dispatch_pc("default", "s1", "broken", 0, 1),
            ProgressOp::SetTotal { total: 1 },
            ProgressOp::SetAnimating { animating: true },
            final_failure_pc(1, 1),
            ProgressOp::SetTotal { total: 1 },
            ProgressOp::Advance { delta: 1 },
            ProgressOp::FinishWarning,
            ProgressOp::Advance { delta: 1 },
            // The status list reports the failed step; the executed count is
            // zero and is dropped by the builder.
            ProgressOp::SetSuffixComponents {
                components: SuffixComponents { custom: "1 failed".into(), ..Default::default() },
            },
            ProgressOp::FinishWarning,
        ],
    );
    tc.shutdown().await;
}
