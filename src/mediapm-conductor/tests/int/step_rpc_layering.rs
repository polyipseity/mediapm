//! Per-step RPC layering: the `ExecuteStep` dispatch carries no transport
//! timeout of its own, and the run-level budget reports itself honestly.
//!
//! The 2026-10-19 media-tagger demo hang exposed three co-sized 300s
//! watchdogs — the per-step `ExecuteStep` RPC cap, the executable watchdog
//! inside the step worker, and the node's `run_workflow` RPC budget. Whichever
//! fired first hid the real cause behind a transport-flavored "RPC timeout".
//! The per-step cap is gone: a step's duration bound is the executable
//! watchdog in its worker, and the run-level bound is the `run_workflow` RPC.
//! The run-level bound keeps its budget, but its timeout error must name that
//! budget and the knob that set it so the caller learns which policy layer
//! fired rather than a bare transport failure.

use std::collections::BTreeMap;

use crate::{RpcEnvGuard, TestConductor, doc_with_workflows, rpc_env_lock, workflow_with_steps};
use mediapm_conductor::api::RunWorkflowOptions;
use mediapm_conductor::{ToolKindSpec, ToolRuntime, ToolSpec, WorkflowStepSpec};

/// Builds the one-step "sleep 3s" workflow used by this module's tests.
///
/// The step is a real `sh` process on unix; it outlasts the 1s RPC budgets
/// these tests install, so the layer under test is the one that speaks.
#[cfg(unix)]
fn sleeping_workflow(tc: &TestConductor) {
    let sleeper = ToolSpec {
        version: None,
        kind: ToolKindSpec::Executable {
            command: vec!["sh".to_string(), "-c".to_string(), "sleep 3".to_string()],
            env_vars: BTreeMap::new(),
            success_codes: vec![0],
        },
        name: "sleeper".into(),
        inputs: BTreeMap::new(),
        default_inputs: BTreeMap::new(),
        outputs: BTreeMap::new(),
        runtime: ToolRuntime::default(),
    };
    let step = WorkflowStepSpec {
        id: "s1".into(),
        tool: "sleeper".into(),
        inputs: BTreeMap::new(),
        outputs: BTreeMap::new(),
        max_retries: 0,
        depends_on: Vec::new(),
    };
    tc.write_config(doc_with_workflows(
        BTreeMap::from([("sleeper".to_string(), sleeper)]),
        vec![workflow_with_steps("default", vec![step])],
    ));
}

/// Proves a step that outlives a short per-step RPC budget still completes.
///
/// Protects the guarantee that step duration is bounded by the worker's own
/// executable watchdog and the run-level budget, never by transport: on the
/// pre-fix coordinator (per-dispatch `Some(1s)` cap) this 3-second step is
/// killed by the RPC layer and the run reports an opaque step failure; with
/// the cap removed the run succeeds and the step executes exactly once.
#[cfg(unix)]
#[tokio::test]
async fn a_step_longer_than_the_per_step_rpc_budget_still_completes() {
    let _lock = rpc_env_lock().await;
    // Start from the unset default so the run-level RPC budget is captured
    // at that default; restores any prior state on drop.
    let _restore = RpcEnvGuard::remove();
    let tc = TestConductor::new();
    // Force the actor client to initialise now, capturing the default
    // run-level budget. The client is a OnceCell built on first use
    // (conductor_impl.rs `ensure_actor_client`), not at `Conductor::new`.
    let _diagnostics = tc
        .conductor()
        .get_runtime_diagnostics()
        .await
        .expect("diagnostics pin the run-level budget");
    // Regression tripwire: set a 1s knob value AFTER the client captured
    // its budget. The passing run must not be affected — production reads
    // the knob exactly once, at client-init capture — but if a per-dispatch
    // read of the knob ever returns, this 1s value kills the 3s step and
    // the summary assertions below fail (see rpc_env_lock docs).
    let _shrink = RpcEnvGuard::set_seconds("1");

    sleeping_workflow(&tc);

    let summary = tc
        .conductor()
        .run_workflow("default", RunWorkflowOptions::default())
        .await
        .expect("workflow completes when only the run-level budget applies");

    assert_eq!(summary.total_steps, 1);
    assert_eq!(summary.executed_steps, 1, "the 3s step executes exactly once: {summary:?}");
    assert_eq!(summary.failed_steps, 0, "no step fails: {summary:?}");
    tc.shutdown().await;
}

/// Proves a run-workflow RPC timeout reports the budget and the knob.
///
/// Protects the caller-facing contract of the run-level watchdog: when the
/// budget expires while the workflow is still executing, the error must name
/// the budget in seconds, the `MEDIAPM_CONDUCTOR_RPC_TIMEOUT_SECONDS` knob
/// that set it, and the sibling per-step bound
/// (`MEDIAPM_CONDUCTOR_EXECUTABLE_TIMEOUT_SECS`), so the caller learns which
/// policy layer fired. Before the fix this surfaced as the bare
/// `RPC call to 'ConductorActor' failed: RPC timeout`, which is what turned
/// the demo's legitimate long run into a misleading transport error.
#[cfg(unix)]
#[tokio::test]
async fn a_run_workflow_rpc_timeout_names_the_budget_and_the_knob() {
    let _lock = rpc_env_lock().await;
    // Set BEFORE the first RPC: the client OnceCell captures the budget on
    // first use, and here that first use is the `run_workflow` call under
    // test, so the 1s budget is the one that fires. See `rpc_env_lock` docs.
    let _budget = RpcEnvGuard::set_seconds("1");

    let tc = TestConductor::new();
    sleeping_workflow(&tc);

    let error = tc
        .conductor()
        .run_workflow("default", RunWorkflowOptions::default())
        .await
        .expect_err("the 1s run-level budget expires while the 3s step still runs");

    // The step is still in flight when the caller gives up; the actor is
    // torn down with this test's runtime rather than joined, which is why the
    // fixture stops it through the path that tolerates the client's own
    // timed-out shutdown RPC.
    let message = error.to_string();
    assert!(
        message.contains("MEDIAPM_CONDUCTOR_RPC_TIMEOUT_SECONDS"),
        "timeout error must name the knob: {message}"
    );
    assert!(message.contains("budget of 1s"), "timeout error must report the budget: {message}");
    assert!(
        message.contains("MEDIAPM_CONDUCTOR_EXECUTABLE_TIMEOUT_SECS"),
        "timeout error must name the per-step bound: {message}"
    );
    tc.shutdown_broken_client().await;
}
