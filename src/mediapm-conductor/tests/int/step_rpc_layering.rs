//! Per-step RPC layering: the `ExecuteStep` dispatch carries no transport
//! timeout of its own.
//!
//! The 2026-10-19 media-tagger demo hang exposed three co-sized 300s
//! watchdogs — the per-step `ExecuteStep` RPC cap, the executable watchdog
//! inside the step worker, and the node's `run_workflow` RPC budget. Whichever
//! fired first hid the real cause behind a transport-flavored "RPC timeout".
//! The per-step cap is gone: a step's duration bound is the executable
//! watchdog in its worker, and the run-level bound is the `run_workflow` RPC.

use std::collections::BTreeMap;

use crate::{RpcEnvGuard, TestConductor, doc_with_workflows, rpc_env_lock, workflow_with_steps};
use mediapm_conductor::api::RunWorkflowOptions;
use mediapm_conductor::{ToolKindSpec, ToolRuntime, ToolSpec, WorkflowStepSpec};

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
    // Shrink the budget the coordinator re-reads per dispatch to 1s — below
    // the 3s step below. Set AFTER client init on purpose: the client has
    // already captured its own copy (see rpc_env_lock docs).
    let _shrink = RpcEnvGuard::set_seconds("1");

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

    let summary = tc
        .conductor()
        .run_workflow("default", RunWorkflowOptions::default())
        .await
        .expect("workflow completes when only the run-level budget applies");

    assert_eq!(summary.total_steps, 1);
    assert_eq!(summary.executed_steps, 1, "the 3s step executes exactly once: {summary:?}");
    assert_eq!(summary.failed_steps, 0, "no step fails: {summary:?}");
}
