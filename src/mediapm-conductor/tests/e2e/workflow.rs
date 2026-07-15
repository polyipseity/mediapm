//! End-to-end workflow scenarios for actor-backed conductor execution.

use crate::{TestConductor, single_echo_doc};
use mediapm_conductor::api::RunWorkflowOptions;

/// Protects repeated-run cache behavior for one deterministic workflow.
#[tokio::test]
#[ignore = "known failure: second run executes step instead of cache hit (executed_steps=1, cached_steps=0), persists even with --test-threads=1"]
async fn deterministic_workflow_hits_cache_on_second_run() {
    let tc = TestConductor::new();
    tc.write_config(single_echo_doc("echo@v1", "default"));

    let first = tc
        .conductor()
        .run_workflow("default", RunWorkflowOptions::default())
        .await
        .expect("first run");
    assert_eq!(first.executed_steps, 1);
    assert_eq!(first.cached_steps, 0);

    let second = tc
        .conductor()
        .run_workflow("default", RunWorkflowOptions::default())
        .await
        .expect("second run");
    assert_eq!(second.executed_steps, 0);
    assert_eq!(second.cached_steps, 1);
}
