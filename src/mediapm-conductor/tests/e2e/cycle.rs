//! End-to-end tests for DAG cycle detection in workflow step dependencies.
//!
//! Covers:
//! - COND-O.5: DAG cycle → cycle detection error
//! - COND-O.7: Circular step reference → graph build error

use std::collections::BTreeMap;

use crate::{TestConductor, bare_step, doc_with_workflows, echo_tool, workflow_with_steps};
use mediapm_conductor::api::RunWorkflowOptions;

/// Verifies that a workflow with a circular step dependency fails with a
/// cycle detection error.
#[tokio::test]
async fn circular_dependency_raises_cycle_error() {
    let tc = TestConductor::new();
    tc.write_config(doc_with_workflows(
        BTreeMap::from([("echo@v1".into(), echo_tool("echo@v1"))]),
        vec![workflow_with_steps(
            "cyclic",
            vec![
                bare_step("a", "echo@v1", &["b"]), // A depends on B
                bare_step("b", "echo@v1", &["a"]), // B depends on A → cycle!
            ],
        )],
    ));

    let err = tc
        .conductor()
        .run_workflow("cyclic", RunWorkflowOptions::default())
        .await
        .expect_err("cyclic workflow should fail");
    let msg = err.to_string();
    assert!(
        msg.contains("cycle") || msg.contains("Cycle"),
        "error should mention cycle detection: {msg}"
    );
}
