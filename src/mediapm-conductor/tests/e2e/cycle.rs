//! End-to-end tests for DAG cycle detection in workflow step dependencies.
//!
//! Covers:
//! - COND-O.5: DAG cycle → cycle detection error
//! - COND-O.7: Circular step reference → graph build error

use std::collections::BTreeMap;

use crate::{TestConductor, WorkflowSpec, WorkflowStepSpec, echo_tool};
use mediapm_conductor::{NickelDocument, api::RunWorkflowOptions};

/// Creates a workflow spec with a circular dependency between two steps.
fn cyclic_workflow() -> WorkflowSpec {
    WorkflowSpec {
        name: "cyclic".to_string(),
        display_name: String::new(),
        description: String::new(),
        impure: false,
        steps: vec![
            WorkflowStepSpec {
                id: "a".to_string(),
                tool: "echo@v1".to_string(),
                inputs: BTreeMap::new(),
                outputs: BTreeMap::new(),
                max_retries: 0,
                depends_on: vec!["b".to_string()], // A depends on B
            },
            WorkflowStepSpec {
                id: "b".to_string(),
                tool: "echo@v1".to_string(),
                inputs: BTreeMap::new(),
                outputs: BTreeMap::new(),
                max_retries: 0,
                depends_on: vec!["a".to_string()], // B depends on A → cycle!
            },
        ],
    }
}

/// Verifies that a workflow with a circular step dependency fails with a
/// cycle detection error.
#[tokio::test]
async fn circular_dependency_raises_cycle_error() {
    let tc = TestConductor::new();

    let doc = NickelDocument {
        tools: BTreeMap::from([("echo@v1".into(), echo_tool("echo@v1"))]),
        workflows: vec![cyclic_workflow()],
        ..NickelDocument::default()
    };
    // Use write_config-like approach.
    let config_path = tc.path().join("conductor.ncl");
    std::fs::write(
        &config_path,
        mediapm_conductor::config::versions::encode_document(doc).expect("encode"),
    )
    .expect("write config");

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
