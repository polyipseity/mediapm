//! Integration tests for conductor bootstrap and validation contracts.

use std::collections::BTreeMap;

use crate::{TestConductor, echo_tool, echo_workflow};
use mediapm_conductor::{
    NickelDocument, api::RunWorkflowOptions, config::versions::encode_document,
};

/// Protects bootstrap behavior when the config document is missing.
#[tokio::test]
async fn run_workflow_bootstraps_missing_documents() {
    let tc = TestConductor::new();

    let doc = NickelDocument {
        tools: BTreeMap::from([("echo@1.0.0".into(), echo_tool("echo@1.0.0"))]),
        workflows: vec![echo_workflow("default", "echo@1.0.0", "hello")],
        ..NickelDocument::default()
    };
    let config_path = tc.path().join("conductor.ncl");
    std::fs::write(&config_path, encode_document(doc).expect("encode")).expect("write config");

    let summary = tc
        .conductor()
        .run_workflow("default", RunWorkflowOptions::default())
        .await
        .expect("workflow");
    assert_eq!(summary.executed_steps, 1);
    assert_eq!(summary.cached_steps, 0);

    let state = tc.conductor().get_state().expect("state snapshot");
    assert_eq!(state.tool_call_instances.len(), 1);
}

/// Protects integer-only validation for executable `success_codes`.
#[tokio::test]
async fn run_workflow_rejects_fractional_success_codes() {
    let tc = TestConductor::new();
    let config_path = tc.path().join("conductor.ncl");

    std::fs::write(
        &config_path,
        br#"{
    version = 1,
    tools = {
        "bad-success-codes@1.0.0" = {
            kind = "executable",
            command = ["bin/tool"],
            env_vars = {},
            success_codes = [0.5],
            is_impure = false,
            inputs = {},
            outputs = {
                result = {
                    capture = { kind = "stdout" },
                },
            },
        },
    },
    workflows = {
        default = {
            steps = [{ id = "s", tool = "bad-success-codes@1.0.0" }],
        },
    },
}
"#,
    )
    .expect("write config");

    let err = tc
        .conductor()
        .run_workflow("default", RunWorkflowOptions::default())
        .await
        .expect_err("fractional success_codes should fail");
    let message = err.to_string();
    assert!(
        message.contains("success_codes") || message.contains("Integer"),
        "expected integer-contract error mentioning success_codes or Integer, got: {message}"
    );
}
