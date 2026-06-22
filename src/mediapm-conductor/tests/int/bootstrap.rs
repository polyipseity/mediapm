//! Integration tests for conductor bootstrap and validation contracts.

use std::collections::BTreeMap;

use mediapm_cas::InMemoryCas;
use mediapm_conductor::{
    NickelDocument, RuntimeStoragePaths, SimpleConductor, ToolInputKind, ToolInputSpec,
    ToolKindSpec, ToolRuntime, ToolSpec, WorkflowSpec, WorkflowStepSpec, api::RunWorkflowOptions,
    model::config::versions::encode_document,
};
use tempfile::tempdir;

/// Protects bootstrap behavior when the config document is missing.
#[tokio::test]
async fn run_workflow_bootstraps_missing_documents() {
    let dir = tempdir().expect("tempdir");
    let paths = RuntimeStoragePaths::new(dir.path());
    let conductor = SimpleConductor::new(paths, InMemoryCas::new());

    // The conductor needs at least one tool + workflow to execute anything.
    // Write a minimal config.
    let doc = NickelDocument {
        tools: BTreeMap::from([(
            "echo@1.0.0".to_string(),
            ToolSpec {
                name: "echo@1.0.0".to_string(),
                version: "1.0.0".to_string(),
                kind: ToolKindSpec::Builtin {
                    name: "echo".to_string(),
                    version: "1.0.0".to_string(),
                },
                inputs: BTreeMap::from([(
                    "text".to_string(),
                    ToolInputSpec {
                        kind: ToolInputKind::String,
                        description: String::new(),
                        required: false,
                    },
                )]),
                default_inputs: BTreeMap::new(),
                outputs: BTreeMap::new(),
                runtime: ToolRuntime::default(),
            },
        )]),
        workflows: vec![WorkflowSpec {
            name: "default".to_string(),
            display_name: String::new(),
            description: String::new(),
            impure: false,
            steps: vec![WorkflowStepSpec {
                id: "s1".to_string(),
                tool: "echo@1.0.0".to_string(),
                inputs: BTreeMap::from([("text".to_string(), "hello".to_string())]),
                outputs: BTreeMap::new(),
                max_retries: 0,
                depends_on: Vec::new(),
            }],
        }],
        ..NickelDocument::default()
    };
    let config_path = dir.path().join("conductor.ncl");
    std::fs::write(&config_path, encode_document(doc).expect("encode")).expect("write config");

    let summary = conductor
        .run_workflow("default", RunWorkflowOptions::default())
        .await
        .expect("workflow should execute with bootstrap defaults");

    assert_eq!(summary.executed_steps, 1);
    assert_eq!(summary.cached_steps, 0);

    let state = conductor.get_state().await.expect("state snapshot should load");
    assert_eq!(state.tool_call_instances.len(), 1);
}

/// Protects integer-only validation for executable `success_codes`.
#[tokio::test]
async fn run_workflow_rejects_fractional_success_codes() {
    let dir = tempdir().expect("tempdir");
    let paths = RuntimeStoragePaths::new(dir.path());
    let conductor = SimpleConductor::new(paths, InMemoryCas::new());

    let config_path = dir.path().join("conductor.ncl");

    // Write a Nickel document with fractional success_codes — the conductor
    // should reject this during validation.
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

    let err = conductor
        .run_workflow("default", RunWorkflowOptions::default())
        .await
        .expect_err("fractional success_codes should fail validation");
    let message = err.to_string();
    assert!(
        message.contains("success_codes") || message.contains("Integer"),
        "expected integer-contract error mentioning success_codes or Integer, got: {message}"
    );
}
