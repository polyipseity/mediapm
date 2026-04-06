//! Integration tests for conductor bootstrap and validation contracts.

use mediapm_cas::InMemoryCas;
use mediapm_conductor::{
    ConductorApi, MachineNickelDocument, SimpleConductor, encode_machine_document,
};
use tempfile::tempdir;

/// Protects bootstrap behavior when user and machine documents are missing.
#[tokio::test]
async fn run_workflow_bootstraps_missing_documents() {
    let conductor = SimpleConductor::new(InMemoryCas::new());
    let dir = tempdir().expect("tempdir");
    let user_path = dir.path().join("missing.conductor.ncl");
    let machine_path = dir.path().join("missing.conductor.machine.ncl");

    let summary = conductor
        .run_workflow(&user_path, &machine_path)
        .await
        .expect("workflow should execute with bootstrap defaults");

    assert_eq!(summary.executed_instances, 1);
    assert!(machine_path.exists(), "machine config should be materialized by runtime");

    let state = conductor.get_state().await.expect("state snapshot should load");
    assert_eq!(state.instances.len(), 1);
}

/// Protects integer-only validation for executable `success_codes`.
#[tokio::test]
async fn run_workflow_rejects_fractional_success_codes() {
    let conductor = SimpleConductor::new(InMemoryCas::new());
    let dir = tempdir().expect("tempdir");
    let user_path = dir.path().join("conductor.ncl");
    let machine_path = dir.path().join("conductor.machine.ncl");

    std::fs::write(
        &user_path,
        r#"
{
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
    .expect("write user document");
    std::fs::write(
        &machine_path,
        encode_machine_document(MachineNickelDocument::default()).expect("encode machine document"),
    )
    .expect("write machine document");

    let err = conductor
        .run_workflow(&user_path, &machine_path)
        .await
        .expect_err("fractional success_codes should fail validation");
    let message = err.to_string();
    assert!(
        message.contains("success_codes") || message.contains("Integer"),
        "expected integer-contract error mentioning success_codes or Integer, got: {message}"
    );
}
