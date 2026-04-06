//! End-to-end workflow scenarios for actor-backed conductor execution.

use std::collections::BTreeMap;

use mediapm_cas::InMemoryCas;
use mediapm_conductor::{
    ConductorApi, MachineNickelDocument, OutputCaptureSpec, ToolKindSpec, ToolOutputSpec, ToolSpec,
    UserNickelDocument, WorkflowSpec, WorkflowStepSpec, default_volatile_state_path,
    encode_machine_document, encode_user_document,
};
use tempfile::tempdir;

/// Protects repeated-run cache behavior for one deterministic workflow.
#[tokio::test]
async fn deterministic_workflow_hits_cache_on_second_run() {
    let conductor = mediapm_conductor::SimpleConductor::new(InMemoryCas::new());
    let dir = tempdir().expect("tempdir");
    let user_path = dir.path().join("conductor.ncl");
    let machine_path = dir.path().join("conductor.machine.ncl");

    let user = UserNickelDocument {
        tools: BTreeMap::from([(
            "echo@1.0.0".to_string(),
            ToolSpec {
                is_impure: false,
                kind: ToolKindSpec::Builtin {
                    name: "echo".to_string(),
                    version: "1.0.0".to_string(),
                },
                outputs: BTreeMap::from([(
                    "result".to_string(),
                    ToolOutputSpec { capture: OutputCaptureSpec::Stdout {} },
                )]),
                ..ToolSpec::default()
            },
        )]),
        workflows: BTreeMap::from([(
            "default".to_string(),
            WorkflowSpec {
                steps: vec![WorkflowStepSpec {
                    id: "s1".to_string(),
                    tool: "echo@1.0.0".to_string(),
                    inputs: BTreeMap::from([("text".to_string(), "hello".to_string().into())]),
                    depends_on: Vec::new(),
                    outputs: BTreeMap::new(),
                }],
            },
        )]),
        ..UserNickelDocument::default()
    };

    std::fs::write(&user_path, encode_user_document(user).expect("encode user document"))
        .expect("write user document");
    std::fs::write(
        &machine_path,
        encode_machine_document(MachineNickelDocument::default()).expect("encode machine document"),
    )
    .expect("write machine document");

    let first = conductor.run_workflow(&user_path, &machine_path).await.expect("first run");
    assert_eq!(first.executed_instances, 1);
    assert_eq!(first.cached_instances, 0);

    let second = conductor.run_workflow(&user_path, &machine_path).await.expect("second run");
    assert_eq!(second.executed_instances, 0);
    assert_eq!(second.cached_instances, 1);

    let state_path = default_volatile_state_path(&user_path, &machine_path);
    assert!(state_path.exists(), "state document should be persisted on run");
}
