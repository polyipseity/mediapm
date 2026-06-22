//! End-to-end workflow scenarios for actor-backed conductor execution.

use std::collections::BTreeMap;

use mediapm_cas::InMemoryCas;
use mediapm_conductor::{
    RuntimeStoragePaths, ToolKindSpec, ToolRuntime, ToolSpec, WorkflowSpec, WorkflowStepSpec,
    api::RunWorkflowOptions, model::config::versions::encode_document,
};
use tempfile::tempdir;

/// Protects repeated-run cache behavior for one deterministic workflow.
#[tokio::test]
async fn deterministic_workflow_hits_cache_on_second_run() {
    let dir = tempdir().expect("tempdir");
    let conductor_dir = dir.path().join(".conductor");
    std::fs::create_dir_all(&conductor_dir).expect("create conductor dir");

    let doc = mediapm_conductor::NickelDocument {
        tools: BTreeMap::from([(
            "echo@1.0.0".to_string(),
            ToolSpec {
                kind: ToolKindSpec::Builtin {
                    name: "echo".to_string(),
                    version: "1.0.0".to_string(),
                },
                name: "echo@1.0.0".to_string(),
                version: "1.0.0".to_string(),
                inputs: BTreeMap::from([(
                    "text".to_string(),
                    mediapm_conductor::model::config::ToolInputSpec {
                        kind: mediapm_conductor::model::config::ToolInputKind::String,
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
        ..mediapm_conductor::NickelDocument::default()
    };

    let user_path = dir.path().join("conductor.ncl");
    std::fs::write(&user_path, encode_document(doc).expect("encode document"))
        .expect("write document");

    let conductor = mediapm_conductor::SimpleConductor::new(
        RuntimeStoragePaths::new(dir.path()),
        InMemoryCas::new(),
    );

    let first =
        conductor.run_workflow("default", RunWorkflowOptions::default()).await.expect("first run");
    assert_eq!(first.executed_steps, 1);
    assert_eq!(first.cached_steps, 0);

    let second =
        conductor.run_workflow("default", RunWorkflowOptions::default()).await.expect("second run");
    assert_eq!(second.executed_steps, 0);
    assert_eq!(second.cached_steps, 1);
}
