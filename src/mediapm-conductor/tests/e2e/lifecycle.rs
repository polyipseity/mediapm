//! Comprehensive workflow-lifecycle integration test covering the full cache,
//! GC, and tool-update lifecycle:
//!
//! Exercises the lifecycle documented in
//! [tool-content-cache-refactoring.instructions.md]:
//!
//! 1. initial → first execution (`executed=1`, `cached=0`)
//! 2. second run → cache hit (`executed=0`, `cached=1`)
//! 3. run GC → referenced instance survives
//! 4. third run → cache hit post-GC (`executed=0`, `cached=1`)
//! 5. new tool_id + new workflow → old tool cached, new tool executed
//!    (`executed=1`, `cached=1`)
//! 6. both workflows cached (`executed=0`, `cached=2`)
//! 7. run GC → both instances survive
//! 8. both cached post-GC (`executed=0`, `cached=2`)
//!
//! Tool identities are distinct tool_id keys (not version changes) since the
//! conductor builtin registry only knows echo@1.0.0.  Each distinct tool_id
//! produces a different instance key and therefore independent cache entries,
//! which matches the real-world semantics of tool-identity change (e.g. a new
//! yt-dlp content hash producing a different tool_id).

use std::collections::BTreeMap;

use mediapm_cas::InMemoryCas;
use mediapm_conductor::{
    NickelDocument, RuntimeStoragePaths, ToolKindSpec, ToolRuntime, ToolSpec, WorkflowSpec,
    WorkflowStepSpec,
    api::RunWorkflowOptions,
    model::config::{ToolInputKind, ToolInputSpec, versions::encode_document},
};
use tempfile::tempdir;

/// Build a `NickelDocument` with one tool and one workflow.
fn single_tool_doc(tool_id: &str, workflow_name: &str) -> NickelDocument {
    let (tool_name, tool_version) = tool_id.split_once('@').unwrap_or((tool_id, "1.0.0"));
    NickelDocument {
        tools: BTreeMap::from([(
            tool_id.to_string(),
            ToolSpec {
                kind: ToolKindSpec::Builtin {
                    name: tool_name.to_string(),
                    version: tool_version.to_string(),
                },
                name: tool_id.to_string(),
                version: tool_version.to_string(),
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
            name: workflow_name.to_string(),
            display_name: String::new(),
            description: String::new(),
            impure: false,
            steps: vec![WorkflowStepSpec {
                id: "s1".to_string(),
                tool: tool_id.to_string(),
                inputs: BTreeMap::from([("text".to_string(), workflow_name.to_string())]),
                outputs: BTreeMap::new(),
                max_retries: 0,
                depends_on: Vec::new(),
            }],
        }],
        ..NickelDocument::default()
    }
}

/// Build a `NickelDocument` with two tools and two workflows.
///
/// Both tools target the same echo builtin but use distinct tool_id keys
/// so they produce independent cache entries.
fn dual_tool_doc() -> NickelDocument {
    NickelDocument {
        tools: BTreeMap::from([
            (
                "echo-v1@1.0.0".to_string(),
                ToolSpec {
                    kind: ToolKindSpec::Builtin {
                        name: "echo".to_string(),
                        version: "1.0.0".to_string(),
                    },
                    name: "echo-v1@1.0.0".to_string(),
                    version: "1.0.0".to_string(),
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
            ),
            (
                "echo-v2@1.0.0".to_string(),
                ToolSpec {
                    kind: ToolKindSpec::Builtin {
                        name: "echo".to_string(),
                        version: "1.0.0".to_string(),
                    },
                    name: "echo-v2@1.0.0".to_string(),
                    version: "1.0.0".to_string(),
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
            ),
        ]),
        workflows: vec![
            WorkflowSpec {
                name: "default".to_string(),
                display_name: String::new(),
                description: String::new(),
                impure: false,
                steps: vec![WorkflowStepSpec {
                    id: "s1".to_string(),
                    tool: "echo-v1@1.0.0".to_string(),
                    inputs: BTreeMap::from([("text".to_string(), "default".to_string())]),
                    outputs: BTreeMap::new(),
                    max_retries: 0,
                    depends_on: Vec::new(),
                }],
            },
            WorkflowSpec {
                name: "updated".to_string(),
                display_name: String::new(),
                description: String::new(),
                impure: false,
                steps: vec![WorkflowStepSpec {
                    id: "s1".to_string(),
                    tool: "echo-v2@1.0.0".to_string(),
                    inputs: BTreeMap::from([("text".to_string(), "updated".to_string())]),
                    outputs: BTreeMap::new(),
                    max_retries: 0,
                    depends_on: Vec::new(),
                }],
            },
        ],
        ..NickelDocument::default()
    }
}

#[tokio::test]
async fn workflow_lifecycle_cache_gc_tool_update() {
    let dir = tempdir().expect("tempdir");
    let config_path = dir.path().join("conductor.ncl");

    let conductor = mediapm_conductor::SimpleConductor::new(
        RuntimeStoragePaths::new(dir.path()),
        InMemoryCas::new(),
    );

    // ---- Phase 1: first execution ----
    let doc = single_tool_doc("echo@1.0.0", "default");
    std::fs::write(&config_path, encode_document(doc).expect("encode")).expect("write config");

    let summary =
        conductor.run_workflow("default", RunWorkflowOptions::default()).await.expect("phase 1");
    assert_eq!(summary.executed_steps, 1, "phase 1: first run executes");
    assert_eq!(summary.cached_steps, 0, "phase 1: no cache on first run");

    // ---- Phase 2: cache hit ----
    let summary =
        conductor.run_workflow("default", RunWorkflowOptions::default()).await.expect("phase 2");
    assert_eq!(summary.executed_steps, 0, "phase 2: cached on second run");
    assert_eq!(summary.cached_steps, 1, "phase 2: cache hit on second run");

    // ---- Phase 3: GC preserves referenced instance ----
    conductor.run_gc().await.expect("phase 3: run_gc succeeds");
    let state = conductor.get_state().await.expect("phase 3: get_state");
    assert_eq!(state.tool_call_instances.len(), 1, "phase 3: instance survives GC");

    // ---- Phase 4: cache hit post-GC ----
    let summary =
        conductor.run_workflow("default", RunWorkflowOptions::default()).await.expect("phase 4");
    assert_eq!(summary.executed_steps, 0, "phase 4: cached post-GC");
    assert_eq!(summary.cached_steps, 1, "phase 4: cache hit post-GC");

    // ---- Phase 5: new tool_id + new workflow ----
    // Switch to a config exposing echo-v1 (old) + echo-v2 (new).  The old
    // "default" workflow still targets echo-v1 → cache hit; the new "updated"
    // workflow targets echo-v2 → fresh execution.
    let dual_doc = dual_tool_doc();
    std::fs::write(&config_path, encode_document(dual_doc).expect("encode dual"))
        .expect("write dual config");

    let summary =
        conductor.run_workflow("default", RunWorkflowOptions::default()).await.expect("phase 5a");
    assert_eq!(summary.executed_steps, 0, "phase 5a: old tool cached");
    assert_eq!(summary.cached_steps, 1, "phase 5a: old tool cached");

    let summary =
        conductor.run_workflow("updated", RunWorkflowOptions::default()).await.expect("phase 5b");
    assert_eq!(summary.executed_steps, 1, "phase 5b: new tool executes");
    assert_eq!(summary.cached_steps, 0, "phase 5b: new tool fresh");

    // ---- Phase 6: both workflows cached ----
    let summary =
        conductor.run_workflow("default", RunWorkflowOptions::default()).await.expect("phase 6a");
    assert_eq!(summary.executed_steps, 0, "phase 6a: default cached");
    assert_eq!(summary.cached_steps, 1, "phase 6a: default cached");

    let summary =
        conductor.run_workflow("updated", RunWorkflowOptions::default()).await.expect("phase 6b");
    assert_eq!(summary.executed_steps, 0, "phase 6b: updated cached");
    assert_eq!(summary.cached_steps, 1, "phase 6b: updated cached");

    // ---- Phase 7: GC preserves both instances ----
    conductor.run_gc().await.expect("phase 7: run_gc succeeds");
    let state = conductor.get_state().await.expect("phase 7: get_state");
    assert_eq!(state.tool_call_instances.len(), 2, "phase 7: both instances survive GC");

    // ---- Phase 8: both cached post-GC ----
    let summary =
        conductor.run_workflow("default", RunWorkflowOptions::default()).await.expect("phase 8a");
    assert_eq!(summary.executed_steps, 0, "phase 8a: default cached post-GC");
    assert_eq!(summary.cached_steps, 1, "phase 8a: default cached post-GC");

    let summary =
        conductor.run_workflow("updated", RunWorkflowOptions::default()).await.expect("phase 8b");
    assert_eq!(summary.executed_steps, 0, "phase 8b: updated cached post-GC");
    assert_eq!(summary.cached_steps, 1, "phase 8b: updated cached post-GC");
}
