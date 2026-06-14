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
    ConductorApi, MachineNickelDocument, OutputCaptureSpec, ToolKindSpec, ToolOutputSpec, ToolSpec,
    UserNickelDocument, WorkflowSpec, WorkflowStepSpec, encode_machine_document,
    encode_user_document,
};
use tempfile::tempdir;

/// Echo input binding for the `text` input key.
fn echo_text(value: &str) -> BTreeMap<String, mediapm_conductor::InputBinding> {
    BTreeMap::from([("text".to_string(), value.to_string().into())])
}

/// Builds user + machine documents with one tool and one workflow.
fn single_tool_docs(
    tool_id: &str,
    workflow_name: &str,
    instance_ttl: Option<u64>,
) -> (UserNickelDocument, MachineNickelDocument) {
    let user = UserNickelDocument {
        tools: BTreeMap::from([(
            tool_id.to_string(),
            ToolSpec {
                is_impure: false,
                kind: ToolKindSpec::Builtin {
                    name: "echo".to_string(),
                    version: "1.0.0".to_string(),
                },
                outputs: BTreeMap::from([(
                    "result".to_string(),
                    ToolOutputSpec { capture: OutputCaptureSpec::Stdout {}, allow_empty: false },
                )]),
                ..ToolSpec::default()
            },
        )]),
        workflows: BTreeMap::from([(
            workflow_name.to_string(),
            WorkflowSpec {
                name: None,
                description: None,
                steps: vec![WorkflowStepSpec {
                    id: "s1".to_string(),
                    tool: tool_id.to_string(),
                    inputs: echo_text(workflow_name),
                    depends_on: Vec::new(),
                    outputs: BTreeMap::new(),
                }],
            },
        )]),
        ..UserNickelDocument::default()
    };
    let machine = MachineNickelDocument {
        runtime: mediapm_conductor::RuntimeStorageConfig {
            instance_ttl_seconds: instance_ttl,
            ..mediapm_conductor::RuntimeStorageConfig::default()
        },
        ..MachineNickelDocument::default()
    };
    (user, machine)
}

/// Builds user + machine documents with two tools and two workflows.
///
/// Both tools target the same echo@1.0.0 builtin but use distinct tool_id keys
/// so they produce independent cache entries.
fn dual_tool_docs(instance_ttl: Option<u64>) -> (UserNickelDocument, MachineNickelDocument) {
    let user = UserNickelDocument {
        tools: BTreeMap::from([
            (
                "echo-v1@1.0.0".to_string(),
                ToolSpec {
                    is_impure: false,
                    kind: ToolKindSpec::Builtin {
                        name: "echo".to_string(),
                        version: "1.0.0".to_string(),
                    },
                    outputs: BTreeMap::from([(
                        "result".to_string(),
                        ToolOutputSpec {
                            capture: OutputCaptureSpec::Stdout {},
                            allow_empty: false,
                        },
                    )]),
                    ..ToolSpec::default()
                },
            ),
            (
                "echo-v2@1.0.0".to_string(),
                ToolSpec {
                    is_impure: false,
                    kind: ToolKindSpec::Builtin {
                        name: "echo".to_string(),
                        version: "1.0.0".to_string(),
                    },
                    outputs: BTreeMap::from([(
                        "result".to_string(),
                        ToolOutputSpec {
                            capture: OutputCaptureSpec::Stdout {},
                            allow_empty: false,
                        },
                    )]),
                    ..ToolSpec::default()
                },
            ),
        ]),
        workflows: BTreeMap::from([
            (
                "default".to_string(),
                WorkflowSpec {
                    name: None,
                    description: None,
                    steps: vec![WorkflowStepSpec {
                        id: "s1".to_string(),
                        tool: "echo-v1@1.0.0".to_string(),
                        inputs: echo_text("default"),
                        depends_on: Vec::new(),
                        outputs: BTreeMap::new(),
                    }],
                },
            ),
            (
                "updated".to_string(),
                WorkflowSpec {
                    name: None,
                    description: None,
                    steps: vec![WorkflowStepSpec {
                        id: "s1".to_string(),
                        tool: "echo-v2@1.0.0".to_string(),
                        inputs: echo_text("updated"),
                        depends_on: Vec::new(),
                        outputs: BTreeMap::new(),
                    }],
                },
            ),
        ]),
        ..UserNickelDocument::default()
    };
    let machine = MachineNickelDocument {
        runtime: mediapm_conductor::RuntimeStorageConfig {
            instance_ttl_seconds: instance_ttl,
            ..mediapm_conductor::RuntimeStorageConfig::default()
        },
        ..MachineNickelDocument::default()
    };
    (user, machine)
}

#[tokio::test]
async fn workflow_lifecycle_cache_gc_tool_update() {
    let conductor = mediapm_conductor::SimpleConductor::new(InMemoryCas::new());
    let dir = tempdir().expect("tempdir");
    let user_path = dir.path().join("conductor.ncl");
    let machine_path = dir.path().join("conductor.machine.ncl");

    // Generous TTL so GC preserves referenced instances while still exercising
    // the GC code path.
    let ttl = 86400u64;

    // ---- Phase 1: first execution ----
    let (user, machine) = single_tool_docs("echo-v1@1.0.0", "default", Some(ttl));
    std::fs::write(&user_path, encode_user_document(user).expect("encode user"))
        .expect("write user");
    std::fs::write(&machine_path, encode_machine_document(machine).expect("encode machine"))
        .expect("write machine");

    let summary = conductor.run_workflow(&user_path, &machine_path).await.expect("phase 1");
    assert_eq!(summary.executed_instances, 1, "phase 1: first run executes");
    assert_eq!(summary.cached_instances, 0, "phase 1: no cache on first run");

    // ---- Phase 2: cache hit ----
    let summary = conductor.run_workflow(&user_path, &machine_path).await.expect("phase 2");
    assert_eq!(summary.executed_instances, 0, "phase 2: cached on second run");
    assert_eq!(summary.cached_instances, 1, "phase 2: cache hit on second run");

    // ---- Phase 3: GC preserves referenced instance ----
    conductor.run_gc(None).await.expect("phase 3: run_gc succeeds");
    let state = conductor.get_state().await.expect("phase 3: get_state");
    assert_eq!(state.instances.len(), 1, "phase 3: instance survives GC");

    // ---- Phase 4: cache hit post-GC ----
    let summary = conductor.run_workflow(&user_path, &machine_path).await.expect("phase 4");
    assert_eq!(summary.executed_instances, 0, "phase 4: cached post-GC");
    assert_eq!(summary.cached_instances, 1, "phase 4: cache hit post-GC");

    // ---- Phase 5: new tool_id + new workflow ----
    // Switch to a config exposing echo-v1 (old) + echo-v2 (new).  The old
    // "default" workflow still targets echo-v1 → cache hit; the new "updated"
    // workflow targets echo-v2 → fresh execution.
    let (dual_user, dual_machine) = dual_tool_docs(Some(ttl));
    std::fs::write(&user_path, encode_user_document(dual_user).expect("encode dual user"))
        .expect("write dual user");
    std::fs::write(
        &machine_path,
        encode_machine_document(dual_machine).expect("encode dual machine"),
    )
    .expect("write dual machine");

    let summary = conductor.run_workflow(&user_path, &machine_path).await.expect("phase 5");
    assert_eq!(summary.executed_instances, 1, "phase 5: new tool executes");
    assert_eq!(summary.cached_instances, 1, "phase 5: old tool cached");

    // ---- Phase 6: both workflows cached ----
    let summary = conductor.run_workflow(&user_path, &machine_path).await.expect("phase 6");
    assert_eq!(summary.executed_instances, 0, "phase 6: neither executes");
    assert_eq!(summary.cached_instances, 2, "phase 6: both cached, each at own tool_id");

    // ---- Phase 7: GC preserves both instances ----
    conductor.run_gc(None).await.expect("phase 7: run_gc succeeds");
    let state = conductor.get_state().await.expect("phase 7: get_state");
    assert_eq!(state.instances.len(), 2, "phase 7: both instances survive GC");

    // ---- Phase 8: both cached post-GC ----
    let summary = conductor.run_workflow(&user_path, &machine_path).await.expect("phase 8");
    assert_eq!(summary.executed_instances, 0, "phase 8: neither executes post-GC");
    assert_eq!(summary.cached_instances, 2, "phase 8: both cached post-GC, tool_ids preserved");
}
