//! Workflow-level tests for the orchestration coordinator.
//!
//! These tests exercise the actor-backed runtime end to end so the coordinator,
//! document loader, execution hub, scheduler, state store, and step workers
//! continue to agree on the same workflow semantics.

use std::collections::BTreeMap;
use std::sync::Arc;

use mediapm_cas::{CasApi, CasError, FileSystemCas, InMemoryCas};
use tempfile::tempdir;

use crate::api::SchedulerTraceKind;
use crate::error::ConductorError;
use crate::model::config::{
    InputBinding, MachineNickelDocument, OutputCaptureSpec, OutputPolicy, ToolInputSpec,
    ToolKindSpec, ToolOutputSpec, ToolSpec, UserNickelDocument, WorkflowSpec, WorkflowStepSpec,
    decode_state_document, encode_machine_document, encode_user_document,
};
use crate::model::state::{
    OutputSaveMode, PersistenceFlags, decode_state, merge_persistence_flags,
};

use super::WorkflowCoordinator;

/// Mutates persisted CAS object bytes for one hash so integrity verification fails on next read.
fn corrupt_filesystem_cas_object(cas: &FileSystemCas, hash: mediapm_cas::Hash) {
    for path in [cas.object_path_for_hash(hash), cas.diff_path_for_hash(hash)] {
        if !path.exists() {
            continue;
        }

        let permissions = std::fs::metadata(&path).expect("metadata").permissions();
        if permissions.readonly() {
            #[cfg(unix)]
            {
                use std::os::unix::fs::PermissionsExt;

                let mode = permissions.mode();
                let writable_mode = mode | 0o200;
                if writable_mode != mode {
                    let mut writable_permissions = permissions;
                    writable_permissions.set_mode(writable_mode);
                    std::fs::set_permissions(&path, writable_permissions)
                        .expect("clear readonly (unix mode)");
                }
            }

            #[cfg(not(unix))]
            {
                #[expect(
                    clippy::permissions_set_readonly_false,
                    reason = "on non-Unix platforms we must clear the readonly flag before managed overwrite/delete operations can succeed"
                )]
                {
                    let mut writable_permissions = permissions;
                    writable_permissions.set_readonly(false);
                    std::fs::set_permissions(&path, writable_permissions)
                        .expect("clear readonly (platform fallback)");
                }
            }
        }

        std::fs::write(&path, b"corrupted-by-test").expect("mutate CAS payload");
    }
}

/// Protects persistence-flag merge semantics used throughout output handling.
#[test]
fn persistence_flags_follow_tri_state_max_ordering() {
    let merged = merge_persistence_flags([
        PersistenceFlags { save: OutputSaveMode::Saved },
        PersistenceFlags { save: OutputSaveMode::Unsaved },
        PersistenceFlags { save: OutputSaveMode::Full },
    ]);

    assert_eq!(merged.save, OutputSaveMode::Full);
}

/// Protects bootstrap execution when no Nickel files exist yet.
#[tokio::test]
async fn workflow_execution_bootstraps_when_nickel_files_are_missing() {
    let mut coordinator = WorkflowCoordinator::new(Arc::new(InMemoryCas::new()));
    let dir = tempdir().expect("tempdir");
    let user_path = dir.path().join("missing.conductor.ncl");
    let machine_path = dir.path().join("missing.conductor.machine.ncl");

    let summary = coordinator
        .run_workflow(&user_path, &machine_path)
        .await
        .expect("workflow should execute with bootstrap defaults");

    assert_eq!(summary.executed_instances, 1);
    assert_eq!(summary.cached_instances, 0);

    let state = coordinator.current_state().await.expect("state snapshot should load");
    assert_eq!(state.instances.len(), 1);
    assert!(machine_path.exists(), "machine config should be materialized by runtime");
}

/// Protects instance deduplication and persistence merging when missing outputs
/// are not referenced by any downstream step input.
#[tokio::test]
async fn dedup_merges_persistence_flags_without_rematerializing_unreferenced_outputs() {
    let cas = Arc::new(InMemoryCas::new());
    let mut coordinator = WorkflowCoordinator::new(cas.clone());
    let dir = tempdir().expect("tempdir");
    let user_path = dir.path().join("conductor.ncl");
    let machine_path = dir.path().join("conductor.machine.ncl");

    let user = UserNickelDocument {
        tools: BTreeMap::from([(
            "echo@1.0.0".to_string(),
            ToolSpec {
                is_impure: false,
                inputs: BTreeMap::from([("data".to_string(), ToolInputSpec::default())]),
                kind: ToolKindSpec::Builtin {
                    name: "echo".to_string(),
                    version: "1.0.0".to_string(),
                },
                outputs: BTreeMap::from([(
                    "result".to_string(),
                    ToolOutputSpec { capture: OutputCaptureSpec::Stdout {}, allow_empty: false },
                )]),
            },
        )]),
        workflows: BTreeMap::from([
            (
                "a".to_string(),
                WorkflowSpec {
                    name: None,
                    description: None,
                    steps: vec![WorkflowStepSpec {
                        id: "s1".to_string(),
                        tool: "echo@1.0.0".to_string(),
                        inputs: BTreeMap::from([(
                            "text".to_string(),
                            InputBinding::String("hello".to_string()),
                        )]),
                        depends_on: Vec::new(),
                        outputs: BTreeMap::from([(
                            "result".to_string(),
                            OutputPolicy { save: Some(OutputSaveMode::Unsaved) },
                        )]),
                    }],
                },
            ),
            (
                "b".to_string(),
                WorkflowSpec {
                    name: None,
                    description: None,
                    steps: vec![WorkflowStepSpec {
                        id: "s2".to_string(),
                        tool: "echo@1.0.0".to_string(),
                        inputs: BTreeMap::from([(
                            "text".to_string(),
                            InputBinding::String("hello".to_string()),
                        )]),
                        depends_on: Vec::new(),
                        outputs: BTreeMap::from([(
                            "result".to_string(),
                            OutputPolicy { save: Some(OutputSaveMode::Unsaved) },
                        )]),
                    }],
                },
            ),
        ]),
        ..UserNickelDocument::default()
    };

    let machine = MachineNickelDocument::default();
    std::fs::write(&user_path, encode_user_document(user).expect("encode user"))
        .expect("write user");
    std::fs::write(&machine_path, encode_machine_document(machine).expect("encode machine"))
        .expect("write machine");

    let summary_1 = coordinator
        .run_workflow(&user_path, &machine_path)
        .await
        .expect("first run should execute");

    assert_eq!(summary_1.executed_instances, 1);
    assert_eq!(summary_1.cached_instances, 1);

    let state_1 = coordinator.current_state().await.expect("state snapshot should load");
    assert_eq!(state_1.instances.len(), 1);
    let output_ref = state_1
        .instances
        .values()
        .next()
        .expect("instance")
        .outputs
        .get("result")
        .expect("result output");

    assert_eq!(output_ref.persistence.save, OutputSaveMode::Unsaved);
    assert!(
        !cas.exists(output_ref.hash).await.expect("exists check should succeed"),
        "save=false output should be dropped from CAS after run"
    );

    let summary_2 = coordinator
        .run_workflow(&user_path, &machine_path)
        .await
        .expect("second run should stay cache-hit when missing output is unreferenced");

    assert_eq!(summary_2.executed_instances, 0);
    assert_eq!(summary_2.cached_instances, 2);
    assert_eq!(summary_2.rematerialized_instances, 0);
}

/// Protects rerun behavior when one missing output is still referenced by
/// downstream `${step_output...}` workflow-step input bindings.
#[tokio::test]
async fn rematerializes_when_referenced_output_is_missing() {
    let cas = Arc::new(InMemoryCas::new());
    let mut coordinator = WorkflowCoordinator::new(cas.clone());
    let dir = tempdir().expect("tempdir");
    let user_path = dir.path().join("conductor.ncl");
    let machine_path = dir.path().join("conductor.machine.ncl");

    let user = UserNickelDocument {
        tools: BTreeMap::from([(
            "echo@1.0.0".to_string(),
            ToolSpec {
                is_impure: false,
                inputs: BTreeMap::new(),
                kind: ToolKindSpec::Builtin {
                    name: "echo".to_string(),
                    version: "1.0.0".to_string(),
                },
                outputs: BTreeMap::from([(
                    "result".to_string(),
                    ToolOutputSpec { capture: OutputCaptureSpec::Stdout {}, allow_empty: false },
                )]),
            },
        )]),
        workflows: BTreeMap::from([(
            "wf".to_string(),
            WorkflowSpec {
                name: None,
                description: None,
                steps: vec![
                    WorkflowStepSpec {
                        id: "producer".to_string(),
                        tool: "echo@1.0.0".to_string(),
                        inputs: BTreeMap::from([(
                            "text".to_string(),
                            InputBinding::String("hello".to_string()),
                        )]),
                        depends_on: Vec::new(),
                        outputs: BTreeMap::from([(
                            "result".to_string(),
                            OutputPolicy { save: Some(OutputSaveMode::Unsaved) },
                        )]),
                    },
                    WorkflowStepSpec {
                        id: "consumer".to_string(),
                        tool: "echo@1.0.0".to_string(),
                        inputs: BTreeMap::from([(
                            "text".to_string(),
                            InputBinding::String("${step_output.producer.result}".to_string()),
                        )]),
                        depends_on: vec!["producer".to_string()],
                        outputs: BTreeMap::new(),
                    },
                ],
            },
        )]),
        ..UserNickelDocument::default()
    };

    let machine = MachineNickelDocument::default();
    std::fs::write(&user_path, encode_user_document(user).expect("encode user"))
        .expect("write user");
    std::fs::write(&machine_path, encode_machine_document(machine).expect("encode machine"))
        .expect("write machine");

    let summary_1 = coordinator
        .run_workflow(&user_path, &machine_path)
        .await
        .expect("first run should execute");

    assert_eq!(summary_1.executed_instances, 2);

    let summary_2 = coordinator
        .run_workflow(&user_path, &machine_path)
        .await
        .expect("second run should rematerialize missing referenced output");

    assert_eq!(summary_2.executed_instances, 1);
    assert_eq!(summary_2.cached_instances, 1);
    assert_eq!(summary_2.rematerialized_instances, 1);
}

/// Protects pure-workflow recovery when referenced cached step outputs are corrupted in CAS.
#[tokio::test]
async fn pure_workflow_recovers_when_referenced_output_is_corrupted() {
    let temp = tempdir().expect("tempdir");
    let cas =
        Arc::new(FileSystemCas::open_for_tests(temp.path().join("cas")).await.expect("open cas"));
    let mut coordinator = WorkflowCoordinator::new(cas.clone());
    let user_path = temp.path().join("conductor.ncl");
    let machine_path = temp.path().join("conductor.machine.ncl");

    let user = UserNickelDocument {
        tools: BTreeMap::from([(
            "echo@1.0.0".to_string(),
            ToolSpec {
                is_impure: false,
                inputs: BTreeMap::new(),
                kind: ToolKindSpec::Builtin {
                    name: "echo".to_string(),
                    version: "1.0.0".to_string(),
                },
                outputs: BTreeMap::from([(
                    "result".to_string(),
                    ToolOutputSpec { capture: OutputCaptureSpec::Stdout {}, allow_empty: false },
                )]),
            },
        )]),
        workflows: BTreeMap::from([(
            "wf".to_string(),
            WorkflowSpec {
                name: None,
                description: None,
                steps: vec![
                    WorkflowStepSpec {
                        id: "producer".to_string(),
                        tool: "echo@1.0.0".to_string(),
                        inputs: BTreeMap::from([(
                            "text".to_string(),
                            InputBinding::String("hello".to_string()),
                        )]),
                        depends_on: Vec::new(),
                        outputs: BTreeMap::new(),
                    },
                    WorkflowStepSpec {
                        id: "consumer".to_string(),
                        tool: "echo@1.0.0".to_string(),
                        inputs: BTreeMap::from([(
                            "text".to_string(),
                            InputBinding::String("${step_output.producer.result}".to_string()),
                        )]),
                        depends_on: vec!["producer".to_string()],
                        outputs: BTreeMap::new(),
                    },
                ],
            },
        )]),
        ..UserNickelDocument::default()
    };

    std::fs::write(&user_path, encode_user_document(user).expect("encode user"))
        .expect("write user");
    std::fs::write(
        &machine_path,
        encode_machine_document(MachineNickelDocument::default()).expect("encode machine"),
    )
    .expect("write machine");

    let summary_1 = coordinator
        .run_workflow(&user_path, &machine_path)
        .await
        .expect("first run should execute");
    assert_eq!(summary_1.executed_instances, 2);

    let state = coordinator.current_state().await.expect("load current state");
    let mut corrupted_hashes = Vec::new();
    for output_hash in state
        .instances
        .values()
        .flat_map(|instance| instance.inputs.values().map(|input| input.hash))
    {
        if corrupted_hashes.contains(&output_hash) {
            continue;
        }
        corrupt_filesystem_cas_object(cas.as_ref(), output_hash);
        corrupted_hashes.push(output_hash);
    }

    assert!(!corrupted_hashes.is_empty(), "workflow should reference at least one output hash");

    let first_corrupted = *corrupted_hashes.first().expect("at least one output hash");
    let corruption = cas.get(first_corrupted).await.expect_err("corrupted hash should fail read");
    assert!(matches!(corruption, CasError::CorruptObject(_) | CasError::InvalidDelta(_)));

    let summary_2 = coordinator
        .run_workflow(&user_path, &machine_path)
        .await
        .expect("pure workflow should recover from corruption and retry");

    assert!(summary_2.executed_instances >= 1);
    let post_recovery = cas.get(first_corrupted).await;
    assert!(
        matches!(post_recovery, Ok(_) | Err(CasError::NotFound(_))),
        "recovery should not leave corrupted bytes readable: {post_recovery:?}"
    );
}

/// Protects impure-workflow fail-fast behavior when cached referenced outputs are corrupted.
#[tokio::test]
#[expect(
    clippy::too_many_lines,
    reason = "this item intentionally keeps end-to-end control flow together so ordering invariants remain explicit during maintenance"
)]
async fn impure_workflow_does_not_auto_recover_corrupted_output() {
    let temp = tempdir().expect("tempdir");
    let cas =
        Arc::new(FileSystemCas::open_for_tests(temp.path().join("cas")).await.expect("open cas"));
    let mut coordinator = WorkflowCoordinator::new(cas.clone());
    let user_path = temp.path().join("conductor.ncl");
    let machine_path = temp.path().join("conductor.machine.ncl");
    std::fs::write(temp.path().join("impure-source.txt"), b"hello")
        .expect("write import source fixture");

    let import_tool_id = format!(
        "{}@{}",
        mediapm_conductor_builtin_import::TOOL_NAME,
        mediapm_conductor_builtin_import::TOOL_VERSION
    );
    let echo_tool_id = format!(
        "{}@{}",
        mediapm_conductor_builtin_echo::TOOL_NAME,
        mediapm_conductor_builtin_echo::TOOL_VERSION
    );

    let user = UserNickelDocument {
        tools: BTreeMap::from([
            (
                import_tool_id.clone(),
                ToolSpec {
                    is_impure: true,
                    inputs: BTreeMap::new(),
                    kind: ToolKindSpec::Builtin {
                        name: mediapm_conductor_builtin_import::TOOL_NAME.to_string(),
                        version: mediapm_conductor_builtin_import::TOOL_VERSION.to_string(),
                    },
                    outputs: BTreeMap::from([(
                        "result".to_string(),
                        ToolOutputSpec {
                            capture: OutputCaptureSpec::Stdout {},
                            allow_empty: false,
                        },
                    )]),
                },
            ),
            (
                echo_tool_id.clone(),
                ToolSpec {
                    is_impure: false,
                    inputs: BTreeMap::new(),
                    kind: ToolKindSpec::Builtin {
                        name: mediapm_conductor_builtin_echo::TOOL_NAME.to_string(),
                        version: mediapm_conductor_builtin_echo::TOOL_VERSION.to_string(),
                    },
                    outputs: BTreeMap::from([(
                        "result".to_string(),
                        ToolOutputSpec {
                            capture: OutputCaptureSpec::Stdout {},
                            allow_empty: false,
                        },
                    )]),
                },
            ),
        ]),
        workflows: BTreeMap::from([(
            "wf".to_string(),
            WorkflowSpec {
                name: None,
                description: None,
                steps: vec![
                    WorkflowStepSpec {
                        id: "producer".to_string(),
                        tool: import_tool_id.clone(),
                        inputs: BTreeMap::from([
                            ("kind".to_string(), InputBinding::String("file".to_string())),
                            ("path_mode".to_string(), InputBinding::String("relative".to_string())),
                            (
                                "path".to_string(),
                                InputBinding::String("impure-source.txt".to_string()),
                            ),
                        ]),
                        depends_on: Vec::new(),
                        outputs: BTreeMap::new(),
                    },
                    WorkflowStepSpec {
                        id: "consumer".to_string(),
                        tool: echo_tool_id,
                        inputs: BTreeMap::from([(
                            "text".to_string(),
                            InputBinding::String("${step_output.producer.result}".to_string()),
                        )]),
                        depends_on: vec!["producer".to_string()],
                        outputs: BTreeMap::new(),
                    },
                ],
            },
        )]),
        ..UserNickelDocument::default()
    };

    std::fs::write(&user_path, encode_user_document(user).expect("encode user"))
        .expect("write user");
    std::fs::write(
        &machine_path,
        encode_machine_document(MachineNickelDocument::default()).expect("encode machine"),
    )
    .expect("write machine");

    coordinator.run_workflow(&user_path, &machine_path).await.expect("first run should execute");

    let state = coordinator.current_state().await.expect("load current state");
    let output_hash = state
        .instances
        .values()
        .find(|instance| instance.tool_name == import_tool_id.as_str())
        .and_then(|instance| instance.outputs.get("result").map(|output| output.hash))
        .expect("workflow should expose producer output hash");
    corrupt_filesystem_cas_object(cas.as_ref(), output_hash);

    let corruption = cas.get(output_hash).await.expect_err("corrupted hash should fail read");
    assert!(matches!(corruption, CasError::CorruptObject(_) | CasError::InvalidDelta(_)));

    let error = coordinator
        .run_workflow(&user_path, &machine_path)
        .await
        .expect_err("impure workflow should fail on corrupt referenced output");
    assert!(error.to_string().contains("impure"));
}

/// Returns a single-input/single-output builtin echo tool spec used in checkpoint tests.
fn echo_tool_spec() -> ToolSpec {
    ToolSpec {
        is_impure: false,
        inputs: BTreeMap::from([("text".to_string(), ToolInputSpec::default())]),
        kind: ToolKindSpec::Builtin { name: "echo".to_string(), version: "1.0.0".to_string() },
        outputs: BTreeMap::from([(
            "result".to_string(),
            ToolOutputSpec { capture: OutputCaptureSpec::Stdout {}, allow_empty: false },
        )]),
    }
}

/// Builds a two-step workflow where the producer (`echo@1.0.0`) succeeds and the
/// consumer (`fail@1.0.0`) always exits non-zero so a checkpoint is created.
fn failing_user_document(echo_tool: ToolSpec) -> UserNickelDocument {
    let failing_command = if cfg!(windows) {
        vec!["cmd".to_string(), "/C".to_string(), "exit 7".to_string()]
    } else {
        vec!["sh".to_string(), "-c".to_string(), "exit 7".to_string()]
    };
    let fail_tool = ToolSpec {
        is_impure: false,
        inputs: BTreeMap::from([("text".to_string(), ToolInputSpec::default())]),
        kind: ToolKindSpec::Executable {
            command: failing_command,
            env_vars: BTreeMap::new(),
            success_codes: vec![0],
        },
        outputs: BTreeMap::from([(
            "result".to_string(),
            ToolOutputSpec { capture: OutputCaptureSpec::Stdout {}, allow_empty: true },
        )]),
    };
    UserNickelDocument {
        tools: BTreeMap::from([
            ("echo@1.0.0".to_string(), echo_tool),
            ("fail@1.0.0".to_string(), fail_tool),
        ]),
        workflows: BTreeMap::from([(
            "wf".to_string(),
            WorkflowSpec {
                name: None,
                description: None,
                steps: vec![
                    WorkflowStepSpec {
                        id: "producer".to_string(),
                        tool: "echo@1.0.0".to_string(),
                        inputs: BTreeMap::from([(
                            "text".to_string(),
                            InputBinding::String("checkpoint me".to_string()),
                        )]),
                        depends_on: Vec::new(),
                        outputs: BTreeMap::from([(
                            "result".to_string(),
                            OutputPolicy { save: Some(OutputSaveMode::Full) },
                        )]),
                    },
                    WorkflowStepSpec {
                        id: "consumer".to_string(),
                        tool: "fail@1.0.0".to_string(),
                        inputs: BTreeMap::from([(
                            "text".to_string(),
                            InputBinding::String("${step_output.producer.result}".to_string()),
                        )]),
                        depends_on: vec!["producer".to_string()],
                        outputs: BTreeMap::new(),
                    },
                ],
            },
        )]),
        ..UserNickelDocument::default()
    }
}

/// Builds the recovered two-step workflow where both steps use `echo@1.0.0` so
/// the run after checkpoint reuse can complete successfully.
fn recovered_user_document(echo_tool: ToolSpec) -> UserNickelDocument {
    UserNickelDocument {
        tools: BTreeMap::from([("echo@1.0.0".to_string(), echo_tool)]),
        workflows: BTreeMap::from([(
            "wf".to_string(),
            WorkflowSpec {
                name: None,
                description: None,
                steps: vec![
                    WorkflowStepSpec {
                        id: "producer".to_string(),
                        tool: "echo@1.0.0".to_string(),
                        inputs: BTreeMap::from([(
                            "text".to_string(),
                            InputBinding::String("checkpoint me".to_string()),
                        )]),
                        depends_on: Vec::new(),
                        outputs: BTreeMap::from([(
                            "result".to_string(),
                            OutputPolicy { save: Some(OutputSaveMode::Full) },
                        )]),
                    },
                    WorkflowStepSpec {
                        id: "consumer".to_string(),
                        tool: "echo@1.0.0".to_string(),
                        inputs: BTreeMap::from([(
                            "text".to_string(),
                            InputBinding::String("${step_output.producer.result}".to_string()),
                        )]),
                        depends_on: vec!["producer".to_string()],
                        outputs: BTreeMap::new(),
                    },
                ],
            },
        )]),
        ..UserNickelDocument::default()
    }
}

/// Protects failure handling by persisting a partial checkpoint and allowing a
/// follow-up rerun to reuse completed upstream work instead of starting over.
#[tokio::test]
async fn failure_checkpoint_persists_partial_state_and_rerun_reuses_it() {
    let echo_tool = echo_tool_spec();
    let cas = Arc::new(InMemoryCas::new());
    let mut coordinator = WorkflowCoordinator::new(cas.clone());
    let dir = tempdir().expect("tempdir");
    let user_path = dir.path().join("conductor.ncl");
    let machine_path = dir.path().join("conductor.machine.ncl");
    let state_path = dir.path().join(".conductor").join("state.ncl");

    std::fs::write(
        &user_path,
        encode_user_document(failing_user_document(echo_tool.clone())).expect("encode user"),
    )
    .expect("write user");
    std::fs::write(
        &machine_path,
        encode_machine_document(MachineNickelDocument::default()).expect("encode machine"),
    )
    .expect("write machine");

    coordinator
        .run_workflow(&user_path, &machine_path)
        .await
        .expect_err("second step should fail after producer checkpoint is created");

    let state_document =
        decode_state_document(&std::fs::read(&state_path).expect("read state doc"))
            .expect("decode state doc");
    let state_pointer =
        state_document.state_pointer.expect("failed run should persist checkpoint pointer");
    let persisted_state =
        decode_state(&cas.get(state_pointer).await.expect("load checkpoint bytes"))
            .expect("decode checkpoint state");
    assert_eq!(
        persisted_state.instances.len(),
        1,
        "only completed upstream work should be checkpointed"
    );
    assert!(
        persisted_state.instances.values().all(|i| i.tool_name == "echo@1.0.0"),
        "checkpoint should only include the completed producer instance"
    );

    let current_state = coordinator.current_state().await.expect("load current state");
    assert_eq!(current_state.instances.len(), persisted_state.instances.len());
    let current_instance =
        current_state.instances.values().next().expect("current checkpointed instance");
    let persisted_instance =
        persisted_state.instances.values().next().expect("persisted checkpointed instance");
    assert_eq!(current_instance.tool_name, persisted_instance.tool_name);
    assert_eq!(current_instance.outputs, persisted_instance.outputs);
    assert_eq!(current_instance.inputs.len(), persisted_instance.inputs.len());
    for (input_name, current_input) in &current_instance.inputs {
        let persisted_input = persisted_instance.inputs.get(input_name).expect("persisted input");
        assert_eq!(current_input.hash, persisted_input.hash);
    }

    std::fs::write(
        &user_path,
        encode_user_document(recovered_user_document(echo_tool)).expect("encode recovered user"),
    )
    .expect("rewrite user");

    let summary = coordinator
        .run_workflow(&user_path, &machine_path)
        .await
        .expect("rerun should reuse checkpointed producer and finish consumer");

    assert_eq!(summary.cached_instances, 1);
    assert_eq!(summary.executed_instances, 1);
    assert_eq!(summary.rematerialized_instances, 0);
}

/// Protects state-schema validation when machine state pointers reference unsupported blobs.
#[tokio::test]
async fn unsupported_state_schema_is_rejected() {
    let cas = Arc::new(InMemoryCas::new());
    let mut coordinator = WorkflowCoordinator::new(cas.clone());
    let dir = tempdir().expect("tempdir");
    let user_path = dir.path().join("conductor.ncl");
    let machine_path = dir.path().join("conductor.machine.ncl");

    let unsupported_state = serde_json::json!({
        "version": 0,
        "instances": {}
    });

    let state_hash = cas
        .put(serde_json::to_vec(&unsupported_state).expect("serialize state"))
        .await
        .expect("put state");

    let machine = MachineNickelDocument { state_pointer: Some(state_hash), ..Default::default() };
    std::fs::write(&machine_path, encode_machine_document(machine).expect("encode machine"))
        .expect("write machine");
    std::fs::write(
        &user_path,
        encode_user_document(UserNickelDocument::default()).expect("encode user"),
    )
    .expect("write user");

    let result = coordinator.run_workflow(&user_path, &machine_path).await;
    match result {
        Err(ConductorError::Workflow(message)) => {
            assert!(message.contains("unsupported orchestration state schema version"));
        }
        other => panic!("expected schema rejection, got {other:?}"),
    }
}

/// Protects recovery when machine state pointers reference missing CAS blobs.
#[tokio::test]
async fn missing_state_pointer_blob_falls_back_to_empty_state() {
    let mut coordinator = WorkflowCoordinator::new(Arc::new(InMemoryCas::new()));
    let dir = tempdir().expect("tempdir");
    let user_path = dir.path().join("missing.conductor.ncl");
    let machine_path = dir.path().join("conductor.machine.ncl");

    let missing_pointer = mediapm_cas::Hash::from_content(b"missing-state-pointer");
    let machine = MachineNickelDocument {
        state_pointer: Some(missing_pointer),
        ..MachineNickelDocument::default()
    };
    std::fs::write(&machine_path, encode_machine_document(machine).expect("encode machine"))
        .expect("write machine");

    let summary = coordinator
        .run_workflow(&user_path, &machine_path)
        .await
        .expect("workflow should recover from missing state pointer blob");

    // Missing user document triggers bootstrap workflow with one deterministic step.
    assert_eq!(summary.executed_instances, 1);
}

/// Protects integer-only validation for executable process success codes.
#[tokio::test]
async fn fractional_success_codes_are_rejected() {
    let mut coordinator = WorkflowCoordinator::new(Arc::new(InMemoryCas::new()));
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
    .expect("write user");
    std::fs::write(
        &machine_path,
        encode_machine_document(MachineNickelDocument::default()).expect("encode machine"),
    )
    .expect("write machine");

    let result = coordinator.run_workflow(&user_path, &machine_path).await;
    match result {
        Err(ConductorError::Workflow(message)) => {
            assert!(
                message.contains("success_codes") || message.contains("Integer"),
                "expected integer-contract error mentioning success_codes or Integer, got: {message}"
            );
        }
        other => panic!("expected workflow validation failure, got {other:?}"),
    }
}

/// Protects scheduler diagnostics after one workflow run through the actor stack.
#[tokio::test]
async fn diagnostics_include_worker_queue_metrics_and_trace_events() {
    let mut coordinator = WorkflowCoordinator::new(Arc::new(InMemoryCas::new()));
    let dir = tempdir().expect("tempdir");
    let user_path = dir.path().join("missing.conductor.ncl");
    let machine_path = dir.path().join("missing.conductor.machine.ncl");

    let _summary =
        coordinator.run_workflow(&user_path, &machine_path).await.expect("workflow should execute");

    let diagnostics = coordinator.runtime_diagnostics().await.expect("diagnostics should load");
    assert!(diagnostics.worker_pool_size >= 1, "expected active worker pool");
    assert_eq!(diagnostics.worker_pool_size, diagnostics.workers.len());
    assert!(
        diagnostics.workers.iter().any(|worker| worker.assigned_steps_total > 0),
        "expected at least one worker to receive assignments"
    );

    let mut saw_level_planned = false;
    let mut saw_step_assigned = false;
    let mut saw_step_completed = false;
    for event in diagnostics.recent_traces {
        match event.kind {
            SchedulerTraceKind::LevelPlanned { .. } => saw_level_planned = true,
            SchedulerTraceKind::StepAssigned { .. } => saw_step_assigned = true,
            SchedulerTraceKind::StepCompleted { .. } => saw_step_completed = true,
            SchedulerTraceKind::RpcFallback { .. } | SchedulerTraceKind::EwmaUpdated { .. } => {}
        }
    }

    assert!(saw_level_planned, "expected level-planned trace event");
    assert!(saw_step_assigned, "expected step-assigned trace event");
    assert!(saw_step_completed, "expected step-completed trace event");
}

/// Protects explicit `depends_on` ordering when no `${step_output...}` binding
/// is present.
#[test]
fn explicit_depends_on_creates_topological_edge() {
    let workflow = WorkflowSpec {
        name: None,
        description: None,
        steps: vec![
            WorkflowStepSpec {
                id: "prepare".to_string(),
                tool: "echo@1.0.0".to_string(),
                inputs: BTreeMap::new(),
                depends_on: Vec::new(),
                outputs: BTreeMap::new(),
            },
            WorkflowStepSpec {
                id: "consume_side_effect".to_string(),
                tool: "echo@1.0.0".to_string(),
                inputs: BTreeMap::new(),
                depends_on: vec!["prepare".to_string()],
                outputs: BTreeMap::new(),
            },
        ],
    };

    let levels = WorkflowCoordinator::<InMemoryCas>::topological_levels("wf", &workflow)
        .expect("depends_on edge should produce a valid topological order");

    assert_eq!(levels.len(), 2);
    assert_eq!(levels[0][0].id, "prepare");
    assert_eq!(levels[1][0].id, "consume_side_effect");
}

/// Protects the explicit-edge contract: `${step_output...}` references must
/// be mirrored in `depends_on`.
#[test]
fn step_output_reference_requires_matching_depends_on() {
    let workflow = WorkflowSpec {
        name: None,
        description: None,
        steps: vec![
            WorkflowStepSpec {
                id: "produce".to_string(),
                tool: "echo@1.0.0".to_string(),
                inputs: BTreeMap::new(),
                depends_on: Vec::new(),
                outputs: BTreeMap::new(),
            },
            WorkflowStepSpec {
                id: "consume".to_string(),
                tool: "echo@1.0.0".to_string(),
                inputs: BTreeMap::from([(
                    "text".to_string(),
                    InputBinding::String("${step_output.produce.result}".to_string()),
                )]),
                depends_on: Vec::new(),
                outputs: BTreeMap::new(),
            },
        ],
    };

    let result = WorkflowCoordinator::<InMemoryCas>::topological_levels("wf", &workflow);
    match result {
        Err(ConductorError::Workflow(message)) => {
            assert!(message.contains("does not list 'produce' in depends_on"));
            assert!(message.contains("step_output.produce"));
        }
        other => panic!("expected explicit depends_on validation failure, got {other:?}"),
    }
}

/// Protects user-facing workflow label selection when metadata provides a
/// display name.
#[test]
fn workflow_display_name_prefers_metadata_name_when_present() {
    let named = WorkflowSpec {
        name: Some("friendly workflow".to_string()),
        description: Some("demo description".to_string()),
        steps: Vec::new(),
    };
    let unnamed = WorkflowSpec { name: None, description: None, steps: Vec::new() };

    assert_eq!(
        WorkflowCoordinator::<InMemoryCas>::workflow_display_name("wf.id", &named),
        "friendly workflow"
    );
    assert_eq!(
        WorkflowCoordinator::<InMemoryCas>::workflow_display_name("wf.id", &unnamed),
        "wf.id"
    );
}

/// Protects workflow progress messaging so active step ids stay visible while
/// level execution is still in flight.
#[test]
fn workflow_level_progress_message_includes_running_step_preview() {
    let step = WorkflowStepSpec {
        id: "download_source".to_string(),
        tool: "yt-dlp@latest".to_string(),
        inputs: BTreeMap::new(),
        depends_on: Vec::new(),
        outputs: BTreeMap::new(),
    };

    let message = WorkflowCoordinator::<InMemoryCas>::workflow_level_progress_message(
        "rickroll",
        0,
        13,
        &[&step],
    );

    assert!(message.contains("rickroll"));
    assert!(message.contains("download_source"));
    assert!(message.contains("  "), "progress message should use two-space separator");
    assert!(!message.contains('·'));
    assert!(!message.contains('%'));
}

/// Protects compact level previews when many steps share one level.
#[test]
fn workflow_level_progress_message_compacts_multi_step_preview() {
    let step_a = WorkflowStepSpec {
        id: "prepare".to_string(),
        tool: "echo@1.0.0".to_string(),
        inputs: BTreeMap::new(),
        depends_on: Vec::new(),
        outputs: BTreeMap::new(),
    };
    let step_b = WorkflowStepSpec {
        id: "download".to_string(),
        tool: "yt-dlp@latest".to_string(),
        inputs: BTreeMap::new(),
        depends_on: Vec::new(),
        outputs: BTreeMap::new(),
    };
    let step_c = WorkflowStepSpec {
        id: "normalize".to_string(),
        tool: "rsgain@latest".to_string(),
        inputs: BTreeMap::new(),
        depends_on: Vec::new(),
        outputs: BTreeMap::new(),
    };

    let message = WorkflowCoordinator::<InMemoryCas>::workflow_level_progress_message(
        "library sync",
        3,
        13,
        &[&step_a, &step_b, &step_c],
    );

    assert!(message.contains("prepare"));
    assert!(message.contains("download"));
    assert!(message.contains("+1 more"));
    assert!(!message.contains("normalize"));
    assert!(message.contains("  "), "progress message should use two-space separator");
    assert!(!message.contains('·'));
    assert!(!message.contains('%'));
}
