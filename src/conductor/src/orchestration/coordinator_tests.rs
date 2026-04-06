//! Workflow-level tests for the orchestration coordinator.
//!
//! These tests exercise the actor-backed runtime end to end so the coordinator,
//! document loader, execution hub, scheduler, state store, and step workers
//! continue to agree on the same workflow semantics.

use std::collections::BTreeMap;
use std::sync::Arc;

use mediapm_cas::{CasApi, InMemoryCas};
use tempfile::tempdir;

use crate::api::SchedulerTraceKind;
use crate::error::ConductorError;
use crate::model::config::{
    InputBinding, MachineNickelDocument, OutputCaptureSpec, OutputPolicy, ToolInputSpec,
    ToolKindSpec, ToolOutputSpec, ToolSpec, UserNickelDocument, WorkflowSpec, WorkflowStepSpec,
    encode_machine_document, encode_user_document,
};
use crate::model::state::{PersistenceFlags, merge_persistence_flags};

use super::WorkflowCoordinator;

/// Protects persistence-flag merge semantics used throughout output handling.
#[test]
fn persistence_flags_follow_intersection_and_union_rules() {
    let merged = merge_persistence_flags([
        PersistenceFlags { save: true, force_full: false },
        PersistenceFlags { save: false, force_full: false },
        PersistenceFlags { save: true, force_full: true },
    ]);

    assert!(!merged.save);
    assert!(merged.force_full);
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
                    ToolOutputSpec { capture: OutputCaptureSpec::Stdout {} },
                )]),
            },
        )]),
        workflows: BTreeMap::from([
            (
                "a".to_string(),
                WorkflowSpec {
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
                            OutputPolicy { save: Some(false), force_full: Some(false) },
                        )]),
                    }],
                },
            ),
            (
                "b".to_string(),
                WorkflowSpec {
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
                            OutputPolicy { save: Some(true), force_full: Some(true) },
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

    assert!(!output_ref.persistence.save);
    assert!(output_ref.persistence.force_full);
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
                    ToolOutputSpec { capture: OutputCaptureSpec::Stdout {} },
                )]),
            },
        )]),
        workflows: BTreeMap::from([(
            "wf".to_string(),
            WorkflowSpec {
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
                            OutputPolicy { save: Some(false), force_full: Some(false) },
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
