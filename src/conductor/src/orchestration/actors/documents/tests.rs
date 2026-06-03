//! Integration tests for `DocumentLoaderActor` merge and validation invariants.
use std::collections::BTreeMap;

use mediapm_cas::Hash;
use tempfile::tempdir;

use crate::api::RunWorkflowOptions;
use crate::error::ConductorError;
use crate::model::config::{
    MachineNickelDocument, OutputCaptureSpec, RuntimeStorageConfig, ToolConfigSpec, ToolKindSpec,
    ToolOutputSpec, ToolSpec, UserNickelDocument, WorkflowSpec,
    default_runtime_inherited_env_vars_for_host, encode_machine_document, encode_user_document,
};

use super::DocumentLoaderActor;

/// Protects the merge invariant that conflicting duplicate definitions fail fast.
#[test]
fn conflicting_duplicate_field_across_documents_is_rejected() {
    let dir = tempdir().expect("tempdir");
    let user_path = dir.path().join("conductor.ncl");
    let machine_path = dir.path().join("conductor.machine.ncl");
    let state_path = dir.path().join(".conductor").join("state.ncl");

    std::fs::write(
        &user_path,
        r#"
{
    version = 1,
    external_data = {
        "blake3:0000000000000000000000000000000000000000000000000000000000000000" = {
            description = "user value",
        },
    },
}
"#,
    )
    .expect("write user");
    std::fs::write(
        &machine_path,
        r#"
{
    version = 1,
    external_data = {
        "blake3:0000000000000000000000000000000000000000000000000000000000000000" = {
            description = "machine value",
        },
    },
}
"#,
    )
    .expect("write machine");

    let result = DocumentLoaderActor::load_and_unify_documents(
        &user_path,
        &machine_path,
        &state_path,
        &RunWorkflowOptions::default(),
        &mut None,
    );
    match result {
        Err(ConductorError::Workflow(message)) => {
            assert!(message.contains(
                "external_data.blake3:0000000000000000000000000000000000000000000000000000000000000000"
            ));
        }
        other => panic!("expected workflow merge conflict, got {other:?}"),
    }
}

/// Protects the invariant that tool configs may only target declared tools.
#[test]
fn tool_config_for_unknown_merged_tool_is_rejected() {
    let dir = tempdir().expect("tempdir");
    let user_path = dir.path().join("conductor.ncl");
    let machine_path = dir.path().join("conductor.machine.ncl");
    let state_path = dir.path().join(".conductor").join("state.ncl");

    let user = UserNickelDocument {
        tools: BTreeMap::from([("known_tool@1.0.0".to_string(), ToolSpec::default())]),
        workflows: BTreeMap::from([("w".to_string(), WorkflowSpec::default())]),
        ..UserNickelDocument::default()
    };

    let machine = MachineNickelDocument {
        external_data: BTreeMap::from([(
            Hash::from_content(b"machine"),
            crate::model::config::ExternalContentRef {
                description: Some("fixture root".to_string()),
                save: None,
            },
        )]),
        tool_configs: BTreeMap::from([(
            "unknown_tool@v9.9.9".to_string(),
            ToolConfigSpec {
                max_concurrent_calls: -1,
                max_retries: -1,
                description: Some("unknown tool test config".to_string()),
                input_defaults: BTreeMap::new(),
                env_vars: BTreeMap::new(),
                content_map: Some(BTreeMap::from([(
                    "bin/tool".to_string(),
                    Hash::from_content(b"machine"),
                )])),
            },
        )]),
        ..MachineNickelDocument::default()
    };

    std::fs::write(&user_path, encode_user_document(user).expect("encode user"))
        .expect("write user");
    std::fs::write(&machine_path, encode_machine_document(machine).expect("encode machine"))
        .expect("write machine");

    let result = DocumentLoaderActor::load_and_unify_documents(
        &user_path,
        &machine_path,
        &state_path,
        &RunWorkflowOptions::default(),
        &mut None,
    );
    match result {
        Err(ConductorError::Workflow(message)) => {
            assert!(message.contains("references unknown tool"));
        }
        other => panic!("expected workflow conflict error, got {other:?}"),
    }
}

/// Protects the invariant that tool config max concurrency must be -1 or positive.
#[test]
fn invalid_tool_config_max_concurrent_calls_is_rejected() {
    let dir = tempdir().expect("tempdir");
    let user_path = dir.path().join("conductor.ncl");
    let machine_path = dir.path().join("conductor.machine.ncl");
    let state_path = dir.path().join(".conductor").join("state.ncl");

    std::fs::write(
        &user_path,
        r#"
{
    version = 1,
    tools = {
        "bad_tool@1.0.0" = {
            kind = "builtin",
            name = "echo",
            version = "1.0.0",
        },
    },
    tool_configs = {
        "bad_tool@1.0.0" = {
            max_concurrent_calls = 0,
        },
    },
    workflows = {
        w = { steps = [] },
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

    let result = DocumentLoaderActor::load_and_unify_documents(
        &user_path,
        &machine_path,
        &state_path,
        &RunWorkflowOptions::default(),
        &mut None,
    );
    match result {
        Err(ConductorError::Workflow(message)) => {
            assert!(message.contains("max_concurrent_calls must be -1 or a positive integer"));
        }
        other => panic!("expected workflow error, got {other:?}"),
    }
}

/// Protects the invariant that tool config retry values must be `-1` or non-negative.
#[test]
fn invalid_tool_config_max_retries_is_rejected() {
    let dir = tempdir().expect("tempdir");
    let user_path = dir.path().join("conductor.ncl");
    let machine_path = dir.path().join("conductor.machine.ncl");
    let state_path = dir.path().join(".conductor").join("state.ncl");

    std::fs::write(
        &user_path,
        r#"
{
    version = 1,
    tools = {
        "bad_tool@1.0.0" = {
            kind = "builtin",
            name = "echo",
            version = "1.0.0",
        },
    },
    tool_configs = {
        "bad_tool@1.0.0" = {
            max_retries = -2,
        },
    },
    workflows = {
        w = { steps = [] },
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

    let result = DocumentLoaderActor::load_and_unify_documents(
        &user_path,
        &machine_path,
        &state_path,
        &RunWorkflowOptions::default(),
        &mut None,
    );
    match result {
        Err(ConductorError::Workflow(message)) => {
            assert!(message.contains("max_retries must be -1 or a non-negative integer"));
        }
        other => panic!("expected workflow error, got {other:?}"),
    }
}

/// Protects invariant that builtin tools cannot receive runtime env maps
/// from `tool_configs`.
#[test]
fn builtin_tool_config_env_vars_is_rejected() {
    let dir = tempdir().expect("tempdir");
    let user_path = dir.path().join("conductor.ncl");
    let machine_path = dir.path().join("conductor.machine.ncl");
    let state_path = dir.path().join(".conductor").join("state.ncl");

    std::fs::write(
        &user_path,
        r#"
{
    version = 1,
    tools = {
        "echo@1.0.0" = {
            kind = "builtin",
            name = "echo",
            version = "1.0.0",
        },
    },
    tool_configs = {
        "echo@1.0.0" = {
            env_vars = { DEMO = "1" },
        },
    },
    workflows = {
        w = { steps = [] },
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

    let result = DocumentLoaderActor::load_and_unify_documents(
        &user_path,
        &machine_path,
        &state_path,
        &RunWorkflowOptions::default(),
        &mut None,
    );
    match result {
        Err(ConductorError::Workflow(message)) => {
            assert!(
                message.contains("env_vars is invalid for builtin tools")
                    || message.contains("tool_configs.env_vars"),
                "unexpected error message: {message}"
            );
        }
        other => panic!("expected workflow error, got {other:?}"),
    }
}

/// Protects invariant that executable environment keys must not be
/// duplicated across `tools.<tool>.env_vars` and
/// `tool_configs.<tool>.env_vars`.
#[test]
fn duplicate_env_key_across_tool_and_tool_config_is_rejected() {
    let dir = tempdir().expect("tempdir");
    let user_path = dir.path().join("conductor.ncl");
    let machine_path = dir.path().join("conductor.machine.ncl");
    let state_path = dir.path().join(".conductor").join("state.ncl");

    std::fs::write(
        &user_path,
        r#"
{
    version = 1,
    tools = {
        "runner@1.0.0" = {
            kind = "executable",
            command = ["bin/tool"],
            env_vars = {
                DUPLICATE = "from_tool",
            },
            outputs = {
                result = {
                    capture = { kind = "stdout" },
                },
            },
        },
    },
    tool_configs = {
        "runner@1.0.0" = {
            env_vars = {
                DUPLICATE = "from_config",
            },
        },
    },
    workflows = {
        w = {
            steps = [
                {
                    id = "s",
                    tool = "runner@1.0.0",
                },
            ],
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

    let result = DocumentLoaderActor::load_and_unify_documents(
        &user_path,
        &machine_path,
        &state_path,
        &RunWorkflowOptions::default(),
        &mut None,
    );
    match result {
        Err(ConductorError::Workflow(message)) => {
            assert!(message.contains("duplicate environment key"));
            assert!(message.contains("tools.runner@1.0.0.env_vars"));
            assert!(message.contains("tool_configs.runner@1.0.0.env_vars"));
        }
        other => panic!("expected workflow error, got {other:?}"),
    }
}

/// Protects strict-safe behavior that rejects conflicting tool redefinitions.
#[test]
fn tool_redefinition_is_rejected_by_default() {
    let dir = tempdir().expect("tempdir");
    let user_path = dir.path().join("conductor.ncl");
    let machine_path = dir.path().join("conductor.machine.ncl");
    let state_path = dir.path().join(".conductor").join("state.ncl");

    let user = UserNickelDocument {
        tools: BTreeMap::from([(
            "echo@1.0.0".to_string(),
            ToolSpec {
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
        ..UserNickelDocument::default()
    };

    let machine = MachineNickelDocument {
        tools: BTreeMap::from([(
            "echo@1.0.0".to_string(),
            ToolSpec {
                kind: ToolKindSpec::Builtin {
                    name: "echo".to_string(),
                    version: "2.0.0".to_string(),
                },
                outputs: BTreeMap::from([(
                    "result".to_string(),
                    ToolOutputSpec { capture: OutputCaptureSpec::Stdout {}, allow_empty: false },
                )]),
                ..ToolSpec::default()
            },
        )]),
        ..MachineNickelDocument::default()
    };

    std::fs::write(&user_path, encode_user_document(user).expect("encode user"))
        .expect("write user");
    std::fs::write(&machine_path, encode_machine_document(machine).expect("encode machine"))
        .expect("write machine");

    let result = DocumentLoaderActor::load_and_unify_documents(
        &user_path,
        &machine_path,
        &state_path,
        &RunWorkflowOptions::default(),
        &mut None,
    );
    match result {
        Err(ConductorError::Workflow(message)) => {
            assert!(message.contains("already defined with a different config"));
            assert!(message.contains("allow-tool-redefinition"));
            assert!(message.contains("tool.kind.changed=true"));
        }
        other => panic!("expected strict redefinition rejection, got {other:?}"),
    }
}

/// Protects explicit override mode that allows tool redefinition updates.
#[test]
fn tool_redefinition_is_allowed_with_override_option() {
    let dir = tempdir().expect("tempdir");
    let user_path = dir.path().join("conductor.ncl");
    let machine_path = dir.path().join("conductor.machine.ncl");
    let state_path = dir.path().join(".conductor").join("state.ncl");

    let user = UserNickelDocument {
        tools: BTreeMap::from([(
            "echo@1.0.0".to_string(),
            ToolSpec {
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
        ..UserNickelDocument::default()
    };

    let machine = MachineNickelDocument {
        tools: BTreeMap::from([(
            "echo@1.0.0".to_string(),
            ToolSpec {
                kind: ToolKindSpec::Builtin {
                    name: "echo".to_string(),
                    version: "2.0.0".to_string(),
                },
                outputs: BTreeMap::from([(
                    "result".to_string(),
                    ToolOutputSpec { capture: OutputCaptureSpec::Stdout {}, allow_empty: false },
                )]),
                ..ToolSpec::default()
            },
        )]),
        ..MachineNickelDocument::default()
    };

    std::fs::write(&user_path, encode_user_document(user).expect("encode user"))
        .expect("write user");
    std::fs::write(&machine_path, encode_machine_document(machine).expect("encode machine"))
        .expect("write machine");

    let loaded = DocumentLoaderActor::load_and_unify_documents(
        &user_path,
        &machine_path,
        &state_path,
        &RunWorkflowOptions { allow_tool_redefinition: true, ..RunWorkflowOptions::default() },
        &mut None,
    )
    .expect("override option should allow redefinition");

    let merged_tool =
        loaded.machine_document.tools.get("echo@1.0.0").expect("merged machine tool should exist");
    let ToolKindSpec::Builtin { version, .. } = &merged_tool.kind else {
        panic!("expected builtin process");
    };
    assert_eq!(version, "1.0.0");
}

/// Protects invariant that user-declared runtime storage path overrides are
/// not copied into `conductor.machine.ncl`, while host-default inherited
/// env names are materialized when omitted.
#[test]
fn machine_document_is_not_backfilled_with_user_runtime_storage() {
    let dir = tempdir().expect("tempdir");
    let user_path = dir.path().join("conductor.ncl");
    let machine_path = dir.path().join("conductor.machine.ncl");
    let state_path = dir.path().join(".conductor").join("state.ncl");

    let user = UserNickelDocument {
        runtime: RuntimeStorageConfig {
            conductor_dir: Some(".runtime".to_string()),
            conductor_state_config: Some(".runtime/state.ncl".to_string()),
            cas_store_dir: Some(".runtime/store".to_string()),
            conductor_schema_dir: Some(".runtime/config/conductor".to_string()),
            inherited_env_vars: None,
            instance_ttl_seconds: None,
        },
        tools: BTreeMap::from([(
            "echo@1.0.0".to_string(),
            ToolSpec {
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
        workflows: BTreeMap::from([("w".to_string(), WorkflowSpec::default())]),
        ..UserNickelDocument::default()
    };

    std::fs::write(&user_path, encode_user_document(user).expect("encode user"))
        .expect("write user");
    std::fs::write(
        &machine_path,
        encode_machine_document(MachineNickelDocument::default()).expect("encode machine"),
    )
    .expect("write machine");

    let loaded = DocumentLoaderActor::load_and_unify_documents(
        &user_path,
        &machine_path,
        &state_path,
        &RunWorkflowOptions::default(),
        &mut None,
    )
    .expect("documents should load and unify");

    assert!(loaded.machine_document.runtime.conductor_dir.is_none());
    assert!(loaded.machine_document.runtime.conductor_state_config.is_none());
    assert!(loaded.machine_document.runtime.cas_store_dir.is_none());
    assert!(loaded.machine_document.runtime.conductor_schema_dir.is_none());

    let expected_defaults = default_runtime_inherited_env_vars_for_host();
    if expected_defaults.is_empty() {
        assert!(loaded.machine_document.runtime.inherited_env_vars.is_none());
    } else {
        assert_eq!(loaded.machine_document.runtime.inherited_env_vars, Some(expected_defaults));
    }
}
