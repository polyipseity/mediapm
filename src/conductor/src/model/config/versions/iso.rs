//! `IsoPrime` document bridges, encoding, decoding, and configuration source evaluation.
//!
//! # Why this file cannot be split further
//!
//! `user_runtime_iso` (~416 lines, already annotated with
//! `#[expect(clippy::too_many_lines)]`) and `vet_latest_envelope` (~377 lines,
//! also annotated) are single function bodies that together account for ~68% of
//! this file.  A function body cannot be split across Rust source files, so any
//! sub-module split would leave those two functions in the same file while only
//! relocating the much smaller helpers (~122 lines total), yielding no meaningful
//! reduction in file size.

use std::collections::BTreeSet;

use fp_library::brands::RcBrand;
use fp_library::types::optics::IsoPrime;
use serde::Serialize;
use serde_json::Value;

use crate::error::ConductorError;
use crate::model::config::{
    ExternalContentRef, ImpureTimestamp, InputBinding, MachineNickelDocument,
    NickelDocumentMetadata, OutputCaptureSpec, OutputPolicy, ParsedInputBindingSegment,
    StateNickelDocument, ToolConfigSpec, ToolInputKind, ToolInputSpec, ToolKindSpec,
    ToolOutputSpec, ToolSpec, UserNickelDocument, WorkflowSpec, WorkflowStepSpec,
    parse_input_binding,
};
use crate::model::state::OutputSaveMode;

use super::nickel_io::{
    TempNickelWorkspace, evaluate_document_source, evaluate_main_file_as,
    latest_version_among_sources, read_document_version_marker, render_document_as_nickel,
    validate_state_document_source_shape, write_nickel_file,
};
use super::{
    MACHINE_NICKEL_VERSION, MOD_NCL_SOURCE, USER_NICKEL_VERSION, latest, resolve_version_contract,
    v_latest,
};

/// Converts persisted latest-schema impure timestamps into runtime shape.
fn impure_timestamps_from_latest(
    latest_map: std::collections::BTreeMap<
        String,
        std::collections::BTreeMap<String, v_latest::ImpureTimestampLatest>,
    >,
) -> std::collections::BTreeMap<String, std::collections::BTreeMap<String, ImpureTimestamp>> {
    latest_map
        .into_iter()
        .map(|(workflow_id, steps)| {
            let mapped_steps = steps
                .into_iter()
                .map(|(step_id, timestamp)| {
                    (
                        step_id,
                        ImpureTimestamp {
                            epoch_seconds: timestamp.epoch_seconds,
                            subsec_nanos: timestamp.subsec_nanos,
                        },
                    )
                })
                .collect();
            (workflow_id, mapped_steps)
        })
        .collect()
}

/// Converts runtime impure timestamps into persisted latest-schema shape.
fn impure_timestamps_to_latest(
    runtime_map: std::collections::BTreeMap<
        String,
        std::collections::BTreeMap<String, ImpureTimestamp>,
    >,
) -> std::collections::BTreeMap<
    String,
    std::collections::BTreeMap<String, v_latest::ImpureTimestampLatest>,
> {
    runtime_map
        .into_iter()
        .map(|(workflow_id, steps)| {
            let mapped_steps = steps
                .into_iter()
                .map(|(step_id, timestamp)| {
                    (
                        step_id,
                        v_latest::ImpureTimestampLatest {
                            epoch_seconds: timestamp.epoch_seconds,
                            subsec_nanos: timestamp.subsec_nanos,
                        },
                    )
                })
                .collect();
            (workflow_id, mapped_steps)
        })
        .collect()
}

/// Returns whether one builtin identity should be treated as impure by runtime
/// planning and deterministic cache invalidation.
fn builtin_is_impure(name: &str, version: &str) -> bool {
    matches!(
        (name, version),
        (
            mediapm_conductor_builtin_import::TOOL_NAME,
            mediapm_conductor_builtin_import::TOOL_VERSION
        ) | (mediapm_conductor_builtin_fs::TOOL_NAME, mediapm_conductor_builtin_fs::TOOL_VERSION)
            | (
                mediapm_conductor_builtin_export::TOOL_NAME,
                mediapm_conductor_builtin_export::TOOL_VERSION
            )
    )
}

/// Builds one runtime builtin tool spec from builtin identity only.
///
/// Persisted builtin definitions are intentionally minimal (`kind`, `name`,
/// `version`), so runtime-only defaults are derived here.
fn runtime_builtin_tool_spec(name: String, version: String) -> ToolSpec {
    ToolSpec {
        is_impure: builtin_is_impure(&name, &version),
        kind: ToolKindSpec::Builtin { name, version },
        ..ToolSpec::default()
    }
}

/// Evaluates fixed Nickel migrations/contracts plus user and machine
/// configuration and returns the normalized compiled payload.
pub(crate) fn compile_total_configuration_sources(
    user_source: &str,
    machine_source: &str,
    state_source: &str,
) -> Result<Value, ConductorError> {
    validate_state_document_source_shape(state_source)?;

    let target_version = latest_version_among_sources(user_source, machine_source, state_source)?;
    let (target_file_name, target_contract_source) =
        resolve_version_contract(target_version, "Nickel configuration")?;
    let validator_name = format!("validate_document_v{target_version}");

    let workspace = TempNickelWorkspace::new()?;
    write_nickel_file(
        &workspace.path().join("mod.ncl"),
        MOD_NCL_SOURCE,
        "writing temporary Nickel migration helper",
    )?;
    write_nickel_file(
        &workspace.path().join(target_file_name),
        target_contract_source,
        &format!("writing temporary Nickel {target_file_name} helper"),
    )?;
    write_nickel_file(
        &workspace.path().join("user_input.ncl"),
        user_source,
        "writing temporary user Nickel input",
    )?;
    write_nickel_file(
        &workspace.path().join("machine_input.ncl"),
        machine_source,
        "writing temporary machine Nickel input",
    )?;
    write_nickel_file(
        &workspace.path().join("state_input.ncl"),
        state_source,
        "writing temporary state Nickel input",
    )?;

    let validate_source = format!(
        r#"
let migration = import "mod.ncl" in
let version = import "{target_file_name}" in
let user = version.{validator_name} (migration.migrate_to {target_version} (import "user_input.ncl")) in
let machine = version.{validator_name} (migration.migrate_to {target_version} (import "machine_input.ncl")) in
let state = version.{validator_name} (migration.migrate_to {target_version} (import "state_input.ncl")) in
{{
    validated_user = user,
    validated_machine = machine,
    validated_state = state,
    total = {{ include [user, machine, state] }},
}}
"#,
    );
    let validate_path = workspace.path().join("validate_total.ncl");
    write_nickel_file(
        &validate_path,
        &validate_source,
        "writing temporary total Nickel validation wrapper",
    )?;

    evaluate_main_file_as(&validate_path, "evaluating full Nickel configuration")
}

/// Evaluates fixed Nickel migrations/contracts plus user and machine
/// configuration together for validation side effects.
pub(crate) fn evaluate_total_configuration_sources(
    user_source: &str,
    machine_source: &str,
    state_source: &str,
) -> Result<(), ConductorError> {
    let _ = compile_total_configuration_sources(user_source, machine_source, state_source)?;
    Ok(())
}

/// Optic bridge from latest persisted Nickel state to runtime user document.
#[expect(
    clippy::too_many_lines,
    reason = "this item intentionally keeps end-to-end control flow together so ordering invariants remain explicit during maintenance"
)]
fn user_runtime_iso() -> IsoPrime<'static, RcBrand, latest::State, UserNickelDocument> {
    IsoPrime::new(
        |state: latest::State| UserNickelDocument {
            metadata: NickelDocumentMetadata::default(),
            runtime: crate::model::config::RuntimeStorageConfig {
                conductor_dir: state.runtime.conductor_dir,
                conductor_state_config: state.runtime.conductor_state_config,
                cas_store_dir: state.runtime.cas_store_dir,
                conductor_tmp_dir: state.runtime.conductor_tmp_dir,
                conductor_schema_dir: state.runtime.conductor_schema_dir,
                inherited_env_vars: state.runtime.inherited_env_vars,
                use_user_tool_cache: state.runtime.use_user_tool_cache,
            },
            external_data: state
                .external_data
                .into_iter()
                .map(|(hash, reference)| {
                    (
                        hash,
                        ExternalContentRef {
                            description: reference.description,
                            save: reference.save.map(|save| match save {
                                v_latest::OutputSaveLatest::Bool(false) => OutputSaveMode::Unsaved,
                                v_latest::OutputSaveLatest::Bool(true) => OutputSaveMode::Saved,
                                v_latest::OutputSaveLatest::Full => OutputSaveMode::Full,
                            }),
                        },
                    )
                })
                .collect(),
            tools: state
                .tools
                .into_iter()
                .map(|(tool_name, tool)| match tool {
                    v_latest::ToolSpecLatest::Executable {
                        is_impure,
                        inputs,
                        command,
                        env_vars,
                        success_codes,
                        outputs,
                    } => (
                        tool_name,
                        ToolSpec {
                            is_impure,
                            inputs: inputs
                                .into_iter()
                                .map(|(input_name, input_spec)| {
                                    (
                                        input_name,
                                        ToolInputSpec {
                                            kind: match input_spec.kind {
                                                v_latest::ToolInputKindLatest::String => {
                                                    ToolInputKind::String
                                                }
                                                v_latest::ToolInputKindLatest::StringList => {
                                                    ToolInputKind::StringList
                                                }
                                            },
                                        },
                                    )
                                })
                                .collect(),
                            kind: ToolKindSpec::Executable { command, env_vars, success_codes },
                            outputs: outputs
                                .into_iter()
                                .map(|(output_name, output_spec)| {
                                    (
                                        output_name,
                                        ToolOutputSpec {
                                            capture: match output_spec.capture {
                                                v_latest::OutputCaptureLatest::Stdout {} => {
                                                    OutputCaptureSpec::Stdout {}
                                                }
                                                v_latest::OutputCaptureLatest::Stderr {} => {
                                                    OutputCaptureSpec::Stderr {}
                                                }
                                                v_latest::OutputCaptureLatest::ProcessCode {} => {
                                                    OutputCaptureSpec::ProcessCode {}
                                                }
                                                v_latest::OutputCaptureLatest::File { path } => {
                                                    OutputCaptureSpec::File { path }
                                                }
                                                v_latest::OutputCaptureLatest::FileRegex {
                                                    path_regex,
                                                } => OutputCaptureSpec::FileRegex { path_regex },
                                                v_latest::OutputCaptureLatest::Folder {
                                                    path,
                                                    include_topmost_folder,
                                                } => OutputCaptureSpec::Folder {
                                                    path,
                                                    include_topmost_folder,
                                                },
                                                v_latest::OutputCaptureLatest::FolderRegex {
                                                    path_regex,
                                                } => OutputCaptureSpec::FolderRegex { path_regex },
                                            },
                                            allow_empty: output_spec.allow_empty,
                                        },
                                    )
                                })
                                .collect(),
                        },
                    ),
                    v_latest::ToolSpecLatest::Builtin { name, version } => {
                        (tool_name, runtime_builtin_tool_spec(name, version))
                    }
                })
                .collect(),
            workflows: state
                .workflows
                .into_iter()
                .map(|(name, workflow)| {
                    (
                        name,
                        WorkflowSpec {
                            name: workflow.name,
                            description: workflow.description,
                            steps: workflow
                                .steps
                                .into_iter()
                                .map(|step| WorkflowStepSpec {
                                    id: step.id,
                                    tool: step.tool,
                                    inputs: step
                                        .inputs
                                        .into_iter()
                                        .map(|(input_name, binding)| {
                                            (
                                                input_name,
                                                match binding {
                                                    v_latest::InputBindingLatest::String(value) => {
                                                        InputBinding::String(value)
                                                    }
                                                    v_latest::InputBindingLatest::StringList(
                                                        values,
                                                    ) => InputBinding::StringList(values),
                                                },
                                            )
                                        })
                                        .collect(),
                                    depends_on: step.depends_on,
                                    outputs: step
                                        .outputs
                                        .into_iter()
                                        .map(|(output_name, policy)| {
                                            (
                                                output_name,
                                                OutputPolicy {
                                                    save: policy.save.map(|save| match save {
                                                        v_latest::OutputSaveLatest::Bool(false) => {
                                                            OutputSaveMode::Unsaved
                                                        }
                                                        v_latest::OutputSaveLatest::Bool(true) => {
                                                            OutputSaveMode::Saved
                                                        }
                                                        v_latest::OutputSaveLatest::Full => {
                                                            OutputSaveMode::Full
                                                        }
                                                    }),
                                                },
                                            )
                                        })
                                        .collect(),
                                })
                                .collect(),
                        },
                    )
                })
                .collect(),
            tool_configs: state
                .tool_configs
                .into_iter()
                .map(|(tool_name, config)| {
                    (
                        tool_name,
                        ToolConfigSpec {
                            max_concurrent_calls: config.max_concurrent_calls,
                            max_retries: config.max_retries,
                            description: config.description,
                            input_defaults: config
                                .input_defaults
                                .into_iter()
                                .map(|(input_name, binding)| {
                                    (
                                        input_name,
                                        match binding {
                                            v_latest::InputBindingLatest::String(value) => {
                                                InputBinding::String(value)
                                            }
                                            v_latest::InputBindingLatest::StringList(values) => {
                                                InputBinding::StringList(values)
                                            }
                                        },
                                    )
                                })
                                .collect(),
                            env_vars: config.env_vars,
                            content_map: config.content_map,
                        },
                    )
                })
                .collect(),
            impure_timestamps: impure_timestamps_from_latest(state.impure_timestamps),
            state_pointer: state.state_pointer,
        },
        |runtime: UserNickelDocument| latest::State {
            runtime: v_latest::RuntimeStorageLatest {
                conductor_dir: runtime.runtime.conductor_dir,
                conductor_state_config: runtime.runtime.conductor_state_config,
                cas_store_dir: runtime.runtime.cas_store_dir,
                conductor_tmp_dir: runtime.runtime.conductor_tmp_dir,
                conductor_schema_dir: runtime.runtime.conductor_schema_dir,
                inherited_env_vars: runtime.runtime.inherited_env_vars,
                use_user_tool_cache: runtime.runtime.use_user_tool_cache,
            },
            external_data: runtime
                .external_data
                .into_iter()
                .map(|(hash, reference)| {
                    (
                        hash,
                        v_latest::ExternalContentRefLatest {
                            description: reference.description,
                            save: reference.save.map(|save| match save {
                                OutputSaveMode::Unsaved => v_latest::OutputSaveLatest::Bool(false),
                                OutputSaveMode::Saved => v_latest::OutputSaveLatest::Bool(true),
                                OutputSaveMode::Full => v_latest::OutputSaveLatest::Full,
                            }),
                        },
                    )
                })
                .collect(),
            tools: runtime
                .tools
                .into_iter()
                .map(|(tool_name, tool)| match tool.kind {
                    ToolKindSpec::Executable { command, env_vars, success_codes } => (
                        tool_name,
                        v_latest::ToolSpecLatest::Executable {
                            is_impure: tool.is_impure,
                            inputs: tool
                                .inputs
                                .into_iter()
                                .map(|(input_name, input_spec)| {
                                    (
                                        input_name,
                                        v_latest::ToolInputSpecLatest {
                                            kind: match input_spec.kind {
                                                ToolInputKind::String => {
                                                    v_latest::ToolInputKindLatest::String
                                                }
                                                ToolInputKind::StringList => {
                                                    v_latest::ToolInputKindLatest::StringList
                                                }
                                            },
                                        },
                                    )
                                })
                                .collect(),
                            command,
                            env_vars,
                            success_codes,
                            outputs: tool
                                .outputs
                                .into_iter()
                                .map(|(output_name, output_spec)| {
                                    (
                                        output_name,
                                        v_latest::ToolOutputSpecLatest {
                                            capture: match output_spec.capture {
                                                OutputCaptureSpec::Stdout {} => {
                                                    v_latest::OutputCaptureLatest::Stdout {}
                                                }
                                                OutputCaptureSpec::Stderr {} => {
                                                    v_latest::OutputCaptureLatest::Stderr {}
                                                }
                                                OutputCaptureSpec::ProcessCode {} => {
                                                    v_latest::OutputCaptureLatest::ProcessCode {}
                                                }
                                                OutputCaptureSpec::File { path } => {
                                                    v_latest::OutputCaptureLatest::File { path }
                                                }
                                                OutputCaptureSpec::FileRegex { path_regex } => {
                                                    v_latest::OutputCaptureLatest::FileRegex {
                                                        path_regex,
                                                    }
                                                }
                                                OutputCaptureSpec::Folder {
                                                    path,
                                                    include_topmost_folder,
                                                } => v_latest::OutputCaptureLatest::Folder {
                                                    path,
                                                    include_topmost_folder,
                                                },
                                                OutputCaptureSpec::FolderRegex { path_regex } => {
                                                    v_latest::OutputCaptureLatest::FolderRegex {
                                                        path_regex,
                                                    }
                                                }
                                            },
                                            allow_empty: output_spec.allow_empty,
                                        },
                                    )
                                })
                                .collect(),
                        },
                    ),
                    ToolKindSpec::Builtin { name, version } => {
                        (tool_name, v_latest::ToolSpecLatest::Builtin { name, version })
                    }
                })
                .collect(),
            workflows: runtime
                .workflows
                .into_iter()
                .map(|(name, workflow)| {
                    (
                        name,
                        v_latest::WorkflowSpecLatest {
                            name: workflow.name,
                            description: workflow.description,
                            steps: workflow
                                .steps
                                .into_iter()
                                .map(|step| v_latest::WorkflowStepSpecLatest {
                                    id: step.id,
                                    tool: step.tool,
                                    inputs: step
                                        .inputs
                                        .into_iter()
                                        .map(|(input_name, binding)| {
                                            (
                                                input_name,
                                                match binding {
                                                    InputBinding::String(value) => {
                                                        v_latest::InputBindingLatest::String(value)
                                                    }
                                                    InputBinding::StringList(values) => {
                                                        v_latest::InputBindingLatest::StringList(
                                                            values,
                                                        )
                                                    }
                                                },
                                            )
                                        })
                                        .collect(),
                                    depends_on: step.depends_on,
                                    outputs: step
                                        .outputs
                                        .into_iter()
                                        .map(|(output_name, policy)| {
                                            (
                                                output_name,
                                                v_latest::OutputPolicyLatest {
                                                    save: policy.save.map(|save| match save {
                                                        OutputSaveMode::Unsaved => {
                                                            v_latest::OutputSaveLatest::Bool(false)
                                                        }
                                                        OutputSaveMode::Saved => {
                                                            v_latest::OutputSaveLatest::Bool(true)
                                                        }
                                                        OutputSaveMode::Full => {
                                                            v_latest::OutputSaveLatest::Full
                                                        }
                                                    }),
                                                },
                                            )
                                        })
                                        .collect(),
                                })
                                .collect(),
                        },
                    )
                })
                .collect(),
            tool_configs: runtime
                .tool_configs
                .into_iter()
                .map(|(tool_name, config)| {
                    (
                        tool_name,
                        v_latest::ToolConfigSpecLatest {
                            max_concurrent_calls: config.max_concurrent_calls,
                            max_retries: config.max_retries,
                            description: config.description,
                            input_defaults: config
                                .input_defaults
                                .into_iter()
                                .map(|(input_name, binding)| {
                                    (
                                        input_name,
                                        match binding {
                                            InputBinding::String(value) => {
                                                v_latest::InputBindingLatest::String(value)
                                            }
                                            InputBinding::StringList(values) => {
                                                v_latest::InputBindingLatest::StringList(values)
                                            }
                                        },
                                    )
                                })
                                .collect(),
                            env_vars: config.env_vars,
                            content_map: config.content_map,
                        },
                    )
                })
                .collect(),
            impure_timestamps: impure_timestamps_to_latest(runtime.impure_timestamps),
            state_pointer: runtime.state_pointer,
        },
    )
}

/// Optic bridge from latest persisted Nickel state to runtime machine document.
fn machine_runtime_iso() -> IsoPrime<'static, RcBrand, latest::State, MachineNickelDocument> {
    IsoPrime::new(
        |state: latest::State| {
            let runtime = user_runtime_iso().from(state);
            MachineNickelDocument {
                metadata: NickelDocumentMetadata::default(),
                runtime: runtime.runtime,
                external_data: runtime.external_data,
                tools: runtime.tools,
                workflows: runtime.workflows,
                tool_configs: runtime.tool_configs,
                impure_timestamps: runtime.impure_timestamps,
                state_pointer: runtime.state_pointer,
            }
        },
        |runtime: MachineNickelDocument| {
            user_runtime_iso().to(UserNickelDocument {
                metadata: NickelDocumentMetadata::default(),
                runtime: runtime.runtime,
                external_data: runtime.external_data,
                tools: runtime.tools,
                workflows: runtime.workflows,
                tool_configs: runtime.tool_configs,
                impure_timestamps: runtime.impure_timestamps,
                state_pointer: runtime.state_pointer,
            })
        },
    )
}

/// Optic bridge from latest persisted Nickel state to runtime volatile state
/// document.
fn state_runtime_iso() -> IsoPrime<'static, RcBrand, latest::State, StateNickelDocument> {
    IsoPrime::new(
        |state: latest::State| StateNickelDocument {
            impure_timestamps: impure_timestamps_from_latest(state.impure_timestamps),
            state_pointer: state.state_pointer,
        },
        |runtime: StateNickelDocument| latest::State {
            runtime: v_latest::RuntimeStorageLatest::default(),
            external_data: std::collections::BTreeMap::new(),
            tools: std::collections::BTreeMap::new(),
            workflows: std::collections::BTreeMap::new(),
            tool_configs: std::collections::BTreeMap::new(),
            impure_timestamps: impure_timestamps_to_latest(runtime.impure_timestamps),
            state_pointer: runtime.state_pointer,
        },
    )
}

/// Encodes `conductor.ncl` with the latest envelope.
pub(crate) fn encode_user_document(
    document: UserNickelDocument,
) -> Result<Vec<u8>, ConductorError> {
    let latest_state = user_runtime_iso().to(document);
    let envelope = latest::version_iso().to(latest_state);

    vet_latest_envelope(&envelope, "conductor.ncl")?;
    render_document_as_nickel(&envelope, "conductor.ncl")
}

/// Decodes `conductor.ncl` through the embedded migration wrapper.
pub(crate) fn decode_user_document(bytes: &[u8]) -> Result<UserNickelDocument, ConductorError> {
    let source = std::str::from_utf8(bytes).map_err(|err| {
        ConductorError::Serialization(format!("conductor.ncl is not valid UTF-8: {err}"))
    })?;
    let envelope: latest::Envelope = evaluate_document_source(source, "conductor.ncl")?;
    let marker = envelope.version;
    if marker != USER_NICKEL_VERSION {
        return Err(ConductorError::Workflow(format!(
            "unsupported conductor.ncl schema version {marker}; expected {USER_NICKEL_VERSION}"
        )));
    }

    vet_latest_envelope(&envelope, "conductor.ncl")?;

    let mut runtime = user_runtime_iso().from(latest::version_iso().from(envelope));
    runtime.metadata = NickelDocumentMetadata::default();
    Ok(runtime)
}

/// Encodes `conductor.machine.ncl` with the latest envelope.
pub(crate) fn encode_machine_document(
    document: MachineNickelDocument,
) -> Result<Vec<u8>, ConductorError> {
    let latest_state = machine_runtime_iso().to(document);
    let envelope = latest::version_iso().to(latest_state);

    vet_latest_envelope(&envelope, "conductor.machine.ncl")?;
    render_document_as_nickel(&envelope, "conductor.machine.ncl")
}

/// Decodes `conductor.machine.ncl` through the embedded migration wrapper.
pub(crate) fn decode_machine_document(
    bytes: &[u8],
) -> Result<MachineNickelDocument, ConductorError> {
    let source = std::str::from_utf8(bytes).map_err(|err| {
        ConductorError::Serialization(format!("conductor.machine.ncl is not valid UTF-8: {err}"))
    })?;
    let envelope: latest::Envelope = evaluate_document_source(source, "conductor.machine.ncl")?;
    let marker = envelope.version;
    if marker != MACHINE_NICKEL_VERSION {
        return Err(ConductorError::Workflow(format!(
            "unsupported conductor.machine.ncl schema version {marker}; expected {MACHINE_NICKEL_VERSION}"
        )));
    }

    vet_latest_envelope(&envelope, "conductor.machine.ncl")?;

    let mut runtime = machine_runtime_iso().from(latest::version_iso().from(envelope));
    runtime.metadata = NickelDocumentMetadata::default();
    Ok(runtime)
}

/// Encodes `.conductor/state.ncl` with the latest envelope.
pub(crate) fn encode_state_document(
    document: StateNickelDocument,
) -> Result<Vec<u8>, ConductorError> {
    /// Minimal persisted envelope emitted for `.conductor/state.ncl`.
    #[derive(Debug, Serialize)]
    struct StateEnvelope {
        /// Explicit schema marker shared with user/machine documents.
        version: u32,
        /// Impure timestamps map (`workflow_id -> step_id -> timestamp`).
        impure_timestamps: std::collections::BTreeMap<
            String,
            std::collections::BTreeMap<String, v_latest::ImpureTimestampLatest>,
        >,
        /// Optional orchestration-state pointer.
        state_pointer: Option<mediapm_cas::Hash>,
    }

    let latest_state = state_runtime_iso().to(document);
    let envelope = latest::version_iso().to(latest_state);

    vet_latest_envelope(&envelope, ".conductor/state.ncl")?;
    render_document_as_nickel(
        &StateEnvelope {
            version: envelope.version,
            impure_timestamps: envelope.impure_timestamps,
            state_pointer: envelope.state_pointer,
        },
        ".conductor/state.ncl",
    )
}

/// Decodes `.conductor/state.ncl` through the embedded migration wrapper.
pub(crate) fn decode_state_document(bytes: &[u8]) -> Result<StateNickelDocument, ConductorError> {
    let source = std::str::from_utf8(bytes).map_err(|err| {
        ConductorError::Serialization(format!(".conductor/state.ncl is not valid UTF-8: {err}"))
    })?;

    validate_state_document_source_shape(source)?;
    let _ = read_document_version_marker(source, ".conductor/state.ncl")?;

    let envelope: latest::Envelope = evaluate_document_source(source, ".conductor/state.ncl")?;
    let marker = envelope.version;
    if marker != MACHINE_NICKEL_VERSION {
        return Err(ConductorError::Workflow(format!(
            "unsupported .conductor/state.ncl schema version {marker}; expected {MACHINE_NICKEL_VERSION}"
        )));
    }

    vet_latest_envelope(&envelope, ".conductor/state.ncl")?;

    Ok(state_runtime_iso().from(latest::version_iso().from(envelope)))
}

/// Performs structural invariant checks on one latest persisted Nickel envelope.
#[expect(
    clippy::too_many_lines,
    reason = "this item intentionally keeps end-to-end control flow together so ordering invariants remain explicit during maintenance"
)]
fn vet_latest_envelope(
    envelope: &latest::Envelope,
    document_kind: &str,
) -> Result<(), ConductorError> {
    if envelope.version != latest::VERSION {
        return Err(ConductorError::Workflow(format!(
            "expected {document_kind} version {} but found {}",
            latest::VERSION,
            envelope.version
        )));
    }

    if let Some(conductor_dir) = &envelope.runtime.conductor_dir
        && conductor_dir.trim().is_empty()
    {
        return Err(ConductorError::Workflow(format!(
            "{document_kind} conductor_dir must be non-empty"
        )));
    }
    if let Some(conductor_state_config) = &envelope.runtime.conductor_state_config
        && conductor_state_config.trim().is_empty()
    {
        return Err(ConductorError::Workflow(format!(
            "{document_kind} conductor_state_config must be non-empty when provided"
        )));
    }
    if let Some(cas_store_dir) = &envelope.runtime.cas_store_dir
        && cas_store_dir.trim().is_empty()
    {
        return Err(ConductorError::Workflow(format!(
            "{document_kind} cas_store_dir must be non-empty when provided"
        )));
    }
    if let Some(conductor_tmp_dir) = &envelope.runtime.conductor_tmp_dir
        && conductor_tmp_dir.trim().is_empty()
    {
        return Err(ConductorError::Workflow(format!(
            "{document_kind} conductor_tmp_dir must be non-empty when provided"
        )));
    }
    if let Some(conductor_schema_dir) = &envelope.runtime.conductor_schema_dir
        && conductor_schema_dir.trim().is_empty()
    {
        return Err(ConductorError::Workflow(format!(
            "{document_kind} conductor_schema_dir must be non-empty when provided"
        )));
    }

    for (tool_name, tool) in &envelope.tools {
        if !tool_name.contains('@') {
            return Err(ConductorError::Workflow(format!(
                "{document_kind} tool '{tool_name}' must include immutable version in its name (for example: compose@1.0.0)"
            )));
        }

        match tool {
            v_latest::ToolSpecLatest::Executable { command, success_codes, outputs, .. } => {
                let Some(executable) = command.first() else {
                    return Err(ConductorError::Workflow(format!(
                        "{document_kind} tool '{tool_name}' executable command must contain at least one entry"
                    )));
                };
                if executable.trim().is_empty() {
                    return Err(ConductorError::Workflow(format!(
                        "{document_kind} tool '{tool_name}' executable command[0] must be non-empty"
                    )));
                }
                if success_codes.is_empty() {
                    return Err(ConductorError::Workflow(format!(
                        "{document_kind} tool '{tool_name}' executable process.success_codes must contain at least one exit code"
                    )));
                }
                if outputs.is_empty() {
                    return Err(ConductorError::Workflow(format!(
                        "{document_kind} tool '{tool_name}' must declare at least one output capture"
                    )));
                }
            }
            v_latest::ToolSpecLatest::Builtin { name, version } => {
                if name.trim().is_empty() || version.trim().is_empty() {
                    return Err(ConductorError::Workflow(format!(
                        "{document_kind} tool '{tool_name}' builtin process must provide non-empty name and version"
                    )));
                }
            }
        }
    }

    let external_hashes = envelope.external_data.keys().copied().collect::<BTreeSet<_>>();

    for (hash, reference) in &envelope.external_data {
        if matches!(reference.save, Some(v_latest::OutputSaveLatest::Bool(false))) {
            return Err(ConductorError::Workflow(format!(
                "{document_kind} external_data '{hash}' save policy cannot be false; use true or \"full\""
            )));
        }
    }

    for (tool_name, tool_config) in &envelope.tool_configs {
        if tool_config.max_concurrent_calls == 0 || tool_config.max_concurrent_calls < -1 {
            return Err(ConductorError::Workflow(format!(
                "{document_kind} tool_configs '{tool_name}' max_concurrent_calls must be -1 or a positive integer"
            )));
        }
        if tool_config.max_retries < -1 {
            return Err(ConductorError::Workflow(format!(
                "{document_kind} tool_configs '{tool_name}' max_retries must be -1 or a non-negative integer"
            )));
        }
        if let Some(description) = &tool_config.description
            && description.trim().is_empty()
        {
            return Err(ConductorError::Workflow(format!(
                "{document_kind} tool_configs '{tool_name}' description must be non-empty when provided"
            )));
        }

        if let Some(tool) = envelope.tools.get(tool_name)
            && tool_config.content_map.is_some()
            && matches!(tool, v_latest::ToolSpecLatest::Builtin { .. })
        {
            return Err(ConductorError::Workflow(format!(
                "{document_kind} tool_configs '{tool_name}' content_map is invalid for builtin tools"
            )));
        }

        if let Some(tool) = envelope.tools.get(tool_name) {
            match tool {
                v_latest::ToolSpecLatest::Builtin { .. } => {
                    if !tool_config.input_defaults.is_empty() {
                        return Err(ConductorError::Workflow(format!(
                            "{document_kind} tool_configs '{tool_name}' input_defaults is invalid for builtin tools"
                        )));
                    }
                }
                v_latest::ToolSpecLatest::Executable { inputs, .. } => {
                    if let Some(content_map) = &tool_config.content_map {
                        for (relative_path, hash) in content_map {
                            if !external_hashes.contains(hash) {
                                return Err(ConductorError::Workflow(format!(
                                    "{document_kind} tool_configs '{tool_name}' content_map '{relative_path}' references hash '{hash}' that is missing from external_data"
                                )));
                            }
                        }
                    }

                    for (input_name, binding) in &tool_config.input_defaults {
                        let Some(input_spec) = inputs.get(input_name) else {
                            return Err(ConductorError::Workflow(format!(
                                "{document_kind} tool_configs '{tool_name}' input_defaults references undeclared tool input '{input_name}'"
                            )));
                        };
                        match (&input_spec.kind, binding) {
                            (
                                v_latest::ToolInputKindLatest::String,
                                v_latest::InputBindingLatest::String(_),
                            )
                            | (
                                v_latest::ToolInputKindLatest::StringList,
                                v_latest::InputBindingLatest::StringList(_),
                            ) => {}
                            (
                                v_latest::ToolInputKindLatest::String,
                                v_latest::InputBindingLatest::StringList(_),
                            ) => {
                                return Err(ConductorError::Workflow(format!(
                                    "{document_kind} tool_configs '{tool_name}' input_defaults['{input_name}'] expects kind 'string' but received 'string_list'"
                                )));
                            }
                            (
                                v_latest::ToolInputKindLatest::StringList,
                                v_latest::InputBindingLatest::String(_),
                            ) => {
                                return Err(ConductorError::Workflow(format!(
                                    "{document_kind} tool_configs '{tool_name}' input_defaults['{input_name}'] expects kind 'string_list' but received 'string'"
                                )));
                            }
                        }
                    }
                }
            }
        }
    }

    for (workflow_id, steps) in &envelope.impure_timestamps {
        for (step_id, timestamp) in steps {
            if timestamp.subsec_nanos >= 1_000_000_000 {
                return Err(ConductorError::Workflow(format!(
                    "{document_kind} impure_timestamps.{workflow_id}.{step_id}.subsec_nanos must be in range 0..999999999"
                )));
            }
        }
    }

    for (workflow_name, workflow) in &envelope.workflows {
        let step_tool_by_id = workflow
            .steps
            .iter()
            .map(|step| (step.id.as_str(), step.tool.as_str()))
            .collect::<std::collections::BTreeMap<_, _>>();

        for step in &workflow.steps {
            let mut explicit_dependencies = BTreeSet::new();
            for dependency_step_id in &step.depends_on {
                if !explicit_dependencies.insert(dependency_step_id.clone()) {
                    return Err(ConductorError::Workflow(format!(
                        "{document_kind} workflow '{workflow_name}' step '{}' contains duplicate depends_on entry '{dependency_step_id}'",
                        step.id
                    )));
                }
                if dependency_step_id == &step.id {
                    return Err(ConductorError::Workflow(format!(
                        "{document_kind} workflow '{workflow_name}' step '{}' must not depend on itself",
                        step.id
                    )));
                }
                if !step_tool_by_id.contains_key(dependency_step_id.as_str()) {
                    return Err(ConductorError::Workflow(format!(
                        "{document_kind} workflow '{workflow_name}' step '{}' depends_on unknown step '{dependency_step_id}'",
                        step.id
                    )));
                }
            }

            if let Some(v_latest::ToolSpecLatest::Executable { inputs, .. }) =
                envelope.tools.get(&step.tool)
            {
                for input_name in step.inputs.keys() {
                    if !inputs.contains_key(input_name) {
                        return Err(ConductorError::Workflow(format!(
                            "{document_kind} workflow '{workflow_name}' step '{}' references undeclared input '{input_name}' for tool '{}'",
                            step.id, step.tool,
                        )));
                    }
                }

                for (input_name, input_spec) in inputs {
                    if let Some(binding) = step.inputs.get(input_name) {
                        match (&input_spec.kind, binding) {
                            (
                                v_latest::ToolInputKindLatest::String,
                                v_latest::InputBindingLatest::String(_),
                            )
                            | (
                                v_latest::ToolInputKindLatest::StringList,
                                v_latest::InputBindingLatest::StringList(_),
                            ) => {}
                            (
                                v_latest::ToolInputKindLatest::String,
                                v_latest::InputBindingLatest::StringList(_),
                            ) => {
                                return Err(ConductorError::Workflow(format!(
                                    "{document_kind} workflow '{workflow_name}' step '{}' input '{input_name}' expects kind 'string' for tool '{}', but received 'string_list'",
                                    step.id, step.tool,
                                )));
                            }
                            (
                                v_latest::ToolInputKindLatest::StringList,
                                v_latest::InputBindingLatest::String(_),
                            ) => {
                                return Err(ConductorError::Workflow(format!(
                                    "{document_kind} workflow '{workflow_name}' step '{}' input '{input_name}' expects kind 'string_list' for tool '{}', but received 'string'",
                                    step.id, step.tool,
                                )));
                            }
                        }
                    } else {
                        let tool_config_default = envelope
                            .tool_configs
                            .get(&step.tool)
                            .and_then(|tool_config| tool_config.input_defaults.get(input_name));

                        if tool_config_default.is_none() {
                            return Err(ConductorError::Workflow(format!(
                                "{document_kind} workflow '{workflow_name}' step '{}' is missing required input '{input_name}' for tool '{}'",
                                step.id, step.tool,
                            )));
                        }

                        if let Some(default_binding) = tool_config_default {
                            match (&input_spec.kind, default_binding) {
                                (
                                    v_latest::ToolInputKindLatest::String,
                                    v_latest::InputBindingLatest::String(_),
                                )
                                | (
                                    v_latest::ToolInputKindLatest::StringList,
                                    v_latest::InputBindingLatest::StringList(_),
                                ) => {}
                                (
                                    v_latest::ToolInputKindLatest::String,
                                    v_latest::InputBindingLatest::StringList(_),
                                ) => {
                                    return Err(ConductorError::Workflow(format!(
                                        "{document_kind} workflow '{workflow_name}' step '{}' uses tool_config input default '{input_name}' with kind 'string_list', but tool '{}' expects kind 'string'",
                                        step.id, step.tool,
                                    )));
                                }
                                (
                                    v_latest::ToolInputKindLatest::StringList,
                                    v_latest::InputBindingLatest::String(_),
                                ) => {
                                    return Err(ConductorError::Workflow(format!(
                                        "{document_kind} workflow '{workflow_name}' step '{}' uses tool_config input default '{input_name}' with kind 'string', but tool '{}' expects kind 'string_list'",
                                        step.id, step.tool,
                                    )));
                                }
                            }
                        }
                    }
                }
            }

            for (input_name, binding) in &step.inputs {
                let binding_items: Vec<(usize, &str)> = match binding {
                    v_latest::InputBindingLatest::String(value) => vec![(0, value.as_str())],
                    v_latest::InputBindingLatest::StringList(values) => {
                        values.iter().enumerate().map(|(idx, item)| (idx, item.as_str())).collect()
                    }
                };

                for (item_index, binding_item) in binding_items {
                    let parsed_segments = parse_input_binding(binding_item).map_err(|err| {
                        ConductorError::Workflow(format!(
                            "{document_kind} workflow '{workflow_name}' step '{}' input '{input_name}' has invalid {}binding '{}': {err}",
                            step.id,
                            if matches!(binding, v_latest::InputBindingLatest::StringList(_)) {
                                format!("list item {item_index} ")
                            } else {
                                String::new()
                            },
                            binding_item,
                        ))
                    })?;

                    for segment in parsed_segments {
                        if let ParsedInputBindingSegment::StepOutput { step_id, output, .. } =
                            segment
                        {
                            if !explicit_dependencies.contains(step_id) {
                                return Err(ConductorError::Workflow(format!(
                                    "{document_kind} workflow '{workflow_name}' step '{}' input '{input_name}' references '${{step_output.{step_id}.{output}}}' but step '{step_id}' is missing from depends_on",
                                    step.id
                                )));
                            }

                            let Some(producer_tool_name) = step_tool_by_id.get(step_id) else {
                                return Err(ConductorError::Workflow(format!(
                                    "{document_kind} workflow '{workflow_name}' step '{}' input '{input_name}' references unknown dependency step '{step_id}'",
                                    step.id
                                )));
                            };

                            let Some(producer_tool) = envelope.tools.get(*producer_tool_name)
                            else {
                                continue;
                            };

                            let producer_outputs = match producer_tool {
                                v_latest::ToolSpecLatest::Executable { outputs, .. } => outputs,
                                v_latest::ToolSpecLatest::Builtin { .. } => continue,
                            };

                            if !producer_outputs.contains_key(output) {
                                return Err(ConductorError::Workflow(format!(
                                    "{document_kind} workflow '{workflow_name}' step '{}' input '{input_name}' references missing output '{output}' on dependency step '{step_id}'",
                                    step.id
                                )));
                            }
                        }
                    }
                }
            }
        }
    }

    Ok(())
}
