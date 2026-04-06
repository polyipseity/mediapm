//! Actor-backed loading and persistence for conductor Nickel documents.
//!
//! This actor owns the user/machine document merge contract so the coordinator
//! can sequence workflows without also carrying parsing and merge logic inline.

use std::collections::{BTreeMap, BTreeSet};
use std::path::{Path, PathBuf};

use ractor::{Actor, ActorProcessingErr, ActorRef, RpcReplyPort, call_t};

use crate::api::RunWorkflowOptions;
use crate::error::ConductorError;
use crate::model::config::{
    InputBinding, MachineNickelDocument, NickelDocumentMetadata, OutputCaptureSpec, ProcessSpec,
    RuntimeStorageConfig, StateNickelDocument, ToolConfigSpec, ToolKindSpec, ToolOutputSpec,
    ToolSpec, UserNickelDocument, WorkflowSpec, WorkflowStepSpec, decode_machine_document,
    decode_state_document, decode_user_document, encode_machine_document, encode_state_document,
    encode_user_document, evaluate_total_configuration_sources,
};
use crate::orchestration::config::DEFAULT_RPC_TIMEOUT_MS;
use crate::orchestration::protocol::{LoadedDocuments, UnifiedNickelDocument, UnifiedToolSpec};

/// Typed client for the document-loader actor.
#[derive(Debug, Clone)]
pub(in crate::orchestration) struct DocumentLoaderClient {
    /// Actor reference used for all document RPC calls.
    actor: ActorRef<DocumentLoaderMessage>,
}

impl DocumentLoaderClient {
    /// Creates a typed client around one actor reference.
    #[must_use]
    fn new(actor: ActorRef<DocumentLoaderMessage>) -> Self {
        Self { actor }
    }

    /// Loads, validates, and merges `conductor.ncl` and `conductor.machine.ncl`.
    pub(in crate::orchestration) async fn load_and_unify(
        &self,
        user_ncl: &Path,
        machine_ncl: &Path,
        state_ncl: &Path,
        options: RunWorkflowOptions,
    ) -> Result<LoadedDocuments, ConductorError> {
        call_t!(
            self.actor,
            DocumentLoaderMessage::LoadAndUnify,
            DEFAULT_RPC_TIMEOUT_MS,
            user_ncl.to_path_buf(),
            machine_ncl.to_path_buf(),
            state_ncl.to_path_buf(),
            options
        )
        .map_err(|err| {
            ConductorError::Internal(format!("document loader load_and_unify RPC failed: {err}"))
        })?
    }

    /// Persists the machine-editable conductor document to disk.
    pub(in crate::orchestration) async fn persist_machine_document(
        &self,
        path: &Path,
        document: MachineNickelDocument,
    ) -> Result<(), ConductorError> {
        call_t!(
            self.actor,
            DocumentLoaderMessage::PersistMachineDocument,
            DEFAULT_RPC_TIMEOUT_MS,
            path.to_path_buf(),
            Box::new(document)
        )
        .map_err(|err| {
            ConductorError::Internal(format!(
                "document loader persist_machine_document RPC failed: {err}"
            ))
        })?
    }

    /// Persists the volatile state document to disk.
    pub(in crate::orchestration) async fn persist_state_document(
        &self,
        path: &Path,
        document: StateNickelDocument,
    ) -> Result<(), ConductorError> {
        call_t!(
            self.actor,
            DocumentLoaderMessage::PersistStateDocument,
            DEFAULT_RPC_TIMEOUT_MS,
            path.to_path_buf(),
            Box::new(document)
        )
        .map_err(|err| {
            ConductorError::Internal(format!(
                "document loader persist_state_document RPC failed: {err}"
            ))
        })?
    }
}

/// Requests supported by the document-loader actor.
#[derive(Debug)]
enum DocumentLoaderMessage {
    /// Loads both Nickel documents, evaluates their combined source, and returns
    /// the merged runtime representation plus machine-document updates.
    LoadAndUnify(
        PathBuf,
        PathBuf,
        PathBuf,
        RunWorkflowOptions,
        RpcReplyPort<Result<LoadedDocuments, ConductorError>>,
    ),
    /// Persists the machine-editable document after runtime updates.
    PersistMachineDocument(
        PathBuf,
        Box<MachineNickelDocument>,
        RpcReplyPort<Result<(), ConductorError>>,
    ),
    /// Persists the volatile state document after runtime updates.
    PersistStateDocument(
        PathBuf,
        Box<StateNickelDocument>,
        RpcReplyPort<Result<(), ConductorError>>,
    ),
}

/// Stateless actor that owns document parsing and merge policy.
#[derive(Debug, Clone, Copy, Default)]
struct DocumentLoaderActor;

impl Actor for DocumentLoaderActor {
    type Msg = DocumentLoaderMessage;
    type State = ();
    type Arguments = ();

    /// Initializes the actor with no mutable state because all behavior is pure over the requested files.
    async fn pre_start(
        &self,
        _myself: ActorRef<Self::Msg>,
        _args: Self::Arguments,
    ) -> Result<Self::State, ActorProcessingErr> {
        Ok(())
    }

    /// Handles document loading and persistence RPC calls.
    async fn handle(
        &self,
        _myself: ActorRef<Self::Msg>,
        message: Self::Msg,
        _state: &mut Self::State,
    ) -> Result<(), ActorProcessingErr> {
        match message {
            DocumentLoaderMessage::LoadAndUnify(
                user_ncl,
                machine_ncl,
                state_ncl,
                options,
                reply,
            ) => {
                let _ = reply.send(Self::load_and_unify_documents(
                    &user_ncl,
                    &machine_ncl,
                    &state_ncl,
                    options,
                ));
            }
            DocumentLoaderMessage::PersistMachineDocument(path, document, reply) => {
                let _ = reply.send(Self::persist_machine_document(&path, &document));
            }
            DocumentLoaderMessage::PersistStateDocument(path, document, reply) => {
                let _ = reply.send(Self::persist_state_document(&path, &document));
            }
        }
        Ok(())
    }
}

impl DocumentLoaderActor {
    /// Loads both documents, evaluates total configuration, and returns the merged runtime representation.
    fn load_and_unify_documents(
        user_ncl: &Path,
        machine_ncl: &Path,
        state_ncl: &Path,
        options: RunWorkflowOptions,
    ) -> Result<LoadedDocuments, ConductorError> {
        let user_source = Self::load_user_source(user_ncl, machine_ncl)?;
        let machine_source = Self::load_machine_source(machine_ncl)?;
        let state_source = Self::load_state_source(state_ncl)?;
        evaluate_total_configuration_sources(&user_source, &machine_source, &state_source)?;

        let user_document = Self::load_user_document(user_ncl, machine_ncl)?;
        let (mut machine_document, _machine_existed) = Self::load_machine_document(machine_ncl)?;
        let mut state_document = Self::load_state_document(state_ncl)?;
        let _validated_runtime_storage = Self::merge_runtime_storage(
            &user_document.runtime_storage,
            &machine_document.runtime_storage,
        )?;
        let (unified, merged_tools) =
            Self::unify_documents(&user_document, &machine_document, options)?;
        machine_document.tools = merged_tools;
        state_document.impure_timestamps = Self::merge_three_named_maps(
            "impure_timestamps",
            &user_document.impure_timestamps,
            &machine_document.impure_timestamps,
            &state_document.impure_timestamps,
        )?;
        let prior_state_pointer = Self::merge_three_state_pointers(
            user_document.state_pointer,
            machine_document.state_pointer,
            state_document.state_pointer,
        )?;
        state_document.state_pointer = prior_state_pointer;
        machine_document.impure_timestamps.clear();
        machine_document.state_pointer = None;

        Ok(LoadedDocuments { machine_document, state_document, prior_state_pointer, unified })
    }

    /// Merges two named maps and rejects conflicting duplicate definitions.
    fn merge_named_maps<T>(
        field_name: &str,
        user: &BTreeMap<String, T>,
        machine: &BTreeMap<String, T>,
    ) -> Result<BTreeMap<String, T>, ConductorError>
    where
        T: Clone + PartialEq,
    {
        let mut merged = user.clone();
        for (key, machine_value) in machine {
            match merged.get(key) {
                None => {
                    merged.insert(key.clone(), machine_value.clone());
                }
                Some(user_value) if user_value == machine_value => {}
                Some(_) => {
                    return Err(ConductorError::Workflow(format!(
                        "conflict while merging conductor.ncl and conductor.machine.ncl: '{field_name}.{key}' is defined differently in both documents"
                    )));
                }
            }
        }
        Ok(merged)
    }

    /// Merges three named maps and rejects conflicting duplicate definitions.
    fn merge_three_named_maps<T>(
        field_name: &str,
        user: &BTreeMap<String, T>,
        machine: &BTreeMap<String, T>,
        state: &BTreeMap<String, T>,
    ) -> Result<BTreeMap<String, T>, ConductorError>
    where
        T: Clone + PartialEq,
    {
        let merged_user_machine = Self::merge_named_maps(field_name, user, machine)?;
        Self::merge_named_maps(field_name, &merged_user_machine, state)
    }

    /// Merges tool definitions while enforcing immutable-by-default tool config.
    fn merge_tools(
        user: &BTreeMap<String, ToolSpec>,
        machine: &BTreeMap<String, ToolSpec>,
        options: RunWorkflowOptions,
    ) -> Result<BTreeMap<String, ToolSpec>, ConductorError> {
        let mut merged = machine.clone();

        for (tool_name, user_tool) in user {
            match merged.get(tool_name) {
                None => {
                    merged.insert(tool_name.clone(), user_tool.clone());
                }
                Some(machine_tool) if machine_tool == user_tool => {}
                Some(_) if options.allow_tool_redefinition => {
                    merged.insert(tool_name.clone(), user_tool.clone());
                }
                Some(machine_tool) => {
                    let diff_summary = Self::summarize_tool_spec_diff(machine_tool, user_tool);
                    return Err(ConductorError::Workflow(format!(
                        "tool '{tool_name}' is already defined with a different config ({diff_summary}); rerun with allow_tool_redefinition=true (CLI: --allow-tool-redefinition) to override"
                    )));
                }
            }
        }

        Ok(merged)
    }

    /// Builds a deterministic, human-readable field diff summary for one tool
    /// redefinition conflict.
    ///
    /// The returned summary is intentionally compact and stable so operators can
    /// quickly identify which portions of one tool spec diverged between
    /// `conductor.ncl` and `conductor.machine.ncl`.
    fn summarize_tool_spec_diff(machine: &ToolSpec, user: &ToolSpec) -> String {
        let mut differences = Vec::new();

        if machine.is_impure != user.is_impure {
            differences
                .push(format!("is_impure(machine={}, user={})", machine.is_impure, user.is_impure));
        }

        Self::append_named_map_differences(
            &mut differences,
            "inputs",
            &machine.inputs,
            &user.inputs,
        );

        if machine.kind != user.kind {
            differences.push("tool.kind.changed=true".to_string());
        }

        Self::append_named_map_differences(
            &mut differences,
            "outputs",
            &machine.outputs,
            &user.outputs,
        );

        if differences.is_empty() {
            "unknown field changes".to_string()
        } else {
            differences.join("; ")
        }
    }

    /// Appends deterministic key-level map differences to the provided output list.
    ///
    /// Invariant: this helper never mutates or reorders map inputs, and it emits
    /// keys in sorted order via `BTreeMap` iteration so diagnostics stay stable
    /// across runs.
    fn append_named_map_differences<T: PartialEq>(
        output: &mut Vec<String>,
        field_name: &str,
        machine: &BTreeMap<String, T>,
        user: &BTreeMap<String, T>,
    ) {
        let added: Vec<String> =
            user.keys().filter(|key| !machine.contains_key(*key)).cloned().collect();
        if !added.is_empty() {
            output.push(format!("{field_name}.added=[{}]", added.join(",")));
        }

        let removed: Vec<String> =
            machine.keys().filter(|key| !user.contains_key(*key)).cloned().collect();
        if !removed.is_empty() {
            output.push(format!("{field_name}.removed=[{}]", removed.join(",")));
        }

        let changed: Vec<String> = machine
            .iter()
            .filter_map(|(key, machine_value)| {
                user.get(key)
                    .and_then(|user_value| (user_value != machine_value).then_some(key.clone()))
            })
            .collect();
        if !changed.is_empty() {
            output.push(format!("{field_name}.changed=[{}]", changed.join(",")));
        }
    }

    /// Merges per-tool runtime configuration from the user and machine documents.
    fn merge_tool_configs(
        user: &BTreeMap<String, ToolConfigSpec>,
        machine: &BTreeMap<String, ToolConfigSpec>,
    ) -> Result<BTreeMap<String, ToolConfigSpec>, ConductorError> {
        let mut merged = user.clone();
        for (tool_name, machine_config) in machine {
            match merged.get_mut(tool_name) {
                None => {
                    merged.insert(tool_name.clone(), machine_config.clone());
                }
                Some(user_config) => {
                    if user_config.max_concurrent_calls != machine_config.max_concurrent_calls {
                        return Err(ConductorError::Workflow(format!(
                            "conflict while merging conductor.ncl and conductor.machine.ncl: 'tool_configs.{tool_name}.max_concurrent_calls' is defined differently in both documents"
                        )));
                    }

                    match (&mut user_config.content_map, &machine_config.content_map) {
                        (None, None) => {}
                        (None, Some(machine_map)) => {
                            user_config.content_map = Some(machine_map.clone());
                        }
                        (Some(_), None) => {}
                        (Some(user_map), Some(machine_map)) => {
                            for (relative_path, machine_hash) in machine_map {
                                match user_map.get(relative_path) {
                                    None => {
                                        user_map.insert(relative_path.clone(), *machine_hash);
                                    }
                                    Some(user_hash) if user_hash == machine_hash => {}
                                    Some(_) => {
                                        return Err(ConductorError::Workflow(format!(
                                            "conflict while merging conductor.ncl and conductor.machine.ncl: 'tool_configs.{tool_name}.content_map.{relative_path}' is defined differently in both documents"
                                        )));
                                    }
                                }
                            }
                        }
                    }
                }
            }
        }
        Ok(merged)
    }

    /// Merges optional state pointers and rejects conflicting duplicates.
    fn merge_state_pointer(
        user: Option<mediapm_cas::Hash>,
        machine: Option<mediapm_cas::Hash>,
    ) -> Result<Option<mediapm_cas::Hash>, ConductorError> {
        match (user, machine) {
            (None, None) => Ok(None),
            (Some(pointer), None) | (None, Some(pointer)) => Ok(Some(pointer)),
            (Some(user_pointer), Some(machine_pointer)) if user_pointer == machine_pointer => {
                Ok(Some(user_pointer))
            }
            (Some(_), Some(_)) => Err(ConductorError::Workflow(
                "conflict while merging conductor.ncl and conductor.machine.ncl: 'state_pointer' is defined differently in both documents"
                    .to_string(),
            )),
        }
    }

    /// Merges grouped runtime-storage path settings from user and machine
    /// documents and rejects conflicting duplicates.
    fn merge_runtime_storage(
        user: &RuntimeStorageConfig,
        machine: &RuntimeStorageConfig,
    ) -> Result<RuntimeStorageConfig, ConductorError> {
        let mut conflict_fields = Vec::new();
        if user.conductor_dir.is_some()
            && machine.conductor_dir.is_some()
            && user.conductor_dir != machine.conductor_dir
        {
            conflict_fields.push("conductor_dir");
        }
        if user.state_ncl.is_some()
            && machine.state_ncl.is_some()
            && user.state_ncl != machine.state_ncl
        {
            conflict_fields.push("state_ncl");
        }
        if user.cas_store_dir.is_some()
            && machine.cas_store_dir.is_some()
            && user.cas_store_dir != machine.cas_store_dir
        {
            conflict_fields.push("cas_store_dir");
        }

        if !conflict_fields.is_empty() {
            return Err(ConductorError::Workflow(format!(
                "conflict while merging conductor.ncl and conductor.machine.ncl: runtime storage path fields differ ({})",
                conflict_fields.join(", ")
            )));
        }

        Ok(RuntimeStorageConfig {
            conductor_dir: user.conductor_dir.clone().or_else(|| machine.conductor_dir.clone()),
            state_ncl: user.state_ncl.clone().or_else(|| machine.state_ncl.clone()),
            cas_store_dir: user.cas_store_dir.clone().or_else(|| machine.cas_store_dir.clone()),
        })
    }

    /// Merges optional state pointers across user, machine, and state
    /// documents and rejects conflicting duplicates.
    fn merge_three_state_pointers(
        user: Option<mediapm_cas::Hash>,
        machine: Option<mediapm_cas::Hash>,
        state: Option<mediapm_cas::Hash>,
    ) -> Result<Option<mediapm_cas::Hash>, ConductorError> {
        let merged_user_machine = Self::merge_state_pointer(user, machine)?;
        Self::merge_state_pointer(merged_user_machine, state)
    }

    /// Loads the user-editable source text, bootstrapping a default document when the file is missing or empty.
    fn load_user_source(user_ncl: &Path, machine_ncl: &Path) -> Result<String, ConductorError> {
        if !user_ncl.exists() {
            let bootstrap = Self::bootstrap_user_document(user_ncl, machine_ncl);
            let encoded = encode_user_document(bootstrap)?;
            return String::from_utf8(encoded).map_err(|err| {
                ConductorError::Serialization(format!(
                    "bootstrap conductor.ncl encoding produced invalid UTF-8: {err}"
                ))
            });
        }

        let content = std::fs::read_to_string(user_ncl).map_err(|source| ConductorError::Io {
            operation: "reading conductor.ncl".to_string(),
            path: user_ncl.to_path_buf(),
            source,
        })?;

        if content.trim().is_empty() {
            let bootstrap = Self::bootstrap_user_document(user_ncl, machine_ncl);
            let encoded = encode_user_document(bootstrap)?;
            return String::from_utf8(encoded).map_err(|err| {
                ConductorError::Serialization(format!(
                    "bootstrap conductor.ncl encoding produced invalid UTF-8: {err}"
                ))
            });
        }

        Ok(content)
    }

    /// Loads the machine-editable source text, returning a default empty document when the file is missing or empty.
    fn load_machine_source(machine_ncl: &Path) -> Result<String, ConductorError> {
        if !machine_ncl.exists() {
            let encoded = encode_machine_document(MachineNickelDocument::default())?;
            return String::from_utf8(encoded).map_err(|err| {
                ConductorError::Serialization(format!(
                    "default conductor.machine.ncl encoding produced invalid UTF-8: {err}"
                ))
            });
        }

        let content =
            std::fs::read_to_string(machine_ncl).map_err(|source| ConductorError::Io {
                operation: "reading conductor.machine.ncl".to_string(),
                path: machine_ncl.to_path_buf(),
                source,
            })?;

        if content.trim().is_empty() {
            let encoded = encode_machine_document(MachineNickelDocument::default())?;
            return String::from_utf8(encoded).map_err(|err| {
                ConductorError::Serialization(format!(
                    "default conductor.machine.ncl encoding produced invalid UTF-8: {err}"
                ))
            });
        }

        Ok(content)
    }

    /// Loads the volatile state source text, returning a default empty
    /// state document when the file is missing or empty.
    fn load_state_source(state_ncl: &Path) -> Result<String, ConductorError> {
        if !state_ncl.exists() {
            let encoded = encode_state_document(StateNickelDocument::default())?;
            return String::from_utf8(encoded).map_err(|err| {
                ConductorError::Serialization(format!(
                    "default .conductor/state.ncl encoding produced invalid UTF-8: {err}"
                ))
            });
        }

        let content = std::fs::read_to_string(state_ncl).map_err(|source| ConductorError::Io {
            operation: "reading .conductor/state.ncl".to_string(),
            path: state_ncl.to_path_buf(),
            source,
        })?;

        if content.trim().is_empty() {
            let encoded = encode_state_document(StateNickelDocument::default())?;
            return String::from_utf8(encoded).map_err(|err| {
                ConductorError::Serialization(format!(
                    "default .conductor/state.ncl encoding produced invalid UTF-8: {err}"
                ))
            });
        }

        Ok(content)
    }

    /// Parses the user-editable Nickel document from the effective source text.
    fn load_user_document(
        user_ncl: &Path,
        machine_ncl: &Path,
    ) -> Result<UserNickelDocument, ConductorError> {
        let content = Self::load_user_source(user_ncl, machine_ncl)?;
        decode_user_document(content.as_bytes())
    }

    /// Builds the default bootstrap document used when no user config exists yet.
    fn bootstrap_user_document(user_ncl: &Path, machine_ncl: &Path) -> UserNickelDocument {
        let tool_name = "workflow-placeholder@1.0.0".to_string();
        let step = WorkflowStepSpec {
            id: "bootstrap".to_string(),
            tool: tool_name.clone(),
            inputs: BTreeMap::from([(
                "text".to_string(),
                InputBinding::String(format!(
                    "bootstrap user_ncl={} machine_ncl={}",
                    user_ncl.display(),
                    machine_ncl.display()
                )),
            )]),
            depends_on: Vec::new(),
            outputs: BTreeMap::new(),
        };

        UserNickelDocument {
            metadata: NickelDocumentMetadata::default(),
            external_data: BTreeMap::new(),
            tools: BTreeMap::from([(
                tool_name,
                ToolSpec {
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
                WorkflowSpec { steps: vec![step] },
            )]),
            ..UserNickelDocument::default()
        }
    }

    /// Parses the machine-editable Nickel document from the effective source text.
    fn load_machine_document(
        machine_ncl: &Path,
    ) -> Result<(MachineNickelDocument, bool), ConductorError> {
        let content = Self::load_machine_source(machine_ncl)?;
        let parsed = decode_machine_document(content.as_bytes())?;
        Ok((parsed, machine_ncl.exists()))
    }

    /// Parses the volatile state document from effective source text.
    fn load_state_document(state_ncl: &Path) -> Result<StateNickelDocument, ConductorError> {
        let content = Self::load_state_source(state_ncl)?;
        decode_state_document(content.as_bytes())
    }

    /// Persists the machine-editable document atomically to its target path.
    fn persist_machine_document(
        path: &Path,
        document: &MachineNickelDocument,
    ) -> Result<(), ConductorError> {
        if let Some(parent) = path.parent() {
            std::fs::create_dir_all(parent).map_err(|source| ConductorError::Io {
                operation: "creating parent directory for conductor.machine.ncl".to_string(),
                path: parent.to_path_buf(),
                source,
            })?;
        }

        let encoded = encode_machine_document(document.clone())?;
        std::fs::write(path, encoded).map_err(|source| ConductorError::Io {
            operation: "writing conductor.machine.ncl".to_string(),
            path: path.to_path_buf(),
            source,
        })
    }

    /// Persists the volatile state document atomically to its target path.
    fn persist_state_document(
        path: &Path,
        document: &StateNickelDocument,
    ) -> Result<(), ConductorError> {
        if let Some(parent) = path.parent() {
            std::fs::create_dir_all(parent).map_err(|source| ConductorError::Io {
                operation: "creating parent directory for .conductor/state.ncl".to_string(),
                path: parent.to_path_buf(),
                source,
            })?;
        }

        let encoded = encode_state_document(document.clone())?;
        std::fs::write(path, encoded).map_err(|source| ConductorError::Io {
            operation: "writing .conductor/state.ncl".to_string(),
            path: path.to_path_buf(),
            source,
        })
    }

    /// Produces the merged runtime representation from the two parsed documents.
    fn unify_documents(
        user: &UserNickelDocument,
        machine: &MachineNickelDocument,
        options: RunWorkflowOptions,
    ) -> Result<(UnifiedNickelDocument, BTreeMap<String, ToolSpec>), ConductorError> {
        let external_data =
            Self::merge_named_maps("external_data", &user.external_data, &machine.external_data)?;
        let merged_tools = Self::merge_tools(&user.tools, &machine.tools, options)?;
        let workflows = Self::merge_named_maps("workflows", &user.workflows, &machine.workflows)?;
        let tool_configs = Self::merge_tool_configs(&user.tool_configs, &machine.tool_configs)?;
        let mut tools = BTreeMap::new();
        let mut tool_content_hashes = BTreeSet::new();

        for machine_tool_name in tool_configs.keys() {
            if !merged_tools.contains_key(machine_tool_name) {
                return Err(ConductorError::Workflow(format!(
                    "conflict while merging conductor.ncl and conductor.machine.ncl: tool_configs references unknown tool '{machine_tool_name}'"
                )));
            }
        }

        for (tool_name, tool_spec) in &merged_tools {
            if !tool_name.contains('@') {
                return Err(ConductorError::Workflow(format!(
                    "tool '{tool_name}' must include immutable version in its name (for example: compose@1.0.0)"
                )));
            }

            let process = match &tool_spec.kind {
                ToolKindSpec::Executable { command, env_vars, success_codes } => {
                    let Some(executable) = command.first() else {
                        return Err(ConductorError::Workflow(format!(
                            "tool '{tool_name}' executable command must contain at least one entry"
                        )));
                    };
                    if executable.trim().is_empty() {
                        return Err(ConductorError::Workflow(format!(
                            "tool '{tool_name}' executable command[0] must be non-empty"
                        )));
                    }
                    ProcessSpec::Executable {
                        command: command.clone(),
                        env_vars: env_vars.clone(),
                        success_codes: success_codes.clone(),
                    }
                }
                ToolKindSpec::Builtin { name, version } => {
                    if name.trim().is_empty() || version.trim().is_empty() {
                        return Err(ConductorError::Workflow(format!(
                            "tool '{tool_name}' builtin process must provide non-empty name and version"
                        )));
                    }
                    ProcessSpec::Builtin {
                        name: name.clone(),
                        version: version.clone(),
                        args: BTreeMap::new(),
                    }
                }
            };

            if tool_spec.outputs.is_empty() {
                return Err(ConductorError::Workflow(format!(
                    "tool '{tool_name}' must declare at least one output with a capture source"
                )));
            }

            let merged_config = tool_configs.get(tool_name).cloned().unwrap_or_default();
            if merged_config.max_concurrent_calls == 0 || merged_config.max_concurrent_calls < -1 {
                return Err(ConductorError::Workflow(format!(
                    "tool '{tool_name}' max_concurrent_calls must be -1 or a positive integer"
                )));
            }

            if merged_config.content_map.is_some()
                && matches!(tool_spec.kind, ToolKindSpec::Builtin { .. })
            {
                return Err(ConductorError::Workflow(format!(
                    "tool '{tool_name}' content_map is invalid for builtin tools"
                )));
            }

            let merged_map = merged_config.content_map.unwrap_or_default();
            tool_content_hashes.extend(merged_map.values().copied());
            tools.insert(
                tool_name.clone(),
                UnifiedToolSpec {
                    is_impure: tool_spec.is_impure,
                    max_concurrent_calls: merged_config.max_concurrent_calls,
                    inputs: tool_spec.inputs.clone(),
                    process,
                    outputs: tool_spec.outputs.clone(),
                    tool_content_map: merged_map,
                },
            );
        }

        for (workflow_name, workflow) in &workflows {
            for step in &workflow.steps {
                if !tools.contains_key(&step.tool) {
                    let available = if tools.is_empty() {
                        "<none>".to_string()
                    } else {
                        tools.keys().cloned().collect::<Vec<_>>().join(", ")
                    };
                    return Err(ConductorError::Workflow(format!(
                        "workflow '{workflow_name}' references unknown tool '{}'; available merged tools: {available}",
                        step.tool,
                    )));
                }
            }
        }

        Ok((
            UnifiedNickelDocument { external_data, tools, workflows, tool_content_hashes },
            merged_tools,
        ))
    }
}

/// Spawns the document-loader actor and returns its typed client.
pub(in crate::orchestration) async fn spawn_document_loader_actor()
-> Result<DocumentLoaderClient, ConductorError> {
    let (actor_ref, _handle) =
        Actor::spawn(None, DocumentLoaderActor, ()).await.map_err(|err| {
            ConductorError::Internal(format!("failed spawning document loader actor: {err}"))
        })?;
    Ok(DocumentLoaderClient::new(actor_ref))
}

#[cfg(test)]
mod tests {
    use std::collections::BTreeMap;

    use mediapm_cas::Hash;
    use tempfile::tempdir;

    use crate::api::RunWorkflowOptions;
    use crate::error::ConductorError;
    use crate::model::config::{
        MachineNickelDocument, OutputCaptureSpec, RuntimeStorageConfig, ToolConfigSpec,
        ToolKindSpec, ToolOutputSpec, ToolSpec, UserNickelDocument, WorkflowSpec,
        encode_machine_document, encode_user_document,
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
        subject = {
            hash = "blake3:0000000000000000000000000000000000000000000000000000000000000000",
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
        subject = {
            hash = "blake3:1111111111111111111111111111111111111111111111111111111111111111",
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
            RunWorkflowOptions::default(),
        );
        match result {
            Err(ConductorError::Workflow(message)) => {
                assert!(message.contains("external_data.subject"));
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
            tool_configs: BTreeMap::from([(
                "unknown_tool@v9.9.9".to_string(),
                ToolConfigSpec {
                    max_concurrent_calls: -1,
                    description: Some("unknown tool test config".to_string()),
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
            RunWorkflowOptions::default(),
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
            RunWorkflowOptions::default(),
        );
        match result {
            Err(ConductorError::Workflow(message)) => {
                assert!(message.contains("max_concurrent_calls must be -1 or a positive integer"));
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
                        ToolOutputSpec { capture: OutputCaptureSpec::Stdout {} },
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
                        ToolOutputSpec { capture: OutputCaptureSpec::Stdout {} },
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
            RunWorkflowOptions::default(),
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
                        ToolOutputSpec { capture: OutputCaptureSpec::Stdout {} },
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
                        ToolOutputSpec { capture: OutputCaptureSpec::Stdout {} },
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
            RunWorkflowOptions { allow_tool_redefinition: true, ..RunWorkflowOptions::default() },
        )
        .expect("override option should allow redefinition");

        let merged_tool = loaded
            .machine_document
            .tools
            .get("echo@1.0.0")
            .expect("merged machine tool should exist");
        let ToolKindSpec::Builtin { version, .. } = &merged_tool.kind else {
            panic!("expected builtin process");
        };
        assert_eq!(version, "1.0.0");
    }

    /// Protects invariant that runtime-storage settings are validated but not
    /// auto-backfilled into `conductor.machine.ncl`.
    #[test]
    fn machine_document_is_not_backfilled_with_user_runtime_storage() {
        let dir = tempdir().expect("tempdir");
        let user_path = dir.path().join("conductor.ncl");
        let machine_path = dir.path().join("conductor.machine.ncl");
        let state_path = dir.path().join(".conductor").join("state.ncl");

        let user = UserNickelDocument {
            runtime_storage: RuntimeStorageConfig {
                conductor_dir: Some(".runtime".to_string()),
                state_ncl: Some(".runtime/state.ncl".to_string()),
                cas_store_dir: Some(".runtime/store".to_string()),
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
                        ToolOutputSpec { capture: OutputCaptureSpec::Stdout {} },
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
            RunWorkflowOptions::default(),
        )
        .expect("documents should load and unify");

        assert!(loaded.machine_document.runtime_storage.is_empty());
    }
}
