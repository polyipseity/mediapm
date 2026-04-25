//! Actor-backed loading and persistence for conductor Nickel documents.
//!
//! This actor owns the user/machine document merge contract so the coordinator
//! can sequence workflows without also carrying parsing and merge logic inline.

use std::collections::{BTreeMap, BTreeSet};
use std::fmt::Display;
use std::path::{Path, PathBuf};

use ractor::{Actor, ActorProcessingErr, ActorRef, RpcReplyPort, call_t};

use crate::api::RunWorkflowOptions;
use crate::error::ConductorError;
use crate::model::config::{
    InputBinding, MachineNickelDocument, NickelDocumentMetadata, OutputCaptureSpec,
    PlatformInheritedEnvVars, ProcessSpec, RuntimeStorageConfig, StateNickelDocument,
    ToolConfigSpec, ToolKindSpec, ToolOutputSpec, ToolSpec, UserNickelDocument, WorkflowSpec,
    WorkflowStepSpec, decode_machine_document, decode_state_document, decode_user_document,
    default_runtime_inherited_env_vars_for_host, encode_machine_document, encode_state_document,
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
        state_config: &Path,
        options: RunWorkflowOptions,
    ) -> Result<LoadedDocuments, ConductorError> {
        call_t!(
            self.actor,
            DocumentLoaderMessage::LoadAndUnify,
            DEFAULT_RPC_TIMEOUT_MS,
            user_ncl.to_path_buf(),
            machine_ncl.to_path_buf(),
            state_config.to_path_buf(),
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
                state_config,
                options,
                reply,
            ) => {
                let _ = reply.send(Self::load_and_unify_documents(
                    &user_ncl,
                    &machine_ncl,
                    &state_config,
                    &options,
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
        state_config: &Path,
        options: &RunWorkflowOptions,
    ) -> Result<LoadedDocuments, ConductorError> {
        let user_source = Self::load_user_source(user_ncl, machine_ncl)?;
        let machine_source = Self::load_machine_source(machine_ncl)?;
        let state_source = Self::load_state_source(state_config)?;
        evaluate_total_configuration_sources(&user_source, &machine_source, &state_source)?;

        let user_document = Self::load_user_document(user_ncl, machine_ncl)?;
        let (mut machine_document, _machine_existed) = Self::load_machine_document(machine_ncl)?;
        let mut state_document = Self::load_state_document(state_config)?;
        let merged_runtime_storage = Self::merge_runtime_storage(
            &user_document.runtime,
            &machine_document.runtime,
            &options.runtime_inherited_env_vars,
        )?;
        Self::materialize_machine_runtime_inherited_env_var_defaults(&mut machine_document);
        let (unified, merged_tools) = Self::unify_documents(
            &user_document,
            &machine_document,
            &merged_runtime_storage,
            options,
        )?;
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
    fn merge_named_maps<K, T>(
        field_name: &str,
        user: &BTreeMap<K, T>,
        machine: &BTreeMap<K, T>,
    ) -> Result<BTreeMap<K, T>, ConductorError>
    where
        K: Ord + Clone + Display,
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
    fn merge_three_named_maps<K, T>(
        field_name: &str,
        user: &BTreeMap<K, T>,
        machine: &BTreeMap<K, T>,
        state: &BTreeMap<K, T>,
    ) -> Result<BTreeMap<K, T>, ConductorError>
    where
        K: Ord + Clone + Display,
        T: Clone + PartialEq,
    {
        let merged_user_machine = Self::merge_named_maps(field_name, user, machine)?;
        Self::merge_named_maps(field_name, &merged_user_machine, state)
    }

    /// Merges tool definitions while enforcing immutable-by-default tool config.
    fn merge_tools(
        user: &BTreeMap<String, ToolSpec>,
        machine: &BTreeMap<String, ToolSpec>,
        options: &RunWorkflowOptions,
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
                    if user_config.max_retries != machine_config.max_retries {
                        return Err(ConductorError::Workflow(format!(
                            "conflict while merging conductor.ncl and conductor.machine.ncl: 'tool_configs.{tool_name}.max_retries' is defined differently in both documents"
                        )));
                    }

                    for (input_name, machine_binding) in &machine_config.input_defaults {
                        match user_config.input_defaults.get(input_name) {
                            None => {
                                user_config
                                    .input_defaults
                                    .insert(input_name.clone(), machine_binding.clone());
                            }
                            Some(user_binding) if user_binding == machine_binding => {}
                            Some(_) => {
                                return Err(ConductorError::Workflow(format!(
                                    "conflict while merging conductor.ncl and conductor.machine.ncl: 'tool_configs.{tool_name}.input_defaults.{input_name}' is defined differently in both documents"
                                )));
                            }
                        }
                    }

                    for (env_key, machine_env_value) in &machine_config.env_vars {
                        match user_config.env_vars.get(env_key) {
                            None => {
                                user_config
                                    .env_vars
                                    .insert(env_key.clone(), machine_env_value.clone());
                            }
                            Some(user_env_value) if user_env_value == machine_env_value => {}
                            Some(_) => {
                                return Err(ConductorError::Workflow(format!(
                                    "conflict while merging conductor.ncl and conductor.machine.ncl: 'tool_configs.{tool_name}.env_vars.{env_key}' is defined differently in both documents"
                                )));
                            }
                        }
                    }

                    match (&mut user_config.content_map, &machine_config.content_map) {
                        (_, None) => {}
                        (None, Some(machine_map)) => {
                            user_config.content_map = Some(machine_map.clone());
                        }
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

    /// Merges executable tool environment from tool declaration and
    /// `tool_configs` runtime config.
    ///
    /// Runtime inherited host keys are merged first and can be intentionally
    /// overridden by explicit map entries.
    ///
    /// Duplicate keys are rejected only across explicit maps
    /// (`tools.<tool>.env_vars` and `tool_configs.<tool>.env_vars`), even when
    /// values match, so one source of truth exists for explicit tool config.
    fn merge_executable_runtime_env_vars(
        tool_name: &str,
        declared_env_vars: &BTreeMap<String, String>,
        config_env_vars: &BTreeMap<String, String>,
        inherited_env_var_names: &[String],
    ) -> Result<BTreeMap<String, String>, ConductorError> {
        let mut merged = BTreeMap::new();

        for inherited_name in inherited_env_var_names {
            if let Some(value) = std::env::var_os(inherited_name) {
                merged.insert(
                    inherited_name.clone(),
                    Self::escape_template_literal(value.to_string_lossy().as_ref()),
                );
            }
        }

        for (env_key, env_value) in declared_env_vars {
            merged.insert(env_key.clone(), env_value.clone());
        }

        for (env_key, env_value) in config_env_vars {
            if declared_env_vars.contains_key(env_key) {
                return Err(ConductorError::Workflow(format!(
                    "tool '{tool_name}' has duplicate environment key '{env_key}' across tools.{tool_name}.env_vars and tool_configs.{tool_name}.env_vars"
                )));
            }
            merged.insert(env_key.clone(), env_value.clone());
        }
        Ok(merged)
    }

    /// Appends trimmed environment-variable names from `source` into `target`
    /// while preserving order and removing case-insensitive duplicates.
    fn append_unique_env_var_names(target: &mut Vec<String>, source: &[String]) {
        for raw_name in source {
            let trimmed = raw_name.trim();
            if trimmed.is_empty() {
                continue;
            }

            if target.iter().any(|existing| existing.eq_ignore_ascii_case(trimmed)) {
                continue;
            }

            target.push(trimmed.to_string());
        }
    }

    /// Normalizes one authored platform key for inherited env-var mapping.
    fn normalize_runtime_platform_key(raw_platform: &str) -> Option<String> {
        let trimmed = raw_platform.trim();
        if trimmed.is_empty() { None } else { Some(trimmed.to_ascii_lowercase()) }
    }

    /// Appends one platform-keyed inherited env-var map into the target map
    /// with case-insensitive key normalization and per-platform de-duplication.
    fn append_platform_inherited_env_var_map(
        target: &mut PlatformInheritedEnvVars,
        source: &PlatformInheritedEnvVars,
    ) {
        for (platform_key, names) in source {
            let Some(normalized_platform) = Self::normalize_runtime_platform_key(platform_key)
            else {
                continue;
            };

            let target_names = target.entry(normalized_platform).or_default();
            Self::append_unique_env_var_names(target_names, names);
        }
    }

    /// Escapes plain string literals for conductor template rendering.
    #[must_use]
    fn escape_template_literal(value: &str) -> String {
        value.replace('\\', "\\\\")
    }

    /// Merges grouped runtime-storage path settings from user and machine
    /// documents and rejects conflicting duplicates.
    fn merge_runtime_storage(
        user: &RuntimeStorageConfig,
        machine: &RuntimeStorageConfig,
        runtime_inherited_env_vars: &[String],
    ) -> Result<RuntimeStorageConfig, ConductorError> {
        let mut conflict_fields = Vec::new();
        if user.conductor_dir.is_some()
            && machine.conductor_dir.is_some()
            && user.conductor_dir != machine.conductor_dir
        {
            conflict_fields.push("conductor_dir");
        }
        if user.state_config.is_some()
            && machine.state_config.is_some()
            && user.state_config != machine.state_config
        {
            conflict_fields.push("state_config");
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

        let mut inherited_env_vars = PlatformInheritedEnvVars::new();
        if let Some(user_map) = user.inherited_env_vars.as_ref() {
            Self::append_platform_inherited_env_var_map(&mut inherited_env_vars, user_map);
        }
        if let Some(machine_map) = machine.inherited_env_vars.as_ref() {
            Self::append_platform_inherited_env_var_map(&mut inherited_env_vars, machine_map);
        }

        let host_platform = std::env::consts::OS.to_ascii_lowercase();
        if !runtime_inherited_env_vars.is_empty() {
            let target_names = inherited_env_vars.entry(host_platform).or_default();
            Self::append_unique_env_var_names(target_names, runtime_inherited_env_vars);
        }

        inherited_env_vars.retain(|_, names| !names.is_empty());

        let inherited_env_vars =
            if inherited_env_vars.is_empty() { None } else { Some(inherited_env_vars) };

        Ok(RuntimeStorageConfig {
            conductor_dir: user.conductor_dir.clone().or_else(|| machine.conductor_dir.clone()),
            state_config: user.state_config.clone().or_else(|| machine.state_config.clone()),
            cas_store_dir: user.cas_store_dir.clone().or_else(|| machine.cas_store_dir.clone()),
            inherited_env_vars,
        })
    }

    /// Ensures machine runtime storage materializes host-default inherited
    /// environment names when omitted.
    ///
    /// This makes default inheritance explicit in `conductor.machine.ncl`
    /// without copying user-authored runtime overrides.
    fn materialize_machine_runtime_inherited_env_var_defaults(machine: &mut MachineNickelDocument) {
        if machine.runtime.inherited_env_vars.is_some() {
            return;
        }

        let defaults = default_runtime_inherited_env_vars_for_host();
        if defaults.is_empty() {
            return;
        }

        machine.runtime.inherited_env_vars = Some(defaults);
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
    fn load_state_source(state_config: &Path) -> Result<String, ConductorError> {
        if !state_config.exists() {
            let encoded = encode_state_document(StateNickelDocument::default())?;
            return String::from_utf8(encoded).map_err(|err| {
                ConductorError::Serialization(format!(
                    "default .conductor/state.ncl encoding produced invalid UTF-8: {err}"
                ))
            });
        }

        let content =
            std::fs::read_to_string(state_config).map_err(|source| ConductorError::Io {
                operation: "reading .conductor/state.ncl".to_string(),
                path: state_config.to_path_buf(),
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
                WorkflowSpec {
                    name: Some("default".to_string()),
                    description: Some(
                        "Bootstrap workflow generated when conductor.ncl is missing".to_string(),
                    ),
                    steps: vec![step],
                },
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
    fn load_state_document(state_config: &Path) -> Result<StateNickelDocument, ConductorError> {
        let content = Self::load_state_source(state_config)?;
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
    #[allow(clippy::too_many_lines)]
    fn unify_documents(
        user: &UserNickelDocument,
        machine: &MachineNickelDocument,
        runtime_storage: &RuntimeStorageConfig,
        options: &RunWorkflowOptions,
    ) -> Result<(UnifiedNickelDocument, BTreeMap<String, ToolSpec>), ConductorError> {
        let external_data =
            Self::merge_named_maps("external_data", &user.external_data, &machine.external_data)?;
        let external_hashes = external_data.keys().copied().collect::<BTreeSet<_>>();
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
            if merged_config.max_retries < -1 {
                return Err(ConductorError::Workflow(format!(
                    "tool '{tool_name}' max_retries must be -1 or a non-negative integer"
                )));
            }

            if merged_config.content_map.is_some()
                && matches!(tool_spec.kind, ToolKindSpec::Builtin { .. })
            {
                return Err(ConductorError::Workflow(format!(
                    "tool '{tool_name}' content_map is invalid for builtin tools"
                )));
            }

            if matches!(tool_spec.kind, ToolKindSpec::Builtin { .. })
                && !merged_config.input_defaults.is_empty()
            {
                return Err(ConductorError::Workflow(format!(
                    "tool '{tool_name}' input_defaults is invalid for builtin tools"
                )));
            }

            if matches!(tool_spec.kind, ToolKindSpec::Builtin { .. })
                && !merged_config.env_vars.is_empty()
            {
                return Err(ConductorError::Workflow(format!(
                    "tool '{tool_name}' env_vars is invalid for builtin tools"
                )));
            }

            let runtime_inherited_env_var_names =
                runtime_storage.inherited_env_vars_with_defaults();

            let execution_env_vars = match &tool_spec.kind {
                ToolKindSpec::Executable { env_vars, .. } => {
                    Self::merge_executable_runtime_env_vars(
                        tool_name,
                        env_vars,
                        &merged_config.env_vars,
                        &runtime_inherited_env_var_names,
                    )?
                }
                ToolKindSpec::Builtin { .. } => BTreeMap::new(),
            };

            for (input_name, binding) in &merged_config.input_defaults {
                let Some(input_spec) = tool_spec.inputs.get(input_name) else {
                    return Err(ConductorError::Workflow(format!(
                        "tool '{tool_name}' input_defaults references undeclared input '{input_name}'"
                    )));
                };

                match (input_spec.kind, binding) {
                    (crate::model::config::ToolInputKind::String, InputBinding::String(_))
                    | (
                        crate::model::config::ToolInputKind::StringList,
                        InputBinding::StringList(_),
                    ) => {}
                    (crate::model::config::ToolInputKind::String, InputBinding::StringList(_)) => {
                        return Err(ConductorError::Workflow(format!(
                            "tool '{tool_name}' input_defaults['{input_name}'] expects kind 'string' but received 'string_list'"
                        )));
                    }
                    (crate::model::config::ToolInputKind::StringList, InputBinding::String(_)) => {
                        return Err(ConductorError::Workflow(format!(
                            "tool '{tool_name}' input_defaults['{input_name}'] expects kind 'string_list' but received 'string'"
                        )));
                    }
                }
            }

            let merged_map = merged_config.content_map.unwrap_or_default();
            for (relative_path, hash) in &merged_map {
                if !external_hashes.contains(hash) {
                    return Err(ConductorError::Workflow(format!(
                        "tool '{tool_name}' content_map '{relative_path}' references hash '{hash}' that is missing from merged external_data"
                    )));
                }
            }
            tool_content_hashes.extend(merged_map.values().copied());
            tools.insert(
                tool_name.clone(),
                UnifiedToolSpec {
                    is_impure: tool_spec.is_impure,
                    max_concurrent_calls: merged_config.max_concurrent_calls,
                    max_retries: if merged_config.max_retries == -1 {
                        0
                    } else {
                        merged_config.max_retries
                    },
                    inputs: tool_spec.inputs.clone(),
                    default_inputs: merged_config.input_defaults,
                    process,
                    execution_env_vars,
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
        );
        match result {
            Err(ConductorError::Workflow(message)) => {
                assert!(message.contains("duplicate environment key 'DUPLICATE'"));
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
            &RunWorkflowOptions::default(),
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
            &RunWorkflowOptions { allow_tool_redefinition: true, ..RunWorkflowOptions::default() },
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
                state_config: Some(".runtime/state.ncl".to_string()),
                cas_store_dir: Some(".runtime/store".to_string()),
                inherited_env_vars: None,
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
            &RunWorkflowOptions::default(),
        )
        .expect("documents should load and unify");

        assert!(loaded.machine_document.runtime.conductor_dir.is_none());
        assert!(loaded.machine_document.runtime.state_config.is_none());
        assert!(loaded.machine_document.runtime.cas_store_dir.is_none());

        let expected_defaults = default_runtime_inherited_env_vars_for_host();
        if expected_defaults.is_empty() {
            assert!(loaded.machine_document.runtime.inherited_env_vars.is_none());
        } else {
            assert_eq!(loaded.machine_document.runtime.inherited_env_vars, Some(expected_defaults));
        }
    }
}
