//! Actor-backed step execution.
//!
//! Step workers own the expensive, side-effecting portion of orchestration:
//! resolving inputs, rendering templates, materializing sandbox files, running
//! processes, and capturing declared outputs. The execution hub interacts with
//! them through deterministic request/response messages while the coordinator
//! keeps state merging separate.

use std::collections::{BTreeMap, BTreeSet};
use std::marker::PhantomData;
use std::path::{Component, Path};
use std::sync::Arc;
use std::time::Instant;

use mediapm_cas::{CasApi, CasError, Constraint, ConstraintPatch, Hash, empty_content_hash};
use ractor::{Actor, ActorProcessingErr, ActorRef, RpcReplyPort};

use crate::error::{ConductorError, CorruptWorkflowOutputContext};
use crate::model::config::{
    InputBinding, OutputCaptureSpec, ParsedInputBindingSegment, ProcessSpec, ToolInputKind,
    ToolKindSpec, ToolSpec, WorkflowStepSpec, parse_input_binding,
};
use crate::model::state::{
    OutputRef, PersistenceFlags, ResolvedInput, ToolCallInstance, merge_persistence_flags,
};
use crate::orchestration::protocol::{
    StepExecutionBundle, StepExecutionRequest, StepOutputs, UnifiedNickelDocument, UnifiedToolSpec,
};

mod template;

/// Worker actor request envelope.
#[derive(Debug)]
pub(super) enum StepWorkerMessage {
    /// Executes one planned step request and returns its merge-ready bundle.
    ExecuteStep(
        Box<StepExecutionRequest>,
        RpcReplyPort<Result<StepExecutionBundle, ConductorError>>,
    ),
}

/// Actor marker for one step worker bound to one CAS handle.
#[derive(Debug, Clone, Copy)]
struct StepWorkerActor<C> {
    /// Type marker for the CAS implementation used by the worker.
    _phantom: PhantomData<C>,
}

impl<C> Default for StepWorkerActor<C> {
    /// Builds one worker marker with no local mutable fields.
    fn default() -> Self {
        Self { _phantom: PhantomData }
    }
}

/// Process definition after template rendering and kind-specific normalization.
#[derive(Debug, Clone)]
enum ResolvedProcessExecution {
    /// External executable process to run inside the ad hoc sandbox directory.
    Executable {
        executable: String,
        args: Vec<String>,
        env_vars: BTreeMap<String, String>,
        success_codes: BTreeSet<i32>,
    },
    /// Builtin process to dispatch by builtin name and version.
    Builtin { name: String, version: String, args: BTreeMap<String, String> },
}

/// Output capture location after template rendering.
#[derive(Debug, Clone)]
enum ResolvedOutputCapture {
    /// Capture bytes from the process stdout stream.
    Stdout,
    /// Capture bytes from the process stderr stream.
    Stderr,
    /// Capture process exit code as UTF-8 text bytes.
    ProcessCode,
    /// Capture bytes from one file relative to the execution sandbox.
    File { relative_path: std::path::PathBuf },
    /// Capture one directory by zipping descendants into one ZIP payload.
    FolderAsZip {
        /// Relative directory path inside the execution sandbox.
        relative_path: std::path::PathBuf,
        /// Whether the top-level folder node is included in the ZIP payload.
        include_topmost_folder: bool,
    },
}

/// Fully resolved output specification for one declared tool output.
#[derive(Debug, Clone)]
struct ResolvedOutputSpec {
    /// Where the output payload should be captured from.
    capture: ResolvedOutputCapture,
    /// Final persistence policy after tool defaults and step overrides merge.
    persistence: PersistenceFlags,
}

/// In-memory capture buffers returned by process execution.
#[derive(Debug, Clone, Default)]
struct ToolExecutionCapture {
    /// Raw stdout bytes from the executed process.
    stdout: Vec<u8>,
    /// Raw stderr bytes from the executed process.
    stderr: Vec<u8>,
    /// Exit code returned by the executed process.
    process_code: i32,
}

/// One deferred `${...:file(...)}` materialization requested during template rendering.
#[derive(Debug, Clone)]
struct TemplateFileWrite {
    /// Relative destination path inside the ad hoc execution directory.
    relative_path: std::path::PathBuf,
    /// Raw bytes that should be written to `relative_path` at execution time.
    plain_content: Vec<u8>,
}

/// Concrete ZIP-selector result before optional template materialization.
#[derive(Debug, Clone)]
enum ExtractedZipSelection {
    /// One concrete file payload selected from the ZIP archive.
    File(Vec<u8>),
    /// All descendant file payloads selected from one ZIP directory entry.
    Directory(BTreeMap<std::path::PathBuf, Vec<u8>>),
}

/// Normalized interpretation of one executable `content_map` entry key.
#[derive(Debug, Clone, PartialEq, Eq)]
enum ToolContentMapEntry {
    /// Regular file materialization (`key` does not end with `/` or `\`).
    File {
        /// Normalized sandbox-relative file path.
        relative_path: std::path::PathBuf,
    },
    /// Directory materialization from ZIP payload (`key` ends with `/` or `\`).
    DirectoryFromZip {
        /// Normalized sandbox-relative destination directory path.
        relative_dir: std::path::PathBuf,
    },
}

/// One planned `content_map` materialization entry prepared before writes.
#[derive(Debug, Clone)]
struct PlannedToolContentMaterialization {
    /// Original `content_map` key used for diagnostics.
    raw_relative_path: String,
    /// Concrete payload write/unpack action for this entry.
    payload: PlannedToolContentPayload,
    /// Concrete sandbox-relative file paths this entry will create or replace.
    claimed_relative_files: BTreeSet<std::path::PathBuf>,
}

/// Concrete payload action for one planned `content_map` entry.
#[derive(Debug, Clone)]
enum PlannedToolContentPayload {
    /// Writes one file payload directly into one sandbox-relative path.
    File {
        /// Destination file path relative to the execution sandbox.
        relative_path: std::path::PathBuf,
        /// Raw file bytes resolved from CAS.
        plain_content: Vec<u8>,
    },
    /// Unpacks one ZIP payload into one sandbox-relative directory.
    DirectoryFromZip {
        /// Destination directory path relative to the execution sandbox.
        relative_dir: std::path::PathBuf,
        /// ZIP bytes resolved from CAS.
        zip_content: Vec<u8>,
    },
}

/// Resolved selector source for one template interpolation token.
#[derive(Debug, Clone, PartialEq, Eq)]
enum TemplateSelectorSource {
    /// Selector resolves from step input bindings (`inputs.*`).
    Input(String),
    /// Selector resolves to current host platform text (`windows`/`linux`/`macos`).
    ContextOs,
}

/// Helper object that executes one step against one CAS implementation.
#[derive(Debug, Clone)]
struct StepWorkerExecutor<C>
where
    C: CasApi,
{
    /// Shared CAS handle used for all input, output, and state-addressing I/O.
    cas: Arc<C>,
}

impl<C> Actor for StepWorkerActor<C>
where
    C: CasApi + Send + Sync + 'static,
{
    type Msg = StepWorkerMessage;
    type State = Arc<C>;
    type Arguments = Arc<C>;

    /// Initializes the worker with the shared CAS handle it will use for all execution.
    async fn pre_start(
        &self,
        _myself: ActorRef<Self::Msg>,
        args: Self::Arguments,
    ) -> Result<Self::State, ActorProcessingErr> {
        Ok(args)
    }

    /// Executes one step request and replies with a merge-ready result bundle.
    async fn handle(
        &self,
        _myself: ActorRef<Self::Msg>,
        message: Self::Msg,
        state: &mut Self::State,
    ) -> Result<(), ActorProcessingErr> {
        match message {
            StepWorkerMessage::ExecuteStep(request, reply) => {
                let executor = StepWorkerExecutor { cas: state.clone() };
                let _ = reply.send(executor.execute_step(*request).await);
            }
        }
        Ok(())
    }
}

impl<C> StepWorkerExecutor<C>
where
    C: CasApi + Send + Sync + 'static,
{
    /// Executes one planned step request and returns the bundle needed for state merge.
    async fn execute_step(
        &self,
        request: StepExecutionRequest,
    ) -> Result<StepExecutionBundle, ConductorError> {
        let started_at = Instant::now();
        let tool = request.unified.tools.get(&request.step.tool).ok_or_else(|| {
            ConductorError::Workflow(format!(
                "workflow '{}' step '{}' references unknown tool '{}'; this should be caught during unification",
                request.workflow_name, request.step.id, request.step.tool
            ))
        })?;

        let mut template_file_writes = Vec::new();
        let resolved_inputs = self
            .resolve_inputs(
                request.unified.as_ref(),
                tool,
                &request.workflow_name,
                &request.step,
                request.step_outputs.as_ref(),
            )
            .await?;
        let resolved_process =
            self.resolve_process_execution(tool, &resolved_inputs, &mut template_file_writes)?;
        let metadata = Self::tool_spec_from_unified(tool);
        let instance_key = Self::derive_instance_key(
            &request.step.tool,
            &metadata,
            request.impure_timestamp,
            &resolved_inputs,
        )?;
        let output_specs = self.resolve_output_specs(
            tool,
            &request.step,
            &resolved_inputs,
            &mut template_file_writes,
        )?;
        for required_output_name in &request.required_output_names {
            if !output_specs.contains_key(required_output_name) {
                return Err(ConductorError::Workflow(format!(
                    "workflow '{}' step '{}' requires unknown output '{}'",
                    request.workflow_name, request.step.id, required_output_name
                )));
            }
        }
        let requested_output_names: Vec<String> =
            request.required_output_names.iter().cloned().collect();

        let existing_instance = request.state_snapshot.instances.get(&instance_key).cloned();
        let mut rematerialized = false;
        let mut needs_execution = existing_instance.is_none();
        if let Some(instance) = &existing_instance {
            for output_name in &request.required_output_names {
                let Some(output_ref) = instance.outputs.get(output_name) else {
                    needs_execution = true;
                    break;
                };
                if !self.cas.exists(output_ref.hash).await? {
                    needs_execution = true;
                    rematerialized = true;
                }
            }
        }

        let mut instance = if let Some(existing) = existing_instance {
            ToolCallInstance {
                tool_name: request.step.tool.clone(),
                metadata,
                impure_timestamp: request.impure_timestamp,
                inputs: resolved_inputs.clone(),
                outputs: existing.outputs,
            }
        } else {
            ToolCallInstance {
                tool_name: request.step.tool.clone(),
                metadata,
                impure_timestamp: request.impure_timestamp,
                inputs: resolved_inputs.clone(),
                outputs: BTreeMap::new(),
            }
        };

        if needs_execution {
            let execution_cwd_temp =
                self.create_execution_temp_cwd(&request.runtime_storage_dir)?;
            let execution_cwd = execution_cwd_temp.path();
            self.materialize_tool_content_map(&tool.tool_content_map, execution_cwd).await?;
            self.materialize_template_file_writes(&template_file_writes, execution_cwd)?;
            let capture = self
                .execute_tool(
                    &resolved_process,
                    &resolved_inputs,
                    execution_cwd,
                    &request.outermost_config_dir,
                )
                .await?;

            for (output_name, output_spec) in &output_specs {
                let payload = self.capture_output_payload(output_spec, &capture, execution_cwd)?;
                let hash = self.cas.put(payload).await?;
                instance.outputs.insert(
                    output_name.clone(),
                    OutputRef { hash, persistence: PersistenceFlags::default() },
                );
            }
        }

        let mut pending_unsaved_hashes = BTreeSet::new();
        for (output_name, output_spec) in &output_specs {
            let output_ref = instance.outputs.get_mut(output_name).ok_or_else(|| {
                ConductorError::Internal(format!(
                    "instance '{}' missing output '{}' after execution",
                    instance_key, output_name
                ))
            })?;
            let merged = merge_persistence_flags([output_ref.persistence, output_spec.persistence]);
            output_ref.persistence = merged;
            let output_exists = self.cas.exists(output_ref.hash).await?;
            if !output_exists {
                if request.required_output_names.contains(output_name) {
                    return Err(ConductorError::Internal(format!(
                        "required output '{}' for instance '{}' is missing from CAS after execution planning",
                        output_name, instance_key
                    )));
                }
                if !merged.save {
                    pending_unsaved_hashes.insert(output_ref.hash);
                }
                continue;
            }
            if merged.force_full {
                self.apply_force_full_hint(output_ref.hash).await?;
            }
            self.apply_reverse_diff_hints(output_ref.hash, &resolved_inputs).await?;
            if !merged.save {
                pending_unsaved_hashes.insert(output_ref.hash);
            }
        }

        Ok(StepExecutionBundle {
            step_id: request.step.id,
            tool_name: request.step.tool,
            worker_index: 0,
            instance_key,
            instance,
            requested_output_names,
            executed: needs_execution,
            rematerialized,
            pending_unsaved_hashes,
            elapsed_ms: started_at.elapsed().as_secs_f64() * 1000.0,
            fallback_used: false,
        })
    }

    /// Resolves workflow-step inputs into concrete byte payloads.
    ///
    /// Step inputs represent call-site input data for both executable and
    /// builtin tools.
    ///
    /// Executable tools additionally enforce declared input/default contracts,
    /// while builtin tools accept pass-through key/value bindings and delegate
    /// strict argument validation to builtin implementations.
    async fn resolve_inputs(
        &self,
        unified: &UnifiedNickelDocument,
        tool: &UnifiedToolSpec,
        workflow_name: &str,
        step: &WorkflowStepSpec,
        step_outputs: &StepOutputs,
    ) -> Result<BTreeMap<String, ResolvedInput>, ConductorError> {
        if matches!(tool.process, ProcessSpec::Builtin { .. }) {
            let mut passthrough = BTreeMap::new();
            for (input_name, binding) in &step.inputs {
                let InputBinding::String(binding_text) = binding else {
                    return Err(ConductorError::Workflow(format!(
                        "workflow '{workflow_name}' step '{}' input '{input_name}' has kind '{}' but builtin tool '{}' accepts only scalar string step inputs",
                        step.id,
                        binding.kind_name(),
                        step.tool,
                    )));
                };
                let input = self
                    .resolve_input_binding(unified, workflow_name, step, binding_text, step_outputs)
                    .await?;
                passthrough.insert(input_name.clone(), input);
            }
            return Ok(passthrough);
        }

        let mut resolved = BTreeMap::new();

        for input_name in step.inputs.keys() {
            if !tool.inputs.contains_key(input_name) {
                return Err(ConductorError::Workflow(format!(
                    "workflow '{workflow_name}' step '{}' provides undeclared input '{input_name}' for tool '{}'",
                    step.id, step.tool,
                )));
            }
        }

        for (input_name, input_spec) in &tool.inputs {
            if let Some(binding) = step.inputs.get(input_name) {
                let input = match (input_spec.kind, binding) {
                    (ToolInputKind::String, InputBinding::String(binding_text)) => {
                        self.resolve_input_binding(
                            unified,
                            workflow_name,
                            step,
                            binding_text,
                            step_outputs,
                        )
                        .await?
                    }
                    (ToolInputKind::StringList, InputBinding::StringList(binding_list)) => {
                        self.resolve_list_input_binding(
                            unified,
                            workflow_name,
                            step,
                            input_name,
                            binding_list,
                            step_outputs,
                        )
                        .await?
                    }
                    (ToolInputKind::String, InputBinding::StringList(_)) => {
                        return Err(ConductorError::Workflow(format!(
                            "workflow '{workflow_name}' step '{}' input '{input_name}' expects kind 'string' for tool '{}', but received 'string_list'",
                            step.id, step.tool,
                        )));
                    }
                    (ToolInputKind::StringList, InputBinding::String(_)) => {
                        return Err(ConductorError::Workflow(format!(
                            "workflow '{workflow_name}' step '{}' input '{input_name}' expects kind 'string_list' for tool '{}', but received 'string'",
                            step.id, step.tool,
                        )));
                    }
                };
                resolved.insert(input_name.clone(), input);
                continue;
            }

            if let Some(default_binding) = tool.default_inputs.get(input_name) {
                let resolved_input = match (input_spec.kind, default_binding) {
                    (ToolInputKind::String, InputBinding::String(binding_text)) => {
                        self.resolve_input_binding(
                            unified,
                            workflow_name,
                            step,
                            binding_text,
                            step_outputs,
                        )
                        .await?
                    }
                    (ToolInputKind::StringList, InputBinding::StringList(binding_list)) => {
                        self.resolve_list_input_binding(
                            unified,
                            workflow_name,
                            step,
                            input_name,
                            binding_list,
                            step_outputs,
                        )
                        .await?
                    }
                    (ToolInputKind::String, InputBinding::StringList(_)) => {
                        return Err(ConductorError::Workflow(format!(
                            "workflow '{workflow_name}' step '{}' input default '{input_name}' expects kind 'string' for tool '{}', but tool_configs default provides 'string_list'",
                            step.id, step.tool,
                        )));
                    }
                    (ToolInputKind::StringList, InputBinding::String(_)) => {
                        return Err(ConductorError::Workflow(format!(
                            "workflow '{workflow_name}' step '{}' input default '{input_name}' expects kind 'string_list' for tool '{}', but tool_configs default provides 'string'",
                            step.id, step.tool,
                        )));
                    }
                };

                resolved.insert(input_name.clone(), resolved_input);
                continue;
            }

            return Err(ConductorError::Workflow(format!(
                "workflow '{workflow_name}' step '{}' is missing required input '{input_name}' for tool '{}'",
                step.id, step.tool,
            )));
        }

        Ok(resolved)
    }

    /// Resolves one string input binding into concrete bytes plus one CAS hash
    /// identity persisted for orchestration state snapshots.
    ///
    /// Input bindings support `${...}` interpolation mixed with literal text.
    /// Supported expression forms are:
    /// - `${external_data.<hash>}`,
    /// - `${step_output.<step_id>.<output_name>}`,
    ///
    /// Every resolved input payload is persisted to CAS and represented by the
    /// resulting hash identity in persisted orchestration state.
    async fn resolve_input_binding(
        &self,
        unified: &UnifiedNickelDocument,
        workflow_name: &str,
        step: &WorkflowStepSpec,
        binding: &str,
        step_outputs: &StepOutputs,
    ) -> Result<ResolvedInput, ConductorError> {
        let plain_content = self
            .resolve_input_binding_plain_content(
                unified,
                workflow_name,
                step,
                binding,
                step_outputs,
            )
            .await?;
        self.persist_resolved_input(plain_content).await
    }

    /// Resolves one string input binding into concrete payload bytes.
    async fn resolve_input_binding_plain_content(
        &self,
        unified: &UnifiedNickelDocument,
        workflow_name: &str,
        step: &WorkflowStepSpec,
        binding: &str,
        step_outputs: &StepOutputs,
    ) -> Result<Vec<u8>, ConductorError> {
        let parsed_segments = parse_input_binding(binding).map_err(|err| {
            ConductorError::Workflow(format!(
                "workflow '{workflow_name}' step '{}' has invalid input binding '{binding}': {err}",
                step.id
            ))
        })?;

        let mut plain_content = Vec::new();

        for segment in parsed_segments {
            match segment {
                ParsedInputBindingSegment::ExternalData { hash } => {
                    if !unified.external_data.contains_key(&hash) {
                        return Err(ConductorError::Workflow(format!(
                            "workflow '{workflow_name}' step '{}' references unknown external data hash '{hash}'",
                            step.id
                        )));
                    }
                    let bytes = self.cas.get(hash).await?;
                    plain_content.extend_from_slice(bytes.as_ref());
                }
                ParsedInputBindingSegment::Literal(content) => {
                    if !content.is_empty() {
                        plain_content.extend_from_slice(content.as_bytes());
                    }
                }
                ParsedInputBindingSegment::StepOutput { step_id, output } => {
                    let producer = step_outputs.get(step_id).ok_or_else(|| {
                        ConductorError::Workflow(format!(
                            "workflow '{workflow_name}' step '{}' references output '{}' from step '{}' before it is available",
                            step.id, output, step_id
                        ))
                    })?;
                    let output_hash = producer.get(output).copied().ok_or_else(|| {
                        ConductorError::Workflow(format!(
                            "workflow '{workflow_name}' step '{}' references missing output '{}' on step '{}'",
                            step.id, output, step_id
                        ))
                    })?;
                    let bytes = match self.cas.get(output_hash).await {
                        Ok(bytes) => bytes,
                        Err(source) if Self::is_cas_corruption_read_error(&source) => {
                            return Err(ConductorError::CorruptWorkflowOutput(Box::new(
                                CorruptWorkflowOutputContext {
                                    workflow_name: workflow_name.to_string(),
                                    consumer_step_id: step.id.clone(),
                                    producer_step_id: step_id.to_string(),
                                    output_name: output.to_string(),
                                    output_hash,
                                    detail: source.to_string(),
                                },
                            )));
                        }
                        Err(source) => return Err(ConductorError::Cas(source)),
                    };
                    plain_content.extend_from_slice(bytes.as_ref());
                }
            }
        }

        Ok(plain_content)
    }

    /// Resolves one list-of-strings input binding into deterministic runtime
    /// list payload plus CAS identity.
    async fn resolve_list_input_binding(
        &self,
        unified: &UnifiedNickelDocument,
        workflow_name: &str,
        step: &WorkflowStepSpec,
        input_name: &str,
        binding_list: &[String],
        step_outputs: &StepOutputs,
    ) -> Result<ResolvedInput, ConductorError> {
        let mut resolved_values = Vec::with_capacity(binding_list.len());
        for (item_index, binding_item) in binding_list.iter().enumerate() {
            let plain_content = self
                .resolve_input_binding_plain_content(
                    unified,
                    workflow_name,
                    step,
                    binding_item,
                    step_outputs,
                )
                .await
                .map_err(|error| match error {
                    ConductorError::Workflow(message) => ConductorError::Workflow(format!(
                        "{message} (while resolving list item {item_index} for input '{input_name}')"
                    )),
                    other => other,
                })?;
            resolved_values.push(String::from_utf8_lossy(&plain_content).to_string());
        }

        self.persist_resolved_list_input(resolved_values).await
    }

    /// Persists one resolved input payload into CAS and returns the runtime
    /// input record carrying both hash identity and in-memory bytes.
    async fn persist_resolved_input(
        &self,
        plain_content: Vec<u8>,
    ) -> Result<ResolvedInput, ConductorError> {
        let hash = self.cas.put(plain_content.clone()).await?;
        Ok(ResolvedInput { hash, plain_content, string_list: None })
    }

    /// Persists one resolved list input payload and preserves list values for
    /// runtime command-argument unpack rendering.
    async fn persist_resolved_list_input(
        &self,
        string_list: Vec<String>,
    ) -> Result<ResolvedInput, ConductorError> {
        let plain_content = serde_json::to_vec(&string_list)
            .map_err(|err| ConductorError::Serialization(err.to_string()))?;
        let hash = self.cas.put(plain_content.clone()).await?;
        Ok(ResolvedInput { hash, plain_content, string_list: Some(string_list) })
    }

    /// Resolves the process definition that should be executed for one step.
    fn resolve_process_execution(
        &self,
        tool: &UnifiedToolSpec,
        inputs: &BTreeMap<String, ResolvedInput>,
        pending_file_writes: &mut Vec<TemplateFileWrite>,
    ) -> Result<ResolvedProcessExecution, ConductorError> {
        match &tool.process {
            ProcessSpec::Executable { command, env_vars, success_codes } => {
                let rendered_command =
                    self.render_template_command(command, inputs, pending_file_writes)?;
                let Some(executable) = rendered_command.first() else {
                    return Err(ConductorError::Workflow(
                        "executable process command resolved to empty vector".to_string(),
                    ));
                };
                Ok(ResolvedProcessExecution::Executable {
                    executable: executable.clone(),
                    args: rendered_command.into_iter().skip(1).collect(),
                    env_vars: self.render_templates(env_vars, inputs, pending_file_writes)?,
                    success_codes: Self::normalize_success_codes(success_codes),
                })
            }
            ProcessSpec::Builtin { name, version, args } => {
                let mut rendered_args = self.render_templates(args, inputs, pending_file_writes)?;
                for (input_name, resolved_input) in inputs {
                    rendered_args.insert(
                        input_name.clone(),
                        String::from_utf8_lossy(&resolved_input.plain_content).to_string(),
                    );
                }

                Ok(ResolvedProcessExecution::Builtin {
                    name: name.clone(),
                    version: version.clone(),
                    args: rendered_args,
                })
            }
        }
    }

    /// Converts one unified runtime tool shape into persisted-state metadata.
    ///
    /// Executable tools keep the full reusable-tool metadata shape because
    /// executable identity depends on declared input/output and process
    /// contracts.
    ///
    /// Builtin tools intentionally keep only builtin identity
    /// (`kind`/`name`/`version`) so orchestration-state snapshots and
    /// deduplication keys do not overfit call-site defaults that builtin crates
    /// do not use for dispatch identity.
    fn tool_spec_from_unified(tool: &UnifiedToolSpec) -> ToolSpec {
        let kind = match &tool.process {
            ProcessSpec::Executable { command, env_vars, success_codes } => {
                ToolKindSpec::Executable {
                    command: command.clone(),
                    env_vars: env_vars.clone(),
                    success_codes: success_codes.clone(),
                }
            }
            ProcessSpec::Builtin { name, version, .. } => {
                ToolKindSpec::Builtin { name: name.clone(), version: version.clone() }
            }
        };

        match kind {
            ToolKindSpec::Executable { command, env_vars, success_codes } => ToolSpec {
                is_impure: tool.is_impure,
                inputs: tool.inputs.clone(),
                kind: ToolKindSpec::Executable { command, env_vars, success_codes },
                outputs: tool.outputs.clone(),
            },
            ToolKindSpec::Builtin { name, version } => ToolSpec {
                is_impure: false,
                inputs: BTreeMap::new(),
                kind: ToolKindSpec::Builtin { name, version },
                outputs: BTreeMap::new(),
            },
        }
    }

    /// Creates one ad hoc sandbox directory only when a step actually needs to
    /// execute.
    ///
    /// Sandboxes are always nested under `<runtime_storage_dir>/tmp`, where
    /// `runtime_storage_dir` is resolved from
    /// `RunWorkflowOptions.runtime_storage_paths.conductor_dir`.
    fn create_execution_temp_cwd(
        &self,
        runtime_storage_dir: &Path,
    ) -> Result<tempfile::TempDir, ConductorError> {
        let scratch_root = runtime_storage_dir.join("tmp");
        std::fs::create_dir_all(&scratch_root).map_err(|source| ConductorError::Io {
            operation: "creating tool sandbox root directory".to_string(),
            path: scratch_root.clone(),
            source,
        })?;
        tempfile::Builder::new().prefix("run-").tempdir_in(&scratch_root).map_err(|source| {
            ConductorError::Io {
                operation: "creating tool sandbox working directory".to_string(),
                path: scratch_root,
                source,
            }
        })
    }

    /// Normalizes one tool-relative path and rejects absolute or escaping paths.
    fn normalized_relative_tool_path(
        &self,
        relative_path: &str,
        context: &str,
    ) -> Result<std::path::PathBuf, ConductorError> {
        if relative_path.trim().is_empty() {
            return Err(ConductorError::Workflow(format!("{context} path must be non-empty")));
        }
        let parsed = Path::new(relative_path);
        if parsed.is_absolute() {
            return Err(ConductorError::Workflow(format!(
                "{context} path '{relative_path}' must be relative"
            )));
        }
        let mut normalized = std::path::PathBuf::new();
        for component in parsed.components() {
            match component {
                Component::Normal(part) => normalized.push(part),
                Component::CurDir => {}
                Component::ParentDir | Component::RootDir | Component::Prefix(_) => {
                    return Err(ConductorError::Workflow(format!(
                        "{context} path '{relative_path}' must not escape the tool sandbox"
                    )));
                }
            }
        }
        if normalized.as_os_str().is_empty() {
            return Err(ConductorError::Workflow(format!(
                "{context} path '{relative_path}' must contain a concrete file name"
            )));
        }
        Ok(normalized)
    }

    /// Resolves one validated tool-relative path against a specific sandbox root.
    fn resolve_tool_relative_path(
        &self,
        relative_path: &str,
        tool_cwd: &Path,
        context: &str,
    ) -> Result<std::path::PathBuf, ConductorError> {
        let normalized = self.normalized_relative_tool_path(relative_path, context)?;
        Ok(tool_cwd.join(normalized))
    }

    /// Resolves output capture specifications after template rendering and policy overrides.
    fn resolve_output_specs(
        &self,
        tool: &UnifiedToolSpec,
        step: &WorkflowStepSpec,
        inputs: &BTreeMap<String, ResolvedInput>,
        pending_file_writes: &mut Vec<TemplateFileWrite>,
    ) -> Result<BTreeMap<String, ResolvedOutputSpec>, ConductorError> {
        let mut resolved: BTreeMap<String, ResolvedOutputSpec> = tool
            .outputs
            .iter()
            .map(|(name, output_spec)| {
                let capture = match &output_spec.capture {
                    OutputCaptureSpec::Stdout {} => ResolvedOutputCapture::Stdout,
                    OutputCaptureSpec::Stderr {} => ResolvedOutputCapture::Stderr,
                    OutputCaptureSpec::ProcessCode {} => ResolvedOutputCapture::ProcessCode,
                    OutputCaptureSpec::File { path } => {
                        let rendered =
                            self.render_template_value(path, inputs, pending_file_writes)?;
                        let relative_path =
                            self.normalized_relative_tool_path(&rendered, "output capture")?;
                        ResolvedOutputCapture::File { relative_path }
                    }
                    OutputCaptureSpec::Folder { path, include_topmost_folder } => {
                        let rendered =
                            self.render_template_value(path, inputs, pending_file_writes)?;
                        let relative_path =
                            self.normalized_relative_tool_path(&rendered, "output capture")?;
                        ResolvedOutputCapture::FolderAsZip {
                            relative_path,
                            include_topmost_folder: *include_topmost_folder,
                        }
                    }
                };
                Ok((
                    name.clone(),
                    ResolvedOutputSpec { capture, persistence: PersistenceFlags::default() },
                ))
            })
            .collect::<Result<_, ConductorError>>()?;

        if resolved.is_empty() {
            return Err(ConductorError::Workflow(format!(
                "tool '{}' must declare at least one output capture",
                step.tool
            )));
        }
        for (name, override_policy) in &step.outputs {
            let Some(existing) = resolved.get_mut(name) else {
                return Err(ConductorError::Workflow(format!(
                    "workflow step '{}' overrides unknown output '{name}' on tool '{}'",
                    step.id, step.tool
                )));
            };
            existing.persistence = override_policy.resolve(existing.persistence);
        }
        Ok(resolved)
    }

    /// Executes either an external executable or one builtin implementation.
    async fn execute_tool(
        &self,
        process: &ResolvedProcessExecution,
        resolved_inputs: &BTreeMap<String, ResolvedInput>,
        tool_cwd: &Path,
        outermost_config_dir: &Path,
    ) -> Result<ToolExecutionCapture, ConductorError> {
        match process {
            ResolvedProcessExecution::Executable { executable, args, env_vars, success_codes } => {
                self.execute_executable_tool(executable, args, env_vars, success_codes, tool_cwd)
                    .await
            }
            ResolvedProcessExecution::Builtin { name, version, args } => {
                self.execute_builtin_tool(
                    name,
                    version,
                    args,
                    resolved_inputs,
                    tool_cwd,
                    outermost_config_dir,
                )
                .await
            }
        }
    }

    /// Materializes deferred template file writes into one execution sandbox.
    fn materialize_template_file_writes(
        &self,
        pending_file_writes: &[TemplateFileWrite],
        tool_cwd: &Path,
    ) -> Result<(), ConductorError> {
        for file_write in pending_file_writes {
            let target_path = tool_cwd.join(&file_write.relative_path);
            if let Some(parent) = target_path.parent() {
                std::fs::create_dir_all(parent).map_err(|source| ConductorError::Io {
                    operation: "creating template materialization parent directories".to_string(),
                    path: parent.to_path_buf(),
                    source,
                })?;
            }
            std::fs::write(&target_path, &file_write.plain_content).map_err(|source| {
                ConductorError::Io {
                    operation: "materializing deferred template input file".to_string(),
                    path: target_path.clone(),
                    source,
                }
            })?;
        }
        Ok(())
    }

    /// Spawns one executable process inside the step sandbox and captures its streams.
    async fn execute_executable_tool(
        &self,
        executable_name: &str,
        resolved_args: &[String],
        env_vars: &BTreeMap<String, String>,
        success_codes: &BTreeSet<i32>,
        tool_cwd: &Path,
    ) -> Result<ToolExecutionCapture, ConductorError> {
        if executable_name.trim().is_empty() {
            return Err(ConductorError::Workflow(
                "executable command[0] must be non-empty for kind='executable'".to_string(),
            ));
        }

        let executable_path =
            self.resolve_tool_relative_path(executable_name, tool_cwd, "tool process executable")?;

        #[cfg(unix)]
        {
            use std::os::unix::fs::PermissionsExt;

            let metadata =
                std::fs::metadata(&executable_path).map_err(|source| ConductorError::Io {
                    operation: "reading executable metadata before process spawn".to_string(),
                    path: executable_path.clone(),
                    source,
                })?;
            let mut permissions = metadata.permissions();
            let current_mode = permissions.mode();
            let desired_mode = current_mode | 0o111;
            if desired_mode != current_mode {
                permissions.set_mode(desired_mode);
                std::fs::set_permissions(&executable_path, permissions).map_err(|source| {
                    ConductorError::Io {
                        operation: "marking executable command[0] path as executable".to_string(),
                        path: executable_path.clone(),
                        source,
                    }
                })?;
            }
        }

        let mut command = std::process::Command::new(&executable_path);
        for arg in resolved_args {
            command.arg(arg);
        }
        command.current_dir(tool_cwd);
        command.envs(env_vars);
        let output = command.output().map_err(|source| ConductorError::Io {
            operation: format!("executing executable process '{executable_name}'"),
            path: executable_path.clone(),
            source,
        })?;

        let Some(process_code) = output.status.code() else {
            return Err(ConductorError::Workflow(format!(
                "process '{executable_name}' terminated without an exit code"
            )));
        };

        if !Self::is_success_exit_code(process_code, success_codes) {
            let stderr = Self::format_process_failure_stderr(&output.stderr);
            return Err(ConductorError::Workflow(format!(
                "process '{executable_name}' exited with code {process_code}, expected one of {:?}: {}",
                success_codes, stderr
            )));
        }

        Ok(ToolExecutionCapture { stdout: output.stdout, stderr: output.stderr, process_code })
    }

    /// Materializes per-tool `content_map` entries from CAS into the sandbox.
    ///
    /// Key semantics:
    /// - keys ending with `/` or `\\` denote directories and require ZIP
    ///   payload hashes,
    /// - key `./` (or `.\\`) denotes sandbox-root directory unpack,
    /// - all other keys denote regular files and write raw bytes.
    ///
    /// Every key is normalized and validated as a sandbox-relative path before
    /// any write or unpack operation occurs. Runtime preflights all entries
    /// and rejects conflicts where two entries would materialize the same file
    /// path.
    async fn materialize_tool_content_map(
        &self,
        tool_content_map: &BTreeMap<String, Hash>,
        tool_cwd: &Path,
    ) -> Result<(), ConductorError> {
        let plans = self.plan_tool_content_map_materialization(tool_content_map).await?;

        for planned in plans {
            match planned.payload {
                PlannedToolContentPayload::File { relative_path, plain_content } => {
                    let target_path = tool_cwd.join(relative_path);
                    if let Some(parent) = target_path.parent() {
                        std::fs::create_dir_all(parent).map_err(|source| ConductorError::Io {
                            operation: "creating tool-content parent directories".to_string(),
                            path: parent.to_path_buf(),
                            source,
                        })?;
                    }

                    std::fs::write(&target_path, plain_content).map_err(|source| {
                        ConductorError::Io {
                            operation: "materializing tool content from CAS".to_string(),
                            path: target_path,
                            source,
                        }
                    })?;
                }
                PlannedToolContentPayload::DirectoryFromZip { relative_dir, zip_content } => {
                    self.materialize_tool_content_directory_from_zip(
                        &planned.raw_relative_path,
                        &relative_dir,
                        &zip_content,
                        tool_cwd,
                    )
                    .await?;
                }
            }
        }

        Ok(())
    }

    /// Resolves and validates all `content_map` entries before writing anything.
    ///
    /// This preflight phase enforces collision safety: two separate
    /// `content_map` entries are not allowed to materialize the same target
    /// file path in the execution sandbox.
    async fn plan_tool_content_map_materialization(
        &self,
        tool_content_map: &BTreeMap<String, Hash>,
    ) -> Result<Vec<PlannedToolContentMaterialization>, ConductorError> {
        let mut planned = Vec::with_capacity(tool_content_map.len());

        for (raw_relative_path, hash) in tool_content_map {
            let payload_bytes = self.cas.get(*hash).await?.to_vec();
            match self.classify_tool_content_map_entry(raw_relative_path)? {
                ToolContentMapEntry::File { relative_path } => {
                    planned.push(PlannedToolContentMaterialization {
                        raw_relative_path: raw_relative_path.clone(),
                        payload: PlannedToolContentPayload::File {
                            relative_path: relative_path.clone(),
                            plain_content: payload_bytes,
                        },
                        claimed_relative_files: BTreeSet::from([relative_path]),
                    });
                }
                ToolContentMapEntry::DirectoryFromZip { relative_dir } => {
                    let claimed_relative_files = self
                        .list_tool_content_directory_target_files(
                            raw_relative_path,
                            &relative_dir,
                            &payload_bytes,
                        )
                        .await?;
                    planned.push(PlannedToolContentMaterialization {
                        raw_relative_path: raw_relative_path.clone(),
                        payload: PlannedToolContentPayload::DirectoryFromZip {
                            relative_dir,
                            zip_content: payload_bytes,
                        },
                        claimed_relative_files,
                    });
                }
            }
        }

        let mut claim_owners: BTreeMap<std::path::PathBuf, String> = BTreeMap::new();
        for entry in &planned {
            for claimed_file in &entry.claimed_relative_files {
                if let Some(previous_owner) =
                    claim_owners.insert(claimed_file.clone(), entry.raw_relative_path.clone())
                {
                    return Err(ConductorError::Workflow(format!(
                        "tool content map entries '{}' and '{}' both materialize '{}' and would overwrite each other",
                        previous_owner,
                        entry.raw_relative_path,
                        claimed_file.to_string_lossy()
                    )));
                }
            }
        }

        Ok(planned)
    }

    /// Lists all concrete file paths one directory-form entry would materialize.
    ///
    /// This uses the same archive unpack path as runtime execution to ensure
    /// collision checks reflect real unpack behavior.
    async fn list_tool_content_directory_target_files(
        &self,
        raw_relative_path: &str,
        relative_dir: &Path,
        zip_content: &[u8],
    ) -> Result<BTreeSet<std::path::PathBuf>, ConductorError> {
        let inspect_workspace = tempfile::tempdir().map_err(|source| ConductorError::Io {
            operation: "creating temporary tool-content ZIP inspection workspace".to_string(),
            path: std::env::temp_dir(),
            source,
        })?;
        let unpack_dir = inspect_workspace.path().join("unpacked");

        mediapm_conductor_builtin_archive::unpack_zip_bytes_to_directory(zip_content, &unpack_dir)
        .map_err(|err| {
            ConductorError::Workflow(format!(
                "tool content map directory key '{raw_relative_path}' expects ZIP payload, but unpack failed: {err}"
            ))
        })?;

        let mut collected_relative_files = BTreeSet::new();
        self.collect_relative_files_recursive(
            &unpack_dir,
            &unpack_dir,
            &mut collected_relative_files,
        )?;

        Ok(collected_relative_files
            .into_iter()
            .map(|relative_file| {
                if relative_dir.as_os_str().is_empty() {
                    relative_file
                } else {
                    relative_dir.join(relative_file)
                }
            })
            .collect())
    }

    /// Collects all regular files under one directory as sandbox-relative paths.
    fn collect_relative_files_recursive(
        &self,
        root_dir: &Path,
        scan_dir: &Path,
        out: &mut BTreeSet<std::path::PathBuf>,
    ) -> Result<(), ConductorError> {
        if !scan_dir.exists() {
            return Ok(());
        }

        let entries = std::fs::read_dir(scan_dir).map_err(|source| ConductorError::Io {
            operation: "reading unpacked tool-content inspection directory".to_string(),
            path: scan_dir.to_path_buf(),
            source,
        })?;

        for entry in entries {
            let entry = entry.map_err(|source| ConductorError::Io {
                operation: "iterating unpacked tool-content inspection directory".to_string(),
                path: scan_dir.to_path_buf(),
                source,
            })?;
            let path = entry.path();
            let file_type = entry.file_type().map_err(|source| ConductorError::Io {
                operation: "reading unpacked tool-content entry type".to_string(),
                path: path.clone(),
                source,
            })?;

            if file_type.is_dir() {
                self.collect_relative_files_recursive(root_dir, &path, out)?;
                continue;
            }

            if file_type.is_file() {
                let relative = path.strip_prefix(root_dir).map_err(|err| {
                    ConductorError::Internal(format!(
                        "failed deriving relative path for unpacked tool-content file '{}': {err}",
                        path.display()
                    ))
                })?;
                out.insert(relative.to_path_buf());
            }
        }

        Ok(())
    }

    /// Classifies one `content_map` key as file or ZIP-backed directory materialization.
    ///
    /// Semantics:
    /// - keys ending with `/` or `\\` are directory targets and therefore
    ///   expect ZIP payload bytes,
    /// - key `./` (or `.\\`) means directory-target unpack at sandbox root,
    /// - keys without trailing slash/backslash are regular file targets.
    ///
    /// Every accepted key is normalized and validated as a sandbox-relative
    /// path; absolute or escaping paths are rejected.
    fn classify_tool_content_map_entry(
        &self,
        raw_relative_path: &str,
    ) -> Result<ToolContentMapEntry, ConductorError> {
        if raw_relative_path.ends_with('/') || raw_relative_path.ends_with('\\') {
            let trimmed = raw_relative_path.trim_end_matches(['/', '\\']);
            if trimmed == "." {
                return Ok(ToolContentMapEntry::DirectoryFromZip {
                    relative_dir: std::path::PathBuf::new(),
                });
            }
            if trimmed.trim().is_empty() {
                return Err(ConductorError::Workflow(format!(
                    "tool content map directory key '{raw_relative_path}' must contain at least one path component before trailing slash"
                )));
            }
            let relative_dir = self.normalized_relative_tool_path(trimmed, "tool content map")?;
            return Ok(ToolContentMapEntry::DirectoryFromZip { relative_dir });
        }

        let relative_path =
            self.normalized_relative_tool_path(raw_relative_path, "tool content map")?;
        Ok(ToolContentMapEntry::File { relative_path })
    }

    /// Materializes one directory-form `content_map` entry by unpacking ZIP bytes.
    ///
    /// The `raw_relative_path` must be a directory key ending in `/` or `\\`.
    /// `relative_dir` is already normalized and guaranteed to stay inside
    /// `tool_cwd`. The referenced CAS payload must be a ZIP archive; invalid
    /// archives fail fast with an actionable workflow error.
    async fn materialize_tool_content_directory_from_zip(
        &self,
        raw_relative_path: &str,
        relative_dir: &Path,
        zip_content: &[u8],
        tool_cwd: &Path,
    ) -> Result<(), ConductorError> {
        let target_dir = tool_cwd.join(relative_dir);
        std::fs::create_dir_all(&target_dir).map_err(|source| ConductorError::Io {
            operation: "creating tool-content destination directory".to_string(),
            path: target_dir.clone(),
            source,
        })?;

        mediapm_conductor_builtin_archive::unpack_zip_bytes_to_directory(zip_content, &target_dir)
        .map_err(|err| {
            ConductorError::Workflow(format!(
                "tool content map directory key '{raw_relative_path}' expects ZIP payload, but unpack failed: {err}"
            ))
        })?;

        Ok(())
    }

    /// Executes one builtin implementation and returns synthetic stdout/stderr.
    /// Executes one builtin tool by delegating to standalone builtin crates.
    ///
    /// Builtin runtime logic lives under `src/conductor-builtins/*`; the
    /// conductor worker only translates orchestration inputs and dispatches.
    ///
    /// Builtin crates expose one uniform contract shape:
    /// - API args are `BTreeMap<String, String>`,
    /// - API may additionally consume raw input payload bytes for
    ///   content-oriented operations,
    /// - pure API success may return raw bytes or string-map payloads,
    /// - CLI uses standard Rust flags/options while keeping values string-only.
    /// - undeclared keys, missing required keys, and invalid argument
    ///   combinations must fail instead of being silently ignored.
    ///
    /// CLI ergonomics may optionally define one default option key so one value
    /// can be provided without spelling a key, but explicit keyed input remains
    /// supported and maps to the same API key.
    async fn execute_builtin_tool(
        &self,
        builtin_name: &str,
        builtin_version: &str,
        resolved_args: &BTreeMap<String, String>,
        resolved_inputs: &BTreeMap<String, ResolvedInput>,
        _tool_cwd: &Path,
        outermost_config_dir: &Path,
    ) -> Result<ToolExecutionCapture, ConductorError> {
        let binary_inputs =
            Self::select_builtin_binary_inputs(builtin_name, builtin_version, resolved_inputs);

        match (builtin_name, builtin_version) {
            (
                mediapm_conductor_builtin_echo::TOOL_NAME,
                mediapm_conductor_builtin_echo::TOOL_VERSION,
            ) => {
                let response =
                    mediapm_conductor_builtin_echo::execute_echo(resolved_args, &BTreeMap::new())
                        .map_err(|err| {
                        ConductorError::Workflow(format!(
                            "builtin '{}@{}' failed: {err}",
                            builtin_name, builtin_version
                        ))
                    })?;

                Ok(ToolExecutionCapture {
                    stdout: response.stdout.into_bytes(),
                    stderr: response.stderr.into_bytes(),
                    process_code: 0,
                })
            }
            (
                mediapm_conductor_builtin_fs::TOOL_NAME,
                mediapm_conductor_builtin_fs::TOOL_VERSION,
            ) => {
                mediapm_conductor_builtin_fs::execute_string_map(
                    outermost_config_dir,
                    resolved_args,
                    &BTreeMap::new(),
                )
                .map_err(|err| {
                    ConductorError::Workflow(format!(
                        "builtin '{}@{}' failed: {err}",
                        builtin_name, builtin_version
                    ))
                })?;

                Ok(ToolExecutionCapture { stdout: Vec::new(), stderr: Vec::new(), process_code: 0 })
            }
            (
                mediapm_conductor_builtin_import::TOOL_NAME,
                mediapm_conductor_builtin_import::TOOL_VERSION,
            ) => {
                let mut resolved_hash_payloads = BTreeMap::new();
                if matches!(resolved_args.get("kind").map(String::as_str), Some("cas_hash")) {
                    let hash_text = resolved_args.get("hash").ok_or_else(|| {
                        ConductorError::Workflow(
                            "builtin 'import@1.0.0' kind='cas_hash' requires 'hash'".to_string(),
                        )
                    })?;
                    let hash = hash_text.parse::<Hash>().map_err(|_| {
                        ConductorError::Workflow(format!(
                            "builtin 'import@1.0.0' kind='cas_hash' received invalid hash '{hash_text}'"
                        ))
                    })?;
                    let bytes = self.cas.get(hash).await?;
                    resolved_hash_payloads.insert(hash_text.clone(), bytes.as_ref().to_vec());
                }

                let payload = mediapm_conductor_builtin_import::execute_content_map_with_hash_resolver(
                    outermost_config_dir,
                    resolved_args,
                    &BTreeMap::new(),
                    |hash_text| {
                        resolved_hash_payloads.get(hash_text).cloned().ok_or_else(|| {
                            format!(
                                "import kind='cas_hash' hash '{hash_text}' was not preloaded from CAS"
                            )
                        })
                    },
                )
                .map_err(|err| {
                    ConductorError::Workflow(format!(
                        "builtin '{}@{}' failed: {err}",
                        builtin_name, builtin_version
                    ))
                })?;

                Ok(ToolExecutionCapture { stdout: payload, stderr: Vec::new(), process_code: 0 })
            }
            (
                mediapm_conductor_builtin_archive::TOOL_NAME,
                mediapm_conductor_builtin_archive::TOOL_VERSION,
            ) => {
                let payload = mediapm_conductor_builtin_archive::execute_content_map(
                    resolved_args,
                    &binary_inputs,
                )
                .map_err(|err| {
                    ConductorError::Workflow(format!(
                        "builtin '{}@{}' failed: {err}",
                        builtin_name, builtin_version
                    ))
                })?;

                Ok(ToolExecutionCapture { stdout: payload, stderr: Vec::new(), process_code: 0 })
            }
            (
                mediapm_conductor_builtin_export::TOOL_NAME,
                mediapm_conductor_builtin_export::TOOL_VERSION,
            ) => {
                let response = mediapm_conductor_builtin_export::execute_string_map(
                    outermost_config_dir,
                    resolved_args,
                    &binary_inputs,
                )
                .map_err(|err| {
                    ConductorError::Workflow(format!(
                        "builtin '{}@{}' failed: {err}",
                        builtin_name, builtin_version
                    ))
                })?;

                let payload = serde_json::to_vec(&response)
                    .map_err(|err| ConductorError::Serialization(err.to_string()))?;

                Ok(ToolExecutionCapture { stdout: payload, stderr: Vec::new(), process_code: 0 })
            }
            _ => Err(ConductorError::Workflow(format!(
                "unsupported builtin tool '{}@{}'",
                builtin_name, builtin_version
            ))),
        }
    }

    /// Selects binary-input keys that each builtin contract accepts.
    ///
    /// Builtin step inputs are mirrored into string args for deterministic
    /// instance-key derivation and explicit arg transport.
    /// Some builtins additionally accept raw binary payload inputs for
    /// content-oriented fields. This helper filters `resolved_inputs` so we do
    /// not pass structural keys (for example `kind` or `path`) as binary inputs
    /// to builtins that reject undeclared input keys.
    fn select_builtin_binary_inputs(
        builtin_name: &str,
        builtin_version: &str,
        resolved_inputs: &BTreeMap<String, ResolvedInput>,
    ) -> BTreeMap<String, Vec<u8>> {
        let accepted_keys: &[&str] = match (builtin_name, builtin_version) {
            (
                mediapm_conductor_builtin_archive::TOOL_NAME,
                mediapm_conductor_builtin_archive::TOOL_VERSION,
            ) => &["content", "archive"],
            (
                mediapm_conductor_builtin_export::TOOL_NAME,
                mediapm_conductor_builtin_export::TOOL_VERSION,
            ) => &["content"],
            _ => &[],
        };

        resolved_inputs
            .iter()
            .filter(|(key, _)| accepted_keys.contains(&key.as_str()))
            .map(|(key, input)| (key.clone(), input.plain_content.clone()))
            .collect()
    }

    /// Captures one declared output payload from the execution results.
    fn capture_output_payload(
        &self,
        output_spec: &ResolvedOutputSpec,
        capture: &ToolExecutionCapture,
        tool_cwd: &Path,
    ) -> Result<Vec<u8>, ConductorError> {
        match &output_spec.capture {
            ResolvedOutputCapture::Stdout => Ok(capture.stdout.clone()),
            ResolvedOutputCapture::Stderr => Ok(capture.stderr.clone()),
            ResolvedOutputCapture::ProcessCode => Ok(capture.process_code.to_string().into_bytes()),
            ResolvedOutputCapture::File { relative_path } => {
                let path = tool_cwd.join(relative_path);
                std::fs::read(&path).map_err(|source| ConductorError::Io {
                    operation: "capturing declared file output".to_string(),
                    path,
                    source,
                })
            }
            ResolvedOutputCapture::FolderAsZip { relative_path, include_topmost_folder } => {
                self.capture_folder_output_as_zip(relative_path, *include_topmost_folder, tool_cwd)
            }
        }
    }

    /// Captures one directory output by packing it into a ZIP payload.
    ///
    /// The implementation delegates packing to the builtin archive/zip crate,
    /// using stored (no-compression) ZIP entries to preserve exact payload
    /// bytes and avoid compression nondeterminism.
    fn capture_folder_output_as_zip(
        &self,
        relative_path: &std::path::Path,
        include_topmost_folder: bool,
        tool_cwd: &Path,
    ) -> Result<Vec<u8>, ConductorError> {
        let folder_path = tool_cwd.join(relative_path);
        mediapm_conductor_builtin_archive::pack_directory_to_uncompressed_zip_bytes(
            &folder_path,
            include_topmost_folder,
        )
        .map_err(|err| {
            ConductorError::Workflow(format!(
                "capturing declared folder output '{}' as ZIP failed: {err}",
                relative_path.to_string_lossy()
            ))
        })
    }

    /// Applies the force-full CAS hint for outputs that must keep a complete base.
    async fn apply_force_full_hint(&self, target_hash: Hash) -> Result<(), ConductorError> {
        let empty_hash = empty_content_hash();
        if target_hash == empty_hash {
            return Ok(());
        }
        let bases = BTreeSet::from([empty_hash]);
        self.cas.set_constraint(Constraint { target_hash, potential_bases: bases }).await?;
        Ok(())
    }

    /// Applies reverse-diff hints from each source input toward the produced output.
    async fn apply_reverse_diff_hints(
        &self,
        output_hash: Hash,
        inputs: &BTreeMap<String, ResolvedInput>,
    ) -> Result<(), ConductorError> {
        for input_hash in inputs.values().map(|input| input.hash) {
            if input_hash == output_hash {
                continue;
            }
            let patch = ConstraintPatch {
                add_bases: BTreeSet::from([output_hash]),
                remove_bases: BTreeSet::new(),
                clear_existing: false,
            };
            self.cas.patch_constraint(input_hash, patch).await?;
        }
        Ok(())
    }

    /// Derives the deterministic instance key used for deduplication and
    /// state merge.
    ///
    /// Key material intentionally includes only tool `tool_name`, projected
    /// tool `metadata`, optional `impure_timestamp`, and resolved input hash
    /// identities.
    ///
    /// Captured outputs, scheduler/runtime diagnostics, and any other runtime
    /// execution details are intentionally excluded from key derivation.
    fn derive_instance_key(
        tool_name: &str,
        metadata: &ToolSpec,
        impure_timestamp: Option<crate::model::config::ImpureTimestamp>,
        inputs: &BTreeMap<String, ResolvedInput>,
    ) -> Result<String, ConductorError> {
        let mut hasher = blake3::Hasher::new();
        let metadata_bytes = Self::serialize_metadata_for_instance_key(metadata)?;
        Self::update_segment(&mut hasher, b"tool.metadata", &metadata_bytes);
        Self::update_segment(&mut hasher, b"tool.name", tool_name.as_bytes());

        if let Some(timestamp) = impure_timestamp {
            Self::update_segment(
                &mut hasher,
                b"tool.impure_timestamp.epoch_seconds",
                &timestamp.epoch_seconds.to_le_bytes(),
            );
            Self::update_segment(
                &mut hasher,
                b"tool.impure_timestamp.subsec_nanos",
                &timestamp.subsec_nanos.to_le_bytes(),
            );
        }
        for (key, input) in inputs {
            Self::update_segment(&mut hasher, b"input.key", key.as_bytes());
            Self::update_hash_identity(&mut hasher, input.hash);
        }
        Ok(hasher.finalize().to_hex().to_string())
    }

    /// Serializes metadata into canonical instance-key material.
    ///
    /// Builtin metadata is intentionally projected to only
    /// `kind`/`name`/`version` identity fields.
    /// Executable metadata keeps the complete `ToolSpec` payload.
    fn serialize_metadata_for_instance_key(metadata: &ToolSpec) -> Result<Vec<u8>, ConductorError> {
        #[derive(serde::Serialize)]
        struct BuiltinKeyMetadata<'a> {
            kind: &'static str,
            name: &'a str,
            version: &'a str,
        }

        match &metadata.kind {
            ToolKindSpec::Builtin { name, version } => serde_json::to_vec(&BuiltinKeyMetadata {
                kind: "builtin",
                name,
                version,
            })
            .map_err(|err| {
                ConductorError::Serialization(format!(
                    "serializing tool metadata for deterministic instance key derivation: {err}"
                ))
            }),
            ToolKindSpec::Executable { .. } => serde_json::to_vec(metadata).map_err(|err| {
                ConductorError::Serialization(format!(
                    "serializing tool metadata for deterministic instance key derivation: {err}"
                ))
            }),
        }
    }

    /// Adds one tagged byte segment to the instance-key hasher.
    fn update_segment(hasher: &mut blake3::Hasher, tag: &[u8], payload: &[u8]) {
        hasher.update(tag);
        hasher.update(&(payload.len() as u64).to_le_bytes());
        hasher.update(payload);
    }

    /// Adds one hash identity (algorithm + digest) to the instance-key hasher.
    fn update_hash_identity(hasher: &mut blake3::Hasher, hash: Hash) {
        hasher.update(&hash.code().to_le_bytes());
        hasher.update(&[hash.size()]);
        hasher.update(hash.digest());
    }

    /// Normalizes one success-code list into a deterministic set.
    fn normalize_success_codes(codes: &[i32]) -> BTreeSet<i32> {
        codes.iter().copied().collect()
    }

    /// Returns whether one CAS read error indicates persistent payload corruption.
    fn is_cas_corruption_read_error(error: &CasError) -> bool {
        matches!(
            error,
            CasError::CorruptObject(_) | CasError::CorruptIndex(_) | CasError::InvalidDelta(_)
        )
    }

    /// Returns whether one process exit code should be treated as success.
    fn is_success_exit_code(process_code: i32, success_codes: &BTreeSet<i32>) -> bool {
        success_codes.contains(&process_code)
    }

    /// Formats process `stderr` for workflow-failure diagnostics.
    ///
    /// ANSI/control bytes are intentionally preserved so callers that render
    /// terminal styling (for example colorized diagnostics) can display the
    /// original formatting. The only normalization performed is an
    /// `is_empty`-style fallback check after trimming whitespace.
    #[must_use]
    fn format_process_failure_stderr(stderr_bytes: &[u8]) -> String {
        let stderr = String::from_utf8_lossy(stderr_bytes);
        if stderr.trim().is_empty() { "no stderr output".to_string() } else { stderr.into_owned() }
    }
}

/// Spawns one deterministic worker pool for workflow step execution.
pub(super) async fn spawn_step_worker_pool<C>(
    cas: Arc<C>,
    worker_count: usize,
) -> Result<Vec<ActorRef<StepWorkerMessage>>, ConductorError>
where
    C: CasApi + Send + Sync + 'static,
{
    let mut workers = Vec::with_capacity(worker_count);
    for index in 0..worker_count {
        let (worker_ref, _handle) =
            Actor::spawn(None, StepWorkerActor::<C>::default(), cas.clone()).await.map_err(
                |err| {
                    ConductorError::Internal(format!(
                        "failed spawning conductor step worker {index}: {err}"
                    ))
                },
            )?;
        workers.push(worker_ref);
    }
    Ok(workers)
}

/// Executes one step directly without going through worker RPC, used for fallback handling.
pub(super) async fn execute_step_direct<C>(
    cas: Arc<C>,
    request: StepExecutionRequest,
) -> Result<StepExecutionBundle, ConductorError>
where
    C: CasApi + Send + Sync + 'static,
{
    StepWorkerExecutor { cas }.execute_step(request).await
}

#[cfg(test)]
mod tests;
