//! Actor-backed step execution.
//!
//! Step workers own the expensive, side-effecting portion of orchestration:
//! resolving inputs, rendering templates, materializing sandbox files, running
//! processes, and capturing declared outputs. The execution hub interacts with
//! them through deterministic request/response messages while the coordinator
//! keeps state merging separate.
//!
//! # Module structure note
//!
//! This file intentionally remains as a single module despite exceeding 2 000
//! lines. Every method in `impl StepWorkerExecutor` takes `&self` or
//! `&mut self`, giving it access to the private fields of the struct. Rust
//! does not allow `impl` blocks to span across multiple files (without the
//! non-idiomatic `include!()` macro), so a child-module split would require
//! either converting all methods to standalone functions (which would lose the
//! ergonomics of associated-function call syntax and the implicit access to
//! `&self`) or using the `include!()` escape hatch (which hides code
//! structure). Neither trade-off is worth the gain here; keep this file whole
//! and rely on the template, tests, and future sibling submodules for
//! genuinely separable concerns.

use std::collections::{BTreeMap, BTreeSet};
use std::marker::PhantomData;
use std::path::{Component, Path, PathBuf};
use std::sync::Arc;
use std::time::{Duration, Instant};

use mediapm_cas::{
    CasApi, CasError, CasExistenceBitmap, Constraint, ConstraintPatch, Hash, empty_content_hash,
};
use ractor::{Actor, ActorProcessingErr, ActorRef, RpcReplyPort};
use regex::Regex;

use crate::error::{ConductorError, CorruptWorkflowOutputContext};
use crate::model::config::{
    ImpureTimestamp, InputBinding, OutputCaptureSpec, ParsedInputBindingSegment, ProcessSpec,
    ToolInputKind, ToolKindSpec, ToolSpec, WorkflowStepSpec, parse_input_binding,
};
use crate::model::state::{
    OutputRef, OutputSaveMode, PersistenceFlags, ResolvedInput, ToolCallInstance,
    merge_persistence_flags,
};
use crate::orchestration::protocol::{
    StepExecutionBundle, StepExecutionPhaseTimings, StepExecutionRequest, StepOutputs,
    UnifiedNickelDocument, UnifiedToolSpec,
};
mod template;
pub(crate) mod tool_content_cache;
use tool_content_cache::ToolCacheReadGuard;

/// Environment-variable override for executable subprocess timeout (seconds).
const EXECUTABLE_TIMEOUT_SECS_ENV_VAR: &str = "MEDIAPM_CONDUCTOR_EXECUTABLE_TIMEOUT_SECS";

/// Default timeout budget for external executable subprocesses.
///
/// This guard prevents indefinitely hanging child processes from stalling
/// conductor workers forever while still leaving enough room for heavy media
/// transforms in normal host conditions.
const DEFAULT_EXECUTABLE_TIMEOUT_SECS: u64 = 15 * 60;

/// Worker actor request envelope.
#[derive(Debug)]
pub(crate) enum StepWorkerMessage {
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
    File { relative_path: PathBuf },
    /// Capture bytes from one file selected by regex against sandbox-relative
    /// normalized paths.
    FileRegex {
        /// Compiled regex for matching sandbox-relative file paths.
        path_regex: Regex,
        /// Original regex pattern for diagnostics.
        pattern: String,
    },
    /// Capture one directory by zipping descendants into one ZIP payload.
    FolderAsZip {
        /// Relative directory path inside the execution sandbox.
        relative_path: PathBuf,
        /// Whether the top-level folder node is included in the ZIP payload.
        include_topmost_folder: bool,
    },
    /// Capture one ZIP payload containing all files selected by regex.
    FolderRegexAsZip {
        /// Compiled regex for matching sandbox-relative file/dir paths.
        path_regex: Regex,
        /// Original regex pattern for diagnostics.
        pattern: String,
    },
}

/// Fully resolved output specification for one declared tool output.
#[derive(Debug, Clone)]
struct ResolvedOutputSpec {
    /// Where the output payload should be captured from.
    capture: ResolvedOutputCapture,
    /// Final persistence policy after tool defaults and step overrides merge.
    persistence: PersistenceFlags,
    /// Whether a capture that produces no output is treated as a successful
    /// empty capture rather than a workflow error.
    ///
    /// When `true` and the capture source is absent, the output is stored as
    /// `allow_empty_capture = true` in the orchestration state. Downstream
    /// steps referencing this output as a step input receive a workflow error.
    allow_empty: bool,
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

/// Resolved selector source for one template interpolation token.
#[derive(Debug, Clone, PartialEq, Eq)]
enum TemplateSelectorSource {
    /// Selector resolves from step input bindings (`inputs.*`).
    Input(String),
    /// Selector resolves to current host platform text (`windows`/`linux`/`macos`).
    ContextOs,
    /// Selector resolves to the current process working-directory path.
    ContextWorkingDirectory,
}

/// Helper object that executes one step against one CAS implementation.
#[derive(Debug, Clone)]
struct StepWorkerExecutor<C>
where
    C: CasApi,
{
    /// Shared CAS handle used for all input, output, and state-addressing I/O.
    cas: Arc<C>,
    /// OS-backed per-conductor-dir temporary root for sandboxes, ZIP
    /// extraction, and regex capture staging.
    conductor_tmp_dir: PathBuf,
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
                let executor = StepWorkerExecutor {
                    cas: state.clone(),
                    conductor_tmp_dir: request.conductor_tmp_dir.clone(),
                };
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
    #[expect(
        clippy::too_many_lines,
        reason = "this item intentionally keeps end-to-end control flow together so ordering invariants remain explicit during maintenance"
    )]
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

        let mut phase_timings = StepExecutionPhaseTimings::default();

        let resolve_inputs_started_at = Instant::now();
        let resolved_inputs = self
            .resolve_inputs(
                request.unified.as_ref(),
                tool,
                &request.workflow_name,
                &request.step,
                request.step_outputs.as_ref(),
            )
            .await?;
        phase_timings.resolve_inputs_ms =
            resolve_inputs_started_at.elapsed().as_secs_f64() * 1000.0;

        // Cheap metadata and key derivation — no template rendering.
        let metadata = Self::tool_spec_from_unified(tool);
        let instance_key = Self::derive_instance_key(
            &request.step.tool,
            &metadata,
            request.impure_timestamp,
            &resolved_inputs,
        )?;

        // Derive the effective output name set from the tool schema.
        // Template-heavy output resolution is deferred until after the cache
        // probe so cache-hit steps skip it entirely.
        let mut effective_output_names = request.required_output_names.clone();
        effective_output_names.extend(request.step.outputs.keys().cloned());
        if effective_output_names.is_empty() {
            effective_output_names.extend(tool.outputs.keys().cloned());
        }
        for required_output_name in &effective_output_names {
            if !tool.outputs.contains_key(required_output_name) {
                return Err(ConductorError::Workflow(format!(
                    "workflow '{}' step '{}' requires unknown output '{}'",
                    request.workflow_name, request.step.id, required_output_name
                )));
            }
        }
        let requested_output_names: Vec<String> = effective_output_names.iter().cloned().collect();

        let cache_probe_started_at = Instant::now();
        let existing_instance = request.state_snapshot.instances.get(&instance_key).cloned();
        let mut rematerialized = false;
        let mut needs_execution = existing_instance.is_none();
        if let Some(instance) = &existing_instance {
            let check_hashes: Vec<Hash> = request
                .required_output_names
                .iter()
                .filter_map(|n| instance.outputs.get(n).map(|r| r.hash))
                .collect();
            if check_hashes.len() != request.required_output_names.len() {
                needs_execution = true;
            } else {
                let bitmap: CasExistenceBitmap = self.cas.exists_many(check_hashes).await?;
                let _span = tracing::span!(tracing::Level::DEBUG, "cache_probe", output_count = %request.required_output_names.len(), batched = true).entered();
                for i in 0..bitmap.len() {
                    if !bitmap[i] {
                        needs_execution = true;
                        rematerialized = true;
                        break;
                    }
                }
            }
        }
        phase_timings.cache_probe_ms = cache_probe_started_at.elapsed().as_secs_f64() * 1000.0;

        // Construct the instance, pulling cached outputs on hit.
        // Step-level persistence overrides are applied upfront so the
        // cache-hit path can skip template-heavy output spec resolution.
        let mut instance = if let Some(existing) = existing_instance {
            let mut outputs = existing.outputs;
            for (name, policy) in &request.step.outputs {
                if let Some(output_ref) = outputs.get_mut(name) {
                    output_ref.persistence = policy.resolve(output_ref.persistence);
                }
            }
            ToolCallInstance {
                tool_name: request.step.tool.clone(),
                metadata,
                impure_timestamp: request.impure_timestamp,
                inputs: resolved_inputs.clone(),
                outputs,
                last_used: ImpureTimestamp::default(),
            }
        } else {
            ToolCallInstance {
                tool_name: request.step.tool.clone(),
                metadata,
                impure_timestamp: request.impure_timestamp,
                inputs: resolved_inputs.clone(),
                outputs: BTreeMap::new(),
                last_used: ImpureTimestamp::default(),
            }
        };

        // Template-heavy resolution (process execution + output specs) runs
        // only when a miss or partial miss forces actual execution.
        let output_specs: BTreeMap<String, ResolvedOutputSpec> = if needs_execution {
            let resolve_specs_started_at = Instant::now();
            let mut template_file_writes = Vec::new();
            let resolved_process =
                self.resolve_process_execution(tool, &resolved_inputs, &mut template_file_writes)?;
            let output_specs = self.resolve_output_specs(
                tool,
                &request.step,
                &resolved_inputs,
                &mut template_file_writes,
            )?;
            phase_timings.resolve_specs_ms =
                resolve_specs_started_at.elapsed().as_secs_f64() * 1000.0;

            let materialization_started_at = Instant::now();
            let execution_cwd_temp = self.create_execution_temp_cwd()?;
            let execution_cwd = execution_cwd_temp.path();
            // payload_dir is Some for all managed executable tools (non-empty
            // content_map). Passed to the subprocess spawner so the binary is
            // executed from its stable persistent cache path rather than from
            // the per-step sandbox hard-link, avoiding repeated macOS
            // Gatekeeper/XProtect per-path security scans on every step.
            let (payload_dir, cache_guard) = self
                .materialize_tool_content_map(
                    &request.step.tool,
                    &tool.tool_content_map,
                    &resolved_process,
                    execution_cwd,
                    &request.runtime_tools_dir,
                )
                .await?;
            self.materialize_template_file_writes(&template_file_writes, execution_cwd)?;
            phase_timings.materialization_ms =
                materialization_started_at.elapsed().as_secs_f64() * 1000.0;

            let execution_started_at = Instant::now();
            let capture = self
                .execute_tool(
                    &resolved_process,
                    &resolved_inputs,
                    execution_cwd,
                    &request.outermost_config_dir,
                    payload_dir.as_deref(),
                    cache_guard,
                )
                .await?;
            phase_timings.execution_ms = execution_started_at.elapsed().as_secs_f64() * 1000.0;

            let capture_outputs_started_at = Instant::now();
            for output_name in &effective_output_names {
                let output_spec = output_specs.get(output_name).ok_or_else(|| {
                    ConductorError::Internal(format!(
                        "output '{output_name}' disappeared from resolved output spec map"
                    ))
                })?;
                let maybe_payload =
                    self.capture_output_payload(output_spec, &capture, execution_cwd)?;
                if let Some(payload) = maybe_payload {
                    let hash = self.cas.put(payload).await?;
                    instance.outputs.insert(
                        output_name.clone(),
                        OutputRef {
                            hash,
                            persistence: output_spec.persistence,
                            allow_empty_capture: false,
                        },
                    );
                } else {
                    // Empty capture: allowed by allow_empty = true on the tool output spec.
                    // Store empty bytes in CAS so cache-hit paths have a stable hash and
                    // mark the OutputRef so coordinators can surface a descriptive error
                    // if a downstream step tries to consume this empty output as input.
                    let empty_hash = self.cas.put(Vec::new()).await?;
                    instance.outputs.insert(
                        output_name.clone(),
                        OutputRef {
                            hash: empty_hash,
                            persistence: output_spec.persistence,
                            allow_empty_capture: true,
                        },
                    );
                }
            }
            phase_timings.capture_outputs_ms =
                capture_outputs_started_at.elapsed().as_secs_f64() * 1000.0;

            output_specs
        } else {
            // Cache hit: build output-spec entries from the already-overridden
            // instance outputs so the persistence merge below is idempotent.
            effective_output_names
                .iter()
                .map(|name| {
                    let persistence =
                        instance.outputs.get(name).map(|r| r.persistence).unwrap_or_default();
                    (
                        name.clone(),
                        ResolvedOutputSpec {
                            capture: ResolvedOutputCapture::Stdout,
                            persistence,
                            allow_empty: false,
                        },
                    )
                })
                .collect()
        };

        let persistence_merge_started_at = Instant::now();
        let mut pending_unsaved_hashes = BTreeSet::new();
        for output_name in &effective_output_names {
            let output_spec = output_specs.get(output_name).ok_or_else(|| {
                ConductorError::Internal(format!(
                    "output '{output_name}' disappeared from resolved output spec map"
                ))
            })?;
            let output_ref = instance.outputs.get_mut(output_name).ok_or_else(|| {
                ConductorError::Internal(format!(
                    "instance '{instance_key}' missing output '{output_name}' after execution"
                ))
            })?;
            let merged = merge_persistence_flags([output_ref.persistence, output_spec.persistence]);
            output_ref.persistence = merged;
            let output_exists = self.cas.exists(output_ref.hash).await?;
            if !output_exists {
                if request.required_output_names.contains(output_name) {
                    return Err(ConductorError::Internal(format!(
                        "required output '{output_name}' for instance '{instance_key}' is missing from CAS after execution planning"
                    )));
                }
                if !merged.save.should_persist() {
                    pending_unsaved_hashes.insert(output_ref.hash);
                }
                continue;
            }
            if merged.save.prefers_full() {
                self.apply_full_save_hint(output_ref.hash).await?;
            }
            self.apply_reverse_diff_hints(output_ref.hash, &resolved_inputs).await?;
            if !merged.save.should_persist() {
                pending_unsaved_hashes.insert(output_ref.hash);
            }
        }
        phase_timings.persistence_merge_ms =
            persistence_merge_started_at.elapsed().as_secs_f64() * 1000.0;

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
            phase_timings,
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
    #[expect(
        clippy::too_many_lines,
        reason = "this item intentionally keeps end-to-end control flow together so ordering invariants remain explicit during maintenance"
    )]
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
    /// - `${step_output.<step_id>.<output_name>:zip(<member>)}`,
    /// - `${env.<VAR_NAME>}`,
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
                    let Some(reference) = unified.external_data.get(&hash) else {
                        return Err(ConductorError::Workflow(format!(
                            "workflow '{workflow_name}' step '{}' references unknown external data hash '{hash}'",
                            step.id
                        )));
                    };
                    let bytes = self.cas.get(hash).await?;
                    if reference.save.is_some_and(OutputSaveMode::prefers_full) {
                        self.apply_full_save_hint(hash).await?;
                    }
                    plain_content.extend_from_slice(bytes.as_ref());
                }
                ParsedInputBindingSegment::Literal(content) => {
                    if !content.is_empty() {
                        plain_content.extend_from_slice(content.as_bytes());
                    }
                }
                ParsedInputBindingSegment::StepOutput { step_id, output, zip_member } => {
                    let producer = step_outputs.get(step_id).ok_or_else(|| {
                        ConductorError::Workflow(format!(
                            "workflow '{workflow_name}' step '{}' references output '{}' from step '{}' before it is available",
                            step.id, output, step_id
                        ))
                    })?;
                    let output_slot = producer.get(output).ok_or_else(|| {
                        ConductorError::Workflow(format!(
                            "workflow '{workflow_name}' step '{}' references missing output '{}' on step '{}'",
                            step.id, output, step_id
                        ))
                    })?;
                    // None = empty capture produced via allow_empty; using an empty capture as
                    // a step input is a workflow error to prevent silent empty-payload propagation.
                    let output_hash = output_slot.ok_or_else(|| {
                        ConductorError::Workflow(format!(
                            "workflow '{workflow_name}' step '{}' references output '{}' from step '{}', \
                             but that output was captured as empty (allow_empty = true) and cannot be \
                             used as a step input",
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

                    if let Some(member) = zip_member {
                        let member_bytes = self.extract_zip_member_from_output(
                            workflow_name,
                            &step.id,
                            step_id,
                            output,
                            member,
                            bytes.as_ref(),
                        )?;
                        plain_content.extend_from_slice(member_bytes.as_slice());
                    } else {
                        plain_content.extend_from_slice(bytes.as_ref());
                    }
                }
                ParsedInputBindingSegment::Env { name } => {
                    let value = std::env::var(name).map_err(|error| {
                        ConductorError::Workflow(format!(
                            "workflow '{workflow_name}' step '{}' references environment variable '{name}' in input binding '{binding}', but it is not available at execution time: {error}",
                            step.id
                        ))
                    })?;
                    plain_content.extend_from_slice(value.as_bytes());
                }
            }
        }

        Ok(plain_content)
    }

    /// Extracts one file member from ZIP bytes referenced by a step-output binding.
    fn extract_zip_member_from_output(
        &self,
        workflow_name: &str,
        consumer_step_id: &str,
        producer_step_id: &str,
        output_name: &str,
        zip_member: &str,
        zip_bytes: &[u8],
    ) -> Result<Vec<u8>, ConductorError> {
        let normalized_member =
            self.normalized_relative_tool_path(zip_member, "step_output zip member selector")?;

        let extraction_workspace = tempfile::Builder::new()
            .prefix("step-output-zip-")
            .tempdir_in(&self.conductor_tmp_dir)
            .map_err(|source| ConductorError::Io {
                operation: "creating temporary ZIP extraction workspace for step_output binding"
                    .to_string(),
                path: self.conductor_tmp_dir.clone(),
                source,
            })?;

        let extraction_root = extraction_workspace.path().join("extracted");
        std::fs::create_dir_all(&extraction_root).map_err(|source| ConductorError::Io {
            operation: "creating temporary ZIP extraction directory for step_output binding"
                .to_string(),
            path: extraction_root.clone(),
            source,
        })?;

        mediapm_conductor_builtin_archive::unpack_zip_bytes_to_directory(zip_bytes, &extraction_root)
            .map_err(|error| {
                ConductorError::Workflow(format!(
                    "workflow '{workflow_name}' step '{consumer_step_id}' failed extracting ZIP member '{zip_member}' from '${{step_output.{producer_step_id}.{output_name}}}': {error}"
                ))
            })?;

        let member_path = extraction_root.join(&normalized_member);
        if !member_path.exists() {
            return Err(ConductorError::Workflow(format!(
                "workflow '{workflow_name}' step '{consumer_step_id}' references ZIP member '{zip_member}' from '${{step_output.{producer_step_id}.{output_name}}}', but no such member exists"
            )));
        }
        if member_path.is_dir() {
            return Err(ConductorError::Workflow(format!(
                "workflow '{workflow_name}' step '{consumer_step_id}' references ZIP member '{zip_member}' from '${{step_output.{producer_step_id}.{output_name}}}', but the selected entry is a directory"
            )));
        }

        std::fs::read(&member_path).map_err(|source| ConductorError::Io {
            operation: format!(
                "reading extracted ZIP member '{}' from step_output binding",
                normalized_member.to_string_lossy()
            ),
            path: member_path,
            source,
        })
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
            ProcessSpec::Executable { command, env_vars: _declared_env_vars, success_codes } => {
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
                    env_vars: self.render_templates(
                        &tool.execution_env_vars,
                        inputs,
                        pending_file_writes,
                    )?,
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

    /// Creates one ad hoc sandbox directory inside `std::env::temp_dir()` only
    /// when a step actually needs to execute.
    fn create_execution_temp_cwd(&self) -> Result<tempfile::TempDir, ConductorError> {
        let scratch_root = self.conductor_tmp_dir.clone();
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
    #[expect(
        clippy::unused_self,
        reason = "helper is intentionally instance-scoped for local API consistency"
    )]
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

    /// Compiles one output-capture regex pattern after template rendering.
    #[expect(
        clippy::unused_self,
        reason = "instance-scoped helper keeps call sites uniform across template and capture helpers"
    )]
    fn compile_output_capture_regex(
        &self,
        pattern: &str,
        context: &str,
    ) -> Result<Regex, ConductorError> {
        let trimmed = pattern.trim();
        if trimmed.is_empty() {
            return Err(ConductorError::Workflow(format!(
                "{context} regex pattern must be non-empty"
            )));
        }

        Regex::new(trimmed).map_err(|err| {
            ConductorError::Workflow(format!(
                "{context} regex pattern '{trimmed}' is invalid: {err}"
            ))
        })
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
                    OutputCaptureSpec::FileRegex { path_regex } => {
                        let rendered =
                            self.render_template_value(path_regex, inputs, pending_file_writes)?;
                        let regex =
                            self.compile_output_capture_regex(&rendered, "output file capture")?;
                        ResolvedOutputCapture::FileRegex { path_regex: regex, pattern: rendered }
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
                    OutputCaptureSpec::FolderRegex { path_regex } => {
                        let rendered =
                            self.render_template_value(path_regex, inputs, pending_file_writes)?;
                        let regex =
                            self.compile_output_capture_regex(&rendered, "output folder capture")?;
                        ResolvedOutputCapture::FolderRegexAsZip {
                            path_regex: regex,
                            pattern: rendered,
                        }
                    }
                };
                Ok((
                    name.clone(),
                    ResolvedOutputSpec {
                        capture,
                        persistence: PersistenceFlags::default(),
                        allow_empty: output_spec.allow_empty,
                    },
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
        payload_dir: Option<&Path>,
        _cache_guard: Option<ToolCacheReadGuard>,
    ) -> Result<ToolExecutionCapture, ConductorError> {
        match process {
            ResolvedProcessExecution::Executable { executable, args, env_vars, success_codes } => {
                self.execute_executable_tool(
                    executable,
                    args,
                    env_vars,
                    success_codes,
                    tool_cwd,
                    payload_dir,
                    _cache_guard,
                )
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
    #[expect(
        clippy::unused_self,
        reason = "method form preserves a cohesive helper surface on StepWorkerExecutor"
    )]
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
    ///
    /// Runtime timeout uses [`EXECUTABLE_TIMEOUT_SECS_ENV_VAR`] when provided,
    /// otherwise [`DEFAULT_EXECUTABLE_TIMEOUT_SECS`].
    async fn execute_executable_tool(
        &self,
        executable_name: &str,
        resolved_args: &[String],
        env_vars: &BTreeMap<String, String>,
        success_codes: &BTreeSet<i32>,
        tool_cwd: &Path,
        payload_dir: Option<&Path>,
        _cache_guard: Option<ToolCacheReadGuard>,
    ) -> Result<ToolExecutionCapture, ConductorError> {
        let executable_timeout = Self::resolve_executable_timeout_duration()?;

        self.execute_executable_tool_with_timeout(
            executable_name,
            resolved_args,
            env_vars,
            success_codes,
            tool_cwd,
            payload_dir,
            _cache_guard,
            executable_timeout,
        )
        .await
    }

    /// Resolves the configured executable timeout budget.
    ///
    /// # Errors
    ///
    /// Returns [`ConductorError::Workflow`] when
    /// [`EXECUTABLE_TIMEOUT_SECS_ENV_VAR`] is set to a non-positive or
    /// non-integer value.
    fn resolve_executable_timeout_duration() -> Result<Duration, ConductorError> {
        let Some(raw) = std::env::var_os(EXECUTABLE_TIMEOUT_SECS_ENV_VAR) else {
            return Ok(Duration::from_secs(DEFAULT_EXECUTABLE_TIMEOUT_SECS));
        };

        let text = raw.to_string_lossy().trim().to_string();
        if text.is_empty() {
            return Ok(Duration::from_secs(DEFAULT_EXECUTABLE_TIMEOUT_SECS));
        }

        Self::parse_executable_timeout_duration(&text)
    }

    /// Parses one explicit timeout override value into a [`Duration`].
    ///
    /// # Errors
    ///
    /// Returns [`ConductorError::Workflow`] when the value is non-integer or
    /// non-positive.
    fn parse_executable_timeout_duration(value: &str) -> Result<Duration, ConductorError> {
        let text = value.trim();
        if text.is_empty() {
            return Err(ConductorError::Workflow(format!(
                "{EXECUTABLE_TIMEOUT_SECS_ENV_VAR} must be a positive integer number of seconds"
            )));
        }

        let timeout_seconds = text.parse::<u64>().map_err(|error| {
            ConductorError::Workflow(format!(
                "{EXECUTABLE_TIMEOUT_SECS_ENV_VAR} must be a positive integer number of seconds, got '{text}': {error}"
            ))
        })?;
        if timeout_seconds == 0 {
            return Err(ConductorError::Workflow(format!(
                "{EXECUTABLE_TIMEOUT_SECS_ENV_VAR} must be greater than 0 seconds"
            )));
        }

        Ok(Duration::from_secs(timeout_seconds))
    }

    /// Spawns one executable process with an explicit timeout budget.
    ///
    /// # Errors
    ///
    /// Returns [`ConductorError::Workflow`] when process startup succeeds but
    /// execution exceeds `executable_timeout`.
    #[expect(
        clippy::too_many_arguments,
        reason = "flat argument list avoids an ad-hoc parameter struct for a single call site"
    )]
    async fn execute_executable_tool_with_timeout(
        &self,
        executable_name: &str,
        resolved_args: &[String],
        env_vars: &BTreeMap<String, String>,
        success_codes: &BTreeSet<i32>,
        tool_cwd: &Path,
        payload_dir: Option<&Path>,
        _cache_guard: Option<ToolCacheReadGuard>,
        executable_timeout: Duration,
    ) -> Result<ToolExecutionCapture, ConductorError> {
        if executable_name.trim().is_empty() {
            return Err(ConductorError::Workflow(
                "executable command[0] must be non-empty for kind='executable'".to_string(),
            ));
        }

        // On macOS, Gatekeeper/XProtect caches scan results per file path (not
        // per inode). Hard-linking the tool binary into a fresh per-step tmpdir
        // path on each step triggers a new ~4-5 s scan every time. By resolving
        // the executable from its stable persistent payload-cache path we pay
        // that cost only once per tool version; the sandbox CWD is still used
        // for all relative I/O paths.
        let executable_path = if let Some(pd) = payload_dir {
            let normalized =
                self.normalized_relative_tool_path(executable_name, "tool process executable")?;
            let candidate = pd.join(&normalized);
            if candidate.exists() { candidate } else { tool_cwd.join(normalized) }
        } else {
            self.resolve_tool_relative_path(executable_name, tool_cwd, "tool process executable")?
        };

        // `command.current_dir(tool_cwd)` changes the child process's working directory
        // before the OS resolves the executable. On Unix, `execve` looks up relative paths
        // against the **child's** CWD (post-chdir), not the spawning process's CWD. An
        // executable_path that is relative to the workspace root (e.g. from a relative
        // tools_dir) would therefore be searched inside the sandbox temp-dir, which does
        // not contain the tool payload — causing ENOENT. Resolve to an absolute path
        // before spawning so the lookup is independent of chdir.
        let executable_path = if executable_path.is_absolute() {
            executable_path
        } else {
            std::env::current_dir()
                .map_err(|source| ConductorError::Io {
                    operation: format!(
                        "determining working directory to resolve executable path for \
                         process '{executable_name}'"
                    ),
                    path: executable_path.clone(),
                    source,
                })?
                .join(executable_path)
        };

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

        let mut command = tokio::process::Command::new(&executable_path);
        for arg in resolved_args {
            command.arg(arg);
        }
        command.current_dir(tool_cwd);
        command.stdin(std::process::Stdio::null());
        command.env_clear();
        command.envs(env_vars);
        command.kill_on_drop(true);
        let output = match tokio::time::timeout(executable_timeout, command.output()).await {
            Ok(output) => output.map_err(|source| ConductorError::Io {
                operation: format!("executing executable process '{executable_name}'"),
                path: executable_path.clone(),
                source,
            })?,
            Err(_) => {
                return Err(ConductorError::Workflow(format!(
                    "process '{executable_name}' exceeded timeout of {} seconds; adjust {EXECUTABLE_TIMEOUT_SECS_ENV_VAR} to override",
                    executable_timeout.as_secs()
                )));
            }
        };

        let Some(process_code) = output.status.code() else {
            return Err(ConductorError::Workflow(format!(
                "process '{executable_name}' terminated without an exit code"
            )));
        };

        if !Self::is_success_exit_code(process_code, success_codes) {
            let stderr = Self::format_process_failure_stderr(&output.stderr);
            return Err(ConductorError::Workflow(format!(
                "process '{executable_name}' exited with code {process_code}, expected one of {success_codes:?}: {stderr}"
            )));
        }

        Ok(ToolExecutionCapture { stdout: output.stdout, stderr: output.stderr, process_code })
    }

    /// Materializes per-tool `content_map` entries from CAS into the persistent
    /// payload cache and, when necessary, links the payload tree into the sandbox.
    ///
    /// Delegates to the persistent tool-content cache (`tool_content_cache`
    /// module) keyed by `tool_id`.  On a cache hit the payload tree is already
    /// extracted; on a miss CAS bytes for all entries are fetched concurrently,
    /// collision-checked, and extracted into a fresh `payload/` directory.
    ///
    /// When the managed-tool executable resolves directly inside the returned
    /// payload cache directory, the function returns early with that path so
    /// the subprocess launcher can execute from the stable cache location.  This
    /// skips the per-step `O(n_files)` sandbox hard-link pass and, on macOS,
    /// avoids Gatekeeper/XProtect re-scan costs that would otherwise trigger
    /// for each new hard-link path.  When the executable is absent from the
    /// cache, the sandbox is populated via hard links from the cache payload
    /// directory (falling back to copies on cross-device setups).
    async fn materialize_tool_content_map(
        &self,
        tool_id: &str,
        tool_content_map: &BTreeMap<String, Hash>,
        resolved_process: &ResolvedProcessExecution,
        tool_cwd: &Path,
        tools_dir: &Path,
    ) -> Result<(Option<PathBuf>, Option<ToolCacheReadGuard>), ConductorError> {
        if tool_content_map.is_empty() {
            return Ok((None, None));
        }
        let (payload_dir, guard) = tool_content_cache::prepare_tool_content_cache(
            tools_dir,
            tool_id,
            tool_content_map,
            &self.cas,
        )
        .await?;

        // Optimization: when command[0] resolves directly inside the persistent
        // payload cache, skip the per-step recursive sandbox linking pass and
        // execute from that stable cache path. This avoids repeated O(n_files)
        // metadata operations for large managed-tool payload trees on each step.
        // On macOS this also avoids Gatekeeper/XProtect re-scan costs that
        // would otherwise trigger for each new hard-link path.
        if let ResolvedProcessExecution::Executable { executable, .. } = resolved_process {
            let normalized =
                self.normalized_relative_tool_path(executable, "tool process executable")?;
            if payload_dir.join(&normalized).is_file() {
                return Ok((Some(payload_dir), Some(guard)));
            }
        }

        tool_content_cache::link_payload_to_sandbox(&payload_dir, tool_cwd).map_err(|err| {
            ConductorError::Workflow(format!("materializing tool content sandbox: {err}"))
        })?;
        Ok((Some(payload_dir), Some(guard)))
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
    #[expect(
        clippy::too_many_lines,
        reason = "this item intentionally keeps end-to-end control flow together so ordering invariants remain explicit during maintenance"
    )]
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
                            "builtin '{builtin_name}@{builtin_version}' failed: {err}"
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
                        "builtin '{builtin_name}@{builtin_version}' failed: {err}"
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
                        "builtin '{builtin_name}@{builtin_version}' failed: {err}"
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
                        "builtin '{builtin_name}@{builtin_version}' failed: {err}"
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
                        "builtin '{builtin_name}@{builtin_version}' failed: {err}"
                    ))
                })?;

                let payload = serde_json::to_vec(&response)
                    .map_err(|err| ConductorError::Serialization(err.to_string()))?;

                Ok(ToolExecutionCapture { stdout: payload, stderr: Vec::new(), process_code: 0 })
            }
            _ => Err(ConductorError::Workflow(format!(
                "unsupported builtin tool '{builtin_name}@{builtin_version}'"
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
    ///
    /// Returns `Ok(Some(bytes))` when the output is captured normally,
    /// `Ok(None)` when the capture source was absent and the output spec
    /// declares `allow_empty = true`, or an error when capture fails and
    /// `allow_empty = false`.
    fn capture_output_payload(
        &self,
        output_spec: &ResolvedOutputSpec,
        capture: &ToolExecutionCapture,
        tool_cwd: &Path,
    ) -> Result<Option<Vec<u8>>, ConductorError> {
        match &output_spec.capture {
            ResolvedOutputCapture::Stdout => Ok(Some(capture.stdout.clone())),
            ResolvedOutputCapture::Stderr => Ok(Some(capture.stderr.clone())),
            ResolvedOutputCapture::ProcessCode => {
                Ok(Some(capture.process_code.to_string().into_bytes()))
            }
            ResolvedOutputCapture::File { relative_path } => {
                let path = tool_cwd.join(relative_path);
                match std::fs::read(&path) {
                    Ok(bytes) => Ok(Some(bytes)),
                    Err(source)
                        if source.kind() == std::io::ErrorKind::NotFound
                            && output_spec.allow_empty =>
                    {
                        Ok(None)
                    }
                    Err(source) => Err(ConductorError::Io {
                        operation: "capturing declared file output".to_string(),
                        path,
                        source,
                    }),
                }
            }
            ResolvedOutputCapture::FileRegex { path_regex, pattern } => self
                .capture_regex_file_output(path_regex, pattern, tool_cwd, output_spec.allow_empty),
            ResolvedOutputCapture::FolderAsZip { relative_path, include_topmost_folder } => {
                let folder_path = tool_cwd.join(relative_path);
                if output_spec.allow_empty && !folder_path.exists() {
                    return Ok(None);
                }
                self.capture_folder_output_as_zip(relative_path, *include_topmost_folder, tool_cwd)
                    .map(Some)
            }
            ResolvedOutputCapture::FolderRegexAsZip { path_regex, pattern } => {
                self.capture_regex_folder_output_as_zip(path_regex, pattern, tool_cwd).map(Some)
            }
        }
    }

    /// Captures one file output selected by regex from the execution sandbox.
    ///
    /// When `allow_empty` is `true` and the regex matches no file, returns
    /// `Ok(None)` instead of returning a workflow error.
    fn capture_regex_file_output(
        &self,
        path_regex: &Regex,
        pattern: &str,
        tool_cwd: &Path,
        allow_empty: bool,
    ) -> Result<Option<Vec<u8>>, ConductorError> {
        let mut relative_files = BTreeSet::new();
        let mut relative_dirs = BTreeSet::new();
        self.collect_capture_candidate_paths(
            tool_cwd,
            tool_cwd,
            &mut relative_files,
            &mut relative_dirs,
        )?;

        let matches: Vec<PathBuf> = relative_files
            .into_iter()
            .filter(|relative| {
                let normalized = Self::normalize_relative_path_for_regex(relative);
                path_regex.is_match(&normalized)
            })
            .collect();

        match matches.len() {
            0 if allow_empty => Ok(None),
            0 => Err(ConductorError::Workflow(format!(
                "capturing declared file output by regex '{pattern}' failed: no sandbox file matched"
            ))),
            1 => {
                let matched_path = matches[0].clone();
                let path = tool_cwd.join(&matched_path);
                std::fs::read(&path).map(Some).map_err(|source| ConductorError::Io {
                    operation: format!(
                        "capturing declared file output by regex '{pattern}' from '{}'",
                        Self::normalize_relative_path_for_regex(&matched_path)
                    ),
                    path,
                    source,
                })
            }
            _ => {
                let joined = matches
                    .iter()
                    .map(|path| Self::normalize_relative_path_for_regex(path.as_path()))
                    .collect::<Vec<_>>()
                    .join(", ");
                Err(ConductorError::Workflow(format!(
                    "capturing declared file output by regex '{pattern}' was ambiguous: matched {} files ({joined})",
                    matches.len()
                )))
            }
        }
    }

    /// Captures one directory output by packing it into a ZIP payload.
    ///
    /// The implementation delegates packing to the builtin archive/zip crate,
    /// using stored (no-compression) ZIP entries to preserve exact payload
    /// bytes and avoid compression nondeterminism.
    #[expect(
        clippy::unused_self,
        reason = "method placement keeps folder and regex capture helpers grouped on the executor"
    )]
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

    /// Captures one ZIP payload containing files selected by regex.
    fn capture_regex_folder_output_as_zip(
        &self,
        path_regex: &Regex,
        pattern: &str,
        tool_cwd: &Path,
    ) -> Result<Vec<u8>, ConductorError> {
        let mut relative_files = BTreeSet::new();
        let mut relative_dirs = BTreeSet::new();
        self.collect_capture_candidate_paths(
            tool_cwd,
            tool_cwd,
            &mut relative_files,
            &mut relative_dirs,
        )?;

        let matched_files: BTreeSet<PathBuf> = relative_files
            .iter()
            .filter(|relative| {
                let normalized = Self::normalize_relative_path_for_regex(relative);
                path_regex.is_match(&normalized)
            })
            .cloned()
            .collect();

        let matched_dirs: BTreeSet<PathBuf> = relative_dirs
            .iter()
            .filter(|relative| {
                let normalized = Self::normalize_relative_path_for_regex(relative);
                path_regex.is_match(&normalized)
            })
            .cloned()
            .collect();

        if matched_files.is_empty() && matched_dirs.is_empty() {
            return self.capture_empty_regex_folder_output_as_zip(pattern);
        }

        let mut selected_files = matched_files;
        for matched_dir in &matched_dirs {
            selected_files.extend(
                relative_files
                    .iter()
                    .filter(|candidate| candidate.starts_with(matched_dir))
                    .cloned(),
            );
        }

        if selected_files.is_empty() {
            return self.capture_empty_regex_folder_output_as_zip(pattern);
        }

        let staging =
            tempfile::tempdir_in(&self.conductor_tmp_dir).map_err(|source| ConductorError::Io {
                operation: "creating temporary regex folder-capture staging directory".to_string(),
                path: self.conductor_tmp_dir.clone(),
                source,
            })?;

        let mut staged_target_sources = BTreeMap::<PathBuf, PathBuf>::new();

        for relative_file in &selected_files {
            let source_path = tool_cwd.join(relative_file);
            let staged_relative_path =
                self.resolve_regex_capture_renamed_path(relative_file, path_regex, pattern)?;
            if let Some(previous_source) =
                staged_target_sources.insert(staged_relative_path.clone(), relative_file.clone())
            {
                let previous = Self::normalize_relative_path_for_regex(&previous_source);
                let current = Self::normalize_relative_path_for_regex(relative_file);
                let target = Self::normalize_relative_path_for_regex(&staged_relative_path);
                return Err(ConductorError::Workflow(format!(
                    "capturing declared folder output by regex '{pattern}' produced renamed-path conflict '{target}' from '{previous}' and '{current}'"
                )));
            }

            let staged_path = staging.path().join(&staged_relative_path);

            if let Some(parent) = staged_path.parent() {
                std::fs::create_dir_all(parent).map_err(|source| ConductorError::Io {
                    operation: format!(
                        "creating parent directories for regex folder capture staging path '{}'",
                        staged_path.display()
                    ),
                    path: parent.to_path_buf(),
                    source,
                })?;
            }

            std::fs::copy(&source_path, &staged_path).map_err(|source| ConductorError::Io {
                operation: format!(
                    "staging regex folder capture file '{}'",
                    Self::normalize_relative_path_for_regex(relative_file)
                ),
                path: source_path,
                source,
            })?;
        }

        mediapm_conductor_builtin_archive::pack_directory_to_uncompressed_zip_bytes(
            staging.path(),
            false,
        )
        .map_err(|err| {
            ConductorError::Workflow(format!(
                "capturing declared folder output by regex '{pattern}' as ZIP failed: {err}"
            ))
        })
    }

    /// Captures an empty ZIP payload for regex folder captures with no files.
    ///
    /// Some output families are optional at runtime (for example provider-side
    /// sidecar generation that may legally emit no files). Returning a stable
    /// empty archive keeps output contracts deterministic without forcing
    /// caller-specific failure handling for "missing optional family" cases.
    fn capture_empty_regex_folder_output_as_zip(
        &self,
        pattern: &str,
    ) -> Result<Vec<u8>, ConductorError> {
        let staging =
            tempfile::tempdir_in(&self.conductor_tmp_dir).map_err(|source| ConductorError::Io {
                operation: "creating temporary empty regex folder-capture staging directory"
                    .to_string(),
                path: self.conductor_tmp_dir.clone(),
                source,
            })?;

        mediapm_conductor_builtin_archive::pack_directory_to_uncompressed_zip_bytes(
            staging.path(),
            false,
        )
        .map_err(|err| {
            ConductorError::Workflow(format!(
                "capturing declared folder output by regex '{pattern}' as ZIP failed: {err}"
            ))
        })
    }

    /// Resolves the staged relative path for one regex-captured folder file.
    ///
    /// Rename policy:
    /// - when regex matching yields zero capture groups, keep the full
    ///   sandbox-relative path unchanged,
    /// - when one or more capture groups are present and matched, join all
    ///   capture strings in order to produce the staged relative path.
    fn resolve_regex_capture_renamed_path(
        &self,
        relative_file: &Path,
        path_regex: &Regex,
        pattern: &str,
    ) -> Result<PathBuf, ConductorError> {
        let normalized_relative = Self::normalize_relative_path_for_regex(relative_file);
        let Some(captures) = path_regex.captures(&normalized_relative) else {
            return Ok(relative_file.to_path_buf());
        };

        let capture_parts =
            captures.iter().skip(1).flatten().map(|capture| capture.as_str()).collect::<Vec<_>>();

        if capture_parts.is_empty() {
            return Ok(relative_file.to_path_buf());
        }

        let renamed_relative = capture_parts.join("");
        if renamed_relative.trim().is_empty() {
            return Err(ConductorError::Workflow(format!(
                "capturing declared folder output by regex '{pattern}' produced an empty renamed path for '{normalized_relative}'"
            )));
        }

        self.normalized_relative_tool_path(&renamed_relative, "output folder regex capture rename")
    }

    /// Collects sandbox-relative file and directory candidates for regex output
    /// capture matching.
    #[expect(
        clippy::self_only_used_in_recursion,
        reason = "recursive traversal helper remains instance-scoped for consistency with sibling capture helpers"
    )]
    fn collect_capture_candidate_paths(
        &self,
        root_dir: &Path,
        scan_dir: &Path,
        relative_files: &mut BTreeSet<PathBuf>,
        relative_dirs: &mut BTreeSet<PathBuf>,
    ) -> Result<(), ConductorError> {
        if !scan_dir.exists() {
            return Ok(());
        }

        let entries = std::fs::read_dir(scan_dir).map_err(|source| ConductorError::Io {
            operation: "reading output-capture candidate directory".to_string(),
            path: scan_dir.to_path_buf(),
            source,
        })?;

        for entry in entries {
            let entry = entry.map_err(|source| ConductorError::Io {
                operation: "iterating output-capture candidate directory".to_string(),
                path: scan_dir.to_path_buf(),
                source,
            })?;
            let path = entry.path();
            let file_type = entry.file_type().map_err(|source| ConductorError::Io {
                operation: "reading output-capture candidate type".to_string(),
                path: path.clone(),
                source,
            })?;

            let relative = path.strip_prefix(root_dir).map_err(|err| {
                ConductorError::Internal(format!(
                    "failed deriving sandbox-relative output-capture path for '{}': {err}",
                    path.display()
                ))
            })?;
            let relative_path = relative.to_path_buf();

            if file_type.is_dir() {
                relative_dirs.insert(relative_path);
                self.collect_capture_candidate_paths(
                    root_dir,
                    &path,
                    relative_files,
                    relative_dirs,
                )?;
                continue;
            }

            if file_type.is_file() {
                relative_files.insert(relative_path);
            }
        }

        Ok(())
    }

    /// Normalizes one relative path to forward-slash form for regex matching.
    fn normalize_relative_path_for_regex(path: &Path) -> String {
        path.components()
            .filter_map(|component| match component {
                Component::Normal(part) => Some(part.to_string_lossy().into_owned()),
                _ => None,
            })
            .collect::<Vec<_>>()
            .join("/")
    }

    /// Applies the full-save CAS hint for outputs that must keep a complete
    /// base snapshot.
    async fn apply_full_save_hint(&self, target_hash: Hash) -> Result<(), ConductorError> {
        let empty_hash = empty_content_hash();
        if target_hash == empty_hash {
            return Ok(());
        }
        let bases = BTreeSet::from([empty_hash]);
        self.cas.set_constraint(Constraint { target_hash, potential_bases: bases }).await?;
        Ok(())
    }

    /// Applies reverse-diff hints from each source input toward the produced output.
    ///
    /// The canonical empty-content root hash is intentionally skipped because
    /// CAS constraint rules do not allow explicit base sets on that root node.
    async fn apply_reverse_diff_hints(
        &self,
        output_hash: Hash,
        inputs: &BTreeMap<String, ResolvedInput>,
    ) -> Result<(), ConductorError> {
        let empty_hash = empty_content_hash();
        for input_hash in inputs.values().map(|input| input.hash) {
            if input_hash == output_hash || input_hash == empty_hash {
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
pub(crate) async fn spawn_step_worker_pool<C>(
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
pub(crate) async fn execute_step_direct<C>(
    cas: Arc<C>,
    request: StepExecutionRequest,
) -> Result<StepExecutionBundle, ConductorError>
where
    C: CasApi + Send + Sync + 'static,
{
    StepWorkerExecutor { cas, conductor_tmp_dir: request.conductor_tmp_dir.clone() }
        .execute_step(request)
        .await
}

#[cfg(test)]
mod tests {
    //! Step-worker tests for template semantics, builtin dispatch contracts, and
    //! output-capture behavior for the actor-backed step worker.

    use std::collections::{BTreeMap, BTreeSet};
    use std::sync::Arc;
    use std::time::Duration;

    use mediapm_cas::{CasApi, InMemoryCas, empty_content_hash};
    use regex::Regex;

    use crate::error::ConductorError;
    use crate::model::config::{
        ExternalContentRef, ImpureTimestamp, InputBinding, ProcessSpec, ToolInputKind,
        ToolInputSpec, ToolKindSpec, ToolOutputSpec, ToolSpec, WorkflowStepSpec,
    };
    use crate::model::state::{OutputSaveMode, PersistenceFlags, ResolvedInput};
    use crate::orchestration::protocol::{UnifiedNickelDocument, UnifiedToolSpec};

    use super::{
        ResolvedOutputCapture, ResolvedOutputSpec, ResolvedProcessExecution, StepWorkerExecutor,
        ToolExecutionCapture,
    };

    /// Builds one minimal executable-process descriptor for helper invocations.
    fn test_executable_process(executable: &str) -> ResolvedProcessExecution {
        ResolvedProcessExecution::Executable {
            executable: executable.to_string(),
            args: Vec::new(),
            env_vars: BTreeMap::new(),
            success_codes: BTreeSet::from([0]),
        }
    }

    /// Returns the host platform directory label used by managed tool payloads.
    fn host_payload_platform_dir() -> &'static str {
        #[cfg(target_os = "macos")]
        {
            "macos"
        }
        #[cfg(target_os = "linux")]
        {
            "linux"
        }
        #[cfg(target_os = "windows")]
        {
            "windows"
        }
        #[cfg(not(any(target_os = "macos", target_os = "linux", target_os = "windows")))]
        {
            "host"
        }
    }

    /// Builds one ZIP payload for template-selector tests.
    fn build_test_zip_payload(entry_relative_path: &str, entry_content: &[u8]) -> Vec<u8> {
        let temp = tempfile::tempdir().expect("tempdir");
        let source_dir = temp.path().join("source");
        let source_file = source_dir.join(entry_relative_path);
        if let Some(parent) = source_file.parent() {
            std::fs::create_dir_all(parent).expect("create zip source parent");
        }
        std::fs::write(&source_file, entry_content).expect("write zip source file");

        mediapm_conductor_builtin_archive::pack_directory_to_uncompressed_zip_bytes(
            &source_dir,
            false,
        )
        .expect("build test zip payload")
    }

    /// Protects deterministic instance-key derivation across repeated calls.
    #[test]
    fn derived_keys_are_deterministic() {
        let metadata = ToolSpec {
            is_impure: false,
            kind: ToolKindSpec::Executable {
                command: vec!["bin/tool".to_string(), "a".to_string(), "1".to_string()],
                env_vars: BTreeMap::from([("RUST_LOG".to_string(), "info".to_string())]),
                success_codes: vec![0],
            },
            ..ToolSpec::default()
        };
        let inputs = BTreeMap::from([(
            "input".to_string(),
            ResolvedInput::from_plain_content(b"abc".to_vec()),
        )]);

        let impure_timestamp = Some(ImpureTimestamp { epoch_seconds: 12, subsec_nanos: 34 });
        let key_a = StepWorkerExecutor::<InMemoryCas>::derive_instance_key(
            "echo@1.0.0",
            &metadata,
            impure_timestamp,
            &inputs,
        )
        .expect("derive instance key");
        let key_b = StepWorkerExecutor::<InMemoryCas>::derive_instance_key(
            "echo@1.0.0",
            &metadata,
            impure_timestamp,
            &inputs,
        )
        .expect("derive instance key");
        assert_eq!(key_a, key_b);
    }

    /// Protects tool-name participation in deterministic instance-key derivation.
    #[test]
    fn derived_keys_include_tool_name() {
        let metadata = ToolSpec {
            kind: ToolKindSpec::Builtin { name: "echo".to_string(), version: "1.0.0".to_string() },
            ..ToolSpec::default()
        };
        let inputs = BTreeMap::from([(
            "text".to_string(),
            ResolvedInput::from_plain_content(b"abc".to_vec()),
        )]);

        let key_a = StepWorkerExecutor::<InMemoryCas>::derive_instance_key(
            "echo@1.0.0",
            &metadata,
            None,
            &inputs,
        )
        .expect("derive key for first tool name");
        let key_b = StepWorkerExecutor::<InMemoryCas>::derive_instance_key(
            "echo@2.0.0",
            &metadata,
            None,
            &inputs,
        )
        .expect("derive key for second tool name");

        assert_ne!(key_a, key_b);
    }

    /// Protects builtin identity-only metadata projection in instance-key derivation.
    #[test]
    fn derived_keys_for_builtin_ignore_non_identity_metadata_fields() {
        let builtin_minimal = ToolSpec {
            is_impure: false,
            inputs: BTreeMap::new(),
            kind: ToolKindSpec::Builtin { name: "echo".to_string(), version: "1.0.0".to_string() },
            outputs: BTreeMap::new(),
        };
        let builtin_verbose = ToolSpec {
            is_impure: true,
            inputs: BTreeMap::from([("text".to_string(), ToolInputSpec::default())]),
            kind: ToolKindSpec::Builtin { name: "echo".to_string(), version: "1.0.0".to_string() },
            outputs: BTreeMap::from([("result".to_string(), ToolOutputSpec::default())]),
        };
        let inputs = BTreeMap::from([(
            "text".to_string(),
            ResolvedInput::from_plain_content(b"abc".to_vec()),
        )]);

        let minimal_key = StepWorkerExecutor::<InMemoryCas>::derive_instance_key(
            "echo@1.0.0",
            &builtin_minimal,
            None,
            &inputs,
        )
        .expect("derive minimal builtin key");
        let verbose_key = StepWorkerExecutor::<InMemoryCas>::derive_instance_key(
            "echo@1.0.0",
            &builtin_verbose,
            None,
            &inputs,
        )
        .expect("derive verbose builtin key");

        assert_eq!(minimal_key, verbose_key);
    }

    /// Protects support for bare-identifier template interpolation.
    #[test]
    fn template_interpolation_supports_bare_identifier() {
        let executor = StepWorkerExecutor {
            cas: Arc::new(InMemoryCas::new()),
            conductor_tmp_dir: std::env::temp_dir(),
        };
        let inputs = BTreeMap::from([(
            "subject".to_string(),
            ResolvedInput::from_plain_content(b"world".to_vec()),
        )]);
        let mut pending_file_writes = Vec::new();

        let rendered = executor
            .render_template_value("hello ${subject}", &inputs, &mut pending_file_writes)
            .expect("bare identifier interpolation should resolve");

        assert_eq!(rendered, "hello world");
        assert!(pending_file_writes.is_empty());
    }

    /// Protects support for JavaScript-style bracket selector interpolation.
    #[test]
    fn template_interpolation_supports_inputs_bracket_notation() {
        let executor = StepWorkerExecutor {
            cas: Arc::new(InMemoryCas::new()),
            conductor_tmp_dir: std::env::temp_dir(),
        };
        let inputs = BTreeMap::from([(
            "subject".to_string(),
            ResolvedInput::from_plain_content(b"world".to_vec()),
        )]);
        let mut pending_file_writes = Vec::new();

        let rendered = executor
            .render_template_value(
                "hello ${inputs[\"subject\"]}",
                &inputs,
                &mut pending_file_writes,
            )
            .expect("bracket interpolation should resolve");

        assert_eq!(rendered, "hello world");
        assert!(pending_file_writes.is_empty());
    }

    /// Protects policy that unsupported context selectors are rejected.
    #[test]
    fn template_interpolation_rejects_context_selector() {
        let executor = StepWorkerExecutor {
            cas: Arc::new(InMemoryCas::new()),
            conductor_tmp_dir: std::env::temp_dir(),
        };
        let inputs = BTreeMap::new();
        let mut pending_file_writes = Vec::new();

        let error = executor
            .render_template_value(
                "${context.config_dir}/notes/report.txt",
                &inputs,
                &mut pending_file_writes,
            )
            .expect_err("context selector should fail");

        match error {
            ConductorError::Workflow(message) => {
                assert!(message.contains("unsupported template expression"));
            }
            other => panic!("expected workflow error, got {other:?}"),
        }
    }

    /// Protects explicit failure for removed `${context.config_dir}` input-binding syntax.
    #[tokio::test]
    async fn resolve_input_binding_rejects_context_config_dir() {
        let executor = StepWorkerExecutor {
            cas: Arc::new(InMemoryCas::new()),
            conductor_tmp_dir: std::env::temp_dir(),
        };
        let workflow_step = WorkflowStepSpec {
            id: "step".to_string(),
            tool: "echo@1.0.0".to_string(),
            inputs: BTreeMap::new(),
            depends_on: Vec::new(),
            outputs: BTreeMap::new(),
        };
        let unified = UnifiedNickelDocument {
            external_data: BTreeMap::new(),
            tools: BTreeMap::new(),
            workflows: BTreeMap::new(),
            tool_content_hashes: BTreeSet::new(),
        };
        let error = executor
            .resolve_input_binding(
                &unified,
                "wf",
                &workflow_step,
                "${context.config_dir}",
                &BTreeMap::new(),
            )
            .await
            .expect_err("context.config_dir binding should fail");

        match error {
            ConductorError::Workflow(message) => {
                assert!(message.contains("unsupported input binding expression"));
                assert!(message.contains("context.config_dir"));
            }
            other => panic!("expected workflow error, got {other:?}"),
        }
    }

    /// Protects `${env.<VAR_NAME>}` input-binding handling by expanding the
    /// environment value during input resolution.
    #[tokio::test]
    async fn resolve_input_binding_expands_env_placeholder_segments() {
        let executor = StepWorkerExecutor {
            cas: Arc::new(InMemoryCas::new()),
            conductor_tmp_dir: std::env::temp_dir(),
        };
        let workflow_step = WorkflowStepSpec {
            id: "step".to_string(),
            tool: "echo@1.0.0".to_string(),
            inputs: BTreeMap::new(),
            depends_on: Vec::new(),
            outputs: BTreeMap::new(),
        };
        let unified = UnifiedNickelDocument {
            external_data: BTreeMap::new(),
            tools: BTreeMap::new(),
            workflows: BTreeMap::new(),
            tool_content_hashes: BTreeSet::new(),
        };
        let path = std::env::var("PATH").expect("PATH available in test environment");

        let resolved = executor
            .resolve_input_binding(
                &unified,
                "wf",
                &workflow_step,
                "prefix-${env.PATH}/bin",
                &BTreeMap::new(),
            )
            .await
            .expect("env placeholder binding should resolve");

        assert_eq!(resolved.plain_content, format!("prefix-{path}/bin").into_bytes());
    }

    /// Protects explicit failure on unsupported expression syntax.
    #[test]
    fn template_interpolation_rejects_unsupported_expression() {
        let executor = StepWorkerExecutor {
            cas: Arc::new(InMemoryCas::new()),
            conductor_tmp_dir: std::env::temp_dir(),
        };
        let inputs = BTreeMap::new();
        let mut pending_file_writes = Vec::new();

        let error = executor
            .render_template_value("${foo.bar}", &inputs, &mut pending_file_writes)
            .expect_err("unsupported expression should fail");

        match error {
            ConductorError::Workflow(message) => {
                assert!(message.contains("unsupported template expression"));
            }
            other => panic!("expected workflow error, got {other:?}"),
        }
    }

    /// Protects literal escaping of `\${...}` markers.
    #[test]
    fn template_interpolation_supports_js_escape_for_literal_start() {
        let executor = StepWorkerExecutor {
            cas: Arc::new(InMemoryCas::new()),
            conductor_tmp_dir: std::env::temp_dir(),
        };
        let inputs = BTreeMap::from([(
            "subject".to_string(),
            ResolvedInput::from_plain_content(b"world".to_vec()),
        )]);
        let mut pending_file_writes = Vec::new();

        let rendered = executor
            .render_template_value(
                r"show \${subject} and ${subject}",
                &inputs,
                &mut pending_file_writes,
            )
            .expect("escaped interpolation marker should render literally");

        assert_eq!(rendered, "show ${subject} and world");
        assert!(pending_file_writes.is_empty());
    }

    /// Protects `${<left> <op> <right>?<true>|<false>}` conditional semantics.
    #[test]
    fn template_interpolation_supports_comparison_conditional_expression() {
        let executor = StepWorkerExecutor {
            cas: Arc::new(InMemoryCas::new()),
            conductor_tmp_dir: std::env::temp_dir(),
        };
        let inputs = BTreeMap::new();
        let mut pending_file_writes = Vec::new();

        let current = "${context.os == \"windows\" ? win | nonwin}";

        let rendered = executor
            .render_template_value(current, &inputs, &mut pending_file_writes)
            .expect("comparison conditional should render selected branch");

        let expected = if cfg!(windows) { "win" } else { "nonwin" };
        assert_eq!(rendered, expected);
        assert!(pending_file_writes.is_empty());
    }

    /// Protects truthiness conditional semantics for `${<operand>?<true>|<false>}`
    /// template expressions.
    #[test]
    fn template_interpolation_supports_truthy_conditional_expression() {
        let executor = StepWorkerExecutor {
            cas: Arc::new(InMemoryCas::new()),
            conductor_tmp_dir: std::env::temp_dir(),
        };
        let inputs = BTreeMap::from([(
            "subject".to_string(),
            ResolvedInput::from_plain_content(b"world".to_vec()),
        )]);
        let mut pending_file_writes = Vec::new();

        let rendered = executor
            .render_template_value(
                "${inputs.subject ? has-subject | no-subject}",
                &inputs,
                &mut pending_file_writes,
            )
            .expect("truthy conditional should render selected branch");

        assert_eq!(rendered, "has-subject");
        assert!(pending_file_writes.is_empty());
    }

    /// Protects comparison conditional support for single-item list inputs used by
    /// value-centric option transport.
    #[test]
    fn template_interpolation_supports_comparison_against_single_item_list_input() {
        let executor = StepWorkerExecutor {
            cas: Arc::new(InMemoryCas::new()),
            conductor_tmp_dir: std::env::temp_dir(),
        };
        let inputs = BTreeMap::from([(
            "switch".to_string(),
            ResolvedInput::from_string_list(vec!["true".to_string()]).expect("build list input"),
        )]);
        let mut pending_file_writes = Vec::new();

        let rendered = executor
            .render_template_value(
                "${inputs.switch == \"true\" ? enabled | disabled}",
                &inputs,
                &mut pending_file_writes,
            )
            .expect("single-item list comparison should render selected branch");

        assert_eq!(rendered, "enabled");
        assert!(pending_file_writes.is_empty());
    }

    /// Protects deterministic errors for ambiguous comparison operands sourced
    /// from multi-item list inputs.
    #[test]
    fn template_interpolation_rejects_comparison_against_multi_item_list_input() {
        let executor = StepWorkerExecutor {
            cas: Arc::new(InMemoryCas::new()),
            conductor_tmp_dir: std::env::temp_dir(),
        };
        let inputs = BTreeMap::from([(
            "switch".to_string(),
            ResolvedInput::from_string_list(vec!["true".to_string(), "false".to_string()])
                .expect("build list input"),
        )]);
        let mut pending_file_writes = Vec::new();

        let error = executor
            .render_template_value(
                "${inputs.switch == \"true\" ? enabled | disabled}",
                &inputs,
                &mut pending_file_writes,
            )
            .expect_err("multi-item list comparison should fail");

        assert!(
            format!("{error}").contains("comparisons support at most one list item"),
            "error should describe list-size comparison constraint"
        );
    }

    /// Protects recursive selector/materialization support inside comparison
    /// values.
    #[test]
    fn template_interpolation_supports_special_forms_inside_conditional_branches() {
        let executor = StepWorkerExecutor {
            cas: Arc::new(InMemoryCas::new()),
            conductor_tmp_dir: std::env::temp_dir(),
        };
        let inputs = BTreeMap::from([(
            "subject".to_string(),
            ResolvedInput::from_plain_content(b"world".to_vec()),
        )]);
        let mut pending_file_writes = Vec::new();

        let current = "${context.os == \"windows\" ? subject:file(runtime/windows-subject.txt) | subject:file(runtime/other-subject.txt)}";

        let rendered = executor
            .render_template_value(current, &inputs, &mut pending_file_writes)
            .expect("matching conditional should resolve special-form value recursively");

        let expected_path =
            if cfg!(windows) { "runtime/windows-subject.txt" } else { "runtime/other-subject.txt" };
        assert_eq!(rendered.replace('\\', "/"), expected_path);
        assert_eq!(pending_file_writes.len(), 1);
        assert_eq!(
            pending_file_writes[0].relative_path.to_string_lossy().replace('\\', "/"),
            expected_path
        );
        assert_eq!(pending_file_writes[0].plain_content, b"world".to_vec());
    }

    /// Protects logical `&&` operator in conditional expressions.
    #[test]
    fn template_interpolation_supports_and_operator_in_condition() {
        let executor = StepWorkerExecutor {
            cas: Arc::new(InMemoryCas::new()),
            conductor_tmp_dir: std::env::temp_dir(),
        };
        let inputs = BTreeMap::from([
            ("a".to_string(), ResolvedInput::from_plain_content(b"x".to_vec())),
            ("b".to_string(), ResolvedInput::from_plain_content(b"y".to_vec())),
        ]);
        let mut pending_file_writes = Vec::new();

        // Both sides true → true branch.
        let rendered = executor
            .render_template_value(
                "${a == \"x\" && b == \"y\" ? true | false}",
                &inputs,
                &mut pending_file_writes,
            )
            .expect("and-condition should render selected branch");
        assert_eq!(rendered, "true");

        // One side false → false branch.
        let rendered_false = executor
            .render_template_value(
                "${a == \"x\" && b == \"z\" ? true | false}",
                &inputs,
                &mut pending_file_writes,
            )
            .expect("and-condition with false rhs should render false branch");
        assert_eq!(rendered_false, "false");
    }

    /// Protects logical `||` operator in conditional expressions.
    #[test]
    fn template_interpolation_supports_or_operator_in_condition() {
        let executor = StepWorkerExecutor {
            cas: Arc::new(InMemoryCas::new()),
            conductor_tmp_dir: std::env::temp_dir(),
        };
        let inputs = BTreeMap::from([
            ("a".to_string(), ResolvedInput::from_plain_content(b"x".to_vec())),
            ("b".to_string(), ResolvedInput::from_plain_content(b"y".to_vec())),
        ]);
        let mut pending_file_writes = Vec::new();

        // First side true → true branch.
        let rendered = executor
            .render_template_value(
                "${a == \"x\" || b == \"z\" ? true | false}",
                &inputs,
                &mut pending_file_writes,
            )
            .expect("or-condition with true lhs should render true branch");
        assert_eq!(rendered, "true");

        // Both sides false → false branch.
        let rendered_false = executor
            .render_template_value(
                "${a == \"nope\" || b == \"nope\" ? true | false}",
                &inputs,
                &mut pending_file_writes,
            )
            .expect("or-condition with both false should render false branch");
        assert_eq!(rendered_false, "false");
    }

    /// Protects mixed `||`, `&&`, and parentheses grouping in conditional expressions.
    #[test]
    fn template_interpolation_supports_mixed_logical_operators_and_parentheses() {
        let executor = StepWorkerExecutor {
            cas: Arc::new(InMemoryCas::new()),
            conductor_tmp_dir: std::env::temp_dir(),
        };
        let inputs = BTreeMap::from([
            ("a".to_string(), ResolvedInput::from_plain_content(b"x".to_vec())),
            ("b".to_string(), ResolvedInput::from_plain_content(b"y".to_vec())),
            ("c".to_string(), ResolvedInput::from_plain_content(b"set".to_vec())),
        ]);
        let mut pending_file_writes = Vec::new();

        // (a == "x" || b == "y") && c → (true || false) && true → true.
        let rendered = executor
            .render_template_value(
                "${(a == \"x\" || b == \"z\") && c ? true | false}",
                &inputs,
                &mut pending_file_writes,
            )
            .expect("mixed condition should render selected branch");
        assert_eq!(rendered, "true");

        // (a == "nope" || b == "nope") && c → false && true → false.
        let rendered_false = executor
            .render_template_value(
                "${(a == \"nope\" || b == \"nope\") && c ? true | false}",
                &inputs,
                &mut pending_file_writes,
            )
            .expect("mixed condition with false group should render false branch");
        assert_eq!(rendered_false, "false");
    }

    /// Protects negation `!` applied to a comparison expression via parentheses.
    #[test]
    fn template_interpolation_supports_negation_of_comparison() {
        let executor = StepWorkerExecutor {
            cas: Arc::new(InMemoryCas::new()),
            conductor_tmp_dir: std::env::temp_dir(),
        };
        let inputs =
            BTreeMap::from([("a".to_string(), ResolvedInput::from_plain_content(b"x".to_vec()))]);
        let mut pending_file_writes = Vec::new();

        // !(a == "x") → !(true) → false → selects false branch.
        let rendered = executor
            .render_template_value(
                "${!(a == \"x\") ? true | false}",
                &inputs,
                &mut pending_file_writes,
            )
            .expect("negated comparison should render selected branch");
        assert_eq!(rendered, "false");

        // !(a == "nope") → !(false) → true → selects true branch.
        let rendered_true = executor
            .render_template_value(
                "${!(a == \"nope\") ? true | false}",
                &inputs,
                &mut pending_file_writes,
            )
            .expect("negated false comparison should render true branch");
        assert_eq!(rendered_true, "true");
    }

    /// Protects omission of conditional branches that resolve to empty output.
    #[test]
    fn command_render_omits_conditionals_with_empty_false_branch() {
        let executor = StepWorkerExecutor {
            cas: Arc::new(InMemoryCas::new()),
            conductor_tmp_dir: std::env::temp_dir(),
        };
        let inputs = BTreeMap::new();
        let mut pending_file_writes = Vec::new();

        let non_matching = if cfg!(windows) {
            "${context.os == \"linux\" ? --linux-only | ''}".to_string()
        } else {
            "${context.os == \"windows\" ? --windows-only | ''}".to_string()
        };
        let command = vec!["tool".to_string(), non_matching, "--always".to_string()];

        let rendered = executor
            .render_template_command(&command, &inputs, &mut pending_file_writes)
            .expect("command rendering should succeed");

        assert_eq!(rendered, vec!["tool".to_string(), "--always".to_string()]);
    }

    /// Protects standalone command unpack token expansion for list-of-strings
    /// inputs.
    #[test]
    fn command_render_expands_standalone_unpack_token_for_list_input() {
        let executor = StepWorkerExecutor {
            cas: Arc::new(InMemoryCas::new()),
            conductor_tmp_dir: std::env::temp_dir(),
        };
        let inputs = BTreeMap::from([(
            "argv".to_string(),
            ResolvedInput::from_string_list(vec!["--alpha".to_string(), "--beta".to_string()])
                .expect("build list input"),
        )]);
        let mut pending_file_writes = Vec::new();

        let command = vec!["tool".to_string(), "${*inputs.argv}".to_string(), "--tail".to_string()];
        let rendered = executor
            .render_template_command(&command, &inputs, &mut pending_file_writes)
            .expect("command unpack token should expand");

        assert_eq!(
            rendered,
            vec![
                "tool".to_string(),
                "--alpha".to_string(),
                "--beta".to_string(),
                "--tail".to_string(),
            ]
        );
    }

    /// Protects scalar unpack behavior for standalone command tokens.
    #[test]
    fn command_render_expands_unpack_token_for_scalar_input() {
        let executor = StepWorkerExecutor {
            cas: Arc::new(InMemoryCas::new()),
            conductor_tmp_dir: std::env::temp_dir(),
        };
        let inputs = BTreeMap::from([(
            "argv".to_string(),
            ResolvedInput::from_plain_content(b"--single".to_vec()),
        )]);
        let mut pending_file_writes = Vec::new();

        let rendered = executor
            .render_template_command(
                &["tool".to_string(), "${*inputs.argv}".to_string(), "--tail".to_string()],
                &inputs,
                &mut pending_file_writes,
            )
            .expect("scalar unpack token should expand to one argument");

        assert_eq!(
            rendered,
            vec!["tool".to_string(), "--single".to_string(), "--tail".to_string()]
        );
    }

    /// Protects conditional unpack support so command templates can include
    /// optional key/value argv pairs without mediapm-specific preprocessing.
    #[test]
    fn command_render_supports_conditional_unpack_key_value_pair() {
        let executor = StepWorkerExecutor {
            cas: Arc::new(InMemoryCas::new()),
            conductor_tmp_dir: std::env::temp_dir(),
        };
        let mut pending_file_writes = Vec::new();

        let with_value_inputs = BTreeMap::from([(
            "test".to_string(),
            ResolvedInput::from_plain_content(b"alpha".to_vec()),
        )]);
        let command = vec![
            "tool".to_string(),
            "${*inputs.test ? --test | ''}".to_string(),
            "${*inputs.test}".to_string(),
        ];
        let rendered_with_value = executor
            .render_template_command(&command, &with_value_inputs, &mut pending_file_writes)
            .expect(
                "conditional unpack command should render key/value pair when value is present",
            );
        assert_eq!(
            rendered_with_value,
            vec!["tool".to_string(), "--test".to_string(), "alpha".to_string()]
        );

        let empty_value_inputs =
            BTreeMap::from([("test".to_string(), ResolvedInput::from_plain_content(Vec::new()))]);
        let rendered_without_value = executor
            .render_template_command(&command, &empty_value_inputs, &mut pending_file_writes)
            .expect("conditional unpack command should omit key/value pair when value is empty");
        assert_eq!(rendered_without_value, vec!["tool".to_string()]);
    }

    /// Protects standalone conditional-unpack semantics for list-valued inputs so
    /// `${*...}` remains compatible with both scalar and list bindings.
    #[test]
    fn command_render_supports_conditional_unpack_with_list_input() {
        let executor = StepWorkerExecutor {
            cas: Arc::new(InMemoryCas::new()),
            conductor_tmp_dir: std::env::temp_dir(),
        };
        let mut pending_file_writes = Vec::new();
        let command = vec![
            "tool".to_string(),
            "${*inputs.argv ? --has-args | ''}".to_string(),
            "${*inputs.argv}".to_string(),
        ];

        let list_inputs = BTreeMap::from([(
            "argv".to_string(),
            ResolvedInput::from_string_list(vec!["--alpha".to_string(), "--beta".to_string()])
                .expect("build list input"),
        )]);
        let rendered_with_list = executor
            .render_template_command(&command, &list_inputs, &mut pending_file_writes)
            .expect("conditional unpack should render list flag and unpacked list values");
        assert_eq!(
            rendered_with_list,
            vec![
                "tool".to_string(),
                "--has-args".to_string(),
                "--alpha".to_string(),
                "--beta".to_string(),
            ]
        );

        let empty_list_inputs = BTreeMap::from([(
            "argv".to_string(),
            ResolvedInput::from_string_list(Vec::new()).expect("build empty list input"),
        )]);
        let rendered_without_list = executor
            .render_template_command(&command, &empty_list_inputs, &mut pending_file_writes)
            .expect("conditional unpack should omit list flag and values when list input is empty");
        assert_eq!(rendered_without_list, vec!["tool".to_string()]);
    }

    /// Protects plain `context.os` selector rendering.
    #[test]
    fn template_interpolation_supports_context_os_selector() {
        let executor = StepWorkerExecutor {
            cas: Arc::new(InMemoryCas::new()),
            conductor_tmp_dir: std::env::temp_dir(),
        };
        let inputs = BTreeMap::new();
        let mut pending_file_writes = Vec::new();

        let rendered = executor
            .render_template_value("${context.os}", &inputs, &mut pending_file_writes)
            .expect("context selector should render");

        let expected = if cfg!(windows) {
            "windows"
        } else if cfg!(target_os = "macos") {
            "macos"
        } else {
            "linux"
        };
        assert_eq!(rendered, expected);
    }

    /// Protects plain `context.working_directory` selector rendering.
    #[test]
    fn template_interpolation_supports_context_working_directory_selector() {
        let executor = StepWorkerExecutor {
            cas: Arc::new(InMemoryCas::new()),
            conductor_tmp_dir: std::env::temp_dir(),
        };
        let inputs = BTreeMap::new();
        let mut pending_file_writes = Vec::new();

        let rendered = executor
            .render_template_value(
                "${context.working_directory}",
                &inputs,
                &mut pending_file_writes,
            )
            .expect("context working-directory selector should render");

        let expected = std::env::current_dir().expect("current directory");
        assert_eq!(rendered, expected.to_string_lossy());
    }

    /// Protects syntax rule that `${*...}` unpack expressions must occupy the
    /// entire command argument (standalone token only).
    #[test]
    fn command_render_rejects_non_standalone_unpack_expression() {
        let executor = StepWorkerExecutor {
            cas: Arc::new(InMemoryCas::new()),
            conductor_tmp_dir: std::env::temp_dir(),
        };
        let inputs = BTreeMap::from([(
            "argv".to_string(),
            ResolvedInput::from_string_list(vec!["--alpha".to_string()]).expect("build list input"),
        )]);
        let mut pending_file_writes = Vec::new();

        let error = executor
            .render_template_command(
                &["tool".to_string(), "prefix-${*inputs.argv}".to_string()],
                &inputs,
                &mut pending_file_writes,
            )
            .expect_err("non-standalone unpack expression should fail");

        match error {
            ConductorError::Workflow(message) => {
                assert!(message.contains("only valid as a standalone executable command argument"));
                assert!(message.contains("${*inputs.argv}"));
            }
            other => panic!("expected workflow error, got {other:?}"),
        }
    }

    /// Protects policy that list-typed inputs are invalid in normal `${...}`
    /// interpolation and must use standalone unpack tokens.
    #[test]
    fn template_interpolation_rejects_list_input_outside_unpack_token() {
        let executor = StepWorkerExecutor {
            cas: Arc::new(InMemoryCas::new()),
            conductor_tmp_dir: std::env::temp_dir(),
        };
        let inputs = BTreeMap::from([(
            "argv".to_string(),
            ResolvedInput::from_string_list(vec!["--alpha".to_string()]).expect("build list input"),
        )]);
        let mut pending_file_writes = Vec::new();

        let error = executor
            .render_template_value("prefix-${inputs.argv}", &inputs, &mut pending_file_writes)
            .expect_err("list interpolation outside unpack token should fail");

        match error {
            ConductorError::Workflow(message) => {
                assert!(
                    message
                        .contains("list inputs are only valid in standalone command unpack tokens")
                );
                assert!(message.contains("inputs.argv"));
            }
            other => panic!("expected workflow error, got {other:?}"),
        }
    }

    /// Protects runtime executable input-kind validation when programmatic callers
    /// bypass config decoding helpers.
    #[tokio::test]
    async fn resolve_inputs_rejects_executable_input_kind_mismatch() {
        let executor = StepWorkerExecutor {
            cas: Arc::new(InMemoryCas::new()),
            conductor_tmp_dir: std::env::temp_dir(),
        };
        let unified = UnifiedNickelDocument {
            external_data: BTreeMap::new(),
            tools: BTreeMap::new(),
            workflows: BTreeMap::new(),
            tool_content_hashes: BTreeSet::new(),
        };
        let tool = UnifiedToolSpec {
            is_impure: false,
            max_concurrent_calls: -1,
            max_retries: 0,
            inputs: BTreeMap::from([(
                "argv".to_string(),
                ToolInputSpec { kind: ToolInputKind::StringList },
            )]),
            default_inputs: BTreeMap::new(),
            process: ProcessSpec::Executable {
                command: vec!["bin/tool".to_string(), "${*inputs.argv}".to_string()],
                env_vars: BTreeMap::new(),
                success_codes: vec![0],
            },
            execution_env_vars: BTreeMap::new(),
            outputs: BTreeMap::from([("result".to_string(), ToolOutputSpec::default())]),
            tool_content_map: BTreeMap::new(),
        };
        let step = WorkflowStepSpec {
            id: "step".to_string(),
            tool: "tool_exec@1.0.0".to_string(),
            inputs: BTreeMap::from([(
                "argv".to_string(),
                InputBinding::String("--scalar-value".to_string()),
            )]),
            depends_on: Vec::new(),
            outputs: BTreeMap::new(),
        };

        let error = executor
            .resolve_inputs(&unified, &tool, "wf", &step, &BTreeMap::new())
            .await
            .expect_err("kind mismatch should fail");

        match error {
            ConductorError::Workflow(message) => {
                assert!(message.contains("expects kind 'string_list'"));
                assert!(message.contains("received 'string'"));
            }
            other => panic!("expected workflow error, got {other:?}"),
        }
    }

    /// Protects JavaScript-style escape decoding in literal template spans.
    #[test]
    fn template_interpolation_applies_js_string_escapes() {
        let executor = StepWorkerExecutor {
            cas: Arc::new(InMemoryCas::new()),
            conductor_tmp_dir: std::env::temp_dir(),
        };
        let inputs = BTreeMap::from([(
            "subject".to_string(),
            ResolvedInput::from_plain_content(b"world".to_vec()),
        )]);
        let mut pending_file_writes = Vec::new();

        let rendered = executor
            .render_template_value(r"line1\nline2 ${subject}", &inputs, &mut pending_file_writes)
            .expect("js escape sequences should decode");

        assert_eq!(rendered, "line1\nline2 world");
        assert!(pending_file_writes.is_empty());
    }

    /// Protects explicit failure on unsupported escape sequences.
    #[test]
    fn template_interpolation_rejects_unsupported_escape_sequence() {
        let executor = StepWorkerExecutor {
            cas: Arc::new(InMemoryCas::new()),
            conductor_tmp_dir: std::env::temp_dir(),
        };
        let inputs = BTreeMap::new();
        let mut pending_file_writes = Vec::new();

        let error = executor
            .render_template_value(r"bad\q", &inputs, &mut pending_file_writes)
            .expect_err("unsupported escape should fail");

        match error {
            ConductorError::Workflow(message) => {
                assert!(message.contains("unsupported escape sequence"));
            }
            other => panic!("expected workflow error, got {other:?}"),
        }
    }

    /// Protects deferred file materialization during planning.
    #[test]
    fn template_file_materialization_is_deferred_until_execution() {
        let executor = StepWorkerExecutor {
            cas: Arc::new(InMemoryCas::new()),
            conductor_tmp_dir: std::env::temp_dir(),
        };
        let inputs = BTreeMap::from([(
            "subject".to_string(),
            ResolvedInput::from_plain_content(b"world".to_vec()),
        )]);
        let temp = tempfile::tempdir().expect("tempdir");
        let deferred_path = temp.path().join("runtime").join("subject.txt");
        let mut pending_file_writes = Vec::new();

        let rendered = executor
            .render_template_value(
                "${subject:file(runtime/subject.txt)}",
                &inputs,
                &mut pending_file_writes,
            )
            .expect("file materialization token should resolve");

        assert_eq!(pending_file_writes.len(), 1);
        assert_eq!(
            pending_file_writes[0].relative_path,
            std::path::PathBuf::from("runtime").join("subject.txt")
        );
        assert_eq!(pending_file_writes[0].plain_content, b"world".to_vec());
        assert!(!deferred_path.exists(), "planning should not write files before execution");
        assert!(rendered.ends_with("subject.txt"));
    }

    /// Protects ZIP-entry selector extraction into inline template text.
    #[test]
    fn template_zip_selector_extracts_zip_entry_content() {
        let executor = StepWorkerExecutor {
            cas: Arc::new(InMemoryCas::new()),
            conductor_tmp_dir: std::env::temp_dir(),
        };
        let zip_bytes = build_test_zip_payload("nested/file.txt", b"hello-from-zip");
        let inputs =
            BTreeMap::from([("archive".to_string(), ResolvedInput::from_plain_content(zip_bytes))]);
        let mut pending_file_writes = Vec::new();

        let rendered = executor
            .render_template_value(
                "${inputs.archive:zip(nested/file.txt)}",
                &inputs,
                &mut pending_file_writes,
            )
            .expect("zip selector should extract entry content");

        assert_eq!(rendered, "hello-from-zip");
        assert!(pending_file_writes.is_empty());
    }

    /// Protects ZIP-entry selector chaining with deferred file materialization.
    #[test]
    fn template_zip_selector_can_materialize_entry_to_file_path() {
        let executor = StepWorkerExecutor {
            cas: Arc::new(InMemoryCas::new()),
            conductor_tmp_dir: std::env::temp_dir(),
        };
        let zip_bytes = build_test_zip_payload("nested/file.txt", b"zip-file-content");
        let inputs =
            BTreeMap::from([("archive".to_string(), ResolvedInput::from_plain_content(zip_bytes))]);
        let mut pending_file_writes = Vec::new();

        let rendered = executor
            .render_template_value(
                "${inputs.archive:zip(nested/file.txt):file(runtime/from_zip.txt)}",
                &inputs,
                &mut pending_file_writes,
            )
            .expect("zip selector + file materialization should resolve");

        assert_eq!(rendered.replace('\\', "/"), "runtime/from_zip.txt");
        assert_eq!(pending_file_writes.len(), 1);
        assert_eq!(
            pending_file_writes[0].relative_path,
            std::path::PathBuf::from("runtime").join("from_zip.txt")
        );
        assert_eq!(pending_file_writes[0].plain_content, b"zip-file-content".to_vec());
    }

    /// Protects explicit failure when a ZIP selector resolves to a directory
    /// without opting into `:folder(...)` materialization.
    #[test]
    fn template_zip_selector_rejects_directory_without_folder_directive() {
        let executor = StepWorkerExecutor {
            cas: Arc::new(InMemoryCas::new()),
            conductor_tmp_dir: std::env::temp_dir(),
        };
        let zip_bytes = build_test_zip_payload("nested/file.txt", b"zip-dir-content");
        let inputs =
            BTreeMap::from([("archive".to_string(), ResolvedInput::from_plain_content(zip_bytes))]);
        let mut pending_file_writes = Vec::new();

        let error = executor
            .render_template_value(
                "${inputs.archive:zip(nested)}",
                &inputs,
                &mut pending_file_writes,
            )
            .expect_err("zip directory selector should require :folder(...) materialization");

        match error {
            ConductorError::Workflow(message) => {
                assert!(message.contains("resolved 'nested' to a directory"));
                assert!(message.contains(":folder(<relative_path>)"));
            }
            other => panic!("expected workflow error, got {other:?}"),
        }
        assert!(pending_file_writes.is_empty());
    }

    /// Protects deferred directory materialization when ZIP selection resolves
    /// to one directory and the token opts into `:folder(...)`.
    #[test]
    fn template_zip_selector_can_materialize_directory_to_folder_path() {
        let executor = StepWorkerExecutor {
            cas: Arc::new(InMemoryCas::new()),
            conductor_tmp_dir: std::env::temp_dir(),
        };
        let zip_bytes = build_test_zip_payload("nested/file.txt", b"zip-dir-content");
        let inputs =
            BTreeMap::from([("archive".to_string(), ResolvedInput::from_plain_content(zip_bytes))]);
        let mut pending_file_writes = Vec::new();

        let rendered = executor
            .render_template_value(
                "${inputs.archive:zip(nested):folder(runtime/from_zip)}",
                &inputs,
                &mut pending_file_writes,
            )
            .expect("zip directory selector + folder materialization should resolve");

        assert_eq!(rendered.replace('\\', "/"), "runtime/from_zip");
        assert_eq!(pending_file_writes.len(), 1);
        assert_eq!(
            pending_file_writes[0].relative_path,
            std::path::PathBuf::from("runtime").join("from_zip").join("file.txt")
        );
        assert_eq!(pending_file_writes[0].plain_content, b"zip-dir-content".to_vec());
    }

    /// Protects explicit failure when `:folder(...)` is requested for a ZIP
    /// selector that resolves to a regular file.
    #[test]
    fn template_zip_selector_folder_materialization_rejects_file_entries() {
        let executor = StepWorkerExecutor {
            cas: Arc::new(InMemoryCas::new()),
            conductor_tmp_dir: std::env::temp_dir(),
        };
        let zip_bytes = build_test_zip_payload("nested/file.txt", b"zip-file-content");
        let inputs =
            BTreeMap::from([("archive".to_string(), ResolvedInput::from_plain_content(zip_bytes))]);
        let mut pending_file_writes = Vec::new();

        let error = executor
            .render_template_value(
                "${inputs.archive:zip(nested/file.txt):folder(runtime/from_zip)}",
                &inputs,
                &mut pending_file_writes,
            )
            .expect_err("zip file selector should reject :folder(...) materialization");

        match error {
            ConductorError::Workflow(message) => {
                assert!(message.contains("resolved 'nested/file.txt' to a file"));
                assert!(message.contains("expected a directory for :folder(...)"));
            }
            other => panic!("expected workflow error, got {other:?}"),
        }
        assert!(pending_file_writes.is_empty());
    }

    /// Protects sandbox enforcement for tool-relative paths.
    #[test]
    fn tool_relative_paths_reject_absolute_and_escape_components() {
        let executor = StepWorkerExecutor {
            cas: Arc::new(InMemoryCas::new()),
            conductor_tmp_dir: std::env::temp_dir(),
        };

        let absolute = if cfg!(windows) { r"C:\\escape.txt" } else { "/escape.txt" };
        let absolute_error = executor
            .normalized_relative_tool_path(absolute, "test")
            .expect_err("absolute tool path should fail");
        match absolute_error {
            ConductorError::Workflow(message) => assert!(message.contains("must be relative")),
            other => panic!("expected workflow error, got {other:?}"),
        }

        let escape_error = executor
            .normalized_relative_tool_path("../escape.txt", "test")
            .expect_err("parent traversal should fail");
        match escape_error {
            ConductorError::Workflow(message) => {
                assert!(message.contains("must not escape the tool sandbox"));
            }
            other => panic!("expected workflow error, got {other:?}"),
        }
    }

    /// Protects regular file materialization for `content_map` entries whose keys
    /// do not end with `/` or `\\`.
    ///
    /// With the sandbox-relinking optimization the payload stays in the persistent
    /// cache directory; the assertion checks the cache path, not the sandbox.
    #[tokio::test]
    async fn content_map_file_entry_materializes_plain_file_bytes() {
        let cas = Arc::new(InMemoryCas::new());
        let payload = b"#!/usr/bin/env sh\necho from-content-map\n".to_vec();
        let hash = cas.put(payload.clone()).await.expect("store payload in CAS");
        let executor = StepWorkerExecutor { cas, conductor_tmp_dir: std::env::temp_dir() };
        let temp = tempfile::tempdir().expect("tempdir");

        let (payload_dir, _guard) = executor
            .materialize_tool_content_map(
                "test-tool",
                &BTreeMap::from([("bin/run.sh".to_string(), hash)]),
                &test_executable_process("bin/run.sh"),
                temp.path(),
                temp.path(),
            )
            .await
            .expect("file-form content_map entry should materialize bytes");
        let payload_dir = payload_dir.expect("payload dir should be returned");

        assert_eq!(
            std::fs::read(payload_dir.join("bin").join("run.sh"))
                .expect("read output from payload cache"),
            payload
        );
    }

    /// Protects the general execution-path optimization by skipping per-step
    /// recursive payload relinking when the managed-tool executable exists in the
    /// persistent payload cache.
    #[tokio::test]
    async fn content_map_skips_sandbox_relink_when_payload_executable_in_cache() {
        let cas = Arc::new(InMemoryCas::new());
        let payload = b"#!/usr/bin/env sh\necho tool\n".to_vec();
        let hash = cas.put(payload.clone()).await.expect("store payload in CAS");
        let executor = StepWorkerExecutor { cas, conductor_tmp_dir: std::env::temp_dir() };
        let temp = tempfile::tempdir().expect("tempdir");
        let runtime_tools_dir = temp.path().join("tools");

        let executable_relative = format!("{}/my-tool", host_payload_platform_dir());
        let (payload_dir, _guard) = executor
            .materialize_tool_content_map(
                "mediapm.tools.my-tool@1.0.0",
                &BTreeMap::from([(executable_relative.clone(), hash)]),
                &test_executable_process(&executable_relative),
                temp.path(),
                runtime_tools_dir.as_path(),
            )
            .await
            .expect("managed-tool payload should materialize");
        let payload_dir = payload_dir.expect("payload dir should be returned");

        assert!(
            payload_dir.join(&executable_relative).is_file(),
            "payload cache should contain the managed-tool executable"
        );
        assert!(
            !temp.path().join(&executable_relative).exists(),
            "optimization should skip recursive payload relinking into sandbox when executable is in cache"
        );
    }

    /// Protects directory materialization semantics for trailing-slash
    /// `content_map` keys where CAS payloads are ZIP archives.
    ///
    /// With the sandbox-relinking optimization the payload stays in the persistent
    /// cache directory; the assertion checks the cache path, not the sandbox.
    #[tokio::test]
    async fn content_map_directory_entry_unpacks_zip_payload() {
        let cas = Arc::new(InMemoryCas::new());
        let zip_payload = build_test_zip_payload("bin/run.sh", b"echo from zip\n");
        let hash = cas.put(zip_payload).await.expect("store zip payload in CAS");
        let executor = StepWorkerExecutor { cas, conductor_tmp_dir: std::env::temp_dir() };
        let temp = tempfile::tempdir().expect("tempdir");

        let (payload_dir, _guard) = executor
            .materialize_tool_content_map(
                "test-tool",
                &BTreeMap::from([("tool/".to_string(), hash)]),
                &test_executable_process("tool/bin/run.sh"),
                temp.path(),
                temp.path(),
            )
            .await
            .expect("directory-form content_map entry should unpack ZIP");
        let payload_dir = payload_dir.expect("payload dir should be returned");

        assert_eq!(
            std::fs::read_to_string(payload_dir.join("tool").join("bin").join("run.sh"))
                .expect("read unpacked script from payload cache"),
            "echo from zip\n"
        );
    }

    /// Protects support for `./` as a directory-form key that unpacks directly
    /// into the payload cache root.
    ///
    /// With the sandbox-relinking optimization the payload stays in the persistent
    /// cache directory; the assertion checks the cache path, not the sandbox.
    #[tokio::test]
    async fn content_map_directory_entry_accepts_current_directory_root() {
        let cas = Arc::new(InMemoryCas::new());
        let zip_payload = build_test_zip_payload("bin/run.sh", b"echo from zip\n");
        let hash = cas.put(zip_payload).await.expect("store zip payload in CAS");
        let executor = StepWorkerExecutor { cas, conductor_tmp_dir: std::env::temp_dir() };
        let temp = tempfile::tempdir().expect("tempdir");

        let (payload_dir, _guard) = executor
            .materialize_tool_content_map(
                "test-tool",
                &BTreeMap::from([("./".to_string(), hash)]),
                &test_executable_process("bin/run.sh"),
                temp.path(),
                temp.path(),
            )
            .await
            .expect("'./' directory-form content_map entry should unpack ZIP at payload root");
        let payload_dir = payload_dir.expect("payload dir should be returned");

        assert_eq!(
            std::fs::read_to_string(payload_dir.join("bin").join("run.sh"))
                .expect("read unpacked root script from payload cache"),
            "echo from zip\n"
        );
    }

    /// Protects ZIP validation for trailing-slash `content_map` directory keys.
    #[tokio::test]
    async fn content_map_directory_entry_rejects_non_zip_payload() {
        let cas = Arc::new(InMemoryCas::new());
        let hash = cas.put(b"not-a-zip".to_vec()).await.expect("store plain payload in CAS");
        let executor = StepWorkerExecutor { cas, conductor_tmp_dir: std::env::temp_dir() };
        let temp = tempfile::tempdir().expect("tempdir");

        let error = executor
            .materialize_tool_content_map(
                "test-tool",
                &BTreeMap::from([("tool/".to_string(), hash)]),
                &test_executable_process("tool/bin/run.sh"),
                temp.path(),
                temp.path(),
            )
            .await
            .expect_err("directory-form content_map entry should require ZIP payload");

        match error {
            ConductorError::Workflow(message) => {
                assert!(message.contains("expects ZIP payload"));
                assert!(message.contains("tool/"));
            }
            other => panic!("expected workflow error, got {other:?}"),
        }
    }

    /// Protects explicit failure for trailing-slash directory keys that do not
    /// include any concrete path component.
    #[tokio::test]
    async fn content_map_directory_entry_requires_non_empty_prefix() {
        let cas = Arc::new(InMemoryCas::new());
        let zip_payload = build_test_zip_payload("nested/file.txt", b"x");
        let hash = cas.put(zip_payload).await.expect("store zip payload in CAS");
        let executor = StepWorkerExecutor { cas, conductor_tmp_dir: std::env::temp_dir() };
        let temp = tempfile::tempdir().expect("tempdir");

        let error = executor
            .materialize_tool_content_map(
                "test-tool",
                &BTreeMap::from([("/".to_string(), hash)]),
                &test_executable_process("bin/run.sh"),
                temp.path(),
                temp.path(),
            )
            .await
            .expect_err("root-only directory key should fail");

        match error {
            ConductorError::Workflow(message) => {
                assert!(message.contains("must contain at least one path component"));
            }
            other => panic!("expected workflow error, got {other:?}"),
        }
    }

    /// Protects collision safety across separate `content_map` entries.
    #[tokio::test]
    async fn content_map_rejects_file_overwrite_between_entries() {
        let cas = Arc::new(InMemoryCas::new());
        let directory_zip = build_test_zip_payload("run.sh", b"echo from dir\n");
        let directory_hash = cas.put(directory_zip).await.expect("store dir zip payload");
        let file_hash = cas
            .put(b"#!/usr/bin/env sh\necho from file\n".to_vec())
            .await
            .expect("store file payload");
        let executor = StepWorkerExecutor { cas, conductor_tmp_dir: std::env::temp_dir() };
        let temp = tempfile::tempdir().expect("tempdir");

        let error = executor
            .materialize_tool_content_map(
                "test-tool",
                &BTreeMap::from([
                    ("tool/".to_string(), directory_hash),
                    ("tool/run.sh".to_string(), file_hash),
                ]),
                &test_executable_process("tool/run.sh"),
                temp.path(),
                temp.path(),
            )
            .await
            .expect_err("conflicting content_map entries should fail before writes");

        match error {
            ConductorError::Workflow(message) => {
                assert!(message.contains("tool/"));
                assert!(message.contains("tool/run.sh"));
                assert!(message.contains("both materialize"));
            }
            other => panic!("expected workflow error, got {other:?}"),
        }
    }

    /// Protects merged directory behavior when entries target different files.
    ///
    /// With the sandbox-relinking optimization the payload stays in the persistent
    /// cache directory; assertions check the cache path, not the sandbox.
    #[tokio::test]
    async fn content_map_allows_distinct_paths_across_directory_entries() {
        let cas = Arc::new(InMemoryCas::new());
        let first_zip = build_test_zip_payload("a.txt", b"A");
        let first_hash = cas.put(first_zip).await.expect("store first zip payload");
        let second_zip = build_test_zip_payload("b.txt", b"B");
        let second_hash = cas.put(second_zip).await.expect("store second zip payload");
        let executor = StepWorkerExecutor { cas, conductor_tmp_dir: std::env::temp_dir() };
        let temp = tempfile::tempdir().expect("tempdir");

        let (payload_dir, _guard) = executor
            .materialize_tool_content_map(
                "test-tool",
                &BTreeMap::from([
                    ("tool/".to_string(), first_hash),
                    ("tool/nested/".to_string(), second_hash),
                ]),
                &test_executable_process("tool/a.txt"),
                temp.path(),
                temp.path(),
            )
            .await
            .expect("non-overlapping directory entries should merge successfully");
        let payload_dir = payload_dir.expect("payload dir should be returned");

        assert_eq!(
            std::fs::read_to_string(payload_dir.join("tool").join("a.txt"))
                .expect("read first from payload cache"),
            "A"
        );
        assert_eq!(
            std::fs::read_to_string(payload_dir.join("tool").join("nested").join("b.txt"))
                .expect("read second from payload cache"),
            "B"
        );
    }

    /// Protects process-code capture payload formatting.
    #[test]
    fn process_code_capture_serializes_exit_code_text() {
        let executor = StepWorkerExecutor {
            cas: Arc::new(InMemoryCas::new()),
            conductor_tmp_dir: std::env::temp_dir(),
        };
        let capture =
            ToolExecutionCapture { stdout: Vec::new(), stderr: Vec::new(), process_code: 27 };
        let output_spec = ResolvedOutputSpec {
            capture: ResolvedOutputCapture::ProcessCode,
            persistence: PersistenceFlags::default(),
            allow_empty: false,
        };
        let sandbox = tempfile::tempdir().expect("tempdir");

        let payload = executor
            .capture_output_payload(&output_spec, &capture, sandbox.path())
            .expect("process-code capture should serialize")
            .expect("process-code capture must not be empty");

        assert_eq!(payload, b"27".to_vec());
    }

    /// Protects folder capture behavior that emits ZIP payload bytes.
    #[test]
    fn folder_capture_emits_zip_payload_with_optional_top_folder() {
        let executor = StepWorkerExecutor {
            cas: Arc::new(InMemoryCas::new()),
            conductor_tmp_dir: std::env::temp_dir(),
        };
        let sandbox = tempfile::tempdir().expect("tempdir");
        let folder = sandbox.path().join("bundle");
        std::fs::create_dir_all(folder.join("nested")).expect("create folder output");
        std::fs::write(folder.join("nested").join("a.txt"), b"A")
            .expect("write folder output file");
        let capture = ToolExecutionCapture::default();

        let without_top = ResolvedOutputSpec {
            capture: ResolvedOutputCapture::FolderAsZip {
                relative_path: std::path::PathBuf::from("bundle"),
                include_topmost_folder: false,
            },
            persistence: PersistenceFlags::default(),
            allow_empty: false,
        };
        let payload_without_top = executor
            .capture_output_payload(&without_top, &capture, sandbox.path())
            .expect("folder capture without top-level folder should succeed")
            .expect("folder capture must not be empty");
        mediapm_conductor_builtin_archive::unpack_zip_bytes_to_directory(
            &payload_without_top,
            &sandbox.path().join("unzipped_without_top"),
        )
        .expect("unpack zip without top folder");
        assert!(sandbox.path().join("unzipped_without_top").join("nested").join("a.txt").exists());
        assert!(
            !sandbox
                .path()
                .join("unzipped_without_top")
                .join("bundle")
                .join("nested")
                .join("a.txt")
                .exists()
        );

        let with_top = ResolvedOutputSpec {
            capture: ResolvedOutputCapture::FolderAsZip {
                relative_path: std::path::PathBuf::from("bundle"),
                include_topmost_folder: true,
            },
            persistence: PersistenceFlags::default(),
            allow_empty: false,
        };
        let payload_with_top = executor
            .capture_output_payload(&with_top, &capture, sandbox.path())
            .expect("folder capture with top-level folder should succeed")
            .expect("folder capture must not be empty");
        mediapm_conductor_builtin_archive::unpack_zip_bytes_to_directory(
            &payload_with_top,
            &sandbox.path().join("unzipped_with_top"),
        )
        .expect("unpack zip with top folder");
        assert!(
            sandbox
                .path()
                .join("unzipped_with_top")
                .join("bundle")
                .join("nested")
                .join("a.txt")
                .exists()
        );
    }

    /// Protects regex file-capture behavior for dynamic output filenames.
    #[test]
    fn file_regex_capture_selects_single_matching_file() {
        let executor = StepWorkerExecutor {
            cas: Arc::new(InMemoryCas::new()),
            conductor_tmp_dir: std::env::temp_dir(),
        };
        let sandbox = tempfile::tempdir().expect("tempdir");
        std::fs::create_dir_all(sandbox.path().join("downloads")).expect("create downloads");
        std::fs::write(sandbox.path().join("downloads").join("video.info.json"), br#"{"id":"1"}"#)
            .expect("write infojson");
        std::fs::write(sandbox.path().join("downloads").join("video.description"), b"desc")
            .expect("write description");

        let output_spec = ResolvedOutputSpec {
            capture: ResolvedOutputCapture::FileRegex {
                path_regex: Regex::new(r"^downloads/.+\.description$").expect("compile regex"),
                pattern: "^downloads/.+\\.description$".to_string(),
            },
            persistence: PersistenceFlags::default(),
            allow_empty: false,
        };

        let payload = executor
            .capture_output_payload(&output_spec, &ToolExecutionCapture::default(), sandbox.path())
            .expect("regex file capture should select one match")
            .expect("regex file capture must not be empty");

        assert_eq!(payload, b"desc");
    }

    /// Protects strict missing-match checks for regex file-capture declarations.
    #[test]
    fn file_regex_capture_rejects_zero_matches() {
        let executor = StepWorkerExecutor {
            cas: Arc::new(InMemoryCas::new()),
            conductor_tmp_dir: std::env::temp_dir(),
        };
        let sandbox = tempfile::tempdir().expect("tempdir");
        std::fs::create_dir_all(sandbox.path().join("downloads")).expect("create downloads");
        std::fs::write(sandbox.path().join("downloads").join("video.info.json"), br#"{"id":"1"}"#)
            .expect("write infojson");

        let output_spec = ResolvedOutputSpec {
            capture: ResolvedOutputCapture::FileRegex {
                path_regex: Regex::new(r"^downloads/.+\.description$").expect("compile regex"),
                pattern: "^downloads/.+\\.description$".to_string(),
            },
            persistence: PersistenceFlags::default(),
            allow_empty: false,
        };

        let error = executor
            .capture_output_payload(&output_spec, &ToolExecutionCapture::default(), sandbox.path())
            .expect_err("regex file capture with zero matches should fail");

        let ConductorError::Workflow(message) = error else {
            panic!("expected workflow error");
        };
        assert!(message.contains("no sandbox file matched"), "unexpected message: {message}");
    }

    /// Protects strict ambiguity checks for regex file-capture declarations.
    #[test]
    fn file_regex_capture_rejects_multiple_matches() {
        let executor = StepWorkerExecutor {
            cas: Arc::new(InMemoryCas::new()),
            conductor_tmp_dir: std::env::temp_dir(),
        };
        let sandbox = tempfile::tempdir().expect("tempdir");
        std::fs::create_dir_all(sandbox.path().join("downloads")).expect("create downloads");
        std::fs::write(sandbox.path().join("downloads").join("first.description"), b"first")
            .expect("write first description");
        std::fs::write(sandbox.path().join("downloads").join("second.description"), b"second")
            .expect("write second description");

        let output_spec = ResolvedOutputSpec {
            capture: ResolvedOutputCapture::FileRegex {
                path_regex: Regex::new(r"^downloads/.+\.description$").expect("compile regex"),
                pattern: "^downloads/.+\\.description$".to_string(),
            },
            persistence: PersistenceFlags::default(),
            allow_empty: false,
        };

        let err = executor
            .capture_output_payload(&output_spec, &ToolExecutionCapture::default(), sandbox.path())
            .expect_err("regex file capture with multiple matches should fail");

        let ConductorError::Workflow(message) = err else {
            panic!("expected workflow error");
        };
        assert!(message.contains("ambiguous"), "unexpected message: {message}");
    }

    /// Protects regex folder-capture behavior by zipping matched descendants.
    #[test]
    fn folder_regex_capture_packages_matched_directory_descendants() {
        let executor = StepWorkerExecutor {
            cas: Arc::new(InMemoryCas::new()),
            conductor_tmp_dir: std::env::temp_dir(),
        };
        let sandbox = tempfile::tempdir().expect("tempdir");
        std::fs::create_dir_all(sandbox.path().join("downloads").join("nested"))
            .expect("create nested downloads");
        std::fs::create_dir_all(sandbox.path().join("logs")).expect("create logs");
        std::fs::write(sandbox.path().join("downloads").join("video.info.json"), br#"{"id":"1"}"#)
            .expect("write infojson");
        std::fs::write(sandbox.path().join("downloads").join("nested").join("clip.mp4"), b"media")
            .expect("write media");
        std::fs::write(sandbox.path().join("logs").join("debug.txt"), b"noise")
            .expect("write unrelated file");

        let output_spec = ResolvedOutputSpec {
            capture: ResolvedOutputCapture::FolderRegexAsZip {
                path_regex: Regex::new(r"^downloads$").expect("compile regex"),
                pattern: "^downloads$".to_string(),
            },
            persistence: PersistenceFlags::default(),
            allow_empty: false,
        };

        let payload = executor
            .capture_output_payload(&output_spec, &ToolExecutionCapture::default(), sandbox.path())
            .expect("regex folder capture should succeed")
            .expect("regex folder capture must not be empty");

        let unzip_dir = sandbox.path().join("unzipped_regex");
        mediapm_conductor_builtin_archive::unpack_zip_bytes_to_directory(&payload, &unzip_dir)
            .expect("unpack regex folder zip");

        assert!(unzip_dir.join("downloads").join("video.info.json").exists());
        assert!(unzip_dir.join("downloads").join("nested").join("clip.mp4").exists());
        assert!(!unzip_dir.join("logs").join("debug.txt").exists());
    }

    /// Protects optional-family behavior by emitting an empty ZIP payload when a
    /// folder-regex capture matches no sandbox paths.
    #[test]
    fn folder_regex_capture_returns_empty_zip_when_no_paths_match() {
        let executor = StepWorkerExecutor {
            cas: Arc::new(InMemoryCas::new()),
            conductor_tmp_dir: std::env::temp_dir(),
        };
        let sandbox = tempfile::tempdir().expect("tempdir");
        std::fs::create_dir_all(sandbox.path().join("downloads")).expect("create downloads");
        std::fs::write(sandbox.path().join("downloads").join("video.mp4"), b"media")
            .expect("write primary output");

        let output_spec = ResolvedOutputSpec {
            capture: ResolvedOutputCapture::FolderRegexAsZip {
                path_regex: Regex::new(r"^downloads/.+\.comments\.json$").expect("compile regex"),
                pattern: "^downloads/.+\\.comments\\.json$".to_string(),
            },
            persistence: PersistenceFlags::default(),
            allow_empty: false,
        };

        let payload = executor
            .capture_output_payload(&output_spec, &ToolExecutionCapture::default(), sandbox.path())
            .expect("regex folder capture with no matches should succeed with empty zip")
            .expect("regex folder capture must not be empty");

        let unzip_dir = sandbox.path().join("unzipped_regex_empty");
        mediapm_conductor_builtin_archive::unpack_zip_bytes_to_directory(&payload, &unzip_dir)
            .expect("unpack empty regex folder zip");

        let mut entries = std::fs::read_dir(&unzip_dir).expect("read empty unzip root");
        assert!(entries.next().is_none(), "empty capture should unpack to an empty directory tree");
    }

    /// Protects regex folder-capture rename semantics driven by capture groups.
    #[test]
    fn folder_regex_capture_renames_zip_members_from_capture_groups() {
        let executor = StepWorkerExecutor {
            cas: Arc::new(InMemoryCas::new()),
            conductor_tmp_dir: std::env::temp_dir(),
        };
        let sandbox = tempfile::tempdir().expect("tempdir");
        std::fs::create_dir_all(sandbox.path().join("downloads")).expect("create downloads");
        std::fs::write(
            sandbox.path().join("downloads").join("clip__mediapm__.en.srt"),
            b"subtitle",
        )
        .expect("write subtitle sidecar");

        let output_spec = ResolvedOutputSpec {
            capture: ResolvedOutputCapture::FolderRegexAsZip {
                path_regex: Regex::new(r"^downloads/(.+?)__mediapm__(\..+)$")
                    .expect("compile regex"),
                pattern: "^downloads/(.+?)__mediapm__(\\..+)$".to_string(),
            },
            persistence: PersistenceFlags::default(),
            allow_empty: false,
        };

        let payload = executor
            .capture_output_payload(&output_spec, &ToolExecutionCapture::default(), sandbox.path())
            .expect("regex folder capture should succeed")
            .expect("regex folder capture must not be empty");

        let unzip_dir = sandbox.path().join("unzipped_regex_renamed");
        mediapm_conductor_builtin_archive::unpack_zip_bytes_to_directory(&payload, &unzip_dir)
            .expect("unpack regex folder zip");

        assert!(unzip_dir.join("clip.en.srt").exists());
        assert!(!unzip_dir.join("downloads").join("clip__mediapm__.en.srt").exists());
    }

    /// Protects regex folder-capture rename conflict detection.
    #[test]
    fn folder_regex_capture_rejects_renamed_path_conflicts() {
        let executor = StepWorkerExecutor {
            cas: Arc::new(InMemoryCas::new()),
            conductor_tmp_dir: std::env::temp_dir(),
        };
        let sandbox = tempfile::tempdir().expect("tempdir");
        std::fs::create_dir_all(sandbox.path().join("downloads")).expect("create downloads");
        std::fs::write(sandbox.path().join("downloads").join("song__mediapm__.mp3"), b"a")
            .expect("write first media output");
        std::fs::write(sandbox.path().join("downloads").join("song__mediapm__.flac"), b"b")
            .expect("write second media output");

        let output_spec = ResolvedOutputSpec {
            capture: ResolvedOutputCapture::FolderRegexAsZip {
                path_regex: Regex::new(r"^downloads/(.+?)__mediapm__\..+$").expect("compile regex"),
                pattern: "^downloads/(.+?)__mediapm__\\..+$".to_string(),
            },
            persistence: PersistenceFlags::default(),
            allow_empty: false,
        };

        let error = executor
            .capture_output_payload(&output_spec, &ToolExecutionCapture::default(), sandbox.path())
            .expect_err("regex folder capture conflict should fail");

        let ConductorError::Workflow(message) = error else {
            panic!("expected workflow error");
        };
        assert!(message.contains("renamed-path conflict"), "unexpected message: {message}");
    }

    /// Protects executable success-code membership logic.
    #[test]
    fn success_code_membership_checks_configured_set() {
        let success_codes = BTreeSet::from([0_i32, 2_i32, 7_i32]);

        assert!(StepWorkerExecutor::<InMemoryCas>::is_success_exit_code(2, &success_codes));
        assert!(!StepWorkerExecutor::<InMemoryCas>::is_success_exit_code(1, &success_codes));
    }

    /// Protects executable timeout-policy parsing by rejecting zero-second values.
    #[test]
    fn executable_timeout_parser_rejects_zero_seconds() {
        let error = StepWorkerExecutor::<InMemoryCas>::parse_executable_timeout_duration("0")
            .expect_err("zero-second timeout should be rejected");

        let ConductorError::Workflow(message) = error else {
            panic!("expected workflow parse error");
        };
        assert!(message.contains("greater than 0 seconds"), "unexpected parse message: {message}");
    }

    /// Protects worker resilience by timing out long-running executable subprocesses.
    #[tokio::test]
    async fn execute_executable_tool_enforces_timeout_budget() {
        let executor = StepWorkerExecutor {
            cas: Arc::new(InMemoryCas::new()),
            conductor_tmp_dir: std::env::temp_dir(),
        };
        let sandbox = tempfile::tempdir().expect("tempdir");

        let executable_name = if cfg!(windows) { "sleep.cmd" } else { "sleep.sh" };
        let executable_path = sandbox.path().join(executable_name);

        let script = if cfg!(windows) {
            "@echo off\r\n\"%SystemRoot%\\System32\\WindowsPowerShell\\v1.0\\powershell.exe\" -NoProfile -ExecutionPolicy Bypass -Command \"Start-Sleep -Seconds 2\"\r\n"
        } else {
            "#!/bin/sh\nsleep 2\n"
        };
        std::fs::write(&executable_path, script).expect("write timeout script");

        let mut env_vars = BTreeMap::new();
        if cfg!(windows) {
            if let Ok(path) = std::env::var("PATH") {
                env_vars.insert("PATH".to_string(), path);
            }
            if let Ok(system_root) = std::env::var("SystemRoot") {
                env_vars.insert("SystemRoot".to_string(), system_root);
            }
        }

        let error = executor
            .execute_executable_tool_with_timeout(
                executable_name,
                &[],
                &env_vars,
                &BTreeSet::from([0]),
                sandbox.path(),
                None,
                None,
                Duration::from_millis(200),
            )
            .await
            .expect_err("long-running subprocess should time out");

        let ConductorError::Workflow(message) = error else {
            panic!("expected workflow timeout error");
        };
        assert!(message.contains("exceeded timeout"), "unexpected timeout message: {message}");
    }

    /// Protects reverse-diff hinting by skipping CAS constraint patches for the
    /// empty-content root input hash.
    #[tokio::test]
    async fn reverse_diff_hints_skip_empty_content_root_input_hash() {
        let cas = Arc::new(InMemoryCas::new());
        let output_hash = cas.put(b"output".to_vec()).await.expect("put output payload");
        let executor =
            StepWorkerExecutor { cas: cas.clone(), conductor_tmp_dir: std::env::temp_dir() };

        let inputs =
            BTreeMap::from([("empty".to_string(), ResolvedInput::from_plain_content(Vec::new()))]);

        executor
            .apply_reverse_diff_hints(output_hash, &inputs)
            .await
            .expect("reverse-diff hinting should skip empty-content root input hash");

        assert!(
            cas.get_constraint(empty_content_hash())
                .await
                .expect("query empty constraint")
                .is_none(),
            "empty-content root should remain unconstrained"
        );
    }

    /// Protects external-data full-save policy behavior by applying full-save CAS
    /// constraints when `${external_data.<hash>}` bindings are consumed.
    #[tokio::test]
    async fn external_data_full_save_policy_applies_full_save_hint_on_input_resolution() {
        let cas = Arc::new(InMemoryCas::new());
        let external_bytes = b"external-data-full".to_vec();
        let external_hash = cas.put(external_bytes.clone()).await.expect("put external data");
        let executor =
            StepWorkerExecutor { cas: cas.clone(), conductor_tmp_dir: std::env::temp_dir() };

        let workflow_step = WorkflowStepSpec {
            id: "step-full-external".to_string(),
            tool: "echo@1.0.0".to_string(),
            inputs: BTreeMap::new(),
            depends_on: Vec::new(),
            outputs: BTreeMap::new(),
        };
        let unified = UnifiedNickelDocument {
            external_data: BTreeMap::from([(
                external_hash,
                ExternalContentRef {
                    description: Some("full external fixture".to_string()),
                    save: Some(OutputSaveMode::Full),
                },
            )]),
            tools: BTreeMap::new(),
            workflows: BTreeMap::new(),
            tool_content_hashes: BTreeSet::new(),
        };

        let resolved = executor
            .resolve_input_binding(
                &unified,
                "wf",
                &workflow_step,
                &format!("${{external_data.{external_hash}}}"),
                &BTreeMap::new(),
            )
            .await
            .expect("full external-data binding should resolve");

        assert_eq!(resolved.plain_content, external_bytes);
        assert_eq!(resolved.hash, external_hash);

        let constraint =
            cas.get_constraint(external_hash).await.expect("query full-save external constraint");
        let expected = BTreeSet::from([empty_content_hash()]);
        assert_eq!(constraint.as_ref().map(|entry| &entry.potential_bases), Some(&expected));
    }

    /// Protects regular external-data save behavior by avoiding full-save hints
    /// for `save = true` references.
    #[tokio::test]
    async fn external_data_saved_policy_does_not_apply_full_save_hint_on_input_resolution() {
        let cas = Arc::new(InMemoryCas::new());
        let external_hash =
            cas.put(b"external-data-saved".to_vec()).await.expect("put external data");
        let executor =
            StepWorkerExecutor { cas: cas.clone(), conductor_tmp_dir: std::env::temp_dir() };

        let workflow_step = WorkflowStepSpec {
            id: "step-saved-external".to_string(),
            tool: "echo@1.0.0".to_string(),
            inputs: BTreeMap::new(),
            depends_on: Vec::new(),
            outputs: BTreeMap::new(),
        };
        let unified = UnifiedNickelDocument {
            external_data: BTreeMap::from([(
                external_hash,
                ExternalContentRef {
                    description: Some("saved external fixture".to_string()),
                    save: Some(OutputSaveMode::Saved),
                },
            )]),
            tools: BTreeMap::new(),
            workflows: BTreeMap::new(),
            tool_content_hashes: BTreeSet::new(),
        };

        executor
            .resolve_input_binding(
                &unified,
                "wf",
                &workflow_step,
                &format!("${{external_data.{external_hash}}}"),
                &BTreeMap::new(),
            )
            .await
            .expect("saved external-data binding should resolve");

        assert!(
            cas.get_constraint(external_hash)
                .await
                .expect("query saved external constraint")
                .is_none(),
            "save=true should not inject full-save CAS hint"
        );
    }

    /// Protects workflow-error diagnostics by preserving ANSI styling bytes.
    #[test]
    fn format_process_failure_stderr_preserves_ansi_sequences() {
        let raw = "\u{001b}[31merror\u{001b}[0m from tool \u{001b}]8;;https://example.com\u{0007}link\u{001b}]8;;\u{0007}";

        let formatted =
            StepWorkerExecutor::<InMemoryCas>::format_process_failure_stderr(raw.as_bytes());

        assert_eq!(formatted, raw);
    }

    /// Protects fallback message when stderr contains only whitespace.
    #[test]
    fn format_process_failure_stderr_uses_default_for_empty_text() {
        let raw = "\n\t\r";

        let formatted =
            StepWorkerExecutor::<InMemoryCas>::format_process_failure_stderr(raw.as_bytes());

        assert_eq!(formatted, "no stderr output");
    }

    /// Protects crate-owned builtin echo dispatch and stream payload shape.
    #[tokio::test]
    async fn builtin_echo_dispatch_uses_echo_crate_streams() {
        let executor = StepWorkerExecutor {
            cas: Arc::new(InMemoryCas::new()),
            conductor_tmp_dir: std::env::temp_dir(),
        };
        let temp = tempfile::tempdir().expect("tempdir");
        let args = BTreeMap::from([
            ("text".to_string(), "hello".to_string()),
            ("stream".to_string(), "both".to_string()),
        ]);

        let capture = executor
            .execute_builtin_tool(
                mediapm_conductor_builtin_echo::TOOL_NAME,
                mediapm_conductor_builtin_echo::TOOL_VERSION,
                &args,
                &BTreeMap::new(),
                temp.path(),
                temp.path(),
            )
            .await
            .expect("builtin echo dispatch should succeed");

        assert_eq!(capture.stdout, b"hello\n".to_vec());
        assert_eq!(capture.stderr, b"hello\n".to_vec());
        assert_eq!(capture.process_code, 0);
    }

    /// Protects builtin import dispatch for relative paths rooted in outermost config directory.
    #[tokio::test]
    async fn builtin_import_dispatch_supports_relative_local_path() {
        let executor = StepWorkerExecutor {
            cas: Arc::new(InMemoryCas::new()),
            conductor_tmp_dir: std::env::temp_dir(),
        };
        let temp = tempfile::tempdir().expect("tempdir");
        let tool_root = temp.path().join("tool-root");
        let config_root = temp.path().join("config-root");
        std::fs::create_dir_all(&tool_root).expect("create tool root");
        std::fs::create_dir_all(&config_root).expect("create config root");

        let source_path = config_root.join("input.txt");
        std::fs::write(&source_path, b"abc").expect("write source");

        let capture = executor
            .execute_builtin_tool(
                mediapm_conductor_builtin_import::TOOL_NAME,
                mediapm_conductor_builtin_import::TOOL_VERSION,
                &BTreeMap::from([
                    ("kind".to_string(), "file".to_string()),
                    ("path_mode".to_string(), "relative".to_string()),
                    ("path".to_string(), "input.txt".to_string()),
                ]),
                &BTreeMap::new(),
                &tool_root,
                &config_root,
            )
            .await
            .expect("builtin import dispatch should succeed");

        assert_eq!(capture.stdout, b"abc");
        assert_eq!(capture.process_code, 0);
    }

    /// Protects builtin import folder dispatch that exports one ZIP payload.
    #[tokio::test]
    async fn builtin_import_dispatch_exports_folder_as_zip() {
        let executor = StepWorkerExecutor {
            cas: Arc::new(InMemoryCas::new()),
            conductor_tmp_dir: std::env::temp_dir(),
        };
        let temp = tempfile::tempdir().expect("tempdir");
        let source_dir = temp.path().join("fixtures").join("pack");
        std::fs::create_dir_all(&source_dir).expect("create source dir");
        std::fs::write(source_dir.join("a.txt"), b"z").expect("write source file");

        let capture = executor
            .execute_builtin_tool(
                mediapm_conductor_builtin_import::TOOL_NAME,
                mediapm_conductor_builtin_import::TOOL_VERSION,
                &BTreeMap::from([
                    ("kind".to_string(), "folder".to_string()),
                    ("path".to_string(), "fixtures/pack".to_string()),
                ]),
                &BTreeMap::new(),
                temp.path(),
                temp.path(),
            )
            .await
            .expect("builtin import folder dispatch should succeed");

        mediapm_conductor_builtin_archive::unpack_zip_bytes_to_directory(
            &capture.stdout,
            &temp.path().join("unzipped"),
        )
        .expect("unpack imported folder zip");

        assert!(temp.path().join("unzipped").join("a.txt").exists());
    }

    /// Protects builtin import `cas_hash` dispatch that reads payload bytes from CAS.
    #[tokio::test]
    async fn builtin_import_dispatch_supports_cas_hash_kind() {
        let cas = Arc::new(InMemoryCas::new());
        let hash = cas.put(b"from-cas".to_vec()).await.expect("seed CAS payload");
        let executor = StepWorkerExecutor { cas, conductor_tmp_dir: std::env::temp_dir() };
        let temp = tempfile::tempdir().expect("tempdir");

        let capture = executor
            .execute_builtin_tool(
                mediapm_conductor_builtin_import::TOOL_NAME,
                mediapm_conductor_builtin_import::TOOL_VERSION,
                &BTreeMap::from([
                    ("kind".to_string(), "cas_hash".to_string()),
                    ("hash".to_string(), hash.to_string()),
                ]),
                &BTreeMap::new(),
                temp.path(),
                temp.path(),
            )
            .await
            .expect("builtin import cas_hash dispatch should succeed");

        assert_eq!(capture.stdout, b"from-cas");
        assert_eq!(capture.process_code, 0);
    }

    /// Protects builtin fs dispatch and rooted file-write behavior.
    #[tokio::test]
    async fn builtin_fs_dispatch_writes_rooted_file() {
        let executor = StepWorkerExecutor {
            cas: Arc::new(InMemoryCas::new()),
            conductor_tmp_dir: std::env::temp_dir(),
        };
        let temp = tempfile::tempdir().expect("tempdir");
        let tool_root = temp.path().join("tool-root");
        let config_root = temp.path().join("config-root");
        std::fs::create_dir_all(&tool_root).expect("create tool root");
        std::fs::create_dir_all(&config_root).expect("create config root");
        let output_path = config_root.join("out").join("out.txt");

        let capture = executor
            .execute_builtin_tool(
                mediapm_conductor_builtin_fs::TOOL_NAME,
                mediapm_conductor_builtin_fs::TOOL_VERSION,
                &BTreeMap::from([
                    ("op".to_string(), "write_text".to_string()),
                    ("path_mode".to_string(), "relative".to_string()),
                    ("path".to_string(), "out/out.txt".to_string()),
                    ("content".to_string(), "payload".to_string()),
                ]),
                &BTreeMap::new(),
                &tool_root,
                &config_root,
            )
            .await
            .expect("builtin fs dispatch should succeed");

        assert!(capture.stdout.is_empty(), "fs builtin should not emit stdout payload");
        assert!(!tool_root.join("out").join("out.txt").exists());
        assert_eq!(std::fs::read_to_string(output_path).expect("read written file"), "payload");
        assert_eq!(capture.process_code, 0);
    }

    /// Protects builtin archive dispatch for pure file-content pack behavior.
    #[tokio::test]
    async fn builtin_archive_dispatch_packs_pure_file_content() {
        let executor = StepWorkerExecutor {
            cas: Arc::new(InMemoryCas::new()),
            conductor_tmp_dir: std::env::temp_dir(),
        };
        let temp = tempfile::tempdir().expect("tempdir");
        let input_bytes = BTreeMap::from([(
            "content".to_string(),
            ResolvedInput::from_plain_content(b"z".to_vec()),
        )]);

        let capture = executor
            .execute_builtin_tool(
                mediapm_conductor_builtin_archive::TOOL_NAME,
                mediapm_conductor_builtin_archive::TOOL_VERSION,
                &BTreeMap::from([
                    ("action".to_string(), "pack".to_string()),
                    ("kind".to_string(), "file".to_string()),
                    ("entry_name".to_string(), "a.txt".to_string()),
                ]),
                &input_bytes,
                temp.path(),
                temp.path(),
            )
            .await
            .expect("builtin archive dispatch should succeed");

        let unpack_dir = temp.path().join("unpacked");
        mediapm_conductor_builtin_archive::unpack_zip_bytes_to_directory(
            &capture.stdout,
            &unpack_dir,
        )
        .expect("unpack archive payload");
        assert_eq!(std::fs::read(unpack_dir.join("a.txt")).ok(), Some(b"z".to_vec()));
        assert_eq!(capture.process_code, 0);
    }
}
