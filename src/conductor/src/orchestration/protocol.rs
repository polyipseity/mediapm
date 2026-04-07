//! Shared internal orchestration contracts.
//!
//! These structures are intentionally kept within the orchestration module so
//! the actor-backed runtime can exchange rich execution data without leaking
//! implementation details into the crate's public API.

use std::collections::{BTreeMap, BTreeSet};
use std::path::PathBuf;
use std::sync::Arc;

use mediapm_cas::Hash;

use crate::model::config::{
    ExternalContentRef, ImpureTimestamp, InputBinding, MachineNickelDocument, ProcessSpec,
    StateNickelDocument, ToolInputSpec, ToolOutputSpec, WorkflowSpec, WorkflowStepSpec,
};
use crate::model::state::{OrchestrationState, ToolCallInstance};

/// Collected output hashes keyed by producing step id and declared output name.
pub(super) type StepOutputs = BTreeMap<String, BTreeMap<String, Hash>>;

/// One tool definition after user/machine document unification.
#[derive(Debug, Clone)]
pub(super) struct UnifiedToolSpec {
    /// Whether the tool is treated as impure for instance-key invalidation.
    pub is_impure: bool,
    /// Maximum concurrent calls allowed for this tool.
    ///
    /// `-1` means unlimited.
    pub max_concurrent_calls: i32,
    /// Declared input contract keyed by input name.
    pub inputs: BTreeMap<String, ToolInputSpec>,
    /// Per-tool default input bindings contributed by merged tool config.
    pub default_inputs: BTreeMap<String, InputBinding>,
    /// Fully merged process definition.
    pub process: ProcessSpec,
    /// Declared output contract keyed by output name.
    pub outputs: BTreeMap<String, ToolOutputSpec>,
    /// Per-tool content-map entries to materialize into the execution sandbox.
    ///
    /// Each key is interpreted as one sandbox-relative destination path:
    /// - trailing `/` or `\\` means directory target and expects ZIP payload
    ///   bytes at the mapped hash,
    /// - `./` (or `.\\`) means unpack ZIP content directly at sandbox root,
    /// - otherwise the key is a file target and bytes are written directly.
    /// - runtime rejects conflicting entries when two separate keys would
    ///   materialize the same file path.
    pub tool_content_map: BTreeMap<String, Hash>,
}

/// The runtime view of the merged conductor documents.
#[derive(Debug, Clone)]
pub(super) struct UnifiedNickelDocument {
    /// External CAS-backed inputs keyed by logical name.
    pub external_data: BTreeMap<String, ExternalContentRef>,
    /// Unified tool catalog keyed by immutable tool name.
    pub tools: BTreeMap<String, UnifiedToolSpec>,
    /// Unified workflow catalog keyed by workflow name.
    pub workflows: BTreeMap<String, WorkflowSpec>,
    /// Every tool-content hash referenced anywhere in the merged config.
    pub tool_content_hashes: BTreeSet<Hash>,
}

/// Result of loading, evaluating, and unifying user/machine/state documents.
#[derive(Debug, Clone)]
pub(super) struct LoadedDocuments {
    /// Machine-editable document after merged runtime-owned fields are updated.
    pub machine_document: MachineNickelDocument,
    /// Volatile runtime state document.
    pub state_document: StateNickelDocument,
    /// Prior state pointer resolved across all three documents.
    pub prior_state_pointer: Option<Hash>,
    /// Effective configuration used for workflow execution.
    pub unified: UnifiedNickelDocument,
}

/// One deterministic worker assignment for a workflow step.
#[derive(Debug, Clone)]
pub(super) struct StepWorkerAssignment {
    /// Worker index selected by the scheduler.
    pub worker_index: usize,
    /// Step payload that the worker should execute.
    pub step: WorkflowStepSpec,
}

/// One step execution request sent from the execution hub to a worker actor.
#[derive(Debug, Clone)]
pub(super) struct StepExecutionRequest {
    /// Unified configuration snapshot shared across one workflow run.
    pub unified: Arc<UnifiedNickelDocument>,
    /// Step definition to execute.
    pub step: WorkflowStepSpec,
    /// Impure timestamp captured before the level starts, when required.
    pub impure_timestamp: Option<ImpureTimestamp>,
    /// Workflow name for diagnostics and error reporting.
    pub workflow_name: String,
    /// State snapshot used for cache-key and rematerialization checks.
    pub state_snapshot: Arc<OrchestrationState>,
    /// Resolved runtime storage root used for creating per-execution scratch
    /// directories.
    ///
    /// This is derived from `RunWorkflowOptions.runtime_storage_paths`
    /// (`conductor_dir`), defaulting to `.conductor` next to the selected
    /// machine or user config files.
    pub runtime_storage_dir: PathBuf,
    /// Absolute directory that directly contains the outermost conductor
    /// configuration file used for this run.
    ///
    /// Builtin `import`, `export`, and `fs` resolve relative path values
    /// against this directory.
    pub outermost_config_dir: PathBuf,
    /// Output hashes already produced by earlier steps in the workflow.
    pub step_outputs: Arc<StepOutputs>,
    /// Declared output names from this step that are actually referenced by
    /// downstream `${step_output...}` input bindings.
    ///
    /// This set drives cache-rematerialization checks: missing unreferenced
    /// outputs do not force rerun of otherwise cache-hit instances.
    pub required_output_names: BTreeSet<String>,
}

/// Result of one worker step execution.
#[derive(Debug)]
pub(super) struct StepExecutionBundle {
    /// Completed step id.
    pub step_id: String,
    /// Immutable tool name used by the step.
    pub tool_name: String,
    /// Worker index that produced the result.
    pub worker_index: usize,
    /// Deterministic instance key for deduplication and cache lookup.
    pub instance_key: String,
    /// Final instance snapshot to merge into orchestration state.
    pub instance: ToolCallInstance,
    /// Outputs the caller asked to materialize for this run.
    pub requested_output_names: Vec<String>,
    /// Whether the step had to execute instead of reusing cached outputs.
    pub executed: bool,
    /// Whether execution was triggered by missing cached outputs.
    pub rematerialized: bool,
    /// Hashes that should be deleted later unless another saved reference protects them.
    pub pending_unsaved_hashes: BTreeSet<Hash>,
    /// Observed execution duration in milliseconds.
    pub elapsed_ms: f64,
    /// Whether execution happened via local fallback after worker RPC failure.
    pub fallback_used: bool,
}

/// Request sent to the execution hub for one workflow level.
#[derive(Debug, Clone)]
pub(super) struct LevelExecutionRequest {
    /// Workflow name used for diagnostics and impure-timestamp lookup.
    pub workflow_name: String,
    /// Zero-based topological level index inside the workflow.
    pub level_index: usize,
    /// Steps ready to execute at this level.
    pub level: Vec<WorkflowStepSpec>,
    /// Unified configuration snapshot shared across the workflow run.
    pub unified: Arc<UnifiedNickelDocument>,
    /// State snapshot used for cache and rematerialization checks.
    pub state_snapshot: Arc<OrchestrationState>,
    /// Resolved runtime storage root used for scratch sandbox creation.
    pub runtime_storage_dir: PathBuf,
    /// Absolute directory that directly contains the outermost conductor
    /// configuration file used for this run.
    pub outermost_config_dir: PathBuf,
    /// Output hashes produced by previous workflow levels.
    pub step_outputs: Arc<StepOutputs>,
    /// Per-step output names that are actually referenced by downstream
    /// `${step_output...}` input bindings.
    pub required_outputs_by_step: BTreeMap<String, BTreeSet<String>>,
    /// Preallocated impure timestamps keyed by step id.
    pub impure_timestamps: BTreeMap<String, Option<ImpureTimestamp>>,
}

/// One level-dispatch result together with worker-RPC failure metadata.
#[derive(Debug)]
pub(super) struct StepDispatchOutcome {
    /// Merge-ready execution result bundle.
    pub result: StepExecutionBundle,
    /// Whether the worker RPC failed before fallback local execution.
    pub rpc_failed: bool,
    /// Optional human-readable RPC failure reason.
    pub rpc_failure_reason: Option<String>,
}

/// Scheduler-facing completion facts derived from one finished step.
#[derive(Debug, Clone)]
pub(super) struct StepCompletionRecord {
    /// Completed step id.
    pub step_id: String,
    /// Immutable tool name used for runtime estimation.
    pub tool_name: String,
    /// Worker index that handled the step.
    pub worker_index: usize,
    /// Whether the step performed real execution work.
    pub executed: bool,
    /// Whether fallback local execution was used.
    pub fallback_used: bool,
    /// Observed runtime in milliseconds.
    pub observed_ms: f64,
    /// Whether the worker RPC itself failed before fallback.
    pub rpc_failed: bool,
    /// Human-readable RPC failure reason when fallback was required.
    pub rpc_failure_reason: Option<String>,
}

/// Request sent to the state-store actor after one workflow run finishes.
#[derive(Debug, Clone)]
pub(super) struct CommitStateRequest {
    /// Final orchestration state to persist and publish as current state.
    pub next_state: OrchestrationState,
    /// Unsaved output hashes eligible for deletion after persistence completes.
    pub pending_unsaved_hashes: BTreeSet<Hash>,
    /// Unified configuration whose references protect hashes from deletion.
    pub unified: UnifiedNickelDocument,
    /// State pointer that was active before the current run started, if any.
    pub prior_state_pointer: Option<Hash>,
}
