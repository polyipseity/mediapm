//! Shared internal orchestration contracts.
//!
//! These types are the wire format between the coordinator, scheduler, step
//! workers, and state-store actor.  They are intentionally kept within the
//! orchestration module so the actor-backed runtime can exchange rich
//! execution data without leaking implementation details into the crate's
//! public API.

use std::collections::{BTreeMap, BTreeSet};
use std::path::PathBuf;
use std::sync::Arc;

use mediapm_cas::Hash;
use serde::{Deserialize, Serialize};

use crate::config::{
    ImpureTimestamp, OutputCaptureSpec, ToolInputSpec, WorkflowSpec, WorkflowStepSpec,
};
pub(super) use crate::state::{OrchestrationState, ToolCallInstance};

/// Collected output hash slots keyed by producing step id and declared output name.
pub(super) type StepOutputs = BTreeMap<String, BTreeMap<String, Hash>>;

/// One tool definition after document unification.
#[derive(Debug, Clone, Serialize)]
pub(crate) struct UnifiedToolSpec {
    /// Whether the tool is treated as impure for tool call instance-key invalidation.
    pub is_impure: bool,
    /// Maximum concurrent calls allowed for this tool.
    ///
    /// `0` means unlimited.
    pub max_concurrent_calls: usize,
    /// Maximum retry count after the initial failed call.
    pub max_retries: usize,
    /// Declared input contract keyed by input name.
    pub inputs: BTreeMap<String, ToolInputSpec>,
    /// Per-tool default input values contributed by merged tool config.
    pub default_inputs: BTreeMap<String, String>,
    /// The command to execute split into parts (exe + args).
    /// Empty for builtin-only tools.
    pub command_parts: Vec<String>,
    /// Expected success exit codes for executable tools.
    pub success_codes: Vec<i32>,
    /// Execution environment variables for executable tools.
    ///
    /// Builtin tools always carry an empty map here.
    pub execution_env_vars: BTreeMap<String, String>,
    /// Declared output capture specs keyed by output name.
    pub outputs: BTreeMap<String, OutputCaptureSpec>,
    /// Per-tool content-map entries to materialize into the execution sandbox.
    pub tool_content_map: BTreeMap<String, String>,
}

/// The runtime view of the merged conductor documents.
#[derive(Debug, Clone, Serialize)]
pub(crate) struct UnifiedNickelDocument {
    /// Unified tool catalog keyed by immutable tool name.
    pub tools: BTreeMap<String, UnifiedToolSpec>,
    /// Unified workflow catalog keyed by workflow name.
    pub workflows: BTreeMap<String, WorkflowSpec>,
    /// Every tool-content hash referenced anywhere in the merged config.
    pub tool_content_hashes: BTreeSet<Hash>,
    /// External data save policies keyed by CAS hash.
    pub external_data_policies: BTreeMap<Hash, crate::state::OutputSaveMode>,
    /// Conductor-level runtime configuration.
    pub runtime: crate::config::ConductorRuntimeConfig,
}

/// One step execution request sent from the coordinator to a worker actor.
#[derive(Debug, Clone)]
pub(crate) struct StepExecutionRequest {
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
    /// Absolute directory that directly contains the outermost conductor
    /// configuration file used for this run.
    ///
    /// Builtins resolve relative path values against this directory.
    pub outermost_config_dir: PathBuf,
    /// Root path for per-step temporary directories.
    pub conductor_tmp_dir: PathBuf,
    /// Output hashes already produced by earlier steps in the workflow.
    pub step_outputs: Arc<StepOutputs>,
    /// Declared output names from this step that are actually referenced by
    /// downstream `$step_output` input bindings.
    ///
    /// This set drives rematerialization checks: missing unreferenced outputs
    /// do not force rerun of otherwise cache-hit instances.
    pub required_output_names: BTreeSet<String>,
}

/// Fine-grained phase timings captured within one step execution.
#[derive(Debug, Clone, Copy, Default, PartialEq, Serialize, Deserialize)]
#[allow(clippy::struct_field_names)]
pub(crate) struct StepPhaseTiming {
    /// Time spent resolving step/default input bindings.
    pub resolve_inputs_ms: f64,
    /// Time spent evaluating rematerialization requirements.
    pub cache_probe_ms: f64,
    /// Time spent preparing execution sandbox content before process start.
    pub materialization_ms: f64,
    /// Time spent running the tool process or builtin implementation.
    pub execution_ms: f64,
    /// Time spent capturing declared outputs into CAS.
    pub capture_outputs_ms: f64,
    /// Time spent applying persistence policies and CAS save hints.
    pub persistence_merge_ms: f64,
}

/// Result of one worker step execution.
#[derive(Debug)]
pub(crate) struct StepExecutionBundle {
    /// Completed step id.
    #[expect(
        dead_code,
        reason = "field is populated but not yet read by any consumer; kept for observability"
    )]
    pub step_id: String,
    /// Immutable tool name used by the step.
    pub tool_name: String,
    /// Worker index that produced the result.
    #[expect(
        dead_code,
        reason = "field is populated but not yet read by any consumer; kept for observability"
    )]
    pub worker_index: usize,
    /// Deterministic tool call instance key for deduplication and cache lookup.
    pub instance_key: String,
    /// Final tool call instance snapshot to merge into orchestration state.
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
    /// Fine-grained timing breakdown for internal execution phases.
    pub phase_timings: StepPhaseTiming,
}
