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
use serde::Serialize;

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

/// Result of one worker step execution.
#[derive(Debug)]
pub(crate) struct StepExecutionBundle {
    /// Final tool call instance snapshot to merge into orchestration state.
    pub instance: ToolCallInstance,
}
