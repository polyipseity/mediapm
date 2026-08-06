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
use mediapm_utils::Timestamp;
use serde::Serialize;

use crate::config::{OutputCaptureSpec, ToolInputSpec, WorkflowSpec, WorkflowStepSpec};
pub(super) use crate::state::{ConductorState, ToolCallInstance};

/// Finds a tool catalog entry by its `name` field (not by map key).
///
/// When multiple document keys share the same `name` (for example a pruned
/// stale `{name}@{hash}` entry and the active provisioned tool), prefers the
/// spec whose `tool_content_map` is non-empty, or a builtin registration.
#[must_use]
pub(crate) fn find_tool_entry_by_name<'a>(
    tools: &'a BTreeMap<String, UnifiedToolSpec>,
    name: &str,
) -> Option<(&'a String, &'a UnifiedToolSpec)> {
    let mut fallback_content: Option<(&'a String, &'a UnifiedToolSpec)> = None;
    let mut fallback_any: Option<(&'a String, &'a UnifiedToolSpec)> = None;
    for (key, spec) in tools {
        if spec.name != name {
            continue;
        }
        if spec.builtin_id.is_some() {
            return Some((key, spec));
        }
        if !spec.tool_content_map.is_empty() {
            fallback_content = Some((key, spec));
        } else if fallback_any.is_none() {
            fallback_any = Some((key, spec));
        }
    }
    fallback_content.or(fallback_any)
}

/// Finds a tool spec by its name field (not by map key).
///
/// Delegates to [`find_tool_entry_by_name`].
#[must_use]
pub(crate) fn find_tool_by_name<'a>(
    tools: &'a BTreeMap<String, UnifiedToolSpec>,
    name: &str,
) -> Option<&'a UnifiedToolSpec> {
    find_tool_entry_by_name(tools, name).map(|(_, spec)| spec)
}

/// Collected output hash slots keyed by producing step id and declared output name.
pub(super) type StepOutputs = BTreeMap<String, BTreeMap<String, Hash>>;

/// One tool definition after document unification.
#[derive(Debug, Clone, Serialize)]
pub(crate) struct UnifiedToolSpec {
    /// Tool name (e.g. "ffmpeg").
    pub name: String,
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
    pub default_inputs: BTreeMap<String, crate::config::InputBinding>,
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
    /// The versioned builtin identifier (e.g. `"echo@v1"`) for builtin tools,
    /// or `None` for executable tools.
    pub builtin_id: Option<String>,
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
    pub impure_timestamp: Option<Timestamp>,
    /// State snapshot used for cache-key and rematerialization checks.
    pub state_snapshot: Arc<ConductorState>,
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
    /// Per-output persistence modes for the fresh execution (empty on cache
    /// hits). Stored on the state's per-instance aux record by the
    /// coordinator.
    pub save_modes: BTreeMap<String, crate::state::OutputSaveMode>,
    /// Whether this result came from a cache hit (vs. fresh execution).
    pub cache_hit: bool,
}

#[cfg(test)]
mod tests {
    use super::*;

    /// Builds a tools catalog holding one MANAGED tool spec: the map key and
    /// `spec.name` both carry the prefixed conductor tool id
    /// (`mediapm.tools.{name}@{hash}`, T7), never a bare logical id.
    fn managed_tools_map() -> BTreeMap<String, UnifiedToolSpec> {
        BTreeMap::from([(
            "mediapm.tools.yt-dlp@somehash".to_string(),
            UnifiedToolSpec {
                name: "mediapm.tools.yt-dlp@somehash".to_string(),
                is_impure: false,
                max_concurrent_calls: 0,
                max_retries: 0,
                inputs: BTreeMap::new(),
                default_inputs: BTreeMap::new(),
                command_parts: Vec::new(),
                success_codes: Vec::new(),
                execution_env_vars: BTreeMap::new(),
                outputs: BTreeMap::new(),
                tool_content_map: BTreeMap::new(),
                builtin_id: None,
            },
        )])
    }

    /// `find_tool_by_name` recognizes ONLY PREFIXED MANAGED NAMES (fact 28(5),
    /// Q-D): managed specs carry the prefixed conductor tool id as their name,
    /// so a `mediapm.tools.*`-prefixed query matches the managed spec while a
    /// BARE logical id and the versioned-key form both return `None`. Bare
    /// `step.tool` references resolve via map-key lookup, never this lookup.
    #[test]
    fn find_tool_by_name_recognizes_only_prefixed_managed_names() {
        let tools = managed_tools_map();
        assert!(find_tool_by_name(&tools, "echo").is_none());
        assert!(find_tool_by_name(&tools, "echo@v1").is_none());
        assert_eq!(
            find_tool_by_name(&tools, "mediapm.tools.yt-dlp@somehash")
                .map(|spec| spec.name.as_str()),
            Some("mediapm.tools.yt-dlp@somehash")
        );
    }

    #[test]
    fn find_tool_by_name_prefers_active_content_map_over_pruned_stale() {
        let base = || UnifiedToolSpec {
            name: "ffmpeg".to_string(),
            is_impure: false,
            max_concurrent_calls: 0,
            max_retries: 0,
            inputs: BTreeMap::new(),
            default_inputs: BTreeMap::new(),
            command_parts: Vec::new(),
            success_codes: vec![0],
            execution_env_vars: BTreeMap::new(),
            outputs: BTreeMap::new(),
            tool_content_map: BTreeMap::new(),
            builtin_id: None,
        };
        let stale = {
            let mut spec = base();
            spec.command_parts = vec!["./legacy/ffmpeg/tool.bin".to_string()];
            spec
        };
        let active = {
            let mut spec = base();
            spec.command_parts = vec!["./bin/ffmpeg".to_string()];
            spec.tool_content_map =
                BTreeMap::from([("bin/ffmpeg".to_string(), "blake3:abc".to_string())]);
            spec
        };
        let tools = BTreeMap::from([
            ("ffmpeg@stale".to_string(), stale),
            ("ffmpeg@fresh".to_string(), active),
        ]);
        let (key, resolved) =
            find_tool_entry_by_name(&tools, "ffmpeg").expect("active ffmpeg spec");
        assert_eq!(key, "ffmpeg@fresh");
        assert!(!resolved.tool_content_map.is_empty());
        assert_eq!(resolved.command_parts[0], "./bin/ffmpeg");
    }
}
