//! Standalone (non-`impl`-block) functions extracted from `service.rs`.
//!
//! These are free functions and helper types used by `MediaPmService` methods
//! and/or re-exported at the crate root for CLI entrypoints.

use std::collections::{BTreeMap, BTreeSet};
use std::fs;
use std::path::Path;

use mediapm_conductor::model::config::ImpureTimestamp;
use mediapm_conductor::{
    MachineNickelDocument, StateNickelDocument, ToolCallInstance, decode_state_document,
    encode_state_document,
};

use crate::config::{
    self, MediaPmDocument, MediaPmState, MediaRuntimeStorage, load_mediapm_document,
    load_mediapm_document_without_validation, save_mediapm_document,
};
use crate::error::MediaPmError;
use crate::paths::MediaPmPaths;
use crate::{load_runtime_dotenv, merge_runtime_storage};

// ---------------------------------------------------------------------------
// Public API helpers
// ---------------------------------------------------------------------------

/// Returns built-in tool ids that mediapm expects to be available.
#[must_use]
pub fn registered_builtin_ids() -> [&'static str; 5] {
    mediapm_conductor::registered_builtin_ids()
}

/// Unlike `load_runtime_dotenv_for_root`, this helper does not bootstrap a
/// missing `mediapm.ncl` and does not load dotenv files into process state.
/// It is intended for passthrough CLI routing where the parent executable must
/// inject its resolved runtime defaults into child CLI argv without creating
/// configuration files as a side effect.
///
/// Only the `runtime` field of `mediapm.ncl` is used here. Cross-field
/// validation is intentionally skipped so that bootstrapping workflows (for
/// example adding tools one at a time before all companions are present) can
/// resolve paths without triggering premature dependency-graph errors.
///
/// # Errors
///
/// Returns [`MediaPmError`] when an existing `mediapm.ncl` cannot be parsed or
/// when effective runtime paths cannot be derived from config plus overrides.
pub fn resolve_effective_paths_for_root(
    root_dir: &Path,
    runtime_storage_overrides: &MediaRuntimeStorage,
) -> Result<MediaPmPaths, MediaPmError> {
    let base_paths = MediaPmPaths::from_root(root_dir);
    let document = if base_paths.mediapm_ncl.exists() {
        load_mediapm_document_without_validation(&base_paths.mediapm_ncl)?
    } else {
        MediaPmDocument::default()
    };

    let merged_runtime_storage =
        merge_runtime_storage(&document.runtime, runtime_storage_overrides);
    Ok(base_paths.with_runtime_storage(&merged_runtime_storage))
}

/// Loads runtime dotenv values for one workspace root using effective path policy.
///
/// This helper is intended for CLI entrypoints that need environment-backed
/// credentials before invoking internal builtins directly.
/// # Errors
///
/// Returns [`MediaPmError`] when config cannot be loaded, effective runtime
/// paths cannot be resolved, or dotenv loading fails.
pub fn load_runtime_dotenv_for_root(
    root_dir: &Path,
    runtime_storage_overrides: &MediaRuntimeStorage,
) -> Result<MediaPmPaths, MediaPmError> {
    let effective_paths = if MediaPmPaths::from_root(root_dir).mediapm_ncl.exists() {
        resolve_effective_paths_for_root(root_dir, runtime_storage_overrides)?
    } else {
        let base_paths = MediaPmPaths::from_root(root_dir);
        let document = ensure_and_load_mediapm_document(&base_paths.mediapm_ncl)?;
        let merged_runtime_storage =
            merge_runtime_storage(&document.runtime, runtime_storage_overrides);
        base_paths.with_runtime_storage(&merged_runtime_storage)
    };
    load_runtime_dotenv(&effective_paths)?;
    Ok(effective_paths)
}

// ---------------------------------------------------------------------------
// Internal helpers used by `MediaPmService` methods
// ---------------------------------------------------------------------------

/// Loads `mediapm.ncl`, writing defaults when absent.
pub(crate) fn ensure_and_load_mediapm_document(
    path: &Path,
) -> Result<MediaPmDocument, MediaPmError> {
    if !config::mediapm_document_exists(path) {
        save_mediapm_document(path, &MediaPmDocument::default())?;
    }

    load_mediapm_document(path)
}

/// Cache-invalidation rule for one immutable managed tool id.
#[derive(Debug, Clone, PartialEq, Eq, Default)]
pub(crate) struct ToolInvalidationRule {
    /// When true, remove all instances for this tool id.
    pub(crate) remove_all: bool,
    /// Otherwise remove only instances with one matching impure timestamp.
    pub(crate) impure_timestamps: Vec<ImpureTimestamp>,
}

/// One managed workflow step target resolved from media-step index mapping.
#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) struct ManagedWorkflowStepTarget {
    /// Deterministic conductor step id.
    pub(crate) step_id: String,
    /// Immutable managed tool id referenced by this step.
    pub(crate) tool_id: String,
}

/// Resolves conductor workflow steps mapped from one media-step index.
pub(crate) fn collect_workflow_step_targets_for_media_step(
    machine: &MachineNickelDocument,
    workflow_id: &str,
    step_index: usize,
) -> Result<Vec<ManagedWorkflowStepTarget>, MediaPmError> {
    let workflow = machine.workflows.get(workflow_id).ok_or_else(|| {
        MediaPmError::Workflow(format!(
            "managed workflow '{workflow_id}' does not exist in conductor machine config"
        ))
    })?;
    let step_prefix = format!("{step_index}-");

    let mut targets = workflow
        .steps
        .iter()
        .filter(|step| step.id.starts_with(step_prefix.as_str()))
        .map(|step| ManagedWorkflowStepTarget {
            step_id: step.id.clone(),
            tool_id: step.tool.clone(),
        })
        .collect::<Vec<_>>();
    targets.sort_by(|left, right| left.step_id.cmp(&right.step_id));

    if targets.is_empty() {
        return Err(MediaPmError::Workflow(format!(
            "managed workflow '{workflow_id}' has no conductor steps for media step index {step_index}; run 'mediapm sync' and retry"
        )));
    }

    Ok(targets)
}

/// Clears one mediapm step refresh timestamp to force regeneration.
pub(crate) fn mark_media_step_for_regeneration(
    lock: &mut MediaPmState,
    media_id: &str,
    step_index: usize,
) -> Result<(), MediaPmError> {
    let Some(step_states) = lock.workflow_states.get_mut(media_id) else {
        return Err(MediaPmError::Workflow(format!(
            "cannot regenerate media step: no workflow state exists for media id '{media_id}'"
        )));
    };
    let Some(step_state) = step_states.get_mut(step_index) else {
        return Err(MediaPmError::Workflow(format!(
            "cannot regenerate media step: media '{media_id}' has {} persisted workflow step state(s), but step index {step_index} was requested",
            step_states.len()
        )));
    };

    step_state.impure_timestamp = None;
    Ok(())
}

/// Loads conductor volatile state document or defaults when missing.
pub(crate) fn load_or_default_conductor_state_document(
    path: &Path,
) -> Result<StateNickelDocument, MediaPmError> {
    if !path.exists() {
        return Ok(StateNickelDocument::default());
    }

    let bytes = fs::read(path).map_err(|source| MediaPmError::Io {
        operation: "reading conductor volatile state document".to_string(),
        path: path.to_path_buf(),
        source,
    })?;

    if bytes.iter().all(u8::is_ascii_whitespace) {
        return Ok(StateNickelDocument::default());
    }

    decode_state_document(&bytes).map_err(MediaPmError::from)
}

/// Persists conductor volatile state document using canonical encoder.
pub(crate) fn save_conductor_state_document(
    path: &Path,
    document: &StateNickelDocument,
) -> Result<(), MediaPmError> {
    let encoded = encode_state_document(document.clone())?;

    if let Some(parent) = path.parent() {
        fs::create_dir_all(parent).map_err(|source| MediaPmError::Io {
            operation: format!(
                "creating parent directory for conductor state document '{}'",
                path.display()
            ),
            path: parent.to_path_buf(),
            source,
        })?;
    }

    fs::write(path, &encoded).map_err(|source| MediaPmError::Io {
        operation: "writing conductor volatile state document".to_string(),
        path: path.to_path_buf(),
        source,
    })
}

/// Removes impure timestamp rows for targeted workflow steps.
pub(crate) fn remove_target_step_impure_timestamps(
    state_document: &mut StateNickelDocument,
    workflow_id: &str,
    step_targets: &[ManagedWorkflowStepTarget],
) -> (usize, BTreeMap<String, Vec<ImpureTimestamp>>, BTreeSet<String>) {
    let mut removed = 0usize;
    let mut timestamps_by_tool = BTreeMap::<String, Vec<ImpureTimestamp>>::new();
    let mut tools_without_timestamp = BTreeSet::<String>::new();

    if let Some(workflow_timestamps) = state_document.impure_timestamps.get_mut(workflow_id) {
        for target in step_targets {
            if let Some(timestamp) = workflow_timestamps.remove(target.step_id.as_str()) {
                timestamps_by_tool.entry(target.tool_id.clone()).or_default().push(timestamp);
                removed = removed.saturating_add(1);
            } else {
                tools_without_timestamp.insert(target.tool_id.clone());
            }
        }

        if workflow_timestamps.is_empty() {
            state_document.impure_timestamps.remove(workflow_id);
        }
    } else {
        tools_without_timestamp.extend(step_targets.iter().map(|target| target.tool_id.clone()));
    }

    (removed, timestamps_by_tool, tools_without_timestamp)
}

/// Builds per-tool invalidation rules from targeted step ids and timestamps.
pub(crate) fn build_tool_invalidation_rules(
    step_targets: &[ManagedWorkflowStepTarget],
    impure_timestamps_by_tool: &BTreeMap<String, Vec<ImpureTimestamp>>,
    tools_without_timestamp: &BTreeSet<String>,
) -> BTreeMap<String, ToolInvalidationRule> {
    let mut target_counts = BTreeMap::<String, usize>::new();
    for target in step_targets {
        *target_counts.entry(target.tool_id.clone()).or_insert(0) += 1;
    }

    let mut rules = BTreeMap::<String, ToolInvalidationRule>::new();
    for (tool_id, count) in target_counts {
        let timestamps =
            impure_timestamps_by_tool.get(tool_id.as_str()).cloned().unwrap_or_default();
        let has_unmapped_target =
            timestamps.len() < count || tools_without_timestamp.contains(&tool_id);

        rules.insert(
            tool_id,
            ToolInvalidationRule { remove_all: has_unmapped_target, impure_timestamps: timestamps },
        );
    }

    rules
}

/// Returns true when one cached orchestration instance should be invalidated.
pub(crate) fn should_invalidate_instance(
    instance: &ToolCallInstance,
    invalidation_rules: &BTreeMap<String, ToolInvalidationRule>,
) -> bool {
    let Some(rule) = invalidation_rules.get(instance.tool_name.as_str()) else {
        return false;
    };

    if rule.remove_all {
        return true;
    }

    instance.impure_timestamp.is_some_and(|timestamp| rule.impure_timestamps.contains(&timestamp))
}

// ---------------------------------------------------------------------------
// Tests
// ---------------------------------------------------------------------------

#[cfg(test)]
#[allow(clippy::needless_pass_by_value)]
mod tests {
    use mediapm_cas::Hash;
    use mediapm_conductor::{ImpureTimestamp, OutputRef, PersistenceFlags, ToolSpec};
    use std::collections::BTreeMap;

    use super::{
        ManagedWorkflowStepTarget, ToolInvalidationRule, remove_target_step_impure_timestamps,
        should_invalidate_instance,
    };

    /// Ensures helper removes targeted impure timestamps and tracks tool mapping.
    #[test]
    fn remove_target_step_impure_timestamps_tracks_removed_entries() {
        let timestamp = ImpureTimestamp { epoch_seconds: 123, subsec_nanos: 456 };
        let mut state_document = mediapm_conductor::StateNickelDocument {
            impure_timestamps: BTreeMap::from([(
                "workflow.media.demo".to_string(),
                BTreeMap::from([("1-0-yt_dlp".to_string(), timestamp)]),
            )]),
            state_pointer: None,
        };
        let targets = vec![ManagedWorkflowStepTarget {
            step_id: "1-0-yt_dlp".to_string(),
            tool_id: "mediapm.tools.yt-dlp@latest".to_string(),
        }];

        let (removed, by_tool, without_timestamp) = remove_target_step_impure_timestamps(
            &mut state_document,
            "workflow.media.demo",
            &targets,
        );

        assert_eq!(removed, 1);
        assert_eq!(by_tool.get("mediapm.tools.yt-dlp@latest"), Some(&vec![timestamp]));
        assert!(without_timestamp.is_empty());
        assert!(state_document.impure_timestamps.is_empty());
    }

    /// Ensures targeted tool invalidation can match specific impure timestamps.
    #[test]
    fn should_invalidate_instance_matches_timestamp_rule() {
        let timestamp = ImpureTimestamp { epoch_seconds: 10, subsec_nanos: 20 };
        let instance = mediapm_conductor::ToolCallInstance {
            tool_name: "tool-a".to_string(),
            metadata: ToolSpec::default(),
            impure_timestamp: Some(timestamp),
            inputs: BTreeMap::new(),
            outputs: BTreeMap::from([(
                "result".to_string(),
                OutputRef {
                    hash: Hash::zero(),
                    persistence: PersistenceFlags::default(),
                    allow_empty_capture: false,
                },
            )]),
        };
        let rules = BTreeMap::from([(
            "tool-a".to_string(),
            ToolInvalidationRule { remove_all: false, impure_timestamps: vec![timestamp] },
        )]);

        assert!(should_invalidate_instance(&instance, &rules));
    }

    /// Ensures `remove_all` flag invalidates all instances for that tool.
    #[test]
    fn should_invalidate_instance_respects_remove_all_rule() {
        let instance = mediapm_conductor::ToolCallInstance {
            tool_name: "tool-a".to_string(),
            metadata: ToolSpec::default(),
            impure_timestamp: None,
            inputs: BTreeMap::new(),
            outputs: BTreeMap::new(),
        };
        let rules = BTreeMap::from([(
            "tool-a".to_string(),
            ToolInvalidationRule { remove_all: true, impure_timestamps: Vec::new() },
        )]);

        assert!(should_invalidate_instance(&instance, &rules));
    }

    /// Ensures untargeted tool instances are not invalidated.
    #[test]
    fn should_invalidate_instance_ignores_non_targeted_tool() {
        let instance = mediapm_conductor::ToolCallInstance {
            tool_name: "tool-b".to_string(),
            metadata: ToolSpec::default(),
            impure_timestamp: Some(ImpureTimestamp { epoch_seconds: 10, subsec_nanos: 20 }),
            inputs: BTreeMap::new(),
            outputs: BTreeMap::new(),
        };
        let rules = BTreeMap::from([(
            "tool-a".to_string(),
            ToolInvalidationRule { remove_all: true, impure_timestamps: Vec::new() },
        )]);

        assert!(!should_invalidate_instance(&instance, &rules));
    }
}
