//! Standalone helper functions for mediapm service operations.
//!
//! This module provides reusable functions used by [`MediaPmService`] that
//! are not directly tied to the service struct's lifecycle, including
//! document loading, invalidation rule building, and conductor state
//! management.
//!
//! [`MediaPmService`]: crate::service::MediaPmService

use std::path::Path;

use std::collections::BTreeMap;

use crate::conductor_bridge;
use crate::config::{
    MediaPmDocument, MediaPmImpureTimestamp, MediaPmState, MediaRuntimeStorage, MediaStepTool,
    load_mediapm_document,
};
use crate::error::MediaPmError;
use crate::paths::{MediaPmPathOverrides, MediaPmPaths};

// ---------------------------------------------------------------------------
// Registered builtins
// ---------------------------------------------------------------------------

/// Returns the set of builtin tool ids known to the conductor bridge.
#[must_use]
pub fn registered_builtin_ids() -> Vec<String> {
    vec![
        "echo@1.0.0".to_string(),
        "fs@1.0.0".to_string(),
        "import@1.0.0".to_string(),
        "export@1.0.0".to_string(),
        "archive@1.0.0".to_string(),
    ]
}

// ---------------------------------------------------------------------------
// Document helpers
// ---------------------------------------------------------------------------

/// Ensures the mediapm document exists, loading it from disk or creating a
/// default.
///
/// # Errors
///
/// Returns [`MediaPmError::Io`] if the document file exists but cannot be
/// read, or [`MediaPmError::Serialization`] if it cannot be parsed.
pub(crate) fn ensure_and_load_mediapm_document(
    paths: &MediaPmPaths,
) -> Result<MediaPmDocument, MediaPmError> {
    if paths.mediapm_ncl.exists() {
        load_mediapm_document(&paths.mediapm_ncl)
    } else {
        Ok(MediaPmDocument::default())
    }
}

/// Resolves effective paths for a given root, applying runtime storage
/// overrides.
///
/// This is the standalone version that does not require a service instance.
#[must_use]
pub fn resolve_effective_paths_for_root(
    root_dir: &Path,
    runtime_storage_overrides: &MediaRuntimeStorage,
) -> MediaPmPaths {
    let overrides = MediaPmPathOverrides {
        mediapm_dir: runtime_storage_overrides.mediapm_dir.as_ref().map(|d| d.into()),
        hierarchy_root_dir: runtime_storage_overrides.hierarchy_root_dir.as_ref().map(|d| d.into()),
        conductor_config: runtime_storage_overrides.conductor_config.as_ref().map(|d| d.into()),
        conductor_generated_config: runtime_storage_overrides
            .conductor_generated_config
            .as_ref()
            .map(|d| d.into()),
        conductor_state_config: runtime_storage_overrides
            .conductor_state_config
            .as_ref()
            .map(|d| d.into()),
        conductor_schema_dir: runtime_storage_overrides
            .conductor_schema_dir
            .as_ref()
            .map(|d| d.into()),
        media_state_config: runtime_storage_overrides.media_state_config.as_ref().map(|d| d.into()),
        env_file: runtime_storage_overrides.env_file.as_ref().map(|d| d.into()),
        env_generated_file: runtime_storage_overrides.env_generated_file.as_ref().map(|d| d.into()),
        mediapm_schema_dir: runtime_storage_overrides
            .mediapm_schema_dir
            .as_ref()
            .map(|inner| inner.as_ref().map(|d| d.into())),
    };
    MediaPmPaths::from_root(root_dir).with_overrides(&overrides)
}

/// Loads runtime dotenv files for a given resolved root path.
#[allow(dead_code)]
pub(crate) fn load_runtime_dotenv_for_root(root_dir: &Path) {
    let paths = MediaPmPaths::from_root(root_dir);
    crate::load_runtime_dotenv(&paths.env_file, &paths.env_generated_file);
}

// ---------------------------------------------------------------------------
// Invalidation helpers
// ----------------------------------------------------------------------------

/// Describes a rule for invalidating tool call instances.
#[allow(dead_code)]
#[derive(Debug, Clone)]
pub(crate) struct ToolInvalidationRule {
    /// The tool id whose instances should be invalidated.
    pub tool_id: String,
    /// Optional step index to invalidate (None = all steps).
    pub step_index: Option<usize>,
    /// Optional expected variant hashes. When provided, an instance is
    /// invalidated if its current hashes differ from these expected values.
    #[allow(dead_code)]
    pub expected_hashes: Option<BTreeMap<String, String>>,
}

/// Describes a managed workflow step target for invalidation.
#[allow(dead_code)]
#[derive(Debug, Clone)]
pub(crate) struct ManagedWorkflowStepTarget {
    /// Media source id.
    pub media_id: String,
    /// Step index in the media source's step list.
    pub step_index: usize,
    /// The tool kind for this step.
    pub tool: MediaStepTool,
}

/// Collects workflow step targets for a specific media step tool kind.
#[must_use]
#[allow(dead_code)]
pub(crate) fn collect_workflow_step_targets_for_media_step(
    document: &MediaPmDocument,
    tool: MediaStepTool,
) -> Vec<ManagedWorkflowStepTarget> {
    let mut targets = Vec::new();
    for (media_id, source) in &document.media {
        for (step_index, step) in source.steps.iter().enumerate() {
            if step.tool == tool {
                targets.push(ManagedWorkflowStepTarget {
                    media_id: media_id.clone(),
                    step_index,
                    tool: step.tool,
                });
            }
        }
    }
    targets
}

/// Marks a media step for regeneration by clearing its variant hashes in
/// the state.
pub(crate) fn mark_media_step_for_regeneration(
    state: &mut MediaPmState,
    media_id: &str,
    step_index: usize,
) {
    if let Some(step_state) = state.media.get_mut(media_id) {
        // Clear variant hashes to force regeneration
        step_state.variant_hashes.clear();
        step_state.steps_completed = u32::try_from(step_index).unwrap_or(u32::MAX);
    }
}

/// Loads the conductor state document, returning a default if it doesn't
/// exist.
///
/// # Errors
///
/// Returns [`MediaPmError::Io`] if the file exists but cannot be read, or
/// [`MediaPmError::Serialization`] if it cannot be parsed.
#[allow(dead_code)]
pub(crate) fn load_or_default_conductor_state_document(
    paths: &MediaPmPaths,
) -> Result<mediapm_conductor::NickelDocument, MediaPmError> {
    if paths.conductor_state_config.exists() {
        conductor_bridge::documents::load_conductor_state_document(paths)
    } else {
        Ok(mediapm_conductor::NickelDocument::default())
    }
}

/// Saves a conductor state document to disk.
///
/// # Errors
///
/// Returns [`MediaPmError::Io`] if the file cannot be written, or
/// [`MediaPmError::Serialization`] if serialization fails.
#[allow(dead_code)]
pub(crate) fn save_conductor_state_document(
    paths: &MediaPmPaths,
    document: &mediapm_conductor::NickelDocument,
) -> Result<(), MediaPmError> {
    conductor_bridge::documents::save_conductor_state_document(paths, document)
}

/// Removes impure timestamps for a specific tool from all media step states.
pub(crate) fn remove_target_step_impure_timestamps(state: &mut MediaPmState, _tool_id: &str) {
    for step_state in state.media.values_mut() {
        if step_state.last_impure_sync_at.is_some() {
            step_state.last_impure_sync_at = None;
        }
    }
}

/// Builds invalidation rules from the given tool id, optional step index,
/// and optional expected hashes.
#[must_use]
#[allow(dead_code)]
pub(crate) fn build_tool_invalidation_rules(
    tool_id: &str,
    step_index: Option<usize>,
    expected_hashes: Option<BTreeMap<String, String>>,
) -> Vec<ToolInvalidationRule> {
    vec![ToolInvalidationRule { tool_id: tool_id.to_string(), step_index, expected_hashes }]
}

/// Determines whether a conductor instance should be invalidated based on
/// tool invalidation rules, current variant hashes, and impure TTL.
///
/// Invalidates when:
/// - A matching rule's expected hashes differ from the current hashes, or
/// - No rule's expected hashes match and the impure sync is past TTL.
#[must_use]
#[allow(dead_code)]
pub(crate) fn should_invalidate_instance(
    instance_tool_id: &str,
    rules: &[ToolInvalidationRule],
    variant_hashes: &BTreeMap<String, String>,
    last_impure_sync_at: Option<&MediaPmImpureTimestamp>,
    impure_ttl_secs: u64,
) -> bool {
    for rule in rules {
        if rule.tool_id != instance_tool_id {
            continue;
        }
        // When step_index is set, skip non-matching steps.
        if rule.step_index.is_some() {
            continue;
        }
        // If the rule carries expected hashes, compare with current hashes.
        if let Some(expected) = &rule.expected_hashes {
            if variant_hashes != expected {
                return true;
            }
            // Hashes match — no invalidation needed from this rule.
            return false;
        }
    }

    // Fallback: impure TTL check.
    if let Some(ts) = last_impure_sync_at {
        let now = std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .unwrap_or_default()
            .as_secs();
        if now.saturating_sub(ts.utc_epoch_seconds) > impure_ttl_secs {
            return true;
        }
    }

    false
}

// ---------------------------------------------------------------------------
// Tests
// ---------------------------------------------------------------------------

#[cfg(test)]
mod tests {
    use super::*;
    use crate::MediaStep;
    use crate::config::MediaSourceSpec;
    use std::collections::BTreeMap;

    /// Ensures `registered_builtin_ids` returns expected builtins.
    #[test]
    fn registered_builtin_ids_returns_expected_set() {
        let ids = registered_builtin_ids();
        assert!(ids.contains(&"echo@1.0.0".to_string()));
        assert!(ids.contains(&"fs@1.0.0".to_string()));
        assert!(ids.contains(&"import@1.0.0".to_string()));
        assert!(ids.contains(&"export@1.0.0".to_string()));
        assert!(ids.contains(&"archive@1.0.0".to_string()));
        assert_eq!(ids.len(), 5);
    }

    /// Ensures `collect_workflow_step_targets_for_media_step` finds matching steps.
    #[test]
    fn collect_workflow_step_targets_finds_import_steps() {
        let mut doc = MediaPmDocument::default();
        doc.media.insert(
            "test-source".to_string(),
            MediaSourceSpec {
                steps: vec![
                    MediaStep {
                        tool: MediaStepTool::Import,
                        input_variants: Vec::new(),
                        output_variants: BTreeMap::new(),
                        options: BTreeMap::new(),
                    },
                    MediaStep {
                        tool: MediaStepTool::Rsgain,
                        input_variants: Vec::new(),
                        output_variants: BTreeMap::new(),
                        options: BTreeMap::new(),
                    },
                ],
                ..MediaSourceSpec::default()
            },
        );

        let targets = collect_workflow_step_targets_for_media_step(&doc, MediaStepTool::Import);
        assert_eq!(targets.len(), 1);
        assert_eq!(targets[0].media_id, "test-source");
        assert_eq!(targets[0].step_index, 0);
    }

    /// Ensures `mark_media_step_for_regeneration` clears variant hashes.
    #[test]
    fn mark_media_step_for_regeneration_clears_variant_hashes() {
        let mut state = MediaPmState::default();
        state.media.insert(
            "test-source".to_string(),
            crate::config::ManagedWorkflowStepState {
                variant_hashes: BTreeMap::from([("media".to_string(), "hash123".to_string())]),
                steps_completed: 3,
                last_impure_sync_at: None,
            },
        );

        mark_media_step_for_regeneration(&mut state, "test-source", 0);
        assert!(state.media["test-source"].variant_hashes.is_empty());
    }

    /// Ensures `resolve_effective_paths_for_root` works with overrides.
    #[test]
    fn resolve_effective_paths_for_root_applies_overrides() {
        let dir = tempfile::TempDir::new().expect("temp dir");
        let overrides = MediaRuntimeStorage {
            mediapm_dir: Some(".custom-mediapm".to_string()),
            ..MediaRuntimeStorage::default()
        };
        let paths = resolve_effective_paths_for_root(dir.path(), &overrides);
        assert_eq!(paths.runtime_root, dir.path().join(".custom-mediapm"));
    }
}
