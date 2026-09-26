//! Standalone helper functions for mediapm service operations.
//!
//! This module provides reusable functions used by [`MediaPmService`] that
//! are not directly tied to the service struct's lifecycle, including
//! document loading.
//!
//! [`MediaPmService`]: crate::service::MediaPmService

use std::path::{Path, PathBuf};

use crate::config::{MediaPmDocument, MediaPmState, MediaRuntimeStorage, load_mediapm_document};
use crate::error::MediaPmError;
use crate::paths::{MediaPmPathOverrides, MediaPmPaths};

/// Returns the set of builtin tool ids known to the conductor bridge.
#[must_use]
pub fn registered_builtin_ids() -> Vec<String> {
    vec![
        "echo@v1".to_string(),
        "fs@v1".to_string(),
        "import@v1".to_string(),
        "export@v1".to_string(),
        "archive@v1".to_string(),
    ]
}

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

/// Converts a `PathBuf` override into an `Option<PathBuf>`, mapping empty
/// paths to `None` (use computed default).
#[must_use]
fn opt_path(path: &Path) -> Option<PathBuf> {
    if path.as_os_str().is_empty() { None } else { Some(path.to_path_buf()) }
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
        mediapm_dir: opt_path(&runtime_storage_overrides.paths.mediapm_dir),
        hierarchy_root_dir: opt_path(&runtime_storage_overrides.paths.hierarchy_root_dir),
        conductor_config: opt_path(&runtime_storage_overrides.paths.conductor_config),
        conductor_generated_config: opt_path(
            &runtime_storage_overrides.paths.conductor_generated_config,
        ),
        conductor_state_config: opt_path(&runtime_storage_overrides.paths.conductor_state_config),
        conductor_schema_dir: opt_path(&runtime_storage_overrides.paths.conductor_schema_dir),
        media_state_config: opt_path(&runtime_storage_overrides.paths.mediapm_state_config),
        env_file: opt_path(&runtime_storage_overrides.paths.env_file),
        env_generated_file: opt_path(&runtime_storage_overrides.paths.env_generated_file),
        mediapm_schema_dir: if runtime_storage_overrides
            .paths
            .mediapm_schema_dir
            .as_os_str()
            .is_empty()
        {
            None
        } else {
            Some(Some(runtime_storage_overrides.paths.mediapm_schema_dir.clone()))
        },
    };
    MediaPmPaths::from_root(root_dir).with_overrides(&overrides)
}

/// Marks a media step for regeneration by clearing its variant hashes in
/// the state.
pub(crate) fn mark_media_step_for_regeneration(
    state: &mut MediaPmState,
    media_id: &str,
    step_index: usize,
) {
    if let Some(step_state) = state.workflow_states.get_mut(media_id) {
        // Clear variant hashes to force regeneration
        step_state.variant_hashes.clear();
        step_state.steps_completed = u32::try_from(step_index).unwrap_or(u32::MAX);
    }
}

/// Removes impure timestamps from every media step state.
///
/// The previous signature took a `_tool_id` that the body never read, while
/// the sole call site passed a media id. The parameter is gone because an
/// unused parameter is not part of the API, and keeping it under a `_`
/// prefix hid the fact that the call site and the name disagreed.
pub(crate) fn remove_all_step_impure_timestamps(state: &mut MediaPmState) {
    for step_state in state.workflow_states.values_mut() {
        if step_state.last_impure_sync_at.is_some() {
            step_state.last_impure_sync_at = None;
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::config::RuntimePathsConfig;
    use std::collections::BTreeMap;

    /// Ensures `registered_builtin_ids` returns expected builtins.
    #[test]
    fn registered_builtin_ids_returns_expected_set() {
        let ids = registered_builtin_ids();
        assert!(ids.contains(&"echo@v1".to_string()));
        assert!(ids.contains(&"fs@v1".to_string()));
        assert!(ids.contains(&"import@v1".to_string()));
        assert!(ids.contains(&"export@v1".to_string()));
        assert!(ids.contains(&"archive@v1".to_string()));
        assert_eq!(ids.len(), 5);
    }

    /// Ensures `mark_media_step_for_regeneration` clears variant hashes.
    #[test]
    fn mark_media_step_for_regeneration_clears_variant_hashes() {
        let mut state = MediaPmState::default();
        state.workflow_states.insert(
            "test-source".to_string(),
            crate::config::ManagedWorkflowStepState {
                variant_hashes: BTreeMap::from([("media".to_string(), "hash123".to_string())]),
                steps_completed: 3,
                last_impure_sync_at: None,
            },
        );

        mark_media_step_for_regeneration(&mut state, "test-source", 0);
        assert!(state.workflow_states["test-source"].variant_hashes.is_empty());
    }

    /// Ensures `resolve_effective_paths_for_root` works with overrides.
    #[test]
    fn resolve_effective_paths_for_root_applies_overrides() {
        let dir = mediapm_utils::temp::artifact_dir().expect("temp dir");
        let overrides = MediaRuntimeStorage {
            paths: RuntimePathsConfig {
                mediapm_dir: ".custom-mediapm".into(),
                ..RuntimePathsConfig::default()
            },
            ..MediaRuntimeStorage::default()
        };
        let paths = resolve_effective_paths_for_root(dir.path(), &overrides);
        assert_eq!(paths.runtime_root, dir.path().join(".custom-mediapm"));
    }
}
