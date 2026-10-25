//! Standalone helper functions for mediapm service operations.
//!
//! This module provides reusable functions used by [`MediaPmService`] that
//! are not directly tied to the service struct's lifecycle, including
//! document loading.
//!
//! [`MediaPmService`]: crate::service::MediaPmService

use std::path::{Path, PathBuf};

use crate::config::{MediaPmDocument, MediaRuntimeStorage, load_mediapm_document};
use crate::error::MediaPmError;
use crate::paths::{MediaPmPathOverrides, MediaPmPaths};

/// Returns the builtin tool ids a generated document may carry.
///
/// Derived from the conductor's production builtin set rather than restated
/// here: a builtin added to the registry reaches this list without a second
/// edit, and a registration marked `test_only` never does. The document writer
/// enumerates that set directly; this is the flat id list for callers that need
/// only the ids.
#[must_use]
pub fn registered_builtin_ids() -> Vec<String> {
    mediapm_conductor::tools::registered_production_builtin_ids()
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

#[cfg(test)]
mod tests {
    use super::*;
    use crate::config::RuntimePathsConfig;

    /// Ensures `registered_builtin_ids` follows the registry's production
    /// entries: the five real builtins are present and the `flaky` double is
    /// not, because a generated document may carry only production builtins.
    #[test]
    fn registered_builtin_ids_returns_expected_set() {
        let ids = registered_builtin_ids();
        for expected in ["echo@v1", "fs@v1", "import@v1", "export@v1", "archive@v1"] {
            assert!(ids.iter().any(|id| id == expected), "missing {expected}");
        }
        assert!(
            !ids.iter().any(|id| id == "flaky@v1"),
            "the flaky test double must never be offered to a document writer"
        );
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
