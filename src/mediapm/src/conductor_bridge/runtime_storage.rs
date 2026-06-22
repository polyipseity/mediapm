//! Runtime-storage path defaulting helpers.

use std::path::PathBuf;

use mediapm_conductor::RuntimeStoragePaths;

use crate::paths::MediaPmPaths;

/// Resolves effective CAS store root used for pruning imported tool binaries.
pub(super) fn resolve_cas_store_path(paths: &MediaPmPaths) -> PathBuf {
    paths.runtime_root.join("store")
}

/// Returns the default conductor runtime-storage values used by `mediapm`.
#[allow(dead_code)]
#[must_use]
pub(super) fn default_runtime_storage(paths: &MediaPmPaths) -> RuntimeStoragePaths {
    RuntimeStoragePaths::new(&paths.runtime_root)
}

/// Returns default user-document runtime-storage values used by `mediapm`.
#[allow(dead_code)]
#[must_use]
pub(super) fn default_user_runtime_storage(paths: &MediaPmPaths) -> RuntimeStoragePaths {
    RuntimeStoragePaths::new(&paths.runtime_root)
}

/// Fills missing runtime-storage fields with `mediapm` defaults.
///
/// All fields are non-optional in the current model, so this is a no-op.
#[allow(dead_code)]
#[must_use]
pub(super) fn normalize_runtime_storage_defaults(
    _paths: &MediaPmPaths,
    _runtime_storage: &mut RuntimeStoragePaths,
) -> bool {
    false
}

/// Applies missing-field runtime defaults for user config.
///
/// All fields are non-optional in the current model, so this is a no-op.
#[allow(dead_code)]
#[must_use]
pub(super) fn normalize_user_runtime_storage_defaults(
    _paths: &MediaPmPaths,
    _runtime_storage: &mut RuntimeStoragePaths,
) -> bool {
    false
}
