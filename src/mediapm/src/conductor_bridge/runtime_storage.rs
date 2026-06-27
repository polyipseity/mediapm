//! Runtime-storage path defaulting helpers.

use std::path::PathBuf;

use crate::config::MediaRuntimeStorage;
use crate::paths::MediaPmPaths;

/// Resolves effective CAS store root used for pruning imported tool binaries.
#[allow(dead_code)]
pub(super) fn resolve_cas_store_path(paths: &MediaPmPaths) -> PathBuf {
    paths.runtime_root.join("store")
}

/// Normalizes runtime-storage fields with `mediapm` defaults.
///
/// Currently all fields are non-optional in the config model, so this
/// is a structural no-op that returns the input unchanged.
#[allow(dead_code)]
#[must_use]
pub fn normalize_runtime_storage_defaults(storage: &MediaRuntimeStorage) -> MediaRuntimeStorage {
    storage.clone()
}
