//! Runtime-storage path defaulting helpers.

use std::path::{Path, PathBuf};

use mediapm_conductor::{
    MachineNickelDocument, RuntimeStorageConfig, default_runtime_inherited_env_vars_for_host,
};

use crate::paths::MediaPmPaths;

/// Resolves effective CAS store root used for pruning imported tool binaries.
pub(super) fn resolve_cas_store_path(
    paths: &MediaPmPaths,
    machine: &MachineNickelDocument,
) -> PathBuf {
    if let Some(raw) = machine.runtime.cas_store_dir.as_deref() {
        let candidate = PathBuf::from(raw);
        if candidate.is_absolute() { candidate } else { paths.root_dir.join(candidate) }
    } else {
        paths.runtime_root.join("store")
    }
}

/// Returns the default conductor runtime-storage values used by `mediapm`.
///
/// These defaults intentionally point Phase 2 runtime state under the
/// effective `mediapm` runtime folder (default `.mediapm/`) instead of
/// conductor's standalone `.conductor/` default tree.
#[must_use]
pub(super) fn default_runtime_storage(paths: &MediaPmPaths) -> RuntimeStorageConfig {
    let conductor_dir = path_to_runtime_storage_value(&paths.root_dir, &paths.runtime_root);
    let state_config =
        path_to_runtime_storage_value(&paths.root_dir, &paths.conductor_state_config);
    let cas_store_dir =
        path_to_runtime_storage_value(&paths.root_dir, &paths.runtime_root.join("store"));
    let inherited_env_vars = default_runtime_inherited_env_vars_for_host();

    RuntimeStorageConfig {
        conductor_dir: Some(conductor_dir),
        state_config: Some(state_config),
        cas_store_dir: Some(cas_store_dir),
        inherited_env_vars: if inherited_env_vars.is_empty() {
            None
        } else {
            Some(inherited_env_vars)
        },
    }
}

/// Returns default user-document runtime-storage values used by `mediapm`.
///
/// User config defaults intentionally omit `runtime.inherited_env_vars` so
/// host defaults are materialized only in machine config unless users opt in.
#[must_use]
pub(super) fn default_user_runtime_storage(paths: &MediaPmPaths) -> RuntimeStorageConfig {
    let mut defaults = default_runtime_storage(paths);
    defaults.inherited_env_vars = None;
    defaults
}

/// Fills missing runtime-storage fields with `mediapm` defaults.
///
/// Returns true when at least one field changed.
fn apply_runtime_storage_defaults(
    paths: &MediaPmPaths,
    runtime_storage: &mut RuntimeStorageConfig,
    include_inherited_env_vars: bool,
) -> bool {
    let defaults = default_runtime_storage(paths);
    let mut changed = false;

    if runtime_storage.conductor_dir.is_none() {
        runtime_storage.conductor_dir = defaults.conductor_dir.clone();
        changed = true;
    }
    if runtime_storage.state_config.is_none() {
        runtime_storage.state_config = defaults.state_config.clone();
        changed = true;
    }
    if runtime_storage.cas_store_dir.is_none() {
        runtime_storage.cas_store_dir = defaults.cas_store_dir.clone();
        changed = true;
    }
    if include_inherited_env_vars && runtime_storage.inherited_env_vars.is_none() {
        runtime_storage.inherited_env_vars = defaults.inherited_env_vars.clone();
        changed = true;
    }

    changed
}

/// Applies missing-field runtime-storage defaults.
///
/// Returns true when any runtime-storage field changed.
pub(super) fn normalize_runtime_storage_defaults(
    paths: &MediaPmPaths,
    runtime_storage: &mut RuntimeStorageConfig,
) -> bool {
    apply_runtime_storage_defaults(paths, runtime_storage, true)
}

/// Applies missing-field runtime defaults for user config while leaving
/// `runtime.inherited_env_vars` untouched.
pub(super) fn normalize_user_runtime_storage_defaults(
    paths: &MediaPmPaths,
    runtime_storage: &mut RuntimeStorageConfig,
) -> bool {
    apply_runtime_storage_defaults(paths, runtime_storage, false)
}

/// Encodes one resolved path into runtime-storage text.
///
/// Paths under `base_root` are emitted as relative slash-normalized text so
/// generated Nickel stays workspace-portable.
#[must_use]
fn path_to_runtime_storage_value(base_root: &Path, path: &Path) -> String {
    let relative = path.strip_prefix(base_root).unwrap_or(path);
    relative.to_string_lossy().replace('\\', "/")
}
