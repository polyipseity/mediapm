//! Conductor Nickel document bootstrap/load/save helpers.

use std::fs;
use std::path::Path;

use mediapm_conductor::{
    MachineNickelDocument, ToolKindSpec, ToolSpec, UserNickelDocument, decode_machine_document,
    decode_user_document, encode_machine_document, encode_user_document,
};

use crate::error::MediaPmError;
use crate::lockfile::{MediaLockFile, ToolRegistryStatus};
use crate::paths::MediaPmPaths;
use crate::registered_builtin_ids;

use super::ConductorToolRow;
use super::runtime_storage::{
    default_runtime_storage, default_user_runtime_storage, normalize_runtime_storage_defaults,
    normalize_user_runtime_storage_defaults,
};
use super::tool_runtime::tool_spec_has_binary;
use super::util::write_bytes;

/// Ensures conductor user/machine Nickel files exist for Phase 3 orchestration.
pub(crate) fn ensure_conductor_documents(paths: &MediaPmPaths) -> Result<(), MediaPmError> {
    if !paths.conductor_user_ncl.exists() {
        let user_document = UserNickelDocument {
            runtime: default_user_runtime_storage(paths),
            ..UserNickelDocument::default()
        };
        write_bytes(
            &paths.conductor_user_ncl,
            &encode_user_document(user_document)?,
            "writing mediapm.conductor.ncl",
        )?;
    } else {
        let mut user_document = load_user_document(&paths.conductor_user_ncl)?;
        if normalize_user_runtime_storage_defaults(paths, &mut user_document.runtime) {
            save_user_document(&paths.conductor_user_ncl, &user_document)?;
        }
    }

    if !paths.conductor_machine_ncl.exists() {
        let mut machine_document = MachineNickelDocument {
            runtime: default_runtime_storage(paths),
            ..MachineNickelDocument::default()
        };
        register_missing_builtin_tools(&mut machine_document);
        write_bytes(
            &paths.conductor_machine_ncl,
            &encode_machine_document(machine_document)?,
            "writing mediapm.conductor.machine.ncl",
        )?;
    } else {
        let mut machine_document = load_machine_document(&paths.conductor_machine_ncl)?;
        let mut changed = normalize_runtime_storage_defaults(paths, &mut machine_document.runtime);
        changed |= register_missing_builtin_tools(&mut machine_document);
        if changed {
            save_machine_document(&paths.conductor_machine_ncl, &machine_document)?;
        }
    }

    Ok(())
}

/// Registers missing phase-2 builtin tool identities in machine config.
///
/// `mediapm` synthesized workflows may reference builtins directly
/// (for example `import@1.0.0` for local-source ingest). This helper ensures
/// those builtin ids are always present in the machine document so workflow
/// reconciliation can resolve them deterministically.
fn register_missing_builtin_tools(machine: &mut MachineNickelDocument) -> bool {
    let mut changed = false;

    for tool_id in registered_builtin_ids() {
        let Some((name, version)) = parse_builtin_tool_identity(tool_id) else {
            continue;
        };

        let should_insert = !matches!(
            machine.tools.get(tool_id),
            Some(ToolSpec {
                kind: ToolKindSpec::Builtin {
                    name: existing_name,
                    version: existing_version,
                },
                ..
            }) if existing_name == name && existing_version == version
        );

        if should_insert {
            machine.tools.insert(
                tool_id.to_string(),
                ToolSpec {
                    kind: ToolKindSpec::Builtin {
                        name: name.to_string(),
                        version: version.to_string(),
                    },
                    ..ToolSpec::default()
                },
            );
            changed = true;
        }
    }

    changed
}

/// Parses one builtin identity tuple from immutable tool id text.
fn parse_builtin_tool_identity(tool_id: &str) -> Option<(&str, &str)> {
    let (raw_name, version) = tool_id.split_once('@')?;
    let name = raw_name.rsplit('.').next().unwrap_or(raw_name).trim();
    let version = version.trim();

    if name.is_empty() || version.is_empty() {
        return None;
    }

    Some((name, version))
}

/// Loads one user Nickel document from disk, returning defaults when absent.
fn load_user_document(path: &Path) -> Result<UserNickelDocument, MediaPmError> {
    if !path.exists() {
        return Ok(UserNickelDocument::default());
    }

    let bytes = fs::read(path).map_err(|source| MediaPmError::Io {
        operation: "reading mediapm.conductor.ncl".to_string(),
        path: path.to_path_buf(),
        source,
    })?;

    if bytes.iter().all(u8::is_ascii_whitespace) {
        return Ok(UserNickelDocument::default());
    }

    decode_user_document(&bytes).map_err(MediaPmError::from)
}

/// Lists conductor tools visible in machine config with lockfile status overlay.
pub(crate) fn list_tools(
    paths: &MediaPmPaths,
    lock: &MediaLockFile,
) -> Result<Vec<ConductorToolRow>, MediaPmError> {
    let machine = load_machine_document(&paths.conductor_machine_ncl)?;
    let mut rows = machine
        .tools
        .keys()
        .map(|tool_id| {
            let has_materialized_content = machine
                .tool_configs
                .get(tool_id)
                .and_then(|config| config.content_map.as_ref())
                .is_some_and(|map| !map.is_empty());
            let has_binary = has_materialized_content
                || machine.tools.get(tool_id).is_some_and(tool_spec_has_binary);
            let status =
                lock.tool_registry.get(tool_id).map(|record| record.status).unwrap_or_else(|| {
                    if has_binary { ToolRegistryStatus::Active } else { ToolRegistryStatus::Pruned }
                });

            ConductorToolRow { tool_id: tool_id.clone(), has_binary, status }
        })
        .collect::<Vec<_>>();

    rows.sort_by(|left, right| left.tool_id.cmp(&right.tool_id));
    Ok(rows)
}

/// Loads one machine Nickel document from disk, returning defaults when absent.
pub(crate) fn load_machine_document(path: &Path) -> Result<MachineNickelDocument, MediaPmError> {
    if !path.exists() {
        return Ok(MachineNickelDocument::default());
    }

    let bytes = fs::read(path).map_err(|source| MediaPmError::Io {
        operation: "reading mediapm.conductor.machine.ncl".to_string(),
        path: path.to_path_buf(),
        source,
    })?;

    if bytes.iter().all(u8::is_ascii_whitespace) {
        return Ok(MachineNickelDocument::default());
    }

    decode_machine_document(&bytes).map_err(MediaPmError::from)
}

/// Saves one machine Nickel document with conductor's canonical encoder.
pub(crate) fn save_machine_document(
    path: &Path,
    document: &MachineNickelDocument,
) -> Result<(), MediaPmError> {
    let bytes = encode_machine_document(document.clone())?;
    write_bytes(path, &bytes, "writing mediapm.conductor.machine.ncl")
}

/// Saves one user Nickel document with conductor's canonical encoder.
fn save_user_document(path: &Path, document: &UserNickelDocument) -> Result<(), MediaPmError> {
    let bytes = encode_user_document(document.clone())?;
    write_bytes(path, &bytes, "writing mediapm.conductor.ncl")
}
