//! Conductor Nickel document bootstrap/load/save helpers.

use std::fs;
use std::path::Path;

use mediapm_conductor::{
    MachineNickelDocument, ToolConfigSpec, ToolKindSpec, ToolSpec, UserNickelDocument,
    decode_machine_document, decode_user_document, encode_machine_document, encode_user_document,
};

use crate::error::MediaPmError;
use crate::lockfile::{MediaLockFile, ToolRegistryStatus};
use crate::paths::MediaPmPaths;
use crate::registered_builtin_ids;

use super::runtime_storage::{
    default_runtime_storage, default_user_runtime_storage, normalize_runtime_storage_defaults,
    normalize_user_runtime_storage_defaults,
};
use super::tool_runtime::{extract_platform_conditional_paths, tool_spec_has_binary};
use super::util::write_bytes;
use super::{ConductorToolRow, ManagedToolExecutableTarget};

/// Ensures conductor user/machine Nickel files exist for Phase 3 orchestration.
pub(crate) fn ensure_conductor_documents(paths: &MediaPmPaths) -> Result<(), MediaPmError> {
    if paths.conductor_user_ncl.exists() {
        let mut user_document = load_user_document(&paths.conductor_user_ncl)?;
        if normalize_user_runtime_storage_defaults(paths, &mut user_document.runtime) {
            save_user_document(&paths.conductor_user_ncl, &user_document)?;
        }
    } else {
        let user_document = UserNickelDocument {
            runtime: default_user_runtime_storage(paths),
            ..UserNickelDocument::default()
        };
        write_bytes(
            &paths.conductor_user_ncl,
            &encode_user_document(user_document)?,
            "writing mediapm.conductor.ncl",
        )?;
    }

    if paths.conductor_machine_ncl.exists() {
        let mut machine_document = load_machine_document(&paths.conductor_machine_ncl)?;
        let mut changed = normalize_runtime_storage_defaults(paths, &mut machine_document.runtime);
        changed |= register_missing_builtin_tools(&mut machine_document);
        changed |= register_missing_builtin_tool_configs(&mut machine_document);
        if changed {
            save_machine_document(&paths.conductor_machine_ncl, &machine_document)?;
        }
    } else {
        let mut machine_document = MachineNickelDocument {
            runtime: default_runtime_storage(paths),
            ..MachineNickelDocument::default()
        };
        register_missing_builtin_tools(&mut machine_document);
        register_missing_builtin_tool_configs(&mut machine_document);
        write_bytes(
            &paths.conductor_machine_ncl,
            &encode_machine_document(machine_document)?,
            "writing mediapm.conductor.machine.ncl",
        )?;
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

/// Registers missing default runtime `tool_configs` for builtin tools.
///
/// `mediapm` emits builtin workflow steps (for example `import@1.0.0`) and
/// expects machine-managed runtime config maps to contain explicit per-tool
/// entries even when option defaults are intentionally empty.
fn register_missing_builtin_tool_configs(machine: &mut MachineNickelDocument) -> bool {
    let mut changed = false;

    for tool_id in registered_builtin_ids() {
        if machine.tool_configs.contains_key(tool_id) {
            continue;
        }

        machine.tool_configs.insert(tool_id.to_string(), ToolConfigSpec::default());
        changed = true;
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
            let status = lock.tool_registry.get(tool_id).map_or_else(
                || {
                    if has_binary { ToolRegistryStatus::Active } else { ToolRegistryStatus::Pruned }
                },
                |record| record.status,
            );

            ConductorToolRow { tool_id: tool_id.clone(), has_binary, status }
        })
        .collect::<Vec<_>>();

    rows.sort_by(|left, right| left.tool_id.cmp(&right.tool_id));
    Ok(rows)
}

/// Resolves one managed-tool executable path from an immutable id or logical name.
pub(crate) fn resolve_managed_tool_executable_target(
    paths: &MediaPmPaths,
    lock: &MediaLockFile,
    tool_selector: &str,
) -> Result<ManagedToolExecutableTarget, MediaPmError> {
    let machine = load_machine_document(&paths.conductor_machine_ncl)?;
    let selector = tool_selector.trim();
    if selector.is_empty() {
        return Err(MediaPmError::Workflow("managed tool selector must be non-empty".to_string()));
    }

    let resolved_tool_id = resolve_managed_tool_id(&machine, lock, selector)?;
    let tool_spec = machine.tools.get(&resolved_tool_id).ok_or_else(|| {
        MediaPmError::Workflow(format!(
            "managed tool '{resolved_tool_id}' is missing from conductor machine config"
        ))
    })?;

    let command_selector = match &tool_spec.kind {
        ToolKindSpec::Executable { command, .. } => {
            command.first().map(String::as_str).ok_or_else(|| {
                MediaPmError::Workflow(format!(
                    "managed tool '{resolved_tool_id}' has no executable command configured"
                ))
            })?
        }
        ToolKindSpec::Builtin { .. } => {
            return Err(MediaPmError::Workflow(format!(
                "tool selector '{selector}' resolved to builtin tool '{resolved_tool_id}', which has no managed executable binary"
            )));
        }
    };

    let host_relative = resolve_host_command_selector_path(command_selector)?.ok_or_else(|| {
        MediaPmError::Workflow(format!(
            "managed tool '{resolved_tool_id}' command selector '{command_selector}' does not resolve to a host executable path for os '{}'",
            std::env::consts::OS
        ))
    })?;

    let relative = normalize_managed_tool_relative_command_path(&host_relative).ok_or_else(|| {
        MediaPmError::Workflow(format!(
            "managed tool '{resolved_tool_id}' command selector '{command_selector}' resolved to an empty path"
        ))
    })?;

    let command_path = paths.tools_dir.join(&resolved_tool_id).join(Path::new(&relative));
    if !command_path.is_file() {
        return Err(MediaPmError::Workflow(format!(
            "managed tool binary for '{resolved_tool_id}' is missing at '{}'",
            command_path.display()
        )));
    }

    Ok(ManagedToolExecutableTarget { tool_id: resolved_tool_id, command_path })
}

/// Resolves one immutable managed tool id from selector text.
fn resolve_managed_tool_id(
    machine: &MachineNickelDocument,
    lock: &MediaLockFile,
    selector: &str,
) -> Result<String, MediaPmError> {
    if let Some(exact) = machine.tools.keys().find(|tool_id| tool_id.eq_ignore_ascii_case(selector))
    {
        return Ok(exact.clone());
    }

    if let Some(active_tool_id) = lock
        .active_tools
        .iter()
        .find(|(logical_name, _)| logical_name.eq_ignore_ascii_case(selector))
        .map(|(_, tool_id)| tool_id)
        .filter(|tool_id| machine.tools.contains_key(*tool_id))
    {
        return Ok(active_tool_id.clone());
    }

    let mut matches = machine
        .tools
        .keys()
        .filter(|tool_id| logical_name_matches_tool_id(tool_id, selector))
        .cloned()
        .collect::<Vec<_>>();

    matches.sort();
    matches.dedup();

    match matches.as_slice() {
        [only] => Ok(only.clone()),
        [] => Err(MediaPmError::Workflow(format!(
            "tool selector '{selector}' did not match any managed tool id in conductor machine config"
        ))),
        _ => Err(MediaPmError::Workflow(format!(
            "tool selector '{selector}' matched multiple managed tool ids ({}) ; use an immutable --tool id",
            matches.join(", ")
        ))),
    }
}

/// Returns true when immutable tool id belongs to one logical tool name.
fn logical_name_matches_tool_id(tool_id: &str, logical_name: &str) -> bool {
    if tool_id.eq_ignore_ascii_case(logical_name) {
        return true;
    }

    let Some((prefix, _)) = tool_id.split_once('@') else {
        return false;
    };

    let canonical_prefix = strip_managed_tool_id_prefix(prefix);
    let canonical_name =
        canonical_prefix.split_once('+').map_or(canonical_prefix, |(name, _)| name);

    canonical_name.trim().eq_ignore_ascii_case(logical_name)
}

/// Removes optional `mediapm.tools.` prefix from one immutable tool id head.
fn strip_managed_tool_id_prefix(prefix: &str) -> &str {
    let marker = "mediapm.tools.";
    if prefix.len() >= marker.len() && prefix[..marker.len()].eq_ignore_ascii_case(marker) {
        &prefix[marker.len()..]
    } else {
        prefix
    }
}

/// Resolves one host command selector path for the active platform.
fn resolve_host_command_selector_path(
    command_selector: &str,
) -> Result<Option<String>, MediaPmError> {
    if command_selector.contains("context.os") {
        let selectors = extract_platform_conditional_paths(command_selector)?;
        return Ok(selectors.get(std::env::consts::OS).cloned());
    }

    let trimmed = command_selector.trim();
    if trimmed.is_empty() { Ok(None) } else { Ok(Some(trimmed.to_string())) }
}

/// Normalizes one managed-tool relative command path for install-root lookup.
fn normalize_managed_tool_relative_command_path(relative_command_path: &str) -> Option<String> {
    let normalized = relative_command_path
        .trim()
        .replace('\\', "/")
        .trim_start_matches("./")
        .trim_start_matches('/')
        .to_string();

    if normalized.is_empty() {
        return None;
    }

    let path = Path::new(&normalized);
    if path.components().any(|component| matches!(component, std::path::Component::ParentDir)) {
        return None;
    }

    if path.is_absolute() {
        return None;
    }

    Some(
        Path::new(&normalized)
            .components()
            .map(|component| component.as_os_str().to_string_lossy().to_string())
            .collect::<Vec<_>>()
            .join("/"),
    )
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
