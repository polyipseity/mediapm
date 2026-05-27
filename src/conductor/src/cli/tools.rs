//! Tool import/remove helpers and CAS passthrough injection for the conductor CLI.
//!
//! These standalone functions implement the core logic for tool import
//! registration, content-map merging, external-data removal, tool removal,
//! and CAS argument injection.

use std::collections::BTreeMap;
use std::path::{Path, PathBuf};

use mediapm_cas::Hash;

use crate::error::ConductorError;
use crate::model::config::{
    AddToolOptions, MachineNickelDocument, ToolConfigSpec, ToolKindSpec, ToolSpec,
};

use super::document_io::{load_machine_document, save_machine_document};
use super::{RemoveArgs, RemoveCommand};

/// Handles remove command variants.
pub(super) fn handle_remove(
    _user_ncl: &Path,
    machine_ncl: &Path,
    args: RemoveArgs,
) -> Result<(), ConductorError> {
    match args.command {
        RemoveCommand::Data { hash } => remove_data(machine_ncl, &hash),
        RemoveCommand::Tool { name, metadata } => remove_tool(machine_ncl, &name, metadata),
    }
}

/// Registers imported tool metadata in the machine document when missing and
/// merges imported content-map entries into machine runtime config.
///
/// Invariants:
/// - all end-user automation mutates only `conductor.machine.ncl`,
/// - a tool imported through this path is immediately runnable without
///   duplicating metadata in `conductor.ncl`,
/// - builtin tools never receive `content_map`.
pub(super) fn register_or_merge_imported_tool(
    machine: &mut MachineNickelDocument,
    tool_name: &str,
    import_path: &Path,
    process_name: Option<&str>,
    imported_content_map: BTreeMap<String, Hash>,
    description_override: Option<&str>,
) -> Result<(), ConductorError> {
    if imported_content_map.is_empty() {
        return Err(ConductorError::Workflow(format!(
            "tool import for '{tool_name}' produced no files"
        )));
    }

    if !machine.tools.contains_key(tool_name) {
        let resolved_process_name = resolve_import_process_name(
            import_path,
            process_name,
            imported_content_map.keys().next().map(String::as_str),
        )?;
        machine.add_tool(
            tool_name,
            AddToolOptions::new(ToolSpec {
                kind: ToolKindSpec::Executable {
                    command: vec![resolved_process_name],
                    env_vars: BTreeMap::new(),
                    success_codes: vec![0],
                },
                ..ToolSpec::default()
            })
            .with_tool_config(ToolConfigSpec {
                max_concurrent_calls: -1,
                max_retries: -1,
                description: Some(description_override.map_or_else(
                    || format!("Imported by conductor CLI from '{}'", import_path.display()),
                    std::string::ToString::to_string,
                )),
                input_defaults: BTreeMap::new(),
                env_vars: BTreeMap::new(),
                content_map: Some(imported_content_map),
            }),
        )?;
        return Ok(());
    }

    if matches!(
        machine.tools.get(tool_name).map(|spec| &spec.kind),
        Some(ToolKindSpec::Builtin { .. })
    ) {
        return Err(ConductorError::Workflow(format!(
            "tool '{tool_name}' is builtin and cannot receive imported executable content_map"
        )));
    }

    let map = machine
        .tool_configs
        .entry(tool_name.to_string())
        .or_default()
        .content_map
        .get_or_insert_with(BTreeMap::new);
    for (relative_path, hash) in imported_content_map {
        map.insert(relative_path, hash);
    }

    machine.sync_tool_content_external_data_roots();

    Ok(())
}

/// Resolves executable process name used when import needs to bootstrap one
/// missing machine tool definition.
pub(super) fn resolve_import_process_name(
    import_path: &Path,
    process_name: Option<&str>,
    fallback_relative_file: Option<&str>,
) -> Result<String, ConductorError> {
    if let Some(explicit) = process_name {
        if explicit.trim().is_empty() {
            return Err(ConductorError::Workflow(
                "--process-name cannot be empty when provided".to_string(),
            ));
        }
        return Ok(explicit.to_string());
    }

    if import_path.is_file() {
        let Some(relative) = fallback_relative_file else {
            return Err(ConductorError::Workflow(
                "tool import expected at least one file when deriving process name".to_string(),
            ));
        };
        return Ok(relative.to_string());
    }

    Err(ConductorError::Workflow(
        "tool import from a directory must specify --process-name when creating new machine tool metadata"
            .to_string(),
    ))
}

/// Removes one external-data reference from the program-edited document.
fn remove_data(machine_ncl: &Path, hash: &str) -> Result<(), ConductorError> {
    let hash = hash.parse::<Hash>().map_err(|source| {
        ConductorError::Workflow(format!(
            "external data key '{hash}' is not a valid CAS hash: {source}"
        ))
    })?;
    let mut machine = load_machine_document(machine_ncl)?;
    let removed = machine.external_data.remove(&hash);
    if removed.is_none() {
        return Err(ConductorError::Workflow(format!(
            "external data '{hash}' is not present in conductor.machine.ncl"
        )));
    }

    machine.sync_tool_content_external_data_roots();

    save_machine_document(machine_ncl, &machine)
}

/// Removes one tool runtime config from the program-edited document.
fn remove_tool(
    machine_ncl: &Path,
    name: &str,
    remove_metadata: bool,
) -> Result<(), ConductorError> {
    let mut machine = load_machine_document(machine_ncl)?;
    let removed = machine.tool_configs.remove(name);
    let metadata_removed = if remove_metadata { machine.tools.remove(name) } else { None };
    if removed.is_none() && metadata_removed.is_none() {
        return Err(ConductorError::Workflow(format!(
            "tool '{name}' is not present in conductor.machine.ncl"
        )));
    }

    machine.sync_tool_content_external_data_roots();

    save_machine_document(machine_ncl, &machine)?;

    Ok(())
}

/// Injects resolved conductor-owned CAS root for passthrough CAS commands when absent.
pub(super) fn inject_cas_root_arg_if_missing(args: &[String], default_root: &Path) -> Vec<String> {
    if args.iter().any(|arg| arg == "--root" || arg.starts_with("--root=")) {
        return args.to_vec();
    }

    let mut injected = vec!["--root".to_string(), default_root.to_string_lossy().to_string()];
    injected.extend(args.iter().cloned());
    injected
}

/// Collects file list for tool import from one file or recursively from one directory.
pub(super) fn collect_tool_files(path: &Path) -> Result<Vec<PathBuf>, ConductorError> {
    if path.is_file() {
        return Ok(vec![path.to_path_buf()]);
    }

    if !path.is_dir() {
        return Err(ConductorError::Workflow(format!(
            "tool import path '{}' does not exist",
            path.display()
        )));
    }

    let mut files = Vec::new();
    collect_tool_files_recursive(path, &mut files)?;
    Ok(files)
}

/// Recursively collects all regular files under one directory.
fn collect_tool_files_recursive(
    path: &Path,
    files: &mut Vec<PathBuf>,
) -> Result<(), ConductorError> {
    let entries = std::fs::read_dir(path).map_err(|source| ConductorError::Io {
        operation: "enumerating tool directory".to_string(),
        path: path.to_path_buf(),
        source,
    })?;

    for entry in entries {
        let entry = entry.map_err(|source| ConductorError::Io {
            operation: "reading tool directory entry".to_string(),
            path: path.to_path_buf(),
            source,
        })?;

        let entry_path = entry.path();
        if entry_path.is_dir() {
            collect_tool_files_recursive(&entry_path, files)?;
        } else if entry_path.is_file() {
            files.push(entry_path);
        }
    }

    Ok(())
}

/// Produces normalized `/`-separated relative path for tool content map keys.
pub(super) fn normalized_relative_path(
    base_dir: &Path,
    file: &Path,
) -> Result<String, ConductorError> {
    let relative = file.strip_prefix(base_dir).map_err(|_| {
        ConductorError::Workflow(format!(
            "tool file '{}' is not under base directory '{}'",
            file.display(),
            base_dir.display()
        ))
    })?;
    Ok(relative.to_string_lossy().replace('\\', "/"))
}
