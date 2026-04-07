//! Command-line interface for `mediapm-conductor`.
//!
//! This module exposes a Phase-2 oriented CLI surface:
//! - workflow execution/state inspection,
//! - program-edited Nickel maintenance through `conductor.machine.ncl`,
//! - direct passthrough command invocation for `cas`.
//!
//! Invariants:
//! - CLI automation mutates only `conductor.machine.ncl`.
//! - `conductor.ncl` remains user-edited input, but it shares the same schema.
//! - CAS mutations always go through configured CAS backends.
//! - passthrough commands forward stdio and preserve external tool output.

use std::collections::{BTreeMap, BTreeSet};
use std::path::{Path, PathBuf};
use std::process::Command;

use clap::{Args, Parser, Subcommand};
use mediapm_cas::{
    CasApi, CasConfig, CasLocatorParseOptions, CasMaintenanceApi, ConfiguredCas, Hash,
};

use crate::api::{
    ConductorApi, RunWorkflowOptions, RuntimeStoragePaths, default_state_paths,
    resolve_runtime_storage_paths,
};
use crate::error::ConductorError;
use crate::model::config::{
    AddExternalDataOptions, AddToolOptions, ExternalContentRef, MachineNickelDocument,
    StateNickelDocument, ToolConfigSpec, ToolKindSpec, ToolSpec, UserNickelDocument,
    decode_machine_document, decode_state_document, decode_user_document, encode_machine_document,
};
use crate::model::state::{decode_state, persisted_state_json_pretty};
use crate::orchestration::SimpleConductor;

/// Default runtime storage root used by the conductor CLI.
const DEFAULT_CONDUCTOR_DIR: &str = ".conductor";

/// Grouped runtime storage path arguments.
#[derive(Debug, Clone, Args)]
struct RuntimePathArgs {
    /// Root directory for runtime-managed artifacts.
    ///
    /// Defaults to `.conductor` relative to the selected config-file parent.
    #[arg(long, global = true, default_value = DEFAULT_CONDUCTOR_DIR)]
    conductor_dir: PathBuf,

    /// Optional override path for the volatile state document.
    ///
    /// Defaults to `<conductor_dir>/state.ncl`.
    #[arg(long = "config-state", global = true)]
    config_state: Option<PathBuf>,

    /// CAS backend locator string or filesystem directory path.
    ///
    /// Accepts any CAS locator (plain filesystem path, URL, or other locator
    /// format supported by `mediapm-cas`). Defaults to `<conductor_dir>/store`.
    #[arg(long, global = true)]
    cas_store_dir: Option<String>,
}

/// Top-level conductor CLI parser.
#[derive(Debug, Parser)]
#[command(name = "conductor", about = "Phase 2 mediapm conductor CLI")]
pub struct Cli {
    /// Grouped runtime storage path arguments.
    #[command(flatten)]
    runtime_paths: RuntimePathArgs,

    /// Path to the user-edited configuration document (`conductor.ncl` by default).
    #[arg(long, global = true)]
    config: Option<PathBuf>,

    /// Path to the program-edited configuration document (`conductor.machine.ncl` by default).
    #[arg(long = "config-machine", global = true)]
    config_machine: Option<PathBuf>,

    /// Top-level CLI command.
    #[command(subcommand)]
    command: CliCommand,
}

/// Top-level conductor CLI commands.
#[derive(Debug, Subcommand)]
pub enum CliCommand {
    /// Executes workflows and updates orchestration state.
    Run {
        /// Allows conflicting tool redefinitions to override existing locked
        /// machine definitions for the same immutable tool name.
        #[arg(long, default_value_t = false)]
        allow_tool_redefinition: bool,
    },
    /// Prints current migrated orchestration state.
    State,
    /// Imports tool/data content into CAS and Nickel docs.
    Import(ImportArgs),
    /// Removes tool/data references from Nickel docs.
    Remove(RemoveArgs),
    /// Runs root-based garbage collection in CAS.
    Gc,
    /// Passthrough to Phase-1 CAS CLI.
    Cas(PassthroughArgs),
}

/// Import command group.
#[derive(Debug, Args)]
pub struct ImportArgs {
    /// Import variant.
    #[command(subcommand)]
    command: ImportCommand,
}

/// Import variants.
#[derive(Debug, Subcommand)]
pub enum ImportCommand {
    /// Registers tool file(s) in CAS and updates machine tool metadata/config.
    Tool {
        /// Path to one tool file or tool directory.
        path: PathBuf,
        /// Logical tool name.
        #[arg(long)]
        name: String,
        /// Optional executable process path recorded as
        /// `tools.<name>.command[0]`
        /// when this import must register new machine tool metadata.
        ///
        /// When omitted and `path` is one file, the default process path is
        /// that file's config-root-relative import key.
        ///
        /// When omitted and `path` is one directory, import fails with an
        /// explicit error because process entrypoint selection is ambiguous.
        #[arg(long)]
        process_name: Option<String>,
    },
    /// Registers external data in CAS and records the reference in
    /// `conductor.machine.ncl`.
    Data {
        /// Path to one data file.
        path: PathBuf,
        /// Optional description override. Defaults to file name.
        #[arg(long)]
        description: Option<String>,
    },
}

/// Remove command group.
#[derive(Debug, Args)]
pub struct RemoveArgs {
    /// Remove variant.
    #[command(subcommand)]
    command: RemoveCommand,
}

/// Remove variants.
#[derive(Debug, Subcommand)]
pub enum RemoveCommand {
    /// Removes one external-data reference from `conductor.machine.ncl`.
    Data {
        /// External data name.
        name: String,
    },
    /// Removes one tool content map from `conductor.machine.ncl`.
    Tool {
        /// Tool logical name.
        name: String,
        /// Also removes any same-named tool metadata stored in `conductor.machine.ncl`.
        #[arg(long)]
        metadata: bool,
    },
}

/// Generic passthrough-argument holder.
#[derive(Debug, Args)]
pub struct PassthroughArgs {
    /// Trailing passthrough arguments.
    #[arg(trailing_var_arg = true, allow_hyphen_values = true)]
    args: Vec<String>,
}

/// Parses CLI from process arguments and executes it.
pub async fn run_from_env() -> Result<(), ConductorError> {
    let cli = Cli::parse();
    run(cli).await
}

/// Executes one parsed CLI command.
pub async fn run(cli: Cli) -> Result<(), ConductorError> {
    let (default_user, default_machine) = default_state_paths();
    let user_ncl = cli.config.unwrap_or(default_user);
    let machine_ncl = cli.config_machine.unwrap_or(default_machine);

    let runtime_storage_paths = RuntimeStoragePaths {
        conductor_dir: cli.runtime_paths.conductor_dir,
        config_state: cli.runtime_paths.config_state,
        cas_store_dir: None,
    };
    let resolved_runtime_paths =
        resolve_runtime_storage_paths(&user_ncl, &machine_ncl, &runtime_storage_paths);

    let cas_locator = cli
        .runtime_paths
        .cas_store_dir
        .unwrap_or_else(|| resolved_runtime_paths.cas_store_dir.to_string_lossy().to_string());

    match cli.command {
        CliCommand::Cas(args) => passthrough_cas(&args.args),
        other => {
            export_nickel_config_schemas(&resolved_runtime_paths.conductor_dir)?;
            let cas = open_cas(&cas_locator).await?;
            match other {
                CliCommand::Run { allow_tool_redefinition } => {
                    run_workflow(
                        cas,
                        &user_ncl,
                        &machine_ncl,
                        allow_tool_redefinition,
                        runtime_storage_paths,
                    )
                    .await
                }
                CliCommand::State => print_state(cas).await,
                CliCommand::Import(args) => handle_import(cas, &user_ncl, &machine_ncl, args).await,
                CliCommand::Remove(args) => handle_remove(&user_ncl, &machine_ncl, args),
                CliCommand::Gc => {
                    run_gc(cas, &user_ncl, &machine_ncl, &resolved_runtime_paths.config_state).await
                }
                CliCommand::Cas(_) => {
                    unreachable!("passthrough handled above")
                }
            }
        }
    }
}

fn export_nickel_config_schemas(runtime_storage_dir: &Path) -> Result<(), ConductorError> {
    let export_dir = schema_export_dir(runtime_storage_dir);
    std::fs::create_dir_all(&export_dir).map_err(|source| ConductorError::Io {
        operation: "creating runtime schema export directory".to_string(),
        path: export_dir.clone(),
        source,
    })?;

    let schemas = [
        ("mod.ncl", include_str!("model/config/versions/mod.ncl")),
        ("v1.ncl", include_str!("model/config/versions/v1.ncl")),
    ];

    for (file_name, content) in schemas {
        let path = export_dir.join(file_name);
        std::fs::write(&path, content).map_err(|source| ConductorError::Io {
            operation: format!("writing exported Nickel schema '{file_name}'"),
            path,
            source,
        })?;
    }

    Ok(())
}

fn schema_export_dir(runtime_storage_dir: &Path) -> PathBuf {
    runtime_storage_dir.join("config")
}

/// Opens configured CAS backend from locator string.
async fn open_cas(locator: &str) -> Result<ConfiguredCas, ConductorError> {
    let config = CasConfig::from_locator_with_options(
        locator,
        CasLocatorParseOptions { allow_plain_filesystem_path: true },
    )
    .map_err(|err| ConductorError::Workflow(format!("invalid CAS locator '{locator}': {err}")))?;

    config
        .open()
        .await
        .map_err(|err| ConductorError::Workflow(format!("failed opening CAS backend: {err}")))
}

/// Executes workflow and prints run summary as pretty JSON.
async fn run_workflow(
    cas: ConfiguredCas,
    user_ncl: &Path,
    machine_ncl: &Path,
    allow_tool_redefinition: bool,
    runtime_storage_paths: RuntimeStoragePaths,
) -> Result<(), ConductorError> {
    let conductor = SimpleConductor::new(cas);
    let summary = conductor
        .run_workflow_with_options(
            user_ncl,
            machine_ncl,
            RunWorkflowOptions { allow_tool_redefinition, runtime_storage_paths },
        )
        .await?;
    println!("executed_instances={}", summary.executed_instances);
    println!("cached_instances={}", summary.cached_instances);
    println!("rematerialized_instances={}", summary.rematerialized_instances);
    Ok(())
}

/// Prints current orchestration state as pretty JSON.
async fn print_state(cas: ConfiguredCas) -> Result<(), ConductorError> {
    let conductor = SimpleConductor::new(cas);
    let state = conductor.get_state().await?;
    let rendered = persisted_state_json_pretty(&state)?;
    println!("{rendered}");
    Ok(())
}

/// Handles import command variants.
async fn handle_import(
    cas: ConfiguredCas,
    _user_ncl: &Path,
    machine_ncl: &Path,
    args: ImportArgs,
) -> Result<(), ConductorError> {
    match args.command {
        ImportCommand::Tool { path, name, process_name } => {
            import_tool(cas, machine_ncl, &path, &name, process_name.as_deref()).await
        }
        ImportCommand::Data { path, description } => {
            import_data(cas, machine_ncl, &path, description.as_deref()).await
        }
    }
}

/// Handles remove command variants.
fn handle_remove(
    _user_ncl: &Path,
    machine_ncl: &Path,
    args: RemoveArgs,
) -> Result<(), ConductorError> {
    match args.command {
        RemoveCommand::Data { name } => remove_data(machine_ncl, &name),
        RemoveCommand::Tool { name, metadata } => remove_tool(machine_ncl, &name, metadata),
    }
}

/// Imports one tool path into CAS and updates tool runtime content-map config in the
/// program-edited document.
async fn import_tool(
    cas: ConfiguredCas,
    machine_ncl: &Path,
    path: &Path,
    tool_name: &str,
    process_name: Option<&str>,
) -> Result<(), ConductorError> {
    if tool_name.trim().is_empty() {
        return Err(ConductorError::Workflow("tool name cannot be empty".to_string()));
    }

    let mut machine = load_machine_document(machine_ncl)?;

    let files = collect_tool_files(path)?;
    let base_dir = if path.is_dir() {
        path.to_path_buf()
    } else {
        path.parent().unwrap_or_else(|| Path::new(".")).to_path_buf()
    };

    let mut imported_content_map = BTreeMap::new();

    for file in files {
        let content = std::fs::read(&file).map_err(|source| ConductorError::Io {
            operation: "reading tool file for import".to_string(),
            path: file.clone(),
            source,
        })?;
        let hash = cas.put(content).await?;
        let relative = normalized_relative_path(&base_dir, &file)?;
        imported_content_map.insert(relative, hash);
    }

    register_or_merge_imported_tool(
        &mut machine,
        tool_name,
        path,
        process_name,
        imported_content_map,
    )?;

    save_machine_document(machine_ncl, &machine)?;
    Ok(())
}

/// Imports one external data file into CAS and records it in the
/// program-edited document.
async fn import_data(
    cas: ConfiguredCas,
    machine_ncl: &Path,
    path: &Path,
    description: Option<&str>,
) -> Result<(), ConductorError> {
    let mut machine = load_machine_document(machine_ncl)?;
    let bytes = std::fs::read(path).map_err(|source| ConductorError::Io {
        operation: "reading external data for import".to_string(),
        path: path.to_path_buf(),
        source,
    })?;
    let hash = cas.put(bytes).await?;
    let name = path
        .file_name()
        .and_then(|name| name.to_str())
        .ok_or_else(|| {
            ConductorError::Workflow(format!(
                "external data path '{}' must end in a valid UTF-8 file name",
                path.display()
            ))
        })?
        .to_string();

    machine.add_external_data(
        name.clone(),
        AddExternalDataOptions::new(ExternalContentRef {
            hash,
            description: description
                .map(std::string::ToString::to_string)
                .or_else(|| Some(name.clone())),
        })
        .overwrite_existing(true),
    )?;
    save_machine_document(machine_ncl, &machine)?;
    Ok(())
}

/// Registers imported tool metadata in the machine document when missing and
/// merges imported content-map entries into machine runtime config.
///
/// Invariants:
/// - all end-user automation mutates only `conductor.machine.ncl`,
/// - a tool imported through this path is immediately runnable without
///   duplicating metadata in `conductor.ncl`,
/// - builtin tools never receive `content_map`.
fn register_or_merge_imported_tool(
    machine: &mut MachineNickelDocument,
    tool_name: &str,
    import_path: &Path,
    process_name: Option<&str>,
    imported_content_map: BTreeMap<String, Hash>,
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
                description: Some(format!(
                    "Imported by conductor CLI from '{}'",
                    import_path.display()
                )),
                input_defaults: BTreeMap::new(),
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

    Ok(())
}

/// Resolves executable process name used when import needs to bootstrap one
/// missing machine tool definition.
fn resolve_import_process_name(
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
fn remove_data(machine_ncl: &Path, name: &str) -> Result<(), ConductorError> {
    let mut machine = load_machine_document(machine_ncl)?;
    let removed = machine.external_data.remove(name);
    if removed.is_none() {
        return Err(ConductorError::Workflow(format!(
            "external data '{name}' is not present in conductor.machine.ncl"
        )));
    }

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

    save_machine_document(machine_ncl, &machine)?;

    Ok(())
}

/// Runs root-based GC using references from user/machine docs and state pointer.
async fn run_gc(
    cas: ConfiguredCas,
    user_ncl: &Path,
    machine_ncl: &Path,
    state_ncl: &Path,
) -> Result<(), ConductorError> {
    let user = load_user_document(user_ncl)?;
    let machine = load_machine_document(machine_ncl)?;
    let state = load_state_document(state_ncl)?;

    let mut roots: BTreeSet<Hash> = BTreeSet::new();
    roots.extend(user.external_data.values().map(|entry| entry.hash));
    roots.extend(machine.external_data.values().map(|entry| entry.hash));
    roots.extend(
        user.tool_configs
            .values()
            .flat_map(|config| config.content_map.iter().flat_map(|map| map.values().copied())),
    );
    roots.extend(
        machine
            .tool_configs
            .values()
            .flat_map(|config| config.content_map.iter().flat_map(|map| map.values().copied())),
    );

    if let Some(pointer) = state.state_pointer {
        roots.insert(pointer);

        if cas.exists(pointer).await? {
            let state_blob = cas.get(pointer).await?;
            let state = decode_state(&state_blob)?;
            for instance in state.instances.values() {
                roots.extend(instance.outputs.values().map(|output| output.hash));
                roots.extend(instance.inputs.values().map(|input| input.hash));
            }
        }
    }

    let roots_vec: Vec<Hash> = roots.into_iter().collect();
    let optimize = cas.optimize_once(mediapm_cas::OptimizeOptions::default()).await?;
    let pruned = cas.prune_constraints().await?;

    println!("gc_roots_computed={}", roots_vec.len());
    println!("optimize_rewritten_objects={}", optimize.rewritten_objects);
    println!("constraints_removed_candidates={}", pruned.removed_candidates);
    Ok(())
}

/// Executes passthrough to `mediapm-cas` via cargo.
fn passthrough_cas(args: &[String]) -> Result<(), ConductorError> {
    let status = Command::new("cargo")
        .arg("run")
        .arg("--package")
        .arg("mediapm-cas")
        .arg("--")
        .args(args)
        .status()
        .map_err(|err| {
            ConductorError::Workflow(format!("failed launching CAS passthrough command: {err}"))
        })?;

    if status.success() {
        Ok(())
    } else {
        Err(ConductorError::Workflow(format!("CAS passthrough exited with status {status}")))
    }
}

/// Loads `conductor.ncl` through versioned decoder, returning default when absent.
fn load_user_document(path: &Path) -> Result<UserNickelDocument, ConductorError> {
    if !path.exists() {
        return Ok(UserNickelDocument::default());
    }

    let bytes = std::fs::read(path).map_err(|source| ConductorError::Io {
        operation: "reading conductor.ncl".to_string(),
        path: path.to_path_buf(),
        source,
    })?;

    if bytes.iter().all(u8::is_ascii_whitespace) {
        return Ok(UserNickelDocument::default());
    }

    decode_user_document(&bytes)
}

/// Loads `conductor.machine.ncl` through versioned decoder, returning default when absent.
fn load_machine_document(path: &Path) -> Result<MachineNickelDocument, ConductorError> {
    if !path.exists() {
        return Ok(MachineNickelDocument::default());
    }

    let bytes = std::fs::read(path).map_err(|source| ConductorError::Io {
        operation: "reading conductor.machine.ncl".to_string(),
        path: path.to_path_buf(),
        source,
    })?;

    if bytes.iter().all(u8::is_ascii_whitespace) {
        return Ok(MachineNickelDocument::default());
    }

    decode_machine_document(&bytes)
}

/// Loads one state document path through versioned decoder, returning default
/// when absent.
fn load_state_document(path: &Path) -> Result<StateNickelDocument, ConductorError> {
    if !path.exists() {
        return Ok(StateNickelDocument::default());
    }

    let bytes = std::fs::read(path).map_err(|source| ConductorError::Io {
        operation: "reading state document".to_string(),
        path: path.to_path_buf(),
        source,
    })?;

    if bytes.iter().all(u8::is_ascii_whitespace) {
        return Ok(StateNickelDocument::default());
    }

    decode_state_document(&bytes)
}

/// Saves `conductor.machine.ncl` using versioned encoder.
fn save_machine_document(
    path: &Path,
    document: &MachineNickelDocument,
) -> Result<(), ConductorError> {
    if let Some(parent) = path.parent() {
        std::fs::create_dir_all(parent).map_err(|source| ConductorError::Io {
            operation: "creating parent directory for conductor.machine.ncl".to_string(),
            path: parent.to_path_buf(),
            source,
        })?;
    }

    let bytes = encode_machine_document(document.clone())?;
    std::fs::write(path, bytes).map_err(|source| ConductorError::Io {
        operation: "writing conductor.machine.ncl".to_string(),
        path: path.to_path_buf(),
        source,
    })
}

/// Collects file list for tool import from one file or recursively from one directory.
fn collect_tool_files(path: &Path) -> Result<Vec<PathBuf>, ConductorError> {
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
fn normalized_relative_path(base_dir: &Path, file: &Path) -> Result<String, ConductorError> {
    let relative = file.strip_prefix(base_dir).map_err(|_| {
        ConductorError::Workflow(format!(
            "tool file '{}' is not under base directory '{}'",
            file.display(),
            base_dir.display()
        ))
    })?;
    Ok(relative.to_string_lossy().replace('\\', "/"))
}

#[cfg(test)]
mod tests {
    use super::{
        Cli, CliCommand, ImportArgs, ImportCommand, export_nickel_config_schemas,
        persisted_state_json_pretty, register_or_merge_imported_tool, resolve_import_process_name,
        schema_export_dir,
    };
    use crate::model::config::{
        InputBinding, MachineNickelDocument, ToolInputSpec, ToolKindSpec, ToolOutputSpec, ToolSpec,
    };
    use crate::model::state::{OrchestrationState, ToolCallInstance};
    use clap::Parser;
    use mediapm_cas::Hash;
    use std::collections::BTreeMap;
    use std::path::PathBuf;
    use std::time::{SystemTime, UNIX_EPOCH};

    #[test]
    fn parse_cas_passthrough_preserves_trailing_args() {
        let cli = Cli::parse_from(["conductor", "cas", "put", "example.bin", "--force"]);
        match cli.command {
            CliCommand::Cas(args) => {
                assert_eq!(args.args, vec!["put", "example.bin", "--force"]);
            }
            other => panic!("expected cas command, got {other:?}"),
        }
    }

    #[test]
    fn parse_import_tool_command() {
        let cli = Cli::parse_from(["conductor", "import", "tool", "./tools/zip", "--name", "zip"]);

        match cli.command {
            CliCommand::Import(ImportArgs { command: ImportCommand::Tool { name, .. } }) => {
                assert_eq!(name, "zip");
            }
            other => panic!("expected import tool command, got {other:?}"),
        }
    }

    #[test]
    fn parse_run_with_allow_tool_redefinition_flag() {
        let cli = Cli::parse_from(["conductor", "run", "--allow-tool-redefinition"]);
        match cli.command {
            CliCommand::Run { allow_tool_redefinition } => {
                assert!(allow_tool_redefinition);
            }
            other => panic!("expected run command, got {other:?}"),
        }
    }

    #[test]
    fn parse_grouped_runtime_storage_path_options() {
        let cli = Cli::parse_from([
            "conductor",
            "--conductor-dir",
            "runtime/.conductor-custom",
            "--config-state",
            "runtime/state.custom.ncl",
            "--cas-store-dir",
            "runtime/cas-root",
            "run",
        ]);

        assert_eq!(cli.runtime_paths.conductor_dir, PathBuf::from("runtime/.conductor-custom"));
        assert_eq!(cli.runtime_paths.config_state, Some(PathBuf::from("runtime/state.custom.ncl")));
        assert_eq!(cli.runtime_paths.cas_store_dir, Some("runtime/cas-root".to_string()));
    }

    #[test]
    fn schema_export_dir_uses_runtime_storage_root() {
        let runtime_storage_dir = PathBuf::from(".conductor");
        assert_eq!(
            schema_export_dir(&runtime_storage_dir),
            PathBuf::from(".conductor").join("config")
        );
    }

    #[test]
    fn schema_export_dir_nests_under_custom_runtime_storage_root() {
        let runtime_storage_dir = PathBuf::from("workspace").join(".conductor-custom");
        assert_eq!(
            schema_export_dir(&runtime_storage_dir),
            PathBuf::from("workspace").join(".conductor-custom").join("config")
        );
    }

    #[test]
    fn export_nickel_config_schemas_writes_schema_files() {
        let unique = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .expect("clock should be monotonic")
            .as_nanos();
        let root = std::env::temp_dir().join(format!("mediapm-conductor-cli-schema-{unique}"));
        let runtime_storage_dir = root.join(".conductor");

        export_nickel_config_schemas(&runtime_storage_dir).expect("schema export should succeed");

        let export_dir = root.join(".conductor").join("config");
        let mod_schema = export_dir.join("mod.ncl");
        let v1_schema = export_dir.join("v1.ncl");

        assert!(mod_schema.exists(), "mod.ncl should be exported");
        assert!(v1_schema.exists(), "v1.ncl should be exported");

        let mod_bytes = std::fs::read(&mod_schema).expect("mod schema should be readable");
        let v1_bytes = std::fs::read(&v1_schema).expect("v1 schema should be readable");

        assert!(!mod_bytes.is_empty(), "mod.ncl should not be empty");
        assert!(!v1_bytes.is_empty(), "v1.ncl should not be empty");

        let _ = std::fs::remove_dir_all(root);
    }

    #[test]
    fn register_or_merge_imported_tool_bootstraps_missing_tool_metadata_in_machine_document() {
        let mut machine = MachineNickelDocument::default();

        register_or_merge_imported_tool(
            &mut machine,
            "demo-tool@1.0.0",
            PathBuf::from("demo.exe").as_path(),
            Some("demo.exe"),
            BTreeMap::from([("demo.exe".to_string(), Hash::from_content(b"demo-a"))]),
        )
        .expect("import registration should bootstrap missing tool metadata");

        assert!(machine.tools.contains_key("demo-tool@1.0.0"));
        assert!(machine.tool_configs.contains_key("demo-tool@1.0.0"));

        let kind = &machine.tools.get("demo-tool@1.0.0").expect("tool metadata should exist").kind;
        let ToolKindSpec::Executable { command, .. } = kind else {
            panic!("bootstrapped tool should be executable");
        };
        assert_eq!(command, &vec!["demo.exe".to_string()]);
    }

    #[test]
    fn register_or_merge_imported_tool_merges_content_for_existing_executable() {
        let mut machine = MachineNickelDocument {
            tools: BTreeMap::from([(
                "demo-tool@1.0.0".to_string(),
                ToolSpec {
                    kind: ToolKindSpec::Executable {
                        command: vec!["demo.exe".to_string()],
                        env_vars: BTreeMap::new(),
                        success_codes: vec![0],
                    },
                    ..ToolSpec::default()
                },
            )]),
            ..MachineNickelDocument::default()
        };

        register_or_merge_imported_tool(
            &mut machine,
            "demo-tool@1.0.0",
            PathBuf::from("demo.exe").as_path(),
            None,
            BTreeMap::from([("payload.txt".to_string(), Hash::from_content(b"demo-b"))]),
        )
        .expect("content-map merge should succeed for existing executable");

        let content_map = machine
            .tool_configs
            .get("demo-tool@1.0.0")
            .and_then(|config| config.content_map.as_ref())
            .expect("content_map should exist after merge");
        assert!(content_map.contains_key("payload.txt"));
    }

    #[test]
    fn resolve_import_process_name_requires_explicit_name_for_directory_bootstrap() {
        let error = resolve_import_process_name(
            PathBuf::from("tool-directory").as_path(),
            None,
            Some("bin/tool"),
        )
        .expect_err("directory bootstrap without explicit process name should fail");

        assert!(error.to_string().contains("--process-name"));
    }

    #[test]
    fn persisted_state_json_pretty_normalizes_builtin_metadata() {
        let state = OrchestrationState {
            version: OrchestrationState::default().version,
            instances: BTreeMap::from([(
                "instance-a".to_string(),
                ToolCallInstance {
                    tool_name: "echo@1.0.0".to_string(),
                    metadata: ToolSpec {
                        is_impure: true,
                        inputs: BTreeMap::from([(
                            "text".to_string(),
                            ToolInputSpec {
                                default: Some(InputBinding::String("fallback".to_string())),
                                ..ToolInputSpec::default()
                            },
                        )]),
                        kind: ToolKindSpec::Builtin {
                            name: "echo".to_string(),
                            version: "1.0.0".to_string(),
                        },
                        outputs: BTreeMap::from([(
                            "result".to_string(),
                            ToolOutputSpec::default(),
                        )]),
                    },
                    impure_timestamp: None,
                    inputs: BTreeMap::new(),
                    outputs: BTreeMap::new(),
                },
            )]),
        };

        let rendered = persisted_state_json_pretty(&state)
            .expect("state rendering should use persistence shape");
        let json: serde_json::Value =
            serde_json::from_str(&rendered).expect("rendered state should be valid JSON");

        assert_eq!(
            json["instances"]["instance-a"]["metadata"],
            serde_json::json!({
                "kind": "builtin",
                "name": "echo",
                "version": "1.0.0"
            })
        );
    }
}
