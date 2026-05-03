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
    CommonExecutableTool, ConductorApi, RunWorkflowOptions, RuntimeStoragePaths,
    default_state_paths, export_nickel_config_schemas, fetch_common_executable_tool_payload,
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

/// Executable suffix used by workspace binaries on the active host platform.
#[cfg(windows)]
const WORKSPACE_BINARY_SUFFIX: &str = ".exe";

/// Executable suffix used by workspace binaries on the active host platform.
#[cfg(not(windows))]
const WORKSPACE_BINARY_SUFFIX: &str = "";

/// Maximum number of parent directories searched when sibling and PATH
/// passthrough binary lookup both miss.
const MAX_ANCESTOR_BINARY_SEARCH_LEVELS: usize = 6;

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
    conductor_state_config: Option<PathBuf>,

    /// CAS backend locator string or filesystem directory path.
    ///
    /// Accepts any CAS locator (plain filesystem path, URL, or other locator
    /// format supported by `mediapm-cas`). Defaults to `<conductor_dir>/store`.
    #[arg(long, global = true)]
    cas_store_dir: Option<String>,

    /// Optional override path for per-step execution sandbox roots.
    ///
    /// Defaults to `<conductor_dir>/tmp`.
    #[arg(long, global = true)]
    conductor_tmp_dir: Option<PathBuf>,

    /// Optional override directory for exported conductor Nickel schemas.
    ///
    /// Defaults to `<conductor_dir>/config/conductor`.
    #[arg(long, global = true)]
    conductor_schema_dir: Option<PathBuf>,

    /// Optional JSON profile artifact output path.
    ///
    /// When set, conductor writes one per-run profiler report at this path.
    #[arg(long = "profile-json", global = true)]
    profile_json: Option<PathBuf>,
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
        ///
        /// This is required unless `--preset` is used.
        #[arg(required_unless_present = "preset", conflicts_with = "preset")]
        path: Option<PathBuf>,
        /// Optional source-install preset for common executable tools.
        ///
        /// When set, the tool binary is fetched from upstream source and
        /// imported directly into machine-managed runtime config.
        #[arg(long)]
        preset: Option<CommonExecutableTool>,
        /// Logical tool name.
        ///
        /// This is required for file/directory imports and optional for
        /// preset imports (defaults to the preset canonical logical name).
        #[arg(long, required_unless_present = "preset")]
        name: Option<String>,
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
        /// External data CAS hash key.
        hash: String,
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
///
/// # Errors
///
/// Returns any workflow, I/O, CAS, or serialization error surfaced while
/// executing the parsed CLI command.
pub async fn run_from_env() -> Result<(), ConductorError> {
    let cli = Cli::parse();
    run(cli).await
}

/// Executes one parsed CLI command.
///
/// # Errors
///
/// Returns any workflow, I/O, CAS, or serialization error produced by the
/// selected subcommand.
pub async fn run(cli: Cli) -> Result<(), ConductorError> {
    let (default_user, default_machine) = default_state_paths();
    let user_ncl = cli.config.unwrap_or(default_user);
    let machine_ncl = cli.config_machine.unwrap_or(default_machine);

    let runtime_storage_paths = RuntimeStoragePaths {
        conductor_dir: cli.runtime_paths.conductor_dir,
        conductor_state_config: cli.runtime_paths.conductor_state_config,
        cas_store_dir: None,
        conductor_tmp_dir: cli.runtime_paths.conductor_tmp_dir,
        conductor_schema_dir: cli.runtime_paths.conductor_schema_dir,
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
            let schema_anchor = resolved_runtime_paths.conductor_schema_dir.as_path();
            export_nickel_config_schemas(schema_anchor)?;
            let cas = open_cas(&cas_locator).await?;
            match other {
                CliCommand::Run { allow_tool_redefinition } => {
                    run_workflow(
                        cas,
                        &user_ncl,
                        &machine_ncl,
                        allow_tool_redefinition,
                        runtime_storage_paths,
                        cli.runtime_paths.profile_json.clone(),
                    )
                    .await
                }
                CliCommand::State => print_state(cas).await,
                CliCommand::Import(args) => handle_import(cas, &user_ncl, &machine_ncl, args).await,
                CliCommand::Remove(args) => handle_remove(&user_ncl, &machine_ncl, args),
                CliCommand::Gc => {
                    run_gc(
                        cas,
                        &user_ncl,
                        &machine_ncl,
                        &resolved_runtime_paths.conductor_state_config,
                    )
                    .await
                }
                CliCommand::Cas(_) => {
                    unreachable!("passthrough handled above")
                }
            }
        }
    }
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
    profile_output_path: Option<PathBuf>,
) -> Result<(), ConductorError> {
    let conductor = SimpleConductor::new(cas);
    let summary = conductor
        .run_workflow_with_options(
            user_ncl,
            machine_ncl,
            RunWorkflowOptions {
                allow_tool_redefinition,
                runtime_storage_paths,
                runtime_inherited_env_vars: Vec::new(),
                profile_output_path,
            },
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
        ImportCommand::Tool { path, preset, name, process_name } => {
            if let Some(tool_preset) = preset {
                return import_common_tool(
                    cas,
                    machine_ncl,
                    tool_preset,
                    name.as_deref(),
                    process_name.as_deref(),
                )
                .await;
            }

            let import_path = path.as_deref().ok_or_else(|| {
                ConductorError::Workflow(
                    "import tool requires a path unless --preset is provided".to_string(),
                )
            })?;
            let tool_name = name.as_deref().ok_or_else(|| {
                ConductorError::Workflow(
                    "import tool requires --name when importing from path".to_string(),
                )
            })?;

            import_tool(cas, machine_ncl, import_path, tool_name, process_name.as_deref()).await
        }
        ImportCommand::Data { path, description } => {
            import_data(cas, machine_ncl, &path, description.as_deref()).await
        }
    }
}

/// Installs one common upstream executable and imports it into machine config.
///
/// The installer fetches the executable bytes through conductor API helper
/// (release-asset download path), stores them in CAS, then wires
/// `tool_configs.<tool>.content_map` plus executable metadata for immediate
/// workflow use.
async fn import_common_tool(
    cas: ConfiguredCas,
    machine_ncl: &Path,
    tool: CommonExecutableTool,
    logical_name_override: Option<&str>,
    process_name_override: Option<&str>,
) -> Result<(), ConductorError> {
    let logical_tool_name = logical_name_override
        .map(str::trim)
        .filter(|value| !value.is_empty())
        .unwrap_or(tool.logical_tool_name())
        .to_string();

    let payload = fetch_common_executable_tool_payload(tool)?;
    let mut machine = load_machine_document(machine_ncl)?;
    let hash = cas.put(payload.executable_bytes).await?;
    let imported_content_map = BTreeMap::from([(payload.executable_file_name.clone(), hash)]);

    let resolved_process_name = process_name_override
        .map(str::trim)
        .filter(|value| !value.is_empty())
        .unwrap_or(payload.executable_file_name.as_str());
    let description_override = format!(
        "Installed by conductor CLI tool preset importer from upstream release assets for '{}'",
        tool.logical_tool_name()
    );

    register_or_merge_imported_tool(
        &mut machine,
        &logical_tool_name,
        Path::new(payload.executable_file_name.as_str()),
        Some(resolved_process_name),
        imported_content_map,
        Some(description_override.as_str()),
    )?;

    save_machine_document(machine_ncl, &machine)?;
    Ok(())
}

/// Handles remove command variants.
fn handle_remove(
    _user_ncl: &Path,
    machine_ncl: &Path,
    args: RemoveArgs,
) -> Result<(), ConductorError> {
    match args.command {
        RemoveCommand::Data { hash } => remove_data(machine_ncl, &hash),
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
        None,
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
    let default_description = path
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
        hash,
        AddExternalDataOptions::new(ExternalContentRef {
            description: description
                .map(std::string::ToString::to_string)
                .or_else(|| Some(default_description.clone())),
            save: None,
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

/// Runs root-based GC using references from user/machine docs and state pointer.
async fn run_gc(
    cas: ConfiguredCas,
    user_ncl: &Path,
    machine_ncl: &Path,
    conductor_state_config: &Path,
) -> Result<(), ConductorError> {
    let user = load_user_document(user_ncl)?;
    let machine = load_machine_document(machine_ncl)?;
    let state = load_state_document(conductor_state_config)?;

    let mut roots: BTreeSet<Hash> = BTreeSet::new();
    roots.extend(user.external_data.keys().copied());
    roots.extend(machine.external_data.keys().copied());
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

/// Returns host-specific executable file name for one binary stem.
#[must_use]
fn workspace_binary_file_name(binary_stem: &str) -> String {
    format!("{binary_stem}{WORKSPACE_BINARY_SUFFIX}")
}

/// Searches one ordered list of directories for the target passthrough binary.
#[must_use]
fn find_binary_in_paths<I>(binary_stem: &str, directories: I) -> Option<PathBuf>
where
    I: IntoIterator<Item = PathBuf>,
{
    let binary_file_name = workspace_binary_file_name(binary_stem);
    directories
        .into_iter()
        .map(|directory| directory.join(&binary_file_name))
        .find(|candidate| candidate.is_file())
}

/// Searches PATH for the target passthrough binary.
#[must_use]
fn find_binary_in_system_path(binary_stem: &str) -> Option<PathBuf> {
    let path = std::env::var_os("PATH")?;
    find_binary_in_paths(binary_stem, std::env::split_paths(&path))
}

/// Searches parent directories for the target passthrough binary.
#[must_use]
fn find_binary_in_ancestors_from(
    binary_stem: &str,
    start_directory: &Path,
    max_levels: usize,
) -> Option<PathBuf> {
    let mut current = Some(start_directory);
    let mut levels_checked = 0usize;
    while let Some(directory) = current {
        if levels_checked > max_levels {
            break;
        }

        if let Some(found) = find_binary_in_paths(binary_stem, [directory.to_path_buf()]) {
            return Some(found);
        }

        current = directory.parent();
        levels_checked = levels_checked.saturating_add(1);
    }

    None
}

/// Resolves one passthrough binary from env override, sibling path, PATH, or
/// ancestor directories.
fn resolve_workspace_binary_path(
    binary_stem: &str,
    env_override_name: Option<&str>,
) -> Result<PathBuf, ConductorError> {
    let binary_file_name = workspace_binary_file_name(binary_stem);
    let current_executable = std::env::current_exe().map_err(|source| ConductorError::Io {
        operation: "resolving current conductor executable path".to_string(),
        path: PathBuf::from("<current-exe>"),
        source,
    })?;

    let executable_directory = current_executable.parent().ok_or_else(|| {
        ConductorError::Workflow(format!(
            "current executable '{}' has no parent directory",
            current_executable.display()
        ))
    })?;
    let sibling_path = executable_directory.join(&binary_file_name);

    let mut attempts = Vec::new();
    if let Some(env_name) = env_override_name
        && let Some(env_value) = std::env::var_os(env_name)
    {
        let env_path = PathBuf::from(env_value);
        attempts.push(format!("${env_name}={}", env_path.display()));
        if env_path.is_file() {
            return Ok(env_path);
        }
    }

    attempts.push(format!("sibling={}", sibling_path.display()));
    if sibling_path.is_file() {
        return Ok(sibling_path);
    }

    attempts.push(format!("PATH ({binary_file_name})"));
    if let Some(path_match) = find_binary_in_system_path(binary_stem) {
        return Ok(path_match);
    }

    attempts.push(format!("ancestor search (max {MAX_ANCESTOR_BINARY_SEARCH_LEVELS} levels)"));
    if let Some(ancestor_match) = find_binary_in_ancestors_from(
        binary_stem,
        executable_directory,
        MAX_ANCESTOR_BINARY_SEARCH_LEVELS,
    ) {
        return Ok(ancestor_match);
    }

    let env_hint = env_override_name.map_or_else(
        || "set an explicit passthrough binary path environment variable".to_string(),
        |name| format!("set {name} to an absolute binary path"),
    );

    Err(ConductorError::Workflow(format!(
        "passthrough binary '{binary_stem}' was not found (attempts: {}). Fix by {} or placing '{}' next to '{}' / on PATH",
        attempts.join("; "),
        env_hint,
        binary_file_name,
        current_executable.display(),
    )))
}

/// Runs one workspace binary by name without build-if-missing behavior.
fn run_workspace_binary(
    _package_name: &str,
    binary_stem: &str,
    args: &[String],
) -> Result<(), ConductorError> {
    let env_override_name = match binary_stem {
        "mediapm-cas" => Some("MEDIAPM_CAS_BINARY"),
        _ => None,
    };
    let binary_path = resolve_workspace_binary_path(binary_stem, env_override_name)?;

    let status =
        Command::new(&binary_path).args(args).status().map_err(|source| ConductorError::Io {
            operation: format!("launching workspace passthrough binary '{binary_stem}'"),
            path: binary_path.clone(),
            source,
        })?;

    if status.success() {
        Ok(())
    } else {
        Err(ConductorError::Workflow(format!(
            "workspace passthrough binary '{}' exited with status {status}",
            binary_path.display()
        )))
    }
}

/// Executes passthrough to `mediapm-cas` through sibling workspace binaries.
fn passthrough_cas(args: &[String]) -> Result<(), ConductorError> {
    run_workspace_binary("mediapm-cas", "mediapm-cas", args)
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
        Cli, CliCommand, CommonExecutableTool, ImportArgs, ImportCommand,
        MAX_ANCESTOR_BINARY_SEARCH_LEVELS, find_binary_in_ancestors_from, find_binary_in_paths,
        persisted_state_json_pretty, register_or_merge_imported_tool, resolve_import_process_name,
        workspace_binary_file_name,
    };
    use crate::model::config::{
        MachineNickelDocument, ToolInputSpec, ToolKindSpec, ToolOutputSpec, ToolSpec,
    };
    use crate::model::state::{OrchestrationState, ToolCallInstance};
    use clap::Parser;
    use mediapm_cas::Hash;
    use std::collections::BTreeMap;
    use std::fs;
    use std::path::PathBuf;

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
            CliCommand::Import(ImportArgs {
                command: ImportCommand::Tool { path, preset, name, process_name },
            }) => {
                assert_eq!(path, Some(PathBuf::from("./tools/zip")));
                assert!(preset.is_none());
                assert_eq!(name, Some("zip".to_string()));
                assert!(process_name.is_none());
            }
            other => panic!("expected import tool command, got {other:?}"),
        }
    }

    #[test]
    fn parse_import_tool_preset_command() {
        let cli = Cli::parse_from(["conductor", "import", "tool", "--preset", "sd"]);

        match cli.command {
            CliCommand::Import(ImportArgs {
                command: ImportCommand::Tool { path, preset, name, process_name },
            }) => {
                assert_eq!(preset, Some(CommonExecutableTool::Sd));
                assert!(path.is_none());
                assert!(name.is_none());
                assert!(process_name.is_none());
            }
            other => panic!("expected import tool --preset command, got {other:?}"),
        }
    }

    /// Protects host-suffix executable-name rendering for passthrough lookup.
    #[test]
    fn workspace_binary_file_name_applies_host_suffix() {
        assert_eq!(
            workspace_binary_file_name("mediapm-cas"),
            format!("mediapm-cas{}", super::WORKSPACE_BINARY_SUFFIX)
        );
    }

    /// Protects deterministic directory-list binary lookup behavior.
    #[test]
    fn find_binary_in_paths_returns_first_existing_match() {
        let root = tempfile::tempdir().expect("tempdir");
        let first = root.path().join("first");
        let second = root.path().join("second");
        fs::create_dir_all(&first).expect("first dir");
        fs::create_dir_all(&second).expect("second dir");

        let binary_name = workspace_binary_file_name("mediapm-cas");
        let binary_path = second.join(binary_name);
        fs::write(&binary_path, b"stub").expect("binary");

        let found = find_binary_in_paths(
            "mediapm-cas",
            vec![first.clone(), second.clone(), root.path().to_path_buf()],
        )
        .expect("binary should be found");

        assert_eq!(found, binary_path);
    }

    /// Protects bounded ancestor lookup semantics for passthrough fallback.
    #[test]
    fn find_binary_in_ancestors_respects_max_level_budget() {
        let root = tempfile::tempdir().expect("tempdir");
        let level_0 = root.path().join("l0");
        let level_1 = level_0.join("l1");
        let level_2 = level_1.join("l2");
        fs::create_dir_all(&level_2).expect("nested directories");

        let binary_name = workspace_binary_file_name("mediapm-cas");
        let binary_path = level_0.join(&binary_name);
        fs::write(&binary_path, b"stub").expect("binary");

        let miss = find_binary_in_ancestors_from("mediapm-cas", &level_2, 1);
        assert!(miss.is_none(), "max level budget should prevent reaching level_0");

        let hit = find_binary_in_ancestors_from(
            "mediapm-cas",
            &level_2,
            MAX_ANCESTOR_BINARY_SEARCH_LEVELS,
        )
        .expect("ancestor lookup should reach level_0 with default budget");
        assert_eq!(hit, binary_path);
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
            "--conductor-tmp-dir",
            "runtime/tmp-root",
            "--conductor-schema-dir",
            "runtime/config/custom-conductor-schemas",
            "run",
        ]);

        assert_eq!(cli.runtime_paths.conductor_dir, PathBuf::from("runtime/.conductor-custom"));
        assert_eq!(
            cli.runtime_paths.conductor_state_config,
            Some(PathBuf::from("runtime/state.custom.ncl"))
        );
        assert_eq!(cli.runtime_paths.cas_store_dir, Some("runtime/cas-root".to_string()));
        assert_eq!(cli.runtime_paths.conductor_tmp_dir, Some(PathBuf::from("runtime/tmp-root")));
        assert_eq!(
            cli.runtime_paths.conductor_schema_dir,
            Some(PathBuf::from("runtime/config/custom-conductor-schemas"))
        );
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
            None,
        )
        .expect("import registration should bootstrap missing tool metadata");

        assert!(machine.tools.contains_key("demo-tool@1.0.0"));
        assert!(machine.tool_configs.contains_key("demo-tool@1.0.0"));

        let kind = &machine.tools.get("demo-tool@1.0.0").expect("tool metadata should exist").kind;
        let ToolKindSpec::Executable { command, .. } = kind else {
            panic!("bootstrapped tool should be executable");
        };
        assert_eq!(command, &vec!["demo.exe".to_string()]);
        assert!(machine.external_data.contains_key(&Hash::from_content(b"demo-a")));
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
            None,
        )
        .expect("content-map merge should succeed for existing executable");

        let content_map = machine
            .tool_configs
            .get("demo-tool@1.0.0")
            .and_then(|config| config.content_map.as_ref())
            .expect("content_map should exist after merge");
        assert!(content_map.contains_key("payload.txt"));
        assert!(machine.external_data.contains_key(&Hash::from_content(b"demo-b")));
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
                        inputs: BTreeMap::from([("text".to_string(), ToolInputSpec::default())]),
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
