//! Command-line interface for `mediapm-conductor`.

use std::collections::BTreeMap;
use std::path::PathBuf;
use std::sync::Arc;

#[cfg(feature = "progress")]
use mediapm_utils::progress::ProgressScreenApi;

use clap::{CommandFactory, Parser, Subcommand};
use clap_complete::Shell;
use mediapm_cas::{CasApi, CasMaintenanceApi, ConfiguredCas, FileSystemCas, Hash};
use tokio::sync::OnceCell;

use crate::api::{PathOverrides, RunWorkflowOptions, RuntimeStoragePaths};
use crate::conductor_impl::Conductor;
use crate::defaults;
use crate::error::ConductorError;
use crate::state::ConductorState;
use crate::state::versions::{decode_state_json, encode_state_json};

// ---------------------------------------------------------------------------
// CLI structure
// ---------------------------------------------------------------------------

#[derive(Parser)]
#[command(name = "conductor", version, about = "mediapm-conductor workflow orchestrator")]
struct Cli {
    /// Override the conductor runtime directory.
    #[arg(long, env = "MEDIAPM_CONDUCTOR_DIR")]
    conductor_dir: Option<PathBuf>,
    /// Override the CAS store directory.
    #[arg(long, env = "MEDIAPM_CAS_STORE_DIR")]
    cas_store_dir: Option<PathBuf>,
    /// Override the schema export directory.
    #[arg(long, env = "MEDIAPM_CONDUCTOR_SCHEMA_DIR")]
    conductor_schema_dir: Option<PathBuf>,
    /// Override the temporary directory.
    #[arg(long, env = "MEDIAPM_CONDUCTOR_TMP_DIR")]
    conductor_tmp_dir: Option<PathBuf>,
    /// Override the tools materialization directory.
    #[arg(long, env = "MEDIAPM_CONDUCTOR_TOOLS_DIR")]
    conductor_tools_dir: Option<PathBuf>,

    #[command(subcommand)]
    command: CliCommand,
}

#[derive(Subcommand)]
enum CliCommand {
    /// Run a workflow by name.
    Run {
        /// Name of the workflow to execute.
        workflow: String,
        /// Re-execute every step even when its instance is cached.
        ///
        /// Explicit invalidation: instances are never invalidated by the
        /// passage of time, so this flag (or changed step material) is the
        /// way to force a fresh run.
        #[arg(long)]
        force_rerun: bool,
    },
    /// Inspect or manipulate orchestration state.
    State(StateArgs),
    /// Import tools or data.
    Import(ImportArgs),
    /// Remove tools or data.
    Remove(RemoveArgs),
    /// Inspect or run tools.
    Tool(ToolArgs),
    /// Passthrough to the CAS CLI.
    Cas {
        /// Arguments forwarded to the CAS binary.
        #[arg(trailing_var_arg = true, allow_hyphen_values = true)]
        args: Vec<String>,
    },
    /// Export schemas to a directory.
    ExportSchemas {
        /// Output directory for schema files.
        #[arg(default_value = "schemas")]
        output: PathBuf,
    },
    /// Run garbage collection on the CAS store.
    Gc,
    /// Generate shell completions.
    Completions {
        /// Target shell.
        shell: Shell,
    },
}

#[derive(clap::Args)]
struct StateArgs {
    #[command(subcommand)]
    command: Option<StateCommand>,
}

#[derive(Subcommand)]
enum StateCommand {
    /// Compile and display the effective configuration.
    Compile,
    /// Export state to a JSON file.
    Export {
        /// Output file path.
        path: PathBuf,
    },
    /// Import state from a JSON file.
    Import {
        /// Input file path.
        path: PathBuf,
    },
    /// Show runtime diagnostics.
    Show,

    /// Mark a tool call instance as not to be reused.
    ///
    /// The cache probe answers "miss" for a marked instance, so the work behind
    /// it runs again on the next sync. Nothing is deleted: the record, its
    /// outputs, and the blobs they name stay until garbage collection finds them
    /// unreachable, and re-running the work clears the marker by recording a
    /// fresh instance. Undo with `state revalidate-tool-call`.
    InvalidateToolCall {
        /// Instance key to invalidate.
        key: String,
    },
    /// Clear a tool call instance's invalidation marker.
    ///
    /// Undoes [`StateCommand::InvalidateToolCall`]: the recorded instance
    /// becomes reusable again. A re-execution that landed on the same key has
    /// already cleared the marker by rewriting the record, so this reports that
    /// there was nothing to clear; one that landed on a new key leaves the old
    /// record marked, and clearing it is still meaningful because that record
    /// can still be projected by the materializer.
    RevalidateToolCall {
        /// Instance key to revalidate.
        key: String,
    },
}

#[derive(clap::Args)]
struct ImportArgs {
    #[command(subcommand)]
    command: ImportCommand,
}

#[derive(Subcommand)]
enum ImportCommand {
    /// Import a tool (directory or file).
    Tool {
        /// Path to the tool directory or binary file.
        path: Option<PathBuf>,
        /// Tool name override.
        #[arg(long)]
        name: Option<String>,
        /// Process name override.
        #[arg(long)]
        process_name: Option<String>,
    },
}

#[derive(clap::Args)]
struct RemoveArgs {
    #[command(subcommand)]
    command: RemoveCommand,
}

#[derive(Subcommand)]
enum RemoveCommand {
    /// Remove external data by hash.
    Data { hash: String },
    /// Remove a tool configuration.
    Tool {
        name: String,
        #[arg(long)]
        metadata: bool,
    },
}

#[derive(clap::Args)]
struct ToolArgs {
    #[command(subcommand)]
    command: ToolCommand,
}

#[derive(Subcommand)]
enum ToolCommand {
    /// Run a tool with passthrough arguments.
    Run {
        /// Tool name to run.
        #[arg(long)]
        tool: String,
        /// Arguments forwarded to the tool.
        #[arg(trailing_var_arg = true, allow_hyphen_values = true)]
        args: Vec<String>,
    },
    /// List configured tools with their binary presence status.
    List,
}

/// Global conductor runtime initialized once per process.
static CONDUCTOR: OnceCell<Conductor<ConfiguredCas>> = OnceCell::const_new();
static CONDUCTOR_DIR_OVERRIDE: OnceCell<Option<PathBuf>> = OnceCell::const_new();
static STORAGE_OVERRIDES: OnceCell<PathOverrides> = OnceCell::const_new();

/// Sets global path overrides for conductor initialization.
fn set_conductor_overrides(conductor_dir_override: Option<PathBuf>, overrides: PathOverrides) {
    let _ = CONDUCTOR_DIR_OVERRIDE.set(conductor_dir_override);
    let _ = STORAGE_OVERRIDES.set(overrides);
}

// ---------------------------------------------------------------------------
// Entry point
// ---------------------------------------------------------------------------

/// Runs the CLI from environment arguments.
///
/// # Errors
///
/// Returns [`ConductorError`] when CLI argument parsing or command dispatch fails.
pub async fn run_from_env() -> Result<(), ConductorError> {
    let cli = Cli::parse_from(std::env::args().skip(1));
    run(cli).await
}

/// Runs the CLI from explicit args (for testing).
///
/// # Errors
///
/// Returns [`ConductorError`] when CLI argument parsing or command dispatch fails.
///
/// Help and version requests are handled by printing to stdout and returning
/// `Ok(())` (they are not treated as errors).
pub async fn run_from_args(args: &[&str]) -> Result<(), ConductorError> {
    let cli = match Cli::try_parse_from(args.iter()) {
        Ok(cli) => cli,
        Err(err)
            if err.kind() == clap::error::ErrorKind::DisplayHelp
                || err.kind() == clap::error::ErrorKind::DisplayVersion =>
        {
            // Help/version is printed by clap internally; return Ok so
            // passthrough callers don't treat it as a routing failure.
            return Ok(());
        }
        Err(err) => {
            return Err(ConductorError::Workflow(format!("CLI parse error: {err}")));
        }
    };
    run(cli).await
}

// ---------------------------------------------------------------------------
// Command dispatch
// ---------------------------------------------------------------------------

async fn run(cli: Cli) -> Result<(), ConductorError> {
    set_conductor_overrides(
        cli.conductor_dir,
        PathOverrides {
            store: cli.cas_store_dir,
            tmp: cli.conductor_tmp_dir,
            schemas: cli.conductor_schema_dir,
            tools: cli.conductor_tools_dir,
            config_docs: None,
            state_file: None,
        },
    );
    match cli.command {
        CliCommand::Run { workflow, force_rerun } => cmd_run(&workflow, force_rerun).await,
        CliCommand::State(state_args) => cmd_state(state_args).await,
        CliCommand::Import(import_args) => cmd_import(import_args).await,
        CliCommand::Remove(remove_args) => cmd_remove(remove_args).await,
        CliCommand::Tool(tool_args) => cmd_tool(tool_args).await,
        CliCommand::Cas { args } => cmd_cas(args).await,
        CliCommand::ExportSchemas { output } => cmd_export_schemas(output).await,
        CliCommand::Gc => cmd_gc().await,
        CliCommand::Completions { shell } => {
            cmd_completions(shell);
            Ok(())
        }
    }
}

// ---------------------------------------------------------------------------
// Command implementations
// ---------------------------------------------------------------------------

async fn ensure_conductor() -> Result<&'static Conductor<ConfiguredCas>, ConductorError> {
    CONDUCTOR
        .get_or_try_init(|| async {
            let root = crate::runtime_env::discover_project_root()?;
            let overrides = STORAGE_OVERRIDES.get().cloned().unwrap_or_default();
            let conductor_dir = CONDUCTOR_DIR_OVERRIDE
                .get()
                .and_then(Clone::clone)
                .unwrap_or_else(|| root.join(defaults::DEFAULT_CONDUCTOR_DIR_NAME));
            let paths = RuntimeStoragePaths::resolve_for(&conductor_dir, &overrides);
            let cas = ConfiguredCas::FileSystem(FileSystemCas::open(&paths.cas_store_dir).await?);
            // Ensure runtime environment files exist.
            crate::runtime_env::ensure_runtime_env_files(&conductor_dir)?;
            crate::runtime_env::ensure_runtime_gitignore(&conductor_dir)?;
            Ok(Conductor::new(paths, cas))
        })
        .await
}

async fn cmd_run(workflow_name: &str, force_rerun: bool) -> Result<(), ConductorError> {
    use mediapm_utils::progress::ProgressTerminal;
    let conductor = ensure_conductor().await?;

    // The terminal owns the draw target for the workflow screen, so it must
    // outlive every frame the coordinator draws through that screen: dropping
    // it finalizes the renderer. Declaring it ahead of `group` releases it
    // after `group.join()` below.
    let terminal = ProgressTerminal::builder().dynamic_height(true).build();
    let (group, overall): (Arc<dyn ProgressScreenApi + Send + Sync>, _) = {
        let (g, overall) = terminal.screen().with_overall("running workflows", 1).build();
        (Arc::new(g), Arc::new(overall))
    };
    let options = RunWorkflowOptions {
        retry_impure: false,
        force_rerun,
        tool_selector: None,
        progress_group: Some(group.clone()),
        overall_bar: Some(overall),
    };
    let summary = conductor.run_workflow(workflow_name, options).await?;
    group.join();
    let icon = if summary.failed_steps > 0 {
        mediapm_utils::report::StatusIcon::Warning
    } else {
        mediapm_utils::report::StatusIcon::Success
    };
    println!(
        "{}",
        mediapm_utils::report::format_result_line(
            icon,
            &format!("workflow '{workflow_name}'"),
            &[
                ("executed", &summary.executed_steps as &dyn std::fmt::Display),
                ("cached", &summary.cached_steps),
                ("failed", &summary.failed_steps),
                ("retried", &summary.retried_steps),
            ],
            None,
        )
    );
    Ok(())
}

async fn cmd_state(args: StateArgs) -> Result<(), ConductorError> {
    match args.command {
        None | Some(StateCommand::Show) => {
            let conductor = ensure_conductor().await?;
            let diagnostics = conductor.get_runtime_diagnostics().await?;
            println!("{diagnostics:#?}");
        }
        Some(StateCommand::Compile) => {
            let conductor = ensure_conductor().await?;
            let unified = conductor.get_unified_config()?;
            let json = serde_json::to_string_pretty(&unified)
                .map_err(|e| ConductorError::Serialization(e.to_string()))?;
            println!("{json}");
        }
        Some(StateCommand::Export { path }) => {
            let conductor = ensure_conductor().await?;
            conductor.ensure_persisted_state_loaded().await?;
            let state = conductor.get_state()?;
            let json = serde_json::to_string_pretty(&state)
                .map_err(|e| ConductorError::Serialization(e.to_string()))?;
            std::fs::write(&path, &json)
                .map_err(|e| ConductorError::io("writing state export", &path, e))?;
            println!("State exported to '{}'", path.display());
        }
        Some(StateCommand::Import { path }) => {
            let conductor = ensure_conductor().await?;
            import_state_file(conductor, &path)?;
            println!("State imported from '{}'", path.display());
        }
        Some(StateCommand::InvalidateToolCall { key }) => {
            let conductor = ensure_conductor().await?;
            let change = change_instance_invalidation(conductor, &key, true).await?;
            report_marker_change(&key, true, change);
        }
        Some(StateCommand::RevalidateToolCall { key }) => {
            let conductor = ensure_conductor().await?;
            let change = change_instance_invalidation(conductor, &key, false).await?;
            report_marker_change(&key, false, change);
        }
    }
    Ok(())
}

/// What an invalidation or revalidation request did to the recorded state.
///
/// The two outcomes are reported differently on purpose: a change is news, and
/// a request that was already satisfied is not an error — asking twice for the
/// same state is how a script recovers from a run it is unsure about, so it has
/// to succeed without pretending anything happened.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum MarkerChange {
    /// The marker was set or cleared by this request.
    Changed,
    /// The record was already in the requested state.
    AlreadyInRequestedState,
}

/// Sets or clears one instance's invalidation marker.
///
/// Setting a marker that is already set leaves the recorded timestamp alone:
/// the timestamp answers "since when has this work been marked", and moving it
/// forward on a repeated request would erase when the operator first asked. An
/// unknown key is an error rather than a quiet no-op, because the only other
/// explanation for it is a mistyped hash, and a mistyped hash that reports
/// success is a script that believes it invalidated something.
///
/// # Errors
///
/// Returns [`ConductorError::Workflow`] when `state` holds no instance under
/// `key`.
fn set_instance_invalidation(
    state: &mut ConductorState,
    key: &Hash,
    invalidated: bool,
) -> Result<MarkerChange, ConductorError> {
    let Some(instance) = state.tool_call_instances.get_mut(key) else {
        return Err(ConductorError::Workflow(format!("tool call instance '{key}' not found")));
    };
    if invalidated {
        if instance.invalidated_at.is_some() {
            return Ok(MarkerChange::AlreadyInRequestedState);
        }
        instance.invalidated_at = Some(mediapm_utils::Timestamp::now());
    } else {
        if instance.invalidated_at.is_none() {
            return Ok(MarkerChange::AlreadyInRequestedState);
        }
        instance.invalidated_at = None;
    }
    Ok(MarkerChange::Changed)
}

/// Applies an invalidation or revalidation to the persisted conductor state.
///
/// This is the whole body of both state-editing commands, split out so the
/// behaviour can be tested against a temporary state file instead of a process.
/// The order matters: load from disk first (the command may be the first thing
/// the process does), mutate the loaded snapshot, hand it back to the facade,
/// and write it out. The facade persists state after a workflow run only, so a
/// command that edits state has to commit it itself or the edit dies with the
/// process.
///
/// # Errors
///
/// Returns [`ConductorError::Workflow`] when `key_text` is not a hash or names
/// no recorded instance, and any error from loading or writing the state file.
async fn change_instance_invalidation<C>(
    conductor: &Conductor<C>,
    key_text: &str,
    invalidated: bool,
) -> Result<MarkerChange, ConductorError>
where
    C: CasApi + CasMaintenanceApi + Send + Sync + 'static,
{
    conductor.ensure_persisted_state_loaded().await?;
    let mut state = conductor.get_state()?;
    let key: Hash = key_text
        .parse()
        .map_err(|e| ConductorError::Workflow(format!("invalid instance key: {e}")))?;
    let change = set_instance_invalidation(&mut state, &key, invalidated)?;
    if change == MarkerChange::Changed {
        // Durable first, memory second, so the two never disagree when the
        // write fails.
        persist_state(conductor, &state)?;
        conductor.replace_resolved_state(state)?;
    }
    Ok(change)
}

/// Replaces the persisted orchestration state with the document at `path`.
///
/// The write is the point: a façade keeps its snapshot in memory, so an import
/// that only swapped memory would be discarded when the process exited, which
/// is what the command used to do. Durable first, memory second, so a failed
/// write never leaves the process holding an edit the file does not have.
///
/// # Errors
///
/// Returns an error when the file cannot be read or decoded, or when the
/// resulting state cannot be written.
fn import_state_file<C>(
    conductor: &Conductor<C>,
    path: &std::path::Path,
) -> Result<(), ConductorError>
where
    C: CasApi + CasMaintenanceApi + Send + Sync + 'static,
{
    let json = std::fs::read_to_string(path)
        .map_err(|e| ConductorError::io("reading state import", path, e))?;
    let state: ConductorState = decode_state_json(json.as_bytes())
        .map_err(|e| ConductorError::Serialization(e.to_string()))?;
    persist_state(conductor, &state)?;
    conductor.replace_resolved_state(state)
}

/// Writes an orchestration state snapshot to the configured state file.
///
/// Mirrors what the facade does after a workflow run: pretty JSON in the
/// current wire schema, written whole. The write is not staged through a
/// temporary file here, because the facade's own saver is not either and two
/// writers with different atomicity is worse than one shared convention.
///
/// # Errors
///
/// Returns an error when the state cannot be encoded or the file cannot be
/// written.
fn persist_state<C>(conductor: &Conductor<C>, state: &ConductorState) -> Result<(), ConductorError>
where
    C: CasApi + CasMaintenanceApi + Send + Sync + 'static,
{
    let path = &conductor.storage_paths().state_file_path;
    if let Some(parent) = path.parent() {
        std::fs::create_dir_all(parent).map_err(|source| {
            ConductorError::io("creating state file directory", parent, source)
        })?;
    }
    let bytes = encode_state_json(state)?;
    std::fs::write(path, bytes)
        .map_err(|source| ConductorError::io("writing state file", path, source))
}

/// Prints the outcome of a marker request.
///
/// A change names the instance the operator asked about; an unchanged request
/// says which state it found, so a script's log records that nothing was needed
/// rather than leaving a silent success. The polarity is part of the message
/// because "already invalidated" and "not marked" describe opposite states, and
/// a shared wording would let a log be read backwards.
fn marker_change_message(key: &str, invalidated: bool, change: MarkerChange) -> String {
    match (invalidated, change) {
        (true, MarkerChange::Changed) => format!("Invalidated tool call instance '{key}'"),
        (true, MarkerChange::AlreadyInRequestedState) => {
            format!("Tool call instance '{key}' is already invalidated")
        }
        (false, MarkerChange::Changed) => format!("Revalidated tool call instance '{key}'"),
        (false, MarkerChange::AlreadyInRequestedState) => {
            format!("Tool call instance '{key}' is not marked invalid")
        }
    }
}

/// Prints the outcome of a marker request.
///
/// Split from [`marker_change_message`] so the wording of both polarities can be
/// asserted without capturing stdout, which is the only way a swapped literal
/// gets caught.
fn report_marker_change(key: &str, invalidated: bool, change: MarkerChange) {
    println!("{}", marker_change_message(key, invalidated, change));
}

async fn cmd_import(args: ImportArgs) -> Result<(), ConductorError> {
    match args.command {
        ImportCommand::Tool { path: Some(p), name, process_name } => {
            let conductor = ensure_conductor().await?;
            let (hash_map, _count) = crate::cli_tools::import_directory_to_content_map(
                conductor.cas().as_ref(),
                &p,
                &["metadata.json"],
            )
            .await?;
            let content_map: BTreeMap<String, String> =
                hash_map.into_iter().map(|(k, v)| (k, v.to_string())).collect();
            let process_name = process_name.as_deref();
            let executable =
                crate::cli_tools::resolve_import_process_name(&p, process_name, None).ok();
            conductor.add_tool_config(
                &name.unwrap_or_else(|| {
                    p.file_stem()
                        .map_or_else(|| "imported".to_string(), |s| s.to_string_lossy().to_string())
                }),
                executable.as_deref(),
                content_map,
            )?;
        }
        ImportCommand::Tool { .. } => {
            return Err(ConductorError::Workflow(
                "tool import requires a path or --preset".to_string(),
            ));
        }
    }
    Ok(())
}

async fn cmd_remove(args: RemoveArgs) -> Result<(), ConductorError> {
    let conductor = ensure_conductor().await?;
    match args.command {
        RemoveCommand::Data { hash } => {
            let hash: Hash =
                hash.parse().map_err(|e| ConductorError::Workflow(format!("invalid hash: {e}")))?;
            conductor.remove_external_data(&hash).await?;
            println!("Removed external data {hash}");
        }
        RemoveCommand::Tool { name, metadata } => {
            conductor.remove_tool_config(&name, metadata)?;
            println!("Removed tool '{name}' (metadata={metadata})");
        }
    }
    Ok(())
}

async fn cmd_tool(args: ToolArgs) -> Result<(), ConductorError> {
    match args.command {
        ToolCommand::Run { tool, args } => {
            let conductor = ensure_conductor().await?;
            let exit_code = conductor.run_tool_passthrough(&tool, &args).await?;
            std::process::exit(exit_code);
        }
        ToolCommand::List => {
            let conductor = ensure_conductor().await?;
            let unified = conductor.get_unified_config()?;
            println!("tool_id\tbinary_present");
            for (name, spec) in &unified.tools {
                let binary_present = if spec.command_parts.is_empty() {
                    true // builtin tool, always present
                } else {
                    let cmd = &spec.command_parts[0];
                    crate::cli_tools::check_binary_exists(cmd)
                };
                println!("{name}\t{binary_present}");
            }
            Ok(())
        }
    }
}

async fn cmd_cas(args: Vec<String>) -> Result<(), ConductorError> {
    let conductor = ensure_conductor().await?;
    let exit_code = conductor.run_cas_passthrough(&args).await?;
    std::process::exit(exit_code);
}

async fn cmd_export_schemas(output: PathBuf) -> Result<(), ConductorError> {
    let conductor = ensure_conductor().await?;
    conductor.export_schemas(&output)?;
    println!("Schemas exported to '{}'", output.display());
    Ok(())
}

async fn cmd_gc() -> Result<(), ConductorError> {
    let conductor = ensure_conductor().await?;
    conductor.run_gc().await?;
    println!("Garbage collection completed");
    Ok(())
}

fn cmd_completions(shell: Shell) {
    let mut cmd = Cli::command();
    clap_complete::generate(shell, &mut cmd, "conductor", &mut std::io::stdout());
}

#[cfg(test)]
mod tests {
    use std::collections::{BTreeMap, BTreeSet};

    use mediapm_cas::{Hash, InMemoryCas};
    use mediapm_utils::Timestamp;

    use super::{
        MarkerChange, change_instance_invalidation, import_state_file, marker_change_message,
        set_instance_invalidation,
    };
    use crate::api::RuntimeStoragePaths;
    use crate::conductor_impl::Conductor;
    use crate::state::versions::encode_state_json;
    use crate::state::{CapturedOutput, ConductorState, ToolCallInstance};

    /// Builds the state one command test starts from: a single recorded
    /// instance with an output, so a test can prove the record survives a
    /// marker change rather than only that a marker moved.
    fn state_with_one_instance(key: Hash) -> ConductorState {
        let mut state = ConductorState::new_empty();
        state.tool_call_instances.insert(
            key,
            ToolCallInstance {
                instance_key: key,
                tool_call_id: "echo@v1".to_string(),
                impure: false,
                invalidated_at: None,
                executed_at: Timestamp::default(),
                step_ids: BTreeSet::from(["step-0-echo".to_string()]),
                command_args: Vec::new(),
                env_vars: BTreeMap::new(),
                materialized_inputs: BTreeMap::new(),
                outputs: BTreeMap::from([(
                    "content".to_string(),
                    CapturedOutput { hash: Hash::from_content(b"bytes"), capture: None },
                )]),
            },
        );
        state
    }

    /// Opens a conductor over a scratch directory and returns it with the
    /// directory that must outlive it.
    fn conductor_in_scratch_dir(
        state: &ConductorState,
    ) -> (tempfile::TempDir, Conductor<InMemoryCas>) {
        let dir = mediapm_utils::temp::artifact_dir().expect("artifact dir");
        let paths = RuntimeStoragePaths::new(dir.path());
        std::fs::create_dir_all(paths.state_file_path.parent().expect("state file parent"))
            .expect("create state dir");
        let bytes = crate::state::versions::encode_state_json(state).expect("encode state");
        std::fs::write(&paths.state_file_path, bytes).expect("write state file");
        (dir, Conductor::new(paths, mediapm_cas::new_in_memory_cas()))
    }

    /// Reloads the state file, which is the only place a command's effect
    /// outlives its process.
    fn state_on_disk(conductor: &Conductor<InMemoryCas>) -> ConductorState {
        let bytes =
            std::fs::read(&conductor.storage_paths().state_file_path).expect("read state file");
        crate::state::versions::decode_state_json(&bytes).expect("decode state file")
    }

    /// Parses a `conductor state <subcommand> <key>` command line.
    ///
    /// The handler's `match` proves the variants are wired; only a parse proves
    /// the names an operator types, because a renamed subcommand still compiles.
    fn parse_state_command(args: [&str; 4]) -> super::StateCommand {
        use clap::Parser;

        let cli = super::Cli::try_parse_from(args).expect("parse state command");
        let super::CliCommand::State(super::StateArgs { command: Some(command) }) = cli.command
        else {
            panic!("expected a state command for {args:?}");
        };
        command
    }

    #[test]
    fn both_marker_commands_parse_from_the_command_line() {
        assert!(matches!(
            parse_state_command(["conductor", "state", "invalidate-tool-call", "abc"]),
            super::StateCommand::InvalidateToolCall { key } if key == "abc"
        ));
        assert!(matches!(
            parse_state_command(["conductor", "state", "revalidate-tool-call", "abc"]),
            super::StateCommand::RevalidateToolCall { key } if key == "abc"
        ));
    }

    /// A key that names no record fails the command.
    ///
    /// The only other explanation for an unknown key is a mistyped hash, and a
    /// mistyped hash that reports success is a script that believes it
    /// invalidated something.
    #[test]
    fn an_unknown_instance_key_is_an_error() {
        let mut state = ConductorState::new_empty();
        let key = Hash::from_content(b"absent");

        let error = set_instance_invalidation(&mut state, &key, true)
            .expect_err("a key with no record must not report success");

        assert!(
            error.to_string().contains("not found"),
            "the diagnostic must name the problem: {error}",
        );
    }

    /// Marking records when it happened and clearing removes only the marker.
    ///
    /// The recorded outputs are asserted unchanged because invalidation is a
    /// request for the next execution, not a deletion of this one.
    #[test]
    fn marking_records_a_timestamp_and_clearing_removes_it() {
        let key = Hash::from_content(b"instance");
        let mut state = state_with_one_instance(key);

        assert_eq!(
            set_instance_invalidation(&mut state, &key, true).expect("mark"),
            MarkerChange::Changed,
        );
        let marked_at = state.tool_call_instances[&key].invalidated_at;
        assert!(marked_at.is_some(), "marking must record when it happened");
        let outputs = state.tool_call_instances[&key].outputs.clone();

        assert_eq!(
            set_instance_invalidation(&mut state, &key, false).expect("unmark"),
            MarkerChange::Changed,
        );
        assert_eq!(
            state.tool_call_instances[&key].invalidated_at, None,
            "revalidating must remove the marker",
        );
        assert_eq!(
            state.tool_call_instances[&key].outputs, outputs,
            "a marker change must never touch what the instance recorded",
        );
    }

    /// A repeated request is satisfied without moving the timestamp.
    ///
    /// The timestamp says since when the work has been marked, so re-marking
    /// must not restart that clock; and the request itself is not an error,
    /// because a script that retries must be able to succeed.
    #[test]
    fn a_repeated_request_reports_the_state_it_found() {
        let key = Hash::from_content(b"instance");
        let mut state = state_with_one_instance(key);
        // Seeded rather than read after a first mark: two `now()` calls a
        // microsecond apart can be equal on a coarse system clock, which would
        // let a restarting implementation pass. A fixed timestamp makes the
        // comparison exact wherever the suite runs.
        state.tool_call_instances.get_mut(&key).expect("seeded instance").invalidated_at =
            Some(Timestamp::from_unix_nanos(7));

        assert_eq!(
            set_instance_invalidation(&mut state, &key, true).expect("mark again"),
            MarkerChange::AlreadyInRequestedState,
        );
        assert_eq!(
            state.tool_call_instances[&key].invalidated_at,
            Some(Timestamp::from_unix_nanos(7)),
            "a repeated mark must not restart the invalidated-since clock",
        );

        set_instance_invalidation(&mut state, &key, false).expect("unmark");
        assert_eq!(
            set_instance_invalidation(&mut state, &key, false).expect("unmark again"),
            MarkerChange::AlreadyInRequestedState,
        );
    }

    /// The marker has to reach the file, because the probe reads what the
    /// state file said when it was loaded.
    ///
    /// The GC epoch is asserted untouched so this command cannot quietly become
    /// a second writer of the auxiliary data.
    #[tokio::test]
    async fn invalidating_persists_a_marker_the_probe_can_see() {
        let key = Hash::from_content(b"instance");
        let mut seeded = state_with_one_instance(key);
        seeded.aux.conductor_gc_epoch = Timestamp::from_unix_nanos(42);
        let (_dir, conductor) = conductor_in_scratch_dir(&seeded);

        let change = change_instance_invalidation(&conductor, &key.to_string(), true)
            .await
            .expect("invalidate");

        assert_eq!(change, MarkerChange::Changed);
        let recorded = state_on_disk(&conductor);
        let instance = recorded.tool_call_instances.get(&key).expect("record kept");
        assert!(
            instance.invalidated_at.is_some(),
            "the marker must survive the process, or the next sync would reuse the record",
        );
        assert_eq!(
            recorded.aux.conductor_gc_epoch,
            Timestamp::from_unix_nanos(42),
            "a marker change must not disturb the GC clock",
        );
        assert_eq!(
            instance.outputs, seeded.tool_call_instances[&key].outputs,
            "invalidation must keep the recorded outputs, not delete them",
        );
    }

    /// Undo is durable too: the cleared marker must not come back on the next
    /// load.
    #[tokio::test]
    async fn revalidating_persists_the_cleared_marker() {
        let key = Hash::from_content(b"instance");
        let mut seeded = state_with_one_instance(key);
        seeded.tool_call_instances.get_mut(&key).expect("seeded instance").invalidated_at =
            Some(Timestamp::from_unix_nanos(7));
        let (_dir, conductor) = conductor_in_scratch_dir(&seeded);

        let change = change_instance_invalidation(&conductor, &key.to_string(), false)
            .await
            .expect("revalidate");

        assert_eq!(change, MarkerChange::Changed);
        let recorded = state_on_disk(&conductor);
        assert_eq!(
            recorded.tool_call_instances[&key].invalidated_at, None,
            "revalidating must clear the marker on disk",
        );
    }

    /// A refused request writes nothing.
    ///
    /// A command that failed must not leave a rewritten state file behind, or
    /// the next sync would read a state no command admitted to producing.
    #[tokio::test]
    async fn an_unknown_key_leaves_the_state_file_untouched() {
        let key = Hash::from_content(b"instance");
        let (_dir, conductor) = conductor_in_scratch_dir(&state_with_one_instance(key));
        let before = std::fs::read(&conductor.storage_paths().state_file_path).expect("read");

        let error = change_instance_invalidation(
            &conductor,
            &Hash::from_content(b"absent").to_string(),
            true,
        )
        .await
        .expect_err("an unknown key must fail the command");

        assert!(error.to_string().contains("not found"), "unexpected diagnostic: {error}");
        let after = std::fs::read(&conductor.storage_paths().state_file_path).expect("read");
        assert_eq!(before, after, "a failed request must not rewrite the state file");
    }

    /// A key that is not a hash is refused with its own diagnostic, so a
    /// quoting mistake reads differently from a key that is simply unknown.
    #[tokio::test]
    async fn a_malformed_key_is_rejected_before_any_write() {
        let key = Hash::from_content(b"instance");
        let (_dir, conductor) = conductor_in_scratch_dir(&state_with_one_instance(key));
        let before = std::fs::read(&conductor.storage_paths().state_file_path).expect("read");

        let error = change_instance_invalidation(&conductor, "not-a-hash", true)
            .await
            .expect_err("a malformed key must fail the command");

        assert!(
            error.to_string().contains("invalid instance key"),
            "unexpected diagnostic: {error}",
        );
        let after = std::fs::read(&conductor.storage_paths().state_file_path).expect("read");
        assert_eq!(before, after, "a malformed key must not rewrite the state file");
    }

    /// Both message polarities say what happened.
    ///
    /// The two call sites pass a literal, so swapping one would print "is not
    /// marked invalid" after a successful invalidation and no output-based test
    /// would notice.
    #[test]
    fn the_reported_message_agrees_with_the_request() {
        assert_eq!(
            marker_change_message("k", true, MarkerChange::Changed),
            "Invalidated tool call instance 'k'",
        );
        assert_eq!(
            marker_change_message("k", true, MarkerChange::AlreadyInRequestedState),
            "Tool call instance 'k' is already invalidated",
        );
        assert_eq!(
            marker_change_message("k", false, MarkerChange::Changed),
            "Revalidated tool call instance 'k'",
        );
        assert_eq!(
            marker_change_message("k", false, MarkerChange::AlreadyInRequestedState),
            "Tool call instance 'k' is not marked invalid",
        );
    }
    /// An import reaches the file, not only the process.
    ///
    /// The façade's state is an in-memory snapshot, so an import that only
    /// replaced it would vanish at exit — which is what this command did before
    /// it grew a durable write.
    #[test]
    fn importing_persists_the_imported_state() {
        let key = Hash::from_content(b"imported");
        let imported = state_with_one_instance(key);
        // The directory owner has to outlive the write: dropping the handle
        // deletes the tree before the file exists.
        let source_dir = mediapm_utils::temp::artifact_dir().expect("temp dir");
        let source_path = source_dir.path().join("import.json");
        std::fs::write(&source_path, encode_state_json(&imported).expect("encode"))
            .expect("write import source");

        let (_dir, conductor) =
            conductor_in_scratch_dir(&state_with_one_instance(Hash::from_content(b"other")));
        import_state_file(&conductor, &source_path).expect("import");

        let recorded = state_on_disk(&conductor);
        assert!(
            recorded.tool_call_instances.contains_key(&key),
            "the imported instance must survive the process, or the command did nothing",
        );
        assert_eq!(
            recorded.tool_call_instances.len(),
            1,
            "an import replaces the document rather than merging into it",
        );
    }
}
