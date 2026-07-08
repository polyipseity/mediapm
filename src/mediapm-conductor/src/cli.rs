//! Command-line interface for `mediapm-conductor`.

use std::collections::BTreeMap;
use std::path::PathBuf;

use clap::{CommandFactory, Parser, Subcommand};
use clap_complete::Shell;
use mediapm_cas::{ConfiguredCas, FileSystemCas, Hash};
use tokio::sync::OnceCell;

use crate::api::{PathOverrides, RunWorkflowOptions, RuntimeStoragePaths};
use crate::defaults;
use crate::error::ConductorError;
use crate::simple_conductor::SimpleConductor;
use crate::state::OrchestrationState;
use crate::state::versions::decode_state_json;

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

    /// Invalidate a tool call instance by its instance key.
    InvalidateToolCall {
        /// Instance key to invalidate.
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
        /// Use a built-in tool preset.
        #[arg(long)]
        preset: Option<CommonExecutableTool>,
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

/// Well-known tool presets for `import tool --preset`.
#[derive(Debug, Clone, Copy, clap::ValueEnum)]
pub(crate) enum CommonExecutableTool {
    Sd,
}

/// Global conductor runtime initialized once per process.
static CONDUCTOR: OnceCell<SimpleConductor<ConfiguredCas>> = OnceCell::const_new();
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
        },
    );
    match cli.command {
        CliCommand::Run { workflow } => cmd_run(&workflow).await,
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

async fn ensure_conductor() -> Result<&'static SimpleConductor<ConfiguredCas>, ConductorError> {
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
            Ok(SimpleConductor::new(paths, cas))
        })
        .await
}

async fn cmd_run(workflow_name: &str) -> Result<(), ConductorError> {
    let conductor = ensure_conductor().await?;

    use mediapm_utils::progress::ProgressGroup;
    let (_group, pb) = ProgressGroup::with_overall("steps", 0);
    let pb2 = pb.clone();

    let options = RunWorkflowOptions {
        retry_impure: false,
        tool_selector: None,
        step_progress: Some(Box::new(move |completed, total, _step_name| {
            pb2.set_total(total as u64);
            pb2.set_position(completed as u64);
        })),
    };
    let summary = conductor.run_workflow(workflow_name, options).await?;
    pb.finish();
    println!("Workflow '{workflow_name}' completed: {summary:?}");
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
            let state = conductor.get_state()?;
            let json = serde_json::to_string_pretty(&state)
                .map_err(|e| ConductorError::Serialization(e.to_string()))?;
            std::fs::write(&path, &json)
                .map_err(|e| ConductorError::io("writing state export", &path, e))?;
            println!("State exported to '{}'", path.display());
        }
        Some(StateCommand::Import { path }) => {
            let json = std::fs::read_to_string(&path)
                .map_err(|e| ConductorError::io("reading state import", &path, e))?;
            let state: OrchestrationState = decode_state_json(json.as_bytes())
                .map_err(|e| ConductorError::Serialization(e.to_string()))?;
            let conductor = ensure_conductor().await?;
            conductor.replace_resolved_state(state)?;
            println!("State imported from '{}'", path.display());
        }
        Some(StateCommand::InvalidateToolCall { key }) => {
            let conductor = ensure_conductor().await?;
            let mut state = conductor.get_state()?;
            if state.tool_call_instances.remove(&key).is_some() {
                conductor.replace_resolved_state(state)?;
                println!("Invalidated tool call instance '{key}'");
            } else {
                eprintln!("Tool call instance '{key}' not found");
            }
        }
    }
    Ok(())
}

async fn cmd_import(args: ImportArgs) -> Result<(), ConductorError> {
    match args.command {
        ImportCommand::Tool { path: Some(p), name, process_name, preset: None } => {
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
        ImportCommand::Tool { path, name, process_name, preset: Some(_) } => {
            // Tool-preset download requires the `tool-presets` Cargo feature.
            let _ = (path, name, process_name);
            return Err(ConductorError::Workflow(
                "tool preset import requires the `tool-presets` feature".to_string(),
            ));
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
