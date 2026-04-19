//! Binary entrypoint for the Phase 3 `mediapm` CLI.
//!
//! This CLI exposes:
//! - media/tool declarative state management,
//! - sync/materialization orchestration,
//! - passthrough commands to Phase 1 CAS and Phase 2 conductor CLIs.

use std::path::PathBuf;
use std::process::Command as ProcessCommand;

use clap::{Args, Parser, Subcommand};
use mediapm::{
    MediaPmService, MediaRuntimeStorage, ToolRegistryStatus,
    builtins::media_tagger::InternalMediaTaggerOptions, ensure_global_directory_layout,
    global_tool_cache_clear, global_tool_cache_prune_expired, global_tool_cache_status,
    load_runtime_dotenv_for_root, resolve_default_global_paths,
};
use url::Url;

/// `mediapm` phase-3 CLI.
#[derive(Debug, Parser)]
#[command(author, version, about = "mediapm phase-3 orchestration CLI")]
struct Cli {
    /// Workspace root that hosts `mediapm.ncl` and `.mediapm/` runtime state.
    #[arg(long, default_value = ".")]
    root: PathBuf,
    /// Overrides `runtime.mediapm_dir` for this command invocation.
    #[arg(long)]
    mediapm_dir: Option<PathBuf>,
    /// Overrides `runtime.conductor_config` for this command invocation.
    #[arg(long)]
    conductor_config: Option<PathBuf>,
    /// Overrides `runtime.conductor_machine_config` for this invocation.
    #[arg(long)]
    conductor_machine_config: Option<PathBuf>,
    /// Overrides `runtime.conductor_state` for this command invocation.
    #[arg(long)]
    conductor_state: Option<PathBuf>,
    /// Overrides `runtime.lockfile` for this command invocation.
    #[arg(long)]
    lockfile: Option<PathBuf>,
    /// Overrides `runtime.env_file` for this command invocation.
    #[arg(long)]
    env_file: Option<PathBuf>,
    /// Top-level command selector.
    #[command(subcommand)]
    command: Command,
}

/// Top-level `mediapm` commands.
#[derive(Debug, Subcommand)]
enum Command {
    /// Reconciles desired state and materializes the media library.
    ///
    /// Default policy skips remote update checks for tag-only selectors unless
    /// `--check-tag-updates` is provided.
    Sync(SyncArgs),
    /// Tool lifecycle commands.
    Tools {
        /// Tool subcommand selector.
        #[command(subcommand)]
        command: ToolsCommand,
    },
    /// Media-source registry commands.
    Media {
        /// Media subcommand selector.
        #[command(subcommand)]
        command: MediaCommand,
    },
    /// User-scoped global `mediapm` directory management commands.
    Global {
        /// Global subcommand selector.
        #[command(subcommand)]
        command: GlobalCommand,
    },
    /// Builtin command implementations exposed by the `mediapm` executable.
    Builtins {
        /// Builtins subcommand selector.
        #[command(subcommand)]
        command: BuiltinsCommand,
    },
    /// Internal implementation commands used by managed workflow shims.
    #[command(hide = true)]
    Internal {
        /// Internal subcommand selector.
        #[command(subcommand)]
        command: InternalCommand,
    },
    /// Passthrough to Phase 1 `mediapm-cas` CLI.
    Cas(PassthroughArgs),
    /// Passthrough to Phase 2 conductor CLI.
    Conductor(PassthroughArgs),
}

/// Tool lifecycle commands.
#[derive(Debug, Subcommand)]
enum ToolsCommand {
    /// Reconciles desired tool requirements only (no workflow/materialization run).
    ///
    /// Default policy checks remote updates for tag-only selectors unless
    /// `--no-check-tag-updates` is provided.
    Sync(ToolsSyncArgs),
    /// Lists registered tools and binary status.
    List,
    /// Removes one installed tool binary while keeping metadata.
    Prune {
        /// Immutable tool id.
        #[arg(long)]
        id: String,
    },
}

/// Media-source commands.
#[derive(Debug, Subcommand)]
enum MediaCommand {
    /// Adds one online source URI to `mediapm.ncl`.
    Add {
        /// Source URI (`http` or `https`).
        uri: String,
    },
    /// Adds one local source file and records an `import-once` CAS-hash ingest step.
    AddLocal {
        /// Local source file path.
        path: PathBuf,
    },
}

/// Global-directory management commands.
#[derive(Debug, Subcommand)]
enum GlobalCommand {
    /// Prints resolved global-directory paths.
    Path,
    /// Creates global-directory folders when missing.
    Init,
    /// Global tool-cache management commands.
    ToolCache {
        /// Tool-cache subcommand selector.
        #[command(subcommand)]
        command: GlobalToolCacheCommand,
    },
}

/// Global tool-cache management commands.
#[derive(Debug, Subcommand)]
enum GlobalToolCacheCommand {
    /// Prints tool-cache status and key metadata paths.
    Status,
    /// Evicts cache rows older than the fixed 30-day TTL.
    Prune,
    /// Deletes the entire global tool-cache directory.
    Clear,
}

/// Builtin command implementations exposed by `mediapm builtins ...`.
#[derive(Debug, Subcommand)]
enum BuiltinsCommand {
    /// Native metadata tagging flow (`Chromaprint -> AcoustID -> MusicBrainz`).
    #[command(name = "media-tagger")]
    MediaTagger(InternalMediaTaggerArgs),
}

/// Internal helper commands used by workspace-local tool shims.
#[derive(Debug, Subcommand)]
enum InternalCommand {
    /// Native metadata tagging flow (`Chromaprint -> AcoustID -> MusicBrainz`).
    #[command(name = "media-tagger")]
    MediaTagger(InternalMediaTaggerArgs),
}

/// Arguments for one internal media-tagger invocation.
#[derive(Debug, Args)]
struct InternalMediaTaggerArgs {
    /// Input media payload path.
    #[arg(long)]
    input: PathBuf,
    /// Output media payload path.
    #[arg(long)]
    output: PathBuf,
    /// Optional AcoustID API key override.
    ///
    /// When omitted and no recording MBID override is provided, AcoustID
    /// lookup now fails immediately with a configuration error.
    /// When provided, AcoustID lookup/authentication failures are also
    /// surfaced as hard errors.
    #[arg(long)]
    acoustid_api_key: Option<String>,
    /// AcoustID lookup endpoint.
    #[arg(long, default_value = mediapm::builtins::media_tagger::DEFAULT_ACOUSTID_ENDPOINT)]
    acoustid_endpoint: String,
    /// MusicBrainz endpoint label used in diagnostics.
    #[arg(long, default_value = mediapm::builtins::media_tagger::DEFAULT_MUSICBRAINZ_ENDPOINT)]
    musicbrainz_endpoint: String,
    /// Enables strict failure when recording identity or metadata cannot be resolved.
    #[arg(long, default_value_t = true)]
    strict_identification: bool,
    /// Optional recording MBID override.
    #[arg(long)]
    recording_mbid: Option<String>,
    /// Optional release MBID override.
    #[arg(long)]
    release_mbid: Option<String>,
    /// `ffmpeg` executable used for decode + metadata-write stages.
    #[arg(long, default_value = "ffmpeg")]
    ffmpeg_bin: String,
}

/// Generic passthrough argument holder.
#[derive(Debug, Args)]
struct PassthroughArgs {
    /// Trailing passthrough arguments.
    #[arg(trailing_var_arg = true, allow_hyphen_values = true)]
    args: Vec<String>,
}

/// Optional tag-update policy override flags for sync commands.
#[derive(Debug, Args, Clone, Copy, Default)]
struct TagUpdatePolicyArgs {
    /// Enables remote update checks for tag-only tool selectors.
    #[arg(long, conflicts_with = "no_check_tag_updates")]
    check_tag_updates: bool,
    /// Disables remote update checks for tag-only tool selectors.
    #[arg(long)]
    no_check_tag_updates: bool,
}

impl TagUpdatePolicyArgs {
    /// Resolves effective tag-update policy using command-specific default.
    fn resolve(self, default_value: bool) -> bool {
        if self.check_tag_updates {
            true
        } else if self.no_check_tag_updates {
            false
        } else {
            default_value
        }
    }
}

/// Arguments for top-level `mediapm sync`.
#[derive(Debug, Args, Clone, Copy, Default)]
struct SyncArgs {
    /// Optional override for tag-only tool update checks.
    #[command(flatten)]
    tag_update_policy: TagUpdatePolicyArgs,
}

/// Arguments for `mediapm tools sync`.
#[derive(Debug, Args, Clone, Copy, Default)]
struct ToolsSyncArgs {
    /// Optional override for tag-only tool update checks.
    #[command(flatten)]
    tag_update_policy: TagUpdatePolicyArgs,
}

#[tokio::main]
/// Parses CLI args and executes one top-level command.
async fn main() -> anyhow::Result<()> {
    let cli = Cli::parse();
    let runtime_storage_overrides = MediaRuntimeStorage {
        mediapm_dir: option_path_to_string(cli.mediapm_dir),
        library_dir: None,
        tmp_dir: None,
        conductor_config: option_path_to_string(cli.conductor_config),
        conductor_machine_config: option_path_to_string(cli.conductor_machine_config),
        conductor_state: option_path_to_string(cli.conductor_state),
        inherited_env_vars: None,
        lockfile: option_path_to_string(cli.lockfile),
        env_file: option_path_to_string(cli.env_file),
        schema_config_dir: None,
        use_user_download_cache: None,
    };
    if matches!(
        &cli.command,
        Command::Sync(_)
            | Command::Tools { .. }
            | Command::Media { .. }
            | Command::Builtins { .. }
            | Command::Internal { .. }
    ) {
        let _ = load_runtime_dotenv_for_root(&cli.root, &runtime_storage_overrides)?;
    }
    let service = MediaPmService::new_in_memory_at_with_runtime_storage_overrides(
        &cli.root,
        runtime_storage_overrides,
    );

    match cli.command {
        Command::Sync(args) => {
            let check_tag_updates = args.tag_update_policy.resolve(false);
            let summary = service.sync_library_with_tag_update_checks(check_tag_updates).await?;
            println!(
                "sync complete: executed={}, cached={}, rematerialized={}, materialized={}, removed={}, tools_added={}, tools_updated={}",
                summary.executed_instances,
                summary.cached_instances,
                summary.rematerialized_instances,
                summary.materialized_paths,
                summary.removed_paths,
                summary.added_tools,
                summary.updated_tools,
            );
            for warning in summary.warnings {
                eprintln!("warning: {warning}");
            }
        }
        Command::Tools { command } => match command {
            ToolsCommand::Sync(args) => {
                let check_tag_updates = args.tag_update_policy.resolve(true);
                let summary = service.sync_tools_with_tag_update_checks(check_tag_updates).await?;
                println!(
                    "tool sync complete: added={}, updated={}, unchanged={}",
                    summary.added_tools, summary.updated_tools, summary.unchanged_tools
                );
                for warning in summary.warnings {
                    eprintln!("warning: {warning}");
                }
            }
            ToolsCommand::List => {
                let rows = service.list_tools()?;
                if rows.is_empty() {
                    println!("no tools registered");
                } else {
                    for row in rows {
                        let status = match row.status {
                            ToolRegistryStatus::Active => "active",
                            ToolRegistryStatus::Pruned => "pruned",
                        };
                        println!(
                            "{}\tstatus={}\tbinary_present={}",
                            row.tool_id, status, row.has_binary
                        );
                    }
                }
            }
            ToolsCommand::Prune { id } => {
                let removed_hashes = service.prune_tool(&id).await?;
                println!("pruned tool binary for {id} (removed_hashes={removed_hashes})");
            }
        },
        Command::Media { command } => match command {
            MediaCommand::Add { uri } => {
                let uri = Url::parse(&uri)?;
                let media_id = service.add_media_source(uri)?;
                println!("registered media source id={media_id}");
            }
            MediaCommand::AddLocal { path } => {
                let media_id = service.add_local_source(&path).await?;
                println!("registered local media source id={media_id}");
            }
        },
        Command::Global { command } => match command {
            GlobalCommand::Path => {
                if let Some(paths) = resolve_default_global_paths() {
                    println!("global_dir={}", paths.root_dir.display());
                    println!("tool_cache_dir={}", paths.tool_cache_dir.display());
                    println!("tool_cache_store={}", paths.tool_cache_store_dir.display());
                    println!("tool_cache_index={}", paths.tool_cache_index_jsonc.display());
                } else {
                    anyhow::bail!("could not resolve global user directory for this environment");
                }
            }
            GlobalCommand::Init => {
                let paths = ensure_global_directory_layout()?;
                println!("initialized global_dir={}", paths.root_dir.display());
            }
            GlobalCommand::ToolCache { command } => match command {
                GlobalToolCacheCommand::Status => {
                    let status = global_tool_cache_status().await?;
                    println!("tool_cache_dir={}", status.tool_cache_dir.display());
                    println!("tool_cache_store={}", status.store_dir.display());
                    println!("tool_cache_index={}", status.index_jsonc.display());
                    println!("entry_count={}", status.entry_count);
                }
                GlobalToolCacheCommand::Prune => {
                    let summary = global_tool_cache_prune_expired().await?;
                    println!(
                        "prune complete: removed_entries={}, removed_payloads={}",
                        summary.removed_entries, summary.removed_payloads
                    );
                }
                GlobalToolCacheCommand::Clear => {
                    global_tool_cache_clear()?;
                    println!("cleared global tool-cache directory");
                }
            },
        },
        Command::Builtins { command } => match command {
            BuiltinsCommand::MediaTagger(args) => run_builtin_media_tagger(args).await?,
        },
        Command::Internal { command } => match command {
            InternalCommand::MediaTagger(args) => run_builtin_media_tagger(args).await?,
        },
        Command::Cas(args) => {
            passthrough_cas(&args.args)?;
        }
        Command::Conductor(args) => {
            passthrough_conductor(&args.args)?;
        }
    }
    Ok(())
}

/// Executes builtin media-tagger command invocation.
async fn run_builtin_media_tagger(args: InternalMediaTaggerArgs) -> anyhow::Result<()> {
    mediapm::builtins::media_tagger::run_internal_media_tagger(InternalMediaTaggerOptions {
        input_path: args.input,
        output_path: args.output,
        acoustid_api_key: args.acoustid_api_key,
        acoustid_endpoint: args.acoustid_endpoint,
        musicbrainz_endpoint: args.musicbrainz_endpoint,
        strict_identification: args.strict_identification,
        recording_mbid: args.recording_mbid,
        release_mbid: args.release_mbid,
        ffmpeg_bin: args.ffmpeg_bin,
    })
    .await
}

/// Converts one optional path into UTF-8-ish string form used by config structs.
#[must_use]
fn option_path_to_string(path: Option<PathBuf>) -> Option<String> {
    path.map(|value| value.to_string_lossy().to_string())
}

/// Executes passthrough to Phase 1 CAS CLI.
fn passthrough_cas(args: &[String]) -> anyhow::Result<()> {
    let status = ProcessCommand::new("cargo")
        .arg("run")
        .arg("--package")
        .arg("mediapm-cas")
        .arg("--")
        .args(args)
        .status()?;

    if status.success() {
        Ok(())
    } else {
        anyhow::bail!("cas passthrough exited with status {status}")
    }
}

/// Executes passthrough to Phase 2 conductor CLI.
fn passthrough_conductor(args: &[String]) -> anyhow::Result<()> {
    let status = ProcessCommand::new("cargo")
        .arg("run")
        .arg("--package")
        .arg("mediapm-conductor")
        .arg("--")
        .args(args)
        .status()?;

    if status.success() {
        Ok(())
    } else {
        anyhow::bail!("conductor passthrough exited with status {status}")
    }
}
