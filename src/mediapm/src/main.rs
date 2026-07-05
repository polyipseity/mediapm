//! Binary entrypoint for the `mediapm` CLI.
//!
//! This CLI exposes:
//! - media/tool declarative state management,
//! - sync/materialization orchestration,
//! - passthrough command surfaces for the CAS and conductor CLIs.
//!
//! This binary requires the `cli` feature.

#[cfg(feature = "cli")]
use std::path::PathBuf;

#[cfg(feature = "cli")]
use clap::{Args, CommandFactory, Parser, Subcommand, ValueEnum};
#[cfg(feature = "cli")]
use clap_complete::Shell;
#[cfg(feature = "cli")]
use mediapm::MediaPmService;
#[cfg(feature = "cli")]
use mediapm::{
    AddInsertPosition, MediaHierarchyPreset, MediaMetadataValue, MediaPmGlobalPaths, MediaPmPaths,
    MediaRuntimeStorage, MediaSourceSpec, MediaStep, MediaStepTool, TransformInputValue,
    ensure_global_directory_layout, global_tool_cache_clear, global_tool_cache_prune_expired,
    global_tool_cache_status, load_runtime_dotenv, media_id_from_uri,
    resolve_effective_paths_for_root,
};
#[cfg(feature = "cli")]
use url::Url;

#[tokio::main]
async fn main() -> anyhow::Result<()> {
    #[cfg(feature = "cli")]
    {
        main_cli().await?;
        Ok(())
    }

    #[cfg(not(feature = "cli"))]
    {
        eprintln!("error: the mediapm binary requires the 'cli' feature");
        std::process::exit(1);
    }
}

#[cfg(feature = "cli")]
#[expect(
    clippy::too_many_lines,
    reason = "this item intentionally keeps end-to-end control flow together so ordering invariants remain explicit during maintenance"
)]
async fn main_cli() -> anyhow::Result<()> {
    tracing_subscriber::fmt()
        .with_env_filter(
            tracing_subscriber::EnvFilter::try_from_default_env()
                .unwrap_or_else(|_| tracing_subscriber::EnvFilter::new("warn")),
        )
        .init();
    let cli = Cli::parse();

    let rt: MediaRuntimeStorage = MediaRuntimeStorage {
        mediapm_dir: cli.mediapm_dir.as_ref().map(|p| p.to_string_lossy().to_string()),
        hierarchy_root_dir: cli
            .hierarchy_root_dir
            .as_ref()
            .map(|p| p.to_string_lossy().to_string()),
        conductor_config: cli.conductor_config.as_ref().map(|p| p.to_string_lossy().to_string()),
        conductor_generated_config: cli
            .conductor_generated_config
            .as_ref()
            .map(|p| p.to_string_lossy().to_string()),
        conductor_state_config: cli
            .conductor_state_config
            .as_ref()
            .map(|p| p.to_string_lossy().to_string()),
        env_file: cli.env_file.as_ref().map(|p| p.to_string_lossy().to_string()),
        env_generated_file: cli
            .env_generated_file
            .as_ref()
            .map(|p| p.to_string_lossy().to_string()),
        media_state_config: cli
            .media_state_config
            .as_ref()
            .map(|p| p.to_string_lossy().to_string()),
        mediapm_schema_dir: cli
            .mediapm_schema_dir
            .as_ref()
            .map(|p| Some(p.to_string_lossy().to_string())),
        retry_impure: cli.retry_impure,
        ..MediaRuntimeStorage::default()
    };
    let _passthrough_rt = rt.clone();

    match cli.command {
        Command::Sync(args) => {
            let root = &cli.root;
            let paths = resolve_effective_paths_for_root(root, &rt);
            load_runtime_dotenv(&paths.env_file, &paths.env_generated_file);
            let mut service =
                MediaPmService::new_fs_at_with_runtime_storage_overrides(root, rt).await?;
            let check_tag_updates = args.tag_update_policy.resolve(true);
            let verify_materialization = args.verify_materialization.resolve(true);
            let summary = service
                .sync_library_with_tag_update_checks(verify_materialization, check_tag_updates)
                .await?;
            mediapm::output::print_sync_summary(&summary);
            Ok(())
        }
        Command::Tool { command } => match command {
            ToolCommand::Run { tool, args } => {
                let effective_paths = MediaPmPaths::from_root(&cli.root);
                let mut conductor_args = vec!["tool".to_string(), "run".to_string()];
                conductor_args.push("--tool".to_string());
                conductor_args.push(tool);
                conductor_args.extend(args);
                passthrough_conductor(&conductor_args, &effective_paths).await
            }
            ToolCommand::Add { name } => {
                let root = &cli.root;
                let paths = resolve_effective_paths_for_root(root, &rt);
                load_runtime_dotenv(&paths.env_file, &paths.env_generated_file);
                let mut service =
                    MediaPmService::new_fs_at_with_runtime_storage_overrides(root, rt).await?;
                service.add_tool_requirement(&name, None, None)?;
                println!(
                    "added tool requirement '{name}' (tag = latest); run 'tool sync' to download"
                );
                Ok(())
            }
            ToolCommand::Sync(args) => {
                let root = &cli.root;
                let paths = resolve_effective_paths_for_root(root, &rt);
                load_runtime_dotenv(&paths.env_file, &paths.env_generated_file);
                let mut service =
                    MediaPmService::new_fs_at_with_runtime_storage_overrides(root, rt).await?;
                let check_tag_updates = args.tag_update_policy.resolve(true);
                let summary = service.sync_tools_with_tag_update_checks(check_tag_updates).await?;
                println!(
                    "tool sync complete: added={}, updated={}, pruned={}, removed={}",
                    summary.added_tools,
                    summary.updated_tools,
                    summary.pruned_tools,
                    summary.removed_tools,
                );
                for warning in &summary.warnings {
                    eprintln!("warning: {warning}");
                }
                Ok(())
            }
            ToolCommand::List => {
                let effective_paths = MediaPmPaths::from_root(&cli.root);
                passthrough_conductor(&["tool".to_string(), "list".to_string()], &effective_paths)
                    .await
            }
            ToolCommand::Remove { name } => {
                let root = &cli.root;
                let paths = resolve_effective_paths_for_root(root, &rt);
                load_runtime_dotenv(&paths.env_file, &paths.env_generated_file);
                let mut service =
                    MediaPmService::new_fs_at_with_runtime_storage_overrides(root, rt).await?;
                service.remove_tool_requirement(&name)?;
                println!(
                    "removed tool requirement '{name}'; run 'tool sync' to reconcile runtime state"
                );
                Ok(())
            }
            ToolCommand::Prune { id, metadata } => {
                let effective_paths = MediaPmPaths::from_root(&cli.root);
                let mut conductor_args = vec!["tool".to_string(), "prune".to_string(), id];
                if metadata {
                    conductor_args.push("--metadata".to_string());
                }
                passthrough_conductor(&conductor_args, &effective_paths).await
            }
            ToolCommand::RefreshRuntime => {
                let root = &cli.root;
                let paths = resolve_effective_paths_for_root(root, &rt);
                load_runtime_dotenv(&paths.env_file, &paths.env_generated_file);
                let service =
                    MediaPmService::new_fs_at_with_runtime_storage_overrides(root, rt).await?;
                service.refresh_runtime_configuration()?;
                println!(
                    "refreshed mediapm-managed conductor runtime configuration and dotenv files"
                );
                Ok(())
            }
        },
        Command::Media { command } => {
            let root = &cli.root;
            let paths = resolve_effective_paths_for_root(root, &rt);
            load_runtime_dotenv(&paths.env_file, &paths.env_generated_file);
            let mut service =
                MediaPmService::new_fs_at_with_runtime_storage_overrides(root, rt).await?;
            match command {
                MediaCommand::Add(args) => {
                    let media_id = match args.preset {
                        MediaAddPreset::YtDlp => {
                            let uri = Url::parse(&args.source).map_err(|e| {
                                anyhow::anyhow!("invalid URL '{}': {}", args.source, e)
                            })?;
                            let media_id = media_id_from_uri(&uri);
                            if args.overwrite {
                                let _ = service.remove_media_source(&media_id);
                            }
                            let steps = default_yt_dlp_steps(
                                args.recording_mbid.as_deref(),
                                args.release_mbid.as_deref(),
                            );
                            let source_spec = MediaSourceSpec {
                                id: None,
                                title: args.title.clone().unwrap_or_default(),
                                description: args.description.clone().unwrap_or_default(),
                                artist: args.artist.clone().unwrap_or_default(),
                                workflow_id: None,
                                metadata: args.album.as_deref().map_or_else(
                                    std::collections::BTreeMap::new,
                                    |album| {
                                        let mut metadata = std::collections::BTreeMap::new();
                                        metadata.insert(
                                            "album".to_string(),
                                            MediaMetadataValue::Literal(album.to_string()),
                                        );
                                        metadata
                                    },
                                ),
                                variant_hashes: std::collections::BTreeMap::new(),
                                steps,
                            };
                            service.add_media_source_with_position(
                                &source_spec,
                                media_id.clone(),
                                &uri,
                                args.title.as_deref(),
                                args.description.as_deref(),
                                args.insert_position.into(),
                                args.overwrite,
                            )?;
                            media_id
                        }
                        MediaAddPreset::Local => {
                            let path = PathBuf::from(&args.source);
                            service.add_local_source_with_position(
                                &path,
                                args.ffprobe_command.as_deref().unwrap_or("ffprobe"),
                                None,
                                args.insert_position.into(),
                                args.overwrite,
                            )?
                        }
                    };
                    println!("registered media source id={media_id}");
                    eprintln!(
                        "hint: run 'mediapm sync' to apply workflow/hierarchy changes (and 'mediapm tool sync' first if tools are out of date)"
                    );
                    Ok(())
                }
                MediaCommand::Remove { media_id } => {
                    service.remove_media_source(&media_id)?;
                    println!("removed media source id={media_id}");
                    eprintln!("hint: run 'mediapm sync' to apply workflow/hierarchy changes");
                    Ok(())
                }
                MediaCommand::Invalidate(args) => {
                    let invalidate_calls =
                        if args.no_invalidate_calls { false } else { args.invalidate_calls };
                    let regenerate = args.regenerate;
                    let summary = service.invalidate_media_step_tool_calls(
                        &args.media_id,
                        args.step_index,
                        invalidate_calls,
                        regenerate,
                    )?;
                    println!(
                        "invalidated media id={} step_index={}: workflow_id={}, removed_instances={}, regenerated_step={}",
                        args.media_id,
                        args.step_index,
                        summary.workflow_id,
                        summary.removed_instances.len(),
                        summary.regenerated_step,
                    );
                    for warning in &summary.warnings {
                        eprintln!("warning: {warning}");
                    }
                    eprintln!(
                        "hint: run 'mediapm sync' to apply invalidation effects to materialized outputs"
                    );
                    Ok(())
                }
            }
        }
        Command::Hierarchy { command } => {
            let root = &cli.root;
            let paths = resolve_effective_paths_for_root(root, &rt);
            load_runtime_dotenv(&paths.env_file, &paths.env_generated_file);
            let mut service =
                MediaPmService::new_fs_at_with_runtime_storage_overrides(root, rt).await?;
            match command {
                HierarchyCommand::Add(args) => {
                    if args.overwrite {
                        let _ = service.remove_media_hierarchy_preset(&format!(
                            "root/{}",
                            args.root_folder.trim_end_matches('/')
                        ));
                    }
                    service.add_media_hierarchy_preset_with_position(
                        args.preset.into(),
                        args.insert_position.into(),
                    )?;
                    println!(
                        "registered hierarchy preset={}",
                        match args.preset {
                            HierarchyPresetArg::Local => "local",
                            HierarchyPresetArg::YtDlp => "yt-dlp",
                        },
                    );
                    eprintln!("hint: run 'mediapm sync' to apply workflow/hierarchy changes");
                    Ok(())
                }
                // `--preset` is accepted for forward-compatibility / CLI
                // discoverability but intentionally unused in the handler:
                // remove-by-root-folder is simpler and covers all preset
                // types since they all share the same node layout.
                HierarchyCommand::Remove { preset: _, root_folder, media_id } => {
                    let _ = service.remove_media_hierarchy_preset(&format!(
                        "root/{}",
                        root_folder.trim_end_matches('/')
                    ));
                    let removed = service.remove_media_hierarchy_preset_by_media_id(&media_id)?;
                    println!("removed hierarchy nodes for media id={media_id}: count={removed}");
                    eprintln!("hint: run 'mediapm sync' to apply workflow/hierarchy changes");
                    Ok(())
                }
            }
        }
        Command::Global { command } => match command {
            GlobalCommand::Path => {
                let paths = MediaPmGlobalPaths::resolve_default().ok_or_else(|| {
                    anyhow::anyhow!("could not resolve global user directory for this environment")
                })?;
                println!("global_dir={}", paths.root_dir.display());
                println!("tool_cache_dir={}", paths.tool_cache_dir.display());
                println!("tool_cache_store={}", paths.tool_cache_store_dir.display());
                println!("tool_cache_index={}", paths.tool_cache_index.display());
                Ok(())
            }
            GlobalCommand::Init => {
                ensure_global_directory_layout()?;
                let paths = MediaPmGlobalPaths::resolve_default().unwrap();
                println!("initialized global_dir={}", paths.root_dir.display());
                Ok(())
            }
            GlobalCommand::ToolCache { command } => match command {
                GlobalToolCacheCommand::Status => {
                    let status = global_tool_cache_status()?;
                    println!("tool_cache_dir={}", status.tool_cache_dir.display());
                    println!("store_dir={}", status.store_dir.display());
                    println!("index={}", status.index.display());
                    println!("entry_count={}", status.entry_count);
                    Ok(())
                }
                GlobalToolCacheCommand::Prune => {
                    let summary = global_tool_cache_prune_expired()?;
                    println!(
                        "prune complete: removed_entries={}, removed_payloads={}",
                        summary.removed_entries, summary.removed_payloads
                    );
                    Ok(())
                }
                GlobalToolCacheCommand::Clear => {
                    global_tool_cache_clear()?;
                    println!("cleared global tool-cache directory");
                    Ok(())
                }
            },
        },
        Command::Builtin { command } => match *command {
            #[cfg(feature = "media-tagger")]
            BuiltinCommand::MediaTagger(args) => run_builtin_media_tagger(args).await,
        },
        Command::Cas(args) => {
            let effective_store = MediaPmPaths::from_root(&cli.root).runtime_root.join("store");
            passthrough_cas(&args.args, &effective_store).await
        }
        Command::Conductor(args) => {
            let effective_paths = MediaPmPaths::from_root(&cli.root);
            passthrough_conductor(&args.args, &effective_paths).await
        }
        Command::Completions { shell } => {
            clap_complete::generate(shell, &mut Cli::command(), "mediapm", &mut std::io::stdout());
            Ok(())
        }
    }
}

// ---------------------------------------------------------------------------
// CLI argument types
// ---------------------------------------------------------------------------

/// `mediapm` orchestration CLI.
#[derive(Debug, Parser)]
#[command(author, version, about = "mediapm orchestration CLI")]
struct Cli {
    /// Workspace root that hosts `mediapm.ncl` and `.mediapm/` runtime state.
    #[arg(long, default_value = ".", env = "MEDIAPM_ROOT")]
    root: PathBuf,
    /// Overrides `runtime.mediapm_dir` for this command invocation.
    #[arg(long, env = "MEDIAPM_DIR")]
    mediapm_dir: Option<PathBuf>,
    /// Override for `runtime.hierarchy_root_dir`.
    ///
    /// Intentionally exposed even though PLAN.md's global CLI-flag table
    /// omits it — consistency with the other path-override flags makes
    /// automation scripts simpler (every path can be set via `--<name>` /
    /// `MEDIAPM_<NAME>`).
    #[arg(long, env = "MEDIAPM_HIERARCHY_ROOT_DIR")]
    hierarchy_root_dir: Option<PathBuf>,
    /// Override for `runtime.conductor_config`.
    #[arg(long, env = "MEDIAPM_CONDUCTOR_CONFIG")]
    conductor_config: Option<PathBuf>,
    /// Override for `runtime.conductor_generated_config`.
    #[arg(long, env = "MEDIAPM_CONDUCTOR_GENERATED_CONFIG")]
    conductor_generated_config: Option<PathBuf>,
    /// Override for `runtime.conductor_state_config`.
    #[arg(long, env = "MEDIAPM_CONDUCTOR_STATE_CONFIG")]
    conductor_state_config: Option<PathBuf>,
    /// Override for `runtime.env_file`.
    #[arg(long, env = "MEDIAPM_ENV_FILE")]
    env_file: Option<PathBuf>,
    /// Override for `runtime.env_generated_file`.
    #[arg(long, env = "MEDIAPM_ENV_GENERATED_FILE")]
    env_generated_file: Option<PathBuf>,
    /// Override for `runtime.mediapm_schema_dir`.
    #[arg(long, env = "MEDIAPM_MEDIAPM_SCHEMA_DIR")]
    mediapm_schema_dir: Option<PathBuf>,
    /// Override for `runtime.media_state_config`.
    #[arg(long, env = "MEDIAPM_MEDIA_STATE_CONFIG")]
    media_state_config: Option<PathBuf>,
    /// Enables `CorruptObject` retry for impure workflow steps.
    #[arg(long, env = "MEDIAPM_RETRY_IMPURE")]
    retry_impure: bool,
    /// Top-level command selector.
    #[command(subcommand)]
    command: Command,
}

/// Top-level `mediapm` commands.
#[derive(Debug, Subcommand)]
enum Command {
    /// Reconciles desired state and materializes the media library.
    ///
    /// Default policy checks remote updates for tag-only selectors unless
    /// `--no-check-tag-updates` is provided.
    Sync(SyncArgs),
    /// Tool lifecycle commands.
    #[command(name = "tool", visible_alias = "tools")]
    Tool {
        /// Tool subcommand selector.
        #[command(subcommand)]
        command: ToolCommand,
    },
    /// Media-source registry commands.
    Media {
        /// Media subcommand selector.
        #[command(subcommand)]
        command: MediaCommand,
    },
    /// Hierarchy registry commands.
    Hierarchy {
        /// Hierarchy subcommand selector.
        #[command(subcommand)]
        command: HierarchyCommand,
    },
    /// User-scoped global `mediapm` directory management commands.
    Global {
        /// Global subcommand selector.
        #[command(subcommand)]
        command: GlobalCommand,
    },
    /// Builtin command implementations exposed by the `mediapm` executable.
    #[command(name = "builtin", visible_alias = "builtins")]
    Builtin {
        /// Builtin subcommand selector.
        #[command(subcommand)]
        command: Box<BuiltinCommand>,
    },
    /// Passthrough to the `mediapm-cas` CLI.
    Cas(PassthroughArgs),
    /// Passthrough to the conductor CLI.
    Conductor(PassthroughArgs),
    /// Generates shell completion scripts for the `mediapm` CLI.
    Completions {
        /// Target shell for completion script generation.
        shell: Shell,
    },
}

/// Tool lifecycle commands.
#[derive(Debug, Subcommand)]
enum ToolCommand {
    /// Adds one tool requirement entry to `mediapm.ncl` by logical name.
    ///
    /// The tool must appear in the built-in downloader catalog. If a
    /// requirement for this name already exists, the command is a no-op.
    /// After adding, run `tool sync` to download and register the tool.
    Add {
        /// Logical tool name (e.g. `yt-dlp`, `ffmpeg`, `deno`, `rsgain`,
        /// `media-tagger`, `sd`).
        name: String,
    },
    /// Reconciles desired tool requirements only (no workflow/materialization
    /// run).
    ///
    /// Default policy checks remote updates for tag-only selectors unless
    /// `--no-check-tag-updates` is provided.
    Sync(SyncArgs),
    /// Lists registered tools and binary status.
    List,
    /// Removes one tool requirement entry from `mediapm.ncl`.
    ///
    /// This updates desired tool state only.
    Remove {
        /// Logical tool name.
        name: String,
    },
    /// Removes one installed tool binary while keeping metadata.
    ///
    /// Pass `--metadata` to also remove tool metadata from the machine
    /// document and the tool registry entirely.
    Prune {
        /// Immutable tool id.
        id: String,
        /// Also remove tool metadata from the machine document and registry.
        #[arg(long)]
        metadata: bool,
    },
    /// Runs one managed tool binary directly.
    Run {
        /// Immutable tool id or logical tool name.
        tool: String,
        /// Trailing arguments passed to the managed tool executable.
        #[arg(trailing_var_arg = true, allow_hyphen_values = true)]
        args: Vec<String>,
    },
    /// Refreshes machine-managed runtime path defaults and dotenv scaffolding.
    RefreshRuntime,
}

/// Media-source registry commands.
#[derive(Debug, Subcommand)]
enum MediaCommand {
    /// Adds one media source registered in `mediapm.ncl`.
    Add(MediaAddArgs),
    /// Removes one media source from `mediapm.ncl` by id.
    Remove {
        /// Media source id to remove.
        media_id: String,
    },
    /// Invalidates cached tool output for a specific media step.
    ///
    /// After invalidation, run `mediapm sync` to regenerate outputs.
    Invalidate(MediaInvalidateArgs),
}

/// Arguments for `media add <preset> <source>`.
#[derive(Debug, Args)]
struct MediaAddArgs {
    /// Preset defining which toolchain to use.
    #[arg(value_enum)]
    preset: MediaAddPreset,
    /// Media source URL or local file path.
    source: String,
    /// Optional human-readable title for the media source.
    #[arg(long)]
    title: Option<String>,
    /// Optional artist name override.
    #[arg(long)]
    artist: Option<String>,
    /// Optional description for the media source.
    #[arg(long)]
    description: Option<String>,
    /// Optional recording MBID for music-related sources.
    #[arg(long)]
    recording_mbid: Option<String>,
    /// Optional release MBID for music-related sources.
    #[arg(long)]
    release_mbid: Option<String>,
    /// Overwrite existing media source if media id already exists.
    #[arg(long)]
    overwrite: bool,
    /// Insert position for the new entry.
    #[arg(long, default_value = "sorted")]
    insert_position: AddInsertPositionArg,
    /// Optional album name override.
    #[arg(long)]
    album: Option<String>,
    /// ffprobe command path (local preset only).
    #[arg(long)]
    ffprobe_command: Option<String>,
}

/// Preset for media add operations.
#[derive(Debug, Clone, Copy, ValueEnum)]
enum MediaAddPreset {
    /// Use yt-dlp to download and process media.
    YtDlp,
    /// Add a local file as a media source.
    Local,
}

/// Insertion position for media/hierarchy add operations.
#[derive(Debug, Clone, Copy, Default, ValueEnum)]
enum AddInsertPositionArg {
    /// Insert in sorted (alphabetical) position.
    #[default]
    Sorted,
    /// Insert at the beginning of the list.
    Beginning,
    /// Insert at the end of the list.
    End,
}

impl From<AddInsertPositionArg> for AddInsertPosition {
    fn from(value: AddInsertPositionArg) -> Self {
        match value {
            AddInsertPositionArg::Sorted => AddInsertPosition::Sorted,
            AddInsertPositionArg::Beginning => AddInsertPosition::Beginning,
            AddInsertPositionArg::End => AddInsertPosition::End,
        }
    }
}

/// Arguments for `media invalidate <media-id> <step-index>`.
#[derive(Debug, Args)]
#[allow(clippy::struct_excessive_bools)]
struct MediaInvalidateArgs {
    /// Media source id whose step cache should be invalidated.
    media_id: String,
    /// Zero-based step index to invalidate.
    step_index: usize,
    /// Invalidate tool calls (default: yes).
    #[arg(
        long,
        default_value_t = true,
        action = clap::ArgAction::Set,
        conflicts_with = "no_invalidate_calls"
    )]
    invalidate_calls: bool,
    /// Skip invalidation of tool calls.
    #[arg(long, conflicts_with = "invalidate_calls")]
    no_invalidate_calls: bool,
    /// Regenerate after invalidation (default: no).
    #[arg(
        long,
        default_value_t = false,
        action = clap::ArgAction::Set,
        conflicts_with = "no_regenerate"
    )]
    regenerate: bool,
    /// Skip regeneration after invalidation.
    #[arg(long, conflicts_with = "regenerate")]
    no_regenerate: bool,
}

/// Hierarchy registry commands.
#[derive(Debug, Subcommand)]
enum HierarchyCommand {
    /// Adds one predefined hierarchy preset entry.
    Add(HierarchyAddArgs),
    /// Removes one hierarchy node by media id.
    Remove {
        /// Hierarchy preset to remove from.
        #[arg(long, value_enum)]
        preset: Option<HierarchyPresetArg>,
        /// Root folder for hierarchy nodes.
        #[arg(long, default_value = "media/")]
        root_folder: String,
        /// Media id of the hierarchy node to remove.
        media_id: String,
    },
}

/// Arguments for `hierarchy add --preset <preset>`.
#[derive(Debug, Args)]
struct HierarchyAddArgs {
    /// Predefined hierarchy preset.
    #[arg(value_enum)]
    preset: HierarchyPresetArg,
    /// Root folder name for hierarchy nodes.
    #[arg(long, default_value = "media/")]
    root_folder: String,
    /// Overwrite existing hierarchy node.
    #[arg(long)]
    overwrite: bool,
    /// Insert position for the new entry.
    #[arg(long, default_value = "sorted")]
    insert_position: AddInsertPositionArg,
    /// Optional media id to associate with the hierarchy node.
    media_id: Option<String>,
}

/// Hierarchy preset selector with user-friendly names.
#[derive(Debug, Clone, Copy, ValueEnum)]
enum HierarchyPresetArg {
    /// Local-file library.
    Local,
    /// yt-dlp channel-based library.
    #[value(name = "yt-dlp")]
    YtDlp,
}

impl From<HierarchyPresetArg> for MediaHierarchyPreset {
    fn from(value: HierarchyPresetArg) -> Self {
        match value {
            HierarchyPresetArg::Local => MediaHierarchyPreset::Local,
            HierarchyPresetArg::YtDlp => MediaHierarchyPreset::YtDlpChannel,
        }
    }
}

/// Global directory management commands.
#[derive(Debug, Subcommand)]
enum GlobalCommand {
    /// Print global directory paths.
    Path,
    /// Creates the global directory layout if absent.
    Init,
    /// Tool-cache management commands.
    ToolCache {
        /// Tool-cache subcommand selector.
        #[command(subcommand)]
        command: GlobalToolCacheCommand,
    },
}

/// Global tool-cache management commands.
#[derive(Debug, Subcommand)]
enum GlobalToolCacheCommand {
    /// Print tool-cache status information.
    Status,
    /// Prune expired entries from the global tool cache.
    Prune,
    /// Clear the global tool cache entirely.
    Clear,
}

/// Builtin command implementations exposed by the `mediapm` executable.
#[derive(Debug, Subcommand)]
enum BuiltinCommand {
    /// Run internal media-tagger (Picard-based tagging pipeline).
    #[cfg(feature = "media-tagger")]
    MediaTagger(InternalMediaTaggerArgs),
}

/// Arguments for `builtin media-tagger`.
#[cfg(feature = "media-tagger")]
#[derive(Debug, Args)]
#[expect(clippy::struct_excessive_bools, reason = "media-tagger has many option toggles")]
struct InternalMediaTaggerArgs {
    /// Input media file path.
    #[arg(short, long)]
    input: String,
    /// Output file path.
    #[arg(short, long)]
    output: String,
    /// `AcoustID` API key.
    #[arg(long)]
    acoustid_api_key: Option<String>,
    /// `AcoustID` endpoint URL.
    #[arg(long, default_value = mediapm::builtins::media_tagger::DEFAULT_ACOUSTID_ENDPOINT)]
    acoustid_endpoint: String,
    /// `MusicBrainz` endpoint URL.
    #[arg(long, default_value = mediapm::builtins::media_tagger::DEFAULT_MUSICBRAINZ_ENDPOINT)]
    musicbrainz_endpoint: String,
    /// Cache directory path.
    #[arg(long)]
    cache_dir: Option<String>,
    /// Cache expiry in seconds.
    #[arg(long, default_value_t = mediapm::builtins::media_tagger::DEFAULT_CACHE_EXPIRY_SECONDS as u64)]
    cache_expiry_seconds: u64,
    /// Enable strict identification.
    #[arg(long, default_value_t = true)]
    strict_identification: bool,
    /// Write all tags.
    #[arg(long, default_value_t = true)]
    write_all_tags: bool,
    /// Write all images.
    #[arg(long, default_value_t = true)]
    write_all_images: bool,
    /// Save images to tags.
    #[arg(long, default_value_t = mediapm::builtins::media_tagger::DEFAULT_SAVE_IMAGES_TO_TAGS)]
    save_images_to_tags: bool,
    /// Embed only one front image.
    #[arg(long, default_value_t = mediapm::builtins::media_tagger::DEFAULT_EMBED_ONLY_ONE_FRONT_IMAGE)]
    embed_only_one_front_image: bool,
    /// Cover art providers.
    #[arg(long, default_value_t = mediapm::builtins::media_tagger::DEFAULT_CA_PROVIDERS.to_string())]
    ca_providers: String,
    /// CAA image types filter.
    #[arg(long, default_value_t = mediapm::builtins::media_tagger::DEFAULT_CAA_IMAGE_TYPES.to_string())]
    caa_image_types: String,
    /// CAA image size.
    #[arg(long, default_value_t = mediapm::builtins::media_tagger::DEFAULT_CAA_IMAGE_SIZE.to_string())]
    caa_image_size: String,
    /// Restrict CAA entries to approved-only.
    #[arg(long, default_value_t = mediapm::builtins::media_tagger::DEFAULT_CAA_APPROVED_ONLY)]
    caa_approved_only: bool,
    /// Preserve existing embedded images.
    #[arg(long, default_value_t = mediapm::builtins::media_tagger::DEFAULT_PRESERVE_IMAGES)]
    preserve_images: bool,
    /// Clear existing textual tags.
    #[arg(long, default_value_t = mediapm::builtins::media_tagger::DEFAULT_CLEAR_EXISTING_TAGS)]
    clear_existing_tags: bool,
    /// Enable tag writing.
    #[arg(long, default_value_t = mediapm::builtins::media_tagger::DEFAULT_ENABLE_TAG_SAVING)]
    enable_tag_saving: bool,
    /// Release relationship processing.
    #[arg(long, default_value_t = mediapm::builtins::media_tagger::DEFAULT_RELEASE_ARS)]
    release_ars: bool,
    /// Cover art slot count.
    #[arg(long, default_value_t = mediapm::builtins::media_tagger::DEFAULT_COVER_ART_SLOT_COUNT)]
    cover_art_slot_count: usize,
    /// Recording MBID override.
    #[arg(long)]
    recording_mbid: Option<String>,
    /// Release MBID override.
    #[arg(long)]
    release_mbid: Option<String>,
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

/// Optional verify-materialization override flags for sync commands.
#[derive(Debug, Args, Clone, Copy, Default)]
struct VerifyMaterializationArgs {
    /// Enables materialization verification.
    #[arg(long, conflicts_with = "no_verify_materialization")]
    verify_materialization: bool,
    /// Disables materialization verification.
    #[arg(long)]
    no_verify_materialization: bool,
}

impl VerifyMaterializationArgs {
    /// Resolves effective verify-materialization policy using command-specific
    /// default.
    fn resolve(self, default_value: bool) -> bool {
        if self.verify_materialization {
            true
        } else if self.no_verify_materialization {
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
    /// Optional override for materialization verification.
    #[command(flatten)]
    verify_materialization: VerifyMaterializationArgs,
}

// ---------------------------------------------------------------------------
// Helpers
// ---------------------------------------------------------------------------

/// Builds default yt-dlp processing steps with optional MBID overrides.
#[must_use]
fn default_yt_dlp_steps(
    recording_mbid: Option<&str>,
    release_mbid: Option<&str>,
) -> Vec<MediaStep> {
    use std::collections::BTreeMap;

    vec![
        MediaStep {
            tool: MediaStepTool::YtDlp,
            input_variants: vec![],
            output_variants: BTreeMap::from([(
                "media".to_string(),
                serde_json::json!({ "kind": "primary" }),
            )]),
            options: BTreeMap::from([(
                "format".to_string(),
                TransformInputValue::String("best".to_string()),
            )]),
        },
        MediaStep {
            tool: MediaStepTool::Ffmpeg,
            input_variants: vec!["media".to_string()],
            output_variants: BTreeMap::from([(
                "media".to_string(),
                serde_json::json!({ "kind": "primary" }),
            )]),
            options: BTreeMap::new(),
        },
        MediaStep {
            tool: MediaStepTool::MediaTagger,
            input_variants: vec!["media".to_string()],
            output_variants: BTreeMap::from([(
                "media".to_string(),
                serde_json::json!({ "kind": "primary" }),
            )]),
            options: {
                let mut opts = BTreeMap::new();
                if let Some(mbid) = recording_mbid.filter(|s| !s.is_empty()) {
                    opts.insert(
                        "recording_mbid".to_string(),
                        TransformInputValue::String(mbid.to_string()),
                    );
                }
                if let Some(mbid) = release_mbid.filter(|s| !s.is_empty()) {
                    opts.insert(
                        "release_mbid".to_string(),
                        TransformInputValue::String(mbid.to_string()),
                    );
                }
                opts
            },
        },
        MediaStep {
            tool: MediaStepTool::Rsgain,
            input_variants: vec!["media".to_string()],
            output_variants: BTreeMap::from([(
                "media".to_string(),
                serde_json::json!({ "kind": "primary" }),
            )]),
            options: BTreeMap::new(),
        },
    ]
}

/// Executes builtin media-tagger command invocation via `run_internal_media_tagger`.
#[cfg(feature = "media-tagger")]
#[allow(clippy::cast_possible_wrap)]
async fn run_builtin_media_tagger(args: InternalMediaTaggerArgs) -> anyhow::Result<()> {
    mediapm::builtins::media_tagger::run_internal_media_tagger(
        mediapm::builtins::media_tagger::InternalMediaTaggerOptions {
            input_path: Some(std::path::PathBuf::from(args.input)),
            output_path: std::path::PathBuf::from(args.output),
            acoustid_api_key: args.acoustid_api_key,
            acoustid_endpoint: args.acoustid_endpoint,
            musicbrainz_endpoint: args.musicbrainz_endpoint,
            cache_dir: args.cache_dir.map(std::path::PathBuf::from),
            cache_expiry_seconds: args.cache_expiry_seconds as i64,
            strict_identification: args.strict_identification,
            write_all_tags: args.write_all_tags,
            write_all_images: args.write_all_images,
            save_images_to_tags: args.save_images_to_tags,
            embed_only_one_front_image: args.embed_only_one_front_image,
            ca_providers: args.ca_providers,
            caa_image_types: args.caa_image_types,
            caa_image_size: args.caa_image_size,
            caa_approved_only: args.caa_approved_only,
            preserve_images: args.preserve_images,
            clear_existing_tags: args.clear_existing_tags,
            enable_tag_saving: args.enable_tag_saving,
            release_ars: args.release_ars,
            cover_art_slot_count: args.cover_art_slot_count,
            recording_mbid: args.recording_mbid,
            release_mbid: args.release_mbid,
        },
    )
    .await
}

/// Executes CAS CLI passthrough in-process.
async fn passthrough_cas(args: &[String], default_root: &std::path::Path) -> anyhow::Result<()> {
    let injected = inject_cas_passthrough_defaults(args, default_root);
    mediapm_cas::cli::run_from_passthrough_args(&injected).await
}

/// Executes conductor CLI passthrough in-process.
async fn passthrough_conductor(
    args: &[String],
    effective_paths: &MediaPmPaths,
) -> anyhow::Result<()> {
    let injected = inject_conductor_passthrough_defaults(args, effective_paths);
    let mut passthrough = vec!["mediapm-conductor"];
    passthrough.extend(injected.iter().map(String::as_str));
    mediapm_conductor::cli::run_from_args(&passthrough).await.map_err(Into::into)
}

/// Injects default CAS root when the caller did not provide one explicitly.
#[must_use]
fn inject_cas_passthrough_defaults(args: &[String], default_root: &std::path::Path) -> Vec<String> {
    if passthrough_requests_help_or_version(args) {
        return args.to_vec();
    }

    let mut injected = Vec::new();
    if !passthrough_option_present(args, &["--root"]) {
        injected.push("--root".to_string());
        injected.push(default_root.to_string_lossy().to_string());
    }
    injected.extend(args.iter().cloned());
    injected
}

/// Injects resolved mediapm-owned conductor runtime defaults into passthrough
/// argv.
#[must_use]
fn inject_conductor_passthrough_defaults(
    args: &[String],
    effective_paths: &MediaPmPaths,
) -> Vec<String> {
    let mut injected = Vec::new();
    append_passthrough_option_if_missing(
        &mut injected,
        args,
        "--conductor-dir",
        effective_paths.runtime_root.to_string_lossy().to_string(),
    );
    append_passthrough_option_if_missing(
        &mut injected,
        args,
        "--cas-store-dir",
        effective_paths.runtime_root.join("store").to_string_lossy().to_string(),
    );
    append_passthrough_option_if_missing(
        &mut injected,
        args,
        "--conductor-schema-dir",
        effective_paths.conductor_schema_dir.to_string_lossy().to_string(),
    );
    append_passthrough_option_if_missing(
        &mut injected,
        args,
        "--conductor-tools-dir",
        effective_paths.tools_dir.to_string_lossy().to_string(),
    );
    injected.extend(args.iter().cloned());
    injected
}

/// Appends one key/value pair when the option is not already present.
fn append_passthrough_option_if_missing(
    injected: &mut Vec<String>,
    args: &[String],
    option_name: &str,
    value: String,
) {
    if passthrough_option_present(args, &[option_name]) {
        return;
    }
    injected.push(option_name.to_string());
    injected.push(value);
}

/// Returns true when any long option name is already present in passthrough
/// argv.
#[must_use]
fn passthrough_option_present(args: &[String], option_names: &[&str]) -> bool {
    args.iter().any(|arg| {
        option_names.iter().any(|option_name| {
            arg == option_name
                || arg.strip_prefix(option_name).is_some_and(|suffix| suffix.starts_with('='))
        })
    })
}

/// Returns true when passthrough argv explicitly requests help/version text.
#[must_use]
fn passthrough_requests_help_or_version(args: &[String]) -> bool {
    args.iter().any(|arg| {
        matches!(arg.as_str(), "-h" | "--help" | "-V" | "--version")
            || arg.split_once('=').is_some_and(|(flag, _)| matches!(flag, "--help" | "--version"))
    })
}

// ---------------------------------------------------------------------------
// Tests
// ---------------------------------------------------------------------------

#[cfg(test)]
mod tests {
    use clap::Parser;
    use mediapm::MediaPmPaths;
    use std::path::PathBuf;
    use tempfile::tempdir;

    use super::{Cli, inject_cas_passthrough_defaults, inject_conductor_passthrough_defaults};

    /// Protects no-backcompat policy by rejecting removed hidden internal route.
    #[test]
    fn removed_internal_command_route_is_not_parsed() {
        let parsed = Cli::try_parse_from([
            "mediapm",
            "internal",
            "media-tagger",
            "--output",
            "output.ffmetadata",
        ]);
        assert!(parsed.is_err(), "internal command route must stay removed");
    }

    /// Protects preset-driven media-add CLI route.
    #[test]
    fn media_add_route_with_preset_is_parsed() {
        let parsed =
            Cli::try_parse_from(["mediapm", "media", "add", "yt-dlp", "https://example.com/media"]);
        assert!(parsed.is_ok(), "media add route with preset must parse");
    }

    /// Protects media-remove CLI route.
    #[test]
    fn media_remove_route_is_parsed() {
        let parsed = Cli::try_parse_from(["mediapm", "media", "remove", "media-123"]);
        assert!(parsed.is_ok(), "media remove route must parse");
    }

    /// Protects media-step invalidation CLI route with default mode.
    #[test]
    fn media_invalidate_route_with_default_mode_is_parsed() {
        let parsed = Cli::try_parse_from(["mediapm", "media", "invalidate", "media-123", "2"]);
        assert!(parsed.is_ok(), "media invalidate route with default mode must parse");
    }

    /// Protects hierarchy-add local preset route with explicit root folder.
    #[test]
    fn hierarchy_add_local_route_is_parsed() {
        let parsed = Cli::try_parse_from(["mediapm", "hierarchy", "add", "local"]);
        assert!(parsed.is_ok(), "hierarchy add local route must parse");
    }

    /// Protects hierarchy-add yt-dlp preset route.
    #[test]
    fn hierarchy_add_yt_dlp_route_is_parsed() {
        let parsed = Cli::try_parse_from(["mediapm", "hierarchy", "add", "yt-dlp"]);
        assert!(parsed.is_ok(), "hierarchy add yt-dlp route must parse");
    }

    /// Protects hierarchy-remove route.
    #[test]
    fn hierarchy_remove_route_is_parsed() {
        let parsed = Cli::try_parse_from(["mediapm", "hierarchy", "remove", "some-node-id"]);
        assert!(parsed.is_ok(), "hierarchy remove route must parse");
    }

    /// Protects media-add insertion-position CLI parsing.
    #[test]
    fn media_add_accepts_insert_position() {
        let parsed = Cli::try_parse_from([
            "mediapm",
            "media",
            "add",
            "yt-dlp",
            "--insert-position",
            "end",
            "https://example.com/media",
        ]);
        assert!(parsed.is_ok(), "media add should parse insert-position values");
    }

    /// Protects tool-remove CLI route.
    #[test]
    fn tool_remove_route_is_parsed() {
        let parsed = Cli::try_parse_from(["mediapm", "tool", "remove", "yt-dlp"]);
        assert!(parsed.is_ok(), "tool remove route must parse");
    }

    /// Protects parent-owned mediapm defaults for direct `cas` passthrough.
    #[test]
    fn inject_cas_passthrough_defaults_adds_root_when_missing() {
        let injected = inject_cas_passthrough_defaults(
            &["optimize".to_string()],
            PathBuf::from(".mediapm/store").as_path(),
        );

        assert_eq!(injected[0], "--root");
        assert_eq!(injected[1], ".mediapm/store");
        assert_eq!(injected[2], "optimize");
    }

    /// Protects explicit passthrough overrides so user-supplied CAS roots win.
    #[test]
    fn inject_cas_passthrough_defaults_respects_explicit_root() {
        let injected = inject_cas_passthrough_defaults(
            &["--root".to_string(), "custom-store".to_string(), "optimize".to_string()],
            PathBuf::from(".mediapm/store").as_path(),
        );

        assert_eq!(
            injected,
            vec!["--root".to_string(), "custom-store".to_string(), "optimize".to_string(),]
        );
    }

    /// Protects direct help passthrough by avoiding default-root injection
    /// when CAS help/version output is requested.
    #[test]
    fn inject_cas_passthrough_defaults_skips_root_for_help_routes() {
        let injected = inject_cas_passthrough_defaults(
            &["--help".to_string()],
            PathBuf::from("store").as_path(),
        );

        assert_eq!(injected, vec!["--help".to_string()]);
    }

    /// Protects parent-owned mediapm defaults for `mediapm conductor ...`
    /// passthrough invocations.
    #[test]
    fn inject_conductor_passthrough_defaults_adds_effective_runtime_paths() {
        let paths = MediaPmPaths::from_root("/tmp/demo-root");
        let injected = inject_conductor_passthrough_defaults(&["state".to_string()], &paths);

        assert!(injected.contains(&"--conductor-dir".to_string()));
        assert!(injected.contains(&"/tmp/demo-root/.mediapm".to_string()));
        assert!(injected.contains(&"--cas-store-dir".to_string()));
        assert!(injected.contains(&"/tmp/demo-root/.mediapm/store".to_string()));
        assert!(injected.contains(&"--conductor-tools-dir".to_string()));
        assert!(injected.contains(&"/tmp/demo-root/.mediapm/tools".to_string()));
        assert_eq!(injected.last().map(String::as_str), Some("state"));
    }

    /// Protects `mediapm cas ...` passthrough by requiring that help rendering
    /// succeeds without external setup.
    #[tokio::test]
    async fn passthrough_cas_help_is_routable() {
        let temp = tempdir().expect("tempdir");
        let default_root = temp.path().join("store");

        let result = super::passthrough_cas(&["--help".to_string()], &default_root).await;

        assert!(result.is_ok(), "cas passthrough help should succeed: {result:?}");
    }

    /// Protects `mediapm tool run <tool> ...` passthrough routing by requiring
    /// that conductor subcommand help is reachable through the injected
    /// runtime-path wrapper.
    #[tokio::test]
    async fn passthrough_conductor_tool_run_help_is_routable() {
        let temp = tempdir().expect("tempdir");
        let paths = MediaPmPaths::from_root(temp.path());

        let result = super::passthrough_conductor(
            &["tool".to_string(), "run".to_string(), "--help".to_string()],
            &paths,
        )
        .await;

        assert!(result.is_ok(), "conductor passthrough help should succeed: {result:?}");
    }

    /// Protects tool-list passthrough CLI route.
    #[test]
    fn tool_list_route_is_parsed() {
        let parsed = Cli::try_parse_from(["mediapm", "tool", "list"]);
        assert!(parsed.is_ok(), "tool list route must parse");
    }

    /// Protects tool-prune CLI route.
    #[test]
    fn tool_prune_route_is_parsed() {
        let parsed = Cli::try_parse_from(["mediapm", "tool", "prune", "tool-id"]);
        assert!(parsed.is_ok(), "tool prune route must parse");
    }

    /// Protects tool-prune with metadata flag.
    #[test]
    fn tool_prune_with_metadata_is_parsed() {
        let parsed = Cli::try_parse_from(["mediapm", "tool", "prune", "tool-id", "--metadata"]);
        assert!(parsed.is_ok(), "tool prune --metadata must parse");
    }
}
