//! Binary entrypoint for the `mediapm` CLI.
//!
//! This CLI exposes:
//! - media/tool declarative state management,
//! - sync/materialization orchestration,
//! - passthrough command surfaces for the CAS and conductor CLIs.

use std::path::PathBuf;

use clap::{Args, CommandFactory, Parser, Subcommand, ValueEnum};
use clap_complete::Shell;
use mediapm::{
    AddInsertPosition, MediaHierarchyPreset, MediaPmPaths, MediaPmService, MediaRuntimeStorage,
    ToolRegistryStatus, builtins::media_tagger::InternalMediaTaggerOptions,
    ensure_global_directory_layout, global_tool_cache_clear, global_tool_cache_prune_expired,
    global_tool_cache_status, load_runtime_dotenv_for_root, resolve_default_global_paths,
    resolve_effective_paths_for_root,
};
use url::Url;

/// `mediapm` orchestration CLI.
#[derive(Debug, Parser)]
#[command(author, version, about = "mediapm orchestration CLI")]
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
    /// Overrides `runtime.conductor_state_config` for this command invocation.
    #[arg(long)]
    conductor_state_config: Option<PathBuf>,
    /// Overrides `runtime.media_state_config` for this command invocation.
    #[arg(long)]
    media_state_config: Option<PathBuf>,
    /// Overrides `runtime.env_file` for this command invocation.
    #[arg(long)]
    env_file: Option<PathBuf>,
    /// Overrides `runtime.env_generated_file` for this command invocation.
    #[arg(long)]
    env_generated_file: Option<PathBuf>,
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
        /// Logical tool name (e.g. `yt-dlp`, `ffmpeg`, `deno`, `rsgain`, `media-tagger`, `sd`).
        name: String,
    },
    /// Reconciles desired tool requirements only (no workflow/materialization run).
    ///
    /// Default policy checks remote updates for tag-only selectors unless
    /// `--no-check-tag-updates` is provided.
    Sync(ToolSyncArgs),
    /// Lists registered tools and binary status.
    List,
    /// Removes one tool requirement entry from `mediapm.ncl`.
    ///
    /// This updates desired tool state only. To remove already-downloaded
    /// binaries for inactive entries, use `tool prune` with immutable tool id.
    Remove {
        /// Logical tool name.
        name: String,
    },
    /// Removes one installed tool binary while keeping metadata.
    ///
    /// Pass `--metadata` to also remove tool metadata from the machine document
    /// and the tool registry entirely.  This is useful when a tool is fully
    /// retired and you no longer want to track its historical state.
    Prune {
        /// Immutable tool id.
        id: String,
        /// Also remove tool metadata from the machine document and registry.
        ///
        /// When set the tool id is completely erased from conductor state.  This
        /// is not recommended for tools that may be re-provisioned because it
        /// forces a full re-fetch on the next sync.
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

/// Media-source commands.
#[derive(Debug, Subcommand)]
enum MediaCommand {
    /// Adds one media source using one explicit preset.
    Add(MediaAddArgs),
    /// Removes one existing media source id from `mediapm.ncl`.
    Remove {
        /// Existing media id in `mediapm.ncl`.
        media_id: String,
    },
    /// Invalidates completed tool calls for one media step.
    Invalidate(MediaInvalidateArgs),
}

/// Arguments for `mediapm media invalidate`.
#[derive(Debug, Args)]
struct MediaInvalidateArgs {
    /// Existing media id in `mediapm.ncl`.
    media_id: String,
    /// Zero-based media step index under `media.<id>.steps`.
    step_index: usize,
    /// Invalidation mode.
    #[arg(long, value_enum, default_value_t = MediaInvalidateMode::ToolCallsOnly)]
    mode: MediaInvalidateMode,
}

/// Supported media-step invalidation modes.
#[derive(Debug, Clone, Copy, PartialEq, Eq, ValueEnum, Default)]
enum MediaInvalidateMode {
    /// Invalidate completed tool calls only.
    #[default]
    ToolCallsOnly,
    /// Invalidate completed tool calls and regenerate this media step.
    ToolCallsAndRegenerate,
}

/// Hierarchy commands.
#[derive(Debug, Subcommand)]
enum HierarchyCommand {
    /// Adds one hierarchy entry using one explicit preset.
    Add(HierarchyAddArgs),
    /// Removes one hierarchy entry using one explicit preset.
    Remove(HierarchyRemoveArgs),
}

/// Arguments for `mediapm media add`.
#[derive(Debug, Args)]
struct MediaAddArgs {
    /// Media-add preset (`yt-dlp` or `local`).
    #[arg(long, value_enum)]
    preset: MediaAddPreset,
    /// Source value interpreted by the selected preset.
    ///
    /// - `yt-dlp`: online URI (`http` or `https`)
    /// - `local`: filesystem path
    source: String,
    /// Optional title override. Takes precedence over metadata fetched from
    /// the source.
    #[arg(long)]
    title: Option<String>,
    /// Optional artist override. Takes precedence over metadata fetched from
    /// the source.
    #[arg(long)]
    artist: Option<String>,
    /// Optional description override. Takes precedence over metadata fetched
    /// from the source.
    #[arg(long)]
    description: Option<String>,
    /// Optional `MusicBrainz` recording MBID UUID passed through to the
    /// media-tagger step.
    #[arg(long)]
    recording_mbid: Option<String>,
    /// Optional `MusicBrainz` release MBID UUID passed through to the
    /// media-tagger step.
    #[arg(long)]
    release_mbid: Option<String>,
    /// Insertion position policy for media-map mutation.
    #[arg(long, value_enum, default_value_t = InsertPosition::Sorted)]
    insert_position: InsertPosition,
    /// Overwrite an existing media entry with the same id.
    #[arg(long)]
    overwrite: bool,
}

/// Media-add presets.
#[derive(Debug, Clone, Copy, PartialEq, Eq, ValueEnum)]
enum MediaAddPreset {
    /// Online downloader preset (`yt-dlp -> ffmpeg -> media-tagger -> rsgain`).
    YtDlp,
    /// Local importer preset (`import -> media-tagger -> rsgain`).
    Local,
}

/// Arguments for `mediapm hierarchy add`.
#[derive(Debug, Args)]
struct HierarchyAddArgs {
    /// Hierarchy-add preset.
    #[arg(long, value_enum)]
    preset: HierarchyPreset,
    /// Optional hierarchy root folder.
    ///
    /// When omitted, defaults to `media/` for all presets.
    #[arg(long = "root-folder", alias = "folder")]
    root_folder: Option<String>,
    /// Existing media id in `mediapm.ncl`.
    media_id: String,
    /// Insertion position policy inside the affected root-folder group.
    #[arg(long, value_enum, default_value_t = InsertPosition::Sorted)]
    insert_position: InsertPosition,
    /// Overwrite an existing hierarchy node with the same id.
    #[arg(long)]
    overwrite: bool,
}

/// Arguments for `mediapm hierarchy remove`.
#[derive(Debug, Args)]
struct HierarchyRemoveArgs {
    /// Hierarchy-remove preset.
    #[arg(long, value_enum)]
    preset: HierarchyPreset,
    /// Optional hierarchy root folder.
    ///
    /// When omitted, defaults to `media/` for all presets.
    #[arg(long = "root-folder", alias = "folder")]
    root_folder: Option<String>,
    /// Existing media id in `mediapm.ncl`.
    media_id: String,
}

/// CLI insertion-position values for add commands.
#[derive(Debug, Clone, Copy, PartialEq, Eq, ValueEnum, Default)]
enum InsertPosition {
    /// Keep deterministic sorted insertion behavior.
    #[default]
    Sorted,
    /// Insert at the beginning of the affected logical group.
    Beginning,
    /// Insert at the end of the affected logical group.
    End,
}

/// Hierarchy presets.
#[derive(Debug, Clone, Copy, PartialEq, Eq, ValueEnum)]
enum HierarchyPreset {
    /// Local-source hierarchy preset.
    Local,
    /// Online yt-dlp hierarchy preset.
    YtDlp,
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

/// Builtin command implementations exposed by `mediapm builtin ...`.
#[derive(Debug, Subcommand)]
enum BuiltinCommand {
    /// Native metadata tagging flow (`Chromaprint -> AcoustID -> MusicBrainz`).
    #[command(name = "media-tagger")]
    MediaTagger(InternalMediaTaggerArgs),
}

/// Arguments for one internal media-tagger invocation.
#[derive(Debug, Args, Clone)]
#[expect(
    clippy::struct_excessive_bools,
    reason = "builtin media-tagger CLI intentionally exposes independent boolean toggles"
)]
struct InternalMediaTaggerArgs {
    /// Optional input media payload path.
    ///
    /// This may be omitted when MBID-based identity is supplied (for example
    /// `--recording-mbid`) and the invocation only needs metadata fetch.
    #[arg(long)]
    input: Option<PathBuf>,
    /// Output media payload path.
    #[arg(long)]
    output: PathBuf,
    /// Optional `AcoustID` API key override.
    ///
    /// When omitted and no recording MBID override is provided, `AcoustID`
    /// lookup now fails immediately with a configuration error.
    /// When provided, `AcoustID` lookup/authentication failures are also
    /// surfaced as hard errors.
    #[arg(long)]
    acoustid_api_key: Option<String>,
    /// `AcoustID` lookup endpoint.
    #[arg(long, default_value = mediapm::builtins::media_tagger::DEFAULT_ACOUSTID_ENDPOINT)]
    acoustid_endpoint: String,
    /// `MusicBrainz` endpoint label used in diagnostics.
    #[arg(long, default_value = mediapm::builtins::media_tagger::DEFAULT_MUSICBRAINZ_ENDPOINT)]
    musicbrainz_endpoint: String,
    /// Optional persistent cache directory for metadata/cover-art fetches.
    #[arg(long)]
    cache_dir: Option<PathBuf>,
    /// Cache-expiry budget in seconds.
    ///
    /// Negative values disable expiry and keep cached rows indefinitely.
    #[arg(long, default_value_t = mediapm::builtins::media_tagger::DEFAULT_CACHE_EXPIRY_SECONDS)]
    cache_expiry_seconds: i64,
    /// Enables strict failure when recording identity or metadata cannot be resolved.
    #[arg(long, default_value_t = true)]
    strict_identification: bool,
    /// Enables extended Picard-compatible tag projection from `MusicBrainz` payloads.
    #[arg(long, default_value_t = true)]
    write_all_tags: bool,
    /// Enables binary cover-art attachment preparation plus Picard-compatible
    /// `coverart_*` metadata enrichment.
    #[arg(long, default_value_t = true)]
    write_all_images: bool,
    /// Enables embedding selected cover-art images into output tags.
    #[arg(
        long,
        default_value_t = mediapm::builtins::media_tagger::DEFAULT_SAVE_IMAGES_TO_TAGS
    )]
    save_images_to_tags: bool,
    /// Keeps only one `front` cover image when available.
    ///
    /// Picard defaults this to enabled; mediapm defaults it to disabled so
    /// selected non-front image kinds can also be embedded.
    #[arg(
        long,
        default_value_t = mediapm::builtins::media_tagger::DEFAULT_EMBED_ONLY_ONE_FRONT_IMAGE
    )]
    embed_only_one_front_image: bool,
    /// Ordered cover-art provider selector list.
    #[arg(long, default_value = mediapm::builtins::media_tagger::DEFAULT_CA_PROVIDERS)]
    ca_providers: String,
    /// CAA image-type selector expression (`all` with optional excludes).
    #[arg(long, default_value = mediapm::builtins::media_tagger::DEFAULT_CAA_IMAGE_TYPES)]
    caa_image_types: String,
    /// Requested CAA image-size selector (`full`, `large`, `medium`, `small`).
    #[arg(long, default_value = mediapm::builtins::media_tagger::DEFAULT_CAA_IMAGE_SIZE)]
    caa_image_size: String,
    /// Restricts CAA entries to approved-only when enabled.
    #[arg(
        long,
        default_value_t = mediapm::builtins::media_tagger::DEFAULT_CAA_APPROVED_ONLY
    )]
    caa_approved_only: bool,
    /// Preserves existing embedded image payloads during clear-tag mode when enabled.
    #[arg(
        long,
        default_value_t = mediapm::builtins::media_tagger::DEFAULT_PRESERVE_IMAGES
    )]
    preserve_images: bool,
    /// Clears existing textual tags before applying newly resolved metadata.
    #[arg(
        long,
        default_value_t = mediapm::builtins::media_tagger::DEFAULT_CLEAR_EXISTING_TAGS
    )]
    clear_existing_tags: bool,
    /// Enables output-tag writing.
    #[arg(
        long,
        default_value_t = mediapm::builtins::media_tagger::DEFAULT_ENABLE_TAG_SAVING
    )]
    enable_tag_saving: bool,
    /// Enables release relationship processing for provider logic that uses ARs.
    #[arg(long, default_value_t = mediapm::builtins::media_tagger::DEFAULT_RELEASE_ARS)]
    release_ars: bool,
    /// Internal slot count used when emitting deterministic cover-art
    /// attachment members for downstream ffmpeg apply stages.
    #[arg(
        long,
        default_value_t = mediapm::builtins::media_tagger::DEFAULT_COVER_ART_SLOT_COUNT
    )]
    cover_art_slot_count: usize,
    /// Optional recording MBID override.
    ///
    /// Sentinel values:
    /// - `auto`/empty => allow AcoustID autodetection,
    /// - `none` => disable AcoustID autodetection.
    #[arg(long)]
    recording_mbid: Option<String>,
    /// Optional release MBID override.
    ///
    /// Sentinel values:
    /// - `auto`/empty => treat as unspecified release MBID,
    /// - `none` => treat as unspecified release MBID and disable AcoustID
    ///   autodetection.
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
    /// Enables materialization verification (recomputes BLAKE3 hash after write).
    #[arg(long, conflicts_with = "no_verify_materialization")]
    verify_materialization: bool,
    /// Disables materialization verification.
    #[arg(long)]
    no_verify_materialization: bool,
}

impl VerifyMaterializationArgs {
    /// Resolves effective verify-materialization policy using command-specific default.
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

/// Arguments for `mediapm tool sync`.
#[derive(Debug, Args, Clone, Copy, Default)]
struct ToolSyncArgs {
    /// Optional override for tag-only tool update checks.
    #[command(flatten)]
    tag_update_policy: TagUpdatePolicyArgs,
    /// Optional override for materialization verification.
    #[command(flatten)]
    verify_materialization: VerifyMaterializationArgs,
}

#[tokio::main]
/// Parses CLI args and executes one top-level command.
#[expect(
    clippy::too_many_lines,
    reason = "this item intentionally keeps end-to-end control flow together so ordering invariants remain explicit during maintenance"
)]
async fn main() -> anyhow::Result<()> {
    let cli = Cli::parse();
    let runtime_storage_overrides = MediaRuntimeStorage {
        mediapm_dir: option_path_to_string(cli.mediapm_dir),
        hierarchy_root_dir: None,
        materialization_preference_order: None,
        conductor_config: option_path_to_string(cli.conductor_config),
        conductor_machine_config: option_path_to_string(cli.conductor_machine_config),
        conductor_state_config: option_path_to_string(cli.conductor_state_config),
        conductor_schema_dir: None,
        inherited_env_vars: None,
        media_state_config: option_path_to_string(cli.media_state_config),
        env_file: option_path_to_string(cli.env_file),
        env_generated_file: option_path_to_string(cli.env_generated_file),
        mediapm_schema_dir: None,
        profiler_enabled: None,
        verify_materialization: None,
        instance_ttl_seconds: None,
        path_sanitization: None,
    };
    let passthrough_runtime_storage_overrides = runtime_storage_overrides.clone();
    if matches!(
        &cli.command,
        Command::Sync(_)
            | Command::Tool { .. }
            | Command::Media { .. }
            | Command::Hierarchy { .. }
            | Command::Builtin { .. }
    ) {
        let _ = load_runtime_dotenv_for_root(&cli.root, &runtime_storage_overrides)?;
    }
    let service = MediaPmService::new_in_memory_at_with_runtime_storage_overrides(
        &cli.root,
        runtime_storage_overrides,
    );

    match cli.command {
        Command::Sync(args) => {
            let check_tag_updates = args.tag_update_policy.resolve(true);
            let verify_materialization = args.verify_materialization.resolve(true);
            let summary = service
                .sync_library_with_tag_update_checks(
                    check_tag_updates,
                    Some(verify_materialization),
                )
                .await?;
            println!(
                "sync complete: executed={}, cached={}, rematerialized={}, materialized={}, removed={}, removed_empty_dirs={}",
                summary.executed_instances,
                summary.cached_instances,
                summary.rematerialized_instances,
                summary.materialized_paths,
                summary.removed_paths,
                summary.removed_empty_dirs,
            );
            for warning in summary.warnings {
                eprintln!("warning: {warning}");
            }
        }
        Command::Tool { command } => match command {
            ToolCommand::Add { name } => {
                let added = service.add_tool_requirement(&name)?;
                if added {
                    println!(
                        "added tool requirement '{name}' (tag = latest); run 'tool sync' to download"
                    );
                } else {
                    println!("tool requirement '{name}' already exists; run 'tool sync' to update");
                }
            }
            ToolCommand::Sync(args) => {
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
            ToolCommand::List => {
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
            ToolCommand::Remove { name } => {
                let removed = service.remove_tool_requirement(&name)?;
                if removed {
                    println!(
                        "removed tool requirement '{name}'; run 'tool sync' to reconcile runtime state"
                    );
                } else {
                    println!("tool requirement '{name}' was not present");
                }
            }
            ToolCommand::Prune { id, metadata } => {
                let removed_hashes = service.prune_tool(&id, metadata).await?;
                println!("pruned tool binary for {id} (removed_hashes={removed_hashes})");
            }
            ToolCommand::Run { tool, args } => {
                let effective_paths = MediaPmPaths::from_root(&cli.root)
                    .with_runtime_storage(&passthrough_runtime_storage_overrides);
                let mut conductor_args = vec!["tool".to_string(), "run".to_string()];
                conductor_args.push("--tool".to_string());
                conductor_args.push(tool);
                conductor_args.extend(args);
                passthrough_conductor(&conductor_args, &effective_paths).await?;
            }
            ToolCommand::RefreshRuntime => {
                service.refresh_runtime_configuration()?;
                println!(
                    "refreshed mediapm-managed conductor runtime configuration and dotenv files"
                );
            }
        },
        Command::Media { command } => match command {
            MediaCommand::Add(args) => {
                let media_id = match args.preset {
                    MediaAddPreset::YtDlp => {
                        let uri = Url::parse(&args.source)?;
                        service
                            .add_media_source_with_position(
                                &uri,
                                args.title.as_deref(),
                                args.artist.as_deref(),
                                args.description.as_deref(),
                                args.recording_mbid.as_deref(),
                                args.release_mbid.as_deref(),
                                map_insert_position(args.insert_position),
                                args.overwrite,
                            )
                            .await?
                    }
                    MediaAddPreset::Local => {
                        let path = PathBuf::from(args.source);
                        service
                            .add_local_source_with_position(
                                &path,
                                args.title.as_deref(),
                                args.artist.as_deref(),
                                args.description.as_deref(),
                                args.recording_mbid.as_deref(),
                                args.release_mbid.as_deref(),
                                map_insert_position(args.insert_position),
                                args.overwrite,
                            )
                            .await?
                    }
                };
                println!("registered media source id={media_id}");
                eprintln!(
                    "hint: run 'mediapm sync' to apply workflow/hierarchy changes (and 'mediapm tool sync' first if tools are out of date)"
                );
            }
            MediaCommand::Remove { media_id } => {
                let removed_hierarchy_nodes = service.remove_media_source(&media_id)?;
                println!(
                    "removed media source id={media_id} (removed_hierarchy_nodes={removed_hierarchy_nodes})"
                );
                eprintln!("hint: run 'mediapm sync' to apply workflow/hierarchy changes");
            }
            MediaCommand::Invalidate(args) => {
                let summary = match args.mode {
                    MediaInvalidateMode::ToolCallsOnly => {
                        service
                            .invalidate_media_step_tool_calls(&args.media_id, args.step_index)
                            .await?
                    }
                    MediaInvalidateMode::ToolCallsAndRegenerate => {
                        service
                            .invalidate_media_step_tool_calls_and_regenerate(
                                &args.media_id,
                                args.step_index,
                            )
                            .await?
                    }
                };

                println!(
                    "invalidated media id={} step_index={} mode={} workflow_id={} targeted_steps={} removed_impure_timestamps={} removed_instances={}",
                    args.media_id,
                    args.step_index,
                    args.mode.to_possible_value().expect("value enum").get_name(),
                    summary.workflow_id,
                    summary.targeted_step_ids.join(","),
                    summary.removed_impure_timestamps,
                    summary.removed_instances,
                );
                eprintln!(
                    "hint: run 'mediapm sync' to apply invalidation effects to materialized outputs"
                );
            }
        },
        Command::Hierarchy { command } => match command {
            HierarchyCommand::Add(args) => {
                let preset = map_hierarchy_preset(args.preset);
                let effective_root = args
                    .root_folder
                    .as_deref()
                    .unwrap_or_else(|| default_hierarchy_root_for_preset(args.preset));
                service.add_media_hierarchy_preset_with_position(
                    preset,
                    &args.media_id,
                    args.root_folder.as_deref(),
                    map_insert_position(args.insert_position),
                    args.overwrite,
                )?;
                println!(
                    "registered hierarchy preset={} for media id={} at folder={}",
                    args.preset.to_possible_value().expect("value enum").get_name(),
                    args.media_id,
                    effective_root
                );
                eprintln!("hint: run 'mediapm sync' to apply workflow/hierarchy changes");
            }
            HierarchyCommand::Remove(args) => {
                let preset = map_hierarchy_preset(args.preset);
                let effective_root = args
                    .root_folder
                    .as_deref()
                    .unwrap_or_else(|| default_hierarchy_root_for_preset(args.preset));
                let removed_nodes = service.remove_media_hierarchy_preset(
                    preset,
                    &args.media_id,
                    effective_root,
                )?;
                println!(
                    "removed hierarchy preset={} for media id={} at folder={} (removed_nodes={removed_nodes})",
                    args.preset.to_possible_value().expect("value enum").get_name(),
                    args.media_id,
                    effective_root
                );
                eprintln!("hint: run 'mediapm sync' to apply workflow/hierarchy changes");
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
        Command::Builtin { command } => match command.as_ref() {
            BuiltinCommand::MediaTagger(args) => run_builtin_media_tagger(args.clone()).await?,
        },
        Command::Cas(args) => {
            let effective_paths = resolve_effective_paths_for_root(
                &cli.root,
                &passthrough_runtime_storage_overrides,
            )?;
            passthrough_cas(&args.args, &effective_paths.runtime_root.join("store")).await?;
        }
        Command::Conductor(args) => {
            let effective_paths = resolve_effective_paths_for_root(
                &cli.root,
                &passthrough_runtime_storage_overrides,
            )?;
            passthrough_conductor(&args.args, &effective_paths).await?;
        }
        Command::Completions { shell } => {
            clap_complete::generate(shell, &mut Cli::command(), "mediapm", &mut std::io::stdout());
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
        cache_dir: args.cache_dir,
        cache_expiry_seconds: args.cache_expiry_seconds,
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
    })
    .await
}

/// Converts one optional path into UTF-8-ish string form used by config structs.
#[must_use]
fn option_path_to_string(path: Option<PathBuf>) -> Option<String> {
    path.map(|value| value.to_string_lossy().to_string())
}

/// Converts CLI hierarchy preset values into service-layer presets.
#[must_use]
fn map_hierarchy_preset(preset: HierarchyPreset) -> MediaHierarchyPreset {
    match preset {
        HierarchyPreset::Local => MediaHierarchyPreset::Local,
        HierarchyPreset::YtDlp => MediaHierarchyPreset::YtDlp,
    }
}

/// Converts CLI insertion-position values into service-layer insertion policy.
#[must_use]
fn map_insert_position(position: InsertPosition) -> AddInsertPosition {
    match position {
        InsertPosition::Sorted => AddInsertPosition::Sorted,
        InsertPosition::Beginning => AddInsertPosition::Beginning,
        InsertPosition::End => AddInsertPosition::End,
    }
}

/// Returns preset-specific default hierarchy root folder for CLI add/remove.
#[must_use]
fn default_hierarchy_root_for_preset(_preset: HierarchyPreset) -> &'static str {
    "media/"
}

/// Executes CAS CLI passthrough in-process.
///
/// This path reuses `mediapm-cas` clap parsing and command dispatch directly,
/// so `mediapm cas ...` does not require a sibling `mediapm-cas` executable.
async fn passthrough_cas(args: &[String], default_root: &std::path::Path) -> anyhow::Result<()> {
    let injected = inject_cas_passthrough_defaults(args, default_root);
    mediapm_cas::cli::run_from_passthrough_args(&injected).await
}

/// Executes conductor CLI passthrough in-process.
///
/// This path reuses `mediapm-conductor` clap parsing and command dispatch
/// directly, so `mediapm conductor ...` does not require a sibling
/// `mediapm-conductor` executable.
async fn passthrough_conductor(
    args: &[String],
    effective_paths: &mediapm::MediaPmPaths,
) -> anyhow::Result<()> {
    let injected = inject_conductor_passthrough_defaults(args, effective_paths);
    mediapm_conductor::cli::run_from_passthrough_args(&injected).await.map_err(anyhow::Error::from)
}

/// Injects default `cas` root when the caller did not provide one explicitly.
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

/// Injects resolved mediapm-owned conductor runtime defaults into passthrough argv.
fn inject_conductor_passthrough_defaults(
    args: &[String],
    effective_paths: &mediapm::MediaPmPaths,
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
        "--config",
        effective_paths.conductor_user_ncl.to_string_lossy().to_string(),
    );
    append_passthrough_option_if_missing(
        &mut injected,
        args,
        "--config-machine",
        effective_paths.conductor_machine_ncl.to_string_lossy().to_string(),
    );
    append_passthrough_option_if_missing(
        &mut injected,
        args,
        "--config-state",
        effective_paths.conductor_state_config.to_string_lossy().to_string(),
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

/// Returns true when any long option name is already present in passthrough argv.
fn passthrough_option_present(args: &[String], option_names: &[&str]) -> bool {
    args.iter().any(|arg| {
        option_names.iter().any(|option_name| {
            arg == option_name
                || arg.strip_prefix(option_name).is_some_and(|suffix| suffix.starts_with('='))
        })
    })
}

/// Returns true when passthrough argv explicitly requests help/version text.
fn passthrough_requests_help_or_version(args: &[String]) -> bool {
    args.iter().any(|arg| {
        matches!(arg.as_str(), "-h" | "--help" | "-V" | "--version")
            || arg.split_once('=').is_some_and(|(flag, _)| matches!(flag, "--help" | "--version"))
    })
}

#[cfg(test)]
mod tests {
    use clap::Parser;

    use std::path::PathBuf;

    use mediapm::MediaPmPaths;
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
        let parsed = Cli::try_parse_from([
            "mediapm",
            "media",
            "add",
            "--preset",
            "yt-dlp",
            "https://example.com/media",
        ]);
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

    /// Protects media-step invalidation CLI route with regeneration mode.
    #[test]
    fn media_invalidate_route_with_regenerate_mode_is_parsed() {
        let parsed = Cli::try_parse_from([
            "mediapm",
            "media",
            "invalidate",
            "media-123",
            "2",
            "--mode",
            "tool-calls-and-regenerate",
        ]);
        assert!(parsed.is_ok(), "media invalidate regenerate mode must parse");
    }

    /// Protects hierarchy-add local preset route with explicit root folder.
    #[test]
    fn hierarchy_add_local_route_with_root_folder_is_parsed() {
        let parsed = Cli::try_parse_from([
            "mediapm",
            "hierarchy",
            "add",
            "--preset",
            "local",
            "--root-folder",
            "music videos",
            "media-123",
        ]);
        assert!(parsed.is_ok(), "hierarchy add local route must parse");
    }

    /// Protects hierarchy-add yt-dlp preset route with explicit root folder.
    #[test]
    fn hierarchy_add_yt_dlp_route_with_root_folder_is_parsed() {
        let parsed = Cli::try_parse_from([
            "mediapm",
            "hierarchy",
            "add",
            "--preset",
            "yt-dlp",
            "--root-folder",
            "music videos",
            "media-123",
        ]);
        assert!(parsed.is_ok(), "hierarchy add yt-dlp route must parse");
    }

    /// Protects hierarchy-remove route with explicit root folder.
    #[test]
    fn hierarchy_remove_route_with_root_folder_is_parsed() {
        let parsed = Cli::try_parse_from([
            "mediapm",
            "hierarchy",
            "remove",
            "--preset",
            "yt-dlp",
            "--root-folder",
            "music videos",
            "media-123",
        ]);
        assert!(parsed.is_ok(), "hierarchy remove route must parse");
    }

    /// Protects hierarchy-add route when preset default root folder is used.
    #[test]
    fn hierarchy_add_allows_omitting_root_folder_for_default() {
        let parsed =
            Cli::try_parse_from(["mediapm", "hierarchy", "add", "--preset", "yt-dlp", "media-123"]);
        assert!(parsed.is_ok(), "hierarchy add should allow preset default root folder");
    }

    /// Protects media-add insertion-position CLI parsing.
    #[test]
    fn media_add_accepts_insert_position() {
        let parsed = Cli::try_parse_from([
            "mediapm",
            "media",
            "add",
            "--preset",
            "yt-dlp",
            "--insert-position",
            "beginning",
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
            vec!["--root".to_string(), "custom-store".to_string(), "optimize".to_string()]
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
        assert!(injected.contains(&"--config".to_string()));
        assert!(injected.contains(&"/tmp/demo-root/mediapm.conductor.ncl".to_string()));
        assert!(injected.contains(&"--config-machine".to_string()));
        assert!(injected.contains(&"/tmp/demo-root/mediapm.conductor.machine.ncl".to_string()));
        assert!(injected.contains(&"--config-state".to_string()));
        assert!(injected.contains(&"/tmp/demo-root/.mediapm/state.conductor.ncl".to_string()));
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
}
