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
    AddInsertPosition, MediaHierarchyPreset, MediaPmService, MediaRuntimeStorage,
    ToolRegistryStatus, builtins::media_tagger::InternalMediaTaggerOptions,
    ensure_global_directory_layout, global_tool_cache_clear, global_tool_cache_prune_expired,
    global_tool_cache_status, load_runtime_dotenv_for_root, resolve_default_global_paths,
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
    /// Overrides `runtime.conductor_state_config` for this command invocation.
    #[arg(long)]
    conductor_state_config: Option<PathBuf>,
    /// Overrides `runtime.media_state_config` for this command invocation.
    #[arg(long)]
    media_state_config: Option<PathBuf>,
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
    Builtins {
        /// Builtins subcommand selector.
        #[command(subcommand)]
        command: BuiltinsCommand,
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
enum ToolsCommand {
    /// Adds one tool requirement entry to `mediapm.ncl` by logical name.
    ///
    /// The tool must appear in the built-in downloader catalog. If a
    /// requirement for this name already exists, the command is a no-op.
    /// After adding, run `tools sync` to download and register the tool.
    Add {
        /// Logical tool name (e.g. `yt-dlp`, `ffmpeg`, `rsgain`, `media-tagger`, `sd`).
        name: String,
    },
    /// Reconciles desired tool requirements only (no workflow/materialization run).
    ///
    /// Default policy checks remote updates for tag-only selectors unless
    /// `--no-check-tag-updates` is provided.
    Sync(ToolsSyncArgs),
    /// Lists registered tools and binary status.
    List,
    /// Removes one tool requirement entry from `mediapm.ncl`.
    ///
    /// This updates desired tool state only. To remove already-downloaded
    /// binaries for inactive entries, use `tools prune` with immutable tool id.
    Remove {
        /// Logical tool name.
        name: String,
    },
    /// Removes one installed tool binary while keeping metadata.
    Prune {
        /// Immutable tool id.
        id: String,
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
    /// Optional `MusicBrainz` recording UUID.
    ///
    /// When supplied the recording is validated and its title, artist, and
    /// description are used as the authoritative source metadata instead of
    /// the values probed from the source file or downloader tool.
    #[arg(long)]
    recording_id: Option<String>,
    /// Insertion position policy for media-map mutation.
    #[arg(long, value_enum, default_value_t = InsertPosition::Sorted)]
    insert_position: InsertPosition,
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

/// Builtin command implementations exposed by `mediapm builtins ...`.
#[derive(Debug, Subcommand)]
enum BuiltinsCommand {
    /// Native metadata tagging flow (`Chromaprint -> AcoustID -> MusicBrainz`).
    #[command(name = "media-tagger")]
    MediaTagger(InternalMediaTaggerArgs),
}

/// Arguments for one internal media-tagger invocation.
#[derive(Debug, Args)]
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
    /// Internal slot count used when emitting deterministic cover-art
    /// attachment members for downstream ffmpeg apply stages.
    #[arg(
        long,
        default_value_t = mediapm::builtins::media_tagger::DEFAULT_COVER_ART_SLOT_COUNT
    )]
    cover_art_slot_count: usize,
    /// Optional recording MBID override.
    #[arg(long)]
    recording_mbid: Option<String>,
    /// Optional release MBID override.
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
#[expect(
    clippy::too_many_lines,
    reason = "this item intentionally keeps end-to-end control flow together so ordering invariants remain explicit during maintenance"
)]
async fn main() -> anyhow::Result<()> {
    let cli = Cli::parse();
    let runtime_storage_overrides = MediaRuntimeStorage {
        mediapm_dir: option_path_to_string(cli.mediapm_dir),
        hierarchy_root_dir: None,
        mediapm_tmp_dir: None,
        materialization_preference_order: None,
        conductor_config: option_path_to_string(cli.conductor_config),
        conductor_machine_config: option_path_to_string(cli.conductor_machine_config),
        conductor_state_config: option_path_to_string(cli.conductor_state_config),
        conductor_tmp_dir: None,
        conductor_schema_dir: None,
        inherited_env_vars: None,
        media_state_config: option_path_to_string(cli.media_state_config),
        env_file: option_path_to_string(cli.env_file),
        mediapm_schema_dir: None,
        use_user_tool_cache: None,
    };
    if matches!(
        &cli.command,
        Command::Sync(_)
            | Command::Tools { .. }
            | Command::Media { .. }
            | Command::Hierarchy { .. }
            | Command::Builtins { .. }
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
            ToolsCommand::Add { name } => {
                let added = service.add_tool_requirement(&name)?;
                if added {
                    println!(
                        "added tool requirement '{name}' (tag = latest); run 'tools sync' to download"
                    );
                } else {
                    println!(
                        "tool requirement '{name}' already exists; run 'tools sync' to update"
                    );
                }
            }
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
            ToolsCommand::Remove { name } => {
                let removed = service.remove_tool_requirement(&name)?;
                if removed {
                    println!(
                        "removed tool requirement '{name}'; run 'tools sync' to reconcile runtime state"
                    );
                } else {
                    println!("tool requirement '{name}' was not present");
                }
            }
            ToolsCommand::Prune { id } => {
                let removed_hashes = service.prune_tool(&id).await?;
                println!("pruned tool binary for {id} (removed_hashes={removed_hashes})");
            }
            ToolsCommand::Run { tool, args } => {
                let exit_code = service.run_managed_tool(&tool, &args)?;
                if exit_code != 0 {
                    std::process::exit(exit_code);
                }
            }
            ToolsCommand::RefreshRuntime => {
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
                                args.recording_id.as_deref(),
                                map_insert_position(args.insert_position),
                            )
                            .await?
                    }
                    MediaAddPreset::Local => {
                        let path = PathBuf::from(args.source);
                        service
                            .add_local_source_with_position(
                                &path,
                                args.recording_id.as_deref(),
                                map_insert_position(args.insert_position),
                            )
                            .await?
                    }
                };
                println!("registered media source id={media_id}");
            }
            MediaCommand::Remove { media_id } => {
                let removed_hierarchy_nodes = service.remove_media_source(&media_id)?;
                println!(
                    "removed media source id={media_id} (removed_hierarchy_nodes={removed_hierarchy_nodes})"
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
                )?;
                println!(
                    "registered hierarchy preset={} for media id={} at folder={}",
                    args.preset.to_possible_value().expect("value enum").get_name(),
                    args.media_id,
                    effective_root
                );
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
        Command::Cas(args) => {
            passthrough_cas(&args.args).await?;
        }
        Command::Conductor(args) => {
            passthrough_conductor(&args.args).await?;
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

/// Executes Phase-1 CAS CLI passthrough in-process.
///
/// This path reuses `mediapm-cas` clap parsing and command dispatch directly,
/// so `mediapm cas ...` does not require a sibling `mediapm-cas` executable.
async fn passthrough_cas(args: &[String]) -> anyhow::Result<()> {
    mediapm_cas::cli::run_from_passthrough_args(args).await
}

/// Executes Phase-2 conductor CLI passthrough in-process.
///
/// This path reuses `mediapm-conductor` clap parsing and command dispatch
/// directly, so `mediapm conductor ...` does not require a sibling
/// `mediapm-conductor` executable.
async fn passthrough_conductor(args: &[String]) -> anyhow::Result<()> {
    mediapm_conductor::cli::run_from_passthrough_args(args).await.map_err(anyhow::Error::from)
}

#[cfg(test)]
mod tests {
    use clap::Parser;

    use super::Cli;

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

    /// Protects tools-remove CLI route.
    #[test]
    fn tools_remove_route_is_parsed() {
        let parsed = Cli::try_parse_from(["mediapm", "tools", "remove", "yt-dlp"]);
        assert!(parsed.is_ok(), "tools remove route must parse");
    }
}
