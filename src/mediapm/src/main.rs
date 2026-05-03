//! Binary entrypoint for the Phase 3 `mediapm` CLI.
//!
//! This CLI exposes:
//! - media/tool declarative state management,
//! - sync/materialization orchestration,
//! - passthrough commands to Phase 1 CAS and Phase 2 conductor CLIs.

use std::path::{Path, PathBuf};
use std::process::Command as ProcessCommand;

use clap::{Args, Parser, Subcommand};
use mediapm::{
    MediaPmService, MediaRuntimeStorage, ToolRegistryStatus,
    builtins::media_tagger::InternalMediaTaggerOptions, ensure_global_directory_layout,
    global_tool_cache_clear, global_tool_cache_prune_expired, global_tool_cache_status,
    load_runtime_dotenv_for_root, resolve_default_global_paths,
};
use url::Url;

/// Executable suffix used by workspace binaries on the active host platform.
#[cfg(windows)]
const WORKSPACE_BINARY_SUFFIX: &str = ".exe";

/// Executable suffix used by workspace binaries on the active host platform.
#[cfg(not(windows))]
const WORKSPACE_BINARY_SUFFIX: &str = "";

/// Maximum number of parent directories searched for sibling passthrough
/// binaries when direct sibling and PATH lookups miss.
const MAX_ANCESTOR_BINARY_SEARCH_LEVELS: usize = 6;

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
    /// Runs one managed tool binary directly.
    Run {
        /// Immutable tool id or logical tool name.
        #[arg(long)]
        tool: String,
        /// Trailing arguments passed to the managed tool executable.
        #[arg(trailing_var_arg = true, allow_hyphen_values = true)]
        args: Vec<String>,
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
    /// Adds one local source file and records an `import` CAS-hash ingest step.
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
        Command::Sync(_) | Command::Tools { .. } | Command::Media { .. } | Command::Builtins { .. }
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
            ToolsCommand::Run { tool, args } => {
                let exit_code = service.run_managed_tool(&tool, &args)?;
                if exit_code != 0 {
                    std::process::exit(exit_code);
                }
            }
        },
        Command::Media { command } => match command {
            MediaCommand::Add { uri } => {
                let uri = Url::parse(&uri)?;
                let media_id = service.add_media_source(&uri)?;
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
) -> anyhow::Result<PathBuf> {
    let binary_file_name = workspace_binary_file_name(binary_stem);
    let current_executable = std::env::current_exe()?;
    let executable_directory = current_executable
        .parent()
        .ok_or_else(|| anyhow::anyhow!("current executable has no parent directory"))?;
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

    anyhow::bail!(
        "passthrough binary '{binary_stem}' was not found (attempts: {}). Fix by {} or placing '{}' next to '{}' / on PATH",
        attempts.join("; "),
        env_hint,
        binary_file_name,
        current_executable.display(),
    );
}

/// Runs one workspace binary resolved for production-safe passthrough.
fn run_workspace_binary(
    _package_name: &str,
    binary_stem: &str,
    args: &[String],
) -> anyhow::Result<()> {
    let env_override_name = match binary_stem {
        "mediapm-cas" => Some("MEDIAPM_CAS_BINARY"),
        "mediapm-conductor" => Some("MEDIAPM_CONDUCTOR_BINARY"),
        _ => None,
    };
    let binary_path = resolve_workspace_binary_path(binary_stem, env_override_name)?;

    let status = ProcessCommand::new(&binary_path).args(args).status()?;
    if status.success() {
        Ok(())
    } else {
        anyhow::bail!(
            "workspace passthrough binary '{}' exited with status {status}",
            binary_path.display()
        )
    }
}

/// Executes passthrough to Phase 1 CAS CLI using sibling workspace binaries.
fn passthrough_cas(args: &[String]) -> anyhow::Result<()> {
    run_workspace_binary("mediapm-cas", "mediapm-cas", args)
}

/// Executes passthrough to Phase 2 conductor CLI using sibling workspace binaries.
fn passthrough_conductor(args: &[String]) -> anyhow::Result<()> {
    run_workspace_binary("mediapm-conductor", "mediapm-conductor", args)
}

#[cfg(test)]
mod tests {
    use std::fs;

    use clap::Parser;

    use super::{
        Cli, MAX_ANCESTOR_BINARY_SEARCH_LEVELS, WORKSPACE_BINARY_SUFFIX,
        find_binary_in_ancestors_from, find_binary_in_paths, workspace_binary_file_name,
    };

    /// Keeps executable-name suffix behavior stable across host platforms.
    #[test]
    fn workspace_binary_file_name_applies_host_suffix() {
        assert_eq!(
            workspace_binary_file_name("mediapm-cas"),
            format!("mediapm-cas{WORKSPACE_BINARY_SUFFIX}")
        );
    }

    /// Protects directory-list passthrough binary lookup semantics.
    #[test]
    fn find_binary_in_paths_returns_first_file_match() {
        let root = tempfile::tempdir().expect("tempdir");
        let first = root.path().join("first");
        let second = root.path().join("second");
        fs::create_dir_all(&first).expect("first dir");
        fs::create_dir_all(&second).expect("second dir");

        let binary_name = workspace_binary_file_name("mediapm-cas");
        let binary_path = second.join(binary_name);
        fs::write(&binary_path, b"stub").expect("binary file");

        let found = find_binary_in_paths(
            "mediapm-cas",
            vec![first.clone(), second.clone(), root.path().to_path_buf()],
        )
        .expect("binary should be found");

        assert_eq!(found, binary_path);
    }

    /// Protects bounded ancestor lookup semantics for passthrough fallbacks.
    #[test]
    fn find_binary_in_ancestors_respects_max_level_budget() {
        let root = tempfile::tempdir().expect("tempdir");
        let level_0 = root.path().join("l0");
        let level_1 = level_0.join("l1");
        let level_2 = level_1.join("l2");
        fs::create_dir_all(&level_2).expect("nested directories");

        let binary_name = workspace_binary_file_name("mediapm-conductor");
        let binary_path = level_0.join(&binary_name);
        fs::write(&binary_path, b"stub").expect("binary");

        let miss = find_binary_in_ancestors_from("mediapm-conductor", &level_2, 1);
        assert!(miss.is_none(), "max level budget should prevent reaching level_0");

        let hit = find_binary_in_ancestors_from(
            "mediapm-conductor",
            &level_2,
            MAX_ANCESTOR_BINARY_SEARCH_LEVELS,
        )
        .expect("ancestor lookup should reach level_0 with default budget");
        assert_eq!(hit, binary_path);
    }

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
}
