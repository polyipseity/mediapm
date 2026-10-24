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
use clap::{CommandFactory, Parser};
#[cfg(feature = "cli")]
use mediapm::MediaPmService;
#[cfg(feature = "cli")]
use mediapm::RecheckPolicy;
#[cfg(feature = "cli")]
use mediapm::config::{RuntimeMaterializationConfig, RuntimePathsConfig};
use mediapm::output::{
    StatusIcon, print_error, print_hint, print_result, print_status_report, print_warning,
};
#[cfg(feature = "cli")]
use mediapm::{
    MediaMetadataValue, MediaPmGlobalPaths, MediaPmPaths, MediaRuntimeStorage, MediaSourceSpec,
    MediaStep, MediaStepTool, OutputVariantValue, TransformInputValue, YtDlpOutputKind,
    YtDlpOutputVariantConfig, ensure_global_directory_layout, global_tool_cache_clear,
    global_tool_cache_prune_expired, global_tool_cache_status, load_runtime_dotenv,
    media_id_from_uri, resolve_effective_paths_for_root,
};
#[cfg(feature = "media-tagger")]
use mediapm_cli::InternalMediaTaggerArgs;
#[cfg(feature = "cli")]
use mediapm_cli::{
    BuiltinCommand, Cli, Command, GlobalCommand, GlobalToolCacheCommand, HierarchyCommand,
    HierarchyPresetArg, MediaAddPreset, MediaCommand, RecheckPolicyArgs, SyncArgs, ToolCommand,
};
#[cfg(feature = "cli")]
use url::Url;

/// Exit status for a run that reported warnings and nothing worse.
///
/// Warnings are printed as `Δ` lines, so this status says the same thing to a
/// caller reading only the number. `2` is left alone because POSIX reserves it
/// for shell misuse.
const EXIT_WARNING: i32 = 3;

/// Exit status for a `sync` that left the library short of what the config
/// asked for.
///
/// Its own status, so a caller can tell a library missing entries from a
/// binary that never ran: the missing-`cli`-feature path exits `1` and says so
/// on stderr, a warning exits `3`, this one exits `4` and names the counts.
const EXIT_INCOMPLETE_LIBRARY: i32 = 4;

/// Exit status for a finished `sync`, or `None` when the run leaves the library
/// in the state it was asked to build.
///
/// This is the only place the mapping from [`mediapm::output::SyncOutcome`] to
/// a process status lives. Split out from the call site so the rule is
/// testable without ending the test process, and reading
/// [`mediapm::output::sync_outcome`] rather than the counters keeps the icon
/// and the status from being two rules over the same fields.
#[must_use]
fn sync_exit_status(summary: &mediapm::SyncSummary) -> Option<i32> {
    match mediapm::output::sync_outcome(summary) {
        mediapm::output::SyncOutcome::Clean => None,
        mediapm::output::SyncOutcome::Warning => Some(EXIT_WARNING),
        mediapm::output::SyncOutcome::Error => Some(EXIT_INCOMPLETE_LIBRARY),
    }
}

/// Exit status for a finished `tool sync`, or `None` when it went clean.
///
/// The only place the mapping from [`mediapm::output::ToolSyncOutcome`] to a
/// process status lives for `mediapm tool sync`. A run whose report carries
/// warnings takes [`EXIT_WARNING`] and not [`EXIT_INCOMPLETE_LIBRARY`]: the
/// warnings are non-fatal notices (resolved-field backfills, hints), so the
/// library gained nothing missing. Provisioning failures never reach this
/// function — under fail fast they abort the sync before any summary exists.
///
/// A tool sync that cannot run at all returns an error out of
/// [`MediaPmService::sync_tools_with_tag_update_checks`] and never reaches this
/// function.
#[must_use]
fn tool_sync_exit_status(summary: &mediapm::ToolsSyncSummary) -> Option<i32> {
    match mediapm::output::tool_sync_outcome(summary) {
        mediapm::output::ToolSyncOutcome::Clean => None,
        mediapm::output::ToolSyncOutcome::Warning => Some(EXIT_WARNING),
    }
}

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

    let cli_mediapm_dir = cli.mediapm_dir.clone();
    // The CLI wants *resolved* runtime overrides, not a wire document, so the
    // resolved model is built directly. Every knob the CLI does not expose
    // keeps its `Default` value, which is exactly what resolving a boundary
    // block with only these fields set would have produced.
    let rt = MediaRuntimeStorage {
        paths: RuntimePathsConfig {
            mediapm_dir: cli_mediapm_dir.clone().unwrap_or_default(),
            hierarchy_root_dir: cli.hierarchy_root_dir.clone().unwrap_or_default(),
            mediapm_state_config: cli.media_state_config.clone().unwrap_or_default(),
            conductor_config: cli.conductor_config.clone().unwrap_or_default(),
            conductor_generated_config: cli.conductor_generated_config.clone().unwrap_or_default(),
            conductor_state_config: cli.conductor_state_config.clone().unwrap_or_default(),
            conductor_schema_dir: PathBuf::new(),
            mediapm_schema_dir: cli.mediapm_schema_dir.clone().unwrap_or_default(),
            env_file: cli.env_file.clone().unwrap_or_default(),
            env_generated_file: cli.env_generated_file.clone().unwrap_or_default(),
        },
        materialization: RuntimeMaterializationConfig::default(),
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
            let options = sync_library_options(args);
            let summary = service.sync_library_with_options(options).await?;
            mediapm::output::print_sync_summary(&summary);
            match sync_exit_status(&summary) {
                None => {}
                // The `Δ` lines above are the warning, so only the status is
                // left to add.
                Some(EXIT_WARNING) => std::process::exit(EXIT_WARNING),
                Some(status) => {
                    print_error(format!(
                        "sync did not produce everything the config asked for: \
                         missing={} failed_steps={}",
                        summary.missing_paths, summary.workflow_failed_steps
                    ));
                    std::process::exit(status);
                }
            }
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
                service.add_tool_requirement(&name, None)?;
                print_result(
                    StatusIcon::Success,
                    "tool added",
                    &[("name", &name as &dyn std::fmt::Display), ("tag", &"latest")],
                    None,
                );
                print_hint("run 'mediapm tool sync' to download and register");
                Ok(())
            }
            ToolCommand::Sync(args) => {
                let root = &cli.root;
                let paths = resolve_effective_paths_for_root(root, &rt);
                load_runtime_dotenv(&paths.env_file, &paths.env_generated_file);
                let mut service =
                    MediaPmService::new_fs_at_with_runtime_storage_overrides(root, rt).await?;
                let recheck_policy = resolve_recheck_policy(args.recheck_policy);
                let check_tag_updates = matches!(recheck_policy, RecheckPolicy::ForceReResolve);
                let no_progress = args.no_progress;
                let summary = service
                    .sync_tools_with_tag_update_checks(check_tag_updates, no_progress)
                    .await?;
                print_result(
                    mediapm::output::tool_sync_icon(&summary),
                    "tools synced",
                    &[
                        ("added", &summary.added_tools as &dyn std::fmt::Display),
                        ("updated", &summary.updated_tools),
                        ("pruned", &summary.pruned_tools),
                        ("removed", &summary.removed_tools),
                    ],
                    None,
                );
                for warning in &summary.warnings {
                    print_warning(warning);
                }
                // The `Δ` lines above carry which tool failed and why, so the
                // status only has to add that the run was not clean.
                if let Some(status) = tool_sync_exit_status(&summary) {
                    std::process::exit(status);
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
                print_result(
                    StatusIcon::Success,
                    "tool removed",
                    &[("name", &name as &dyn std::fmt::Display)],
                    None,
                );
                print_hint("run 'mediapm tool sync' to reconcile runtime state");
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
                print_result(StatusIcon::Success, "runtime refreshed", &[], None);
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
                                title: args.title.clone().unwrap_or_default(),
                                description: args.description.clone().unwrap_or_default(),
                                artist: args.artist.clone().unwrap_or_default(),
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
                            service
                                .add_local_source_with_position(
                                    &path,
                                    args.ffprobe_command.as_deref().unwrap_or("ffprobe"),
                                    None,
                                    args.insert_position.into(),
                                    args.overwrite,
                                )
                                .await?
                        }
                    };
                    print_result(
                        StatusIcon::Success,
                        "media added",
                        &[("id", &media_id as &dyn std::fmt::Display)],
                        None,
                    );
                    print_hint(
                        "run 'mediapm sync' to apply workflow/hierarchy changes (and 'mediapm tool sync' first if tools are out of date)",
                    );
                    Ok(())
                }
                MediaCommand::Remove { media_id } => {
                    service.remove_media_source(&media_id)?;
                    print_result(
                        StatusIcon::Success,
                        "media removed",
                        &[("id", &media_id as &dyn std::fmt::Display)],
                        None,
                    );
                    print_hint("run 'mediapm sync' to apply workflow/hierarchy changes");
                    Ok(())
                }
                MediaCommand::Invalidate(args) => {
                    let summary = if args.undo {
                        service
                            .revalidate_media_step_tool_calls(&args.media_id, args.step_index)
                            .await?
                    } else {
                        service
                            .invalidate_media_step_tool_calls(&args.media_id, args.step_index)
                            .await?
                    };
                    let marker = if args.undo { "cleared" } else { "marked" };
                    print_result(
                        StatusIcon::Success,
                        if args.undo { "media revalidated" } else { "media invalidated" },
                        &[
                            ("id", &args.media_id as &dyn std::fmt::Display),
                            ("step", &args.step_index),
                            ("workflow", &summary.workflow_id as &dyn std::fmt::Display),
                            ("steps", &summary.targeted_step_ids.len()),
                            ("instances", &summary.changed_instances),
                            ("unchanged", &summary.unchanged_instances),
                        ],
                        None,
                    );
                    if summary.changed_instances == 0 && summary.unchanged_instances == 0 {
                        print_hint(
                            "no recorded instance carries the step ids this index resolves to; \
                             the next sync runs the step from scratch",
                        );
                    } else if summary.changed_instances == 0 {
                        print_hint(
                            "every matching instance already carried this marker, so there is \
                             nothing new to apply",
                        );
                    } else {
                        print_hint(format!(
                            "{marker} {} instance(s); run 'mediapm sync' to apply",
                            summary.changed_instances,
                        ));
                    }
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
                    print_result(
                        StatusIcon::Success,
                        "hierarchy added",
                        &[(
                            "preset",
                            &(match args.preset {
                                HierarchyPresetArg::Local => "local",
                                HierarchyPresetArg::YtDlp => "yt-dlp",
                            }) as &dyn std::fmt::Display,
                        )],
                        None,
                    );
                    print_hint("run 'mediapm sync' to apply workflow/hierarchy changes");
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
                    print_result(
                        StatusIcon::Success,
                        "hierarchy removed",
                        &[("id", &media_id as &dyn std::fmt::Display), ("count", &removed)],
                        None,
                    );
                    print_hint("run 'mediapm sync' to apply workflow/hierarchy changes");
                    Ok(())
                }
            }
        }
        Command::Global { command } => match command {
            GlobalCommand::Path => {
                let paths = MediaPmGlobalPaths::resolve_default().ok_or_else(|| {
                    anyhow::anyhow!("could not resolve global user directory for this environment")
                })?;
                print_status_report(&[
                    ("root_dir", &paths.root_dir.display() as &dyn std::fmt::Display),
                    ("tool_cache_dir", &paths.tool_cache_dir.display()),
                    ("tool_cache_store", &paths.tool_cache_store_dir.display()),
                    ("tool_cache_index", &paths.tool_cache_index.display()),
                ]);
                Ok(())
            }
            GlobalCommand::Init => {
                ensure_global_directory_layout()?;
                let paths = MediaPmGlobalPaths::resolve_default().unwrap();
                print_result(
                    StatusIcon::Success,
                    "global dir initialized",
                    &[("path", &paths.root_dir.display() as &dyn std::fmt::Display)],
                    None,
                );
                Ok(())
            }
            GlobalCommand::ToolCache { command } => match command {
                GlobalToolCacheCommand::Status => {
                    let status = global_tool_cache_status(None).await?;
                    print_status_report(&[
                        (
                            "tool_cache_dir",
                            &status.tool_cache_dir.display() as &dyn std::fmt::Display,
                        ),
                        ("store_dir", &status.store_dir.display()),
                        ("index", &status.index.display()),
                        ("entry_count", &status.entry_count),
                    ]);
                    Ok(())
                }
                GlobalToolCacheCommand::Prune => {
                    let summary = global_tool_cache_prune_expired(None).await?;
                    print_result(
                        StatusIcon::Success,
                        "tool cache pruned",
                        &[
                            ("removed_entries", &summary.removed_entries as &dyn std::fmt::Display),
                            ("removed_payloads", &summary.removed_payloads),
                        ],
                        None,
                    );
                    Ok(())
                }
                GlobalToolCacheCommand::Clear => {
                    global_tool_cache_clear()?;
                    print_result(StatusIcon::Success, "tool cache cleared", &[], None);
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

/// Resolves the recheck policy a `sync` invocation asked for.
///
/// A free function rather than a method on [`RecheckPolicyArgs`]: the method
/// form has to be written in the crate that defines the type, and that crate
/// now lives in `mediapm-cli`, which cannot name `RecheckPolicy`.
#[must_use]
fn resolve_recheck_policy(args: RecheckPolicyArgs) -> RecheckPolicy {
    if args.force_re_resolve {
        RecheckPolicy::ForceReResolve
    } else if args.no_force_re_resolve {
        RecheckPolicy::UseCached
    } else {
        RecheckPolicy::ForceReResolve
    }
}

/// Builds the library-sync options `mediapm sync` runs with.
///
/// Extracted from the command arm so the flag-to-option mapping is exercised
/// by a test instead of only by a live sync: a flag that parses but never
/// reaches the options would look identical from the command line.
fn sync_library_options(args: SyncArgs) -> mediapm::SyncLibraryOptions {
    let check_tag_updates =
        matches!(resolve_recheck_policy(args.recheck_policy), RecheckPolicy::ForceReResolve);
    mediapm::SyncLibraryOptions {
        verify_materialization: args.verify_materialization.resolve(true),
        check_tag_updates,
        no_progress: args.no_progress,
        observer: Some(std::sync::Arc::new(mediapm::output::observer::CliSyncObserver)),
        force_workflow_rerun: args.force_rerun,
    }
}

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
                OutputVariantValue::YtDlp(YtDlpOutputVariantConfig {
                    kind: YtDlpOutputKind::Primary,
                    ..Default::default()
                }),
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
                OutputVariantValue::YtDlp(YtDlpOutputVariantConfig {
                    kind: YtDlpOutputKind::Primary,
                    ..Default::default()
                }),
            )]),
            options: BTreeMap::new(),
        },
        MediaStep {
            tool: MediaStepTool::MediaTagger,
            input_variants: vec!["media".to_string()],
            output_variants: BTreeMap::from([(
                "media".to_string(),
                OutputVariantValue::YtDlp(YtDlpOutputVariantConfig {
                    kind: YtDlpOutputKind::Primary,
                    ..Default::default()
                }),
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
                OutputVariantValue::YtDlp(YtDlpOutputVariantConfig {
                    kind: YtDlpOutputKind::Primary,
                    ..Default::default()
                }),
            )]),
            options: BTreeMap::new(),
        },
    ]
}

/// Executes builtin media-tagger command invocation via `run_internal_media_tagger`.
#[cfg(feature = "media-tagger")]
#[expect(
    clippy::cast_possible_wrap,
    reason = "u32 option values cast to i64; wrap is impossible for the bounded input domain"
)]
async fn run_builtin_media_tagger(args: InternalMediaTaggerArgs) -> anyhow::Result<()> {
    mediapm::builtins::media_tagger::run_internal_media_tagger(
        mediapm::builtins::media_tagger::InternalMediaTaggerOptions {
            input_path: Some(std::path::PathBuf::from(args.input)),
            output_path: std::path::PathBuf::from(args.output),
            acoustid_api_key: args.acoustid_api_key,
            acoustid_endpoint: args.acoustid_endpoint,
            musicbrainz_endpoint: args.musicbrainz_endpoint,
            caa_endpoint: args.caa_endpoint,
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
///
/// The CAS entry point supplies the program name clap reads from position zero,
/// so the vector built here carries the caller's arguments plus the injected
/// store locator only.
async fn passthrough_cas(args: &[String], default_store: &std::path::Path) -> anyhow::Result<()> {
    let injected = inject_cas_passthrough_defaults(args, default_store);
    mediapm_cas::cli::run_from_passthrough_args(&injected).await
}

/// Executes conductor CLI passthrough in-process.
///
/// The conductor entry point supplies the program name clap reads from position
/// zero, so the vector built here carries the caller's arguments plus the
/// injected runtime paths only. Prepending a name would give the vector two.
async fn passthrough_conductor(
    args: &[String],
    effective_paths: &MediaPmPaths,
) -> anyhow::Result<()> {
    let injected = inject_conductor_passthrough_defaults(args, effective_paths);
    let passthrough: Vec<&str> = injected.iter().map(String::as_str).collect();
    mediapm_conductor::cli::run_from_args(&passthrough).await.map_err(Into::into)
}

/// Injects a default CAS store locator when the caller did not provide one.
///
/// The injected flag is the one the CAS CLI declares, `--storage`, and its value
/// is a storage locator, which a directory path is. The name here has to match
/// the definition rather than describe the value: a flag the CLI does not
/// declare makes the whole vector fail to parse.
#[must_use]
fn inject_cas_passthrough_defaults(
    args: &[String],
    default_store: &std::path::Path,
) -> Vec<String> {
    if passthrough_requests_help_or_version(args) {
        return args.to_vec();
    }

    let mut injected = Vec::new();
    if !passthrough_option_present(args, &["--storage"]) {
        injected.push("--storage".to_string());
        injected.push(default_store.to_string_lossy().to_string());
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

#[cfg(test)]
mod tests {
    use clap::Parser;
    use mediapm::MediaPmPaths;
    use std::path::PathBuf;

    use super::{
        Cli, Command, EXIT_INCOMPLETE_LIBRARY, EXIT_WARNING, MediaCommand,
        inject_cas_passthrough_defaults, inject_conductor_passthrough_defaults, sync_exit_status,
        tool_sync_exit_status,
    };

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

    /// `--undo` selects the clearing half of the command.
    #[test]
    fn media_invalidate_accepts_undo() {
        let parsed =
            Cli::try_parse_from(["mediapm", "media", "invalidate", "media-123", "2", "--undo"]);
        let cli = parsed.expect("media invalidate --undo must parse");
        let Command::Media { command } = cli.command else { panic!("expected a media command") };
        let MediaCommand::Invalidate(args) = command else { panic!("expected invalidate") };
        assert!(args.undo, "--undo must reach the args struct");
    }

    /// The two retired modes stay parseable as aliases.
    ///
    /// They named "clear recorded progress" and "queue a run", neither of which
    /// exists any more, so they select nothing — but a script that still passes
    /// one must not start failing.
    #[test]
    fn media_invalidate_still_accepts_the_retired_flags() {
        for flag in ["--invalidate-calls", "--regenerate"] {
            let parsed =
                Cli::try_parse_from(["mediapm", "media", "invalidate", "media-123", "2", flag]);
            assert!(parsed.is_ok(), "media invalidate {flag} must still parse");
        }
    }

    /// `--undo` cannot be combined with a flag that asks for the opposite.
    #[test]
    fn media_invalidate_rejects_undo_with_a_marking_flag() {
        let parsed = Cli::try_parse_from([
            "mediapm",
            "media",
            "invalidate",
            "media-123",
            "2",
            "--undo",
            "--invalidate-calls",
        ]);
        assert!(parsed.is_err(), "--undo and --invalidate-calls contradict each other");
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
    fn inject_cas_passthrough_defaults_adds_storage_when_missing() {
        let injected = inject_cas_passthrough_defaults(
            &["stat".to_string()],
            PathBuf::from(".mediapm/store").as_path(),
        );

        assert_eq!(injected[0], "--storage");
        assert_eq!(injected[1], ".mediapm/store");
        assert_eq!(injected[2], "stat");
    }

    /// Protects explicit passthrough overrides so user-supplied CAS stores win.
    #[test]
    fn inject_cas_passthrough_defaults_respects_explicit_storage() {
        let injected = inject_cas_passthrough_defaults(
            &["--storage".to_string(), "custom-store".to_string(), "stat".to_string()],
            PathBuf::from(".mediapm/store").as_path(),
        );

        assert_eq!(
            injected,
            vec!["--storage".to_string(), "custom-store".to_string(), "stat".to_string(),]
        );
    }

    /// Protects direct help passthrough by avoiding default-store injection
    /// when CAS help/version output is requested.
    #[test]
    fn inject_cas_passthrough_defaults_skips_storage_for_help_routes() {
        let injected = inject_cas_passthrough_defaults(
            &["--help".to_string()],
            PathBuf::from("store").as_path(),
        );

        assert_eq!(injected, vec!["--help".to_string()]);
    }

    /// The flag and subcommand the injection tests use are declared by the CAS
    /// CLI.
    ///
    /// Those tests assert the injected vector's shape without parsing it, so a
    /// flag or subcommand the CLI does not declare leaves them green while every
    /// real invocation fails. This is the check that the `--root` injection and
    /// the `optimize` subcommand beside it would have failed.
    #[test]
    fn cas_passthrough_test_inputs_are_declared_by_the_cas_cli() {
        let command = mediapm_cas_cli::command();
        let declared: Vec<&str> = command.get_subcommands().map(clap::Command::get_name).collect();
        assert!(
            declared.contains(&"stat"),
            "the CAS CLI must declare the subcommand these tests pass through; declared: \
             {declared:?}"
        );
        assert!(
            command.get_arguments().any(|argument| argument.get_long() == Some("storage")),
            "the CAS CLI must declare the `--storage` flag the injection supplies"
        );
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
        let temp = mediapm_utils::temp::artifact_dir().expect("tempdir");
        let default_store = temp.path().join("store");

        let result = super::passthrough_cas(&["--help".to_string()], &default_store).await;

        assert!(result.is_ok(), "cas passthrough help should succeed: {result:?}");
    }

    /// Protects `mediapm cas stat --help` passthrough by requiring that help
    /// rendering succeeds and targets the `stat` subcommand rather than the
    /// root command. Before the fix, the help fast-path matched `--help`
    /// anywhere in the args and printed root help; now clap parses the
    /// subcommand first and renders its help.
    #[tokio::test]
    async fn passthrough_cas_subcommand_help_prints_subcommand_help() {
        let temp = mediapm_utils::temp::artifact_dir().expect("tempdir");
        let default_store = temp.path().join("store");

        let result =
            super::passthrough_cas(&["stat".to_string(), "--help".to_string()], &default_store)
                .await;

        assert!(result.is_ok(), "cas passthrough subcommand help should succeed: {result:?}");
    }

    /// Protects `mediapm tool run <tool> ...` passthrough routing by requiring
    /// that conductor subcommand help is reachable through the injected
    /// runtime-path wrapper.
    #[tokio::test]
    async fn passthrough_conductor_tool_run_help_is_routable() {
        let temp = mediapm_utils::temp::artifact_dir().expect("tempdir");
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

    /// Protects the explicit-invalidation surface: `--force-rerun` reaches the
    /// library-sync options, and a plain sync stays cache-first.
    ///
    /// A flag that parses but never reaches the options looks identical from
    /// the command line, so the mapping — not just the parse — is asserted.
    #[test]
    fn sync_force_rerun_flag_reaches_the_sync_options() {
        let parsed = Cli::try_parse_from(["mediapm", "sync", "--force-rerun"])
            .expect("sync --force-rerun must parse");
        let super::Command::Sync(args) = parsed.command else {
            panic!("--force-rerun must parse onto the sync command");
        };
        assert!(
            super::sync_library_options(args).force_workflow_rerun,
            "the flag must reach SyncLibraryOptions::force_workflow_rerun"
        );

        let default = Cli::try_parse_from(["mediapm", "sync"]).expect("plain sync must parse");
        let super::Command::Sync(default_args) = default.command else {
            panic!("a plain sync must parse onto the sync command");
        };
        assert!(
            !super::sync_library_options(default_args).force_workflow_rerun,
            "a plain sync must not force re-execution"
        );
    }

    /// Extracts the parsed media-tagger arguments from a `builtin
    /// media-tagger` CLI invocation.
    #[cfg(feature = "media-tagger")]
    fn parse_media_tagger_args(
        extra: &[&str],
    ) -> Result<super::InternalMediaTaggerArgs, clap::Error> {
        let mut argv = vec![
            "mediapm",
            "builtin",
            "media-tagger",
            "--input",
            "in.mkv",
            "--output",
            "out.ffmeta",
        ];
        argv.extend_from_slice(extra);
        let parsed = Cli::try_parse_from(argv)?;
        match parsed.command {
            super::Command::Builtin { command } => match *command {
                super::BuiltinCommand::MediaTagger(tagger_args) => Ok(tagger_args),
            },
            other => panic!("expected builtin route, got {other:?}"),
        }
    }

    /// Protects the CLI contract that a media-tagger boolean option accepts
    /// an explicit `false` value in both `--flag false` and `--flag=false`
    /// spellings — the workflow renders these as value pairs, and a parse
    /// failure would surface as a step error on every sync.
    #[cfg(feature = "media-tagger")]
    #[test]
    fn media_tagger_boolean_option_accepts_explicit_false() {
        let spaced = parse_media_tagger_args(&["--write-all-images", "false"])
            .expect("--write-all-images false must parse");
        assert!(!spaced.write_all_images, "--write-all-images false must parse to false");

        let equals = parse_media_tagger_args(&["--write-all-images=false"])
            .expect("--write-all-images=false must parse");
        assert!(!equals.write_all_images, "--write-all-images=false must parse to false");
    }

    /// Protects the defaults every absent boolean flag must keep: toggles
    /// the managed defaults set to `true` stay true, and the ones defaulting
    /// to `false` stay false, so omitting a flag never changes behavior.
    #[cfg(feature = "media-tagger")]
    #[test]
    fn media_tagger_boolean_option_defaults_are_preserved() {
        let args = parse_media_tagger_args(&[]).expect("baseline media-tagger args must parse");
        assert!(args.write_all_images, "absent --write-all-images must default to true");
        assert!(args.strict_identification, "absent --strict-identification must default to true");
        assert!(args.write_all_tags, "absent --write-all-tags must default to true");
        assert!(args.save_images_to_tags, "absent --save-images-to-tags must default to true");
        assert!(args.enable_tag_saving, "absent --enable-tag-saving must default to true");
        assert!(args.release_ars, "absent --release-ars must default to true");
        assert!(
            !args.embed_only_one_front_image,
            "absent --embed-only-one-front-image must default to false"
        );
        assert!(!args.caa_approved_only, "absent --caa-approved-only must default to false");
        assert!(!args.preserve_images, "absent --preserve-images must default to false");
        assert!(!args.clear_existing_tags, "absent --clear-existing-tags must default to false");
    }

    /// Protects the accepted CLI break: a boolean option is a value argument,
    /// so a bare flag without its value is a parse error rather than a silent
    /// fallback to `true`.
    #[cfg(feature = "media-tagger")]
    #[test]
    fn media_tagger_boolean_option_bare_flag_is_rejected() {
        let result = parse_media_tagger_args(&["--write-all-images"]);
        assert!(result.is_err(), "bare --write-all-images without a value must fail to parse");
    }

    /// Protects the `caa-endpoint` option: the default is the real Cover Art
    /// Archive, and an explicit value reaches the process so hermetic tests
    /// (and mirrors) can redirect image traffic the same way
    /// `--musicbrainz-endpoint` redirects entity traffic.
    #[cfg(feature = "media-tagger")]
    #[test]
    fn media_tagger_caa_endpoint_option_parses() {
        let args = parse_media_tagger_args(&[]).expect("baseline media-tagger args must parse");
        assert_eq!(
            args.caa_endpoint,
            mediapm::builtins::media_tagger::DEFAULT_CAA_ENDPOINT,
            "absent --caa-endpoint must default to the Cover Art Archive"
        );

        let args = parse_media_tagger_args(&["--caa-endpoint", "http://127.0.0.1:9/ws"])
            .expect("explicit --caa-endpoint must parse");
        assert_eq!(
            args.caa_endpoint, "http://127.0.0.1:9/ws",
            "an explicit --caa-endpoint value must reach the options unchanged"
        );
    }

    /// Protects tool-prune with metadata flag.
    #[test]
    fn tool_prune_with_metadata_is_parsed() {
        let parsed = Cli::try_parse_from(["mediapm", "tool", "prune", "tool-id", "--metadata"]);
        assert!(parsed.is_ok(), "tool prune --metadata must parse");
    }

    /// Builds a whole-run summary with every count at zero.
    ///
    /// Tests set the one or two fields their rule reads, so a summary a test
    /// did not mean to change cannot carry a count into the assertion.
    fn sync_summary() -> mediapm::SyncSummary {
        mediapm::SyncSummary {
            executed_instances: 0,
            cached_instances: 0,
            materialized_paths: 0,
            skipped_paths: 0,
            missing_paths: 0,
            removed_paths: 0,
            removed_empty_dirs: 0,
            added_tools: 0,
            updated_tools: 0,
            pruned_tools: 0,
            removed_tools: 0,
            skipped_tools: 0,
            workflow_failed_steps: 0,
            warnings: vec![],
        }
    }

    /// A run that wrote what it was asked to write exits as it always did.
    ///
    /// Without this, a rule that fails every `sync` would satisfy the tests
    /// below, including on a workspace that landed every path.
    #[test]
    fn sync_exits_zero_when_everything_landed() {
        let summary = mediapm::SyncSummary { materialized_paths: 5, ..sync_summary() };
        assert_eq!(sync_exit_status(&summary), None, "summary: {summary:?}");
    }

    /// A run whose entries were all already correct exits zero.
    ///
    /// This is the direction a wrong fix breaks. A skipped entry means the target
    /// already matched the resolved hash and its length, so a rule that reads it
    /// as damage turns a perfectly good second run into a red one.
    #[test]
    fn sync_exits_zero_when_every_entry_was_already_correct() {
        let summary =
            mediapm::SyncSummary { executed_instances: 2, skipped_paths: 3, ..sync_summary() };
        assert_eq!(
            sync_exit_status(&summary),
            None,
            "a skipped entry matched its recorded hash and length; \
             summary: {summary:?}"
        );
    }

    /// A path the run could not produce ends `mediapm sync` on the error status.
    ///
    /// The status holds whatever the counts around it say, so a run that wrote
    /// plenty of paths and missed one still tells a caller the library is short.
    #[test]
    fn sync_exits_four_when_a_path_is_missing() {
        let summary =
            mediapm::SyncSummary { materialized_paths: 5, missing_paths: 1, ..sync_summary() };
        assert_eq!(
            sync_exit_status(&summary),
            Some(EXIT_INCOMPLETE_LIBRARY),
            "the library is short an entry, so the command must not report success; \
             summary: {summary:?}"
        );
    }

    /// A failed workflow step ends the run on the error status too.
    ///
    /// It used to warn and exit zero, on the grounds that the paths the step
    /// owns may still have materialized. They materialize from what the step
    /// was supposed to produce, so that reasoning puts the run's exit code at
    /// the mercy of a coincidence.
    #[test]
    fn sync_exits_four_when_a_workflow_step_failed() {
        let summary = mediapm::SyncSummary {
            materialized_paths: 5,
            workflow_failed_steps: 1,
            ..sync_summary()
        };
        assert_eq!(
            sync_exit_status(&summary),
            Some(EXIT_INCOMPLETE_LIBRARY),
            "the media the failed step was to produce is not on disk; \
             summary: {summary:?}"
        );
    }

    /// A run that only warned takes the warning status, between clean and error.
    #[test]
    fn sync_exits_three_when_the_run_only_warned() {
        let summary = mediapm::SyncSummary {
            materialized_paths: 5,
            warnings: vec!["tools require sync before library sync: ffmpeg".to_string()],
            ..sync_summary()
        };
        assert_eq!(sync_exit_status(&summary), Some(EXIT_WARNING), "summary: {summary:?}");
    }

    /// Builds a tool-sync summary with every count at zero.
    fn tools_sync_summary() -> mediapm::ToolsSyncSummary {
        mediapm::ToolsSyncSummary {
            added_tools: 0,
            updated_tools: 0,
            pruned_tools: 0,
            removed_tools: 0,
            skipped_tools: 0,
            warnings: vec![],
        }
    }

    /// A run that registered every desired tool exits as it always did.
    ///
    /// The control the warning tests below read against. A rule that warned on
    /// any tool-sync summary at all satisfies them.
    #[test]
    fn tool_sync_exits_zero_when_every_tool_provisioned() {
        let summary =
            mediapm::ToolsSyncSummary { added_tools: 2, skipped_tools: 1, ..tools_sync_summary() };
        assert_eq!(tool_sync_exit_status(&summary), None, "summary: {summary:?}");
    }

    /// A warning-bearing run takes the warning status.
    ///
    /// The command used to print the `Δ` line and exit zero, so a script
    /// reading the number could not tell a clean run from one where notices
    /// were reported. Warnings are non-fatal notices (backfills, hints) —
    /// provisioning failures abort before a summary exists.
    #[test]
    fn tool_sync_exits_three_when_warnings_are_reported() {
        let summary = mediapm::ToolsSyncSummary {
            added_tools: 2,
            warnings: vec![
                "backfilled resolved fields for 3 tool(s) from provider metadata".to_string(),
            ],
            ..tools_sync_summary()
        };
        assert_eq!(tool_sync_exit_status(&summary), Some(EXIT_WARNING), "summary: {summary:?}");
    }

    /// A warning-bearing run does not take the library's status.
    ///
    /// This is the mistake the warning status is easy to make. A backfill or
    /// hint leaves nothing the library needs missing, so status 4 would tell
    /// a script the run had left work undone.
    #[test]
    fn tool_sync_does_not_exit_four_for_warnings() {
        let summary = mediapm::ToolsSyncSummary {
            added_tools: 2,
            warnings: vec![
                "backfilled resolved fields for 3 tool(s) from provider metadata".to_string(),
            ],
            ..tools_sync_summary()
        };
        assert_ne!(
            tool_sync_exit_status(&summary),
            Some(EXIT_INCOMPLETE_LIBRARY),
            "a tool-sync warning is not an incomplete library; summary: {summary:?}"
        );
    }

    /// The three statuses a sync can end on stay apart from each other and from
    /// the `1` the missing-`cli`-feature path already uses.
    ///
    /// `1` is written as a literal at that call site and cannot be asserted
    /// here with the `cli` feature on, so the comparison is against the value
    /// the other path prints. A script that reads only the number has nothing
    /// else to go on.
    #[test]
    fn exit_statuses_stay_distinct() {
        let missing_cli_feature: i32 = 1;
        assert_ne!(EXIT_WARNING, EXIT_INCOMPLETE_LIBRARY);
        assert_ne!(EXIT_WARNING, missing_cli_feature, "a warning must not read as a usage problem");
        assert_ne!(
            EXIT_INCOMPLETE_LIBRARY, missing_cli_feature,
            "an incomplete library must not read as a usage problem"
        );
    }
}
