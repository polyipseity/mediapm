//! Desired-tool reconciliation and prune flows for mediapm.

mod content_import;
mod lifecycle;
mod provision;
mod tool_config;

use self::content_import::import_tool_content_files_into_cas;
use self::lifecycle::{
    ensure_internal_launcher_content_entries_exist, is_builtin_source_ingest_requirement,
    is_hash_still_referenced_by_tool_configs, lock_registry_version,
    prune_unmanaged_tool_artifacts, should_skip_tag_update_check,
};
use self::provision::provision_desired_tools_concurrently;
use self::tool_config::{
    augment_tool_id_with_dependency_selector, ensure_machine_runtime_inherits_generated_env_vars,
    prefix_same_step_companion_content_entries, prefix_same_step_companion_content_map,
    prefix_same_step_companion_selector_path, remove_redundant_inherited_env_vars_from_tool_config,
    resolve_companion_deno_selection, resolve_companion_ffmpeg_selection,
    resolve_conductor_runtime_dir, resolve_managed_tool_payload_command_path_from_selector,
    resolve_managed_tool_payload_directory_from_selector, resolve_media_tagger_ffmpeg_selection,
    should_set_yt_dlp_ffmpeg_location, should_set_yt_dlp_js_runtimes,
    write_generated_runtime_env_file,
};
use std::collections::{BTreeMap, BTreeSet};
use std::sync::Arc;

use mediapm_cas::{CasApi, FileSystemCas, Hash};
use mediapm_conductor::runtime_env::ensure_runtime_env_files;
use mediapm_conductor::{AddToolOptions, InputBinding, ToolKindSpec};

use crate::builtins::media_tagger::MEDIA_TAGGER_FFMPEG_BIN_ENV;
use crate::config::MediaPmDocument;
use crate::config::{MediaPmState, ToolRegistryRecord, ToolRegistryStatus};
use crate::error::MediaPmError;
use crate::paths::MediaPmPaths;
use crate::tools::downloader::{ToolDownloadCache, default_global_tool_cache_root};

use super::ToolSyncReport;
use super::documents::{ensure_conductor_documents, load_machine_document, save_machine_document};
use super::runtime_storage::resolve_cas_store_path;
use super::tool_runtime::{
    YT_DLP_FFMPEG_LOCATION_ENV, YT_DLP_JS_RUNTIMES_ENV, build_tool_env, build_tool_spec,
    default_tool_config_description, merge_tool_config_defaults, resolve_ffmpeg_slot_limits,
    validate_tool_command,
};
use super::util::now_unix_seconds;

/// Reconciles desired tools from `mediapm.ncl` into conductor machine config.
#[expect(
    clippy::too_many_lines,
    reason = "this item intentionally keeps end-to-end control flow together so ordering invariants remain explicit during maintenance"
)]
#[allow(clippy::single_match, clippy::collapsible_if)]
pub(crate) async fn reconcile_desired_tools(
    paths: &MediaPmPaths,
    document: &MediaPmDocument,
    inherited_env_vars: &[String],
    lock: &mut MediaPmState,
    check_tag_updates: bool,
) -> Result<ToolSyncReport, MediaPmError> {
    ensure_conductor_documents(paths)?;

    let mut report = ToolSyncReport::default();
    let mut machine = load_machine_document(&paths.conductor_machine_ncl)?;
    let conductor_runtime_dir = resolve_conductor_runtime_dir(paths, &machine);
    ensure_runtime_env_files(&conductor_runtime_dir).map_err(MediaPmError::from)?;
    let ffmpeg_slot_limits = resolve_ffmpeg_slot_limits(&document.tools)?;
    let cas_root = resolve_cas_store_path(paths, &machine);
    let cas = FileSystemCas::open(&cas_root).await.map_err(|source| {
        MediaPmError::Workflow(format!(
            "opening conductor CAS store '{}' for tool sync failed: {source}",
            cas_root.display()
        ))
    })?;

    let mut requirements_to_provision = BTreeMap::new();
    let mut skipped_tag_update_tool_ids = BTreeMap::new();

    for (tool_name, requirement) in &document.tools {
        if is_builtin_source_ingest_requirement(tool_name) {
            continue;
        }

        if should_skip_tag_update_check(requirement, tool_name, lock, &machine, check_tag_updates)
            && let Some(active_tool_id) = lock.active_tools.get(tool_name).cloned()
        {
            skipped_tag_update_tool_ids.insert(tool_name.clone(), active_tool_id);
            continue;
        }

        requirements_to_provision.insert(tool_name.clone(), requirement.clone());
    }

    let shared_tool_cache = if requirements_to_provision.is_empty() {
        None
    } else {
        let cache_root = default_global_tool_cache_root().ok_or_else(|| {
            MediaPmError::Workflow(
                "resolving shared global user cache root for managed-tool downloads failed"
                    .to_string(),
            )
        })?;
        let cache = ToolDownloadCache::open(&cache_root).await.map_err(|error| {
            MediaPmError::Workflow(format!(
                "opening shared global user cache at '{}' failed: {error}",
                cache_root.display()
            ))
        })?;
        let _ = cache.prune_expired_entries().await;
        Some(Arc::new(cache))
    };

    let mut provisioned_by_name =
        provision_desired_tools_concurrently(paths, &requirements_to_provision, shared_tool_cache)
            .await?;
    let provisioned_snapshot = provisioned_by_name.clone();
    let mut desired_tool_ids = BTreeSet::new();
    let mut generated_runtime_env_vars = BTreeMap::<String, String>::new();

    for (name, requirement) in &document.tools {
        if is_builtin_source_ingest_requirement(name) {
            continue;
        }

        if let Some(active_tool_id) = skipped_tag_update_tool_ids.get(name) {
            desired_tool_ids.insert(active_tool_id.clone());
            report.unchanged_tool_ids.push(active_tool_id.clone());
            continue;
        }

        let provisioned = provisioned_by_name.remove(name).ok_or_else(|| {
            MediaPmError::Workflow(format!(
                "concurrent provisioning did not return payload for logical tool '{name}'"
            ))
        })?;
        report.warnings.extend(provisioned.warnings.clone());
        let mut effective_content_entries: BTreeMap<
            String,
            crate::tools::downloader::ContentMapSource,
        > = provisioned.content_entries.clone();
        let mut desired_tool_id = provisioned.tool_id.clone();
        #[allow(unused_assignments)]
        let mut media_tagger_ffmpeg_host_command_path: Option<String> = None;
        let mut yt_dlp_resolved_ffmpeg_path: Option<String> = None;
        let mut yt_dlp_resolved_js_runtimes_path: Option<String> = None;
        let mut companion_ffmpeg_content_map = BTreeMap::new();
        let mut companion_ffmpeg_host_command_path: Option<String> = None;
        let mut companion_deno_content_map = BTreeMap::new();
        let mut companion_deno_host_command_path: Option<String> = None;

        if name.eq_ignore_ascii_case("media-tagger") {
            let ffmpeg_selection = resolve_media_tagger_ffmpeg_selection(
                paths,
                requirement,
                &provisioned_snapshot,
                lock,
                &machine,
            )?;

            media_tagger_ffmpeg_host_command_path = ffmpeg_selection.host_command_path;
        }

        if name.eq_ignore_ascii_case("yt-dlp") {
            let companion_selection = resolve_companion_ffmpeg_selection(
                paths,
                name,
                requirement,
                &provisioned_snapshot,
                lock,
                &machine,
            )?;
            desired_tool_id = augment_tool_id_with_dependency_selector(
                &desired_tool_id,
                "ffmpeg",
                &companion_selection.selector,
            );
            companion_ffmpeg_content_map = prefix_same_step_companion_content_map(
                &companion_selection.companion_tool_id,
                &companion_selection.existing_content_map,
            );
            companion_ffmpeg_host_command_path =
                companion_selection.host_command_path.as_deref().and_then(|path| {
                    prefix_same_step_companion_selector_path(
                        &companion_selection.companion_tool_id,
                        path,
                    )
                });

            for (entry_key, entry_source) in prefix_same_step_companion_content_entries(
                &companion_selection.companion_tool_id,
                &companion_selection.provisioned_content_entries,
            ) {
                effective_content_entries.entry(entry_key).or_insert(entry_source);
            }

            let companion_deno_selection = resolve_companion_deno_selection(
                name,
                requirement,
                &provisioned_snapshot,
                lock,
                &machine,
            )?;
            desired_tool_id = augment_tool_id_with_dependency_selector(
                &desired_tool_id,
                "deno",
                &companion_deno_selection.selector,
            );
            companion_deno_content_map = prefix_same_step_companion_content_map(
                &companion_deno_selection.companion_tool_id,
                &companion_deno_selection.existing_content_map,
            );
            companion_deno_host_command_path =
                companion_deno_selection.host_command_path.as_deref().and_then(|path| {
                    prefix_same_step_companion_selector_path(
                        &companion_deno_selection.companion_tool_id,
                        path,
                    )
                });

            for (entry_key, entry_source) in prefix_same_step_companion_content_entries(
                &companion_deno_selection.companion_tool_id,
                &companion_deno_selection.provisioned_content_entries,
            ) {
                effective_content_entries.entry(entry_key).or_insert(entry_source);
            }
        }

        desired_tool_ids.insert(desired_tool_id.clone());
        let desired_version = lock_registry_version(&provisioned)?;
        let existing_active = lock.active_tools.get(name).cloned();
        let spec = build_tool_spec(paths, name, &provisioned, ffmpeg_slot_limits);
        let command_vector = match &spec.kind {
            ToolKindSpec::Executable { command, .. } => command.clone(),
            ToolKindSpec::Builtin { .. } => {
                return Err(MediaPmError::Workflow(format!(
                    "managed tool '{name}' unexpectedly resolved to builtin spec"
                )));
            }
        };

        ensure_internal_launcher_content_entries_exist(&provisioned, &effective_content_entries)?;

        let content_map =
            import_tool_content_files_into_cas(&cas, &effective_content_entries).await?;
        validate_tool_command(name, &command_vector, &content_map)?;
        let mut desired_config = merge_tool_config_defaults(
            machine.tool_configs.get(&desired_tool_id),
            paths,
            name,
            content_map,
            default_tool_config_description(
                name,
                &provisioned.identity,
                provisioned.catalog.description,
            ),
            ffmpeg_slot_limits,
        );
        if name.eq_ignore_ascii_case("yt-dlp") {
            for (relative_path, multihash) in companion_ffmpeg_content_map {
                desired_config
                    .content_map
                    .get_or_insert_with(BTreeMap::new)
                    .entry(relative_path)
                    .or_insert(multihash);
            }

            for (relative_path, multihash) in companion_deno_content_map {
                desired_config
                    .content_map
                    .get_or_insert_with(BTreeMap::new)
                    .entry(relative_path)
                    .or_insert(multihash);
            }

            // Always resolve companion paths when yt-dlp is provisioned.
            // Env var generation (below) always fires when companion paths
            // resolve. The should_set_* guards only gate input_defaults
            // template-string injection so re-runs don't skip companion
            // resolution once the template ref is already in place.
            {
                let companion_ffmpeg_path =
                    companion_ffmpeg_host_command_path.as_deref().and_then(|selector_path| {
                        resolve_managed_tool_payload_directory_from_selector(
                            paths,
                            &desired_tool_id,
                            selector_path,
                        )
                    });
                if let Some(ffmpeg_path) = companion_ffmpeg_path {
                    yt_dlp_resolved_ffmpeg_path = Some(ffmpeg_path);
                    if should_set_yt_dlp_ffmpeg_location(&desired_config.input_defaults) {
                        desired_config.input_defaults.insert(
                            "ffmpeg_location".to_string(),
                            InputBinding::String(
                                "${env.MEDIAPM_YT_DLP_FFMPEG_LOCATION}".to_string(),
                            ),
                        );
                    }
                }
            }

            {
                let companion_js_path =
                    companion_deno_host_command_path.as_deref().and_then(|selector_path| {
                        resolve_managed_tool_payload_command_path_from_selector(
                            paths,
                            &desired_tool_id,
                            selector_path,
                        )
                    });
                if let Some(js_runtimes_path) = companion_js_path {
                    yt_dlp_resolved_js_runtimes_path = Some(js_runtimes_path);
                    if should_set_yt_dlp_js_runtimes(&desired_config.input_defaults) {
                        desired_config.input_defaults.insert(
                            "js_runtimes".to_string(),
                            InputBinding::String(
                                "deno:${env.MEDIAPM_YT_DLP_JS_RUNTIMES}".to_string(),
                            ),
                        );
                    }
                }
            }
        }
        remove_redundant_inherited_env_vars_from_tool_config(
            &mut desired_config,
            inherited_env_vars,
        );
        if let Some(ref ffmpeg_abs_path) = yt_dlp_resolved_ffmpeg_path {
            generated_runtime_env_vars
                .insert(YT_DLP_FFMPEG_LOCATION_ENV.to_string(), ffmpeg_abs_path.clone());
        }
        if let Some(ref js_runtimes_abs_path) = yt_dlp_resolved_js_runtimes_path {
            generated_runtime_env_vars
                .insert(YT_DLP_JS_RUNTIMES_ENV.to_string(), js_runtimes_abs_path.clone());
        }
        let generated_env_vars = build_tool_env(paths, name)?;
        for (env_key, env_value) in generated_env_vars {
            generated_runtime_env_vars.insert(env_key, env_value);
        }
        if name.eq_ignore_ascii_case("media-tagger")
            && let Some(ffmpeg_path) = media_tagger_ffmpeg_host_command_path
        {
            generated_runtime_env_vars.insert(MEDIA_TAGGER_FFMPEG_BIN_ENV.to_string(), ffmpeg_path);
        }

        if existing_active.as_deref() == Some(desired_tool_id.as_str())
            && machine.tools.contains_key(&desired_tool_id)
        {
            machine.tools.insert(desired_tool_id.clone(), spec);

            machine.tool_configs.insert(desired_tool_id.clone(), desired_config);
            report.unchanged_tool_ids.push(desired_tool_id);
            continue;
        }

        machine.add_tool(
            desired_tool_id.clone(),
            AddToolOptions::new(spec).overwrite_existing(true).with_tool_config(desired_config),
        )?;

        let registry_multihash = Hash::from_content(desired_tool_id.as_bytes()).to_string();
        lock.tool_registry.insert(
            desired_tool_id.clone(),
            ToolRegistryRecord {
                name: name.clone(),
                version: desired_version,
                source: provisioned.source_label.clone(),
                registry_multihash,
                last_transition_unix_seconds: now_unix_seconds(),
                status: ToolRegistryStatus::Active,
            },
        );
        lock.active_tools.insert(name.clone(), desired_tool_id.clone());

        if let Some(old_tool_id) = existing_active {
            report.updated_tool_ids.push(desired_tool_id);
            report.replaced_tool_ids.push(old_tool_id);
        } else {
            report.added_tool_ids.push(desired_tool_id);
        }
    }

    prune_unmanaged_tool_artifacts(
        paths,
        document,
        &cas,
        &mut machine,
        lock,
        &desired_tool_ids,
        &mut report,
    )
    .await?;

    write_generated_runtime_env_file(paths, &machine, &generated_runtime_env_vars)?;
    ensure_machine_runtime_inherits_generated_env_vars(&mut machine, &generated_runtime_env_vars);

    save_machine_document(&paths.conductor_machine_ncl, &machine)?;
    Ok(report)
}

/// Prunes one tool binary and optionally removes all associated metadata.
///
/// When `remove_metadata` is `false` only `tool_configs.<tool_id>` is removed
/// and the lock registry entry is marked `Pruned`, preserving historical state.
///
/// When `remove_metadata` is `true` the tool spec entry in `machine.tools`,
/// all `tool_configs` entries, and the lock registry entry are fully erased so
/// no trace of the tool remains in conductor machine state.
pub(crate) async fn prune_tool_binary(
    paths: &MediaPmPaths,
    lock: &mut MediaPmState,
    tool_id: &str,
    remove_metadata: bool,
) -> Result<usize, MediaPmError> {
    let mut machine = load_machine_document(&paths.conductor_machine_ncl)?;
    let removed_hashes = machine
        .tool_configs
        .remove(tool_id)
        .and_then(|config| config.content_map)
        .map(|map| map.into_values().collect::<Vec<_>>())
        .unwrap_or_default();

    if removed_hashes.is_empty() && !machine.tools.contains_key(tool_id) {
        return Err(MediaPmError::Workflow(format!("tool '{tool_id}' is not registered")));
    }

    if remove_metadata {
        machine.tools.remove(tool_id);
    }

    save_machine_document(&paths.conductor_machine_ncl, &machine)?;

    let cas_root = resolve_cas_store_path(paths, &machine);
    if !removed_hashes.is_empty() {
        let cas = FileSystemCas::open(&cas_root).await.map_err(|source| {
            MediaPmError::Workflow(format!(
                "opening conductor CAS store '{}' for prune: {source}",
                cas_root.display()
            ))
        })?;

        for hash in &removed_hashes {
            if is_hash_still_referenced_by_tool_configs(&machine, *hash) {
                continue;
            }

            if cas.exists(*hash).await.unwrap_or(false) {
                let _ = cas.delete(*hash).await;
            }
        }
    }

    if remove_metadata {
        lock.tool_registry.remove(tool_id);
    } else if let Some(entry) = lock.tool_registry.get_mut(tool_id) {
        entry.status = ToolRegistryStatus::Pruned;
        entry.last_transition_unix_seconds = now_unix_seconds();
    }

    let remove_keys = lock
        .active_tools
        .iter()
        .filter_map(|(name, active)| if active == tool_id { Some(name.clone()) } else { None })
        .collect::<Vec<_>>();
    for key in remove_keys {
        lock.active_tools.remove(&key);
    }

    Ok(removed_hashes.len())
}

#[cfg(test)]
mod tests {
    use std::collections::{BTreeMap, BTreeSet};

    use mediapm_cas::{FileSystemCas, Hash};
    use mediapm_conductor::{
        InputBinding, MachineNickelDocument, ToolConfigSpec, ToolKindSpec, ToolSpec, WorkflowSpec,
        WorkflowStepSpec,
    };

    use crate::config::{MediaPmDocument, ToolRequirement};
    use crate::config::{MediaPmState, ToolRegistryRecord, ToolRegistryStatus};
    use crate::paths::MediaPmPaths;
    use crate::tools::catalog::{
        DownloadPayloadMode, PlatformValue, ToolCatalogEntry, ToolDownloadDescriptor,
        current_tool_os, tool_catalog_entry,
    };
    use crate::tools::downloader::{
        ContentMapSource, DownloadProgressSnapshot, ProvisionedToolPayload,
    };

    use crate::conductor_bridge::ToolSyncReport;
    use crate::conductor_bridge::sync::content_import::{
        ContentMapSourceCacheKey, import_tool_content_source_into_cas,
    };
    use crate::conductor_bridge::sync::lifecycle::{
        ensure_internal_launcher_content_entries_exist, is_builtin_source_ingest_requirement,
        is_hash_still_referenced_by_tool_configs, lock_registry_version,
        prune_unmanaged_tool_artifacts, should_skip_tag_update_check,
    };
    use crate::conductor_bridge::sync::provision::{
        TOOL_PROGRESS_BAR_SCALE, ToolDownloadProgressState, format_overall_tool_download_message,
        format_tool_download_completion_message, format_tool_download_message,
        normalize_download_progress_snapshot, tool_progress_position,
    };
    use crate::conductor_bridge::sync::tool_config::{
        augment_tool_id_with_dependency_selector, ffmpeg_selector_from_registry_or_tool_id,
        media_tagger_ffmpeg_content_key, prefix_same_step_companion_content_entries,
        prefix_same_step_companion_content_key, prefix_same_step_companion_content_map,
        remove_redundant_inherited_env_vars_from_tool_config, resolve_companion_deno_selection,
        resolve_companion_ffmpeg_selection, resolve_host_command_selector_path,
        resolve_managed_tool_command_absolute_path,
        resolve_managed_tool_payload_command_path_from_selector,
        resolve_managed_tool_payload_directory_from_selector, resolve_yt_dlp_js_runtime_path,
        should_set_yt_dlp_ffmpeg_location,
    };
    use crate::conductor_bridge::tool_runtime::{
        MEDIA_TAGGER_LAUNCHER_MEDIAPM_BIN_LINUX_ENV, MEDIA_TAGGER_LAUNCHER_MEDIAPM_BIN_MACOS_ENV,
        MEDIA_TAGGER_LAUNCHER_MEDIAPM_BIN_WINDOWS_ENV,
    };
    fn catalog_entry_fixture(download: ToolDownloadDescriptor) -> ToolCatalogEntry {
        ToolCatalogEntry {
            name: "fixture",
            description: "fixture",
            registry_track: "latest",
            source_label: PlatformValue { windows: "fixture", linux: "fixture", macos: "fixture" },
            source_identifier: PlatformValue {
                windows: "fixture",
                linux: "fixture",
                macos: "fixture",
            },
            executable_name: PlatformValue {
                windows: "fixture.exe",
                linux: "fixture",
                macos: "fixture",
            },
            download,
            additional_download_sources: &[],
        }
    }

    fn provisioned_fixture(
        identity: crate::tools::downloader::ResolvedToolIdentity,
    ) -> ProvisionedToolPayload {
        ProvisionedToolPayload {
            tool_id: "mediapm.tools.fixture+fixture@latest".to_string(),
            command_selector: "fixture".to_string(),
            content_entries: BTreeMap::new(),
            identity,
            source_label: "fixture".to_string(),
            source_identifier: "fixture".to_string(),
            catalog: catalog_entry_fixture(ToolDownloadDescriptor::StaticUrls {
                modes: PlatformValue {
                    windows: DownloadPayloadMode::DirectBinary,
                    linux: DownloadPayloadMode::DirectBinary,
                    macos: DownloadPayloadMode::DirectBinary,
                },
                urls: PlatformValue {
                    windows: &["https://example.invalid/windows"],
                    linux: &["https://example.invalid/linux"],
                    macos: &["https://example.invalid/macos"],
                },
                release_repo: None,
            }),
            warnings: Vec::new(),
        }
    }

    /// Protects percentage scaling so per-tool bars map byte snapshots to the
    /// fixed shared progress range used by `MultiProgress` rows.
    #[test]
    fn tool_progress_position_scales_known_totals() {
        let snapshot = DownloadProgressSnapshot { downloaded_bytes: 50, total_bytes: Some(200) };

        assert_eq!(tool_progress_position(snapshot), TOOL_PROGRESS_BAR_SCALE / 4);
    }

    /// Keeps companion ffmpeg auto-wiring active when yt-dlp defaults omit
    /// `ffmpeg_location` entirely.
    #[test]
    fn should_set_yt_dlp_ffmpeg_location_when_missing() {
        let input_defaults = BTreeMap::new();
        assert!(should_set_yt_dlp_ffmpeg_location(&input_defaults));
    }

    /// Preserves explicit non-fallback ffmpeg paths instead of overwriting
    /// user-provided yt-dlp defaults.
    #[test]
    fn should_not_set_yt_dlp_ffmpeg_location_for_explicit_value() {
        let input_defaults = BTreeMap::from([(
            "ffmpeg_location".to_string(),
            InputBinding::String("/custom/ffmpeg/bin".to_string()),
        )]);
        assert!(!should_set_yt_dlp_ffmpeg_location(&input_defaults));
    }

    /// Protects message contract by preserving compact known-size transfer
    /// text during active downloads.
    #[test]
    fn format_tool_download_message_reports_known_totals() {
        let message = format_tool_download_message(
            "ffmpeg",
            DownloadProgressSnapshot { downloaded_bytes: 1_024, total_bytes: Some(2_048) },
        );

        assert!(message.contains("ffmpeg:"));
        assert!(message.contains("1.0 KiB / 2.0 KiB — downloading"));
    }

    /// Protects unknown-size transfer messaging so rows stay compact and avoid
    /// redundant wording.
    #[test]
    fn format_tool_download_message_handles_unknown_totals() {
        let message = format_tool_download_message(
            "yt-dlp",
            DownloadProgressSnapshot { downloaded_bytes: 512, total_bytes: None },
        );

        assert_eq!(message, "yt-dlp: 512 B — downloading");
    }

    /// Protects transfer rendering from zero-size `Content-Length` headers by
    /// treating them as unknown totals instead of forcing `0 B / 0 B` labels.
    #[test]
    fn normalize_download_progress_snapshot_treats_zero_total_as_unknown() {
        let normalized = normalize_download_progress_snapshot(DownloadProgressSnapshot {
            downloaded_bytes: 16 * 1024,
            total_bytes: Some(0),
        });

        assert_eq!(normalized.downloaded_bytes, 16 * 1024);
        assert_eq!(normalized.total_bytes, None);
    }

    /// Protects aggregate status labels so active downloads report compact
    /// completed/total counts.
    #[test]
    fn format_overall_tool_download_message_reports_known_totals() {
        let states = BTreeMap::from([
            (
                "ffmpeg".to_string(),
                ToolDownloadProgressState {
                    last_snapshot: Some(DownloadProgressSnapshot {
                        downloaded_bytes: 1_024,
                        total_bytes: Some(2_048),
                    }),
                    completed: true,
                },
            ),
            (
                "yt-dlp".to_string(),
                ToolDownloadProgressState {
                    last_snapshot: Some(DownloadProgressSnapshot {
                        downloaded_bytes: 512,
                        total_bytes: Some(1_024),
                    }),
                    completed: false,
                },
            ),
        ]);

        let message = format_overall_tool_download_message(2, &states);
        assert_eq!(message, "tool downloads: 1/2 — downloading",);
    }

    /// Protects completion-row labels so successful tools collapse to one
    /// downloaded-size value with stable status text.
    #[test]
    fn format_tool_download_completion_message_appends_status() {
        let message = format_tool_download_completion_message(
            "media-tagger",
            DownloadProgressSnapshot { downloaded_bytes: 2_048, total_bytes: Some(4_096) },
            "ready",
        );

        assert_eq!(message, "media-tagger: 2.0 KiB — ready");
    }

    /// Protects aggregate pre-download labels so the top row stays minimal
    /// while workers are still resolving releases.
    #[test]
    fn format_overall_tool_download_message_reports_resolving_phase() {
        let states = BTreeMap::from([
            ("ffmpeg".to_string(), ToolDownloadProgressState::default()),
            ("yt-dlp".to_string(), ToolDownloadProgressState::default()),
        ]);

        let message = format_overall_tool_download_message(2, &states);
        assert_eq!(message, "tool downloads: resolving");
    }

    /// Protects aggregate completion labels so ready state reports only the
    /// completed tool count and terminal status.
    #[test]
    fn format_overall_tool_download_message_reports_ready_phase() {
        let states = BTreeMap::from([
            (
                "ffmpeg".to_string(),
                ToolDownloadProgressState {
                    last_snapshot: Some(DownloadProgressSnapshot {
                        downloaded_bytes: 1_024,
                        total_bytes: Some(1_024),
                    }),
                    completed: true,
                },
            ),
            (
                "yt-dlp".to_string(),
                ToolDownloadProgressState {
                    last_snapshot: Some(DownloadProgressSnapshot {
                        downloaded_bytes: 2_048,
                        total_bytes: None,
                    }),
                    completed: true,
                },
            ),
        ]);

        let message = format_overall_tool_download_message(2, &states);
        assert_eq!(message, "tool downloads: 2 — ready");
    }

    /// Verifies lock registry version uses immutable identity precedence and
    /// fails when all identity selectors are absent.
    #[test]
    fn lock_registry_version_uses_identity_precedence() {
        let with_hash = provisioned_fixture(crate::tools::downloader::ResolvedToolIdentity {
            git_hash: Some("abc123".to_string()),
            version: Some("1.2.3".to_string()),
            tag: Some("v1.2.3".to_string()),
            release_description: None,
        });
        assert_eq!(lock_registry_version(&with_hash).expect("hash wins"), "abc123");

        let with_version = provisioned_fixture(crate::tools::downloader::ResolvedToolIdentity {
            git_hash: None,
            version: Some("1.2.3".to_string()),
            tag: Some("v1.2.3".to_string()),
            release_description: None,
        });
        assert_eq!(lock_registry_version(&with_version).expect("version wins"), "1.2.3");

        let with_tag = provisioned_fixture(crate::tools::downloader::ResolvedToolIdentity {
            git_hash: None,
            version: None,
            tag: Some("v1.2.3".to_string()),
            release_description: None,
        });
        assert_eq!(lock_registry_version(&with_tag).expect("tag wins"), "v1.2.3");

        let missing = provisioned_fixture(crate::tools::downloader::ResolvedToolIdentity {
            git_hash: None,
            version: None,
            tag: None,
            release_description: None,
        });
        assert!(lock_registry_version(&missing).is_err());
    }

    /// Verifies reconciliation drops redundant inherited env-vars from
    /// generated tool config rows while preserving tool-specific entries.
    #[test]
    fn inherited_env_vars_are_not_duplicated_into_tool_config_env_vars() {
        let mut config = mediapm_conductor::ToolConfigSpec {
            env_vars: BTreeMap::from([
                ("SYSTEMROOT".to_string(), "C:/Windows".to_string()),
                ("Temp".to_string(), "C:/Temp".to_string()),
                (
                    MEDIA_TAGGER_LAUNCHER_MEDIAPM_BIN_WINDOWS_ENV.to_string(),
                    "C:/tools/mediapm.exe".to_string(),
                ),
                ("CUSTOM_TOOL_FLAG".to_string(), "enabled".to_string()),
            ]),
            ..mediapm_conductor::ToolConfigSpec::default()
        };

        remove_redundant_inherited_env_vars_from_tool_config(
            &mut config,
            &["systemroot".to_string(), "TEMP".to_string()],
        );

        assert!(!config.env_vars.contains_key("SYSTEMROOT"));
        assert!(!config.env_vars.contains_key("Temp"));
        assert!(config.env_vars.contains_key(MEDIA_TAGGER_LAUNCHER_MEDIAPM_BIN_WINDOWS_ENV));
        assert_eq!(config.env_vars.get("CUSTOM_TOOL_FLAG").map(String::as_str), Some("enabled"));
    }

    /// Verifies internal launchers do not use tag-only skip mode so stale
    /// launcher content maps can be refreshed on sync.
    #[test]
    fn should_not_skip_tag_updates_for_internal_launcher() {
        let requirement = ToolRequirement {
            version: None,
            tag: Some("latest".to_string()),
            dependencies: crate::config::ToolRequirementDependencies::default(),
            recheck_seconds: None,
            max_input_slots: None,
            max_output_slots: None,
        };

        let lock = MediaPmState {
            active_tools: BTreeMap::from([(
                "media-tagger".to_string(),
                "mediapm.tools.media-tagger+mediapm-internal@latest".to_string(),
            )]),
            ..MediaPmState::default()
        };

        let machine = MachineNickelDocument {
            tools: BTreeMap::from([(
                "mediapm.tools.media-tagger+mediapm-internal@latest".to_string(),
                mediapm_conductor::ToolSpec::default(),
            )]),
            ..MachineNickelDocument::default()
        };

        assert!(!should_skip_tag_update_check(
            &requirement,
            "media-tagger",
            &lock,
            &machine,
            false,
        ));
    }

    /// Verifies yt-dlp never uses tag-only skip mode so same-step companion
    /// dependency wiring (ffmpeg + deno) always runs during reconciliation.
    #[test]
    fn should_not_skip_tag_updates_for_yt_dlp_same_step_companions() {
        let requirement = ToolRequirement {
            version: None,
            tag: Some("latest".to_string()),
            dependencies: crate::config::ToolRequirementDependencies::default(),
            recheck_seconds: None,
            max_input_slots: None,
            max_output_slots: None,
        };

        let active_tool_id = "mediapm.tools.yt-dlp+github-releases-yt-dlp@latest".to_string();
        let lock = MediaPmState {
            active_tools: BTreeMap::from([("yt-dlp".to_string(), active_tool_id.clone())]),
            ..MediaPmState::default()
        };

        let machine = MachineNickelDocument {
            tools: BTreeMap::from([(
                active_tool_id.clone(),
                ToolSpec {
                    kind: ToolKindSpec::Executable {
                        command: vec!["linux/yt-dlp".to_string()],
                        env_vars: BTreeMap::new(),
                        success_codes: vec![0],
                    },
                    ..ToolSpec::default()
                },
            )]),
            tool_configs: BTreeMap::from([(
                active_tool_id,
                ToolConfigSpec {
                    content_map: Some(BTreeMap::from([(
                        "linux/yt-dlp".to_string(),
                        Hash::from_content(b"yt-dlp"),
                    )])),
                    ..ToolConfigSpec::default()
                },
            )]),
            ..MachineNickelDocument::default()
        };

        assert!(!should_skip_tag_update_check(&requirement, "yt-dlp", &lock, &machine, false,));
    }

    /// Verifies managed yt-dlp ffmpeg defaults use the concrete companion path
    /// rather than an env placeholder in machine config.
    #[test]
    fn yt_dlp_ffmpeg_default_uses_concrete_companion_path() {
        let root = tempfile::tempdir().expect("tempdir");
        let paths = MediaPmPaths::from_root(root.path());
        let resolved = resolve_managed_tool_payload_directory_from_selector(
            &paths,
            "mediapm.tools.yt-dlp+github-releases-yt-dlp@latest",
            "mediapm.tools.ffmpeg+github-releases-btbn-ffmpeg-builds@latest",
        )
        .expect("resolve companion directory");

        assert!(std::path::Path::new(&resolved).is_absolute());
        assert!(!resolved.contains("${env."));
        let binding = InputBinding::String(resolved.clone());
        match binding {
            InputBinding::String(value) => {
                assert_eq!(value, resolved);
            }
            InputBinding::StringList(_) => unreachable!("expected scalar string binding"),
        }
    }

    /// Verifies managed yt-dlp JS runtime defaults use the concrete companion
    /// command path rather than an env placeholder in machine config.
    #[test]
    fn yt_dlp_js_runtimes_default_uses_concrete_companion_path() {
        let root = tempfile::tempdir().expect("tempdir");
        let paths = MediaPmPaths::from_root(root.path());
        let resolved = resolve_managed_tool_payload_command_path_from_selector(
            &paths,
            "mediapm.tools.yt-dlp+github-releases-yt-dlp@latest",
            "mediapm.tools.deno+github-releases-denoland-deno@latest",
        )
        .expect("resolve companion command path");

        assert!(std::path::Path::new(&resolved).is_absolute());
        assert!(!resolved.contains("${env."));
        let binding = InputBinding::String(format!("deno:{resolved}"));
        match binding {
            InputBinding::String(value) => {
                assert!(value.starts_with("deno:"));
                assert!(value.ends_with(&resolved));
            }
            InputBinding::StringList(_) => unreachable!("expected scalar string binding"),
        }
    }

    #[test]
    fn should_not_skip_tag_updates_when_platform_selector_content_is_incomplete() {
        let requirement = ToolRequirement {
            version: None,
            tag: Some("latest".to_string()),
            dependencies: crate::config::ToolRequirementDependencies::default(),
            recheck_seconds: None,
            max_input_slots: None,
            max_output_slots: None,
        };
        let active_tool_id =
            "mediapm.tools.ffmpeg+github-releases-btbn-ffmpeg-builds@latest".to_string();
        let command_selector = "${context.os == \"windows\" ? windows/ffmpeg.exe | ''}${context.os == \"linux\" ? linux/ffmpeg | ''}${context.os == \"macos\" ? macos/ffmpeg | ''}".to_string();

        let lock = MediaPmState {
            active_tools: BTreeMap::from([("ffmpeg".to_string(), active_tool_id.clone())]),
            ..MediaPmState::default()
        };

        let machine = MachineNickelDocument {
            tools: BTreeMap::from([(
                active_tool_id.clone(),
                ToolSpec {
                    kind: ToolKindSpec::Executable {
                        command: vec![command_selector],
                        env_vars: BTreeMap::new(),
                        success_codes: vec![0],
                    },
                    ..ToolSpec::default()
                },
            )]),
            tool_configs: BTreeMap::from([(
                active_tool_id,
                ToolConfigSpec {
                    content_map: Some(BTreeMap::from([(
                        "windows/ffmpeg.exe".to_string(),
                        Hash::from_content(b"windows"),
                    )])),
                    ..ToolConfigSpec::default()
                },
            )]),
            ..MachineNickelDocument::default()
        };

        assert!(!should_skip_tag_update_check(&requirement, "ffmpeg", &lock, &machine, false,));
    }

    /// Verifies tag-only skip mode remains enabled when active executable
    /// content maps include every platform selector branch target.
    #[test]
    fn should_skip_tag_updates_when_platform_selector_content_is_complete() {
        let requirement = ToolRequirement {
            version: None,
            tag: Some("latest".to_string()),
            dependencies: crate::config::ToolRequirementDependencies::default(),
            recheck_seconds: None,
            max_input_slots: None,
            max_output_slots: None,
        };
        let active_tool_id =
            "mediapm.tools.ffmpeg+github-releases-btbn-ffmpeg-builds@latest".to_string();
        let command_selector = "${context.os == \"windows\" ? windows/ffmpeg.exe | ''}${context.os == \"linux\" ? linux/ffmpeg | ''}${context.os == \"macos\" ? macos/ffmpeg | ''}".to_string();

        let lock = MediaPmState {
            active_tools: BTreeMap::from([("ffmpeg".to_string(), active_tool_id.clone())]),
            ..MediaPmState::default()
        };

        let machine = MachineNickelDocument {
            tools: BTreeMap::from([(
                active_tool_id.clone(),
                ToolSpec {
                    kind: ToolKindSpec::Executable {
                        command: vec![command_selector],
                        env_vars: BTreeMap::new(),
                        success_codes: vec![0],
                    },
                    ..ToolSpec::default()
                },
            )]),
            tool_configs: BTreeMap::from([(
                active_tool_id,
                ToolConfigSpec {
                    content_map: Some(BTreeMap::from([
                        ("windows/ffmpeg.exe".to_string(), Hash::from_content(b"windows")),
                        ("linux/ffmpeg".to_string(), Hash::from_content(b"linux")),
                        ("macos/ffmpeg".to_string(), Hash::from_content(b"macos")),
                    ])),
                    ..ToolConfigSpec::default()
                },
            )]),
            ..MachineNickelDocument::default()
        };

        assert!(should_skip_tag_update_check(&requirement, "ffmpeg", &lock, &machine, false,));
    }

    /// Verifies host-specific managed executable path resolution from
    /// platform-conditional command selector templates.
    #[test]
    fn resolve_host_command_selector_path_prefers_host_selector_branch() {
        let selector = "${context.os == \"windows\" ? windows/tool.exe | ''}${context.os == \"linux\" ? linux/tool | ''}${context.os == \"macos\" ? macos/tool | ''}";
        let resolved = resolve_host_command_selector_path(selector).expect("path");
        let expected = if cfg!(windows) {
            "windows/tool.exe"
        } else if cfg!(target_os = "macos") {
            "macos/tool"
        } else {
            "linux/tool"
        };

        assert_eq!(resolved, expected);
    }

    /// Verifies command selector resolution returns direct path values when
    /// selector is already host-specific text.
    #[test]
    fn resolve_host_command_selector_path_accepts_direct_path() {
        let resolved = resolve_host_command_selector_path("windows/ffmpeg-master/bin/ffmpeg.exe")
            .expect("direct path");

        assert_eq!(resolved, "windows/ffmpeg-master/bin/ffmpeg.exe");
    }

    /// Verifies same-step companion dependencies fold selected selector identity
    /// into the dependent managed tool id.
    #[test]
    fn generic_dependency_selector_fragment_is_folded_into_tool_id() {
        let base_tool_id = "mediapm.tools.yt-dlp+github-releases-yt-dlp@latest";
        let augmented = augment_tool_id_with_dependency_selector(base_tool_id, "ffmpeg", "v7.1");

        assert_eq!(augmented, "mediapm.tools.yt-dlp+github-releases-yt-dlp+ffmpeg-v7-1@latest");
    }

    /// Verifies same-step companion content keys are always namespaced with the
    /// selected companion tool id.
    #[test]
    fn same_step_companion_content_keys_are_prefixed_with_tool_id() {
        let key = prefix_same_step_companion_content_key(
            "mediapm.tools.deno+github-releases-denoland-deno@v2.5.0",
            "linux/deno",
        );
        assert_eq!(key, "mediapm.tools.deno+github-releases-denoland-deno@v2.5.0/linux/deno");
    }

    /// Verifies same-step companion content-map re-keying preserves hash values
    /// while namespacing every entry under the companion tool id.
    #[test]
    fn same_step_companion_content_map_is_namespaced() {
        let content_map = BTreeMap::from([
            ("./".to_string(), Hash::from_content(b"bundle")),
            ("windows/deno.exe".to_string(), Hash::from_content(b"deno-exe")),
        ]);

        let prefixed = prefix_same_step_companion_content_map(
            "mediapm.tools.deno+github-releases-denoland-deno@v2.5.0",
            &content_map,
        );

        assert!(prefixed.contains_key("mediapm.tools.deno+github-releases-denoland-deno@v2.5.0/"));
        assert!(prefixed.contains_key(
            "mediapm.tools.deno+github-releases-denoland-deno@v2.5.0/windows/deno.exe"
        ));
    }

    /// Verifies same-step companion provisioned content entries are namespaced
    /// identically to persisted content-map entries.
    #[test]
    fn same_step_companion_content_entries_are_namespaced() {
        let temp = tempfile::tempdir().expect("tempdir");
        let payload = temp.path().join("deno");
        std::fs::write(&payload, b"deno").expect("write payload");

        let entries =
            BTreeMap::from([("linux/deno".to_string(), ContentMapSource::FilePath(payload))]);
        let prefixed = prefix_same_step_companion_content_entries(
            "mediapm.tools.deno+github-releases-denoland-deno@v2.5.0",
            &entries,
        );

        assert!(
            prefixed
                .contains_key("mediapm.tools.deno+github-releases-denoland-deno@v2.5.0/linux/deno")
        );
    }

    /// Verifies ffmpeg selector derivation prefers lock registry versions and
    /// falls back to immutable tool-id suffixes when registry rows are absent.
    #[test]
    fn ffmpeg_selector_resolution_uses_registry_then_tool_id_suffix() {
        let mut lock = MediaPmState::default();
        lock.tool_registry.insert(
            "mediapm.tools.ffmpeg+github-releases-btbn-ffmpeg-builds@v7.1".to_string(),
            ToolRegistryRecord {
                name: "ffmpeg".to_string(),
                version: "v7.1".to_string(),
                source: "GitHub BTBN".to_string(),
                registry_multihash: "blake3:fixture".to_string(),
                last_transition_unix_seconds: 0,
                status: ToolRegistryStatus::Active,
            },
        );

        let from_registry = ffmpeg_selector_from_registry_or_tool_id(
            "mediapm.tools.ffmpeg+github-releases-btbn-ffmpeg-builds@v7.1",
            &lock,
        );
        assert_eq!(from_registry.as_deref(), Some("v7.1"));

        let from_suffix = ffmpeg_selector_from_registry_or_tool_id(
            "mediapm.tools.ffmpeg+github-releases-btbn-ffmpeg-builds@blake3-abcdef1234",
            &MediaPmState::default(),
        );
        assert_eq!(from_suffix.as_deref(), Some("blake3-abcdef1234"));
    }

    /// Verifies media-tagger ffmpeg content entries are mounted under a stable
    /// namespaced prefix to avoid collisions with launcher paths.
    #[test]
    fn media_tagger_ffmpeg_content_keys_are_namespaced() {
        assert_eq!(
            media_tagger_ffmpeg_content_key("windows/ffmpeg/bin/ffmpeg.exe"),
            "ffmpeg/windows/ffmpeg/bin/ffmpeg.exe"
        );
        assert_eq!(media_tagger_ffmpeg_content_key("ffmpeg/linux/ffmpeg"), "ffmpeg/linux/ffmpeg");
    }

    /// Verifies media-tagger ffmpeg env path resolves payload-layout binary paths
    /// for installed managed tools.
    #[test]
    fn resolve_managed_tool_command_absolute_path_prefers_payload_layout() {
        let temp = tempfile::tempdir().expect("tempdir");
        let paths = MediaPmPaths::from_root(temp.path());
        let tool_id = "mediapm.tools.ffmpeg+github-releases-btbn-ffmpeg-builds@v7.1";
        let relative = "windows/bin/ffmpeg.exe";

        let absolute = paths.tools_dir.join(tool_id).join("payload").join(relative);
        std::fs::create_dir_all(absolute.parent().expect("parent dir")).expect("mkdirs");
        std::fs::write(&absolute, b"ffmpeg").expect("write fake ffmpeg binary");

        let resolved = resolve_managed_tool_command_absolute_path(&paths, Some(tool_id), relative)
            .expect("absolute path");

        assert_eq!(resolved, absolute.to_string_lossy().replace('\\', "/"));
    }

    /// Verifies media-tagger namespaced ffmpeg selectors resolve to the same
    /// payload-layout managed-tool binary path.
    #[test]
    fn resolve_managed_tool_command_absolute_path_accepts_media_tagger_namespaced_paths() {
        let temp = tempfile::tempdir().expect("tempdir");
        let paths = MediaPmPaths::from_root(temp.path());
        let tool_id = "mediapm.tools.ffmpeg+github-releases-btbn-ffmpeg-builds@v7.1";
        let installed_relative = "windows/bin/ffmpeg.exe";
        let namespaced_relative = "ffmpeg/windows/bin/ffmpeg.exe";

        let absolute = paths.tools_dir.join(tool_id).join("payload").join(installed_relative);
        std::fs::create_dir_all(absolute.parent().expect("parent dir")).expect("mkdirs");
        std::fs::write(&absolute, b"ffmpeg").expect("write fake ffmpeg binary");

        let resolved =
            resolve_managed_tool_command_absolute_path(&paths, Some(tool_id), namespaced_relative)
                .expect("absolute path");

        assert_eq!(resolved, absolute.to_string_lossy().replace('\\', "/"));
    }

    /// Verifies missing internal media-tagger launcher files are
    /// deterministically regenerated before CAS import.
    #[test]
    fn internal_media_tagger_launcher_entries_are_regenerated_when_missing() {
        let temp = tempfile::tempdir().expect("tempdir");
        let install_root = temp.path().join("mediapm.tools.media-tagger+mediapm-internal@0.0.0");
        let windows_path = install_root.join("windows").join("media-tagger.cmd");
        let linux_path = install_root.join("linux").join("media-tagger");
        let macos_path = install_root.join("macos").join("media-tagger");

        let content_entries = BTreeMap::from([
            (
                "windows/media-tagger.cmd".to_string(),
                ContentMapSource::FilePath(windows_path.clone()),
            ),
            ("linux/media-tagger".to_string(), ContentMapSource::FilePath(linux_path.clone())),
            ("macos/media-tagger".to_string(), ContentMapSource::FilePath(macos_path.clone())),
        ]);

        let provisioned = ProvisionedToolPayload {
            tool_id: "mediapm.tools.media-tagger+mediapm-internal@0.0.0".to_string(),
            command_selector: "windows/media-tagger.cmd".to_string(),
            content_entries: content_entries.clone(),
            identity: crate::tools::downloader::ResolvedToolIdentity::default(),
            source_label: "mediapm internal launcher".to_string(),
            source_identifier: "mediapm-internal".to_string(),
            catalog: tool_catalog_entry("media-tagger").expect("catalog entry"),
            warnings: Vec::new(),
        };

        ensure_internal_launcher_content_entries_exist(&provisioned, &content_entries)
            .expect("regenerate missing launcher files");

        assert!(windows_path.is_file());
        assert!(linux_path.is_file());
        assert!(macos_path.is_file());

        let windows_script =
            std::fs::read_to_string(&windows_path).expect("read regenerated windows launcher");
        assert!(windows_script.contains(MEDIA_TAGGER_LAUNCHER_MEDIAPM_BIN_WINDOWS_ENV));

        let linux_script =
            std::fs::read_to_string(&linux_path).expect("read regenerated linux launcher");
        assert!(linux_script.contains(MEDIA_TAGGER_LAUNCHER_MEDIAPM_BIN_LINUX_ENV));

        let macos_script =
            std::fs::read_to_string(&macos_path).expect("read regenerated macos launcher");
        assert!(macos_script.contains(MEDIA_TAGGER_LAUNCHER_MEDIAPM_BIN_MACOS_ENV));
    }

    /// Verifies per-pass content-source caching reuses file-path imports.
    #[test]
    fn import_tool_content_source_into_cas_reuses_cached_file_path_hash() {
        let temp = tempfile::tempdir().expect("tempdir");
        let cas_root = temp.path().join("cas");
        let payload_path = temp.path().join("payload.bin");
        std::fs::write(&payload_path, b"fixture-payload").expect("write payload file");

        let runtime = tokio::runtime::Builder::new_current_thread()
            .enable_all()
            .build()
            .expect("build runtime");

        runtime.block_on(async {
            let cas = FileSystemCas::open(&cas_root).await.expect("open cas");
            let source = ContentMapSource::FilePath(payload_path.clone());
            let mut cache = BTreeMap::<ContentMapSourceCacheKey, Hash>::new();

            let first =
                import_tool_content_source_into_cas(&cas, "windows/tool.exe", &source, &mut cache)
                    .await
                    .expect("first import");

            std::fs::remove_file(&payload_path).expect("remove source payload file");

            let second = import_tool_content_source_into_cas(
                &cas,
                "windows/tool-copy.exe",
                &source,
                &mut cache,
            )
            .await
            .expect("cached import");

            assert_eq!(first, second);
        });
    }

    /// Verifies per-pass content-source caching reuses directory-ZIP imports.
    #[test]
    fn import_tool_content_source_into_cas_reuses_cached_directory_zip_hash() {
        let temp = tempfile::tempdir().expect("tempdir");
        let cas_root = temp.path().join("cas");
        let directory_root = temp.path().join("tool-dir");
        std::fs::create_dir_all(&directory_root).expect("create tool directory");
        std::fs::write(directory_root.join("tool.txt"), b"tool-bytes")
            .expect("write directory payload");

        let runtime = tokio::runtime::Builder::new_current_thread()
            .enable_all()
            .build()
            .expect("build runtime");

        runtime.block_on(async {
            let cas = FileSystemCas::open(&cas_root).await.expect("open cas");
            let source = ContentMapSource::DirectoryZip { root_dir: directory_root.clone() };
            let mut cache = BTreeMap::<ContentMapSourceCacheKey, Hash>::new();

            let first = import_tool_content_source_into_cas(&cas, "windows/", &source, &mut cache)
                .await
                .expect("first directory import");

            std::fs::remove_dir_all(&directory_root).expect("remove source directory");

            let second =
                import_tool_content_source_into_cas(&cas, "windows-copy/", &source, &mut cache)
                    .await
                    .expect("cached directory import");

            assert_eq!(first, second);
        });
    }

    /// Verifies companion ffmpeg selector resolution for yt-dlp can pin to an
    /// already-registered managed ffmpeg tool without requiring reprovision.
    #[test]
    fn companion_ffmpeg_selection_matches_registered_ffmpeg_tool() {
        let temp = tempfile::tempdir().expect("tempdir");
        let paths = MediaPmPaths::from_root(temp.path());
        let requirement = ToolRequirement {
            version: None,
            tag: Some("latest".to_string()),
            dependencies: crate::config::ToolRequirementDependencies {
                ffmpeg_version: Some("v7.1".to_string()),
                deno_version: None,
                sd_version: None,
            },
            recheck_seconds: None,
            max_input_slots: None,
            max_output_slots: None,
        };

        let mut lock = MediaPmState::default();
        lock.tool_registry.insert(
            "mediapm.tools.ffmpeg+github-releases-btbn-ffmpeg-builds@v7.1".to_string(),
            ToolRegistryRecord {
                name: "ffmpeg".to_string(),
                version: "v7.1".to_string(),
                source: "GitHub BTBN".to_string(),
                registry_multihash: "blake3:fixture".to_string(),
                last_transition_unix_seconds: 0,
                status: ToolRegistryStatus::Active,
            },
        );

        let mut machine = MachineNickelDocument::default();
        machine.tools.insert(
            "mediapm.tools.ffmpeg+github-releases-btbn-ffmpeg-builds@v7.1".to_string(),
            ToolSpec {
                kind: ToolKindSpec::Executable {
                    command: vec!["windows/ffmpeg/bin/ffmpeg.exe".to_string()],
                    env_vars: BTreeMap::new(),
                    success_codes: vec![0],
                },
                ..ToolSpec::default()
            },
        );
        machine.tool_configs.insert(
            "mediapm.tools.ffmpeg+github-releases-btbn-ffmpeg-builds@v7.1".to_string(),
            ToolConfigSpec {
                content_map: Some(BTreeMap::from([(
                    "windows/ffmpeg/bin/ffmpeg.exe".to_string(),
                    Hash::from_content(b"ffmpeg-v7.1"),
                )])),
                ..ToolConfigSpec::default()
            },
        );
        let selection = resolve_companion_ffmpeg_selection(
            &paths,
            "yt-dlp",
            &requirement,
            &BTreeMap::new(),
            &lock,
            &machine,
        )
        .expect("companion selection should succeed");

        assert_eq!(selection.selector, "v7.1");
        assert!(selection.provisioned_content_entries.is_empty());
        assert!(selection.existing_content_map.contains_key("windows/ffmpeg/bin/ffmpeg.exe"));
        assert_eq!(selection.host_command_path.as_deref(), Some("windows/ffmpeg/bin/ffmpeg.exe"));
    }

    /// Verifies companion ffmpeg linkage resolves payload-layout paths for
    /// installed managed tools.
    #[test]
    fn companion_ffmpeg_selection_uses_payload_layout() {
        let temp = tempfile::tempdir().expect("tempdir");
        let paths = MediaPmPaths::from_root(temp.path());
        let tool_id = "mediapm.tools.ffmpeg+github-releases-btbn-ffmpeg-builds@v7.1";
        let host_os = current_tool_os().as_str();
        let ffmpeg_file_name = if cfg!(windows) { "ffmpeg.exe" } else { "ffmpeg" };

        let payload_dir = paths.tools_dir.join(tool_id).join("payload").join(host_os);
        std::fs::create_dir_all(&payload_dir).expect("create tool payload dir");
        std::fs::write(payload_dir.join(ffmpeg_file_name), b"ffmpeg").expect("write ffmpeg");

        let requirement = ToolRequirement {
            version: None,
            tag: Some("latest".to_string()),
            dependencies: crate::config::ToolRequirementDependencies {
                ffmpeg_version: Some("inherit".to_string()),
                deno_version: None,
                sd_version: None,
            },
            recheck_seconds: None,
            max_input_slots: None,
            max_output_slots: None,
        };

        let mut lock = MediaPmState::default();
        lock.active_tools.insert("ffmpeg".to_string(), tool_id.to_string());

        let mut machine = MachineNickelDocument::default();
        machine.tools.insert(
            tool_id.to_string(),
            ToolSpec {
                kind: ToolKindSpec::Executable {
                    command: vec![format!("{host_os}/{ffmpeg_file_name}")],
                    env_vars: BTreeMap::new(),
                    success_codes: vec![0],
                },
                ..ToolSpec::default()
            },
        );
        machine.tool_configs.insert(
            tool_id.to_string(),
            ToolConfigSpec {
                content_map: Some(BTreeMap::from([(
                    format!("{host_os}/{ffmpeg_file_name}"),
                    Hash::from_content(b"ffmpeg"),
                )])),
                ..ToolConfigSpec::default()
            },
        );

        let selection = resolve_companion_ffmpeg_selection(
            &paths,
            "yt-dlp",
            &requirement,
            &BTreeMap::new(),
            &lock,
            &machine,
        )
        .expect("companion selection should succeed");

        assert_eq!(selection.selector, "v7.1");
        assert_eq!(selection.existing_content_map.len(), 1);
        assert_eq!(
            selection.existing_content_map.get(&format!("{host_os}/{ffmpeg_file_name}")),
            Some(&Hash::from_content(b"ffmpeg")),
        );
        assert_eq!(
            selection.host_command_path.as_deref(),
            Some(format!("{host_os}/{ffmpeg_file_name}").as_str())
        );
    }

    /// Verifies companion deno selector resolution for yt-dlp can pin to an
    /// already-registered managed deno tool without requiring reprovision.
    #[test]
    fn companion_deno_selection_matches_registered_deno_tool() {
        let requirement = ToolRequirement {
            version: None,
            tag: Some("latest".to_string()),
            dependencies: crate::config::ToolRequirementDependencies {
                ffmpeg_version: None,
                deno_version: Some("v2.5.0".to_string()),
                sd_version: None,
            },
            recheck_seconds: None,
            max_input_slots: None,
            max_output_slots: None,
        };

        let mut lock = MediaPmState::default();
        lock.tool_registry.insert(
            "mediapm.tools.deno+github-releases-denoland-deno@v2.5.0".to_string(),
            ToolRegistryRecord {
                name: "deno".to_string(),
                version: "v2.5.0".to_string(),
                source: "GitHub denoland/deno".to_string(),
                registry_multihash: "blake3:fixture".to_string(),
                last_transition_unix_seconds: 0,
                status: ToolRegistryStatus::Active,
            },
        );

        let mut machine = MachineNickelDocument::default();
        machine.tools.insert(
            "mediapm.tools.deno+github-releases-denoland-deno@v2.5.0".to_string(),
            ToolSpec {
                kind: ToolKindSpec::Executable {
                    command: vec!["windows/deno/deno.exe".to_string()],
                    env_vars: BTreeMap::new(),
                    success_codes: vec![0],
                },
                ..ToolSpec::default()
            },
        );
        machine.tool_configs.insert(
            "mediapm.tools.deno+github-releases-denoland-deno@v2.5.0".to_string(),
            ToolConfigSpec {
                content_map: Some(BTreeMap::from([(
                    "windows/deno/deno.exe".to_string(),
                    Hash::from_content(b"deno-v2.5.0"),
                )])),
                ..ToolConfigSpec::default()
            },
        );

        let selection = resolve_companion_deno_selection(
            "yt-dlp",
            &requirement,
            &BTreeMap::new(),
            &lock,
            &machine,
        )
        .expect("companion deno selection should succeed");

        assert_eq!(selection.selector, "v2.5.0");
        assert!(selection.existing_content_map.contains_key("windows/deno/deno.exe"));
        assert_eq!(selection.host_command_path.as_deref(), Some("windows/deno/deno.exe"));
    }

    /// Verifies same-step companion deno selection tolerates malformed legacy
    /// managed deno rows that have no materialized content map.
    #[test]
    fn companion_deno_selection_ignores_missing_content_map() {
        let requirement = ToolRequirement {
            version: None,
            tag: Some("latest".to_string()),
            dependencies: crate::config::ToolRequirementDependencies {
                ffmpeg_version: None,
                deno_version: Some("v2.5.0".to_string()),
                sd_version: None,
            },
            recheck_seconds: None,
            max_input_slots: None,
            max_output_slots: None,
        };

        let mut lock = MediaPmState::default();
        lock.tool_registry.insert(
            "mediapm.tools.deno+github-releases-denoland-deno@v2.5.0".to_string(),
            ToolRegistryRecord {
                name: "deno".to_string(),
                version: "v2.5.0".to_string(),
                source: "GitHub denoland/deno".to_string(),
                registry_multihash: "blake3:fixture".to_string(),
                last_transition_unix_seconds: 0,
                status: ToolRegistryStatus::Active,
            },
        );

        let mut machine = MachineNickelDocument::default();
        machine.tools.insert(
            "mediapm.tools.deno+github-releases-denoland-deno@v2.5.0".to_string(),
            ToolSpec {
                kind: ToolKindSpec::Executable {
                    command: vec!["windows/deno/deno.exe".to_string()],
                    env_vars: BTreeMap::new(),
                    success_codes: vec![0],
                },
                ..ToolSpec::default()
            },
        );

        let selection = resolve_companion_deno_selection(
            "yt-dlp",
            &requirement,
            &BTreeMap::new(),
            &lock,
            &machine,
        )
        .expect("missing deno content map should be tolerated");

        assert_eq!(selection.selector, "v2.5.0");
        assert!(selection.existing_content_map.is_empty());
        assert!(selection.host_command_path.is_none());
    }

    /// Verifies explicit yt-dlp companion deno selectors fail fast when no
    /// managed deno identity matches the requested selector.
    #[test]
    fn companion_deno_selection_rejects_unknown_selector() {
        let requirement = ToolRequirement {
            version: None,
            tag: Some("latest".to_string()),
            dependencies: crate::config::ToolRequirementDependencies {
                ffmpeg_version: None,
                deno_version: Some("v9.9.9".to_string()),
                sd_version: None,
            },
            recheck_seconds: None,
            max_input_slots: None,
            max_output_slots: None,
        };

        let error = resolve_companion_deno_selection(
            "yt-dlp",
            &requirement,
            &BTreeMap::new(),
            &MediaPmState::default(),
            &MachineNickelDocument::default(),
        )
        .expect_err("unknown deno selector should fail");

        assert!(
        error.to_string().contains(
            "tools.yt-dlp.dependencies.deno_version 'v9.9.9' did not match any managed deno tool"
        ),
        "unexpected error: {error}"
    );
    }

    /// Verifies yt-dlp js runtime resolution resolves payload-layout paths only.
    #[test]
    fn resolve_yt_dlp_js_runtime_path_uses_payload_layout() {
        let temp = tempfile::tempdir().expect("tempdir");
        let paths = MediaPmPaths::from_root(temp.path());
        let tool_id = "mediapm.tools.yt-dlp+github-releases-yt-dlp@latest";
        let host_os = current_tool_os().as_str();
        let runtime_file_name = if cfg!(windows) { "deno.exe" } else { "deno" };

        let runtime_path =
            paths.tools_dir.join(tool_id).join("payload").join(host_os).join(runtime_file_name);
        std::fs::create_dir_all(runtime_path.parent().expect("runtime parent")).expect("mkdir");
        std::fs::write(&runtime_path, b"deno").expect("write runtime");

        let resolved = resolve_yt_dlp_js_runtime_path(&paths, tool_id).expect("resolved path");

        assert_eq!(resolved, runtime_path.to_string_lossy());
    }

    /// Verifies companion-derived ffmpeg directory defaults are emitted as absolute
    /// paths even when workspace roots are relative.
    #[test]
    fn resolve_managed_tool_payload_directory_from_selector_is_absolute_for_relative_workspace_root()
     {
        let paths = MediaPmPaths::from_root(std::path::Path::new("."));
        let resolved = resolve_managed_tool_payload_directory_from_selector(
            &paths,
            "mediapm.tools.yt-dlp+github-releases-yt-dlp@latest",
            "windows/ffmpeg/bin/ffmpeg.exe",
        )
        .expect("resolved path");

        assert!(std::path::Path::new(&resolved).is_absolute());
    }

    /// Verifies companion-derived js runtime defaults are emitted as absolute
    /// paths even when workspace roots are relative.
    #[test]
    fn resolve_managed_tool_payload_command_path_from_selector_is_absolute_for_relative_workspace_root()
     {
        let paths = MediaPmPaths::from_root(std::path::Path::new("."));
        let resolved = resolve_managed_tool_payload_command_path_from_selector(
            &paths,
            "mediapm.tools.yt-dlp+github-releases-yt-dlp@latest",
            "windows/deno/deno.exe",
        )
        .expect("resolved path");

        assert!(std::path::Path::new(&resolved).is_absolute());
    }

    /// Verifies explicit yt-dlp companion ffmpeg selectors fail fast when no
    /// managed ffmpeg identity matches the requested selector.
    #[test]
    fn companion_ffmpeg_selection_rejects_unknown_selector() {
        let temp = tempfile::tempdir().expect("tempdir");
        let paths = MediaPmPaths::from_root(temp.path());
        let requirement = ToolRequirement {
            version: None,
            tag: Some("latest".to_string()),
            dependencies: crate::config::ToolRequirementDependencies {
                ffmpeg_version: Some("v9.9".to_string()),
                deno_version: None,
                sd_version: None,
            },
            recheck_seconds: None,
            max_input_slots: None,
            max_output_slots: None,
        };

        let error = resolve_companion_ffmpeg_selection(
            &paths,
            "yt-dlp",
            &requirement,
            &BTreeMap::new(),
            &MediaPmState::default(),
            &MachineNickelDocument::default(),
        )
        .expect_err("unknown selector should fail");

        assert!(
        error.to_string().contains(
            "tools.yt-dlp.dependencies.ffmpeg_version 'v9.9' did not match any managed ffmpeg tool"
        ),
        "unexpected error: {error}"
    );
    }

    /// Verifies builtin source-ingest logical tool requirements are skipped
    /// from downloader provisioning.
    #[test]
    fn builtin_source_ingest_tool_requirements_are_skipped_from_provisioning() {
        assert!(is_builtin_source_ingest_requirement("import"));
        assert!(!is_builtin_source_ingest_requirement("ffmpeg"));
        assert!(!is_builtin_source_ingest_requirement("yt-dlp"));
    }

    /// Verifies shared CAS hashes are preserved when any remaining machine tool
    /// config still references them.
    #[test]
    fn hash_reference_guard_detects_shared_tool_content_hashes() {
        let shared_hash = Hash::from_content(b"shared-companion-bytes");
        let unique_hash = Hash::from_content(b"unique-bytes");

        let machine = MachineNickelDocument {
            tool_configs: BTreeMap::from([
                (
                    "mediapm.tools.yt-dlp@new".to_string(),
                    ToolConfigSpec {
                        content_map: Some(BTreeMap::from([(
                            "linux/deno/deno".to_string(),
                            shared_hash,
                        )])),
                        ..ToolConfigSpec::default()
                    },
                ),
                (
                    "mediapm.tools.ffmpeg@new".to_string(),
                    ToolConfigSpec {
                        content_map: Some(BTreeMap::from([(
                            "linux/ffmpeg".to_string(),
                            unique_hash,
                        )])),
                        ..ToolConfigSpec::default()
                    },
                ),
            ]),
            ..MachineNickelDocument::default()
        };

        assert!(is_hash_still_referenced_by_tool_configs(&machine, shared_hash));
        assert!(is_hash_still_referenced_by_tool_configs(&machine, unique_hash));
        assert!(!is_hash_still_referenced_by_tool_configs(
            &machine,
            Hash::from_content(b"missing-hash")
        ));
    }

    /// Verifies that a tool entry referenced by an existing workflow step in the
    /// machine document is preserved even when the tool is not in the desired set
    /// and not declared in the user document.
    #[test]
    fn prune_unmanaged_tool_artifacts_preserves_workflow_referenced_tool_ids() {
        let temp = tempfile::tempdir().expect("tempdir");
        let cas_root = temp.path().join("cas");
        std::fs::create_dir_all(&cas_root).expect("create cas root");

        let runtime = tokio::runtime::Builder::new_current_thread()
            .enable_all()
            .build()
            .expect("build runtime");

        runtime.block_on(async {
            let paths = MediaPmPaths::from_root(temp.path());
            let cas = FileSystemCas::open(&cas_root).await.expect("open cas");

            let tool_id = "mediapm.tools.workflow-referenced-tool@v1".to_string();
            let logical_name = "workflow-referenced-tool".to_string();

            let document = MediaPmDocument::default();

            let mut lock = MediaPmState {
                tool_registry: BTreeMap::from([(
                    tool_id.clone(),
                    ToolRegistryRecord {
                        name: logical_name.clone(),
                        version: "latest".to_string(),
                        source: "fixture".to_string(),
                        registry_multihash: "hash:mock".to_string(),
                        last_transition_unix_seconds: 100,
                        status: ToolRegistryStatus::Active,
                    },
                )]),
                ..MediaPmState::default()
            };

            let mut machine = MachineNickelDocument::default();
            machine.tools.insert(
                tool_id.clone(),
                ToolSpec {
                    kind: ToolKindSpec::Executable {
                        command: vec!["test-tool".to_string()],
                        env_vars: BTreeMap::new(),
                        success_codes: vec![0],
                    },
                    ..ToolSpec::default()
                },
            );
            machine.tool_configs.insert(
                tool_id.clone(),
                ToolConfigSpec {
                    content_map: Some(BTreeMap::from([(
                        "test-content".to_string(),
                        Hash::from_content(b"payload"),
                    )])),
                    ..ToolConfigSpec::default()
                },
            );
            // Workflow step references the tool by id.
            machine.workflows.insert(
                "test-workflow".to_string(),
                WorkflowSpec {
                    steps: vec![WorkflowStepSpec {
                        id: "step-1".to_string(),
                        tool: tool_id.clone(),
                        inputs: BTreeMap::new(),
                        depends_on: Vec::new(),
                        outputs: BTreeMap::new(),
                    }],
                    ..WorkflowSpec::default()
                },
            );

            let desired_tool_ids = BTreeSet::new();
            let mut report = ToolSyncReport::default();

            prune_unmanaged_tool_artifacts(
                &paths,
                &document,
                &cas,
                &mut machine,
                &mut lock,
                &desired_tool_ids,
                &mut report,
            )
            .await
            .expect("prune should not error");

            // The tool should still be present because it is referenced by a
            // workflow step.
            assert!(machine.tools.contains_key(&tool_id), "tool should be preserved");

            let registry = lock.tool_registry.get(&tool_id);
            assert!(registry.is_some(), "registry entry should still exist");
            assert_eq!(
                registry.expect("registry entry").status,
                ToolRegistryStatus::Active,
                "tool should remain active"
            );
        });
    }

    /// Verifies that a stale tool entry not in the desired set and not referenced
    /// by any workflow step is pruned from the machine document and marked as
    /// Pruned in the state registry.
    #[test]
    fn prune_unmanaged_tool_artifacts_prunes_stale_non_referenced_tools() {
        let temp = tempfile::tempdir().expect("tempdir");
        let cas_root = temp.path().join("cas");
        std::fs::create_dir_all(&cas_root).expect("create cas root");

        let runtime = tokio::runtime::Builder::new_current_thread()
            .enable_all()
            .build()
            .expect("build runtime");

        runtime.block_on(async {
            let paths = MediaPmPaths::from_root(temp.path());
            let cas = FileSystemCas::open(&cas_root).await.expect("open cas");

            let tool_id = "mediapm.tools.stale-tool@v1".to_string();
            let logical_name = "stale-tool".to_string();

            let document = MediaPmDocument::default();

            let mut lock = MediaPmState {
                tool_registry: BTreeMap::from([(
                    tool_id.clone(),
                    ToolRegistryRecord {
                        name: logical_name.clone(),
                        version: "latest".to_string(),
                        source: "fixture".to_string(),
                        registry_multihash: "hash:mock".to_string(),
                        last_transition_unix_seconds: 200,
                        status: ToolRegistryStatus::Active,
                    },
                )]),
                ..MediaPmState::default()
            };

            let mut machine = MachineNickelDocument::default();
            machine.tools.insert(
                tool_id.clone(),
                ToolSpec {
                    kind: ToolKindSpec::Executable {
                        command: vec!["test-tool".to_string()],
                        env_vars: BTreeMap::new(),
                        success_codes: vec![0],
                    },
                    ..ToolSpec::default()
                },
            );
            machine.tool_configs.insert(
                tool_id.clone(),
                ToolConfigSpec {
                    content_map: Some(BTreeMap::from([(
                        "test-content".to_string(),
                        Hash::from_content(b"payload"),
                    )])),
                    ..ToolConfigSpec::default()
                },
            );
            // No workflow step references the tool.

            let desired_tool_ids = BTreeSet::new();
            let mut report = ToolSyncReport::default();

            prune_unmanaged_tool_artifacts(
                &paths,
                &document,
                &cas,
                &mut machine,
                &mut lock,
                &desired_tool_ids,
                &mut report,
            )
            .await
            .expect("prune should not error");

            // The tool should be removed from the machine document.
            assert!(!machine.tools.contains_key(&tool_id), "tool should be removed");

            let registry = lock.tool_registry.get(&tool_id);
            assert!(registry.is_some(), "registry entry should still exist");
            assert_eq!(
                registry.expect("registry entry").status,
                ToolRegistryStatus::Pruned,
                "tool should be marked as Pruned"
            );

            assert!(
                report.warnings.iter().any(|w| w.contains(&tool_id)),
                "warning should mention the pruned tool id"
            );
        });
    }
}
