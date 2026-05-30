//! Desired-tool reconciliation and prune flows for mediapm.

mod content_import;
mod lifecycle;
mod provision;
mod tool_config;

use self::content_import::import_tool_content_files_into_cas;
use self::lifecycle::{
    ensure_internal_launcher_content_entries_exist, is_builtin_source_ingest_requirement,
    lock_registry_version, prune_unmanaged_tool_artifacts, should_skip_tag_update_check,
};
use self::provision::provision_desired_tools_concurrently;
use self::tool_config::{
    augment_media_tagger_tool_id_with_ffmpeg_selector, augment_tool_id_with_dependency_selector,
    ensure_machine_runtime_inherits_generated_env_vars,
    remove_redundant_inherited_env_vars_from_tool_config, resolve_companion_deno_selection,
    resolve_companion_ffmpeg_selection, resolve_conductor_runtime_dir,
    resolve_managed_tool_payload_command_path_from_selector,
    resolve_managed_tool_payload_directory_from_selector, resolve_media_tagger_ffmpeg_selection,
    resolve_yt_dlp_js_runtime_path, should_set_yt_dlp_ffmpeg_location,
    should_set_yt_dlp_js_runtimes, write_generated_runtime_env_file,
};
use std::collections::{BTreeMap, BTreeSet};
use std::sync::Arc;

use mediapm_cas::{CasApi, FileSystemCas, Hash};
use mediapm_conductor::runtime_env::ensure_runtime_env_files;
use mediapm_conductor::{AddToolOptions, InputBinding, ToolKindSpec};

use crate::builtins::media_tagger::MEDIA_TAGGER_FFMPEG_BIN_ENV;
use crate::config::MediaPmDocument;
use crate::error::MediaPmError;
use crate::lockfile::{MediaLockFile, ToolRegistryRecord, ToolRegistryStatus};
use crate::paths::MediaPmPaths;
use crate::tools::downloader::{ToolDownloadCache, default_global_tool_cache_root};

use super::ToolSyncReport;
use super::documents::{ensure_conductor_documents, load_machine_document, save_machine_document};
use super::runtime_storage::resolve_cas_store_path;
use super::tool_runtime::{
    build_tool_env, build_tool_spec, default_tool_config_description, merge_tool_config_defaults,
    resolve_ffmpeg_slot_limits, validate_tool_command,
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
    lock: &mut MediaLockFile,
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

    let cache_root = default_global_tool_cache_root().ok_or_else(|| {
        MediaPmError::Workflow(
            "resolving shared global user cache root for managed-tool downloads failed".to_string(),
        )
    })?;
    let cache = ToolDownloadCache::open(&cache_root).await.map_err(|error| {
        MediaPmError::Workflow(format!(
            "opening shared global user cache at '{}' failed: {error}",
            cache_root.display()
        ))
    })?;
    let _ = cache.prune_expired_entries().await;
    let shared_tool_cache = Some(Arc::new(cache));

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

            desired_tool_id = augment_media_tagger_tool_id_with_ffmpeg_selector(
                &desired_tool_id,
                &ffmpeg_selection.selector,
            );
            media_tagger_ffmpeg_host_command_path = ffmpeg_selection.host_command_path;
            for (entry_key, entry_source) in ffmpeg_selection.provisioned_content_entries {
                effective_content_entries.entry(entry_key).or_insert(entry_source);
            }
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
            companion_ffmpeg_content_map = companion_selection.existing_content_map;
            companion_ffmpeg_host_command_path = companion_selection.host_command_path;

            for (entry_key, entry_source) in companion_selection.provisioned_content_entries {
                effective_content_entries.entry(entry_key).or_insert(entry_source);
            }

            if let Some(companion_deno_selection) = resolve_companion_deno_selection(
                name,
                requirement,
                &provisioned_snapshot,
                lock,
                &machine,
            )? {
                desired_tool_id = augment_tool_id_with_dependency_selector(
                    &desired_tool_id,
                    "deno",
                    &companion_deno_selection.selector,
                );
                companion_deno_content_map = companion_deno_selection.existing_content_map;
                companion_deno_host_command_path = companion_deno_selection.host_command_path;

                for (entry_key, entry_source) in
                    companion_deno_selection.provisioned_content_entries
                {
                    effective_content_entries.entry(entry_key).or_insert(entry_source);
                }
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

            if should_set_yt_dlp_ffmpeg_location(&desired_config.input_defaults)
                && let Some(companion_selector_path) = companion_ffmpeg_host_command_path.as_deref()
                && let Some(ffmpeg_path) = resolve_managed_tool_payload_directory_from_selector(
                    paths,
                    &desired_tool_id,
                    companion_selector_path,
                )
            {
                desired_config
                    .input_defaults
                    .insert("ffmpeg_location".to_string(), InputBinding::String(ffmpeg_path));
            }

            if should_set_yt_dlp_js_runtimes(&desired_config.input_defaults)
                && let Some(js_runtimes_path) = companion_deno_host_command_path
                    .as_deref()
                    .and_then(|selector_path| {
                        resolve_managed_tool_payload_command_path_from_selector(
                            paths,
                            &desired_tool_id,
                            selector_path,
                        )
                    })
                    .or_else(|| resolve_yt_dlp_js_runtime_path(paths, &desired_tool_id))
            {
                desired_config.input_defaults.insert(
                    "js_runtimes".to_string(),
                    InputBinding::String(format!("deno:{js_runtimes_path}")),
                );
            }
        }
        remove_redundant_inherited_env_vars_from_tool_config(
            &mut desired_config,
            inherited_env_vars,
        );
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

        if existing_active.is_some() {
            report.updated_tool_ids.push(desired_tool_id);
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

/// Prunes one tool binary while preserving tool metadata.
///
/// This operation removes only `tool_configs.<tool_id>` so conductor metadata
/// for historical versions is retained.
pub(crate) async fn prune_tool_binary(
    paths: &MediaPmPaths,
    lock: &mut MediaLockFile,
    tool_id: &str,
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
            if cas.exists(*hash).await.unwrap_or(false) {
                let _ = cas.delete(*hash).await;
            }
        }
    }

    if let Some(entry) = lock.tool_registry.get_mut(tool_id) {
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
mod tests;
