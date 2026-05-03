//! Desired-tool reconciliation and prune flows for Phase 3.

use std::collections::{BTreeMap, BTreeSet};
use std::fs;
use std::io::{Cursor, Write};
use std::path::Path;
use std::path::PathBuf;
use std::sync::Arc;
use std::time::Duration;

use mediapm_cas::{CasApi, FileSystemCas, Hash};
use mediapm_conductor::{AddToolOptions, InputBinding, MachineNickelDocument, ToolKindSpec};
use pulsebar::{MultiProgress, ProgressBar};

use crate::builtins::media_tagger::MEDIA_TAGGER_FFMPEG_BIN_ENV;
use crate::config::{
    MediaPmDocument, ToolRequirement, normalize_selector_compare_value, normalize_selector_value,
};
use crate::error::MediaPmError;
use crate::lockfile::{MediaLockFile, ToolRegistryRecord, ToolRegistryStatus};
use crate::paths::MediaPmPaths;
use crate::tools::catalog::{ToolDownloadDescriptor, tool_catalog_entry};
use crate::tools::downloader::{
    ContentMapSource, DownloadProgressCallback, DownloadProgressSnapshot, ProvisionedToolPayload,
    ResolvedToolIdentity, ToolDownloadCache, default_global_tool_cache_root,
    provision_tool_payload,
};

use super::ToolSyncReport;
use super::documents::{ensure_conductor_documents, load_machine_document, save_machine_document};
use super::runtime_storage::resolve_cas_store_path;
use super::tool_runtime::{
    MEDIA_TAGGER_LAUNCHER_MEDIAPM_BIN_LINUX_ENV, MEDIA_TAGGER_LAUNCHER_MEDIAPM_BIN_MACOS_ENV,
    MEDIA_TAGGER_LAUNCHER_MEDIAPM_BIN_WINDOWS_ENV, build_tool_env, build_tool_spec,
    default_tool_config_description, extract_platform_conditional_paths,
    merge_tool_config_defaults, resolve_ffmpeg_slot_limits, validate_tool_command,
};
use super::util::now_unix_seconds;

/// Reconciles desired tools from `mediapm.ncl` into conductor machine config.
#[expect(
    clippy::too_many_lines,
    reason = "this item intentionally keeps end-to-end control flow together so ordering invariants remain explicit during maintenance"
)]
pub(crate) async fn reconcile_desired_tools(
    paths: &MediaPmPaths,
    document: &MediaPmDocument,
    inherited_env_vars: &[String],
    lock: &mut MediaLockFile,
    check_tag_updates: bool,
    use_user_tool_cache: bool,
) -> Result<ToolSyncReport, MediaPmError> {
    ensure_conductor_documents(paths)?;

    let mut report = ToolSyncReport::default();
    let mut machine = load_machine_document(&paths.conductor_machine_ncl)?;
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

    let shared_tool_cache = if use_user_tool_cache {
        if let Some(cache_root) = default_global_tool_cache_root() {
            match ToolDownloadCache::open(&cache_root).await {
                Ok(cache) => {
                    let _ = cache.prune_expired_entries().await;
                    Some(Arc::new(cache))
                }
                Err(error) => {
                    report.warnings.push(format!("shared global user cache disabled: {error}"));
                    None
                }
            }
        } else {
            report.warnings.push(
                "shared global user cache disabled: global user directory could not be resolved"
                    .to_string(),
            );
            None
        }
    } else {
        None
    };

    let mut provisioned_by_name =
        provision_desired_tools_concurrently(paths, &requirements_to_provision, shared_tool_cache)
            .await?;
    let provisioned_snapshot = provisioned_by_name.clone();
    let mut desired_tool_ids = BTreeSet::new();

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
        let mut effective_content_entries = provisioned.content_entries.clone();
        let mut desired_tool_id = provisioned.tool_id.clone();
        let mut media_tagger_ffmpeg_content_map = BTreeMap::new();
        let mut media_tagger_ffmpeg_host_command_path: Option<String> = None;
        let mut media_tagger_ffmpeg_tool_id: Option<String> = None;
        let mut companion_ffmpeg_content_map = BTreeMap::new();
        let mut companion_ffmpeg_host_command_path: Option<String> = None;

        if name.eq_ignore_ascii_case("media-tagger") {
            let ffmpeg_selection = resolve_media_tagger_ffmpeg_selection(
                requirement,
                &provisioned_snapshot,
                lock,
                &machine,
            )?;

            desired_tool_id = augment_media_tagger_tool_id_with_ffmpeg_selector(
                &desired_tool_id,
                &ffmpeg_selection.selector,
            );
            media_tagger_ffmpeg_content_map = ffmpeg_selection.existing_content_map;
            media_tagger_ffmpeg_host_command_path = ffmpeg_selection.host_command_path;
            media_tagger_ffmpeg_tool_id = Some(ffmpeg_selection.selected_tool_id);
            for (entry_key, entry_source) in ffmpeg_selection.provisioned_content_entries {
                effective_content_entries.entry(entry_key).or_insert(entry_source);
            }
        }

        if name.eq_ignore_ascii_case("yt-dlp")
            && let Some(companion_selection) = resolve_companion_ffmpeg_selection(
                "yt-dlp",
                requirement,
                &provisioned_snapshot,
                lock,
                &machine,
            )?
        {
            companion_ffmpeg_content_map = companion_selection.existing_content_map;
            companion_ffmpeg_host_command_path = companion_selection.host_command_path;

            for (entry_key, entry_source) in companion_selection.provisioned_content_entries {
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
        if name.eq_ignore_ascii_case("media-tagger") {
            for (relative_path, multihash) in media_tagger_ffmpeg_content_map {
                desired_config
                    .content_map
                    .get_or_insert_with(BTreeMap::new)
                    .entry(relative_path)
                    .or_insert(multihash);
            }
        }
        if name.eq_ignore_ascii_case("yt-dlp") {
            for (relative_path, multihash) in companion_ffmpeg_content_map {
                desired_config
                    .content_map
                    .get_or_insert_with(BTreeMap::new)
                    .entry(relative_path)
                    .or_insert(multihash);
            }
        }
        if name.eq_ignore_ascii_case("yt-dlp")
            && let Some(ffmpeg_path) = companion_ffmpeg_host_command_path
            && matches!(
                desired_config.input_defaults.get("ffmpeg_location"),
                Some(InputBinding::String(value))
                    if value.trim().is_empty() || value.eq_ignore_ascii_case("ffmpeg")
            )
        {
            desired_config
                .input_defaults
                .insert("ffmpeg_location".to_string(), InputBinding::String(ffmpeg_path));
        }
        remove_redundant_inherited_env_vars_from_tool_config(
            &mut desired_config,
            inherited_env_vars,
        );
        let generated_env_vars = build_tool_env(paths, name)?;
        for (env_key, env_value) in generated_env_vars {
            let is_managed_launcher_key = matches!(
                env_key.as_str(),
                MEDIA_TAGGER_LAUNCHER_MEDIAPM_BIN_WINDOWS_ENV
                    | MEDIA_TAGGER_LAUNCHER_MEDIAPM_BIN_LINUX_ENV
                    | MEDIA_TAGGER_LAUNCHER_MEDIAPM_BIN_MACOS_ENV
            );

            if is_managed_launcher_key {
                desired_config.env_vars.insert(env_key, env_value);
            } else {
                desired_config.env_vars.entry(env_key).or_insert(env_value);
            }
        }
        if name.eq_ignore_ascii_case("media-tagger")
            && let Some(ffmpeg_path) = media_tagger_ffmpeg_host_command_path
        {
            let ffmpeg_path = resolve_managed_tool_command_absolute_path(
                paths,
                media_tagger_ffmpeg_tool_id.as_deref(),
                &ffmpeg_path,
            )
            .unwrap_or(ffmpeg_path);
            desired_config.env_vars.insert(MEDIA_TAGGER_FFMPEG_BIN_ENV.to_string(), ffmpeg_path);
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

    save_machine_document(&paths.conductor_machine_ncl, &machine)?;
    Ok(report)
}

/// Removes env-var entries from tool configs when they are already inherited
/// globally by conductor runtime storage.
///
/// This keeps managed tool configs focused on tool-specific overrides and
/// avoids duplicating baseline host environment names under
/// `tool_configs.<tool>.env_vars`.
fn remove_redundant_inherited_env_vars_from_tool_config(
    tool_config: &mut mediapm_conductor::ToolConfigSpec,
    inherited_env_vars: &[String],
) {
    if inherited_env_vars.is_empty() || tool_config.env_vars.is_empty() {
        return;
    }

    let inherited_lower = inherited_env_vars
        .iter()
        .map(|name| name.trim())
        .filter(|name| !name.is_empty())
        .map(str::to_ascii_lowercase)
        .collect::<BTreeSet<_>>();
    if inherited_lower.is_empty() {
        return;
    }

    tool_config
        .env_vars
        .retain(|name, _| !inherited_lower.contains(&name.trim().to_ascii_lowercase()));
}

/// Resolves preferred managed ffmpeg binary path for current host OS.
#[must_use]
fn resolve_host_command_selector_path(command_selector: &str) -> Option<String> {
    if command_selector.contains("context.os") {
        let selectors = extract_platform_conditional_paths(command_selector).ok()?;
        return selectors.get(std::env::consts::OS).cloned();
    }

    let trimmed = command_selector.trim();
    if trimmed.is_empty() { None } else { Some(trimmed.to_string()) }
}

/// Resolved ffmpeg linkage used to stabilize managed media-tagger identity and
/// content-map compatibility with the selected ffmpeg payload.
#[derive(Debug, Clone)]
struct MediaTaggerFfmpegSelection {
    /// Stable selector fragment (hash, version, or tag) folded into tool id.
    selector: String,
    /// Concrete managed ffmpeg tool id selected for this linkage.
    selected_tool_id: String,
    /// Optional provisioned payload entries for selected ffmpeg content.
    provisioned_content_entries: BTreeMap<String, ContentMapSource>,
    /// Existing machine content-map entries for selected ffmpeg payload.
    existing_content_map: BTreeMap<String, Hash>,
    /// Host-resolved ffmpeg executable path for media-tagger subprocess env.
    host_command_path: Option<String>,
}

/// Resolved companion ffmpeg linkage for tools that invoke ffmpeg subprocesses.
#[derive(Debug, Clone)]
struct CompanionFfmpegSelection {
    /// Optional provisioned payload entries for selected ffmpeg content.
    provisioned_content_entries: BTreeMap<String, ContentMapSource>,
    /// Existing machine content-map entries for selected ffmpeg payload.
    existing_content_map: BTreeMap<String, Hash>,
    /// Host-resolved ffmpeg executable path for companion tool arguments.
    host_command_path: Option<String>,
}

/// Stable sandbox prefix where media-tagger mounts selected ffmpeg payloads.
const MEDIA_TAGGER_FFMPEG_CONTENT_PREFIX: &str = "ffmpeg/";

/// Normalizes one managed-tool relative command path for install-root lookup.
#[must_use]
fn normalize_managed_tool_relative_command_path(relative_command_path: &str) -> Option<String> {
    let normalized = relative_command_path
        .trim()
        .replace('\\', "/")
        .trim_start_matches("./")
        .trim_start_matches('/')
        .to_string();
    if normalized.is_empty() {
        return None;
    }

    let without_media_tagger_prefix = normalized
        .strip_prefix(MEDIA_TAGGER_FFMPEG_CONTENT_PREFIX)
        .unwrap_or(&normalized)
        .trim_start_matches('/')
        .to_string();

    if without_media_tagger_prefix.is_empty() { None } else { Some(without_media_tagger_prefix) }
}

/// Resolves an absolute managed-tool command path from one relative selector path.
///
/// Returns `None` when no tool id is provided or when the candidate path does not
/// currently exist as a regular file under `paths.tools_dir/<tool_id>/`.
///
/// For media-tagger ffmpeg linkage, this also accepts namespaced selectors such
/// as `ffmpeg/windows/...` and resolves them against the selected managed ffmpeg
/// install root.
#[must_use]
fn resolve_managed_tool_command_absolute_path(
    paths: &MediaPmPaths,
    tool_id: Option<&str>,
    relative_command_path: &str,
) -> Option<String> {
    let tool_id = tool_id?.trim();
    if tool_id.is_empty() {
        return None;
    }

    let relative = normalize_managed_tool_relative_command_path(relative_command_path)?;

    let candidate = paths.tools_dir.join(tool_id).join(Path::new(&relative));
    if candidate.is_file() { Some(candidate.to_string_lossy().replace('\\', "/")) } else { None }
}

/// Prefixes one ffmpeg content-map key for media-tagger sandbox mounting.
#[must_use]
fn media_tagger_ffmpeg_content_key(relative_path: &str) -> String {
    let normalized = relative_path
        .replace('\\', "/")
        .trim_start_matches("./")
        .trim_start_matches('/')
        .to_string();

    if normalized.is_empty() {
        MEDIA_TAGGER_FFMPEG_CONTENT_PREFIX.to_string()
    } else if normalized.starts_with(MEDIA_TAGGER_FFMPEG_CONTENT_PREFIX) {
        normalized
    } else {
        format!("{MEDIA_TAGGER_FFMPEG_CONTENT_PREFIX}{normalized}")
    }
}

/// Prefixes ffmpeg provisioned content-map entries for media-tagger tool rows.
#[must_use]
fn prefix_media_tagger_ffmpeg_content_entries(
    entries: &BTreeMap<String, ContentMapSource>,
) -> BTreeMap<String, ContentMapSource> {
    entries
        .iter()
        .map(|(path, source)| (media_tagger_ffmpeg_content_key(path), source.clone()))
        .collect()
}

/// Prefixes ffmpeg hash-only content-map rows for media-tagger tool rows.
#[must_use]
fn prefix_media_tagger_ffmpeg_hash_map(entries: &BTreeMap<String, Hash>) -> BTreeMap<String, Hash> {
    entries.iter().map(|(path, hash)| (media_tagger_ffmpeg_content_key(path), *hash)).collect()
}

/// Resolves the ffmpeg payload that media-tagger should bind to.
///
/// Selection priority honors `tools.media-tagger.dependencies.ffmpeg_version`: explicit
/// selector first, otherwise inherited active/provisioned ffmpeg.
#[expect(
    clippy::too_many_lines,
    reason = "this item intentionally keeps end-to-end control flow together so ordering invariants remain explicit during maintenance"
)]
fn resolve_media_tagger_ffmpeg_selection(
    requirement: &ToolRequirement,
    provisioned_snapshot: &BTreeMap<String, ProvisionedToolPayload>,
    lock: &MediaLockFile,
    machine: &MachineNickelDocument,
) -> Result<MediaTaggerFfmpegSelection, MediaPmError> {
    let requested_selector = requirement.normalized_ffmpeg_selector().filter(|selector| {
        !selector.eq_ignore_ascii_case("inherit") && !selector.eq_ignore_ascii_case("global")
    });

    if let Some(requested_selector) = requested_selector {
        let normalized_requested = normalize_selector_compare_value(&requested_selector);

        if let Some(payload) = provisioned_snapshot.get("ffmpeg")
            && ffmpeg_identity_matches_selector(&payload.identity, &normalized_requested)
        {
            return Ok(MediaTaggerFfmpegSelection {
                selector: ffmpeg_selector_from_identity(&payload.identity).unwrap_or_else(|| {
                    normalize_selector_value(Some(&requested_selector))
                        .unwrap_or_else(|| requested_selector.clone())
                }),
                selected_tool_id: payload.tool_id.clone(),
                provisioned_content_entries: prefix_media_tagger_ffmpeg_content_entries(
                    &payload.content_entries,
                ),
                existing_content_map: prefix_media_tagger_ffmpeg_hash_map(
                    &machine
                        .tool_configs
                        .get(&payload.tool_id)
                        .and_then(|config| config.content_map.clone())
                        .unwrap_or_default(),
                ),
                host_command_path: resolve_host_command_selector_path(&payload.command_selector)
                    .map(|path| media_tagger_ffmpeg_content_key(&path)),
            });
        }

        let mut candidates = lock
            .tool_registry
            .iter()
            .filter(|(_, record)| record.name.eq_ignore_ascii_case("ffmpeg"))
            .filter_map(|(tool_id, record)| {
                let selector = normalize_selector_value(Some(&record.version))?;
                if normalize_selector_compare_value(&selector) == normalized_requested
                    && machine.tools.contains_key(tool_id)
                {
                    Some((tool_id.clone(), selector))
                } else {
                    None
                }
            })
            .collect::<Vec<_>>();

        if candidates.is_empty() {
            return Err(MediaPmError::Workflow(format!(
                "tools.media-tagger.dependencies.ffmpeg_version '{requested_selector}' did not match any managed ffmpeg tool"
            )));
        }

        candidates.sort_by(|left, right| left.0.cmp(&right.0));
        let active_ffmpeg_tool_id = lock.active_tools.get("ffmpeg");
        let (selected_tool_id, selected_selector) = if let Some(active_tool_id) =
            active_ffmpeg_tool_id
        {
            candidates
                    .iter()
                    .find(|(tool_id, _)| tool_id == active_tool_id)
                    .cloned()
                    .or_else(|| candidates.first().cloned())
                    .ok_or_else(|| {
                        MediaPmError::Workflow(
                            "tools.media-tagger.dependencies.ffmpeg_version matched no viable ffmpeg candidates"
                                .to_string(),
                        )
                    })?
        } else {
            candidates.first().cloned().ok_or_else(|| {
                    MediaPmError::Workflow(
                        "tools.media-tagger.dependencies.ffmpeg_version matched no viable ffmpeg candidates"
                            .to_string(),
                    )
                })?
        };

        return Ok(MediaTaggerFfmpegSelection {
            selector: selected_selector,
            selected_tool_id: selected_tool_id.clone(),
            provisioned_content_entries: BTreeMap::new(),
            existing_content_map: prefix_media_tagger_ffmpeg_hash_map(
                &machine
                    .tool_configs
                    .get(&selected_tool_id)
                    .and_then(|config| config.content_map.clone())
                    .unwrap_or_default(),
            ),
            host_command_path: resolve_host_ffmpeg_command_path_from_machine_tool(
                machine,
                &selected_tool_id,
            )
            .map(|path| media_tagger_ffmpeg_content_key(&path)),
        });
    }

    if let Some(payload) = provisioned_snapshot.get("ffmpeg") {
        let selector = ffmpeg_selector_from_identity(&payload.identity).ok_or_else(|| {
            MediaPmError::Workflow(
                "managed ffmpeg payload did not expose hash/version/tag identity for media-tagger linkage"
                    .to_string(),
            )
        })?;

        return Ok(MediaTaggerFfmpegSelection {
            selector,
            selected_tool_id: payload.tool_id.clone(),
            provisioned_content_entries: prefix_media_tagger_ffmpeg_content_entries(
                &payload.content_entries,
            ),
            existing_content_map: prefix_media_tagger_ffmpeg_hash_map(
                &machine
                    .tool_configs
                    .get(&payload.tool_id)
                    .and_then(|config| config.content_map.clone())
                    .unwrap_or_default(),
            ),
            host_command_path: resolve_host_command_selector_path(&payload.command_selector)
                .map(|path| media_tagger_ffmpeg_content_key(&path)),
        });
    }

    let active_ffmpeg_tool_id = lock.active_tools.get("ffmpeg").ok_or_else(|| {
        MediaPmError::Workflow(
            "media-tagger requires active logical tool 'ffmpeg' when tools.media-tagger.dependencies.ffmpeg_version is not pinned"
                .to_string(),
        )
    })?;

    if !machine.tools.contains_key(active_ffmpeg_tool_id) {
        return Err(MediaPmError::Workflow(format!(
            "active ffmpeg tool '{active_ffmpeg_tool_id}' is missing from conductor machine config"
        )));
    }

    let selector = ffmpeg_selector_from_registry_or_tool_id(active_ffmpeg_tool_id, lock)
        .ok_or_else(|| {
            MediaPmError::Workflow(format!(
                "could not derive ffmpeg selector identity from active tool '{active_ffmpeg_tool_id}'"
            ))
        })?;

    Ok(MediaTaggerFfmpegSelection {
        selector,
        selected_tool_id: active_ffmpeg_tool_id.clone(),
        provisioned_content_entries: BTreeMap::new(),
        existing_content_map: prefix_media_tagger_ffmpeg_hash_map(
            &machine
                .tool_configs
                .get(active_ffmpeg_tool_id)
                .and_then(|config| config.content_map.clone())
                .unwrap_or_default(),
        ),
        host_command_path: resolve_host_ffmpeg_command_path_from_machine_tool(
            machine,
            active_ffmpeg_tool_id,
        )
        .map(|path| media_tagger_ffmpeg_content_key(&path)),
    })
}

/// Resolves optional ffmpeg linkage for companion tools like `yt-dlp`.
///
/// Selection priority honors `<tool>.dependencies.ffmpeg_version`: explicit selector first,
/// then inherited active/provisioned ffmpeg payload when available.
fn resolve_companion_ffmpeg_selection(
    logical_tool_name: &str,
    requirement: &ToolRequirement,
    provisioned_snapshot: &BTreeMap<String, ProvisionedToolPayload>,
    lock: &MediaLockFile,
    machine: &MachineNickelDocument,
) -> Result<Option<CompanionFfmpegSelection>, MediaPmError> {
    let requested_selector = requirement.normalized_ffmpeg_selector().filter(|selector| {
        !selector.eq_ignore_ascii_case("inherit") && !selector.eq_ignore_ascii_case("global")
    });

    if let Some(requested_selector) = requested_selector {
        let normalized_requested = normalize_selector_compare_value(&requested_selector);

        if let Some(payload) = provisioned_snapshot.get("ffmpeg")
            && ffmpeg_identity_matches_selector(&payload.identity, &normalized_requested)
        {
            return Ok(Some(CompanionFfmpegSelection {
                provisioned_content_entries: payload.content_entries.clone(),
                existing_content_map: machine
                    .tool_configs
                    .get(&payload.tool_id)
                    .and_then(|config| config.content_map.clone())
                    .unwrap_or_default(),
                host_command_path: resolve_host_command_selector_path(&payload.command_selector),
            }));
        }

        let mut candidates = lock
            .tool_registry
            .iter()
            .filter(|(_, record)| record.name.eq_ignore_ascii_case("ffmpeg"))
            .filter_map(|(tool_id, record)| {
                let selector = normalize_selector_value(Some(&record.version))?;
                if normalize_selector_compare_value(&selector) == normalized_requested
                    && machine.tools.contains_key(tool_id)
                {
                    Some((tool_id.clone(), selector))
                } else {
                    None
                }
            })
            .collect::<Vec<_>>();

        if candidates.is_empty() {
            return Err(MediaPmError::Workflow(format!(
                "tools.{logical_tool_name}.dependencies.ffmpeg_version '{requested_selector}' did not match any managed ffmpeg tool"
            )));
        }

        candidates.sort_by(|left, right| left.0.cmp(&right.0));
        let active_ffmpeg_tool_id = lock.active_tools.get("ffmpeg");
        let (selected_tool_id, _) = if let Some(active_tool_id) = active_ffmpeg_tool_id {
            candidates
                .iter()
                .find(|(tool_id, _)| tool_id == active_tool_id)
                .cloned()
                .or_else(|| candidates.first().cloned())
                .ok_or_else(|| {
                    MediaPmError::Workflow(format!(
                        "tools.{logical_tool_name}.dependencies.ffmpeg_version matched no viable ffmpeg candidates"
                    ))
                })?
        } else {
            candidates.first().cloned().ok_or_else(|| {
                MediaPmError::Workflow(format!(
                    "tools.{logical_tool_name}.dependencies.ffmpeg_version matched no viable ffmpeg candidates"
                ))
            })?
        };

        return Ok(Some(CompanionFfmpegSelection {
            provisioned_content_entries: BTreeMap::new(),
            existing_content_map: machine
                .tool_configs
                .get(&selected_tool_id)
                .and_then(|config| config.content_map.clone())
                .unwrap_or_default(),
            host_command_path: resolve_host_ffmpeg_command_path_from_machine_tool(
                machine,
                &selected_tool_id,
            ),
        }));
    }

    if let Some(payload) = provisioned_snapshot.get("ffmpeg") {
        return Ok(Some(CompanionFfmpegSelection {
            provisioned_content_entries: payload.content_entries.clone(),
            existing_content_map: machine
                .tool_configs
                .get(&payload.tool_id)
                .and_then(|config| config.content_map.clone())
                .unwrap_or_default(),
            host_command_path: resolve_host_command_selector_path(&payload.command_selector),
        }));
    }

    if let Some(active_ffmpeg_tool_id) = lock.active_tools.get("ffmpeg")
        && machine.tools.contains_key(active_ffmpeg_tool_id)
    {
        return Ok(Some(CompanionFfmpegSelection {
            provisioned_content_entries: BTreeMap::new(),
            existing_content_map: machine
                .tool_configs
                .get(active_ffmpeg_tool_id)
                .and_then(|config| config.content_map.clone())
                .unwrap_or_default(),
            host_command_path: resolve_host_ffmpeg_command_path_from_machine_tool(
                machine,
                active_ffmpeg_tool_id,
            ),
        }));
    }

    Ok(None)
}

/// Resolves host ffmpeg executable path from one machine-managed tool spec.
#[must_use]
fn resolve_host_ffmpeg_command_path_from_machine_tool(
    machine: &MachineNickelDocument,
    tool_id: &str,
) -> Option<String> {
    let tool_spec = machine.tools.get(tool_id)?;
    let ToolKindSpec::Executable { command, .. } = &tool_spec.kind else {
        return None;
    };

    command.first().and_then(|selector| resolve_host_command_selector_path(selector))
}

/// Returns true when requested selector equals ffmpeg hash/version/tag.
#[must_use]
fn ffmpeg_identity_matches_selector(
    identity: &ResolvedToolIdentity,
    normalized_requested: &str,
) -> bool {
    [identity.git_hash.as_deref(), identity.version.as_deref(), identity.tag.as_deref()]
        .into_iter()
        .flatten()
        .filter_map(|value| normalize_selector_value(Some(value)))
        .any(|value| normalize_selector_compare_value(&value).as_str() == normalized_requested)
}

/// Extracts selector identity from a resolved ffmpeg catalog identity.
#[must_use]
fn ffmpeg_selector_from_identity(identity: &ResolvedToolIdentity) -> Option<String> {
    identity
        .git_hash
        .as_deref()
        .or(identity.version.as_deref())
        .or(identity.tag.as_deref())
        .and_then(|value| normalize_selector_value(Some(value)))
}

/// Derives selector identity from lock registry first, then tool-id suffix.
#[must_use]
fn ffmpeg_selector_from_registry_or_tool_id(tool_id: &str, lock: &MediaLockFile) -> Option<String> {
    if let Some(registry_entry) = lock.tool_registry.get(tool_id)
        && registry_entry.name.eq_ignore_ascii_case("ffmpeg")
        && let Some(selector) = normalize_selector_value(Some(&registry_entry.version))
    {
        return Some(selector);
    }

    tool_id.rsplit_once('@').and_then(|(_, suffix)| normalize_selector_value(Some(suffix)))
}

/// Folds ffmpeg selector identity into media-tagger managed tool id.
#[must_use]
fn augment_media_tagger_tool_id_with_ffmpeg_selector(base_tool_id: &str, selector: &str) -> String {
    let normalized_fragment =
        selector
            .chars()
            .map(|character| {
                if character.is_ascii_alphanumeric() { character.to_ascii_lowercase() } else { '-' }
            })
            .collect::<String>()
            .trim_matches('-')
            .to_string();

    if normalized_fragment.is_empty() {
        return base_tool_id.to_string();
    }

    if let Some((prefix, suffix)) = base_tool_id.rsplit_once('@') {
        format!("{prefix}+ffmpeg-{normalized_fragment}@{suffix}")
    } else {
        format!("{base_tool_id}+ffmpeg-{normalized_fragment}")
    }
}

/// Provisions all desired tools concurrently and reports completion with pulsebar.
///
/// This keeps network transfer concurrency while rendering one progress row per
/// logical tool so users can see byte-level status without mixed output.
#[expect(
    clippy::too_many_lines,
    reason = "this item intentionally keeps end-to-end control flow together so ordering invariants remain explicit during maintenance"
)]
async fn provision_desired_tools_concurrently(
    paths: &MediaPmPaths,
    requirements: &BTreeMap<String, ToolRequirement>,
    shared_download_cache: Option<Arc<ToolDownloadCache>>,
) -> Result<BTreeMap<String, ProvisionedToolPayload>, MediaPmError> {
    if requirements.is_empty() {
        return Ok(BTreeMap::new());
    }

    let multi_progress = MultiProgress::new();
    let overall_progress_total =
        TOOL_PROGRESS_BAR_SCALE.saturating_mul(requirements.len() as u64).max(1);
    let overall_progress = multi_progress
        .add_bar(overall_progress_total)
        .with_message("tool downloads")
        .with_format("{msg} [{bar:24}] {pct}");

    let mut tool_progress_by_name = BTreeMap::<String, ProgressBar>::new();
    for tool_name in requirements.keys() {
        let tool_progress = multi_progress
            .add_bar(TOOL_PROGRESS_BAR_SCALE)
            .with_message(&format!("{tool_name}: queued"))
            .with_format("{msg} [{bar:24}] {pct}");
        tool_progress_by_name.insert(tool_name.clone(), tool_progress);
    }

    let (sender, mut receiver) = tokio::sync::mpsc::unbounded_channel::<ProvisionWorkerEvent>();
    let mut handles = Vec::new();
    let mut progress_state_by_name = requirements
        .keys()
        .map(|name| (name.clone(), ToolDownloadProgressState::default()))
        .collect::<BTreeMap<_, _>>();
    let mut overall_render_state = OverallProgressRenderState::default();

    update_overall_tool_download_progress(
        &overall_progress,
        requirements.len(),
        &progress_state_by_name,
        &mut overall_render_state,
    );

    for (tool_name, requirement) in requirements {
        let worker_paths = paths.clone();
        let worker_tool_name = tool_name.clone();
        let progress_tool_name = worker_tool_name.clone();
        let worker_requirement: ToolRequirement = requirement.clone();
        let worker_progress = tool_progress_by_name.get(tool_name).cloned().ok_or_else(|| {
            MediaPmError::Workflow(format!("missing progress row for logical tool '{tool_name}'"))
        })?;
        let worker_sender = sender.clone();
        let worker_download_cache = shared_download_cache.clone();

        handles.push((
            tool_name.clone(),
            tokio::spawn(async move {
                worker_progress.set_message(&format!("{worker_tool_name}: resolving"));

                let callback_sender = worker_sender.clone();
                let callback: DownloadProgressCallback = Arc::new(move |snapshot| {
                    let snapshot = normalize_download_progress_snapshot(snapshot);
                    let _ = callback_sender.send(ProvisionWorkerEvent::Snapshot {
                        tool_name: progress_tool_name.clone(),
                        snapshot,
                    });
                });

                let result = provision_tool_payload(
                    &worker_paths,
                    &worker_tool_name,
                    &worker_requirement,
                    Some(callback),
                    worker_download_cache,
                )
                .await;

                let _ = worker_sender.send(ProvisionWorkerEvent::Finished {
                    tool_name: worker_tool_name,
                    result: result.map(Box::new),
                });
            }),
        ));
    }
    drop(sender);

    let mut first_error: Option<MediaPmError> = None;
    let mut provisioned = BTreeMap::new();
    let mut completed_tools = BTreeSet::new();

    while completed_tools.len() < requirements.len() {
        let Some(event) = receiver.recv().await else {
            break;
        };

        match event {
            ProvisionWorkerEvent::Snapshot { tool_name, snapshot } => {
                if completed_tools.contains(&tool_name) {
                    continue;
                }

                if let Some(state) = progress_state_by_name.get_mut(&tool_name) {
                    if state.last_snapshot == Some(snapshot) {
                        continue;
                    }
                    state.last_snapshot = Some(snapshot);
                }

                if let Some(tool_progress) = tool_progress_by_name.get(&tool_name) {
                    update_tool_download_progress(tool_progress, &tool_name, snapshot);
                }

                update_overall_tool_download_progress(
                    &overall_progress,
                    requirements.len(),
                    &progress_state_by_name,
                    &mut overall_render_state,
                );
            }
            ProvisionWorkerEvent::Finished { tool_name, result } => {
                if completed_tools.contains(&tool_name) {
                    continue;
                }

                if let Some(state) = progress_state_by_name.get_mut(&tool_name) {
                    state.completed = true;
                }

                match result {
                    Ok(payload) => {
                        if let Some(tool_progress) = tool_progress_by_name.get(&tool_name) {
                            set_tool_completion_progress_row(
                                tool_progress,
                                &tool_name,
                                progress_state_by_name
                                    .get(&tool_name)
                                    .and_then(|state| state.last_snapshot),
                                "ready",
                            );
                        }

                        provisioned.insert(tool_name.clone(), *payload);
                    }
                    Err(error) => {
                        if let Some(tool_progress) = tool_progress_by_name.get(&tool_name) {
                            set_tool_completion_progress_row(
                                tool_progress,
                                &tool_name,
                                progress_state_by_name
                                    .get(&tool_name)
                                    .and_then(|state| state.last_snapshot),
                                "download failed",
                            );
                        }

                        if first_error.is_none() {
                            first_error = Some(MediaPmError::Workflow(format!(
                                "tool '{tool_name}' provisioning failed: {error}"
                            )));
                        }
                    }
                }

                completed_tools.insert(tool_name);
                update_overall_tool_download_progress(
                    &overall_progress,
                    requirements.len(),
                    &progress_state_by_name,
                    &mut overall_render_state,
                );
            }
        }
    }

    if completed_tools.len() < requirements.len() && first_error.is_none() {
        first_error = Some(MediaPmError::Workflow(
            "tool provisioning worker channel closed unexpectedly before all workers reported"
                .to_string(),
        ));
    }

    for (tool_name, handle) in handles {
        if handle.await.is_err() {
            if !completed_tools.contains(&tool_name) {
                if let Some(state) = progress_state_by_name.get_mut(&tool_name) {
                    state.completed = true;
                }
                if let Some(tool_progress) = tool_progress_by_name.get(&tool_name) {
                    set_tool_completion_progress_row(
                        tool_progress,
                        &tool_name,
                        progress_state_by_name
                            .get(&tool_name)
                            .and_then(|state| state.last_snapshot),
                        "worker panicked",
                    );
                }
                completed_tools.insert(tool_name.clone());
                update_overall_tool_download_progress(
                    &overall_progress,
                    requirements.len(),
                    &progress_state_by_name,
                    &mut overall_render_state,
                );
            }
            if first_error.is_none() {
                first_error = Some(MediaPmError::Workflow(format!(
                    "tool provisioning worker thread panicked for '{tool_name}'"
                )));
            }
        } else if !completed_tools.contains(&tool_name) {
            if let Some(state) = progress_state_by_name.get_mut(&tool_name) {
                state.completed = true;
            }
            if let Some(tool_progress) = tool_progress_by_name.get(&tool_name) {
                set_tool_completion_progress_row(
                    tool_progress,
                    &tool_name,
                    progress_state_by_name.get(&tool_name).and_then(|state| state.last_snapshot),
                    "worker finished without result",
                );
            }
            completed_tools.insert(tool_name.clone());
            update_overall_tool_download_progress(
                &overall_progress,
                requirements.len(),
                &progress_state_by_name,
                &mut overall_render_state,
            );
            if first_error.is_none() {
                first_error = Some(MediaPmError::Workflow(format!(
                    "tool provisioning worker for '{tool_name}' finished without reporting a result"
                )));
            }
        }
    }

    if let Some(error) = first_error {
        overall_progress.set_message(&format_overall_tool_download_message(
            requirements.len(),
            &progress_state_by_name,
        ));
        settle_progress_renderer_frame().await;
        drop(multi_progress);
        return Err(error);
    }

    overall_progress.set_position(overall_progress_total);
    overall_progress.set_message(&format_overall_tool_download_message(
        requirements.len(),
        &progress_state_by_name,
    ));
    settle_progress_renderer_frame().await;
    drop(multi_progress);

    Ok(provisioned)
}

/// Worker-channel event emitted while one tool is provisioning.
#[derive(Debug)]
enum ProvisionWorkerEvent {
    /// Incremental byte-progress snapshot for one tool.
    Snapshot {
        /// Logical tool name owning this snapshot row.
        tool_name: String,
        /// Download progress reported by downloader callbacks.
        snapshot: DownloadProgressSnapshot,
    },
    /// Terminal success/failure result for one tool worker.
    Finished {
        /// Logical tool name owning this terminal result.
        tool_name: String,
        /// Final provisioning result for this logical tool.
        result: Result<Box<ProvisionedToolPayload>, MediaPmError>,
    },
}

/// Mutable transfer state tracked for one tool progress row.
#[derive(Debug, Clone, Copy, Default)]
struct ToolDownloadProgressState {
    /// Last reported transfer snapshot, if any callback has fired.
    last_snapshot: Option<DownloadProgressSnapshot>,
    /// Whether provisioning reported a terminal worker result.
    completed: bool,
}

/// Cached render state used to avoid writing duplicate aggregate rows.
#[derive(Debug, Clone, Default)]
struct OverallProgressRenderState {
    /// Last position rendered to the aggregate progress bar.
    position: u64,
    /// Last message rendered to the aggregate progress bar.
    message: String,
    /// Whether at least one aggregate render has been emitted.
    initialized: bool,
}

/// Fixed UI scale used for per-tool transfer bars.
const TOOL_PROGRESS_BAR_SCALE: u64 = 10_000;

/// Delay used to allow one managed progress render cycle before teardown.
///
/// `pulsebar::MultiProgress` repaints from a background thread at a fixed
/// interval. Without a short settle delay, the final `set_message` updates
/// for the last completed tool can be dropped during shutdown, leaving stale
/// terminal rows like `3/4 ready`.
const PROGRESS_RENDER_SETTLE_DELAY: Duration = Duration::from_millis(75);

/// Gives the managed progress renderer one final frame to flush updates.
async fn settle_progress_renderer_frame() {
    tokio::time::sleep(PROGRESS_RENDER_SETTLE_DELAY).await;
}

/// Normalizes one downloader snapshot before UI rendering.
///
/// A zero `Content-Length` is treated as unknown (`None`) because some
/// release endpoints report `0` even when payload bytes are later streamed.
/// Known totals clamp `downloaded_bytes` to avoid overrun labels.
#[must_use]
fn normalize_download_progress_snapshot(
    snapshot: DownloadProgressSnapshot,
) -> DownloadProgressSnapshot {
    match snapshot.total_bytes {
        Some(total_bytes) if total_bytes > 0 => DownloadProgressSnapshot {
            downloaded_bytes: snapshot.downloaded_bytes.min(total_bytes),
            total_bytes: Some(total_bytes),
        },
        _ => DownloadProgressSnapshot {
            downloaded_bytes: snapshot.downloaded_bytes,
            total_bytes: None,
        },
    }
}

/// Applies one byte-progress snapshot to a per-tool progress row.
///
/// The bar visualizes percentage while the message shows concrete byte counts
/// (`downloaded / total` when total is known).
fn update_tool_download_progress(
    progress_bar: &ProgressBar,
    tool_name: &str,
    snapshot: DownloadProgressSnapshot,
) {
    if progress_bar.is_finished() {
        return;
    }

    progress_bar.set_message(&format_tool_download_message(tool_name, snapshot));
    progress_bar.set_position(tool_progress_position(snapshot));
}

/// Applies one terminal tool status message without marking pulsebar finished.
///
/// We intentionally avoid `finish_success`/`finish_error` because pulsebar
/// currently appends elapsed duration to finished rows using render-time clock,
/// which makes every concurrent row show the same elapsed suffix.
fn set_tool_completion_progress_row(
    progress_bar: &ProgressBar,
    tool_name: &str,
    snapshot: Option<DownloadProgressSnapshot>,
    status: &str,
) {
    progress_bar.set_position(TOOL_PROGRESS_BAR_SCALE);

    if let Some(snapshot) = snapshot {
        progress_bar
            .set_message(&format_tool_download_completion_message(tool_name, snapshot, status));
    } else {
        progress_bar.set_message(&format!("{tool_name}: {status}"));
    }
}

/// Recomputes aggregate progress row from all tracked tool states.
fn update_overall_tool_download_progress(
    overall_progress: &ProgressBar,
    total_tools: usize,
    progress_state_by_name: &BTreeMap<String, ToolDownloadProgressState>,
    render_state: &mut OverallProgressRenderState,
) {
    let total_progress = TOOL_PROGRESS_BAR_SCALE.saturating_mul(total_tools as u64).max(1);
    let position = progress_state_by_name
        .values()
        .map(|state| {
            if state.completed {
                TOOL_PROGRESS_BAR_SCALE
            } else {
                state.last_snapshot.map_or(0, tool_progress_position)
            }
        })
        .sum::<u64>()
        .min(total_progress);

    let message = format_overall_tool_download_message(total_tools, progress_state_by_name);
    if render_state.initialized
        && render_state.position == position
        && render_state.message == message
    {
        return;
    }

    overall_progress.set_position(position);
    overall_progress.set_message(&message);

    render_state.position = position;
    render_state.message = message;
    render_state.initialized = true;
}

/// Formats the aggregate download row using compact tool-count phases.
#[must_use]
fn format_overall_tool_download_message(
    total_tools: usize,
    progress_state_by_name: &BTreeMap<String, ToolDownloadProgressState>,
) -> String {
    let completed_tools = progress_state_by_name.values().filter(|state| state.completed).count();

    if completed_tools == total_tools {
        return format!("tool downloads: {completed_tools} — ready");
    }

    if completed_tools == 0
        && progress_state_by_name.values().all(|state| state.last_snapshot.is_none())
    {
        return "tool downloads: resolving".to_string();
    }

    format!("tool downloads: {completed_tools}/{total_tools} — downloading")
}

/// Converts a transfer snapshot into the shared fixed-range progress position.
fn tool_progress_position(snapshot: DownloadProgressSnapshot) -> u64 {
    if let Some(total_bytes) = snapshot.total_bytes
        && total_bytes > 0
    {
        let scaled =
            snapshot.downloaded_bytes.saturating_mul(TOOL_PROGRESS_BAR_SCALE) / total_bytes;
        return scaled.min(TOOL_PROGRESS_BAR_SCALE);
    }

    let coarse_position = snapshot.downloaded_bytes / (256_u64 * 1024_u64);
    coarse_position.min(TOOL_PROGRESS_BAR_SCALE.saturating_sub(1))
}

/// Formats one compact downloading label for a tool transfer row.
fn format_tool_download_message(tool_name: &str, snapshot: DownloadProgressSnapshot) -> String {
    let downloaded = format_byte_count(snapshot.downloaded_bytes);
    if let Some(total_bytes) = snapshot.total_bytes {
        let total = format_byte_count(total_bytes);
        return format!("{tool_name}: {downloaded} / {total} — downloading");
    }

    format!("{tool_name}: {downloaded} — downloading")
}

/// Formats one compact completion label for a tool transfer row.
fn format_tool_download_completion_message(
    tool_name: &str,
    snapshot: DownloadProgressSnapshot,
    status: &str,
) -> String {
    let downloaded = format_byte_count(snapshot.downloaded_bytes);
    format!("{tool_name}: {downloaded} — {status}")
}

/// Formats one byte count using binary-size units for concise progress labels.
fn format_byte_count(bytes: u64) -> String {
    const UNITS: [&str; 5] = ["B", "KiB", "MiB", "GiB", "TiB"];

    if bytes < 1024 {
        return format!("{bytes} B");
    }

    let mut value_tenths = u128::from(bytes) * 10;
    let mut unit_index = 0_usize;
    while value_tenths >= 10 * 1024 && unit_index + 1 < UNITS.len() {
        value_tenths = (value_tenths + 512) / 1024;
        unit_index += 1;
    }

    let whole = value_tenths / 10;
    let fractional = value_tenths % 10;
    format!("{whole}.{fractional} {}", UNITS[unit_index])
}

/// Returns true when tag-only requirements should skip remote update checks.
fn should_skip_tag_update_check(
    requirement: &ToolRequirement,
    tool_name: &str,
    lock: &MediaLockFile,
    machine: &MachineNickelDocument,
    check_tag_updates: bool,
) -> bool {
    if check_tag_updates || !is_tag_only_requirement(requirement) {
        return false;
    }

    if tool_catalog_entry(tool_name)
        .ok()
        .is_some_and(|entry| matches!(entry.download, ToolDownloadDescriptor::InternalLauncher))
    {
        return false;
    }

    let Some(active_tool_id) = lock.active_tools.get(tool_name) else {
        return false;
    };

    let Some(tool_spec) = machine.tools.get(active_tool_id) else {
        return false;
    };
    let Some(tool_config) = machine.tool_configs.get(active_tool_id) else {
        return false;
    };
    let Some(content_map) = tool_config.content_map.as_ref() else {
        return false;
    };

    let ToolKindSpec::Executable { command, .. } = &tool_spec.kind else {
        return false;
    };

    validate_tool_command(tool_name, command, content_map).is_ok()
}

/// Returns true when one requirement selects only by moving tag.
fn is_tag_only_requirement(requirement: &ToolRequirement) -> bool {
    requirement.normalized_tag().is_some() && requirement.normalized_version().is_none()
}

/// Returns true when one logical tool requirement targets a builtin
/// source-ingest step tool that is not downloader-provisioned.
#[must_use]
fn is_builtin_source_ingest_requirement(tool_name: &str) -> bool {
    tool_name.eq_ignore_ascii_case("import")
}

/// Removes stale managed tool artifacts that are not declared in `mediapm.ncl`.
async fn prune_unmanaged_tool_artifacts(
    paths: &MediaPmPaths,
    document: &MediaPmDocument,
    cas: &FileSystemCas,
    machine: &mut MachineNickelDocument,
    lock: &mut MediaLockFile,
    desired_tool_ids: &BTreeSet<String>,
    report: &mut ToolSyncReport,
) -> Result<(), MediaPmError> {
    let desired_logical_names = document.tools.keys().cloned().collect::<BTreeSet<_>>();

    let stale_registry_ids = lock
        .tool_registry
        .iter()
        .filter_map(|(tool_id, record)| {
            let still_declared = desired_logical_names.contains(&record.name);
            let still_active = desired_tool_ids.contains(tool_id);
            if still_declared && still_active { None } else { Some(tool_id.clone()) }
        })
        .collect::<BTreeSet<_>>();

    for stale_tool_id in &stale_registry_ids {
        let removed_hashes = machine
            .tool_configs
            .remove(stale_tool_id)
            .and_then(|config| config.content_map)
            .map(|map| map.into_values().collect::<Vec<_>>())
            .unwrap_or_default();

        for hash in removed_hashes {
            if cas.exists(hash).await.unwrap_or(false) {
                let _ = cas.delete(hash).await;
            }
        }

        let artifact_dir = paths.tools_dir.join(stale_tool_id);
        if artifact_dir.exists() {
            fs::remove_dir_all(&artifact_dir).map_err(|source| MediaPmError::Io {
                operation: format!(
                    "removing unmanaged workspace-local tool artifacts for '{stale_tool_id}'"
                ),
                path: artifact_dir.clone(),
                source,
            })?;
        }

        if let Some(entry) = lock.tool_registry.get_mut(stale_tool_id) {
            entry.status = ToolRegistryStatus::Pruned;
            entry.last_transition_unix_seconds = now_unix_seconds();
        }

        report.warnings.push(format!("pruned unmanaged tool artifacts for '{stale_tool_id}'"));
    }

    let stale_active_names = lock
        .active_tools
        .iter()
        .filter_map(|(logical_name, active_tool_id)| {
            if desired_logical_names.contains(logical_name)
                && desired_tool_ids.contains(active_tool_id)
            {
                None
            } else {
                Some(logical_name.clone())
            }
        })
        .collect::<Vec<_>>();
    for logical_name in stale_active_names {
        lock.active_tools.remove(&logical_name);
    }

    if paths.tools_dir.exists() {
        for entry in fs::read_dir(&paths.tools_dir).map_err(|source| MediaPmError::Io {
            operation: "enumerating managed tools directory for prune".to_string(),
            path: paths.tools_dir.clone(),
            source,
        })? {
            let entry = entry.map_err(|source| MediaPmError::Io {
                operation: "reading managed tools directory entry for prune".to_string(),
                path: paths.tools_dir.clone(),
                source,
            })?;
            if !entry.file_type().map(|ty| ty.is_dir()).unwrap_or(false) {
                continue;
            }

            let directory_name = entry.file_name().to_string_lossy().to_string();
            if !directory_name.contains('@') {
                continue;
            }
            if desired_tool_ids.contains(&directory_name) {
                continue;
            }

            let remove_path = entry.path();
            fs::remove_dir_all(&remove_path).map_err(|source| MediaPmError::Io {
                operation: format!("removing unmanaged tool install directory '{directory_name}'"),
                path: remove_path.clone(),
                source,
            })?;

            report.warnings.push(format!("removed unmanaged tool directory '{directory_name}'"));
        }
    }

    Ok(())
}

/// Resolves lockfile version label from provisioned identity metadata.
fn lock_registry_version(provisioned: &ProvisionedToolPayload) -> Result<String, MediaPmError> {
    if let Some(hash) =
        provisioned.identity.git_hash.as_deref().map(str::trim).filter(|value| !value.is_empty())
    {
        return Ok(hash.to_string());
    }

    if let Some(version) =
        provisioned.identity.version.as_deref().map(str::trim).filter(|value| !value.is_empty())
    {
        return Ok(version.to_string());
    }

    if let Some(tag) =
        provisioned.identity.tag.as_deref().map(str::trim).filter(|value| !value.is_empty())
    {
        return Ok(tag.to_string());
    }

    Err(MediaPmError::Workflow(format!(
        "tool '{}' resolved with no git hash, version, or tag; lockfile tool registry requires one immutable selector",
        provisioned.catalog.name
    )))
}

/// Ensures internal launcher payload files exist before CAS import.
///
/// Some environments can remove non-host launcher files between provisioning
/// and CAS import. Internal launchers are deterministic, so missing files are
/// regenerated from their known relative content-map keys.
fn ensure_internal_launcher_content_entries_exist(
    provisioned: &ProvisionedToolPayload,
    content_entries: &BTreeMap<String, ContentMapSource>,
) -> Result<(), MediaPmError> {
    if !matches!(provisioned.catalog.download, ToolDownloadDescriptor::InternalLauncher) {
        return Ok(());
    }

    for (relative_path, source) in content_entries {
        let ContentMapSource::FilePath(absolute_path) = source else {
            continue;
        };

        if absolute_path.exists() {
            continue;
        }

        if !provisioned.catalog.name.eq_ignore_ascii_case("media-tagger") {
            return Err(MediaPmError::Workflow(format!(
                "internal launcher '{}' is missing payload file '{}' at '{}' and has no regeneration strategy",
                provisioned.catalog.name,
                relative_path,
                absolute_path.display()
            )));
        }

        regenerate_media_tagger_internal_launcher_file(relative_path, absolute_path)?;
    }

    Ok(())
}

/// Regenerates one missing internal media-tagger launcher script file.
fn regenerate_media_tagger_internal_launcher_file(
    relative_path: &str,
    absolute_path: &Path,
) -> Result<(), MediaPmError> {
    let normalized = relative_path
        .replace('\\', "/")
        .trim_start_matches("./")
        .trim_start_matches('/')
        .to_string();

    let launcher_env_key = match normalized.as_str() {
        "windows/media-tagger.cmd" => MEDIA_TAGGER_LAUNCHER_MEDIAPM_BIN_WINDOWS_ENV,
        "linux/media-tagger" => MEDIA_TAGGER_LAUNCHER_MEDIAPM_BIN_LINUX_ENV,
        "macos/media-tagger" => MEDIA_TAGGER_LAUNCHER_MEDIAPM_BIN_MACOS_ENV,
        _ => {
            return Err(MediaPmError::Workflow(format!(
                "cannot regenerate internal media-tagger launcher for unsupported path key '{relative_path}'"
            )));
        }
    };

    let content = if normalized.starts_with("windows/") {
        format!(
            concat!(
                "@echo off\r\n",
                "setlocal\r\n",
                "if \"%{launcher_env_key}%\"==\"\" (\r\n",
                "  echo internal media-tagger launcher requires %{launcher_env_key}% to be set>&2\r\n",
                "  exit /b 1\r\n",
                ")\r\n",
                "\"%{launcher_env_key}%\" builtins media-tagger %*\r\n"
            ),
            launcher_env_key = launcher_env_key,
        )
    } else {
        format!(
            concat!(
                "#!/usr/bin/env sh\n",
                "if [ -z \"${launcher_env_key}\" ]; then\n",
                "  printf '%s\\n' \"internal media-tagger launcher requires {launcher_env_key} to be set\" >&2\n",
                "  exit 1\n",
                "fi\n",
                "exec \"${launcher_env_key}\" builtins media-tagger \"$@\"\n"
            ),
            launcher_env_key = launcher_env_key,
        )
    };

    if let Some(parent) = absolute_path.parent() {
        fs::create_dir_all(parent).map_err(|source| MediaPmError::Io {
            operation: "creating internal launcher parent directory during regeneration"
                .to_string(),
            path: parent.to_path_buf(),
            source,
        })?;
    }

    fs::write(absolute_path, content.as_bytes()).map_err(|source| MediaPmError::Io {
        operation: "writing regenerated internal launcher payload".to_string(),
        path: absolute_path.to_path_buf(),
        source,
    })?;

    #[cfg(unix)]
    {
        use std::os::unix::fs::PermissionsExt;

        let mut permissions = fs::metadata(absolute_path)
            .map_err(|source| MediaPmError::Io {
                operation: "reading regenerated internal launcher metadata".to_string(),
                path: absolute_path.to_path_buf(),
                source,
            })?
            .permissions();
        permissions.set_mode(0o755);
        fs::set_permissions(absolute_path, permissions).map_err(|source| MediaPmError::Io {
            operation: "setting regenerated internal launcher executable permissions".to_string(),
            path: absolute_path.to_path_buf(),
            source,
        })?;
    }

    Ok(())
}

/// Imports materialized tool payload files into conductor CAS.
async fn import_tool_content_files_into_cas(
    cas: &FileSystemCas,
    content_entries: &BTreeMap<String, ContentMapSource>,
) -> Result<BTreeMap<String, Hash>, MediaPmError> {
    let mut map = BTreeMap::new();
    let mut source_hash_cache = BTreeMap::<ContentMapSourceCacheKey, Hash>::new();

    for (relative_path, entry) in content_entries {
        let hash = import_tool_content_source_into_cas(
            cas,
            relative_path.as_str(),
            entry,
            &mut source_hash_cache,
        )
        .await?;
        map.insert(relative_path.clone(), hash);
    }

    Ok(map)
}

/// Cache key for one materialized content-map source import.
#[derive(Debug, Clone, PartialEq, Eq, PartialOrd, Ord)]
enum ContentMapSourceCacheKey {
    /// Raw file payload imported directly from one absolute path.
    FilePath(PathBuf),
    /// Directory payload imported as one deterministic uncompressed ZIP blob.
    DirectoryZip(PathBuf),
}

/// Returns one stable source-cache key for content-map deduplication.
fn content_map_source_cache_key(source: &ContentMapSource) -> ContentMapSourceCacheKey {
    match source {
        ContentMapSource::FilePath(path) => ContentMapSourceCacheKey::FilePath(path.clone()),
        ContentMapSource::DirectoryZip { root_dir } => {
            ContentMapSourceCacheKey::DirectoryZip(root_dir.clone())
        }
    }
}

/// Imports one content-map source into CAS with per-pass source-hash caching.
///
/// Blocking file I/O is offloaded to `spawn_blocking` so the async executor
/// remains available for progress rendering and other tasks while large tool
/// payloads (e.g. ffmpeg directory ZIPs) are read and serialized.
async fn import_tool_content_source_into_cas(
    cas: &FileSystemCas,
    relative_path: &str,
    source: &ContentMapSource,
    source_hash_cache: &mut BTreeMap<ContentMapSourceCacheKey, Hash>,
) -> Result<Hash, MediaPmError> {
    let cache_key = content_map_source_cache_key(source);
    if let Some(hash) = source_hash_cache.get(&cache_key) {
        return Ok(*hash);
    }

    let bytes = match source {
        ContentMapSource::FilePath(absolute_path) => {
            let path = absolute_path.clone();
            tokio::task::spawn_blocking(move || {
                fs::read(&path).map_err(|source| MediaPmError::Io {
                    operation: format!(
                        "reading tool payload file '{}' before CAS import",
                        path.display()
                    ),
                    path: path.clone(),
                    source,
                })
            })
            .await
            .map_err(|e| {
                MediaPmError::Workflow(format!("tool payload file read task panicked: {e}"))
            })??
        }
        ContentMapSource::DirectoryZip { root_dir } => {
            let dir = root_dir.clone();
            tokio::task::spawn_blocking(move || build_uncompressed_zip_bytes_from_directory(&dir))
                .await
                .map_err(|e| {
                    MediaPmError::Workflow(format!("tool payload directory ZIP task panicked: {e}"))
                })??
        }
    };

    let hash = cas.put(bytes).await.map_err(|source| {
        MediaPmError::Workflow(format!(
            "importing tool payload entry '{relative_path}' into CAS failed: {source}",
        ))
    })?;
    source_hash_cache.insert(cache_key, hash);

    Ok(hash)
}

/// Serializes one directory tree as an uncompressed ZIP payload.
///
/// This encoding keeps conductor `content_map` compact for archive-style tools:
/// one folder key can carry a complete tool payload without one hash per file.
fn build_uncompressed_zip_bytes_from_directory(root_dir: &Path) -> Result<Vec<u8>, MediaPmError> {
    if !root_dir.exists() || !root_dir.is_dir() {
        return Err(MediaPmError::Workflow(format!(
            "cannot build ZIP payload: '{}' is not a directory",
            root_dir.display()
        )));
    }

    let mut files = Vec::<PathBuf>::new();
    let mut stack = vec![root_dir.to_path_buf()];
    while let Some(next) = stack.pop() {
        let entries = fs::read_dir(&next).map_err(|source| MediaPmError::Io {
            operation: "enumerating tool payload directory for ZIP serialization".to_string(),
            path: next.clone(),
            source,
        })?;

        for entry in entries {
            let entry = entry.map_err(|source| MediaPmError::Io {
                operation: "reading tool payload directory entry for ZIP serialization".to_string(),
                path: next.clone(),
                source,
            })?;
            let path = entry.path();
            let ty = entry.file_type().map_err(|source| MediaPmError::Io {
                operation: "reading tool payload entry type for ZIP serialization".to_string(),
                path: path.clone(),
                source,
            })?;

            if ty.is_dir() {
                stack.push(path);
            } else if ty.is_file() {
                files.push(path);
            }
        }
    }

    files.sort();

    let mut buffer = Cursor::new(Vec::new());
    let mut zip = zip::ZipWriter::new(&mut buffer);
    let options = zip::write::SimpleFileOptions::default()
        .compression_method(zip::CompressionMethod::Stored)
        .unix_permissions(0o644);

    for path in files {
        let relative = path.strip_prefix(root_dir).map_err(|_| {
            MediaPmError::Workflow(format!(
                "failed deriving ZIP entry path from '{}' under '{}'",
                path.display(),
                root_dir.display()
            ))
        })?;
        let entry_name = relative.to_string_lossy().replace('\\', "/");

        zip.start_file(entry_name, options).map_err(|source| {
            MediaPmError::Workflow(format!(
                "creating ZIP entry for '{}' failed: {source}",
                path.display()
            ))
        })?;

        let bytes = fs::read(&path).map_err(|source| MediaPmError::Io {
            operation: "reading tool payload file for ZIP serialization".to_string(),
            path: path.clone(),
            source,
        })?;
        zip.write_all(&bytes).map_err(|source| {
            MediaPmError::Workflow(format!(
                "writing ZIP entry bytes for '{}' failed: {source}",
                path.display()
            ))
        })?;
    }

    zip.finish().map_err(|source| {
        MediaPmError::Workflow(format!(
            "finalizing uncompressed ZIP payload for '{}' failed: {source}",
            root_dir.display()
        ))
    })?;

    Ok(buffer.into_inner())
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
    let tool_artifact_dir = paths.tools_dir.join(tool_id);
    let removed_workspace_artifacts = if tool_artifact_dir.exists() {
        fs::remove_dir_all(&tool_artifact_dir).map_err(|source| MediaPmError::Io {
            operation: format!("removing workspace-local tool artifacts for '{tool_id}'"),
            path: tool_artifact_dir.clone(),
            source,
        })?;
        1
    } else {
        0
    };

    if removed_hashes.is_empty()
        && !machine.tools.contains_key(tool_id)
        && removed_workspace_artifacts == 0
    {
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

    Ok(removed_hashes.len() + removed_workspace_artifacts)
}

#[cfg(test)]
mod tests {
    use mediapm_conductor::{ToolConfigSpec, ToolSpec};

    use crate::tools::catalog::{
        DownloadPayloadMode, PlatformValue, ToolCatalogEntry, ToolDownloadDescriptor,
        tool_catalog_entry,
    };

    use super::*;

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

        let lock = MediaLockFile {
            active_tools: BTreeMap::from([(
                "media-tagger".to_string(),
                "mediapm.tools.media-tagger+mediapm-internal@latest".to_string(),
            )]),
            ..MediaLockFile::default()
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

    /// Verifies tag-only skip mode is disabled when the active executable tool
    /// row is missing non-host platform payload keys.
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

        let lock = MediaLockFile {
            active_tools: BTreeMap::from([("ffmpeg".to_string(), active_tool_id.clone())]),
            ..MediaLockFile::default()
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

        let lock = MediaLockFile {
            active_tools: BTreeMap::from([("ffmpeg".to_string(), active_tool_id.clone())]),
            ..MediaLockFile::default()
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

    /// Verifies media-tagger managed ids include selected ffmpeg selector
    /// identity to invalidate stale launcher rows when ffmpeg changes.
    #[test]
    fn media_tagger_tool_id_includes_ffmpeg_selector_fragment() {
        let base_tool_id = "mediapm.tools.media-tagger+mediapm-internal@latest";
        let augmented =
            augment_media_tagger_tool_id_with_ffmpeg_selector(base_tool_id, "blake3:ABC_def");

        assert_eq!(
            augmented,
            "mediapm.tools.media-tagger+mediapm-internal+ffmpeg-blake3-abc-def@latest"
        );
    }

    /// Verifies ffmpeg selector derivation prefers lock registry versions and
    /// falls back to immutable tool-id suffixes when registry rows are absent.
    #[test]
    fn ffmpeg_selector_resolution_uses_registry_then_tool_id_suffix() {
        let mut lock = MediaLockFile::default();
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
            &MediaLockFile::default(),
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

    /// Verifies media-tagger ffmpeg env path prefers absolute managed-tool binary
    /// paths when the selected companion tool is installed locally.
    #[test]
    fn resolve_managed_tool_command_absolute_path_prefers_installed_tool_binary() {
        let temp = tempfile::tempdir().expect("tempdir");
        let paths = MediaPmPaths::from_root(temp.path());
        let tool_id = "mediapm.tools.ffmpeg+github-releases-btbn-ffmpeg-builds@v7.1";
        let relative = "windows/bin/ffmpeg.exe";

        let absolute = paths.tools_dir.join(tool_id).join(relative);
        std::fs::create_dir_all(absolute.parent().expect("parent dir")).expect("mkdirs");
        std::fs::write(&absolute, b"ffmpeg").expect("write fake ffmpeg binary");

        let resolved = resolve_managed_tool_command_absolute_path(&paths, Some(tool_id), relative)
            .expect("absolute path");

        assert_eq!(resolved, absolute.to_string_lossy().replace('\\', "/"));
    }

    /// Verifies media-tagger namespaced ffmpeg selectors resolve to the same
    /// installed managed-tool binary path.
    #[test]
    fn resolve_managed_tool_command_absolute_path_accepts_media_tagger_namespaced_paths() {
        let temp = tempfile::tempdir().expect("tempdir");
        let paths = MediaPmPaths::from_root(temp.path());
        let tool_id = "mediapm.tools.ffmpeg+github-releases-btbn-ffmpeg-builds@v7.1";
        let installed_relative = "windows/bin/ffmpeg.exe";
        let namespaced_relative = "ffmpeg/windows/bin/ffmpeg.exe";

        let absolute = paths.tools_dir.join(tool_id).join(installed_relative);
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
        let requirement = ToolRequirement {
            version: None,
            tag: Some("latest".to_string()),
            dependencies: crate::config::ToolRequirementDependencies {
                ffmpeg_version: Some("v7.1".to_string()),
                sd_version: None,
            },
            recheck_seconds: None,
            max_input_slots: None,
            max_output_slots: None,
        };

        let mut lock = MediaLockFile::default();
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
            "yt-dlp",
            &requirement,
            &BTreeMap::new(),
            &lock,
            &machine,
        )
        .expect("companion selection should succeed")
        .expect("selection should be present");

        assert!(selection.provisioned_content_entries.is_empty());
        assert!(selection.existing_content_map.contains_key("windows/ffmpeg/bin/ffmpeg.exe"));
        assert_eq!(selection.host_command_path.as_deref(), Some("windows/ffmpeg/bin/ffmpeg.exe"));
    }

    /// Verifies explicit yt-dlp companion ffmpeg selectors fail fast when no
    /// managed ffmpeg identity matches the requested selector.
    #[test]
    fn companion_ffmpeg_selection_rejects_unknown_selector() {
        let requirement = ToolRequirement {
            version: None,
            tag: Some("latest".to_string()),
            dependencies: crate::config::ToolRequirementDependencies {
                ffmpeg_version: Some("v9.9".to_string()),
                sd_version: None,
            },
            recheck_seconds: None,
            max_input_slots: None,
            max_output_slots: None,
        };

        let error = resolve_companion_ffmpeg_selection(
            "yt-dlp",
            &requirement,
            &BTreeMap::new(),
            &MediaLockFile::default(),
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
}
