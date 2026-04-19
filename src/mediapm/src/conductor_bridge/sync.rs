//! Desired-tool reconciliation and prune flows for Phase 3.

use std::collections::{BTreeMap, BTreeSet};
use std::fs;
use std::io::{Cursor, Write};
use std::path::Path;
use std::path::PathBuf;
use std::sync::Arc;
use std::time::Duration;

use mediapm_cas::{CasApi, FileSystemCas, Hash};
use mediapm_conductor::{AddToolOptions, MachineNickelDocument, ToolKindSpec};
use pulsebar::{MultiProgress, ProgressBar};

use crate::config::{MediaPmDocument, ToolRequirement};
use crate::error::MediaPmError;
use crate::lockfile::{MediaLockFile, ToolRegistryRecord, ToolRegistryStatus};
use crate::paths::MediaPmPaths;
use crate::tools::catalog::{ToolDownloadDescriptor, tool_catalog_entry};
use crate::tools::downloader::{
    ContentMapSource, DownloadProgressCallback, DownloadProgressSnapshot, ProvisionedToolPayload,
    ToolDownloadCache, default_global_tool_cache_root, provision_tool_payload,
};

use super::ToolSyncReport;
use super::documents::{ensure_conductor_documents, load_machine_document, save_machine_document};
use super::runtime_storage::resolve_cas_store_path;
use super::tool_runtime::{
    MEDIA_TAGGER_LAUNCHER_MEDIAPM_BIN_LINUX_ENV, MEDIA_TAGGER_LAUNCHER_MEDIAPM_BIN_MACOS_ENV,
    MEDIA_TAGGER_LAUNCHER_MEDIAPM_BIN_WINDOWS_ENV, build_tool_env, build_tool_spec,
    default_tool_config_description, merge_tool_config_defaults, resolve_ffmpeg_slot_limits,
    validate_tool_command,
};
use super::util::now_unix_seconds;

/// Reconciles desired tools from `mediapm.ncl` into conductor machine config.
pub(crate) async fn reconcile_desired_tools(
    paths: &MediaPmPaths,
    document: &MediaPmDocument,
    inherited_env_vars: &[String],
    lock: &mut MediaLockFile,
    check_tag_updates: bool,
    use_user_download_cache: bool,
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
        if should_skip_tag_update_check(requirement, tool_name, lock, &machine, check_tag_updates)
            && let Some(active_tool_id) = lock.active_tools.get(tool_name).cloned()
        {
            skipped_tag_update_tool_ids.insert(tool_name.clone(), active_tool_id);
            continue;
        }

        requirements_to_provision.insert(tool_name.clone(), requirement.clone());
    }

    let shared_download_cache = if use_user_download_cache {
        match default_global_tool_cache_root() {
            Some(cache_root) => match ToolDownloadCache::open(&cache_root).await {
                Ok(cache) => {
                    let _ = cache.prune_expired_entries().await;
                    Some(Arc::new(cache))
                }
                Err(error) => {
                    report
                        .warnings
                        .push(format!("shared global user tool-cache disabled: {error}"));
                    None
                }
            },
            None => {
                report
                    .warnings
                    .push("shared global user tool-cache disabled: global user directory could not be resolved".to_string());
                None
            }
        }
    } else {
        None
    };

    let mut provisioned_by_name = provision_desired_tools_concurrently(
        paths,
        &requirements_to_provision,
        shared_download_cache,
    )
    .await?;
    let mut desired_tool_ids = BTreeSet::new();

    for name in document.tools.keys() {
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
        let desired_tool_id = provisioned.tool_id.clone();
        desired_tool_ids.insert(desired_tool_id.clone());
        let desired_version = lock_registry_version(&provisioned)?;
        let existing_active = lock.active_tools.get(name).cloned();
        let spec = build_tool_spec(paths, name, &provisioned, ffmpeg_slot_limits)?;
        let command_vector = match &spec.kind {
            ToolKindSpec::Executable { command, .. } => command.clone(),
            ToolKindSpec::Builtin { .. } => {
                return Err(MediaPmError::Workflow(format!(
                    "managed tool '{name}' unexpectedly resolved to builtin spec"
                )));
            }
        };
        let content_map =
            import_tool_content_files_into_cas(&cas, &provisioned.content_entries).await?;
        validate_tool_command(name, &command_vector, &content_map)?;
        let mut desired_config = merge_tool_config_defaults(
            machine.tool_configs.get(&desired_tool_id),
            name,
            content_map,
            default_tool_config_description(
                name,
                &provisioned.identity,
                provisioned.catalog.description,
            ),
            ffmpeg_slot_limits,
        );
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

/// Provisions all desired tools concurrently and reports completion with pulsebar.
///
/// This keeps network transfer concurrency while rendering one progress row per
/// logical tool so users can see byte-level status without mixed output.
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
                worker_progress.set_message(&format!("{worker_tool_name}: resolving release"));

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
                state.last_snapshot.map(tool_progress_position).unwrap_or(0)
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

/// Formats the aggregate download row using completed-tool counts and bytes.
#[must_use]
fn format_overall_tool_download_message(
    total_tools: usize,
    progress_state_by_name: &BTreeMap<String, ToolDownloadProgressState>,
) -> String {
    let completed_tools = progress_state_by_name.values().filter(|state| state.completed).count();

    let mut downloaded_bytes_total = 0_u64;
    let mut total_bytes_all_known = true;
    let mut total_bytes_total = 0_u64;

    for state in progress_state_by_name.values() {
        if let Some(snapshot) = state.last_snapshot {
            downloaded_bytes_total =
                downloaded_bytes_total.saturating_add(snapshot.downloaded_bytes);
            if let Some(total_bytes) = snapshot.total_bytes {
                total_bytes_total = total_bytes_total.saturating_add(total_bytes);
            } else {
                total_bytes_all_known = false;
            }
        } else {
            total_bytes_all_known = false;
        }
    }

    let downloaded_label = format_byte_count(downloaded_bytes_total);
    if total_bytes_all_known {
        return format!(
            "tool downloads: {completed_tools}/{total_tools} ready • {downloaded_label} / {}",
            format_byte_count(total_bytes_total),
        );
    }

    format!("tool downloads: {completed_tools}/{total_tools} ready • {downloaded_label} downloaded")
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

/// Formats one human-readable byte-progress label for a tool transfer row.
fn format_tool_download_message(tool_name: &str, snapshot: DownloadProgressSnapshot) -> String {
    let downloaded = format_byte_count(snapshot.downloaded_bytes);
    if let Some(total_bytes) = snapshot.total_bytes {
        let total = format_byte_count(total_bytes);
        return format!("{tool_name}: {downloaded} / {total}");
    }

    format!("{tool_name}: {downloaded} downloaded")
}

/// Formats one human-readable completion label for a tool transfer row.
fn format_tool_download_completion_message(
    tool_name: &str,
    snapshot: DownloadProgressSnapshot,
    status: &str,
) -> String {
    format!("{} — {status}", format_tool_download_message(tool_name, snapshot))
}

/// Formats one byte count using binary-size units for concise progress labels.
fn format_byte_count(bytes: u64) -> String {
    const UNITS: [&str; 5] = ["B", "KiB", "MiB", "GiB", "TiB"];

    if bytes < 1024 {
        return format!("{bytes} B");
    }

    let mut value = bytes as f64;
    let mut unit_index = 0_usize;
    while value >= 1024.0 && unit_index + 1 < UNITS.len() {
        value /= 1024.0;
        unit_index += 1;
    }

    format!("{value:.1} {}", UNITS[unit_index])
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

    machine.tools.contains_key(active_tool_id)
}

/// Returns true when one requirement selects only by moving tag.
fn is_tag_only_requirement(requirement: &ToolRequirement) -> bool {
    requirement.normalized_tag().is_some() && requirement.normalized_version().is_none()
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

/// Imports materialized tool payload files into conductor CAS.
async fn import_tool_content_files_into_cas(
    cas: &FileSystemCas,
    content_entries: &BTreeMap<String, ContentMapSource>,
) -> Result<BTreeMap<String, Hash>, MediaPmError> {
    let mut map = BTreeMap::new();
    for (relative_path, entry) in content_entries {
        let bytes = match entry {
            ContentMapSource::FilePath(absolute_path) => {
                fs::read(absolute_path).map_err(|source| MediaPmError::Io {
                    operation: format!(
                        "reading tool payload file '{}' before CAS import",
                        absolute_path.display()
                    ),
                    path: absolute_path.clone(),
                    source,
                })?
            }
            ContentMapSource::DirectoryZip { root_dir } => {
                build_uncompressed_zip_bytes_from_directory(root_dir)?
            }
        };

        let hash = cas.put(bytes).await.map_err(|source| {
            MediaPmError::Workflow(format!(
                "importing tool payload entry '{relative_path}' into CAS failed: {source}",
            ))
        })?;
        map.insert(relative_path.clone(), hash);
    }

    Ok(map)
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
    use crate::tools::catalog::{
        DownloadPayloadMode, PlatformValue, ToolCatalogEntry, ToolDownloadDescriptor,
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

    /// Protects message contract by preserving explicit downloaded/total text
    /// for known payload sizes.
    #[test]
    fn format_tool_download_message_reports_known_totals() {
        let message = format_tool_download_message(
            "ffmpeg",
            DownloadProgressSnapshot { downloaded_bytes: 1_024, total_bytes: Some(2_048) },
        );

        assert!(message.contains("ffmpeg:"));
        assert!(message.contains("1.0 KiB / 2.0 KiB"));
    }

    /// Protects fallback transfer messaging for servers that omit
    /// `Content-Length` while bytes are still streamed successfully.
    #[test]
    fn format_tool_download_message_handles_unknown_totals() {
        let message = format_tool_download_message(
            "yt-dlp",
            DownloadProgressSnapshot { downloaded_bytes: 512, total_bytes: None },
        );

        assert_eq!(message, "yt-dlp: 512 B downloaded");
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

    /// Protects aggregate status labels so total row reports ready-count and
    /// total bytes when all tools expose determinate transfer sizes.
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
        assert_eq!(message, "tool downloads: 1/2 ready • 1.5 KiB / 3.0 KiB",);
    }

    /// Protects completion-row labels so successful tools preserve byte totals
    /// and append stable terminal status text.
    #[test]
    fn format_tool_download_completion_message_appends_status() {
        let message = format_tool_download_completion_message(
            "media-tagger",
            DownloadProgressSnapshot { downloaded_bytes: 2_048, total_bytes: Some(4_096) },
            "ready",
        );

        assert_eq!(message, "media-tagger: 2.0 KiB / 4.0 KiB — ready");
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
}
