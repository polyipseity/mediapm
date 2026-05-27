use std::collections::{BTreeMap, BTreeSet};
use std::sync::Arc;
use std::time::Duration;

use pulsebar::{MultiProgress, ProgressBar};

use crate::config::ToolRequirement;
use crate::error::MediaPmError;
use crate::paths::MediaPmPaths;
use crate::tools::downloader::{
    DownloadProgressCallback, DownloadProgressSnapshot, ProvisionedToolPayload, ToolDownloadCache,
    provision_tool_payload,
};

/// Provisions all desired tools concurrently and reports completion with pulsebar.
///
/// This keeps network transfer concurrency while rendering one progress row per
/// logical tool so users can see byte-level status without mixed output.
#[expect(
    clippy::too_many_lines,
    reason = "this item intentionally keeps end-to-end control flow together so ordering invariants remain explicit during maintenance"
)]
pub(super) async fn provision_desired_tools_concurrently(
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
pub(super) struct ToolDownloadProgressState {
    /// Last reported transfer snapshot, if any callback has fired.
    pub(super) last_snapshot: Option<DownloadProgressSnapshot>,
    /// Whether provisioning reported a terminal worker result.
    pub(super) completed: bool,
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
pub(super) const TOOL_PROGRESS_BAR_SCALE: u64 = 10_000;

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
pub(super) fn normalize_download_progress_snapshot(
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
pub(super) fn format_overall_tool_download_message(
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
pub(super) fn tool_progress_position(snapshot: DownloadProgressSnapshot) -> u64 {
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
pub(super) fn format_tool_download_message(
    tool_name: &str,
    snapshot: DownloadProgressSnapshot,
) -> String {
    let downloaded = format_byte_count(snapshot.downloaded_bytes);
    if let Some(total_bytes) = snapshot.total_bytes {
        let total = format_byte_count(total_bytes);
        return format!("{tool_name}: {downloaded} / {total} — downloading");
    }

    format!("{tool_name}: {downloaded} — downloading")
}

/// Formats one compact completion label for a tool transfer row.
pub(super) fn format_tool_download_completion_message(
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
