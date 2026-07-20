//! Tool-reconciliation coordinator.
//!
//! This module orchestrates the full tool-sync lifecycle:
//! 1. Ensure conductor documents exist (generated + state)
//! 2. Load the generated document
//! 3. Fetch desired tool payloads, import to CAS, build content maps
//! 4. Build proper ToolSpec + ToolRuntime for each tool
//! 5. Apply lifecycle transitions (tag updates, launcher files)
//! 6. Write generated runtime env file
/// 7. Save the generated document
pub(crate) mod lifecycle;
pub(crate) mod provision;
pub(crate) mod tool_config;

use std::collections::BTreeMap;
use std::sync::Arc;

use mediapm_cas::{CasApi, Hash};
use mediapm_conductor::ToolRuntime;
use mediapm_conductor::cache_user_level::default_mediapm_user_download_cache_root;
use mediapm_conductor::config::ExternalDataEntry;
use mediapm_conductor::state::OutputSaveMode;

use crate::conductor_bridge::documents::{
    apply_builtin_runtime_defaults, load_conductor_generated_document,
    register_missing_builtin_tools, save_conductor_generated_document,
};
use crate::conductor_bridge::sync::lifecycle::is_builtin_source_ingest_requirement;
use crate::conductor_bridge::sync::provision::fetch_and_import_tool_payload;
use crate::conductor_bridge::sync::tool_config::{
    resolve_companion_deno_selection, resolve_companion_ffmpeg_selection,
    write_generated_runtime_env_file,
};
use crate::conductor_bridge::tool_runtime::{build_tool_spec, resolve_ffmpeg_slot_limits};
use crate::config::ToolRegistryEntry;
use crate::config::defaults;
use crate::error::MediaPmError;
use crate::output::{ProgressBarApi, ProgressGroup, ProgressGroupApi};
use crate::paths::MediaPmPaths;
use crate::tools::downloader::ToolDownloadCache;

/// Summary of one `mediapm tool sync` reconciliation pass.
#[derive(Debug, Clone, Default, PartialEq, Eq)]
pub(crate) struct ToolSyncReport {
    /// Number of tools newly registered.
    pub(crate) tools_added: usize,
    /// Number of tools removed (no longer in desired set).
    pub(crate) tools_removed: usize,
    /// Number of tools updated to match desired version.
    pub(crate) tools_updated: usize,
    /// Non-fatal warnings collected during reconciliation.
    pub(crate) warnings: Vec<String>,
    /// Per-tool deployment records populated during provisioning.
    /// Keyed by tool id (the desired-tools key, not the content-addressed key).
    pub(crate) tool_records: BTreeMap<String, ToolRegistryEntry>,
}

/// Runs the full tool-reconciliation cycle for the current workspace.
///
/// # Errors
///
/// Returns an error when any critical step (document loading, builtin
/// registration, content-map import) fails. Non-critical failures are
/// reported as warnings in [`ToolSyncReport`].
#[allow(clippy::too_many_lines)]
pub(crate) async fn reconcile_desired_tools(
    cas: &impl CasApi,
    paths: &MediaPmPaths,
    desired_tools: &BTreeMap<String, serde_json::Value>,
    inherited_env_vars: &BTreeMap<String, Vec<String>>,
    _check_tag_updates: bool,
    progress_group: Option<&dyn ProgressGroupApi>,
) -> Result<ToolSyncReport, MediaPmError> {
    let mut report = ToolSyncReport::default();

    // 1. Load or create generated document.
    let mut generated_doc = load_conductor_generated_document(paths)?;

    // 2. Register missing builtin tool definitions and config stubs.
    register_missing_builtin_tools(&mut generated_doc);
    apply_builtin_runtime_defaults(&mut generated_doc);

    // 3. Provision desired tools: download payloads, import to CAS, build
    //    content maps and tool specs.
    let mut tool_runtimes: BTreeMap<String, ToolRuntime> = BTreeMap::new();

    // Open or create the tool download cache and tool metadata cache.
    let cache_root = default_mediapm_user_download_cache_root().ok_or_else(|| {
        MediaPmError::Workflow("could not determine default tool cache root".to_string())
    })?;
    let cache = ToolDownloadCache::open(&cache_root, "tools.json", 30 * 24 * 60 * 60)
        .await
        .map_err(|e| MediaPmError::Workflow(format!("failed to open tool download cache: {e}")))?;
    let metadata_cache = ToolDownloadCache::open(&cache_root, "tool_metadata.json", 24 * 60 * 60)
        .await
        .map_err(|e| MediaPmError::Workflow(format!("failed to open tool metadata cache: {e}")))?;

    // Progress bar for the per-tool provisioning loop.
    let total_tools = desired_tools.len() as u64;
    let (owned_group, pb): (Option<ProgressGroup>, Arc<dyn ProgressBarApi>) =
        if let Some(pg) = progress_group {
            (None, pg.add_bar(total_tools, "syncing tools"))
        } else {
            let (g, p) = ProgressGroup::builder()
                .dynamic_height(true)
                .with_overall("syncing tools", total_tools)
                .build_with_overall();
            (Some(g), Arc::new(p))
        };
    let effective_group: &dyn ProgressGroupApi = owned_group
        .as_ref()
        .map(|g| g as &dyn ProgressGroupApi)
        .or(progress_group)
        .expect("at least one progress group available");

    for (_i, (tool_id, _requirement_value)) in desired_tools.iter().enumerate() {
        let is_builtin_code = is_builtin_source_ingest_requirement(tool_id);
        let already_exists = generated_doc.tools.values().any(|s| s.name == *tool_id);

        // Fetch tool payload, import to CAS, get content map + command.
        let payload_result =
            fetch_and_import_tool_payload(cas, tool_id, &cache, &metadata_cache, effective_group)
                .await;

        match payload_result {
            Ok(Some(payload)) => {
                // Compute content-addressed hash from content_map before it's
                // moved into build_tool_spec.
                let content_hash = if payload.content_map.is_empty() {
                    None
                } else {
                    let json = serde_json::to_string(&payload.content_map)
                        .expect("content_map serializes to JSON");
                    Some(blake3::hash(json.as_bytes()).to_hex())
                };

                // Determine ffmpeg slot limits (default for now; overrides
                // from tool requirements can be wired later).
                let ffmpeg_limits = resolve_ffmpeg_slot_limits(
                    defaults::DEFAULT_FFMPEG_MAX_INPUT_SLOTS,
                    defaults::DEFAULT_FFMPEG_MAX_OUTPUT_SLOTS,
                );

                // Build proper spec and runtime.
                let (spec, runtime) = build_tool_spec(
                    tool_id,
                    payload.content_map,
                    &payload.os_exec_paths,
                    ffmpeg_limits,
                );

                if !already_exists && !is_builtin_code {
                    report.tools_added += 1;
                } else {
                    report.tools_updated += 1;
                }

                // Record deployment metadata for the managed-tool registry.
                let now = std::time::SystemTime::now()
                    .duration_since(std::time::UNIX_EPOCH)
                    .unwrap_or_default()
                    .as_secs();
                let req_version = _requirement_value
                    .get("version")
                    .and_then(|v| v.as_str())
                    .unwrap_or("")
                    .to_string();
                let req_tag = _requirement_value
                    .get("tag")
                    .and_then(|v| v.as_str())
                    .unwrap_or("")
                    .to_string();
                report.tool_records.insert(
                    tool_id.clone(),
                    ToolRegistryEntry {
                        version: Some(req_version),
                        tag: if req_tag.is_empty() { None } else { Some(req_tag) },
                        fetch_hash: content_hash.as_ref().map(|h| h.to_string()),
                        deployed_at: now,
                    },
                );

                // Inject inherited_env_vars from requirement config.
                let inherited = inherited_env_vars.get(tool_id).cloned().unwrap_or_default();

                let mut full_runtime = runtime.clone();
                full_runtime.inherited_env_vars = inherited;

                // Use content-addressed key: "{name}@{hash}".
                let tool_key = if let Some(ref hash) = content_hash {
                    format!("{}@{}", tool_id, hash)
                } else {
                    tool_id.to_string()
                };

                // Populate external_data from content_map CAS hashes so the
                // content_map ⊆ external_data invariant is satisfied.
                for hash_str in spec.runtime.content_map.values() {
                    if let Ok(hash) = hash_str.parse::<Hash>() {
                        generated_doc.external_data.entry(hash).or_insert(ExternalDataEntry {
                            description: format!("managed tool content root for {tool_id}"),
                            save_mode: OutputSaveMode::Saved,
                        });
                    }
                }

                generated_doc.tools.insert(tool_key.clone(), spec);
                tool_runtimes.insert(tool_key.clone(), full_runtime);
            }
            Ok(None) => {
                // No payload fetched (internal launcher, no catalog entry,
                // or no host-OS action). Create a minimal spec without
                // content map so the tool is still registered.
                let runtime = ToolRuntime {
                    impure: false,
                    inherited_env_vars: inherited_env_vars
                        .get(tool_id)
                        .cloned()
                        .unwrap_or_default(),
                    ..ToolRuntime::default()
                };
                tool_runtimes.insert(tool_id.clone(), runtime.clone());

                // Record deployment metadata (no payload — builtin or launcher).
                let now = std::time::SystemTime::now()
                    .duration_since(std::time::UNIX_EPOCH)
                    .unwrap_or_default()
                    .as_secs();
                report.tool_records.insert(
                    tool_id.clone(),
                    ToolRegistryEntry {
                        version: None,
                        tag: None,
                        fetch_hash: None,
                        deployed_at: now,
                    },
                );

                if !already_exists && !is_builtin_code {
                    report.tools_added += 1;
                }

                if !generated_doc.tools.contains_key(tool_id) {
                    generated_doc.tools.insert(
                        tool_id.clone(),
                        mediapm_conductor::ToolSpec {
                            name: tool_id.clone(),
                            kind: mediapm_conductor::ToolKindSpec::Executable {
                                command: Vec::new(),
                                env_vars: BTreeMap::new(),
                                success_codes: vec![0],
                            },
                            inputs: BTreeMap::new(),
                            default_inputs: BTreeMap::new(),
                            outputs: BTreeMap::new(),
                            runtime,
                        },
                    );
                } else {
                    report.tools_updated += 1;
                }
            }
            Err(e) => {
                report.warnings.push(format!(
                    "tool {tool_id}: provisioning failed (will retry on next sync): {e}",
                ));
            }
        }

        pb.advance(1);
    }

    if report.warnings.is_empty() {
        pb.finish_success();
    } else {
        pb.finish_error();
    }
    if let Some(g) = owned_group {
        g.join();
    }

    // Companion binding resolution (for ffmpeg/deno selectors).
    let _ffmpeg_selection = resolve_companion_ffmpeg_selection(desired_tools);
    let _deno_selection = resolve_companion_deno_selection(desired_tools);

    // 4. Ensure the tools runtime directory exists.
    std::fs::create_dir_all(&paths.tools_dir).map_err(|source| MediaPmError::Io {
        operation: "creating tools directory".to_string(),
        path: paths.tools_dir.clone(),
        source,
    })?;

    // 5. Write generated runtime env file from tool runtimes.
    write_generated_runtime_env_file(paths, &tool_runtimes)?;

    // 5. Save generated document.
    save_conductor_generated_document(paths, &generated_doc)?;

    Ok(report)
}

#[cfg(test)]
mod tests {
    use std::collections::BTreeMap;

    use mediapm_cas::InMemoryCas;
    use mediapm_conductor::cache_user_level::default_mediapm_user_download_cache_root;
    use mediapm_utils::progress::recording::{ProgressOp, RecordingProgressTracker};

    use super::*;

    #[tokio::test]
    async fn reconcile_desired_tools_records_progress_ops() {
        // The function opens a user-level download cache rooted at the OS
        // cache dir; skip when the cache dir is unavailable (CI containers).
        if default_mediapm_user_download_cache_root().is_none() {
            return;
        }

        let tmp = tempfile::tempdir().unwrap();
        let paths = MediaPmPaths::from_root(tmp.path());
        let tracker = RecordingProgressTracker::new();
        let cas = InMemoryCas::default();

        let result = reconcile_desired_tools(
            &cas,
            &paths,
            &BTreeMap::new(),
            &BTreeMap::new(),
            false,
            Some(&tracker),
        )
        .await;

        assert!(result.is_ok(), "reconcile_desired_tools failed: {:?}", result.err(),);

        let ops = tracker.ops();

        // The overall progress bar is registered via the tracker, so we see
        // exactly one AddBar op.
        let add_bars: Vec<&ProgressOp> =
            ops.iter().filter(|op| matches!(op, ProgressOp::AddBar { .. })).collect();
        assert_eq!(
            add_bars.len(),
            1,
            "expected exactly one AddBar op (overall progress), got {add_bars:?}",
        );

        if let ProgressOp::AddBar { total, label } = &add_bars[0] {
            assert_eq!(*total, 0, "overall bar total should be 0 (indeterminate)");
            assert_eq!(label.as_str(), "syncing tools", "overall bar label mismatch");
        }

        // The overall bar is finished with success after the tool loop.
        let finish_successes: Vec<&ProgressOp> =
            ops.iter().filter(|op| matches!(op, ProgressOp::FinishSuccess { .. })).collect();
        assert_eq!(
            finish_successes.len(),
            1,
            "expected exactly one FinishSuccess op, got {finish_successes:?}",
        );
        assert!(
            matches!(&finish_successes[0], ProgressOp::FinishSuccess { .. }),
            "expected FinishSuccess"
        );
    }
}
