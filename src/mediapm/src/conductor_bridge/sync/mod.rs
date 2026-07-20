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
use std::path::Path;
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
use crate::conductor_bridge::sync::provision::{PreResolveOutcome, fetch_and_import_tool_payload};
use crate::conductor_bridge::sync::tool_config::{
    resolve_companion_deno_selection, resolve_companion_ffmpeg_selection,
    write_generated_runtime_env_file,
};
use crate::conductor_bridge::tool_runtime::{build_tool_spec, resolve_ffmpeg_slot_limits};
use crate::config::defaults;
use crate::config::{MediaPmState, ToolRegistryEntry};
use crate::error::MediaPmError;
use crate::output::{ProgressBarApi, ProgressGroup, ProgressGroupApi};
use crate::paths::MediaPmPaths;
use crate::tools::downloader::ToolDownloadCache;
use crate::tools::provider;

/// Summary of one `mediapm tool sync` reconciliation pass.
#[derive(Debug, Clone, Default, PartialEq, Eq)]
pub(crate) struct ToolSyncReport {
    /// Number of tools newly registered.
    pub(crate) tools_added: usize,
    /// Number of tools removed (no longer in desired set).
    pub(crate) tools_removed: usize,
    /// Number of tools updated to match desired version.
    pub(crate) tools_updated: usize,
    /// Number of tools skipped because their canonical version was already provisioned.
    pub(crate) tools_skipped: usize,
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
    state: &MediaPmState,
    cache_root_override: Option<&Path>,
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
    // Use cache_root_override when provided (for hermetic tests), otherwise
    // fall back to the default OS-level user cache root.
    let cache_root = match cache_root_override {
        Some(root) => root.to_path_buf(),
        None => default_mediapm_user_download_cache_root().ok_or_else(|| {
            MediaPmError::Workflow("could not determine default tool cache root".to_string())
        })?,
    };
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

        // Initialized in the Ok(fetch) arm before the skip check;
        // used in the Ok(None) payload branch below. String::new() is
        // the dead initial value because the assignment in the match
        // arm always runs before any read (other paths `continue`).
        #[allow(unused_assignments)]
        let mut resolved_canonical_version = String::new();
        let pre_resolved = match provider::resolve_tool_fetch(tool_id, Some(&metadata_cache)).await
        {
            Ok((fetch, canonical_version)) => {
                resolved_canonical_version = canonical_version.clone();

                // Check skip: if state has an entry with the same canonical_version
                // AND a non-empty fetch_hash, skip provisioning entirely.
                let should_skip = state.managed_tools.get(tool_id).is_some_and(|existing| {
                    existing.canonical_version == canonical_version && existing.fetch_hash.is_some()
                });

                if should_skip {
                    PreResolveOutcome::Skip { name: tool_id.clone(), version: canonical_version }
                } else {
                    PreResolveOutcome::Resolved(fetch, canonical_version)
                }
            }
            Err(e) => {
                report.warnings.push(format!(
                    "tool {tool_id}: resolve failed (will retry on next sync): {e}",
                ));
                pb.advance(1);
                continue;
            }
        };

        let was_skip = matches!(&pre_resolved, PreResolveOutcome::Skip { .. });
        let payload_result = fetch_and_import_tool_payload(
            cas,
            tool_id,
            &cache,
            &metadata_cache,
            effective_group,
            pre_resolved,
        )
        .await;

        if was_skip {
            report.tools_skipped += 1;
            pb.advance(1);
            continue;
        }

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
                        canonical_version: payload.canonical_version.clone(),
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
                        canonical_version: resolved_canonical_version.clone(),
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
        let tmp = tempfile::tempdir().unwrap();
        let cache_root = tempfile::tempdir().unwrap();
        let paths = MediaPmPaths::from_root(tmp.path());
        let tracker = RecordingProgressTracker::new();
        let cas = InMemoryCas::default();

        let state = MediaPmState::default();
        let result = reconcile_desired_tools(
            &cas,
            &paths,
            &BTreeMap::new(),
            &BTreeMap::new(),
            false,
            &state,
            Some(cache_root.path()),
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

    #[tokio::test]
    async fn reconcile_desired_tools_with_override_does_not_touch_real_cache() {
        let tmp = tempfile::tempdir().unwrap();
        let cache_root = tempfile::tempdir().unwrap();
        let paths = MediaPmPaths::from_root(tmp.path());
        let cas = InMemoryCas::default();

        // Record real cache state before the call.
        let real_cache_mtime = default_mediapm_user_download_cache_root()
            .and_then(|p| std::fs::metadata(p.join("tools.json")).ok())
            .and_then(|m| m.modified().ok());

        let state = MediaPmState::default();
        let result = reconcile_desired_tools(
            &cas,
            &paths,
            &BTreeMap::new(),
            &BTreeMap::new(),
            false,
            &state,
            Some(cache_root.path()),
            None,
        )
        .await;

        assert!(result.is_ok(), "reconcile_desired_tools failed: {:?}", result.err());
        let report = result.unwrap();
        assert_eq!(report.tools_added, 0, "no tools should be added");
        assert_eq!(report.tools_updated, 0, "no tools should be updated");
        assert_eq!(report.tools_skipped, 0, "no tools should be skipped");
        assert!(report.warnings.is_empty(), "no warnings expected: {:?}", report.warnings);

        // Verify the override path was used (cache files initialized there).
        assert!(
            cache_root.path().join("tools.json").exists()
                || cache_root.path().join("store").exists(),
            "override cache dir should have been initialized",
        );

        // Verify the real cache was not modified by the call (mtime unchanged).
        let real_cache_mtime_after = default_mediapm_user_download_cache_root()
            .and_then(|p| std::fs::metadata(p.join("tools.json")).ok())
            .and_then(|m| m.modified().ok());
        assert_eq!(
            real_cache_mtime, real_cache_mtime_after,
            "real cache directory must not be modified when cache_root_override is set",
        );
    }

    #[tokio::test]
    async fn reconcile_desired_tools_cache_override_supports_explicit_paths() {
        let tmp = tempfile::tempdir().unwrap();
        let cache_root = tempfile::tempdir().unwrap();
        let paths = MediaPmPaths::from_root(tmp.path());
        let cas = InMemoryCas::default();

        // Pre-populate the cache dir with an empty store/ dir so the CAS
        // opens cleanly at the override path.
        std::fs::create_dir_all(cache_root.path().join("store")).unwrap();

        let state = MediaPmState::default();
        let result = reconcile_desired_tools(
            &cas,
            &paths,
            &BTreeMap::new(),
            &BTreeMap::new(),
            false,
            &state,
            Some(cache_root.path()),
            None,
        )
        .await;

        assert!(
            result.is_ok(),
            "reconcile_desired_tools with pre-populated cache dir failed: {:?}",
            result.err()
        );
        let report = result.unwrap();
        assert!(report.warnings.is_empty(), "no warnings expected: {:?}", report.warnings);
    }
}
