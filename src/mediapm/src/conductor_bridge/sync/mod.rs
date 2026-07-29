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
pub(crate) mod external_data;
pub(crate) mod lifecycle;
pub(crate) mod provision;

use std::collections::{BTreeMap, HashSet};
use std::path::Path;
use std::sync::Arc;

use mediapm_cas::{CasApi, Hash};
use mediapm_conductor::ToolRuntime;
use mediapm_conductor::cache::Cache;
use mediapm_conductor::cache::CacheDomainConfig;
use mediapm_conductor::cache_user_level::default_mediapm_user_download_cache_root;
use mediapm_conductor::config::ExternalDataEntry;
use mediapm_conductor::provision::retain_only_tool_dirs;
use mediapm_conductor::runtime_env::write_generated_dotenv;
use mediapm_conductor::state::OutputSaveMode;
use mediapm_conductor::tools::provider::VersionSpec;
use mediapm_conductor::tools::spec::spec_matches_entry;

use crate::tools::provider::RecheckPolicy;

use crate::conductor_bridge::documents::{
    apply_builtin_runtime_defaults, load_conductor_generated_document,
    register_missing_builtin_tools, save_conductor_generated_document,
};
use crate::conductor_bridge::sync::lifecycle::is_builtin_source_ingest_requirement;
use crate::conductor_bridge::sync::provision::{PreResolveOutcome, fetch_and_import_tool_payload};

use crate::conductor_bridge::tool_runtime::{build_tool_spec, resolve_ffmpeg_slot_limits};
use crate::config::ToolRequirement;
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
    /// Number of old-version tool entries pruned from machine config.
    pub(crate) pruned_tools: usize,
    /// Non-fatal warnings collected during reconciliation.
    pub(crate) warnings: Vec<String>,
    /// Per-tool deployment records populated during provisioning.
    /// Keyed by tool id (the desired-tools key, not the content-addressed key).
    pub(crate) tool_records: BTreeMap<String, ToolRegistryEntry>,
}

/// Compute the set of tool IDs that are needed, seeded from ALL desired tools
/// plus their transitive dependencies via `dependencies` field.
///
/// Every tool listed in the mediapm config's `tools` section is considered
/// "used". Additional tools that appear only as dependencies of configured
/// tools are also included (transitive closure).
///
/// Pruning only removes `content_map` entries for older versions of tools
/// that are superseded by a newer content-addressed key (the old key is
/// removed from the generated doc). A tool NOT in this set also has its
/// filesystem payloads removed via `retain_only_tool_dirs`. Under normal
/// operation every desired tool is in this set, so the payload-prune
/// branch never fires for actively-configured tools.
#[must_use]
pub(crate) fn compute_used_tool_ids(
    desired_tools: &BTreeMap<String, serde_json::Value>,
) -> HashSet<String> {
    let mut used = HashSet::new();
    let mut stack: Vec<String> = desired_tools.keys().cloned().collect();
    while let Some(tool_id) = stack.pop() {
        if !used.insert(tool_id.clone()) {
            continue;
        }
        if let Some(value) = desired_tools.get(&tool_id) {
            if let Ok(req) = serde_json::from_value::<ToolRequirement>(value.clone()) {
                for dep_id in req.dependencies.keys() {
                    if !used.contains(dep_id.as_str()) {
                        stack.push(dep_id.clone());
                    }
                }
            }
        }
    }
    used
}

/// Resolve a dependency's effective version spec, handling "inherit".
///
/// - `VersionSpec::Inherit` → look up the dependency tool's global
///   `version_spec` in `global_requirements` and use that.
/// - `VersionSpec::Exact(...)` / `VersionSpec::Latest` → use as-is.
///
/// Returns error when dep tool is missing and `Inherit` is requested,
/// or when a circular inherit is detected (the dep tool itself has
/// `version_spec: Inherit`).
///
/// # Errors
///
/// Returns [`MediaPmError::Workflow`] when inherit cannot be resolved.
#[allow(dead_code)]
pub(crate) fn resolve_dep_version_spec(
    dep_spec: &VersionSpec,
    dep_tool_id: &str,
    global_requirements: &BTreeMap<String, ToolRequirement>,
) -> Result<VersionSpec, MediaPmError> {
    match dep_spec {
        VersionSpec::Inherit => {
            let global = global_requirements.get(dep_tool_id).ok_or_else(|| {
                MediaPmError::Workflow(format!(
                    "tool dependency '{dep_tool_id}' has 'inherit' version spec \
                     but the tool is not configured in the tools section"
                ))
            })?;
            // Also error if the global tool itself has "inherit" (circular).
            if global.version_spec == VersionSpec::Inherit {
                return Err(MediaPmError::Workflow(format!(
                    "tool '{dep_tool_id}' has 'inherit' version_spec but is itself \
                     a dependency (circular inherit resolution)"
                )));
            }
            Ok(global.version_spec.clone())
        }
        other => Ok(other.clone()),
    }
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
    recheck_policy: RecheckPolicy,
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
    let _store_dir = cache_root.join("store");
    let content_domain = CacheDomainConfig {
        domain: "tools".to_string(),
        index_file_name: "tools.json".to_string(),
        entry_ttl_seconds: 30 * 24 * 60 * 60,
    };
    let metadata_domain = CacheDomainConfig {
        domain: "tool_metadata".to_string(),
        index_file_name: "tool_metadata.json".to_string(),
        entry_ttl_seconds: 24 * 60 * 60,
    };
    let cache = Cache::open(&cache_root, &[content_domain, metadata_domain])
        .await
        .map(ToolDownloadCache::from_cache)
        .map_err(|e| MediaPmError::Workflow(format!("failed to open tool download cache: {e}")))?;

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

    // Compute used tool set: tools that are actually referenced by workflow
    // steps or their transitive dependencies.
    let used_tool_ids = compute_used_tool_ids(desired_tools);

    let mut pruned_tools: usize = 0;
    for (_i, (tool_id, tool_value)) in desired_tools.iter().enumerate() {
        let is_used = used_tool_ids.contains(tool_id.as_str());
        let is_builtin_code = is_builtin_source_ingest_requirement(tool_id);
        let already_exists = generated_doc.tools.values().any(|s| s.name == *tool_id);

        if !is_used {
            let prune_bar = effective_group.add_bar(1, &format!("{tool_id} [prune]"));
            // Tool not in active set — register with empty runtime and skip provisioning.
            // Record minimal deployment state (no payload).
            let now = std::time::SystemTime::now()
                .duration_since(std::time::UNIX_EPOCH)
                .unwrap_or_default()
                .as_secs();
            report.tool_records.insert(
                tool_id.clone(),
                ToolRegistryEntry {
                    version: String::new(),
                    canonical_version: String::new(),
                    content_map_hash: None,
                    deployed_at: now,
                    resolved_tag: String::new(),
                    resolved_version: String::new(),
                    resolved_vcs_hash: String::new(),
                },
            );
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
                        runtime: mediapm_conductor::ToolRuntime::default(),
                    },
                );
            }
            prune_bar.set_position(1);
            prune_bar.set_message("pruned");
            prune_bar.finish_success();
            pb.advance(1);
            continue;
        }

        // --- Spec-based skip: if desired spec is already satisfied, skip. ---
        let tool_req = serde_json::from_value::<ToolRequirement>(tool_value.clone()).ok();
        if let Some(req) = &tool_req {
            if req.version_spec != VersionSpec::Latest && req.version_spec != VersionSpec::Inherit {
                if let Some(entry) = state.managed_tools.get(tool_id) {
                    if spec_matches_entry(
                        &req.version_spec,
                        &entry.resolved_tag,
                        &entry.resolved_version,
                        &entry.resolved_vcs_hash,
                    ) {
                        // Already have the desired version — skip provisioning.
                        for (key, spec) in &generated_doc.tools {
                            if spec.name == *tool_id {
                                tool_runtimes.insert(key.clone(), spec.runtime.clone());
                                break;
                            }
                        }
                        report.tools_skipped += 1;
                        pb.advance(1);
                        continue;
                    }
                }
            }
        }
        // --- End spec-based skip ---

        // Initialized in the Ok(fetch) arm before the skip check;
        // used in the Ok(None) payload branch below. String::new() is
        // the dead initial value because the assignment in the match
        // arm always runs before any read (other paths `continue`).
        #[allow(unused_assignments)]
        let mut resolved_canonical_version = String::new();
        #[allow(unused_assignments)]
        let mut resolved_tag_value = String::new();
        let pre_resolved = match provider::resolve_tool_fetch(
            tool_id,
            Some((&*cache, "tool_metadata")),
            recheck_policy,
        )
        .await
        {
            Ok((
                fetch,
                human_readable_version,
                canonical_version,
                _metadata_cached,
                _metadata_fetch_count,
                resolved_tag,
            )) => {
                resolved_canonical_version = canonical_version.clone();
                resolved_tag_value = resolved_tag.clone();

                // --- Post-resolve validation: verify resolved result matches desired spec ---
                if let Some(req) = &tool_req {
                    match &req.version_spec {
                        VersionSpec::Exact(fields) => {
                            if let Some(hash) = &fields.vcs_hash {
                                if resolved_canonical_version != *hash
                                    && resolved_tag_value != *hash
                                {
                                    return Err(MediaPmError::Workflow(format!(
                                        "tool {tool_id}: requested vcs_hash {hash} but resolved canonical {resolved_canonical_version} and tag {resolved_tag_value}"
                                    )));
                                }
                            }
                            if let Some(tag) = &fields.tag {
                                if resolved_tag_value != *tag {
                                    return Err(MediaPmError::Workflow(format!(
                                        "tool {tool_id}: requested tag {tag} but resolved {resolved_tag_value}"
                                    )));
                                }
                            }
                            if let Some(ver) = &fields.version {
                                if human_readable_version != *ver {
                                    return Err(MediaPmError::Workflow(format!(
                                        "tool {tool_id}: requested version {ver} but resolved {human_readable_version}"
                                    )));
                                }
                            }
                        }
                        VersionSpec::Latest => {} // always OK
                        VersionSpec::Inherit => {
                            return Err(MediaPmError::Workflow(format!(
                                "tool {tool_id}: 'inherit' version_spec is only valid for dependencies, not global tool requirements"
                            )));
                        }
                    }
                }
                // --- End post-resolve validation ---

                // Check skip: if state has an entry with the same canonical_version
                // AND a non-empty content_map_hash, skip provisioning entirely.
                let should_skip = state.managed_tools.get(tool_id).is_some_and(|existing| {
                    existing.canonical_version == canonical_version
                        && existing.content_map_hash.is_some()
                });

                if should_skip {
                    PreResolveOutcome::Skip {
                        name: tool_id.clone(),
                        human_readable_version: human_readable_version.clone(),
                        version: canonical_version,
                        metadata_cached: _metadata_cached,
                        metadata_fetch_count: _metadata_fetch_count,
                        resolved_tag: resolved_tag.clone(),
                    }
                } else {
                    PreResolveOutcome::Resolved(
                        fetch,
                        human_readable_version,
                        canonical_version,
                        _metadata_cached,
                        _metadata_fetch_count,
                        resolved_tag,
                    )
                }
            }
            Err(e) => {
                let error_bar = effective_group.add_bar(1, &format!("{tool_id} [resolve]"));
                error_bar.finish_error();
                report.warnings.push(format!(
                    "tool {tool_id}: resolve failed (will retry on next sync): {e}",
                ));
                pb.advance(1);
                continue;
            }
        };

        let was_skip = matches!(&pre_resolved, PreResolveOutcome::Skip { .. });
        let payload_result =
            fetch_and_import_tool_payload(cas, tool_id, &cache, effective_group, pre_resolved)
                .await;

        if was_skip {
            // Skipped tools still need env var entries. Reconstruct runtime
            // from the existing spec in the generated document.
            for (key, spec) in &generated_doc.tools {
                if spec.name == *tool_id {
                    tool_runtimes.insert(key.clone(), spec.runtime.clone());
                    break;
                }
            }
            report.tools_skipped += 1;
            pb.advance(1);
            continue;
        }

        match payload_result {
            Ok(Some(payload)) => {
                // Compute content-addressed hash from content_map before it's
                // moved into build_tool_spec.
                let content_map_hash = if payload.content_map.is_empty() {
                    None
                } else {
                    let json = serde_json::to_string(&payload.content_map)
                        .expect("content_map serializes to JSON");
                    Some(format!("blake3:{}", blake3::hash(json.as_bytes()).to_hex()))
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
                report.tool_records.insert(
                    tool_id.clone(),
                    ToolRegistryEntry {
                        version: payload.human_readable_version.clone(),
                        canonical_version: payload.canonical_version.clone(),
                        content_map_hash: content_map_hash.clone(),
                        deployed_at: now,
                        resolved_tag: resolved_tag_value.clone(),
                        resolved_version: String::new(),
                        resolved_vcs_hash: String::new(),
                    },
                );

                // Inject inherited_env_vars from requirement config.
                let inherited = inherited_env_vars.get(tool_id).cloned().unwrap_or_default();

                let mut full_runtime = runtime.clone();
                full_runtime.inherited_env_vars = inherited;

                // Use content-addressed key: "{name}@{hash}".
                let tool_key = if let Some(ref hash) = content_map_hash {
                    format!("{}@{}", tool_id, hash)
                } else {
                    tool_id.to_string()
                };

                // Prune old version keys from generated documents.
                let prefix = format!("{}@", tool_id);
                let old: Vec<String> = generated_doc
                    .tools
                    .keys()
                    .filter(|k| (k.starts_with(&prefix) || *k == tool_id) && *k != &tool_key)
                    .cloned()
                    .collect();
                pruned_tools += old.len();
                for k in &old {
                    generated_doc.tools.remove(k);
                    tool_runtimes.remove(k);
                }

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
                        version: format!(
                            "{}+{}",
                            env!("CARGO_PKG_VERSION"),
                            crate::global::MEDIAPM_GIT_HASH
                        ),
                        canonical_version: resolved_canonical_version.clone(),
                        content_map_hash: None,
                        deployed_at: now,
                        resolved_tag: resolved_tag_value.clone(),
                        resolved_version: String::new(),
                        resolved_vcs_hash: String::new(),
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

    // 4. Ensure the tools runtime directory exists.
    std::fs::create_dir_all(&paths.tools_dir).map_err(|source| MediaPmError::Io {
        operation: "creating tools directory".to_string(),
        path: paths.tools_dir.clone(),
        source,
    })?;

    // 5. Write generated runtime env file from tool runtimes.
    write_generated_dotenv(&paths.runtime_root, &paths.tools_dir, &tool_runtimes)?;

    // 5. Save generated document.
    save_conductor_generated_document(paths, &generated_doc)?;

    // 6. Prune filesystem tool directories for non-active tools.
    retain_only_tool_dirs(paths.tools_dir.clone(), used_tool_ids.clone()).await?;

    report.pruned_tools = pruned_tools;

    Ok(report)
}

#[cfg(test)]
mod tests {
    use std::collections::BTreeMap;

    use mediapm_cas::InMemoryCas;
    use mediapm_conductor::cache_user_level::default_mediapm_user_download_cache_root;
    use mediapm_conductor::tools::provider::VersionSpecFields;
    use mediapm_conductor::{NickelDocument, ToolKindSpec, ToolSpec};
    use mediapm_utils::progress::recording::{ProgressOp, RecordingProgressTracker};

    use crate::config::ToolRequirement;
    use serde_json;

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
            RecheckPolicy::default(),
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
            RecheckPolicy::default(),
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
            RecheckPolicy::default(),
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

    #[tokio::test]
    async fn reconcile_desired_tools_skipped_tool_preserves_env_entries() {
        let tmp = tempfile::tempdir().unwrap();
        let cache_root = tempfile::tempdir().unwrap();
        let paths = MediaPmPaths::from_root(tmp.path());
        let cas = InMemoryCas::default();

        // Pre-populate generated doc with a tool that has content_map entries.
        // The skip branch should reconstruct the runtime from this doc.
        let mut content_map = BTreeMap::new();
        content_map.insert("linux/media-tagger".to_string(), "blake3:abc123".to_string());
        content_map.insert("macos/media-tagger".to_string(), "blake3:def456".to_string());
        let tool_spec = ToolSpec {
            name: "media-tagger".to_string(),
            kind: ToolKindSpec::default(),
            runtime: ToolRuntime { content_map, ..Default::default() },
            ..Default::default()
        };
        let mut tools = BTreeMap::new();
        tools.insert("media-tagger".to_string(), tool_spec);
        let doc = NickelDocument { tools, ..Default::default() };
        save_conductor_generated_document(&paths, &doc).expect("pre-save generated doc");

        // State with matching canonical_version and content_map_hash → triggers skip.
        let mut state = MediaPmState::default();
        state.managed_tools.insert(
            "media-tagger".to_string(),
            ToolRegistryEntry {
                version: format!(
                    "{}+{}",
                    env!("CARGO_PKG_VERSION"),
                    crate::global::MEDIAPM_GIT_HASH
                ),
                canonical_version: crate::global::MEDIAPM_GIT_HASH.to_string(),
                content_map_hash: Some("blake3:abc".to_string()),
                deployed_at: 0,
                resolved_tag: String::new(),
                resolved_version: String::new(),
                resolved_vcs_hash: String::new(),
            },
        );

        // Desired tools with media-tagger.
        let mut desired_tools = BTreeMap::new();
        let req = ToolRequirement::default();
        desired_tools.insert("media-tagger".to_string(), serde_json::to_value(req).unwrap());

        let result = reconcile_desired_tools(
            &cas,
            &paths,
            &desired_tools,
            &BTreeMap::new(),
            RecheckPolicy::default(),
            &state,
            Some(cache_root.path()),
            None,
        )
        .await;

        assert!(result.is_ok(), "reconcile_desired_tools failed: {:?}", result.err(),);

        // Verify env file has entries reconstructed from the generated doc.
        let env_path = &paths.env_generated_file;
        assert!(env_path.exists(), ".env.generated should exist");
        let content = std::fs::read_to_string(env_path).expect("env file readable");
        assert!(
            content.contains("MEDIAPM_MEDIA_TAGGER_LINUX"),
            "env file should have MEDIAPM_MEDIA_TAGGER_LINUX\n--- content:\n{content}",
        );
        assert!(
            content.contains("MEDIAPM_MEDIA_TAGGER_LINUX_DIR"),
            "env file should have MEDIAPM_MEDIA_TAGGER_LINUX_DIR\n--- content:\n{content}",
        );
        assert!(
            content.contains("MEDIAPM_MEDIA_TAGGER_MACOS"),
            "env file should have MEDIAPM_MEDIA_TAGGER_MACOS\n--- content:\n{content}",
        );
        assert!(
            content.contains("MEDIAPM_MEDIA_TAGGER_MACOS_DIR"),
            "env file should have MEDIAPM_MEDIA_TAGGER_MACOS_DIR\n--- content:\n{content}",
        );
        assert!(
            content.contains("/media-tagger/payload/"),
            "env file paths should contain /media-tagger/payload/\n--- content:\n{content}",
        );
    }

    #[tokio::test]
    async fn reconcile_prunes_old_tool_version_from_generated_doc() {
        let tmp = tempfile::tempdir().unwrap();
        let cache_root = tempfile::tempdir().unwrap();
        let paths = MediaPmPaths::from_root(tmp.path());
        let cas = InMemoryCas::default();

        // Pre-populate generated doc with an old version key that has a bogus
        // content hash suffix.  This simulates a stale entry from a previous
        // sync that should be removed when a fresh key is computed.
        let mut content_map = BTreeMap::new();
        content_map.insert("linux/media-tagger".to_string(), "blake3:abc".to_string());
        let tool_spec = ToolSpec {
            name: "media-tagger".to_string(),
            kind: ToolKindSpec::default(),
            runtime: ToolRuntime { content_map, ..Default::default() },
            ..Default::default()
        };
        let mut tools = BTreeMap::new();
        tools.insert("media-tagger@bogus_hash".to_string(), tool_spec);
        let doc = NickelDocument { tools, ..Default::default() };
        save_conductor_generated_document(&paths, &doc).expect("pre-save generated doc");

        // State with a different canonical_version so the skip path does not
        // fire — forcing a fresh resolve and a new tool_key computation.
        let mut state = MediaPmState::default();
        state.managed_tools.insert(
            "media-tagger".to_string(),
            ToolRegistryEntry {
                version: "old-version".to_string(),
                canonical_version: "old-canonical".to_string(),
                content_map_hash: None,
                deployed_at: 0,
                resolved_tag: String::new(),
                resolved_version: String::new(),
                resolved_vcs_hash: String::new(),
            },
        );

        // Desired tools with media-tagger.
        let mut desired_tools = BTreeMap::new();
        let req = ToolRequirement::default();
        desired_tools.insert("media-tagger".to_string(), serde_json::to_value(req).unwrap());

        let result = reconcile_desired_tools(
            &cas,
            &paths,
            &desired_tools,
            &BTreeMap::new(),
            RecheckPolicy::default(),
            &state,
            Some(cache_root.path()),
            None,
        )
        .await;

        assert!(result.is_ok(), "reconcile_desired_tools failed: {:?}", result.err());
        let report = result.unwrap();

        // The old bogus key should have been counted toward pruned_tools.
        assert!(
            report.pruned_tools >= 1,
            "expected at least 1 pruned tool, got {}",
            report.pruned_tools
        );

        // Reload generated doc and verify the old key is gone.
        let doc = load_conductor_generated_document(&paths).expect("load generated doc after sync");
        assert!(
            !doc.tools.contains_key("media-tagger@bogus_hash"),
            "old version key should have been pruned"
        );

        // The new key should exist (bare media-tagger or media-tagger@<hash>).
        let has_new_key =
            doc.tools.keys().any(|k| k == "media-tagger" || k.starts_with("media-tagger@"));
        assert!(
            has_new_key,
            "new version key should exist after sync, keys: {:?}",
            doc.tools.keys().collect::<Vec<_>>()
        );
    }

    // ---------------------------------------------------------------------------
    // Phase 6 — compute_used_tool_ids
    // ---------------------------------------------------------------------------

    #[test]
    fn compute_used_tool_ids_empty_desired() {
        let empty: BTreeMap<String, serde_json::Value> = BTreeMap::new();
        let used = compute_used_tool_ids(&empty);
        assert!(used.is_empty(), "empty desired_tools → empty used set");
    }

    #[test]
    fn compute_used_tool_ids_single_no_deps() {
        let mut desired: BTreeMap<String, serde_json::Value> = BTreeMap::new();
        desired.insert(
            "ffmpeg".to_string(),
            serde_json::to_value(ToolRequirement::default()).unwrap(),
        );
        let used = compute_used_tool_ids(&desired);
        assert!(used.contains("ffmpeg"));
        assert_eq!(used.len(), 1);
    }

    #[test]
    fn compute_used_tool_ids_with_transitive_deps() {
        let mut desired: BTreeMap<String, serde_json::Value> = BTreeMap::new();
        // yt-dlp depends on ffmpeg and deno
        let yt_dlp_req = ToolRequirement {
            version_spec: mediapm_conductor::tools::provider::VersionSpec::Latest,
            dependencies: BTreeMap::from([
                ("ffmpeg".to_string(), mediapm_conductor::tools::provider::VersionSpec::Inherit),
                ("deno".to_string(), mediapm_conductor::tools::provider::VersionSpec::Inherit),
            ]),
            ..Default::default()
        };
        desired.insert("yt-dlp".to_string(), serde_json::to_value(yt_dlp_req).unwrap());
        desired.insert(
            "ffmpeg".to_string(),
            serde_json::to_value(ToolRequirement::default()).unwrap(),
        );
        desired
            .insert("deno".to_string(), serde_json::to_value(ToolRequirement::default()).unwrap());
        desired.insert(
            "unrelated".to_string(),
            serde_json::to_value(ToolRequirement::default()).unwrap(),
        );

        let used = compute_used_tool_ids(&desired);
        assert!(used.contains("yt-dlp"), "step tool must be in used set");
        assert!(used.contains("ffmpeg"), "dep tool must be in used set");
        assert!(used.contains("deno"), "dep tool must be in used set");
        assert!(
            used.contains("unrelated"),
            "all desired tools are now seeds, so unrelated IS used"
        );
        assert_eq!(used.len(), 4);
    }

    #[test]
    fn compute_used_tool_ids_circular_deps_terminates() {
        let mut desired: BTreeMap<String, serde_json::Value> = BTreeMap::new();
        // tool_a → tool_b, tool_b → tool_a (circular)
        let a_req = ToolRequirement {
            version_spec: mediapm_conductor::tools::provider::VersionSpec::Latest,
            dependencies: BTreeMap::from([(
                "tool_b".to_string(),
                mediapm_conductor::tools::provider::VersionSpec::Inherit,
            )]),
            ..Default::default()
        };
        let b_req = ToolRequirement {
            version_spec: mediapm_conductor::tools::provider::VersionSpec::Latest,
            dependencies: BTreeMap::from([(
                "tool_a".to_string(),
                mediapm_conductor::tools::provider::VersionSpec::Inherit,
            )]),
            ..Default::default()
        };
        desired.insert("tool_a".to_string(), serde_json::to_value(a_req).unwrap());
        desired.insert("tool_b".to_string(), serde_json::to_value(b_req).unwrap());

        let used = compute_used_tool_ids(&desired);
        assert!(used.contains("tool_a"));
        assert!(used.contains("tool_b"));
        assert_eq!(used.len(), 2);
    }

    // ---------------------------------------------------------------------------
    // Phase 6 — resolve_dep_version_spec
    // ---------------------------------------------------------------------------

    #[test]
    fn resolve_dep_version_spec_inherit_resolves() {
        let mut globals: BTreeMap<String, ToolRequirement> = BTreeMap::new();
        globals.insert(
            "ffmpeg".to_string(),
            ToolRequirement {
                version_spec: mediapm_conductor::tools::provider::VersionSpec::Exact(
                    VersionSpecFields { vcs_hash: Some("abc".into()), version: None, tag: None },
                ),
                ..Default::default()
            },
        );
        let result = resolve_dep_version_spec(
            &mediapm_conductor::tools::provider::VersionSpec::Inherit,
            "ffmpeg",
            &globals,
        )
        .unwrap();
        assert_eq!(
            result,
            mediapm_conductor::tools::provider::VersionSpec::Exact(VersionSpecFields {
                vcs_hash: Some("abc".into()),
                version: None,
                tag: None,
            })
        );
    }

    #[test]
    fn resolve_dep_version_spec_exact_passthrough() {
        let globals: BTreeMap<String, ToolRequirement> = BTreeMap::new();
        let spec = mediapm_conductor::tools::provider::VersionSpec::Exact(VersionSpecFields {
            vcs_hash: None,
            version: Some("1.0".into()),
            tag: None,
        });
        let result = resolve_dep_version_spec(&spec, "any", &globals).unwrap();
        assert_eq!(result, spec);
    }

    #[test]
    fn resolve_dep_version_spec_latest_passthrough() {
        let globals: BTreeMap<String, ToolRequirement> = BTreeMap::new();
        let result = resolve_dep_version_spec(
            &mediapm_conductor::tools::provider::VersionSpec::Latest,
            "any",
            &globals,
        )
        .unwrap();
        assert_eq!(result, mediapm_conductor::tools::provider::VersionSpec::Latest);
    }

    #[test]
    fn resolve_dep_version_spec_inherit_missing_tool_error() {
        let globals: BTreeMap<String, ToolRequirement> = BTreeMap::new();
        let result = resolve_dep_version_spec(
            &mediapm_conductor::tools::provider::VersionSpec::Inherit,
            "missing",
            &globals,
        );
        assert!(result.is_err());
        assert!(result.unwrap_err().to_string().contains("not configured"));
    }

    #[test]
    fn resolve_dep_version_spec_circular_inherit_error() {
        let mut globals: BTreeMap<String, ToolRequirement> = BTreeMap::new();
        globals.insert(
            "foo".to_string(),
            ToolRequirement {
                version_spec: mediapm_conductor::tools::provider::VersionSpec::Inherit,
                ..Default::default()
            },
        );
        let result = resolve_dep_version_spec(
            &mediapm_conductor::tools::provider::VersionSpec::Inherit,
            "foo",
            &globals,
        );
        assert!(result.is_err());
        assert!(result.unwrap_err().to_string().contains("circular"));
    }
}
