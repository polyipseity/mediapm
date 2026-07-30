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

use std::collections::{BTreeMap, HashMap, HashSet};
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
use mediapm_conductor::tools::provider::{ConfigVersionSpec, VersionSpec};
use mediapm_conductor::tools::spec::spec_matches_entry;

use crate::tools::dependency::DependencyType;
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
    /// Flat list ordered by iteration order of desired_tools.
    pub(crate) tool_records: Vec<ToolRegistryEntry>,
}

/// A single entry in the provisioning pipeline.
struct ProvisionEntry {
    /// Bare tool_id used for provider resolution (e.g., "ffmpeg").
    tool_id: String,
    /// The tool requirement to apply when provisioning this entry.
    tool_requirement: ToolRequirement,
    /// Whether this entry came from the user config or was auto-vivified.
    #[allow(dead_code)]
    kind: EntryKind,
}

#[allow(dead_code)]
enum EntryKind {
    /// Entry from the user's `tools.<id>` config.
    Explicit,
    /// Auto-vivified from a dependency declaration.
    Dep { dependent: String },
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
            match serde_json::from_value::<ToolRequirement>(value.clone()) {
                Ok(req) => {
                    for dep_id in req.dependencies.keys() {
                        if !used.contains(dep_id.as_str()) {
                            stack.push(dep_id.clone());
                        }
                    }
                }
                Err(e) => {
                    tracing::warn!(
                        "warning[MPM-W001]: failed to deserialize ToolRequirement for \
                         tool \"{tool_id}\": {e}",
                    );
                }
            }
        }
    }
    used
}

/// Resolve a dependency's effective version spec, converting from
/// [`ConfigVersionSpec`] (serde type, may contain `Inherit`) to
/// [`VersionSpec`] (clean resolved type, no `Inherit`).
///
/// - `ConfigVersionSpec::Inherit` → look up the dependency tool's global
///   `version_spec` in `global_requirements` and return that (resolved to
///   `VersionSpec::Latest` or `VersionSpec::Exact`).
/// - `ConfigVersionSpec::Exact(...)` / `ConfigVersionSpec::Latest` → convert
///   directly to the corresponding `VersionSpec` variant.
///
/// This is the single boundary point where `ConfigVersionSpec::Inherit`
/// is resolved away before reaching internal code.
///
/// # Errors
///
/// Returns [`MediaPmError::ConfigValidation`] with `MPM-E002` when inherit
/// cannot be resolved because the tool is not configured.
/// Returns [`MediaPmError::ConfigValidation`] with `MPM-E003` on circular inherit.
pub(crate) fn resolve_dep_version_spec(
    dep_spec: &ConfigVersionSpec,
    dep_tool_id: &str,
    global_requirements: &BTreeMap<String, ToolRequirement>,
    parent_tool_id: &str,
) -> Result<VersionSpec, MediaPmError> {
    match dep_spec {
        ConfigVersionSpec::Inherit => {
            let global = global_requirements.get(dep_tool_id).ok_or_else(|| {
                MediaPmError::ConfigValidation {
                    code: "MPM-E002",
                    context: format!("tool \"{parent_tool_id}\" dependency \"{dep_tool_id}\""),
                    detail: format!(
                        "uses \"inherit\" version spec but \"{dep_tool_id}\" \
                         is not configured in the tools section"
                    ),
                    suggestion: format!(
                        "add \"{dep_tool_id}\" to the tools section, or use \"latest\" \
                         or an explicit version spec like {{ \"version\" = \"...\" }}"
                    ),
                }
            })?;
            // Also error if the global tool itself has "inherit" (circular).
            if global.version_spec == ConfigVersionSpec::Inherit {
                return Err(MediaPmError::ConfigValidation {
                    code: "MPM-E003",
                    context: format!("tool \"{parent_tool_id}\" dependency \"{dep_tool_id}\""),
                    detail: format!(
                        "has \"inherit\" version_spec but \"{dep_tool_id}\" itself \
                         uses \"inherit\" (circular inherit resolution)"
                    ),
                    suggestion: format!(
                        "set an explicit version for \"{dep_tool_id}\" in the tools section \
                         to break the cycle"
                    ),
                });
            }
            match &global.version_spec {
                ConfigVersionSpec::Latest => Ok(VersionSpec::Latest),
                ConfigVersionSpec::Exact(fields) => Ok(VersionSpec::Exact(fields.clone())),
                ConfigVersionSpec::Inherit => unreachable!(), // caught above
            }
        }
        ConfigVersionSpec::Latest => Ok(VersionSpec::Latest),
        ConfigVersionSpec::Exact(fields) => Ok(VersionSpec::Exact(fields.clone())),
    }
}

/// Build in-memory index from flat state Vec for O(1) tool_id group lookup.
pub(crate) fn index_managed_tools(
    entries: &[ToolRegistryEntry],
) -> HashMap<String, Vec<ToolRegistryEntry>> {
    let mut map: HashMap<String, Vec<ToolRegistryEntry>> = HashMap::new();
    for entry in entries {
        map.entry(entry.tool_id.clone()).or_default().push(entry.clone());
    }
    map
}

/// Build provisioning entries from desired tools.
///
/// Each explicit tool gets one entry. Each dependency of each tool gets a
/// separate entry with the resolved version spec from the dependency
/// declaration. Dep entries are sorted before explicit entries so they
/// provision first (making dep canonical_versions available for composites).
fn build_provisioning_entries(
    desired_tools: &BTreeMap<String, serde_json::Value>,
) -> Result<Vec<ProvisionEntry>, MediaPmError> {
    // Build a ToolRequirement map for resolve_dep_version_spec which expects
    // &BTreeMap<String, ToolRequirement>
    let global_reqs: BTreeMap<String, ToolRequirement> = desired_tools
        .iter()
        .filter_map(|(id, val)| {
            serde_json::from_value::<ToolRequirement>(val.clone()).ok().map(|req| (id.clone(), req))
        })
        .collect();

    let mut explicit_entries: BTreeMap<String, ProvisionEntry> = BTreeMap::new();
    let mut dep_entries: Vec<ProvisionEntry> = Vec::new();

    for (tool_id, tool_value) in desired_tools {
        let req: ToolRequirement = serde_json::from_value(tool_value.clone()).map_err(|e| {
            MediaPmError::Workflow(format!("invalid tool requirement for {tool_id}: {e}"))
        })?;

        // Explicit entry
        explicit_entries.entry(tool_id.clone()).or_insert_with(|| ProvisionEntry {
            tool_id: tool_id.clone(),
            tool_requirement: req.clone(),
            kind: EntryKind::Explicit,
        });

        // Dependency entries — one per dep, with resolved version spec
        for (dep_id, dep_spec) in &req.dependencies {
            let resolved_spec = resolve_dep_version_spec(dep_spec, dep_id, &global_reqs, tool_id)?;
            let dep_req = ToolRequirement {
                version_spec: match resolved_spec {
                    VersionSpec::Latest => ConfigVersionSpec::Latest,
                    VersionSpec::Exact(fields) => ConfigVersionSpec::Exact(fields),
                },
                dependencies: BTreeMap::new(),
                ..Default::default()
            };
            dep_entries.push(ProvisionEntry {
                tool_id: dep_id.clone(),
                tool_requirement: dep_req,
                kind: EntryKind::Dep { dependent: tool_id.clone() },
            });
        }
    }

    // Dep entries first (provision deps before dependents), then explicit
    // Dedup consecutive entries with same (tool_id, version_spec)
    let mut all_entries: Vec<ProvisionEntry> = dep_entries;
    all_entries.extend(explicit_entries.into_values());
    all_entries.dedup_by(|a, b| {
        a.tool_id == b.tool_id && a.tool_requirement.version_spec == b.tool_requirement.version_spec
    });

    Ok(all_entries)
}

/// Build composite canonical_version from bare version and same-step dep
/// version pairs. Dep identifiers are bare dep_ids (not PKeys), sorted
/// alphabetically for determinism.
///
/// Format: `<bare>;<dep_id_1>:<dep_ver_1>;<dep_id_2>:<dep_ver_2>;...`
fn composite_canonical_version(bare: &str, dep_versions: &[(&str, &str)]) -> String {
    if dep_versions.is_empty() {
        return bare.to_string();
    }
    let mut sorted: Vec<(&str, &str)> = dep_versions.to_vec();
    sorted.sort_by(|a, b| a.0.cmp(b.0));
    let suffix: String = sorted.iter().map(|(dep_id, ver)| format!(";{dep_id}:{ver}")).collect();
    format!("{bare}{suffix}")
}

/// Collect same-step dep_ids for a given entry (returns bare dep_ids).
fn collect_same_step_dep_ids(
    tool_id: &str,
    tool_req: &ToolRequirement,
    known_dep_type: fn(&str, &str) -> Option<DependencyType>,
) -> Vec<String> {
    tool_req
        .dependencies
        .keys()
        .filter_map(|dep_id| {
            if !known_dep_type(tool_id, dep_id)
                .is_some_and(|t| matches!(t, DependencyType::SameStep | DependencyType::Both))
            {
                return None;
            }
            Some(dep_id.clone())
        })
        .collect()
}

/// Compute canonical_version for persistence, including same-step dep versions.
///
/// The canonical version stored in [`ToolRegistryEntry`] is a composite of the
/// bare provider-resolved version and same-step dependency versions. This
/// ensures skip detection works correctly — when a same-step dep version
/// changes, the composite changes and triggers re-provisioning.
///
/// For tools without same-step deps, returns the bare version unchanged.
pub(crate) fn compute_composite_canonical_version(
    bare: &str,
    tool_id: &str,
    tool_req: &ToolRequirement,
    live_state: &HashMap<String, Vec<ToolRegistryEntry>>,
) -> String {
    let same_step_deps = collect_same_step_dep_ids(
        tool_id,
        tool_req,
        crate::tools::dependency::known_dependency_type,
    );
    let dep_versions: Vec<(&str, &str)> = same_step_deps
        .iter()
        .filter_map(|dep_id| {
            let dep_req = tool_req.dependencies.get(dep_id)?;
            let dep_group = live_state.get(dep_id.as_str())?;
            // Find the active entry for this dep. The dep spec in
            // tool_req.dependencies is ConfigVersionSpec (from serde). For
            // Inherit/Latest (the common case for SameStep deps like ffmpeg
            // → yt-dlp), we match any active entry regardless of spec values
            // — the dep was already resolved and provisioned earlier in the
            // same sync pass, so whatever version is active IS the version
            // to include. For Exact, verify against the spec via
            // spec_matches_entry.
            let matched = dep_group.iter().find(|e| {
                if e.content_map_hash.is_empty() {
                    return false;
                }
                match dep_req {
                    ConfigVersionSpec::Inherit | ConfigVersionSpec::Latest => true,
                    ConfigVersionSpec::Exact(fields) => spec_matches_entry(
                        &VersionSpec::Exact(fields.clone()),
                        &e.resolved_tag,
                        &e.resolved_version,
                        &e.resolved_vcs_hash,
                    ),
                }
            })?;
            Some((dep_id.as_str(), matched.canonical_version.as_str()))
        })
        .collect();
    composite_canonical_version(bare, &dep_versions)
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

    // Validate all dependency keys before any provisioning begins.
    // Fail-fast with MPM-E001 if any tool has an unrecognized dependency key.
    for (tool_id, tool_value) in desired_tools {
        if let Ok(req) = serde_json::from_value::<ToolRequirement>(tool_value.clone()) {
            crate::tools::dependency::validate_dependency_keys(tool_id, &req.dependencies)?;
        }
    }

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

    // Build provisioning entries and in-memory live state index for
    // O(1) skip checking across entries. Must happen before the progress
    // bar setup since the bar total uses entries.len().
    let entries = build_provisioning_entries(desired_tools)?;
    let mut live_state = index_managed_tools(&state.managed_tools);

    // Progress bar for the per-tool provisioning loop.
    let total_tools = entries.len() as u64;
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
    for entry in &entries {
        let tool_id = &entry.tool_id;
        let tool_req = &entry.tool_requirement;
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
            report.tool_records.push(ToolRegistryEntry {
                tool_id: tool_id.clone(),
                version: String::new(),
                canonical_version: String::new(),
                content_map_hash: String::new(),
                deployed_at: now,
                resolved_tag: String::new(),
                resolved_version: String::new(),
                resolved_vcs_hash: String::new(),
            });
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
        if tool_req.version_spec != ConfigVersionSpec::Latest
            && tool_req.version_spec != ConfigVersionSpec::Inherit
        {
            if let Some(entry) = state.managed_tools.iter().find(|e| e.tool_id == *tool_id) {
                // Convert ConfigVersionSpec to VersionSpec for spec matching.
                // At this point we know it's Exact (guarded by the != Latest/Inherit check above).
                let resolved_spec = match &tool_req.version_spec {
                    ConfigVersionSpec::Exact(fields) => VersionSpec::Exact(fields.clone()),
                    _ => unreachable!(), // Latest/Inherit already filtered above
                };
                if spec_matches_entry(
                    &resolved_spec,
                    &entry.resolved_tag,
                    &entry.resolved_version,
                    &entry.resolved_vcs_hash,
                ) {
                    // Already have the desired version — skip provisioning.
                    for (_, spec) in &generated_doc.tools {
                        if spec.name == *tool_id {
                            tool_runtimes.entry(tool_id.clone()).or_insert(spec.runtime.clone());
                            break;
                        }
                    }
                    report.tools_skipped += 1;
                    pb.advance(1);
                    continue;
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
                match &tool_req.version_spec {
                    ConfigVersionSpec::Exact(fields) => {
                        if let Some(hash) = &fields.vcs_hash {
                            if resolved_canonical_version != *hash && resolved_tag_value != *hash {
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
                    ConfigVersionSpec::Latest => {} // always OK
                    ConfigVersionSpec::Inherit => {
                        return Err(MediaPmError::Workflow(format!(
                            "tool {tool_id}: 'inherit' version_spec is only valid for dependencies, not global tool requirements"
                        )));
                    }
                }
                // --- End post-resolve validation ---

                // Compute expected composite canonical_version for skip check.
                // For tools with same-step dependencies, include dep versions in composite.
                let expected_composite = compute_composite_canonical_version(
                    &canonical_version,
                    tool_id,
                    tool_req,
                    &live_state,
                );

                // Check skip: does live_state[tool_id] have any ACTIVE entry with
                // canonical_version == expected_composite? Filter to only non-empty hashes.
                let should_skip = live_state.get(tool_id.as_str()).is_some_and(|entries| {
                    entries.iter().any(|e| {
                        !e.content_map_hash.is_empty() && e.canonical_version == expected_composite
                    })
                });

                if should_skip {
                    PreResolveOutcome::Skip {
                        name: tool_id.clone(),
                        human_readable_version: human_readable_version.clone(),
                        version: expected_composite.clone(),
                        metadata_cached: _metadata_cached,
                        metadata_fetch_count: _metadata_fetch_count,
                        resolved_tag: resolved_tag.clone(),
                    }
                } else {
                    PreResolveOutcome::Resolved(
                        fetch,
                        human_readable_version,
                        expected_composite,
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
            for (_, spec) in &generated_doc.tools {
                if spec.name == *tool_id {
                    tool_runtimes.entry(tool_id.clone()).or_insert(spec.runtime.clone());
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
                let content_map_hash: String = if payload.content_map.is_empty() {
                    String::new()
                } else {
                    let json = serde_json::to_string(&payload.content_map)
                        .expect("content_map serializes to JSON");
                    format!("blake3:{}", blake3::hash(json.as_bytes()).to_hex())
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
                report.tool_records.push(ToolRegistryEntry {
                    tool_id: tool_id.clone(),
                    version: payload.human_readable_version.clone(),
                    canonical_version: payload.canonical_version.clone(),
                    content_map_hash: content_map_hash.clone(),
                    deployed_at: now,
                    resolved_tag: resolved_tag_value.clone(),
                    resolved_version: String::new(),
                    resolved_vcs_hash: String::new(),
                });

                // Update live_state for subsequent entries in the same sync.
                let entry_for_live = report.tool_records.last().unwrap().clone();
                live_state.entry(tool_id.clone()).or_default().push(entry_for_live.clone());

                // Inject inherited_env_vars from requirement config.
                let inherited = inherited_env_vars.get(tool_id).cloned().unwrap_or_default();

                let mut full_runtime = runtime.clone();
                full_runtime.inherited_env_vars = inherited;

                // Use content-addressed key: "{name}@{hash}".
                let tool_key = if !content_map_hash.is_empty() {
                    format!("{}@{}", tool_id, content_map_hash)
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

                generated_doc.tools.entry(tool_key.clone()).or_insert(spec);
                tool_runtimes.entry(tool_id.clone()).or_insert(full_runtime);
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
                let composite_for_ok_none = compute_composite_canonical_version(
                    &resolved_canonical_version,
                    tool_id,
                    &entry.tool_requirement,
                    &live_state,
                );
                report.tool_records.push(ToolRegistryEntry {
                    tool_id: tool_id.clone(),
                    version: format!(
                        "{}+{}",
                        env!("CARGO_PKG_VERSION"),
                        crate::global::MEDIAPM_GIT_HASH
                    ),
                    canonical_version: composite_for_ok_none,
                    content_map_hash: String::new(),
                    deployed_at: now,
                    resolved_tag: resolved_tag_value.clone(),
                    resolved_version: String::new(),
                    resolved_vcs_hash: String::new(),
                });

                // Update live_state for subsequent entries in the same sync.
                let entry_for_live = report.tool_records.last().unwrap().clone();
                live_state.entry(tool_id.clone()).or_default().push(entry_for_live.clone());

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
    use crate::tools::dependency::known_dependency_type;
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
        state.managed_tools.push(ToolRegistryEntry {
            tool_id: "media-tagger".to_string(),
            version: format!("{}+{}", env!("CARGO_PKG_VERSION"), crate::global::MEDIAPM_GIT_HASH),
            canonical_version: crate::global::MEDIAPM_GIT_HASH.to_string(),
            content_map_hash: "blake3:abc".to_string(),
            deployed_at: 0,
            resolved_tag: String::new(),
            resolved_version: String::new(),
            resolved_vcs_hash: String::new(),
        });

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
        state.managed_tools.push(ToolRegistryEntry {
            tool_id: "media-tagger".to_string(),
            version: "old-version".to_string(),
            canonical_version: "old-canonical".to_string(),
            content_map_hash: String::new(),
            deployed_at: 0,
            resolved_tag: String::new(),
            resolved_version: String::new(),
            resolved_vcs_hash: String::new(),
        });

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
            version_spec: mediapm_conductor::tools::provider::ConfigVersionSpec::Latest,
            dependencies: BTreeMap::from([
                (
                    "ffmpeg".to_string(),
                    mediapm_conductor::tools::provider::ConfigVersionSpec::Inherit,
                ),
                (
                    "deno".to_string(),
                    mediapm_conductor::tools::provider::ConfigVersionSpec::Inherit,
                ),
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
            version_spec: mediapm_conductor::tools::provider::ConfigVersionSpec::Latest,
            dependencies: BTreeMap::from([(
                "tool_b".to_string(),
                mediapm_conductor::tools::provider::ConfigVersionSpec::Inherit,
            )]),
            ..Default::default()
        };
        let b_req = ToolRequirement {
            version_spec: mediapm_conductor::tools::provider::ConfigVersionSpec::Latest,
            dependencies: BTreeMap::from([(
                "tool_a".to_string(),
                mediapm_conductor::tools::provider::ConfigVersionSpec::Inherit,
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
                version_spec: mediapm_conductor::tools::provider::ConfigVersionSpec::Exact(
                    VersionSpecFields { vcs_hash: Some("abc".into()), version: None, tag: None },
                ),
                ..Default::default()
            },
        );
        let result = resolve_dep_version_spec(
            &mediapm_conductor::tools::provider::ConfigVersionSpec::Inherit,
            "ffmpeg",
            &globals,
            "test_parent",
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
        let spec = ConfigVersionSpec::Exact(VersionSpecFields {
            vcs_hash: None,
            version: Some("1.0".into()),
            tag: None,
        });
        let result = resolve_dep_version_spec(&spec, "any", &globals, "test_parent").unwrap();
        assert_eq!(
            result,
            VersionSpec::Exact(VersionSpecFields {
                vcs_hash: None,
                version: Some("1.0".into()),
                tag: None,
            })
        );
    }

    #[test]
    fn resolve_dep_version_spec_latest_passthrough() {
        let globals: BTreeMap<String, ToolRequirement> = BTreeMap::new();
        let result = resolve_dep_version_spec(
            &mediapm_conductor::tools::provider::ConfigVersionSpec::Latest,
            "any",
            &globals,
            "test_parent",
        )
        .unwrap();
        assert_eq!(result, mediapm_conductor::tools::provider::VersionSpec::Latest);
    }

    #[test]
    fn resolve_dep_version_spec_inherit_missing_tool_error() {
        let globals: BTreeMap<String, ToolRequirement> = BTreeMap::new();
        let result = resolve_dep_version_spec(
            &mediapm_conductor::tools::provider::ConfigVersionSpec::Inherit,
            "missing",
            &globals,
            "test_parent",
        );
        assert!(result.is_err());
        let err = result.unwrap_err();
        let msg = err.to_string();
        assert!(msg.contains("MPM-E002"), "should contain MPM-E002 code");
        assert!(msg.contains("not configured"), "should mention not configured");
        assert!(msg.contains("inherit"), "should mention inherit");
    }

    #[test]
    fn resolve_dep_version_spec_circular_inherit_error() {
        let mut globals: BTreeMap<String, ToolRequirement> = BTreeMap::new();
        globals.insert(
            "foo".to_string(),
            ToolRequirement {
                version_spec: mediapm_conductor::tools::provider::ConfigVersionSpec::Inherit,
                ..Default::default()
            },
        );
        let result = resolve_dep_version_spec(
            &mediapm_conductor::tools::provider::ConfigVersionSpec::Inherit,
            "foo",
            &globals,
            "test_parent",
        );
        assert!(result.is_err());
        let err = result.unwrap_err();
        let msg = err.to_string();
        assert!(msg.contains("MPM-E003"), "should contain MPM-E003 code");
        assert!(msg.contains("circular"), "should mention circular");
        assert!(msg.contains("inherit"), "should mention inherit");
    }

    // Phase 7 — composite_canonical_version tests
    // ---------------------------------------------------------------------------

    #[test]
    fn composite_canonical_version_no_deps() {
        assert_eq!(composite_canonical_version("v1", &[]), "v1");
    }

    #[test]
    fn composite_canonical_version_single_dep() {
        let deps = [("ffmpeg", "ffmpeg-v7.1")];
        assert_eq!(composite_canonical_version("yt-dlp-v2", &deps), "yt-dlp-v2;ffmpeg:ffmpeg-v7.1");
    }

    #[test]
    fn composite_canonical_version_multi_dep_alphabetical() {
        let deps = [("deno", "deno-v2.0"), ("ffmpeg", "ffmpeg-v7.1")];
        assert_eq!(
            composite_canonical_version("yt-dlp-v2", &deps),
            "yt-dlp-v2;deno:deno-v2.0;ffmpeg:ffmpeg-v7.1"
        );
    }

    // Phase 7 — build_provisioning_entries tests
    // ---------------------------------------------------------------------------

    #[test]
    fn build_provisioning_entries_empty() {
        let entries = build_provisioning_entries(&BTreeMap::new()).unwrap();
        assert!(entries.is_empty());
    }

    #[test]
    fn build_provisioning_entries_single_no_deps() {
        let mut desired = BTreeMap::new();
        desired.insert(
            "ffmpeg".to_string(),
            serde_json::to_value(ToolRequirement {
                version_spec: ConfigVersionSpec::Latest,
                ..Default::default()
            })
            .unwrap(),
        );
        let entries = build_provisioning_entries(&desired).unwrap();
        assert_eq!(entries.len(), 1);
        assert_eq!(entries[0].tool_id, "ffmpeg");
        assert!(matches!(entries[0].kind, EntryKind::Explicit));
    }

    #[test]
    fn build_provisioning_entries_with_deps() {
        let mut desired = BTreeMap::new();
        let mut deps = BTreeMap::new();
        deps.insert(
            "ffmpeg".to_string(),
            ConfigVersionSpec::Exact(VersionSpecFields {
                tag: Some("v7.1".to_string()),
                version: None,
                vcs_hash: None,
            }),
        );
        desired.insert(
            "yt-dlp".to_string(),
            serde_json::to_value(ToolRequirement {
                version_spec: ConfigVersionSpec::Latest,
                dependencies: deps,
                ..Default::default()
            })
            .unwrap(),
        );
        let entries = build_provisioning_entries(&desired).unwrap();
        assert_eq!(entries.len(), 2);
        // Dep entry should come first (dep-first sort)
        assert_eq!(entries[0].tool_id, "ffmpeg");
        assert!(matches!(entries[0].kind, EntryKind::Dep { .. }));
        assert_eq!(entries[1].tool_id, "yt-dlp");
        assert!(matches!(entries[1].kind, EntryKind::Explicit));
    }

    #[test]
    fn build_provisioning_entries_dedup_same_spec() {
        let mut desired = BTreeMap::new();
        let deps = BTreeMap::from([("ffmpeg".to_string(), ConfigVersionSpec::Latest)]);
        desired.insert(
            "yt-dlp".to_string(),
            serde_json::to_value(ToolRequirement {
                version_spec: ConfigVersionSpec::Latest,
                dependencies: deps.clone(),
                ..Default::default()
            })
            .unwrap(),
        );
        desired.insert(
            "rsgain".to_string(),
            serde_json::to_value(ToolRequirement {
                version_spec: ConfigVersionSpec::Latest,
                dependencies: deps,
                ..Default::default()
            })
            .unwrap(),
        );
        let entries = build_provisioning_entries(&desired).unwrap();
        // Two same-spec ffmpeg dep entries dedup → total 3
        assert_eq!(entries.len(), 3);
        assert_eq!(entries.iter().filter(|e| e.tool_id == "ffmpeg").count(), 1);
    }

    #[test]
    fn build_provisioning_entries_different_spec_no_dedup() {
        let mut desired = BTreeMap::new();
        let mut deps_yt = BTreeMap::new();
        deps_yt.insert(
            "ffmpeg".to_string(),
            ConfigVersionSpec::Exact(VersionSpecFields {
                tag: Some("v7.1".to_string()),
                version: None,
                vcs_hash: None,
            }),
        );
        let mut deps_rsgain = BTreeMap::new();
        deps_rsgain.insert(
            "ffmpeg".to_string(),
            ConfigVersionSpec::Exact(VersionSpecFields {
                tag: Some("v6.0".to_string()),
                version: None,
                vcs_hash: None,
            }),
        );
        desired.insert(
            "yt-dlp".to_string(),
            serde_json::to_value(ToolRequirement {
                version_spec: ConfigVersionSpec::Latest,
                dependencies: deps_yt,
                ..Default::default()
            })
            .unwrap(),
        );
        desired.insert(
            "rsgain".to_string(),
            serde_json::to_value(ToolRequirement {
                version_spec: ConfigVersionSpec::Latest,
                dependencies: deps_rsgain,
                ..Default::default()
            })
            .unwrap(),
        );
        let entries = build_provisioning_entries(&desired).unwrap();
        // Two different ffmpeg specs → NO dedup, total = 4
        assert_eq!(entries.len(), 4);
        assert_eq!(entries.iter().filter(|e| e.tool_id == "ffmpeg").count(), 2);
    }

    // Phase 7 — collect_same_step_dep_ids tests
    // ---------------------------------------------------------------------------

    #[test]
    fn collect_same_step_dep_ids_empty_deps() {
        let req = ToolRequirement::default();
        let ids = collect_same_step_dep_ids("ffmpeg", &req, known_dependency_type);
        assert!(ids.is_empty());
    }

    #[test]
    fn collect_same_step_dep_ids_yt_dlp_ffmpeg() {
        let deps = BTreeMap::from([("ffmpeg".to_string(), ConfigVersionSpec::Latest)]);
        let req = ToolRequirement {
            version_spec: ConfigVersionSpec::Latest,
            dependencies: deps,
            ..Default::default()
        };
        let ids = collect_same_step_dep_ids("yt-dlp", &req, known_dependency_type);
        assert_eq!(ids, vec!["ffmpeg"]);
    }

    #[test]
    fn collect_same_step_dep_ids_rsgain_ffmpeg() {
        let deps = BTreeMap::from([("ffmpeg".to_string(), ConfigVersionSpec::Latest)]);
        let req = ToolRequirement {
            version_spec: ConfigVersionSpec::Latest,
            dependencies: deps,
            ..Default::default()
        };
        // rsgain has CrossStep dep on ffmpeg → NOT in same-step list
        let ids = collect_same_step_dep_ids("rsgain", &req, known_dependency_type);
        assert!(ids.is_empty());
    }

    // Phase 0 — compute_composite_canonical_version tests
    // ---------------------------------------------------------------------------

    #[test]
    fn compute_composite_canonical_version_no_deps() {
        let req = ToolRequirement::default();
        let live_state = HashMap::new();
        let result = compute_composite_canonical_version("v1.0", "ffmpeg", &req, &live_state);
        assert_eq!(result, "v1.0");
    }

    #[test]
    fn compute_composite_canonical_version_with_same_step_deps() {
        // Use VersionSpec::Exact so spec_matches_entry returns true.
        let deps = BTreeMap::from([(
            "ffmpeg".to_string(),
            ConfigVersionSpec::Exact(VersionSpecFields {
                tag: Some("v7.1".to_string()),
                version: None,
                vcs_hash: None,
            }),
        )]);
        let req = ToolRequirement {
            version_spec: ConfigVersionSpec::Latest,
            dependencies: deps,
            ..Default::default()
        };
        let mut live_state: HashMap<String, Vec<ToolRegistryEntry>> = HashMap::new();
        live_state.insert(
            "ffmpeg".to_string(),
            vec![ToolRegistryEntry {
                tool_id: "ffmpeg".to_string(),
                version: "v7.1".to_string(),
                canonical_version: "ffmpeg-v7.1".to_string(),
                content_map_hash: "blake3:abc".to_string(), // non-empty → matched
                deployed_at: 0,
                resolved_tag: "v7.1".to_string(),
                resolved_version: "7.1".to_string(),
                resolved_vcs_hash: "abc123".to_string(),
            }],
        );
        let result = compute_composite_canonical_version("yt-dlp-v2", "yt-dlp", &req, &live_state);
        assert_eq!(result, "yt-dlp-v2;ffmpeg:ffmpeg-v7.1");
    }

    #[test]
    fn compute_composite_canonical_version_with_same_step_deps_inherit() {
        // SameStep deps using Inherit — spec_matches_entry returns false for
        // Inherit, so the old code would fail to find the dep and return bare.
        // The fix: for Inherit/Latest, match any active entry.
        let deps = BTreeMap::from([("ffmpeg".to_string(), ConfigVersionSpec::Inherit)]);
        let req = ToolRequirement {
            version_spec: ConfigVersionSpec::Latest,
            dependencies: deps,
            ..Default::default()
        };
        let mut live_state: HashMap<String, Vec<ToolRegistryEntry>> = HashMap::new();
        live_state.insert(
            "ffmpeg".to_string(),
            vec![ToolRegistryEntry {
                tool_id: "ffmpeg".to_string(),
                version: "v7.1".to_string(),
                canonical_version: "ffmpeg-v7.1".to_string(),
                content_map_hash: "blake3:abc".to_string(), // non-empty → matched
                deployed_at: 0,
                resolved_tag: "v7.1".to_string(),
                resolved_version: "7.1".to_string(),
                resolved_vcs_hash: "abc123".to_string(),
            }],
        );
        let result = compute_composite_canonical_version("yt-dlp-v2", "yt-dlp", &req, &live_state);
        assert_eq!(
            result, "yt-dlp-v2;ffmpeg:ffmpeg-v7.1",
            "Inherit dep specs must find active entry and include its version in composite"
        );
    }

    #[test]
    fn compute_composite_canonical_version_with_latest_dep() {
        // Latest dep spec — same fix as Inherit: match any active entry.
        let deps = BTreeMap::from([("ffmpeg".to_string(), ConfigVersionSpec::Latest)]);
        let req = ToolRequirement {
            version_spec: ConfigVersionSpec::Latest,
            dependencies: deps,
            ..Default::default()
        };
        let mut live_state: HashMap<String, Vec<ToolRegistryEntry>> = HashMap::new();
        live_state.insert(
            "ffmpeg".to_string(),
            vec![ToolRegistryEntry {
                tool_id: "ffmpeg".to_string(),
                version: "v7.1".to_string(),
                canonical_version: "ffmpeg-v7.1".to_string(),
                content_map_hash: "blake3:abc".to_string(),
                deployed_at: 0,
                resolved_tag: "v7.1".to_string(),
                resolved_version: "7.1".to_string(),
                resolved_vcs_hash: "abc123".to_string(),
            }],
        );
        let result = compute_composite_canonical_version("yt-dlp-v2", "yt-dlp", &req, &live_state);
        assert_eq!(
            result, "yt-dlp-v2;ffmpeg:ffmpeg-v7.1",
            "Latest dep specs must find active entry and include its version in composite"
        );
    }

    // Phase 7 — index_managed_tools tests
    // ---------------------------------------------------------------------------

    #[test]
    fn index_managed_tools_empty() {
        let map = index_managed_tools(&[]);
        assert!(map.is_empty());
    }

    #[test]
    fn index_managed_tools_single_tool() {
        let entries = vec![ToolRegistryEntry {
            tool_id: "ffmpeg".to_string(),
            version: String::new(),
            canonical_version: "v7.1".to_string(),
            content_map_hash: String::new(),
            deployed_at: 0,
            resolved_tag: String::new(),
            resolved_version: String::new(),
            resolved_vcs_hash: String::new(),
        }];
        let map = index_managed_tools(&entries);
        assert_eq!(map.len(), 1);
        assert_eq!(map["ffmpeg"].len(), 1);
    }

    #[test]
    fn index_managed_tools_multi_instance() {
        let entries = vec![
            ToolRegistryEntry {
                tool_id: "ffmpeg".to_string(),
                version: String::new(),
                canonical_version: "ffmpeg-v7.1".to_string(),
                content_map_hash: String::new(),
                deployed_at: 0,
                resolved_tag: String::new(),
                resolved_version: String::new(),
                resolved_vcs_hash: String::new(),
            },
            ToolRegistryEntry {
                tool_id: "ffmpeg".to_string(),
                version: String::new(),
                canonical_version: "ffmpeg-v6.0".to_string(),
                content_map_hash: String::new(),
                deployed_at: 0,
                resolved_tag: String::new(),
                resolved_version: String::new(),
                resolved_vcs_hash: String::new(),
            },
        ];
        let map = index_managed_tools(&entries);
        assert_eq!(map.len(), 1);
        assert_eq!(map["ffmpeg"].len(), 2);
    }

    // Phase 7 — Inactive tool regression tests
    // ---------------------------------------------------------------------------

    #[test]
    fn regression_inactive_index_managed_tools() {
        // An entry with empty content_map_hash is still indexed (the inactive
        // filter is applied at skip-check time, not at index time).
        let entries = vec![ToolRegistryEntry {
            tool_id: "ffmpeg".to_string(),
            version: String::new(),
            canonical_version: "ffmpeg-v7.1".to_string(),
            content_map_hash: String::new(), // inactive
            deployed_at: 0,
            resolved_tag: String::new(),
            resolved_version: String::new(),
            resolved_vcs_hash: String::new(),
        }];
        let map = index_managed_tools(&entries);
        assert_eq!(map.len(), 1, "inactive entry should still be indexed");
        assert_eq!(map["ffmpeg"].len(), 1);
    }

    #[test]
    fn regression_active_only_skips() {
        // Two entries with the same canonical_version and non-empty
        // content_map_hash → both active, skip check matches.
        let entries = vec![
            ToolRegistryEntry {
                tool_id: "ffmpeg".to_string(),
                version: String::new(),
                canonical_version: "ffmpeg-v7.1".to_string(),
                content_map_hash: "blake3:abc".to_string(),
                deployed_at: 0,
                resolved_tag: String::new(),
                resolved_version: String::new(),
                resolved_vcs_hash: String::new(),
            },
            ToolRegistryEntry {
                tool_id: "ffmpeg".to_string(),
                version: String::new(),
                canonical_version: "ffmpeg-v7.1".to_string(),
                content_map_hash: "blake3:def".to_string(),
                deployed_at: 0,
                resolved_tag: String::new(),
                resolved_version: String::new(),
                resolved_vcs_hash: String::new(),
            },
        ];
        let map = index_managed_tools(&entries);
        assert_eq!(map.len(), 1);
        assert_eq!(map["ffmpeg"].len(), 2);
    }
}
