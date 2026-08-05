//! Tool-reconciliation coordinator.
//!
//! This module orchestrates the full tool-sync lifecycle:
//! 1. Ensure conductor documents exist (generated + state)
//! 2. Load the generated document
//! 3. Fetch desired tool payloads, import to CAS, build content maps
//! 4. Build proper `ToolSpec` + `ToolRuntime` for each tool
//! 5. Apply lifecycle transitions (tag updates, launcher files)
//! 6. Write generated runtime env file
/// 7. Save the generated document
pub(crate) mod external_data;
pub(crate) mod lifecycle;
pub(crate) mod provision;

use std::collections::{BTreeMap, HashMap, HashSet};
use std::fmt::Write;
use std::path::Path;
use std::sync::Arc;

use mediapm_cas::{CasApi, Hash};
use mediapm_conductor::cache::Cache;
use mediapm_conductor::cache::CacheDomainConfig;
use mediapm_conductor::cache_user_level::default_mediapm_user_download_cache_root;
use mediapm_conductor::provision::retain_only_tool_dirs;
use mediapm_conductor::runtime_env::write_generated_dotenv;
use mediapm_conductor::tools::provider::{ConfigVersionSpec, VersionSpec};
use mediapm_conductor::tools::spec::spec_matches_entry;
use mediapm_conductor::{NickelDocument, ToolRuntime, ToolSpec};

use crate::tools::dependency::DependencyTypes;
use crate::tools::provider::RecheckPolicy;

use crate::conductor_bridge::documents::{
    apply_builtin_runtime_defaults, load_conductor_generated_document,
    load_conductor_user_document, register_missing_builtin_tools,
    save_conductor_generated_document,
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
    /// Number of tool entries removed from the generated conductor document
    /// during the wholesale rewrite (condition 3 of the two-file model):
    /// stale managed versions whose `content_map` was cleared, plus manual
    /// entries mediapm did not produce (they belong in the user-owned
    /// `mediapm.conductor.ncl`).
    pub(crate) pruned_tools: usize,
    /// Non-fatal warnings collected during reconciliation.
    pub(crate) warnings: Vec<String>,
    /// Per-tool deployment records populated during provisioning.
    /// Flat list ordered by iteration order of `desired_tools`.
    pub(crate) tool_records: Vec<ToolRegistryEntry>,
    /// Skip-path backfill records: fresh provider-resolved metadata for tools
    /// that were skipped because their canonical version was already
    /// provisioned. Applied in place to the persisted registry by the service
    /// layer (fills `None` resolved fields only).
    pub(crate) resolved_field_backfills: Vec<ToolRegistryEntry>,
}

/// Applies skip-path resolved-field backfills to the persisted managed-tool
/// registry in place.
///
/// For each backfill entry, finds the stored entry with the same
/// `(tool_id, canonical_version)` and fills only `None` resolved_* fields
/// from the backfill's fresh provider metadata. Existing `Some` values are
/// never overwritten, identity fields (`version`, `canonical_version`,
/// `content_map_hash`, `deployed_at`) are preserved, and why-empty fields
/// stay `None` (backfills never invent values — providers return `None` for
/// them). No-op when nothing differs, keeping re-sync state.json
/// byte-identical.
pub(crate) fn apply_resolved_field_backfills(
    managed_tools: &mut [ToolRegistryEntry],
    backfills: &[ToolRegistryEntry],
) {
    for backfill in backfills {
        let Some(existing) = managed_tools.iter_mut().find(|e| {
            e.tool_id == backfill.tool_id && e.canonical_version == backfill.canonical_version
        }) else {
            continue;
        };
        if existing.resolved_tag.is_none() {
            existing.resolved_tag.clone_from(&backfill.resolved_tag);
        }
        if existing.resolved_version.is_none() {
            existing.resolved_version.clone_from(&backfill.resolved_version);
        }
        if existing.resolved_vcs_hash.is_none() {
            existing.resolved_vcs_hash.clone_from(&backfill.resolved_vcs_hash);
        }
    }
}

/// A single entry in the provisioning pipeline.
struct ProvisionEntry {
    /// Bare `tool_id` used for provider resolution (e.g., "ffmpeg").
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

/// Build in-memory index from flat state Vec for O(1) `tool_id` group lookup.
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
/// provision first (making dep `canonical_versions` available for composites).
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

/// Build composite `canonical_version` from bare version and same-step dep
/// version pairs. Dep identifiers are bare `dep_ids` (not `PKeys`), sorted
/// alphabetically for determinism.
///
/// Format: `<bare>;<dep_id_1>:<dep_ver_1>;<dep_id_2>:<dep_ver_2>;...`
fn composite_canonical_version(bare: &str, dep_versions: &[(&str, &str)]) -> String {
    if dep_versions.is_empty() {
        return bare.to_string();
    }
    let mut sorted: Vec<(&str, &str)> = dep_versions.to_vec();
    sorted.sort_by(|a, b| a.0.cmp(b.0));
    let suffix: String = sorted.iter().fold(String::new(), |mut acc, (dep_id, ver)| {
        let _ = write!(acc, ";{dep_id}:{ver}");
        acc
    });
    format!("{bare}{suffix}")
}

/// Collect same-step `dep_ids` for a given entry (returns bare `dep_ids`).
///
/// A dependency carrying both roles contributes its same-step role.
fn collect_same_step_dep_ids(
    tool_id: &str,
    tool_req: &ToolRequirement,
    known_dep_type: fn(&str, &str) -> Option<DependencyTypes>,
) -> Vec<String> {
    tool_req
        .dependencies
        .keys()
        .filter_map(|dep_id| {
            if !known_dep_type(tool_id, dep_id).is_some_and(DependencyTypes::contains_same_step) {
                return None;
            }
            Some(dep_id.clone())
        })
        .collect()
}

/// Extract a tool's own version segment from a possibly-composite
/// `canonical_version`.
///
/// Composite format is `<bare>;<dep_id>:<dep_ver>;...`; bare versions never
/// contain `;`. Dependencies are **direct-only and non-transitive**: a
/// composite segment must reference a dep's OWN version segment, never the
/// dep's composite — nesting a composite inside another would create a
/// transitive cascade (a dep's deps would leak into the requester's
/// identity).
#[must_use]
fn own_version_segment(canonical: &str) -> &str {
    canonical.split(';').next().unwrap_or(canonical)
}

/// Inline direct same-step dependency payload maps into a requester's content
/// map under `deps/<dep_id>/<key>`.
///
/// Dependencies are **direct-only and non-transitive**: each dep's OWN payload
/// keys are copied under the dep's bare mediapm tool id, and a dep's own
/// inlined `deps/...` entries are never re-inlined — `deps/` never nests.
/// Deps absent from `provisioned_own_maps` (skipped or failed provisioning)
/// contribute nothing.
///
/// Returns the inlined key → hash entries; the requester's own keys are not
/// touched. The `known_dep_type` parameter mirrors
/// [`collect_same_step_dep_ids`] and is injectable for tests.
fn inline_same_step_deps(
    tool_id: &str,
    tool_req: &ToolRequirement,
    provisioned_own_maps: &BTreeMap<String, BTreeMap<String, String>>,
    known_dep_type: fn(&str, &str) -> Option<DependencyTypes>,
) -> BTreeMap<String, String> {
    let mut inlined = BTreeMap::new();
    for dep_id in collect_same_step_dep_ids(tool_id, tool_req, known_dep_type) {
        let Some(own_map) = provisioned_own_maps.get(&dep_id) else {
            continue; // dep not provisioned this pass — nothing to inline
        };
        for (key, hash) in own_map {
            // Own maps are pre-inline by construction; defensively skip any
            // residual `deps/` prefix so inlined entries never nest
            // (non-transitive invariant).
            if key.starts_with("deps/") {
                continue;
            }
            inlined.insert(format!("deps/{dep_id}/{key}"), hash.clone());
        }
    }
    inlined
}

/// Strip `deps/`-prefixed keys from a content map, recovering a tool's own
/// (pre-inline) payload map.
///
/// Used to reconstruct own maps for deps that were skipped on a re-sync: the
/// generated doc runtime carries inlined `deps/...` entries, but only the
/// dep's own payload keys may be re-inlined into a requester.
#[must_use]
fn strip_inlined_deps_keys(content_map: &BTreeMap<String, String>) -> BTreeMap<String, String> {
    content_map
        .iter()
        .filter(|(key, _)| !key.starts_with("deps/"))
        .map(|(key, hash)| (key.clone(), hash.clone()))
        .collect()
}

/// Compute `canonical_version` for persistence, including same-step dep versions.
///
/// The canonical version stored in [`ToolRegistryEntry`] is a composite of the
/// bare provider-resolved version and same-step dependency versions. This
/// ensures skip detection works correctly — when a same-step dep version
/// changes, the composite changes and triggers re-provisioning.
///
/// For tools without same-step deps, returns the bare version unchanged.
///
/// Dependency versions are **non-transitive**: each `dep_id:dep_ver` segment
/// carries the dep's OWN version segment (via [`own_version_segment`]), never
/// the dep's full composite.
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
                        e.resolved_tag.as_deref(),
                        e.resolved_version.as_deref(),
                        e.resolved_vcs_hash.as_deref(),
                    ),
                }
            })?;
            // Non-transitive: reference the dep's OWN version segment. A dep
            // that is also an explicitly configured tool with its own
            // same-step deps carries a composite canonical_version; nesting
            // it would leak the dep's deps transitively into the requester.
            Some((dep_id.as_str(), own_version_segment(&matched.canonical_version)))
        })
        .collect();
    composite_canonical_version(bare, &dep_versions)
}

/// Find the active spec for a logical tool name in a generated document.
///
/// The generated doc may hold several specs with the same bare name: pruned
/// stale versions keep the name with an emptied `content_map` while the active
/// version carries the payload map. The active tool is therefore the spec
/// whose `runtime.content_map` is non-empty. This is the single authoritative
/// resolution used by the reconcile skip paths and by callers that need the
/// current managed-tool identity (e.g. the demo examples).
///
/// Resolution contract:
/// - Prefer the first spec (deterministic `BTreeMap` key order) whose
///   `runtime.content_map` is non-empty — that is the active entry.
/// - Fall back to the first spec matching `tool_name` (any content map) so a
///   no-payload tool (empty map) still resolves deterministically.
/// - Return `None` when no spec matches `tool_name`.
///
/// Returns the generated-doc key and the matched spec.
#[must_use]
pub fn find_active_tool_spec<'a>(
    doc: &'a NickelDocument,
    tool_name: &str,
) -> Option<(&'a String, &'a ToolSpec)> {
    let mut fallback: Option<(&'a String, &'a ToolSpec)> = None;
    for (key, spec) in &doc.tools {
        if spec.name != tool_name {
            continue;
        }
        if !spec.runtime.content_map.is_empty() {
            return Some((key, spec));
        }
        if fallback.is_none() {
            fallback = Some((key, spec));
        }
    }
    fallback
}

/// Runs the full tool-reconciliation cycle for the current workspace.
///
/// # Errors
///
/// Returns an error when any critical step (document loading, builtin
/// registration, content-map import) fails. Non-critical failures are
/// reported as warnings in [`ToolSyncReport`].
#[expect(
    clippy::too_many_lines,
    reason = "reconciliation runs the provisioning phase sequence in strict order; splitting would obscure the ordering invariant"
)]
#[expect(
    clippy::too_many_arguments,
    reason = "reconciliation entrypoint; all 8 parameters are distinct required inputs"
)]
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

    // 1a. Load the user-owned conductor document, if present, rejecting
    //     reserved-namespace collisions (two-file model, condition 2). The
    //     user doc is never a reconcile save target — manual tools belong
    //     there and are merged by the conductor at load time.
    load_conductor_user_document(paths)?;

    // 2. Register missing builtin tool definitions and config stubs.
    register_missing_builtin_tools(&mut generated_doc);
    apply_builtin_runtime_defaults(&mut generated_doc);

    // 3. Provision desired tools: download payloads, import to CAS, build
    //    content maps and tool specs.
    // Keys are mediapm conductor tool ids — the generated doc `tools` map
    // keys (`{name}@{content_map_hash}` when the content map is non-empty,
    // bare `{name}` otherwise) — so env payload paths and the provision
    // cache retain set match the ProvisionCache deployment layout.
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

    let mut pruned_tools: usize = 0;
    // Own (pre-inline) content maps for tools processed this pass, keyed by
    // bare mediapm tool id. Requesters re-inline direct same-step deps under
    // `deps/<dep_id>/` from these maps, so they must hold each dep's OWN
    // payload keys only — never inlined `deps/` entries (non-transitive).
    let mut provisioned_own_maps: BTreeMap<String, BTreeMap<String, String>> = BTreeMap::new();
    for entry in &entries {
        let tool_id = &entry.tool_id;
        let tool_req = &entry.tool_requirement;
        let is_builtin_code = is_builtin_source_ingest_requirement(tool_id);
        let already_exists = generated_doc.tools.values().any(|s| s.name == *tool_id);

        // --- Spec-based skip: if desired spec is already satisfied, skip. ---
        if tool_req.version_spec != ConfigVersionSpec::Latest
            && tool_req.version_spec != ConfigVersionSpec::Inherit
            && let Some(entry) = state.managed_tools.iter().find(|e| e.tool_id == *tool_id)
        {
            // Convert ConfigVersionSpec to VersionSpec for spec matching.
            // At this point we know it's Exact (guarded by the != Latest/Inherit check above).
            let resolved_spec = match &tool_req.version_spec {
                ConfigVersionSpec::Exact(fields) => VersionSpec::Exact(fields.clone()),
                _ => unreachable!(), // Latest/Inherit already filtered above
            };
            if spec_matches_entry(
                &resolved_spec,
                entry.resolved_tag.as_deref(),
                entry.resolved_version.as_deref(),
                entry.resolved_vcs_hash.as_deref(),
            ) {
                // Already have the desired version — skip provisioning.
                // Reconstruct the runtime under its conductor tool id
                // (generated doc key). The active `{name}@{hash}` entry wins;
                // stale pruned keys have cleared maps and a bare stale entry
                // may linger.
                if let Some((key, spec)) = find_active_tool_spec(&generated_doc, tool_id) {
                    tool_runtimes.entry(key.clone()).or_insert(spec.runtime.clone());
                    // Track the dep's own (pre-inline) payload map so
                    // requesters processed later in this pass can re-inline
                    // it under `deps/<tool_id>/`.
                    provisioned_own_maps.insert(
                        tool_id.clone(),
                        strip_inlined_deps_keys(&spec.runtime.content_map),
                    );
                }
                report.tools_skipped += 1;
                pb.advance(1);
                continue;
            }
        }
        // --- End spec-based skip ---

        // Initialized in the Ok(fetch) arm before the skip check;
        // used in the Ok(None) payload branch below. String::new() is
        // the dead initial value because the assignment in the match
        // arm always runs before any read (other paths `continue`).
        #[allow(unused_assignments)]
        let mut resolved_canonical_version = String::new();
        // Captured from provider metadata in the Ok(fetch) arm before the skip
        // check; used in the Ok(None) payload branch below. None is the dead
        // initial value because the assignment in the match arm always runs
        // before any read (other paths `continue`).
        #[allow(unused_assignments)]
        let mut resolved_tag_value: Option<String> = None;
        #[allow(unused_assignments)]
        let mut resolved_version_value: Option<String> = None;
        #[allow(unused_assignments)]
        let mut resolved_vcs_hash_value: Option<String> = None;
        let pre_resolved = match provider::resolve_tool_fetch(
            tool_id,
            Some((&*cache, "tool_metadata")),
            recheck_policy,
        )
        .await
        {
            Ok((fetch, metadata)) => {
                let human_readable_version = metadata.human_readable_version.clone();
                let canonical_version = metadata.canonical_version.clone();
                let metadata_cached = metadata.metadata_cached;
                let metadata_fetch_count = metadata.metadata_fetch_count;
                resolved_canonical_version.clone_from(&canonical_version);
                resolved_tag_value.clone_from(&metadata.resolved_tag);
                resolved_version_value.clone_from(&metadata.resolved_version);
                resolved_vcs_hash_value.clone_from(&metadata.resolved_vcs_hash);

                // --- Post-resolve validation: verify resolved result matches desired spec ---
                match &tool_req.version_spec {
                    ConfigVersionSpec::Exact(fields) => {
                        if let Some(hash) = &fields.vcs_hash {
                            // A `None` resolved tag never satisfies the hash
                            // check; only the canonical version may match.
                            if resolved_canonical_version != *hash
                                && resolved_tag_value.as_deref() != Some(hash.as_str())
                            {
                                return Err(MediaPmError::Workflow(format!(
                                    "tool {tool_id}: requested vcs_hash {hash} but resolved canonical {resolved_canonical_version} and tag {}",
                                    resolved_tag_value.as_deref().unwrap_or("(none)")
                                )));
                            }
                        }
                        if let Some(tag) = &fields.tag
                            && resolved_tag_value.as_deref() != Some(tag.as_str())
                        {
                            return Err(MediaPmError::Workflow(format!(
                                "tool {tool_id}: requested tag {tag} but resolved {}",
                                resolved_tag_value.as_deref().unwrap_or("(none)")
                            )));
                        }
                        if let Some(ver) = &fields.version
                            && human_readable_version != *ver
                        {
                            return Err(MediaPmError::Workflow(format!(
                                "tool {tool_id}: requested version {ver} but resolved {human_readable_version}"
                            )));
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
                        metadata_cached,
                        metadata_fetch_count,
                        resolved_tag: metadata.resolved_tag.clone(),
                        resolved_version: metadata.resolved_version.clone(),
                        resolved_vcs_hash: metadata.resolved_vcs_hash.clone(),
                    }
                } else {
                    // The composite canonical_version (including same-step dep
                    // versions) overrides the provider's canonical_version for
                    // provisioning identity.
                    let mut provision_metadata = metadata;
                    provision_metadata.canonical_version = expected_composite;
                    PreResolveOutcome::Resolved(fetch, provision_metadata)
                }
            }
            Err(e) => {
                let error_bar = effective_group.add_bar(1, &format!("{tool_id} [res]"));
                error_bar.finish_error();
                report.warnings.push(format!(
                    "tool {tool_id}: resolve failed (will retry on next sync): {e}",
                ));
                pb.advance(1);
                continue;
            }
        };

        let was_skip = matches!(&pre_resolved, PreResolveOutcome::Skip { .. });
        // Capture fresh resolved metadata for the skip backfill BEFORE
        // pre_resolved is moved into fetch_and_import_tool_payload below.
        let skip_backfill: Option<ToolRegistryEntry> = match &pre_resolved {
            PreResolveOutcome::Skip {
                name,
                version,
                resolved_tag,
                resolved_version,
                resolved_vcs_hash,
                ..
            } => Some(ToolRegistryEntry {
                tool_id: name.clone(),
                version: String::new(),
                canonical_version: version.clone(),
                content_map_hash: String::new(),
                deployed_at: mediapm_utils::Timestamp::default(),
                resolved_tag: resolved_tag.clone(),
                resolved_version: resolved_version.clone(),
                resolved_vcs_hash: resolved_vcs_hash.clone(),
            }),
            PreResolveOutcome::Resolved(..) => None,
        };
        let payload_result =
            fetch_and_import_tool_payload(cas, tool_id, &cache, effective_group, pre_resolved)
                .await;

        if was_skip {
            // Skipped tools still need env var entries. Reconstruct the
            // runtime under its conductor tool id (generated doc key): the
            // active `{name}@{hash}` entry with a non-empty content map wins
            // (stale pruned keys have cleared maps).
            if let Some((key, spec)) = find_active_tool_spec(&generated_doc, tool_id) {
                tool_runtimes.entry(key.clone()).or_insert(spec.runtime.clone());
                // Track the dep's own (pre-inline) payload map — the doc
                // runtime carries inlined `deps/...` entries, so strip them
                // before storing the own map for re-inlining.
                provisioned_own_maps
                    .insert(tool_id.clone(), strip_inlined_deps_keys(&spec.runtime.content_map));
            }
            // Backfill fresh resolved metadata into the persisted registry.
            if let Some(backfill) = skip_backfill {
                report.resolved_field_backfills.push(backfill);
            }
            report.tools_skipped += 1;
            pb.advance(1);
            continue;
        }

        match payload_result {
            Ok(Some(mut payload)) => {
                // Track the tool's own (pre-inline) content map BEFORE
                // inlining so requesters later in this pass can re-inline it
                // under `deps/<tool_id>/`.
                provisioned_own_maps.insert(tool_id.clone(), payload.content_map.clone());

                // Inline direct same-step dependency payloads under
                // `deps/<dep_id>/<key>`. The hash below is computed AFTER
                // this extend so the tool key and skip identity reflect the
                // inlined deps.
                payload.content_map.extend(inline_same_step_deps(
                    tool_id,
                    tool_req,
                    &provisioned_own_maps,
                    crate::tools::dependency::known_dependency_type,
                ));

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
                let now = mediapm_utils::Timestamp::now();
                report.tool_records.push(ToolRegistryEntry {
                    tool_id: tool_id.clone(),
                    version: payload.human_readable_version.clone(),
                    canonical_version: payload.canonical_version.clone(),
                    content_map_hash: content_map_hash.clone(),
                    deployed_at: now,
                    resolved_tag: payload.resolved_tag.clone(),
                    resolved_version: payload.resolved_version.clone(),
                    resolved_vcs_hash: payload.resolved_vcs_hash.clone(),
                });

                // Update live_state for subsequent entries in the same sync.
                let entry_for_live = report.tool_records.last().unwrap().clone();
                live_state.entry(tool_id.clone()).or_default().push(entry_for_live.clone());

                // Inject inherited_env_vars from requirement config.
                let inherited = inherited_env_vars.get(tool_id).cloned().unwrap_or_default();

                let mut full_runtime = runtime.clone();
                full_runtime.inherited_env_vars = inherited;

                // Use content-addressed key: "{name}@{hash}".
                let tool_key = if content_map_hash.is_empty() {
                    tool_id.clone()
                } else {
                    format!("{tool_id}@{content_map_hash}")
                };

                // Prune old version keys from generated documents.
                let prefix = format!("{tool_id}@");
                let old: Vec<String> = generated_doc
                    .tools
                    .keys()
                    .filter(|k| (k.starts_with(&prefix) || *k == tool_id) && *k != &tool_key)
                    .cloned()
                    .collect();
                pruned_tools += old.len();
                for k in &old {
                    // Clear content_map instead of removing the entry.
                    // User-added manual entries (whose bare tool_id is not in
                    // used_tool_ids) are never touched.
                    if let Some(spec) = generated_doc.tools.get_mut(k) {
                        spec.runtime.content_map.clear();
                    }
                }

                generated_doc.tools.entry(tool_key.clone()).or_insert(spec);
                // Key by the conductor tool id (the generated doc key) so
                // env paths match the ProvisionCache deployment layout.
                tool_runtimes.insert(tool_key.clone(), full_runtime);
            }
            Ok(None) => {
                // No payload fetched (internal launcher, no catalog entry,
                // or no host-OS action). Create a minimal spec without
                // content map so the tool is still registered. The own map
                // is empty — nothing to inline for requesters.
                provisioned_own_maps.insert(tool_id.clone(), BTreeMap::new());
                let runtime = ToolRuntime {
                    impure: false,
                    inherited_env_vars: inherited_env_vars
                        .get(tool_id)
                        .cloned()
                        .unwrap_or_default(),
                    ..ToolRuntime::default()
                };
                // Key by the conductor tool id — bare form here because
                // there is no content map.
                tool_runtimes.insert(tool_id.clone(), runtime.clone());

                // Record deployment metadata (no payload — builtin or launcher).
                let now = mediapm_utils::Timestamp::now();
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
                    resolved_version: resolved_version_value.clone(),
                    resolved_vcs_hash: resolved_vcs_hash_value.clone(),
                });

                // Update live_state for subsequent entries in the same sync.
                let entry_for_live = report.tool_records.last().unwrap().clone();
                live_state.entry(tool_id.clone()).or_default().push(entry_for_live.clone());

                if !already_exists && !is_builtin_code {
                    report.tools_added += 1;
                }

                if generated_doc.tools.contains_key(tool_id) {
                    report.tools_updated += 1;
                } else {
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

    // ── Generated-doc purity (condition 3) ───────────────────────────────
    // The generated document is a pure machine artifact: drop any tool
    // entries mediapm did not produce this sync (hand-added manual entries).
    // Retain everything the provisioning pipeline manages — explicit tools
    // AND their companion dependencies (each gets its own entry from
    // `entries`) plus conductor builtins — so stale versions of managed
    // tools keep their emptied `content_map` (see the per-entry pruning
    // above) and companion tools written this sync (or skipped from an
    // earlier sync) survive. Anything else belongs in the user-owned
    // `mediapm.conductor.ncl`, never here.
    let provisioned_names: HashSet<&str> =
        entries.iter().map(|entry| entry.tool_id.as_str()).collect();
    let builtin_names: HashSet<&str> =
        mediapm_conductor::tools::ALL_BUILTINS.iter().map(|builtin| builtin.name).collect();
    let tools_before_rewrite = generated_doc.tools.len();
    generated_doc.tools.retain(|key, _| {
        let bare = key.split('@').next().unwrap_or(key.as_str());
        provisioned_names.contains(bare) || builtin_names.contains(bare)
    });
    pruned_tools += tools_before_rewrite - generated_doc.tools.len();

    // ── external_data: independent post-processing ──────────────────────
    // Rebuild external_data from scratch by scanning all tool specs'
    // content_maps. Hashes not referenced by any tool are automatically
    // excluded — no separate cleanup needed.
    let mut data_usage = self::external_data::DataUsageTracker::new();
    for spec in generated_doc.tools.values() {
        for hash_str in spec.runtime.content_map.values() {
            if let Ok(hash) = hash_str.parse::<Hash>() {
                data_usage.record(hash, format!("managed tool content root for {}", spec.name));
            }
        }
    }
    generated_doc.external_data = data_usage.finalize();

    // 4. Ensure the tools runtime directory exists.
    std::fs::create_dir_all(&paths.tools_dir).map_err(|source| MediaPmError::Io {
        operation: "creating tools directory".to_string(),
        path: paths.tools_dir.clone(),
        source,
    })?;

    // 5. Write generated runtime env file from tool runtimes (keyed by
    //    conductor tool id — env names derive from the stripped mediapm id,
    //    path values from the sanitized conductor id).
    write_generated_dotenv(&paths.runtime_root, &paths.tools_dir, &tool_runtimes)?;

    // 5. Save generated document.
    save_conductor_generated_document(paths, &generated_doc)?;

    // 6. Prune filesystem tool directories not in the active set. The
    //    provision cache keys directories by the sanitized conductor tool
    //    id (`tools_dir/<sanitize_tool_id(conductor_tool_id)>/payload/`),
    //    so the retain set must be conductor tool ids (the `tool_runtimes`
    //    keys), never mediapm tool ids — a mediapm-id set would prune every
    //    provisioned directory.
    let active_conductor_ids: HashSet<String> = tool_runtimes.keys().cloned().collect();
    retain_only_tool_dirs(paths.tools_dir.clone(), active_conductor_ids).await?;

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
    use crate::tools::dependency::DependencyTypes;
    use crate::tools::dependency::known_dependency_type;

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

        assert!(result.is_ok(), "reconcile_desired_tools failed: {:?}", result.err());

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
            ops.iter().filter(|op| matches!(op, ProgressOp::FinishSuccess)).collect();
        assert_eq!(
            finish_successes.len(),
            1,
            "expected exactly one FinishSuccess op, got {finish_successes:?}",
        );
        assert!(
            matches!(&finish_successes[0], ProgressOp::FinishSuccess),
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
            deployed_at: mediapm_utils::Timestamp::default(),
            resolved_tag: None,
            resolved_version: None,
            resolved_vcs_hash: None,
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

    /// The spec-based skip path reconstructs the runtime under its conductor
    /// tool id (the generated doc key), so env payload paths match the
    /// `ProvisionCache` deployment layout.
    #[tokio::test]
    async fn reconcile_keys_tool_runtimes_by_conductor_tool_id() {
        let tmp = tempfile::tempdir().unwrap();
        let cache_root = tempfile::tempdir().unwrap();
        let paths = MediaPmPaths::from_root(tmp.path());
        let cas = InMemoryCas::default();

        // Pre-populate generated doc with an active `{name}@{hash}` key.
        let mut content_map = BTreeMap::new();
        content_map.insert("linux/yt-dlp".to_string(), "blake3:abc".to_string());
        let tool_spec = ToolSpec {
            name: "yt-dlp".to_string(),
            kind: ToolKindSpec::default(),
            runtime: ToolRuntime { content_map, ..Default::default() },
            ..Default::default()
        };
        let mut tools = BTreeMap::new();
        tools.insert("yt-dlp@blake3:abc".to_string(), tool_spec);
        let doc = NickelDocument { tools, ..Default::default() };
        save_conductor_generated_document(&paths, &doc).expect("pre-save generated doc");

        // State whose resolved version matches the exact spec → spec-based
        // skip fires without any network access.
        let mut state = MediaPmState::default();
        state.managed_tools.push(ToolRegistryEntry {
            tool_id: "yt-dlp".to_string(),
            version: "seeded-version".to_string(),
            canonical_version: "yt-dlp-2024.01.01".to_string(),
            content_map_hash: "blake3:abc".to_string(),
            deployed_at: mediapm_utils::Timestamp::default(),
            resolved_tag: None,
            resolved_version: Some("2024.01.01".to_string()),
            resolved_vcs_hash: None,
        });

        let mut desired_tools = BTreeMap::new();
        let req = ToolRequirement {
            version_spec: mediapm_conductor::tools::provider::ConfigVersionSpec::Exact(
                VersionSpecFields {
                    version: Some("2024.01.01".to_string()),
                    vcs_hash: None,
                    tag: None,
                },
            ),
            ..Default::default()
        };
        desired_tools.insert("yt-dlp".to_string(), serde_json::to_value(req).unwrap());

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
        assert_eq!(result.unwrap().tools_skipped, 1, "exact spec matching stored fields must skip");

        // Env paths must be keyed by the conductor tool id, not the plain id.
        let env_path = &paths.env_generated_file;
        let content = std::fs::read_to_string(env_path).expect("env file readable");
        assert!(
            content.contains("MEDIAPM_YT_DLP_LINUX="),
            "env file should have MEDIAPM_YT_DLP_LINUX\n--- content:\n{content}",
        );
        assert!(
            content.contains("/yt-dlp@blake3_abc/payload/linux/yt-dlp"),
            "env path must use the sanitized conductor tool id\n--- content:\n{content}",
        );
        assert!(
            !content.contains("/yt-dlp/payload/"),
            "env path must not use the plain mediapm tool id\n--- content:\n{content}",
        );
    }

    /// When multiple generated doc entries match a tool (a stale bare entry
    /// with a cleared content map plus the active `{name}@{hash}` entry), the
    /// skip path must prefer the entry with a non-empty content map.
    #[tokio::test]
    async fn reconcile_skip_prefers_entry_with_content_map() {
        let tmp = tempfile::tempdir().unwrap();
        let cache_root = tempfile::tempdir().unwrap();
        let paths = MediaPmPaths::from_root(tmp.path());
        let cas = InMemoryCas::default();

        // Bare stale entry (cleared content map) sorts before the `@` key;
        // the active hashed entry carries the content map.
        let mut content_map = BTreeMap::new();
        content_map.insert("linux/yt-dlp".to_string(), "blake3:abc".to_string());
        let stale_spec = ToolSpec {
            name: "yt-dlp".to_string(),
            kind: ToolKindSpec::default(),
            runtime: ToolRuntime::default(),
            ..Default::default()
        };
        let active_spec = ToolSpec {
            name: "yt-dlp".to_string(),
            kind: ToolKindSpec::default(),
            runtime: ToolRuntime { content_map, ..Default::default() },
            ..Default::default()
        };
        let mut tools = BTreeMap::new();
        tools.insert("yt-dlp".to_string(), stale_spec);
        tools.insert("yt-dlp@blake3:abc".to_string(), active_spec);
        let doc = NickelDocument { tools, ..Default::default() };
        save_conductor_generated_document(&paths, &doc).expect("pre-save generated doc");

        let mut state = MediaPmState::default();
        state.managed_tools.push(ToolRegistryEntry {
            tool_id: "yt-dlp".to_string(),
            version: "seeded-version".to_string(),
            canonical_version: "yt-dlp-2024.01.01".to_string(),
            content_map_hash: "blake3:abc".to_string(),
            deployed_at: mediapm_utils::Timestamp::default(),
            resolved_tag: None,
            resolved_version: Some("2024.01.01".to_string()),
            resolved_vcs_hash: None,
        });

        let mut desired_tools = BTreeMap::new();
        let req = ToolRequirement {
            version_spec: mediapm_conductor::tools::provider::ConfigVersionSpec::Exact(
                VersionSpecFields {
                    version: Some("2024.01.01".to_string()),
                    vcs_hash: None,
                    tag: None,
                },
            ),
            ..Default::default()
        };
        desired_tools.insert("yt-dlp".to_string(), serde_json::to_value(req).unwrap());

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

        // The active hashed entry must win: env paths carry the conductor id.
        let env_path = &paths.env_generated_file;
        let content = std::fs::read_to_string(env_path).expect("env file readable");
        assert!(
            content.contains("MEDIAPM_YT_DLP_LINUX="),
            "env file should have MEDIAPM_YT_DLP_LINUX\n--- content:\n{content}",
        );
        assert!(
            content.contains("/yt-dlp@blake3_abc/payload/linux/yt-dlp"),
            "skip path must prefer the entry with a content map\n--- content:\n{content}",
        );
    }

    /// A stale bare-name entry with a cleared content map plus an active
    /// `{name}@{hash}` entry: the active entry (non-empty content map) wins
    /// regardless of `BTreeMap` key order.
    #[test]
    fn find_active_tool_spec_prefers_non_empty_content_map() {
        let mut content_map = BTreeMap::new();
        content_map.insert("linux/yt-dlp".to_string(), "blake3:abc".to_string());
        let mut tools = BTreeMap::new();
        tools.insert(
            "yt-dlp".to_string(),
            ToolSpec {
                name: "yt-dlp".to_string(),
                kind: ToolKindSpec::default(),
                runtime: ToolRuntime::default(),
                ..Default::default()
            },
        );
        tools.insert(
            "yt-dlp@blake3:abc".to_string(),
            ToolSpec {
                name: "yt-dlp".to_string(),
                kind: ToolKindSpec::default(),
                runtime: ToolRuntime { content_map, ..Default::default() },
                ..Default::default()
            },
        );
        let doc = NickelDocument { tools, ..Default::default() };

        let (key, spec) = find_active_tool_spec(&doc, "yt-dlp").expect("active spec must resolve");
        assert_eq!(key, "yt-dlp@blake3:abc");
        assert_eq!(spec.name, "yt-dlp");
        assert!(!spec.runtime.content_map.is_empty());
    }

    /// No spec carries a content map: resolution falls back to the first
    /// name match in deterministic key order (bare key sorts first).
    #[test]
    fn find_active_tool_spec_falls_back_to_first_name_match() {
        let mut tools = BTreeMap::new();
        tools.insert(
            "yt-dlp@blake3:abc".to_string(),
            ToolSpec {
                name: "yt-dlp".to_string(),
                kind: ToolKindSpec::default(),
                runtime: ToolRuntime::default(),
                ..Default::default()
            },
        );
        tools.insert(
            "yt-dlp@blake3:def".to_string(),
            ToolSpec {
                name: "yt-dlp".to_string(),
                kind: ToolKindSpec::default(),
                runtime: ToolRuntime::default(),
                ..Default::default()
            },
        );
        let doc = NickelDocument { tools, ..Default::default() };

        let (key, spec) =
            find_active_tool_spec(&doc, "yt-dlp").expect("fallback spec must resolve");
        assert_eq!(key, "yt-dlp@blake3:abc");
        assert_eq!(spec.name, "yt-dlp");
    }

    /// No spec matches the logical name at all: `None`.
    #[test]
    fn find_active_tool_spec_none_when_name_missing() {
        let mut content_map = BTreeMap::new();
        content_map.insert("linux/ffmpeg".to_string(), "blake3:abc".to_string());
        let mut tools = BTreeMap::new();
        tools.insert(
            "ffmpeg@blake3:abc".to_string(),
            ToolSpec {
                name: "ffmpeg".to_string(),
                kind: ToolKindSpec::default(),
                runtime: ToolRuntime { content_map, ..Default::default() },
                ..Default::default()
            },
        );
        let doc = NickelDocument { tools, ..Default::default() };

        assert!(find_active_tool_spec(&doc, "yt-dlp").is_none());
    }

    /// Specs with other names never match the queried logical name.
    #[test]
    fn find_active_tool_spec_skips_other_names() {
        let mut content_map = BTreeMap::new();
        content_map.insert("linux/ffmpeg".to_string(), "blake3:abc".to_string());
        let mut tools = BTreeMap::new();
        tools.insert(
            "ffmpeg@blake3:abc".to_string(),
            ToolSpec {
                name: "ffmpeg".to_string(),
                kind: ToolKindSpec::default(),
                runtime: ToolRuntime { content_map, ..Default::default() },
                ..Default::default()
            },
        );
        let doc = NickelDocument { tools, ..Default::default() };

        assert!(find_active_tool_spec(&doc, "yt-dlp").is_none());
    }

    /// The filesystem retain set uses conductor tool ids (the `tool_runtimes`
    /// keys), matching the provision cache's
    /// `<sanitize_tool_id(conductor_tool_id)>` directory layout. A
    /// mediapm-id set would prune every provisioned directory.
    #[tokio::test]
    async fn reconcile_retain_active_set_uses_conductor_tool_ids() {
        let tmp = tempfile::tempdir().unwrap();
        let cache_root = tempfile::tempdir().unwrap();
        let paths = MediaPmPaths::from_root(tmp.path());
        let cas = InMemoryCas::default();

        // Pre-seed the tools dir with a conductor-keyed active dir and a
        // stale dir that must be pruned. Retain-only only removes dirs that
        // carry a `.lock` file, so the stale dir gets one.
        std::fs::create_dir_all(paths.tools_dir.join("yt-dlp@blake3_abc"))
            .expect("create active dir");
        std::fs::create_dir_all(paths.tools_dir.join("stale_dir")).expect("create stale dir");
        std::fs::write(paths.tools_dir.join("stale_dir").join(".lock"), b"")
            .expect("create stale lock file");

        // Generated doc with an active `{name}@{hash}` entry.
        let mut content_map = BTreeMap::new();
        content_map.insert("linux/yt-dlp".to_string(), "blake3:abc".to_string());
        let tool_spec = ToolSpec {
            name: "yt-dlp".to_string(),
            kind: ToolKindSpec::default(),
            runtime: ToolRuntime { content_map, ..Default::default() },
            ..Default::default()
        };
        let mut tools = BTreeMap::new();
        tools.insert("yt-dlp@blake3:abc".to_string(), tool_spec);
        let doc = NickelDocument { tools, ..Default::default() };
        save_conductor_generated_document(&paths, &doc).expect("pre-save generated doc");

        let mut state = MediaPmState::default();
        state.managed_tools.push(ToolRegistryEntry {
            tool_id: "yt-dlp".to_string(),
            version: "seeded-version".to_string(),
            canonical_version: "yt-dlp-2024.01.01".to_string(),
            content_map_hash: "blake3:abc".to_string(),
            deployed_at: mediapm_utils::Timestamp::default(),
            resolved_tag: None,
            resolved_version: Some("2024.01.01".to_string()),
            resolved_vcs_hash: None,
        });

        let mut desired_tools = BTreeMap::new();
        let req = ToolRequirement {
            version_spec: mediapm_conductor::tools::provider::ConfigVersionSpec::Exact(
                VersionSpecFields {
                    version: Some("2024.01.01".to_string()),
                    vcs_hash: None,
                    tag: None,
                },
            ),
            ..Default::default()
        };
        desired_tools.insert("yt-dlp".to_string(), serde_json::to_value(req).unwrap());

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

        // The conductor-keyed dir survives; the stale dir is pruned.
        assert!(
            paths.tools_dir.join("yt-dlp@blake3_abc").exists(),
            "active conductor-keyed dir must survive retain-only",
        );
        assert!(
            !paths.tools_dir.join("stale_dir").exists(),
            "non-active dir must be pruned by retain-only",
        );
    }

    #[tokio::test]
    async fn reconcile_prunes_old_tool_version_clears_content_map() {
        let tmp = tempfile::tempdir().unwrap();
        let cache_root = tempfile::tempdir().unwrap();
        let paths = MediaPmPaths::from_root(tmp.path());
        let cas = InMemoryCas::default();

        // Pre-populate generated doc with an old version key that has a bogus
        // content hash suffix.  This simulates a stale entry from a previous
        // sync whose content_map should be cleared when a fresh key is computed.
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
            deployed_at: mediapm_utils::Timestamp::default(),
            resolved_tag: None,
            resolved_version: None,
            resolved_vcs_hash: None,
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

        // Reload generated doc: the old key must still exist with empty content_map.
        let doc = load_conductor_generated_document(&paths).expect("load generated doc after sync");
        let old_spec = doc
            .tools
            .get("media-tagger@bogus_hash")
            .expect("old version key should still exist after sync");
        assert!(
            old_spec.runtime.content_map.is_empty(),
            "old version key should have empty content_map, got: {:?}",
            old_spec.runtime.content_map
        );

        // The new key should exist with non-empty content_map.
        let has_new_key =
            doc.tools.keys().any(|k| k == "media-tagger" || k.starts_with("media-tagger@"));
        assert!(
            has_new_key,
            "new version key should exist after sync, keys: {:?}",
            doc.tools.keys().collect::<Vec<_>>()
        );
        let new_spec = doc.tools.values().find(|s| s.name == "media-tagger").unwrap();
        assert!(
            !new_spec.runtime.content_map.is_empty(),
            "new version key should have non-empty content_map"
        );
    }

    #[tokio::test]
    async fn reconcile_drops_manual_entries_from_generated_doc() {
        let tmp = tempfile::tempdir().unwrap();
        let cache_root = tempfile::tempdir().unwrap();
        let paths = MediaPmPaths::from_root(tmp.path());
        let cas = InMemoryCas::default();

        // Pre-populate generated doc with a manual entry whose bare tool_id
        // is NOT in the desired set (e.g., "user_script"). Under condition 3
        // (generated-doc purity) the generated document is rewritten
        // wholesale on every sync, so such entries are dropped — manual
        // tools belong in the user-owned `mediapm.conductor.ncl` instead.
        let mut content_map = BTreeMap::new();
        content_map.insert("linux/user_script".to_string(), "blake3:manual".to_string());
        let tool_spec = ToolSpec {
            name: "user_script".to_string(),
            kind: ToolKindSpec::default(),
            runtime: ToolRuntime { content_map, ..Default::default() },
            ..Default::default()
        };
        let mut tools = BTreeMap::new();
        tools.insert("user_script@somehash".to_string(), tool_spec);
        let doc = NickelDocument { tools, ..Default::default() };
        save_conductor_generated_document(&paths, &doc).expect("pre-save generated doc");

        // Empty desired_tools — nothing is "used" so every non-managed entry
        // (the manual one) must be dropped on rewrite.
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
        assert!(
            report.pruned_tools >= 1,
            "manual entry dropped on rewrite must be counted as pruned, got {}",
            report.pruned_tools
        );

        // Verify the manual entry is gone after the wholesale rewrite.
        let doc = load_conductor_generated_document(&paths).expect("load generated doc after sync");
        assert!(
            !doc.tools.contains_key("user_script@somehash"),
            "manual entry must be dropped on generated-doc rewrite",
        );
    }

    #[test]
    fn external_data_rebuilt_independently_from_tool_specs() {
        // Create two tool specs with different content_map hashes using
        // Hash::from for deterministic test values.
        let hash_a = Hash::from([0u8; 32]);
        let hash_b = Hash::from([1u8; 32]);
        let hash_zero_hex = format!("blake3:{}", blake3::Hash::from([0u8; 32]).to_hex());
        let hash_one_hex = format!("blake3:{}", blake3::Hash::from([1u8; 32]).to_hex());

        let mut cm1 = BTreeMap::new();
        cm1.insert("linux/tool_a".to_string(), hash_zero_hex);
        let spec_a = ToolSpec {
            name: "tool_a".to_string(),
            kind: ToolKindSpec::default(),
            runtime: ToolRuntime { content_map: cm1, ..Default::default() },
            ..Default::default()
        };
        let mut cm2 = BTreeMap::new();
        cm2.insert("macos/tool_b".to_string(), hash_one_hex);
        let spec_b = ToolSpec {
            name: "tool_b".to_string(),
            kind: ToolKindSpec::default(),
            runtime: ToolRuntime { content_map: cm2, ..Default::default() },
            ..Default::default()
        };

        // Build external_data from both tool specs.
        let mut data_usage = self::external_data::DataUsageTracker::new();
        for spec in [&spec_a, &spec_b] {
            for hash_str in spec.runtime.content_map.values() {
                if let Ok(hash) = hash_str.parse::<Hash>() {
                    data_usage.record(hash, format!("managed tool content root for {}", spec.name));
                }
            }
        }
        let external_data = data_usage.finalize();

        // Both hashes should be present.
        assert!(external_data.contains_key(&hash_a), "hash_a should be in external_data");
        assert!(external_data.contains_key(&hash_b), "hash_b should be in external_data");
        assert_eq!(external_data.len(), 2, "external_data should have exactly 2 entries");

        // Remove tool_a and verify its hash is excluded.
        let mut data_usage = self::external_data::DataUsageTracker::new();
        for hash_str in spec_b.runtime.content_map.values() {
            if let Ok(hash) = hash_str.parse::<Hash>() {
                data_usage.record(hash, format!("managed tool content root for {}", spec_b.name));
            }
        }
        let external_data_one = data_usage.finalize();

        assert!(!external_data_one.contains_key(&hash_a), "hash_a should be absent after removal");
        assert!(external_data_one.contains_key(&hash_b), "hash_b should remain");
        assert_eq!(external_data_one.len(), 1, "external_data should have 1 entry");
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

    #[allow(clippy::unnecessary_wraps)] // must match the `fn(&str, &str) -> Option<DependencyTypes>` parameter
    fn both_roles_dep_type(_tool_id: &str, _dep_id: &str) -> Option<DependencyTypes> {
        Some(DependencyTypes::SAME_STEP.combine(DependencyTypes::CROSS_STEP))
    }

    #[test]
    fn collect_same_step_dep_ids_combined_roles() {
        // A dependency carrying both roles contributes its same-step role
        // (replaces the removed `DependencyType::Both` semantics).
        let deps = BTreeMap::from([("ffmpeg".to_string(), ConfigVersionSpec::Latest)]);
        let req = ToolRequirement {
            version_spec: ConfigVersionSpec::Latest,
            dependencies: deps,
            ..Default::default()
        };
        let ids = collect_same_step_dep_ids("tool", &req, both_roles_dep_type);
        assert_eq!(ids, vec!["ffmpeg"]);
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

    // Phase 1 — own_version_segment (non-transitive composites)
    // -------------------------------------------------------------------------

    #[test]
    fn own_version_segment_bare_passthrough() {
        assert_eq!(own_version_segment("v1.2.3"), "v1.2.3");
    }

    #[test]
    fn own_version_segment_strips_composite() {
        assert_eq!(own_version_segment("v1.2.3;ffmpeg:abc;deno:def"), "v1.2.3");
    }

    #[test]
    fn own_version_segment_empty() {
        assert_eq!(own_version_segment(""), "");
    }

    #[test]
    fn compute_composite_canonical_version_non_transitive() {
        // A dep that is itself an explicitly configured tool with its own
        // same-step deps carries a composite canonical_version in live_state.
        // The requester composite must reference the dep's OWN version
        // segment, never the dep's composite (no transitive nesting).
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
                version: "ffmpeg-v7.1".to_string(),
                // ffmpeg itself has a same-step dep on "x" at "y" — its
                // canonical_version is a composite.
                canonical_version: "ffmpeg-v7.1;x:y".to_string(),
                content_map_hash: "blake3:abc".to_string(),
                deployed_at: mediapm_utils::Timestamp::default(),
                resolved_tag: Some("v7.1".to_string()),
                resolved_version: Some("7.1".to_string()),
                resolved_vcs_hash: Some("abc123".to_string()),
            }],
        );
        let result = compute_composite_canonical_version("yt-dlp-v2", "yt-dlp", &req, &live_state);
        assert_eq!(
            result, "yt-dlp-v2;ffmpeg:ffmpeg-v7.1",
            "composite must use the dep's own version segment, never the dep's composite"
        );
        assert!(
            !result.contains(";x:y"),
            "no transitive nesting allowed — dep's deps must not leak: got {result}"
        );
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
                deployed_at: mediapm_utils::Timestamp::default(),
                resolved_tag: Some("v7.1".to_string()),
                resolved_version: Some("7.1".to_string()),
                resolved_vcs_hash: Some("abc123".to_string()),
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
                deployed_at: mediapm_utils::Timestamp::default(),
                resolved_tag: Some("v7.1".to_string()),
                resolved_version: Some("7.1".to_string()),
                resolved_vcs_hash: Some("abc123".to_string()),
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
                deployed_at: mediapm_utils::Timestamp::default(),
                resolved_tag: Some("v7.1".to_string()),
                resolved_version: Some("7.1".to_string()),
                resolved_vcs_hash: Some("abc123".to_string()),
            }],
        );
        let result = compute_composite_canonical_version("yt-dlp-v2", "yt-dlp", &req, &live_state);
        assert_eq!(
            result, "yt-dlp-v2;ffmpeg:ffmpeg-v7.1",
            "Latest dep specs must find active entry and include its version in composite"
        );
    }

    // Phase 2 — inline_same_step_deps / strip_inlined_deps_keys tests
    // -------------------------------------------------------------------------

    #[test]
    fn inline_same_step_deps_empty_deps() {
        let req = ToolRequirement::default();
        let maps = BTreeMap::new();
        let result = inline_same_step_deps("yt-dlp", &req, &maps, known_dependency_type);
        assert!(result.is_empty());
    }

    #[test]
    fn inline_same_step_deps_yt_dlp_ffmpeg_deno() {
        let deps = BTreeMap::from([
            ("ffmpeg".to_string(), ConfigVersionSpec::Latest),
            ("deno".to_string(), ConfigVersionSpec::Latest),
        ]);
        let req = ToolRequirement {
            version_spec: ConfigVersionSpec::Latest,
            dependencies: deps,
            ..Default::default()
        };
        let mut maps: BTreeMap<String, BTreeMap<String, String>> = BTreeMap::new();
        maps.insert(
            "ffmpeg".to_string(),
            BTreeMap::from([
                ("linux/ffmpeg".to_string(), "blake3:a".to_string()),
                ("macos/ffmpeg".to_string(), "blake3:b".to_string()),
            ]),
        );
        maps.insert(
            "deno".to_string(),
            BTreeMap::from([("linux/deno".to_string(), "blake3:c".to_string())]),
        );
        let result = inline_same_step_deps("yt-dlp", &req, &maps, known_dependency_type);
        assert_eq!(
            result,
            BTreeMap::from([
                ("deps/ffmpeg/linux/ffmpeg".to_string(), "blake3:a".to_string()),
                ("deps/ffmpeg/macos/ffmpeg".to_string(), "blake3:b".to_string()),
                ("deps/deno/linux/deno".to_string(), "blake3:c".to_string()),
            ]),
        );
    }

    #[test]
    fn inline_same_step_deps_cross_step_excluded() {
        // rsgain's ffmpeg dep is CrossStep → never inlined.
        let deps = BTreeMap::from([("ffmpeg".to_string(), ConfigVersionSpec::Latest)]);
        let req = ToolRequirement {
            version_spec: ConfigVersionSpec::Latest,
            dependencies: deps,
            ..Default::default()
        };
        let mut maps: BTreeMap<String, BTreeMap<String, String>> = BTreeMap::new();
        maps.insert(
            "ffmpeg".to_string(),
            BTreeMap::from([("linux/ffmpeg".to_string(), "blake3:a".to_string())]),
        );
        let result = inline_same_step_deps("rsgain", &req, &maps, known_dependency_type);
        assert!(result.is_empty());
    }

    #[test]
    fn inline_same_step_deps_dep_absent_skipped() {
        // Dep listed but not provisioned this pass (skipped/failed) → nothing.
        let deps = BTreeMap::from([("ffmpeg".to_string(), ConfigVersionSpec::Latest)]);
        let req = ToolRequirement {
            version_spec: ConfigVersionSpec::Latest,
            dependencies: deps,
            ..Default::default()
        };
        let maps = BTreeMap::new();
        let result = inline_same_step_deps("yt-dlp", &req, &maps, known_dependency_type);
        assert!(result.is_empty());
    }

    #[test]
    fn inline_same_step_deps_no_recursion() {
        // A dep's stored own map may (defensively) contain `deps/...` keys;
        // those must never be re-inlined — deps are non-transitive, so the
        // output never contains nested `deps/` paths like
        // `deps/ffmpeg/deps/x/...`.
        let deps = BTreeMap::from([("ffmpeg".to_string(), ConfigVersionSpec::Latest)]);
        let req = ToolRequirement {
            version_spec: ConfigVersionSpec::Latest,
            dependencies: deps,
            ..Default::default()
        };
        let mut maps: BTreeMap<String, BTreeMap<String, String>> = BTreeMap::new();
        maps.insert(
            "ffmpeg".to_string(),
            BTreeMap::from([
                ("linux/ffmpeg".to_string(), "blake3:a".to_string()),
                ("deps/x/linux/x".to_string(), "blake3:b".to_string()),
            ]),
        );
        let result = inline_same_step_deps("yt-dlp", &req, &maps, known_dependency_type);
        assert_eq!(
            result,
            BTreeMap::from([("deps/ffmpeg/linux/ffmpeg".to_string(), "blake3:a".to_string())]),
            "only the dep's own payload keys are inlined; deps/ keys are never re-inlined",
        );
        assert!(
            result.keys().all(|k| !k.contains("/deps/")),
            "no nested deps/ paths allowed: {result:?}",
        );
    }

    #[test]
    fn inline_same_step_deps_own_keys_untouched() {
        // Inlining returns only `deps/`-prefixed entries; the requester's own
        // keys live in the payload content map, never in the inlined set.
        let deps = BTreeMap::from([("ffmpeg".to_string(), ConfigVersionSpec::Latest)]);
        let req = ToolRequirement {
            version_spec: ConfigVersionSpec::Latest,
            dependencies: deps,
            ..Default::default()
        };
        let mut maps: BTreeMap<String, BTreeMap<String, String>> = BTreeMap::new();
        maps.insert(
            "ffmpeg".to_string(),
            BTreeMap::from([("linux/ffmpeg".to_string(), "blake3:a".to_string())]),
        );
        let result = inline_same_step_deps("yt-dlp", &req, &maps, known_dependency_type);
        assert!(result.keys().all(|k| k.starts_with("deps/")));
    }

    #[test]
    fn strip_inlined_deps_keys_removes_deps_prefix() {
        let map = BTreeMap::from([
            ("linux/yt-dlp".to_string(), "blake3:a".to_string()),
            ("deps/ffmpeg/linux/ffmpeg".to_string(), "blake3:b".to_string()),
        ]);
        assert_eq!(
            strip_inlined_deps_keys(&map),
            BTreeMap::from([("linux/yt-dlp".to_string(), "blake3:a".to_string())]),
        );
    }

    #[test]
    fn strip_inlined_deps_keys_keeps_own_keys() {
        let map = BTreeMap::from([
            ("linux/yt-dlp".to_string(), "blake3:a".to_string()),
            ("macos/yt-dlp".to_string(), "blake3:c".to_string()),
        ]);
        assert_eq!(strip_inlined_deps_keys(&map), map);
    }

    #[test]
    fn strip_inlined_deps_keys_empty_when_only_deps() {
        let map = BTreeMap::from([
            ("deps/ffmpeg/linux/ffmpeg".to_string(), "blake3:b".to_string()),
            ("deps/deno/linux/deno".to_string(), "blake3:c".to_string()),
        ]);
        assert!(strip_inlined_deps_keys(&map).is_empty());
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
            deployed_at: mediapm_utils::Timestamp::default(),
            resolved_tag: None,
            resolved_version: None,
            resolved_vcs_hash: None,
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
                deployed_at: mediapm_utils::Timestamp::default(),
                resolved_tag: None,
                resolved_version: None,
                resolved_vcs_hash: None,
            },
            ToolRegistryEntry {
                tool_id: "ffmpeg".to_string(),
                version: String::new(),
                canonical_version: "ffmpeg-v6.0".to_string(),
                content_map_hash: String::new(),
                deployed_at: mediapm_utils::Timestamp::default(),
                resolved_tag: None,
                resolved_version: None,
                resolved_vcs_hash: None,
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
            deployed_at: mediapm_utils::Timestamp::default(),
            resolved_tag: None,
            resolved_version: None,
            resolved_vcs_hash: None,
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
                deployed_at: mediapm_utils::Timestamp::default(),
                resolved_tag: None,
                resolved_version: None,
                resolved_vcs_hash: None,
            },
            ToolRegistryEntry {
                tool_id: "ffmpeg".to_string(),
                version: String::new(),
                canonical_version: "ffmpeg-v7.1".to_string(),
                content_map_hash: "blake3:def".to_string(),
                deployed_at: mediapm_utils::Timestamp::default(),
                resolved_tag: None,
                resolved_version: None,
                resolved_vcs_hash: None,
            },
        ];
        let map = index_managed_tools(&entries);
        assert_eq!(map.len(), 1);
        assert_eq!(map["ffmpeg"].len(), 2);
    }

    // Phase 3 — resolved-field skip backfill merge
    // ---------------------------------------------------------------------------

    fn backfill_entry(
        tool_id: &str,
        canonical_version: &str,
        resolved_tag: Option<&str>,
        resolved_version: Option<&str>,
        resolved_vcs_hash: Option<&str>,
    ) -> ToolRegistryEntry {
        ToolRegistryEntry {
            tool_id: tool_id.to_string(),
            version: String::new(),
            canonical_version: canonical_version.to_string(),
            content_map_hash: String::new(),
            deployed_at: mediapm_utils::Timestamp::default(),
            resolved_tag: resolved_tag.map(str::to_string),
            resolved_version: resolved_version.map(str::to_string),
            resolved_vcs_hash: resolved_vcs_hash.map(str::to_string),
        }
    }

    #[test]
    fn apply_resolved_field_backfills_fills_none_fields_in_place() {
        let mut managed = vec![backfill_entry("ffmpeg", "ffmpeg-v7.1", None, None, None)];
        let backfills =
            vec![backfill_entry("ffmpeg", "ffmpeg-v7.1", Some("autobuild-2025-07-15"), None, None)];
        apply_resolved_field_backfills(&mut managed, &backfills);
        assert_eq!(managed[0].resolved_tag.as_deref(), Some("autobuild-2025-07-15"));
        // Why-empty fields stay `None` — backfills never invent values.
        assert_eq!(managed[0].resolved_version, None);
        assert_eq!(managed[0].resolved_vcs_hash, None);
    }

    #[test]
    fn apply_resolved_field_backfills_never_overwrites_some() {
        let mut managed =
            vec![backfill_entry("yt-dlp", "yt-dlp-v2", Some("v2"), Some("2.0"), Some("abc"))];
        let backfills = vec![backfill_entry(
            "yt-dlp",
            "yt-dlp-v2",
            Some("DIFFERENT"),
            Some("9.9"),
            Some("def"),
        )];
        apply_resolved_field_backfills(&mut managed, &backfills);
        assert_eq!(managed[0].resolved_tag.as_deref(), Some("v2"));
        assert_eq!(managed[0].resolved_version.as_deref(), Some("2.0"));
        assert_eq!(managed[0].resolved_vcs_hash.as_deref(), Some("abc"));
    }

    #[test]
    fn apply_resolved_field_backfills_noop_when_unchanged() {
        let mut managed = vec![backfill_entry("yt-dlp", "yt-dlp-v2", Some("v2"), None, None)];
        let backfills = vec![backfill_entry("yt-dlp", "yt-dlp-v2", Some("v2"), None, None)];
        apply_resolved_field_backfills(&mut managed, &backfills);
        assert_eq!(managed[0].resolved_tag.as_deref(), Some("v2"));
        assert_eq!(managed[0].resolved_version, None);
    }

    #[test]
    fn apply_resolved_field_backfills_preserves_identity_fields() {
        let mut managed = vec![ToolRegistryEntry {
            tool_id: "ffmpeg".to_string(),
            version: "7.1".to_string(),
            canonical_version: "ffmpeg-v7.1".to_string(),
            content_map_hash: "blake3:abc".to_string(),
            deployed_at: mediapm_utils::Timestamp::from_unix_secs(1234),
            resolved_tag: None,
            resolved_version: None,
            resolved_vcs_hash: None,
        }];
        let backfills =
            vec![backfill_entry("ffmpeg", "ffmpeg-v7.1", Some("tag"), Some("ver"), Some("hash"))];
        apply_resolved_field_backfills(&mut managed, &backfills);
        assert_eq!(managed[0].version, "7.1");
        assert_eq!(managed[0].canonical_version, "ffmpeg-v7.1");
        assert_eq!(managed[0].content_map_hash, "blake3:abc");
        assert_eq!(managed[0].deployed_at, mediapm_utils::Timestamp::from_unix_secs(1234));
    }

    #[test]
    fn apply_resolved_field_backfills_no_matching_entry_ignored() {
        let mut managed = vec![backfill_entry("yt-dlp", "yt-dlp-v2", None, None, None)];
        let backfills =
            vec![backfill_entry("ffmpeg", "ffmpeg-v7.1", Some("tag"), Some("ver"), Some("hash"))];
        apply_resolved_field_backfills(&mut managed, &backfills);
        assert_eq!(managed[0].resolved_tag, None);
        assert_eq!(managed[0].resolved_version, None);
        assert_eq!(managed[0].resolved_vcs_hash, None);
    }

    #[test]
    fn apply_resolved_field_backfills_entry_not_in_backfills_unchanged() {
        let mut managed =
            vec![backfill_entry("sd", "sd-v1.1.0", Some("v1.1.0"), Some("1.1.0"), Some("xyz"))];
        let backfills =
            vec![backfill_entry("yt-dlp", "yt-dlp-v2", Some("v2"), Some("2.0"), Some("abc"))];
        apply_resolved_field_backfills(&mut managed, &backfills);
        assert_eq!(managed[0].resolved_tag.as_deref(), Some("v1.1.0"));
        assert_eq!(managed[0].resolved_version.as_deref(), Some("1.1.0"));
        assert_eq!(managed[0].resolved_vcs_hash.as_deref(), Some("xyz"));
    }
}
