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

use futures_util::stream::{self, StreamExt};

use mediapm_cas::{CasApi, FileSystemCas, Hash};
use mediapm_conductor::cache::Cache;
use mediapm_conductor::cache::CacheDomainConfig;
use mediapm_conductor::cache::ENTRY_TTL_SECONDS;
use mediapm_conductor::cache_user_level::default_mediapm_user_download_cache_root;
use mediapm_conductor::provision::{ProvisionCache, retain_only_tool_dirs};
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
use crate::conductor_bridge::sync::provision::{
    FetchedToolPayload, PreResolveOutcome, fetch_and_import_tool_payload,
};

use crate::conductor_bridge::tool_runtime::{build_tool_spec, resolve_ffmpeg_slot_limits};
use crate::config::ToolRequirement;
use crate::config::{MediaPmState, ToolRegistryEntry};
use crate::error::MediaPmError;
use crate::output::{ProgressBarApi, ProgressScreenApi};
use crate::paths::MediaPmPaths;
use crate::source_metadata::resolve_conductor_cas_root;
use crate::tools::downloader::ToolDownloadCache;
use crate::tools::provider;

/// Environment override for tool provisioning concurrency.
const ENV_TOOL_PROVISION_CONCURRENCY: &str = "MEDIAPM_TOOL_PROVISION_CONCURRENCY";

/// Returns the maximum number of provisioning entries resolved/fetched/processed at once.
///
/// Reads `MEDIAPM_TOOL_PROVISION_CONCURRENCY` from env. Falls back to the
/// host's available parallelism, capped at 1 minimum.
fn default_tool_provision_concurrency() -> usize {
    std::env::var(ENV_TOOL_PROVISION_CONCURRENCY)
        .ok()
        .and_then(|value| value.parse::<usize>().ok())
        .filter(|&v| v > 0)
        .unwrap_or_else(|| std::thread::available_parallelism().map_or(4, usize::from).max(1))
}

/// Outcome of provisioning a single entry (resolve + fetch + I/O).
///
/// Carries all computed data so `apply_entry_outcome` can perform state
/// mutations without any async operations.
#[derive(Debug)]
enum EntryOutcome {
    /// First-skip: exact version match against persisted state + CAS available.
    Skipped {
        /// Tool identifier.
        tool_id: String,
        /// Generated-doc key for the active tool spec.
        key: String,
        /// Runtime from the active tool spec.
        spec_runtime: ToolRuntime,
        /// Own (pre-inline) content map for dep inlining.
        own_map: BTreeMap<String, String>,
    },
    /// Resolve failed (network/cache error).
    ResolveError {
        /// Tool identifier.
        tool_id: String,
        /// The error that caused the failure.
        error: MediaPmError,
    },
    /// Resolved but skipped: composite canonical version already provisioned.
    SkippedAfterResolve {
        /// Tool identifier.
        tool_id: String,
        /// Generated-doc key for the active tool spec.
        key: String,
        /// Runtime from the active tool spec.
        spec_runtime: ToolRuntime,
        /// Own (pre-inline) content map for dep inlining.
        own_map: BTreeMap<String, String>,
        /// Backfill entry for resolved-field population.
        backfill: ToolRegistryEntry,
        /// Tool record for `managed_tools` registration.
        ///
        /// Boxed so this variant does not dominate the enum's size: both this
        /// and `backfill` are full records of the same type, so boxing either
        /// one clears `clippy::large_enum_variant`. The indirection is uniform
        /// (one allocation per successful skip) and the enum is private.
        tool_record: Box<ToolRegistryEntry>,
    },
    /// Resolved and fetched with payload.
    Fetched {
        /// Tool identifier.
        tool_id: String,
        /// Whether this is a builtin source-ingest tool.
        is_builtin_code: bool,
        /// Whether the tool already existed in the generated doc.
        already_exists: bool,
        /// The fetched payload (`content_map` NOT yet inlined with deps).
        payload: FetchedToolPayload,
    },
    /// Resolved and fetched with no payload (builtin/launcher).
    FetchedNone {
        /// Tool identifier.
        tool_id: String,
        /// Whether this is a builtin source-ingest tool.
        is_builtin_code: bool,
        /// Whether the tool already existed in the generated doc.
        already_exists: bool,
        /// Resolved canonical version from provider metadata.
        resolved_canonical_version: String,
        /// Resolved upstream tag from provider metadata.
        resolved_tag: Option<String>,
        /// Resolved upstream version from provider metadata.
        resolved_version: Option<String>,
        /// Resolved upstream VCS hash from provider metadata.
        resolved_vcs_hash: Option<String>,
    },
    /// Fetch/import failed (network/disk error).
    FetchError {
        /// Tool identifier.
        tool_id: String,
        /// The error that caused the failure.
        error: MediaPmError,
    },
}

/// Provisions a single tool entry: resolve metadata, skip checks, and fetch
/// the tool payload. Returns an [`EntryOutcome`] carrying all computed data;
/// state mutations happen in [`apply_entry_outcome`].
///
/// This function is fully async and takes only immutable references, so it
/// can be called concurrently for parallel provisioning.
#[expect(
    clippy::too_many_arguments,
    reason = "the parameters are the distinct collaborators the pass reads (persisted state, generated document, live tool state, workspace CAS, tool cache, progress screen, recheck policy); gathering them into a struct would relocate the list without narrowing any borrow"
)]
#[expect(
    clippy::too_many_lines,
    reason = "the first-skip check, the resolve, and the four outcome branches all borrow the same entry data; splitting them would re-derive the identity and version decisions and widen the borrow surface across helpers"
)]
async fn provision_entry(
    entry: &ProvisionEntry,
    state: &MediaPmState,
    generated_doc: &NickelDocument,
    live_state: &HashMap<String, Vec<ToolRegistryEntry>>,
    workspace_cas: &FileSystemCas,
    cache: &ToolDownloadCache,
    effective_group: &dyn ProgressScreenApi,
    recheck_policy: RecheckPolicy,
) -> EntryOutcome {
    let tool_id = &entry.tool_id;
    let tool_req = &entry.tool_requirement;
    let is_builtin_code = is_builtin_source_ingest_requirement(tool_id);
    let already_exists = generated_doc.tools.values().any(|s| s.name == *tool_id);

    // First skip check: exact version match against persisted state.
    if tool_req.version_spec != ConfigVersionSpec::Latest
        && tool_req.version_spec != ConfigVersionSpec::Inherit
        && let Some(entry) = state.managed_tools.iter().find(|e| e.tool_id == *tool_id)
    {
        let resolved_spec = match &tool_req.version_spec {
            ConfigVersionSpec::Exact(fields) => VersionSpec::Exact(fields.clone()),
            _ => unreachable!(),
        };
        if spec_matches_entry(
            &resolved_spec,
            entry.resolved_tag.as_deref(),
            entry.resolved_version.as_deref(),
            entry.resolved_vcs_hash.as_deref(),
        ) && let Some((key, spec)) = find_active_tool_spec(generated_doc, tool_id)
            && workspace_content_map_is_available(workspace_cas, &spec.runtime.content_map).await
        {
            return EntryOutcome::Skipped {
                tool_id: tool_id.clone(),
                key: key.clone(),
                spec_runtime: spec.runtime.clone(),
                own_map: strip_inlined_deps_keys(&spec.runtime.content_map),
            };
        }
    }

    // Resolve: get source descriptors from the provider.
    let mut resolved_canonical_version = String::new();
    let mut resolved_tag_value: Option<String> = None;
    let mut resolved_version_value: Option<String> = None;
    let mut resolved_vcs_hash_value: Option<String> = None;
    let pre_resolved = match provider::resolve_tool_fetch(
        tool_id,
        Some((cache, "tool_metadata")),
        recheck_policy,
    )
    .await
    {
        Ok((fetch, metadata)) => {
            let human_readable_version = metadata.human_readable_version.clone();
            let canonical_version = metadata.canonical_version.clone();
            resolved_canonical_version.clone_from(&canonical_version);
            resolved_tag_value.clone_from(&metadata.resolved_tag);
            resolved_version_value.clone_from(&metadata.resolved_version);
            resolved_vcs_hash_value.clone_from(&metadata.resolved_vcs_hash);

            // Version validation.
            match &tool_req.version_spec {
                ConfigVersionSpec::Exact(fields) => {
                    if let Some(hash) = &fields.vcs_hash
                        && resolved_canonical_version != *hash
                        && resolved_tag_value.as_deref() != Some(hash.as_str())
                    {
                        return EntryOutcome::ResolveError {
                            tool_id: tool_id.clone(),
                            error: MediaPmError::Workflow(format!(
                                "tool {tool_id}: requested vcs_hash {hash} but resolved canonical {resolved_canonical_version} and tag {}",
                                resolved_tag_value.as_deref().unwrap_or("(none)")
                            )),
                        };
                    }
                    if let Some(tag) = &fields.tag
                        && resolved_tag_value.as_deref() != Some(tag.as_str())
                    {
                        return EntryOutcome::ResolveError {
                            tool_id: tool_id.clone(),
                            error: MediaPmError::Workflow(format!(
                                "tool {tool_id}: requested tag {tag} but resolved {}",
                                resolved_tag_value.as_deref().unwrap_or("(none)")
                            )),
                        };
                    }
                    if let Some(ver) = &fields.version
                        && human_readable_version != *ver
                    {
                        return EntryOutcome::ResolveError {
                            tool_id: tool_id.clone(),
                            error: MediaPmError::Workflow(format!(
                                "tool {tool_id}: requested version {ver} but resolved {human_readable_version}"
                            )),
                        };
                    }
                }
                ConfigVersionSpec::Latest => {}
                ConfigVersionSpec::Inherit => {
                    return EntryOutcome::ResolveError {
                        tool_id: tool_id.clone(),
                        error: MediaPmError::Workflow(format!(
                            "tool {tool_id}: 'inherit' version_spec is only valid for dependencies, not global tool requirements"
                        )),
                    };
                }
            }

            // Compute expected composite canonical_version for skip check.
            let expected_composite = compute_composite_canonical_version(
                &canonical_version,
                tool_id,
                tool_req,
                live_state,
            );

            // Second skip check: composite match against live_state.
            let should_skip = live_state.get(tool_id.as_str()).is_some_and(|entries| {
                entries.iter().any(|e| {
                    !e.content_map_hash.is_empty() && e.canonical_version == expected_composite
                })
            });
            let content_map_available = find_active_tool_spec(generated_doc, tool_id)
                .map(|(_, spec)| spec.runtime.content_map.clone())
                .is_some_and(|content_map| !content_map.is_empty());

            if should_skip
                && content_map_available
                && match find_active_tool_spec(generated_doc, tool_id) {
                    Some((_, spec)) => {
                        workspace_content_map_is_available(workspace_cas, &spec.runtime.content_map)
                            .await
                    }
                    None => false,
                }
            {
                // Build the skip outcome.
                let human_readable_version = metadata.human_readable_version.clone();
                let key_and_spec = find_active_tool_spec(generated_doc, tool_id);
                let (key, spec_runtime, own_map) = match key_and_spec {
                    Some((key, spec)) => (
                        key,
                        spec.runtime.clone(),
                        strip_inlined_deps_keys(&spec.runtime.content_map),
                    ),
                    None => unreachable!("content_map_available checked above"),
                };
                let backfill = ToolRegistryEntry {
                    tool_id: tool_id.clone(),
                    version: human_readable_version.clone(),
                    canonical_version: expected_composite.clone(),
                    content_map_hash: String::new(),
                    deployed_at: mediapm_utils::Timestamp::default(),
                    resolved_tag: metadata.resolved_tag.clone(),
                    resolved_version: metadata.resolved_version.clone(),
                    resolved_vcs_hash: metadata.resolved_vcs_hash.clone(),
                };
                let tool_record = Box::new(ToolRegistryEntry {
                    tool_id: tool_id.clone(),
                    version: human_readable_version,
                    canonical_version: expected_composite,
                    content_map_hash: {
                        let json = serde_json::to_string(&spec_runtime.content_map)
                            .expect("content_map serializes to JSON");
                        if spec_runtime.content_map.is_empty() {
                            String::new()
                        } else {
                            format!("blake3:{}", blake3::hash(json.as_bytes()).to_hex())
                        }
                    },
                    deployed_at: mediapm_utils::Timestamp::default(),
                    resolved_tag: metadata.resolved_tag,
                    resolved_version: metadata.resolved_version,
                    resolved_vcs_hash: metadata.resolved_vcs_hash,
                });
                return EntryOutcome::SkippedAfterResolve {
                    tool_id: tool_id.clone(),
                    key: key.clone(),
                    spec_runtime,
                    own_map,
                    backfill,
                    tool_record,
                };
            }

            let mut provision_metadata = metadata;
            provision_metadata.canonical_version = expected_composite;
            PreResolveOutcome::Resolved(fetch, provision_metadata)
        }
        Err(e) => {
            return EntryOutcome::ResolveError {
                tool_id: tool_id.clone(),
                error: MediaPmError::Conductor(e),
            };
        }
    };

    // Fetch and import tool payload.
    let payload_result = if is_builtin_code {
        Ok(None)
    } else {
        fetch_and_import_tool_payload(workspace_cas, tool_id, cache, effective_group, pre_resolved)
            .await
    };

    match payload_result {
        Ok(Some(payload)) => EntryOutcome::Fetched {
            tool_id: tool_id.clone(),
            is_builtin_code,
            already_exists,
            payload,
        },
        Ok(None) => EntryOutcome::FetchedNone {
            tool_id: tool_id.clone(),
            is_builtin_code,
            already_exists,
            resolved_canonical_version,
            resolved_tag: resolved_tag_value,
            resolved_version: resolved_version_value,
            resolved_vcs_hash: resolved_vcs_hash_value,
        },
        Err(e) => EntryOutcome::FetchError { tool_id: tool_id.clone(), error: e },
    }
}

/// Applies a single entry outcome to the shared mutable state.
///
/// This is the ordered-merge half of the parallel provisioning design:
/// outcomes are produced concurrently by [`provision_entry`], then applied
/// sequentially in entry order here.
#[expect(
    clippy::too_many_arguments,
    reason = "this is the ordered-merge half of the parallel provisioning design, so it deliberately takes every shared collection the merge can touch rather than hiding the merge surface behind accessors"
)]
#[expect(
    clippy::too_many_lines,
    reason = "each outcome variant's merge is a few lines and they are written in variant order to mirror the production order; extracting them would separate each merge from the invariant that only one variant applies per entry"
)]
fn apply_entry_outcome(
    entry: &ProvisionEntry,
    outcome: EntryOutcome,
    generated_doc: &mut NickelDocument,
    tool_runtimes: &mut BTreeMap<String, ToolRuntime>,
    provisioned_own_maps: &mut BTreeMap<String, BTreeMap<String, String>>,
    report: &mut ToolSyncReport,
    live_state: &mut HashMap<String, Vec<ToolRegistryEntry>>,
    pruned_tools: &mut usize,
    inherited_env_vars: &BTreeMap<String, Vec<String>>,
    pb: &dyn ProgressBarApi,
) {
    let tool_req = &entry.tool_requirement;

    match outcome {
        EntryOutcome::Skipped { tool_id, key, spec_runtime, own_map } => {
            tool_runtimes.entry(key).or_insert(spec_runtime);
            provisioned_own_maps.insert(tool_id, own_map);
            report.tools_skipped += 1;
            pb.advance(1);
        }
        EntryOutcome::ResolveError { tool_id, error } => {
            report.warnings.push(format!(
                "tool {tool_id}: resolve failed (will retry on next sync): {error}",
            ));
            pb.advance(1);
        }
        EntryOutcome::SkippedAfterResolve {
            tool_id,
            key,
            spec_runtime,
            own_map,
            backfill,
            tool_record,
        } => {
            tool_runtimes.entry(key).or_insert(spec_runtime);
            provisioned_own_maps.insert(tool_id, own_map);
            report.resolved_field_backfills.push(backfill);
            report.tool_records.push(*tool_record);
            report.tools_skipped += 1;
            pb.advance(1);
        }
        EntryOutcome::Fetched { tool_id, is_builtin_code, already_exists, mut payload } => {
            // Record the tool's own (pre-inline) content map.
            provisioned_own_maps.insert(tool_id.clone(), payload.content_map.clone());

            // Inline direct same-step dependency payloads.
            payload.content_map.extend(inline_same_step_deps(
                &tool_id,
                tool_req,
                provisioned_own_maps,
                crate::tools::dependency::known_dependency_type,
            ));

            // Compute content-addressed hash.
            let content_map_hash: String = if payload.content_map.is_empty() {
                String::new()
            } else {
                let json = serde_json::to_string(&payload.content_map)
                    .expect("content_map serializes to JSON");
                format!("blake3:{}", blake3::hash(json.as_bytes()).to_hex())
            };

            let ffmpeg_limits =
                resolve_ffmpeg_slot_limits(tool_req.max_input_slots, tool_req.max_output_slots);
            let (mut spec, runtime) = build_tool_spec(
                &tool_id,
                payload.content_map,
                &payload.os_exec_paths,
                ffmpeg_limits,
            );
            // The provider resolved this human-readable version, so the
            // generated document can carry the claim it actually made. A
            // workflow row reads it back through the merged tool spec.
            spec.version = Some(payload.human_readable_version.clone());

            if !already_exists && !is_builtin_code {
                report.tools_added += 1;
            } else {
                report.tools_updated += 1;
            }

            let now = mediapm_utils::Timestamp::now();
            report.tool_records.push(ToolRegistryEntry {
                tool_id: tool_id.clone(),
                version: payload.human_readable_version,
                canonical_version: payload.canonical_version,
                content_map_hash: content_map_hash.clone(),
                deployed_at: now,
                resolved_tag: payload.resolved_tag,
                resolved_version: payload.resolved_version,
                resolved_vcs_hash: payload.resolved_vcs_hash,
            });

            let entry_for_live = report.tool_records.last().unwrap().clone();
            live_state.entry(tool_id.clone()).or_default().push(entry_for_live);

            let inherited = inherited_env_vars.get(tool_id.as_str()).cloned().unwrap_or_default();
            let mut full_runtime = runtime;
            full_runtime.inherited_env_vars = inherited;

            let tool_key = if content_map_hash.is_empty() {
                tool_id.clone()
            } else {
                format!("{tool_id}@{content_map_hash}")
            };

            // Prune old version keys.
            let prefix = format!("{tool_id}@");
            let old: Vec<String> = generated_doc
                .tools
                .keys()
                .filter(|k| {
                    (k.starts_with(&prefix) || k.as_str() == tool_id.as_str())
                        && k.as_str() != tool_key.as_str()
                })
                .cloned()
                .collect();
            *pruned_tools += old.len();
            for k in &old {
                if let Some(spec) = generated_doc.tools.get_mut(k) {
                    spec.runtime.content_map.clear();
                }
            }

            generated_doc.tools.insert(tool_key.clone(), spec);
            tool_runtimes.insert(tool_key, full_runtime);
        }
        EntryOutcome::FetchedNone {
            tool_id,
            is_builtin_code,
            already_exists,
            resolved_canonical_version,
            resolved_tag,
            resolved_version,
            resolved_vcs_hash,
        } => {
            provisioned_own_maps.insert(tool_id.clone(), BTreeMap::new());
            let runtime = ToolRuntime {
                impure: false,
                inherited_env_vars: inherited_env_vars.get(&tool_id).cloned().unwrap_or_default(),
                ..ToolRuntime::default()
            };
            tool_runtimes.insert(tool_id.clone(), runtime.clone());

            let now = mediapm_utils::Timestamp::now();
            let composite_for_ok_none = compute_composite_canonical_version(
                &resolved_canonical_version,
                &tool_id,
                tool_req,
                live_state,
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
                resolved_tag,
                resolved_version,
                resolved_vcs_hash,
            });

            let entry_for_live = report.tool_records.last().unwrap().clone();
            live_state.entry(tool_id.clone()).or_default().push(entry_for_live);

            if !already_exists && !is_builtin_code {
                report.tools_added += 1;
            }

            if is_builtin_code {
                if already_exists {
                    report.tools_updated += 1;
                }
            } else if generated_doc.tools.contains_key(&tool_id) {
                report.tools_updated += 1;
            } else {
                generated_doc.tools.insert(
                    tool_id.clone(),
                    mediapm_conductor::ToolSpec {
                        version: None,
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
        EntryOutcome::FetchError { tool_id, error } => {
            report.warnings.push(format!(
                "tool {tool_id}: provisioning failed (will retry on next sync): {error}",
            ));
            pb.advance(1);
        }
    }
}

/// Returns whether every hash in a string content map is present in `cas`.
async fn workspace_content_map_is_available(
    cas: &impl CasApi,
    content_map: &BTreeMap<String, String>,
) -> bool {
    for hash_str in content_map.values() {
        if let Ok(hash) = hash_str.parse::<Hash>()
            && cas.get(hash).await.is_err()
        {
            return false;
        }
    }
    true
}

/// Parses content-map hashes that are present in the workspace CAS store.
///
/// Placeholder non-hash strings (unit-test fixtures) are ignored; when every
/// entry is a valid hash, a missing blob is a hard error.
async fn parse_available_content_map_hashes(
    cas: &impl CasApi,
    content_map: &BTreeMap<String, String>,
) -> Result<BTreeMap<String, Hash>, MediaPmError> {
    let mut parsed = BTreeMap::new();
    for (key, hash_str) in content_map {
        if let Ok(hash) = hash_str.parse::<Hash>() {
            if cas.get(hash).await.is_err() {
                return Err(MediaPmError::Workflow(format!(
                    "content_map key '{key}' hash not found in workspace CAS store"
                )));
            }
            parsed.insert(key.clone(), hash);
        }
    }
    Ok(parsed)
}

/// Materializes active managed tools into `tools_dir` via the conductor provision cache.
async fn materialize_active_tool_runtimes(
    tools_dir: &Path,
    provision_cas: Arc<FileSystemCas>,
    generated_doc: &NickelDocument,
    tool_runtimes: &BTreeMap<String, ToolRuntime>,
) -> Result<(), MediaPmError> {
    let provision_cache =
        ProvisionCache::new(tools_dir.to_path_buf(), Arc::clone(&provision_cas), None);
    for conductor_tool_id in tool_runtimes.keys() {
        // `tool_runtimes` keys are conductor tool ids for managed tools, but bare
        // mediapm ids for builtins (e.g. `import` vs generated-doc `import@v1`).
        let (materialize_key, spec) = if let Some(spec) = generated_doc.tools.get(conductor_tool_id)
        {
            if spec.runtime.content_map.is_empty() {
                continue;
            }
            (conductor_tool_id.clone(), spec)
        } else if let Some((key, spec)) = find_active_tool_spec(generated_doc, conductor_tool_id) {
            (key.clone(), spec)
        } else {
            continue;
        };
        let content_map =
            parse_available_content_map_hashes(provision_cas.as_ref(), &spec.runtime.content_map)
                .await?;
        if content_map.is_empty() {
            continue;
        }
        let guard = provision_cache.materialize(&materialize_key, &content_map).await?;
        drop(guard);
    }
    Ok(())
}

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
    kind: EntryKind,
}

enum EntryKind {
    /// Entry from the user's `tools.<id>` config.
    Explicit,
    /// Auto-vivified from a dependency declaration.
    Dep,
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
                kind: EntryKind::Dep,
            });
        }
    }

    // Dep entries first (provision deps before dependents), then explicit.
    // Keyed dedup: dep entries have precedence over explicit entries
    // (dep version specs are resolved via resolve_dep_version_spec).
    let mut seen: BTreeMap<String, usize> = BTreeMap::new();
    let mut all_entries: Vec<ProvisionEntry> = dep_entries;
    all_entries.extend(explicit_entries.into_values());
    let mut deduped: Vec<ProvisionEntry> = Vec::with_capacity(all_entries.len());
    for entry in all_entries {
        // Key by (tool_id, serialized version_spec) for deterministic dedup.
        let key = format!(
            "{}:{}",
            entry.tool_id,
            serde_json::to_string(&entry.tool_requirement.version_spec).unwrap_or_default()
        );
        if let Some(&idx) = seen.get(&key) {
            // Dep entries come first in the vec; if the existing entry is
            // a dep and this is explicit, skip the explicit one.
            if matches!(entry.kind, EntryKind::Explicit)
                && matches!(deduped[idx].kind, EntryKind::Dep)
            {
                continue;
            }
            // Otherwise replace (dep replaces dep, explicit replaces explicit)
            deduped[idx] = entry;
        } else {
            seen.insert(key, deduped.len());
            deduped.push(entry);
        }
    }

    Ok(deduped)
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

/// Opens (or creates) the workspace-scoped persistent CAS store used for tool payloads.
pub(crate) async fn open_workspace_cas_store(
    paths: &MediaPmPaths,
) -> Result<Arc<FileSystemCas>, MediaPmError> {
    let workspace_cas_root = resolve_conductor_cas_root(paths);
    std::fs::create_dir_all(&workspace_cas_root).map_err(|source| MediaPmError::Io {
        operation: "create workspace CAS store directory".to_string(),
        path: workspace_cas_root.clone(),
        source,
    })?;
    let workspace_cas = FileSystemCas::open(&workspace_cas_root)
        .await
        .map_err(|source| MediaPmError::Workflow(format!("open workspace CAS store: {source}")))?;
    Ok(Arc::new(workspace_cas))
}

/// Runs the full tool-reconciliation cycle for the current workspace.
///
/// This phase owns no progress terminal and builds no screen: every bar it
/// registers belongs to `progress_group`, which the caller derives from the
/// sync's single terminal. A phase that built its own terminal would give one
/// sync two draw targets, which is the defect this contract removes.
///
/// # Arguments
///
/// - `progress_group`: the live screen every bar of this phase is added to.
/// - `overall_bar`: the caller's pinned `"syncing tools"` overall bar. When
///   supplied it becomes the phase's own progress bar and the phase sets its
///   total; when absent the phase registers a child bar on `progress_group`
///   instead (the shape the in-file tests' recording screens drive).
///
/// The caller joins the screen afterwards: this phase keeps it live for the
/// whole call, including the `[prn]` prune bar it registers after the
/// provisioning loop, so it must not be committed before the call returns.
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
    reason = "reconciliation entrypoint; all 9 parameters are distinct required inputs"
)]
pub(crate) async fn reconcile_desired_tools(
    workspace_cas: Arc<FileSystemCas>,
    paths: &MediaPmPaths,
    desired_tools: &BTreeMap<String, serde_json::Value>,
    inherited_env_vars: &BTreeMap<String, Vec<String>>,
    recheck_policy: RecheckPolicy,
    state: &MediaPmState,
    cache_root_override: Option<&Path>,
    progress_group: &dyn ProgressScreenApi,
    overall_bar: Option<Arc<dyn ProgressBarApi>>,
) -> Result<ToolSyncReport, MediaPmError> {
    let mut report = ToolSyncReport::default();

    // Validate all dependency keys before any provisioning begins.
    // Fail-fast with MPM-E001 if any tool has an unrecognized dependency key.
    for (tool_id, tool_value) in desired_tools {
        if let Ok(req) = serde_json::from_value::<ToolRequirement>(tool_value.clone()) {
            crate::tools::dependency::validate_dependency_keys(tool_id, &req.dependencies)?;
        }
        crate::tools::dependency::validate_tool_requirement_fields(tool_id, tool_value)?;
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
    let content_domain = CacheDomainConfig {
        domain: "tools".to_string(),
        index_file_name: "tools.json".to_string(),
        entry_ttl_seconds: ENTRY_TTL_SECONDS,
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

    // The pinned overall bar, when the caller has one, is this phase's own
    // progress bar: `set_total` mirrors the overall-bar contract of
    // `materializer::sync_hierarchy` and `RunWorkflowOptions`. Without one the
    // phase takes a child bar on the caller's screen, keeping the bar visible
    // to whichever screen is driving the sync.
    let pb: Arc<dyn ProgressBarApi> = match overall_bar {
        Some(bar) => {
            bar.set_total(total_tools);
            bar
        }
        None => progress_group.add_bar(total_tools, "syncing tools"),
    };

    let mut pruned_tools: usize = 0;
    // Own (pre-inline) content maps for tools processed this pass, keyed by
    // bare mediapm tool id. Requesters re-inline direct same-step deps under
    // `deps/<dep_id>/` from these maps, so they must hold each dep's OWN
    // payload keys only — never inlined `deps/` entries (non-transitive).
    let mut provisioned_own_maps: BTreeMap<String, BTreeMap<String, String>> = BTreeMap::new();
    // Split entries into two levels: level-0 (deps, no own deps) and
    // level-1 (explicit tools, may have deps). All level-0 entries must be
    // applied before level-1 entries start, because level-1 inlining reads
    // `provisioned_own_maps` populated by level-0 apply.
    let level0: Vec<(usize, &ProvisionEntry)> =
        entries.iter().enumerate().filter(|(_, e)| matches!(e.kind, EntryKind::Dep)).collect();
    let level1: Vec<(usize, &ProvisionEntry)> =
        entries.iter().enumerate().filter(|(_, e)| matches!(e.kind, EntryKind::Explicit)).collect();

    // Level 0: parallel resolve + fetch, sequential apply.
    let level0_outcomes: Vec<(usize, EntryOutcome)> = stream::iter(level0.iter().copied())
        .map(|(idx, entry)| {
            let cache_ref = &cache;
            let cas_ref = workspace_cas.as_ref();
            let state_ref = state;
            let gen_ref = &generated_doc;
            let live_ref = &live_state;
            let group_ref = progress_group;
            async move {
                let outcome = provision_entry(
                    entry,
                    state_ref,
                    gen_ref,
                    live_ref,
                    cas_ref,
                    cache_ref,
                    group_ref,
                    recheck_policy,
                )
                .await;
                (idx, outcome)
            }
        })
        .buffer_unordered(default_tool_provision_concurrency().min(level0.len()).max(1))
        .collect()
        .await;
    let mut sorted0 = level0_outcomes;
    sorted0.sort_by_key(|(idx, _)| *idx);
    for (idx, outcome) in sorted0 {
        apply_entry_outcome(
            &entries[idx],
            outcome,
            &mut generated_doc,
            &mut tool_runtimes,
            &mut provisioned_own_maps,
            &mut report,
            &mut live_state,
            &mut pruned_tools,
            inherited_env_vars,
            pb.as_ref(),
        );
    }

    // Level 1: parallel resolve + fetch, sequential apply.
    // `provisioned_own_maps` is now fully populated from level-0, so
    // level-1 entries can inline same-step dep payloads.
    let level1_outcomes: Vec<(usize, EntryOutcome)> = stream::iter(level1.iter().copied())
        .map(|(idx, entry)| {
            let cache_ref = &cache;
            let cas_ref = workspace_cas.as_ref();
            let state_ref = state;
            let gen_ref = &generated_doc;
            let live_ref = &live_state;
            let group_ref = progress_group;
            async move {
                let outcome = provision_entry(
                    entry,
                    state_ref,
                    gen_ref,
                    live_ref,
                    cas_ref,
                    cache_ref,
                    group_ref,
                    recheck_policy,
                )
                .await;
                (idx, outcome)
            }
        })
        .buffer_unordered(default_tool_provision_concurrency().min(level1.len()).max(1))
        .collect()
        .await;
    let mut sorted1 = level1_outcomes;
    sorted1.sort_by_key(|(idx, _)| *idx);
    for (idx, outcome) in sorted1 {
        apply_entry_outcome(
            &entries[idx],
            outcome,
            &mut generated_doc,
            &mut tool_runtimes,
            &mut provisioned_own_maps,
            &mut report,
            &mut live_state,
            &mut pruned_tools,
            inherited_env_vars,
            pb.as_ref(),
        );
    }

    if report.warnings.is_empty() {
        pb.finish_success();
    } else {
        pb.finish_warning();
    }
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

    // [prn] Bar: count prune candidates (document rewrite + filesystem prune).
    // Total = number of entries the `retain` closure will remove, so the bar
    // shows actual prune progress rather than a fixed step count.
    let prune_candidates = generated_doc
        .tools
        .keys()
        .filter(|key| {
            let bare = key.split('@').next().unwrap_or(key.as_str());
            if builtin_names.contains(bare) {
                return false;
            }
            if !provisioned_names.contains(bare) {
                return true;
            }
            // Will be removed by retain if content_map is empty
            generated_doc.tools[*key].runtime.content_map.is_empty()
        })
        .count();
    // The prune bar belongs to the screen that is driving the sync. It is
    // registered after the provisioning loop, so the screen must still be live
    // here — the caller joins it only once this function returns.
    let prn_bar: Option<Arc<dyn ProgressBarApi>> = if prune_candidates > 0 {
        Some(progress_group.add_bar(prune_candidates as u64, "pruning [prn]"))
    } else {
        None
    };

    let tools_before_rewrite = generated_doc.tools.len();
    generated_doc.tools.retain(|key, spec| {
        let bare = key.split('@').next().unwrap_or(key.as_str());
        if builtin_names.contains(bare) {
            return true;
        }
        if !provisioned_names.contains(bare) {
            return false;
        }
        // Drop pruned version keys whose content_map was cleared — an empty
        // map still matches workflow step `tool = "{name}"` and shadows the
        // active `{name}@{hash}` entry.
        !spec.runtime.content_map.is_empty()
    });
    let actual_pruned = tools_before_rewrite - generated_doc.tools.len();
    pruned_tools += actual_pruned;
    if let Some(ref bar) = prn_bar {
        bar.advance(actual_pruned as u64);
    }

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

    // 4b. Materialize active tool payloads into tools_dir via the provision
    // cache so `.env.generated` paths and workflow execution share one layout.
    materialize_active_tool_runtimes(
        &paths.tools_dir,
        Arc::clone(&workspace_cas),
        &generated_doc,
        &tool_runtimes,
    )
    .await?;

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

    if let Some(ref bar) = prn_bar {
        bar.advance(1);
        bar.finish_success();
    }

    report.pruned_tools = pruned_tools;

    Ok(report)
}

#[cfg(test)]
mod tests {
    use std::collections::{BTreeMap, BTreeSet};
    use std::sync::Mutex;

    use mediapm_conductor::cache_user_level::default_mediapm_user_download_cache_root;
    use mediapm_conductor::tools::provider::VersionSpecFields;
    use mediapm_conductor::{NickelDocument, ToolKindSpec, ToolSpec};
    use mediapm_utils::progress::PrefixComponents;
    use mediapm_utils::progress::recording::{ProgressOp, RecordingProgressTracker};

    use crate::config::ToolRequirement;
    use crate::output::{DimensionSource, ProgressScreen, ProgressTerminal, TestDimensionSource};
    use crate::tools::dependency::DependencyTypes;
    use crate::tools::dependency::known_dependency_type;

    use super::*;

    #[tokio::test]
    async fn reconcile_desired_tools_records_progress_ops() {
        let tmp = mediapm_utils::temp::artifact_dir().unwrap();
        let cache_root = mediapm_utils::temp::cache_dir().unwrap();
        let paths = MediaPmPaths::from_root(tmp.path());
        let tracker = RecordingProgressTracker::new();
        let state = MediaPmState::default();
        let workspace_cas =
            super::open_workspace_cas_store(&paths).await.expect("open workspace cas");
        let result = reconcile_desired_tools(
            workspace_cas,
            &paths,
            &BTreeMap::new(),
            &BTreeMap::new(),
            RecheckPolicy::default(),
            &state,
            Some(cache_root.path()),
            &tracker,
            None,
        )
        .await;

        assert!(result.is_ok(), "reconcile_desired_tools failed: {:?}", result.err());

        let ops = tracker.ops();

        // Both bars the sync creates are registered through the tracker: the
        // overall "syncing tools" bar, and the `[prn]` prune bar — which this
        // fixture must NOT create, because nothing in the empty generated doc
        // is a prune candidate (the documented zero-bar guard). Each bar is
        // asserted by label, not by count: a count would also pass if the
        // overall bar vanished and two prune bars appeared in its place.
        let add_bars: Vec<(usize, &ProgressOp)> = ops
            .iter()
            .enumerate()
            .filter(|(_, op)| matches!(op, ProgressOp::AddBar { .. }))
            .collect();
        let label_of = |op: &ProgressOp| match op {
            ProgressOp::AddBar { label, .. } => Some(label.clone()),
            _ => None,
        };
        let overall_index = add_bars
            .iter()
            .position(|(_, op)| label_of(op).as_deref() == Some("syncing tools"))
            .expect("the overall bar must be registered through the tracker");
        let ProgressOp::AddBar { total: overall_total, .. } = add_bars[overall_index].1 else {
            unreachable!("filtered to AddBar ops")
        };
        assert_eq!(*overall_total, 0, "overall bar total should be 0 (indeterminate)");
        assert_eq!(add_bars.len(), 1, "nothing to prune means no `[prn]` bar: {add_bars:?}");
        assert!(
            add_bars.iter().all(|(_, op)| label_of(op).as_deref() == Some("syncing tools")),
            "the only registered bar is the overall bar: {add_bars:?}"
        );

        // Every bar created finishes successfully.
        let finish_successes: usize =
            ops.iter().filter(|op| matches!(op, ProgressOp::FinishSuccess)).count();
        assert_eq!(
            finish_successes,
            add_bars.len(),
            "every registered bar must finish successfully, got {finish_successes} finishes for {} bars",
            add_bars.len(),
        );
    }

    // A sync that has something to prune registers the `[prn]` bar on the screen
    // that drives it, after the overall bar.
    #[tokio::test]
    async fn reconcile_desired_tools_registers_prune_bar_on_the_caller_screen() {
        let tmp = mediapm_utils::temp::artifact_dir().unwrap();
        let cache_root = mediapm_utils::temp::cache_dir().unwrap();
        let paths = MediaPmPaths::from_root(tmp.path());
        // Seed a generated doc holding one entry the rewrite must prune, so the
        // prune bar has a real total.
        let mut content_map = BTreeMap::new();
        content_map.insert("linux/user_script".to_string(), "blake3:manual".to_string());
        let tool_spec = ToolSpec {
            version: None,
            name: "user_script".to_string(),
            kind: ToolKindSpec::default(),
            runtime: ToolRuntime { content_map, ..Default::default() },
            ..Default::default()
        };
        let doc = NickelDocument {
            tools: BTreeMap::from([("user_script@somehash".to_string(), tool_spec)]),
            ..Default::default()
        };
        save_conductor_generated_document(&paths, &doc).expect("pre-save generated doc");

        let tracker = RecordingProgressTracker::new();
        let state = MediaPmState::default();
        let workspace_cas =
            super::open_workspace_cas_store(&paths).await.expect("open workspace cas");
        let result = reconcile_desired_tools(
            workspace_cas,
            &paths,
            &BTreeMap::new(),
            &BTreeMap::new(),
            RecheckPolicy::default(),
            &state,
            Some(cache_root.path()),
            &tracker,
            None,
        )
        .await;
        assert!(result.is_ok(), "reconcile_desired_tools failed: {:?}", result.err());
        let report = result.unwrap();
        assert!(
            report.pruned_tools >= 1,
            "fixture must prune the seeded manual entry, got {}",
            report.pruned_tools
        );

        let ops = tracker.ops();
        let labels: Vec<String> = ops
            .iter()
            .filter_map(|op| match op {
                ProgressOp::AddBar { label, .. } => Some(label.clone()),
                _ => None,
            })
            .collect();
        assert_eq!(
            labels,
            vec!["syncing tools".to_string(), "pruning [prn]".to_string()],
            "the sync registers its overall bar, then the prune bar, on the caller's screen"
        );
        let prune_total = ops.iter().find_map(|op| match op {
            ProgressOp::AddBar { total, label } if label == "pruning [prn]" => Some(*total),
            _ => None,
        });
        assert_eq!(
            prune_total,
            Some(1),
            "prune bar total must be the candidate count (the seeded manual entry)"
        );
    }

    // The `[prn]` prune bar must reach the display: it is created on the screen
    // that drives the sync (the caller's, when there is one) and the sync keeps
    // that screen live until the prune phase is done. Asserted on a real draw
    // target, because "nothing panicked" is exactly the failure mode this test
    // exists to catch: the pre-fix code added the bar to an already-committed
    // screen, whose finalized renderer ignored it, so the bar never rendered.
    //
    // Red in both directions: sourcing the bar from the sync's own fallback
    // screen creates no bar here at all (the caller owns this screen), and
    // joining the sync's own screen before the prune phase panics — see
    // `reconcile_desired_tools_records_progress_ops`, which runs that path.
    #[tokio::test]
    async fn prune_bar_reaches_the_display() {
        let tmp = mediapm_utils::temp::artifact_dir().unwrap();
        let cache_root = mediapm_utils::temp::cache_dir().unwrap();
        let paths = MediaPmPaths::from_root(tmp.path());
        // Seed a generated doc holding one entry the rewrite must prune: it is
        // absent from the desired set, so the prune bar gets a real total.
        let mut content_map = BTreeMap::new();
        content_map.insert("linux/user_script".to_string(), "blake3:manual".to_string());
        let tool_spec = ToolSpec {
            version: None,
            name: "user_script".to_string(),
            kind: ToolKindSpec::default(),
            runtime: ToolRuntime { content_map, ..Default::default() },
            ..Default::default()
        };
        let doc = NickelDocument {
            tools: BTreeMap::from([("user_script@somehash".to_string(), tool_spec)]),
            ..Default::default()
        };
        save_conductor_generated_document(&paths, &doc).expect("pre-save generated doc");

        // The caller-owned screen, drawing into a captured grid.
        let term = indicatif::InMemoryTerm::new(24, 80);
        let target = indicatif::ProgressDrawTarget::term_like(Box::new(term.clone()));
        let dims = Arc::new(TestDimensionSource::new((24, 80)));
        let terminal = ProgressTerminal::builder()
            .with_multi_progress(indicatif::MultiProgress::with_draw_target(target))
            .with_dim_source(dims as Arc<dyn DimensionSource>)
            .with_pre_roll_capture(Box::new(indicatif::InMemoryTerm::new(24, 80)))
            .with_ticker_enabled(false)
            .build();
        let screen = terminal.screen().build();

        let state = MediaPmState::default();
        let workspace_cas =
            super::open_workspace_cas_store(&paths).await.expect("open workspace cas");
        let result = reconcile_desired_tools(
            workspace_cas,
            &paths,
            &BTreeMap::new(),
            &BTreeMap::new(),
            RecheckPolicy::default(),
            &state,
            Some(cache_root.path()),
            &screen,
            None,
        )
        .await;
        assert!(result.is_ok(), "reconcile_desired_tools failed: {:?}", result.err());
        let report = result.unwrap();
        assert!(
            report.pruned_tools >= 1,
            "fixture must prune the seeded manual entry, got {}",
            report.pruned_tools
        );

        terminal.tick();
        let contents = term.contents();
        let prune_line = contents.lines().find(|line| line.contains("[prn]")).map_or_else(
            || panic!("the `[prn]` bar must reach the display:\n{contents}"),
            str::to_owned,
        );
        // The bar renders `position/total`, so the whole count token is matched
        // rather than a `"/1"` substring: that substring also matches a bar
        // whose total is 100 (`1/100`), which would hide a wrong candidate
        // count. The position is 2 because the sync advances the prune bar once
        // per pruned document entry and once more for the filesystem prune.
        let counts: Vec<&str> =
            prune_line.split_whitespace().filter(|token| token.contains('/')).collect();
        assert_eq!(
            counts,
            vec!["2/1"],
            "the drawn prune bar must carry the candidate count as its total: {prune_line:?}"
        );
    }

    /// A caller-owned screen with the inert behaviour of
    /// [`ProgressScreen::disabled`] that remembers what the sync asked it for.
    ///
    /// A disabled screen draws nothing and hands out no-op handles, so the bars
    /// the sync routes through it are invisible from the outside: the recorded
    /// `(label, reported total)` pairs are the only way a test can tell "the
    /// sync asked for the prune bar and got an inert handle" apart from "the
    /// sync never asked for the prune bar at all". The reported total is the
    /// one the handle the disabled screen returned carries: `0` for a no-op
    /// handle, the requested total for a live screen's handle.
    struct RecordingDisabledScreen {
        /// The inert screen the sync is handed; it does the real work.
        inner: ProgressScreen,
        /// One `(label, reported total)` pair per `add_bar` call, in order.
        calls: Mutex<Vec<(String, u64)>>,
    }

    impl RecordingDisabledScreen {
        /// Create an inert screen whose `add_bar` calls are recorded.
        fn new() -> Self {
            Self { inner: ProgressScreen::disabled(), calls: Mutex::new(Vec::new()) }
        }

        /// The total the handle for `label` reported, or `None` when the sync
        /// never asked this screen for that bar.
        fn reported_total(&self, label: &str) -> Option<u64> {
            self.calls
                .lock()
                .expect("recording lock")
                .iter()
                .find(|(recorded, _)| recorded == label)
                .map(|(_, total)| *total)
        }
    }

    impl ProgressScreenApi for RecordingDisabledScreen {
        /// Record the request, then hand back the inert handle.
        fn add_bar(&self, total: u64, label: &str) -> Arc<dyn ProgressBarApi> {
            let handle = self.inner.add_bar(total, label);
            let reported = handle.snapshot().total;
            self.calls.lock().expect("recording lock").push((label.to_string(), reported));
            Arc::new(handle)
        }

        /// Log the components under the label they render to, so a
        /// structured bar is looked up the same way a string one is.
        fn add_bar_with_prefix(
            &self,
            total: u64,
            prefix: &PrefixComponents,
        ) -> Arc<dyn ProgressBarApi> {
            self.add_bar(total, &prefix.display_label())
        }

        /// Joining the inert screen is a no-op; delegate so the sync's join
        /// path stays exercised.
        fn join(&self) {
            self.inner.join();
        }
    }

    // The `--no-progress` path hands the disabled screen down as the caller's
    // screen, so the prune bar is added to it. That must stay inert: the sync
    // still routes the prune bar through this screen and still does its work,
    // while every handle the screen hands out reports no total, so nothing is
    // allocated and nothing draws.
    //
    // Both halves are asserted per label. The `[prn]` half is what separates
    // this test from the other prune-bar tests: a prune bar sourced from the
    // sync's own fallback screen is never requested here (the caller owns the
    // screen), so `reported_total("pruning [prn]")` would be `None`, while a
    // disabled screen that began allocating would report the candidate count.
    #[tokio::test]
    async fn prune_bar_is_inert_on_a_disabled_screen() {
        let tmp = mediapm_utils::temp::artifact_dir().unwrap();
        let cache_root = mediapm_utils::temp::cache_dir().unwrap();
        let paths = MediaPmPaths::from_root(tmp.path());
        let mut content_map = BTreeMap::new();
        content_map.insert("linux/user_script".to_string(), "blake3:manual".to_string());
        let tool_spec = ToolSpec {
            version: None,
            name: "user_script".to_string(),
            kind: ToolKindSpec::default(),
            runtime: ToolRuntime { content_map, ..Default::default() },
            ..Default::default()
        };
        let doc = NickelDocument {
            tools: BTreeMap::from([("user_script@somehash".to_string(), tool_spec)]),
            ..Default::default()
        };
        save_conductor_generated_document(&paths, &doc).expect("pre-save generated doc");

        let screen = RecordingDisabledScreen::new();
        let state = MediaPmState::default();
        let workspace_cas =
            super::open_workspace_cas_store(&paths).await.expect("open workspace cas");
        let result = reconcile_desired_tools(
            workspace_cas,
            &paths,
            &BTreeMap::new(),
            &BTreeMap::new(),
            RecheckPolicy::default(),
            &state,
            Some(cache_root.path()),
            &screen,
            None,
        )
        .await;
        assert!(result.is_ok(), "a disabled screen must not fail the sync: {:?}", result.err());
        assert!(
            result.unwrap().pruned_tools >= 1,
            "the prune must still run with progress disabled"
        );
        assert_eq!(
            screen.reported_total("pruning [prn]"),
            Some(0),
            "the sync must route the prune bar through the caller's screen, and the disabled \
             screen must answer it with a no-op handle"
        );
        assert_eq!(
            screen.reported_total("syncing tools"),
            Some(0),
            "the overall bar must be inert on a disabled screen too"
        );
    }

    // Non-fatal tool-sync failures (resolve/provision errors) are recorded as
    // `report.warnings` and retried on the next sync. The child bars for those
    // failed tools must therefore finish with a warning (yellow `[W]`), never
    // with an error (red `[F]`). This guards the Phase 2 fix: a resolve failure
    // used to call `finish_error()` on the child bar, producing a misleading
    // `[F]` while the overall bar correctly showed `[W]`.
    #[tokio::test]
    async fn reconcile_desired_tools_resolve_failure_shows_warning_not_error() {
        let tmp = mediapm_utils::temp::artifact_dir().unwrap();
        let cache_root = mediapm_utils::temp::cache_dir().unwrap();
        let paths = MediaPmPaths::from_root(tmp.path());
        let tracker = RecordingProgressTracker::new();
        let state = MediaPmState::default();
        let workspace_cas =
            super::open_workspace_cas_store(&paths).await.expect("open workspace cas");

        // An unknown tool name has no provider registered for resolution, so
        // `resolve_tool_fetch` returns Err and the tool is skipped with a
        // warning rather than aborting the whole sync.
        let mut desired = BTreeMap::new();
        desired.insert(
            "nonexistent-tool".to_string(),
            serde_json::json!({ "version_spec": "latest" }),
        );

        let result = reconcile_desired_tools(
            workspace_cas,
            &paths,
            &desired,
            &BTreeMap::new(),
            RecheckPolicy::default(),
            &state,
            Some(cache_root.path()),
            &tracker,
            None,
        )
        .await;

        // The sync still succeeds overall (the failure is non-fatal).
        assert!(result.is_ok(), "reconcile_desired_tools failed: {:?}", result.err());
        let report = result.unwrap();
        assert_eq!(
            report.warnings.len(),
            1,
            "expected exactly one warning for the unresolvable tool, got {:?}",
            report.warnings
        );

        let ops = tracker.ops();
        let finish_errors: Vec<&ProgressOp> =
            ops.iter().filter(|op| matches!(op, ProgressOp::FinishError)).collect();
        assert!(
            finish_errors.is_empty(),
            "non-fatal tool-sync failure must not emit FinishError (red [F]); got {finish_errors:?}",
        );

        let finish_warnings: Vec<&ProgressOp> =
            ops.iter().filter(|op| matches!(op, ProgressOp::FinishWarning)).collect();
        assert!(
            !finish_warnings.is_empty(),
            "expected at least one FinishWarning (yellow [W]) for the failed tool, got {ops:?}",
        );
    }

    /// The caller's pinned overall bar is driven by this phase, not left idle.
    ///
    /// The tool phase keeps its pinned `"syncing tools"` overall bar instead of
    /// taking a child bar on the caller's screen, so it must adopt the handle
    /// the caller built `with_overall` and set its total to the entry count.
    /// A phase that ignored the handle would commit a permanently idle overall
    /// bar — a bar the renderer draws from state nobody ever updates.
    #[tokio::test]
    async fn reconcile_desired_tools_drives_the_callers_overall_bar() {
        let tmp = mediapm_utils::temp::artifact_dir().unwrap();
        let cache_root = mediapm_utils::temp::cache_dir().unwrap();
        let paths = MediaPmPaths::from_root(tmp.path());
        let state = MediaPmState::default();
        let workspace_cas =
            super::open_workspace_cas_store(&paths).await.expect("open workspace cas");
        // The caller's own screen and overall handle, as the service supplies
        // them: the overall bar starts at a placeholder total of 1 and must be
        // re-totalled by the phase.
        let (screen, overall) = RecordingProgressTracker::with_overall("syncing tools", 1);

        // One desired tool, unresolvable, so the entry count is known without
        // any network access (the resolve failure is a warning, not an error).
        let mut desired = BTreeMap::new();
        desired.insert(
            "nonexistent-tool".to_string(),
            serde_json::json!({ "version_spec": "latest" }),
        );

        let result = reconcile_desired_tools(
            workspace_cas,
            &paths,
            &desired,
            &BTreeMap::new(),
            RecheckPolicy::default(),
            &state,
            Some(cache_root.path()),
            &screen,
            Some(Arc::new(overall)),
        )
        .await;
        assert!(result.is_ok(), "reconcile_desired_tools failed: {:?}", result.err());

        let ops = screen.ops();
        assert!(
            ops.iter().any(|op| matches!(op, ProgressOp::SetTotal { total: 1 })),
            "the caller's `syncing tools` overall bar must be totalled to the tool count; \
             got {ops:?}"
        );
    }

    #[tokio::test]
    async fn reconcile_desired_tools_with_override_does_not_touch_real_cache() {
        let tmp = mediapm_utils::temp::artifact_dir().unwrap();
        let cache_root = mediapm_utils::temp::cache_dir().unwrap();
        let paths = MediaPmPaths::from_root(tmp.path());
        // Record real cache state before the call.
        let real_cache_mtime = default_mediapm_user_download_cache_root()
            .and_then(|p| std::fs::metadata(p.join("tools.json")).ok())
            .and_then(|m| m.modified().ok());

        let state = MediaPmState::default();
        let workspace_cas =
            super::open_workspace_cas_store(&paths).await.expect("open workspace cas");
        let result = reconcile_desired_tools(
            workspace_cas,
            &paths,
            &BTreeMap::new(),
            &BTreeMap::new(),
            RecheckPolicy::default(),
            &state,
            Some(cache_root.path()),
            &ProgressScreen::disabled(),
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
        let tmp = mediapm_utils::temp::artifact_dir().unwrap();
        let cache_root = mediapm_utils::temp::cache_dir().unwrap();
        let paths = MediaPmPaths::from_root(tmp.path());
        // Pre-populate the cache dir with an empty store/ dir so the CAS
        // opens cleanly at the override path.
        std::fs::create_dir_all(cache_root.path().join("store")).unwrap();

        let state = MediaPmState::default();
        let workspace_cas =
            super::open_workspace_cas_store(&paths).await.expect("open workspace cas");
        let result = reconcile_desired_tools(
            workspace_cas,
            &paths,
            &BTreeMap::new(),
            &BTreeMap::new(),
            RecheckPolicy::default(),
            &state,
            Some(cache_root.path()),
            &ProgressScreen::disabled(),
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
        let tmp = mediapm_utils::temp::artifact_dir().unwrap();
        let cache_root = mediapm_utils::temp::cache_dir().unwrap();
        let paths = MediaPmPaths::from_root(tmp.path());
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

        let workspace_cas =
            super::open_workspace_cas_store(&paths).await.expect("open workspace cas");
        let result = reconcile_desired_tools(
            workspace_cas,
            &paths,
            &desired_tools,
            &BTreeMap::new(),
            RecheckPolicy::default(),
            &state,
            Some(cache_root.path()),
            &ProgressScreen::disabled(),
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
        let tmp = mediapm_utils::temp::artifact_dir().unwrap();
        let cache_root = mediapm_utils::temp::cache_dir().unwrap();
        let paths = MediaPmPaths::from_root(tmp.path());
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

        let workspace_cas =
            super::open_workspace_cas_store(&paths).await.expect("open workspace cas");
        let result = reconcile_desired_tools(
            workspace_cas,
            &paths,
            &desired_tools,
            &BTreeMap::new(),
            RecheckPolicy::default(),
            &state,
            Some(cache_root.path()),
            &ProgressScreen::disabled(),
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
        let tmp = mediapm_utils::temp::artifact_dir().unwrap();
        let cache_root = mediapm_utils::temp::cache_dir().unwrap();
        let paths = MediaPmPaths::from_root(tmp.path());
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

        let workspace_cas =
            super::open_workspace_cas_store(&paths).await.expect("open workspace cas");
        let result = reconcile_desired_tools(
            workspace_cas,
            &paths,
            &desired_tools,
            &BTreeMap::new(),
            RecheckPolicy::default(),
            &state,
            Some(cache_root.path()),
            &ProgressScreen::disabled(),
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
                version: None,
                name: "yt-dlp".to_string(),
                kind: ToolKindSpec::default(),
                runtime: ToolRuntime::default(),
                ..Default::default()
            },
        );
        tools.insert(
            "yt-dlp@blake3:abc".to_string(),
            ToolSpec {
                version: None,
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
                version: None,
                name: "yt-dlp".to_string(),
                kind: ToolKindSpec::default(),
                runtime: ToolRuntime::default(),
                ..Default::default()
            },
        );
        tools.insert(
            "yt-dlp@blake3:def".to_string(),
            ToolSpec {
                version: None,
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
                version: None,
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
        let tmp = mediapm_utils::temp::artifact_dir().unwrap();
        let cache_root = mediapm_utils::temp::cache_dir().unwrap();
        let paths = MediaPmPaths::from_root(tmp.path());
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

        let workspace_cas =
            super::open_workspace_cas_store(&paths).await.expect("open workspace cas");
        let result = reconcile_desired_tools(
            workspace_cas,
            &paths,
            &desired_tools,
            &BTreeMap::new(),
            RecheckPolicy::default(),
            &state,
            Some(cache_root.path()),
            &ProgressScreen::disabled(),
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
        let tmp = mediapm_utils::temp::artifact_dir().unwrap();
        let cache_root = mediapm_utils::temp::cache_dir().unwrap();
        let paths = MediaPmPaths::from_root(tmp.path());
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

        let workspace_cas =
            super::open_workspace_cas_store(&paths).await.expect("open workspace cas");
        let result = reconcile_desired_tools(
            workspace_cas,
            &paths,
            &desired_tools,
            &BTreeMap::new(),
            RecheckPolicy::default(),
            &state,
            Some(cache_root.path()),
            &ProgressScreen::disabled(),
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

        // Reload generated doc: the old bogus key is removed once its
        // content_map was cleared — empty maps shadow active tool specs.
        let doc = load_conductor_generated_document(&paths).expect("load generated doc after sync");
        assert!(
            !doc.tools.contains_key("media-tagger@bogus_hash"),
            "old version key should be removed after sync, keys: {:?}",
            doc.tools.keys().collect::<Vec<_>>()
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

    /// Runs one `media-tagger` reconcile pass and hands back its report.
    ///
    /// media-tagger is the fixture tool for the stale-seed tests because its
    /// provider is a builtin launcher: resolution needs neither network nor a
    /// seeded download cache, yet the launcher it generates is a real payload.
    /// The entry therefore reaches the `Fetched` outcome branch, which is the
    /// branch where `already_exists` decides `tools_added` against
    /// `tools_updated`. The no-payload branch makes a different decision, so a
    /// fixture that landed there would prove nothing about the name-match.
    async fn reconcile_media_tagger_latest(
        paths: &MediaPmPaths,
        state: &MediaPmState,
        cache_root: &std::path::Path,
    ) -> ToolSyncReport {
        let workspace_cas =
            super::open_workspace_cas_store(paths).await.expect("open workspace cas");
        let mut desired_tools = BTreeMap::new();
        desired_tools.insert(
            "media-tagger".to_string(),
            serde_json::to_value(ToolRequirement {
                version_spec: ConfigVersionSpec::Latest,
                ..Default::default()
            })
            .unwrap(),
        );
        reconcile_desired_tools(
            workspace_cas,
            paths,
            &desired_tools,
            &BTreeMap::new(),
            RecheckPolicy::default(),
            state,
            Some(cache_root),
            &ProgressScreen::disabled(),
            None,
        )
        .await
        .expect("reconcile_desired_tools must succeed for an offline builtin launcher")
    }

    /// A generated-doc entry whose `name` is the bare logical tool id makes the
    /// pass an update, never an addition.
    ///
    /// Both arms run the same fixture with one difference, the seeded entry, so
    /// the assertion has nothing to read but that difference. The unseeded arm
    /// is what makes the seeded one meaningful: a pass that ignored the
    /// name-match would report `(1, 0)` in both arms, and a pass that counted
    /// nothing at all would report `(0, 0)` in both.
    #[tokio::test]
    async fn reconcile_counts_a_name_matched_seed_as_an_update_not_an_addition() {
        let seeded_tmp = mediapm_utils::temp::artifact_dir().unwrap();
        let seeded_cache = mediapm_utils::temp::cache_dir().unwrap();
        let seeded_paths = MediaPmPaths::from_root(seeded_tmp.path());
        let mut stale_content_map = BTreeMap::new();
        stale_content_map.insert("linux/media-tagger".to_string(), "blake3:stale".to_string());
        let seeded_doc = NickelDocument {
            tools: BTreeMap::from([(
                "media-tagger@blake3:stale".to_string(),
                ToolSpec {
                    name: "media-tagger".to_string(),
                    kind: ToolKindSpec::default(),
                    runtime: ToolRuntime { content_map: stale_content_map, ..Default::default() },
                    ..Default::default()
                },
            )]),
            ..Default::default()
        };
        save_conductor_generated_document(&seeded_paths, &seeded_doc)
            .expect("pre-save generated doc");

        let seeded = reconcile_media_tagger_latest(
            &seeded_paths,
            &MediaPmState::default(),
            seeded_cache.path(),
        )
        .await;

        let bare_tmp = mediapm_utils::temp::artifact_dir().unwrap();
        let bare_cache = mediapm_utils::temp::cache_dir().unwrap();
        let bare_paths = MediaPmPaths::from_root(bare_tmp.path());
        let bare =
            reconcile_media_tagger_latest(&bare_paths, &MediaPmState::default(), bare_cache.path())
                .await;

        assert_eq!(
            (seeded.tools_added, seeded.tools_updated),
            (0, 1),
            "a generated-doc spec named `media-tagger` is an update, never an addition: {seeded:?}"
        );
        assert_eq!(
            (bare.tools_added, bare.tools_updated),
            (1, 0),
            "the same pass over an empty generated doc is an addition: {bare:?}"
        );
    }

    /// The install a real upgrade walks into: a generated-doc entry under a
    /// stale `{name}@{hash}` key, a `managed_tools` record whose
    /// `canonical_version` no longer matches what the provider resolves, and
    /// the provisioned directory that stale hash owns.
    ///
    /// One pass has to carry all three, so the assertions read the whole trail
    /// in order rather than one field of it. The state record is seeded with a
    /// non-empty `content_map_hash` on purpose: that is the half of the skip
    /// check that lets a matching `canonical_version` skip, and pairing it with
    /// a canonical version the provider cannot produce is what keeps this pass
    /// on the reprovision path.
    #[tokio::test]
    async fn stale_seed_drives_name_match_reprovision_and_prune_in_one_pass() {
        let tmp = mediapm_utils::temp::artifact_dir().unwrap();
        let cache_root = mediapm_utils::temp::cache_dir().unwrap();
        let paths = MediaPmPaths::from_root(tmp.path());

        // The document half of the seed: a previous sync's payload, filed under
        // its own content hash.
        let mut stale_content_map = BTreeMap::new();
        stale_content_map.insert("linux/media-tagger".to_string(), "blake3:stale".to_string());
        let stale_doc = NickelDocument {
            tools: BTreeMap::from([(
                "media-tagger@blake3:stale".to_string(),
                ToolSpec {
                    name: "media-tagger".to_string(),
                    kind: ToolKindSpec::default(),
                    runtime: ToolRuntime { content_map: stale_content_map, ..Default::default() },
                    ..Default::default()
                },
            )]),
            ..Default::default()
        };
        save_conductor_generated_document(&paths, &stale_doc).expect("pre-save generated doc");

        // The state half of the seed.
        let mut state = MediaPmState::default();
        state.managed_tools.push(ToolRegistryEntry {
            tool_id: "media-tagger".to_string(),
            version: "old-version".to_string(),
            canonical_version: "old".to_string(),
            content_map_hash: "blake3:stale".to_string(),
            deployed_at: mediapm_utils::Timestamp::default(),
            resolved_tag: None,
            resolved_version: None,
            resolved_vcs_hash: None,
        });

        // The filesystem half of the seed. Retain-only removes a directory only
        // when it can take that directory's lock, so the stale one carries a
        // lock file the way a real provisioned entry does.
        let stale_dir = paths.tools_dir.join("media-tagger@blake3_stale");
        std::fs::create_dir_all(&stale_dir).expect("create stale tool dir");
        std::fs::write(stale_dir.join(".lock"), b"").expect("create stale lock file");

        let report = reconcile_media_tagger_latest(&paths, &state, cache_root.path()).await;

        // The name-match makes this an update, and the mismatched canonical
        // version keeps it off both skip paths.
        assert_eq!(
            report.tools_skipped, 0,
            "a canonical_version the provider cannot produce must reprovision: {report:?}"
        );
        assert_eq!(
            (report.tools_added, report.tools_updated),
            (0, 1),
            "the seeded install is an update, not an addition: {report:?}"
        );

        // The stale key loses its content map on the new entry's arrival and is
        // dropped by the rewrite; the fresh hash key is what survives.
        let after =
            load_conductor_generated_document(&paths).expect("load generated doc after sync");
        assert!(
            !after.tools.contains_key("media-tagger@blake3:stale"),
            "the stale key must be dropped once its content map is cleared, keys: {:?}",
            after.tools.keys().collect::<Vec<_>>()
        );
        let (active_key, active_spec) = find_active_tool_spec(&after, "media-tagger")
            .expect("the freshly provisioned tool must resolve as active");
        assert_ne!(
            active_key, "media-tagger@blake3:stale",
            "the active key must be the fresh content hash, got {active_key}"
        );
        assert!(
            !active_spec.runtime.content_map.is_empty(),
            "the fresh key must carry the reprovisioned payload"
        );
        assert!(
            report.pruned_tools >= 1,
            "clearing and dropping the stale key counts as a prune: {report:?}"
        );

        // The provisioned directory follows the same trail on disk.
        assert!(!stale_dir.exists(), "retain-only must remove the directory the stale hash owned");
        let provisioned: Vec<String> = std::fs::read_dir(&paths.tools_dir)
            .expect("read tools dir")
            .map(|entry| entry.expect("tools dir entry").file_name().to_string_lossy().into_owned())
            .filter(|name| name.starts_with("media-tagger@"))
            .collect();
        assert_eq!(
            provisioned.len(),
            1,
            "exactly one fresh directory is left for the tool: {provisioned:?}"
        );
    }

    /// `dependencies` is flat on the wire: each tool id is a direct key of the
    /// map holding its version spec, with nothing wrapping the specs.
    ///
    /// The shape is read out of the encoded document rather than compared as a
    /// whole string, because a whole-string compare also passes when the map
    /// grows a level of nesting that deserializes back to the same value. The
    /// key set and the per-dependency reads below are what pin the flatness.
    #[test]
    fn tool_requirement_dependencies_round_trip_flat() {
        let mut dependencies = BTreeMap::new();
        dependencies.insert(
            "ffmpeg".to_string(),
            ConfigVersionSpec::Exact(VersionSpecFields {
                tag: Some("v7.1".to_string()),
                version: None,
                vcs_hash: None,
            }),
        );
        dependencies.insert("deno".to_string(), ConfigVersionSpec::Inherit);
        dependencies.insert("sd".to_string(), ConfigVersionSpec::Latest);
        let requirement = ToolRequirement {
            version_spec: ConfigVersionSpec::Latest,
            dependencies: dependencies.clone(),
            ..Default::default()
        };

        let encoded =
            serde_json::to_value(&requirement).expect("a ToolRequirement serializes to JSON");
        let wire_dependencies = encoded
            .get("dependencies")
            .and_then(serde_json::Value::as_object)
            .expect("dependencies encodes as an object");

        assert_eq!(
            wire_dependencies.keys().cloned().collect::<BTreeSet<_>>(),
            BTreeSet::from(["deno".to_string(), "ffmpeg".to_string(), "sd".to_string()]),
            "each dependency tool id is a direct key of the map: {wire_dependencies:?}"
        );
        assert_eq!(
            wire_dependencies["deno"],
            serde_json::json!("inherit"),
            "a unit spec encodes as its bare string: {wire_dependencies:?}"
        );
        assert_eq!(
            wire_dependencies["sd"],
            serde_json::json!("latest"),
            "a unit spec encodes as its bare string: {wire_dependencies:?}"
        );
        assert_eq!(
            wire_dependencies["ffmpeg"]["tag"],
            serde_json::json!("v7.1"),
            "an exact spec encodes its fields at the top of the dependency value: {wire_dependencies:?}"
        );
        assert_eq!(
            wire_dependencies["ffmpeg"].as_object().map(serde_json::Map::len),
            Some(1),
            "an exact spec carries only the fields it set, so a wrapper level cannot hide here: {wire_dependencies:?}"
        );

        let decoded: ToolRequirement =
            serde_json::from_value(encoded.clone()).expect("the encoded requirement decodes");
        assert_eq!(
            decoded.dependencies, dependencies,
            "every dependency spec survives the round trip"
        );
        assert_eq!(
            serde_json::to_value(&decoded).expect("the decoded requirement re-encodes"),
            encoded,
            "re-encoding reproduces the same document, so a config rewritten by a sync does not drift"
        );
    }

    /// A requirement that declares no dependency reads back as an empty map.
    ///
    /// The field is `#[serde(default)]`, so the document a user writes without
    /// a `dependencies` key must decode rather than fail, and must decode to
    /// the same empty map a written-out empty map produces. The two encodings
    /// are compared so a default that ever stopped matching the explicit form
    /// would show up here.
    #[test]
    fn tool_requirement_without_dependencies_decodes_to_an_empty_map() {
        let omitted = serde_json::json!({ "version_spec": "latest" });
        let decoded: ToolRequirement = serde_json::from_value(omitted.clone())
            .expect("an omitted map falls back to the default");
        assert!(
            decoded.dependencies.is_empty(),
            "no dependencies declared means no dependencies: {:?}",
            decoded.dependencies
        );

        let explicit = serde_json::json!({ "version_spec": "latest", "dependencies": {} });
        let decoded_explicit: ToolRequirement =
            serde_json::from_value(explicit).expect("an empty map decodes");
        assert_eq!(
            decoded_explicit.dependencies, decoded.dependencies,
            "the omitted and the written-out empty map must land on the same value"
        );
    }

    #[tokio::test]
    async fn reconcile_drops_manual_entries_from_generated_doc() {
        let tmp = mediapm_utils::temp::artifact_dir().unwrap();
        let cache_root = mediapm_utils::temp::cache_dir().unwrap();
        let paths = MediaPmPaths::from_root(tmp.path());
        // Pre-populate generated doc with a manual entry whose bare tool_id
        // is NOT in the desired set (e.g., "user_script"). Under condition 3
        // (generated-doc purity) the generated document is rewritten
        // wholesale on every sync, so such entries are dropped — manual
        // tools belong in the user-owned `mediapm.conductor.ncl` instead.
        let mut content_map = BTreeMap::new();
        content_map.insert("linux/user_script".to_string(), "blake3:manual".to_string());
        let tool_spec = ToolSpec {
            version: None,
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
        let workspace_cas =
            super::open_workspace_cas_store(&paths).await.expect("open workspace cas");
        let result = reconcile_desired_tools(
            workspace_cas,
            &paths,
            &BTreeMap::new(),
            &BTreeMap::new(),
            RecheckPolicy::default(),
            &state,
            Some(cache_root.path()),
            &ProgressScreen::disabled(),
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
            version: None,
            name: "tool_a".to_string(),
            kind: ToolKindSpec::default(),
            runtime: ToolRuntime { content_map: cm1, ..Default::default() },
            ..Default::default()
        };
        let mut cm2 = BTreeMap::new();
        cm2.insert("macos/tool_b".to_string(), hash_one_hex);
        let spec_b = ToolSpec {
            version: None,
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
        assert!(matches!(entries[0].kind, EntryKind::Dep));
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
    fn build_provisioning_entries_level_split_ordering() {
        let mut desired = BTreeMap::new();
        // yt-dlp depends on ffmpeg and deno (both same-step)
        let mut deps_yt = BTreeMap::new();
        deps_yt.insert(
            "ffmpeg".to_string(),
            ConfigVersionSpec::Exact(VersionSpecFields {
                tag: Some("v7.1".to_string()),
                version: None,
                vcs_hash: None,
            }),
        );
        deps_yt.insert(
            "deno".to_string(),
            ConfigVersionSpec::Exact(VersionSpecFields {
                tag: Some("v1.46".to_string()),
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
        // rsgain depends on ffmpeg with the same spec (shared dep)
        let deps_rsgain = BTreeMap::from([(
            "ffmpeg".to_string(),
            ConfigVersionSpec::Exact(VersionSpecFields {
                tag: Some("v7.1".to_string()),
                version: None,
                vcs_hash: None,
            }),
        )]);
        desired.insert(
            "rsgain".to_string(),
            serde_json::to_value(ToolRequirement {
                version_spec: ConfigVersionSpec::Latest,
                dependencies: deps_rsgain,
                ..Default::default()
            })
            .unwrap(),
        );
        // media-tagger has no dependencies
        desired.insert(
            "media-tagger".to_string(),
            serde_json::to_value(ToolRequirement::default()).unwrap(),
        );

        let entries = build_provisioning_entries(&desired).unwrap();

        // ffmpeg deduped from two requesters (2→1), deno separate dep → total 5
        assert_eq!(entries.len(), 5, "expected 5 entries (ffmpeg deduped), got {}", entries.len());

        // All Dep entries precede all Explicit entries.
        let last_dep = entries
            .iter()
            .rposition(|e| matches!(e.kind, EntryKind::Dep))
            .expect("at least one Dep entry");
        let first_explicit = entries
            .iter()
            .position(|e| matches!(e.kind, EntryKind::Explicit))
            .expect("at least one Explicit entry");
        assert!(
            last_dep < first_explicit,
            "all Dep entries must precede all Explicit entries: last_dep={last_dep}, first_explicit={first_explicit}",
        );

        // Single ffmpeg dep, empty dependencies (non-transitive).
        let ffmpeg_deps: Vec<_> = entries.iter().filter(|e| e.tool_id == "ffmpeg").collect();
        assert_eq!(ffmpeg_deps.len(), 1, "exactly one ffmpeg dep entry");
        assert!(
            ffmpeg_deps[0].tool_requirement.dependencies.is_empty(),
            "dep entry must have empty dependencies (non-transitive)",
        );
        assert!(matches!(ffmpeg_deps[0].kind, EntryKind::Dep));

        // Three explicit entries.
        let explicit_ids: Vec<_> = entries
            .iter()
            .filter(|e| matches!(e.kind, EntryKind::Explicit))
            .map(|e| e.tool_id.as_str())
            .collect();
        assert_eq!(explicit_ids.len(), 3);
        assert!(explicit_ids.contains(&"yt-dlp"));
        assert!(explicit_ids.contains(&"rsgain"));
        assert!(explicit_ids.contains(&"media-tagger"));
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

    #[test]
    fn compute_composite_canonical_version_no_deps() {
        let req = ToolRequirement::default();
        let live_state = HashMap::new();
        let result = compute_composite_canonical_version("v1.0", "ffmpeg", &req, &live_state);
        assert_eq!(result, "v1.0");
    }

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

    /// Regression: the skip path MUST register the skipped tool in
    /// `report.tool_records` (and therefore in `state.managed_tools`), not
    /// only push a `resolved_field_backfill`. A provisioned-but-unregistered
    /// tool is illegal state — the post-sync warning check would otherwise
    /// flag it as needing sync on every pass.
    ///
    /// Uses `media-tagger`, whose provider resolves to `MEDIAPM_GIT_HASH`
    /// without network access, so the skip path fires hermetically.
    #[tokio::test]
    async fn regression_skip_path_registers_managed_tool() {
        let tmp = mediapm_utils::temp::artifact_dir().unwrap();
        let cache_root = mediapm_utils::temp::cache_dir().unwrap();
        let paths = MediaPmPaths::from_root(tmp.path());

        // Generated doc carries the tool with a non-empty content map.
        // Placeholder values pass the CAS availability check without real
        // CAS bytes.
        let mut content_map = BTreeMap::new();
        content_map.insert("linux/media-tagger".to_string(), "provisioned".to_string());
        let tool_spec = ToolSpec {
            name: "media-tagger".to_string(),
            kind: ToolKindSpec::default(),
            runtime: ToolRuntime { content_map, ..Default::default() },
            ..Default::default()
        };
        let mut tools = BTreeMap::new();
        tools.insert("media-tagger@blake3:mt1".to_string(), tool_spec);
        let doc = NickelDocument { tools, ..Default::default() };
        save_conductor_generated_document(&paths, &doc).expect("pre-save generated doc");

        // State with a matching canonical_version (media-tagger resolves to
        // the git hash without network) and non-empty content_map_hash → the
        // skip path fires.
        let mut state = MediaPmState::default();
        state.managed_tools.push(ToolRegistryEntry {
            tool_id: "media-tagger".to_string(),
            version: format!("{}+{}", env!("CARGO_PKG_VERSION"), crate::global::MEDIAPM_GIT_HASH),
            canonical_version: crate::global::MEDIAPM_GIT_HASH.to_string(),
            content_map_hash: "blake3:mt1".to_string(),
            deployed_at: mediapm_utils::Timestamp::default(),
            resolved_tag: None,
            resolved_version: None,
            resolved_vcs_hash: None,
        });

        let mut desired_tools = BTreeMap::new();
        let req = ToolRequirement::default();
        desired_tools.insert("media-tagger".to_string(), serde_json::to_value(req).unwrap());

        let workspace_cas =
            super::open_workspace_cas_store(&paths).await.expect("open workspace cas");
        let result = reconcile_desired_tools(
            workspace_cas,
            &paths,
            &desired_tools,
            &BTreeMap::new(),
            RecheckPolicy::default(),
            &state,
            Some(cache_root.path()),
            &ProgressScreen::disabled(),
            None,
        )
        .await;

        assert!(result.is_ok(), "reconcile_desired_tools failed: {:?}", result.err());
        let report = result.unwrap();
        assert_eq!(report.tools_skipped, 1, "skip path must fire for matching state");

        // The skip path MUST contribute a tool_records entry with a non-empty
        // content_map_hash — currently it does NOT, which is the bug.
        let registered = report
            .tool_records
            .iter()
            .find(|e| e.tool_id == "media-tagger")
            .expect("skip path must register the tool in tool_records");
        assert!(
            !registered.content_map_hash.is_empty(),
            "skip-path registration must carry a non-empty content_map_hash",
        );
    }

    /// Seeds a user-level download cache with yt-dlp metadata (tag+hash)
    /// and three plain-binary payloads (windows/macos/linux).
    ///
    /// Returns the cache root path. The `Cache` handle is dropped before
    /// return, releasing the directory lock; the caller keeps the
    /// underlying `TempDir` alive so the data persists.
    ///
    /// media-tagger is an offline builtin launcher (`GenerateLauncher`);
    /// its `resolve_tool_fetch` arm returns `sources()` directly with
    /// `metadata_fetch_count: 0`, so no metadata seeding is required.
    async fn seed_two_tool_cache(cache_root: &std::path::Path) {
        let cache = Cache::open(
            cache_root,
            &[
                CacheDomainConfig {
                    domain: "tools".to_string(),
                    index_file_name: "tools.json".to_string(),
                    entry_ttl_seconds: ENTRY_TTL_SECONDS,
                },
                CacheDomainConfig {
                    domain: "tool_metadata".to_string(),
                    index_file_name: "tool_metadata.json".to_string(),
                    entry_ttl_seconds: 24 * 60 * 60,
                },
            ],
        )
        .await
        .expect("test cache opens");

        // Metadata: yt-dlp tag resolution served from cache (no GitHub API).
        let tag = "2025.07.15";
        let hash = "a1b2c3d4e5f6a7b8c9d0e1f2a3b4c5d6e7f8a9b0";
        let api_key = "https://api.github.com/repos/yt-dlp/yt-dlp/releases/latest";
        cache.store_bytes("tool_metadata", api_key, format!("{tag}\n{hash}").as_bytes()).await;

        // Payloads: three plain binaries at the REWRITTEN release URLs.
        // `fetch_tool_sources` consults the cache by final URL, so seeding
        // these exact keys makes the run network-free.
        for (filename, payload) in &[
            ("yt-dlp.exe", &b"fake yt-dlp windows binary"[..]),
            ("yt-dlp_macos", &b"fake yt-dlp macos binary"[..]),
            ("yt-dlp_linux", &b"fake yt-dlp linux binary"[..]),
        ] {
            let url =
                format!("https://github.com/yt-dlp/yt-dlp/releases/download/{tag}/{filename}");
            cache.store_bytes("tools", &url, payload).await;
        }
        // Cache handle dropped here — directory lock released, data persists
        // on disk under the TempDir that the caller holds.
    }

    /// Verifies that the parallel provisioning driver creates the correct
    /// per-tool bars regardless of completion order. The assertion is a
    /// **sorted multiset** of `(tool_id, phase)` pairs extracted from every
    /// `AddBar` label — order-free by construction.
    ///
    /// Two tools: yt-dlp (seeded metadata + payloads) and media-tagger
    /// (offline `GenerateLauncher`). Both take the `Resolved` path (empty
    /// `MediaPmState` defeats both skip checks), so each produces exactly
    /// 3 bars: `[res]`, `[fch]`, `[pro]`. The overall bar adds 1.
    #[tokio::test]
    async fn sync_multi_tool_per_tool_bars_are_order_independent() {
        let tmp = mediapm_utils::temp::artifact_dir().unwrap();
        let cache_root_tmp = mediapm_utils::temp::cache_dir().unwrap();
        let paths = MediaPmPaths::from_root(tmp.path());
        let tracker = RecordingProgressTracker::new();
        let state = MediaPmState::default();

        // Seed the cache with yt-dlp metadata + payloads.
        seed_two_tool_cache(cache_root_tmp.path()).await;
        // Cache handle dropped inside the fixture — directory lock released.

        let workspace_cas =
            super::open_workspace_cas_store(&paths).await.expect("open workspace cas");

        // desired_tools: yt-dlp (needs cache seed) + media-tagger (offline).
        // Both use Latest so neither skip-check fires (empty state).
        let mut desired_tools = BTreeMap::new();
        desired_tools.insert(
            "yt-dlp".to_string(),
            serde_json::to_value(ToolRequirement {
                version_spec: ConfigVersionSpec::Latest,
                ..Default::default()
            })
            .unwrap(),
        );
        desired_tools.insert(
            "media-tagger".to_string(),
            serde_json::to_value(ToolRequirement {
                version_spec: ConfigVersionSpec::Latest,
                ..Default::default()
            })
            .unwrap(),
        );

        let result = reconcile_desired_tools(
            workspace_cas,
            &paths,
            &desired_tools,
            &BTreeMap::new(),
            RecheckPolicy::default(),
            &state,
            Some(cache_root_tmp.path()),
            &tracker,
            None,
        )
        .await;

        assert!(result.is_ok(), "reconcile_desired_tools failed: {:?}", result.err());

        let ops = tracker.ops();

        // --- overall bar ---
        let add_bars: Vec<&ProgressOp> =
            ops.iter().filter(|op| matches!(op, ProgressOp::AddBar { .. })).collect();
        assert_eq!(
            add_bars.len(),
            1 + 3 * 2,
            "expected 1 overall + 3 bars per tool (res/fch/pro) × 2 tools = 7, got {}",
            add_bars.len()
        );

        // --- extract (tool_id, phase) from each AddBar label ---
        let mut observed: Vec<(String, String)> = Vec::new();
        for op in &add_bars {
            if let ProgressOp::AddBar { label, .. } = op {
                // Labels: "syncing tools", "yt-dlp <ver> [res]", etc.
                // Phase is the last token in brackets.
                let phase = label
                    .rsplit_once('[')
                    .and_then(|(_, rest)| rest.strip_suffix(']'))
                    .unwrap_or("overall")
                    .to_string();
                // Tool id: everything before the phase tag (or "tools" for overall).
                let tool_id = if phase == "overall" {
                    "tools".to_string()
                } else {
                    label.split_once(' ').map(|(id, _)| id.to_string()).unwrap_or_default()
                };
                observed.push((tool_id, phase));
            }
        }
        observed.sort();

        let mut expected: Vec<(String, String)> = vec![
            ("tools".to_string(), "overall".to_string()),
            ("yt-dlp".to_string(), "res".to_string()),
            ("yt-dlp".to_string(), "fch".to_string()),
            ("yt-dlp".to_string(), "pro".to_string()),
            ("media-tagger".to_string(), "res".to_string()),
            ("media-tagger".to_string(), "fch".to_string()),
            ("media-tagger".to_string(), "pro".to_string()),
        ];
        expected.sort();

        assert_eq!(
            observed, expected,
            "sorted (tool_id, phase) multiset mismatch — order-independence violated"
        );

        // --- bar totals: [fch] = 3, [pro] = 3 for both tools ---
        for op in &add_bars {
            if let ProgressOp::AddBar { label, total } = op
                && (label.ends_with("[fch]") || label.ends_with("[pro]"))
            {
                assert_eq!(
                    *total, 3,
                    "{label}: expected total 3 for plain-binary/launcher sources, got {total}"
                );
            }
        }
    }

    /// Every per-tool phase bar carries the tool id and the phase tag, with the
    /// version between them when the provider reported one.
    ///
    /// The three fields reach the bar as `PrefixComponents` rather than as one
    /// rendered string, so what the row reads is the renderer's joining of them:
    /// the tool id first, then the version, then the bracketed phase. The
    /// version is legitimately absent for a tool whose provider reports none,
    /// which is why this asserts the two segments on either side of it rather
    /// than a fixed three-part label.
    ///
    /// The fixture is the same two tools the order-independence test seeds:
    /// yt-dlp resolves a tag out of the seeded metadata, and media-tagger
    /// generates its launcher offline. Both take the `Resolved` path, so each
    /// produces a `[res]`, a `[fch]` and a `[pro]` bar.
    #[tokio::test]
    async fn per_tool_phase_bars_carry_the_version_between_the_tool_and_the_phase() {
        let tmp = mediapm_utils::temp::artifact_dir().unwrap();
        let cache_root_tmp = mediapm_utils::temp::cache_dir().unwrap();
        let paths = MediaPmPaths::from_root(tmp.path());
        let tracker = RecordingProgressTracker::new();
        let state = MediaPmState::default();

        seed_two_tool_cache(cache_root_tmp.path()).await;

        let workspace_cas =
            super::open_workspace_cas_store(&paths).await.expect("open workspace cas");

        let mut desired_tools = BTreeMap::new();
        for tool in ["yt-dlp", "media-tagger"] {
            desired_tools.insert(
                tool.to_string(),
                serde_json::to_value(ToolRequirement {
                    version_spec: ConfigVersionSpec::Latest,
                    ..Default::default()
                })
                .unwrap(),
            );
        }

        let result = reconcile_desired_tools(
            workspace_cas,
            &paths,
            &desired_tools,
            &BTreeMap::new(),
            RecheckPolicy::default(),
            &state,
            Some(cache_root_tmp.path()),
            &tracker,
            None,
        )
        .await;
        assert!(result.is_ok(), "reconcile_desired_tools failed: {:?}", result.err());

        let ops = tracker.ops();
        let phase_labels: Vec<&str> = ops
            .iter()
            .filter_map(|op| match op {
                ProgressOp::AddBar { label, .. }
                    if label.ends_with(']')
                        && !label.starts_with("syncing")
                        && !label.starts_with("pruning") =>
                {
                    Some(label.as_str())
                }
                _ => None,
            })
            .collect();
        assert_eq!(phase_labels.len(), 6, "two tools at three phases each: {phase_labels:?}");

        // yt-dlp's provider resolves a tag from the seeded metadata, so its
        // rows must carry it between the tool id and the phase tag. The tag is
        // `2025.07.15`; pinning it here is what makes a version that went
        // missing, or moved behind the phase tag, a failure rather than a
        // shorter label nobody reads.
        // Each bar reads tool id, then the version the provider reported, then the phase
        // tag. Pinning the exact version strings would fail whenever a resolved `Latest`
        // moved to a new content hash, which is not what this row is about: the row asks
        // that the segment be present at all. So the shape is checked, and the segment is
        // checked for being non-empty, which is the part the order-independent test
        // discarded.
        let mut seen: BTreeMap<&str, BTreeSet<&str>> = BTreeMap::new();
        for label in &phase_labels {
            let (tool, rest) = label.split_once(' ').unwrap_or_else(|| {
                panic!("a phase bar reads tool id then version then tag: {label}")
            });
            let (version, tag) = rest
                .split_once(' ')
                .unwrap_or_else(|| panic!("a phase bar carries a version segment: {label}"));
            assert!(
                !version.is_empty(),
                "the version segment between the tool id and the phase must not be empty: {label}"
            );
            let tag = tag
                .strip_prefix('[')
                .and_then(|t| t.strip_suffix(']'))
                .unwrap_or_else(|| panic!("the phase tag is bracketed: {label}"));
            assert!(
                matches!(tag, "res" | "fch" | "pro"),
                "the tail is a phase tag, not a version: {label}"
            );
            seen.entry(tool).or_default().insert(tag);
        }

        assert_eq!(seen.len(), 2, "both tools drew their own phase bars: {seen:?}");
        for (tool, phases) in &seen {
            assert_eq!(
                phases,
                &["fch", "pro", "res"].into_iter().collect(),
                "{tool} drew one bar per phase: {phases:?}"
            );
        }
    }

    /// Parallel source fetch must be deterministic: running
    /// `reconcile_desired_tools` twice with the same seeded cache must
    /// produce identical generated-document bytes and bar-operation multisets.
    #[tokio::test]
    #[expect(
        clippy::too_many_lines,
        reason = "the determinism contract is the comparison itself: both runs, the document-byte equality, and the bar-multiset equality must stay visible together so a future edit cannot quietly compare only one of the two artifacts"
    )]
    async fn sync_parallel_fetch_is_deterministic() {
        let tmp1 = mediapm_utils::temp::artifact_dir().unwrap();
        let tmp2 = mediapm_utils::temp::artifact_dir().unwrap();
        let cache1 = mediapm_utils::temp::cache_dir().unwrap();
        let cache2 = mediapm_utils::temp::cache_dir().unwrap();

        let paths1 = MediaPmPaths::from_root(tmp1.path());
        let paths2 = MediaPmPaths::from_root(tmp2.path());

        // Seed both caches identically.
        seed_two_tool_cache(cache1.path()).await;
        seed_two_tool_cache(cache2.path()).await;

        let mut desired_tools = BTreeMap::new();
        desired_tools.insert(
            "yt-dlp".to_string(),
            serde_json::to_value(ToolRequirement {
                version_spec: ConfigVersionSpec::Latest,
                ..Default::default()
            })
            .unwrap(),
        );
        desired_tools.insert(
            "media-tagger".to_string(),
            serde_json::to_value(ToolRequirement {
                version_spec: ConfigVersionSpec::Latest,
                ..Default::default()
            })
            .unwrap(),
        );

        let state = MediaPmState::default();

        // --- Run 1 ---
        let cas1 = super::open_workspace_cas_store(&paths1).await.expect("open cas 1");
        let tracker1 = RecordingProgressTracker::new();
        reconcile_desired_tools(
            cas1,
            &paths1,
            &desired_tools,
            &BTreeMap::new(),
            RecheckPolicy::default(),
            &state,
            Some(cache1.path()),
            &tracker1,
            None,
        )
        .await
        .expect("run 1 failed");

        // --- Run 2 ---
        let cas2 = super::open_workspace_cas_store(&paths2).await.expect("open cas 2");
        let tracker2 = RecordingProgressTracker::new();
        reconcile_desired_tools(
            cas2,
            &paths2,
            &desired_tools,
            &BTreeMap::new(),
            RecheckPolicy::default(),
            &state,
            Some(cache2.path()),
            &tracker2,
            None,
        )
        .await
        .expect("run 2 failed");

        // --- Assert identical generated-doc bytes ---
        let doc1 = std::fs::read(&paths1.conductor_generated_ncl).expect("read generated doc 1");
        let doc2 = std::fs::read(&paths2.conductor_generated_ncl).expect("read generated doc 2");
        assert_eq!(doc1, doc2, "generated-doc bytes differ between runs");

        // --- Assert identical bar-operation multisets ---
        let mut bars1: Vec<(String, String)> = tracker1
            .ops()
            .iter()
            .filter_map(|op| {
                if let ProgressOp::AddBar { label, .. } = op {
                    let phase = label
                        .rsplit_once('[')
                        .and_then(|(_, rest)| rest.strip_suffix(']'))
                        .unwrap_or("overall")
                        .to_string();
                    let tool_id = if phase == "overall" {
                        "tools".to_string()
                    } else {
                        label.split_once(' ').map(|(id, _)| id.to_string()).unwrap_or_default()
                    };
                    Some((tool_id, phase))
                } else {
                    None
                }
            })
            .collect();
        let mut bars2: Vec<(String, String)> = tracker2
            .ops()
            .iter()
            .filter_map(|op| {
                if let ProgressOp::AddBar { label, .. } = op {
                    let phase = label
                        .rsplit_once('[')
                        .and_then(|(_, rest)| rest.strip_suffix(']'))
                        .unwrap_or("overall")
                        .to_string();
                    let tool_id = if phase == "overall" {
                        "tools".to_string()
                    } else {
                        label.split_once(' ').map(|(id, _)| id.to_string()).unwrap_or_default()
                    };
                    Some((tool_id, phase))
                } else {
                    None
                }
            })
            .collect();
        bars1.sort();
        bars2.sort();
        assert_eq!(bars1, bars2, "bar-operation multisets differ between runs");
    }

    #[test]
    fn tool_provision_concurrency_default_returns_positive() {
        let concurrency = default_tool_provision_concurrency();
        assert!(concurrency >= 1, "concurrency must be >= 1, got {concurrency}");
    }

    #[test]
    fn tool_provision_concurrency_env_override() {
        unsafe {
            std::env::set_var(ENV_TOOL_PROVISION_CONCURRENCY, "2");
        }
        let concurrency = default_tool_provision_concurrency();
        assert_eq!(concurrency, 2, "env override should return 2");
        unsafe {
            std::env::remove_var(ENV_TOOL_PROVISION_CONCURRENCY);
        }
    }

    #[test]
    fn tool_provision_concurrency_env_invalid_falls_back() {
        unsafe {
            std::env::set_var(ENV_TOOL_PROVISION_CONCURRENCY, "xyz");
        }
        let concurrency = default_tool_provision_concurrency();
        assert!(concurrency >= 1, "invalid env should fall back to >= 1, got {concurrency}");
        unsafe {
            std::env::remove_var(ENV_TOOL_PROVISION_CONCURRENCY);
        }
    }

    /// The arm that resolved a tool without fetching a payload inserts a
    /// spec whose version is `None`, even though the provider resolved a tag,
    /// a version, and a canonical version for it.
    ///
    /// Those three values are real, but they describe what the provider would
    /// have fetched, and nothing was fetched. A row that printed one of them
    /// would name a release the workspace has no payload for. The registry
    /// entry this arm also pushes does carry the composite, which is the audit
    /// record; the document spec is what a step row reads, so it stays empty.
    #[test]
    fn fetched_none_registers_a_spec_without_a_version() {
        let screen = ProgressScreen::disabled();
        let bar = screen.add_bar(0, "fetched-none");
        let entry = ProvisionEntry {
            tool_id: "media-tagger".to_string(),
            tool_requirement: ToolRequirement::default(),
            kind: EntryKind::Explicit,
        };
        let mut generated_doc = NickelDocument::default();
        let mut tool_runtimes = BTreeMap::new();
        let mut provisioned_own_maps = BTreeMap::new();
        let mut report = ToolSyncReport::default();
        let mut live_state = std::collections::HashMap::new();
        let mut pruned_tools = 0usize;
        let inherited_env_vars = BTreeMap::new();

        apply_entry_outcome(
            &entry,
            EntryOutcome::FetchedNone {
                tool_id: "media-tagger".to_string(),
                is_builtin_code: false,
                already_exists: false,
                resolved_canonical_version: "mediapm-0123456789ab".to_string(),
                resolved_tag: Some("v2024.01.01".to_string()),
                resolved_version: Some("2024.01.01".to_string()),
                resolved_vcs_hash: Some("0123456789abcdef".to_string()),
            },
            &mut generated_doc,
            &mut tool_runtimes,
            &mut provisioned_own_maps,
            &mut report,
            &mut live_state,
            &mut pruned_tools,
            &inherited_env_vars,
            &bar,
        );

        let spec = generated_doc
            .tools
            .get("media-tagger")
            .expect("the no-payload arm registers the tool under its bare id");
        assert_eq!(
            spec.version, None,
            "no payload was fetched, so nothing claimed a release for this tool"
        );

        assert!(
            !generated_doc.tools.keys().any(|key| key.contains('@')),
            "an empty content map yields the bare tool id as the document key: {keys:?}",
            keys = generated_doc.tools.keys().collect::<Vec<_>>()
        );

        let record = report
            .tool_records
            .first()
            .expect("the no-payload arm records the tool in the registry");
        assert_eq!(
            record.version,
            format!("{}+{}", env!("CARGO_PKG_VERSION"), crate::global::MEDIAPM_GIT_HASH),
            "the registry entry names the workspace build that will run the launcher, \
             which is a different claim from a release the provider resolved"
        );
    }

    /// Every builtin the coordinator registers carries no version, and its
    /// document key is the bare registration id.
    ///
    /// This is the tripwire for the convenience someone will eventually
    /// propose. A builtin has no release, and both things a derivation could
    /// reach for say something other than a release: `builtin_id` names a
    /// registration and the key is that same id, so neither is a claim a user
    /// wrote. Either derivation lands here as a `Some`, and a content-hash
    /// suffix would show up as an `@` in the key.
    ///
    /// The loop is over `ALL_BUILTINS` rather than one named builtin, so a
    /// builtin added later is covered without editing this test.
    #[test]
    fn registered_builtins_carry_no_version_derived_from_id_or_key() {
        let mut generated_doc = NickelDocument::default();
        register_missing_builtin_tools(&mut generated_doc);

        assert!(
            !mediapm_conductor::tools::ALL_BUILTINS.is_empty(),
            "no builtins are registered, so this test would pass over an empty document"
        );

        for builtin in mediapm_conductor::tools::ALL_BUILTINS {
            let spec = generated_doc
                .tools
                .get(builtin.builtin_id)
                .unwrap_or_else(|| panic!("{} was not registered", builtin.builtin_id));
            assert_eq!(
                spec.version, None,
                "{} has no release to name, so its row must read {}",
                builtin.builtin_id, spec.name
            );
        }

        // Every key the registration wrote is a registration id and nothing
        // else. A key of the shape `{name}@{content-hash}` would name a
        // payload, which is exactly the string a derivation would have
        // reached for.
        let registered: Vec<&str> =
            mediapm_conductor::tools::ALL_BUILTINS.iter().map(|b| b.builtin_id).collect();
        for key in generated_doc.tools.keys() {
            assert!(
                registered.contains(&key.as_str()),
                "{key} is not a builtin id, so registration wrote something the builtin ids do not cover"
            );
            let spec = &generated_doc.tools[key];
            assert!(
                matches!(&spec.kind, ToolKindSpec::Builtin { builtin_id } if builtin_id == key),
                "{key} is registered under a key that is not its builtin id"
            );
        }
    }
}
