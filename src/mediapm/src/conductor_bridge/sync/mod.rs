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
mod pool;
pub(crate) mod provision;

use std::collections::{BTreeMap, HashMap, HashSet};
use std::fmt::Write;
use std::path::Path;
use std::sync::{Arc, Mutex};

use mediapm_cas::{CasApi, FileSystemCas, Hash};
use mediapm_conductor::cache::Cache;
use mediapm_conductor::cache::CacheDomainConfig;
use mediapm_conductor::cache::ENTRY_TTL_SECONDS;
use mediapm_conductor::cache_user_level::default_mediapm_user_download_cache_root;
use mediapm_conductor::config::ExternalDataEntry;
use mediapm_conductor::provision::{ProvisionCache, ProvisionedTool, retain_only_tool_dirs};
use mediapm_conductor::runtime_env::{provisioned_payload_dir, write_generated_dotenv};
use mediapm_conductor::state::OutputSaveMode;
use mediapm_conductor::tools::provider::{ConfigVersionSpec, VersionSpec};
use mediapm_conductor::tools::spec::spec_matches_entry;
use mediapm_conductor::{NickelDocument, PayloadStaging, ToolRuntime, ToolSpec};

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

/// The provisioning phase in which a managed-tool failure occurred.
///
/// Rendered lowercase (`resolve`/`fetch`/`process`) in the fail-fast sync
/// error so the message names the exact pipeline stage that failed.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) enum ProvisionPhase {
    /// Metadata/source resolution (`resolve_tool_fetch`).
    Resolve,
    /// Payload download (`fetch_tool_sources`).
    Fetch,
    /// Archive extraction / CAS import (`process_tool_sources`).
    Process,
}

impl std::fmt::Display for ProvisionPhase {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.write_str(match self {
            Self::Resolve => "resolve",
            Self::Fetch => "fetch",
            Self::Process => "process",
        })
    }
}

/// A managed-tool provisioning failure: the phase it failed in and the
/// underlying cause. [`provision_entry`] returns this instead of an
/// [`EntryOutcome`] so the coordinator can abort the whole sync (fail fast)
/// with the real cause rather than recording a misleading warning. The failing
/// tool is identified by the pool job's index (the coordinator maps it back to
/// its [`ProvisionEntry`]), so the failure itself need not duplicate it.
#[derive(Debug)]
pub(crate) struct ProvisionFailure {
    /// The phase the failure occurred in.
    pub(crate) phase: ProvisionPhase,
    /// The underlying cause.
    pub(crate) source: MediaPmError,
}

/// Outcome of provisioning a single entry (resolve + fetch + I/O).
///
/// Carries all computed data so `apply_entry_outcome` can perform state
/// mutations without any async operations.
#[derive(Debug)]
enum EntryOutcome {
    /// First-skip: exact version match against persisted state + CAS available.
    Skipped {
        /// Generated-doc key for the active tool spec.
        key: String,
        /// Runtime from the active tool spec.
        spec_runtime: ToolRuntime,
    },
    /// Resolved but skipped: composite canonical version already provisioned.
    SkippedAfterResolve {
        /// Generated-doc key for the active tool spec.
        key: String,
        /// Runtime from the active tool spec.
        spec_runtime: ToolRuntime,
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
        /// The fetched payload (`content_map` already inlined with direct
        /// same-step dep payloads by `provision_entry`).
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
        /// Composite canonical version (bare + same-step dep segments),
        /// computed in `provision_entry` from the gated dep versions so the
        /// apply pass never re-derives it from live state.
        canonical_version: String,
        /// Resolved upstream tag from provider metadata.
        resolved_tag: Option<String>,
        /// Resolved upstream version from provider metadata.
        resolved_version: Option<String>,
        /// Resolved upstream VCS hash from provider metadata.
        resolved_vcs_hash: Option<String>,
    },
}

/// Provisions a single tool entry: resolve metadata, skip checks, and fetch
/// the tool payload. Returns an [`EntryOutcome`] carrying all computed data;
/// state mutations happen in [`apply_entry_outcome`].
///
/// Returns [`ProvisionFailure`] instead when the tool cannot be provisioned.
/// The dependency gates/publisher wire this entry's same-step edges into the
/// provisioning pool: before the composite/skip check the entry awaits each
/// same-step dep's resolved version via `gates.dep_version`, publishes its own
/// bare version right after resolve, and before inlining awaits each dep's
/// pre-inline payload map via `gates.dep_own_map`. Because it awaits pool
/// edges, it is polled inline by the pool (never spawned) and may borrow
/// stack-local collaborators for the duration of the call.
///
/// This function is fully async and takes only immutable references (plus the
/// per-edge gates/publisher), so it can run concurrently for parallel
/// provisioning.
#[expect(
    clippy::too_many_arguments,
    reason = "the parameters are the distinct collaborators the pass reads (persisted state, generated document, live tool state, workspace CAS, tool cache, progress screen, recheck policy, per-edge gates/publisher and dep-index map); gathering them into a struct would relocate the list without narrowing any borrow"
)]
#[expect(
    clippy::too_many_lines,
    reason = "the first-skip check, the resolve, and the outcome branches all borrow the same entry data; splitting them would re-derive the identity and version decisions and widen the borrow surface across helpers"
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
    gates: &pool::DepGates,
    publisher: &mut pool::DepPublisher,
    dep_indices: &BTreeMap<String, usize>,
) -> Result<EntryOutcome, ProvisionFailure> {
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
            let own_map = strip_inlined_deps_keys(&spec.runtime.content_map);
            // A first-skipped tool still resolves identity for its
            // dependents: publish its persisted bare version and its
            // reconstructed (pre-inline) own map so a same-step dependent's
            // composite and inlining can proceed without re-provisioning it.
            publisher.publish_version(own_version_segment(&entry.canonical_version).to_string());
            publisher.publish_own_map(own_map);
            return Ok(EntryOutcome::Skipped {
                key: key.clone(),
                spec_runtime: spec.runtime.clone(),
            });
        }
    }

    // Resolve: get source descriptors from the provider. The tool's version
    // target drives resolution: a tag-pinned GitHub tool fetches that
    // release's assets; a bare vcs_hash/version pin resolves latest and the
    // validation below still compares.
    let version_target = VersionSpec::from(&tool_req.version_spec);
    let mut resolved_canonical_version = String::new();
    // Composite canonical version (bare + gated same-step dep segments), kept
    // out of the resolve arm so the no-payload outcome can carry it.
    let mut resolved_composite = String::new();
    let mut resolved_tag_value: Option<String> = None;
    let mut resolved_version_value: Option<String> = None;
    let mut resolved_vcs_hash_value: Option<String> = None;
    let pre_resolved = match provider::resolve_tool_fetch(
        tool_id,
        &version_target,
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
                        return Err(ProvisionFailure {
                            phase: ProvisionPhase::Resolve,
                            source: MediaPmError::Workflow(format!(
                                "requested vcs_hash {hash} but resolved canonical {resolved_canonical_version} and tag {}",
                                resolved_tag_value.as_deref().unwrap_or("(none)")
                            )),
                        });
                    }
                    if let Some(tag) = &fields.tag
                        && resolved_tag_value.as_deref() != Some(tag.as_str())
                    {
                        return Err(ProvisionFailure {
                            phase: ProvisionPhase::Resolve,
                            source: MediaPmError::Workflow(format!(
                                "requested tag {tag} but resolved {}",
                                resolved_tag_value.as_deref().unwrap_or("(none)")
                            )),
                        });
                    }
                    if let Some(ver) = &fields.version
                        && human_readable_version != *ver
                    {
                        return Err(ProvisionFailure {
                            phase: ProvisionPhase::Resolve,
                            source: MediaPmError::Workflow(format!(
                                "requested version {ver} but resolved {human_readable_version}"
                            )),
                        });
                    }
                }
                ConfigVersionSpec::Latest => {}
                ConfigVersionSpec::Inherit => {
                    return Err(ProvisionFailure {
                        phase: ProvisionPhase::Resolve,
                        source: MediaPmError::Workflow(
                            "'inherit' version_spec is only valid for dependencies, not global tool requirements"
                                .to_string(),
                        ),
                    });
                }
            }

            // Await same-step deps' resolved versions (the pool edges) and
            // fold them into this entry's composite canonical version in
            // place of the old live-state lookups. Each dep publishes its
            // bare version right after its own resolve, so this wait ends as
            // soon as the deps have resolved — not after they finish
            // fetching. A dep that is not a pool job (no edge) is skipped,
            // matching the ungated fallthrough of the old live-state path.
            let mut dep_segments: BTreeMap<String, String> = BTreeMap::new();
            for dep_id in collect_same_step_dep_ids(
                tool_id,
                tool_req,
                crate::tools::dependency::known_dependency_type,
            ) {
                if let Some(&idx) = dep_indices.get(&dep_id)
                    && let Some(version) = gates.dep_version(idx).await
                {
                    dep_segments.insert(dep_id, version);
                }
            }

            // Compute expected composite canonical_version for skip check.
            let expected_composite = compute_composite_canonical_version(
                &canonical_version,
                tool_id,
                tool_req,
                &dep_segments,
            );

            // Publish this entry's bare (pre-composite) version to its
            // dependents right after resolve, so a dependent's composite
            // check unblocks while this entry is still fetching. The bare
            // segment preserves the non-transitive rule (never a composite).
            publisher.publish_version(canonical_version.clone());
            resolved_composite.clone_from(&expected_composite);

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
                // A skipped tool still contributes its reconstructed
                // (pre-inline) own map so a same-step dependent can inline it
                // without re-provisioning it.
                publisher.publish_own_map(own_map);
                return Ok(EntryOutcome::SkippedAfterResolve {
                    key: key.clone(),
                    spec_runtime,
                    backfill,
                    tool_record,
                });
            }

            let mut provision_metadata = metadata;
            provision_metadata.canonical_version = expected_composite;
            PreResolveOutcome::Resolved(fetch, provision_metadata)
        }
        Err(e) => {
            return Err(ProvisionFailure {
                phase: ProvisionPhase::Resolve,
                source: MediaPmError::Conductor(e),
            });
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
        Ok(Some(mut payload)) => {
            // Publish this tool's own pre-inline payload map so a same-step
            // dependent can inline it. Published before this tool's own
            // inlining so a dependent unblocks as soon as the payload exists
            // (after this tool's fetch AND process — the map is only known
            // once the bytes are CAS-imported); the map is exactly this
            // tool's own keys, never its deps' inlined keys (non-transitive).
            let pre_inline = payload.content_map.clone();
            publisher.publish_own_map(pre_inline);

            // Await same-step deps' pre-inline payload maps (the pool edges)
            // and inline them into this tool's content map under
            // `deps/<dep_id>/`. This runs after this tool's own fetch, so the
            // wait covers only the deps' processing, not this tool's.
            let mut dep_maps: BTreeMap<String, BTreeMap<String, String>> = BTreeMap::new();
            for dep_id in collect_same_step_dep_ids(
                tool_id,
                tool_req,
                crate::tools::dependency::known_dependency_type,
            ) {
                if let Some(&idx) = dep_indices.get(&dep_id)
                    && let Some(map) = gates.dep_own_map(idx).await
                {
                    dep_maps.insert(dep_id, map);
                }
            }
            payload.content_map.extend(inline_same_step_deps(
                tool_id,
                tool_req,
                &dep_maps,
                crate::tools::dependency::known_dependency_type,
            ));

            Ok(EntryOutcome::Fetched {
                tool_id: tool_id.clone(),
                is_builtin_code,
                already_exists,
                payload,
            })
        }
        Ok(None) => {
            // No payload (builtin/launcher or no sources): publish an empty
            // own map and carry the composite computed at resolve time.
            publisher.publish_own_map(BTreeMap::new());
            Ok(EntryOutcome::FetchedNone {
                tool_id: tool_id.clone(),
                is_builtin_code,
                already_exists,
                canonical_version: resolved_composite,
                resolved_tag: resolved_tag_value,
                resolved_version: resolved_version_value,
                resolved_vcs_hash: resolved_vcs_hash_value,
            })
        }
        // `fetch_and_import_tool_payload` tags its errors with the fetch or
        // process phase; pass the phase-tagged failure straight through.
        Err(failure) => Err(failure),
    }
}

/// Counts each tool-sync entry's completion exactly once against the
/// caller's `syncing tools` overall bar.
///
/// The bar's position means tools completed / total (spec R2). The pool
/// calls [`advance_once`](Self::advance_once) when an entry's provision
/// future settles, which moves the bar during the visible download phase;
/// the post-pool apply pass may re-assert the same index and is absorbed.
/// A raw `pb.advance(1)` on this bar is forbidden: two call sites that each
/// believe they own an outcome double-count under the first refactor.
pub(crate) struct OverallProgress {
    /// The `syncing tools` overall bar this counter drives.
    bar: Arc<dyn ProgressBarApi>,
    /// Entry indices already counted: `insert` returning `false` means an
    /// earlier call already advanced for that index, so this ask is absorbed.
    advanced: Mutex<HashSet<usize>>,
}

impl OverallProgress {
    /// Wrap the phase's overall bar in a per-entry counter.
    pub(crate) fn new(bar: &Arc<dyn ProgressBarApi>) -> Self {
        Self { bar: Arc::clone(bar), advanced: Mutex::new(HashSet::new()) }
    }

    /// Advance the overall bar for `entry_index`, at most once ever.
    pub(crate) fn advance_once(&self, entry_index: usize) {
        let counted =
            self.advanced.lock().expect("overall progress counter lock").insert(entry_index);
        if counted {
            self.bar.advance(1);
        }
    }
}

/// Applies a single entry outcome to the shared mutable state.
///
/// This is the ordered-merge half of the provisioning design: outcomes are
/// produced concurrently by the provisioning pool (see `reconcile_desired_tools`)
/// and applied sequentially in entry index order here, after the whole pool
/// has succeeded. Same-step dep inlining and dep-gated composites happen in
/// [`provision_entry`]; this pass only merges the finished result.
///
/// The trailing `entry_index`/`progress` pair re-asserts this entry against
/// the caller's [`OverallProgress`] in the skip arms. The pool already
/// counted every entry when its future settled, so the re-assert is absorbed
/// today; it matters for a skip decided outside the pool, which must land
/// exactly once if its index never advanced.
#[expect(
    clippy::too_many_arguments,
    reason = "this is the ordered-merge half of the provisioning design, so it deliberately takes every shared collection the merge can touch rather than hiding the merge surface behind accessors"
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
    report: &mut ToolSyncReport,
    live_state: &mut HashMap<String, Vec<ToolRegistryEntry>>,
    pruned_tools: &mut usize,
    inherited_env_vars: &BTreeMap<String, Vec<String>>,
    entry_index: usize,
    progress: &OverallProgress,
) {
    let tool_req = &entry.tool_requirement;

    match outcome {
        EntryOutcome::Skipped { key, spec_runtime, .. } => {
            tool_runtimes.entry(key).or_insert(spec_runtime);
            report.tools_skipped += 1;
            progress.advance_once(entry_index);
        }
        EntryOutcome::SkippedAfterResolve { key, spec_runtime, backfill, tool_record, .. } => {
            tool_runtimes.entry(key).or_insert(spec_runtime);
            report.resolved_field_backfills.push(backfill);
            report.tool_records.push(*tool_record);
            report.tools_skipped += 1;
            progress.advance_once(entry_index);
        }
        EntryOutcome::Fetched { tool_id, is_builtin_code, already_exists, mut payload } => {
            // Chain-member failures a later member covered: the tool
            // provisioned, but a source it declares does not serve. The sync
            // summary colours a non-empty warning list as a warning rather
            // than an error, so the run still succeeds while saying so.
            report.warnings.append(&mut payload.warnings);
            // Compute content-addressed hash. The payload's content map was
            // already inlined with same-step dep payloads by `provision_entry`.
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
            canonical_version,
            resolved_tag,
            resolved_version,
            resolved_vcs_hash,
        } => {
            let runtime = ToolRuntime {
                impure: false,
                inherited_env_vars: inherited_env_vars.get(&tool_id).cloned().unwrap_or_default(),
                ..ToolRuntime::default()
            };
            tool_runtimes.insert(tool_id.clone(), runtime.clone());

            let now = mediapm_utils::Timestamp::now();
            report.tool_records.push(ToolRegistryEntry {
                tool_id: tool_id.clone(),
                version: format!(
                    "{}+{}",
                    env!("CARGO_PKG_VERSION"),
                    crate::global::MEDIAPM_GIT_HASH
                ),
                canonical_version,
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
///
/// Returns one guard per materialized payload. The guard holds a shared lock on
/// the entry directory, so a step that executes from that directory — which is
/// what the `in_place` staging strategy does — keeps the payload it is running
/// from for as long as the caller holds the guard. The guards become part of
/// [`ToolSyncOutcome`] for that reason: a phase that dropped them on return
/// would let an expiry sweep or a retain pass remove a tree a live step is
/// executing out of.
async fn materialize_active_tool_runtimes(
    tools_dir: &Path,
    provision_cas: Arc<FileSystemCas>,
    generated_doc: &NickelDocument,
    tool_runtimes: &BTreeMap<String, ToolRuntime>,
) -> Result<Vec<ProvisionedTool>, MediaPmError> {
    let provision_cache =
        ProvisionCache::new(tools_dir.to_path_buf(), Arc::clone(&provision_cas), None);
    let mut guards: Vec<ProvisionedTool> = Vec::new();
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
        guards.push(provision_cache.materialize(&materialize_key, &content_map).await?);
    }
    Ok(guards)
}

/// What one reconciliation pass answers: its report, and the guards that keep
/// the payloads it materialized alive.
///
/// The guards are part of the outcome rather than a detail of the pass because
/// the caller owns the phases that execute out of those payloads. A pass that
/// kept them for itself would hand the next phase a directory any expiry sweep
/// or prune is free to remove.
pub(crate) struct ToolSyncOutcome {
    /// What the pass recorded, for the summary and the persisted registry.
    pub(crate) report: ToolSyncReport,
    /// One guard per materialized tool payload, held until the last phase that
    /// executes from the provisioned tree has finished.
    pub(crate) payload_guards: Vec<ProvisionedTool>,
}

#[cfg(test)]
impl ToolSyncOutcome {
    /// Answers the report and lets the guards go.
    ///
    /// A test drives one reconciliation pass and asserts on its counts; it runs
    /// no phase that executes from a payload, so it takes the report and drops
    /// the guards with it. The library sync keeps them instead.
    pub(crate) fn into_report(self) -> ToolSyncReport {
        self.report
    }
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
#[derive(Clone)]
struct ProvisionEntry {
    /// Bare `tool_id` used for provider resolution (e.g., "ffmpeg").
    tool_id: String,
    /// The tool requirement to apply when provisioning this entry.
    tool_requirement: ToolRequirement,
    /// Whether this entry came from the user config or was auto-vivified.
    kind: EntryKind,
}

#[derive(Clone)]
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
/// Deps absent from `dep_own_maps` (no published map) contribute nothing.
///
/// Returns the inlined key → hash entries; the requester's own keys are not
/// touched. The `known_dep_type` parameter mirrors
/// [`collect_same_step_dep_ids`] and is injectable for tests.
fn inline_same_step_deps(
    tool_id: &str,
    tool_req: &ToolRequirement,
    dep_own_maps: &BTreeMap<String, BTreeMap<String, String>>,
    known_dep_type: fn(&str, &str) -> Option<DependencyTypes>,
) -> BTreeMap<String, String> {
    let mut inlined = BTreeMap::new();
    for dep_id in collect_same_step_dep_ids(tool_id, tool_req, known_dep_type) {
        let Some(own_map) = dep_own_maps.get(&dep_id) else {
            continue; // dep published no map this pass — nothing to inline
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
/// `dep_segments` maps a same-step `dep_id` to the bare version that dep
/// published through the provisioning pool (see `provision_entry`). A dep with
/// no published segment contributes no segment here. For tools without
/// same-step deps, returns the bare version unchanged.
///
/// Dependency versions are **non-transitive**: each `dep_id:dep_ver` segment
/// carries the dep's OWN version segment (via [`own_version_segment`]), never
/// the dep's full composite — so even a caller that passes a composite segment
/// cannot nest it transitively.
pub(crate) fn compute_composite_canonical_version(
    bare: &str,
    tool_id: &str,
    tool_req: &ToolRequirement,
    dep_segments: &BTreeMap<String, String>,
) -> String {
    let same_step_deps = collect_same_step_dep_ids(
        tool_id,
        tool_req,
        crate::tools::dependency::known_dependency_type,
    );
    let dep_versions: Vec<(&str, &str)> = same_step_deps
        .iter()
        .filter_map(|dep_id| {
            let version = dep_segments.get(dep_id)?;
            // Non-transitive: reference the dep's OWN version segment even if
            // a caller passed a composite, so a dep's deps never leak
            // transitively into the requester.
            Some((dep_id.as_str(), own_version_segment(version)))
        })
        .collect();
    composite_canonical_version(bare, &dep_versions)
}

/// Build same-step dep version segments from stored live state, for the
/// post-sync "requires sync" check in `service.rs`.
///
/// The provisioning pool publishes dep versions through its gates at resolve
/// time; this check instead reads the deps' last-synced entries from
/// `live_state`, because it compares the stored composite against a
/// freshly-resolved own version without running a new provisioning pool. A dep
/// with no active entry (empty content map or spec mismatch) contributes no
/// segment, matching the pool's ungated fallthrough.
pub(crate) fn dep_segments_from_live_state(
    tool_id: &str,
    tool_req: &ToolRequirement,
    live_state: &HashMap<String, Vec<ToolRegistryEntry>>,
) -> BTreeMap<String, String> {
    let mut segments = BTreeMap::new();
    for dep_id in collect_same_step_dep_ids(
        tool_id,
        tool_req,
        crate::tools::dependency::known_dependency_type,
    ) {
        let Some(dep_group) = live_state.get(dep_id.as_str()) else {
            continue;
        };
        let Some(dep_req) = tool_req.dependencies.get(&dep_id) else {
            continue;
        };
        // Find the active entry for this dep. Inherit/Latest match any active
        // entry (the dep was resolved earlier in the pass); Exact verifies
        // against the spec via `spec_matches_entry`.
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
        });
        if let Some(matched) = matched {
            segments.insert(
                dep_id.clone(),
                own_version_segment(&matched.canonical_version).to_string(),
            );
        }
    }
    segments
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
/// The outcome carries the provision guards for the payloads this pass
/// materialized; the caller holds them for as long as the run executes from
/// those payloads.
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
) -> Result<ToolSyncOutcome, MediaPmError> {
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
    // The overall bar animates while the sync runs (Task 6); its
    // `finish_*` transition parks the row.
    pb.set_animating(true);
    // Exactly one advancement path: every entry-termination route below goes
    // through this counter, never a raw `advance` on `pb`.
    let progress = Arc::new(OverallProgress::new(&pb));

    let mut pruned_tools: usize = 0;

    // Build the pool's dependency edges. Each tool id maps to its FIRST
    // entries-order index (dedup keeps dep entries first), and a job's
    // `same_step_deps` are the pool indices of its same-step deps resolved
    // through that first-match rule. A same-step dep with no pool job
    // contributes no edge (ungated), matching the old live-state fallthrough.
    //
    // Structural safety notes (no config can violate these today):
    // - Edges always point from a later (explicit) index to an earlier
    //   (auto-vivified dep) index, so the pool graph is index-decreasing and
    //   therefore acyclic — no deadlock is possible, even sequentially. This,
    //   not any config check, is what makes cycle detection unnecessary
    //   (MPM-E003 covers circular `inherit` only).
    // - First-match means that if two requirers declared the same dep id with
    //   DIFFERENT version specs, dedup would keep both entries but the second
    //   requirer would fold the first requirer's dep version into its
    //   composite. No registry pair allows this today (only yt-dlp has
    //   same-step deps, with unique keys).
    // - A same-step dep with an EMPTY content map publishes a bare version,
    //   while the service-side comparison skips empty-content-map entries —
    //   such a dep would make sync-time and check-time composites diverge
    //   (perpetual `requires sync`). No registry same-step dep lacks a
    //   payload today (ffmpeg/deno fetch binaries).
    let mut dep_indices: BTreeMap<String, usize> = BTreeMap::new();
    for (idx, entry) in entries.iter().enumerate() {
        dep_indices.entry(entry.tool_id.clone()).or_insert(idx);
    }
    let jobs: Vec<pool::PoolJob<ProvisionEntry>> = entries
        .iter()
        .enumerate()
        .map(|(index, entry)| pool::PoolJob {
            index,
            same_step_deps: collect_same_step_dep_ids(
                &entry.tool_id,
                &entry.tool_requirement,
                crate::tools::dependency::known_dependency_type,
            )
            .iter()
            .filter_map(|dep_id| dep_indices.get(dep_id).copied())
            .collect(),
            input: entry.clone(),
        })
        .collect();

    // One bounded pool for every tool: `default_tool_provision_concurrency()`
    // caps how many are resolved/fetched/processed at once. A job awaits only
    // its same-step deps' published versions (before the composite/skip check)
    // and payload maps (before inlining), so nothing is serialized behind an
    // unrelated tool's full pipeline.
    let concurrency = default_tool_provision_concurrency().min(jobs.len()).max(1);
    // Pre-bind immutable views of the shared collaborators so the pool closure
    // captures plain `&` references (the mutable borrows are released once the
    // pool's closure/future are dropped, before the sequential apply below).
    let state_view: &MediaPmState = state;
    let gen_view: &NickelDocument = &generated_doc;
    let live_view: &HashMap<String, Vec<ToolRegistryEntry>> = &live_state;
    let cas_view: &FileSystemCas = workspace_cas.as_ref();
    let cache_view: &ToolDownloadCache = &cache;
    let dep_view: &BTreeMap<String, usize> = &dep_indices;
    let progress_view: &OverallProgress = &progress;
    let pool_result = pool::run_pool(jobs, concurrency, |job, gates, mut publisher| async move {
        let outcome = provision_entry(
            &job.input,
            state_view,
            gen_view,
            live_view,
            cas_view,
            cache_view,
            progress_group,
            recheck_policy,
            &gates,
            &mut publisher,
            dep_view,
        )
        .await;
        // Count the entry the moment its provision future settles — inside
        // the concurrent phase, so the bar moves during the visible download
        // phase rather than only after the pool returns. A failure settles
        // too and counts; the entries the failure cancels never settle and
        // never count, so `position == total` still means every entry
        // terminated.
        progress_view.advance_once(job.index);
        outcome
    })
    .await;

    match pool_result {
        Ok(outcomes) => {
            // Apply outcomes sequentially in entry index order — equal to the
            // old dep-then-explicit order, because dedup keeps dep entries
            // first. The whole pool succeeded before anything is applied.
            for (idx, outcome) in outcomes {
                apply_entry_outcome(
                    &entries[idx],
                    outcome,
                    &mut generated_doc,
                    &mut tool_runtimes,
                    &mut report,
                    &mut live_state,
                    &mut pruned_tools,
                    inherited_env_vars,
                    idx,
                    progress.as_ref(),
                );
            }
        }
        Err(pool_failure) => {
            // Fail fast: the first provisioning failure aborts the whole sync.
            // run_pool already dropped every in-flight job (cancelling
            // downloads); this returns before any apply, document save, env
            // write, or state write, so a failed run leaves the workspace
            // unchanged. The overall bar is finished red to match.
            pb.finish_error();
            return Err(MediaPmError::Workflow(format!(
                "tool sync failed: {} ({}): {}",
                entries[pool_failure.index].tool_id,
                pool_failure.source.phase,
                pool_failure.source.source,
            )));
        }
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
    // `entries`) plus the production conductor builtins — so stale versions of
    // managed tools keep their emptied `content_map` (see the per-entry pruning
    // above) and companion tools written this sync (or skipped from an earlier
    // sync) survive. Anything else belongs in the user-owned
    // `mediapm.conductor.ncl`, never here.
    //
    // The builtin set is the registry's production entries, the same set the
    // writer registers: a registration marked `test_only` is a test double, so
    // an entry an earlier build left behind in a generated document is pruned
    // like any other entry this sync did not produce.
    let provisioned_names: HashSet<&str> =
        entries.iter().map(|entry| entry.tool_id.as_str()).collect();
    let builtin_names: HashSet<&str> =
        mediapm_conductor::tools::production_builtins().map(|builtin| builtin.name).collect();

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
    if let Some(ref bar) = prn_bar {
        // The prune pass is live work (Task 6); its finish parks the row.
        bar.set_animating(true);
    }

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

    // Rebuild external_data from scratch: every hash a surviving tool content
    // map names, plus every hash a surviving workflow step references. A step
    // that names a bare CAS hash (the import builtin's payload) or an
    // `${external_data.<hash>}` expression is a consumer of those bytes, so
    // dropping its entry would both fail the conductor's encode invariant and
    // hand the payload to the GC. Hashes nothing references are excluded — no
    // separate cleanup needed.
    //
    // A step reference is declared without a description, because that is how
    // the plan declares the same hash when it synthesizes the step. A
    // description here would make the second sync rewrite the document the
    // first sync wrote, and an unchanged library has to leave it byte-identical.
    let mut data_usage = self::external_data::DataUsageTracker::new();
    for spec in generated_doc.tools.values() {
        for hash_str in spec.runtime.content_map.values() {
            if let Ok(hash) = hash_str.parse::<Hash>() {
                data_usage.record(hash, format!("managed tool content root for {}", spec.name));
            }
        }
    }
    let mut rebuilt = data_usage.finalize();
    for hash in self::external_data::referenced_hashes(&generated_doc) {
        rebuilt
            .entry(hash)
            .or_insert(ExternalDataEntry { description: None, save_mode: OutputSaveMode::Saved });
    }
    generated_doc.external_data = rebuilt;

    // 4. Ensure the tools runtime directory exists.
    std::fs::create_dir_all(&paths.tools_dir).map_err(|source| MediaPmError::Io {
        operation: "creating tools directory".to_string(),
        path: paths.tools_dir.clone(),
        source,
    })?;

    // 4b. Materialize active tool payloads into tools_dir via the provision
    // cache so `.env.generated` paths and workflow execution share one layout.
    // The guards go back to the caller, which holds them for the phases that
    // execute from these payloads: everything below, including this phase's own
    // prune pass, runs while the payloads it names are locked.
    let payload_guards = materialize_active_tool_runtimes(
        &paths.tools_dir,
        Arc::clone(&workspace_cas),
        &generated_doc,
        &tool_runtimes,
    )
    .await?;

    // 4c. Record where each payload actually landed and how a step stages it, so
    // the document names the directory the provisioner filled instead of one a
    // reader has to reconstruct.
    stamp_payload_roots(paths, &mut generated_doc, &mut tool_runtimes);

    // 5. Write generated runtime env file from tool runtimes (keyed by
    //    conductor tool id — env names derive from the stripped mediapm id,
    //    path values from the sanitized conductor id).
    write_generated_dotenv(&paths.runtime_root, &paths.tools_dir, &tool_runtimes)?;

    // 5. Save generated document.
    //
    // The media workflows are reconciled after this returns and save again, so a
    // concurrent reader can observe this intermediate document: it declares what
    // the tools phase knows, and not yet what the media plan adds. The
    // external-data invariant can reject a merge of it against a user document
    // that names a hash only the media plan declares; the reader's next attempt
    // succeeds once the second save lands.
    save_conductor_generated_document(paths, &generated_doc)?;

    // 6. Prune filesystem tool directories not in the active set. The
    //    provision cache keys directories by the sanitized conductor tool
    //    id (`tools_dir/<sanitize_tool_id(conductor_tool_id)>/payload/`),
    //    so the retain set must be conductor tool ids (the `tool_runtimes`
    //    keys), never mediapm tool ids — a mediapm-id set would prune every
    //    provisioned directory.
    let active_conductor_ids: HashSet<String> = tool_runtimes.keys().cloned().collect();
    // A directory that survives its own removal is still on disk, and the run
    // that asked for it to go is the only place a reader can learn that, so the
    // failure joins the sync warnings instead of ending the pass. The sweep
    // continues past it either way.
    report
        .warnings
        .extend(retain_only_tool_dirs(paths.tools_dir.clone(), active_conductor_ids).await?);

    if let Some(ref bar) = prn_bar {
        bar.advance(1);
        bar.finish_success();
    }

    report.pruned_tools = pruned_tools;

    Ok(ToolSyncOutcome { report, payload_guards })
}

/// Records the provisioned payload directory and the staging strategy on every
/// tool that has one.
///
/// The location belongs to the sync because the sync is what materializes the
/// payload: writing it here means no reader has to reconstruct the layout from a
/// tool id, and a payload that was already deployed on a previous run is named
/// exactly as a freshly provisioned one is. A tool whose payload directory does
/// not exist keeps the strategy its spec builder declared, and the executor
/// refuses it by name rather than running whatever happens to be there.
fn stamp_payload_roots(
    paths: &MediaPmPaths,
    generated_doc: &mut NickelDocument,
    tool_runtimes: &mut BTreeMap<String, ToolRuntime>,
) {
    let keys: Vec<String> = tool_runtimes.keys().cloned().collect();
    for conductor_tool_id in keys {
        let payload_dir = provisioned_payload_dir(&paths.tools_dir, &conductor_tool_id);
        if !payload_dir.is_dir() {
            continue;
        }
        let Ok(payload_root) = payload_dir.into_os_string().into_string() else {
            continue;
        };
        let Some(spec_key) = active_spec_key(generated_doc, &conductor_tool_id) else {
            continue;
        };
        if let Some(spec) = generated_doc.tools.get_mut(&spec_key) {
            if spec.runtime.content_map.is_empty() {
                continue;
            }
            spec.runtime.payload_root.clone_from(&payload_root);
            spec.runtime.payload_staging = PayloadStaging::InPlace;
        }
        if let Some(runtime) = tool_runtimes.get_mut(&conductor_tool_id) {
            runtime.payload_root = payload_root;
            runtime.payload_staging = PayloadStaging::InPlace;
        }
    }
}

/// Returns the generated-document key that holds a tool's active spec.
///
/// `tool_runtimes` keys managed tools by their conductor tool id and builtins by
/// their bare id, while the document may keep stale pruned versions of the same
/// name beside the active one. `find_active_tool_spec` is the same preference
/// the materializer uses, so both name one spec.
fn active_spec_key(generated_doc: &NickelDocument, conductor_tool_id: &str) -> Option<String> {
    if generated_doc.tools.contains_key(conductor_tool_id) {
        return Some(conductor_tool_id.to_string());
    }
    find_active_tool_spec(generated_doc, conductor_tool_id).map(|(key, _spec)| key.clone())
}

#[cfg(test)]
mod tests_active_tool_spec;
#[cfg(test)]
mod tests_cache_root;
#[cfg(test)]
mod tests_composite_version;
#[cfg(test)]
mod tests_dependency_specs;
#[cfg(test)]
mod tests_fetch_warnings;
#[cfg(test)]
mod tests_generated_document;
#[cfg(test)]
mod tests_managed_tools_registry;
#[cfg(test)]
mod tests_multi_tool_sync;
#[cfg(test)]
mod tests_overall_bar;
#[cfg(test)]
mod tests_payload_roots;
#[cfg(test)]
mod tests_provision_concurrency;
#[cfg(test)]
mod tests_provisioning_entries;
#[cfg(test)]
mod tests_prune_bar;
#[cfg(test)]
mod tests_pruning;
#[cfg(test)]
mod tests_same_step_deps;
#[cfg(test)]
mod tests_skip_path;
