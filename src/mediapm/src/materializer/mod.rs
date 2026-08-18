//! Top-level materialization orchestration: hierarchy sync.
//!
//! Coordinates concurrent materialization of hierarchy entries from CAS
//! content to the filesystem hierarchy root.

pub(crate) mod commit;
pub(crate) mod file_ops;
mod metadata;
mod playlist;
mod resolve;
mod zip;

use std::collections::{BTreeMap, BTreeSet};
use std::path::{Path, PathBuf};
use std::sync::Arc;

use mediapm_cas::{FileSystemCas, Hash};
use tokio::sync::Semaphore;
use tracing::{info, warn};

use crate::config::hierarchy_types::{
    FlattenedHierarchyEntry, HierarchyEntryKind, collect_playlist_media_index,
    expand_variant_selectors, flatten_hierarchy_nodes_for_runtime,
};
use crate::config::source_types::MediaSourceSpec;
use crate::config::{ManagedFileRecord, MediaPmDocument, MediaPmState};
use crate::error::MediaPmError;
use crate::output::progress::{PrefixComponents, ProgressBarApi, ProgressGroup, ProgressGroupApi};
use crate::paths::MediaPmPaths;
use crate::tools::workflows::resolve_ffmpeg_slot_limits;
use mediapm_conductor::{ConductorState, NickelDocument};
pub(crate) use resolve::backfill_source_variant_hashes_from_workflow_outputs;

use self::metadata::{
    MaterializationLookupContext, StepOutputHashes, resolve_interpolated_folder_rename_rules,
};
use self::playlist::{
    PlaylistEntryPathMode, RenderedPlaylistEntry, generate_playlist_bytes,
    resolve_playlist_target_relative_path,
};
use self::resolve::{
    collect_media_source_available_variants, resolve_hierarchy_source, resolve_variant_hash,
    resolve_variant_source_bytes,
};

// ---------------------------------------------------------------------------
// Internal resolve types (shared with resolve.rs)
// ---------------------------------------------------------------------------

/// Per-workflow required step output names (`step_id -> output_name[]`).
pub(super) type RequiredStepOutputNames = BTreeMap<String, BTreeSet<String>>;

/// Per-workflow required ZIP member selectors (`step_id -> output_name -> zip_member[]`).
pub(super) type RequiredStepZipMembers = BTreeMap<String, BTreeMap<String, BTreeSet<String>>>;

/// Per-step expected inputs used to match runtime workflow instances.
#[derive(Debug, Clone, PartialEq, Eq, Default)]
pub(super) struct ExpectedStepInputs {
    /// Deterministically resolved input hashes.
    pub(super) resolved_hashes: BTreeMap<String, Hash>,
    /// Input names whose hashes cannot be reconstructed from persisted CAS.
    pub(super) unresolved_hash_input_names: BTreeSet<String>,
}

/// One input-binding hash resolution result.
pub(super) enum InputBindingHashResolution {
    /// Fully reconstructed deterministic input hash.
    Resolved(Hash),
    /// Referenced prior step output is unavailable in the current traversal order.
    MissingPriorStepOutput,
    /// Referenced step output exists but cannot be reconstructed from CAS bytes.
    MissingMaterializedStepOutput,
}

/// Resolved variant payload for materialization.
pub(super) struct VariantSourceBytes {
    /// Materialized file bytes.
    pub(super) bytes: Vec<u8>,
    /// Optional non-fatal fallback notice.
    pub(super) notice: Option<String>,
    /// Source CAS hash when bytes map directly to one stored object.
    pub(super) source_hash: Option<Hash>,
}
use self::zip::{compile_hierarchy_folder_rename_rules, extract_zip_folder_variant_bytes};

// ---------------------------------------------------------------------------
// Report type
// ---------------------------------------------------------------------------

/// Summary of one `sync_hierarchy` invocation.
#[derive(Debug, Clone, PartialEq, Eq, Default)]
pub struct MaterializeReport {
    /// Number of hierarchy paths materialized (new or updated).
    pub materialized_paths: usize,
    /// Number of hierarchy paths skipped (unchanged).
    pub skipped_paths: usize,
    /// Number of stale hierarchy paths removed.
    pub removed_paths: usize,
    /// Number of empty parent directories removed after stale path cleanup.
    pub removed_empty_dirs: usize,
    /// Non-fatal notices collected during materialization.
    pub notices: Vec<String>,
}

// ---------------------------------------------------------------------------
// Internal types
// ---------------------------------------------------------------------------

/// The result of preparing one flattened hierarchy entry.
struct PreparedHierarchyEntryResult {
    /// Whether the entry was actually materialized (not skipped).
    materialized: bool,
    /// Managed file records keyed by hierarchy-relative path.
    managed_files: BTreeMap<String, ManagedFileRecord>,
    /// Per-media variant hash updates (`media_id -> variant -> hash`).
    media_variant_updates: BTreeMap<String, BTreeMap<String, String>>,
}

/// Shared state passed to each hierarchy entry worker.
struct SyncSharedState {
    /// Resolved library root path.
    hierarchy_root: PathBuf,
    /// CAS store reference.
    cas: FileSystemCas,
    /// Flattened hierarchy for stale-path scanning.
    flattened: Vec<FlattenedHierarchyEntry>,
    /// Whether to CAS-verify materialized outputs after writing.
    verify_materialization: bool,
}

// ---------------------------------------------------------------------------
// Worker count
// ---------------------------------------------------------------------------

/// Returns the number of concurrent hierarchy-worker tasks.
fn hierarchy_worker_count() -> usize {
    let count = std::thread::available_parallelism().map_or(4, std::num::NonZero::get);
    count.clamp(1, 1024)
}

// ---------------------------------------------------------------------------
// Public entry point
// ---------------------------------------------------------------------------

/// Synchronises all hierarchy entries from CAS content to the filesystem
/// hierarchy root.
///
/// # Fast-path
///
/// If the document content hash matches the hash stored during a previous
/// sync cycle (tracked in-memory), the entire sync is skipped.
///
/// # Concurrency
///
/// Hierarchy entries are processed concurrently using a bounded worker pool
/// sized to the number of available CPU cores (capped at 1024).
#[expect(
    clippy::too_many_arguments,
    clippy::too_many_lines,
    reason = "sync_hierarchy orchestrates the full materialization pipeline in one place"
)]
pub async fn sync_hierarchy(
    paths: &MediaPmPaths,
    document: &MediaPmDocument,
    state: &mut MediaPmState,
    cas: &FileSystemCas,
    verify_materialization: bool,
    conductor_state: &ConductorState,
    generated_doc: &NickelDocument,
    progress_group: Option<Arc<dyn ProgressGroupApi + Send + Sync>>,
) -> Result<MaterializeReport, MediaPmError> {
    let hierarchy_root = &paths.hierarchy_root_dir;

    let mut flattened = flatten_hierarchy_nodes_for_runtime(&document.hierarchy)?;
    if flattened.is_empty() {
        info!("hierarchy is empty, nothing to materialize");
        return Ok(MaterializeReport::default());
    }

    let ffmpeg_slot_limits = resolve_ffmpeg_slot_limits(document);
    let lookup_context = MaterializationLookupContext::new(
        cas.clone(),
        Some(conductor_state.clone()),
        generated_doc.clone(),
        ffmpeg_slot_limits,
    );
    metadata::resolve_flattened_entry_paths(&mut flattened, document, &lookup_context).await?;
    let shared = Arc::new(SyncSharedState {
        hierarchy_root: hierarchy_root.clone(),
        cas: cas.clone(),
        flattened: flattened.clone(),
        verify_materialization,
    });

    // --- Concurrent materialization ---
    let worker_count = hierarchy_worker_count();
    let semaphore = Arc::new(Semaphore::new(worker_count));
    let (owned_group, pb) = if let Some(ref pg) = progress_group {
        let bar = pg.add_bar(flattened.len() as u64, "materializing [mat]");
        bar.set_prefix_components(PrefixComponents {
            tool_name: "materializing".to_string(),
            phase: "mat".to_string(),
            count: "0".to_string(),
            total: flattened.len().to_string(),
            ..PrefixComponents::default()
        });
        (None, bar)
    } else {
        let g = ProgressGroup::builder().dynamic_height(true).build();
        let p: Arc<dyn ProgressBarApi> =
            Arc::new(g.add_bar(flattened.len() as u64, "materializing [mat]"));
        (Some(g), p)
    };

    let mut join_set = tokio::task::JoinSet::new();
    let document_arc = Arc::new(document.clone());

    for entry in &flattened {
        let entry = entry.clone();
        let document = document_arc.clone();
        let shared = shared.clone();
        let lookup_context = lookup_context.clone();
        let semaphore = semaphore.clone();
        let pb = pb.clone();
        let progress_group = progress_group.clone();

        join_set.spawn(async move {
            let _permit = semaphore.acquire().await.unwrap();
            let result = prepare_hierarchy_entry(
                &entry,
                document.as_ref(),
                &shared,
                &lookup_context,
                progress_group,
            )
            .await;
            pb.advance(1);
            result
        });
    }

    // Collect results.
    let mut report = MaterializeReport::default();
    let mut materialize_error: Option<MediaPmError> = None;
    let mut desired_managed_paths = BTreeSet::new();
    while let Some(result) = join_set.join_next().await {
        match result {
            Ok(Ok(entry_result)) => {
                if entry_result.materialized {
                    report.materialized_paths += 1;
                } else {
                    report.skipped_paths += 1;
                }
                desired_managed_paths.extend(entry_result.managed_files.keys().cloned());
                for (path, record) in entry_result.managed_files {
                    state.managed_files.insert(path, record);
                }
                for (media_id, variant_hashes) in entry_result.media_variant_updates {
                    let step_state = state.workflow_states.entry(media_id).or_default();
                    for (variant, hash) in variant_hashes {
                        step_state.variant_hashes.insert(variant, hash);
                    }
                }
            }
            Ok(Err(e)) => {
                materialize_error = Some(e);
                break;
            }
            Err(e) => {
                materialize_error = Some(MediaPmError::Workflow(format!(
                    "hierarchy materialization task panicked: {e}"
                )));
                break;
            }
        }
    }

    if materialize_error.is_some() {
        pb.finish_error();
    } else {
        pb.finish_success();
    }
    if let Some(g) = owned_group {
        g.join();
    }
    if let Some(e) = materialize_error {
        return Err(e);
    }

    let stale_managed_paths: Vec<String> = state
        .managed_files
        .keys()
        .filter(|path| !desired_managed_paths.contains(path.as_str()))
        .cloned()
        .collect();
    for stale in stale_managed_paths {
        state.managed_files.remove(&stale);
    }

    // --- Stale path cleanup ---
    let stale_result = remove_stale_paths(hierarchy_root, &flattened, &desired_managed_paths)?;
    report.removed_paths = stale_result.0;
    report.removed_empty_dirs = stale_result.1;

    info!(
        "materialization complete: {} materialized, {} skipped, {} removed, {} empty dirs removed",
        report.materialized_paths,
        report.skipped_paths,
        report.removed_paths,
        report.removed_empty_dirs,
    );

    Ok(report)
}

// ---------------------------------------------------------------------------
// Prepare one hierarchy entry
// ---------------------------------------------------------------------------

/// Materialises one flattened hierarchy entry from CAS content to the
/// filesystem hierarchy root.
///
/// Handles all three entry kinds:
/// - `Media`: single-file variant materialization.
/// - `MediaFolder`: multi-variant or ZIP-folder materialization.
/// - `Playlist`: playlist file generation.
async fn prepare_hierarchy_entry(
    entry: &FlattenedHierarchyEntry,
    document: &MediaPmDocument,
    shared: &SyncSharedState,
    lookup: &MaterializationLookupContext,
    progress_group: Option<Arc<dyn ProgressGroupApi + Send + Sync>>,
) -> Result<PreparedHierarchyEntryResult, MediaPmError> {
    let relative_path = entry.path_str();
    let target_path = shared.hierarchy_root.join(&relative_path);

    // Per-entry phase bar: stage → verify → commit. Owned by mediapm (not the
    // conductor), so it carries the `[stg]`/`[vrf]`/`[cmt]` phase tags.
    let entry_bar: Option<Arc<dyn ProgressBarApi>> = progress_group.clone().map(|pg| {
        let bar = pg.add_bar(3, &format!("{relative_path} [stg]"));
        bar.set_prefix_components(PrefixComponents {
            tool_name: relative_path.clone(),
            phase: "stg".to_string(),
            count: "0".to_string(),
            total: "3".to_string(),
            ..PrefixComponents::default()
        });
        bar
    });

    match entry.entry.kind {
        HierarchyEntryKind::Media => {
            // Resolve the source spec (playlist entries carry no media id).
            let source = resolve_hierarchy_source(document, &entry.entry)?;
            let media_id = &entry.entry.media_id;

            // Single-file materialization.
            let variant_name =
                entry.entry.variants.first().cloned().unwrap_or_else(|| "default".to_string());

            let variant_selector = expand_variant_selectors(
                &entry.entry.variants,
                &collect_media_source_available_variants(source),
            )
            .map_err(|e| {
                MediaPmError::Workflow(format!(
                    "media '{media_id}': variant selector expansion failed: {e}"
                ))
            })?;

            let effective_variant = variant_selector.first().cloned().unwrap_or(variant_name);

            if let Some(ref bar) = entry_bar {
                bar.set_prefix_components(PrefixComponents {
                    tool_name: relative_path.clone(),
                    phase: "vrf".to_string(),
                    count: "1".to_string(),
                    total: "3".to_string(),
                    ..PrefixComponents::default()
                });
            }
            let hash = resolve_variant_hash(media_id, &effective_variant, source, lookup).await?;

            if let Some(hash) = hash {
                if let Some(ref bar) = entry_bar {
                    bar.set_prefix_components(PrefixComponents {
                        tool_name: relative_path.clone(),
                        phase: "cmt".to_string(),
                        count: "2".to_string(),
                        total: "3".to_string(),
                        ..PrefixComponents::default()
                    });
                }
                materialize_file_entry(&target_path, &relative_path, &hash, shared).await?;
                let record = ManagedFileRecord {
                    media_id: media_id.clone(),
                    variant: effective_variant.clone(),
                    hash: hash.to_string(),
                };
                if let Some(ref bar) = entry_bar {
                    bar.advance(1);
                    bar.finish_success();
                }
                Ok(PreparedHierarchyEntryResult {
                    materialized: true,
                    managed_files: BTreeMap::from([(relative_path.clone(), record)]),
                    media_variant_updates: BTreeMap::from([(
                        media_id.clone(),
                        BTreeMap::from([(effective_variant.clone(), hash.to_string())]),
                    )]),
                })
            } else {
                shared.notice(format!(
                    "media '{media_id}' variant '{effective_variant}' has no content hash; skipping"
                ));
                if let Some(ref bar) = entry_bar {
                    bar.advance(1);
                    bar.finish_warning();
                }
                Ok(PreparedHierarchyEntryResult {
                    materialized: false,
                    managed_files: BTreeMap::new(),
                    media_variant_updates: BTreeMap::new(),
                })
            }
        }
        HierarchyEntryKind::MediaFolder => {
            // Resolve the source spec (playlist entries carry no media id).
            let source = resolve_hierarchy_source(document, &entry.entry)?;
            let media_id = &entry.entry.media_id;

            // Multi-variant materialization (directory output).
            let result = materialize_media_folder_entry(
                entry,
                source,
                media_id,
                &target_path,
                &relative_path,
                shared,
                lookup,
                progress_group,
            )
            .await;
            if let Some(ref bar) = entry_bar {
                match &result {
                    Ok(_) => {
                        bar.advance(1);
                        bar.finish_success();
                    }
                    Err(_) => {
                        bar.advance(1);
                        bar.finish_error();
                    }
                }
            }
            result
        }
        HierarchyEntryKind::Playlist => {
            // Playlist generation.
            let result =
                materialize_playlist_entry(entry, document, &target_path, &relative_path, shared)
                    .await;
            if let Some(ref bar) = entry_bar {
                match &result {
                    Ok(_) => {
                        bar.advance(1);
                        bar.finish_success();
                    }
                    Err(_) => {
                        bar.advance(1);
                        bar.finish_error();
                    }
                }
            }
            result
        }
    }
}

// ---------------------------------------------------------------------------
// Media entry materialization
// ---------------------------------------------------------------------------

/// Materialises one file entry from CAS directly to the target path.
async fn materialize_file_entry(
    target_path: &Path,
    relative_path: &str,
    hash: &Hash,
    shared: &SyncSharedState,
) -> Result<(), MediaPmError> {
    use crate::config::MaterializationMethod;
    use crate::materializer::file_ops::materialize_file_from_cas_with_order;

    // Ensure parent directory exists.
    if let Some(parent) = target_path.parent() {
        tokio::fs::create_dir_all(parent).await.map_err(|source| MediaPmError::Io {
            operation: "creating parent directory for materialized output".to_string(),
            path: parent.to_path_buf(),
            source,
        })?;
    }

    // Determine materialization methods from runtime config.
    // Use default order: hardlink → symlink → reflink → copy.
    let methods = vec![
        MaterializationMethod::Hardlink,
        MaterializationMethod::Symlink,
        MaterializationMethod::Reflink,
        MaterializationMethod::Copy,
    ];

    let mut notices = Vec::new();
    materialize_file_from_cas_with_order(
        &shared.cas,
        *hash,
        target_path,
        relative_path,
        &methods,
        &mut notices,
    )
    .await?;

    // Verify materialized content matches the expected CAS hash.
    if shared.verify_materialization {
        let data = tokio::fs::read(target_path).await.map_err(|source| MediaPmError::Io {
            operation: "reading materialized file for verification".to_string(),
            path: target_path.to_path_buf(),
            source,
        })?;
        let actual_hash = Hash::from_content(&data);
        if actual_hash != *hash {
            return Err(MediaPmError::Workflow(format!(
                "materialized file '{relative_path}' verification failed: \
                 expected {hash}, got {actual_hash}"
            )));
        }
    }

    // Mark output as read-only.
    crate::materializer::commit::ensure_managed_path_readonly(target_path)?;

    Ok(())
}

/// Materialises a media-folder (multi-variant or ZIP-folder) entry.
#[expect(
    clippy::too_many_lines,
    reason = "media-folder materialization handles folder variants and rename rules inline"
)]
async fn materialize_media_folder_entry(
    entry: &FlattenedHierarchyEntry,
    source: &MediaSourceSpec,
    media_id: &str,
    target_path: &Path,
    relative_path: &str,
    shared: &SyncSharedState,
    lookup: &MaterializationLookupContext,
    progress_group: Option<Arc<dyn ProgressGroupApi + Send + Sync>>,
) -> Result<PreparedHierarchyEntryResult, MediaPmError> {
    tokio::fs::create_dir_all(target_path).await.map_err(|source| MediaPmError::Io {
        operation: "creating media-folder directory".to_string(),
        path: target_path.to_path_buf(),
        source,
    })?;

    // Resolve variant selectors.
    let available = collect_media_source_available_variants(source);
    let selected_variants = if entry.entry.variants.is_empty() {
        // No selectors → use all available variants.
        available.iter().cloned().collect::<Vec<_>>()
    } else {
        expand_variant_selectors(&entry.entry.variants, &available).map_err(|e| {
            MediaPmError::Workflow(format!(
                "media '{media_id}': variant selector expansion failed: {e}"
            ))
        })?
    };

    let interpolated_rename_rules = resolve_interpolated_folder_rename_rules(
        &entry.entry.rename_files,
        media_id,
        source,
        lookup,
    )
    .await?;
    let rename_rules = compile_hierarchy_folder_rename_rules(&interpolated_rename_rules)?;
    let mut managed_files = BTreeMap::new();
    let mut variant_hashes = BTreeMap::new();

    for variant_name in &selected_variants {
        let variant_path = target_path.join(variant_name);

        let payload = match resolve_variant_source_bytes(
            lookup,
            media_id,
            source,
            variant_name,
            true,
        )
        .await
        {
            Ok(payload) => payload,
            Err(error) => {
                shared.notice(format!(
                    "media '{media_id}' variant '{variant_name}' resolution failed: {error}"
                ));
                continue;
            }
        };

        let data = payload.bytes;
        if let Some(notice) = payload.notice {
            shared.notice(notice);
        }
        if let Some(source_hash) = payload.source_hash {
            variant_hashes.insert(variant_name.clone(), source_hash.to_string());
        }

        // Per-variant file sub-bar: advanced once per written extracted/file
        // member so the materialization screen shows per-file progress.
        let is_zip = is_zip_content(&data);
        if is_zip {
            let extracted = extract_zip_folder_variant_bytes(&data, &rename_rules)?;
            if extracted.is_empty() {
                shared.notice(format!(
                    "media '{media_id}' variant '{variant_name}': ZIP archive contained zero extractable files"
                ));
            }
            let file_bar = progress_group.clone().map(|pg| {
                let sub = pg.add_bar(extracted.len() as u64, &format!("{variant_name} [wrt]"));
                sub.set_prefix_components(PrefixComponents {
                    tool_name: format!("{relative_path}/{variant_name}"),
                    phase: "wrt".to_string(),
                    count: "0".to_string(),
                    total: extracted.len().to_string(),
                    ..PrefixComponents::default()
                });
                sub
            });
            for (file_rel_path, content) in extracted {
                let file_rel_path = normalize_yt_dlp_sandbox_zip_member_path(&file_rel_path);
                let file_target = target_path.join(&file_rel_path);
                let file_relative = format!(
                    "{relative_path}/{}",
                    file_rel_path.to_string_lossy().replace('\\', "/")
                );
                if let Some(parent) = file_target.parent() {
                    tokio::fs::create_dir_all(parent).await.map_err(|source| MediaPmError::Io {
                        operation: "creating extracted-file parent directory".to_string(),
                        path: parent.to_path_buf(),
                        source,
                    })?;
                }
                tokio::fs::write(&file_target, &content).await.map_err(|source| {
                    MediaPmError::Io {
                        operation: "writing extracted variant file".to_string(),
                        path: file_target.clone(),
                        source,
                    }
                })?;
                crate::materializer::commit::ensure_managed_path_readonly(&file_target)?;
                let hash = Hash::from_content(&content);
                managed_files.insert(
                    file_relative,
                    ManagedFileRecord {
                        media_id: media_id.to_string(),
                        variant: variant_name.clone(),
                        hash: hash.to_string(),
                    },
                );
                if let Some(ref sub) = file_bar {
                    sub.advance(1);
                }
            }
            if let Some(ref sub) = file_bar {
                sub.finish_success();
            }
        } else {
            if variant_path.exists()
                && std::fs::metadata(&variant_path).is_ok_and(|metadata| metadata.is_dir())
            {
                shared.notice(format!(
                    "media '{media_id}' variant '{variant_name}': skipping non-archive write because '{}' is already a directory",
                    variant_path.display()
                ));
                continue;
            }
            if let Some(parent) = variant_path.parent() {
                tokio::fs::create_dir_all(parent).await.map_err(|source| MediaPmError::Io {
                    operation: "creating variant-file parent directory".to_string(),
                    path: parent.to_path_buf(),
                    source,
                })?;
            }
            tokio::fs::write(&variant_path, &data).await.map_err(|source| MediaPmError::Io {
                operation: "writing variant file".to_string(),
                path: variant_path.clone(),
                source,
            })?;
            crate::materializer::commit::ensure_managed_path_readonly(&variant_path)?;
            let file_relative = format!("{relative_path}/{variant_name}");
            let hash = payload.source_hash.unwrap_or_else(|| Hash::from_content(&data));
            managed_files.insert(
                file_relative,
                ManagedFileRecord {
                    media_id: media_id.to_string(),
                    variant: variant_name.clone(),
                    hash: hash.to_string(),
                },
            );
        }
    }

    Ok(PreparedHierarchyEntryResult {
        materialized: true,
        managed_files,
        media_variant_updates: BTreeMap::from([(media_id.to_string(), variant_hashes)]),
    })
}

// ---------------------------------------------------------------------------
// Playlist entry materialization
// ---------------------------------------------------------------------------

/// Generates a playlist file from the media entries referenced by a playlist
/// hierarchy node.
async fn materialize_playlist_entry(
    entry: &FlattenedHierarchyEntry,
    _document: &MediaPmDocument,
    target_path: &Path,
    relative_path: &str,
    shared: &SyncSharedState,
) -> Result<PreparedHierarchyEntryResult, MediaPmError> {
    // Build playlist entries from the flattened hierarchy.
    let media_index = collect_playlist_media_index(&shared.flattened).map_err(|e| {
        MediaPmError::Workflow(format!("collecting playlist media index failed: {e}"))
    })?;

    // Find media ids referenced by this playlist entry.
    let playlist_media_ids = media_index.get(&entry.path_str()).cloned().unwrap_or_default();

    let mut rendered_entries = Vec::new();

    for media_id in &playlist_media_ids {
        // Find the flattened entry for this media id.
        if let Some(media_entry) = shared.flattened.iter().find(|fe| {
            fe.entry.media_id == *media_id
                && matches!(
                    fe.entry.kind,
                    HierarchyEntryKind::Media | HierarchyEntryKind::MediaFolder
                )
        }) {
            let media_relative_path = media_entry.path_str();
            let resolved = resolve_playlist_target_relative_path(
                relative_path,
                &media_relative_path,
                PlaylistEntryPathMode::Relative,
            );
            rendered_entries.push(RenderedPlaylistEntry {
                id: media_id.clone(),
                path: resolved.to_string_lossy().to_string(),
            });
        }
    }

    // Ensure parent directory exists.
    if let Some(parent) = target_path.parent() {
        tokio::fs::create_dir_all(parent).await.map_err(|source| MediaPmError::Io {
            operation: "creating playlist parent directory".to_string(),
            path: parent.to_path_buf(),
            source,
        })?;
    }

    let bytes = generate_playlist_bytes(&rendered_entries, entry.entry.format);
    tokio::fs::write(target_path, &bytes).await.map_err(|source| MediaPmError::Io {
        operation: "writing playlist file".to_string(),
        path: target_path.to_path_buf(),
        source,
    })?;

    crate::materializer::commit::ensure_managed_path_readonly(target_path)?;

    Ok(PreparedHierarchyEntryResult {
        materialized: true,
        managed_files: BTreeMap::new(),
        media_variant_updates: BTreeMap::new(),
    })
}

// ---------------------------------------------------------------------------
// Stale-path cleanup
// ---------------------------------------------------------------------------

/// Workspace-root entries that must survive stale hierarchy scans when
/// `hierarchy_root_dir` defaults to the mediapm config directory.
const HIERARCHY_ROOT_RESERVED_NAMES: &[&str] =
    &["mediapm.ncl", "mediapm.conductor.ncl", "mediapm.conductor.generated.ncl", "tool-cache"];

/// Returns true when a hierarchy-root scan entry must not be removed or
/// descended into (hidden paths and workspace config artifacts).
fn is_stale_scan_excluded(name: &str, relative_prefix: &str) -> bool {
    name.starts_with('.')
        || (relative_prefix.is_empty() && HIERARCHY_ROOT_RESERVED_NAMES.contains(&name))
}

/// Returns true when `relative_path` is declared by the flattened hierarchy or
/// materialized as a managed descendant of one declared directory path.
#[must_use]
fn is_protected_hierarchy_path(
    relative_path: &str,
    current_paths: &BTreeSet<String>,
    managed_paths: &BTreeSet<String>,
) -> bool {
    if current_paths.contains(relative_path) || managed_paths.contains(relative_path) {
        return true;
    }

    current_paths.iter().any(|current| is_relative_path_under_prefix(relative_path, current))
        || managed_paths.iter().any(|managed| is_relative_path_under_prefix(relative_path, managed))
}

#[must_use]
fn is_relative_path_under_prefix(relative_path: &str, prefix: &str) -> bool {
    if relative_path.len() <= prefix.len() {
        return false;
    }

    let boundary = relative_path.as_bytes().get(prefix.len());
    relative_path.starts_with(prefix) && matches!(boundary, Some(b'/'))
}

/// Removes filesystem paths that are no longer present in the flattened
/// hierarchy, plus any empty parent directories left behind.
///
/// Returns `(removed_paths, removed_empty_dirs)`.
fn remove_stale_paths(
    hierarchy_root: &Path,
    current_entries: &[FlattenedHierarchyEntry],
    managed_paths: &BTreeSet<String>,
) -> Result<(usize, usize), MediaPmError> {
    let current_paths: BTreeSet<String> =
        current_entries.iter().map(FlattenedHierarchyEntry::path_str).collect();

    let mut removed_paths = 0usize;
    let mut removed_empty_dirs = 0usize;

    // Scan the hierarchy root directory for stale paths.
    if hierarchy_root.exists() {
        remove_stale_recursive(
            hierarchy_root,
            hierarchy_root,
            "",
            &current_paths,
            managed_paths,
            &mut removed_paths,
            &mut removed_empty_dirs,
        )?;
    }

    Ok((removed_paths, removed_empty_dirs))
}

/// Recursively scans for stale paths relative to the current hierarchy.
#[expect(
    clippy::only_used_in_recursion,
    reason = "recursive stale-path scan with a single external entry point"
)]
fn remove_stale_recursive(
    absolute_root: &Path,
    absolute_dir: &Path,
    relative_prefix: &str,
    current_paths: &BTreeSet<String>,
    managed_paths: &BTreeSet<String>,
    removed_paths: &mut usize,
    removed_empty_dirs: &mut usize,
) -> Result<(), MediaPmError> {
    use crate::materializer::commit::remove_path;

    let Ok(mut dir) = std::fs::read_dir(absolute_dir) else {
        return Ok(());
    };

    while let Some(entry) = dir.next().transpose().map_err(|source| MediaPmError::Io {
        operation: "reading directory entry during stale-path scan".to_string(),
        path: absolute_dir.to_path_buf(),
        source,
    })? {
        let name = entry.file_name();
        let name_str = name.to_string_lossy().to_string();
        if is_stale_scan_excluded(&name_str, relative_prefix) {
            continue;
        }
        let relative_path = if relative_prefix.is_empty() {
            name_str.clone()
        } else {
            format!("{relative_prefix}/{name_str}")
        };
        let absolute_path = entry.path();

        if entry
            .file_type()
            .map_err(|source| MediaPmError::Io {
                operation: "reading file type during stale-path scan".to_string(),
                path: absolute_path.clone(),
                source,
            })?
            .is_dir()
        {
            // Recurse into subdirectory.
            remove_stale_recursive(
                absolute_root,
                &absolute_path,
                &relative_path,
                current_paths,
                managed_paths,
                removed_paths,
                removed_empty_dirs,
            )?;

            // After recursion, remove directory if empty and not in current hierarchy.
            if !is_protected_hierarchy_path(&relative_path, current_paths, managed_paths)
                && is_directory_empty(&absolute_path)?
            {
                remove_path(&absolute_path)?;
                *removed_empty_dirs += 1;
            }
        } else if !is_protected_hierarchy_path(&relative_path, current_paths, managed_paths) {
            // Remove stale file.
            remove_path(&absolute_path)?;
            *removed_paths += 1;
        }
    }

    Ok(())
}

/// Returns `true` if a directory is empty or contains only `.DS_Store`.
fn is_directory_empty(path: &Path) -> Result<bool, MediaPmError> {
    let mut dir = std::fs::read_dir(path).map_err(|source| MediaPmError::Io {
        operation: "reading directory to check emptiness".to_string(),
        path: path.to_path_buf(),
        source,
    })?;

    while let Some(entry) = dir.next().transpose().map_err(|source| MediaPmError::Io {
        operation: "reading directory entry during emptiness check".to_string(),
        path: path.to_path_buf(),
        source,
    })? {
        let name = entry.file_name();
        let name_str = name.to_string_lossy();
        if name_str != ".DS_Store" {
            return Ok(false);
        }
    }

    Ok(true)
}

// ---------------------------------------------------------------------------
// Helpers
// ---------------------------------------------------------------------------

/// Checks if a byte slice is a ZIP archive (local file header or EOCD signature).
fn is_zip_content(data: &[u8]) -> bool {
    data.len() >= 2 && data[0] == 0x50 && data[1] == 0x4B
}

/// yt-dlp sandbox disambiguation marker in captured filenames (must not appear in hierarchy).
const YT_DLP_MEDIAPM_SANDBOX_MARKER: &str = "__mediapm__";

/// Strips yt-dlp sandbox `downloads/` prefix from one ZIP member path.
fn strip_yt_dlp_sandbox_downloads_prefix(path: &Path) -> PathBuf {
    let normalized = path.to_string_lossy().replace('\\', "/");
    let stripped = normalized.strip_prefix("downloads/").unwrap_or(normalized.as_ref());
    PathBuf::from(stripped)
}

/// Strips yt-dlp sandbox `__mediapm__` disambiguation marker from one path component.
#[must_use]
fn strip_yt_dlp_mediapm_marker_from_path_component(name: &str) -> String {
    name.replace(YT_DLP_MEDIAPM_SANDBOX_MARKER, "")
}

/// Normalizes one yt-dlp ZIP member path for hierarchy materialization.
#[must_use]
fn normalize_yt_dlp_sandbox_zip_member_path(path: &Path) -> PathBuf {
    let without_downloads = strip_yt_dlp_sandbox_downloads_prefix(path);
    let normalized = without_downloads.to_string_lossy().replace('\\', "/");
    if normalized.is_empty() {
        return PathBuf::new();
    }
    let stripped = normalized
        .split('/')
        .map(strip_yt_dlp_mediapm_marker_from_path_component)
        .collect::<Vec<_>>()
        .join("/");
    PathBuf::from(stripped)
}

impl SyncSharedState {
    #[expect(
        clippy::unused_self,
        reason = "method-shaped diagnostic helper; self kept for caller symmetry"
    )]
    fn notice(&self, message: impl Into<String>) {
        warn!("{}", message.into());
    }
}

// ---------------------------------------------------------------------------
// Tests
// ---------------------------------------------------------------------------

#[cfg(test)]
mod tests {
    use std::collections::BTreeMap;

    use super::*;
    use crate::config::hierarchy_types::{
        HierarchyNode, HierarchyNodeKind, HierarchyPath, PlaylistFormat, SanitizeNamesConfig,
    };
    use crate::config::source_types::{MediaSourceSpec, MediaStep, MediaStepTool};
    use crate::config::{GenericOutputVariantConfig, OutputVariantValue};
    use mediapm_utils::progress::recording::RecordingProgressTracker;
    use std::path::Path;

    #[test]
    fn normalize_yt_dlp_sandbox_zip_member_path_strips_downloads_and_mediapm_marker() {
        let path = Path::new("downloads/Rick Astley [dQw4w9WgXcQ]__mediapm__.url");
        assert_eq!(
            normalize_yt_dlp_sandbox_zip_member_path(path),
            PathBuf::from("Rick Astley [dQw4w9WgXcQ].url")
        );
    }

    #[test]
    fn normalize_yt_dlp_sandbox_zip_member_path_preserves_paths_without_marker() {
        let path = Path::new("downloads/subtitles/foo.en.vtt");
        assert_eq!(
            normalize_yt_dlp_sandbox_zip_member_path(path),
            PathBuf::from("subtitles/foo.en.vtt")
        );
    }

    #[test]
    fn normalize_yt_dlp_sandbox_zip_member_path_strips_marker_from_nested_components() {
        let path = Path::new("downloads/links/Rick Astley [dQw4w9WgXcQ]__mediapm__.desktop");
        assert_eq!(
            normalize_yt_dlp_sandbox_zip_member_path(path),
            PathBuf::from("links/Rick Astley [dQw4w9WgXcQ].desktop")
        );
    }

    #[test]
    fn is_protected_hierarchy_path_accepts_managed_descendants() {
        let current_paths = BTreeSet::from(["music videos/demo [id]/sidecars/links".to_string()]);
        let managed_paths = BTreeSet::from([
            "music videos/demo [id]/sidecars/links/Rick [dQw4w9WgXcQ].url".to_string(),
        ]);
        assert!(is_protected_hierarchy_path(
            "music videos/demo [id]/sidecars/links/Rick [dQw4w9WgXcQ].url",
            &current_paths,
            &managed_paths,
        ));
        assert!(is_protected_hierarchy_path(
            "music videos/demo [id]/Rick [id].link.url",
            &BTreeSet::from(["music videos/demo [id]".to_string()]),
            &BTreeSet::new(),
        ));
        assert!(!is_protected_hierarchy_path(
            "music videos/other [id]/stale.txt",
            &current_paths,
            &managed_paths,
        ));
    }

    /// Injected [`RecordingProgressTracker`] produces no ops when hierarchy is
    /// empty (early return before any progress bar work).
    #[tokio::test]
    async fn sync_hierarchy_with_empty_hierarchy_no_progress_ops() {
        let root = mediapm_utils::temp::artifact_dir().unwrap();
        let paths = MediaPmPaths::from_root(root.path());

        // Create a CAS at the runtime store path (needed for the CAS parameter,
        // though it's unused in the empty-hierarchy fast path).
        let cas_root = paths.runtime_root.join("store");
        tokio::fs::create_dir_all(&cas_root).await.unwrap();
        let cas = FileSystemCas::open(&cas_root).await.unwrap();

        let document = MediaPmDocument::default();
        let mut state = MediaPmState::default();
        let conductor_state = ConductorState::new_empty();
        let generated_doc = NickelDocument::default();

        let recording = RecordingProgressTracker::new();
        let result = sync_hierarchy(
            &paths,
            &document,
            &mut state,
            &cas,
            true,
            &conductor_state,
            &generated_doc,
            Some(Arc::new(recording.clone())),
        )
        .await;

        assert!(result.is_ok());
        let ops = recording.ops();
        assert!(ops.is_empty(), "empty hierarchy should produce no progress ops, got {ops:?}");
    }

    /// Injected [`RecordingProgressTracker`] records progress ops when
    /// hierarchy has one media entry (even when the source has no variant
    /// hashes — the entry is still processed and advance is called).
    #[tokio::test]
    async fn sync_hierarchy_with_single_media_produces_progress_ops() {
        let root = mediapm_utils::temp::artifact_dir().unwrap();
        let paths = MediaPmPaths::from_root(root.path());

        let cas_root = paths.runtime_root.join("store");
        tokio::fs::create_dir_all(&cas_root).await.unwrap();
        let cas = FileSystemCas::open(&cas_root).await.unwrap();

        let document = MediaPmDocument {
            media: BTreeMap::from([(
                "src1".into(),
                MediaSourceSpec {
                    steps: vec![MediaStep {
                        tool: MediaStepTool::Import,
                        input_variants: vec![],
                        output_variants: BTreeMap::from([(
                            "default".into(),
                            OutputVariantValue::Generic(GenericOutputVariantConfig {
                                kind: "primary".to_string(),
                                ..Default::default()
                            }),
                        )]),
                        options: BTreeMap::new(),
                    }],
                    ..MediaSourceSpec::default()
                },
            )]),
            hierarchy: vec![HierarchyNode {
                path: HierarchyPath::simple("test_file"),
                kind: HierarchyNodeKind::Media,
                id: None,
                media_id: Some("src1".into()),
                variant: Some("default".into()),
                variants: vec![],
                rename_files: vec![],
                format: PlaylistFormat::M3u8,
                ids: vec![],
                sanitize_names: SanitizeNamesConfig::Inherit,
                children: vec![],
            }],
            ..MediaPmDocument::default()
        };
        let mut state = MediaPmState::default();
        let conductor_state = ConductorState::new_empty();
        let generated_doc = NickelDocument::default();

        let recording = RecordingProgressTracker::new();
        let result = sync_hierarchy(
            &paths,
            &document,
            &mut state,
            &cas,
            true,
            &conductor_state,
            &generated_doc,
            Some(Arc::new(recording.clone())),
        )
        .await;

        assert!(result.is_ok(), "sync_hierarchy should succeed: {result:?}");
        let ops = recording.ops();
        assert!(!ops.is_empty(), "non-empty hierarchy should produce progress ops, got {ops:?}");
        // Expect: AddBar (materializing), Advance, FinishSuccess or FinishError
        assert!(
            ops.iter().any(|op| matches!(
                op,
                mediapm_utils::progress::recording::ProgressOp::AddBar { .. }
            )),
            "expected AddBar op: {ops:?}"
        );
        assert!(
            ops.iter().any(|op| matches!(
                op,
                mediapm_utils::progress::recording::ProgressOp::Advance { .. }
            )),
            "expected Advance op: {ops:?}"
        );
    }
}
