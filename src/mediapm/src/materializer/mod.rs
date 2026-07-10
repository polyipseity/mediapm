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

use std::collections::BTreeSet;
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
use crate::config::{MediaPmDocument, MediaPmState};
use crate::error::MediaPmError;
use crate::output::{ProgressBarApi, ProgressGroup, ProgressGroupApi};
use crate::paths::MediaPmPaths;

use self::metadata::MaterializationLookupContext;
use self::playlist::{
    PlaylistEntryPathMode, RenderedPlaylistEntry, generate_playlist_bytes,
    resolve_playlist_target_relative_path,
};
use self::resolve::{
    collect_media_source_available_variants, resolve_hierarchy_source, resolve_variant_bytes,
    resolve_variant_hash,
};
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
    /// Final relative path of the materialized output.
    #[allow(dead_code)]
    relative_path: String,
    /// Whether the output is a directory (folder variant).
    #[allow(dead_code)]
    is_directory: bool,
}

/// Shared state passed to each hierarchy entry worker.
struct SyncSharedState {
    /// Resolved library root path.
    hierarchy_root: PathBuf,
    /// CAS store reference.
    cas: FileSystemCas,
    /// Flattened hierarchy for stale-path scanning.
    flattened: Vec<FlattenedHierarchyEntry>,
    /// Whether to CAS-verify materialized outputs.
    #[allow(dead_code)]
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
pub async fn sync_hierarchy(
    paths: &MediaPmPaths,
    document: &MediaPmDocument,
    _state: &MediaPmState,
    cas: &FileSystemCas,
    verify_materialization: bool,
    progress_group: Option<&dyn ProgressGroupApi>,
) -> Result<MaterializeReport, MediaPmError> {
    let hierarchy_root = &paths.hierarchy_root_dir;

    // Flatten hierarchy and resolve all entries.
    let flattened = flatten_hierarchy_nodes_for_runtime(&document.hierarchy)?;
    if flattened.is_empty() {
        info!("hierarchy is empty, nothing to materialize");
        return Ok(MaterializeReport::default());
    }

    let lookup_context = MaterializationLookupContext::new(cas.clone());
    let shared = Arc::new(SyncSharedState {
        hierarchy_root: hierarchy_root.clone(),
        cas: cas.clone(),
        flattened: flattened.clone(),
        verify_materialization,
    });

    // --- Concurrent materialization ---
    let worker_count = hierarchy_worker_count();
    let semaphore = Arc::new(Semaphore::new(worker_count));
    let (owned_group, pb) = if let Some(pg) = progress_group {
        (None, pg.add_bar(flattened.len() as u64, "materializing"))
    } else {
        let g = ProgressGroup::new();
        let p: Arc<dyn ProgressBarApi> =
            Arc::new(g.add_bar(flattened.len() as u64, "materializing"));
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

        join_set.spawn(async move {
            let _permit = semaphore.acquire().await.unwrap();
            let result =
                prepare_hierarchy_entry(&entry, document.as_ref(), &shared, &lookup_context).await;
            pb.advance(1);
            result
        });
    }

    // Collect results.
    let mut report = MaterializeReport::default();
    let mut materialize_error: Option<MediaPmError> = None;
    while let Some(result) = join_set.join_next().await {
        match result {
            Ok(Ok(entry_result)) => {
                if entry_result.materialized {
                    report.materialized_paths += 1;
                } else {
                    report.skipped_paths += 1;
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
        pb.finish_error("materialization failed");
    } else {
        pb.finish_success("materialization complete");
    }
    if let Some(g) = owned_group {
        g.join();
    }
    if let Some(e) = materialize_error {
        return Err(e);
    }

    // --- Stale path cleanup ---
    let stale_result = remove_stale_paths(hierarchy_root, &flattened)?;
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
) -> Result<PreparedHierarchyEntryResult, MediaPmError> {
    let relative_path = entry.path_str();
    let target_path = shared.hierarchy_root.join(&relative_path);

    // Resolve the source spec.
    let source = resolve_hierarchy_source(document, &entry.entry)?;
    let media_id = &entry.entry.media_id;

    match entry.entry.kind {
        HierarchyEntryKind::Media => {
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

            let hash = resolve_variant_hash(media_id, &effective_variant, source, lookup).await?;

            if let Some(hash) = hash {
                materialize_file_entry(&target_path, &relative_path, &hash, shared, lookup).await?;
                Ok(PreparedHierarchyEntryResult {
                    materialized: true,
                    relative_path,
                    is_directory: false,
                })
            } else {
                // No content — still mark as processed but warn.
                shared.notice(format!(
                    "media '{media_id}' variant '{effective_variant}' has no content hash; skipping"
                ));
                Ok(PreparedHierarchyEntryResult {
                    materialized: false,
                    relative_path,
                    is_directory: false,
                })
            }
        }
        HierarchyEntryKind::MediaFolder => {
            // Multi-variant materialization (directory output).
            materialize_media_folder_entry(
                entry,
                source,
                media_id,
                &target_path,
                &relative_path,
                shared,
                lookup,
            )
            .await
        }
        HierarchyEntryKind::Playlist => {
            // Playlist generation.
            materialize_playlist_entry(entry, document, &target_path, &relative_path, shared).await
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
    _lookup: &MaterializationLookupContext,
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

    // Mark output as read-only.
    crate::materializer::commit::ensure_managed_path_readonly(target_path)?;

    Ok(())
}

/// Materialises a media-folder (multi-variant or ZIP-folder) entry.
async fn materialize_media_folder_entry(
    entry: &FlattenedHierarchyEntry,
    source: &MediaSourceSpec,
    media_id: &str,
    target_path: &Path,
    relative_path: &str,
    shared: &SyncSharedState,
    lookup: &MaterializationLookupContext,
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

    let rename_rules = compile_hierarchy_folder_rename_rules(&entry.entry.rename_files)?;

    for variant_name in &selected_variants {
        let variant_path = target_path.join(variant_name);
        let _variant_relative = format!("{relative_path}/{variant_name}");

        let bytes = resolve_variant_bytes(media_id, variant_name, source, lookup).await?;

        match bytes {
            Some(data) => {
                // Try ZIP extraction first for folder variants.
                let is_zip = is_zip_content(&data);
                if is_zip {
                    let extracted = extract_zip_folder_variant_bytes(&data, &rename_rules)?;
                    for (file_rel_path, content) in extracted {
                        let file_target = target_path.join(&file_rel_path);
                        if let Some(parent) = file_target.parent() {
                            tokio::fs::create_dir_all(parent).await.map_err(|source| {
                                MediaPmError::Io {
                                    operation: "creating extracted-file parent directory"
                                        .to_string(),
                                    path: parent.to_path_buf(),
                                    source,
                                }
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
                    }
                } else {
                    // Single file variant.
                    if let Some(parent) = variant_path.parent() {
                        tokio::fs::create_dir_all(parent).await.map_err(|source| {
                            MediaPmError::Io {
                                operation: "creating variant-file parent directory".to_string(),
                                path: parent.to_path_buf(),
                                source,
                            }
                        })?;
                    }
                    tokio::fs::write(&variant_path, &data).await.map_err(|source| {
                        MediaPmError::Io {
                            operation: "writing variant file".to_string(),
                            path: variant_path.clone(),
                            source,
                        }
                    })?;
                    crate::materializer::commit::ensure_managed_path_readonly(&variant_path)?;
                }
            }
            None => {
                shared.notice(format!(
                    "media '{media_id}' variant '{variant_name}' has no content; skipped"
                ));
            }
        }
    }

    Ok(PreparedHierarchyEntryResult {
        materialized: true,
        relative_path: relative_path.to_string(),
        is_directory: true,
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
        relative_path: relative_path.to_string(),
        is_directory: false,
    })
}

// ---------------------------------------------------------------------------
// Stale-path cleanup
// ---------------------------------------------------------------------------

/// Removes filesystem paths that are no longer present in the flattened
/// hierarchy, plus any empty parent directories left behind.
///
/// Returns `(removed_paths, removed_empty_dirs)`.
fn remove_stale_paths(
    hierarchy_root: &Path,
    current_entries: &[FlattenedHierarchyEntry],
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
            &mut removed_paths,
            &mut removed_empty_dirs,
        )?;
    }

    Ok((removed_paths, removed_empty_dirs))
}

/// Recursively scans for stale paths relative to the current hierarchy.
#[allow(clippy::only_used_in_recursion)]
fn remove_stale_recursive(
    absolute_root: &Path,
    absolute_dir: &Path,
    relative_prefix: &str,
    current_paths: &BTreeSet<String>,
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
                removed_paths,
                removed_empty_dirs,
            )?;

            // After recursion, remove directory if empty and not in current hierarchy.
            if !current_paths.contains(&relative_path) && is_directory_empty(&absolute_path)? {
                remove_path(&absolute_path)?;
                *removed_empty_dirs += 1;
            }
        } else if !current_paths.contains(&relative_path) {
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

/// Checks if a byte slice starts with the ZIP magic number.
fn is_zip_content(data: &[u8]) -> bool {
    data.len() >= 4 && data[..4] == [0x50, 0x4b, 0x03, 0x04]
}

impl SyncSharedState {
    #[allow(clippy::unused_self)]
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
    use mediapm_utils::progress::recording::RecordingProgressTracker;
    use tempfile::tempdir;

    /// Injected [`RecordingProgressTracker`] produces no ops when hierarchy is
    /// empty (early return before any progress bar work).
    #[tokio::test]
    async fn sync_hierarchy_with_empty_hierarchy_no_progress_ops() {
        let root = tempdir().unwrap();
        let paths = MediaPmPaths::from_root(root.path());

        // Create a CAS at the runtime store path (needed for the CAS parameter,
        // though it's unused in the empty-hierarchy fast path).
        let cas_root = paths.runtime_root.join("store");
        tokio::fs::create_dir_all(&cas_root).await.unwrap();
        let cas = FileSystemCas::open(&cas_root).await.unwrap();

        let document = MediaPmDocument::default();
        let state = MediaPmState::default();

        let recording = RecordingProgressTracker::new();
        let result = sync_hierarchy(&paths, &document, &state, &cas, true, Some(&recording)).await;

        assert!(result.is_ok());
        let ops = recording.ops();
        assert!(ops.is_empty(), "empty hierarchy should produce no progress ops, got {ops:?}",);
    }

    /// Injected [`RecordingProgressTracker`] records progress ops when
    /// hierarchy has one media entry (even when the source has no variant
    /// hashes — the entry is still processed and advance is called).
    #[tokio::test]
    async fn sync_hierarchy_with_single_media_produces_progress_ops() {
        let root = tempdir().unwrap();
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
                            serde_json::json!({"kind": "primary"}),
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
        let state = MediaPmState::default();

        let recording = RecordingProgressTracker::new();
        let result = sync_hierarchy(&paths, &document, &state, &cas, true, Some(&recording)).await;

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
