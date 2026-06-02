//! Atomic staging + commit materializer for mediapm hierarchy sync.
//!
//! The materializer enforces path invariants, stages all outputs under
//! the resolved runtime staging directory, and then commits with atomic rename
//! operations into the resolved library directory.

use std::collections::{BTreeMap, BTreeSet, VecDeque};
use std::fs;
use std::path::{Path, PathBuf};
use std::sync::Arc;
use std::time::Duration;

use mediapm_cas::{CasApi, FileSystemCas, Hash};
use mediapm_conductor::{MachineNickelDocument, OrchestrationState};
use pulsebar::{MultiProgress, ProgressBar};
use regex::Regex;

use crate::conductor_bridge::resolve_ffmpeg_slot_limits;
use crate::config::{
    FlattenedHierarchyEntry, HierarchyEntryKind, MediaPmDocument, PlaylistEntryPathMode,
    expand_variant_selectors, flatten_hierarchy_nodes_for_runtime,
};
use crate::error::MediaPmError;
use crate::lockfile::{ManagedFileRecord, MediaLockFile};
use crate::paths::MediaPmPaths;

mod commit;
mod file_ops;
mod metadata;
mod playlist;
mod resolve;
#[cfg(test)]
mod tests;
mod zip;

use self::commit::{
    commit_staged_output, now_unix_seconds, remove_path, sanitize_hierarchy_path,
    unix_epoch_millis, validate_hierarchy_path,
};
use self::file_ops::materialize_file_from_cas_with_order;
use self::metadata::{
    resolve_hierarchy_folder_rename_rule_replacements, resolve_hierarchy_relative_path,
    resolve_managed_ffprobe_path,
};
use self::playlist::{
    collect_media_file_hierarchy_templates, collect_playlist_media_index, join_relative_paths,
    normalize_resolved_hierarchy_path_to_nfd, playlist_format_label, render_absolute_playlist_path,
    render_playlist_bytes, render_relative_playlist_path,
    resolve_playlist_media_target_relative_path,
};
use self::resolve::{
    collect_media_source_available_variants, load_runtime_orchestration_state,
    resolve_hierarchy_source, resolve_variant_source_bytes, resolve_variant_source_hash,
};
#[cfg(test)]
use self::resolve::{instance_matches_expected_inputs, resolve_workflow_step_output_hashes};
#[cfg(test)]
use self::zip::{apply_hierarchy_folder_rename_rules, register_zip_file_entry};
use self::zip::{compile_hierarchy_folder_rename_rules, extract_zip_folder_variant_bytes};

/// Upper bound for concurrent hierarchy staging workers.
const HIERARCHY_STAGE_MAX_CONCURRENCY: usize = 8;

/// Maximum number of Unicode scalar values shown in one progress filename.
const HIERARCHY_PROGRESS_MAX_FILENAME_CHARS: usize = 48;

/// One prepared hierarchy-entry staging result.
#[derive(Debug)]
struct PreparedHierarchyEntryResult {
    /// Flat hierarchy entry path template after placeholder resolution.
    relative_path: String,
    /// Entry kind used during final commit policy selection.
    entry_kind: HierarchyEntryKind,
    /// Staged output path prepared for commit.
    staged_path: PathBuf,
    /// Final destination path used for commit.
    final_path: PathBuf,
    /// Managed media id to persist in lock records when materialized.
    managed_media_id: Option<String>,
    /// Managed variant table keyed by materialized relative path.
    managed_file_variants: BTreeMap<String, String>,
    /// Managed CAS hash table keyed by materialized relative path.
    managed_file_hashes: BTreeMap<String, Hash>,
    /// Desired managed paths produced when one entry is skipped.
    skipped_paths: Vec<String>,
    /// Paths whose lock timestamps should be refreshed on skip.
    refreshed_lock_paths: Vec<String>,
    /// Worker notices collected while preparing this entry.
    notices: Vec<String>,
    /// Whether this entry was skipped by hash-change detection.
    skipped_entry: bool,
}

/// Summary of one materialization pass.
#[derive(Debug, Clone, PartialEq, Eq, Default)]
pub struct MaterializeReport {
    /// Number of hierarchy entries staged and committed.
    pub materialized_paths: usize,
    /// Number of hierarchy entries whose CAS hash matched the lock record and
    /// whose final output path was confirmed present on disk — skipped.
    pub skipped_paths: usize,
    /// Number of previously managed outputs removed as stale.
    pub removed_paths: usize,
    /// Number of empty parent directories removed after stale path cleanup.
    pub removed_empty_dirs: usize,
    /// Link/copy fallback notices captured during materialization.
    pub notices: Vec<String>,
}

/// Per-workflow step output hash table (`step_id -> output_name -> CAS hash`).
type StepOutputHashes = BTreeMap<String, BTreeMap<String, Hash>>;

/// Per-workflow required step output names (`step_id -> output_name[]`).
type RequiredStepOutputNames = BTreeMap<String, BTreeSet<String>>;

/// Per-workflow required ZIP member selectors (`step_id -> output_name -> zip_member[]`).
type RequiredStepZipMembers = BTreeMap<String, BTreeMap<String, BTreeSet<String>>>;

/// Per-step expected inputs used to match runtime workflow instances.
#[derive(Debug, Clone, PartialEq, Eq, Default)]
struct ExpectedStepInputs {
    /// Deterministically resolved input hashes.
    resolved_hashes: BTreeMap<String, Hash>,
    /// Input names whose hashes cannot be reconstructed from persisted CAS,
    /// but that must still exist on candidate runtime instances.
    unresolved_hash_input_names: BTreeSet<String>,
}

/// One input-binding hash resolution result.
enum InputBindingHashResolution {
    /// Fully reconstructed deterministic input hash.
    Resolved(Hash),
    /// Referenced prior step output is unavailable in the current traversal
    /// order, so this step cannot be matched yet.
    MissingPriorStepOutput,
    /// Referenced step output exists in state but cannot be reconstructed from
    /// persisted CAS bytes (for example sandbox artifacts only available in
    /// ephemeral/in-memory execution context).
    MissingMaterializedStepOutput,
}

/// One compiled hierarchy rename rule used during ZIP folder extraction.
#[derive(Debug, Clone)]
struct CompiledHierarchyFolderRenameRule {
    /// Original configured regex pattern (for diagnostics).
    pattern: String,
    /// Replacement text applied when `pattern` matches.
    replacement: String,
    /// Precompiled regex for efficient per-entry application.
    regex: Regex,
}

/// Shared lookup context for resolving materialization-time variant payloads.
///
/// The materializer repeatedly resolves variant bytes from either local CAS
/// pointers or managed workflow outputs. This context groups immutable lookup
/// dependencies so helper signatures remain compact and consistent.
#[derive(Clone)]
struct MaterializationLookupContext {
    /// Conductor CAS store used for payload reads.
    cas: Arc<FileSystemCas>,
    /// Resolved conductor machine document for tool/workflow metadata.
    machine: Arc<MachineNickelDocument>,
    /// Optional persisted orchestration state loaded from runtime pointer.
    orchestration_state: Option<Arc<OrchestrationState>>,
    /// Effective ffmpeg input-slot limit used for output-binding resolution.
    ffmpeg_max_input_slots: usize,
    /// Effective ffmpeg output-slot limit used for output-binding resolution.
    ffmpeg_max_output_slots: usize,
    /// Host-resolved managed ffprobe path derived from active managed ffmpeg.
    managed_ffprobe_path: Option<PathBuf>,
}

/// Resolved payload bytes for one materialized variant request.
#[derive(Debug, Clone, PartialEq, Eq)]
struct VariantSourceBytes {
    /// Bytes to stage for the requested variant.
    bytes: Vec<u8>,
    /// Optional fallback notice (for example variant-default fallback).
    notice: Option<String>,
    /// Optional direct source hash when staged bytes exactly match one
    /// existing CAS object (no derived ZIP-member extraction).
    source_hash: Option<Hash>,
}

/// One rendered playlist row with source identity and emitted path text.
#[derive(Debug, Clone, PartialEq, Eq)]
struct RenderedPlaylistItem {
    /// Referenced hierarchy id used for diagnostics and title fields.
    id: String,
    /// Rendered path written to playlist payload.
    path: String,
}

/// Computes bounded hierarchy staging worker parallelism.
#[must_use]
fn hierarchy_stage_worker_count(total_entries: usize) -> usize {
    if total_entries == 0 {
        return 1;
    }

    let cpu_hint = std::thread::available_parallelism().map_or(1, std::num::NonZeroUsize::get);
    total_entries.min(cpu_hint).clamp(1, HIERARCHY_STAGE_MAX_CONCURRENCY)
}

/// Formats one hierarchy entry kind for progress-row messages.
#[must_use]
fn hierarchy_entry_kind_label(kind: HierarchyEntryKind) -> &'static str {
    match kind {
        HierarchyEntryKind::Media => "media",
        HierarchyEntryKind::MediaFolder => "media_folder",
        HierarchyEntryKind::Playlist => "playlist",
    }
}

/// Returns the basename-oriented hierarchy label shown in worker progress.
#[must_use]
fn hierarchy_progress_filename_label(path: &str) -> String {
    let trimmed = path.trim_end_matches(['/', '\\']);
    let candidate = if trimmed.is_empty() { path } else { trimmed };
    let file_name = Path::new(candidate)
        .file_name()
        .and_then(|name| name.to_str())
        .filter(|name| !name.trim().is_empty())
        .unwrap_or(candidate);
    truncate_progress_label(file_name, HIERARCHY_PROGRESS_MAX_FILENAME_CHARS)
}

/// Truncates one progress label to a bounded character length with ellipsis.
#[must_use]
fn truncate_progress_label(value: &str, max_chars: usize) -> String {
    if max_chars == 0 {
        return String::new();
    }

    let char_count = value.chars().count();
    if char_count <= max_chars {
        return value.to_string();
    }

    if max_chars <= 1 {
        return "…".to_string();
    }

    let prefix = value.chars().take(max_chars - 1).collect::<String>();
    format!("{prefix}…")
}

/// Prepares one hierarchy entry in the staging root without final commit.
#[expect(
    clippy::too_many_lines,
    reason = "this helper intentionally keeps per-entry staging behavior unified for deterministic worker execution"
)]
#[expect(
    clippy::too_many_arguments,
    reason = "per-entry staging needs explicit immutable inputs to keep worker behavior deterministic"
)]
async fn prepare_hierarchy_entry(
    paths: &MediaPmPaths,
    document: &MediaPmDocument,
    lock: &MediaLockFile,
    lookup: &MaterializationLookupContext,
    materialization_methods: &[crate::config::MaterializationMethod],
    staging_root: &Path,
    playlist_media_index: &BTreeMap<String, String>,
    media_file_templates: &BTreeMap<String, crate::config::HierarchyEntry>,
    flattened_entry: &FlattenedHierarchyEntry,
    job_index: usize,
    progress_bar: &ProgressBar,
) -> Result<PreparedHierarchyEntryResult, MediaPmError> {
    let relative_path_template = flattened_entry.path.as_str();
    let entry = &flattened_entry.entry;

    progress_bar.set_position(0);
    progress_bar
        .set_message(&format!("{}: resolving filename", hierarchy_entry_kind_label(entry.kind)));

    let relative_path =
        if matches!(entry.kind, HierarchyEntryKind::Media | HierarchyEntryKind::MediaFolder) {
            let source = resolve_hierarchy_source(document, entry)?;
            resolve_hierarchy_relative_path(relative_path_template, entry, source, lookup).await?
        } else {
            relative_path_template.to_string()
        };
    let mut relative_path = normalize_resolved_hierarchy_path_to_nfd(&relative_path);
    if entry.sanitize_names.is_enabled() {
        let runtime_replacements = document.runtime.path_sanitization_mapping_with_defaults()?;
        let effective_replacements =
            entry.sanitize_names.replacement_map_with_defaults(&runtime_replacements);
        relative_path = sanitize_hierarchy_path(&relative_path, &effective_replacements);
    }
    validate_hierarchy_path(&relative_path)?;
    progress_bar.set_message(&format!(
        "{}: {}",
        hierarchy_entry_kind_label(entry.kind),
        hierarchy_progress_filename_label(&relative_path)
    ));
    let fs_relative_path = relative_path.as_str();

    if fs_relative_path.is_empty() {
        return Err(MediaPmError::Workflow(format!(
            "hierarchy path '{relative_path}' must not resolve to an empty filesystem path"
        )));
    }

    // MediaFolder entries must each have their own isolated staging directory.
    //
    // Two or more MediaFolder entries may resolve to the same final path (for
    // example two `path=""` nodes materializing thumbnails and links into the
    // same media-root folder). If their staged paths shared a common prefix, the
    // first commit's `merge_staged_directory_into_existing` call would
    // recursively consume and remove nested staging directories that still
    // belong to sibling entries, leaving those siblings with a missing staged
    // path on their own commit.
    //
    // Using a flat `staging_root/{job_index}/` directory for every MediaFolder
    // entry guarantees isolation: no commit can ever traverse into another
    // entry's staging area.
    let staged_path = if matches!(entry.kind, HierarchyEntryKind::MediaFolder) {
        staging_root.join(job_index.to_string())
    } else {
        staging_root.join(fs_relative_path)
    };
    let final_path = paths.hierarchy_root_dir.join(fs_relative_path);
    progress_bar.set_position(10);

    let mut notices = Vec::new();
    let mut skipped_paths = Vec::new();
    let mut refreshed_lock_paths = Vec::new();
    let (managed_media_id, managed_file_variants, managed_file_hashes, skipped_entry) = match entry
        .kind
    {
        HierarchyEntryKind::Media => {
            let source = resolve_hierarchy_source(document, entry)?;

            if entry.variants.is_empty() {
                return Err(MediaPmError::Workflow(format!(
                    "hierarchy path '{relative_path}' must define at least one variant"
                )));
            }

            let available_variants = collect_media_source_available_variants(source);
            let resolved_variants = expand_variant_selectors(&entry.variants, &available_variants)
                .map_err(|reason| {
                    MediaPmError::Workflow(format!(
                        "hierarchy path '{relative_path}' {reason} for media '{}'",
                        entry.media_id
                    ))
                })?;

            if resolved_variants.len() != 1 {
                return Err(MediaPmError::Workflow(format!(
                    "hierarchy file path '{relative_path}' must resolve exactly one variant"
                )));
            }
            let variant = resolved_variants
                .first()
                .expect("checked non-empty and len==1 for hierarchy file path");

            if let Some(hint_hash) =
                resolve_variant_source_hash(lookup, &entry.media_id, source, variant).await?
            {
                let hint_hash_str = hint_hash.to_string();
                if lock.managed_files.get(&relative_path).is_some_and(|r| r.hash == hint_hash_str)
                    && fs::symlink_metadata(&final_path).is_ok()
                {
                    skipped_paths.push(relative_path.clone());
                    refreshed_lock_paths.push(relative_path.clone());
                    progress_bar.set_position(100);
                    return Ok(PreparedHierarchyEntryResult {
                        relative_path,
                        entry_kind: entry.kind,
                        staged_path,
                        final_path,
                        managed_media_id: None,
                        managed_file_variants: BTreeMap::new(),
                        managed_file_hashes: BTreeMap::new(),
                        skipped_paths,
                        refreshed_lock_paths,
                        notices,
                        skipped_entry: true,
                    });
                }
            }

            if let Some(parent) = staged_path.parent() {
                tokio::fs::create_dir_all(parent).await.map_err(|source_err| MediaPmError::Io {
                    operation: "creating staged parent directory".to_string(),
                    path: parent.to_path_buf(),
                    source: source_err,
                })?;
            }

            progress_bar.set_position(35);
            let variant_source =
                resolve_variant_source_bytes(lookup, &entry.media_id, source, variant).await?;
            if let Some(message) = variant_source.notice.as_deref() {
                notices.push(message.to_string());
            }

            let file_hash = if let Some(source_hash) = variant_source.source_hash {
                source_hash
            } else {
                lookup.cas.put(variant_source.bytes).await.map_err(|source| {
                    MediaPmError::Workflow(format!(
                        "importing materialized file '{relative_path}' into CAS failed: {source}",
                    ))
                })?
            };

            progress_bar.set_position(70);
            materialize_file_from_cas_with_order(
                &lookup.cas,
                file_hash,
                &staged_path,
                relative_path.as_str(),
                materialization_methods,
                &mut notices,
            )
            .await?;

            (
                Some(entry.media_id.clone()),
                BTreeMap::from([(relative_path.clone(), variant.clone())]),
                BTreeMap::from([(relative_path.clone(), file_hash)]),
                false,
            )
        }
        HierarchyEntryKind::MediaFolder => {
            let source = resolve_hierarchy_source(document, entry)?;

            if entry.variants.is_empty() {
                return Err(MediaPmError::Workflow(format!(
                    "hierarchy path '{relative_path}' must define at least one variant"
                )));
            }

            let available_variants = collect_media_source_available_variants(source);
            let resolved_variants = expand_variant_selectors(&entry.variants, &available_variants)
                .map_err(|reason| {
                    MediaPmError::Workflow(format!(
                        "hierarchy path '{relative_path}' {reason} for media '{}'",
                        entry.media_id
                    ))
                })?;

            tokio::fs::create_dir_all(&staged_path).await.map_err(|source_err| {
                MediaPmError::Io {
                    operation: "creating staged output directory".to_string(),
                    path: staged_path.clone(),
                    source: source_err,
                }
            })?;

            let rename_replacements = if entry.sanitize_names.is_enabled() {
                let runtime_replacements =
                    document.runtime.path_sanitization_mapping_with_defaults()?;
                entry.sanitize_names.replacement_map_with_defaults(&runtime_replacements)
            } else {
                BTreeMap::new()
            };

            let resolved_rename_rules = resolve_hierarchy_folder_rename_rule_replacements(
                &entry.rename_files,
                &relative_path,
                entry,
                source,
                lookup,
                &rename_replacements,
            )
            .await?;
            let compiled_rename_rules = compile_hierarchy_folder_rename_rules(
                &resolved_rename_rules,
                &relative_path,
                &entry.media_id,
            )?;

            progress_bar.set_position(30);
            let mut extracted_entries = BTreeMap::new();
            let mut extracted_entry_variants = BTreeMap::<String, String>::new();
            for variant in &resolved_variants {
                let variant_source =
                    resolve_variant_source_bytes(lookup, &entry.media_id, source, variant).await?;
                if let Some(message) = variant_source.notice.as_deref() {
                    notices.push(message.to_string());
                }

                extract_zip_folder_variant_bytes(
                    variant_source.bytes.as_slice(),
                    &staged_path,
                    &relative_path,
                    &entry.media_id,
                    variant,
                    &compiled_rename_rules,
                    &mut extracted_entries,
                    &mut extracted_entry_variants,
                )?;
            }

            let mut managed_file_hashes = BTreeMap::new();
            let mut managed_file_variants = BTreeMap::new();
            for (entry_path, is_directory) in &extracted_entries {
                if *is_directory {
                    continue;
                }

                let managed_path = join_relative_paths(fs_relative_path, entry_path);
                let staged_file_path = staged_path.join(entry_path);
                let staged_bytes =
                    tokio::fs::read(&staged_file_path).await.map_err(|source_err| {
                        MediaPmError::Io {
                            operation: "reading extracted staged file bytes for CAS import"
                                .to_string(),
                            path: staged_file_path.clone(),
                            source: source_err,
                        }
                    })?;
                let staged_hash = lookup.cas.put(staged_bytes).await.map_err(|source| {
                        MediaPmError::Workflow(format!(
                            "importing materialized folder member '{managed_path}' into CAS failed: {source}",
                        ))
                    })?;
                materialize_file_from_cas_with_order(
                    &lookup.cas,
                    staged_hash,
                    &staged_file_path,
                    &managed_path,
                    materialization_methods,
                    &mut notices,
                )
                .await?;
                managed_file_hashes.insert(managed_path, staged_hash);

                let entry_variant = extracted_entry_variants
                        .get(entry_path)
                        .cloned()
                        .ok_or_else(|| {
                            MediaPmError::Workflow(format!(
                                "missing extracted variant provenance for hierarchy path '{relative_path}' media '{}' extracted file '{entry_path}'",
                                entry.media_id
                            ))
                        })?;
                managed_file_variants
                    .insert(join_relative_paths(fs_relative_path, entry_path), entry_variant);
            }

            (Some(entry.media_id.clone()), managed_file_variants, managed_file_hashes, false)
        }
        HierarchyEntryKind::Playlist => {
            if relative_path.ends_with('/') || relative_path.ends_with('\\') {
                return Err(MediaPmError::Workflow(format!(
                    "hierarchy playlist path '{relative_path}' must be a file path"
                )));
            }
            if entry.ids.is_empty() {
                return Err(MediaPmError::Workflow(format!(
                    "hierarchy playlist path '{relative_path}' must define at least one playlist id"
                )));
            }

            let mut resolved_playlist_media_targets = BTreeMap::<String, String>::new();
            let mut rendered_items = Vec::with_capacity(entry.ids.len());
            for (item_index, item) in entry.ids.iter().enumerate() {
                let requested_id = item.id().trim();
                if requested_id.is_empty() {
                    return Err(MediaPmError::Workflow(format!(
                        "hierarchy playlist path '{relative_path}' ids[{item_index}] has empty id"
                    )));
                }

                let media_path_template = playlist_media_index.get(requested_id).ok_or_else(|| {
                        MediaPmError::Workflow(format!(
                            "hierarchy playlist path '{relative_path}' ids[{item_index}] references unknown hierarchy id '{requested_id}'"
                        ))
                    })?;

                let target_relative = resolve_playlist_media_target_relative_path(
                    document,
                    lookup,
                    media_path_template,
                    media_file_templates,
                    &mut resolved_playlist_media_targets,
                )
                .await?;

                let rendered_path = match item.path_mode() {
                    PlaylistEntryPathMode::Relative => {
                        render_relative_playlist_path(&relative_path, &target_relative)
                    }
                    PlaylistEntryPathMode::Absolute => {
                        render_absolute_playlist_path(paths, &target_relative)
                    }
                };

                rendered_items.push(RenderedPlaylistItem {
                    id: requested_id.to_string(),
                    path: rendered_path,
                });
            }

            if let Some(parent) = staged_path.parent() {
                tokio::fs::create_dir_all(parent).await.map_err(|source_err| MediaPmError::Io {
                    operation: "creating staged parent directory".to_string(),
                    path: parent.to_path_buf(),
                    source: source_err,
                })?;
            }

            progress_bar.set_position(70);
            let playlist_bytes = render_playlist_bytes(entry.format, &rendered_items);
            let playlist_hash = lookup.cas.put(playlist_bytes).await.map_err(|source| {
                MediaPmError::Workflow(format!(
                    "importing generated playlist '{relative_path}' into CAS failed: {source}",
                ))
            })?;
            materialize_file_from_cas_with_order(
                &lookup.cas,
                playlist_hash,
                &staged_path,
                relative_path.as_str(),
                materialization_methods,
                &mut notices,
            )
            .await?;

            (
                Some("playlist".to_string()),
                BTreeMap::from([(
                    relative_path.clone(),
                    format!("playlist:{}", playlist_format_label(entry.format)),
                )]),
                BTreeMap::from([(relative_path.clone(), playlist_hash)]),
                false,
            )
        }
    };

    progress_bar.set_position(100);
    Ok(PreparedHierarchyEntryResult {
        relative_path,
        entry_kind: entry.kind,
        staged_path,
        final_path,
        managed_media_id,
        managed_file_variants,
        managed_file_hashes,
        skipped_paths,
        refreshed_lock_paths,
        notices,
        skipped_entry,
    })
}

/// Synchronizes desired hierarchy entries using stage-verify-commit flow.
#[expect(
    clippy::too_many_lines,
    reason = "this item intentionally keeps end-to-end control flow together so ordering invariants remain explicit during maintenance"
)]
pub async fn sync_hierarchy(
    paths: &MediaPmPaths,
    document: &MediaPmDocument,
    machine: &MachineNickelDocument,
    conductor_cas_root: &Path,
    lock: &mut MediaLockFile,
) -> Result<MaterializeReport, MediaPmError> {
    let ffmpeg_slot_limits = resolve_ffmpeg_slot_limits(&document.tools)?;
    let ffmpeg_max_input_slots = ffmpeg_slot_limits.max_input_slots;
    let ffmpeg_max_output_slots = ffmpeg_slot_limits.max_output_slots;
    let materialization_methods = document.runtime.materialization_preference_order_with_defaults();

    fs::create_dir_all(&paths.mediapm_tmp_dir).map_err(|source| MediaPmError::Io {
        operation: "creating mediapm runtime temporary directory".to_string(),
        path: paths.mediapm_tmp_dir.clone(),
        source,
    })?;
    tokio::fs::create_dir_all(&paths.hierarchy_root_dir).await.map_err(|source| {
        MediaPmError::Io {
            operation: "creating resolved library directory".to_string(),
            path: paths.hierarchy_root_dir.clone(),
            source,
        }
    })?;

    let cas = Arc::new(FileSystemCas::open(conductor_cas_root).await.map_err(|source| {
        MediaPmError::Workflow(format!(
            "opening conductor CAS store '{}' for materialization failed: {source}",
            conductor_cas_root.display()
        ))
    })?);
    let orchestration_state = load_runtime_orchestration_state(paths, &cas).await?.map(Arc::new);
    let managed_ffprobe_path = resolve_managed_ffprobe_path(paths, machine, lock);
    let lookup = MaterializationLookupContext {
        cas: Arc::clone(&cas),
        machine: Arc::new(machine.clone()),
        orchestration_state,
        ffmpeg_max_input_slots,
        ffmpeg_max_output_slots,
        managed_ffprobe_path,
    };

    let staging_root = paths.mediapm_tmp_dir.join(format!("sync-{}", now_unix_seconds()));
    tokio::fs::create_dir_all(&staging_root).await.map_err(|source| MediaPmError::Io {
        operation: "creating sync staging directory".to_string(),
        path: staging_root.clone(),
        source,
    })?;

    let mut report = MaterializeReport::default();
    let mut desired_paths = BTreeSet::new();
    let flattened_hierarchy = flatten_hierarchy_nodes_for_runtime(&document.hierarchy)?;
    let playlist_media_index = collect_playlist_media_index(&flattened_hierarchy)?;
    let media_file_templates = collect_media_file_hierarchy_templates(&flattened_hierarchy)?;
    let worker_count = hierarchy_stage_worker_count(flattened_hierarchy.len());

    let total_entries = flattened_hierarchy.len();
    let multi = MultiProgress::new();
    let hierarchy_progress = multi
        .add_bar(total_entries.max(1) as u64)
        .with_message(&format!("syncing hierarchy ({worker_count} concurrent workers)"))
        .with_format("{msg}  {bar}  {pos}/{total}  {elapsed}");
    hierarchy_progress.set_position(0);
    let operation_bars = (0..worker_count)
        .map(|worker_index| {
            multi
                .add_bar(100)
                .with_message(&format!("worker#{worker_index}: queued"))
                .with_format("{msg}  [{bar:18}]  {pct}  {elapsed}")
        })
        .collect::<Vec<_>>();

    let shared_jobs = Arc::new(tokio::sync::Mutex::new(
        flattened_hierarchy.into_iter().enumerate().collect::<VecDeque<_>>(),
    ));
    let (result_sender, mut result_receiver) = tokio::sync::mpsc::unbounded_channel::<(
        usize,
        Result<PreparedHierarchyEntryResult, MediaPmError>,
    )>();

    let shared_paths = Arc::new(paths.clone());
    let shared_document = Arc::new(document.clone());
    let shared_lookup = Arc::new(lookup);
    let shared_materialization_methods = Arc::new(materialization_methods);
    let shared_staging_root = Arc::new(staging_root.clone());
    let shared_playlist_media_index = Arc::new(playlist_media_index);
    let shared_media_file_templates = Arc::new(media_file_templates);
    let shared_lock_snapshot = Arc::new(lock.clone());

    let mut worker_handles = Vec::with_capacity(worker_count);
    for (worker_index, progress_bar) in operation_bars.iter().enumerate() {
        let jobs = Arc::clone(&shared_jobs);
        let sender = result_sender.clone();
        let worker_paths = Arc::clone(&shared_paths);
        let worker_document = Arc::clone(&shared_document);
        let worker_lookup = Arc::clone(&shared_lookup);
        let worker_materialization_methods = Arc::clone(&shared_materialization_methods);
        let worker_staging_root = Arc::clone(&shared_staging_root);
        let worker_playlist_media_index = Arc::clone(&shared_playlist_media_index);
        let worker_media_file_templates = Arc::clone(&shared_media_file_templates);
        let worker_lock_snapshot = Arc::clone(&shared_lock_snapshot);
        let worker_bar = progress_bar.clone();

        worker_handles.push(tokio::spawn(async move {
            loop {
                let next_job = {
                    let mut queue = jobs.lock().await;
                    queue.pop_front()
                };

                let Some((job_index, flattened_entry)) = next_job else {
                    break;
                };

                worker_bar.set_position(0);
                worker_bar.set_message(&format!(
                    "worker#{worker_index}: {}",
                    hierarchy_entry_kind_label(flattened_entry.entry.kind)
                ));

                let prepared = prepare_hierarchy_entry(
                    worker_paths.as_ref(),
                    worker_document.as_ref(),
                    worker_lock_snapshot.as_ref(),
                    worker_lookup.as_ref(),
                    worker_materialization_methods.as_ref(),
                    worker_staging_root.as_ref(),
                    worker_playlist_media_index.as_ref(),
                    worker_media_file_templates.as_ref(),
                    &flattened_entry,
                    job_index,
                    &worker_bar,
                )
                .await;

                if prepared.is_err() {
                    worker_bar.set_position(100);
                    worker_bar.set_message(&format!(
                        "worker#{worker_index}: failed {}",
                        hierarchy_entry_kind_label(flattened_entry.entry.kind)
                    ));
                } else if let Ok(ref prepared_entry) = prepared {
                    worker_bar.set_message(&format!(
                        "worker#{worker_index}: {}",
                        hierarchy_progress_filename_label(&prepared_entry.relative_path)
                    ));
                }

                let _ = sender.send((job_index, prepared));
            }

            worker_bar.finish_success(&format!("worker#{worker_index}: done"));
        }));
    }
    drop(result_sender);

    let mut prepared_results = (0..total_entries).map(|_| None).collect::<Vec<_>>();
    let mut first_prepare_error: Option<MediaPmError> = None;
    let mut completed_entries = 0usize;

    while completed_entries < total_entries {
        let Some((entry_index, prepared_result)) = result_receiver.recv().await else {
            break;
        };

        hierarchy_progress.advance(1);
        completed_entries += 1;
        hierarchy_progress.set_message(&format!(
            "syncing hierarchy ({worker_count} concurrent workers, prepared {completed_entries}/{total_entries})"
        ));

        match prepared_result {
            Ok(prepared) => {
                prepared_results[entry_index] = Some(prepared);
            }
            Err(error) => {
                if first_prepare_error.is_none() {
                    first_prepare_error = Some(error);
                }
            }
        }
    }

    for handle in worker_handles {
        handle
            .await
            .map_err(|e| MediaPmError::Workflow(format!("hierarchy worker task panicked: {e}")))?;
    }

    if let Some(error) = first_prepare_error {
        return Err(error);
    }

    for prepared in prepared_results {
        let prepared = prepared.ok_or_else(|| {
            MediaPmError::Workflow(
                "hierarchy worker channel closed before all entries were prepared".to_string(),
            )
        })?;

        report.notices.extend(prepared.notices);
        desired_paths.extend(prepared.skipped_paths.iter().cloned());
        desired_paths.extend(prepared.managed_file_hashes.keys().cloned());

        if prepared.skipped_entry {
            for managed_path in &prepared.refreshed_lock_paths {
                if let Some(record) = lock.managed_files.get_mut(managed_path) {
                    record.last_synced_unix_millis = unix_epoch_millis();
                }
            }
            report.skipped_paths += 1;
            continue;
        }

        if let Some(parent) = prepared.final_path.parent() {
            tokio::fs::create_dir_all(parent).await.map_err(|source_err| MediaPmError::Io {
                operation: "creating final output parent directory".to_string(),
                path: parent.to_path_buf(),
                source: source_err,
            })?;
        }

        let staged_commit = prepared.staged_path.clone();
        let final_commit = prepared.final_path.clone();
        let entry_kind = prepared.entry_kind;
        tokio::task::spawn_blocking(move || {
            commit_staged_output(&staged_commit, &final_commit, entry_kind)
        })
        .await
        .map_err(|e| {
            MediaPmError::Workflow(format!("commit staged output task panicked: {e}"))
        })??;

        let managed_media_id = prepared.managed_media_id.ok_or_else(|| {
            MediaPmError::Workflow(format!(
                "missing managed media id for prepared hierarchy path '{}'",
                prepared.relative_path
            ))
        })?;

        for (managed_file_path, managed_hash) in prepared.managed_file_hashes {
            let managed_variant = prepared
                .managed_file_variants
                .get(&managed_file_path)
                .cloned()
                .ok_or_else(|| {
                MediaPmError::Workflow(format!(
                    "missing managed variant metadata for materialized path '{managed_file_path}'"
                ))
            })?;

            lock.managed_files.insert(
                managed_file_path,
                ManagedFileRecord {
                    media_id: managed_media_id.clone(),
                    variant: managed_variant,
                    hash: managed_hash.to_string(),
                    last_synced_unix_millis: unix_epoch_millis(),
                },
            );
        }

        report.materialized_paths += 1;
    }

    let stale_paths = lock
        .managed_files
        .keys()
        .filter(|path| !desired_paths.contains(*path))
        .cloned()
        .collect::<Vec<_>>();

    for stale in &stale_paths {
        if stale.ends_with('/') || stale.ends_with('\\') {
            // Legacy lock rows from historical directory-level tracking should
            // not remove whole directories once file-level tracking is active.
            lock.managed_files.remove(stale);
            continue;
        }

        let final_path = paths.hierarchy_root_dir.join(stale);
        if tokio::fs::try_exists(&final_path).await.unwrap_or(false) {
            let owned = final_path.clone();
            tokio::task::spawn_blocking(move || remove_path(&owned)).await.map_err(|e| {
                MediaPmError::Workflow(format!("remove stale path task panicked: {e}"))
            })??;
            report.removed_paths += 1;
        }
        lock.managed_files.remove(stale);
    }

    // Remove empty parent directories after stale path cleanup.
    // Walk up from each removed path's parent, removing directories that
    // contain no files (recursively), stopping at the hierarchy root.
    let mut checked_parents = BTreeSet::new();
    for stale in &stale_paths {
        if stale.ends_with('/') || stale.ends_with('\\') {
            continue;
        }
        let mut parent = paths.hierarchy_root_dir.join(stale);
        if !parent.pop() {
            continue;
        }
        loop {
            if !checked_parents.insert(parent.clone()) {
                break;
            }
            if parent == paths.hierarchy_root_dir {
                break;
            }
            let is_empty = match tokio::fs::read_dir(&parent).await {
                Ok(mut entries) => entries.next_entry().await.unwrap_or(None).is_none(),
                Err(_) => false,
            };
            if !is_empty {
                break;
            }
            let owned = parent.clone();
            tokio::task::spawn_blocking(move || remove_path(&owned)).await.map_err(|e| {
                MediaPmError::Workflow(format!("remove empty parent dir task panicked: {e}"))
            })??;
            report.removed_empty_dirs += 1;
            if !parent.pop() {
                break;
            }
        }
    }

    hierarchy_progress.finish_success("done");
    tokio::time::sleep(Duration::from_millis(75)).await;

    let _ = fs::remove_dir_all(&staging_root);
    Ok(report)
}
