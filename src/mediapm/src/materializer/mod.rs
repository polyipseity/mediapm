//! Atomic staging + commit materializer for mediapm hierarchy sync.
//!
//! The materializer enforces path invariants, stages all outputs under
//! the resolved runtime staging directory, and then commits with atomic rename
//! operations into the resolved library directory.

use std::collections::{BTreeMap, BTreeSet};
use std::fs;
use std::path::{Path, PathBuf};
use std::time::Duration;

use mediapm_cas::{CasApi, FileSystemCas, Hash};
use mediapm_conductor::{MachineNickelDocument, OrchestrationState};
use pulsebar::MultiProgress;
use regex::Regex;

use crate::conductor_bridge::resolve_ffmpeg_slot_limits;
use crate::config::{
    HierarchyEntryKind, MediaPmDocument, PlaylistEntryPathMode, expand_variant_selectors,
    flatten_hierarchy_nodes_for_runtime,
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
    commit_staged_output, now_unix_seconds, remove_path, unix_epoch_millis, validate_hierarchy_path,
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
struct MaterializationLookupContext<'a> {
    /// Conductor CAS store used for payload reads.
    cas: &'a FileSystemCas,
    /// Resolved conductor machine document for tool/workflow metadata.
    machine: &'a MachineNickelDocument,
    /// Optional persisted orchestration state loaded from runtime pointer.
    orchestration_state: Option<&'a OrchestrationState>,
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
        operation: "creating .mediapm/tmp".to_string(),
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

    let cas = FileSystemCas::open(conductor_cas_root).await.map_err(|source| {
        MediaPmError::Workflow(format!(
            "opening conductor CAS store '{}' for materialization failed: {source}",
            conductor_cas_root.display()
        ))
    })?;
    let orchestration_state = load_runtime_orchestration_state(paths, &cas).await?;
    let managed_ffprobe_path = resolve_managed_ffprobe_path(paths, machine, lock);
    let lookup = MaterializationLookupContext {
        cas: &cas,
        machine,
        orchestration_state: orchestration_state.as_ref(),
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
    let mut resolved_playlist_media_targets = BTreeMap::<String, String>::new();

    let total_entries = flattened_hierarchy.len();
    let multi = MultiProgress::new();
    let hierarchy_progress = multi
        .add_bar(total_entries.max(1) as u64)
        .with_message("syncing hierarchy")
        .with_format("{msg}  {bar}  {pos}/{total}  {elapsed}");
    hierarchy_progress.set_position(0);

    'entries: for flattened_entry in &flattened_hierarchy {
        let relative_path_template = flattened_entry.path.as_str();
        let entry = &flattened_entry.entry;

        let relative_path =
            if matches!(entry.kind, HierarchyEntryKind::Media | HierarchyEntryKind::MediaFolder) {
                let source = resolve_hierarchy_source(document, entry)?;
                resolve_hierarchy_relative_path(relative_path_template, entry, source, &lookup)
                    .await?
            } else {
                relative_path_template.to_string()
            };
        let relative_path = normalize_resolved_hierarchy_path_to_nfd(&relative_path);
        validate_hierarchy_path(&relative_path)?;

        let fs_relative_path = relative_path.as_str();

        if fs_relative_path.is_empty() {
            return Err(MediaPmError::Workflow(format!(
                "hierarchy path '{relative_path}' must not resolve to an empty filesystem path"
            )));
        }

        let staged_path = staging_root.join(fs_relative_path);
        hierarchy_progress.advance(1);
        let (managed_media_id, managed_file_variants, managed_file_hashes) = match entry.kind {
            HierarchyEntryKind::Media => {
                let source = resolve_hierarchy_source(document, entry)?;

                if entry.variants.is_empty() {
                    return Err(MediaPmError::Workflow(format!(
                        "hierarchy path '{relative_path}' must define at least one variant"
                    )));
                }

                let available_variants = collect_media_source_available_variants(source);
                let resolved_variants =
                    expand_variant_selectors(&entry.variants, &available_variants).map_err(
                        |reason| {
                            MediaPmError::Workflow(format!(
                                "hierarchy path '{relative_path}' {reason} for media '{}'",
                                entry.media_id
                            ))
                        },
                    )?;

                if resolved_variants.len() != 1 {
                    return Err(MediaPmError::Workflow(format!(
                        "hierarchy file path '{relative_path}' must resolve exactly one variant"
                    )));
                }
                let variant = resolved_variants
                    .first()
                    .expect("checked non-empty and len==1 for hierarchy file path");

                // Change detection: resolve the expected hash without fetching
                // CAS bytes. When the lock already records the same hash and
                // the final output path exists on disk, skip re-materialization
                // entirely — this avoids large CAS object reads on repeat runs.
                let final_path = paths.hierarchy_root_dir.join(fs_relative_path);
                if let Some(hint_hash) =
                    resolve_variant_source_hash(&lookup, &entry.media_id, source, variant).await?
                {
                    let hint_hash_str = hint_hash.to_string();
                    if lock
                        .managed_files
                        .get(&relative_path)
                        .is_some_and(|r| r.hash == hint_hash_str)
                        && fs::symlink_metadata(&final_path).is_ok()
                    {
                        desired_paths.insert(relative_path.clone());
                        if let Some(r) = lock.managed_files.get_mut(&relative_path) {
                            r.last_synced_unix_millis = unix_epoch_millis();
                        }
                        report.skipped_paths += 1;
                        continue 'entries;
                    }
                }

                if let Some(parent) = staged_path.parent() {
                    tokio::fs::create_dir_all(parent).await.map_err(|source_err| {
                        MediaPmError::Io {
                            operation: "creating staged parent directory".to_string(),
                            path: parent.to_path_buf(),
                            source: source_err,
                        }
                    })?;
                }

                let variant_source =
                    resolve_variant_source_bytes(&lookup, &entry.media_id, source, variant).await?;
                if let Some(message) = variant_source.notice.as_deref() {
                    report.notices.push(message.to_string());
                }

                let file_hash = if let Some(source_hash) = variant_source.source_hash {
                    source_hash
                } else {
                    cas.put(variant_source.bytes).await.map_err(|source| {
                        MediaPmError::Workflow(format!(
                            "importing materialized file '{relative_path}' into CAS failed: {source}",
                        ))
                    })?
                };

                materialize_file_from_cas_with_order(
                    &cas,
                    file_hash,
                    &staged_path,
                    relative_path.as_str(),
                    &materialization_methods,
                    &mut report.notices,
                )
                .await?;

                (
                    entry.media_id.clone(),
                    BTreeMap::from([(relative_path.clone(), variant.clone())]),
                    BTreeMap::from([(relative_path.clone(), file_hash)]),
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
                let resolved_variants =
                    expand_variant_selectors(&entry.variants, &available_variants).map_err(
                        |reason| {
                            MediaPmError::Workflow(format!(
                                "hierarchy path '{relative_path}' {reason} for media '{}'",
                                entry.media_id
                            ))
                        },
                    )?;

                tokio::fs::create_dir_all(&staged_path).await.map_err(|source_err| {
                    MediaPmError::Io {
                        operation: "creating staged output directory".to_string(),
                        path: staged_path.clone(),
                        source: source_err,
                    }
                })?;

                let resolved_rename_rules = resolve_hierarchy_folder_rename_rule_replacements(
                    &entry.rename_files,
                    &relative_path,
                    entry,
                    source,
                    &lookup,
                )
                .await?;
                let compiled_rename_rules = compile_hierarchy_folder_rename_rules(
                    &resolved_rename_rules,
                    &relative_path,
                    &entry.media_id,
                )?;

                let mut extracted_entries = BTreeMap::new();
                let mut extracted_entry_variants = BTreeMap::<String, String>::new();
                for variant in &resolved_variants {
                    let variant_source =
                        resolve_variant_source_bytes(&lookup, &entry.media_id, source, variant)
                            .await?;
                    if let Some(message) = variant_source.notice.as_deref() {
                        report.notices.push(message.to_string());
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
                    let staged_hash = cas.put(staged_bytes).await.map_err(|source| {
                        MediaPmError::Workflow(format!(
                            "importing materialized folder member '{managed_path}' into CAS failed: {source}",
                        ))
                    })?;
                    materialize_file_from_cas_with_order(
                        &cas,
                        staged_hash,
                        &staged_file_path,
                        &managed_path,
                        &materialization_methods,
                        &mut report.notices,
                    )
                    .await?;
                    managed_file_hashes.insert(managed_path, staged_hash);

                    let entry_variant = extracted_entry_variants.get(entry_path).cloned().ok_or_else(
                        || {
                            MediaPmError::Workflow(format!(
                                "missing extracted variant provenance for hierarchy path '{relative_path}' media '{}' extracted file '{entry_path}'",
                                entry.media_id
                            ))
                        },
                    )?;
                    managed_file_variants
                        .insert(join_relative_paths(fs_relative_path, entry_path), entry_variant);
                }

                (entry.media_id.clone(), managed_file_variants, managed_file_hashes)
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
                        &lookup,
                        media_path_template,
                        &media_file_templates,
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
                    tokio::fs::create_dir_all(parent).await.map_err(|source_err| {
                        MediaPmError::Io {
                            operation: "creating staged parent directory".to_string(),
                            path: parent.to_path_buf(),
                            source: source_err,
                        }
                    })?;
                }

                let playlist_bytes = render_playlist_bytes(entry.format, &rendered_items);
                let playlist_hash = cas.put(playlist_bytes).await.map_err(|source| {
                    MediaPmError::Workflow(format!(
                        "importing generated playlist '{relative_path}' into CAS failed: {source}",
                    ))
                })?;
                materialize_file_from_cas_with_order(
                    &cas,
                    playlist_hash,
                    &staged_path,
                    relative_path.as_str(),
                    &materialization_methods,
                    &mut report.notices,
                )
                .await?;

                (
                    "playlist".to_string(),
                    BTreeMap::from([(
                        relative_path.clone(),
                        format!("playlist:{}", playlist_format_label(entry.format)),
                    )]),
                    BTreeMap::from([(relative_path.clone(), playlist_hash)]),
                )
            }
        };

        desired_paths.extend(managed_file_hashes.keys().cloned());

        let final_path = paths.hierarchy_root_dir.join(fs_relative_path);
        if let Some(parent) = final_path.parent() {
            tokio::fs::create_dir_all(parent).await.map_err(|source_err| MediaPmError::Io {
                operation: "creating final output parent directory".to_string(),
                path: parent.to_path_buf(),
                source: source_err,
            })?;
        }
        let staged_commit = staged_path.clone();
        let final_commit = final_path.clone();
        let entry_kind = entry.kind;
        tokio::task::spawn_blocking(move || {
            commit_staged_output(&staged_commit, &final_commit, entry_kind)
        })
        .await
        .map_err(|e| {
            MediaPmError::Workflow(format!("commit staged output task panicked: {e}"))
        })??;

        for (managed_file_path, managed_hash) in managed_file_hashes {
            let managed_variant = managed_file_variants
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

    for stale in stale_paths {
        if stale.ends_with('/') || stale.ends_with('\\') {
            // Legacy lock rows from historical directory-level tracking should
            // not remove whole directories once file-level tracking is active.
            lock.managed_files.remove(&stale);
            continue;
        }

        let final_path = paths.hierarchy_root_dir.join(&stale);
        if tokio::fs::try_exists(&final_path).await.unwrap_or(false) {
            let owned = final_path.clone();
            tokio::task::spawn_blocking(move || remove_path(&owned)).await.map_err(|e| {
                MediaPmError::Workflow(format!("remove stale path task panicked: {e}"))
            })??;
            report.removed_paths += 1;
        }
        lock.managed_files.remove(&stale);
    }

    hierarchy_progress.finish_success("done");
    tokio::time::sleep(Duration::from_millis(75)).await;

    let _ = fs::remove_dir_all(&staging_root);
    Ok(report)
}
