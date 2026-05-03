//! Atomic staging + commit materializer for Phase 3 hierarchy sync.
//!
//! The materializer enforces path invariants, stages all outputs under
//! the resolved runtime staging directory, and then commits with atomic rename
//! operations into the resolved library directory.

use std::cmp::Ordering;
use std::collections::{BTreeMap, BTreeSet};
use std::fs;
use std::io::{self, Read};
use std::path::{Path, PathBuf};
use std::process::Command;
use std::time::{SystemTime, UNIX_EPOCH};

use mediapm_cas::{CasApi, FileSystemCas, Hash};
use mediapm_conductor::model::config::ImpureTimestamp;
use mediapm_conductor::{
    InputBinding, MachineNickelDocument, OrchestrationState, ToolCallInstance, ToolKindSpec,
    ToolSpec, decode_state, decode_state_document,
};
use regex::Regex;
use unicode_normalization::UnicodeNormalization;

use crate::conductor_bridge::{
    managed_workflow_id_for_media, resolve_ffmpeg_slot_limits,
    resolve_media_variant_output_binding_with_limits,
};
use crate::config::{
    FlattenedHierarchyEntry, HierarchyEntry, HierarchyEntryKind, HierarchyFolderRenameRule,
    MaterializationMethod, MediaMetadataRegexTransform, MediaMetadataValue, MediaPmDocument,
    MediaSourceSpec, PlaylistEntryPathMode, PlaylistFormat, expand_variant_selectors,
    flatten_hierarchy_nodes_for_runtime, hierarchy_metadata_placeholder_keys, media_source_uri,
};
use crate::error::MediaPmError;
use crate::lockfile::{ManagedFileRecord, MediaLockFile};
use crate::paths::MediaPmPaths;

/// Summary of one materialization pass.
#[derive(Debug, Clone, PartialEq, Eq, Default)]
pub struct MaterializeReport {
    /// Number of hierarchy entries staged and committed.
    pub materialized_paths: usize,
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

/// Removes one destination path if it already exists.
///
/// This helper treats broken symlinks as existing paths and removes them too.
fn remove_existing_destination_path(path: &Path) -> Result<(), MediaPmError> {
    if fs::symlink_metadata(path).is_ok() {
        remove_path(path)?;
    }
    Ok(())
}

/// Creates one filesystem symlink for a regular file.
#[cfg(unix)]
fn create_file_symlink(source_path: &Path, destination_path: &Path) -> io::Result<()> {
    std::os::unix::fs::symlink(source_path, destination_path)
}

/// Creates one filesystem symlink for a regular file.
#[cfg(windows)]
fn create_file_symlink(source_path: &Path, destination_path: &Path) -> io::Result<()> {
    std::os::windows::fs::symlink_file(source_path, destination_path)
}

/// Attempts reflink/clone materialization for one file.
///
/// Current implementation reports unsupported on this build/runtime and lets
/// ordered fallback proceed to subsequent configured methods.
fn attempt_reflink_materialization(
    _source_path: &Path,
    _destination_path: &Path,
) -> io::Result<()> {
    Err(io::Error::new(
        io::ErrorKind::Unsupported,
        "reflink materialization is not supported on this build",
    ))
}

/// Attempts one configured materialization method for one destination file.
async fn attempt_materialization_method(
    method: MaterializationMethod,
    cas: &FileSystemCas,
    hash: Hash,
    source_path: Option<&Path>,
    destination_path: &Path,
) -> io::Result<()> {
    match method {
        MaterializationMethod::Hardlink => {
            let source = source_path.ok_or_else(|| {
                io::Error::new(
                    io::ErrorKind::NotFound,
                    "CAS object file is unavailable for hardlink materialization",
                )
            })?;
            fs::hard_link(source, destination_path)
        }
        MaterializationMethod::Symlink => {
            let source = source_path.ok_or_else(|| {
                io::Error::new(
                    io::ErrorKind::NotFound,
                    "CAS object file is unavailable for symlink materialization",
                )
            })?;
            create_file_symlink(source, destination_path)
        }
        MaterializationMethod::Reflink => {
            let source = source_path.ok_or_else(|| {
                io::Error::new(
                    io::ErrorKind::NotFound,
                    "CAS object file is unavailable for reflink materialization",
                )
            })?;
            attempt_reflink_materialization(source, destination_path)
        }
        MaterializationMethod::Copy => {
            if let Some(source) = source_path {
                fs::copy(source, destination_path).map(|_| ())
            } else {
                let bytes = cas.get(hash).await.map_err(|error| {
                    io::Error::other(format!(
                        "reading CAS bytes for copy materialization failed: {error}"
                    ))
                })?;
                fs::write(destination_path, bytes.as_ref())
            }
        }
    }
}

/// Materializes one managed file from CAS using ordered runtime policy.
async fn materialize_file_from_cas_with_order(
    cas: &FileSystemCas,
    hash: Hash,
    destination_path: &Path,
    managed_relative_path: &str,
    methods: &[MaterializationMethod],
    notices: &mut Vec<String>,
) -> Result<(), MediaPmError> {
    let source_path = cas.object_path_for_hash(hash);
    let source_path = source_path.is_file().then_some(source_path);
    let mut failures = Vec::new();

    for (method_index, method) in methods.iter().enumerate() {
        remove_existing_destination_path(destination_path)?;

        match attempt_materialization_method(
            *method,
            cas,
            hash,
            source_path.as_deref(),
            destination_path,
        )
        .await
        {
            Ok(()) => {
                if method_index > 0 {
                    notices.push(format!(
                        "hierarchy file '{managed_relative_path}' materialization fell back to '{}'",
                        method.as_label()
                    ));
                }
                return Ok(());
            }
            Err(error) => {
                failures.push(format!("{}: {error}", method.as_label()));
                let _ = remove_existing_destination_path(destination_path);
            }
        }
    }

    Err(MediaPmError::Workflow(format!(
        "materializing hierarchy file '{managed_relative_path}' failed for all configured methods ({})",
        failures.join("; ")
    )))
}

/// Collects effective hierarchy-id -> hierarchy media-path mappings.
fn collect_playlist_media_index(
    flattened_hierarchy: &[FlattenedHierarchyEntry],
) -> Result<BTreeMap<String, String>, MediaPmError> {
    let mut index = BTreeMap::new();

    for flattened_entry in flattened_hierarchy {
        if !matches!(flattened_entry.entry.kind, HierarchyEntryKind::Media) {
            continue;
        }

        let Some(hierarchy_id) = flattened_entry.hierarchy_id.as_deref() else {
            continue;
        };

        if let Some(previous_path) =
            index.insert(hierarchy_id.to_string(), flattened_entry.path.clone())
            && previous_path != flattened_entry.path
        {
            return Err(MediaPmError::Workflow(format!(
                "hierarchy id '{}' resolves to multiple media paths ('{}' and '{}')",
                hierarchy_id, previous_path, flattened_entry.path
            )));
        }
    }

    Ok(index)
}

/// Collects file-target hierarchy templates keyed by hierarchy path.
///
/// Playlist generation consumes only explicit `kind = "media"` entries.
/// Folder semantics are owned by explicit `kind = "media_folder"` entries and
/// must never be inferred from path text.
fn collect_media_file_hierarchy_templates(
    flattened_hierarchy: &[FlattenedHierarchyEntry],
) -> Result<BTreeMap<String, HierarchyEntry>, MediaPmError> {
    let mut templates = BTreeMap::new();

    for flattened_entry in flattened_hierarchy {
        let entry = &flattened_entry.entry;

        if !matches!(entry.kind, HierarchyEntryKind::Media) || entry.media_id.trim().is_empty() {
            continue;
        }

        if let Some(previous_entry) = templates.insert(flattened_entry.path.clone(), entry.clone())
            && previous_entry != *entry
        {
            return Err(MediaPmError::Workflow(format!(
                "hierarchy path '{}' resolves to conflicting media entries",
                flattened_entry.path
            )));
        }
    }

    Ok(templates)
}

/// Resolves one media output relative path used by playlist generation.
async fn resolve_playlist_media_target_relative_path(
    document: &MediaPmDocument,
    lookup: &MaterializationLookupContext<'_>,
    media_path_template: &str,
    media_file_templates: &BTreeMap<String, HierarchyEntry>,
    cache: &mut BTreeMap<String, String>,
) -> Result<String, MediaPmError> {
    if let Some(cached) = cache.get(media_path_template) {
        return Ok(cached.clone());
    }

    let entry = media_file_templates.get(media_path_template).ok_or_else(|| {
        MediaPmError::Workflow(format!(
            "playlist resolution references hierarchy path '{media_path_template}' that is not a media file target"
        ))
    })?;
    let source = document.media.get(entry.media_id.as_str()).ok_or_else(|| {
        MediaPmError::Workflow(format!(
            "playlist resolution references unknown media '{}'",
            entry.media_id
        ))
    })?;

    let resolved =
        resolve_hierarchy_relative_path(media_path_template, entry, source, lookup).await?;
    if resolved.ends_with('/') || resolved.ends_with('\\') {
        return Err(MediaPmError::Workflow(format!(
            "playlist resolution for hierarchy path '{media_path_template}' requires file hierarchy target, but '{resolved}' is a directory path"
        )));
    }

    cache.insert(media_path_template.to_string(), resolved.clone());
    Ok(resolved)
}

/// Renders one playlist-relative path from playlist file to media target file.
fn render_relative_playlist_path(playlist_path: &str, target_path: &str) -> String {
    let mut playlist_components = normalize_path_components(playlist_path);
    if !playlist_components.is_empty() {
        let _ = playlist_components.pop();
    }
    let target_components = normalize_path_components(target_path);

    let mut shared_prefix = 0usize;
    while shared_prefix < playlist_components.len()
        && shared_prefix < target_components.len()
        && playlist_components[shared_prefix] == target_components[shared_prefix]
    {
        shared_prefix = shared_prefix.saturating_add(1);
    }

    let mut relative_components = Vec::new();
    for _ in shared_prefix..playlist_components.len() {
        relative_components.push("..".to_string());
    }
    relative_components.extend(target_components[shared_prefix..].iter().cloned());

    relative_components.join("/")
}

/// Normalizes one path string into non-empty slash-separated components.
fn normalize_path_components(path: &str) -> Vec<String> {
    path.replace('\\', "/")
        .split('/')
        .filter(|segment| !segment.is_empty())
        .map(ToString::to_string)
        .collect::<Vec<_>>()
}

/// Joins two managed relative path segments using forward slashes.
#[must_use]
fn join_relative_paths(base: &str, child: &str) -> String {
    let normalized_base = base.trim_end_matches(['/', '\\']).replace('\\', "/");
    let normalized_child = child.trim_start_matches(['/', '\\']);
    if normalized_base.is_empty() {
        normalized_child.to_string()
    } else {
        format!("{normalized_base}/{normalized_child}")
    }
}

/// Renders absolute playlist path text from one library-relative target path.
fn render_absolute_playlist_path(paths: &MediaPmPaths, target_path: &str) -> String {
    let absolute = paths.hierarchy_root_dir.join(target_path);
    absolute.to_string_lossy().replace('\\', "/")
}

/// Renders playlist payload bytes for one configured format.
fn render_playlist_bytes(format: PlaylistFormat, items: &[RenderedPlaylistItem]) -> Vec<u8> {
    let content = match format {
        PlaylistFormat::M3u8 | PlaylistFormat::M3u => {
            let mut rendered = String::from("#EXTM3U\n");
            for item in items {
                rendered.push_str(item.path.as_str());
                rendered.push('\n');
            }
            rendered
        }
        PlaylistFormat::Pls => {
            let mut rendered = String::from("[playlist]\n");
            for (index, item) in items.iter().enumerate() {
                let item_number = index.saturating_add(1);
                rendered.push_str(format!("File{item_number}={}\n", item.path).as_str());
                rendered.push_str(format!("Title{item_number}={}\n", item.id).as_str());
                rendered.push_str(format!("Length{item_number}=-1\n").as_str());
            }
            rendered.push_str(format!("NumberOfEntries={}\n", items.len()).as_str());
            rendered.push_str("Version=2\n");
            rendered
        }
        PlaylistFormat::Xspf => {
            let mut rendered = String::from(
                "<?xml version=\"1.0\" encoding=\"UTF-8\"?>\n<playlist version=\"1\" xmlns=\"http://xspf.org/ns/0/\">\n  <trackList>\n",
            );
            for item in items {
                let location = escape_xml(item.path.as_str());
                let title = escape_xml(item.id.as_str());
                rendered.push_str("    <track>\n");
                rendered.push_str(format!("      <title>{title}</title>\n").as_str());
                rendered.push_str(format!("      <location>{location}</location>\n").as_str());
                rendered.push_str("    </track>\n");
            }
            rendered.push_str("  </trackList>\n</playlist>\n");
            rendered
        }
        PlaylistFormat::Wpl => {
            let mut rendered =
                String::from("<?wpl version=\"1.0\"?>\n<smil>\n  <body>\n    <seq>\n");
            for item in items {
                let source = escape_xml(item.path.as_str());
                rendered.push_str(format!("      <media src=\"{source}\" />\n").as_str());
            }
            rendered.push_str("    </seq>\n  </body>\n</smil>\n");
            rendered
        }
        PlaylistFormat::Asx => {
            let mut rendered = String::from("<asx version=\"3.0\">\n");
            for item in items {
                let source = escape_xml(item.path.as_str());
                let title = escape_xml(item.id.as_str());
                rendered.push_str("  <entry>\n");
                rendered.push_str(format!("    <title>{title}</title>\n").as_str());
                rendered.push_str(format!("    <ref href=\"{source}\" />\n").as_str());
                rendered.push_str("  </entry>\n");
            }
            rendered.push_str("</asx>\n");
            rendered
        }
    };

    content.into_bytes()
}

/// Returns stable format label for lockfile provenance records.
const fn playlist_format_label(format: PlaylistFormat) -> &'static str {
    match format {
        PlaylistFormat::M3u8 => "m3u8",
        PlaylistFormat::M3u => "m3u",
        PlaylistFormat::Pls => "pls",
        PlaylistFormat::Xspf => "xspf",
        PlaylistFormat::Wpl => "wpl",
        PlaylistFormat::Asx => "asx",
    }
}

/// Escapes XML-special characters in one text value.
fn escape_xml(value: &str) -> String {
    value
        .replace('&', "&amp;")
        .replace('<', "&lt;")
        .replace('>', "&gt;")
        .replace('"', "&quot;")
        .replace('\'', "&apos;")
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
    fs::create_dir_all(&paths.hierarchy_root_dir).map_err(|source| MediaPmError::Io {
        operation: "creating resolved library directory".to_string(),
        path: paths.hierarchy_root_dir.clone(),
        source,
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
    fs::create_dir_all(&staging_root).map_err(|source| MediaPmError::Io {
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

    for flattened_entry in &flattened_hierarchy {
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
        validate_hierarchy_path(&relative_path)?;

        let fs_relative_path = relative_path.as_str();

        if fs_relative_path.is_empty() {
            return Err(MediaPmError::Workflow(format!(
                "hierarchy path '{relative_path}' must not resolve to an empty filesystem path"
            )));
        }

        let staged_path = staging_root.join(fs_relative_path);
        let (managed_media_id, managed_variant, managed_file_hashes) = match entry.kind {
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

                if let Some(parent) = staged_path.parent() {
                    fs::create_dir_all(parent).map_err(|source_err| MediaPmError::Io {
                        operation: "creating staged parent directory".to_string(),
                        path: parent.to_path_buf(),
                        source: source_err,
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
                    resolved_variants.join("+"),
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

                fs::create_dir_all(&staged_path).map_err(|source_err| MediaPmError::Io {
                    operation: "creating staged output directory".to_string(),
                    path: staged_path.clone(),
                    source: source_err,
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
                    )?;
                }

                let mut managed_file_hashes = BTreeMap::new();
                for (entry_path, is_directory) in &extracted_entries {
                    if *is_directory {
                        continue;
                    }

                    let managed_path = join_relative_paths(fs_relative_path, entry_path);
                    let staged_file_path = staged_path.join(entry_path);
                    let staged_bytes =
                        fs::read(&staged_file_path).map_err(|source_err| MediaPmError::Io {
                            operation: "reading extracted staged file bytes for CAS import"
                                .to_string(),
                            path: staged_file_path.clone(),
                            source: source_err,
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
                }

                (entry.media_id.clone(), resolved_variants.join("+"), managed_file_hashes)
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
                    fs::create_dir_all(parent).map_err(|source_err| MediaPmError::Io {
                        operation: "creating staged parent directory".to_string(),
                        path: parent.to_path_buf(),
                        source: source_err,
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
                    format!("playlist:{}", playlist_format_label(entry.format)),
                    BTreeMap::from([(relative_path.clone(), playlist_hash)]),
                )
            }
        };

        desired_paths.extend(managed_file_hashes.keys().cloned());

        let final_path = paths.hierarchy_root_dir.join(fs_relative_path);
        if let Some(parent) = final_path.parent() {
            fs::create_dir_all(parent).map_err(|source_err| MediaPmError::Io {
                operation: "creating final output parent directory".to_string(),
                path: parent.to_path_buf(),
                source: source_err,
            })?;
        }
        commit_staged_output(&staged_path, &final_path, entry.kind)?;

        for (managed_file_path, managed_hash) in managed_file_hashes {
            lock.managed_files.insert(
                managed_file_path,
                ManagedFileRecord {
                    media_id: managed_media_id.clone(),
                    variant: managed_variant.clone(),
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
        if final_path.exists() {
            remove_path(&final_path)?;
            report.removed_paths += 1;
        }
        lock.managed_files.remove(&stale);
    }

    let _ = fs::remove_dir_all(&staging_root);
    Ok(report)
}

/// Merges one staged directory into one existing destination directory.
///
/// Existing destination children with the same names are replaced, while
/// unrelated existing children are preserved.
fn merge_staged_directory_into_existing(
    staged_dir: &Path,
    final_dir: &Path,
) -> Result<(), MediaPmError> {
    clear_path_readonly_recursively(final_dir)?;

    for entry in fs::read_dir(staged_dir).map_err(|source| MediaPmError::Io {
        operation: "reading staged directory before merge".to_string(),
        path: staged_dir.to_path_buf(),
        source,
    })? {
        let entry = entry.map_err(|source| MediaPmError::Io {
            operation: "iterating staged directory before merge".to_string(),
            path: staged_dir.to_path_buf(),
            source,
        })?;

        let staged_child = entry.path();
        let final_child = final_dir.join(entry.file_name());

        if final_child.exists() {
            let staged_metadata =
                fs::symlink_metadata(&staged_child).map_err(|source| MediaPmError::Io {
                    operation: "reading staged child metadata before merge".to_string(),
                    path: staged_child.clone(),
                    source,
                })?;
            let final_metadata =
                fs::symlink_metadata(&final_child).map_err(|source| MediaPmError::Io {
                    operation: "reading destination child metadata before merge".to_string(),
                    path: final_child.clone(),
                    source,
                })?;

            if staged_metadata.is_dir() && final_metadata.is_dir() {
                merge_staged_directory_into_existing(&staged_child, &final_child)?;
                continue;
            }

            remove_path(&final_child)?;
        }

        fs::rename(&staged_child, &final_child).map_err(|source| MediaPmError::Io {
            operation: "merging staged directory child".to_string(),
            path: final_child.clone(),
            source,
        })?;
    }

    fs::remove_dir(staged_dir).map_err(|source| MediaPmError::Io {
        operation: "removing emptied staged directory after merge".to_string(),
        path: staged_dir.to_path_buf(),
        source,
    })?;

    Ok(())
}

/// Commits one staged hierarchy output into the final library destination.
///
/// File outputs always replace the previous destination path atomically. For
/// folder outputs, existing destination directories are merged so overlapping
/// hierarchy entries do not delete already-materialized sibling content.
fn commit_staged_output(
    staged_path: &Path,
    final_path: &Path,
    entry_kind: HierarchyEntryKind,
) -> Result<(), MediaPmError> {
    if matches!(entry_kind, HierarchyEntryKind::MediaFolder) && final_path.exists() {
        let final_metadata =
            fs::symlink_metadata(final_path).map_err(|source| MediaPmError::Io {
                operation: "reading destination metadata before folder-merge commit".to_string(),
                path: final_path.to_path_buf(),
                source,
            })?;

        if final_metadata.is_dir() {
            merge_staged_directory_into_existing(staged_path, final_path)?;
            ensure_managed_path_readonly(final_path)?;
            return Ok(());
        }

        remove_path(final_path)?;
    } else if final_path.exists() {
        remove_path(final_path)?;
    }

    fs::rename(staged_path, final_path).map_err(|source| MediaPmError::Io {
        operation: "committing staged output via rename".to_string(),
        path: final_path.to_path_buf(),
        source,
    })?;
    ensure_managed_path_readonly(final_path)?;

    Ok(())
}

/// Resolves `${media.id}` and `${media.metadata.*}` placeholders for one
/// hierarchy key template.
async fn resolve_hierarchy_relative_path(
    relative_path_template: &str,
    entry: &HierarchyEntry,
    source: &MediaSourceSpec,
    lookup: &MaterializationLookupContext<'_>,
) -> Result<String, MediaPmError> {
    let context_label = format!("hierarchy path '{relative_path_template}'");
    resolve_media_placeholder_template(
        relative_path_template,
        entry,
        source,
        lookup,
        context_label.as_str(),
    )
    .await
}

/// Resolves supported media placeholder forms in one arbitrary template.
///
/// Supported placeholders:
/// - `${media.id}`
/// - `${media.metadata.<key>}`
async fn resolve_media_placeholder_template(
    template: &str,
    entry: &HierarchyEntry,
    source: &MediaSourceSpec,
    lookup: &MaterializationLookupContext<'_>,
    context_label: &str,
) -> Result<String, MediaPmError> {
    let placeholder_keys = hierarchy_metadata_placeholder_keys(template).map_err(|reason| {
        MediaPmError::Workflow(format!(
            "{context_label} has invalid metadata placeholder syntax: {reason}"
        ))
    })?;

    let has_media_id_placeholder = template.contains("${media.id}");

    if placeholder_keys.is_empty() && !has_media_id_placeholder {
        return Ok(template.to_string());
    }

    let mut resolved_values = BTreeMap::new();
    if !placeholder_keys.is_empty() {
        let metadata = source.metadata.as_ref().ok_or_else(|| {
            MediaPmError::Workflow(format!(
                "{context_label} references metadata placeholders but media '{}' does not define metadata",
                entry.media_id
            ))
        })?;

        for metadata_key in placeholder_keys {
            if resolved_values.contains_key(&metadata_key) {
                continue;
            }

            let metadata_value = metadata.get(metadata_key.as_str()).ok_or_else(|| {
                MediaPmError::Workflow(format!(
                    "{context_label} references undefined metadata key '{}' for media '{}'",
                    metadata_key, entry.media_id
                ))
            })?;

            let resolved = resolve_media_metadata_string_value(
                &entry.media_id,
                metadata_key.as_str(),
                metadata_value,
                source,
                lookup,
            )
            .await?;
            resolved_values.insert(metadata_key, resolved);
        }
    }

    let mut resolved_path = template.to_string();
    for (metadata_key, metadata_value) in resolved_values {
        let placeholder = format!("${{media.metadata.{metadata_key}}}");
        resolved_path = resolved_path.replace(&placeholder, metadata_value.as_str());
    }

    if has_media_id_placeholder {
        resolved_path = resolved_path.replace("${media.id}", entry.media_id.as_str());
    }

    Ok(resolved_path)
}

/// Resolves placeholder templates used by folder rename-rule replacements.
async fn resolve_hierarchy_folder_rename_rule_replacements(
    rules: &[HierarchyFolderRenameRule],
    hierarchy_path: &str,
    entry: &HierarchyEntry,
    source: &MediaSourceSpec,
    lookup: &MaterializationLookupContext<'_>,
) -> Result<Vec<HierarchyFolderRenameRule>, MediaPmError> {
    let mut resolved_rules = Vec::with_capacity(rules.len());

    for (rule_index, rule) in rules.iter().enumerate() {
        let context_label =
            format!("hierarchy path '{hierarchy_path}' rename_files[{rule_index}] replacement");
        let resolved_replacement = resolve_media_placeholder_template(
            rule.replacement.as_str(),
            entry,
            source,
            lookup,
            context_label.as_str(),
        )
        .await?;

        resolved_rules.push(HierarchyFolderRenameRule {
            pattern: rule.pattern.clone(),
            replacement: resolved_replacement,
        });
    }

    Ok(resolved_rules)
}

/// Resolves one media metadata value into a concrete string.
async fn resolve_media_metadata_string_value(
    media_id: &str,
    metadata_key: &str,
    metadata_value: &MediaMetadataValue,
    source: &MediaSourceSpec,
    lookup: &MaterializationLookupContext<'_>,
) -> Result<String, MediaPmError> {
    match metadata_value {
        MediaMetadataValue::Literal(value) => Ok(value.clone()),
        MediaMetadataValue::Variant(binding) => {
            let variant_source =
                resolve_variant_source_bytes(lookup, media_id, source, binding.variant.as_str())
                    .await?;

            let extracted = extract_metadata_value_from_variant_payload(
                lookup,
                media_id,
                metadata_key,
                binding.variant.as_str(),
                binding.metadata_key.as_str(),
                variant_source.bytes.as_slice(),
            )?;

            apply_metadata_regex_transform(
                media_id,
                metadata_key,
                binding.transform.as_ref(),
                extracted,
            )
        }
    }
}

/// Extracts one metadata value from variant payload bytes.
///
/// Resolution first attempts JSON lookup, then falls back to running ffprobe
/// against the variant bytes when JSON extraction does not produce the key.
fn extract_metadata_value_from_variant_payload(
    lookup: &MaterializationLookupContext<'_>,
    media_id: &str,
    metadata_name: &str,
    variant_name: &str,
    metadata_key: &str,
    variant_bytes: &[u8],
) -> Result<String, MediaPmError> {
    if let Ok(parsed) = serde_json::from_slice::<serde_json::Value>(variant_bytes)
        && let Some(extracted) = extract_metadata_key_from_json(&parsed, metadata_key)
    {
        return Ok(extracted);
    }

    let ffprobe_path = lookup.managed_ffprobe_path.as_deref().ok_or_else(|| {
        MediaPmError::Workflow(format!(
            "media '{media_id}' metadata '{metadata_name}' bound to variant '{variant_name}' requires ffprobe lookup for key '{metadata_key}', but active managed ffmpeg is not configured"
        ))
    })?;

    extract_metadata_key_with_ffprobe(
        ffprobe_path,
        media_id,
        metadata_name,
        variant_name,
        metadata_key,
        variant_bytes,
    )
}

/// Applies optional regex transform to one extracted metadata value.
fn apply_metadata_regex_transform(
    media_id: &str,
    metadata_name: &str,
    transform: Option<&MediaMetadataRegexTransform>,
    extracted: String,
) -> Result<String, MediaPmError> {
    let Some(transform) = transform else {
        return Ok(extracted);
    };

    let full_match_pattern = format!("^(?:{})$", transform.pattern);
    let regex = Regex::new(&full_match_pattern).map_err(|error| {
        MediaPmError::Workflow(format!(
            "media '{media_id}' metadata '{metadata_name}' transform.pattern '{}' is invalid regex: {error}",
            transform.pattern
        ))
    })?;

    if !regex.is_match(&extracted) {
        return Err(MediaPmError::Workflow(format!(
            "media '{media_id}' metadata '{metadata_name}' transform.pattern '{}' must fully match extracted value '{}'",
            transform.pattern, extracted
        )));
    }

    Ok(regex.replace(&extracted, transform.replacement.as_str()).into_owned())
}

/// Extracts one metadata key from JSON payloads, including ffprobe nested tag
/// layouts.
#[must_use]
fn extract_metadata_key_from_json(
    payload: &serde_json::Value,
    metadata_key: &str,
) -> Option<String> {
    let object = payload.as_object()?;

    if let Some(value) = lookup_json_string_key(object, metadata_key) {
        return Some(value);
    }

    if let Some(format_object) = object.get("format").and_then(serde_json::Value::as_object) {
        if let Some(value) = lookup_json_string_key(format_object, metadata_key) {
            return Some(value);
        }

        if let Some(tags) = format_object.get("tags").and_then(serde_json::Value::as_object)
            && let Some(value) = lookup_json_string_key(tags, metadata_key)
        {
            return Some(value);
        }
    }

    if let Some(streams) = object.get("streams").and_then(serde_json::Value::as_array) {
        for stream in streams {
            let Some(stream_object) = stream.as_object() else {
                continue;
            };

            if let Some(value) = lookup_json_string_key(stream_object, metadata_key) {
                return Some(value);
            }

            if let Some(tags) = stream_object.get("tags").and_then(serde_json::Value::as_object)
                && let Some(value) = lookup_json_string_key(tags, metadata_key)
            {
                return Some(value);
            }
        }
    }

    None
}

/// Looks up one string value by key with case-insensitive matching.
#[must_use]
fn lookup_json_string_key(
    object: &serde_json::Map<String, serde_json::Value>,
    key: &str,
) -> Option<String> {
    object
        .iter()
        .find_map(|(candidate_key, candidate_value)| {
            candidate_key.eq_ignore_ascii_case(key).then_some(candidate_value)
        })
        .and_then(serde_json::Value::as_str)
        .map(ToString::to_string)
}

/// Runs managed ffprobe and extracts one metadata key from probe output JSON.
fn extract_metadata_key_with_ffprobe(
    ffprobe_path: &Path,
    media_id: &str,
    metadata_name: &str,
    variant_name: &str,
    metadata_key: &str,
    variant_bytes: &[u8],
) -> Result<String, MediaPmError> {
    let unique = SystemTime::now().duration_since(UNIX_EPOCH).unwrap_or_default().as_nanos();
    let temp_path = std::env::temp_dir()
        .join(format!("mediapm-metadata-probe-{}-{unique}.bin", std::process::id()));

    fs::write(&temp_path, variant_bytes).map_err(|source| MediaPmError::Io {
        operation: "writing temporary metadata probe payload".to_string(),
        path: temp_path.clone(),
        source,
    })?;

    let output = Command::new(ffprobe_path)
        .arg("-v")
        .arg("error")
        .arg("-show_entries")
        .arg(format!(
            "format={metadata_key}:stream={metadata_key}:format_tags={metadata_key}:stream_tags={metadata_key}"
        ))
        .arg("-of")
        .arg("json")
        .arg(&temp_path)
        .output()
        .map_err(|source| {
            let _ = fs::remove_file(&temp_path);
            MediaPmError::Workflow(format!(
                "running managed ffprobe '{}' for media '{media_id}' metadata '{metadata_name}' failed: {source}",
                ffprobe_path.display()
            ))
        })?;

    let _ = fs::remove_file(&temp_path);

    if !output.status.success() {
        return Err(MediaPmError::Workflow(format!(
            "managed ffprobe '{}' failed while resolving media '{media_id}' metadata '{metadata_name}' from variant '{}': {}",
            ffprobe_path.display(),
            variant_name,
            String::from_utf8_lossy(&output.stderr)
        )));
    }

    let parsed = serde_json::from_slice::<serde_json::Value>(&output.stdout).map_err(|error| {
        MediaPmError::Workflow(format!(
            "managed ffprobe output for media '{media_id}' metadata '{metadata_name}' could not be decoded as JSON: {error}"
        ))
    })?;

    extract_metadata_key_from_json(&parsed, metadata_key).ok_or_else(|| {
        MediaPmError::Workflow(format!(
            "media '{media_id}' metadata '{metadata_name}' expected key '{metadata_key}' in variant '{variant_name}', but ffprobe reported no matching field or tag"
        ))
    })
}

/// Resolves host ffprobe path from active managed ffmpeg executable selector.
#[must_use]
fn resolve_managed_ffprobe_path(
    paths: &MediaPmPaths,
    machine: &MachineNickelDocument,
    lock: &MediaLockFile,
) -> Option<PathBuf> {
    let ffmpeg_tool_id = lock.active_tools.get("ffmpeg")?;
    let ffmpeg_tool = machine.tools.get(ffmpeg_tool_id)?;
    let ToolKindSpec::Executable { command, .. } = &ffmpeg_tool.kind else {
        return None;
    };

    let selector = command.first()?.trim();
    if selector.is_empty() {
        return None;
    }

    let ffmpeg_selector_path = PathBuf::from(resolve_host_command_selector_path(selector)?);
    let ffmpeg_path = if ffmpeg_selector_path.is_absolute() {
        ffmpeg_selector_path
    } else {
        paths.tools_dir.join(ffmpeg_tool_id).join(ffmpeg_selector_path)
    };
    let ffprobe_file_name = if cfg!(windows) { "ffprobe.exe" } else { "ffprobe" };

    if let Some(parent) = ffmpeg_path.parent() {
        Some(parent.join(ffprobe_file_name))
    } else {
        Some(PathBuf::from(ffprobe_file_name))
    }
}

/// Resolves a command selector expression to the host-specific path.
#[must_use]
fn resolve_host_command_selector_path(command_selector: &str) -> Option<String> {
    if command_selector.contains("context.os") {
        let host_os = std::env::consts::OS;
        let regex =
            Regex::new(r#"\$\{context\.os\s*==\s*\"([^\"]+)\"\s*\?\s*([^|}]*)\|\s*[^}]*\}"#)
                .ok()?;

        for captures in regex.captures_iter(command_selector) {
            let selector_os = captures.get(1).map(|value| value.as_str())?;
            if selector_os != host_os {
                continue;
            }

            let branch = captures.get(2).map(|value| value.as_str())?.trim();
            let unquoted = branch
                .strip_prefix('"')
                .and_then(|value| value.strip_suffix('"'))
                .or_else(|| branch.strip_prefix('\'').and_then(|value| value.strip_suffix('\'')))
                .unwrap_or(branch)
                .trim();

            if !unquoted.is_empty() {
                return Some(unquoted.to_string());
            }
        }

        return None;
    }

    let trimmed = command_selector.trim();
    if trimmed.is_empty() { None } else { Some(trimmed.to_string()) }
}

/// Loads persisted orchestration state referenced by runtime state pointer.
///
/// Returns `None` when the volatile runtime state document is absent, empty, or
/// does not carry a state pointer yet. A missing pointed CAS object is also
/// treated as unavailable state so sync can continue in mixed-backend flows
/// (for example in-memory conductor bootstrapping with filesystem materializer).
async fn load_runtime_orchestration_state(
    paths: &MediaPmPaths,
    cas: &FileSystemCas,
) -> Result<Option<OrchestrationState>, MediaPmError> {
    if !paths.conductor_state_config.exists() {
        return Ok(None);
    }

    let state_bytes =
        fs::read(&paths.conductor_state_config).map_err(|source| MediaPmError::Io {
            operation: "reading conductor runtime state document for materialization".to_string(),
            path: paths.conductor_state_config.clone(),
            source,
        })?;

    if state_bytes.iter().all(u8::is_ascii_whitespace) {
        return Ok(None);
    }

    let state_document = decode_state_document(&state_bytes).map_err(|error| {
        MediaPmError::Workflow(format!(
            "decoding conductor runtime state document '{}' failed: {error}",
            paths.conductor_state_config.display()
        ))
    })?;
    let Some(state_pointer) = state_document.state_pointer else {
        return Ok(None);
    };

    let Ok(state_blob) = cas.get(state_pointer).await else {
        return Ok(None);
    };

    let orchestration_state = decode_state(&state_blob).map_err(|error| {
        MediaPmError::Serialization(format!(
            "decoding persisted orchestration-state blob '{state_pointer}' failed: {error}"
        ))
    })?;

    Ok(Some(orchestration_state))
}

/// Resolves one hierarchy source reference.
fn resolve_hierarchy_source<'a>(
    document: &'a MediaPmDocument,
    entry: &HierarchyEntry,
) -> Result<&'a MediaSourceSpec, MediaPmError> {
    document.media.get(&entry.media_id).ok_or_else(|| {
        MediaPmError::Workflow(format!(
            "hierarchy references unknown media id '{}'",
            entry.media_id
        ))
    })
}

/// Collects available variant names for one media source.
///
/// The selector resolver needs the union of local hash variants and declared
/// step output variants so hierarchy selector expressions can expand into
/// concrete variant names before materialization.
#[must_use]
fn collect_media_source_available_variants(source: &MediaSourceSpec) -> BTreeSet<String> {
    let mut available = source.variant_hashes.keys().cloned().collect::<BTreeSet<_>>();
    for step in &source.steps {
        for variant in step.output_variants.keys() {
            available.insert(variant.clone());
        }
    }

    available
}

/// Resolves one source variant into concrete bytes for staging.
async fn resolve_variant_source_bytes(
    lookup: &MaterializationLookupContext<'_>,
    media_id: &str,
    source: &MediaSourceSpec,
    variant: &str,
) -> Result<VariantSourceBytes, MediaPmError> {
    let source_uri = media_source_uri(media_id, source);

    if let Some(state) = lookup.orchestration_state
        && let Some((workflow_hash, fallback_notice)) =
            resolve_variant_hash_from_workflow_state(lookup, state, media_id, source, variant)
                .await?
    {
        let bytes = lookup.cas.get(workflow_hash).await.map_err(|source| {
            MediaPmError::Workflow(format!(
                "workflow output hash '{workflow_hash}' for '{source_uri}' variant '{variant}' is missing from CAS: {source}"
            ))
        })?;

        let (materialized_bytes, source_hash) = if let Some(binding) =
            resolve_media_variant_output_binding_with_limits(
                source,
                variant,
                lookup.ffmpeg_max_input_slots,
                lookup.ffmpeg_max_output_slots,
            )? {
            if let Some(zip_member) = binding.zip_member.as_deref() {
                (
                    extract_zip_member_bytes(bytes.as_ref(), zip_member).map_err(|error| {
                        MediaPmError::Workflow(format!(
                            "extracting ZIP member '{zip_member}' for '{source_uri}' variant '{variant}' failed: {error}"
                        ))
                    })?,
                    None,
                )
            } else {
                (bytes.as_ref().to_vec(), Some(workflow_hash))
            }
        } else {
            (bytes.as_ref().to_vec(), Some(workflow_hash))
        };

        return Ok(VariantSourceBytes {
            bytes: materialized_bytes,
            notice: fallback_notice,
            source_hash,
        });
    }

    if source.variant_hashes.is_empty() {
        Err(MediaPmError::Workflow(format!(
            "source '{source_uri}' variant '{variant}' has no local variant hashes and no workflow output hash resolved from runtime state"
        )))
    } else {
        let hash_string = source
            .variant_hashes
            .get(variant)
            .or_else(|| source.variant_hashes.get("default"))
            .ok_or_else(|| {
                MediaPmError::Workflow(format!(
                    "local source '{source_uri}' does not define hash pointer for variant '{variant}'"
                ))
            })?
            .clone();

        let hash = hash_string.parse::<Hash>().map_err(|_| {
            MediaPmError::Workflow(format!(
                "local source '{source_uri}' variant '{variant}' has invalid CAS hash '{hash_string}': expected multihash string"
            ))
        })?;

        match lookup.cas.get(hash).await {
            Ok(bytes) => {
                if source.variant_hashes.contains_key(variant) {
                    Ok(VariantSourceBytes {
                        bytes: bytes.as_ref().to_vec(),
                        notice: None,
                        source_hash: Some(hash),
                    })
                } else {
                    Ok(VariantSourceBytes {
                        bytes: bytes.as_ref().to_vec(),
                        notice: Some(format!(
                            "variant '{variant}' missing for '{source_uri}'; used fallback variant 'default'"
                        )),
                        source_hash: Some(hash),
                    })
                }
            }
            Err(source) => Err(MediaPmError::Workflow(format!(
                "CAS hash '{hash}' for '{source_uri}' variant '{variant}' is missing from CAS: {source}"
            ))),
        }
    }
}

/// Resolves one materialization variant hash from workflow runtime outputs.
///
/// Returns `None` when the target variant is not produced by a workflow step,
/// when no managed workflow exists for the source, or when matching runtime
/// step instances are unavailable in current orchestration state.
async fn resolve_variant_hash_from_workflow_state(
    lookup: &MaterializationLookupContext<'_>,
    state: &OrchestrationState,
    media_id: &str,
    source: &MediaSourceSpec,
    variant: &str,
) -> Result<Option<(Hash, Option<String>)>, MediaPmError> {
    let Some(binding) = resolve_media_variant_output_binding_with_limits(
        source,
        variant,
        lookup.ffmpeg_max_input_slots,
        lookup.ffmpeg_max_output_slots,
    )?
    else {
        return Ok(None);
    };

    let workflow_id = managed_workflow_id_for_media(media_id, source);
    let Some(workflow) = lookup.machine.workflows.get(&workflow_id) else {
        return Ok(None);
    };

    let Some(step_output_hashes) =
        resolve_workflow_step_output_hashes(lookup.cas, lookup.machine, state, workflow).await?
    else {
        return Ok(None);
    };

    let output_hash = step_output_hashes
        .get(&binding.step_id)
        .and_then(|outputs| outputs.get(&binding.output_name))
        .copied();

    let Some(hash) = output_hash else {
        return Ok(None);
    };

    let fallback_notice = if binding.used_default_variant {
        Some(format!(
            "variant '{variant}' missing for media '{media_id}'; used workflow fallback variant 'default'"
        ))
    } else {
        None
    };

    Ok(Some((hash, fallback_notice)))
}

/// Resolves concrete output hashes for each workflow step using orchestration state.
///
/// Steps are matched by immutable tool id, canonical tool metadata, and exact
/// resolved input hash identities. `None` is returned when a required step
/// instance cannot be resolved from the current persisted orchestration state.
async fn resolve_workflow_step_output_hashes(
    cas: &FileSystemCas,
    machine: &MachineNickelDocument,
    state: &OrchestrationState,
    workflow: &mediapm_conductor::WorkflowSpec,
) -> Result<Option<StepOutputHashes>, MediaPmError> {
    let mut step_outputs = StepOutputHashes::new();
    let required_step_output_names = collect_required_step_output_names(workflow);
    let required_step_zip_members = collect_required_step_zip_members(workflow);

    for step in &workflow.steps {
        let expected_inputs =
            resolve_expected_input_hashes(cas, machine, &step.inputs, &step_outputs).await?;
        let Some(expected_inputs) = expected_inputs else {
            return Ok(None);
        };

        let expected_metadata = machine.tools.get(&step.tool).ok_or_else(|| {
            MediaPmError::Workflow(format!(
                "workflow step '{}' references unknown tool '{}' in machine config",
                step.id, step.tool
            ))
        })?;

        let required_output_names =
            required_step_output_names.get(&step.id).cloned().unwrap_or_default();
        let required_zip_members = required_step_zip_members.get(&step.id);

        let mut matching_instances = state
            .instances
            .iter()
            .filter_map(|(instance_id, instance)| {
                (instance.tool_name == step.tool
                    && tool_metadata_matches(expected_metadata, &instance.metadata)
                    && instance_matches_expected_inputs(instance, &expected_inputs)
                    && instance_matches_expected_output_names(instance, &step.outputs)
                    && instance_matches_required_output_names(instance, &required_output_names))
                .then_some((instance_id, instance))
            })
            .collect::<Vec<_>>();

        matching_instances.sort_by(|(left_id, left_instance), (right_id, right_instance)| {
            compare_instance_recency(
                left_id.as_str(),
                left_instance.impure_timestamp,
                right_id.as_str(),
                right_instance.impure_timestamp,
            )
        });

        let mut selected_instance = None;
        for (_, instance) in &matching_instances {
            if instance_has_materializable_required_outputs(
                cas,
                instance,
                &required_output_names,
                required_zip_members,
            )
            .await
            {
                selected_instance = Some(*instance);
                break;
            }
        }

        let Some(instance) =
            selected_instance.or_else(|| matching_instances.first().map(|(_, instance)| *instance))
        else {
            return Ok(None);
        };

        let output_hashes = instance
            .outputs
            .iter()
            .map(|(name, output)| (name.clone(), output.hash))
            .collect::<BTreeMap<_, _>>();
        step_outputs.insert(step.id.clone(), output_hashes);
    }

    Ok(Some(step_outputs))
}

/// Collects output names that must resolve for each workflow step.
///
/// Required names include:
/// - explicit step output policies (`step.outputs`), and
/// - downstream `${step_output.<step>.<output>...}` references.
fn collect_required_step_output_names(
    workflow: &mediapm_conductor::WorkflowSpec,
) -> RequiredStepOutputNames {
    let mut required = RequiredStepOutputNames::new();

    for step in &workflow.steps {
        for output_name in step.outputs.keys() {
            required.entry(step.id.clone()).or_default().insert(output_name.clone());
        }
    }

    for step in &workflow.steps {
        for binding in step.inputs.values() {
            let InputBinding::String(value) = binding else {
                continue;
            };

            if let Some(reference) = parse_step_output_reference(value) {
                required
                    .entry(reference.step_id.to_string())
                    .or_default()
                    .insert(reference.output_name.to_string());
            }
        }
    }

    required
}

/// Collects ZIP members that must be readable for each step-output reference.
fn collect_required_step_zip_members(
    workflow: &mediapm_conductor::WorkflowSpec,
) -> RequiredStepZipMembers {
    let mut required = RequiredStepZipMembers::new();

    for step in &workflow.steps {
        for binding in step.inputs.values() {
            let InputBinding::String(value) = binding else {
                continue;
            };

            let Some(reference) = parse_step_output_reference(value) else {
                continue;
            };

            let Some(zip_member) = reference.zip_member else {
                continue;
            };

            required
                .entry(reference.step_id.to_string())
                .or_default()
                .entry(reference.output_name.to_string())
                .or_default()
                .insert(zip_member.to_string());
        }
    }

    required
}

/// Returns true when one runtime instance exposes all required output names.
fn instance_matches_required_output_names(
    instance: &ToolCallInstance,
    required_output_names: &BTreeSet<String>,
) -> bool {
    required_output_names.iter().all(|output_name| instance.outputs.contains_key(output_name))
}

/// Compares two matching instances by recency for deterministic selection.
///
/// Higher recency wins:
/// 1. presence of impure timestamp,
/// 2. larger `epoch_seconds`,
/// 3. larger `subsec_nanos`,
/// 4. lexicographically larger instance id (stable tie-breaker).
fn compare_instance_recency(
    left_id: &str,
    left_timestamp: Option<ImpureTimestamp>,
    right_id: &str,
    right_timestamp: Option<ImpureTimestamp>,
) -> Ordering {
    let left_rank = instance_recency_rank(left_id, left_timestamp);
    let right_rank = instance_recency_rank(right_id, right_timestamp);
    right_rank.cmp(&left_rank)
}

/// Builds one sortable recency tuple for runtime instance selection.
fn instance_recency_rank(
    instance_id: &str,
    timestamp: Option<ImpureTimestamp>,
) -> (bool, u64, u32, &str) {
    match timestamp {
        Some(timestamp) => (true, timestamp.epoch_seconds, timestamp.subsec_nanos, instance_id),
        None => (false, 0, 0, instance_id),
    }
}

/// Returns whether required outputs are readable from CAS for one candidate.
///
/// This filters out stale instances whose required output hashes no longer
/// exist in CAS and verifies required ZIP-selector members when present.
async fn instance_has_materializable_required_outputs(
    cas: &FileSystemCas,
    instance: &ToolCallInstance,
    required_output_names: &BTreeSet<String>,
    required_zip_members: Option<&BTreeMap<String, BTreeSet<String>>>,
) -> bool {
    for output_name in required_output_names {
        let Some(output_ref) = instance.outputs.get(output_name) else {
            return false;
        };

        let Ok(output_bytes) = cas.get(output_ref.hash).await else {
            return false;
        };

        let Some(members) = required_zip_members.and_then(|by_output| by_output.get(output_name))
        else {
            continue;
        };

        for member in members {
            if extract_zip_member_bytes(output_bytes.as_ref(), member).is_err() {
                return false;
            }
        }
    }

    true
}

/// Returns true when two tool metadata specs represent the same runtime tool.
///
/// Builtin metadata persisted in orchestration state intentionally remains in a
/// strict minimal wire shape (`kind`/`name`/`version`) while decoded machine
/// config builtins may carry runtime defaults for `is_impure` and outputs.
/// Materializer instance matching must therefore compare builtin identity by
/// name/version only, while executable tools keep full-struct equality.
fn tool_metadata_matches(expected: &ToolSpec, actual: &ToolSpec) -> bool {
    match (&expected.kind, &actual.kind) {
        (
            ToolKindSpec::Builtin { name: expected_name, version: expected_version },
            ToolKindSpec::Builtin { name: actual_name, version: actual_version },
        ) => expected_name == actual_name && expected_version == actual_version,
        (ToolKindSpec::Executable { .. }, ToolKindSpec::Executable { .. }) => expected == actual,
        _ => false,
    }
}

/// Resolves expected input hashes for one workflow step from concrete bindings.
///
/// Returns `None` when the step depends on unresolved prior step output hashes.
async fn resolve_expected_input_hashes(
    cas: &FileSystemCas,
    machine: &MachineNickelDocument,
    step_inputs: &BTreeMap<String, InputBinding>,
    step_outputs: &StepOutputHashes,
) -> Result<Option<ExpectedStepInputs>, MediaPmError> {
    let mut expected = ExpectedStepInputs::default();

    for (input_name, binding) in step_inputs {
        match resolve_input_binding_hash(cas, machine, binding, step_outputs).await? {
            InputBindingHashResolution::Resolved(hash) => {
                expected.resolved_hashes.insert(input_name.clone(), hash);
            }
            InputBindingHashResolution::MissingPriorStepOutput => {
                return Ok(None);
            }
            InputBindingHashResolution::MissingMaterializedStepOutput => {
                expected.unresolved_hash_input_names.insert(input_name.clone());
            }
        }
    }

    Ok(Some(expected))
}

/// Resolves one workflow input binding into deterministic input hash identity.
///
/// This mirrors conductor runtime semantics:
/// - scalar values hash raw UTF-8 bytes,
/// - list values hash canonical JSON encoding,
/// - `${external_data.<hash>}` resolves directly to the declared CAS hash,
/// - `${step_output.<step_id>.<output_name>}` resolves from prior step outputs.
/// - `${step_output.<step_id>.<output_name>:zip(<member>)}` resolves ZIP
///   member bytes from that output before hashing.
async fn resolve_input_binding_hash(
    cas: &FileSystemCas,
    machine: &MachineNickelDocument,
    binding: &InputBinding,
    step_outputs: &StepOutputHashes,
) -> Result<InputBindingHashResolution, MediaPmError> {
    match binding {
        InputBinding::String(value) => {
            if let Some(reference) = parse_step_output_reference(value) {
                let Some(hash) = step_outputs
                    .get(reference.step_id)
                    .and_then(|outputs| outputs.get(reference.output_name))
                    .copied()
                else {
                    return Ok(InputBindingHashResolution::MissingPriorStepOutput);
                };

                if let Some(zip_member) = reference.zip_member {
                    let Ok(zip_bytes) = cas.get(hash).await else {
                        return Ok(InputBindingHashResolution::MissingMaterializedStepOutput);
                    };
                    let Ok(member_bytes) = extract_zip_member_bytes(zip_bytes.as_ref(), zip_member)
                    else {
                        return Ok(InputBindingHashResolution::MissingMaterializedStepOutput);
                    };
                    return Ok(InputBindingHashResolution::Resolved(Hash::from_content(
                        member_bytes.as_slice(),
                    )));
                }

                return Ok(InputBindingHashResolution::Resolved(hash));
            }

            if let Some(external_hash) = parse_external_data_reference(value)? {
                return machine
                    .external_data
                    .contains_key(&external_hash)
                    .then_some(InputBindingHashResolution::Resolved(external_hash))
                    .ok_or_else(|| {
                        MediaPmError::Workflow(format!(
                            "workflow binding references unknown external_data hash '{external_hash}'"
                        ))
                    });
            }

            Ok(InputBindingHashResolution::Resolved(Hash::from_content(value.as_bytes())))
        }
        InputBinding::StringList(values) => {
            let encoded = serde_json::to_vec(values).map_err(|error| {
                MediaPmError::Serialization(format!(
                    "encoding workflow string-list binding for deterministic hash resolution failed: {error}"
                ))
            })?;
            Ok(InputBindingHashResolution::Resolved(Hash::from_content(encoded.as_slice())))
        }
    }
}

/// Returns true when one runtime instance contains all expected input hashes.
///
/// Runtime may inject additional resolved inputs from tool-level defaults.
/// Materialization matching therefore treats step-declared bindings as a
/// required subset instead of requiring exact key-set equality.
fn instance_matches_expected_inputs(
    instance: &ToolCallInstance,
    expected_inputs: &ExpectedStepInputs,
) -> bool {
    expected_inputs.resolved_hashes.iter().all(|(name, hash)| {
        instance.inputs.get(name).is_some_and(|resolved| resolved.hash == *hash)
    }) && expected_inputs
        .unresolved_hash_input_names
        .iter()
        .all(|name| instance.inputs.contains_key(name))
}

/// Returns true when one runtime instance exposes all step-declared outputs.
///
/// Multiple equivalent tool calls can share tool identity and resolved input
/// hashes while persisting different output families (for example `yt-dlp`
/// primary content vs sidecar-only captures). Materialization must constrain
/// matching to instances that provide the workflow step's expected output keys.
fn instance_matches_expected_output_names(
    instance: &ToolCallInstance,
    expected_outputs: &BTreeMap<String, mediapm_conductor::OutputPolicy>,
) -> bool {
    expected_outputs.keys().all(|output_name| instance.outputs.contains_key(output_name))
}

/// Parsed `${step_output...}` binding reference metadata.
struct StepOutputReference<'a> {
    /// Producer step id.
    step_id: &'a str,
    /// Producer output name.
    output_name: &'a str,
    /// Optional ZIP-member selector.
    zip_member: Option<&'a str>,
}

/// Parses exact `${step_output.<step_id>.<output_name>}` references with
/// optional `${step_output.<step_id>.<output_name>:zip(<member>)}` selector.
fn parse_step_output_reference(value: &str) -> Option<StepOutputReference<'_>> {
    let content = value.strip_prefix("${step_output.")?.strip_suffix('}')?;

    let (selector, zip_member) = if let Some(without_suffix) = content.strip_suffix(')') {
        if let Some((prefix, member)) = without_suffix.rsplit_once(":zip(") {
            if member.is_empty() || member.contains('/') || member.contains('\\') {
                return None;
            }
            (prefix, Some(member))
        } else {
            (content, None)
        }
    } else {
        (content, None)
    };

    let (step_id, output_name) = selector.rsplit_once('.')?;
    if step_id.is_empty() || output_name.is_empty() {
        return None;
    }

    Some(StepOutputReference { step_id, output_name, zip_member })
}

/// Extracts one file payload from ZIP bytes using one flat member key.
fn extract_zip_member_bytes(zip_bytes: &[u8], member_key: &str) -> Result<Vec<u8>, String> {
    if member_key.is_empty() || member_key.contains('/') || member_key.contains('\\') {
        return Err(
            "ZIP member key must be non-empty and must not contain path separators".to_string()
        );
    }

    let reader = std::io::Cursor::new(zip_bytes);
    let mut archive = zip::ZipArchive::new(reader)
        .map_err(|error| format!("decoding ZIP payload failed: {error}"))?;

    let mut index = 0usize;
    while index < archive.len() {
        let mut entry = archive
            .by_index(index)
            .map_err(|error| format!("reading ZIP entry #{index} failed: {error}"))?;
        let entry_name = entry.name().replace('\\', "/");
        if entry_name == member_key {
            if entry.is_dir() {
                return Err(format!("ZIP member '{member_key}' resolves to a directory"));
            }
            let mut bytes = Vec::new();
            entry
                .read_to_end(&mut bytes)
                .map_err(|error| format!("reading ZIP member '{member_key}' failed: {error}"))?;
            return Ok(bytes);
        }
        index = index.saturating_add(1);
    }

    Err(format!("ZIP member '{member_key}' not found"))
}

/// Extracts one ZIP folder payload into a staged directory with merge checks.
///
/// Multiple hierarchy variants may contribute archive entries into the same
/// destination directory. This helper enforces strict path-collision rules so
/// no file or directory path can be overwritten by a later variant, except
/// duplicate file paths where the first extracted file is retained.
fn extract_zip_folder_variant_bytes(
    zip_bytes: &[u8],
    target_dir: &Path,
    hierarchy_path: &str,
    media_id: &str,
    variant: &str,
    rename_rules: &[CompiledHierarchyFolderRenameRule],
    extracted_entries: &mut BTreeMap<String, bool>,
) -> Result<(), MediaPmError> {
    let reader = std::io::Cursor::new(zip_bytes);
    let mut archive = zip::ZipArchive::new(reader).map_err(|error| {
        MediaPmError::Workflow(format!(
            "hierarchy path '{hierarchy_path}' media '{media_id}' variant '{variant}' is expected to be a ZIP folder payload: {error}"
        ))
    })?;

    for index in 0..archive.len() {
        let mut entry = archive.by_index(index).map_err(|error| {
            MediaPmError::Workflow(format!(
                "reading ZIP entry #{index} for hierarchy path '{hierarchy_path}' media '{media_id}' variant '{variant}' failed: {error}"
            ))
        })?;

        let normalized = normalize_zip_entry_relative_path(entry.name()).map_err(|reason| {
            MediaPmError::Workflow(format!(
                "invalid ZIP entry '{}' for hierarchy path '{hierarchy_path}' media '{media_id}' variant '{variant}': {reason}",
                entry.name()
            ))
        })?;

        if normalized.is_empty() {
            continue;
        }

        if entry.is_dir() {
            register_zip_directory_entry(&normalized, extracted_entries).map_err(|reason| {
                MediaPmError::Workflow(format!(
                    "directory merge conflict for hierarchy path '{hierarchy_path}' media '{media_id}' variant '{variant}': {reason}"
                ))
            })?;

            let directory_path = target_dir.join(&normalized);
            fs::create_dir_all(&directory_path).map_err(|source| MediaPmError::Io {
                operation: "creating staged hierarchy directory from ZIP payload".to_string(),
                path: directory_path,
                source,
            })?;
        } else {
            let renamed = apply_hierarchy_folder_rename_rules(
                &normalized,
                rename_rules,
                hierarchy_path,
                media_id,
                variant,
            )?;

            let should_write_entry =
                register_zip_file_entry(&renamed, extracted_entries).map_err(|reason| {
                    MediaPmError::Workflow(format!(
                        "file merge conflict for hierarchy path '{hierarchy_path}' media '{media_id}' variant '{variant}': {reason}"
                    ))
                })?;

            if !should_write_entry {
                continue;
            }

            let file_path = target_dir.join(&renamed);
            if let Some(parent) = file_path.parent() {
                fs::create_dir_all(parent).map_err(|source| MediaPmError::Io {
                    operation: "creating staged hierarchy file parent from ZIP payload".to_string(),
                    path: parent.to_path_buf(),
                    source,
                })?;
            }

            let mut bytes = Vec::new();
            entry.read_to_end(&mut bytes).map_err(|error| {
                MediaPmError::Workflow(format!(
                    "reading ZIP file entry '{}' for hierarchy path '{hierarchy_path}' media '{media_id}' variant '{variant}' failed: {error}",
                    entry.name()
                ))
            })?;

            fs::write(&file_path, bytes).map_err(|source| MediaPmError::Io {
                operation: "writing staged hierarchy file from ZIP payload".to_string(),
                path: file_path,
                source,
            })?;
        }
    }

    Ok(())
}

/// Compiles configured folder rename rules for one hierarchy entry.
fn compile_hierarchy_folder_rename_rules(
    rules: &[HierarchyFolderRenameRule],
    hierarchy_path: &str,
    media_id: &str,
) -> Result<Vec<CompiledHierarchyFolderRenameRule>, MediaPmError> {
    rules
        .iter()
        .enumerate()
        .map(|(rule_index, rule)| {
            let pattern = rule.pattern.trim();
            if pattern.is_empty() {
                return Err(MediaPmError::Workflow(format!(
                    "hierarchy path '{hierarchy_path}' media '{media_id}' has empty rename_files[{rule_index}] pattern"
                )));
            }
            let regex = Regex::new(pattern).map_err(|error| {
                MediaPmError::Workflow(format!(
                    "hierarchy path '{hierarchy_path}' media '{media_id}' has invalid rename_files[{rule_index}] pattern '{pattern}': {error}"
                ))
            })?;

            Ok(CompiledHierarchyFolderRenameRule {
                pattern: pattern.to_string(),
                replacement: rule.replacement.clone(),
                regex,
            })
        })
        .collect()
}

/// Applies ordered rename rules to one normalized ZIP file member path.
fn apply_hierarchy_folder_rename_rules(
    normalized_file_path: &str,
    rules: &[CompiledHierarchyFolderRenameRule],
    hierarchy_path: &str,
    media_id: &str,
    variant: &str,
) -> Result<String, MediaPmError> {
    if rules.is_empty() {
        return Ok(normalized_file_path.to_string());
    }

    let renamed = rules.iter().fold(normalized_file_path.to_string(), |current, rule| {
        rule.regex.replace_all(current.as_str(), rule.replacement.as_str()).into_owned()
    });

    let normalized_renamed = normalize_zip_entry_relative_path(&renamed).map_err(|reason| {
        let patterns = rules.iter().map(|rule| rule.pattern.as_str()).collect::<Vec<_>>();
        MediaPmError::Workflow(format!(
            "hierarchy path '{hierarchy_path}' media '{media_id}' variant '{variant}' rename_files {patterns:?} transformed ZIP file path '{normalized_file_path}' into invalid path '{renamed}': {reason}",
        ))
    })?;

    if normalized_renamed.is_empty() {
        let patterns = rules.iter().map(|rule| rule.pattern.as_str()).collect::<Vec<_>>();
        return Err(MediaPmError::Workflow(format!(
            "hierarchy path '{hierarchy_path}' media '{media_id}' variant '{variant}' rename_files {patterns:?} transformed ZIP file path '{normalized_file_path}' to an empty path",
        )));
    }

    Ok(normalized_renamed)
}

/// Normalizes one ZIP entry path into a safe relative path.
fn normalize_zip_entry_relative_path(entry_name: &str) -> Result<String, String> {
    let mut normalized = entry_name.replace('\\', "/");
    while let Some(stripped) = normalized.strip_prefix('/') {
        normalized = stripped.to_string();
    }
    while let Some(stripped) = normalized.strip_prefix("./") {
        normalized = stripped.to_string();
    }

    let mut components = Vec::new();
    for segment in normalized.split('/') {
        if segment.is_empty() {
            continue;
        }
        if segment == "." || segment == ".." {
            return Err("contains '.' or '..' path components".to_string());
        }
        if segment.contains(':') {
            return Err("contains ':' path segment characters".to_string());
        }
        components.push(segment);
    }

    Ok(components.join("/"))
}

/// Registers one ZIP directory path, rejecting file/directory collisions.
fn register_zip_directory_entry(
    entry_path: &str,
    extracted_entries: &mut BTreeMap<String, bool>,
) -> Result<(), String> {
    let mut cursor = String::new();

    for segment in entry_path.split('/') {
        if !cursor.is_empty() {
            cursor.push('/');
        }
        cursor.push_str(segment);

        match extracted_entries.get(&cursor).copied() {
            Some(true) => {}
            Some(false) => {
                return Err(format!(
                    "directory '{entry_path}' conflicts with existing file '{cursor}'"
                ));
            }
            None => {
                extracted_entries.insert(cursor.clone(), true);
            }
        }
    }

    Ok(())
}

/// Registers one ZIP file path and returns whether caller should write bytes.
///
/// Return value semantics:
/// - `Ok(true)`: caller should write file bytes,
/// - `Ok(false)`: duplicate file path encountered; keep first-writer bytes,
/// - `Err(..)`: invalid file/dir collision.
fn register_zip_file_entry(
    entry_path: &str,
    extracted_entries: &mut BTreeMap<String, bool>,
) -> Result<bool, String> {
    let mut parts = entry_path.split('/').collect::<Vec<_>>();
    if parts.is_empty() {
        return Err("file entry path is empty".to_string());
    }

    let file_name = parts.pop().expect("checked non-empty split result");
    let mut parent = String::new();

    for segment in parts {
        if !parent.is_empty() {
            parent.push('/');
        }
        parent.push_str(segment);

        match extracted_entries.get(&parent).copied() {
            Some(true) => {}
            Some(false) => {
                return Err(format!(
                    "file '{entry_path}' has parent '{parent}' that is already a file"
                ));
            }
            None => {
                extracted_entries.insert(parent.clone(), true);
            }
        }
    }

    let full_file_path =
        if parent.is_empty() { file_name.to_string() } else { format!("{parent}/{file_name}") };

    match extracted_entries.get(&full_file_path).copied() {
        Some(true) => {
            Err(format!("file '{entry_path}' conflicts with existing directory '{full_file_path}'"))
        }
        Some(false) => {
            // Keep first writer semantics for duplicate file names produced by
            // overlapping sidecar families (for example subtitle vs
            // auto-subtitle flattening into one media root).
            Ok(false)
        }
        None => {
            extracted_entries.insert(full_file_path, false);
            Ok(true)
        }
    }
}

/// Parses exact `${external_data.<hash>}` references.
fn parse_external_data_reference(value: &str) -> Result<Option<Hash>, MediaPmError> {
    let Some(hash_text) =
        value.strip_prefix("${external_data.").and_then(|text| text.strip_suffix('}'))
    else {
        return Ok(None);
    };

    if hash_text.is_empty() {
        return Err(MediaPmError::Workflow(
            "workflow binding '${external_data.<hash>}' requires a non-empty hash".to_string(),
        ));
    }

    let hash = hash_text.parse::<Hash>().map_err(|source| {
        MediaPmError::Workflow(format!(
            "workflow binding references invalid external_data hash '{hash_text}': {source}"
        ))
    })?;
    Ok(Some(hash))
}

/// Removes one path recursively when it is a directory, or as one file otherwise.
fn remove_path(path: &Path) -> Result<(), MediaPmError> {
    clear_path_readonly_recursively(path)?;

    let metadata = fs::symlink_metadata(path).map_err(|source| MediaPmError::Io {
        operation: "reading path metadata before removal".to_string(),
        path: path.to_path_buf(),
        source,
    })?;

    if metadata.is_dir() {
        fs::remove_dir_all(path).map_err(|source| MediaPmError::Io {
            operation: "removing stale directory".to_string(),
            path: path.to_path_buf(),
            source,
        })
    } else {
        fs::remove_file(path).map_err(|source| MediaPmError::Io {
            operation: "removing stale file".to_string(),
            path: path.to_path_buf(),
            source,
        })
    }
}

/// Marks one managed output path as read-only after successful commit.
///
/// For directory outputs, this recursively marks descendant files/directories
/// read-only. For symlinks, permissions are applied to the resolved target.
fn ensure_managed_path_readonly(path: &Path) -> Result<(), MediaPmError> {
    let metadata = fs::symlink_metadata(path).map_err(|source| MediaPmError::Io {
        operation: "reading managed output metadata before readonly enforcement".to_string(),
        path: path.to_path_buf(),
        source,
    })?;

    if metadata.is_dir() {
        for entry in fs::read_dir(path).map_err(|source| MediaPmError::Io {
            operation: "reading managed output directory before readonly enforcement".to_string(),
            path: path.to_path_buf(),
            source,
        })? {
            let entry = entry.map_err(|source| MediaPmError::Io {
                operation: "iterating managed output directory before readonly enforcement"
                    .to_string(),
                path: path.to_path_buf(),
                source,
            })?;
            ensure_managed_path_readonly(&entry.path())?;
        }
    }

    let mut permissions = fs::metadata(path)
        .map_err(|source| MediaPmError::Io {
            operation: "reading managed output permissions before readonly enforcement".to_string(),
            path: path.to_path_buf(),
            source,
        })?
        .permissions();
    if !permissions.readonly() {
        permissions.set_readonly(true);
        fs::set_permissions(path, permissions).map_err(|source| MediaPmError::Io {
            operation: "marking managed output path readonly".to_string(),
            path: path.to_path_buf(),
            source,
        })?;
    }

    Ok(())
}

/// Clears read-only bit recursively so stale managed paths can be removed.
fn clear_path_readonly_recursively(path: &Path) -> Result<(), MediaPmError> {
    let metadata = fs::symlink_metadata(path).map_err(|source| MediaPmError::Io {
        operation: "reading path metadata before readonly clear".to_string(),
        path: path.to_path_buf(),
        source,
    })?;

    if metadata.is_dir() {
        for entry in fs::read_dir(path).map_err(|source| MediaPmError::Io {
            operation: "reading directory before readonly clear".to_string(),
            path: path.to_path_buf(),
            source,
        })? {
            let entry = entry.map_err(|source| MediaPmError::Io {
                operation: "iterating directory before readonly clear".to_string(),
                path: path.to_path_buf(),
                source,
            })?;
            clear_path_readonly_recursively(&entry.path())?;
        }
    }

    let mut permissions = fs::metadata(path)
        .map_err(|source| MediaPmError::Io {
            operation: "reading path permissions before readonly clear".to_string(),
            path: path.to_path_buf(),
            source,
        })?
        .permissions();
    if permissions.readonly() {
        #[cfg(unix)]
        {
            use std::os::unix::fs::PermissionsExt;

            let mode = permissions.mode();
            let writable_mode = mode | 0o200;
            if writable_mode != mode {
                permissions.set_mode(writable_mode);
            }
        }

        #[cfg(not(unix))]
        {
            #[expect(
                clippy::permissions_set_readonly_false,
                reason = "on non-Unix platforms we must clear the readonly flag before managed overwrite/delete operations can succeed"
            )]
            {
                permissions.set_readonly(false);
            }
        }

        fs::set_permissions(path, permissions).map_err(|source| MediaPmError::Io {
            operation: "clearing readonly bit before managed-path removal".to_string(),
            path: path.to_path_buf(),
            source,
        })?;
    }

    Ok(())
}

/// Validates one relative hierarchy path against Phase 3 invariants.
fn validate_hierarchy_path(relative_path: &str) -> Result<(), MediaPmError> {
    let path = Path::new(relative_path);

    if path.is_absolute() {
        return Err(MediaPmError::Workflow(format!(
            "hierarchy path '{relative_path}' must be relative"
        )));
    }

    for component in path.components() {
        let segment = component.as_os_str().to_string_lossy();
        if segment == "." || segment == ".." {
            return Err(MediaPmError::Workflow(format!(
                "hierarchy path '{relative_path}' must not contain '.' or '..' components"
            )));
        }

        if segment.chars().any(is_rejected_char) {
            return Err(MediaPmError::Workflow(format!(
                "hierarchy path '{relative_path}' contains forbidden characters"
            )));
        }

        let nfd = segment.nfd().collect::<String>();
        if nfd != segment {
            return Err(MediaPmError::Workflow(format!(
                "hierarchy path '{relative_path}' is not Unicode NFD normalized"
            )));
        }
    }

    Ok(())
}

/// Returns whether one character is forbidden by cross-platform filename rules.
fn is_rejected_char(ch: char) -> bool {
    matches!(ch, '<' | '>' | ':' | '"' | '|' | '?' | '*')
}

/// Returns current Unix epoch timestamp in seconds.
fn now_unix_seconds() -> u64 {
    SystemTime::now().duration_since(UNIX_EPOCH).unwrap_or_default().as_secs()
}

/// Returns current Unix epoch timestamp in milliseconds.
fn unix_epoch_millis() -> u64 {
    let millis = SystemTime::now().duration_since(UNIX_EPOCH).unwrap_or_default().as_millis();
    u64::try_from(millis).unwrap_or(u64::MAX)
}

#[cfg(test)]
mod tests {
    use std::collections::{BTreeMap, BTreeSet};
    use std::path::Path;

    use mediapm_cas::{CasApi, FileSystemCas, Hash};
    use mediapm_conductor::model::config::ImpureTimestamp;
    use mediapm_conductor::model::config::ToolInputKind;
    use mediapm_conductor::{
        InputBinding, MachineNickelDocument, OrchestrationState, OutputCaptureSpec, OutputPolicy,
        OutputRef, OutputSaveMode, PersistenceFlags, StateNickelDocument, ToolCallInstance,
        ToolInputSpec, ToolKindSpec, ToolOutputSpec, ToolSpec, WorkflowSpec, WorkflowStepSpec,
        encode_state_document,
    };

    use crate::config::{
        HierarchyEntry, HierarchyEntryKind, HierarchyFolderRenameRule, HierarchyNode,
        HierarchyNodeKind, MaterializationMethod, MediaMetadataRegexTransform, MediaMetadataValue,
        MediaMetadataVariantBinding, MediaPmDocument, MediaSourceSpec, MediaStep, MediaStepTool,
        PlaylistEntryPathMode, PlaylistFormat, PlaylistItemRef, TransformInputValue,
    };
    use crate::lockfile::MediaLockFile;
    use crate::paths::MediaPmPaths;

    use super::{
        instance_matches_expected_inputs, resolve_managed_ffprobe_path, sync_hierarchy,
        validate_hierarchy_path,
    };

    /// Protects managed ffprobe metadata lookup by resolving relative command
    /// selectors under the active managed ffmpeg tool root.
    #[test]
    fn resolve_managed_ffprobe_path_anchors_relative_selector_to_tool_root() {
        let temp = tempfile::tempdir().expect("tempdir");
        let paths = MediaPmPaths::from_root(temp.path());

        let ffmpeg_tool_id =
            "mediapm.tools.ffmpeg+github-releases-btbn-ffmpeg-builds@demo".to_string();
        let mut machine = MachineNickelDocument::default();
        machine.tools.insert(
            ffmpeg_tool_id.clone(),
            ToolSpec {
                kind: ToolKindSpec::Executable {
                    command: vec![
                        "windows/ffmpeg-master-latest-win64-gpl-shared/bin/ffmpeg.exe".to_string(),
                    ],
                    env_vars: BTreeMap::new(),
                    success_codes: vec![0],
                },
                ..ToolSpec::default()
            },
        );

        let mut lock = MediaLockFile::default();
        lock.active_tools.insert("ffmpeg".to_string(), ffmpeg_tool_id.clone());

        let resolved = resolve_managed_ffprobe_path(&paths, &machine, &lock)
            .expect("managed ffprobe path should resolve");
        let expected_file_name = if cfg!(windows) { "ffprobe.exe" } else { "ffprobe" };
        let expected = paths
            .tools_dir
            .join(ffmpeg_tool_id)
            .join("windows/ffmpeg-master-latest-win64-gpl-shared/bin")
            .join(expected_file_name);

        assert_eq!(resolved, expected);
    }

    fn yt_dlp_output_variant(kind: &str) -> serde_json::Value {
        serde_json::json!({ "kind": kind, "save": "full" })
    }

    /// Builds one in-memory ZIP payload from relative file entries.
    fn zip_payload(entries: &[(&str, &[u8])]) -> Vec<u8> {
        let mut bytes = Vec::new();
        {
            let cursor = std::io::Cursor::new(&mut bytes);
            let mut writer = zip::ZipWriter::new(cursor);
            let options = zip::write::SimpleFileOptions::default();

            for (path, payload) in entries {
                writer.start_file(path, options).expect("start zip file entry");
                std::io::Write::write_all(&mut writer, payload).expect("write zip file entry");
            }

            writer.finish().expect("finish zip payload");
        }
        bytes
    }

    /// Ensures two files are realized as one hardlinked inode/file record.
    fn assert_hardlinked_paths(source_path: &Path, destination_path: &Path) {
        assert!(
            same_file::is_same_file(source_path, destination_path)
                .expect("compare hardlinked file identity"),
            "source '{}' should share the same backing file identity as destination '{}'",
            source_path.display(),
            destination_path.display()
        );
    }

    /// Protects default-order behavior by preferring hard links when available.
    #[tokio::test]
    async fn materialize_file_from_cas_with_order_prefers_hardlink_when_available() {
        let temp = tempfile::tempdir().expect("tempdir");
        let cas_root = temp.path().join("store");
        let cas = FileSystemCas::open(&cas_root).await.expect("open cas");

        let payload = b"hardlink-preferred".to_vec();
        let hash = cas.put(payload.clone()).await.expect("put bytes");
        let destination_path = temp.path().join("materialized.bin");

        let mut notices = Vec::new();
        super::materialize_file_from_cas_with_order(
            &cas,
            hash,
            &destination_path,
            "library/materialized.bin",
            &[
                MaterializationMethod::Hardlink,
                MaterializationMethod::Symlink,
                MaterializationMethod::Reflink,
                MaterializationMethod::Copy,
            ],
            &mut notices,
        )
        .await
        .expect("materialize with preferred hardlink");

        let source_path = cas.object_path_for_hash(hash);
        assert_hardlinked_paths(&source_path, &destination_path);
        assert_eq!(std::fs::read(&destination_path).expect("read destination"), payload);
        assert!(notices.is_empty(), "hardlink-first success should not emit fallback notices");
    }

    /// Protects fallback ordering by continuing after failed methods.
    #[tokio::test]
    async fn materialize_file_from_cas_with_order_falls_back_to_copy() {
        let temp = tempfile::tempdir().expect("tempdir");
        let cas_root = temp.path().join("store");
        let cas = FileSystemCas::open(&cas_root).await.expect("open cas");

        let payload = b"copy-fallback".to_vec();
        let hash = cas.put(payload.clone()).await.expect("put bytes");
        let destination_path = temp.path().join("fallback.bin");

        let mut notices = Vec::new();
        super::materialize_file_from_cas_with_order(
            &cas,
            hash,
            &destination_path,
            "library/fallback.bin",
            &[MaterializationMethod::Reflink, MaterializationMethod::Copy],
            &mut notices,
        )
        .await
        .expect("materialize with fallback to copy");

        assert_eq!(std::fs::read(&destination_path).expect("read destination"), payload);
        assert_eq!(
            notices,
            vec![
                "hierarchy file 'library/fallback.bin' materialization fell back to 'copy'"
                    .to_string()
            ]
        );
    }

    /// Protects strict failure behavior when every configured method fails.
    #[tokio::test]
    async fn materialize_file_from_cas_with_order_errors_when_all_methods_fail() {
        let temp = tempfile::tempdir().expect("tempdir");
        let cas_root = temp.path().join("store");
        let cas = FileSystemCas::open(&cas_root).await.expect("open cas");

        let hash = cas.put(b"all-methods-fail".to_vec()).await.expect("put bytes");
        let destination_path = temp.path().join("failed.bin");

        let mut notices = Vec::new();
        let error = super::materialize_file_from_cas_with_order(
            &cas,
            hash,
            &destination_path,
            "library/failed.bin",
            &[MaterializationMethod::Reflink],
            &mut notices,
        )
        .await
        .expect_err("all configured methods should fail");

        assert!(error.to_string().contains(
            "materializing hierarchy file 'library/failed.bin' failed for all configured methods"
        ));
        assert!(!destination_path.exists());
        assert!(notices.is_empty());
    }

    fn hierarchy_nodes(
        entries: BTreeMap<String, HierarchyEntry>,
    ) -> Vec<crate::config::HierarchyNode> {
        let mut media_id_counts = BTreeMap::<String, usize>::new();
        for entry in entries.values() {
            if matches!(entry.kind, HierarchyEntryKind::Media | HierarchyEntryKind::MediaFolder) {
                *media_id_counts.entry(entry.media_id.clone()).or_insert(0) += 1;
            }
        }

        entries
            .into_iter()
            .map(|(path, entry)| {
                let hierarchy_id = if matches!(
                    entry.kind,
                    HierarchyEntryKind::Media | HierarchyEntryKind::MediaFolder
                ) {
                    let count = media_id_counts.get(&entry.media_id).copied().unwrap_or(0);
                    Some(if count <= 1 {
                        entry.media_id.clone()
                    } else {
                        format!("{}:{path}", entry.media_id)
                    })
                } else {
                    None
                };

                match entry.kind {
                    HierarchyEntryKind::Media => crate::config::HierarchyNode {
                        path,
                        kind: HierarchyNodeKind::Media,
                        id: hierarchy_id,
                        media_id: Some(entry.media_id),
                        variant: entry.variants.first().cloned(),
                        variants: Vec::new(),
                        rename_files: Vec::new(),
                        format: PlaylistFormat::M3u8,
                        ids: Vec::new(),
                        children: Vec::new(),
                    },
                    HierarchyEntryKind::MediaFolder => crate::config::HierarchyNode {
                        path,
                        kind: HierarchyNodeKind::MediaFolder,
                        id: hierarchy_id,
                        media_id: Some(entry.media_id),
                        variant: None,
                        variants: entry.variants,
                        rename_files: entry.rename_files,
                        format: PlaylistFormat::M3u8,
                        ids: Vec::new(),
                        children: Vec::new(),
                    },
                    HierarchyEntryKind::Playlist => crate::config::HierarchyNode {
                        path,
                        kind: HierarchyNodeKind::Playlist,
                        id: None,
                        media_id: None,
                        variant: None,
                        variants: Vec::new(),
                        rename_files: Vec::new(),
                        format: entry.format,
                        ids: entry.ids,
                        children: Vec::new(),
                    },
                }
            })
            .collect()
    }

    /// Protects strict forbidden-character enforcement in hierarchy paths.
    #[test]
    fn hierarchy_path_rejects_forbidden_characters() {
        let err = validate_hierarchy_path("movies/Star:Wars.mkv").expect_err("path should fail");
        assert!(err.to_string().contains("forbidden characters"));
    }

    /// Protects NFD-only normalization policy for hierarchy path segments.
    #[test]
    fn hierarchy_path_rejects_non_nfd_segments() {
        let err = validate_hierarchy_path("movies/épisode.mkv").expect_err("NFD should fail");
        assert!(err.to_string().contains("NFD"));
    }

    /// Protects flattened sidecar materialization by allowing duplicate ZIP
    /// file names to co-exist as first-writer-wins entries.
    #[test]
    fn register_zip_file_entry_allows_duplicate_file_paths() {
        let mut extracted_entries = BTreeMap::new();

        let first_write = super::register_zip_file_entry("captions.en.vtt", &mut extracted_entries)
            .expect("first file registration should succeed");
        let duplicate_write =
            super::register_zip_file_entry("captions.en.vtt", &mut extracted_entries)
                .expect("duplicate file registration should preserve first writer");

        assert!(first_write, "first registration should request file write");
        assert!(
            !duplicate_write,
            "duplicate registration should skip write to keep first-writer bytes"
        );
        assert_eq!(extracted_entries.get("captions.en.vtt"), Some(&false));
    }

    /// Protects folder-rename rule sequencing by applying rules in declaration
    /// order against normalized ZIP file paths.
    #[test]
    fn folder_rename_rules_apply_in_declaration_order() {
        let rules = super::compile_hierarchy_folder_rename_rules(
            &[
                HierarchyFolderRenameRule {
                    pattern: "^(.+)\\.en\\.vtt$".to_string(),
                    replacement: "$1.subtitles.en.vtt".to_string(),
                },
                HierarchyFolderRenameRule {
                    pattern: "^(.+)\\.subtitles\\.en\\.vtt$".to_string(),
                    replacement: "$1.en.vtt".to_string(),
                },
            ],
            "library/subtitles/",
            "media-a",
        )
        .expect("compile rename rules");

        let renamed = super::apply_hierarchy_folder_rename_rules(
            "Artist - Title [rickroll].en.vtt",
            &rules,
            "library/subtitles/",
            "media-a",
            "subtitles",
        )
        .expect("apply rename rules");

        assert_eq!(renamed, "Artist - Title [rickroll].en.vtt");
    }

    /// Protects rename replacement semantics by treating `$0` as the entire
    /// matched path while still allowing `$1..$N` group rewrites.
    #[test]
    fn folder_rename_rules_support_dollar_zero_full_match_token() {
        let rules = super::compile_hierarchy_folder_rename_rules(
            &[HierarchyFolderRenameRule {
                pattern: "^(.+)$".to_string(),
                replacement: "mirror/$0".to_string(),
            }],
            "library/subtitles/",
            "media-a",
        )
        .expect("compile rename rules");

        let renamed = super::apply_hierarchy_folder_rename_rules(
            "captions.en.vtt",
            &rules,
            "library/subtitles/",
            "media-a",
            "subtitles",
        )
        .expect("apply rename rules");

        assert_eq!(renamed, "mirror/captions.en.vtt");
    }

    /// Protects folder-rename safety by rejecting traversal/escaping outputs.
    #[test]
    fn folder_rename_rules_reject_invalid_paths_after_rewrite() {
        let rules = super::compile_hierarchy_folder_rename_rules(
            &[HierarchyFolderRenameRule {
                pattern: "^(.+)$".to_string(),
                replacement: "../$1".to_string(),
            }],
            "library/subtitles/",
            "media-a",
        )
        .expect("compile rename rules");

        let error = super::apply_hierarchy_folder_rename_rules(
            "captions.en.vtt",
            &rules,
            "library/subtitles/",
            "media-a",
            "subtitles",
        )
        .expect_err("invalid rewritten path should fail");

        assert!(error.to_string().contains("invalid path"));
    }

    /// Protects sync wiring by ensuring folder rename rules rewrite extracted
    /// ZIP member file names before final materialization.
    #[tokio::test]
    async fn sync_hierarchy_applies_folder_rename_rules_to_zip_members() {
        let temp = tempfile::tempdir().expect("tempdir");
        let paths = MediaPmPaths::from_root(temp.path());
        let cas_root = paths.root_dir.join(".mediapm").join("store");
        let cas = FileSystemCas::open(&cas_root).await.expect("open cas");

        let zip_bytes = zip_payload(&[
            ("Rick Astley - Never Gonna Give You Up [dQw4w9WgXcQ].jpg", b"jpg"),
            ("Rick Astley - Never Gonna Give You Up [dQw4w9WgXcQ].url", b"[InternetShortcut]"),
        ]);
        let zip_hash = cas.put(zip_bytes).await.expect("put zip bytes");

        let document = MediaPmDocument {
            media: BTreeMap::from([(
                "rickroll".to_string(),
                MediaSourceSpec {
                    id: None,
                    description: Some("local sidecars".to_string()),
                    title: None,
                    workflow_id: None,
                    metadata: None,
                    variant_hashes: BTreeMap::from([(
                        "sidecars".to_string(),
                        zip_hash.to_string(),
                    )]),
                    steps: Vec::new(),
                },
            )]),
            hierarchy: hierarchy_nodes(BTreeMap::from([(
                "library/rickroll".to_string(),
                HierarchyEntry {
                    kind: HierarchyEntryKind::MediaFolder,
                    format: PlaylistFormat::M3u8,
                    ids: Vec::new(),
                    media_id: "rickroll".to_string(),
                    variants: vec!["sidecars".to_string()],
                    rename_files: vec![HierarchyFolderRenameRule {
                        pattern: "^Rick Astley - Never Gonna Give You Up \\[dQw4w9WgXcQ\\](\\..+)$"
                            .to_string(),
                        replacement: "Rick Astley - Never Gonna Give You Up [rickroll]$1"
                            .to_string(),
                    }],
                },
            )])),
            ..MediaPmDocument::default()
        };

        let mut lock = MediaLockFile::default();
        let report = sync_hierarchy(
            &paths,
            &document,
            &MachineNickelDocument::default(),
            &cas_root,
            &mut lock,
        )
        .await
        .expect("sync hierarchy");

        assert_eq!(report.materialized_paths, 1);
        assert!(
            paths
                .hierarchy_root_dir
                .join("library/rickroll/Rick Astley - Never Gonna Give You Up [rickroll].jpg")
                .is_file()
        );
        assert!(
            paths
                .hierarchy_root_dir
                .join("library/rickroll/Rick Astley - Never Gonna Give You Up [rickroll].url")
                .is_file()
        );
        assert!(
            !paths
                .hierarchy_root_dir
                .join("library/rickroll/Rick Astley - Never Gonna Give You Up [dQw4w9WgXcQ].jpg")
                .exists()
        );
        assert!(
            !lock.managed_files.contains_key("library/rickroll/"),
            "managed_files must track extracted files, not folder paths"
        );
        assert!(
            lock.managed_files.contains_key(
                "library/rickroll/Rick Astley - Never Gonna Give You Up [rickroll].jpg"
            )
        );
        assert!(
            lock.managed_files.contains_key(
                "library/rickroll/Rick Astley - Never Gonna Give You Up [rickroll].url"
            )
        );
    }

    /// Protects overlapping media-folder commits by ensuring parent-folder
    /// updates preserve previously materialized nested outputs.
    #[tokio::test]
    async fn sync_hierarchy_preserves_nested_outputs_when_parent_media_folder_commits_later() {
        let temp = tempfile::tempdir().expect("tempdir");
        let paths = MediaPmPaths::from_root(temp.path());
        let cas_root = paths.root_dir.join(".mediapm").join("store");
        let cas = FileSystemCas::open(&cas_root).await.expect("open cas");

        let sidecar_hash = cas
            .put(zip_payload(&[("info.json", br#"{"id":"demo"}"#)]))
            .await
            .expect("put sidecar zip");
        let root_hash =
            cas.put(zip_payload(&[("thumb.webp", b"webp")])).await.expect("put root zip");

        let document = MediaPmDocument {
            media: BTreeMap::from([(
                "media-a".to_string(),
                MediaSourceSpec {
                    id: None,
                    description: Some("local zip variants".to_string()),
                    title: None,
                    workflow_id: None,
                    metadata: None,
                    variant_hashes: BTreeMap::from([
                        ("sidecars".to_string(), sidecar_hash.to_string()),
                        ("root".to_string(), root_hash.to_string()),
                    ]),
                    steps: Vec::new(),
                },
            )]),
            hierarchy: vec![
                HierarchyNode {
                    path: "library/${media.id}/sidecars".to_string(),
                    kind: HierarchyNodeKind::MediaFolder,
                    id: Some("media-a-sidecars".to_string()),
                    media_id: Some("media-a".to_string()),
                    variant: None,
                    variants: vec!["sidecars".to_string()],
                    rename_files: Vec::new(),
                    format: PlaylistFormat::M3u8,
                    ids: Vec::new(),
                    children: Vec::new(),
                },
                HierarchyNode {
                    path: "library/${media.id}".to_string(),
                    kind: HierarchyNodeKind::MediaFolder,
                    id: Some("media-a-root".to_string()),
                    media_id: Some("media-a".to_string()),
                    variant: None,
                    variants: vec!["root".to_string()],
                    rename_files: Vec::new(),
                    format: PlaylistFormat::M3u8,
                    ids: Vec::new(),
                    children: Vec::new(),
                },
            ],
            ..MediaPmDocument::default()
        };

        let mut lock = MediaLockFile::default();
        let report = sync_hierarchy(
            &paths,
            &document,
            &MachineNickelDocument::default(),
            &cas_root,
            &mut lock,
        )
        .await
        .expect("sync hierarchy");

        assert_eq!(report.materialized_paths, 2);
        assert!(paths.hierarchy_root_dir.join("library/media-a/sidecars/info.json").is_file());
        assert!(paths.hierarchy_root_dir.join("library/media-a/thumb.webp").is_file());
        assert!(lock.managed_files.contains_key("library/media-a/sidecars/info.json"));
        assert!(lock.managed_files.contains_key("library/media-a/thumb.webp"));
    }

    /// Protects nested folder-merge semantics by preserving existing children
    /// when a later parent media-folder commit contributes overlapping
    /// directory names.
    #[tokio::test]
    async fn sync_hierarchy_preserves_nested_children_on_directory_name_collision() {
        let temp = tempfile::tempdir().expect("tempdir");
        let paths = MediaPmPaths::from_root(temp.path());
        let cas_root = paths.root_dir.join(".mediapm").join("store");
        let cas = FileSystemCas::open(&cas_root).await.expect("open cas");

        let sidecar_hash = cas
            .put(zip_payload(&[("info.json", br#"{"id":"demo"}"#)]))
            .await
            .expect("put sidecar zip");
        let root_hash = cas
            .put(zip_payload(&[
                ("sidecars/links.url", b"[InternetShortcut]"),
                ("thumb.webp", b"webp"),
            ]))
            .await
            .expect("put root zip");

        let document = MediaPmDocument {
            media: BTreeMap::from([(
                "media-a".to_string(),
                MediaSourceSpec {
                    id: None,
                    description: Some("local zip variants".to_string()),
                    title: None,
                    workflow_id: None,
                    metadata: None,
                    variant_hashes: BTreeMap::from([
                        ("sidecars".to_string(), sidecar_hash.to_string()),
                        ("root".to_string(), root_hash.to_string()),
                    ]),
                    steps: Vec::new(),
                },
            )]),
            hierarchy: vec![
                HierarchyNode {
                    path: "library/${media.id}/sidecars".to_string(),
                    kind: HierarchyNodeKind::MediaFolder,
                    id: Some("media-a-sidecars".to_string()),
                    media_id: Some("media-a".to_string()),
                    variant: None,
                    variants: vec!["sidecars".to_string()],
                    rename_files: Vec::new(),
                    format: PlaylistFormat::M3u8,
                    ids: Vec::new(),
                    children: Vec::new(),
                },
                HierarchyNode {
                    path: "library/${media.id}".to_string(),
                    kind: HierarchyNodeKind::MediaFolder,
                    id: Some("media-a-root".to_string()),
                    media_id: Some("media-a".to_string()),
                    variant: None,
                    variants: vec!["root".to_string()],
                    rename_files: Vec::new(),
                    format: PlaylistFormat::M3u8,
                    ids: Vec::new(),
                    children: Vec::new(),
                },
            ],
            ..MediaPmDocument::default()
        };

        let mut lock = MediaLockFile::default();
        let report = sync_hierarchy(
            &paths,
            &document,
            &MachineNickelDocument::default(),
            &cas_root,
            &mut lock,
        )
        .await
        .expect("sync hierarchy");

        assert_eq!(report.materialized_paths, 2);
        assert!(paths.hierarchy_root_dir.join("library/media-a/sidecars/info.json").is_file());
        assert!(paths.hierarchy_root_dir.join("library/media-a/sidecars/links.url").is_file());
        assert!(paths.hierarchy_root_dir.join("library/media-a/thumb.webp").is_file());
        assert!(lock.managed_files.contains_key("library/media-a/sidecars/info.json"));
        assert!(lock.managed_files.contains_key("library/media-a/sidecars/links.url"));
        assert!(lock.managed_files.contains_key("library/media-a/thumb.webp"));
    }

    /// Protects rename-rule replacement interpolation by supporting
    /// `${media.id}` and `${media.metadata.*}` placeholders.
    #[tokio::test]
    async fn sync_hierarchy_applies_folder_rename_replacement_media_placeholders() {
        let temp = tempfile::tempdir().expect("tempdir");
        let paths = MediaPmPaths::from_root(temp.path());
        let cas_root = paths.root_dir.join(".mediapm").join("store");
        let cas = FileSystemCas::open(&cas_root).await.expect("open cas");

        let zip_bytes = zip_payload(&[("thumb [video-id].jpg", b"jpg")]);
        let zip_hash = cas.put(zip_bytes).await.expect("put zip bytes");

        let document = MediaPmDocument {
            media: BTreeMap::from([(
                "media-a".to_string(),
                MediaSourceSpec {
                    id: None,
                    description: Some("local sidecars".to_string()),
                    title: None,
                    workflow_id: None,
                    metadata: Some(BTreeMap::from([(
                        "title".to_string(),
                        MediaMetadataValue::Literal("Demo Title".to_string()),
                    )])),
                    variant_hashes: BTreeMap::from([(
                        "sidecars".to_string(),
                        zip_hash.to_string(),
                    )]),
                    steps: Vec::new(),
                },
            )]),
            hierarchy: hierarchy_nodes(BTreeMap::from([(
                "library/renamed".to_string(),
                HierarchyEntry {
                    kind: HierarchyEntryKind::MediaFolder,
                    format: PlaylistFormat::M3u8,
                    ids: Vec::new(),
                    media_id: "media-a".to_string(),
                    variants: vec!["sidecars".to_string()],
                    rename_files: vec![HierarchyFolderRenameRule {
                        pattern: "^thumb \\[.+\\](\\.[^/\\\\]+)$".to_string(),
                        replacement: "${media.metadata.title} [${media.id}]$1".to_string(),
                    }],
                },
            )])),
            ..MediaPmDocument::default()
        };

        let mut lock = MediaLockFile::default();
        let report = sync_hierarchy(
            &paths,
            &document,
            &MachineNickelDocument::default(),
            &cas_root,
            &mut lock,
        )
        .await
        .expect("sync hierarchy");

        assert_eq!(report.materialized_paths, 1);
        assert!(
            paths.hierarchy_root_dir.join("library/renamed/Demo Title [media-a].jpg").is_file()
        );
        assert!(!paths.hierarchy_root_dir.join("library/renamed/thumb [video-id].jpg").exists());
    }

    /// Protects stage-commit materialization for local source entries.
    #[tokio::test]
    async fn sync_hierarchy_materializes_local_source_from_cas_variant_pointer() {
        let temp = tempfile::tempdir().expect("tempdir");
        let paths = MediaPmPaths::from_root(temp.path());
        let cas_root = paths.root_dir.join(".mediapm").join("store");
        let cas = FileSystemCas::open(&cas_root).await.expect("open cas");
        let hash = cas.put(b"abc".to_vec()).await.expect("put local bytes");

        let document = MediaPmDocument {
            media: BTreeMap::from([(
                "media-a".to_string(),
                MediaSourceSpec {
                    id: None,
                    description: Some("file: source.bin".to_string()),
                    title: None,
                    workflow_id: None,
                    metadata: None,
                    variant_hashes: BTreeMap::from([("default".to_string(), hash.to_string())]),
                    steps: Vec::new(),
                },
            )]),
            hierarchy: hierarchy_nodes(BTreeMap::from([(
                "library/media-a.bin".to_string(),
                HierarchyEntry {
                    kind: HierarchyEntryKind::Media,
                    format: PlaylistFormat::M3u8,
                    ids: Vec::new(),
                    media_id: "media-a".to_string(),
                    variants: vec!["default".to_string()],
                    rename_files: Vec::new(),
                },
            )])),
            ..MediaPmDocument::default()
        };

        let mut lock = MediaLockFile::default();
        let report = sync_hierarchy(
            &paths,
            &document,
            &MachineNickelDocument::default(),
            &cas_root,
            &mut lock,
        )
        .await
        .expect("sync hierarchy");

        assert_eq!(report.materialized_paths, 1);
        assert!(paths.hierarchy_root_dir.join("library/media-a.bin").exists());
        let metadata = std::fs::metadata(paths.hierarchy_root_dir.join("library/media-a.bin"))
            .expect("metadata");
        assert!(metadata.permissions().readonly(), "managed file should be readonly");
        assert_eq!(
            std::fs::read(paths.hierarchy_root_dir.join("library/media-a.bin"))
                .expect("read output"),
            b"abc"
        );
        let record = lock.managed_files.get("library/media-a.bin").expect("managed record");
        assert_eq!(record.media_id, "media-a");
        assert_eq!(record.hash, hash.to_string());

        let source_path = cas.object_path_for_hash(hash);
        let output_path = paths.hierarchy_root_dir.join("library/media-a.bin");
        assert_hardlinked_paths(&source_path, &output_path);
    }

    /// Protects playlist hierarchy generation by preserving declared id order,
    /// default relative path rendering, and explicit absolute-path overrides.
    #[tokio::test]
    #[allow(clippy::too_many_lines)]
    async fn sync_hierarchy_generates_playlist_with_relative_and_absolute_entries() {
        let temp = tempfile::tempdir().expect("tempdir");
        let paths = MediaPmPaths::from_root(temp.path());
        let cas_root = paths.root_dir.join(".mediapm").join("store");
        let cas = FileSystemCas::open(&cas_root).await.expect("open cas");
        let alpha_hash = cas.put(b"alpha".to_vec()).await.expect("put alpha bytes");
        let beta_hash = cas.put(b"beta".to_vec()).await.expect("put beta bytes");

        let document = MediaPmDocument {
            media: BTreeMap::from([
                (
                    "alpha-source".to_string(),
                    MediaSourceSpec {
                        id: None,
                        description: Some("file: alpha.bin".to_string()),
                        title: None,
                        workflow_id: None,
                        metadata: None,
                        variant_hashes: BTreeMap::from([(
                            "default".to_string(),
                            alpha_hash.to_string(),
                        )]),
                        steps: Vec::new(),
                    },
                ),
                (
                    "beta-source".to_string(),
                    MediaSourceSpec {
                        id: None,
                        description: Some("file: beta.bin".to_string()),
                        title: None,
                        workflow_id: None,
                        metadata: None,
                        variant_hashes: BTreeMap::from([(
                            "default".to_string(),
                            beta_hash.to_string(),
                        )]),
                        steps: Vec::new(),
                    },
                ),
            ]),
            hierarchy: hierarchy_nodes(BTreeMap::from([
                (
                    "library/music/alpha.mp3".to_string(),
                    HierarchyEntry {
                        kind: HierarchyEntryKind::Media,
                        format: PlaylistFormat::M3u8,
                        ids: Vec::new(),
                        media_id: "alpha-source".to_string(),
                        variants: vec!["default".to_string()],
                        rename_files: Vec::new(),
                    },
                ),
                (
                    "library/music/beta.mp3".to_string(),
                    HierarchyEntry {
                        kind: HierarchyEntryKind::Media,
                        format: PlaylistFormat::M3u8,
                        ids: Vec::new(),
                        media_id: "beta-source".to_string(),
                        variants: vec!["default".to_string()],
                        rename_files: Vec::new(),
                    },
                ),
                (
                    "library/playlists/demo.m3u8".to_string(),
                    HierarchyEntry {
                        kind: HierarchyEntryKind::Playlist,
                        format: PlaylistFormat::M3u8,
                        ids: vec![
                            PlaylistItemRef {
                                id: "alpha-source".to_string(),
                                path: PlaylistEntryPathMode::Relative,
                            },
                            PlaylistItemRef {
                                id: "beta-source".to_string(),
                                path: PlaylistEntryPathMode::Absolute,
                            },
                            PlaylistItemRef {
                                id: "alpha-source".to_string(),
                                path: PlaylistEntryPathMode::Relative,
                            },
                        ],
                        media_id: String::new(),
                        variants: Vec::new(),
                        rename_files: Vec::new(),
                    },
                ),
            ])),
            ..MediaPmDocument::default()
        };

        let mut lock = MediaLockFile::default();
        let report = sync_hierarchy(
            &paths,
            &document,
            &MachineNickelDocument::default(),
            &cas_root,
            &mut lock,
        )
        .await
        .expect("sync hierarchy");

        assert_eq!(report.materialized_paths, 3);

        let playlist_path = paths.hierarchy_root_dir.join("library/playlists/demo.m3u8");
        let playlist_text = std::fs::read_to_string(&playlist_path).expect("read playlist file");
        let expected_absolute_beta = paths
            .hierarchy_root_dir
            .join("library/music/beta.mp3")
            .to_string_lossy()
            .replace('\\', "/");
        let expected =
            format!("#EXTM3U\n../music/alpha.mp3\n{expected_absolute_beta}\n../music/alpha.mp3\n");
        assert_eq!(playlist_text, expected);

        let record =
            lock.managed_files.get("library/playlists/demo.m3u8").expect("playlist lock record");
        assert_eq!(record.media_id, "playlist");
        assert_eq!(record.variant, "playlist:m3u8");
        let playlist_hash = record.hash.parse::<Hash>().expect("playlist hash");
        let playlist_bytes_from_cas =
            cas.get(playlist_hash).await.expect("playlist bytes from cas");
        assert_eq!(playlist_bytes_from_cas.as_ref(), playlist_text.as_bytes());
    }

    /// Protects playlist id resolution by ensuring runtime uses
    /// `PlaylistItemRef.id` text as lookup key against media-node hierarchy
    /// `id` fields.
    #[tokio::test]
    #[allow(clippy::too_many_lines)]
    async fn sync_hierarchy_playlist_resolves_hierarchy_id_mapping() {
        let temp = tempfile::tempdir().expect("tempdir");
        let paths = MediaPmPaths::from_root(temp.path());
        let cas_root = paths.root_dir.join(".mediapm").join("store");
        let cas = FileSystemCas::open(&cas_root).await.expect("open cas");
        let alpha_hash = cas.put(b"alpha".to_vec()).await.expect("put alpha bytes");
        let beta_hash = cas.put(b"beta".to_vec()).await.expect("put beta bytes");

        let document = MediaPmDocument {
            media: BTreeMap::from([
                (
                    "alpha-source".to_string(),
                    MediaSourceSpec {
                        id: None,
                        description: Some("file: alpha.bin".to_string()),
                        title: None,
                        workflow_id: None,
                        metadata: None,
                        variant_hashes: BTreeMap::from([(
                            "default".to_string(),
                            alpha_hash.to_string(),
                        )]),
                        steps: Vec::new(),
                    },
                ),
                (
                    "beta-source".to_string(),
                    MediaSourceSpec {
                        id: None,
                        description: Some("file: beta.bin".to_string()),
                        title: None,
                        workflow_id: None,
                        metadata: None,
                        variant_hashes: BTreeMap::from([(
                            "default".to_string(),
                            beta_hash.to_string(),
                        )]),
                        steps: Vec::new(),
                    },
                ),
            ]),
            hierarchy: vec![
                crate::config::HierarchyNode {
                    path: "library/music/alpha.mp3".to_string(),
                    kind: HierarchyNodeKind::Media,
                    id: Some("alpha-playlist-id".to_string()),
                    media_id: Some("alpha-source".to_string()),
                    variant: Some("default".to_string()),
                    variants: Vec::new(),
                    rename_files: Vec::new(),
                    format: PlaylistFormat::M3u8,
                    ids: Vec::new(),
                    children: Vec::new(),
                },
                crate::config::HierarchyNode {
                    path: "library/music/beta.mp3".to_string(),
                    kind: HierarchyNodeKind::Media,
                    id: Some("beta-source".to_string()),
                    media_id: Some("beta-source".to_string()),
                    variant: Some("default".to_string()),
                    variants: Vec::new(),
                    rename_files: Vec::new(),
                    format: PlaylistFormat::M3u8,
                    ids: Vec::new(),
                    children: Vec::new(),
                },
                crate::config::HierarchyNode {
                    path: "library/playlists/mixed-ids.m3u8".to_string(),
                    kind: HierarchyNodeKind::Playlist,
                    id: None,
                    media_id: None,
                    variant: None,
                    variants: Vec::new(),
                    rename_files: Vec::new(),
                    format: PlaylistFormat::M3u8,
                    ids: vec![
                        PlaylistItemRef {
                            id: "alpha-playlist-id".to_string(),
                            path: PlaylistEntryPathMode::Relative,
                        },
                        PlaylistItemRef {
                            id: "beta-source".to_string(),
                            path: PlaylistEntryPathMode::Relative,
                        },
                    ],
                    children: Vec::new(),
                },
            ],
            ..MediaPmDocument::default()
        };

        let mut lock = MediaLockFile::default();
        let report = sync_hierarchy(
            &paths,
            &document,
            &MachineNickelDocument::default(),
            &cas_root,
            &mut lock,
        )
        .await
        .expect("sync hierarchy");

        assert_eq!(report.materialized_paths, 3);

        let playlist_path = paths.hierarchy_root_dir.join("library/playlists/mixed-ids.m3u8");
        let playlist_text = std::fs::read_to_string(&playlist_path).expect("read playlist file");
        assert_eq!(playlist_text, "#EXTM3U\n../music/alpha.mp3\n../music/beta.mp3\n");
    }

    /// Protects playlist safety by rejecting ids that do not resolve to
    /// media-file hierarchy nodes.
    #[tokio::test]
    async fn sync_hierarchy_playlist_rejects_non_media_hierarchy_id() {
        let temp = tempfile::tempdir().expect("tempdir");
        let paths = MediaPmPaths::from_root(temp.path());
        let cas_root = paths.root_dir.join(".mediapm").join("store");
        let cas = FileSystemCas::open(&cas_root).await.expect("open cas");
        let sidecar_zip_hash =
            cas.put(zip_payload(&[("captions.en.vtt", b"sub")])).await.expect("put sidecar zip");

        let document = MediaPmDocument {
            media: BTreeMap::from([(
                "folder-only".to_string(),
                MediaSourceSpec {
                    id: None,
                    description: Some("local sidecars".to_string()),
                    title: None,
                    workflow_id: None,
                    metadata: None,
                    variant_hashes: BTreeMap::from([(
                        "sidecars".to_string(),
                        sidecar_zip_hash.to_string(),
                    )]),
                    steps: Vec::new(),
                },
            )]),
            hierarchy: vec![
                crate::config::HierarchyNode {
                    path: "library/playlists/folder-only.m3u8".to_string(),
                    kind: HierarchyNodeKind::Playlist,
                    id: None,
                    media_id: None,
                    variant: None,
                    variants: Vec::new(),
                    rename_files: Vec::new(),
                    format: PlaylistFormat::M3u8,
                    ids: vec![PlaylistItemRef {
                        id: "folder-only".to_string(),
                        path: PlaylistEntryPathMode::Relative,
                    }],
                    children: Vec::new(),
                },
                crate::config::HierarchyNode {
                    path: "library/sidecars".to_string(),
                    kind: HierarchyNodeKind::MediaFolder,
                    id: Some("folder-only".to_string()),
                    media_id: Some("folder-only".to_string()),
                    variant: None,
                    variants: vec!["sidecars".to_string()],
                    rename_files: Vec::new(),
                    format: PlaylistFormat::M3u8,
                    ids: Vec::new(),
                    children: Vec::new(),
                },
            ],
            ..MediaPmDocument::default()
        };

        let mut lock = MediaLockFile::default();
        let error = sync_hierarchy(
            &paths,
            &document,
            &MachineNickelDocument::default(),
            &cas_root,
            &mut lock,
        )
        .await
        .expect_err("playlist references to non-media hierarchy ids must fail");

        assert!(error.to_string().contains("references unknown hierarchy id 'folder-only'"));
        assert!(!paths.hierarchy_root_dir.join("library/playlists/folder-only.m3u8").exists());
    }

    /// Protects playlist renderer support across configured common formats.
    #[test]
    fn render_playlist_bytes_supports_common_formats() {
        let items = vec![
            super::RenderedPlaylistItem {
                id: "alpha-id".to_string(),
                path: "../music/alpha.mp3".to_string(),
            },
            super::RenderedPlaylistItem {
                id: "beta-id".to_string(),
                path: "/library/music/beta.mp3".to_string(),
            },
        ];

        let m3u8 = String::from_utf8(super::render_playlist_bytes(PlaylistFormat::M3u8, &items))
            .expect("m3u8 should be utf-8");
        assert!(m3u8.starts_with("#EXTM3U\n"));

        let m3u = String::from_utf8(super::render_playlist_bytes(PlaylistFormat::M3u, &items))
            .expect("m3u should be utf-8");
        assert!(m3u.starts_with("#EXTM3U\n"));

        let pls = String::from_utf8(super::render_playlist_bytes(PlaylistFormat::Pls, &items))
            .expect("pls should be utf-8");
        assert!(pls.contains("[playlist]\n"));
        assert!(pls.contains("File1=../music/alpha.mp3\n"));
        assert!(pls.contains("NumberOfEntries=2\n"));

        let xspf = String::from_utf8(super::render_playlist_bytes(PlaylistFormat::Xspf, &items))
            .expect("xspf should be utf-8");
        assert!(xspf.contains("<playlist version=\"1\""));
        assert!(xspf.contains("<title>alpha-id</title>"));

        let wpl = String::from_utf8(super::render_playlist_bytes(PlaylistFormat::Wpl, &items))
            .expect("wpl should be utf-8");
        assert!(wpl.contains("<?wpl version=\"1.0\"?>"));
        assert!(wpl.contains("<media src=\"../music/alpha.mp3\" />"));

        let asx = String::from_utf8(super::render_playlist_bytes(PlaylistFormat::Asx, &items))
            .expect("asx should be utf-8");
        assert!(asx.contains("<asx version=\"3.0\">"));
        assert!(asx.contains("<title>alpha-id</title>"));
        assert!(asx.contains("<ref href=\"../music/alpha.mp3\" />"));
    }

    /// Protects non-default playlist materialization by rendering configured
    /// PLS output and recording playlist format provenance in the lockfile.
    #[tokio::test]
    async fn sync_hierarchy_generates_pls_playlist_and_records_format_label() {
        let temp = tempfile::tempdir().expect("tempdir");
        let paths = MediaPmPaths::from_root(temp.path());
        let cas_root = paths.root_dir.join(".mediapm").join("store");
        let cas = FileSystemCas::open(&cas_root).await.expect("open cas");
        let alpha_hash = cas.put(b"alpha".to_vec()).await.expect("put alpha bytes");

        let document = MediaPmDocument {
            media: BTreeMap::from([(
                "alpha-source".to_string(),
                MediaSourceSpec {
                    id: None,
                    description: Some("file: alpha.bin".to_string()),
                    title: None,
                    workflow_id: None,
                    metadata: None,
                    variant_hashes: BTreeMap::from([(
                        "default".to_string(),
                        alpha_hash.to_string(),
                    )]),
                    steps: Vec::new(),
                },
            )]),
            hierarchy: hierarchy_nodes(BTreeMap::from([
                (
                    "library/music/alpha.mp3".to_string(),
                    HierarchyEntry {
                        kind: HierarchyEntryKind::Media,
                        format: PlaylistFormat::M3u8,
                        ids: Vec::new(),
                        media_id: "alpha-source".to_string(),
                        variants: vec!["default".to_string()],
                        rename_files: Vec::new(),
                    },
                ),
                (
                    "library/playlists/demo.pls".to_string(),
                    HierarchyEntry {
                        kind: HierarchyEntryKind::Playlist,
                        format: PlaylistFormat::Pls,
                        ids: vec![
                            PlaylistItemRef {
                                id: "alpha-source".to_string(),
                                path: PlaylistEntryPathMode::Relative,
                            },
                            PlaylistItemRef {
                                id: "alpha-source".to_string(),
                                path: PlaylistEntryPathMode::Relative,
                            },
                        ],
                        media_id: String::new(),
                        variants: Vec::new(),
                        rename_files: Vec::new(),
                    },
                ),
            ])),
            ..MediaPmDocument::default()
        };

        let mut lock = MediaLockFile::default();
        let report = sync_hierarchy(
            &paths,
            &document,
            &MachineNickelDocument::default(),
            &cas_root,
            &mut lock,
        )
        .await
        .expect("sync hierarchy");

        assert_eq!(report.materialized_paths, 2);

        let playlist_path = paths.hierarchy_root_dir.join("library/playlists/demo.pls");
        let playlist_text = std::fs::read_to_string(&playlist_path).expect("read playlist file");
        assert!(playlist_text.starts_with("[playlist]\n"));
        assert!(playlist_text.contains("File1=../music/alpha.mp3\n"));
        assert!(playlist_text.contains("File2=../music/alpha.mp3\n"));
        assert!(playlist_text.contains("Title1=alpha-source\n"));
        assert!(playlist_text.contains("Title2=alpha-source\n"));
        assert!(playlist_text.contains("NumberOfEntries=2\n"));
        assert!(playlist_text.ends_with("Version=2\n"));

        let record =
            lock.managed_files.get("library/playlists/demo.pls").expect("playlist lock record");
        assert_eq!(record.media_id, "playlist");
        assert_eq!(record.variant, "playlist:pls");
        let playlist_hash = record.hash.parse::<Hash>().expect("playlist hash");
        let playlist_bytes_from_cas =
            cas.get(playlist_hash).await.expect("playlist bytes from cas");
        assert_eq!(playlist_bytes_from_cas.as_ref(), playlist_text.as_bytes());
    }

    /// Protects hierarchy placeholder interpolation for literal metadata values.
    #[tokio::test]
    async fn sync_hierarchy_interpolates_literal_media_metadata_placeholders() {
        let temp = tempfile::tempdir().expect("tempdir");
        let paths = MediaPmPaths::from_root(temp.path());
        let cas_root = paths.root_dir.join(".mediapm").join("store");
        let cas = FileSystemCas::open(&cas_root).await.expect("open cas");
        let hash = cas.put(b"abc".to_vec()).await.expect("put local bytes");

        let document = MediaPmDocument {
            media: BTreeMap::from([(
                "media-a".to_string(),
                MediaSourceSpec {
                    id: None,
                    description: Some("file: source.bin".to_string()),
                    title: None,
                    workflow_id: None,
                    metadata: Some(BTreeMap::from([(
                        "title".to_string(),
                        MediaMetadataValue::Literal("Demo Title".to_string()),
                    )])),
                    variant_hashes: BTreeMap::from([("default".to_string(), hash.to_string())]),
                    steps: Vec::new(),
                },
            )]),
            hierarchy: hierarchy_nodes(BTreeMap::from([(
                "library/${media.metadata.title}.bin".to_string(),
                HierarchyEntry {
                    kind: HierarchyEntryKind::Media,
                    format: PlaylistFormat::M3u8,
                    ids: Vec::new(),
                    media_id: "media-a".to_string(),
                    variants: vec!["default".to_string()],
                    rename_files: Vec::new(),
                },
            )])),
            ..MediaPmDocument::default()
        };

        let mut lock = MediaLockFile::default();
        let report = sync_hierarchy(
            &paths,
            &document,
            &MachineNickelDocument::default(),
            &cas_root,
            &mut lock,
        )
        .await
        .expect("sync hierarchy");

        assert_eq!(report.materialized_paths, 1);
        assert!(paths.hierarchy_root_dir.join("library/Demo Title.bin").exists());
    }

    /// Protects hierarchy placeholder interpolation for variant-backed
    /// metadata extraction from JSON sidecar payloads.
    #[tokio::test]
    async fn sync_hierarchy_interpolates_variant_backed_media_metadata_placeholders() {
        let temp = tempfile::tempdir().expect("tempdir");
        let paths = MediaPmPaths::from_root(temp.path());
        let cas_root = paths.root_dir.join(".mediapm").join("store");
        let cas = FileSystemCas::open(&cas_root).await.expect("open cas");
        let audio_hash = cas.put(b"audio-bytes".to_vec()).await.expect("put audio bytes");
        let infojson_hash =
            cas.put(br#"{"title":"Variant Title"}"#.to_vec()).await.expect("put infojson bytes");

        let document = MediaPmDocument {
            media: BTreeMap::from([(
                "media-a".to_string(),
                MediaSourceSpec {
                    id: None,
                    description: Some("file: source.bin".to_string()),
                    title: None,
                    workflow_id: None,
                    metadata: Some(BTreeMap::from([(
                        "title".to_string(),
                        MediaMetadataValue::Variant(MediaMetadataVariantBinding {
                            variant: "infojson".to_string(),
                            metadata_key: "title".to_string(),
                            transform: None,
                        }),
                    )])),
                    variant_hashes: BTreeMap::from([
                        ("audio".to_string(), audio_hash.to_string()),
                        ("infojson".to_string(), infojson_hash.to_string()),
                    ]),
                    steps: Vec::new(),
                },
            )]),
            hierarchy: hierarchy_nodes(BTreeMap::from([(
                "library/${media.metadata.title}.bin".to_string(),
                HierarchyEntry {
                    kind: HierarchyEntryKind::Media,
                    format: PlaylistFormat::M3u8,
                    ids: Vec::new(),
                    media_id: "media-a".to_string(),
                    variants: vec!["audio".to_string()],
                    rename_files: Vec::new(),
                },
            )])),
            ..MediaPmDocument::default()
        };

        let mut lock = MediaLockFile::default();
        let report = sync_hierarchy(
            &paths,
            &document,
            &MachineNickelDocument::default(),
            &cas_root,
            &mut lock,
        )
        .await
        .expect("sync hierarchy");

        assert_eq!(report.materialized_paths, 1);
        assert!(paths.hierarchy_root_dir.join("library/Variant Title.bin").exists());
        assert_eq!(
            std::fs::read(paths.hierarchy_root_dir.join("library/Variant Title.bin"))
                .expect("read output"),
            b"audio-bytes"
        );
    }

    /// Protects hierarchy placeholder interpolation for `${media.id}`.
    #[tokio::test]
    async fn sync_hierarchy_interpolates_media_id_placeholder() {
        let temp = tempfile::tempdir().expect("tempdir");
        let paths = MediaPmPaths::from_root(temp.path());
        let cas_root = paths.root_dir.join(".mediapm").join("store");
        let cas = FileSystemCas::open(&cas_root).await.expect("open cas");
        let hash = cas.put(b"abc".to_vec()).await.expect("put local bytes");

        let document = MediaPmDocument {
            media: BTreeMap::from([(
                "media-a".to_string(),
                MediaSourceSpec {
                    id: None,
                    description: Some("file: source.bin".to_string()),
                    title: None,
                    workflow_id: None,
                    metadata: None,
                    variant_hashes: BTreeMap::from([("default".to_string(), hash.to_string())]),
                    steps: Vec::new(),
                },
            )]),
            hierarchy: hierarchy_nodes(BTreeMap::from([(
                "library/${media.id}/output.bin".to_string(),
                HierarchyEntry {
                    kind: HierarchyEntryKind::Media,
                    format: PlaylistFormat::M3u8,
                    ids: Vec::new(),
                    media_id: "media-a".to_string(),
                    variants: vec!["default".to_string()],
                    rename_files: Vec::new(),
                },
            )])),
            ..MediaPmDocument::default()
        };

        let mut lock = MediaLockFile::default();
        let report = sync_hierarchy(
            &paths,
            &document,
            &MachineNickelDocument::default(),
            &cas_root,
            &mut lock,
        )
        .await
        .expect("sync hierarchy");

        assert_eq!(report.materialized_paths, 1);
        assert!(paths.hierarchy_root_dir.join("library/media-a/output.bin").exists());
    }

    /// Protects metadata extension interpolation by applying full-match regex
    /// transforms with capture groups.
    #[tokio::test]
    async fn sync_hierarchy_interpolates_variant_metadata_with_dot_prefix() {
        let temp = tempfile::tempdir().expect("tempdir");
        let paths = MediaPmPaths::from_root(temp.path());
        let cas_root = paths.root_dir.join(".mediapm").join("store");
        let cas = FileSystemCas::open(&cas_root).await.expect("open cas");
        let audio_hash = cas.put(b"audio-bytes".to_vec()).await.expect("put audio bytes");
        let infojson_hash =
            cas.put(br#"{"ext":"mkv"}"#.to_vec()).await.expect("put infojson bytes");

        let document = MediaPmDocument {
            media: BTreeMap::from([(
                "media-a".to_string(),
                MediaSourceSpec {
                    id: None,
                    description: Some("file: source.bin".to_string()),
                    title: None,
                    workflow_id: None,
                    metadata: Some(BTreeMap::from([(
                        "video_ext".to_string(),
                        MediaMetadataValue::Variant(MediaMetadataVariantBinding {
                            variant: "infojson".to_string(),
                            metadata_key: "ext".to_string(),
                            transform: Some(MediaMetadataRegexTransform {
                                pattern: "(.+)".to_string(),
                                replacement: ".$0".to_string(),
                            }),
                        }),
                    )])),
                    variant_hashes: BTreeMap::from([
                        ("audio".to_string(), audio_hash.to_string()),
                        ("infojson".to_string(), infojson_hash.to_string()),
                    ]),
                    steps: Vec::new(),
                },
            )]),
            hierarchy: hierarchy_nodes(BTreeMap::from([(
                "library/output${media.metadata.video_ext}".to_string(),
                HierarchyEntry {
                    kind: HierarchyEntryKind::Media,
                    format: PlaylistFormat::M3u8,
                    ids: Vec::new(),
                    media_id: "media-a".to_string(),
                    variants: vec!["audio".to_string()],
                    rename_files: Vec::new(),
                },
            )])),
            ..MediaPmDocument::default()
        };

        let mut lock = MediaLockFile::default();
        let report = sync_hierarchy(
            &paths,
            &document,
            &MachineNickelDocument::default(),
            &cas_root,
            &mut lock,
        )
        .await
        .expect("sync hierarchy");

        assert_eq!(report.materialized_paths, 1);
        assert!(paths.hierarchy_root_dir.join("library/output.mkv").exists());
    }

    /// Protects optional metadata transform behavior by allowing empty values
    /// to pass through unchanged when replacement omits dot-prefixing.
    #[tokio::test]
    async fn sync_hierarchy_interpolates_empty_variant_metadata_without_dot_prefix() {
        let temp = tempfile::tempdir().expect("tempdir");
        let paths = MediaPmPaths::from_root(temp.path());
        let cas_root = paths.root_dir.join(".mediapm").join("store");
        let cas = FileSystemCas::open(&cas_root).await.expect("open cas");
        let audio_hash = cas.put(b"audio-bytes".to_vec()).await.expect("put audio bytes");
        let infojson_hash = cas.put(br#"{"ext":""}"#.to_vec()).await.expect("put infojson bytes");

        let document = MediaPmDocument {
            media: BTreeMap::from([(
                "media-a".to_string(),
                MediaSourceSpec {
                    id: None,
                    description: Some("file: source.bin".to_string()),
                    title: None,
                    workflow_id: None,
                    metadata: Some(BTreeMap::from([(
                        "video_ext".to_string(),
                        MediaMetadataValue::Variant(MediaMetadataVariantBinding {
                            variant: "infojson".to_string(),
                            metadata_key: "ext".to_string(),
                            transform: Some(MediaMetadataRegexTransform {
                                pattern: "(.*)".to_string(),
                                replacement: "$0".to_string(),
                            }),
                        }),
                    )])),
                    variant_hashes: BTreeMap::from([
                        ("audio".to_string(), audio_hash.to_string()),
                        ("infojson".to_string(), infojson_hash.to_string()),
                    ]),
                    steps: Vec::new(),
                },
            )]),
            hierarchy: hierarchy_nodes(BTreeMap::from([(
                "library/output${media.metadata.video_ext}".to_string(),
                HierarchyEntry {
                    kind: HierarchyEntryKind::Media,
                    format: PlaylistFormat::M3u8,
                    ids: Vec::new(),
                    media_id: "media-a".to_string(),
                    variants: vec!["audio".to_string()],
                    rename_files: Vec::new(),
                },
            )])),
            ..MediaPmDocument::default()
        };

        let mut lock = MediaLockFile::default();
        let report = sync_hierarchy(
            &paths,
            &document,
            &MachineNickelDocument::default(),
            &cas_root,
            &mut lock,
        )
        .await
        .expect("sync hierarchy");

        assert_eq!(report.materialized_paths, 1);
        assert!(paths.hierarchy_root_dir.join("library/output").exists());
    }

    /// Protects stale-path removal for readonly managed files on Windows/Linux.
    #[test]
    fn remove_path_handles_readonly_files() {
        let temp = tempfile::tempdir().expect("tempdir");
        let file_path = temp.path().join("readonly.txt");
        std::fs::write(&file_path, b"x").expect("write file");

        let mut permissions = std::fs::metadata(&file_path).expect("metadata").permissions();
        permissions.set_readonly(true);
        std::fs::set_permissions(&file_path, permissions).expect("set readonly");

        super::remove_path(&file_path).expect("remove readonly file");
        assert!(!file_path.exists());
    }

    /// Protects online-source materialization by resolving workflow output hashes
    /// from persisted orchestration state instead of writing placeholders.
    #[tokio::test]
    #[expect(
        clippy::too_many_lines,
        reason = "this item intentionally keeps end-to-end control flow together so ordering invariants remain explicit during maintenance"
    )]
    async fn sync_hierarchy_materializes_online_variant_from_workflow_state() {
        let temp = tempfile::tempdir().expect("tempdir");
        let paths = MediaPmPaths::from_root(temp.path());
        let cas_root = paths.root_dir.join(".mediapm").join("store");
        let cas = FileSystemCas::open(&cas_root).await.expect("open cas");

        let media_id = "remote-a";
        let source_uri = "https://example.com/audio";
        let output_bytes = b"ID3workflow-output".to_vec();
        let output_hash = cas.put(output_bytes.clone()).await.expect("put output bytes");

        let tool_id = "mediapm.tools.yt-dlp+github-releases-yt-dlp-yt-dlp@latest".to_string();
        let tool_spec = ToolSpec {
            kind: ToolKindSpec::Executable {
                command: vec!["yt-dlp.exe".to_string()],
                env_vars: BTreeMap::new(),
                success_codes: vec![0],
            },
            inputs: BTreeMap::from([
                ("source_url".to_string(), ToolInputSpec::default()),
                ("leading_args".to_string(), ToolInputSpec { kind: ToolInputKind::StringList }),
                ("trailing_args".to_string(), ToolInputSpec { kind: ToolInputKind::StringList }),
            ]),
            outputs: BTreeMap::from([(
                "primary".to_string(),
                ToolOutputSpec {
                    allow_empty: false,
                    capture: OutputCaptureSpec::File {
                        path: "downloads/yt-dlp-output.media".to_string(),
                    },
                },
            )]),
            ..ToolSpec::default()
        };

        let step_id = "0-0-yt-dlp".to_string();
        let workflow = WorkflowSpec {
            name: Some(media_id.to_string()),
            description: Some("online source".to_string()),
            steps: vec![WorkflowStepSpec {
                id: step_id.clone(),
                tool: tool_id.clone(),
                inputs: BTreeMap::from([
                    ("source_url".to_string(), InputBinding::String(source_uri.to_string())),
                    (
                        "leading_args".to_string(),
                        InputBinding::StringList(vec![
                            "--format".to_string(),
                            "bestaudio/best".to_string(),
                        ]),
                    ),
                    ("trailing_args".to_string(), InputBinding::StringList(Vec::new())),
                ]),
                depends_on: Vec::new(),
                outputs: BTreeMap::new(),
            }],
        };

        let mut machine = MachineNickelDocument::default();
        machine.tools.insert(tool_id.clone(), tool_spec.clone());
        let workflow_id = crate::conductor_bridge::managed_workflow_id_for_media(
            media_id,
            &MediaSourceSpec {
                id: None,
                description: Some("online source".to_string()),
                title: None,
                workflow_id: None,
                metadata: None,
                variant_hashes: BTreeMap::new(),
                steps: vec![MediaStep {
                    tool: MediaStepTool::YtDlp,
                    input_variants: Vec::new(),
                    output_variants: BTreeMap::from([(
                        "normalized".to_string(),
                        yt_dlp_output_variant("primary"),
                    )]),
                    options: BTreeMap::from([
                        ("uri".to_string(), TransformInputValue::String(source_uri.to_string())),
                        (
                            "leading_args".to_string(),
                            TransformInputValue::StringList(vec![
                                "--format".to_string(),
                                "bestaudio/best".to_string(),
                            ]),
                        ),
                    ]),
                }],
            },
        );
        machine.workflows.insert(workflow_id, workflow);

        let instance_inputs = BTreeMap::from([
            (
                "source_url".to_string(),
                mediapm_conductor::ResolvedInput::from_hash(Hash::from_content(
                    source_uri.as_bytes(),
                )),
            ),
            (
                "leading_args".to_string(),
                mediapm_conductor::ResolvedInput::from_string_list(vec![
                    "--format".to_string(),
                    "bestaudio/best".to_string(),
                ])
                .expect("list hash"),
            ),
            (
                "trailing_args".to_string(),
                mediapm_conductor::ResolvedInput::from_string_list(Vec::new())
                    .expect("empty list hash"),
            ),
        ]);

        let state = OrchestrationState {
            version: 1,
            instances: BTreeMap::from([(
                "instance-a".to_string(),
                mediapm_conductor::ToolCallInstance {
                    tool_name: tool_id.clone(),
                    metadata: tool_spec,
                    impure_timestamp: None,
                    inputs: instance_inputs,
                    outputs: BTreeMap::from([(
                        "primary".to_string(),
                        OutputRef {
                            allow_empty_capture: false,
                            hash: output_hash,
                            persistence: PersistenceFlags::default(),
                        },
                    )]),
                },
            )]),
        };

        let state_blob = serde_json::to_vec(&state).expect("encode state blob");
        let state_pointer = cas.put(state_blob).await.expect("put state blob");
        let encoded_state_document = encode_state_document(StateNickelDocument {
            impure_timestamps: BTreeMap::new(),
            state_pointer: Some(state_pointer),
        })
        .expect("encode state document");

        std::fs::create_dir_all(paths.conductor_state_config.parent().expect("state parent"))
            .expect("create state parent");
        std::fs::write(&paths.conductor_state_config, encoded_state_document)
            .expect("write state document");

        let source = MediaSourceSpec {
            id: None,
            description: Some("online source".to_string()),
            title: None,
            workflow_id: None,
            metadata: None,
            variant_hashes: BTreeMap::new(),
            steps: vec![MediaStep {
                tool: MediaStepTool::YtDlp,
                input_variants: Vec::new(),
                output_variants: BTreeMap::from([(
                    "normalized".to_string(),
                    yt_dlp_output_variant("primary"),
                )]),
                options: BTreeMap::from([
                    ("uri".to_string(), TransformInputValue::String(source_uri.to_string())),
                    (
                        "leading_args".to_string(),
                        TransformInputValue::StringList(vec![
                            "--format".to_string(),
                            "bestaudio/best".to_string(),
                        ]),
                    ),
                ]),
            }],
        };

        let binding =
            crate::conductor_bridge::resolve_media_variant_output_binding(&source, "normalized")
                .expect("resolve variant binding")
                .expect("binding exists");
        assert_eq!(binding.step_id, step_id);
        assert_eq!(binding.output_name, "primary");

        let document = MediaPmDocument {
            media: BTreeMap::from([(media_id.to_string(), source)]),
            hierarchy: hierarchy_nodes(BTreeMap::from([(
                "demo/online.bin".to_string(),
                HierarchyEntry {
                    kind: HierarchyEntryKind::Media,
                    format: PlaylistFormat::M3u8,
                    ids: Vec::new(),
                    media_id: media_id.to_string(),
                    variants: vec!["normalized".to_string()],
                    rename_files: Vec::new(),
                },
            )])),
            ..MediaPmDocument::default()
        };

        let mut lock = MediaLockFile::default();
        let report = sync_hierarchy(&paths, &document, &machine, &cas_root, &mut lock)
            .await
            .expect("sync hierarchy");

        assert_eq!(report.materialized_paths, 1);
        assert!(report.notices.is_empty());
        assert_eq!(
            std::fs::read(paths.hierarchy_root_dir.join("demo/online.bin")).expect("read output"),
            output_bytes
        );
    }

    /// Protects strict local-source materialization by failing when the
    /// declared CAS hash pointer is unavailable.
    #[tokio::test]
    async fn sync_hierarchy_fails_when_local_variant_hash_is_missing_from_cas() {
        let temp = tempfile::tempdir().expect("tempdir");
        let paths = MediaPmPaths::from_root(temp.path());
        let cas_root = paths.root_dir.join(".mediapm").join("store");

        let missing_hash = Hash::from_content(b"missing-local-payload");
        let document = MediaPmDocument {
            media: BTreeMap::from([(
                "local-missing".to_string(),
                MediaSourceSpec {
                    id: None,
                    description: Some("local source".to_string()),
                    title: None,
                    workflow_id: None,
                    metadata: None,
                    variant_hashes: BTreeMap::from([(
                        "default".to_string(),
                        missing_hash.to_string(),
                    )]),
                    steps: Vec::new(),
                },
            )]),
            hierarchy: hierarchy_nodes(BTreeMap::from([(
                "demo/local.bin".to_string(),
                HierarchyEntry {
                    kind: HierarchyEntryKind::Media,
                    format: PlaylistFormat::M3u8,
                    ids: Vec::new(),
                    media_id: "local-missing".to_string(),
                    variants: vec!["default".to_string()],
                    rename_files: Vec::new(),
                },
            )])),
            ..MediaPmDocument::default()
        };

        let mut lock = MediaLockFile::default();
        let error = sync_hierarchy(
            &paths,
            &document,
            &MachineNickelDocument::default(),
            &cas_root,
            &mut lock,
        )
        .await
        .expect_err("missing local CAS payload must fail materialization");

        let error_text = error.to_string();
        assert!(error_text.contains("variant 'default'"), "unexpected error: {error_text}");
        assert!(error_text.contains("missing") || error_text.contains("not found"));
        assert!(!paths.hierarchy_root_dir.join("demo/local.bin").exists());
    }

    /// Protects strict online-source materialization by failing when no
    /// workflow output hash can be resolved from runtime state.
    #[tokio::test]
    async fn sync_hierarchy_fails_when_online_variant_hash_is_unresolved() {
        let temp = tempfile::tempdir().expect("tempdir");
        let paths = MediaPmPaths::from_root(temp.path());
        let cas_root = paths.root_dir.join(".mediapm").join("store");

        let source_uri = "https://example.com/audio";
        let document = MediaPmDocument {
            media: BTreeMap::from([(
                "remote-unresolved".to_string(),
                MediaSourceSpec {
                    id: None,
                    description: Some("online source".to_string()),
                    title: None,
                    workflow_id: None,
                    metadata: None,
                    variant_hashes: BTreeMap::new(),
                    steps: vec![MediaStep {
                        tool: MediaStepTool::YtDlp,
                        input_variants: Vec::new(),
                        output_variants: BTreeMap::from([(
                            "normalized".to_string(),
                            yt_dlp_output_variant("primary"),
                        )]),
                        options: BTreeMap::from([(
                            "uri".to_string(),
                            TransformInputValue::String(source_uri.to_string()),
                        )]),
                    }],
                },
            )]),
            hierarchy: hierarchy_nodes(BTreeMap::from([(
                "demo/online.bin".to_string(),
                HierarchyEntry {
                    kind: HierarchyEntryKind::Media,
                    format: PlaylistFormat::M3u8,
                    ids: Vec::new(),
                    media_id: "remote-unresolved".to_string(),
                    variants: vec!["normalized".to_string()],
                    rename_files: Vec::new(),
                },
            )])),
            ..MediaPmDocument::default()
        };

        let mut lock = MediaLockFile::default();
        let error = sync_hierarchy(
            &paths,
            &document,
            &MachineNickelDocument::default(),
            &cas_root,
            &mut lock,
        )
        .await
        .expect_err("online source without resolved workflow output hash must fail");

        let error_text = error.to_string();
        assert!(error_text.contains(source_uri), "unexpected error: {error_text}");
        assert!(error_text.contains("runtime state") || error_text.contains("workflow"));
        assert!(!paths.hierarchy_root_dir.join("demo/online.bin").exists());
    }

    /// Protects runtime-instance matching by allowing extra runtime-injected
    /// default inputs while still requiring all step-declared input hashes.
    #[test]
    fn instance_matching_allows_extra_runtime_inputs() {
        let expected_text_hash = Hash::from_content(b"hello");
        let expected = super::ExpectedStepInputs {
            resolved_hashes: BTreeMap::from([("text".to_string(), expected_text_hash)]),
            unresolved_hash_input_names: BTreeSet::new(),
        };

        let instance = ToolCallInstance {
            tool_name: "echo@1.0.0".to_string(),
            metadata: ToolSpec {
                kind: ToolKindSpec::Builtin {
                    name: "echo".to_string(),
                    version: "1.0.0".to_string(),
                },
                ..ToolSpec::default()
            },
            impure_timestamp: None,
            inputs: BTreeMap::from([
                (
                    "text".to_string(),
                    mediapm_conductor::ResolvedInput::from_hash(expected_text_hash),
                ),
                (
                    "leading_args".to_string(),
                    mediapm_conductor::ResolvedInput::from_string_list(vec![
                        "--verbose".to_string(),
                    ])
                    .expect("list hash"),
                ),
            ]),
            outputs: BTreeMap::new(),
        };

        assert!(instance_matches_expected_inputs(&instance, &expected));
    }

    /// Protects runtime step resolution against equivalent-call collisions by
    /// requiring matched instances to expose step-declared output names.
    #[tokio::test]
    #[expect(
        clippy::too_many_lines,
        reason = "this item intentionally keeps end-to-end control flow together so ordering invariants remain explicit during maintenance"
    )]
    async fn resolve_step_output_hashes_matches_instance_with_expected_output_names() {
        let temp = tempfile::tempdir().expect("tempdir");
        let cas = FileSystemCas::open(temp.path()).await.expect("open cas");

        let tool_id = "mediapm.tools.yt-dlp+github-releases-yt-dlp-yt-dlp@latest".to_string();
        let tool_spec = ToolSpec {
            kind: ToolKindSpec::Executable {
                command: vec!["yt-dlp.exe".to_string()],
                env_vars: BTreeMap::new(),
                success_codes: vec![0],
            },
            inputs: BTreeMap::from([("source_url".to_string(), ToolInputSpec::default())]),
            outputs: BTreeMap::from([
                (
                    "content".to_string(),
                    ToolOutputSpec {
                        allow_empty: false,
                        capture: OutputCaptureSpec::File {
                            path: "downloads/yt-dlp-output.media".to_string(),
                        },
                    },
                ),
                (
                    "yt_dlp_thumbnail_artifacts".to_string(),
                    ToolOutputSpec {
                        allow_empty: false,
                        capture: OutputCaptureSpec::Folder {
                            path: "downloads".to_string(),
                            include_topmost_folder: false,
                        },
                    },
                ),
            ]),
            ..ToolSpec::default()
        };

        let step_id = "step-0-primary".to_string();
        let source_url = "https://example.com/video".to_string();
        let source_url_hash = Hash::from_content(source_url.as_bytes());

        let workflow = WorkflowSpec {
            name: Some("demo".to_string()),
            description: None,
            steps: vec![WorkflowStepSpec {
                id: step_id.clone(),
                tool: tool_id.clone(),
                inputs: BTreeMap::from([(
                    "source_url".to_string(),
                    InputBinding::String(source_url.clone()),
                )]),
                depends_on: Vec::new(),
                outputs: BTreeMap::from([(
                    "content".to_string(),
                    OutputPolicy { save: Some(OutputSaveMode::Full) },
                )]),
            }],
        };

        let mut machine = MachineNickelDocument::default();
        machine.tools.insert(tool_id.clone(), tool_spec.clone());

        let state = OrchestrationState {
            version: 1,
            instances: BTreeMap::from([
                (
                    "a-thumbnail-first".to_string(),
                    ToolCallInstance {
                        tool_name: tool_id.clone(),
                        metadata: tool_spec.clone(),
                        impure_timestamp: None,
                        inputs: BTreeMap::from([(
                            "source_url".to_string(),
                            mediapm_conductor::ResolvedInput::from_hash(source_url_hash),
                        )]),
                        outputs: BTreeMap::from([(
                            "yt_dlp_thumbnail_artifacts".to_string(),
                            OutputRef {
                                allow_empty_capture: false,
                                hash: Hash::from_content(b"thumb-zip"),
                                persistence: PersistenceFlags::default(),
                            },
                        )]),
                    },
                ),
                (
                    "z-primary-second".to_string(),
                    ToolCallInstance {
                        tool_name: tool_id,
                        metadata: tool_spec,
                        impure_timestamp: None,
                        inputs: BTreeMap::from([(
                            "source_url".to_string(),
                            mediapm_conductor::ResolvedInput::from_hash(source_url_hash),
                        )]),
                        outputs: BTreeMap::from([(
                            "content".to_string(),
                            OutputRef {
                                allow_empty_capture: false,
                                hash: Hash::from_content(b"primary-media"),
                                persistence: PersistenceFlags::default(),
                            },
                        )]),
                    },
                ),
            ]),
        };

        let step_output_hashes =
            super::resolve_workflow_step_output_hashes(&cas, &machine, &state, &workflow)
                .await
                .expect("resolve step outputs")
                .expect("step outputs should resolve");

        let output_hash = step_output_hashes
            .get(&step_id)
            .and_then(|outputs| outputs.get("content"))
            .copied()
            .expect("content hash should resolve from primary instance");
        assert_eq!(output_hash, Hash::from_content(b"primary-media"));
    }

    /// Protects ZIP-selector materialization by skipping stale matching
    /// instances whose required step outputs are missing from CAS.
    #[tokio::test]
    #[expect(
        clippy::too_many_lines,
        reason = "this item intentionally keeps end-to-end control flow together so ordering invariants remain explicit during maintenance"
    )]
    async fn resolve_step_output_hashes_prefers_materializable_zip_selector_instance() {
        let temp = tempfile::tempdir().expect("tempdir");
        let cas = FileSystemCas::open(temp.path()).await.expect("open cas");

        let tagger_tool_id = "mediapm.tools.media-tagger@latest".to_string();
        let tagger_tool_spec = ToolSpec {
            kind: ToolKindSpec::Executable {
                command: vec!["media-tagger.exe".to_string()],
                env_vars: BTreeMap::new(),
                success_codes: vec![0],
            },
            inputs: BTreeMap::from([("input_content".to_string(), ToolInputSpec::default())]),
            outputs: BTreeMap::from([(
                "sandbox_artifacts".to_string(),
                ToolOutputSpec {
                    allow_empty: false,
                    capture: OutputCaptureSpec::Folder {
                        path: "sandbox".to_string(),
                        include_topmost_folder: false,
                    },
                },
            )]),
            ..ToolSpec::default()
        };

        let apply_tool_id = "mediapm.tools.ffmpeg@latest".to_string();
        let apply_tool_spec = ToolSpec {
            kind: ToolKindSpec::Executable {
                command: vec!["ffmpeg.exe".to_string()],
                env_vars: BTreeMap::new(),
                success_codes: vec![0],
            },
            inputs: BTreeMap::from([("cover_flag".to_string(), ToolInputSpec::default())]),
            outputs: BTreeMap::from([(
                "content".to_string(),
                ToolOutputSpec {
                    allow_empty: false,
                    capture: OutputCaptureSpec::File { path: "output.media".to_string() },
                },
            )]),
            ..ToolSpec::default()
        };

        let step_tagger_id = "step-0-media-tagger".to_string();
        let step_apply_id = "step-1-ffmpeg".to_string();
        let input_hash = Hash::from_content(b"tagger-input");

        let required_member = "coverart-slot-0.flag";
        let member_bytes = b"coverart-present";
        let valid_zip_hash = cas
            .put(zip_payload(&[(required_member, member_bytes.as_slice())]))
            .await
            .expect("put valid zip payload");
        let missing_zip_hash = Hash::from_content(b"missing-zip-payload");

        let final_output_hash = Hash::from_content(b"final-media-output");
        let workflow = WorkflowSpec {
            name: Some("zip-selector-demo".to_string()),
            description: None,
            steps: vec![
                WorkflowStepSpec {
                    id: step_tagger_id.clone(),
                    tool: tagger_tool_id.clone(),
                    inputs: BTreeMap::from([(
                        "input_content".to_string(),
                        InputBinding::String("tagger-input".to_string()),
                    )]),
                    depends_on: Vec::new(),
                    outputs: BTreeMap::from([(
                        "sandbox_artifacts".to_string(),
                        OutputPolicy { save: Some(OutputSaveMode::Full) },
                    )]),
                },
                WorkflowStepSpec {
                    id: step_apply_id.clone(),
                    tool: apply_tool_id.clone(),
                    inputs: BTreeMap::from([(
                        "cover_flag".to_string(),
                        InputBinding::String(format!(
                            "${{step_output.{step_tagger_id}.sandbox_artifacts:zip({required_member})}}"
                        )),
                    )]),
                    depends_on: vec![step_tagger_id.clone()],
                    outputs: BTreeMap::from([(
                        "content".to_string(),
                        OutputPolicy { save: Some(OutputSaveMode::Full) },
                    )]),
                },
            ],
        };

        let mut machine = MachineNickelDocument::default();
        machine.tools.insert(tagger_tool_id.clone(), tagger_tool_spec.clone());
        machine.tools.insert(apply_tool_id.clone(), apply_tool_spec.clone());

        let state = OrchestrationState {
            version: 1,
            instances: BTreeMap::from([
                (
                    "a-stale-missing-zip".to_string(),
                    ToolCallInstance {
                        tool_name: tagger_tool_id.clone(),
                        metadata: tagger_tool_spec.clone(),
                        impure_timestamp: Some(ImpureTimestamp {
                            epoch_seconds: 1,
                            subsec_nanos: 0,
                        }),
                        inputs: BTreeMap::from([(
                            "input_content".to_string(),
                            mediapm_conductor::ResolvedInput::from_hash(input_hash),
                        )]),
                        outputs: BTreeMap::from([(
                            "sandbox_artifacts".to_string(),
                            OutputRef {
                                allow_empty_capture: false,
                                hash: missing_zip_hash,
                                persistence: PersistenceFlags::default(),
                            },
                        )]),
                    },
                ),
                (
                    "z-fresh-valid-zip".to_string(),
                    ToolCallInstance {
                        tool_name: tagger_tool_id,
                        metadata: tagger_tool_spec,
                        impure_timestamp: Some(ImpureTimestamp {
                            epoch_seconds: 2,
                            subsec_nanos: 0,
                        }),
                        inputs: BTreeMap::from([(
                            "input_content".to_string(),
                            mediapm_conductor::ResolvedInput::from_hash(input_hash),
                        )]),
                        outputs: BTreeMap::from([(
                            "sandbox_artifacts".to_string(),
                            OutputRef {
                                allow_empty_capture: false,
                                hash: valid_zip_hash,
                                persistence: PersistenceFlags::default(),
                            },
                        )]),
                    },
                ),
                (
                    "apply-instance".to_string(),
                    ToolCallInstance {
                        tool_name: apply_tool_id,
                        metadata: apply_tool_spec,
                        impure_timestamp: None,
                        inputs: BTreeMap::from([(
                            "cover_flag".to_string(),
                            mediapm_conductor::ResolvedInput::from_hash(Hash::from_content(
                                member_bytes,
                            )),
                        )]),
                        outputs: BTreeMap::from([(
                            "content".to_string(),
                            OutputRef {
                                allow_empty_capture: false,
                                hash: final_output_hash,
                                persistence: PersistenceFlags::default(),
                            },
                        )]),
                    },
                ),
            ]),
        };

        let step_output_hashes =
            super::resolve_workflow_step_output_hashes(&cas, &machine, &state, &workflow)
                .await
                .expect("resolve step outputs")
                .expect("step outputs should resolve");

        let tagger_output_hash = step_output_hashes
            .get(&step_tagger_id)
            .and_then(|outputs| outputs.get("sandbox_artifacts"))
            .copied()
            .expect("sandbox_artifacts hash should resolve");
        assert_eq!(tagger_output_hash, valid_zip_hash);

        let apply_output_hash = step_output_hashes
            .get(&step_apply_id)
            .and_then(|outputs| outputs.get("content"))
            .copied()
            .expect("content hash should resolve");
        assert_eq!(apply_output_hash, final_output_hash);
    }

    /// Protects runtime-state resolution when ZIP-selector source artifacts are
    /// unavailable in persisted CAS but downstream instance inputs are still
    /// present in orchestration state.
    #[tokio::test]
    #[allow(clippy::too_many_lines)]
    async fn resolve_step_output_hashes_tolerates_missing_zip_selector_source_bytes() {
        let temp = tempfile::tempdir().expect("tempdir");
        let cas = FileSystemCas::open(temp.path()).await.expect("open cas");

        let tagger_tool_id = "mediapm.tools.media-tagger@latest".to_string();
        let tagger_tool_spec = ToolSpec {
            kind: ToolKindSpec::Executable {
                command: vec!["media-tagger.exe".to_string()],
                env_vars: BTreeMap::new(),
                success_codes: vec![0],
            },
            inputs: BTreeMap::from([("input_content".to_string(), ToolInputSpec::default())]),
            outputs: BTreeMap::from([(
                "sandbox_artifacts".to_string(),
                ToolOutputSpec {
                    allow_empty: false,
                    capture: OutputCaptureSpec::Folder {
                        path: "sandbox".to_string(),
                        include_topmost_folder: false,
                    },
                },
            )]),
            ..ToolSpec::default()
        };

        let apply_tool_id = "mediapm.tools.ffmpeg@latest".to_string();
        let apply_tool_spec = ToolSpec {
            kind: ToolKindSpec::Executable {
                command: vec!["ffmpeg.exe".to_string()],
                env_vars: BTreeMap::new(),
                success_codes: vec![0],
            },
            inputs: BTreeMap::from([("cover_flag".to_string(), ToolInputSpec::default())]),
            outputs: BTreeMap::from([(
                "content".to_string(),
                ToolOutputSpec {
                    allow_empty: false,
                    capture: OutputCaptureSpec::File { path: "output.media".to_string() },
                },
            )]),
            ..ToolSpec::default()
        };

        let step_tagger_id = "step-0-media-tagger".to_string();
        let step_apply_id = "step-1-ffmpeg".to_string();
        let missing_zip_hash = Hash::from_content(b"missing-zip-payload");
        let final_output_hash = Hash::from_content(b"final-media-output");

        let workflow = WorkflowSpec {
            name: Some("zip-selector-missing-source-demo".to_string()),
            description: None,
            steps: vec![
                WorkflowStepSpec {
                    id: step_tagger_id.clone(),
                    tool: tagger_tool_id.clone(),
                    inputs: BTreeMap::from([(
                        "input_content".to_string(),
                        InputBinding::String("tagger-input".to_string()),
                    )]),
                    depends_on: Vec::new(),
                    outputs: BTreeMap::from([(
                        "sandbox_artifacts".to_string(),
                        OutputPolicy { save: Some(OutputSaveMode::Full) },
                    )]),
                },
                WorkflowStepSpec {
                    id: step_apply_id.clone(),
                    tool: apply_tool_id.clone(),
                    inputs: BTreeMap::from([(
                        "cover_flag".to_string(),
                        InputBinding::String(format!(
                            "${{step_output.{step_tagger_id}.sandbox_artifacts:zip(coverart-slot-0.flag)}}"
                        )),
                    )]),
                    depends_on: vec![step_tagger_id.clone()],
                    outputs: BTreeMap::from([(
                        "content".to_string(),
                        OutputPolicy { save: Some(OutputSaveMode::Full) },
                    )]),
                },
            ],
        };

        let mut machine = MachineNickelDocument::default();
        machine.tools.insert(tagger_tool_id.clone(), tagger_tool_spec.clone());
        machine.tools.insert(apply_tool_id.clone(), apply_tool_spec.clone());

        let state = OrchestrationState {
            version: 1,
            instances: BTreeMap::from([
                (
                    "tagger-instance".to_string(),
                    ToolCallInstance {
                        tool_name: tagger_tool_id,
                        metadata: tagger_tool_spec,
                        impure_timestamp: Some(ImpureTimestamp {
                            epoch_seconds: 2,
                            subsec_nanos: 0,
                        }),
                        inputs: BTreeMap::from([(
                            "input_content".to_string(),
                            mediapm_conductor::ResolvedInput::from_hash(Hash::from_content(
                                b"tagger-input",
                            )),
                        )]),
                        outputs: BTreeMap::from([(
                            "sandbox_artifacts".to_string(),
                            OutputRef {
                                allow_empty_capture: false,
                                hash: missing_zip_hash,
                                persistence: PersistenceFlags::default(),
                            },
                        )]),
                    },
                ),
                (
                    "apply-instance".to_string(),
                    ToolCallInstance {
                        tool_name: apply_tool_id,
                        metadata: apply_tool_spec,
                        impure_timestamp: Some(ImpureTimestamp {
                            epoch_seconds: 3,
                            subsec_nanos: 0,
                        }),
                        inputs: BTreeMap::from([(
                            "cover_flag".to_string(),
                            mediapm_conductor::ResolvedInput::from_hash(Hash::from_content(
                                b"opaque-runtime-cover-flag",
                            )),
                        )]),
                        outputs: BTreeMap::from([(
                            "content".to_string(),
                            OutputRef {
                                allow_empty_capture: false,
                                hash: final_output_hash,
                                persistence: PersistenceFlags::default(),
                            },
                        )]),
                    },
                ),
            ]),
        };

        let step_output_hashes =
            super::resolve_workflow_step_output_hashes(&cas, &machine, &state, &workflow)
                .await
                .expect("resolve step outputs")
                .expect("step outputs should resolve");

        let apply_output_hash = step_output_hashes
            .get(&step_apply_id)
            .and_then(|outputs| outputs.get("content"))
            .copied()
            .expect("content hash should resolve");
        assert_eq!(apply_output_hash, final_output_hash);
    }
}
