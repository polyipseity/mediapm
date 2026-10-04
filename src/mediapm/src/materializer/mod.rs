//! Top-level materialization orchestration: hierarchy sync.
//!
//! Coordinates concurrent materialization of hierarchy entries from CAS
//! content to the filesystem hierarchy root.

pub(crate) mod commit;
pub(crate) mod file_ops;
mod metadata;
pub(crate) mod playlist;
pub(crate) mod progress_labels;
mod resolve;
// Named `zip_reader` rather than `zip` because a child module shadows the
// `zip` extern crate in the type namespace from edition 2018 on, and the
// tests below this one write a ZIP archive.
mod zip_reader;

use std::collections::{BTreeMap, BTreeSet};
use std::path::{Path, PathBuf};
use std::sync::Arc;

use mediapm_cas::{CasApi, FileSystemCas, Hash};
use tokio::sync::Semaphore;
use tracing::{info, warn};

use self::progress_labels::{MaterializationBarLabel, MaterializationPhase, split_entry_path};
use crate::config::hierarchy_types::{
    FlattenedHierarchyEntry, HierarchyEntryKind, PlaylistItemRef, ValidatedHierarchyEntry,
    collect_playlist_media_index, expand_variant_selectors, flatten_hierarchy_nodes_for_runtime,
};
use crate::config::source_types::MediaSourceSpec;
use crate::config::{ManagedFileRecord, MediaPmDocument, MediaPmState};
use crate::error::MediaPmError;
use crate::output::progress::{ProgressBarApi, ProgressBarHandle, ProgressScreenApi};
use crate::path_component::{SanitizePolicy, render_relative_path};
use crate::paths::MediaPmPaths;
use crate::tools::workflows::{
    resolve_ffmpeg_slot_limits, resolve_media_variant_output_binding_with_limits,
};
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
use self::zip_reader::extract_zip_member_bytes;

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
use self::zip_reader::{compile_hierarchy_folder_rename_rules, extract_zip_folder_variant_bytes};

/// Summary of one `sync_hierarchy` invocation.
#[derive(Debug, Clone, PartialEq, Eq, Default)]
pub struct MaterializeReport {
    /// Number of hierarchy paths materialized (new or updated).
    pub materialized_paths: usize,
    /// Number of hierarchy paths whose variant resolved no content hash.
    pub skipped_paths: usize,
    /// Number of stale hierarchy paths removed.
    pub removed_paths: usize,
    /// Number of empty parent directories removed after stale path cleanup.
    pub removed_empty_dirs: usize,
    /// Non-fatal notices collected during materialization.
    pub notices: Vec<String>,
}

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
    /// Flattened hierarchy for stale-path scanning, in validated form.
    ///
    /// Only the validated shape is stored, so the playlist index built from it
    /// cannot yield an unvalidated component either.
    flattened: Vec<ValidatedHierarchyEntry>,
    /// Whether to CAS-verify materialized outputs after writing.
    verify_materialization: bool,
}

/// Returns the number of concurrent hierarchy-worker tasks.
fn hierarchy_worker_count() -> usize {
    let count = std::thread::available_parallelism().map_or(4, std::num::NonZero::get);
    count.clamp(1, 1024)
}

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
    progress_group: Option<Arc<dyn ProgressScreenApi + Send + Sync>>,
    overall_bar: Option<Arc<dyn ProgressBarApi>>,
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
    let validated = sanitize_and_validate_hierarchy_paths(flattened)?;
    let shared = Arc::new(SyncSharedState {
        hierarchy_root: hierarchy_root.clone(),
        cas: cas.clone(),
        flattened: validated.clone(),
        verify_materialization,
    });

    let worker_count = hierarchy_worker_count();
    let semaphore = Arc::new(Semaphore::new(worker_count));

    // The phase's bars belong to the caller's screen, which the sync derives
    // from its single terminal: this function never builds a screen or a
    // terminal of its own, because a phase with its own draw target is the
    // defect the one-terminal-per-sync contract removes.
    let pb: Arc<dyn ProgressBarApi> = if let Some(bar) = overall_bar {
        // Caller owns the overall bar — set the real entry count.
        bar.set_total(validated.len() as u64);
        bar.set_truncation(Arc::new(MaterializationBarLabel {
            entry_name: "materializing".to_string(),
            ..Default::default()
        }));
        bar
    } else if let Some(ref pg) = progress_group {
        let bar = pg.add_bar(validated.len() as u64, "materializing");
        bar.set_truncation(Arc::new(MaterializationBarLabel {
            entry_name: "materializing".to_string(),
            ..Default::default()
        }));
        bar
    } else {
        // Neither a screen nor an overall handle: the caller asked for no
        // progress output. This handle has no render slot to draw into — it is
        // never attached to a screen — so the phase stays callable without
        // progress while adding no draw target of its own.
        Arc::new(ProgressBarHandle::disabled())
    };

    let mut join_set = tokio::task::JoinSet::new();
    let document_arc = Arc::new(document.clone());

    for entry in &validated {
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

    let stale_result = remove_stale_paths(hierarchy_root, &validated, &desired_managed_paths)?;
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

/// Sanitizes and validates every flattened hierarchy entry's path components,
/// returning the entries in their validated form.
///
/// This is the production entry point of the chain in
/// [`commit`]: NFD normalization, reserved-character sanitization (when the
/// entry's effective [`SanitizeNamesConfig`] enables it), then strict
/// validation. It runs *after* [`metadata::resolve_flattened_entry_paths`],
/// because that step interpolates `${media.id}` and
/// `${media.metadata.<key>}` into components — the first point at which
/// externally sourced text (tag metadata, ffprobe output, ZIP `info.json`)
/// can reach a filesystem path — and *before* any worker starts, so a
/// rejected entry never reaches staging, verification, or commit and no
/// partially-validated path is ever written.
///
/// Consuming the flattened entries and returning
/// [`ValidatedHierarchyEntry`]s is what enforces this. The unvalidated text
/// has no way to reach a read site: the workers, the stale scan, and the
/// playlist index are all typed on the returned value, and that value's path
/// components cannot be built except through
/// [`crate::path_component::PathComponent::parse`]. Writing the parsed
/// components back into the flattened entries instead would leave every read
/// site reading a field that may or may not hold parsed text, which is the
/// guess this function exists to remove.
///
/// Every entry goes through the parser, whether or not it holds a
/// placeholder: the config-level reserved-character check in
/// `config::hierarchy_types` does not reject `.` or `..`, so a statically
/// declared `..` component would otherwise commit outside the library root.
///
/// # Errors
///
/// Returns the first [`MediaPmError::Workflow`] from
/// [`commit::sanitize_and_validate_components`], naming the offending
/// component. Failing the whole sync is intentional: a hierarchy that cannot
/// produce a safe path has no correct partial materialization.
fn sanitize_and_validate_hierarchy_paths(
    flattened: Vec<FlattenedHierarchyEntry>,
) -> Result<Vec<ValidatedHierarchyEntry>, MediaPmError> {
    let default_replacements = commit::default_sanitize_replacements();
    flattened
        .into_iter()
        .map(|flattened_entry| {
            let path = commit::sanitize_and_validate_components(
                &flattened_entry.path_components,
                &flattened_entry.entry.sanitize_names,
                &default_replacements,
            )?;
            Ok(ValidatedHierarchyEntry::new(
                path,
                flattened_entry.hierarchy_id,
                flattened_entry.entry,
            ))
        })
        .collect()
}

/// Open the sub-bar for one extracted folder variant and label it.
///
/// The parent row has already entered [`MaterializationPhase::Write`] by the
/// time this runs, and both rows render that one phase through
/// [`MaterializationPhase::tag`], so the parent and the rows under it carry
/// the same word while the folder is being written. The seed label names the
/// tag too, since the seed is what a row shows before its first render, and a
/// seed carrying its own literal is a second answer to a question
/// [`MaterializationPhase::tag`] already answers.
///
/// `relative_path` is the folder's own path, so the label carries the folder
/// as its entry and the variant as `file_name`; that is what lets a member
/// row name itself beside the entry it belongs to.
///
/// Returns `None` when the caller has no screen, which is the shape every use
/// of the handle already reads: it is advanced and finished conditionally.
fn add_variant_sub_bar(
    progress_group: Option<&Arc<dyn ProgressScreenApi + Send + Sync>>,
    relative_path: &str,
    variant_name: &str,
    member_count: usize,
) -> Option<Arc<dyn ProgressBarApi>> {
    let pg = progress_group?;
    let (entry_path, entry_name) = split_entry_path(relative_path);
    let bar = pg.add_bar(
        member_count as u64,
        &format!("{variant_name} [{}]", MaterializationPhase::Write.tag()),
    );
    bar.set_truncation(Arc::new(MaterializationBarLabel {
        entry_path: entry_path.to_string(),
        entry_name: entry_name.to_string(),
        file_name: variant_name.to_string(),
        phase: Some(MaterializationPhase::Write),
        ..Default::default()
    }));
    Some(bar)
}

/// Drives one hierarchy entry's phase bar from the phases its kind declares.
///
/// The bar opens on [`HierarchyEntryKind::first_phase`] and every later phase
/// arrives through [`EntryPhaseBar::enter`], so a row's tag follows the arm
/// instead of following a literal beside it. Holding the phase here is what
/// lets [`EntryPhaseBar::finish`] name the phase an entry was actually in when
/// it failed, rather than the phase the arm that failed used to hardcode.
struct EntryPhaseBar<'a> {
    /// The bar, absent when the caller asked for no progress output. Every
    /// method is a no-op without it, so an arm never re-checks for progress.
    handle: Option<Arc<dyn ProgressBarApi>>,
    /// Directory half of the hierarchy-relative path.
    entry_path: &'a str,
    /// Basename half of the same path.
    entry_name: &'a str,
    /// Phase the row is on, always one of the kind's declared phases.
    phase: MaterializationPhase,
}

impl<'a> EntryPhaseBar<'a> {
    /// Split `relative_path` and open a bar for `kind`, on its first declared
    /// phase. Holds nothing when `progress_group` is `None`.
    fn create(
        progress_group: Option<Arc<dyn ProgressScreenApi + Send + Sync>>,
        relative_path: &'a str,
        kind: HierarchyEntryKind,
    ) -> EntryPhaseBar<'a> {
        let (entry_path, entry_name) = split_entry_path(relative_path);
        let phase = kind.first_phase();
        let handle = progress_group.map(|pg| {
            // The total is the number of advances the row will see, and an
            // entry arm advances once after its work returns, so a finished
            // entry reads 1/1 whether it succeeded, warned or failed. The
            // phases the row passed through are on the tag, not in the
            // fraction, and the outcome rides on the colour and the status
            // marker the arm installs at the finish.
            let bar = pg.add_bar(1, &format!("{relative_path} [{}]", phase.tag()));
            bar.set_truncation(Arc::new(MaterializationBarLabel {
                entry_path: entry_path.to_string(),
                entry_name: entry_name.to_string(),
                phase: Some(phase),
                ..Default::default()
            }));
            bar
        });
        EntryPhaseBar { handle, entry_path, entry_name, phase }
    }

    /// Install `phase` as the row's current phase.
    ///
    /// Takes `&mut self` because the phase the row is on is the one a later
    /// `finish` reports, so moving on changes this row rather than something
    /// the bar is told separately.
    fn enter(&mut self, phase: MaterializationPhase) {
        self.phase = phase;
        self.label(phase, "");
    }

    /// Install `phase` only when the row is somewhere else.
    ///
    /// The folder arm writes one variant after another and never comes back
    /// off `[wrt]`, so calling [`Self::enter`] per variant would re-install a
    /// label the row already carries once per variant for no new fact. A
    /// caller that moves forward through distinct phases wants [`Self::enter`]
    /// instead, because a second call there would mean a phase went backwards.
    fn enter_once(&mut self, phase: MaterializationPhase) {
        if self.phase != phase {
            self.enter(phase);
        }
    }

    /// Re-install the row's current phase with a terminal-state marker.
    ///
    /// `finish_warning` and `finish_error` change a bar's colour and nothing
    /// else, so the text of a finished entry never said what happened. The
    /// marker is the caller's, because it is the finish that decides between
    /// them: a skipped media entry warns and a rejected one errors.
    ///
    /// The phase is the one the row is on, not a fresh literal, so a failure
    /// part-way through a multi-phase entry reports where it stopped.
    fn finish(&self, status_marker: &str) {
        self.label(self.phase, status_marker);
    }

    /// The bar itself, for the calls an arm makes on the work rather than on
    /// its label. Returns a handle the caller owns, so holding it cannot
    /// borrow the row for the duration.
    fn handle(&self) -> Option<Arc<dyn ProgressBarApi>> {
        self.handle.clone()
    }

    /// Install `phase` and `status_marker` together, the one place a row's
    /// label text is built.
    fn label(&self, phase: MaterializationPhase, status_marker: &str) {
        let Some(ref bar) = self.handle else {
            return;
        };
        bar.set_truncation(Arc::new(MaterializationBarLabel {
            entry_path: self.entry_path.to_string(),
            entry_name: self.entry_name.to_string(),
            file_name: String::new(),
            phase: Some(phase),
            status_marker: status_marker.to_string(),
        }));
    }
}

/// Materialises one flattened hierarchy entry from CAS content to the
/// filesystem hierarchy root.
///
/// Handles all three entry kinds:
/// - `Media`: single-file variant materialization.
/// - `MediaFolder`: multi-variant or ZIP-folder materialization.
/// - `Playlist`: playlist file generation.
#[expect(
    clippy::too_many_lines,
    reason = "hierarchy dispatch consolidates variant-specific staging and progress tracking"
)]
async fn prepare_hierarchy_entry(
    entry: &ValidatedHierarchyEntry,
    document: &MediaPmDocument,
    shared: &SyncSharedState,
    lookup: &MaterializationLookupContext,
    progress_group: Option<Arc<dyn ProgressScreenApi + Send + Sync>>,
) -> Result<PreparedHierarchyEntryResult, MediaPmError> {
    let relative_path = entry.relative_path_text();
    // The join takes the parsed components, not the rendered string above, so
    // there is no `&str` here for a caller to hand unvalidated text to.
    let target_path = shared.hierarchy_root.join(commit::join_path_components(&entry.path));

    // Per-entry phase bar, owned by mediapm (not the conductor), so it carries
    // the `[stg]`/`[vrf]`/`[cmt]` phase tags. Which phases this row walks is
    // the kind's declaration, and the driver installs them as the arm enters
    // them, so a kind that gains a phase cannot leave its row on the old tag.
    let mut entry_bar =
        EntryPhaseBar::create(progress_group.clone(), &relative_path, entry.entry.kind);

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

            entry_bar.enter(MaterializationPhase::Verify);
            let hash = resolve_variant_hash(media_id, &effective_variant, source, lookup).await?;

            if let Some(hash) = hash {
                entry_bar.enter(MaterializationPhase::Commit);

                // Check if this variant has a zip_member binding (e.g., subtitles_en
                // produces a ZIP containing `.en.vtt`). The raw CAS hash points at the
                // ZIP archive; we must extract the specific member before writing.
                let binding = resolve_media_variant_output_binding_with_limits(
                    source,
                    &effective_variant,
                    lookup.ffmpeg_slot_limits.max_input_slots,
                    lookup.ffmpeg_slot_limits.max_output_slots,
                )?;
                let materialized_hash = if let Some(zip_member) =
                    binding.as_ref().and_then(|b| b.zip_member.as_deref())
                {
                    let zip_bytes = shared.cas.get(hash).await.map_err(|source| {
                        MediaPmError::Workflow(format!(
                            "reading ZIP variant '{hash}' for media '{media_id}' \
                                 variant '{effective_variant}' member extraction: {source}"
                        ))
                    })?;
                    let extracted_bytes = extract_zip_member_bytes(&zip_bytes, zip_member)
                        .map_err(|error| {
                            MediaPmError::Workflow(format!(
                                "extracting ZIP member '{zip_member}' for media '{media_id}' \
                                 variant '{effective_variant}': {error}"
                            ))
                        })?;
                    if let Some(parent) = target_path.parent() {
                        tokio::fs::create_dir_all(parent).await.map_err(|source| {
                            MediaPmError::Io {
                                operation: "creating parent directory for extracted variant"
                                    .to_string(),
                                path: parent.to_path_buf(),
                                source,
                            }
                        })?;
                    }
                    tokio::fs::write(&target_path, &extracted_bytes).await.map_err(|source| {
                        MediaPmError::Io {
                            operation: "writing extracted ZIP member".to_string(),
                            path: target_path.clone(),
                            source,
                        }
                    })?;
                    crate::materializer::commit::ensure_managed_path_readonly(&target_path)?;
                    Hash::from_content(&extracted_bytes)
                } else {
                    materialize_file_entry(&target_path, &relative_path, &hash, shared).await?;
                    hash
                };

                let record = ManagedFileRecord {
                    media_id: media_id.clone(),
                    variant: effective_variant.clone(),
                    hash: materialized_hash.to_string(),
                };
                if let Some(bar) = entry_bar.handle() {
                    bar.advance(1);
                    bar.finish_success();
                }
                Ok(PreparedHierarchyEntryResult {
                    materialized: true,
                    managed_files: BTreeMap::from([(relative_path.clone(), record)]),
                    media_variant_updates: BTreeMap::from([(
                        media_id.clone(),
                        BTreeMap::from([(
                            effective_variant.clone(),
                            materialized_hash.to_string(),
                        )]),
                    )]),
                })
            } else {
                shared.notice(format!(
                    "media '{media_id}' variant '{effective_variant}' has no content hash; skipping"
                ));
                if let Some(bar) = entry_bar.handle() {
                    bar.advance(1);
                    // The media arm last installed `[vrf]` and never moved on,
                    // because there is no hash to commit, so the warning names
                    // the phase the row is actually on.
                    entry_bar.finish("W");
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
                &mut entry_bar,
            )
            .await;
            // The folder arm moves to `[wrt]` when it starts writing a variant
            // and stays there, so a failure during the write names that phase
            // while one raised before the first write names `[stg]`.
            if let Some(bar) = entry_bar.handle() {
                bar.advance(1);
                if result.is_ok() {
                    bar.finish_success();
                } else {
                    entry_bar.finish("F");
                    bar.finish_error();
                }
            }
            result
        }
        HierarchyEntryKind::Playlist => {
            // Playlist generation.
            let result = materialize_playlist_entry(
                entry,
                document,
                &target_path,
                &relative_path,
                shared,
                &mut entry_bar,
            )
            .await;
            if let Some(bar) = entry_bar.handle() {
                bar.advance(1);
                if result.is_ok() {
                    bar.finish_success();
                } else {
                    entry_bar.finish("F");
                    bar.finish_error();
                }
            }
            result
        }
    }
}

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
    clippy::too_many_arguments,
    reason = "media-folder materialization handles folder variants and rename rules inline"
)]
async fn materialize_media_folder_entry(
    entry: &ValidatedHierarchyEntry,
    source: &MediaSourceSpec,
    media_id: &str,
    target_path: &Path,
    relative_path: &str,
    shared: &SyncSharedState,
    lookup: &MaterializationLookupContext,
    progress_group: Option<Arc<dyn ProgressScreenApi + Send + Sync>>,
    entry_bar: &mut EntryPhaseBar<'_>,
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
        // A variant name is a second untrusted join on this path: it reaches
        // `target_path.join(...)` and then `tokio::fs::write`, and it can
        // carry a separator or `..` because it is a free-form key in the
        // source's `variant_hashes` map rather than a validated hierarchy
        // component. Parsing it through `PathComponent` closes the same class
        // of write-outside-the-target-folder bug the ZIP member path had.
        let variant_path = commit::join_path_components(
            &commit::parse_relative_path_components(
                Path::new(variant_name),
                &SanitizePolicy::disabled(),
            )
            .map_err(|error| {
                MediaPmError::Workflow(format!(
                    "media '{media_id}': refusing unsafe variant name '{variant_name}': {error}"
                ))
            })?,
        );
        let variant_path = target_path.join(variant_path);

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
        // The parent row moves to `[wrt]` here, before the ZIP test decides
        // whether this variant opens a sub-bar or writes a plain file, because
        // both arms write and both belong to the phase the folder declares.
        entry_bar.enter_once(MaterializationPhase::Write);
        let is_zip = is_zip_content(&data);
        if is_zip {
            let extracted = extract_zip_folder_variant_bytes(&data, &rename_rules)?;
            if extracted.is_empty() {
                shared.notice(format!(
                    "media '{media_id}' variant '{variant_name}': ZIP archive contained zero extractable files"
                ));
            }
            let file_bar = add_variant_sub_bar(
                progress_group.as_ref(),
                relative_path,
                variant_name,
                extracted.len(),
            );
            for (file_rel_path, content) in extracted {
                let file_rel_path = normalize_yt_dlp_sandbox_zip_member_path(&file_rel_path);
                // The extracted member name is untrusted archive data, and
                // this is the join that writes it to disk. Parsing it through
                // `PathComponent` is the same contract the hierarchy path
                // uses, so a traversal component cannot reach the join even if
                // the archive-shape normalizer above is later loosened.
                let file_rel_path = commit::join_path_components(
                    &commit::parse_relative_path_components(
                        &file_rel_path,
                        &SanitizePolicy::disabled(),
                    )
                    .map_err(|error| MediaPmError::Workflow(format!(
                        "media '{media_id}' variant '{variant_name}': refusing extracted ZIP member '{}': {error}",
                        file_rel_path.to_string_lossy()
                    )))?,
                );
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
                // Rewrite .desktop Name= content to strip yt-dlp artifacts.
                let content =
                    if file_target.extension().is_some_and(|e| e.eq_ignore_ascii_case("desktop")) {
                        let rewritten = rewrite_desktop_link_content(
                            std::str::from_utf8(&content).unwrap_or_default(),
                        );
                        rewritten.into_bytes()
                    } else {
                        content
                    };
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

/// Generates a playlist file from the media entries referenced by a playlist
/// hierarchy node.
async fn materialize_playlist_entry(
    entry: &ValidatedHierarchyEntry,
    _document: &MediaPmDocument,
    target_path: &Path,
    relative_path: &str,
    shared: &SyncSharedState,
    entry_bar: &mut EntryPhaseBar<'_>,
) -> Result<PreparedHierarchyEntryResult, MediaPmError> {
    // Build playlist entries from the media ids referenced by this playlist
    // node. The references are carried on `entry.entry.ids` as
    // `PlaylistItemRef` values whose `id` is a hierarchy id; the media index
    // maps each hierarchy id to its flattened path components.
    let media_index = collect_playlist_media_index(&shared.flattened).map_err(|e| {
        MediaPmError::Workflow(format!("collecting playlist media index failed: {e}"))
    })?;

    let mut rendered_entries = Vec::new();

    for item_ref in &entry.entry.ids {
        // Resolve the referenced hierarchy id to its flattened path.
        let (hierarchy_id, path_mode) = match item_ref {
            PlaylistItemRef::Shorthand(id) => (id.as_str(), PlaylistEntryPathMode::Relative),
            PlaylistItemRef::Object { id, path } => {
                let mode = match path.as_deref() {
                    Some("absolute") => PlaylistEntryPathMode::Absolute,
                    _ => PlaylistEntryPathMode::Relative,
                };
                (id.as_str(), mode)
            }
        };

        let Some(path_components) = media_index.get(hierarchy_id) else {
            return Err(MediaPmError::Workflow(format!(
                "playlist references unknown hierarchy id '{hierarchy_id}'"
            )));
        };

        let media_relative_path = render_relative_path(path_components);
        let resolved =
            resolve_playlist_target_relative_path(relative_path, &media_relative_path, path_mode);
        rendered_entries.push(RenderedPlaylistEntry {
            id: hierarchy_id.to_string(),
            path: resolved.to_string_lossy().to_string(),
        });
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
    // The row reaches `[cmt]` at the write rather than at the top of the
    // function, because a playlist that fails while resolving a reference has
    // not committed anything and should say so.
    entry_bar.enter(MaterializationPhase::Commit);
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
    current_entries: &[ValidatedHierarchyEntry],
    managed_paths: &BTreeSet<String>,
) -> Result<(usize, usize), MediaPmError> {
    let current_paths: BTreeSet<String> =
        current_entries.iter().map(ValidatedHierarchyEntry::relative_path_text).collect();

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

/// Rewrites the content of a `.desktop` link file produced by yt-dlp.
///
/// yt-dlp's output template embeds the `downloads/` prefix and `__mediapm__`
/// marker in the `Name=` field, and escapes spaces as `\s`. This helper
/// strips both artifacts and unescapes spaces so the displayed name is clean.
#[must_use]
fn rewrite_desktop_link_content(content: &str) -> String {
    content
        .lines()
        .map(|line| {
            if let Some(rest) = line.strip_prefix("Name=") {
                let cleaned = rest
                    .trim_start_matches("downloads/")
                    .replace(YT_DLP_MEDIAPM_SANDBOX_MARKER, "")
                    .replace("\\s", " ");
                format!("Name={cleaned}")
            } else {
                line.to_string()
            }
        })
        .collect::<Vec<_>>()
        .join("\n")
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

#[cfg(test)]
mod tests {
    use std::collections::BTreeMap;

    use super::*;
    use crate::config::hierarchy_types::{
        HierarchyFolderRenameRule, HierarchyNode, HierarchyNodeKind, HierarchyPath, PlaylistFormat,
        SanitizeNamesConfig,
    };
    use crate::config::source_types::{MediaSourceSpec, MediaStep, MediaStepTool};
    use crate::config::{GenericOutputVariantConfig, MediaMetadataValue, OutputVariantValue};
    use mediapm_utils::progress::recording::{
        BarId, ProgressOp, RecordedProgressOp, RecordingProgressTracker,
    };
    use std::path::Path;

    /// Media-folder variant name carrying a reserved character, so the
    /// `SanitizePolicy` rewrite half has something it would change.
    const UNSAFE_VARIANT_NAME: &str = "raw<b";

    /// The spelling `SanitizePolicy::Enabled` produces from
    /// [`UNSAFE_VARIANT_NAME`]. The variant join uses the reject half, so
    /// this path must stay absent; its presence is the silent rewrite.
    const REWRITTEN_VARIANT_NAME: &str = "raw_b";

    /// A `rename_files` replacement emitting a reserved character, applied to
    /// the member `cover.jpg` so the produced leaf is [`REPLACED_LEAF`].
    const UNSAFE_REPLACEMENT: &str = "raw<b";

    /// The file name `UNSAFE_REPLACEMENT` produces on `cover.jpg`.
    const REPLACED_LEAF: &str = "raw<b.jpg";

    /// The spelling the configured replacement map would produce from
    /// [`REPLACED_LEAF`]. Its presence means the map reached the rename chain.
    const REWRITTEN_MEMBER_NAME: &str = "raw_b.jpg";

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
    fn regression_desktop_link_name_strips_downloads_prefix() {
        let input = "[Desktop Entry]\nName=downloads/Rick Astley - Never Gonna Give You Up\nURL=https://example.com";
        let expected =
            "[Desktop Entry]\nName=Rick Astley - Never Gonna Give You Up\nURL=https://example.com";
        assert_eq!(rewrite_desktop_link_content(input), expected);
    }

    #[test]
    fn regression_desktop_link_name_strips_mediapm_marker() {
        let input =
            "[Desktop Entry]\nName=Rick Astley [dQw4w9WgXcQ]__mediapm__\nURL=https://example.com";
        let expected = "[Desktop Entry]\nName=Rick Astley [dQw4w9WgXcQ]\nURL=https://example.com";
        assert_eq!(rewrite_desktop_link_content(input), expected);
    }

    #[test]
    fn regression_desktop_link_name_unescapes_spaces() {
        let input = "Name=downloads/Rick\\sAstley\\s[dQw4w9WgXcQ]__mediapm__";
        let expected = "Name=Rick Astley [dQw4w9WgXcQ]";
        assert_eq!(rewrite_desktop_link_content(input), expected);
    }

    #[test]
    fn regression_desktop_link_name_full_ytdlp_template() {
        let input = "[Desktop Entry]\nName=downloads/Rick\\sAstley\\s-\\sNever\\sGonna\\sGive\\sYou\\sUp\\s[dQw4w9WgXcQ]__mediapm__\nURL=https://example.com";
        let expected = "[Desktop Entry]\nName=Rick Astley - Never Gonna Give You Up [dQw4w9WgXcQ]\nURL=https://example.com";
        assert_eq!(rewrite_desktop_link_content(input), expected);
    }

    #[test]
    fn regression_desktop_link_name_preserves_non_name_lines() {
        let input =
            "[Desktop Entry]\nType=Link\nName=downloads/Test__mediapm__\nURL=https://example.com";
        let expected = "[Desktop Entry]\nType=Link\nName=Test\nURL=https://example.com";
        assert_eq!(rewrite_desktop_link_content(input), expected);
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

    /// Injected [`RecordingProgressTracker`] with overall bar produces no ops
    /// beyond the overall `AddBar` when hierarchy is empty (early return
    /// before any per-entry progress bar work).
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

        let (recording, overall) = RecordingProgressTracker::with_overall("materializing", 1);
        let result = sync_hierarchy(
            &paths,
            &document,
            &mut state,
            &cas,
            true,
            &conductor_state,
            &generated_doc,
            Some(Arc::new(recording.clone())),
            Some(Arc::new(overall)),
        )
        .await;

        assert!(result.is_ok());
        let ops = recording.ops();
        // Only the overall bar AddBar from with_overall(); the early return
        // prevents set_total/set_prefix_components from running.
        assert_eq!(
            ops,
            vec![ProgressOp::AddBar { total: 1, label: "materializing".into() }],
            "empty hierarchy should only produce the overall AddBar, got {ops:?}",
        );
    }

    /// Single media entry with no CAS content emits the full progress
    /// sequence: overall bar → per-entry `[stg]`/`[vrf]` phases →
    /// `Advance(1)` + `FinishWarning` (skipped) → overall
    /// `Advance(1)` + `FinishSuccess`.
    ///
    /// The order is deterministic because the single spawned task completes
    /// (advance + `entry_bar` ops) before the overall bar is finished.
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
                sanitize_names: Some(SanitizeNamesConfig::Inherit),
                children: vec![],
            }],
            ..MediaPmDocument::default()
        };
        let mut state = MediaPmState::default();
        let conductor_state = ConductorState::new_empty();
        let generated_doc = NickelDocument::default();

        let (recording, overall) = RecordingProgressTracker::with_overall("materializing", 1);
        let result = sync_hierarchy(
            &paths,
            &document,
            &mut state,
            &cas,
            true,
            &conductor_state,
            &generated_doc,
            Some(Arc::new(recording.clone())),
            Some(Arc::new(overall)),
        )
        .await;

        assert!(result.is_ok(), "sync_hierarchy should succeed: {result:?}");
        let ops = recording.ops();
        // Exact op sequence: overall bar (AddBar from with_overall,
        // then SetTotal + SetTruncation from sync_hierarchy) → per-entry
        // `[stg]`/`[vrf]` phases → `Advance(1)` + `FinishWarning` (skipped, no
        // CAS content) → overall `Advance(1)` + `FinishSuccess`.
        assert_eq!(
            ops,
            vec![
                // Overall bar created by with_overall(), total set by sync_hierarchy.
                ProgressOp::AddBar { total: 1, label: "materializing".into() },
                ProgressOp::SetTotal { total: 1 },
                ProgressOp::SetTruncation { prefix: "materializing".into(), suffix: String::new() },
                // Per-entry bar: staging.
                ProgressOp::AddBar { total: 1, label: "test_file [stg]".into() },
                ProgressOp::SetTruncation {
                    prefix: "test_file [stg]".into(),
                    suffix: String::new(),
                },
                // Per-entry bar: verify phase (set before hash resolution).
                ProgressOp::SetTruncation {
                    prefix: "test_file [vrf]".into(),
                    suffix: String::new(),
                },
                // Skipped: advance(1) on entry_bar, the `[W]` label that
                // names the skip in the text, then FinishWarning.
                ProgressOp::Advance { delta: 1 },
                ProgressOp::SetTruncation {
                    prefix: "[W] test_file [vrf]".into(),
                    suffix: String::new(),
                },
                ProgressOp::FinishWarning,
                // Overall bar: advance(1) after entry completes + finish_success.
                ProgressOp::Advance { delta: 1 },
                ProgressOp::FinishSuccess,
            ],
            "\nops mismatch — expected the overall bar + [stg]→[vrf] skip path",
        );
    }

    /// The label installed immediately before a warning finish carries `[W]`.
    ///
    /// `finish_warning` on its own only changes the bar's colour, and the
    /// materialization label has no marker, so a skipped entry and a failed
    /// one used to be indistinguishable in the text. The phase in the marker
    /// label is the one the bar already shows, so the marker adds a fact
    /// instead of replacing one.
    #[tokio::test]
    async fn sync_hierarchy_marks_a_skipped_entry_warning() {
        let root = mediapm_utils::temp::artifact_dir().unwrap();
        let paths = MediaPmPaths::from_root(root.path());
        let cas = open_hierarchy_cas(&paths).await;

        let document = single_media_document("src1", HierarchyPath::simple("test_file"));
        let mut state = MediaPmState::default();
        let conductor_state = ConductorState::new_empty();
        let generated_doc = NickelDocument::default();

        let (recording, overall) = RecordingProgressTracker::with_overall("materializing", 1);
        sync_hierarchy(
            &paths,
            &document,
            &mut state,
            &cas,
            true,
            &conductor_state,
            &generated_doc,
            Some(Arc::new(recording.clone())),
            Some(Arc::new(overall)),
        )
        .await
        .expect("sync_hierarchy should succeed");

        let ops = recording.ops();
        let finish = ops
            .iter()
            .position(|op| matches!(op, ProgressOp::FinishWarning))
            .expect("a media entry with no content hash finishes with a warning");
        assert_eq!(
            ops.get(finish.saturating_sub(1)),
            Some(&ProgressOp::SetTruncation {
                prefix: "[W] test_file [vrf]".into(),
                suffix: String::new(),
            }),
            "the label installed before the warning finish must carry [W]; got {ops:?}",
        );
    }

    /// The label installed immediately before a failed entry's finish carries
    /// `[F]`, and a failure now reads the same way as a warning.
    ///
    /// The failure is provoked through the folder arm rather than asserted
    /// against a mock: a variant name of `..` is refused by the
    /// path-component parser before any byte is written, which is the
    /// cheapest real `Err` the materializer produces.
    #[tokio::test]
    async fn sync_hierarchy_marks_a_failed_folder_entry() {
        let root = mediapm_utils::temp::artifact_dir().unwrap();
        let paths = MediaPmPaths::from_root(root.path());
        let cas = open_hierarchy_cas(&paths).await;

        let mut document = single_media_document("src1", HierarchyPath::simple("album"));
        document.hierarchy[0].kind = HierarchyNodeKind::MediaFolder;
        let source = document.media.get_mut("src1").unwrap();
        source.steps[0].output_variants = BTreeMap::from([(
            "..".to_string(),
            OutputVariantValue::Generic(GenericOutputVariantConfig {
                kind: "primary".to_string(),
                ..Default::default()
            }),
        )]);
        let mut state = MediaPmState::default();
        let conductor_state = ConductorState::new_empty();
        let generated_doc = NickelDocument::default();

        let (recording, overall) = RecordingProgressTracker::with_overall("materializing", 1);
        let result = sync_hierarchy(
            &paths,
            &document,
            &mut state,
            &cas,
            true,
            &conductor_state,
            &generated_doc,
            Some(Arc::new(recording.clone())),
            Some(Arc::new(overall)),
        )
        .await;

        assert!(result.is_err(), "a rejected variant name must fail the entry: {result:?}");
        let ops = recording.ops();
        let finish = ops
            .iter()
            .position(|op| matches!(op, ProgressOp::FinishError))
            .expect("a refused variant name finishes the entry bar with an error");
        assert_eq!(
            ops.get(finish.saturating_sub(1)),
            Some(&ProgressOp::SetTruncation {
                prefix: "[F] album [stg]".into(),
                suffix: String::new(),
            }),
            "the label installed before the error finish must carry [F]; got {ops:?}",
        );
    }

    /// The phase tag a rendered prefix ends with, or `None` when it carries
    /// none, which is how a row is checked against its declaration.
    fn rendered_phase_tag(prefix: &str) -> Option<&str> {
        prefix.rsplit_once('[').and_then(|(_, tail)| tail.strip_suffix(']'))
    }

    /// A finished entry names the phase its kind declares last, and no phase
    /// outside that declaration ever reaches the row.
    ///
    /// The bug this pins: the folder and playlist arms installed a phase only
    /// on the failure path, by passing the literal `"stg"` beside the bar,
    /// while the row they had already created sat on `[stg]` for its whole
    /// run. Nothing tied the tag to what the arm did, so a finished folder row
    /// was indistinguishable from one still staging and the two could drift
    /// apart silently. Driving the bar from `HierarchyEntryKind::phases`
    /// instead of from literals makes a tag the arm never declared a build of
    /// the same kind that fails here.
    #[test]
    fn entry_bar_installs_exactly_the_phases_its_kind_declares() {
        for kind in [
            HierarchyEntryKind::Media,
            HierarchyEntryKind::MediaFolder,
            HierarchyEntryKind::Playlist,
        ] {
            let phases = kind.phases();
            let tracker = RecordingProgressTracker::new();
            let mut bar =
                EntryPhaseBar::create(Some(Arc::new(tracker.clone())), "Music/album", kind);
            for phase in phases.iter().skip(1) {
                bar.enter(*phase);
            }
            bar.finish("F");

            let rendered: Vec<String> = tracker
                .ops()
                .iter()
                .filter_map(|op| match op {
                    ProgressOp::SetTruncation { prefix, .. } => {
                        Some(rendered_phase_tag(prefix).unwrap_or_default().to_string())
                    }
                    _ => None,
                })
                .collect();
            let mut expected: Vec<String> =
                phases.iter().map(|phase| phase.tag().to_string()).collect();
            let last = expected.last().cloned().expect("a kind declares at least one phase");
            expected.push(last.clone());

            assert_eq!(
                rendered, expected,
                "{kind:?} rendered a tag its own declaration does not list",
            );
            assert_eq!(
                tracker.ops().last(),
                Some(&ProgressOp::SetTruncation {
                    prefix: format!("[F] album Music [{last}]"),
                    suffix: String::new(),
                }),
                "{kind:?} did not finish on the last phase it declares",
            );
        }
    }

    /// Every kind declares the phases its arm walks.
    ///
    /// A folder makes its directory and then writes each selected variant, so
    /// it declares staging and write. A playlist resolves its references,
    /// builds the bytes and writes one file, so it declares staging and
    /// commit. Neither verifies, so a list with `[vrf]` in it would put a tag
    /// on the screen with no code behind it. A media entry walks all three
    /// phases, and is the only kind that claims verify.
    ///
    /// The media assertion is here for that reason. Deleting the test as a
    /// duplicate of the tag-driven ones above would leave nothing saying that
    /// media is the only kind that lists verify, since every other assertion
    /// in the module reads a declaration rather than pinning one.
    #[test]
    fn every_kind_declares_the_phases_its_arm_walks() {
        assert_eq!(
            HierarchyEntryKind::MediaFolder.phases(),
            &[MaterializationPhase::Staging, MaterializationPhase::Write],
        );
        assert_eq!(
            HierarchyEntryKind::Playlist.phases(),
            &[MaterializationPhase::Staging, MaterializationPhase::Commit],
        );
        assert_eq!(
            HierarchyEntryKind::Media.phases(),
            &[
                MaterializationPhase::Staging,
                MaterializationPhase::Verify,
                MaterializationPhase::Commit,
            ],
        );
    }

    /// A media entry that stops before the end names the phase it reached.
    ///
    /// The finish reads the phase the row is on rather than a literal, so a
    /// stop at verify and a stop at commit name different tags. The media
    /// arm has one non-success exit, the missing-hash skip, which is a warning
    /// and leaves the row on `[vrf]`; the commit case here is the declared end
    /// of the media phases rather than a second exit, and pins that the same
    /// finish reports it.
    ///
    /// One literal per exit is the shape this replaced: the media arm passed
    /// `"vrf"` at its skip and the folder and playlist arms passed `"stg"` at
    /// a failure, each correct only as long as no arm moved.
    #[test]
    fn a_finished_media_entry_names_the_phase_it_reached() {
        for (phase, tag) in
            [(MaterializationPhase::Verify, "vrf"), (MaterializationPhase::Commit, "cmt")]
        {
            let tracker = RecordingProgressTracker::new();
            let mut bar = EntryPhaseBar::create(
                Some(Arc::new(tracker.clone())),
                "song.mkv",
                HierarchyEntryKind::Media,
            );
            bar.enter(phase);
            bar.finish("W");

            assert_eq!(
                tracker.ops().last(),
                Some(&ProgressOp::SetTruncation {
                    prefix: format!("[W] song.mkv [{tag}]"),
                    suffix: String::new(),
                }),
                "the warning must name the phase the entry reached",
            );
        }
    }

    /// A folder row moves to `[wrt]` where it starts writing a variant and
    /// stays there, and a playlist row reaches `[cmt]` where it writes.
    ///
    /// Both were `[stg]` for their whole run before a kind declared the
    /// phases its arm walks, so a finished folder read exactly like a folder
    /// still staging. The folder's row and the sub-bars under it now name the
    /// same work at two granularities, which is the point of declaring write
    /// on the folder: during a folder sync the parent says what its children
    /// say.
    ///
    /// This drives a real `sync_hierarchy` rather than a bar in isolation,
    /// because the arms install these phases deep inside the folder and
    /// playlist work, and a test that reached for the bar directly would pass
    /// with the arms still sitting on `[stg]`.
    ///
    /// The variant here is a plain file rather than a ZIP, so no `[wrt]`
    /// sub-bar opens. The archive case, where a parent on `[wrt]` opens a
    /// sub-bar of its own, is in
    /// `a_zip_folder_variant_opens_a_wrt_sub_bar_under_a_wrt_parent`.
    #[tokio::test]
    async fn a_folder_row_reaches_wrt_and_a_playlist_row_reaches_cmt() {
        let root = mediapm_utils::temp::artifact_dir().unwrap();
        let paths = MediaPmPaths::from_root(root.path());
        let cas = open_hierarchy_cas(&paths).await;

        let hash = cas.put(bytes::Bytes::from_static(b"folder-member-bytes")).await.unwrap();
        let document = folder_and_playlist_document(&hash.to_string());

        let mut state = MediaPmState::default();
        let conductor_state = ConductorState::new_empty();
        let generated_doc = NickelDocument::default();

        let (recording, overall) = RecordingProgressTracker::with_overall("materializing", 2);
        sync_hierarchy(
            &paths,
            &document,
            &mut state,
            &cas,
            true,
            &conductor_state,
            &generated_doc,
            Some(Arc::new(recording.clone())),
            Some(Arc::new(overall)),
        )
        .await
        .expect("sync_hierarchy should succeed");

        let ops = recording.ops();
        // Only the folder's own row carries these two prefixes, so the pair is
        // everything that row said. It never comes back off `[wrt]`.
        let folder_tags: Vec<Option<&str>> = ops
            .iter()
            .filter_map(|op| match op {
                ProgressOp::SetTruncation { prefix, .. }
                    if prefix == "album [stg]" || prefix == "album [wrt]" =>
                {
                    Some(rendered_phase_tag(prefix))
                }
                _ => None,
            })
            .collect();
        assert_eq!(
            folder_tags,
            [Some("stg"), Some("wrt")],
            "the folder row must leave [stg] for [wrt] and finish there; got {ops:?}",
        );

        let playlist_tags: Vec<Option<&str>> = ops
            .iter()
            .filter_map(|op| match op {
                ProgressOp::SetTruncation { prefix, .. }
                    if prefix.starts_with("rickroll.m3u8 ") =>
                {
                    Some(rendered_phase_tag(prefix))
                }
                _ => None,
            })
            .collect();
        assert_eq!(
            playlist_tags,
            [Some("stg"), Some("cmt")],
            "a playlist reaches [cmt] at the write; got {ops:?}",
        );
    }

    /// A folder row and the sub-bar under it render the same phase word.
    ///
    /// Both labels come from the real constructors the folder arm uses:
    /// [`EntryPhaseBar`] for the parent and [`add_variant_sub_bar`] for the
    /// member row, both recorded through a [`RecordingProgressTracker`] so
    /// what is asserted is what the screen receives. Nothing here builds a
    /// label by hand, so the test cannot pass while the arm renders something
    /// else.
    ///
    /// The agreement is the point. A folder row that sat on `[stg]` under
    /// children reading `[wrt]`, or the reverse, would describe the same work
    /// two different ways in one column, and nothing else on the screen
    /// compares the two rows.
    #[test]
    fn a_folder_row_and_its_variant_sub_bar_render_the_same_phase() {
        let tracker = RecordingProgressTracker::new();
        let mut parent = EntryPhaseBar::create(
            Some(Arc::new(tracker.clone())),
            "album",
            HierarchyEntryKind::MediaFolder,
        );
        parent.enter_once(MaterializationPhase::Write);
        let screen: Arc<dyn ProgressScreenApi + Send + Sync> = Arc::new(tracker.clone());
        add_variant_sub_bar(Some(&screen), "album", "links", 3)
            .expect("a screen is supplied, so the sub-bar opens");

        let ops = tracker.ops();
        let seeds: Vec<(u64, String)> = ops
            .iter()
            .filter_map(|op| match op {
                ProgressOp::AddBar { total, label } => Some((*total, label.clone())),
                _ => None,
            })
            .collect();
        assert_eq!(
            seeds,
            vec![(1, "album [stg]".to_string()), (3, "links [wrt]".to_string())],
            "both seeds name a phase, since a seed is what a row shows before its first render; got {ops:?}",
        );

        let rendered: Vec<String> = ops
            .iter()
            .filter_map(|op| match op {
                ProgressOp::SetTruncation { prefix, .. } => Some(prefix.clone()),
                _ => None,
            })
            .collect();
        assert_eq!(
            rendered,
            vec![
                "album [stg]".to_string(),
                "album [wrt]".to_string(),
                "album links [wrt]".to_string(),
            ],
            "the parent opens on [stg], moves to [wrt], and the member row follows it; got {ops:?}",
        );

        let parent_tag = rendered_phase_tag(&rendered[1]);
        let child_tag = rendered_phase_tag(&rendered[2]);
        assert_eq!(parent_tag, Some("wrt"), "the folder row is on its write phase");
        assert_eq!(child_tag, parent_tag, "a child may not name another phase than its parent");
        assert_eq!(
            parent_tag,
            HierarchyEntryKind::MediaFolder.phases().last().map(|phase| phase.tag()),
            "the write is the last phase a folder declares, so the row that shows it is the declared end",
        );
    }

    /// A ZIP folder variant opens a `[wrt]` sub-bar under a parent that is
    /// already reading `[wrt]`, with one unit per member the archive held.
    ///
    /// The sibling test `a_folder_row_and_its_variant_sub_bar_render_the_same_phase`
    /// builds both labels from their constructors and so pins the agreement
    /// between the two rows without ever opening an archive. It cannot see
    /// whether the arm takes the ZIP branch at all: the branch is one
    /// `is_zip_content` call at the top of the per-variant work, and a
    /// constructor-level test supplies the extracted member count itself, so an
    /// arm that stopped reading ZIP content would still pass it.
    ///
    /// This one drives the whole `sync_hierarchy`, so the archive is what
    /// decides the arm. What it pins:
    ///
    /// the parent row reaches `[wrt]` before the sub-bar exists, which is what
    /// makes the two rows read as one piece of work rather than a parent that
    /// moved on while its children still write;
    ///
    /// the sub-bar's total is the number of members the archive held, not the
    /// one file a non-archive variant writes, so the fraction beside the member
    /// rows is the archive's shape;
    ///
    /// each advance is tagged to the sub-bar's own row and not to the parent's,
    /// which is what [`BarId::index`] is for, so the parent cannot appear to
    /// finish its members;
    ///
    /// and the members land on disk, so the extraction ran rather than a bar
    /// having been opened over an archive that was never read.
    #[tokio::test]
    async fn a_zip_folder_variant_opens_a_wrt_sub_bar_under_a_wrt_parent() {
        let root = mediapm_utils::temp::artifact_dir().unwrap();
        let paths = MediaPmPaths::from_root(root.path());
        let cas = open_hierarchy_cas(&paths).await;
        let members = [("cover.jpg", &b"jpeg-bytes"[..]), ("notes/liner.txt", &b"liner"[..])];
        let hash = cas
            .put(bytes::Bytes::from(zip_payload(&members)))
            .await
            .expect("the archive enters the store");

        let mut document = folder_and_playlist_document(&hash.to_string());
        document.hierarchy.retain(|node| matches!(node.kind, HierarchyNodeKind::MediaFolder));

        let (recording, overall) = RecordingProgressTracker::with_overall("materializing", 1);
        sync_hierarchy(
            &paths,
            &document,
            &mut MediaPmState::default(),
            &cas,
            true,
            &ConductorState::new_empty(),
            &NickelDocument::default(),
            Some(Arc::new(recording.clone())),
            Some(Arc::new(overall)),
        )
        .await
        .expect("sync_hierarchy should succeed");

        let ops = recording.ops();
        let rendered: Vec<String> = ops
            .iter()
            .filter_map(|op| match op {
                ProgressOp::SetTruncation { prefix, .. } => Some(prefix.clone()),
                _ => None,
            })
            .collect();
        assert_eq!(
            rendered,
            vec![
                "materializing".to_string(),
                "album [stg]".to_string(),
                "album [wrt]".to_string(),
                "album default [wrt]".to_string(),
            ],
            "the folder row reads the write, and the member row under it reads the same one",
        );

        let seeds: Vec<(u64, String)> = ops
            .iter()
            .filter_map(|op| match op {
                ProgressOp::AddBar { total, label } => Some((*total, label.clone())),
                _ => None,
            })
            .collect();
        assert_eq!(
            seeds,
            vec![
                (1, "materializing".to_string()),
                (1, "album [stg]".to_string()),
                (2, "default [wrt]".to_string()),
            ],
            "the sub-bar's total is the archive's member count, not the one file a plain variant writes",
        );

        let parent_at = ops
            .iter()
            .position(|op| matches!(op, ProgressOp::SetTruncation { prefix, .. } if prefix == "album [wrt]"))
            .expect("the folder row reaches the write");
        let sub_bar_at = ops
            .iter()
            .position(
                |op| matches!(op, ProgressOp::AddBar { label, .. } if label == "default [wrt]"),
            )
            .expect("the archive opens one sub-bar for its variant");
        assert!(
            parent_at < sub_bar_at,
            "the parent must be on the write before the member rows exist; got {ops:?}",
        );

        let recorded = recording.recorded();
        let parent_bar = bar_opened_as(&recorded, "album [stg]");
        let sub_bar = bar_opened_as(&recorded, "default [wrt]");
        assert_ne!(parent_bar, sub_bar, "the member rows are their own row, not the folder's");

        let member_advances = recorded
            .iter()
            .filter(|entry| entry.bar == sub_bar && matches!(entry.op, ProgressOp::Advance { .. }))
            .count();
        assert_eq!(
            member_advances,
            members.len(),
            "one advance per extracted member, tagged to the sub-bar; got {recorded:?}",
        );

        for member in members {
            let written = root.path().join("album").join(member.0);
            assert_eq!(
                std::fs::read(&written).unwrap_or_default(),
                member.1,
                "member '{}' must land under the folder it was extracted into",
                written.display(),
            );
        }
    }

    /// The [`BarId`] of the bar the recorder logged under `label`.
    ///
    /// Read off the `AddBar` op rather than counted, so a row keeps its
    /// identity when another row is added ahead of it.
    fn bar_opened_as(recorded: &[RecordedProgressOp], label: &str) -> BarId {
        recorded
            .iter()
            .find_map(|entry| match &entry.op {
                ProgressOp::AddBar { label: opened, .. } if opened == label => Some(entry.bar),
                _ => None,
            })
            .unwrap_or_else(|| panic!("a bar opened as '{label}'; got {recorded:?}"))
    }

    /// Builds a ZIP payload holding `members`, stored without compression.
    ///
    /// Stored rather than deflated on purpose: it keeps the archive readable
    /// back through the same `ZipArchive` the materializer uses, and it does
    /// not ask for a compression backend the crate's features do not name.
    fn zip_payload(members: &[(&str, &[u8])]) -> Vec<u8> {
        use std::io::Write as _;

        let mut buffer = std::io::Cursor::new(Vec::new());
        let mut writer = zip::ZipWriter::new(&mut buffer);
        for (name, content) in members {
            writer
                .start_file::<&str, ()>(name, zip::write::SimpleFileOptions::default())
                .expect("a stored member starts");
            writer.write_all(content).expect("the member is written");
        }
        writer.finish().expect("the archive closes");
        buffer.into_inner()
    }

    /// Runs one `sync_hierarchy` in a workspace of its own and reports the
    /// total its entry row opened with beside the advances that row made.
    ///
    /// `build` receives the hash of the CAS payload the workspace holds, so a
    /// document whose materialization needs content gets a hash it can
    /// resolve. The workspace is fresh because `sync_hierarchy` treats files
    /// it already wrote as current, and a second run over the same tree would
    /// skip the write and advance nothing.
    ///
    /// Only one entry is on the screen, so the first bar it opens belongs to
    /// that entry, and the arm advances the entry row after its work returns,
    /// which puts the advance after the last bar the screen opened. Counting
    /// from there keeps a sub-bar's advances out of the total.
    ///
    /// The overall row is handed in as a disabled handle rather than left to
    /// the screen, because a screen of its own would open an overall bar in
    /// the same log and advance it after the entry, which would put a second
    /// advance behind the last bar on the screen.
    async fn entry_row_total_and_advances(
        build: impl FnOnce(String) -> MediaPmDocument,
    ) -> (u64, u64) {
        let root = mediapm_utils::temp::artifact_dir().unwrap();
        let paths = MediaPmPaths::from_root(root.path());
        let cas = open_hierarchy_cas(&paths).await;
        let hash = cas.put(bytes::Bytes::from_static(b"entry-row-total-bytes")).await.unwrap();
        let document = build(hash.to_string());

        let recording = RecordingProgressTracker::new();
        let _ = sync_hierarchy(
            &paths,
            &document,
            &mut MediaPmState::default(),
            &cas,
            true,
            &ConductorState::new_empty(),
            &NickelDocument::default(),
            Some(Arc::new(recording.clone())),
            Some(Arc::new(ProgressBarHandle::disabled())),
        )
        .await;

        let ops = recording.ops();
        let total = ops
            .iter()
            .find_map(|op| match op {
                ProgressOp::AddBar { total, .. } => Some(*total),
                _ => None,
            })
            .unwrap_or(0);
        let last_bar =
            ops.iter().rposition(|op| matches!(op, ProgressOp::AddBar { .. })).unwrap_or(0);
        let advances =
            ops[last_bar..].iter().filter(|op| matches!(op, ProgressOp::Advance { .. })).count()
                as u64;
        (total, advances)
    }

    /// An entry row's total is the number of advances that row makes.
    ///
    /// The total was three while every arm advanced once, so a finished entry
    /// read `1/3` and the fraction claimed the row had two thirds of its work
    /// left. Nothing held the total to the advances, so an arm that advanced
    /// zero times or twice would still have rendered a plausible fraction and
    /// nothing would have said so.
    ///
    /// The two numbers come from the recorder rather than from the constants
    /// in the source, and each case drives a real `sync_hierarchy` over a
    /// document holding one entry. The media entry appears twice because it
    /// has two arms: one that writes its variant and one that skips it for
    /// want of a hash. The last case is a folder whose variant name is
    /// refused, which is the failure path where the row advances before it
    /// learns the work failed.
    #[tokio::test]
    async fn an_entry_row_total_is_the_number_of_advances_it_makes() {
        let media_written = entry_row_total_and_advances(|hash| {
            let mut document = folder_and_playlist_document(&hash);
            document.hierarchy.retain(|node| matches!(node.kind, HierarchyNodeKind::Media));
            document
        })
        .await;
        let media_skipped = entry_row_total_and_advances(|_| {
            single_media_document("src1", HierarchyPath::simple("song"))
        })
        .await;
        let folder = entry_row_total_and_advances(|hash| {
            let mut document = folder_and_playlist_document(&hash);
            document.hierarchy.retain(|node| matches!(node.kind, HierarchyNodeKind::MediaFolder));
            document
        })
        .await;
        let playlist = entry_row_total_and_advances(|hash| {
            let mut document = folder_and_playlist_document(&hash);
            document.hierarchy.retain(|node| matches!(node.kind, HierarchyNodeKind::Folder));
            if let Some(playlist) =
                document.hierarchy.first_mut().and_then(|folder| folder.children.first_mut())
            {
                playlist.ids.clear();
            }
            document
        })
        .await;
        let refused_folder = entry_row_total_and_advances(|_| {
            let mut document = single_media_document("src1", HierarchyPath::simple("album"));
            document.hierarchy[0].kind = HierarchyNodeKind::MediaFolder;
            let source = document.media.get_mut("src1").unwrap();
            source.steps[0].output_variants = BTreeMap::from([(
                "..".to_string(),
                OutputVariantValue::Generic(GenericOutputVariantConfig {
                    kind: "primary".to_string(),
                    ..Default::default()
                }),
            )]);
            document
        })
        .await;

        for (case, (total, advances)) in [
            ("a media entry that writes its variant", media_written),
            ("a media entry with no hash to commit", media_skipped),
            ("a media folder", folder),
            ("a playlist", playlist),
            ("a media folder whose variant name is refused", refused_folder),
        ] {
            assert_eq!(
                total, advances,
                "{case}: the total an entry row opens with is the advances it will see",
            );
            assert_eq!(total, 1, "{case}: an entry row advances once, after its work returns");
        }
    }

    /// A media-folder variant name the sanitizer would rewrite is refused, and
    /// neither the raw spelling nor the rewritten one reaches the library.
    ///
    /// A variant name is a free-form key in the source's variant map, so it
    /// reaches `target_path.join(...)` and then `tokio::fs::write` without
    /// being a hierarchy component. The join parses it under
    /// `SanitizePolicy::disabled()`, which is the reject half of the policy
    /// rather than the rewrite half: the name is a key an upstream tool chose,
    /// so committing `raw_b` would materialize a file the document never
    /// spelled.
    ///
    /// The assertion is the absence of both spellings on disk, not the error
    /// alone. A refusal and a silent rewrite are told apart by the library
    /// tree, because a rewritten name leaves a file where the refused one
    /// leaves nothing.
    #[tokio::test]
    async fn a_media_folder_variant_name_the_sanitizer_would_rewrite_is_refused() {
        let root = mediapm_utils::temp::artifact_dir().unwrap();
        let paths = MediaPmPaths::from_root(root.path());
        let cas = open_hierarchy_cas(&paths).await;
        let hash = cas.put(bytes::Bytes::from_static(b"variant-name-guard-bytes")).await.unwrap();

        let mut document = folder_and_playlist_document(&hash.to_string());
        document.hierarchy.retain(|node| matches!(node.kind, HierarchyNodeKind::MediaFolder));
        let source = document.media.get_mut("src1").unwrap();
        source.steps[0].output_variants = BTreeMap::from([(
            UNSAFE_VARIANT_NAME.to_string(),
            OutputVariantValue::Generic(GenericOutputVariantConfig {
                kind: "primary".to_string(),
                ..Default::default()
            }),
        )]);

        let result = sync_hierarchy(
            &paths,
            &document,
            &mut MediaPmState::default(),
            &cas,
            true,
            &ConductorState::new_empty(),
            &NickelDocument::default(),
            None,
            None,
        )
        .await;

        let folder = root.path().join("album");
        let raw = folder.join(UNSAFE_VARIANT_NAME);
        let rewritten = folder.join(REWRITTEN_VARIANT_NAME);
        assert!(!raw.exists(), "the raw variant name was written to '{}'", raw.display());
        assert!(
            !rewritten.exists(),
            "the sanitizer rewrote '{UNSAFE_VARIANT_NAME}' to '{REWRITTEN_VARIANT_NAME}', \
             which is what this guard exists to prevent; '{}' exists",
            rewritten.display()
        );

        let error = result.err().unwrap_or_else(|| {
            panic!("a variant name carrying '{UNSAFE_VARIANT_NAME}' must not reach the join")
        });
        assert!(
            error.to_string().contains(UNSAFE_VARIANT_NAME),
            "the error must name the refused variant so the document key is identifiable; got: {error}"
        );
    }

    /// A `rename_files` replacement emitting a reserved character is refused
    /// even when the entry configures `sanitize_names = Enabled`, and the
    /// rewritten spelling never reaches the library.
    ///
    /// `SanitizePolicy` has two halves: `Enabled` and `Custom` carry a
    /// replacement map and rewrite reserved characters, `disabled` carries
    /// none and rejects them. The ZIP rename chain uses the second half, so
    /// the configured map has no effect on a replacement's output. That is a
    /// decision rather than an omission, and `SanitizePolicy::disabled` gives
    /// the reason: a member name belongs to whatever produced the archive, so
    /// rewriting it would materialize a file nobody asked for.
    ///
    /// Configuring `Enabled` on the entry is what makes this a guard. With the
    /// entry left on its default the map is not in play anywhere, so the test
    /// would pass against any policy. Here the document asks for the rewrite
    /// and the run refuses, which fails the moment someone routes the
    /// configured map into the rename chain.
    ///
    /// The coverage-matrix row this replaces asked for the opposite: that
    /// replacement strings are sanitized with the configured map. Asserting
    /// that would have pinned a rewrite that does not happen. What is pinned
    /// instead is the refusal, and the absence of both spellings on disk.
    #[tokio::test]
    async fn a_rename_rule_output_is_refused_even_when_sanitize_names_is_enabled() {
        let root = mediapm_utils::temp::artifact_dir().unwrap();
        let paths = MediaPmPaths::from_root(root.path());
        let cas = open_hierarchy_cas(&paths).await;
        let members = [("cover.jpg", &b"rename-rule-bytes"[..])];
        let hash = cas
            .put(bytes::Bytes::from(zip_payload(&members)))
            .await
            .expect("the archive enters the store");

        let mut document = folder_and_playlist_document(&hash.to_string());
        document.hierarchy.retain(|node| matches!(node.kind, HierarchyNodeKind::MediaFolder));
        let folder = document.hierarchy.first_mut().expect("the folder node is retained");
        folder.sanitize_names = Some(SanitizeNamesConfig::Enabled);
        folder.rename_files = vec![HierarchyFolderRenameRule {
            pattern: "^cover".to_string(),
            replacement: UNSAFE_REPLACEMENT.to_string(),
        }];

        let result = sync_hierarchy(
            &paths,
            &document,
            &mut MediaPmState::default(),
            &cas,
            true,
            &ConductorState::new_empty(),
            &NickelDocument::default(),
            None,
            None,
        )
        .await;

        let folder_dir = root.path().join("album");
        let rewritten = folder_dir.join(REWRITTEN_MEMBER_NAME);
        assert!(
            !rewritten.exists(),
            "'{}' was written, so the configured replacement map reached the rename chain; \
             the member name is not the user's to rewrite",
            rewritten.display()
        );

        let error = result.err().unwrap_or_else(|| {
            panic!(
                "a replacement emitting '{UNSAFE_REPLACEMENT}' must be refused under \
                 sanitize_names = Enabled, not rewritten"
            )
        });
        assert!(
            error.to_string().contains(REPLACED_LEAF),
            "the error must name the file name the rule produced; got: {error}"
        );
    }

    /// A media entry that writes its variant walks `[stg]`, `[vrf]`, `[cmt]`.
    ///
    /// The fixture the `[cmt]` row of the coverage matrix leaned on was a
    /// media entry with no variant hash. That entry stops at the verify
    /// phase and finishes as skipped, so its row never reached the commit
    /// phase and the tag had nothing behind it. This entry carries a hash the
    /// workspace can resolve, so the arm gets as far as the write.
    ///
    /// The tags are read off the recorded ops rather than off a drawn frame,
    /// so the assertion is which phase the row was on, not which glyph that
    /// phase happened to draw. The bytes come back off disk as well, so a row
    /// cannot claim a commit that did not land.
    #[tokio::test]
    async fn a_written_media_entry_walks_stg_vrf_cmt() {
        let root = mediapm_utils::temp::artifact_dir().unwrap();
        let paths = MediaPmPaths::from_root(root.path());
        let cas = open_hierarchy_cas(&paths).await;
        let payload = b"phase-sequence-bytes";
        let hash = cas.put(bytes::Bytes::from_static(payload)).await.unwrap();
        let mut document = folder_and_playlist_document(&hash.to_string());
        document.hierarchy.retain(|node| matches!(node.kind, HierarchyNodeKind::Media));

        let (recording, overall) = RecordingProgressTracker::with_overall("materializing", 1);
        sync_hierarchy(
            &paths,
            &document,
            &mut MediaPmState::default(),
            &cas,
            true,
            &ConductorState::new_empty(),
            &NickelDocument::default(),
            Some(Arc::new(recording.clone())),
            Some(Arc::new(overall)),
        )
        .await
        .expect("a media entry holding a resolvable variant hash should write");

        let ops = recording.ops();
        let tags: Vec<Option<&str>> = ops
            .iter()
            .filter_map(|op| match op {
                ProgressOp::SetTruncation { prefix, .. } if prefix.contains("song ") => {
                    Some(rendered_phase_tag(prefix))
                }
                _ => None,
            })
            .collect();
        assert_eq!(
            tags,
            [Some("stg"), Some("vrf"), Some("cmt")],
            "a written media entry stages, verifies, then commits, and names no other phase; \
             got {ops:?}",
        );

        assert_eq!(
            std::fs::read(root.path().join("song")).unwrap_or_default().as_slice(),
            payload.as_slice(),
            "the row reached [cmt] because the variant it staged landed in the library",
        );
    }

    /// A playlist that fails before its write stays on `[stg]`.
    ///
    /// The playlist arm reaches `[cmt]` at the write, not at the top of the
    /// function, so an entry that fails while resolving a reference has
    /// committed nothing and says so. This is deliberate rather than an
    /// oversight: a row that reached `[cmt]` before the write would claim a
    /// commit that never happened.
    ///
    /// The same failure was previously visible only in the
    /// `mediapm_progress_materialize` transcripts, which pin what a row looks
    /// like and not that the phase is a consequence of where the arm failed.
    #[tokio::test]
    async fn a_failed_playlist_stays_on_stg() {
        let root = mediapm_utils::temp::artifact_dir().unwrap();
        let paths = MediaPmPaths::from_root(root.path());
        let cas = open_hierarchy_cas(&paths).await;

        let (recording, overall) = RecordingProgressTracker::with_overall("materializing", 1);
        let result = sync_hierarchy(
            &paths,
            &playlist_with_unknown_reference_document(),
            &mut MediaPmState::default(),
            &cas,
            true,
            &ConductorState::new_empty(),
            &NickelDocument::default(),
            Some(Arc::new(recording.clone())),
            Some(Arc::new(overall)),
        )
        .await;

        assert!(result.is_err(), "an unknown reference must fail the playlist: {result:?}");
        let ops = recording.ops();
        let playlist_tags: Vec<Option<&str>> = ops
            .iter()
            .filter_map(|op| match op {
                ProgressOp::SetTruncation { prefix, .. } if prefix.contains("broken.m3u8 ") => {
                    Some(rendered_phase_tag(prefix))
                }
                _ => None,
            })
            .collect();
        assert_eq!(
            playlist_tags,
            [Some("stg"), Some("stg")],
            "a playlist that never reached the write must never claim [cmt]; got {ops:?}",
        );
        assert_eq!(
            ops.iter().rev().find_map(|op| match op {
                ProgressOp::SetTruncation { prefix, .. } if prefix.contains("broken.m3u8 ") => {
                    Some(prefix.as_str())
                }
                _ => None,
            }),
            Some("[F] broken.m3u8 playlists [stg]"),
            "the failure must finish the row on the phase it stopped at; got {ops:?}",
        );
    }

    /// The overall row finishes `FinishError` when an entry fails and
    /// `FinishSuccess` when the only entry was skipped.
    ///
    /// The overall handle and the per-entry rows share the op vocabulary, so
    /// an unfiltered search for a finish finds whichever row emitted it first
    /// and a test written that way says nothing about the overall row. Every
    /// finish assertion here is therefore keyed on the [`BarId`] of the
    /// handle [`RecordingProgressTracker::with_overall`] handed back.
    ///
    /// Each half establishes the entry's outcome from the run itself before
    /// reading the overall finish, so the assertion cannot pass against a run
    /// that did the opposite: the failing half checks the returned `Err` and
    /// the entry row's own `FinishError`, the skipping half checks
    /// `skipped_paths` and the entry row's `FinishWarning`.
    ///
    /// What this pins is the implemented contract, which is not what the
    /// coverage-matrix row originally claimed. The row asked for
    /// `finish_warning` when any entry is skipped; the overall handle has no
    /// `finish_warning` call, and a skipped entry ends `FinishSuccess`. The
    /// open product question is whether one skipped entry should turn the
    /// whole screen yellow; until that is decided deliberately, the behaviour
    /// to protect is the one that runs.
    #[tokio::test]
    async fn the_overall_row_finishes_error_on_a_failed_entry_and_success_on_a_skipped_one() {
        let failed = {
            let root = mediapm_utils::temp::artifact_dir().unwrap();
            let paths = MediaPmPaths::from_root(root.path());
            let cas = open_hierarchy_cas(&paths).await;
            let (recording, overall) = RecordingProgressTracker::with_overall("materializing", 1);

            let result = sync_hierarchy(
                &paths,
                &playlist_with_unknown_reference_document(),
                &mut MediaPmState::default(),
                &cas,
                true,
                &ConductorState::new_empty(),
                &NickelDocument::default(),
                Some(Arc::new(recording.clone())),
                Some(Arc::new(overall.clone())),
            )
            .await;

            assert!(result.is_err(), "the broken playlist must fail the run: {result:?}");
            assert_eq!(
                entry_finishes(&recording),
                vec![ProgressOp::FinishError],
                "the failing entry row must be the one that reports the error; got {recorded:?}",
                recorded = recording.recorded()
            );
            (recording, overall)
        };
        assert_eq!(
            overall_finish(&failed.0, &failed.1),
            Some(ProgressOp::FinishError),
            "a run with a failed entry must end the overall row as an error; got {ops:?}",
            ops = failed.0.recorded()
        );

        let skipped = {
            let root = mediapm_utils::temp::artifact_dir().unwrap();
            let paths = MediaPmPaths::from_root(root.path());
            let cas = open_hierarchy_cas(&paths).await;
            let (recording, overall) = RecordingProgressTracker::with_overall("materializing", 1);

            let result = sync_hierarchy(
                &paths,
                &single_media_document("src1", HierarchyPath::simple("test_file")),
                &mut MediaPmState::default(),
                &cas,
                true,
                &ConductorState::new_empty(),
                &NickelDocument::default(),
                Some(Arc::new(recording.clone())),
                Some(Arc::new(overall.clone())),
            )
            .await;

            let report = result.expect("a skipped entry is not a failed run");
            assert_eq!(
                report.skipped_paths, 1,
                "the run must really have skipped its one entry before the overall finish is read; \
                 got {report:?}"
            );
            assert_eq!(
                entry_finishes(&recording),
                vec![ProgressOp::FinishWarning],
                "the skipped entry row must be the one carrying the warning; got {ops:?}",
                ops = recording.ops()
            );
            (recording, overall)
        };
        assert_eq!(
            overall_finish(&skipped.0, &skipped.1),
            Some(ProgressOp::FinishSuccess),
            "a run whose only entry was skipped must still end the overall row as a success; \
             got {ops:?}",
            ops = skipped.0.recorded()
        );
    }

    /// Returns the terminal op of every bar the tracker opened other than the
    /// overall one, in the order the bars were added.
    fn entry_finishes(tracker: &RecordingProgressTracker) -> Vec<ProgressOp> {
        tracker
            .recorded()
            .iter()
            .filter(|entry| entry.bar != BarId::Index(0))
            .filter_map(|entry| match entry.op {
                ProgressOp::FinishSuccess | ProgressOp::FinishWarning | ProgressOp::FinishError => {
                    Some(entry.op.clone())
                }
                _ => None,
            })
            .collect()
    }

    /// Returns the terminal op of one handle's own bar, or `None` when it never
    /// finished.
    fn overall_finish(
        tracker: &RecordingProgressTracker,
        overall: &mediapm_utils::progress::recording::RecordingTrackedHandle,
    ) -> Option<ProgressOp> {
        let bar = overall.bar();
        tracker.recorded().iter().find_map(|entry| {
            if entry.bar != bar {
                return None;
            }
            match entry.op {
                ProgressOp::FinishSuccess | ProgressOp::FinishWarning | ProgressOp::FinishError => {
                    Some(entry.op.clone())
                }
                _ => None,
            }
        })
    }

    /// A playlist at `playlists/broken.m3u8` whose only item names a hierarchy
    /// id no media entry declares, so the arm fails while resolving references
    /// and never reaches the write.
    fn playlist_with_unknown_reference_document() -> MediaPmDocument {
        MediaPmDocument {
            hierarchy: vec![HierarchyNode {
                path: HierarchyPath::from("playlists"),
                kind: HierarchyNodeKind::Folder,
                id: None,
                media_id: None,
                variant: None,
                variants: vec![],
                rename_files: vec![],
                format: PlaylistFormat::M3u8,
                ids: vec![],
                sanitize_names: Some(SanitizeNamesConfig::Inherit),
                children: vec![HierarchyNode {
                    path: HierarchyPath::from("broken.m3u8"),
                    kind: HierarchyNodeKind::Playlist,
                    id: None,
                    media_id: None,
                    variant: None,
                    variants: vec![],
                    rename_files: vec![],
                    format: PlaylistFormat::M3u8,
                    ids: vec![PlaylistItemRef::Shorthand("absent.local.1".to_string())],
                    sanitize_names: Some(SanitizeNamesConfig::Inherit),
                    children: vec![],
                }],
            }],
            ..MediaPmDocument::default()
        }
    }

    /// A media folder at `album` whose single variant writes one file, and a
    /// playlist at `playlists/rickroll.m3u8` that references the media entry
    /// beside it.
    ///
    /// The playlist names the media entry by its hierarchy id, which is why
    /// the media node carries an explicit `id`.
    fn folder_and_playlist_document(variant_hash: &str) -> MediaPmDocument {
        let media_node = |id: Option<&str>| HierarchyNode {
            path: HierarchyPath::simple("song"),
            kind: HierarchyNodeKind::Media,
            id: id.map(str::to_string),
            media_id: Some("src1".to_string()),
            variant: Some("default".into()),
            variants: vec![],
            rename_files: vec![],
            format: PlaylistFormat::M3u8,
            ids: vec![],
            sanitize_names: Some(SanitizeNamesConfig::Inherit),
            children: vec![],
        };
        MediaPmDocument {
            media: BTreeMap::from([(
                "src1".to_string(),
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
                    variant_hashes: BTreeMap::from([("default".to_string(), variant_hash.into())]),
                    ..MediaSourceSpec::default()
                },
            )]),
            hierarchy: vec![
                media_node(Some("song.local.1")),
                HierarchyNode {
                    path: HierarchyPath::simple("album"),
                    kind: HierarchyNodeKind::MediaFolder,
                    id: None,
                    media_id: Some("src1".to_string()),
                    variant: Some("default".into()),
                    variants: vec![],
                    rename_files: vec![],
                    format: PlaylistFormat::M3u8,
                    ids: vec![],
                    sanitize_names: Some(SanitizeNamesConfig::Inherit),
                    children: vec![],
                },
                HierarchyNode {
                    path: HierarchyPath::from("playlists"),
                    kind: HierarchyNodeKind::Folder,
                    id: None,
                    media_id: None,
                    variant: None,
                    variants: vec![],
                    rename_files: vec![],
                    format: PlaylistFormat::M3u8,
                    ids: vec![],
                    sanitize_names: Some(SanitizeNamesConfig::Inherit),
                    children: vec![HierarchyNode {
                        path: HierarchyPath::from("rickroll.m3u8"),
                        kind: HierarchyNodeKind::Playlist,
                        id: None,
                        media_id: None,
                        variant: None,
                        variants: vec![],
                        rename_files: vec![],
                        format: PlaylistFormat::M3u8,
                        ids: vec![PlaylistItemRef::Shorthand("song.local.1".to_string())],
                        sanitize_names: Some(SanitizeNamesConfig::Inherit),
                        children: vec![],
                    }],
                },
            ],
            ..MediaPmDocument::default()
        }
    }

    /// Builds a one-media-entry document whose hierarchy path is `path`.
    fn single_media_document(media_id: &str, path: HierarchyPath) -> MediaPmDocument {
        MediaPmDocument {
            media: BTreeMap::from([(
                media_id.to_string(),
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
                path,
                kind: HierarchyNodeKind::Media,
                id: None,
                media_id: Some(media_id.to_string()),
                variant: Some("default".into()),
                variants: vec![],
                rename_files: vec![],
                format: PlaylistFormat::M3u8,
                ids: vec![],
                sanitize_names: Some(SanitizeNamesConfig::Inherit),
                children: vec![],
            }],
            ..MediaPmDocument::default()
        }
    }

    /// Opens a CAS under the workspace runtime root for a `sync_hierarchy` call.
    async fn open_hierarchy_cas(paths: &MediaPmPaths) -> FileSystemCas {
        let cas_root = paths.runtime_root.join("store");
        tokio::fs::create_dir_all(&cas_root).await.unwrap();
        FileSystemCas::open(&cas_root).await.unwrap()
    }

    /// Builds a one-media-entry document whose source declares `metadata_key` as
    /// `metadata_value`, in addition to the shape [`single_media_document`]
    /// builds.
    ///
    /// The metadata-carrying form exists because a media id is no longer a
    /// legal route for unvalidated text into a path component:
    /// `config::hierarchy_types::validate_media_id` rejects a separator at the
    /// config boundary, so `${media.id}` can no longer reach the sanitizing
    /// stage. `${media.metadata.<key>}` is the interpolation that still carries
    /// text the user does not control, and therefore the one the sanitizing
    /// stage must normalize.
    fn single_media_document_with_metadata(
        media_id: &str,
        metadata_key: &str,
        metadata_value: &str,
        path: HierarchyPath,
    ) -> MediaPmDocument {
        let mut document = single_media_document(media_id, path);
        let source = document.media.get_mut(media_id).unwrap();
        source.metadata.insert(
            metadata_key.to_string(),
            MediaMetadataValue::Literal(metadata_value.to_string()),
        );
        document
    }

    /// End-to-end negative control for hierarchy path validation on the
    /// materializer commit path.
    ///
    /// A statically declared `..` component survives the config-level
    /// reserved-character check (`.` is not reserved), so it reaches
    /// `sync_hierarchy`. Before the validation chain was wired in, the entry
    /// was processed and its `hierarchy_root/..` target — one level *above*
    /// the library root — was accepted, so this test fails on that tree.
    ///
    /// The guarantee protected here: a rejected component aborts the sync
    /// before any worker starts, so no staging bar is created and nothing is
    /// written outside the hierarchy root.
    #[tokio::test]
    async fn regression_sync_hierarchy_rejects_parent_traversal_component() {
        let root = mediapm_utils::temp::artifact_dir().unwrap();
        let paths = MediaPmPaths::from_root(root.path());
        let cas = open_hierarchy_cas(&paths).await;

        let document = single_media_document("src1", HierarchyPath::simple(".."));
        let mut state = MediaPmState::default();
        let conductor_state = ConductorState::new_empty();
        let generated_doc = NickelDocument::default();

        let (recording, overall) = RecordingProgressTracker::with_overall("materializing", 1);
        let result = sync_hierarchy(
            &paths,
            &document,
            &mut state,
            &cas,
            true,
            &conductor_state,
            &generated_doc,
            Some(Arc::new(recording.clone())),
            Some(Arc::new(overall)),
        )
        .await;

        let Err(error) = result else {
            panic!(
                "sync_hierarchy accepted a '..' path component, which would commit \
                 outside the hierarchy root"
            );
        };
        assert!(
            error.to_string().contains("must not be '.' or '..'"),
            "unexpected rejection reason: {error}"
        );
        assert_eq!(
            recording.ops(),
            vec![ProgressOp::AddBar { total: 1, label: "materializing".into() }],
            "rejection must precede the overall-bar setup and every entry worker, \
             so no per-entry staging bar was ever created"
        );
    }

    /// End-to-end proof that the *sanitized* component — not the raw
    /// interpolated one — is what the commit path uses.
    ///
    /// A metadata value is text the user does not control (a tag, an ffprobe
    /// string, an upstream title), and `${media.metadata.<key>}` interpolation
    /// is how it first reaches a hierarchy path component. Before the chain was
    /// wired in, the per-entry staging bar was labelled with the raw
    /// `AC/DC [stg]`, i.e. the separator survived into a path component that
    /// `hierarchy_root.join(...)` would split. The effective
    /// `SanitizeNamesConfig` for this entry resolves to `Enabled` (the
    /// flattening default), so the separator is rewritten to `_` and the commit
    /// path sees `AC_DC`.
    ///
    /// The `${media.id}` route this test originally used is no longer a case:
    /// `config::hierarchy_types::validate_media_id` rejects a separator in a
    /// media id at the config boundary, because an id is the user's own key and
    /// splitting it between the state and the disk is an identity split rather
    /// than a spelling the materializer may fix.
    #[tokio::test]
    async fn regression_sync_hierarchy_sanitizes_interpolated_path_component() {
        let root = mediapm_utils::temp::artifact_dir().unwrap();
        let paths = MediaPmPaths::from_root(root.path());
        let cas = open_hierarchy_cas(&paths).await;

        let document = single_media_document_with_metadata(
            "src1",
            "artist",
            "AC/DC",
            HierarchyPath::simple("${media.metadata.artist}"),
        );
        let mut state = MediaPmState::default();
        let conductor_state = ConductorState::new_empty();
        let generated_doc = NickelDocument::default();

        let (recording, overall) = RecordingProgressTracker::with_overall("materializing", 1);
        let result = sync_hierarchy(
            &paths,
            &document,
            &mut state,
            &cas,
            true,
            &conductor_state,
            &generated_doc,
            Some(Arc::new(recording.clone())),
            Some(Arc::new(overall)),
        )
        .await;

        assert!(result.is_ok(), "sanitization must not fail the sync: {result:?}");
        assert_eq!(
            recording.ops(),
            vec![
                ProgressOp::AddBar { total: 1, label: "materializing".into() },
                ProgressOp::SetTotal { total: 1 },
                ProgressOp::SetTruncation { prefix: "materializing".into(), suffix: String::new() },
                ProgressOp::AddBar { total: 1, label: "AC_DC [stg]".into() },
                ProgressOp::SetTruncation { prefix: "AC_DC [stg]".into(), suffix: String::new() },
                ProgressOp::SetTruncation { prefix: "AC_DC [vrf]".into(), suffix: String::new() },
                ProgressOp::Advance { delta: 1 },
                ProgressOp::SetTruncation {
                    prefix: "[W] AC_DC [vrf]".into(),
                    suffix: String::new(),
                },
                ProgressOp::FinishWarning,
                ProgressOp::Advance { delta: 1 },
                ProgressOp::FinishSuccess,
            ],
            "the interpolated separator must be sanitized out of the committed path",
        );
    }

    /// The sanitizer returns parsed components, and the returned entry renders
    /// the sanitized spelling rather than the declared one.
    ///
    /// `sanitize_and_validate_hierarchy_paths` is the only producer of
    /// `ValidatedHierarchyEntry`, so this is the seam the whole type split
    /// stands on: if it returned the declared text, every read site downstream
    /// would be joining an unvalidated string. A reserved character is injected
    /// into the flattened entry directly, because the config boundary refuses
    /// one in a declared component and the end-to-end rewriting of an
    /// interpolated one is already covered by
    /// `regression_sync_hierarchy_sanitizes_interpolated_path_component`.
    #[test]
    fn sanitize_and_validate_hierarchy_paths_returns_parsed_components() {
        let document = single_media_document("src1", HierarchyPath::simple("album"));
        let mut flattened = flatten_hierarchy_nodes_for_runtime(&document.hierarchy).unwrap();
        flattened[0].path_components = vec!["AC/DC".to_string()];

        let validated = sanitize_and_validate_hierarchy_paths(flattened).unwrap();
        assert_eq!(validated.len(), 1);
        assert_eq!(validated[0].relative_path_text(), "AC_DC");
    }

    /// A component the parser refuses fails the sanitizer, and therefore the
    /// whole sync, before any worker starts.
    ///
    /// `..` is the case the config boundary deliberately lets through: it is
    /// not a reserved character, so `validate_hierarchy_path_component`
    /// accepts it, and the materializer is the only stage that refuses it. If
    /// the split had let the unvalidated entry through, this sync would commit
    /// one level above the library root.
    #[test]
    fn sanitize_and_validate_hierarchy_paths_rejects_parent_traversal_component() {
        let document = single_media_document("src1", HierarchyPath::simple(".."));
        let flattened = flatten_hierarchy_nodes_for_runtime(&document.hierarchy).unwrap();
        let err = sanitize_and_validate_hierarchy_paths(flattened)
            .expect_err("a '..' component must fail the sanitizer");
        assert!(
            err.to_string().contains("must not be '.' or '..'"),
            "unexpected rejection reason: {err}"
        );
    }
}
