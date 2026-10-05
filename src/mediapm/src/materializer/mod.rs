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
///
/// The three path counts are disjoint and mean different things, which is why
/// they are three counters rather than one counter and an interpretation.
/// [`Self::skipped_paths`] is a clean outcome: the library matched what this
/// run resolved, so the run wrote nothing. How much that match was proved
/// depends on the method that produced each output, which
/// [`Self::skipped_paths`] describes. [`Self::missing_paths`] is the opposite,
/// a library left short an entry because its content was unavailable.
#[derive(Debug, Clone, PartialEq, Eq, Default)]
pub struct MaterializeReport {
    /// Number of hierarchy paths materialized (new or updated).
    pub materialized_paths: usize,
    /// Number of hierarchy paths the run left untouched because each already
    /// matched the resolved content. A hardlinked or symlinked output is
    /// checked against the CAS object it links to, and a reflinked or copied
    /// one is checked by length and then by hash; the materializer's
    /// `target_already_holds` says what each of those establishes.
    pub skipped_paths: usize,
    /// Number of hierarchy paths the run left unwritten because it could not
    /// produce their output.
    pub missing_paths: usize,
    /// Number of stale hierarchy paths removed.
    pub removed_paths: usize,
    /// Number of empty parent directories removed after stale path cleanup.
    pub removed_empty_dirs: usize,
    /// Non-fatal notices collected during materialization.
    pub notices: Vec<String>,
}

/// What one hierarchy entry's worker did with it.
///
/// A bool could carry two of these and lose the third, which is exactly the
/// confusion these counters exist to remove: a variant that resolved no
/// content hash and a variant already on disk both wrote nothing, and only one
/// of them leaves the library short an entry.
enum EntryOutcome {
    /// The entry's output was written.
    Materialized,
    /// The entry's output already matched the resolved content and was left
    /// alone. How much that match proved depends on the method that produced
    /// the output, and on the store being intact for the two link methods.
    AlreadyCorrect,
    /// The entry's output could not be written, so the library is left short
    /// of an entry the document asked for.
    Missing,
}

/// The result of preparing one flattened hierarchy entry.
struct PreparedHierarchyEntryResult {
    /// What the worker did with the entry.
    outcome: EntryOutcome,
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
    /// The hash [`crate::config::ManagedFileRecord`] recorded for each managed
    /// output path when this sync started, copied out of
    /// `state.managed_files` before any worker runs.
    ///
    /// Copying is what makes the comparison mean "what the previous run
    /// wrote". Reading the map through the worker would judge an entry against
    /// whatever this same run happened to have written first, so a duplicate
    /// entry could skip on the strength of the other one's work.
    recorded_hashes: BTreeMap<String, String>,
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
        recorded_hashes: state
            .managed_files
            .iter()
            .map(|(path, record)| (path.clone(), record.hash.clone()))
            .collect(),
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
                match entry_result.outcome {
                    EntryOutcome::Materialized => report.materialized_paths += 1,
                    EntryOutcome::AlreadyCorrect => report.skipped_paths += 1,
                    EntryOutcome::Missing => report.missing_paths += 1,
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

    // A path left unwritten ends the row as an error, not as a neutral
    // success: its content was unavailable, so the library is not the shape
    // the document asked for. A path that already matched its recorded hash
    // and length does not, because writing it again would have produced the
    // library the run already has.
    //
    // The `Err` below stays reserved for a real `materialize_error`. Returning
    // one here would discard the report, and the caller still needs it to say
    // how many paths materialized and how many were left missing. A run that
    // materialized most of its entries has a real result to report even when
    // one entry did not land.
    if materialize_error.is_some() || report.missing_paths > 0 {
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
        "materialization complete: {} materialized, {} already correct, {} missing, \
         {} removed, {} empty dirs removed",
        report.materialized_paths,
        report.skipped_paths,
        report.missing_paths,
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

/// Builds the state record for one materialized media output.
///
/// Both the write path and the already-correct path name the record the same
/// way, because the record states what this run resolved for the output rather
/// than what a past run left behind. A short circuit that replayed the stored
/// record would carry a stale `media_id` or `variant` into the state the
/// moment the document renamed either.
fn record_for(media_id: &str, variant: &str, hash: &Hash) -> ManagedFileRecord {
    ManagedFileRecord {
        media_id: media_id.to_string(),
        variant: variant.to_string(),
        hash: hash.to_string(),
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
                if shared.target_already_holds(&relative_path, &hash, &target_path).await {
                    // The row stays on `[vrf]`: it measured the target
                    // against the resolved content and then had nothing to
                    // commit, so a `[cmt]` tag here would name a write that
                    // never happened.
                    if let Some(bar) = entry_bar.handle() {
                        bar.advance(1);
                        bar.finish_success();
                    }
                    return Ok(PreparedHierarchyEntryResult {
                        outcome: EntryOutcome::AlreadyCorrect,
                        managed_files: BTreeMap::from([(
                            relative_path.clone(),
                            record_for(media_id, &effective_variant, &hash),
                        )]),
                        media_variant_updates: BTreeMap::from([(
                            media_id.clone(),
                            BTreeMap::from([(effective_variant.clone(), hash.to_string())]),
                        )]),
                    });
                }

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

                let record = record_for(media_id, &effective_variant, &materialized_hash);
                if let Some(bar) = entry_bar.handle() {
                    bar.advance(1);
                    bar.finish_success();
                }
                Ok(PreparedHierarchyEntryResult {
                    outcome: EntryOutcome::Materialized,
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
                    "media '{media_id}' variant '{effective_variant}' has no content hash, so its \
                     output was not written"
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
                    outcome: EntryOutcome::Missing,
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
            //
            // The row reports what the entry did, not only whether it raised.
            // A folder that could not write one variant left the library short
            // of what the document asked for, which is the same warning the
            // media arm gives for a variant with no content hash.
            if let Some(bar) = entry_bar.handle() {
                bar.advance(1);
                match result {
                    Ok(ref prepared) if matches!(prepared.outcome, EntryOutcome::Missing) => {
                        entry_bar.finish("W");
                        bar.finish_warning();
                    }
                    Ok(_) => bar.finish_success(),
                    Err(_) => {
                        entry_bar.finish("F");
                        bar.finish_error();
                    }
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
    // Whether any variant ended the loop without a file. The counters are per
    // hierarchy path, and a folder is one path however many variants it holds,
    // so one blocked variant makes the whole entry unwritten: counting it as
    // materialized would report a library the document did not ask for, and
    // the files the other variants did write are recorded in `managed_files`
    // either way.
    //
    // An entry with no variants starts out unwritten, because the loop below
    // runs zero times and reaches none of the arms that set this. That is the
    // one shape in which the loop leaves the flag false without having
    // written anything.
    let mut unwritten_variant = selected_variants.is_empty();
    if selected_variants.is_empty() {
        shared
            .notice(format!("media '{media_id}' resolved to no variants, so nothing was written"));
    }

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
                unwritten_variant = true;
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
                unwritten_variant = true;
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
                    "media '{media_id}' variant '{variant_name}': not writing '{}' because it is \
                     already a directory",
                    variant_path.display()
                ));
                unwritten_variant = true;
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
        outcome: if unwritten_variant { EntryOutcome::Missing } else { EntryOutcome::Materialized },
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
        outcome: EntryOutcome::Materialized,
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

    /// Whether `target_path` still holds what `hash` names, closely enough
    /// that writing it again would reproduce the file the run already has.
    ///
    /// A recorded hash answers "what did an earlier run put here". It says
    /// nothing about what the file holds now, and a managed library is a
    /// directory something else can write into. A user editing a file in place,
    /// a tool writing through the library, and a restore from backup all leave
    /// the record untouched while the bytes change, so the record has to name
    /// this hash and the file's own content has to be established separately.
    ///
    /// How the content gets established depends on the relationship between
    /// the output and the CAS object, because two of the four methods leave a
    /// relationship that answers the question outright.
    ///
    /// - A hardlink and the object are one inode. Comparing device and inode
    ///   is the whole check: two stat calls, no bytes read, and nothing that
    ///   can be wrong about what the output holds.
    /// - A symlink names the object's path. Reading the link and comparing it
    ///   to that path is the whole check: one syscall, no bytes read. A link
    ///   naming anywhere else is wrong whatever it resolves to, because a
    ///   managed output is materialized from the store.
    /// - A reflink and a copy are separate inodes by construction, and the
    ///   blocks a reflink shares say nothing about the bytes in the file. The
    ///   length decides first, and a matching length is followed by hashing
    ///   the target, so an edit that kept the number of bytes is caught at
    ///   the cost of reading the outputs this run declined to write.
    ///
    /// The check reads which relationship is present rather than being told
    /// which method ran, because no record of that survives to the next run:
    /// the write path tries the four methods in order and falls back whenever
    /// one fails, and nothing persisted says which one succeeded. Probing for
    /// the relationship reports what is on disk whichever method put it there.
    ///
    /// # What each branch assumes
    ///
    /// Both link branches assume the store is intact, and detecting a store
    /// that is not is the store's job. A hardlink's content is the object's
    /// content by construction, so an edit written through the library path
    /// edits the object with it. That is a corrupted store rather than a stale
    /// library, and rehashing the object here would answer a question this
    /// layer has no standing to answer.
    ///
    /// A path no record names, a path that is not a regular file, and a CAS
    /// that cannot answer for the hash all decline the skip. Declining costs a
    /// rewrite that reproduces the file the target should have held.
    ///
    /// A variant bound to a ZIP member never matches, because the record holds
    /// the hash of the extracted member while `hash` names the archive. That
    /// is the safe direction to be wrong in: the arm extracts, finds the
    /// member's hash differs, and writes.
    async fn target_already_holds(
        &self,
        relative_path: &str,
        hash: &Hash,
        target_path: &Path,
    ) -> bool {
        let Some(recorded) = self.recorded_hashes.get(relative_path) else {
            return false;
        };
        if recorded != &hash.to_string() {
            return false;
        }
        // A blob the store holds as a WAL entry or as a delta is not a file on
        // disk, so no link can point at it and the content branch answers on
        // its own.
        if let Some(object_path) =
            self.cas.object_path_for_hash(*hash).filter(|path| path.is_file())
        {
            match file_ops::output_relationship(target_path, &object_path).await {
                file_ops::OutputRelationship::Linked => return true,
                file_ops::OutputRelationship::Mislinked => return false,
                file_ops::OutputRelationship::Separate => {}
            }
        }
        self.target_content_matches(hash, target_path).await
    }

    /// Whether the bytes at `target_path` are what `hash` names.
    ///
    /// The branch a reflink and a copy land in, the two methods that leave no
    /// relationship to compare. Length first, because one stat settles the
    /// common case of a file something else replaced, and a matching length is
    /// followed by hashing the target.
    async fn target_content_matches(&self, hash: &Hash, target_path: &Path) -> bool {
        // Metadata follows symlinks, so a link whose target is gone reads as
        // absent and gets rewritten instead of standing in for the file it
        // once named.
        let Ok(metadata) = tokio::fs::metadata(target_path).await else {
            return false;
        };
        if !metadata.is_file() {
            return false;
        }
        match self.cas.stat(*hash).await {
            Ok(expected) if metadata.len() != expected.len => false,
            Ok(_) => file_ops::file_content_matches_hash(target_path, hash).await,
            // A CAS that cannot answer for this hash holds nothing this run
            // could write either, so declining the skip costs one failed
            // write and reports the cause with the path it failed on.
            Err(_) => false,
        }
    }
}

#[cfg(test)]
mod tests_common;
#[cfg(test)]
mod tests_entry_row_phases;
#[cfg(test)]
mod tests_entry_row_total;
#[cfg(test)]
mod tests_overall_row_finish;
#[cfg(test)]
mod tests_path_validation;
#[cfg(test)]
mod tests_phase_sequence;
#[cfg(test)]
mod tests_progress_ops;
#[cfg(test)]
mod tests_reserved_names;
#[cfg(test)]
mod tests_unchanged_targets;
#[cfg(test)]
mod tests_yt_dlp_sandbox_paths;
