//! # Invalidating a media step marks its recorded instances
//!
//! Conductor instance keys are content-derived: an unchanged step is served
//! from cache however much wall-clock time passed. Explicit invalidation is
//! therefore the only way to re-execute a step whose external circumstances
//! changed, and the marker that expresses it lives on the recorded instance
//! itself — `ToolCallInstance.invalidated_at` — so the probe that decides reuse
//! is the one place that has to consult it.
//!
//! These tests pin the mediapm half: `media invalidate` resolves a media's step
//! index to the step ids the generated document assigned, marks every instance
//! that served one of them, and `--undo` clears the same markers. Nothing else
//! is written: the request is a marker on the instance, so no queue carries it
//! forward and no recorded progress has to be cleared to make it take effect.
//!
//! Every test walks the full sequence — a proven cache hit, the invalidation, the
//! re-run — because a "re-executed" step is meaningless without a cache hit
//! first: it could just be work the first run never finished.

use mediapm::{
    AddInsertPosition, MediaPmError, MediaPmPaths, MediaPmService, MediaRuntimeStorage,
    load_mediapm_state_document, media_id_from_uri,
};
use mediapm_cas::Hash;
use mediapm_conductor::ConductorState;
use url::Url;

use crate::common::{service_with_cache, sync_library_with_test_terminal};
use crate::int::sync_idempotency_double_run::{
    SOURCE_URI, import_source_spec, seed_probe_workspace,
};

/// URI of the second, unrelated media source the "other work stays cached"
/// assertion needs.
const OTHER_SOURCE_URI: &str = "local:invalidation-other-probe";

/// Payload seeded into CAS for that second source, distinct from the first so
/// the two workflows never share an instance.
const OTHER_PAYLOAD: &[u8] = b"a second probe payload nothing else ingests";

/// Reads the conductor state as it was last written to disk.
///
/// The service's conductor holds an in-memory snapshot, so a test that reads it
/// would prove only that the call mutated memory. The marker exists so the next
/// sync — possibly in another process — misses, which makes the file the
/// contract.
fn conductor_state_on_disk(paths: &MediaPmPaths) -> ConductorState {
    let bytes = std::fs::read(&paths.conductor_state_config).expect("read conductor state");
    mediapm_conductor::decode_state_json(&bytes).expect("decode conductor state")
}

/// Asserts the marker reaches exactly the instances that served `step_id`.
///
/// The instances are named by the key they are filed under, because that is the
/// only identity a record has.
fn assert_step_instances_marked(
    state: &ConductorState,
    step_id: &str,
    marked: bool,
    context: &str,
) {
    let served: Vec<_> = state
        .tool_call_instances
        .iter()
        .filter(|(_, instance)| instance.step_ids.iter().any(|step| step.step_id == step_id))
        .collect();
    assert!(!served.is_empty(), "{context}: no recorded instance served '{step_id}'");
    for (key, instance) in served {
        assert_eq!(
            instance.invalidated_at.is_some(),
            marked,
            "{context}: instance {} of '{step_id}' must be {}",
            key.to_hex(),
            if marked { "marked" } else { "unmarked" },
        );
    }
}

/// Builds the single-step offline import source publishing `variant`.
///
/// The variant name is part of the synthesized step id
/// (`step-0-0-import-{variant}-to-{variant}`), so naming a different one is how
/// a second media gets a step id of its own instead of sharing the first's.
fn import_source_spec_with_variant(hash: &str, variant: &str) -> mediapm::MediaSourceSpec {
    let mut spec = import_source_spec(hash);
    spec.steps[0].output_variants = std::collections::BTreeMap::from([(
        variant.to_string(),
        mediapm::OutputVariantValue::YtDlp(mediapm::YtDlpOutputVariantConfig::default()),
    )]);
    spec
}

/// Adds a second, independent media source so a sync runs two workflows.
///
/// Neither of them is bound to a hierarchy node: these tests assert which steps
/// re-execute, and materialization would only add a second thing to fail.
async fn seed_other_workspace(
    service: &mut MediaPmService<mediapm_cas::FileSystemCas>,
    variant: &str,
) -> Result<String, MediaPmError> {
    let uri = Url::parse(OTHER_SOURCE_URI).expect("source URI must parse");
    let media_id = media_id_from_uri(&uri);
    let hash = crate::common::seed_cas(
        service,
        bytes::Bytes::copy_from_slice(OTHER_PAYLOAD),
        "second probe payload",
    )
    .await?;
    service.add_media_source(
        &import_source_spec_with_variant(&hash.to_string(), variant),
        media_id.clone(),
        &uri,
        None,
        None,
    )?;
    Ok(media_id)
}

/// Invalidating a step marks its instances and leaves the recorded progress
/// alone.
///
/// The user-level guarantee: `mediapm media invalidate` is a reversible request
/// to re-execute, not a state edit. The `managed_files` records are what the
/// materializer projects into the library; clearing them would destroy
/// information an undo could not restore, and nothing in the request needs them
/// gone for it to take effect.
#[tokio::test]
async fn invalidating_marks_the_instances_and_leaves_recorded_progress_alone()
-> Result<(), MediaPmError> {
    let (mut service, _root, _cache) = service_with_cache(MediaRuntimeStorage::default()).await?;
    let uri = Url::parse(SOURCE_URI).expect("source URI must parse");
    let media_id = media_id_from_uri(&uri);
    seed_probe_workspace(&mut service, &uri).await?;

    let first = sync_library_with_test_terminal(&mut service, false).await?;
    assert!(first.executed_instances >= 1, "the first sync must execute the workflow: {first:?}");

    // Capture the records the sync wrote, so "untouched" is an assertion about a
    // value that is actually there.
    let paths = service.paths().clone();
    let before = load_mediapm_state_document(&paths.mediapm_state_json)?;
    let recorded_files = before.managed_files.clone();
    assert!(
        !recorded_files.is_empty(),
        "the sync must record the files it materialized: {recorded_files:?}"
    );

    let summary = service.invalidate_media_step_tool_calls(&media_id, 0).await?;
    assert_eq!(
        summary.workflow_id,
        format!("mediapm.media.{media_id}"),
        "the summary must name the workflow the step belongs to",
    );
    assert_eq!(
        summary.targeted_step_ids.len(),
        1,
        "one step index resolves to one step: {summary:?}"
    );
    assert_eq!(
        summary.changed_instances, 1,
        "the recorded instance must be the one this call marked: {summary:?}"
    );
    assert_eq!(
        summary.unchanged_instances, 0,
        "a first invalidation leaves nothing unchanged: {summary:?}"
    );

    let after = load_mediapm_state_document(&paths.mediapm_state_json)?;
    assert_eq!(
        after.managed_files, recorded_files,
        "invalidation must not clear the recorded materialized files",
    );

    let marked = conductor_state_on_disk(&paths);
    assert_step_instances_marked(&marked, &summary.targeted_step_ids[0], true, "after invalidate");

    drop(service);
    Ok(())
}

/// A re-run executes the targeted workflow and serves every other one from
/// cache.
///
/// The user-level guarantee: invalidation is targeted. Marking one media's step
/// must not cost a re-run of unrelated media, which is the whole reason the
/// marker sits on the instances rather than on a run-wide flag.
#[tokio::test]
async fn invalidating_one_workflow_leaves_the_other_cached() -> Result<(), MediaPmError> {
    let (mut service, _root, _cache) = service_with_cache(MediaRuntimeStorage::default()).await?;
    let uri = Url::parse(SOURCE_URI).expect("source URI must parse");
    let media_id = media_id_from_uri(&uri);
    seed_probe_workspace(&mut service, &uri).await?;
    let other_media_id = seed_other_workspace(&mut service, "secondary").await?;

    let first = sync_library_with_test_terminal(&mut service, false).await?;
    assert_eq!(
        first.executed_instances, 2,
        "two media sources must execute one step each: {first:?}"
    );
    let cached = sync_library_with_test_terminal(&mut service, false).await?;
    assert_eq!(
        cached.executed_instances, 0,
        "an unchanged re-run must be served from cache before invalidation: {cached:?}"
    );
    assert_eq!(cached.cached_instances, 2, "both steps must be cache hits: {cached:?}");

    let summary = service.invalidate_media_step_tool_calls(&media_id, 0).await?;
    assert_eq!(summary.changed_instances, 1, "only the targeted media is marked: {summary:?}");

    let forced = sync_library_with_test_terminal(&mut service, false).await?;
    assert_eq!(
        forced.executed_instances, 1,
        "exactly the invalidated workflow must re-execute: {forced:?}",
    );
    assert_eq!(
        forced.cached_instances, 1,
        "the unrelated workflow must stay cached: {forced:?} \
         (other media id: {other_media_id})",
    );

    // The marker is gone because the re-execution recorded a fresh instance
    // under the same key, not because anything spent it.
    let after = conductor_state_on_disk(&service.paths().clone());
    assert_step_instances_marked(&after, &summary.targeted_step_ids[0], false, "after re-run");

    let free = sync_library_with_test_terminal(&mut service, false).await?;
    assert_eq!(
        free.executed_instances, 0,
        "the sync after the re-run must be cache-first again: {free:?}"
    );

    drop(service);
    Ok(())
}

/// Undo clears the marker, so the recorded work is reusable again.
///
/// The user-level guarantee: invalidation is reversible. The operator asked for
/// a re-run they then changed their mind about; after `--undo` the next sync must
/// be free again, and undoing twice must say so rather than fail.
#[tokio::test]
async fn undo_clears_the_marker_so_the_next_sync_is_free() -> Result<(), MediaPmError> {
    let (mut service, _root, _cache) = service_with_cache(MediaRuntimeStorage::default()).await?;
    let uri = Url::parse(SOURCE_URI).expect("source URI must parse");
    let media_id = media_id_from_uri(&uri);
    seed_probe_workspace(&mut service, &uri).await?;

    let first = sync_library_with_test_terminal(&mut service, false).await?;
    assert!(first.executed_instances >= 1, "the first sync must execute: {first:?}");
    let cached = sync_library_with_test_terminal(&mut service, false).await?;
    assert_eq!(cached.executed_instances, 0, "the step must be cached: {cached:?}");

    let marked = service.invalidate_media_step_tool_calls(&media_id, 0).await?;
    assert_eq!(marked.changed_instances, 1, "the invalidation must mark: {marked:?}");

    let cleared = service.revalidate_media_step_tool_calls(&media_id, 0).await?;
    assert_eq!(cleared.changed_instances, 1, "undo must clear the marker it finds: {cleared:?}");
    let after_undo = conductor_state_on_disk(&service.paths().clone());
    assert_step_instances_marked(&after_undo, &marked.targeted_step_ids[0], false, "after undo");

    let repeated = service.revalidate_media_step_tool_calls(&media_id, 0).await?;
    assert_eq!(repeated.changed_instances, 0, "undoing twice changes nothing: {repeated:?}");
    assert_eq!(
        repeated.unchanged_instances, 1,
        "a repeated undo must report what it found instead of failing: {repeated:?}"
    );

    let free = sync_library_with_test_terminal(&mut service, false).await?;
    assert_eq!(
        free.executed_instances, 0,
        "undoing before the sync must let the recorded instance serve the run: {free:?}"
    );

    drop(service);
    Ok(())
}

/// A run that fails leaves the marker set for the next sync.
///
/// The user-level guarantee: an invalidation is not spent by a run that did not
/// do the work it was asked for. The marker survives precisely because nothing
/// spends it — only a fresh settlement rewrites the record — so the next sync
/// still re-executes.
#[tokio::test]
async fn a_failed_run_leaves_the_marker_set() -> Result<(), MediaPmError> {
    let (mut service, _root, _cache) = service_with_cache(MediaRuntimeStorage::default()).await?;
    let uri = Url::parse(SOURCE_URI).expect("source URI must parse");
    let media_id = media_id_from_uri(&uri);
    seed_probe_workspace(&mut service, &uri).await?;

    let first = sync_library_with_test_terminal(&mut service, false).await?;
    assert!(first.executed_instances >= 1, "the first sync must execute: {first:?}");

    let summary = service.invalidate_media_step_tool_calls(&media_id, 0).await?;
    assert_eq!(summary.changed_instances, 1, "the invalidation must mark: {summary:?}");

    // Make the run fail for a real reason: the import step reads a CAS hash the
    // store does not hold, which is how an impure step's external world moves
    // out from under an unchanged input.
    let missing = Hash::from_content(b"a payload the store never held");
    service.add_media_source_with_position(
        &import_source_spec(&missing.to_string()),
        media_id.clone(),
        &uri,
        None,
        None,
        AddInsertPosition::End,
        true,
    )?;

    let failed = sync_library_with_test_terminal(&mut service, false).await?;
    assert!(
        failed.workflow_failed_steps >= 1,
        "the run must fail on the payload the store cannot resolve: {failed:?}"
    );

    let state = conductor_state_on_disk(&service.paths().clone());
    assert_step_instances_marked(&state, &summary.targeted_step_ids[0], true, "after a failed run");

    drop(service);
    Ok(())
}

/// Marking one media's step leaves another media that reuses the step id alone.
///
/// Step ids carry no media identity: two sources of the same shape get the same
/// `step-0-0-import-…` id in their own workflows. The marker selects on the pair
/// — the workflow generated for the media and the step id inside it — so asking
/// to re-run one media's step re-runs exactly that media. Selecting on the bare
/// id would discard the other media's work, which nobody asked about, and the
/// operator would see a sync redo an unrelated download.
#[tokio::test]
async fn marking_a_shared_step_leaves_another_media_that_reuses_it_alone()
-> Result<(), MediaPmError> {
    let (mut service, _root, _cache) = service_with_cache(MediaRuntimeStorage::default()).await?;
    let uri = Url::parse(SOURCE_URI).expect("source URI must parse");
    let media_id = media_id_from_uri(&uri);
    seed_probe_workspace(&mut service, &uri).await?;
    seed_other_workspace(&mut service, "primary").await?;

    let first = sync_library_with_test_terminal(&mut service, false).await?;
    assert_eq!(first.executed_instances, 2, "both sources must execute: {first:?}");

    let summary = service.invalidate_media_step_tool_calls(&media_id, 0).await?;
    assert_eq!(
        summary.changed_instances, 1,
        "only the named media's instance carries the marker: {summary:?}",
    );

    let state = conductor_state_on_disk(&service.paths().clone());
    assert_eq!(
        state
            .tool_call_instances
            .values()
            .filter(|instance| instance.invalidated_at.is_some())
            .count(),
        1,
        "exactly one recorded instance is marked",
    );
    for instance in state.tool_call_instances.values() {
        let names_the_media =
            instance.step_ids.iter().any(|step| step.workflow_id == summary.workflow_id);
        assert_eq!(
            instance.invalidated_at.is_some(),
            names_the_media,
            "the marker must follow the workflow the request named",
        );
    }

    let forced = sync_library_with_test_terminal(&mut service, false).await?;
    assert_eq!(
        forced.executed_instances, 1,
        "the named media re-executes while the other is served from cache: {forced:?}",
    );

    drop(service);
    Ok(())
}

/// An unknown media id is an error, and a step index the workflow does not have
/// is an error too.
///
/// The user-level guarantee: a typo is reported instead of silently doing
/// nothing. A command that exits 0 after marking nothing is the failure mode the
/// whole marker design exists to remove.
#[tokio::test]
async fn unknown_media_and_absent_step_indexes_are_errors() -> Result<(), MediaPmError> {
    let (mut service, _root, _cache) = service_with_cache(MediaRuntimeStorage::default()).await?;
    let uri = Url::parse(SOURCE_URI).expect("source URI must parse");
    let media_id = media_id_from_uri(&uri);
    seed_probe_workspace(&mut service, &uri).await?;

    let unknown = service.invalidate_media_step_tool_calls("no-such-media", 0).await;
    let error = unknown.expect_err("an unknown media id must fail");
    assert!(
        error.to_string().contains("no-such-media"),
        "the diagnostic must name the media it could not find: {error}"
    );

    // Before the first sync the generated document has no workflow for this
    // media, so there is nothing to mark yet. That is a different mistake from
    // an unknown media and says so.
    let unsynced = service.invalidate_media_step_tool_calls(&media_id, 0).await;
    let error = unsynced.expect_err("a media whose workflow was never generated must fail");
    assert!(
        error.to_string().contains("sync"),
        "the diagnostic must point at the missing sync: {error}"
    );

    let first = sync_library_with_test_terminal(&mut service, false).await?;
    assert!(first.executed_instances >= 1, "the first sync must execute: {first:?}");

    let absent = service.invalidate_media_step_tool_calls(&media_id, 7).await;
    let error = absent.expect_err("a step index the workflow lacks must fail");
    assert!(
        error.to_string().contains('7'),
        "the diagnostic must name the index it could not resolve: {error}"
    );

    // Neither failure marked anything.
    let state = conductor_state_on_disk(&service.paths().clone());
    for (key, instance) in &state.tool_call_instances {
        assert!(
            instance.invalidated_at.is_none(),
            "a rejected invalidation must not mark instance {}",
            key.to_hex(),
        );
    }

    drop(service);
    Ok(())
}
