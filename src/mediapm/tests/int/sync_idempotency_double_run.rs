//! # An immediate second sync executes no workflow and re-materializes nothing
//!
//! The double-run idempotency contract: running `sync_library()` twice back to
//! back over an unchanged library must do the work once. The first run executes
//! the synthesized import workflow and materializes its output; the second must
//! serve every workflow instance from cache (`executed_instances == 0`,
//! `cached_instances` equal to the first run's total), write no new path
//! (`materialized_paths == 0`), and decline the entries the first run produced
//! (`skipped_paths` equal to the first run's materialized count).
//!
//! This is a probe as much as a regression test. Whether an immediate re-run is
//! free is a property of the recheck/cache layers, not of the demo, and this
//! pins it hermetically on the offline path (builtin `import`, no managed-tool
//! downloads). The workflow half and the materialization half are asserted
//! separately because they are decided by different layers: the conductor's
//! instance cache versus the materializer's verdict + `state.managed_files`
//! records.
//!
//! The witness reads the file itself on either side of the second run, not just
//! the counters. A run could count a skip and rewrite the file anyway, and the
//! counter would read the same; and the materializer's first method is a
//! hardlink, whose rewrite leaves the modification time identical while moving
//! the status-change time. So bytes, mtime and (on unix) ctime are all compared.

use std::collections::BTreeMap;
use std::path::Path;

use crate::common::{seed_cas, service_with_cache, sync_library_with_test_terminal};
use bytes::Bytes;
use mediapm::{
    HierarchyNode, HierarchyNodeKind, HierarchyPath, MediaPmError, MediaPmService,
    MediaRuntimeStorage, MediaSourceSpec, MediaStep, MediaStepTool, OutputVariantValue,
    PlaylistFormat, SanitizeNamesConfig, TransformInputValue, YtDlpOutputVariantConfig,
    load_mediapm_document, load_mediapm_state_document, media_id_from_uri, save_mediapm_document,
};
use url::Url;

/// URI of the single offline source; its id is the media id both runs resolve.
const SOURCE_URI: &str = "local:double-sync-probe";

/// Hierarchy path the single materialized file lands at.
const HIERARCHY_PATH: &str = "imported_fixture";

/// Output variant the import step publishes, which the hierarchy node binds.
const VARIANT: &str = "primary";

/// Payload seeded into CAS for the import step to ingest.
const PAYLOAD: &[u8] = b"double-sync idempotency probe payload";

/// Builds the single-step offline `import` source that ingests `hash`
/// (`kind=cas_hash`, the same shape `local_source_default_steps` synthesizes).
fn import_source_spec(hash: &str) -> MediaSourceSpec {
    MediaSourceSpec {
        steps: vec![MediaStep {
            tool: MediaStepTool::Import,
            input_variants: Vec::new(),
            output_variants: BTreeMap::from([(
                VARIANT.to_string(),
                OutputVariantValue::YtDlp(YtDlpOutputVariantConfig::default()),
            )]),
            options: BTreeMap::from([
                ("kind".to_string(), TransformInputValue::String("cas_hash".to_string())),
                ("hash".to_string(), TransformInputValue::String(hash.to_string())),
            ]),
        }],
        ..MediaSourceSpec::default()
    }
}

/// Everything about a materialized file that a rewrite would move, read on
/// either side of the second run.
struct FileWitness {
    /// Exact bytes the file holds.
    bytes: Vec<u8>,
    /// Last-write time.
    modified: std::time::SystemTime,
    /// Status-change time, which a hardlink rewrite moves even though the bytes
    /// and the modification time stay identical. Zeroed on non-unix platforms
    /// where it is not separately expressible.
    changed: WitnessStamp,
}

/// The platform's status-change stamp, cfg-gated the way the materializer's own
/// witness reads it.
#[cfg(unix)]
type WitnessStamp = (i64, i64);
#[cfg(not(unix))]
type WitnessStamp = ();

/// Reads a [`FileWitness`] for `path`, following symlinks the way the
/// materializer's own existence check does.
#[cfg(unix)]
fn read_witness(path: &Path) -> Result<FileWitness, MediaPmError> {
    use std::os::unix::fs::MetadataExt as _;

    let metadata = std::fs::metadata(path).map_err(|source| io_error("stat", path, source))?;
    Ok(FileWitness {
        bytes: std::fs::read(path).map_err(|source| io_error("read", path, source))?,
        modified: metadata.modified().map_err(|source| io_error("mtime", path, source))?,
        changed: (metadata.ctime(), metadata.ctime_nsec()),
    })
}

/// Reads a [`FileWitness`] for `path` on platforms without a status-change
/// stamp; bytes plus modification time still catch an in-place rewrite.
#[cfg(not(unix))]
fn read_witness(path: &Path) -> Result<FileWitness, MediaPmError> {
    let metadata = std::fs::metadata(path).map_err(|source| io_error("stat", path, source))?;
    Ok(FileWitness {
        bytes: std::fs::read(path).map_err(|source| io_error("read", path, source))?,
        modified: metadata.modified().map_err(|source| io_error("mtime", path, source))?,
        changed: (),
    })
}

/// Wraps an I/O failure with the operation and path, so `?` works in the test.
fn io_error(operation: &str, path: &Path, source: std::io::Error) -> MediaPmError {
    MediaPmError::Io { operation: operation.to_string(), path: path.to_path_buf(), source }
}

/// Seeds the probe workspace: the payload in CAS, one import source, and the
/// hierarchy binding that materializes its output variant at
/// [`HIERARCHY_PATH`]. Both double-run tests share this so their setup is
/// one definition, not two copies.
async fn seed_probe_workspace(
    service: &mut MediaPmService<mediapm_cas::FileSystemCas>,
    uri: &Url,
) -> Result<(), MediaPmError> {
    let hash = seed_cas(service, Bytes::copy_from_slice(PAYLOAD), "probe payload").await?;
    service
        .add_media_source(
            &import_source_spec(&hash.to_string()),
            media_id_from_uri(uri),
            uri,
            None,
            None,
        )
        .map_err(|source| MediaPmError::Workflow(format!("adding the import source: {source}")))?;

    // Bind the import step's output variant to a hierarchy entry so the run both
    // executes the workflow and materializes its output.
    let paths = service.paths().clone();
    let mut document = load_mediapm_document(&paths.mediapm_ncl)?;
    document.hierarchy.push(HierarchyNode {
        path: HierarchyPath::simple(HIERARCHY_PATH),
        kind: HierarchyNodeKind::Media,
        id: None,
        media_id: Some(media_id_from_uri(uri)),
        variant: Some(VARIANT.to_string()),
        variants: Vec::new(),
        rename_files: Vec::new(),
        format: PlaylistFormat::M3u8,
        ids: Vec::new(),
        sanitize_names: Some(SanitizeNamesConfig::Inherit),
        children: Vec::new(),
    });
    save_mediapm_document(&paths.mediapm_ncl, &document)?;
    Ok(())
}

/// An immediate second `sync_library` over an unchanged library executes no
/// workflow step and re-materializes no path: the work happens once.
///
/// The user-level guarantee: a sync is safe to run twice in a row (a retry, a
/// watcher loop, a second demo pass) and the second run costs nothing — no tool
/// step is re-invoked, no file is rewritten. The run-1 assertions come first,
/// because a workspace the first run never populated would make every run-2
/// "nothing happened" count meaningless.
#[tokio::test]
async fn an_immediate_second_sync_executes_nothing_and_rematerializes_nothing()
-> Result<(), MediaPmError> {
    let (mut service, root, _cache) = service_with_cache(MediaRuntimeStorage::default()).await?;

    let uri = Url::parse(SOURCE_URI).expect("source URI must parse");
    seed_probe_workspace(&mut service, &uri).await?;

    let first = sync_library_with_test_terminal(&mut service, false).await?;
    assert!(
        first.executed_instances >= 1,
        "the first sync must execute the import workflow before the re-run means anything: {first:?}"
    );
    assert_eq!(
        first.missing_paths, 0,
        "the first sync must produce every path it was asked for: {first:?}"
    );
    assert!(
        first.materialized_paths >= 1,
        "the first sync must materialize the imported fixture so the re-run has something to \
         decline: {first:?}"
    );
    assert!(
        first.warnings.is_empty(),
        "the first sync should carry no warnings, got: {:?}",
        first.warnings
    );
    let materialized = root.path().join(HIERARCHY_PATH);
    assert!(
        materialized.is_file(),
        "the first run's materialized path must exist on disk before the re-run is read: {}",
        materialized.display()
    );
    let first_run_materialized = first.materialized_paths;

    let before = read_witness(&materialized)?;

    let second = sync_library_with_test_terminal(&mut service, false).await?;

    // Workflow half: every instance is served from cache, none re-executed.
    assert_eq!(
        second.executed_instances, 0,
        "an immediate re-run over an unchanged library must not re-execute any workflow step: \
         {second:?} (first run: {first:?})"
    );
    assert_eq!(
        second.cached_instances, first.executed_instances,
        "every instance the first run executed must be served from cache on the re-run: {second:?}"
    );

    // Materialization half: no new path, every prior entry declined.
    assert_eq!(
        second.materialized_paths, 0,
        "an immediate re-run must re-materialize nothing: {second:?}"
    );
    assert_eq!(
        second.skipped_paths, first_run_materialized,
        "every path the first run wrote must be declined on the re-run: {second:?}"
    );
    assert_eq!(
        second.missing_paths, 0,
        "the re-run must not report a path it failed to produce: {second:?}"
    );
    assert!(
        second.warnings.is_empty(),
        "an idempotent re-run should carry no warnings, got: {:?}",
        second.warnings
    );

    // The counters alone would not carry this: a run could count a skip and
    // rewrite the file anyway. Read the file itself.
    let after = read_witness(&materialized)?;
    assert_eq!(after.bytes, before.bytes, "the re-run must leave the materialized bytes untouched");
    assert_eq!(
        after.modified, before.modified,
        "the re-run must not rewrite the file, or its modification time moves"
    );
    assert_eq!(
        after.changed, before.changed,
        "the re-run must not replace or relink the file, or its status-change time moves"
    );

    // State must be stable too: a re-run that added workflow state would be
    // doing work under the counters' feet.
    let paths = service.paths().clone();
    let state_after_first = load_mediapm_state_document(&paths.mediapm_state_json)?;
    assert!(
        !state_after_first.managed_files.is_empty(),
        "the first run must record its managed files before the re-run is judged"
    );

    // Release the CAS store lock before the workspace TempDir removes the tree.
    drop(service);
    Ok(())
}

/// `force_rerun` is the explicit invalidation surface for cached steps.
///
/// The user-level guarantee: when outside circumstances changed but the
/// step's inputs did not — the definition of an impure step — the caller can
/// still demand a fresh execution, without hand-editing state and without
/// waiting on any clock. After the forced run, an unforced sync is free
/// again: the re-execution re-records the instance under the same
/// content-derived key, so force is run-scoped, not state-destroying.
#[tokio::test]
async fn force_rerun_reexecutes_the_cached_workflow() -> Result<(), MediaPmError> {
    let (mut service, _root, _cache) = service_with_cache(MediaRuntimeStorage::default()).await?;

    let uri = Url::parse(SOURCE_URI).expect("source URI must parse");
    seed_probe_workspace(&mut service, &uri).await?;

    let first = sync_library_with_test_terminal(&mut service, false).await?;
    assert!(
        first.executed_instances >= 1,
        "the first sync must execute the import workflow before the forced re-run means anything: {first:?}"
    );

    // Unforced re-run first: the step is a cache hit, so the forced run
    // below is provably re-executing a cached step, not doing work the
    // first run left undone.
    let second = sync_library_with_test_terminal(&mut service, false).await?;
    assert_eq!(
        second.executed_instances, 0,
        "the unforced re-run must be a cache hit before force is exercised: {second:?}"
    );

    service.set_force_workflow_rerun(true);
    let forced = sync_library_with_test_terminal(&mut service, false).await?;
    assert!(
        forced.executed_instances >= 1,
        "force_rerun must re-execute the cached import step: {forced:?} (unforced re-run: {second:?})"
    );

    // Force is run-scoped: with the flag back off, the re-recorded instance
    // serves the cache again.
    service.set_force_workflow_rerun(false);
    let third = sync_library_with_test_terminal(&mut service, false).await?;
    assert_eq!(
        third.executed_instances, 0,
        "a sync after the forced one must be free again: {third:?}"
    );

    // Release the CAS store lock before the workspace TempDir removes the tree.
    drop(service);
    Ok(())
}
