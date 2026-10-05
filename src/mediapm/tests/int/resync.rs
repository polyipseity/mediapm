//! # Re-sync over an unchanged library
//!
//! A second `sync_library` over a library nothing changed has to decline the
//! entry rather than rewrite it. `resolve_variant_hash` reads the document and
//! the conductor state, so the second run resolves the same hash the first one
//! did, and `state.managed_files` already records that hash against the output
//! path. Matching the two is what lets the run write nothing.
//!
//! The counters alone would not carry this file. `skipped_paths == 1` says
//! the entry was declined, not that the file was left alone: an implementation
//! could count the skip and then write anyway, and the count would read the
//! same. So every test here also watches the file itself.
//!
//! Modification time alone cannot carry it either, and that is the reason the
//! witness below reads two timestamps rather than one. The materializer's
//! first method is a hardlink, so a rewrite unlinks the target and links the
//! same CAS inode back. Bytes, inode, link count and modification time all
//! come out identical; only the status-change time records that the link was
//! replaced. The second timestamp is therefore the one that tells a rewrite
//! from a no-op, and `the_write_witness_moves_when_a_file_is_relinked_with_identical_bytes`
//! calibrates it on the filesystem the suite is running on rather than
//! assuming the platform can express the difference.

use std::collections::BTreeMap;
use std::path::Path;
use std::time::SystemTime;

use crate::common::{seed_cas, service_with_cache, sync_library_with_test_terminal};
use bytes::Bytes;
use mediapm::{
    HierarchyNode, HierarchyNodeKind, HierarchyPath, MediaPmDocument, MediaRuntimeStorage,
    MediaSourceSpec, PlaylistFormat, save_mediapm_document,
};

/// Media id of the single source, and the directory its id materializes as.
const MEDIA_ID: &str = "resync-media";

/// The variant the single CAS payload is published under.
const VARIANT: &str = "default";

/// Hierarchy path template that puts the media id itself in the materialized
/// path, so the entry's output is observable on disk after each run.
const HIERARCHY_PATH: &str = "${media.id}/track.mp4";

/// Payload stored in CAS for [`VARIANT`].
const PAYLOAD: &[u8] = b"resync payload";

/// Second payload, used where a test has to make the document resolve content
/// the library does not hold.
const OTHER_PAYLOAD: &[u8] = b"resync payload, revised";

/// Builds a document with one media source bound to `media_id` at
/// [`HIERARCHY_PATH`], its variant pointing at `variant_hash`.
///
/// The source carries no steps, so both syncs resolve the variant from the
/// hash the document declares and no workflow has to run to produce one.
fn document_with_one_media_entry(variant_hash: &str) -> MediaPmDocument {
    MediaPmDocument {
        media: BTreeMap::from([(
            MEDIA_ID.to_string(),
            MediaSourceSpec {
                variant_hashes: BTreeMap::from([(VARIANT.to_string(), variant_hash.to_string())]),
                steps: Vec::new(),
                ..MediaSourceSpec::default()
            },
        )]),
        hierarchy: vec![HierarchyNode {
            path: HierarchyPath::from(HIERARCHY_PATH),
            kind: HierarchyNodeKind::Media,
            id: None,
            media_id: Some(MEDIA_ID.to_string()),
            variant: Some(VARIANT.to_string()),
            variants: Vec::new(),
            rename_files: Vec::new(),
            format: PlaylistFormat::M3u8,
            ids: Vec::new(),
            sanitize_names: None,
            children: Vec::new(),
        }],
        ..MediaPmDocument::default()
    }
}

/// A second `sync_library` over an unchanged library counts one normal skip,
/// writes nothing, and leaves the file's timestamps alone.
///
/// The three run-1 assertions come first because they are what makes the run-2
/// ones mean anything. A workspace that never wrote has nothing to decline, and
/// a run-2 `skipped_paths == 1` over it would be counting an entry the first
/// run never materialized.
///
/// The witness is read either side of the second sync. Without it the test
/// passes against an implementation that counts a skip and then writes the file
/// anyway, which is the failure this file exists to rule out.
#[tokio::test]
async fn resync_over_an_unchanged_library_skips_the_entry_and_writes_nothing()
-> Result<(), mediapm::MediaPmError> {
    let (mut service, root, _cache) = service_with_cache(MediaRuntimeStorage::default()).await?;

    let hash = seed_cas(&service, Bytes::from_static(PAYLOAD), "resync variant").await?;
    ensure_blob_is_materialized(&service, &hash).await?;

    save_mediapm_document(
        &service.paths().mediapm_ncl,
        &document_with_one_media_entry(&hash.to_string()),
    )?;

    let first = sync_library_with_test_terminal(&mut service, false).await?;
    assert_eq!(
        (first.materialized_paths, first.skipped_paths, first.missing_paths),
        (1, 0, 0),
        "the first run has nothing recorded for the entry, so it must write it: {first:?}"
    );
    let materialized = root.path().join(MEDIA_ID).join("track.mp4");
    assert!(
        materialized.is_file(),
        "the first run's materialized path must exist on disk before the re-sync is read: {}",
        materialized.display()
    );

    let before = read_witness(&materialized)?;

    let second = sync_library_with_test_terminal(&mut service, false).await?;
    assert_eq!(
        (second.skipped_paths, second.materialized_paths, second.missing_paths),
        (1, 0, 0),
        "the re-sync resolved the hash the first run wrote and the file is still there, so it \
         must decline the entry and write nothing: {second:?}"
    );

    let after = read_witness(&materialized)?;
    assert_eq!(
        after.modified, before.modified,
        "the re-sync must not rewrite the entry, or the file's modification time moves"
    );
    assert_eq!(
        after.changed, before.changed,
        "the re-sync must not replace the entry, or the file's status-change time moves; a \
         hardlink rewrite leaves the modification time alone, which is why this is read \
         separately"
    );

    // Release the CAS `store/lock` before the workspace `TempDir` removes the
    // tree it guards.
    drop(service);
    Ok(())
}

/// A document that resolves different content rewrites the entry, however
/// correct the recorded hash still reads.
#[tokio::test]
async fn resync_after_a_changed_hash_rewrites_the_entry() -> Result<(), mediapm::MediaPmError> {
    let (mut service, root, _cache) = service_with_cache(MediaRuntimeStorage::default()).await?;

    let first_hash =
        seed_cas(&service, Bytes::from_static(PAYLOAD), "resync variant, first").await?;
    let second_hash =
        seed_cas(&service, Bytes::from_static(OTHER_PAYLOAD), "resync variant, second").await?;
    ensure_blob_is_materialized(&service, &first_hash).await?;
    ensure_blob_is_materialized(&service, &second_hash).await?;

    save_mediapm_document(
        &service.paths().mediapm_ncl,
        &document_with_one_media_entry(&first_hash.to_string()),
    )?;
    let first = sync_library_with_test_terminal(&mut service, false).await?;
    assert_eq!(first.materialized_paths, 1, "the first run must write the entry: {first:?}");

    save_mediapm_document(
        &service.paths().mediapm_ncl,
        &document_with_one_media_entry(&second_hash.to_string()),
    )?;
    let second = sync_library_with_test_terminal(&mut service, false).await?;
    assert_eq!(
        (second.materialized_paths, second.skipped_paths),
        (1, 0),
        "the document now resolves different content, so the recorded hash no longer describes \
         the target and the entry must be written: {second:?}"
    );
    assert_eq!(
        std::fs::read(root.path().join(MEDIA_ID).join("track.mp4")).map_err(|source| {
            io_error("reading the re-materialized entry", &root.path().join(MEDIA_ID), source)
        })?,
        OTHER_PAYLOAD,
        "the entry must hold the bytes the revised document names"
    );

    drop(service);
    Ok(())
}

/// A recorded hash whose file was deleted is rewritten rather than skipped.
///
/// Deleting an output leaves its record in `state.managed_files`, so the record
/// alone cannot tell a re-sync which paths still exist.
#[tokio::test]
async fn resync_after_the_file_was_deleted_rewrites_the_entry() -> Result<(), mediapm::MediaPmError>
{
    let (mut service, root, _cache) = service_with_cache(MediaRuntimeStorage::default()).await?;

    let hash = seed_cas(&service, Bytes::from_static(PAYLOAD), "resync variant").await?;
    ensure_blob_is_materialized(&service, &hash).await?;

    save_mediapm_document(
        &service.paths().mediapm_ncl,
        &document_with_one_media_entry(&hash.to_string()),
    )?;
    let first = sync_library_with_test_terminal(&mut service, false).await?;
    assert_eq!(first.materialized_paths, 1, "the first run must write the entry: {first:?}");

    let materialized = root.path().join(MEDIA_ID).join("track.mp4");
    std::fs::remove_file(&materialized)
        .map_err(|source| io_error("deleting the materialized entry", &materialized, source))?;
    assert!(
        !materialized.exists(),
        "the deletion has to have happened, or the second run has nothing to restore"
    );

    let second = sync_library_with_test_terminal(&mut service, false).await?;
    assert_eq!(
        (second.materialized_paths, second.skipped_paths),
        (1, 0),
        "the record still names this hash but the file is gone, so the entry must be written: \
         {second:?}"
    );
    assert_eq!(
        std::fs::read(&materialized).map_err(|source| io_error(
            "reading the restored entry",
            &materialized,
            source
        ))?,
        PAYLOAD,
        "the entry must be back on disk holding the bytes its record names"
    );

    drop(service);
    Ok(())
}

/// The witness has to move when a file is rewritten with identical bytes, or
/// asserting it stayed still proves nothing.
///
/// This is the calibration for every assertion above that compares two
/// witnesses. It performs the operation the materializer performs on a rewrite,
/// unlink the target and link the same inode back, and requires the witness to
/// notice. A platform whose filesystem cannot express that difference would
/// otherwise let a rewriting implementation pass a test written to rule it out.
#[test]
fn the_write_witness_moves_when_a_file_is_relinked_with_identical_bytes() {
    let root = mediapm_utils::temp::artifact_dir().expect("artifact dir");
    let source = root.path().join("source.bin");
    let target = root.path().join("target.bin");
    std::fs::write(&source, PAYLOAD).expect("the source is written");

    link_over(&source, &target);
    let before = write_witness(&target).expect("the first link is witnessed");
    link_over(&source, &target);
    let after = write_witness(&target).expect("the second link is witnessed");

    assert_eq!(
        std::fs::read(&target).expect("the target is readable"),
        PAYLOAD,
        "both links carry identical bytes, so content cannot be what distinguishes them"
    );
    assert_ne!(
        after.changed, before.changed,
        "relinking identical bytes must move the status-change time, or the witness cannot tell \
         a rewrite from a no-op on this filesystem"
    );
}

/// Reads the [`WriteWitness`] for `path`, naming the path when the read fails.
fn read_witness(path: &Path) -> Result<WriteWitness, mediapm::MediaPmError> {
    write_witness(path).map_err(|source| io_error("reading the entry's timestamps", path, source))
}

/// Replaces whatever is at `target` with a hardlink to `source`, which is the
/// shape of a rewrite the materializer performs.
fn link_over(source: &Path, target: &Path) {
    if target.exists() {
        std::fs::remove_file(target).expect("the old link is removed");
    }
    std::fs::hard_link(source, target).expect("the replacement link is created");
}

/// Forces the CAS to hold `hash` as a file, which the materializer's first
/// materialization method needs before it can link the blob into the library.
async fn ensure_blob_is_materialized(
    service: &mediapm::MediaPmService<mediapm_cas::FileSystemCas>,
    hash: &mediapm_cas::Hash,
) -> Result<(), mediapm::MediaPmError> {
    service.conductor().cas().ensure_blob_materialized(*hash).await.map_err(|source| {
        mediapm::MediaPmError::Workflow(format!("ensure blob materialized: {source}"))
    })
}

/// The platform's record that a file was created, replaced or relinked, in
/// whatever units the platform reports it.
#[cfg(unix)]
type ChangeStamp = (i64, i64);
#[cfg(windows)]
type ChangeStamp = (u128, u32);

/// Both timestamps of one file, which between them tell a rewrite from a
/// no-op.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
struct WriteWitness {
    /// Last-write time.
    modified: SystemTime,
    /// Status-change time, which a hardlink rewrite moves even though the
    /// modification time and the bytes do not.
    changed: ChangeStamp,
}

/// Names the operation and path when a filesystem call in this file fails.
fn io_error(operation: &str, path: &Path, source: std::io::Error) -> mediapm::MediaPmError {
    mediapm::MediaPmError::Io { operation: operation.to_string(), path: path.to_path_buf(), source }
}

/// Reads [`WriteWitness`] for `path`, following symlinks as the materializer's
/// own existence check does.
#[cfg(unix)]
fn write_witness(path: &Path) -> std::io::Result<WriteWitness> {
    use std::os::unix::fs::MetadataExt as _;

    let metadata = std::fs::metadata(path)?;
    Ok(WriteWitness {
        modified: metadata.modified()?,
        changed: (metadata.ctime(), metadata.ctime_nsec()),
    })
}

/// Reads [`WriteWitness`] for `path`, following symlinks as the materializer's
/// own existence check does.
#[cfg(windows)]
fn write_witness(path: &Path) -> std::io::Result<WriteWitness> {
    let metadata = std::fs::metadata(path)?;
    let created = metadata.created()?;
    let since_epoch = created.duration_since(SystemTime::UNIX_EPOCH).map_err(|_| {
        std::io::Error::other("a file this test wrote cannot predate the unix epoch")
    })?;
    Ok(WriteWitness {
        modified: metadata.modified()?,
        changed: (since_epoch.as_secs(), since_epoch.subsec_nanos()),
    })
}
