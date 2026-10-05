//! # Re-sync over an unchanged library
//!
//! A second `sync_library` over a library nothing changed has to decline the
//! entry rather than rewrite it. `resolve_variant_hash` reads the document and
//! the conductor state, so the second run resolves the same hash the first one
//! did, and `state.managed_files` already records that hash against the output
//! path. Matching the two is what lets the run write nothing.
//!
//! The recorded hash on its own is not enough, because it says what an earlier
//! run put there and not what the file holds now. The decline therefore also
//! establishes what the file holds, by whichever means the method that produced
//! it allows: an identity or a name for a hardlink or a symlink, and the bytes
//! themselves for a reflink or a copy. That is what catches a write into the
//! library from outside mediapm. The method-dependent cases are pinned in the
//! materializer's own tests, where each relationship is set up directly,
//! rather than here, where every run writes through hardlink.
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

/// Hierarchy path template for the same source as a media folder, whose output
/// lands under the media id rather than at it.
const FOLDER_HIERARCHY_PATH: &str = "${media.id}";

/// Hierarchy path of the playlist [`document_with_one_playlist`] declares.
const PLAYLIST_PATH: &str = "playlists/track.m3u8";

/// Hierarchy id of the media entry that playlist references. A playlist
/// resolves its references through the media index, which holds media entries
/// and nothing else, so the entry it names has to carry an id.
const PLAYLISTED_HIERARCHY_ID: &str = "playlist-item";

/// Payload stored in CAS for [`VARIANT`].
const PAYLOAD: &[u8] = b"resync payload";

/// Second payload, used where a test has to make the document resolve content
/// the library does not hold.
const OTHER_PAYLOAD: &[u8] = b"resync payload, revised";

/// Bytes written over a materialized entry to stand in for something outside
/// mediapm writing into the library. Deliberately a different length from
/// [`PAYLOAD`], which is the first thing the re-sync's content check looks at.
const EXTERNAL_WRITE: &[u8] = b"an outside writer left these bytes here instead";

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

/// Builds a document with one media source bound to `media_id`, whose single
/// hierarchy entry is a media folder at [`FOLDER_HIERARCHY_PATH`].
///
/// The folder arm writes one file per variant under its own path, so the
/// materialized output is `<root>/<MEDIA_ID>/<VARIANT>` rather than the single
/// file the media entry produces. Everything else matches the media fixture, so
/// the two arms differ in the shape of what they leave on disk and in nothing
/// about how the runs reach them.
fn document_with_one_media_folder(variant_hash: &str) -> MediaPmDocument {
    let mut document = document_with_one_media_entry(variant_hash);
    document.hierarchy[0].path = HierarchyPath::from(FOLDER_HIERARCHY_PATH);
    document.hierarchy[0].kind = HierarchyNodeKind::MediaFolder;
    document
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

/// A second `sync_library` over an unchanged media folder declines it and
/// leaves its member alone.
///
/// The folder arm is a separate one from the media arm, and a counter of one
/// against zero is all either of them can report, so this reads the member's
/// timestamps as well. The materializer marks every member read-only and a
/// rewrite unlinks and rewrites it, so a folder that was rewritten leaves
/// either a different modification time or a different status-change time.
///
/// One member is all this can cover. Which members a folder resolves to comes
/// from its document, and a member that drifts is the case the materializer's
/// own tests cover, where each member can be set up on its own.
#[tokio::test]
async fn resync_over_an_unchanged_media_folder_skips_the_entry_and_writes_nothing()
-> Result<(), mediapm::MediaPmError> {
    let (mut service, root, _cache) = service_with_cache(MediaRuntimeStorage::default()).await?;

    let hash = seed_cas(&service, Bytes::from_static(PAYLOAD), "resync folder variant").await?;
    ensure_blob_is_materialized(&service, &hash).await?;

    save_mediapm_document(
        &service.paths().mediapm_ncl,
        &document_with_one_media_folder(&hash.to_string()),
    )?;

    let first = sync_library_with_test_terminal(&mut service, false).await?;
    assert_eq!(
        (first.materialized_paths, first.skipped_paths, first.missing_paths),
        (1, 0, 0),
        "the first run has nothing recorded for the folder, so it must write it: {first:?}"
    );
    let member = root.path().join(MEDIA_ID).join(VARIANT);
    assert!(
        member.is_file(),
        "the first run's folder member must exist on disk before the re-sync is read: {}",
        member.display()
    );

    let before = read_witness(&member)?;

    let second = sync_library_with_test_terminal(&mut service, false).await?;
    assert_eq!(
        (second.materialized_paths, second.skipped_paths, second.missing_paths),
        (0, 1, 0),
        "the re-sync resolved the hash the first run wrote and the member is still there, so it \
         must decline the folder and write nothing: {second:?}"
    );

    let after = read_witness(&member)?;
    assert_eq!(
        after.modified, before.modified,
        "the re-sync must not rewrite the folder's member, or its modification time moves"
    );
    assert_eq!(
        after.changed, before.changed,
        "the re-sync must not replace the folder's member, or its status-change time moves"
    );
    assert_eq!(
        std::fs::read(&member).map_err(|source| io_error(
            "reading the folder member",
            &member,
            source
        ))?,
        PAYLOAD,
        "the member must still hold what the first run wrote"
    );

    drop(service);
    Ok(())
}

/// Builds a document with the media entry [`document_with_one_media_entry`]
/// declares and a playlist naming it, so a run reaches the media arm and the
/// playlist arm.
///
/// The playlist's entry carries an explicit `id`, which is the only thing that
/// makes it addressable: a playlist looks a reference up by hierarchy id and
/// the media index is built from ids, so an entry without one could not be
/// named at all. Both entries count towards every counter a caller reads, so
/// the counters say two where a playlist-only document would say one.
fn document_with_one_playlist(variant_hash: &str) -> MediaPmDocument {
    let mut document = document_with_one_media_entry(variant_hash);
    document.hierarchy[0].id = Some(PLAYLISTED_HIERARCHY_ID.to_string());
    document.hierarchy.push(HierarchyNode {
        path: HierarchyPath::from(PLAYLIST_PATH),
        kind: HierarchyNodeKind::Playlist,
        id: None,
        media_id: None,
        variant: None,
        variants: Vec::new(),
        rename_files: Vec::new(),
        format: PlaylistFormat::M3u8,
        ids: vec![mediapm::PlaylistItemRef::Shorthand(PLAYLISTED_HIERARCHY_ID.to_string())],
        sanitize_names: None,
        children: Vec::new(),
    });
    document
}

/// A second `sync_library` over an unchanged playlist declines it and leaves
/// its body alone.
///
/// The playlist arm is a separate one from the media and folder arms, and a
/// counter cannot say which of the document's two entries a run acted on, so
/// this reads the body's timestamps as well. A playlist records nothing in
/// `state.managed_files`, so nothing survives into a second run to say the body
/// was ever written: the bytes it renders are the only statement the arm has,
/// and a rewrite leaves either a different modification time or a different
/// status-change time.
///
/// What the body resolves is a question about the document, and an edit to it
/// is the case the materializer's own tests cover, where the body can be
/// staged on its own.
#[tokio::test]
async fn resync_over_an_unchanged_playlist_skips_the_entry_and_writes_nothing()
-> Result<(), mediapm::MediaPmError> {
    let (mut service, root, _cache) = service_with_cache(MediaRuntimeStorage::default()).await?;

    let hash = seed_cas(&service, Bytes::from_static(PAYLOAD), "resync playlist variant").await?;
    ensure_blob_is_materialized(&service, &hash).await?;

    save_mediapm_document(
        &service.paths().mediapm_ncl,
        &document_with_one_playlist(&hash.to_string()),
    )?;

    let first = sync_library_with_test_terminal(&mut service, false).await?;
    assert_eq!(
        (first.materialized_paths, first.skipped_paths, first.missing_paths),
        (2, 0, 0),
        "the first run has nothing recorded for either entry, so it must write both: {first:?}"
    );
    let body = root.path().join(PLAYLIST_PATH);
    assert!(
        body.is_file(),
        "the first run's playlist body must exist on disk before the re-sync is read: {}",
        body.display()
    );

    let before = read_witness(&body)?;
    let before_bytes = std::fs::read(&body)
        .map_err(|source| io_error("reading the playlist body", &body, source))?;

    let second = sync_library_with_test_terminal(&mut service, false).await?;
    assert_eq!(
        (second.materialized_paths, second.skipped_paths, second.missing_paths),
        (0, 2, 0),
        "the re-sync rendered the same body the first run wrote and it is still on disk, so the \
         playlist and the media entry it references must both be declined: {second:?}"
    );

    let after = read_witness(&body)?;
    assert_eq!(
        after.modified, before.modified,
        "the re-sync must not rewrite the playlist body, or its modification time moves"
    );
    assert_eq!(
        after.changed, before.changed,
        "the re-sync must not replace the playlist body, or its status-change time moves"
    );
    assert_eq!(
        std::fs::read(&body).map_err(|source| io_error(
            "reading the playlist body",
            &body,
            source
        ))?,
        before_bytes,
        "the body must still hold what the first run rendered"
    );

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

/// An output something outside mediapm rewrote with a different number of
/// bytes is materialized again, because the record cannot see the write.
///
/// The record cannot see the write. Nothing an outside writer does reaches
/// `state.managed_files`, so it still names the hash the first run resolved
/// while the file holds something else, and a run that trusted the record
/// would report a clean library over bytes it never looked at. Reading the
/// file is what notices.
///
/// The external write replaces the file rather than writing through it. The
/// library file is a hardlink of the CAS object, so writing through it would
/// edit the content the re-sync restores from and leave nothing to assert.
#[tokio::test]
async fn resync_after_an_external_write_rewrites_the_entry() -> Result<(), mediapm::MediaPmError> {
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
        .map_err(|source| io_error("removing the materialized entry", &materialized, source))?;
    std::fs::write(&materialized, EXTERNAL_WRITE)
        .map_err(|source| io_error("writing over the materialized entry", &materialized, source))?;

    let second = sync_library_with_test_terminal(&mut service, false).await?;
    assert_eq!(
        (second.materialized_paths, second.skipped_paths),
        (1, 0),
        "the record still names the resolved hash but the file no longer has that length, so the \
         entry must be written rather than declined: {second:?}"
    );
    assert_eq!(
        std::fs::read(&materialized).map_err(|source| io_error(
            "reading the rewritten entry",
            &materialized,
            source
        ))?,
        PAYLOAD,
        "the entry must hold the bytes the document resolves"
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
