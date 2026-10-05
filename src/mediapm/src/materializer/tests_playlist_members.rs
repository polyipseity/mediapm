//! What a second `sync_hierarchy` does with a playlist.
//!
//! A playlist renders to exactly one file, so its expected set has one member
//! and the "every member verifies" rule a folder follows has nowhere to hide.
//! That is what makes the cases below the same three a folder is put through,
//! one entry narrower: the body a first run rendered is left alone, an edit to
//! it is repaired, and a body the library no longer has is written again.
//!
//! The document is the source of the expected bytes, as it is for a folder's
//! members, so the file on disk is never read to decide what the entry should
//! hold. That is what stops an edited body from confirming itself, and the
//! drifted case stages an edit that keeps the number of bytes so a comparison
//! stopping at a length would take it for the content the document renders.
//!
//! One counter cannot say which entry a run acted on, because the document
//! holds the media entry the playlist references as well. Both entries count
//! towards every number here, and the tests say which of the two each count is
//! about.

use mediapm_utils::progress::recording::{ProgressOp, RecordingProgressTracker};

use super::tests_common::{
    PLAYLIST_PATH, clear_readonly, finish_of_bar_opened_as, open_hierarchy_cas, overall_finish,
    playlist_document, write_playlist,
};

use super::*;

/// Edits the first byte of `body` to a different value of the same width, so
/// the result has the length `body` had and none of its content.
///
/// A playlist body is rendered from the document rather than from the store,
/// so it has no recorded hash to compare and the bytes are the only statement
/// the arm can check. An edit that kept the number of bytes and changed one of
/// them is the case that check exists for.
fn same_length_edit(body: &[u8]) -> Vec<u8> {
    let mut edited = body.to_vec();
    let first = edited.first_mut().expect("a rendered playlist body is never empty");
    *first = if *first == b'X' { b'Y' } else { b'X' };
    assert_eq!(
        edited.len(),
        body.len(),
        "the edit has to keep the number of bytes, or the test would say nothing about a check \
         that reaches the bytes"
    );
    assert_ne!(
        edited, body,
        "the edit has to change the content, or there is nothing for the second run to find"
    );
    edited
}

/// A playlist whose rendered body is already on disk is declined.
///
/// The counters are read together for the reason the folder and single-file
/// tests give: two of them can be satisfied by a run that counted the entry and
/// then wrote it anyway, so the body is read back and the overall row is read
/// as well.
#[tokio::test]
async fn a_playlist_whose_body_is_unchanged_is_skipped() {
    let mut written = write_playlist().await;
    let body = std::fs::read(&written.output).unwrap();

    let (recording, overall) = RecordingProgressTracker::with_overall("materializing", 2);
    let second =
        written.resync(Some(Arc::new(recording.clone())), Some(Arc::new(overall.clone()))).await;
    assert_eq!(
        (second.materialized_paths, second.skipped_paths, second.missing_paths),
        (0, 2, 0),
        "the re-sync rendered the same body the first run wrote and it is still on disk, so the \
         playlist and the media entry it references must both be declined: {second:?}"
    );
    assert_eq!(
        overall_finish(&recording, &overall),
        Some(ProgressOp::FinishSuccess),
        "two entries were left alone, so the overall row must not end as an error; got {ops:?}",
        ops = recording.recorded()
    );
    assert_eq!(
        std::fs::read(&written.output).unwrap(),
        body,
        "the playlist body must still hold what the first run rendered"
    );
}

/// A playlist whose body was edited in place is written again.
///
/// The edit is written through the file rather than over it, which is what an
/// editor or a tagger does, and the read-only bit the materializer sets is
/// cleared first so the write can land. The file therefore stays read-only
/// afterwards, and the arm has to clear the way before it can write: a plain
/// write would fail on a file it marked read-only itself, which is a second
/// reason this case needs the shared writer rather than a bare one.
#[tokio::test]
async fn a_playlist_whose_body_drifted_is_written_again() {
    let mut written = write_playlist().await;
    let body = std::fs::read(&written.output).unwrap();
    let drifted = same_length_edit(&body);

    clear_readonly(&written.output);
    std::fs::write(&written.output, &drifted)
        .expect("an outside writer's edit lands on the playlist body");
    assert_eq!(
        std::fs::read(&written.output).unwrap(),
        drifted,
        "the edit has to have landed, or the second run has nothing to find"
    );

    let second = written.resync(None, None).await;
    assert_eq!(
        (second.materialized_paths, second.skipped_paths, second.missing_paths),
        (1, 1, 0),
        "the playlist body no longer holds what the document renders, so the playlist must be \
         written while the media entry it references is declined: {second:?}"
    );
    assert_eq!(
        std::fs::read(&written.output).unwrap(),
        body,
        "the playlist body must hold the bytes this run rendered, not the ones the edit left"
    );
}

/// A playlist whose body was deleted is written again.
///
/// A playlist records nothing in `state.managed_files`, so nothing survives the
/// deletion to say the entry was ever written, and the file's absence is the
/// only thing that settles it.
#[tokio::test]
async fn a_playlist_whose_body_was_deleted_is_written_again() {
    let mut written = write_playlist().await;
    let body = std::fs::read(&written.output).unwrap();

    commit::remove_path(&written.output).expect("a readonly managed output is removable");
    assert!(
        !written.output.exists(),
        "the deletion has to have happened, or the second run has nothing to restore"
    );

    let second = written.resync(None, None).await;
    assert_eq!(
        (second.materialized_paths, second.skipped_paths, second.missing_paths),
        (1, 1, 0),
        "the playlist body is gone and nothing records that it was ever written, so the entry \
         must be written while the media entry it references is declined: {second:?}"
    );
    assert_eq!(
        std::fs::read(&written.output).unwrap(),
        body,
        "the playlist body must be back on disk holding the bytes this run rendered"
    );
}

/// A file beside a playlist that the document never asked for does not stop the
/// playlist being declined.
///
/// The expected body is rendered from the document and the document declares
/// one path, so the expected set has one member and a second file changes
/// nothing. That is the same conclusion a folder reaches, and it is reached
/// for a different reason: a folder protects every path under its own declared
/// directory, while a playlist's parent is a plain `Folder` node that flattens
/// to no entry at all, so nothing under it is protected from the stale scan.
///
/// The stray file is therefore removed rather than left alone, which is how the
/// stale scan treats a playlist's whole directory today and is not this step's
/// business to change. Asserting it pins that, so a later change to the scan
/// has to say so here rather than arriving as a surprise.
#[tokio::test]
async fn a_file_the_document_never_asked_for_does_not_stop_the_playlist_being_skipped() {
    let mut written = write_playlist().await;
    let body = std::fs::read(&written.output).unwrap();

    let stray =
        written.output.parent().expect("a playlist body lives in a directory").join("stray.m3u8");
    let stray_contents = b"nothing in the document produced this";
    std::fs::write(&stray, stray_contents).expect("the stray file lands");

    let second = written.resync(None, None).await;
    assert_eq!(
        (second.materialized_paths, second.skipped_paths, second.missing_paths),
        (0, 2, 0),
        "both entries the document declares are correct, so the run writes nothing: {second:?}"
    );
    assert_eq!(
        second.removed_paths, 1,
        "the playlist's parent directory holds no declared path of its own, so the stray file is \
         not under anything the run must protect and the stale scan takes it: {second:?}"
    );
    assert_eq!(
        std::fs::read(&written.output).unwrap(),
        body,
        "the playlist body must still hold what the first run rendered"
    );
    assert!(
        !stray.exists(),
        "the stray file is removed by the stale scan today, and nothing in this step changes that"
    );
}

/// A playlist whose path is occupied by a directory counts as missing, and the
/// run still reports what it wrote.
///
/// The shared writer clears whatever is in the way so a body this run marked
/// read-only can be replaced, and this is the case that must not go through it:
/// the path is the entry's own, so clearing it would delete a directory
/// something else put there.
///
/// The arm refused the write and raised. A playlist renders to one file, so
/// there is no variant loop to carry on past the refusal in and nothing later
/// in the entry to reach, which is the whole difference from the folder arm
/// that answers the same collision with a missing count. Raising ended the
/// worker, the join loop broke on the `Err`, and the report the counters live
/// in went back with it, so a caller saw a run that failed and learned nothing
/// about the media entry beside the playlist, which did write.
///
/// The three counts are read together, and the media entry is read off disk,
/// because any one of them alone is satisfiable by a run that stopped early.
#[tokio::test]
async fn a_playlist_whose_path_is_a_directory_is_counted_as_missing() {
    let root = mediapm_utils::temp::artifact_dir().unwrap();
    let paths = MediaPmPaths::from_root(root.path());
    let cas = open_hierarchy_cas(&paths).await;
    let hash = cas.put(bytes::Bytes::from_static(b"blocked-playlist-media")).await.unwrap();
    let document = playlist_document(&hash.to_string());

    let occupied = paths.hierarchy_root_dir.join(PLAYLIST_PATH);
    tokio::fs::create_dir_all(&occupied).await.unwrap();
    assert!(
        occupied.is_dir(),
        "the conflict has to be in place, or the playlist arm finds a free name and writes"
    );

    let (recording, overall) = RecordingProgressTracker::with_overall("materializing", 2);
    let report = sync_hierarchy(
        &paths,
        &document,
        &mut MediaPmState::default(),
        &cas,
        false,
        &ConductorState::new_empty(),
        &NickelDocument::default(),
        Some(Arc::new(recording.clone())),
        Some(Arc::new(overall.clone())),
    )
    .await
    .expect("a playlist that cannot be written leaves a report to read; it is not a failed run");

    assert_eq!(
        (report.materialized_paths, report.skipped_paths, report.missing_paths),
        (1, 0, 1),
        "the document declares two entries: the media entry wrote and the playlist did not, so \
         the counts have to say one of each rather than failing the run and reporting neither: \
         {report:?}"
    );
    assert!(
        occupied.is_dir(),
        "the directory has to survive the run, or the shared writer cleared a path the run does \
         not own"
    );
    assert!(
        paths.hierarchy_root_dir.join("song").is_file(),
        "the media entry the playlist references has to be on disk, or the materialized count \
         above is what a run that wrote nothing reports"
    );
    assert_eq!(
        finish_of_bar_opened_as(&recording, &format!("{PLAYLIST_PATH} [stg]")),
        Some(ProgressOp::FinishWarning),
        "an entry left unwritten is the same warning a folder with a blocked variant carries, so \
         the playlist row must warn rather than error; got {ops:?}",
        ops = recording.recorded()
    );
    assert_eq!(
        overall_finish(&recording, &overall),
        Some(ProgressOp::FinishError),
        "the run left the library short of the playlist, so the overall row must end as an \
         error; got {ops:?}",
        ops = recording.recorded()
    );
}
