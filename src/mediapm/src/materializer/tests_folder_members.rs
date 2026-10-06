//! What a second `sync_hierarchy` does with a media folder, member by member.
//!
//! A folder is one hierarchy path holding as many files as its variants
//! produce, so a folder is correct only when every one of those files is. The
//! tests here put the folder in three states a second run has to tell apart:
//! every member correct, one member drifted, and a member the document never
//! asked for sitting in the directory beside them.
//!
//! The drifted case is the one that separates this from a check that reads a
//! folder as correct whenever the directory exists. The edit that case stages
//! keeps the number of bytes, so a comparison that stopped at a length would
//! take it for the content the document resolves, and the folder would be
//! declined with a file in it that nothing asked for.

use mediapm_utils::progress::recording::{ProgressOp, RecordingProgressTracker};

use super::tests_common::{
    BLOCKED_VARIANT, FOLDER_PATH, FOLDER_VARIANT, folder_and_media_document, folder_only_document,
    folder_with_blocked_and_good_variants_document, open_hierarchy_cas, overall_finish,
    write_folder_over_archive, zip_payload,
};

use super::*;

/// Name of the first member the folder fixture's archive holds.
const FIRST_MEMBER: &str = "a.bin";

/// Name of the second member, and the one the drifted case disturbs.
///
/// The drift lands on the second member rather than the first on purpose. The
/// members arrive in the archive's sorted order, so checking only the first of
/// them finds a drift on the first member and misses a drift on any other. A
/// case that drifts the first member therefore cannot tell an arm that reads
/// every member from one that reads one.
const DRIFTED_MEMBER: &str = "b.bin";

/// Name of the member the drifted case leaves untouched, so a reader can see
/// which file the run acted on.
const UNTOUCHED_MEMBER: &str = FIRST_MEMBER;

/// Bytes the fixture's archive stores under [`FIRST_MEMBER`].
const FIRST_PAYLOAD: &[u8] = b"first member payload";

/// Bytes the fixture's archive stores under [`DRIFTED_MEMBER`].
const SECOND_PAYLOAD: &[u8] = b"second member payload";

/// Replacement for [`SECOND_PAYLOAD`]: the same number of bytes and different
/// ones, which is the edit a length comparison cannot see.
const SECOND_PAYLOAD_EDIT: &[u8] = b"SECOND MEMBER PAYLOAD";

/// A folder every member of which is already correct is declined.
///
/// The counters are read together for the reason the single-file tests give:
/// `skipped_paths == 1` alone is satisfiable by a run that counted the folder
/// and then wrote every member anyway, so the test also asserts that no
/// member's bytes changed and that the overall row stayed green.
#[tokio::test]
async fn a_folder_whose_every_member_is_correct_is_skipped() {
    let mut written = write_folder_over_archive(&[
        (FIRST_MEMBER, FIRST_PAYLOAD),
        (DRIFTED_MEMBER, SECOND_PAYLOAD),
    ])
    .await;

    let (recording, overall) = RecordingProgressTracker::with_overall("materializing", 1);
    let second =
        written.resync(Some(Arc::new(recording.clone())), Some(Arc::new(overall.clone()))).await;
    assert_eq!(
        (second.materialized_paths, second.skipped_paths, second.missing_paths),
        (0, 1, 0),
        "the run resolved the archive the first run wrote and every member is still there, so it \
         must decline the folder and write nothing: {second:?}"
    );
    assert_eq!(
        overall_finish(&recording, &overall),
        Some(ProgressOp::FinishSuccess),
        "a folder that was left alone is a library the run materialized, so the overall row must \
         not end as an error; got {ops:?}",
        ops = recording.recorded()
    );

    for (member, payload) in [(FIRST_MEMBER, FIRST_PAYLOAD), (DRIFTED_MEMBER, SECOND_PAYLOAD)] {
        assert_eq!(
            std::fs::read(written.output.join(member)).unwrap(),
            payload,
            "the member must still hold what the first run wrote"
        );
    }
}

/// A folder with one drifted member is written again, and the counters say
/// materialized.
///
/// This is the case the whole change turns on, twice over. The edit keeps the
/// number of bytes, so a check that compared lengths would take the member for
/// the content the document resolves. It lands on the second member, so a check
/// that read only the first would miss it as well. Together they leave an arm
/// that declines a folder without reading all of it unable to pass.
///
/// The untouched member is asserted afterwards for a third reason: a run that
/// repaired the drifted member on its own would leave that one correct too, so
/// the pair says the folder was decided as a whole.
///
/// What the counters carry is that the folder went through the write path, and
/// the write path writes every member. Pinning the untouched member being
/// rewritten would take a status-change-time witness like the one the `resync`
/// integration test uses, which is a per-file concern rather than a per-folder
/// one.
#[tokio::test]
async fn a_folder_with_one_drifted_member_is_written_again() {
    let mut written = write_folder_over_archive(&[
        (FIRST_MEMBER, FIRST_PAYLOAD),
        (DRIFTED_MEMBER, SECOND_PAYLOAD),
    ])
    .await;
    assert_eq!(
        SECOND_PAYLOAD_EDIT.len(),
        SECOND_PAYLOAD.len(),
        "the edit has to keep the number of bytes, or a length comparison would catch it and this \
         test would not say whether the folder arm reads its members"
    );

    let drifted = written.output.join(DRIFTED_MEMBER);
    commit::remove_path(&drifted).expect("a readonly managed output is removable");
    std::fs::write(&drifted, SECOND_PAYLOAD_EDIT).expect("the external write lands on the member");

    let second = written.resync(None, None).await;
    assert_eq!(
        (second.materialized_paths, second.skipped_paths, second.missing_paths),
        (1, 0, 0),
        "one member no longer holds what the document resolves, so the folder must be written \
         again and must not be reported as a skip: {second:?}"
    );
    assert_eq!(
        std::fs::read(&drifted).unwrap(),
        SECOND_PAYLOAD,
        "the drifted member must hold the bytes the document resolves, not the ones the edit left \
         behind"
    );
    assert_eq!(
        std::fs::read(written.output.join(UNTOUCHED_MEMBER)).unwrap(),
        FIRST_PAYLOAD,
        "the member that never drifted must still hold what the document resolves"
    );
}

/// A folder missing one member is written again.
///
/// Deleting a member leaves its record in `state.managed_files`, so the
/// records alone name a file the library no longer has.
#[tokio::test]
async fn a_folder_with_a_deleted_member_is_written_again() {
    let mut written = write_folder_over_archive(&[
        (FIRST_MEMBER, FIRST_PAYLOAD),
        (DRIFTED_MEMBER, SECOND_PAYLOAD),
    ])
    .await;

    let deleted = written.output.join(DRIFTED_MEMBER);
    commit::remove_path(&deleted).expect("a readonly managed output is removable");
    assert!(
        !deleted.exists(),
        "the deletion has to have happened, or the second run has nothing to restore"
    );

    let second = written.resync(None, None).await;
    assert_eq!(
        (second.materialized_paths, second.skipped_paths, second.missing_paths),
        (1, 0, 0),
        "the record still names the member but the file is gone, so the folder must be written: \
         {second:?}"
    );
    assert_eq!(
        std::fs::read(&deleted).unwrap(),
        SECOND_PAYLOAD,
        "the member must be back on disk holding the bytes its record names"
    );
}

/// A file the document never asked for does not stop the folder being
/// declined.
///
/// This is the opposite of what the folder's own shape suggests, and the
/// reason is the stale scan's, not this check's. Every path under a declared
/// folder path is protected from removal, so a file a user or another tool put
/// beside an album's members is never swept, and counting one as drift would
/// make that folder rewrite on every run for ever with nothing to show for it.
///
/// The expected member set is still built from the document rather than from a
/// directory listing, which is what keeps a member the document dropped out of
/// the set. What a listing would have added is exactly this case, and adding it
/// would have cost the folder its skip for good.
#[tokio::test]
async fn a_file_the_document_never_asked_for_does_not_stop_the_folder_being_skipped() {
    let mut written = write_folder_over_archive(&[
        (FIRST_MEMBER, FIRST_PAYLOAD),
        (DRIFTED_MEMBER, SECOND_PAYLOAD),
    ])
    .await;

    let stray = written.output.join("stray.bin");
    let stray_contents = b"nothing in the document produced this";
    std::fs::write(&stray, stray_contents).expect("the stray file lands");

    let second = written.resync(None, None).await;
    assert_eq!(
        (second.materialized_paths, second.skipped_paths, second.missing_paths),
        (0, 1, 0),
        "both members the document resolves are correct, so the folder is correct and the file \
         beside them is none of this run's business: {second:?}"
    );
    assert_eq!(
        std::fs::read(&stray).unwrap(),
        stray_contents,
        "the stale scan protects every path under a declared folder path, so the run must not \
         have removed a file it did not write"
    );
}

/// A folder whose variant path is occupied by a directory stays missing over
/// two runs.
///
/// The three earlier commits made this shape land in `missing_paths` and end
/// the overall row `finish_error`. The folder arm now reads the directory
/// before it writes anything, which is a second chance to read a folder with
/// nothing in it as already correct. It must not take it: the variant's path
/// is a directory where the document asked for a file, so the folder is short
/// of what it resolved to.
#[tokio::test]
async fn a_folder_whose_variant_path_is_a_directory_stays_missing() {
    let root = mediapm_utils::temp::artifact_dir().unwrap();
    let paths = MediaPmPaths::from_root(root.path());
    let cas = open_hierarchy_cas(&paths).await;
    let hash = cas.put(bytes::Bytes::from_static(b"blocked-folder-variant")).await.unwrap();
    let document = folder_only_document(&hash.to_string());

    let occupied = paths.hierarchy_root_dir.join(FOLDER_PATH).join(FOLDER_VARIANT);
    tokio::fs::create_dir_all(&occupied).await.unwrap();
    assert!(
        occupied.is_dir(),
        "the conflict has to be in place, or the folder arm finds a free name and writes"
    );

    for run in ["first", "second"] {
        let (recording, overall) = RecordingProgressTracker::with_overall("materializing", 1);
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
        .expect("one blocked variant leaves a report to read; it is not a failed run");
        assert_eq!(
            (report.missing_paths, report.materialized_paths, report.skipped_paths),
            (1, 0, 0),
            "the {run} run found the variant's path occupied by a directory, so the library is \
             short of what the document asked for and nothing was correct: {report:?}"
        );
        assert_eq!(
            overall_finish(&recording, &overall),
            Some(ProgressOp::FinishError),
            "the {run} run left the library short of the folder, so the overall row must end as \
             an error; got {ops:?}",
            ops = recording.recorded()
        );
    }
}

/// Name of the member a directory is placed on, so the folder arm refuses to
/// write it.
///
/// It sorts after [`FIRST_MEMBER`], which is what makes the test say the
/// refusal ended one member rather than the loop: the member before it has
/// already been written by the time the arm reaches the collision.
const OCCUPIED_MEMBER: &str = "cover.bin";

/// Bytes the fixture's archive stores under [`OCCUPIED_MEMBER`].
const OCCUPIED_PAYLOAD: &[u8] = b"cover art payload";

/// Name of a file inside the directory that occupies [`OCCUPIED_MEMBER`], so
/// the directory is not empty and clearing it would destroy something visible.
const RESIDENT_FILE: &str = "resident.bin";

/// Bytes [`RESIDENT_FILE`] holds, chosen so a reader can tell it from anything
/// the archive carries.
const RESIDENT_PAYLOAD: &[u8] = b"put there by something other than this run";

/// A folder ZIP member whose path is already a directory counts as missing, and
/// the other members still write.
///
/// The plain variant arm refuses a path a directory occupies, because the
/// shared writer clears whatever sits at the target and clearing one would
/// delete something the run does not own. The member path made no such
/// refusal: a directory where the archive named a member was removed and the
/// file written in its place, so the folder reported itself materialized on the
/// strength of having destroyed it. A member name is archive data rather than a
/// path the document declared, which leaves the arm no claim on what is already
/// there, and refusing is what the plain variant arm already does one level up.
///
/// The members the arm can write are still written, because one member it
/// cannot leaves the run as much to do as a blocked plain variant does.
/// [`folder_and_media_document`] holds a media entry beside the folder so the
/// counts can be read: a document holding only the folder cannot tell
/// `materialized_paths == 1` apart from a run that counted the folder wrongly.
#[tokio::test]
async fn a_folder_member_whose_path_is_a_directory_is_counted_as_missing() {
    let root = mediapm_utils::temp::artifact_dir().unwrap();
    let paths = MediaPmPaths::from_root(root.path());
    let cas = open_hierarchy_cas(&paths).await;
    let archive = cas
        .put(bytes::Bytes::from(zip_payload(&[
            (FIRST_MEMBER, FIRST_PAYLOAD),
            (OCCUPIED_MEMBER, OCCUPIED_PAYLOAD),
        ])))
        .await
        .unwrap();

    let occupied = paths.hierarchy_root_dir.join(FOLDER_PATH).join(OCCUPIED_MEMBER);
    tokio::fs::create_dir_all(&occupied).await.unwrap();
    std::fs::write(occupied.join(RESIDENT_FILE), RESIDENT_PAYLOAD).unwrap();

    let (recording, overall) = RecordingProgressTracker::with_overall("materializing", 2);
    let report = sync_hierarchy(
        &paths,
        &folder_and_media_document(&archive.to_string()),
        &mut MediaPmState::default(),
        &cas,
        false,
        &ConductorState::new_empty(),
        &NickelDocument::default(),
        Some(Arc::new(recording.clone())),
        Some(Arc::new(overall.clone())),
    )
    .await
    .expect("one refused member leaves a report to read; it is not a failed run");

    assert_eq!(
        (report.missing_paths, report.materialized_paths, report.skipped_paths),
        (1, 1, 0),
        "the folder is short the member it refused while the media entry beside it wrote, so the \
         three counters stay disjoint and the run keeps the ones that did land: {report:?}"
    );
    assert!(
        occupied.is_dir(),
        "the arm must leave the directory alone rather than clear it and write the member into \
         its place: {}",
        occupied.display()
    );
    assert_eq!(
        std::fs::read(occupied.join(RESIDENT_FILE)).unwrap(),
        RESIDENT_PAYLOAD,
        "the file inside the directory has to survive, which is the whole reason the member is \
         refused rather than written"
    );
    assert_eq!(
        std::fs::read(paths.hierarchy_root_dir.join(FOLDER_PATH).join(FIRST_MEMBER)).unwrap(),
        FIRST_PAYLOAD,
        "the member the archive names before the collision must still be written, or the refusal \
         ended the loop rather than one iteration of it"
    );
    assert_eq!(
        overall_finish(&recording, &overall),
        Some(ProgressOp::FinishError),
        "a folder left short a member has not materialized the library, so the overall row must \
         end as an error; got {ops:?}",
        ops = recording.recorded()
    );
}

/// Name of the member that claims the path [`NESTED_MEMBER`] needs as a
/// directory, so the two cannot both land.
const COLLIDING_FILE_MEMBER: &str = "cover";

/// Bytes [`COLLIDING_FILE_MEMBER`] holds when it lands as a file.
const COLLIDING_FILE_PAYLOAD: &[u8] = b"cover art file";

/// Name of the member inside [`COLLIDING_FILE_MEMBER`], which is what makes
/// the pair collide over one path.
const NESTED_MEMBER: &str = "cover/thumb.jpg";

/// Bytes [`NESTED_MEMBER`] holds.
const NESTED_PAYLOAD: &[u8] = b"cover art thumbnail";

/// The name [`COLLIDING_FILE_MEMBER`] is declared under in the archive where
/// [`NESTED_MEMBER`] has to land first.
///
/// Extraction sorts members by their declared path, and `cover` sorts ahead
/// of `cover/thumb.jpg` however the archive orders them. Declared
/// `downloads/cover` it sorts behind, and the sandbox prefix is stripped as
/// the member lands, so it still collides at `cover`.
const DOWNLOADS_PREFIXED_FILE_MEMBER: &str = "downloads/cover";

/// The file member lands first, the member inside it cannot, and the run
/// still hands back a report.
///
/// The archive asks for `cover` as a file and for `cover/thumb.jpg` inside
/// it, which the library cannot hold at once. Extraction sorts members by
/// their declared paths, so `cover` reaches the loop before
/// `cover/thumb.jpg` whichever order the archive stores them in, and by the
/// time the nested member asks for its directory the file is already on
/// disk. The refusal ends that one member: the members written before it
/// stay, the folder counts as missing, and the run returns its counters
/// instead of raising.
///
/// The media entry [`folder_and_media_document`] declares beside the folder
/// is what lets the counters be read, for the reason the test above gives.
#[tokio::test]
async fn a_folder_whose_file_member_lands_first_is_counted_as_missing() {
    let root = mediapm_utils::temp::artifact_dir().unwrap();
    let paths = MediaPmPaths::from_root(root.path());
    let cas = open_hierarchy_cas(&paths).await;
    let archive = cas
        .put(bytes::Bytes::from(zip_payload(&[
            (FIRST_MEMBER, FIRST_PAYLOAD),
            (COLLIDING_FILE_MEMBER, COLLIDING_FILE_PAYLOAD),
            (NESTED_MEMBER, NESTED_PAYLOAD),
        ])))
        .await
        .unwrap();

    let (recording, overall) = RecordingProgressTracker::with_overall("materializing", 2);
    let report = sync_hierarchy(
        &paths,
        &folder_and_media_document(&archive.to_string()),
        &mut MediaPmState::default(),
        &cas,
        false,
        &ConductorState::new_empty(),
        &NickelDocument::default(),
        Some(Arc::new(recording.clone())),
        Some(Arc::new(overall.clone())),
    )
    .await
    .expect("one member that cannot land leaves a report to read; it is not a failed run");

    assert_eq!(
        (report.missing_paths, report.materialized_paths, report.skipped_paths),
        (1, 1, 0),
        "the folder is short the member that could not have a directory while the media entry \
         beside it wrote, and nothing was correct enough to skip: {report:?}"
    );
    let folder = paths.hierarchy_root_dir.join(FOLDER_PATH);
    assert_eq!(
        std::fs::read(folder.join(FIRST_MEMBER)).unwrap(),
        FIRST_PAYLOAD,
        "the member written before the collision has to survive it, or the refusal ended the \
         loop rather than one iteration of it"
    );
    assert_eq!(
        std::fs::read(folder.join(COLLIDING_FILE_MEMBER)).unwrap(),
        COLLIDING_FILE_PAYLOAD,
        "the file member is what the nested member collided with, so the file has to be what is \
         still on disk"
    );
    assert!(
        !folder.join(NESTED_MEMBER).exists(),
        "the member whose directory a file occupies must be refused rather than written"
    );
    assert_eq!(
        overall_finish(&recording, &overall),
        Some(ProgressOp::FinishError),
        "a folder left short a member has not materialized the library, so the overall row must \
         end as an error; got {ops:?}",
        ops = recording.recorded()
    );
}

/// The nested member lands first, the file over it is refused, and the run
/// hands back the same report the other order gives.
///
/// `cover/thumb.jpg` writes before the file member reaches the loop, so a
/// directory is standing by the time the file member wants `cover` and the
/// refusal that has always made for an occupied directory takes it. The
/// archive declares the file member `downloads/cover` to get that order,
/// because sorting alone always puts a file ahead of the member inside it.
///
/// This is the order that already behaved. It keeps a test on both halves of
/// one collision, so the two refusals cannot drift apart again.
#[tokio::test]
async fn a_folder_whose_nested_member_lands_first_is_counted_as_missing() {
    let root = mediapm_utils::temp::artifact_dir().unwrap();
    let paths = MediaPmPaths::from_root(root.path());
    let cas = open_hierarchy_cas(&paths).await;
    let archive = cas
        .put(bytes::Bytes::from(zip_payload(&[
            (FIRST_MEMBER, FIRST_PAYLOAD),
            (NESTED_MEMBER, NESTED_PAYLOAD),
            (DOWNLOADS_PREFIXED_FILE_MEMBER, COLLIDING_FILE_PAYLOAD),
        ])))
        .await
        .unwrap();

    let (recording, overall) = RecordingProgressTracker::with_overall("materializing", 2);
    let report = sync_hierarchy(
        &paths,
        &folder_and_media_document(&archive.to_string()),
        &mut MediaPmState::default(),
        &cas,
        false,
        &ConductorState::new_empty(),
        &NickelDocument::default(),
        Some(Arc::new(recording.clone())),
        Some(Arc::new(overall.clone())),
    )
    .await
    .expect("one member that cannot land leaves a report to read; it is not a failed run");

    assert_eq!(
        (report.missing_paths, report.materialized_paths, report.skipped_paths),
        (1, 1, 0),
        "the folder is short the file member the directory displaced while the media entry \
         beside it wrote, and nothing was correct enough to skip: {report:?}"
    );
    let folder = paths.hierarchy_root_dir.join(FOLDER_PATH);
    assert_eq!(
        std::fs::read(folder.join(FIRST_MEMBER)).unwrap(),
        FIRST_PAYLOAD,
        "the member written before the collision has to survive it, or the refusal ended the \
         loop rather than one iteration of it"
    );
    assert_eq!(
        std::fs::read(folder.join(NESTED_MEMBER)).unwrap(),
        NESTED_PAYLOAD,
        "the nested member landed first, so its bytes are on disk"
    );
    assert!(
        folder.join(COLLIDING_FILE_MEMBER).is_dir(),
        "the file member must leave the directory alone rather than replace it: {}",
        folder.join(COLLIDING_FILE_MEMBER).display()
    );
    assert_eq!(
        overall_finish(&recording, &overall),
        Some(ProgressOp::FinishError),
        "a folder left short a member has not materialized the library, so the overall row must \
         end as an error; got {ops:?}",
        ops = recording.recorded()
    );
}

/// A folder with one blocked variant and one good variant stays missing over
/// two runs, though its good variant's file is correct.
///
/// Two variants, one of which cannot be written, is the shape the folder's
/// per-member check has to survive: the good variant's member verifies, so a
/// check that read only what verified would decline a folder still short its
/// blocked member.
#[tokio::test]
async fn a_folder_with_one_blocked_variant_stays_missing_though_its_good_one_is_correct() {
    let root = mediapm_utils::temp::artifact_dir().unwrap();
    let paths = MediaPmPaths::from_root(root.path());
    let cas = open_hierarchy_cas(&paths).await;
    let payload = b"blocked-and-good-folder-variant";
    let hash = cas.put(bytes::Bytes::from_static(payload)).await.unwrap();
    let document = folder_with_blocked_and_good_variants_document(&hash.to_string());

    let blocked = paths.hierarchy_root_dir.join(FOLDER_PATH).join(BLOCKED_VARIANT);
    tokio::fs::create_dir_all(&blocked).await.unwrap();

    let mut state = MediaPmState::default();
    for run in ["first", "second"] {
        let (recording, overall) = RecordingProgressTracker::with_overall("materializing", 1);
        let report = sync_hierarchy(
            &paths,
            &document,
            &mut state,
            &cas,
            false,
            &ConductorState::new_empty(),
            &NickelDocument::default(),
            Some(Arc::new(recording.clone())),
            Some(Arc::new(overall.clone())),
        )
        .await
        .expect("a blocked variant leaves a report to read; it is not a failed run");
        assert_eq!(
            (report.missing_paths, report.materialized_paths, report.skipped_paths),
            (1, 0, 0),
            "the {run} run wrote the good variant and could not write the blocked one, so the \
             folder is short a member and must be counted missing: {report:?}"
        );
        assert_eq!(
            overall_finish(&recording, &overall),
            Some(ProgressOp::FinishError),
            "the {run} run left the library short of the blocked member, so the overall row must \
             end as an error; got {ops:?}",
            ops = recording.recorded()
        );
        assert_eq!(
            std::fs::read(paths.hierarchy_root_dir.join(FOLDER_PATH).join(FOLDER_VARIANT)).unwrap(),
            payload,
            "the variant that can be written must be there whichever run is being read, so the \
             counts above are not being satisfied by a folder that wrote nothing at all"
        );
    }
}
