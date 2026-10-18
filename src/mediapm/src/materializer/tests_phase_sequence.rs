//! The rows each materialization arm advances and finishes, observed through a real `sync_hierarchy`.

use crate::config::hierarchy_types::HierarchyNodeKind;
use mediapm_utils::progress::recording::{
    BarId, ProgressOp, RecordedProgressOp, RecordingProgressTracker,
};

use super::tests_common::{
    folder_and_playlist_document, open_hierarchy_cas, playlist_with_unknown_reference_document,
    zip_payload,
};

use super::*;

/// A folder row and a playlist row each advance once and finish on success.
///
/// Both rows used to be checked by the phase tag they carried, which the
/// label no longer renders. What the tag walk was standing in for is still
/// real and is what this pins: each arm advances its row once when its
/// work returns and terminates the row on success. This drives a real
/// `sync_hierarchy` rather than bars in isolation, because the arms
/// advance and finish deep inside the folder and playlist work, and a
/// test that reached for the bar directly would pass with the arms no
/// longer driving their rows at all.
///
/// The variant here is a plain file rather than a ZIP, so no sub-bar
/// opens under the folder. The archive case, where a parent row opens a
/// sub-bar of its own, is in
/// `a_zip_folder_variant_opens_a_sub_bar_under_its_parent_row`.
#[tokio::test]
async fn a_folder_row_and_a_playlist_row_advance_and_terminate() {
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

    let recorded = recording.recorded();
    for label in ["album", "playlists/rickroll.m3u8"] {
        let bar = bar_opened_as(&recorded, label);
        let ops = row_ops(&recorded, bar);
        assert_eq!(
            ops.iter().filter(|op| matches!(op, ProgressOp::Advance { delta: 1 })).count(),
            1,
            "the {label:?} row advances once when its arm's work returns; got {ops:?}",
        );
        assert!(
            matches!(ops.last(), Some(ProgressOp::FinishSuccess)),
            "the {label:?} row terminates on success; got {ops:?}",
        );
    }
}

/// A ZIP folder variant opens a sub-bar under its parent row, with one
/// unit per member the archive held.
///
/// The sibling test `a_folder_row_and_its_variant_sub_bar_share_the_media_id`
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
/// the parent row reaches its write phase — its second identity install —
/// before the sub-bar exists, which is what makes the two rows read as one
/// piece of work rather than a parent that moved on while its children
/// still write;
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
async fn a_zip_folder_variant_opens_a_sub_bar_under_its_parent_row() {
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
            "[src1] album".to_string(),
            "[src1] album".to_string(),
            "[src1] [default] album".to_string(),
        ],
        "the parent row names the entry's identity, and the member row under it adds the \
         variant; got {ops:?}",
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
            (1, "album".to_string()),
            (2, "default".to_string()),
        ],
        "the sub-bar's total is the archive's member count, not the one file a plain variant \
         writes; got {ops:?}",
    );

    let recorded = recording.recorded();
    let parent_bar = bar_opened_as(&recorded, "album");
    let sub_bar = bar_opened_as(&recorded, "default");
    assert_ne!(parent_bar, sub_bar, "the member rows are their own row, not the folder's");

    let sub_bar_at = recorded
        .iter()
        .position(
            |entry| matches!(&entry.op, ProgressOp::AddBar { label, .. } if label == "default"),
        )
        .expect("the archive opens one sub-bar for its variant");
    let parent_installs_before = recorded
        .iter()
        .take(sub_bar_at)
        .filter(|entry| {
            entry.bar == parent_bar && matches!(entry.op, ProgressOp::SetTruncation { .. })
        })
        .count();
    assert_eq!(
        parent_installs_before, 2,
        "the parent row opens and reaches its write phase — its second identity install — \
         before the member rows exist; got {recorded:?}",
    );

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

/// The ops one row recorded, in order.
///
/// Bar-attributed, so a sibling row's interleaved ops cannot be mistaken
/// for this row's sequence.
fn row_ops(recorded: &[RecordedProgressOp], bar: BarId) -> Vec<ProgressOp> {
    recorded.iter().filter(|entry| entry.bar == bar).map(|entry| entry.op.clone()).collect()
}

/// A media entry that writes its variant advances once and finishes on success.
///
/// The fixture this leaned on used to be a media entry with no variant
/// hash, which stops earlier and finishes as skipped; this entry carries a
/// hash the workspace can resolve, so the arm gets as far as the write.
/// The label no longer names the phases the arm walked, so what is pinned
/// is the row's progress instead: one advance when the arm's work returns,
/// then a success terminal.
///
/// The bytes come back off disk as well, so a row cannot claim a
/// successful finish that did not land.
#[tokio::test]
async fn a_written_media_entry_row_advances_and_finishes() {
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

    let recorded = recording.recorded();
    let bar = bar_opened_as(&recorded, "song");
    let ops = row_ops(&recorded, bar);
    assert_eq!(
        ops.iter().filter(|op| matches!(op, ProgressOp::Advance { delta: 1 })).count(),
        1,
        "the song row advances once when the arm's work returns; got {ops:?}",
    );
    assert!(
        matches!(ops.last(), Some(ProgressOp::FinishSuccess)),
        "an entry whose variant landed terminates on success; got {ops:?}",
    );

    assert_eq!(
        std::fs::read(root.path().join("song")).unwrap_or_default().as_slice(),
        payload.as_slice(),
        "the row finished on success because the variant it staged landed in the library",
    );
}

/// A playlist that fails before its write ends its row with the failure marker.
///
/// The playlist arm writes at the end of its work, so an entry that fails
/// while resolving a reference has committed nothing and its row must not
/// read like a completed one. The marker the finish installs is what says
/// so in the text; before the label carried markers, this failure was
/// visible only in the `mediapm_progress_materialize` transcripts, which
/// pin what a row looks like and not that the marker is a consequence of
/// where the arm failed.
#[tokio::test]
async fn a_failed_playlist_row_carries_the_failure_marker() {
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
    let recorded = recording.recorded();
    let bar = bar_opened_as(&recorded, "playlists/broken.m3u8");
    let ops = row_ops(&recorded, bar);
    assert!(
        matches!(ops.last(), Some(ProgressOp::FinishError)),
        "a playlist that never wrote terminates on error; got {ops:?}",
    );
    assert!(
        !ops.iter().any(|op| matches!(op, ProgressOp::FinishSuccess)),
        "a playlist that never wrote must not finish success; got {ops:?}",
    );
    assert_eq!(
        ops.iter().rev().find_map(|op| match op {
            ProgressOp::SetTruncation { prefix, .. } => Some(prefix.as_str()),
            _ => None,
        }),
        Some("[F] broken.m3u8"),
        "the failure marker must sit on the row's identity; got {ops:?}",
    );
}
