//! The total an entry row opens with, against the advances that row makes.

use crate::config::hierarchy_types::{HierarchyNodeKind, HierarchyPath};
use crate::config::{GenericOutputVariantConfig, OutputVariantValue};
use mediapm_utils::progress::recording::{ProgressOp, RecordingProgressTracker};

use super::tests_common::{
    folder_and_playlist_document, open_hierarchy_cas, single_media_document,
};

use super::*;

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
async fn entry_row_total_and_advances(build: impl FnOnce(String) -> MediaPmDocument) -> (u64, u64) {
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
    let last_bar = ops.iter().rposition(|op| matches!(op, ProgressOp::AddBar { .. })).unwrap_or(0);
    let advances =
        ops[last_bar..].iter().filter(|op| matches!(op, ProgressOp::Advance { .. })).count() as u64;
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
