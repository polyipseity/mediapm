//! The phases each materialization arm walks, observed through a real `sync_hierarchy`.

use crate::config::hierarchy_types::HierarchyNodeKind;
use mediapm_utils::progress::recording::{
    BarId, ProgressOp, RecordedProgressOp, RecordingProgressTracker,
};

use super::tests_common::{
    folder_and_playlist_document, open_hierarchy_cas, playlist_with_unknown_reference_document,
    rendered_phase_tag, zip_payload,
};

use super::*;

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
            ProgressOp::SetTruncation { prefix, .. } if prefix.starts_with("rickroll.m3u8 ") => {
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
        .position(
            |op| matches!(op, ProgressOp::SetTruncation { prefix, .. } if prefix == "album [wrt]"),
        )
        .expect("the folder row reaches the write");
    let sub_bar_at = ops
        .iter()
        .position(|op| matches!(op, ProgressOp::AddBar { label, .. } if label == "default [wrt]"))
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
