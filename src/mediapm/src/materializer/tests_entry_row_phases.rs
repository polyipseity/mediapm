//! The identity an entry row renders, read off the bar constructors alone.
//!
//! Rows carry media id, variant and basename; the phase an arm walks is the
//! bar's own bookkeeping and never reaches the row's text. These tests drive
//! the real constructors through a [`RecordingProgressTracker`], so what is
//! asserted is what the screen receives.

use mediapm_utils::progress::recording::{ProgressOp, RecordingProgressTracker};

use super::*;

/// A bar for every kind renders the entry's identity, and no phase tag ever
/// reaches the row however many phases the arm walks.
///
/// The number of label installs still tracks the declaration: one at
/// creation, one per later declared phase, one at the finish. What changes
/// between them is nothing the reader can see, which is the point — the
/// fraction and the outcome colour carry the row's state, and `[stg]` beside
/// them cost three columns to repeat it.
#[test]
fn entry_bar_renders_identity_not_phase_tags() {
    for kind in
        [HierarchyEntryKind::Media, HierarchyEntryKind::MediaFolder, HierarchyEntryKind::Playlist]
    {
        let phases = kind.phases();
        let tracker = RecordingProgressTracker::new();
        let mut bar =
            EntryPhaseBar::create(Some(Arc::new(tracker.clone())), "Music/album", "album-id", kind);
        for phase in phases.iter().skip(1) {
            bar.enter(*phase);
        }
        bar.finish("F");

        let rendered: Vec<String> = tracker
            .ops()
            .iter()
            .filter_map(|op| match op {
                ProgressOp::SetTruncation { prefix, .. } => Some(prefix.clone()),
                _ => None,
            })
            .collect();
        let installs = 1 + (phases.len() - 1) + 1;
        assert_eq!(
            rendered.len(),
            installs,
            "{kind:?} must reinstall the label at each declared phase and at the finish; \
             got {ops:?}",
            ops = tracker.ops(),
        );
        assert!(
            rendered[..installs - 1].iter().all(|prefix| prefix == "[album-id] album"),
            "{kind:?} rendered something other than its identity before the finish: \
             {rendered:?}",
        );
        assert_eq!(
            tracker.ops().last(),
            Some(&ProgressOp::SetTruncation {
                prefix: "[F] [album-id] album".to_string(),
                suffix: String::new(),
            }),
            "{kind:?} did not finish with its marker on its identity",
        );
    }
}

/// Every kind declares the phases its arm walks.
///
/// A folder makes its directory and then writes each selected variant, so
/// it declares staging and write. A playlist resolves its references,
/// builds the bytes and writes one file, so it declares staging and
/// commit. Neither verifies, so a list with `Verify` in it would put a
/// phase in a bar's bookkeeping with no code behind it. A media entry walks
/// all three phases, and is the only kind that claims verify.
///
/// The media assertion is here for that reason. Deleting the test as a
/// duplicate would leave nothing saying that media is the only kind that
/// lists verify.
#[test]
fn every_kind_declares_the_phases_its_arm_walks() {
    assert_eq!(
        HierarchyEntryKind::MediaFolder.phases(),
        &[MaterializationPhase::Staging, MaterializationPhase::Write],
    );
    assert_eq!(
        HierarchyEntryKind::Playlist.phases(),
        &[MaterializationPhase::Staging, MaterializationPhase::Commit],
    );
    assert_eq!(
        HierarchyEntryKind::Media.phases(),
        &[
            MaterializationPhase::Staging,
            MaterializationPhase::Verify,
            MaterializationPhase::Commit,
        ],
    );
}

/// A media entry that stops before the end renders the same identity as one
/// that finishes, with its marker and resolved variant on it.
///
/// The media arm has one non-success exit, the missing-hash skip, which is a
/// warning; the commit case is the declared end of the media phases rather
/// than a second exit. Both stops render `[W] [song-id] [remaster] song.mkv`:
/// where the arm stopped is the bar's bookkeeping, and the row's text names
/// what is being written and how it ended.
#[test]
fn a_finished_media_entry_carries_marker_and_variant() {
    for phase in [MaterializationPhase::Verify, MaterializationPhase::Commit] {
        let tracker = RecordingProgressTracker::new();
        let mut bar = EntryPhaseBar::create(
            Some(Arc::new(tracker.clone())),
            "song.mkv",
            "song-id",
            HierarchyEntryKind::Media,
        );
        bar.set_variant("remaster");
        bar.enter(phase);
        bar.finish("W");

        assert_eq!(
            tracker.ops().last(),
            Some(&ProgressOp::SetTruncation {
                prefix: "[W] [song-id] [remaster] song.mkv".to_string(),
                suffix: String::new(),
            }),
            "the warning must sit on the row's identity, not on a phase tag",
        );
    }
}

/// A folder row and the sub-bar under it share the media id, and the sub-bar
/// adds the variant it writes.
///
/// Both labels come from the real constructors the folder arm uses:
/// [`EntryPhaseBar`] for the parent and [`add_variant_sub_bar`] for the
/// member row, both recorded through a [`RecordingProgressTracker`] so
/// what is asserted is what the screen receives. Nothing here builds a
/// label by hand, so the test cannot pass while the arm renders something
/// else.
///
/// The shared media id is the point. A folder row covers every variant of
/// the entry, so it names no variant of its own; the rows under it each name
/// one. What ties parent and children together in one column is the media
/// id, and nothing else on the screen compares the two rows.
#[test]
fn a_folder_row_and_its_variant_sub_bar_share_the_media_id() {
    let tracker = RecordingProgressTracker::new();
    let mut parent = EntryPhaseBar::create(
        Some(Arc::new(tracker.clone())),
        "album",
        "album-id",
        HierarchyEntryKind::MediaFolder,
    );
    parent.enter_once(MaterializationPhase::Write);
    let screen: Arc<dyn ProgressScreenApi + Send + Sync> = Arc::new(tracker.clone());
    add_variant_sub_bar(Some(&screen), "album", "album-id", "links", 3)
        .expect("a screen is supplied, so the sub-bar opens");

    let ops = tracker.ops();
    let seeds: Vec<(u64, String)> = ops
        .iter()
        .filter_map(|op| match op {
            ProgressOp::AddBar { total, label } => Some((*total, label.clone())),
            _ => None,
        })
        .collect();
    assert_eq!(
        seeds,
        vec![(1, "album".to_string()), (3, "links".to_string())],
        "seeds name the entry and the variant, never a phase; got {ops:?}",
    );

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
            "[album-id] album".to_string(),
            "[album-id] album".to_string(),
            "[album-id] [links] album".to_string(),
        ],
        "the parent opens on the entry's identity, and the member row adds the variant; \
         got {ops:?}",
    );
}
