//! The phase tag an entry row shows, read off the bar constructors alone.

use mediapm_utils::progress::recording::{ProgressOp, RecordingProgressTracker};

use super::tests_common::rendered_phase_tag;

use super::*;

/// A finished entry names the phase its kind declares last, and no phase
/// outside that declaration ever reaches the row.
///
/// The bug this pins: the folder and playlist arms installed a phase only
/// on the failure path, by passing the literal `"stg"` beside the bar,
/// while the row they had already created sat on `[stg]` for its whole
/// run. Nothing tied the tag to what the arm did, so a finished folder row
/// was indistinguishable from one still staging and the two could drift
/// apart silently. Driving the bar from `HierarchyEntryKind::phases`
/// instead of from literals makes a tag the arm never declared a build of
/// the same kind that fails here.
#[test]
fn entry_bar_installs_exactly_the_phases_its_kind_declares() {
    for kind in
        [HierarchyEntryKind::Media, HierarchyEntryKind::MediaFolder, HierarchyEntryKind::Playlist]
    {
        let phases = kind.phases();
        let tracker = RecordingProgressTracker::new();
        let mut bar = EntryPhaseBar::create(Some(Arc::new(tracker.clone())), "Music/album", kind);
        for phase in phases.iter().skip(1) {
            bar.enter(*phase);
        }
        bar.finish("F");

        let rendered: Vec<String> = tracker
            .ops()
            .iter()
            .filter_map(|op| match op {
                ProgressOp::SetTruncation { prefix, .. } => {
                    Some(rendered_phase_tag(prefix).unwrap_or_default().to_string())
                }
                _ => None,
            })
            .collect();
        let mut expected: Vec<String> =
            phases.iter().map(|phase| phase.tag().to_string()).collect();
        let last = expected.last().cloned().expect("a kind declares at least one phase");
        expected.push(last.clone());

        assert_eq!(rendered, expected, "{kind:?} rendered a tag its own declaration does not list");
        assert_eq!(
            tracker.ops().last(),
            Some(&ProgressOp::SetTruncation {
                prefix: format!("[F] album Music [{last}]"),
                suffix: String::new(),
            }),
            "{kind:?} did not finish on the last phase it declares",
        );
    }
}

/// Every kind declares the phases its arm walks.
///
/// A folder makes its directory and then writes each selected variant, so
/// it declares staging and write. A playlist resolves its references,
/// builds the bytes and writes one file, so it declares staging and
/// commit. Neither verifies, so a list with `[vrf]` in it would put a tag
/// on the screen with no code behind it. A media entry walks all three
/// phases, and is the only kind that claims verify.
///
/// The media assertion is here for that reason. Deleting the test as a
/// duplicate of the tag-driven ones above would leave nothing saying that
/// media is the only kind that lists verify, since every other assertion
/// in the module reads a declaration rather than pinning one.
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

/// A media entry that stops before the end names the phase it reached.
///
/// The finish reads the phase the row is on rather than a literal, so a
/// stop at verify and a stop at commit name different tags. The media
/// arm has one non-success exit, the missing-hash skip, which is a warning
/// and leaves the row on `[vrf]`; the commit case here is the declared end
/// of the media phases rather than a second exit, and pins that the same
/// finish reports it.
///
/// One literal per exit is the shape this replaced: the media arm passed
/// `"vrf"` at its skip and the folder and playlist arms passed `"stg"` at
/// a failure, each correct only as long as no arm moved.
#[test]
fn a_finished_media_entry_names_the_phase_it_reached() {
    for (phase, tag) in
        [(MaterializationPhase::Verify, "vrf"), (MaterializationPhase::Commit, "cmt")]
    {
        let tracker = RecordingProgressTracker::new();
        let mut bar = EntryPhaseBar::create(
            Some(Arc::new(tracker.clone())),
            "song.mkv",
            HierarchyEntryKind::Media,
        );
        bar.enter(phase);
        bar.finish("W");

        assert_eq!(
            tracker.ops().last(),
            Some(&ProgressOp::SetTruncation {
                prefix: format!("[W] song.mkv [{tag}]"),
                suffix: String::new(),
            }),
            "the warning must name the phase the entry reached",
        );
    }
}

/// A folder row and the sub-bar under it render the same phase word.
///
/// Both labels come from the real constructors the folder arm uses:
/// [`EntryPhaseBar`] for the parent and [`add_variant_sub_bar`] for the
/// member row, both recorded through a [`RecordingProgressTracker`] so
/// what is asserted is what the screen receives. Nothing here builds a
/// label by hand, so the test cannot pass while the arm renders something
/// else.
///
/// The agreement is the point. A folder row that sat on `[stg]` under
/// children reading `[wrt]`, or the reverse, would describe the same work
/// two different ways in one column, and nothing else on the screen
/// compares the two rows.
#[test]
fn a_folder_row_and_its_variant_sub_bar_render_the_same_phase() {
    let tracker = RecordingProgressTracker::new();
    let mut parent = EntryPhaseBar::create(
        Some(Arc::new(tracker.clone())),
        "album",
        HierarchyEntryKind::MediaFolder,
    );
    parent.enter_once(MaterializationPhase::Write);
    let screen: Arc<dyn ProgressScreenApi + Send + Sync> = Arc::new(tracker.clone());
    add_variant_sub_bar(Some(&screen), "album", "links", 3)
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
        vec![(1, "album [stg]".to_string()), (3, "links [wrt]".to_string())],
        "both seeds name a phase, since a seed is what a row shows before its first render; got {ops:?}",
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
        vec!["album [stg]".to_string(), "album [wrt]".to_string(), "album links [wrt]".to_string(),],
        "the parent opens on [stg], moves to [wrt], and the member row follows it; got {ops:?}",
    );

    let parent_tag = rendered_phase_tag(&rendered[1]);
    let child_tag = rendered_phase_tag(&rendered[2]);
    assert_eq!(parent_tag, Some("wrt"), "the folder row is on its write phase");
    assert_eq!(child_tag, parent_tag, "a child may not name another phase than its parent");
    assert_eq!(
        parent_tag,
        HierarchyEntryKind::MediaFolder.phases().last().map(|phase| phase.tag()),
        "the write is the last phase a folder declares, so the row that shows it is the declared end",
    );
}
