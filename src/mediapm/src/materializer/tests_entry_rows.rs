//! The identity an entry row renders, read off the real bar constructor.
//!
//! Rows carry media id, variant and basename, and a row's total is the
//! files it writes; no phase enum exists to reach the row's text. These
//! tests drive [`EntryRowBar`] through a [`RecordingProgressTracker`], so
//! what is asserted is what the screen receives.

use mediapm_utils::progress::recording::{ProgressOp, RecordingProgressTracker};

use super::*;

/// The prefix installs a row's label recorded, in order.
fn rendered_prefixes(tracker: &RecordingProgressTracker) -> Vec<String> {
    tracker
        .ops()
        .iter()
        .filter_map(|op| match op {
            ProgressOp::SetTruncation { prefix, .. } => Some(prefix.clone()),
            _ => None,
        })
        .collect()
}

/// A row opens on its entry's identity and finishes with its marker on the
/// same identity.
///
/// The finish marker is the only label change a row makes after creation —
/// the fraction and the outcome colour carry everything else — so a failed
/// row reads `[F] [album-id] album`, with no phase tag anywhere on the
/// install path.
#[test]
fn entry_row_opens_on_identity_and_finishes_with_its_marker() {
    let tracker = RecordingProgressTracker::new();
    let bar =
        EntryRowBar::create(Some(Arc::new(tracker.clone())), "Music/album", "album-id", "", 1);
    bar.finish("F");

    let ops = tracker.ops();
    assert!(
        ops.iter().any(
            |op| matches!(op, ProgressOp::AddBar { total: 1, label } if label == "Music/album")
        ),
        "the seed names the entry's path, never a phase; got {ops:?}",
    );
    assert_eq!(
        rendered_prefixes(&tracker),
        vec!["[album-id] album".to_string(), "[F] [album-id] album".to_string(),],
        "a row opens on its identity and the finish puts the marker on it; got {ops:?}",
    );
}

/// A media row carries the variant its selector resolved, on every install
/// from the resolution onward.
///
/// The selector resolves inside the arm, after the row exists, so the row
/// opens variant-less and `set_variant` re-installs the label with the
/// variant the write actually takes. The no-hash skip then puts `[W]` on
/// that same identity — the row names what it was writing and how it ended.
#[test]
fn a_media_row_carries_its_resolved_variant() {
    let tracker = RecordingProgressTracker::new();
    let mut bar =
        EntryRowBar::create(Some(Arc::new(tracker.clone())), "song.mkv", "song-id", "", 1);
    bar.set_variant("remaster");
    bar.finish("W");

    assert_eq!(
        rendered_prefixes(&tracker),
        vec![
            "[song-id] song.mkv".to_string(),
            "[song-id] [remaster] song.mkv".to_string(),
            "[W] [song-id] [remaster] song.mkv".to_string(),
        ],
        "the variant reaches the label when the selector resolves, and the marker sits \
         on it; got {ops:?}",
        ops = tracker.ops(),
    );
}

/// A folder row and the variant sub-bar under it share the media id, and
/// the sub-bar adds the variant it writes.
///
/// Both rows come from the real constructor the folder arm uses, recorded
/// through a [`RecordingProgressTracker`], so nothing here builds a label
/// by hand. The shared media id is the point: the parent row covers every
/// variant of the entry and names none of its own, each sub-bar names one,
/// and the media id is what ties parent and children together in one
/// column.
#[test]
fn a_folder_row_and_its_variant_sub_bar_share_the_media_id() {
    let tracker = RecordingProgressTracker::new();
    let _parent = EntryRowBar::create(Some(Arc::new(tracker.clone())), "album", "album-id", "", 1);
    let _sub_bar =
        EntryRowBar::create(Some(Arc::new(tracker.clone())), "album", "album-id", "links", 3);

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
        "the parent totals the one file its entry writes, the sub-bar totals the \
         variant's files, and the seeds name the entry and the variant; got {ops:?}",
    );
    assert_eq!(
        rendered_prefixes(&tracker),
        vec!["[album-id] album".to_string(), "[album-id] [links] album".to_string(),],
        "the parent opens on the entry's identity and the sub-bar adds the variant; \
         got {ops:?}",
    );
}

/// No label the constructor installs names a phase.
///
/// Phase tags were the screen's legibility defect and the type that backed
/// them is gone; this sweep over the constructor's installs — a variant-less
/// row, a variant sub-bar, and a finished media row — is what notices if a
/// tag comes back through the production path rather than through the label
/// alone.
#[test]
fn no_row_render_carries_a_phase_tag() {
    let tracker = RecordingProgressTracker::new();
    let _parent = EntryRowBar::create(Some(Arc::new(tracker.clone())), "album", "album-id", "", 1);
    let _sub_bar =
        EntryRowBar::create(Some(Arc::new(tracker.clone())), "album", "album-id", "links", 3);
    let mut media =
        EntryRowBar::create(Some(Arc::new(tracker.clone())), "song.mkv", "song-id", "", 1);
    media.set_variant("remaster");
    media.finish("W");

    for prefix in rendered_prefixes(&tracker) {
        for tag in ["stg", "vrf", "cmt", "wrt"] {
            assert!(!prefix.contains(tag), "phase tag {tag:?} in {prefix:?}");
        }
    }
}
