//! Bracket guard for every progress bar label on every screen.
//!
//! A label hands the renderer [`Segment`]s, and a segment's brackets are
//! decoration that survives only when the segment renders whole. The fault
//! this file watches for is a bracket reaching a row without its partner: a
//! clip that cut the text beside it, or a caller that formatted its own
//! brackets into elastic text instead of handing them over as
//! [`Brackets`]. Either way the row reads as `Give You Up]`, which says
//! nothing about what is being written.
//!
//! The clip fault was found twice in one shape. `WorkerBarLabel.tool` and
//! `StepBarLabel.tool` put their parentheses on the segment rather than in the
//! text, and `MaterializationBarLabel` split its media id out of `entry_name`
//! for the same reason. A third label could reintroduce it by forgetting, and
//! a name carrying a bracket group of its own would bring it back through the
//! front door. So the check is on the rendered row rather than on one label's
//! segments, and it runs over every label a screen can install.
//!
//! Widths sweep 8 to 120, the range the screen harnesses accept, because the
//! bracket shows up in the middle of that range rather than at its edge: a
//! row wide enough for the whole label and a row too narrow for any of it both
//! render cleanly.

use mediapm::MaterializationBarLabel;
use mediapm_conductor::orchestration::progress_labels::{StepBarLabel, WorkerBarLabel};
use mediapm_utils::progress::BarLabelTruncation;

/// First bracket on `row` that has no partner, in either direction.
///
/// A `[` that is never closed and a `]` that closes nothing are both an
/// orphan, and a `)` closing a `[` is one too, so the scan carries the
/// expected partner rather than counting each kind separately.
fn orphan_bracket(row: &str) -> Option<char> {
    let mut open: Vec<char> = Vec::new();
    for ch in row.chars() {
        match ch {
            '[' => open.push(']'),
            '(' => open.push(')'),
            ']' | ')' if open.pop() != Some(ch) => return Some(ch),
            _ => {}
        }
    }
    open.into_iter().next()
}

/// Every label a screen can install, in the states its coordinator builds.
///
/// One instance per field shape rather than one per value: what the guard
/// checks is whether a bracket can reach a row without its partner, and that
/// turns on which segments carry decoration and which of them are elastic,
/// not on the strings inside them. `MaterializationBarLabel` is the reason
/// this list is here at all, so it carries both a name whose bracket group
/// ends the basename and one whose group is followed by an extension.
fn every_label() -> Vec<(&'static str, Box<dyn BarLabelTruncation>)> {
    let materialization = |entry_name: &'static str| {
        Box::new(MaterializationBarLabel {
            status_marker: String::new(),
            entry_path: "Music/Artist/Album".into(),
            entry_name: entry_name.into(),
            file_name: String::new(),
            phase: None,
        }) as Box<dyn BarLabelTruncation>
    };
    vec![
        (
            "materialization media link name",
            materialization("01 - Telepathy.flac [youtube.dQw4w9WgXcQ].link.mkv"),
        ),
        (
            "materialization folder name",
            materialization("Rick Astley - Never Gonna Give You Up [youtube.dQw4w9WgXcQ]"),
        ),
        ("materialization plain name", materialization("song.mkv")),
        (
            "materialization failed sub-bar",
            Box::new(MaterializationBarLabel {
                status_marker: "F".into(),
                entry_path: "Music/Artist/Album".into(),
                entry_name: "album [youtube.dQw4w9WgXcQ]".into(),
                file_name: "01 - cover.jpg".into(),
                phase: Some(mediapm::MaterializationPhase::Write),
            }),
        ),
        (
            "step bar running",
            Box::new(StepBarLabel {
                status_marker: String::new(),
                workflow_id: "default".into(),
                step_id: "s3".into(),
                tool: "media-conductor-builtin-ffmpeg".into(),
                version: "7.1".into(),
                completed: "2".into(),
                total: "5".into(),
            }),
        ),
        (
            "step bar failed",
            Box::new(StepBarLabel {
                status_marker: "F".into(),
                workflow_id: "default".into(),
                step_id: "s3".into(),
                tool: "ffmpeg".into(),
                version: String::new(),
                completed: String::new(),
                total: String::new(),
            }),
        ),
        (
            "worker slot active",
            Box::new(WorkerBarLabel {
                status_marker: String::new(),
                workflow_id: "default".into(),
                step_id: "s5".into(),
                tool: "media-conductor-builtin-echo".into(),
                activity: "active".into(),
            }),
        ),
        (
            "worker slot failed",
            Box::new(WorkerBarLabel {
                status_marker: "F".into(),
                workflow_id: String::new(),
                step_id: String::new(),
                tool: String::new(),
                activity: "idle".into(),
            }),
        ),
        (
            "worker slot retrying",
            Box::new(WorkerBarLabel {
                status_marker: "W".into(),
                workflow_id: String::new(),
                step_id: String::new(),
                tool: String::new(),
                activity: "idle".into(),
            }),
        ),
    ]
}

/// No rendered row, at any width, shows half a bracket pair.
///
/// The range is the one the screen harnesses accept, so this covers the
/// narrow rows that clip and the wide ones that do not. A failure names the
/// label and the width, since the two together are what a reader needs to
/// reproduce it.
#[test]
fn no_rendered_row_carries_an_orphaned_bracket() {
    for (name, label) in every_label() {
        for width in 8..=120 {
            let row = label.truncate_prefix(width);
            assert!(
                row.chars().count() <= width,
                "{name} overflowed its slot at width {width}: {row:?}"
            );
            if let Some(orphan) = orphan_bracket(&row) {
                panic!("{name} rendered an orphan {orphan:?} at width {width}: {row:?}");
            }
        }
    }
}
