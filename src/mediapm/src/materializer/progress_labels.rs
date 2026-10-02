//! Client-defined bar-label truncation for materialization progress.
//!
//! Materialization bars carry file-path identity and phase tags. The
//! [`MaterializationBarLabel`] struct owns its field names and truncation
//! order independently of the conductor's `StepBarLabel` and
//! `WorkerBarLabel`, which live in `mediapm-conductor` and are not visible
//! to rustdoc from this crate.

use mediapm_utils::progress::{BarLabelTruncation, Segment, SuffixComponents, fit_segments};

/// Truncation order for a materialization bar.
///
/// Segments are ordered most important first and yield from the tail. The
/// prefix is `phase`, `status_marker`, `entry_name`, `entry_path`,
/// `file_name`, with the two path halves elastic so they shorten from the
/// front. The suffix is `elapsed`, `rate`, `eta`, then an elastic `custom`,
/// in the same order `WorkerBarLabel` uses.
///
/// The order is the whole mechanism. `fit_segments` drops from the tail
/// unconditionally, so a field survives by its position in the list, and it
/// shrinks elastic fields from the tail too, so a field listed later is the
/// one that gives up columns first.
///
/// A reader scanning this screen wants to know, per row, what is being
/// written and whether it worked. `phase` says what the bar is doing,
/// `status_marker` says it failed or was skipped, and the two path halves say
/// which file. A row that renders as `[stg]` on its own answers none of that,
/// which is what made the old ranking a defect rather than a preference.
///
/// The prefix carries no version, no count/total, and no workflow/step
/// identity, and neither does the suffix: the bar's position out of total is
/// not a number this label can put a name to.
#[derive(Debug, Clone, Default)]
pub struct MaterializationBarLabel {
    /// Terminal-state marker, rendered bracketed as `[F]` or `[W]`. Empty
    /// when the entry has no status, in which case the segment is omitted.
    pub status_marker: String,
    /// Directory portion of hierarchy path, e.g. `"Music/Artist/Album"`.
    pub entry_path: String,
    /// Basename of hierarchy entry, e.g. `"song.mkv"`.
    pub entry_name: String,
    /// Extracted file basename (sub-bars only), e.g. `"cover.jpg"`.
    pub file_name: String,
    /// Materialization phase tag: `"stg"` / `"vrf"` / `"cmt"` / `"wrt"` / `"mat"`.
    pub phase: String,
}

impl MaterializationBarLabel {
    /// Build the prefix segments, most important first.
    ///
    /// `phase` and `status_marker` lead, so width pressure sheds the path
    /// before it sheds the two things that say what the row is doing and
    /// whether it went wrong.
    ///
    /// `entry_name` and `entry_path` are both elastic, and they yield in that
    /// order because `fit_segments` shrinks from the tail: the directory
    /// shortens to its last column first, then the basename shortens to its
    /// last few characters. That tail is the informative end of each, since
    /// the leading half of a path is the artist's name and the row is not
    /// about the artist.
    ///
    /// `entry_name` is elastic rather than kept whole because a kept name is a
    /// name that vanishes: at 36 columns the online demo's 59-column folder
    /// name left the reader with a bare `[stg]`. Shortened, its tail still
    /// carries the extension and the bracketed media id, which is what
    /// separates one video from another in a list of them.
    ///
    /// `file_name` ranks last. It only appears on a `[wrt]` sub-bar, where it
    /// repeats the variant name the parent row already implies, and there are
    /// as many of those rows as there are extracted members. The directory
    /// says where the members are going, and no other row carries it.
    fn prefix_segments(&self) -> Vec<Segment> {
        let mut segs = Vec::new();
        if !self.phase.is_empty() {
            segs.push(Segment::keep(format!("[{}]", self.phase)));
        }
        if !self.status_marker.is_empty() {
            segs.push(Segment::keep(format!("[{}]", self.status_marker)));
        }
        if !self.entry_name.is_empty() {
            segs.push(Segment::elastic(self.entry_name.clone()));
        }
        if !self.entry_path.is_empty() {
            segs.push(Segment::elastic(self.entry_path.clone()));
        }
        if !self.file_name.is_empty() {
            segs.push(Segment::keep(self.file_name.clone()));
        }
        segs
    }

    /// Build the suffix segments, most important first.
    ///
    /// Takes no `&self`: a materialization row has no suffix field of its own
    /// to contribute, which is the same reason
    /// `WorkerBarLabel::suffix_segments` takes none, and it is why this body
    /// is identical to that one.
    ///
    /// The renderer passes its merged `SuffixComponents` in, and
    /// `elapsed`, `rate`, and `eta` arrive through it, so reading them here
    /// is the only way a materialization row shows how long it has been
    /// running. They are `Segment::keep`, so a narrow suffix drops them whole
    /// rather than shaving `0m12s` into `…2s`. Only free-form `custom` text
    /// is elastic.
    ///
    /// The count and the total are not rendered. This label has no tally of
    /// its own to name the numbers, which is the same reason
    /// `WorkerBarLabel::suffix_segments` skips them.
    fn suffix_segments(suffix: &SuffixComponents) -> Vec<Segment> {
        let mut segs = Vec::new();
        if !suffix.elapsed.is_empty() {
            segs.push(Segment::keep(suffix.elapsed.clone()));
        }
        if let Some(ref rate) = suffix.rate {
            segs.push(Segment::keep(rate.clone()));
        }
        if let Some(ref eta) = suffix.eta {
            segs.push(Segment::keep(eta.clone()));
        }
        if !suffix.custom.is_empty() {
            segs.push(Segment::elastic(suffix.custom.clone()));
        }
        segs
    }
}

impl BarLabelTruncation for MaterializationBarLabel {
    fn truncate_prefix(&self, max_width: usize) -> String {
        fit_segments(&self.prefix_segments(), max_width)
    }

    fn truncate_suffix(&self, max_width: usize, suffix: &SuffixComponents) -> String {
        fit_segments(&Self::suffix_segments(suffix), max_width)
    }
}

/// Split a relative path into `(entry_path, entry_name)`.
///
/// The first element is everything before the last `/`, which is what
/// [`MaterializationBarLabel::entry_path`] holds, and the second is the
/// basename, which is what [`MaterializationBarLabel::entry_name`] holds. A
/// path with no `/` yields `("", path)`, so a caller can pass the result
/// straight into the label without special-casing a bare filename.
///
/// This is the split the materializer applies at
/// `src/mediapm/src/materializer/mod.rs:396` and `:744` when it builds a bar
/// label, so an entry path outside the library, such as the
/// `mediapm_progress_materialize` example, can render the same label the
/// library renders instead of splitting the path a second way.
#[must_use]
pub fn split_entry_path(path: &str) -> (&str, &str) {
    match path.rfind('/') {
        Some(pos) => (&path[..pos], &path[pos + 1..]),
        None => ("", path),
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn split_entry_path_with_separators() {
        let (dir, name) = split_entry_path("Music/Artist/Album/song.mkv");
        assert_eq!(dir, "Music/Artist/Album");
        assert_eq!(name, "song.mkv");
    }

    #[test]
    fn split_entry_path_no_separators() {
        let (dir, name) = split_entry_path("song.mkv");
        assert_eq!(dir, "");
        assert_eq!(name, "song.mkv");
    }

    /// Every leading field survives the width band, and no width
    /// overflows. Detects `entry_name` or `entry_path` being promoted ahead
    /// of `phase` or `status_marker`: such a reordering sheds the head at
    /// these widths and fails the first two assertions.
    #[test]
    fn head_survives_across_narrow_widths() {
        /// Visible width of the leading `[wrt] [F]` head.
        const HEAD_WIDTH: usize = 9;
        let label = MaterializationBarLabel {
            status_marker: "F".into(),
            entry_path: "Music/Artist/Album".into(),
            entry_name: "song.mkv".into(),
            file_name: "cover.jpg".into(),
            phase: "wrt".into(),
        };
        for width in HEAD_WIDTH..=18 {
            let out = label.truncate_prefix(width);
            assert!(out.contains("[wrt]"), "phase lost at width {width}: {out:?}");
            assert!(out.contains("[F]"), "status lost at width {width}: {out:?}");
            assert!(out.chars().count() <= width, "overflowed at width {width}: {out:?}");
        }
    }

    /// The elastic path is shortened from the front so the directory tail —
    /// the part adjacent to the filename — survives. It used to be dropped
    /// whole, because it ranked last.
    #[test]
    fn entry_path_is_shortened_from_the_front() {
        let label = MaterializationBarLabel {
            entry_path: "Music/Artist/Album/1977".into(),
            entry_name: "song.mkv".into(),
            phase: "stg".into(),
            ..Default::default()
        };
        // Wide enough for the full path, so nothing is shortened.
        let wide = label.truncate_prefix(80);
        assert!(wide.contains("Music/Artist/Album/1977"), "wide path missing: {wide:?}");

        // Tight: the path must keep its tail, never its head.
        let tight = label.truncate_prefix(20);
        assert!(tight.contains('…'), "path not front-ellipsised: {tight:?}");
        assert!(!tight.contains("Music/"), "path head retained instead of tail: {tight:?}");
        assert!(tight.contains("song.mkv"), "entry_name lost: {tight:?}");
    }

    /// The name shortens from the front, so the extension survives, and the
    /// path yields its columns first because it is listed after the name and
    /// `fit_segments` shrinks from the tail.
    #[test]
    fn entry_name_shortens_from_the_front_and_the_path_yields_first() {
        let label = MaterializationBarLabel {
            entry_path: "very/long/path/segments".into(),
            entry_name: "important-file.mkv".into(),
            phase: "cmt".into(),
            ..Default::default()
        };
        assert_eq!(label.truncate_prefix(25), "[cmt] …ortant-file.mkv …s");
        assert_eq!(label.truncate_prefix(20), "[cmt] …t-file.mkv …s");
    }

    /// A folder name wider than the budget still names itself.
    ///
    /// The name is the online demo's, and 59 columns is ordinary for a
    /// YouTube-style title, not exotic. `entry_name` used to be a
    /// `Segment::keep` ranked above the elastic `entry_path`, so once the
    /// path had shrunk as far as it could, `fit_segments` dropped the name
    /// whole and the row rendered as a bare `[stg]`. See
    /// `MaterializationBarLabel` for the order that fixes it.
    #[test]
    fn an_over_long_folder_name_keeps_the_tail_of_its_name() {
        let label = MaterializationBarLabel {
            entry_path: "music videos".into(),
            entry_name: "Rick Astley - Never Gonna Give You Up [youtube.dQw4w9WgXcQ]".into(),
            phase: "stg".into(),
            ..Default::default()
        };
        let out = label.truncate_prefix(36);
        assert!(out.contains("[stg]"), "phase lost: {out:?}");
        assert!(out.contains("dQw4w9WgXcQ"), "the media id identifying the name was cut: {out:?}");
        assert!(out.chars().count() <= 36, "overflowed: {out:?}");
    }

    /// A materialization row shows the timing the renderer derives for it.
    ///
    /// `elapsed`, `rate`, and `eta` reach the label through the merged
    /// `SuffixComponents` the renderer passes in, so dropping them from
    /// `truncate_suffix` strips the timing off every row on the screen.
    #[test]
    fn suffix_renders_the_auto_derived_timing() {
        let label = MaterializationBarLabel {
            entry_name: "song.mkv".into(),
            phase: "cmt".into(),
            ..Default::default()
        };
        let suffix = SuffixComponents {
            count: "2".into(),
            total: "5".into(),
            elapsed: "0m12s".into(),
            rate: Some("1.2 MiB/s".into()),
            eta: Some("[0m03s]".into()),
            custom: String::new(),
        };
        assert_eq!(label.truncate_suffix(80, &suffix), "0m12s 1.2 MiB/s [0m03s]");
        // eta yields before rate, and rate before elapsed, so a narrow
        // suffix still names how long the row has been running.
        assert_eq!(label.truncate_suffix(6, &suffix), "0m12s");
    }

    /// `file_name` ranks below both halves of the path, so a sub-bar row
    /// surrenders the variant name whole before it loses the directory.
    ///
    /// The old ranking put `file_name` above `entry_path`, and at 21 columns
    /// that difference was invisible because nothing had to be dropped. The
    /// pair of widths below straddles the point where it does, so a reordering
    /// that moved `file_name` back up would fail the narrow one.
    #[test]
    fn file_name_yields_before_the_path_on_sub_bars() {
        let label = MaterializationBarLabel {
            entry_path: "Music/Rick Astley/Never Gonna Give You Up".into(),
            entry_name: "album".into(),
            file_name: "cover.jpg".into(),
            phase: "wrt".into(),
            ..Default::default()
        };
        assert_eq!(label.truncate_prefix(21), "[wrt] …m …p cover.jpg");
        assert_eq!(label.truncate_prefix(20), "[wrt] …m …p");
    }
}
