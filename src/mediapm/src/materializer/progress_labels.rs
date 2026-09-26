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
/// Segments are ordered most important first and yield from the tail.
/// Prefix: `phase` → `status_marker` → `entry_name` → `file_name` (sub-bars
/// only) → `entry_path` (elastic, shortened from the front so the directory
/// tail survives).
///
/// The order is the whole mechanism. `fit_segments` drops from the tail
/// unconditionally, so a field survives by its position in the list.
///
/// Materialization bars carry no version, no count/total, and no
/// workflow/step identity.
#[derive(Debug, Clone, Default)]
pub(crate) struct MaterializationBarLabel {
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
    fn prefix_segments(&self) -> Vec<Segment> {
        let mut segs = Vec::new();
        if !self.phase.is_empty() {
            segs.push(Segment::keep(format!("[{}]", self.phase)));
        }
        if !self.status_marker.is_empty() {
            segs.push(Segment::keep(format!("[{}]", self.status_marker)));
        }
        if !self.entry_name.is_empty() {
            segs.push(Segment::keep(self.entry_name.clone()));
        }
        if !self.file_name.is_empty() {
            segs.push(Segment::keep(self.file_name.clone()));
        }
        // Entry path is the directory portion and the only elastic segment:
        // shortened from the front so the tail adjacent to the filename
        // survives, rather than dropped whole.
        if !self.entry_path.is_empty() {
            segs.push(Segment::elastic(self.entry_path.clone()));
        }
        segs
    }
}

impl BarLabelTruncation for MaterializationBarLabel {
    fn truncate_prefix(&self, max_width: usize) -> String {
        fit_segments(&self.prefix_segments(), max_width)
    }

    fn truncate_suffix(&self, _max_width: usize, _suffix: &SuffixComponents) -> String {
        // Materialization bars have no suffix components.
        String::new()
    }
}

/// Split a relative path into `(entry_path, entry_name)`.
///
/// Returns `("", path)` when the path has no `/` separator.
pub(crate) fn split_entry_path(path: &str) -> (&str, &str) {
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

    /// `entry_name` outranks the path: the long name survives while the
    /// longer path is shortened and then surrendered whole.
    #[test]
    fn entry_name_outranks_entry_path() {
        let label = MaterializationBarLabel {
            entry_path: "very/long/path/segments".into(),
            entry_name: "important-file.mkv".into(),
            phase: "cmt".into(),
            ..Default::default()
        };
        let tight = label.truncate_prefix(25);
        assert!(tight.contains("important-file.mkv"), "entry_name dropped: {tight:?}");
        assert!(tight.contains("[cmt]"), "phase dropped: {tight:?}");
    }

    /// On sub-bars the extracted `file_name` outranks the directory path.
    ///
    /// The prior version's tight assertion was vacuous; this version
    /// asserts on the tight case, where ordering actually bites.
    #[test]
    fn file_name_outranks_entry_path_on_sub_bars() {
        let label = MaterializationBarLabel {
            entry_path: "Music/Artist/Album".into(),
            entry_name: "album".into(),
            file_name: "cover.jpg".into(),
            phase: "wrt".into(),
            ..Default::default()
        };
        let wide = label.truncate_prefix(80);
        assert!(wide.contains("cover.jpg"), "file_name missing when wide: {wide:?}");

        let tight = label.truncate_prefix(21);
        assert!(tight.contains("[wrt]"), "phase lost: {tight:?}");
        assert!(tight.contains("cover.jpg"), "file_name dropped before entry_path: {tight:?}");
        assert!(!tight.contains("Music/"), "entry_path survived past file_name: {tight:?}");
        assert!(tight.chars().count() <= 21, "overflowed: {tight:?}");
    }
}
