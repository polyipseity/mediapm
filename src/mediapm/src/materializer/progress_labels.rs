//! Client-defined bar-label truncation for materialization progress.
//!
//! Materialization bars carry file-path identity and phase tags. The
//! [`MaterializationBarLabel`] struct owns its field names and truncation
//! order independently of the conductor's [`StepBarLabel`] and
//! [`WorkerBarLabel`].

use mediapm_utils::progress::{BarLabelTruncation, Segment, SuffixComponents, fit_segments};

/// Truncation order for a materialization bar.
///
/// Segments are ordered most important first and yield from the tail.
/// Prefix: `phase` → `status_marker` (both protected) → `entry_name` →
/// `file_name` (sub-bars only) → `entry_path` (elastic, shortened from the
/// front so the directory tail survives).
///
/// Materialization bars carry no version, no count/total, and no
/// workflow/step identity.
#[derive(Debug, Clone, Default)]
pub(crate) struct MaterializationBarLabel {
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
    /// Minimum prefix width at which the protected head still fits whole.
    ///
    /// Measured as the rendered width of `[wrt] [F]`. Below this width
    /// protection lifts and the tail is dropped as a whole segment.
    pub(crate) const PREFIX_FLOOR: usize = 9;

    fn prefix_segments(&self) -> Vec<Segment> {
        let mut segs = Vec::new();
        if !self.phase.is_empty() {
            segs.push(Segment::protected(format!("[{}]", self.phase)));
        }
        if !self.status_marker.is_empty() {
            segs.push(Segment::protected(format!("[{}]", self.status_marker)));
        }
        if !self.entry_name.is_empty() {
            segs.push(Segment::new_keep(self.entry_name.clone()));
        }
        if !self.file_name.is_empty() {
            segs.push(Segment::new_keep(self.file_name.clone()));
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
        fit_segments(&self.prefix_segments(), max_width, Self::PREFIX_FLOOR)
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

    /// Every protected field survives the full floor band, and no width
    /// overflows. Detects `entry_name` or `entry_path` being promoted ahead
    /// of `phase` or `status_marker`: such a reordering sheds the head at
    /// these widths and fails the first two assertions.
    #[test]
    fn protected_head_survives_at_the_floor() {
        let label = MaterializationBarLabel {
            status_marker: "F".into(),
            entry_path: "Music/Artist/Album".into(),
            entry_name: "song.mkv".into(),
            file_name: "cover.jpg".into(),
            phase: "wrt".into(),
        };
        for width in MaterializationBarLabel::PREFIX_FLOOR..=18 {
            let out = label.truncate_prefix(width);
            assert!(out.contains("[wrt]"), "phase lost at width {width}: {out:?}");
            assert!(out.contains("[F]"), "status lost at width {width}: {out:?}");
            assert!(out.chars().count() <= width, "overflowed at width {width}: {out:?}");
        }
    }

    /// The phase tag outlives the elastic path, and `entry_name` outlives
    /// the path too: both outrank the single elastic segment.
    #[test]
    fn protected_phase_survives_entry_path_pressure() {
        let label = MaterializationBarLabel {
            entry_path: "Music/Artist/Album".into(),
            entry_name: "song.mkv".into(),
            phase: "stg".into(),
            ..Default::default()
        };
        let tight = label.truncate_prefix(25);
        assert!(tight.contains("[stg]"), "phase lost: {tight:?}");
        assert!(tight.contains("song.mkv"), "entry_name lost: {tight:?}");
    }

    /// The elastic path is shortened from the front so the directory tail —
    /// the part adjacent to the filename — survives, where the prefix cut
    /// this replaces yielded the uninformative `Music/Artist/Al…` head.
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

        // Tight: the path must keep its tail, never its head. The old
        // implementation prefix-cut, yielding `Music/Artist/Al…`.
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
    /// The previous version of this test asserted only on the wide case,
    /// which passes trivially and did not test what its name claimed. This
    /// version asserts on the tight case, where ordering actually bites.
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
