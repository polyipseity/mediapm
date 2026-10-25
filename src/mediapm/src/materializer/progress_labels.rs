//! Client-defined bar-label truncation for materialization progress.
//!
//! Materialization bars carry identity: the media id they are materializing
//! for, the variant they write, and the basename of the file or folder the
//! row names. The [`MaterializationBarLabel`] struct owns its field names and
//! truncation order independently of the conductor's `StepBarLabel` and
//! `WorkerBarLabel`, which live in `mediapm-conductor` and are not visible
//! to rustdoc from this crate.
//!
//! The label renders no phase tag. The `A/B` fraction, the outcome colour
//! and the status marker already say what a row is doing and how it ended;
//! a bracketed phase tag cost three columns each to repeat it. The screen
//! has no phase model: a row's total is the number of files it writes and
//! its label carries only identity.

use mediapm_utils::progress::{
    BarLabelTruncation, Brackets, Segment, SuffixComponents, fit_segments,
};

/// Truncation order for a materialization bar.
///
/// Segments are ordered most important first and yield from the tail. The
/// prefix is `status_marker`, `media_id`, `variant`, `entry_name`, with the
/// two identity segments bracketed and elastic and the basename elastic.
/// The suffix is `elapsed`, `rate`, `eta`, then an elastic `custom`, in the
/// same order `WorkerBarLabel` uses.
///
/// The order is the whole mechanism. `fit_segments` drops from the tail
/// unconditionally, so a field survives by its position in the list, and it
/// shrinks elastic fields from the tail too, so a field listed later is
/// the one that gives up columns first. A narrowing terminal therefore
/// spends the basename, then the variant, then the media id, and the row
/// keeps the identity that separates one entry from another at every width
/// that holds any of it.
///
/// The brackets on the identity segments are decoration, not text: the
/// clipper strips the pair from any segment it shortens, so a squeezed
/// media id renders bare (`media-001`) rather than as half a pair
/// (`a-001]`). A bracket pair whose opener was cut away reads as a typo on
/// the row, which is why decoration lives on [`Segment`] instead of inside
/// the field's text.
///
/// The status marker leads because a `[W]` or `[F]` clipped away would
/// leave a failed row reading like a succeeded one. Tool-sync leads its
/// marker for the same reason.
///
/// The basename is one elastic segment and is cut from the left, so its
/// informative end — the extension, the tail of a long title — is what
/// survives. An embedded bracket group inside a projected filename
/// (`... [youtube.dQw4w9WgXcQ].link.mkv`) is filename text and is cut blind
/// like any other text: the identity that must survive a narrow row is the
/// media-id segment in front of it, not a second copy embedded in the name.
///
/// The prefix carries no path directory, no phase tag, no count/total, and
/// no workflow identity, and neither does the suffix: the bar's position
/// out of total is not a number this label can put a name to.
#[derive(Debug, Clone, Default)]
pub struct MaterializationBarLabel {
    /// Terminal-state marker, rendered bracketed as `[F]` or `[W]`. Empty
    /// when the entry has no status, in which case the segment is omitted.
    pub status_marker: String,
    /// Identity the row is materializing for, rendered bracketed, e.g.
    /// `[youtube.dQw4w9WgXcQ]`. Elastic, so an id longer than its column
    /// budget shortens from the front and keeps the tail that discriminates
    /// it from its siblings. A playlist entry carries no media id, so its
    /// row passes the hierarchy id here (empty omits the segment).
    pub media_id: String,
    /// Variant this row writes, rendered bracketed, e.g. `[remaster]`.
    /// Elastic like [`Self::media_id`]. Empty while the variant is not yet
    /// resolved, on a folder row that covers every variant of the entry,
    /// and on the overall bar.
    pub variant: String,
    /// Basename of the hierarchy entry, e.g. `"song.mkv"`. Elastic and cut
    /// from the left, so the suffix carrying the extension survives a cut
    /// that the stem cannot.
    pub entry_name: String,
}

impl MaterializationBarLabel {
    /// Build the prefix segments, most important first.
    ///
    /// `status_marker` leads, so width pressure sheds everything else before
    /// it sheds the field that says whether the row went wrong. The two
    /// identity segments rank next and truncate independently of each other
    /// and of the basename: each is its own bracketed elastic segment, so a
    /// narrow row can shorten the variant while the media id stays whole.
    /// `entry_name` ranks last and yields first, because a row that lost its
    /// ids has lost the entry and a row that lost its basename still names
    /// which entry it is.
    fn prefix_segments(&self) -> Vec<Segment> {
        let mut segs = Vec::new();
        if !self.status_marker.is_empty() {
            segs.push(Segment::keep(self.status_marker.clone()).brackets(Brackets::Square));
        }
        if !self.media_id.is_empty() {
            segs.push(Segment::elastic(self.media_id.clone()).brackets(Brackets::Square));
        }
        if !self.variant.is_empty() {
            segs.push(Segment::elastic(self.variant.clone()).brackets(Brackets::Square));
        }
        if !self.entry_name.is_empty() {
            segs.push(Segment::elastic(self.entry_name.clone()));
        }
        segs
    }

    /// Build the suffix segments, most important first.
    ///
    /// Takes no `&self`: a materialization row has no suffix field of its own
    /// to contribute.
    ///
    /// The renderer passes its merged `SuffixComponents` in, and
    /// `elapsed`, `rate`, and `eta` arrive through it, so reading them here
    /// is the only way a materialization row shows how long it has been
    /// running. They are `Segment::keep`, so a narrow suffix drops them whole
    /// rather than clipping `0m12s` down to `2s`. Only free-form `custom`
    /// text is elastic.
    ///
    /// The count and the total are not rendered: this label has no tally of
    /// its own to name the numbers.
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
/// The first element is everything before the last `/` and the second is the
/// basename, which is what [`MaterializationBarLabel::entry_name`] holds. A
/// path with no `/` yields `("", path)`, so a caller can pass the result
/// straight into the label without special-casing a bare filename.
///
/// This is the split the materializer applies in `prepare_hierarchy_entry`
/// when it opens a bar, so an entry path outside the library, such as the
/// `mediapm_progress_materialize` example, renders the same label the
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

    /// A row with all three identity fields renders them in truncation-
    /// priority order: media id, variant, basename.
    ///
    /// This is the shape every per-`(entry, variant)` row draws on a terminal
    /// wide enough for all of it, and the order is the whole mechanism:
    /// [`fit_segments`] drops from the tail, so the basename is the first
    /// field a narrowing terminal takes and the media id is the last.
    #[test]
    fn three_identity_segments_render_in_order() {
        let label = MaterializationBarLabel {
            media_id: "media-001".into(),
            variant: "1080p".into(),
            entry_name: "Movie (2024).mkv".into(),
            ..Default::default()
        };
        assert_eq!(label.truncate_prefix(80), "[media-001] [1080p] Movie (2024).mkv");
    }

    /// The basename elides from the left before either id gives anything up,
    /// and what it keeps is its tail: the extension-bearing end.
    ///
    /// `...e (2024).mkv` beats `Movie (2024)...`: a name cut from the front
    /// still says which file the row is writing, while a name cut from the
    /// back loses exactly the suffix that names the format.
    #[test]
    fn the_basename_elides_from_the_left_and_keeps_its_extension() {
        let label = MaterializationBarLabel {
            media_id: "media-001".into(),
            variant: "1080p".into(),
            entry_name: "Movie (2024).mkv".into(),
            ..Default::default()
        };
        assert_eq!(label.truncate_prefix(32), "[media-001] [1080p] e (2024).mkv");
        assert_eq!(label.truncate_prefix(24), "[media-001] [1080p] .mkv");
    }

    /// Under sustained width pressure the fields yield in reverse rank: the
    /// basename first, then the variant, then the media id, and the media id
    /// gives its brackets up before its characters.
    ///
    /// A bracket pair is decoration on the segment, so a shortened segment
    /// renders bare rather than wearing half a pair: `media-001` at ten
    /// columns is the id with its decoration gone, never `a-001]`.
    #[test]
    fn identity_segments_yield_basename_then_variant_then_media_id() {
        let label = MaterializationBarLabel {
            media_id: "media-001".into(),
            variant: "1080p".into(),
            entry_name: "Movie (2024).mkv".into(),
            ..Default::default()
        };
        assert_eq!(label.truncate_prefix(18), "[media-001] 1080p");
        assert_eq!(label.truncate_prefix(12), "[media-001]");
        assert_eq!(label.truncate_prefix(10), "media-001");
    }

    /// The status marker survives every width that can hold it, and no width
    /// overflows. Detects the marker being demoted behind an identity field:
    /// such a reorder spends the columns on identity first and loses the one
    /// field that says the row went wrong.
    #[test]
    fn the_status_marker_survives_across_narrow_widths() {
        /// Visible width of the leading `[F]` marker.
        const MARKER_WIDTH: usize = 3;
        let label = MaterializationBarLabel {
            status_marker: "F".into(),
            media_id: "media-001".into(),
            variant: "1080p".into(),
            entry_name: "song.mkv".into(),
        };
        for width in MARKER_WIDTH..=18 {
            let out = label.truncate_prefix(width);
            assert!(out.contains("[F]"), "status lost at width {width}: {out:?}");
            assert!(out.chars().count() <= width, "overflowed at width {width}: {out:?}");
        }
    }

    /// No width leaves half a bracket pair on the row.
    ///
    /// The identity segments carry their brackets as decoration, and the
    /// clipper strips the pair from any segment it shortens, so a sweep over
    /// the whole width range sees balanced brackets or none. The basename in
    /// this sweep holds no bracket group of its own; an embedded group is
    /// plain filename text and is cut blind, like any other text.
    #[test]
    fn no_width_leaves_half_a_bracket_pair_on_an_identity_segment() {
        let label = MaterializationBarLabel {
            status_marker: "W".into(),
            media_id: "media-001".into(),
            variant: "1080p".into(),
            entry_name: "Movie (2024).mkv".into(),
        };
        for width in 3..=44 {
            let out = label.truncate_prefix(width);
            assert!(
                out.contains(']') == out.contains('['),
                "half a bracket pair at width {width}: {out:?}"
            );
        }
    }

    /// A basename wider than the whole budget still names itself by its tail.
    ///
    /// The name is the online demo's 45-column folder basename, ordinary for
    /// a YouTube-style title. A name kept whole is a name the fitter drops
    /// once nothing else can shrink, and the row would render as bare ids
    /// with no file on it. The tail it keeps can begin inside the name's own
    /// embedded bracket group, because the group is filename text now and the
    /// identity that must survive is the media-id segment in front of it.
    #[test]
    fn a_basename_wider_than_the_budget_shortens_rather_than_dropping() {
        let label = MaterializationBarLabel {
            media_id: "youtube.dQw4w9WgXcQ".into(),
            entry_name: "Rick Astley - Never Gonna Give You Up [youtube.dQw4w9WgXcQ]".into(),
            ..Default::default()
        };
        let out = label.truncate_prefix(40);
        assert!(
            out.starts_with("[youtube.dQw4w9WgXcQ] "),
            "the identifying media id was not kept whole: {out:?}"
        );
        assert!(
            out.ends_with("dQw4w9WgXcQ]"),
            "the basename lost the tail that names the video: {out:?}"
        );
        assert!(out.chars().count() <= 40, "overflowed: {out:?}");
    }

    /// A truncated `entry_name` always ends in the file's extension.
    ///
    /// Regression pin for the demo naming fix: the materialized file basename
    /// is a long single-source title (`${artist} - ${title} [${media-id}].mkv`), and a
    /// narrowing terminal must still show its `.mkv` tail so the row names the format it
    /// writes. The fixture below is upstream's own provider title, the longest spelling the
    /// family takes; a truncated name is cut the same way whatever produced its stem. This is
    /// the end-shown truncation the materialization screen depends on.
    #[test]
    fn truncated_entry_name_keeps_its_extension() {
        let label = MaterializationBarLabel {
            media_id: "youtube.dQw4w9WgXcQ".into(),
            variant: "video".into(),
            entry_name: "Rick Astley - Never Gonna Give You Up (Official Video) (4K Remaster) [youtube.dQw4w9WgXcQ].mkv".into(),
            ..Default::default()
        };
        let out = label.truncate_prefix(44);
        assert!(
            std::path::Path::new(&out).extension() == Some(std::ffi::OsStr::new("mkv")),
            "truncated entry_name lost its extension tail: {out:?}",
        );
    }

    /// A label with only a basename renders it alone, which is what the
    /// overall `materializing` bar shows: it names the screen and carries no
    /// identity of its own.
    #[test]
    fn a_label_with_only_a_basename_renders_it_alone() {
        let label =
            MaterializationBarLabel { entry_name: "materializing".into(), ..Default::default() };
        assert_eq!(label.truncate_prefix(80), "materializing");
    }

    /// No width of a fully-populated row names a phase.
    ///
    /// Phase tags were the screen's legibility defect: `[stg]` and `[cmt]`
    /// said what the bar was doing while the `A/B` fraction and the outcome
    /// colour already said everything the row needs. The label carries no
    /// phase field, so the tags cannot come back through it without a field
    /// coming back with them, and this sweep is what notices if one does.
    #[test]
    fn no_rendered_row_names_a_phase_tag() {
        let label = MaterializationBarLabel {
            status_marker: "W".into(),
            media_id: "media-001".into(),
            variant: "1080p".into(),
            entry_name: "song.mkv".into(),
        };
        for width in 3..=44 {
            let out = label.truncate_prefix(width);
            for tag in ["stg", "vrf", "cmt", "wrt"] {
                assert!(!out.contains(tag), "phase tag {tag:?} at width {width}: {out:?}");
            }
        }
    }

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

    /// A materialization row shows the timing the renderer derives for it.
    ///
    /// `elapsed`, `rate`, and `eta` reach the label through the merged
    /// `SuffixComponents` the renderer passes in, so dropping them from
    /// `truncate_suffix` strips the timing off every row on the screen.
    #[test]
    fn suffix_renders_the_auto_derived_timing() {
        let label = MaterializationBarLabel { entry_name: "song.mkv".into(), ..Default::default() };
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
}
