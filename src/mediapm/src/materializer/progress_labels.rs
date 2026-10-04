//! Client-defined bar-label truncation for materialization progress.
//!
//! Materialization bars carry file-path identity and phase tags. The
//! [`MaterializationBarLabel`] struct owns its field names and truncation
//! order independently of the conductor's `StepBarLabel` and
//! `WorkerBarLabel`, which live in `mediapm-conductor` and are not visible
//! to rustdoc from this crate.

use crate::config::hierarchy_types::HierarchyEntryKind;

use mediapm_utils::progress::{
    BarLabelTruncation, Brackets, Join, Segment, SuffixComponents, fit_segments,
};

use self::MaterializationPhase::{Commit, Staging, Verify, Write};

/// One phase of the materialization screen.
///
/// The set is closed, so a bar cannot name a phase this screen does not have
/// and a typo cannot reach a rendered row. The tag is fixed per variant here
/// rather than spelled at each call site, which is what let the folder and
/// playlist arms drift: each named its finish tag with its own literal.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum MaterializationPhase {
    /// Staging CAS content into the entry's target directory.
    Staging,
    /// Checking staged bytes against the expected hash before they are
    /// committed into the library.
    Verify,
    /// Writing the entry into the library.
    Commit,
    /// Writing one extracted member of a ZIP folder variant, on the sub-bar
    /// under a folder row rather than on the folder row itself.
    Write,
}

impl MaterializationPhase {
    /// Text inside the brackets, as [`MaterializationBarLabel`] renders it.
    #[must_use]
    pub const fn tag(self) -> &'static str {
        match self {
            Self::Staging => "stg",
            Self::Verify => "vrf",
            Self::Commit => "cmt",
            Self::Write => "wrt",
        }
    }
}

impl HierarchyEntryKind {
    /// Phases a bar for this kind installs, in the order its arm enters them.
    ///
    /// Declared here, next to the tag that renders it, rather than inside each
    /// arm of the materializer's entry match, because that is where the two
    /// agreed by accident: the folder and playlist arms never installed a
    /// second phase, so their rows kept `[stg]` for their whole run and the
    /// failure path repeated the same literal. The match is exhaustive over the
    /// enum, so a kind cannot exist without saying what it walks.
    ///
    /// A kind lists only phases its arm really runs. A folder makes its target
    /// directory and then writes each selected variant, so it walks staging
    /// and write. A playlist resolves its references, builds the bytes and
    /// writes one file, so it walks staging and commit. Neither verifies, so
    /// neither lists [`Verify`]: it is the one phase only [`Self::Media`]
    /// claims, which is why a reader counting four tags against three kinds
    /// is not looking at a dead variant.
    ///
    /// A folder's write phase is also the tag its sub-bars carry, so during a
    /// folder sync the parent row and the rows under it name the same work at
    /// two granularities. That is the point of declaring it here rather than
    /// on the sub-bar alone.
    #[must_use]
    pub fn phases(self) -> &'static [MaterializationPhase] {
        match self {
            Self::Media => &[Staging, Verify, Commit],
            Self::MediaFolder => &[Staging, Write],
            Self::Playlist => &[Staging, Commit],
        }
    }

    /// The phase a bar for this kind is created on, which is `phases()[0]`.
    ///
    /// Stated as its own match rather than read off [`Self::phases`] because a
    /// const item cannot index a slice. `phases_agree_with_first_phase` holds
    /// the two to each other.
    #[must_use]
    pub(crate) const fn first_phase(self) -> MaterializationPhase {
        match self {
            Self::Media | Self::MediaFolder | Self::Playlist => Staging,
        }
    }
}

/// Truncation order for a materialization bar.
///
/// Segments are ordered most important first and yield from the tail. The
/// prefix is `status_marker`, `entry_name` stem, `entry_id`, the tail the id
/// leaves behind, `entry_path`, `file_name`, `phase`, with the two path
/// halves elastic so they shorten from the front.
/// The suffix is `elapsed`, `rate`, `eta`, then an elastic `custom`, in the
/// same order `WorkerBarLabel` uses.
///
/// The order is the whole mechanism. `fit_segments` drops from the tail
/// unconditionally, so a field survives by its position in the list, and it
/// shrinks elastic fields from the tail too, so a field listed later is the
/// one that gives up columns first.
///
/// The phase tag sits at the tail so that a row reads as a file carrying a
/// phase, the way tool-sync renders `ffmpeg v7.1 [res]`, rather than as a
/// phase with a file under it. Tail position also makes the tag the first
/// thing a narrowing terminal takes, and that is what tool-sync does with its
/// own tag, so the screens agree on what yields first.
///
/// The status marker leads because a `[W]` or `[F]` clipped away would leave
/// a failed row reading like a succeeded one. Tool-sync leads its marker for
/// the same reason.
///
/// A reader scanning this screen wants to know, per row, what is being
/// written and whether it worked. The path halves say which file, the
/// status marker says it failed or was skipped, and the phase tag says what
/// the bar is doing.
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
    /// Basename of hierarchy entry, e.g. `"song.mkv"`. A square-bracket
    /// group anywhere in it is taken out into the label's own segment by
    /// `split_entry_id`, along with anything the group leaves behind, so
    /// the elastic name carries no brackets for a clip to cut in half.
    pub entry_name: String,
    /// Extracted file basename (sub-bars only), e.g. `"cover.jpg"`.
    pub file_name: String,
    /// Phase the row is in. A per-entry row names it; the overall bar leaves
    /// it `None`, because its label already says it is materializing.
    pub phase: Option<MaterializationPhase>,
}

impl MaterializationBarLabel {
    /// Build the prefix segments, most important first.
    ///
    /// `status_marker` leads, so width pressure sheds everything else before
    /// it sheds the field that says whether the row went wrong. `phase` is
    /// last, so it is the first whole segment a narrow row gives up; the
    /// reason the tag sits at the tail is on [`MaterializationBarLabel`].
    ///
    /// `entry_name` and `entry_path` are both elastic, and they yield in that
    /// order because `fit_segments` shrinks from the tail: the directory
    /// gives up its leading elements first, and only then does the basename
    /// yield anything of its own. Both are cut between characters, so a
    /// directory at 16 columns can read as the last few columns of an
    /// element, and the leading half of a path is the artist's name, which
    /// the row is not about.
    ///
    /// The id [`split_entry_id`] lifts out of the name ranks above the path:
    /// it is short, whole, and names the entry exactly where the path names
    /// only a directory in it. It is `Keep`, so width pressure spends the
    /// elastic halves before giving it up.
    ///
    /// The tail the group leaves behind is a `Keep` segment joined directly
    /// to the id, because it is the rest of the same filename rather than a
    /// field of its own: a media projection is `...[id].link.mkv`, and a row
    /// reading `...[id] .link.mkv` would be showing a space the name does
    /// not have. It ranks last of the three, so a narrow row gives up the
    /// extension before the id and before any of the name.
    ///
    /// `entry_name` is elastic rather than kept whole because a kept name is a
    /// name that vanishes: at 39 columns the online demo's 59-column folder
    /// name has a tail that carries the words in front of the media id, which
    /// is what separates one video from another in a list of them, and that
    /// tail only exists because the elastic name is cut rather than
    /// surrendered.
    ///
    /// `file_name` only appears on a `[wrt]` sub-bar, where it repeats the
    /// variant name the parent row already implies, and there are as many of
    /// those rows as there are extracted members. It outranks `phase`
    /// because a member name still says which member a row is writing once
    /// the tag is gone, while the directory beside it says nothing the parent
    /// row does not. The directory outranks both, since no other row carries
    /// it.
    fn prefix_segments(&self) -> Vec<Segment> {
        let mut segs = Vec::new();
        if !self.status_marker.is_empty() {
            segs.push(Segment::keep(self.status_marker.clone()).brackets(Brackets::Square));
        }
        let (stem, entry_id, entry_tail) = split_entry_id(&self.entry_name);
        if !stem.is_empty() {
            segs.push(Segment::elastic(stem.to_string()));
        }
        if !entry_id.is_empty() {
            segs.push(Segment::keep(entry_id.to_string()).brackets(Brackets::Square));
        }
        if !entry_tail.is_empty() {
            segs.push(Segment::keep(entry_tail.to_string()).joined(Join::Direct));
        }
        if !self.entry_path.is_empty() {
            segs.push(Segment::elastic(self.entry_path.clone()));
        }
        if !self.file_name.is_empty() {
            segs.push(Segment::keep(self.file_name.clone()));
        }
        if let Some(phase) = self.phase {
            segs.push(Segment::keep(phase.tag()).brackets(Brackets::Square));
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
    /// rather than clipping `0m12s` down to `2s`. Only free-form `custom`
    /// text is elastic.
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
/// This is the split the materializer applies in `prepare_hierarchy_entry`
/// when it builds a bar label, so an entry path outside the library, such as
/// the `mediapm_progress_materialize` example, can render the same label the
/// library renders instead of splitting the path a second way.
#[must_use]
pub fn split_entry_path(path: &str) -> (&str, &str) {
    match path.rfind('/') {
        Some(pos) => (&path[..pos], &path[pos + 1..]),
        None => ("", path),
    }
}

/// Split a hierarchy basename into `(stem, bracket_id, tail)`.
///
/// The `rename_files` projection puts the media id in square brackets, e.g.
/// `Rick Astley - Never Gonna Give You Up [youtube.dQw4w9WgXcQ]`. That
/// group is the id, it is what separates one video from another in a list of
/// them, and it is the piece a narrow row cannot afford to half-show.
/// [`MaterializationBarLabel`] renders it as a segment of its own, so the
/// name beside it carries no brackets for a clip to cut.
///
/// The group is not always last. A media file's projected name continues past
/// it into its extension, `...[youtube.dQw4w9WgXcQ].link.mkv`, so the tail
/// comes back as a third piece and the label joins it to the id directly.
/// Leaving it inside the elastic name is what let a clip leave a bare `]` on
/// the row: the name held both a bracket group and an extension, and only the
/// extension survived a narrow cut.
///
/// The tail is returned as it stands, whitespace and all, because the join
/// adds nothing in front of it. A name whose group is followed by a space
/// keeps that space.
///
/// A basename with no bracketed group is returned whole with two empty
/// pieces, so a caller can pass the result straight into the label.
#[must_use]
pub fn split_entry_id(name: &str) -> (&str, &str, &str) {
    let Some(open) = name.rfind('[') else {
        return (name, "", "");
    };
    let Some(close) = name[open..].find(']') else {
        return (name, "", "");
    };
    (name[..open].trim_end(), &name[open + 1..open + close], &name[open + close + 1..])
}

#[cfg(test)]
mod tests {
    use super::*;

    /// The two declarations of a kind's first phase cannot drift apart: the
    /// bar a kind creates itself opens on `first_phase`, and the list an arm
    /// walks starts at `phases()[0]`. A kind added to one and not the other
    /// would create a row on a phase its arm never enters.
    #[test]
    fn phases_agree_with_first_phase() {
        for kind in [
            HierarchyEntryKind::Media,
            HierarchyEntryKind::MediaFolder,
            HierarchyEntryKind::Playlist,
        ] {
            assert_eq!(
                kind.phases().first(),
                Some(&kind.first_phase()),
                "{kind:?} declares a first phase its own list does not start with",
            );
            assert!(
                !kind.phases().is_empty(),
                "{kind:?} declares no phase, so its row would have no tag to open on",
            );
        }
    }

    /// The tag is the only thing that differs between phases, so a mistyped
    /// variant cannot render as free text.
    #[test]
    fn phase_tags_are_the_documented_three_letters() {
        assert_eq!(MaterializationPhase::Staging.tag(), "stg");
        assert_eq!(MaterializationPhase::Verify.tag(), "vrf");
        assert_eq!(MaterializationPhase::Commit.tag(), "cmt");
        assert_eq!(MaterializationPhase::Write.tag(), "wrt");
    }

    /// A label with no phase names the screen and nothing more, which is what
    /// the overall bar shows.
    #[test]
    fn a_label_without_a_phase_renders_no_tag() {
        let label =
            MaterializationBarLabel { entry_name: "materializing".into(), ..Default::default() };
        assert_eq!(label.truncate_prefix(80), "materializing");
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

    /// The split is three ways whatever the group sits at, since a media
    /// projection continues into its extension after the group.
    #[test]
    fn split_entry_id_separates_the_group_from_the_tail_behind_it() {
        assert_eq!(
            split_entry_id("Rick Astley - Never Gonna Give You Up [youtube.dQw4w9WgXcQ]"),
            ("Rick Astley - Never Gonna Give You Up", "youtube.dQw4w9WgXcQ", ""),
        );
        assert_eq!(
            split_entry_id("01 - Telepathy.flac [youtube.dQw4w9WgXcQ].link.mkv"),
            ("01 - Telepathy.flac", "youtube.dQw4w9WgXcQ", ".link.mkv"),
        );
        assert_eq!(split_entry_id("song.mkv"), ("song.mkv", "", ""));
        assert_eq!(
            split_entry_id("album [unclosed.mkv"),
            ("album [unclosed.mkv", "", ""),
            "a group that never closes is part of the name, not an id to render",
        );
    }

    /// A projected link name renders as the one filename it is, and no width
    /// cuts a bracket out of it. The group and the extension used to sit
    /// inside the elastic name together, so a row too narrow for the whole
    /// name kept the extension and lost the half of the group that opened it.
    #[test]
    fn a_projected_link_name_keeps_its_brackets_and_loses_no_space() {
        let label = MaterializationBarLabel {
            entry_path: "Music/Artist".into(),
            entry_name: "01 - Telepathy.flac [youtube.dQw4w9WgXcQ].link.mkv".into(),
            phase: Some(MaterializationPhase::Commit),
            ..Default::default()
        };
        assert_eq!(
            label.truncate_prefix(80),
            "01 - Telepathy.flac [youtube.dQw4w9WgXcQ].link.mkv Music/Artist [cmt]",
        );
        for width in 8..=60 {
            let out = label.truncate_prefix(width);
            assert!(
                out.contains(']') == out.contains('['),
                "half a bracket pair at width {width}: {out:?}"
            );
        }
    }

    /// The status marker survives every width that can hold it, and no width
    /// overflows. Detects `entry_name` or `entry_path` being promoted ahead
    /// of `status_marker`: such a reordering spends the columns on the path
    /// first and loses the one field that says the row failed.
    #[test]
    fn the_status_marker_survives_across_narrow_widths() {
        /// Visible width of the leading `[F]` marker.
        const MARKER_WIDTH: usize = 3;
        let label = MaterializationBarLabel {
            status_marker: "F".into(),
            entry_path: "Music/Artist/Album".into(),
            entry_name: "song.mkv".into(),
            file_name: "cover.jpg".into(),
            phase: Some(MaterializationPhase::Write),
        };
        for width in MARKER_WIDTH..=18 {
            let out = label.truncate_prefix(width);
            assert!(out.contains("[F]"), "status lost at width {width}: {out:?}");
            assert!(out.chars().count() <= width, "overflowed at width {width}: {out:?}");
        }
    }

    /// The elastic path is clipped from the front so the directory tail,
    /// the part adjacent to the filename, survives. It used to be dropped
    /// whole, because it ranked last.
    #[test]
    fn entry_path_is_shortened_from_the_front() {
        let label = MaterializationBarLabel {
            entry_path: "Music/Artist/Album/1977".into(),
            entry_name: "song.mkv".into(),
            phase: Some(MaterializationPhase::Staging),
            ..Default::default()
        };
        // Wide enough for the full path, so nothing is shortened.
        let wide = label.truncate_prefix(80);
        assert!(wide.contains("Music/Artist/Album/1977"), "wide path missing: {wide:?}");

        // Tight: the path must keep its last directory, never its head.
        let tight = label.truncate_prefix(20);
        assert_eq!(tight, "song.mkv /1977 [stg]");
        assert!(!tight.contains("Music/"), "path head retained instead of tail: {tight:?}");
        assert!(tight.contains("song.mkv"), "entry_name lost: {tight:?}");
    }

    /// The path yields its columns before the name yields any of its own.
    /// When the path can yield nothing at all, it is dropped and the name
    /// takes the overage instead, cut between characters.
    #[test]
    fn entry_name_shortens_from_the_front_and_the_path_yields_first() {
        let label = MaterializationBarLabel {
            entry_path: "very/long/path/segments".into(),
            entry_name: "important-file.mkv".into(),
            phase: Some(MaterializationPhase::Commit),
            ..Default::default()
        };
        // The path gives up everything above its last element and the name is
        // untouched.
        assert_eq!(label.truncate_prefix(40), "important-file.mkv g/path/segments [cmt]");
        // Too narrow for the path at any answer it can give, so it is dropped
        // and the name takes the overage instead.
        assert_eq!(label.truncate_prefix(23), "mportant-file.mkv [cmt]");
    }

    /// A folder name wider than the budget still names itself.
    ///
    /// The name is the online demo's, and 59 columns is ordinary for a
    /// YouTube-style title, not exotic. `entry_name` used to be a
    /// `Segment::keep` ranked above the elastic `entry_path`, so once the
    /// path had shrunk as far as it could, `fit_segments` dropped the name
    /// whole and the row rendered as a bare `[stg]`. See
    /// `MaterializationBarLabel` for the order that fixes it.
    ///
    /// What the name keeps at this width is the bracketed media id and the
    /// words in front of it.
    ///
    /// The id is a segment of its own, so it is rendered whole or dropped
    /// whole at every width: no clip of the name beside it can leave half a
    /// bracket pair on the row, which is the whole reason the label splits it
    /// out of `entry_name`.
    #[test]
    fn an_over_long_folder_name_keeps_the_tail_of_its_name() {
        let label = MaterializationBarLabel {
            entry_path: "music videos".into(),
            entry_name: "Rick Astley - Never Gonna Give You Up [youtube.dQw4w9WgXcQ]".into(),
            phase: Some(MaterializationPhase::Staging),
            ..Default::default()
        };
        let out = label.truncate_prefix(39);
        assert!(out.contains("[stg]"), "phase lost: {out:?}");
        assert!(out.contains("dQw4w9WgXcQ"), "the media id identifying the name was cut: {out:?}");
        assert_eq!(
            out, "Give You Up [youtube.dQw4w9WgXcQ] [stg]",
            "name not clipped to its tail: {out:?}"
        );
        assert!(out.chars().count() <= 39, "overflowed: {out:?}");

        for width in 3..=39 {
            let out = label.truncate_prefix(width);
            assert!(
                !out.contains(']') || out.contains("[youtube.dQw4w9WgXcQ]"),
                "stray closing bracket at width {width}: {out:?}"
            );
        }
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
            phase: Some(MaterializationPhase::Commit),
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

    /// The phase tag is the first thing a sub-bar row gives up, and the
    /// variant name goes with it once nothing else can shrink.
    ///
    /// The pair of widths below straddles that point, so a reordering that
    /// moved the phase tag off the tail would fail the wide one: with the tag
    /// ahead of the variant name it would be the segment left standing.
    #[test]
    fn the_phase_tag_yields_first_and_the_file_name_after_it() {
        let label = MaterializationBarLabel {
            entry_path: "Music/Rick Astley/Never Gonna Give You Up".into(),
            entry_name: "album".into(),
            file_name: "cover.jpg".into(),
            phase: Some(MaterializationPhase::Write),
            ..Default::default()
        };
        assert_eq!(label.truncate_prefix(15), "cover.jpg [wrt]");
        assert_eq!(label.truncate_prefix(14), "cover.jpg");
    }
}
