//! Client-defined bar-label truncation contract.
//!
//! mediapm-utils renders progress bars but must NOT bake in a fixed field
//! layout (e.g. a "prefix version" component). Instead it exposes the
//! minimal [`BarLabelTruncation`] trait and only *calls* it during render.
//! The client crate (mediapm-conductor) owns the field structs, the
//! truncation order, and the final display-string assembly.
//!
//! When a bar has no truncation set ([`None`](Option::None)), mediapm-utils
//! falls back to its built-in component rendering so existing callers keep
//! working unchanged.

use crate::progress::SuffixComponents;

/// Client-supplied truncation for a tracked bar's prefix and suffix.
///
/// Implementors receive the maximum visible width budget and return the
/// final display string (already colored/escaped as the client sees fit).
/// mediapm-utils never inspects the field layout — it only invokes these
/// two methods at the single render push point.
///
/// `truncate_suffix` (`Self::truncate_suffix`) receives the merged
/// [`SuffixComponents`] so the client can render auto-derived fields
/// (count/total, elapsed, rate, eta) alongside its own fields.
///
/// # Width budgets
///
/// The two methods receive deliberately different budgets:
///
/// - [`truncate_prefix`](Self::truncate_prefix) receives the prefix slot
///   **less** 4 bytes of ANSI overhead, because the renderer prepends the
///   `\x1b[0m` reset that the returned string has to hold.
/// - [`truncate_suffix`](Self::truncate_suffix) receives the **full** suffix
///   slot. The renderer prepends no reset to the suffix, so nothing is
///   subtracted from it.
///
/// Fill each method to the budget it is actually given. An implementation
/// that assumes the suffix arrives pre-reduced under-fills it by four columns.
#[cfg(feature = "progress")]
pub trait BarLabelTruncation: Send + Sync {
    /// Return the rendered prefix string fitting within `max_width` visible
    /// columns.
    fn truncate_prefix(&self, max_width: usize) -> String;
    /// Return the rendered suffix string fitting within `max_width` visible
    /// columns.  The `suffix` parameter carries the full merged suffix
    /// component set (auto-derived count/total/elapsed/rate/eta plus the
    /// user-set custom text) so the client can render auto-fields
    /// alongside its own fields.
    fn truncate_suffix(&self, max_width: usize, suffix: &SuffixComponents) -> String;
}

/// How a segment yields width when the bar line does not fit.
///
/// A segment is either shortened (elastic) or surrendered whole (keep), and
/// an elastic segment gives columns back from one end or the other. Every
/// segment is eventually droppable; the distinction only governs *how* width
/// is reclaimed first.
#[cfg(feature = "progress")]
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Shrink {
    /// Never shortened. The segment is dropped whole when it cannot fit.
    ///
    /// Right for a fixed marker, whose brackets are the segment's own
    /// [`Brackets`] decoration rather than part of its text: a marker whose
    /// decoration is gone is loose text that no longer says what the piece
    /// is, so a `Keep` marker is kept whole or dropped whole.
    Keep,
    /// Shortened from the front, keeping the informative tail, so
    /// `Music/Artist/Album/song.mkv` clipped to 15 columns reads
    /// `st/Album/song.mkv`.
    ///
    /// Use for values with a meaningful end: paths, tool names, and free
    /// user text. The clip is blind, so a tail can begin inside a word and
    /// read like `he Wall`. That is the deliberate trade: the cut lands
    /// between characters, so what survives is the widest tail that fits
    /// rather than the widest one that happens to end on a boundary, which
    /// is what makes the rendered length monotone in the width it is given.
    /// A value that must never be cut mid-word belongs in
    /// [`Keep`](Self::Keep), which drops it instead of cutting it.
    Front,
    /// Shortened from the end, keeping the leading characters, so `7.1` at
    /// two columns reads `7.`.
    ///
    /// Use for values that number or name themselves from the left, where
    /// the leading columns carry the identity. The clip is blind here too, so
    /// this is [`Front`](Self::Front) taken from the other end: the two modes
    /// differ only in which end of the value survives.
    ///
    /// One production label builds this mode, `StepBarLabel::version`, and
    /// nothing on the production path fills that field: the conductor has no
    /// versioned tool to read, so a live workflow bar renders no head-keeping
    /// segment and this mode never runs there. The workflow example supplies
    /// a literal version and is the only caller that renders it.
    Head,
}

/// The brackets a segment carries around its content.
///
/// Decoration belongs here rather than inside [`Segment::text`] because the
/// clipper cuts the text. A caller that formats its own brackets hands the
/// brackets to the clipper with the content, so a one-column clip of
/// `(ffmpeg)` leaves `)` on the row: a bracket with nothing opening it.
///
/// [`fit_segments`] renders a segment's brackets only when it rendered the
/// segment whole, so decoration is all or nothing. A segment that had to be
/// shortened reads as its bare content, which is a name a reader can still
/// use, rather than as a fragment wearing the punctuation of something else.
#[cfg(feature = "progress")]
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Brackets {
    /// Round brackets, so content `ffmpeg` renders as `(ffmpeg)`.
    Round,
    /// Square brackets, so content `active` renders as `[active]`.
    Square,
}

/// What a segment puts between itself and the segment before it.
///
/// [`fit_segments`] joins with [`Space`](Self::Space), which is what a label
/// wants between two of its own fields: `default s3 (ffmpeg)`. A filename is
/// not two fields, so a caller that splits one into segments says
/// [`Direct`](Self::Direct) and the pieces rejoin as the single string it was.
#[cfg(feature = "progress")]
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Join {
    /// A single space before this piece.
    Space,
    /// Nothing before this piece: it continues the text in front of it.
    Direct,
}

/// One ordered piece of a bar label.
///
/// Segments are supplied most important first. [`fit_segments`] walks the
/// list from the tail, so the last segment is the first to yield.
#[cfg(feature = "progress")]
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Segment {
    /// Content of this piece, without decoration and without the joining
    /// separator. This is the only part a clip touches.
    pub text: String,
    /// How this piece yields width.
    pub shrink: Shrink,
    /// Brackets to wrap the content in, applied only when the piece is
    /// rendered whole. [`None`](Option::None) renders the bare content.
    pub brackets: Option<Brackets>,
    /// What separates this piece from the one before it.
    pub join: Join,
}

#[cfg(feature = "progress")]
impl Segment {
    /// A piece that is surrendered whole rather than shortened: it is
    /// dropped when it cannot fit.
    #[must_use]
    pub fn keep(text: impl Into<String>) -> Self {
        Self { text: text.into(), shrink: Shrink::Keep, brackets: None, join: Join::Space }
    }

    /// An elastic piece: shortened from the front before anything is dropped.
    #[must_use]
    pub fn elastic(text: impl Into<String>) -> Self {
        Self { text: text.into(), shrink: Shrink::Front, brackets: None, join: Join::Space }
    }

    /// An elastic piece that keeps its leading characters: it gives columns
    /// back from the end before anything is dropped.
    #[must_use]
    pub fn elastic_head(text: impl Into<String>) -> Self {
        Self { text: text.into(), shrink: Shrink::Head, brackets: None, join: Join::Space }
    }

    /// Wrap this piece's content in `brackets`, which are rendered only when
    /// the piece survives whole.
    #[must_use]
    pub fn brackets(mut self, brackets: Brackets) -> Self {
        self.brackets = Some(brackets);
        self
    }

    /// Set what separates this piece from the one before it, for a value
    /// split across two segments that is a single string when joined.
    #[must_use]
    pub fn joined(mut self, join: Join) -> Self {
        self.join = join;
        self
    }
}

/// Shorten `text` to at most `target` visible columns, keeping the tail.
///
/// The tail is the last `target` characters and the cut lands between them,
/// with no attempt to move it left to a word boundary. A boundary-snapped
/// cut is a step function of the budget, so widening the slot by one column
/// can leave the row shorter than it was one column earlier, and a row that
/// loses text as the window widens is worse to read than a row that shows
/// half a word.
///
/// Nothing marks the cut, so the tail itself is all the reader gets:
/// `front_tail("Music/Pink Floyd/The Wall", 7)` is `he Wall`, and
/// `front_tail("Music/Has the Right to Children", 6)` is `ildren`. A value
/// whose cut tail would say nothing is a [`Segment`] with a shrink mode of
/// [`Shrink::Keep`], which is dropped whole instead.
///
/// Returns `text` unchanged when it already fits, and an empty string when
/// `target` is `0`.
#[cfg(feature = "progress")]
#[must_use]
pub fn front_tail(text: &str, target: usize) -> String {
    let len = text.chars().count();
    if len <= target {
        return text.to_string();
    }
    text.chars().skip(len - target).collect()
}

/// Shorten `text` to at most `target` visible columns, keeping the head.
///
/// The head is the first `target` characters and the cut lands between them.
/// Nothing snaps the cut to a boundary: the values that use this clip carry
/// none to snap to, and `7.` is what a version is supposed to read at two
/// columns rather than whatever piece a boundary rule would happen to pick.
///
/// Returns `text` unchanged when it already fits, and the empty string when
/// `target` is `0`.
#[cfg(feature = "progress")]
#[must_use]
fn tail_head(text: &str, target: usize) -> String {
    text.chars().take(target).collect()
}

/// A segment together with what fitting has already done to it.
///
/// `clipped` is the only state fitting adds, and it is there so
/// [`render_one`] knows whether the segment's decoration still applies. It is
/// set whenever the segment's own text is shortened, and never set for a
/// segment that was dropped, since a dropped segment renders nothing.
#[cfg(feature = "progress")]
#[derive(Debug, Clone)]
struct Fitted {
    /// The caller's segment, with `text` possibly shortened.
    segment: Segment,
    /// Whether `text` is a shortened copy of the content the caller supplied.
    clipped: bool,
}

#[cfg(feature = "progress")]
impl Fitted {
    /// Clip `text` to `target` columns in `shrink`'s own mode.
    ///
    /// `None` means the segment can give nothing back at that width: a
    /// [`Shrink::Keep`] segment is not eligible, and an elastic segment
    /// clipped to nothing has no piece left to render. The caller drops such
    /// a segment rather than leaving an empty piece behind, because
    /// [`render`] joins with single spaces and an empty piece would leave two
    /// where the text was.
    fn clip(text: &str, shrink: Shrink, target: usize) -> Option<String> {
        let clipped = match shrink {
            Shrink::Keep => return None,
            Shrink::Front => front_tail(text, target),
            Shrink::Head => tail_head(text, target),
        };
        if clipped.is_empty() { None } else { Some(clipped) }
    }
}

/// Render one fitted segment: its content, and its decoration only when
/// fitting left the content whole.
#[cfg(feature = "progress")]
fn render_one(fitted: &Fitted) -> String {
    if fitted.clipped {
        return fitted.segment.text.clone();
    }
    match fitted.segment.brackets {
        None => fitted.segment.text.clone(),
        Some(Brackets::Round) => format!("({})", fitted.segment.text),
        Some(Brackets::Square) => format!("[{}]", fitted.segment.text),
    }
}

/// Join `fitted`, decorating each piece that is whole.
///
/// A piece joined [`Direct`](Join::Direct) contributes nothing in front of
/// it, so a value the caller split across two segments comes back out as the
/// one string it was. Every other piece is preceded by a single space.
#[cfg(feature = "progress")]
fn render(fitted: &[Fitted]) -> String {
    let mut out = String::new();
    for (idx, piece) in fitted.iter().map(render_one).enumerate() {
        if idx > 0 && fitted[idx].segment.join == Join::Space {
            out.push(' ');
        }
        out.push_str(&piece);
    }
    out
}

/// Visible column count of `text`.
#[cfg(feature = "progress")]
fn visible_len(text: &str) -> usize {
    text.chars().count()
}

/// Fit `segments` to `max_width` visible columns.
///
/// `segments` is ordered most important first and is walked from the tail.
///
/// * **Phase A (shrink)**: every elastic segment, from the tail forward, is
///   shortened by the overage before the next one yields anything. The
///   overage is measured in rendered columns and answered in rendered
///   columns, so a segment's own brackets count against it; a clipped
///   segment then gives its brackets up as well, which only ever widens the
///   gap it just closed. A segment whose clip lands on nothing is dropped on
///   the spot rather than left for Phase B: it is about to give its columns
///   back, and a segment still holding them would deny the segments ahead of
///   it the overage they need to yield at all.
/// * **Phase B (drop)**: once no segment can shrink further, whole segments
///   drop from the tail until the remainder fits or one segment is left. The
///   leading segment is never popped here, so a leading head group yields
///   progressively rather than all at once: a step bar headed by
///   `[wf] [F] 1/4` still renders `[wf] [F]` at width 11, and only `[wf]` at
///   width 7.
///
/// Both phases only ever remove columns, so the rendered length is monotone
/// in `max_width`: a narrower slot never yields a longer row. That is what
/// makes a narrow terminal readable, since a row that gains text as the
/// window shrinks reads as jitter rather than as a label.
///
/// Decoration is rendered after fitting, so a segment that had to be
/// shortened loses its brackets. A row never shows a closing bracket whose
/// opening half was cut away.
///
/// # Examples
///
/// ```
/// # use mediapm_utils::progress::{Brackets, Segment, fit_segments};
/// let segs = vec![
///     Segment::keep("wf").brackets(Brackets::Square),
///     Segment::elastic("Music/Artist/Album/song.mkv"),
/// ];
/// assert_eq!(fit_segments(&segs, 40), "[wf] Music/Artist/Album/song.mkv");
/// // The cut is blind, so the tail can begin inside an element.
/// assert_eq!(fit_segments(&segs, 20), "[wf] /Album/song.mkv");
/// // At 5 columns the path has nothing left to give, so it is dropped
/// // whole rather than rendered as a fragment.
/// assert_eq!(fit_segments(&segs, 5), "[wf]");
/// ```
///
/// A head-keeping segment gives its columns back from the other end, which is
/// what a version needs: it numbers itself from the left, so a clip that kept
/// the tail could never produce `7.` at any width.
///
/// ```
/// # use mediapm_utils::progress::{Brackets, Segment, fit_segments};
/// let segs = vec![
///     Segment::keep("wf").brackets(Brackets::Square),
///     Segment::elastic_head("7.1"),
/// ];
/// assert_eq!(fit_segments(&segs, 20), "[wf] 7.1");
/// assert_eq!(fit_segments(&segs, 7), "[wf] 7.");
/// assert_eq!(fit_segments(&segs, 6), "[wf] 7");
/// ```
///
/// A value the caller split across two segments rejoins as the one string it
/// was, because the tail piece declares [`Join::Direct`] rather than taking
/// the default space:
///
/// ```
/// # use mediapm_utils::progress::{Brackets, Join, Segment, fit_segments};
/// let segs = vec![
///     Segment::elastic("01 - Telepathy.flac"),
///     Segment::keep("youtube.dQw4w9WgXcQ").brackets(Brackets::Square),
///     Segment::keep(".link.mkv").joined(Join::Direct),
/// ];
/// assert_eq!(
///     fit_segments(&segs, 50),
///     "01 - Telepathy.flac [youtube.dQw4w9WgXcQ].link.mkv"
/// );
/// // The elastic piece clips while the group and the tail beside it, neither
/// // of which can be shortened, come through whole.
/// assert_eq!(fit_segments(&segs, 40), "athy.flac [youtube.dQw4w9WgXcQ].link.mkv");
/// ```
///
/// # Degenerate widths
///
/// Below the width of a single segment the ladder yields an empty string
/// rather than a clipped one:
///
/// ```
/// # use mediapm_utils::progress::{Brackets, Segment, fit_segments};
/// let segs = vec![Segment::keep("wf").brackets(Brackets::Square)];
/// assert_eq!(fit_segments(&segs, 4), "[wf]");
/// assert_eq!(fit_segments(&segs, 3), "");
/// ```
#[cfg(feature = "progress")]
#[must_use]
pub fn fit_segments(segments: &[Segment], max_width: usize) -> String {
    if segments.is_empty() {
        return String::new();
    }

    let mut kept: Vec<Fitted> =
        segments.iter().cloned().map(|segment| Fitted { segment, clipped: false }).collect();

    // Phase A: elastic segments yield width, tail first, each from the end its
    // own mode names. `current` is the segment's rendered width, brackets
    // included, so the target it is given is a budget for the clipped text
    // the renderer will actually produce.
    let mut idx = kept.len();
    while idx > 0 {
        idx -= 1;
        let current_rendered = render_one(&kept[idx]);
        if visible_len(&render(&kept)) <= max_width {
            break;
        }
        let over = visible_len(&render(&kept)) - max_width;
        let current = visible_len(&current_rendered);
        if kept[idx].segment.shrink == Shrink::Keep {
            continue;
        }
        let target = current.saturating_sub(over);
        if target >= current {
            continue;
        }
        let Some(clipped) = Fitted::clip(&kept[idx].segment.text, kept[idx].segment.shrink, target)
        else {
            kept.remove(idx);
            continue;
        };
        // Every elastic segment gives back exactly the overage: the clip
        // takes the text down to `target`, and it gives up its decoration as
        // well, which covers the widths where the overage is smaller than the
        // decoration and there is no character left to cut.
        kept[idx].segment.text = clipped;
        kept[idx].clipped = true;
    }

    // Phase B — drop whole segments from the tail until the remainder fits
    // or one segment is left. A lone segment that is still too wide is not
    // popped here; the `text` check below yields the empty string for that
    // case, so the result is always a whole-segment rendering.
    while kept.len() > 1 && visible_len(&render(&kept)) > max_width {
        kept.pop();
    }

    let text = render(&kept);
    if visible_len(&text) <= max_width { text } else { String::new() }
}
