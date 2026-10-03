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
    Keep,
    /// Shortened from the front, keeping the informative tail, so
    /// `Music/Artist/Album/song.mkv` at 15 columns reads `Album/song.mkv`.
    ///
    /// Use for values with a meaningful end: paths, tool names, and free
    /// user text. Never for a fixed marker, where the cut takes the brackets
    /// that say what the piece is: `[wf]` clipped to two columns reads `wf]`,
    /// loose text instead of a phase tag, and no amount of fitting puts the
    /// brackets back. A one-column window holds no boundary to cut after, so
    /// it yields nothing rather than `f]`. A `Keep` marker is dropped whole
    /// or kept whole.
    Front,
    /// Shortened from the end, keeping the leading characters, so `7.1` at
    /// two columns reads `7.`.
    ///
    /// Use for values that number or name themselves from the left, where
    /// the leading columns carry the identity. The cut lands between
    /// characters rather than after a boundary, because a version holds no
    /// boundary and `7.` is the shape the row is meant to read. A value that
    /// does hold boundaries is better served by [`Front`](Self::Front), whose
    /// cut leaves a piece that names something on its own.
    Head,
}

/// Characters whose following character a clipped tail may start at.
///
/// A tail is the right-hand end of a value, so the only place worth cutting is
/// just after one of these. Each one ends a piece of the value, so cutting
/// after it leaves a piece that stands on its own.
///
/// * space and `/` end a word and a path element, so `Children` survives where
///   `hildren` does not.
/// * `(` and `[` open a bracketed group the label conventions write as one
///   piece, so `(ffmpeg)` and `[youtube.dQw4w9WgXcQ]` survive whole.
/// * `-` ends a word of a hyphenated name, the same way a space ends a word
///   of a phrase, so `builtin-archive` survives where `archive)` does not say
///   which tool it came from.
///
/// Two characters are deliberately absent. A closing bracket, because
/// `(ffmpeg)` cut to two columns keeps `g)` whichever way the cut is made: the
/// bracket that opened the name is the boundary the cut uses. A dot, because
/// it is the one boundary that lands inside a word rather than between words,
/// so a filename cut back to it comes back as `mkv`.
#[cfg(feature = "progress")]
const SEGMENT_BOUNDARIES: [char; 5] = [' ', '/', '(', '[', '-'];

/// The boundaries that join two pieces of a value rather than opening one.
///
/// A tail that begins on one of these is a cut that landed *on* the joiner
/// instead of after it, and it reads as noise on the row: `[cmt] - Telepathy.flac`
/// carries a dash that names nothing, and a tail of nothing but spaces is the
/// double space a row used to show between two segments. The bracketing
/// boundaries are absent, because a tail that opens with `[` or `(` opens a
/// complete group and reads as one.
#[cfg(feature = "progress")]
const JOINING_BOUNDARIES: [char; 3] = [' ', '/', '-'];

/// Whether `c` ends a piece of a label value, so that a tail cut after it
/// names something on its own.
#[cfg(feature = "progress")]
fn is_boundary(c: char) -> bool {
    SEGMENT_BOUNDARIES.contains(&c)
}

/// One ordered piece of a bar label.
///
/// Segments are supplied most important first. [`fit_segments`] walks the
/// list from the tail, so the last segment is the first to yield.
#[cfg(feature = "progress")]
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Segment {
    /// Rendered text of this piece, without the joining separator.
    pub text: String,
    /// How this piece yields width.
    pub shrink: Shrink,
}

#[cfg(feature = "progress")]
impl Segment {
    /// A piece that is surrendered whole rather than shortened: it is
    /// dropped when it cannot fit.
    #[must_use]
    pub fn keep(text: impl Into<String>) -> Self {
        Self { text: text.into(), shrink: Shrink::Keep }
    }

    /// An elastic piece: shortened from the front before anything is dropped.
    #[must_use]
    pub fn elastic(text: impl Into<String>) -> Self {
        Self { text: text.into(), shrink: Shrink::Front }
    }

    /// An elastic piece that keeps its leading characters: it gives columns
    /// back from the end before anything is dropped.
    #[must_use]
    pub fn elastic_head(text: impl Into<String>) -> Self {
        Self { text: text.into(), shrink: Shrink::Head }
    }
}

/// Shorten `text` to at most `target` visible columns, keeping the tail.
///
/// The tail is the last `target` columns, and the cut moves left to just after
/// the first boundary character in it, so what survives is a whole word or a
/// whole bracketed group rather than a piece of one. The first boundary is
/// the right one to cut after because it keeps the most text: `he Wall` becomes
/// `Wall` rather than `W`, and `/Album/song.mkv` becomes `Album/song.mkv`
/// rather than `song.mkv`. A tail with no boundary left in it names nothing,
/// and this returns the empty string for it: the caller drops the segment
/// whole instead of rendering `g)` or `n`.
///
/// Nothing marks the cut, so the tail itself is all the reader gets:
/// `front_tail("(ffmpeg)", 7)` is `ffmpeg)`,
/// `front_tail("Music/Pink Floyd/The Wall", 7)` is `Wall`, and
/// `front_tail("Music/Has the Right to Children", 6)` is empty. A tail that
/// would begin on a space, a `/`, or a `-` starts after it instead, so
/// `front_tail("01 - Telepathy.flac", 16)` is `Telepathy.flac` and not
/// `- Telepathy.flac`.
///
/// Returns `text` unchanged when it already fits, and an empty string when
/// `target` is `0`.
#[cfg(feature = "progress")]
#[must_use]
pub fn front_tail(text: &str, target: usize) -> String {
    let chars: Vec<char> = text.chars().collect();
    if chars.len() <= target {
        return text.to_string();
    }
    if target == 0 {
        return String::new();
    }
    let mut start = chars.len() - target;
    // A tail that already begins at a boundary is left alone, so `ffmpeg)`
    // and `Astley` are not trimmed a second time by their own brackets and
    // separators. Otherwise the cut takes what follows the first boundary
    // inside the tail, which is the empty string when there is none.
    if start > 0 && !is_boundary(chars[start - 1]) {
        match chars[start..].iter().position(|c| is_boundary(*c)) {
            Some(offset) => start += offset + 1,
            None => return String::new(),
        }
    }
    // A tail may not begin on a joiner: the row shows one space between
    // segments, so a tail that opens with another renders as a double space,
    // and a tail of `-` renders as a dash with nothing after it.
    while start < chars.len() && JOINING_BOUNDARIES.contains(&chars[start]) {
        start += 1;
    }
    chars[start..].iter().collect()
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

/// Join `segments` with single spaces, using each `text` verbatim.
#[cfg(feature = "progress")]
fn render(segments: &[Segment]) -> String {
    segments.iter().map(|s| s.text.as_str()).collect::<Vec<_>>().join(" ")
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
///   shortened by the overage before the next one yields anything. A
///   [`Shrink::Front`] segment stops at the first boundary in its own tail, so
///   a segment whose overage reaches past that boundary is shortened by less
///   than the overage, and the remainder can still be too wide when Phase A
///   ends. A [`Shrink::Head`] segment has no boundary to stop at and is cut
///   between characters, so it always gives back exactly the overage. A
///   tail-keeping segment with no boundary left in its tail yields nothing
///   at all: a tail that names nothing is worse than no segment, because
///   `render` joins with single spaces, so an empty piece would leave two
///   spaces where the text was. Such a segment is dropped on the spot rather
///   than left for Phase B: it is about to give its columns back, and a
///   segment still holding them would deny the segments ahead of it the
///   overage they need to yield at all.
/// * **Phase B (drop)**: once no segment can shrink further, whole segments
///   drop from the tail until the remainder fits or one segment is left. The
///   leading segment is never popped here, so a leading head group yields
///   progressively rather than all at once: a step bar headed by
///   `[wf] [F] 1/4` still renders `[wf] [F]` at width 11, and only `[wf]` at
///   width 7.
///
/// The ladder **never shaves**: no segment is ever cut inside a word. A
/// segment is shown whole, shortened from the front to a tail that names
/// something on its own, shortened from the end when it numbers itself from
/// the left, or absent. This is the property that separates it from the prefix
/// cut it replaces.
///
/// # Examples
///
/// ```
/// # use mediapm_utils::progress::{Segment, fit_segments};
/// let segs = vec![
///     Segment::keep("[wf]"),
///     Segment::elastic("Music/Artist/Album/song.mkv"),
/// ];
/// assert_eq!(fit_segments(&segs, 40), "[wf] Music/Artist/Album/song.mkv");
/// assert_eq!(fit_segments(&segs, 20), "[wf] Album/song.mkv");
/// // At 12 columns the path's own tail holds no boundary, so the path is
/// // dropped whole rather than rendered as a fragment.
/// assert_eq!(fit_segments(&segs, 12), "[wf]");
/// ```
///
/// A head-keeping segment gives its columns back from the other end, which is
/// what a version needs: it numbers itself from the left, so a clip that kept
/// the tail could never produce `7.` at any width.
///
/// ```
/// # use mediapm_utils::progress::{Segment, fit_segments};
/// let segs = vec![Segment::keep("[wf]"), Segment::elastic_head("7.1")];
/// assert_eq!(fit_segments(&segs, 20), "[wf] 7.1");
/// assert_eq!(fit_segments(&segs, 7), "[wf] 7.");
/// assert_eq!(fit_segments(&segs, 6), "[wf] 7");
/// ```
///
/// # Degenerate widths
///
/// Below the width of a single segment the ladder yields an empty string
/// rather than a clipped one:
///
/// ```
/// # use mediapm_utils::progress::{Segment, fit_segments};
/// let segs = vec![Segment::keep("[wf]")];
/// assert_eq!(fit_segments(&segs, 4), "[wf]");
/// assert_eq!(fit_segments(&segs, 3), "");
/// ```
#[cfg(feature = "progress")]
#[must_use]
pub fn fit_segments(segments: &[Segment], max_width: usize) -> String {
    if segments.is_empty() {
        return String::new();
    }

    let mut kept: Vec<Segment> = segments.to_vec();

    // Phase A: elastic segments yield width, tail first, each from the end its
    // own mode names. A tail-keeping segment whose tail has no boundary to be
    // cut at is dropped here rather than in Phase B: it is about to hand its
    // columns back, and while it still holds them the segments ahead of it see
    // an overage they cannot answer either.
    let mut idx = kept.len();
    while idx > 0 {
        idx -= 1;
        if visible_len(&render(&kept)) <= max_width {
            break;
        }
        let over = visible_len(&render(&kept)) - max_width;
        let current = kept[idx].text.chars().count();
        let target = current.saturating_sub(over);
        if target >= current {
            continue;
        }
        let clipped = match kept[idx].shrink {
            Shrink::Keep => continue,
            Shrink::Front => front_tail(&kept[idx].text, target),
            Shrink::Head => tail_head(&kept[idx].text, target),
        };
        if clipped.is_empty() {
            kept.remove(idx);
            continue;
        }
        kept[idx].text = clipped;
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
