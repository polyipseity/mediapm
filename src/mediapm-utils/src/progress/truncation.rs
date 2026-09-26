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
/// (count/total, elapsed, rate, eta) alongside its own fields.  The
/// width budget is the visible-char budget after subtracting ANSI
/// overhead (4 bytes for the leading `\x1b[0m` reset the renderer
/// prepends).
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

/// Join `parts` with a single space, dropping trailing parts until the
/// joined string fits `max_width` visible columns. When the first part
/// alone exceeds the budget, it is hard-truncated to fit. A chars-based
/// safety-net truncation is applied as a final fallback.
///
/// This is the shared truncation primitive used by all three bar-label
/// structs (`StepBarLabel`, `WorkerBarLabel`, `MaterializationBarLabel`).
#[cfg(feature = "progress")]
#[must_use]
pub fn truncate_ordered(parts: &[String], max_width: usize) -> String {
    let mut kept: Vec<&String> = Vec::new();
    let mut width = 0usize;
    for part in parts {
        let add = if kept.is_empty() { part.len() } else { part.len() + 1 };
        if width + add > max_width && !kept.is_empty() {
            break;
        }
        // Hard-truncate the first part if it alone exceeds the budget.
        if kept.is_empty() && add > max_width {
            kept.push(part);
            break;
        }
        width += add;
        kept.push(part);
    }
    let result: String = kept.iter().map(|s| s.as_str()).collect::<Vec<_>>().join(" ");
    // Safety-net: chars().take handles non-ASCII edge cases.
    if result.len() > max_width { result.chars().take(max_width).collect() } else { result }
}

/// Minimum visible columns retained for a front-ellipsised segment.
///
/// One column of tail plus the leading ellipsis. A segment already this
/// short yields no further width and is left alone.
#[cfg(feature = "progress")]
pub const MIN_TAIL: usize = 2;

/// How a segment yields width when the bar line does not fit.
///
/// A segment is either shortened (elastic) or surrendered whole (keep).
/// Every segment is eventually droppable; the distinction only governs
/// *how* width is reclaimed first.
#[cfg(feature = "progress")]
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Shrink {
    /// Never shortened. The segment is dropped whole when it cannot fit.
    Keep,
    /// Shortened from the front, keeping the informative tail: `…/tail`.
    ///
    /// Use for values with a meaningful end — paths, tool names, and free
    /// user text. Never for values that are meaningless when clipped:
    /// `[wf]` is not `…f]`.
    FrontEllipsis,
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
    /// Protected pieces are spared by the drop phase while `max_width` is
    /// at or above the label's floor. They are never shortened.
    pub protected: bool,
}

#[cfg(feature = "progress")]
impl Segment {
    /// A protected piece: never shortened, dropped only below the floor.
    #[must_use]
    pub fn protected(text: impl Into<String>) -> Self {
        Self { text: text.into(), shrink: Shrink::Keep, protected: true }
    }

    /// An elastic piece: shortened from the front before anything is dropped.
    #[must_use]
    pub fn elastic(text: impl Into<String>) -> Self {
        Self { text: text.into(), shrink: Shrink::FrontEllipsis, protected: false }
    }

    /// A non-protected piece that is dropped whole rather than shortened.
    #[must_use]
    pub fn new_keep(text: impl Into<String>) -> Self {
        Self { text: text.into(), shrink: Shrink::Keep, protected: false }
    }
}

/// Shorten `text` to `target` visible columns, keeping the tail.
///
/// Returns `text` unchanged when it already fits. A `target` of `0` yields
/// an empty string; `1` yields a bare ellipsis.
#[cfg(feature = "progress")]
#[must_use]
pub fn front_ellipsis(text: &str, target: usize) -> String {
    let chars: Vec<char> = text.chars().collect();
    if chars.len() <= target {
        return text.to_string();
    }
    if target == 0 {
        return String::new();
    }
    if target == 1 {
        return "…".to_string();
    }
    let tail: String = chars[chars.len() - (target - 1)..].iter().collect();
    format!("…{tail}")
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
/// * **Phase A (shrink)** — each [`Shrink::FrontEllipsis`] segment, from the
///   tail forward, is shortened by exactly the overage, never below
///   [`MIN_TAIL`], before the next one yields anything.
/// * **Phase B (drop)** — once no segment can shrink further, segments drop
///   from the tail, sparing protected ones while `max_width` is at or above
///   `floor`. Below the floor, protection lifts and the tail is dropped as
///   needed.
///
/// The result never exceeds `max_width`: a final `chars().take` safety net
/// applies, so layout safety never depends on the policy above.
///
/// # Examples
///
/// ```
/// # use mediapm_utils::progress::{Segment, fit_segments};
/// let segs = vec![
///     Segment::protected("[wf]"),
///     Segment::elastic("Music/Artist/Album/song.mkv"),
/// ];
/// assert_eq!(fit_segments(&segs, 40, 12), "[wf] Music/Artist/Album/song.mkv");
/// assert_eq!(fit_segments(&segs, 20, 12), "[wf] …Album/song.mkv");
/// ```
#[cfg(feature = "progress")]
#[must_use]
pub fn fit_segments(segments: &[Segment], max_width: usize, floor: usize) -> String {
    if segments.is_empty() {
        return String::new();
    }
    let protect = max_width >= floor;
    let mut kept: Vec<Segment> = segments.to_vec();

    // Phase A — elastic segments yield width from the front, tail first.
    for idx in (0..kept.len()).rev() {
        if visible_len(&render(&kept)) <= max_width {
            break;
        }
        if kept[idx].shrink != Shrink::FrontEllipsis {
            continue;
        }
        let over = visible_len(&render(&kept)) - max_width;
        let current = kept[idx].text.chars().count();
        let target = current.saturating_sub(over).max(MIN_TAIL);
        if target >= current {
            continue;
        }
        kept[idx].text = front_ellipsis(&kept[idx].text, target);
    }

    // Phase B — drop from the tail, sparing protected segments in force.
    while kept.len() > 1 {
        if visible_len(&render(&kept)) <= max_width {
            break;
        }
        if protect && kept[kept.len() - 1].protected {
            break;
        }
        kept.pop();
    }

    let text = render(&kept);
    if visible_len(&text) > max_width { text.chars().take(max_width).collect() } else { text }
}
