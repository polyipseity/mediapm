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
/// `truncate_suffix`(Self::truncate_suffix) receives the merged
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
