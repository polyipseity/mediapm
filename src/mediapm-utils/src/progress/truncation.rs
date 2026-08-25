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

/// Client-supplied truncation for a tracked bar's prefix and suffix.
///
/// Implementors receive the maximum visible width budget and return the
/// final display string (already colored/escaped as the client sees fit).
/// mediapm-utils never inspects the field layout — it only invokes these
/// two methods at the single render push point.
#[cfg(feature = "progress")]
pub trait BarLabelTruncation: Send + Sync {
    /// Return the rendered prefix string fitting within `max_width` visible
    /// columns.
    fn truncate_prefix(&self, max_width: usize) -> String;
    /// Return the rendered suffix string fitting within `max_width` visible
    /// columns.
    fn truncate_suffix(&self, max_width: usize) -> String;
}
