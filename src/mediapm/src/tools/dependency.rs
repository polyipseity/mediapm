//! Dependency type classification for managed-tool dependencies.
//!
//! `DependencyType` determines how a dependency is provisioned:
//! - `SameStep`: payload is inlined into the same conductor step (companion).
//! - `CrossStep`: dependency runs as a separate conductor workflow step.
//! - `Both`: functions as both.
//!
//! The registry function [`known_dependency_type`] provides per-tool lookup.

/// Dependency provisioning type for a managed-tool dependency.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
#[allow(dead_code)]
pub(crate) enum DependencyType {
    /// Dependency payload is inlined into the same conductor step as the
    /// primary tool (same-step companion).
    SameStep,
    /// Dependency runs as a separate conductor workflow step (cross-step).
    CrossStep,
    /// Functions as both same-step companion AND cross-step tool.
    Both,
}

/// Looks up the known `DependencyType` for a dependency of a given tool.
///
/// Returns `None` if the tool or dependency is unknown.
#[must_use]
#[allow(dead_code)]
pub(crate) fn known_dependency_type(tool_id: &str, dep_id: &str) -> Option<DependencyType> {
    match tool_id {
        "yt-dlp" => super::preset::yt_dlp::dependency_types().get(dep_id).copied(),
        "media-tagger" => super::preset::media_tagger::dependency_types().get(dep_id).copied(),
        "rsgain" => super::preset::rsgain::dependency_types().get(dep_id).copied(),
        _ => None,
    }
}
