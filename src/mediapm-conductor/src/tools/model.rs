//! Shared data structures for release resolution and payload provisioning.
//!
//! Feature-gated behind `tool-presets`.
//!
//! These types are used by the resolver and provisioner modules to represent
//! resolved tool identity, download plans, and provisioned payloads.

#[cfg(feature = "tool-presets")]
use std::collections::BTreeMap;
#[cfg(feature = "tool-presets")]
use std::path::PathBuf;

#[cfg(feature = "tool-presets")]
use crate::tools::catalog::{ToolCatalogEntry, ToolOs};

/// GitHub API base URL used for release metadata queries.
#[cfg(feature = "tool-presets")]
#[allow(dead_code)]
pub const GITHUB_API_BASE: &str = "https://api.github.com/repos";

/// Resolved release identity metadata for immutable tool-id construction.
#[cfg(feature = "tool-presets")]
#[derive(Debug, Clone, PartialEq, Eq, Default)]
pub struct ResolvedToolIdentity {
    /// Optional git commit hash (preferred immutable selector).
    pub git_hash: Option<String>,
    /// Optional semantic version label.
    pub version: Option<String>,
    /// Optional tag label.
    pub tag: Option<String>,
    /// Optional human-readable release summary.
    pub release_description: Option<String>,
}

/// Provisioned tool payload under the workspace tools cache root.
#[cfg(feature = "tool-presets")]
#[derive(Debug, Clone, PartialEq, Eq)]
#[allow(dead_code)]
pub struct ProvisionedToolPayload {
    /// Fully resolved immutable tool id.
    pub tool_id: String,
    /// Sandbox-relative command selector for executable tool specs.
    pub command_selector: String,
    /// Materialized content-map payload entries.
    pub content_entries: BTreeMap<String, ContentMapSource>,
    /// Resolved identity metadata for lock state and diagnostics.
    pub identity: ResolvedToolIdentity,
    /// Human-readable source label recorded in lock metadata.
    pub source_label: String,
    /// Stable source identifier fragment used in immutable tool id.
    pub source_identifier: String,
    /// Catalog entry that produced this payload.
    pub catalog: ToolCatalogEntry,
    /// Non-fatal metadata-resolution warnings.
    pub warnings: Vec<String>,
}

/// Source payload representation for conductor `content_map` entries.
#[cfg(feature = "tool-presets")]
#[derive(Debug, Clone, PartialEq, Eq)]
#[allow(dead_code)]
pub enum ContentMapSource {
    /// Entry bytes come from a materialized regular file path.
    FilePath(PathBuf),
    /// Entry bytes should be generated as an uncompressed ZIP of this folder.
    DirectoryZip { root_dir: PathBuf },
}

/// One resolved per-OS download action.
#[cfg(feature = "tool-presets")]
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct OsDownloadAction {
    /// OS target for this action.
    pub os: ToolOs,
    /// URL candidates tried in order.
    pub urls: Vec<String>,
    /// Payload archive format.
    pub archive_format: &'static str,
}

/// Resolved download plan including release identity metadata.
#[cfg(feature = "tool-presets")]
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ResolvedDownloadPlan {
    /// Concrete actions keyed by OS target.
    pub per_os_actions: BTreeMap<ToolOs, OsDownloadAction>,
    /// Whether all targets share one package.
    pub shared_package: bool,
    /// Whether payload is a local internal-launcher shim.
    pub internal_launcher: bool,
    /// Resolved release identity metadata.
    pub identity: ResolvedToolIdentity,
    /// Human-readable source label for lock metadata.
    pub source_label: String,
    /// Stable source identifier for immutable tool ids.
    pub source_identifier: String,
    /// Non-fatal warnings from release-metadata resolution.
    pub warnings: Vec<String>,
}

/// Snapshot of download progress at one point in time.
#[cfg(feature = "tool-presets")]
pub use mediapm_utils::progress::{
    DownloadProgressSnapshot, ProgressCallback as DownloadProgressCallback,
};
