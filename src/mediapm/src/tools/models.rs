//! Shared data structures for release resolution and payload provisioning.
//!
//! These types are used by the downloader and tool-runtime modules to
//! represent resolved tool identity, download plans, and provisioned payloads.

use std::collections::BTreeMap;
use std::path::PathBuf;

use crate::tools::catalog::{ToolCatalogEntry, ToolOs};

/// GitHub API base URL used for release metadata queries.
#[allow(dead_code)]
pub(crate) const GITHUB_API_BASE: &str = "https://api.github.com/repos";

/// Resolved release identity metadata for immutable tool-id construction.
#[derive(Debug, Clone, PartialEq, Eq, Default)]
pub(crate) struct ResolvedToolIdentity {
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
#[derive(Debug, Clone, PartialEq, Eq)]
#[allow(dead_code)]
pub(crate) struct ProvisionedToolPayload {
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
#[derive(Debug, Clone, PartialEq, Eq)]
#[allow(dead_code)]
pub(crate) enum ContentMapSource {
    /// Entry bytes come from a materialized regular file path.
    FilePath(PathBuf),
    /// Entry bytes should be generated as an uncompressed ZIP of this folder.
    DirectoryZip { root_dir: PathBuf },
}

/// One resolved per-OS download action.
#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) struct OsDownloadAction {
    /// OS target for this action.
    pub os: ToolOs,
    /// URL candidates tried in order.
    pub urls: Vec<String>,
    /// Payload archive format.
    pub archive_format: &'static str,
}

/// Resolved download plan including release identity metadata.
#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) struct ResolvedDownloadPlan {
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
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) struct DownloadProgressSnapshot {
    /// Bytes downloaded so far.
    pub downloaded_bytes: u64,
    /// Total expected bytes, if known.
    pub total_bytes: Option<u64>,
}

/// Callback invoked with progress snapshots during transfer.
pub(crate) type DownloadProgressCallback =
    std::sync::Arc<dyn Fn(DownloadProgressSnapshot) + Send + Sync>;
