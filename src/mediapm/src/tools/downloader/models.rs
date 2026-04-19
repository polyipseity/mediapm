//! Shared data structures for release resolution and payload materialization.

use std::collections::BTreeMap;
use std::path::PathBuf;

use crate::tools::catalog::{DownloadPayloadMode, ToolCatalogEntry, ToolOs};

/// GitHub API base URL used for release metadata queries.
pub(super) const GITHUB_API_BASE: &str = "https://api.github.com/repos";

/// Resolved release identity metadata used for immutable tool-id construction.
#[derive(Debug, Clone, PartialEq, Eq, Default)]
pub(crate) struct ResolvedToolIdentity {
    /// Optional git commit hash (preferred immutable selector when available).
    pub git_hash: Option<String>,
    /// Optional semantic/version label.
    pub version: Option<String>,
    /// Optional tag label.
    pub tag: Option<String>,
    /// Optional human-readable release summary.
    pub release_description: Option<String>,
}

/// Provisioned tool payload prepared under `.mediapm/tools/<tool-id>/`.
#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) struct ProvisionedToolPayload {
    /// Fully resolved immutable tool id.
    pub tool_id: String,
    /// Sandbox-relative command selector used by executable tool specs.
    pub command_selector: String,
    /// Materialized content-map payload entries (`content-map key -> payload source`).
    ///
    /// Archive downloads may emit directory keys (for example `./` or
    /// `windows/`) that should be serialized as ZIP payload bytes.
    pub content_entries: BTreeMap<String, ContentMapSource>,
    /// Resolved identity metadata for lock state and diagnostics.
    pub identity: ResolvedToolIdentity,
    /// Human-readable source label recorded in lock metadata.
    pub source_label: String,
    /// Stable source identifier fragment used in immutable tool id.
    pub source_identifier: String,
    /// Catalog entry that produced this payload.
    pub catalog: ToolCatalogEntry,
    /// Non-fatal metadata-resolution warnings produced during provisioning.
    pub warnings: Vec<String>,
}

/// Source payload representation for one conductor `content_map` entry.
#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) enum ContentMapSource {
    /// Entry bytes come directly from one materialized regular file path.
    FilePath(PathBuf),
    /// Entry bytes should be generated as an uncompressed ZIP of this folder.
    ///
    /// The map key determines where conductor unpacks this ZIP payload.
    DirectoryZip { root_dir: PathBuf },
}

/// One resolved per-OS download action.
#[derive(Debug, Clone, PartialEq, Eq)]
pub(super) struct OsDownloadAction {
    /// OS target represented by this action.
    pub os: ToolOs,
    /// URL candidates tried in-order.
    pub urls: Vec<String>,
    /// Payload transfer/extraction mode.
    pub mode: DownloadPayloadMode,
}

/// Resolved download plan including release identity metadata.
#[derive(Debug, Clone, PartialEq, Eq)]
pub(super) struct ResolvedDownloadPlan {
    /// Concrete actions keyed by OS target.
    pub per_os_actions: BTreeMap<ToolOs, OsDownloadAction>,
    /// Whether all targets are satisfied by one shared package.
    pub shared_package: bool,
    /// Whether payload should be generated as local internal-launcher shim.
    pub internal_launcher: bool,
    /// Resolved release identity metadata.
    pub identity: ResolvedToolIdentity,
    /// Human-readable source label for lock metadata.
    pub source_label: String,
    /// Stable source identifier fragment for immutable tool ids.
    pub source_identifier: String,
    /// Non-fatal warnings emitted while resolving release metadata.
    pub warnings: Vec<String>,
}
