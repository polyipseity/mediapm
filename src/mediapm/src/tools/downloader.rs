//! Managed-tool download cache type aliases.
//!
//! Re-exports download-related types from the conductor. The actual
//! fetch/resolve/extract/provision logic lives in the conductor's
//! [`mediapm_conductor::tools`] module behind the `tool-presets` feature.

use mediapm_conductor::cache::CachePruneReport;
use mediapm_conductor::cache_user_level::UserLevelCache;

/// User-level managed-tool download cache.
pub(crate) type ToolDownloadCache = UserLevelCache;

/// Summary of one cache-prune operation.
#[allow(dead_code)]
pub(crate) type ToolCachePruneReport = CachePruneReport;
