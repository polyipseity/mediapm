//! Provider/download progress snapshot types (always available).

use std::sync::Arc;

/// Snapshot of download progress at one point in time.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct DownloadProgressSnapshot {
    /// Bytes downloaded so far.
    pub downloaded_bytes: u64,
    /// Total expected bytes, if known.
    pub total_bytes: Option<u64>,
}

/// Callback invoked with progress snapshots during a transfer.
pub type ProgressCallback = Arc<dyn Fn(DownloadProgressSnapshot) + Send + Sync>;

/// Which provider phase is currently active.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum ProviderPhase {
    /// Phase 1: resolving metadata and sources.
    Resolve,
    /// Phase 2: fetching or generating bytes.
    Fetch,
    /// Phase 3: processing (extract, repack, CAS import).
    Process,
}

/// Snapshot of provider progress at one point in time across all three phases.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct ProviderProgressSnapshot {
    /// Current phase.
    pub phase: ProviderPhase,
    /// Items completed vs total `(completed, total)`.
    /// Phase 1: sources resolved; Phase 2: files fetched; Phase 3: entries processed.
    pub items: (u64, u64),
    /// Bytes completed vs total `(completed, total)`.
    /// Phase 1: `(0, 0)`; Phase 2: downloaded bytes; Phase 3: CAS-imported bytes.
    pub bytes: (u64, u64),
}

/// Callback invoked with provider progress snapshots during tool provisioning.
pub type ProviderProgressCallback = Arc<dyn Fn(ProviderProgressSnapshot) + Send + Sync>;
