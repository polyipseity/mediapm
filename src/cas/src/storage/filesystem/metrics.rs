//! Filesystem CAS observability counters and actor RPC timing scopes.
//!
//! Counters here are intentionally low-overhead and lock-free so they can be
//! sampled from hot paths without materially affecting throughput.

use std::sync::atomic::{AtomicU64, Ordering};
use std::time::Instant;

/// Public observability counters for the filesystem backend.
#[derive(Debug, Clone, Copy, PartialEq)]
pub struct FileSystemMetrics {
    /// Number of successful object-byte cache hits.
    pub cache_hits: u64,
    /// Aggregate ratio of persisted delta payload bytes over reconstructed content bytes.
    pub delta_compression_ratio: f64,
    /// Cumulative optimizer runtime in milliseconds across optimize runs.
    pub optimizer_runtime_ms: u64,
    /// Current number of in-flight object-actor RPC requests.
    pub object_actor_inflight: u64,
    /// Peak in-flight object-actor RPC requests observed since startup.
    pub object_actor_inflight_peak: u64,
    /// Total completed object-actor RPC calls.
    pub object_actor_rpc_calls: u64,
    /// Cumulative object-actor RPC wait time in milliseconds.
    pub object_actor_rpc_wait_ms: u64,
}

/// Mutable atomic backing store for [`FileSystemMetrics`] snapshots.
///
/// The filesystem backend updates these counters in-place and periodically
/// materializes immutable snapshots for API consumers.
#[derive(Debug, Default)]
pub(super) struct FileSystemMetricsState {
    /// Number of successful object cache hits.
    pub(super) cache_hits: AtomicU64,
    /// Total persisted delta payload bytes.
    pub(super) delta_payload_bytes: AtomicU64,
    /// Total reconstructed content bytes represented by delta objects.
    pub(super) delta_content_bytes: AtomicU64,
    /// Aggregate optimizer execution time in milliseconds.
    pub(super) optimizer_runtime_ms: AtomicU64,
    /// Current number of in-flight object actor RPC calls.
    pub(super) object_actor_inflight: AtomicU64,
    /// Maximum in-flight object actor RPC calls observed so far.
    pub(super) object_actor_inflight_peak: AtomicU64,
    /// Count of completed object actor RPC calls.
    pub(super) object_actor_rpc_calls: AtomicU64,
    /// Cumulative wall-clock wait time spent in object actor RPC calls.
    pub(super) object_actor_rpc_wait_ms: AtomicU64,
}

/// Scope guard that tracks one object-actor RPC's in-flight and wait metrics.
///
/// Construct with [`ObjectActorRpcScope::new`], then drop naturally when the
/// RPC finishes to publish elapsed timing and decrement in-flight counters.
pub(super) struct ObjectActorRpcScope<'a> {
    metrics: &'a FileSystemMetricsState,
    started: Instant,
}

impl<'a> ObjectActorRpcScope<'a> {
    /// Starts a new object-actor RPC metrics scope.
    pub(super) fn new(metrics: &'a FileSystemMetricsState) -> Self {
        Self { metrics, started: Instant::now() }
    }
}

impl Drop for ObjectActorRpcScope<'_> {
    fn drop(&mut self) {
        self.metrics.object_actor_inflight.fetch_sub(1, Ordering::AcqRel);
        self.metrics.object_actor_rpc_calls.fetch_add(1, Ordering::Relaxed);
        let elapsed_ms = self.started.elapsed().as_millis().max(1) as u64;
        self.metrics.object_actor_rpc_wait_ms.fetch_add(elapsed_ms, Ordering::Relaxed);
    }
}
