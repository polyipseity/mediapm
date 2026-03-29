//! Shared orchestration runtime constants.
//!
//! These defaults intentionally live in one module so clients, runtime actor
//! implementations, and spawn helpers remain aligned.

/// Default actor RPC timeout used by orchestration clients and runtime calls.
pub(super) const DEFAULT_RPC_TIMEOUT_MS: u64 = 30_000;
/// Maximum concurrent client requests allowed for storage RPC paths.
pub(super) const DEFAULT_MAX_INFLIGHT_CLIENT_REQUESTS: usize = 256;
/// Soft disk-pressure threshold as percent of CAS size (available/cas * 100).
pub(super) const SOFT_DISK_PRESSURE_PERCENT: u128 = 5;
/// Hard disk-pressure threshold as percent of CAS size (available/cas * 100).
pub(super) const HARD_DISK_PRESSURE_PERCENT: u128 = 1;
/// Absolute free-space threshold that triggers hard pressure handling.
pub(super) const CRITICAL_SPACE_BYTES: u64 = 64 * 1024 * 1024;
