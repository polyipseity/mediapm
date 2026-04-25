//! Orchestration actor/runtime configuration constants.

use std::num::NonZeroUsize;

/// Default RPC timeout for actor request/response calls.
///
/// Workflow execution can include managed tool downloads and online processing
/// that routinely exceed short interactive RPC windows. Keep this timeout long
/// enough for end-to-end workflow calls while still bounded for failed actor
/// paths.
pub const DEFAULT_RPC_TIMEOUT_MS: u64 = 1_800_000;

/// Default EWMA alpha for adaptive tool runtime estimation.
pub const DEFAULT_SCHEDULER_EWMA_ALPHA: f64 = 0.35;

/// Default estimated runtime (ms) for tools with no history.
pub const DEFAULT_UNKNOWN_STEP_COST_MS: f64 = 10.0;

/// Default in-memory scheduler trace buffer capacity.
pub const DEFAULT_SCHEDULER_TRACE_CAPACITY: usize = 1_024;

/// Environment override key for worker pool size.
pub const ENV_WORKER_POOL_SIZE: &str = "MEDIAPM_CONDUCTOR_WORKER_POOL_SIZE";

/// Environment override key for scheduler EWMA alpha.
pub const ENV_SCHEDULER_EWMA_ALPHA: &str = "MEDIAPM_CONDUCTOR_SCHEDULER_EWMA_ALPHA";

/// Environment override key for unknown-step cost estimate in milliseconds.
pub const ENV_UNKNOWN_STEP_COST_MS: &str = "MEDIAPM_CONDUCTOR_UNKNOWN_STEP_COST_MS";

/// Environment override key for scheduler trace ring-buffer capacity.
pub const ENV_SCHEDULER_TRACE_CAPACITY: &str = "MEDIAPM_CONDUCTOR_SCHEDULER_TRACE_CAPACITY";

/// Returns default step-worker pool size for multi-actor execution.
#[must_use]
pub fn default_worker_pool_size() -> usize {
    std::env::var(ENV_WORKER_POOL_SIZE)
        .ok()
        .and_then(|value| value.parse::<usize>().ok())
        .and_then(NonZeroUsize::new)
        .map_or_else(
            || std::thread::available_parallelism().map(usize::from).unwrap_or(1).max(1),
            NonZeroUsize::get,
        )
}

/// Returns EWMA alpha used by adaptive scheduler.
#[must_use]
pub fn scheduler_ewma_alpha() -> f64 {
    std::env::var(ENV_SCHEDULER_EWMA_ALPHA)
        .ok()
        .and_then(|value| value.parse::<f64>().ok())
        .filter(|alpha| (0.0..=1.0).contains(alpha) && *alpha > 0.0)
        .unwrap_or(DEFAULT_SCHEDULER_EWMA_ALPHA)
}

/// Returns default estimated cost for unseen tools (milliseconds).
#[must_use]
pub fn unknown_step_cost_ms() -> f64 {
    std::env::var(ENV_UNKNOWN_STEP_COST_MS)
        .ok()
        .and_then(|value| value.parse::<f64>().ok())
        .filter(|estimate| estimate.is_finite() && *estimate > 0.0)
        .unwrap_or(DEFAULT_UNKNOWN_STEP_COST_MS)
}

/// Returns scheduler trace ring-buffer capacity.
#[must_use]
pub fn scheduler_trace_capacity() -> usize {
    std::env::var(ENV_SCHEDULER_TRACE_CAPACITY)
        .ok()
        .and_then(|value| value.parse::<usize>().ok())
        .unwrap_or(DEFAULT_SCHEDULER_TRACE_CAPACITY)
}
