//! Orchestration actor/runtime configuration constants.

use std::path::PathBuf;

use std::num::NonZeroUsize;

/// Default RPC timeout for actor request/response calls.
///
/// Workflow execution can include managed tool downloads and online processing
/// that routinely exceed short interactive RPC windows. Keep this timeout long
/// enough for end-to-end workflow calls while still bounded for failed actor
/// paths.
pub const DEFAULT_RPC_TIMEOUT_MS: u64 = 300_000;

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

/// Environment override key for optional JSON profile artifact output path.
pub const ENV_PROFILE_OUTPUT_PATH: &str = "MEDIAPM_CONDUCTOR_PROFILE_JSON";

/// Environment override key for conductor RPC timeout in seconds.
///
/// All actor-to-actor RPC calls within conductor use this timeout. When a
/// workflow run or internal actor operation exceeds this limit, the call
/// fails with a timeout error. Default is 300 seconds.
pub const ENV_RPC_TIMEOUT_SECONDS: &str = "MEDIAPM_CONDUCTOR_RPC_TIMEOUT_SECONDS";

/// Reads an environment variable, parses it, and falls back to a default.
#[must_use]
fn env_parse_or<T: std::str::FromStr>(name: &str, default: T) -> T {
    std::env::var(name).ok().and_then(|v| v.parse::<T>().ok()).unwrap_or(default)
}

/// Returns the conductor RPC timeout in milliseconds from env var or default.
///
/// Reads `MEDIAPM_CONDUCTOR_RPC_TIMEOUT_SECONDS` and multiplies by 1000.
/// Falls back to [`DEFAULT_RPC_TIMEOUT_MS`] when unset, unparseable, or zero.
#[must_use]
pub fn rpc_timeout_ms() -> u64 {
    let seconds = env_parse_or::<u64>(ENV_RPC_TIMEOUT_SECONDS, 0);
    if seconds > 0 { seconds * 1000 } else { DEFAULT_RPC_TIMEOUT_MS }
}

/// Returns default step-worker pool size for multi-actor execution.
#[must_use]
pub fn default_worker_pool_size() -> usize {
    std::env::var(ENV_WORKER_POOL_SIZE)
        .ok()
        .and_then(|value| value.parse::<usize>().ok())
        .and_then(NonZeroUsize::new)
        .map_or_else(
            || std::thread::available_parallelism().map_or(1, usize::from).max(1),
            NonZeroUsize::get,
        )
}

/// Returns EWMA alpha used by adaptive scheduler.
#[must_use]
pub fn scheduler_ewma_alpha() -> f64 {
    let v = env_parse_or::<f64>(ENV_SCHEDULER_EWMA_ALPHA, DEFAULT_SCHEDULER_EWMA_ALPHA);
    if (0.0..=1.0).contains(&v) && v > 0.0 { v } else { DEFAULT_SCHEDULER_EWMA_ALPHA }
}

/// Returns default estimated cost for unseen tools (milliseconds).
#[must_use]
pub fn unknown_step_cost_ms() -> f64 {
    let v = env_parse_or::<f64>(ENV_UNKNOWN_STEP_COST_MS, DEFAULT_UNKNOWN_STEP_COST_MS);
    if v.is_finite() && v > 0.0 { v } else { DEFAULT_UNKNOWN_STEP_COST_MS }
}

/// Returns scheduler trace ring-buffer capacity.
#[must_use]
pub fn scheduler_trace_capacity() -> usize {
    env_parse_or::<usize>(ENV_SCHEDULER_TRACE_CAPACITY, DEFAULT_SCHEDULER_TRACE_CAPACITY)
}

/// Returns optional profile-artifact path from environment configuration.
///
/// Empty values are treated as unset.
#[must_use]
pub fn profile_output_path_from_env() -> Option<PathBuf> {
    std::env::var_os(ENV_PROFILE_OUTPUT_PATH)
        .map(PathBuf::from)
        .filter(|path| !path.as_os_str().is_empty())
}
