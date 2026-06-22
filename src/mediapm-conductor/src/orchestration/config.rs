//! Orchestration runtime-configuration helpers.
//!
//! Constants and environment-reading functions used by the coordinator and
//! step workers.  Core default values live in [`crate::defaults`]; this module
//! adds env-override convenience wrappers.

use std::num::NonZeroUsize;

/// Environment override key for worker pool size.
pub(crate) const ENV_WORKER_POOL_SIZE: &str = "MEDIAPM_CONDUCTOR_WORKER_POOL_SIZE";

/// Environment override key for scheduler EWMA alpha.
pub(crate) const ENV_SCHEDULER_EWMA_ALPHA: &str = "MEDIAPM_CONDUCTOR_SCHEDULER_EWMA_ALPHA";

/// Environment override key for conductor RPC timeout in seconds.
pub(crate) const ENV_RPC_TIMEOUT_SECONDS: &str = "MEDIAPM_CONDUCTOR_RPC_TIMEOUT_SECONDS";

/// Reads an environment variable, parses it, and falls back to a default.
fn env_parse_or<T: std::str::FromStr>(name: &str, default: T) -> T {
    std::env::var(name).ok().and_then(|v| v.parse::<T>().ok()).unwrap_or(default)
}

/// Returns the conductor RPC timeout in milliseconds from env var or default.
///
/// Reads `MEDIAPM_CONDUCTOR_RPC_TIMEOUT_SECONDS` and multiplies by 1000.
/// Falls back to [`crate::defaults::DEFAULT_RPC_TIMEOUT_MS`] when unset,
/// unparseable, or zero.
#[must_use]
pub(crate) fn rpc_timeout_ms() -> u64 {
    let seconds = env_parse_or::<u64>(ENV_RPC_TIMEOUT_SECONDS, 0);
    if seconds > 0 { seconds * 1000 } else { crate::defaults::DEFAULT_RPC_TIMEOUT_MS }
}

/// Returns default step-worker pool size for multi-actor execution.
///
/// Reads `MEDIAPM_CONDUCTOR_WORKER_POOL_SIZE` from env.  Falls back to the
/// host's available parallelism (capped at 1 minimum).
#[must_use]
pub(crate) fn default_worker_pool_size() -> usize {
    std::env::var(ENV_WORKER_POOL_SIZE)
        .ok()
        .and_then(|value| value.parse::<usize>().ok())
        .and_then(NonZeroUsize::new)
        .map_or_else(
            || std::thread::available_parallelism().map_or(1, usize::from).max(1),
            NonZeroUsize::get,
        )
}

/// Returns the EWMA alpha used by the adaptive scheduler.
///
/// Clamped to `(0.0, 1.0]` when a valid env value is provided; otherwise
/// falls back to [`crate::defaults::DEFAULT_EWMA_ALPHA`].
#[must_use]
pub(crate) fn scheduler_ewma_alpha() -> f64 {
    let v = env_parse_or::<f64>(ENV_SCHEDULER_EWMA_ALPHA, crate::defaults::DEFAULT_EWMA_ALPHA);
    if (0.0..=1.0).contains(&v) && v > 0.0 { v } else { crate::defaults::DEFAULT_EWMA_ALPHA }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn env_parse_or_returns_default_when_unset() {
        // Use a variable unlikely to be set.
        let val = env_parse_or::<u64>("MEDIAPM_CONDUCTOR_TEST_UNSET_VAR", 42);
        assert_eq!(val, 42);
    }

    #[test]
    fn rpc_timeout_ms_default_when_unset() {
        // Safety: not thread-safe but fine in single-threaded test.
        unsafe {
            std::env::remove_var(ENV_RPC_TIMEOUT_SECONDS);
        }
        let timeout = rpc_timeout_ms();
        assert_eq!(timeout, crate::defaults::DEFAULT_RPC_TIMEOUT_MS);
    }

    #[test]
    fn scheduler_ewma_alpha_default_when_unset() {
        // Safety: not thread-safe but fine in single-threaded test.
        unsafe {
            std::env::remove_var(ENV_SCHEDULER_EWMA_ALPHA);
        }
        let alpha = scheduler_ewma_alpha();
        assert!((alpha - crate::defaults::DEFAULT_EWMA_ALPHA).abs() < f64::EPSILON);
    }
}
