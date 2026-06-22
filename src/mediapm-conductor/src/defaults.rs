//! Centralized defaults for all conductor configuration.
//!
//! All config defaults live here so core config types can drop `Option`
//! wrappers and use concrete values directly.  See PLAN.md §3.7 (P5).

/// Default conductor config directory name.
pub const DEFAULT_CONDUCTOR_DIR_NAME: &str = ".conductor";

/// Default CAS store directory inside `conductor_dir`.
pub const DEFAULT_CAS_STORE_DIR_NAME: &str = "store";

/// Default temporary files directory inside `conductor_dir`.
pub const DEFAULT_CONDUCTOR_TMP_DIR_NAME: &str = "tmp";

/// Default schema export directory inside `conductor_dir`.
pub const DEFAULT_CONDUCTOR_SCHEMA_DIR_NAME: &str = "schemas";

/// Default tools materialization directory inside `conductor_dir`.
pub const DEFAULT_CONDUCTOR_TOOLS_DIR_NAME: &str = "tools";

/// Default maximum concurrent worker tasks.
pub const DEFAULT_WORKER_POOL_SIZE: usize = 4;

/// Default timeout for actor/task RPC calls (milliseconds).
pub const DEFAULT_RPC_TIMEOUT_MS: u64 = 300_000;

/// Default EWMA alpha for adaptive scheduler.
pub const DEFAULT_EWMA_ALPHA: f64 = 0.35;

/// Default estimated runtime in ms for unknown-cost steps.
pub const DEFAULT_UNKNOWN_STEP_COST_MS: f64 = 10.0;

/// Default scheduler trace ring-buffer capacity.
pub const DEFAULT_SCHEDULER_TRACE_CAPACITY: usize = 1024;

/// Default GC interval in seconds (24h).
pub const DEFAULT_CONDUCTOR_GC_INTERVAL_SECONDS: u64 = 86_400;

/// Default conductor GC TTL (grace period) in seconds (7 days).
/// Instances unreferenced after this window are evicted.
pub const DEFAULT_CONDUCTOR_GC_TTL_SECONDS: u64 = 604_800;

/// Default maximum concurrent extraction tasks for tool content cache.
pub const DEFAULT_TOOL_CACHE_MAX_CONCURRENT: usize = 8;

/// Default (primary) output variant name.
pub const DEFAULT_OUTPUT_VARIANT: &str = "primary";
