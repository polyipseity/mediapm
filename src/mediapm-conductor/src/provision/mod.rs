//! Persistent managed-tool provisioning with CAS-backed materialization.
//!
//! Each tool that declares a `tool_content_map` gets one provisioned entry
//! under `<tools_dir>/<sanitized_tool_id>/`.  An entry contains:
//!
//! - `metadata.json` — version, the full `content_map` (the validity key),
//!   and `last_used_unix_seconds` for TTL-based expiry.
//! - `payload/` — the fully-extracted, ready-to-execute tool content tree.
//!
//! # Single-flight extraction
//!
//! [`ProvisionCache::materialize`] deduplicates concurrent extraction of the
//! same tool: the first caller drives extraction; subsequent callers wait on a
//! [`Notify`] and retry the shared-lock fast path after extraction completes.
//!
//! # Lock protocol
//!
//! - **Shared lock** — held by callers that have a valid cache hit.  Multiple
//!   concurrent users of the same tool share the entry via independent fds.
//! - **Exclusive lock** — held by the extraction task while populating or
//!   refreshing an entry.  Prevents concurrent writers.
//!
//! Extraction uses an exclusive lock, then downgrades to shared before
//! returning the [`ProvisionedTool`] RAII guard.  The guard keeps the shared
//! lock alive so the entry cannot be pruned while in use.
//!
//! # Cache lifecycle
//!
//! - **Hit** — `metadata.json` is present, its `content_map` matches, and
//!   `payload/` exists.  The entry's `last_used_unix_seconds` is refreshed.
//! - **Miss** — CAS bytes are fetched concurrently, extracted into a fresh
//!   `payload/` tree, and `metadata.json` is written atomically.
//! - **Expiry** — entries not used within 1 day are pruned on a best-effort
//!   basis (never blocks workflow execution).

pub(crate) mod extract;
pub(crate) mod helpers;
mod provisioner;
mod retain;
mod types;

pub use provisioner::ProvisionCache;
pub use retain::retain_only_tool_dirs;
pub use types::ProvisionedTool;

/// Per-entry payload directory name (relative to the entry directory).
pub const PAYLOAD_DIR_NAME: &str = "payload";

// Internal constants shared across sub-modules.

/// How long since last use before a provisioned entry is considered expired
/// (24 hours).
pub(crate) const TTL_SECONDS: u64 = 86_400;

/// Minimum interval between automatic prune sweeps (5 minutes).
pub(crate) const PRUNE_COOLDOWN_SECONDS: u64 = 300;

/// Marker file that records the last-prune Unix timestamp in the tools root.
pub(crate) const PRUNE_MARKER_FILE_NAME: &str = ".prune-last-used-unix-seconds";

/// Per-entry metadata file name.
pub(crate) const METADATA_FILE_NAME: &str = "metadata.json";

/// Per-entry advisory-lock file name.
pub(crate) const LOCK_FILE_NAME: &str = ".lock";

/// On-disk metadata format version.
pub(crate) const VERSION: u32 = 1;

/// Default maximum number of concurrent extraction tasks.
pub(crate) const DEFAULT_MAX_CONCURRENT: usize = 8;

/// Platform directory names that are never relevant on the current OS.
#[cfg(target_os = "macos")]
pub(crate) const FOREIGN_PLATFORM_DIRS: &[&str] = &["linux", "windows"];
#[cfg(target_os = "linux")]
pub(crate) const FOREIGN_PLATFORM_DIRS: &[&str] = &["macos", "windows"];
#[cfg(target_os = "windows")]
pub(crate) const FOREIGN_PLATFORM_DIRS: &[&str] = &["linux", "macos"];
#[cfg(not(any(target_os = "macos", target_os = "linux", target_os = "windows")))]
pub(crate) const FOREIGN_PLATFORM_DIRS: &[&str] = &[];
