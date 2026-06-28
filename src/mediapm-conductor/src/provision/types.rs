//! Shared types for the provisioning subsystem.

use std::collections::BTreeMap;
use std::path::PathBuf;
use std::sync::Arc;

use dashmap::DashMap;
use mediapm_cas::Hash;
use tokio::sync::Notify;

/// RAII guard that holds a shared advisory lock on a provisioned tool entry.
///
/// While this guard lives, the payload directory is guaranteed to exist and
/// be internally consistent (all `content_map` entries are materialized).
/// Dropping the guard releases the shared lock, allowing the entry to be
/// pruned.
///
/// This type derefs to [`std::path::Path`] for convenience.
#[derive(Debug)]
#[must_use]
pub struct ProvisionedTool {
    pub(crate) payload_dir: PathBuf,
    pub(crate) _lock_file: std::fs::File,
}

impl AsRef<std::path::Path> for ProvisionedTool {
    fn as_ref(&self) -> &std::path::Path {
        &self.payload_dir
    }
}

impl std::ops::Deref for ProvisionedTool {
    type Target = std::path::Path;

    fn deref(&self) -> &std::path::Path {
        &self.payload_dir
    }
}

/// Per-entry cached metadata.
#[derive(Debug, Clone, serde::Serialize, serde::Deserialize)]
pub(crate) struct Metadata {
    pub(crate) version: u32,
    pub(crate) content_map: BTreeMap<String, Hash>,
    #[serde(rename = "lastUsedUnixSeconds")]
    pub(crate) last_used_unix_seconds: u64,
    #[serde(default)]
    pub(crate) execute_bits_verified: bool,
}

/// Classifies a raw `content_map` key into a file or directory extraction target.
#[derive(Debug)]
pub(crate) enum ContentMapKeyKind {
    /// A regular file that should be written verbatim.
    File {
        /// Relative path under `payload/`.
        relative_path: PathBuf,
    },
    /// A ZIP archive that should be unpacked into a subdirectory.
    Directory {
        /// Relative directory under `payload/` (empty means payload root).
        relative_dir: PathBuf,
    },
}

/// Drop guard that removes a pending extraction and notifies waiters.
pub(crate) struct CleanupGuard {
    pub(crate) pending: Arc<DashMap<String, Arc<Notify>>>,
    pub(crate) key: String,
    pub(crate) notify: Arc<Notify>,
}

impl Drop for CleanupGuard {
    fn drop(&mut self) {
        self.pending.remove(&self.key);
        self.notify.notify_waiters();
    }
}
