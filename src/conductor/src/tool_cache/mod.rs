//! Persistent tool-content cache with concurrent single-flight extraction.
//!
//! Each tool that declares a `tool_content_map` gets one cache entry under
//! `<tools_dir>/<sanitized_tool_id>/`.  An entry contains:
//!
//! - `metadata.json` — version, the full `content_map` (the cache-validity
//!   key), and `last_used_unix_seconds` for TTL-based expiry.
//! - `payload/` — the fully-extracted, ready-to-execute tool content tree.
//!
//! # Single-flight extraction
//!
//! [`ToolContentCache::materialize`] deduplicates concurrent extraction of the
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
//! returning the [`ToolCacheEntry`] RAII guard.  The guard keeps the shared
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

use std::collections::{BTreeMap, HashSet};
use std::fs;
use std::path::{Component, Path, PathBuf};
use std::sync::Arc;

use dashmap::DashMap;
use futures_util::future::try_join_all;
use mediapm_cas::Hash;
use serde::{Deserialize, Serialize};
use tokio::sync::{Notify, Semaphore};

use crate::CasBound;
use crate::error::ConductorError;

// ---------------------------------------------------------------------------
// Constants
// ---------------------------------------------------------------------------

/// How long since last use before a tool-cache entry is considered expired
/// (24 hours).
const TTL_SECONDS: u64 = 86_400;

/// Minimum interval between automatic prune sweeps (5 minutes).
const PRUNE_COOLDOWN_SECONDS: u64 = 300;

/// Marker file that records the last-prune Unix timestamp in the tools root.
const PRUNE_MARKER_FILE_NAME: &str = ".prune-last-used-unix-seconds";

/// Per-entry metadata file name.
const METADATA_FILE_NAME: &str = "metadata.json";

/// Per-entry advisory-lock file name.
const LOCK_FILE_NAME: &str = ".lock";

/// Cache entry format version.
const VERSION: u32 = 1;

/// Default maximum number of concurrent extraction tasks.
const DEFAULT_MAX_CONCURRENT: usize = 8;

// ---------------------------------------------------------------------------
// Public items
// ---------------------------------------------------------------------------

/// Per-entry payload directory name (relative to the entry directory).
///
/// Exposed so callers that skip [`ToolContentCache::link_to_sandbox`] can
/// construct the payload path themselves.
pub const PAYLOAD_DIR_NAME: &str = "payload";

/// RAII guard that holds a shared advisory lock on a tool-cache entry.
///
/// While this guard lives, the payload directory is guaranteed to exist and
/// be internally consistent (all `content_map` entries are materialized).
/// Dropping the guard releases the shared lock, allowing the entry to be
/// pruned.
///
/// This type derefs to [`Path`] (the payload directory) for convenience.
#[derive(Debug)]
#[must_use]
pub struct ToolCacheEntry {
    payload_dir: PathBuf,
    _lock_file: std::fs::File,
}

impl AsRef<Path> for ToolCacheEntry {
    fn as_ref(&self) -> &Path {
        &self.payload_dir
    }
}

impl std::ops::Deref for ToolCacheEntry {
    type Target = Path;

    fn deref(&self) -> &Path {
        &self.payload_dir
    }
}

impl ToolCacheEntry {
    /// Returns the payload directory path.
    #[must_use]
    pub fn payload_dir(&self) -> &Path {
        &self.payload_dir
    }
}

// ---------------------------------------------------------------------------
// Private types
// ---------------------------------------------------------------------------

/// Per-entry cached metadata.
#[derive(Debug, Clone, Serialize, Deserialize)]
struct Metadata {
    version: u32,
    content_map: BTreeMap<String, Hash>,
    #[serde(rename = "lastUsedUnixSeconds")]
    last_used_unix_seconds: u64,
    #[serde(default)]
    execute_bits_verified: bool,
}

/// Classifies a raw `content_map` key into a file or directory extraction target.
#[derive(Debug)]
enum ContentMapKeyKind {
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
///
/// Ensures cleanup happens even when the extraction future panics, so waiting
/// tasks don't block forever.
struct CleanupGuard {
    pending: Arc<DashMap<String, Arc<Notify>>>,
    key: String,
    notify: Arc<Notify>,
}

impl Drop for CleanupGuard {
    fn drop(&mut self) {
        self.pending.remove(&self.key);
        self.notify.notify_waiters();
    }
}

// ---------------------------------------------------------------------------
// Main struct
// ---------------------------------------------------------------------------

/// A persistent tool-content cache with CAS-backed materialization and
/// concurrent single-flight extraction deduplication.
///
/// # Type parameters
///
/// * `C` — The CAS backend (must implement [`CasApi`]).
pub struct ToolContentCache<C> {
    /// Root directory under which per-tool cache entries live.
    tools_dir: PathBuf,
    /// CAS client used to fetch payload bytes.
    cas: Arc<C>,
    /// Map from sanitized tool id to notification handle for in-flight
    /// extractions.  Used to implement single-flight deduplication.
    pending: Arc<DashMap<String, Arc<Notify>>>,
    /// Global concurrency limiter for extraction tasks.
    semaphore: Arc<Semaphore>,
}

impl<C: std::fmt::Debug> std::fmt::Debug for ToolContentCache<C> {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("ToolContentCache")
            .field("tools_dir", &self.tools_dir)
            .field("pending", &self.pending)
            .field("cas", &self.cas)
            .field("semaphore", &self.semaphore)
            .finish()
    }
}

/// Removes all tool cache directories under `tools_dir` whose sanitized names
/// are not present in `active_tool_ids`.
///
/// This is the free-function counterpart of [`ToolContentCache::retain_only`],
/// usable without constructing a [`ToolContentCache`].  Only entries without
/// active locks are removed; in-use entries are preserved even if they are not
/// in the active set.
///
/// # Errors
///
/// Returns [`ConductorError`] on I/O errors.
pub async fn retain_only_tool_dirs<S: std::hash::BuildHasher>(
    tools_dir: PathBuf,
    active_tool_ids: HashSet<String, S>,
) -> Result<(), ConductorError> {
    let active: HashSet<String> =
        active_tool_ids.into_iter().map(|id| sanitize_tool_id(&id)).collect();
    tokio::task::spawn_blocking(move || do_retain_only(&tools_dir, &active)).await.map_err(
        |join_err| ConductorError::Internal(format!("retain-only task panicked: {join_err}")),
    )?
}

impl<C: CasBound> ToolContentCache<C> {
    /// Creates a new tool-content cache rooted at `tools_dir`.
    ///
    /// # Arguments
    ///
    /// * `tools_dir` — Directory under which per-tool cache entries are stored.
    /// * `cas` — CAS client for fetching payload bytes.
    /// * `max_concurrent` — Maximum number of concurrent extraction tasks
    ///   (`None` uses a reasonable default).
    #[must_use]
    pub fn new(tools_dir: PathBuf, cas: Arc<C>, max_concurrent: Option<usize>) -> Self {
        Self {
            tools_dir,
            cas,
            pending: Arc::new(DashMap::new()),
            semaphore: Arc::new(Semaphore::new(max_concurrent.unwrap_or(DEFAULT_MAX_CONCURRENT))),
        }
    }

    // -----------------------------------------------------------------------
    // Public API
    // -----------------------------------------------------------------------

    /// Returns a cache entry for `tool_id`, fetching and extracting payloads
    /// from CAS on cache miss.
    ///
    /// The returned [`ToolCacheEntry`] holds a shared advisory lock on the
    /// entry, keeping it alive until the guard is dropped.
    ///
    /// Concurrent calls for the same `tool_id` are deduplicated:
    /// only the first caller drives extraction; others wait on a notification
    /// and retry the fast path.
    ///
    /// # Errors
    ///
    /// Returns [`ConductorError`] on CAS failures, I/O errors, or content-map
    /// validation failures.
    pub async fn materialize(
        &self,
        tool_id: &str,
        content_map: &BTreeMap<String, Hash>,
    ) -> Result<ToolCacheEntry, ConductorError> {
        let sanitized = sanitize_tool_id(tool_id);
        let now = now_unix_seconds();

        // Best-effort expiry pruning (cooldown-gated, errors swallowed).
        self.try_maybe_prune(now).await;

        let entry_dir = self.tools_dir.join(&sanitized);
        let payload_dir = entry_dir.join(PAYLOAD_DIR_NAME);
        let metadata_path = entry_dir.join(METADATA_FILE_NAME);
        let lock_path = entry_dir.join(LOCK_FILE_NAME);

        // Retry loop: after waiting for another task's extraction we loop
        // back and re-check the fast path.
        loop {
            // Fast path: non-blocking shared lock + freshness check.
            if let Some(guard) = Self::try_lock_fast_path(
                &entry_dir,
                &lock_path,
                &payload_dir,
                &metadata_path,
                content_map,
                now,
            )? {
                return Ok(guard);
            }

            // Single-flight: check whether another task is already extracting
            // this tool.
            let pending_entry = self.pending.entry(sanitized.clone());
            match pending_entry {
                dashmap::mapref::entry::Entry::Occupied(occupied) => {
                    // Another task is extracting — wait for notification,
                    // then retry the fast path.
                    let notify = occupied.get().clone();
                    drop(occupied); // Release the DashMap shard lock.
                    notify.notified().await;
                }
                dashmap::mapref::entry::Entry::Vacant(vacant) => {
                    // We are the first caller for this tool.
                    let notify = Arc::new(Notify::new());
                    let cleanup_notify = notify.clone();
                    vacant.insert(notify); // Inserts and releases the shard lock.

                    let _cleanup = CleanupGuard {
                        pending: Arc::clone(&self.pending),
                        key: sanitized.clone(),
                        notify: cleanup_notify,
                    };

                    // Acquire a global concurrency permit so we don't
                    // overwhelm the system with concurrent extractions.
                    let _permit = Arc::clone(&self.semaphore)
                        .acquire_owned()
                        .await
                        .map_err(|_| ConductorError::Internal("semaphore closed".to_string()))?;

                    return self
                        .do_extract(
                            content_map,
                            &entry_dir,
                            &payload_dir,
                            &metadata_path,
                            &lock_path,
                        )
                        .await;
                }
            }
        }
    }

    /// Hard-links (or copies as fallback) all files from a cached `payload/`
    /// directory into an execution sandbox directory.
    ///
    /// Because all steps that use the same conductor tool share a single
    /// `payload/` tree, this is the only step-specific operation: files are
    /// linked into the per-step sandbox without re-extracting or copying bytes.
    ///
    /// Hard links are attempted first (near-zero-cost metadata operation).  If
    /// the link fails (e.g. cross-device), a byte-for-byte copy with
    /// permission preservation is used as fallback.
    ///
    /// Top-level subdirectories whose name matches a known foreign-platform
    /// identifier are skipped so platform-foreign binaries are never linked.
    ///
    /// # Errors
    ///
    /// Returns a descriptive `String` on failure.  Callers should wrap this
    /// into an appropriate [`ConductorError`] variant.
    pub fn link_to_sandbox(payload_dir: &Path, sandbox_dir: &Path) -> Result<(), String> {
        if !payload_dir.exists() {
            return Err(format!(
                "tool-content cache payload directory '{}' does not exist",
                payload_dir.display()
            ));
        }

        fs::create_dir_all(sandbox_dir).map_err(|err| {
            format!("creating sandbox directory '{}' failed: {err}", sandbox_dir.display())
        })?;

        let entries = fs::read_dir(payload_dir).map_err(|err| {
            format!("reading payload directory '{}' failed: {err}", payload_dir.display())
        })?;

        for entry in entries {
            let entry =
                entry.map_err(|err| format!("reading payload directory entry failed: {err}"))?;
            let file_name = entry.file_name();
            let name = file_name.to_string_lossy();
            let path = entry.path();
            let target_path = sandbox_dir.join(&file_name);

            let file_type = entry.file_type().map_err(|err| {
                format!("reading file type for '{}' failed: {err}", path.display())
            })?;

            // Skip top-level directories that belong to a foreign platform.
            if file_type.is_dir() && FOREIGN_PLATFORM_DIRS.contains(&name.as_ref()) {
                continue;
            }

            if file_type.is_dir() {
                copy_directory_recursive(&path, &target_path)?;
            } else {
                if let Some(parent) = target_path.parent() {
                    fs::create_dir_all(parent).map_err(|err| {
                        format!("creating parent directory '{}' failed: {err}", parent.display())
                    })?;
                }
                if fs::hard_link(&path, &target_path).is_ok() {
                    continue;
                }
                fs::copy(&path, &target_path).map_err(|err| {
                    format!(
                        "copying '{}' to '{}' failed: {err}",
                        path.display(),
                        target_path.display()
                    )
                })?;
                let source_permissions = fs::metadata(&path)
                    .map_err(|err| {
                        format!("reading permissions for '{}' failed: {err}", path.display())
                    })?
                    .permissions();
                fs::set_permissions(&target_path, source_permissions).map_err(|err| {
                    format!("setting permissions on '{}' failed: {err}", target_path.display())
                })?;
            }
        }

        Ok(())
    }

    /// Removes tool-cache entries that have not been used within the TTL.
    ///
    /// Pruning is cooldown-gated: a marker file records the last prune time
    /// and subsequent calls within `PRUNE_COOLDOWN_SECONDS` are no-ops.
    ///
    /// Only entries that are not currently locked (shared or exclusive) are
    /// removed — active entries are always protected.
    ///
    /// # Errors
    ///
    /// Returns [`ConductorError`] on I/O errors.
    pub async fn prune(&self) -> Result<(), ConductorError> {
        let now = now_unix_seconds();
        self.prune_internal(now).await
    }

    /// Removes all tool-cache entries except those in `active_tool_ids`.
    ///
    /// This is useful before a workflow run to reclaim space from tools that
    /// are no longer referenced by the current configuration.
    ///
    /// Only entries without active locks are removed; in-use entries are
    /// preserved even if they are not in the active set.
    ///
    /// # Errors
    ///
    /// Returns [`ConductorError`] on I/O errors.
    pub async fn retain_only(
        &self,
        active_tool_ids: &HashSet<String>,
    ) -> Result<(), ConductorError> {
        retain_only_tool_dirs(self.tools_dir.clone(), active_tool_ids.clone()).await
    }

    // -----------------------------------------------------------------------
    // Private methods
    // -----------------------------------------------------------------------

    /// Best-effort prune that never blocks workflow execution.
    async fn try_maybe_prune(&self, now: u64) {
        let marker_path = self.tools_dir.join(PRUNE_MARKER_FILE_NAME);

        // Synchronous cooldown check (single file read — fast enough for an
        // async context).
        if let Ok(raw) = fs::read_to_string(&marker_path)
            && let Ok(last) = raw.trim().parse::<u64>()
            && now.saturating_sub(last) < PRUNE_COOLDOWN_SECONDS
        {
            return;
        }

        // Spawn blocking for the full prune sweep.
        let tools_dir = self.tools_dir.clone();
        let _ = tokio::task::spawn_blocking(move || -> Result<(), ConductorError> {
            let _ = fs::create_dir_all(&tools_dir);
            let _ = fs::write(&marker_path, format!("{now}\n"));
            prune_expired_entries(&tools_dir, now)
        })
        .await;
    }

    /// Prunes expired entries unconditionally (no cooldown check).
    async fn prune_internal(&self, now: u64) -> Result<(), ConductorError> {
        let tools_dir = self.tools_dir.clone();
        let marker_path = tools_dir.join(PRUNE_MARKER_FILE_NAME);

        tokio::task::spawn_blocking(move || -> Result<(), ConductorError> {
            let _ = fs::create_dir_all(&tools_dir);
            let _ = fs::write(&marker_path, format!("{now}\n"));
            prune_expired_entries(&tools_dir, now)
        })
        .await
        .map_err(|join_err| ConductorError::Internal(format!("prune task panicked: {join_err}")))?
    }

    /// Runs the full extraction pipeline: CAS pre-fetch + blocking I/O.
    async fn do_extract(
        &self,
        content_map: &BTreeMap<String, Hash>,
        entry_dir: &Path,
        payload_dir: &Path,
        metadata_path: &Path,
        lock_path: &Path,
    ) -> Result<ToolCacheEntry, ConductorError> {
        // Phase 1 — classify keys and fetch all CAS bytes concurrently.
        let entries_with_bytes = fetch_all_cas_entries(Arc::clone(&self.cas), content_map).await?;

        // Phase 2 — blocking I/O: extraction, metadata, lock downgrade.
        let entry_dir = entry_dir.to_path_buf();
        let payload_dir = payload_dir.to_path_buf();
        let metadata_path = metadata_path.to_path_buf();
        let lock_path = lock_path.to_path_buf();
        let content_map = content_map.clone();

        tokio::task::spawn_blocking(move || {
            extract_sync(
                &entry_dir,
                &lock_path,
                &payload_dir,
                &metadata_path,
                &content_map,
                entries_with_bytes,
            )
        })
        .await
        .map_err(|join_err| {
            ConductorError::Internal(format!("extraction task panicked: {join_err}"))
        })?
    }

    /// Attempts a non-blocking shared lock and cache-hit check on one entry.
    ///
    /// Returns `Ok(Some(guard))` on cache hit (shared lock acquired, metadata
    /// valid, payload exists).  Returns `Ok(None)` when the entry does not
    /// exist, the lock file is missing, the lock is held exclusively, or the
    /// entry data is stale.  Returns `Err` on actual I/O errors.
    #[allow(clippy::too_many_arguments)]
    fn try_lock_fast_path(
        _entry_dir: &Path,
        lock_path: &Path,
        payload_dir: &Path,
        metadata_path: &Path,
        content_map: &BTreeMap<String, Hash>,
        now: u64,
    ) -> Result<Option<ToolCacheEntry>, ConductorError> {
        // Fast bail-out when no entry exists at all.
        if !lock_path.exists() {
            return Ok(None);
        }

        let Ok(lock_file) = std::fs::OpenOptions::new().read(true).write(true).open(lock_path)
        else {
            return Ok(None);
        };

        match lock_file.try_lock_shared() {
            Ok(()) => {
                // Shared lock acquired — check cache validity.
                if !payload_dir.is_dir() {
                    return Ok(None);
                }
                let Ok(raw) = fs::read_to_string(metadata_path) else {
                    return Ok(None);
                };
                let Ok(metadata) = serde_json::from_str::<Metadata>(&raw) else {
                    return Ok(None);
                };
                if metadata.version != VERSION {
                    return Ok(None);
                }
                if &metadata.content_map != content_map {
                    return Ok(None);
                }

                // Cache hit!
                if !metadata.execute_bits_verified {
                    ensure_payload_tree_user_execute_bits(payload_dir)?;
                }
                let _ = persist_cache_metadata(
                    metadata_path,
                    &Metadata {
                        version: VERSION,
                        content_map: content_map.clone(),
                        last_used_unix_seconds: now,
                        execute_bits_verified: true,
                    },
                );
                Ok(Some(ToolCacheEntry {
                    payload_dir: payload_dir.to_path_buf(),
                    _lock_file: lock_file,
                }))
            }
            Err(std::fs::TryLockError::WouldBlock) => {
                // Another worker holds the exclusive lock (extraction in
                // progress).
                Ok(None)
            }
            Err(std::fs::TryLockError::Error(e)) => Err(ConductorError::Io {
                operation: "trying shared lock on tool-content cache entry".to_string(),
                path: lock_path.to_path_buf(),
                source: e,
            }),
        }
    }
}

// ---------------------------------------------------------------------------
// Top-level extraction logic (runs inside spawn_blocking)
// ---------------------------------------------------------------------------

/// Synchronous extraction: acquires exclusive lock, extracts payloads, writes
/// metadata, then downgrades to a shared lock.
#[allow(clippy::too_many_arguments, clippy::needless_pass_by_value, clippy::too_many_lines)]
fn extract_sync(
    entry_dir: &Path,
    lock_path: &Path,
    payload_dir: &Path,
    metadata_path: &Path,
    content_map: &BTreeMap<String, Hash>,
    entries_with_bytes: Vec<(String, ContentMapKeyKind, Vec<u8>)>,
) -> Result<ToolCacheEntry, ConductorError> {
    let now = now_unix_seconds();

    // Ensure the entry directory exists before opening the lock file.
    fs::create_dir_all(entry_dir).map_err(|source| ConductorError::Io {
        operation: "creating tool-content cache entry directory".to_string(),
        path: entry_dir.to_path_buf(),
        source,
    })?;

    // Open/create the lock file and acquire an exclusive (write) lock.
    // This blocks until any other worker's extraction finishes.
    let excl_file = std::fs::OpenOptions::new()
        .create(true)
        .read(true)
        .write(true)
        .truncate(false)
        .open(lock_path)
        .map_err(|source| ConductorError::Io {
            operation: "opening tool-content cache lock file".to_string(),
            path: lock_path.to_path_buf(),
            source,
        })?;
    excl_file.lock().map_err(|source| ConductorError::Io {
        operation: "acquiring exclusive lock on tool-content cache entry".to_string(),
        path: lock_path.to_path_buf(),
        source,
    })?;

    // Double-check: another process may have populated the entry while we
    // were waiting for the exclusive lock.
    if let Some(guard) =
        double_check_hit(payload_dir, metadata_path, lock_path, &excl_file, content_map, now)?
    {
        drop(excl_file);
        return Ok(guard);
    }

    // --- Cache miss: build collision map ---

    // Phase 1: build a set of every file path that will be written.
    // Overlapping paths across distinct content-map entries are rejected
    // before any extraction starts.
    let mut claimed: BTreeMap<PathBuf, String> = BTreeMap::new();

    for (key, kind, bytes) in &entries_with_bytes {
        match kind {
            ContentMapKeyKind::File { relative_path } => {
                if let Some(prev) = claimed.insert(relative_path.clone(), key.clone()) {
                    return Err(ConductorError::Workflow(format!(
                        "tool content map entries '{prev}' and '{key}' both materialize \
                         '{}' and would overwrite each other",
                        relative_path.display()
                    )));
                }
            }
            ContentMapKeyKind::Directory { relative_dir } => {
                let members = mediapm_conductor_builtin_archive::list_zip_member_file_paths(bytes)
                    .map_err(|err| {
                        ConductorError::Workflow(format!(
                            "tool content map directory key '{key}' expects ZIP payload, \
                                 but member listing failed: {err}"
                        ))
                    })?;
                for member in members {
                    let full_path = if relative_dir.as_os_str().is_empty() {
                        PathBuf::from(&member)
                    } else {
                        relative_dir.join(&member)
                    };
                    if let Some(prev) = claimed.insert(full_path.clone(), key.clone()) {
                        return Err(ConductorError::Workflow(format!(
                            "tool content map entries '{prev}' and '{key}' both materialize \
                             '{}' and would overwrite each other",
                            full_path.display()
                        )));
                    }
                }
            }
        }
    }

    // Phase 2: remove stale entry, create fresh payload dir, extract.
    if entry_dir.exists() {
        fs::remove_dir_all(entry_dir).map_err(|source| ConductorError::Io {
            operation: "removing stale tool-content cache entry".to_string(),
            path: entry_dir.to_path_buf(),
            source,
        })?;
    }
    fs::create_dir_all(payload_dir).map_err(|source| ConductorError::Io {
        operation: "creating tool-content cache payload directory".to_string(),
        path: payload_dir.to_path_buf(),
        source,
    })?;

    for (key, kind, bytes) in &entries_with_bytes {
        match kind {
            ContentMapKeyKind::File { relative_path } => {
                let target_path = payload_dir.join(relative_path);
                if let Some(parent) = target_path.parent() {
                    fs::create_dir_all(parent).map_err(|source| ConductorError::Io {
                        operation: "creating tool-content file parent directories".to_string(),
                        path: parent.to_path_buf(),
                        source,
                    })?;
                }
                fs::write(&target_path, bytes).map_err(|source| ConductorError::Io {
                    operation: "writing tool-content file to cache payload".to_string(),
                    path: target_path.clone(),
                    source,
                })?;
                ensure_user_execute_bit(&target_path)?;
            }
            ContentMapKeyKind::Directory { relative_dir } => {
                let unpack_dir = if relative_dir.as_os_str().is_empty() {
                    payload_dir.to_path_buf()
                } else {
                    payload_dir.join(relative_dir)
                };
                mediapm_conductor_builtin_archive::unpack_zip_bytes_to_directory(
                    bytes,
                    &unpack_dir,
                )
                .map_err(|err| {
                    ConductorError::Workflow(format!(
                        "tool content map directory key '{key}' expects ZIP payload, \
                         but unpack failed: {err}"
                    ))
                })?;
            }
        }
    }

    ensure_payload_tree_user_execute_bits(payload_dir)?;

    // Phase 3: recreate the lock file (removed by remove_dir_all above) and
    // persist metadata atomically.
    std::fs::OpenOptions::new()
        .create(true)
        .read(true)
        .write(true)
        .truncate(true)
        .open(lock_path)
        .map_err(|source| ConductorError::Io {
            operation: "recreating tool-content cache lock file after extraction".to_string(),
            path: lock_path.to_path_buf(),
            source,
        })?;

    persist_cache_metadata(
        metadata_path,
        &Metadata {
            version: VERSION,
            content_map: content_map.clone(),
            last_used_unix_seconds: now,
            execute_bits_verified: true,
        },
    )?;

    // Acquire a shared lock on the new lock file while still holding the
    // exclusive lock on the old inode (orphaned by remove_dir_all).
    let shared_lock = std::fs::OpenOptions::new()
        .create(true)
        .read(true)
        .write(true)
        .truncate(true)
        .open(lock_path)
        .map_err(|source| ConductorError::Io {
            operation: "opening new tool-content cache lock file for shared lock".to_string(),
            path: lock_path.to_path_buf(),
            source,
        })?;
    shared_lock.lock_shared().map_err(|source| ConductorError::Io {
        operation: "acquiring shared lock on new tool-content cache entry".to_string(),
        path: lock_path.to_path_buf(),
        source,
    })?;

    // Drop the exclusive lock fd — the old inode is now orphaned and the new
    // .lock file holds the shared lock via shared_lock.
    drop(excl_file);

    Ok(ToolCacheEntry { payload_dir: payload_dir.to_path_buf(), _lock_file: shared_lock })
}

/// Checks whether another process completed extraction while we waited for
/// the exclusive lock.  If so, downgrades and returns a guard.
fn double_check_hit(
    payload_dir: &Path,
    metadata_path: &Path,
    lock_path: &Path,
    excl_file: &std::fs::File,
    content_map: &BTreeMap<String, Hash>,
    now: u64,
) -> Result<Option<ToolCacheEntry>, ConductorError> {
    if !payload_dir.is_dir() {
        return Ok(None);
    }
    let Ok(raw) = fs::read_to_string(metadata_path) else {
        return Ok(None);
    };
    let Ok(metadata) = serde_json::from_str::<Metadata>(&raw) else {
        return Ok(None);
    };
    if metadata.version != VERSION || metadata.content_map != *content_map {
        return Ok(None);
    }

    // Double-check hit — another worker finished extraction while we waited.
    if !metadata.execute_bits_verified {
        ensure_payload_tree_user_execute_bits(payload_dir)?;
    }
    let _ = persist_cache_metadata(
        metadata_path,
        &Metadata {
            version: VERSION,
            content_map: content_map.clone(),
            last_used_unix_seconds: now,
            execute_bits_verified: true,
        },
    );
    // Clone the exclusive-lock fd and downgrade to shared (avoids
    // per-inode per-process self-deadlock on macOS).
    let shared_file = excl_file.try_clone().map_err(|source| ConductorError::Io {
        operation: "duplicating lock fd for shared downgrade".to_string(),
        path: lock_path.to_path_buf(),
        source,
    })?;
    shared_file.lock_shared().map_err(|source| ConductorError::Io {
        operation: "downgrading exclusive lock to shared after double-check hit".to_string(),
        path: lock_path.to_path_buf(),
        source,
    })?;
    Ok(Some(ToolCacheEntry { payload_dir: payload_dir.to_path_buf(), _lock_file: shared_file }))
}

// ---------------------------------------------------------------------------
// CAS pre-fetch
// ---------------------------------------------------------------------------

/// Fetches all bytes referenced by `content_map` from the CAS concurrently.
async fn fetch_all_cas_entries<C: CasBound>(
    cas: Arc<C>,
    content_map: &BTreeMap<String, Hash>,
) -> Result<Vec<(String, ContentMapKeyKind, Vec<u8>)>, ConductorError> {
    let classified: Vec<(String, Hash, ContentMapKeyKind)> = content_map
        .iter()
        .map(|(key, hash)| {
            let kind = classify_content_map_key(key)?;
            Ok::<_, ConductorError>((key.clone(), *hash, kind))
        })
        .collect::<Result<Vec<_>, _>>()?;

    try_join_all(classified.into_iter().map(|(key, hash, kind)| {
        let cas = Arc::clone(&cas);
        async move {
            let bytes = cas.get(hash).await.map_err(ConductorError::Cas)?;
            Ok::<_, ConductorError>((key, kind, bytes.to_vec()))
        }
    }))
    .await
}

// ---------------------------------------------------------------------------
// Retain-only helper
// ---------------------------------------------------------------------------

/// Removes all entry directories in `tools_dir` whose name is not in
/// `active_sanitized`.
fn do_retain_only(
    tools_dir: &Path,
    active_sanitized: &HashSet<String>,
) -> Result<(), ConductorError> {
    if !tools_dir.exists() {
        return Ok(());
    }
    for entry in fs::read_dir(tools_dir).map_err(|source| ConductorError::Io {
        operation: "enumerating tool-content cache root for retain-only".to_string(),
        path: tools_dir.to_path_buf(),
        source,
    })? {
        let entry = entry.map_err(|source| ConductorError::Io {
            operation: "reading tool-content cache directory entry".to_string(),
            path: tools_dir.to_path_buf(),
            source,
        })?;
        let path = entry.path();
        let Ok(dir_name) = entry.file_name().into_string() else {
            continue;
        };
        if !entry.file_type().is_ok_and(|ty| ty.is_dir()) {
            continue;
        }
        if active_sanitized.contains(&dir_name) {
            continue;
        }

        // Try to acquire an exclusive lock non-blockingly.  If the entry is
        // in use, skip it.
        let lock_path = path.join(LOCK_FILE_NAME);
        let Ok(lock_file) = std::fs::OpenOptions::new().read(true).write(true).open(&lock_path)
        else {
            continue;
        };
        if lock_file.try_lock().is_err() {
            continue;
        }
        let _ = fs::remove_dir_all(&path);
    }
    Ok(())
}

// ---------------------------------------------------------------------------
// Prune helper
// ---------------------------------------------------------------------------

/// Removes tool-content cache entries that have not been used within `TTL_SECONDS`.
fn prune_expired_entries(tools_dir: &Path, now: u64) -> Result<(), ConductorError> {
    if !tools_dir.exists() {
        return Ok(());
    }

    let cutoff = now.saturating_sub(TTL_SECONDS);

    let entries = fs::read_dir(tools_dir).map_err(|source| ConductorError::Io {
        operation: "enumerating tool-content cache root".to_string(),
        path: tools_dir.to_path_buf(),
        source,
    })?;

    for entry in entries {
        let entry = entry.map_err(|source| ConductorError::Io {
            operation: "reading tool-content cache directory entry".to_string(),
            path: tools_dir.to_path_buf(),
            source,
        })?;
        let path = entry.path();
        if !entry.file_type().is_ok_and(|ty| ty.is_dir()) {
            continue;
        }

        let metadata_path = path.join(METADATA_FILE_NAME);
        let Ok(raw) = fs::read_to_string(&metadata_path) else {
            continue;
        };
        let Ok(metadata) = serde_json::from_str::<Metadata>(&raw) else {
            continue;
        };
        if metadata.version != VERSION {
            continue;
        }
        if metadata.last_used_unix_seconds > cutoff {
            continue;
        }

        // Try to acquire an exclusive lock non-blockingly.  If another worker
        // is using this entry (shared or exclusive lock held), skip it.
        let lock_path = path.join(LOCK_FILE_NAME);
        let Ok(lock_file) = std::fs::OpenOptions::new().read(true).write(true).open(&lock_path)
        else {
            continue;
        };
        if lock_file.try_lock().is_err() {
            continue;
        }

        let _ = fs::remove_dir_all(&path);
    }

    Ok(())
}

// ---------------------------------------------------------------------------
// Free helper functions
// ---------------------------------------------------------------------------

/// Returns a filesystem-safe directory name from a tool identifier.
///
/// Characters unsafe on major platforms are replaced with `_`.
#[must_use]
pub fn sanitize_tool_id(tool_id: &str) -> String {
    tool_id
        .chars()
        .map(|ch| {
            if matches!(ch, '/' | '\\' | ':' | '?' | '*' | '<' | '>' | '|' | '"') {
                '_'
            } else {
                ch
            }
        })
        .collect()
}

/// Classifies one raw `content_map` key into a file or directory extraction
/// target.
///
/// Rules:
/// - Keys ending with `/` or `\` are directory ZIP targets.
/// - The special `./` (or `.\`) key unpacks a ZIP directly at `payload/` root.
/// - All other keys are regular file targets.
fn classify_content_map_key(raw: &str) -> Result<ContentMapKeyKind, ConductorError> {
    if raw.ends_with('/') || raw.ends_with('\\') {
        let trimmed = raw.trim_end_matches(['/', '\\']);
        if trimmed == "." {
            // `./` or `.\` — unpack ZIP at payload root.
            return Ok(ContentMapKeyKind::Directory { relative_dir: PathBuf::new() });
        }
        if trimmed.trim().is_empty() {
            return Err(ConductorError::Workflow(format!(
                "tool content map directory key '{raw}' must contain at least one path \
                 component before trailing slash"
            )));
        }
        let relative_dir = normalize_sandbox_relative_path(trimmed, raw)?;
        return Ok(ContentMapKeyKind::Directory { relative_dir });
    }

    let relative_path = normalize_sandbox_relative_path(raw, raw)?;
    Ok(ContentMapKeyKind::File { relative_path })
}

/// Normalizes and validates one sandbox-relative path string.
///
/// `raw` is the path string to normalize; `context_key` is included in error
/// messages for diagnostic clarity.
fn normalize_sandbox_relative_path(
    raw: &str,
    context_key: &str,
) -> Result<PathBuf, ConductorError> {
    if raw.trim().is_empty() {
        return Err(ConductorError::Workflow(format!(
            "tool content map key '{context_key}' path must be non-empty"
        )));
    }
    let parsed = Path::new(raw);
    if parsed.is_absolute() {
        return Err(ConductorError::Workflow(format!(
            "tool content map key '{context_key}' path must be relative"
        )));
    }
    let mut normalized = PathBuf::new();
    for component in parsed.components() {
        match component {
            Component::Normal(part) => normalized.push(part),
            Component::CurDir => {}
            Component::ParentDir | Component::RootDir | Component::Prefix(_) => {
                return Err(ConductorError::Workflow(format!(
                    "tool content map key '{context_key}' must not escape the tool sandbox"
                )));
            }
        }
    }
    if normalized.as_os_str().is_empty() {
        return Err(ConductorError::Workflow(format!(
            "tool content map key '{context_key}' must contain a concrete path component"
        )));
    }
    Ok(normalized)
}

/// Ensures one extracted payload file is executable by the current user.
///
/// On Unix hosts this sets `u+x` while preserving all existing permission
/// bits. Non-Unix hosts leave permissions unchanged.
fn ensure_user_execute_bit(path: &Path) -> Result<(), ConductorError> {
    #[cfg(unix)]
    {
        use std::os::unix::fs::PermissionsExt;

        let metadata = fs::metadata(path).map_err(|source| ConductorError::Io {
            operation: "reading extracted tool-content file permissions".to_string(),
            path: path.to_path_buf(),
            source,
        })?;
        let mut permissions = metadata.permissions();
        let mode = permissions.mode();
        if mode & 0o100 == 0 {
            permissions.set_mode(mode | 0o100);
            fs::set_permissions(path, permissions).map_err(|source| ConductorError::Io {
                operation: "marking extracted tool-content file executable".to_string(),
                path: path.to_path_buf(),
                source,
            })?;
        }
    }

    Ok(())
}

/// Recursively ensures all regular files under one payload tree have owner
/// execute permissions so bundled companion binaries remain runnable.
fn ensure_payload_tree_user_execute_bits(root: &Path) -> Result<(), ConductorError> {
    if !root.exists() {
        return Ok(());
    }

    #[cfg(unix)]
    {
        let entries = fs::read_dir(root).map_err(|source| ConductorError::Io {
            operation: "enumerating tool-content payload tree for permission refresh".to_string(),
            path: root.to_path_buf(),
            source,
        })?;

        for entry in entries {
            let entry = entry.map_err(|source| ConductorError::Io {
                operation: "reading tool-content payload directory entry".to_string(),
                path: root.to_path_buf(),
                source,
            })?;
            let path = entry.path();
            let file_type = entry.file_type().map_err(|source| ConductorError::Io {
                operation: "reading tool-content payload entry file type".to_string(),
                path: path.clone(),
                source,
            })?;

            if file_type.is_dir() {
                ensure_payload_tree_user_execute_bits(&path)?;
            } else if file_type.is_file() {
                ensure_user_execute_bit(&path)?;
            }
        }
    }

    Ok(())
}

/// Writes cache metadata atomically via a temporary-file rename.
fn persist_cache_metadata(metadata_path: &Path, metadata: &Metadata) -> Result<(), ConductorError> {
    if let Some(parent) = metadata_path.parent() {
        fs::create_dir_all(parent).map_err(|source| ConductorError::Io {
            operation: "creating tool-content cache metadata parent directory".to_string(),
            path: parent.to_path_buf(),
            source,
        })?;
    }

    let rendered = serde_json::to_string_pretty(metadata).map_err(|error| {
        ConductorError::Serialization(format!("encoding tool-content cache metadata: {error}"))
    })?;
    let temp_path = metadata_path.with_extension("json.tmp");
    fs::write(&temp_path, format!("{rendered}\n")).map_err(|source| ConductorError::Io {
        operation: "writing temporary tool-content cache metadata".to_string(),
        path: temp_path.clone(),
        source,
    })?;
    if metadata_path.exists() {
        let _ = fs::remove_file(metadata_path);
    }
    fs::rename(&temp_path, metadata_path).map_err(|source| ConductorError::Io {
        operation: "replacing tool-content cache metadata".to_string(),
        path: metadata_path.to_path_buf(),
        source,
    })
}

/// Copies (preferring hard links) all files from `source_dir` into `target_dir`.
fn copy_directory_recursive(source_dir: &Path, target_dir: &Path) -> Result<(), String> {
    fs::create_dir_all(target_dir)
        .map_err(|err| format!("creating destination '{}' failed: {err}", target_dir.display()))?;

    let entries = fs::read_dir(source_dir).map_err(|err| {
        format!("reading source directory '{}' failed: {err}", source_dir.display())
    })?;

    for entry in entries {
        let entry = entry.map_err(|err| format!("reading source directory entry failed: {err}"))?;
        let path = entry.path();
        let target_path = target_dir.join(entry.file_name());
        let file_type = entry
            .file_type()
            .map_err(|err| format!("reading file type for '{}' failed: {err}", path.display()))?;

        if file_type.is_dir() {
            copy_directory_recursive(&path, &target_path)?;
            continue;
        }

        if let Some(parent) = target_path.parent() {
            fs::create_dir_all(parent).map_err(|err| {
                format!("creating parent directory '{}' failed: {err}", parent.display())
            })?;
        }

        if fs::hard_link(&path, &target_path).is_ok() {
            continue;
        }

        fs::copy(&path, &target_path).map_err(|err| {
            format!("copying '{}' to '{}' failed: {err}", path.display(), target_path.display())
        })?;

        let source_permissions = fs::metadata(&path)
            .map_err(|err| format!("reading permissions for '{}' failed: {err}", path.display()))?
            .permissions();
        fs::set_permissions(&target_path, source_permissions).map_err(|err| {
            format!("setting permissions on '{}' failed: {err}", target_path.display())
        })?;
    }

    Ok(())
}

/// Returns the current Unix timestamp in whole seconds.
#[must_use]
fn now_unix_seconds() -> u64 {
    std::time::SystemTime::now().duration_since(std::time::UNIX_EPOCH).unwrap_or_default().as_secs()
}

// Platform directory names that are never relevant on the current OS.
#[cfg(target_os = "macos")]
const FOREIGN_PLATFORM_DIRS: &[&str] = &["linux", "windows"];
#[cfg(target_os = "linux")]
const FOREIGN_PLATFORM_DIRS: &[&str] = &["macos", "windows"];
#[cfg(target_os = "windows")]
const FOREIGN_PLATFORM_DIRS: &[&str] = &["linux", "macos"];
#[cfg(not(any(target_os = "macos", target_os = "linux", target_os = "windows")))]
const FOREIGN_PLATFORM_DIRS: &[&str] = &[];

// ---------------------------------------------------------------------------
// Tests
// ---------------------------------------------------------------------------

#[cfg(test)]
mod tests {
    use super::*;

    /// Confirms that tool IDs containing only safe characters are not altered.
    #[test]
    fn sanitize_tool_id_preserves_safe_characters() {
        let safe = "ffmpeg+evermeet-ffmpeg@6e66d4d1e81f75b5f34dc2a369cc341e12edc531";
        assert_eq!(sanitize_tool_id(safe), safe);
    }

    /// Confirms that all filesystem-unsafe characters are replaced with `_`.
    #[test]
    fn sanitize_tool_id_replaces_unsafe_characters() {
        let input = r#"tool/a:b*c?d<e>f|g"h\i"#;
        let sanitized = sanitize_tool_id(input);
        assert_eq!(sanitized, "tool_a_b_c_d_e_f_g_h_i");
    }

    /// `./` or `.\` key must classify as Directory with an empty `relative_dir`.
    #[test]
    fn classify_dot_slash_key_maps_to_empty_relative_dir() {
        match classify_content_map_key("./").expect("classify ./") {
            ContentMapKeyKind::Directory { relative_dir } => {
                assert!(relative_dir.as_os_str().is_empty(), "expected empty relative_dir for ./");
            }
            ContentMapKeyKind::File { .. } => panic!("expected Directory for ./"),
        }
    }

    /// A named directory key (`linux/`) must classify as Directory with the
    /// named component as `relative_dir`.
    #[test]
    fn classify_named_directory_key_maps_to_relative_dir() {
        match classify_content_map_key("linux/").expect("classify linux/") {
            ContentMapKeyKind::Directory { relative_dir } => {
                assert_eq!(relative_dir, PathBuf::from("linux"));
            }
            ContentMapKeyKind::File { .. } => panic!("expected Directory for linux/"),
        }
    }

    /// A file-form key must classify as File with the normalized path.
    #[test]
    fn classify_file_key_maps_to_relative_path() {
        match classify_content_map_key("bin/tool").expect("classify bin/tool") {
            ContentMapKeyKind::File { relative_path } => {
                assert_eq!(relative_path, PathBuf::from("bin/tool"));
            }
            ContentMapKeyKind::Directory { .. } => panic!("expected File for bin/tool"),
        }
    }

    /// A bare `/` key (no component before the trailing slash) must be rejected.
    #[test]
    fn classify_bare_slash_key_is_rejected() {
        let err = classify_content_map_key("/").expect_err("bare slash should be rejected");
        match err {
            ConductorError::Workflow(msg) => {
                assert!(msg.contains("must contain at least one path component"), "{msg}");
            }
            other => panic!("unexpected error kind: {other:?}"),
        }
    }

    /// Extracted raw-file entries should gain `u+x` so bundled companion
    /// executables remain runnable from payload cache trees.
    #[cfg(unix)]
    #[test]
    fn ensure_user_execute_bit_sets_owner_execute_permission() {
        use std::os::unix::fs::PermissionsExt;

        let temp = tempfile::tempdir().expect("tempdir");
        let file_path = temp.path().join("tool.bin");
        fs::write(&file_path, b"tool-bytes").expect("write file");

        let mut permissions = fs::metadata(&file_path).expect("metadata").permissions();
        permissions.set_mode(0o644);
        fs::set_permissions(&file_path, permissions).expect("set non-executable mode");

        ensure_user_execute_bit(&file_path).expect("set execute bit");

        let mode = fs::metadata(&file_path).expect("metadata after").permissions().mode();
        assert_ne!(mode & 0o100, 0, "owner execute bit should be set");
    }
}
