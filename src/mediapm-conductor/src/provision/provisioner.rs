//! Core provisioning engine.

use std::collections::{BTreeMap, HashSet};
use std::fs;
use std::path::{Path, PathBuf};
use std::sync::Arc;

use dashmap::DashMap;
use mediapm_cas::{CasApi, Hash};
use tokio::sync::{Notify, Semaphore};

use crate::error::ConductorError;

use super::extract::{ExtractPaths, extract_sync, fetch_all_cas_entries};
use super::helpers::{
    copy_directory_recursive, ensure_payload_tree_user_execute_bits, now_unix_seconds,
    persist_cache_metadata, sanitize_tool_id,
};
use super::retain::{prune_expired_entries, retain_only_tool_dirs};
use super::types::{CleanupGuard, Metadata, ProvisionedTool};
use super::{
    DEFAULT_MAX_CONCURRENT, FOREIGN_PLATFORM_DIRS, LOCK_FILE_NAME, METADATA_FILE_NAME,
    PAYLOAD_DIR_NAME, PRUNE_COOLDOWN_SECONDS, PRUNE_MARKER_FILE_NAME, VERSION,
};

/// A persistent managed-tool provisioning cache with CAS-backed
/// materialization and concurrent single-flight extraction deduplication.
pub struct ProvisionCache<C> {
    /// Root directory under which per-tool provisioned entries live.
    tools_dir: PathBuf,
    /// CAS client used to fetch payload bytes.
    cas: Arc<C>,
    /// Map from sanitized tool id to notification handle for in-flight
    /// extractions.  Used to implement single-flight deduplication.
    pending: Arc<DashMap<String, Arc<Notify>>>,
    /// Global concurrency limiter for extraction tasks.
    semaphore: Arc<Semaphore>,
}

impl<C: std::fmt::Debug> std::fmt::Debug for ProvisionCache<C> {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("ProvisionCache")
            .field("tools_dir", &self.tools_dir)
            .field("pending", &self.pending)
            .field("cas", &self.cas)
            .field("semaphore", &self.semaphore)
            .finish()
    }
}

impl<C: CasApi + Send + Sync + 'static> ProvisionCache<C> {
    /// Creates a new provisioning cache rooted at `tools_dir`.
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

    /// Returns a provisioned entry for `tool_id`, fetching and extracting
    /// payloads from CAS on cache miss.
    ///
    /// The returned [`ProvisionedTool`] holds a shared advisory lock on the
    /// entry, keeping it alive until the guard is dropped.
    ///
    /// Concurrent calls for the same `tool_id` are deduplicated:
    /// only the first caller drives extraction; others wait on a notification
    /// and retry the fast path.
    ///
    /// # Errors
    /// Returns [`ConductorError`] if extraction or metadata I/O fails.
    pub async fn materialize(
        &self,
        tool_id: &str,
        content_map: &BTreeMap<String, Hash>,
    ) -> Result<ProvisionedTool, ConductorError> {
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
                    drop(occupied);
                    notify.notified().await;
                }
                dashmap::mapref::entry::Entry::Vacant(vacant) => {
                    // We are the first caller for this tool.
                    let notify = Arc::new(Notify::new());
                    let cleanup_notify = notify.clone();
                    vacant.insert(notify);

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
    #[allow(clippy::similar_names)]
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

    /// Removes provisioned entries that have not been used within the TTL.
    ///
    /// Only entries that are not currently locked are removed — active
    /// entries are always protected.
    ///
    /// # Errors
    /// Returns [`ConductorError`] if filesystem operations fail during pruning.
    pub async fn prune(&self) -> Result<(), ConductorError> {
        let now = now_unix_seconds();
        self.prune_internal(now).await
    }

    /// Removes all provisioned entries except those in `active_tool_ids`.
    ///
    /// Only entries without active locks are removed; in-use entries are
    /// preserved even if they are not in the active set.
    ///
    /// # Errors
    /// Returns [`ConductorError`] if filesystem operations fail during retention.
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

        // Synchronous cooldown check (single file read — fast enough).
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
    ) -> Result<ProvisionedTool, ConductorError> {
        // Phase 1 — classify keys and fetch all CAS bytes concurrently.
        let entries_with_bytes = fetch_all_cas_entries(Arc::clone(&self.cas), content_map).await?;

        // Phase 2 — blocking I/O: extraction, metadata, lock downgrade.
        let entry_dir = entry_dir.to_path_buf();
        let payload_dir = payload_dir.to_path_buf();
        let metadata_path = metadata_path.to_path_buf();
        let lock_path = lock_path.to_path_buf();
        let content_map = content_map.clone();

        tokio::task::spawn_blocking(move || {
            let extract_paths = ExtractPaths { entry_dir, lock_path, payload_dir, metadata_path };
            extract_sync(&extract_paths, &content_map, &entries_with_bytes)
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
    ) -> Result<Option<ProvisionedTool>, ConductorError> {
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
                Ok(Some(ProvisionedTool {
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
