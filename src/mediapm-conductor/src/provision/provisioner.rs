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
    ensure_payload_tree_user_execute_bits, now_unix_seconds, persist_cache_metadata,
    sanitize_tool_id,
};
use super::retain::{prune_expired_entries, retain_only_tool_dirs};
use super::types::{CleanupGuard, Metadata, ProvisionedTool};
use super::{
    DEFAULT_MAX_CONCURRENT, LOCK_FILE_NAME, METADATA_FILE_NAME, PAYLOAD_DIR_NAME,
    PRUNE_COOLDOWN_SECONDS, PRUNE_MARKER_FILE_NAME, VERSION,
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

    /// Removes provisioned entries that have not been used within the TTL.
    ///
    /// Only entries that are not currently locked are removed — active
    /// entries are always protected.
    ///
    /// # Errors
    /// Returns [`ConductorError`] if filesystem operations fail during pruning.
    pub async fn prune_expired(&self) -> Result<(), ConductorError> {
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

    fn try_lock_fast_path(
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

#[cfg(test)]
mod tests {
    use std::path::PathBuf;

    use crate::provision::helpers::link_to_sandbox_filtered;

    #[test]
    fn link_to_sandbox_filtered_skips_foreign_platform_dirs() {
        let payload = tempfile::tempdir().unwrap();
        let sandbox = tempfile::tempdir().unwrap();

        for os in &["linux", "macos", "windows"] {
            let dir = payload.path().join(os);
            std::fs::create_dir_all(&dir).unwrap();
            std::fs::write(dir.join("tool"), "content").unwrap();
        }

        link_to_sandbox_filtered(payload.path(), sandbox.path(), &["linux"]).unwrap();

        assert!(!sandbox.path().join("linux").exists(), "foreign platform dir was linked");
        assert!(sandbox.path().join("macos").exists(), "native platform dir was skipped");
        assert!(sandbox.path().join("windows").exists(), "native platform dir was skipped");
    }

    #[test]
    fn link_to_sandbox_filtered_no_foreign_dirs_copies_all() {
        let payload = tempfile::tempdir().unwrap();
        let sandbox = tempfile::tempdir().unwrap();

        for os in &["linux", "windows"] {
            let dir = payload.path().join(os);
            std::fs::create_dir_all(&dir).unwrap();
            std::fs::write(dir.join("tool"), "content").unwrap();
        }

        link_to_sandbox_filtered(payload.path(), sandbox.path(), &[]).unwrap();

        assert!(sandbox.path().join("linux").exists());
        assert!(sandbox.path().join("windows").exists());
    }

    #[test]
    fn link_to_sandbox_filtered_empty_payload_creates_empty_sandbox() {
        let payload = tempfile::tempdir().unwrap();
        let sandbox = tempfile::tempdir().unwrap();

        link_to_sandbox_filtered(payload.path(), sandbox.path(), &["linux"]).unwrap();

        assert!(sandbox.path().exists());
        assert!(sandbox.path().read_dir().unwrap().next().is_none());
    }

    #[test]
    fn link_to_sandbox_filtered_nonexistent_payload_errors() {
        let sandbox = tempfile::tempdir().unwrap();
        let result = link_to_sandbox_filtered(
            &PathBuf::from("/nonexistent/payload"),
            sandbox.path(),
            &["linux"],
        );
        assert!(result.is_err());
    }

    #[tokio::test]
    async fn provisioned_tool_guard_holds_shared_lock_prevents_prune() {
        use std::collections::{BTreeMap, HashSet};
        use std::sync::Arc;

        use bytes::Bytes;
        use mediapm_cas::{CasApi, Hash, InMemoryCas};

        use crate::provision::ProvisionCache;

        let tools_dir = tempfile::tempdir().unwrap();
        let cas = Arc::new(InMemoryCas::new());

        // Put some content in the CAS.
        let content = b"tool binary content";
        let hash = Hash::from_content(content);
        cas.put(Bytes::from(content.to_vec())).await.unwrap();

        // Build content map with one file entry.
        let mut content_map = BTreeMap::new();
        content_map.insert("my-file".to_string(), hash);

        // Create provision cache.
        let cache = ProvisionCache::new(tools_dir.path().to_path_buf(), cas, None);

        // Materialize the tool. This acquires a shared lock on the entry.
        let guard = cache.materialize("test-tool", &content_map).await.unwrap();

        let entry_dir = tools_dir.path().join("test-tool");
        assert!(entry_dir.exists(), "entry should exist after materialize");

        // While the guard holds a shared lock, retain_only should NOT be able
        // to exclusively lock and remove the entry.
        let empty_set: HashSet<String> = HashSet::new();
        cache.retain_only(&empty_set).await.unwrap();
        assert!(
            entry_dir.exists(),
            "entry should survive retain_only while guard holds shared lock"
        );

        // Drop the guard to release the shared lock.
        drop(guard);

        // Now retain_only should succeed in exclusively locking and removing the entry.
        cache.retain_only(&empty_set).await.unwrap();
        assert!(
            !entry_dir.exists(),
            "entry should be removed after guard is dropped and retain_only runs"
        );
    }
}
