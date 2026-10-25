//! Core provisioning engine.

use std::collections::{BTreeMap, HashMap, HashSet};
use std::fs;
use std::path::{Path, PathBuf};
use std::sync::{Arc, LazyLock, Mutex};
use std::time::Instant;

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
    DEFAULT_MAX_CONCURRENT, LEGACY_PRUNE_MARKER_FILE_NAME, LOCK_FILE_NAME, METADATA_FILE_NAME,
    PAYLOAD_DIR_NAME, PRUNE_COOLDOWN_SECONDS, VERSION,
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

/// Instants of the last automatic prune sweep this process started, per tools
/// root.
///
/// The cooldown lives in the process that does the work. A file beside the tools
/// root used to record it, which let a process inherit a stamp written by a
/// process that had already swept, and left a file in the tree that read as
/// residue. It is keyed by root rather than held as one instant because the
/// marker it replaced was per root: a process driving two tools roots — two
/// workspaces, or a test that builds a second cache — sweeps each of them on its
/// own clock instead of refusing the second root's first sweep.
static LAST_AUTOMATIC_SWEEP: LazyLock<Mutex<HashMap<PathBuf, Instant>>> =
    LazyLock::new(|| Mutex::new(HashMap::new()));

/// Claims the next automatic sweep of `tools_dir`, answering whether this call
/// may run it.
///
/// The claim is taken before the sweep starts, so two concurrent materializes
/// cannot both sweep and the second one does not wait for the first. A claimed
/// sweep that then fails is not retried until the cooldown elapses again, which
/// is the same bargain a timestamped file made: the work is bounded, the entry
/// is pruned on a later pass, and no caller is made to wait on it.
///
/// A root this process has not swept yet answers `true` on its first call,
/// because there is no earlier sweep of that root to be inside the cooldown of.
fn claim_automatic_sweep(tools_dir: &Path) -> bool {
    let mut all = lock_sweep_state();
    let now = Instant::now();
    if let Some(previous) = all.get(tools_dir)
        && now.duration_since(*previous).as_secs() < PRUNE_COOLDOWN_SECONDS
    {
        return false;
    }
    all.insert(tools_dir.to_path_buf(), now);
    true
}

/// Records that a sweep of `tools_dir` is starting, so the next automatic call
/// for that root waits out the cooldown instead of sweeping immediately after an
/// explicit one.
fn record_sweep(tools_dir: &Path) {
    lock_sweep_state().insert(tools_dir.to_path_buf(), Instant::now());
}

/// Takes the lock on the per-root sweep instants.
///
/// A poisoned lock yields the map it guards: the values are timestamps, and a
/// panic in another thread leaves them as usable as they were before.
fn lock_sweep_state() -> std::sync::MutexGuard<'static, HashMap<PathBuf, Instant>> {
    match LAST_AUTOMATIC_SWEEP.lock() {
        Ok(all) => all,
        Err(poisoned) => poisoned.into_inner(),
    }
}

/// Forgets every recorded sweep, so the next automatic call for any root behaves
/// as a process's first one.
///
/// Test-only: the cooldown is process state, and a test is the only caller that
/// needs to start from a process which has never swept.
#[cfg(test)]
fn reset_automatic_sweep() {
    lock_sweep_state().clear();
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
    /// Returns [`ConductorError`] when the tools root cannot be enumerated.
    ///
    /// Answers one warning per directory the sweep could not remove, for the
    /// caller to forward to whatever channel reports to a person — nothing else
    /// will ever remove those directories.
    pub async fn retain_only(
        &self,
        active_tool_ids: &HashSet<String>,
    ) -> Result<Vec<String>, ConductorError> {
        retain_only_tool_dirs(self.tools_dir.clone(), active_tool_ids.clone()).await
    }

    // -----------------------------------------------------------------------
    // Private methods
    // -----------------------------------------------------------------------

    /// Best-effort prune, run on the blocking pool by the materialize that
    /// triggers it.
    ///
    /// One sweep per `PRUNE_COOLDOWN_SECONDS` per tools root in this process:
    /// the claim is taken synchronously, so a second concurrent materialize does
    /// not also sweep. The walk itself runs on the blocking pool so it never
    /// occupies a worker thread, and the materialize that claims the sweep does
    /// await its completion — the alternative, leaving the join handle unawaited,
    /// would let the walk outlive the cache it is walking.
    async fn try_maybe_prune(&self, now: u64) {
        if !claim_automatic_sweep(&self.tools_dir) {
            return;
        }

        let tools_dir = self.tools_dir.clone();
        let _ = tokio::task::spawn_blocking(move || sweep_expired_entries(&tools_dir, now)).await;
    }

    /// Prunes expired entries unconditionally (no cooldown check).
    ///
    /// An explicit sweep restarts the cooldown for this tools root, so the
    /// automatic passthrough does not immediately repeat the work this call just
    /// did.
    async fn prune_internal(&self, now: u64) -> Result<(), ConductorError> {
        let tools_dir = self.tools_dir.clone();
        record_sweep(&self.tools_dir);

        tokio::task::spawn_blocking(move || sweep_expired_entries(&tools_dir, now)).await.map_err(
            |join_err| ConductorError::Internal(format!("prune task panicked: {join_err}")),
        )?
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

/// Runs one expiry sweep over `tools_dir`, dropping the cooldown stamp an older
/// build kept there and then pruning what has expired.
///
/// The stamp is removed rather than read: nothing in this build consults it, and
/// a file whose only purpose was to be read by code that no longer exists is the
/// residue the sweep is there to clear. The removal is best-effort because the
/// sweep it precedes is the work a caller is waiting on, and a stamp that will
/// not go is one directory entry rather than a failed prune; the next sweep
/// tries it again.
fn sweep_expired_entries(tools_dir: &Path, now: u64) -> Result<(), ConductorError> {
    let _ = fs::create_dir_all(tools_dir);
    remove_legacy_prune_marker(tools_dir);
    for warning in prune_expired_entries(tools_dir, now)? {
        tracing::warn!("{warning}");
    }
    Ok(())
}

/// Removes the cooldown stamp an older build wrote beside the tools root.
fn remove_legacy_prune_marker(tools_dir: &Path) {
    let marker = tools_dir.join(LEGACY_PRUNE_MARKER_FILE_NAME);
    match fs::remove_file(&marker) {
        Ok(()) => {}
        Err(error) if error.kind() == std::io::ErrorKind::NotFound => {}
        Err(error) => {
            tracing::warn!(
                "could not remove the superseded prune stamp '{}': {error}",
                marker.display()
            );
        }
    }
}

#[cfg(test)]
mod tests {
    use std::path::PathBuf;

    use tempfile::TempDir;

    use super::*;
    use crate::provision::LOCK_FILE_HEADER;
    use crate::provision::SWEEP_STATE_LOCK;
    use crate::provision::helpers::link_to_sandbox_filtered;

    /// Builds a cache over a fresh tools root, with a payload in CAS.
    async fn cache_with_payload() -> (TempDir, ProvisionCache<mediapm_cas::InMemoryCas>) {
        let tools_dir = mediapm_utils::temp::artifact_dir().expect("tools dir");
        let cas = Arc::new(mediapm_cas::InMemoryCas::new());
        cas.put(bytes::Bytes::from_static(b"tool binary content")).await.expect("cas put");
        let cache = ProvisionCache::new(tools_dir.path().to_path_buf(), cas, None);
        (tools_dir, cache)
    }

    /// Writes an entry directory whose metadata says when it was last used.
    fn seed_entry(tools_dir: &std::path::Path, name: &str, last_used: u64) {
        let entry = tools_dir.join(name);
        std::fs::create_dir_all(entry.join(PAYLOAD_DIR_NAME)).expect("payload dir");
        std::fs::write(entry.join(LOCK_FILE_NAME), b"").expect("lock file");
        let metadata = format!(
            "{{\"version\":{VERSION},\"content_map\":{{}},\"lastUsedUnixSeconds\":{last_used},\"execute_bits_verified\":true}}"
        );
        std::fs::write(entry.join(METADATA_FILE_NAME), metadata).expect("metadata");
    }

    /// An extracted entry leaves a lock file a person can identify.
    ///
    /// The file's bytes are documentation for whoever finds it in the tools
    /// root: a blank `lock` reads as residue, and the header says what holds it
    /// and what deleting it costs.
    #[tokio::test]
    async fn a_provision_lock_names_itself() {
        let _serialized = SWEEP_STATE_LOCK.lock().await;
        let (tools_dir, cache) = cache_with_payload().await;
        let content_map = BTreeMap::new();
        let guard = cache.materialize("named-tool", &content_map).await.expect("materialize");
        let lock_path = tools_dir.path().join("named-tool").join(LOCK_FILE_NAME);

        let contents = std::fs::read_to_string(&lock_path).expect("read lock file");

        assert_eq!(contents, LOCK_FILE_HEADER);
        drop(guard);
    }

    /// A cache hit leaves the entry's lock file bytes alone.
    ///
    /// The second materialize finds a valid entry and takes the shared lock
    /// without writing, so text a person added to the file survives it. That is
    /// what makes the header trustworthy: it is written when the file is
    /// created, never as a side effect of reading the entry.
    #[tokio::test]
    async fn the_provision_lock_is_written_once() {
        let _serialized = SWEEP_STATE_LOCK.lock().await;
        let (tools_dir, cache) = cache_with_payload().await;
        let content_map = BTreeMap::new();
        let guard =
            cache.materialize("stable-tool", &content_map).await.expect("first materialize");
        drop(guard);
        let lock_path = tools_dir.path().join("stable-tool").join(LOCK_FILE_NAME);
        let first = std::fs::read(&lock_path).expect("read lock file");
        // A path the header cannot explain: if a later open rewrote the file,
        // this text is what a rewrite would have to reproduce.
        let edited = format!("{}\nsomething a person added\n", LOCK_FILE_HEADER.trim_end());
        std::fs::write(&lock_path, &edited).expect("edit lock file");

        let guard =
            cache.materialize("stable-tool", &content_map).await.expect("second materialize");
        let second = std::fs::read(&lock_path).expect("read lock file again");
        drop(guard);

        assert_eq!(first, LOCK_FILE_HEADER.as_bytes());
        assert_eq!(
            second,
            edited.as_bytes(),
            "the lock header is written when the file is created, not on every open"
        );
    }

    /// Re-extracting an entry writes the header again, because the extraction
    /// removed the file that held it.
    ///
    /// A changed content map is the only thing that reaches the extraction path
    /// with an entry already on disk, so it is what proves the header survives
    /// the removal-and-rebuild cycle rather than only the first creation.
    #[tokio::test]
    async fn a_reextracted_entry_writes_the_header_again() {
        let _serialized = SWEEP_STATE_LOCK.lock().await;
        let (tools_dir, cache) = cache_with_payload().await;
        let mut first_map = BTreeMap::new();
        first_map.insert(
            "linux/tool".to_string(),
            mediapm_cas::Hash::from_content(b"tool binary content"),
        );
        let guard = cache.materialize("rebuilt-tool", &first_map).await.expect("first materialize");
        drop(guard);
        let lock_path = tools_dir.path().join("rebuilt-tool").join(LOCK_FILE_NAME);

        // A different map misses the cache and drives a fresh extraction over
        // the entry that is already there.
        let mut second_map = BTreeMap::new();
        second_map.insert(
            "linux/tool".to_string(),
            mediapm_cas::Hash::from_content(b"tool binary content"),
        );
        second_map.insert(
            "linux/extra".to_string(),
            mediapm_cas::Hash::from_content(b"tool binary content"),
        );
        let guard =
            cache.materialize("rebuilt-tool", &second_map).await.expect("second materialize");
        let contents = std::fs::read_to_string(&lock_path).expect("read lock file");
        drop(guard);

        assert_eq!(
            contents, LOCK_FILE_HEADER,
            "the rebuilt entry's lock file has to carry the header again"
        );
    }

    /// The automatic sweep runs once per cooldown in a process, and a process
    /// that has not swept yet does sweep.
    ///
    /// The cooldown lives in the process, so the first materialize of a run
    /// reclaims what expired between runs, and the rest of the run does not pay
    /// for a directory walk each time.
    #[tokio::test]
    async fn prune_cooldown_is_process_local() {
        let _serialized = SWEEP_STATE_LOCK.lock().await;
        let (tools_dir, cache) = cache_with_payload().await;
        let content_map = BTreeMap::new();
        seed_entry(tools_dir.path(), "expired-first", 0);

        reset_automatic_sweep();
        let guard = cache.materialize("first-tool", &content_map).await.expect("materialize");
        drop(guard);
        assert!(
            !tools_dir.path().join("expired-first").exists(),
            "a process that has not swept yet has to sweep on its first materialize"
        );

        seed_entry(tools_dir.path(), "expired-second", 0);
        let guard = cache.materialize("second-tool", &content_map).await.expect("materialize");
        drop(guard);
        assert!(
            tools_dir.path().join("expired-second").exists(),
            "a second materialize inside the cooldown must not sweep again"
        );

        reset_automatic_sweep();
        let guard = cache.materialize("third-tool", &content_map).await.expect("materialize");
        drop(guard);
        assert!(
            !tools_dir.path().join("expired-second").exists(),
            "a fresh process state sweeps again"
        );
    }

    /// The claim itself refuses a second sweep inside the cooldown and grants
    /// one again once the process state is cleared.
    #[tokio::test]
    async fn the_sweep_claim_holds_for_the_cooldown() {
        let _serialized = SWEEP_STATE_LOCK.lock().await;
        reset_automatic_sweep();
        let root = Path::new("claim-root");

        assert!(claim_automatic_sweep(root), "the first claim of a process is granted");
        assert!(!claim_automatic_sweep(root), "a claim inside the cooldown is refused");

        reset_automatic_sweep();
        assert!(claim_automatic_sweep(root), "clearing the state grants the next claim");
    }

    /// Each tools root has its own cooldown, so a second root in the same
    /// process sweeps on its first materialize.
    ///
    /// The cooldown used to be recorded in a file beside the root, which made it
    /// per root; holding one instant for the process would make a second root
    /// inherit the first one's sweep and wait out its interval.
    #[tokio::test]
    async fn a_second_tools_root_sweeps_without_waiting() {
        let _serialized = SWEEP_STATE_LOCK.lock().await;
        let (first_dir, first_cache) = cache_with_payload().await;
        let (second_dir, second_cache) = cache_with_payload().await;
        let content_map = BTreeMap::new();
        seed_entry(first_dir.path(), "expired-first", 0);
        seed_entry(second_dir.path(), "expired-second", 0);

        reset_automatic_sweep();
        let guard = first_cache.materialize("first", &content_map).await.expect("materialize");
        drop(guard);
        assert!(!first_dir.path().join("expired-first").exists(), "the first root sweeps");

        let guard = second_cache.materialize("second", &content_map).await.expect("materialize");
        drop(guard);
        assert!(
            !second_dir.path().join("expired-second").exists(),
            "a second root does not inherit the first root's cooldown"
        );
    }

    /// A tools root upgraded in place loses the stamp the older build wrote.
    ///
    /// The stamp is not read by this build, so leaving it would leave a file
    /// whose only reader no longer exists. Best-effort removal is what keeps a
    /// stamp that will not go from failing the prune it precedes.
    #[tokio::test]
    async fn regression_a_legacy_prune_marker_is_removed() {
        let _serialized = SWEEP_STATE_LOCK.lock().await;
        let (tools_dir, cache) = cache_with_payload().await;
        let marker = tools_dir.path().join(LEGACY_PRUNE_MARKER_FILE_NAME);
        std::fs::write(&marker, b"0\n").expect("seed legacy stamp");

        cache.prune_expired().await.expect("prune");

        assert!(!marker.exists(), "the superseded prune stamp has to go with the sweep");
    }

    #[test]
    fn link_to_sandbox_filtered_skips_foreign_platform_dirs() {
        let payload = mediapm_utils::temp::artifact_dir().expect("artifact dir");
        let sandbox = mediapm_utils::temp::artifact_dir().expect("artifact dir");

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
        let payload = mediapm_utils::temp::artifact_dir().expect("artifact dir");
        let sandbox = mediapm_utils::temp::artifact_dir().expect("artifact dir");

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
        let payload = mediapm_utils::temp::artifact_dir().expect("artifact dir");
        let sandbox = mediapm_utils::temp::artifact_dir().expect("artifact dir");

        link_to_sandbox_filtered(payload.path(), sandbox.path(), &["linux"]).unwrap();

        assert!(sandbox.path().exists());
        assert!(sandbox.path().read_dir().unwrap().next().is_none());
    }

    #[test]
    fn link_to_sandbox_filtered_nonexistent_payload_errors() {
        let sandbox = mediapm_utils::temp::artifact_dir().expect("artifact dir");
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

        let tools_dir = mediapm_utils::temp::artifact_dir().expect("artifact dir");
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

        let _serialized = SWEEP_STATE_LOCK.lock().await;

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
