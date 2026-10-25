//! Retain-only and expiry-prune logic.

use std::collections::HashSet;
use std::fs;
use std::path::{Path, PathBuf};

use crate::error::ConductorError;

use super::helpers::sanitize_tool_id;
use super::types::Metadata;
use super::{LEGACY_LOCK_FILE_NAME, LOCK_FILE_NAME, METADATA_FILE_NAME, TTL_SECONDS, VERSION};

/// Removes all provisioned tool directories under `tools_dir` whose sanitized
/// names are not present in `active_tool_ids`.
///
/// This is the free-function counterpart of [`super::ProvisionCache::retain_only`],
/// usable without constructing a [`super::ProvisionCache`].  Only entries without
/// active locks are removed; in-use entries are preserved even if they are not
/// in the active set.
///
/// # Errors
/// Returns [`ConductorError`] if directory listing or removal fails.
#[expect(clippy::implicit_hasher, reason = "public API uses a concrete HashSet")]
pub async fn retain_only_tool_dirs(
    tools_dir: PathBuf,
    active_tool_ids: HashSet<String>,
) -> Result<(), ConductorError> {
    let active: HashSet<String> =
        active_tool_ids.into_iter().map(|id| sanitize_tool_id(&id)).collect();
    tokio::task::spawn_blocking(move || do_retain_only(&tools_dir, &active)).await.map_err(
        |join_err| ConductorError::Internal(format!("retain-only task panicked: {join_err}")),
    )?
}

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

        // A directory that predates the lock rename cannot be locked through
        // the current name, so it would be skipped on every sweep for as long
        // as it exists. It is not in the active set, so reclaim it.
        if remove_legacy_lock_entry(&path)? {
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

/// Removes `entry_dir` when it carries the lock name this build no longer
/// writes, and answers whether it did.
///
/// A directory with `.lock` and no `lock` was extracted by an older build.
/// The rename means nothing can take its lock any more, so the sweep that
/// reaches it can only skip it or remove it, and skipping leaves it for every
/// later sweep to skip again.
///
/// # Errors
/// Returns the I/O failure that stopped the removal. A directory that will not
/// go is reported rather than swallowed: the sweep is the only thing that will
/// ever remove it, so silence here means it stays for the life of the store.
fn remove_legacy_lock_entry(entry_dir: &Path) -> Result<bool, ConductorError> {
    if !entry_dir.join(LEGACY_LOCK_FILE_NAME).exists() {
        return Ok(false);
    }
    if entry_dir.join(LOCK_FILE_NAME).exists() {
        return Ok(false);
    }
    fs::remove_dir_all(entry_dir).map_err(|source| ConductorError::Io {
        operation: "removing a tool-content cache entry left under the legacy lock name"
            .to_string(),
        path: entry_dir.to_path_buf(),
        source,
    })?;
    Ok(true)
}

/// Removes provisioning entries that have not been used within `TTL_SECONDS`.
pub(crate) fn prune_expired_entries(tools_dir: &Path, now: u64) -> Result<(), ConductorError> {
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

        // An expired entry from before the lock rename cannot be locked
        // through the current name and would otherwise never be reclaimed.
        if remove_legacy_lock_entry(&path)? {
            continue;
        }

        // Try to acquire an exclusive lock non-blockingly.
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

#[cfg(test)]
mod tests {
    use mediapm_utils::temp::artifact_dir;

    use super::*;

    /// Writes an entry directory with the files a sweep looks at.
    fn seed_entry(tools_dir: &Path, name: &str, lock_name: &str, last_used: u64) {
        let entry = tools_dir.join(name);
        fs::create_dir_all(entry.join("payload")).expect("entry payload dir");
        fs::write(entry.join(lock_name), b"").expect("entry lock file");
        let metadata = format!(
            "{{\"version\":{VERSION},\"content_map\":{{}},\"lastUsedUnixSeconds\":{last_used},\"execute_bits_verified\":true}}"
        );
        fs::write(entry.join(METADATA_FILE_NAME), metadata).expect("entry metadata");
    }

    /// A directory extracted before the lock rename cannot be locked through
    /// the current name, so it is reclaimed instead of skipped forever.
    ///
    /// Skipping is the failure this pins: a directory nothing can lock is a
    /// directory every later sweep skips, so it survives until a person deletes
    /// it by hand, and the sweep that should have reclaimed it reports success.
    #[tokio::test]
    async fn regression_a_legacy_lock_directory_is_reclaimed() {
        let tools_dir = artifact_dir().expect("tools dir");
        seed_entry(tools_dir.path(), "legacy", LEGACY_LOCK_FILE_NAME, 0);
        seed_entry(tools_dir.path(), "current", LOCK_FILE_NAME, 0);

        retain_only_tool_dirs(
            tools_dir.path().to_path_buf(),
            HashSet::from(["current".to_string()]),
        )
        .await
        .expect("retain");

        assert!(
            !tools_dir.path().join("legacy").exists(),
            "a directory carrying only the old lock name has to be reclaimed"
        );
        assert!(
            tools_dir.path().join("current").exists(),
            "a directory carrying the current lock name is this sweep's to keep"
        );
    }

    /// The expiry sweep reclaims a legacy directory once its TTL has passed,
    /// and leaves a legacy directory that is still inside the TTL alone.
    ///
    /// The TTL is the second thing that decides whether an entry is still
    /// wanted, so a rename must not turn it into a reason to delete an entry
    /// that is in use by the build that wrote it.
    #[test]
    fn a_legacy_lock_directory_is_reclaimed_only_after_its_ttl() {
        let tools_dir = artifact_dir().expect("tools dir");
        seed_entry(tools_dir.path(), "expired", LEGACY_LOCK_FILE_NAME, 0);
        seed_entry(tools_dir.path(), "fresh", LEGACY_LOCK_FILE_NAME, 1_000 + TTL_SECONDS);

        prune_expired_entries(tools_dir.path(), 1_000 + TTL_SECONDS + 1).expect("prune");

        assert!(!tools_dir.path().join("expired").exists());
        assert!(tools_dir.path().join("fresh").exists());
    }

    /// The retain set is compared by sanitized name, so an active legacy
    /// directory is left for the next materialize to rebuild rather than
    /// removed underneath a configured tool.
    #[tokio::test]
    async fn an_active_legacy_lock_directory_is_kept() {
        let tools_dir = artifact_dir().expect("tools dir");
        seed_entry(tools_dir.path(), "active", LEGACY_LOCK_FILE_NAME, 0);

        retain_only_tool_dirs(
            tools_dir.path().to_path_buf(),
            HashSet::from(["active".to_string()]),
        )
        .await
        .expect("retain");

        assert!(
            tools_dir.path().join("active").exists(),
            "retain removes what is not active; an active entry is rebuilt by its next materialize"
        );
    }

    /// A retain pass leaves a payload alone while a live guard holds it, and
    /// removes it once nothing does.
    ///
    /// The `in_place` staging strategy executes a tool out of its provisioned
    /// directory, so the directory a step is running from is the directory a
    /// prune would delete. The provision cache's shared lock on the entry is the
    /// only signal that separates the two cases, and this is what says it is
    /// read: without it, a prune during a run removes bytes a step is executing.
    #[tokio::test]
    async fn a_prune_never_removes_a_payload_in_use() {
        let tools_dir = mediapm_utils::temp::artifact_dir().expect("tools dir");
        let cas = mediapm_cas::InMemoryCas::new();
        let hash = mediapm_cas::CasApi::put(&cas, bytes::Bytes::from_static(b"payload bytes"))
            .await
            .expect("cas put");
        let content_map = std::collections::BTreeMap::from([("linux/tool".to_string(), hash)]);
        let cache = crate::provision::ProvisionCache::new(
            tools_dir.path().to_path_buf(),
            std::sync::Arc::new(cas),
            None,
        );

        let guard = cache.materialize("in-use@hash", &content_map).await.expect("materialize");
        let entry_dir = tools_dir.path().join("in-use@hash");
        assert!(entry_dir.is_dir(), "the payload is materialized under its tool id");

        retain_only_tool_dirs(tools_dir.path().to_path_buf(), std::collections::HashSet::new())
            .await
            .expect("retain pass");
        assert!(entry_dir.is_dir(), "a retain pass must not remove a payload a live guard holds");

        drop(guard);
        retain_only_tool_dirs(tools_dir.path().to_path_buf(), std::collections::HashSet::new())
            .await
            .expect("retain pass");
        assert!(!entry_dir.exists(), "once nothing holds the entry, the same pass removes it");
    }
}
