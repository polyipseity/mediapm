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
/// A directory this sweep cannot remove is reported in the answer and the sweep
/// carries on with the rest: one directory that will not go is a warning about
/// that directory, not a reason to leave every other stale entry in place.
/// Callers with a user-facing warning channel are expected to forward the answer
/// there, because nothing else will ever remove those directories.
///
/// # Errors
/// Returns [`ConductorError`] when the tools root cannot be listed at all, since
/// a sweep that cannot see its own entries has nothing to report per entry.
#[expect(clippy::implicit_hasher, reason = "public API uses a concrete HashSet")]
pub async fn retain_only_tool_dirs(
    tools_dir: PathBuf,
    active_tool_ids: HashSet<String>,
) -> Result<Vec<String>, ConductorError> {
    let active: HashSet<String> =
        active_tool_ids.into_iter().map(|id| sanitize_tool_id(&id)).collect();
    let warnings =
        tokio::task::spawn_blocking(move || do_retain_only(&tools_dir, &active)).await.map_err(
            |join_err| ConductorError::Internal(format!("retain-only task panicked: {join_err}")),
        )??;
    // Logged as well as returned: this sweep is called both from a sync a person
    // is watching and from maintenance nobody waits for, and the log is the only
    // record the second caller leaves. A caller with a user-facing channel
    // forwards the answer instead of relying on the log.
    for warning in &warnings {
        tracing::warn!("{warning}");
    }
    Ok(warnings)
}

/// Removes all entry directories in `tools_dir` whose name is not in
/// `active_sanitized`, answering one warning per directory it could not remove.
fn do_retain_only(
    tools_dir: &Path,
    active_sanitized: &HashSet<String>,
) -> Result<Vec<String>, ConductorError> {
    let mut warnings = Vec::new();
    if !tools_dir.exists() {
        return Ok(warnings);
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
        // as it exists. It is not in the active set, so reclaim it — unless the
        // build that wrote it still holds its lock, which is the one signal that
        // says its payload may be executing right now.
        match remove_legacy_lock_entry(&path) {
            Ok(LegacyLockEntry::Absent) => {}
            Ok(LegacyLockEntry::Removed) => continue,
            Ok(LegacyLockEntry::InUse) => {
                warnings.push(legacy_entry_in_use_warning(&path));
                continue;
            }
            Err(source) => {
                warnings.push(removal_warning(&path, "under the legacy lock name", &source));
                continue;
            }
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
        if let Err(source) = fs::remove_dir_all(&path) {
            warnings.push(removal_warning(&path, "an inactive provisioned entry", &source));
        }
    }
    Ok(warnings)
}

/// The warning a sweep reports for a directory it wanted gone and could not
/// remove.
///
/// The path and the operating system's own account are both named: the path is
/// what a reader has to delete by hand, and the error is what tells a permission
/// problem from a directory another process still holds something in.
fn removal_warning(path: &Path, what: &str, source: &std::io::Error) -> String {
    format!("could not remove the tool payload directory '{}' ({what}): {source}", path.display())
}

/// What the sweep found when it looked at a directory for the pre-rename lock
/// name.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum LegacyLockEntry {
    /// The directory carries the current lock name, or no lock name at all:
    /// it is not this sweep's legacy case.
    Absent,
    /// The directory carried only the old lock name, nothing held that lock,
    /// and the directory is gone.
    Removed,
    /// The directory carried only the old lock name and a process other than
    /// this one holds it, so its payload may be executing right now.
    InUse,
}

/// Removes `entry_dir` when it carries the lock name this build no longer
/// writes, and answers what it found.
///
/// A directory with `.lock` and no `lock` was extracted by an older build. The
/// rename means nothing can take its lock through the current name, so the
/// sweep that reaches it can only skip it or remove it, and skipping leaves it
/// for every later sweep to skip again.
///
/// Skipping is not the same as being unable to check, though: a build that
/// predates the rename still holds a shared lock on `.lock` for as long as it
/// uses the payload, and that lock is the one guard on this file that still
/// works. So the lock is probed before the directory goes, and a held lock keeps
/// it.
///
/// # Errors
/// Returns the I/O failure that stopped the removal, for the caller to report.
/// A directory that will not go is reported rather than swallowed: the sweep is
/// the only thing that will ever remove it, so silence here means it stays for
/// the life of the store. The caller reports it and carries on with the other
/// entries, which is why this answers the failure instead of raising it.
fn remove_legacy_lock_entry(entry_dir: &Path) -> Result<LegacyLockEntry, std::io::Error> {
    let legacy_lock_path = entry_dir.join(LEGACY_LOCK_FILE_NAME);
    if !legacy_lock_path.exists() || entry_dir.join(LOCK_FILE_NAME).exists() {
        return Ok(LegacyLockEntry::Absent);
    }

    // The probe takes an exclusive lock, so a shared lock held by an older build
    // counts as held. The file is closed before the removal: Windows will not
    // delete a directory that still has an open handle inside it.
    {
        match std::fs::OpenOptions::new().read(true).write(true).open(&legacy_lock_path) {
            Ok(lock_file) => {
                if lock_file.try_lock().is_err() {
                    return Ok(LegacyLockEntry::InUse);
                }
            }
            // The file was there when this function checked and is gone now: a
            // concurrent sweep removed the directory, which is the outcome this
            // call wanted anyway.
            Err(source) if source.kind() == std::io::ErrorKind::NotFound => {
                return Ok(LegacyLockEntry::Removed);
            }
            Err(source) => return Err(source),
        }
    }

    fs::remove_dir_all(entry_dir)?;
    Ok(LegacyLockEntry::Removed)
}

/// The warning a sweep reports for a pre-rename entry another process holds.
///
/// The failure this names is not an I/O error: the directory is fine and the
/// sweep simply must not touch it. A reader needs the path to delete by hand
/// once nothing runs, and needs to know that the reason it survived is a live
/// user rather than a permission problem.
fn legacy_entry_in_use_warning(entry_dir: &Path) -> String {
    format!(
        "kept the tool payload directory '{}': it still carries the pre-rename lock '{}', which \
         another process holds; remove it once no mediapm process runs",
        entry_dir.display(),
        LEGACY_LOCK_FILE_NAME
    )
}

/// Removes provisioning entries that have not been used within `TTL_SECONDS`,
/// answering one warning per directory it could not remove.
///
/// The answer reaches a user only through the automatic sweep's log, because
/// this runs where nobody is waiting on it; the alternative — dropping the
/// failure — leaves a directory that every later sweep also fails to remove
/// with nothing anywhere saying so.
pub(crate) fn prune_expired_entries(
    tools_dir: &Path,
    now: u64,
) -> Result<Vec<String>, ConductorError> {
    let mut warnings = Vec::new();
    if !tools_dir.exists() {
        return Ok(warnings);
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
        match remove_legacy_lock_entry(&path) {
            Ok(LegacyLockEntry::Absent) => {}
            Ok(LegacyLockEntry::Removed) => continue,
            Ok(LegacyLockEntry::InUse) => {
                warnings.push(legacy_entry_in_use_warning(&path));
                continue;
            }
            Err(source) => {
                warnings.push(removal_warning(
                    &path,
                    "expired, under the legacy lock name",
                    &source,
                ));
                continue;
            }
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

        if let Err(source) = fs::remove_dir_all(&path) {
            warnings.push(removal_warning(&path, "expired", &source));
        }
    }

    Ok(warnings)
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

    /// A pre-rename entry whose old lock is still held is kept and reported.
    ///
    /// The lock under `.lock` is the one guard that still works on such a
    /// directory: a build that predates the rename takes it for as long as it
    /// uses the payload, so deleting the directory on a name test alone deletes
    /// bytes that build may be executing. Once nothing holds it, the same sweep
    /// reclaims the directory — the two halves are one test because the point is
    /// that the check reads the lock, not that a directory is forever exempt.
    #[tokio::test]
    async fn regression_a_legacy_lock_directory_in_use_is_kept() {
        let tools_dir = artifact_dir().expect("tools dir");
        seed_entry(tools_dir.path(), "legacy-in-use", LEGACY_LOCK_FILE_NAME, 0);

        let legacy_lock_path = tools_dir.path().join("legacy-in-use").join(LEGACY_LOCK_FILE_NAME);
        // Shared, because that is how a build that predates the rename holds it
        // while it executes the payload.
        let held_by_older_build = fs::OpenOptions::new()
            .read(true)
            .write(true)
            .open(&legacy_lock_path)
            .expect("open the legacy lock file as an older build would");
        held_by_older_build.try_lock_shared().expect("an older build holds the shared lock");

        let warnings = retain_only_tool_dirs(tools_dir.path().to_path_buf(), HashSet::new())
            .await
            .expect("retain");

        assert!(
            tools_dir.path().join("legacy-in-use").exists(),
            "a pre-rename payload another process holds is not this sweep's to delete"
        );
        assert_eq!(warnings.len(), 1, "the kept directory has to be reported: {warnings:?}");
        assert!(
            warnings[0].contains("legacy-in-use"),
            "the report has to name the directory: {warnings:?}"
        );
        assert!(
            warnings[0].contains("another process holds"),
            "the report has to say why it was kept: {warnings:?}"
        );

        drop(held_by_older_build);
        retain_only_tool_dirs(tools_dir.path().to_path_buf(), HashSet::new())
            .await
            .expect("retain");
        assert!(
            !tools_dir.path().join("legacy-in-use").exists(),
            "once nothing holds the old lock, the sweep reclaims the directory"
        );
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

    /// Sets an entry directory's own mode, which is what decides whether this
    /// account can list it.
    ///
    /// `0o300` is the mode that makes `remove_dir_all` fail while the entry's
    /// lock file still opens: opening a child needs search permission, which the
    /// mode keeps, and listing the directory needs read permission, which the
    /// mode drops.
    #[cfg(unix)]
    fn set_entry_dir_mode(path: &Path, mode: u32) {
        use std::os::unix::fs::PermissionsExt;
        let mut permissions = fs::metadata(path).expect("entry directory metadata").permissions();
        permissions.set_mode(mode);
        fs::set_permissions(path, permissions).expect("set entry directory mode");
    }

    /// A directory the sweep cannot remove is reported, and the sweep still
    /// removes the other stale entries.
    ///
    /// Both halves matter. Swallowing the failure leaves a directory that no
    /// later sweep reports either, so the store keeps it for the life of the
    /// workspace with nothing saying why; aborting the pass instead leaves every
    /// other stale entry behind because of one directory that would not go.
    #[cfg(unix)]
    #[tokio::test]
    async fn regression_a_failed_tool_dir_removal_is_reported() {
        let tools_dir = artifact_dir().expect("tools dir");
        seed_entry(tools_dir.path(), "blocked", LOCK_FILE_NAME, 0);
        seed_entry(tools_dir.path(), "removable", LOCK_FILE_NAME, 0);
        seed_entry(tools_dir.path(), "kept", LOCK_FILE_NAME, 0);
        let blocked = tools_dir.path().join("blocked");
        set_entry_dir_mode(&blocked, 0o300);

        let warnings = retain_only_tool_dirs(
            tools_dir.path().to_path_buf(),
            HashSet::from(["kept".to_string()]),
        )
        .await
        .expect("a sweep that cannot remove one entry still finishes");

        assert!(
            !tools_dir.path().join("removable").exists(),
            "one unremovable entry must not stop the sweep removing the others"
        );
        assert!(
            tools_dir.path().join("kept").exists(),
            "the retain set is decided before removal, so an active entry stays"
        );

        if blocked.exists() {
            set_entry_dir_mode(&blocked, 0o700);
            assert_eq!(
                warnings.len(),
                1,
                "the entry that would not go has to be reported exactly once: {warnings:?}"
            );
            assert!(
                warnings[0].contains("blocked"),
                "the report has to name the directory to delete by hand: {warnings:?}"
            );
            assert!(
                warnings[0].contains("could not remove"),
                "the report has to say what failed: {warnings:?}"
            );
        } else {
            // An account that ignores a directory's own permission bits — root
            // on unix — removes the entry, so there is nothing to report. The
            // other two assertions above still hold, and this branch is
            // announced so a reader of the output is not told the reporting
            // half ran when it did not.
            eprintln!(
                "note: this account ignores permission bits, so the reported-failure half of \
                 regression_a_failed_tool_dir_removal_is_reported did not run"
            );
            assert!(
                warnings.is_empty(),
                "a sweep that removed every entry has nothing to report: {warnings:?}"
            );
        }
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
        let _serialized = crate::provision::SWEEP_STATE_LOCK.lock().await;
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
