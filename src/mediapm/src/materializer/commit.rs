//! Path validation, readonly enforcement, and filesystem helpers.
//!
//! Two concerns live here. The first is the hierarchy **path-component
//! validation chain** ([`parse_relative_path_components`] and
//! [`sanitize_and_validate_components`]): NFD normalization,
//! reserved-character sanitization, and strict per-component validation
//! including `.`/`..` traversal rejection. Its production entry point is
//! [`crate::materializer::sanitize_and_validate_hierarchy_paths`], which the
//! materializer runs over every resolved hierarchy entry before any path is
//! staged or written, and the ZIP extraction path in
//! [`crate::materializer::zip_reader`]. The second is **readonly enforcement** for
//! managed outputs ([`ensure_managed_path_readonly`]) and **stale-path
//! removal** ([`remove_path`]).
//!
//! The component type itself and the per-character invariants live in
//! [`crate::path_component`], because `config` and `materializer` both name
//! it and neither module should sit below the other.

use std::collections::BTreeMap;
use std::fs;
use std::path::{Path, PathBuf};

use crate::config::hierarchy_types::SanitizeNamesConfig;
use crate::error::MediaPmError;
use crate::path_component::{PathComponent, SanitizePolicy};

/// Removes one path recursively when it is a directory, or as one file otherwise.
pub(super) fn remove_path(path: &Path) -> Result<(), MediaPmError> {
    clear_path_readonly_recursively(path)?;

    // On Unix, removing a child entry requires write permission on the parent
    // directory. The parent directory is usually already writable, but may be
    // read-only when it is itself a managed output that was marked read-only by
    // `ensure_managed_path_readonly()` during a previous materialization cycle
    // (e.g. a hierarchy folder node containing stale file entries).
    clear_directory_writable(path)?;

    let metadata = fs::symlink_metadata(path).map_err(|source| MediaPmError::Io {
        operation: "reading path metadata before removal".to_string(),
        path: path.to_path_buf(),
        source,
    })?;

    if metadata.is_dir() {
        fs::remove_dir_all(path).map_err(|source| MediaPmError::Io {
            operation: "removing stale directory".to_string(),
            path: path.to_path_buf(),
            source,
        })
    } else {
        fs::remove_file(path).map_err(|source| MediaPmError::Io {
            operation: "removing stale file".to_string(),
            path: path.to_path_buf(),
            source,
        })
    }
}

/// Marks one managed output path as read-only after successful materialization.
///
/// For directory outputs, this recursively marks descendant files/directories
/// read-only. For symlinks, permissions are applied to the resolved target.
pub(super) fn ensure_managed_path_readonly(path: &Path) -> Result<(), MediaPmError> {
    let metadata = fs::symlink_metadata(path).map_err(|source| MediaPmError::Io {
        operation: "reading managed output metadata before readonly enforcement".to_string(),
        path: path.to_path_buf(),
        source,
    })?;

    if metadata.is_dir() {
        for entry in fs::read_dir(path).map_err(|source| MediaPmError::Io {
            operation: "reading managed output directory before readonly enforcement".to_string(),
            path: path.to_path_buf(),
            source,
        })? {
            let entry = entry.map_err(|source| MediaPmError::Io {
                operation: "iterating managed output directory before readonly enforcement"
                    .to_string(),
                path: path.to_path_buf(),
                source,
            })?;
            ensure_managed_path_readonly(&entry.path())?;
        }
    }

    let mut permissions = fs::metadata(path)
        .map_err(|source| MediaPmError::Io {
            operation: "reading managed output permissions before readonly enforcement".to_string(),
            path: path.to_path_buf(),
            source,
        })?
        .permissions();
    if !permissions.readonly() {
        permissions.set_readonly(true);
        fs::set_permissions(path, permissions).map_err(|source| MediaPmError::Io {
            operation: "marking managed output path readonly".to_string(),
            path: path.to_path_buf(),
            source,
        })?;
    }

    Ok(())
}

/// Clears read-only bit recursively so stale managed paths can be removed.
///
/// A symlink owns nothing that gates its own removal: unlinking one needs a
/// writable parent directory, which [`clear_directory_writable`] clears, and
/// `chmod` on a link reaches the target rather than the link. So a link whose
/// target exists is cleared through that target, and a link whose target is
/// gone has nothing left to clear and reaches the unlink instead of failing.
///
/// On BSD platforms (macOS, FreeBSD, etc.) this also clears the user/system
/// immutable flags (`UF_IMMUTABLE` / `SF_IMMUTABLE` / `uchg` / `schg`) which
/// prevent file deletion independently of Unix permission bits.
fn clear_path_readonly_recursively(path: &Path) -> Result<(), MediaPmError> {
    let metadata = fs::symlink_metadata(path).map_err(|source| MediaPmError::Io {
        operation: "reading path metadata before readonly clear".to_string(),
        path: path.to_path_buf(),
        source,
    })?;

    if metadata.is_dir() {
        for entry in fs::read_dir(path).map_err(|source| MediaPmError::Io {
            operation: "reading directory before readonly clear".to_string(),
            path: path.to_path_buf(),
            source,
        })? {
            let entry = entry.map_err(|source| MediaPmError::Io {
                operation: "iterating directory before readonly clear".to_string(),
                path: path.to_path_buf(),
                source,
            })?;
            clear_path_readonly_recursively(&entry.path())?;
        }
    }

    // On BSD platforms (macOS, FreeBSD, etc.), clear immutable file flags
    // that prevent deletion independently of Unix permission bits.
    // These flags can be inherited from tool outputs, set by backup software,
    // or applied manually by the user.
    #[cfg(any(
        target_os = "macos",
        target_os = "freebsd",
        target_os = "netbsd",
        target_os = "openbsd"
    ))]
    {
        clear_bsd_immutable_flags(path)?;
    }

    let mut permissions = match fs::metadata(path) {
        Ok(target) => target.permissions(),
        // A symlink that resolves to nothing: the doc rule above says there is
        // nothing of its own to clear, so the removal continues. Every other
        // unreadable path still reports, because a non-symlink the walk has
        // already resolved failing to resolve again is a real I/O failure.
        Err(_) if metadata.file_type().is_symlink() => return Ok(()),
        Err(source) => {
            return Err(MediaPmError::Io {
                operation: "reading path permissions before readonly clear".to_string(),
                path: path.to_path_buf(),
                source,
            });
        }
    };
    if permissions.readonly() {
        #[cfg(unix)]
        {
            use std::os::unix::fs::PermissionsExt;

            let mode = permissions.mode();
            let writable_mode = mode | 0o200;
            if writable_mode != mode {
                permissions.set_mode(writable_mode);
            }
        }

        #[cfg(not(unix))]
        {
            #[expect(
                clippy::permissions_set_readonly_false,
                reason = "on non-Unix platforms we must clear the readonly flag before managed overwrite/delete operations can succeed"
            )]
            {
                permissions.set_readonly(false);
            }
        }

        fs::set_permissions(path, permissions).map_err(|source| MediaPmError::Io {
            operation: "clearing readonly bit before managed-path removal".to_string(),
            path: path.to_path_buf(),
            source,
        })?;
    }

    Ok(())
}

/// Clears BSD immutable flags (`UF_IMMUTABLE` / `SF_IMMUTABLE`) on the given
/// path so the file can be removed.
///
/// Both calls follow symlinks, so the flags cleared here belong to the target
/// whenever the path is a symlink with a target. A symlink with no target
/// carries no flags of its own to clear: `stat` fails, this is a no-op, and
/// [`clear_path_readonly_recursively`] carries on to the unlink.
#[cfg(any(
    target_os = "macos",
    target_os = "freebsd",
    target_os = "netbsd",
    target_os = "openbsd"
))]
fn clear_bsd_immutable_flags(path: &Path) -> Result<(), MediaPmError> {
    use std::ffi::CString;
    use std::os::unix::ffi::OsStrExt;

    let c_path = CString::new(path.as_os_str().as_bytes())
        .map_err(|_| MediaPmError::Workflow("path contains null byte for chflags".to_string()))?;

    // Read current flags via stat (follows symlinks, matching fs::metadata behavior).
    let mut st: libc::stat = unsafe { std::mem::zeroed() };
    if unsafe { libc::stat(c_path.as_ptr(), &raw mut st) } != 0 {
        // Path may not exist; let the caller's fs::metadata surface the error.
        return Ok(());
    }

    let immutable_mask = libc::UF_IMMUTABLE | libc::SF_IMMUTABLE;
    if (st.st_flags as u32) & immutable_mask != 0 {
        let new_flags = (st.st_flags as u32) & !immutable_mask;
        if unsafe { libc::chflags(c_path.as_ptr(), new_flags) } != 0 {
            let err = std::io::Error::last_os_error();
            return Err(MediaPmError::Io {
                operation: "clearing immutable flags before managed-path removal".to_string(),
                path: path.to_path_buf(),
                source: err,
            });
        }
    }

    Ok(())
}

/// Ensures the parent directory of `path` is writable so the child entry can
/// be removed.
///
/// On Unix, unlinking or renaming a child requires write permission on the
/// containing directory. Managed directory outputs may be read-only when they
/// are themselves part of a previously materialized hierarchy tree. This
/// helper clears the readonly bit and BSD immutable flags on the parent
/// (without recursing into sibling entries).
fn clear_directory_writable(path: &Path) -> Result<(), MediaPmError> {
    let Some(parent) = path.parent() else { return Ok(()) };

    #[cfg(any(
        target_os = "macos",
        target_os = "freebsd",
        target_os = "netbsd",
        target_os = "openbsd"
    ))]
    {
        clear_bsd_immutable_flags(parent)?;
    }

    let Ok(metadata) = fs::metadata(parent) else { return Ok(()) };
    let mut permissions = metadata.permissions();
    if permissions.readonly() {
        #[cfg(unix)]
        {
            use std::os::unix::fs::PermissionsExt;

            let mode = permissions.mode();
            let writable_mode = mode | 0o200;
            if writable_mode != mode {
                permissions.set_mode(writable_mode);
            }
        }

        #[cfg(not(unix))]
        {
            #[expect(
                clippy::permissions_set_readonly_false,
                reason = "on non-Unix platforms we must clear the readonly flag before managed delete operations can succeed"
            )]
            {
                permissions.set_readonly(false);
            }
        }

        fs::set_permissions(parent, permissions).map_err(|source| MediaPmError::Io {
            operation: "clearing readonly bit on parent directory before removal".to_string(),
            path: parent.to_path_buf(),
            source,
        })?;
    }

    Ok(())
}

/// Reserved characters rewritten by [`default_sanitize_replacements`].
///
/// Mirrors the character set rejected by `is_rejected_char`, so
/// [`SanitizeNamesConfig::Enabled`] rewrites exactly the characters that
/// would otherwise fail validation. `sanitized_reserved_chars_match_is_rejected_char`
/// pins the two literals together, because "rewrite under `Enabled`, reject
/// under `Disabled`" is a contract that only holds while they agree.
pub(super) const SANITIZED_RESERVED_CHARS: [char; 9] =
    ['<', '>', ':', '"', '|', '?', '*', '/', '\\'];

/// Builds the default replacement map applied under
/// [`SanitizeNamesConfig::Enabled`]: every reserved character listed in
/// [`SANITIZED_RESERVED_CHARS`] becomes `_`.
///
/// This is the concrete meaning of the `Enabled` variant's documented
/// "reserved chars to `_`" policy. Per-entry [`SanitizeNamesConfig::Custom`]
/// maps are layered on top of this map by
/// [`sanitize_and_validate_components`].
#[must_use]
pub(super) fn default_sanitize_replacements() -> BTreeMap<char, char> {
    SANITIZED_RESERVED_CHARS.into_iter().map(|ch| (ch, '_')).collect()
}

/// Builds the [`SanitizePolicy`] a [`SanitizeNamesConfig`] selects.
///
/// `Disabled` and `Inherit` both yield [`SanitizePolicy::disabled`]:
/// `Inherit` is resolved to its effective value before the materializer runs
/// this chain, and a value that reaches here unresolved falls back to the
/// no-rewrite behavior rather than silently sanitizing. `Enabled` yields the
/// default replacement map alone; `Custom` layers the per-entry map on top.
pub(super) fn sanitize_policy_for(
    sanitize_names: &SanitizeNamesConfig,
    default_replacements: &BTreeMap<char, char>,
) -> SanitizePolicy {
    match sanitize_names {
        SanitizeNamesConfig::Enabled => {
            SanitizePolicy::with_replacements(default_replacements, None)
        }
        SanitizeNamesConfig::Custom(custom) => {
            SanitizePolicy::with_replacements(default_replacements, Some(custom))
        }
        SanitizeNamesConfig::Disabled | SanitizeNamesConfig::Inherit => SanitizePolicy::disabled(),
    }
}

/// Joins validated components into one relative [`PathBuf`].
///
/// # Panics
///
/// Never: each component is a single validated path segment, so joining them
/// cannot produce a separator or a traversal component.
pub(super) fn join_path_components(components: &[PathComponent]) -> PathBuf {
    components.iter().map(|component| PathBuf::from(component.to_string())).collect()
}

/// Parses every component of one untrusted relative path under `policy`.
///
/// Both `/` and `\` are treated as structural separators, so a Windows-style
/// `..\..\evil.txt` is split rather than smuggled through as one opaque
/// component. Separators left after splitting would still be rejected by
/// [`PathComponent::parse`], so the split narrows the input, never widens it.
///
/// # Errors
///
/// Propagates the first component failure from [`PathComponent::parse`].
pub(super) fn parse_relative_path_components(
    path: &Path,
    policy: &SanitizePolicy,
) -> Result<Vec<PathComponent>, MediaPmError> {
    path.to_string_lossy()
        .split(['/', '\\'])
        .filter(|component| !component.is_empty())
        .map(|component| PathComponent::parse(component, policy))
        .collect()
}

/// Applies NFD normalization, optional reserved-character sanitization, and
/// strict validation to resolved hierarchy path components.
///
/// Every component is parsed through [`PathComponent::parse`] under the
/// [`SanitizePolicy`] that `sanitize_names` selects, so the returned values
/// cannot exist without having passed the parser.
///
/// # Errors
///
/// Propagates the first component failure, naming the offending component.
///
/// Production entry point of the chain: the materializer calls this for every
/// flattened hierarchy entry, after metadata interpolation and before any
/// path is staged, verified, or committed (see
/// [`crate::materializer::sanitize_and_validate_hierarchy_paths`]).
pub(super) fn sanitize_and_validate_components(
    components: &[String],
    sanitize_names: &SanitizeNamesConfig,
    default_replacements: &BTreeMap<char, char>,
) -> Result<Vec<PathComponent>, MediaPmError> {
    let policy = sanitize_policy_for(sanitize_names, default_replacements);
    components.iter().map(|component| PathComponent::parse(component, &policy)).collect()
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::config::{MediaPmDocument, MediaPmState};
    use crate::materializer::tests_common::{open_hierarchy_cas, resolvable_media_document};
    use crate::materializer::{MaterializeReport, sync_hierarchy};
    use crate::path_component::is_rejected_char;
    use crate::paths::MediaPmPaths;
    use mediapm_cas::{CasApi, FileSystemCas};
    use mediapm_conductor::{ConductorState, NickelDocument};

    /// Hierarchy path of the single entry [`a_dangling_symlink_output_is_replaced_by_the_next_run`]
    /// re-syncs.
    const SYNC_ENTRY_PATH: &str = "song";

    /// Bytes the entry in that test is materialized from.
    const SYNC_ENTRY_PAYLOAD: &[u8] = b"dangling-symlink-output";

    /// Runs `sync_hierarchy` over `document` with no progress output.
    ///
    /// The bare call the materializer's other test modules wrap for their own
    /// shape. It sits here so the removal cases below read as one whole sync
    /// each rather than as a helper plus a wall of arguments.
    async fn run_materialize(
        paths: &MediaPmPaths,
        document: &MediaPmDocument,
        state: &mut MediaPmState,
        cas: &FileSystemCas,
    ) -> MaterializeReport {
        sync_hierarchy(
            paths,
            document,
            state,
            cas,
            false,
            &ConductorState::new_empty(),
            &NickelDocument::default(),
            None,
            None,
        )
        .await
        .expect("a run over one resolvable entry either writes it or says why it did not")
    }

    /// The replacement source and the rejection predicate are two literals of
    /// the same nine characters, and this pins them together.
    ///
    /// [`default_sanitize_replacements`] builds the rewrite map from
    /// [`SANITIZED_RESERVED_CHARS`] while [`is_rejected_char`] answers the
    /// reject question as a separate `matches!`, so the "rewrite under
    /// `Enabled`, reject under `Disabled`" contract holds only while the two
    /// agree. The sweep covers ASCII and near misses in both directions: a
    /// character added to either literal alone is a component the sanitizer
    /// rewrites and the reject policy accepts, or one it refuses and never
    /// rewrites.
    #[test]
    fn sanitized_reserved_chars_match_is_rejected_char() {
        for ch in '\0'..=char::from(u8::MAX) {
            assert_eq!(
                SANITIZED_RESERVED_CHARS.contains(&ch),
                is_rejected_char(ch),
                "SANITIZED_RESERVED_CHARS and is_rejected_char disagree on {ch:?} (U+{:04X})",
                u32::from(ch),
            );
        }
        // The predicate is the shared definition for the config boundary, so it
        // must not be empty: an empty set would make both literals agree while
        // rejecting nothing at all.
        assert!(SANITIZED_RESERVED_CHARS.iter().all(|&ch| is_rejected_char(ch)));
    }

    #[test]
    fn parse_relative_path_splits_backslash_form() {
        let parsed = parse_relative_path_components(
            Path::new("..\\..\\evil.txt"),
            &SanitizePolicy::disabled(),
        )
        .unwrap_err();
        assert!(parsed.to_string().contains("must not be '.' or '..'"));
    }

    #[test]
    fn parse_relative_path_joins_nested_members() {
        let parsed =
            parse_relative_path_components(Path::new("a/b/c.txt"), &SanitizePolicy::disabled())
                .unwrap();
        assert_eq!(join_path_components(&parsed), PathBuf::from("a/b/c.txt"));
    }

    #[test]
    fn sanitize_and_validate_components_disabled() {
        let components = vec!["a<b".to_string()];
        let err = sanitize_and_validate_components(
            &components,
            &SanitizeNamesConfig::Disabled,
            &BTreeMap::new(),
        )
        .unwrap_err();
        assert!(err.to_string().contains("forbidden characters"));
    }

    #[test]
    fn sanitize_and_validate_components_enabled() {
        let components = vec!["a<b".to_string()];
        let replacements = BTreeMap::from([('<', '_')]);
        let result = sanitize_and_validate_components(
            &components,
            &SanitizeNamesConfig::Enabled,
            &replacements,
        )
        .unwrap();
        let rendered: Vec<String> = result.iter().map(PathComponent::to_string).collect();
        assert_eq!(rendered, vec!["a_b".to_string()]);
    }

    #[test]
    fn sanitize_and_validate_components_custom_layers_over_defaults() {
        let components = vec!["a<b:c".to_string()];
        let defaults = BTreeMap::from([('<', '_'), (':', '#')]);
        let custom = BTreeMap::from([(':', '%')]);
        let result = sanitize_and_validate_components(
            &components,
            &SanitizeNamesConfig::Custom(custom),
            &defaults,
        )
        .unwrap();
        let rendered: Vec<String> = result.iter().map(PathComponent::to_string).collect();
        assert_eq!(rendered, vec!["a_b%c".to_string()]);
    }

    #[test]
    fn readonly_file() {
        let dir = mediapm_utils::temp::artifact_dir().unwrap();
        let file_path = dir.path().join("test.txt");
        std::fs::write(&file_path, b"data").unwrap();

        ensure_managed_path_readonly(&file_path).unwrap();

        assert!(file_path.metadata().unwrap().permissions().readonly());
    }

    #[test]
    fn readonly_directory() {
        let dir = mediapm_utils::temp::artifact_dir().unwrap();
        let sub = dir.path().join("subdir");
        std::fs::create_dir(&sub).unwrap();
        let child = sub.join("child.txt");
        std::fs::write(&child, b"child").unwrap();

        ensure_managed_path_readonly(&sub).unwrap();

        assert!(sub.metadata().unwrap().permissions().readonly());
        assert!(child.metadata().unwrap().permissions().readonly());

        // The `TempDir` guard's plain `remove_dir_all` silently fails on the
        // readonly-marked subtree (on Unix, unlinking `child.txt` requires
        // write on its parent `sub`), which would leak the tempdir from
        // `$TMPDIR`. Clear the bits via the retry helper so the guard has
        // nothing left to remove.
        mediapm_utils::temp::remove_dir_all_with_retry(dir.path())
            .expect("cleanup of readonly-marked tree");
    }

    #[test]
    fn remove_file() {
        let dir = mediapm_utils::temp::artifact_dir().unwrap();
        let file_path = dir.path().join("toremove.txt");
        std::fs::write(&file_path, b"data").unwrap();

        remove_path(&file_path).unwrap();

        assert!(!file_path.exists());
    }

    #[test]
    fn remove_dir() {
        let dir = mediapm_utils::temp::artifact_dir().unwrap();
        let sub = dir.path().join("subdir");
        std::fs::create_dir(&sub).unwrap();
        std::fs::write(sub.join("a.txt"), b"a").unwrap();
        std::fs::write(sub.join("b.txt"), b"b").unwrap();

        remove_path(&sub).unwrap();

        assert!(!sub.exists());
    }

    /// A live symlink still has its target cleared, and a dead one reaches the
    /// unlink that replaces it.
    ///
    /// `sync_hierarchy` writes through hardlinks and produces no symlink of its
    /// own, so a symlink sitting at a managed output appeared between two runs:
    /// a link the user made, or one whose CAS target was reclaimed. Unlinking it
    /// never depends on the target's mode bits, so a link with no target has
    /// nothing to clear. Before this case was handled, resolving that link to
    /// read its permissions failed the whole removal, and every later sync
    /// failed on the same entry. The live case sits in the same test because it
    /// is the one the change must leave alone: its target's readonly bit is
    /// still cleared, and the link is still removed.
    #[cfg(unix)]
    #[test]
    fn remove_live_and_dangling_symlinks() {
        use std::os::unix::fs::symlink;

        let dir = mediapm_utils::temp::artifact_dir().unwrap();

        // Live link: the target carries the readonly bit and the clear reaches
        // it through the link.
        let target_path = dir.path().join("live-target");
        std::fs::write(&target_path, b"payload").unwrap();
        let mut read_only = std::fs::metadata(&target_path).unwrap().permissions();
        read_only.set_readonly(true);
        std::fs::set_permissions(&target_path, read_only).unwrap();
        let live_link = dir.path().join("live.link");
        symlink(&target_path, &live_link).unwrap();

        clear_path_readonly_recursively(&live_link).unwrap();
        assert!(
            !std::fs::metadata(&target_path).unwrap().permissions().readonly(),
            "the live link has to keep clearing its target, or a target the sync marked read-only \
             stays read-only after the entry is replaced"
        );

        remove_path(&live_link).unwrap();
        assert!(
            std::fs::symlink_metadata(&live_link).is_err(),
            "a live link must still be removable through the old path"
        );

        // Dead link: nothing resolves it, so the removal has to go through, and
        // the path must be writable for the write the same run performs.
        let missing = dir.path().join("reclaimed-target");
        let dead_link = dir.path().join("dead.link");
        symlink(&missing, &dead_link).unwrap();

        remove_path(&dead_link).unwrap();
        assert!(
            std::fs::symlink_metadata(&dead_link).is_err(),
            "a dangling symlink must not survive the removal that replaces it"
        );

        std::fs::write(&dead_link, b"replacement").unwrap();
        assert_eq!(
            std::fs::read(&dead_link).unwrap(),
            b"replacement",
            "the entry the dead link occupied must take the content of the next sync"
        );
    }

    /// The readonly clear still reaches a regular read-only file and every
    /// entry under a read-only directory.
    ///
    /// The dead-link case is a skip, and a skip that was not scoped to a
    /// symlink would make removal report success for paths it never inspected.
    /// This pins the other side of the same match arm: a file the walk can
    /// resolve has its bit cleared, the recursion still descends, and the
    /// removal that follows goes through.
    #[test]
    fn clear_readonly_entries_still_clears_the_readonly_bit() {
        let dir = mediapm_utils::temp::artifact_dir().unwrap();
        let readonly_dir = dir.path().join("subdir");
        std::fs::create_dir(&readonly_dir).unwrap();
        let readonly_child = readonly_dir.join("child.txt");
        std::fs::write(&readonly_child, b"child").unwrap();
        ensure_managed_path_readonly(&readonly_dir).unwrap();
        assert!(
            readonly_child.metadata().unwrap().permissions().readonly(),
            "the fixture must start from a read-only entry, or the assertions below prove nothing"
        );

        clear_path_readonly_recursively(&readonly_dir).unwrap();

        assert!(!readonly_child.metadata().unwrap().permissions().readonly());
        assert!(!readonly_dir.metadata().unwrap().permissions().readonly());

        remove_path(&readonly_dir).unwrap();
        assert!(!readonly_dir.exists());
    }

    /// A managed output that is a regular read-only file is cleared, then removed.
    ///
    /// This is the case the clearing was written for. Nothing about it
    /// changed, and nothing about it should: a fix for the symlink cases
    /// below is allowed to move the clearing, not to narrow it.
    ///
    /// The two halves are checked apart on purpose. Unlinking a read-only file
    /// needs write permission on its parent and not on the file, so a removal
    /// that leaves the file's own bit set can still pass. Calling the clearing
    /// on its own is what pins the bit actually being cleared, and the removal
    /// after it pins the entry really going.
    #[test]
    fn remove_readonly_file() {
        let dir = mediapm_utils::temp::artifact_dir().unwrap();
        let file_path = dir.path().join("readonly_remove.txt");
        std::fs::write(&file_path, b"data").unwrap();

        let mut perms = file_path.metadata().unwrap().permissions();
        perms.set_readonly(true);
        std::fs::set_permissions(&file_path, perms).unwrap();
        assert!(file_path.metadata().unwrap().permissions().readonly());

        clear_path_readonly_recursively(&file_path).unwrap();
        assert!(
            file_path.exists(),
            "clearing the read-only bit is not a removal: the entry has to still be there to be \
             removed"
        );
        assert!(
            !file_path.metadata().unwrap().permissions().readonly(),
            "the clearing has to reach the file's own permissions, not only its parent's"
        );

        let mut perms = file_path.metadata().unwrap().permissions();
        perms.set_readonly(true);
        std::fs::set_permissions(&file_path, perms).unwrap();

        remove_path(&file_path).unwrap();
        assert!(file_path.symlink_metadata().is_err());
    }

    #[cfg(unix)]
    #[tokio::test]
    async fn a_dangling_symlink_output_is_replaced_by_the_next_run() {
        let root = mediapm_utils::temp::artifact_dir().unwrap();
        let paths = MediaPmPaths::from_root(root.path());
        let cas = open_hierarchy_cas(&paths).await;
        let hash = cas.put(bytes::Bytes::from_static(SYNC_ENTRY_PAYLOAD)).await.unwrap();
        let document = resolvable_media_document(&hash.to_string());
        let mut state = MediaPmState::default();

        let first = run_materialize(&paths, &document, &mut state, &cas).await;
        assert_eq!(
            (first.materialized_paths, first.missing_paths),
            (1, 0),
            "the first run has nothing recorded for the entry, so it must write it: {first:?}"
        );

        // Point the entry at a sibling that was never there.
        let output = paths.hierarchy_root_dir.join(SYNC_ENTRY_PATH);
        std::fs::remove_file(&output).unwrap();
        std::os::unix::fs::symlink(output.with_file_name("absent"), &output).unwrap();
        assert!(
            output.metadata().is_err(),
            "the entry has to be a dead link for this test to be about anything"
        );

        let second = run_materialize(&paths, &document, &mut state, &cas).await;
        assert_eq!(
            (second.materialized_paths, second.missing_paths),
            (1, 0),
            "a dead link holds no content to keep, so the run must write the entry again and leave \
             nothing missing: {second:?}"
        );
        assert_eq!(
            std::fs::read(&output).unwrap(),
            SYNC_ENTRY_PAYLOAD,
            "the entry has to be the file the document resolved, not the link that was there"
        );
    }
}
