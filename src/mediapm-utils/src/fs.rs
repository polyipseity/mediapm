//! Atomic publication of a document's bytes.
//!
//! A document that a later run must be able to parse is never rewritten in
//! place. [`write_atomically`] writes a sibling temporary file and renames it
//! over the target, so the only version of the document that ever exists at the
//! path is a whole one: a reader sees the previous document or the new one, and a
//! crash between the two leaves the previous one intact.

use std::fs::File;
use std::io::{self, Write};
use std::path::{Path, PathBuf};

/// Publishes `bytes` at `path` by writing a sibling file and renaming it over
/// the target.
///
/// # Errors
///
/// Returns an I/O error when the temporary file cannot be created or written,
/// when the rename fails, or when the target's directory does not exist.
pub fn write_bytes_atomically(path: &Path, bytes: &[u8]) -> io::Result<()> {
    write_atomically(path, |file| file.write_all(bytes))
}

/// Runs `write` against a temporary sibling of `path` and renames it into place.
///
/// A rename within one directory is the only replacement the filesystem makes
/// atomic, so the temporary file lives beside the target rather than in the
/// system temp directory, which may be another filesystem. The temporary name
/// carries the process id and a per-call counter, so no two writers of one path
/// share a temporary: another process differs in the id, and another save in
/// this one differs in the counter. A save that fails removes its own temporary.
/// A killed writer cannot, so it leaves `.tmp-<pid>-<n>` beside the target;
/// nothing sweeps that file, and the next save names a different one and ignores
/// it.
///
/// A save keeps the target's permissions. The rename replaces the target's
/// inode with the temporary's, so without copying the mode across, a document a
/// user restricted would come back with the mode a fresh file gets. The copy is
/// best-effort: a target whose metadata cannot be read is not a reason to lose
/// the document the caller asked to publish, and the save proceeds.
///
/// After the rename the parent directory is synced, which makes the new name
/// durable rather than only the bytes it points at. That sync is best-effort
/// because opening a directory as a file is not portable — Windows refuses it —
/// and losing it costs durability of the rename across a power cut, not
/// atomicity.
///
/// The directory `path` lives in must already exist: this publishes into a root
/// its owner created and never creates one. A save that could create its own
/// parent would resurrect a directory a teardown had just removed.
///
/// Split from [`write_bytes_atomically`] so a caller's test can fail the write
/// halfway and observe what the target holds afterwards. That is the state a
/// crash leaves behind, and it is the reason the temporary file exists.
///
/// # Errors
///
/// Returns whatever `write` returns, or the I/O error from creating, syncing or
/// renaming the temporary file. Returns [`io::ErrorKind::InvalidInput`] when
/// `path` has no final component, because such a path names no sibling to build
/// a temporary from.
pub fn write_atomically(
    path: &Path,
    write: impl FnOnce(&mut File) -> io::Result<()>,
) -> io::Result<()> {
    let temporary = temporary_path(path)?;

    let publish = |temporary: &Path| -> io::Result<()> {
        let existing_permissions = std::fs::metadata(path).ok().map(|meta| meta.permissions());
        let mut file = File::create(temporary)?;
        write(&mut file)?;
        file.sync_all()?;
        if let Some(permissions) = existing_permissions {
            drop(file.set_permissions(permissions));
        }
        std::fs::rename(temporary, path)?;
        if let Some(parent) = path.parent()
            && let Ok(directory) = File::open(parent)
        {
            drop(directory.sync_all());
        }
        Ok(())
    };
    publish(&temporary).inspect_err(|_error| {
        drop(std::fs::remove_file(&temporary));
    })
}

/// The temporary path one save publishes through.
///
/// Sibling of the target, never a name in another directory: only a rename inside
/// one directory is atomic, so a temporary elsewhere would make the publish a
/// copy.
///
/// # Errors
///
/// Returns [`io::ErrorKind::InvalidInput`] when `path` has no final component,
/// which is a path that names no file to replace and none to build a sibling
/// from.
fn temporary_path(path: &Path) -> io::Result<PathBuf> {
    let Some(file_name) = path.file_name() else {
        return Err(io::Error::new(
            io::ErrorKind::InvalidInput,
            format!("{} names no file to publish", path.display()),
        ));
    };
    let sequence = TEMPORARY_SEQUENCE.fetch_add(1, std::sync::atomic::Ordering::Relaxed);
    let mut temporary_name = file_name.to_os_string();
    temporary_name.push(format!(".tmp-{}-{sequence}", std::process::id()));
    Ok(path.with_file_name(temporary_name))
}

/// Counts the temporaries this process has named, so two saves of one path in
/// one process never share a temporary file.
static TEMPORARY_SEQUENCE: std::sync::atomic::AtomicU64 = std::sync::atomic::AtomicU64::new(0);

#[cfg(test)]
mod tests {
    use super::*;

    /// A document at `state.json` under a fresh artifact root, holding `contents`.
    ///
    /// The root must outlive the test body: dropping it removes the document the
    /// assertions read back.
    fn document_with(contents: &[u8]) -> (tempfile::TempDir, std::path::PathBuf) {
        let root = crate::temp::artifact_dir().expect("artifact dir");
        let path = root.path().join("state.json");
        std::fs::write(&path, contents).expect("write the initial document");
        (root, path)
    }

    /// A write that fails halfway leaves the previous document and removes the
    /// temporary.
    ///
    /// This is the state a crash leaves behind: the target still holds what the
    /// last good save published, so the next run loads a document rather than a
    /// prefix of one. `write_atomically` is the seam that makes the failure
    /// reachable in a test without a real power loss.
    #[test]
    fn an_interrupted_write_keeps_the_previous_document() {
        let previous = b"{\"version\": 5, \"managed_files\": {}, \"managed_tools\": []}";
        let (root, path) = document_with(previous);

        let error = write_atomically(&path, |file| {
            file.write_all(b"{\"version\": 5, \"managed_fil")?;
            Err(io::Error::other("the disk went away"))
        })
        .expect_err("a write that fails must be reported");

        assert_eq!(error.to_string(), "the disk went away", "the writer's error must survive");
        assert_eq!(
            std::fs::read(&path).expect("read the published document"),
            previous,
            "the previous document must survive a write that never finished",
        );
        let names: Vec<String> = std::fs::read_dir(root.path())
            .expect("read the document directory")
            .map(|entry| entry.expect("entry").file_name().to_string_lossy().into_owned())
            .collect();
        assert_eq!(names, vec!["state.json".to_string()], "the abandoned temporary must be gone");
    }

    /// A completed write publishes the new bytes and leaves no temporary.
    ///
    /// The target holds exactly what was written, which is what lets a reader
    /// treat a successful return as a complete document.
    #[test]
    fn a_completed_write_publishes_the_new_document_only() {
        let (root, path) = document_with(b"stale");

        write_bytes_atomically(&path, b"fresh").expect("publish the document");

        assert_eq!(std::fs::read(&path).expect("read the document"), b"fresh");
        let names: Vec<String> = std::fs::read_dir(root.path())
            .expect("read the document directory")
            .map(|entry| entry.expect("entry").file_name().to_string_lossy().into_owned())
            .collect();
        assert_eq!(names, vec!["state.json".to_string()], "a completed write leaves no temporary");
    }

    /// Two saves of one document inside one process use different temporaries.
    ///
    /// A name keyed on the process id alone is shared by every save in that
    /// process, so a nested save truncates the temporary its caller is still
    /// writing, renames it into place, and then leaves the caller publishing a
    /// file name that no longer exists. Its rename fails after the caller has
    /// already written into the renamed document, so the published bytes are the
    /// caller's prefix rather than its whole output. The counter in the name is
    /// what keeps each save to its own temporary.
    #[test]
    fn two_saves_in_one_process_use_different_temporaries() {
        let previous = b"previous";
        let (root, path) = document_with(previous);

        write_atomically(&path, |file| {
            write_bytes_atomically(&path, b"inner")?;
            file.write_all(b"outer")
        })
        .expect("a nested save must not break the outer one");

        assert_eq!(
            std::fs::read(&path).expect("read the published document"),
            b"outer",
            "the outer save must be the one that lands",
        );
        let names: Vec<String> = std::fs::read_dir(root.path())
            .expect("read the document directory")
            .map(|entry| entry.expect("entry").file_name().to_string_lossy().into_owned())
            .collect();
        assert_eq!(names, vec!["state.json".to_string()], "no temporary may survive");
    }

    /// A save keeps the permissions the target already had.
    ///
    /// The rename replaces the target's inode with the temporary's, so a fresh
    /// file's mode becomes the document's mode unless it is copied across. A
    /// `state.json` a user restricted to their own account would otherwise come
    /// back readable by everyone after the next sync.
    #[cfg(unix)]
    #[test]
    fn a_save_keeps_the_permissions_the_target_had() {
        use std::os::unix::fs::PermissionsExt;

        let (root, path) = document_with(b"previous");
        std::fs::set_permissions(&path, std::fs::Permissions::from_mode(0o600))
            .expect("restrict the document");

        write_bytes_atomically(&path, b"fresh").expect("publish the document");

        let mode =
            std::fs::metadata(&path).expect("read the document").permissions().mode() & 0o777;
        assert_eq!(mode, 0o600, "the save widened the document's permissions");
        drop(root);
    }

    /// A target path with no final component is refused before anything is written.
    ///
    /// Such a path has no name to build a sibling from, and the fallback built one
    /// in the target's parent directory instead, so the write landed somewhere the
    /// caller never named. No caller passes such a path, which is why the refusal
    /// can be clean rather than a documented hazard.
    #[test]
    fn a_target_without_a_file_name_is_refused() {
        let root = crate::temp::artifact_dir().expect("artifact dir");
        let path = root.path().join("gone").join("..");

        let error = write_bytes_atomically(&path, b"{}").expect_err("the write must be refused");

        assert_eq!(
            error.kind(),
            io::ErrorKind::InvalidInput,
            "a path without a file name must be refused before the write, got: {error}",
        );
        let names: Vec<String> = std::fs::read_dir(root.path())
            .expect("read the artifact root")
            .map(|entry| entry.expect("entry").file_name().to_string_lossy().into_owned())
            .collect();
        assert!(names.is_empty(), "the refused write left {names:?} behind");
    }

    /// A write into a directory that does not exist fails and creates nothing.
    ///
    /// The publish path belongs to an owner that creates its own directory; one
    /// that created the parent would resurrect a root a teardown had removed.
    #[test]
    fn a_write_into_a_missing_directory_is_refused() {
        let root = crate::temp::artifact_dir().expect("artifact dir");
        let path = root.path().join("gone").join("state.json");

        let error = write_bytes_atomically(&path, b"{}").expect_err("the write must fail");

        assert!(
            !path.exists(),
            "the write published a document into a directory it created: {error}"
        );
        assert!(
            !root.path().join("gone").exists(),
            "the write recreated the directory its owner removed",
        );
    }
}
