//! Atomic publication of a document's bytes.
//!
//! A document that a later run must be able to parse is never rewritten in
//! place. [`write_atomically`] writes a sibling temporary file and renames it
//! over the target, so the only version of the document that ever exists at the
//! path is a whole one: a reader sees the previous document or the new one, and a
//! crash between the two leaves the previous one intact.
//!
//! Each save also removes the temporaries a dead writer left beside the same
//! target, so an interrupted publish costs one stale name rather than a growing
//! pile of them. The name carries [`TEMPORARY_MARKER`], which narrows what counts
//! as a leftover of this helper from what another program put there.

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
/// A killed writer cannot, so it leaves one beside the target; the next save of
/// that target removes it, because it is older than [`STALE_TEMPORARY_AGE`]. The
/// sweep is best effort: a temporary that cannot be removed leaves the save that
/// was beside it unaffected.
///
/// A leftover named the old way, without [`TEMPORARY_MARKER`], is not swept: the
/// sweep removes only names this version of the helper builds. That is a fixed
/// set only while every writer is this version. An older installed binary keeps
/// writing the old name, and nothing reclaims what it leaves. Matching a name a
/// person's backup or editor can also produce was the larger risk.
///
/// A save keeps the target's permissions. The rename replaces the target's
/// inode with the temporary's, so without copying the mode across, a document a
/// user restricted would come back with the mode a fresh file gets. Both halves
/// of the copy are best effort: a target whose metadata cannot be read is
/// published at the mode a fresh file gets, and a mode that cannot be applied
/// leaves the document published the same way. A document the caller asked to
/// publish is worth more than the mode it lands with, and a filesystem that
/// refuses a chmod would otherwise refuse every save of a document it lets us
/// write. Nothing records a copy that did not take: this module has no logging
/// dependency, and the only place a widened mode is visible is the file itself.
///
/// Windows refuses to rename over a file whose read-only attribute is set, so a
/// document a user protected would refuse every later save. The rename is tried
/// first and, when it fails with a permission error, the target's attribute is
/// cleared and the rename is tried once more. POSIX needs only write permission
/// on the containing directory, so the clear touches nothing there.
///
/// Before the write, the target's directory is read once so the temporaries a
/// dead writer left can be removed. That enumeration is the sweep's whole cost,
/// and it is an upper bound per call rather than per document: a caller that
/// skips the publish when its bytes are unchanged — `write_bytes_if_changed` in
/// mediapm does — reads no directory at all. The read happens before the write,
/// so a slow listing delays the document rather than risking it.
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
    sweep_stale_temporaries(path);

    let publish = |temporary: &Path| -> io::Result<()> {
        let existing_permissions = std::fs::metadata(path).ok().map(|meta| meta.permissions());
        let mut file = File::create(temporary)?;
        write(&mut file)?;
        file.sync_all()?;
        if let Some(permissions) = existing_permissions {
            drop(file.set_permissions(permissions));
        }
        drop(file);
        replace(temporary, path)?;
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
    temporary_name.push(format!("{TEMPORARY_MARKER}{}-{sequence}", std::process::id()));
    Ok(path.with_file_name(temporary_name))
}

/// Counts the temporaries this process has named, so two saves of one path in
/// one process never share a temporary file.
static TEMPORARY_SEQUENCE: std::sync::atomic::AtomicU64 = std::sync::atomic::AtomicU64::new(0);

/// The part of a temporary's name that marks it as this helper's.
///
/// The sweep has to tell a leftover of a killed save from a file somebody else
/// wrote beside the same document, and only the name can carry that. A bare
/// `.tmp-<digits>-<digits>` suffix does not: `state.json.tmp-2024-01` is what a
/// backup or archive tool produces, and it has the shape a process id and a
/// counter take. The marker narrows the collision from any two numeric parts to
/// this literal, which is a name only this helper writes. It does not make the
/// sweep safe by construction: a person who types `state.json.mediapm-tmp-2024-01`
/// beside a document still produces a name the sweep treats as its own once the
/// file is an hour old. What the marker buys is that the shape alone, with no
/// marker, is never enough.
///
/// The spelling is persisted state. A leftover keeps the marker of the version
/// that wrote it, so editing this literal abandons every leftover already on
/// disk: the sweep stops matching them and nothing else reclaims them. A test
/// pins the literal for that reason.
const TEMPORARY_MARKER: &str = ".mediapm-tmp-";

/// How old a sibling temporary must be before a later save removes it.
///
/// A save holds its temporary for one write and one rename, which is
/// milliseconds, so an hour of sitting untouched is what separates a temporary a
/// dead writer left from one a running save is still using. The threshold is
/// what protects a concurrent writer: its temporary is younger, so a save by
/// another process leaves it alone. A writer whose temporary is removed anyway —
/// one stopped for over an hour, or one whose clock went backwards — fails its
/// rename with `NotFound` and reports that error, and the target keeps the
/// document it had. Removing by process liveness would need a per-platform probe
/// and still could not see a process on another host, so the age test is the
/// whole rule.
const STALE_TEMPORARY_AGE: std::time::Duration = std::time::Duration::from_hours(1);

/// Removes the temporaries a dead writer left beside `path`.
///
/// Only names carrying [`TEMPORARY_MARKER`] for this target are candidates, so a
/// sibling document and a name another program chose are never candidates. The
/// marker is a name only this helper writes. Everything here is best effort: an
/// unreadable directory, an entry that cannot be stat'd, and a temporary that
/// cannot be removed all leave the save that follows them to proceed, because the
/// save's own work is the document and this is housekeeping beside it.
///
/// The directory is read once per call, and one call is what a publish makes, so a
/// caller that skips the publish when its bytes are unchanged reads nothing. That
/// read is the sweep's whole cost and the reason it is the only one.
///
/// A path whose final component is not valid UTF-8 is skipped rather than
/// mangled, and a temporary of such a document stays where it is. A temporary
/// whose modification time is in the future is skipped too: its age cannot be
/// subtracted, and a clock ahead of this one is not evidence of a dead writer.
fn sweep_stale_temporaries(path: &Path) {
    let Some(file_name) = path.file_name().and_then(|name| name.to_str()) else {
        return;
    };
    let Some(parent) = path.parent() else {
        return;
    };
    let prefix = format!("{file_name}{TEMPORARY_MARKER}");
    let Ok(entries) = std::fs::read_dir(parent) else {
        return;
    };
    let now = std::time::SystemTime::now();
    for entry in entries.flatten() {
        let name = entry.file_name();
        let Some(name) = name.to_str() else {
            continue;
        };
        let Some(tail) = name.strip_prefix(&prefix) else {
            continue;
        };
        if !is_temporary_suffix(tail) {
            continue;
        }
        let Ok(modified) = entry.metadata().and_then(|meta| meta.modified()) else {
            continue;
        };
        let Ok(age) = now.duration_since(modified) else {
            continue;
        };
        if age < STALE_TEMPORARY_AGE {
            continue;
        }
        drop(std::fs::remove_file(entry.path()));
    }
}

/// Whether the part of a name after [`TEMPORARY_MARKER`] is the `<pid>-<sequence>`
/// pair [`temporary_path`] appends.
///
/// The marker says the name is one this helper writes, so this check catches the
/// case the marker cannot: a marked name somebody composed by hand, such as
/// `state.json.mediapm-tmp-backup`. The age rule alone would delete that file once
/// it was an hour old. The check cannot separate a hand-planted
/// `state.json.mediapm-tmp-2024-01` from a process id and a counter, because that
/// name has the shape too; the marker plus the age threshold decide that one.
fn is_temporary_suffix(tail: &str) -> bool {
    let mut parts = tail.split('-');
    let (Some(process_id), Some(sequence), None) = (parts.next(), parts.next(), parts.next())
    else {
        return false;
    };
    let digits = |part: &str| !part.is_empty() && part.bytes().all(|byte| byte.is_ascii_digit());
    digits(process_id) && digits(sequence)
}

/// Renames `temporary` over `path`, clearing a read-only target on Windows.
///
/// Windows refuses the rename when the target carries the read-only attribute,
/// so the failure a protected document produces is retried once the attribute is
/// cleared. POSIX replaces the target with the directory's write permission
/// alone; a permission error there is reported unchanged, because the retry
/// clears nothing and fails the same way.
///
/// A retry that clears the attribute and then fails leaves the target writable.
/// The alternative is to clear before the first attempt, which would widen the
/// mode of every protected document on a save that never lands.
///
/// No gate exercises the retry. It is unreachable on POSIX, where the rename needs
/// only the directory's write permission, and CI's Windows job runs its tests over
/// `mediapm-tests` and `test-runner` alone. That job compiles this body through the
/// packages that depend on `mediapm-utils`, and runs no clippy, so the Windows
/// branch is compiled by CI and checked by a developer running `cargo clippy-all`
/// on Windows, and nothing else touches it.
fn replace(temporary: &Path, path: &Path) -> io::Result<()> {
    match std::fs::rename(temporary, path) {
        Ok(()) => Ok(()),
        Err(error) if error.kind() == io::ErrorKind::PermissionDenied => {
            clear_read_only_attribute(path)?;
            std::fs::rename(temporary, path)
        }
        Err(error) => Err(error),
    }
}

/// Clears the read-only attribute so `path` can be replaced.
///
/// Windows refuses to replace a read-only file, so the attribute has to go. On
/// POSIX the containing directory's write permission is what governs a replace,
/// and the target's own mode only decides what can be written through it, so the
/// body is empty there. Clearing bits on POSIX would rewrite a mode this helper
/// has no reason to touch.
///
/// A missing target is success: the caller is about to create it.
#[cfg_attr(
    not(windows),
    expect(
        clippy::unnecessary_wraps,
        reason = "the fallible signature is shared with the Windows body, where clearing the attribute can fail"
    )
)]
#[cfg_attr(
    windows,
    expect(
        clippy::permissions_set_readonly_false,
        reason = "Windows refuses to replace a read-only file, so the attribute is cleared before the retry renames over it; on POSIX the directory's write permission governs the replace, and clearing bits there would widen the mode rather than restore it"
    )
)]
fn clear_read_only_attribute(path: &Path) -> io::Result<()> {
    #[cfg(windows)]
    {
        let metadata = match std::fs::metadata(path) {
            Ok(metadata) => metadata,
            Err(error) if error.kind() == io::ErrorKind::NotFound => return Ok(()),
            Err(error) => return Err(error),
        };
        if !metadata.permissions().readonly() {
            return Ok(());
        }
        let mut permissions = metadata.permissions();
        permissions.set_readonly(false);
        std::fs::set_permissions(path, permissions)
    }

    #[cfg(not(windows))]
    {
        let _ = path;
        Ok(())
    }
}

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

    /// A file named as this helper's temporary for `path`, sitting `age` in the
    /// past.
    ///
    /// The name is built the way [`temporary_path`] builds one, so a test can
    /// plant the leftover a killed writer leaves without killing anything. The
    /// age comes from `File::set_modified` rather than from sleeping, because
    /// [`STALE_TEMPORARY_AGE`] is an hour.
    fn temporary_of_age(
        path: &Path,
        process_id: u32,
        sequence: u64,
        age: std::time::Duration,
    ) -> PathBuf {
        let file_name = path.file_name().expect("the document has a name").to_string_lossy();
        let leftover =
            path.with_file_name(format!("{file_name}{TEMPORARY_MARKER}{process_id}-{sequence}"));
        std::fs::write(&leftover, b"half a document").expect("plant the leftover");
        let handle =
            std::fs::OpenOptions::new().write(true).open(&leftover).expect("open the leftover");
        handle.set_modified(std::time::SystemTime::now() - age).expect("age the leftover");
        leftover
    }

    /// A save removes the temporary a dead writer left beside its own target,
    /// and leaves every other name alone.
    ///
    /// The leftover is the one file a killed publish costs that a rename cannot
    /// clean up itself, so without this a directory that had a crash collects one
    /// stale name per save. The second assertion is what keeps the sweep from
    /// becoming a directory-wide delete: only names built for this target are
    /// candidates.
    #[test]
    fn a_stale_temporary_beside_the_target_is_removed() {
        let (root, path) = document_with(b"previous");
        let two_hours = std::time::Duration::from_hours(2);
        let leftover = temporary_of_age(&path, 999_999, 0, two_hours);
        let stranger = temporary_of_age(&root.path().join("other.json"), 999_999, 0, two_hours);

        write_bytes_atomically(&path, b"fresh").expect("publish the document");

        assert!(!leftover.exists(), "the stale temporary of this target must be gone");
        assert!(
            stranger.exists(),
            "the sweep removed a temporary that belongs to another document",
        );
        assert_eq!(std::fs::read(&path).expect("read the document"), b"fresh");
        drop(root);
    }

    /// A file whose name only resembles a temporary survives a save.
    ///
    /// The sweep exists to clean up after this helper, and a name it did not
    /// write belongs to someone else. `state.json.tmp-2024-01` is what a
    /// person's editor or archive makes beside a document, and it is also the
    /// shape a `<pid>-<sequence>` suffix takes, so nothing in the older naming
    /// told the two apart.
    #[test]
    fn a_name_that_resembles_a_temporary_is_not_swept() {
        let (root, path) = document_with(b"previous");
        let foreign = root.path().join("state.json.tmp-2024-01");
        std::fs::write(&foreign, b"somebody else's file").expect("plant the foreign file");
        let handle =
            std::fs::OpenOptions::new().write(true).open(&foreign).expect("open the foreign file");
        handle
            .set_modified(std::time::SystemTime::now() - std::time::Duration::from_hours(2))
            .expect("age the foreign file");

        write_bytes_atomically(&path, b"fresh").expect("publish the document");

        assert!(foreign.exists(), "the sweep removed a file this helper never wrote");
        assert_eq!(std::fs::read(&path).expect("read the document"), b"fresh");
        drop(root);
    }

    /// A save leaves a temporary younger than the threshold alone.
    ///
    /// That temporary can only belong to a save that is running, and removing it
    /// would make that save's rename fail. The test publishes into a document
    /// whose fresh temporary is present, which is exactly the state two processes
    /// saving one path are in.
    #[test]
    fn a_live_temporary_beside_the_target_is_kept() {
        let (root, path) = document_with(b"previous");
        let live = temporary_of_age(&path, 999_999, 0, std::time::Duration::from_secs(0));

        write_bytes_atomically(&path, b"fresh").expect("publish the document");

        assert!(live.exists(), "a temporary a live save may still be writing must survive");
        assert_eq!(std::fs::read(&path).expect("read the document"), b"fresh");
        drop(root);
    }

    /// A document a user made read-only is still replaceable, and comes back
    /// read-only.
    ///
    /// Windows refuses to rename over a read-only file, so without clearing the
    /// attribute first a protected document refuses every later save while a
    /// reader can still open it. The mode has to survive both halves: the copy
    /// puts it on the temporary, and the replace has to leave it in place.
    #[test]
    fn a_publish_replaces_a_read_only_target() {
        let (root, path) = document_with(b"previous");
        let mut permissions = std::fs::metadata(&path).expect("read the document").permissions();
        permissions.set_readonly(true);
        std::fs::set_permissions(&path, permissions).expect("protect the document");

        write_bytes_atomically(&path, b"fresh").expect("a protected document must be replaceable");

        assert_eq!(std::fs::read(&path).expect("read the document"), b"fresh");
        assert!(
            std::fs::metadata(&path).expect("read the document").permissions().readonly(),
            "the save widened a document the user protected",
        );
        drop(root);
    }

    /// The read-only clear does nothing on POSIX.
    ///
    /// POSIX replaces a target with the containing directory's write permission,
    /// so the clear exists for Windows alone. A clear here would rewrite the mode
    /// of a file this helper only ever replaces, and `set_readonly(false)` on
    /// POSIX grants write permission to every account instead of restoring the
    /// owner's.
    #[cfg(unix)]
    #[test]
    fn clearing_a_read_only_target_leaves_a_posix_mode_alone() {
        use std::os::unix::fs::PermissionsExt;

        let (root, path) = document_with(b"previous");
        std::fs::set_permissions(&path, std::fs::Permissions::from_mode(0o400))
            .expect("restrict the document");

        clear_read_only_attribute(&path).expect("clearing is a no-op on POSIX");

        let mode =
            std::fs::metadata(&path).expect("read the document").permissions().mode() & 0o777;
        assert_eq!(mode, 0o400, "the POSIX path rewrote the document's mode");
        drop(root);
    }

    /// The suffix check accepts exactly a process id and a counter.
    ///
    /// The marker decides ownership, so the check is narrow on purpose: a marked
    /// name whose tail is something else is refused. Each case below is a shape a
    /// looser split would accept and the sweep would then treat as its own.
    #[test]
    fn only_a_process_id_and_a_counter_are_a_temporary_suffix() {
        assert!(is_temporary_suffix("4242-7"));
        assert!(!is_temporary_suffix("a-1"), "a non-numeric process id was accepted");
        assert!(!is_temporary_suffix("1-2-3"), "a three-part suffix was accepted");
        assert!(!is_temporary_suffix("1"), "a suffix with no counter was accepted");
        assert!(!is_temporary_suffix("-"), "an empty pair was accepted");
        assert!(!is_temporary_suffix(""), "an empty suffix was accepted");
    }

    /// The temporary's spelling is a persisted fact, so a test pins it.
    ///
    /// [`temporary_path`] and [`sweep_stale_temporaries`] both read
    /// [`TEMPORARY_MARKER`], so an edit keeps them consistent with each other and
    /// still strands every leftover already on disk: the sweep stops matching the
    /// names the previous version wrote, and nothing else reclaims them. Writing
    /// the literal out here makes such an edit deliberate, and makes the commit
    /// that changes it say what happens to the leftovers it abandons.
    #[test]
    fn the_temporary_name_carries_the_literal_marker() {
        assert_eq!(TEMPORARY_MARKER, ".mediapm-tmp-", "the marker spelling is persisted state");

        let (root, path) = document_with(b"previous");
        let temporary = temporary_path(&path).expect("the document has a sibling name");
        let name =
            temporary.file_name().expect("the temporary has a name").to_string_lossy().into_owned();
        let prefix = format!("state.json.mediapm-tmp-{}-", std::process::id());
        assert!(
            name.starts_with(&prefix),
            "the temporary is named {name}, which does not start with {prefix}",
        );
        let sequence = &name[prefix.len()..];
        assert!(
            !sequence.is_empty() && sequence.bytes().all(|byte| byte.is_ascii_digit()),
            "the temporary carries {sequence} where a counter belongs",
        );
        drop(root);
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
