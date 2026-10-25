//! What a run's link strategy actually is.
//!
//! Three questions live here, and they are one question asked three ways.
//! The document names the order methods are attempted in, so a run must use
//! that order rather than a built-in one. An attempt that falls through to a
//! later method succeeded by a method the document did not prefer, so the run
//! has to say so rather than pass it off as the preferred method. And a
//! hardlink is a claim about inodes: an output that is not the store object's
//! own inode was not hardlinked, whatever the order said.
//!
//! The fallback case is forced without a second filesystem. A CAS object small
//! enough to live only in the write-ahead log has no blob file until something
//! asks for one, and every link method needs that file. Making the blob tree
//! unwritable therefore fails the link methods deterministically while the copy
//! method still reads the logged bytes. That is a genuine fallback through the
//! production write path, not a mocked one.

use super::tests_common::{
    ZIP_MEMBER_PATH, ZIP_MEMBER_SUFFIX, clear_readonly, open_hierarchy_cas,
    resolvable_media_document, run_sync, zip_member_media_document, zip_payload,
};
use super::*;

/// A blob tree made unwritable for the lifetime of a run.
///
/// Restoring the permissions on drop is what keeps the test's temporary
/// directory removable: a write-protected directory refuses the deletion of
/// its own entries, so a test that froze the tree and panicked before thawing
/// would leak the workspace instead of failing cleanly.
#[cfg(unix)]
struct FrozenBlobTree {
    /// Every directory the freeze touched, so the drop restores each one.
    directories: Vec<PathBuf>,
}

#[cfg(unix)]
impl FrozenBlobTree {
    /// Write-protects `root` and every directory below it.
    ///
    /// Only the write bit is cleared. Read and search stay, because the store
    /// still has to be able to answer what it holds; it is writing a blob that
    /// must fail.
    fn freeze(root: &Path) -> Self {
        let mut directories = Vec::new();
        collect_directories(root, &mut directories);
        for directory in &directories {
            set_mode(directory, 0o555);
        }
        Self { directories }
    }
}

#[cfg(unix)]
impl Drop for FrozenBlobTree {
    fn drop(&mut self) {
        for directory in &self.directories {
            set_mode(directory, 0o755);
        }
    }
}

/// Collects `root` and every directory below it, so a freeze reaches the
/// fan-out directories a blob write would otherwise create.
#[cfg(unix)]
fn collect_directories(root: &Path, into: &mut Vec<PathBuf>) {
    if !root.is_dir() {
        return;
    }
    into.push(root.to_path_buf());
    let Ok(entries) = std::fs::read_dir(root) else {
        return;
    };
    for entry in entries.flatten() {
        collect_directories(&entry.path(), into);
    }
}

/// Sets the permission bits of one directory.
#[cfg(unix)]
fn set_mode(path: &Path, mode: u32) {
    use std::os::unix::fs::PermissionsExt as _;

    std::fs::set_permissions(path, std::fs::Permissions::from_mode(mode))
        .expect("the test can set the directory's mode");
}

/// Runs one `sync_hierarchy` over a document whose order is `order`.
///
/// The document is the same single-media fixture every case here uses, so only
/// the order differs between two runs and a difference in the output is a
/// difference the order made.
async fn sync_with_order(
    paths: &MediaPmPaths,
    cas: &FileSystemCas,
    hash: &mediapm_cas::Hash,
    order: &[MaterializationMethod],
) -> MaterializeReport {
    let mut document = resolvable_media_document(&hash.to_string());
    document.runtime.materialization.materialization_preference_order = Some(order.to_vec());
    let mut state = MediaPmState::default();
    run_sync(paths, &document, &mut state, cas, None, None).await
}

/// The path the single-media fixture's only entry writes.
fn single_entry_target(paths: &MediaPmPaths) -> PathBuf {
    paths.hierarchy_root_dir.join("song")
}

/// Runs one `sync_hierarchy` over the same fixture with the knob left unset.
///
/// An absent order is the shape a document has before it names one, and it is
/// the shape a library gets by default, so what the default order does is
/// worth its own run rather than being inferred from an explicit list that
/// happens to spell the same four methods.
async fn sync_with_default_order(
    paths: &MediaPmPaths,
    cas: &FileSystemCas,
    hash: &mediapm_cas::Hash,
) -> MaterializeReport {
    let mut document = resolvable_media_document(&hash.to_string());
    document.runtime.materialization.materialization_preference_order = None;
    let mut state = MediaPmState::default();
    run_sync(paths, &document, &mut state, cas, None, None).await
}

/// The document's order decides which method writes the output.
///
/// The two runs differ only in the order they declare, so a shared inode under
/// `["hardlink", "copy"]` and a separate one under `["copy", "hardlink"]` is
/// the order at work. A literal order would produce the same relationship for
/// both, which is exactly what this case refuses.
#[tokio::test]
async fn strict_the_configured_order_is_the_order_attempted() {
    for (order, expects_hardlink) in [
        ([MaterializationMethod::Copy, MaterializationMethod::Hardlink], false),
        ([MaterializationMethod::Hardlink, MaterializationMethod::Copy], true),
    ] {
        let workspace = mediapm_utils::temp::artifact_dir().expect("an artifact dir");
        let paths = MediaPmPaths::from_root(workspace.path());
        let cas = open_hierarchy_cas(&paths).await;
        let hash = cas
            .put(bytes::Bytes::from_static(b"configured-order payload"))
            .await
            .expect("the payload is stored");

        let report = sync_with_order(&paths, &cas, &hash, &order).await;
        assert_eq!(
            (report.materialized_paths, report.missing_paths),
            (1, 0),
            "the entry has a resolvable hash, so the run has to write it: {report:?}"
        );

        let object = {
            // The copy method never needs the object's own file, so a run that
            // copied leaves the store holding the bytes in its log. Asking for
            // the file is what makes the inode comparison below possible, and
            // it changes nothing about the output this run wrote.
            cas.ensure_blob_materialized(hash).await.expect("the store can produce its own object");
            cas.object_path_for_hash(hash).expect("the store's path for the object")
        };
        let target = single_entry_target(&paths);
        let shares_inode = same_file::is_same_file(&object, &target).expect("the same_file check");
        assert_eq!(
            shares_inode,
            expects_hardlink,
            "order {order:?} must write {}, not {}",
            if expects_hardlink { "a hardlink" } else { "a copy" },
            if expects_hardlink { "a copy" } else { "a hardlink" }
        );

        #[cfg(unix)]
        if !expects_hardlink {
            use std::os::unix::fs::MetadataExt as _;

            let links = std::fs::metadata(&target).expect("the target's metadata").nlink();
            assert_eq!(
                links, 1,
                "a copied output holds its own bytes, so nothing else may count it as a link"
            );
        }
    }
}

/// A successful preferred method says nothing, because nothing happened worth
/// reporting.
///
/// `["copy"]` is the order that cannot fall through: it is the method that
/// always works, so a notice here would mean the run invented one.
#[tokio::test]
async fn no_notice_when_the_preferred_method_succeeds() {
    let workspace = mediapm_utils::temp::artifact_dir().expect("an artifact dir");
    let paths = MediaPmPaths::from_root(workspace.path());
    let cas = open_hierarchy_cas(&paths).await;
    let hash = cas
        .put(bytes::Bytes::from_static(b"preferred-method payload"))
        .await
        .expect("the payload is stored");

    let report = sync_with_order(&paths, &cas, &hash, &[MaterializationMethod::Copy]).await;

    assert_eq!(report.materialized_paths, 1, "the run has to write the entry: {report:?}");
    assert!(
        report.notices.is_empty(),
        "the first configured method wrote the output, so there is no fallback to report: {:?}",
        report.notices
    );
}

/// A fallback reaches the report with the path it applies to and the method
/// that actually wrote the file.
///
/// The link methods need a blob file, and the blob tree is unwritable, so
/// `ensure_blob_materialized` fails for the hardlink and the copy method —
/// which streams the logged bytes — writes the output. The notice therefore
/// has to name both the entry and `copy`, because a notice that named only one
/// of them would leave a reader unable to tell what happened to what.
#[cfg(unix)]
#[tokio::test]
async fn a_fallback_is_reported_with_the_path_and_the_method() {
    let workspace = mediapm_utils::temp::artifact_dir().expect("an artifact dir");
    let paths = MediaPmPaths::from_root(workspace.path());
    let cas = open_hierarchy_cas(&paths).await;
    let payload = b"fallback payload";
    let hash = cas.put(bytes::Bytes::from_static(payload)).await.expect("the payload is stored");
    assert!(
        !cas.object_path_for_hash(hash).is_some_and(|path| path.is_file()),
        "a small put stays in the log until something needs a blob file, and this case needs \
         that to be true before it freezes the blob tree"
    );

    let report = {
        let blob_root = paths.runtime_root.join("store").join("blobs");
        std::fs::create_dir_all(&blob_root).expect("the blob root exists before it is frozen");
        let frozen = FrozenBlobTree::freeze(&blob_root);
        // A freeze the run could write through would turn this case into a
        // second copy of the preferred-method case, so the freeze proves
        // itself before the run leans on it.
        assert!(
            std::fs::write(blob_root.join("freeze-probe"), b"probe").is_err(),
            "the blob tree must refuse a write, or the fallback this case is about cannot happen"
        );
        let report = sync_with_order(
            &paths,
            &cas,
            &hash,
            &[MaterializationMethod::Hardlink, MaterializationMethod::Copy],
        )
        .await;
        drop(frozen);
        report
    };

    assert_eq!(
        (report.materialized_paths, report.missing_paths),
        (1, 0),
        "the copy method can still read the logged bytes, so the run must complete: {report:?}"
    );
    assert_eq!(
        report.notices.len(),
        1,
        "one entry fell back once, so the report carries exactly one notice: {:?}",
        report.notices
    );
    let notice = &report.notices[0];
    assert!(notice.contains("'song'"), "the notice has to name the entry it applies to: {notice}");
    assert!(
        notice.contains("'copy'"),
        "the notice has to name the method that actually wrote the file: {notice}"
    );
    assert!(
        notice.contains("hardlink"),
        "the notice has to name the method that did not apply, since that is the reason this one \
         ran: {notice}"
    );
    assert_eq!(
        std::fs::read(single_entry_target(&paths)).expect("the output exists"),
        payload,
        "the fallback wrote the entry's bytes, so the work itself still stands"
    );
}

/// A hardlinked output is the store object's own inode.
///
/// This is the claim the default order makes, and the claim a read-only output
/// rests on: the bytes on disk are the store's bytes, not a copy of them. Link
/// count is the second half, because a file that shares an inode with the
/// object has at least two names, one of which is the object's. The run leaves
/// the order unset, so the method that answers is the default order's first
/// one, and it asserts no notice: a notice here would mean the default order
/// fell through on the layout every library starts with.
#[tokio::test]
async fn strict_a_materialized_hardlink_shares_the_cas_inode() {
    let workspace = mediapm_utils::temp::artifact_dir().expect("an artifact dir");
    let paths = MediaPmPaths::from_root(workspace.path());
    let cas = open_hierarchy_cas(&paths).await;
    let hash = cas
        .put(bytes::Bytes::from_static(b"hardlink identity payload"))
        .await
        .expect("the payload is stored");

    let report = sync_with_default_order(&paths, &cas, &hash).await;
    assert_eq!(report.materialized_paths, 1, "the run has to write the entry: {report:?}");
    assert!(
        report.notices.is_empty(),
        "the default order hardlinks into a library beside its store, so nothing fell back: {:?}",
        report.notices
    );

    let object = cas.object_path_for_hash(hash).expect("the store's own path for the object");
    let target = single_entry_target(&paths);
    assert!(
        same_file::is_same_file(&object, &target).expect("the same_file check"),
        "a hardlinked output is the store object under another name, so the two paths must name \
         one file"
    );

    #[cfg(unix)]
    {
        use std::os::unix::fs::MetadataExt as _;

        let links = std::fs::metadata(&target).expect("the target's metadata").nlink();
        assert!(
            links >= 2,
            "a hardlinked output adds a name to the object's inode, so the count is at least two, \
             and {links} says the output was not hardlinked"
        );
    }
}

/// A zip-member entry replaces its target with the member, and records the
/// member's hash rather than the archive's.
///
/// The archive and the member have different hashes, which is what makes the
/// record worth asserting: a record naming the archive would tell the next run
/// the target must hold the whole pack, and the target holds one extracted
/// file. The rewrite half is the read-only path: an output the library wrote is
/// removable and replaceable, so a run that finds it changed in place rewrites
/// it from the member rather than leaving the edit standing.
#[tokio::test]
async fn strict_a_zip_member_entry_replaces_its_target() {
    let workspace = mediapm_utils::temp::artifact_dir().expect("an artifact dir");
    let paths = MediaPmPaths::from_root(workspace.path());
    let cas = open_hierarchy_cas(&paths).await;
    let member = b"WEBVTT\n\n00:00.000 --> 00:01.000\nmember cue\n";
    let pack = zip_payload(&[(ZIP_MEMBER_SUFFIX, member), ("other.txt", b"unrelated member")]);
    let pack_hash = cas.put(bytes::Bytes::from(pack)).await.expect("the pack is stored");
    let input_hash = cas
        .put(bytes::Bytes::from_static(b"consumed input"))
        .await
        .expect("the step's declared input is stored");
    let document = zip_member_media_document(&input_hash.to_string(), &pack_hash.to_string());
    let mut state = MediaPmState::default();

    let first = run_sync(&paths, &document, &mut state, &cas, None, None).await;
    assert_eq!(
        (first.materialized_paths, first.missing_paths),
        (1, 0),
        "the member is inside the pack, so the run has to extract and write it: {first:?}"
    );

    let target = paths.hierarchy_root_dir.join(ZIP_MEMBER_PATH);
    assert_eq!(
        std::fs::read(&target).expect("the extracted member is on disk"),
        member,
        "the entry writes the member's bytes, not the archive's"
    );
    let expected_member_hash = mediapm_cas::Hash::from_content(member).to_string();
    assert_ne!(
        expected_member_hash,
        pack_hash.to_string(),
        "the fixture is only meaningful while the member and the pack differ, or the record \
         assertion below could not tell them apart"
    );
    let record = state
        .managed_files
        .get(ZIP_MEMBER_PATH)
        .expect("the run records what it wrote under the entry's path");
    assert_eq!(
        record.hash, expected_member_hash,
        "the record names the member the target holds, because that is what the next run must \
         find there"
    );

    clear_readonly(&target);
    std::fs::write(&target, b"an edit something outside mediapm made").expect("the edit lands");

    let second = run_sync(&paths, &document, &mut state, &cas, None, None).await;
    assert_eq!(
        (second.materialized_paths, second.missing_paths),
        (1, 0),
        "the target no longer holds the member, so the run has to replace it: {second:?}"
    );
    assert_eq!(
        std::fs::read(&target).expect("the rewritten member is on disk"),
        member,
        "the rewrite restores the member, not a copy of the edited file"
    );
}
