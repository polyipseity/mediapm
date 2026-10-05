//! What a remembered verdict saves a run, and what it does not.
//!
//! The witness throughout is a read count rather than a timestamp. The
//! materializer's first method is a hardlink, so a rewrite relinks the same
//! inode with the same bytes and the same modification time, and a modification
//! time cannot tell one from a no-op. Counting the reads
//! [`file_content_matches_hash`](super::file_ops::file_content_matches_hash)
//! performed is the only measurement here that separates "the run declined the
//! entry" from "the run declined the entry after reading it anyway".
//!
//! The fixture is a media folder whose members are extracted from an archive.
//! They are written from bytes the run resolved rather than from a store
//! object, so they hold no relationship to any object and always land on the
//! content branch, which is the only branch a verdict can serve. A media entry
//! written through the materializer's default method is a hardlink and never
//! reaches it, which is the point the last test here makes.

use super::file_ops::read_watch::ReadCountGuard;
use super::tests_common::{
    FOLDER_PATH, open_hierarchy_cas, resolvable_media_document, run_sync, write_folder_over_archive,
};
use super::verdicts::CacheFile;
use super::*;

/// Hierarchy-relative path of the single media entry the media-arm tests
/// re-sync, which is the key both `state.managed_files` and the verdict cache
/// record it under.
const MEDIA_RELATIVE_PATH: &str = "song";

/// Two members the folder fixture's archive holds, kept at two so a run that
/// consults the cache for one of them has the other to get right or wrong, and
/// a read count is a count of members rather than a single number that reads
/// the same either way.
const ARCHIVE_MEMBERS: [(&str, &[u8]); 2] =
    [("one.txt", b"the first member's bytes"), ("two.txt", b"the second member's bytes")];

/// Content the media-arm tests resolve, whose length
/// [`SAME_LENGTH_REPLACEMENT`] matches exactly.
///
/// Same length on purpose. A replacement holding a different number of bytes is
/// caught by the length comparison that runs before a verdict is consulted, so
/// the test would pass against a build whose verdicts were consulted nowhere
/// and never noticed an edit at all.
const ORIGINAL: &[u8] = b"same-number-of-bytes";

/// Different bytes and the same number of them, which is the case a length
/// comparison alone cannot see and a modification time is what has to catch.
const SAME_LENGTH_REPLACEMENT: &[u8] = b"SAME-NUMBER-OF-BYTES";

/// Hierarchy-relative path of the first archive member, which is the key its
/// verdict is stored under.
fn first_member_relative() -> String {
    format!("{FOLDER_PATH}/{}", ARCHIVE_MEMBERS[0].0)
}

/// The files [`ARCHIVE_MEMBERS`] became inside `folder`.
fn members_of(folder: &Path) -> Vec<PathBuf> {
    ARCHIVE_MEMBERS.iter().map(|(name, _)| folder.join(name)).collect()
}

/// Reads the verdict cache a run left at `paths`.
fn read_cache(paths: &MediaPmPaths) -> CacheFile {
    let file = verdicts::cache_file_path(paths);
    serde_json::from_slice(&std::fs::read(&file).expect("the run wrote the verdict cache"))
        .expect("a cache this build wrote is a cache this build can read")
}

/// Replaces the hash one verdict names, leaving its length and its
/// modification time alone.
///
/// The result is what a corrupt or a stale cache holds: a stamp whose length
/// and timestamp still match the file while the content it claims does not.
/// A run that trusts it skips an output whose bytes are wrong, which is the
/// failure the hash in the stamp is there to stop.
fn rewrite_verdict_hash(paths: &MediaPmPaths, relative_path: &str, hash: &str) {
    let file = verdicts::cache_file_path(paths);
    let mut parsed = read_cache(paths);
    parsed.verdicts.get_mut(relative_path).expect("the run confirmed this path").hash =
        hash.to_string();
    std::fs::write(&file, serde_json::to_vec_pretty(&parsed).unwrap())
        .expect("the tampered cache is writable");
}

/// Writes `bytes` over the verdict cache, standing in for a file a person
/// truncated or a write that stopped part way.
fn corrupt_cache(paths: &MediaPmPaths, bytes: &[u8]) {
    let file = verdicts::cache_file_path(paths);
    std::fs::create_dir_all(file.parent().expect("the cache path names a directory"))
        .expect("the cache directory is creatable");
    std::fs::write(&file, bytes).expect("the corrupt cache is writable");
}

/// A second run over a folder nothing changed reads none of its members.
///
/// This is the case the whole cache exists for. A reflink and a copy leave an
/// output that is not its object, so the only way to learn that nothing changed
/// is to read it, and reading a library end to end on every run to answer a
/// question whose answer has not moved is the cost being removed. The counters
/// alone would not show it: a run that read both members and then skipped the
/// folder reports exactly the same report as one that read neither.
///
/// The folder is already on disk and both members are already correct, so the
/// only work the run had left was proving it, and the count says it proved it
/// with a stat.
/// [`a_deleted_verdict_cache_costs_a_reread_and_nothing_else`] is the same run
/// with the cache removed, and it is what shows this count is the cache talking
/// rather than the run happening to be cheap.
#[tokio::test]
async fn a_second_run_over_an_unchanged_folder_reads_no_bytes() {
    let mut entry = write_folder_over_archive(&ARCHIVE_MEMBERS).await;
    let paths = entry.paths().clone();
    let members = members_of(&entry.output);
    assert_eq!(
        read_cache(&paths).verdicts.len(),
        ARCHIVE_MEMBERS.len(),
        "the first run wrote each member from resolved bytes, so it has to have stamped both, \
         or the second run has nothing to consult"
    );

    let guard = ReadCountGuard::new(members.clone());
    let second = entry.resync(None, None).await;

    assert_eq!(
        (second.materialized_paths, second.skipped_paths, second.missing_paths),
        (0, 1, 0),
        "the document resolves the same archive and both members are on disk holding it, so the \
         folder has to be declined: {second:?}"
    );
    assert_eq!(
        guard.total_hashed(),
        0,
        "the run left the folder alone, and a member it hashed in order to leave it alone is a \
         member it read: {members:?}"
    );
    assert_eq!(second.removed_paths, 0, "nothing changed, so nothing was removed: {second:?}");
}

/// Deleting the verdict cache costs a re-read and nothing else.
///
/// The entry is still correct and is still skipped; only the reads come back.
/// That is the whole cost of the cache being absent, and it is why the file
/// lives under the workspace cache directory rather than in `state.json`: a
/// person can delete it at any time and nothing about the library changes
/// except how long the next run takes.
///
/// This is also the control for
/// [`a_second_run_over_an_unchanged_folder_reads_no_bytes`]. The two differ by
/// one deleted file, and the read count differs by exactly the number of
/// members, so the zero there is this cache and not the fixture.
#[tokio::test]
async fn a_deleted_verdict_cache_costs_a_reread_and_nothing_else() {
    let mut entry = write_folder_over_archive(&ARCHIVE_MEMBERS).await;
    let paths = entry.paths().clone();
    let members = members_of(&entry.output);
    let cache = verdicts::cache_file_path(&paths);
    assert!(cache.is_file(), "the first run has to have written the cache this test deletes");
    std::fs::remove_file(&cache).expect("a cache file a run wrote is removable");

    let guard = ReadCountGuard::new(members.clone());
    let second = entry.resync(None, None).await;

    assert_eq!(
        (second.materialized_paths, second.skipped_paths, second.missing_paths),
        (0, 1, 0),
        "the folder is still correct, so a missing cache must not turn a correct entry into a \
         write: {second:?}"
    );
    assert_eq!(
        guard.total_hashed(),
        ARCHIVE_MEMBERS.len() as u64,
        "with no verdicts to consult, each member is hashed again, which is the re-read the \
         cache was saving"
    );
    assert!(
        verdicts::cache_file_path(&paths).is_file(),
        "the run writes the verdicts it just established, so the run after this one is the cheap \
         one rather than every run from here on"
    );
}

/// Replacing an output with different content moves the stamp, so the next run
/// reads it and writes it again.
///
/// The stamp carries four numbers and this is the case where the fourth is the
/// one that has to change. The replacement holds as many bytes as the content
/// the document resolves, so the length still agrees, and the hash the stamp
/// names is still the hash the document resolves. What moved is the
/// modification time, and without it in the stamp the run would take the
/// replaced file for the file it wrote and leave a stranger's bytes under the
/// document's name.
///
/// The write goes through a fresh inode, because an output mediapm marked
/// read-only cannot be opened for writing, which is what a real edit has to do
/// as well. The `assert_ne` is that precondition stated rather than assumed: a
/// filesystem whose timestamps cannot tell two writes apart would make the
/// assertions below meaningless, and saying so beats a run that passes for the
/// wrong reason.
#[tokio::test]
async fn replacing_an_output_with_different_content_moves_the_stamp() {
    let root = mediapm_utils::temp::artifact_dir().unwrap();
    let paths = MediaPmPaths::from_root(root.path());
    let cas = open_hierarchy_cas(&paths).await;
    let hash = cas.put(bytes::Bytes::from_static(ORIGINAL)).await.unwrap();
    let document = resolvable_media_document(&hash.to_string());
    let mut state = MediaPmState::default();

    let first = run_sync(&paths, &document, &mut state, &cas, None, None).await;
    assert_eq!(first.materialized_paths, 1, "the first run has nothing recorded: {first:?}");

    let target = paths.hierarchy_root_dir.join(MEDIA_RELATIVE_PATH);
    let before = verdicts::mtime_nanos(&std::fs::metadata(&target).unwrap());
    commit::remove_path(&target).expect("a readonly managed output is removable");
    std::fs::write(&target, SAME_LENGTH_REPLACEMENT).expect("the replacement lands on the library");
    let after = verdicts::mtime_nanos(&std::fs::metadata(&target).unwrap());
    assert_ne!(
        after, before,
        "the replacement has to have moved the modification time for this test to be about the \
         stamp: {before:?} then {after:?}"
    );

    let guard = ReadCountGuard::new([target.clone()]);
    let second = run_sync(&paths, &document, &mut state, &cas, None, None).await;

    assert_eq!(guard.hashed(&target), 1, "the stamp moved, so the run had to read the file");
    assert_eq!(
        (second.materialized_paths, second.skipped_paths, second.missing_paths),
        (1, 0, 0),
        "the file holds the replacement's bytes under the document's name, so the run has to put \
         the content the document resolves back: {second:?}"
    );
    assert_eq!(
        std::fs::read(&target).unwrap(),
        ORIGINAL,
        "the entry must hold the bytes the document resolved, not the ones the replacement left"
    );
}

/// A stamp naming a hash the file does not have is not trusted.
///
/// The tampering leaves the length and the modification time alone, so a run
/// that consulted only those two would skip a member whose bytes are something
/// else entirely. Reading the file is what finds out. The other member is the
/// control: its stamp was not touched and it is not read, which is what makes
/// this a statement about the tampered entry rather than about the cache as a
/// whole.
#[tokio::test]
async fn a_verdict_naming_another_hash_is_not_trusted() {
    let mut entry = write_folder_over_archive(&ARCHIVE_MEMBERS).await;
    let paths = entry.paths().clone();
    let members = members_of(&entry.output);
    let tampered = first_member_relative();
    let decoy = Hash::from_content(b"content no member of this folder holds");
    rewrite_verdict_hash(&paths, &tampered, &decoy.to_string());

    let guard = ReadCountGuard::new(members.clone());
    let second = entry.resync(None, None).await;

    assert_eq!(
        guard.hashed(&members[0]),
        1,
        "the stamp named a hash this member does not hold, so the run had to read it rather than \
         take the stamp's word: {tampered}"
    );
    assert_eq!(
        guard.hashed(&members[1]),
        0,
        "the second member's stamp was not touched and still describes its file, so consulting \
         one entry's cache cannot have disturbed the other"
    );
    assert_eq!(
        (second.materialized_paths, second.skipped_paths, second.missing_paths),
        (0, 1, 0),
        "reading the tampered member found the bytes the document resolves there after all, so \
         the folder is still correct: {second:?}"
    );
}

/// A stamp for a path whose file is gone is ignored rather than believed.
///
/// The stamp is still in the cache, still names this hash, and still carries
/// the length and the modification time the deleted file had. Every one of
/// those numbers is true of a file that no longer exists, which is why the run
/// stats the file before it consults the stamp rather than the other way
/// round. Believing the stamp here would leave the library short an entry the
/// document asked for and report a clean run over it.
#[tokio::test]
async fn a_verdict_whose_file_is_gone_is_ignored() {
    let root = mediapm_utils::temp::artifact_dir().unwrap();
    let paths = MediaPmPaths::from_root(root.path());
    let cas = open_hierarchy_cas(&paths).await;
    let hash = cas.put(bytes::Bytes::from_static(ORIGINAL)).await.unwrap();
    let document = resolvable_media_document(&hash.to_string());
    let mut state = MediaPmState::default();

    let first = run_sync(&paths, &document, &mut state, &cas, None, None).await;
    assert_eq!(first.materialized_paths, 1, "the first run has nothing recorded: {first:?}");

    let target = paths.hierarchy_root_dir.join(MEDIA_RELATIVE_PATH);
    assert!(
        read_cache(&paths).verdicts.contains_key(MEDIA_RELATIVE_PATH),
        "the first run stamped the path this test is about to empty, or the case never arises"
    );
    commit::remove_path(&target).expect("a readonly managed output is removable");
    assert!(!target.exists(), "the deletion has to have happened, or there is no case here");

    let guard = ReadCountGuard::new([target.clone()]);
    let second = run_sync(&paths, &document, &mut state, &cas, None, None).await;

    assert_eq!(
        guard.hashed(&target),
        0,
        "a file that is not there cannot be read, and a stat is how the run found that out: it \
         did not have to hash anything to learn the stamp no longer described a file"
    );
    assert_eq!(
        (second.materialized_paths, second.skipped_paths, second.missing_paths),
        (1, 0, 0),
        "the record still names this hash and the cache still carries its stamp, so the entry \
         must still be written: {second:?}"
    );
    assert_eq!(
        std::fs::read(&target).unwrap(),
        ORIGINAL,
        "the entry must be back on disk with the bytes its record names"
    );
}

/// An empty or a corrupt verdict cache costs a re-read and fails nothing.
///
/// A cache is a file a person is free to truncate, and an interrupted write
/// leaves the same shape, so neither may end a run. Both answer the empty
/// cache a fresh workspace starts with, which is why the run still skips the
/// folder: the members hold what the document resolves, and only the
/// remembered proof of it is gone.
#[tokio::test]
async fn an_empty_or_corrupt_verdict_cache_is_handled() {
    for (label, bytes) in [("an empty one", b"".as_slice()), ("a truncated one", b"{ \"verdicts\"")]
    {
        let mut entry = write_folder_over_archive(&ARCHIVE_MEMBERS).await;
        let paths = entry.paths().clone();
        let members = members_of(&entry.output);
        corrupt_cache(&paths, bytes);

        let guard = ReadCountGuard::new(members.clone());
        let second = entry.resync(None, None).await;

        assert_eq!(
            (second.materialized_paths, second.skipped_paths, second.missing_paths),
            (0, 1, 0),
            "{label} has to read as no verdicts at all, which is the state a fresh workspace \
             starts in, and the folder is still correct: {second:?}"
        );
        assert_eq!(
            guard.total_hashed(),
            ARCHIVE_MEMBERS.len() as u64,
            "{label} leaves nothing to consult, so each member is hashed again"
        );
        assert_eq!(
            read_cache(&paths).verdicts.len(),
            ARCHIVE_MEMBERS.len(),
            "{label} has to be replaced by a cache holding the verdicts the run just established, \
             or every run from here on pays the re-read"
        );
    }
}

/// A hardlinked output is declined without its stamp being consulted at all.
///
/// The stamp here names a hash the output does not hold, and a build that
/// consulted it before asking what relationship the output has to its object
/// would read the file and cost a hash of the whole library on every run. The
/// read count is the witness: it stays at zero, and the run declines the entry
/// anyway.
///
/// This is the constraint that keeps the cache to outputs that share no inode
/// with their object. A hardlink rewrite relinks the same inode with the same
/// bytes and the same modification time, so a modification time cannot tell one
/// from a no-op, and a stamp recorded for a hardlinked output is decoration
/// whatever it says.
#[cfg(unix)]
#[tokio::test]
async fn a_linked_output_is_skipped_though_its_verdict_names_another_hash() {
    let root = mediapm_utils::temp::artifact_dir().unwrap();
    let paths = MediaPmPaths::from_root(root.path());
    let cas = open_hierarchy_cas(&paths).await;
    let hash = cas.put(bytes::Bytes::from_static(ORIGINAL)).await.unwrap();
    let document = resolvable_media_document(&hash.to_string());
    let mut state = MediaPmState::default();

    let first = run_sync(&paths, &document, &mut state, &cas, None, None).await;
    assert_eq!(first.materialized_paths, 1, "the first run has nothing recorded: {first:?}");

    let target = paths.hierarchy_root_dir.join(MEDIA_RELATIVE_PATH);
    let object_path = cas
        .object_path_for_hash(hash)
        .expect("the first run linked a hardlink, so the object is a file on disk");
    assert!(
        same_file::is_same_file(&object_path, &target)
            .expect("the same_file check after the first run"),
        "the materializer's first method is a hardlink, so the two paths have to be one inode \
         before this test has the case it is about"
    );
    let decoy = Hash::from_content(b"content this output does not hold");
    rewrite_verdict_hash(&paths, MEDIA_RELATIVE_PATH, &decoy.to_string());

    let guard = ReadCountGuard::new([target.clone()]);
    let second = run_sync(&paths, &document, &mut state, &cas, None, None).await;

    assert_eq!(
        guard.hashed(&target),
        0,
        "the output is the inode the object names, so answering that is two stat calls and no \
         bytes, and a stamp saying otherwise cannot make it cost a read"
    );
    assert_eq!(
        (second.materialized_paths, second.skipped_paths, second.missing_paths),
        (0, 1, 0),
        "the output is what the run would have written, so it has to be left alone whatever its \
         stamp says: {second:?}"
    );
}
