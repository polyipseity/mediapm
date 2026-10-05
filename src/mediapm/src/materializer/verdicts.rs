//! Remembering which managed outputs a run already proved.
//!
//! A managed output that shares no inode with its CAS object can only be checked
//! by reading it. A reflink and a copy are separate inodes by construction, and
//! so is every folder member and playlist body, because those are written from
//! bytes the run resolved rather than from a store object. Reading a library of
//! them to answer a question that has not changed is the cost this cache
//! removes.
//!
//! # What one verdict records
//!
//! The hierarchy-relative path of an output, the hash its bytes were verified
//! against, how many bytes long it was, and its modification time read after
//! the read finished. A later run compares those four numbers against one stat
//! and reuses the verdict when they all agree.
//!
//! Reading the modification time after the read rather than before is what makes
//! the four numbers describe one state. A file replaced while it was being read
//! would otherwise produce a stamp pairing the new modification time with the
//! old bytes, and the next run would trust it.
//!
//! Four matching numbers are not proof of content. They are evidence, and the
//! evidence is a modification time: strong enough to skip a re-read, weak
//! enough that what it cannot see is written down below. The one case it cannot
//! see is a file that was edited and then given its old modification time back.
//!
//!
//! # What invalidates a verdict
//!
//! Any disagreement between those numbers and what the file shows now, plus a
//! file the platform will not stamp at all. A rewrite moves the length and the
//! modification time. An edit that keeps the length moves the modification
//! time. A document that resolves a different hash is compared against a
//! different hash and never matches. A file whose modification time the
//! platform will not report is never stamped, so it is read on every run, which
//! is the behaviour without this cache.
//!
//! One case survives, and it is the price of trusting a modification time: a
//! file rewritten and then handed its old modification time back keeps a verdict
//! that has stopped being true. Moving a timestamp backwards is deliberate work
//! that mediapm does not do, and nothing else in the tree does either.
//!
//! # What a stale cache costs
//!
//! One full read of one output, and nothing else. A cache that is missing,
//! unreadable, empty or written by a version this build does not know is read as
//! no verdicts at all, which is the state a fresh workspace starts in. No run
//! fails over it and nothing in `state.json` depends on it.
//!
//! # Where it lives
//!
//! `<runtime>/cache/mediapm/content_verdicts.json`, beside the workspace
//! metadata cache. It is a cache, so a person is free to delete it.

use std::collections::BTreeMap;
use std::fs::Metadata;
use std::io::Write as _;
use std::path::{Path, PathBuf};
use std::sync::Mutex;
use std::time::UNIX_EPOCH;

use mediapm_cas::Hash;
use serde::{Deserialize, Serialize};
use tracing::warn;

use crate::error::MediaPmError;
use crate::paths::MediaPmPaths;

/// Format version this build reads and writes.
///
/// A file carrying any other version is treated as carrying no verdicts. That
/// costs one read per output, which is what a missing cache costs, and it keeps
/// a future format from being read as the wrong numbers.
const CACHE_VERSION: u32 = 1;

/// One output the materializer verified, and the four numbers that proved it.
///
/// Public only so the tests can build and corrupt one by hand; the production
/// call sites read and write it through [`VerdictCache`].
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub(super) struct Verdict {
    /// Hash the output's bytes were verified against, spelled the way the run
    /// spells it. A verdict naming any other hash describes a different file
    /// and is never matched against this one.
    pub(super) hash: String,
    /// Length the output had when it was verified, which settles the common
    /// case of something having replaced it in one stat.
    pub(super) len: u64,
    /// Modification time in nanoseconds since the unix epoch.
    pub(super) mtime_nanos: u64,
}

/// The cache file as it sits on disk.
///
/// Unknown fields are ignored rather than rejected. This is a cache rather than
/// a boundary type, and a field a future version adds must not turn a whole
/// file into a parse error, which would cost the same re-read a missing cache
/// costs but for no reason.
///
/// Visible to the crate so a test can corrupt one by hand. A cache whose
/// numbers are only ever written by the code that reads them is never tested
/// against a number somebody else put there, which is the only kind of number
/// a corrupt cache actually holds.
#[derive(Debug, Serialize, Deserialize)]
pub(super) struct CacheFile {
    /// Format version, defaulted so a file that names none is read as this
    /// version rather than rejected.
    #[serde(default = "current_cache_version")]
    pub(super) version: u32,
    /// Verdicts by hierarchy-relative path, defaulted so a file carrying only a
    /// version reads as carrying none.
    #[serde(default)]
    pub(super) verdicts: BTreeMap<String, Verdict>,
}

/// The version a file that names none is read as.
fn current_cache_version() -> u32 {
    CACHE_VERSION
}

/// Where the verdicts for this workspace are kept.
pub(super) fn cache_file_path(paths: &MediaPmPaths) -> PathBuf {
    paths.workspace_mediapm_cache_dir().join("content_verdicts.json")
}

/// The file's modification time as whole nanoseconds since the unix epoch.
///
/// `None` when the platform declines to report it, or when the value falls
/// outside the range a `u64` of nanoseconds covers. Both answers mean the same
/// thing: this file cannot be stamped, so its check reads it.
pub(super) fn mtime_nanos(metadata: &Metadata) -> Option<u64> {
    let modified = metadata.modified().ok()?;
    let since_epoch = modified.duration_since(UNIX_EPOCH).ok()?;
    u64::try_from(since_epoch.as_nanos()).ok()
}

/// The verdicts one `sync_hierarchy` run reads and writes.
///
/// Two sets, because they answer different questions. `remembered` comes from
/// the previous run's file and is never written to during this run, so a verdict
/// is only ever reused on the strength of a completed earlier run. `confirmed`
/// collects what this run proved and becomes the file it leaves behind, which is
/// why the file is written once after the materialization loop rather than from
/// the workers: a run that ends part way leaves the previous run's verdicts
/// rather than a partial set.
pub(super) struct VerdictCache {
    /// The file this cache reads at the start of a run and writes at its end.
    file: PathBuf,
    /// Verdicts loaded from `file`, which this run reads and never changes.
    remembered: BTreeMap<String, Verdict>,
    /// Verdicts this run established, written out by [`Self::flush`].
    confirmed: Mutex<BTreeMap<String, Verdict>>,
}

impl VerdictCache {
    /// Load the verdicts this workspace was left with.
    ///
    /// A missing file, an unreadable one, one that is not JSON, and one written
    /// by another format version all produce the same empty cache, with a note
    /// for the two cases a person could act on. The cost is one read per output,
    /// which is what this cache saves when it is intact.
    pub(super) fn open(paths: &MediaPmPaths) -> Self {
        let file = cache_file_path(paths);
        let remembered = Self::read_file(&file);
        Self { file, remembered, confirmed: Mutex::new(BTreeMap::new()) }
    }

    /// Reads `file`, answering an empty map for anything it cannot use.
    fn read_file(file: &Path) -> BTreeMap<String, Verdict> {
        let bytes = match std::fs::read(file) {
            Ok(bytes) => bytes,
            Err(error) if error.kind() == std::io::ErrorKind::NotFound => return BTreeMap::new(),
            Err(error) => {
                warn!("ignoring unreadable content verdict cache '{}': {error}", file.display());
                return BTreeMap::new();
            }
        };
        match serde_json::from_slice::<CacheFile>(&bytes) {
            Ok(parsed) if parsed.version == CACHE_VERSION => parsed.verdicts,
            Ok(parsed) => {
                warn!(
                    "ignoring content verdict cache '{}': it is version {} and this build reads \
                     version {CACHE_VERSION}",
                    file.display(),
                    parsed.version
                );
                BTreeMap::new()
            }
            Err(error) => {
                warn!("ignoring malformed content verdict cache '{}': {error}", file.display());
                BTreeMap::new()
            }
        }
    }

    /// Whether a remembered verdict still describes `relative_path`.
    ///
    /// All four numbers have to agree. The hash says which content, the length
    /// says the file is still that long, and the modification time says the
    /// bytes have not moved since. A file whose modification time cannot be
    /// read answers `false` without consulting the map, because there is no
    /// number to compare.
    pub(super) fn is_current(
        &self,
        relative_path: &str,
        hash: &Hash,
        len: u64,
        mtime_nanos: Option<u64>,
    ) -> bool {
        let Some(mtime_nanos) = mtime_nanos else {
            return false;
        };
        self.remembered.get(relative_path).is_some_and(|verdict| {
            verdict.hash == hash.to_string()
                && verdict.len == len
                && verdict.mtime_nanos == mtime_nanos
        })
    }

    /// Record that an output was just written from `hash`, or just verified to
    /// hold `hash`.
    ///
    /// Both callers know the content without reading it, which is what makes a
    /// stamp here sound rather than a guess: the write path produced the file
    /// from those bytes and the check path compared those bytes.
    ///
    /// A file whose modification time cannot be read is not stamped at all. The
    /// alternative, a stamp that compares equal against anything, would turn a
    /// filesystem that does not report modification times into a library that
    /// is never verified again.
    pub(super) fn confirm(
        &self,
        relative_path: &str,
        hash: &Hash,
        len: u64,
        mtime_nanos: Option<u64>,
    ) {
        let Some(mtime_nanos) = mtime_nanos else {
            return;
        };
        let mut confirmed = match self.confirmed.lock() {
            Ok(confirmed) => confirmed,
            Err(poisoned) => poisoned.into_inner(),
        };
        confirmed.insert(
            relative_path.to_string(),
            Verdict { hash: hash.to_string(), len, mtime_nanos },
        );
    }

    /// Write this run's verdicts, and only this run's.
    ///
    /// Paths this run never confirmed are dropped rather than carried forward, so
    /// the file tracks the library instead of growing for as long as the
    /// workspace lives.
    ///
    /// # Errors
    ///
    /// Returns the I/O failure that stopped the write. The caller reports it and
    /// carries on, because the verdicts are a cache and a run that cannot write
    /// one is still a run that verified the library.
    pub(super) fn flush(&self) -> Result<(), MediaPmError> {
        let confirmed = match self.confirmed.lock() {
            Ok(confirmed) => confirmed.clone(),
            Err(poisoned) => poisoned.into_inner().clone(),
        };
        let directory = self.file.parent().unwrap_or(Path::new("."));
        if let Err(source) = std::fs::create_dir_all(directory) {
            return Err(MediaPmError::Io {
                operation: "creating the content verdict cache directory".to_string(),
                path: directory.to_path_buf(),
                source,
            });
        }
        let bytes =
            serde_json::to_vec_pretty(&CacheFile { version: CACHE_VERSION, verdicts: confirmed })
                .map_err(|error| {
                MediaPmError::Workflow(format!(
                    "encoding the content verdict cache failed: {error}"
                ))
            })?;

        // Write beside the target and rename over it, so a run that is killed
        // mid-write leaves the previous verdicts rather than half of this run's.
        let mut staging =
            tempfile::NamedTempFile::new_in(directory).map_err(|source| MediaPmError::Io {
                operation: "creating the content verdict cache staging file".to_string(),
                path: directory.to_path_buf(),
                source,
            })?;
        staging.write_all(&bytes).map_err(|source| MediaPmError::Io {
            operation: "writing the content verdict cache staging file".to_string(),
            path: directory.to_path_buf(),
            source,
        })?;
        staging.as_file().sync_all().map_err(|source| MediaPmError::Io {
            operation: "flushing the content verdict cache staging file".to_string(),
            path: directory.to_path_buf(),
            source,
        })?;
        staging.persist(&self.file).map_err(|error| MediaPmError::Io {
            operation: "renaming the content verdict cache into place".to_string(),
            path: self.file.clone(),
            source: error.error,
        })?;
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    /// Writes `bytes` where a workspace's verdict cache goes and answers the
    /// cache a run would then open.
    fn cache_over(paths: &MediaPmPaths, bytes: &[u8]) -> VerdictCache {
        let file = cache_file_path(paths);
        std::fs::create_dir_all(file.parent().expect("the cache path names a directory")).unwrap();
        std::fs::write(&file, bytes).expect("the cache file is writable");
        VerdictCache::open(paths)
    }

    /// A workspace nothing has run against, which is the state a fresh one
    /// starts in and the state every unusable cache file falls back to.
    fn empty_workspace() -> (tempfile::TempDir, MediaPmPaths) {
        let root = mediapm_utils::temp::artifact_dir().unwrap();
        let paths = MediaPmPaths::from_root(root.path());
        (root, paths)
    }

    /// A file carrying a version this build does not read is read as carrying
    /// no verdicts rather than as the wrong numbers.
    ///
    /// Reading it as the wrong numbers is the failure this branch exists to
    /// prevent: a future build that adds a field and keeps the same version
    /// would otherwise have its entries matched against a hash a file does not
    /// have, which is a silent skip rather than a re-read.
    #[test]
    fn a_file_from_another_version_is_read_as_no_verdicts() {
        let (_workspace, paths) = empty_workspace();
        let from_the_future = serde_json::to_vec_pretty(&CacheFile {
            version: CACHE_VERSION + 1,
            verdicts: BTreeMap::from([(
                "song".to_string(),
                Verdict { hash: "blake3:nonsense".to_string(), len: 7, mtime_nanos: 7 },
            )]),
        })
        .unwrap();
        let cache = cache_over(&paths, &from_the_future);

        let verdict = cache.remembered.get("song");
        assert!(
            verdict.is_none(),
            "a version this build cannot read has to cost a re-read, not a skip on numbers it \
             never checked: {verdict:?}"
        );
    }

    /// A file that is empty, or that is not JSON at all, costs the re-read a
    /// missing file costs.
    ///
    /// A cache is a file a person is free to truncate, and an interrupted write
    /// is the same shape. Neither may end a run: the answer is an empty cache,
    /// which is the state a workspace starts in.
    #[test]
    fn an_empty_or_malformed_file_is_read_as_no_verdicts() {
        let (_workspace, paths) = empty_workspace();
        for (label, bytes) in [("empty", b"".as_slice()), ("malformed", b"{ \"verdicts\": ")] {
            let cache = cache_over(&paths, bytes);
            assert!(
                cache.remembered.is_empty(),
                "a {label} cache file has to read as no verdicts rather than fail the run: \
                 {:?}",
                cache.remembered
            );
        }
    }

    /// A file naming no version at all is read as this one, because a version
    /// field a hand-written file leaves out is not a reason to throw the
    /// verdicts beside it away.
    #[test]
    fn a_file_naming_no_version_is_read_as_this_one() {
        let (_workspace, paths) = empty_workspace();
        let unversioned = br#"{"verdicts": {"song": {"hash": "h", "len": 7, "mtime_nanos": 7}}}"#;
        let cache = cache_over(&paths, unversioned);

        assert_eq!(
            cache.remembered.get("song").map(|verdict| (verdict.hash.as_str(), verdict.len)),
            Some(("h", 7)),
            "the verdicts beside a missing version field are still verdicts: {:?}",
            cache.remembered
        );
    }
}
