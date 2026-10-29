//! Remembering which managed outputs a run already proved.
//!
//! A managed output that shares no inode with its CAS object can only be checked
//! by reading it. A reflink and a copy are separate inodes by construction, and
//! so is every folder member and playlist body, because those are written from
//! bytes the run resolved rather than from a store object. Reading a library of
//! them to answer a question that has not changed is the cost this cache
//! removes.
//!
//! # What one record holds
//!
//! The hierarchy-relative path of an output, the hash its bytes were verified
//! against, how many bytes long it was, and its modification time read after
//! the read finished. A later run compares those four numbers against one stat
//! and reuses the record when they all agree.
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
//! # What invalidates a record
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
//! file rewritten and then handed its old modification time back keeps a record
//! that has stopped being true. Moving a timestamp backwards is deliberate work
//! that mediapm does not do, and nothing else in the tree does either.
//!
//! # What a stale cache costs
//!
//! One full read of one output, and nothing else. A cache that is missing,
//! unreadable, empty or written by a version this build does not know is read as
//! no records at all, which is the state a fresh workspace starts in. No run
//! fails over it and nothing in `state.json` depends on it.
//!
//! # Where it lives
//!
//! `<runtime>/cache/mediapm/materialization.json`, beside the workspace
//! metadata cache, and named for what it holds: the records of what this
//! workspace's materialization already proved. It is a cache, so a person is
//! free to delete it.
//!
//! The file names its format version and holds its records under `outputs`. A
//! file naming no version, naming one this build does not read, or failing to
//! parse is read as holding nothing, which is the state a fresh workspace
//! starts in; nothing about it is migrated.

use std::collections::BTreeMap;
use std::fs::Metadata;
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
/// A file carrying any other version is treated as carrying no records. That
/// costs one read per output, which is what a missing cache costs, and it keeps
/// numbers another build wrote from being read as these ones.
const CACHE_VERSION: u32 = 2;

/// One output the materializer verified, and the four numbers that proved it.
///
/// Public only so the tests can build and corrupt one by hand; the production
/// call sites read and write it through [`OutputCache`].
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub(super) struct OutputRecord {
    /// Hash the output's bytes were verified against, spelled the way the run
    /// spells it. A record naming any other hash describes a different file
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
    /// Format version the file was written with.
    ///
    /// Required rather than defaulted: a file that names no version carries
    /// nothing this build can read, and a default would adopt it instead.
    pub(super) version: u32,
    /// Records by hierarchy-relative path, defaulted so a file carrying only a
    /// version reads as carrying none.
    #[serde(default)]
    pub(super) outputs: BTreeMap<String, OutputRecord>,
}

/// Name of the file this build reads and writes.
const CACHE_FILE_NAME: &str = "materialization.json";

/// Where the records for this workspace are kept.
pub(super) fn cache_file_path(paths: &MediaPmPaths) -> PathBuf {
    paths.workspace_mediapm_cache_dir().join(CACHE_FILE_NAME)
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

/// The records one `sync_hierarchy` run reads and writes.
///
/// Two sets, because they answer different questions. `remembered` comes from
/// the previous run's file and is never written to during this run, so a record
/// is only ever reused on the strength of a completed earlier run. `confirmed`
/// collects what this run proved and becomes the file it leaves behind, which is
/// why the file is written once after the materialization loop rather than from
/// the workers: a run that ends part way leaves the previous run's records
/// rather than a partial set.
pub(super) struct OutputCache {
    /// The file this cache reads at the start of a run and writes at its end.
    file: PathBuf,
    /// Records loaded from `file`, which this run reads and never changes.
    remembered: BTreeMap<String, OutputRecord>,
    /// Records this run established, written out by [`Self::flush`].
    confirmed: Mutex<BTreeMap<String, OutputRecord>>,
}

impl OutputCache {
    /// Load the records this workspace was left with.
    ///
    /// A missing file, an unreadable one, one that is not JSON, and one written
    /// by another format version all produce the same empty cache, with a note
    /// for the cases a person could act on. A file naming no version at all
    /// produces it too: the version is what says whose numbers these are, so a
    /// file without one describes nothing this build can reuse.
    ///
    /// Opening has no side effect on the disk: it reads, or it does not.
    pub(super) fn open(paths: &MediaPmPaths) -> Self {
        let file = cache_file_path(paths);
        let remembered = Self::read_file(&file);
        Self { file, remembered, confirmed: Mutex::new(BTreeMap::new()) }
    }

    /// Reads `file`, answering an empty map for anything that is absent or
    /// unusable.
    ///
    /// One answer covers every case because the caller has one recovery: read
    /// the outputs again and write the records in the current shape. A warning
    /// separates the cases that leave a file behind unread, so a shape that
    /// keeps arriving stays visible in the logs.
    fn read_file(file: &Path) -> BTreeMap<String, OutputRecord> {
        let bytes = match std::fs::read(file) {
            Ok(bytes) => bytes,
            Err(error) if error.kind() == std::io::ErrorKind::NotFound => {
                return BTreeMap::new();
            }
            Err(error) => {
                warn!("ignoring unreadable materialization cache '{}': {error}", file.display());
                return BTreeMap::new();
            }
        };
        let parsed = match serde_json::from_slice::<CacheFile>(&bytes) {
            Ok(parsed) => parsed,
            Err(error) => {
                warn!("ignoring malformed materialization cache '{}': {error}", file.display());
                return BTreeMap::new();
            }
        };
        if parsed.version != CACHE_VERSION {
            warn!(
                "ignoring materialization cache '{}': it is version {} and this build reads \
                 version {CACHE_VERSION}",
                file.display(),
                parsed.version
            );
            return BTreeMap::new();
        }
        parsed.outputs
    }

    /// Whether a remembered record still describes `relative_path`.
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
        self.remembered.get(relative_path).is_some_and(|record| {
            record.hash == hash.to_string()
                && record.len == len
                && record.mtime_nanos == mtime_nanos
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
            OutputRecord { hash: hash.to_string(), len, mtime_nanos },
        );
    }

    /// Write this run's records, and only this run's.
    ///
    /// Paths this run never confirmed are dropped rather than carried forward, so
    /// the file tracks the library instead of growing for as long as the
    /// workspace lives.
    ///
    /// # Errors
    ///
    /// Returns the I/O failure that stopped the write. The caller reports it and
    /// carries on, because the records are a cache and a run that cannot write
    /// one is still a run that verified the library.
    pub(super) fn flush(&self) -> Result<(), MediaPmError> {
        let confirmed = match self.confirmed.lock() {
            Ok(confirmed) => confirmed.clone(),
            Err(poisoned) => poisoned.into_inner().clone(),
        };
        Self::write_file(&self.file, &confirmed)
    }

    /// Writes `records` to `file`, through the shared atomic publish.
    ///
    /// A run killed mid-write leaves the previous records rather than half of
    /// this run's. The parent is created here, because the publish creates
    /// nothing.
    ///
    /// # Errors
    ///
    /// Returns the I/O failure that stopped the write, or the encode failure
    /// that stopped it earlier.
    fn write_file(
        file: &Path,
        records: &BTreeMap<String, OutputRecord>,
    ) -> Result<(), MediaPmError> {
        let directory = file.parent().unwrap_or(Path::new("."));
        if let Err(source) = std::fs::create_dir_all(directory) {
            return Err(MediaPmError::Io {
                operation: "creating the materialization cache directory".to_string(),
                path: directory.to_path_buf(),
                source,
            });
        }
        let bytes = serde_json::to_vec_pretty(&CacheFile {
            version: CACHE_VERSION,
            outputs: records.clone(),
        })
        .map_err(|error| {
            MediaPmError::Workflow(format!("encoding the materialization cache failed: {error}"))
        })?;

        mediapm_utils::fs::write_bytes_atomically(file, &bytes).map_err(|source| MediaPmError::Io {
            operation: "publishing the materialization cache".to_string(),
            path: file.to_path_buf(),
            source,
        })
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    /// Writes `bytes` where a workspace's record cache goes and answers the
    /// cache a run would then open.
    fn cache_over(paths: &MediaPmPaths, bytes: &[u8]) -> OutputCache {
        let file = cache_file_path(paths);
        std::fs::create_dir_all(file.parent().expect("the cache path names a directory")).unwrap();
        std::fs::write(&file, bytes).expect("the cache file is writable");
        OutputCache::open(paths)
    }

    /// A workspace nothing has run against, which is the state a fresh one
    /// starts in and the state every unusable cache file falls back to.
    fn empty_workspace() -> (tempfile::TempDir, MediaPmPaths) {
        let root = mediapm_utils::temp::artifact_dir().unwrap();
        let paths = MediaPmPaths::from_root(root.path());
        (root, paths)
    }

    /// A record file this build writes, with the numbers a run would leave
    /// behind.
    fn current_record_file(relative_path: &str) -> Vec<u8> {
        serde_json::to_vec_pretty(&CacheFile {
            version: CACHE_VERSION,
            outputs: BTreeMap::from([(
                relative_path.to_string(),
                OutputRecord { hash: "blake3:recorded".to_string(), len: 7, mtime_nanos: 7 },
            )]),
        })
        .unwrap()
    }

    /// A file carrying a version this build does not read is read as carrying
    /// no records rather than as the wrong numbers.
    ///
    /// Two versions matter. A later one is the field-adds-without-a-bump
    /// failure: entries matched against a hash a file does not have, which is a
    /// silent skip rather than a re-read. An earlier one is the record a build
    /// left under this name before the format moved, whose numbers describe the
    /// same library in a shape this build does not read.
    #[test]
    fn a_cache_from_another_version_is_read_as_empty() {
        let (_workspace, paths) = empty_workspace();
        let later = serde_json::to_vec_pretty(&CacheFile {
            version: CACHE_VERSION + 1,
            outputs: BTreeMap::from([(
                "song".to_string(),
                OutputRecord { hash: "blake3:nonsense".to_string(), len: 7, mtime_nanos: 7 },
            )]),
        })
        .unwrap();
        let earlier = br#"{"version": 1, "outputs": {"song": {"hash": "blake3:earlier", "len": 7, "mtime_nanos": 7}}}"#;

        for (label, bytes) in
            [("a later version", later.as_slice()), ("version 1", earlier.as_slice())]
        {
            let cache = cache_over(&paths, bytes);
            let record = cache.remembered.get("song");
            assert!(
                record.is_none(),
                "{label} has to cost a re-read, not a skip on numbers it never checked: {record:?}"
            );
        }
    }

    /// A file that is empty, or that is not JSON at all, costs the re-read a
    /// missing file costs.
    ///
    /// A cache is a file a person is free to truncate, and an interrupted write
    /// is the same shape. Neither may end a run: the answer is an empty cache,
    /// which is the state a workspace starts in.
    #[test]
    fn an_empty_or_malformed_file_is_read_as_no_records() {
        let (_workspace, paths) = empty_workspace();
        for (label, bytes) in [("empty", b"".as_slice()), ("malformed", b"{ \"outputs\": ")] {
            let cache = cache_over(&paths, bytes);
            assert!(
                cache.remembered.is_empty(),
                "a {label} cache file has to read as no records rather than fail the run: \
                 {:?}",
                cache.remembered
            );
        }
    }

    /// A file naming no version at all holds nothing this build can reuse.
    ///
    /// The version is what says whose numbers these are. A hand-written file
    /// that leaves it out states no format, so its numbers cannot be compared
    /// against records this build derived, and the run reads the outputs again
    /// instead of trusting them.
    #[test]
    fn a_file_naming_no_version_is_read_as_empty() {
        let (_workspace, paths) = empty_workspace();
        let unversioned = br#"{"outputs": {"song": {"hash": "h", "len": 7, "mtime_nanos": 7}}}"#;
        let cache = cache_over(&paths, unversioned);

        assert!(
            cache.remembered.is_empty(),
            "a file that names no version has to cost a re-read: {:?}",
            cache.remembered
        );
    }

    /// A record this run confirms reaches the file through `flush`, and the next
    /// run reads it back out of `outputs`.
    ///
    /// The file is the only thing that carries a record between runs, so this is
    /// the round trip the whole cache rests on.
    #[test]
    fn a_confirmed_record_reaches_the_next_run_through_the_file() {
        let (_workspace, paths) = empty_workspace();
        let cache = cache_over(&paths, &current_record_file("song"));

        cache.confirm("other", &Hash::from_content(b"seven!"), 7, Some(7));
        cache.flush().expect("flush");

        let reopened = OutputCache::open(&paths);
        assert!(
            reopened.is_current("other", &Hash::from_content(b"seven!"), 7, Some(7)),
            "a record written by one run has to answer the next one: {:?}",
            reopened.remembered
        );
        assert!(
            !reopened.is_current("song", &Hash::from_content(b"seven!"), 7, Some(7)),
            "a path this run never confirmed is dropped rather than carried forward: {:?}",
            reopened.remembered
        );
    }

    /// The record cache names the shape it writes: a version and the records,
    /// under `outputs`.
    ///
    /// A reader opening the file learns which format wrote these numbers before
    /// trusting one, and the field name says what a record is rather than what
    /// a run concluded about it.
    #[test]
    fn the_materialization_cache_names_its_records_outputs() {
        let (_workspace, paths) = empty_workspace();
        let cache = cache_over(&paths, &[]);
        cache.confirm("song", &Hash::from_content(b"seven!"), 7, Some(7));
        cache.flush().expect("flush");

        let text =
            std::fs::read_to_string(cache_file_path(&paths)).expect("the run wrote the cache file");
        let parsed: serde_json::Value = serde_json::from_str(&text).expect("the cache is json");
        let mut keys: Vec<&str> = parsed
            .as_object()
            .expect("the cache file is an object")
            .keys()
            .map(String::as_str)
            .collect();
        keys.sort_unstable();
        assert_eq!(
            keys,
            ["outputs", "version"],
            "the file holds the version and the records and nothing else: {text}"
        );
        assert_eq!(
            parsed["version"],
            serde_json::json!(2),
            "the file names the format version it carries: {text}"
        );
        assert_eq!(
            parsed["outputs"]["song"]["hash"],
            serde_json::json!(Hash::from_content(b"seven!").to_string()),
            "every record lives under `outputs`: {text}"
        );
    }
}
