//! Startup recovery, explicit repair, and backup helpers for filesystem CAS.
//!
//! This module keeps index-loss handling deterministic by treating the object
//! store as the authoritative source for object metadata while using backup
//! snapshots only to restore explicit constraint rows when possible.
//!
//! ## DO NOT REMOVE: external versions boundary guard
//!
//! This file is outside `codec/versions/` and `index/versions/`. Keep recovery
//! state models unversioned and use `versions/mod.rs` only at
//! serialization/deserialization boundaries.

use std::collections::{BTreeMap, BTreeSet, HashMap, HashSet};
use std::io::Write as _;
use std::path::{Path, PathBuf};
use std::str::FromStr;
use std::thread;
use std::time::{Duration, SystemTime, UNIX_EPOCH};

use postcard::{from_bytes, to_allocvec};
use serde::{Deserialize, Serialize};

use crate::storage::normalize_explicit_constraint_set;
use crate::{
    CasError, CasIndexDb, DeltaPatch, FileSystemRecoveryOptions, Hash, IndexRepairConstraintSource,
    IndexRepairReport, IndexState, ObjectMeta, StoredObject, empty_content_hash,
    ensure_empty_record, recalculate_depths,
};

use super::STORAGE_VERSION;

/// Directory name for serialized index backup snapshots.
const INDEX_BACKUP_DIR_NAME: &str = "index-backups";
/// Prefix for backup snapshot file names.
const INDEX_BACKUP_FILE_PREFIX: &str = "index-backup-";
/// Backup snapshot file extension.
const INDEX_BACKUP_FILE_SUFFIX: &str = ".postcard";
/// Current serialized backup payload format version.
const INDEX_BACKUP_FORMAT_VERSION: u32 = 1;
/// Maximum stale-backup deletion attempts before surfacing an IO failure.
const INDEX_BACKUP_PRUNE_REMOVE_ATTEMPTS: usize = 6;
/// Fixed backoff used between stale-backup deletion retries.
const INDEX_BACKUP_PRUNE_BACKOFF_MS: u64 = 40;

#[derive(Debug, Clone)]
/// Constraint source seed used during index rebuild.
pub(super) struct ConstraintSeed {
    /// Explicit constraint rows to attempt restoring.
    pub(super) constraints: BTreeMap<Hash, BTreeSet<Hash>>,
    /// Number of backup snapshots scanned while looking for constraints.
    pub(super) backup_snapshots_considered: usize,
    /// Provenance of restored constraints.
    pub(super) source: IndexRepairConstraintSource,
}

#[derive(Debug, Clone)]
/// Result of rebuilding runtime index state from object-store scan.
pub(super) struct RecoveredIndexState {
    /// Reconstructed runtime index state.
    pub(super) state: IndexState,
    /// User-visible repair report counters and source metadata.
    pub(super) report: IndexRepairReport,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
/// Serializable snapshot payload used for backup files.
struct IndexBackupState {
    /// Object metadata rows by hash.
    objects: BTreeMap<Hash, ObjectMeta>,
    /// Explicit constraint rows by target hash.
    constraints: BTreeMap<Hash, BTreeSet<Hash>>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
/// Backup file envelope with versioning and timestamp metadata.
struct IndexBackupSnapshot {
    /// Snapshot payload schema version.
    format_version: u32,
    /// Snapshot creation time in milliseconds since UNIX epoch.
    created_unix_millis: u64,
    /// Serialized index state payload.
    state: IndexBackupState,
}

#[derive(Debug, Clone)]
/// Object-store scan catalog used as recovery input.
struct ScannedObjectCatalog {
    /// Parsed objects keyed by hash.
    objects: BTreeMap<Hash, StoredObject>,
    /// Count of file entries visited during scan.
    scanned_object_files: usize,
    /// Count of file entries skipped as invalid/corrupt/unparseable.
    skipped_object_files: usize,
}

#[derive(Debug, Clone, Copy)]
/// Parsed object-file kind derived from file-name extension.
enum ParsedObjectKind {
    /// Raw full-payload object file (`<hash>`).
    Full,
    /// Delta-envelope object file (`<hash>.diff`).
    Delta,
}

/// Loads durable index state, recovering from object files when required.
///
/// Returns:
/// - opened index database handle,
/// - usable in-memory index state,
/// - optional repair report when recovery was executed.
pub(super) fn load_or_recover_primary_index(
    root: &Path,
    recovery: &FileSystemRecoveryOptions,
) -> Result<(CasIndexDb, IndexState, Option<IndexRepairReport>), CasError> {
    let primary_missing = !primary_index_path(root).exists();

    match CasIndexDb::open(root) {
        Ok(db) => {
            let has_data = db.has_data()?;
            if has_data {
                match db.load_state() {
                    Ok(mut state) => {
                        ensure_empty_record(&mut state);
                        state.rebuild_constraint_reverse();
                        return Ok((db, state, None));
                    }
                    Err(err) => {
                        return recover_after_primary_failure(
                            root,
                            recovery,
                            Some(err),
                            Some(db),
                            primary_missing,
                        );
                    }
                }
            }

            if !object_store_contains_non_empty_objects(root)? {
                let mut state = IndexState::default();
                ensure_empty_record(&mut state);
                state.rebuild_constraint_reverse();
                return Ok((db, state, None));
            }

            if recovery.mode == crate::IndexRecoveryMode::Strict {
                return Err(primary_index_missing_or_empty_error(root, primary_missing));
            }

            let seed = choose_constraint_seed(root, None)?;
            let recovered = rebuild_index_from_object_store(root, &seed)?;
            db.persist_state(&recovered.state)?;
            Ok((db, recovered.state, Some(recovered.report)))
        }
        Err(err) => recover_after_primary_failure(root, recovery, Some(err), None, primary_missing),
    }
}

/// Chooses explicit-constraint seed rows for recovery workflows.
///
/// Preference order:
/// 1. caller-provided in-memory constraints,
/// 2. newest valid backup snapshot constraints,
/// 3. no constraints.
pub(super) fn choose_constraint_seed(
    root: &Path,
    current_constraints: Option<BTreeMap<Hash, BTreeSet<Hash>>>,
) -> Result<ConstraintSeed, CasError> {
    if let Some(current_constraints) = current_constraints
        && !current_constraints.is_empty()
    {
        return Ok(ConstraintSeed {
            constraints: current_constraints,
            backup_snapshots_considered: 0,
            source: IndexRepairConstraintSource::InMemoryIndex,
        });
    }

    let backup_paths = backup_snapshot_paths(root)?;
    let mut considered = 0usize;
    for path in backup_paths {
        considered = considered.saturating_add(1);
        let Ok(bytes) = std::fs::read(&path) else {
            continue;
        };
        let Ok(snapshot) = from_bytes::<IndexBackupSnapshot>(&bytes) else {
            continue;
        };
        if snapshot.format_version != INDEX_BACKUP_FORMAT_VERSION {
            continue;
        }

        if snapshot.state.constraints.is_empty() {
            continue;
        }

        return Ok(ConstraintSeed {
            constraints: snapshot.state.constraints,
            backup_snapshots_considered: considered,
            source: IndexRepairConstraintSource::BackupSnapshot,
        });
    }

    Ok(ConstraintSeed {
        constraints: BTreeMap::new(),
        backup_snapshots_considered: considered,
        source: IndexRepairConstraintSource::None,
    })
}

/// Rebuilds full index state by scanning object files and replaying invariants.
///
/// Recovery pipeline:
/// 1. scan and parse object files,
/// 2. validate hash/content integrity and delta dependencies,
/// 3. restore explicit constraints from selected seed,
/// 4. rebuild reverse maps and depth invariants.
pub(super) fn rebuild_index_from_object_store(
    root: &Path,
    constraint_seed: &ConstraintSeed,
) -> Result<RecoveredIndexState, CasError> {
    let catalog = scan_object_store(root)?;
    let (mut state, additional_skipped) = validate_catalog_into_index_state(&catalog);
    let restored_constraint_rows = restore_explicit_constraints(&mut state, constraint_seed);

    ensure_empty_record(&mut state);
    state.rebuild_constraint_reverse();
    recalculate_depths(&mut state)?;

    Ok(RecoveredIndexState {
        report: IndexRepairReport {
            object_rows_rebuilt: state.objects.len().saturating_sub(1),
            explicit_constraint_rows_restored: restored_constraint_rows,
            scanned_object_files: catalog.scanned_object_files,
            skipped_object_files: catalog.skipped_object_files.saturating_add(additional_skipped),
            backup_snapshots_considered: constraint_seed.backup_snapshots_considered,
            constraint_source: if restored_constraint_rows == 0 {
                IndexRepairConstraintSource::None
            } else {
                constraint_seed.source
            },
        },
        state,
    })
}

/// Writes one atomic backup snapshot and prunes old snapshots by retention.
pub(super) fn write_backup_snapshot(
    root: &Path,
    state: &IndexState,
    max_backup_snapshots: usize,
) -> Result<(), CasError> {
    if max_backup_snapshots == 0 {
        return Ok(());
    }

    let backup_root = backup_snapshot_root(root);
    std::fs::create_dir_all(&backup_root)
        .map_err(|source| CasError::io("creating index backup directory", &backup_root, source))?;

    let snapshot = IndexBackupSnapshot {
        format_version: INDEX_BACKUP_FORMAT_VERSION,
        created_unix_millis: unix_epoch_millis(),
        state: IndexBackupState {
            objects: state.objects.clone(),
            constraints: state.constraints.clone(),
        },
    };
    let bytes = to_allocvec(&snapshot).map_err(CasError::codec)?;
    let file_nonce = unix_epoch_nanos();

    let file_name =
        format!("{INDEX_BACKUP_FILE_PREFIX}{file_nonce:032}-{:010}.postcard", std::process::id());
    let path = backup_root.join(file_name);
    write_backup_file_atomic(&backup_root, &path, &bytes)?;
    prune_old_backups(&backup_root, max_backup_snapshots)
}

/// Handles recovery flow after primary index open/load failures.
fn recover_after_primary_failure(
    root: &Path,
    recovery: &FileSystemRecoveryOptions,
    error: Option<CasError>,
    existing_db: Option<CasIndexDb>,
    primary_missing: bool,
) -> Result<(CasIndexDb, IndexState, Option<IndexRepairReport>), CasError> {
    if recovery.mode == crate::IndexRecoveryMode::Strict {
        return Err(
            error.unwrap_or_else(|| primary_index_missing_or_empty_error(root, primary_missing))
        );
    }

    if !object_store_contains_non_empty_objects(root)? {
        let db = match existing_db {
            Some(db) => db,
            None => CasIndexDb::open(root)?,
        };
        let mut state = IndexState::default();
        ensure_empty_record(&mut state);
        state.rebuild_constraint_reverse();
        return Ok((db, state, None));
    }

    let seed = choose_constraint_seed(root, None)?;
    let recovered = rebuild_index_from_object_store(root, &seed)?;
    let db = recreate_primary_index(root)?;
    db.persist_state(&recovered.state)?;
    Ok((db, recovered.state, Some(recovered.report)))
}

/// Builds strict-mode startup error for missing/empty primary index state.
fn primary_index_missing_or_empty_error(root: &Path, primary_missing: bool) -> CasError {
    let reason = if primary_missing { "missing" } else { "empty" };
    CasError::corrupt_index(format!(
        "primary index is {reason} while persisted object files exist under {}; reopen with recover mode or run repair_index",
        root.display()
    ))
}

/// Recreates the primary index file from scratch.
fn recreate_primary_index(root: &Path) -> Result<CasIndexDb, CasError> {
    let path = primary_index_path(root);
    if path.exists() {
        std::fs::remove_file(&path).map_err(|source| {
            CasError::io("removing stale primary index before recovery", &path, source)
        })?;
    }
    CasIndexDb::open(root)
}

/// Returns canonical primary index file path.
fn primary_index_path(root: &Path) -> PathBuf {
    root.join("index.redb")
}

/// Returns canonical backup snapshot directory path.
fn backup_snapshot_root(root: &Path) -> PathBuf {
    root.join(INDEX_BACKUP_DIR_NAME)
}

/// Opens one directory for object-store traversal, tolerating transient
/// `NotFound` races caused by concurrent prune/delete operations.
fn read_object_store_dir_tolerant(dir: &Path) -> Result<Option<std::fs::ReadDir>, CasError> {
    match std::fs::read_dir(dir) {
        Ok(entries) => Ok(Some(entries)),
        Err(source) if source.kind() == std::io::ErrorKind::NotFound => Ok(None),
        Err(source) => Err(CasError::io("reading object store directory", dir, source)),
    }
}

/// Returns whether object store contains at least one non-empty object file.
fn object_store_contains_non_empty_objects(root: &Path) -> Result<bool, CasError> {
    let storage_root = root.join(STORAGE_VERSION);
    if !storage_root.exists() {
        return Ok(false);
    }

    let mut stack = vec![storage_root];
    while let Some(dir) = stack.pop() {
        let Some(entries) = read_object_store_dir_tolerant(&dir)? else {
            continue;
        };

        for entry in entries {
            let entry = match entry {
                Ok(entry) => entry,
                Err(source) if source.kind() == std::io::ErrorKind::NotFound => continue,
                Err(source) => {
                    return Err(CasError::io("iterating object store directory", &dir, source));
                }
            };

            let path = entry.path();
            let file_type = match entry.file_type() {
                Ok(file_type) => file_type,
                Err(source) if source.kind() == std::io::ErrorKind::NotFound => continue,
                Err(source) => {
                    return Err(CasError::io("reading object store entry type", path, source));
                }
            };

            if file_type.is_dir() {
                if path.file_name().is_some_and(|name| name == "tmp") {
                    continue;
                }
                stack.push(path);
                continue;
            }
            if !file_type.is_file() {
                continue;
            }

            let Some((hash, _kind)) = parse_hash_from_object_path(root, &path) else {
                continue;
            };
            if hash != empty_content_hash() {
                return Ok(true);
            }
        }
    }

    Ok(false)
}

/// Recursively scans object-store files and builds a best-effort object catalog.
fn scan_object_store(root: &Path) -> Result<ScannedObjectCatalog, CasError> {
    let storage_root = root.join(STORAGE_VERSION);
    if !storage_root.exists() {
        return Ok(ScannedObjectCatalog {
            objects: BTreeMap::new(),
            scanned_object_files: 0,
            skipped_object_files: 0,
        });
    }

    let mut scanned_object_files = 0usize;
    let mut skipped_object_files = 0usize;
    let mut objects = BTreeMap::new();
    let mut stack = vec![storage_root];

    while let Some(dir) = stack.pop() {
        let Some(entries) = read_object_store_dir_tolerant(&dir)? else {
            continue;
        };

        for entry in entries {
            let entry = match entry {
                Ok(entry) => entry,
                Err(source) if source.kind() == std::io::ErrorKind::NotFound => continue,
                Err(source) => {
                    return Err(CasError::io("iterating object store directory", &dir, source));
                }
            };

            let path = entry.path();
            let file_type = match entry.file_type() {
                Ok(file_type) => file_type,
                Err(source) if source.kind() == std::io::ErrorKind::NotFound => continue,
                Err(source) => {
                    return Err(CasError::io("reading object store entry type", path, source));
                }
            };

            if file_type.is_dir() {
                if path.file_name().is_some_and(|name| name == "tmp") {
                    continue;
                }
                stack.push(path);
                continue;
            }
            if !file_type.is_file() {
                continue;
            }

            scanned_object_files = scanned_object_files.saturating_add(1);
            let Some((hash, kind)) = parse_hash_from_object_path(root, &path) else {
                skipped_object_files = skipped_object_files.saturating_add(1);
                continue;
            };

            let Ok(bytes) = std::fs::read(&path) else {
                skipped_object_files = skipped_object_files.saturating_add(1);
                continue;
            };

            let candidate = match kind {
                ParsedObjectKind::Full => {
                    if Hash::from_content(&bytes) != hash {
                        skipped_object_files = skipped_object_files.saturating_add(1);
                        continue;
                    }
                    StoredObject::full(bytes)
                }
                ParsedObjectKind::Delta => {
                    if let Ok(object) = StoredObject::decode_delta(&bytes) {
                        object
                    } else {
                        skipped_object_files = skipped_object_files.saturating_add(1);
                        continue;
                    }
                }
            };

            match objects.get(&hash) {
                Some(existing)
                    if matches!(existing, StoredObject::Delta { .. })
                        && matches!(candidate, StoredObject::Full { .. }) =>
                {
                    objects.insert(hash, candidate);
                }
                None => {
                    objects.insert(hash, candidate);
                }
                Some(_) => {}
            }
        }
    }

    Ok(ScannedObjectCatalog { objects, scanned_object_files, skipped_object_files })
}

/// Validates scanned catalog content integrity and converts to runtime index state.
fn validate_catalog_into_index_state(catalog: &ScannedObjectCatalog) -> (IndexState, usize) {
    let mut memo = HashMap::<Hash, Vec<u8>>::new();
    let mut invalid = HashSet::<Hash>::new();
    let mut visiting = HashSet::<Hash>::new();

    for hash in catalog.objects.keys().copied() {
        let _ =
            validate_hash_content(hash, &catalog.objects, &mut memo, &mut invalid, &mut visiting);
    }

    let mut state = IndexState::default();
    for (hash, object) in &catalog.objects {
        if invalid.contains(hash) {
            continue;
        }
        match object.base_hash() {
            Some(base_hash) => {
                state.objects.insert(
                    *hash,
                    ObjectMeta::delta(object.payload_len(), object.content_len(), 0, base_hash),
                );
            }
            None => {
                state
                    .objects
                    .insert(*hash, ObjectMeta::full(object.payload_len(), object.content_len(), 0));
            }
        }
    }

    (state, invalid.len())
}

/// Validates one object hash by recursively reconstructing/confirming its bytes.
///
/// Uses memoization and cycle detection to avoid repeated reconstruction.
fn validate_hash_content(
    hash: Hash,
    objects: &BTreeMap<Hash, StoredObject>,
    memo: &mut HashMap<Hash, Vec<u8>>,
    invalid: &mut HashSet<Hash>,
    visiting: &mut HashSet<Hash>,
) -> Option<Vec<u8>> {
    if invalid.contains(&hash) {
        return None;
    }
    if let Some(bytes) = memo.get(&hash) {
        return Some(bytes.clone());
    }
    if !visiting.insert(hash) {
        invalid.insert(hash);
        return None;
    }

    let resolved = match objects.get(&hash)? {
        StoredObject::Full { payload } => {
            if Hash::from_content(payload) == hash {
                Some(payload.clone())
            } else {
                None
            }
        }
        StoredObject::Delta { state } => {
            let base_bytes =
                validate_hash_content(state.base_hash, objects, memo, invalid, visiting)?;
            let patch = DeltaPatch::decode(state.payload.as_ref()).ok()?;
            let rebuilt = patch.apply(&base_bytes).ok()?;
            if rebuilt.len() as u64 != state.content_len || Hash::from_content(&rebuilt) != hash {
                None
            } else {
                Some(rebuilt)
            }
        }
    };

    let _ = visiting.remove(&hash);
    if let Some(bytes) = resolved {
        memo.insert(hash, bytes.clone());
        Some(bytes)
    } else {
        invalid.insert(hash);
        None
    }
}

/// Restores explicit constraints from seed while filtering invalid rows/bases.
fn restore_explicit_constraints(state: &mut IndexState, constraint_seed: &ConstraintSeed) -> usize {
    let mut restored = 0usize;

    for (target_hash, bases) in &constraint_seed.constraints {
        if *target_hash == empty_content_hash() || !state.objects.contains_key(target_hash) {
            continue;
        }

        let filtered = bases
            .iter()
            .copied()
            .filter(|base| {
                *base != *target_hash
                    && (*base == empty_content_hash() || state.objects.contains_key(base))
            })
            .collect::<BTreeSet<_>>();

        if let Some(explicit) = normalize_explicit_constraint_set(filtered) {
            state.constraints.insert(*target_hash, explicit);
            restored = restored.saturating_add(1);
        }
    }

    restored
}

/// Lists backup snapshot files newest-first.
fn backup_snapshot_paths(root: &Path) -> Result<Vec<PathBuf>, CasError> {
    let backup_root = backup_snapshot_root(root);
    if !backup_root.exists() {
        return Ok(Vec::new());
    }

    let mut paths = std::fs::read_dir(&backup_root)
        .map_err(|source| CasError::io("reading index backup directory", &backup_root, source))?
        .filter_map(|entry| entry.ok().map(|entry| entry.path()))
        .filter(|path| {
            path.file_name().and_then(|name| name.to_str()).is_some_and(|name| {
                name.starts_with(INDEX_BACKUP_FILE_PREFIX)
                    && name.ends_with(INDEX_BACKUP_FILE_SUFFIX)
            })
        })
        .collect::<Vec<_>>();
    paths.sort();
    paths.reverse();
    Ok(paths)
}

/// Removes stale backup snapshots beyond `keep` newest files.
fn prune_old_backups(backup_root: &Path, keep: usize) -> Result<(), CasError> {
    let mut paths = std::fs::read_dir(backup_root)
        .map_err(|source| CasError::io("reading index backup directory", backup_root, source))?
        .filter_map(|entry| entry.ok().map(|entry| entry.path()))
        .filter(|path| path.is_file())
        .collect::<Vec<_>>();
    paths.sort();
    paths.reverse();

    for stale in paths.into_iter().skip(keep) {
        remove_stale_backup_file_with_retry(&stale).map_err(|source| {
            CasError::io("removing stale index backup snapshot", &stale, source)
        })?;
    }

    Ok(())
}

/// Removes one stale backup file with bounded retries for transient locks.
///
/// Windows antivirus/indexer races may hold backup snapshots briefly. Treat
/// `NotFound` as already-pruned and retry only transient lock-style errors.
fn remove_stale_backup_file_with_retry(path: &Path) -> Result<(), std::io::Error> {
    for attempt in 0..INDEX_BACKUP_PRUNE_REMOVE_ATTEMPTS {
        match std::fs::remove_file(path) {
            Ok(()) => return Ok(()),
            Err(error) if error.kind() == std::io::ErrorKind::NotFound => return Ok(()),
            Err(error) => {
                let retryable = error.kind() == std::io::ErrorKind::PermissionDenied
                    || error.raw_os_error() == Some(32);
                if !retryable || attempt + 1 == INDEX_BACKUP_PRUNE_REMOVE_ATTEMPTS {
                    return Err(error);
                }
                thread::sleep(Duration::from_millis(INDEX_BACKUP_PRUNE_BACKOFF_MS));
            }
        }
    }

    Ok(())
}

/// Atomically writes one backup file via staged temp-file rename.
fn write_backup_file_atomic(
    backup_root: &Path,
    target: &Path,
    bytes: &[u8],
) -> Result<(), CasError> {
    let staging_root = backup_root.join("tmp");
    std::fs::create_dir_all(&staging_root).map_err(|source| {
        CasError::io("creating index backup staging directory", &staging_root, source)
    })?;

    let mut temp = tempfile::Builder::new()
        .prefix("index-backup-")
        .suffix(".stage")
        .tempfile_in(&staging_root)
        .map_err(|source| CasError::io("creating index backup temp file", &staging_root, source))?;
    temp.write_all(bytes)
        .map_err(|source| CasError::io("writing index backup temp file", temp.path(), source))?;
    temp.as_file()
        .sync_all()
        .map_err(|source| CasError::io("syncing index backup temp file", temp.path(), source))?;
    let (_file, staged_path) = temp.keep().map_err(|source| {
        CasError::io("materializing index backup temp file path", source.file.path(), source.error)
    })?;

    if let Err(source) = std::fs::rename(&staged_path, target) {
        let _ = std::fs::remove_file(&staged_path);
        return Err(CasError::io("renaming index backup snapshot into place", target, source));
    }

    Ok(())
}

/// Parses one object file path into `(hash, kind)` when layout is recognized.
fn parse_hash_from_object_path(root: &Path, path: &Path) -> Option<(Hash, ParsedObjectKind)> {
    let storage_root = root.join(STORAGE_VERSION);
    let relative = path.strip_prefix(storage_root).ok()?;
    let components = relative
        .iter()
        .map(|component| component.to_string_lossy().to_string())
        .collect::<Vec<_>>();
    if components.len() != 4 {
        return None;
    }

    let algorithm = &components[0];
    let first = &components[1];
    let second = &components[2];
    let file_name = &components[3];

    let (rest, kind) = match file_name.strip_suffix(".diff") {
        Some(rest) => (rest, ParsedObjectKind::Delta),
        None if !file_name.contains('.') => (file_name.as_str(), ParsedObjectKind::Full),
        None => return None,
    };

    let hash_hex = format!("{first}{second}{rest}");

    let hash = if hash_hex.len() == 64 {
        Hash::from_str(&format!("{algorithm}:{hash_hex}")).ok()?
    } else {
        // Backward-compatible parse path for older stores that used
        // multihash storage-hex fanout (`1e20...`) under algorithm directory.
        let bytes = decode_hex(&hash_hex)?;
        let parsed = Hash::from_storage_bytes(&bytes).ok()?;
        if parsed.algorithm_name() != algorithm {
            return None;
        }
        parsed
    };

    Some((hash, kind))
}

/// Decodes an even-length hexadecimal string into bytes.
fn decode_hex(value: &str) -> Option<Vec<u8>> {
    if !value.len().is_multiple_of(2) {
        return None;
    }

    let mut bytes = Vec::with_capacity(value.len() / 2);
    let mut chars = value.as_bytes().chunks_exact(2);
    for chunk in &mut chars {
        let high = decode_hex_nibble(chunk[0])?;
        let low = decode_hex_nibble(chunk[1])?;
        bytes.push((high << 4) | low);
    }
    Some(bytes)
}

/// Decodes one hexadecimal nibble byte.
const fn decode_hex_nibble(byte: u8) -> Option<u8> {
    match byte {
        b'0'..=b'9' => Some(byte - b'0'),
        b'a'..=b'f' => Some(byte - b'a' + 10),
        b'A'..=b'F' => Some(byte - b'A' + 10),
        _ => None,
    }
}

/// Returns current UNIX epoch milliseconds, saturating on conversion bounds.
fn unix_epoch_millis() -> u64 {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .map(|duration| {
            u64::try_from(duration.as_millis().min(u128::from(u64::MAX))).unwrap_or(u64::MAX)
        })
        .unwrap_or(0)
}

/// Returns current UNIX epoch nanoseconds (best-effort).
fn unix_epoch_nanos() -> u128 {
    SystemTime::now().duration_since(UNIX_EPOCH).map(|duration| duration.as_nanos()).unwrap_or(0)
}

#[cfg(test)]
mod tests {
    use super::read_object_store_dir_tolerant;
    use super::remove_stale_backup_file_with_retry;

    /// Ensures stale-backup pruning treats already-removed files as success.
    #[test]
    fn remove_stale_backup_file_with_retry_ignores_not_found() {
        let temp = tempfile::tempdir().expect("tempdir");
        let missing = temp.path().join("missing.postcard");

        remove_stale_backup_file_with_retry(&missing)
            .expect("missing stale backup should be treated as already pruned");
    }

    /// Ensures stale-backup pruning removes existing files successfully.
    #[test]
    fn remove_stale_backup_file_with_retry_removes_existing_file() {
        let temp = tempfile::tempdir().expect("tempdir");
        let stale = temp.path().join("stale.postcard");
        std::fs::write(&stale, b"backup").expect("write stale backup");

        remove_stale_backup_file_with_retry(&stale)
            .expect("existing stale backup should be removed");
        assert!(!stale.exists(), "stale backup should no longer exist");
    }

    /// Ensures object-store traversal can tolerate directories that disappear
    /// between enumeration and recursive descent.
    #[test]
    fn read_object_store_dir_tolerant_returns_none_for_missing_directory() {
        let temp = tempfile::tempdir().expect("tempdir");
        let missing = temp.path().join("missing-dir");

        let entries = read_object_store_dir_tolerant(&missing)
            .expect("missing directory should not surface an IO error");

        assert!(entries.is_none());
    }
}
