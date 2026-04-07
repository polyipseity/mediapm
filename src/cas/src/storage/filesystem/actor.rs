//! Mutation-focused object actor for filesystem CAS.
//!
//! This actor serializes physical object-file mutations so we can keep
//! `total_store_size` and mmap-safe delete/replace behavior synchronized.

use std::collections::HashMap;
use std::path::{Path, PathBuf};
use std::sync::Arc;
use std::time::Instant;

use memmap2::MmapOptions;
use parking_lot::Mutex;
use ractor::{Actor, ActorProcessingErr, ActorRef, RpcReplyPort};
use tokio::fs;
use tokio::sync::Notify;
use tracing::info;

use crate::{CasError, Hash, StoredObject};

use super::{
    FILESYSTEM_MMAP_MIN_BYTES, STORAGE_VERSION, diff_object_path, object_path, write_object_atomic,
};

/// Message contract for the file-mutation actor.
#[derive(Debug)]
pub(super) enum FileObjectActorMessage {
    /// Persist one object variant (full or delta) for `Hash`.
    PersistObjectVariant(Hash, StoredObject, RpcReplyPort<Result<(), CasError>>),
    /// Remove all on-disk object variants (`<hash>` and `<hash>.diff`) for a hash.
    DeleteObjectFiles(Hash, RpcReplyPort<Result<(), CasError>>),
    /// Return the actor's tracked total CAS-store byte size.
    CasStoreSizeBytes(RpcReplyPort<Result<u64, CasError>>),
}

/// Mutable actor state for serialized object-file mutations.
#[derive(Debug, Clone)]
pub(super) struct FileObjectActorState {
    /// Filesystem root configured for this CAS backend.
    pub(super) root: PathBuf,
    /// Cached total size in bytes of persisted object files under `root`.
    pub(super) total_store_size: u64,
    /// Registry of currently active memory mappings by object hash.
    pub(super) active_mmaps: Arc<ActiveMmapRegistry>,
}

/// Shared registry tracking active mmap readers per hash.
#[derive(Debug, Default)]
pub(super) struct ActiveMmapRegistry {
    /// Map keyed by object hash containing active-reader counters.
    pub(super) state: Mutex<HashMap<Hash, ActiveMmapEntry>>,
}

/// Per-hash mmap activity metadata.
#[derive(Debug)]
pub(super) struct ActiveMmapEntry {
    /// Number of currently active mmap readers for this hash.
    pub(super) active: usize,
    /// Notifier triggered when `active` drops to zero.
    pub(super) released: Arc<Notify>,
}

/// RAII lease that keeps a hash marked as actively memory-mapped.
#[derive(Debug)]
pub(super) struct ActiveMmapLease {
    hash: Hash,
    registry: Arc<ActiveMmapRegistry>,
}

/// Lease lifecycle utilities for active-mmap registry accounting.
impl ActiveMmapLease {
    /// Acquires one active-mmap lease for `hash` in `registry`.
    ///
    /// Dropping the returned lease decrements the active counter and notifies
    /// waiters when the last reader releases the hash.
    pub(super) fn acquire(registry: Arc<ActiveMmapRegistry>, hash: Hash) -> Self {
        {
            let mut state = registry.state.lock();
            let entry = state.entry(hash).or_insert_with(|| ActiveMmapEntry {
                active: 0,
                released: Arc::new(Notify::new()),
            });
            entry.active = entry.active.saturating_add(1);
        }

        Self { hash, registry }
    }
}

/// Decrements active-mmap counter and wakes waiters when it reaches zero.
impl Drop for ActiveMmapLease {
    fn drop(&mut self) {
        let mut notify = None;
        {
            let mut state = self.registry.state.lock();
            if let Some(entry) = state.get_mut(&self.hash) {
                entry.active = entry.active.saturating_sub(1);
                if entry.active == 0 {
                    notify = Some(entry.released.clone());
                    state.remove(&self.hash);
                }
            }
        }

        if let Some(notify) = notify {
            notify.notify_waiters();
        }
    }
}

/// Memory map coupled with an active-mmap lease.
///
/// Holding this type guarantees delete/replace paths see the mmap as active
/// until all bytes owners are dropped.
#[derive(Debug)]
pub(super) struct GuardedMmap {
    /// Memory-mapped file region containing full-object bytes.
    pub(super) mmap: memmap2::Mmap,
    /// Lease that keeps mmap activity visible to mutation paths.
    pub(super) _lease: ActiveMmapLease,
}

/// Borrows mmap bytes while keeping lease alive.
impl AsRef<[u8]> for GuardedMmap {
    fn as_ref(&self) -> &[u8] {
        self.mmap.as_ref()
    }
}

/// Actor implementation type for serialized filesystem object mutations.
#[derive(Debug, Default, Clone, Copy)]
pub(super) struct FileObjectActor;

/// Filesystem-path derivation and mutation helpers for object actor state.
impl FileObjectActorState {
    /// Returns canonical full-object path for `hash`.
    fn object_path_for_hash(&self, hash: Hash) -> PathBuf {
        object_path(&self.root, hash)
    }

    /// Returns canonical delta-object (`.diff`) path for `hash`.
    fn diff_path_for_hash(&self, hash: Hash) -> PathBuf {
        diff_object_path(&self.root, hash)
    }

    /// Returns shared staging-temp root used by atomic write flows.
    fn staging_tmp_root(&self) -> PathBuf {
        self.root.join(STORAGE_VERSION).join("tmp")
    }

    /// Returns file length when path exists, otherwise zero.
    async fn file_len_if_exists(
        path: &Path,
        check_operation: &str,
        metadata_operation: &str,
    ) -> Result<u64, CasError> {
        if !fs::try_exists(path)
            .await
            .map_err(|source| CasError::io(check_operation, path, source))?
        {
            return Ok(0);
        }

        let metadata = fs::metadata(path)
            .await
            .map_err(|source| CasError::io(metadata_operation, path, source))?;
        Ok(metadata.len())
    }

    /// Scans on-disk object tree and computes total payload bytes.
    async fn scan_cas_store_size_bytes(&self) -> Result<u64, CasError> {
        let mut total = 0u64;
        let root = self.root.join(STORAGE_VERSION);
        if !fs::try_exists(&root)
            .await
            .map_err(|source| CasError::io("checking cas store root", root.clone(), source))?
        {
            return Ok(0);
        }

        let mut stack = vec![root];
        while let Some(dir) = stack.pop() {
            let mut entries = fs::read_dir(&dir).await.map_err(|source| {
                CasError::io("reading cas store directory", dir.clone(), source)
            })?;
            while let Some(entry) = entries.next_entry().await.map_err(|source| {
                CasError::io("iterating cas store directory", dir.clone(), source)
            })? {
                let path = entry.path();
                let metadata = entry.metadata().await.map_err(|source| {
                    CasError::io("reading cas entry metadata", path.clone(), source)
                })?;
                if metadata.is_dir() {
                    if path.file_name().map(|name| name == "tmp").unwrap_or(false) {
                        continue;
                    }
                    stack.push(path);
                } else if metadata.is_file() {
                    total = total.saturating_add(metadata.len());
                }
            }
        }

        Ok(total)
    }

    /// Waits until no active mmap lease remains for `hash`.
    async fn wait_for_no_active_mmap(&self, hash: Hash) {
        loop {
            let wait = {
                let state = self.active_mmaps.state.lock();
                state.get(&hash).and_then(|entry| {
                    if entry.active == 0 { None } else { Some(entry.released.clone()) }
                })
            };

            let Some(notify) = wait else {
                return;
            };

            notify.notified().await;
        }
    }

    /// Removes one file path when present.
    async fn remove_file_if_exists(
        path: &Path,
        check_operation: &str,
        remove_operation: &str,
    ) -> Result<(), CasError> {
        if fs::try_exists(path)
            .await
            .map_err(|source| CasError::io(check_operation, path, source))?
        {
            fs::remove_file(path)
                .await
                .map_err(|source| CasError::io(remove_operation, path, source))?;
        }

        Ok(())
    }

    /// Persists full/delta object variant and updates tracked byte counters.
    async fn persist_object_variant(
        &mut self,
        hash: Hash,
        object: &StoredObject,
    ) -> Result<(), CasError> {
        self.wait_for_no_active_mmap(hash).await;

        let full_path = self.object_path_for_hash(hash);
        let diff_path = self.diff_path_for_hash(hash);

        let existing_full_len = Self::file_len_if_exists(
            &full_path,
            "checking stale full object size",
            "reading stale full object metadata",
        )
        .await?;
        let existing_diff_len = Self::file_len_if_exists(
            &diff_path,
            "checking stale diff object size",
            "reading stale diff object metadata",
        )
        .await?;
        let previous_total = existing_full_len.saturating_add(existing_diff_len);

        let next_total = object.payload_len();

        match object {
            StoredObject::Full { .. } => {
                let full_bytes = object.encode()?;
                let staging_root = self.staging_tmp_root();
                write_object_atomic(&staging_root, &full_path, full_bytes.as_ref()).await?;
                Self::remove_file_if_exists(
                    &diff_path,
                    "checking stale diff existence",
                    "removing stale diff",
                )
                .await?;
            }
            StoredObject::Delta { .. } => {
                let diff_bytes = object.encode()?;
                let staging_root = self.staging_tmp_root();
                write_object_atomic(&staging_root, &diff_path, diff_bytes.as_ref()).await?;
                Self::remove_file_if_exists(
                    &full_path,
                    "checking stale full object existence",
                    "removing stale full object",
                )
                .await?;
            }
        }

        self.total_store_size = self.total_store_size.saturating_sub(previous_total);
        self.total_store_size = self.total_store_size.saturating_add(next_total);

        Ok(())
    }

    /// Deletes both full/delta file variants for `hash` and updates counters.
    async fn delete_object_files(&mut self, hash: Hash) -> Result<(), CasError> {
        self.wait_for_no_active_mmap(hash).await;

        let full_path = self.object_path_for_hash(hash);
        let diff_path = self.diff_path_for_hash(hash);

        let existing_full_len = Self::file_len_if_exists(
            &full_path,
            "checking full object deletion target size",
            "reading full object deletion target metadata",
        )
        .await?;
        let existing_diff_len = Self::file_len_if_exists(
            &diff_path,
            "checking diff object deletion target size",
            "reading diff object deletion target metadata",
        )
        .await?;

        Self::remove_file_if_exists(
            &full_path,
            "checking full object deletion target",
            "deleting full object",
        )
        .await?;
        Self::remove_file_if_exists(
            &diff_path,
            "checking diff object deletion target",
            "deleting diff object",
        )
        .await?;

        self.total_store_size = self.total_store_size.saturating_sub(existing_full_len);
        self.total_store_size = self.total_store_size.saturating_sub(existing_diff_len);

        Ok(())
    }

    /// Returns cached total CAS store bytes tracked by actor state.
    async fn cas_store_size_bytes(&self) -> Result<u64, CasError> {
        Ok(self.total_store_size)
    }
}

/// Ractor implementation for serialized filesystem object mutations.
impl Actor for FileObjectActor {
    type Msg = FileObjectActorMessage;
    type State = FileObjectActorState;
    type Arguments = FileObjectActorState;

    async fn pre_start(
        &self,
        _myself: ActorRef<Self::Msg>,
        mut args: Self::Arguments,
    ) -> Result<Self::State, ActorProcessingErr> {
        args.total_store_size = args
            .scan_cas_store_size_bytes()
            .await
            .map_err(|err| ActorProcessingErr::from(err.to_string()))?;
        Ok(args)
    }

    async fn handle(
        &self,
        _myself: ActorRef<Self::Msg>,
        message: Self::Msg,
        state: &mut Self::State,
    ) -> Result<(), ActorProcessingErr> {
        let started = Instant::now();
        let operation = match &message {
            FileObjectActorMessage::PersistObjectVariant(_, _, _) => "persist_object_variant",
            FileObjectActorMessage::DeleteObjectFiles(_, _) => "delete_object_files",
            FileObjectActorMessage::CasStoreSizeBytes(_) => "cas_store_size_bytes",
        };

        match message {
            FileObjectActorMessage::PersistObjectVariant(hash, object, reply) => {
                let _ = reply.send(state.persist_object_variant(hash, &object).await);
            }
            FileObjectActorMessage::DeleteObjectFiles(hash, reply) => {
                let _ = reply.send(state.delete_object_files(hash).await);
            }
            FileObjectActorMessage::CasStoreSizeBytes(reply) => {
                let _ = reply.send(state.cas_store_size_bytes().await);
            }
        }

        let processing_ms = started.elapsed().as_millis().max(1) as u64;
        info!(
            operation,
            processing_ms,
            active_mmaps = state.active_mmaps.state.lock().len(),
            "file object actor processed message"
        );

        Ok(())
    }
}

/// Loads a full object payload, using memory mapping for larger files.
pub(super) async fn read_full_object_bytes_mmap(
    hash: Hash,
    path: PathBuf,
    active_mmaps: Arc<ActiveMmapRegistry>,
) -> Result<bytes::Bytes, CasError> {
    tokio::task::spawn_blocking(move || {
        let file = std::fs::File::open(&path)
            .map_err(|source| CasError::io("opening full object for mmap", &path, source))?;
        let metadata = file.metadata().map_err(|source| {
            CasError::io("reading full object metadata for mmap", &path, source)
        })?;

        if metadata.len() == 0 {
            return Ok(bytes::Bytes::new());
        }

        if metadata.len() < FILESYSTEM_MMAP_MIN_BYTES {
            let bytes = std::fs::read(&path).map_err(|source| {
                CasError::io("reading full object without mmap", &path, source)
            })?;
            return Ok(bytes::Bytes::from(bytes));
        }

        let lease = ActiveMmapLease::acquire(active_mmaps, hash);

        // SAFETY: The file descriptor remains alive for the lifetime of the mmap
        // object, and we return a Bytes owner that holds the mapping alive.
        let mmap = unsafe {
            MmapOptions::new()
                .map(&file)
                .map_err(|source| CasError::io("memory-mapping full object", &path, source))?
        };
        Ok(bytes::Bytes::from_owner(GuardedMmap { mmap, _lease: lease }))
    })
    .await
    .map_err(|err| CasError::task_join("reading full object bytes via mmap", err))?
}
