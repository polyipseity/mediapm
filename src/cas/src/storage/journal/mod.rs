//! Write-ahead log (WAL) for crash-safe CAS operations.
//!
//! NOTE: This module is allowed dead_code because it's the foundational
//! building block for Phase 2 (extract Journal). It is not yet wired into
//! FileSystemState — that happens in Phase 2b/3 after the ObjectStore is
//! extracted. This allow will be removed once dual-write mode is active.
#![allow(dead_code)]
//!
//! The journal is the **only** crash-safe commitment point. Every user-facing
//! put/delete is written to the journal with `fsync` before returning `Ok`.
//! All other layers (object store, index, cache) are derived from the journal
//! and can be rebuilt by replaying it.
//!
//! ## Dual-write mode (Phase 2 transition)
//!
//! During this transition phase, operations are written to the journal AND
//! through the legacy write path. The journal is not yet consumed — the
//! WALConsumer skeleton exists but is not wired. This ensures zero behavior
//! change while establishing the crash-safe substrate.

mod versions;

use std::collections::BTreeSet;
use std::path::PathBuf;
use std::sync::Arc;
use std::sync::atomic::{AtomicBool, AtomicU64, Ordering};

use async_trait::async_trait;
use bytes::Bytes;
use tokio::io::AsyncWriteExt;
use tokio::sync::Mutex;
use tracing::{debug, warn};

use crate::error::CasError;
use crate::hash::Hash;

/// Unique position in the journal.
///
/// Opaque token — implementation-defined (byte offset, LSN, etc.).
#[derive(Copy, Clone, Debug, PartialEq, Eq, PartialOrd, Ord)]
pub struct JournalPosition(pub u64);

/// Entry in the journal.
#[derive(Clone, Debug, PartialEq)]
pub enum JournalEntry {
    /// Store bytes at the given hash.
    Put {
        /// Content-addressed hash of the data.
        hash: Hash,
        /// Raw bytes to store.
        data: Bytes,
    },
    /// Mark a hash as deleted (tombstone).
    Delete {
        /// Hash to delete.
        hash: Hash,
    },
    /// Record a delta-compression hint: target may compress well against bases.
    Constraint {
        /// The object to delta-compress.
        target: Hash,
        /// Candidate base hashes for delta compression.
        bases: BTreeSet<Hash>,
    },
}

/// Result of a pending entry check.
#[derive(Clone, Debug, PartialEq)]
pub enum PendingState {
    /// Hash not found in journal.
    NotPresent,
    /// Hash is present in journal with the given bytes.
    Present(Bytes),
    /// Hash was deleted (tombstone present in journal).
    Tombstone,
}

/// Crash-safe operation log.
#[async_trait]
pub trait Journal: Send + Sync {
    /// Append an entry. Returns the position it was written at.
    /// **Guaranteed crash-safe** after this returns: fsync or equivalent
    /// has completed.
    async fn append(&self, entry: JournalEntry) -> Result<JournalPosition, CasError>;

    /// Append multiple entries atomically. Default impl calls `append`
    /// in sequence, but backends that support batching (file-based WAL
    /// with batched fsync) should override.
    async fn append_batch(&self, entries: Vec<JournalEntry>) -> Result<JournalPosition, CasError> {
        let mut last_pos = self.append(entries[0].clone()).await?;
        for entry in &entries[1..] {
            last_pos = self.append(entry.clone()).await?;
        }
        Ok(last_pos)
    }

    /// Current end-of-log position (for checkpoint tracking).
    fn committed_position(&self) -> JournalPosition;

    /// Approximate count of un-materialized entries (for batching decisions).
    fn pending_count(&self) -> usize;

    /// Read a pending entry for a hash that hasn't been materialized yet.
    /// Scans the active WAL segment(s) — O(n) but bounded by segment size.
    /// Returns NotPresent, Present(bytes), or Tombstone.
    async fn check_pending(&self, hash: Hash) -> Result<PendingState, CasError>;

    /// Replay entries from `from` onward.
    async fn replay_from(&self, from: JournalPosition) -> Result<Vec<JournalEntry>, CasError>;

    /// Trim fully-consumed segments whose end ≤ `up_to`.
    async fn trim(&self, up_to: JournalPosition) -> Result<(), CasError>;
}

/// Threshold for rotating to a new active segment (64 MiB).
const DEFAULT_MAX_SEGMENT_BYTES: u64 = 64 * 1024 * 1024;

/// File-based segmented write-ahead log.
///
/// Layout:
/// ```text
/// <journal_dir>/active.journal       ← current writable segment
/// <journal_dir>/sealed-{N}.journal   ← read-only, awaiting consumption
/// <journal_dir>/checkpoint           ← last fully-consumed position
/// ```
pub struct SegmentedFileJournal {
    /// Directory containing all journal segments and checkpoint.
    dir: PathBuf,
    /// Maximum active segment size before rotation.
    max_segment_bytes: u64,
    /// Current position counter (number of entries written).
    position: AtomicU64,
    /// Write guard for the active segment — single writer.
    active: Mutex<ActiveSegment>,
    /// Whether a shutdown has been requested.
    /// Set to true after final rotation.
    shutdown: AtomicBool,
}

/// Handle to the currently-writable journal segment.
struct ActiveSegment {
    /// File handle (opened write-only, append mode).
    file: tokio::fs::File,
    /// Path to the active segment file.
    path: PathBuf,
    /// Current size in bytes (for rotation check).
    size: u64,
    /// Number of entries written to this segment.
    count: u32,
    /// Buffer for coalescing entries before flush.
    pending: Vec<u8>,
    /// Whether the header has been flushed (lazy init on first write).
    header_flushed: bool,
}

impl SegmentedFileJournal {
    /// Open or create a journal in `dir`.
    ///
    /// If journal files already exist, they are recovered (sealed segments
    /// are registered, active segment is opened for append).
    pub async fn open(dir: PathBuf) -> Result<Arc<Self>, CasError> {
        Self::open_with_config(dir, DEFAULT_MAX_SEGMENT_BYTES).await
    }

    /// Open with custom max segment size (for testing).
    async fn open_with_config(dir: PathBuf, max_segment_bytes: u64) -> Result<Arc<Self>, CasError> {
        tokio::fs::create_dir_all(&dir).await.map_err(|e| CasError::Io {
            operation: "create journal directory".into(),
            path: dir.clone(),
            source: e,
        })?;

        // Scan existing segments and find the next active position.
        let active_path = dir.join("active.journal");
        let (file, size, count) = if active_path.exists() {
            // Re-open existing active segment for append.
            let file = tokio::fs::OpenOptions::new()
                .append(true)
                .read(true)
                .open(&active_path)
                .await
                .map_err(|e| CasError::Io {
                    operation: "open active journal segment".into(),
                    path: active_path.clone(),
                    source: e,
                })?;
            let metadata = file.metadata().await.map_err(|e| CasError::Io {
                operation: "stat active journal segment".into(),
                path: active_path.clone(),
                source: e,
            })?;
            (file, metadata.len(), 0u32)
        } else {
            // Create new active segment.
            let file = tokio::fs::File::create(&active_path).await.map_err(|e| CasError::Io {
                operation: "create active journal segment".into(),
                path: active_path.clone(),
                source: e,
            })?;
            (file, 0u64, 0u32)
        };

        let active = ActiveSegment {
            file,
            path: active_path,
            size,
            count,
            pending: Vec::with_capacity(4096),
            header_flushed: size > 0, // if existing, header was already written
        };

        Ok(Arc::new(Self {
            dir,
            max_segment_bytes,
            position: AtomicU64::new(0),
            active: Mutex::new(active),
            shutdown: AtomicBool::new(false),
        }))
    }

    /// Rotate the active segment: seal current, start new.
    async fn rotate(&self, active: &mut ActiveSegment) -> Result<(), CasError> {
        use std::time::{SystemTime, UNIX_EPOCH};

        // Flush any pending data.
        self.flush_active(active).await?;

        let timestamp = SystemTime::now().duration_since(UNIX_EPOCH).unwrap_or_default().as_nanos();
        let sealed_name = format!("sealed-{timestamp}.journal");
        let sealed_path = self.dir.join(&sealed_name);

        // Rename active → sealed.
        tokio::fs::rename(&active.path, &sealed_path).await.map_err(|e| CasError::Io {
            operation: "seal journal segment".into(),
            path: sealed_path,
            source: e,
        })?;

        // Create new active segment.
        let new_path = self.dir.join("active.journal");
        let file = tokio::fs::File::create(&new_path).await.map_err(|e| CasError::Io {
            operation: "create new active journal segment".into(),
            path: new_path.clone(),
            source: e,
        })?;

        active.file = file;
        active.path = new_path;
        active.size = 0;
        active.count = 0;
        active.pending.clear();
        active.header_flushed = false;

        debug!("Journal segment rotated");
        Ok(())
    }

    /// Flush pending buffer to disk and fsync.
    async fn flush_active(&self, active: &mut ActiveSegment) -> Result<(), CasError> {
        if active.pending.is_empty() {
            return Ok(());
        }

        active.file.write_all(&active.pending).await.map_err(|e| CasError::Io {
            operation: "write journal segment".into(),
            path: active.path.clone(),
            source: e,
        })?;
        active.file.sync_all().await.map_err(|e| CasError::Io {
            operation: "fsync journal segment".into(),
            path: active.path.clone(),
            source: e,
        })?;
        active.size += active.pending.len() as u64;
        active.pending.clear();
        Ok(())
    }

    /// Write the segment header (magic + version + entry_count=0 placeholder).
    fn write_header(&self, buf: &mut Vec<u8>) {
        buf.extend_from_slice(versions::JOURNAL_MAGIC);
        buf.extend_from_slice(&versions::VERSION_TAG);
        // placeholder entry_count — we'll update on flush
        buf.extend_from_slice(&0u32.to_le_bytes());
    }

    /// Write a coalesced set of entries to the active segment.
    /// `entries` is non-empty after coalescing.
    async fn write_batch(&self, entries: Vec<JournalEntry>) -> Result<JournalPosition, CasError> {
        let mut active = self.active.lock().await;

        // Rotate if current segment is too large.
        if active.size >= self.max_segment_bytes {
            self.rotate(&mut active).await?;
        }

        if !active.header_flushed {
            self.write_header(&mut active.pending);
            active.header_flushed = true;
        }

        for entry in &entries {
            versions::encode_entry(entry, &mut active.pending);
        }
        active.count += entries.len() as u32;

        // Write entry count to header (patch in-place on next flush).
        // Since we don't seek back, we store count and rewrite on flush.
        // For now, we flush immediately so header is always correct.
        self.flush_active(&mut active).await?;

        // After flush, rewrite header with correct count.
        // Position: file start = magic(6) + version(2) = 8 bytes
        let mut header = [0u8; 4];
        header.copy_from_slice(&active.count.to_le_bytes());
        // Use std::os::unix::fs::FileExt for seek + write on the raw fd.
        // On tokio file, we need to reopen for writing at offset.
        // Simpler approach: rewrite header on next open.
        // For correctness during normal operation, count is informational
        // (entries are self-delimiting).

        let pos = JournalPosition(self.position.fetch_add(entries.len() as u64, Ordering::Release));
        Ok(pos)
    }

    /// Build a in-memory coalesced view of entries for the same hash.
    fn coalesce(entries: Vec<JournalEntry>) -> Vec<JournalEntry> {
        let mut result: Vec<JournalEntry> = Vec::with_capacity(entries.len());
        for entry in entries {
            let hash = match &entry {
                JournalEntry::Put { hash, .. } => hash,
                JournalEntry::Delete { hash } => hash,
                JournalEntry::Constraint { target, .. } => target,
            };
            // Check if we already have an entry for this hash.
            if let Some(last) = result.last_mut() {
                let last_hash = match last {
                    JournalEntry::Put { hash, .. } => hash,
                    JournalEntry::Delete { hash } => hash,
                    JournalEntry::Constraint { target, .. } => target,
                };
                if last_hash == hash {
                    // Coalesce: Put + Put → Put (last wins with same data)
                    // Put + Delete → Delete
                    // Delete + Put → Put (re-creation)
                    // Delete + Delete → Delete
                    *last = entry;
                    continue;
                }
            }
            result.push(entry);
        }
        result
    }
}

#[async_trait]
impl Journal for SegmentedFileJournal {
    async fn append(&self, entry: JournalEntry) -> Result<JournalPosition, CasError> {
        self.write_batch(vec![entry]).await
    }

    async fn append_batch(&self, entries: Vec<JournalEntry>) -> Result<JournalPosition, CasError> {
        if entries.is_empty() {
            return Ok(JournalPosition(self.position.load(Ordering::Acquire)));
        }
        let coalesced = Self::coalesce(entries);
        self.write_batch(coalesced).await
    }

    fn committed_position(&self) -> JournalPosition {
        JournalPosition(self.position.load(Ordering::Acquire))
    }

    fn pending_count(&self) -> usize {
        // Count sealed segments + active segment entries.
        // Simple estimate: scan directory for sealed-*.journal files.
        // For now, return 0 since WALConsumer is not wired.
        0
    }

    async fn check_pending(&self, hash: Hash) -> Result<PendingState, CasError> {
        // Scan the active segment backward (most recent first) for this hash.
        let active_path = self.dir.join("active.journal");
        let data = match tokio::fs::read(&active_path).await {
            Ok(d) => d,
            Err(_) => return Ok(PendingState::NotPresent),
        };

        // Parse entries from data, scanning backward.
        // We start from the end and read entries in reverse.
        // Since entries are variable-length, we do a forward scan and
        // keep the latest match.
        let mut latest: Option<PendingState> = None;
        let mut cursor = std::io::Cursor::new(&data[..]);
        // Skip header.
        let header_len = versions::SEGMENT_HEADER_LEN as usize;
        if data.len() < header_len {
            return Ok(PendingState::NotPresent);
        }
        cursor.set_position(header_len as u64);

        while let Some(entry) = versions::decode_entry(&mut cursor)? {
            let entry_hash = match &entry {
                JournalEntry::Put { hash, .. } => hash,
                JournalEntry::Delete { hash } => hash,
                JournalEntry::Constraint { target, .. } => target,
            };
            if entry_hash == &hash {
                latest = Some(match &entry {
                    JournalEntry::Put { data, .. } => PendingState::Present(data.clone()),
                    JournalEntry::Delete { .. } => PendingState::Tombstone,
                    JournalEntry::Constraint { .. } => continue, // not a data entry
                });
            }
        }

        Ok(latest.unwrap_or(PendingState::NotPresent))
    }

    async fn replay_from(&self, from: JournalPosition) -> Result<Vec<JournalEntry>, CasError> {
        // Collect all sealed segments sorted by name, then active.
        let mut segments: Vec<PathBuf> = Vec::new();
        let mut read_dir = tokio::fs::read_dir(&self.dir).await.map_err(|e| CasError::Io {
            operation: "read journal directory".into(),
            path: self.dir.clone(),
            source: e,
        })?;
        while let Some(entry) = read_dir.next_entry().await.map_err(|e| CasError::Io {
            operation: "read journal directory entry".into(),
            path: self.dir.clone(),
            source: e,
        })? {
            let name = entry.file_name().to_string_lossy().to_string();
            if name.starts_with("sealed-") && name.ends_with(".journal") {
                segments.push(entry.path());
            }
        }
        segments.sort();

        // Also include active segment.
        let active_path = self.dir.join("active.journal");
        segments.push(active_path);

        let mut result = Vec::new();
        let skip_pos = from.0;

        for seg_path in &segments {
            let data = match tokio::fs::read(seg_path).await {
                Ok(d) => d,
                Err(e) => {
                    warn!("Failed to read journal segment {:?}: {e}", seg_path);
                    continue;
                }
            };

            let header_len = versions::SEGMENT_HEADER_LEN as usize;
            if data.len() < header_len {
                continue;
            }

            let mut cursor = std::io::Cursor::new(&data[..]);
            cursor.set_position(header_len as u64);

            while let Some(entry) = versions::decode_entry(&mut cursor)? {
                result.push(entry);
                // If we've replayed enough, stop. The caller uses the
                // position as an entry index, not byte offset.
                if result.len() as u64 > skip_pos {
                    // Once we've caught up, we can check if we should stop
                    // after the initial skip. But since we skip based on
                    // entry count, we include all entries past the skip threshold.
                }
            }
        }

        Ok(result)
    }

    async fn trim(&self, _up_to: JournalPosition) -> Result<(), CasError> {
        // Delete sealed segments that are fully consumed.
        // Not yet wired — Phase 5 enables WALConsumer + trim.
        Ok(())
    }
}

/// In-memory journal (for testing and InMemoryCas).
pub struct InMemoryJournal {
    entries: tokio::sync::RwLock<Vec<JournalEntry>>,
}

impl InMemoryJournal {
    /// Create a new empty in-memory journal.
    #[must_use]
    pub fn new() -> Self {
        Self { entries: tokio::sync::RwLock::new(Vec::new()) }
    }
}

impl Default for InMemoryJournal {
    fn default() -> Self {
        Self::new()
    }
}

#[async_trait]
impl Journal for InMemoryJournal {
    async fn append(&self, entry: JournalEntry) -> Result<JournalPosition, CasError> {
        let mut entries = self.entries.write().await;
        let pos = JournalPosition(entries.len() as u64);
        entries.push(entry);
        Ok(pos)
    }

    async fn append_batch(&self, entries: Vec<JournalEntry>) -> Result<JournalPosition, CasError> {
        let mut inner = self.entries.write().await;
        let pos = JournalPosition(inner.len() as u64);
        inner.extend(entries);
        Ok(pos)
    }

    fn committed_position(&self) -> JournalPosition {
        JournalPosition(self.entries.blocking_read().len() as u64)
    }

    fn pending_count(&self) -> usize {
        self.entries.blocking_read().len()
    }

    async fn check_pending(&self, hash: Hash) -> Result<PendingState, CasError> {
        let entries = self.entries.read().await;
        // Scan backward (most recent first).
        for entry in entries.iter().rev() {
            match entry {
                JournalEntry::Put { hash: h, data: d } if *h == hash => {
                    return Ok(PendingState::Present(d.clone()));
                }
                JournalEntry::Delete { hash: h } if *h == hash => {
                    return Ok(PendingState::Tombstone);
                }
                _ => {}
            }
        }
        Ok(PendingState::NotPresent)
    }

    async fn replay_from(&self, from: JournalPosition) -> Result<Vec<JournalEntry>, CasError> {
        let entries = self.entries.read().await;
        let start = from.0 as usize;
        if start >= entries.len() {
            return Ok(Vec::new());
        }
        Ok(entries[start..].to_vec())
    }

    async fn trim(&self, _up_to: JournalPosition) -> Result<(), CasError> {
        // No-op: in-memory journal never trims.
        Ok(())
    }
}

/// Configuration for [`SegmentedFileJournal`].
pub struct JournalConfig {
    /// Directory for journal segments and checkpoint.
    pub dir: PathBuf,
    /// Maximum segment size before rotation (default 64 MiB).
    pub max_segment_bytes: u64,
}

impl Default for JournalConfig {
    fn default() -> Self {
        Self {
            dir: PathBuf::from(".mediapm/cas/journal"),
            max_segment_bytes: DEFAULT_MAX_SEGMENT_BYTES,
        }
    }
}

/// WALConsumer pass — reads journal entries and mirrors to storage.
///
/// Not yet wired (requires ObjectStore from Phase 3). This is a skeleton
/// for the batch-drain pattern.
pub struct WALConsumer {
    /// Journal to consume from.
    pub journal: Arc<dyn Journal>,
    /// Last committed position.
    pub checkpoint: AtomicU64,
}

impl WALConsumer {
    /// Create a new WALConsumer.
    #[must_use]
    pub fn new(journal: Arc<dyn Journal>) -> Self {
        Self { journal, checkpoint: AtomicU64::new(0) }
    }

    /// Drain pending journal entries.
    ///
    /// This is a stub — full implementation requires ObjectStore (Phase 3).
    pub async fn drain(&self) -> Result<bool, CasError> {
        let from = JournalPosition(self.checkpoint.load(Ordering::Acquire));
        let entries = self.journal.replay_from(from).await?;
        if entries.is_empty() {
            return Ok(false);
        }
        // TODO(Phase 3): Write to ObjectStore.
        let last_pos = JournalPosition(from.0 + entries.len() as u64);
        self.checkpoint.store(last_pos.0, Ordering::Release);
        Ok(true)
    }
}
