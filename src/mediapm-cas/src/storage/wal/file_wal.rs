//! File-based journal and checkpoint implementation.
//!
//! ## File locations
//!
//! - `<cas_dir>/checkpoint` — checkpoint file (last consumed position)
//! - `<cas_dir>/journal/active.seg` — active segment (append-only)
//! - `<cas_dir>/journal/<first:020x>-<last:020x>.seg` — sealed segments
//!
//! ## Segment lifecycle
//!
//! 1. Active segment is created on first write. Entries are appended.
//! 2. When active exceeds `max_segment_size`, it is **sealed**: the file
//!    is renamed to `<first_pos>-<last_pos>.seg` and a new active is
//!    created.
//! 3. On `trim(up_to)`, sealed segments whose `last_pos ≤ up_to` are
//!    physically deleted.
//!
//! ## Startup recovery
//!
//! On creation, the journal:
//! 1. Reads the checkpoint file (or starts at ZERO if absent).
//! 2. Scans `<cas_dir>/journal/` for all segment files.
//! 3. Reads each segment to determine position ranges and rebuild the
//!    in-memory pending state for all entries from the checkpoint forward.
//! 4. Resumes appending to the active segment (or creates one).

use std::collections::HashMap;
use std::path::PathBuf;
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::{Arc, Mutex};

use async_trait::async_trait;
use tokio::io::{AsyncReadExt, AsyncWriteExt};

use crate::error::CasError;
use crate::hash::Hash;

use super::versions as format;
use super::{PendingState, Wal, WalEntry, WalPosition};

// ---------------------------------------------------------------------------
// Checkpoint
// ---------------------------------------------------------------------------

/// Manages the checkpoint file.
struct Checkpoint {
    path: PathBuf,
    /// Cached last-consumed position (also persisted to disk).
    last_pos: AtomicU64,
}

impl Checkpoint {
    /// Load checkpoint from disk, or start at ZERO.
    async fn load(path: PathBuf) -> Result<Self, CasError> {
        let last_pos = match tokio::fs::read(&path).await {
            Ok(buf) => format::decode_checkpoint(&buf)?.as_u64(),
            Err(e) if e.kind() == std::io::ErrorKind::NotFound => 0,
            Err(e) => return Err(CasError::Io(e)),
        };
        Ok(Self { path, last_pos: AtomicU64::new(last_pos) })
    }

    /// Return the last consumed position.
    fn position(&self) -> WalPosition {
        WalPosition::from_u64(self.last_pos.load(Ordering::SeqCst))
    }

    /// Persist a new position atomically.
    async fn persist(&self, pos: WalPosition) -> Result<(), CasError> {
        let data = format::encode_checkpoint(pos);
        // Atomic write: write to .tmp, then rename.
        let tmp_path = self.path.with_extension("tmp");
        tokio::fs::write(&tmp_path, &data).await.map_err(CasError::Io)?;
        tokio::fs::rename(&tmp_path, &self.path).await.map_err(CasError::Io)?;
        self.last_pos.store(pos.as_u64(), Ordering::SeqCst);
        Ok(())
    }
}

// ---------------------------------------------------------------------------
// Sealed segment metadata
// ---------------------------------------------------------------------------

#[derive(Debug, Clone)]
struct SealedSegment {
    first_pos: WalPosition,
    last_pos: WalPosition,
    path: PathBuf,
}

/// Parse the position range from a sealed segment filename.
///
/// Expected format: `<first:020x>-<last:020x>.seg`
fn parse_sealed_filename(name: &str) -> Option<(WalPosition, WalPosition)> {
    let name = name.strip_suffix(".seg")?;
    let (first_hex, last_hex) = name.split_once('-')?;
    let first = u64::from_str_radix(first_hex, 16).ok()?;
    let last = u64::from_str_radix(last_hex, 16).ok()?;
    Some((WalPosition::from_u64(first), WalPosition::from_u64(last)))
}

/// Format a sealed segment filename.
fn sealed_filename(first: WalPosition, last: WalPosition) -> String {
    format!("{:020x}-{:020x}.seg", first.as_u64(), last.as_u64())
}

// ---------------------------------------------------------------------------
// Active segment
// ---------------------------------------------------------------------------

/// Writable segment for new journal entries.
struct ActiveSegment {
    file: tokio::fs::File,
    first_pos: WalPosition,
    bytes_written: u64,
}

impl ActiveSegment {
    /// Create a new active segment file, writing the header.
    async fn create(path: &PathBuf) -> Result<Self, CasError> {
        let mut file = tokio::fs::OpenOptions::new()
            .write(true)
            .create(true)
            .append(true)
            .open(path)
            .await
            .map_err(CasError::Io)?;

        // Write header (8 bytes: magic + version).
        let header = format::encode_header(format::JOURNAL_MAGIC, format::JOURNAL_VERSION);
        file.write_all(&header).await.map_err(CasError::Io)?;
        file.sync_data().await.map_err(CasError::Io)?;

        Ok(Self { file, first_pos: WalPosition::ZERO, bytes_written: 0 })
    }

    /// Open an existing active segment file for appending.
    /// Determines its position range by reading existing entries.
    async fn open(path: &PathBuf) -> Result<Self, CasError> {
        let file = tokio::fs::OpenOptions::new()
            .write(true)
            .append(true)
            .read(true)
            .open(path)
            .await
            .map_err(CasError::Io)?;

        // Read existing content to determine position range.
        let mut file_for_read = file.try_clone().await.map_err(CasError::Io)?;
        let mut buf = Vec::new();
        file_for_read.read_to_end(&mut buf).await.map_err(CasError::Io)?;

        // Skip header (8 bytes) and parse entries.
        let first_pos = if buf.len() > format::HEADER_LEN {
            let (_, pos, _) = format::decode_entry(&buf[format::HEADER_LEN..])?;
            pos
        } else {
            WalPosition::ZERO
        };

        let bytes_written = (buf.len() - format::HEADER_LEN) as u64;

        Ok(Self { file, first_pos, bytes_written })
    }

    /// Return the path to the active segment file.
    fn path(journal_dir: &PathBuf) -> PathBuf {
        journal_dir.join("active.seg")
    }

    /// Write a single encoded entry to the file.
    async fn write_entry(&mut self, encoded: &[u8]) -> Result<(), CasError> {
        self.file.write_all(encoded).await.map_err(CasError::Io)?;
        self.bytes_written += encoded.len() as u64;
        Ok(())
    }

    /// Flush and fsync.
    async fn flush(&mut self) -> Result<(), CasError> {
        self.file.flush().await.map_err(CasError::Io)?;
        self.file.sync_data().await.map_err(CasError::Io)?;
        Ok(())
    }

    /// Seal this segment by determining its last position, renaming the
    /// active file to a sealed filename, and returning the metadata.
    async fn seal(
        mut self,
        journal_dir: &PathBuf,
        last_pos: WalPosition,
    ) -> Result<SealedSegment, CasError> {
        self.flush().await?;
        drop(self.file); // Close the file.

        let src = ActiveSegment::path(journal_dir);
        let dst = journal_dir.join(sealed_filename(self.first_pos, last_pos));
        tokio::fs::rename(&src, &dst).await.map_err(CasError::Io)?;

        Ok(SealedSegment { first_pos: self.first_pos, last_pos, path: dst })
    }
}

// ---------------------------------------------------------------------------
// FileWal
// ---------------------------------------------------------------------------

/// A [`Wal`] implementation backed by files on disk.
///
/// ## Crash safety
///
/// - Every `append`/`append_batch` is written to the active segment file
///   with `sync_data`.
/// - The checkpoint is written atomically (`.tmp` + `rename`).
/// - On startup, the checkpoint determines the replay start position.
/// - Unknown segment or checkpoint versions are refused with an error.
pub struct FileWal {
    inner: Arc<FileWalInner>,
}

struct FileWalInner {
    /// CAS directory root.
    #[expect(dead_code, reason = "deferring: unused field, clarify with user")]
    dir: PathBuf,
    /// Journal segment directory: `<dir>/journal/`.
    journal_dir: PathBuf,
    /// Checkpoint manager.
    checkpoint: Checkpoint,
    /// Maximum segment size before sealing (in bytes).
    max_segment_size: u64,
    /// Serializes write operations to the active segment.
    /// Also protects sealed segment metadata and pending map mutations.
    write_lock: tokio::sync::Mutex<JournalWriterState>,
    /// Sealed segment metadata (read-only after creation).
    sealed: Mutex<Vec<SealedSegment>>,
    /// Next position to assign.
    next_pos: AtomicU64,
    /// In-memory pending state for fast `check_pending`.
    /// Maps hash → (position, state). Retains only entries > checkpoint.
    pending: Mutex<HashMap<Hash, (WalPosition, PendingState)>>,
}

struct JournalWriterState {
    active: Option<ActiveSegment>,
}

impl FileWal {
    /// Default max segment size: 64 MiB.
    pub const DEFAULT_MAX_SEGMENT_SIZE: u64 = 64 * 1024 * 1024;

    /// Create or open a file-based journal at `cas_dir`.
    ///
    /// If the directory does not exist, it is created. If journal files
    /// already exist, they are scanned and recovered.
    pub async fn create(cas_dir: PathBuf) -> Result<Self, CasError> {
        Self::create_with_max_size(cas_dir, Self::DEFAULT_MAX_SEGMENT_SIZE).await
    }

    /// Create or open a file-based journal with a custom max segment size.
    pub async fn create_with_max_size(
        cas_dir: PathBuf,
        max_segment_size: u64,
    ) -> Result<Self, CasError> {
        let journal_dir = cas_dir.join("journal");
        tokio::fs::create_dir_all(&journal_dir).await.map_err(CasError::Io)?;

        // Load checkpoint.
        let checkpoint_path = cas_dir.join("checkpoint");
        let checkpoint = Checkpoint::load(checkpoint_path).await?;
        let checkpoint_pos = checkpoint.position();

        // Scan and collect sealed segments, and the active segment.
        let mut sealed = Vec::new();
        let mut active_entries = Vec::new();
        let mut active_path = None;
        let mut max_position = 0u64;

        let mut read_dir = tokio::fs::read_dir(&journal_dir).await.map_err(CasError::Io)?;
        while let Some(entry) = read_dir.next_entry().await.map_err(CasError::Io)? {
            let path = entry.path();
            if !path.is_file() {
                continue;
            }
            let name = path.file_name().unwrap().to_string_lossy().to_string();

            if name == "active.seg" {
                active_path = Some(path.clone());
                // Read active segment entries.
                let (entries, pos_range) = Self::read_segment_entries(&path).await?;
                active_entries = entries;
                if let Some((_, last_pos)) = pos_range {
                    if last_pos.as_u64() > max_position {
                        max_position = last_pos.as_u64();
                    }
                }
            } else if let Some((first, last)) = parse_sealed_filename(&name) {
                sealed.push(SealedSegment { first_pos: first, last_pos: last, path });
                if last.as_u64() > max_position {
                    max_position = last.as_u64();
                }
            }
        }

        // Sort sealed segments by first_pos.
        sealed.sort_by_key(|s| s.first_pos);

        // Determine next position.
        let next_pos = max_position + 1;

        // Open or create the active segment.
        let active = if let Some(path) = active_path {
            ActiveSegment::open(&path).await?
        } else {
            ActiveSegment::create(&ActiveSegment::path(&journal_dir)).await?
        };

        // Build pending state from checkpoint forward.
        let mut pending = HashMap::new();
        let all_entries =
            Self::collect_entries_from_checkpoint(&sealed, &active_entries, &[], checkpoint_pos)?;
        for (pos, entry) in &all_entries {
            // Constraint entries don't affect check_pending (only Put/Delete).
            match entry {
                WalEntry::Put { hash, data } => {
                    pending.insert(*hash, (*pos, PendingState::Present(data.clone())));
                }
                WalEntry::Delete { hash } => {
                    pending.insert(*hash, (*pos, PendingState::Tombstone));
                }
                WalEntry::Constraint { .. } => {}
            }
        }

        Ok(Self {
            inner: Arc::new(FileWalInner {
                dir: cas_dir,
                journal_dir,
                checkpoint,
                max_segment_size,
                write_lock: tokio::sync::Mutex::new(JournalWriterState { active: Some(active) }),
                sealed: Mutex::new(sealed),
                next_pos: AtomicU64::new(next_pos),
                pending: Mutex::new(pending),
            }),
        })
    }

    /// Read all entries from a segment file and return them along with
    /// the first/last position range.
    async fn read_segment_entries(
        path: &PathBuf,
    ) -> Result<(Vec<(WalPosition, WalEntry)>, Option<(WalPosition, WalPosition)>), CasError> {
        let buf = tokio::fs::read(path).await.map_err(CasError::Io)?;
        if buf.len() < format::HEADER_LEN {
            return Err(CasError::corrupt_object(format!(
                "segment file too short: {}",
                path.display()
            )));
        }

        // Verify header.
        let mut header = [0u8; format::HEADER_LEN];
        header.copy_from_slice(&buf[..format::HEADER_LEN]);
        format::decode_header(&header, format::JOURNAL_MAGIC, format::MAX_JOURNAL_VERSION)?;

        let entries = format::decode_entries(&buf[format::HEADER_LEN..])?;
        let range = entries.first().zip(entries.last()).map(|((fp, _), (lp, _))| (*fp, *lp));
        Ok((entries, range))
    }

    /// Collect all entries from checkpoint position forward, reading
    /// from sealed segments, active entries list, and new entries.
    fn collect_entries_from_checkpoint(
        sealed: &[SealedSegment],
        active_entries: &[(WalPosition, WalEntry)],
        pre_pended: &[(WalPosition, WalEntry)],
        checkpoint_pos: WalPosition,
    ) -> Result<Vec<(WalPosition, WalEntry)>, CasError> {
        let mut all = Vec::new();

        // Collect from sealed segments whose range overlaps checkpoint_pos.
        for seg in sealed {
            if seg.last_pos < checkpoint_pos {
                continue; // Fully consumed, skip.
            }
            let buf = std::fs::read(&seg.path).map_err(CasError::Io)?;
            let entries = format::decode_entries(&buf[format::HEADER_LEN..])?;
            all.extend(entries.into_iter().filter(|(pos, _)| *pos > checkpoint_pos));
        }

        // Collect from active entries.
        all.extend(active_entries.iter().filter(|(pos, _)| *pos > checkpoint_pos).cloned());

        // Collect from pre-pended entries (for use after sealing).
        all.extend(pre_pended.iter().filter(|(pos, _)| *pos > checkpoint_pos).cloned());

        // Sort by position (entries from different segments may interleave
        // in theory, but in practice positions are monotonic across segments).
        all.sort_by_key(|(pos, _)| *pos);
        all.dedup_by_key(|(pos, _)| *pos);

        Ok(all)
    }

    /// Ensure the active segment is ready to accept writes.
    /// If it exceeds max_segment_size, seal it and create a new one.
    async fn maybe_seal(
        state: &mut JournalWriterState,
        inner: &FileWalInner,
    ) -> Result<(), CasError> {
        let Some(active) = &state.active else {
            return Ok(());
        };
        if active.bytes_written < inner.max_segment_size {
            return Ok(());
        }

        // Seal the active segment.
        // We need to know the last position written to the active segment.
        // Since we track next_pos atomically, the last position written
        // is next_pos - 1 at this point. But we must not race with other
        // writers — we hold the write lock, so this is safe.
        //
        // However, the active segment might be empty (no entries written).
        // In that case, don't seal.
        if active.first_pos == WalPosition::ZERO && active.bytes_written == 0 {
            return Ok(());
        }

        // Determine last pos from the active segment's entries.
        let act_path = ActiveSegment::path(&inner.journal_dir);
        let entries = Self::read_segment_entries(&act_path).await?;
        let last_pos = entries.0.last().map(|(pos, _)| *pos).unwrap_or(WalPosition::ZERO);

        // If no entries were written yet, nothing to seal.
        if last_pos == WalPosition::ZERO {
            return Ok(());
        }

        let active = state.active.take().unwrap();
        let sealed_seg = active.seal(&inner.journal_dir, last_pos).await?;

        // Add to sealed list.
        inner.sealed.lock().unwrap().push(sealed_seg);

        // Create new active segment.
        let new_active = ActiveSegment::create(&ActiveSegment::path(&inner.journal_dir)).await?;
        state.active = Some(new_active);

        Ok(())
    }
}

#[async_trait]
impl Wal for FileWal {
    async fn append(&self, entry: WalEntry) -> Result<WalPosition, CasError> {
        let inner = &*self.inner;
        let mut state = inner.write_lock.lock().await;

        // Check sealing before writing.
        Self::maybe_seal(&mut state, inner).await?;

        let pos = WalPosition::from_u64(inner.next_pos.fetch_add(1, Ordering::SeqCst));

        let encoded = format::encode_entry(&entry, pos);
        if let Some(active) = &mut state.active {
            // Record first_pos if this is the first entry.
            if active.first_pos == WalPosition::ZERO {
                // Re-read from file to find the actual first entry's position.
                // We'll set it from the entries in the active segment.
                let (existing_entries, _) =
                    Self::read_segment_entries(&ActiveSegment::path(&inner.journal_dir)).await?;
                if let Some((first, _)) = existing_entries.first() {
                    active.first_pos = *first;
                } else {
                    active.first_pos = pos;
                }
                drop(existing_entries); // release the Vec
            }
            active.write_entry(&encoded).await?;
            active.flush().await?;
        }

        // Update pending state.
        match &entry {
            WalEntry::Put { hash, data } => {
                inner
                    .pending
                    .lock()
                    .unwrap()
                    .insert(*hash, (pos, PendingState::Present(data.clone())));
            }
            WalEntry::Delete { hash } => {
                inner.pending.lock().unwrap().insert(*hash, (pos, PendingState::Tombstone));
            }
            WalEntry::Constraint { .. } => {}
        }

        Ok(pos)
    }

    async fn append_batch(&self, entries: Vec<WalEntry>) -> Result<(), CasError> {
        if entries.is_empty() {
            return Ok(());
        }

        let inner = &*self.inner;
        let mut state = inner.write_lock.lock().await;

        // Check sealing before batch.
        Self::maybe_seal(&mut state, inner).await?;

        if let Some(active) = &mut state.active {
            for entry in &entries {
                let pos = WalPosition::from_u64(inner.next_pos.fetch_add(1, Ordering::SeqCst));
                let encoded = format::encode_entry(entry, pos);

                if active.first_pos == WalPosition::ZERO {
                    // Find actual first pos from existing entries + current.
                    let (existing_entries, _) =
                        Self::read_segment_entries(&ActiveSegment::path(&inner.journal_dir))
                            .await?;
                    if let Some((first, _)) = existing_entries.first() {
                        active.first_pos = *first;
                    } else {
                        active.first_pos = pos;
                    }
                    drop(existing_entries);
                }

                active.write_entry(&encoded).await?;

                // Update pending state.
                match entry {
                    WalEntry::Put { hash, data } => {
                        inner
                            .pending
                            .lock()
                            .unwrap()
                            .insert(*hash, (pos, PendingState::Present(data.clone())));
                    }
                    WalEntry::Delete { hash } => {
                        inner.pending.lock().unwrap().insert(*hash, (pos, PendingState::Tombstone));
                    }
                    WalEntry::Constraint { .. } => {}
                }
            }
            active.flush().await?;
        }

        Ok(())
    }

    async fn committed_position(&self) -> WalPosition {
        let next = self.inner.next_pos.load(Ordering::SeqCst);
        if next == 0 { WalPosition::ZERO } else { WalPosition::from_u64(next - 1) }
    }

    async fn pending_count(&self) -> u64 {
        self.inner.pending.lock().unwrap().len() as u64
    }

    async fn check_pending(&self, hash: &Hash) -> PendingState {
        let map = self.inner.pending.lock().unwrap();
        map.get(hash).map(|(_, state)| state.clone()).unwrap_or(PendingState::NotPresent)
    }

    async fn replay_from(&self, pos: WalPosition) -> Vec<(WalPosition, WalEntry)> {
        let inner = &*self.inner;
        // Acquire write lock first (tokio::sync::Mutex) to prevent concurrent writes,
        // then sealed list (std::sync::Mutex). Order ensures no non-Send MutexGuard
        // is held across an await point.
        let _state = inner.write_lock.lock().await;
        let sealed = inner.sealed.lock().unwrap();

        let mut all = Vec::new();

        // Collect from sealed segments.
        for seg in sealed.iter() {
            if seg.last_pos < pos {
                continue;
            }
            let buf = match std::fs::read(&seg.path) {
                Ok(b) => b,
                Err(_) => continue,
            };
            if buf.len() < format::HEADER_LEN {
                continue;
            }
            match format::decode_entries(&buf[format::HEADER_LEN..]) {
                Ok(entries) => {
                    for (epos, entry) in entries {
                        if epos >= pos {
                            all.push((epos, entry));
                        }
                    }
                }
                Err(_) => continue,
            }
        }

        // Collect from active segment.
        let active_path = ActiveSegment::path(&inner.journal_dir);
        if let Ok(buf) = std::fs::read(&active_path) {
            if buf.len() >= format::HEADER_LEN {
                if let Ok(entries) = format::decode_entries(&buf[format::HEADER_LEN..]) {
                    for (epos, entry) in entries {
                        if epos >= pos {
                            all.push((epos, entry));
                        }
                    }
                }
            }
        }

        // Sort and dedup by position (ordering from different segments).
        all.sort_by_key(|(p, _)| *p);
        all.dedup_by_key(|(p, _)| *p);

        all
    }

    async fn trim(&self, up_to: WalPosition) -> Result<(), CasError> {
        let inner = &*self.inner;

        // Persist checkpoint first.
        inner.checkpoint.persist(up_to).await?;

        // Collect segments to delete, then drop the lock before I/O.
        let to_delete: Vec<PathBuf> = {
            let mut sealed = inner.sealed.lock().unwrap();
            let mut remaining = Vec::new();
            let mut delete = Vec::new();
            for seg in sealed.drain(..) {
                if seg.last_pos <= up_to {
                    delete.push(seg.path);
                } else {
                    remaining.push(seg);
                }
            }
            *sealed = remaining;
            delete
        };

        // Delete segment files without holding the lock.
        for path in &to_delete {
            let _ = tokio::fs::remove_file(path).await;
        }

        // Prune pending entries whose position ≤ up_to.
        inner.pending.lock().unwrap().retain(|_, (pos, _)| *pos > up_to);

        Ok(())
    }
}

impl Clone for FileWal {
    fn clone(&self) -> Self {
        Self { inner: self.inner.clone() }
    }
}

// ---------------------------------------------------------------------------
// Tests
// ---------------------------------------------------------------------------

#[cfg(test)]
mod tests {
    use super::*;
    use crate::hash::Hash;
    use bytes::Bytes;
    use tempfile::TempDir;

    async fn create_test_journal() -> (FileWal, TempDir) {
        let tmp = TempDir::new().unwrap();
        let journal =
            FileWal::create_with_max_size(tmp.path().to_path_buf(), 1024 * 1024).await.unwrap();
        (journal, tmp)
    }

    #[tokio::test]
    async fn put_and_get() {
        let (journal, _tmp) = create_test_journal().await;
        let data = Bytes::from_static(b"hello");
        let hash = Hash::from_content(&data);
        journal.append(WalEntry::Put { hash, data: data.clone() }).await.unwrap();

        match journal.check_pending(&hash).await {
            PendingState::Present(d) => assert_eq!(d, data),
            _ => panic!("expected Present"),
        }
    }

    #[tokio::test]
    async fn delete_and_tombstone() {
        let (journal, _tmp) = create_test_journal().await;
        let hash = Hash::from_content(b"gone");
        journal.append(WalEntry::Delete { hash }).await.unwrap();

        match journal.check_pending(&hash).await {
            PendingState::Tombstone => {}
            _ => panic!("expected Tombstone"),
        }
    }

    #[tokio::test]
    async fn replay_from_position() {
        let (journal, _tmp) = create_test_journal().await;
        let h1 = Hash::from_content(b"a");
        let h2 = Hash::from_content(b"b");
        journal.append(WalEntry::Put { hash: h1, data: Bytes::from_static(b"a") }).await.unwrap();
        let pos = journal
            .append(WalEntry::Put { hash: h2, data: Bytes::from_static(b"b") })
            .await
            .unwrap();

        let replayed = journal.replay_from(pos).await;
        assert_eq!(replayed.len(), 1);
        assert!(matches!(replayed[0].1, WalEntry::Put { hash, .. } if hash == h2));
    }

    #[tokio::test]
    async fn trim_removes_entries() {
        let (journal, _tmp) = create_test_journal().await;
        let hash = Hash::from_content(b"x");
        let pos =
            journal.append(WalEntry::Put { hash, data: Bytes::from_static(b"x") }).await.unwrap();
        journal.trim(pos).await.unwrap();
        assert_eq!(journal.pending_count().await, 0);
    }

    #[tokio::test]
    async fn trim_up_to_deletes_older_entries() {
        let (journal, _tmp) = create_test_journal().await;
        let h1 = Hash::from_content(b"1");
        let h2 = Hash::from_content(b"2");

        let pos1 = journal
            .append(WalEntry::Put { hash: h1, data: Bytes::from_static(b"1") })
            .await
            .unwrap();
        let _pos2 = journal
            .append(WalEntry::Put { hash: h2, data: Bytes::from_static(b"2") })
            .await
            .unwrap();

        // Trim up to pos1 — h2 should still be pending.
        journal.trim(pos1).await.unwrap();
        match journal.check_pending(&h1).await {
            PendingState::NotPresent => {}
            other => panic!("expected NotPresent, got {other:?}"),
        }
        match journal.check_pending(&h2).await {
            PendingState::Present(d) => assert_eq!(d, Bytes::from_static(b"2")),
            other => panic!("expected Present, got {other:?}"),
        }
    }

    #[tokio::test]
    async fn append_batch() {
        let (journal, _tmp) = create_test_journal().await;
        let h1 = Hash::from_content(b"1");
        let h2 = Hash::from_content(b"2");
        journal
            .append_batch(vec![
                WalEntry::Put { hash: h1, data: Bytes::from_static(b"1") },
                WalEntry::Put { hash: h2, data: Bytes::from_static(b"2") },
            ])
            .await
            .unwrap();
        assert_eq!(journal.pending_count().await, 2);
    }

    #[tokio::test]
    async fn checkpoint_persists_across_reopen() {
        let tmp = TempDir::new().unwrap();
        let cas_dir = tmp.path().to_path_buf();

        // Create journal, append, trim (which writes checkpoint).
        let hash = Hash::from_content(b"checkpoint-test");
        {
            let journal = FileWal::create(cas_dir.clone()).await.unwrap();
            let pos = journal
                .append(WalEntry::Put { hash, data: Bytes::from_static(b"data") })
                .await
                .unwrap();
            journal.trim(pos).await.unwrap();
        }

        // Reopen — checkpoint should have persisted.
        let journal = FileWal::create(cas_dir.clone()).await.unwrap();
        // The appended entry was trimmed, so it should not be pending.
        match journal.check_pending(&hash).await {
            PendingState::NotPresent => {}
            other => panic!("expected NotPresent after reopen, got {other:?}"),
        }
    }

    #[tokio::test]
    async fn segment_sealing_and_replay() {
        let tmp = TempDir::new().unwrap();
        // Very small max segment size to force sealing.
        let journal = FileWal::create_with_max_size(tmp.path().to_path_buf(), 128).await.unwrap();

        // Append enough entries to trigger sealing.
        let mut last_pos = WalPosition::ZERO;
        for i in 0..20u8 {
            let data = vec![i; 256];
            let hash = Hash::from_content(&data);
            last_pos =
                journal.append(WalEntry::Put { hash, data: Bytes::from(data) }).await.unwrap();
        }

        // Replay from first position should return all entries.
        let all = journal.replay_from(WalPosition::ZERO).await;
        assert_eq!(all.len(), 20, "expected 20 entries from replay");
        assert_eq!(all.last().unwrap().0, last_pos);

        // Trim and verify.
        journal.trim(last_pos).await.unwrap();
        assert_eq!(journal.pending_count().await, 0);
    }

    #[tokio::test]
    async fn replay_returns_entries_in_order() {
        let (journal, _tmp) = create_test_journal().await;
        let mut positions = Vec::new();
        for i in 0..5u8 {
            let data = [i; 64];
            let hash = Hash::from_content(&data);
            let pos = journal
                .append(WalEntry::Put { hash, data: Bytes::from(data.to_vec()) })
                .await
                .unwrap();
            positions.push(pos);
        }

        let all = journal.replay_from(WalPosition::ZERO).await;
        assert_eq!(all.len(), 5);
        for (i, (pos, _)) in all.iter().enumerate() {
            assert_eq!(*pos, positions[i], "entry {i} position mismatch");
        }
    }

    #[tokio::test]
    async fn committed_position() {
        let (journal, _tmp) = create_test_journal().await;
        assert_eq!(journal.committed_position().await, WalPosition::ZERO);

        let h = Hash::from_content(b"x");
        let pos = journal
            .append(WalEntry::Put { hash: h, data: Bytes::from_static(b"x") })
            .await
            .unwrap();
        assert_eq!(journal.committed_position().await, pos);
    }
}
