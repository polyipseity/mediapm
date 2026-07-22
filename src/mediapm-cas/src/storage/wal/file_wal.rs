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

use std::collections::{BTreeSet, HashMap};
use std::path::{Path, PathBuf};
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
    fn path(journal_dir: &Path) -> PathBuf {
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
        journal_dir: &Path,
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
    /// In-memory pending constraint state for fast `check_pending_constraint`.
    /// Maps target hash → (position, bases). Retains only entries > checkpoint.
    pending_constraints: Mutex<HashMap<Hash, (WalPosition, BTreeSet<Hash>)>>,
}

struct JournalWriterState {
    active: Option<ActiveSegment>,
}

impl FileWal {
    /// Default max segment size: 64 MiB.
    pub const DEFAULT_MAX_SEGMENT_SIZE: u64 = crate::defaults::WAL_MAX_SEGMENT_SIZE;

    /// Create or open a file-based journal at `cas_dir`.
    ///
    /// # Errors
    ///
    /// Delegates to [`create_with_max_size`](Self::create_with_max_size).
    ///
    /// If the directory does not exist, it is created. If journal files
    /// already exist, they are scanned and recovered.
    pub async fn create(cas_dir: PathBuf) -> Result<Self, CasError> {
        Self::create_with_max_size(cas_dir, Self::DEFAULT_MAX_SEGMENT_SIZE).await
    }

    /// Create or open a file-based journal with a custom max segment size.
    ///
    /// # Errors
    ///
    /// Returns [`CasError::Io`] if the journal directory cannot be created
    /// or if segment files cannot be read.
    /// Returns [`CasError::CorruptObject`] if segment files have an invalid
    /// format.
    ///
    /// # Panics
    ///
    /// Panics if an entry path has no valid file name (should not happen
    /// for entries returned by `read_dir`).
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
                if let Some((_, last_pos)) = pos_range
                    && last_pos.as_u64() > max_position
                {
                    max_position = last_pos.as_u64();
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

        // Dedup overlapping sealed segments — keep the one with the largest last_pos.
        // Overlapping segments can occur from crash-during-seal leaving both the
        // old active segment and the partially-sealed replacement.
        let mut write_idx = 0;
        while write_idx < sealed.len() {
            let mut best = write_idx;
            let mut next = write_idx + 1;
            while next < sealed.len() && sealed[next].first_pos <= sealed[best].last_pos {
                if sealed[next].last_pos > sealed[best].last_pos {
                    best = next;
                }
                next += 1;
            }
            // Delete all in [write_idx, next) except best.
            for k in write_idx..next {
                if k != best {
                    let _ = std::fs::remove_file(&sealed[k].path);
                }
            }
            // Shift best to write_idx position.
            if best != write_idx {
                sealed[write_idx] = sealed[best].clone();
            }
            write_idx += 1;
            // Drain the now-stale entries after current write_idx.
            if next > write_idx {
                sealed.drain(write_idx..next);
            }
        }

        // Determine next position.
        let next_pos = max_position + 1;

        // Open or create the active segment.
        let active = if let Some(path) = active_path {
            ActiveSegment::open(&path).await?
        } else {
            ActiveSegment::create(&ActiveSegment::path(&journal_dir)).await?
        };

        // Build pending state from checkpoint forward (streaming, one segment at a time).
        let mut pending = HashMap::new();
        let mut pending_constraints = HashMap::new();
        for seg in &sealed {
            if seg.last_pos < checkpoint_pos {
                continue;
            }
            let buf = std::fs::read(&seg.path).map_err(CasError::Io)?;
            if buf.len() < format::HEADER_LEN {
                return Err(CasError::corrupt_object(format!(
                    "segment file too short: {}",
                    seg.path.display()
                )));
            }
            let mut header = [0u8; format::HEADER_LEN];
            header.copy_from_slice(&buf[..format::HEADER_LEN]);
            format::decode_header(header, format::JOURNAL_MAGIC, format::MAX_JOURNAL_VERSION)?;
            for result in format::decode_entries_streaming(&buf[format::HEADER_LEN..]) {
                let (pos, entry) = result?;
                if pos > checkpoint_pos {
                    Self::apply_entry_to_pending(
                        &mut pending,
                        &mut pending_constraints,
                        pos,
                        entry,
                    );
                }
            }
            // buf dropped — segment memory freed
        }
        for (pos, entry) in &active_entries {
            if *pos > checkpoint_pos {
                Self::apply_entry_to_pending(
                    &mut pending,
                    &mut pending_constraints,
                    *pos,
                    entry.clone(),
                );
            }
        }

        Ok(Self {
            inner: Arc::new(FileWalInner {
                journal_dir,
                checkpoint,
                max_segment_size,
                write_lock: tokio::sync::Mutex::new(JournalWriterState { active: Some(active) }),
                sealed: Mutex::new(sealed),
                next_pos: AtomicU64::new(next_pos),
                pending: Mutex::new(pending),
                pending_constraints: Mutex::new(pending_constraints),
            }),
        })
    }

    /// Read all entries from a segment file and return them along with
    /// the first/last position range.
    async fn read_segment_entries(
        path: &Path,
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
        format::decode_header(header, format::JOURNAL_MAGIC, format::MAX_JOURNAL_VERSION)?;

        let entries = format::decode_entries(&buf[format::HEADER_LEN..])?;
        let range = entries.first().zip(entries.last()).map(|((fp, _), (lp, _))| (*fp, *lp));
        Ok((entries, range))
    }

    /// Apply one entry to the pending state maps.
    fn apply_entry_to_pending(
        pending: &mut HashMap<Hash, (WalPosition, PendingState)>,
        pending_constraints: &mut HashMap<Hash, (WalPosition, BTreeSet<Hash>)>,
        pos: WalPosition,
        entry: WalEntry,
    ) {
        match entry {
            WalEntry::Put { hash, data } => {
                pending.insert(hash, (pos, PendingState::Present(data)));
                pending_constraints.remove(&hash);
            }
            WalEntry::PutLarge { hash, content_len } => {
                pending.insert(hash, (pos, PendingState::PresentExternal { content_len }));
                pending_constraints.remove(&hash);
            }
            WalEntry::Delete { hash } => {
                pending.insert(hash, (pos, PendingState::Tombstone));
                pending_constraints.remove(&hash);
            }
            WalEntry::Constraint { target, bases } => {
                pending_constraints.insert(target, (pos, bases));
            }
        }
    }

    /// Ensure the active segment is ready to accept writes.
    /// If it exceeds `max_segment_size`, seal it and create a new one.
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
        let last_pos = entries.0.last().map_or(WalPosition::ZERO, |(pos, _)| *pos);

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
                inner.pending_constraints.lock().unwrap().remove(hash);
            }
            WalEntry::PutLarge { hash, content_len } => {
                inner.pending.lock().unwrap().insert(
                    *hash,
                    (pos, PendingState::PresentExternal { content_len: *content_len }),
                );
                inner.pending_constraints.lock().unwrap().remove(hash);
            }
            WalEntry::Delete { hash } => {
                inner.pending.lock().unwrap().insert(*hash, (pos, PendingState::Tombstone));
                inner.pending_constraints.lock().unwrap().remove(hash);
            }
            WalEntry::Constraint { target, bases } => {
                inner.pending_constraints.lock().unwrap().insert(*target, (pos, bases.clone()));
            }
        }

        Ok(pos)
    }

    async fn consumed_position(&self) -> WalPosition {
        self.inner.checkpoint.position()
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
        map.get(hash).map_or(PendingState::NotPresent, |(_, state)| state.clone())
    }

    async fn check_pending_constraint(&self, target: &Hash) -> Option<BTreeSet<Hash>> {
        let map = self.inner.pending_constraints.lock().unwrap();
        map.get(target).map(|(_, bases)| bases.clone())
    }

    async fn replay_from(&self, pos: WalPosition) -> Vec<(WalPosition, WalEntry)> {
        let inner = &*self.inner;
        // Acquire write lock first (tokio::sync::Mutex) to prevent concurrent writes,
        // then sealed list (std::sync::Mutex). Order ensures no non-Send MutexGuard
        // is held across an await point.
        let _state = inner.write_lock.lock().await;
        let sealed = inner.sealed.lock().unwrap();

        let mut all = Vec::new();

        // Collect from sealed segments (streaming, one segment at a time).
        for seg in sealed.iter() {
            if seg.last_pos < pos {
                continue;
            }
            let Ok(buf) = std::fs::read(&seg.path) else {
                continue;
            };
            if buf.len() < format::HEADER_LEN {
                continue;
            }
            let mut header = [0u8; format::HEADER_LEN];
            header.copy_from_slice(&buf[..format::HEADER_LEN]);
            if format::decode_header(header, format::JOURNAL_MAGIC, format::MAX_JOURNAL_VERSION)
                .is_err()
            {
                continue;
            }
            for result in format::decode_entries_streaming(&buf[format::HEADER_LEN..]) {
                let Ok((epos, entry)) = result else {
                    continue;
                };
                if epos >= pos {
                    all.push((epos, entry));
                }
            }
            // buf dropped — segment memory freed
        }

        // Collect from active segment (streaming).
        let active_path = ActiveSegment::path(&inner.journal_dir);
        if let Ok(buf) = std::fs::read(&active_path)
            && buf.len() >= format::HEADER_LEN
        {
            let mut header = [0u8; format::HEADER_LEN];
            header.copy_from_slice(&buf[..format::HEADER_LEN]);
            if format::decode_header(header, format::JOURNAL_MAGIC, format::MAX_JOURNAL_VERSION)
                .is_err()
            {
                // Skip active segment if header is corrupt.
            } else {
                for result in format::decode_entries_streaming(&buf[format::HEADER_LEN..]) {
                    let Ok((epos, entry)) = result else {
                        break;
                    };
                    if epos >= pos {
                        all.push((epos, entry));
                    }
                }
            }
        }

        // Sort and dedup by position (ordering from different segments).
        all.sort_by_key(|(p, _)| *p);
        all.dedup_by_key(|(p, _)| *p);

        all
    }

    async fn segment_boundaries(&self, from: WalPosition) -> Vec<(WalPosition, WalPosition)> {
        let inner = &*self.inner;
        // Acquire write lock to prevent concurrent writes (same ordering
        // as replay_from) before reading sealed + active segment metadata.
        let state = inner.write_lock.lock().await;
        let sealed = inner.sealed.lock().unwrap();
        let committed =
            WalPosition::from_u64(inner.next_pos.load(Ordering::SeqCst).saturating_sub(1));

        let mut boundaries = Vec::new();

        // Collect from sealed segments where last_pos >= from.
        for seg in sealed.iter() {
            if seg.last_pos < from {
                continue;
            }
            boundaries.push((seg.first_pos, seg.last_pos));
        }

        // Add active segment if it overlaps [from, ∞).
        if let Some(ref active) = state.active {
            // Active segment covers [first_pos, committed_position].
            if committed >= from || active.first_pos >= from {
                boundaries.push((active.first_pos, committed));
            }
        }

        // Sort by first_pos (both sealed and active are already in order,
        // but the active segment always comes last).
        boundaries.sort_by_key(|(first, _)| *first);

        boundaries
    }

    async fn replay_range(
        &self,
        from: WalPosition,
        to: WalPosition,
    ) -> Vec<(WalPosition, WalEntry)> {
        let inner = &*self.inner;
        // Acquire write lock first (tokio::sync::Mutex) to prevent concurrent
        // writes, then sealed list (std::sync::Mutex).
        let _state = inner.write_lock.lock().await;
        let sealed = inner.sealed.lock().unwrap();

        let mut all = Vec::new();

        // Collect from sealed segments that overlap [from, to].
        for seg in sealed.iter() {
            if seg.last_pos < from {
                continue;
            }
            if seg.first_pos > to {
                break;
            }
            let Ok(buf) = std::fs::read(&seg.path) else {
                continue;
            };
            if buf.len() < format::HEADER_LEN {
                continue;
            }
            let mut header = [0u8; format::HEADER_LEN];
            header.copy_from_slice(&buf[..format::HEADER_LEN]);
            if format::decode_header(header, format::JOURNAL_MAGIC, format::MAX_JOURNAL_VERSION)
                .is_err()
            {
                continue;
            }
            for result in format::decode_entries_streaming(&buf[format::HEADER_LEN..]) {
                let Ok((epos, entry)) = result else {
                    continue;
                };
                if epos >= from && epos <= to {
                    all.push((epos, entry));
                }
            }
        }

        // Collect from active segment.
        let active_path = ActiveSegment::path(&inner.journal_dir);
        if let Ok(buf) = std::fs::read(&active_path)
            && buf.len() >= format::HEADER_LEN
        {
            let mut header = [0u8; format::HEADER_LEN];
            header.copy_from_slice(&buf[..format::HEADER_LEN]);
            if format::decode_header(header, format::JOURNAL_MAGIC, format::MAX_JOURNAL_VERSION)
                .is_err()
            {
                // Skip active segment if header is corrupt.
            } else {
                for result in format::decode_entries_streaming(&buf[format::HEADER_LEN..]) {
                    let Ok((epos, entry)) = result else {
                        break;
                    };
                    if epos >= from && epos <= to {
                        all.push((epos, entry));
                    }
                }
            }
        }

        // Sort and dedup by position.
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
        inner.pending_constraints.lock().unwrap().retain(|_, (pos, _)| *pos > up_to);

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
    use crate::storage::wal::versions as wal_format;
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
    async fn append_multiple_entries() {
        let (journal, _tmp) = create_test_journal().await;
        let h1 = Hash::from_content(b"1");
        let h2 = Hash::from_content(b"2");
        journal.append(WalEntry::Put { hash: h1, data: Bytes::from_static(b"1") }).await.unwrap();
        journal.append(WalEntry::Put { hash: h2, data: Bytes::from_static(b"2") }).await.unwrap();
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

    // -----------------------------------------------------------------------
    // Segment dedup tests
    // -----------------------------------------------------------------------

    /// Helper: create a valid sealed segment file with one entry at `first`.
    async fn create_dedup_segment(jd: &std::path::Path, first: u64, last: u64) -> PathBuf {
        let mut bytes =
            wal_format::encode_header(wal_format::JOURNAL_MAGIC, wal_format::JOURNAL_VERSION)
                .to_vec();
        let entry =
            WalEntry::Put { hash: Hash::from_content(b"x"), data: Bytes::from_static(b"x") };
        bytes.extend_from_slice(&wal_format::encode_entry(&entry, WalPosition::from_u64(first)));
        let filename = format!("{:020x}-{:020x}.seg", first, last);
        let path = jd.join(filename);
        tokio::fs::write(&path, &bytes).await.unwrap();
        path
    }

    #[tokio::test]
    async fn dedup_removes_overlapping_segment() {
        let tmp = TempDir::new().unwrap();
        let cas_dir = tmp.path().to_path_buf();
        let journal_dir = cas_dir.join("journal");
        tokio::fs::create_dir_all(&journal_dir).await.unwrap();

        // Segment A: first=1, last=10
        let seg_a = create_dedup_segment(&journal_dir, 1, 10).await;
        // Segment B: first=5, last=20 (overlaps A, larger last_pos)
        let seg_b = create_dedup_segment(&journal_dir, 5, 20).await;

        // Create FileWal — dedup should keep B (larger) and remove A.
        let _journal = FileWal::create_with_max_size(cas_dir, 1024 * 1024).await.unwrap();

        assert!(!seg_a.exists(), "shorter overlapping segment should be removed");
        assert!(seg_b.exists(), "longer segment should survive");
    }

    #[tokio::test]
    async fn dedup_non_overlapping_segments_untouched() {
        let tmp = TempDir::new().unwrap();
        let cas_dir = tmp.path().to_path_buf();
        let journal_dir = cas_dir.join("journal");
        tokio::fs::create_dir_all(&journal_dir).await.unwrap();

        // Three non-overlapping segments.
        let seg_a = create_dedup_segment(&journal_dir, 1, 10).await;
        let seg_b = create_dedup_segment(&journal_dir, 11, 20).await;
        let seg_c = create_dedup_segment(&journal_dir, 21, 30).await;

        let _journal = FileWal::create_with_max_size(cas_dir, 1024 * 1024).await.unwrap();

        assert!(seg_a.exists(), "seg_a should still exist");
        assert!(seg_b.exists(), "seg_b should still exist");
        assert!(seg_c.exists(), "seg_c should still exist");
    }

    /// Regression test for streaming replay in `create_with_max_size`.
    ///
    /// Creates entries across multiple sealed segments, then opens a new
    /// `FileWal` on the same directory. Verifies that the streaming decoder
    /// produces the same pending state as the original append sequence.
    #[tokio::test]
    async fn create_replays_streaming_matches_append_sequence() {
        let tmp = TempDir::new().unwrap();
        let cas_dir = tmp.path().to_path_buf();

        // Tiny max segment size forces sealing after a few entries.
        let entries_per_segment = 3usize;
        let segment_count = 3usize;
        let total_entries = entries_per_segment * segment_count;

        let mut hashes = Vec::new();
        {
            let journal = FileWal::create_with_max_size(cas_dir.clone(), 64).await.unwrap();
            for i in 0..total_entries {
                let data = vec![i as u8; 16];
                let hash = Hash::from_content(&data);
                journal.append(WalEntry::Put { hash, data: Bytes::from(data) }).await.unwrap();
                hashes.push(hash);
            }
        }

        // Reopen — streaming decoder must produce identical pending state.
        let journal = FileWal::create(cas_dir.clone()).await.unwrap();
        for (i, hash) in hashes.iter().enumerate() {
            match journal.check_pending(hash).await {
                PendingState::Present(data) => {
                    assert_eq!(data.len(), 16, "entry {i}: unexpected data length");
                    assert_eq!(data[0], i as u8, "entry {i}: unexpected first byte");
                }
                other => {
                    panic!("entry {i}: expected Present, got {other:?}");
                }
            }
        }
        assert_eq!(journal.pending_count().await, total_entries as u64);
    }
}
