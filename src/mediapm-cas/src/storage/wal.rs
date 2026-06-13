//! Journal (WAL) — crash-safe append-only log.
//!
//! The journal is the **only** crash-safe commitment point. Every operation
//! is appended to the journal before being acknowledged. Other layers
//! (ObjectStore, ReadView) are derived — they can be rebuilt by replaying
//! the journal.
//!
//! See [`InMemoryJournal`] for the in-memory implementation (used by
//! [`InMemoryCas`](crate::storage::in_memory::InMemoryCas) for tests and
//! ephemeral use). A file-based `SegmentedFileJournal` is planned but not
//! yet implemented.

use async_trait::async_trait;
use bytes::Bytes;
use std::collections::BTreeSet;
use std::collections::VecDeque;
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::{Arc, Mutex};

use crate::error::CasError;
use crate::hash::Hash;

// ---------------------------------------------------------------------------
// Position, Entry, PendingState
// ---------------------------------------------------------------------------

/// Unique position in the journal.
///
/// Opaque token — implementation-defined. For [`InMemoryJournal`] this is
/// a monotonically increasing counter.
#[derive(Copy, Clone, Debug, PartialEq, Eq, PartialOrd, Ord)]
pub struct JournalPosition(u64);

impl JournalPosition {
    /// The zero position (before any entry).
    pub const ZERO: JournalPosition = JournalPosition(0);

    /// Return the next consecutive position.
    pub fn next(self) -> Self {
        JournalPosition(self.0 + 1)
    }

    /// Return the inner value.
    pub fn as_u64(self) -> u64 {
        self.0
    }

    /// Create a position from a raw value.
    pub fn from_u64(value: u64) -> Self {
        JournalPosition(value)
    }
}

/// An entry in the journal.
#[derive(Debug, Clone)]
pub enum JournalEntry {
    /// Store `data` under `hash`.
    Put { hash: Hash, data: Bytes },
    /// Delete the object at `hash`.
    Delete { hash: Hash },
    /// A hint that `target` may compress well against `bases`.
    Constraint { target: Hash, bases: BTreeSet<Hash> },
}

/// Result of a pending-entry check.
#[derive(Debug, Clone)]
pub enum PendingState {
    /// No entry for this hash exists in the journal.
    NotPresent,
    /// A `Put` entry exists; the data is available.
    Present(Bytes),
    /// A `Delete` tombstone exists; the object should be considered deleted.
    Tombstone,
}

// ---------------------------------------------------------------------------
// Journal trait
// ---------------------------------------------------------------------------

/// Crash-safe operation log.
#[async_trait]
pub trait Journal: Send + Sync {
    /// Append a single entry. Returns its position.
    async fn append(&self, entry: JournalEntry) -> Result<JournalPosition, CasError>;

    /// Append multiple entries atomically (single flush).
    async fn append_batch(&self, entries: Vec<JournalEntry>) -> Result<(), CasError>;

    /// Return the highest committed position.
    async fn committed_position(&self) -> JournalPosition;

    /// Return the number of entries not yet consumed.
    async fn pending_count(&self) -> u64;

    /// Check whether a hash has a pending entry.
    async fn check_pending(&self, hash: &Hash) -> PendingState;

    /// Replay entries from (and including) `pos`.
    async fn replay_from(&self, pos: JournalPosition) -> Vec<(JournalPosition, JournalEntry)>;

    /// Trim all entries up to and including `up_to`.
    async fn trim(&self, up_to: JournalPosition) -> Result<(), CasError>;
}

// ---------------------------------------------------------------------------
// InMemoryJournal
// ---------------------------------------------------------------------------

/// A [`Journal`] implementation backed by an in-memory `VecDeque`.
///
/// Entries are never persisted. Suitable for testing and ephemeral
/// [`InMemoryCas`](crate::storage::in_memory::InMemoryCas) usage.
///
/// Cloning shares the underlying state (all clones see the same entries).
#[derive(Clone)]
pub struct InMemoryJournal {
    inner: Arc<InMemoryJournalInner>,
}

/// Inner state shared across [`InMemoryJournal`] clones.
pub struct InMemoryJournalInner {
    entries: Mutex<VecDeque<(JournalPosition, JournalEntry)>>,
    next_pos: AtomicU64,
}

impl InMemoryJournal {
    /// Create an empty journal.
    pub fn new() -> Self {
        Self {
            inner: Arc::new(InMemoryJournalInner {
                entries: Mutex::new(VecDeque::new()),
                next_pos: AtomicU64::new(0),
            }),
        }
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
        let pos = JournalPosition(self.inner.next_pos.fetch_add(1, Ordering::SeqCst));
        self.inner.entries.lock().unwrap().push_back((pos, entry));
        Ok(pos)
    }

    async fn append_batch(&self, entries: Vec<JournalEntry>) -> Result<(), CasError> {
        let mut guard = self.inner.entries.lock().unwrap();
        for entry in entries {
            let pos = JournalPosition(self.inner.next_pos.fetch_add(1, Ordering::SeqCst));
            guard.push_back((pos, entry));
        }
        Ok(())
    }

    async fn committed_position(&self) -> JournalPosition {
        let n = self.inner.next_pos.load(Ordering::SeqCst);
        if n == 0 { JournalPosition::ZERO } else { JournalPosition(n - 1) }
    }

    async fn pending_count(&self) -> u64 {
        self.inner.entries.lock().unwrap().len() as u64
    }

    async fn check_pending(&self, hash: &Hash) -> PendingState {
        let guard = self.inner.entries.lock().unwrap();
        // Scan in reverse to find the most recent entry for this hash.
        for (_, entry) in guard.iter().rev() {
            match entry {
                JournalEntry::Put { hash: h, data } if h == hash => {
                    return PendingState::Present(data.clone());
                }
                JournalEntry::Delete { hash: h } if h == hash => {
                    return PendingState::Tombstone;
                }
                _ => {}
            }
        }
        PendingState::NotPresent
    }

    async fn replay_from(&self, pos: JournalPosition) -> Vec<(JournalPosition, JournalEntry)> {
        let guard = self.inner.entries.lock().unwrap();
        let skip = guard.iter().position(|(p, _)| *p >= pos).unwrap_or(guard.len());
        guard.iter().skip(skip).cloned().collect()
    }

    async fn trim(&self, up_to: JournalPosition) -> Result<(), CasError> {
        let mut guard = self.inner.entries.lock().unwrap();
        while guard.front().map_or(false, |(p, _)| *p <= up_to) {
            guard.pop_front();
        }
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use bytes::Bytes;

    #[tokio::test]
    async fn put_and_get() {
        let journal = InMemoryJournal::new();
        let data = Bytes::from_static(b"hello");
        let hash = Hash::from_content(&data);
        journal.append(JournalEntry::Put { hash, data: data.clone() }).await.unwrap();

        match journal.check_pending(&hash).await {
            PendingState::Present(d) => assert_eq!(d, data),
            _ => panic!("expected Present"),
        }
    }

    #[tokio::test]
    async fn delete_and_not_found() {
        let journal = InMemoryJournal::new();
        let hash = Hash::from_content(b"gone");
        journal.append(JournalEntry::Delete { hash }).await.unwrap();

        match journal.check_pending(&hash).await {
            PendingState::Tombstone => {}
            _ => panic!("expected Tombstone"),
        }
    }

    #[tokio::test]
    async fn replay_from() {
        let journal = InMemoryJournal::new();
        let h1 = Hash::from_content(b"a");
        let h2 = Hash::from_content(b"b");
        journal
            .append(JournalEntry::Put { hash: h1, data: Bytes::from_static(b"a") })
            .await
            .unwrap();
        let pos = journal
            .append(JournalEntry::Put { hash: h2, data: Bytes::from_static(b"b") })
            .await
            .unwrap();

        let replayed = journal.replay_from(pos).await;
        assert_eq!(replayed.len(), 1);
        assert!(matches!(replayed[0].1, JournalEntry::Put { hash, .. } if hash == h2));
    }

    #[tokio::test]
    async fn trim_removes_entries() {
        let journal = InMemoryJournal::new();
        let hash = Hash::from_content(b"x");
        let pos = journal
            .append(JournalEntry::Put { hash, data: Bytes::from_static(b"x") })
            .await
            .unwrap();
        journal.trim(pos).await.unwrap();
        assert_eq!(journal.pending_count().await, 0);
    }

    #[tokio::test]
    async fn append_batch() {
        let journal = InMemoryJournal::new();
        let h1 = Hash::from_content(b"1");
        let h2 = Hash::from_content(b"2");
        journal
            .append_batch(vec![
                JournalEntry::Put { hash: h1, data: Bytes::from_static(b"1") },
                JournalEntry::Put { hash: h2, data: Bytes::from_static(b"2") },
            ])
            .await
            .unwrap();
        assert_eq!(journal.pending_count().await, 2);
    }
}
