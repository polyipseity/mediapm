//! Write-ahead log (WAL) — crash-safe append-only log.
//!
//! The WAL is the **only** crash-safe commitment point. Every operation
//! is appended to the WAL before being acknowledged. Other layers
//! (ObjectIndex, ReadView) are derived — they can be rebuilt by replaying
//! the WAL.
//!
//! See [`InMemoryWal`] for the in-memory implementation (used by
//! [`InMemoryCas`](crate::storage::in_memory::InMemoryCas) for tests and
//! ephemeral use). [`FileWal`] provides a file-backed implementation.

pub(crate) mod file_wal;
pub(crate) mod versions;

pub use file_wal::FileWal;

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

/// Unique position in the WAL.
///
/// Opaque token — implementation-defined. For [`InMemoryWal`] this is
/// a monotonically increasing counter.
#[derive(Copy, Clone, Debug, PartialEq, Eq, PartialOrd, Ord)]
pub struct WalPosition(u64);

impl WalPosition {
    /// The zero position (before any entry).
    pub const ZERO: WalPosition = WalPosition(0);

    /// Return the next consecutive position.
    pub fn next(self) -> Self {
        WalPosition(self.0 + 1)
    }

    /// Return the inner value.
    pub fn as_u64(self) -> u64 {
        self.0
    }

    /// Create a position from a raw value.
    pub fn from_u64(value: u64) -> Self {
        WalPosition(value)
    }
}

/// An entry in the WAL.
#[derive(Debug, Clone)]
pub enum WalEntry {
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
    /// No entry for this hash exists in the WAL.
    NotPresent,
    /// A `Put` entry exists; the data is available.
    Present(Bytes),
    /// A `Delete` tombstone exists; the object should be considered deleted.
    Tombstone,
}

// ---------------------------------------------------------------------------
// Wal trait
// ---------------------------------------------------------------------------

/// Crash-safe operation log.
#[async_trait]
pub trait Wal: Send + Sync {
    /// Append a single entry. Returns its position.
    async fn append(&self, entry: WalEntry) -> Result<WalPosition, CasError>;

    /// Return the highest committed position.
    async fn committed_position(&self) -> WalPosition;

    /// Return the number of entries not yet consumed.
    async fn pending_count(&self) -> u64;

    /// Check whether a hash has a pending entry.
    async fn check_pending(&self, hash: &Hash) -> PendingState;

    /// Replay entries from (and including) `pos`.
    async fn replay_from(&self, pos: WalPosition) -> Vec<(WalPosition, WalEntry)>;

    /// Trim all entries up to and including `up_to`.
    async fn trim(&self, up_to: WalPosition) -> Result<(), CasError>;
}

// ---------------------------------------------------------------------------
// InMemoryWal
// ---------------------------------------------------------------------------

/// A [`Wal`] implementation backed by an in-memory `VecDeque`.
///
/// Entries are never persisted. Suitable for testing and ephemeral
/// [`InMemoryCas`](crate::storage::in_memory::InMemoryCas) usage.
///
/// Cloning shares the underlying state (all clones see the same entries).
#[derive(Clone)]
pub struct InMemoryWal {
    inner: Arc<InMemoryWalInner>,
}

/// Inner state shared across [`InMemoryWal`] clones.
pub struct InMemoryWalInner {
    entries: Mutex<VecDeque<(WalPosition, WalEntry)>>,
    next_pos: AtomicU64,
}

impl InMemoryWal {
    /// Create an empty Wal.
    pub fn new() -> Self {
        Self {
            inner: Arc::new(InMemoryWalInner {
                entries: Mutex::new(VecDeque::new()),
                next_pos: AtomicU64::new(0),
            }),
        }
    }
}

impl Default for InMemoryWal {
    fn default() -> Self {
        Self::new()
    }
}

#[async_trait]
impl Wal for InMemoryWal {
    async fn append(&self, entry: WalEntry) -> Result<WalPosition, CasError> {
        let pos = WalPosition(self.inner.next_pos.fetch_add(1, Ordering::SeqCst));
        let mut guard = self.inner.entries.lock().unwrap();
        guard.push_back((pos, entry));
        drop(guard);
        Ok(pos)
    }

    async fn committed_position(&self) -> WalPosition {
        let n = self.inner.next_pos.load(Ordering::SeqCst);
        if n == 0 { WalPosition::ZERO } else { WalPosition(n - 1) }
    }

    async fn pending_count(&self) -> u64 {
        self.inner.entries.lock().unwrap().len() as u64
    }

    async fn check_pending(&self, hash: &Hash) -> PendingState {
        let guard = self.inner.entries.lock().unwrap();
        // Scan in reverse to find the most recent entry for this hash.
        for (_, entry) in guard.iter().rev() {
            match entry {
                WalEntry::Put { hash: h, data } if h == hash => {
                    return PendingState::Present(data.clone());
                }
                WalEntry::Delete { hash: h } if h == hash => {
                    return PendingState::Tombstone;
                }
                _ => {}
            }
        }
        PendingState::NotPresent
    }

    async fn replay_from(&self, pos: WalPosition) -> Vec<(WalPosition, WalEntry)> {
        let guard = self.inner.entries.lock().unwrap();
        let skip = guard.iter().position(|(p, _)| *p >= pos).unwrap_or(guard.len());
        guard.iter().skip(skip).cloned().collect()
    }

    async fn trim(&self, up_to: WalPosition) -> Result<(), CasError> {
        let mut guard = self.inner.entries.lock().unwrap();
        while guard.front().map_or(false, |(p, _)| *p <= up_to) {
            guard.pop_front();
        }
        Ok(())
    }
}
