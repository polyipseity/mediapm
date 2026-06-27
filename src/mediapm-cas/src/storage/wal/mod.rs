//! Write-ahead log (WAL) — crash-safe append-only log.
//!
//! The WAL is the **only** crash-safe commitment point. Every operation
//! is appended to the WAL before being acknowledged. Other layers
//! (`ObjectIndex`, `ReadView`) are derived — they can be rebuilt by replaying
//! the WAL.
//!
//! See [`InMemoryWal`] for the in-memory implementation (used by
//! [`InMemoryCas`](crate::storage::in_memory::InMemoryCas) for tests and
//! ephemeral use). [`FileWal`] provides a file-backed implementation.

pub(crate) mod file_wal;
pub(crate) mod mem_wal;
pub(crate) mod versions;

pub use file_wal::FileWal;
pub use mem_wal::InMemoryWal;

use async_trait::async_trait;
use bytes::Bytes;
use std::collections::BTreeSet;

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
    ///
    /// Positions form a dense sequential sequence starting at 0. Each entry
    /// appended to the WAL receives the next sequential position. Must not
    /// overflow `u64::MAX` — callers are responsible for ensuring the WAL
    /// is trimmed (checkpoint advanced) before exhausting the position space.
    #[must_use]
    pub fn next(self) -> Self {
        debug_assert!(self.0 != u64::MAX, "WalPosition overflow");
        WalPosition(self.0 + 1)
    }

    /// Return the inner value.
    #[must_use]
    pub fn as_u64(self) -> u64 {
        self.0
    }

    /// Create a position from a raw value.
    #[must_use]
    pub fn from_u64(value: u64) -> Self {
        WalPosition(value)
    }
}

/// An entry in the WAL.
#[derive(Debug, Clone)]
pub enum WalEntry {
    /// Store `data` under `hash` (small: data inlined).
    Put { hash: Hash, data: Bytes },
    /// Store large data at `hash` (payload immediately materialized to blob).
    PutLarge { hash: Hash, content_len: u64 },
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
    /// A `PutLarge` entry exists; data is materialized to blob store.
    PresentExternal { content_len: u64 },
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

    /// Return the highest consumed (checkpointed) position.
    ///
    /// For persistent WALs this is the position recovered from the
    /// checkpoint file on startup. For in-memory WALs it is ZERO
    /// (no persistence).
    async fn consumed_position(&self) -> WalPosition;

    /// Return the number of entries not yet consumed.
    async fn pending_count(&self) -> u64;

    /// Check whether a hash has a pending entry.
    async fn check_pending(&self, hash: &Hash) -> PendingState;

    /// Check whether a hash has a pending Constraint entry.
    ///
    /// Returns the constraint bases if the most recent pending entry for
    /// `target` is a `Constraint`. Returns `None` if there is no pending
    /// `Constraint` for `target` (either no entry, or a Put/Delete entry
    /// is more recent).
    async fn check_pending_constraint(&self, target: &Hash) -> Option<BTreeSet<Hash>>;

    /// Replay entries from (and including) `pos`.
    async fn replay_from(&self, pos: WalPosition) -> Vec<(WalPosition, WalEntry)>;

    /// Trim all entries up to and including `up_to`.
    async fn trim(&self, up_to: WalPosition) -> Result<(), CasError>;
}
