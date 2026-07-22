//! In-memory WAL — ephemeral, `VecDeque`-backed.
//!
//! Entries are never persisted. Suitable for testing and ephemeral
//! [`InMemoryCas`](crate::storage::in_memory::InMemoryCas) usage.
//!
//! Cloning shares the underlying state (all clones see the same entries).

use async_trait::async_trait;
use std::collections::{BTreeSet, VecDeque};
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::{Arc, Mutex};

use crate::error::CasError;
use crate::hash::Hash;

use super::{PendingState, Wal, WalEntry, WalPosition};

/// A [`Wal`] implementation backed by an in-memory `VecDeque`.
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
    #[must_use]
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

    async fn consumed_position(&self) -> WalPosition {
        WalPosition::ZERO
    }

    async fn pending_count(&self) -> u64 {
        self.inner.entries.lock().unwrap().len() as u64
    }

    async fn check_pending(&self, hash: &Hash) -> PendingState {
        let guard = self.inner.entries.lock().unwrap();
        for (_, entry) in guard.iter().rev() {
            match entry {
                WalEntry::Put { hash: h, data } if h == hash => {
                    return PendingState::Present(data.clone());
                }
                WalEntry::PutLarge { hash: h, content_len } if h == hash => {
                    return PendingState::PresentExternal { content_len: *content_len };
                }
                WalEntry::Delete { hash: h } if h == hash => {
                    return PendingState::Tombstone;
                }
                _ => {}
            }
        }
        PendingState::NotPresent
    }

    async fn check_pending_constraint(&self, target: &Hash) -> Option<BTreeSet<Hash>> {
        let guard = self.inner.entries.lock().unwrap();
        for (_, entry) in guard.iter().rev() {
            match entry {
                WalEntry::Put { hash: h, .. } if h == target => return None,
                WalEntry::PutLarge { hash: h, .. } if h == target => return None,
                WalEntry::Delete { hash: h } if h == target => return None,
                WalEntry::Constraint { target: t, bases } if t == target => {
                    return Some(bases.clone());
                }
                _ => {}
            }
        }
        None
    }

    async fn replay_from(&self, pos: WalPosition) -> Vec<(WalPosition, WalEntry)> {
        let guard = self.inner.entries.lock().unwrap();
        let skip = guard.iter().position(|(p, _)| *p >= pos).unwrap_or(guard.len());
        guard.iter().skip(skip).cloned().collect()
    }

    async fn segment_boundaries(&self, from: WalPosition) -> Vec<(WalPosition, WalPosition)> {
        let guard = self.inner.entries.lock().unwrap();
        let Some(&(last_pos, _)) = guard.back() else {
            return Vec::new();
        };
        if last_pos < from {
            return Vec::new();
        }
        vec![(from, last_pos)]
    }

    async fn replay_range(
        &self,
        from: WalPosition,
        to: WalPosition,
    ) -> Vec<(WalPosition, WalEntry)> {
        let guard = self.inner.entries.lock().unwrap();
        guard.iter().filter(|(pos, _)| *pos >= from && *pos <= to).cloned().collect()
    }

    async fn trim(&self, up_to: WalPosition) -> Result<(), CasError> {
        let mut guard = self.inner.entries.lock().unwrap();
        while guard.front().is_some_and(|(p, _)| *p <= up_to) {
            guard.pop_front();
        }
        Ok(())
    }
}
