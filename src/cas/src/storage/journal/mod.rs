//! File-based journal and checkpoint persistence.
//!
//! Provides [`FileJournal`], a [`Journal`](super::wal::Journal) implementation
//! backed by append-only segment files on disk with atomic checkpoint
//! persistence. Read-write separation and segment sealing ensure bounded
//! replay performance.
// TODO(phase8): remove this when journal is wired into storage backends.
#![allow(dead_code)]

pub(crate) mod file_journal;
pub(crate) mod format; // Re-exports from `versions/`; keep as canonical import path.
pub(crate) mod versions;

// Re-export for use by factory functions (e.g., new_file_cas).
#[expect(unused_imports)]
pub(crate) use file_journal::FileJournal;
