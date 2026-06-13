//! Versioned binary wire formats for journal and checkpoint artifacts.
//!
//! The long-lived functional core is [`Journal`](super::super::wal::Journal).
//! Each wire version owns:
//! - its exact byte layout,
//! - parse/validate/encode behavior,
//! - direct `From` conversions to/from version-specific state types.
//!
//! ## DO NOT REMOVE: versions policy guard
//!
//! - `vX.rs` files must never import unversioned structs outside `versions/`.
//! - A `vX` file may only reference the most recent previous version, and only
//!   for version-to-version migration.
//! - This `mod.rs` is the only place where latest version state is bridged to
//!   unversioned runtime state.
//! - Files outside `versions/` must interact with versioned envelopes only
//!   through this `mod.rs`, never through direct `versions::vX` imports.
//! - Do not directly re-export `versions::vX` structs/types from this module.
//!   Expose unversioned APIs here and keep versioned internals encapsulated.

// TODO: remove when FileJournal is wired into storage backends.
#![allow(dead_code)]

pub(crate) mod v1;

use crate::error::CasError;

use super::super::wal::{JournalEntry, JournalPosition};

// ---------------------------------------------------------------------------
// Re-exported constants
// ---------------------------------------------------------------------------

/// Header size for all on-disk artifacts: 6-byte magic + 2-byte version.
pub(crate) const HEADER_LEN: usize = 8;

/// Magic prefix for journal segment files.
pub(crate) const JOURNAL_MAGIC: &[u8; 6] = v1::JOURNAL_MAGIC;
/// Current journal segment format version.
pub(crate) const JOURNAL_VERSION: u16 = v1::JOURNAL_VERSION;

/// Magic prefix for checkpoint files.
pub(crate) const CHECKPOINT_MAGIC: &[u8; 6] = v1::CHECKPOINT_MAGIC;
/// Current checkpoint format version.
pub(crate) const CHECKPOINT_VERSION: u16 = v1::CHECKPOINT_VERSION;

/// Maximum supported journal segment format version.
pub(crate) const MAX_JOURNAL_VERSION: u16 = v1::MAX_JOURNAL_VERSION;
/// Maximum supported checkpoint format version.
pub(crate) const MAX_CHECKPOINT_VERSION: u16 = v1::MAX_CHECKPOINT_VERSION;

// ---------------------------------------------------------------------------
// Re-exported header helpers
// ---------------------------------------------------------------------------

pub(crate) use v1::{decode_header, encode_header};

// ---------------------------------------------------------------------------
// Entry encode/decode (bridge between unversioned JournalEntry and V1 types)
// ---------------------------------------------------------------------------

/// Encode a journal entry at the given position.
pub(crate) fn encode_entry(entry: &JournalEntry, pos: JournalPosition) -> Vec<u8> {
    entry_to_v1(entry).encode(pos.as_u64())
}

/// Decode a single journal entry from bytes.
///
/// Returns `(entry, position, bytes_consumed)`.
pub(crate) fn decode_entry(buf: &[u8]) -> Result<(JournalEntry, JournalPosition, usize), CasError> {
    let (v1_entry, pos_u64, consumed) = v1::JournalEntryV1::decode(buf)?;
    let entry = entry_from_v1(&v1_entry);
    Ok((entry, JournalPosition::from_u64(pos_u64), consumed))
}

/// Decode all journal entries from a buffer.
pub(crate) fn decode_entries(buf: &[u8]) -> Result<Vec<(JournalPosition, JournalEntry)>, CasError> {
    let mut entries = Vec::new();
    let mut offset = 0;
    while offset < buf.len() {
        let (entry, pos, consumed) = decode_entry(&buf[offset..])?;
        entries.push((pos, entry));
        offset += consumed;
    }
    Ok(entries)
}

// ---------------------------------------------------------------------------
// Checkpoint encode/decode
// ---------------------------------------------------------------------------

/// Encode a checkpoint file for the given position.
pub(crate) fn encode_checkpoint(pos: JournalPosition) -> Vec<u8> {
    v1::CheckpointV1::encode(pos.as_u64())
}

/// Decode a checkpoint file, returning the last consumed position.
pub(crate) fn decode_checkpoint(buf: &[u8]) -> Result<JournalPosition, CasError> {
    let pos_u64 = v1::CheckpointV1::decode(buf)?;
    Ok(JournalPosition::from_u64(pos_u64))
}

// ---------------------------------------------------------------------------
// Bridge conversions between versioned and unversioned entry types
// ---------------------------------------------------------------------------

fn entry_to_v1(entry: &JournalEntry) -> v1::JournalEntryV1 {
    match entry {
        JournalEntry::Put { hash, data } => {
            v1::JournalEntryV1::Put { hash: *hash, data: data.clone() }
        }
        JournalEntry::Delete { hash } => v1::JournalEntryV1::Delete { hash: *hash },
        JournalEntry::Constraint { target, bases } => {
            v1::JournalEntryV1::Constraint { target: *target, bases: bases.clone() }
        }
    }
}

fn entry_from_v1(entry: &v1::JournalEntryV1) -> JournalEntry {
    match entry {
        v1::JournalEntryV1::Put { hash, data } => {
            JournalEntry::Put { hash: *hash, data: data.clone() }
        }
        v1::JournalEntryV1::Delete { hash } => JournalEntry::Delete { hash: *hash },
        v1::JournalEntryV1::Constraint { target, bases } => {
            JournalEntry::Constraint { target: *target, bases: bases.clone() }
        }
    }
}
