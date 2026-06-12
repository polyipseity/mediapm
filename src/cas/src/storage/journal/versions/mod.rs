//! Versioned on-disk format for journal segments and checkpoint files.
//!
//! ## DO NOT REMOVE: versions policy guard
//!
//! - `vX.rs` files must never import unversioned structs outside `versions/`.
//! - A `vX` file may only reference the most recent previous version, and only
//!   for version-to-version isomorphism/migration.
//! - This `mod.rs` is the only place where latest version state is bridged to
//!   unversioned runtime state.
//! - Files outside this module must interact with versioned formats only
//!   through this `mod.rs`, never through direct `versions::vX` imports.
//! - Do not directly re-export `versions::vX` structs/types from this module.
//!   Expose unversioned APIs here and keep versioned internals encapsulated.

pub(crate) mod v1;

use crate::error::CasError;
use crate::hash::Hash;
use crate::storage::journal::JournalEntry;

/// Encode a journal entry using the latest version format.
pub(crate) fn encode_entry(entry: &JournalEntry, buf: &mut Vec<u8>) {
    v1::encode_entry(entry, buf)
}

/// Decode a journal entry using the latest version format.
pub(crate) fn decode_entry<R: std::io::Read>(
    reader: &mut R,
) -> Result<Option<JournalEntry>, CasError> {
    v1::decode_entry(reader)
}

/// Encode a checkpoint file using the latest version format.
pub(crate) fn encode_checkpoint(position: u64, integrity_hash: &Hash) -> Vec<u8> {
    v1::encode_checkpoint(position, integrity_hash)
}

/// Decode a checkpoint file using the latest version format.
pub(crate) fn decode_checkpoint(data: &[u8]) -> Result<(u64, Hash), CasError> {
    v1::decode_checkpoint(data)
}

/// Header length for journal segments (latest version).
pub(crate) const SEGMENT_HEADER_LEN: u32 = v1::SEGMENT_HEADER_LEN;
/// Magic prefix for journal segment files.
pub(crate) const JOURNAL_MAGIC: &[u8; 6] = v1::JOURNAL_MAGIC;
/// Magic prefix for checkpoint files.
pub(crate) const CHECKPOINT_MAGIC: &[u8; 6] = v1::CHECKPOINT_MAGIC;
/// Version tag.
pub(crate) const VERSION_TAG: [u8; 2] = v1::VERSION_TAG;
/// Total checkpoint file length.
pub(crate) const CHECKPOINT_LEN: u64 = v1::CHECKPOINT_LEN;
