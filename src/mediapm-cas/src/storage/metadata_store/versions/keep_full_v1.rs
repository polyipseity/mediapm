//! V1 of the keep-full marker file's format.
//!
//! The marker records which objects must keep their own full encoding, and it is
//! a ladder of its own: it answers a different question from the metadata
//! snapshot, it is written to a different file on its own schedule, and it can be
//! bumped without bumping the snapshot. That is why it lives in its own module
//! rather than in `v1.rs`: sharing a rung file would tie the two ladders
//! together and, on the first bump of either, hand one format's bytes to the
//! other's parser.
//!
//! The format is identified by the filename (`keep-full-v1.json`), not by an
//! internal version field, exactly as the snapshot format is.
//!
//! ## DO NOT REMOVE: versions policy guard
//!
//! - This file must never import unversioned structs from outside `versions/`.
//! - A `vX` module may reference only the most recent previous version module,
//!   and only for version-to-version isomorphism/migration.
//! - Latest-version bridging to unversioned runtime structs is owned by
//!   `versions/mod.rs`.

use std::collections::BTreeSet;

use crate::error::CasError;
use crate::hash::Hash;

/// On-disk representation of one directory's keep-full markers.
///
/// The keep-full marker is a second, independent V1 ladder in this module: its
/// filename carries its version exactly as `metadata-v1.json` does, and its file
/// is read and written on its own schedule. The two ladders share a rung count
/// today and can diverge, so neither one's version number speaks for the other.
#[derive(serde::Serialize, serde::Deserialize)]
struct KeepFullFile {
    /// Hex-encoded hashes whose bytes must stay full encodings.
    ///
    /// Sorted, because the writer emits a `BTreeSet`: the file is compared by
    /// eye and in tests, and an unstable order would make an unchanged set look
    /// like a change.
    hashes: Vec<String>,
}

/// Parse one directory's keep-full marker file.
///
/// # Errors
///
/// Returns [`CasError::CorruptObject`] when the bytes are not a keep-full file
/// or name a hash this build cannot parse. A marker that cannot be read is
/// reported rather than treated as empty: it protects bytes that already exist.
pub(crate) fn parse_v1_keep_full(data: &[u8]) -> Result<BTreeSet<Hash>, CasError> {
    if data.is_empty() {
        return Ok(BTreeSet::new());
    }

    let file: KeepFullFile = serde_json::from_slice(data).map_err(|e| CasError::CorruptObject {
        hash: None,
        details: format!("failed to parse keep-full marker file: {e}"),
    })?;

    let mut marked = BTreeSet::new();
    for hex in &file.hashes {
        let hash: Hash = hex.parse().map_err(|_| CasError::CorruptObject {
            hash: None,
            details: format!("invalid hash in keep-full marker file: {hex}"),
        })?;
        marked.insert(hash);
    }
    Ok(marked)
}

/// Serialize one directory's keep-full markers.
///
/// # Errors
///
/// Returns [`CasError::Io`] when the JSON cannot be encoded.
pub(crate) fn serialize_v1_keep_full(marked: &BTreeSet<Hash>) -> Result<Vec<u8>, CasError> {
    let file = KeepFullFile { hashes: marked.iter().map(ToString::to_string).collect() };
    serde_json::to_vec_pretty(&file).map_err(|e| CasError::Io(std::io::Error::other(e)))
}
