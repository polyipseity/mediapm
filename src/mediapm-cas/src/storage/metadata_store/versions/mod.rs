//! Versioned persistence for metadata constraint data and entries.
//!
//! Currently only V1 is supported.

mod v1;

use std::collections::{BTreeMap, BTreeSet};

use crate::api::ObjectEncoding;
use crate::error::CasError;
use crate::hash::Hash;

/// Return type for snapshot load functions: (constraints, entries).
pub(crate) type SnapshotData = (
    BTreeMap<Hash, BTreeSet<Hash>>,        // constraints: target → bases
    BTreeMap<Hash, (u64, ObjectEncoding)>, // entries: hash → (len, encoding)
);

/// Parse snapshot data from raw bytes (V1).
///
/// Returns empty maps for empty input.
pub(crate) fn load_from_bytes(data: &[u8]) -> Result<SnapshotData, CasError> {
    v1::parse_v1_snapshot(data).map(Option::unwrap_or_default)
}

/// Serialize snapshot (constraints + entries) to `Vec<u8>` in V1 format.
pub(crate) fn save_to_vec(
    constraints: &BTreeMap<Hash, BTreeSet<Hash>>,
    entries: &BTreeMap<Hash, (u64, ObjectEncoding)>,
) -> Result<Vec<u8>, CasError> {
    v1::serialize_v1_snapshot(constraints, entries)
}
