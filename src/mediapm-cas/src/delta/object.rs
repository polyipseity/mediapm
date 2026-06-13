//! On-disk object encoding helpers for CAS entries.
//!
//! Full objects are stored as raw bytes with no headers.
//! Delta objects are stored in `.diff` files through versioned envelopes under
//! `delta/versions/`.
//!
//! ## Functional Core / Imperative Shell
//!
//! This module keeps a version-agnostic functional core [`DeltaState`] and
//! stores it in [`StoredObject::Delta`].
//!
//! ## DO NOT REMOVE: external versions boundary guard
//!
//! This file is outside `delta/versions/` and must consume versioned envelope
//! behavior only through `delta::versions` (`versions/mod.rs`) entry points,
//! never by importing `delta::versions::vX` modules directly.
//!
//! All wire-format logic (magic bytes, checksums, binary layout, multihash
//! parsing details) is delegated to versioned modules in `delta/versions/`.
//! No Unicode normalization (including NFD) is applied in this module.

use crate::delta::versions::{decode_delta_state, encode_delta_state};
use crate::{CasError, Hash};

/// Version-agnostic delta state used by all wire versions.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct DeltaState {
    /// Base hash used for reconstruction.
    pub base_hash: Hash,
    /// Reconstructed logical content length.
    pub content_len: u64,
    /// Encoded patch payload (VCDIFF bytes).
    pub payload: Vec<u8>,
}

/// Tagged union of persisted object payload variants.
#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) enum StoredObject {
    /// Full object payload (raw bytes only).
    Full { payload: Vec<u8> },
    /// Delta object payload stored in `.diff` files.
    Delta {
        /// Version-agnostic delta state.
        state: DeltaState,
    },
}

/// Constructors and encode/decode helpers for persisted object variants.
impl StoredObject {
    /// Builds one full stored object wrapper.
    #[cfg_attr(not(test), expect(dead_code))]
    pub(crate) const fn full(payload: Vec<u8>) -> Self {
        Self::Full { payload }
    }

    /// Builds one delta stored object wrapper.
    pub(crate) const fn delta(base_hash: Hash, content_len: u64, payload: Vec<u8>) -> Self {
        Self::Delta { state: DeltaState { base_hash, content_len, payload } }
    }

    /// Returns base hash when object is delta, otherwise `None`.
    #[expect(dead_code)]
    pub(crate) const fn base_hash(&self) -> Option<Hash> {
        match self {
            Self::Delta { state } => Some(state.base_hash),
            Self::Full { .. } => None,
        }
    }

    /// Returns payload byte length.
    #[expect(dead_code)]
    pub(crate) fn payload_len(&self) -> u64 {
        match self {
            Self::Full { payload } => payload.len() as u64,
            Self::Delta { state } => state.payload.len() as u64,
        }
    }

    /// Returns payload bytes.
    pub(crate) fn payload(&self) -> &[u8] {
        match self {
            Self::Full { payload } => payload,
            Self::Delta { state } => &state.payload,
        }
    }

    /// Encodes one stored object payload.
    ///
    /// - For [`StoredObject::Full`], this returns raw payload bytes.
    /// - For [`StoredObject::Delta`], this delegates encoding to versioned
    ///   envelope modules.
    ///
    /// The method is deliberately version-agnostic at this layer: all binary
    /// framing, checksums, and wire-layout details live in
    /// `delta::versions/*`.
    pub(crate) fn encode(&self) -> Vec<u8> {
        match self {
            Self::Full { payload } => payload.clone(),
            Self::Delta { state } => encode_delta_state(state.clone()),
        }
    }

    /// Decodes one delta object from `.diff` bytes and validates invariants.
    ///
    /// Integrity checks are delegated to the versioned envelope parser,
    /// including envelope magic/version validation and embedded payload checks.
    pub(crate) fn decode_delta(bytes: &[u8]) -> Result<Self, CasError> {
        let state = decode_delta_state(bytes)?;

        Ok(Self::Delta { state })
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::Hash;

    #[test]
    fn diff_payload_roundtrip_preserves_delta_fields() {
        let base_hash = Hash::from_content(b"base-object");
        let payload = b"encoded-delta-payload".to_vec();
        let original = StoredObject::delta(base_hash, 42, payload.clone());

        let encoded = original.encode();
        let decoded =
            StoredObject::decode_delta(&encoded).expect("encoded bytes should decode successfully");

        assert_eq!(decoded, original);
    }

    #[test]
    fn encode_diff_payload_returns_raw_full_payload() {
        let full = StoredObject::full(vec![1, 2, 3]);
        let encoded = full.encode();

        assert_eq!(encoded, [1, 2, 3]);
    }
}
