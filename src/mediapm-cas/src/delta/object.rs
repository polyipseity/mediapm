//! On-disk object encoding helpers for CAS entries.
//!
//! Full objects are stored as raw bytes with no headers.
//! Delta objects are stored in `.diff` files through versioned envelopes under
//! `delta/versions/`.
//!
//! ## Functional Core / Imperative Shell
//!
//! This module keeps a version-agnostic functional core [`DeltaState`] and
//! stores it in [`StoredObject`].
//!
//! ## DO NOT REMOVE: external versions boundary guard
//!
//! See `delta/versions/mod.rs` for the canonical versions boundary policy. This file
//! must consume versioned envelope behavior only through `delta::versions`
//! entry points, never via `delta::versions::vX` imports.
//!
//! All wire-format logic is delegated to `delta/versions/`.

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

/// Wrapper around a [`DeltaState`] for persisted delta encoding/decoding.
#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) struct StoredObject {
    /// Version-agnostic delta state.
    state: DeltaState,
}

impl StoredObject {
    /// Builds one delta stored object wrapper.
    pub(crate) const fn delta(base_hash: Hash, content_len: u64, payload: Vec<u8>) -> Self {
        Self { state: DeltaState { base_hash, content_len, payload } }
    }

    /// Returns payload bytes.
    pub(crate) fn payload(&self) -> &[u8] {
        &self.state.payload
    }

    /// Encodes this stored object's delta payload to wire format.
    pub(crate) fn encode(&self) -> Vec<u8> {
        encode_delta_state(self.state.clone())
    }

    /// Decodes one delta object from `.diff` bytes and validates invariants.
    ///
    /// Integrity checks are delegated to the versioned envelope parser,
    /// including envelope magic/version validation and embedded payload checks.
    pub(crate) fn decode_delta(bytes: &[u8]) -> Result<Self, CasError> {
        let state = decode_delta_state(bytes)?;
        Ok(Self { state })
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
}
