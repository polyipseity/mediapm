//! On-disk object encoding helpers for CAS entries.
//!
//! Full objects are stored as raw bytes with no headers.
//! Delta objects are stored in `.diff` files through versioned envelopes under
//! `codec/versions/`.
//!
//! ## Functional Core / Imperative Shell
//!
//! This module keeps a version-agnostic functional core [`DeltaState`] and
//! stores it in [`StoredObject::Delta`].
//!
//! ## DO NOT REMOVE: external versions boundary guard
//!
//! This file is outside `codec/versions/` and must consume versioned envelope
//! behavior only through `codec::versions` (`versions/mod.rs`) entry points,
//! never by importing `codec::versions::vX` modules directly.
//!
//! All wire-format logic (magic bytes, checksums, binary layout, multihash
//! parsing details) is delegated to versioned modules in `codec/versions/`.
//! No Unicode normalization (including NFD) is applied in this module.

use std::borrow::Cow;

use crate::codec::versions::{decode_delta_state, encode_delta_state};
use crate::{CasError, Hash};

/// Version-agnostic delta state used by all wire versions.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct DeltaState<'a> {
    /// Base hash used for reconstruction.
    pub base_hash: Hash,
    /// Reconstructed logical content length.
    pub content_len: u64,
    /// Encoded patch payload (VCDIFF bytes).
    pub payload: Cow<'a, [u8]>,
}

/// Delta-state ownership conversion helpers.
impl<'a> DeltaState<'a> {
    /// Converts this state into an owned `'static` representation.
    pub fn into_owned(self) -> DeltaState<'static> {
        DeltaState {
            base_hash: self.base_hash,
            content_len: self.content_len,
            payload: Cow::Owned(self.payload.into_owned()),
        }
    }
}

/// Tagged union of persisted object payload variants.
#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) enum StoredObject {
    /// Full object payload (raw bytes only).
    Full { payload: Vec<u8> },
    /// Delta object payload stored in `.diff` files.
    Delta {
        /// Version-agnostic delta state.
        state: DeltaState<'static>,
    },
}

/// Constructors and encode/decode helpers for persisted object variants.
impl StoredObject {
    /// Builds one full stored object wrapper.
    pub(crate) const fn full(payload: Vec<u8>) -> Self {
        Self::Full { payload }
    }

    /// Builds one delta stored object wrapper.
    pub(crate) const fn delta(base_hash: Hash, content_len: u64, payload: Vec<u8>) -> Self {
        Self::Delta { state: DeltaState { base_hash, content_len, payload: Cow::Owned(payload) } }
    }

    /// Returns base hash when object is delta, otherwise `None`.
    pub(crate) const fn base_hash(&self) -> Option<Hash> {
        match self {
            Self::Delta { state } => Some(state.base_hash),
            Self::Full { .. } => None,
        }
    }

    /// Returns reconstructed content length.
    pub(crate) const fn content_len(&self) -> u64 {
        match self {
            Self::Full { payload } => payload.len() as u64,
            Self::Delta { state } => state.content_len,
        }
    }

    /// Returns payload byte length.
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
            Self::Delta { state } => state.payload.as_ref(),
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
    /// `codec::versions/*`.
    pub(crate) fn encode(&self) -> Result<Cow<'_, [u8]>, CasError> {
        let Self::Delta { state } = self else {
            return Ok(Cow::Borrowed(self.payload()));
        };

        let borrowed_state = DeltaState {
            base_hash: state.base_hash,
            content_len: state.content_len,
            payload: Cow::Borrowed(state.payload.as_ref()),
        };

        Ok(Cow::Owned(encode_delta_state(borrowed_state)))
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
    use std::borrow::Cow;
    use std::collections::BTreeSet;

    use bytes::Bytes;
    use tempfile::tempdir;

    use super::*;
    use crate::{CasApi, CasMaintenanceApi, Constraint, FileSystemCas, Hash, OptimizeOptions};

    #[test]
    fn diff_payload_roundtrip_preserves_delta_fields() {
        let base_hash = Hash::from_content(b"base-object");
        let payload = b"encoded-delta-payload".to_vec();
        let original = StoredObject::delta(base_hash, 42, payload.clone());

        let encoded = original.encode().expect("delta object should encode successfully");
        let decoded = StoredObject::decode_delta(encoded.as_ref())
            .expect("encoded bytes should decode successfully");

        assert_eq!(decoded, original);
    }

    #[test]
    fn encode_diff_payload_returns_raw_full_payload() {
        let full = StoredObject::full(vec![1, 2, 3]);
        let encoded = full.encode().expect("full object encoding should return raw payload");

        assert!(matches!(encoded, Cow::Borrowed(_)));
        assert_eq!(encoded.as_ref(), [1, 2, 3]);
    }

    #[tokio::test]
    async fn full_object_is_stored_as_raw_data_without_headers() {
        let dir = tempdir().expect("tempdir");
        let cas = FileSystemCas::open_for_tests(dir.path()).await.expect("open cas");
        let payload = Bytes::from_static(b"raw-full-object");
        let hash = cas.put(payload.clone()).await.expect("put object");

        let path = cas.object_path_for_hash(hash);
        let file_bytes = tokio::fs::read(&path).await.expect("read object file");

        assert_eq!(file_bytes, payload);
        assert!(!cas.diff_path_for_hash(hash).exists());
    }

    #[tokio::test]
    async fn optimized_delta_is_stored_in_diff_extension_file() {
        let dir = tempdir().expect("tempdir");
        let cas = FileSystemCas::open_with_alpha_for_tests(dir.path(), 0).await.expect("open cas");

        let base = cas
            .put(Bytes::from_static(b"aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaBBBBBBBBBBBBBBBB"))
            .await
            .expect("put base");
        let target_bytes = Bytes::from_static(b"aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaCCCCCCCCCCCCCCCC");
        let target = cas.put(target_bytes.clone()).await.expect("put target");

        cas.set_constraint(Constraint {
            target_hash: target,
            potential_bases: BTreeSet::from([base]),
        })
        .await
        .expect("set constraint");

        let _ = cas.optimize_once(OptimizeOptions::default()).await.expect("optimize");
        let restored = cas.get(target).await.expect("get target");

        assert_eq!(restored, target_bytes);
        assert!(cas.diff_path_for_hash(target).exists());
        assert!(!cas.object_path_for_hash(target).exists());
    }
}
