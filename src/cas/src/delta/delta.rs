//! VCDIFF-based delta codec used by CAS delta objects.
//!
//! This module intentionally delegates patch generation/application to
//! `oxidelta`, which implements RFC 3284 VCDIFF and interoperates with xdelta3.

use oxidelta::compress::encoder::{CompressOptions, encode_all};
use oxidelta::vcdiff::decoder::decode_memory;

use crate::{CasError, Hash};

/// Encoded VCDIFF patch payload.
#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) struct DeltaPatch {
    vcdiff: Vec<u8>,
}

/// Owned-builder helpers for constructing new delta payloads.
impl DeltaPatch {
    /// Computes a VCDIFF patch from `base` to `target`.
    pub(crate) fn diff(base: &[u8], target: &[u8]) -> Result<Self, CasError> {
        let mut output = Vec::new();
        encode_all(&mut output, base, target, CompressOptions::default())
            .map_err(|err| CasError::codec(format!("vcdiff encode failed: {err}")))?;
        Ok(Self { vcdiff: output })
    }

    /// Returns encoded patch bytes.
    pub(crate) fn encode(&self) -> &[u8] {
        &self.vcdiff
    }

    /// Reconstructs patch wrapper from encoded VCDIFF payload.
    pub(crate) fn decode(bytes: &[u8]) -> Self {
        Self { vcdiff: bytes.to_vec() }
    }

    /// Applies this VCDIFF patch to `base` and returns reconstructed target bytes.
    ///
    /// The `target`, `current`, and `base_hash` parameters provide hash context
    /// for error reporting when delta decode fails during reconstruction.
    ///
    /// # Errors
    /// Returns [`CasError::CorruptObject`] when patch decoding/apply fails,
    /// indicating the encoded delta payload is invalid for the provided base.
    pub(crate) fn apply(
        &self,
        base: &[u8],
        target: Hash,
        current: Hash,
        base_hash: Hash,
    ) -> Result<Vec<u8>, CasError> {
        decode_memory(&self.vcdiff, base).map_err(|err| {
            CasError::corrupt_reconstruction(
                target,
                current,
                base_hash,
                format!("vcdiff decode failed: {err}"),
            )
        })
    }
}

#[cfg(test)]
mod tests {
    use super::DeltaPatch;
    use crate::Hash;

    #[test]
    fn vcdiff_patch_roundtrip_reconstructs_target() {
        let base = b"hello base";
        let target = b"hello evolved target payload";

        let patch = DeltaPatch::diff(base, target).expect("diff should encode");
        let encoded = patch.encode();
        let decoded = DeltaPatch::decode(encoded);
        let restored = decoded
            .apply(
                base,
                Hash::from_content(b"target"),
                Hash::from_content(b"current"),
                Hash::from_content(b"base"),
            )
            .expect("apply should reconstruct target");

        assert_eq!(restored, target);
    }
}
