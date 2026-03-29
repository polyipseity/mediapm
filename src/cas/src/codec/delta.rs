//! VCDIFF-based delta codec used by CAS delta objects.
//!
//! This module intentionally delegates patch generation/application to
//! `oxidelta`, which implements RFC 3284 VCDIFF and interoperates with xdelta3.

use std::borrow::Cow;

use oxidelta::compress::encoder::{CompressOptions, encode_all};
use oxidelta::vcdiff::decoder::decode_memory;

use crate::CasError;

/// RFC 3284 VCDIFF header bytes (magic + supported version).
const VCDIFF_HEADER: [u8; 4] = [0xD6, 0xC3, 0xC4, 0x00];

/// Encoded VCDIFF patch payload.
#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) struct DeltaPatch<'a> {
    vcdiff: Cow<'a, [u8]>,
}

impl DeltaPatch<'static> {
    /// Computes a VCDIFF patch from `base` to `target`.
    pub(crate) fn diff(base: &[u8], target: &[u8]) -> Result<Self, CasError> {
        let mut output = Vec::new();
        encode_all(&mut output, base, target, CompressOptions::default())
            .map_err(|err| CasError::codec(format!("vcdiff encode failed: {err}")))?;
        Ok(Self { vcdiff: Cow::Owned(output) })
    }
}

impl<'a> DeltaPatch<'a> {
    /// Returns encoded patch bytes.
    pub(crate) fn encode(&self) -> &[u8] {
        self.vcdiff.as_ref()
    }

    /// Reconstructs patch wrapper from encoded VCDIFF payload.
    pub(crate) fn decode(bytes: &'a [u8]) -> Result<Self, CasError> {
        if bytes.is_empty() {
            return Err(CasError::corrupt_object("delta payload cannot be empty VCDIFF stream"));
        }

        if bytes.len() < VCDIFF_HEADER.len() {
            return Err(CasError::corrupt_object("delta payload too short for VCDIFF header"));
        }

        if bytes[..3] != VCDIFF_HEADER[..3] {
            return Err(CasError::corrupt_object(
                "delta payload missing supported VCDIFF header magic",
            ));
        }

        if bytes[3] != VCDIFF_HEADER[3] {
            return Err(CasError::corrupt_object(format!(
                "delta payload unsupported VCDIFF version byte: {:#04x}",
                bytes[3]
            )));
        }

        Ok(Self { vcdiff: Cow::Borrowed(bytes) })
    }

    /// Applies this VCDIFF patch to `base` and returns reconstructed target bytes.
    pub(crate) fn apply(&self, base: &[u8]) -> Result<Vec<u8>, CasError> {
        // `decode_memory` consumes borrowed input slices; it does not clone `base`
        // before decoding, keeping reads zero-copy on the base buffer.
        decode_memory(self.vcdiff.as_ref(), base)
            .map_err(|err| CasError::corrupt_object(format!("vcdiff decode failed: {err}")))
    }
}

#[cfg(test)]
mod tests {
    use bytes::Bytes;
    use std::collections::BTreeSet;
    use tempfile::tempdir;

    use super::DeltaPatch;
    use crate::{CasApi, CasError, CasMaintenanceApi, Constraint, FileSystemCas, OptimizeOptions};

    #[test]
    fn vcdiff_patch_roundtrip_reconstructs_target() {
        let base = b"hello base";
        let target = b"hello evolved target payload";

        let patch = DeltaPatch::diff(base, target).expect("diff should encode");
        let encoded = patch.encode();
        let decoded = DeltaPatch::decode(encoded).expect("decode should accept encoded payload");
        let restored = decoded.apply(base).expect("apply should reconstruct target");

        assert_eq!(restored, target);
    }

    #[test]
    fn decode_rejects_empty_payload() {
        let error = DeltaPatch::decode(&[]).expect_err("empty payload must fail");
        assert!(matches!(error, CasError::CorruptObject(_)));
    }

    #[test]
    fn decode_rejects_non_vcdiff_magic() {
        let error = DeltaPatch::decode(&[0x01, 0x02, 0x03, 0x04])
            .expect_err("non-VCDIFF payload must fail");
        assert!(matches!(error, CasError::CorruptObject(_)));
    }

    #[test]
    fn decode_rejects_truncated_vcdiff_header() {
        let error =
            DeltaPatch::decode(&[0xD6, 0xC3]).expect_err("truncated VCDIFF header must fail");
        assert!(matches!(error, CasError::CorruptObject(_)));
    }

    #[test]
    fn decode_rejects_unsupported_vcdiff_version() {
        let error = DeltaPatch::decode(&[0xD6, 0xC3, 0xC4, 0x01])
            .expect_err("unsupported VCDIFF version must fail");
        assert!(matches!(error, CasError::CorruptObject(_)));
    }

    #[tokio::test]
    async fn optimizer_delta_flow_preserves_target_reconstruction() {
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
    }
}
