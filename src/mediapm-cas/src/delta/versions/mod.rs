//! Versioned binary wire-format envelopes for CAS delta objects.
//!
//! The long-lived functional core is [`crate::delta::object::DeltaState`].
//! Each wire version owns its exact byte layout, parse/validate/encode
//! behavior, and `From` conversions to/from version-specific state types.
//! Version checks should always start checking from the latest version to ensure performance.
//!
//! ## DO NOT REMOVE: versions policy guard
//!
//! - `vX.rs` files must never import unversioned structs outside `versions/`.
//! - A `vX` file may only reference the most recent previous version, and only
//!   for version-to-version migration.
//! - This `mod.rs` is the only place where latest version state is bridged to
//!   unversioned runtime state.
//! - Files outside a `versions/` directory must interact with its versioned
//!   envelopes only through that directory's `versions/mod.rs`, never through
//!   direct `versions::vX` imports.
//! - Do not directly re-export `versions::vX` structs/types from this module.
//!   Expose unversioned APIs here and keep versioned internals encapsulated.

use crate::delta::object::DeltaState;
use crate::{CasError, HashParseError};

pub(crate) mod v1;
pub(crate) mod v2;
pub(crate) mod v3;

/// Bytes required to dispatch a delta envelope: the six-byte magic prefix plus
/// the two-byte version field that follows it.
const ENVELOPE_PREFIX_LEN: usize = 8;

/// Magic prefix for V3+ delta envelopes (`CASDLT` = Content-Addressed Storage Delta).
const DIFF_STORAGE_MAGIC_PREFIX_V3: &[u8; 6] = b"CASDLT";
/// Legacy magic prefix for V1/V2 delta envelopes (`MDCASD` = Media Delta).
const DIFF_STORAGE_MAGIC_PREFIX_LEGACY: &[u8; 6] = b"MDCASD";

/// Decodes and validates the embedded wire version from envelope magic bytes.
///
/// Accepts two magic prefixes:
/// - `CASDLT` (V3+, new)
/// - `MDCASD` (V1/V2, legacy)
///
/// Cross-validation: `CASDLT` requires version ≥ 3; `MDCASD` requires version ≤ 2.
fn decode_magic_embedded_version(bytes: &[u8]) -> Result<u16, CasError> {
    let (prefix, version) = if bytes[..6] == *DIFF_STORAGE_MAGIC_PREFIX_V3 {
        (DIFF_STORAGE_MAGIC_PREFIX_V3, u16::from_le_bytes([bytes[6], bytes[7]]))
    } else if bytes[..6] == *DIFF_STORAGE_MAGIC_PREFIX_LEGACY {
        (DIFF_STORAGE_MAGIC_PREFIX_LEGACY, u16::from_le_bytes([bytes[6], bytes[7]]))
    } else {
        return Err(CasError::corrupt_object("delta envelope: magic mismatch"));
    };

    if version == 0 {
        return Err(CasError::corrupt_object(
            "delta envelope: embedded version 0 is reserved for a future >65535-version scheme",
        ));
    }

    // Cross-validate: CASDLT → V3+, MDCASD → V1/V2
    match prefix {
        p if p == DIFF_STORAGE_MAGIC_PREFIX_V3 && version < 3 => {
            return Err(CasError::corrupt_object(
                "delta envelope: CASDLT magic requires version >= 3",
            ));
        }
        p if p == DIFF_STORAGE_MAGIC_PREFIX_LEGACY && version > 2 => {
            return Err(CasError::corrupt_object(
                "delta envelope: MDCASD magic only valid for V1/V2 (use CASDLT for V3+)",
            ));
        }
        _ => {}
    }

    Ok(version)
}

/// Parses a multihash from bytes, returning both hash and consumed byte count.
pub(crate) fn parse_multihash_from_bytes(
    bytes: &[u8],
) -> Result<(crate::Hash, usize), HashParseError> {
    crate::Hash::from_storage_bytes_with_len(bytes)
}

/// Decodes a `.diff` envelope, reporting the wire version it was written in.
///
/// [`decode_delta_state`] answers what the bytes mean and deliberately forgets
/// which format carried that meaning. A caller that must rewrite a legacy
/// envelope needs the version too - not to decode it differently, but to know
/// whether a rewrite is owed - so it is returned rather than re-derived.
pub(crate) fn decode_delta_state_versioned(bytes: &[u8]) -> Result<(u32, DeltaState), CasError> {
    if bytes.len() < ENVELOPE_PREFIX_LEN {
        return Err(CasError::corrupt_object(
            "delta envelope: buffer too short for magic-with-version prefix",
        ));
    }

    let version = decode_magic_embedded_version(bytes)?;

    let state = match version {
        3 => {
            let envelope = v3::V3Envelope::parse(bytes)?;
            envelope.validate()?;
            let state = v3::DeltaStateV3::from(envelope);
            DeltaState {
                base_hash: state.base_hash,
                content_len: state.content_len,
                payload: state.payload,
            }
        }
        2 => {
            let envelope = v2::V2Envelope::parse(bytes)?;
            envelope.validate()?;
            let state_v2 = v2::DeltaStateV2::from(envelope);
            let state_v3 = v3::DeltaStateV3::from(state_v2);
            DeltaState {
                base_hash: state_v3.base_hash,
                content_len: state_v3.content_len,
                payload: state_v3.payload,
            }
        }
        1 => {
            let envelope = v1::V1Envelope::parse(bytes)?;
            envelope.validate()?;
            let state_v1 = v1::DeltaStateV1::from(envelope);
            let state_v2 = v2::DeltaStateV2::from(state_v1);
            let state_v3 = v3::DeltaStateV3::from(state_v2);
            DeltaState {
                base_hash: state_v3.base_hash,
                content_len: state_v3.content_len,
                payload: state_v3.payload,
            }
        }
        _ => {
            return Err(CasError::corrupt_object(format!(
                "delta envelope: unsupported version {version}"
            )));
        }
    };

    Ok((u32::from(version), state))
}

/// Decodes versioned `.diff` bytes into version-agnostic [`DeltaState`].
///
/// This function is the only version-dispatch entry point. It peeks at
/// `magic_with_embedded_version` and dispatches to the matching version parser,
/// migrating older formats forward to the latest state representation via `From` impls.
pub(crate) fn decode_delta_state(bytes: &[u8]) -> Result<DeltaState, CasError> {
    decode_delta_state_versioned(bytes).map(|(_, state)| state)
}

/// Shared payload-length validation for V1/V2 envelopes.
pub(crate) fn validate_payload_len(
    field_payload_len: u64,
    actual_payload_len: u64,
) -> Result<(), CasError> {
    if field_payload_len != actual_payload_len {
        return Err(CasError::corrupt_object(format!(
            "delta envelope: payload_len mismatch (field {field_payload_len}, actual {actual_payload_len})"
        )));
    }
    Ok(())
}

/// Shared trailing-bytes / truncation check for V1/V2 parsers.
pub(crate) fn check_payload_bounds(bytes_len: usize, payload_end: usize) -> Result<(), CasError> {
    if bytes_len < payload_end {
        return Err(CasError::corrupt_object(format!(
            "delta envelope: buffer too short for payload (need {payload_end}, have {bytes_len})"
        )));
    }
    if bytes_len != payload_end {
        return Err(CasError::corrupt_object(format!(
            "delta envelope: trailing bytes after payload (expected {payload_end}, have {bytes_len})"
        )));
    }
    Ok(())
}

/// Encodes unversioned runtime delta state using the latest wire version.
pub(crate) fn encode_delta_state(state: DeltaState) -> Vec<u8> {
    let envelope = v3::V3Envelope::from_parts(state.base_hash, state.content_len, state.payload);
    envelope.encode()
}

/// Wire version written by [`encode_delta_state`].
///
/// A reader that rewrites a legacy envelope compares the version it found with
/// this constant, so the "is this current?" decision lives next to the writer
/// that defines what "current" means.
pub(crate) const LATEST_DELTA_VERSION: u32 = 3;

/// Re-encodes delta state as a legacy V1 envelope.
///
/// Used by tests that verify a verifying store rewrites legacy bytes on first
/// read. V1 has no integrity field, so it is the format the rewrite targets.
pub(crate) fn legacy_delta_bytes_for_tests(state: &DeltaState) -> Vec<u8> {
    let payload_len = u64::try_from(state.payload.len()).expect("payload length exceeds u64::MAX");
    let envelope = v1::V1Envelope {
        base_hash: state.base_hash,
        content_len: state.content_len,
        payload_len,
        checksum: 0,
        payload: state.payload.clone(),
    };
    envelope.encode()
}

/// Decodes a current delta envelope and re-encodes it as a legacy V1 envelope.
///
/// Integration tests build legacy on-disk bytes through this helper so they
/// exercise the rewrite path without naming a version module.
#[doc(hidden)]
#[must_use]
pub fn legacy_delta_v1_wire_from_current(current: &[u8]) -> Vec<u8> {
    let state = decode_delta_state(current).expect("current delta envelope must decode");
    legacy_delta_bytes_for_tests(&state)
}

// The test module is split into themed siblings because it exceeded the
// ~300-line inline-test limit in
// `.agents/instructions/rust-workflow.instructions.md`. Both siblings
// carry the non-removable versions policy guard marker in their own module
// doc, which `every_versions_dir_keeps_policy_guard_docstring` requires of
// every file in a `versions/` directory.
#[cfg(test)]
mod mod_envelope_codec;
#[cfg(test)]
mod mod_policy_guard;

#[cfg(test)]
mod tests {
    use super::*;

    /// The dispatch must report which version it found, because the caller that
    /// rewrites a legacy envelope needs to know there is something to rewrite.
    #[test]
    fn the_versioned_decode_reports_the_format_it_read() {
        let state = DeltaState {
            base_hash: crate::Hash::from_content(b"base"),
            content_len: 4,
            payload: vec![b'd', b'i', b'f', b'f'],
        };
        let v3 = encode_delta_state(state.clone());
        let (version, decoded) =
            decode_delta_state_versioned(&v3).expect("a V3 envelope must decode");
        assert_eq!(version, 3);
        assert_eq!(decoded, state);

        let v1_state = v1::DeltaStateV1 {
            base_hash: state.base_hash,
            content_len: state.content_len,
            payload: state.payload.clone(),
        };
        let v1_bytes = v1::V1Envelope::from(v1_state).encode();
        let (version, decoded) =
            decode_delta_state_versioned(&v1_bytes).expect("a V1 envelope must decode");
        assert_eq!(version, 1, "a V1 envelope must be reported as V1, not as the state it became");
        assert_eq!(decoded, state);
    }

    /// Re-encoding a legacy state must produce bytes the current format owns,
    /// which is what makes the one-time rewrite a fix rather than a copy.
    #[test]
    fn re_encoding_a_legacy_state_yields_a_v3_envelope() {
        let state = DeltaState {
            base_hash: crate::Hash::from_content(b"base"),
            content_len: 4,
            payload: vec![b'd', b'i', b'f', b'f'],
        };
        let rewritten = encode_delta_state(state.clone());
        let (version, decoded) =
            decode_delta_state_versioned(&rewritten).expect("re-encoded bytes must decode");
        assert_eq!(version, 3, "the rewrite must land on the newest format");
        assert_eq!(decoded, state);
    }

    /// The version constant is the writer's claim; pinning it to the bytes the
    /// writer actually emits keeps a future format bump from forgetting to move
    /// the constant.
    #[test]
    fn latest_delta_version_matches_the_writer() {
        let state = DeltaState {
            base_hash: crate::Hash::from_content(b"base"),
            content_len: 1,
            payload: vec![0],
        };
        let bytes = encode_delta_state(state);
        let (version, _) = decode_delta_state_versioned(&bytes).expect("fresh bytes must decode");
        assert_eq!(version, LATEST_DELTA_VERSION);
    }
}
