//! Envelope-codec tests for the `delta/versions/` dispatcher.
//!
//! Split out of `mod.rs` because the combined inline test module exceeded the
//! ~300-line limit in `.agents/instructions/rust-conventions.instructions.md`.
//! Codec correctness and structural policy are different concerns with
//! different failure modes, so keeping them apart keeps a failure readable.
//!
//! ## DO NOT REMOVE: versions policy guard
//! - `vX.rs` files must never import unversioned structs outside `versions/`.
//! - A `vX` file may only reference the most recent previous version, and only
//!   for version-to-version migration.
//! - `mod.rs` is the only place where latest version state is bridged to
//!   unversioned runtime state.
//! - Files outside `versions/` must interact with versioned envelopes only
//!   through `mod.rs`, never through direct `versions::vX` imports.
//! - Do not directly re-export `versions::vX` structs/types. Expose
//!   unversioned APIs and keep versioned internals encapsulated.
//!
//! This file carries the guard marker because the guard applies to every file
//! in a `versions/` directory, not only to the ones with production code. It
//! restates `mod.rs`'s guard; it does not replace it.
//!
//! The guard marker here is load-bearing rather than decorative:
//! `every_versions_dir_keeps_policy_guard_docstring` requires it in every
//! `.rs` file whose parent directory is named `versions`, so removing it from
//! this sibling would fail the test that protects the guard.

use super::*;
use crate::Hash;

#[test]
/// Verifies latest-version encoded payloads decode through dispatch path.
fn decode_latest_roundtrip_restores_state() {
    let state = DeltaState {
        base_hash: Hash::from_content(b"base"),
        content_len: 12,
        payload: vec![1, 2, 3, 4],
    };

    let bytes = encode_delta_state(state.clone());
    let restored = decode_delta_state(&bytes).expect("v1 payload should decode via dispatcher");

    assert_eq!(restored, state);
}

#[test]
/// Verifies V1 wire format bytes decode through V1→V2 migration dispatch.
fn decode_dispatches_v1_and_restores_state() {
    let v1_envelope = v1::V1Envelope {
        base_hash: Hash::from_content(b"base"),
        content_len: 12,
        payload_len: 4,
        checksum: 0,
        payload: vec![1, 2, 3, 4],
    };

    let v1_bytes = v1_envelope.encode();
    let restored =
        decode_delta_state(&v1_bytes).expect("V1 payload should decode and migrate via dispatcher");

    assert_eq!(
        restored,
        DeltaState {
            base_hash: v1_envelope.base_hash,
            content_len: v1_envelope.content_len,
            payload: v1_envelope.payload,
        }
    );
}

#[test]
/// Verifies dispatch rejects payloads with invalid family magic.
fn decode_rejects_bad_magic() {
    let state = DeltaState {
        base_hash: Hash::from_content(b"base"),
        content_len: 12,
        payload: vec![1, 2, 3, 4],
    };

    let mut bytes = encode_delta_state(state);
    bytes[0] ^= 0xFF;

    let error =
        decode_delta_state(&bytes).expect_err("bad magic must fail dispatcher prefix validation");
    assert!(matches!(error, CasError::CorruptObject { .. }));
}

#[test]
/// Verifies dispatcher rejects unknown/unsupported wire versions.
fn decode_rejects_unsupported_version() {
    let state = DeltaState {
        base_hash: Hash::from_content(b"base"),
        content_len: 12,
        payload: vec![1, 2, 3, 4],
    };

    let mut bytes = encode_delta_state(state);
    // Use version 99 (well beyond latest=3) to ensure rejection
    bytes[6] = 99;
    bytes[7] = 0;

    let error =
        decode_delta_state(&bytes).expect_err("unknown envelope version must fail dispatcher");
    assert!(matches!(error, CasError::CorruptObject { .. }));
}
