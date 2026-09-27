//! Version dispatch for persisted state JSON.
//!
//! This module is the **only** place that knows which schema versions exist
//! and which file owns each one. Every read of `state.json` on disk enters
//! through [`from_json_value`], and every write leaves through
//! [`to_json_value`].
//!
//! ## Supported versions
//!
//! | Version | Wire owner | Shape | Status |
//! | --- | --- | --- | --- |
//! | 1 | `v1.rs` | legacy Nickel wrapper + flat set formats | read-only (migrated on load) |
//! | 2 | `v2.rs` | `managed_tools` as `BTreeMap`, nullable `content_map_hash` | read-only (migrated on load) |
//! | 3 | `v3.rs` | `managed_tools` as flat `Vec` | current; the only format written |
//!
//! All three versions remain readable: no shipped writer ever emitted V1 or
//! V2 alone, but both formats exist on disk and their read paths are load
//! bearing.
//!
//! ## Boundaries
//!
//! - `vX.rs` files own their wire shape and their migration into themselves.
//!   A `vX` file may reference only the version immediately before it.
//! - This `mod.rs` owns the wire-version → runtime-model resolution and the
//!   version dispatch itself. Callers outside `versions/` use the unversioned
//!   functions below and never name a `versions::vX` path.
//! - Legacy `state.ncl` migration also lands here, so the caller in
//!   `config/nickel_io.rs` never has to know a version module.
//!
//! ## DO NOT REMOVE: versions policy guard
//!
//! - Version dispatch lives here, not in any caller. Adding a version to the
//!   `match` arms below is the only supported way to accept a new on-disk
//!   format.
//! - Do not directly re-export `vX` structs or paths from this module; expose
//!   unversioned functions and keep versioned internals encapsulated.
//! - Never delete a read path for a version that could exist on disk. Doing so
//!   is data loss, not cleanup.

mod v1;
mod v2;
mod v3;

use serde_json::Value;

use crate::config::MediaPmState;
use crate::error::MediaPmError;

/// Extracts the numeric `version` field from a state JSON value.
///
/// Returns `MediaPmError::Workflow` when the field is missing or not
/// representable as `u64`.
fn extract_state_version_field(value: &Value) -> Result<u64, MediaPmError> {
    value.get("version").and_then(serde_json::Value::as_u64).ok_or_else(|| {
        MediaPmError::Workflow("missing or invalid 'version' field in state JSON".to_string())
    })
}

/// Decodes one state JSON [`Value`] into the runtime model, dispatching on the
/// document's `version` marker.
///
/// Versions 1 and 2 are read-only and migrate forward; version 3 is decoded
/// natively. An unknown marker is an error rather than a silent fallback, so a
/// document written by a newer mediapm is reported instead of being
/// mis-parsed as an older shape.
///
/// # Errors
///
/// Returns [`MediaPmError::Workflow`] when the version marker is missing,
/// malformed, or unsupported, and [`MediaPmError::Serialization`] when the
/// payload does not match the dispatched version's wire shape.
pub(super) fn from_json_value(value: Value) -> Result<MediaPmState, MediaPmError> {
    match extract_state_version_field(&value)? {
        1 => v1::from_v1_json_value(value),
        2 => v3::from_v2_into_v3(value),
        3 => v3::from_v3_json_value(value),
        v => Err(MediaPmError::Workflow(format!("unsupported mediapm state schema version {v}"))),
    }
}

/// Encodes one [`MediaPmState`] into a state JSON [`Value`].
///
/// Always emits V3, regardless of which version the in-memory state was read
/// from: the ladder is one-way forward.
///
/// # Errors
///
/// Returns [`MediaPmError::Serialization`] if serialization fails.
pub(super) fn to_json_value(state: &MediaPmState) -> Result<Value, MediaPmError> {
    v3::to_v3_json_value(state)
}

/// Migrates one legacy Nickel-sourced state [`Value`] into the runtime model.
///
/// Accepts both V1 shapes (the `state`-key wrapper and the flat post-rewrite
/// format) and routes to the V1 wire owner, which owns every V1 shape.
///
/// # Errors
///
/// Returns [`MediaPmError::Workflow`] if the format is unrecognized, or
/// [`MediaPmError::Serialization`] if deserialization fails.
pub(super) fn migrate_from_old_nickel(value: Value) -> Result<MediaPmState, MediaPmError> {
    v1::from_v1_json_value(value)
}

#[cfg(test)]
mod tests {
    use serde_json::json;

    use super::*;

    /// Builds a representative V1 document in the flat post-rewrite shape.
    fn v1_document() -> Value {
        json!({
            "version": 1,
            "managed_files": { "/media/a.mkv": { "media_id": "a", "variant": "p", "hash": "blake3:a" } },
            "tool_registry": {},
            "active_tools": {},
            "workflow_states": {},
            "last_materialized_state_hash": ""
        })
    }

    /// Builds a representative V2 document with a map-keyed tool entry.
    fn v2_document() -> Value {
        json!({
            "version": 2,
            "managed_files": { "/media/a.mkv": { "media_id": "a", "variant": "p", "hash": "blake3:a" } },
            "managed_tools": {
                "ffmpeg": {
                    "version": "7.1",
                    "canonical_version": "ffmpeg-v7.1",
                    "content_map_hash": "blake3:abc",
                    "deployed_at": 1000
                }
            },
            "workflow_states": {}
        })
    }

    /// Every version the dispatch accepts must load through it and resolve to
    /// the unversioned runtime model. This is the ladder's readability
    /// contract: dropping any arm here would make an on-disk format
    /// unreadable.
    #[test]
    fn dispatch_reads_every_supported_version() {
        let v1_state = from_json_value(v1_document()).expect("V1 document must load");
        assert_eq!(v1_state.managed_files.len(), 1);
        assert_eq!(
            v1_state.managed_tools.len(),
            0,
            "V1 tool_registry is dropped by the V1→V2 migration"
        );

        let v2_state = from_json_value(v2_document()).expect("V2 document must load");
        assert_eq!(v2_state.managed_tools.len(), 1);
        assert_eq!(v2_state.managed_tools[0].tool_id, "ffmpeg");
        assert_eq!(v2_state.managed_tools[0].content_map_hash, "blake3:abc");
    }

    /// Every readable version must round-trip: load, then write back, then
    /// load again, with all preserved data intact and the output in the
    /// current (V3) format.
    #[test]
    fn dispatch_round_trips_every_supported_version_through_v3() {
        for (label, document) in [("v1", v1_document()), ("v2", v2_document())] {
            let loaded =
                from_json_value(document).unwrap_or_else(|e| panic!("{label} must load: {e}"));
            let re_encoded = to_json_value(&loaded)
                .unwrap_or_else(|e| panic!("{label} state must re-encode: {e}"));
            assert_eq!(re_encoded["version"], 3, "{label} must be written forward as V3");
            let reloaded = from_json_value(re_encoded)
                .unwrap_or_else(|e| panic!("{label} re-encoded document must load: {e}"));
            assert_eq!(
                reloaded.managed_files, loaded.managed_files,
                "{label} managed_files must survive the round trip"
            );
            assert_eq!(
                reloaded.managed_tools, loaded.managed_tools,
                "{label} managed_tools must survive the round trip"
            );
            assert_eq!(
                reloaded.workflow_states, loaded.workflow_states,
                "{label} workflow_states must survive the round trip"
            );
        }
    }

    /// An unsupported version marker must fail loudly rather than being parsed
    /// under some other version's rules.
    #[test]
    fn dispatch_rejects_unknown_version() {
        let err = from_json_value(json!({ "version": 99 }))
            .expect_err("an unknown version must not be silently accepted");
        match err {
            MediaPmError::Workflow(message) => {
                assert!(message.contains("99"), "error must name the rejected version: {message}");
            }
            other => panic!("expected a Workflow error, got {other:?}"),
        }
    }

    /// A document with no `version` marker must fail with the documented
    /// error rather than defaulting to a version.
    #[test]
    fn dispatch_rejects_missing_version_marker() {
        assert!(from_json_value(json!({ "managed_files": {} })).is_err());
    }
}
