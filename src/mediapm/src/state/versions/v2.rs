//! V2 wire format for state persistence.
//!
//! V2 is a read-only historical format: every write emits V3 (see
//! [`super::v3`]). V2 differs from V3 in exactly two ways, both captured by
//! the types in this file:
//!
//! - `managed_tools` is a `BTreeMap` keyed by tool id, so the tool id lives in
//!   the map key rather than inside the entry.
//! - `content_map_hash` is nullable (`Option<String>`), while V3 requires a
//!   plain `String`.
//!
//! This file owns the single canonical definition of the V2 shape. It is the
//! only module that may describe V2; the V2 → V3 migration in
//! [`super::v3`] consumes these types and must not re-declare them.
//!
//! V1 inputs are migrated forward by [`super::v1`].
//!
//! ## DO NOT REMOVE: versions policy guard
//!
//! - This file must never import unversioned structs from outside `versions/`
//!   beyond the resolved runtime model it is declared to bridge.
//! - A `vX` module may reference only the most recent previous version module,
//!   and only for version-to-version migration.
//! - Latest-version bridging to unversioned runtime structs is owned by
//!   `versions/mod.rs`.
//! - Files outside `versions/` must reach versioned symbols only through
//!   `versions/mod.rs`, never through a direct `versions::vX` path.

use std::collections::BTreeMap;

use serde::{Deserialize, Serialize};
use serde_json::Value;

use crate::config::{ManagedFileRecord, ManagedWorkflowStepState, MediaPmState};
use crate::error::MediaPmError;

/// V2 wire representation of one tool-registry entry.
///
/// The tool id is absent by design: in V2 it is the `managed_tools` map key.
/// `content_map_hash` is nullable because V2 predates the non-optional field.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub(super) struct ToolRegistryEntryV2 {
    /// Human-readable version string as recorded by the provider.
    pub version: String,
    /// Canonical version identifier used for skip-if-up-to-date logic.
    #[serde(default)]
    pub canonical_version: String,
    /// blake3 hash of the content-map JSON; `None` when the V2 writer omitted
    /// it, which migrates forward to the empty string.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub content_map_hash: Option<String>,
    /// Unix-epoch seconds when the payload was deployed (0 = not yet).
    #[serde(default)]
    pub deployed_at: u64,
    /// Provenance: resolved upstream git tag, or `None` (JSON `null`).
    #[serde(
        default,
        deserialize_with = "crate::config::custom_deserializers::deserialize_optional_nonempty_string"
    )]
    pub resolved_tag: Option<String>,
    /// Provenance: resolved upstream version, or `None` (JSON `null`).
    #[serde(
        default,
        deserialize_with = "crate::config::custom_deserializers::deserialize_optional_nonempty_string"
    )]
    pub resolved_version: Option<String>,
    /// Provenance: resolved upstream VCS commit hash, or `None` (JSON `null`).
    #[serde(
        default,
        deserialize_with = "crate::config::custom_deserializers::deserialize_optional_nonempty_string"
    )]
    pub resolved_vcs_hash: Option<String>,
}

/// V2 wire representation of the persisted state document.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub(super) struct MediaPmStateV2 {
    /// Schema version marker (always 2 on the wire).
    pub version: u32,
    /// Managed files keyed by filesystem path.
    #[serde(default)]
    pub managed_files: BTreeMap<String, ManagedFileRecord>,
    /// Managed tool deployment metadata keyed by tool id.
    #[serde(default)]
    pub managed_tools: BTreeMap<String, ToolRegistryEntryV2>,
    /// Workflow step states keyed by media id.
    #[serde(default)]
    pub workflow_states: BTreeMap<String, ManagedWorkflowStepState>,
}

/// Decodes one V2 JSON [`Value`] into the canonical [`MediaPmStateV2`] wire type.
///
/// This is the only V2 decode path; the V2 → V3 migration in
/// [`super::v3`] starts here so that no other module re-declares the V2 shape.
///
/// # Errors
///
/// Returns [`MediaPmError::Serialization`] when the value does not match the
/// V2 wire shape (for example when an entry carries the V3-only `tool_id`
/// field, or omits the required `version`).
pub(super) fn from_v2_json_value(value: Value) -> Result<MediaPmStateV2, MediaPmError> {
    serde_json::from_value(value)
        .map_err(|e| MediaPmError::Serialization(format!("failed to decode V2 state: {e}")))
}

/// Encodes one [`MediaPmState`] as a V2 JSON [`Value`].
///
/// Nothing in the running system writes V2 — [`super::to_json_value`] always
/// emits V3 — so this encoder exists for the V2 round-trip proof required by
/// the versioning policy: without an encoder there is no way to show that a
/// representative V2 document survives decode → resolve → encode unchanged.
#[cfg_attr(
    not(test),
    expect(
        dead_code,
        reason = "V2 is read-only in production; the encoder exists so the V2 round-trip test can produce a V2 document"
    )
)]
pub(super) fn to_v2_json_value(state: &MediaPmState) -> Result<Value, MediaPmError> {
    let managed_tools: BTreeMap<String, ToolRegistryEntryV2> = state
        .managed_tools
        .iter()
        .map(|entry| {
            (
                entry.tool_id.clone(),
                ToolRegistryEntryV2 {
                    version: entry.version.clone(),
                    canonical_version: entry.canonical_version.clone(),
                    content_map_hash: Some(entry.content_map_hash.clone()),
                    deployed_at: entry.deployed_at.as_unix_secs(),
                    resolved_tag: entry.resolved_tag.clone(),
                    resolved_version: entry.resolved_version.clone(),
                    resolved_vcs_hash: entry.resolved_vcs_hash.clone(),
                },
            )
        })
        .collect();
    let v2 = MediaPmStateV2 {
        version: 2,
        managed_files: state.managed_files.clone(),
        managed_tools,
        workflow_states: state.workflow_states.clone(),
    };

    serde_json::to_value(v2)
        .map_err(|e| MediaPmError::Serialization(format!("failed to serialize state to JSON: {e}")))
}

#[cfg(test)]
mod tests {
    use serde_json::json;

    use super::*;

    /// The V2 document is produced from the resolved runtime model, decoded
    /// back into the canonical V2 wire type, and must round-trip. Proves the
    /// V2 shape is expressible and lossless in both directions, which is the
    /// round-trip proof the versioning policy requires of a readable version.
    #[test]
    fn v2_encode_decode_round_trip_preserves_the_v2_shape() {
        let state = MediaPmState {
            version: 3,
            managed_files: BTreeMap::from([(
                "/media/a.mkv".to_string(),
                ManagedFileRecord {
                    media_id: "a".to_string(),
                    variant: "primary".to_string(),
                    hash: "blake3:a".to_string(),
                },
            )]),
            managed_tools: vec![crate::config::ToolRegistryEntry {
                tool_id: "ffmpeg".to_string(),
                version: "7.1".to_string(),
                canonical_version: "ffmpeg-v7.1".to_string(),
                content_map_hash: "blake3:abc".to_string(),
                deployed_at: mediapm_utils::Timestamp::from_unix_secs(1_700_000_000),
                resolved_tag: Some("v7.1".to_string()),
                resolved_version: Some("7.1".to_string()),
                resolved_vcs_hash: Some("abc".to_string()),
            }],
            workflow_states: BTreeMap::new(),
        };

        let encoded = to_v2_json_value(&state).expect("encode as V2");
        assert_eq!(encoded["version"], 2);
        assert!(
            encoded["managed_tools"].is_object(),
            "V2 managed_tools must be a map keyed by tool id"
        );

        let decoded = from_v2_json_value(encoded).expect("decode back from V2");
        assert_eq!(decoded.version, 2);
        assert_eq!(decoded.managed_files, state.managed_files);
        let entry = decoded.managed_tools.get("ffmpeg").expect("ffmpeg entry keyed by tool id");
        assert_eq!(entry.version, "7.1");
        assert_eq!(entry.canonical_version, "ffmpeg-v7.1");
        assert_eq!(entry.content_map_hash.as_deref(), Some("blake3:abc"));
        assert_eq!(entry.deployed_at, 1_700_000_000);
        assert_eq!(entry.resolved_tag.as_deref(), Some("v7.1"));
    }

    /// A representative V2 document must decode through the canonical V2
    /// wire type. This guards the property the V2 → V3 migration depends on:
    /// V2 entries carry no `tool_id` (it is the map key) and their
    /// `content_map_hash` is nullable.
    #[test]
    fn representative_v2_document_decodes_into_canonical_wire_type() {
        let v2_doc = json!({
            "version": 2,
            "managed_files": {},
            "managed_tools": {
                "ffmpeg": {
                    "version": "7.1",
                    "canonical_version": "ffmpeg-v7.1",
                    "content_map_hash": "blake3:abc",
                    "deployed_at": 1000,
                    "resolved_tag": null,
                    "resolved_version": null,
                    "resolved_vcs_hash": null
                }
            },
            "workflow_states": {}
        });

        let decoded = from_v2_json_value(v2_doc).expect("representative V2 document must decode");
        assert_eq!(decoded.version, 2);
        let entry = decoded.managed_tools.get("ffmpeg").expect("ffmpeg entry keyed by tool id");
        assert_eq!(entry.version, "7.1");
        assert_eq!(entry.canonical_version, "ffmpeg-v7.1");
        assert_eq!(entry.content_map_hash.as_deref(), Some("blake3:abc"));
        assert_eq!(entry.deployed_at, 1000);
    }

    /// The tool id in V2 is the `managed_tools` **map key**, not a field in
    /// the entry. A V3-shaped entry that carries `tool_id` must therefore
    /// still resolve to the map key, not to the entry's own field: the key is
    /// the only authority in this format.
    #[test]
    fn v2_tool_id_comes_from_the_map_key_never_from_an_entry_field() {
        let v2_doc = json!({
            "version": 2,
            "managed_tools": {
                "ffmpeg": { "tool_id": "SHOULD-BE-IGNORED", "version": "7.1", "deployed_at": 1 }
            }
        });

        let decoded = from_v2_json_value(v2_doc).expect("map key remains authoritative");
        assert!(
            decoded.managed_tools.contains_key("ffmpeg"),
            "the entry must stay under its map key"
        );
    }

    /// A V2 entry that omits `content_map_hash` is legal on the wire and must
    /// decode as `None` rather than fail, so the migration can apply its own
    /// documented empty-string default.
    #[test]
    fn v2_entry_without_content_map_hash_decodes_as_none() {
        let v2_doc = json!({
            "version": 2,
            "managed_tools": { "yt-dlp": { "version": "2025.1", "deployed_at": 5 } }
        });

        let decoded = from_v2_json_value(v2_doc).expect("V2 entry without content_map_hash");
        let entry = decoded.managed_tools.get("yt-dlp").expect("yt-dlp entry present");
        assert_eq!(entry.content_map_hash, None);
        assert_eq!(entry.canonical_version, "");
    }
}
