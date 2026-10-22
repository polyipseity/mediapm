//! V4 wire format for state persistence — the format every write emits.
//!
//! V4 is V3 with `pending_force_rerun` removed. The field was the queue half of
//! destructive invalidation: a workflow name a later run had to remember to look
//! up. Explicit invalidation is now a marker on the recorded instance
//! (`ToolCallInstance::invalidated_at`), which the probe that decides reuse
//! cannot skip, so a queue beside it would be a second place for the same
//! decision to live and drift. A V3 document still loads, and its queue is
//! dropped rather than honoured: the request named a mechanism that no longer
//! exists, and force-rerunning a whole workflow would be a wider action than the
//! one the operator asked for.
//!
//! ## DO NOT REMOVE: versions policy guard
//!
//! - This file must never import unversioned structs from outside `versions/`
//!   beyond the runtime model it is declared to bridge and the wire field types
//!   it carries.
//! - A `vX` module may reference only the most recent previous version module,
//!   and only for version-to-version migration. The `super::v3` reference below
//!   is that single permitted edge.
//! - Latest-version bridging to unversioned runtime structs is owned here, so
//!   `versions/mod.rs` can compose the ladder without naming a wire shape.
//! - Files outside `versions/` must reach versioned symbols only through
//!   `versions/mod.rs`, never through a direct `versions::vX` path.

use std::collections::BTreeMap;

use serde::{Deserialize, Serialize};
use serde_json::Value;

use super::v3::{MediaPmStateV3, dedup_managed_tools};
use crate::config::{ManagedFileRecord, ManagedWorkflowStepState, MediaPmState, ToolRegistryEntry};
use crate::error::MediaPmError;

/// V4 wire representation of [`MediaPmState`].
///
/// Closed: an unknown field is an error rather than being dropped, so a document
/// written by a newer mediapm is reported instead of being read as if the extra
/// field were not there.
#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub(super) struct MediaPmStateV4 {
    /// Schema version marker (always 4).
    pub(super) version: u32,
    /// Managed files keyed by filesystem path.
    #[serde(default)]
    pub(super) managed_files: BTreeMap<String, ManagedFileRecord>,
    /// Managed tool deployment metadata (flat list).
    #[serde(default)]
    pub(super) managed_tools: Vec<ToolRegistryEntry>,
    /// Workflow step states keyed by media id.
    #[serde(default)]
    pub(super) workflow_states: BTreeMap<String, ManagedWorkflowStepState>,
}

/// Resolves a decoded [`MediaPmStateV4`] into the unversioned runtime model.
///
/// The resolved list keeps one entry per `(tool_id, canonical_version)` group —
/// the newest deploy — and orders it newest first, so a migrated state and a
/// freshly written one are indistinguishable downstream. The runtime marker is
/// stamped from the constant rather than echoed from the wire.
pub(super) fn resolve_v4(v4: MediaPmStateV4) -> MediaPmState {
    let mut managed_tools = dedup_managed_tools(v4.managed_tools);
    managed_tools.sort_by_key(|entry| std::cmp::Reverse(entry.deployed_at));

    MediaPmState {
        version: crate::config::defaults::MEDIAPM_STATE_VERSION,
        managed_files: v4.managed_files,
        managed_tools,
        workflow_states: v4.workflow_states,
    }
}

/// Encodes a [`MediaPmState`] as a V4 JSON [`Value`].
///
/// # Errors
///
/// Returns [`MediaPmError::Serialization`] if serialization fails.
pub(super) fn to_v4_json_value(state: &MediaPmState) -> Result<Value, MediaPmError> {
    let mut managed_tools = dedup_managed_tools(state.managed_tools.clone());
    managed_tools.sort_by_key(|entry| std::cmp::Reverse(entry.deployed_at));
    let v4 = MediaPmStateV4 {
        version: 4,
        managed_files: state.managed_files.clone(),
        managed_tools,
        workflow_states: state.workflow_states.clone(),
    };

    serde_json::to_value(v4)
        .map_err(|e| MediaPmError::Serialization(format!("failed to serialize state to JSON: {e}")))
}

/// Decodes a V4 JSON [`Value`] into [`MediaPmState`].
///
/// The `version` marker is checked against this module's own number. V4's shape
/// is a subset of V3's — every V4 field exists in V3 — so a document that
/// labelled itself V3 and happened to carry no queue would otherwise decode here
/// without complaint, which is exactly the silent mis-parse the dispatch exists
/// to prevent.
///
/// # Errors
///
/// Returns [`MediaPmError::Serialization`] when the value does not match the V4
/// wire shape, when it carries a field the shape does not have, or when its
/// `version` marker is not 4.
pub(super) fn from_v4_json_value(value: Value) -> Result<MediaPmState, MediaPmError> {
    let v4: MediaPmStateV4 = serde_json::from_value(value)
        .map_err(|e| MediaPmError::Serialization(format!("failed to decode V4 state: {e}")))?;
    if v4.version != 4 {
        return Err(MediaPmError::Serialization(format!(
            "state document is marked version {} but was decoded as V4",
            v4.version,
        )));
    }
    Ok(resolve_v4(v4))
}

/// Migrates a V3 wire document forward into the runtime model.
///
/// The queue is dropped, not translated: V4 has no place to keep it and no code
/// that would act on it. Everything V3 shares with V4 — files, tools, workflow
/// steps — moves through [`MediaPmStateV4`] into [`resolve_v4`], so a migrated
/// document is ordered and deduplicated exactly like a native one.
pub(super) fn from_v3_into_v4(v3: MediaPmStateV3) -> MediaPmState {
    resolve_v4(MediaPmStateV4 {
        version: 4,
        managed_files: v3.managed_files,
        managed_tools: v3.managed_tools,
        workflow_states: v3.workflow_states,
    })
}

#[cfg(test)]
mod tests {
    use std::collections::BTreeMap;

    use serde_json::json;

    use super::super::v3::decode_v3;
    use crate::config::{ManagedFileRecord, ManagedWorkflowStepState, MediaPmState};

    use super::*;

    /// Builds a runtime state holding one managed file and one tool entry.
    fn sample_state() -> MediaPmState {
        MediaPmState {
            version: 4,
            managed_files: BTreeMap::from([(
                "/media/file.mp4".to_string(),
                ManagedFileRecord {
                    media_id: "test-source".to_string(),
                    variant: "primary".to_string(),
                    hash: "blake3:abc123".to_string(),
                },
            )]),
            managed_tools: vec![ToolRegistryEntry {
                tool_id: "ffmpeg".to_string(),
                version: "7.1".to_string(),
                canonical_version: "ffmpeg-v7.1".to_string(),
                content_map_hash: "blake3:def456".to_string(),
                deployed_at: mediapm_utils::Timestamp::from_unix_secs(1_700_000_000),
                resolved_tag: Some("v7.1".to_string()),
                resolved_version: Some("7.1".to_string()),
                resolved_vcs_hash: Some("abc".to_string()),
            }],
            workflow_states: BTreeMap::from([(
                "media-a".to_string(),
                ManagedWorkflowStepState {
                    variant_hashes: BTreeMap::from([(
                        "primary".to_string(),
                        "blake3:p".to_string(),
                    )]),
                    steps_completed: 1,
                    last_impure_sync_at: Some(mediapm_utils::Timestamp::from_unix_secs(42)),
                },
            )]),
        }
    }

    /// A runtime state round-trips through the V4 wire with no field lost, and
    /// the encoded document is marked version 4.
    #[test]
    fn v4_round_trip_preserves_every_field() {
        let state = sample_state();

        let value = to_v4_json_value(&state).expect("encode v4");
        assert_eq!(value["version"], json!(4));
        let decoded = from_v4_json_value(value).expect("decode v4");

        assert_eq!(decoded.version, 4);
        assert_eq!(decoded.managed_files, state.managed_files);
        assert_eq!(decoded.managed_tools, state.managed_tools);
        assert_eq!(decoded.workflow_states, state.workflow_states);
    }

    /// The removed queue is not written back.
    ///
    /// A state that carried a queue in a previous version must not resurrect the
    /// field on the way out: the field is gone, and a document that still had it
    /// would make some later reader believe a request is pending.
    #[test]
    fn v4_never_writes_a_queue() {
        let value = to_v4_json_value(&sample_state()).expect("encode v4");
        assert!(
            value.get("pending_force_rerun").is_none(),
            "V4 has no queue, so the encoded document must not carry one: {value}",
        );
    }

    /// A V3 document loads, and its queue is dropped rather than honoured.
    #[test]
    fn from_v3_into_v4_drops_the_queue_and_keeps_everything_else() {
        let v3_json = json!({
            "version": 3,
            "managed_files": {
                "/media/a.mkv": { "media_id": "a", "variant": "primary", "hash": "blake3:a" }
            },
            "managed_tools": [],
            "workflow_states": {
                "a": { "variant_hashes": { "primary": "blake3:p" }, "steps_completed": 2 }
            },
            "pending_force_rerun": ["mediapm.media.a"]
        });

        let state = from_v3_into_v4(decode_v3(v3_json).expect("decode the v3 document"));

        assert_eq!(state.version, 4);
        assert_eq!(state.managed_files.len(), 1);
        assert_eq!(state.workflow_states["a"].steps_completed, 2);
        assert_eq!(state.workflow_states["a"].variant_hashes["primary"], "blake3:p");
        assert_eq!(
            to_v4_json_value(&state).expect("re-encode").get("pending_force_rerun"),
            None,
            "the dropped queue must not reappear on write",
        );
    }

    /// An unknown field is an error, not something to skip.
    #[test]
    fn v4_rejects_an_unknown_field() {
        let document = json!({
            "version": 4,
            "managed_files": {},
            "managed_tools": [],
            "workflow_states": {},
            "pending_force_rerun": ["mediapm.media.a"]
        });

        let error = from_v4_json_value(document).expect_err("an unknown field must be rejected");
        assert!(
            error.to_string().contains("pending_force_rerun"),
            "the diagnostic must name the rejected field: {error}",
        );
    }

    /// The resolved tool list is ordered newest deploy first, which is the order
    /// the "active tool" lookup relies on.
    #[test]
    fn resolve_v4_orders_tools_newest_first() {
        let state = MediaPmState {
            managed_tools: vec![
                ToolRegistryEntry {
                    tool_id: "media-tagger".to_string(),
                    version: "1.0".to_string(),
                    canonical_version: "media-tagger-v1.0".to_string(),
                    content_map_hash: String::new(),
                    deployed_at: mediapm_utils::Timestamp::from_unix_secs(1000),
                    resolved_tag: None,
                    resolved_version: None,
                    resolved_vcs_hash: None,
                },
                ToolRegistryEntry {
                    tool_id: "ffmpeg".to_string(),
                    version: "7.1".to_string(),
                    canonical_version: "ffmpeg-v7.1".to_string(),
                    content_map_hash: String::new(),
                    deployed_at: mediapm_utils::Timestamp::from_unix_secs(3000),
                    resolved_tag: None,
                    resolved_version: None,
                    resolved_vcs_hash: None,
                },
                ToolRegistryEntry {
                    tool_id: "yt-dlp".to_string(),
                    version: "v2".to_string(),
                    canonical_version: "yt-dlp-v2".to_string(),
                    content_map_hash: String::new(),
                    deployed_at: mediapm_utils::Timestamp::from_unix_secs(2000),
                    resolved_tag: None,
                    resolved_version: None,
                    resolved_vcs_hash: None,
                },
            ],
            ..sample_state()
        };

        let value = to_v4_json_value(&state).expect("encode v4");
        let decoded = from_v4_json_value(value).expect("decode v4");

        let ids: Vec<&str> = decoded.managed_tools.iter().map(|e| e.tool_id.as_str()).collect();
        assert_eq!(ids, ["ffmpeg", "yt-dlp", "media-tagger"]);
    }

    /// Two deploys of the same tool at the same canonical version resolve to the
    /// newer one; a different version is kept beside it.
    #[test]
    fn resolve_v4_dedups_by_tool_and_canonical_version() {
        let entry = |deployed_at: u64, canonical: &str| ToolRegistryEntry {
            tool_id: "ffmpeg".to_string(),
            version: String::new(),
            canonical_version: canonical.to_string(),
            content_map_hash: String::new(),
            deployed_at: mediapm_utils::Timestamp::from_unix_secs(deployed_at),
            resolved_tag: None,
            resolved_version: None,
            resolved_vcs_hash: None,
        };
        let state = MediaPmState {
            managed_tools: vec![
                entry(1000, "ffmpeg-v7.1"),
                entry(2000, "ffmpeg-v7.1"),
                entry(1500, "ffmpeg-v6.0"),
            ],
            ..sample_state()
        };

        let value = to_v4_json_value(&state).expect("encode v4");
        let decoded = from_v4_json_value(value).expect("decode v4");

        assert_eq!(decoded.managed_tools.len(), 2, "one entry per (tool, canonical version) group");
        assert_eq!(
            decoded.managed_tools[0].deployed_at,
            mediapm_utils::Timestamp::from_unix_secs(2000),
            "the surviving deploy is the newest of its group",
        );
    }

    /// Loading, re-encoding and loading again is byte-stable.
    #[test]
    fn v4_re_encode_is_stable() {
        let first = to_v4_json_value(&sample_state()).expect("encode");
        let reloaded = from_v4_json_value(first.clone()).expect("decode");
        let second = to_v4_json_value(&reloaded).expect("re-encode");
        assert_eq!(first, second, "a second round trip must produce identical JSON");
    }

    /// The wire type says which versions the module handles, so a document that
    /// claims another version is rejected rather than migrated under this shape.
    #[test]
    fn v4_accepts_only_its_own_version_marker() {
        let v3_shaped = json!({
            "version": 3,
            "managed_files": {},
            "managed_tools": [],
            "workflow_states": {}
        });
        assert!(from_v4_json_value(v3_shaped).is_err(), "a V3 marker is not a V4 document");

        let empty = json!({
            "version": 4,
            "managed_files": {},
            "managed_tools": [],
            "workflow_states": {}
        });
        let decoded = from_v4_json_value(empty).expect("a minimal V4 document must load");
        assert!(decoded.managed_files.is_empty());
        assert!(decoded.managed_tools.is_empty());
        assert!(decoded.workflow_states.is_empty());
    }
}
