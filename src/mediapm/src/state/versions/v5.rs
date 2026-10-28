//! V5 wire format for state persistence — the format every write emits.
//!
//! V5 is V4 without `workflow_states`. The map recorded, per media, which
//! variants its workflow had produced — a second copy of what the conductor
//! already records per tool-call instance, and one that could disagree with it.
//! The copy nothing consulted is the one that had to go: a materialized variant
//! keeps its identity in [`ManagedFileRecord`], and a captured variant the
//! library does not materialize keeps it in the conductor's instance records.
//!
//! A V4 document still loads. Its workflow map is dropped rather than migrated,
//! because there is nothing to migrate it into and no reader that would consult
//! it.
//!
//! ## DO NOT REMOVE: versions policy guard
//!
//! - This file must never import unversioned structs from outside `versions/`
//!   beyond the runtime model it is declared to bridge and the wire field types
//!   it carries.
//! - A `vX` module may reference only the most recent previous version module,
//!   and only for version-to-version migration. The `super::v4` reference below
//!   is that single permitted edge.
//! - Latest-version bridging to unversioned runtime structs is owned here, so
//!   `versions/mod.rs` can compose the ladder without naming a wire shape.
//! - Files outside `versions/` must reach versioned symbols only through
//!   `versions/mod.rs`, never through a direct `versions::vX` path.

use std::collections::BTreeMap;

use serde::{Deserialize, Serialize};
use serde_json::Value;

use super::v4::MediaPmStateV4;
use crate::config::{ManagedFileRecord, MediaPmState, ToolRegistryEntry};
use crate::error::MediaPmError;

/// V5 wire representation of [`MediaPmState`].
///
/// Closed: an unknown field is an error rather than being dropped, so a document
/// written by a newer mediapm is reported instead of being read as if the extra
/// field were not there. The closed shape is also what refuses a V4 body
/// mislabelled as V5: its workflow map is unknown here, so it fails to decode
/// instead of having that data silently dropped.
#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub(super) struct MediaPmStateV5 {
    /// Schema version marker (always 5).
    pub(super) version: u32,
    /// Managed files keyed by filesystem path.
    #[serde(default)]
    pub(super) managed_files: BTreeMap<String, ManagedFileRecord>,
    /// Managed tool deployment metadata (flat list).
    #[serde(default)]
    pub(super) managed_tools: Vec<ToolRegistryEntry>,
}

/// Decodes a V5 JSON [`Value`] into the canonical [`MediaPmStateV5`] wire type.
///
/// The `version` marker is checked against this module's own number. V5's shape
/// is a subset of V4's — every V5 field exists in V4 — so a document that
/// labelled itself V4 would otherwise decode here without complaint, which is
/// exactly the silent mis-parse the dispatch exists to prevent.
///
/// # Errors
///
/// Returns [`MediaPmError::Serialization`] when the value does not match the V5
/// wire shape, when it carries a field the shape does not have (a V4 workflow
/// map included), or when its `version` marker is not 5.
pub(super) fn decode_v5(value: Value) -> Result<MediaPmStateV5, MediaPmError> {
    let v5: MediaPmStateV5 = serde_json::from_value(value)
        .map_err(|e| MediaPmError::Serialization(format!("failed to decode V5 state: {e}")))?;
    if v5.version != 5 {
        return Err(MediaPmError::Serialization(format!(
            "state document is marked version {} but was decoded as V5",
            v5.version,
        )));
    }
    Ok(v5)
}

/// Advances a canonical V4 wire value into the V5 wire shape.
///
/// The workflow map is dropped, not translated: V5 has no place to keep it and
/// no reader that would consult it. Everything else moves through unchanged, so
/// a migrated document is ordered and deduplicated exactly like a native one
/// once [`resolve_v5`] runs.
pub(super) fn from_v4_into_v5(v4: MediaPmStateV4) -> MediaPmStateV5 {
    let MediaPmStateV4 {
        version: _,
        managed_files,
        managed_tools,
        workflow_states: _dropped_workflow_states,
    } = v4;

    MediaPmStateV5 { version: 5, managed_files, managed_tools }
}

/// Resolves a decoded [`MediaPmStateV5`] into the unversioned runtime model.
///
/// The resolved list keeps one entry per `(tool_id, canonical_version)` group —
/// the newest deploy — and orders it newest first, so a migrated state and a
/// freshly written one are indistinguishable downstream. The runtime marker is
/// stamped from the constant rather than echoed from the wire.
pub(super) fn resolve_v5(v5: MediaPmStateV5) -> MediaPmState {
    let mut managed_tools = dedup_managed_tools(v5.managed_tools);
    managed_tools.sort_by_key(|entry| std::cmp::Reverse(entry.deployed_at));

    MediaPmState {
        version: crate::config::defaults::MEDIAPM_STATE_VERSION,
        managed_files: v5.managed_files,
        managed_tools,
    }
}

/// Encodes a [`MediaPmState`] as a V5 JSON [`Value`].
///
/// # Errors
///
/// Returns [`MediaPmError::Serialization`] if serialization fails.
pub(super) fn to_v5_json_value(state: &MediaPmState) -> Result<Value, MediaPmError> {
    let mut managed_tools = dedup_managed_tools(state.managed_tools.clone());
    managed_tools.sort_by_key(|entry| std::cmp::Reverse(entry.deployed_at));
    let v5 =
        MediaPmStateV5 { version: 5, managed_files: state.managed_files.clone(), managed_tools };

    serde_json::to_value(v5)
        .map_err(|e| MediaPmError::Serialization(format!("failed to serialize state to JSON: {e}")))
}

/// Decodes a V5 JSON [`Value`] into the runtime model.
///
/// # Errors
///
/// Returns the failures [`decode_v5`] reports.
pub(super) fn from_v5_json_value(value: Value) -> Result<MediaPmState, MediaPmError> {
    Ok(resolve_v5(decode_v5(value)?))
}

/// Deduplicates a `managed_tools` list by `(tool_id, canonical_version)`.
///
/// Keeps the entry with the most recent `deployed_at` for each group, so a state
/// file that accumulated several deploys of one version resolves to the newest
/// of them rather than to whichever came last in the file.
pub(crate) fn dedup_managed_tools(entries: Vec<ToolRegistryEntry>) -> Vec<ToolRegistryEntry> {
    let mut seen: BTreeMap<(String, String), ToolRegistryEntry> = BTreeMap::new();
    for entry in entries {
        let key = (entry.tool_id.clone(), entry.canonical_version.clone());
        match seen.entry(key) {
            std::collections::btree_map::Entry::Occupied(mut o) => {
                if entry.deployed_at > o.get().deployed_at {
                    o.insert(entry);
                }
            }
            std::collections::btree_map::Entry::Vacant(v) => {
                v.insert(entry);
            }
        }
    }
    seen.into_values().collect()
}

#[cfg(test)]
mod tests {
    use serde_json::json;

    use super::super::v3::decode_v3;
    use super::super::v4::{decode_v4, from_v3_into_v4};
    use crate::config::{ManagedFileRecord, MediaPmState};

    use super::*;

    /// Builds a runtime state holding one managed file and one tool entry.
    fn sample_state() -> MediaPmState {
        MediaPmState {
            version: 5,
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
        }
    }

    /// A runtime state round-trips through the V5 wire with no field lost, and
    /// the encoded document is marked version 5.
    #[test]
    fn v5_round_trip_preserves_every_field() {
        let state = sample_state();

        let value = to_v5_json_value(&state).expect("encode v5");
        assert_eq!(value["version"], json!(5));
        let decoded = from_v5_json_value(value).expect("decode v5");

        assert_eq!(decoded.version, 5);
        assert_eq!(decoded.managed_files, state.managed_files);
        assert_eq!(decoded.managed_tools, state.managed_tools);
    }

    /// The deleted workflow map is not written back.
    ///
    /// A document that carried one in a previous version must not resurrect the
    /// field on the way out: the conductor records the same facts per instance,
    /// and a second copy is a second answer.
    #[test]
    fn v5_never_writes_the_workflow_map() {
        let value = to_v5_json_value(&sample_state()).expect("encode v5");
        assert!(
            value.get("workflow_states").is_none(),
            "V5 has no workflow map, so the encoded document must not carry one: {value}",
        );
    }

    /// A V4 body mislabelled as V5 is refused rather than read with its map dropped.
    ///
    /// The marker is what identifies the shape, so a document that claims V5 and
    /// still carries the V4 map is not a V5 document: reading it would discard
    /// data the marker says is not there.
    #[test]
    fn v5_rejects_a_workflow_map() {
        let mislabelled = json!({
            "version": 5,
            "managed_files": {},
            "managed_tools": [],
            "workflow_states": { "a": { "variant_hashes": {}, "steps_completed": 1 } }
        });

        let error = decode_v5(mislabelled).expect_err("a V5 document carries no workflow map");
        assert!(
            error.to_string().contains("workflow_states"),
            "the diagnostic must name the rejected field: {error}",
        );
    }

    /// A V4 document loads, and its workflow map is dropped rather than carried.
    #[test]
    fn from_v4_into_v5_drops_the_workflow_map_and_keeps_everything_else() {
        let v4_json = json!({
            "version": 4,
            "managed_files": {
                "/media/a.mkv": { "media_id": "a", "variant": "primary", "hash": "blake3:a" }
            },
            "managed_tools": [],
            "workflow_states": {
                "a": { "variant_hashes": { "primary": "blake3:p" }, "steps_completed": 2 }
            }
        });

        let state =
            resolve_v5(from_v4_into_v5(decode_v4(v4_json).expect("decode the v4 document")));

        assert_eq!(state.version, 5);
        assert_eq!(state.managed_files.len(), 1);
        let written = to_v5_json_value(&state).expect("re-encode");
        assert!(
            written.get("workflow_states").is_none(),
            "the dropped map must not reappear on write: {written}",
        );
    }

    /// A V3 document loads, and its queue and workflow map are both dropped.
    #[test]
    fn from_v3_drops_the_queue_and_the_workflow_map() {
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

        let state = resolve_v5(from_v4_into_v5(from_v3_into_v4(
            decode_v3(v3_json).expect("decode the v3 document"),
        )));

        assert_eq!(state.version, 5);
        assert_eq!(state.managed_files.len(), 1);
        let written = to_v5_json_value(&state).expect("re-encode");
        assert_eq!(written.get("pending_force_rerun"), None, "the dropped queue must not reappear");
        assert_eq!(written.get("workflow_states"), None, "the dropped map must not reappear");
    }

    /// An unknown field is an error, not something to skip.
    #[test]
    fn v5_rejects_an_unknown_field() {
        let document =
            json!({ "version": 5, "managed_files": {}, "managed_tools": [], "bogus": 1 });

        let error = decode_v5(document).expect_err("an unknown field must be rejected");
        assert!(
            error.to_string().contains("bogus"),
            "the diagnostic must name the rejected field: {error}",
        );
    }

    /// The resolved tool list is ordered newest deploy first, which is the order
    /// the "active tool" lookup relies on.
    #[test]
    fn resolve_v5_orders_tools_newest_first() {
        let tool = |tool_id: &str, deployed_at: u64| ToolRegistryEntry {
            tool_id: tool_id.to_string(),
            version: "1.0".to_string(),
            canonical_version: format!("{tool_id}-v1.0"),
            content_map_hash: String::new(),
            deployed_at: mediapm_utils::Timestamp::from_unix_secs(deployed_at),
            resolved_tag: None,
            resolved_version: None,
            resolved_vcs_hash: None,
        };
        let state = MediaPmState {
            managed_tools: vec![
                tool("media-tagger", 1000),
                tool("ffmpeg", 3000),
                tool("yt-dlp", 2000),
            ],
            ..sample_state()
        };

        let value = to_v5_json_value(&state).expect("encode v5");
        let decoded = from_v5_json_value(value).expect("decode v5");

        let ids: Vec<&str> = decoded.managed_tools.iter().map(|e| e.tool_id.as_str()).collect();
        assert_eq!(ids, ["ffmpeg", "yt-dlp", "media-tagger"]);
    }

    /// Two deploys of the same tool at the same canonical version resolve to the
    /// newer one; a different version is kept beside it.
    #[test]
    fn resolve_v5_dedups_by_tool_and_canonical_version() {
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

        let value = to_v5_json_value(&state).expect("encode v5");
        let decoded = from_v5_json_value(value).expect("decode v5");

        assert_eq!(decoded.managed_tools.len(), 2, "one entry per (tool, canonical version) group");
        assert_eq!(
            decoded.managed_tools[0].deployed_at,
            mediapm_utils::Timestamp::from_unix_secs(2000),
            "the surviving deploy is the newest of its group",
        );
    }

    /// Loading, re-encoding and loading again is byte-stable.
    #[test]
    fn v5_re_encode_is_stable() {
        let first = to_v5_json_value(&sample_state()).expect("encode");
        let reloaded = from_v5_json_value(first.clone()).expect("decode");
        let second = to_v5_json_value(&reloaded).expect("re-encode");
        assert_eq!(first, second, "a second round trip must produce identical JSON");
    }

    /// The wire type says which version the module handles, so a document that
    /// claims another version is rejected rather than migrated under this shape.
    #[test]
    fn v5_accepts_only_its_own_version_marker() {
        let v4_shaped = json!({
            "version": 4,
            "managed_files": {},
            "managed_tools": []
        });
        assert!(decode_v5(v4_shaped).is_err(), "a V4 marker is not a V5 document");

        let empty = json!({ "version": 5, "managed_files": {}, "managed_tools": [] });
        let decoded = from_v5_json_value(empty).expect("a minimal V5 document must load");
        assert!(decoded.managed_files.is_empty());
        assert!(decoded.managed_tools.is_empty());
    }

    /// A single entry is returned unchanged: deduplication groups by
    /// `(tool_id, canonical_version)` and one entry is one group.
    #[test]
    fn dedup_managed_tools_no_duplicates() {
        let entries = vec![ToolRegistryEntry {
            tool_id: "ffmpeg".to_string(),
            version: String::new(),
            canonical_version: "ffmpeg-v7.1".to_string(),
            content_map_hash: String::new(),
            deployed_at: mediapm_utils::Timestamp::from_unix_secs(1000),
            resolved_tag: None,
            resolved_version: None,
            resolved_vcs_hash: None,
        }];
        let result = dedup_managed_tools(entries);
        assert_eq!(result.len(), 1);
    }

    #[test]
    fn dedup_managed_tools_keeps_newest() {
        let entries = vec![
            ToolRegistryEntry {
                tool_id: "ffmpeg".to_string(),
                version: String::new(),
                canonical_version: "ffmpeg-v7.1".to_string(),
                content_map_hash: String::new(),
                deployed_at: mediapm_utils::Timestamp::from_unix_secs(1000),
                resolved_tag: None,
                resolved_version: None,
                resolved_vcs_hash: None,
            },
            ToolRegistryEntry {
                tool_id: "ffmpeg".to_string(),
                version: String::new(),
                canonical_version: "ffmpeg-v7.1".to_string(),
                content_map_hash: String::new(),
                deployed_at: mediapm_utils::Timestamp::from_unix_secs(2000),
                resolved_tag: None,
                resolved_version: None,
                resolved_vcs_hash: None,
            },
        ];
        let result = dedup_managed_tools(entries);
        assert_eq!(result.len(), 1);
        assert_eq!(result[0].deployed_at, mediapm_utils::Timestamp::from_unix_secs(2000));
    }

    #[test]
    fn dedup_managed_tools_different_versions_kept() {
        let entries = vec![
            ToolRegistryEntry {
                tool_id: "ffmpeg".to_string(),
                version: String::new(),
                canonical_version: "ffmpeg-v7.1".to_string(),
                content_map_hash: String::new(),
                deployed_at: mediapm_utils::Timestamp::from_unix_secs(1000),
                resolved_tag: None,
                resolved_version: None,
                resolved_vcs_hash: None,
            },
            ToolRegistryEntry {
                tool_id: "ffmpeg".to_string(),
                version: String::new(),
                canonical_version: "ffmpeg-v6.0".to_string(),
                content_map_hash: String::new(),
                deployed_at: mediapm_utils::Timestamp::from_unix_secs(2000),
                resolved_tag: None,
                resolved_version: None,
                resolved_vcs_hash: None,
            },
        ];
        let result = dedup_managed_tools(entries);
        assert_eq!(result.len(), 2);
    }

    #[test]
    fn dedup_managed_tools_different_tools_kept() {
        let entries = vec![
            ToolRegistryEntry {
                tool_id: "ffmpeg".to_string(),
                version: String::new(),
                canonical_version: "ffmpeg-v7.1".to_string(),
                content_map_hash: String::new(),
                deployed_at: mediapm_utils::Timestamp::default(),
                resolved_tag: None,
                resolved_version: None,
                resolved_vcs_hash: None,
            },
            ToolRegistryEntry {
                tool_id: "yt-dlp".to_string(),
                version: String::new(),
                canonical_version: "yt-dlp-v2".to_string(),
                content_map_hash: String::new(),
                deployed_at: mediapm_utils::Timestamp::default(),
                resolved_tag: None,
                resolved_version: None,
                resolved_vcs_hash: None,
            },
        ];
        let result = dedup_managed_tools(entries);
        assert_eq!(result.len(), 2);
    }
}
