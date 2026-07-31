//! V3 wire format for state persistence.
//!
//! V3 replaces the V2 BTreeMap-based managed_tools with a flat Vec.
//! V2→V3 migration is one-way forward — we never write V2 format.

use std::collections::BTreeMap;

use serde::{Deserialize, Serialize};
use serde_json::Value;

use crate::config::{ManagedFileRecord, ManagedWorkflowStepState, MediaPmState, ToolRegistryEntry};
use crate::error::MediaPmError;

/// V3 wire representation of [`MediaPmState`].
#[derive(Debug, Clone, Serialize, Deserialize)]
pub(super) struct MediaPmStateV3 {
    /// Schema version marker (always 3).
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

/// Encodes a [`MediaPmState`] as a V3 JSON [`Value`].
pub(crate) fn to_v3_json_value(state: &MediaPmState) -> Result<Value, MediaPmError> {
    let mut deduped_tools = dedup_managed_tools(state.managed_tools.clone());
    deduped_tools.sort_by(|a, b| b.deployed_at.cmp(&a.deployed_at));
    let v3 = MediaPmStateV3 {
        version: 3,
        managed_files: state.managed_files.clone(),
        managed_tools: deduped_tools,
        workflow_states: state.workflow_states.clone(),
    };

    serde_json::to_value(v3)
        .map_err(|e| MediaPmError::Serialization(format!("failed to serialize state to JSON: {e}")))
}

/// Decodes a V3 JSON [`Value`] into [`MediaPmState`].
pub(crate) fn from_v3_json_value(value: Value) -> Result<MediaPmState, MediaPmError> {
    let v3: MediaPmStateV3 = serde_json::from_value(value)
        .map_err(|e| MediaPmError::Serialization(format!("failed to decode V3 state: {e}")))?;

    let mut deduped_tools = dedup_managed_tools(v3.managed_tools);
    deduped_tools.sort_by(|a, b| b.deployed_at.cmp(&a.deployed_at));

    Ok(MediaPmState {
        version: crate::config::defaults::MEDIAPM_STATE_VERSION,
        managed_files: v3.managed_files,
        managed_tools: deduped_tools,
        workflow_states: v3.workflow_states,
    })
}

// ---------------------------------------------------------------------------
// V2→V3 bridge types
// ---------------------------------------------------------------------------

/// V2-compatible wire format for reading old state files (used by V3 bridge only).
///
/// Old V2 state files store `content_map_hash` as `Option<String>` and do NOT
/// contain a `tool_id` field inside the entry — the tool id is the BTreeMap key.
#[derive(Debug, Clone, Serialize, Deserialize)]
struct ToolRegistryEntryV2Bridge {
    pub version: String,
    #[serde(default)]
    pub canonical_version: String,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub content_map_hash: Option<String>,
    #[serde(default)]
    pub deployed_at: u64,
    #[serde(
        default,
        deserialize_with = "crate::config::custom_deserializers::deserialize_optional_nonempty_string"
    )]
    pub resolved_tag: Option<String>,
    #[serde(
        default,
        deserialize_with = "crate::config::custom_deserializers::deserialize_optional_nonempty_string"
    )]
    pub resolved_version: Option<String>,
    #[serde(
        default,
        deserialize_with = "crate::config::custom_deserializers::deserialize_optional_nonempty_string"
    )]
    pub resolved_vcs_hash: Option<String>,
}

/// V2-compatible MediaPmState for reading old state files.
#[derive(Debug, Clone, Serialize, Deserialize)]
struct MediaPmStateV2Bridge {
    pub version: u32,
    #[serde(default)]
    pub managed_files: BTreeMap<String, ManagedFileRecord>,
    #[serde(default)]
    pub managed_tools: BTreeMap<String, ToolRegistryEntryV2Bridge>,
    #[serde(default)]
    pub workflow_states: BTreeMap<String, ManagedWorkflowStepState>,
}

/// Bridges a V2 JSON [`Value`] (BTreeMap-based managed_tools) into V3
/// [`MediaPmState`].
///
/// This is needed when a V2 state file is loaded — we read it as V2 format
/// then convert to the V3 native type.
pub(crate) fn from_v2_into_v3(value: Value) -> Result<MediaPmState, MediaPmError> {
    let v2: MediaPmStateV2Bridge = serde_json::from_value(value).map_err(|e| {
        MediaPmError::Serialization(format!("failed to decode V2 state for V3 bridge: {e}"))
    })?;

    let managed_tools: Vec<ToolRegistryEntry> = v2
        .managed_tools
        .into_iter()
        .map(|(tool_id, entry)| ToolRegistryEntry {
            tool_id,
            version: entry.version,
            canonical_version: entry.canonical_version,
            content_map_hash: entry.content_map_hash.unwrap_or_default(),
            deployed_at: entry.deployed_at,
            resolved_tag: entry.resolved_tag,
            resolved_version: entry.resolved_version,
            resolved_vcs_hash: entry.resolved_vcs_hash,
        })
        .collect();

    let deduped_tools = dedup_managed_tools(managed_tools);

    Ok(MediaPmState {
        version: crate::config::defaults::MEDIAPM_STATE_VERSION,
        managed_files: v2.managed_files,
        managed_tools: deduped_tools,
        workflow_states: v2.workflow_states,
    })
}

/// Deduplicate managed_tools Vec by `(tool_id, canonical_version)`.
/// Keeps the entry with the most recent `deployed_at` for each group.
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
    use std::collections::BTreeMap;

    use serde_json::json;

    use crate::config::{ManagedFileRecord, MediaPmState, ToolRegistryEntry};

    use super::*;

    // Phase 7 — v2→v3 bridge tests
    // ---------------------------------------------------------------------------

    #[test]
    fn v3_roundtrip_preserves_all_fields() {
        let state = MediaPmState {
            version: 3,
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
                deployed_at: 1_700_000_000,
                resolved_tag: Some("v7.1".to_string()),
                resolved_version: Some("7.1".to_string()),
                resolved_vcs_hash: Some("abc".to_string()),
            }],
            workflow_states: BTreeMap::new(),
        };

        let value = to_v3_json_value(&state).expect("serialize v3");
        let decoded = from_v3_json_value(value).expect("deserialize v3");

        assert_eq!(decoded.managed_files, state.managed_files);
        assert_eq!(decoded.managed_tools.len(), 1);
        assert_eq!(decoded.managed_tools[0].tool_id, "ffmpeg");
        assert_eq!(decoded.managed_tools[0].canonical_version, "ffmpeg-v7.1");
        assert_eq!(decoded.managed_tools[0].content_map_hash, "blake3:def456");
        assert_eq!(decoded.managed_tools[0].deployed_at, 1_700_000_000);
        assert_eq!(decoded.version, 3);
    }

    #[test]
    fn v2_v3_bridge_converts_btreemap_to_vec() {
        let v2_json = json!({
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
                },
                "yt-dlp": {
                    "version": "v2",
                    "canonical_version": "yt-dlp-v2",
                    "content_map_hash": "blake3:def",
                    "deployed_at": 2000,
                    "resolved_tag": null,
                    "resolved_version": null,
                    "resolved_vcs_hash": null
                }
            },
            "workflow_states": {}
        });

        let state = from_v2_into_v3(v2_json).expect("v2→v3 bridge");
        assert_eq!(state.managed_tools.len(), 2);
        assert_eq!(state.managed_tools[0].tool_id, "ffmpeg");
        assert_eq!(state.managed_tools[1].tool_id, "yt-dlp");
        assert_eq!(state.version, 3);
    }

    #[test]
    fn v2_v3_bridge_preserves_all_entry_fields() {
        let v2_json = json!({
            "version": 2,
            "managed_files": {},
            "managed_tools": {
                "ffmpeg": {
                    "version": "7.1",
                    "canonical_version": "ffmpeg-v7.1",
                    "content_map_hash": "blake3:abc123",
                    "deployed_at": 1_700_000_000,
                    "resolved_tag": "v7.1",
                    "resolved_version": "7.1",
                    "resolved_vcs_hash": "abc123def"
                }
            },
            "workflow_states": {}
        });

        let state = from_v2_into_v3(v2_json).expect("v2→v3 bridge");
        assert_eq!(state.managed_tools.len(), 1);
        let entry = &state.managed_tools[0];
        assert_eq!(entry.tool_id, "ffmpeg");
        assert_eq!(entry.version, "7.1");
        assert_eq!(entry.canonical_version, "ffmpeg-v7.1");
        assert_eq!(entry.content_map_hash, "blake3:abc123");
        assert_eq!(entry.deployed_at, 1_700_000_000);
        assert_eq!(entry.resolved_tag.as_deref(), Some("v7.1"));
        assert_eq!(entry.resolved_version.as_deref(), Some("7.1"));
        assert_eq!(entry.resolved_vcs_hash.as_deref(), Some("abc123def"));
    }

    #[test]
    fn v2_v3_bridge_handles_empty_managed_tools() {
        let v2_json = json!({
            "version": 2,
            "managed_files": {},
            "managed_tools": {},
            "workflow_states": {}
        });

        let state = from_v2_into_v3(v2_json).expect("v2→v3 bridge with empty tools");
        assert!(state.managed_tools.is_empty());
        assert_eq!(state.version, 3);
    }

    #[test]
    fn v2_v3_bridge_handles_missing_content_map_hash() {
        let v2_json = json!({
            "version": 2,
            "managed_files": {},
            "managed_tools": {
                "ffmpeg": {
                    "version": "7.1",
                    "canonical_version": "ffmpeg-v7.1",
                    "deployed_at": 1000
                }
            },
            "workflow_states": {}
        });

        let state = from_v2_into_v3(v2_json).expect("v2→v3 bridge with missing content_map_hash");
        assert_eq!(state.managed_tools.len(), 1);
        assert_eq!(
            state.managed_tools[0].content_map_hash, "",
            "missing content_map_hash should default to empty string"
        );
    }

    #[test]
    fn v2_v3_bridge_serialized_back_as_v3() {
        let v2_json = json!({
            "version": 2,
            "managed_files": {},
            "managed_tools": {
                "ffmpeg": {
                    "version": "7.1",
                    "canonical_version": "ffmpeg-v7.1",
                    "content_map_hash": "blake3:abc",
                    "deployed_at": 1000
                }
            },
            "workflow_states": {}
        });

        let state = from_v2_into_v3(v2_json).expect("v2→v3 bridge");
        let v3_value = to_v3_json_value(&state).expect("serialize as v3");

        let obj = v3_value.as_object().expect("v3 output should be object");
        assert_eq!(obj["version"], 3, "v3 output must have version=3");
        assert!(
            obj["managed_tools"].is_array(),
            "v3 managed_tools must be an array, got {:?}",
            obj["managed_tools"]
        );
    }

    #[test]
    fn v3_reads_native_v3_format() {
        let v3_json = json!({
            "version": 3,
            "managed_files": {},
            "managed_tools": [
                {
                    "tool_id": "ffmpeg",
                    "version": "7.1",
                    "canonical_version": "ffmpeg-v7.1",
                    "content_map_hash": "blake3:abc",
                    "deployed_at": 1000
                }
            ],
            "workflow_states": {}
        });

        let state = from_v3_json_value(v3_json).expect("read native v3 format");
        assert_eq!(state.managed_tools.len(), 1);
        assert_eq!(state.managed_tools[0].tool_id, "ffmpeg");
        assert_eq!(state.version, 3);
    }

    #[test]
    fn v3_rejects_v2_btreemap_format() {
        let v2_json = json!({
            "version": 3,
            "managed_files": {},
            "managed_tools": {
                "ffmpeg": {
                    "version": "7.1",
                    "canonical_version": "ffmpeg-v7.1",
                    "content_map_hash": "blake3:abc",
                    "deployed_at": 1000
                }
            },
            "workflow_states": {}
        });

        let result = from_v3_json_value(v2_json);
        assert!(result.is_err(), "v3 reader should reject v2 BTreeMap format for managed_tools");
    }

    // Phase 10 — dedup_managed_tools tests
    // ---------------------------------------------------------------------------

    #[test]
    fn dedup_managed_tools_no_duplicates() {
        let entries = vec![ToolRegistryEntry {
            tool_id: "ffmpeg".to_string(),
            version: String::new(),
            canonical_version: "ffmpeg-v7.1".to_string(),
            content_map_hash: String::new(),
            deployed_at: 1000,
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
                deployed_at: 1000,
                resolved_tag: None,
                resolved_version: None,
                resolved_vcs_hash: None,
            },
            ToolRegistryEntry {
                tool_id: "ffmpeg".to_string(),
                version: String::new(),
                canonical_version: "ffmpeg-v7.1".to_string(),
                content_map_hash: String::new(),
                deployed_at: 2000,
                resolved_tag: None,
                resolved_version: None,
                resolved_vcs_hash: None,
            },
        ];
        let result = dedup_managed_tools(entries);
        assert_eq!(result.len(), 1);
        assert_eq!(result[0].deployed_at, 2000);
    }

    #[test]
    fn dedup_managed_tools_different_versions_kept() {
        let entries = vec![
            ToolRegistryEntry {
                tool_id: "ffmpeg".to_string(),
                version: String::new(),
                canonical_version: "ffmpeg-v7.1".to_string(),
                content_map_hash: String::new(),
                deployed_at: 1000,
                resolved_tag: None,
                resolved_version: None,
                resolved_vcs_hash: None,
            },
            ToolRegistryEntry {
                tool_id: "ffmpeg".to_string(),
                version: String::new(),
                canonical_version: "ffmpeg-v6.0".to_string(),
                content_map_hash: String::new(),
                deployed_at: 2000,
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
                deployed_at: 0,
                resolved_tag: None,
                resolved_version: None,
                resolved_vcs_hash: None,
            },
            ToolRegistryEntry {
                tool_id: "yt-dlp".to_string(),
                version: String::new(),
                canonical_version: "yt-dlp-v2".to_string(),
                content_map_hash: String::new(),
                deployed_at: 0,
                resolved_tag: None,
                resolved_version: None,
                resolved_vcs_hash: None,
            },
        ];
        let result = dedup_managed_tools(entries);
        assert_eq!(result.len(), 2);
    }

    // Phase 6 — sort by decreasing deploy_time tests
    // ---------------------------------------------------------------------------

    #[test]
    fn sort_managed_tools_by_deploy_time() {
        let entries = vec![
            ToolRegistryEntry {
                tool_id: "yt-dlp".to_string(),
                version: "v2".to_string(),
                canonical_version: "yt-dlp-v2".to_string(),
                content_map_hash: String::new(),
                deployed_at: 2000,
                resolved_tag: None,
                resolved_version: None,
                resolved_vcs_hash: None,
            },
            ToolRegistryEntry {
                tool_id: "ffmpeg".to_string(),
                version: "7.1".to_string(),
                canonical_version: "ffmpeg-v7.1".to_string(),
                content_map_hash: String::new(),
                deployed_at: 3000,
                resolved_tag: None,
                resolved_version: None,
                resolved_vcs_hash: None,
            },
            ToolRegistryEntry {
                tool_id: "media-tagger".to_string(),
                version: "1.0".to_string(),
                canonical_version: "media-tagger-v1.0".to_string(),
                content_map_hash: String::new(),
                deployed_at: 1000,
                resolved_tag: None,
                resolved_version: None,
                resolved_vcs_hash: None,
            },
        ];
        let mut sorted = entries.clone();
        sorted.sort_by(|a, b| b.deployed_at.cmp(&a.deployed_at));

        let state = MediaPmState {
            version: 3,
            managed_files: BTreeMap::new(),
            managed_tools: entries,
            workflow_states: BTreeMap::new(),
        };
        let value = to_v3_json_value(&state).expect("serialize");
        let decoded = from_v3_json_value(value).expect("deserialize");

        assert_eq!(decoded.managed_tools.len(), 3);
        assert_eq!(decoded.managed_tools[0].tool_id, "ffmpeg");
        assert_eq!(decoded.managed_tools[0].deployed_at, 3000);
        assert_eq!(decoded.managed_tools[1].tool_id, "yt-dlp");
        assert_eq!(decoded.managed_tools[1].deployed_at, 2000);
        assert_eq!(decoded.managed_tools[2].tool_id, "media-tagger");
        assert_eq!(decoded.managed_tools[2].deployed_at, 1000);
    }

    #[test]
    fn sort_managed_tools_empty() {
        let state = MediaPmState {
            version: 3,
            managed_files: BTreeMap::new(),
            managed_tools: vec![],
            workflow_states: BTreeMap::new(),
        };
        let value = to_v3_json_value(&state).expect("serialize");
        let decoded = from_v3_json_value(value).expect("deserialize");
        assert!(decoded.managed_tools.is_empty());
    }

    #[test]
    fn sort_managed_tools_same_deploy_time() {
        let entries = vec![
            ToolRegistryEntry {
                tool_id: "ffmpeg".to_string(),
                version: "7.1".to_string(),
                canonical_version: "ffmpeg-v7.1".to_string(),
                content_map_hash: String::new(),
                deployed_at: 1000,
                resolved_tag: None,
                resolved_version: None,
                resolved_vcs_hash: None,
            },
            ToolRegistryEntry {
                tool_id: "yt-dlp".to_string(),
                version: "v2".to_string(),
                canonical_version: "yt-dlp-v2".to_string(),
                content_map_hash: String::new(),
                deployed_at: 1000,
                resolved_tag: None,
                resolved_version: None,
                resolved_vcs_hash: None,
            },
        ];
        let state = MediaPmState {
            version: 3,
            managed_files: BTreeMap::new(),
            managed_tools: entries,
            workflow_states: BTreeMap::new(),
        };
        let value = to_v3_json_value(&state).expect("serialize");
        let decoded = from_v3_json_value(value).expect("deserialize");
        assert_eq!(decoded.managed_tools.len(), 2);
        // Same deploy_time — order is stable (input order preserved within same timestamp)
        assert_eq!(decoded.managed_tools[0].tool_id, "ffmpeg");
        assert_eq!(decoded.managed_tools[1].tool_id, "yt-dlp");
    }

    #[test]
    fn roundtrip_sort_preserved() {
        let entries = vec![
            ToolRegistryEntry {
                tool_id: "media-tagger".to_string(),
                version: "1.0".to_string(),
                canonical_version: "media-tagger-v1.0".to_string(),
                content_map_hash: String::new(),
                deployed_at: 1000,
                resolved_tag: None,
                resolved_version: None,
                resolved_vcs_hash: None,
            },
            ToolRegistryEntry {
                tool_id: "ffmpeg".to_string(),
                version: "7.1".to_string(),
                canonical_version: "ffmpeg-v7.1".to_string(),
                content_map_hash: String::new(),
                deployed_at: 3000,
                resolved_tag: None,
                resolved_version: None,
                resolved_vcs_hash: None,
            },
        ];
        let state = MediaPmState {
            version: 3,
            managed_files: BTreeMap::new(),
            managed_tools: entries,
            workflow_states: BTreeMap::new(),
        };
        let value = to_v3_json_value(&state).expect("serialize");
        // Round-trip: serialize → JSON → deserialize → serialize → JSON
        let decoded = from_v3_json_value(value.clone()).expect("deserialize");
        let value2 = to_v3_json_value(&decoded).expect("serialize");

        assert_eq!(value, value2, "second round-trip must produce identical JSON");
    }
}
