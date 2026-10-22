//! V3 wire format for state persistence — read-only since the V4 bump.
//!
//! V3 replaced the V2 `BTreeMap`-based `managed_tools` with a flat `Vec` and
//! made `content_map_hash` non-optional. Nothing writes V3 any more: the
//! document that carried `pending_force_rerun` is exactly the one this module
//! still has to read, so the shape stays here until no such file can exist.
//!
//! ## DO NOT REMOVE: versions policy guard
//!
//! - This file must never import unversioned structs from outside `versions/`
//!   beyond the wire field types it is declared to carry.
//! - A `vX` module may reference only the most recent previous version module,
//!   and only for version-to-version migration. The `super::v2` reference
//!   below is that single permitted edge.
//! - Latest-version bridging to unversioned runtime structs is owned by the
//!   current version's module, never by this one.
//! - Files outside `versions/` must reach versioned symbols only through
//!   `versions/mod.rs`, never through a direct `versions::vX` path.

use std::collections::{BTreeMap, BTreeSet};

use serde::{Deserialize, Serialize};
use serde_json::Value;

use super::v2::{MediaPmStateV2, ToolRegistryEntryV2};
use crate::config::{ManagedFileRecord, ManagedWorkflowStepState, ToolRegistryEntry};
use crate::error::MediaPmError;

/// V3 wire representation of [`MediaPmState`].
///
/// Read-only: V4 is the only format written, and this shape exists so a
/// document written before the bump still loads. `pending_force_rerun` is part
/// of it because V3 carried it; V4 removed the field, and the migration drops
/// it rather than pretending the queue never existed.
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
    /// Workflows whose next run had to ignore the instance cache.
    ///
    /// V3's only writer was the destructive `media invalidate`, which has since
    /// become a marker on the recorded instances. Read and discarded: a queued
    /// name is a request whose mechanism no longer exists, and honouring it by
    /// force-rerunning the whole workflow would be a different, wider action
    /// than the operator's.
    #[serde(default, skip_serializing_if = "BTreeSet::is_empty")]
    pub(super) pending_force_rerun: BTreeSet<String>,
}

/// Decodes a V3 JSON [`Value`] into the canonical [`MediaPmStateV3`] wire type.
///
/// Read-only: the resolution into the runtime model belongs to V4, which owns
/// the current shape, so that no runtime field is declared in two versions.
///
/// # Errors
///
/// Returns [`MediaPmError::Serialization`] when the value does not match the V3
/// wire shape.
pub(super) fn decode_v3(value: Value) -> Result<MediaPmStateV3, MediaPmError> {
    serde_json::from_value(value)
        .map_err(|e| MediaPmError::Serialization(format!("failed to decode V3 state: {e}")))
}

/// Advances a V2 JSON [`Value`] into the canonical [`MediaPmStateV3`] wire shape.
///
/// The path is deliberately `Value` → [`super::v2::MediaPmStateV2`] →
/// [`MediaPmStateV3`], so V2's shape is stated exactly once (in [`super::v2`]).
/// Resolution into the runtime model belongs to the current version's module.
///
/// # Errors
///
/// Returns [`MediaPmError::Serialization`] when the value is not a valid V2
/// document.
pub(super) fn decode_v2_as_v3(value: Value) -> Result<MediaPmStateV3, MediaPmError> {
    Ok(advance_v2_to_v3(super::v2::from_v2_json_value(value)?))
}

/// Advances one canonical V2 wire value into the V3 wire shape.
///
/// This is the version-to-version isomorphism: the `managed_tools` map key
/// becomes the entry's `tool_id`, the nullable V2 `content_map_hash` becomes
/// the V3 plain field (missing maps to the empty string, matching the V3
/// field's documented default), and the epoch-seconds `deployed_at` becomes a
/// [`mediapm_utils::Timestamp`].
fn advance_v2_to_v3(v2: MediaPmStateV2) -> MediaPmStateV3 {
    let managed_tools: Vec<ToolRegistryEntry> = v2
        .managed_tools
        .into_iter()
        .map(|(tool_id, entry)| tool_entry_from_v2(tool_id, entry))
        .collect();

    MediaPmStateV3 {
        version: 3,
        managed_files: v2.managed_files,
        managed_tools,
        workflow_states: v2.workflow_states,
        // A V2 document predates the queue, so a migrated document has none.
        pending_force_rerun: BTreeSet::new(),
    }
}

/// Converts one canonical V2 tool entry into its V3 counterpart.
fn tool_entry_from_v2(tool_id: String, entry: ToolRegistryEntryV2) -> ToolRegistryEntry {
    ToolRegistryEntry {
        tool_id,
        version: entry.version,
        canonical_version: entry.canonical_version,
        content_map_hash: entry.content_map_hash.unwrap_or_default(),
        deployed_at: mediapm_utils::Timestamp::from_unix_secs(entry.deployed_at),
        resolved_tag: entry.resolved_tag,
        resolved_version: entry.resolved_version,
        resolved_vcs_hash: entry.resolved_vcs_hash,
    }
}

/// Deduplicate `managed_tools` Vec by `(tool_id, canonical_version)`.
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
    use serde_json::json;

    use crate::config::ToolRegistryEntry;

    use super::*;

    /// A V3 document decodes into the V3 wire type with every field intact.
    ///
    /// V3 is read-only now, so "reads correctly" is the whole of its contract;
    /// what the runtime model does with these fields is V4's business.
    #[test]
    fn decode_v3_reads_the_native_v3_shape() {
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

        let state = decode_v3(v3_json).expect("read native v3 format");
        assert_eq!(state.version, 3);
        assert_eq!(state.managed_tools.len(), 1);
        assert_eq!(state.managed_tools[0].tool_id, "ffmpeg");
        assert!(
            state.pending_force_rerun.is_empty(),
            "a document without the queue decodes to an empty one, not an error",
        );
    }

    /// The V2 shape is rejected by the V3 reader, so a mislabelled document is
    /// reported instead of being parsed under a shape it does not have.
    #[test]
    fn decode_v3_rejects_the_v2_btreemap_shape() {
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

        assert!(decode_v3(v2_json).is_err(), "the v3 reader must reject the v2 map shape");
    }

    /// The V2 → V3 advance states the map-key-to-`tool_id` move and leaves the
    /// entries in the map's own order: sorting is a resolution rule and belongs
    /// to the current version.
    #[test]
    fn decode_v2_as_v3_moves_the_map_key_into_the_entry() {
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

        let state = decode_v2_as_v3(v2_json).expect("v2→v3 advance");
        assert_eq!(state.version, 3);
        assert_eq!(state.managed_tools.len(), 2);
        assert_eq!(state.managed_tools[0].tool_id, "ffmpeg");
        assert_eq!(state.managed_tools[1].tool_id, "yt-dlp");
        assert!(
            state.pending_force_rerun.is_empty(),
            "a V2 document predates the queue and cannot carry one",
        );
    }

    /// Every entry field survives the V2 → V3 advance, including the nullable
    /// `content_map_hash` that becomes a plain string.
    #[test]
    fn decode_v2_as_v3_preserves_every_entry_field() {
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

        let state = decode_v2_as_v3(v2_json).expect("v2→v3 advance");
        let entry = &state.managed_tools[0];
        assert_eq!(entry.tool_id, "ffmpeg");
        assert_eq!(entry.version, "7.1");
        assert_eq!(entry.canonical_version, "ffmpeg-v7.1");
        assert_eq!(entry.content_map_hash, "blake3:abc123");
        assert_eq!(entry.deployed_at, mediapm_utils::Timestamp::from_unix_secs(1_700_000_000));
        assert_eq!(entry.resolved_tag.as_deref(), Some("v7.1"));
        assert_eq!(entry.resolved_version.as_deref(), Some("7.1"));
        assert_eq!(entry.resolved_vcs_hash.as_deref(), Some("abc123def"));
    }

    /// An empty V2 tool map advances to an empty V3 list.
    #[test]
    fn decode_v2_as_v3_handles_empty_managed_tools() {
        let v2_json = json!({
            "version": 2,
            "managed_files": {},
            "managed_tools": {},
            "workflow_states": {}
        });

        let state = decode_v2_as_v3(v2_json).expect("v2→v3 advance with empty tools");
        assert!(state.managed_tools.is_empty());
        assert_eq!(state.version, 3);
    }

    /// A V2 entry with no `content_map_hash` advances to the empty string, the
    /// V3 field's documented default.
    #[test]
    fn decode_v2_as_v3_defaults_a_missing_content_map_hash() {
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

        let state = decode_v2_as_v3(v2_json).expect("v2→v3 advance without a content map hash");
        assert_eq!(state.managed_tools[0].content_map_hash, "");
    }

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
