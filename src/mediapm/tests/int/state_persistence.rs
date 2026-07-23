//! # State persistence tests
//!
//! Tests for [`MediaPmState`] JSON serialization, deserialization, version
//! dispatch, and migration from legacy formats.
//!
//! These tests verify:
//! - V2 round-trip: `to_json_value` + `from_json_value` produces the same state
//! - V1→V2 migration: pre-rewrite wrapper format converts correctly
//! - Flat→V2 migration: post-rewrite flat format converts correctly
//! - File-level migration: `state.ncl` → `state.json` via
//!   `load_mediapm_state_document`
//! - Idempotency: re-saving produces identical bytes
//!
//! Does NOT test the full tool-sync pipeline — see `tool_sync.rs` for that.

use std::collections::BTreeMap;

use mediapm::{
    ManagedFileRecord, MediaPmState, ToolRegistryEntry, load_mediapm_state_document,
    state::ser::{from_json_value, migrate_from_old_nickel, to_json_value},
};
use serde_json::json;
use tempfile::tempdir;

// ---------------------------------------------------------------------------
// V2 round-trip
// ---------------------------------------------------------------------------

#[test]
fn v2_round_trip() {
    let state = MediaPmState {
        version: 2,
        managed_files: BTreeMap::from([(
            "/media/file.mp4".to_string(),
            ManagedFileRecord {
                media_id: "test-source".to_string(),
                variant: "primary".to_string(),
                hash: "blake3:abc123".to_string(),
            },
        )]),
        managed_tools: BTreeMap::from([(
            "yt-dlp".to_string(),
            ToolRegistryEntry {
                tag: None,
                canonical_version: String::new(),
                fetch_hash: Some("blake3:def456".to_string()),
                deployed_at: 1_700_000_000,
            },
        )]),
        workflow_states: BTreeMap::new(),
    };

    let value = to_json_value(&state).expect("serialization must succeed");
    let decoded = from_json_value(value).expect("deserialization must succeed");

    assert_eq!(state.managed_files, decoded.managed_files);
    assert_eq!(state.managed_tools, decoded.managed_tools);
    assert_eq!(state.workflow_states, decoded.workflow_states);
    assert_eq!(decoded.version, 2);
}

// ---------------------------------------------------------------------------
// V1→V2 migration (pre-rewrite wrapper format)
// ---------------------------------------------------------------------------

#[test]
fn migrate_v1_wrapper_to_v2() {
    let v1_json = json!({
        "version": 1,
        "state": {
            "managed_files": {
                "path/to/file.mp4": {
                    "media_id": "source-1",
                    "variant": "primary",
                    "hash": "blake3:abc"
                }
            },
            "tool_registry": {
                "ffmpeg": {
                    "name": "ffmpeg",
                    "version": "6.0",
                    "source": "github",
                    "registry_multihash": "Qmhash",
                    "last_transition_unix_seconds": 1_600_000_000
                }
            },
            "active_tools": {
                "ffmpeg": "ffmpeg@Qmhash"
            },
            "workflow_states": {
                "source-1": [
                    {
                        "variant_hashes": { "primary": "blake3:abc" },
                        "steps_completed": 3,
                        "last_impure_sync_at": null
                    }
                ]
            },
            "last_materialized_state_hash": "blake3:oldhash"
        }
    });

    let state = migrate_from_old_nickel(v1_json).expect("migration must succeed");

    // V1 wrapper fields dropped
    assert!(state.managed_tools.is_empty(), "tool_registry should be dropped");
    // managed_files preserved
    assert_eq!(state.managed_files.len(), 1);
    assert_eq!(state.managed_files.get("path/to/file.mp4").unwrap().hash, "blake3:abc");
    // workflow_states converted from Vec<T> to T (last entry)
    assert_eq!(state.workflow_states.len(), 1);
    let ws = &state.workflow_states["source-1"];
    assert_eq!(ws.steps_completed, 3);
    assert_eq!(ws.variant_hashes.get("primary").unwrap(), "blake3:abc");
}

// ---------------------------------------------------------------------------
// Flat→V2 migration (post-rewrite flat format)
// ---------------------------------------------------------------------------

#[test]
fn migrate_flat_to_v2() {
    let flat_json = json!({
        "version": 1,
        "managed_files": [
            "path/to/file.mp4"
        ],
        "workflow_states": {
            "source-1": {
                "variant_hashes": { "primary": "blake3:abc" },
                "steps_completed": 3,
                "last_impure_sync_at": null
            }
        }
    });

    let state = migrate_from_old_nickel(flat_json).expect("migration must succeed");

    // managed_files converted from BTreeSet to BTreeMap
    assert_eq!(state.managed_files.len(), 1);
    let record = &state.managed_files["path/to/file.mp4"];
    assert_eq!(record.media_id, "");
    assert_eq!(record.variant, "");
    assert_eq!(record.hash, "path/to/file.mp4");

    // workflow_states at top level
    assert_eq!(state.workflow_states.len(), 1);
    let ws = &state.workflow_states["source-1"];
    assert_eq!(ws.steps_completed, 3);
}

// ---------------------------------------------------------------------------
// Default state
// ---------------------------------------------------------------------------

#[test]
fn default_state_round_trip() {
    let state = MediaPmState::default();
    assert_eq!(state.version, 2);

    let value = to_json_value(&state).expect("serialization must succeed");
    let decoded = from_json_value(value).expect("deserialization must succeed");

    assert!(decoded.managed_files.is_empty());
    assert!(decoded.managed_tools.is_empty());
    assert!(decoded.workflow_states.is_empty());
    assert_eq!(decoded.version, 2);
}

// ---------------------------------------------------------------------------
// V2 rejects unsupported versions
// ---------------------------------------------------------------------------

#[test]
fn reject_unsupported_version() {
    let bad_json = json!({ "version": 99, "managed_files": {} });
    let result = from_json_value(bad_json);
    assert!(result.is_err(), "unsupported version should be rejected");
}

// ---------------------------------------------------------------------------
// JSON file idempotency
// ---------------------------------------------------------------------------

#[test]
fn json_save_idempotent() {
    let state = MediaPmState {
        version: 2,
        managed_files: BTreeMap::from([(
            "file.mp4".to_string(),
            ManagedFileRecord {
                media_id: "src".to_string(),
                variant: "primary".to_string(),
                hash: "blake3:x".to_string(),
            },
        )]),
        managed_tools: BTreeMap::from([(
            "tool-a".to_string(),
            ToolRegistryEntry {
                tag: None,
                canonical_version: String::new(),
                fetch_hash: Some("blake3:y".to_string()),
                deployed_at: 1_700_000_000,
            },
        )]),
        workflow_states: BTreeMap::new(),
    };

    let value1 = to_json_value(&state).expect("first serialization");
    let json1 = serde_json::to_string_pretty(&value1).expect("first pretty-print");

    let value2 = to_json_value(&state).expect("second serialization");
    let json2 = serde_json::to_string_pretty(&value2).expect("second pretty-print");

    assert_eq!(json1, json2, "serialization must be deterministic");
}

// ---------------------------------------------------------------------------
// File-level migration: state.ncl → state.json
// ---------------------------------------------------------------------------

#[test]
fn ncl_to_json_file_migration() {
    let dir = tempdir().expect("tempdir");
    let json_path = dir.path().join("state.json");
    let ncl_path = dir.path().join("state.ncl");

    // Create a minimal state.ncl with the flat format
    let ncl_content = r#"{
  version = 1,
  managed_files = [],
  workflow_states = {},
}
"#;
    std::fs::write(&ncl_path, ncl_content).expect("write state.ncl");

    // Load should migrate .ncl → .json and delete .ncl
    let state = load_mediapm_state_document(&json_path).expect("load should migrate from .ncl");

    assert!(json_path.exists(), "state.json should exist");
    assert!(!ncl_path.exists(), "state.ncl should be deleted after migration");
    assert_eq!(state.version, 2);
    assert!(state.managed_files.is_empty());
    assert!(state.workflow_states.is_empty());
}

#[test]
fn load_missing_state_returns_default() {
    let dir = tempdir().expect("tempdir");
    let json_path = dir.path().join("state.json");

    let state = load_mediapm_state_document(&json_path).expect("load should return default");
    assert_eq!(state.version, 2);
    assert!(state.managed_files.is_empty());
    assert!(state.managed_tools.is_empty());
}

// ---------------------------------------------------------------------------
// ToolRegistryEntry serde with canonical_version
// ---------------------------------------------------------------------------

#[test]
fn tool_registry_entry_round_trip() {
    let entry = ToolRegistryEntry {
        tag: None,
        canonical_version: "abc123".to_string(),
        fetch_hash: None,
        deployed_at: 0,
    };
    let json = serde_json::to_value(&entry).expect("serialize");
    let back: ToolRegistryEntry = serde_json::from_value(json).expect("deserialize");
    assert_eq!(back.canonical_version, "abc123");
}

#[test]
fn tool_registry_entry_backward_compat_deserialize_without_canonical_version() {
    let json = serde_json::json!({
        "version": "1.0",
        "fetch_hash": "blake3:x",
        "deployed_at": 0
    });
    let entry: ToolRegistryEntry = serde_json::from_value(json).expect("deserialize old format");
    assert_eq!(
        entry.canonical_version, "",
        "missing canonical_version should default to empty string"
    );
}

#[test]
fn tool_registry_entry_normalize_drops_blank_entry() {
    let mut state = MediaPmState::default();
    state.managed_tools.insert(
        "tool".to_string(),
        ToolRegistryEntry {
            tag: Some(String::new()),
            canonical_version: "".to_string(),
            fetch_hash: None,
            deployed_at: 0,
        },
    );
    state.normalize();
    assert!(state.managed_tools.is_empty(), "blank entry should be dropped");
}

#[test]
fn tool_registry_entry_normalize_keeps_entry_with_only_canonical_version() {
    let mut state = MediaPmState::default();
    state.managed_tools.insert(
        "tool".to_string(),
        ToolRegistryEntry {
            tag: None,
            canonical_version: "abc123".to_string(),
            fetch_hash: None,
            deployed_at: 0,
        },
    );
    state.normalize();
    assert_eq!(state.managed_tools.len(), 1, "entry with canonical_version should survive");
}

#[test]
fn state_normalize_retains_tool_with_canonical_version() {
    let mut state = MediaPmState::default();
    state.managed_tools.insert(
        "media-tagger".to_string(),
        ToolRegistryEntry {
            tag: None,
            canonical_version: "abc123".to_string(),
            fetch_hash: None,
            deployed_at: 0,
        },
    );
    state.normalize();
    assert!(
        state.managed_tools.contains_key("media-tagger"),
        "tool with canonical_version should be retained"
    );
}

#[test]
fn state_normalize_drops_tool_with_all_blank() {
    let mut state = MediaPmState::default();
    state.managed_tools.insert(
        "blank-tool".to_string(),
        ToolRegistryEntry {
            tag: Some(String::new()),
            canonical_version: "".to_string(),
            fetch_hash: None,
            deployed_at: 0,
        },
    );
    state.normalize();
    assert!(!state.managed_tools.contains_key("blank-tool"), "blank tool should be dropped");
}

#[test]
fn canonical_version_json_round_trip() {
    let long = "a".repeat(64);
    let versions = vec!["", "abc123", "v1.0.0", "2025.07.15", "L2025-07-15", &long];
    for v in &versions {
        let entry = ToolRegistryEntry {
            tag: None,
            canonical_version: (*v).to_string(),
            fetch_hash: None,
            deployed_at: 0,
        };
        let json = serde_json::to_value(&entry).unwrap();
        let back: ToolRegistryEntry = serde_json::from_value(json).unwrap();
        assert_eq!(back.canonical_version, *v, "canonical_version round-trip failed for {:?}", v);
    }
}

#[test]
fn tool_registry_entry_serialization_omits_version_field() {
    let entry = ToolRegistryEntry {
        tag: None,
        canonical_version: "v1.0.0".to_string(),
        fetch_hash: None,
        deployed_at: 0,
    };
    let json = serde_json::to_value(&entry).unwrap();
    let map = json.as_object().expect("ToolRegistryEntry should serialize to a JSON object");
    assert!(
        !map.contains_key("version"),
        "serialized ToolRegistryEntry must NOT contain a 'version' field"
    );
}

#[test]
fn tool_registry_entry_backward_compat_ignores_unknown_version_field() {
    let json = serde_json::json!({
        "tag": null,
        "canonical_version": "v1.0.0",
        "fetch_hash": null,
        "deployed_at": 0,
        "version": "v1.0.0"
    });
    let entry: ToolRegistryEntry =
        serde_json::from_value(json).expect("should deserialize even with unknown 'version' field");
    assert_eq!(entry.canonical_version, "v1.0.0");
}

#[test]
fn tool_registry_entry_backward_compat_ignores_version_with_canonical_null() {
    let json = serde_json::json!({
        "tag": null,
        "canonical_version": "",
        "fetch_hash": null,
        "deployed_at": 0,
        "version": "v2.0.0"
    });
    let entry: ToolRegistryEntry =
        serde_json::from_value(json).expect("should deserialize with empty canonical_version");
    assert_eq!(entry.canonical_version, "");
}
