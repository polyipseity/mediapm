//! V4 wire format for state persistence — read-only since the V5 bump.
//!
//! V4 is V3 with `pending_force_rerun` removed. The field was the queue half of
//! destructive invalidation: a workflow name a later run had to remember to look
//! up. Explicit invalidation is a marker on the recorded instance
//! (`ToolCallInstance::invalidated_at`), which the probe that decides reuse
//! cannot skip, so a queue beside it would be a second place for the same
//! decision to live and drift. A V3 document still loads, and its queue is
//! dropped rather than honoured: the request named a mechanism that no longer
//! exists, and force-rerunning a whole workflow would be a wider action than the
//! operator asked for.
//!
//! This module owns the V4 shape only. Resolution into the runtime model moved
//! to [`super::v5`] with the bump, so a document that was written before V5
//! resolves through exactly the code a native V5 document does.
//!
//! ## DO NOT REMOVE: versions policy guard
//!
//! - This file must never import unversioned structs from outside `versions/`
//!   beyond the wire field types it carries.
//! - A `vX` module may reference only the most recent previous version module.
//!   The `super::v3` references below are that single permitted edge; the
//!   workflow-state shape V3 re-exports is V2's, declared once there.
//! - Latest-version bridging to unversioned runtime structs belongs to the
//!   current version's module, which is [`super::v5`].
//! - Files outside `versions/` must reach versioned symbols only through
//!   `versions/mod.rs`, never through a direct `versions::vX` path.

use std::collections::BTreeMap;

use serde::{Deserialize, Serialize};
use serde_json::Value;

use super::v3::{MediaPmStateV3, WorkflowStepStateV2};
use crate::config::{ManagedFileRecord, ToolRegistryEntry};
use crate::error::MediaPmError;

/// V4 wire representation of the persisted state.
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
    /// Per-media workflow state, keyed by media id.
    ///
    /// Accepted because V4 documents carry it and dropped by
    /// [`super::v5::from_v4_into_v5`]: the conductor records the same facts per
    /// tool-call instance, and the map had no production reader.
    #[serde(default)]
    pub(super) workflow_states: BTreeMap<String, WorkflowStepStateV2>,
}

/// Decodes a V4 JSON [`Value`] into the canonical [`MediaPmStateV4`] wire type.
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
pub(super) fn decode_v4(value: Value) -> Result<MediaPmStateV4, MediaPmError> {
    let v4: MediaPmStateV4 = serde_json::from_value(value)
        .map_err(|e| MediaPmError::Serialization(format!("failed to decode V4 state: {e}")))?;
    if v4.version != 4 {
        return Err(MediaPmError::Serialization(format!(
            "state document is marked version {} but was decoded as V4",
            v4.version,
        )));
    }
    Ok(v4)
}

/// Advances a V3 wire document into the V4 wire shape.
///
/// The queue is dropped, not translated: V4 has no place to keep it and no code
/// that would act on it. Everything V3 shares with V4 — files, tools, workflow
/// steps — moves through unchanged, so the next rung drops the workflow map from
/// a shape that is already V4's.
pub(super) fn from_v3_into_v4(v3: MediaPmStateV3) -> MediaPmStateV4 {
    MediaPmStateV4 {
        version: 4,
        managed_files: v3.managed_files,
        managed_tools: v3.managed_tools,
        workflow_states: v3.workflow_states,
    }
}

#[cfg(test)]
mod tests {
    use serde_json::json;

    use super::super::v3::decode_v3;

    use super::*;

    /// A V3 document advances into the V4 wire shape with every shared field intact.
    #[test]
    fn from_v3_into_v4_keeps_every_shared_field_and_drops_the_queue() {
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

        let v4 = from_v3_into_v4(decode_v3(v3_json).expect("decode the v3 document"));

        assert_eq!(v4.version, 4);
        assert_eq!(v4.managed_files.len(), 1);
        assert_eq!(v4.workflow_states["a"].steps_completed, 2);
        assert_eq!(v4.workflow_states["a"].variant_hashes["primary"], "blake3:p");
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

        let error = decode_v4(document).expect_err("an unknown field must be rejected");
        assert!(
            error.to_string().contains("pending_force_rerun"),
            "the diagnostic must name the rejected field: {error}",
        );
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
        assert!(decode_v4(v3_shaped).is_err(), "a V3 marker is not a V4 document");

        let empty = json!({
            "version": 4,
            "managed_files": {},
            "managed_tools": [],
            "workflow_states": {}
        });
        let decoded = decode_v4(empty).expect("a minimal V4 document must load");
        assert!(decoded.managed_files.is_empty());
        assert!(decoded.managed_tools.is_empty());
        assert!(decoded.workflow_states.is_empty());
    }
}
