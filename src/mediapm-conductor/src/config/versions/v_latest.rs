//! Latest persisted Nickel envelope shape for conductor configuration documents.
//!
//! Uses runtime config types directly — the only bridge types are for fields
//! whose persisted serde representation differs from runtime (currently only
//! `external_data.save_mode`).

use std::collections::BTreeMap;

use serde::{Deserialize, Serialize};

use crate::config::{
    ConductorRuntimeConfig, ExternalDataEntry, NickelDocument, OutputPolicy, ToolSpec, WorkflowSpec,
};
use crate::state::OutputSaveMode;

/// Latest persisted Nickel schema marker supported by the Rust bridge.
pub(crate) const NICKEL_VERSION_LATEST: u32 = 2;

// ---------------------------------------------------------------------------
// Bridge types — thin wrappers for persisted fields that differ from runtime
// ---------------------------------------------------------------------------

/// Bridge for external data entries in the envelope — matches Nickel's
/// `"full"` / `true` / `false` tri-state serialization for `save_mode`.
///
/// The runtime `ExternalDataEntry.save_mode` uses `OutputSaveMode` with
/// different serde representation. This bridge handles the conversion.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub(crate) struct ExternalDataEntryBridge {
    /// Human-readable description.
    description: String,
    /// Save policy — uses `OutputPolicy` for Nickel-compatible serde.
    save_mode: OutputPolicy,
}

/// Top-level Nickel envelope for the latest schema version.
///
/// This is the primary deserialization target after Nickel evaluation and
/// migration.  All persisted documents produce this type on decode.
///
/// Uses runtime config types directly. The only difference from
/// `NickelDocument` is:
/// - includes a `version` marker,
/// - `external_data` uses `ExternalDataEntryBridge` for Nickel-compatible
///   `save_mode` serde.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub(crate) struct NickelEnvelopeLatest {
    /// Schema version marker.
    pub(crate) version: u32,
    /// Tool definitions keyed by tool name.
    #[serde(default, skip_serializing_if = "BTreeMap::is_empty")]
    pub(crate) tools: BTreeMap<String, ToolSpec>,
    /// Workflow definitions.
    #[serde(default)]
    pub(crate) workflows: Vec<WorkflowSpec>,
    /// External data entries keyed by CAS hash.
    #[serde(default, skip_serializing_if = "BTreeMap::is_empty")]
    pub(crate) external_data: BTreeMap<mediapm_cas::Hash, ExternalDataEntryBridge>,
    /// Conductor-level runtime configuration.
    #[serde(default)]
    pub(crate) runtime: ConductorRuntimeConfig,
}

// ---------------------------------------------------------------------------
// Conversions: envelope ↔ runtime config
// ---------------------------------------------------------------------------

impl From<NickelEnvelopeLatest> for NickelDocument {
    fn from(envelope: NickelEnvelopeLatest) -> Self {
        NickelDocument {
            tools: envelope.tools,
            workflows: envelope.workflows,
            external_data: envelope
                .external_data
                .into_iter()
                .map(|(hash, entry)| {
                    (
                        hash,
                        ExternalDataEntry {
                            description: entry.description,
                            save_mode: match entry.save_mode {
                                OutputPolicy::Bool(true) => OutputSaveMode::Saved,
                                OutputPolicy::Bool(false) => OutputSaveMode::Unsaved,
                                OutputPolicy::Full => OutputSaveMode::Full,
                            },
                        },
                    )
                })
                .collect(),
            runtime: envelope.runtime,
        }
    }
}

impl From<NickelDocument> for NickelEnvelopeLatest {
    fn from(doc: NickelDocument) -> Self {
        NickelEnvelopeLatest {
            version: NICKEL_VERSION_LATEST,
            tools: doc.tools,
            workflows: doc.workflows,
            external_data: doc
                .external_data
                .into_iter()
                .map(|(hash, entry)| {
                    (
                        hash,
                        ExternalDataEntryBridge {
                            description: entry.description,
                            save_mode: match entry.save_mode {
                                OutputSaveMode::Saved => OutputPolicy::Bool(true),
                                OutputSaveMode::Unsaved => OutputPolicy::Bool(false),
                                OutputSaveMode::Full => OutputPolicy::Full,
                            },
                        },
                    )
                })
                .collect(),
            runtime: doc.runtime,
        }
    }
}

#[cfg(test)]
mod tests {
    //! Tests for latest envelope ↔ runtime config conversion.
    use super::*;
    use crate::config::{ToolKindSpec, ToolRuntime};

    /// Verifies that `NickelEnvelopeLatest` round-trips through
    /// `NickelDocument` without data loss.
    #[test]
    fn envelope_round_trip() {
        let envelope = NickelEnvelopeLatest {
            version: NICKEL_VERSION_LATEST,
            tools: BTreeMap::from([(
                "echo".to_string(),
                ToolSpec {
                    kind: ToolKindSpec::Builtin {
                        name: "echo".to_string(),
                        version: "1.0.0".to_string(),
                    },
                    name: "echo".to_string(),
                    version: "1.0.0".to_string(),
                    inputs: BTreeMap::new(),
                    default_inputs: BTreeMap::new(),
                    outputs: BTreeMap::new(),
                    runtime: ToolRuntime::default(),
                },
            )]),
            workflows: vec![],
            runtime: ConductorRuntimeConfig::default(),
            external_data: BTreeMap::new(),
        };

        let doc: NickelDocument = envelope.clone().into();
        let back: NickelEnvelopeLatest = doc.into();

        assert_eq!(envelope.version, back.version);
        assert_eq!(envelope.tools.len(), back.tools.len());
        assert!(back.tools.contains_key("echo"));
        assert_eq!(back.tools["echo"].name, "echo".to_string());
    }
}
