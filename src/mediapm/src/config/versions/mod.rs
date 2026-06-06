//! Versioned persistence envelopes for `mediapm.ncl`.
//!
//! Runtime source-of-truth structs live in `config.rs`. Versioned envelopes and
//! schema dispatch live under `config/versions/`.
//!
//! ## DO NOT REMOVE: versions policy guard
//!
//! - `vX.rs` files must never import unversioned runtime structs outside
//!   `config/versions/`.
//! - A `vX` file may reference only adjacent versions for migration.
//! - This `mod.rs` is the only bridge between latest version state and
//!   unversioned runtime `MediaPmDocument`.
//! - Files outside `config/versions/` must use only APIs in this module and
//!   never import `versions::vX` directly.
//! - Do not directly re-export `versions::vX` structs/types from this module.

use fp_library::brands::RcBrand;
use fp_library::types::optics::IsoPrime;
use serde_json::Value;

use crate::config::MediaPmDocument;
use crate::error::MediaPmError;

pub(crate) mod v1;

/// File name for embedded `mediapm.ncl` migration module.
pub(crate) const MOD_NCL_FILE_NAME: &str = "mod.ncl";
/// Embedded `mediapm.ncl` migration module source.
pub(crate) const MOD_NCL_SOURCE: &str = include_str!("mod.ncl");
/// File name for embedded `mediapm.ncl` v1 contract.
pub(crate) const V1_NCL_FILE_NAME: &str = "v1.ncl";
/// Embedded `mediapm.ncl` v1 contract source.
pub(crate) const V1_NCL_SOURCE: &str = include_str!("v1.ncl");

/// Returns embedded Nickel schema sources exported at runtime.
#[must_use]
pub(crate) const fn embedded_schema_sources() -> [(&'static str, &'static str); 2] {
    [(MOD_NCL_FILE_NAME, MOD_NCL_SOURCE), (V1_NCL_FILE_NAME, V1_NCL_SOURCE)]
}

/// Latest-version bindings.
///
/// Keep explicit latest-version references centralized for safe schema bumps.
// BEGIN latest-version bindings
mod latest {
    use fp_library::brands::RcBrand;
    use fp_library::types::optics::IsoPrime;

    use super::v1;

    /// Latest schema marker supported by `mediapm.ncl` Rust bridge.
    pub(super) const VERSION: u32 = v1::MEDIAPM_NICKEL_VERSION_V1;
    /// Latest envelope type.
    pub(super) type Envelope = v1::MediaPmDocumentEnvelopeV1;
    /// Latest version-local state type.
    pub(super) type State = v1::MediaPmDocumentStateV1;

    /// Returns whether `marker` matches latest schema marker.
    pub(super) const fn is_version(marker: u32) -> bool {
        v1::is_mediapm_nickel_version_v1(marker)
    }

    /// Latest envelope/state isomorphism.
    pub(super) fn version_iso() -> IsoPrime<'static, RcBrand, Envelope, State> {
        v1::mediapm_document_v1_iso()
    }
}
// END latest-version bindings

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum MediaPmDocumentLayoutVersion {
    V1,
}

/// Optic-based migration trait between persistence envelope versions.
pub(crate) trait Migrate<To> {
    /// Migrates `self` to `To` via optic composition.
    fn migrate(self) -> To;
}

/// Shared migration helper using `from_iso.view` then `to_iso.review`.
pub(crate) fn migrate_with_version_state<'a, From, To, State>(
    from: From,
    from_iso: &IsoPrime<'a, RcBrand, From, State>,
    to_iso: &IsoPrime<'a, RcBrand, To, State>,
) -> To {
    let state = from_iso.from(from);
    to_iso.to(state)
}

impl Migrate<latest::Envelope> for latest::Envelope {
    fn migrate(self) -> latest::Envelope {
        migrate_with_version_state(self, &latest::version_iso(), &latest::version_iso())
    }
}

/// Decodes one `mediapm.ncl` envelope using one already-dispatched version
/// marker.
fn decode_envelope_for_version(
    value: Value,
    marker: u32,
) -> Result<latest::Envelope, MediaPmError> {
    match dispatch_nickel_version(marker)? {
        MediaPmDocumentLayoutVersion::V1 => serde_json::from_value(value).map_err(|err| {
            MediaPmError::Serialization(format!(
                "decoding mediapm.ncl v{} envelope failed: {err}",
                latest::VERSION
            ))
        }),
    }
}

/// Migrates one parsed `mediapm.ncl` envelope from `from_version` to
/// `target_version`.
///
/// This function is the central migration gateway used by decode flows.
fn migrate_envelope_to_version(
    envelope: latest::Envelope,
    from_version: u32,
    target_version: u32,
) -> Result<latest::Envelope, MediaPmError> {
    let from_layout = dispatch_nickel_version(from_version)?;
    let to_layout = dispatch_nickel_version(target_version)?;

    match (from_layout, to_layout) {
        (MediaPmDocumentLayoutVersion::V1, MediaPmDocumentLayoutVersion::V1) => {
            Ok(envelope.migrate())
        }
    }
}

/// Returns latest `mediapm.ncl` schema marker.
#[must_use]
pub(crate) const fn latest_nickel_version() -> u32 {
    latest::VERSION
}

/// ## DO NOT REMOVE: latest-first version dispatch guard
///
/// Version checks should always start from latest.
fn dispatch_nickel_version(marker: u32) -> Result<MediaPmDocumentLayoutVersion, MediaPmError> {
    if latest::is_version(marker) {
        Ok(MediaPmDocumentLayoutVersion::V1)
    } else {
        Err(MediaPmError::Workflow(format!(
            "unsupported mediapm.ncl version {marker}; expected {}",
            latest_nickel_version()
        )))
    }
}

/// Extracts numeric top-level version marker from evaluated document value.
fn decode_version_marker(value: &Value) -> Result<u32, MediaPmError> {
    let object = value.as_object().ok_or_else(|| {
        MediaPmError::Workflow("mediapm.ncl must evaluate to a top-level record".to_string())
    })?;

    let marker = object.get("version").and_then(Value::as_u64).ok_or_else(|| {
        MediaPmError::Workflow("mediapm.ncl must define a numeric top-level 'version'".to_string())
    })?;

    u32::try_from(marker).map_err(|_| {
        MediaPmError::Workflow(format!("mediapm.ncl version {marker} exceeds supported u32 range"))
    })
}

/// Decodes one evaluated `mediapm.ncl` JSON value through version dispatch.
pub(crate) fn decode_mediapm_document_value(value: Value) -> Result<MediaPmDocument, MediaPmError> {
    let marker = decode_version_marker(&value)?;

    let envelope = decode_envelope_for_version(value, marker)?;
    let migrated = migrate_envelope_to_version(envelope, marker, latest_nickel_version())?;

    let state = latest::version_iso().from(migrated);

    // Serialize typed state, then inject the version marker back.
    let mut object = serde_json::to_value(&state)
        .map_err(|err| {
            MediaPmError::Serialization(format!(
                "serializing typed mediapm.ncl v{} state to JSON: {err}",
                latest::VERSION
            ))
        })?
        .as_object()
        .cloned()
        .ok_or_else(|| {
            MediaPmError::Serialization(
                "decoding mediapm.ncl value: typed state must be a top-level object".to_string(),
            )
        })?;
    object.insert("version".to_string(), Value::from(latest::VERSION));

    serde_json::from_value(Value::Object(object)).map_err(|err| {
        MediaPmError::Serialization(format!(
            "decoding latest mediapm.ncl payload into runtime document: {err}"
        ))
    })
}

/// Encodes runtime `MediaPmDocument` into latest versioned JSON value.
pub(crate) fn encode_mediapm_document_value(
    document: MediaPmDocument,
) -> Result<Value, MediaPmError> {
    let _ = dispatch_nickel_version(document.version)?;

    // Serialize runtime document to JSON, then separate version from payload.
    let value = serde_json::to_value(document)
        .map_err(|err| MediaPmError::Serialization(format!("encoding mediapm.ncl value: {err}")))?;
    let mut object = value.as_object().cloned().ok_or_else(|| {
        MediaPmError::Serialization(
            "encoding mediapm.ncl value: runtime document must be a top-level object".to_string(),
        )
    })?;
    let _ = object.remove("version");

    // Build typed state from raw JSON object fields — serde handles field-level
    // deserialization and validation.
    let state: latest::State = serde_json::from_value(Value::Object(object)).map_err(|err| {
        MediaPmError::Serialization(format!(
            "encoding mediapm.ncl value: building typed state from runtime fields: {err}"
        ))
    })?;

    let envelope = latest::version_iso().to(state);
    serde_json::to_value(envelope)
        .map_err(|err| MediaPmError::Serialization(format!("encoding mediapm.ncl value: {err}")))
}

#[cfg(test)]
mod tests {
    use serde_json::json;

    use super::{
        decode_mediapm_document_value, encode_mediapm_document_value, latest_nickel_version,
    };
    use crate::config::{
        ManagedFileRecord, ManagedWorkflowStepState, MediaPmDocument, MediaPmImpureTimestamp,
        MediaPmState, MediaRuntimeStorage, ToolRegistryRecord, ToolRegistryStatus,
    };

    /// Verifies decode rejects unsupported top-level schema markers.
    #[test]
    fn decode_rejects_unsupported_mediapm_ncl_version() {
        let value = json!({ "version": latest_nickel_version() + 1 });
        let err =
            decode_mediapm_document_value(value).expect_err("unsupported version should fail");
        assert!(err.to_string().contains("unsupported mediapm.ncl version"));
    }

    /// Verifies latest round-trip keeps explicit top-level version marker.
    #[test]
    fn encode_round_trip_preserves_latest_version_marker() {
        let runtime = MediaPmDocument::default();
        let encoded = encode_mediapm_document_value(runtime).expect("encode document");
        let decoded = decode_mediapm_document_value(encoded).expect("decode document");
        assert_eq!(decoded.version, latest_nickel_version());
    }

    /// Verifies round-trip preserves all `MediaRuntimeStorage` fields through
    /// the typed envelope.
    #[test]
    fn populated_runtime_storage_round_trips_through_typed_envelope() {
        let doc = MediaPmDocument {
            runtime: MediaRuntimeStorage {
                mediapm_dir: Some("/custom/mediapm".into()),
                hierarchy_root_dir: Some("/custom/hierarchy".into()),
                materialization_preference_order: Some(vec![
                    crate::config::MaterializationMethod::Hardlink,
                    crate::config::MaterializationMethod::Copy,
                ]),
                conductor_config: Some("/custom/conductor.ncl".into()),
                conductor_machine_config: Some("/custom/machine.ncl".into()),
                conductor_state_config: Some("/custom/state.ncl".into()),
                conductor_schema_dir: Some("/custom/schemas".into()),
                inherited_env_vars: Some(
                    [("macos".into(), vec!["PATH".into(), "HOME".into()])].into_iter().collect(),
                ),
                media_state_config: Some("/custom/media_state.ncl".into()),
                env_file: Some("/custom/.env".into()),
                mediapm_schema_dir: Some(Some("/custom/schemas/mediapm".into())),
                instance_ttl_seconds: Some(3600),
                ..Default::default()
            },
            ..MediaPmDocument::default()
        };

        let encoded = encode_mediapm_document_value(doc.clone()).expect("encode document");
        let decoded = decode_mediapm_document_value(encoded).expect("decode document");

        assert_eq!(decoded.version, doc.version);
        assert_eq!(decoded.runtime.mediapm_dir, doc.runtime.mediapm_dir);
        assert_eq!(decoded.runtime.hierarchy_root_dir, doc.runtime.hierarchy_root_dir);
        assert_eq!(
            decoded.runtime.materialization_preference_order,
            doc.runtime.materialization_preference_order
        );
        assert_eq!(decoded.runtime.conductor_config, doc.runtime.conductor_config);
        assert_eq!(decoded.runtime.conductor_machine_config, doc.runtime.conductor_machine_config);
        assert_eq!(decoded.runtime.conductor_state_config, doc.runtime.conductor_state_config);
        assert_eq!(decoded.runtime.conductor_schema_dir, doc.runtime.conductor_schema_dir);
        assert_eq!(decoded.runtime.media_state_config, doc.runtime.media_state_config);
        assert_eq!(decoded.runtime.env_file, doc.runtime.env_file);
        assert_eq!(decoded.runtime.mediapm_schema_dir, doc.runtime.mediapm_schema_dir);
    }

    /// Verifies round-trip preserves `MediaPmState` fields through the typed
    /// envelope, including `managed_files`, `tool_registry`, and `workflow_states`
    /// with impure timestamps.
    #[test]
    fn populated_mediapm_state_round_trips_through_typed_envelope() {
        let doc = MediaPmDocument {
            state: MediaPmState {
                managed_files: [(
                    "videos/demo.mp4".into(),
                    ManagedFileRecord {
                        media_id: "demo".into(),
                        variant: "default".into(),
                        hash: "abc123".into(),
                        last_synced_unix_millis: 1_700_000_000_000,
                    },
                )]
                .into_iter()
                .collect(),
                tool_registry: [(
                    "ffmpeg@7.0".into(),
                    ToolRegistryRecord {
                        name: "ffmpeg".into(),
                        version: "7.0".into(),
                        source: "catalog".into(),
                        registry_multihash: "Qm123".into(),
                        last_transition_unix_seconds: 1_700_000_000,
                        status: ToolRegistryStatus::Active,
                    },
                )]
                .into_iter()
                .collect(),
                active_tools: [("ffmpeg".into(), "ffmpeg@7.0".into())]
                    .into_iter()
                    .collect(),
                workflow_states: [(
                    "demo-media".into(),
                    vec![ManagedWorkflowStepState {
                        explicit_config: json!({ "tool": "echo@1.0.0", "inputs": { "args": "hello" } }),
                        impure_timestamp: Some(MediaPmImpureTimestamp {
                            epoch_seconds: 1_700_000_000,
                            subsec_nanos: 123_456_789,
                        }),
                    }],
                )]
                .into_iter()
                .collect(),
            },
            ..MediaPmDocument::default()
        };

        let encoded = encode_mediapm_document_value(doc.clone()).expect("encode document");
        let decoded = decode_mediapm_document_value(encoded).expect("decode document");

        assert_eq!(decoded.state.managed_files, doc.state.managed_files);
        assert_eq!(decoded.state.tool_registry, doc.state.tool_registry);
        assert_eq!(decoded.state.active_tools, doc.state.active_tools);
        assert_eq!(decoded.state.workflow_states, doc.state.workflow_states);
    }

    /// Verifies the typed envelope's `deny_unknown_fields` on
    /// `MediaPmDocumentEnvelopeV1` rejects extra top-level fields.
    #[test]
    fn typed_envelope_rejects_unknown_top_level_field() {
        let value = json!({
            "version": latest_nickel_version(),
            "runtime": {},
            "unknown_field": "should_be_rejected",
        });

        let err = decode_mediapm_document_value(value)
            .expect_err("unknown top-level field should be rejected by envelope");
        assert!(
            err.to_string().contains("unknown field")
                || err.to_string().contains("unknown_field")
                || err.to_string().contains("`deny_unknown_fields`")
        );
    }

    /// Verifies round-trip preserves `PlatformInheritedEnvVars` correctly
    /// through the typed envelope for all three platform entries.
    #[test]
    fn inherited_env_vars_round_trip_preserves_platform_keys() {
        let doc = MediaPmDocument {
            runtime: MediaRuntimeStorage {
                inherited_env_vars: Some(
                    [
                        ("windows".into(), vec!["PATH".into(), "TEMP".into()]),
                        ("linux".into(), vec!["LD_LIBRARY_PATH".into()]),
                        ("macos".into(), vec!["PATH".into(), "HOME".into(), "SHELL".into()]),
                    ]
                    .into_iter()
                    .collect(),
                ),
                ..Default::default()
            },
            ..MediaPmDocument::default()
        };

        let encoded = encode_mediapm_document_value(doc.clone()).expect("encode document");
        let decoded = decode_mediapm_document_value(encoded).expect("decode document");

        let got = decoded.runtime.inherited_env_vars.expect("inherited_env_vars should be present");
        assert_eq!(
            got.get("windows").map(Vec::as_slice),
            Some(&["PATH".to_string(), "TEMP".to_string()][..])
        );
        assert_eq!(got.get("linux").map(Vec::as_slice), Some(&["LD_LIBRARY_PATH".to_string()][..]));
        assert_eq!(
            got.get("macos").map(Vec::as_slice),
            Some(&["PATH".to_string(), "HOME".to_string(), "SHELL".to_string()][..])
        );
    }
}
