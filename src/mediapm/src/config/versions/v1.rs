//! V1 wire envelopes and migration definitions.
//!
//! This module provides the V1-specific deserialization envelopes and the
//! `Migrate` implementation that transforms V1 JSON into the current
//! runtime document model (`MediaPmDocument`).

use std::collections::BTreeMap;

use serde::{Deserialize, Serialize};

use super::super::{
    MaterializationMethod, MediaPmDocument, MediaPmState, SanitizeNamesConfig, ToolRequirement,
    VerifyStrategy, defaults, hierarchy_types, source_types,
};
use super::Migrate;
use super::v_latest::{
    MediaRuntimeStorageLatest, RuntimeCachingConfigLatest, RuntimeEnvironmentConfigLatest,
    RuntimeLifecycleConfigLatest, RuntimeMaterializationConfigLatest, RuntimePathsConfigLatest,
    RuntimeVerificationConfigLatest,
};

use serde_json::Value;

/// V1 deserialization envelope for `mediapm.ncl`.
#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub(super) struct MediaPmDocumentEnvelopeV1 {
    /// Schema version marker.
    pub(super) version: u32,
    /// Media source registry entries keyed by id.
    #[serde(default)]
    pub(super) media: BTreeMap<String, source_types::MediaSourceSpec>,
    /// Hierarchy node declarations.
    #[serde(default)]
    pub(super) hierarchy: Vec<hierarchy_types::HierarchyNode>,
    /// Managed tool requirement declarations keyed by tool id.
    #[serde(default)]
    pub(super) tools: BTreeMap<String, ToolRequirement>,
    /// Runtime configuration overrides.
    #[serde(default)]
    pub(super) runtime: MediaRuntimeStorageV1,
    /// Legacy `state` payload accepted on V1 documents.
    ///
    /// Dropped when unifying into the runtime model; state is managed
    /// separately via `state.json`.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub(super) state: Option<MediaPmState>,
}

impl From<MediaPmDocumentEnvelopeV1> for MediaPmDocument {
    fn from(envelope: MediaPmDocumentEnvelopeV1) -> Self {
        Self {
            version: envelope.version,
            media: envelope.media,
            hierarchy: envelope.hierarchy,
            tools: envelope.tools,
            runtime: MediaRuntimeStorageLatest::from(envelope.runtime),
            // The legacy `state` payload is dropped when unifying into the
            // runtime model: state is managed separately via `state.json`.
            state: None,
        }
    }
}

impl From<&MediaPmDocument> for MediaPmDocumentEnvelopeV1 {
    fn from(doc: &MediaPmDocument) -> Self {
        Self {
            version: 1,
            media: doc.media.clone(),
            hierarchy: doc.hierarchy.clone(),
            tools: doc.tools.clone(),
            runtime: MediaRuntimeStorageV1::from(doc.runtime.clone()),
            state: None,
        }
    }
}

pub(super) fn mediapm_document_v1_iso() -> &'static str {
    "mediapm_document_v1_iso"
}

impl Migrate for MediaPmDocumentEnvelopeV1 {
    fn version() -> u32 {
        1
    }

    fn decode(value: Value) -> Result<Self, crate::error::MediaPmError> {
        serde_json::from_value(value).map_err(|err| {
            crate::error::MediaPmError::Serialization(format!(
                "failed to decode V1 document envelope: {err}"
            ))
        })
    }

    fn encode(&self) -> Result<Value, crate::error::MediaPmError> {
        serde_json::to_value(self).map_err(|err| {
            crate::error::MediaPmError::Serialization(format!(
                "failed to encode V1 document envelope: {err}"
            ))
        })
    }
}

/// V1 wire shape for runtime storage overrides.
///
/// Flat record matching the V1 Nickel `MediaRuntimeStorageV1` contract. V1
/// keeps its own shape; it bridges to [`MediaRuntimeStorageLatest`] (the V2
/// boundary) via `From` impls — it is never reshaped to mirror V2's internal
/// grouping.
#[derive(Debug, Clone, Serialize, Deserialize, Default)]
#[serde(deny_unknown_fields)]
pub(super) struct MediaRuntimeStorageV1 {
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub mediapm_dir: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub hierarchy_root_dir: Option<String>,
    #[serde(default, skip)]
    pub tools: BTreeMap<String, ToolRequirement>,
    #[serde(
        default = "defaults::default_materialization_preference_order",
        deserialize_with = "super::super::deserialize_materialization_preference_order"
    )]
    pub materialization_preference_order: Vec<MaterializationMethod>,
    #[serde(default = "defaults::default_verify_on_read")]
    pub verify_on_read: Vec<VerifyStrategy>,
    #[serde(default = "defaults::default_verify_on_read_sample_denominator")]
    pub verify_on_read_sample_denominator: u64,
    #[serde(default = "defaults::default_verify_on_read_stale_timeout_secs")]
    pub verify_on_read_stale_timeout_secs: u64,
    #[serde(default = "defaults::default_reconstructed_cache_ttl_seconds")]
    pub reconstructed_cache_ttl_seconds: u64,
    #[serde(default = "defaults::default_instance_ttl_seconds")]
    pub instance_ttl_seconds: u64,
    #[serde(default)]
    pub inherited_env_vars: BTreeMap<String, Vec<String>>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub media_state_config: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub conductor_config: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub conductor_generated_config: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub conductor_state_config: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub conductor_schema_dir: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub env_file: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub env_generated_file: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub mediapm_schema_dir: Option<String>,
    #[serde(default = "defaults::default_profiler_enabled")]
    pub profiler_enabled: bool,
    #[serde(default = "defaults::default_verify_materialization")]
    pub verify_materialization: bool,
    #[serde(default = "defaults::default_retry_impure")]
    pub retry_impure: bool,
    #[serde(default = "defaults::default_path_sanitization")]
    pub path_sanitization: SanitizeNamesConfig,
}

impl From<MediaRuntimeStorageV1> for MediaRuntimeStorageLatest {
    fn from(v1: MediaRuntimeStorageV1) -> Self {
        MediaRuntimeStorageLatest {
            paths: RuntimePathsConfigLatest {
                mediapm_dir: v1.mediapm_dir,
                hierarchy_root_dir: v1.hierarchy_root_dir,
                mediapm_state_config: v1.media_state_config,
                conductor_config: v1.conductor_config,
                conductor_generated_config: v1.conductor_generated_config,
                conductor_state_config: v1.conductor_state_config,
                conductor_schema_dir: v1.conductor_schema_dir,
                mediapm_schema_dir: v1.mediapm_schema_dir,
                env_file: v1.env_file,
                env_generated_file: v1.env_generated_file,
            },
            materialization: RuntimeMaterializationConfigLatest {
                materialization_preference_order: Some(v1.materialization_preference_order),
                verify_materialization: Some(v1.verify_materialization),
            },
            verification: RuntimeVerificationConfigLatest {
                verify_on_read: Some(v1.verify_on_read),
                verify_on_read_sample_denominator: Some(v1.verify_on_read_sample_denominator),
                verify_on_read_stale_timeout_secs: Some(v1.verify_on_read_stale_timeout_secs),
            },
            caching: RuntimeCachingConfigLatest {
                reconstructed_cache_ttl_seconds: Some(v1.reconstructed_cache_ttl_seconds),
            },
            lifecycle: RuntimeLifecycleConfigLatest {
                instance_ttl_seconds: Some(v1.instance_ttl_seconds),
            },
            environment: RuntimeEnvironmentConfigLatest {
                inherited_env_vars: Some(v1.inherited_env_vars),
                profiler_enabled: Some(v1.profiler_enabled),
            },
            path_sanitization: Some(v1.path_sanitization),
            retry_impure: Some(v1.retry_impure),
            tools: v1.tools,
        }
    }
}

impl From<MediaRuntimeStorageLatest> for MediaRuntimeStorageV1 {
    fn from(latest: MediaRuntimeStorageLatest) -> Self {
        MediaRuntimeStorageV1 {
            mediapm_dir: latest.paths.mediapm_dir,
            hierarchy_root_dir: latest.paths.hierarchy_root_dir,
            tools: latest.tools,
            materialization_preference_order: latest
                .materialization
                .materialization_preference_order
                .unwrap_or_else(defaults::default_materialization_preference_order),
            verify_materialization: latest
                .materialization
                .verify_materialization
                .unwrap_or_else(defaults::default_verify_materialization),
            verify_on_read: latest
                .verification
                .verify_on_read
                .unwrap_or_else(defaults::default_verify_on_read),
            verify_on_read_sample_denominator: latest
                .verification
                .verify_on_read_sample_denominator
                .unwrap_or_else(defaults::default_verify_on_read_sample_denominator),
            verify_on_read_stale_timeout_secs: latest
                .verification
                .verify_on_read_stale_timeout_secs
                .unwrap_or_else(defaults::default_verify_on_read_stale_timeout_secs),
            reconstructed_cache_ttl_seconds: latest
                .caching
                .reconstructed_cache_ttl_seconds
                .unwrap_or_else(defaults::default_reconstructed_cache_ttl_seconds),
            instance_ttl_seconds: latest
                .lifecycle
                .instance_ttl_seconds
                .unwrap_or_else(defaults::default_instance_ttl_seconds),
            inherited_env_vars: latest.environment.inherited_env_vars.unwrap_or_default(),
            media_state_config: latest.paths.mediapm_state_config,
            conductor_config: latest.paths.conductor_config,
            conductor_generated_config: latest.paths.conductor_generated_config,
            conductor_state_config: latest.paths.conductor_state_config,
            conductor_schema_dir: latest.paths.conductor_schema_dir,
            env_file: latest.paths.env_file,
            env_generated_file: latest.paths.env_generated_file,
            mediapm_schema_dir: latest.paths.mediapm_schema_dir,
            profiler_enabled: latest
                .environment
                .profiler_enabled
                .unwrap_or_else(defaults::default_profiler_enabled),
            retry_impure: latest.retry_impure.unwrap_or(false),
            path_sanitization: latest.path_sanitization.unwrap_or(SanitizeNamesConfig::Inherit),
        }
    }
}
