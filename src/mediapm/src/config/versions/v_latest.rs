//! V2 boundary (latest) runtime storage types and resolver.
//!
//! This module holds the `*Latest` boundary structs for runtime storage
//! overrides and the resolver that maps a boundary value into the resolved
//! [`MediaRuntimeStorage`] model. Keeping these version-specific boundary
//! types here (rather than in `config/mod.rs`) mirrors the conductor
//! convention: `config/mod.rs` owns resolved types only; version-specific
//! types and `From` bridges live under `versions/`.

use std::collections::BTreeMap;

use serde::{Deserialize, Serialize};

use super::super::{
    MaterializationMethod, MediaRuntimeStorage, RuntimeBasePaths, SanitizeNamesConfig,
    ToolRequirement, VerifyStrategy, defaults,
};

#[derive(Debug, Clone, Serialize, Deserialize, Default)]
pub struct RuntimePathsConfigLatest {
    #[serde(default)]
    pub mediapm_dir: Option<String>,
    #[serde(default)]
    pub hierarchy_root_dir: Option<String>,
    #[serde(default)]
    pub mediapm_state_config: Option<String>,
    #[serde(default)]
    pub conductor_config: Option<String>,
    #[serde(default)]
    pub conductor_generated_config: Option<String>,
    #[serde(default)]
    pub conductor_state_config: Option<String>,
    #[serde(default)]
    pub conductor_schema_dir: Option<String>,
    #[serde(default)]
    pub mediapm_schema_dir: Option<String>,
    #[serde(default)]
    pub env_file: Option<String>,
    #[serde(default)]
    pub env_generated_file: Option<String>,
}

#[derive(Debug, Clone, Serialize, Deserialize, Default)]
pub struct RuntimeMaterializationConfigLatest {
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub materialization_preference_order: Option<Vec<MaterializationMethod>>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub verify_materialization: Option<bool>,
}

#[derive(Debug, Clone, Serialize, Deserialize, Default)]
pub struct RuntimeVerificationConfigLatest {
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub verify_on_read: Option<Vec<VerifyStrategy>>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub verify_on_read_sample_denominator: Option<u64>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub verify_on_read_stale_timeout_secs: Option<u64>,
}

#[derive(Debug, Clone, Serialize, Deserialize, Default)]
pub struct RuntimeCachingConfigLatest {
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub reconstructed_cache_ttl_seconds: Option<u64>,
}

#[derive(Debug, Clone, Serialize, Deserialize, Default)]
pub struct RuntimeLifecycleConfigLatest {
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub instance_ttl_seconds: Option<u64>,
}

#[derive(Debug, Clone, Serialize, Deserialize, Default)]
pub struct RuntimeEnvironmentConfigLatest {
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub inherited_env_vars: Option<BTreeMap<String, Vec<String>>>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub profiler_enabled: Option<bool>,
}

#[derive(Debug, Clone, Serialize, Deserialize, Default)]
#[serde(deny_unknown_fields)]
pub struct MediaRuntimeStorageLatest {
    #[serde(default)]
    pub paths: RuntimePathsConfigLatest,
    #[serde(default)]
    pub materialization: RuntimeMaterializationConfigLatest,
    #[serde(default)]
    pub verification: RuntimeVerificationConfigLatest,
    #[serde(default)]
    pub caching: RuntimeCachingConfigLatest,
    #[serde(default)]
    pub lifecycle: RuntimeLifecycleConfigLatest,
    #[serde(default)]
    pub environment: RuntimeEnvironmentConfigLatest,
    #[serde(default)]
    pub path_sanitization: Option<SanitizeNamesConfig>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub retry_impure: Option<bool>,
    // Holding field for the boundary conversion (tools live at the document
    // top level, not inside runtime); never serialized.
    #[serde(default, skip_serializing)]
    pub tools: BTreeMap<String, ToolRequirement>,
}

/// Resolves a boundary [`MediaRuntimeStorageLatest`] into the resolved
/// [`MediaRuntimeStorage`] model.
///
/// Unset boundary path values map to an empty `PathBuf` so the override
/// machinery (pick_path/opt_path/pathbuf_to_opt) treats them as "no
/// override" and the `MediaPmPaths::from_root` defaults win. Populating
/// defaults here would redirect saves to a different path than the one
/// `MediaPmPaths` exposes for reads.
#[must_use]
pub fn resolve_runtime_storage(
    latest: &MediaRuntimeStorageLatest,
    _base: &RuntimeBasePaths,
) -> MediaRuntimeStorage {
    MediaRuntimeStorage {
        paths: super::super::RuntimePathsConfig {
            mediapm_dir: latest
                .paths
                .mediapm_dir
                .clone()
                .map(std::path::PathBuf::from)
                .unwrap_or_default(),
            hierarchy_root_dir: latest
                .paths
                .hierarchy_root_dir
                .clone()
                .map(std::path::PathBuf::from)
                .unwrap_or_default(),
            mediapm_state_config: latest
                .paths
                .mediapm_state_config
                .clone()
                .map(std::path::PathBuf::from)
                .unwrap_or_default(),
            conductor_config: latest
                .paths
                .conductor_config
                .clone()
                .map(std::path::PathBuf::from)
                .unwrap_or_default(),
            conductor_generated_config: latest
                .paths
                .conductor_generated_config
                .clone()
                .map(std::path::PathBuf::from)
                .unwrap_or_default(),
            conductor_state_config: latest
                .paths
                .conductor_state_config
                .clone()
                .map(std::path::PathBuf::from)
                .unwrap_or_default(),
            conductor_schema_dir: latest
                .paths
                .conductor_schema_dir
                .clone()
                .map(std::path::PathBuf::from)
                .unwrap_or_default(),
            mediapm_schema_dir: latest
                .paths
                .mediapm_schema_dir
                .clone()
                .map(std::path::PathBuf::from)
                .unwrap_or_default(),
            env_file: latest
                .paths
                .env_file
                .clone()
                .map(std::path::PathBuf::from)
                .unwrap_or_default(),
            env_generated_file: latest
                .paths
                .env_generated_file
                .clone()
                .map(std::path::PathBuf::from)
                .unwrap_or_default(),
        },
        materialization: super::super::RuntimeMaterializationConfig {
            materialization_preference_order: latest
                .materialization
                .materialization_preference_order
                .clone()
                .unwrap_or_else(defaults::default_materialization_preference_order),
            verify_materialization: latest
                .materialization
                .verify_materialization
                .unwrap_or_else(defaults::default_verify_materialization),
        },
        verification: super::super::RuntimeVerificationConfig {
            verify_on_read: latest
                .verification
                .verify_on_read
                .clone()
                .unwrap_or_else(defaults::default_verify_on_read),
            verify_on_read_sample_denominator: latest
                .verification
                .verify_on_read_sample_denominator
                .unwrap_or_else(defaults::default_verify_on_read_sample_denominator),
            verify_on_read_stale_timeout_secs: latest
                .verification
                .verify_on_read_stale_timeout_secs
                .unwrap_or_else(defaults::default_verify_on_read_stale_timeout_secs),
        },
        caching: super::super::RuntimeCachingConfig {
            reconstructed_cache_ttl_seconds: latest
                .caching
                .reconstructed_cache_ttl_seconds
                .unwrap_or_else(defaults::default_reconstructed_cache_ttl_seconds),
        },
        lifecycle: super::super::RuntimeLifecycleConfig {
            instance_ttl_seconds: latest
                .lifecycle
                .instance_ttl_seconds
                .unwrap_or_else(defaults::default_instance_ttl_seconds),
        },
        environment: super::super::RuntimeEnvironmentConfig {
            inherited_env_vars: latest.environment.inherited_env_vars.clone().unwrap_or_default(),
            profiler_enabled: latest
                .environment
                .profiler_enabled
                .unwrap_or_else(defaults::default_profiler_enabled),
        },
        path_sanitization: latest.path_sanitization.clone().unwrap_or(SanitizeNamesConfig::Inherit),
        retry_impure: latest.retry_impure.unwrap_or(false),
        tools: latest.tools.clone(),
        cache_root_override: None,
    }
}
