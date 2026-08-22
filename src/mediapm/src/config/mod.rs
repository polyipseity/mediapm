//! Configuration types, constants, and validation for mediapm.
//!
//! This module provides the type-level model for `mediapm.ncl` config
//! documents and persisted state, along with schema-version dispatch,
//! Nickel I/O, and cross-field validation.
//!
//! # Organization
//!
//! | Submodule | Purpose |
//! |---|---|
//! | [`defaults`] | `pub const` default values for `#[serde(default)]` fields |
//! | [`custom_deserializers`] | Serde helper deserializers bridging Nickel → Rust |
//! | [`output_types`] | Output variant config, persistence policy |
//! | [`source_types`] | Media source, step, and tool types |
//! | [`hierarchy_types`] | Hierarchy node, path, and flattening utilities |
//! | [`nickel_io`] | Evaluate `.ncl` files to JSON, render terms, state I/O |
//! | [`versions`] | Schema version dispatch and V1 document envelope types |
//! | [`validation`] | Cross-field document validation |

pub mod custom_deserializers;
pub mod defaults;
pub mod hierarchy_types;
pub mod nickel_io;
pub mod output_types;
pub mod source_types;
pub mod validation;
pub mod versions;

pub use hierarchy_types::{
    HierarchyEntry, HierarchyEntryKind, HierarchyFolderRenameRule, HierarchyNode,
    HierarchyNodeKind, HierarchyPath, PlaylistEntryPathMode, PlaylistFormat, PlaylistItemRef,
    SanitizeNamesConfig, flatten_hierarchy_value, nest_hierarchy_value, regex_variant_selector,
};
pub use nickel_io::{
    load_mediapm_document, load_mediapm_state_document, merge_mediapm_document_with_state,
    save_mediapm_document, save_mediapm_state_document,
};
pub use output_types::{
    GenericOutputVariantConfig, OutputCaptureKind, OutputSaveConfig, OutputVariantValue,
    YtDlpOutputKind, YtDlpOutputVariantConfig,
};
pub use source_types::{
    MediaMetadataRegexTransform, MediaMetadataValue, MediaMetadataValueCandidate,
    MediaMetadataVariantBinding, MediaSourceSpec, MediaStep, MediaStepTool, TransformInputValue,
};

use std::collections::BTreeMap;
use std::path::PathBuf;

use mediapm_conductor::tools::provider::ConfigVersionSpec;
use serde::{Deserialize, Serialize};
use serde_json::Value;

// ---------------------------------------------------------------------------
// Materialization method
// ---------------------------------------------------------------------------

// Constants for default materialization methods used in the preferences list.
#[allow(dead_code)]
pub const MATERIALIZE_HARDLINK: &str = "hardlink";
#[allow(dead_code)]
pub const MATERIALIZE_SYMLINK: &str = "symlink";
#[allow(dead_code)]
pub const MATERIALIZE_REFLINK: &str = "reflink";
#[allow(dead_code)]
pub const MATERIALIZE_COPY: &str = "copy";

/// Supported file materialization methods in preference order.
#[derive(Debug, Clone, Copy, Default, PartialEq, Eq, PartialOrd, Ord, Serialize, Deserialize)]
#[serde(rename_all = "lowercase")]
pub enum MaterializationMethod {
    /// Hard-link target file into the output tree.
    #[default]
    Hardlink,
    /// Symbolic-link target file into the output tree.
    Symlink,
    /// Copy-on-write clone (reflink) into the output tree.
    Reflink,
    /// Full byte copy into the output tree.
    Copy,
}

impl MaterializationMethod {
    /// Returns a human-readable label for this materialization method.
    #[must_use]
    pub fn as_label(&self) -> &'static str {
        match self {
            Self::Hardlink => "hardlink",
            Self::Symlink => "symlink",
            Self::Reflink => "reflink",
            Self::Copy => "copy",
        }
    }
}

/// Deserializes a materialization method or named-object form from Nickel.
#[allow(dead_code)]
pub fn deserialize_materialization_method<'de, D>(
    deserializer: D,
) -> Result<MaterializationMethod, D::Error>
where
    D: serde::Deserializer<'de>,
{
    let value = Value::deserialize(deserializer)?;

    match &value {
        Value::String(_name) => serde_json::from_value(value).map_err(serde::de::Error::custom),
        Value::Object(obj) => {
            let method_name = obj.get("method").and_then(|v| v.as_str()).ok_or_else(|| {
                serde::de::Error::custom(
                    "materialization preference objects must have a 'method' string field",
                )
            })?;
            serde_json::from_value(Value::String(method_name.to_string()))
                .map_err(serde::de::Error::custom)
        }
        _ => Err(serde::de::Error::custom(
            "materialization preference must be a string (method name) or object with 'method'",
        )),
    }
}

// ---------------------------------------------------------------------------
// Materialization preference order
// ---------------------------------------------------------------------------

/// Deserializes the materialization preference order.
pub fn deserialize_materialization_preference_order<'de, D>(
    deserializer: D,
) -> Result<Vec<MaterializationMethod>, D::Error>
where
    D: serde::Deserializer<'de>,
{
    let values = Vec::<Value>::deserialize(deserializer)?;

    let methods: Result<Vec<MaterializationMethod>, _> = values
        .into_iter()
        .map(|value| {
            let method_name = match &value {
                Value::String(name) => name.clone(),
                Value::Object(obj) => {
                    obj.get("method").and_then(|v| v.as_str()).map(String::from).ok_or_else(
                        || {
                            serde::de::Error::custom(
                                "each entry must be a string or object with 'method'",
                            )
                        },
                    )?
                }
                _ => {
                    return Err(serde::de::Error::custom(
                        "each entry must be a string or object with 'method'",
                    ));
                }
            };

            serde_json::from_value(Value::String(method_name)).map_err(serde::de::Error::custom)
        })
        .collect();

    let methods = methods?;

    if methods.is_empty() {
        return Err(serde::de::Error::custom("materialization_preference_order must be non-empty"));
    }

    let mut seen = std::collections::BTreeSet::new();
    for method in &methods {
        if !seen.insert(method) {
            return Err(serde::de::Error::custom(format!(
                "duplicate materialization method '{method:?}' in preference order",
            )));
        }
    }

    Ok(methods)
}

// ---------------------------------------------------------------------------
// Platform inherited env vars
// ---------------------------------------------------------------------------

/// Platform-grouped inherited environment variable configuration.
#[derive(Debug, Clone, Default, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct PlatformInheritedEnvVars {
    /// Variables inherited on all platforms.
    #[serde(default)]
    pub shared: Vec<String>,
    /// Variables inherited on macOS.
    #[serde(default)]
    pub macos: Vec<String>,
    /// Variables inherited on Linux.
    #[serde(default)]
    pub linux: Vec<String>,
    /// Variables inherited on Windows.
    #[serde(default)]
    pub windows: Vec<String>,
}

// ---------------------------------------------------------------------------
// VerifyStrategy
// ---------------------------------------------------------------------------

/// Verify-on-read trigger strategy name (S-E3).
///
/// Mirrors the [`mediapm_cas::VerifyTriggerStrategy`] variant set: `always`,
/// `modified`, `sample`, `stale`.  Unknown names fail fast at the serde
/// boundary instead of being silently ignored.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum VerifyStrategy {
    /// Verify every read.
    Always,
    /// Verify when the on-disk artifact differs from the recorded hash.
    Modified,
    /// Verify a sampled fraction of reads.
    Sample,
    /// Verify when the recorded artifact is older than the stale timeout.
    Stale,
}

// ---------------------------------------------------------------------------
// MediaRuntimeStorage
// ---------------------------------------------------------------------------

#[derive(Debug, Clone, Serialize, Deserialize, Default)]
#[serde(deny_unknown_fields)]
pub struct RuntimePathsConfig {
    #[serde(default)]
    pub mediapm_dir: PathBuf,
    #[serde(default)]
    pub hierarchy_root_dir: PathBuf,
    #[serde(default)]
    pub mediapm_state_config: PathBuf,
    #[serde(default)]
    pub conductor_config: PathBuf,
    #[serde(default)]
    pub conductor_generated_config: PathBuf,
    #[serde(default)]
    pub conductor_state_config: PathBuf,
    #[serde(default)]
    pub conductor_schema_dir: PathBuf,
    #[serde(default)]
    pub mediapm_schema_dir: PathBuf,
    #[serde(default)]
    pub env_file: PathBuf,
    #[serde(default)]
    pub env_generated_file: PathBuf,
}

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
#[serde(deny_unknown_fields)]
pub struct RuntimeMaterializationConfig {
    #[serde(
        default = "defaults::default_materialization_preference_order",
        deserialize_with = "deserialize_materialization_preference_order"
    )]
    pub materialization_preference_order: Vec<MaterializationMethod>,
    #[serde(default = "defaults::default_verify_materialization")]
    pub verify_materialization: bool,
}

#[derive(Debug, Clone, Serialize, Deserialize, Default)]
pub struct RuntimeMaterializationConfigLatest {
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub materialization_preference_order: Option<Vec<MaterializationMethod>>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub verify_materialization: Option<bool>,
}

#[derive(Debug, Clone, Serialize, Deserialize, Default)]
#[serde(deny_unknown_fields)]
pub struct RuntimeVerificationConfig {
    #[serde(default = "defaults::default_verify_on_read")]
    pub verify_on_read: Vec<VerifyStrategy>,
    #[serde(default = "defaults::default_verify_on_read_sample_denominator")]
    pub verify_on_read_sample_denominator: u64,
    #[serde(default = "defaults::default_verify_on_read_stale_timeout_secs")]
    pub verify_on_read_stale_timeout_secs: u64,
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
#[serde(deny_unknown_fields)]
pub struct RuntimeCachingConfig {
    #[serde(default = "defaults::default_reconstructed_cache_ttl_seconds")]
    pub reconstructed_cache_ttl_seconds: u64,
}

#[derive(Debug, Clone, Serialize, Deserialize, Default)]
pub struct RuntimeCachingConfigLatest {
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub reconstructed_cache_ttl_seconds: Option<u64>,
}

#[derive(Debug, Clone, Serialize, Deserialize, Default)]
#[serde(deny_unknown_fields)]
pub struct RuntimeLifecycleConfig {
    #[serde(default = "defaults::default_instance_ttl_seconds")]
    pub instance_ttl_seconds: u64,
}

#[derive(Debug, Clone, Serialize, Deserialize, Default)]
pub struct RuntimeLifecycleConfigLatest {
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub instance_ttl_seconds: Option<u64>,
}

#[derive(Debug, Clone, Serialize, Deserialize, Default)]
#[serde(deny_unknown_fields)]
pub struct RuntimeEnvironmentConfig {
    #[serde(default)]
    pub inherited_env_vars: BTreeMap<String, Vec<String>>,
    #[serde(default = "defaults::default_profiler_enabled")]
    pub profiler_enabled: bool,
}

#[derive(Debug, Clone, Serialize, Deserialize, Default)]
pub struct RuntimeEnvironmentConfigLatest {
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub inherited_env_vars: Option<BTreeMap<String, Vec<String>>>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub profiler_enabled: Option<bool>,
}

/// Runtime storage and behavior overrides for mediapm document processing.
///
/// Fields here use `#[serde(default)]` to fill in defaults when omitted.
/// Path-override fields use `Option` (`None` = use computed default from
/// [`MediaPmPaths`](crate::paths::MediaPmPaths)).
#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct MediaRuntimeStorage {
    pub paths: RuntimePathsConfig,
    pub materialization: RuntimeMaterializationConfig,
    pub verification: RuntimeVerificationConfig,
    pub caching: RuntimeCachingConfig,
    pub lifecycle: RuntimeLifecycleConfig,
    pub environment: RuntimeEnvironmentConfig,
    #[serde(default = "defaults::default_path_sanitization")]
    pub path_sanitization: SanitizeNamesConfig,
    #[serde(default = "defaults::default_retry_impure")]
    pub retry_impure: bool,
    #[serde(default, skip)]
    pub tools: BTreeMap<String, ToolRequirement>,
    /// Sole exception to the no-Option policy; reason: testing only.
    #[doc(hidden)]
    #[serde(skip)]
    pub cache_root_override: Option<PathBuf>,
}

impl MediaRuntimeStorage {
    /// Map the typed verify-on-read configuration to CAS enum variants.
    ///
    /// The strategy names are closed at the serde boundary (S-E3), so this
    /// conversion is total: every decoded name maps to exactly one CAS variant.
    #[must_use]
    #[expect(
        clippy::cast_possible_truncation,
        reason = "config sample denominator is clamped to u32 at the CAS boundary; oversized values truncate intentionally"
    )]
    pub fn to_verify_strategies(&self) -> Vec<mediapm_cas::VerifyTriggerStrategy> {
        use mediapm_cas::VerifyTriggerStrategy;

        self.verification
            .verify_on_read
            .iter()
            .map(|strategy| match strategy {
                VerifyStrategy::Always => VerifyTriggerStrategy::Always,
                VerifyStrategy::Modified => VerifyTriggerStrategy::Modified,
                VerifyStrategy::Sample => VerifyTriggerStrategy::Sample {
                    denominator: self.verification.verify_on_read_sample_denominator.max(1) as u32,
                },
                VerifyStrategy::Stale => VerifyTriggerStrategy::Stale {
                    timeout: std::time::Duration::from_secs(
                        self.verification.verify_on_read_stale_timeout_secs,
                    ),
                },
            })
            .collect()
    }
}

impl Default for MediaRuntimeStorage {
    fn default() -> Self {
        MediaRuntimeStorage {
            paths: RuntimePathsConfig::default(),
            materialization: RuntimeMaterializationConfig::default(),
            verification: RuntimeVerificationConfig::default(),
            caching: RuntimeCachingConfig::default(),
            lifecycle: RuntimeLifecycleConfig::default(),
            environment: RuntimeEnvironmentConfig::default(),
            path_sanitization: defaults::default_path_sanitization(),
            retry_impure: defaults::default_retry_impure(),
            tools: BTreeMap::new(),
            cache_root_override: None,
        }
    }
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
    #[serde(default)]
    pub tools: BTreeMap<String, ToolRequirement>,
}

#[derive(Debug, Clone)]
pub struct RuntimeBasePaths {
    pub workspace_root: PathBuf,
    pub mediapm_dir: PathBuf,
}

impl MediaRuntimeStorage {
    pub fn from_boundary(
        latest: &MediaRuntimeStorageLatest,
        _base: &RuntimeBasePaths,
    ) -> MediaRuntimeStorage {
        // Unset boundary path values map to an empty PathBuf so the override
        // machinery (pick_path/opt_path/pathbuf_to_opt) treats them as "no
        // override" and the MediaPmPaths::from_root defaults win. Populating
        // defaults here would redirect saves to a different path than the one
        // MediaPmPaths exposes for reads.
        MediaRuntimeStorage {
            paths: RuntimePathsConfig {
                mediapm_dir: latest
                    .paths
                    .mediapm_dir
                    .clone()
                    .map(PathBuf::from)
                    .unwrap_or_default(),
                hierarchy_root_dir: latest
                    .paths
                    .hierarchy_root_dir
                    .clone()
                    .map(PathBuf::from)
                    .unwrap_or_default(),
                mediapm_state_config: latest
                    .paths
                    .mediapm_state_config
                    .clone()
                    .map(PathBuf::from)
                    .unwrap_or_default(),
                conductor_config: latest
                    .paths
                    .conductor_config
                    .clone()
                    .map(PathBuf::from)
                    .unwrap_or_default(),
                conductor_generated_config: latest
                    .paths
                    .conductor_generated_config
                    .clone()
                    .map(PathBuf::from)
                    .unwrap_or_default(),
                conductor_state_config: latest
                    .paths
                    .conductor_state_config
                    .clone()
                    .map(PathBuf::from)
                    .unwrap_or_default(),
                conductor_schema_dir: latest
                    .paths
                    .conductor_schema_dir
                    .clone()
                    .map(PathBuf::from)
                    .unwrap_or_default(),
                mediapm_schema_dir: latest
                    .paths
                    .mediapm_schema_dir
                    .clone()
                    .map(PathBuf::from)
                    .unwrap_or_default(),
                env_file: latest.paths.env_file.clone().map(PathBuf::from).unwrap_or_default(),
                env_generated_file: latest
                    .paths
                    .env_generated_file
                    .clone()
                    .map(PathBuf::from)
                    .unwrap_or_default(),
            },
            materialization: RuntimeMaterializationConfig {
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
            verification: RuntimeVerificationConfig {
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
            caching: RuntimeCachingConfig {
                reconstructed_cache_ttl_seconds: latest
                    .caching
                    .reconstructed_cache_ttl_seconds
                    .unwrap_or_else(defaults::default_reconstructed_cache_ttl_seconds),
            },
            lifecycle: RuntimeLifecycleConfig {
                instance_ttl_seconds: latest
                    .lifecycle
                    .instance_ttl_seconds
                    .unwrap_or_else(defaults::default_instance_ttl_seconds),
            },
            environment: RuntimeEnvironmentConfig {
                inherited_env_vars: latest
                    .environment
                    .inherited_env_vars
                    .clone()
                    .unwrap_or_default(),
                profiler_enabled: latest
                    .environment
                    .profiler_enabled
                    .unwrap_or_else(defaults::default_profiler_enabled),
            },
            path_sanitization: latest
                .path_sanitization
                .clone()
                .unwrap_or(SanitizeNamesConfig::Inherit),
            retry_impure: latest.retry_impure.unwrap_or(false),
            tools: latest.tools.clone(),
            cache_root_override: None,
        }
    }
}

// ---------------------------------------------------------------------------
// ToolRequirement
// ---------------------------------------------------------------------------

/// Managed tool version and dependency requirements.
#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct ToolRequirement {
    /// Version specification: "latest", "inherit", or { `vcs_hash`?, version?, tag? }.
    #[serde(default = "defaults::default_tool_version_spec")]
    pub version_spec: ConfigVersionSpec,
    /// Cross-tool dependency version selectors.
    #[serde(default)]
    pub dependencies: BTreeMap<String, ConfigVersionSpec>,
    /// Recheck interval seconds (0 = use default heuristic).
    #[serde(default, deserialize_with = "custom_deserializers::deserialize_u64_from_number")]
    pub recheck_seconds: u64,
    /// Max ffmpeg input slot count.
    #[serde(
        default = "defaults::default_ffmpeg_max_input_slots",
        deserialize_with = "custom_deserializers::deserialize_u32_from_number"
    )]
    pub max_input_slots: u32,
    /// Max ffmpeg output slot count.
    #[serde(
        default = "defaults::default_ffmpeg_max_output_slots",
        deserialize_with = "custom_deserializers::deserialize_u32_from_number"
    )]
    pub max_output_slots: u32,
}

impl Default for ToolRequirement {
    fn default() -> Self {
        Self {
            version_spec: ConfigVersionSpec::Latest,
            dependencies: BTreeMap::new(),
            recheck_seconds: 0,
            max_input_slots: defaults::DEFAULT_FFMPEG_MAX_INPUT_SLOTS,
            max_output_slots: defaults::DEFAULT_FFMPEG_MAX_OUTPUT_SLOTS,
        }
    }
}

impl ToolRequirement {
    /// Returns metadata recheck seconds (0 = use default heuristic).
    #[must_use]
    pub const fn metadata_recheck_seconds(&self) -> u64 {
        self.recheck_seconds
    }
}

// ---------------------------------------------------------------------------
// MediaPmDocument (top-level config)
// ---------------------------------------------------------------------------

/// Top-level mediapm document deserialized from `mediapm.ncl`.
#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct MediaPmDocument {
    /// Schema version marker.
    #[serde(default = "defaults::default_mediapm_document_version")]
    pub version: u32,
    /// Media source entries keyed by unique id.
    #[serde(default)]
    pub media: BTreeMap<String, source_types::MediaSourceSpec>,
    /// Hierarchy declaration.
    #[serde(default)]
    pub hierarchy: Vec<hierarchy_types::HierarchyNode>,
    /// Managed tool requirement declarations keyed by tool id.
    #[serde(default)]
    pub tools: BTreeMap<String, ToolRequirement>,
    /// Runtime configuration overrides.
    #[serde(default)]
    pub runtime: MediaRuntimeStorageLatest,
    /// Legacy `state` payload accepted for V1 documents.
    ///
    /// State is managed separately via `state.json`; the V2 schema drops this
    /// field, so it is accepted on read for legacy documents and never
    /// emitted on V2 writes.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub state: Option<MediaPmState>,
}

impl Default for MediaPmDocument {
    fn default() -> Self {
        Self {
            version: defaults::MEDIAPM_DOCUMENT_VERSION,
            media: BTreeMap::new(),
            hierarchy: Vec::new(),
            tools: BTreeMap::new(),
            runtime: MediaRuntimeStorageLatest::default(),
            state: None,
        }
    }
}

impl MediaPmDocument {
    /// Normalizes string fields (trimming whitespace).
    pub fn normalize(&mut self) {
        // Version is already a concrete u32, no trimming needed.
        // Trimming media source titles, descriptions, etc.
        for source in self.media.values_mut() {
            let trimmed = source.description.trim().to_string();
            source.description = trimmed;
            let trimmed = source.title.trim().to_string();
            source.title = trimmed;
            let trimmed = source.artist.trim().to_string();
            source.artist = trimmed;
        }
        // Remove tool entries that are Latest with no explicit dependencies.
        self.tools.retain(|_, tool_req| {
            tool_req.version_spec != ConfigVersionSpec::Latest || !tool_req.dependencies.is_empty()
        });
    }
}

// ---------------------------------------------------------------------------
// MediaPmState (persisted machine state)
// ---------------------------------------------------------------------------

/// Per-media-source workflow step state.
#[derive(Debug, Clone, Default, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct ManagedWorkflowStepState {
    /// Pre-seeded CAS hash pointers keyed by variant name.
    #[serde(default)]
    pub variant_hashes: BTreeMap<String, String>,
    /// Number of completed steps (0 = none).
    #[serde(default)]
    pub steps_completed: u32,
    /// Optional last impure sync timestamp.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub last_impure_sync_at: Option<mediapm_utils::Timestamp>,
}

/// Entry in the managed-tool registry tracking fetch/deployment metadata.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct ToolRegistryEntry {
    /// Tool identifier matching the key in `desired_tools`.
    pub tool_id: String,
    /// Human-readable version string. Has zero semantic use in state logic —
    /// version comparison, skip-if-up-to-date, and update decisions all use
    /// `canonical_version`. This field is informational only, populated by
    /// the provider Resolution layer. The provider determines the format; no
    /// prefix stripping or normalization is performed.
    pub version: String,
    /// Canonical version identifier for skip-if-up-to-date logic.
    /// Non-optional — always populated by the provisioning pipeline.
    /// Defaults to empty string (`""`) for backward-compat with old state files.
    #[serde(default)]
    pub canonical_version: String,
    /// blake3 hash of the `content_map` JSON (used for content-addressed identity).
    #[serde(default)]
    pub content_map_hash: String,
    /// Unix-nano timestamp when the payload was deployed (0 = not yet deployed).
    #[serde(default)]
    pub deployed_at: mediapm_utils::Timestamp,
    /// The git tag that was resolved during the last resolve phase.
    /// `None` (JSON `null`) when the provider does not resolve from tags;
    /// empty strings never occur and are rejected at load. Any `None` field
    /// must carry a documented why-empty reason in the provider dispatch arm,
    /// the provider module doc, and `provider-dispatch.instructions.md`.
    #[serde(
        default,
        deserialize_with = "custom_deserializers::deserialize_optional_nonempty_string"
    )]
    pub resolved_tag: Option<String>,
    /// The version string that was resolved during the last resolve phase.
    /// `None` (JSON `null`) when the provider does not produce a version
    /// string; empty strings never occur and are rejected at load.
    /// Why-empty documentation applies (see `resolved_tag`).
    #[serde(
        default,
        deserialize_with = "custom_deserializers::deserialize_optional_nonempty_string"
    )]
    pub resolved_version: Option<String>,
    /// The VCS hash that was resolved during the last resolve phase.
    /// `None` (JSON `null`) when the provider does not resolve from hashes;
    /// empty strings never occur and are rejected at load. Why-empty
    /// documentation applies (see `resolved_tag`).
    #[serde(
        default,
        deserialize_with = "custom_deserializers::deserialize_optional_nonempty_string"
    )]
    pub resolved_vcs_hash: Option<String>,
}

/// Managed file record stored in persisted state.
///
/// Tracks each materialized output file with its originating media source,
/// variant name, and content hash for integrity verification.
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
#[serde(deny_unknown_fields)]
pub struct ManagedFileRecord {
    /// Media source id that produced this file.
    pub media_id: String,
    /// Output variant name.
    pub variant: String,
    /// Content hash (blake3:...).
    pub hash: String,
}

/// Persisted mediapm machine state (`state.json`).
///
/// V3 format with `managed_files` (path → record map), `managed_tools`
/// (flat tool deployment metadata list), and `workflow_states` (per-media
/// workflow progress). No longer stores tool requirements, active instances,
/// or last-materialization hash — the document config owns those.
#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct MediaPmState {
    /// Schema version marker.
    #[serde(default = "defaults::default_mediapm_state_version")]
    pub version: u32,
    /// Managed files keyed by filesystem path.
    #[serde(default)]
    pub managed_files: BTreeMap<String, ManagedFileRecord>,
    /// Managed tool deployment metadata (flat list).
    #[serde(default)]
    pub managed_tools: Vec<ToolRegistryEntry>,
    /// Per-media-source workflow state.
    #[serde(default)]
    pub workflow_states: BTreeMap<String, ManagedWorkflowStepState>,
}

impl Default for MediaPmState {
    fn default() -> Self {
        Self {
            version: defaults::MEDIAPM_STATE_VERSION,
            managed_files: BTreeMap::new(),
            managed_tools: Vec::new(),
            workflow_states: BTreeMap::new(),
        }
    }
}

impl MediaPmState {
    /// Normalizes string fields in managed file records and tool entries.
    pub fn normalize(&mut self) {
        self.managed_files.retain(|path, record| {
            !path.trim().is_empty()
                && !record.media_id.trim().is_empty()
                && !record.hash.trim().is_empty()
        });
        self.managed_tools.retain(|entry| {
            !entry.canonical_version.trim().is_empty()
                || entry.resolved_tag.is_some()
                || entry.resolved_version.is_some()
                || entry.resolved_vcs_hash.is_some()
        });
    }
}
