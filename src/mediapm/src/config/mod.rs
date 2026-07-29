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
    DecodedOutputVariantConfig, GenericOutputVariantConfig, OutputCaptureKind, OutputSaveConfig,
    YtDlpOutputKind, YtDlpOutputVariantConfig,
};
pub use source_types::{
    MediaMetadataRegexTransform, MediaMetadataValue, MediaMetadataValueCandidate,
    MediaMetadataVariantBinding, MediaSourceSpec, MediaStep, MediaStepTool, TransformInputValue,
};

use std::collections::BTreeMap;

use mediapm_conductor::tools::provider::VersionSpec;
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
// MediaRuntimeStorage
// ---------------------------------------------------------------------------

/// Runtime storage and behavior overrides for mediapm document processing.
///
/// Fields here use `#[serde(default)]` to fill in defaults when omitted.
/// Path-override fields use `Option` (`None` = use computed default from
/// [`MediaPmPaths`](crate::paths::MediaPmPaths)).
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct MediaRuntimeStorage {
    /// Override for `mediapm.ncl` `runtime.mediapm_dir`.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub mediapm_dir: Option<String>,
    /// Override for hierarchy root directory.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub hierarchy_root_dir: Option<String>,
    /// Tool requirement metadata (serde-skipped; tools are now at
    /// [`MediaPmDocument::tools`](MediaPmDocument)).
    #[serde(default, skip)]
    pub tools: BTreeMap<String, ToolRequirement>,
    /// Materialization method preference order.
    #[serde(
        default = "defaults::default_materialization_preference_order",
        deserialize_with = "deserialize_materialization_preference_order"
    )]
    pub materialization_preference_order: Vec<MaterializationMethod>,
    /// Verify-on-read strategy.
    #[serde(default = "defaults::default_verify_on_read")]
    pub verify_on_read: Vec<String>,
    /// Verify-on-read sampling denominator.
    #[serde(default = "defaults::default_verify_on_read_sample_denominator")]
    pub verify_on_read_sample_denominator: u64,
    /// Verify-on-read stale timeout seconds.
    #[serde(default = "defaults::default_verify_on_read_stale_timeout_secs")]
    pub verify_on_read_stale_timeout_secs: u64,
    /// Reconstructed cache TTL seconds.
    #[serde(default = "defaults::default_reconstructed_cache_ttl_seconds")]
    pub reconstructed_cache_ttl_seconds: u64,
    /// Instance TTL seconds.
    #[serde(default = "defaults::default_instance_ttl_seconds")]
    pub instance_ttl_seconds: u64,
    /// Inherited environment variables for managed tools.
    #[serde(default)]
    pub inherited_env_vars: BTreeMap<String, Vec<String>>,
    /// Media state overrides.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub media_state_config: Option<String>,
    /// Override for conductor user config path.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub conductor_config: Option<String>,
    /// Override for conductor generated config path.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub conductor_generated_config: Option<String>,
    /// Override for conductor state config path.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub conductor_state_config: Option<String>,
    /// Override for conductor schema directory.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub conductor_schema_dir: Option<String>,
    /// Override for user-authored dotenv file path.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub env_file: Option<String>,
    /// Override for auto-generated dotenv file path.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub env_generated_file: Option<String>,
    /// Override for mediapm schema export directory (`None` = use computed,
    /// `Some(None)` = disable export).
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub mediapm_schema_dir: Option<Option<String>>,
    /// Enable runtime profiler.
    #[serde(default = "defaults::default_profiler_enabled")]
    pub profiler_enabled: bool,
    /// Verify CAS→filesystem hash after materialization.
    #[serde(default = "defaults::default_verify_materialization")]
    pub verify_materialization: bool,
    /// Retry impure workflows flag.
    #[serde(default = "defaults::default_retry_impure")]
    pub retry_impure: bool,
    /// Hierarchy filename sanitization mode.
    #[serde(default = "defaults::default_path_sanitization")]
    pub path_sanitization: hierarchy_types::SanitizeNamesConfig,
    /// Override for the download cache root directory.
    ///
    /// Only intended for test use; hidden from documentation. When set, all
    /// download cache operations use this path instead of the OS-level default.
    #[doc(hidden)]
    #[serde(skip)]
    pub cache_root_override: Option<std::path::PathBuf>,
}

impl MediaRuntimeStorage {
    /// Map the string-based verify-on-read configuration to CAS enum variants.
    ///
    /// Unknown strategy names are silently ignored.
    #[must_use]
    pub fn to_verify_strategies(&self) -> Vec<mediapm_cas::VerifyTriggerStrategy> {
        use mediapm_cas::VerifyTriggerStrategy;

        let mut strategies: Vec<VerifyTriggerStrategy> = Vec::new();

        for name in &self.verify_on_read {
            match name.as_str() {
                "always" => strategies.push(VerifyTriggerStrategy::Always),
                "modified" => strategies.push(VerifyTriggerStrategy::Modified),
                "sample" => strategies.push(VerifyTriggerStrategy::Sample {
                    denominator: self.verify_on_read_sample_denominator.max(1) as u32,
                }),
                "stale" => strategies.push(VerifyTriggerStrategy::Stale {
                    timeout: std::time::Duration::from_secs(self.verify_on_read_stale_timeout_secs),
                }),
                _ => {
                    // Unknown strategy names are silently ignored.
                }
            }
        }

        strategies
    }
}

impl Default for MediaRuntimeStorage {
    fn default() -> Self {
        Self {
            mediapm_dir: None,
            hierarchy_root_dir: None,
            tools: BTreeMap::new(),
            materialization_preference_order: defaults::default_materialization_preference_order(),
            verify_on_read: defaults::default_verify_on_read(),
            verify_on_read_sample_denominator: defaults::default_verify_on_read_sample_denominator(
            ),
            verify_on_read_stale_timeout_secs: defaults::default_verify_on_read_stale_timeout_secs(
            ),
            reconstructed_cache_ttl_seconds: defaults::default_reconstructed_cache_ttl_seconds(),
            instance_ttl_seconds: defaults::default_instance_ttl_seconds(),
            inherited_env_vars: BTreeMap::new(),
            media_state_config: None,
            conductor_config: None,
            conductor_generated_config: None,
            conductor_state_config: None,
            conductor_schema_dir: None,
            env_file: None,
            env_generated_file: None,
            mediapm_schema_dir: None,
            profiler_enabled: defaults::default_profiler_enabled(),
            verify_materialization: defaults::default_verify_materialization(),
            retry_impure: defaults::default_retry_impure(),
            path_sanitization: defaults::default_path_sanitization(),
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
    /// Version specification: "latest", "inherit", or { vcs_hash?, version?, tag? }.
    #[serde(default = "defaults::default_tool_version_spec")]
    pub version_spec: VersionSpec,
    /// Cross-tool dependency version selectors.
    #[serde(default)]
    pub dependencies: BTreeMap<String, VersionSpec>,
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
            version_spec: VersionSpec::Latest,
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
    pub runtime: MediaRuntimeStorage,
}

impl Default for MediaPmDocument {
    fn default() -> Self {
        Self {
            version: defaults::MEDIAPM_DOCUMENT_VERSION,
            media: BTreeMap::new(),
            hierarchy: Vec::new(),
            tools: BTreeMap::new(),
            runtime: MediaRuntimeStorage::default(),
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
            tool_req.version_spec != VersionSpec::Latest || !tool_req.dependencies.is_empty()
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
    pub last_impure_sync_at: Option<MediaPmImpureTimestamp>,
}

/// Impure sync timestamp tracked per media source.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct MediaPmImpureTimestamp {
    /// Seconds since Unix epoch when the last impure sync occurred.
    pub utc_epoch_seconds: u64,
}

/// Entry in the managed-tool registry tracking fetch/deployment metadata.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct ToolRegistryEntry {
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
    /// blake3 hash of the content_map JSON (used for content-addressed identity).
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub content_map_hash: Option<String>,
    /// Unix-epoch seconds when the payload was deployed (0 = not yet deployed).
    #[serde(default)]
    pub deployed_at: u64,
    /// The git tag that was resolved during the last resolve phase.
    /// Empty string if the provider does not resolve from tags.
    #[serde(default)]
    pub resolved_tag: String,
    /// The version string that was resolved during the last resolve phase.
    /// Empty string if the provider does not produce a version string.
    #[serde(default)]
    pub resolved_version: String,
    /// The VCS hash that was resolved during the last resolve phase.
    /// Empty string if the provider does not resolve from hashes.
    #[serde(default)]
    pub resolved_vcs_hash: String,
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
/// V2 format with `managed_files` (path → record map), `managed_tools`
/// (tool deployment metadata), and `workflow_states` (per-media workflow
/// progress). No longer stores tool requirements, active instances, or
/// last-materialization hash — the document config owns those.
#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct MediaPmState {
    /// Schema version marker.
    #[serde(default = "defaults::default_mediapm_state_version")]
    pub version: u32,
    /// Managed files keyed by filesystem path.
    #[serde(default)]
    pub managed_files: BTreeMap<String, ManagedFileRecord>,
    /// Managed tool deployment metadata keyed by tool id.
    #[serde(default)]
    pub managed_tools: BTreeMap<String, ToolRegistryEntry>,
    /// Per-media-source workflow state.
    #[serde(default)]
    pub workflow_states: BTreeMap<String, ManagedWorkflowStepState>,
}

impl Default for MediaPmState {
    fn default() -> Self {
        Self {
            version: defaults::MEDIAPM_STATE_VERSION,
            managed_files: BTreeMap::new(),
            managed_tools: BTreeMap::new(),
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
        self.managed_tools.retain(|_, entry| {
            !entry.canonical_version.trim().is_empty()
                || !entry.resolved_tag.trim().is_empty()
                || !entry.resolved_version.trim().is_empty()
                || !entry.resolved_vcs_hash.trim().is_empty()
        });
    }
}
