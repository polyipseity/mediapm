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
//! | [`nickel_io`] | Evaluate `.ncl` files to JSON, render terms |
//! | [`versions`] | Schema version dispatch and V1 envelope types |
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

use std::collections::{BTreeMap, BTreeSet};

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
#[serde(deny_unknown_fields)]
pub struct MediaRuntimeStorage {
    /// Override for `mediapm.ncl` `runtime.mediapm_dir`.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub mediapm_dir: Option<String>,
    /// Override for hierarchy root directory.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub hierarchy_root_dir: Option<String>,
    /// Tool requirement metadata.
    #[serde(default)]
    pub tools: BTreeMap<String, ToolRequirement>,
    /// Managed tool version requirements.
    #[serde(default)]
    pub tool_configs: BTreeMap<String, Value>,
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
    #[serde(default)]
    pub retry_impure: bool,
    /// Hierarchy filename sanitization mode.
    #[serde(default = "defaults::default_path_sanitization")]
    pub path_sanitization: hierarchy_types::SanitizeNamesConfig,
}

impl Default for MediaRuntimeStorage {
    fn default() -> Self {
        Self {
            mediapm_dir: None,
            hierarchy_root_dir: None,
            tools: BTreeMap::new(),
            tool_configs: BTreeMap::new(),
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
            retry_impure: false,
            path_sanitization: defaults::default_path_sanitization(),
        }
    }
}

// ---------------------------------------------------------------------------
// ToolRequirement
// ---------------------------------------------------------------------------

/// Managed tool version and dependency requirements.
#[derive(Debug, Clone, Default, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct ToolRequirement {
    /// Version metadata value or selector binding.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub version: Option<MediaMetadataValue>,
    /// Tag metadata value or selector binding.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub tag: Option<String>,
    /// Cross-tool dependency version selectors.
    #[serde(default)]
    pub dependencies: ToolRequirementDependencies,
    /// Recheck interval seconds (None = use default heuristic).
    #[serde(
        default,
        deserialize_with = "custom_deserializers::deserialize_optional_u64_from_number",
        skip_serializing_if = "Option::is_none"
    )]
    pub recheck_seconds: Option<u64>,
    /// Max ffmpeg input slot count.
    #[serde(
        default,
        deserialize_with = "custom_deserializers::deserialize_optional_runtime_slot_count",
        skip_serializing_if = "Option::is_none"
    )]
    pub max_input_slots: Option<u32>,
    /// Max ffmpeg output slot count.
    #[serde(
        default,
        deserialize_with = "custom_deserializers::deserialize_optional_runtime_slot_count",
        skip_serializing_if = "Option::is_none"
    )]
    pub max_output_slots: Option<u32>,
}

/// Selector-based dependency version requirements for managed tools.
#[derive(Debug, Clone, Default, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct ToolRequirementDependencies {
    /// Selector or literal version for ffmpeg dependency.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub ffmpeg_version: Option<MediaMetadataValue>,
    /// Selector or literal version for deno dependency.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub deno_version: Option<MediaMetadataValue>,
    /// Selector or literal version for sd (stable-diffusion) dependency.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub sd_version: Option<MediaMetadataValue>,
}

impl ToolRequirement {
    /// Returns the normalized version string from the version selector.
    #[must_use]
    pub fn normalized_version(&self) -> Option<String> {
        self.version.as_ref().and_then(|v| {
            if let MediaMetadataValue::Literal(s) = v {
                let trimmed = s.trim();
                if trimmed.is_empty() { None } else { Some(trimmed.to_string()) }
            } else {
                None
            }
        })
    }

    /// Returns the normalized tag string.
    #[must_use]
    pub fn normalized_tag(&self) -> Option<String> {
        self.tag.as_ref().map(|t| t.trim().to_string()).filter(|t| !t.is_empty())
    }

    /// Returns effective max input slots or the default.
    #[must_use]
    pub fn max_input_slots_or_default(self) -> u32 {
        self.max_input_slots.unwrap_or(defaults::DEFAULT_FFMPEG_MAX_INPUT_SLOTS)
    }

    /// Returns effective max output slots or the default.
    #[must_use]
    pub fn max_output_slots_or_default(self) -> u32 {
        self.max_output_slots.unwrap_or(defaults::DEFAULT_FFMPEG_MAX_OUTPUT_SLOTS)
    }

    /// Returns metadata recheck seconds or None (caller uses heuristic).
    #[must_use]
    pub const fn metadata_recheck_seconds(&self) -> Option<u64> {
        self.recheck_seconds
    }
}

/// Returns true when `ToolRequirementDependencies` has no active selector.
#[must_use]
#[allow(dead_code)]
pub fn tool_requirement_dependencies_is_empty(deps: &ToolRequirementDependencies) -> bool {
    deps.ffmpeg_version.is_none() && deps.deno_version.is_none() && deps.sd_version.is_none()
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
    /// Runtime configuration overrides.
    #[serde(default)]
    pub runtime: MediaRuntimeStorage,
    /// Conductor config (opaque passthrough).
    #[serde(default)]
    pub conductor: BTreeMap<String, Value>,
}

impl Default for MediaPmDocument {
    fn default() -> Self {
        Self {
            version: defaults::MEDIAPM_DOCUMENT_VERSION,
            media: BTreeMap::new(),
            hierarchy: Vec::new(),
            runtime: MediaRuntimeStorage::default(),
            conductor: BTreeMap::new(),
        }
    }
}

impl MediaPmDocument {
    /// Normalizes string fields (trimming whitespace).
    pub fn normalize(&mut self) {
        // Version is already a concrete u32, no trimming needed.
        // Trimming media source titles, descriptions, etc.
        for source in self.media.values_mut() {
            if let Some(ref mut description) = source.description {
                let trimmed = description.trim().to_string();
                if trimmed.is_empty() {
                    source.description = None;
                } else {
                    *description = trimmed;
                }
            }
            if let Some(ref mut title) = source.title {
                let trimmed = title.trim().to_string();
                if trimmed.is_empty() {
                    source.title = None;
                } else {
                    *title = trimmed;
                }
            }
            if let Some(ref mut artist) = source.artist {
                let trimmed = artist.trim().to_string();
                if trimmed.is_empty() {
                    source.artist = None;
                } else {
                    *artist = trimmed;
                }
            }
        }
    }
}

// ---------------------------------------------------------------------------
// MediaPmState (persisted machine state)
// ---------------------------------------------------------------------------

/// Per-media-source workflow step state.
#[derive(Debug, Clone, Default, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct ManagedWorkflowStepState {
    /// Pre-seeded CAS hash pointers keyed by variant name.
    #[serde(default)]
    pub variant_hashes: BTreeMap<String, String>,
    /// Optional number of completed steps.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub steps_completed: Option<u32>,
    /// Optional last impure sync timestamp.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub last_impure_sync_at: Option<MediaPmImpureTimestamp>,
}

/// Impure sync timestamp tracked per media source.
#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct MediaPmImpureTimestamp {
    /// Seconds since Unix epoch when the last impure sync occurred.
    pub utc_epoch_seconds: u64,
}

/// Entry in the managed-tool registry tracking fetch/deployment metadata.
#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct ToolRegistryEntry {
    /// Tool version as fetched.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub version: Option<String>,
    /// Tag as fetched.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub tag: Option<String>,
    /// CAS content hash of the fetched payload.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub fetch_hash: Option<String>,
    /// Unix-epoch seconds when the payload was deployed.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub deployed_at: Option<u64>,
}

/// Active instance of a managed tool deployed to the local filesystem.
#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct ActiveToolInstance {
    /// Tool identifier used for registry lookups.
    pub tool_id: String,
    /// CAS content hash of the deployed payload.
    pub content_hash: String,
    /// Filesystem path to the deployed executable or bundle.
    pub deployed_path: String,
}

/// Persisted mediapm machine state (`state.ncl`).
#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct MediaPmState {
    /// Schema version marker.
    #[serde(default = "defaults::default_mediapm_document_version")]
    pub version: u32,
    /// Per-media-source workflow state.
    #[serde(default)]
    pub media: BTreeMap<String, ManagedWorkflowStepState>,
    /// Tool registry version/tracking state.
    #[serde(default)]
    pub tools: BTreeMap<String, ToolRequirement>,
    /// Hash of the state snapshot at last materialization.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub last_materialized_state_hash: Option<String>,
    /// Set of files currently managed (tracked for cleanup).
    #[serde(default)]
    pub managed_files: BTreeSet<String>,
    /// Fetched-tool registry keyed by tool id.
    #[serde(default)]
    pub tool_registry: BTreeMap<String, ToolRegistryEntry>,
    /// Active tool deployments keyed by tool id.
    #[serde(default)]
    pub active_tools: BTreeMap<String, ActiveToolInstance>,
}

impl Default for MediaPmState {
    fn default() -> Self {
        Self {
            version: defaults::MEDIAPM_DOCUMENT_VERSION,
            media: BTreeMap::new(),
            tools: BTreeMap::new(),
            last_materialized_state_hash: None,
            managed_files: BTreeSet::new(),
            tool_registry: BTreeMap::new(),
            active_tools: BTreeMap::new(),
        }
    }
}

impl MediaPmState {
    /// Normalizes string fields and removes empty tool entries.
    pub fn normalize(&mut self) {
        self.tools.retain(|_, tool_req| {
            normalized_version(tool_req.version.as_ref()).is_some()
                || normalized_tag(tool_req.tag.as_ref()).is_some()
        });
        self.tool_registry.retain(|_, entry| {
            entry.version.as_ref().is_none_or(|v| !v.trim().is_empty())
                || entry.tag.as_ref().is_none_or(|t| !t.trim().is_empty())
        });
        self.managed_files.retain(|f| !f.trim().is_empty());
    }
}

/// Helper: normalize a version metadata selector to trimmed Option.
#[must_use]
fn normalized_version(version: Option<&MediaMetadataValue>) -> Option<String> {
    match version? {
        MediaMetadataValue::Literal(s) => {
            let trimmed = s.trim().to_string();
            if trimmed.is_empty() { None } else { Some(trimmed) }
        }
        _ => None,
    }
}

/// Helper: normalize a tag string to trimmed Option.
#[must_use]
fn normalized_tag(tag: Option<&String>) -> Option<String> {
    let trimmed = tag?.trim().to_string();
    if trimmed.is_empty() { None } else { Some(trimmed) }
}
