//! Nickel-backed `mediapm.ncl` and `state.ncl` document model and I/O
//! helpers.
//!
//! The `mediapm.ncl` file is the declarative desired-state surface for
//! mediapm: media sources, hierarchy mapping, and desired tool enablement.
//! The `state.ncl` file is the machine-managed realized-state surface. Both
//! documents share the same versioned Nickel schema and merge into one runtime
//! `MediaPmDocument`.
//!
//! We evaluate Nickel through `nickel-lang-core` and deserialize the exported
//! value into Rust structs. This keeps parsing behavior deterministic while still
//! supporting regular Nickel syntax in user-authored files.

use std::collections::BTreeMap;
use std::fmt;
use std::fs;
use std::path::Path;

use mediapm_conductor::default_runtime_inherited_env_vars_for_host;
use serde::{Deserialize, Deserializer, Serialize};
use serde_json::Value;

use crate::error::MediaPmError;

pub(crate) mod versions;

/// Current persisted schema marker for `mediapm.ncl`.
pub const MEDIAPM_DOCUMENT_VERSION: u32 = versions::latest_nickel_version();

/// Default max number of ffmpeg indexed input slots when `tools.ffmpeg`
/// does not provide an explicit override.
pub const DEFAULT_FFMPEG_MAX_INPUT_SLOTS: u32 = 16;
/// Default max number of ffmpeg indexed output slots when `tools.ffmpeg`
/// does not provide an explicit override.
pub const DEFAULT_FFMPEG_MAX_OUTPUT_SLOTS: u32 = 4;

/// Default runtime materialization fallback order.
///
/// The order is intentionally deterministic so managed-file realization remains
/// predictable across hosts and repeated sync runs.
pub const DEFAULT_MATERIALIZATION_PREFERENCE_ORDER: [MaterializationMethod; 4] = [
    MaterializationMethod::Hardlink,
    MaterializationMethod::Symlink,
    MaterializationMethod::Reflink,
    MaterializationMethod::Copy,
];

/// Platform-keyed inherited environment-variable names.
///
/// Keys are normalized case-insensitively at merge/read time so users can
/// author values with natural casing (`windows`, `Windows`, `WINDOWS`, ...)
/// without changing runtime semantics.
pub type PlatformInheritedEnvVars = BTreeMap<String, Vec<String>>;

mod hierarchy_types;
mod output_types;
mod source_types;

use self::hierarchy_types::{
    deserialize_hierarchy_node_list, deserialize_variant_selector_list, playlist_format_is_default,
    serialize_hierarchy_node_list, serialize_variant_selector_list,
};
use self::source_types::{has_step_option_scalar, step_option_scalar};

#[cfg(test)]
pub(crate) use self::hierarchy_types::hierarchy_nodes_from_flat_entries;
pub(crate) use self::hierarchy_types::{
    FlattenedHierarchyEntry, expand_variant_selectors, flatten_hierarchy_nodes_for_runtime,
};
pub use self::hierarchy_types::{
    HierarchyEntry, HierarchyEntryKind, HierarchyFolderRenameRule, HierarchyNode,
    HierarchyNodeKind, PlaylistEntryPathMode, PlaylistFormat, PlaylistItemRef, SanitizeNamesConfig,
    flatten_hierarchy_value, nest_hierarchy_value, regex_variant_selector,
};

pub use self::source_types::{
    MediaMetadataRegexTransform, MediaMetadataValue, MediaMetadataValueCandidate,
    MediaMetadataVariantBinding, MediaSourceSpec, MediaStep, MediaStepTool, TransformInputValue,
};

pub use self::output_types::OutputSaveConfig;
pub(crate) use self::output_types::{
    DecodedOutputVariantConfig, OutputCaptureKind, ResolvedStepVariantFlow, YtDlpOutputKind,
    YtDlpOutputVariantConfig, decode_output_variant_config, decode_output_variant_policy,
    resolve_step_variant_flow,
};
/// Top-level mediapm Nickel document.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct MediaPmDocument {
    /// Explicit schema marker for migration safety.
    pub version: u32,
    /// Optional runtime-path overrides for mediapm local state.
    #[serde(default)]
    pub runtime: MediaRuntimeStorage,
    /// Declarative desired tool requirements keyed by logical tool name.
    #[serde(default)]
    pub tools: BTreeMap<String, ToolRequirement>,
    /// Media source registry keyed by stable media id.
    #[serde(default)]
    pub media: BTreeMap<String, MediaSourceSpec>,
    /// Ordered hierarchy node declarations.
    ///
    /// Persisted schema uses an explicit node list (with recursive `children`)
    /// instead of map-based path keys so author order stays stable.
    #[serde(
        default,
        deserialize_with = "deserialize_hierarchy_node_list",
        serialize_with = "serialize_hierarchy_node_list"
    )]
    pub hierarchy: Vec<HierarchyNode>,
    /// Machine-managed realized state loaded from `state.ncl`.
    ///
    /// Config loads may omit this field; runtime merges the state document
    /// after resolving runtime-storage paths.
    #[serde(default, skip_serializing_if = "mediapm_state_is_empty")]
    pub state: MediaPmState,
}

impl Default for MediaPmDocument {
    fn default() -> Self {
        Self {
            version: MEDIAPM_DOCUMENT_VERSION,
            runtime: MediaRuntimeStorage::default(),
            tools: BTreeMap::new(),
            media: BTreeMap::new(),
            hierarchy: Vec::new(),
            state: MediaPmState::default(),
        }
    }
}

/// Machine-managed realized state persisted by `mediapm`.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize, Default)]
#[serde(deny_unknown_fields)]
pub struct MediaPmState {
    /// Materialized path registry keyed by relative target path.
    #[serde(default)]
    pub managed_files: BTreeMap<String, ManagedFileRecord>,
    /// Tool registry mirror keyed by immutable tool id.
    #[serde(default)]
    pub tool_registry: BTreeMap<String, ToolRegistryRecord>,
    /// Active tool id per logical tool name.
    #[serde(default)]
    pub active_tools: BTreeMap<String, String>,
    /// Managed workflow step refresh state grouped by media id.
    ///
    /// This ordered list keeps prior explicit step snapshots in synthesis
    /// order so reconciliation can forward-scan for exact matches.
    ///
    /// Matching policy is intentionally strict and order-aware:
    /// - for each current step, scan for the first exact `explicit_config`
    ///   match after the last matched index,
    /// - refresh when no exact match exists,
    /// - after an exact match, refresh only when the matched
    ///   `impure_timestamp` is missing.
    #[serde(default)]
    pub workflow_states: BTreeMap<String, Vec<ManagedWorkflowStepState>>,
}

/// Timezone-independent mediapm step-refresh timestamp.
///
/// This wire shape is mediapm-local state (separate from conductor runtime
/// instance timestamps) and accepts integral float values exported by Nickel
/// during decode.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize, Default)]
#[serde(deny_unknown_fields)]
pub struct MediaPmImpureTimestamp {
    /// Whole seconds since Unix epoch (UTC).
    #[serde(deserialize_with = "deserialize_u64_from_number")]
    pub epoch_seconds: u64,
    /// Nanoseconds within `epoch_seconds`, in range `0..=999_999_999`.
    #[serde(deserialize_with = "deserialize_u32_from_number")]
    pub subsec_nanos: u32,
}

/// Machine-managed refresh state for one workflow step.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct ManagedWorkflowStepState {
    /// Explicit user-authored step config snapshot.
    ///
    /// This value is serialized from `media.<id>.steps[<index>]` exactly as
    /// authored (with serde default elision semantics), so implicit managed
    /// defaults do not appear in the snapshot and therefore do not count as
    /// user-facing config changes.
    pub explicit_config: Value,
    /// Last mediapm-managed impure timestamp used for this step.
    ///
    /// `None` means this step must refresh on the next reconciliation pass.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub impure_timestamp: Option<MediaPmImpureTimestamp>,
}

/// Returns whether one [`MediaPmState`] value keeps default-empty behavior.
fn mediapm_state_is_empty(value: &MediaPmState) -> bool {
    value.managed_files.is_empty()
        && value.tool_registry.is_empty()
        && value.active_tools.is_empty()
        && value.workflow_states.is_empty()
}

/// Materialized file ledger entry.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct ManagedFileRecord {
    /// Media id that produced this path.
    pub media_id: String,
    /// Variant id selected for this materialized output.
    pub variant: String,
    /// Canonical CAS hash string for this exact materialized file payload.
    ///
    /// This identity is used by machine-config external-data reconciliation so
    /// all managed file bytes remain rooted in conductor persistence metadata.
    pub hash: String,
    /// Last successful sync timestamp in Unix epoch milliseconds.
    ///
    /// `mediapm` uses explicit unit-suffixed epoch fields to match CAS-style
    /// timestamp conventions.
    #[serde(deserialize_with = "deserialize_u64_from_number")]
    pub last_synced_unix_millis: u64,
}

/// Safety-pinned external-data entry.
/// Tool lifecycle status tracked by `mediapm` state.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum ToolRegistryStatus {
    /// Tool binary/config is present and expected to be runnable.
    Active,
    /// Tool binary was intentionally pruned while metadata remains.
    Pruned,
}

/// Materialization method used when writing managed hierarchy files.
///
/// Runtime attempts methods in configured order and stops on the first
/// successful realization for each file.
#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Serialize, Deserialize)]
#[serde(rename_all = "lowercase")]
pub enum MaterializationMethod {
    /// Realize output as a hard link to CAS object bytes.
    Hardlink,
    /// Realize output as a filesystem symlink to CAS object bytes.
    Symlink,
    /// Realize output via copy-on-write reflink/clone semantics.
    Reflink,
    /// Realize output by copying bytes from CAS object payload.
    Copy,
}

impl MaterializationMethod {
    /// Returns stable config/diagnostic label for this method.
    #[must_use]
    pub const fn as_label(self) -> &'static str {
        match self {
            Self::Hardlink => "hardlink",
            Self::Symlink => "symlink",
            Self::Reflink => "reflink",
            Self::Copy => "copy",
        }
    }
}

impl fmt::Display for MaterializationMethod {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter.write_str(self.as_label())
    }
}

/// Deserializes one non-negative integral number into `u64`.
fn deserialize_u64_from_number<'de, D>(deserializer: D) -> Result<u64, D::Error>
where
    D: serde::Deserializer<'de>,
{
    let value = Value::deserialize(deserializer)?;

    if let Some(raw) = value.as_u64() {
        return Ok(raw);
    }
    if let Some(raw) = value.as_f64()
        && let Some(normalized) = parse_non_negative_integral_u64(raw)
    {
        return Ok(normalized);
    }

    Err(serde::de::Error::custom("expected one non-negative integral number representable as u64"))
}

/// Deserializes one non-negative integral number into `u32`.
fn deserialize_u32_from_number<'de, D>(deserializer: D) -> Result<u32, D::Error>
where
    D: serde::Deserializer<'de>,
{
    let value = Value::deserialize(deserializer)?;

    if let Some(raw) = value.as_u64() {
        return u32::try_from(raw).map_err(|_| {
            serde::de::Error::custom(
                "expected one non-negative integral number representable as u32",
            )
        });
    }
    if let Some(raw) = value.as_f64()
        && let Some(normalized) = parse_non_negative_integral_u32(raw)
    {
        return Ok(normalized);
    }

    Err(serde::de::Error::custom("expected one non-negative integral number representable as u32"))
}

/// Tool registry metadata persisted in `mediapm` state.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct ToolRegistryRecord {
    /// Logical tool name without version suffix.
    pub name: String,
    /// Catalog release track recorded at activation time.
    pub version: String,
    /// Catalog source label used for this registration.
    pub source: String,
    /// Content-derived multihash fingerprint used for validation bookkeeping.
    pub registry_multihash: String,
    /// Last status transition timestamp in Unix seconds.
    #[serde(deserialize_with = "deserialize_u64_from_number")]
    pub last_transition_unix_seconds: u64,
    /// Current lifecycle state.
    pub status: ToolRegistryStatus,
}

/// Runtime path overrides for mediapm local state.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize, Default)]
#[serde(deny_unknown_fields)]
pub struct MediaRuntimeStorage {
    /// Optional override for `.mediapm/` runtime root.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub mediapm_dir: Option<String>,
    /// Optional override for materialized hierarchy root directory.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub hierarchy_root_dir: Option<String>,
    /// Optional ordered policy for hierarchy file materialization.
    ///
    /// When omitted, runtime defaults to:
    /// `hardlink -> symlink -> reflink -> copy`.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub materialization_preference_order: Option<Vec<MaterializationMethod>>,
    /// Optional override for `mediapm`-managed conductor user config path.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub conductor_config: Option<String>,
    /// Optional override for `mediapm`-managed conductor machine config path.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub conductor_machine_config: Option<String>,
    /// Optional override for `mediapm`-managed conductor runtime state path.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub conductor_state_config: Option<String>,
    /// Optional override for conductor schema export directory.
    ///
    /// Defaults to `<runtime.mediapm_dir>/config/conductor`.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub conductor_schema_dir: Option<String>,
    /// Optional additional inherited host environment-variable names for
    /// conductor executable process environments, keyed by platform.
    ///
    /// Runtime always keeps the host-default baseline and merges only the
    /// active host platform entry (`windows`, `linux`, `macos`, etc.) on top
    /// with case-insensitive de-duplication.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub inherited_env_vars: Option<PlatformInheritedEnvVars>,
    /// Optional override for machine-managed `mediapm` state path.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub media_state_config: Option<String>,
    /// Optional override for runtime dotenv file used for credential loading.
    ///
    /// When omitted, the effective default path is `<runtime.mediapm_dir>/.env`.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub env_file: Option<String>,
    /// Optional override for the machine-generated runtime dotenv file.
    ///
    /// This file is written by tooling (not by users) with computed runtime
    /// variables such as internal `ffmpeg` binary paths. When omitted, the
    /// effective default path is `<runtime.mediapm_dir>/.env.generated`.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub env_generated_file: Option<String>,
    /// Optional schema export directory policy for embedded `mediapm.ncl`
    /// Nickel contracts.
    ///
    /// Tri-state semantics:
    /// - omitted (`None`): export schemas to default `<runtime.mediapm_dir>/config/mediapm`,
    /// - explicit `null` (`Some(None)`): disable schema export,
    /// - explicit string (`Some(Some(path))`): export to that path.
    #[serde(default, skip_serializing_if = "runtime_mediapm_schema_export_is_omitted")]
    pub mediapm_schema_dir: Option<Option<String>>,
    /// Optional custom reserved-character replacement mapping used by
    /// `hierarchy[*].sanitize_names` when set to `true` or a custom mapping.
    ///
    /// When omitted, each reserved char defaults to `_`.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub path_sanitization: Option<BTreeMap<String, String>>,
    /// Optional toggle for conductor workflow profiling during managed runs.
    ///
    /// When `Some(true)`, conductor writes a per-step timing profile to
    /// `<mediapm_dir>/profile.json` after each successful workflow execution.
    /// When `Some(false)` or `None`, profiling is disabled.
    ///
    /// Default: `None` (disabled).
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub profiler_enabled: Option<bool>,
}

/// Returns whether runtime schema-export policy was omitted from config.
#[expect(
    clippy::option_option,
    reason = "tri-state schema export policy intentionally distinguishes omitted/null/path"
)]
#[expect(
    clippy::ref_option,
    reason = "serde skip_serializing_if requires borrowing the full field type"
)]
fn runtime_mediapm_schema_export_is_omitted(value: &Option<Option<String>>) -> bool {
    value.is_none()
}

fn default_path_sanitization_mapping() -> BTreeMap<char, char> {
    BTreeMap::from([
        ('<', '_'),
        ('>', '_'),
        (':', '_'),
        ('"', '_'),
        ('|', '_'),
        ('?', '_'),
        ('*', '_'),
    ])
}

fn append_unique_env_var_names(target: &mut Vec<String>, source: &[String]) {
    for raw_name in source {
        let trimmed = raw_name.trim();
        if trimmed.is_empty() {
            continue;
        }

        if target.iter().any(|existing| existing.eq_ignore_ascii_case(trimmed)) {
            continue;
        }

        target.push(trimmed.to_string());
    }
}

/// Normalizes one platform key used by `runtime.inherited_env_vars`.
#[must_use]
fn normalize_runtime_platform_key(raw_platform: &str) -> Option<String> {
    let trimmed = raw_platform.trim();
    if trimmed.is_empty() { None } else { Some(trimmed.to_ascii_lowercase()) }
}

/// Appends one platform-scoped inherited env-var list for the active host.
fn append_platform_inherited_env_var_names_for_host(
    target: &mut Vec<String>,
    source: &PlatformInheritedEnvVars,
    host_platform: &str,
) {
    for (platform_key, names) in source {
        if normalize_runtime_platform_key(platform_key).as_deref() == Some(host_platform) {
            append_unique_env_var_names(target, names);
        }
    }
}

impl MediaRuntimeStorage {
    /// Returns ordered materialization policy with runtime defaults applied.
    #[must_use]
    pub fn materialization_preference_order_with_defaults(&self) -> Vec<MaterializationMethod> {
        self.materialization_preference_order
            .clone()
            .unwrap_or_else(|| DEFAULT_MATERIALIZATION_PREFERENCE_ORDER.to_vec())
    }

    /// Returns inherited env-var names merged with host defaults.
    ///
    /// This reads only the active host-platform entry from
    /// `runtime.inherited_env_vars` and ignores lists for other platforms.
    #[must_use]
    pub fn inherited_env_vars_with_defaults(&self) -> Vec<String> {
        let host_platform = std::env::consts::OS.to_ascii_lowercase();
        let mut merged = Vec::new();

        append_platform_inherited_env_var_names_for_host(
            &mut merged,
            &default_runtime_inherited_env_vars_for_host(),
            &host_platform,
        );

        if let Some(configured) = &self.inherited_env_vars {
            append_platform_inherited_env_var_names_for_host(
                &mut merged,
                configured,
                &host_platform,
            );
        }
        merged
    }

    /// Returns one effective reserved-character replacement mapping.
    ///
    /// If the config is omitted, this uses the runtime defaults for all
    /// rejected reserved filename characters.
    #[must_use]
    pub fn path_sanitization_mapping_with_defaults(
        &self,
    ) -> Result<BTreeMap<char, char>, MediaPmError> {
        let mut replacements = default_path_sanitization_mapping();

        if let Some(custom) = &self.path_sanitization {
            for (key, value) in custom {
                let key_char = key.chars().next().ok_or_else(|| {
                    MediaPmError::Workflow(
                        "runtime.path_sanitization keys must be single characters".to_string(),
                    )
                })?;
                if key.chars().count() != 1 {
                    return Err(MediaPmError::Workflow(
                        "runtime.path_sanitization keys must be single characters".to_string(),
                    ));
                }

                let replacement_char = value.chars().next().ok_or_else(|| {
                    MediaPmError::Workflow(
                        "runtime.path_sanitization values must be single-character strings"
                            .to_string(),
                    )
                })?;
                if value.chars().count() != 1 {
                    return Err(MediaPmError::Workflow(
                        "runtime.path_sanitization values must be single-character strings"
                            .to_string(),
                    ));
                }

                replacements.insert(key_char, replacement_char);
            }
        }

        Ok(replacements)
    }
}

/// Declarative tool requirement for one logical media tool.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct ToolRequirement {
    /// Optional version selector for this logical tool.
    ///
    /// At least one of `version` or `tag` must be provided by
    /// `validate_tool_requirements` during document load.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub version: Option<String>,
    /// Optional tag selector for this logical tool.
    ///
    /// When both `version` and `tag` are provided, they must refer to the
    /// same normalized release selector.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub tag: Option<String>,
    /// Optional grouped dependency selector overrides for this logical tool.
    ///
    /// These selectors pin companion logical tools for workflows that depend
    /// on additional executables (for example `ffmpeg` and `sd`).
    #[serde(default, skip_serializing_if = "tool_requirement_dependencies_is_empty")]
    pub dependencies: ToolRequirementDependencies,
    /// Optional release-metadata recheck interval in seconds.
    ///
    /// When present, `mediapm` reuses cached release metadata until the
    /// interval elapses, then refreshes from upstream release APIs.
    /// When omitted, `mediapm` reuses cached release metadata for one day
    /// before refreshing from upstream release APIs (while still allowing
    /// cache fallback on refresh errors).
    #[serde(
        default,
        skip_serializing_if = "Option::is_none",
        deserialize_with = "deserialize_optional_u64_from_number"
    )]
    pub recheck_seconds: Option<u64>,
    /// Optional max number of indexed ffmpeg input slots exposed by generated
    /// managed tool contracts and workflow synthesis.
    ///
    /// This setting is valid only on logical tool `ffmpeg`.
    #[serde(
        default,
        skip_serializing_if = "Option::is_none",
        deserialize_with = "deserialize_optional_runtime_slot_count"
    )]
    pub max_input_slots: Option<u32>,
    /// Optional max number of indexed ffmpeg output slots exposed by generated
    /// managed tool contracts and workflow synthesis.
    ///
    /// This setting is valid only on logical tool `ffmpeg`.
    #[serde(
        default,
        skip_serializing_if = "Option::is_none",
        deserialize_with = "deserialize_optional_runtime_slot_count"
    )]
    pub max_output_slots: Option<u32>,
}

/// Grouped dependency selector overrides for one logical tool requirement.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize, Default)]
#[serde(deny_unknown_fields)]
pub struct ToolRequirementDependencies {
    /// Optional ffmpeg selector used by tools with explicit ffmpeg companion
    /// selection support.
    ///
    /// Selection semantics:
    /// - omitted / `global` / `inherit`: use active logical `ffmpeg` tool,
    /// - explicit selector text: match immutable ffmpeg identity by
    ///   hash/version/tag (normalized compare).
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub ffmpeg_version: Option<String>,
    /// Optional `deno` selector used by tools that require a JavaScript
    /// runtime companion (for example `yt-dlp`).
    ///
    /// Selection semantics mirror `ffmpeg_version`.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub deno_version: Option<String>,
    /// Optional `sd` selector used by tools that require `sd` companion
    /// transforms (for example `ReplayGain` metadata rewrites).
    ///
    /// Selection semantics mirror `ffmpeg_version`.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub sd_version: Option<String>,
}

impl ToolRequirementDependencies {
    /// Returns true when no dependency selector overrides are configured.
    #[must_use]
    pub const fn is_empty(&self) -> bool {
        self.ffmpeg_version.is_none() && self.deno_version.is_none() && self.sd_version.is_none()
    }
}

/// Returns whether grouped dependency selector config can be omitted from
/// serialized `mediapm.ncl` output.
fn tool_requirement_dependencies_is_empty(value: &ToolRequirementDependencies) -> bool {
    value.is_empty()
}

impl ToolRequirement {
    /// Returns normalized non-empty version selector text.
    #[must_use]
    pub fn normalized_version(&self) -> Option<String> {
        normalize_selector_value(self.version.as_deref())
    }

    /// Returns normalized non-empty tag selector text.
    #[must_use]
    pub fn normalized_tag(&self) -> Option<String> {
        normalize_selector_value(self.tag.as_deref())
    }

    /// Returns normalized ffmpeg selector text.
    #[must_use]
    pub fn normalized_ffmpeg_selector(&self) -> Option<String> {
        normalize_selector_value(self.dependencies.ffmpeg_version.as_deref())
    }

    /// Returns normalized `deno` selector text.
    #[must_use]
    pub fn normalized_deno_selector(&self) -> Option<String> {
        normalize_selector_value(self.dependencies.deno_version.as_deref())
    }

    /// Returns normalized `sd` selector text.
    #[must_use]
    pub fn normalized_sd_selector(&self) -> Option<String> {
        normalize_selector_value(self.dependencies.sd_version.as_deref())
    }

    /// Returns optional release-metadata recheck interval in seconds.
    #[must_use]
    pub const fn metadata_recheck_seconds(&self) -> Option<u64> {
        self.recheck_seconds
    }

    /// Returns effective max input slot count for this tool row.
    #[must_use]
    pub const fn max_input_slots_or_default(&self) -> u32 {
        match self.max_input_slots {
            Some(value) => value,
            None => DEFAULT_FFMPEG_MAX_INPUT_SLOTS,
        }
    }

    /// Returns effective max output slot count for this tool row.
    #[must_use]
    pub const fn max_output_slots_or_default(&self) -> u32 {
        match self.max_output_slots {
            Some(value) => value,
            None => DEFAULT_FFMPEG_MAX_OUTPUT_SLOTS,
        }
    }
}

/// Normalizes optional selector text by trimming whitespace and leading `@`.
#[must_use]
pub(crate) fn normalize_selector_value(raw: Option<&str>) -> Option<String> {
    raw.map(str::trim)
        .map(|value| value.trim_start_matches('@'))
        .filter(|value| !value.is_empty())
        .map(ToString::to_string)
}

/// Normalizes version/tag text for equality comparison.
#[must_use]
pub(crate) fn normalize_selector_compare_value(value: &str) -> String {
    value.trim().trim_start_matches('@').trim_start_matches(['v', 'V']).to_string()
}

/// Deserializes optional `u64` values while accepting integral floating-point
/// numbers exported by Nickel (for example `3600.0`).
fn deserialize_optional_u64_from_number<'de, D>(deserializer: D) -> Result<Option<u64>, D::Error>
where
    D: Deserializer<'de>,
{
    let raw = Option::<Value>::deserialize(deserializer)?;
    let Some(raw) = raw else {
        return Ok(None);
    };

    if let Some(value) = raw.as_u64() {
        return Ok(Some(value));
    }

    if let Some(value) = raw.as_f64()
        && let Some(normalized) = parse_non_negative_integral_u64(value)
    {
        return Ok(Some(normalized));
    }

    Err(serde::de::Error::custom("recheck_seconds must be a non-negative integer"))
}

/// Deserializes optional runtime slot-count `u32` values while accepting
/// integral floating-point numbers exported by Nickel (for example `96.0`).
fn deserialize_optional_runtime_slot_count<'de, D>(deserializer: D) -> Result<Option<u32>, D::Error>
where
    D: Deserializer<'de>,
{
    let raw = Option::<Value>::deserialize(deserializer)?;
    let Some(raw) = raw else {
        return Ok(None);
    };

    if let Some(value) = raw.as_u64() {
        return u32::try_from(value)
            .map(Some)
            .map_err(|_| serde::de::Error::custom("ffmpeg slot limit must be within u32 range"));
    }

    if let Some(value) = raw.as_f64()
        && let Some(normalized) = parse_non_negative_integral_u32(value)
    {
        return Ok(Some(normalized));
    }

    Err(serde::de::Error::custom("ffmpeg slot limit must be a non-negative integer"))
}

/// Loads `mediapm.ncl` from disk or returns defaults when the file is absent.
///
/// # Errors
///
/// Returns [`MediaPmError`] when file I/O, Nickel evaluation, schema decoding,
/// or cross-field validation fails.
pub fn load_mediapm_document(path: &Path) -> Result<MediaPmDocument, MediaPmError> {
    load_mediapm_document_inner(path, true)
}

/// Loads `mediapm.ncl` from disk without cross-field validation.
///
/// This loader is intended for edit/bootstrap flows that need to read and
/// mutate an existing document before the full dependency graph is complete.
///
/// # Errors
///
/// Returns [`MediaPmError`] when file I/O, Nickel evaluation, or schema
/// decoding fails.
pub(crate) fn load_mediapm_document_without_validation(
    path: &Path,
) -> Result<MediaPmDocument, MediaPmError> {
    load_mediapm_document_inner(path, false)
}

fn load_mediapm_document_inner(
    path: &Path,
    validate: bool,
) -> Result<MediaPmDocument, MediaPmError> {
    if !path.exists() {
        return Ok(MediaPmDocument::default());
    }

    let bytes = fs::read(path).map_err(|source| MediaPmError::Io {
        operation: "reading mediapm.ncl".to_string(),
        path: path.to_path_buf(),
        source,
    })?;

    if bytes.iter().all(u8::is_ascii_whitespace) {
        return Ok(MediaPmDocument::default());
    }

    let source = std::str::from_utf8(&bytes).map_err(|err| {
        MediaPmError::Serialization(format!("mediapm.ncl is not valid UTF-8: {err}"))
    })?;

    let mut value = evaluate_nickel_source_to_json(path, source)?;
    normalize_version_field_to_u64(&mut value, "mediapm.ncl")?;

    let document = versions::decode_mediapm_document_value(value)?;

    if validate {
        validate_media_document(&document)?;
    }

    Ok(document)
}

/// Loads machine-managed `state.ncl` from disk using shared `mediapm` schema.
///
/// # Errors
///
/// Returns [`MediaPmError`] when file I/O, Nickel evaluation, schema decoding,
/// or state-only shape validation fails.
pub fn load_mediapm_state_document(path: &Path) -> Result<MediaPmState, MediaPmError> {
    if !path.exists() {
        return Ok(MediaPmState::default());
    }

    let bytes = fs::read(path).map_err(|source| MediaPmError::Io {
        operation: "reading state.ncl".to_string(),
        path: path.to_path_buf(),
        source,
    })?;

    if bytes.iter().all(u8::is_ascii_whitespace) {
        return Ok(MediaPmState::default());
    }

    let source = std::str::from_utf8(&bytes).map_err(|err| {
        MediaPmError::Serialization(format!("state.ncl is not valid UTF-8: {err}"))
    })?;

    let mut value = evaluate_nickel_source_to_json(path, source)?;
    normalize_version_field_to_u64(&mut value, "state.ncl")?;

    let document = versions::decode_mediapm_document_value(value)?;
    validate_mediapm_state_document_shape(path, &document)?;

    Ok(document.state)
}

/// Merges user config document and machine-managed state into one runtime view.
#[must_use]
pub fn merge_mediapm_document_with_state(
    mut config_document: MediaPmDocument,
    state: MediaPmState,
) -> MediaPmDocument {
    config_document.state = state;
    config_document
}

mod nickel_io;

use self::nickel_io::{
    evaluate_nickel_source_to_json, normalize_version_field_to_u64,
    parse_non_negative_integral_u32, parse_non_negative_integral_u64, render_nickel_value,
};

mod validation;

pub(crate) use self::validation::{hierarchy_metadata_placeholder_keys, media_source_uri};
use self::validation::{validate_media_document, validate_mediapm_state_document_shape};

/// Saves `mediapm.ncl` to disk using deterministic Nickel rendering.
///
/// # Errors
///
/// Returns [`MediaPmError`] when parent directories cannot be created,
/// schema encoding fails, or the rendered Nickel payload cannot be written.
pub fn save_mediapm_document(path: &Path, document: &MediaPmDocument) -> Result<(), MediaPmError> {
    if let Some(parent) = path.parent() {
        fs::create_dir_all(parent).map_err(|source| MediaPmError::Io {
            operation: "creating mediapm.ncl parent directory".to_string(),
            path: parent.to_path_buf(),
            source,
        })?;
    }

    let json = versions::encode_mediapm_document_value(document.clone())?;
    let rendered = format!("{}\n", render_nickel_value(&json, 0));

    fs::write(path, rendered.as_bytes()).map_err(|source| MediaPmError::Io {
        operation: "writing mediapm.ncl".to_string(),
        path: path.to_path_buf(),
        source,
    })
}

/// Saves machine-managed `state.ncl` using shared `mediapm` schema rendering.
///
/// Persisted output includes only top-level `version` and `state` fields.
///
/// # Errors
///
/// Returns [`MediaPmError`] when parent directories cannot be created,
/// schema encoding fails, or output bytes cannot be written.
pub fn save_mediapm_state_document(path: &Path, state: &MediaPmState) -> Result<(), MediaPmError> {
    if let Some(parent) = path.parent() {
        fs::create_dir_all(parent).map_err(|source| MediaPmError::Io {
            operation: "creating state.ncl parent directory".to_string(),
            path: parent.to_path_buf(),
            source,
        })?;
    }

    let document = MediaPmDocument { state: state.clone(), ..MediaPmDocument::default() };

    let json = versions::encode_mediapm_document_value(document)?;
    let object = json.as_object().cloned().ok_or_else(|| {
        MediaPmError::Serialization(
            "encoding state.ncl value: top-level record required".to_string(),
        )
    })?;

    let mut state_only = serde_json::Map::new();
    if let Some(version) = object.get("version") {
        state_only.insert("version".to_string(), version.clone());
    }
    let state_value = serde_json::to_value(state)
        .map_err(|err| MediaPmError::Serialization(format!("encoding state payload: {err}")))?;
    state_only.insert("state".to_string(), state_value);

    let rendered = format!("{}\n", render_nickel_value(&Value::Object(state_only), 0));

    fs::write(path, rendered.as_bytes()).map_err(|source| MediaPmError::Io {
        operation: "writing state.ncl".to_string(),
        path: path.to_path_buf(),
        source,
    })
}

/// Returns true when `mediapm.ncl` is present and non-empty.
pub fn mediapm_document_exists(path: &Path) -> bool {
    path.exists() && fs::metadata(path).is_ok_and(|meta| meta.len() > 0)
}

#[cfg(test)]
mod tests;
