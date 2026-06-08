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
use std::time::Duration;

use mediapm_cas::{CasIntegrityConfig, Hash, VerifyTriggerStrategy};
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
    HierarchyNodeKind, HierarchyPath, PlaylistEntryPathMode, PlaylistFormat, PlaylistItemRef,
    SanitizeNamesConfig, flatten_hierarchy_value, nest_hierarchy_value, regex_variant_selector,
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
    /// Materialization-skip gate hash: when the latest orchestration state
    /// hash equals this value, materialization is a no-op.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub last_materialized_state_hash: Option<Hash>,
}

/// Timezone-independent mediapm "step was generated" marker set at synthesis
/// time, distinct from conductor instance-key timestamps.
///
/// This value is set unconditionally during workflow synthesis (step
/// generation, not execution) and persisted in `lock.workflow_states` to track
/// which steps have been produced by a synthesis pass. A missing timestamp
/// triggers re-synthesis on the next reconciliation.
///
/// In contrast, [`mediapm_conductor::ImpureTimestamp`] is a conductor-level
/// timestamp used for instance-key derivation during execution planning — it
/// lives in the conductor state document and drives cache-hit/miss decisions
/// independently of this mediapm-local synthesis marker.
///
/// This wire shape is mediapm-local state and accepts integral float values
/// exported by Nickel during decode.
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
        && value.last_materialized_state_hash.is_none()
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

    /// When true, verify materialized output against CAS hash after write.
    ///
    /// Defaults to `false` (trust CAS by default). When enabled, the
    /// materializer recomputes the BLAKE3 hash of each written output file
    /// and panics/errors on mismatch.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub verify_materialization: Option<bool>,

    /// Optional instance GC time-to-live in seconds.
    ///
    /// When set, stale orchestration instances are pruned after this many
    /// seconds of inactivity. Passed through to conductor runtime config.
    /// When `None`, GC is left to conductor defaults (usually disabled).
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub instance_ttl_seconds: Option<u64>,

    /// Ordered list of strategies that trigger CAS integrity re-verification on read.
    /// Accepted values: "always", "modified", "sample", "stale".
    /// Default: `["modified", "sample"]`.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub verify_on_read: Option<Vec<String>>,

    /// Sampling denominator for the "sample" verify-on-read strategy.
    /// Controls approximate verification frequency: 1 out of N reads triggers
    /// a full BLAKE3 re-verification.
    /// Default: 100.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub verify_on_read_sample_denominator: Option<u64>,

    /// Timeout in seconds after which a "stale" verify-on-read strategy
    /// triggers re-verification.
    /// Default: 604800 (7 days).
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub verify_on_read_stale_timeout_secs: Option<u64>,

    /// TTL in seconds for the CAS reconstructed bytes cache.
    /// After this duration, cached object bytes are re-fetched and
    /// re-verified from storage.
    /// Default: 3600 (1 hour).
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub reconstructed_bytes_cache_ttl_secs: Option<u64>,
}

impl MediaRuntimeStorage {
    /// Returns whether materialization verification is enabled.
    #[must_use]
    pub fn verify_materialization(&self) -> bool {
        self.verify_materialization.unwrap_or(false)
    }

    /// Returns the `verify_on_read` strategy list or the default.
    #[must_use]
    pub fn verify_on_read(&self) -> Vec<String> {
        self.verify_on_read.clone().unwrap_or_else(|| vec!["modified".into(), "sample".into()])
    }

    /// Returns the verify-on-read sampling denominator.
    /// Default: 100.
    #[must_use]
    pub fn verify_on_read_sample_denominator(&self) -> u64 {
        self.verify_on_read_sample_denominator.unwrap_or(100)
    }

    /// Returns the stale timeout in seconds for verify-on-read.
    /// Default: `604_800` (7 days).
    #[must_use]
    pub fn verify_on_read_stale_timeout_secs(&self) -> u64 {
        self.verify_on_read_stale_timeout_secs.unwrap_or(604_800)
    }

    /// Returns the reconstructed bytes cache TTL in seconds.
    /// Default: 3600 (1 hour).
    #[must_use]
    pub fn reconstructed_bytes_cache_ttl_secs(&self) -> u64 {
        self.reconstructed_bytes_cache_ttl_secs.unwrap_or(3600)
    }

    /// Converts this runtime storage config into a [`CasIntegrityConfig`]
    /// for CAS integrity verification settings.
    ///
    /// Maps `verify_on_read` strategy name strings to
    /// [`VerifyTriggerStrategy`] variants:
    /// - `"always"` → [`VerifyTriggerStrategy::Always`]
    /// - `"modified"` → [`VerifyTriggerStrategy::Modified`]
    /// - `"sample"` → [`VerifyTriggerStrategy::Sample`]
    /// - `"stale"` → [`VerifyTriggerStrategy::Stale`]
    ///
    /// Unknown strings are silently skipped. If the resulting list is empty,
    /// returns [`CasIntegrityConfig::default()`].
    #[must_use]
    pub fn to_cas_integrity_config(&self) -> CasIntegrityConfig {
        let verify_on_read: Vec<VerifyTriggerStrategy> = self
            .verify_on_read()
            .into_iter()
            .filter_map(|strategy| match strategy.as_str() {
                "always" => Some(VerifyTriggerStrategy::Always),
                "modified" => Some(VerifyTriggerStrategy::Modified),
                "sample" => Some(VerifyTriggerStrategy::Sample {
                    denominator: self.verify_on_read_sample_denominator(),
                }),
                "stale" => Some(VerifyTriggerStrategy::Stale {
                    timeout: Duration::from_secs(self.verify_on_read_stale_timeout_secs()),
                }),
                _ => None,
            })
            .collect();

        if verify_on_read.is_empty() {
            return CasIntegrityConfig::default();
        }

        CasIntegrityConfig {
            verify_on_read,
            reconstructed_bytes_cache_ttl: Duration::from_secs(
                self.reconstructed_bytes_cache_ttl_secs(),
            ),
        }
    }
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
        ('/', '_'),
        ('\\', '_'),
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
    ///
    /// # Errors
    ///
    /// Returns [`MediaPmError::Workflow`] if a custom mapping key or value is
    /// not a single character.
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
mod tests {
    use std::collections::BTreeMap;

    use super::{
        HierarchyEntry, HierarchyEntryKind, MEDIAPM_DOCUMENT_VERSION, MaterializationMethod,
        MediaMetadataValue, MediaMetadataValueCandidate, MediaPmDocument, MediaPmImpureTimestamp,
        MediaPmState, MediaRuntimeStorage, MediaSourceSpec, MediaStep, MediaStepTool,
        OutputSaveConfig, PlaylistEntryPathMode, PlaylistFormat, SanitizeNamesConfig,
        ToolRequirement, TransformInputValue, Value, flatten_hierarchy_nodes_for_runtime,
        load_mediapm_document, load_mediapm_state_document, media_source_uri,
        resolve_step_variant_flow, save_mediapm_document, save_mediapm_state_document,
    };

    fn hierarchy_flat_map(document: &MediaPmDocument) -> BTreeMap<String, HierarchyEntry> {
        flatten_hierarchy_nodes_for_runtime(&document.hierarchy)
            .expect("flatten hierarchy")
            .into_iter()
            .map(|flattened| (flattened.path_str(), flattened.entry))
            .collect()
    }

    fn hierarchy_nodes(entries: BTreeMap<String, HierarchyEntry>) -> Vec<super::HierarchyNode> {
        entries
            .into_iter()
            .map(|(path, entry)| match entry.kind {
                HierarchyEntryKind::Media if path.ends_with('/') || path.ends_with('\\') => {
                    super::HierarchyNode {
                        path: super::HierarchyPath::from(path.trim_end_matches(['/', '\\'])),
                        kind: super::HierarchyNodeKind::MediaFolder,
                        id: Some(entry.media_id.clone()),
                        media_id: Some(entry.media_id),
                        variant: None,
                        variants: entry.variants,
                        rename_files: entry.rename_files,
                        format: PlaylistFormat::M3u8,
                        ids: Vec::new(),
                        sanitize_names: SanitizeNamesConfig::Inherit,
                        children: Vec::new(),
                    }
                }
                HierarchyEntryKind::Media => super::HierarchyNode {
                    path: super::HierarchyPath::from(path.as_str()),
                    kind: super::HierarchyNodeKind::Media,
                    id: Some(entry.media_id.clone()),
                    media_id: Some(entry.media_id),
                    variant: entry.variants.first().cloned(),
                    variants: Vec::new(),
                    rename_files: Vec::new(),
                    sanitize_names: SanitizeNamesConfig::Inherit,
                    format: PlaylistFormat::M3u8,
                    ids: Vec::new(),
                    children: Vec::new(),
                },
                HierarchyEntryKind::MediaFolder => super::HierarchyNode {
                    path: super::HierarchyPath::from(path.as_str()),
                    kind: super::HierarchyNodeKind::MediaFolder,
                    id: Some(entry.media_id.clone()),
                    media_id: Some(entry.media_id),
                    variant: None,
                    variants: entry.variants,
                    rename_files: entry.rename_files,
                    format: PlaylistFormat::M3u8,
                    ids: Vec::new(),
                    sanitize_names: SanitizeNamesConfig::Inherit,
                    children: Vec::new(),
                },
                HierarchyEntryKind::Playlist => super::HierarchyNode {
                    path: super::HierarchyPath::from(path.as_str()),
                    kind: super::HierarchyNodeKind::Playlist,
                    id: None,
                    media_id: None,
                    variant: None,
                    variants: Vec::new(),
                    rename_files: Vec::new(),
                    sanitize_names: SanitizeNamesConfig::Inherit,
                    format: entry.format,
                    ids: entry.ids,
                    children: Vec::new(),
                },
            })
            .collect()
    }

    /// Protects flat-to-node conversion helper semantics used by migration-
    /// period Rust callsites by covering `media`, `media_folder`, and
    /// `playlist`
    /// entry mapping behavior.
    #[test]
    fn hierarchy_nodes_from_flat_entries_converts_all_supported_kinds() {
        let entries = BTreeMap::from([
            (
                "library/video.mkv".to_string(),
                HierarchyEntry {
                    kind: HierarchyEntryKind::Media,
                    media_id: "demo".to_string(),
                    variants: vec!["video".to_string()],
                    rename_files: Vec::new(),
                    sanitize_names: SanitizeNamesConfig::Inherit,
                    format: PlaylistFormat::M3u8,
                    ids: Vec::new(),
                },
            ),
            (
                "library/subtitles/".to_string(),
                HierarchyEntry {
                    kind: HierarchyEntryKind::MediaFolder,
                    media_id: "demo".to_string(),
                    variants: vec!["subtitles".to_string()],
                    rename_files: Vec::new(),
                    sanitize_names: SanitizeNamesConfig::Inherit,
                    format: PlaylistFormat::M3u8,
                    ids: Vec::new(),
                },
            ),
            (
                "library/mixed.m3u8".to_string(),
                HierarchyEntry {
                    kind: HierarchyEntryKind::Playlist,
                    media_id: String::new(),
                    variants: Vec::new(),
                    rename_files: Vec::new(),
                    sanitize_names: SanitizeNamesConfig::Inherit,
                    format: PlaylistFormat::M3u8,
                    ids: vec![super::PlaylistItemRef {
                        id: "demo".to_string(),
                        path: PlaylistEntryPathMode::Relative,
                    }],
                },
            ),
        ]);

        let nodes = super::hierarchy_nodes_from_flat_entries(&entries)
            .expect("flat hierarchy entries should convert to node-list form");

        assert_eq!(nodes.len(), 3);

        let media = nodes
            .iter()
            .find(|node| node.path == "library/video.mkv".into())
            .expect("media node should exist");
        assert!(matches!(media.kind, super::HierarchyNodeKind::Media));
        assert_eq!(media.media_id.as_deref(), Some("demo"));
        assert_eq!(media.variant.as_deref(), Some("video"));

        let media_folder = nodes
            .iter()
            .find(|node| node.path == "library/subtitles".into())
            .expect("media_folder node should exist");
        assert!(matches!(media_folder.kind, super::HierarchyNodeKind::MediaFolder));
        assert_eq!(media_folder.media_id.as_deref(), Some("demo"));
        assert_eq!(media_folder.variants, vec!["subtitles".to_string()]);

        let playlist = nodes
            .iter()
            .find(|node| node.path == "library/mixed.m3u8".into())
            .expect("playlist node should exist");
        assert!(matches!(playlist.kind, super::HierarchyNodeKind::Playlist));
        assert!(playlist.media_id.is_none());
        assert_eq!(playlist.ids.len(), 1);
        assert_eq!(playlist.ids[0].id(), "demo");
    }

    /// Protects round-trip persistence semantics for `mediapm.ncl` defaults.
    #[test]
    fn mediapm_document_round_trip_preserves_schema_version() {
        let root = tempfile::tempdir().expect("tempdir");
        let path = root.path().join("mediapm.ncl");
        let document = MediaPmDocument::default();

        save_mediapm_document(&path, &document).expect("save mediapm.ncl");
        let decoded = load_mediapm_document(&path).expect("load mediapm.ncl");

        assert_eq!(decoded.version, MEDIAPM_DOCUMENT_VERSION);
    }

    /// Protects Nickel rendering by quoting reserved field names such as
    /// `import` so saved documents round-trip through Nickel evaluation.
    #[test]
    fn save_document_quotes_nickel_reserved_tool_key_import() {
        let root = tempfile::tempdir().expect("tempdir");
        let path = root.path().join("mediapm.ncl");
        let mut document = MediaPmDocument::default();
        document.tools.insert(
            "import".to_string(),
            ToolRequirement {
                version: None,
                tag: None,
                dependencies: super::ToolRequirementDependencies::default(),
                recheck_seconds: None,
                max_input_slots: None,
                max_output_slots: None,
            },
        );

        save_mediapm_document(&path, &document).expect("save mediapm.ncl");
        let rendered = std::fs::read_to_string(&path).expect("read rendered mediapm.ncl");
        assert!(
            rendered.contains("\"import\" = {") || rendered.contains("'import' = {"),
            "reserved key must be quoted in rendered Nickel"
        );

        let decoded = load_mediapm_document(&path).expect("load mediapm.ncl");
        assert!(decoded.tools.contains_key("import"));
    }

    /// Protects Nickel numeric rendering by emitting integral ffmpeg `idx`
    /// values without trailing decimal notation.
    #[test]
    fn save_document_renders_integral_output_variant_idx_without_decimal() {
        let root = tempfile::tempdir().expect("tempdir");
        let path = root.path().join("mediapm.ncl");
        let mut document = MediaPmDocument::default();

        document.media.insert(
            "demo".to_string(),
            MediaSourceSpec {
                id: None,
                description: None,
                title: None,
                artist: None,
                workflow_id: None,
                metadata: None,
                variant_hashes: BTreeMap::new(),
                steps: vec![MediaStep {
                    tool: MediaStepTool::Ffmpeg,
                    input_variants: vec!["video".to_string()],
                    output_variants: BTreeMap::from([(
                        "video".to_string(),
                        serde_json::json!({
                            "kind": "primary",
                            "idx": 0,
                            "extension": "mkv",
                        }),
                    )]),
                    options: BTreeMap::new(),
                }],
            },
        );

        save_mediapm_document(&path, &document).expect("save mediapm.ncl");
        let rendered = std::fs::read_to_string(&path).expect("read rendered mediapm.ncl");

        assert!(rendered.contains("idx = 0,"));
        assert!(!rendered.contains("idx = 0.0"));
    }

    /// Protects machine-managed state persistence shape and round-trip decode.
    #[test]
    fn mediapm_state_document_round_trip_is_state_only() {
        let root = tempfile::tempdir().expect("tempdir");
        let path = root.path().join("state.ncl");
        let mut state = MediaPmState::default();
        state.active_tools.insert("ffmpeg".to_string(), "tool-id".to_string());

        save_mediapm_state_document(&path, &state).expect("save state.ncl");
        let decoded = load_mediapm_state_document(&path).expect("load state.ncl");
        let rendered = std::fs::read_to_string(&path).expect("read state.ncl");

        assert_eq!(decoded, state);
        assert!(rendered.contains("version = 1"));
        assert!(rendered.contains("state = {"));
        assert!(!rendered.lines().any(|line| line.trim_start().starts_with("runtime =")));
        assert!(!rendered.lines().any(|line| line.trim_start().starts_with("tools =")));
        assert!(!rendered.lines().any(|line| line.trim_start().starts_with("media =")));
        assert!(!rendered.lines().any(|line| line.trim_start().starts_with("hierarchy =")));
    }

    /// Protects workflow-step refresh state persistence by round-tripping
    /// explicit step config snapshots and mediapm-managed impure timestamps.
    #[test]
    fn mediapm_state_round_trip_preserves_workflow_states() {
        let root = tempfile::tempdir().expect("tempdir");
        let path = root.path().join("state.ncl");
        let mut state = MediaPmState::default();
        state.workflow_states.insert(
            "demo-media".to_string(),
            vec![
                super::ManagedWorkflowStepState {
                    explicit_config: serde_json::json!({
                        "tool": "yt-dlp",
                        "output_variants": {
                            "default": { "kind": "primary", "save": "full" }
                        },
                        "options": { "uri": "https://example.com/video" }
                    }),
                    impure_timestamp: Some(MediaPmImpureTimestamp {
                        epoch_seconds: 123,
                        subsec_nanos: 456,
                    }),
                },
                super::ManagedWorkflowStepState {
                    explicit_config: serde_json::json!({
                        "tool": "rsgain",
                        "input_variants": ["default"],
                        "output_variants": {
                            "default": { "kind": "primary", "save": "full" }
                        },
                        "options": {}
                    }),
                    impure_timestamp: None,
                },
            ],
        );

        save_mediapm_state_document(&path, &state).expect("save state.ncl");
        let decoded = load_mediapm_state_document(&path).expect("load state.ncl");
        let rendered = std::fs::read_to_string(&path).expect("read state.ncl");

        assert_eq!(decoded, state);
        assert!(rendered.contains("workflow_states = {"));
    }

    /// Protects strict state-file shape by rejecting non-state top-level keys.
    #[test]
    fn mediapm_state_document_rejects_non_state_top_level_fields() {
        let root = tempfile::tempdir().expect("tempdir");
        let path = root.path().join("state.ncl");
        let source = r#"
{
version = 1,
runtime = {
    mediapm_dir = ".mediapm-custom",
},
state = {
    active_tools = {
        ffmpeg = "tool-id",
    },
},
}
"#;

        std::fs::write(&path, source).expect("write state.ncl");
        let err = load_mediapm_state_document(&path)
            .expect_err("state.ncl with runtime section must fail shape validation");

        assert!(
            err.to_string()
                .contains("must contain only top-level 'version' and 'state' properties")
        );
    }

    /// Protects node-list hierarchy decode by flattening recursive folder nodes
    /// into runtime flat-path entries while preserving directory/file targets.
    #[test]
    fn hierarchy_nested_nodes_flatten_into_runtime_paths() {
        let root = tempfile::tempdir().expect("tempdir");
        let path = root.path().join("mediapm.ncl");
        let source = r#"
{
version = 1,
media = {
    demo = {
        steps = [
            {
                tool = "yt-dlp",
                output_variants = {
                    video = { kind = "primary", save = "full" },
                    subtitles = { kind = "subtitles", save = "full" },
                },
                options = {
                    uri = "https://example.com/video",
                },
            },
        ],
    },
},
hierarchy = [
    {
        path = "library",
        children = [
            {
                path = "artist",
                children = [
                    {
                        path = "video.mkv",
                        kind = "media",
                        id = "demo-video",
                        media_id = "demo",
                        variant = "video",
                    },
                    {
                        path = "subtitles",
                        kind = "media_folder",
                        id = "demo-subtitles",
                        media_id = "demo",
                        variants = ["subtitles"],
                    },
                ],
            },
        ],
    },
],
}
"#;

        std::fs::write(&path, source).expect("write source");
        let document = load_mediapm_document(&path).expect("decode nested hierarchy document");

        let hierarchy = hierarchy_flat_map(&document);
        assert!(hierarchy.contains_key("library/artist/video.mkv"));
        assert!(hierarchy.contains_key("library/artist/subtitles"));
    }

    /// Same template path with different `media_ids` is allowed — `${media.id}`
    /// placeholders resolve to different paths during materialization.
    #[test]
    fn hierarchy_same_path_different_media_id_allowed() {
        let root = tempfile::tempdir().expect("tempdir");
        let path = root.path().join("mediapm.ncl");
        let source = r#"
{
version = 1,
media = {
    song_a = {
        steps = [
            {
                tool = "yt-dlp",
                output_variants = {
                    audio = { kind = "primary", save = "full" },
                },
                options = {
                    uri = "https://example.com/song_a",
                },
            },
        ],
    },
    song_b = {
        steps = [
            {
                tool = "yt-dlp",
                output_variants = {
                    audio = { kind = "primary", save = "full" },
                },
                options = {
                    uri = "https://example.com/song_b",
                },
            },
        ],
    },
},
hierarchy = [
    {
        path = "music/${media.id}.mkv",
        kind = "media",
        id = "entry-a",
        media_id = "song_a",
        variant = "audio",
    },
    {
        path = "music/${media.id}.mkv",
        kind = "media",
        id = "entry-b",
        media_id = "song_b",
        variant = "audio",
    },
],
}
"#;

        std::fs::write(&path, source).expect("write source");
        let document = load_mediapm_document(&path).expect("decode hierarchy document");

        // Different media_ids with same template path must not produce duplicate errors.
        let hierarchy = hierarchy_flat_map(&document);
        assert!(hierarchy.contains_key("music/${media.id}.mkv"));
    }

    /// Same template path and same `media_id` must still be rejected as duplicate.
    #[test]
    fn hierarchy_same_path_same_media_id_rejected() {
        let root = tempfile::tempdir().expect("tempdir");
        let path = root.path().join("mediapm.ncl");
        let source = r#"
{
version = 1,
media = {
    song = {
        steps = [
            {
                tool = "yt-dlp",
                output_variants = {
                    audio = { kind = "primary", save = "full" },
                },
                options = {
                    uri = "https://example.com/song",
                },
            },
        ],
    },
},
hierarchy = [
    {
        path = "music/${media.id}.mkv",
        kind = "media",
        id = "entry-1",
        media_id = "song",
        variant = "audio",
    },
    {
        path = "music/${media.id}.mkv",
        kind = "media",
        id = "entry-2",
        media_id = "song",
        variant = "audio",
    },
],
}
"#;

        std::fs::write(&path, source).expect("write source");
        let result = load_mediapm_document(&path);

        assert!(result.is_err(), "same path + same media_id should be rejected at load");
        let err = result.unwrap_err();
        let err_str = err.to_string();
        assert!(
            err_str.contains("duplicate") || err_str.contains("conflicting"),
            "error should mention duplicate or conflicting, got: {err_str}"
        );
    }

    /// Protects hierarchy defaults by treating omitted `kind` as structural
    /// folder nodes.
    #[test]
    fn hierarchy_nested_nodes_default_to_folder_kind_when_kind_is_omitted() {
        let root = tempfile::tempdir().expect("tempdir");
        let path = root.path().join("mediapm.ncl");
        let source = r#"
{
version = 1,
media = {
    demo = {
        steps = [
            {
                tool = "yt-dlp",
                output_variants = {
                    video = { kind = "primary", save = "full" },
                },
                options = {
                    uri = "https://example.com/video",
                },
            },
        ],
    },
},
hierarchy = [
    {
        path = "top",
        children = [
            {
                path = "middle",
                children = [
                    {
                        path = "final.mkv",
                        kind = "media",
                        id = "demo-final",
                        media_id = "demo",
                        variant = "video",
                    },
                ],
            },
        ],
    },
],
}
"#;

        std::fs::write(&path, source).expect("write source");
        let document = load_mediapm_document(&path).expect("decode nested hierarchy document");

        assert!(hierarchy_flat_map(&document).contains_key("top/middle/final.mkv"));
    }

    /// Protects node-kind typing by requiring media leaf declarations to set
    /// `kind = "media"`.
    #[test]
    fn hierarchy_nested_leaf_requires_kind_marker_for_media_or_playlist() {
        let root = tempfile::tempdir().expect("tempdir");
        let path = root.path().join("mediapm.ncl");
        let source = r#"
{
version = 1,
media = {
    demo = {
        steps = [
            {
                tool = "yt-dlp",
                output_variants = {
                    video = { kind = "primary", save = "full" },
                },
                options = {
                    uri = "https://example.com/video",
                },
            },
        ],
    },
},
hierarchy = [
    {
        path = "library/video.mkv",
        media_id = "demo",
        variant = "video",
    },
],
}
"#;

        std::fs::write(&path, source).expect("write source");
        let err =
            load_mediapm_document(&path).expect_err("media leaf without explicit kind should fail");

        assert!(err.to_string().contains("kind 'folder' must not define 'variant'"));
    }

    /// Ensures configured hierarchy path literals are rejected when segments
    /// are not Unicode NFD normalized.
    #[test]
    fn hierarchy_path_rejects_non_nfd_literal_segments() {
        let root = tempfile::tempdir().expect("tempdir");
        let path = root.path().join("mediapm.ncl");
        let source = r#"
{
version = 1,
media = {
    demo = {
        steps = [
            {
                tool = "yt-dlp",
                output_variants = {
                    video = { kind = "primary", save = "full" },
                },
                options = {
                    uri = "https://example.com/video",
                },
            },
        ],
    },
},
hierarchy = [
    {
        path = "library/épisode.mkv",
        kind = "media",
        id = "demo-video",
        media_id = "demo",
        variant = "video",
    },
],
}
"#;

        std::fs::write(&path, source).expect("write source");
        let err =
            load_mediapm_document(&path).expect_err("non-NFD hierarchy path should fail decode");
        assert!(err.to_string().contains("must be Unicode NFD normalized"));
    }

    /// Ensures configured hierarchy path templates still enforce NFD on the
    /// literal path text around placeholders.
    #[test]
    fn hierarchy_path_template_rejects_non_nfd_literal_segments() {
        let root = tempfile::tempdir().expect("tempdir");
        let path = root.path().join("mediapm.ncl");
        let source = r#"
{
version = 1,
media = {
    demo = {
        metadata = {
            artist = "demo",
        },
        steps = [
            {
                tool = "yt-dlp",
                output_variants = {
                    video = { kind = "primary", save = "full" },
                },
                options = {
                    uri = "https://example.com/video",
                },
            },
        ],
    },
},
hierarchy = [
    {
        path = "library/${media.metadata.artist}/épisode.mkv",
        kind = "media",
        id = "demo-video",
        media_id = "demo",
        variant = "video",
    },
],
}
"#;

        std::fs::write(&path, source).expect("write source");
        let err = load_mediapm_document(&path)
            .expect_err("non-NFD template literal path segment should fail decode");
        assert!(err.to_string().contains("must be Unicode NFD normalized"));
    }

    /// Protects persistence rendering by serializing hierarchy as ordered node
    /// arrays with explicit `kind`/`path` fields.
    #[test]
    fn save_mediapm_document_emits_nested_hierarchy_kind_field() {
        let root = tempfile::tempdir().expect("tempdir");
        let path = root.path().join("mediapm.ncl");
        let mut document = MediaPmDocument::default();

        document.media.insert(
            "demo".to_string(),
            MediaSourceSpec {
                id: None,
                description: None,
                title: None,
                artist: None,
                workflow_id: None,
                metadata: None,
                variant_hashes: BTreeMap::from([(
                    "video".to_string(),
                    "blake3:aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa"
                        .to_string(),
                )]),
                steps: Vec::new(),
            },
        );

        document.hierarchy = hierarchy_nodes(BTreeMap::from([(
            "library/demo.mkv".to_string(),
            HierarchyEntry {
                kind: HierarchyEntryKind::Media,
                media_id: "demo".to_string(),
                variants: vec!["video".to_string()],
                rename_files: Vec::new(),
                sanitize_names: SanitizeNamesConfig::Inherit,
                format: PlaylistFormat::M3u8,
                ids: Vec::new(),
            },
        )]));

        save_mediapm_document(&path, &document).expect("save hierarchy node-list document");
        let rendered = std::fs::read_to_string(&path).expect("read rendered mediapm.ncl");

        assert!(rendered.contains("kind = \"media\""));
        assert!(
            rendered.contains("path = [\n        \"library\",\n        \"demo.mkv\",\n      ]")
        );

        let decoded = load_mediapm_document(&path).expect("decode rendered hierarchy node-list");
        assert!(hierarchy_flat_map(&decoded).contains_key("library/demo.mkv"));
    }

    /// Protects tool requirement decoding for explicit version/tag selectors.
    #[test]
    fn tool_requirements_decode_with_version_or_tag_selectors() {
        let root = tempfile::tempdir().expect("tempdir");
        let path = root.path().join("mediapm.ncl");
        let source = r#"
{
  version = 1,
  tools = {
        ffmpeg = { version = "8.2" },
        deno = { version = "1.0" },
        sd = { version = "1.0" },
        rsgain = { version = "3.7.0", tag = "v3.7.0", dependencies = { ffmpeg_version = "inherit", sd_version = "inherit" } },
        "media-tagger" = { tag = "latest", dependencies = { ffmpeg_version = "inherit" } },
        "yt-dlp" = { tag = "v2026.04.01", dependencies = { ffmpeg_version = "inherit" }, recheck_seconds = 3600 },
  },
}
"#;

        std::fs::write(&path, source).expect("write source");
        let document = load_mediapm_document(&path).expect("decode document");

        assert_eq!(document.tools["ffmpeg"].version.as_deref(), Some("8.2"));
        assert!(document.tools["ffmpeg"].tag.is_none());
        assert!(document.tools["yt-dlp"].version.is_none());
        assert_eq!(document.tools["yt-dlp"].tag.as_deref(), Some("v2026.04.01"));
        assert_eq!(document.tools["yt-dlp"].recheck_seconds, Some(3600));
        assert_eq!(
            document.tools["yt-dlp"].dependencies.ffmpeg_version.as_deref(),
            Some("inherit")
        );
        assert_eq!(document.tools["rsgain"].version.as_deref(), Some("3.7.0"));
        assert_eq!(document.tools["rsgain"].tag.as_deref(), Some("v3.7.0"));
        assert_eq!(
            document.tools["rsgain"].dependencies.ffmpeg_version.as_deref(),
            Some("inherit")
        );
        assert_eq!(document.tools["rsgain"].dependencies.sd_version.as_deref(), Some("inherit"));
        assert_eq!(
            document.tools["media-tagger"].dependencies.ffmpeg_version.as_deref(),
            Some("inherit")
        );
    }

    /// Protects tool-requirement schema by rejecting ffmpeg selector overrides
    /// on unsupported logical tools.
    #[test]
    fn unsupported_tool_rejects_ffmpeg_version_selector() {
        let root = tempfile::tempdir().expect("tempdir");
        let path = root.path().join("mediapm.ncl");
        let source = r#"
{
  version = 1,
  tools = {
archive = { tag = "latest", dependencies = { ffmpeg_version = "inherit" } },
  },
}
"#;

        std::fs::write(&path, source).expect("write source");
        let err = load_mediapm_document(&path)
            .expect_err("ffmpeg_version on unsupported tool should fail");
        assert!(
            err.to_string().contains("must not define dependency selector overrides"),
            "unexpected error: {err}"
        );
    }

    /// Protects grouped dependency selector support for rsgain workflows.
    #[test]
    fn rsgain_accepts_grouped_dependency_selectors() {
        let root = tempfile::tempdir().expect("tempdir");
        let path = root.path().join("mediapm.ncl");
        let source = r#"
{
  version = 1,
  tools = {
        ffmpeg = { tag = "latest" },
        sd = { version = "1.0" },
rsgain = { tag = "latest", dependencies = { ffmpeg_version = "inherit", sd_version = "inherit" } },
  },
}
"#;

        std::fs::write(&path, source).expect("write source");
        let document =
            load_mediapm_document(&path).expect("rsgain dependencies should pass validation");
        assert_eq!(
            document.tools["rsgain"].dependencies.ffmpeg_version.as_deref(),
            Some("inherit")
        );
        assert_eq!(document.tools["rsgain"].dependencies.sd_version.as_deref(), Some("inherit"));
    }

    /// Protects dependency-inherit semantics by requiring configured dependency
    /// tool rows when yt-dlp asks to inherit ffmpeg/deno selectors.
    #[test]
    fn yt_dlp_inherit_dependencies_require_configured_tools() {
        let root = tempfile::tempdir().expect("tempdir");
        let path = root.path().join("mediapm.ncl");
        let source = r#"
{
version = 1,
tools = {
    "yt-dlp" = {
        tag = "latest",
        dependencies = {
            ffmpeg_version = "inherit",
            deno_version = "inherit",
        },
    },
},
}
"#;

        std::fs::write(&path, source).expect("write source");
        let err = load_mediapm_document(&path)
            .expect_err("yt-dlp inherit dependencies should require tools.ffmpeg/tools.deno");

        assert!(err.to_string().contains("requires tools.ffmpeg"), "unexpected error: {err}");
    }

    /// Protects dependency-inherit semantics by treating omitted dependency
    /// selectors as the implicit inherit default.
    #[test]
    fn yt_dlp_missing_dependency_selectors_require_configured_tools() {
        let root = tempfile::tempdir().expect("tempdir");
        let path = root.path().join("mediapm.ncl");
        let source = r#"
{
version = 1,
tools = {
    "yt-dlp" = {
        tag = "latest",
    },
},
}
"#;

        std::fs::write(&path, source).expect("write source");
        let err = load_mediapm_document(&path)
            .expect_err("yt-dlp omitted dependencies should still require tools.ffmpeg");

        assert!(err.to_string().contains("requires tools.ffmpeg"), "unexpected error: {err}");
    }

    /// Protects dependency-inherit semantics by requiring configured `sd` rows
    /// when rsgain inherits `sd_version`.
    #[test]
    fn rsgain_inherit_sd_dependency_requires_configured_sd_tool() {
        let root = tempfile::tempdir().expect("tempdir");
        let path = root.path().join("mediapm.ncl");
        let source = r#"
{
version = 1,
tools = {
    ffmpeg = { tag = "latest" },
    rsgain = {
        tag = "latest",
        dependencies = {
            ffmpeg_version = "inherit",
            sd_version = "inherit",
        },
    },
},
}
"#;

        std::fs::write(&path, source).expect("write source");
        let err = load_mediapm_document(&path)
            .expect_err("rsgain inherit sd dependency should require tools.sd");

        assert!(err.to_string().contains("requires tools.sd"), "unexpected error: {err}");
    }

    /// Protects yt-dlp output-variant schema by requiring `format` to be set
    /// in step `options`, not inside output-variant config objects.
    #[test]
    fn yt_dlp_output_variant_rejects_format_field() {
        let root = tempfile::tempdir().expect("tempdir");
        let path = root.path().join("mediapm.ncl");
        let source = r#"
{
version = 1,
media = {
    demo = {
        steps = [
            {
                tool = "yt-dlp",
                output_variants = {
                    video = {
                        kind = "primary",
                        save = "full",
                        format = "bestvideo*+bestaudio/best",
                    },
                },
                options = { uri = "https://example.com/video" },
            },
        ],
    },
},
}
"#;

        std::fs::write(&path, source).expect("write source");
        let err = load_mediapm_document(&path).expect_err("format field must be rejected");
        assert!(err.to_string().contains("unknown field `format`"));
    }

    /// Protects no-backward-compat policy by rejecting removed runtime
    /// `use_user_tool_cache` key.
    #[test]
    fn runtime_storage_rejects_removed_use_user_tool_cache_key() {
        let root = tempfile::tempdir().expect("tempdir");
        let path = root.path().join("mediapm.ncl");
        let source = r"
{
version = 1,
runtime = {
    use_user_tool_cache = false,
},
}
";
        std::fs::write(&path, source).expect("write source");
        let err = load_mediapm_document(&path)
            .expect_err("removed runtime key must fail decode strictly");
        assert!(err.to_string().contains("use_user_tool_cache"));
    }

    /// Protects runtime-storage decode for explicit dotenv file overrides.
    #[test]
    fn runtime_storage_decodes_env_file_override() {
        let root = tempfile::tempdir().expect("tempdir");
        let path = root.path().join("mediapm.ncl");
        let source = r#"
{
version = 1,
runtime = {
    env_file = ".mediapm/.env.custom",
},
}
"#;

        std::fs::write(&path, source).expect("write source");
        let document = load_mediapm_document(&path).expect("decode document");

        assert_eq!(document.runtime.env_file.as_deref(), Some(".mediapm/.env.custom"));
    }

    /// Protects tool-requirement decode for ffmpeg slot-limit overrides.
    #[test]
    fn tool_requirements_decode_ffmpeg_slot_limits_on_ffmpeg_tool() {
        let root = tempfile::tempdir().expect("tempdir");
        let path = root.path().join("mediapm.ncl");
        let source = r#"
{
version = 1,
tools = {
    ffmpeg = {
        version = "latest",
        max_input_slots = 96,
        max_output_slots = 80,
    },
},
}
"#;

        std::fs::write(&path, source).expect("write source");
        let document = load_mediapm_document(&path).expect("decode document");

        assert_eq!(document.tools["ffmpeg"].max_input_slots, Some(96));
        assert_eq!(document.tools["ffmpeg"].max_output_slots, Some(80));
        assert_eq!(document.tools["ffmpeg"].max_input_slots_or_default(), 96);
        assert_eq!(document.tools["ffmpeg"].max_output_slots_or_default(), 80);
    }

    /// Protects runtime-storage decode for platform-keyed inherited env vars.
    #[test]
    fn runtime_storage_decodes_platform_inherited_env_vars() {
        let root = tempfile::tempdir().expect("tempdir");
        let path = root.path().join("mediapm.ncl");
        let source = r#"
{
version = 1,
runtime = {
    inherited_env_vars = {
        windows = ["ComSpec", "Path"],
        linux = ["PATH"],
    },
},
}
"#;

        std::fs::write(&path, source).expect("write source");
        let document = load_mediapm_document(&path).expect("decode document");

        let inherited =
            document.runtime.inherited_env_vars.as_ref().expect("inherited env map should decode");
        assert_eq!(
            inherited.get("windows"),
            Some(&vec!["ComSpec".to_string(), "Path".to_string()])
        );
        assert_eq!(inherited.get("linux"), Some(&vec!["PATH".to_string()]));
    }

    /// Protects runtime materialization policy decoding for ordered methods.
    #[test]
    fn runtime_storage_decodes_materialization_preference_order() {
        let root = tempfile::tempdir().expect("tempdir");
        let path = root.path().join("mediapm.ncl");
        let source = r#"
{
version = 1,
runtime = {
    materialization_preference_order = ["copy", "hardlink"],
},
}
"#;

        std::fs::write(&path, source).expect("write source");
        let document = load_mediapm_document(&path).expect("decode document");

        assert_eq!(
            document.runtime.materialization_preference_order,
            Some(vec![MaterializationMethod::Copy, MaterializationMethod::Hardlink])
        );
    }

    /// Protects runtime materialization policy by rejecting duplicate methods.
    #[test]
    fn runtime_storage_rejects_duplicate_materialization_preference_order() {
        let root = tempfile::tempdir().expect("tempdir");
        let path = root.path().join("mediapm.ncl");
        let source = r#"
{
version = 1,
runtime = {
    materialization_preference_order = ["hardlink", "copy", "hardlink"],
},
}
"#;

        std::fs::write(&path, source).expect("write source");
        let error =
            load_mediapm_document(&path).expect_err("duplicate materialization methods must fail");
        assert!(
            error
                .to_string()
                .contains("runtime.materialization_preference_order contains duplicate method")
        );
    }

    /// Protects runtime materialization policy by rejecting empty method lists.
    #[test]
    fn runtime_storage_rejects_empty_materialization_preference_order() {
        let root = tempfile::tempdir().expect("tempdir");
        let path = root.path().join("mediapm.ncl");
        let source = r"
{
version = 1,
runtime = {
    materialization_preference_order = [],
},
}
";

        std::fs::write(&path, source).expect("write source");
        let error =
            load_mediapm_document(&path).expect_err("empty materialization methods must fail");
        assert!(
            error.to_string().contains(
                "runtime.materialization_preference_order must contain at least one method"
            )
        );
    }

    /// Protects host-platform filtering when resolving inherited env names.
    #[test]
    fn inherited_env_vars_with_defaults_reads_only_host_platform() {
        let runtime = MediaRuntimeStorage {
            inherited_env_vars: Some(BTreeMap::from([
                ("windows".to_string(), vec!["SYSTEMROOT".to_string(), "ComSpec".to_string()]),
                ("linux".to_string(), vec!["LD_LIBRARY_PATH".to_string()]),
                ("macos".to_string(), vec!["DYLD_LIBRARY_PATH".to_string()]),
            ])),
            ..MediaRuntimeStorage::default()
        };

        let resolved = runtime.inherited_env_vars_with_defaults();

        if cfg!(windows) {
            assert!(resolved.iter().any(|value| value.eq_ignore_ascii_case("ComSpec")));
            assert!(!resolved.iter().any(|value| value.eq_ignore_ascii_case("LD_LIBRARY_PATH")));
            assert!(!resolved.iter().any(|value| value.eq_ignore_ascii_case("DYLD_LIBRARY_PATH")));
        } else if cfg!(target_os = "linux") {
            assert!(resolved.iter().any(|value| value.eq_ignore_ascii_case("LD_LIBRARY_PATH")));
            assert!(!resolved.iter().any(|value| value.eq_ignore_ascii_case("ComSpec")));
            assert!(!resolved.iter().any(|value| value.eq_ignore_ascii_case("DYLD_LIBRARY_PATH")));
        } else if cfg!(target_os = "macos") {
            assert!(resolved.iter().any(|value| value.eq_ignore_ascii_case("DYLD_LIBRARY_PATH")));
            assert!(!resolved.iter().any(|value| value.eq_ignore_ascii_case("ComSpec")));
            assert!(!resolved.iter().any(|value| value.eq_ignore_ascii_case("LD_LIBRARY_PATH")));
        }
    }

    /// Protects runtime materialization policy defaults when runtime value is omitted.
    #[test]
    fn runtime_storage_defaults_materialization_preference_order() {
        let runtime_storage = MediaRuntimeStorage::default();
        assert_eq!(
            runtime_storage.materialization_preference_order_with_defaults(),
            vec![
                MaterializationMethod::Hardlink,
                MaterializationMethod::Symlink,
                MaterializationMethod::Reflink,
                MaterializationMethod::Copy,
            ]
        );
    }

    /// Protects runtime path sanitization defaults for reserved characters.
    #[test]
    fn runtime_storage_path_sanitization_defaults_reserved_char_replacements() {
        let mapping = MediaRuntimeStorage::default()
            .path_sanitization_mapping_with_defaults()
            .expect("default runtime path sanitization map");

        assert_eq!(mapping.get(&'<'), Some(&'_'));
        assert_eq!(mapping.get(&'>'), Some(&'_'));
        assert_eq!(mapping.get(&':'), Some(&'_'));
        assert_eq!(mapping.get(&'"'), Some(&'_'));
        assert_eq!(mapping.get(&'|'), Some(&'_'));
        assert_eq!(mapping.get(&'?'), Some(&'_'));
        assert_eq!(mapping.get(&'*'), Some(&'_'));
        assert_eq!(mapping.get(&'/'), Some(&'_'));
        assert_eq!(mapping.get(&'\\'), Some(&'_'));
    }

    /// Protects runtime path sanitization custom mapping merge semantics.
    #[test]
    fn runtime_storage_path_sanitization_merges_custom_mapping() {
        let runtime = MediaRuntimeStorage {
            path_sanitization: Some(BTreeMap::from([
                ("<".to_string(), "x".to_string()),
                ("*".to_string(), "+".to_string()),
            ])),
            ..MediaRuntimeStorage::default()
        };

        let mapping = runtime
            .path_sanitization_mapping_with_defaults()
            .expect("custom runtime path sanitization map");

        assert_eq!(mapping.get(&'<'), Some(&'x'));
        assert_eq!(mapping.get(&'*'), Some(&'+'));
        assert_eq!(mapping.get(&'>'), Some(&'_'));
    }

    /// Protects hierarchy node `sanitize_names` inheritance from parent nodes.
    #[test]
    fn hierarchy_nodes_inherit_sanitize_names_from_parent() {
        let nodes = vec![super::HierarchyNode {
            path: super::HierarchyPath::from("root"),
            kind: super::HierarchyNodeKind::Folder,
            id: None,
            media_id: None,
            variant: None,
            variants: Vec::new(),
            rename_files: Vec::new(),
            format: PlaylistFormat::M3u8,
            ids: Vec::new(),
            sanitize_names: SanitizeNamesConfig::Enabled,
            children: vec![super::HierarchyNode {
                path: super::HierarchyPath::from("child.mp4"),
                kind: super::HierarchyNodeKind::Media,
                id: None,
                media_id: Some("demo".to_string()),
                variant: Some("video".to_string()),
                variants: Vec::new(),
                rename_files: Vec::new(),
                format: PlaylistFormat::M3u8,
                ids: Vec::new(),
                sanitize_names: SanitizeNamesConfig::Inherit,
                children: Vec::new(),
            }],
        }];

        let flattened = flatten_hierarchy_nodes_for_runtime(&nodes).expect("flatten hierarchy");
        assert_eq!(flattened.len(), 1);
        assert_eq!(flattened[0].entry.sanitize_names, SanitizeNamesConfig::Enabled);
    }

    /// Protects ffmpeg slot-limit validation by rejecting zero input slots.
    #[test]
    fn tool_requirements_reject_zero_ffmpeg_input_slots() {
        let root = tempfile::tempdir().expect("tempdir");
        let path = root.path().join("mediapm.ncl");
        let source = r#"
{
version = 1,
tools = {
    ffmpeg = {
        version = "latest",
        max_input_slots = 0,
    },
},
}
"#;

        std::fs::write(&path, source).expect("write source");
        let err = load_mediapm_document(&path).expect_err("zero input slots must fail");
        assert!(err.to_string().contains("tools.ffmpeg.max_input_slots"));
    }

    /// Protects ffmpeg slot-limit validation by rejecting zero output slots.
    #[test]
    fn tool_requirements_reject_zero_ffmpeg_output_slots() {
        let root = tempfile::tempdir().expect("tempdir");
        let path = root.path().join("mediapm.ncl");
        let source = r#"
{
version = 1,
tools = {
    ffmpeg = {
        version = "latest",
        max_output_slots = 0,
    },
},
}
"#;

        std::fs::write(&path, source).expect("write source");
        let err = load_mediapm_document(&path).expect_err("zero output slots must fail");
        assert!(err.to_string().contains("tools.ffmpeg.max_output_slots"));
    }

    /// Protects tool-requirement validation by rejecting ffmpeg slot settings
    /// on non-ffmpeg logical tools.
    #[test]
    fn non_ffmpeg_tools_reject_ffmpeg_slot_settings() {
        let root = tempfile::tempdir().expect("tempdir");
        let path = root.path().join("mediapm.ncl");
        let source = r#"
{
version = 1,
tools = {
    ffmpeg = { version = "latest" },
    deno = { version = "latest" },
    "yt-dlp" = {
        version = "latest",
        max_input_slots = 72,
    },
},
}
"#;

        std::fs::write(&path, source).expect("write source");
        let err =
            load_mediapm_document(&path).expect_err("non-ffmpeg slot settings must be rejected");
        assert!(err.to_string().contains("must not define ffmpeg slot settings"));
    }

    /// Protects no-backward-compat policy by rejecting legacy ffmpeg slot key
    /// names under `tools.ffmpeg`.
    #[test]
    fn tools_ffmpeg_rejects_legacy_ffmpeg_slot_key_names() {
        let root = tempfile::tempdir().expect("tempdir");
        let path = root.path().join("mediapm.ncl");
        let source = r#"
{
version = 1,
tools = {
    ffmpeg = {
        version = "latest",
        ffmpeg_max_input_slots = 96,
    },
},
}
"#;

        std::fs::write(&path, source).expect("write source");
        let err = load_mediapm_document(&path)
            .expect_err("legacy tools.ffmpeg slot key should be rejected");
        assert!(err.to_string().contains("ffmpeg_max_input_slots"));
    }

    /// Protects no-backward-compat migration policy by rejecting legacy
    /// ffmpeg slot settings under `runtime` via strict unknown-field decoding.
    #[test]
    fn runtime_rejects_legacy_ffmpeg_slot_keys() {
        let root = tempfile::tempdir().expect("tempdir");
        let path = root.path().join("mediapm.ncl");
        let source = r#"
{
version = 1,
runtime = {
    ffmpeg_max_input_slots = 96,
},
tools = {
    ffmpeg = { version = "latest" },
},
}
"#;

        std::fs::write(&path, source).expect("write source");
        let err =
            load_mediapm_document(&path).expect_err("legacy runtime ffmpeg key must be rejected");
        assert!(err.to_string().contains("ffmpeg_max_input_slots"));
    }

    /// Protects renamed runtime key policy by rejecting legacy key spelling
    /// through strict top-level unknown-field decoding.
    #[test]
    fn runtime_storage_key_is_rejected_after_runtime_rename() {
        let root = tempfile::tempdir().expect("tempdir");
        let path = root.path().join("mediapm.ncl");
        let source = r#"
{
version = 1,
runtime_storage = {
    mediapm_dir = ".mediapm",
},
}
"#;

        std::fs::write(&path, source).expect("write source");
        let err = load_mediapm_document(&path).expect_err("legacy runtime_storage key must fail");
        assert!(err.to_string().contains("runtime_storage"));
    }

    /// Protects no-backward-compat policy by rejecting removed
    /// yt-dlp output-variant `filename_template` fields.
    #[test]
    fn yt_dlp_output_variant_rejects_filename_template_field() {
        let root = tempfile::tempdir().expect("tempdir");
        let path = root.path().join("mediapm.ncl");
        let source = r#"
{
version = 1,
media = {
    demo = {
        steps = [
            {
                tool = "yt-dlp",
                output_variants = {
                    "subtitles/" = {
                        kind = "subtitles",
                        save = "full",
                        filename_template = "%(title)s [%(id)s].%(ext)s",
                    },
                },
                options = {
                    uri = "https://example.com/video",
                },
            },
        ],
    },
},
}
"#;

        std::fs::write(&path, source).expect("write source");
        let err = load_mediapm_document(&path).expect_err("filename_template must be rejected");

        assert!(err.to_string().contains("unknown field `filename_template`"));
    }

    /// Protects selector validation by requiring at least one version/tag entry.
    #[test]
    fn tool_requirements_reject_missing_version_and_tag() {
        let root = tempfile::tempdir().expect("tempdir");
        let path = root.path().join("mediapm.ncl");
        let source = r"
{
  version = 1,
  tools = {
  ffmpeg = {},
  },
}
";

        std::fs::write(&path, source).expect("write source");
        let err = load_mediapm_document(&path).expect_err("document should fail validation");
        assert!(err.to_string().contains("must define at least one selector"));
    }

    /// Protects selector validation by rejecting mismatched version/tag pairs.
    #[test]
    fn tool_requirements_reject_mismatched_version_and_tag() {
        let root = tempfile::tempdir().expect("tempdir");
        let path = root.path().join("mediapm.ncl");
        let source = r#"
{
  version = 1,
  tools = {
  ffmpeg = { version = "8.2", tag = "v8.1" },
  },
}
"#;

        std::fs::write(&path, source).expect("write source");
        let err = load_mediapm_document(&path).expect_err("document should fail validation");
        assert!(err.to_string().contains("mismatched version"));
    }

    /// Protects online-step schema by requiring explicit `options.uri`.
    #[test]
    fn online_step_requires_options_uri() {
        let root = tempfile::tempdir().expect("tempdir");
        let path = root.path().join("mediapm.ncl");
        let source = r#"
{
version = 1,
media = {
    demo = {
        steps = [
            {
                tool = "yt-dlp",
                output_variants = { default = { kind = "primary", save = "full" } },
            },
        ],
    },
},
}
"#;

        std::fs::write(&path, source).expect("write source");
        let err = load_mediapm_document(&path).expect_err("document should fail validation");
        assert!(err.to_string().contains("must define options.uri"));
    }

    /// Protects simplified boolean-option semantics by accepting non-`true`
    /// values and deferring enablement checks to runtime command templates.
    #[test]
    fn online_step_write_description_accepts_non_true_values() {
        let root = tempfile::tempdir().expect("tempdir");
        let path = root.path().join("mediapm.ncl");
        let source = r#"
{
version = 1,
media = {
    demo = {
        steps = [
            {
                tool = "yt-dlp",
                output_variants = { default = { kind = "primary", save = "full" } },
                options = {
                    uri = "https://example.com/video",
                    write_description = "false",
                },
            },
        ],
    },
},
}
"#;

        std::fs::write(&path, source).expect("write source");
        let document = load_mediapm_document(&path).expect("document should decode");

        assert_eq!(
            document.media["demo"].steps[0].options.get("write_description").map(
                |value| match value {
                    TransformInputValue::String(value) => value.as_str(),
                }
            ),
            Some("false"),
        );
    }

    /// Protects step option validation by rejecting undeclared keys.
    #[test]
    fn step_options_reject_unknown_keys() {
        let root = tempfile::tempdir().expect("tempdir");
        let path = root.path().join("mediapm.ncl");
        let source = r#"
{
version = 1,
media = {
    local_demo = {
        variant_hashes = { default = "blake3:abc" },
        steps = [
            {
                tool = "ffmpeg",
                input_variants = ["default"],
                output_variants = { default = { kind = "primary", save = "full", idx = 0 } },
                options = { unsupported = "yes" },
            },
        ],
    },
},
}
"#;

        std::fs::write(&path, source).expect("write source");
        let err = load_mediapm_document(&path).expect_err("document should fail validation");
        assert!(err.to_string().contains("unsupported option 'unsupported'"));
    }

    /// Protects split subtitle option semantics by accepting explicit
    /// `write_auto_subs` step options.
    #[test]
    fn step_options_accept_write_auto_subs_key() {
        let root = tempfile::tempdir().expect("tempdir");
        let path = root.path().join("mediapm.ncl");
        let source = r#"
{
version = 1,
media = {
    remote_demo = {
        steps = [
            {
                tool = "yt-dlp",
                output_variants = { downloaded = { kind = "primary", save = "full" } },
                options = {
                    uri = "https://example.com/video",
                    write_auto_subs = "true",
                },
            },
        ],
    },
},
}
"#;

        std::fs::write(&path, source).expect("write source");
        let document = load_mediapm_document(&path).expect("write_auto_subs option should decode");
        assert_eq!(document.media["remote_demo"].steps.len(), 1);
    }

    /// Protects expanded step-option allowlists so audited CLI keys are
    /// accepted for all managed media tools.
    #[test]
    fn step_options_accept_expanded_tool_keys() {
        let root = tempfile::tempdir().expect("tempdir");
        let path = root.path().join("mediapm.ncl");
        let source = r#"
{
version = 1,
media = {
    remote_demo = {
        steps = [
            {
                tool = "yt-dlp",
                output_variants = { downloaded = { kind = "primary", save = "full" } },
                options = {
                    uri = "https://example.com/video",
                    merge_output_format = "mkv",
                    format_sort = "res,codec",
                    cache_dir = "./cache/yt-dlp",
                    playlist_items = "1:3",
                    sleep_subtitles = "60",
                    skip_download = "true",
                },
            },
            {
                tool = "ffmpeg",
                input_variants = ["downloaded"],
                output_variants = { normalized = { kind = "primary", save = "full", idx = 0 } },
                options = {
                    audio_quality = "2",
                    map = "0:a:0",
                    map_channel = "0.0.0",
                    id3v2_version = "3",
                },
            },
            {
                tool = "rsgain",
                input_variants = ["normalized"],
                output_variants = { gained = { kind = "primary", save = "full" } },
                options = {
                    tagmode = "i",
                    clip_mode = "p",
                    true_peak = "true",
                    preserve_mtimes = "true",
                },
            },
            {
                tool = "media-tagger",
                input_variants = ["gained"],
                output_variants = { tagged = { kind = "primary", save = "full" } },
                options = {
                    strict_identification = "false",
                    cache_dir = "./cache",
                    cache_expiry_seconds = "86400",
                    musicbrainz_endpoint = "https://musicbrainz.org/ws/2",
                    output_container = "mp4",
                },
            },
        ],
    },
},
}
"#;

        std::fs::write(&path, source).expect("write source");
        let document = load_mediapm_document(&path).expect("document should decode");

        assert_eq!(document.media["remote_demo"].steps.len(), 4);
    }

    /// Protects scalar-first option typing by rejecting list values for
    /// non-list option keys.
    #[test]
    fn step_options_reject_list_value_for_scalar_option_key() {
        let root = tempfile::tempdir().expect("tempdir");
        let path = root.path().join("mediapm.ncl");
        let source = r#"
{
version = 1,
media = {
    local_demo = {
        variant_hashes = { source = "blake3:abc" },
        steps = [
            {
                tool = "ffmpeg",
                input_variants = ["source"],
                output_variants = { normalized = { kind = "primary", save = "full", idx = 0 } },
                options = {
                    audio_quality = ["2"],
                },
            },
        ],
    },
},
}
"#;

        std::fs::write(&path, source).expect("write source");
        let err = load_mediapm_document(&path).expect_err("document should fail validation");
        assert!(!err.to_string().trim().is_empty());
    }

    /// Protects strict output-variant schema by rejecting non-object values
    /// for non-yt-dlp tools.
    #[test]
    fn non_yt_dlp_output_variant_rejects_string_shorthand() {
        let root = tempfile::tempdir().expect("tempdir");
        let path = root.path().join("mediapm.ncl");
        let source = r#"
{
version = 1,
media = {
    local_demo = {
        variant_hashes = { source = "blake3:abc" },
        steps = [
            {
                tool = "ffmpeg",
                input_variants = ["source"],
                output_variants = { normalized = "primary" },
            },
        ],
    },
},
}
"#;

        std::fs::write(&path, source).expect("write source");
        let err = load_mediapm_document(&path).expect_err("document should fail validation");
        assert!(err.to_string().contains("must be an object with at least field 'kind'"));
    }

    /// Protects value-explicit output semantics by rejecting empty-object
    /// output-variant values for single-output simple tools.
    #[test]
    fn single_output_simple_tool_rejects_empty_object_output_variant() {
        let root = tempfile::tempdir().expect("tempdir");
        let path = root.path().join("mediapm.ncl");
        let source = r#"
{
version = 1,
media = {
    local_demo = {
        variant_hashes = { source = "blake3:abc" },
        steps = [
            {
                tool = "ffmpeg",
                input_variants = ["source"],
                output_variants = { normalized = {} },
            },
        ],
    },
},
}
"#;

        std::fs::write(&path, source).expect("write source");
        let err = load_mediapm_document(&path).expect_err("document should fail validation");
        let error_text = err.to_string();
        assert!(
            error_text.contains("required fields") || error_text.contains("missing field `kind`")
        );
    }

    /// Protects per-step variant-flow decoding and string option decoding.
    #[test]
    fn media_step_supports_variant_flow_and_string_options() {
        let root = tempfile::tempdir().expect("tempdir");
        let path = root.path().join("mediapm.ncl");
        let source = r#"
{
version = 1,
media = {
    local_demo = {
        variant_hashes = { source = "blake3:abc" },
        steps = [
            {
                tool = "ffmpeg",
                input_variants = ["source"],
                output_variants = { aac = { kind = "primary", save = "full", idx = 0 } },
                options = {
                    option_args = "-vn",
                    leading_args = "-hide_banner",
                    trailing_args = "-c:a aac",
                },
            },
        ],
    },
},
}
"#;

        std::fs::write(&path, source).expect("write source");
        let document = load_mediapm_document(&path).expect("decode document");
        let step = &document.media["local_demo"].steps[0];
        let flow = resolve_step_variant_flow(step).expect("resolve flow");

        assert_eq!(flow.len(), 1);
        assert_eq!(flow[0].input, "source");
        assert_eq!(flow[0].output, "aac");
        assert!(step.options.contains_key("leading_args"));
        assert!(step.options.contains_key("trailing_args"));
    }

    /// Protects key-agnostic semantics by allowing deep slash-separated output
    /// variant names when values are valid.
    #[test]
    fn output_variants_allow_more_than_one_slash_in_keys() {
        let root = tempfile::tempdir().expect("tempdir");
        let path = root.path().join("mediapm.ncl");
        let source = r#"
{
version = 1,
media = {
    demo = {
        steps = [
            {
                tool = "yt-dlp",
                output_variants = { "subtitles/en/srt" = { kind = "primary", save = "full" } },
                options = { uri = "https://example.com/video" },
            },
        ],
    },
},
}
"#;

        std::fs::write(&path, source).expect("write source");
        let document = load_mediapm_document(&path).expect("document should decode");
        assert!(document.media["demo"].steps[0].output_variants.contains_key("subtitles/en/srt"));
    }

    /// Protects yt-dlp output config decoding by requiring object values.
    #[test]
    fn yt_dlp_output_variants_reject_non_object_values() {
        let root = tempfile::tempdir().expect("tempdir");
        let path = root.path().join("mediapm.ncl");
        let source = r#"
{
version = 1,
media = {
    demo = {
        steps = [
            {
                tool = "yt-dlp",
                output_variants = { video = "audio" },
                options = { uri = "https://example.com/video" },
            },
        ],
    },
},
}
"#;

        std::fs::write(&path, source).expect("write source");
        let err = load_mediapm_document(&path).expect_err("document should fail validation");
        assert!(err.to_string().contains("must be an object"));
    }

    /// Protects strict value schema by rejecting legacy yt-dlp
    /// `*_artifacts` kind names.
    #[test]
    fn yt_dlp_legacy_artifact_kind_aliases_are_rejected() {
        let root = tempfile::tempdir().expect("tempdir");
        let path = root.path().join("mediapm.ncl");
        let source = r#"
{
version = 1,
media = {
    demo = {
        steps = [
            {
                tool = "yt-dlp",
                output_variants = {
                    "subtitles/" = { kind = "subtitle_artifacts", save = "full", langs = "en" },
                },
                options = { uri = "https://example.com/video" },
            },
        ],
    },
},
}
"#;

        std::fs::write(&path, source).expect("write source");
        let err = load_mediapm_document(&path).expect_err("legacy kind aliases should fail");
        assert!(err.to_string().contains("invalid yt-dlp config"));
    }

    /// Protects key-agnostic semantics by allowing folder and scoped keys to
    /// coexist in the same output map when filename templates are not used.
    #[test]
    fn output_variants_allow_scoped_and_folder_keys_together() {
        let root = tempfile::tempdir().expect("tempdir");
        let path = root.path().join("mediapm.ncl");
        let source = r#"
{
version = 1,
media = {
    demo = {
        steps = [
            {
                tool = "yt-dlp",
                output_variants = {
                    "subtitles/" = { kind = "subtitles", save = "full" },
                    "subtitles/en" = { kind = "subtitles", save = "full" },
                },
                options = { uri = "https://example.com/video" },
            },
        ],
    },
},
}
"#;

        std::fs::write(&path, source).expect("write source");
        let document = load_mediapm_document(&path).expect("document should decode");
        let output_variants = &document.media["demo"].steps[0].output_variants;
        assert!(output_variants.contains_key("subtitles/"));
        assert!(output_variants.contains_key("subtitles/en"));
    }

    /// Protects yt-dlp value schema by allowing `langs`/`sub_format` on
    /// non-subtitle kinds for capture-side filtering semantics.
    #[test]
    fn yt_dlp_non_subtitle_variant_allows_langs_and_sub_format() {
        let root = tempfile::tempdir().expect("tempdir");
        let path = root.path().join("mediapm.ncl");
        let source = r#"
{
version = 1,
media = {
    demo = {
        steps = [
            {
                tool = "yt-dlp",
                output_variants = {
                    "thumbnails/" = {
                        kind = "thumbnails",
                        save = "full",
                        langs = "all",
                        sub_format = "vtt",
                    },
                },
                options = { uri = "https://example.com/video" },
            },
        ],
    },
},
}
"#;

        std::fs::write(&path, source).expect("write source");
        let document = load_mediapm_document(&path).expect("document should decode");
        let step = &document.media["demo"].steps[0];
        let decoded = super::decode_output_variant_config(
            MediaStepTool::YtDlp,
            "thumbnails/",
            step.output_variants
                .get("thumbnails/")
                .expect("thumbnails output variant should exist"),
        )
        .expect("yt-dlp output variant should decode");

        match decoded {
            super::DecodedOutputVariantConfig::YtDlp(config) => {
                assert_eq!(config.langs.as_deref(), Some("all"));
                assert_eq!(config.sub_format.as_deref(), Some("vtt"));
            }
            super::DecodedOutputVariantConfig::Generic(config) => {
                panic!("expected yt-dlp config, got Generic({config:?})")
            }
        }
    }

    /// Protects hierarchy file-target semantics by keeping subtitle variants
    /// folder-captured by default.
    #[test]
    fn hierarchy_file_target_rejects_default_folder_subtitle_capture() {
        let root = tempfile::tempdir().expect("tempdir");
        let path = root.path().join("mediapm.ncl");
        let source = r#"
{
version = 1,
media = {
    demo = {
        steps = [
            {
                tool = "yt-dlp",
                output_variants = {
                    subtitles = {
                        kind = "subtitles",
                        save = "full",
                    },
                },
                options = {
                    uri = "https://example.com/video",
                },
            },
        ],
    },
},
hierarchy = [
    {
        path = "demo/subtitles.srt",
        kind = "media",
        id = "demo-subtitles-file",
        media_id = "demo",
        variant = "subtitles",
    },
],
}
"#;

        std::fs::write(&path, source).expect("write source");
        let err = load_mediapm_document(&path)
            .expect_err("default subtitle capture should remain folder output");
        assert!(err.to_string().contains("requires file variants"));
    }

    /// Protects capture-kind override semantics by allowing subtitle
    /// variants to opt into file capture behavior.
    #[test]
    fn hierarchy_file_target_accepts_subtitle_capture_kind_file() {
        let root = tempfile::tempdir().expect("tempdir");
        let path = root.path().join("mediapm.ncl");
        let source = r#"
{
version = 1,
media = {
    demo = {
        steps = [
            {
                tool = "yt-dlp",
                output_variants = {
                    subtitles = {
                        kind = "subtitles",
                        capture_kind = "file",
                        save = "full",
                    },
                },
                options = {
                    uri = "https://example.com/video",
                },
            },
        ],
    },
},
hierarchy = [
    {
        path = "demo/subtitles.srt",
        kind = "media",
        id = "demo-subtitles-file",
        media_id = "demo",
        variant = "subtitles",
    },
],
}
"#;

        std::fs::write(&path, source).expect("write source");
        let document = load_mediapm_document(&path)
            .expect("capture_kind=file should permit file hierarchy target");
        assert!(hierarchy_flat_map(&document).contains_key("demo/subtitles.srt"));
    }

    /// Protects generalized capture-kind semantics by allowing generic
    /// transform outputs to opt into folder validation behavior.
    #[test]
    fn hierarchy_file_target_rejects_generic_capture_kind_folder() {
        let root = tempfile::tempdir().expect("tempdir");
        let path = root.path().join("mediapm.ncl");
        let source = r#"
{
version = 1,
media = {
    demo = {
        variant_hashes = {
            source = "blake3:aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa",
        },
        steps = [
            {
                tool = "ffmpeg",
                input_variants = ["source"],
                output_variants = {
                    result = {
                        kind = "primary",
                        idx = 0,
                        capture_kind = "folder",
                        save = "full",
                    },
                },
                options = {},
            },
        ],
    },
},
hierarchy = [
    {
        path = "demo/result.mkv",
        kind = "media",
        id = "demo-result-file",
        media_id = "demo",
        variant = "result",
    },
],
}
"#;

        std::fs::write(&path, source).expect("write source");
        let err = load_mediapm_document(&path)
            .expect_err("generic capture_kind=folder should reject file target");
        assert!(err.to_string().contains("requires file variants"));
    }

    /// Protects generalized capture-kind semantics by allowing generic
    /// transform outputs to target directory hierarchy paths when set to
    /// folder capture behavior.
    #[test]
    fn hierarchy_directory_target_accepts_generic_capture_kind_folder() {
        let root = tempfile::tempdir().expect("tempdir");
        let path = root.path().join("mediapm.ncl");
        let source = r#"
{
version = 1,
media = {
    demo = {
        variant_hashes = {
            source = "blake3:aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa",
        },
        steps = [
            {
                tool = "ffmpeg",
                input_variants = ["source"],
                output_variants = {
                    result = {
                        kind = "primary",
                        idx = 0,
                        capture_kind = "folder",
                        save = "full",
                    },
                },
                options = {},
            },
        ],
    },
},
hierarchy = [
    {
        path = "demo/result",
        kind = "media_folder",
        id = "demo-result-folder",
        media_id = "demo",
        variants = ["result"],
    },
],
}
"#;

        std::fs::write(&path, source).expect("write source");
        let document = load_mediapm_document(&path)
            .expect("generic capture_kind=folder should permit directory target");
        assert!(hierarchy_flat_map(&document).contains_key("demo/result"));
    }

    /// Protects hierarchy rename semantics by rejecting file-target usage.
    #[test]
    fn hierarchy_file_target_rejects_rename_files_rules() {
        let root = tempfile::tempdir().expect("tempdir");
        let path = root.path().join("mediapm.ncl");
        let source = r#"
{
version = 1,
media = {
    demo = {
        steps = [
            {
                tool = "yt-dlp",
                output_variants = {
                    subtitles = {
                        kind = "subtitles",
                        capture_kind = "file",
                        save = "full",
                    },
                },
                options = {
                    uri = "https://example.com/video",
                },
            },
        ],
    },
},
hierarchy = [
    {
        path = "demo/subtitles.vtt",
        kind = "media",
        id = "demo-subtitles-file",
        media_id = "demo",
        variant = "subtitles",
        rename_files = [
            { pattern = "^(.+)\\.vtt$", replacement = "$1.en.vtt" },
        ],
    },
],
}
"#;

        std::fs::write(&path, source).expect("write source");
        let err =
            load_mediapm_document(&path).expect_err("file-target rename_files must be rejected");
        assert!(err.to_string().contains("rename_files"));
    }

    /// Protects hierarchy rename semantics by allowing directory-target usage.
    #[test]
    fn hierarchy_directory_target_accepts_rename_files_rules() {
        let root = tempfile::tempdir().expect("tempdir");
        let path = root.path().join("mediapm.ncl");
        let source = r#"
{
version = 1,
media = {
    demo = {
        steps = [
            {
                tool = "yt-dlp",
                output_variants = {
                    subtitles = {
                        kind = "subtitles",
                        save = "full",
                    },
                },
                options = {
                    uri = "https://example.com/video",
                },
            },
        ],
    },
},
hierarchy = [
    {
        path = "demo/subtitles",
        kind = "media_folder",
        id = "demo-subtitles-folder",
        media_id = "demo",
        variants = ["subtitles"],
        rename_files = [
            { pattern = "^(.+)\\.vtt$", replacement = "$1.en.vtt" },
        ],
    },
],
}
"#;

        std::fs::write(&path, source).expect("write source");
        let document =
            load_mediapm_document(&path).expect("directory-target rename_files should decode");
        assert!(hierarchy_flat_map(&document).contains_key("demo/subtitles"));
    }

    /// Protects rename replacement interpolation by accepting `${media.id}`
    /// and `${media.metadata.*}` placeholders in directory-target rules.
    #[test]
    fn hierarchy_directory_target_accepts_rename_files_replacement_placeholders() {
        let root = tempfile::tempdir().expect("tempdir");
        let path = root.path().join("mediapm.ncl");
        let source = r#"
{
version = 1,
media = {
    demo = {
        metadata = {
            title = "Demo Title",
        },
        steps = [
            {
                tool = "yt-dlp",
                output_variants = {
                    subtitles = {
                        kind = "subtitles",
                        save = "full",
                    },
                },
                options = {
                    uri = "https://example.com/video",
                },
            },
        ],
    },
},
hierarchy = [
    {
        path = "demo/subtitles",
        kind = "media_folder",
        id = "demo-subtitles-folder",
        media_id = "demo",
        variants = ["subtitles"],
        rename_files = [
            { pattern = "^(.+)\\.vtt$", replacement = "${media.metadata.title} [${media.id}]$1.vtt" },
        ],
    },
],
}
"#;

        std::fs::write(&path, source).expect("write source");
        let document = load_mediapm_document(&path)
            .expect("rename_files replacement placeholders should decode");
        let node = &document.hierarchy[0];
        assert_eq!(node.path, "demo/subtitles".into());
        assert_eq!(node.variants, vec!["subtitles".to_string()]);
        assert_eq!(node.rename_files.len(), 1);
        assert!(node.rename_files[0].replacement.contains("${media.metadata.title}"));
    }

    /// Protects rename replacement placeholder validation by rejecting
    /// undefined metadata references.
    #[test]
    fn hierarchy_directory_target_rejects_rename_files_replacement_unknown_metadata_key() {
        let root = tempfile::tempdir().expect("tempdir");
        let path = root.path().join("mediapm.ncl");
        let source = r#"
{
version = 1,
media = {
    demo = {
        metadata = {
            title = "Demo Title",
        },
        steps = [
            {
                tool = "yt-dlp",
                output_variants = {
                    subtitles = {
                        kind = "subtitles",
                        save = "full",
                    },
                },
                options = {
                    uri = "https://example.com/video",
                },
            },
        ],
    },
},
hierarchy = [
    {
        path = "demo/subtitles",
        kind = "media_folder",
        id = "demo-subtitles-folder",
        media_id = "demo",
        variants = ["subtitles"],
        rename_files = [
            { pattern = "^(.+)\\.vtt$", replacement = "${media.metadata.artist} [${media.id}]$1.vtt" },
        ],
    },
],
}
"#;

        std::fs::write(&path, source).expect("write source");
        let err = load_mediapm_document(&path)
            .expect_err("unknown rename_files replacement metadata key must fail validation");
        assert!(err.to_string().contains("undefined metadata key 'artist'"));
    }

    /// Protects downloader schema by allowing omitted input variants.
    #[test]
    fn yt_dlp_step_allows_omitted_input_variants() {
        let root = tempfile::tempdir().expect("tempdir");
        let path = root.path().join("mediapm.ncl");
        let source = r#"
{
version = 1,
media = {
    demo = {
        steps = [
            {
                tool = "yt-dlp",
                output_variants = { downloaded = { kind = "primary", save = "full" } },
                options = { uri = "https://example.com/video" },
            },
        ],
    },
},
}
"#;

        std::fs::write(&path, source).expect("write source");
        let document = load_mediapm_document(&path).expect("document should decode");
        assert!(document.media["demo"].steps[0].input_variants.is_empty());
    }

    /// Protects yt-dlp schema by rejecting explicit input variant wiring.
    #[test]
    fn yt_dlp_step_rejects_non_empty_input_variants() {
        let root = tempfile::tempdir().expect("tempdir");
        let path = root.path().join("mediapm.ncl");
        let source = r#"
{
version = 1,
media = {
    demo = {
        steps = [
            {
                tool = "yt-dlp",
                input_variants = ["source"],
                output_variants = { downloaded = { kind = "primary", save = "full" } },
                options = { uri = "https://example.com/video" },
            },
        ],
    },
},
}
"#;

        std::fs::write(&path, source).expect("write source");
        let err = load_mediapm_document(&path).expect_err("yt-dlp input_variants must be rejected");
        assert!(
            err.to_string()
                .contains("must not define input_variants for source-ingest tool 'yt-dlp'")
        );
    }

    /// Protects source-ingest schema by rejecting explicit input variants for
    /// import-style ingest steps.
    #[test]
    fn import_step_rejects_non_empty_input_variants() {
        let root = tempfile::tempdir().expect("tempdir");
        let path = root.path().join("mediapm.ncl");
        let source = r#"
{
version = 1,
media = {
    demo = {
        steps = [
            {
                tool = "import",
                input_variants = ["default"],
                output_variants = { default = { kind = "primary", save = "full" } },
                options = {
                    kind = "cas_hash",
                    hash = "blake3:aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa",
                },
            },
        ],
    },
},
}
"#;

        std::fs::write(&path, source).expect("write source");
        let err = load_mediapm_document(&path).expect_err("import input_variants must be rejected");
        assert!(
            err.to_string()
                .contains("must not define input_variants for source-ingest tool 'import'")
        );
    }

    /// Protects step graph validation by requiring top-to-bottom variant wiring.
    #[test]
    fn step_graph_rejects_unknown_input_variant() {
        let root = tempfile::tempdir().expect("tempdir");
        let path = root.path().join("mediapm.ncl");
        let source = r#"
{
version = 1,
media = {
    local_demo = {
        steps = [
            {
                tool = "ffmpeg",
                input_variants = ["default"],
                output_variants = { aac = { kind = "primary", save = "full", idx = 0 } },
            },
        ],
    },
},
}
"#;

        std::fs::write(&path, source).expect("write source");
        let err = load_mediapm_document(&path).expect_err("document should fail validation");
        assert!(err.to_string().contains("default") && err.to_string().contains("unknown"));
    }

    /// Protects local-import source validation for required `cas_hash` options.
    #[test]
    fn import_step_requires_cas_hash_kind_and_hash() {
        let root = tempfile::tempdir().expect("tempdir");
        let path = root.path().join("mediapm.ncl");
        let source = r#"
{
version = 1,
media = {
    local_demo = {
        steps = [
            {
                tool = "import",
                output_variants = { default = { kind = "primary", save = "full" } },
                options = { kind = "cas_hash" },
            },
        ],
    },
},
}
"#;

        std::fs::write(&path, source).expect("write source");
        let err = load_mediapm_document(&path).expect_err("document should fail validation");
        assert!(err.to_string().contains("must define options.hash"));
    }

    /// Protects source-uri bookkeeping helper for online and local media specs.
    #[test]
    fn media_source_uri_prefers_online_uri_and_falls_back_to_local() {
        let online = MediaSourceSpec {
            id: None,
            description: None,
            title: None,
            artist: None,
            workflow_id: None,
            metadata: None,
            variant_hashes: BTreeMap::new(),
            steps: vec![MediaStep {
                tool: MediaStepTool::YtDlp,
                input_variants: Vec::new(),
                output_variants: BTreeMap::from([(
                    "default".to_string(),
                    Value::Object(serde_json::Map::new()),
                )]),
                options: BTreeMap::from([(
                    "uri".to_string(),
                    TransformInputValue::String("https://example.com/video.mkv".to_string()),
                )]),
            }],
        };
        let local = MediaSourceSpec {
            id: None,
            description: None,
            title: None,
            artist: None,
            workflow_id: None,
            metadata: None,
            variant_hashes: BTreeMap::new(),
            steps: vec![MediaStep {
            tool: MediaStepTool::Import,
            input_variants: Vec::new(),
            output_variants: BTreeMap::from([(
                "default".to_string(),
                Value::Object(serde_json::Map::new()),
            )]),
            options: BTreeMap::from([
                ("kind".to_string(), TransformInputValue::String("cas_hash".to_string())),
                (
                    "hash".to_string(),
                    TransformInputValue::String(
                        "blake3:aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa"
                            .to_string(),
                    ),
                ),
            ]),
        }],
        };

        assert_eq!(media_source_uri("remote-id", &online), "https://example.com/video.mkv");
        assert_eq!(media_source_uri("local-id", &local), "local:local-id");
    }

    /// Protects strict metadata schema by accepting literal and
    /// variant-binding metadata values.
    #[test]
    fn media_source_metadata_accepts_literal_and_variant_bindings() {
        let root = tempfile::tempdir().expect("tempdir");
        let path = root.path().join("mediapm.ncl");
        let source = r#"
{
version = 1,
media = {
    demo = {
        metadata = {
            curator = "alice",
            title = {
                variant = "infojson",
                metadata_key = "title",
            },
        },
        steps = [
            {
                tool = "yt-dlp",
                output_variants = {
                    infojson = {
                        kind = "infojson",
                    },
                },
                options = {
                    uri = "https://example.com/video",
                },
            },
        ],
    },
},
}
"#;

        std::fs::write(&path, source).expect("write source");
        let document = load_mediapm_document(&path).expect("decode document");
        let metadata = document
            .media
            .get("demo")
            .and_then(|spec| spec.metadata.as_ref())
            .expect("metadata should decode as object");

        assert_eq!(
            metadata.get("curator"),
            Some(&MediaMetadataValue::Literal("alice".to_string()))
        );

        match metadata.get("title") {
            Some(MediaMetadataValue::Variant(binding)) => {
                assert_eq!(binding.variant, "infojson");
                assert_eq!(binding.metadata_key, "title");
                assert!(binding.transform.is_none());
            }
            other => panic!("expected metadata variant binding, got {other:?}"),
        }
    }

    /// Protects metadata decode by accepting ordered fallback lists that mix
    /// variant bindings and literal values.
    #[test]
    fn media_source_metadata_accepts_fallback_candidate_lists() {
        let root = tempfile::tempdir().expect("tempdir");
        let path = root.path().join("mediapm.ncl");
        let source = r#"
{
version = 1,
media = {
    demo = {
        metadata = {
            title = [
                {
                    variant = "infojson",
                    metadata_key = "title",
                },
                "Unknown Title",
            ],
        },
        steps = [
            {
                tool = "yt-dlp",
                output_variants = {
                    infojson = {
                        kind = "infojson",
                    },
                },
                options = {
                    uri = "https://example.com/video",
                },
            },
        ],
    },
},
}
"#;

        std::fs::write(&path, source).expect("write source");
        let document = load_mediapm_document(&path).expect("decode document");
        let metadata = document
            .media
            .get("demo")
            .and_then(|spec| spec.metadata.as_ref())
            .expect("metadata should decode as object");

        match metadata.get("title") {
            Some(MediaMetadataValue::Fallback(candidates)) => {
                assert_eq!(candidates.len(), 2);
                assert!(matches!(
                    candidates.first(),
                    Some(MediaMetadataValueCandidate::Variant(binding))
                        if binding.variant == "infojson" && binding.metadata_key == "title"
                ));
                assert_eq!(
                    candidates.get(1),
                    Some(&MediaMetadataValueCandidate::Literal("Unknown Title".to_string()))
                );
            }
            other => panic!("expected metadata fallback list, got {other:?}"),
        }
    }

    /// Protects metadata validation by rejecting empty fallback lists.
    #[test]
    fn media_source_metadata_rejects_empty_fallback_lists() {
        let root = tempfile::tempdir().expect("tempdir");
        let path = root.path().join("mediapm.ncl");
        let source = r#"
{
version = 1,
media = {
    demo = {
        metadata = {
            title = [],
        },
        steps = [
            {
                tool = "yt-dlp",
                output_variants = {
                    infojson = {
                        kind = "infojson",
                    },
                },
                options = {
                    uri = "https://example.com/video",
                },
            },
        ],
    },
},
}
"#;

        std::fs::write(&path, source).expect("write source");
        let err = load_mediapm_document(&path).expect_err("empty metadata fallback list must fail");
        assert!(err.to_string().contains("fallback list must be non-empty"));
    }

    /// Protects metadata binding decode by accepting regex transform settings
    /// for variant-backed placeholders.
    #[test]
    fn media_source_metadata_variant_binding_accepts_regex_transform() {
        let root = tempfile::tempdir().expect("tempdir");
        let path = root.path().join("mediapm.ncl");
        let source = r#"
{
version = 1,
media = {
    demo = {
        metadata = {
            video_ext = {
                variant = "infojson",
                metadata_key = "ext",
                transform = {
                    pattern = "(.+)",
                    replacement = ".$1",
                },
            },
        },
        steps = [
            {
                tool = "yt-dlp",
                output_variants = {
                    infojson = {
                        kind = "infojson",
                    },
                },
                options = {
                    uri = "https://example.com/video",
                },
            },
        ],
    },
},
}
"#;

        std::fs::write(&path, source).expect("write source");
        let document = load_mediapm_document(&path).expect("decode document");
        let metadata = document
            .media
            .get("demo")
            .and_then(|spec| spec.metadata.as_ref())
            .expect("metadata should decode as object");

        match metadata.get("video_ext") {
            Some(MediaMetadataValue::Variant(binding)) => {
                assert_eq!(binding.variant, "infojson");
                assert_eq!(binding.metadata_key, "ext");
                let transform = binding.transform.as_ref().expect("transform should decode");
                assert_eq!(transform.pattern, "(.+)");
                assert_eq!(transform.replacement, ".$1");
            }
            other => panic!("expected metadata variant binding, got {other:?}"),
        }
    }

    /// Protects output-variant extension policy by allowing extension only for
    /// ffmpeg/rsgain/media-tagger outputs.
    #[test]
    fn output_variant_extension_rejects_unsupported_tools() {
        let root = tempfile::tempdir().expect("tempdir");
        let path = root.path().join("mediapm.ncl");
        let source = r#"
{
version = 1,
media = {
    demo = {
        steps = [
            {
                tool = "import",
                output_variants = {
                    default = {
                        kind = "primary",
                        extension = "mkv",
                    },
                },
                options = {
                    kind = "cas_hash",
                    hash = "blake3:aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa",
                },
            },
        ],
    },
},
}
"#;

        std::fs::write(&path, source).expect("write source");
        let err = load_mediapm_document(&path).expect_err("import extension should be rejected");
        assert!(err.to_string().contains("must not define extension"));
    }

    /// Protects source metadata top-level shape policy by rejecting
    /// non-object metadata values.
    #[test]
    fn media_source_metadata_rejects_non_object_values() {
        let root = tempfile::tempdir().expect("tempdir");
        let path = root.path().join("mediapm.ncl");
        let source = r#"
{
version = 1,
media = {
    demo = {
        metadata = "invalid",
        steps = [
            {
                tool = "yt-dlp",
                output_variants = {
                    default = {
                        kind = "primary",
                        save = "full",
                    },
                },
                options = {
                    uri = "https://example.com/video",
                },
            },
        ],
    },
},
}
"#;

        std::fs::write(&path, source).expect("write source");
        let err = load_mediapm_document(&path).expect_err("string metadata should be rejected");

        assert!(err.to_string().contains("invalid type: string \"invalid\""));
    }

    /// Protects strict metadata schema by rejecting folder-output variant
    /// bindings for metadata lookup.
    #[test]
    fn media_source_metadata_rejects_folder_output_binding() {
        let root = tempfile::tempdir().expect("tempdir");
        let path = root.path().join("mediapm.ncl");
        let source = r#"
{
version = 1,
media = {
    demo = {
        metadata = {
            title = {
                variant = "subtitles",
                metadata_key = "title",
            },
        },
        steps = [
            {
                tool = "yt-dlp",
                output_variants = {
                    subtitles = {
                        kind = "subtitles",
                    },
                },
                options = {
                    uri = "https://example.com/video",
                },
            },
        ],
    },
},
}
"#;

        std::fs::write(&path, source).expect("write source");
        let err = load_mediapm_document(&path)
            .expect_err("folder variants should be rejected for metadata binding");

        assert!(err.to_string().contains("metadata bindings require file variants"));
    }

    /// Protects output policy defaults by treating omitted save as `true`.
    #[test]
    fn output_variant_policy_defaults_apply_when_save_fields_are_omitted() {
        let yt_dlp = super::decode_output_variant_policy(
            MediaStepTool::YtDlp,
            "video",
            &serde_json::json!({ "kind": "primary" }),
        )
        .expect("decode yt-dlp output policy");
        assert_eq!(yt_dlp.save, OutputSaveConfig::Bool(true));

        let ffmpeg = super::decode_output_variant_policy(
            MediaStepTool::Ffmpeg,
            "audio",
            &serde_json::json!({ "kind": "primary", "idx": 0 }),
        )
        .expect("decode ffmpeg output policy");
        assert_eq!(ffmpeg.save, OutputSaveConfig::Bool(true));
    }

    /// Protects hierarchy validation by allowing file variants to keep the
    /// default `save=true` policy when materialized to file paths.
    #[test]
    fn hierarchy_file_variant_allows_default_save_true() {
        let root = tempfile::tempdir().expect("tempdir");
        let path = root.path().join("mediapm.ncl");
        let source = r#"
{
version = 1,
media = {
    demo = {
        steps = [
            {
                tool = "yt-dlp",
                output_variants = {
                    video = {
                        kind = "primary",
                    },
                },
                options = {
                    uri = "https://example.com/video",
                },
            },
        ],
    },
},
hierarchy = [
    {
        path = "demo/video.mp4",
        kind = "media",
        id = "demo-video",
        media_id = "demo",
        variant = "video",
    },
],
}
"#;

        std::fs::write(&path, source).expect("write source");
        let document =
            load_mediapm_document(&path).expect("hierarchy file variant should be allowed");
        assert!(hierarchy_flat_map(&document).contains_key("demo/video.mp4"));
    }

    /// Protects hierarchy interpolation policy by requiring every
    /// `${media.metadata.*}` placeholder key to be declared in source metadata.
    #[test]
    fn hierarchy_metadata_placeholder_requires_declared_metadata_key() {
        let root = tempfile::tempdir().expect("tempdir");
        let path = root.path().join("mediapm.ncl");
        let source = r#"
{
version = 1,
media = {
    demo = {
        metadata = {
            artist = "The Artist",
        },
        steps = [
            {
                tool = "yt-dlp",
                output_variants = {
                    video = {
                        kind = "primary",
                        save = "full",
                    },
                },
                options = {
                    uri = "https://example.com/video",
                },
            },
        ],
    },
},
hierarchy = [
    {
        path = "library/${media.metadata.title}/demo.mp4",
        kind = "media",
        id = "demo-video",
        media_id = "demo",
        variant = "video",
    },
],
}
"#;

        std::fs::write(&path, source).expect("write source");
        let err = load_mediapm_document(&path)
            .expect_err("undefined metadata placeholder keys should be rejected");
        assert!(err.to_string().contains("undefined metadata key 'title'"));
    }

    /// Protects hierarchy interpolation grammar by rejecting unsupported
    /// placeholder expressions.
    #[test]
    fn hierarchy_metadata_placeholder_rejects_unsupported_expression() {
        let root = tempfile::tempdir().expect("tempdir");
        let path = root.path().join("mediapm.ncl");
        let source = r#"
{
version = 1,
media = {
    demo = {
        metadata = {
            title = "Demo",
        },
        steps = [
            {
                tool = "yt-dlp",
                output_variants = {
                    video = {
                        kind = "primary",
                        save = "full",
                    },
                },
                options = {
                    uri = "https://example.com/video",
                },
            },
        ],
    },
},
hierarchy = [
    {
        path = "library/${media.title}/demo.mp4",
        kind = "media",
        id = "demo-video",
        media_id = "demo",
        variant = "video",
    },
],
}
"#;

        std::fs::write(&path, source).expect("write source");
        let err = load_mediapm_document(&path)
            .expect_err("unsupported placeholder expressions should be rejected");
        assert!(err.to_string().contains("unsupported placeholder"));
    }

    /// Protects hierarchy interpolation grammar by allowing `${media.id}`
    /// placeholders without requiring metadata declarations.
    #[test]
    fn hierarchy_placeholder_allows_media_id_expression() {
        let root = tempfile::tempdir().expect("tempdir");
        let path = root.path().join("mediapm.ncl");
        let source = r#"
{
version = 1,
media = {
    demo = {
        steps = [
            {
                tool = "yt-dlp",
                output_variants = {
                    video = {
                        kind = "primary",
                        save = "full",
                    },
                },
                options = {
                    uri = "https://example.com/video",
                },
            },
        ],
    },
},
hierarchy = [
    {
        path = "library/${media.id}/demo.mp4",
        kind = "media",
        id = "demo-video",
        media_id = "demo",
        variant = "video",
    },
],
}
"#;

        std::fs::write(&path, source).expect("write source");
        let document = load_mediapm_document(&path).expect("media.id placeholder should decode");
        assert!(hierarchy_flat_map(&document).contains_key("library/${media.id}/demo.mp4"));
    }

    /// Protects playlist hierarchy decoding by preserving ordered id entries,
    /// default format policy, and per-item absolute-path overrides.
    #[test]
    fn hierarchy_playlist_entry_decodes_ordered_ids_and_path_modes() {
        let root = tempfile::tempdir().expect("tempdir");
        let path = root.path().join("mediapm.ncl");
        let source = r#"
{
version = 1,
media = {
    a = {
        steps = [
            {
                tool = "yt-dlp",
                output_variants = {
                    video = {
                        kind = "primary",
                        save = "full",
                    },
                },
                options = {
                    uri = "https://example.com/a",
                },
            },
        ],
    },
    b = {
        steps = [
            {
                tool = "yt-dlp",
                output_variants = {
                    video = {
                        kind = "primary",
                        save = "full",
                    },
                },
                options = {
                    uri = "https://example.com/b",
                },
            },
        ],
    },
},
hierarchy = [
    {
        path = "library/a.mp4",
        kind = "media",
        id = "playlist-a",
        media_id = "a",
        variant = "video",
    },
    {
        path = "library/b.mp4",
        kind = "media",
        id = "b",
        media_id = "b",
        variant = "video",
    },
    {
        path = "playlists/demo.m3u8",
        kind = "playlist",
        ids = [
            "playlist-a",
            {
                id = "b",
                path = "absolute",
            },
        ],
    },
],
}
"#;

        std::fs::write(&path, source).expect("write source");
        let document = load_mediapm_document(&path).expect("playlist hierarchy should decode");
        let hierarchy = hierarchy_flat_map(&document);
        let playlist_entry = hierarchy.get("playlists/demo.m3u8").expect("playlist entry exists");

        assert!(matches!(playlist_entry.kind, HierarchyEntryKind::Playlist));
        assert!(matches!(playlist_entry.format, PlaylistFormat::M3u8));
        assert_eq!(playlist_entry.ids.len(), 2);
        assert_eq!(playlist_entry.ids[0].id(), "playlist-a");
        assert!(matches!(playlist_entry.ids[0].path_mode(), PlaylistEntryPathMode::Relative));
        assert_eq!(playlist_entry.ids[1].id(), "b");
        assert!(matches!(playlist_entry.ids[1].path_mode(), PlaylistEntryPathMode::Absolute));
    }

    /// Protects playlist hierarchy decoding by preserving explicit non-default
    /// format selections and duplicate id ordering semantics.
    #[test]
    fn hierarchy_playlist_entry_decodes_explicit_format_and_duplicate_ids() {
        let root = tempfile::tempdir().expect("tempdir");
        let path = root.path().join("mediapm.ncl");
        let source = r#"
{
version = 1,
media = {
    a = {
        steps = [
            {
                tool = "yt-dlp",
                output_variants = {
                    video = {
                        kind = "primary",
                        save = "full",
                    },
                },
                options = {
                    uri = "https://example.com/a",
                },
            },
        ],
    },
},
hierarchy = [
    {
        path = "library/a.mp4",
        kind = "media",
        id = "playlist-a",
        media_id = "a",
        variant = "video",
    },
    {
        path = "playlists/demo.xspf",
        kind = "playlist",
        format = "xspf",
        ids = [
            "playlist-a",
            {
                id = "playlist-a",
            },
            "playlist-a",
        ],
    },
],
}
"#;

        std::fs::write(&path, source).expect("write source");
        let document =
            load_mediapm_document(&path).expect("playlist hierarchy with xspf should decode");
        let hierarchy = hierarchy_flat_map(&document);
        let playlist_entry = hierarchy.get("playlists/demo.xspf").expect("playlist entry exists");

        assert!(matches!(playlist_entry.kind, HierarchyEntryKind::Playlist));
        assert!(matches!(playlist_entry.format, PlaylistFormat::Xspf));
        assert_eq!(playlist_entry.ids.len(), 3);
        assert_eq!(playlist_entry.ids[0].id(), "playlist-a");
        assert_eq!(playlist_entry.ids[1].id(), "playlist-a");
        assert_eq!(playlist_entry.ids[2].id(), "playlist-a");
        assert!(matches!(playlist_entry.ids[0].path_mode(), PlaylistEntryPathMode::Relative));
        assert!(matches!(playlist_entry.ids[1].path_mode(), PlaylistEntryPathMode::Relative));
        assert!(matches!(playlist_entry.ids[2].path_mode(), PlaylistEntryPathMode::Relative));
    }

    /// Protects playlist hierarchy validation by rejecting unknown referenced
    /// ids.
    #[test]
    fn hierarchy_playlist_entry_rejects_unknown_referenced_id() {
        let root = tempfile::tempdir().expect("tempdir");
        let path = root.path().join("mediapm.ncl");
        let source = r#"
{
version = 1,
media = {
    demo = {
        steps = [
            {
                tool = "yt-dlp",
                output_variants = {
                    video = {
                        kind = "primary",
                        save = "full",
                    },
                },
                options = {
                    uri = "https://example.com/video",
                },
            },
        ],
    },
},
hierarchy = [
    {
        path = "library/demo.mp4",
        kind = "media",
        id = "demo-video",
        media_id = "demo",
        variant = "video",
    },
    {
        path = "playlists/demo.m3u8",
        kind = "playlist",
        ids = ["unknown-id"],
    },
],
}
"#;

        std::fs::write(&path, source).expect("write source");
        let error = load_mediapm_document(&path)
            .expect_err("playlist should reject unknown referenced ids");
        assert!(error.to_string().contains("unknown hierarchy id 'unknown-id'"));
    }

    /// Protects hierarchy id uniqueness by rejecting duplicate `hierarchy[*].id`
    /// assignments across media nodes.
    #[test]
    fn media_hierarchy_id_rejects_duplicates_across_media_entries() {
        let root = tempfile::tempdir().expect("tempdir");
        let path = root.path().join("mediapm.ncl");
        let source = r#"
{
version = 1,
media = {
    a = {
        variant_hashes = {
            video = "blake3:aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa",
        },
    },
    b = {
        variant_hashes = {
            video = "blake3:bbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbb",
        },
    },
},
hierarchy = [
    {
        path = "library/a.mp4",
        kind = "media",
        id = "duplicate",
        media_id = "a",
        variant = "video",
    },
    {
        path = "library/b.mp4",
        kind = "media",
        id = "duplicate",
        media_id = "b",
        variant = "video",
    },
],
}
"#;

        std::fs::write(&path, source).expect("write source");
        let error =
            load_mediapm_document(&path).expect_err("duplicate hierarchy ids should be rejected");
        assert!(error.to_string().contains("hierarchy id 'duplicate'"));
        assert!(
            error.to_string().contains("duplicated") || error.to_string().contains("duplicates")
        );
    }

    /// Protects hierarchy validation by allowing folder variants to keep the
    /// default `save=true` policy when materialized to directory paths.
    #[test]
    fn hierarchy_directory_variant_allows_default_save_true() {
        let root = tempfile::tempdir().expect("tempdir");
        let path = root.path().join("mediapm.ncl");
        let source = r#"
{
version = 1,
media = {
    demo = {
        steps = [
            {
                tool = "yt-dlp",
                output_variants = {
                    "subtitles" = {
                        kind = "subtitles",
                    },
                },
                options = {
                    uri = "https://example.com/video",
                },
            },
        ],
    },
},
hierarchy = [
    {
        path = "demo/subtitles",
        kind = "media_folder",
        id = "demo-subtitles-folder",
        media_id = "demo",
        variants = ["subtitles"],
    },
],
}
"#;

        std::fs::write(&path, source).expect("write source");
        let document =
            load_mediapm_document(&path).expect("hierarchy folder variant should be allowed");
        assert!(hierarchy_flat_map(&document).contains_key("demo/subtitles"));
    }

    /// Protects hierarchy typing by rejecting folder variants for file paths.
    #[test]
    fn hierarchy_file_path_rejects_folder_variant_output() {
        let root = tempfile::tempdir().expect("tempdir");
        let path = root.path().join("mediapm.ncl");
        let source = r#"
{
version = 1,
media = {
    demo = {
        steps = [
            {
                tool = "yt-dlp",
                output_variants = {
                    subtitles = {
                        kind = "subtitles",
                        save = "full",
                    },
                },
                options = {
                    uri = "https://example.com/video",
                },
            },
        ],
    },
},
hierarchy = [
    {
        path = "demo/subtitles.txt",
        kind = "media",
        id = "demo-subtitles-file",
        media_id = "demo",
        variant = "subtitles",
    },
],
}
"#;

        std::fs::write(&path, source).expect("write source");
        let err = load_mediapm_document(&path)
            .expect_err("file paths must reject folder output variants");
        assert!(err.to_string().contains("requires file variants"));
    }

    /// Protects selector-object support by allowing regex object syntax in
    /// both `input_variants` and `media_folder` hierarchy `variants`.
    #[test]
    fn regex_selector_objects_are_supported_for_steps_and_hierarchy() {
        let root = tempfile::tempdir().expect("tempdir");
        let path = root.path().join("mediapm.ncl");
        let source = r#"
{
version = 1,
media = {
    demo = {
        steps = [
            {
                tool = "import",
                output_variants = {
                    source = {
                        kind = "result",
                        save = "full",
                    },
                },
                options = {
                    kind = "cas_hash",
                    hash = "blake3:aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa",
                },
            },
            {
                tool = "ffmpeg",
                input_variants = [{ regex = "^source$" }],
                output_variants = {
                    video = {
                        kind = "primary",
                        idx = 0,
                        capture_kind = "folder",
                        save = "full",
                        extension = "mkv",
                    },
                },
            },
        ],
    },
},
hierarchy = [
    {
        path = "library/demo",
        kind = "media_folder",
        id = "demo-folder",
        media_id = "demo",
        variants = [{ regex = "^video$" }],
    },
],
}
"#;

        std::fs::write(&path, source).expect("write source");
        let document = load_mediapm_document(&path).expect("regex selector objects should decode");

        assert!(document.media["demo"].steps[1].input_variants[0].contains("source"));
        let hierarchy = hierarchy_flat_map(&document);
        let media_folder = hierarchy
            .get("library/demo")
            .expect("media_folder hierarchy entry should flatten without trailing slash");
        assert_eq!(media_folder.variants.len(), 1);
        assert!(media_folder.variants[0].contains("video"));
    }

    /// Protects selector decode by rejecting malformed regex selector objects.
    #[test]
    fn regex_selector_object_rejects_invalid_pattern() {
        let root = tempfile::tempdir().expect("tempdir");
        let path = root.path().join("mediapm.ncl");
        let source = r#"
{
version = 1,
media = {
    demo = {
        steps = [
            {
                tool = "yt-dlp",
                output_variants = {
                    video = {
                        kind = "primary",
                        save = "full",
                    },
                },
                options = {
                    uri = "https://example.com/video",
                },
            },
        ],
    },
},
hierarchy = [
    {
        path = "library/demo",
        kind = "media_folder",
        media_id = "demo",
        variants = [{ regex = "[" }],
    },
],
}
"#;

        std::fs::write(&path, source).expect("write source");
        let err = load_mediapm_document(&path)
            .expect_err("invalid regex selector object must be rejected");
        assert!(err.to_string().contains("regex selector"));
    }
}
