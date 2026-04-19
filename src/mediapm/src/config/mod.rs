//! Nickel-backed Phase 3 `mediapm.ncl` document model and I/O helpers.
//!
//! The `mediapm.ncl` file is the declarative desired-state surface for Phase 3:
//! media sources, hierarchy mapping, and desired tool enablement.
//!
//! We evaluate Nickel through `nickel-lang-core` and deserialize the exported
//! value into Rust structs. This keeps parsing behavior deterministic while still
//! supporting regular Nickel syntax in user-authored files.

use std::collections::BTreeMap;
use std::fs;
use std::path::{Path, PathBuf};
use std::str::FromStr;
use std::time::{SystemTime, UNIX_EPOCH};

use mediapm_cas::Hash;
use mediapm_conductor::default_runtime_inherited_env_vars_for_host;
use nickel_lang_core::error::{Error as NickelError, NullReporter};
use nickel_lang_core::eval::cache::CacheImpl;
use nickel_lang_core::program::Program;
use serde::{Deserialize, Deserializer, Serialize};
use serde_json::Value;
use url::Url;

use crate::error::MediaPmError;

pub(crate) mod versions;

/// Current persisted schema marker for `mediapm.ncl`.
pub const MEDIAPM_DOCUMENT_VERSION: u32 = versions::latest_nickel_version();

/// Default max number of ffmpeg indexed input slots when `tools.ffmpeg`
/// does not provide an explicit override.
pub const DEFAULT_FFMPEG_MAX_INPUT_SLOTS: u32 = 64;
/// Default max number of ffmpeg indexed output slots when `tools.ffmpeg`
/// does not provide an explicit override.
pub const DEFAULT_FFMPEG_MAX_OUTPUT_SLOTS: u32 = 64;

/// Platform-keyed inherited environment-variable names.
///
/// Keys are normalized case-insensitively at merge/read time so users can
/// author values with natural casing (`windows`, `Windows`, `WINDOWS`, ...)
/// without changing runtime semantics.
pub type PlatformInheritedEnvVars = BTreeMap<String, Vec<String>>;

/// Top-level Phase 3 Nickel document.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct MediaPmDocument {
    /// Explicit schema marker for migration safety.
    pub version: u32,
    /// Optional runtime-path overrides for Phase 3 local state.
    #[serde(default)]
    pub runtime: MediaRuntimeStorage,
    /// Declarative desired tool requirements keyed by logical tool name.
    #[serde(default)]
    pub tools: BTreeMap<String, ToolRequirement>,
    /// Media source registry keyed by stable media id.
    #[serde(default)]
    pub media: BTreeMap<String, MediaSourceSpec>,
    /// Hierarchy mapping from relative path to media variant selection.
    #[serde(default)]
    pub hierarchy: BTreeMap<String, HierarchyEntry>,
}

impl Default for MediaPmDocument {
    fn default() -> Self {
        Self {
            version: MEDIAPM_DOCUMENT_VERSION,
            runtime: MediaRuntimeStorage::default(),
            tools: BTreeMap::new(),
            media: BTreeMap::new(),
            hierarchy: BTreeMap::new(),
        }
    }
}

/// Runtime path overrides for Phase 3 local state.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize, Default)]
pub struct MediaRuntimeStorage {
    /// Optional override for `.mediapm/` runtime root.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub mediapm_dir: Option<String>,
    /// Optional override for materialized library directory.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub library_dir: Option<String>,
    /// Optional override for staging tmp directory.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub tmp_dir: Option<String>,
    /// Optional override for `mediapm`-managed conductor user config path.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub conductor_config: Option<String>,
    /// Optional override for `mediapm`-managed conductor machine config path.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub conductor_machine_config: Option<String>,
    /// Optional override for `mediapm`-managed conductor runtime state path.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub conductor_state: Option<String>,
    /// Optional additional inherited host environment-variable names for
    /// conductor executable process environments, keyed by platform.
    ///
    /// Runtime always keeps the host-default baseline and merges only the
    /// active host platform entry (`windows`, `linux`, `macos`, etc.) on top
    /// with case-insensitive de-duplication.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub inherited_env_vars: Option<PlatformInheritedEnvVars>,
    /// Optional override for `mediapm` lockfile path.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub lockfile: Option<String>,
    /// Optional override for runtime dotenv file used for credential loading.
    ///
    /// When omitted, the effective default path is `<runtime.mediapm_dir>/.env`.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub env_file: Option<String>,
    /// Optional schema export directory policy for embedded `mediapm.ncl`
    /// Nickel contracts.
    ///
    /// Tri-state semantics:
    /// - omitted (`None`): export schemas to default `<runtime.mediapm_dir>/config`,
    /// - explicit `null` (`Some(None)`): disable schema export,
    /// - explicit string (`Some(Some(path))`): export to that path.
    #[serde(default, skip_serializing_if = "runtime_schema_export_is_omitted")]
    pub schema_config_dir: Option<Option<String>>,
    /// Optional toggle for shared global user-level managed-tool download cache.
    ///
    /// When omitted, the cache is enabled by default.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub use_user_download_cache: Option<bool>,
}

/// Returns whether runtime schema-export policy was omitted from config.
fn runtime_schema_export_is_omitted(value: &Option<Option<String>>) -> bool {
    value.is_none()
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
    /// Returns whether shared global user-level download cache should be used.
    ///
    /// Absent configuration defaults to `true` so repeated tool downloads can
    /// reuse payload bytes across all local `mediapm` workspaces for this user.
    #[must_use]
    pub const fn use_user_download_cache_enabled(&self) -> bool {
        match self.use_user_download_cache {
            Some(value) => value,
            None => true,
        }
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
}

/// Declarative tool requirement for one logical media tool.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
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
    /// Optional release-metadata recheck interval in seconds.
    ///
    /// When present, `mediapm` reuses cached release metadata until the
    /// interval elapses, then refreshes from upstream release APIs.
    /// When omitted, release metadata is refreshed on each reconciliation
    /// attempt (while still allowing cache fallback on refresh errors).
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
        && value.is_finite()
        && value >= 0.0
        && value.fract() == 0.0
        && value <= u64::MAX as f64
    {
        return Ok(Some(value as u64));
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
        && value.is_finite()
        && value >= 0.0
        && value.fract() == 0.0
        && value <= u32::MAX as f64
    {
        return Ok(Some(value as u32));
    }

    Err(serde::de::Error::custom("ffmpeg slot limit must be a non-negative integer"))
}

/// One media metadata value source declared under `media.<id>.metadata`.
///
/// Metadata values are intentionally strict and support exactly two forms:
/// - `"text"` literal values,
/// - object bindings that extract one key from one produced file variant.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(untagged)]
pub enum MediaMetadataValue {
    /// Literal metadata text value.
    Literal(String),
    /// Variant-file metadata lookup binding.
    Variant(MediaMetadataVariantBinding),
}

/// Variant-file metadata lookup binding for `media.<id>.metadata` values.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct MediaMetadataVariantBinding {
    /// Variant key whose produced file bytes should be inspected.
    pub variant: String,
    /// Metadata key to extract from that variant file.
    pub metadata_key: String,
}

/// Source registry entry for one media item.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct MediaSourceSpec {
    /// Optional human-readable description.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub description: Option<String>,
    /// Optional explicit conductor workflow id override.
    ///
    /// When omitted, `mediapm` maps each media id to exactly one managed
    /// workflow id using the default prefix policy.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub workflow_id: Option<String>,
    /// Optional strict metadata object for media-specific path interpolation.
    ///
    /// Each key maps to either:
    /// - one literal string value, or
    /// - one `{ variant, metadata_key }` object that resolves metadata from a
    ///   file variant produced by this media source.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub metadata: Option<BTreeMap<String, MediaMetadataValue>>,
    /// Optional pre-seeded CAS hash pointers keyed by variant name.
    ///
    /// These variants seed step input bindings before the ordered step graph
    /// executes.
    #[serde(default, skip_serializing_if = "BTreeMap::is_empty")]
    pub variant_hashes: BTreeMap<String, String>,
    /// Ordered media-processing steps.
    ///
    /// Every step declares tool-specific `options`, `input_variants` for
    /// non-source-ingest transforms, and `output_variants` keyed by output
    /// variant name.
    /// Source-ingest tools (`yt-dlp`, `import`, `import-once`) must keep
    /// `input_variants` empty.
    /// Variant outputs flow top-to-bottom across this ordered list.
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub steps: Vec<MediaStep>,
}

/// One ordered media-processing step.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct MediaStep {
    /// Tool kind used for this step.
    pub tool: MediaStepTool,
    /// Input variants consumed by this step.
    ///
    /// Source-ingest tools must keep this list empty because they originate
    /// content directly from their own options (for example `options.uri` or
    /// `options.hash`) rather than from prior step outputs.
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub input_variants: Vec<String>,
    /// Output variants produced by this step.
    ///
    /// Each key is one produced output variant name and each value is one
    /// tool-specific output config value.
    ///
    /// Key names are identity-only and have no built-in tool semantics.
    /// Tool behavior is decoded entirely from each value.
    ///
    /// Value-shape policy:
    /// - values must always be objects,
    /// - all values must define `kind`,
    /// - `save` defaults to `true` when omitted,
    /// - `save_full` defaults to `false` when omitted,
    /// - ffmpeg values must also define numeric `idx`.
    #[serde(default, skip_serializing_if = "BTreeMap::is_empty")]
    pub output_variants: BTreeMap<String, Value>,
    /// Operation-specific option map.
    ///
    /// Unknown option keys are rejected at document-load validation time.
    /// For online downloaders, the source URL is declared in this map as
    /// `options.uri`.
    ///
    /// For generated boolean-style option inputs, runtime command templates
    /// only enable boolean toggles when the value is exactly `"true"`.
    /// Any other value is treated as disabled.
    ///
    /// Values are scalar strings by default. Ordered string lists are only
    /// valid for low-level list-style input bindings (`option_args`,
    /// `leading_args`, and `trailing_args`).
    ///
    /// Low-level input bindings are declared here (instead of a separate
    /// `input_options` map).
    #[serde(default, skip_serializing_if = "BTreeMap::is_empty")]
    pub options: BTreeMap<String, TransformInputValue>,
}

/// Supported media-step tool kinds.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "kebab-case")]
pub enum MediaStepTool {
    /// `yt-dlp` online-media downloader.
    YtDlp,
    /// `import` builtin source ingestion from existing CAS payload hash.
    Import,
    /// `import` builtin source ingestion that pins imported output for cache
    /// reuse by default.
    ImportOnce,
    /// `ffmpeg` media transform.
    Ffmpeg,
    /// `rsgain` loudness transform.
    Rsgain,
    /// `media-tagger` native metadata tagging transform.
    MediaTagger,
}

impl MediaStepTool {
    /// Returns canonical persisted tool label.
    #[must_use]
    pub const fn as_str(self) -> &'static str {
        match self {
            Self::YtDlp => "yt-dlp",
            Self::Import => "import",
            Self::ImportOnce => "import-once",
            Self::Ffmpeg => "ffmpeg",
            Self::Rsgain => "rsgain",
            Self::MediaTagger => "media-tagger",
        }
    }

    /// Returns true when this tool is an online-media downloader.
    #[must_use]
    pub const fn is_online_media_downloader(self) -> bool {
        matches!(self, Self::YtDlp)
    }

    /// Returns true when this step acts as source-ingest entrypoint.
    #[must_use]
    pub const fn is_source_ingest_tool(self) -> bool {
        matches!(self, Self::YtDlp | Self::Import | Self::ImportOnce)
    }
}

/// One transform input-option binding value.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(untagged)]
pub enum TransformInputValue {
    /// Scalar string input value.
    String(String),
    /// Ordered list-of-strings input value.
    StringList(Vec<String>),
}

/// Shared optional per-variant persistence-policy settings.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Deserialize)]
#[serde(deny_unknown_fields)]
pub(crate) struct OutputVariantPolicyConfig {
    /// Optional save-policy override (defaults to `true`).
    #[serde(default = "default_output_variant_save")]
    pub(crate) save: bool,
    /// Optional full-persistence override (defaults to `false`).
    #[serde(default = "default_output_variant_save_full")]
    pub(crate) save_full: bool,
}

/// Returns the default output-persistence save policy for one variant.
#[must_use]
const fn default_output_variant_save() -> bool {
    true
}

/// Returns the default output-persistence full-retention policy for one variant.
#[must_use]
const fn default_output_variant_save_full() -> bool {
    false
}

/// Generic output-variant configuration for non-yt-dlp tools.
///
/// Output-variant values are always explicit objects with:
/// - required `kind` output capture key,
/// - optional `save` policy (defaults to `true`),
/// - optional `save_full` policy (defaults to `false`),
/// - optional `zip_member` selector,
/// - optional `idx` selector for ffmpeg multi-output routing.
#[derive(Debug, Clone, PartialEq, Eq, Deserialize)]
#[serde(deny_unknown_fields)]
pub(crate) struct GenericOutputVariantConfig {
    /// Explicit generated-tool output kind/capture name bound for this variant.
    pub(crate) kind: String,
    /// Optional save-policy override (defaults to `true`).
    #[serde(default = "default_output_variant_save")]
    pub(crate) save: bool,
    /// Optional full-persistence override (defaults to `false`).
    #[serde(default = "default_output_variant_save_full")]
    pub(crate) save_full: bool,
    /// Optional ZIP member selector used when downstream bindings consume this
    /// output variant.
    ///
    /// When provided, runtime resolves `${step_output...}` references through
    /// `:zip(<member>)` against the selected output payload.
    #[serde(default)]
    pub(crate) zip_member: Option<String>,
    /// Optional ffmpeg output index selector.
    ///
    /// For ffmpeg steps, this field is required and selects which generated
    /// ffmpeg output slot this variant should bind.
    #[serde(default, deserialize_with = "deserialize_optional_u32_from_number")]
    pub(crate) idx: Option<u32>,
}

impl From<&GenericOutputVariantConfig> for OutputVariantPolicyConfig {
    fn from(value: &GenericOutputVariantConfig) -> Self {
        Self { save: value.save, save_full: value.save_full }
    }
}

/// Deserializes optional `u32` values while accepting integral floating-point
/// numbers exported by Nickel (for example `3.0`).
fn deserialize_optional_u32_from_number<'de, D>(deserializer: D) -> Result<Option<u32>, D::Error>
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
            .map_err(|_| serde::de::Error::custom("idx must be within u32 range"));
    }

    if let Some(value) = raw.as_f64()
        && value.is_finite()
        && value >= 0.0
        && value.fract() == 0.0
        && value <= u32::MAX as f64
    {
        return Ok(Some(value as u32));
    }

    Err(serde::de::Error::custom("idx must be a non-negative integer"))
}

/// Value-driven yt-dlp output-variant kinds.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Deserialize)]
pub(crate) enum YtDlpOutputKind {
    /// Primary downloaded media payload.
    #[serde(rename = "primary")]
    Primary,
    /// Full sandbox artifact bundle.
    #[serde(rename = "sandbox", alias = "sandbox_artifacts")]
    Sandbox,
    /// Subtitle artifact bundle.
    #[serde(rename = "subtitle", alias = "subtitle_artifacts")]
    Subtitle,
    /// Auto-subtitle artifact bundle.
    #[serde(rename = "auto_subtitle", alias = "auto_subtitle_artifacts")]
    AutoSubtitle,
    /// Thumbnail artifact bundle.
    #[serde(rename = "thumbnail", alias = "thumbnail_artifacts")]
    Thumbnail,
    /// Description sidecar file.
    #[serde(rename = "description", alias = "description_artifacts")]
    Description,
    /// Annotation artifact bundle.
    #[serde(rename = "annotation", alias = "annotation_artifacts")]
    Annotation,
    /// Info-JSON sidecar file.
    #[serde(rename = "infojson", alias = "infojson_artifacts")]
    Infojson,
    /// Comment artifact bundle.
    #[serde(rename = "comments", alias = "comments_artifacts")]
    Comments,
    /// Link/internet-shortcut artifact bundle.
    #[serde(rename = "link", alias = "link_artifacts")]
    Link,
    /// Split chapter artifact bundle.
    #[serde(rename = "chapter", alias = "chapter_artifacts")]
    Chapter,
    /// Playlist-video artifact bundle.
    #[serde(rename = "playlist_video", alias = "playlist_video_artifacts")]
    PlaylistVideo,
    /// Playlist-thumbnail artifact bundle.
    #[serde(rename = "playlist_thumbnail", alias = "playlist_thumbnail_artifacts")]
    PlaylistThumbnail,
    /// Playlist-description artifact bundle.
    #[serde(rename = "playlist_description", alias = "playlist_description_artifacts")]
    PlaylistDescription,
    /// Playlist-infojson artifact bundle.
    #[serde(rename = "playlist_infojson", alias = "playlist_infojson_artifacts")]
    PlaylistInfojson,
}

/// yt-dlp per-variant config.
#[derive(Debug, Clone, PartialEq, Eq, Deserialize)]
#[serde(deny_unknown_fields)]
pub(crate) struct YtDlpOutputVariantConfig {
    /// Value-driven output semantic kind.
    pub(crate) kind: YtDlpOutputKind,
    /// Optional save-policy override (defaults to `true`).
    #[serde(default = "default_output_variant_save")]
    pub(crate) save: bool,
    /// Optional full-persistence override (defaults to `false`).
    #[serde(default = "default_output_variant_save_full")]
    pub(crate) save_full: bool,
    /// Optional explicit yt-dlp format selector (`-f`).
    #[serde(default)]
    pub(crate) format: Option<String>,
    /// Optional subtitle language selector (`--sub-langs`).
    #[serde(default)]
    pub(crate) langs: Option<String>,
    /// Optional subtitle format selector (`--sub-format`).
    #[serde(default)]
    pub(crate) sub_format: Option<String>,
    /// Optional conversion target for selected output family.
    #[serde(default)]
    pub(crate) convert: Option<String>,
    /// Optional ZIP member selector used when downstream bindings consume this
    /// output variant.
    #[serde(default)]
    pub(crate) zip_member: Option<String>,
}

impl From<&YtDlpOutputVariantConfig> for OutputVariantPolicyConfig {
    fn from(value: &YtDlpOutputVariantConfig) -> Self {
        Self { save: value.save, save_full: value.save_full }
    }
}

/// Parsed output-variant config for one step output entry.
#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) enum DecodedOutputVariantConfig {
    /// Generic output mapping semantics.
    Generic(GenericOutputVariantConfig),
    /// yt-dlp kind-driven output mapping semantics.
    YtDlp(YtDlpOutputVariantConfig),
}

/// Decodes one output-variant configuration value using tool-specific value
/// semantics.
pub(crate) fn decode_output_variant_config(
    tool: MediaStepTool,
    variant_key: &str,
    value: &Value,
) -> Result<DecodedOutputVariantConfig, String> {
    let decoded = match tool {
        MediaStepTool::YtDlp => {
            if !value.is_object() {
                return Err(format!(
                    "output variant '{variant_key}' for tool '{}' must be an object with at least a 'kind' field",
                    tool.as_str()
                ));
            }

            let config = serde_json::from_value::<YtDlpOutputVariantConfig>(value.clone())
                .map_err(|error| {
                    format!(
                        "output variant '{variant_key}' for tool '{}' has invalid yt-dlp config: {error}",
                        tool.as_str()
                    )
                })?;

            if !matches!(config.kind, YtDlpOutputKind::Subtitle | YtDlpOutputKind::AutoSubtitle)
                && (config.langs.as_deref().is_some() || config.sub_format.as_deref().is_some())
            {
                return Err(format!(
                    "output variant '{variant_key}' for tool '{}' uses langs/sub_format, but those fields are only valid for subtitle kinds",
                    tool.as_str()
                ));
            }

            DecodedOutputVariantConfig::YtDlp(config)
        }
        _ => {
            if !value.is_object() {
                return Err(format!(
                    "output variant '{variant_key}' for tool '{}' must be an object with at least field 'kind'",
                    tool.as_str()
                ));
            }

            let config = serde_json::from_value::<GenericOutputVariantConfig>(value.clone())
                .map_err(|error| {
                    format!(
                        "output variant '{variant_key}' for tool '{}' has invalid config: {error}",
                        tool.as_str()
                    )
                })?;
            DecodedOutputVariantConfig::Generic(config)
        }
    };

    match &decoded {
        DecodedOutputVariantConfig::Generic(config) => {
            if config.kind.trim().is_empty() {
                return Err(format!(
                    "output variant '{variant_key}' for tool '{}' kind must be non-empty",
                    tool.as_str()
                ));
            }

            if !matches!(tool, MediaStepTool::Ffmpeg) && config.idx.is_some() {
                return Err(format!(
                    "output variant '{variant_key}' for tool '{}' must not define idx; idx is only valid for tool 'ffmpeg'",
                    tool.as_str()
                ));
            }

            if matches!(tool, MediaStepTool::Ffmpeg) && config.idx.is_none() {
                return Err(format!(
                    "output variant '{variant_key}' for tool '{}' must define idx",
                    tool.as_str()
                ));
            }

            if let Some(zip_member) = config.zip_member.as_deref()
                && zip_member.trim().is_empty()
            {
                return Err(format!(
                    "output variant '{variant_key}' for tool '{}' zip_member must be non-empty",
                    tool.as_str()
                ));
            }
        }
        DecodedOutputVariantConfig::YtDlp(config) => {
            if let Some(zip_member) = config.zip_member.as_deref()
                && zip_member.trim().is_empty()
            {
                return Err(format!(
                    "output variant '{variant_key}' for tool '{}' zip_member must be non-empty",
                    tool.as_str()
                ));
            }
        }
    }

    Ok(decoded)
}

/// Decodes one output-variant policy object for workflow output persistence.
pub(crate) fn decode_output_variant_policy(
    tool: MediaStepTool,
    variant_key: &str,
    value: &Value,
) -> Result<OutputVariantPolicyConfig, String> {
    match decode_output_variant_config(tool, variant_key, value)? {
        DecodedOutputVariantConfig::Generic(config) => Ok(OutputVariantPolicyConfig::from(&config)),
        DecodedOutputVariantConfig::YtDlp(config) => Ok(OutputVariantPolicyConfig::from(&config)),
    }
}

/// Resolved per-step input/output variant mapping.
#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) struct ResolvedStepVariantFlow {
    /// Input variant name consumed by one generated step.
    pub input: String,
    /// Output variant name produced by one generated step.
    pub output: String,
}

/// Resolves one step's effective variant-flow entries.
///
/// Rules:
/// - source-ingest tools must not declare `input_variants` (always empty),
/// - non-source-ingest tools require non-empty input + output variant lists,
/// - non-source-ingest lists must have equal length, or one input fan-outs to
///   all outputs,
/// - empty/blank variant names are rejected,
pub(crate) fn resolve_step_variant_flow(
    step: &MediaStep,
) -> Result<Vec<ResolvedStepVariantFlow>, String> {
    for output in step.output_variants.keys() {
        let output = output.trim();
        if output.is_empty() {
            return Err("contains an empty output variant".to_string());
        }
    }

    for input in &step.input_variants {
        let input = input.trim();
        if input.is_empty() {
            return Err("contains an empty input variant".to_string());
        }
    }

    if step.tool.is_source_ingest_tool() && !step.input_variants.is_empty() {
        return Err(format!(
            "must not define input_variants for source-ingest tool '{}'",
            step.tool.as_str()
        ));
    }

    if !step.tool.is_source_ingest_tool() && step.input_variants.is_empty() {
        return Err("must define at least one input variant".to_string());
    }
    if step.output_variants.is_empty() {
        return Err("must define at least one output variant".to_string());
    }

    if matches!(step.tool, MediaStepTool::Ffmpeg) {
        let outputs = step
            .output_variants
            .keys()
            .map(|output| Ok(output.trim().to_string()))
            .collect::<Result<Vec<_>, String>>()?;
        let primary_input = step
            .input_variants
            .first()
            .map(|value| value.trim().to_string())
            .ok_or_else(|| "must define at least one input variant".to_string())?;

        return Ok(outputs
            .into_iter()
            .map(|output| ResolvedStepVariantFlow { input: primary_input.clone(), output })
            .collect());
    }

    if !step.input_variants.is_empty()
        && step.input_variants.len() != 1
        && step.input_variants.len() != step.output_variants.len()
    {
        return Err(format!(
            "must define one input variant or equal counts of input_variants ({}) and output_variants ({})",
            step.input_variants.len(),
            step.output_variants.len()
        ));
    }

    let outputs = step
        .output_variants
        .keys()
        .map(|output| Ok(output.trim().to_string()))
        .collect::<Result<Vec<_>, String>>()?;

    if step.input_variants.is_empty() {
        return Ok(outputs
            .into_iter()
            .map(|output| ResolvedStepVariantFlow { input: output.clone(), output })
            .collect());
    }

    let inputs = step
        .input_variants
        .iter()
        .map(|input| Ok(input.trim().to_string()))
        .collect::<Result<Vec<_>, String>>()?;

    if inputs.len() == 1 {
        return Ok(outputs
            .into_iter()
            .map(|output| ResolvedStepVariantFlow { input: inputs[0].clone(), output })
            .collect());
    }

    Ok(inputs
        .into_iter()
        .zip(outputs)
        .map(|(input, output)| ResolvedStepVariantFlow { input, output })
        .collect())
}

/// Resolves one option key to a scalar string value when present.
#[must_use]
fn step_option_scalar<'a>(step: &'a MediaStep, key: &str) -> Option<&'a str> {
    match step.options.get(key) {
        Some(TransformInputValue::String(value)) => Some(value.as_str()),
        _ => None,
    }
}

/// Returns true when one option key currently stores a scalar value.
#[must_use]
fn has_step_option_scalar(step: &MediaStep, key: &str) -> bool {
    step_option_scalar(step, key).is_some()
}

/// One hierarchy mapping target for a media variant.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct HierarchyEntry {
    /// Referenced media id in `media` map.
    pub media_id: String,
    /// Logical variant keys for this placement.
    ///
    /// - file hierarchy paths (no trailing `/`) must contain exactly one
    ///   variant,
    /// - directory hierarchy paths (trailing `/`) may contain one or more
    ///   variants and merge their unzipped folder payloads.
    pub variants: Vec<String>,
}

/// Loads `mediapm.ncl` from disk or returns defaults when the file is absent.
pub fn load_mediapm_document(path: &Path) -> Result<MediaPmDocument, MediaPmError> {
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
    reject_legacy_runtime_storage_key(&value)?;
    reject_legacy_runtime_ffmpeg_slot_keys(&value)?;
    reject_legacy_tool_ffmpeg_slot_keys(&value)?;

    let document = versions::decode_mediapm_document_value(value)?;

    validate_media_document(&document)?;

    Ok(document)
}

/// Rejects legacy top-level `runtime_storage` key spelling.
fn reject_legacy_runtime_storage_key(value: &Value) -> Result<(), MediaPmError> {
    if value.as_object().is_some_and(|object| object.contains_key("runtime_storage")) {
        return Err(MediaPmError::Workflow(
            "mediapm.ncl uses legacy key 'runtime_storage'; rename it to 'runtime'".to_string(),
        ));
    }

    Ok(())
}

/// Rejects legacy ffmpeg slot settings under top-level `runtime`.
fn reject_legacy_runtime_ffmpeg_slot_keys(value: &Value) -> Result<(), MediaPmError> {
    let Some(runtime) = value.as_object().and_then(|object| object.get("runtime")) else {
        return Ok(());
    };
    let Some(runtime_object) = runtime.as_object() else {
        return Ok(());
    };

    for (legacy_key, replacement_path) in [
        ("ffmpeg_max_input_slots", "tools.ffmpeg.max_input_slots"),
        ("ffmpeg_max_output_slots", "tools.ffmpeg.max_output_slots"),
    ] {
        if runtime_object.contains_key(legacy_key) {
            return Err(MediaPmError::Workflow(format!(
                "mediapm.ncl runtime.{legacy_key} is no longer supported; move this setting to {replacement_path}"
            )));
        }
    }

    Ok(())
}

/// Rejects legacy ffmpeg slot settings under top-level `tools` rows.
fn reject_legacy_tool_ffmpeg_slot_keys(value: &Value) -> Result<(), MediaPmError> {
    let Some(tools) = value.as_object().and_then(|object| object.get("tools")) else {
        return Ok(());
    };
    let Some(tools_object) = tools.as_object() else {
        return Ok(());
    };

    for (tool_name, tool_value) in tools_object {
        let Some(tool_object) = tool_value.as_object() else {
            continue;
        };

        for (legacy_key, replacement_path) in [
            ("ffmpeg_max_input_slots", "tools.ffmpeg.max_input_slots"),
            ("ffmpeg_max_output_slots", "tools.ffmpeg.max_output_slots"),
        ] {
            if tool_object.contains_key(legacy_key) {
                return Err(MediaPmError::Workflow(format!(
                    "mediapm.ncl tools.{tool_name}.{legacy_key} is no longer supported; use {replacement_path}"
                )));
            }
        }
    }

    Ok(())
}

/// Validates media-source schema invariants that require cross-field checks.
fn validate_media_document(document: &MediaPmDocument) -> Result<(), MediaPmError> {
    validate_tool_requirements(document)?;

    for (media_id, source) in &document.media {
        validate_media_source(media_id, source)?;
    }

    validate_hierarchy_entries(document)?;
    Ok(())
}

/// Metadata describing one resolved producer for hierarchy-policy validation.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum VariantProducerValidationMeta {
    /// Variant resolves to pre-seeded local CAS hash content.
    LocalHash,
    /// Variant resolves to one step output with explicit persistence policy.
    StepOutput {
        /// Whether this output kind captures ZIP-encoded folder payload.
        is_folder_output: bool,
        /// Whether output persistence enables save policy.
        save: bool,
        /// Whether output persistence enables full-payload retention policy.
        save_full: bool,
    },
}

/// Returns whether one decoded output variant maps to a folder capture payload.
#[must_use]
fn decoded_output_variant_is_folder_capture(decoded: &DecodedOutputVariantConfig) -> bool {
    match decoded {
        DecodedOutputVariantConfig::Generic(config) => {
            let kind = config.kind.trim();
            kind == "sandbox_artifacts" || kind.ends_with("_artifacts")
        }
        DecodedOutputVariantConfig::YtDlp(config) => !matches!(
            config.kind,
            YtDlpOutputKind::Primary | YtDlpOutputKind::Description | YtDlpOutputKind::Infojson
        ),
    }
}

/// Collects latest producer metadata for every variant defined by one source.
fn collect_variant_producer_validation_meta(
    media_id: &str,
    source: &MediaSourceSpec,
) -> Result<BTreeMap<String, VariantProducerValidationMeta>, MediaPmError> {
    let mut producers = BTreeMap::new();

    for variant in source.variant_hashes.keys() {
        producers.insert(variant.clone(), VariantProducerValidationMeta::LocalHash);
    }

    for (step_index, step) in source.steps.iter().enumerate() {
        for (variant_key, value) in &step.output_variants {
            let decoded =
                decode_output_variant_config(step.tool, variant_key, value).map_err(|reason| {
                    MediaPmError::Workflow(format!(
                        "media '{media_id}' step #{step_index} {reason}"
                    ))
                })?;
            let policy =
                decode_output_variant_policy(step.tool, variant_key, value).map_err(|reason| {
                    MediaPmError::Workflow(format!(
                        "media '{media_id}' step #{step_index} {reason}"
                    ))
                })?;

            producers.insert(
                variant_key.clone(),
                VariantProducerValidationMeta::StepOutput {
                    is_folder_output: decoded_output_variant_is_folder_capture(&decoded),
                    save: policy.save,
                    save_full: policy.save_full,
                },
            );
        }
    }

    Ok(producers)
}

/// Validates hierarchy entry invariants, including persistence-policy
/// guarantees for referenced workflow-produced variants.
///
/// Policy summary:
/// - all hierarchy-referenced step outputs must keep `save = true`,
/// - hierarchy file paths must reference file variants with
///   `save_full = true`,
/// - hierarchy directory paths must reference folder variants and may keep
///   default `save_full = false`.
fn validate_hierarchy_entries(document: &MediaPmDocument) -> Result<(), MediaPmError> {
    for (hierarchy_path, entry) in &document.hierarchy {
        if entry.media_id.trim().is_empty() {
            return Err(MediaPmError::Workflow(format!(
                "hierarchy path '{hierarchy_path}' has empty media_id"
            )));
        }

        let source = document.media.get(&entry.media_id).ok_or_else(|| {
            MediaPmError::Workflow(format!(
                "hierarchy path '{hierarchy_path}' references unknown media '{}'",
                entry.media_id
            ))
        })?;

        let metadata_placeholders =
            hierarchy_metadata_placeholder_keys(hierarchy_path).map_err(|reason| {
                MediaPmError::Workflow(format!(
                    "hierarchy path '{hierarchy_path}' has invalid metadata placeholder syntax: {reason}"
                ))
            })?;

        for metadata_key in metadata_placeholders {
            if source
                .metadata
                .as_ref()
                .and_then(|metadata| metadata.get(metadata_key.as_str()))
                .is_none()
            {
                return Err(MediaPmError::Workflow(format!(
                    "hierarchy path '{hierarchy_path}' references undefined metadata key '{metadata_key}' for media '{}'",
                    entry.media_id
                )));
            }
        }

        if entry.variants.is_empty() {
            return Err(MediaPmError::Workflow(format!(
                "hierarchy path '{hierarchy_path}' must define at least one variant"
            )));
        }

        let is_directory_target = hierarchy_path.ends_with('/');
        if !is_directory_target && entry.variants.len() != 1 {
            return Err(MediaPmError::Workflow(format!(
                "hierarchy file path '{hierarchy_path}' must define exactly one variant"
            )));
        }

        let producers = collect_variant_producer_validation_meta(&entry.media_id, source)?;

        for requested_variant in &entry.variants {
            let normalized_variant = requested_variant.trim();
            if normalized_variant.is_empty() {
                return Err(MediaPmError::Workflow(format!(
                    "hierarchy path '{hierarchy_path}' contains an empty variant name"
                )));
            }

            let (resolved_variant, producer) = if let Some(producer) =
                producers.get(normalized_variant)
            {
                (normalized_variant, producer)
            } else if let Some(default_producer) = producers.get("default") {
                ("default", default_producer)
            } else {
                return Err(MediaPmError::Workflow(format!(
                    "hierarchy path '{hierarchy_path}' references unknown variant '{normalized_variant}' for media '{}'",
                    entry.media_id
                )));
            };

            if is_directory_target
                && matches!(
                    producer,
                    VariantProducerValidationMeta::StepOutput { is_folder_output: false, .. }
                )
            {
                return Err(MediaPmError::Workflow(format!(
                    "hierarchy directory path '{hierarchy_path}' requires folder variants, but variant '{normalized_variant}' (resolved as '{resolved_variant}') for media '{}' is not a folder output",
                    entry.media_id
                )));
            }

            if !is_directory_target
                && matches!(
                    producer,
                    VariantProducerValidationMeta::StepOutput { is_folder_output: true, .. }
                )
            {
                return Err(MediaPmError::Workflow(format!(
                    "hierarchy file path '{hierarchy_path}' requires file variants, but variant '{normalized_variant}' (resolved as '{resolved_variant}') for media '{}' is a folder output",
                    entry.media_id
                )));
            }

            if let VariantProducerValidationMeta::StepOutput { is_folder_output, save, save_full } =
                *producer
            {
                if !save {
                    return Err(MediaPmError::Workflow(format!(
                        "hierarchy path '{hierarchy_path}' requires variant '{normalized_variant}' (resolved as '{resolved_variant}') for media '{}' to have save=true on its latest producer step",
                        entry.media_id
                    )));
                }

                if !is_folder_output && !save_full {
                    return Err(MediaPmError::Workflow(format!(
                        "hierarchy file path '{hierarchy_path}' requires file variant '{normalized_variant}' (resolved as '{resolved_variant}') for media '{}' to have save_full=true on its latest producer step",
                        entry.media_id
                    )));
                }
            }
        }
    }

    Ok(())
}

/// Validates desired tool requirement selector invariants.
fn validate_tool_requirements(document: &MediaPmDocument) -> Result<(), MediaPmError> {
    for (tool_name, requirement) in &document.tools {
        let version = requirement.normalized_version();
        let tag = requirement.normalized_tag();

        if version.is_none() && tag.is_none() {
            return Err(MediaPmError::Workflow(format!(
                "tool '{tool_name}' must define at least one selector: version or tag"
            )));
        }

        if let (Some(version), Some(tag)) = (&version, &tag)
            && normalize_selector_compare_value(version) != normalize_selector_compare_value(tag)
        {
            return Err(MediaPmError::Workflow(format!(
                "tool '{tool_name}' defines mismatched version '{version}' and tag '{tag}'; when both are provided they must refer to the same release selector"
            )));
        }

        if tool_name.eq_ignore_ascii_case("ffmpeg") {
            if requirement.max_input_slots_or_default() == 0 {
                return Err(MediaPmError::Workflow(format!(
                    "tools.ffmpeg.max_input_slots must be at least 1 (default {})",
                    DEFAULT_FFMPEG_MAX_INPUT_SLOTS,
                )));
            }

            if requirement.max_output_slots_or_default() == 0 {
                return Err(MediaPmError::Workflow(format!(
                    "tools.ffmpeg.max_output_slots must be at least 1 (default {})",
                    DEFAULT_FFMPEG_MAX_OUTPUT_SLOTS,
                )));
            }
        } else if requirement.max_input_slots.is_some() || requirement.max_output_slots.is_some() {
            return Err(MediaPmError::Workflow(format!(
                "tool '{tool_name}' must not define ffmpeg slot settings; only tools.ffmpeg.max_input_slots and tools.ffmpeg.max_output_slots are supported"
            )));
        }
    }

    Ok(())
}

/// Validates one media source entry.
fn validate_media_source(media_id: &str, source: &MediaSourceSpec) -> Result<(), MediaPmError> {
    if source.steps.is_empty() && source.variant_hashes.is_empty() {
        return Err(MediaPmError::Workflow(format!(
            "media '{media_id}' must define at least one step or at least one variant_hashes entry"
        )));
    }

    if let Some(workflow_id) = source.workflow_id.as_deref()
        && workflow_id.trim().is_empty()
    {
        return Err(MediaPmError::Workflow(format!(
            "media '{media_id}' defines an empty workflow_id override"
        )));
    }

    let mut available_variants = source
        .variant_hashes
        .keys()
        .map(ToString::to_string)
        .collect::<std::collections::BTreeSet<_>>();

    for (variant, hash) in &source.variant_hashes {
        if variant.trim().is_empty() {
            return Err(MediaPmError::Workflow(format!(
                "media '{media_id}' has an empty variant name in variant_hashes"
            )));
        }
        if hash.trim().is_empty() {
            return Err(MediaPmError::Workflow(format!(
                "media '{media_id}' variant '{variant}' has an empty CAS hash pointer"
            )));
        }
    }

    for (index, step) in source.steps.iter().enumerate() {
        let flow = resolve_step_variant_flow(step).map_err(|reason| {
            MediaPmError::Workflow(format!("media '{media_id}' step #{index} {reason}"))
        })?;

        validate_step_output_variant_configs(media_id, index, step)?;

        for key in step.options.keys() {
            if !is_allowed_step_option(step.tool, key) {
                return Err(MediaPmError::Workflow(format!(
                    "media '{media_id}' step #{index} uses unsupported option '{key}' for tool '{}'",
                    step.tool.as_str()
                )));
            }
        }

        if step.tool.is_online_media_downloader() {
            let uri = step_option_scalar(step, "uri").ok_or_else(|| {
                MediaPmError::Workflow(format!(
                    "media '{media_id}' step #{index} uses tool '{}' and must define options.uri",
                    step.tool.as_str()
                ))
            })?;

            let uri = Url::parse(uri).map_err(|err| {
                MediaPmError::Workflow(format!(
                    "media '{media_id}' step #{index} has invalid options.uri '{uri}': {err}"
                ))
            })?;
            if !matches!(uri.scheme(), "http" | "https") {
                return Err(MediaPmError::Workflow(format!(
                    "media '{media_id}' step #{index} options.uri must use http/https, observed '{}'",
                    uri.scheme()
                )));
            }
        } else if matches!(step.tool, MediaStepTool::Import | MediaStepTool::ImportOnce) {
            let kind = step_option_scalar(step, "kind").ok_or_else(|| {
                MediaPmError::Workflow(format!(
                    "media '{media_id}' step #{index} uses tool '{}' and must define options.kind",
                    step.tool.as_str()
                ))
            })?;

            if kind != "cas_hash" {
                return Err(MediaPmError::Workflow(format!(
                    "media '{media_id}' step #{index} options.kind must be 'cas_hash' for tool '{}', observed '{kind}'",
                    step.tool.as_str()
                )));
            }

            let hash_text = step_option_scalar(step, "hash").ok_or_else(|| {
                MediaPmError::Workflow(format!(
                    "media '{media_id}' step #{index} uses tool '{}' and must define options.hash",
                    step.tool.as_str()
                ))
            })?;
            Hash::from_str(hash_text).map_err(|_| {
                MediaPmError::Workflow(format!(
                    "media '{media_id}' step #{index} has invalid options.hash '{hash_text}'"
                ))
            })?;
        } else if has_step_option_scalar(step, "uri") {
            return Err(MediaPmError::Workflow(format!(
                "media '{media_id}' step #{index} uses tool '{}' and must not define options.uri",
                step.tool.as_str()
            )));
        }

        if matches!(step.tool, MediaStepTool::Ffmpeg) {
            for input_variant in &step.input_variants {
                if !available_variants.contains(input_variant.trim()) {
                    return Err(MediaPmError::Workflow(format!(
                        "media '{media_id}' step #{index} references unknown input variant '{input_variant}'"
                    )));
                }
            }
        }

        for mapping in &flow {
            if !step.tool.is_source_ingest_tool() && !available_variants.contains(&mapping.input) {
                return Err(MediaPmError::Workflow(format!(
                    "media '{media_id}' step #{index} references unknown input variant '{}'",
                    mapping.input
                )));
            }

            available_variants.insert(mapping.output.clone());
        }

        for (key, value) in &step.options {
            if key.trim().is_empty() {
                return Err(MediaPmError::Workflow(format!(
                    "media '{media_id}' step #{index} has an empty options key"
                )));
            }

            match value {
                TransformInputValue::String(text) => {
                    let _ = text;
                }
                TransformInputValue::StringList(items) => {
                    if !step_option_accepts_list_value(step.tool, key) {
                        return Err(MediaPmError::Workflow(format!(
                            "media '{media_id}' step #{index} options['{key}'] must be a string; list values are only supported for 'option_args', 'leading_args', and 'trailing_args'"
                        )));
                    }
                    if items.iter().any(|item| item.trim().is_empty()) {
                        return Err(MediaPmError::Workflow(format!(
                            "media '{media_id}' step #{index} options['{key}'] contains an empty list item"
                        )));
                    }
                }
            }
        }
    }

    validate_media_metadata_entries(media_id, source)?;

    Ok(())
}

/// Validates strict media-metadata entry semantics for one source.
fn validate_media_metadata_entries(
    media_id: &str,
    source: &MediaSourceSpec,
) -> Result<(), MediaPmError> {
    let Some(metadata) = source.metadata.as_ref() else {
        return Ok(());
    };

    let producers = collect_variant_producer_validation_meta(media_id, source)?;

    for (metadata_name, metadata_value) in metadata {
        if metadata_name.trim().is_empty() {
            return Err(MediaPmError::Workflow(format!(
                "media '{media_id}' defines an empty metadata key"
            )));
        }

        if let MediaMetadataValue::Variant(binding) = metadata_value {
            let variant_name = binding.variant.trim();
            if variant_name.is_empty() {
                return Err(MediaPmError::Workflow(format!(
                    "media '{media_id}' metadata '{metadata_name}' must define a non-empty variant"
                )));
            }

            let metadata_key = binding.metadata_key.trim();
            if metadata_key.is_empty() {
                return Err(MediaPmError::Workflow(format!(
                    "media '{media_id}' metadata '{metadata_name}' must define a non-empty metadata_key"
                )));
            }

            let producer = producers.get(variant_name).ok_or_else(|| {
                MediaPmError::Workflow(format!(
                    "media '{media_id}' metadata '{metadata_name}' references unknown variant '{variant_name}'"
                ))
            })?;

            if matches!(
                producer,
                VariantProducerValidationMeta::StepOutput { is_folder_output: true, .. }
            ) {
                return Err(MediaPmError::Workflow(format!(
                    "media '{media_id}' metadata '{metadata_name}' references variant '{variant_name}' that resolves to a folder output; metadata bindings require file variants"
                )));
            }
        }
    }

    Ok(())
}

/// Parses `${media.metadata.<key>}` placeholders from one hierarchy key.
///
/// Returns each referenced metadata key in first-seen order.
pub(crate) fn hierarchy_metadata_placeholder_keys(
    hierarchy_path: &str,
) -> Result<Vec<String>, String> {
    let mut keys = Vec::new();
    let mut cursor = 0usize;

    while let Some(relative_start) = hierarchy_path[cursor..].find("${") {
        let placeholder_start = cursor + relative_start;
        let after_marker = &hierarchy_path[placeholder_start + 2..];
        let Some(relative_end) = after_marker.find('}') else {
            return Err("missing closing '}' for placeholder".to_string());
        };

        let expression = &after_marker[..relative_end];
        let metadata_key = expression
            .strip_prefix("media.metadata.")
            .ok_or_else(|| {
                format!(
                    "unsupported placeholder '${{{expression}}}'; only '${{media.metadata.<key>}}' is supported"
                )
            })?
            .trim();

        if metadata_key.is_empty() {
            return Err(format!(
                "placeholder '${{{expression}}}' must reference a non-empty metadata key"
            ));
        }

        keys.push(metadata_key.to_string());
        cursor = placeholder_start + 2 + relative_end + 1;
    }

    Ok(keys)
}

/// Validates tool-specific output-variant configuration object schemas.
fn validate_step_output_variant_configs(
    media_id: &str,
    step_index: usize,
    step: &MediaStep,
) -> Result<(), MediaPmError> {
    for (key, value) in &step.output_variants {
        let normalized_key = key.trim();
        let decoded =
            decode_output_variant_config(step.tool, normalized_key, value).map_err(|reason| {
                MediaPmError::Workflow(format!("media '{media_id}' step #{step_index} {reason}"))
            })?;

        if matches!(step.tool, MediaStepTool::Ffmpeg)
            && matches!(decoded, DecodedOutputVariantConfig::Generic(ref config) if config.kind != "output_content")
        {
            return Err(MediaPmError::Workflow(format!(
                "media '{media_id}' step #{step_index} ffmpeg output variant '{normalized_key}' must use kind 'output_content'"
            )));
        }
    }

    Ok(())
}

/// Returns whether one step option key supports list-form values.
#[must_use]
fn step_option_accepts_list_value(_tool: MediaStepTool, key: &str) -> bool {
    matches!(key, "option_args" | "leading_args" | "trailing_args")
}

/// Returns whether one step option key is supported for the given tool.
#[must_use]
fn is_allowed_step_option(tool: MediaStepTool, key: &str) -> bool {
    match tool {
        MediaStepTool::YtDlp => matches!(
            key,
            "uri"
                | "leading_args"
                | "trailing_args"
                | "option_args"
                | "format"
                | "format_sort"
                | "extract_audio"
                | "audio_format"
                | "audio_quality"
                | "remux_video"
                | "recode_video"
                | "convert_subs"
                | "convert_thumbnails"
                | "merge_output_format"
                | "embed_thumbnail"
                | "embed_metadata"
                | "embed_subs"
                | "embed_chapters"
                | "embed_info_json"
                | "write_subs"
                | "write_auto_subs"
                | "sub_langs"
                | "sub_format"
                | "write_thumbnail"
                | "write_all_thumbnails"
                | "write_info_json"
                | "write_comments"
                | "write_description"
                | "write_link"
                | "split_chapters"
                | "playlist_items"
                | "no_playlist"
                | "skip_download"
                | "retries"
                | "limit_rate"
                | "concurrent_fragments"
                | "proxy"
                | "socket_timeout"
                | "user_agent"
                | "referer"
                | "add_header"
                | "cookies"
                | "cookies_from_browser"
                | "ffmpeg_location"
                | "paths"
                | "output"
                | "parse_metadata"
                | "replace_in_metadata"
                | "download_sections"
                | "postprocessor_args"
                | "extractor_args"
                | "http_chunk_size"
                | "download_archive"
                | "sponsorblock_mark"
                | "sponsorblock_remove"
        ),
        MediaStepTool::Import | MediaStepTool::ImportOnce => matches!(key, "kind" | "hash"),
        MediaStepTool::Ffmpeg => matches!(
            key,
            "leading_args"
                | "trailing_args"
                // common options
                | "option_args"
                | "audio_codec"
                | "video_codec"
                | "container"
                | "audio_bitrate"
                | "video_bitrate"
                | "audio_quality"
                | "video_quality"
                | "crf"
                | "preset"
                | "threads"
                | "log_level"
                | "progress"
                // less-common but useful options
                | "tune"
                | "profile"
                | "level"
                | "pixel_format"
                | "frame_rate"
                | "sample_rate"
                | "channels"
                | "audio_filters"
                | "video_filters"
                | "filter_complex"
                | "start_time"
                | "duration"
                | "to"
                | "movflags"
                | "map_metadata"
                | "map"
                | "map_channel"
                | "copy_ts"
                | "start_at_zero"
                | "stats"
                | "no_overwrite"
                | "codec_copy"
                | "faststart"
                | "hwaccel"
                | "sample_format"
                | "channel_layout"
                | "metadata"
                | "timestamp"
                | "disposition"
                | "fps_mode"
                | "force_key_frames"
                | "aspect"
                | "stream_loop"
                | "max_muxing_queue_size"
                | "strict"
                | "maxrate"
                | "bufsize"
                | "bitstream_filter"
                | "shortest"
                | "vn"
                | "an"
                | "sn"
                | "dn"
                | "id3v2_version"
        ),
        MediaStepTool::Rsgain => matches!(
            key,
            "leading_args"
                | "trailing_args"
                | "option_args"
                | "mode"
                | "album"
                | "album_aes77"
                | "skip_existing"
                | "tagmode"
                | "loudness"
                | "target_lufs"
                | "clip_mode"
                | "true_peak"
                | "dual_mono"
                | "album_mode"
                | "max_peak"
                | "lowercase"
                | "id3v2_version"
                | "opus_mode"
                | "jobs"
                | "multithread"
                | "preset"
                | "dry_run"
                | "output"
                | "quiet"
                | "skip_tags"
                | "preserve_mtime"
                | "preserve_mtimes"
        ),
        MediaStepTool::MediaTagger => matches!(
            key,
            "leading_args"
                | "trailing_args"
                | "option_args"
                | "acoustid_endpoint"
                | "musicbrainz_endpoint"
                | "strict_identification"
                | "recording_mbid"
                | "release_mbid"
                | "ffmpeg_version"
                | "ffmpeg_bin"
                | "output_container"
        ),
    }
}

/// Returns one source URI string for diagnostics/materialization bookkeeping.
#[must_use]
pub(crate) fn media_source_uri(media_id: &str, source: &MediaSourceSpec) -> String {
    source
        .steps
        .iter()
        .find_map(|step| {
            if step.tool.is_online_media_downloader() {
                step_option_scalar(step, "uri").map(ToString::to_string)
            } else {
                None
            }
        })
        .unwrap_or_else(|| format!("local:{media_id}"))
}

/// Saves `mediapm.ncl` to disk using deterministic Nickel rendering.
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

/// Returns true when `mediapm.ncl` is present and non-empty.
pub fn mediapm_document_exists(path: &Path) -> bool {
    path.exists() && fs::metadata(path).map(|meta| meta.len() > 0).unwrap_or(false)
}

/// Creates a temporary Nickel workspace that is cleaned up on drop.
#[derive(Debug)]
struct TempNickelWorkspace {
    /// Temporary workspace root.
    path: PathBuf,
}

impl TempNickelWorkspace {
    /// Allocates one unique temporary Nickel workspace directory.
    fn new() -> Result<Self, MediaPmError> {
        let pid = std::process::id();
        let nanos = SystemTime::now().duration_since(UNIX_EPOCH).unwrap_or_default().as_nanos();
        let path = std::env::temp_dir().join(format!("mediapm-nickel-{pid}-{nanos}"));

        fs::create_dir_all(&path).map_err(|source| MediaPmError::Io {
            operation: "creating temporary Nickel workspace".to_string(),
            path: path.clone(),
            source,
        })?;

        Ok(Self { path })
    }
}

impl Drop for TempNickelWorkspace {
    fn drop(&mut self) {
        let _ = fs::remove_dir_all(&self.path);
    }
}

/// Evaluates one Nickel source string into exported JSON value.
fn evaluate_nickel_source_to_json(path: &Path, source: &str) -> Result<Value, MediaPmError> {
    let workspace = TempNickelWorkspace::new()?;
    let source_path = workspace.path.join("mediapm.ncl");

    fs::write(&source_path, source).map_err(|source_err| MediaPmError::Io {
        operation: "writing temporary mediapm.ncl source".to_string(),
        path: source_path.clone(),
        source: source_err,
    })?;

    let mut program = Program::<CacheImpl>::new_from_file(
        source_path.as_os_str(),
        std::io::sink(),
        NullReporter {},
    )
    .map_err(|source_err| MediaPmError::Io {
        operation: "constructing Nickel program".to_string(),
        path: path.to_path_buf(),
        source: source_err,
    })?;

    let exported = program.eval_full_for_export().map_err(|err| {
        MediaPmError::Workflow(format!(
            "evaluating mediapm.ncl: {}",
            render_nickel_error(&mut program, err)
        ))
    })?;

    Value::deserialize(exported).map_err(|err| {
        MediaPmError::Serialization(format!("deserializing exported Nickel value: {err}"))
    })
}

/// Renders one Nickel interpreter error as user-facing text.
fn render_nickel_error(program: &mut Program<CacheImpl>, err: NickelError) -> String {
    nickel_lang_core::error::report::report_as_str(
        &mut program.files(),
        err,
        nickel_lang_core::error::report::ColorOpt::Never,
    )
}

/// Renders a field name in Nickel record syntax.
fn render_field_name(name: &str) -> String {
    if is_bare_identifier(name) {
        name.to_string()
    } else {
        serde_json::to_string(name).unwrap_or_else(|_| format!("\"{name}\""))
    }
}

/// Returns true when one record key can be emitted as a bare Nickel identifier.
fn is_bare_identifier(input: &str) -> bool {
    let mut chars = input.chars().peekable();

    while matches!(chars.peek(), Some('_')) {
        let _ = chars.next();
    }

    let Some(head) = chars.next() else {
        return false;
    };

    if !head.is_ascii_alphabetic() {
        return false;
    }

    chars.all(|ch| ch.is_ascii_alphanumeric() || matches!(ch, '_' | '-' | '\''))
}

/// Renders JSON as deterministic Nickel source with sorted object keys.
fn render_nickel_value(value: &Value, indent: usize) -> String {
    let pad = " ".repeat(indent);
    let next_pad = " ".repeat(indent + 2);

    match value {
        Value::Null => "null".to_string(),
        Value::Bool(flag) => flag.to_string(),
        Value::Number(number) => number.to_string(),
        Value::String(text) => serde_json::to_string(text).unwrap_or_else(|_| "\"\"".to_string()),
        Value::Array(items) => {
            if items.is_empty() {
                "[]".to_string()
            } else {
                let body = items
                    .iter()
                    .map(|item| format!("{next_pad}{},", render_nickel_value(item, indent + 2)))
                    .collect::<Vec<_>>()
                    .join("\n");
                format!("[\n{body}\n{pad}]")
            }
        }
        Value::Object(entries) => {
            if entries.is_empty() {
                "{}".to_string()
            } else {
                let mut ordered = entries.iter().collect::<Vec<_>>();
                ordered.sort_by(|(left, _), (right, _)| left.cmp(right));
                let body = ordered
                    .into_iter()
                    .map(|(key, item)| {
                        format!(
                            "{next_pad}{} = {},",
                            render_field_name(key),
                            render_nickel_value(item, indent + 2)
                        )
                    })
                    .collect::<Vec<_>>()
                    .join("\n");
                format!("{{\n{body}\n{pad}}}")
            }
        }
    }
}

/// Normalizes `version` field numbers exported by Nickel into integer JSON numbers.
fn normalize_version_field_to_u64(
    value: &mut Value,
    document_name: &str,
) -> Result<(), MediaPmError> {
    let Some(object) = value.as_object_mut() else {
        return Err(MediaPmError::Workflow(format!(
            "{document_name} must evaluate to a top-level record"
        )));
    };

    let Some(version_value) = object.get("version").cloned() else {
        return Ok(());
    };

    let normalized = if let Some(raw) = version_value.as_u64() {
        raw
    } else if let Some(raw) = version_value.as_f64() {
        if !raw.is_finite() || raw.fract() != 0.0 || raw < 0.0 {
            return Err(MediaPmError::Workflow(format!(
                "{document_name} version must be a non-negative integer"
            )));
        }
        raw as u64
    } else {
        return Err(MediaPmError::Workflow(format!("{document_name} version must be numeric")));
    };

    object.insert("version".to_string(), Value::from(normalized));
    Ok(())
}

#[cfg(test)]
mod tests {
    use std::collections::BTreeMap;

    use super::{
        MEDIAPM_DOCUMENT_VERSION, MediaMetadataValue, MediaPmDocument, MediaRuntimeStorage,
        MediaSourceSpec, MediaStep, MediaStepTool, TransformInputValue, Value,
        load_mediapm_document, media_source_uri, resolve_step_variant_flow, save_mediapm_document,
    };

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
                        "yt-dlp" = { tag = "v2026.04.01", recheck_seconds = 3600 },
            rsgain = { version = "3.7.0", tag = "v3.7.0" },
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
        assert_eq!(document.tools["rsgain"].version.as_deref(), Some("3.7.0"));
        assert_eq!(document.tools["rsgain"].tag.as_deref(), Some("v3.7.0"));
    }

    /// Protects runtime-storage decode for shared user-cache policy toggle.
    #[test]
    fn runtime_storage_decodes_use_user_download_cache_toggle() {
        let root = tempfile::tempdir().expect("tempdir");
        let path = root.path().join("mediapm.ncl");
        let source = r#"
{
    version = 1,
    runtime = {
        use_user_download_cache = false,
    },
}
"#;

        std::fs::write(&path, source).expect("write source");
        let document = load_mediapm_document(&path).expect("decode document");

        assert_eq!(document.runtime.use_user_download_cache, Some(false));
        assert!(!document.runtime.use_user_download_cache_enabled());
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

    /// Protects default cache policy when runtime-storage toggle is omitted.
    #[test]
    fn runtime_storage_defaults_to_enabled_shared_download_cache() {
        let runtime_storage = MediaRuntimeStorage::default();
        assert!(runtime_storage.use_user_download_cache_enabled());
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
    /// ffmpeg slot settings under `runtime`.
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
        assert!(err.to_string().contains("runtime.ffmpeg_max_input_slots"));
    }

    /// Protects renamed runtime key policy by rejecting legacy key spelling.
    #[test]
    fn runtime_storage_key_is_rejected_after_runtime_rename() {
        let root = tempfile::tempdir().expect("tempdir");
        let path = root.path().join("mediapm.ncl");
        let source = r#"
{
    version = 1,
    runtime_storage = {
        use_user_download_cache = false,
    },
}
"#;

        std::fs::write(&path, source).expect("write source");
        let err = load_mediapm_document(&path).expect_err("legacy runtime_storage key must fail");
        assert!(err.to_string().contains("legacy key 'runtime_storage'"));
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
                            kind = "subtitle",
                            save = true,
                            save_full = true,
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
        let source = r#"
{
  version = 1,
  tools = {
      ffmpeg = {},
  },
}
"#;

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
                    output_variants = { default = { kind = "primary", save = true, save_full = true } },
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
                    output_variants = { default = { kind = "primary", save = true, save_full = true } },
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
            document.media["demo"].steps[0].options.get("write_description").and_then(|value| {
                match value {
                    TransformInputValue::String(value) => Some(value.as_str()),
                    TransformInputValue::StringList(_) => None,
                }
            }),
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
                    output_variants = { default = { kind = "output_content", save = true, save_full = true, idx = 0 } },
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
                    output_variants = { downloaded = { kind = "primary", save = true, save_full = true } },
                    options = {
                        uri = "https://example.com/video",
                        merge_output_format = "mkv",
                        format_sort = "res,codec",
                        playlist_items = "1:3",
                        skip_download = "true",
                    },
                },
                {
                    tool = "ffmpeg",
                    input_variants = ["downloaded"],
                    output_variants = { normalized = { kind = "output_content", save = true, save_full = true, idx = 0 } },
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
                    output_variants = { gained = { kind = "output_content", save = true, save_full = true } },
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
                    output_variants = { tagged = { kind = "output_content", save = true, save_full = true } },
                    options = {
                        strict_identification = "false",
                        musicbrainz_endpoint = "https://musicbrainz.org/ws/2",
                        ffmpeg_bin = "ffmpeg",
                        output_container = "mp4",
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
                    output_variants = { normalized = { kind = "output_content", save = true, save_full = true, idx = 0 } },
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
        assert!(err.to_string().contains("options['audio_quality'] must be a string"));
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
                    output_variants = { normalized = "output_content" },
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

    /// Protects per-step variant-flow decoding and list-option decoding.
    #[test]
    fn media_step_supports_variant_flow_and_list_options() {
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
                    output_variants = { aac = { kind = "output_content", save = true, save_full = true, idx = 0 } },
                    options = {
                        option_args = "-vn",
                        leading_args = ["-hide_banner"],
                        trailing_args = ["-c:a", "aac"],
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
                    output_variants = { "subtitles/en/srt" = { kind = "primary", save = true, save_full = true } },
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

    /// Protects backward compatibility by accepting legacy yt-dlp
    /// `*_artifacts` kind names through decode aliases.
    #[test]
    fn yt_dlp_legacy_artifact_kind_aliases_still_decode() {
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
                        "subtitles/" = { kind = "subtitle_artifacts", save = true, save_full = true, langs = "en" },
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
        assert!(document.media["demo"].steps[0].output_variants.contains_key("subtitles/"));
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
                        "subtitles/" = { kind = "subtitle", save = true, save_full = true },
                        "subtitles/en" = { kind = "auto_subtitle", save = true, save_full = true },
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

    /// Protects yt-dlp value schema by rejecting `langs` on non-subtitle kinds.
    #[test]
    fn yt_dlp_non_subtitle_folder_rejects_langs_and_sub_format() {
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
                        "thumbnails/" = { kind = "thumbnail", save = true, save_full = true, langs = "all" },
                    },
                    options = { uri = "https://example.com/video" },
                },
            ],
        },
    },
}
"#;

        std::fs::write(&path, source).expect("write source");
        let err = load_mediapm_document(&path).expect_err("document should fail validation");
        assert!(err.to_string().contains("only valid for subtitle kinds"));
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
                    output_variants = { downloaded = { kind = "primary", save = true, save_full = true } },
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
                    output_variants = { downloaded = { kind = "primary", save = true, save_full = true } },
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
    fn import_once_step_rejects_non_empty_input_variants() {
        let root = tempfile::tempdir().expect("tempdir");
        let path = root.path().join("mediapm.ncl");
        let source = r#"
{
    version = 1,
    media = {
        demo = {
            steps = [
                {
                    tool = "import-once",
                    input_variants = ["default"],
                    output_variants = { default = { kind = "output_content", save = true, save_full = true } },
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
        let err =
            load_mediapm_document(&path).expect_err("import-once input_variants must be rejected");
        assert!(
            err.to_string()
                .contains("must not define input_variants for source-ingest tool 'import-once'")
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
                    output_variants = { aac = { kind = "output_content", save = true, save_full = true, idx = 0 } },
                },
            ],
        },
    },
}
"#;

        std::fs::write(&path, source).expect("write source");
        let err = load_mediapm_document(&path).expect_err("document should fail validation");
        assert!(err.to_string().contains("unknown input variant 'default'"));
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
                    tool = "import-once",
                    output_variants = { default = { kind = "output_content", save = true, save_full = true } },
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
            description: None,
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
            description: None,
            workflow_id: None,
            metadata: None,
            variant_hashes: BTreeMap::new(),
            steps: vec![MediaStep {
                tool: MediaStepTool::ImportOnce,
                input_variants: Vec::new(),
                output_variants: BTreeMap::from([(
                    "default".to_string(),
                    Value::Object(serde_json::Map::new()),
                )]),
                options: BTreeMap::from([
                    (
                        "kind".to_string(),
                        TransformInputValue::String("cas_hash".to_string()),
                    ),
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
            }
            other => panic!("expected metadata variant binding, got {other:?}"),
        }
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
                            save = true,
                            save_full = true,
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
                            kind = "subtitle",
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

    /// Protects output policy defaults by treating omitted save/save_full as
    /// save=true and save_full=false.
    #[test]
    fn output_variant_policy_defaults_apply_when_save_fields_are_omitted() {
        let yt_dlp = super::decode_output_variant_policy(
            MediaStepTool::YtDlp,
            "video",
            &serde_json::json!({ "kind": "primary" }),
        )
        .expect("decode yt-dlp output policy");
        assert!(yt_dlp.save);
        assert!(!yt_dlp.save_full);

        let ffmpeg = super::decode_output_variant_policy(
            MediaStepTool::Ffmpeg,
            "audio",
            &serde_json::json!({ "kind": "output_content", "idx": 0 }),
        )
        .expect("decode ffmpeg output policy");
        assert!(ffmpeg.save);
        assert!(!ffmpeg.save_full);
    }

    /// Protects hierarchy validation by requiring save_full=true only for
    /// file variants directly materialized to file paths.
    #[test]
    fn hierarchy_file_variant_requires_save_full_true() {
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
    hierarchy = {
        "demo/video.mp4" = {
            media_id = "demo",
            variants = ["video"],
        },
    },
}
"#;

        std::fs::write(&path, source).expect("write source");
        let err = load_mediapm_document(&path)
            .expect_err("hierarchy file variant must require save_full=true");
        assert!(err.to_string().contains("requires file variant"));
        assert!(err.to_string().contains("save_full=true"));
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
                            save_full = true,
                        },
                    },
                    options = {
                        uri = "https://example.com/video",
                    },
                },
            ],
        },
    },
    hierarchy = {
        "library/${media.metadata.title}/demo.mp4" = {
            media_id = "demo",
            variants = ["video"],
        },
    },
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
                            save_full = true,
                        },
                    },
                    options = {
                        uri = "https://example.com/video",
                    },
                },
            ],
        },
    },
    hierarchy = {
        "library/${media.title}/demo.mp4" = {
            media_id = "demo",
            variants = ["video"],
        },
    },
}
"#;

        std::fs::write(&path, source).expect("write source");
        let err = load_mediapm_document(&path)
            .expect_err("unsupported placeholder expressions should be rejected");
        assert!(err.to_string().contains("unsupported placeholder"));
    }

    /// Protects hierarchy validation by allowing folder variants to keep the
    /// default save_full=false policy when materialized to directory paths.
    #[test]
    fn hierarchy_directory_variant_allows_default_save_full_false() {
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
                            kind = "subtitle",
                        },
                    },
                    options = {
                        uri = "https://example.com/video",
                    },
                },
            ],
        },
    },
    hierarchy = {
        "demo/subtitles/" = {
            media_id = "demo",
            variants = ["subtitles"],
        },
    },
}
"#;

        std::fs::write(&path, source).expect("write source");
        let document =
            load_mediapm_document(&path).expect("hierarchy folder variant should be allowed");
        assert!(document.hierarchy.contains_key("demo/subtitles/"));
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
                            kind = "subtitle",
                            save_full = true,
                        },
                    },
                    options = {
                        uri = "https://example.com/video",
                    },
                },
            ],
        },
    },
    hierarchy = {
        "demo/subtitles.txt" = {
            media_id = "demo",
            variants = ["subtitles"],
        },
    },
}
"#;

        std::fs::write(&path, source).expect("write source");
        let err = load_mediapm_document(&path)
            .expect_err("file paths must reject folder output variants");
        assert!(err.to_string().contains("requires file variants"));
    }
}
