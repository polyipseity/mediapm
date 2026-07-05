//! Media source, step, tool, and metadata types for mediapm configuration.
//!
//! These types model the `media.<id>` entries in `mediapm.ncl` plus the
//! per-step tool taxonomy.

use std::collections::BTreeMap;

use serde_json::Value;

use super::hierarchy_types::{deserialize_variant_selector_list, serialize_variant_selector_list};

// ---------------------------------------------------------------------------
// Metadata value types
// ---------------------------------------------------------------------------

/// One media metadata value source declared under `media.<id>.metadata`.
///
/// Supports three forms:
/// - `"text"` literal values,
/// - object bindings that extract one key from one produced file variant,
/// - ordered fallback lists of literal/object candidates.
#[derive(Debug, Clone, PartialEq, Eq, serde::Serialize, serde::Deserialize)]
#[serde(untagged)]
pub enum MediaMetadataValue {
    /// Literal metadata text value.
    Literal(String),
    /// Variant-file metadata lookup binding.
    Variant(MediaMetadataVariantBinding),
    /// Ordered fallback candidates evaluated top-to-bottom until one
    /// resolves to a non-empty metadata string.
    Fallback(Vec<MediaMetadataValueCandidate>),
}

impl Default for MediaMetadataValue {
    fn default() -> Self {
        Self::Literal(String::new())
    }
}

/// One metadata value candidate entry used by fallback lists.
#[derive(Debug, Clone, PartialEq, Eq, serde::Serialize, serde::Deserialize)]
#[serde(untagged)]
pub enum MediaMetadataValueCandidate {
    /// Literal fallback metadata text.
    Literal(String),
    /// Variant-file metadata lookup fallback binding.
    Variant(MediaMetadataVariantBinding),
}

/// Variant-file metadata lookup binding for `media.<id>.metadata` values.
#[derive(Debug, Clone, PartialEq, Eq, serde::Serialize, serde::Deserialize)]
#[serde(deny_unknown_fields)]
pub struct MediaMetadataVariantBinding {
    /// Variant key whose produced file bytes should be inspected.
    pub variant: String,
    /// Metadata key to extract from that variant file.
    pub metadata_key: String,
    /// Optional regex transform applied to the extracted metadata string.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub transform: Option<MediaMetadataRegexTransform>,
}

/// Regex-based metadata string transform for variant metadata bindings.
#[derive(Debug, Clone, PartialEq, Eq, serde::Serialize, serde::Deserialize)]
#[serde(deny_unknown_fields)]
pub struct MediaMetadataRegexTransform {
    /// Regex pattern evaluated with full-match semantics.
    pub pattern: String,
    /// Replacement template applied when `pattern` matches.
    pub replacement: String,
}

// ---------------------------------------------------------------------------
// Media source, step, and tool types
// ---------------------------------------------------------------------------

/// Source registry entry for one media item.
#[derive(Debug, Clone, PartialEq, Eq, Default, serde::Serialize, serde::Deserialize)]
#[serde(deny_unknown_fields)]
pub struct MediaSourceSpec {
    /// Legacy media id override (rejected by validation; use hierarchy ids).
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub id: Option<String>,
    /// Human-readable description.
    #[serde(default, skip_serializing_if = "String::is_empty")]
    pub description: String,
    /// Human-readable title.
    #[serde(default, skip_serializing_if = "String::is_empty")]
    pub title: String,
    /// Artist.
    #[serde(default, skip_serializing_if = "String::is_empty")]
    pub artist: String,
    /// Optional explicit conductor workflow id override.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub workflow_id: Option<String>,
    /// Metadata keyed by attribute name.
    #[serde(default, skip_serializing_if = "BTreeMap::is_empty")]
    pub metadata: BTreeMap<String, MediaMetadataValue>,
    /// Optional pre-seeded CAS hash pointers keyed by variant name.
    #[serde(default, skip_serializing_if = "BTreeMap::is_empty")]
    pub variant_hashes: BTreeMap<String, String>,
    /// Ordered media-processing steps.
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub steps: Vec<MediaStep>,
}

/// One ordered media-processing step.
#[derive(Debug, Clone, PartialEq, Eq, serde::Serialize, serde::Deserialize)]
#[serde(deny_unknown_fields)]
pub struct MediaStep {
    /// Tool kind used for this step.
    pub tool: MediaStepTool,
    /// Input variants consumed by this step.
    #[serde(
        default,
        skip_serializing_if = "Vec::is_empty",
        deserialize_with = "deserialize_variant_selector_list",
        serialize_with = "serialize_variant_selector_list"
    )]
    pub input_variants: Vec<String>,
    /// Output variants produced by this step.
    #[serde(default, skip_serializing_if = "BTreeMap::is_empty")]
    pub output_variants: BTreeMap<String, Value>,
    /// Operation-specific option map.
    #[serde(default, skip_serializing_if = "BTreeMap::is_empty")]
    pub options: BTreeMap<String, TransformInputValue>,
}

/// Supported media-step tool kinds.
#[derive(Debug, Clone, Copy, PartialEq, Eq, serde::Serialize, serde::Deserialize)]
#[serde(rename_all = "kebab-case")]
pub enum MediaStepTool {
    /// `yt-dlp` online-media downloader.
    #[serde(rename = "yt-dlp")]
    YtDlp,
    /// `import` builtin source ingestion from existing CAS payload hash.
    Import,
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
        matches!(self, Self::YtDlp | Self::Import)
    }

    /// Returns true when the given tool name identifies a builtin
    /// source-ingest step that is never downloader-provisioned.
    #[must_use]
    pub fn is_builtin_source_ingest_name(tool_name: &str) -> bool {
        tool_name.eq_ignore_ascii_case("import")
    }
}

// ---------------------------------------------------------------------------
// Step option accessors
// ---------------------------------------------------------------------------

/// One transform input-option binding value.
#[derive(Debug, Clone, PartialEq, Eq, serde::Serialize, serde::Deserialize)]
#[serde(untagged)]
pub enum TransformInputValue {
    /// Scalar string input value.
    String(String),
}

/// Resolves one option key to a scalar string value when present.
#[must_use]
#[allow(dead_code)]
pub fn step_option_scalar<'a>(step: &'a MediaStep, key: &str) -> Option<&'a str> {
    match step.options.get(key) {
        Some(TransformInputValue::String(value)) => Some(value.as_str()),
        _ => None,
    }
}

/// Returns true when one option key currently stores a scalar value.
#[must_use]
#[allow(dead_code)]
pub fn has_step_option_scalar(step: &MediaStep, key: &str) -> bool {
    step_option_scalar(step, key).is_some()
}
