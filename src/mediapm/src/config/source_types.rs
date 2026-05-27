//! Media source, step, tool, and metadata types for mediapm configuration.

use std::collections::BTreeMap;

use serde_json::Value;

use super::{deserialize_variant_selector_list, serialize_variant_selector_list};

/// One media metadata value source declared under `media.<id>.metadata`.
///
/// Metadata values are intentionally strict and support three forms:
/// - `"text"` literal values,
/// - object bindings that extract one key from one produced file variant.
/// - ordered fallback lists of literal/object candidates where runtime
///   resolves the first non-empty candidate.
#[derive(Debug, Clone, PartialEq, Eq, serde::Serialize, serde::Deserialize)]
#[serde(untagged)]
pub enum MediaMetadataValue {
    /// Literal metadata text value.
    Literal(String),
    /// Variant-file metadata lookup binding.
    Variant(MediaMetadataVariantBinding),
    /// Ordered fallback candidates evaluated top-to-bottom until one
    /// candidate resolves to a non-empty metadata string.
    Fallback(Vec<MediaMetadataValueCandidate>),
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
    ///
    /// Transform semantics are full-match only: the `pattern` must match the
    /// entire extracted value. When it matches, `replacement` is rendered using
    /// regular regex capture-group substitution (`$0` = entire match;
    /// `$1..$N` = explicit capture groups).
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub transform: Option<MediaMetadataRegexTransform>,
}

/// Regex-based metadata string transform for variant metadata bindings.
#[derive(Debug, Clone, PartialEq, Eq, serde::Serialize, serde::Deserialize)]
#[serde(deny_unknown_fields)]
pub struct MediaMetadataRegexTransform {
    /// Regex pattern evaluated with full-match semantics against extracted
    /// metadata text.
    pub pattern: String,
    /// Replacement template applied when `pattern` matches.
    pub replacement: String,
}

/// Source registry entry for one media item.
#[derive(Debug, Clone, PartialEq, Eq, serde::Serialize, serde::Deserialize)]
#[serde(deny_unknown_fields)]
pub struct MediaSourceSpec {
    /// Legacy media id override field.
    ///
    /// This field is intentionally rejected by runtime validation. Playlist
    /// references must target explicit hierarchy node `id` values instead.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub id: Option<String>,
    /// Optional human-readable description.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub description: Option<String>,
    /// Optional human-readable title used for readability and path templates.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub title: Option<String>,
    /// Optional explicit conductor workflow id override.
    ///
    /// When omitted, `mediapm` maps each media id to exactly one managed
    /// workflow id using the default prefix policy.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub workflow_id: Option<String>,
    /// Optional strict metadata object for media-specific path interpolation.
    ///
    /// Each key maps to one of:
    /// - one literal string value, or
    /// - one `{ variant, metadata_key, transform? }` object that
    ///   resolves metadata from a
    ///   file variant produced by this media source, or
    /// - one ordered list of string/object candidates where runtime picks the
    ///   first non-empty value.
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
    /// Source-ingest tools (`yt-dlp`, `import`) must keep
    /// `input_variants` empty.
    /// Variant outputs flow top-to-bottom across this ordered list.
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
    ///
    /// Source-ingest tools must keep this list empty because they originate
    /// content directly from their own options (for example `options.uri` or
    /// `options.hash`) rather than from prior step outputs.
    ///
    /// Selector entries support both exact-string and regex-object forms:
    /// - `"variant_name"`
    /// - `{ regex = "^subtitles/.+$" }`
    #[serde(
        default,
        skip_serializing_if = "Vec::is_empty",
        deserialize_with = "deserialize_variant_selector_list",
        serialize_with = "serialize_variant_selector_list"
    )]
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
    /// Values are always scalar strings.
    #[serde(default, skip_serializing_if = "BTreeMap::is_empty")]
    pub options: BTreeMap<String, TransformInputValue>,
}

/// Supported media-step tool kinds.
#[derive(Debug, Clone, Copy, PartialEq, Eq, serde::Serialize, serde::Deserialize)]
#[serde(rename_all = "kebab-case")]
pub enum MediaStepTool {
    /// `yt-dlp` online-media downloader.
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
    /// source-ingest step that is never downloader-provisioned and therefore
    /// does not require a release selector (version or tag).
    #[must_use]
    pub fn is_builtin_source_ingest_name(tool_name: &str) -> bool {
        tool_name.eq_ignore_ascii_case("import")
    }
}

/// One transform input-option binding value.
#[derive(Debug, Clone, PartialEq, Eq, serde::Serialize, serde::Deserialize)]
#[serde(untagged)]
pub enum TransformInputValue {
    /// Scalar string input value.
    String(String),
}

/// Resolves one option key to a scalar string value when present.
#[must_use]
pub(super) fn step_option_scalar<'a>(step: &'a MediaStep, key: &str) -> Option<&'a str> {
    match step.options.get(key) {
        Some(TransformInputValue::String(value)) => Some(value.as_str()),
        _ => None,
    }
}

/// Returns true when one option key currently stores a scalar value.
#[must_use]
pub(super) fn has_step_option_scalar(step: &MediaStep, key: &str) -> bool {
    step_option_scalar(step, key).is_some()
}
