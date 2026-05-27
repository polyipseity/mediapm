//! Output variant configuration, save policy, and step variant flow
//! resolution for mediapm.

use std::fmt;

use serde::de::{self, Visitor};
use serde::{Deserialize, Deserializer, Serialize, Serializer};
use serde_json::Value;

use super::{MediaStep, MediaStepTool, parse_non_negative_integral_u32};

/// Shared optional per-variant persistence-policy settings.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Deserialize)]
#[serde(deny_unknown_fields)]
pub(crate) struct OutputVariantPolicyConfig {
    /// Optional tri-state save-policy override (defaults to `true`).
    #[serde(default = "default_output_variant_save")]
    pub(crate) save: OutputSaveConfig,
}

/// Tri-state output-persistence policy for one output variant.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum OutputSaveConfig {
    /// Boolean save policy (`false` or `true`).
    Bool(bool),
    /// Full-save policy keyword.
    Full,
}

impl Serialize for OutputSaveConfig {
    fn serialize<S>(&self, serializer: S) -> Result<S::Ok, S::Error>
    where
        S: Serializer,
    {
        match self {
            Self::Bool(value) => serializer.serialize_bool(*value),
            Self::Full => serializer.serialize_str("full"),
        }
    }
}

impl<'de> Deserialize<'de> for OutputSaveConfig {
    fn deserialize<D>(deserializer: D) -> Result<Self, D::Error>
    where
        D: Deserializer<'de>,
    {
        struct OutputSaveConfigVisitor;

        impl Visitor<'_> for OutputSaveConfigVisitor {
            type Value = OutputSaveConfig;

            fn expecting(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
                formatter.write_str("a boolean save policy or the string \"full\"")
            }

            fn visit_bool<E>(self, value: bool) -> Result<Self::Value, E>
            where
                E: de::Error,
            {
                Ok(OutputSaveConfig::Bool(value))
            }

            fn visit_str<E>(self, value: &str) -> Result<Self::Value, E>
            where
                E: de::Error,
            {
                if value == "full" {
                    Ok(OutputSaveConfig::Full)
                } else {
                    Err(E::invalid_value(de::Unexpected::Str(value), &self))
                }
            }

            fn visit_string<E>(self, value: String) -> Result<Self::Value, E>
            where
                E: de::Error,
            {
                self.visit_str(&value)
            }
        }

        deserializer.deserialize_any(OutputSaveConfigVisitor)
    }
}

impl OutputSaveConfig {
    /// Returns whether this policy keeps output bytes persisted.
    #[must_use]
    pub const fn should_persist(self) -> bool {
        !matches!(self, Self::Bool(false))
    }
}

impl Default for OutputSaveConfig {
    fn default() -> Self {
        Self::Bool(true)
    }
}

/// Returns the default output-persistence save policy for one variant.
#[must_use]
fn default_output_variant_save() -> OutputSaveConfig {
    OutputSaveConfig::default()
}

/// Generic output-variant configuration for non-yt-dlp tools.
///
/// Output-variant values are always explicit objects with:
/// - required `kind` output capture key,
/// - optional tri-state `save` policy (defaults to `true`),
/// - optional `zip_member` selector,
/// - optional `idx` selector for ffmpeg multi-output routing.
#[derive(Debug, Clone, PartialEq, Eq, Deserialize)]
#[serde(deny_unknown_fields)]
pub(crate) struct GenericOutputVariantConfig {
    /// Explicit generated-tool output kind/capture name bound for this variant.
    pub(crate) kind: String,
    /// Optional tri-state save-policy override (defaults to `true`).
    #[serde(default = "default_output_variant_save")]
    pub(crate) save: OutputSaveConfig,
    /// Optional capture kind override.
    ///
    /// When omitted, mediapm derives file-vs-folder behavior from `kind`
    /// naming conventions (`*_artifacts` remains folder-capture by default).
    #[serde(default)]
    pub(crate) capture_kind: Option<OutputCaptureKind>,
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
    /// Optional output filename extension override.
    ///
    /// This field is supported for `ffmpeg`, `rsgain`, and `media-tagger`
    /// output variants and maps to generated `output_path_<idx>` tool inputs.
    /// Values may be specified with or without a leading dot.
    #[serde(default)]
    pub(crate) extension: Option<String>,
}

impl GenericOutputVariantConfig {
    /// Returns effective file-vs-folder capture kind for this variant.
    #[must_use]
    pub(crate) fn effective_capture_kind(&self) -> OutputCaptureKind {
        match self.capture_kind {
            Some(value) => value,
            None => default_generic_capture_kind_for_kind(self.kind.as_str()),
        }
    }
}

impl From<&GenericOutputVariantConfig> for OutputVariantPolicyConfig {
    fn from(value: &GenericOutputVariantConfig) -> Self {
        Self { save: value.save }
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
        && let Some(normalized) = parse_non_negative_integral_u32(value)
    {
        return Ok(Some(normalized));
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
    #[serde(rename = "sandbox")]
    Sandbox,
    /// Subtitle artifact bundle.
    #[serde(rename = "subtitles")]
    Subtitles,
    /// Thumbnail artifact bundle.
    #[serde(rename = "thumbnails")]
    Thumbnails,
    /// Description sidecar file.
    #[serde(rename = "description")]
    Description,
    /// Annotation sidecar file.
    #[serde(rename = "annotation")]
    Annotation,
    /// Info-JSON sidecar file.
    #[serde(rename = "infojson")]
    Infojson,
    /// Comment-in-infojson semantic output.
    ///
    /// yt-dlp stores comments inside info-json payloads; this kind exists to
    /// enforce comment capture toggles without introducing a dedicated
    /// standalone comment sidecar family.
    #[serde(rename = "comment")]
    Comment,
    /// Link/internet-shortcut artifact bundle.
    #[serde(rename = "links")]
    Links,
    /// Split chapter artifact bundle.
    #[serde(rename = "chapters")]
    Chapters,
    /// Download-archive file output.
    #[serde(rename = "archive")]
    Archive,
    /// Playlist-description artifact bundle.
    #[serde(rename = "playlist_description")]
    PlaylistDescription,
    /// Playlist-infojson artifact bundle.
    #[serde(rename = "playlist_infojson")]
    PlaylistInfojson,
}

/// Per-variant output capture kind.
///
/// This setting controls whether one variant should be treated as a file or
/// folder output by mediapm-side validation/materialization policy.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Deserialize)]
pub(crate) enum OutputCaptureKind {
    /// Prefer file-style capture/materialization semantics.
    #[serde(rename = "file")]
    File,
    /// Prefer folder-style capture/materialization semantics.
    #[serde(rename = "folder")]
    Folder,
}

/// Returns default capture kind for one generic output kind.
#[must_use]
pub(crate) fn default_generic_capture_kind_for_kind(kind: &str) -> OutputCaptureKind {
    let normalized = kind.trim();
    if normalized == "sandbox_artifacts" || normalized.ends_with("_artifacts") {
        OutputCaptureKind::Folder
    } else {
        OutputCaptureKind::File
    }
}

/// Returns default capture kind for one yt-dlp output kind.
#[must_use]
pub(crate) const fn default_yt_dlp_capture_kind_for_kind(
    kind: YtDlpOutputKind,
) -> OutputCaptureKind {
    match kind {
        YtDlpOutputKind::Primary
        | YtDlpOutputKind::Description
        | YtDlpOutputKind::Annotation
        | YtDlpOutputKind::Infojson
        | YtDlpOutputKind::Comment
        | YtDlpOutputKind::Archive
        | YtDlpOutputKind::PlaylistDescription
        | YtDlpOutputKind::PlaylistInfojson => OutputCaptureKind::File,
        YtDlpOutputKind::Sandbox
        | YtDlpOutputKind::Subtitles
        | YtDlpOutputKind::Thumbnails
        | YtDlpOutputKind::Links
        | YtDlpOutputKind::Chapters => OutputCaptureKind::Folder,
    }
}

/// Selector cardinality for comma-separated capture hints.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum SelectorCardinality {
    /// No selector values were configured.
    None,
    /// Exactly one selector value was configured.
    One,
    /// Two or more selector values were configured.
    Many,
}

/// Returns selector cardinality for one optional comma-separated string.
#[must_use]
fn selector_cardinality(value: Option<&str>) -> SelectorCardinality {
    let Some(value) = value else {
        return SelectorCardinality::None;
    };

    let count = value.split(',').filter(|candidate| !candidate.trim().is_empty()).count();
    match count {
        0 => SelectorCardinality::None,
        1 => SelectorCardinality::One,
        _ => SelectorCardinality::Many,
    }
}

/// yt-dlp per-variant config.
#[derive(Debug, Clone, PartialEq, Eq, Deserialize)]
#[serde(deny_unknown_fields)]
pub(crate) struct YtDlpOutputVariantConfig {
    /// Value-driven output semantic kind.
    pub(crate) kind: YtDlpOutputKind,
    /// Optional tri-state save-policy override (defaults to `true`).
    #[serde(default = "default_output_variant_save")]
    pub(crate) save: OutputSaveConfig,
    /// Optional capture kind override.
    ///
    /// When omitted, mediapm applies kind-based defaults so legacy configs keep
    /// stable behavior.
    #[serde(default)]
    pub(crate) capture_kind: Option<OutputCaptureKind>,
    /// Optional capture-side language hint used by variant materialization.
    ///
    /// Downloader language selection remains step-option-driven via
    /// `steps[*].options.sub_langs`.
    #[serde(default)]
    pub(crate) langs: Option<String>,
    /// Optional thumbnail-id hint used by variant materialization.
    ///
    /// Thumbnail generation remains downloader-option owned.
    #[serde(default)]
    pub(crate) thumbnail_ids: Option<String>,
    /// Optional subtitle format hint for capture/materialization policy.
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

impl YtDlpOutputVariantConfig {
    /// Returns effective file-vs-folder capture kind for this variant.
    #[must_use]
    pub(crate) fn effective_capture_kind(&self) -> OutputCaptureKind {
        if let Some(value) = self.capture_kind {
            return value;
        }

        match self.kind {
            YtDlpOutputKind::Subtitles => {
                if matches!(selector_cardinality(self.langs.as_deref()), SelectorCardinality::One) {
                    OutputCaptureKind::File
                } else {
                    OutputCaptureKind::Folder
                }
            }
            YtDlpOutputKind::Thumbnails => {
                if matches!(
                    selector_cardinality(self.thumbnail_ids.as_deref()),
                    SelectorCardinality::One
                ) {
                    OutputCaptureKind::File
                } else {
                    OutputCaptureKind::Folder
                }
            }
            _ => default_yt_dlp_capture_kind_for_kind(self.kind),
        }
    }
}

impl From<&YtDlpOutputVariantConfig> for OutputVariantPolicyConfig {
    fn from(value: &YtDlpOutputVariantConfig) -> Self {
        Self { save: value.save }
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
#[allow(clippy::too_many_lines)]
pub(crate) fn decode_output_variant_config(
    tool: MediaStepTool,
    variant_key: &str,
    value: &Value,
) -> Result<DecodedOutputVariantConfig, String> {
    let decoded = if matches!(tool, MediaStepTool::YtDlp) {
        if !value.is_object() {
            return Err(format!(
                "output variant '{variant_key}' for tool '{}' must be an object with at least a 'kind' field",
                tool.as_str()
            ));
        }

        let config = serde_json::from_value::<YtDlpOutputVariantConfig>(value.clone()).map_err(
            |error| {
                format!(
                    "output variant '{variant_key}' for tool '{}' has invalid yt-dlp config: {error}",
                    tool.as_str()
                )
            },
        )?;

        DecodedOutputVariantConfig::YtDlp(config)
    } else {
        if !value.is_object() {
            return Err(format!(
                "output variant '{variant_key}' for tool '{}' must be an object with at least field 'kind'",
                tool.as_str()
            ));
        }

        let config = serde_json::from_value::<GenericOutputVariantConfig>(value.clone()).map_err(
            |error| {
                format!(
                    "output variant '{variant_key}' for tool '{}' has invalid config: {error}",
                    tool.as_str()
                )
            },
        )?;
        DecodedOutputVariantConfig::Generic(config)
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

            if config.extension.is_some()
                && !matches!(
                    tool,
                    MediaStepTool::Ffmpeg | MediaStepTool::Rsgain | MediaStepTool::MediaTagger
                )
            {
                return Err(format!(
                    "output variant '{variant_key}' for tool '{}' must not define extension; extension is only valid for tools 'ffmpeg', 'rsgain', and 'media-tagger'",
                    tool.as_str()
                ));
            }

            if let Some(extension) = config.extension.as_deref() {
                let normalized = extension.trim();
                if normalized.contains('/') || normalized.contains('\\') {
                    return Err(format!(
                        "output variant '{variant_key}' for tool '{}' extension must not contain path separators",
                        tool.as_str()
                    ));
                }
                if normalized.chars().any(char::is_whitespace) {
                    return Err(format!(
                        "output variant '{variant_key}' for tool '{}' extension must not contain whitespace",
                        tool.as_str()
                    ));
                }
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

            if config.thumbnail_ids.as_deref().is_some_and(|value| value.trim().is_empty()) {
                return Err(format!(
                    "output variant '{variant_key}' for tool '{}' thumbnail_ids must be non-empty",
                    tool.as_str()
                ));
            }

            if !matches!(config.kind, YtDlpOutputKind::Thumbnails) && config.thumbnail_ids.is_some()
            {
                return Err(format!(
                    "output variant '{variant_key}' for tool '{}' must not define thumbnail_ids unless kind = 'thumbnails'",
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
