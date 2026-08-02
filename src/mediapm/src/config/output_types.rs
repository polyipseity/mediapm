//! Output variant configuration types for mediapm step outputs.
//!
//! These types model per-variant output behavior across tools.  Tool-specific
//! variant decoding (yt-dlp vs generic) is dispatched at decode time so
//! downstream code works with uniformly typed variant configs.

use serde::{Deserialize, Serialize};
use serde_json::Value;

use super::source_types::MediaStep;

// ---------------------------------------------------------------------------
// Shared output behavior
// ---------------------------------------------------------------------------

/// Output persistence policy for one output variant.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(untagged)]
pub enum OutputSaveConfig {
    /// Boolean toggle (`true` / `false`).
    Bool(bool),
    /// Full persistence (all materialized outputs kept, not pruned).
    #[serde(rename = "full")]
    Full,
}

impl Default for OutputSaveConfig {
    fn default() -> Self {
        Self::Bool(true)
    }
}

/// Returns true when `save` holds the default `Bool(true)` persistence policy.
#[must_use]
#[expect(clippy::trivially_copy_pass_by_ref)]
fn is_default_save(save: &OutputSaveConfig) -> bool {
    *save == OutputSaveConfig::Bool(true)
}

/// Returns true when `idx` holds the default zero index.
#[must_use]
#[expect(clippy::trivially_copy_pass_by_ref)]
fn is_zero_idx(idx: &u32) -> bool {
    *idx == 0
}

// ---------------------------------------------------------------------------
// Generic variant config (used by import, ffmpeg, rsgain, media-tagger)
// ---------------------------------------------------------------------------

/// Output capture kind for one variant's artifact type.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "lowercase")]
pub enum OutputCaptureKind {
    /// Variant produces a single file.
    File,
    /// Variant produces a folder of files.
    Folder,
}

/// Generic tool output variant config (non-yt-dlp tools).
#[derive(Debug, Clone, Default, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct GenericOutputVariantConfig {
    /// Output kind label.
    pub kind: String,
    /// Persistence policy.
    #[serde(default, skip_serializing_if = "is_default_save")]
    pub save: OutputSaveConfig,
    /// Optional explicit capture kind override.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub capture_kind: Option<OutputCaptureKind>,
    /// Archive member name for zip-folder variants.
    #[serde(default, skip_serializing_if = "String::is_empty")]
    pub zip_member: String,
    /// Numeric index (used by ffmpeg output slots).
    #[serde(default, skip_serializing_if = "is_zero_idx")]
    pub idx: u32,
    /// File extension override.
    #[serde(default, skip_serializing_if = "String::is_empty")]
    pub extension: String,
}

// ---------------------------------------------------------------------------
// yt-dlp specific variant config
// ---------------------------------------------------------------------------

/// yt-dlp output variant kind identifiers.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum YtDlpOutputKind {
    /// Primary media output.
    Primary,
    /// Chapter-segmented outputs.
    Chapters,
    /// Subtitle track outputs.
    Subtitles,
    /// Thumbnail image outputs.
    Thumbnails,
    /// Description text outputs.
    Description,
    /// `InfoJSON` metadata outputs.
    Infojson,
    /// Comment metadata outputs.
    Comment,
    /// Archive/log outputs.
    Archive,
    /// Annotation outputs.
    Annotation,
    /// URL/link sidecar outputs.
    Links,
}

/// yt-dlp specific output variant config.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct YtDlpOutputVariantConfig {
    /// yt-dlp output kind.
    pub kind: YtDlpOutputKind,
    /// Persistence policy.
    #[serde(default, skip_serializing_if = "is_default_save")]
    pub save: OutputSaveConfig,
    /// Optional explicit capture kind override.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub capture_kind: Option<OutputCaptureKind>,
    /// Language filter hint (for subtitle variants).
    #[serde(default, skip_serializing_if = "String::is_empty")]
    pub langs: String,
    /// Thumbnail id filter (for thumbnail variants).
    #[serde(default, skip_serializing_if = "String::is_empty")]
    pub thumbnail_ids: String,
    /// Subtitle format override.
    #[serde(default, skip_serializing_if = "String::is_empty")]
    pub sub_format: String,
    /// Subtitle conversion format.
    #[serde(default, skip_serializing_if = "String::is_empty")]
    pub convert: String,
    /// Archive member name.
    #[serde(default, skip_serializing_if = "String::is_empty")]
    pub zip_member: String,
}

impl Default for YtDlpOutputVariantConfig {
    fn default() -> Self {
        Self {
            kind: YtDlpOutputKind::Primary,
            save: OutputSaveConfig::default(),
            capture_kind: None,
            langs: String::new(),
            thumbnail_ids: String::new(),
            sub_format: String::new(),
            convert: String::new(),
            zip_member: String::new(),
        }
    }
}

// ---------------------------------------------------------------------------
// Typed output variant value
// ---------------------------------------------------------------------------

/// Typed output variant value stored on [`MediaStep`].
///
/// The untagged decode tries yt-dlp-specific decoding first (matching the
/// historical tool-dispatched decode order), then falls back to the generic
/// variant config.  Both arms reject unknown fields.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(untagged)]
pub enum OutputVariantValue {
    /// yt-dlp specific variant config.
    YtDlp(YtDlpOutputVariantConfig),
    /// Generic tool variant config.
    Generic(GenericOutputVariantConfig),
}

/// Simplified output variant policy used by workflow persistence.
#[allow(dead_code)]
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct OutputVariantPolicyConfig {
    /// Whether this variant's outputs are persisted.
    pub save: bool,
    /// Whether all outputs are kept (not just the latest).
    pub keep_all: bool,
}

impl From<&GenericOutputVariantConfig> for OutputVariantPolicyConfig {
    fn from(config: &GenericOutputVariantConfig) -> Self {
        match config.save {
            OutputSaveConfig::Bool(true) => Self { save: true, keep_all: false },
            OutputSaveConfig::Bool(false) => Self { save: false, keep_all: false },
            OutputSaveConfig::Full => Self { save: true, keep_all: true },
        }
    }
}

impl From<&YtDlpOutputVariantConfig> for OutputVariantPolicyConfig {
    fn from(config: &YtDlpOutputVariantConfig) -> Self {
        match config.save {
            OutputSaveConfig::Bool(true) => Self { save: true, keep_all: false },
            OutputSaveConfig::Bool(false) => Self { save: false, keep_all: false },
            OutputSaveConfig::Full => Self { save: true, keep_all: true },
        }
    }
}

// ---------------------------------------------------------------------------
// Resolved variant flow
// ---------------------------------------------------------------------------

/// Resolved per-step input/output variant mapping.
#[allow(dead_code)]
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ResolvedStepVariantFlow {
    /// Input variant name consumed by one generated step.
    pub input: String,
    /// Output variant name produced by one generated step.
    pub output: String,
}

// ---------------------------------------------------------------------------
// Default capture kind helpers
// ---------------------------------------------------------------------------

/// Returns the default [`OutputCaptureKind`] for a generic output kind label.
#[must_use]
#[allow(dead_code)]
pub fn default_generic_capture_kind_for_kind(kind: &str) -> OutputCaptureKind {
    match kind {
        "subtitles" | "thumbnails" | "chapters" | "links" => OutputCaptureKind::Folder,
        _ => OutputCaptureKind::File,
    }
}

/// Returns the default [`OutputCaptureKind`] for a yt-dlp output kind.
#[must_use]
#[allow(dead_code)]
pub fn default_yt_dlp_capture_kind_for_kind(kind: YtDlpOutputKind) -> OutputCaptureKind {
    match kind {
        YtDlpOutputKind::Subtitles
        | YtDlpOutputKind::Thumbnails
        | YtDlpOutputKind::Chapters
        | YtDlpOutputKind::Links => OutputCaptureKind::Folder,
        _ => OutputCaptureKind::File,
    }
}

// ---------------------------------------------------------------------------
// Decode functions
// ---------------------------------------------------------------------------

/// Decodes one output variant config value for the given tool.
///
/// yt-dlp steps use [`YtDlpOutputVariantConfig`]; all other tools use
/// [`GenericOutputVariantConfig`].
#[allow(dead_code)]
pub fn decode_output_variant_config(
    tool: &MediaStep,
    variant_key: &str,
    value: &Value,
) -> Result<OutputVariantValue, String> {
    if tool.tool.is_online_media_downloader() {
        serde_json::from_value::<YtDlpOutputVariantConfig>(value.clone())
            .map(OutputVariantValue::YtDlp)
            .map_err(|err| format!("output variant '{variant_key}' for yt-dlp: {err}"))
    } else {
        serde_json::from_value::<GenericOutputVariantConfig>(value.clone())
            .map(OutputVariantValue::Generic)
            .map_err(|err| {
                format!("output variant '{variant_key}' for tool '{}': {err}", tool.tool.as_str())
            })
    }
}

/// Decodes one output-variant policy object for workflow output persistence.
#[allow(dead_code)]
pub fn decode_output_variant_policy(
    tool: &MediaStep,
    variant_key: &str,
    value: &Value,
) -> Result<OutputVariantPolicyConfig, String> {
    match decode_output_variant_config(tool, variant_key, value)? {
        OutputVariantValue::Generic(config) => Ok(OutputVariantPolicyConfig::from(&config)),
        OutputVariantValue::YtDlp(config) => Ok(OutputVariantPolicyConfig::from(&config)),
    }
}

/// Resolves one step's effective variant-flow entries.
#[allow(dead_code)]
pub fn resolve_step_variant_flow(step: &MediaStep) -> Result<Vec<ResolvedStepVariantFlow>, String> {
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

    // ffmpeg: single input fans out to all outputs.
    if matches!(step.tool, crate::config::source_types::MediaStepTool::Ffmpeg) {
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
