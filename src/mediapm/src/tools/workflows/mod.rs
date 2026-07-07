//! Workflow-step synthesis from `mediapm.ncl` media-step configs.
//!
//! Each per-tool submodule converts a [`MediaStep`] + source config into one or
//! more [`WorkflowStepSpec`] entries for conductor execution.

#![allow(dead_code)]
// TODO: Stream A stubs — wired when provisioning pipeline is complete.

pub(crate) mod deno;
pub(crate) mod ffmpeg;
pub(crate) mod media_tagger;
pub(crate) mod rsgain;
pub(crate) mod sd;
pub(crate) mod spec;
pub(crate) mod yt_dlp;
mod yt_dlp_inputs;

use mediapm_conductor::{OutputCaptureSpec, OutputSaveMode, SaveMode};

use crate::config::{
    DecodedOutputVariantConfig, GenericOutputVariantConfig, MediaSourceSpec, MediaStep,
    MediaStepTool, OutputCaptureKind, OutputSaveConfig, TransformInputValue,
    YtDlpOutputVariantConfig,
};

// ---------------------------------------------------------------------------
// Shared constants
// ---------------------------------------------------------------------------

/// Prefix for managed workflow names synthesized by mediapm.
pub(crate) const MANAGED_WORKFLOW_PREFIX: &str = "mediapm.media.";

/// Prefix for managed external data descriptions.
pub(crate) const MANAGED_EXTERNAL_DESCRIPTION_PREFIX: &str = "managed external data:";

/// Logical output name for source-ingest primary results.
pub(crate) const OUTPUT_PRIMARY: &str = "primary";
/// Logical output name for import result (CAS hash pointer).
pub(crate) const OUTPUT_IMPORT_RESULT: &str = "result";

/// Logical input name for source URI.
pub(crate) const INPUT_SOURCE_URL: &str = "source_url";
/// Logical input name for import kind selection.
pub(crate) const INPUT_IMPORT_KIND: &str = "kind";
/// Value for import kind: CAS hash pointer.
pub(crate) const IMPORT_KIND_CAS_HASH: &str = "cas_hash";

// ---------------------------------------------------------------------------
// Shared helpers
// ---------------------------------------------------------------------------

/// Converts a mediapm [`OutputSaveConfig`] to a conductor [`OutputSaveMode`].
#[must_use]
pub(crate) fn conductor_output_save_mode(config: OutputSaveConfig) -> OutputSaveMode {
    match config {
        OutputSaveConfig::Bool(true) => OutputSaveMode::Saved,
        OutputSaveConfig::Bool(false) => OutputSaveMode::Unsaved,
        OutputSaveConfig::Full => OutputSaveMode::Full,
    }
}

/// Resolves the managed conductor tool id for one media-step tool.
#[must_use]
pub(crate) fn resolve_step_tool_id(tool: MediaStepTool) -> String {
    tool.as_str().to_string()
}

/// Resolves the managed conductor tool id for a dependency tool.
#[must_use]
pub(crate) fn resolve_selected_dependency_tool_id(tool_name: &str) -> String {
    tool_name.to_string()
}

/// Builds a step-output capture spec from one decoded variant config.
#[must_use]
pub(crate) fn variant_to_output_capture_spec(
    name: &str,
    config: &DecodedOutputVariantConfig,
) -> OutputCaptureSpec {
    match config {
        DecodedOutputVariantConfig::Generic(g) => {
            let (capture, save) = generic_variant_capture_and_save(g);
            OutputCaptureSpec {
                name: name.to_string(),
                capture,
                save,
                allow_empty: false,
                include_topmost_folder: true,
            }
        }
        DecodedOutputVariantConfig::YtDlp(y) => {
            let (capture, save) = yt_dlp_variant_capture_and_save(y);
            OutputCaptureSpec {
                name: name.to_string(),
                capture,
                save,
                allow_empty: false,
                include_topmost_folder: true,
            }
        }
    }
}

fn generic_variant_capture_and_save(config: &GenericOutputVariantConfig) -> (String, SaveMode) {
    let capture = match config.capture_kind {
        Some(OutputCaptureKind::Folder) => format!("file:{}/*", config.kind),
        _ => format!("file:{}", config.kind),
    };
    let save = match config.save {
        OutputSaveConfig::Bool(true) => SaveMode::True,
        OutputSaveConfig::Bool(false) => SaveMode::False,
        OutputSaveConfig::Full => SaveMode::Full,
    };
    (capture, save)
}

fn yt_dlp_variant_capture_and_save(config: &YtDlpOutputVariantConfig) -> (String, SaveMode) {
    use crate::config::YtDlpOutputKind;
    let capture = match config.kind {
        YtDlpOutputKind::Primary => "file:primary.*".to_string(),
        YtDlpOutputKind::Subtitles => "file:subtitles/*".to_string(),
        YtDlpOutputKind::Thumbnails => "file:thumbnails/*".to_string(),
        YtDlpOutputKind::Chapters => "file:chapters/*".to_string(),
        YtDlpOutputKind::Description => "file:description.*".to_string(),
        YtDlpOutputKind::Infojson => "file:info.json".to_string(),
        YtDlpOutputKind::Comment => "file:comment.*".to_string(),
        YtDlpOutputKind::Archive => "file:archive.txt".to_string(),
        YtDlpOutputKind::Annotation => "file:annotations.*".to_string(),
        YtDlpOutputKind::Links => "file:links/*".to_string(),
    };
    let save = match config.save {
        OutputSaveConfig::Bool(true) => SaveMode::True,
        OutputSaveConfig::Bool(false) => SaveMode::False,
        OutputSaveConfig::Full => SaveMode::Full,
    };
    (capture, save)
}

/// Returns managed workflow name for a source entry.
#[must_use]
pub(crate) fn managed_workflow_name(_source: &MediaSourceSpec) -> String {
    String::new()
}

/// Returns the default source-URI input binding for a media step.
#[must_use]
pub(crate) fn source_uri_input(_source: &MediaSourceSpec) -> (String, String) {
    (INPUT_SOURCE_URL.to_string(), String::new())
}

/// Returns step-option input bindings as `(input_key, value_string)` entries.
pub(crate) fn step_option_input_bindings(step: &MediaStep) -> Vec<(String, String)> {
    step.options
        .iter()
        .map(|(k, v)| {
            let value = match v {
                TransformInputValue::String(s) => s.clone(),
            };
            (k.clone(), value)
        })
        .collect()
}

/// Returns true when the given output-variant config has folder-like capture.
#[must_use]
pub(crate) fn variant_is_folder_capture(config: &DecodedOutputVariantConfig) -> bool {
    match config {
        DecodedOutputVariantConfig::Generic(g) => {
            matches!(g.capture_kind, Some(OutputCaptureKind::Folder))
        }
        DecodedOutputVariantConfig::YtDlp(y) => matches!(
            y.kind,
            crate::config::YtDlpOutputKind::Subtitles
                | crate::config::YtDlpOutputKind::Thumbnails
                | crate::config::YtDlpOutputKind::Chapters
                | crate::config::YtDlpOutputKind::Links
        ),
    }
}

/// Prefix delegated step ids with the source media id to avoid collisions.
#[must_use]
pub(crate) fn qualify_step_id(source_id: &str, suffix: &str) -> String {
    format!("{source_id}.{suffix}")
}
