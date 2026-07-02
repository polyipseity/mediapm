//! Media-tagger workflow step synthesis.
//!
//! Produces the conductor workflow steps for one `media-tagger` metadata step.

#![allow(dead_code)]
// TODO: Stream A stubs — wired when provisioning pipeline is complete.

use std::collections::BTreeMap;
use std::path::{Path, PathBuf};

use mediapm_conductor::{
    InputBinding, OutputCaptureSpec, ToolInputKind, ToolInputSpec, ToolRuntime, ToolSpec,
    WorkflowStepSpec,
};

use crate::conductor_bridge::constants::*;
use crate::config::{MediaSourceSpec, MediaStep};

use super::spec::{TokenSpec, assemble_tool_spec, command_option_tokens_for_tool};
use super::{
    OUTPUT_PRIMARY, qualify_step_id, resolve_step_tool_id, source_uri_input,
    step_option_input_bindings, variant_to_output_capture_spec,
};

/// Fixed sandbox output file path for media-tagger FFmetadata documents.
const MEDIA_TAGGER_OUTPUT_FILE: &str = "metadata/output.ffmeta";

const MEDIA_TAGGER_INPUT_DEFAULTS: &[(&str, &str)] = &[
    ("strict_identification", "true"),
    ("write_all_tags", "true"),
    ("write_all_images", "true"),
    ("save_images_to_tags", "true"),
    ("embed_only_one_front_image", "false"),
    ("ca_providers", "caa_release,url_relationships,caa_release_group"),
    ("caa_image_types", "all,-matrix/runout,-raw/unedited,-watermark"),
    ("caa_image_size", "full"),
    ("caa_approved_only", "false"),
    ("preserve_images", "false"),
    ("clear_existing_tags", "false"),
    ("enable_tag_saving", "true"),
    ("release_ars", "true"),
    ("cover_art_slot_count", "16"),
    ("acoustid_endpoint", "https://api.acoustid.org/v2/lookup"),
    ("musicbrainz_endpoint", "https://musicbrainz.org/ws/2"),
    ("cache_dir", ""),
    ("cache_expiry_seconds", "86400"),
];

const MEDIA_TAGGER_TOKEN_SPECS: &[(&str, TokenSpec)] = &[
    ("acoustid_endpoint", TokenSpec::Pair("--acoustid-endpoint")),
    ("musicbrainz_endpoint", TokenSpec::Pair("--musicbrainz-endpoint")),
    ("cache_dir", TokenSpec::Pair("--cache-dir")),
    ("cache_expiry_seconds", TokenSpec::Pair("--cache-expiry-seconds")),
    ("strict_identification", TokenSpec::Bool("--strict-identification")),
    ("write_all_tags", TokenSpec::Bool("--write-all-tags")),
    ("write_all_images", TokenSpec::Bool("--write-all-images")),
    ("save_images_to_tags", TokenSpec::Bool("--save-images-to-tags")),
    ("embed_only_one_front_image", TokenSpec::Bool("--embed-only-one-front-image")),
    ("ca_providers", TokenSpec::Pair("--ca-providers")),
    ("caa_image_types", TokenSpec::Pair("--caa-image-types")),
    ("caa_image_size", TokenSpec::Pair("--caa-image-size")),
    ("caa_approved_only", TokenSpec::Bool("--caa-approved-only")),
    ("preserve_images", TokenSpec::Bool("--preserve-images")),
    ("clear_existing_tags", TokenSpec::Bool("--clear-existing-tags")),
    ("enable_tag_saving", TokenSpec::Bool("--enable-tag-saving")),
    ("release_ars", TokenSpec::Bool("--release-ars")),
    ("cover_art_slot_count", TokenSpec::Pair("--cover-art-slot-count")),
    ("recording_mbid", TokenSpec::Pair("--recording-mbid")),
    ("release_mbid", TokenSpec::Pair("--release-mbid")),
];

const MEDIA_TAGGER_OPTION_INPUTS: &[&str] = &[
    "option_args",
    "acoustid_endpoint",
    "musicbrainz_endpoint",
    "cache_dir",
    "cache_expiry_seconds",
    "strict_identification",
    "write_all_tags",
    "write_all_images",
    "save_images_to_tags",
    "embed_only_one_front_image",
    "ca_providers",
    "caa_image_types",
    "caa_image_size",
    "caa_approved_only",
    "preserve_images",
    "clear_existing_tags",
    "enable_tag_saving",
    "release_ars",
    "cover_art_slot_count",
    "recording_mbid",
    "release_mbid",
    "acoustid_api_key",
    "enable_acoustid",
];

/// Synthesizes one media-tagger workflow step from a media step definition.
///
/// # Errors
///
#[must_use]
pub(crate) fn synthesize_media_tagger_step(
    source: &MediaSourceSpec,
    step_index: usize,
    step: &MediaStep,
) -> Vec<WorkflowStepSpec> {
    let step_id = qualify_step_id(
        source.id.as_deref().unwrap_or("unknown"),
        &format!("media_tagger_{step_index}"),
    );

    let mut inputs = BTreeMap::from([source_uri_input(source)]);
    for (k, v) in step_option_input_bindings(step) {
        inputs.insert(k, v);
    }

    let mut outputs = BTreeMap::new();
    for (name, variant_json) in &step.output_variants {
        if let Ok(config) =
            crate::config::DecodedOutputVariantConfig::from_json_value(variant_json.clone())
        {
            outputs.insert(name.clone(), variant_to_output_capture_spec(name, &config));
        }
    }
    if outputs.is_empty() {
        outputs.insert(
            OUTPUT_PRIMARY.to_string(),
            mediapm_conductor::OutputCaptureSpec {
                name: OUTPUT_PRIMARY.to_string(),
                capture: "file:tagged.*".to_string(),
                save: true,
                allow_empty: false,
                include_topmost_folder: true,
            },
        );
    }

    vec![WorkflowStepSpec {
        id: step_id,
        tool: resolve_step_tool_id(crate::config::MediaStepTool::MediaTagger),
        inputs,
        outputs,
        max_retries: 0,
        depends_on: Vec::new(),
    }]
}

/// Resolves the path to a media-tagger launcher script under the given
/// content-tools directory.
#[must_use]
pub(crate) fn resolve_media_tagger_launcher_binary_path(tools_dir: &Path) -> PathBuf {
    tools_dir.join("media-tagger-launcher")
}

/// Resolves a profile-adjacent `mediapm` binary path for tool discovery.
#[must_use]
pub(crate) fn resolve_profile_adjacent_mediapm_binary(current_exe: &Path) -> Option<PathBuf> {
    let exe_name = current_exe.file_name()?;

    if let Some(parent) = current_exe.parent() {
        // Check sibling directory (same profile).
        let sibling = parent.join(exe_name);
        if sibling.exists() {
            return Some(sibling);
        }

        // Check parent directory (profile-adjacent).
        if let Some(grandparent) = parent.parent() {
            let adjacent = grandparent.join(exe_name);
            if adjacent.exists() {
                return Some(adjacent);
            }
        }
    }

    None
}

#[must_use]
fn build_media_tagger_command(command_path: &str) -> Vec<String> {
    let mut command = vec![
        command_path.to_string(),
        format!("${{*inputs.{INPUT_LEADING_ARGS}}}"),
        format!("${{*inputs.{INPUT_CONTENT} ? --input | ''}}"),
        format!(
            "${{*inputs.{INPUT_CONTENT} ? inputs.{INPUT_CONTENT}:file(inputs/input.media) | ''}}"
        ),
        "--output".to_string(),
        MEDIA_TAGGER_OUTPUT_FILE.to_string(),
    ];
    command.extend(command_option_tokens_for_tool(
        MEDIA_TAGGER_OPTION_INPUTS,
        MEDIA_TAGGER_TOKEN_SPECS,
    ));
    command.push(format!("${{*inputs.{INPUT_TRAILING_ARGS}}}"));
    command
}

#[must_use]
fn build_media_tagger_inputs() -> BTreeMap<String, ToolInputSpec> {
    let mut inputs = BTreeMap::from([
        (
            INPUT_LEADING_ARGS.to_string(),
            ToolInputSpec {
                kind: ToolInputKind::String,
                description: String::new(),
                required: false,
            },
        ),
        (
            INPUT_TRAILING_ARGS.to_string(),
            ToolInputSpec {
                kind: ToolInputKind::String,
                description: String::new(),
                required: false,
            },
        ),
        (
            INPUT_CONTENT.to_string(),
            ToolInputSpec {
                kind: ToolInputKind::String,
                description: String::new(),
                required: false,
            },
        ),
    ]);
    for option_input in MEDIA_TAGGER_OPTION_INPUTS {
        inputs.insert(
            (*option_input).to_string(),
            ToolInputSpec {
                kind: ToolInputKind::String,
                description: String::new(),
                required: false,
            },
        );
    }
    inputs
}

#[must_use]
fn build_media_tagger_outputs() -> BTreeMap<String, OutputCaptureSpec> {
    BTreeMap::from([
        (
            OUTPUT_CONTENT.to_string(),
            OutputCaptureSpec {
                name: OUTPUT_CONTENT.to_string(),
                capture: format!("file:{MEDIA_TAGGER_OUTPUT_FILE}"),
                save: true,
                allow_empty: false,
                include_topmost_folder: true,
            },
        ),
        (
            OUTPUT_SANDBOX_ARTIFACTS.to_string(),
            OutputCaptureSpec {
                name: OUTPUT_SANDBOX_ARTIFACTS.to_string(),
                capture: "folder:coverart".to_string(),
                save: true,
                allow_empty: false,
                include_topmost_folder: true,
            },
        ),
        (
            "stdout".to_string(),
            OutputCaptureSpec {
                name: "stdout".to_string(),
                capture: "stdout".to_string(),
                save: true,
                allow_empty: false,
                include_topmost_folder: true,
            },
        ),
        (
            "stderr".to_string(),
            OutputCaptureSpec {
                name: "stderr".to_string(),
                capture: "stderr".to_string(),
                save: true,
                allow_empty: false,
                include_topmost_folder: true,
            },
        ),
        (
            "process_code".to_string(),
            OutputCaptureSpec {
                name: "process_code".to_string(),
                capture: "process_code".to_string(),
                save: true,
                allow_empty: false,
                include_topmost_folder: true,
            },
        ),
    ])
}

#[must_use]
fn build_media_tagger_default_input_defaults() -> BTreeMap<String, InputBinding> {
    let mut defaults = BTreeMap::from([
        (INPUT_LEADING_ARGS.to_string(), InputBinding::Vec(vec![])),
        (INPUT_TRAILING_ARGS.to_string(), InputBinding::Vec(vec![])),
    ]);
    for option_input in MEDIA_TAGGER_OPTION_INPUTS {
        defaults.entry((*option_input).to_string()).or_default();
    }
    for (key, value) in MEDIA_TAGGER_INPUT_DEFAULTS {
        defaults.insert(key.to_string(), InputBinding::String(value.to_string()));
    }
    defaults
}

/// Builds the full [`ToolSpec`] and [`ToolRuntime`] for the managed media-tagger tool.
#[must_use]
pub(crate) fn build_media_tagger_spec(
    content_map: BTreeMap<String, String>,
    command_path: &str,
) -> (ToolSpec, ToolRuntime) {
    assemble_tool_spec(
        "media-tagger",
        content_map,
        build_media_tagger_command(command_path),
        build_media_tagger_inputs(),
        build_media_tagger_outputs(),
        build_media_tagger_default_input_defaults(),
        true, // impure — media-tagger requires network
        0,    // max_concurrent_calls
        0,    // max_retries
    )
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn build_media_tagger_command_includes_input_output_flags() {
        let command = build_media_tagger_command("media-tagger");
        assert!(command.iter().any(|c| c.contains("--input")), "expected --input flag");
        assert!(command.iter().any(|c| c.contains("--output")), "expected --output flag");
        assert!(
            command.iter().any(|c| c.contains(MEDIA_TAGGER_OUTPUT_FILE)),
            "expected output file path"
        );
    }

    #[test]
    fn build_media_tagger_inputs_includes_expected_entries() {
        let inputs = build_media_tagger_inputs();
        assert!(inputs.contains_key("strict_identification"));
        assert!(inputs.contains_key("ca_providers"));
        assert!(inputs.contains_key("cover_art_slot_count"));
    }

    #[test]
    fn build_media_tagger_outputs_include_standard_captures() {
        let outputs = build_media_tagger_outputs();
        assert!(outputs.contains_key("stdout"), "missing stdout output");
        assert!(outputs.contains_key("stderr"), "missing stderr output");
        assert!(outputs.contains_key("process_code"), "missing process_code output");
    }

    #[test]
    fn build_media_tagger_defaults_include_cache_and_endpoints() {
        let defaults = build_media_tagger_default_input_defaults();
        assert_eq!(
            defaults.get("strict_identification"),
            Some(&InputBinding::String("true".to_string()))
        );
        assert_eq!(
            defaults.get("embed_only_one_front_image"),
            Some(&InputBinding::String("false".to_string()))
        );
        assert_eq!(
            defaults.get("cache_expiry_seconds"),
            Some(&InputBinding::String("86400".to_string()))
        );
    }

    #[test]
    fn build_media_tagger_spec_sets_impure() {
        let content_map = BTreeMap::new();
        let (_spec, runtime) = build_media_tagger_spec(content_map, "media-tagger");
        assert!(runtime.impure);
    }
}
