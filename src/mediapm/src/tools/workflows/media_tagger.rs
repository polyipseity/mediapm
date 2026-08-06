//! Media-tagger workflow step synthesis.
//!
//! Produces the conductor workflow steps for one `media-tagger` metadata step.

#![allow(dead_code)]

use std::collections::BTreeMap;
use std::path::{Path, PathBuf};

use mediapm_conductor::{
    InputBinding, NickelDocument, OutputCaptureSpec, SaveMode, ToolInputKind, ToolInputSpec,
    ToolRuntime, ToolSpec, WorkflowSpec, WorkflowStepSpec,
};

use mediapm_conductor::tools::helpers::build_os_conditional_selector;

use crate::conductor_bridge::constants::{
    INPUT_CONTENT, INPUT_FFMETADATA_CONTENT, INPUT_LEADING_ARGS, INPUT_TRAILING_ARGS,
    OUTPUT_CONTENT, OUTPUT_SANDBOX_ARTIFACTS,
};
use crate::conductor_bridge::sync::find_active_tool_spec;
use crate::config::MediaStep;
use crate::config::output_types::{OutputVariantValue, ResolvedStepVariantFlow};
use crate::config::source_types::step_option_scalar;
use crate::error::MediaPmError;

use super::ffmpeg::{
    ffmpeg_input_content_name, ffmpeg_output_capture_name, ffmpeg_output_file_path,
    ffmpeg_output_path_input_name,
};
use super::spec::{TokenSpec, assemble_tool_spec, command_option_tokens_for_tool};
use super::yt_dlp_inputs::resolve_step_output_binding;
use super::{
    FfmpegSlotLimits, VariantProducer, media_step_id, resolve_input_variant_producer,
    step_option_input_bindings, variant_to_output_capture_spec,
};

/// Fixed sandbox output file path for media-tagger `FFmetadata` documents.
const MEDIA_TAGGER_OUTPUT_FILE: &str = "metadata/output.ffmeta";

/// Regex matching the apply step's single ffmpeg output file (slot 0).
///
/// Mirrors the private `ffmpeg::ffmpeg_output_file_regex(0)` pattern so the
/// apply step (synthesized here, not by the ffmpeg synthesizer) captures the
/// output regardless of the container-override extension.
const MEDIA_TAGGER_APPLY_OUTPUT_FILE_REGEX: &str = r"^output-0(?:[.][^/\\]+)?$";

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

/// Synthesizes one media-tagger step as a metadata-fetch + ffmpeg-apply pair.
///
/// Each variant-flow mapping produces two consecutive [`WorkflowStepSpec`]s:
///
/// - a **metadata** step running the managed `media-tagger` tool (`tool_id`,
///   resolved upstream by name), feeding the source content plus the step's
///   option inputs and emitting an `FFmetadata` document on `OUTPUT_CONTENT`
///   (`SaveMode::Full`), and
/// - an **apply** step running the selected ffmpeg tool (the `ffmpeg`
///   dependency), merging the metadata back into a copy of the source bytes
///   with `map_metadata`, `codec_copy`, and `vn` fixed inputs, an optional
///   `container` override, and a `primary` output capture matching the
///   ffmpeg slot-0 output file.
///
/// The apply step's `depends_on` chains the metadata step; the metadata step
/// depends only on the input producer. The `mapping.output` variant producer
/// registers against the apply step so downstream steps consume the merged
/// file.
///
/// # Errors
///
/// Returns `MediaPmError::Workflow` when the input variant has no producer,
/// the `ffmpeg` dependency tool is not active, or the output variant is
/// missing.
#[allow(clippy::too_many_arguments)]
pub(crate) fn synthesize_media_tagger_step(
    workflow: &mut WorkflowSpec,
    media_id: &str,
    step_index: usize,
    step: &MediaStep,
    mappings: &[ResolvedStepVariantFlow],
    tool_id: &str,
    generated_doc: &NickelDocument,
    producer_snapshot: &BTreeMap<String, VariantProducer>,
    variant_producers: &mut BTreeMap<String, VariantProducer>,
) -> Result<(), MediaPmError> {
    let ffmpeg_tool_id = resolve_media_tagger_ffmpeg_tool_id(step, generated_doc)?;
    let ffmpeg_bin_for_metadata = resolve_media_tagger_ffmpeg_bin(step);

    let mut pending_variant_updates = Vec::new();

    for (mapping_index, mapping) in mappings.iter().enumerate() {
        let Some(producer) = resolve_input_variant_producer(&mapping.input, producer_snapshot)
        else {
            return Err(MediaPmError::Workflow(format!(
                "media '{media_id}' step #{step_index} references unknown input variant '{}'",
                mapping.input
            )));
        };
        let (input_binding, input_dependency) = producer.to_binding()?;

        let base_step_id = media_step_id(step_index, mapping_index, step.tool, mapping);
        let metadata_step_id = format!("{base_step_id}-metadata");
        let apply_step_id = format!("{base_step_id}-apply");
        // media-tagger dispatch omits ffmpeg slot limits (only ffmpeg slots
        // are bounded), so the default zeroed limits are used for the binding.
        let output_binding = resolve_step_output_binding(
            step.tool,
            &step.output_variants,
            &mapping.output,
            FfmpegSlotLimits::default(),
        )?;
        let output_variant_value = step.output_variants.get(&mapping.output).ok_or_else(|| {
            MediaPmError::Workflow(format!(
                "missing output variant '{}' while resolving output policy",
                mapping.output
            ))
        })?;
        let apply_output_name = ffmpeg_output_capture_name(0);

        // Metadata step: run media-tagger to produce the FFmetadata document.
        let mut metadata_depends_on = Vec::new();
        if let Some(step_dependency) = input_dependency.clone() {
            metadata_depends_on.push(step_dependency);
        }
        workflow.steps.push(WorkflowStepSpec {
            id: metadata_step_id.clone(),
            tool: tool_id.to_string(),
            inputs: build_media_tagger_metadata_inputs(
                step,
                &input_binding,
                &ffmpeg_bin_for_metadata,
            ),
            depends_on: metadata_depends_on,
            outputs: BTreeMap::from([(
                OUTPUT_CONTENT.to_string(),
                OutputCaptureSpec {
                    name: OUTPUT_CONTENT.to_string(),
                    capture: format!("file:{MEDIA_TAGGER_OUTPUT_FILE}"),
                    save: SaveMode::Full,
                    allow_empty: false,
                    include_topmost_folder: true,
                },
            )]),
            max_retries: 0,
        });

        // Apply step: merge the metadata back into a copy of the source.
        let mut apply_depends_on = Vec::new();
        if let Some(step_dependency) = input_dependency {
            apply_depends_on.push(step_dependency);
        }
        apply_depends_on.push(metadata_step_id.clone());
        workflow.steps.push(WorkflowStepSpec {
            id: apply_step_id.clone(),
            tool: ffmpeg_tool_id.clone(),
            inputs: build_media_tagger_apply_inputs(step, &input_binding, &metadata_step_id),
            depends_on: apply_depends_on,
            outputs: BTreeMap::from([(
                apply_output_name.clone(),
                build_media_tagger_apply_output(&apply_output_name, output_variant_value),
            )]),
            max_retries: 0,
        });

        pending_variant_updates.push((
            mapping.output.clone(),
            VariantProducer::StepOutput {
                step_id: apply_step_id,
                output_name: apply_output_name,
                zip_member: output_binding.zip_member,
            },
        ));
    }

    for (output_variant, producer) in pending_variant_updates {
        variant_producers.insert(output_variant, producer);
    }

    Ok(())
}

/// Builds the metadata step's input bindings for one variant flow.
///
/// `input_binding` is the source content binding resolved from the input
/// producer; `ffmpeg_bin_for_metadata` seeds the `ffmpeg_bin` option when the
/// step does not override it. List-style options (`INPUT_LEADING_ARGS`,
/// `INPUT_TRAILING_ARGS`, `option_args`) are JSON-encoded as array strings for
/// the conductor's `${*inputs.*}` splat expansion; synthesizer-owned options
/// (`ffmpeg_version`, `output_container`) are dropped.
fn build_media_tagger_metadata_inputs(
    step: &MediaStep,
    input_binding: &str,
    ffmpeg_bin_for_metadata: &str,
) -> BTreeMap<String, String> {
    let mut inputs = BTreeMap::from([(INPUT_CONTENT.to_string(), input_binding.to_string())]);
    inputs.extend(step_option_input_bindings(step));
    // List-style options are re-encoded below as JSON array strings for the
    // conductor's `${*inputs.*}` splat expansion.
    inputs.remove(INPUT_LEADING_ARGS);
    inputs.remove(INPUT_TRAILING_ARGS);
    inputs.remove("option_args");
    // Consumed by this synthesizer, never forwarded to media-tagger.
    inputs.remove("ffmpeg_version");
    inputs.remove("output_container");
    // The shared option filter drops MBID lookups; they remain media-tagger
    // pair options.
    for mbid_key in ["recording_mbid", "release_mbid"] {
        if let Some(value) = step_option_scalar(step, mbid_key) {
            inputs.insert(mbid_key.to_string(), value.to_string());
        }
    }
    inputs.insert(
        INPUT_LEADING_ARGS.to_string(),
        step_option_json_list(step, INPUT_LEADING_ARGS).unwrap_or_else(|| "[]".to_string()),
    );
    inputs.insert(
        INPUT_TRAILING_ARGS.to_string(),
        step_option_json_list(step, INPUT_TRAILING_ARGS).unwrap_or_else(|| "[]".to_string()),
    );
    if let Some(option_args) = step_option_json_list(step, "option_args") {
        inputs.insert("option_args".to_string(), option_args);
    }
    inputs.entry("ffmpeg_bin".to_string()).or_insert_with(|| ffmpeg_bin_for_metadata.to_string());
    inputs
}

/// Builds the apply step's input bindings for one variant flow.
///
/// The apply step runs the selected ffmpeg tool: slot-0 content input,
/// `INPUT_FFMETADATA_CONTENT` bound to the metadata step's `OUTPUT_CONTENT`,
/// slot-0 output path, empty leading/trailing args, fixed `map_metadata`,
/// `codec_copy`, and `vn` values, plus an optional `container` override from
/// the `output_container` option.
fn build_media_tagger_apply_inputs(
    step: &MediaStep,
    input_binding: &str,
    metadata_step_id: &str,
) -> BTreeMap<String, String> {
    let mut inputs = BTreeMap::from([(ffmpeg_input_content_name(0), input_binding.to_string())]);
    inputs.insert(
        INPUT_FFMETADATA_CONTENT.to_string(),
        format!("${{step_output.{metadata_step_id}.{OUTPUT_CONTENT}}}"),
    );
    inputs.insert(ffmpeg_output_path_input_name(0), ffmpeg_output_file_path(0));
    inputs.insert(INPUT_LEADING_ARGS.to_string(), "[]".to_string());
    inputs.insert(INPUT_TRAILING_ARGS.to_string(), "[]".to_string());
    inputs.insert("map_metadata".to_string(), "1".to_string());
    inputs.insert("codec_copy".to_string(), "true".to_string());
    inputs.insert("vn".to_string(), "false".to_string());
    if let Some(output_container) = step_option_scalar(step, "output_container")
        .map(str::trim)
        .filter(|value| !value.is_empty())
    {
        inputs.insert("container".to_string(), output_container.to_string());
    }
    inputs
}

/// Builds the apply step's single slot-0 output capture.
///
/// The capture matches the ffmpeg slot-0 output file regardless of the
/// container-override extension; the save mode derives from the step's
/// output-variant configuration.
fn build_media_tagger_apply_output(
    apply_output_name: &str,
    output_variant_value: &OutputVariantValue,
) -> OutputCaptureSpec {
    OutputCaptureSpec {
        name: apply_output_name.to_string(),
        capture: format!("file_regex:{MEDIA_TAGGER_APPLY_OUTPUT_FILE_REGEX}"),
        save: variant_to_output_capture_spec(apply_output_name, output_variant_value).save,
        allow_empty: false,
        include_topmost_folder: true,
    }
}

/// Resolves the selected ffmpeg tool id for one `media-tagger` step.
///
/// The `ffmpeg_version` option is consumed by this synthesizer (never
/// forwarded as a metadata input). An unset option or the `"global"` selector
/// resolves the active ffmpeg tool; a pinned version request also resolves
/// through the active ffmpeg tool because managed [`ToolSpec`] entries carry
/// no version field to match against (the old version-filtering registry
/// lookup has no equivalent in the content-addressed tool registry).
///
/// # Errors
///
/// Returns `MediaPmError::Workflow` when no active ffmpeg tool is registered
/// in the conductor machine config.
fn resolve_media_tagger_ffmpeg_tool_id(
    step: &MediaStep,
    generated_doc: &NickelDocument,
) -> Result<String, MediaPmError> {
    let requested_version =
        step_option_scalar(step, "ffmpeg_version").map(str::trim).filter(|value| !value.is_empty());

    match requested_version {
        // No pin or the "global" selector: use the active ffmpeg tool.
        None | Some("global") => {
            let (_, spec) = find_active_tool_spec(generated_doc, "ffmpeg").ok_or_else(|| {
                MediaPmError::Workflow(
                    "media-tagger step requires active logical tool 'ffmpeg' for metadata apply"
                        .to_string(),
                )
            })?;
            Ok(spec.name.clone())
        }
        // Pinned `ffmpeg_version`: any registered ffmpeg tool satisfies the
        // request (managed specs carry no version data to match); prefer the
        // active entry.
        Some(requested_version) => {
            let (_, spec) = find_active_tool_spec(generated_doc, "ffmpeg").ok_or_else(|| {
                MediaPmError::Workflow(format!(
                    "media-tagger step requested ffmpeg_version '{requested_version}', but no matching ffmpeg tool is registered in conductor machine config"
                ))
            })?;
            Ok(spec.name.clone())
        }
    }
}

/// Resolves the ffmpeg executable path passed into the metadata-fetch stage.
fn resolve_media_tagger_ffmpeg_bin(step: &MediaStep) -> String {
    if let Some(explicit_bin) =
        step_option_scalar(step, "ffmpeg_bin").map(str::trim).filter(|value| !value.is_empty())
    {
        return explicit_bin.to_string();
    }

    "ffmpeg".to_string()
}

/// JSON-encodes one list-style step option into a splat-compatible array
/// string, whitespace-split (matching the legacy `StringList` binding).
fn step_option_json_list(step: &MediaStep, key: &str) -> Option<String> {
    let tokens = step_option_scalar(step, key)?.split_whitespace().collect::<Vec<_>>();
    // The conductor's `${*inputs.*}` splat JSON-decodes the value; serializing
    // a string-slice array cannot fail.
    Some(serde_json::to_string(&tokens).expect("serializing a string-slice array is infallible"))
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
            ToolInputSpec { kind: ToolInputKind::String, required: false },
        ),
        (
            INPUT_TRAILING_ARGS.to_string(),
            ToolInputSpec { kind: ToolInputKind::String, required: false },
        ),
        (INPUT_CONTENT.to_string(), ToolInputSpec { kind: ToolInputKind::String, required: false }),
    ]);
    for option_input in MEDIA_TAGGER_OPTION_INPUTS {
        inputs.insert(
            (*option_input).to_string(),
            ToolInputSpec { kind: ToolInputKind::String, required: false },
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
                save: SaveMode::True,
                allow_empty: false,
                include_topmost_folder: true,
            },
        ),
        (
            OUTPUT_SANDBOX_ARTIFACTS.to_string(),
            OutputCaptureSpec {
                name: OUTPUT_SANDBOX_ARTIFACTS.to_string(),
                capture: "folder:coverart".to_string(),
                save: SaveMode::True,
                allow_empty: false,
                include_topmost_folder: true,
            },
        ),
        (
            "stdout".to_string(),
            OutputCaptureSpec {
                name: "stdout".to_string(),
                capture: "stdout".to_string(),
                save: SaveMode::True,
                allow_empty: false,
                include_topmost_folder: true,
            },
        ),
        (
            "stderr".to_string(),
            OutputCaptureSpec {
                name: "stderr".to_string(),
                capture: "stderr".to_string(),
                save: SaveMode::True,
                allow_empty: false,
                include_topmost_folder: true,
            },
        ),
        (
            "process_code".to_string(),
            OutputCaptureSpec {
                name: "process_code".to_string(),
                capture: "process_code".to_string(),
                save: SaveMode::True,
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
    os_exec_paths: &BTreeMap<String, String>,
) -> (ToolSpec, ToolRuntime) {
    let command_path = build_os_conditional_selector(os_exec_paths);
    assemble_tool_spec(
        "media-tagger",
        content_map,
        build_media_tagger_command(&command_path),
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
        let os_exec_paths = BTreeMap::from([("linux".into(), "media-tagger".into())]);
        let (_spec, runtime) = build_media_tagger_spec(content_map, &os_exec_paths);
        assert!(runtime.impure);
    }
}
