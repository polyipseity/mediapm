//! Media-tagger workflow step synthesis.
//!
//! Produces the conductor workflow steps for one `media-tagger` metadata step.
//!
//! The tagging step names a builtin instead of a program: it declares no command
//! parts, and the tool definition [`build_media_tagger_spec`] builds carries the
//! `media-tagger` builtin id, so the conductor dispatches it to the runner
//! mediapm registers for that id (see
//! [`MediaTaggerBuiltin`](crate::builtins::media_tagger::host::MediaTaggerBuiltin))
//! rather than spawning a launcher.

use std::collections::BTreeMap;
use std::path::Path;

use mediapm_conductor::{
    NickelDocument, OutputCaptureSpec, SaveMode, ToolKindSpec, ToolRuntime, ToolSpec, WorkflowSpec,
    WorkflowStepSpec,
};

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
use super::yt_dlp_inputs::resolve_step_output_binding;
use super::{
    AutoSaveContext, FfmpegSlotLimits, VariantProducer, media_step_id,
    resolve_input_variant_producer, step_option_input_bindings, variant_to_output_capture_spec,
};

/// Fixed sandbox output file path for media-tagger `FFmetadata` documents.
///
/// The tagging step captures it as a `file:` output and the host builtin writes
/// it at the same sandbox-relative path, so the capture and the write cannot
/// disagree about where the document lands.
pub(crate) const MEDIA_TAGGER_OUTPUT_FILE: &str = "metadata/output.ffmeta";

/// The builtin id `media-tagger` is dispatched by.
///
/// One spelling for the tool definition, the host runner that answers for it
/// and the generated document's retention set.
pub(crate) const MEDIA_TAGGER_BUILTIN_ID: &str = "media-tagger";

/// Regex matching the apply step's single ffmpeg output file (slot 0).
///
/// Mirrors the private `ffmpeg::ffmpeg_output_file_regex(0)` pattern so the
/// apply step (synthesized here, not by the ffmpeg synthesizer) captures the
/// output regardless of the container-override extension.
const MEDIA_TAGGER_APPLY_OUTPUT_FILE_REGEX: &str = r"^output-0(?:[.][^/\\]+)?$";

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
    media_tagger_cache_dir: &Path,
    context: &AutoSaveContext,
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
                media_tagger_cache_dir.to_str().unwrap_or(""),
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
                build_media_tagger_apply_output(
                    &apply_output_name,
                    &mapping.output,
                    output_variant_value,
                    step_index,
                    context,
                    step.tool.is_impure(),
                ),
            )]),
            max_retries: 0,
        });

        pending_variant_updates.push((
            mapping.output.clone(),
            VariantProducer::StepOutput {
                step_id: apply_step_id,
                output_name: apply_output_name,
                zip_member: output_binding.zip_member,
                extension: None,
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
/// step does not override it. Synthesizer-owned options (`ffmpeg_version`,
/// `output_container`) are dropped; nothing here renders a command, because the
/// tagging step declares no command parts and the host builtin reads the
/// inputs directly.
fn build_media_tagger_metadata_inputs(
    step: &MediaStep,
    input_binding: &str,
    ffmpeg_bin_for_metadata: &str,
    cache_dir: &str,
) -> BTreeMap<String, String> {
    let mut inputs = BTreeMap::from([(INPUT_CONTENT.to_string(), input_binding.to_string())]);
    inputs.extend(step_option_input_bindings(step));
    // Consumed by this synthesizer, never forwarded to media-tagger.
    inputs.remove("ffmpeg_version");
    inputs.remove("output_container");
    // The shared option filter drops MBID lookups; they remain media-tagger
    // inputs.
    for mbid_key in ["recording_mbid", "release_mbid"] {
        if let Some(value) = step_option_scalar(step, mbid_key) {
            inputs.insert(mbid_key.to_string(), value.to_string());
        }
    }
    inputs.entry("ffmpeg_bin".to_string()).or_insert_with(|| ffmpeg_bin_for_metadata.to_string());
    inputs.entry("cache_dir".to_string()).or_insert_with(|| cache_dir.to_string());
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
/// output-variant configuration, resolved against the declared variant name the
/// hierarchy selector matches rather than against the capture key.
fn build_media_tagger_apply_output(
    apply_output_name: &str,
    variant: &str,
    output_variant_value: &OutputVariantValue,
    step_index: usize,
    context: &AutoSaveContext,
    step_impure: bool,
) -> OutputCaptureSpec {
    OutputCaptureSpec {
        name: apply_output_name.to_string(),
        capture: format!("file_regex:{MEDIA_TAGGER_APPLY_OUTPUT_FILE_REGEX}"),
        save: variant_to_output_capture_spec(
            apply_output_name,
            variant,
            output_variant_value,
            step_index,
            context,
            step_impure,
        )
        .save,
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

/// Builds media-tagger output capture spec map.
///
/// These are live defaults, so the cover-art folder tolerates an empty match:
/// whether a release has artwork is the provider's answer, not the run's, and a
/// track without it must not fail the step.
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
                allow_empty: true,
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

/// Builds the tool definition the conductor dispatches `media-tagger` through.
///
/// The tool is a builtin mediapm's host supplies for its own id: it declares no
/// command and carries no payload, so a step naming it runs the host runner
/// in-process instead of spawning a binary. The declared outputs are the
/// captures every run records — the `FFmetadata` document and the cover-art
/// folder — and each step says which of them it saves.
///
/// The inputs a tagging step binds are its own: no default lives here, so the
/// step's bindings are the whole answer for which option a run uses.
#[must_use]
pub(crate) fn build_media_tagger_spec() -> (ToolSpec, ToolRuntime) {
    let runtime = ToolRuntime { impure: true, ..ToolRuntime::default() };
    let spec = ToolSpec {
        version: None,
        kind: ToolKindSpec::Builtin { builtin_id: MEDIA_TAGGER_BUILTIN_ID.to_string() },
        name: MEDIA_TAGGER_BUILTIN_ID.to_string(),
        inputs: BTreeMap::new(),
        default_inputs: BTreeMap::new(),
        outputs: build_media_tagger_outputs(),
        runtime: runtime.clone(),
    };
    (spec, runtime)
}

#[cfg(test)]
mod tests {
    use super::*;

    use crate::config::MediaStepTool;
    use crate::config::source_types::TransformInputValue;

    #[test]
    fn build_media_tagger_outputs_include_standard_captures() {
        let outputs = build_media_tagger_outputs();
        assert!(outputs.contains_key("stdout"), "missing stdout output");
        assert!(outputs.contains_key("stderr"), "missing stderr output");
        assert!(outputs.contains_key("process_code"), "missing process_code output");
    }

    /// The cover-art default tolerates an empty extraction.
    ///
    /// Whether a cover image exists is a property of the release, not of the
    /// run: a track with no artwork on the provider legitimately produces an
    /// empty cover-art directory, and a folder capture treats that as a failure.
    #[test]
    fn the_cover_art_default_tolerates_an_empty_match() {
        let outputs = build_media_tagger_outputs();
        assert!(
            outputs[OUTPUT_SANDBOX_ARTIFACTS].allow_empty,
            "cover art is optional for a release, so an empty extraction must not fail the step",
        );
    }

    /// The tool definition names the builtin and carries no payload.
    ///
    /// The entry a sync writes for `media-tagger` is what the run-start check
    /// and the dispatcher read: a builtin id means the step runs the host
    /// runner, and an empty content map means there is nothing to fetch,
    /// materialize or place on a command line. The declared outputs are the
    /// captures a run records, so they stay part of the definition.
    #[test]
    fn the_tool_definition_is_a_payload_free_builtin() {
        let (spec, runtime) = build_media_tagger_spec();

        assert_eq!(
            spec.kind,
            ToolKindSpec::Builtin { builtin_id: MEDIA_TAGGER_BUILTIN_ID.to_string() },
            "the tool must dispatch to the host runner, not to a command",
        );
        assert_eq!(spec.name, MEDIA_TAGGER_BUILTIN_ID);
        assert!(runtime.content_map.is_empty(), "a host builtin holds no payload");
        assert!(runtime.impure, "tagging reaches the network, so the step is impure");
        assert!(
            spec.inputs.is_empty() && spec.default_inputs.is_empty(),
            "the step binds every option it uses; a definition default would be a second home for it",
        );
        assert!(
            spec.outputs.contains_key(OUTPUT_CONTENT)
                && spec.outputs.contains_key(OUTPUT_SANDBOX_ARTIFACTS),
            "both artifacts a run produces must be capturable, got {:?}",
            spec.outputs.keys().collect::<Vec<_>>(),
        );
    }

    /// The regenerated demo document's `media-tagger` entry is this definition.
    ///
    /// `tests/fixtures/demo_conductor_generated.ncl` is hand-regenerated
    /// whenever the generated-document shape changes, which leaves it able to
    /// drift from the definition the sync actually writes. Pinning the entry
    /// against [`build_media_tagger_spec`] makes the fixture itself the
    /// assertion: a synthesizer change that is not mirrored into the fixture
    /// fails here, and so does a fixture edit that no synthesizer produces.
    #[test]
    fn the_regenerated_fixture_entry_matches_the_tool_definition() {
        let bytes = std::fs::read(
            std::path::Path::new(env!("CARGO_MANIFEST_DIR"))
                .join("tests/fixtures/demo_conductor_generated.ncl"),
        )
        .expect("read the regenerated demo conductor document");
        let document = mediapm_conductor::decode_document(&bytes)
            .expect("the regenerated demo conductor document decodes");
        let entry = document.tools.get(MEDIA_TAGGER_BUILTIN_ID).unwrap_or_else(|| {
            panic!(
                "the fixture must hold a '{MEDIA_TAGGER_BUILTIN_ID}' entry, got {:?}",
                document.tools.keys().collect::<Vec<_>>()
            )
        });

        let (spec, runtime) = build_media_tagger_spec();
        assert_eq!(entry.kind, spec.kind, "the entry must dispatch to the host runner");
        assert_eq!(entry.name, spec.name, "the entry keeps the builtin id as its name");
        assert_eq!(entry.inputs, spec.inputs, "the definition declares no input of its own");
        assert_eq!(
            entry.default_inputs, spec.default_inputs,
            "the definition declares no default of its own",
        );
        assert_eq!(entry.outputs, spec.outputs, "the captures a run records must match");
        assert!(
            entry.runtime.content_map.is_empty() && runtime.content_map.is_empty(),
            "a host builtin holds no payload, so neither side names a content root: fixture {:?}",
            entry.runtime.content_map
        );
    }

    /// Ensures synthesized metadata inputs default `cache_dir` to the
    /// caller-provided workspace cache directory (empty when omitted, so the
    /// HTTP cache stays disabled unless a real path is threaded through).
    #[test]
    fn build_media_tagger_metadata_inputs_default_cache_dir() {
        let step = MediaStep {
            tool: MediaStepTool::MediaTagger,
            input_variants: vec![],
            output_variants: BTreeMap::new(),
            options: BTreeMap::new(),
        };
        let inputs = build_media_tagger_metadata_inputs(
            &step,
            "input",
            "ffmpeg",
            "/workspace/cache/media_tagger",
        );
        assert_eq!(
            inputs.get("cache_dir").map(String::as_str),
            Some("/workspace/cache/media_tagger"),
            "caller-provided cache dir must be injected when the step sets none"
        );
    }

    /// Ensures a user-set `options.cache_dir` wins over the threaded
    /// workspace default.
    #[test]
    fn build_media_tagger_metadata_inputs_step_cache_dir_wins() {
        let step = MediaStep {
            tool: MediaStepTool::MediaTagger,
            input_variants: vec![],
            output_variants: BTreeMap::new(),
            options: BTreeMap::from([(
                "cache_dir".to_string(),
                TransformInputValue::String("/user/cache".to_string()),
            )]),
        };
        let inputs = build_media_tagger_metadata_inputs(
            &step,
            "input",
            "ffmpeg",
            "/workspace/cache/media_tagger",
        );
        assert_eq!(
            inputs.get("cache_dir").map(String::as_str),
            Some("/user/cache"),
            "user-set cache_dir must override the workspace default"
        );
    }

    /// Builds a media-tagger step with the given `output_container` option.
    fn media_tagger_step(output_container: Option<&str>) -> MediaStep {
        MediaStep {
            tool: MediaStepTool::MediaTagger,
            input_variants: vec![],
            output_variants: BTreeMap::new(),
            options: output_container
                .map(|value| {
                    BTreeMap::from([(
                        "output_container".to_string(),
                        TransformInputValue::String(value.to_string()),
                    )])
                })
                .unwrap_or_default(),
        }
    }

    /// The apply step's fixed merge bindings and the file it writes.
    ///
    /// The user-level guarantee: an apply step merges the metadata document
    /// into a copy of the source instead of re-encoding it. `map_metadata=1`
    /// names the metadata document as the merge source, `codec_copy=true`
    /// keeps the streams untouched, and `vn=false` keeps the video stream —
    /// the three values that decide whether a tagged file still holds its
    /// media. The output path is the slot-0 file the step's own capture
    /// matches, so a tool that wrote anywhere else would produce no output.
    #[test]
    fn build_media_tagger_apply_inputs_fixes_the_merge_bindings() {
        let step = media_tagger_step(None);
        let metadata_step_id = "step-0-0-media-tagger-default-to-tagged-metadata";
        let source_binding = "${external_data.blake3:abc}";
        let inputs = build_media_tagger_apply_inputs(&step, source_binding, metadata_step_id);

        assert_eq!(inputs.get("map_metadata").map(String::as_str), Some("1"));
        assert_eq!(inputs.get("codec_copy").map(String::as_str), Some("true"));
        assert_eq!(inputs.get("vn").map(String::as_str), Some("false"));
        assert_eq!(inputs.get(INPUT_LEADING_ARGS).map(String::as_str), Some("[]"));
        assert_eq!(inputs.get(INPUT_TRAILING_ARGS).map(String::as_str), Some("[]"));
        assert_eq!(
            inputs.get(INPUT_FFMETADATA_CONTENT).map(String::as_str),
            Some(format!("${{step_output.{metadata_step_id}.content}}").as_str()),
            "the metadata document is the step's second input, so the merge source is the step that wrote it",
        );
        assert_eq!(
            inputs.get(&ffmpeg_input_content_name(0)).map(String::as_str),
            Some(source_binding),
            "the source keeps the binding its producer resolved",
        );
        assert_eq!(
            inputs.get(&ffmpeg_output_path_input_name(0)).map(String::as_str),
            Some(ffmpeg_output_file_path(0).as_str()),
            "the output path is the slot-0 file the apply capture matches",
        );
        assert!(
            !inputs.contains_key("container"),
            "a step that names no output_container must not constrain the muxer",
        );
    }

    /// A step's `output_container` option reaches the apply step's command.
    ///
    /// The user-level guarantee: asking for a container is enough to get it.
    /// The option is the only way a media-tagger step can override the muxer,
    /// and dropping it would silently write the tool's own default instead.
    #[test]
    fn build_media_tagger_apply_inputs_forwards_a_container_override() {
        let step = media_tagger_step(Some("mp4"));
        let inputs = build_media_tagger_apply_inputs(&step, "input", "meta");

        assert_eq!(inputs.get("container").map(String::as_str), Some("mp4"));
    }
}
