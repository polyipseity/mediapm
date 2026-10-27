//! Hermetic execution of the media-tagger apply step.
//!
//! The tagger's hermetic tests stop at the `FFmetadata` document, so the
//! boundary between that document and an applied file was exercised only by a
//! live run. This module runs the **synthesized** apply step — the step a sync
//! writes, not a hand-built copy of it — against the workspace's stub `ffmpeg`,
//! which is enough to prove the wiring: the metadata document reaches the step
//! as an input, the step's command runs, and its declared capture records an
//! output.
//!
//! # What this proves, and what it does not
//!
//! It proves wiring, not tagging. The stub copies a file, so nothing here says
//! the tags reached a media stream — the names that survive a real apply are
//! asserted by the live online demo, which is the only run that has a real
//! `ffmpeg`. What is pinned here is the part a live run cannot localize: the
//! binding from the metadata step's `content` output to the apply step's
//! `ffmetadata_content` input, that the input is staged as a readable file, and
//! that the step's capture records what the command wrote.
//!
//! # The command template is a stand-in, and its order is deliberately not the preset's
//!
//! The apply step's command comes from this module, not from the ffmpeg preset's
//! `build_ffmpeg_spec`. The reason is the stub's own argv contract: it copies the
//! input named after the **first** `-i`, while the preset emits slot 0's content
//! first and the metadata document second. A template in the preset's order would
//! make the stub copy the source media, and the assertion below would then read
//! the wrong bytes while still passing as a copy. The metadata input is put first
//! so the stub's documented rule selects it, which means this module covers the
//! **binding** and not the preset's argument order — exercising that faithfully
//! would need a stub that identifies its metadata input by content rather than by
//! position.
//!
//! # Prerequisite: the stub binary must be built first
//!
//! `cargo test-pkg mediapm` selects the `mediapm` package alone, and Cargo does
//! not build a dev-dependency's binary target for it, so this module's test needs
//! `cargo build-pkg mediapm-test-support` to have run. The workspace gate
//! (`cargo test-all`) passes `--workspace --all-targets` and builds the stub
//! itself, so only the selective per-crate command needs the extra step.

use std::collections::BTreeMap;

use bytes::Bytes;
use mediapm_cas::{CasApi, Hash};
use mediapm_conductor::api::RunWorkflowOptions;
use mediapm_conductor::{
    Conductor, ExternalDataEntry, NickelDocument, OutputCaptureSpec, OutputSaveMode,
    RuntimeStoragePaths, SaveMode, ToolInputKind, ToolInputSpec, ToolKindSpec, ToolRuntime,
    ToolSpec, WorkflowSpec, WorkflowStepSpec, config::versions::encode_document,
};
use mediapm_test_support::stub_tool_path;
use serde_json::json;

use crate::config::MediaPmDocument;
use crate::config::output_types::OutputVariantValue;
use crate::config::source_types::{MediaSourceSpec, MediaStep, MediaStepTool};
use crate::tools::workflows::{
    build_media_workflow_plan_with_limits, ffmpeg::ffmpeg_input_content_name,
    managed_workflow_name, resolve_ffmpeg_slot_limits,
};

/// The media id the synthesized workflow is named after.
const MEDIA_ID: &str = "tag-a";

/// The name the apply step gives the file it writes, and the capture this test
/// reads back.
const APPLY_OUTPUT_NAME: &str = "primary";

/// An `FFmetadata` document shaped like the one the metadata step writes.
const FFMETADATA_DOCUMENT: &[u8] = b";FFMETADATA1\ntitle=Test Recording\nartist=Test Artist\n";

/// The source media bytes the apply step is handed. The stub never decodes
/// them: it has to find the file the step's input staged, nothing more.
const SOURCE_MEDIA_BYTES: &[u8] = b"source media bytes";

/// Builds the media-tagger step's config document over one staged source hash.
///
/// The step is the whole media entry's chain, so its input variant is the media
/// source itself and the synthesizer binds the apply step's source input to
/// `${external_data.<hash>}` rather than to a preceding step.
fn media_document(source_hash: Hash) -> MediaPmDocument {
    let output_variant = serde_json::from_value::<OutputVariantValue>(json!({
        "kind": "output_content",
        "save": true,
    }))
    .expect("the generic output variant shape must decode");
    MediaPmDocument {
        media: BTreeMap::from([(
            MEDIA_ID.to_string(),
            MediaSourceSpec {
                description: String::new(),
                variant_hashes: BTreeMap::from([("default".to_string(), source_hash.to_string())]),
                steps: vec![MediaStep {
                    tool: MediaStepTool::MediaTagger,
                    input_variants: vec!["default".to_string()],
                    output_variants: BTreeMap::from([("tagged".to_string(), output_variant)]),
                    options: BTreeMap::new(),
                }],
                ..MediaSourceSpec::default()
            },
        )]),
        ..MediaPmDocument::default()
    }
}

/// A generated-document tool entry that exists only to be resolved by name.
///
/// The planner looks both managed tools up while it synthesizes the chain; it
/// reads neither tool's command, which this test supplies separately.
fn resolvable_tool(name: &str) -> ToolSpec {
    ToolSpec {
        version: None,
        kind: ToolKindSpec::default(),
        name: name.to_string(),
        inputs: BTreeMap::new(),
        default_inputs: BTreeMap::new(),
        outputs: BTreeMap::new(),
        runtime: ToolRuntime::default(),
    }
}

/// The generated document the planner resolves `media-tagger` and `ffmpeg` in.
fn generated_document() -> NickelDocument {
    NickelDocument {
        tools: BTreeMap::from([
            ("media-tagger@stub".to_string(), resolvable_tool("media-tagger")),
            ("ffmpeg@stub".to_string(), resolvable_tool("ffmpeg")),
        ]),
        ..NickelDocument::default()
    }
}

/// An executable tool spec that runs `stub` with `arguments`.
///
/// The command is a template, so `${inputs.<name>}` reaches the stub as the
/// value the step's input resolved to — the same mechanism the real presets use.
fn stub_tool(
    name: &str,
    stub: &std::path::Path,
    arguments: &[&str],
    inputs: &[(&str, ToolInputKind)],
) -> ToolSpec {
    ToolSpec {
        version: None,
        kind: ToolKindSpec::Executable {
            command: std::iter::once(stub.display().to_string())
                .chain(arguments.iter().map(|argument| (*argument).to_string()))
                .collect(),
            env_vars: BTreeMap::new(),
            success_codes: vec![0],
        },
        name: name.to_string(),
        inputs: inputs
            .iter()
            .map(|(name, kind)| {
                ((*name).to_string(), ToolInputSpec { kind: *kind, required: false })
            })
            .collect(),
        default_inputs: BTreeMap::new(),
        outputs: BTreeMap::new(),
        runtime: ToolRuntime::default(),
    }
}

/// The step that hands the apply step its `FFmetadata` document.
///
/// It carries the synthesized metadata step's id, because the apply step's
/// `ffmetadata_content` input is bound to `${step_output.<that id>.content}`.
/// Reusing the id is what lets the real apply step run unmodified: the binding
/// is the contract under test, so rewriting it would test nothing.
fn metadata_producer_step(id: &str, metadata_hash: Hash) -> WorkflowStepSpec {
    WorkflowStepSpec {
        id: id.to_string(),
        tool: "ffmetadata-producer".to_string(),
        inputs: BTreeMap::from([
            ("source".to_string(), format!("${{external_data.{metadata_hash}}}")),
            ("output_path".to_string(), "ffmeta.txt".to_string()),
        ]),
        outputs: BTreeMap::from([(
            "content".to_string(),
            OutputCaptureSpec {
                name: "content".to_string(),
                capture: "file:ffmeta.txt".to_string(),
                save: SaveMode::Full,
                allow_empty: false,
                include_topmost_folder: true,
            },
        )]),
        max_retries: 0,
        depends_on: Vec::new(),
    }
}

/// The synthesized apply step runs, consumes the metadata document, and records
/// an output.
///
/// The user-level guarantee: a tagged file is produced by a step that received
/// the metadata document, not by one that merely declared it. The stub reads the
/// first `-i` — which this module's template arranges to be the step's
/// `ffmetadata_content` input — and writes the last argument, which the
/// template's single output slot makes the step's own `output_path_0`. The
/// captured bytes are therefore the document that reached the step, so a
/// binding that stopped resolving, an input that stopped being staged, or a
/// capture that stopped matching all fail here.
#[tokio::test]
async fn the_synthesized_apply_step_consumes_the_metadata_document() {
    let workspace = mediapm_utils::temp::artifact_dir().expect("artifact dir");
    let conductor =
        Conductor::new(RuntimeStoragePaths::new(workspace.path()), mediapm_cas::InMemoryCas::new());

    let source_hash = conductor
        .cas()
        .put(Bytes::from_static(SOURCE_MEDIA_BYTES))
        .await
        .expect("the source bytes must be storable");
    let metadata_hash = conductor
        .cas()
        .put(Bytes::from_static(FFMETADATA_DOCUMENT))
        .await
        .expect("the metadata document must be storable");

    let document = media_document(source_hash);
    let generated = generated_document();
    let plan = build_media_workflow_plan_with_limits(
        &document,
        &generated,
        resolve_ffmpeg_slot_limits(&document),
        workspace.path(),
    )
    .expect("the plan must synthesize");
    let steps = &plan.workflows[&managed_workflow_name(MEDIA_ID)].steps;
    let metadata_step_id = steps.first().expect("a metadata step").id.clone();
    let apply_step = steps.get(1).expect("an apply step");
    assert_eq!(apply_step.tool, "ffmpeg", "the apply step runs the ffmpeg dependency");
    assert_eq!(
        apply_step.inputs.get(&ffmpeg_input_content_name(0)).map(String::as_str),
        Some(format!("${{external_data.{source_hash}}}").as_str()),
        "the source input is the media's own variant, so the step needs no rewrite",
    );

    let stub = stub_tool_path();
    let doc = apply_document(apply_step, &metadata_step_id, source_hash, metadata_hash, &stub);
    std::fs::write(workspace.path().join("conductor.ncl"), encode_document(doc).expect("encode"))
        .expect("write the conductor document");

    let summary = conductor
        .run_workflow("default", RunWorkflowOptions::default())
        .await
        .expect("the apply step must run");
    assert_eq!(
        (summary.executed_steps, summary.cached_steps, summary.failed_steps),
        (2, 0, 0),
        "the producer and the apply step each run once and succeed: {summary:?}",
    );
    assert_eq!(
        captured_primary(&conductor).await.as_ref(),
        FFMETADATA_DOCUMENT,
        "the captured output must be the metadata document the step received",
    );

    conductor.shutdown().await.expect("conductor shutdown");
}

/// The conductor document the run executes.
///
/// The apply step is the synthesized one, unmodified; everything else exists to
/// give it a producer and a tool to run. Both tool specs declare their inputs as
/// the step supplies them, so a command template that named an input the step
/// does not pass would fail to resolve rather than silently expand to nothing.
///
/// The ffmpeg template is hand-written rather than the preset's, and it puts the
/// metadata input before the source input: see the module doc's note on why the
/// order deliberately differs from `build_ffmpeg_spec`.
fn apply_document(
    apply_step: &WorkflowStepSpec,
    metadata_step_id: &str,
    source_hash: Hash,
    metadata_hash: Hash,
    stub: &std::path::Path,
) -> NickelDocument {
    let mut doc = NickelDocument {
        tools: BTreeMap::from([
            (
                "ffmetadata-producer@stub".to_string(),
                stub_tool(
                    "ffmetadata-producer",
                    stub,
                    &[
                        "--mode",
                        "copy",
                        "-i",
                        "${inputs.source:file(inputs/ffmeta-source.bin)}",
                        "${inputs.output_path}",
                    ],
                    &[("source", ToolInputKind::Content), ("output_path", ToolInputKind::String)],
                ),
            ),
            (
                "ffmpeg@stub".to_string(),
                stub_tool(
                    "ffmpeg",
                    stub,
                    &[
                        "--mode",
                        "copy",
                        "-i",
                        // First `-i` on purpose: the stub copies that one, and the
                        // metadata document is what this test must read back.
                        "${inputs.ffmetadata_content:file(inputs/ffmetadata.ffmeta)}",
                        "-i",
                        "${inputs.input_content_0:file(inputs/input-0.bin)}",
                        "${inputs.output_path_0}",
                    ],
                    &[
                        ("ffmetadata_content", ToolInputKind::Content),
                        ("input_content_0", ToolInputKind::Content),
                        ("output_path_0", ToolInputKind::String),
                        ("leading_args", ToolInputKind::StringList),
                        ("trailing_args", ToolInputKind::StringList),
                        ("map_metadata", ToolInputKind::String),
                        ("codec_copy", ToolInputKind::String),
                        ("vn", ToolInputKind::String),
                    ],
                ),
            ),
        ]),
        workflows: vec![WorkflowSpec {
            name: "default".to_string(),
            display_name: None,
            description: None,
            impure: false,
            steps: vec![
                metadata_producer_step(metadata_step_id, metadata_hash),
                apply_step.clone(),
            ],
        }],
        ..NickelDocument::default()
    };
    for hash in [source_hash, metadata_hash] {
        doc.external_data.insert(
            hash,
            ExternalDataEntry { description: None, save_mode: OutputSaveMode::Saved },
        );
    }
    doc
}

/// Reads back the bytes the apply step recorded under its `primary` capture.
///
/// The instance is found by the capture name rather than by tool, because the
/// producer step also writes a captured output and the two runs share one
/// instance map.
async fn captured_primary<C>(conductor: &Conductor<C>) -> bytes::Bytes
where
    C: CasApi + mediapm_cas::CasMaintenanceApi + mediapm_cas::ConstraintApi + Send + Sync + 'static,
{
    let state = conductor.get_state().expect("the run must record state");
    let instance = state
        .tool_call_instances
        .values()
        .find(|instance| instance.outputs.contains_key(APPLY_OUTPUT_NAME))
        .expect("the apply step must record its captured output");
    let captured = instance.outputs.get(APPLY_OUTPUT_NAME).expect("primary output");
    conductor.cas().get(captured.hash).await.expect("the captured output must be in the store")
}
