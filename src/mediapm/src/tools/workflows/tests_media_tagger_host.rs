//! The tagging step runs in-process, as a host builtin.
//!
//! Nothing about tagging may depend on a `mediapm` binary on disk. The step
//! declares no command parts, the tool entry a sync writes carries no payload,
//! and the conductor dispatches the step to the runner mediapm registers under
//! the `media-tagger` builtin id.
//! This module runs the **synthesized** metadata step — the step a sync writes,
//! not a hand-built copy of it — against a local `wiremock` `MusicBrainz` host,
//! so a run that still reached for a binary would fail here while the real
//! network stayed untouched.

use std::collections::BTreeMap;

use bytes::Bytes;
use mediapm_cas::CasApi;
use mediapm_conductor::{
    Conductor, ExternalDataEntry, NickelDocument, OutputSaveMode, RuntimeStoragePaths, ToolSpec,
    WorkflowSpec,
};
use serde_json::json;

use crate::conductor_bridge::constants::OUTPUT_CONTENT;
use crate::conductor_run_workflow_options;
use crate::config::MediaPmDocument;
use crate::config::MediaRuntimeStorage;
use crate::config::output_types::OutputVariantValue;
use crate::config::source_types::{MediaSourceSpec, MediaStep, MediaStepTool, TransformInputValue};
use crate::tools::workflows::media_tagger::build_media_tagger_spec;
use crate::tools::workflows::{build_media_workflow_plan_with_limits, managed_workflow_name};

/// Media id the synthesized workflow is named after.
const MEDIA_ID: &str = "tag-host";

/// The output variant the media-tagger step publishes.
const VARIANT: &str = "tagged";

/// A valid recording MBID the mock host serves and the run requests.
const RECORDING_MBID: &str = "8f3471b5-7e6a-48da-86a9-c1c07a0f47ae";

/// A valid release MBID the recording's embedded release points to.
const RELEASE_MBID: &str = "11111111-1111-1111-1111-111111111111";

/// A valid release-group MBID the release points to.
const GROUP_MBID: &str = "22222222-2222-2222-2222-222222222222";

/// A valid artist MBID the artist credits point to.
const ARTIST_MBID: &str = "33d21b86-dc54-47b2-9258-92d5b42c99a1";

/// The artist credit shared by the recording and release payloads.
fn artist_credit() -> serde_json::Value {
    json!([
        {"name": "Test Artist", "joinphrase": "", "artist": {
            "id": ARTIST_MBID,
            "name": "Test Artist",
            "sort-name": "Artist, Test"
        }}
    ])
}

/// The recording payload in the real kebab-case ws/2 shape.
fn recording_json() -> serde_json::Value {
    json!({
        "id": RECORDING_MBID,
        "title": "Test Recording",
        "artist-credit": artist_credit(),
        "isrcs": ["USUM71300001"],
        "first-release-date": "1987-07-27",
        "releases": [
            {"id": RELEASE_MBID, "title": "Test Album", "first-release-date": "1987-07-27"}
        ]
    })
}

/// The release payload in the real kebab-case ws/2 shape.
fn release_json() -> serde_json::Value {
    json!({
        "id": RELEASE_MBID,
        "title": "Test Album",
        "date": "1987-07-27",
        "country": "GB",
        "barcode": "0000000000000",
        "artist-credit": artist_credit(),
        "release-group": {"id": GROUP_MBID, "title": "Test Album"},
        "media": [
            {
                "id": "55555555-5555-5555-5555-555555555555",
                "format": "Vinyl",
                "position": 1,
                "track-count": 1,
                "disc-count": 1,
                "track-offset": 0,
                "tracks": [
                    {
                        "id": "66666666-6666-6666-6666-666666666666",
                        "title": "Test Recording",
                        "number": "1",
                        "position": 1,
                        "length": 1000,
                        "recording": {"id": RECORDING_MBID, "title": "Test Recording"}
                    }
                ]
            }
        ]
    })
}

/// Mounts the `MusicBrainz` ws/2 endpoints the metadata fetch reads.
async fn mount_musicbrainz(server: &wiremock::MockServer) {
    wiremock::Mock::given(wiremock::matchers::method("GET"))
        .and(wiremock::matchers::path(format!("/ws/2/recording/{RECORDING_MBID}")))
        .respond_with(wiremock::ResponseTemplate::new(200).set_body_json(recording_json()))
        .mount(server)
        .await;
    wiremock::Mock::given(wiremock::matchers::method("GET"))
        .and(wiremock::matchers::path(format!("/ws/2/release/{RELEASE_MBID}")))
        .respond_with(wiremock::ResponseTemplate::new(200).set_body_json(release_json()))
        .mount(server)
        .await;
}

/// Builds the media document whose single step is the tagging step under test.
///
/// Every value the fetch needs is a step option, which is where a user sets it:
/// the endpoints point at the local mock host, the recording MBID skips the
/// fingerprint path, and images are off so the run makes exactly the two
/// requests the mock host serves.
fn tagging_document(musicbrainz_endpoint: &str, source_hash: mediapm_cas::Hash) -> MediaPmDocument {
    let output_variant = serde_json::from_value::<OutputVariantValue>(json!({
        "kind": "output_content",
        "save": true,
    }))
    .expect("the generic output variant shape must decode");
    let option = |value: &str| TransformInputValue::String(value.to_string());
    MediaPmDocument {
        media: BTreeMap::from([(
            MEDIA_ID.to_string(),
            MediaSourceSpec {
                // The media's own variant is the seeded source, so the
                // synthesized step binds its content input to that hash — what
                // a real library looks like, even though this run drops the
                // binding below.
                variant_hashes: BTreeMap::from([("default".to_string(), source_hash.to_string())]),
                steps: vec![MediaStep {
                    tool: MediaStepTool::MediaTagger,
                    input_variants: vec!["default".to_string()],
                    output_variants: BTreeMap::from([(VARIANT.to_string(), output_variant)]),
                    options: BTreeMap::from([
                        ("recording_mbid".to_string(), option(RECORDING_MBID)),
                        ("musicbrainz_endpoint".to_string(), option(musicbrainz_endpoint)),
                        ("write_all_images".to_string(), option("false")),
                    ]),
                }],
                ..MediaSourceSpec::default()
            },
        )]),
        ..MediaPmDocument::default()
    }
}

/// A generated-document tool entry the planner resolves a tool by name through.
///
/// The planner reads neither tool's command, so the entries exist only to be
/// found: the run document below supplies the definition the step executes
/// against.
fn resolvable_tool(name: &str) -> ToolSpec {
    ToolSpec {
        version: None,
        name: name.to_string(),
        kind: mediapm_conductor::ToolKindSpec::default(),
        ..ToolSpec::default()
    }
}

/// The conductor document the run executes.
///
/// The media-tagger step is the synthesized one, with two documented changes.
///
/// Its `content` binding is dropped, because the only thing the pipeline does
/// with those bytes is hand them to ffmpeg, and a test that needs a decoder on
/// the machine is not hermetic. The staging itself is covered where it happens
/// (`builtins::media_tagger::host`); what this module is about is where the step
/// is dispatched and what reaches the captures.
///
/// The ffmpeg apply step the synthesizer pairs it with is absent: the run
/// document holds the tagging step alone, and the apply half is covered by
/// `tests_media_tagger_apply`.
fn run_document(
    document: &MediaPmDocument,
    source_hash: mediapm_cas::Hash,
    cache_dir: &std::path::Path,
) -> NickelDocument {
    let generated = NickelDocument {
        tools: BTreeMap::from([
            ("media-tagger".to_string(), resolvable_tool("media-tagger")),
            ("ffmpeg@stub".to_string(), resolvable_tool("ffmpeg")),
        ]),
        ..NickelDocument::default()
    };
    let plan = build_media_workflow_plan_with_limits(
        document,
        &generated,
        crate::conductor_bridge::tool_runtime::FfmpegSlotLimits::default(),
        cache_dir,
    )
    .expect("the plan must synthesize");
    let steps = &plan.workflows[&managed_workflow_name(MEDIA_ID)].steps;
    let mut metadata_step = steps.first().expect("a metadata step").clone();
    assert_eq!(
        steps.len(),
        2,
        "a media-tagger step synthesizes the tagging step plus its ffmpeg apply step",
    );
    assert_eq!(
        metadata_step.tool, "media-tagger",
        "the first step runs the tagger, got '{}'",
        metadata_step.tool,
    );
    metadata_step.inputs.remove(crate::conductor_bridge::constants::INPUT_CONTENT);

    let (tagger_spec, tagger_runtime) = build_media_tagger_spec();
    // The runtime map is what a sync stamps on the entry it writes: a host
    // builtin holds no payload, so nothing here names a content root.
    assert!(tagger_runtime.content_map.is_empty(), "a host builtin holds no payload");
    let mut doc = NickelDocument {
        tools: BTreeMap::from([("media-tagger".to_string(), tagger_spec)]),
        workflows: vec![WorkflowSpec {
            name: "default".to_string(),
            display_name: None,
            description: None,
            impure: false,
            steps: vec![metadata_step],
        }],
        ..NickelDocument::default()
    };
    doc.external_data.insert(
        source_hash,
        ExternalDataEntry { description: None, save_mode: OutputSaveMode::Saved },
    );
    doc
}

/// Reads back the `FFmetadata` bytes the tagging step captured.
///
/// The instance is found by the capture name, which is the name the downstream
/// apply step binds, so a step that stopped declaring it fails here rather than
/// in a later stage.
async fn captured_content(conductor: &Conductor<mediapm_cas::InMemoryCas>) -> Bytes {
    let state = conductor.get_state().expect("the run must record state");
    let instance = state
        .tool_call_instances
        .values()
        .find(|instance| instance.outputs.contains_key(OUTPUT_CONTENT))
        .expect("the tagging step must record its captured content");
    let captured = instance.outputs.get(OUTPUT_CONTENT).expect("content");
    conductor.cas().get(captured.hash).await.expect("the captured content must be in the store")
}

/// Runs the synthesized tagging step and returns the document it captured.
async fn run_tagging_step() -> String {
    let workspace = mediapm_utils::temp::artifact_dir().expect("artifact dir");
    let musicbrainz = wiremock::MockServer::start().await;
    mount_musicbrainz(&musicbrainz).await;

    let conductor =
        Conductor::new(RuntimeStoragePaths::new(workspace.path()), mediapm_cas::InMemoryCas::new());
    let source_hash = conductor
        .cas()
        .put(Bytes::from_static(b"host-builtin tagging source bytes"))
        .await
        .expect("the source bytes must be storable");

    let endpoint = format!("{}/ws/2", musicbrainz.uri());
    let document = tagging_document(&endpoint, source_hash);
    let cache_dir = workspace.path().join("cache").join("media_tagger");
    let doc = run_document(&document, source_hash, &cache_dir);
    std::fs::write(
        workspace.path().join("conductor.ncl"),
        mediapm_conductor::config::versions::encode_document(doc).expect("encode the document"),
    )
    .expect("write the conductor document");

    let options = conductor_run_workflow_options(&MediaRuntimeStorage::default(), None, None)
        .expect("the host builtins must register");
    assert!(
        options.host_builtins.get("media-tagger").is_some(),
        "every run must carry the media-tagger runner, or a library that tags cannot sync",
    );

    let summary = conductor
        .run_workflow("default", options)
        .await
        .expect("the tagging step must run with no binary on disk");
    assert_eq!(
        (summary.executed_steps, summary.failed_steps),
        (1, 0),
        "the tagging step runs once and succeeds: {summary:?}",
    );

    let text = String::from_utf8(captured_content(&conductor).await.to_vec())
        .expect("the captured document is UTF-8");
    conductor.shutdown().await.expect("conductor shutdown");
    text
}

/// The tagging step runs to success with no binary on disk.
///
/// The user-level guarantee: a library that tags does not need a `mediapm`
/// binary beside it. The step declares no command parts, so the conductor
/// dispatches it to the host runner instead of spawning anything, and the
/// document the run captures carries the tags the mock host served. Whether a
/// binary happens to sit next to this test binary is not something the run may
/// consult, which is why the step is synthesized here rather than hand-built.
#[tokio::test]
async fn the_tagging_step_tags_in_process() {
    let document = run_tagging_step().await;

    assert!(
        document.contains("title=Test Recording"),
        "the captured document must carry the title the host served, got: {document}",
    );
    assert!(
        document.contains(&format!("MUSICBRAINZ_RECORDINGID={RECORDING_MBID}")),
        "the recording id the run resolved must reach the document, got: {document}",
    );
}
