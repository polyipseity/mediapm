//! End-to-end online demo that exercises managed tool provisioning.
//!
//! This example is intentionally **compile-only** in automated test/CI workflows
//! (`test = false` in `Cargo.toml`). Runtime execution depends on external
//! network/tool provider availability.
//!
//! Runtime flow:
//! 1. Creates a clean artifact workspace under `examples/.artifacts/demo-online/`.
//! 2. Declares managed downloader + transform tools in `mediapm.ncl`.
//! 3. Declares one media workflow that processes
//!    `https://www.youtube.com/watch?v=dQw4w9WgXcQ` through:
//!    `yt-dlp -> ffmpeg -> ffmpeg -> rsgain -> media-tagger`.
//! 4. Runs full `mediapm sync` through
//!    `MediaPmService::sync_library_with_tag_update_checks`, which reconciles
//!    tools and executes managed workflows.
//! 5. Verifies all managed tools are registered with executable command
//!    selectors and non-empty content maps.
//! 6. Verifies the managed demo workflow exists and keeps the expected
//!    platform-specific tool sequence.
//! 7. Verifies the demo video output and tagged-video output are materialized
//!    under one metadata-interpolated hierarchy root.
//! 8. Writes a compact `manifest.json` with generated paths and sync metadata.

use std::collections::BTreeMap;
use std::error::Error;
use std::fs;
use std::path::{Path, PathBuf};
use std::process;
use std::thread;
use std::time::{Duration, SystemTime, UNIX_EPOCH};

use mediapm::{
    HierarchyEntry, MediaMetadataValue, MediaMetadataVariantBinding, MediaPmService,
    MediaSourceSpec, MediaStep, MediaStepTool, ToolRequirement, TransformInputValue,
    load_mediapm_document, save_mediapm_document,
};
use mediapm_conductor::{MachineNickelDocument, ToolKindSpec, decode_machine_document};
use serde::Serialize;
use serde_json::json;

/// Stable media id used by the online demo media workflow.
const DEMO_MEDIA_ID: &str = "rickroll";
/// Online source URI processed by the demo workflow.
const DEMO_SOURCE_URI: &str = "https://www.youtube.com/watch?v=dQw4w9WgXcQ";
/// Human-facing description mirrored into managed workflow metadata.
const DEMO_WORKFLOW_DESCRIPTION: &str = "Online demo pipeline downloading video + sidecars, transcoding, loudness-normalizing, and applying metadata";

/// Metadata title value used in hierarchy interpolation.
const DEMO_METADATA_TITLE: &str = "Rickroll Demo";

/// Metadata key used for variant-backed hierarchy interpolation.
const DEMO_METADATA_VIDEO_ID_KEY: &str = "id";

/// Expected interpolated video-id value for the demo source URL.
const DEMO_EXPECTED_VIDEO_ID: &str = "dQw4w9WgXcQ";

/// Returns final variant key materialized into the demo hierarchy path.
fn final_demo_output_variant() -> &'static str {
    "video_144p"
}

/// Relative output path for transcoded demo video.
const DEMO_VIDEO_RELATIVE_PATH: &str = "demo/Rickroll Demo/dQw4w9WgXcQ/rickroll-144p.mp4";

/// Relative output path for loudness-normalized + tagged demo video.
const DEMO_TAGGED_VIDEO_RELATIVE_PATH: &str =
    "demo/Rickroll Demo/dQw4w9WgXcQ/rickroll-144p-tagged.mp4";

/// Hierarchy-root template that demonstrates metadata interpolation.
const DEMO_HIERARCHY_ROOT_TEMPLATE: &str =
    "demo/${media.metadata.title}/${media.metadata.video_id}";

/// Hierarchy-root value expected after metadata interpolation.
const DEMO_HIERARCHY_ROOT_RESOLVED: &str = "demo/Rickroll Demo/dQw4w9WgXcQ";

/// Named sidecar variants and suffixes materialized from yt-dlp artifacts.
///
/// This demo intentionally materializes all currently supported yt-dlp
/// sidecar families so hierarchy/output-policy behavior is exercised end to
/// end for file + folder captures.
const DEMO_SIDECAR_VARIANT_SUFFIXES: [(&str, &str); 12] = [
    ("subtitles", "subtitles/"),
    ("auto_subtitles", "auto-subtitles/"),
    ("thumbnails", "thumbnails/"),
    ("description", "description.txt"),
    ("infojson", "info.json"),
    ("comments", "comments/"),
    ("links", "links/"),
    ("chapters", "chapters/"),
    ("playlist_video", "playlist-video/"),
    ("playlist_thumbnails", "playlist-thumbnails/"),
    ("playlist_description", "playlist-description/"),
    ("playlist_infojson", "playlist-infojson/"),
];

/// Expected yt-dlp step fan-out count:
/// one primary media variant (`video`) plus one step for each
/// configured sidecar variant.
const DEMO_EXPECTED_YT_DLP_STEP_COUNT: usize = DEMO_SIDECAR_VARIANT_SUFFIXES.len() + 1;

/// Shared result type for this online example.
type ExampleResult<T> = Result<T, Box<dyn Error>>;

/// Runtime manifest persisted under the demo artifact root.
#[derive(Debug, Clone, PartialEq, Eq, Serialize)]
struct DemoManifest {
    /// Unix timestamp (seconds) when this run completed.
    generated_unix_epoch_seconds: u64,
    /// Root directory that contains all generated example files.
    artifact_root: String,
    /// Workspace root used by `MediaPmService`.
    workspace_root: String,
    /// Resolved immutable tool ids observed in machine config.
    tool_ids: Vec<String>,
    /// Tool executable paths resolved from conductor machine document.
    tool_binaries: BTreeMap<String, String>,
    /// Enforced yt-dlp concurrency setting observed in machine config.
    yt_dlp_max_concurrent_calls: i32,
    /// Enforced yt-dlp retry setting observed in machine config.
    yt_dlp_max_retries: i32,
    /// Path to `mediapm.ncl` used by this run.
    mediapm_ncl_path: String,
    /// Path to `mediapm.conductor.machine.ncl` used by this run.
    conductor_machine_ncl_path: String,
    /// Managed workflow id synthesized for this demo source.
    workflow_id: String,
    /// Number of steps in the managed demo workflow.
    workflow_step_count: usize,
    /// Materialized library path for the transcoded demo video variant.
    materialized_demo_video_path: String,
    /// Materialized library path for the normalized + tagged video variant.
    materialized_demo_tagged_video_path: String,
    /// Materialized library paths for downloader sidecar artifact variants.
    materialized_demo_sidecar_paths: BTreeMap<String, String>,
    /// Number of workflow instances executed during sync.
    executed_instances: usize,
    /// Number of workflow instances served from cache.
    cached_instances: usize,
    /// Number of workflow instances rematerialized from cache metadata.
    rematerialized_instances: usize,
    /// Number of hierarchy paths materialized by sync.
    materialized_paths: usize,
    /// Number of stale hierarchy paths removed by sync.
    removed_paths: usize,
    /// Number of tool ids added during this sync run.
    added_tools: usize,
    /// Number of tool ids updated during this sync run.
    updated_tools: usize,
    /// Number of non-fatal warnings reported by full sync.
    warning_count: usize,
}

/// Stable path bundle returned by one successful online demo run.
#[derive(Debug, Clone, PartialEq, Eq)]
struct DemoRunPaths {
    /// Artifact root used by this run.
    artifact_root: PathBuf,
    /// Workspace root used by this run.
    workspace_root: PathBuf,
    /// Manifest path written at the end of the run.
    manifest_path: PathBuf,
}

/// Returns deterministic artifact root for this online demo.
fn artifact_root() -> PathBuf {
    PathBuf::from(env!("CARGO_MANIFEST_DIR"))
        .join("examples")
        .join(".artifacts")
        .join("demo-online")
}

/// Returns current Unix timestamp in seconds.
fn unix_timestamp_seconds() -> u64 {
    SystemTime::now().duration_since(UNIX_EPOCH).map(|duration| duration.as_secs()).unwrap_or(0)
}

/// Converts one path to slash-normalized display text.
fn display_path(path: &Path) -> String {
    path.display().to_string().replace('\\', "/")
}

/// Recreates artifact root for deterministic outputs on each run.
fn reset_artifact_root() -> ExampleResult<PathBuf> {
    let root = artifact_root();
    if root.exists()
        && let Err(error) = remove_dir_all_with_retry(&root)
    {
        if is_share_violation_remove_error(error.as_ref()) {
            return prepare_fallback_artifact_root(&root);
        }

        return Err(error);
    }
    fs::create_dir_all(&root)?;
    Ok(root)
}

/// Returns true when one cleanup error is consistent with transient
/// Windows-style sharing violations.
fn is_share_violation_remove_error(error: &(dyn Error + 'static)) -> bool {
    error.downcast_ref::<std::io::Error>().is_some_and(|io_error| {
        io_error.kind() == std::io::ErrorKind::PermissionDenied
            || io_error.raw_os_error() == Some(32)
    })
}

/// Creates one unique fallback artifact root when the canonical path is
/// temporarily locked by an external process.
fn prepare_fallback_artifact_root(canonical_root: &Path) -> ExampleResult<PathBuf> {
    let suffix = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .map(|duration| duration.as_nanos())
        .unwrap_or(0);
    let fallback_root =
        canonical_root.with_file_name(format!("demo-online-fallback-{}-{}", process::id(), suffix));

    fs::create_dir_all(&fallback_root)?;
    Ok(fallback_root)
}

/// Removes one directory with retries to tolerate transient Windows locks.
fn remove_dir_all_with_retry(path: &Path) -> ExampleResult<()> {
    const ATTEMPTS: usize = 6;
    const BACKOFF_MS: u64 = 40;

    let mut last_error: Option<std::io::Error> = None;

    for attempt in 0..ATTEMPTS {
        match fs::remove_dir_all(path) {
            Ok(()) => return Ok(()),
            Err(error) => {
                let retryable = error.kind() == std::io::ErrorKind::PermissionDenied
                    || error.raw_os_error() == Some(32);
                last_error = Some(error);
                if !retryable || attempt + 1 == ATTEMPTS {
                    break;
                }
                clear_readonly_bits_recursively(path);
                thread::sleep(Duration::from_millis(BACKOFF_MS));
            }
        }
    }

    match last_error {
        Some(error) => Err(Box::new(error)),
        None => Ok(()),
    }
}

/// Best-effort recursive readonly-bit clearing for Windows cleanup retries.
///
/// Some tool archives mark extracted files/directories as readonly, which can
/// make `remove_dir_all` fail with `PermissionDenied` on repeat demo runs.
/// Clearing readonly flags before the next retry keeps cleanup deterministic
/// without failing when metadata probing itself encounters transient locks.
#[cfg_attr(windows, allow(clippy::permissions_set_readonly_false))]
fn clear_readonly_bits_recursively(path: &Path) {
    #[cfg(not(windows))]
    {
        let _ = path;
        return;
    }

    #[cfg(windows)]
    {
        if !path.exists() {
            return;
        }

        let mut stack = vec![path.to_path_buf()];
        while let Some(next) = stack.pop() {
            if let Ok(metadata) = fs::metadata(&next) {
                let mut permissions = metadata.permissions();
                if permissions.readonly() {
                    permissions.set_readonly(false);
                    let _ = fs::set_permissions(&next, permissions);
                }
            }

            if next.is_dir()
                && let Ok(entries) = fs::read_dir(&next)
            {
                for entry in entries.flatten() {
                    stack.push(entry.path());
                }
            }
        }
    }
}

/// Writes one serializable value as pretty JSON, creating parent dirs as needed.
fn write_json_file<T>(path: &Path, value: &T) -> ExampleResult<()>
where
    T: Serialize,
{
    if let Some(parent) = path.parent() {
        fs::create_dir_all(parent)?;
    }
    fs::write(path, serde_json::to_vec_pretty(value)?)?;
    Ok(())
}

/// Seeds one `mediapm.ncl` document with managed tools + media workflow.
fn configure_document_for_online_demo(workspace_root: &Path) -> ExampleResult<Vec<String>> {
    let mediapm_ncl = workspace_root.join("mediapm.ncl");
    let mut document = load_mediapm_document(&mediapm_ncl)?;
    document.tools = BTreeMap::from([
        (
            "yt-dlp".to_string(),
            ToolRequirement {
                version: None,
                tag: Some("latest".to_string()),
                recheck_seconds: None,
                max_input_slots: None,
                max_output_slots: None,
            },
        ),
        (
            "ffmpeg".to_string(),
            ToolRequirement {
                version: None,
                tag: Some("latest".to_string()),
                recheck_seconds: None,
                max_input_slots: None,
                max_output_slots: None,
            },
        ),
        (
            "rsgain".to_string(),
            ToolRequirement {
                version: None,
                tag: Some("latest".to_string()),
                recheck_seconds: None,
                max_input_slots: None,
                max_output_slots: None,
            },
        ),
        (
            "media-tagger".to_string(),
            ToolRequirement {
                version: None,
                tag: Some("latest".to_string()),
                recheck_seconds: None,
                max_input_slots: None,
                max_output_slots: None,
            },
        ),
    ]);

    let steps = vec![
        MediaStep {
            tool: MediaStepTool::YtDlp,
            input_variants: Vec::new(),
            output_variants: BTreeMap::from([
                (
                    "video".to_string(),
                    json!({
                        "kind": "primary",
                        "save": true,
                        "save_full": true,
                        "format": "best"
                    }),
                ),
                (
                    "thumbnails".to_string(),
                    json!({
                        "kind": "thumbnail",
                    }),
                ),
                (
                    "subtitles".to_string(),
                    json!({
                        "kind": "subtitle",
                    }),
                ),
                (
                    "auto_subtitles".to_string(),
                    json!({
                        "kind": "auto_subtitle",
                    }),
                ),
                (
                    "description".to_string(),
                    json!({
                        "kind": "description",
                        "save_full": true,
                    }),
                ),
                (
                    "infojson".to_string(),
                    json!({
                        "kind": "infojson",
                        "save_full": true,
                    }),
                ),
                (
                    "comments".to_string(),
                    json!({
                        "kind": "comments",
                    }),
                ),
                (
                    "links".to_string(),
                    json!({
                        "kind": "link",
                    }),
                ),
                (
                    "chapters".to_string(),
                    json!({
                        "kind": "chapter",
                    }),
                ),
                (
                    "playlist_video".to_string(),
                    json!({
                        "kind": "playlist_video",
                    }),
                ),
                (
                    "playlist_thumbnails".to_string(),
                    json!({
                        "kind": "playlist_thumbnail",
                    }),
                ),
                (
                    "playlist_description".to_string(),
                    json!({
                        "kind": "playlist_description",
                    }),
                ),
                (
                    "playlist_infojson".to_string(),
                    json!({
                        "kind": "playlist_infojson",
                    }),
                ),
            ]),
            options: BTreeMap::from([
                ("uri".to_string(), TransformInputValue::String(DEMO_SOURCE_URI.to_string())),
                ("no_playlist".to_string(), TransformInputValue::String("true".to_string())),
                (
                    "extractor_args".to_string(),
                    TransformInputValue::String("youtube:max_comments=20".to_string()),
                ),
            ]),
        },
        MediaStep {
            tool: MediaStepTool::Ffmpeg,
            input_variants: vec!["video".to_string()],
            output_variants: BTreeMap::from([(
                "video_144p".to_string(),
                json!({ "kind": "output_content", "idx": 0, "save": true, "save_full": true }),
            )]),
            options: BTreeMap::from([
                ("container".to_string(), TransformInputValue::String("mp4".to_string())),
                ("vn".to_string(), TransformInputValue::String("false".to_string())),
                ("video_codec".to_string(), TransformInputValue::String("libx264".to_string())),
                ("audio_codec".to_string(), TransformInputValue::String("aac".to_string())),
                (
                    "video_filters".to_string(),
                    TransformInputValue::String("scale=-2:144".to_string()),
                ),
                ("preset".to_string(), TransformInputValue::String("medium".to_string())),
                ("crf".to_string(), TransformInputValue::String("23".to_string())),
                ("movflags".to_string(), TransformInputValue::String("+faststart".to_string())),
            ]),
        },
        MediaStep {
            tool: MediaStepTool::Ffmpeg,
            input_variants: vec!["video_144p".to_string()],
            output_variants: BTreeMap::from([(
                "audio_mp3".to_string(),
                json!({ "kind": "output_content", "idx": 0, "save": true, "save_full": true }),
            )]),
            options: BTreeMap::from([
                ("container".to_string(), TransformInputValue::String("mp3".to_string())),
                ("vn".to_string(), TransformInputValue::String("true".to_string())),
                ("audio_codec".to_string(), TransformInputValue::String("libmp3lame".to_string())),
                ("audio_quality".to_string(), TransformInputValue::String("2".to_string())),
                ("map_metadata".to_string(), TransformInputValue::String("0".to_string())),
            ]),
        },
        MediaStep {
            tool: MediaStepTool::Rsgain,
            input_variants: vec!["audio_mp3".to_string()],
            output_variants: BTreeMap::from([(
                "audio_normalized".to_string(),
                json!({ "kind": "output_content", "save": true, "save_full": true }),
            )]),
            options: BTreeMap::from([(
                "tagmode".to_string(),
                TransformInputValue::String("i".to_string()),
            )]),
        },
        MediaStep {
            tool: MediaStepTool::MediaTagger,
            input_variants: vec!["video_144p".to_string()],
            output_variants: BTreeMap::from([(
                "tagged".to_string(),
                json!({ "kind": "output_content", "save": true, "save_full": true }),
            )]),
            options: BTreeMap::from([
                (
                    "strict_identification".to_string(),
                    TransformInputValue::String("true".to_string()),
                ),
                (
                    "recording_mbid".to_string(),
                    TransformInputValue::String("8f3471b5-7e6a-48da-86a9-c1c07a0f47ae".to_string()),
                ),
                ("output_container".to_string(), TransformInputValue::String("mp4".to_string())),
            ]),
        },
    ];

    document.media = BTreeMap::from([(
        DEMO_MEDIA_ID.to_string(),
        MediaSourceSpec {
            description: Some(DEMO_WORKFLOW_DESCRIPTION.to_string()),
            workflow_id: None,
            metadata: Some(BTreeMap::from([
                ("title".to_string(), MediaMetadataValue::Literal(DEMO_METADATA_TITLE.to_string())),
                (
                    "video_id".to_string(),
                    MediaMetadataValue::Variant(MediaMetadataVariantBinding {
                        variant: "infojson".to_string(),
                        metadata_key: DEMO_METADATA_VIDEO_ID_KEY.to_string(),
                    }),
                ),
            ])),
            variant_hashes: BTreeMap::new(),
            steps,
        },
    )]);
    document.hierarchy = BTreeMap::from([
        (
            format!("{DEMO_HIERARCHY_ROOT_TEMPLATE}/rickroll-144p.mp4"),
            HierarchyEntry {
                media_id: DEMO_MEDIA_ID.to_string(),
                variants: vec![final_demo_output_variant().to_string()],
            },
        ),
        (
            format!("{DEMO_HIERARCHY_ROOT_TEMPLATE}/rickroll-144p-tagged.mp4"),
            HierarchyEntry {
                media_id: DEMO_MEDIA_ID.to_string(),
                variants: vec!["tagged".to_string()],
            },
        ),
    ])
    .into_iter()
    .chain(DEMO_SIDECAR_VARIANT_SUFFIXES.into_iter().map(|(variant, suffix)| {
        (
            format!("{DEMO_HIERARCHY_ROOT_TEMPLATE}/{suffix}"),
            HierarchyEntry {
                media_id: DEMO_MEDIA_ID.to_string(),
                variants: vec![variant.to_string()],
            },
        )
    }))
    .collect();

    save_mediapm_document(&mediapm_ncl, &document)?;
    Ok(vec![
        "yt-dlp".to_string(),
        "ffmpeg".to_string(),
        "rsgain".to_string(),
        "media-tagger".to_string(),
    ])
}

/// Loads conductor machine document from one path.
fn load_machine(path: &Path) -> ExampleResult<MachineNickelDocument> {
    let raw = fs::read_to_string(path)?;
    Ok(decode_machine_document(raw.as_bytes())?)
}

/// Resolves managed tool binary paths by logical tool names.
///
/// The machine document stores immutable ids (for example
/// `mediapm.tools.yt-dlp+github-releases@latest`).
/// This helper accepts logical ids declared in `mediapm.ncl` (for example
/// `yt-dlp`) and resolves each one to exactly one immutable id.
fn resolve_tool_binaries(
    machine: &MachineNickelDocument,
    tool_ids: &[String],
) -> ExampleResult<BTreeMap<String, String>> {
    let mut binaries = BTreeMap::new();

    for tool_id in tool_ids {
        let resolved_tool_id = resolve_managed_tool_id(machine, tool_id)?;

        let spec = machine.tools.get(&resolved_tool_id).ok_or_else(|| {
            format!("machine config is missing tool spec for '{resolved_tool_id}'")
        })?;

        let ToolKindSpec::Executable { command, .. } = &spec.kind else {
            return Err(
                format!("tool '{resolved_tool_id}' is not executable in machine config").into()
            );
        };
        let Some(first) = command.first() else {
            return Err(format!("tool '{resolved_tool_id}' has an empty command vector").into());
        };

        if first.trim().is_empty() {
            return Err(
                format!("tool '{resolved_tool_id}' command selector must be non-empty").into()
            );
        }

        let has_content_map = machine
            .tool_configs
            .get(&resolved_tool_id)
            .and_then(|config| config.content_map.as_ref())
            .is_some_and(|map| !map.is_empty());
        if !has_content_map {
            return Err(format!(
                "tool '{resolved_tool_id}' is missing content_map payload entries"
            )
            .into());
        }

        binaries.insert(resolved_tool_id, first.to_string());
    }

    Ok(binaries)
}

/// Reads yt-dlp max concurrency from machine config and enforces policy value `1`.
fn assert_yt_dlp_concurrency_policy(
    machine: &MachineNickelDocument,
    yt_dlp_tool_id: &str,
) -> ExampleResult<i32> {
    let observed = machine
        .tool_configs
        .get(yt_dlp_tool_id)
        .map(|config| config.max_concurrent_calls)
        .unwrap_or(-1);

    if observed != 1 {
        return Err(format!(
            "yt-dlp default max_concurrent_calls must be 1 but observed {observed} for tool '{yt_dlp_tool_id}'"
        )
        .into());
    }

    Ok(observed)
}

/// Reads yt-dlp retry policy from machine config and enforces value `1`.
fn assert_yt_dlp_retry_policy(
    machine: &MachineNickelDocument,
    yt_dlp_tool_id: &str,
) -> ExampleResult<i32> {
    let observed =
        machine.tool_configs.get(yt_dlp_tool_id).map(|config| config.max_retries).unwrap_or(-1);

    if observed != 1 {
        return Err(format!(
            "yt-dlp default max_retries must be 1 but observed {observed} for tool '{yt_dlp_tool_id}'"
        )
        .into());
    }

    Ok(observed)
}

/// Verifies the managed demo workflow exists and keeps the expected tool flow.
fn assert_demo_workflow_shape(machine: &MachineNickelDocument) -> ExampleResult<(String, usize)> {
    let workflow_id = format!("mediapm.media.{DEMO_MEDIA_ID}");
    let workflow = machine
        .workflows
        .get(&workflow_id)
        .ok_or_else(|| format!("machine config is missing managed workflow '{workflow_id}'"))?;

    if workflow.name.as_deref() != Some(DEMO_MEDIA_ID) {
        return Err(format!(
            "managed workflow '{workflow_id}' must set name='{}' but observed {:?}",
            DEMO_MEDIA_ID, workflow.name
        )
        .into());
    }

    if workflow.description.as_deref() != Some(DEMO_WORKFLOW_DESCRIPTION) {
        return Err(format!(
            "managed workflow '{workflow_id}' must mirror description='{}' but observed {:?}",
            DEMO_WORKFLOW_DESCRIPTION, workflow.description
        )
        .into());
    }

    let expected_total_steps = DEMO_EXPECTED_YT_DLP_STEP_COUNT + 5;
    if workflow.steps.len() != expected_total_steps {
        return Err(format!(
            "managed workflow '{workflow_id}' must contain {} steps but has {}",
            expected_total_steps,
            workflow.steps.len()
        )
        .into());
    }

    for index in 0..DEMO_EXPECTED_YT_DLP_STEP_COUNT {
        let actual = &workflow.steps[index].tool;
        let actual_logical = logical_name_from_managed_tool_id(actual).ok_or_else(|| {
            format!(
                "managed workflow '{workflow_id}' step #{index} uses non-managed tool id '{actual}'"
            )
        })?;
        if actual_logical != "yt-dlp" {
            return Err(format!(
                "managed workflow '{workflow_id}' step #{index} expected tool 'yt-dlp' but observed '{actual}'"
            )
            .into());
        }
    }

    let ffmpeg_video_step_index = DEMO_EXPECTED_YT_DLP_STEP_COUNT;
    let ffmpeg_video_tool = &workflow.steps[ffmpeg_video_step_index].tool;
    let ffmpeg_video_logical = logical_name_from_managed_tool_id(ffmpeg_video_tool).ok_or_else(|| {
        format!(
            "managed workflow '{workflow_id}' step #{ffmpeg_video_step_index} uses non-managed tool id '{ffmpeg_video_tool}'"
        )
    })?;
    if ffmpeg_video_logical != "ffmpeg" {
        return Err(format!(
            "managed workflow '{workflow_id}' step #{ffmpeg_video_step_index} expected tool 'ffmpeg' but observed '{ffmpeg_video_tool}'"
        )
        .into());
    }

    let ffmpeg_audio_step_index = ffmpeg_video_step_index + 1;
    let ffmpeg_audio_tool = &workflow.steps[ffmpeg_audio_step_index].tool;
    let ffmpeg_audio_logical = logical_name_from_managed_tool_id(ffmpeg_audio_tool).ok_or_else(|| {
        format!(
            "managed workflow '{workflow_id}' step #{ffmpeg_audio_step_index} uses non-managed tool id '{ffmpeg_audio_tool}'"
        )
    })?;
    if ffmpeg_audio_logical != "ffmpeg" {
        return Err(format!(
            "managed workflow '{workflow_id}' step #{ffmpeg_audio_step_index} expected tool 'ffmpeg' but observed '{ffmpeg_audio_tool}'"
        )
        .into());
    }

    let rsgain_step_index = ffmpeg_video_step_index + 2;
    let rsgain_tool = &workflow.steps[rsgain_step_index].tool;
    let rsgain_logical = logical_name_from_managed_tool_id(rsgain_tool).ok_or_else(|| {
        format!(
            "managed workflow '{workflow_id}' step #{rsgain_step_index} uses non-managed tool id '{rsgain_tool}'"
        )
    })?;
    if rsgain_logical != "rsgain" {
        return Err(format!(
            "managed workflow '{workflow_id}' step #{rsgain_step_index} expected tool 'rsgain' but observed '{rsgain_tool}'"
        )
        .into());
    }

    let media_tagger_metadata_step_index = ffmpeg_video_step_index + 3;
    let media_tagger_metadata_tool = &workflow.steps[media_tagger_metadata_step_index].tool;
    let media_tagger_metadata_logical =
        logical_name_from_managed_tool_id(media_tagger_metadata_tool).ok_or_else(|| {
            format!(
                "managed workflow '{workflow_id}' step #{media_tagger_metadata_step_index} uses non-managed tool id '{media_tagger_metadata_tool}'"
            )
        })?;
    if media_tagger_metadata_logical != "media-tagger" {
        return Err(format!(
            "managed workflow '{workflow_id}' step #{media_tagger_metadata_step_index} expected tool 'media-tagger' but observed '{media_tagger_metadata_tool}'"
        )
        .into());
    }

    let media_tagger_apply_step_index = ffmpeg_video_step_index + 4;
    let media_tagger_apply_tool = &workflow.steps[media_tagger_apply_step_index].tool;
    let media_tagger_apply_logical =
        logical_name_from_managed_tool_id(media_tagger_apply_tool).ok_or_else(|| {
            format!(
                "managed workflow '{workflow_id}' step #{media_tagger_apply_step_index} uses non-managed tool id '{media_tagger_apply_tool}'"
            )
        })?;
    if media_tagger_apply_logical != "ffmpeg" {
        return Err(format!(
            "managed workflow '{workflow_id}' step #{media_tagger_apply_step_index} expected tool 'ffmpeg' but observed '{media_tagger_apply_tool}'"
        )
        .into());
    }

    Ok((workflow_id, workflow.steps.len()))
}

/// Returns true when file bytes look like an MP4/ISO BMFF file.
fn bytes_look_like_mp4(bytes: &[u8]) -> bool {
    bytes.len() >= 8 && &bytes[4..8] == b"ftyp"
}

/// Resolves and verifies the materialized demo outputs.
///
/// The demo expects one transcoded video and one tagged-video output under one
/// metadata-interpolated hierarchy root.
fn resolve_demo_output_paths(
    workspace_root: &Path,
) -> ExampleResult<(PathBuf, PathBuf, BTreeMap<String, PathBuf>)> {
    let interpolated_root = workspace_root.join(DEMO_HIERARCHY_ROOT_RESOLVED);
    if !interpolated_root.exists() {
        return Err(format!(
            "expected interpolated hierarchy root '{}' to exist after sync",
            interpolated_root.display()
        )
        .into());
    }

    let expected_suffix =
        PathBuf::from("demo").join(DEMO_METADATA_TITLE).join(DEMO_EXPECTED_VIDEO_ID);
    if !interpolated_root.ends_with(&expected_suffix) {
        return Err("demo metadata interpolation root invariants drifted".into());
    }

    let video_path = workspace_root.join(DEMO_VIDEO_RELATIVE_PATH);
    if !video_path.exists() {
        return Err(format!(
            "expected transcoded demo video '{}' to exist after sync",
            video_path.display()
        )
        .into());
    }

    let video_bytes = fs::read(&video_path)?;
    if !bytes_look_like_mp4(&video_bytes) {
        return Err(format!(
            "expected transcoded demo video '{}' to contain MP4 bytes",
            video_path.display()
        )
        .into());
    }

    let tagged_video_path = workspace_root.join(DEMO_TAGGED_VIDEO_RELATIVE_PATH);
    if !tagged_video_path.exists() {
        return Err(format!(
            "expected tagged demo video '{}' to exist after sync",
            tagged_video_path.display()
        )
        .into());
    }

    let tagged_video_bytes = fs::read(&tagged_video_path)?;
    if !bytes_look_like_mp4(&tagged_video_bytes) {
        return Err(format!(
            "expected tagged demo video '{}' to contain MP4 bytes",
            tagged_video_path.display()
        )
        .into());
    }

    let mut sidecar_paths = BTreeMap::new();
    for (variant, suffix) in DEMO_SIDECAR_VARIANT_SUFFIXES {
        let relative_path = format!("{DEMO_HIERARCHY_ROOT_RESOLVED}/{suffix}");
        let output_path = workspace_root.join(relative_path);
        if !output_path.exists() {
            return Err(format!(
                "expected demo sidecar variant '{}' at '{}' to exist after sync",
                variant,
                output_path.display()
            )
            .into());
        }

        if suffix.ends_with('/') {
            if !output_path.is_dir() {
                return Err(format!(
                    "expected demo sidecar variant '{}' at '{}' to be a directory",
                    variant,
                    output_path.display()
                )
                .into());
            }
        } else if !output_path.is_file() {
            return Err(format!(
                "expected demo sidecar variant '{}' at '{}' to be a file",
                variant,
                output_path.display()
            )
            .into());
        }

        sidecar_paths.insert(variant.to_string(), output_path);
    }

    Ok((video_path, tagged_video_path, sidecar_paths))
}

/// Executes the full online workflow and writes one artifact manifest.
async fn run_online_demo() -> ExampleResult<DemoRunPaths> {
    let root = reset_artifact_root()?;
    let workspace_root = root.clone();

    // Use the in-memory conductor facade for the demo entrypoint.
    //
    // `sync_tools*` opens the persistent runtime CAS store to import managed
    // tool payload bytes. If the demo also holds that same redb-backed store
    // open via `SimpleConductor<FileSystemCas>`, the second open attempt fails
    // with "Database already open. Cannot acquire lock.".
    //
    // The in-memory facade avoids that lock contention while still allowing
    // workflow execution to fall back to filesystem CAS when tool payload
    // hashes are only present in the runtime store.
    let service = MediaPmService::new_in_memory_at(&workspace_root);
    let logical_tool_ids = configure_document_for_online_demo(&workspace_root)?;

    let summary = service
        .sync_library_with_tag_update_checks(true)
        .await
        .map_err(|error| format!("online demo sync failed: {error}"))?;

    let machine = load_machine(&service.paths().conductor_machine_ncl)?;
    let tool_binaries = resolve_tool_binaries(&machine, &logical_tool_ids)?;

    let tool_ids = tool_binaries.keys().cloned().collect::<Vec<_>>();

    let yt_dlp_tool_id = resolve_managed_tool_id(&machine, "yt-dlp")?;
    let yt_dlp_max_concurrent_calls = assert_yt_dlp_concurrency_policy(&machine, &yt_dlp_tool_id)?;
    let yt_dlp_max_retries = assert_yt_dlp_retry_policy(&machine, &yt_dlp_tool_id)?;
    let (workflow_id, workflow_step_count) = assert_demo_workflow_shape(&machine)?;
    let (output_video_path, output_tagged_video_path, output_sidecar_paths) =
        resolve_demo_output_paths(&workspace_root)?;

    let _yt_dlp_binary =
        tool_binaries.get(&yt_dlp_tool_id).ok_or("yt-dlp command selector missing")?;

    let manifest = DemoManifest {
        generated_unix_epoch_seconds: unix_timestamp_seconds(),
        artifact_root: display_path(&root),
        workspace_root: display_path(&workspace_root),
        tool_ids,
        tool_binaries: tool_binaries
            .iter()
            .map(|(tool_id, command)| (tool_id.clone(), command.clone()))
            .collect(),
        yt_dlp_max_concurrent_calls,
        yt_dlp_max_retries,
        mediapm_ncl_path: display_path(&service.paths().mediapm_ncl),
        conductor_machine_ncl_path: display_path(&service.paths().conductor_machine_ncl),
        workflow_id,
        workflow_step_count,
        materialized_demo_video_path: display_path(&output_video_path),
        materialized_demo_tagged_video_path: display_path(&output_tagged_video_path),
        materialized_demo_sidecar_paths: output_sidecar_paths
            .iter()
            .map(|(variant, path)| (variant.clone(), display_path(path)))
            .collect(),
        executed_instances: summary.executed_instances,
        cached_instances: summary.cached_instances,
        rematerialized_instances: summary.rematerialized_instances,
        materialized_paths: summary.materialized_paths,
        removed_paths: summary.removed_paths,
        added_tools: summary.added_tools,
        updated_tools: summary.updated_tools,
        warning_count: summary.warnings.len(),
    };

    let manifest_path = root.join("manifest.json");
    write_json_file(&manifest_path, &manifest)?;

    Ok(DemoRunPaths { artifact_root: root, workspace_root, manifest_path })
}

/// Extracts logical tool name from one immutable managed tool id.
fn logical_name_from_managed_tool_id(tool_id: &str) -> Option<&str> {
    let (selector, _) = tool_id.split_once('@')?;
    let selector = selector.strip_prefix("mediapm.tools.")?;
    let (logical_name, _) = selector.split_once('+')?;
    (!logical_name.is_empty()).then_some(logical_name)
}

/// Resolves exactly one immutable managed tool id for one logical name.
fn resolve_managed_tool_id(
    machine: &MachineNickelDocument,
    logical_name: &str,
) -> ExampleResult<String> {
    let mut matches = machine
        .tools
        .keys()
        .filter(|candidate| {
            logical_name_from_managed_tool_id(candidate).is_some_and(|name| name == logical_name)
        })
        .cloned();

    let Some(first) = matches.next() else {
        return Err(format!(
            "machine config is missing immutable managed tool id for logical tool '{logical_name}'"
        )
        .into());
    };

    if matches.next().is_some() {
        return Err(format!(
            "machine config has multiple immutable managed tool ids for logical tool '{logical_name}'"
        )
        .into());
    }

    Ok(first)
}

#[tokio::main]
/// Runs the online sync demo and prints generated artifact paths.
async fn main() -> ExampleResult<()> {
    let paths = run_online_demo().await?;

    println!("generated artifacts root: {}", paths.artifact_root.display());
    println!("generated workspace root: {}", paths.workspace_root.display());
    println!("manifest: {}", paths.manifest_path.display());

    Ok(())
}

#[cfg(test)]
mod tests {
    use std::fs;

    use super::{
        artifact_root, clear_readonly_bits_recursively, display_path, remove_dir_all_with_retry,
    };

    /// Ensures artifact root remains stable for docs/scripts that reference it.
    #[test]
    fn artifact_root_is_stable() {
        let text = display_path(&artifact_root());
        assert!(text.ends_with("src/mediapm/examples/.artifacts/demo-online"));
    }

    /// Ensures cleanup helpers can remove readonly-marked trees created by
    /// prior tool downloads on repeated demo runs.
    #[test]
    fn remove_dir_all_with_retry_handles_readonly_tree() {
        let temp = tempfile::tempdir().expect("tempdir");
        let tree_root = temp.path().join("readonly-tree");
        fs::create_dir_all(&tree_root).expect("create tree root");

        let nested = tree_root.join("nested").join("tool.bin");
        fs::create_dir_all(nested.parent().expect("parent")).expect("create nested parent");
        fs::write(&nested, b"demo").expect("write nested file");

        let mut file_permissions = fs::metadata(&nested).expect("metadata").permissions();
        file_permissions.set_readonly(true);
        fs::set_permissions(&nested, file_permissions).expect("set readonly on file");

        clear_readonly_bits_recursively(&tree_root);
        remove_dir_all_with_retry(&tree_root).expect("retrying remove should succeed");
        assert!(!tree_root.exists());
    }
}
