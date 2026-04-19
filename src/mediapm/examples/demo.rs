//! Persistent `mediapm` demo that produces inspectable artifacts.
//!
//! This example demonstrates a fully local ingest + transform flow:
//! - loads one bundled binary MP4 fixture,
//! - imports fixture bytes into the workspace CAS,
//! - defines one managed workflow chain
//!   (`import-once -> ffmpeg -> rsgain -> media-tagger`),
//! - writes inspectable artifacts under `examples/.artifacts/demo`.
//!
//! Default runtime behavior executes full sync (`run_sync = true`). Tests and
//! automation can force configuration-only mode by setting
//! `MEDIAPM_DEMO_RUN_SYNC=false`.

use std::collections::BTreeMap;
use std::error::Error;
use std::fs;
use std::path::{Path, PathBuf};
use std::thread;
use std::time::{Duration, SystemTime, UNIX_EPOCH};

use mediapm::{
    HierarchyEntry, MediaMetadataValue, MediaPmApi, MediaPmService, MediaSourceSpec, MediaStep,
    MediaStepTool, ToolRequirement, TransformInputValue, load_lockfile, load_mediapm_document,
    save_mediapm_document,
};
use mediapm_cas::{CasApi, FileSystemCas, Hash};
use mediapm_conductor::SimpleConductor;
use serde::Serialize;
use serde_json::json;

/// Shared result type for this demo.
type ExampleResult<T> = Result<T, Box<dyn Error>>;

/// Embedded tiny MP4 payload containing both video and audio tracks.
const SAMPLE_AV_MP4_BYTES: &[u8] = include_bytes!("assets/sample-av.mp4");

/// Canonical demo media id.
const DEMO_MEDIA_ID: &str = "local-av";

/// Demo metadata artist value used in hierarchy interpolation.
const DEMO_METADATA_ARTIST: &str = "Rick Astley";

/// Demo metadata title value used in hierarchy interpolation.
const DEMO_METADATA_TITLE: &str = "Never Gonna Give You Up";

/// Import-once source kind expected by runtime for CAS-hash ingest.
const IMPORT_ONCE_KIND_CAS_HASH: &str = "cas_hash";

/// Environment variable controlling whether this example runs full sync.
///
/// - unset: full sync enabled,
/// - set to one of `0`, `false`, `no`, or `off` (case-insensitive): sync
///   disabled and only artifact/config generation runs.
const DEMO_RUN_SYNC_ENV_VAR: &str = "MEDIAPM_DEMO_RUN_SYNC";

/// Manifest persisted under `examples/.artifacts/demo/manifest.json`.
#[derive(Debug, Clone, PartialEq, Eq, Serialize)]
struct DemoManifest {
    /// Unix timestamp (seconds) when artifacts were generated.
    generated_unix_epoch_seconds: u64,
    /// Artifact root for this demo run.
    artifact_root: String,
    /// Demo workspace root (same path as artifact root).
    workspace_root: String,
    /// Canonical media id configured by this demo.
    media_id: String,
    /// Source local MP4 fixture path used for CAS ingest.
    source_file_path: String,
    /// CAS hash string used by the `import-once` source step.
    source_hash: String,
    /// Whether source bytes include a video-track marker.
    source_has_video_track_marker: bool,
    /// Whether source bytes include an audio-track marker.
    source_has_audio_track_marker: bool,
    /// Configured managed tool count in `mediapm.ncl`.
    configured_tool_count: usize,
    /// Configured step count in the managed source workflow.
    configured_step_count: usize,
    /// Materialized output path #1.
    materialized_primary_path: String,
    /// Materialized output path #2.
    materialized_secondary_path: String,
    /// Whether output #1 exists after optional sync.
    materialized_primary_exists: bool,
    /// Whether output #2 exists after optional sync.
    materialized_secondary_exists: bool,
    /// Whether full sync was executed during this demo run.
    sync_executed: bool,
    /// Number of managed files recorded in lock state.
    lock_managed_files_count: usize,
    /// Number of tool-registry rows in lock state.
    lock_tool_registry_count: usize,
    /// Sync executed count.
    executed_instances: usize,
    /// Sync cache-hit count.
    cached_instances: usize,
    /// Sync rematerialized count.
    rematerialized_instances: usize,
    /// Sync materialized path count.
    materialized_paths: usize,
    /// Sync removed-path count.
    removed_paths: usize,
    /// Sync warning count.
    warning_count: usize,
    /// Path to `mediapm.ncl`.
    mediapm_ncl_path: String,
    /// Path to conductor user document.
    conductor_user_ncl_path: String,
    /// Path to conductor machine document.
    conductor_machine_ncl_path: String,
    /// Path to `.mediapm/lock.jsonc`.
    lock_jsonc_path: String,
    /// Path to resolved library root.
    library_root_path: String,
}

/// Paths printed at the end of demo execution.
#[derive(Debug, Clone)]
struct DemoRunPaths {
    /// Artifact root containing all generated demo outputs.
    artifact_root: PathBuf,
    /// Workspace root used by `MediaPmService`.
    workspace_root: PathBuf,
    /// Manifest file path for quick human inspection.
    manifest_path: PathBuf,
    /// Materialized media library path.
    library_root: PathBuf,
}

/// Returns deterministic artifact root for this persistent demo.
fn artifact_root() -> PathBuf {
    PathBuf::from(env!("CARGO_MANIFEST_DIR")).join("examples").join(".artifacts").join("demo")
}

/// Returns current Unix timestamp in seconds.
fn unix_timestamp_seconds() -> u64 {
    SystemTime::now().duration_since(UNIX_EPOCH).map(|duration| duration.as_secs()).unwrap_or(0)
}

/// Converts one filesystem path to a slash-normalized display string.
fn display_path(path: &Path) -> String {
    path.display().to_string().replace('\\', "/")
}

/// Recreates artifact root so every run starts clean.
fn reset_artifact_root() -> ExampleResult<PathBuf> {
    let root = artifact_root();
    if root.exists() {
        remove_dir_all_with_retry(&root)?;
    }
    fs::create_dir_all(&root)?;
    Ok(root)
}

/// Removes one directory with short retry policy for Windows locking behavior.
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
                if !retryable || attempt + 1 == ATTEMPTS {
                    last_error = Some(error);
                    break;
                }
                thread::sleep(Duration::from_millis(BACKOFF_MS));
                last_error = Some(error);
            }
        }
    }

    match last_error {
        Some(error) => Err(Box::new(error)),
        None => Ok(()),
    }
}

/// Writes one serializable value as pretty JSON.
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

/// Writes one local MP4 fixture file from the embedded binary bytes.
fn write_local_av_fixture(path: &Path) -> ExampleResult<Vec<u8>> {
    if let Some(parent) = path.parent() {
        fs::create_dir_all(parent)?;
    }

    fs::write(path, SAMPLE_AV_MP4_BYTES)?;
    Ok(SAMPLE_AV_MP4_BYTES.to_vec())
}

/// Returns true when one byte payload contains one ASCII marker sequence.
fn bytes_contain_ascii(bytes: &[u8], marker: &[u8]) -> bool {
    bytes.windows(marker.len()).any(|window| window == marker)
}

/// Parses optional env-var text into a sync-enabled flag.
///
/// Empty/missing values default to `true` so direct `cargo run --example demo`
/// remains a full end-to-end execution.
fn sync_enabled_from_env_value(value: Option<&str>) -> bool {
    let Some(raw) = value else {
        return true;
    };

    let normalized = raw.trim().to_ascii_lowercase();
    !matches!(normalized.as_str(), "0" | "false" | "no" | "off")
}

/// Resolves sync execution mode from `MEDIAPM_DEMO_RUN_SYNC`.
fn demo_run_sync_enabled() -> bool {
    sync_enabled_from_env_value(std::env::var(DEMO_RUN_SYNC_ENV_VAR).ok().as_deref())
}

/// Imports one source payload into the runtime CAS store and returns its hash.
async fn import_source_fixture_into_cas(
    cas: &FileSystemCas,
    source_bytes: &[u8],
) -> ExampleResult<Hash> {
    let hash = cas.put(source_bytes.to_vec()).await?;
    Ok(hash)
}

/// Builds one demo `mediapm.ncl` document that demonstrates a local ingest
/// pipeline (`import-once -> ffmpeg -> rsgain -> media-tagger`).
fn configure_document_for_local_tool_chain(
    workspace_root: &Path,
    source_hash: &str,
) -> ExampleResult<(usize, usize)> {
    let mediapm_ncl = workspace_root.join("mediapm.ncl");
    let mut document = load_mediapm_document(&mediapm_ncl)?;

    document.tools = BTreeMap::from([
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
            tool: MediaStepTool::ImportOnce,
            input_variants: Vec::new(),
            output_variants: BTreeMap::from([(
                "video".to_string(),
                json!({ "kind": "result", "save": true, "save_full": true }),
            )]),
            options: BTreeMap::from([
                (
                    "kind".to_string(),
                    TransformInputValue::String(IMPORT_ONCE_KIND_CAS_HASH.to_string()),
                ),
                ("hash".to_string(), TransformInputValue::String(source_hash.to_string())),
            ]),
        },
        MediaStep {
            tool: MediaStepTool::Ffmpeg,
            input_variants: vec!["video".to_string()],
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
            options: BTreeMap::new(),
        },
        MediaStep {
            tool: MediaStepTool::MediaTagger,
            input_variants: vec!["audio_normalized".to_string()],
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
            ]),
        },
    ];

    let configured_step_count = steps.len();

    document.media = BTreeMap::from([(
        DEMO_MEDIA_ID.to_string(),
        MediaSourceSpec {
            description: Some(
                "Local demo pipeline exercising import-once, ffmpeg, rsgain, and media-tagger"
                    .to_string(),
            ),
            workflow_id: None,
            metadata: Some(BTreeMap::from([
                (
                    "artist".to_string(),
                    MediaMetadataValue::Literal(DEMO_METADATA_ARTIST.to_string()),
                ),
                ("title".to_string(), MediaMetadataValue::Literal(DEMO_METADATA_TITLE.to_string())),
            ])),
            variant_hashes: BTreeMap::new(),
            steps,
        },
    )]);

    document.hierarchy = BTreeMap::from([
        (
            "Library/${media.metadata.artist}/${media.metadata.title}/local-av.mp4".to_string(),
            HierarchyEntry {
                media_id: DEMO_MEDIA_ID.to_string(),
                variants: vec!["video".to_string()],
            },
        ),
        (
            "Library/${media.metadata.artist}/${media.metadata.title}/local-av-tagged.mp3"
                .to_string(),
            HierarchyEntry {
                media_id: DEMO_MEDIA_ID.to_string(),
                variants: vec!["tagged".to_string()],
            },
        ),
    ]);

    save_mediapm_document(&mediapm_ncl, &document)?;
    Ok((document.tools.len(), configured_step_count))
}

/// Runs the persistent demo and returns generated paths.
///
/// `run_sync = true` executes full tool reconciliation + workflow execution.
/// `run_sync = false` writes a complete showcase configuration and manifest
/// without external network/tool download requirements (used by automated
/// tests that still need to execute the real example entrypoint).
async fn generate_demo_artifacts(run_sync: bool) -> ExampleResult<DemoRunPaths> {
    let root = reset_artifact_root()?;
    let workspace_root = root.clone();

    let source_path = workspace_root.join("input").join("sample-av.mp4");
    let source_bytes = write_local_av_fixture(&source_path)?;
    let source_has_video_track_marker = bytes_contain_ascii(&source_bytes, b"vide");
    let source_has_audio_track_marker = bytes_contain_ascii(&source_bytes, b"soun");

    let ingest_service = MediaPmService::new_in_memory_at(&workspace_root);
    let paths = ingest_service.paths().clone();
    let cas = FileSystemCas::open(paths.runtime_root.join("store")).await?;

    let source_hash = import_source_fixture_into_cas(&cas, &source_bytes).await?;
    let source_hash_text = source_hash.to_string();

    let (configured_tool_count, configured_step_count) =
        configure_document_for_local_tool_chain(&workspace_root, &source_hash_text)?;

    let service = MediaPmService::new(SimpleConductor::new(cas), paths);

    let maybe_summary = if run_sync { Some(service.sync_library().await?) } else { None };

    let materialized_primary = service
        .paths()
        .library_dir
        .join("Library")
        .join(DEMO_METADATA_ARTIST)
        .join(DEMO_METADATA_TITLE)
        .join("local-av-tagged.mp3");
    let materialized_secondary = service
        .paths()
        .library_dir
        .join("Library")
        .join(DEMO_METADATA_ARTIST)
        .join(DEMO_METADATA_TITLE)
        .join("local-av.mp4");

    let lock = load_lockfile(&service.paths().lock_jsonc)?;

    let manifest = DemoManifest {
        generated_unix_epoch_seconds: unix_timestamp_seconds(),
        artifact_root: display_path(&root),
        workspace_root: display_path(&workspace_root),
        media_id: DEMO_MEDIA_ID.to_string(),
        source_file_path: display_path(&source_path),
        source_hash: source_hash_text,
        source_has_video_track_marker,
        source_has_audio_track_marker,
        configured_tool_count,
        configured_step_count,
        materialized_primary_path: display_path(&materialized_primary),
        materialized_secondary_path: display_path(&materialized_secondary),
        materialized_primary_exists: materialized_primary.exists(),
        materialized_secondary_exists: materialized_secondary.exists(),
        sync_executed: maybe_summary.is_some(),
        lock_managed_files_count: lock.managed_files.len(),
        lock_tool_registry_count: lock.tool_registry.len(),
        executed_instances: maybe_summary.as_ref().map_or(0, |summary| summary.executed_instances),
        cached_instances: maybe_summary.as_ref().map_or(0, |summary| summary.cached_instances),
        rematerialized_instances: maybe_summary
            .as_ref()
            .map_or(0, |summary| summary.rematerialized_instances),
        materialized_paths: maybe_summary.as_ref().map_or(0, |summary| summary.materialized_paths),
        removed_paths: maybe_summary.as_ref().map_or(0, |summary| summary.removed_paths),
        warning_count: maybe_summary.as_ref().map_or(0, |summary| summary.warnings.len()),
        mediapm_ncl_path: display_path(&service.paths().mediapm_ncl),
        conductor_user_ncl_path: display_path(&service.paths().conductor_user_ncl),
        conductor_machine_ncl_path: display_path(&service.paths().conductor_machine_ncl),
        lock_jsonc_path: display_path(&service.paths().lock_jsonc),
        library_root_path: display_path(&service.paths().library_dir),
    };

    let manifest_path = root.join("manifest.json");
    write_json_file(&manifest_path, &manifest)?;

    Ok(DemoRunPaths {
        artifact_root: root,
        workspace_root,
        manifest_path,
        library_root: service.paths().library_dir.clone(),
    })
}

#[tokio::main]
/// Runs the persistent demo and prints generated artifact paths.
async fn main() -> ExampleResult<()> {
    let run_sync = demo_run_sync_enabled();
    let paths = generate_demo_artifacts(run_sync).await?;
    println!("generated artifacts root: {}", paths.artifact_root.display());
    println!("generated workspace root: {}", paths.workspace_root.display());
    println!("generated library root: {}", paths.library_root.display());
    println!("manifest: {}", paths.manifest_path.display());
    println!("sync executed: {run_sync}");
    Ok(())
}

#[cfg(test)]
mod tests {
    use std::fs;

    /// Verifies demo artifact generation writes one complete import-once
    /// workflow manifest when runtime sync is intentionally skipped.
    #[tokio::test]
    async fn generate_demo_artifacts_writes_manifest_and_import_once_metadata() {
        let run = super::generate_demo_artifacts(false).await.expect("demo artifact generation");

        assert!(run.manifest_path.exists(), "manifest should be written");
        assert!(run.workspace_root.exists(), "workspace root should exist");

        let manifest_text = fs::read_to_string(&run.manifest_path).expect("read manifest");
        let manifest_json: serde_json::Value =
            serde_json::from_str(&manifest_text).expect("manifest JSON");

        assert_eq!(
            manifest_json.get("configured_tool_count").and_then(serde_json::Value::as_u64),
            Some(3),
            "demo should configure three managed tools"
        );
        assert_eq!(
            manifest_json.get("configured_step_count").and_then(serde_json::Value::as_u64),
            Some(4),
            "demo should configure four workflow steps including import-once"
        );
        assert_eq!(
            manifest_json.get("source_has_video_track_marker").and_then(serde_json::Value::as_bool),
            Some(true),
            "source fixture should expose a video marker"
        );
        assert_eq!(
            manifest_json.get("source_has_audio_track_marker").and_then(serde_json::Value::as_bool),
            Some(true),
            "source fixture should expose an audio marker"
        );
        assert!(
            manifest_json
                .get("source_hash")
                .and_then(serde_json::Value::as_str)
                .is_some_and(|hash| hash.starts_with("blake3:")),
            "manifest should record a blake3-prefixed CAS hash"
        );

        let demo_config_path = run.workspace_root.join("mediapm.ncl");
        let demo_config =
            fs::read_to_string(&demo_config_path).expect("read generated mediapm.ncl");

        assert!(
            demo_config.contains("tool = \"import-once\""),
            "demo should ingest source data via import-once"
        );
        assert!(
            !demo_config.contains("tool = \"yt-dlp\""),
            "demo should not route local sample ingest through yt-dlp"
        );
    }

    /// Ensures artifact root stays stable for docs and scripts.
    #[test]
    fn artifact_root_is_stable() {
        let text = super::display_path(&super::artifact_root());
        assert!(text.ends_with("src/mediapm/examples/.artifacts/demo"));
    }

    /// Verifies sync-mode parser defaults to true and recognizes explicit false
    /// tokens used by automated test execution.
    #[test]
    fn sync_enabled_env_parser_handles_false_tokens() {
        assert!(super::sync_enabled_from_env_value(None));
        assert!(super::sync_enabled_from_env_value(Some("true")));
        assert!(!super::sync_enabled_from_env_value(Some("false")));
        assert!(!super::sync_enabled_from_env_value(Some("OFF")));
        assert!(!super::sync_enabled_from_env_value(Some("0")));
    }
}
