//! End-to-end online demo that exercises managed tool provisioning.
//!
//! Manual runs execute the full online workflow. When this example is executed
//! as a Cargo test binary, it auto-switches to configuration-only mode so
//! automated runs avoid network/provider calls and external-tool execution.
//! The online demo keeps runtime bounded while still validating downloader
//! sidecar capture behavior.
//!
//! Runtime flow:
//! 1. Creates a clean artifact workspace under `examples/.artifacts/demo-online/`.
//! 2. Declares managed downloader + transform tools in `mediapm.ncl`.
//! 3. Declares one media workflow that processes
//!    `https://www.youtube.com/watch?v=dQw4w9WgXcQ` through:
//!    `yt-dlp -> ffmpeg -> media-tagger -> rsgain`.
//! 4. Runs full `mediapm sync` through
//!    `MediaPmService::sync_library_with_tag_update_checks`, which reconciles
//!    tools and executes managed workflows.
//! 5. Verifies all managed tools are registered with executable command
//!    selectors and non-empty content maps.
//! 6. Verifies the managed demo workflow exists and keeps the expected
//!    platform-specific tool sequence.
//! 7. Verifies the demo media output and tagged-media output are materialized
//!    under one metadata-interpolated hierarchy root using
//!    `${media.metadata.artist} - ${media.metadata.title} [${media.id}]`
//!    filename templates.
//! 8. Writes a compact `manifest.json` with generated paths and sync metadata.

use std::collections::{BTreeMap, BTreeSet};
use std::error::Error;
use std::fs;
use std::path::{Path, PathBuf};
use std::process::{self, Command};
use std::str::FromStr;
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::{Arc, OnceLock};
use std::thread;
use std::time::{Duration, SystemTime, UNIX_EPOCH};

use mediapm::{
    HierarchyFolderRenameRule, HierarchyNode, HierarchyNodeKind, MaterializationMethod,
    MediaMetadataRegexTransform, MediaMetadataValue, MediaMetadataVariantBinding, MediaPmService,
    MediaRuntimeStorage, MediaSourceSpec, MediaStep, MediaStepTool, PlaylistEntryPathMode,
    PlaylistFormat, PlaylistItemRef, ToolRequirement, ToolRequirementDependencies,
    TransformInputValue, load_lockfile, load_mediapm_document, save_mediapm_document,
};
use mediapm_cas::{CasApi, FileSystemCas, Hash};
use mediapm_conductor::{
    MachineNickelDocument, ToolKindSpec, decode_machine_document,
    default_runtime_inherited_env_vars_for_host,
};
use same_file::is_same_file;
use serde::Serialize;
use serde_json::json;

/// Stable media id used by the online demo media workflow.
const DEMO_MEDIA_ID: &str = "rickroll";
/// Hierarchy id assigned to the tagged media-file node and playlist target.
///
/// Only nodes that appear in playlist `ids` entries require a hierarchy id.
/// The untagged media node and sidecar nodes intentionally omit ids.
const DEMO_TAGGED_HIERARCHY_ID: &str = "rickroll-tagged";
/// Online source URI processed by the demo workflow.
const DEMO_SOURCE_URI: &str = "https://www.youtube.com/watch?v=dQw4w9WgXcQ";
/// Human-facing description mirrored into managed workflow metadata.
const DEMO_WORKFLOW_DESCRIPTION: &str = "Online demo pipeline downloading video + sidecars, transcoding, loudness-normalizing, and applying metadata";

/// Metadata key used for variant-backed title interpolation.
const DEMO_METADATA_TITLE_KEY: &str = "title";

/// Metadata key used for variant-backed artist interpolation from tagged media.
const DEMO_METADATA_ARTIST_KEY: &str = "artist";

/// Metadata key used for variant-backed hierarchy interpolation.
const DEMO_METADATA_VIDEO_ID_KEY: &str = "id";

/// Metadata key used for extension extraction from tagged media metadata.
const DEMO_METADATA_VIDEO_EXT_KEY: &str = "format_name";

/// Additional literal metadata field value used by this demo.
const DEMO_METADATA_SOURCE_LITERAL: &str = "youtube-demo";

/// Expected interpolated video-id value for the demo source URL.
const DEMO_EXPECTED_VIDEO_ID: &str = "dQw4w9WgXcQ";

/// Expected title metadata emitted by the demo source.
const DEMO_EXPECTED_TITLE: &str = "Never Gonna Give You Up";

/// Expected managed output extension (with leading dot) for media variants.
const DEMO_EXPECTED_VIDEO_EXTENSION_WITH_DOT: &str = ".mkv";

/// Shared base name used for demo media outputs and mirrored file sidecars.
const DEMO_OUTPUT_FILE_NAME_BASE: &str =
    "${media.metadata.artist} - ${media.metadata.title} [${media.id}]";

/// Demo yt-dlp subtitle language selector that avoids translated subtitle
/// requests.
///
/// This pattern keeps focused English subtitle capture and avoids broad
/// translated-language subtitle fetches that can trigger provider-side HTTP
/// 429 throttling.
const DEMO_SAFE_SUB_LANGS: &str = "en-en,en-AU,en-CA,en-IN,en-IE,en-GB,en-US,en-orig";

/// Stable untagged media filename materialized under the hierarchy root.
const DEMO_UNTAGGED_MEDIA_FILE_NAME: &str = "${media.metadata.artist} - ${media.metadata.title} [${media.id}].untagged${media.metadata.video_ext}";

/// Stable tagged media filename materialized under the hierarchy root.
const DEMO_TAGGED_MEDIA_FILE_NAME: &str =
    "${media.metadata.artist} - ${media.metadata.title} [${media.id}]${media.metadata.video_ext}";

/// Explicit runtime materialization preference order shown in this demo.
const DEMO_MATERIALIZATION_PREFERENCE_ORDER: [MaterializationMethod; 4] = [
    MaterializationMethod::Hardlink,
    MaterializationMethod::Symlink,
    MaterializationMethod::Reflink,
    MaterializationMethod::Copy,
];

/// Returns final variant key materialized into the demo hierarchy path.
fn final_demo_output_variant() -> &'static str {
    "video"
}

/// Top-level library folder used by the demo hierarchy.
const DEMO_LIBRARY_ROOT: &str = "music videos";

/// Metadata-interpolated media folder name used under `DEMO_LIBRARY_ROOT`.
const DEMO_HIERARCHY_MEDIA_ROOT_TEMPLATE: &str =
    "${media.metadata.artist} - ${media.metadata.title} [${media.id}]";

/// Hierarchy-root template that demonstrates metadata interpolation.
const DEMO_HIERARCHY_ROOT_TEMPLATE: &str =
    "music videos/${media.metadata.artist} - ${media.metadata.title} [${media.id}]";

/// Named sidecar variants and relative hierarchy paths materialized from yt-dlp artifacts.
///
/// Each tuple is `(entry_label, variant_name, relative_path)`. Labels remain
/// unique even when one variant is materialized to multiple hierarchy paths.
///
/// This demo keeps directory-style sidecars under `sidecars/` and mirrors
/// selected file sidecars directly beside media outputs.
const DEMO_SIDECAR_VARIANT_PATHS: [(&str, &str, &str); 9] = [
    ("subtitles_sidecars", "subtitles", "sidecars/subtitles/"),
    ("thumbnails_sidecars", "thumbnails", "sidecars/thumbnails/"),
    ("links_sidecars", "links", "sidecars/links/"),
    ("archive_sidecars", "archive", "sidecars/archive.txt"),
    ("description_sidecars", "description", "sidecars/description.txt"),
    ("infojson_sidecars", "infojson", "sidecars/info.json"),
    (
        "description_media",
        "description",
        "${media.metadata.artist} - ${media.metadata.title} [${media.id}].description.txt",
    ),
    (
        "infojson_media",
        "infojson",
        "${media.metadata.artist} - ${media.metadata.title} [${media.id}].info.json",
    ),
    (
        "subtitles_en_media",
        DEMO_ROOT_SELECTED_SUBTITLE_VARIANT,
        DEMO_ROOT_SELECTED_SUBTITLE_FILE_NAME,
    ),
];

/// Flat non-subtitle sidecar-family variants materialized directly in media root.
const DEMO_MEDIA_ROOT_FLAT_VARIANTS: [&str; 2] = ["thumbnails", "links"];

/// One language-scoped subtitle variant materialized outside `sidecars/`.
const DEMO_ROOT_SELECTED_SUBTITLE_VARIANT: &str = "subtitles_en";

/// Root-level subtitle file name bound to the media output base-name template.
const DEMO_ROOT_SELECTED_SUBTITLE_FILE_NAME: &str =
    "${media.metadata.artist} - ${media.metadata.title} [${media.id}].en.vtt";

/// Root-sidecar rename rule that rebases any filename onto the media output
/// base by preserving only the final extension.
///
/// Pattern captures the extension after the last dot. Replacement applies
/// the metadata-interpolated base and re-attaches the captured extension so
/// thumbnails, link sidecars, and other root-flat captures adopt the
/// canonical `<artist> - <title> [<id>].<ext>` naming automatically.
const DEMO_MEDIA_ROOT_RENAME_PATTERN: &str = "^.*\\.([^.]*)$";
/// Replacement used with `DEMO_MEDIA_ROOT_RENAME_PATTERN` after metadata
/// template resolution.
const DEMO_MEDIA_ROOT_RENAME_REPLACEMENT: &str =
    "${media.metadata.artist} - ${media.metadata.title} [${media.id}].$1";

/// Expected yt-dlp step count.
///
/// Managed workflow synthesis now emits one shared yt-dlp step that can
/// produce primary + sidecar outputs in a single downloader invocation.
const DEMO_EXPECTED_YT_DLP_STEP_COUNT: usize = 1;

/// Environment variable override for demo sync timeout seconds.
const DEMO_ONLINE_TIMEOUT_SECS_ENV: &str = "MEDIAPM_DEMO_ONLINE_TIMEOUT_SECS";

/// Environment variable override for enabling/disabling full sync execution.
///
/// - unset: full sync enabled in manual runs; disabled in test-binary runs,
/// - set to one of `0`, `false`, `no`, or `off`: force config-only mode,
/// - any other non-empty value: force full sync mode.
const DEMO_ONLINE_RUN_SYNC_ENV: &str = "MEDIAPM_DEMO_ONLINE_RUN_SYNC";

/// Default timeout for the online demo sync phase.
const DEMO_ONLINE_TIMEOUT_SECS_DEFAULT: u64 = 3 * 60;

/// Environment variable override for per-step conductor executable timeout.
const DEMO_CONDUCTOR_EXECUTABLE_TIMEOUT_SECS_ENV: &str =
    "MEDIAPM_CONDUCTOR_EXECUTABLE_TIMEOUT_SECS";

/// Default per-step executable timeout used by demo-online when no override is set.
const DEMO_CONDUCTOR_EXECUTABLE_TIMEOUT_DEFAULT_SECS: u64 = 5 * 60;

/// Safety reserve kept between demo timeout and per-step timeout defaults.
const DEMO_CONDUCTOR_EXECUTABLE_TIMEOUT_RESERVE_SECS: u64 = 60;

/// Settle delay before printing timeout notices after cancellation.
///
/// The conductor progress renderer writes asynchronously. Waiting one short
/// interval after the timeout branch wins the `select!` helps avoid racing a
/// final progress repaint with the timeout notice line.
const DEMO_ONLINE_TIMEOUT_NOTICE_SETTLE_MILLIS: u64 = 120;

/// Additional grace window for the hard watchdog after graceful timeout path.
///
/// The async timeout path should usually terminate first and preserve cleaner
/// progress output. This watchdog grace still guarantees process exit when
/// runtime cancellation stalls.
const DEMO_ONLINE_HARD_TIMEOUT_GRACE_SECS: u64 = 15;

/// Maximum ffprobe polling attempts while waiting for `ReplayGain` tags.
const DEMO_REPLAYGAIN_FFPROBE_MAX_ATTEMPTS: usize = 20;

/// Delay between ffprobe polling attempts for `ReplayGain` tag visibility.
const DEMO_REPLAYGAIN_FFPROBE_RETRY_DELAY_MILLIS: u64 = 350;

/// `ReplayGain` keys required on final tagged media outputs.
///
/// Managed `rsgain` defaults stay in single-track mode, so track-family tags
/// plus reference loudness are required while album-family keys remain
/// disallowed.
const DEMO_REPLAYGAIN_REQUIRED_TAG_KEYS: [&str; 3] =
    ["replaygain_track_gain", "replaygain_track_peak", "replaygain_reference_loudness"];

/// `ReplayGain` keys that must stay absent in default single-track mode.
const DEMO_REPLAYGAIN_DISALLOWED_TAG_KEYS: [&str; 3] =
    ["replaygain_album_gain", "replaygain_album_peak", "replaygain_album_range"];

/// Shared result type for this online example.
type ExampleResult<T> = Result<T, Box<dyn Error>>;

/// Lazily initialized `ffprobe` command selector used by media validation.
///
/// When available, this points to the sibling binary of the resolved managed
/// `ffmpeg` tool so demo validation does not rely on host-global PATH state.
static DEMO_FFPROBE_COMMAND: OnceLock<String> = OnceLock::new();

/// Structured timeout error emitted by graceful online demo timeout handling.
#[derive(Debug, Clone, PartialEq, Eq)]
struct DemoOnlineTimeoutError {
    /// Configured timeout (seconds) that elapsed before cancellation.
    timeout_seconds: u64,
}

/// Formats the graceful timeout notice shown to end users.
#[must_use]
fn format_graceful_timeout_notice(timeout_seconds: u64) -> String {
    format!(
        "Online demo timed out after {timeout_seconds} seconds. This is usually temporary provider/network throttling—wait briefly, then rerun."
    )
}

/// Formats the fallback hard-watchdog notice shown before forced exit.
#[must_use]
fn format_hard_watchdog_notice(total_timeout_seconds: u64) -> String {
    format!(
        "Online demo exceeded the timeout grace period ({total_timeout_seconds} seconds total). Exiting now with code 124."
    )
}

impl std::fmt::Display for DemoOnlineTimeoutError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "{}", format_graceful_timeout_notice(self.timeout_seconds))
    }
}

impl Error for DemoOnlineTimeoutError {}

/// Runtime manifest persisted under the demo artifact root.
#[derive(Debug, Clone, PartialEq, Serialize)]
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
    /// Runtime materialization policy order written into `mediapm.ncl`.
    materialization_preference_order: Vec<String>,
    /// Materialized library path for the transcoded demo video variant.
    materialized_demo_video_path: String,
    /// Materialized library path for the normalized + tagged video variant.
    materialized_demo_tagged_video_path: String,
    /// Whether demo video output is hardlinked to CAS object bytes.
    materialized_demo_video_hardlinked_to_cas: bool,
    /// Whether tagged demo video output is hardlinked to CAS object bytes.
    materialized_demo_tagged_video_hardlinked_to_cas: bool,
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
    /// Path to conductor workflow profile JSON written after sync execution.
    ///
    /// The profile records per-step elapsed times for the managed pipeline.
    /// Set `MEDIAPM_CONDUCTOR_PROFILE_JSON` to override the output path.
    profile_path: String,
    /// Logical CAS store footprint without delta compression (bytes).
    store_size_without_delta_bytes: u64,
    /// Effective CAS store footprint with delta compression (bytes).
    store_size_with_delta_bytes: u64,
    /// Ratio `with_delta / without_delta` for quick compression comparison.
    ///
    /// Values `< 1.0` indicate on-disk savings from delta compression; values
    /// near `1.0` are expected when most payloads are already compressed.
    ///
    /// When `store_size_without_delta_bytes == 0`, this demo emits `1.0` so
    /// empty/objectless stores report a neutral ratio instead of
    /// divide-by-zero noise.
    store_size_ratio_with_delta_over_without: f64,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
/// Aggregate CAS store-size metrics shared by manifest serialization.
struct StoreSizeStats {
    /// Logical CAS store footprint without delta compression (bytes).
    without_delta_bytes: u64,
    /// Effective CAS store footprint with delta compression (bytes).
    with_delta_bytes: u64,
}

impl StoreSizeStats {
    /// Returns `with_delta / without_delta` for manifest reporting.
    ///
    /// Values `< 1.0` indicate on-disk savings from delta compression.
    ///
    /// For zero-byte logical stores, this returns `1.0` to represent a neutral
    /// no-change baseline and avoid divide-by-zero artifacts.
    #[must_use]
    #[expect(
        clippy::cast_precision_loss,
        reason = "manifest ratio output is intentionally approximate for human-facing diagnostics"
    )]
    fn ratio_with_delta_over_without(self) -> f64 {
        if self.without_delta_bytes == 0 {
            1.0
        } else {
            self.with_delta_bytes as f64 / self.without_delta_bytes as f64
        }
    }
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

/// Resolves the online demo sync timeout from environment configuration.
///
/// This guard prevents perceived infinite hangs when external providers stall
/// or throttle requests.
fn online_demo_timeout() -> ExampleResult<Duration> {
    let Some(raw_value) = std::env::var_os(DEMO_ONLINE_TIMEOUT_SECS_ENV) else {
        return Ok(Duration::from_secs(DEMO_ONLINE_TIMEOUT_SECS_DEFAULT));
    };

    let text = raw_value.to_string_lossy().trim().to_string();
    if text.is_empty() {
        return Ok(Duration::from_secs(DEMO_ONLINE_TIMEOUT_SECS_DEFAULT));
    }

    let seconds = text.parse::<u64>().map_err(|error| {
        format!(
            "{DEMO_ONLINE_TIMEOUT_SECS_ENV} must be a positive integer number of seconds, got '{text}': {error}"
        )
    })?;
    if seconds == 0 {
        return Err(format!("{DEMO_ONLINE_TIMEOUT_SECS_ENV} must be greater than 0 seconds").into());
    }

    Ok(Duration::from_secs(seconds))
}

/// Parses one optional sync-mode override string into a boolean flag.
#[must_use]
fn sync_enabled_from_env_value(value: Option<&str>, default_enabled: bool) -> bool {
    let Some(raw) = value else {
        return default_enabled;
    };

    let normalized = raw.trim().to_ascii_lowercase();
    !matches!(normalized.as_str(), "0" | "false" | "no" | "off")
}

/// Returns whether this example binary was compiled as a Cargo test target.
#[must_use]
fn running_as_test_binary() -> bool {
    cfg!(test)
}

/// Resolves online-demo sync mode from env override + build mode.
#[must_use]
fn demo_online_run_sync_enabled() -> bool {
    sync_enabled_from_env_value(
        std::env::var(DEMO_ONLINE_RUN_SYNC_ENV).ok().as_deref(),
        !running_as_test_binary(),
    )
}

/// Configures a bounded default per-step executable timeout for this demo.
///
/// The online demo's first downloader step can block for long periods under
/// provider throttling. When callers do not provide an explicit
/// `MEDIAPM_CONDUCTOR_EXECUTABLE_TIMEOUT_SECS` override, this helper chooses a
/// conservative timeout that is always below the overall demo timeout and emits
/// one plain-text notice describing the applied default.
fn configure_demo_conductor_executable_timeout(sync_timeout: Duration) {
    if std::env::var_os(DEMO_CONDUCTOR_EXECUTABLE_TIMEOUT_SECS_ENV).is_some() {
        return;
    }

    let sync_seconds = sync_timeout.as_secs();
    let computed_timeout = sync_seconds
        .saturating_sub(DEMO_CONDUCTOR_EXECUTABLE_TIMEOUT_RESERVE_SECS)
        .clamp(30, DEMO_CONDUCTOR_EXECUTABLE_TIMEOUT_DEFAULT_SECS);

    unsafe {
        std::env::set_var(DEMO_CONDUCTOR_EXECUTABLE_TIMEOUT_SECS_ENV, computed_timeout.to_string());
    }
    emit_watchdog_notice(&format!(
        "demo_online defaulted {DEMO_CONDUCTOR_EXECUTABLE_TIMEOUT_SECS_ENV}={computed_timeout} to fail long-running external steps before the overall demo timeout; set {DEMO_CONDUCTOR_EXECUTABLE_TIMEOUT_SECS_ENV} to override"
    ));
}

/// Spawns a hard timeout guard that force-terminates the process when elapsed.
///
/// This guard exists because graceful async timeout cancellation may fail to
/// stop deeply nested runtime tasks or external subprocess orchestration in
/// some host/runtime states.
fn spawn_hard_timeout_guard(timeout: Duration) -> Arc<AtomicBool> {
    let watchdog_timeout =
        timeout.saturating_add(Duration::from_secs(DEMO_ONLINE_HARD_TIMEOUT_GRACE_SECS));
    let completed = Arc::new(AtomicBool::new(false));
    let completed_for_thread = Arc::clone(&completed);

    thread::spawn(move || {
        thread::sleep(watchdog_timeout);
        if completed_for_thread.load(Ordering::SeqCst) {
            return;
        }

        emit_watchdog_notice(&format_hard_watchdog_notice(watchdog_timeout.as_secs()));
        process::exit(124);
    });

    completed
}

/// Prints one timeout/watchdog notice line without progress-row control codes.
///
/// Keeping this output as plain newline-separated text avoids ANSI-row clears
/// that can duplicate pulsebar render rows when async progress updates and
/// watchdog notices race near cancellation boundaries.
fn emit_watchdog_notice(message: &str) {
    eprintln!();
    eprintln!("{message}");
}

/// Metadata values resolved from the materialized downloader infojson sidecar.
#[derive(Debug, Clone, PartialEq, Eq)]
struct DemoResolvedMetadata {
    /// Human-facing artist name.
    artist: String,
    /// Human-facing video title.
    title: String,
    /// Stable downloader-provided video id.
    video_id: String,
    /// Stable mediapm media id used by `${media.id}` placeholders.
    media_id: String,
    /// Media extension (including leading dot) used for output filenames.
    video_ext: String,
}

/// Reads one non-empty string field from one JSON object payload.
fn require_non_empty_json_string(
    payload: &serde_json::Value,
    key: &str,
    source_path: &Path,
) -> ExampleResult<String> {
    payload
        .get(key)
        .and_then(serde_json::Value::as_str)
        .map(str::trim)
        .filter(|value| !value.is_empty())
        .map(str::to_string)
        .ok_or_else(|| {
            format!(
                "expected non-empty string key '{}' in infojson sidecar '{}'",
                key,
                source_path.display()
            )
            .into()
        })
}

/// Resolves demo `${media.metadata.*}` placeholders in one template string.
#[must_use]
fn resolve_demo_metadata_template(template: &str, metadata: &DemoResolvedMetadata) -> String {
    template
        .replace("${media.metadata.artist}", &metadata.artist)
        .replace("${media.metadata.title}", &metadata.title)
        .replace("${media.metadata.video_id}", &metadata.video_id)
        .replace("${media.id}", &metadata.media_id)
        .replace("${media.metadata.video_ext}", &metadata.video_ext)
}

/// Parses one Jellyfin-style media root folder name
/// `<artist> - <title> [<media-id>]`.
fn parse_jellyfin_root_folder_name(folder_name: &str) -> Option<(String, String, String)> {
    let (artist_and_title, media_id_segment) = folder_name.rsplit_once(" [")?;
    let media_id = media_id_segment.strip_suffix(']')?.trim();
    let (artist, title) = artist_and_title.split_once(" - ")?;

    let artist = artist.trim();
    let title = title.trim();
    if artist.is_empty() || title.is_empty() || media_id.is_empty() {
        return None;
    }

    Some((artist.to_string(), title.to_string(), media_id.to_string()))
}

/// Loads resolved metadata from one interpolated media root folder.
fn load_resolved_demo_metadata(interpolated_root: &Path) -> ExampleResult<DemoResolvedMetadata> {
    let infojson_path = interpolated_root.join("sidecars").join("info.json");
    let payload: serde_json::Value = serde_json::from_slice(&fs::read(&infojson_path)?)?;

    let folder_name =
        interpolated_root.file_name().and_then(|value| value.to_str()).ok_or_else(|| {
            format!(
                "expected interpolated media root '{}' to have a UTF-8 folder name",
                interpolated_root.display()
            )
        })?;

    let (artist, folder_title, media_id) = parse_jellyfin_root_folder_name(folder_name)
        .ok_or_else(|| {
            format!(
                "expected interpolated media root '{}' to match '<artist> - <title> [<media-id>]'",
                interpolated_root.display()
            )
        })?;

    Ok(DemoResolvedMetadata {
        artist,
        // The hierarchy title template now binds to `video_tagged` metadata,
        // so treat the interpolated folder title as source-of-truth.
        title: folder_title,
        video_id: require_non_empty_json_string(
            &payload,
            DEMO_METADATA_VIDEO_ID_KEY,
            &infojson_path,
        )?,
        media_id,
        video_ext: DEMO_EXPECTED_VIDEO_EXTENSION_WITH_DOT.to_string(),
    })
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
    eprintln!(
        "[demo_online] canonical artifact root '{}' is locked; using fallback root '{}'",
        canonical_root.display(),
        fallback_root.display()
    );
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
#[cfg_attr(
    windows,
    expect(
        clippy::permissions_set_readonly_false,
        reason = "Windows cleanup retries must clear readonly flags on downloaded artifacts so repeated demo runs can remove prior trees"
    )
)]
fn clear_readonly_bits_recursively(path: &Path) {
    #[cfg(not(windows))]
    {
        let _ = path;
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

/// Returns whether one path segment is a hexadecimal fragment.
#[must_use]
fn is_hex_segment(value: &str) -> bool {
    !value.is_empty() && value.bytes().all(|byte| byte.is_ascii_hexdigit())
}

/// Parses one filesystem CAS object path into a canonical hash when possible.
fn parse_hash_from_store_object_path(objects_root: &Path, path: &Path) -> Option<Hash> {
    let relative = path.strip_prefix(objects_root).ok()?;
    let mut components = relative.iter();
    let algorithm = components.next()?.to_string_lossy().to_string();
    if algorithm.is_empty() {
        return None;
    }

    let mut hex = String::new();
    for component in components {
        let segment = component.to_string_lossy();
        let segment = segment.strip_suffix(".diff").unwrap_or(segment.as_ref());
        if !is_hex_segment(segment) {
            return None;
        }
        hex.push_str(segment);
    }

    if hex.len() != 64 {
        return None;
    }

    Hash::from_str(&format!("{algorithm}:{hex}")).ok()
}

/// Recursively visits one CAS object directory and records discovered hashes.
fn collect_store_object_hashes_recursive(
    objects_root: &Path,
    current_dir: &Path,
    hashes: &mut BTreeSet<Hash>,
) -> ExampleResult<()> {
    for entry in fs::read_dir(current_dir)? {
        let entry = entry?;
        let path = entry.path();
        if entry.file_type()?.is_dir() {
            collect_store_object_hashes_recursive(objects_root, &path, hashes)?;
            continue;
        }

        if !entry.file_type()?.is_file() {
            continue;
        }

        if let Some(hash) = parse_hash_from_store_object_path(objects_root, &path) {
            let _ = hashes.insert(hash);
        }
    }

    Ok(())
}

/// Collects all persisted object hashes currently present in one CAS root.
fn collect_store_object_hashes(cas_root: &Path) -> ExampleResult<BTreeSet<Hash>> {
    let mut hashes = BTreeSet::new();
    let objects_root = cas_root.join("v1");
    if !objects_root.exists() {
        return Ok(hashes);
    }

    collect_store_object_hashes_recursive(&objects_root, &objects_root, &mut hashes)?;

    Ok(hashes)
}

/// Computes logical and effective store-size totals from all persisted objects.
async fn summarize_store_sizes(cas_root: &Path) -> ExampleResult<StoreSizeStats> {
    let cas = FileSystemCas::open(cas_root).await?;
    let mut without_delta = 0u64;
    let mut with_delta = 0u64;

    for hash in collect_store_object_hashes(cas_root)? {
        let info = cas.info(hash).await?;
        without_delta = without_delta.saturating_add(info.content_len);
        with_delta = with_delta.saturating_add(info.payload_len);
    }

    Ok(StoreSizeStats { without_delta_bytes: without_delta, with_delta_bytes: with_delta })
}

/// Resolves canonical CAS object file path for one content hash.
fn cas_object_path_for_hash(cas_root: &Path, hash: Hash) -> PathBuf {
    let hex = hash.to_hex();
    let algorithm = hash.algorithm_name();
    cas_root.join("v1").join(algorithm).join(&hex[..2]).join(&hex[2..4]).join(&hex[4..])
}

/// Returns lockfile-relative managed path for one materialized output file.
fn managed_relative_path(hierarchy_root: &Path, output_path: &Path) -> ExampleResult<String> {
    let relative = output_path.strip_prefix(hierarchy_root).map_err(|error| {
        std::io::Error::other(format!(
            "materialized path '{}' is outside hierarchy root '{}': {error}",
            output_path.display(),
            hierarchy_root.display()
        ))
    })?;

    Ok(relative.to_string_lossy().replace('\\', "/"))
}

/// Returns whether one materialized file is hardlinked to one CAS object.
fn output_is_hardlinked_to_cas_object(
    cas_root: &Path,
    hash: Hash,
    output_path: &Path,
) -> ExampleResult<bool> {
    let source_path = cas_object_path_for_hash(cas_root, hash);
    if !source_path.is_file() || !output_path.is_file() {
        return Ok(false);
    }

    Ok(is_same_file(&source_path, output_path)?
        && fs::read(&source_path)? == fs::read(output_path)?)
}

/// Validates one materialized output file is hardlinked to its CAS object hash.
fn assert_materialized_output_hardlinked_to_cas(
    cas_root: &Path,
    hierarchy_root: &Path,
    lock: &mediapm::MediaLockFile,
    output_path: &Path,
) -> ExampleResult<bool> {
    let relative_path = managed_relative_path(hierarchy_root, output_path)?;
    let record = lock.managed_files.get(relative_path.as_str()).ok_or_else(|| {
        std::io::Error::other(format!(
            "managed output '{relative_path}' missing from lockfile tracking"
        ))
    })?;
    let hash = Hash::from_str(record.hash.as_str()).map_err(|error| {
        std::io::Error::other(format!(
            "managed output '{}' has invalid CAS hash '{}': {error}",
            relative_path, record.hash
        ))
    })?;

    if !output_is_hardlinked_to_cas_object(cas_root, hash, output_path)? {
        return Err(std::io::Error::other(format!(
            "materialized output '{}' is not hardlinked to CAS object '{}'",
            output_path.display(),
            record.hash
        ))
        .into());
    }

    Ok(true)
}

/// Seeds one `mediapm.ncl` document with managed tools + media workflow.
#[expect(
    clippy::too_many_lines,
    reason = "this item intentionally keeps end-to-end control flow together so ordering invariants remain explicit during maintenance"
)]
fn configure_document_for_online_demo(workspace_root: &Path) -> ExampleResult<Vec<String>> {
    let mediapm_ncl = workspace_root.join("mediapm.ncl");
    let mut document = load_mediapm_document(&mediapm_ncl)?;
    document.tools = BTreeMap::from([
        (
            "yt-dlp".to_string(),
            ToolRequirement {
                version: None,
                tag: Some("latest".to_string()),
                dependencies: ToolRequirementDependencies {
                    ffmpeg_version: Some("inherit".to_string()),
                    sd_version: None,
                },
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
                dependencies: ToolRequirementDependencies::default(),
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
                dependencies: ToolRequirementDependencies {
                    ffmpeg_version: Some("inherit".to_string()),
                    sd_version: Some("inherit".to_string()),
                },
                recheck_seconds: None,
                max_input_slots: None,
                max_output_slots: None,
            },
        ),
        (
            "sd".to_string(),
            ToolRequirement {
                version: None,
                tag: Some("latest".to_string()),
                dependencies: ToolRequirementDependencies::default(),
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
                dependencies: ToolRequirementDependencies {
                    ffmpeg_version: Some("inherit".to_string()),
                    sd_version: None,
                },
                recheck_seconds: None,
                max_input_slots: None,
                max_output_slots: None,
            },
        ),
        (
            "import".to_string(),
            ToolRequirement {
                version: None,
                tag: None,
                dependencies: ToolRequirementDependencies::default(),
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
                    }),
                ),
                (
                    "subtitles".to_string(),
                    json!({
                        "kind": "subtitles",
                    }),
                ),
                (
                    "subtitles_en".to_string(),
                    json!({
                        "kind": "subtitles",
                        "capture_kind": "file",
                        // Keep yt-dlp language download selection authoritative in
                        // step options (`options.sub_langs`); this variant-level
                        // `langs` value only scopes capture/materialization so the
                        // media root can project one selected subtitle.
                        "langs": "en",
                    }),
                ),
                (
                    "thumbnails".to_string(),
                    json!({
                        "kind": "thumbnails",
                    }),
                ),
                (
                    "description".to_string(),
                    json!({
                        "kind": "description",
                    }),
                ),
                (
                    "infojson".to_string(),
                    json!({
                        "kind": "infojson",
                    }),
                ),
                (
                    "links".to_string(),
                    json!({
                        "kind": "links",
                    }),
                ),
                (
                    "archive".to_string(),
                    json!({
                        "kind": "archive",
                    }),
                ),
            ]),
            options: BTreeMap::from([
                ("uri".to_string(), TransformInputValue::String(DEMO_SOURCE_URI.to_string())),
                (
                    "format".to_string(),
                    TransformInputValue::String(
                        "bestvideo[height<=144]+bestaudio/best[height<=144]/best".to_string(),
                    ),
                ),
                (
                    "sub_langs".to_string(),
                    TransformInputValue::String(DEMO_SAFE_SUB_LANGS.to_string()),
                ),
            ]),
        },
        MediaStep {
            tool: MediaStepTool::Ffmpeg,
            input_variants: vec!["video".to_string()],
            output_variants: BTreeMap::from([(
                "video".to_string(),
                json!({ "kind": "primary", "idx": 0, "extension": "mkv" }),
            )]),
            options: BTreeMap::from([
                ("codec_copy".to_string(), TransformInputValue::String("true".to_string())),
                ("container".to_string(), TransformInputValue::String("matroska".to_string())),
            ]),
        },
        MediaStep {
            tool: MediaStepTool::MediaTagger,
            input_variants: vec!["video".to_string()],
            output_variants: BTreeMap::from([(
                "video_tagged".to_string(),
                json!({ "kind": "primary", "extension": "mkv" }),
            )]),
            options: BTreeMap::from([
                (
                    "recording_mbid".to_string(),
                    TransformInputValue::String("8f3471b5-7e6a-48da-86a9-c1c07a0f47ae".to_string()),
                ),
                ("write_all_images".to_string(), TransformInputValue::String("false".to_string())),
            ]),
        },
        MediaStep {
            tool: MediaStepTool::Rsgain,
            input_variants: vec!["video_tagged".to_string()],
            output_variants: BTreeMap::from([(
                "video_tagged".to_string(),
                json!({ "kind": "primary", "extension": "mkv" }),
            )]),
            options: BTreeMap::new(),
        },
    ];

    document.media = BTreeMap::from([(
        DEMO_MEDIA_ID.to_string(),
        MediaSourceSpec {
            id: None,
            description: Some(DEMO_WORKFLOW_DESCRIPTION.to_string()),
            title: Some(DEMO_EXPECTED_TITLE.to_string()),
            workflow_id: None,
            metadata: Some(BTreeMap::from([
                (
                    "title".to_string(),
                    MediaMetadataValue::Variant(MediaMetadataVariantBinding {
                        variant: "video_tagged".to_string(),
                        metadata_key: DEMO_METADATA_TITLE_KEY.to_string(),
                        transform: None,
                    }),
                ),
                (
                    "artist".to_string(),
                    MediaMetadataValue::Variant(MediaMetadataVariantBinding {
                        variant: "video_tagged".to_string(),
                        metadata_key: DEMO_METADATA_ARTIST_KEY.to_string(),
                        transform: None,
                    }),
                ),
                (
                    "video_id".to_string(),
                    MediaMetadataValue::Variant(MediaMetadataVariantBinding {
                        variant: "infojson".to_string(),
                        metadata_key: DEMO_METADATA_VIDEO_ID_KEY.to_string(),
                        transform: None,
                    }),
                ),
                (
                    "video_ext".to_string(),
                    MediaMetadataValue::Variant(MediaMetadataVariantBinding {
                        variant: "video_tagged".to_string(),
                        metadata_key: DEMO_METADATA_VIDEO_EXT_KEY.to_string(),
                        transform: Some(MediaMetadataRegexTransform {
                            pattern: "(?i)matroska(?:,.*)?".to_string(),
                            replacement: ".mkv".to_string(),
                        }),
                    }),
                ),
                (
                    "source".to_string(),
                    MediaMetadataValue::Literal(DEMO_METADATA_SOURCE_LITERAL.to_string()),
                ),
            ])),
            variant_hashes: BTreeMap::new(),
            steps,
        },
    )]);
    let mut media_root_children = vec![
        HierarchyNode {
            path: DEMO_UNTAGGED_MEDIA_FILE_NAME.to_string(),
            kind: HierarchyNodeKind::Media,
            id: None,
            media_id: Some(DEMO_MEDIA_ID.to_string()),
            variant: Some(final_demo_output_variant().to_string()),
            variants: Vec::new(),
            rename_files: Vec::new(),
            format: PlaylistFormat::M3u8,
            ids: Vec::new(),
            children: Vec::new(),
        },
        HierarchyNode {
            path: DEMO_TAGGED_MEDIA_FILE_NAME.to_string(),
            kind: HierarchyNodeKind::Media,
            id: Some(DEMO_TAGGED_HIERARCHY_ID.to_string()),
            media_id: Some(DEMO_MEDIA_ID.to_string()),
            variant: Some("video_tagged".to_string()),
            variants: Vec::new(),
            rename_files: Vec::new(),
            format: PlaylistFormat::M3u8,
            ids: Vec::new(),
            children: Vec::new(),
        },
    ];
    let mut sidecar_folder_children = Vec::new();

    for (_entry_label, variant, relative_path) in DEMO_SIDECAR_VARIANT_PATHS {
        let (hierarchy_path, sidecars_child) =
            if let Some(stripped) = relative_path.strip_prefix("sidecars/") {
                (stripped.to_string(), true)
            } else {
                (relative_path.to_string(), false)
            };

        let target_children =
            if sidecars_child { &mut sidecar_folder_children } else { &mut media_root_children };

        if relative_path.ends_with('/') {
            target_children.push(HierarchyNode {
                path: hierarchy_path.trim_end_matches('/').to_string(),
                kind: HierarchyNodeKind::MediaFolder,
                id: None,
                media_id: Some(DEMO_MEDIA_ID.to_string()),
                variant: None,
                variants: vec![variant.to_string()],
                rename_files: Vec::new(),
                format: PlaylistFormat::M3u8,
                ids: Vec::new(),
                children: Vec::new(),
            });
        } else {
            target_children.push(HierarchyNode {
                path: hierarchy_path,
                kind: HierarchyNodeKind::Media,
                id: None,
                media_id: Some(DEMO_MEDIA_ID.to_string()),
                variant: Some(variant.to_string()),
                variants: Vec::new(),
                rename_files: Vec::new(),
                format: PlaylistFormat::M3u8,
                ids: Vec::new(),
                children: Vec::new(),
            });
        }
    }

    media_root_children.push(HierarchyNode {
        path: String::new(),
        kind: HierarchyNodeKind::MediaFolder,
        id: None,
        media_id: Some(DEMO_MEDIA_ID.to_string()),
        variant: None,
        variants: DEMO_MEDIA_ROOT_FLAT_VARIANTS
            .iter()
            .map(|variant| (*variant).to_string())
            .collect(),
        rename_files: vec![HierarchyFolderRenameRule {
            pattern: DEMO_MEDIA_ROOT_RENAME_PATTERN.to_string(),
            replacement: DEMO_MEDIA_ROOT_RENAME_REPLACEMENT.to_string(),
        }],
        format: PlaylistFormat::M3u8,
        ids: Vec::new(),
        children: Vec::new(),
    });

    if !sidecar_folder_children.is_empty() {
        media_root_children.push(HierarchyNode {
            path: "sidecars".to_string(),
            kind: HierarchyNodeKind::Folder,
            id: None,
            media_id: None,
            variant: None,
            variants: Vec::new(),
            rename_files: Vec::new(),
            format: PlaylistFormat::M3u8,
            ids: Vec::new(),
            children: sidecar_folder_children,
        });
    }

    document.hierarchy = vec![
        HierarchyNode {
            path: DEMO_LIBRARY_ROOT.to_string(),
            kind: HierarchyNodeKind::Folder,
            id: None,
            media_id: None,
            variant: None,
            variants: Vec::new(),
            rename_files: Vec::new(),
            format: PlaylistFormat::M3u8,
            ids: Vec::new(),
            children: vec![HierarchyNode {
                path: DEMO_HIERARCHY_MEDIA_ROOT_TEMPLATE.to_string(),
                kind: HierarchyNodeKind::Folder,
                id: None,
                media_id: None,
                variant: None,
                variants: Vec::new(),
                rename_files: Vec::new(),
                format: PlaylistFormat::M3u8,
                ids: Vec::new(),
                children: media_root_children,
            }],
        },
        HierarchyNode {
            path: "playlists".to_string(),
            kind: HierarchyNodeKind::Folder,
            id: None,
            media_id: None,
            variant: None,
            variants: Vec::new(),
            rename_files: Vec::new(),
            format: PlaylistFormat::M3u8,
            ids: Vec::new(),
            children: vec![HierarchyNode {
                path: "rickroll.m3u8".to_string(),
                kind: HierarchyNodeKind::Playlist,
                id: None,
                media_id: None,
                variant: None,
                variants: Vec::new(),
                rename_files: Vec::new(),
                format: PlaylistFormat::M3u8,
                ids: vec![
                    PlaylistItemRef {
                        id: DEMO_TAGGED_HIERARCHY_ID.to_string(),
                        path: PlaylistEntryPathMode::Relative,
                    },
                    PlaylistItemRef {
                        id: DEMO_TAGGED_HIERARCHY_ID.to_string(),
                        path: PlaylistEntryPathMode::Absolute,
                    },
                ],
                children: Vec::new(),
            }],
        },
    ];

    // Expose all runtime-storage fields with explicit default values so the
    // written mediapm.ncl documents every runtime knob by default.
    document.runtime = MediaRuntimeStorage {
        // Runtime root for all managed state files.
        // Default: `.mediapm/` relative to the workspace root.
        mediapm_dir: Some(".mediapm".to_string()),
        // Materialized hierarchy root directory.
        // Default: workspace root containing `mediapm.ncl`.
        hierarchy_root_dir: Some(".".to_string()),
        // Staging directory relative to `runtime.mediapm_dir`.
        // Default: `<mediapm_dir>/tmp/`.
        mediapm_tmp_dir: Some("tmp".to_string()),
        // Ordered file-materialization method preference.
        // Default when omitted: hardlink -> symlink -> reflink -> copy.
        materialization_preference_order: Some(DEMO_MATERIALIZATION_PREFERENCE_ORDER.to_vec()),
        // User-owned conductor config path relative to workspace root.
        // Default: `mediapm.conductor.ncl`.
        conductor_config: Some("mediapm.conductor.ncl".to_string()),
        // Machine-managed conductor config path relative to workspace root.
        // Default: `mediapm.conductor.machine.ncl`.
        conductor_machine_config: Some("mediapm.conductor.machine.ncl".to_string()),
        // Volatile conductor state path relative to workspace root.
        // Default: `.mediapm/state.conductor.ncl`.
        conductor_state_config: Some(".mediapm/state.conductor.ncl".to_string()),
        // Conductor execution tmp path relative to workspace root.
        // Default: `runtime.mediapm_tmp_dir`.
        conductor_tmp_dir: Some(".mediapm/tmp".to_string()),
        // Conductor schema export directory relative to workspace root.
        // Default: `<mediapm_dir>/config/conductor`.
        conductor_schema_dir: Some(".mediapm/config/conductor".to_string()),
        // Explicit host default inherited env-var map.
        // Runtime still merges this map case-insensitively with host defaults.
        inherited_env_vars: Some(default_runtime_inherited_env_vars_for_host()),
        // Machine-managed mediapm state path relative to workspace root.
        // Default: `.mediapm/state.ncl`.
        media_state_config: Some(".mediapm/state.ncl".to_string()),
        // Dotenv credential source path relative to workspace root.
        // Default: `.mediapm/.env`.
        env_file: Some(".mediapm/.env".to_string()),
        // Embedded schema export directory policy.
        // `Some(Some(path))` keeps export enabled with an explicit default path.
        mediapm_schema_dir: Some(Some(".mediapm/config/mediapm".to_string())),
        // Enable global user tool cache to reuse downloaded tool binaries
        // across runs without re-downloading from the network.
        // Default when omitted: true.
        use_user_tool_cache: Some(true),
    };

    save_mediapm_document(&mediapm_ncl, &document)?;
    Ok(vec![
        "yt-dlp".to_string(),
        "ffmpeg".to_string(),
        "rsgain".to_string(),
        "sd".to_string(),
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
/// `mediapm.tools.yt-dlp+github-releases-yt-dlp-yt-dlp@latest`).
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

        binaries.insert(resolved_tool_id, first.clone());
    }

    Ok(binaries)
}

/// Returns the command selector used to invoke `ffprobe`.
#[must_use]
fn demo_ffprobe_command() -> &'static str {
    DEMO_FFPROBE_COMMAND.get().map_or("ffprobe", String::as_str)
}

/// Derives one `ffprobe` sibling path from a resolved `ffmpeg` command.
///
/// Returns `None` when the provided command does not look like an ffmpeg
/// executable selector.
#[must_use]
fn derive_ffprobe_path_from_ffmpeg_command(ffmpeg_command: &str) -> Option<PathBuf> {
    let trimmed = ffmpeg_command.trim().trim_matches('"').trim_matches('\'');
    if trimmed.is_empty() {
        return None;
    }

    let (prefix, file_name) = match trimmed.rfind(['/', '\\']) {
        Some(index) => (&trimmed[..=index], &trimmed[index + 1..]),
        None => ("", trimmed),
    };

    if file_name.eq_ignore_ascii_case("ffmpeg.exe") {
        return Some(PathBuf::from(format!("{prefix}ffprobe.exe")));
    }

    if file_name.eq_ignore_ascii_case("ffmpeg") {
        return Some(PathBuf::from(format!("{prefix}ffprobe")));
    }

    None
}

/// Configures the demo-local `ffprobe` command selector from managed tools.
///
/// Falls back to bare `ffprobe` when no managed sibling binary is available.
fn configure_demo_ffprobe_command(
    machine: &MachineNickelDocument,
    tool_binaries: &BTreeMap<String, String>,
) -> ExampleResult<()> {
    let ffmpeg_tool_id = resolve_managed_tool_id(machine, "ffmpeg")?;

    let ffprobe_command = tool_binaries
        .get(&ffmpeg_tool_id)
        .and_then(|command| derive_ffprobe_path_from_ffmpeg_command(command))
        .filter(|candidate| candidate.is_file())
        .map_or_else(|| "ffprobe".to_string(), |candidate| candidate.display().to_string());

    let _ = DEMO_FFPROBE_COMMAND.set(ffprobe_command);
    Ok(())
}

/// Reads yt-dlp max concurrency from machine config and enforces policy value `1`.
fn assert_yt_dlp_concurrency_policy(
    machine: &MachineNickelDocument,
    yt_dlp_tool_id: &str,
) -> ExampleResult<i32> {
    let observed =
        machine.tool_configs.get(yt_dlp_tool_id).map_or(-1, |config| config.max_concurrent_calls);

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
    let observed = machine.tool_configs.get(yt_dlp_tool_id).map_or(-1, |config| config.max_retries);

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
    fn tool_order_text(logical_tools: &[String]) -> String {
        logical_tools.join(" -> ")
    }

    fn find_next_tool_index(
        workflow_id: &str,
        logical_tools: &[String],
        expected_tool: &str,
        start_at: usize,
    ) -> ExampleResult<usize> {
        logical_tools
            .iter()
            .enumerate()
            .skip(start_at)
            .find_map(|(index, actual)| (actual == expected_tool).then_some(index))
            .ok_or_else(|| {
                format!(
                    "managed workflow '{workflow_id}' is missing required tool '{expected_tool}' at/after step #{start_at}; observed order: {}",
                    tool_order_text(logical_tools)
                )
                .into()
            })
    }

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

    let logical_tools = workflow
        .steps
        .iter()
        .enumerate()
        .map(|(index, step)| {
            logical_name_from_managed_tool_id(&step.tool).map(str::to_string).ok_or_else(|| {
                format!(
                    "managed workflow '{workflow_id}' step #{index} uses non-managed tool id '{}'",
                    step.tool
                )
                .into()
            })
        })
        .collect::<ExampleResult<Vec<_>>>()?;

    let yt_dlp_steps = logical_tools.iter().filter(|tool| tool.as_str() == "yt-dlp").count();
    if yt_dlp_steps < DEMO_EXPECTED_YT_DLP_STEP_COUNT {
        return Err(format!(
            "managed workflow '{workflow_id}' must contain at least {} yt-dlp step(s) but observed {yt_dlp_steps}; order: {}",
            DEMO_EXPECTED_YT_DLP_STEP_COUNT,
            tool_order_text(&logical_tools)
        )
        .into());
    }

    // Require core online-demo stage ordering while allowing synthesis to
    // insert additional helper steps between these stages over time.
    let required_order = [
        "yt-dlp",
        "ffmpeg",
        "media-tagger",
        "ffmpeg",
        "ffmpeg",
        "rsgain",
        "ffmpeg",
        "sd",
        "ffmpeg",
    ];

    let mut cursor = 0usize;
    for required_tool in required_order {
        let found_index =
            find_next_tool_index(&workflow_id, &logical_tools, required_tool, cursor)?;
        cursor = found_index + 1;
    }

    debug_assert!(
        DEMO_UNTAGGED_MEDIA_FILE_NAME.starts_with(DEMO_OUTPUT_FILE_NAME_BASE)
            && DEMO_TAGGED_MEDIA_FILE_NAME.starts_with(DEMO_OUTPUT_FILE_NAME_BASE),
        "demo media filenames should share the configured output base"
    );
    debug_assert_eq!(
        DEMO_HIERARCHY_ROOT_TEMPLATE,
        format!("{DEMO_LIBRARY_ROOT}/{DEMO_HIERARCHY_MEDIA_ROOT_TEMPLATE}"),
        "full hierarchy-root template should stay aligned with nested-root constants"
    );

    Ok((workflow_id, workflow.steps.len()))
}

/// Returns true when file bytes look like a Matroska/EBML payload.
fn bytes_look_like_matroska(bytes: &[u8]) -> bool {
    // Matroska/WebM containers begin with the EBML header marker.
    bytes.starts_with(&[0x1A, 0x45, 0xDF, 0xA3])
}

/// Collects all regular files under one directory tree.
fn collect_regular_files_recursive(root: &Path) -> ExampleResult<Vec<PathBuf>> {
    let mut pending = vec![root.to_path_buf()];
    let mut files = Vec::new();

    while let Some(next) = pending.pop() {
        for entry in fs::read_dir(&next)? {
            let path = entry?.path();
            if path.is_dir() {
                pending.push(path);
            } else if path.is_file() {
                files.push(path);
            }
        }
    }

    Ok(files)
}

/// Returns lowercase file extension text without leading dot.
#[must_use]
fn lowercase_extension(path: &Path) -> Option<String> {
    path.extension().and_then(|value| value.to_str()).map(str::to_ascii_lowercase)
}

/// Returns whether one file extension is recognized as subtitle content.
#[must_use]
fn is_subtitle_extension(extension: &str) -> bool {
    matches!(
        extension,
        "vtt" | "srt" | "ass" | "ssa" | "lrc" | "ttml" | "srv1" | "srv2" | "srv3" | "json3"
    )
}

/// Returns whether one file extension is recognized as image content.
#[must_use]
fn is_image_extension(extension: &str) -> bool {
    matches!(extension, "jpg" | "jpeg" | "png" | "webp" | "avif" | "gif" | "bmp" | "tiff")
}

/// Validates sidecar directory payloads contain expected family file types.
fn assert_sidecar_directory_family_content(variant: &str, directory: &Path) -> ExampleResult<()> {
    let files = collect_regular_files_recursive(directory)?;
    let extensions = files.iter().filter_map(|path| lowercase_extension(path)).collect::<Vec<_>>();

    match variant {
        "subtitles" => {
            if !extensions.iter().any(|extension| is_subtitle_extension(extension)) {
                return Err(format!(
                    "expected sidecar variant '{variant}' at '{}' to contain subtitle files",
                    directory.display()
                )
                .into());
            }
        }
        "thumbnails" => {
            if !extensions.iter().any(|extension| is_image_extension(extension)) {
                return Err(format!(
                    "expected sidecar variant '{variant}' at '{}' to contain thumbnail image files",
                    directory.display()
                )
                .into());
            }
        }
        "links" => {
            if !extensions
                .iter()
                .any(|extension| matches!(extension.as_str(), "url" | "webloc" | "desktop"))
            {
                return Err(format!(
                    "expected sidecar variant '{variant}' at '{}' to contain internet shortcut files",
                    directory.display()
                )
                .into());
            }
        }
        _ => {}
    }

    Ok(())
}

/// Validates root-level sidecar layout outside `sidecars/`.
///
/// Policy:
/// - one selected subtitle file is projected directly into media root,
/// - root subtitle naming must follow the media output base template,
/// - thumbnails and links remain flattened next to media files.
#[expect(
    clippy::too_many_lines,
    reason = "this helper keeps all media-root sidecar invariants in one place so failures are easy to diagnose"
)]
fn assert_flat_media_root_sidecar_families(
    interpolated_root: &Path,
    expected_output_base: &str,
) -> ExampleResult<()> {
    let expected_media_id =
        parse_jellyfin_root_folder_name(expected_output_base).map(|(_, _, media_id)| media_id);

    let subtitles_root = interpolated_root.join("subtitles");
    if subtitles_root.exists() {
        return Err(format!(
            "media root '{}' must not contain dedicated root subtitles folder '{}'",
            interpolated_root.display(),
            subtitles_root.display()
        )
        .into());
    }

    let root_files = fs::read_dir(interpolated_root)?
        .flatten()
        .map(|entry| entry.path())
        .filter(|path| path.is_file())
        .collect::<Vec<_>>();
    let subtitle_files = root_files
        .iter()
        .filter(|path| lowercase_extension(path).as_deref().is_some_and(is_subtitle_extension))
        .cloned()
        .collect::<Vec<_>>();
    if subtitle_files.is_empty() {
        return Err(format!(
            "expected flattened media root '{}' to contain subtitle sidecar files",
            interpolated_root.display()
        )
        .into());
    }

    if !subtitle_files.iter().all(|path| {
        path.file_name()
            .and_then(|value| value.to_str())
            .is_some_and(|name| name.starts_with(expected_output_base))
    }) {
        return Err(format!(
            "expected flattened subtitle sidecar names in '{}' to start with media output base '{}': {:?}",
            interpolated_root.display(),
            expected_output_base,
            subtitle_files
                .iter()
                .filter_map(|path| path.file_name().and_then(|value| value.to_str()))
                .collect::<Vec<_>>()
        )
        .into());
    }

    let has_selected_subtitle = subtitle_files.iter().any(|path| {
        path.file_name().and_then(|value| value.to_str()).is_some_and(|name| {
            let normalized = name.to_ascii_lowercase();
            normalized.ends_with(".en.vtt")
        })
    });
    if !has_selected_subtitle {
        return Err(format!(
            "expected flattened media root '{}' to include selected subtitle filename suffix '.en.vtt'",
            interpolated_root.display()
        )
        .into());
    }

    let root_extensions =
        root_files.iter().filter_map(|path| lowercase_extension(path)).collect::<Vec<_>>();

    if !root_extensions.iter().any(|extension| is_image_extension(extension)) {
        return Err(format!(
            "expected flattened media root '{}' to contain thumbnail sidecar files",
            interpolated_root.display()
        )
        .into());
    }

    if !root_extensions
        .iter()
        .any(|extension| matches!(extension.as_str(), "url" | "webloc" | "desktop"))
    {
        return Err(format!(
            "expected flattened media root '{}' to contain link sidecar files",
            interpolated_root.display()
        )
        .into());
    }

    let non_subtitle_root_sidecars = root_files
        .iter()
        .filter(|path| {
            lowercase_extension(path).as_deref().is_some_and(|extension| {
                is_image_extension(extension) || matches!(extension, "url" | "webloc" | "desktop")
            })
        })
        .collect::<Vec<_>>();
    if !non_subtitle_root_sidecars.iter().all(|path| {
        path.file_name().and_then(|value| value.to_str()).is_some_and(|name| {
            name.starts_with(expected_output_base)
                || expected_media_id
                    .as_deref()
                    .is_some_and(|media_id| name.contains(&format!(" [{media_id}].")))
        })
    }) {
        return Err(format!(
            "expected flattened thumbnail/link sidecar names in '{}' to start with media output base '{}': {:?}",
            interpolated_root.display(),
            expected_output_base,
            non_subtitle_root_sidecars
                .iter()
                .filter_map(|path| path.file_name().and_then(|value| value.to_str()))
                .collect::<Vec<_>>()
        )
        .into());
    }

    Ok(())
}

/// Returns whether one ffprobe JSON payload includes required `ReplayGain`
/// tags.
#[must_use]
fn ffprobe_payload_has_required_replaygain_tags(payload: &serde_json::Value) -> bool {
    let observed_keys = ffprobe_payload_observed_tag_keys(payload);

    DEMO_REPLAYGAIN_REQUIRED_TAG_KEYS.iter().all(|key| observed_keys.contains(*key))
}

/// Collects normalized (lowercase) tag keys observed in one ffprobe payload.
#[must_use]
fn ffprobe_payload_observed_tag_keys(payload: &serde_json::Value) -> BTreeSet<String> {
    let mut observed_keys = BTreeSet::new();

    let mut collect_tag_keys = |value: &serde_json::Value| {
        if let Some(tags) = value.as_object() {
            for (key, raw_value) in tags {
                if raw_value.as_str().is_some_and(|text| !text.trim().is_empty()) {
                    observed_keys.insert(key.to_ascii_lowercase());
                }
            }
        }
    };

    if let Some(format_tags) = payload.get("format").and_then(|format| format.get("tags")) {
        collect_tag_keys(format_tags);
    }

    if let Some(streams) = payload.get("streams").and_then(serde_json::Value::as_array) {
        for stream in streams {
            if let Some(tags) = stream.get("tags") {
                collect_tag_keys(tags);
            }
        }
    }

    observed_keys
}

/// Returns `ReplayGain` keys that are unexpected in single-track mode.
#[must_use]
fn ffprobe_payload_unexpected_single_track_replaygain_tags(
    payload: &serde_json::Value,
) -> Vec<&'static str> {
    let observed_keys = ffprobe_payload_observed_tag_keys(payload);

    DEMO_REPLAYGAIN_DISALLOWED_TAG_KEYS
        .iter()
        .copied()
        .filter(|key| observed_keys.contains(*key))
        .collect::<Vec<_>>()
}

/// Returns whether one ffprobe JSON payload exposes MKV container + video/audio streams.
#[must_use]
fn ffprobe_payload_has_mkv_video_and_audio(payload: &serde_json::Value) -> bool {
    let has_mkv_container = payload
        .get("format")
        .and_then(|format| format.get("format_name"))
        .and_then(serde_json::Value::as_str)
        .is_some_and(|name| {
            name.split(',').any(|entry| entry.trim().eq_ignore_ascii_case("matroska"))
        });

    let mut has_video_stream = false;
    let mut has_audio_stream = false;

    if let Some(streams) = payload.get("streams").and_then(serde_json::Value::as_array) {
        for stream in streams {
            match stream.get("codec_type").and_then(serde_json::Value::as_str) {
                Some("video") => has_video_stream = true,
                Some("audio") => has_audio_stream = true,
                _ => {}
            }
        }
    }

    has_mkv_container && has_video_stream && has_audio_stream
}

/// Verifies one media file keeps MKV container identity and video/audio streams.
fn assert_mkv_video_audio_with_ffprobe(path: &Path) -> ExampleResult<()> {
    let ffprobe_command = demo_ffprobe_command();
    let output = Command::new(ffprobe_command)
        .arg("-v")
        .arg("error")
        .arg("-show_entries")
        .arg("format=format_name:stream=codec_type")
        .arg("-of")
        .arg("json")
        .arg(path)
        .output()
        .map_err(|error| {
            format!(
                "failed to launch ffprobe command '{}' for '{}': {error}",
                ffprobe_command,
                path.display()
            )
        })?;

    if !output.status.success() {
        let stderr = String::from_utf8_lossy(&output.stderr).trim().to_string();
        return Err(format!(
            "ffprobe failed for '{}' with status {}: {stderr}",
            path.display(),
            output.status
        )
        .into());
    }

    let payload: serde_json::Value = serde_json::from_slice(&output.stdout)
        .map_err(|error| format!("ffprobe JSON decode failed for '{}': {error}", path.display()))?;

    if !ffprobe_payload_has_mkv_video_and_audio(&payload) {
        return Err(format!(
            "expected media '{}' to keep MKV container with both video and audio streams",
            path.display()
        )
        .into());
    }

    Ok(())
}

/// Executes one ffprobe probe and returns parsed JSON payload.
fn probe_replaygain_tags_with_ffprobe(
    tagged_video_path: &Path,
) -> ExampleResult<serde_json::Value> {
    let ffprobe_command = demo_ffprobe_command();
    let output = Command::new(ffprobe_command)
        .arg("-v")
        .arg("error")
        .arg("-show_entries")
        .arg("format_tags:stream_tags")
        .arg("-of")
        .arg("json")
        .arg(tagged_video_path)
        .output()
        .map_err(|error| {
            format!(
                "failed to launch ffprobe command '{}' for '{}': {error}",
                ffprobe_command,
                tagged_video_path.display()
            )
        })?;

    if !output.status.success() {
        let stderr = String::from_utf8_lossy(&output.stderr).trim().to_string();
        return Err(format!(
            "ffprobe failed for '{}' with status {}: {stderr}",
            tagged_video_path.display(),
            output.status
        )
        .into());
    }

    serde_json::from_slice(&output.stdout).map_err(|error| {
        format!("ffprobe JSON decode failed for '{}': {error}", tagged_video_path.display()).into()
    })
}

/// Polls ffprobe until tagged media exposes required `ReplayGain` tags.
async fn assert_tagged_media_replaygain_tags(tagged_video_path: &Path) -> ExampleResult<()> {
    let mut last_error = String::new();

    for attempt in 1..=DEMO_REPLAYGAIN_FFPROBE_MAX_ATTEMPTS {
        match probe_replaygain_tags_with_ffprobe(tagged_video_path) {
            Ok(payload) if ffprobe_payload_has_required_replaygain_tags(&payload) => {
                let unexpected_album_family =
                    ffprobe_payload_unexpected_single_track_replaygain_tags(&payload);
                if !unexpected_album_family.is_empty() {
                    return Err(format!(
                        "tagged media '{}' exposed album ReplayGain tags {:?} in default single-track mode",
                        tagged_video_path.display(),
                        unexpected_album_family
                    )
                    .into());
                }
                return Ok(());
            }
            Ok(_) => {
                last_error = format!(
                    "ffprobe did not expose required ReplayGain tags {DEMO_REPLAYGAIN_REQUIRED_TAG_KEYS:?}"
                );
            }
            Err(error) => {
                last_error = error.to_string();
            }
        }

        if attempt < DEMO_REPLAYGAIN_FFPROBE_MAX_ATTEMPTS {
            tokio::time::sleep(Duration::from_millis(DEMO_REPLAYGAIN_FFPROBE_RETRY_DELAY_MILLIS))
                .await;
        }
    }

    Err(format!(
        "tagged media '{}' never exposed ReplayGain tags after {} ffprobe attempts: {}",
        tagged_video_path.display(),
        DEMO_REPLAYGAIN_FFPROBE_MAX_ATTEMPTS,
        last_error
    )
    .into())
}

/// Validates file-capture sidecar payload shape for selected variants.
fn assert_sidecar_file_content_shape(variant: &str, path: &Path) -> ExampleResult<()> {
    match variant {
        "description" | "playlist_description" | "archive" => {
            let text = fs::read_to_string(path)?;
            if text.trim().is_empty() {
                return Err(format!(
                    "expected sidecar variant '{variant}' at '{}' to contain non-empty text",
                    path.display()
                )
                .into());
            }
        }
        "infojson" | "playlist_infojson" => {
            let payload = fs::read(path)?;
            let value: serde_json::Value = serde_json::from_slice(&payload)?;
            if !value.is_object() {
                return Err(format!(
                    "expected sidecar variant '{variant}' at '{}' to contain a JSON object",
                    path.display()
                )
                .into());
            }
        }
        _ => {}
    }

    Ok(())
}

/// Resolves the single interpolated Jellyfin media root for this demo run.
fn resolve_interpolated_demo_root(workspace_root: &Path) -> ExampleResult<PathBuf> {
    let parent = workspace_root.join(DEMO_LIBRARY_ROOT);
    if !parent.is_dir() {
        return Err(
            format!("expected Jellyfin root '{}' to exist after sync", parent.display()).into()
        );
    }

    let mut candidates = fs::read_dir(&parent)?
        .flatten()
        .map(|entry| entry.path())
        .filter(|path| path.is_dir())
        .filter(|path| {
            path.file_name()
                .and_then(|value| value.to_str())
                .is_some_and(|name| name.contains(&format!("[{DEMO_MEDIA_ID}]")))
        })
        .filter(|path| path.join("sidecars").join("info.json").is_file())
        .collect::<Vec<_>>();

    if candidates.len() != 1 {
        return Err(format!(
            "expected exactly one demo media root under '{}', observed {}",
            parent.display(),
            candidates.len()
        )
        .into());
    }

    Ok(candidates.remove(0))
}

/// Resolves and verifies the materialized demo outputs.
///
/// The demo expects one transcoded media and one tagged-media output under one
/// metadata-interpolated hierarchy root.
#[expect(
    clippy::too_many_lines,
    reason = "this item intentionally keeps end-to-end control flow together so ordering invariants remain explicit during maintenance"
)]
fn resolve_demo_output_paths(
    workspace_root: &Path,
) -> ExampleResult<(PathBuf, PathBuf, BTreeMap<String, PathBuf>)> {
    let interpolated_root = resolve_interpolated_demo_root(workspace_root)?;
    if !interpolated_root.exists() {
        return Err(format!(
            "expected interpolated hierarchy root '{}' to exist after sync",
            interpolated_root.display()
        )
        .into());
    }

    let resolved_metadata = load_resolved_demo_metadata(&interpolated_root)?;
    if resolved_metadata.media_id != DEMO_MEDIA_ID {
        return Err(format!(
            "expected resolved media id '{}' but observed '{}'",
            DEMO_MEDIA_ID, resolved_metadata.media_id
        )
        .into());
    }
    if resolved_metadata.video_id != DEMO_EXPECTED_VIDEO_ID {
        return Err(format!(
            "expected resolved video id '{}' but observed '{}'",
            DEMO_EXPECTED_VIDEO_ID, resolved_metadata.video_id
        )
        .into());
    }
    if !resolved_metadata.video_ext.eq_ignore_ascii_case(DEMO_EXPECTED_VIDEO_EXTENSION_WITH_DOT) {
        return Err(format!(
            "expected resolved video extension '{}' but observed '{}'",
            DEMO_EXPECTED_VIDEO_EXTENSION_WITH_DOT, resolved_metadata.video_ext
        )
        .into());
    }

    let resolved_output_base =
        resolve_demo_metadata_template(DEMO_OUTPUT_FILE_NAME_BASE, &resolved_metadata);
    let root_folder_name = interpolated_root
        .file_name()
        .and_then(|value| value.to_str())
        .ok_or_else(|| format!("invalid UTF-8 media root '{}'", interpolated_root.display()))?;
    if root_folder_name != resolved_output_base {
        return Err(format!(
            "interpolated root folder '{root_folder_name}' must match resolved base '{resolved_output_base}'"
        )
        .into());
    }

    let resolved_untagged_file_name =
        resolve_demo_metadata_template(DEMO_UNTAGGED_MEDIA_FILE_NAME, &resolved_metadata);
    let resolved_tagged_file_name =
        resolve_demo_metadata_template(DEMO_TAGGED_MEDIA_FILE_NAME, &resolved_metadata);

    if !resolved_untagged_file_name.starts_with(&resolved_output_base)
        || !resolved_tagged_file_name.starts_with(&resolved_output_base)
    {
        return Err("demo metadata template naming invariants drifted".into());
    }

    let video_path = interpolated_root.join(resolved_untagged_file_name);
    if !video_path.is_file() {
        return Err(format!(
            "expected stable untagged media output '{}' to exist after sync",
            video_path.display()
        )
        .into());
    }

    if !video_path.extension().and_then(|extension| extension.to_str()).is_some_and(|extension| {
        extension
            .eq_ignore_ascii_case(DEMO_EXPECTED_VIDEO_EXTENSION_WITH_DOT.trim_start_matches('.'))
    }) {
        return Err(format!(
            "expected untagged demo media '{}' to keep extension '{}'",
            video_path.display(),
            DEMO_EXPECTED_VIDEO_EXTENSION_WITH_DOT
        )
        .into());
    }

    let video_bytes = fs::read(&video_path)?;
    if !bytes_look_like_matroska(&video_bytes) {
        return Err(format!(
            "expected untagged demo media '{}' to contain Matroska bytes",
            video_path.display()
        )
        .into());
    }
    assert_mkv_video_audio_with_ffprobe(&video_path)?;

    let tagged_video_path = interpolated_root.join(resolved_tagged_file_name);
    if !tagged_video_path.is_file() {
        return Err(format!(
            "expected stable tagged media output '{}' to exist after sync",
            tagged_video_path.display()
        )
        .into());
    }

    if !tagged_video_path.extension().and_then(|extension| extension.to_str()).is_some_and(
        |extension| {
            extension.eq_ignore_ascii_case(
                DEMO_EXPECTED_VIDEO_EXTENSION_WITH_DOT.trim_start_matches('.'),
            )
        },
    ) {
        return Err(format!(
            "expected tagged demo media '{}' to keep extension '{}'",
            tagged_video_path.display(),
            DEMO_EXPECTED_VIDEO_EXTENSION_WITH_DOT
        )
        .into());
    }

    let tagged_video_bytes = fs::read(&tagged_video_path)?;
    if !bytes_look_like_matroska(&tagged_video_bytes) {
        return Err(format!(
            "expected tagged demo media '{}' to contain Matroska bytes",
            tagged_video_path.display()
        )
        .into());
    }
    assert_mkv_video_audio_with_ffprobe(&tagged_video_path)?;

    let mut sidecar_paths = BTreeMap::new();
    for (entry_label, variant, relative_suffix) in DEMO_SIDECAR_VARIANT_PATHS {
        let resolved_relative_suffix =
            resolve_demo_metadata_template(relative_suffix, &resolved_metadata);

        if !resolved_relative_suffix.starts_with("sidecars/")
            && !resolved_relative_suffix.is_empty()
            && resolved_relative_suffix.contains('/')
        {
            return Err(format!(
                "demo sidecar mapping '{entry_label}' must stay flat outside sidecars/ but uses nested path '{relative_suffix}'"
            )
            .into());
        }

        let output_path = if resolved_relative_suffix.is_empty() {
            interpolated_root.clone()
        } else {
            interpolated_root.join(&resolved_relative_suffix)
        };

        let is_playlist_variant = variant.starts_with("playlist_");
        if !output_path.exists() {
            if is_playlist_variant {
                sidecar_paths.insert(entry_label.to_string(), output_path);
                continue;
            }

            return Err(format!(
                "expected demo sidecar variant '{}' at '{}' to exist after sync",
                variant,
                output_path.display()
            )
            .into());
        }

        if resolved_relative_suffix.ends_with('/') || resolved_relative_suffix.is_empty() {
            if !output_path.is_dir() {
                return Err(format!(
                    "expected demo sidecar variant '{}' at '{}' to be a directory",
                    variant,
                    output_path.display()
                )
                .into());
            }

            if is_playlist_variant {
                let playlist_files = collect_regular_files_recursive(&output_path)?;
                if playlist_files.is_empty() {
                    sidecar_paths.insert(entry_label.to_string(), output_path);
                    continue;
                }
            }

            assert_sidecar_directory_family_content(variant, &output_path)?;
        } else if !output_path.is_file() {
            return Err(format!(
                "expected demo sidecar variant '{}' at '{}' to be a file",
                variant,
                output_path.display()
            )
            .into());
        } else {
            assert_sidecar_file_content_shape(variant, &output_path)?;
        }

        sidecar_paths.insert(entry_label.to_string(), output_path);
    }

    assert_flat_media_root_sidecar_families(&interpolated_root, &resolved_output_base)?;

    Ok((video_path, tagged_video_path, sidecar_paths))
}

/// Executes the full online workflow and writes one artifact manifest.
#[expect(
    clippy::too_many_lines,
    reason = "this example keeps end-to-end demo orchestration and artifact assertions in one function for traceability"
)]
async fn run_online_demo(sync_timeout: Duration) -> ExampleResult<DemoRunPaths> {
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

    // Auto-enable conductor profiling to the artifact root so every sync run
    // produces a per-step timing profile for latency investigation.
    // Users can override by setting MEDIAPM_CONDUCTOR_PROFILE_JSON before running.
    let profile_path = root.join("profile.json");
    if std::env::var_os(mediapm_conductor::ENV_PROFILE_OUTPUT_PATH).is_none() {
        // SAFETY: set before tokio workers are spawned; written once and
        // not mutated during the concurrent sync execution.
        unsafe {
            std::env::set_var(mediapm_conductor::ENV_PROFILE_OUTPUT_PATH, &profile_path);
        }
    }

    eprintln!(
        "[demo_online] starting sync (timeout={}s) in '{}'",
        sync_timeout.as_secs(),
        workspace_root.display()
    );

    let sync_future = service.sync_library_with_tag_update_checks(true);
    tokio::pin!(sync_future);
    let timeout_future = tokio::time::sleep(sync_timeout);
    tokio::pin!(timeout_future);

    let summary = tokio::select! {
        () = &mut timeout_future => {
            tokio::time::sleep(Duration::from_millis(DEMO_ONLINE_TIMEOUT_NOTICE_SETTLE_MILLIS))
                .await;
            return Err(Box::new(DemoOnlineTimeoutError {
                timeout_seconds: sync_timeout.as_secs(),
            }));
        }
        result = &mut sync_future => {
            result.map_err(|error| format!("online demo sync failed: {error}"))?
        }
    };

    let machine = load_machine(&service.paths().conductor_machine_ncl)?;
    let tool_binaries = resolve_tool_binaries(&machine, &logical_tool_ids)?;
    configure_demo_ffprobe_command(&machine, &tool_binaries)?;

    let tool_ids = tool_binaries.keys().cloned().collect::<Vec<_>>();

    let yt_dlp_tool_id = resolve_managed_tool_id(&machine, "yt-dlp")?;
    let yt_dlp_max_concurrent_calls = assert_yt_dlp_concurrency_policy(&machine, &yt_dlp_tool_id)?;
    let yt_dlp_max_retries = assert_yt_dlp_retry_policy(&machine, &yt_dlp_tool_id)?;
    let (workflow_id, workflow_step_count) = assert_demo_workflow_shape(&machine)?;
    let (output_video_path, output_tagged_video_path, output_sidecar_paths) =
        resolve_demo_output_paths(&workspace_root)?;
    assert_tagged_media_replaygain_tags(&output_tagged_video_path).await?;
    let cas_root = service.paths().runtime_root.join("store");
    let lock = load_lockfile(&service.paths().mediapm_state_ncl)?;
    let hierarchy_root = &service.paths().hierarchy_root_dir;
    let materialized_demo_video_hardlinked_to_cas = assert_materialized_output_hardlinked_to_cas(
        &cas_root,
        hierarchy_root,
        &lock,
        &output_video_path,
    )?;
    let materialized_demo_tagged_video_hardlinked_to_cas =
        assert_materialized_output_hardlinked_to_cas(
            &cas_root,
            hierarchy_root,
            &lock,
            &output_tagged_video_path,
        )?;
    let store_size_stats = summarize_store_sizes(&cas_root).await?;
    let materialization_preference_order = DEMO_MATERIALIZATION_PREFERENCE_ORDER
        .iter()
        .map(|method| method.as_label().to_string())
        .collect::<Vec<_>>();

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
        materialization_preference_order,
        materialized_demo_video_path: display_path(&output_video_path),
        materialized_demo_tagged_video_path: display_path(&output_tagged_video_path),
        materialized_demo_video_hardlinked_to_cas,
        materialized_demo_tagged_video_hardlinked_to_cas,
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
        profile_path: display_path(&profile_path),
        store_size_without_delta_bytes: store_size_stats.without_delta_bytes,
        store_size_with_delta_bytes: store_size_stats.with_delta_bytes,
        store_size_ratio_with_delta_over_without: store_size_stats.ratio_with_delta_over_without(),
    };

    let manifest_path = root.join("manifest.json");
    write_json_file(&manifest_path, &manifest)?;

    Ok(DemoRunPaths { artifact_root: root, workspace_root, manifest_path })
}

/// Generates demo-online configuration artifacts without running sync.
///
/// This mode exists for automated test-binary execution where network/provider
/// availability and external tool provisioning are intentionally avoided.
fn run_online_demo_config_only() -> ExampleResult<DemoRunPaths> {
    let root = reset_artifact_root()?;
    let workspace_root = root.clone();

    let service = MediaPmService::new_in_memory_at(&workspace_root);
    let logical_tool_ids = configure_document_for_online_demo(&workspace_root)?;

    let materialization_preference_order = DEMO_MATERIALIZATION_PREFERENCE_ORDER
        .iter()
        .map(|method| method.as_label().to_string())
        .collect::<Vec<_>>();

    let manifest = DemoManifest {
        generated_unix_epoch_seconds: unix_timestamp_seconds(),
        artifact_root: display_path(&root),
        workspace_root: display_path(&workspace_root),
        tool_ids: logical_tool_ids,
        tool_binaries: BTreeMap::new(),
        yt_dlp_max_concurrent_calls: 0,
        yt_dlp_max_retries: 0,
        mediapm_ncl_path: display_path(&service.paths().mediapm_ncl),
        conductor_machine_ncl_path: display_path(&service.paths().conductor_machine_ncl),
        workflow_id: format!("mediapm.media.{DEMO_MEDIA_ID}"),
        workflow_step_count: 0,
        materialization_preference_order,
        materialized_demo_video_path: String::new(),
        materialized_demo_tagged_video_path: String::new(),
        materialized_demo_video_hardlinked_to_cas: false,
        materialized_demo_tagged_video_hardlinked_to_cas: false,
        materialized_demo_sidecar_paths: BTreeMap::new(),
        executed_instances: 0,
        cached_instances: 0,
        rematerialized_instances: 0,
        materialized_paths: 0,
        removed_paths: 0,
        added_tools: 0,
        updated_tools: 0,
        warning_count: 0,
        profile_path: String::new(),
        store_size_without_delta_bytes: 0,
        store_size_with_delta_bytes: 0,
        store_size_ratio_with_delta_over_without: 1.0,
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
    let run_sync = demo_online_run_sync_enabled();
    let paths = if run_sync {
        let sync_timeout = online_demo_timeout()?;
        configure_demo_conductor_executable_timeout(sync_timeout);
        let hard_timeout_guard = spawn_hard_timeout_guard(sync_timeout);

        match run_online_demo(sync_timeout).await {
            Ok(paths) => {
                hard_timeout_guard.store(true, Ordering::SeqCst);
                paths
            }
            Err(error) => {
                hard_timeout_guard.store(true, Ordering::SeqCst);
                match error.downcast::<DemoOnlineTimeoutError>() {
                    Ok(timeout_error) => {
                        emit_watchdog_notice(&timeout_error.to_string());
                        process::exit(124);
                    }
                    Err(other) => return Err(other),
                }
            }
        }
    } else {
        run_online_demo_config_only()?
    };

    println!("generated artifacts root: {}", paths.artifact_root.display());
    println!("generated workspace root: {}", paths.workspace_root.display());
    println!("manifest: {}", paths.manifest_path.display());
    println!("sync executed: {run_sync}");

    Ok(())
}

#[cfg(test)]
mod tests {
    /// Ensures env parser honors explicit false tokens and caller defaults.
    #[test]
    fn sync_enabled_env_parser_handles_false_tokens() {
        assert!(super::sync_enabled_from_env_value(None, true));
        assert!(!super::sync_enabled_from_env_value(None, false));
        assert!(super::sync_enabled_from_env_value(Some("true"), false));
        assert!(!super::sync_enabled_from_env_value(Some("false"), true));
        assert!(!super::sync_enabled_from_env_value(Some("OFF"), true));
        assert!(!super::sync_enabled_from_env_value(Some("0"), true));
    }

    /// Ensures test-target binaries default to config-only mode unless
    /// explicitly overridden by environment.
    #[test]
    fn demo_online_run_sync_defaults_to_config_only_in_test_binary() {
        let previous = std::env::var(super::DEMO_ONLINE_RUN_SYNC_ENV).ok();
        // SAFETY: test mutates one process env key in a controlled scope and
        // restores the previous value before exit.
        unsafe {
            std::env::remove_var(super::DEMO_ONLINE_RUN_SYNC_ENV);
        }

        let enabled = super::demo_online_run_sync_enabled();

        // SAFETY: restore previous env var value for test isolation.
        unsafe {
            if let Some(value) = previous {
                std::env::set_var(super::DEMO_ONLINE_RUN_SYNC_ENV, value);
            }
        }

        assert!(!enabled, "test-target demo_online runs should default to config-only mode");
    }

    /// Ensures config-only mode still generates workspace config artifacts and
    /// a manifest without running network/tool sync.
    #[test]
    fn run_online_demo_config_only_writes_manifest_and_config() {
        let run = super::run_online_demo_config_only()
            .expect("config-only demo_online run should succeed");

        assert!(run.manifest_path.exists(), "manifest should be written");
        assert!(
            run.workspace_root.join("mediapm.ncl").exists(),
            "config-only run should still write mediapm.ncl"
        );

        let manifest_text = std::fs::read_to_string(&run.manifest_path).expect("read manifest");
        let manifest_json: serde_json::Value =
            serde_json::from_str(&manifest_text).expect("parse manifest");

        assert_eq!(
            manifest_json.get("executed_instances").and_then(serde_json::Value::as_u64),
            Some(0),
            "config-only run must not execute workflow instances"
        );
        assert_eq!(
            manifest_json
                .get("materialized_demo_video_hardlinked_to_cas")
                .and_then(serde_json::Value::as_bool),
            Some(false),
            "config-only run should not report materialized outputs"
        );
    }

    /// Ensures artifact root remains stable for docs/scripts that reference it.
    #[test]
    fn artifact_root_is_stable() {
        let text = super::display_path(&super::artifact_root());
        assert!(text.ends_with("src/mediapm/examples/.artifacts/demo-online"));
    }

    /// Ensures demo runtime config surfaces the explicit default
    /// materialization order in a stable serialized form.
    #[test]
    fn demo_materialization_preference_order_matches_default_labels() {
        let labels = super::DEMO_MATERIALIZATION_PREFERENCE_ORDER
            .iter()
            .map(|method| method.as_label())
            .collect::<Vec<_>>();
        assert_eq!(labels, vec!["hardlink", "symlink", "reflink", "copy"]);
    }

    /// Ensures cleanup helpers can remove readonly-marked trees created by
    /// prior tool downloads on repeated demo runs.
    #[test]
    fn remove_dir_all_with_retry_handles_readonly_tree() {
        let temp = tempfile::tempdir().expect("tempdir");
        let tree_root = temp.path().join("readonly-tree");
        std::fs::create_dir_all(&tree_root).expect("create tree root");

        let nested = tree_root.join("nested").join("tool.bin");
        std::fs::create_dir_all(nested.parent().expect("parent")).expect("create nested parent");
        std::fs::write(&nested, b"demo").expect("write nested file");

        let mut file_permissions = std::fs::metadata(&nested).expect("metadata").permissions();
        file_permissions.set_readonly(true);
        std::fs::set_permissions(&nested, file_permissions).expect("set readonly on file");

        super::clear_readonly_bits_recursively(&tree_root);
        super::remove_dir_all_with_retry(&tree_root).expect("retrying remove should succeed");
        assert!(!tree_root.exists());
    }

    /// Ensures graceful timeout errors keep stable wording for exit-code and
    /// docs-level timeout handling contracts.
    #[test]
    fn demo_online_timeout_error_message_is_stable() {
        let message = super::DemoOnlineTimeoutError { timeout_seconds: 120 }.to_string();
        assert_eq!(
            message,
            "Online demo timed out after 120 seconds. This is usually temporary provider/network throttling—wait briefly, then rerun."
        );
    }

    /// Ensures emergency hard-timeout notice keeps one friendly sentence with
    /// no ANSI terminal control codes.
    #[test]
    fn hard_watchdog_notice_is_human_facing_and_plain_text() {
        let message = super::format_hard_watchdog_notice(915);
        assert_eq!(
            message,
            "Online demo exceeded the timeout grace period (915 seconds total). Exiting now with code 124."
        );
        assert!(!message.contains("\u{1b}"));
    }

    /// Ensures hard-watchdog grace remains enabled so graceful timeout paths
    /// can usually flush progress bars before forced process exit.
    #[test]
    fn hard_watchdog_timeout_grace_is_enabled() {
        assert!(super::DEMO_ONLINE_HARD_TIMEOUT_GRACE_SECS > 0);
    }

    /// Ensures ReplayGain payload detection requires single-track gain/peak
    /// keys plus reference loudness.
    #[test]
    fn ffprobe_payload_detection_requires_track_gain_peak_and_reference_loudness() {
        let payload_with_required_keys = serde_json::json!({
            "format": {
                "tags": {
                    "REPLAYGAIN_TRACK_GAIN": "-8.30 dB",
                    "replaygain_reference_loudness": "89.0 dB"
                }
            },
            "streams": [
                {
                    "tags": {
                        "replaygain_track_peak": "0.991"
                    }
                }
            ]
        });
        assert!(super::ffprobe_payload_has_required_replaygain_tags(&payload_with_required_keys));

        let payload_missing_reference_loudness = serde_json::json!({
            "format": {
                "tags": {
                    "replaygain_track_gain": "-8.30 dB"
                }
            },
            "streams": [
                {
                    "tags": {
                        "replaygain_track_peak": "0.991"
                    }
                }
            ]
        });
        assert!(!super::ffprobe_payload_has_required_replaygain_tags(
            &payload_missing_reference_loudness
        ));

        let payload_missing_track_peak = serde_json::json!({
            "format": {
                "tags": {
                    "replaygain_track_gain": "-8.30 dB",
                    "replaygain_reference_loudness": "89.0 dB"
                }
            }
        });
        assert!(!super::ffprobe_payload_has_required_replaygain_tags(&payload_missing_track_peak));
    }

    /// Ensures ratio rendering stays neutral for empty/objectless stores.
    #[test]
    fn store_size_ratio_uses_neutral_value_for_zero_denominator() {
        let stats = super::StoreSizeStats { without_delta_bytes: 0, with_delta_bytes: 0 };
        assert_eq!(stats.ratio_with_delta_over_without(), 1.0);
    }

    /// Ensures single-track mode rejects album-family ReplayGain tags.
    #[test]
    fn ffprobe_payload_single_track_detection_reports_unexpected_album_tags() {
        let payload = serde_json::json!({
            "format": {
                "tags": {
                    "replaygain_track_gain": "-8.30 dB",
                    "replaygain_track_peak": "0.991",
                    "replaygain_album_gain": "-8.11 dB"
                }
            }
        });

        assert_eq!(
            super::ffprobe_payload_unexpected_single_track_replaygain_tags(&payload),
            vec!["replaygain_album_gain"]
        );
    }

    /// Ensures Matroska magic-byte detection recognizes EBML headers only.
    #[test]
    fn matroska_magic_byte_detection_matches_ebml_header() {
        assert!(super::bytes_look_like_matroska(&[0x1A, 0x45, 0xDF, 0xA3, 0x93, 0x42]));
        assert!(!super::bytes_look_like_matroska(&[0x49, 0x44, 0x33]));
    }

    /// Ensures ffprobe payload checks require MKV container and both AV streams.
    #[test]
    fn ffprobe_payload_detection_requires_mkv_container_and_av_streams() {
        let valid_payload = serde_json::json!({
            "format": {
                "format_name": "matroska,webm"
            },
            "streams": [
                { "codec_type": "video" },
                { "codec_type": "audio" }
            ]
        });
        assert!(super::ffprobe_payload_has_mkv_video_and_audio(&valid_payload));

        let missing_video = serde_json::json!({
            "format": {
                "format_name": "matroska,webm"
            },
            "streams": [
                { "codec_type": "audio" }
            ]
        });
        assert!(!super::ffprobe_payload_has_mkv_video_and_audio(&missing_video));

        let non_mkv = serde_json::json!({
            "format": {
                "format_name": "mp3"
            },
            "streams": [
                { "codec_type": "video" },
                { "codec_type": "audio" }
            ]
        });
        assert!(!super::ffprobe_payload_has_mkv_video_and_audio(&non_mkv));
    }

    /// Ensures ffprobe sibling-path derivation rewrites ffmpeg binary names.
    #[test]
    fn ffprobe_path_derives_from_ffmpeg_selector() {
        let windows =
            super::derive_ffprobe_path_from_ffmpeg_command(r#"D:\tools\ffmpeg\bin\ffmpeg.exe"#)
                .expect("windows ffmpeg selector should derive ffprobe sibling");
        let windows_suffix = windows.to_string_lossy().replace('\\', "/");
        assert!(windows_suffix.ends_with("ffprobe.exe"));

        let unix = super::derive_ffprobe_path_from_ffmpeg_command("/opt/ffmpeg/bin/ffmpeg")
            .expect("unix ffmpeg selector should derive ffprobe sibling");
        let unix_suffix = unix.to_string_lossy().replace('\\', "/");
        assert!(unix_suffix.ends_with("ffprobe"));
    }

    /// Ensures non-ffmpeg selectors do not synthesize invalid ffprobe paths.
    #[test]
    fn ffprobe_path_derivation_rejects_non_ffmpeg_selectors() {
        assert!(super::derive_ffprobe_path_from_ffmpeg_command("powershell.exe").is_none());
        assert!(super::derive_ffprobe_path_from_ffmpeg_command(" ").is_none());
    }

    /// Ensures media-root sidecar projection keeps selected subtitles as
    /// root files named from the shared media output base.
    #[test]
    fn media_root_sidecars_accept_root_subtitle_file_named_from_output_base() {
        let temp = tempfile::tempdir().expect("tempdir");
        let root = temp.path();
        let output_base = "Artist - Title [rickroll]";

        std::fs::write(root.join("Artist - Title [rickroll].en.vtt"), b"WEBVTT")
            .expect("write subtitle");
        std::fs::write(root.join("Artist - Title [rickroll].jpg"), b"jpg")
            .expect("write thumbnail");
        std::fs::write(root.join("Artist - Title [rickroll].url"), b"[InternetShortcut]")
            .expect("write link");

        super::assert_flat_media_root_sidecar_families(root, output_base)
            .expect("flat media root sidecars should be accepted");
    }

    /// Ensures media-root subtitle projection rejects a dedicated `subtitles/`
    /// folder outside the `sidecars/` hierarchy.
    #[test]
    fn media_root_sidecars_reject_dedicated_subtitles_folder() {
        let temp = tempfile::tempdir().expect("tempdir");
        let root = temp.path();
        let output_base = "Artist - Title [rickroll]";

        std::fs::create_dir_all(root.join("subtitles")).expect("create subtitles folder");
        std::fs::write(root.join("Artist - Title [rickroll].en.vtt"), b"WEBVTT")
            .expect("write subtitle");
        std::fs::write(root.join("Artist - Title [rickroll].jpg"), b"jpg")
            .expect("write thumbnail");
        std::fs::write(root.join("Artist - Title [rickroll].url"), b"[InternetShortcut]")
            .expect("write link");

        let error = super::assert_flat_media_root_sidecar_families(root, output_base)
            .expect_err("subtitles folder projection should be rejected");
        assert!(error.to_string().contains("must not contain dedicated root subtitles folder"));
    }

    /// Ensures flattened non-subtitle sidecars can retain provider-native
    /// title text as long as they are aligned to the media-id suffix.
    #[test]
    fn media_root_sidecars_accept_non_subtitle_files_aligned_by_media_id_suffix() {
        let temp = tempfile::tempdir().expect("tempdir");
        let root = temp.path();
        let output_base = "Artist - Title [rickroll]";

        std::fs::write(root.join("Artist - Title [rickroll].en.vtt"), b"WEBVTT")
            .expect("write subtitle");
        std::fs::write(root.join("Artist - Title (Official Video) [rickroll].webp"), b"webp")
            .expect("write thumbnail");
        std::fs::write(
            root.join("Artist - Title (Official Video) [rickroll].url"),
            b"[InternetShortcut]",
        )
        .expect("write link");

        super::assert_flat_media_root_sidecar_families(root, output_base)
            .expect("media-id-aligned non-subtitle sidecars should be accepted");
    }
}
