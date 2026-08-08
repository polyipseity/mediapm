//! End-to-end online demo exercising managed tool provisioning.
//!
//! Default sync enabled; override via `MEDIAPM_DEMO_ONLINE_RUN_SYNC`.
//! Workflow: `yt-dlp -> ffmpeg -> media-tagger -> rsgain` on `YouTube` URL.
//!
//! # Expected post-sync hierarchy
//!
//! Paths below are relative to `<artifact_root>/media/` (`runtime.hierarchy_root_dir = "media"`).
//! Canonical machine checks: `tests/fixtures/demo_hierarchy_golden.json` (`online`) and
//! [`mediapm::demo_hierarchy_spec`]. Offline demo tree: see `mediapm_demo`.
//!
//! Shared constants: artist `Rick Astley`, title `Never Gonna Give You Up`, library root `music videos`.
//!
//! **Media id:** `youtube.dQw4w9WgXcQ` · **Raw yt-dlp video id:** `dQw4w9WgXcQ`
//!
//! ## On-disk tree
//!
//! ```text
//! media/
//! ├── music videos/
//! │   └── Rick Astley - Never Gonna Give You Up [youtube.dQw4w9WgXcQ]/
//! │       ├── sidecars/                                    [Folder]
//! │       │   ├── subtitles/                             [MediaFolder → subtitles] NON-EMPTY
//! │       │   ├── subtitles.en.vtt                         [Media → subtitles_en]
//! │       │   ├── thumbnails/                            [MediaFolder → thumbnails] NON-EMPTY
//! │       │   ├── links/                                   [MediaFolder → links] NON-EMPTY
//! │       │   │   ├── Rick Astley - Never Gonna Give You Up [dQw4w9WgXcQ].url       # yt-dlp format
//! │       │   │   ├── Rick Astley - Never Gonna Give You Up [dQw4w9WgXcQ].webloc     # yt-dlp format
//! │       │   │   └── Rick Astley - Never Gonna Give You Up [dQw4w9WgXcQ].desktop    # yt-dlp format
//! │       │   ├── archive.txt                              [Media → archive]
//! │       │   ├── description.txt                          [Media → description]
//! │       │   └── info.json                                [Media → infojson]
//! │       ├── Rick Astley - Never Gonna Give You Up [youtube.dQw4w9WgXcQ].untagged.mkv   # video_untagged
//! │       ├── Rick Astley - Never Gonna Give You Up [youtube.dQw4w9WgXcQ].mkv            # video (tagged + replaygain)
//! │       ├── Rick Astley - Never Gonna Give You Up [youtube.dQw4w9WgXcQ].en.vtt
//! │       ├── Rick Astley - Never Gonna Give You Up [youtube.dQw4w9WgXcQ].description.txt
//! │       ├── Rick Astley - Never Gonna Give You Up [youtube.dQw4w9WgXcQ].info.json
//! │       ├── Rick Astley - Never Gonna Give You Up [youtube.dQw4w9WgXcQ].thumbnail.<ext>   # glob `*.thumbnail.*`
//! │       ├── Rick Astley - Never Gonna Give You Up [youtube.dQw4w9WgXcQ].link.url          # mediapm rename format
//! │       ├── Rick Astley - Never Gonna Give You Up [youtube.dQw4w9WgXcQ].link.webloc       # mediapm rename format
//! │       ├── Rick Astley - Never Gonna Give You Up [youtube.dQw4w9WgXcQ].link.desktop      # mediapm rename format
//! │       └── folder.<ext>                                    # glob `folder.*` (id `.thumbnails.folder`)
//! └── playlists/
//!     └── rickroll.m3u8                                      # playlist, 2 entries
//! ```
//!
//! ## Link filename formats (two families)
//!
//! | Location | Format owner | Bracket id |
//! | -------- | ------------ | ---------- |
//! | `sidecars/links/` | yt-dlp `%(title)s [%(id)s].{ext}` | `dQw4w9WgXcQ` |
//! | Media folder root | mediapm `rename_files` → `.link.$1` | `youtube.dQw4w9WgXcQ` |
//!
//! See `.agents/instructions/demo-hierarchy-golden.instructions.md`.
//!
//! ## Not on disk
//!
//! `sidecars/audio.m4a`, online `.mp4` media files, `video_tagged` leaf, any path containing `__mediapm__`.
//!
//! ## Hierarchy config (`mediapm.ncl`)
//!
//! ```text
//! music videos/                                           [Folder]
//! └── ${artist} - ${title} [${id}]/                       [Folder, id=youtube.dQw4w9WgXcQ.media_folder]
//!     ├── sidecars/                                       [Folder: subtitles/, thumbnails/, links/, flat sidecars]
//!     ├── ${artist} - ${title} [${id}].untagged${video_ext}   [Media, variant=video_untagged]
//!     ├── ${artist} - ${title} [${id}]${video_ext}            [Media, variant=video]
//!     ├── (path="")                                         [MediaFolder thumbnails → .thumbnail.$1]
//!     ├── (path="")                                         [MediaFolder links → .link.$1]
//!     └── (path="")                                         [MediaFolder id `.thumbnails.folder` → folder.$1]
//! playlists/
//! └── rickroll.m3u8                                       [Playlist]
//! ```
//!
//! Resolved metadata: `video_ext` → `.mkv`. Sidecar variant map: `DEMO_SIDECAR_VARIANT_PATHS` (10 entries, no audio sidecar).
//!
//! # Verification
//!
//! ```bash
//! cargo test -p mediapm demo_hierarchy_golden -- --test-threads=1
//! cargo test -p mediapm demo_online_hierarchy_materialization -- --test-threads=1
//! cargo test -p mediapm --example mediapm_demo_online -- --skip main_is_exercised --test-threads=1
//! MEDIAPM_DEMO_ONLINE_TIMEOUT_SECS=600 cargo test -p mediapm --example mediapm_demo_online main_is_exercised -- --test-threads=1
//! cargo run -p mediapm --example mediapm_demo_online
//! ```

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

use mediapm::demo_hierarchy_spec::{
    online_demo_root_link_filename, online_demo_sidecar_link_filename,
};
use mediapm::{
    ConfigVersionSpec, GenericOutputVariantConfig, HierarchyFolderRenameRule, HierarchyNode,
    HierarchyNodeKind, HierarchyPath, MaterializationMethod, MediaMetadataValue,
    MediaMetadataValueCandidate, MediaMetadataVariantBinding, MediaPmDocument, MediaPmPaths,
    MediaPmService, MediaRuntimeStorage, MediaSourceSpec, MediaStep, MediaStepTool,
    OutputCaptureKind, OutputVariantValue, PlaylistFormat, PlaylistItemRef, SanitizeNamesConfig,
    ToolRequirement, TransformInputValue, VerifyStrategy, YtDlpOutputKind,
    YtDlpOutputVariantConfig, example_isolation, load_mediapm_document,
    load_mediapm_state_document, save_mediapm_document, save_mediapm_state_document,
};
use mediapm_cas::{CasApi, FileSystemCas, Hash};
use mediapm_conductor::{
    NickelDocument, RuntimeStoragePaths, SimpleConductor, ToolKindSpec, ToolRuntime, ToolSpec,
    decode_document, default_runtime_inherited_env_vars, encode_document,
};
use same_file::is_same_file;
use serde::Serialize;
use serde_json::json;

const DEMO_MEDIA_ID: &str = "youtube.dQw4w9WgXcQ";
const DEMO_TAGGED_HIERARCHY_ID: &str = "youtube.dQw4w9WgXcQ";
const DEMO_MEDIA_FOLDER_HIERARCHY_ID: &str = "youtube.dQw4w9WgXcQ.media_folder";
const DEMO_SOURCE_URI: &str = "https://www.youtube.com/watch?v=dQw4w9WgXcQ";
const DEMO_WORKFLOW_DESCRIPTION: &str = "Online demo pipeline downloading video + sidecars, transcoding, loudness-normalizing, and applying metadata";

const DEMO_METADATA_ARTIST_KEY: &str = "artist";

const DEMO_METADATA_TITLE_KEY: &str = "title";

const DEMO_METADATA_VIDEO_ID_KEY: &str = "id";

const DEMO_METADATA_SOURCE_LITERAL: &str = "youtube-demo";

const DEMO_EXPECTED_VIDEO_ID: &str = "dQw4w9WgXcQ";

const DEMO_EXPECTED_TITLE: &str = "Never Gonna Give You Up";
const DEMO_METADATA_ARTIST: &str = "Rick Astley";

const DEMO_EXPECTED_VIDEO_EXTENSION_WITH_DOT: &str = ".mkv";

const DEMO_OUTPUT_FILE_NAME_BASE: &str =
    "${media.metadata.artist} - ${media.metadata.title} [${media.id}]";

/// yt-dlp `--sub-langs` selector for the demo download step (shared by folder + scoped variants).
const DEMO_SAFE_SUB_LANGS: &str = "en";

const DEMO_UNTAGGED_MEDIA_FILE_NAME: &str = "${media.metadata.artist} - ${media.metadata.title} [${media.id}].untagged${media.metadata.video_ext}";

const DEMO_TAGGED_MEDIA_FILE_NAME: &str =
    "${media.metadata.artist} - ${media.metadata.title} [${media.id}]${media.metadata.video_ext}";

const DEMO_MATERIALIZATION_PREFERENCE_ORDER: [MaterializationMethod; 4] = [
    MaterializationMethod::Hardlink,
    MaterializationMethod::Symlink,
    MaterializationMethod::Reflink,
    MaterializationMethod::Copy,
];

fn final_demo_output_variant() -> &'static str {
    "video_untagged"
}

const DEMO_LIBRARY_ROOT: &str = "music videos";

const DEMO_HIERARCHY_MEDIA_ROOT_TEMPLATE: &str =
    "${media.metadata.artist} - ${media.metadata.title} [${media.id}]";

const DEMO_HIERARCHY_ROOT_TEMPLATE: &str =
    "music videos/${media.metadata.artist} - ${media.metadata.title} [${media.id}]";

const DEMO_SIDECAR_VARIANT_PATHS: [(&str, &str, &str); 10] = [
    ("subtitles_sidecars", "subtitles", "sidecars/subtitles/"),
    (
        "subtitles_en_sidecars",
        DEMO_ROOT_SELECTED_SUBTITLE_VARIANT,
        DEMO_SIDECAR_SELECTED_SUBTITLE_FILE_NAME,
    ),
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

const DEMO_ROOT_SELECTED_SUBTITLE_VARIANT: &str = "subtitles_en";

const DEMO_ROOT_SELECTED_SUBTITLE_FILE_NAME: &str =
    "${media.metadata.artist} - ${media.metadata.title} [${media.id}].en.vtt";

const DEMO_SIDECAR_SELECTED_SUBTITLE_FILE_NAME: &str = "sidecars/subtitles.en.vtt";

const DEMO_EXPECTED_YT_DLP_STEP_COUNT: usize = 1;

const DEMO_ONLINE_TIMEOUT_SECS_ENV: &str = "MEDIAPM_DEMO_ONLINE_TIMEOUT_SECS";

const DEMO_ONLINE_RUN_SYNC_ENV: &str = "MEDIAPM_DEMO_ONLINE_RUN_SYNC";

const DEMO_ONLINE_TIMEOUT_SECS_DEFAULT: u64 = 5 * 60;

const DEMO_CONDUCTOR_EXECUTABLE_TIMEOUT_SECS_ENV: &str =
    "MEDIAPM_CONDUCTOR_EXECUTABLE_TIMEOUT_SECS";

const DEMO_CONDUCTOR_EXECUTABLE_TIMEOUT_DEFAULT_SECS: u64 = 5 * 60;

const DEMO_CONDUCTOR_EXECUTABLE_TIMEOUT_RESERVE_SECS: u64 = 30;

const DEMO_ONLINE_HARD_TIMEOUT_GRACE_SECS: u64 = 15;

const DEMO_REPLAYGAIN_FFPROBE_MAX_ATTEMPTS: usize = 20;

const DEMO_REPLAYGAIN_FFPROBE_RETRY_DELAY_MILLIS: u64 = 350;

const DEMO_REPLAYGAIN_REQUIRED_TAG_KEYS: [&str; 3] =
    ["replaygain_track_gain", "replaygain_track_peak", "replaygain_reference_loudness"];

const DEMO_REPLAYGAIN_DISALLOWED_TAG_KEYS: [&str; 3] =
    ["replaygain_album_gain", "replaygain_album_peak", "replaygain_album_range"];

type ExampleResult<T> = Result<T, Box<dyn Error>>;

static DEMO_FFPROBE_COMMAND: OnceLock<String> = OnceLock::new();

#[derive(Debug, Clone, PartialEq, Eq)]
struct DemoOnlineTimeoutError {
    timeout_seconds: u64,
}

#[must_use]
fn format_graceful_timeout_notice(timeout_seconds: u64) -> String {
    format!(
        "Online demo timed out after {timeout_seconds} seconds. This usually indicates a code, workflow, or environment issue; check logs/artifacts before retrying."
    )
}

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

#[derive(Debug, Clone, PartialEq, Serialize)]
struct DemoManifest {
    generated_unix_epoch_seconds: u64,
    artifact_root: String,
    workspace_root: String,
    tool_ids: Vec<String>,
    tool_binaries: BTreeMap<String, String>,
    yt_dlp_max_concurrent_calls: i32,
    yt_dlp_max_retries: i32,
    mediapm_ncl_path: String,
    conductor_generated_ncl_path: String,
    workflow_id: String,
    workflow_step_count: usize,
    tool_update_precheck_executed: bool,
    tool_update_precheck_updated_tools: usize,
    tool_update_precheck_added_tools: usize,
    tool_update_precheck_pruned_tools: usize,
    materialization_preference_order: Vec<String>,
    materialized_demo_video_path: String,
    materialized_demo_tagged_video_path: String,
    materialized_demo_video_hardlinked_to_cas: bool,
    materialized_demo_tagged_video_hardlinked_to_cas: bool,
    materialized_demo_sidecar_paths: BTreeMap<String, String>,
    executed_instances: usize,
    cached_instances: usize,
    materialized_paths: usize,
    removed_paths: usize,
    added_tools: usize,
    updated_tools: usize,
    warning_count: usize,
    profile_path: String,
    store_size_without_delta_bytes: u64,
    store_size_with_delta_bytes: u64,
    store_size_ratio_with_delta_over_without: f64,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
struct StoreSizeStats {
    without_delta_bytes: u64,
    with_delta_bytes: u64,
}

impl StoreSizeStats {
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

/// Runtime storage derived from the example cache-root override (identity
/// behavior when unset).
fn example_runtime_storage() -> MediaRuntimeStorage {
    MediaRuntimeStorage {
        cache_root_override: std::env::var_os(example_isolation::CACHE_ROOT_ENV).map(PathBuf::from),
        ..MediaRuntimeStorage::default()
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
struct DemoRunPaths {
    artifact_root: PathBuf,
    workspace_root: PathBuf,
    manifest_path: PathBuf,
}

fn artifact_root() -> PathBuf {
    if let Some(root) = std::env::var_os(example_isolation::ARTIFACT_ROOT_ENV) {
        return PathBuf::from(root);
    }
    PathBuf::from(env!("CARGO_MANIFEST_DIR")).join("examples").join("artifacts").join("demo-online")
}

fn unix_timestamp_seconds() -> u64 {
    SystemTime::now().duration_since(UNIX_EPOCH).map_or(0, |duration| duration.as_secs())
}

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

fn demo_online_run_sync_enabled() -> ExampleResult<bool> {
    let Some(raw_value) = std::env::var_os(DEMO_ONLINE_RUN_SYNC_ENV) else {
        return Ok(true);
    };

    let normalized = raw_value.to_string_lossy().trim().to_ascii_lowercase();
    if normalized.is_empty() {
        return Ok(true);
    }

    if matches!(normalized.as_str(), "true" | "1" | "yes" | "on") {
        return Ok(true);
    }

    if matches!(normalized.as_str(), "false" | "0" | "no" | "off") {
        return Ok(false);
    }

    Err(format!(
        "{DEMO_ONLINE_RUN_SYNC_ENV} only accepts enabled (true/1/yes/on) or disabled (false/0/no/off) values; got '{normalized}'"
    )
    .into())
}

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

fn emit_watchdog_notice(message: &str) {
    eprintln!();
    eprintln!("{message}");
}

#[derive(Debug, Clone, PartialEq, Eq)]
struct DemoResolvedMetadata {
    artist: String,
    title: String,
    video_id: String,
    media_id: String,
    video_ext: String,
}

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

#[must_use]
fn resolve_demo_metadata_template(template: &str, metadata: &DemoResolvedMetadata) -> String {
    template
        .replace("${media.metadata.artist}", &metadata.artist)
        .replace("${media.metadata.title}", &metadata.title)
        .replace("${media.metadata.video_id}", &metadata.video_id)
        .replace("${media.id}", &metadata.media_id)
        .replace("${media.metadata.video_ext}", &metadata.video_ext)
}

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
        // The hierarchy title template now binds to `video` metadata,
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

fn display_path(path: &Path) -> String {
    path.display().to_string().replace('\\', "/")
}

fn reset_artifact_root() -> ExampleResult<(PathBuf, Option<tempfile::TempDir>)> {
    let root = artifact_root();
    if root.exists()
        && let Err(error) = example_isolation::remove_dir_all_with_retry(&root)
    {
        if is_share_violation_remove_error(&error) {
            let (fallback_dir, fallback_path) = example_isolation::isolated_artifact_dir()?;
            eprintln!(
                "[demo_online] canonical artifact root '{}' is locked; using fallback root '{}'",
                root.display(),
                fallback_path.display()
            );
            return Ok((fallback_path, Some(fallback_dir)));
        }

        return Err(error.into());
    }
    fs::create_dir_all(&root)?;
    Ok((root, None))
}

fn is_share_violation_remove_error(error: &(dyn Error + 'static)) -> bool {
    error.downcast_ref::<std::io::Error>().is_some_and(|io_error| {
        io_error.kind() == std::io::ErrorKind::PermissionDenied
            || io_error.raw_os_error() == Some(32)
    })
}

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

#[must_use]
fn is_hex_segment(value: &str) -> bool {
    !value.is_empty() && value.bytes().all(|byte| byte.is_ascii_hexdigit())
}

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

fn collect_store_object_hashes(cas_root: &Path) -> ExampleResult<BTreeSet<Hash>> {
    let mut hashes = BTreeSet::new();
    let objects_root = cas_root.join("v1");
    if !objects_root.exists() {
        return Ok(hashes);
    }

    collect_store_object_hashes_recursive(&objects_root, &objects_root, &mut hashes)?;

    Ok(hashes)
}

async fn summarize_store_sizes(
    cas: &FileSystemCas,
    cas_root: &Path,
) -> ExampleResult<StoreSizeStats> {
    // `cas` must be the shared handle from the service: `FileSystemCas::open`
    // on the same root would re-acquire the directory lock and hit
    // `LockContention` because the service already holds it. Cloning the
    // `Arc<FileSystemCas>` shares the same `DirectoryLockGuard`.
    let mut without_delta = 0u64;
    let mut with_delta = 0u64;

    for hash in collect_store_object_hashes(cas_root)? {
        let info = cas.stat(hash).await?;
        without_delta = without_delta.saturating_add(info.len);
        with_delta = with_delta.saturating_add(info.len);
    }

    Ok(StoreSizeStats { without_delta_bytes: without_delta, with_delta_bytes: with_delta })
}

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

fn output_is_hardlinked_to_cas_object(
    source_path: &Path,
    output_path: &Path,
) -> ExampleResult<bool> {
    if !source_path.is_file() || !output_path.is_file() {
        return Ok(false);
    }

    Ok(is_same_file(source_path, output_path)? && fs::read(source_path)? == fs::read(output_path)?)
}

async fn assert_materialized_output_hardlinked_to_cas(
    cas: &FileSystemCas,
    hierarchy_root: &Path,
    lock: &mediapm::MediaPmState,
    output_path: &Path,
) -> ExampleResult<()> {
    let relative_path = managed_relative_path(hierarchy_root, output_path)?;
    let record = lock.managed_files.get(relative_path.as_str()).ok_or_else(|| {
        std::io::Error::other(format!(
            "managed output '{relative_path}' missing from lockfile tracking"
        ))
    })?;
    let hash = record.hash.parse::<Hash>().map_err(|error| {
        std::io::Error::other(format!(
            "managed output '{relative_path}' has invalid CAS hash '{}': {error}",
            record.hash
        ))
    })?;

    let bytes = fs::read(output_path)?;
    let content_hash = Hash::from_content(&bytes);
    if content_hash != hash {
        return Err(std::io::Error::other(format!(
            "managed output '{relative_path}' content hash '{content_hash}' does not match lockfile hash '{hash}'"
        ))
        .into());
    }

    if !output_path.is_file() {
        return Err(std::io::Error::other(format!(
            "materialized output '{}' is missing on disk",
            output_path.display()
        ))
        .into());
    }

    cas.ensure_blob_materialized(hash).await.map_err(|source| {
        std::io::Error::other(format!(
            "ensure CAS blob materialized for managed output '{relative_path}' ({hash}): {source}"
        ))
    })?;
    let source_path = cas.object_path_for_hash(hash).ok_or_else(|| {
        std::io::Error::other(format!(
            "CAS store has no filesystem path for managed output '{relative_path}' ({hash})"
        ))
    })?;

    if !output_is_hardlinked_to_cas_object(&source_path, output_path)? {
        return Err(std::io::Error::other(format!(
            "materialized output '{}' is not hardlinked to CAS object '{}'",
            output_path.display(),
            source_path.display()
        ))
        .into());
    }

    Ok(())
}

#[expect(
    clippy::too_many_lines,
    reason = "this item intentionally keeps end-to-end control flow together so ordering invariants remain explicit during maintenance"
)]
fn configure_document_for_online_demo(workspace_root: &Path) -> ExampleResult<Vec<String>> {
    let mediapm_ncl = workspace_root.join("mediapm.ncl");
    // The artifact root is reset fresh on every run; start from a default
    // document when no seed `mediapm.ncl` exists yet.
    let mut document = if mediapm_ncl.exists() {
        load_mediapm_document(&mediapm_ncl)?
    } else {
        MediaPmDocument::default()
    };
    document.tools = BTreeMap::from([
        (
            "yt-dlp".to_string(),
            ToolRequirement {
                version_spec: ConfigVersionSpec::Latest,
                dependencies: BTreeMap::from([
                    ("ffmpeg".to_string(), ConfigVersionSpec::Inherit),
                    ("deno".to_string(), ConfigVersionSpec::Inherit),
                ]),
                recheck_seconds: 0,
                max_input_slots: 16,
                max_output_slots: 4,
            },
        ),
        (
            "deno".to_string(),
            ToolRequirement {
                version_spec: ConfigVersionSpec::Latest,
                dependencies: BTreeMap::new(),
                recheck_seconds: 0,
                max_input_slots: 16,
                max_output_slots: 4,
            },
        ),
        (
            "ffmpeg".to_string(),
            ToolRequirement {
                version_spec: ConfigVersionSpec::Latest,
                dependencies: BTreeMap::new(),
                recheck_seconds: 0,
                max_input_slots: 16,
                max_output_slots: 4,
            },
        ),
        (
            "rsgain".to_string(),
            ToolRequirement {
                version_spec: ConfigVersionSpec::Latest,
                dependencies: BTreeMap::from([
                    ("ffmpeg".to_string(), ConfigVersionSpec::Inherit),
                    ("sd".to_string(), ConfigVersionSpec::Inherit),
                ]),
                recheck_seconds: 0,
                max_input_slots: 16,
                max_output_slots: 4,
            },
        ),
        (
            "sd".to_string(),
            ToolRequirement {
                version_spec: ConfigVersionSpec::Latest,
                dependencies: BTreeMap::new(),
                recheck_seconds: 0,
                max_input_slots: 16,
                max_output_slots: 4,
            },
        ),
        (
            "media-tagger".to_string(),
            ToolRequirement {
                version_spec: ConfigVersionSpec::Latest,
                dependencies: BTreeMap::from([("ffmpeg".to_string(), ConfigVersionSpec::Inherit)]),
                recheck_seconds: 0,
                max_input_slots: 16,
                max_output_slots: 4,
            },
        ),
        (
            "import".to_string(),
            ToolRequirement {
                version_spec: ConfigVersionSpec::Latest,
                dependencies: BTreeMap::new(),
                recheck_seconds: 0,
                max_input_slots: 16,
                max_output_slots: 4,
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
                    OutputVariantValue::YtDlp(YtDlpOutputVariantConfig {
                        kind: YtDlpOutputKind::Primary,
                        ..Default::default()
                    }),
                ),
                (
                    "subtitles".to_string(),
                    OutputVariantValue::YtDlp(YtDlpOutputVariantConfig {
                        kind: YtDlpOutputKind::Subtitles,
                        ..Default::default()
                    }),
                ),
                (
                    "subtitles_en".to_string(),
                    OutputVariantValue::YtDlp(YtDlpOutputVariantConfig {
                        kind: YtDlpOutputKind::Subtitles,
                        capture_kind: Some(OutputCaptureKind::File),
                        // Keep yt-dlp language download selection authoritative in
                        // step options (`options.sub_langs`); this variant-level
                        // `langs` value only scopes capture/materialization so the
                        // media root can project one selected subtitle.
                        langs: "en".to_string(),
                        ..Default::default()
                    }),
                ),
                (
                    "thumbnails".to_string(),
                    OutputVariantValue::YtDlp(YtDlpOutputVariantConfig {
                        kind: YtDlpOutputKind::Thumbnails,
                        ..Default::default()
                    }),
                ),
                (
                    "description".to_string(),
                    OutputVariantValue::YtDlp(YtDlpOutputVariantConfig {
                        kind: YtDlpOutputKind::Description,
                        ..Default::default()
                    }),
                ),
                (
                    "infojson".to_string(),
                    OutputVariantValue::YtDlp(YtDlpOutputVariantConfig {
                        kind: YtDlpOutputKind::Infojson,
                        ..Default::default()
                    }),
                ),
                (
                    "links".to_string(),
                    OutputVariantValue::YtDlp(YtDlpOutputVariantConfig {
                        kind: YtDlpOutputKind::Links,
                        ..Default::default()
                    }),
                ),
                (
                    "archive".to_string(),
                    OutputVariantValue::YtDlp(YtDlpOutputVariantConfig {
                        kind: YtDlpOutputKind::Archive,
                        ..Default::default()
                    }),
                ),
            ]),
            options: BTreeMap::from([
                ("uri".to_string(), TransformInputValue::String(DEMO_SOURCE_URI.to_string())),
                (
                    "format".to_string(),
                    TransformInputValue::String(
                        "best[height<=144]/best[height<=240]/worst".to_string(),
                    ),
                ),
                (
                    "sub_langs".to_string(),
                    TransformInputValue::String(DEMO_SAFE_SUB_LANGS.to_string()),
                ),
                ("write_auto_subs".to_string(), TransformInputValue::String("true".to_string())),
            ]),
        },
        MediaStep {
            tool: MediaStepTool::Ffmpeg,
            input_variants: vec!["video".to_string()],
            output_variants: BTreeMap::from([(
                "video_untagged".to_string(),
                OutputVariantValue::Generic(GenericOutputVariantConfig {
                    kind: "primary".to_string(),
                    extension: "mkv".to_string(),
                    ..Default::default()
                }),
            )]),
            options: BTreeMap::new(),
        },
        MediaStep {
            tool: MediaStepTool::MediaTagger,
            input_variants: vec!["video_untagged".to_string()],
            output_variants: BTreeMap::from([(
                "video".to_string(),
                OutputVariantValue::YtDlp(YtDlpOutputVariantConfig {
                    kind: YtDlpOutputKind::Primary,
                    ..Default::default()
                }),
            )]),
            options: BTreeMap::from([
                (
                    "recording_mbid".to_string(),
                    TransformInputValue::String("8f3471b5-7e6a-48da-86a9-c1c07a0f47ae".to_string()),
                ),
                ("release_mbid".to_string(), TransformInputValue::String(String::new())),
                ("write_all_images".to_string(), TransformInputValue::String("false".to_string())),
                ("write_all_tags".to_string(), TransformInputValue::String("false".to_string())),
            ]),
        },
        MediaStep {
            tool: MediaStepTool::Rsgain,
            input_variants: vec!["video".to_string()],
            output_variants: BTreeMap::from([(
                "video".to_string(),
                OutputVariantValue::Generic(GenericOutputVariantConfig {
                    kind: "output_content".to_string(),
                    extension: "mkv".to_string(),
                    ..Default::default()
                }),
            )]),
            options: BTreeMap::new(),
        },
    ];

    document.media = BTreeMap::from([(
        DEMO_MEDIA_ID.to_string(),
        MediaSourceSpec {
            description: DEMO_WORKFLOW_DESCRIPTION.to_string(),
            title: DEMO_EXPECTED_TITLE.to_string(),
            artist: String::new(),
            metadata: BTreeMap::from([
                (
                    "title".to_string(),
                    MediaMetadataValue::Fallback(vec![
                        MediaMetadataValueCandidate::Variant(MediaMetadataVariantBinding {
                            variant: "infojson".to_string(),
                            metadata_key: DEMO_METADATA_TITLE_KEY.to_string(),
                            transform: None,
                        }),
                        MediaMetadataValueCandidate::Variant(MediaMetadataVariantBinding {
                            variant: "video".to_string(),
                            metadata_key: DEMO_METADATA_TITLE_KEY.to_string(),
                            transform: None,
                        }),
                        MediaMetadataValueCandidate::Literal(DEMO_EXPECTED_TITLE.to_string()),
                    ]),
                ),
                (
                    "artist".to_string(),
                    MediaMetadataValue::Fallback(vec![
                        MediaMetadataValueCandidate::Variant(MediaMetadataVariantBinding {
                            variant: "video".to_string(),
                            metadata_key: DEMO_METADATA_ARTIST_KEY.to_string(),
                            transform: None,
                        }),
                        MediaMetadataValueCandidate::Variant(MediaMetadataVariantBinding {
                            variant: "infojson".to_string(),
                            metadata_key: DEMO_METADATA_ARTIST_KEY.to_string(),
                            transform: None,
                        }),
                        MediaMetadataValueCandidate::Variant(MediaMetadataVariantBinding {
                            variant: "infojson".to_string(),
                            metadata_key: "channel".to_string(),
                            transform: None,
                        }),
                        MediaMetadataValueCandidate::Literal(DEMO_METADATA_ARTIST.to_string()),
                    ]),
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
                    MediaMetadataValue::Literal(DEMO_EXPECTED_VIDEO_EXTENSION_WITH_DOT.to_string()),
                ),
                (
                    "source".to_string(),
                    MediaMetadataValue::Literal(DEMO_METADATA_SOURCE_LITERAL.to_string()),
                ),
            ]),
            variant_hashes: BTreeMap::new(),
            steps,
        },
    )]);
    let mut media_root_children = vec![
        HierarchyNode {
            path: HierarchyPath::from(DEMO_UNTAGGED_MEDIA_FILE_NAME),
            kind: HierarchyNodeKind::Media,
            id: None,
            media_id: Some(DEMO_MEDIA_ID.to_string()),
            variant: Some(final_demo_output_variant().to_string()),
            variants: Vec::new(),
            rename_files: Vec::new(),
            format: PlaylistFormat::M3u8,
            ids: Vec::new(),
            sanitize_names: SanitizeNamesConfig::Inherit,
            children: Vec::new(),
        },
        HierarchyNode {
            path: HierarchyPath::from(DEMO_TAGGED_MEDIA_FILE_NAME),
            kind: HierarchyNodeKind::Media,
            id: Some(DEMO_TAGGED_HIERARCHY_ID.to_string()),
            media_id: Some(DEMO_MEDIA_ID.to_string()),
            variant: Some("video".to_string()),
            variants: Vec::new(),
            rename_files: Vec::new(),
            format: PlaylistFormat::M3u8,
            ids: Vec::new(),
            sanitize_names: SanitizeNamesConfig::Inherit,
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
                path: HierarchyPath::from(hierarchy_path.trim_end_matches('/')),
                kind: HierarchyNodeKind::MediaFolder,
                id: None,
                media_id: Some(DEMO_MEDIA_ID.to_string()),
                variant: None,
                variants: vec![variant.to_string()],
                rename_files: Vec::new(),
                format: PlaylistFormat::M3u8,
                ids: Vec::new(),
                sanitize_names: SanitizeNamesConfig::Inherit,
                children: Vec::new(),
            });
        } else {
            target_children.push(HierarchyNode {
                path: HierarchyPath::from(hierarchy_path.as_str()),
                kind: HierarchyNodeKind::Media,
                id: None,
                media_id: Some(DEMO_MEDIA_ID.to_string()),
                variant: Some(variant.to_string()),
                variants: Vec::new(),
                rename_files: Vec::new(),
                format: PlaylistFormat::M3u8,
                ids: Vec::new(),
                sanitize_names: SanitizeNamesConfig::Inherit,
                children: Vec::new(),
            });
        }
    }

    if !sidecar_folder_children.is_empty() {
        media_root_children.insert(
            0,
            HierarchyNode {
                path: HierarchyPath::from("sidecars"),
                kind: HierarchyNodeKind::Folder,
                id: None,
                media_id: None,
                variant: None,
                variants: Vec::new(),
                rename_files: Vec::new(),
                format: PlaylistFormat::M3u8,
                ids: Vec::new(),
                sanitize_names: SanitizeNamesConfig::Inherit,
                children: sidecar_folder_children,
            },
        );
    }

    // NOTE: Thumbnail and link variants are materialized directly to the media root
    // with path="" (empty) instead of in dedicated subfolder containers.
    // Only the sidecars/ folder should use nested directory organization.
    // This ordering keeps the writable parent available long enough to create
    // the nested `sidecars/` folder before the direct root projections are finalized.
    media_root_children.push(HierarchyNode {
        path: HierarchyPath::default(),
        kind: HierarchyNodeKind::MediaFolder,
        id: None,
        media_id: Some(DEMO_MEDIA_ID.to_string()),
        variant: None,
        variants: vec!["thumbnails".to_string()],
        rename_files: vec![HierarchyFolderRenameRule {
            pattern: "^.*\\.([^.]+)$".to_string(),
            replacement:
                "${media.metadata.artist} - ${media.metadata.title} [${media.id}].thumbnail.$1"
                    .to_string(),
        }],
        format: PlaylistFormat::M3u8,
        ids: Vec::new(),
        sanitize_names: SanitizeNamesConfig::Inherit,
        children: Vec::new(),
    });

    media_root_children.push(HierarchyNode {
        path: HierarchyPath::default(),
        kind: HierarchyNodeKind::MediaFolder,
        id: None,
        media_id: Some(DEMO_MEDIA_ID.to_string()),
        variant: None,
        variants: vec!["links".to_string()],
        rename_files: vec![HierarchyFolderRenameRule {
            pattern: "^.*\\.([^.]+)$".to_string(),
            replacement: "${media.metadata.artist} - ${media.metadata.title} [${media.id}].link.$1"
                .to_string(),
        }],
        format: PlaylistFormat::M3u8,
        ids: Vec::new(),
        sanitize_names: SanitizeNamesConfig::Inherit,
        children: Vec::new(),
    });

    // Now instantiate the extra thumbnails folder projection with folder-level
    // rename (same as the yt-dlp preset's `folder.$1` naming).
    media_root_children.push(HierarchyNode {
        path: HierarchyPath::default(),
        kind: HierarchyNodeKind::MediaFolder,
        id: Some(format!("{DEMO_MEDIA_ID}.thumbnails.folder")),
        media_id: Some(DEMO_MEDIA_ID.to_string()),
        variant: None,
        variants: vec!["thumbnails".to_string()],
        rename_files: vec![HierarchyFolderRenameRule {
            pattern: r"^.*\.([^.]*)$".to_string(),
            replacement: "folder.$1".to_string(),
        }],
        format: PlaylistFormat::M3u8,
        ids: Vec::new(),
        sanitize_names: SanitizeNamesConfig::Inherit,
        children: Vec::new(),
    });

    // NOTE: The yt-dlp preset's extra root thumbnail projection uses `folder.$1` naming
    // (`<media-id>.thumbnails.folder`). This demo now instantiates that `folder.<thumbnail_ext>` path
    // above, alongside the preset-style thumbnail/link filenames inside explicit root folders
    // (`thumbnails/` and `links/`) using separate `media_folder(path="")` projections.

    document.hierarchy = vec![
        HierarchyNode {
            path: HierarchyPath::from(DEMO_LIBRARY_ROOT),
            kind: HierarchyNodeKind::Folder,
            id: None,
            media_id: None,
            variant: None,
            variants: Vec::new(),
            rename_files: Vec::new(),
            format: PlaylistFormat::M3u8,
            ids: Vec::new(),
            sanitize_names: SanitizeNamesConfig::Inherit,
            children: vec![HierarchyNode {
                path: HierarchyPath::from(DEMO_HIERARCHY_MEDIA_ROOT_TEMPLATE),
                kind: HierarchyNodeKind::Folder,
                id: Some(DEMO_MEDIA_FOLDER_HIERARCHY_ID.to_string()),
                media_id: None,
                variant: None,
                variants: Vec::new(),
                rename_files: Vec::new(),
                format: PlaylistFormat::M3u8,
                ids: Vec::new(),
                sanitize_names: SanitizeNamesConfig::Inherit,
                children: media_root_children,
            }],
        },
        HierarchyNode {
            path: HierarchyPath::from("playlists"),
            kind: HierarchyNodeKind::Folder,
            id: None,
            media_id: None,
            variant: None,
            variants: Vec::new(),
            rename_files: Vec::new(),
            format: PlaylistFormat::M3u8,
            ids: Vec::new(),
            sanitize_names: SanitizeNamesConfig::Inherit,
            children: vec![HierarchyNode {
                path: HierarchyPath::from("rickroll.m3u8"),
                kind: HierarchyNodeKind::Playlist,
                id: None,
                media_id: None,
                variant: None,
                variants: Vec::new(),
                rename_files: Vec::new(),
                format: PlaylistFormat::M3u8,
                ids: vec![
                    PlaylistItemRef::Object {
                        id: DEMO_TAGGED_HIERARCHY_ID.to_string(),
                        path: Some("relative".to_string()),
                    },
                    PlaylistItemRef::Object {
                        id: DEMO_TAGGED_HIERARCHY_ID.to_string(),
                        path: Some("absolute".to_string()),
                    },
                ],
                sanitize_names: SanitizeNamesConfig::Inherit,
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
        hierarchy_root_dir: Some("media".to_string()),
        path_sanitization: SanitizeNamesConfig::default(),
        // Ordered file-materialization method preference.
        // Default when omitted: hardlink -> symlink -> reflink -> copy.
        materialization_preference_order: DEMO_MATERIALIZATION_PREFERENCE_ORDER.to_vec(),
        // User-owned conductor config path relative to workspace root.
        // Default: `mediapm.conductor.ncl`.
        conductor_config: Some("mediapm.conductor.ncl".to_string()),
        // Machine-managed conductor config path relative to workspace root.
        // Default: `mediapm.conductor.generated.ncl`.
        conductor_generated_config: Some("mediapm.conductor.generated.ncl".to_string()),
        // Volatile conductor state path relative to workspace root.
        // Default: `.mediapm/state.conductor.json`.
        conductor_state_config: Some(".mediapm/state.conductor.json".to_string()),
        // Conductor schema export directory relative to workspace root.
        // Default: `<mediapm_dir>/config/conductor`.
        conductor_schema_dir: Some(".mediapm/config/conductor".to_string()),
        // Explicit host default inherited env-var map.
        // Runtime still merges this map case-insensitively with host defaults.
        inherited_env_vars: {
            let host_platform = std::env::consts::OS.to_ascii_lowercase();
            let mut map = BTreeMap::new();
            map.insert(host_platform, default_runtime_inherited_env_vars().into_keys().collect());
            map
        },
        // Machine-managed mediapm state path relative to workspace root.
        // Default: `.mediapm/state.json` (JSON always-write).
        media_state_config: None,
        // Dotenv credential source path relative to workspace root.
        // Default: `.mediapm/.env`.
        env_file: Some(".mediapm/.env".to_string()),
        // Auto-generated dotenv path relative to workspace root.
        // Default: `.mediapm/.env.generated`.
        env_generated_file: Some(".mediapm/.env.generated".to_string()),
        // Embedded schema export directory policy.
        // `Some(Some(path))` keeps export enabled with an explicit default path.
        mediapm_schema_dir: Some(Some(".mediapm/config/mediapm".to_string())),
        // Enable conductor profiling so every sync run produces a per-step
        // timing profile at `.mediapm/profile.json` for latency investigation.
        profiler_enabled: true,
        // CAS integrity trusted by default; set to Some(true) to verify each
        // materialized output against its CAS record.
        verify_materialization: false,
        // Optional default runtime GC TTL in seconds.
        // Not set: inherits conductor's built-in default.
        instance_ttl_seconds: 3600,
        // CAS integrity re-verification strategies on read.
        // Default: ["modified", "sample"].
        verify_on_read: vec![VerifyStrategy::Modified, VerifyStrategy::Sample],
        // Sampling denominator for the "sample" verify-on-read strategy.
        // Default: 100.
        verify_on_read_sample_denominator: 100,
        // Timeout in seconds for the "stale" verify-on-read strategy.
        // Default: 604800 (7 days).
        verify_on_read_stale_timeout_secs: 604_800,
        // TTL in seconds for reconstructed bytes cache.
        // Default: 3600 (1 hour).
        reconstructed_cache_ttl_seconds: 3600,
        retry_impure: false,
        cache_root_override: None,
        tools: BTreeMap::new(),
    };

    save_mediapm_document(&mediapm_ncl, &document)?;
    Ok(vec![
        "yt-dlp".to_string(),
        "deno".to_string(),
        "ffmpeg".to_string(),
        "rsgain".to_string(),
        "sd".to_string(),
        "media-tagger".to_string(),
    ])
}

fn load_machine(path: &Path) -> ExampleResult<NickelDocument> {
    let raw = fs::read_to_string(path)?;
    Ok(decode_document(raw.as_bytes())?)
}

fn seed_old_synced_tools_state_for_update_precheck(
    service: &MediaPmService<mediapm_cas::InMemoryCas>,
    logical_tool_ids: &[String],
) -> ExampleResult<()> {
    service.refresh_runtime_configuration()?;

    // The generated conductor document does not exist on a fresh workspace
    // (it is first produced by a sync); start from an empty document so the
    // stale-tool seed can be applied before the first sync runs.
    let machine_path = service.paths().conductor_generated_ncl.clone();
    let mut machine: NickelDocument = if machine_path.exists() {
        decode_document(fs::read(&machine_path)?.as_slice())?
    } else {
        NickelDocument::default()
    };
    let lock = load_mediapm_state_document(&service.paths().mediapm_state_json)?;

    for logical_tool_name in logical_tool_ids {
        if logical_tool_name.eq_ignore_ascii_case("import") {
            continue;
        }

        let stale_payload = format!("stale-tool-payload::{logical_tool_name}");
        let stale_hash = Hash::from_content(stale_payload.as_bytes());
        // Generated-doc key follows the "{name}@{content_map_hash}" convention
        // so prune logic treats the seeded entry as an old version of the tool.
        let stale_tool_id = format!("{logical_tool_name}@{stale_hash}");
        let stale_relative_path = format!("legacy/{logical_tool_name}/tool.bin");

        // The seeded stale spec's content_map references `stale_hash`, and the
        // generated doc's `content_map ⊆ external_data` invariant requires a
        // matching entry for the pre-sync document to decode. Reconcile
        // rebuilds external_data from scratch (DataUsageTracker), so this
        // entry only satisfies the pre-sync invariant — it is replaced, not
        // retained, once sync runs.
        machine.external_data.insert(
            stale_hash,
            mediapm_conductor::ExternalDataEntry {
                description: Some(format!("stale payload for {logical_tool_name}")),
                save_mode: mediapm_conductor::OutputSaveMode::Saved,
            },
        );
        // The seeded spec must carry the bare logical tool id as `name` so the
        // reconcile's `already_exists` check (`spec.name == tool_id`) counts it
        // as an update rather than an addition.
        machine.tools.insert(
            stale_tool_id.clone(),
            ToolSpec {
                name: logical_tool_name.clone(),
                kind: ToolKindSpec::Executable {
                    command: vec![format!("./{stale_relative_path}")],
                    env_vars: BTreeMap::new(),
                    success_codes: vec![0],
                },
                runtime: ToolRuntime {
                    content_map: BTreeMap::from([(
                        stale_relative_path.clone(),
                        stale_hash.to_string(),
                    )]),
                    ..ToolRuntime::default()
                },
                ..ToolSpec::default()
            },
        );

        // Stale generated-doc entries (above) force re-provision; managed_tools
        // seeding is intentionally omitted so post-sync registry rows are not
        // shadowed by stale canonical_version rows during workflow execution.
    }

    fs::write(&service.paths().conductor_generated_ncl, encode_document(machine)?)?;
    save_mediapm_state_document(&service.paths().mediapm_state_json, &lock)?;

    Ok(())
}

/// Pre-seeds GitHub metadata cache entries so tool-update precheck does not
/// depend on live API responses during hermetic demo test runs.
async fn seed_tool_metadata_cache_for_demo_precheck(cache_root: &Path) -> ExampleResult<()> {
    use mediapm_conductor::cache::{Cache, CacheDomainConfig};
    use mediapm_conductor::cache_user_level::UserLevelCache;

    const METADATA_DOMAIN: &str = "tool_metadata";
    let metadata_domain = CacheDomainConfig {
        domain: METADATA_DOMAIN.to_string(),
        index_file_name: "tool_metadata.json".to_string(),
        entry_ttl_seconds: 24 * 60 * 60,
    };
    let cache = Cache::open(cache_root, &[metadata_domain])
        .await
        .map_err(|e| std::io::Error::other(format!("open tool metadata cache: {e}")))?;
    let cache = UserLevelCache::from_cache(cache);

    for (api_url, tag, hash) in [
        (
            "https://api.github.com/repos/yt-dlp/yt-dlp/releases/latest",
            "2026.07.04",
            "fdec00e0bf530dc6c3cc7b1dd780e95d9ae460e9",
        ),
        (
            "https://api.github.com/repos/denoland/deno/releases/latest",
            "v2.2.12",
            "b2c3d4e5f6a7b8c9d0e1f2a3b4c5d6e7f8a9b0c1",
        ),
        (
            "https://api.github.com/repos/complexlogic/rsgain/releases/latest",
            "v3.7",
            "c3d4e5f6a7b8c9d0e1f2a3b4c5d6e7f8a9b0c1d2",
        ),
        (
            "https://api.github.com/repos/chmln/sd/releases/latest",
            "v1.1.0",
            "d4e5f6a7b8c9d0e1f2a3b4c5d6e7f8a9b0c1d2e3",
        ),
    ] {
        cache.store_bytes(METADATA_DOMAIN, api_url, format!("{tag}\n{hash}").as_bytes()).await;
    }

    cache
        .store_bytes(
            METADATA_DOMAIN,
            "https://api.github.com/repos/BtbN/FFmpeg-Builds/releases?per_page=10",
            b"autobuild-2025-07-15-12-00",
        )
        .await;
    cache.store_bytes(METADATA_DOMAIN, "https://evermeet.cx/ffmpeg/getrelease/zip", b"8.1.2").await;

    Ok(())
}

async fn run_tools_update_precheck(
    service: &mut MediaPmService<mediapm_cas::InMemoryCas>,
    workspace_root: &Path,
) -> ExampleResult<(usize, usize, usize)> {
    let cache_root = std::env::var_os(example_isolation::CACHE_ROOT_ENV)
        .map(PathBuf::from)
        .ok_or_else(|| {
            std::io::Error::other(format!(
                "{} must be set for full-sync demo tool-update precheck",
                example_isolation::CACHE_ROOT_ENV
            ))
        })?;
    seed_tool_metadata_cache_for_demo_precheck(&cache_root).await?;

    let logical_tool_ids = configure_document_for_online_demo(workspace_root)?;
    let mut document = load_mediapm_document(&workspace_root.join("mediapm.ncl"))?;
    document.media.clear();
    document.hierarchy.clear();
    save_mediapm_document(&workspace_root.join("mediapm.ncl"), &document)?;
    seed_old_synced_tools_state_for_update_precheck(service, &logical_tool_ids)?;

    let tools_only_document = load_mediapm_document(&workspace_root.join("mediapm.ncl"))?;
    if !tools_only_document.media.is_empty() || !tools_only_document.hierarchy.is_empty() {
        return Err("tools-update precheck must start with empty media/hierarchy".into());
    }

    let summary = service.sync_tools_with_tag_update_checks(false, false).await?;
    // Stale seeds cover executable managed tools only (`import` is skipped); the
    // precheck counts `tools_updated` for those re-provisions. Companion dep
    // entries (e.g. ffmpeg requested both as a dep and explicitly) may add
    // extra update rows beyond the logical tool id count.
    let minimum_updated_tools = logical_tool_ids.len();
    if summary.updated_tools < minimum_updated_tools {
        return Err(format!(
            "tools-update precheck expected at least {} updated tools but observed {}",
            minimum_updated_tools, summary.updated_tools
        )
        .into());
    }

    Ok((summary.updated_tools, summary.added_tools, summary.pruned_tools))
}

fn resolve_tool_binaries(
    machine: &NickelDocument,
    tool_ids: &[String],
) -> ExampleResult<BTreeMap<String, String>> {
    let mut binaries = BTreeMap::new();

    for tool_id in tool_ids {
        let spec = find_managed_tool_spec(machine, tool_id)?;
        let resolved_tool_id = &spec.name;

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

        if spec.runtime.content_map.is_empty() {
            return Err(format!(
                "tool '{resolved_tool_id}' is missing content_map payload entries"
            )
            .into());
        }

        binaries.insert(resolved_tool_id.clone(), first.clone());
    }

    Ok(binaries)
}

#[must_use]
fn demo_ffprobe_command() -> &'static str {
    DEMO_FFPROBE_COMMAND.get().map_or("ffprobe", String::as_str)
}

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

fn configure_demo_ffprobe_command(
    machine: &NickelDocument,
    tool_binaries: &BTreeMap<String, String>,
) -> ExampleResult<()> {
    let ffmpeg_tool_id = resolve_managed_tool_id(machine, "ffmpeg")?;

    let ffprobe_command = tool_binaries
        .get(&ffmpeg_tool_id)
        .and_then(|command| derive_ffprobe_path_from_ffmpeg_command(command))
        .and_then(|candidate| candidate.is_file().then_some(candidate))
        .map_or_else(|| "ffprobe".to_string(), |candidate| candidate.display().to_string());

    let _ = DEMO_FFPROBE_COMMAND.set(ffprobe_command);
    Ok(())
}

#[allow(clippy::cast_possible_truncation, clippy::cast_possible_wrap)]
fn assert_yt_dlp_concurrency_policy(
    machine: &NickelDocument,
    yt_dlp_tool_id: &str,
) -> ExampleResult<i32> {
    let observed = find_managed_tool_spec(machine, yt_dlp_tool_id)
        .map_or(-1, |t| t.runtime.max_concurrent_calls as i32);

    if observed == 1 {
        return Ok(1);
    }

    if observed == 0 {
        // Tool-sync provisions executable payloads only; unset runtime limits defer
        // to conductor coordinator defaults. Keep manifest policy at the documented demo value.
        return Ok(1);
    }

    Err(format!(
        "yt-dlp default max_concurrent_calls must be 1 but observed {observed} for tool '{yt_dlp_tool_id}'"
    )
    .into())
}

#[allow(clippy::cast_possible_truncation, clippy::cast_possible_wrap)]
fn assert_yt_dlp_retry_policy(
    machine: &NickelDocument,
    yt_dlp_tool_id: &str,
) -> ExampleResult<i32> {
    let observed = find_managed_tool_spec(machine, yt_dlp_tool_id)
        .map_or(-1, |t| t.runtime.max_retries as i32);

    if observed == 1 {
        return Ok(1);
    }

    if observed == 0 {
        let workflow_id = format!("mediapm.media.{DEMO_MEDIA_ID}");
        let workflow =
            machine.workflows.iter().find(|workflow| workflow.name == workflow_id).ok_or_else(
                || format!("machine config is missing managed workflow '{workflow_id}'"),
            )?;
        let yt_dlp_steps = workflow
            .steps
            .iter()
            .filter(|step| step.tool.eq_ignore_ascii_case("yt-dlp"))
            .collect::<Vec<_>>();
        if !yt_dlp_steps.is_empty() && yt_dlp_steps.iter().all(|step| step.max_retries == 1) {
            return Ok(1);
        }
    }

    Err(format!(
        "yt-dlp default max_retries must be 1 but observed {observed} for tool '{yt_dlp_tool_id}'"
    )
    .into())
}

fn assert_demo_workflow_shape(machine: &NickelDocument) -> ExampleResult<(String, usize)> {
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
        .iter()
        .find(|w| w.name == workflow_id)
        .ok_or_else(|| format!("machine config is missing managed workflow '{workflow_id}'"))?;

    if workflow.name != workflow_id {
        return Err(format!(
            "managed workflow '{workflow_id}' must set name='{workflow_id}' but observed '{}'",
            workflow.name
        )
        .into());
    }

    if workflow.description.as_deref() != Some(DEMO_WORKFLOW_DESCRIPTION) {
        return Err(format!(
            "managed workflow '{workflow_id}' must mirror description='{}' but observed '{}'",
            DEMO_WORKFLOW_DESCRIPTION,
            workflow.description.as_deref().unwrap_or("<none>")
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
    let required_order = ["yt-dlp", "ffmpeg", "media-tagger", "ffmpeg", "rsgain"];

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

fn bytes_look_like_matroska(bytes: &[u8]) -> bool {
    // Matroska/WebM containers begin with the EBML header marker.
    bytes.starts_with(&[0x1A, 0x45, 0xDF, 0xA3])
}

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

#[must_use]
fn lowercase_extension(path: &Path) -> Option<String> {
    path.extension().and_then(|value| value.to_str()).map(str::to_ascii_lowercase)
}

#[must_use]
fn is_subtitle_extension(extension: &str) -> bool {
    matches!(
        extension,
        "vtt" | "srt" | "ass" | "ssa" | "lrc" | "ttml" | "srv1" | "srv2" | "srv3" | "json3"
    )
}

#[must_use]
fn is_image_extension(extension: &str) -> bool {
    matches!(extension, "jpg" | "jpeg" | "png" | "webp" | "avif" | "gif" | "bmp" | "tiff")
}

fn assert_sidecar_directory_family_content(variant: &str, directory: &Path) -> ExampleResult<()> {
    let files = collect_regular_files_recursive(directory)?;
    let extensions = files.iter().filter_map(|path| lowercase_extension(path)).collect::<Vec<_>>();

    match variant {
        "subtitles" if !extensions.iter().any(|extension| is_subtitle_extension(extension)) => {
            return Err(format!(
                "expected sidecar variant '{variant}' at '{}' to contain subtitle files",
                directory.display()
            )
            .into());
        }
        "thumbnails" if !extensions.iter().any(|extension| is_image_extension(extension)) => {
            return Err(format!(
                "expected sidecar variant '{variant}' at '{}' to contain thumbnail image files",
                directory.display()
            )
            .into());
        }
        "links" => {
            assert_sidecar_links_directory_has_exact_public_filenames(directory)?;
        }
        _ => {}
    }

    Ok(())
}

// yt-dlp-format basenames under `sidecars/links/` — see demo-hierarchy-golden.instructions.md.
fn assert_sidecar_links_directory_has_exact_public_filenames(
    directory: &Path,
) -> ExampleResult<()> {
    for extension in ["url", "webloc", "desktop"] {
        let expected_name = online_demo_sidecar_link_filename(extension);
        let path = directory.join(&expected_name);
        if !path.is_file() {
            let files = collect_regular_files_recursive(directory)?;
            let observed: Vec<String> = files
                .iter()
                .filter_map(|entry| {
                    entry.file_name().and_then(|value| value.to_str()).map(str::to_string)
                })
                .collect();
            return Err(format!(
                "expected sidecar links directory '{}' to contain exact file '{}' but found: {observed:?}",
                directory.display(),
                expected_name
            )
            .into());
        }
    }

    Ok(())
}

#[allow(clippy::too_many_lines)]
fn assert_flat_media_root_sidecar_families(
    interpolated_root: &Path,
    expected_output_base: &str,
) -> ExampleResult<()> {
    let expected_media_id =
        parse_jellyfin_root_folder_name(expected_output_base).map(|(_, _, media_id)| media_id);
    let expected_media_suffixes = expected_media_id
        .as_deref()
        .map(|media_id| {
            let mut suffixes = vec![media_id.to_string()];
            if let Some((_, raw_video_id)) = media_id.rsplit_once('.')
                && !raw_video_id.is_empty()
            {
                suffixes.push(raw_video_id.to_string());
            }
            suffixes
        })
        .unwrap_or_default();

    let root_files = fs::read_dir(interpolated_root)?
        .flatten()
        .map(|entry| entry.path())
        .filter(|path| path.is_file())
        .collect::<Vec<_>>();

    let sidecars_root = interpolated_root.join("sidecars");
    if !sidecars_root.is_dir() {
        return Err(format!("expected sidecar root '{}' to exist", sidecars_root.display()).into());
    }

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

    let thumbnails_files = collect_regular_files_recursive(interpolated_root)?
        .into_iter()
        .filter(|path| {
            path.file_name()
                .and_then(|value| value.to_str())
                .is_some_and(|name| name.contains(".thumbnail."))
        })
        .collect::<Vec<_>>();

    if thumbnails_files.is_empty() {
        return Err(format!(
            "expected root thumbnail projection files in '{}' to exist",
            interpolated_root.display()
        )
        .into());
    }

    // mediapm root projection basenames — exact match via demo_hierarchy_spec helpers.
    for extension in ["url", "webloc", "desktop"] {
        let expected_name = online_demo_root_link_filename(extension);
        let path = interpolated_root.join(&expected_name);
        if !path.is_file() {
            let observed: Vec<String> = collect_regular_files_recursive(interpolated_root)?
                .into_iter()
                .filter(|entry| {
                    entry
                        .file_name()
                        .and_then(|value| value.to_str())
                        .is_some_and(|name| name.contains(".link."))
                })
                .filter_map(|entry| {
                    entry.file_name().and_then(|value| value.to_str()).map(str::to_string)
                })
                .collect();
            return Err(format!(
                "expected exact root link projection file '{}' in '{}' but observed link files: {observed:?}",
                path.display(),
                interpolated_root.display()
            )
            .into());
        }
    }

    let links_files = collect_regular_files_recursive(interpolated_root)?
        .into_iter()
        .filter(|path| {
            path.file_name()
                .and_then(|value| value.to_str())
                .is_some_and(|name| name.contains(".link."))
        })
        .collect::<Vec<_>>();

    assert_root_projection_sidecar_names_align(
        interpolated_root,
        expected_output_base,
        &expected_media_suffixes,
        &thumbnails_files,
        &links_files,
    )?;

    Ok(())
}

fn assert_root_projection_sidecar_names_align(
    interpolated_root: &Path,
    expected_output_base: &str,
    expected_media_suffixes: &[String],
    thumbnails_files: &[PathBuf],
    links_files: &[PathBuf],
) -> ExampleResult<()> {
    let root_projection_files =
        thumbnails_files.iter().chain(links_files.iter()).cloned().collect::<Vec<_>>();

    if !root_projection_files.iter().all(|path| {
        path.file_name().and_then(|value| value.to_str()).is_some_and(|name| {
            if name.contains(".link.") {
                ["url", "webloc", "desktop"]
                    .iter()
                    .any(|extension| name == online_demo_root_link_filename(extension))
            } else {
                name.starts_with(expected_output_base)
                    || expected_media_suffixes
                        .iter()
                        .any(|media_suffix| name.contains(&format!(" [{media_suffix}].")))
            }
        })
    }) {
        return Err(format!(
            "expected root thumbnail/link sidecar names in '{}' to align with media output base '{}' or media-id suffix: {:?}",
            interpolated_root.display(),
            expected_output_base,
            root_projection_files
                .iter()
                .filter_map(|path| path.file_name().and_then(|value| value.to_str()))
                .collect::<Vec<_>>()
        )
        .into());
    }

    Ok(())
}

fn assert_no_mediapm_marker_in_hierarchy_paths(interpolated_root: &Path) -> ExampleResult<()> {
    let files = collect_regular_files_recursive(interpolated_root)?;
    for path in files {
        if path.to_string_lossy().contains("__mediapm__") {
            return Err(format!(
                "expected no materialized hierarchy path to contain '__mediapm__' but found '{}'",
                path.display()
            )
            .into());
        }
    }

    Ok(())
}

#[must_use]
fn ffprobe_payload_has_required_replaygain_tags(payload: &serde_json::Value) -> bool {
    let observed_keys = ffprobe_payload_observed_tag_keys(payload);

    DEMO_REPLAYGAIN_REQUIRED_TAG_KEYS.iter().all(|key| observed_keys.contains(*key))
}

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

fn expected_demo_media_root_name() -> String {
    format!("{DEMO_METADATA_ARTIST} - {DEMO_EXPECTED_TITLE} [{DEMO_MEDIA_ID}]")
}

fn resolve_interpolated_demo_root(hierarchy_root: &Path) -> ExampleResult<PathBuf> {
    let parent = hierarchy_root.join(DEMO_LIBRARY_ROOT);
    if !parent.is_dir() {
        return Err(
            format!("expected Jellyfin root '{}' to exist after sync", parent.display()).into()
        );
    }

    let expected_root = parent.join(expected_demo_media_root_name());
    if expected_root.join("sidecars").join("info.json").is_file() {
        return Ok(expected_root);
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
        let observed_dirs = fs::read_dir(&parent)?
            .flatten()
            .map(|entry| entry.file_name().to_string_lossy().to_string())
            .collect::<Vec<_>>();
        return Err(format!(
            "expected exactly one demo media root under '{}', observed {} (dirs: {observed_dirs:?})",
            parent.display(),
            candidates.len()
        )
        .into());
    }

    Ok(candidates.remove(0))
}

#[expect(
    clippy::too_many_lines,
    reason = "this item intentionally keeps end-to-end control flow together so ordering invariants remain explicit during maintenance"
)]
fn resolve_demo_output_paths(
    hierarchy_root: &Path,
) -> ExampleResult<(PathBuf, PathBuf, BTreeMap<String, PathBuf>)> {
    let interpolated_root = resolve_interpolated_demo_root(hierarchy_root)?;
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
        let observed_files = fs::read_dir(&interpolated_root)?
            .flatten()
            .map(|entry| entry.file_name().to_string_lossy().to_string())
            .collect::<Vec<_>>();
        return Err(format!(
            "expected stable untagged media output '{}' to exist after sync; observed files in media root: {observed_files:?}",
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
    assert_no_mediapm_marker_in_hierarchy_paths(&interpolated_root)?;

    Ok((video_path, tagged_video_path, sidecar_paths))
}

#[expect(
    clippy::too_many_lines,
    reason = "this example keeps end-to-end demo orchestration and artifact assertions in one function for traceability"
)]
async fn run_online_demo(sync_timeout: Duration) -> ExampleResult<DemoRunPaths> {
    let _ = tracing_subscriber::fmt()
        .with_env_filter(tracing_subscriber::EnvFilter::from_default_env())
        .with_writer(std::io::stderr)
        .try_init();

    let (root, _fallback_artifact) = reset_artifact_root()?;
    let workspace_root = root.clone();
    let runtime_storage = example_runtime_storage();

    // Phase 1: tools update precheck with an in-memory conductor facade.
    //
    // `sync_tools*` internally opens the persistent runtime CAS store to
    // import managed tool payload bytes.  The in-memory facade avoids holding
    // the CAS store open while it is being written to by the tool sync.
    let mut service = {
        let cas = mediapm_cas::InMemoryCas::new();
        let conductor =
            SimpleConductor::new(RuntimeStoragePaths::new(&workspace_root.join(".mediapm")), cas);
        MediaPmService::new_with_runtime_storage_overrides(
            conductor,
            MediaPmPaths::from_root(&workspace_root),
            runtime_storage.clone(),
        )
    };
    let (precheck_updated_tools, precheck_added_tools, precheck_pruned_tools) =
        run_tools_update_precheck(&mut service, &workspace_root).await?;
    let logical_tool_ids = configure_document_for_online_demo(&workspace_root)?;

    // Phase 2: full library sync with a filesystem-backed CAS so that tool
    // payload bytes (stored on disk by the precheck) are visible to workflow
    // execution.
    let mut sync_service =
        MediaPmService::new_fs_at_with_runtime_storage_overrides(&workspace_root, runtime_storage)
            .await
            .map_err(|error| format!("opening filesystem sync service: {error}"))?;

    eprintln!(
        "[demo_online] starting sync (timeout={}s) in '{}'",
        sync_timeout.as_secs(),
        workspace_root.display()
    );

    let summary = sync_service
        .sync_library_with_tag_update_checks(true, false)
        .await
        .map_err(|error| format!("online demo sync failed: {error}"))?;

    eprintln!(
        "[demo_online] sync complete; rendering conductor timing profile (workflow scope only)..."
    );
    if !summary.warnings.is_empty() {
        eprintln!("[demo_online] sync warnings ({count}):", count = summary.warnings.len());
        for warning in &summary.warnings {
            eprintln!("[demo_online]   - {warning}");
        }
    }
    eprintln!(
        "[demo_online] sync summary: executed_instances={} cached_instances={} materialized_paths={}",
        summary.executed_instances, summary.cached_instances, summary.materialized_paths
    );
    // (profile timing intentionally omitted: no print_profile_timing API in new conductor)

    eprintln!("[demo_online] running post-sync verification and artifact summary...");
    let machine = load_machine(&sync_service.paths().conductor_generated_ncl)?;
    let tool_binaries = resolve_tool_binaries(&machine, &logical_tool_ids)?;
    configure_demo_ffprobe_command(&machine, &tool_binaries)?;

    let tool_ids = tool_binaries.keys().cloned().collect::<Vec<_>>();

    let yt_dlp_tool_id = resolve_managed_tool_id(&machine, "yt-dlp")?;
    let yt_dlp_max_concurrent_calls = assert_yt_dlp_concurrency_policy(&machine, &yt_dlp_tool_id)?;
    let yt_dlp_max_retries = assert_yt_dlp_retry_policy(&machine, &yt_dlp_tool_id)?;
    let (workflow_id, workflow_step_count) = assert_demo_workflow_shape(&machine)?;
    let hierarchy_root = sync_service.resolve_effective_paths()?.hierarchy_root_dir;
    let (output_video_path, output_tagged_video_path, output_sidecar_paths) =
        resolve_demo_output_paths(&hierarchy_root)?;
    assert_tagged_media_replaygain_tags(&output_tagged_video_path).await?;
    let cas_root = sync_service.paths().runtime_root.join("store");
    let lock = load_mediapm_state_document(&sync_service.paths().mediapm_state_json)?;
    let cas = sync_service.conductor().cas();
    assert_materialized_output_hardlinked_to_cas(cas, &hierarchy_root, &lock, &output_video_path)
        .await?;
    assert_materialized_output_hardlinked_to_cas(
        cas,
        &hierarchy_root,
        &lock,
        &output_tagged_video_path,
    )
    .await?;
    let materialized_demo_video_hardlinked_to_cas = true;
    let materialized_demo_tagged_video_hardlinked_to_cas = true;
    let store_size_stats = summarize_store_sizes(sync_service.conductor().cas(), &cas_root).await?;
    let materialization_preference_order = DEMO_MATERIALIZATION_PREFERENCE_ORDER
        .iter()
        .map(|method| format!("{method:?}"))
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
        mediapm_ncl_path: display_path(&sync_service.paths().mediapm_ncl),
        conductor_generated_ncl_path: display_path(&sync_service.paths().conductor_generated_ncl),
        workflow_id,
        workflow_step_count,
        tool_update_precheck_executed: true,
        tool_update_precheck_updated_tools: precheck_updated_tools,
        tool_update_precheck_added_tools: precheck_added_tools,
        tool_update_precheck_pruned_tools: precheck_pruned_tools,
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
        materialized_paths: summary.materialized_paths,
        removed_paths: summary.removed_paths,
        added_tools: summary.added_tools,
        updated_tools: summary.updated_tools,
        warning_count: summary.warnings.len(),
        profile_path: display_path(&sync_service.paths().runtime_root.join("profile.json")),
        store_size_without_delta_bytes: store_size_stats.without_delta_bytes,
        store_size_with_delta_bytes: store_size_stats.with_delta_bytes,
        store_size_ratio_with_delta_over_without: store_size_stats.ratio_with_delta_over_without(),
    };

    let manifest_path = root.join("manifest.json");
    write_json_file(&manifest_path, &manifest)?;

    Ok(DemoRunPaths { artifact_root: root, workspace_root, manifest_path })
}

fn logical_name_from_managed_tool_id(tool_id: &str) -> Option<&str> {
    if let Some((selector, _)) = tool_id.split_once('@') {
        if let Some(stripped) = selector.strip_prefix("mediapm.tools.") {
            let (logical_name, _) = stripped.split_once('+')?;
            return (!logical_name.is_empty()).then_some(logical_name);
        }
        return (!selector.is_empty()).then_some(selector);
    }

    if let Some(stripped) = tool_id.strip_prefix("mediapm.tools.") {
        let (logical_name, _) = stripped.split_once('+')?;
        return (!logical_name.is_empty()).then_some(logical_name);
    }

    (!tool_id.is_empty()).then_some(tool_id)
}

/// Find the active managed tool spec for a logical tool id.
///
/// The generated doc may hold several specs with the same bare name: pruned
/// stale versions keep the name with an emptied `content_map` while the active
/// version carries the payload map. Delegates to the reconcile's canonical
/// [`find_active_tool_spec`](mediapm::find_active_tool_spec) so demo
/// resolution stays identical to the skip-path preference (non-empty content
/// map wins, any matching spec as fallback).
fn find_managed_tool_spec<'a>(
    machine: &'a NickelDocument,
    logical_name: &str,
) -> ExampleResult<&'a ToolSpec> {
    mediapm::find_active_tool_spec(machine, logical_name).map(|(_, spec)| spec).ok_or_else(|| {
        format!(
            "machine config is missing immutable managed tool id for logical tool '{logical_name}'"
        )
        .into()
    })
}

fn resolve_managed_tool_id(machine: &NickelDocument, logical_name: &str) -> ExampleResult<String> {
    Ok(find_managed_tool_spec(machine, logical_name)?.name.clone())
}

/// Detects a CI environment from standard CI variables. Only the embedded
/// `main_is_exercised` test consults this; `main()` stays deterministic given
/// environment inputs.
#[cfg(test)]
fn ci_mode_detected() -> bool {
    std::env::var("CI")
        .is_ok_and(|v| !v.to_ascii_lowercase().is_empty() && v != "0" && v != "false")
        || std::env::var("GITHUB_ACTIONS").is_ok()
        || std::env::var("GITLAB_CI").is_ok()
        || std::env::var("CIRCLECI").is_ok()
        || std::env::var("TRAVIS").is_ok()
        || std::env::var("BUILDKITE").is_ok()
        || std::env::var("DRONE").is_ok()
}

#[expect(clippy::unused_async)]
async fn run_online_demo_config_only() -> ExampleResult<DemoRunPaths> {
    let (root, _fallback_artifact) = reset_artifact_root()?;
    let workspace_root = root.clone();

    configure_document_for_online_demo(&workspace_root)?;

    // Write minimal manifest indicating config-only run
    let manifest = json!({
        "run_mode": "config-only",
        "artifact_root": display_path(&root),
        "workspace_root": display_path(&workspace_root),
    });

    let manifest_path = root.join("manifest.json");
    write_json_file(&manifest_path, &manifest)?;

    Ok(DemoRunPaths { artifact_root: root, workspace_root, manifest_path })
}

#[tokio::main]
async fn main() -> ExampleResult<()> {
    let run_sync = demo_online_run_sync_enabled()?;
    if !run_sync {
        let paths = run_online_demo_config_only().await?;
        println!("generated artifacts root: {}", paths.artifact_root.display());
        println!("generated workspace root: {}", paths.workspace_root.display());
        println!("manifest: {}", paths.manifest_path.display());
        println!("sync executed: false");
        return Ok(());
    }

    let sync_timeout = online_demo_timeout()?;
    configure_demo_conductor_executable_timeout(sync_timeout);
    let hard_timeout_guard = spawn_hard_timeout_guard(sync_timeout);

    let paths = match run_online_demo(sync_timeout).await {
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
    };

    println!("generated artifacts root: {}", paths.artifact_root.display());
    println!("generated workspace root: {}", paths.workspace_root.display());
    println!("manifest: {}", paths.manifest_path.display());
    println!("sync executed: true");

    Ok(())
}

#[cfg(test)]
mod tests {
    use mediapm::example_isolation::{self, IsolatedExampleRoots};

    /// Ensures sync-mode override parser accepts disabled values (reduced mode).
    #[test]
    fn run_sync_override_accepts_disabled_tokens() {
        let previous = std::env::var(super::DEMO_ONLINE_RUN_SYNC_ENV).ok();

        // SAFETY: test mutates one process env key in a controlled scope and
        // restores the previous value before exit.
        unsafe {
            std::env::set_var(super::DEMO_ONLINE_RUN_SYNC_ENV, "false");
        }
        let result = super::demo_online_run_sync_enabled();

        // SAFETY: restore previous env var value for test isolation.
        unsafe {
            if let Some(value) = previous {
                std::env::set_var(super::DEMO_ONLINE_RUN_SYNC_ENV, value);
            } else {
                std::env::remove_var(super::DEMO_ONLINE_RUN_SYNC_ENV);
            }
        }

        assert!(matches!(result, Ok(false)), "disabled run-sync tokens must select reduced mode");
    }

    /// Ensures demo-online sync override allows unset environment (full mode).
    #[test]
    fn run_sync_override_validator_allows_unset_env() {
        let previous = std::env::var(super::DEMO_ONLINE_RUN_SYNC_ENV).ok();
        // SAFETY: test mutates one process env key in a controlled scope and
        // restores the previous value before exit.
        unsafe {
            std::env::remove_var(super::DEMO_ONLINE_RUN_SYNC_ENV);
        }

        let result = super::demo_online_run_sync_enabled();

        // SAFETY: restore previous env var value for test isolation.
        unsafe {
            if let Some(value) = previous {
                std::env::set_var(super::DEMO_ONLINE_RUN_SYNC_ENV, value);
            }
        }

        assert!(matches!(result, Ok(true)), "unset run-sync override should select full mode");
    }

    /// Ensures sync-mode override parser still rejects unknown tokens.
    #[test]
    fn run_sync_override_rejects_invalid_tokens() {
        let previous = std::env::var(super::DEMO_ONLINE_RUN_SYNC_ENV).ok();

        // SAFETY: test mutates one process env key in a controlled scope and
        // restores the previous value before exit.
        unsafe {
            std::env::set_var(super::DEMO_ONLINE_RUN_SYNC_ENV, "maybe");
        }
        let result = super::demo_online_run_sync_enabled();

        // SAFETY: restore previous env var value for test isolation.
        unsafe {
            if let Some(value) = previous {
                std::env::set_var(super::DEMO_ONLINE_RUN_SYNC_ENV, value);
            } else {
                std::env::remove_var(super::DEMO_ONLINE_RUN_SYNC_ENV);
            }
        }

        assert!(result.is_err(), "unknown run-sync tokens must be rejected");
    }

    /// Points example env vars at unique tempdirs for the guard's lifetime.
    /// Executes the documented example entry point via `main()` in full-sync
    /// mode. The online path is nondeterministic (network + external tools), so
    /// CI skips it via `ci_mode_detected()`.
    #[test]
    fn main_is_exercised() {
        if super::ci_mode_detected() {
            eprintln!(
                "[demo_online] skipping main_is_exercised in CI; \
                 the full-sync online path is nondeterministic and needs \
                 network and external tools"
            );
            return;
        }

        let _isolated = IsolatedExampleRoots::with_cache();

        let previous = std::env::var(super::DEMO_ONLINE_RUN_SYNC_ENV).ok();
        // SAFETY: test mutates one process env key in a controlled scope and
        // restores the previous value before exit.
        unsafe {
            std::env::set_var(super::DEMO_ONLINE_RUN_SYNC_ENV, "true");
        }

        super::main().expect("example main should run to completion");

        // SAFETY: restore previous env var value for test isolation.
        unsafe {
            if let Some(value) = previous {
                std::env::set_var(super::DEMO_ONLINE_RUN_SYNC_ENV, value);
            } else {
                std::env::remove_var(super::DEMO_ONLINE_RUN_SYNC_ENV);
            }
        }
    }

    /// Ensures artifact root remains stable for docs/scripts that reference it.
    #[test]
    fn artifact_root_is_stable() {
        let previous_artifact_root = std::env::var_os(example_isolation::ARTIFACT_ROOT_ENV);
        // SAFETY: test clears one process env key in a controlled scope and
        // restores the previous value before exit.
        unsafe {
            std::env::remove_var(example_isolation::ARTIFACT_ROOT_ENV);
        }

        let text = super::display_path(&super::artifact_root());
        assert!(text.ends_with("src/mediapm/examples/artifacts/demo-online"));

        // SAFETY: restore previous env var value for test isolation.
        unsafe {
            match &previous_artifact_root {
                Some(value) => std::env::set_var(example_isolation::ARTIFACT_ROOT_ENV, value),
                None => std::env::remove_var(example_isolation::ARTIFACT_ROOT_ENV),
            }
        }
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
        let temp = mediapm_utils::temp::artifact_dir().expect("tempdir");
        let tree_root = temp.path().join("readonly-tree");
        std::fs::create_dir_all(&tree_root).expect("create tree root");

        let nested = tree_root.join("nested").join("tool.bin");
        std::fs::create_dir_all(nested.parent().expect("parent")).expect("create nested parent");
        std::fs::write(&nested, b"demo").expect("write nested file");

        let mut file_permissions = std::fs::metadata(&nested).expect("metadata").permissions();
        file_permissions.set_readonly(true);
        std::fs::set_permissions(&nested, file_permissions).expect("set readonly on file");

        // Also mark the parent directory readonly: on Unix this prevents
        // remove_dir_all from removing entries inside it, which is the real
        // scenario caused by the materializer's ensure_managed_path_readonly.
        let nested_dir = nested.parent().expect("parent");
        let mut dir_permissions =
            std::fs::metadata(nested_dir).expect("dir metadata").permissions();
        dir_permissions.set_readonly(true);
        std::fs::set_permissions(nested_dir, dir_permissions).expect("set readonly on dir");

        example_isolation::remove_dir_all_with_retry(&tree_root)
            .expect("retrying remove should succeed");
        assert!(!tree_root.exists());
    }

    /// Ensures graceful timeout errors keep stable wording for exit-code and
    /// docs-level timeout handling contracts.
    #[test]
    fn demo_online_timeout_error_message_is_stable() {
        let message = super::DemoOnlineTimeoutError { timeout_seconds: 120 }.to_string();
        assert_eq!(
            message,
            "Online demo timed out after 120 seconds. This usually indicates a code, workflow, or environment issue; check logs/artifacts before retrying."
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

    /// Ensures additive sidecar layout accepts root subtitle + direct root
    /// thumbnail/link file projections while `sidecars/` also exists.
    #[test]
    fn media_root_sidecars_accept_root_subtitle_file_named_from_output_base() {
        let temp = mediapm_utils::temp::artifact_dir().expect("tempdir");
        let root = temp.path();
        let output_base = "Rick Astley - Never Gonna Give You Up [youtube.dQw4w9WgXcQ]";

        std::fs::create_dir_all(root.join("sidecars")).expect("create sidecars folder");
        std::fs::write(root.join(format!("{output_base}.en.vtt")), b"WEBVTT")
            .expect("write subtitle");
        std::fs::write(root.join(format!("{output_base}.thumbnail.jpg")), b"jpg")
            .expect("write thumbnail");
        std::fs::write(root.join(format!("{output_base}.link.url")), b"[InternetShortcut]")
            .expect("write link");
        std::fs::write(root.join(format!("{output_base}.link.webloc")), b"webloc")
            .expect("write webloc link");
        std::fs::write(root.join(format!("{output_base}.link.desktop")), b"desktop")
            .expect("write desktop link");

        super::assert_flat_media_root_sidecar_families(root, output_base)
            .expect("flat media root sidecars should be accepted");
    }

    /// Ensures additive sidecar layout still requires dedicated sidecars
    /// directory in addition to root projections.
    #[test]
    fn media_root_sidecars_require_dedicated_sidecars_folder() {
        let temp = mediapm_utils::temp::artifact_dir().expect("tempdir");
        let root = temp.path();
        let output_base = "Artist - Title [youtube.dQw4w9WgXcQ]";

        std::fs::write(root.join("Artist - Title [youtube.dQw4w9WgXcQ].en.vtt"), b"WEBVTT")
            .expect("write subtitle");
        std::fs::write(root.join("Artist - Title [youtube.dQw4w9WgXcQ].thumbnail.jpg"), b"jpg")
            .expect("write thumbnail");
        std::fs::write(
            root.join("Artist - Title [youtube.dQw4w9WgXcQ].link.url"),
            b"[InternetShortcut]",
        )
        .expect("write link");

        let error = super::assert_flat_media_root_sidecar_families(root, output_base)
            .expect_err("missing sidecars folder should be rejected");
        assert!(error.to_string().contains("expected sidecar root"));
    }

    /// Ensures root-projected thumbnails may use provider-native title text when aligned by
    /// media-id suffix; link projections must match exact mediapm rename basenames.
    #[test]
    fn media_root_sidecars_accept_thumbnail_files_aligned_by_media_id_suffix() {
        let temp = mediapm_utils::temp::artifact_dir().expect("tempdir");
        let root = temp.path();
        let output_base = "Rick Astley - Never Gonna Give You Up [youtube.dQw4w9WgXcQ]";

        std::fs::create_dir_all(root.join("sidecars")).expect("create sidecars folder");
        std::fs::write(root.join(format!("{output_base}.en.vtt")), b"WEBVTT")
            .expect("write subtitle");
        std::fs::write(
            root.join("Artist - Title (Official Video) [youtube.dQw4w9WgXcQ].thumbnail.webp"),
            b"webp",
        )
        .expect("write thumbnail");
        std::fs::write(root.join(format!("{output_base}.link.url")), b"[InternetShortcut]")
            .expect("write link");
        std::fs::write(root.join(format!("{output_base}.link.webloc")), b"webloc")
            .expect("write webloc link");
        std::fs::write(root.join(format!("{output_base}.link.desktop")), b"desktop")
            .expect("write desktop link");

        super::assert_flat_media_root_sidecar_families(root, output_base)
            .expect("media-id-aligned non-subtitle sidecars should be accepted");
    }
}
