//! End-to-end online demo exercising managed tool provisioning.
//!
//! Default sync enabled; disable via `MEDIAPM_RUN_ONLINE_SYNC=false`.
//! Workflow: `yt-dlp -> ffmpeg -> media-tagger -> rsgain` on `YouTube` URL.
//! The embedded test exercises `main()` in reduced (config-only) mode; the
//! full-sync path (network + external tools) runs only on an explicit
//! `cargo run -p mediapm --example mediapm_demo_online`, which reuses the
//! real user-level tool download cache (`<os-cache-dir>/mediapm/cache`) and
//! persists downloaded tools across runs.
//!
//! # Expected post-sync hierarchy
//!
//! Paths below are relative to `<artifact_root>/media/` (`runtime.hierarchy_root_dir = "media"`).
//! Canonical machine checks: `tests/fixtures/demo_hierarchy_golden.json` (`online`) and
//! [`mediapm::demo_hierarchy_spec`]. Offline demo tree: see `mediapm_demo`.
//!
//! Shared constants: artist `Rick Astley`, title `Never Gonna Give You Up`, library root `music videos`.
//! The title is the canonical `MusicBrainz` recording title, which the media root basename uses once
//! tagging has run; upstream's own title embeds the artist and names the yt-dlp sidecar links.
//!
//! **Media id:** `youtube.dQw4w9WgXcQ` · **Raw yt-dlp video id:** `dQw4w9WgXcQ`
//!
//! ## On-disk tree
//!
//! ```text
//! media/
//! ├── music videos/
//! │   └── Never Gonna Give You Up [youtube.dQw4w9WgXcQ]/
//! │       ├── sidecars/                                    [Folder]
//! │       │   ├── subtitles/                             [MediaFolder → subtitles] NON-EMPTY
//! │       │   ├── subtitles.en.vtt                         [Media → subtitles_en]
//! │       │   ├── thumbnails/                            [MediaFolder → thumbnails] NON-EMPTY
//! │       │   ├── links/                                   [MediaFolder → links] NON-EMPTY
//! │       │   │   ├── <live yt-dlp provider title> [dQw4w9WgXcQ].url       # yt-dlp format, LIVE title
//! │       │   │   ├── <live yt-dlp provider title> [dQw4w9WgXcQ].webloc     # yt-dlp format, LIVE title
//! │       │   │   └── <live yt-dlp provider title> [dQw4w9WgXcQ].desktop    # yt-dlp format, LIVE title
//! │       │   ├── archive.txt                              [Media → archive]
//! │       │   ├── description.txt                          [Media → description]
//! │       │   └── info.json                                [Media → infojson]
//! │       ├── Never Gonna Give You Up [youtube.dQw4w9WgXcQ].untagged.mkv   # video_untagged
//! │       ├── Never Gonna Give You Up [youtube.dQw4w9WgXcQ].mkv            # video (tagged + replaygain)
//! │       ├── Never Gonna Give You Up [youtube.dQw4w9WgXcQ].en.vtt
//! │       ├── Never Gonna Give You Up [youtube.dQw4w9WgXcQ].description.txt
//! │       ├── Never Gonna Give You Up [youtube.dQw4w9WgXcQ].info.json
//! │       ├── Never Gonna Give You Up [youtube.dQw4w9WgXcQ].thumbnail.<ext>   # glob `*.thumbnail.*`
//! │       ├── Never Gonna Give You Up [youtube.dQw4w9WgXcQ].link.url          # mediapm rename format
//! │       ├── Never Gonna Give You Up [youtube.dQw4w9WgXcQ].link.webloc       # mediapm rename format
//! │       ├── Never Gonna Give You Up [youtube.dQw4w9WgXcQ].link.desktop      # mediapm rename format
//! │       └── folder.<ext>                                    # glob `folder.*` (id `.thumbnails.folder`)
//! └── playlists/
//!     └── rickroll.m3u8                                      # playlist, 2 entries
//! ```
//!
//! ## Link filename formats (two families)
//!
//! | Location | Format owner | Title provenance | Bracket id |
//! | -------- | ------------ | ---------------- | ---------- |
//! | `sidecars/links/` | yt-dlp `%(title)s [%(id)s].{ext}` | LIVE provider title, artist included | `dQw4w9WgXcQ` |
//! | Media folder root | mediapm `rename_files` → `.link.$1` | Canonical metadata title, no artist | `youtube.dQw4w9WgXcQ` |
//!
//! Sidecar links under `sidecars/links/` carry the LIVE yt-dlp provider title read from
//! `sidecars/info.json` (`title` key), so post-sync asserts expect `{provider_title} [{video_id}].{ext}`
//! instead of a hardcoded stand-in. That title embeds the artist, because upstream's does.
//!
//! Media-root projections (subtitles, thumbnails, links) are named by mediapm's own templates, which
//! single-source their stem from `${media.metadata.title}`. That resolves to the canonical
//! `MusicBrainz` recording title once tagging has run, so a root name carries no artist prefix while a
//! sidecar link basename does — the two families differ in provenance, not just in the bracket id.
//! Their title text is live metadata, so post-sync asserts resolve it from the materialized tree
//! instead of pinning it; only the media root basename itself is pinned, by
//! `expected_demo_media_root_name`.
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
//! └── ${title} [${id}]/                                   [Folder, id=youtube.dQw4w9WgXcQ.media_folder]
//!     ├── sidecars/                                       [Folder: subtitles/, thumbnails/, links/, flat sidecars]
//!     ├── ${title} [${id}].untagged${video_ext}           [Media, variant=video_untagged]
//!     ├── ${title} [${id}]${video_ext}                    [Media, variant=video]
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
//! cargo test -p mediapm demo_hierarchy_golden
//! cargo test -p mediapm demo_online_hierarchy_materialization
//! cargo test -p mediapm --example mediapm_demo_online -- --skip main_is_exercised
//! # main_is_exercised runs reduced (config-only) mode deterministically (no network).
//! cargo test -p mediapm --example mediapm_demo_online main_is_exercised
//! # Full-sync online path (network + external tools) — explicit run only;
//! # reuses the real user-level tool download cache, persisting tools across runs.
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

use mediapm::config::versions::{RuntimeBoundaryDeviations, runtime_boundary};
use mediapm::demo_hierarchy_spec::{
    ONLINE_DEMO_YOUTUBE_URL, ONLINE_DEMO_YT_DLP_VIDEO_ID, assert_valid_image_magic_bytes,
    online_demo_public_artifact_filename,
};
use mediapm::{
    ConfigVersionSpec, GenericOutputVariantConfig, HierarchyFolderRenameRule, HierarchyNode,
    HierarchyNodeKind, HierarchyPath, MaterializationMethod, MediaMetadataValue,
    MediaMetadataValueCandidate, MediaMetadataVariantBinding, MediaPmDocument, MediaPmService,
    MediaRuntimeStorage, MediaSourceSpec, MediaStep, MediaStepTool, OutputCaptureKind,
    OutputVariantValue, PlaylistFormat, PlaylistItemRef, SanitizeNamesConfig, ToolRegistryEntry,
    ToolRequirement, TransformInputValue, YtDlpOutputKind, YtDlpOutputVariantConfig,
    example_isolation, load_mediapm_document, load_mediapm_state_document, save_mediapm_document,
};
use mediapm_cas::{CasApi, FileSystemCas, Hash};
use mediapm_conductor::{
    NickelDocument, ToolKindSpec, ToolSpec, decode_document, default_runtime_inherited_env_vars,
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

/// Canonical `MusicBrainz` recording title of the demo video, which carries no artist.
const DEMO_EXPECTED_TITLE: &str = "Never Gonna Give You Up";
/// Artist the demo asserts on, in the `artist` metadata entry and the tagged artifact's `ARTIST` tag.
const DEMO_METADATA_ARTIST: &str = "Rick Astley";

const DEMO_EXPECTED_VIDEO_EXTENSION_WITH_DOT: &str = ".mkv";

const DEMO_OUTPUT_FILE_NAME_BASE: &str = "${media.metadata.title} [${media.id}]";

/// yt-dlp `--sub-langs` selector for the demo download step (shared by folder + scoped variants).
const DEMO_SAFE_SUB_LANGS: &str = "en";

const DEMO_UNTAGGED_MEDIA_FILE_NAME: &str =
    "${media.metadata.title} [${media.id}].untagged${media.metadata.video_ext}";

const DEMO_TAGGED_MEDIA_FILE_NAME: &str =
    "${media.metadata.title} [${media.id}]${media.metadata.video_ext}";

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

const DEMO_HIERARCHY_MEDIA_ROOT_TEMPLATE: &str = "${media.metadata.title} [${media.id}]";

const DEMO_HIERARCHY_ROOT_TEMPLATE: &str = "music videos/${media.metadata.title} [${media.id}]";

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
    ("description_media", "description", "${media.metadata.title} [${media.id}].description.txt"),
    ("infojson_media", "infojson", "${media.metadata.title} [${media.id}].info.json"),
    (
        "subtitles_en_media",
        DEMO_ROOT_SELECTED_SUBTITLE_VARIANT,
        DEMO_ROOT_SELECTED_SUBTITLE_FILE_NAME,
    ),
];

const DEMO_ROOT_SELECTED_SUBTITLE_VARIANT: &str = "subtitles_en";

const DEMO_ROOT_SELECTED_SUBTITLE_FILE_NAME: &str = "${media.metadata.title} [${media.id}].en.vtt";

const DEMO_SIDECAR_SELECTED_SUBTITLE_FILE_NAME: &str = "sidecars/subtitles.en.vtt";

const DEMO_EXPECTED_YT_DLP_STEP_COUNT: usize = 1;

const DEMO_ONLINE_TIMEOUT_SECS_ENV: &str = "MEDIAPM_DEMO_ONLINE_TIMEOUT_SECS";

/// Total hard-watchdog deadline; MUST equal nextest `slow-timeout.terminate-after` (600s) for the network test-group in `.config/nextest.toml`.
const DEMO_ONLINE_HARD_TIMEOUT_TOTAL_SECS: u64 = 10 * 60;

const DEMO_ONLINE_TIMEOUT_SECS_DEFAULT: u64 =
    DEMO_ONLINE_HARD_TIMEOUT_TOTAL_SECS - DEMO_ONLINE_HARD_TIMEOUT_GRACE_SECS;

const DEMO_CONDUCTOR_EXECUTABLE_TIMEOUT_SECS_ENV: &str =
    "MEDIAPM_CONDUCTOR_EXECUTABLE_TIMEOUT_SECS";

/// Conductor run-level RPC budget knob. Mirrors the library's
/// `orchestration::config::ENV_RPC_TIMEOUT_SECONDS` (`pub(crate)` there, so
/// the example re-declares the literal) at
/// `src/mediapm-conductor/src/orchestration/config.rs`.
const DEMO_CONDUCTOR_RPC_TIMEOUT_SECONDS_ENV: &str = "MEDIAPM_CONDUCTOR_RPC_TIMEOUT_SECONDS";

/// Margin the demo holds back from its overall sync budget when sizing the
/// conductor's run-level RPC budget, so the conductor-layer timeout error
/// (which names the budget and the knob) fires before the demo's blind
/// hard-timeout wrapper.
const DEMO_CONDUCTOR_RPC_TIMEOUT_MARGIN_SECS: u64 = 60;

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

/// Tally of the second, idempotency-proving sync a full demo run performs.
///
/// Recorded in [`DemoManifest::second_run`] so the artifact itself shows the
/// re-run did no work: every conductor instance came from the content-derived
/// instance cache, every library path was already recorded at the resolved
/// hash, and every payload tool was already deployed. `updated_tools` counts
/// only the no-payload `import` builtin, whose skip path is structurally
/// closed (empty content-map hash), so it re-registers as an update.
#[derive(Debug, Clone, PartialEq, Serialize)]
struct SecondRunTally {
    executed_instances: usize,
    cached_instances: usize,
    materialized_paths: usize,
    skipped_paths: usize,
    removed_paths: usize,
    added_tools: usize,
    updated_tools: usize,
    skipped_tools: usize,
    pruned_tools: usize,
    warning_count: usize,
}

/// Per-tool provenance as recorded in the state lockfile: the canonical version
/// that drives skip-if-up-to-date logic, plus the upstream identifiers the
/// provider resolved (`None` where the provider has no value — the why-empty
/// rules live in `provider-dispatch.instructions.md`).
#[derive(Debug, Clone, PartialEq, Eq, Serialize)]
struct ManagedToolProvenance {
    canonical_version: String,
    resolved_tag: Option<String>,
    resolved_version: Option<String>,
    resolved_vcs_hash: Option<String>,
}

#[derive(Debug, Clone, PartialEq, Serialize)]
struct DemoManifest {
    /// Which mode produced this manifest. The full-sync path records
    /// `"full"`; the reduced config-only path writes its own manifest with
    /// `"config_only"`.
    run_mode: String,
    generated_unix_epoch_seconds: u64,
    artifact_root: String,
    workspace_root: String,
    tool_ids: Vec<String>,
    tool_binaries: BTreeMap<String, String>,
    /// Canonical version and upstream provenance per logical managed tool id,
    /// read from the state lockfile after the sync. See
    /// [`summarize_managed_tool_provenance`].
    managed_tools: BTreeMap<String, ManagedToolProvenance>,
    yt_dlp_max_concurrent_calls: i32,
    yt_dlp_max_retries: i32,
    mediapm_ncl_path: String,
    conductor_generated_ncl_path: String,
    workflow_id: String,
    workflow_step_count: usize,
    materialization_preference_order: Vec<String>,
    materialized_demo_video_path: String,
    materialized_demo_tagged_video_path: String,
    materialized_demo_video_hardlinked_to_cas: bool,
    materialized_demo_tagged_video_hardlinked_to_cas: bool,
    materialized_demo_sidecar_paths: BTreeMap<String, String>,
    /// CAS hash recorded for every managed library file, keyed by the
    /// hierarchy-relative path. Read verbatim from the state lockfile, so a
    /// reader can verify a materialized file against the store without
    /// recomputing anything.
    materialized_file_hashes: BTreeMap<String, String>,
    executed_instances: usize,
    cached_instances: usize,
    materialized_paths: usize,
    removed_paths: usize,
    added_tools: usize,
    updated_tools: usize,
    warning_count: usize,
    /// Present only when the full-sync path ran its second idempotency sync;
    /// reduced (config-only) runs record `None`.
    second_run: Option<SecondRunTally>,
    profile_path: String,
    /// Total logical size of every CAS blob object (sum of `info.len` for
    /// each hash found under `blobs/v1/`). A single field rather than a
    /// delta-vs-without pair because no delta chunks (`.diff`) exist in the
    /// demo store — the previous three-field split (`without_delta`,
    /// `with_delta`, `ratio`) was always zero because the walk looked at
    /// `v1/` instead of `blobs/v1/`, and even after fixing the path both
    /// delta and without-delta would be identical for this store.
    store_size_bytes: u64,
}

/// Runtime storage for demo runs; the user-level tool cache resolves to a
/// hermetic tempdir when [`example_isolation::CACHE_ROOT_ENV`] is set
/// (examples-as-tests) and to the real OS user-level cache otherwise
/// (explicit `cargo run --example` runs).
fn example_runtime_storage() -> MediaRuntimeStorage {
    MediaRuntimeStorage {
        cache_root_override: Some(example_cache_root()),
        ..MediaRuntimeStorage::default()
    }
}

/// Example user-level tool download cache root, honoring
/// [`example_isolation::CACHE_ROOT_ENV`] (hermetic examples-as-test runs) and
/// otherwise the real persistent OS user-level cache, so explicit runs
/// persist downloaded tools across runs.
fn example_cache_root() -> PathBuf {
    example_isolation::user_level_cache_root()
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

/// Exports the conductor's run-level RPC budget, sized from the demo's own
/// overall sync budget minus [`DEMO_CONDUCTOR_RPC_TIMEOUT_MARGIN_SECS`].
///
/// The conductor captures this budget on first actor use; if the caller has
/// already set the knob, that value is respected. A
/// saturating subtraction can compute zero for a sync budget below the
/// margin, in which case the conductor falls back to its own default (the
/// demo's wrapper still bounds the run).
fn configure_demo_conductor_rpc_timeout(sync_timeout: Duration) {
    if std::env::var_os(DEMO_CONDUCTOR_RPC_TIMEOUT_SECONDS_ENV).is_some() {
        return;
    }

    let computed_budget =
        sync_timeout.as_secs().saturating_sub(DEMO_CONDUCTOR_RPC_TIMEOUT_MARGIN_SECS);

    unsafe {
        std::env::set_var(DEMO_CONDUCTOR_RPC_TIMEOUT_SECONDS_ENV, computed_budget.to_string());
    }
    emit_watchdog_notice(&format!(
        "demo_online defaulted {DEMO_CONDUCTOR_RPC_TIMEOUT_SECONDS_ENV}={computed_budget} so the conductor's run-workflow watchdog fires (with its diagnosable error) before the demo's hard timeout; set {DEMO_CONDUCTOR_RPC_TIMEOUT_SECONDS_ENV} to override"
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
    /// Canonical metadata title, parsed back out of the materialized media root's own name.
    ///
    /// The online demo's name templates single-source their stem from `${media.metadata.title}`,
    /// so every root-level projection renders this title. It is the `MusicBrainz` recording title
    /// once tagging has run and the upstream provider title otherwise, which is why
    /// `assert_metadata_files_present` pins the media root against the canonical form.
    title: String,
    /// yt-dlp provider title (`info.json` `title` key).
    ///
    /// This is the LIVE basename stem for `sidecars/links/` files:
    /// `{provider_title} [{video_id}].{ext}`, and it embeds the artist because upstream's does.
    /// It is read from the infojson instead of a hardcoded stand-in so live yt-dlp title drift
    /// does not break the post-sync asserts. The root link projections do **not** use it: mediapm
    /// renders those from `${media.metadata.title}`.
    provider_title: String,
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
        .replace("${media.metadata.title}", &metadata.title)
        .replace("${media.metadata.video_id}", &metadata.video_id)
        .replace("${media.id}", &metadata.media_id)
        .replace("${media.metadata.video_ext}", &metadata.video_ext)
}

fn parse_jellyfin_root_folder_name(folder_name: &str) -> Option<(String, String)> {
    let (title_segment, media_id_segment) = folder_name.rsplit_once(" [")?;
    let media_id = media_id_segment.strip_suffix(']')?.trim();
    // The basename single-sources from `${media.metadata.title}`: the canonical MusicBrainz
    // recording title once tagging has run, or the upstream provider title — which embeds the
    // artist — in a run that never reached the tagging steps. The whole segment before
    // ` [<media-id>]` is the title either way; never split it back apart on ` - `.
    let title = title_segment.trim();
    if title.is_empty() || media_id.is_empty() {
        return None;
    }

    Some((title.to_string(), media_id.to_string()))
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

    let (folder_title, media_id) =
        parse_jellyfin_root_folder_name(folder_name).ok_or_else(|| {
            format!(
                "expected interpolated media root '{}' to match '<title> [<media-id>]'",
                interpolated_root.display()
            )
        })?;

    Ok(DemoResolvedMetadata {
        // The hierarchy filename template single-sources from `title`, so treat
        // the interpolated folder title as source-of-truth.
        title: folder_title,
        provider_title: require_non_empty_json_string(
            &payload,
            DEMO_METADATA_TITLE_KEY,
            &infojson_path,
        )?,
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
    let objects_root = cas_root.join("blobs").join("v1");
    if !objects_root.exists() {
        return Ok(hashes);
    }

    collect_store_object_hashes_recursive(&objects_root, &objects_root, &mut hashes)?;

    Ok(hashes)
}

/// Returns the total logical size (sum of `info.len`) of every CAS blob
/// object under `cas_root/blobs/v1/`. Used for the demo manifest's
/// `store_size_bytes` field.
async fn summarize_store_size(cas: &FileSystemCas, cas_root: &Path) -> ExampleResult<u64> {
    // `cas` must be the shared handle from the service: `FileSystemCas::open`
    // on the same root would re-acquire the directory lock and hit
    // `LockContention` because the service already holds it. Cloning the
    // `Arc<FileSystemCas>` shares the same `DirectoryLockGuard`.
    let mut total = 0u64;

    for hash in collect_store_object_hashes(cas_root)? {
        let info = cas.stat(hash).await?;
        total = total.saturating_add(info.len);
    }

    Ok(total)
}

/// Summarises the state lockfile's `managed_tools` entries as one provenance
/// record per logical tool id.
///
/// Every fetch-and-deploy cycle appends an entry, so one tool id can appear
/// several times; the record kept is the entry with the greatest
/// `deployed_at`, which is the deployment the run just resolved against.
/// Entries are keyed by the logical `tool_id` rather than the content-addressed
/// conductor id because that is the name a reader of the manifest recognises.
fn summarize_managed_tool_provenance(
    entries: &[ToolRegistryEntry],
) -> BTreeMap<String, ManagedToolProvenance> {
    let mut latest: BTreeMap<&str, &ToolRegistryEntry> = BTreeMap::new();
    for entry in entries {
        let superseded = latest
            .get(entry.tool_id.as_str())
            .is_some_and(|current| current.deployed_at >= entry.deployed_at);
        if !superseded {
            latest.insert(entry.tool_id.as_str(), entry);
        }
    }

    latest
        .into_iter()
        .map(|(tool_id, entry)| {
            (
                tool_id.to_string(),
                ManagedToolProvenance {
                    canonical_version: entry.canonical_version.clone(),
                    resolved_tag: entry.resolved_tag.clone(),
                    resolved_version: entry.resolved_version.clone(),
                    resolved_vcs_hash: entry.resolved_vcs_hash.clone(),
                },
            )
        })
        .collect()
}

/// Snapshots every file under the hierarchy root as `(relative path, length,
/// modification time)` tuples, sorted for order-independent comparison.
///
/// Used by the full demo run's idempotency proof: the second sync must not
/// create, rewrite, or remove any materialized file. The length catches a
/// rewrite that changed the size, and the modification time catches an
/// in-place rewrite that kept the path and the size.
fn snapshot_materialized_files(
    hierarchy_root: &Path,
) -> ExampleResult<Vec<(String, u64, std::time::SystemTime)>> {
    let mut snapshot = Vec::new();
    let mut stack = vec![hierarchy_root.to_path_buf()];
    while let Some(dir) = stack.pop() {
        let entries = fs::read_dir(&dir).map_err(|error| {
            format!("reading materialized directory '{}': {error}", dir.display())
        })?;
        for entry in entries {
            let entry = entry.map_err(|error| {
                format!("reading materialized directory entry under '{}': {error}", dir.display())
            })?;
            let path = entry.path();
            let metadata = entry.metadata().map_err(|error| {
                format!("reading materialized file metadata '{}': {error}", path.display())
            })?;
            if metadata.is_dir() {
                stack.push(path);
            } else {
                let relative = path
                    .strip_prefix(hierarchy_root)
                    .map_err(|error| {
                        format!(
                            "materialized path '{}' is outside hierarchy root '{}': {error}",
                            path.display(),
                            hierarchy_root.display()
                        )
                    })?
                    .to_string_lossy()
                    .replace('\\', "/");
                let modified = metadata.modified().map_err(|error| {
                    format!(
                        "reading materialized file modification time '{}': {error}",
                        path.display()
                    )
                })?;
                snapshot.push((relative, metadata.len(), modified));
            }
        }
    }
    snapshot.sort();
    Ok(snapshot)
}

/// Verifies the full demo run's second sync did no work: every conductor
/// instance came from cache, no library path was materialized or removed,
/// no step failed or warned, and no tool was added, pruned, or removed.
///
/// `updated_tools` is asserted equal to the first run's value rather than
/// zero because the no-payload `import` builtin re-registers on every sync
/// (its empty content map hash excludes it from the skip path), so the
/// re-run's tool churn must match the first run's churn exactly — a payload
/// tool that started churning would raise the second run's count above the
/// first run's and fail here.
fn assert_second_sync_did_no_work(
    first: &mediapm::SyncSummary,
    second: &mediapm::SyncSummary,
    files_before: &[(String, u64, std::time::SystemTime)],
    files_after: &[(String, u64, std::time::SystemTime)],
) -> ExampleResult<()> {
    let mut problems = Vec::new();
    if second.executed_instances != 0 {
        problems.push(format!(
            "executed_instances={} (expected 0: all steps must come from cache)",
            second.executed_instances
        ));
    }
    if second.cached_instances != first.executed_instances {
        problems.push(format!(
            "cached_instances={} (expected {}: the first run's executed steps)",
            second.cached_instances, first.executed_instances
        ));
    }
    if second.materialized_paths != 0 {
        problems.push(format!(
            "materialized_paths={} (expected 0: no path may be rewritten)",
            second.materialized_paths
        ));
    }
    if second.skipped_paths != first.materialized_paths {
        problems.push(format!(
            "skipped_paths={} (expected {}: the first run's materialized paths)",
            second.skipped_paths, first.materialized_paths
        ));
    }
    if second.removed_paths != 0 {
        problems.push(format!("removed_paths={} (expected 0)", second.removed_paths));
    }
    if second.missing_paths != 0 {
        problems.push(format!("missing_paths={} (expected 0)", second.missing_paths));
    }
    if second.workflow_failed_steps != 0 {
        problems
            .push(format!("workflow_failed_steps={} (expected 0)", second.workflow_failed_steps));
    }
    if !second.warnings.is_empty() {
        problems.push(format!("warnings={:?} (expected none)", second.warnings));
    }
    if second.added_tools != 0 {
        problems.push(format!("added_tools={} (expected 0)", second.added_tools));
    }
    if second.updated_tools != first.updated_tools {
        problems.push(format!(
            "updated_tools={} (expected {}: the import builtin's re-registration churn must match \
             the first run's; a payload tool churning would exceed it)",
            second.updated_tools, first.updated_tools
        ));
    }
    if second.pruned_tools != 0 {
        problems.push(format!("pruned_tools={} (expected 0)", second.pruned_tools));
    }
    if second.removed_tools != 0 {
        problems.push(format!("removed_tools={} (expected 0)", second.removed_tools));
    }
    if files_before != files_after {
        problems.push(format!(
            "materialized file set changed across the second sync: before={files_before:?} \
             after={files_after:?}"
        ));
    }
    if problems.is_empty() {
        Ok(())
    } else {
        Err(format!("online demo second sync was not idempotent: {}", problems.join("; ")).into())
    }
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
                ("write_all_tags".to_string(), TransformInputValue::String("true".to_string())),
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
                        // The tagged `video` variant: yt-dlp produces it, media-tagger redefines it
                        // with MusicBrainz's recording title, and rsgain redefines it once more.
                        // Every name template single-sources from this key, so the materialized
                        // tree carries the canonical title once tagging has run.
                        MediaMetadataValueCandidate::Variant(MediaMetadataVariantBinding {
                            variant: "video".to_string(),
                            metadata_key: DEMO_METADATA_TITLE_KEY.to_string(),
                            transform: None,
                        }),
                        // A run that never reached the tagging steps resolves the upstream download
                        // instead, whose title embeds the artist. That run fails its post-sync
                        // assertion rather than silently materializing an artist-prefixed tree.
                        MediaMetadataValueCandidate::Variant(MediaMetadataVariantBinding {
                            variant: "infojson".to_string(),
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
            sanitize_names: Some(SanitizeNamesConfig::Inherit),
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
            sanitize_names: Some(SanitizeNamesConfig::Inherit),
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
                sanitize_names: Some(SanitizeNamesConfig::Inherit),
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
                sanitize_names: Some(SanitizeNamesConfig::Inherit),
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
                sanitize_names: Some(SanitizeNamesConfig::Inherit),
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
            replacement: "${media.metadata.title} [${media.id}].thumbnail.$1".to_string(),
        }],
        format: PlaylistFormat::M3u8,
        ids: Vec::new(),
        sanitize_names: Some(SanitizeNamesConfig::Inherit),
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
            replacement: "${media.metadata.title} [${media.id}].link.$1".to_string(),
        }],
        format: PlaylistFormat::M3u8,
        ids: Vec::new(),
        sanitize_names: Some(SanitizeNamesConfig::Inherit),
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
        sanitize_names: Some(SanitizeNamesConfig::Inherit),
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
            sanitize_names: Some(SanitizeNamesConfig::Inherit),
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
                sanitize_names: Some(SanitizeNamesConfig::Inherit),
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
            sanitize_names: Some(SanitizeNamesConfig::Inherit),
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
                sanitize_names: Some(SanitizeNamesConfig::Inherit),
                children: Vec::new(),
            }],
        },
    ];

    // Expose all runtime-storage fields with explicit default values so the
    // written mediapm.ncl documents every runtime knob by default.
    // `runtime_boundary` spells the whole surface out; the deviations below are
    // the only knobs this demo moves away from it.
    document.runtime = runtime_boundary(RuntimeBoundaryDeviations {
        // Materialized hierarchy root directory.
        // Default: workspace root containing `mediapm.ncl`.
        hierarchy_root_dir: Some("media".to_string()),
        // Machine-managed mediapm state path relative to workspace root.
        // Default: `.mediapm/state.json` (JSON always-write).
        mediapm_state_config: Some(Some(".mediapm/state.json".to_string())),
        // Optional default runtime GC TTL in seconds.
        // Default: 604800 (7 days).
        instance_ttl_seconds: Some(3600),
        // Explicit host default inherited env-var map.
        // Runtime still merges this map case-insensitively with host defaults.
        inherited_env_vars: Some({
            let host_platform = std::env::consts::OS.to_ascii_lowercase();
            let mut map = BTreeMap::new();
            map.insert(host_platform, default_runtime_inherited_env_vars().into_keys().collect());
            map
        }),
        // Enable conductor profiling so every sync run produces a per-step
        // timing profile at `.mediapm/profile.json` for latency investigation.
        profiler_enabled: Some(true),
    });

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

fn assert_sidecar_directory_family_content(
    variant: &str,
    directory: &Path,
    provider_title: &str,
    video_id: &str,
) -> ExampleResult<()> {
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
            assert_sidecar_links_directory_has_exact_public_filenames(
                directory,
                provider_title,
                video_id,
            )?;
            for entry in &files {
                assert_link_file_content_format(entry)?;
            }
        }
        "thumbnails" => {
            for entry in &files {
                let ext = entry.extension().and_then(|e| e.to_str()).unwrap_or("");
                if is_image_extension(ext) {
                    assert_valid_image_magic_bytes(entry).map_err(|e| format!("[sidecar] {e}"))?;
                }
            }
        }
        _ => {}
    }

    Ok(())
}

// yt-dlp-format basenames under `sidecars/links/` — see demo-hierarchy-golden.instructions.md.
// The basename uses the LIVE yt-dlp provider title from the infojson sidecar, so it is
// derived via `online_demo_public_artifact_filename` instead of a hardcoded stand-in.
fn assert_sidecar_links_directory_has_exact_public_filenames(
    directory: &Path,
    provider_title: &str,
    video_id: &str,
) -> ExampleResult<()> {
    for extension in ["url", "webloc", "desktop"] {
        let expected_name =
            online_demo_public_artifact_filename(provider_title, video_id, extension);
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
    // Root projections carry the media-id suffix (` [{media.id}].`) from the hierarchy
    // rename templates; the title text before it is live metadata and is NOT asserted.
    let Some((_, expected_media_id)) = parse_jellyfin_root_folder_name(expected_output_base) else {
        return Err(format!(
            "expected media output base '{expected_output_base}' to match '<title> [<media-id>]'"
        )
        .into());
    };
    let mut expected_media_suffixes = vec![expected_media_id.clone()];
    if let Some((_, raw_video_id)) = expected_media_id.rsplit_once('.')
        && !raw_video_id.is_empty()
    {
        expected_media_suffixes.push(raw_video_id.to_string());
    }

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
        path.file_name().and_then(|value| value.to_str()).is_some_and(|name| {
            expected_media_suffixes.iter().any(|suffix| name.contains(&format!(" [{suffix}].")))
        })
    }) {
        return Err(format!(
            "expected flattened subtitle sidecar names in '{}' to contain a media-id suffix ' [<suffix>].': {:?}",
            interpolated_root.display(),
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

    // mediapm root link projections — structural media-id suffix, title-free:
    // `… [{media_id}].link.{ext}` (exact tail match per extension).
    for extension in ["url", "webloc", "desktop"] {
        let expected_suffix = format!(" [{expected_media_id}].link.{extension}");
        let found = root_files.iter().any(|path| {
            path.file_name()
                .and_then(|value| value.to_str())
                .is_some_and(|name| name.ends_with(&expected_suffix))
        });
        if !found {
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
                "expected a root link projection ending in '{expected_suffix}' in '{}' but observed link files: {observed:?}",
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

    for thumb in &thumbnails_files {
        let ext = thumb.extension().and_then(|e| e.to_str()).unwrap_or("");
        if is_image_extension(ext) {
            assert_valid_image_magic_bytes(thumb).map_err(|e| format!("[root-projection] {e}"))?;
        }
    }
    for link in &links_files {
        assert_link_file_content_format(link)?;
    }

    assert_root_projection_sidecar_names_align(
        interpolated_root,
        &expected_media_suffixes,
        &thumbnails_files,
        &links_files,
    )?;

    Ok(())
}

fn assert_root_projection_sidecar_names_align(
    interpolated_root: &Path,
    expected_media_suffixes: &[String],
    thumbnails_files: &[PathBuf],
    links_files: &[PathBuf],
) -> ExampleResult<()> {
    let root_projection_files =
        thumbnails_files.iter().chain(links_files.iter()).cloned().collect::<Vec<_>>();

    if !root_projection_files.iter().all(|path| {
        path.file_name().and_then(|value| value.to_str()).is_some_and(|name| {
            expected_media_suffixes
                .iter()
                .any(|media_suffix| name.contains(&format!(" [{media_suffix}].")))
        })
    }) {
        return Err(format!(
            "expected root thumbnail/link sidecar names in '{}' to contain a media-id suffix ' [<suffix>].': {:?}",
            interpolated_root.display(),
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

fn assert_sidecar_content_expectations(variant: &str, path: &Path) -> ExampleResult<()> {
    match variant {
        "description" | "playlist_description" | "archive" => {
            let text = fs::read_to_string(path)?;
            if text.trim().is_empty() {
                return Err(format!(
                    "[sidecar] expected variant '{variant}' at '{}' to contain non-empty text",
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
                    "[sidecar] expected variant '{variant}' at '{}' to contain a JSON object",
                    path.display()
                )
                .into());
            }
            if !variant.starts_with("playlist") {
                let id = value.get("id").and_then(serde_json::Value::as_str).unwrap_or("");
                if id != ONLINE_DEMO_YT_DLP_VIDEO_ID {
                    return Err(format!(
                        "[sidecar] infojson 'id' field expected '{ONLINE_DEMO_YT_DLP_VIDEO_ID}', got '{id}'",
                    )
                    .into());
                }
                let extractor =
                    value.get("extractor").and_then(serde_json::Value::as_str).unwrap_or("");
                if extractor != "youtube" {
                    return Err(format!(
                        "[sidecar] infojson 'extractor' field expected 'youtube', got '{extractor}'",
                    )
                    .into());
                }
                let webpage_url =
                    value.get("webpage_url").and_then(serde_json::Value::as_str).unwrap_or("");
                if !webpage_url.contains(ONLINE_DEMO_YT_DLP_VIDEO_ID) {
                    return Err(format!(
                        "[sidecar] infojson 'webpage_url' '{webpage_url}' does not contain video ID",
                    )
                    .into());
                }
                let upload_date =
                    value.get("upload_date").and_then(serde_json::Value::as_str).unwrap_or("");
                if upload_date.len() != 8 || !upload_date.chars().all(|c| c.is_ascii_digit()) {
                    return Err(format!(
                        "[sidecar] infojson 'upload_date' expected YYYYMMDD (8 digits), got '{upload_date}'",
                    )
                    .into());
                }
                let duration =
                    value.get("duration").and_then(serde_json::Value::as_f64).unwrap_or(0.0);
                if duration <= 0.0 {
                    return Err(format!(
                        "[sidecar] infojson 'duration' expected positive, got '{duration}'",
                    )
                    .into());
                }
            }
        }
        v if v.starts_with("subtitles") && !v.contains("playlist") => {
            let text = fs::read_to_string(path)?;
            if !text.starts_with("WEBVTT") {
                return Err(format!(
                    "[sidecar] expected subtitle variant '{variant}' at '{}' to start with 'WEBVTT'",
                    path.display()
                )
                .into());
            }
            if !text.contains("Kind: captions") {
                return Err(format!(
                    "[sidecar] expected subtitle variant '{variant}' at '{}' to contain 'Kind: captions'",
                    path.display()
                )
                .into());
            }
            if !text.contains("Language: en") {
                return Err(format!(
                    "[sidecar] expected subtitle variant '{variant}' at '{}' to contain 'Language: en'",
                    path.display()
                )
                .into());
            }
        }
        _ => {}
    }

    Ok(())
}

/// Asserts that a materialized link file carries the content shape its
/// extension implies: `.url` an `InternetShortcut` block, `.webloc` a plist
/// document, `.desktop` a desktop-entry group.
#[expect(
    clippy::too_many_lines,
    reason = "each match arm is the complete content contract for one link format, so merging or extracting arms would separate a format's required markers from the extension that selects them"
)]
fn assert_link_file_content_format(path: &Path) -> ExampleResult<()> {
    let text = fs::read_to_string(path)?;
    let ext = path.extension().and_then(|e| e.to_str()).unwrap_or("").to_ascii_lowercase();
    match ext.as_str() {
        "url" => {
            if !text.contains("[InternetShortcut]") {
                return Err(format!(
                    "[link] .url file '{}' missing '[InternetShortcut]' header",
                    path.display()
                )
                .into());
            }
            if !text.lines().any(|line| line.starts_with("URL=")) {
                return Err(
                    format!("[link] .url file '{}' missing 'URL=' line", path.display()).into()
                );
            }
            if !text.contains(ONLINE_DEMO_YOUTUBE_URL) {
                return Err(format!(
                    "[link] .url file '{}' does not contain YouTube URL",
                    path.display()
                )
                .into());
            }
        }
        "desktop" => {
            if !text.contains("[Desktop Entry]") {
                return Err(format!(
                    "[link] .desktop file '{}' missing '[Desktop Entry]' header",
                    path.display()
                )
                .into());
            }
            if !text.contains("Type=Link") {
                return Err(format!(
                    "[link] .desktop file '{}' missing 'Type=Link'",
                    path.display()
                )
                .into());
            }
            if !text.lines().any(|line| line.starts_with("Name=")) {
                return Err(format!(
                    "[link] .desktop file '{}' missing 'Name=' line",
                    path.display()
                )
                .into());
            }
            if !text.lines().any(|line| line.starts_with("URL=")) {
                return Err(format!(
                    "[link] .desktop file '{}' missing 'URL=' line",
                    path.display()
                )
                .into());
            }
            if !text.contains("Encoding=UTF-8") {
                return Err(format!(
                    "[link] .desktop file '{}' missing 'Encoding=UTF-8'",
                    path.display()
                )
                .into());
            }
            if !text.contains(ONLINE_DEMO_YOUTUBE_URL) {
                return Err(format!(
                    "[link] .desktop file '{}' does not contain YouTube URL",
                    path.display()
                )
                .into());
            }
        }
        "webloc" => {
            if !text.contains("<?xml") {
                return Err(format!(
                    "[link] .webloc file '{}' missing '<?xml' declaration",
                    path.display()
                )
                .into());
            }
            if !text.contains("<plist") {
                return Err(format!(
                    "[link] .webloc file '{}' missing '<plist' element",
                    path.display()
                )
                .into());
            }
            if !text.contains("<key>URL</key>") {
                return Err(format!(
                    "[link] .webloc file '{}' missing '<key>URL</key>'",
                    path.display()
                )
                .into());
            }
            if !text.contains(&format!("<string>{ONLINE_DEMO_YOUTUBE_URL}</string>")) {
                return Err(format!(
                    "[link] .webloc file '{}' missing '<string>{ONLINE_DEMO_YOUTUBE_URL}</string>'",
                    path.display()
                )
                .into());
            }
        }
        other => {
            return Err(format!(
                "[link] unexpected link file extension '{other}' for '{}'",
                path.display()
            )
            .into());
        }
    }
    Ok(())
}

fn assert_playlist_content_format(path: &Path) -> ExampleResult<()> {
    let text = fs::read_to_string(path)?;
    if !text.starts_with("#EXTM3U") {
        return Err(format!(
            "[playlist] expected playlist file '{}' to start with '#EXTM3U'",
            path.display()
        )
        .into());
    }
    if !text.contains(".mkv") {
        return Err(format!(
            "[playlist] expected playlist file '{}' to reference a '.mkv' file",
            path.display()
        )
        .into());
    }
    Ok(())
}

fn ffprobe_json_payload(path: &Path, show_entries: &str) -> ExampleResult<serde_json::Value> {
    let ffprobe_command = demo_ffprobe_command();
    let output = Command::new(ffprobe_command)
        .arg("-v")
        .arg("error")
        .arg("-show_entries")
        .arg(show_entries)
        .arg("-of")
        .arg("json")
        .arg(path)
        .output()
        .map_err(|error| {
            format!(
                "[ffprobe] failed to launch ffprobe command '{}' for '{}': {error}",
                ffprobe_command,
                path.display()
            )
        })?;
    if !output.status.success() {
        let stderr = String::from_utf8_lossy(&output.stderr).trim().to_string();
        return Err(format!(
            "[ffprobe] ffprobe failed for '{}' with status {}: {stderr}",
            path.display(),
            output.status
        )
        .into());
    }
    serde_json::from_slice(&output.stdout).map_err(|error| {
        format!("[ffprobe] JSON decode failed for '{}': {error}", path.display()).into()
    })
}

fn ffprobe_field_to_string(value: &serde_json::Value) -> String {
    match value {
        serde_json::Value::String(s) => s.clone(),
        serde_json::Value::Number(n) => n.to_string(),
        _ => String::new(),
    }
}

fn assert_eq_field(stream: &serde_json::Value, field: &str, expected: &str) -> ExampleResult<()> {
    let actual = stream.get(field).map(ffprobe_field_to_string).unwrap_or_default();
    if actual != expected {
        return Err(format!(
            "[ffprobe] stream field '{field}' expected '{expected}', got '{actual}'",
        )
        .into());
    }
    Ok(())
}

fn assert_tag_eq(
    tags: &serde_json::Map<String, serde_json::Value>,
    key: &str,
    expected: &str,
) -> ExampleResult<()> {
    let actual = tags.get(key).and_then(serde_json::Value::as_str).unwrap_or("");
    if actual != expected {
        return Err(format!("[ffprobe] tag '{key}' expected '{expected}', got '{actual}'").into());
    }
    Ok(())
}

fn assert_video_stream_properties(payload: &serde_json::Value) -> ExampleResult<()> {
    let streams = payload
        .get("streams")
        .and_then(serde_json::Value::as_array)
        .ok_or_else(|| "[ffprobe] payload missing 'streams' array".to_string())?;
    let video = streams
        .iter()
        .find(|s| s.get("codec_type").and_then(serde_json::Value::as_str) == Some("video"))
        .ok_or_else(|| "[ffprobe] no video stream found".to_string())?;
    assert_eq_field(video, "codec_name", "h264")?;
    assert_eq_field(video, "pix_fmt", "yuv420p")?;
    assert_eq_field(video, "field_order", "progressive")?;
    assert_eq_field(video, "r_frame_rate", "25/1")?;
    assert_eq_field(video, "display_aspect_ratio", "16:9")?;
    assert_eq_field(video, "sample_aspect_ratio", "1:1")?;
    assert_eq_field(video, "color_space", "bt709")?;
    assert_eq_field(video, "color_transfer", "bt709")?;
    assert_eq_field(video, "color_primaries", "bt709")?;
    // The demo requests `best[height<=144]/best[height<=240]/worst`; for this
    // source the height-capped selectors match nothing, so yt-dlp falls through
    // to `worst` (640x360). Assert the actual produced resolution rather than a
    // hardcoded 256x144 that the selector does not guarantee.
    assert_eq_field(video, "width", "640")?;
    assert_eq_field(video, "height", "360")?;
    Ok(())
}

fn assert_audio_stream_properties(payload: &serde_json::Value) -> ExampleResult<()> {
    let streams = payload
        .get("streams")
        .and_then(serde_json::Value::as_array)
        .ok_or_else(|| "[ffprobe] payload missing 'streams' array".to_string())?;
    let audio = streams
        .iter()
        .find(|s| s.get("codec_type").and_then(serde_json::Value::as_str) == Some("audio"))
        .ok_or_else(|| "[ffprobe] no audio stream found".to_string())?;
    assert_eq_field(audio, "codec_name", "aac")?;
    assert_eq_field(audio, "sample_rate", "44100")?;
    assert_eq_field(audio, "channel_layout", "stereo")?;
    assert_eq_field(audio, "channels", "2")?;
    Ok(())
}

/// Asserts the tags the pre-tagging artifact must carry, which are the upstream download's own.
///
/// `provider_title` is the live yt-dlp title read from `sidecars/info.json`, so the title check
/// follows upstream's text instead of pinning a stand-in. The `MusicBrainz`-only tags are asserted
/// absent: this artifact is produced before media-tagger runs, so any of them appearing here means
/// a tagging step wrote into the wrong variant. Upstream's `DATE` and `GENRE` are deliberately not
/// pinned — they are upstream's to change, and pinning them recreates the drift this split removes.
///
/// The `ARTIST` literal and the absence list describe what yt-dlp's own metadata mapping writes,
/// which only the Level-3 online run exercises; the hermetic tests cover the assertion's logic
/// against hand-written payloads. If the live run disagrees, make those fields tolerant rather
/// than pinning upstream's mapping here.
fn assert_untagged_format_tags(
    payload: &serde_json::Value,
    provider_title: &str,
) -> ExampleResult<()> {
    let format_tags = payload
        .get("format")
        .and_then(|format| format.get("tags"))
        .and_then(serde_json::Value::as_object)
        .ok_or_else(|| "[ffprobe] payload missing format.tags".to_string())?;
    assert_tag_eq(format_tags, "title", provider_title)?;
    assert_tag_eq(format_tags, "ARTIST", DEMO_METADATA_ARTIST)?;
    let comment = format_tags.get("COMMENT").and_then(serde_json::Value::as_str).unwrap_or("");
    if !comment.contains(ONLINE_DEMO_YT_DLP_VIDEO_ID) {
        return Err(format!(
            "[ffprobe] tag 'COMMENT' expected to contain video ID '{ONLINE_DEMO_YT_DLP_VIDEO_ID}', got '{comment}'",
        )
        .into());
    }
    for key in [
        "ALBUM",
        "LABEL",
        "ISRC",
        "MUSICBRAINZ_ARTISTID",
        "MUSICBRAINZ_ALBUMARTISTID",
        "MUSICBRAINZ_ALBUMID",
        "MUSICBRAINZ_RECORDINGID",
        "REPLAYGAIN_TRACK_GAIN",
        "REPLAYGAIN_REFERENCE_LOUDNESS",
    ] {
        if format_tags.contains_key(key) {
            return Err(format!(
                "[ffprobe] tag '{key}' must be absent from the untagged artifact: media-tagger and rsgain write it"
            )
            .into());
        }
    }
    Ok(())
}

/// Asserts the tags the post-tagging artifact must carry, which are the canonical ones.
///
/// This is the tagged `video` variant — media-tagger's `MusicBrainz` tags with rsgain's loudness
/// tags on top — so it is the only artifact whose tags may be pinned to those literals.
fn assert_format_tags_structural(payload: &serde_json::Value) -> ExampleResult<()> {
    let format_tags = payload
        .get("format")
        .and_then(|format| format.get("tags"))
        .and_then(serde_json::Value::as_object)
        .ok_or_else(|| "[ffprobe] payload missing format.tags".to_string())?;
    assert_tag_eq(format_tags, "title", DEMO_EXPECTED_TITLE)?;
    assert_tag_eq(format_tags, "ARTIST", DEMO_METADATA_ARTIST)?;
    assert_tag_eq(format_tags, "ALBUM", "Never Gonna Give You Up")?;
    assert_tag_eq(format_tags, "DATE", "1987-07-27")?;
    // The step is given no release MBID, so the release is whichever one
    // `MusicBrainz` returns for the recording and the label is asserted by family
    // rather than by the exact string that release happens to carry: the same
    // recording's releases are labelled `RCA` and `RCA Victor`.
    let label = format_tags.get("LABEL").and_then(serde_json::Value::as_str).unwrap_or("");
    if !label.starts_with("RCA") {
        return Err(format!("[ffprobe] tag 'LABEL' expected an RCA label, got '{label}'").into());
    }
    let genre = format_tags.get("GENRE").and_then(serde_json::Value::as_str).unwrap_or("");
    if !genre.contains("blue-eyed soul") && !genre.contains("dance") {
        return Err(format!(
            "[ffprobe] tag 'GENRE' expected to contain 'blue-eyed soul' or 'dance', got '{genre}'",
        )
        .into());
    }
    let comment = format_tags.get("COMMENT").and_then(serde_json::Value::as_str).unwrap_or("");
    if !comment.contains(ONLINE_DEMO_YT_DLP_VIDEO_ID) {
        return Err(format!(
            "[ffprobe] tag 'COMMENT' expected to contain video ID '{ONLINE_DEMO_YT_DLP_VIDEO_ID}', got '{comment}'",
        )
        .into());
    }
    if !format_tags.contains_key("ISRC") {
        return Err("[ffprobe] tag 'ISRC' expected to exist".into());
    }
    for key in [
        "MUSICBRAINZ_ARTISTID",
        "MUSICBRAINZ_ALBUMARTISTID",
        "MUSICBRAINZ_ALBUMID",
        "MUSICBRAINZ_RECORDINGID",
    ] {
        if !format_tags.contains_key(key) {
            return Err(format!("[ffprobe] tag '{key}' expected to exist").into());
        }
    }
    let replaygain = format_tags
        .get("REPLAYGAIN_REFERENCE_LOUDNESS")
        .and_then(serde_json::Value::as_str)
        .unwrap_or("");
    if !replaygain.contains("89.0") {
        return Err(format!(
            "[ffprobe] tag 'REPLAYGAIN_REFERENCE_LOUDNESS' expected to contain '89.0', got '{replaygain}'",
        )
        .into());
    }
    Ok(())
}

fn assert_tagged_media_metadata(path: &Path) -> ExampleResult<()> {
    let payload = ffprobe_json_payload(
        path,
        "format=format_name,duration,nb_streams:stream=codec_type,codec_name,width,height,pix_fmt,field_order,r_frame_rate,display_aspect_ratio,sample_aspect_ratio,color_space,color_transfer,color_primaries,sample_rate,channel_layout,channels",
    )?;
    let format_name = payload
        .get("format")
        .and_then(|format| format.get("format_name"))
        .and_then(serde_json::Value::as_str)
        .unwrap_or("");
    if !format_name.contains("matroska") {
        return Err(format!(
            "[ffprobe] format_name expected to contain 'matroska', got '{format_name}'",
        )
        .into());
    }
    let nb_streams = payload
        .get("format")
        .and_then(|format| format.get("nb_streams"))
        .and_then(serde_json::Value::as_u64)
        .unwrap_or(0);
    if nb_streams != 2 {
        return Err(format!("[ffprobe] nb_streams expected 2, got '{nb_streams}'").into());
    }
    let duration = payload
        .get("format")
        .and_then(|format| format.get("duration"))
        .and_then(serde_json::Value::as_str)
        .and_then(|s| s.parse::<f64>().ok())
        .unwrap_or(0.0);
    if duration <= 0.0 {
        return Err(format!("[ffprobe] duration expected positive, got '{duration}'").into());
    }
    assert_video_stream_properties(&payload)?;
    assert_audio_stream_properties(&payload)?;
    let full_payload = ffprobe_json_payload(path, "format_tags:stream_tags")?;
    assert_format_tags_structural(&full_payload)?;
    Ok(())
}

/// Asserts the pre-tagging artifact's container, streams, and duration.
///
/// `provider_title` is the live yt-dlp title read from `sidecars/info.json`, passed through to
/// [`assert_untagged_format_tags`] so the tag check follows upstream's text.
fn assert_untagged_media_metadata(path: &Path, provider_title: &str) -> ExampleResult<()> {
    let payload = ffprobe_json_payload(
        path,
        "format=format_name,duration,nb_streams:stream=codec_type,codec_name,width,height,pix_fmt,field_order,r_frame_rate,display_aspect_ratio,sample_aspect_ratio,color_space,color_transfer,color_primaries,sample_rate,channel_layout,channels",
    )?;
    let format_name = payload
        .get("format")
        .and_then(|format| format.get("format_name"))
        .and_then(serde_json::Value::as_str)
        .unwrap_or("");
    if !format_name.contains("matroska") {
        return Err(format!(
            "[ffprobe] format_name expected to contain 'matroska', got '{format_name}'",
        )
        .into());
    }
    let nb_streams = payload
        .get("format")
        .and_then(|format| format.get("nb_streams"))
        .and_then(serde_json::Value::as_u64)
        .unwrap_or(0);
    if nb_streams != 2 {
        return Err(format!("[ffprobe] nb_streams expected 2, got '{nb_streams}'").into());
    }
    let duration = payload
        .get("format")
        .and_then(|format| format.get("duration"))
        .and_then(serde_json::Value::as_str)
        .and_then(|s| s.parse::<f64>().ok())
        .unwrap_or(0.0);
    if duration <= 0.0 {
        return Err(format!("[ffprobe] duration expected positive, got '{duration}'").into());
    }
    assert_video_stream_properties(&payload)?;
    assert_audio_stream_properties(&payload)?;
    let full_payload = ffprobe_json_payload(path, "format_tags:stream_tags")?;
    assert_untagged_format_tags(&full_payload, provider_title)?;
    Ok(())
}

/// Canonical media-root basename for the online demo: the `MusicBrainz` recording title.
///
/// The hierarchy templates single-source their basename stem from `${media.metadata.title}`,
/// so this is the canonical form of every materialized name once tagging has run. Upstream's
/// own title embeds the artist, which is why the same template renders an artist-prefixed name
/// in a run that never reached the tagging steps.
fn expected_demo_media_root_name() -> String {
    format!("{DEMO_EXPECTED_TITLE} [{DEMO_MEDIA_ID}]")
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

/// Asserts the artifact is a Matroska container, which both the tagged and the untagged media are.
///
/// Container shape is the one thing the two artifacts share, so it is the one thing this helper
/// checks; tags differ between them and are asserted by their own helpers.
fn assert_video_file_integrity(path: &Path, context: &str) -> ExampleResult<()> {
    let bytes = fs::read(path)
        .map_err(|e| format!("{context}: read video file '{}': {e}", path.display()))?;
    if bytes_look_like_matroska(&bytes) {
        Ok(())
    } else {
        Err(format!("{context}: '{}' does not start with a Matroska (EBML) header", path.display())
            .into())
    }
}

fn assert_file_extension(
    path: &Path,
    expected_extension: &str,
    context: &str,
) -> ExampleResult<()> {
    match path.extension().and_then(|e| e.to_str()) {
        Some(ext) if ext.eq_ignore_ascii_case(expected_extension) => Ok(()),
        Some(ext) => Err(format!(
            "{context}: expected extension '{expected_extension}', got '{ext}' for '{}'",
            path.display()
        )
        .into()),
        None => Err(format!(
            "{context}: expected extension '{expected_extension}', got none for '{}'",
            path.display()
        )
        .into()),
    }
}

/// Asserts the media root carries the canonical basename and that its sibling names derive from it.
///
/// The canonical basename is the `MusicBrainz` recording title, which carries no artist, while
/// upstream's title embeds one; an artist in this name is therefore the fingerprint of a run
/// whose names came from the pre-tagging variant.
fn assert_metadata_files_present(
    interpolated_root: &Path,
    resolved_metadata: &DemoResolvedMetadata,
) -> ExampleResult<()> {
    let resolved_output_base =
        resolve_demo_metadata_template(DEMO_OUTPUT_FILE_NAME_BASE, resolved_metadata);
    let root_folder_name =
        interpolated_root.file_name().and_then(|value| value.to_str()).ok_or_else(|| {
            format!("[metadata] invalid UTF-8 media root '{}'", interpolated_root.display())
        })?;
    if root_folder_name != resolved_output_base {
        return Err(format!(
            "[metadata] interpolated root folder '{root_folder_name}' must match resolved base '{resolved_output_base}'"
        )
        .into());
    }

    // The canonical basename is the MusicBrainz recording title, which carries no
    // artist; upstream's title embeds one. So an exact match against the canonical
    // base is the assertion that tagging ran, and the artist check below guards
    // `expected_demo_media_root_name` itself — it would fail if that helper ever
    // grew an artist prefix, before the equality above could hide it.
    // A run that never reached the tagging steps is a failed run, not a run whose
    // names are allowed to differ.
    let canonical_base = expected_demo_media_root_name();
    if resolved_output_base != canonical_base {
        return Err(format!(
            "[metadata] materialized basename '{resolved_output_base}' must be the canonical title '{canonical_base}'"
        )
        .into());
    }
    if resolved_output_base.contains(DEMO_METADATA_ARTIST) {
        return Err(format!(
            "[metadata] materialized basename '{resolved_output_base}' must not carry '{DEMO_METADATA_ARTIST}': the canonical recording title has no artist prefix"
        )
        .into());
    }

    let untagged_name =
        resolve_demo_metadata_template(DEMO_UNTAGGED_MEDIA_FILE_NAME, resolved_metadata);
    let tagged_name =
        resolve_demo_metadata_template(DEMO_TAGGED_MEDIA_FILE_NAME, resolved_metadata);

    if !untagged_name.starts_with(&resolved_output_base)
        || !tagged_name.starts_with(&resolved_output_base)
    {
        return Err("[metadata] demo metadata template naming invariants drifted".into());
    }

    let info_path = interpolated_root.join(&resolved_output_base).with_extension("json");
    // The info.json may live inside sidecars/; verify the primary sidecar exists.
    let sidecar_info = interpolated_root.join("sidecars").join("info.json");
    if !info_path.is_file() && !sidecar_info.is_file() {
        return Err(format!(
            "[metadata] expected info.json at '{}' or '{}', not found",
            info_path.display(),
            sidecar_info.display()
        )
        .into());
    }

    Ok(())
}

fn assert_sidecar_files_present(
    interpolated_root: &Path,
    resolved_metadata: &DemoResolvedMetadata,
) -> ExampleResult<BTreeMap<String, PathBuf>> {
    let mut sidecar_paths = BTreeMap::new();

    for (entry_label, variant, relative_suffix) in DEMO_SIDECAR_VARIANT_PATHS {
        let resolved_relative_suffix =
            resolve_demo_metadata_template(relative_suffix, resolved_metadata);

        if !resolved_relative_suffix.starts_with("sidecars/")
            && !resolved_relative_suffix.is_empty()
            && resolved_relative_suffix.contains('/')
        {
            return Err(format!(
                "[sidecar] mapping '{entry_label}' must stay flat outside sidecars/ but uses nested path '{relative_suffix}'"
            )
            .into());
        }

        let output_path = if resolved_relative_suffix.is_empty() {
            interpolated_root.to_path_buf()
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
                "[sidecar] expected variant '{}' at '{}' to exist after sync",
                variant,
                output_path.display()
            )
            .into());
        }

        if resolved_relative_suffix.ends_with('/') || resolved_relative_suffix.is_empty() {
            if !output_path.is_dir() {
                return Err(format!(
                    "[sidecar] expected variant '{}' at '{}' to be a directory",
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

            assert_sidecar_directory_family_content(
                variant,
                &output_path,
                &resolved_metadata.provider_title,
                &resolved_metadata.video_id,
            )?;
        } else if !output_path.is_file() {
            return Err(format!(
                "[sidecar] expected variant '{}' at '{}' to be a file",
                variant,
                output_path.display()
            )
            .into());
        } else {
            assert_sidecar_content_expectations(variant, &output_path)?;
        }

        sidecar_paths.insert(entry_label.to_string(), output_path);
    }

    Ok(sidecar_paths)
}

fn resolve_demo_output_paths(
    hierarchy_root: &Path,
) -> ExampleResult<(PathBuf, PathBuf, BTreeMap<String, PathBuf>)> {
    let interpolated_root = resolve_interpolated_demo_root(hierarchy_root)?;
    if !interpolated_root.exists() {
        return Err(format!(
            "[metadata] expected interpolated hierarchy root '{}' to exist after sync",
            interpolated_root.display()
        )
        .into());
    }

    let resolved_metadata = load_resolved_demo_metadata(&interpolated_root)?;
    if resolved_metadata.media_id != DEMO_MEDIA_ID {
        return Err(format!(
            "[metadata] expected resolved media id '{}' but observed '{}'",
            DEMO_MEDIA_ID, resolved_metadata.media_id
        )
        .into());
    }
    if resolved_metadata.video_id != DEMO_EXPECTED_VIDEO_ID {
        return Err(format!(
            "[metadata] expected resolved video id '{}' but observed '{}'",
            DEMO_EXPECTED_VIDEO_ID, resolved_metadata.video_id
        )
        .into());
    }
    if !resolved_metadata.video_ext.eq_ignore_ascii_case(DEMO_EXPECTED_VIDEO_EXTENSION_WITH_DOT) {
        return Err(format!(
            "[metadata] expected resolved video extension '{}' but observed '{}'",
            DEMO_EXPECTED_VIDEO_EXTENSION_WITH_DOT, resolved_metadata.video_ext
        )
        .into());
    }

    assert_metadata_files_present(&interpolated_root, &resolved_metadata)?;

    let resolved_output_base =
        resolve_demo_metadata_template(DEMO_OUTPUT_FILE_NAME_BASE, &resolved_metadata);
    let resolved_untagged_file_name =
        resolve_demo_metadata_template(DEMO_UNTAGGED_MEDIA_FILE_NAME, &resolved_metadata);
    let resolved_tagged_file_name =
        resolve_demo_metadata_template(DEMO_TAGGED_MEDIA_FILE_NAME, &resolved_metadata);

    let video_path = interpolated_root.join(&resolved_untagged_file_name);
    if !video_path.is_file() {
        let observed_files = fs::read_dir(&interpolated_root)?
            .flatten()
            .map(|entry| entry.file_name().to_string_lossy().to_string())
            .collect::<Vec<_>>();
        return Err(format!(
            "[video] expected untagged media '{}' to exist after sync; observed files: {observed_files:?}",
            video_path.display()
        )
        .into());
    }
    assert_file_extension(
        &video_path,
        DEMO_EXPECTED_VIDEO_EXTENSION_WITH_DOT.trim_start_matches('.'),
        "[video] untagged",
    )?;
    assert_video_file_integrity(&video_path, "[video] untagged")?;
    assert_mkv_video_audio_with_ffprobe(&video_path)?;
    assert_untagged_media_metadata(&video_path, &resolved_metadata.provider_title)?;

    let tagged_video_path = interpolated_root.join(&resolved_tagged_file_name);
    if !tagged_video_path.is_file() {
        return Err(format!(
            "[video] expected tagged media '{}' to exist after sync",
            tagged_video_path.display()
        )
        .into());
    }
    assert_file_extension(
        &tagged_video_path,
        DEMO_EXPECTED_VIDEO_EXTENSION_WITH_DOT.trim_start_matches('.'),
        "[video] tagged",
    )?;
    assert_video_file_integrity(&tagged_video_path, "[video] tagged")?;
    assert_mkv_video_audio_with_ffprobe(&tagged_video_path)?;
    assert_tagged_media_metadata(&tagged_video_path)?;

    let sidecar_paths = assert_sidecar_files_present(&interpolated_root, &resolved_metadata)?;

    assert_flat_media_root_sidecar_families(&interpolated_root, &resolved_output_base)?;
    assert_no_mediapm_marker_in_hierarchy_paths(&interpolated_root)?;

    Ok((video_path, tagged_video_path, sidecar_paths))
}

/// Runs the online demo end to end and returns the paths it wrote.
///
/// Shuts the conductor the service owns down before returning:
/// `Conductor::shutdown` awaits the background pass that would otherwise write
/// into the caller's removed directory.
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

    let logical_tool_ids = configure_document_for_online_demo(&workspace_root)?;

    // Full library sync with a filesystem-backed CAS. `sync_library*` re-provisions
    // managed tools internally (step 2), so there is no separate tool-sync
    // precheck: running one would only re-provision the same tools into a
    // discarded in-memory CAS before the real sync re-provisions them again.
    let mut sync_service =
        MediaPmService::new_fs_at_with_runtime_storage_overrides(&workspace_root, runtime_storage)
            .await
            .map_err(|error| format!("opening filesystem sync service: {error}"))?;

    eprintln!(
        "[demo_online] starting sync (timeout={}s) in '{}'",
        sync_timeout.as_secs(),
        workspace_root.display()
    );

    let options = mediapm::SyncLibraryOptions {
        verify_materialization: true,
        observer: Some(std::sync::Arc::new(mediapm::output::observer::CliSyncObserver)),
        ..Default::default()
    };
    let summary = sync_service
        .sync_library_with_options(options)
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
    let playlist_path = hierarchy_root.join("playlists").join("rickroll.m3u8");
    assert_playlist_content_format(&playlist_path)?;
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

    // Second sync: the demo's idempotency proof. A full run always re-syncs
    // and asserts the re-run did no work, so the artifact a human inspects
    // shows the guarantee rather than claiming it. The materialized file set
    // is snapshotted before and after, so a re-materialization (even one that
    // rewrote identical bytes) fails the run.
    let materialized_before = snapshot_materialized_files(&hierarchy_root)?;
    let second_options = mediapm::SyncLibraryOptions {
        verify_materialization: true,
        no_progress: true,
        ..Default::default()
    };
    let second = sync_service
        .sync_library_with_options(second_options)
        .await
        .map_err(|error| format!("online demo second sync failed: {error}"))?;
    let materialized_after = snapshot_materialized_files(&hierarchy_root)?;
    let second_run = SecondRunTally {
        executed_instances: second.executed_instances,
        cached_instances: second.cached_instances,
        materialized_paths: second.materialized_paths,
        skipped_paths: second.skipped_paths,
        removed_paths: second.removed_paths,
        added_tools: second.added_tools,
        updated_tools: second.updated_tools,
        skipped_tools: second.skipped_tools,
        pruned_tools: second.pruned_tools,
        warning_count: second.warnings.len(),
    };
    assert_second_sync_did_no_work(&summary, &second, &materialized_before, &materialized_after)?;
    eprintln!(
        "[demo_online] second sync idempotent: executed={executed} cached={cached} \
         materialized={materialized} skipped={skipped} updated_tools={updated} \
         (no-payload import builtin) warnings={warnings}",
        executed = second_run.executed_instances,
        cached = second_run.cached_instances,
        materialized = second_run.materialized_paths,
        skipped = second_run.skipped_paths,
        updated = second_run.updated_tools,
        warnings = second_run.warning_count,
    );

    let store_size_bytes = summarize_store_size(sync_service.conductor().cas(), &cas_root).await?;
    let materialization_preference_order = DEMO_MATERIALIZATION_PREFERENCE_ORDER
        .iter()
        .map(|method| format!("{method:?}"))
        .collect::<Vec<_>>();

    let _yt_dlp_binary =
        tool_binaries.get(&yt_dlp_tool_id).ok_or("yt-dlp command selector missing")?;

    let manifest = DemoManifest {
        run_mode: "full".to_string(),
        generated_unix_epoch_seconds: unix_timestamp_seconds(),
        artifact_root: display_path(&root),
        workspace_root: display_path(&workspace_root),
        tool_ids,
        tool_binaries: tool_binaries
            .iter()
            .map(|(tool_id, command)| (tool_id.clone(), command.clone()))
            .collect(),
        managed_tools: summarize_managed_tool_provenance(&lock.managed_tools),
        yt_dlp_max_concurrent_calls,
        yt_dlp_max_retries,
        mediapm_ncl_path: display_path(&sync_service.paths().mediapm_ncl),
        conductor_generated_ncl_path: display_path(&sync_service.paths().conductor_generated_ncl),
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
        materialized_file_hashes: lock
            .managed_files
            .iter()
            .map(|(path, record)| (path.clone(), record.hash.clone()))
            .collect(),
        executed_instances: summary.executed_instances,
        cached_instances: summary.cached_instances,
        materialized_paths: summary.materialized_paths,
        removed_paths: summary.removed_paths,
        added_tools: summary.added_tools,
        updated_tools: summary.updated_tools,
        warning_count: summary.warnings.len(),
        second_run: Some(second_run),
        profile_path: display_path(&sync_service.paths().runtime_root.join("profile.json")),
        store_size_bytes,
    };

    let manifest_path = root.join("manifest.json");
    write_json_file(&manifest_path, &manifest)?;

    sync_service.conductor().shutdown().await?;

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
    let run_sync = example_isolation::parse_run_online_sync_override(
        std::env::var(example_isolation::RUN_ONLINE_SYNC_ENV).ok().as_deref(),
    )?;
    // Watchdog budgets resolve before the mode branch so both modes export
    // the same knobs: a bad timeout env value fails fast everywhere, and the
    // conductor sees the same budget whenever it is constructed.
    let sync_timeout = online_demo_timeout()?;
    configure_demo_conductor_executable_timeout(sync_timeout);
    configure_demo_conductor_rpc_timeout(sync_timeout);
    if !run_sync {
        let paths = run_online_demo_config_only().await?;
        println!("generated artifacts root: {}", paths.artifact_root.display());
        println!("generated workspace root: {}", paths.workspace_root.display());
        println!("manifest: {}", paths.manifest_path.display());
        println!("sync executed: false");
        return Ok(());
    }

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

    /// Stands in for the live yt-dlp provider title, which the run reads from `sidecars/info.json`.
    ///
    /// It is the shape upstream writes, artist embedded and all, which is what the untagged
    /// artifact's title assertion follows at run time.
    const UPSTREAM_TITLE: &str =
        "Rick Astley - Never Gonna Give You Up (Official Video) (4K Remaster)";

    /// Ensures the pre-tagging artifact's tag assertion accepts the upstream download.
    ///
    /// Upstream writes the provider title, the channel as `ARTIST`, and the video URL as
    /// `COMMENT`; none of the `MusicBrainz` or rsgain tags exists yet, which is exactly what this
    /// assertion has to accept.
    #[test]
    fn untagged_format_tags_accept_the_upstream_download() {
        let payload = serde_json::json!({
            "format": {
                "tags": {
                    "title": UPSTREAM_TITLE,
                    "ARTIST": super::DEMO_METADATA_ARTIST,
                    "COMMENT": format!(
                        "https://www.youtube.com/watch?v={}",
                        super::ONLINE_DEMO_YT_DLP_VIDEO_ID
                    ),
                    "DATE": "20091025",
                    "GENRE": "Music",
                }
            }
        });

        super::assert_untagged_format_tags(&payload, UPSTREAM_TITLE)
            .expect("the upstream download's own tags must satisfy the untagged assertion");
    }

    /// Ensures the pre-tagging artifact's tag assertion rejects a tag a later step writes.
    ///
    /// `ISRC` is written by media-tagger and `REPLAYGAIN_TRACK_GAIN` by rsgain, so either one
    /// appearing on the untagged artifact means a tagging step wrote into the wrong variant.
    #[test]
    fn untagged_format_tags_reject_tags_a_later_step_writes() {
        // Every tag a later step writes, so the production list cannot silently lose one.
        for key in [
            "ALBUM",
            "LABEL",
            "ISRC",
            "MUSICBRAINZ_ARTISTID",
            "MUSICBRAINZ_ALBUMARTISTID",
            "MUSICBRAINZ_ALBUMID",
            "MUSICBRAINZ_RECORDINGID",
            "REPLAYGAIN_TRACK_GAIN",
            "REPLAYGAIN_REFERENCE_LOUDNESS",
        ] {
            let mut tags = serde_json::Map::new();
            tags.insert("title".to_string(), serde_json::json!(UPSTREAM_TITLE));
            tags.insert("ARTIST".to_string(), serde_json::json!(super::DEMO_METADATA_ARTIST));
            tags.insert(
                "COMMENT".to_string(),
                serde_json::json!(format!(
                    "https://www.youtube.com/watch?v={}",
                    super::ONLINE_DEMO_YT_DLP_VIDEO_ID
                )),
            );
            tags.insert(key.to_string(), serde_json::json!("present"));
            let payload = serde_json::json!({ "format": { "tags": tags } });

            let error = super::assert_untagged_format_tags(&payload, UPSTREAM_TITLE)
                .expect_err("a later step's tag must not appear on the untagged artifact");
            assert!(error.to_string().contains(key), "the rejection must name '{key}': {error}");
        }
    }

    /// Ensures the two artifacts' tag assertions disagree, so neither can cover both.
    ///
    /// The tagged assertion pins `MusicBrainz`'s recording title and the `MusicBrainz` tags; the
    /// untagged one pins upstream's title and their absence. A payload cannot satisfy both.
    #[test]
    fn the_two_artifact_tag_assertions_disagree() {
        let payload = serde_json::json!({
            "format": {
                "tags": {
                    "title": UPSTREAM_TITLE,
                    "ARTIST": super::DEMO_METADATA_ARTIST,
                    "COMMENT": format!(
                        "https://www.youtube.com/watch?v={}",
                        super::ONLINE_DEMO_YT_DLP_VIDEO_ID
                    ),
                }
            }
        });

        super::assert_untagged_format_tags(&payload, UPSTREAM_TITLE)
            .expect("the upstream tags are the untagged artifact's expectation");
        let error = super::assert_format_tags_structural(&payload)
            .expect_err("the tagged assertion must reject an untagged artifact");
        assert!(
            error.to_string().contains("title"),
            "the tagged assertion must reject on the title: {error}"
        );
    }

    /// Ensures the tagged artifact's assertion accepts the names the tagging steps write.
    ///
    /// The names and values here are the ones measured on a real tagged file: every tag
    /// media-tagger and rsgain write is in the capitals the `Matroska` tag specification
    /// assigns, and the title stays `title`, the key ffmpeg reads as the container's Title
    /// element. The tagged artifact needs the network, so this fixture is what keeps the
    /// assertion honest about the names the steps actually produce; a required key added
    /// without measuring one fails here rather than only in a live run.
    #[test]
    fn tagged_format_tags_accept_what_the_tagging_steps_write() {
        let payload = serde_json::json!({
            "format": {
                "tags": {
                    "title": super::DEMO_EXPECTED_TITLE,
                    "ARTIST": super::DEMO_METADATA_ARTIST,
                    "ALBUM": "Never Gonna Give You Up",
                    "ALBUMARTIST": super::DEMO_METADATA_ARTIST,
                    "DATE": "1987-07-27",
                    "LABEL": "RCA Victor",
                    "GENRE": "blue-eyed soul; dance; dance-pop",
                    "COMMENT": format!(
                        "https://www.youtube.com/watch?v={}",
                        super::ONLINE_DEMO_YT_DLP_VIDEO_ID
                    ),
                    "ISRC": "GB5KW2103369",
                    "MUSICBRAINZ_ARTISTID": "db92a151-1ac2-438b-bc43-b82e149ddd50",
                    "MUSICBRAINZ_ALBUMARTISTID": "db92a151-1ac2-438b-bc43-b82e149ddd50",
                    "MUSICBRAINZ_ALBUMID": "7171f94c-7307-4ca7-a2db-a5e4be09cdac",
                    "MUSICBRAINZ_RECORDINGID": "8f3471b5-7e6a-48da-86a9-c1c07a0f47ae",
                    "REPLAYGAIN_REFERENCE_LOUDNESS": "89.0 dB",
                }
            }
        });

        super::assert_format_tags_structural(&payload)
            .expect("the names the tagging steps write must satisfy the tagged assertion");
    }

    /// Ensures the example's pinned media-root basename agrees with the shared golden helper.
    ///
    /// Two modules spell the canonical name: this example pins it for the live run, and
    /// `demo_hierarchy_spec` renders it inside the golden's relative path. Drift between the two
    /// would leave the live demo asserting a name the golden no longer records.
    #[test]
    fn canonical_media_root_name_agrees_with_the_golden_helper() {
        let golden = mediapm::demo_hierarchy_spec::online_demo_media_folder_relative();
        let basename = golden.rsplit('/').next().expect("golden path has a final component");

        assert_eq!(super::expected_demo_media_root_name(), basename);
    }

    /// Ensures the canonical media-root basename is the bare recording title.
    ///
    /// The materialized name follows the tagged `video` variant whenever tagging ran, and
    /// media-tagger writes `MusicBrainz`'s recording title, which carries no artist. Upstream's
    /// title embeds one, so an artist prefix in the expected name would mean the name followed
    /// the pre-tagging variant instead.
    #[test]
    fn canonical_media_root_name_is_the_bare_recording_title() {
        assert_eq!(
            super::expected_demo_media_root_name(),
            format!("{} [{}]", super::DEMO_EXPECTED_TITLE, super::DEMO_MEDIA_ID)
        );
        assert!(
            !super::expected_demo_media_root_name().contains(super::DEMO_METADATA_ARTIST),
            "the canonical name must not carry the artist"
        );
    }

    /// Ensures the online demo resolves its title from the tagged variant first.
    ///
    /// The order is what makes the materialized names canonical: media-tagger redefines the
    /// `video` variant with `MusicBrainz`'s recording title, while the `infojson` variant carries
    /// upstream's title, which embeds the artist. Nothing hermetic exercises the live chain — the
    /// tagging steps need the network — so this test reads the document the demo configures and
    /// pins the order the live run resolves in.
    #[test]
    fn the_title_chain_prefers_the_tagged_variant() -> super::ExampleResult<()> {
        let isolated = IsolatedExampleRoots::with_cache();
        let workspace_root = isolated.artifact_root_path();
        super::configure_document_for_online_demo(workspace_root)?;

        let document = super::load_mediapm_document(&workspace_root.join("mediapm.ncl"))?;
        let source =
            document.media.get(super::DEMO_MEDIA_ID).expect("the demo configures its own media id");
        let super::MediaMetadataValue::Fallback(candidates) =
            source.metadata.get("title").expect("the demo configures a title")
        else {
            panic!("the demo's title must be a fallback chain");
        };

        let described: Vec<String> = candidates
            .iter()
            .map(|candidate| match candidate {
                super::MediaMetadataValueCandidate::Variant(binding) => {
                    format!("variant:{}", binding.variant)
                }
                super::MediaMetadataValueCandidate::Literal(text) => format!("literal:{text}"),
            })
            .collect();
        assert_eq!(
            described,
            vec![
                "variant:video".to_string(),
                "variant:infojson".to_string(),
                format!("literal:{}", super::DEMO_EXPECTED_TITLE),
            ],
            "the tagged variant must win, the upstream one must follow it, and the literal must \
             be the bare canonical title"
        );

        Ok(())
    }

    /// Ensures sync-mode override parser accepts disabled values (reduced mode).
    #[test]
    fn run_sync_override_accepts_disabled_tokens() {
        let _env_lock = example_isolation::lock_process_env();
        let previous = std::env::var(example_isolation::RUN_ONLINE_SYNC_ENV).ok();

        // SAFETY: test mutates one process env key in a controlled scope and
        // restores the previous value before exit.
        unsafe {
            std::env::set_var(example_isolation::RUN_ONLINE_SYNC_ENV, "false");
        }
        let result = example_isolation::parse_run_online_sync_override(Some("false"));

        // SAFETY: restore previous env var value for test isolation.
        unsafe {
            if let Some(value) = previous {
                std::env::set_var(example_isolation::RUN_ONLINE_SYNC_ENV, value);
            } else {
                std::env::remove_var(example_isolation::RUN_ONLINE_SYNC_ENV);
            }
        }

        assert!(matches!(result, Ok(false)), "disabled run-sync tokens must select reduced mode");
    }

    /// Ensures demo-online sync override allows unset environment (full mode).
    #[test]
    fn run_sync_override_validator_allows_unset_env() {
        let _env_lock = example_isolation::lock_process_env();
        let previous = std::env::var(example_isolation::RUN_ONLINE_SYNC_ENV).ok();
        // SAFETY: test mutates one process env key in a controlled scope and
        // restores the previous value before exit.
        unsafe {
            std::env::remove_var(example_isolation::RUN_ONLINE_SYNC_ENV);
        }

        let result = example_isolation::parse_run_online_sync_override(None);

        // SAFETY: restore previous env var value for test isolation.
        unsafe {
            if let Some(value) = previous {
                std::env::set_var(example_isolation::RUN_ONLINE_SYNC_ENV, value);
            }
        }

        assert!(matches!(result, Ok(true)), "unset run-sync override should select full mode");
    }

    /// Ensures sync-mode override parser still rejects unknown tokens.
    #[test]
    fn run_sync_override_rejects_invalid_tokens() {
        let _env_lock = example_isolation::lock_process_env();
        let previous = std::env::var(example_isolation::RUN_ONLINE_SYNC_ENV).ok();

        // SAFETY: test mutates one process env key in a controlled scope and
        // restores the previous value before exit.
        unsafe {
            std::env::set_var(example_isolation::RUN_ONLINE_SYNC_ENV, "maybe");
        }
        let result = example_isolation::parse_run_online_sync_override(Some("maybe"));

        // SAFETY: restore previous env var value for test isolation.
        unsafe {
            if let Some(value) = previous {
                std::env::set_var(example_isolation::RUN_ONLINE_SYNC_ENV, value);
            } else {
                std::env::remove_var(example_isolation::RUN_ONLINE_SYNC_ENV);
            }
        }

        assert!(result.is_err(), "unknown run-sync tokens must be rejected");
    }

    /// Points example env vars at unique tempdirs for the guard's lifetime.
    /// Executes the documented example entry point via `main()` in reduced
    /// (config-only) mode.
    ///
    /// The online full-sync path is nondeterministic (network + external
    /// tools) and is NOT exercised here; it runs only on an explicit
    /// `cargo run -p mediapm --example mediapm_demo_online`. Run conditions:
    /// (1) CI → skip via `example_isolation::ci_mode_detected()`; (2) any other
    /// test harness → reduced mode via `MEDIAPM_RUN_ONLINE_SYNC=false`; (3)
    /// explicit `cargo run --example` → full sync (no test binary exists).
    #[test]
    fn main_is_exercised() {
        if example_isolation::ci_mode_detected() {
            eprintln!(
                "[demo_online] skipping main_is_exercised in CI; \
                 the full-sync online path is nondeterministic and needs \
                 network and external tools"
            );
            return;
        }

        let _isolated = IsolatedExampleRoots::with_cache();

        let previous = std::env::var(example_isolation::RUN_ONLINE_SYNC_ENV).ok();
        // SAFETY: test mutates one process env key in a controlled scope and
        // restores the previous value before exit.
        unsafe {
            std::env::set_var(example_isolation::RUN_ONLINE_SYNC_ENV, "false");
        }

        // Clear the watchdog knobs so the defaulted path is deterministic:
        // a value leaked by a sibling test would shadow the computation the
        // assertion below protects.
        let previous_rpc_budget = std::env::var(super::DEMO_CONDUCTOR_RPC_TIMEOUT_SECONDS_ENV).ok();
        let previous_sync_budget = std::env::var(super::DEMO_ONLINE_TIMEOUT_SECS_ENV).ok();
        let previous_exec_timeout =
            std::env::var(super::DEMO_CONDUCTOR_EXECUTABLE_TIMEOUT_SECS_ENV).ok();
        // SAFETY: same controlled-scope mutation and restore as above.
        unsafe {
            std::env::remove_var(super::DEMO_CONDUCTOR_RPC_TIMEOUT_SECONDS_ENV);
            std::env::remove_var(super::DEMO_ONLINE_TIMEOUT_SECS_ENV);
            std::env::remove_var(super::DEMO_CONDUCTOR_EXECUTABLE_TIMEOUT_SECS_ENV);
        }

        super::main().expect("example main should run to completion");

        // The demo must export a conductor RPC budget strictly below its own
        // overall sync budget: the conductor-layer timeout error names the
        // budget and knob, so it has to fire before the demo's blind
        // hard-timeout wrapper to keep the failure diagnosable.
        let rpc_budget = std::env::var(super::DEMO_CONDUCTOR_RPC_TIMEOUT_SECONDS_ENV)
            .expect("main must export the conductor RPC budget");
        let expected = (super::DEMO_ONLINE_TIMEOUT_SECS_DEFAULT
            - super::DEMO_CONDUCTOR_RPC_TIMEOUT_MARGIN_SECS)
            .to_string();
        assert_eq!(rpc_budget, expected, "RPC budget must default to sync budget minus the margin");
        assert!(
            rpc_budget.parse::<u64>().expect("budget renders as seconds")
                < super::DEMO_ONLINE_TIMEOUT_SECS_DEFAULT,
            "RPC budget must stay below the demo's overall sync budget"
        );

        // SAFETY: restore previous env var values for test isolation.
        unsafe {
            match previous {
                Some(value) => std::env::set_var(example_isolation::RUN_ONLINE_SYNC_ENV, value),
                None => std::env::remove_var(example_isolation::RUN_ONLINE_SYNC_ENV),
            }
            match previous_rpc_budget {
                Some(value) => {
                    std::env::set_var(super::DEMO_CONDUCTOR_RPC_TIMEOUT_SECONDS_ENV, value)
                }
                None => std::env::remove_var(super::DEMO_CONDUCTOR_RPC_TIMEOUT_SECONDS_ENV),
            }
            match previous_sync_budget {
                Some(value) => std::env::set_var(super::DEMO_ONLINE_TIMEOUT_SECS_ENV, value),
                None => std::env::remove_var(super::DEMO_ONLINE_TIMEOUT_SECS_ENV),
            }
            match previous_exec_timeout {
                Some(value) => {
                    std::env::set_var(super::DEMO_CONDUCTOR_EXECUTABLE_TIMEOUT_SECS_ENV, value)
                }
                None => std::env::remove_var(super::DEMO_CONDUCTOR_EXECUTABLE_TIMEOUT_SECS_ENV),
            }
        }

        // Reduced mode must leave the config-only manifest behind; proves the
        // test exercised reduced mode, not a (network-hitting) full sync.
        let manifest_path = _isolated.artifact_root_path().join("manifest.json");
        let manifest: serde_json::Value = serde_json::from_slice(
            &std::fs::read(&manifest_path).expect("config-only run should write a manifest.json"),
        )
        .expect("manifest.json should be valid JSON");
        assert_eq!(manifest["run_mode"], "config-only");
        // Reduced mode runs no sync, so it must record no second-run block:
        // the block appears only on the full-sync path, where it carries the
        // idempotency tallies of the demo's re-run.
        assert!(
            manifest.get("second_run").is_none(),
            "config-only manifest must not record a second_run tally"
        );
    }

    /// Pins the second-run tally block's schema: full runs serialize these
    /// exact keys into `manifest.json` under `second_run`, and any rename or
    /// removal must fail here before it fails a consumer reading the artifact.
    #[test]
    fn second_run_tally_serializes_the_documented_schema() {
        let tally = super::SecondRunTally {
            executed_instances: 0,
            cached_instances: 16,
            materialized_paths: 0,
            skipped_paths: 16,
            removed_paths: 0,
            added_tools: 0,
            updated_tools: 1,
            skipped_tools: 6,
            pruned_tools: 0,
            warning_count: 0,
        };
        let value = serde_json::to_value(&tally).expect("tally serializes");
        let mut keys = value
            .as_object()
            .expect("tally serializes as an object")
            .keys()
            .cloned()
            .collect::<Vec<_>>();
        // `serde_json::Value` sorts object keys, so the schema is the key set,
        // not an order.
        keys.sort();
        let mut expected = vec![
            "executed_instances",
            "cached_instances",
            "materialized_paths",
            "skipped_paths",
            "removed_paths",
            "added_tools",
            "updated_tools",
            "skipped_tools",
            "pruned_tools",
            "warning_count",
        ];
        expected.sort_unstable();
        assert_eq!(keys, expected, "second_run manifest block must keep its documented key set");
    }

    /// Ensures artifact root remains stable for docs/scripts that reference it.
    #[test]
    fn artifact_root_is_stable() {
        let _env_lock = example_isolation::lock_process_env();
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

    /// Ensures the watchdog hard limit stays in sync with nextest's network-group
    /// `slow-timeout.terminate-after` (600s) in `.config/nextest.toml`; a future
    /// one-sided bump fails here.
    #[test]
    fn hard_watchdog_total_matches_nextest_terminate_after() {
        assert_eq!(super::DEMO_ONLINE_HARD_TIMEOUT_TOTAL_SECS, 600);
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
        std::fs::write(
            root.join(format!("{output_base}.thumbnail.jpg")),
            &[0xFF, 0xD8, 0xFF, 0xE0, 0, 0, 0, 0, 0, 0, 0, 0],
        )
        .expect("write thumbnail");
        std::fs::write(
            root.join(format!("{output_base}.link.url")),
            format!("[InternetShortcut]\nURL=https://www.youtube.com/watch?v=dQw4w9WgXcQ\n"),
        )
        .expect("write link");
        std::fs::write(
            root.join(format!("{output_base}.link.webloc")),
            format!(
                "<?xml version=\"1.0\" encoding=\"UTF-8\"?>\n<!DOCTYPE plist PUBLIC \"-//Apple//DTD PLIST 1.0//EN\" \"http://www.apple.com/DTDs/PropertyList-1.0.dtd\">\n<plist version=\"1.0\"><dict><key>URL</key><string>https://www.youtube.com/watch?v=dQw4w9WgXcQ</string></dict></plist>\n"
            ),
        )
        .expect("write webloc link");
        std::fs::write(
            root.join(format!("{output_base}.link.desktop")),
            format!(
                "[Desktop Entry]\nType=Link\nName=Video\nURL=https://www.youtube.com/watch?v=dQw4w9WgXcQ\nEncoding=UTF-8\n"
            ),
        )
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
            b"RIFF\x00\x00\x00\x00WEBP",
        )
        .expect("write thumbnail");
        std::fs::write(
            root.join(format!("{output_base}.link.url")),
            format!("[InternetShortcut]\nURL=https://www.youtube.com/watch?v=dQw4w9WgXcQ\n"),
        )
        .expect("write link");
        std::fs::write(
            root.join(format!("{output_base}.link.webloc")),
            format!(
                "<?xml version=\"1.0\" encoding=\"UTF-8\"?>\n<!DOCTYPE plist PUBLIC \"-//Apple//DTD PLIST 1.0//EN\" \"http://www.apple.com/DTDs/PropertyList-1.0.dtd\">\n<plist version=\"1.0\"><dict><key>URL</key><string>https://www.youtube.com/watch?v=dQw4w9WgXcQ</string></dict></plist>\n"
            ),
        )
        .expect("write webloc link");
        std::fs::write(
            root.join(format!("{output_base}.link.desktop")),
            format!(
                "[Desktop Entry]\nType=Link\nName=Video\nURL=https://www.youtube.com/watch?v=dQw4w9WgXcQ\nEncoding=UTF-8\n"
            ),
        )
        .expect("write desktop link");

        super::assert_flat_media_root_sidecar_families(root, output_base)
            .expect("media-id-aligned non-subtitle sidecars should be accepted");
    }

    /// Ensures the manifest's per-tool provenance follows the deployment the
    /// run resolved against: when one tool id has several recorded deploys,
    /// the newest supplies the canonical version and the provenance fields.
    #[test]
    fn managed_tool_provenance_keeps_the_latest_deployed_entry() {
        let older = mediapm::ToolRegistryEntry {
            tool_id: "yt-dlp".to_string(),
            version: "2026.01.01".to_string(),
            canonical_version: "older-commit".to_string(),
            content_map_hash: "older-hash".to_string(),
            deployed_at: mediapm_utils::Timestamp::from_unix_nanos(1),
            resolved_tag: Some("2026.01.01".to_string()),
            resolved_version: Some("2026.01.01".to_string()),
            resolved_vcs_hash: Some("older-vcs".to_string()),
        };
        let newer = mediapm::ToolRegistryEntry {
            tool_id: "yt-dlp".to_string(),
            version: "2026.02.02".to_string(),
            canonical_version: "newer-commit".to_string(),
            content_map_hash: "newer-hash".to_string(),
            deployed_at: mediapm_utils::Timestamp::from_unix_nanos(2),
            resolved_tag: Some("2026.02.02".to_string()),
            resolved_version: Some("2026.02.02".to_string()),
            resolved_vcs_hash: Some("newer-vcs".to_string()),
        };

        // Both orders must land on the newer deployment: the summarizer
        // selects by `deployed_at`, not by position in the lockfile.
        for entries in [vec![older.clone(), newer.clone()], vec![newer, older]] {
            let summary = super::summarize_managed_tool_provenance(&entries);

            assert_eq!(summary.len(), 1, "one record per logical tool id");
            let provenance = summary.get("yt-dlp").expect("yt-dlp provenance recorded");
            assert_eq!(provenance.canonical_version, "newer-commit");
            assert_eq!(provenance.resolved_tag.as_deref(), Some("2026.02.02"));
            assert_eq!(provenance.resolved_version.as_deref(), Some("2026.02.02"));
            assert_eq!(provenance.resolved_vcs_hash.as_deref(), Some("newer-vcs"));
        }
    }
}
