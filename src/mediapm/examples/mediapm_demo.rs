//! Persistent `mediapm` demo producing inspectable artifacts.
//!
//! Demonstrates local ingest + transform flow: bundled MP4 fixture → CAS
//! → `import -> ffmpeg -> rsgain -> media-tagger` pipeline.
//! Default sync enabled; the offline demo always runs a full sync.
//!
//! # Expected post-sync hierarchy
//!
//! Paths below are relative to `<artifact_root>/media/` (`runtime.hierarchy_root_dir = "media"`).
//! Canonical machine checks: `tests/fixtures/demo_hierarchy_golden.json` (`offline`) and
//! [`mediapm::demo_hierarchy_spec`]. Online demo tree: see `mediapm_demo_online`.
//!
//! Shared constants: artist `Rick Astley`, title `Never Gonna Give You Up`, library root `music videos`.
//!
//! **Media id:** `demo.local.dQw4w9WgXcQ`
//!
//! ## On-disk tree
//!
//! ```text
//! media/
//! ├── music videos/
//! │   └── Rick Astley - Never Gonna Give You Up [demo.local.dQw4w9WgXcQ]/
//! │       ├── Rick Astley - Never Gonna Give You Up [demo.local.dQw4w9WgXcQ].untagged.mp4   # variant video_untagged
//! │       └── Rick Astley - Never Gonna Give You Up [demo.local.dQw4w9WgXcQ].m4a            # variant audio (replaygain target)
//! └── playlists/
//!     └── local-demo.m3u8                                                                   # playlist, 2 entries → tagged audio leaf
//! ```
//!
//! ## Hierarchy config (`mediapm.ncl`)
//!
//! ```text
//! music videos/                                           [Folder]
//! └── ${artist} - ${title} [${id}]/                       [Folder, id=demo.local.dQw4w9WgXcQ.media_folder]
//!     ├── ${artist} - ${title} [${id}].untagged${video_ext_untagged}   [Media, variant=video_untagged]
//!     └── ${artist} - ${title} [${id}]${video_ext}                     [Media, variant=audio]
//! playlists/
//! └── local-demo.m3u8                                     [Playlist, 2 shorthand ids → demo.local.dQw4w9WgXcQ]
//! ```
//!
//! Resolved metadata: `video_ext_untagged` → `.mp4`, `video_ext` → `.m4a`.
//!
//! # Verification
//!
//! ```bash
//! cargo test -p mediapm demo_hierarchy_golden
//! cargo test -p mediapm --example mediapm_demo -- --skip main_is_exercised
//! cargo run -p mediapm --example mediapm_demo
//! ```
//!
//! The explicit `cargo run --example` run reuses the real user-level tool
//! download cache (`<os-cache-dir>/mediapm/cache`), persisting downloaded
//! tools across runs; embedded tests stay isolated via
//! `MEDIAPM_EXAMPLE_CACHE_ROOT`.

use std::collections::{BTreeMap, BTreeSet};
use std::error::Error;
use std::fs;
use std::path::{Path, PathBuf};
use std::str::FromStr;
use std::time::{SystemTime, UNIX_EPOCH};

use mediapm::{
    AddInsertPosition, ConfigVersionSpec, GenericOutputVariantConfig, HierarchyNode,
    HierarchyNodeKind, HierarchyPath, MaterializationMethod, MediaMetadataValue, MediaPmService,
    MediaRuntimeStorage, MediaSourceSpec, MediaStep, MediaStepTool, OutputVariantValue,
    PlaylistFormat, PlaylistItemRef, SanitizeNamesConfig, ToolRequirement, TransformInputValue,
    YtDlpOutputKind, YtDlpOutputVariantConfig, example_isolation, load_mediapm_document,
    load_mediapm_state_document, save_mediapm_document, save_mediapm_state_document,
};
use mediapm_cas::{CasApi, FileSystemCas, Hash};
use mediapm_conductor::{
    NickelDocument, ToolKindSpec, ToolRuntime, ToolSpec, decode_document,
    default_runtime_inherited_env_vars, encode_document,
};
use same_file::is_same_file;
use serde::Serialize;

/// Shared result type for this demo.
type ExampleResult<T> = Result<T, Box<dyn Error>>;

/// Embedded tiny MP4 payload containing both video and audio tracks.
const SAMPLE_AV_MP4_BYTES: &[u8] = include_bytes!("assets/sample-av.mp4");

const DEMO_MEDIA_ID: &str = "demo.local.dQw4w9WgXcQ";
const DEMO_PLAYLIST_TARGET_HIERARCHY_ID: &str = "demo.local.dQw4w9WgXcQ";
const DEMO_UNTAGGED_HIERARCHY_ID: &str = "demo.local.dQw4w9WgXcQ.untagged";
const DEMO_MEDIA_FOLDER_HIERARCHY_ID: &str = "demo.local.dQw4w9WgXcQ.media_folder";
const DEMO_METADATA_TITLE: &str = "Never Gonna Give You Up";
const DEMO_METADATA_ARTIST: &str = "Rick Astley";
const DEMO_METADATA_VIDEO_ID: &str = "dQw4w9WgXcQ";
const DEMO_METADATA_SOURCE_LITERAL: &str = "local-fixture";
const DEMO_LIBRARY_ROOT: &str = "music videos";
const DEMO_MEDIA_FOLDER_TEMPLATE: &str =
    "${media.metadata.artist} - ${media.metadata.title} [${media.id}]";
const IMPORT_KIND_CAS_HASH: &str = "cas_hash";
const DEMO_MATERIALIZATION_PREFERENCE_ORDER: [MaterializationMethod; 4] = [
    MaterializationMethod::Hardlink,
    MaterializationMethod::Symlink,
    MaterializationMethod::Reflink,
    MaterializationMethod::Copy,
];

/// Manifest persisted under `examples/artifacts/demo/manifest.json`.
#[derive(Debug, Clone, PartialEq, Serialize)]
#[expect(
    clippy::struct_excessive_bools,
    reason = "demo manifest intentionally records many explicit invariant flags for regression inspection"
)]
struct DemoManifest {
    generated_unix_epoch_seconds: u64,
    artifact_root: String,
    workspace_root: String,
    media_id: String,
    auto_added_media_id: String,
    auto_added_source_title: String,
    auto_added_source_description: String,
    source_file_path: String,
    source_hash: String,
    source_has_video_track_marker: bool,
    source_has_audio_track_marker: bool,
    configured_tool_count: usize,
    configured_step_count: usize,
    tool_update_precheck_executed: bool,
    tool_update_precheck_updated_tools: usize,
    tool_update_precheck_added_tools: usize,
    materialization_preference_order: Vec<String>,
    materialized_primary_path: String,
    materialized_secondary_path: String,
    materialized_primary_exists: bool,
    materialized_secondary_exists: bool,
    materialized_primary_hardlinked_to_cas: bool,
    materialized_secondary_hardlinked_to_cas: bool,
    sync_executed: bool,
    lock_managed_files_count: usize,
    lock_managed_tools_count: usize,
    executed_instances: usize,
    cached_instances: usize,
    materialized_paths: usize,
    removed_paths: usize,
    warning_count: usize,
    profile_path: String,
    mediapm_ncl_path: String,
    conductor_user_ncl_path: String,
    conductor_generated_ncl_path: String,
    mediapm_state_json_path: String,
    library_root_path: String,
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

#[derive(Debug, Clone)]
struct DemoRunPaths {
    artifact_root: PathBuf,
    workspace_root: PathBuf,
    manifest_path: PathBuf,
    library_root: PathBuf,
}

fn artifact_root() -> PathBuf {
    if let Some(root) = std::env::var_os(example_isolation::ARTIFACT_ROOT_ENV) {
        return PathBuf::from(root);
    }
    PathBuf::from(env!("CARGO_MANIFEST_DIR")).join("examples").join("artifacts").join("demo")
}

fn unix_timestamp_seconds() -> u64 {
    SystemTime::now().duration_since(UNIX_EPOCH).map_or(0, |duration| duration.as_secs())
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
                "[demo] canonical artifact root '{}' is locked; using fallback root '{}'",
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

    let bytes = std::fs::read(output_path)?;
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

async fn summarize_store_sizes(
    cas: &FileSystemCas,
    cas_root: &Path,
) -> ExampleResult<StoreSizeStats> {
    let mut without_delta = 0u64;
    let mut with_delta = 0u64;

    for hash in collect_store_object_hashes(cas_root)? {
        let info = cas.stat(hash).await?;
        without_delta = without_delta.saturating_add(info.len);
        with_delta = with_delta.saturating_add(info.len);
    }

    Ok(StoreSizeStats { without_delta_bytes: without_delta, with_delta_bytes: with_delta })
}

fn write_local_av_fixture(path: &Path) -> ExampleResult<Vec<u8>> {
    if let Some(parent) = path.parent() {
        fs::create_dir_all(parent)?;
    }

    fs::write(path, SAMPLE_AV_MP4_BYTES)?;
    Ok(SAMPLE_AV_MP4_BYTES.to_vec())
}

fn bytes_contain_ascii(bytes: &[u8], marker: &[u8]) -> bool {
    bytes.windows(marker.len()).any(|window| window == marker)
}

async fn import_source_fixture_into_cas(
    cas: &FileSystemCas,
    source_bytes: &[u8],
) -> ExampleResult<Hash> {
    let hash = cas.put(source_bytes.to_vec().into()).await?;
    Ok(hash)
}

#[expect(
    clippy::too_many_lines,
    reason = "this item intentionally keeps end-to-end control flow together so ordering invariants remain explicit during maintenance"
)]
fn configure_document_for_local_tool_chain(
    workspace_root: &Path,
    source_hash: &str,
) -> ExampleResult<(usize, usize)> {
    let mediapm_ncl = workspace_root.join("mediapm.ncl");
    let mut document = load_mediapm_document(&mediapm_ncl)?;

    document.tools = BTreeMap::from([
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
    ]);

    let steps = vec![
        MediaStep {
            tool: MediaStepTool::Import,
            input_variants: Vec::new(),
            output_variants: BTreeMap::from([(
                "video_untagged".to_string(),
                OutputVariantValue::YtDlp(YtDlpOutputVariantConfig {
                    kind: YtDlpOutputKind::Primary,
                    ..Default::default()
                }),
            )]),
            options: BTreeMap::from([
                ("kind".to_string(), TransformInputValue::String(IMPORT_KIND_CAS_HASH.to_string())),
                ("hash".to_string(), TransformInputValue::String(source_hash.to_string())),
            ]),
        },
        MediaStep {
            tool: MediaStepTool::Ffmpeg,
            input_variants: vec!["video_untagged".to_string()],
            output_variants: BTreeMap::from([(
                "audio".to_string(),
                OutputVariantValue::Generic(GenericOutputVariantConfig {
                    kind: "primary".to_string(),
                    extension: "m4a".to_string(),
                    ..Default::default()
                }),
            )]),
            options: BTreeMap::from([
                ("vn".to_string(), TransformInputValue::String("true".to_string())),
                ("container".to_string(), TransformInputValue::String("mp4".to_string())),
            ]),
        },
        MediaStep {
            tool: MediaStepTool::Rsgain,
            input_variants: vec!["audio".to_string()],
            output_variants: BTreeMap::from([(
                "audio".to_string(),
                OutputVariantValue::Generic(GenericOutputVariantConfig {
                    kind: "output_content".to_string(),
                    extension: "m4a".to_string(),
                    ..Default::default()
                }),
            )]),
            options: BTreeMap::from([(
                "input_extension".to_string(),
                TransformInputValue::String("m4a".to_string()),
            )]),
        },
        MediaStep {
            tool: MediaStepTool::MediaTagger,
            input_variants: vec!["audio".to_string()],
            output_variants: BTreeMap::from([(
                "audio".to_string(),
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
            ]),
        },
    ];

    let configured_step_count = steps.len();

    document.media = BTreeMap::from([(
        DEMO_MEDIA_ID.to_string(),
        MediaSourceSpec {
            description: "Local demo pipeline exercising import, ffmpeg, rsgain, and media-tagger"
                .to_string(),
            title: DEMO_METADATA_TITLE.to_string(),
            artist: String::new(),
            metadata: BTreeMap::from([
                ("title".to_string(), MediaMetadataValue::Literal(DEMO_METADATA_TITLE.to_string())),
                (
                    "artist".to_string(),
                    MediaMetadataValue::Literal(DEMO_METADATA_ARTIST.to_string()),
                ),
                (
                    "video_id".to_string(),
                    MediaMetadataValue::Literal(DEMO_METADATA_VIDEO_ID.to_string()),
                ),
                (
                    "source".to_string(),
                    MediaMetadataValue::Literal(DEMO_METADATA_SOURCE_LITERAL.to_string()),
                ),
                ("video_ext".to_string(), MediaMetadataValue::Literal(".m4a".to_string())),
                ("video_ext_untagged".to_string(), MediaMetadataValue::Literal(".mp4".to_string())),
            ]),
            variant_hashes: BTreeMap::new(),
            steps,
        },
    )]);

    let media_hierarchy_children = vec![HierarchyNode {
        path: HierarchyPath::from(DEMO_MEDIA_FOLDER_TEMPLATE),
        kind: HierarchyNodeKind::Folder,
        id: Some(DEMO_MEDIA_FOLDER_HIERARCHY_ID.to_string()),
        media_id: None,
        variant: None,
        variants: Vec::new(),
        rename_files: Vec::new(),
        format: PlaylistFormat::M3u8,
        ids: Vec::new(),
        sanitize_names: SanitizeNamesConfig::Inherit,
        children: vec![
            HierarchyNode {
                path: HierarchyPath::from(
                    "${media.metadata.artist} - ${media.metadata.title} [${media.id}].untagged${media.metadata.video_ext_untagged}",
                ),
                kind: HierarchyNodeKind::Media,
                id: Some(DEMO_UNTAGGED_HIERARCHY_ID.to_string()),
                media_id: Some(DEMO_MEDIA_ID.to_string()),
                variant: Some("video_untagged".to_string()),
                variants: Vec::new(),
                rename_files: Vec::new(),
                format: PlaylistFormat::M3u8,
                ids: Vec::new(),
                sanitize_names: SanitizeNamesConfig::Inherit,
                children: Vec::new(),
            },
            HierarchyNode {
                path: HierarchyPath::from(
                    "${media.metadata.artist} - ${media.metadata.title} [${media.id}]${media.metadata.video_ext}",
                ),
                kind: HierarchyNodeKind::Media,
                id: Some(DEMO_PLAYLIST_TARGET_HIERARCHY_ID.to_string()),
                media_id: Some(DEMO_MEDIA_ID.to_string()),
                variant: Some("audio".to_string()),
                variants: Vec::new(),
                rename_files: Vec::new(),
                format: PlaylistFormat::M3u8,
                ids: Vec::new(),
                sanitize_names: SanitizeNamesConfig::Inherit,
                children: Vec::new(),
            },
        ],
    }];

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
            children: media_hierarchy_children,
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
                path: HierarchyPath::from("local-demo.m3u8"),
                kind: HierarchyNodeKind::Playlist,
                id: None,
                media_id: None,
                variant: None,
                variants: Vec::new(),
                rename_files: Vec::new(),
                format: PlaylistFormat::M3u8,
                ids: vec![
                    PlaylistItemRef::Shorthand(DEMO_PLAYLIST_TARGET_HIERARCHY_ID.to_string()),
                    PlaylistItemRef::Shorthand(DEMO_PLAYLIST_TARGET_HIERARCHY_ID.to_string()),
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
        // Ordered file-materialization method preference.
        // Default when omitted: hardlink -> symlink -> reflink -> copy.
        materialization_preference_order: DEMO_MATERIALIZATION_PREFERENCE_ORDER.to_vec(),
        // User-owned conductor config path relative to workspace root.
        // Default: `mediapm.conductor.ncl`.
        conductor_config: Some("mediapm.conductor.ncl".to_string()),
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
        // Machine-generated runtime dotenv file (written by tooling, not users).
        // Default: `.mediapm/.env.generated`.
        env_generated_file: Some(".mediapm/.env.generated".to_string()),
        // Embedded schema export directory policy.
        // `Some(Some(path))` keeps export enabled with an explicit default path.
        mediapm_schema_dir: Some(Some(".mediapm/config/mediapm".to_string())),
        // Enable conductor profiling so every sync run produces a per-step
        // timing profile at `.mediapm/profile.json` for latency investigation.
        profiler_enabled: true,
        // All other fields use their respective defaults.
        ..Default::default()
    };

    save_mediapm_document(&mediapm_ncl, &document)?;
    Ok((document.tools.len(), configured_step_count))
}

fn local_demo_tool_requirements() -> BTreeMap<String, ToolRequirement> {
    BTreeMap::from([
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
    ])
}

fn configure_document_for_tools_only_precheck(workspace_root: &Path) -> ExampleResult<usize> {
    let mediapm_ncl = workspace_root.join("mediapm.ncl");
    let mut document = load_mediapm_document(&mediapm_ncl)?;
    document.tools = local_demo_tool_requirements();
    document.media.clear();
    document.hierarchy.clear();
    save_mediapm_document(&mediapm_ncl, &document)?;
    Ok(document.tools.len())
}

fn seed_old_synced_tools_state_for_update_precheck(
    service: &MediaPmService<mediapm_cas::FileSystemCas>,
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

    for logical_tool_name in local_demo_tool_requirements().into_keys() {
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
/// depend on live API responses during hermetic demo test runs. Callers must
/// gate this behind [`example_isolation::uses_isolated_cache_root`]; the
/// hardcoded entries must never be written into the real user-level cache.
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
            "2025.07.15",
            "a1b2c3d4e5f6a7b8c9d0e1f2a3b4c5d6e7f8a9b0",
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
    service: &mut MediaPmService<mediapm_cas::FileSystemCas>,
    workspace_root: &Path,
) -> ExampleResult<(usize, usize)> {
    // Seed hardcoded metadata only for hermetic test runs; explicit runs
    // (env unset) use the real user-level cache and fetch live metadata.
    if example_isolation::uses_isolated_cache_root() {
        let cache_root = example_cache_root();
        seed_tool_metadata_cache_for_demo_precheck(&cache_root).await?;
    }

    let expected_updated_tools = configure_document_for_tools_only_precheck(workspace_root)?;
    seed_old_synced_tools_state_for_update_precheck(service)?;

    let document = load_mediapm_document(&workspace_root.join("mediapm.ncl"))?;
    if !document.media.is_empty() || !document.hierarchy.is_empty() {
        return Err("tools-update precheck must start with empty media/hierarchy".into());
    }

    let summary = service.sync_tools_with_tag_update_checks(false, false).await?;
    if summary.updated_tools != expected_updated_tools {
        return Err(format!(
            "tools-update precheck expected {expected_updated_tools} updated tools but observed {} (added={}, skipped warnings={:?})",
            summary.updated_tools,
            summary.added_tools,
            summary.warnings,
        )
        .into());
    }

    Ok((summary.updated_tools, summary.added_tools))
}

fn clear_machine_workflows(machine_path: &Path) -> ExampleResult<()> {
    let mut machine: NickelDocument = decode_document(fs::read(machine_path)?.as_slice())?;
    machine.workflows.clear();
    fs::write(machine_path, encode_document(machine)?)?;
    Ok(())
}

#[expect(
    clippy::too_many_lines,
    reason = "this demo keeps end-to-end orchestration and manifest wiring in one place for maintainability"
)]
async fn generate_demo_artifacts(run_sync: bool) -> ExampleResult<DemoRunPaths> {
    let (root, _fallback_artifact) = reset_artifact_root()?;
    let workspace_root = root.clone();

    let source_path = workspace_root.join("input").join("sample-av.mp4");
    let source_bytes = write_local_av_fixture(&source_path)?;
    let source_has_video_track_marker = bytes_contain_ascii(&source_bytes, b"vide");
    let source_has_audio_track_marker = bytes_contain_ascii(&source_bytes, b"soun");

    let mut ingest_service = MediaPmService::new_fs_at_with_runtime_storage_overrides(
        &workspace_root,
        example_runtime_storage(),
    )
    .await?;
    let paths = ingest_service.paths().clone();

    let (precheck_updated_tools, precheck_added_tools) = if run_sync {
        run_tools_update_precheck(&mut ingest_service, &workspace_root).await?
    } else {
        (0, 0)
    };

    let source_hash_text = {
        // Clone the service's CAS handle instead of reopening the same root:
        // `FileSystemCas::open` would re-acquire the directory lock and hit
        // `LockContention` because the service already holds it. Cloning the
        // `Arc<FileSystemCas>` shares the same `DirectoryLockGuard`.
        let cas = ingest_service.conductor().cas().clone();
        let source_hash = import_source_fixture_into_cas(&cas, &source_bytes).await?;
        source_hash.to_string()
    };

    let auto_added_media_id =
        ingest_service.add_local_source(&source_path, "ffprobe", None, AddInsertPosition::End)?;
    let auto_added_document = load_mediapm_document(&paths.mediapm_ncl)?;
    let auto_added_source = auto_added_document.media.get(&auto_added_media_id).ok_or_else(|| {
        std::io::Error::other(format!(
            "demo preflight add_local_source media '{auto_added_media_id}' missing from mediapm.ncl"
        ))
    })?;
    let auto_added_source_title = auto_added_source.title.clone();
    if auto_added_source_title.trim().is_empty() {
        return Err(
            std::io::Error::other("demo preflight add_local_source produced empty title").into()
        );
    }
    let auto_added_source_description = auto_added_source.description.clone();
    if auto_added_source_description.trim().is_empty() {
        return Err(std::io::Error::other(
            "demo preflight add_local_source produced empty description",
        )
        .into());
    }

    let (configured_tool_count, configured_step_count) =
        configure_document_for_local_tool_chain(&workspace_root, &source_hash_text)?;

    ingest_service.refresh_runtime_configuration()?;
    if run_sync {
        clear_machine_workflows(&ingest_service.paths().conductor_generated_ncl)?;
    }

    let maybe_summary =
        if run_sync { Some(ingest_service.sync_library(false).await?) } else { None };
    if let Some(summary) = &maybe_summary
        && summary.warnings.iter().any(|w| w.contains("failed step"))
    {
        return Err(format!("sync_library workflow warnings: {:?}", summary.warnings).into());
    }
    let service = ingest_service;
    let effective_paths = service
        .resolve_effective_paths()
        .map_err(|e| std::io::Error::other(format!("resolve effective paths: {e}")))?;
    let cas_root = effective_paths.runtime_root.join("store");
    let store_size_stats = summarize_store_sizes(service.conductor().cas(), &cas_root).await?;
    let materialization_preference_order = DEMO_MATERIALIZATION_PREFERENCE_ORDER
        .iter()
        .map(MaterializationMethod::as_label)
        .map(str::to_owned)
        .collect::<Vec<_>>();

    let materialized_primary = effective_paths
        .hierarchy_root_dir
        .join("music videos")
        .join(format!("{DEMO_METADATA_ARTIST} - {DEMO_METADATA_TITLE} [{DEMO_MEDIA_ID}]"))
        .join(format!("{DEMO_METADATA_ARTIST} - {DEMO_METADATA_TITLE} [{DEMO_MEDIA_ID}].m4a"));
    let materialized_secondary = effective_paths
        .hierarchy_root_dir
        .join("music videos")
        .join(format!("{DEMO_METADATA_ARTIST} - {DEMO_METADATA_TITLE} [{DEMO_MEDIA_ID}]"))
        .join(format!(
            "{DEMO_METADATA_ARTIST} - {DEMO_METADATA_TITLE} [{DEMO_MEDIA_ID}].untagged.mp4"
        ));

    let lock = load_mediapm_state_document(&effective_paths.mediapm_state_json)?;
    let (materialized_primary_hardlinked_to_cas, materialized_secondary_hardlinked_to_cas) =
        if maybe_summary.is_some() {
            let hierarchy_root = &effective_paths.hierarchy_root_dir;
            let cas = service.conductor().cas();
            assert_materialized_output_hardlinked_to_cas(
                cas,
                hierarchy_root,
                &lock,
                &materialized_primary,
            )
            .await?;
            assert_materialized_output_hardlinked_to_cas(
                cas,
                hierarchy_root,
                &lock,
                &materialized_secondary,
            )
            .await?;
            (true, true)
        } else {
            (false, false)
        };

    let manifest = DemoManifest {
        generated_unix_epoch_seconds: unix_timestamp_seconds(),
        artifact_root: display_path(&root),
        workspace_root: display_path(&workspace_root),
        media_id: DEMO_MEDIA_ID.to_string(),
        auto_added_media_id,
        auto_added_source_title,
        auto_added_source_description,
        source_file_path: display_path(&source_path),
        source_hash: source_hash_text,
        source_has_video_track_marker,
        source_has_audio_track_marker,
        configured_tool_count,
        configured_step_count,
        tool_update_precheck_executed: run_sync,
        tool_update_precheck_updated_tools: precheck_updated_tools,
        tool_update_precheck_added_tools: precheck_added_tools,

        materialization_preference_order,
        materialized_primary_path: display_path(&materialized_primary),
        materialized_secondary_path: display_path(&materialized_secondary),
        materialized_primary_exists: materialized_primary.exists(),
        materialized_secondary_exists: materialized_secondary.exists(),
        materialized_primary_hardlinked_to_cas,
        materialized_secondary_hardlinked_to_cas,
        sync_executed: maybe_summary.is_some(),
        lock_managed_files_count: lock.managed_files.len(),
        lock_managed_tools_count: lock.managed_tools.len(),
        executed_instances: maybe_summary.as_ref().map_or(0, |summary| summary.executed_instances),
        cached_instances: maybe_summary.as_ref().map_or(0, |summary| summary.cached_instances),
        materialized_paths: maybe_summary.as_ref().map_or(0, |summary| summary.materialized_paths),
        removed_paths: maybe_summary.as_ref().map_or(0, |summary| summary.removed_paths),
        warning_count: maybe_summary.as_ref().map_or(0, |summary| summary.warnings.len()),
        profile_path: display_path(&service.paths().runtime_root.join("profile.json")),
        mediapm_ncl_path: display_path(&service.paths().mediapm_ncl),
        conductor_user_ncl_path: display_path(&service.paths().conductor_user_ncl),
        conductor_generated_ncl_path: display_path(&service.paths().conductor_generated_ncl),
        mediapm_state_json_path: display_path(&service.paths().mediapm_state_json),
        library_root_path: display_path(&effective_paths.hierarchy_root_dir),
        store_size_without_delta_bytes: store_size_stats.without_delta_bytes,
        store_size_with_delta_bytes: store_size_stats.with_delta_bytes,
        store_size_ratio_with_delta_over_without: store_size_stats.ratio_with_delta_over_without(),
    };

    let manifest_path = root.join("manifest.json");
    write_json_file(&manifest_path, &manifest)?;

    Ok(DemoRunPaths {
        artifact_root: root,
        workspace_root,
        manifest_path,
        library_root: service.paths().hierarchy_root_dir.clone(),
    })
}

#[tokio::main]
async fn main() -> ExampleResult<()> {
    let paths = generate_demo_artifacts(true).await?;
    println!("generated artifacts root: {}", paths.artifact_root.display());
    println!("generated workspace root: {}", paths.workspace_root.display());
    println!("generated library root: {}", paths.library_root.display());
    println!("manifest: {}", paths.manifest_path.display());
    println!("sync executed: true");
    Ok(())
}

#[cfg(test)]
mod tests {
    use mediapm::example_isolation::{self, IsolatedExampleRoots};

    /// Executes the documented example entry point via `main()` in full-sync mode.
    #[test]
    fn main_is_exercised() {
        let _isolated = IsolatedExampleRoots::with_cache();

        super::main().expect("example main should run to completion");
    }

    /// Verifies demo artifact generation writes one complete import workflow manifest in full-sync mode.
    #[tokio::test]
    async fn generate_demo_artifacts_writes_manifest_and_import_metadata() {
        let _isolated = IsolatedExampleRoots::with_cache();
        let _tracing = tracing_subscriber::fmt()
            .with_max_level(tracing::Level::ERROR)
            .with_test_writer()
            .try_init();
        let run = super::generate_demo_artifacts(true).await.expect("demo artifact generation");

        assert!(run.manifest_path.exists(), "manifest should be written");
        assert!(run.workspace_root.exists(), "workspace root should exist");

        let manifest_text = std::fs::read_to_string(&run.manifest_path).expect("read manifest");
        let manifest_json: serde_json::Value =
            serde_json::from_str(&manifest_text).expect("manifest JSON");

        assert_eq!(
            manifest_json.get("configured_tool_count").and_then(serde_json::Value::as_u64),
            Some(5),
            "demo should configure five managed tools including import"
        );
        assert_eq!(
            manifest_json.get("configured_step_count").and_then(serde_json::Value::as_u64),
            Some(4),
            "demo should configure four workflow steps including import"
        );
        assert_eq!(
            manifest_json.get("tool_update_precheck_executed").and_then(serde_json::Value::as_bool),
            Some(true),
            "full-sync demo run should execute tools-update precheck"
        );
        let without_delta = manifest_json
            .get("store_size_without_delta_bytes")
            .and_then(serde_json::Value::as_u64)
            .expect("manifest should include store_size_without_delta_bytes");
        let with_delta = manifest_json
            .get("store_size_with_delta_bytes")
            .and_then(serde_json::Value::as_u64)
            .expect("manifest should include store_size_with_delta_bytes");
        let ratio = manifest_json
            .get("store_size_ratio_with_delta_over_without")
            .and_then(serde_json::Value::as_f64)
            .expect("manifest should include store_size_ratio_with_delta_over_without");
        let expected_ratio =
            if without_delta == 0 { 1.0 } else { with_delta as f64 / without_delta as f64 };
        assert!(
            (ratio - expected_ratio).abs() <= f64::EPSILON,
            "manifest ratio should match with/without store-size math"
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
        assert_eq!(
            manifest_json
                .get("materialization_preference_order")
                .and_then(serde_json::Value::as_array)
                .map(|value| {
                    value.iter().filter_map(serde_json::Value::as_str).collect::<Vec<_>>()
                }),
            Some(vec!["hardlink", "symlink", "reflink", "copy"]),
            "manifest should expose explicit default materialization order"
        );
        assert!(
            manifest_json
                .get("auto_added_source_title")
                .and_then(serde_json::Value::as_str)
                .is_some_and(|value| !value.trim().is_empty()),
            "manifest should record non-empty auto-populated title"
        );
        assert!(
            manifest_json
                .get("auto_added_source_description")
                .and_then(serde_json::Value::as_str)
                .is_some_and(|value| !value.trim().is_empty()),
            "manifest should record non-empty auto-populated description"
        );
        assert_eq!(
            manifest_json.get("sync_executed").and_then(serde_json::Value::as_bool),
            Some(true),
            "full-sync demo run should execute sync_library"
        );
        assert_eq!(
            manifest_json.get("materialized_primary_exists").and_then(serde_json::Value::as_bool),
            Some(true),
            "full-sync demo should materialize primary output"
        );
        assert_eq!(
            manifest_json.get("materialized_secondary_exists").and_then(serde_json::Value::as_bool),
            Some(true),
            "full-sync demo should materialize secondary untagged output"
        );
        assert_eq!(
            manifest_json
                .get("materialized_primary_hardlinked_to_cas")
                .and_then(serde_json::Value::as_bool),
            Some(true),
            "primary output should be hardlinked to CAS after ensure"
        );
        assert_eq!(
            manifest_json
                .get("materialized_secondary_hardlinked_to_cas")
                .and_then(serde_json::Value::as_bool),
            Some(true),
            "secondary output should be hardlinked to CAS after ensure"
        );
        assert!(
            manifest_json
                .get("lock_managed_files_count")
                .and_then(serde_json::Value::as_u64)
                .is_some_and(|count| count > 0),
            "full-sync demo should populate managed_files lockfile entries"
        );
        assert!(
            manifest_json
                .get("executed_instances")
                .and_then(serde_json::Value::as_u64)
                .is_some_and(|count| count > 0),
            "full-sync demo should execute managed workflow steps"
        );

        let demo_config_path = run.workspace_root.join("mediapm.ncl");
        let demo_config =
            std::fs::read_to_string(&demo_config_path).expect("read generated mediapm.ncl");

        assert!(
            demo_config.contains("tool = \"import\""),
            "demo should ingest source data via import"
        );
        assert!(
            !demo_config.contains("tool = \"yt-dlp\""),
            "demo should not route local sample ingest through yt-dlp"
        );
        assert!(
            !demo_config.contains("codec_copy = \"true\"")
                && !demo_config.contains("map_metadata = \"0\""),
            "demo should omit explicit ffmpeg defaults and rely on managed codec_copy/map_metadata defaults"
        );
        assert!(
            demo_config.contains("materialization_preference_order")
                && demo_config.contains("\"hardlink\"")
                && demo_config.contains("\"symlink\"")
                && demo_config.contains("\"reflink\"")
                && demo_config.contains("\"copy\""),
            "demo runtime config should explicitly include default materialization order"
        );
        assert!(
            !demo_config.contains("audio_codec = \"libmp3lame\""),
            "demo ffmpeg step should avoid re-encode-specific codec settings"
        );
        assert!(
            !demo_config.contains("strict_identification"),
            "demo should rely on managed media-tagger input defaults for strict identification"
        );
        assert!(
            demo_config.contains("music videos")
                && demo_config
                    .contains("${media.metadata.artist} - ${media.metadata.title} [${media.id}]")
                && demo_config.contains("${media.id}")
                && demo_config.contains("${media.metadata.video_ext}")
                && demo_config.contains("${media.metadata.video_ext_untagged}"),
            "demo hierarchy output should use Jellyfin-style media-id layout with metadata-driven extensions"
        );
    }

    /// Ensures artifact root stays stable for docs and scripts.
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
        assert!(text.ends_with("src/mediapm/examples/artifacts/demo"));

        // SAFETY: restore previous env var value for test isolation.
        unsafe {
            match &previous_artifact_root {
                Some(value) => std::env::set_var(example_isolation::ARTIFACT_ROOT_ENV, value),
                None => std::env::remove_var(example_isolation::ARTIFACT_ROOT_ENV),
            }
        }
    }

    /// Ensures cleanup retries can remove readonly-marked demo artifact trees
    /// created by prior sync runs on Windows hosts.
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

        example_isolation::remove_dir_all_with_retry(&tree_root)
            .expect("retrying remove should succeed");
        assert!(!tree_root.exists());
    }

    /// Ensures ratio rendering stays neutral for empty/objectless stores.
    #[test]
    fn store_size_ratio_uses_neutral_value_for_zero_denominator() {
        let stats = super::StoreSizeStats { without_delta_bytes: 0, with_delta_bytes: 0 };
        assert_eq!(stats.ratio_with_delta_over_without(), 1.0);
    }
}
