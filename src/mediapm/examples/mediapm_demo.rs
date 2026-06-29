//! Persistent `mediapm` demo producing inspectable artifacts.
//!
//! Demonstrates local ingest + transform flow: bundled MP4 fixture → CAS
//! → `import -> ffmpeg -> rsgain -> media-tagger` pipeline.
//! Default sync enabled; override via `MEDIAPM_DEMO_RUN_SYNC`.

use std::collections::{BTreeMap, BTreeSet};
use std::error::Error;
use std::fs;
use std::path::{Path, PathBuf};
use std::process;
use std::str::FromStr;
use std::thread;
use std::time::{Duration, SystemTime, UNIX_EPOCH};

use mediapm::{
    ActiveToolInstance, AddInsertPosition, HierarchyNode, HierarchyNodeKind, HierarchyPath,
    MaterializationMethod, MediaMetadataValue, MediaPmPaths, MediaPmService, MediaRuntimeStorage,
    MediaSourceSpec, MediaStep, MediaStepTool, PlaylistFormat, PlaylistItemRef,
    SanitizeNamesConfig, ToolRegistryEntry, ToolRequirement, ToolRequirementDependencies,
    TransformInputValue, load_mediapm_document, load_mediapm_state_document, save_mediapm_document,
    save_mediapm_state_document,
};
use mediapm_cas::{CasApi, FileSystemCas, Hash};
use mediapm_conductor::{
    NickelDocument, RuntimeStoragePaths, SimpleConductor, ToolKindSpec, ToolRuntime, ToolSpec,
    decode_document, default_runtime_inherited_env_vars, encode_document,
};
use same_file::is_same_file;
use serde::Serialize;
use serde_json::json;

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
const DEMO_RUN_SYNC_ENV_VAR: &str = "MEDIAPM_DEMO_RUN_SYNC";
const DEMO_MATERIALIZATION_PREFERENCE_ORDER: [MaterializationMethod; 4] = [
    MaterializationMethod::Hardlink,
    MaterializationMethod::Symlink,
    MaterializationMethod::Reflink,
    MaterializationMethod::Copy,
];

/// Manifest persisted under `examples/.artifacts/demo/manifest.json`.
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
    lock_tool_registry_count: usize,
    executed_instances: usize,
    cached_instances: usize,
    rematerialized_instances: usize,
    materialized_paths: usize,
    removed_paths: usize,
    warning_count: usize,
    profile_path: String,
    mediapm_ncl_path: String,
    conductor_user_ncl_path: String,
    conductor_machine_ncl_path: String,
    mediapm_state_ncl_path: String,
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

#[derive(Debug, Clone)]
struct DemoRunPaths {
    artifact_root: PathBuf,
    workspace_root: PathBuf,
    manifest_path: PathBuf,
    library_root: PathBuf,
}

fn artifact_root() -> PathBuf {
    PathBuf::from(env!("CARGO_MANIFEST_DIR")).join("examples").join(".artifacts").join("demo")
}

fn unix_timestamp_seconds() -> u64 {
    SystemTime::now().duration_since(UNIX_EPOCH).map_or(0, |duration| duration.as_secs())
}

fn display_path(path: &Path) -> String {
    path.display().to_string().replace('\\', "/")
}

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

fn is_share_violation_remove_error(error: &(dyn Error + 'static)) -> bool {
    error.downcast_ref::<std::io::Error>().is_some_and(|io_error| {
        io_error.kind() == std::io::ErrorKind::PermissionDenied
            || io_error.raw_os_error() == Some(32)
    })
}

fn prepare_fallback_artifact_root(canonical_root: &Path) -> ExampleResult<PathBuf> {
    let suffix =
        SystemTime::now().duration_since(UNIX_EPOCH).map_or(0, |duration| duration.as_nanos());
    let fallback_root =
        canonical_root.with_file_name(format!("demo-fallback-{}-{suffix}", process::id()));

    fs::create_dir_all(&fallback_root)?;
    eprintln!(
        "[demo] canonical artifact root '{}' is locked; using fallback root '{}'",
        canonical_root.display(),
        fallback_root.display()
    );
    Ok(fallback_root)
}

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
                clear_readonly_bits_recursively(path);
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

#[cfg_attr(
    windows,
    expect(
        clippy::permissions_set_readonly_false,
        reason = "Windows demo cleanup retries must clear readonly flags on managed artifacts before recursive removal"
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

/// Resolves canonical CAS object file path for one content hash.
fn cas_object_path_for_hash(cas_root: &Path, hash: Hash) -> PathBuf {
    let hex = hash.to_hex();
    cas_root.join("v1").join("blake3").join(&hex[..2]).join(&hex[2..4]).join(&hex[4..])
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

fn assert_materialized_output_hardlinked_to_cas(
    cas_root: &Path,
    hierarchy_root: &Path,
    lock: &mediapm::MediaPmState,
    output_path: &Path,
) -> ExampleResult<bool> {
    let relative_path = managed_relative_path(hierarchy_root, output_path)?;
    if !lock.managed_files.contains(relative_path.as_str()) {
        return Err(std::io::Error::other(format!(
            "managed output '{relative_path}' missing from lockfile tracking"
        ))
        .into());
    }

    let bytes = std::fs::read(output_path)?;
    let hash = Hash::from_content(&bytes);

    if !output_is_hardlinked_to_cas_object(cas_root, hash, output_path)? {
        return Err(std::io::Error::other(format!(
            "materialized output '{}' is not hardlinked to CAS object '{}'",
            output_path.display(),
            hash
        ))
        .into());
    }

    Ok(true)
}

async fn summarize_store_sizes(cas_root: &Path) -> ExampleResult<StoreSizeStats> {
    let cas = FileSystemCas::open(cas_root).await?;
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

fn sync_enabled_from_env_value(value: Option<&str>) -> bool {
    let Some(raw) = value else {
        return true;
    };

    let normalized = raw.trim().to_ascii_lowercase();
    !matches!(normalized.as_str(), "0" | "false" | "no" | "off")
}

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

fn demo_run_sync_enabled() -> bool {
    // CI mode always disables sync (config-only)
    if ci_mode_detected() {
        return false;
    }
    sync_enabled_from_env_value(std::env::var(DEMO_RUN_SYNC_ENV_VAR).ok().as_deref())
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
                version: None,
                tag: None,
                dependencies: ToolRequirementDependencies::default(),
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
                    ffmpeg_version: Some(MediaMetadataValue::Literal("inherit".to_string())),
                    deno_version: None,
                    sd_version: Some(MediaMetadataValue::Literal("inherit".to_string())),
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
                    ffmpeg_version: Some(MediaMetadataValue::Literal("inherit".to_string())),
                    deno_version: None,
                    sd_version: None,
                },
                recheck_seconds: None,
                max_input_slots: None,
                max_output_slots: None,
            },
        ),
    ]);

    let steps = vec![
        MediaStep {
            tool: MediaStepTool::Import,
            input_variants: Vec::new(),
            output_variants: BTreeMap::from([(
                "video_untagged".to_string(),
                json!({ "kind": "primary" }),
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
                json!({ "kind": "primary", "idx": 0, "extension": "m4a" }),
            )]),
            options: BTreeMap::from([(
                "vn".to_string(),
                TransformInputValue::String("true".to_string()),
            )]),
        },
        MediaStep {
            tool: MediaStepTool::MediaTagger,
            input_variants: vec!["audio".to_string()],
            output_variants: BTreeMap::from([("audio".to_string(), json!({ "kind": "primary" }))]),
            options: BTreeMap::from([
                (
                    "recording_mbid".to_string(),
                    TransformInputValue::String("8f3471b5-7e6a-48da-86a9-c1c07a0f47ae".to_string()),
                ),
                ("release_mbid".to_string(), TransformInputValue::String(String::new())),
                ("write_all_images".to_string(), TransformInputValue::String("false".to_string())),
            ]),
        },
        MediaStep {
            tool: MediaStepTool::Rsgain,
            input_variants: vec!["audio".to_string()],
            output_variants: BTreeMap::from([("audio".to_string(), json!({ "kind": "primary" }))]),
            options: BTreeMap::from([(
                "input_extension".to_string(),
                TransformInputValue::String("m4a".to_string()),
            )]),
        },
    ];

    let configured_step_count = steps.len();

    document.media = BTreeMap::from([(
        DEMO_MEDIA_ID.to_string(),
        MediaSourceSpec {
            id: None,
            description: Some(
                "Local demo pipeline exercising import, ffmpeg, rsgain, and media-tagger"
                    .to_string(),
            ),
            title: Some(DEMO_METADATA_TITLE.to_string()),
            artist: None,
            workflow_id: None,
            metadata: Some(BTreeMap::from([
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
            ])),
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
        // Default: `.mediapm/state.conductor.ncl`.
        conductor_state_config: Some(".mediapm/state.conductor.ncl".to_string()),
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
        // Default: `.mediapm/state.ncl`.
        media_state_config: Some(".mediapm/state.ncl".to_string()),
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
                version: None,
                tag: None,
                dependencies: ToolRequirementDependencies::default(),
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
                    ffmpeg_version: Some(MediaMetadataValue::Literal("inherit".to_string())),
                    deno_version: None,
                    sd_version: Some(MediaMetadataValue::Literal("inherit".to_string())),
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
                    ffmpeg_version: Some(MediaMetadataValue::Literal("inherit".to_string())),
                    deno_version: None,
                    sd_version: None,
                },
                recheck_seconds: None,
                max_input_slots: None,
                max_output_slots: None,
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
    Ok(document.tools.keys().filter(|name| !name.eq_ignore_ascii_case("import")).count())
}

fn seed_old_synced_tools_state_for_update_precheck(
    service: &MediaPmService<mediapm_cas::FileSystemCas>,
) -> ExampleResult<()> {
    service.refresh_runtime_configuration()?;

    let mut machine: NickelDocument =
        decode_document(fs::read(&service.paths().conductor_machine_ncl)?.as_slice())?;
    let mut lock = load_mediapm_state_document(&service.paths().mediapm_state_ncl)?;

    for logical_tool_name in local_demo_tool_requirements().into_keys() {
        if logical_tool_name.eq_ignore_ascii_case("import") {
            continue;
        }

        let stale_tool_id =
            format!("mediapm.tools.{}+demo@old", logical_tool_name.trim().to_ascii_lowercase());
        let stale_payload = format!("stale-tool-payload::{logical_tool_name}");
        let stale_hash = Hash::from_content(stale_payload.as_bytes());
        let stale_relative_path = format!("legacy/{logical_tool_name}/tool.bin");

        machine.tools.insert(
            stale_tool_id.clone(),
            ToolSpec {
                name: stale_tool_id.clone(),
                version: "old".to_string(),
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

        lock.active_tools.insert(
            logical_tool_name.clone(),
            ActiveToolInstance {
                tool_id: stale_tool_id.clone(),
                content_hash: stale_hash.to_string(),
                deployed_path: stale_relative_path.clone(),
            },
        );
        lock.tool_registry.insert(
            stale_tool_id,
            ToolRegistryEntry {
                version: Some("old".to_string()),
                tag: None,
                fetch_hash: Some(stale_hash.to_string()),
                deployed_at: Some(unix_timestamp_seconds()),
            },
        );
    }

    fs::write(&service.paths().conductor_machine_ncl, encode_document(machine)?)?;
    save_mediapm_state_document(&service.paths().mediapm_state_ncl, &lock)?;

    Ok(())
}

async fn run_tools_update_precheck(
    service: &mut MediaPmService<mediapm_cas::FileSystemCas>,
    workspace_root: &Path,
) -> ExampleResult<(usize, usize)> {
    let expected_updated_tools = configure_document_for_tools_only_precheck(workspace_root)?;
    seed_old_synced_tools_state_for_update_precheck(service)?;

    let document = load_mediapm_document(&workspace_root.join("mediapm.ncl"))?;
    if !document.media.is_empty() || !document.hierarchy.is_empty() {
        return Err("tools-update precheck must start with empty media/hierarchy".into());
    }

    let summary = service.sync_tools_with_tag_update_checks(false).await?;
    if summary.updated_tools != expected_updated_tools {
        return Err(format!(
            "tools-update precheck expected {expected_updated_tools} updated tools but observed {}",
            summary.updated_tools
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
    let root = reset_artifact_root()?;
    let workspace_root = root.clone();

    let source_path = workspace_root.join("input").join("sample-av.mp4");
    let source_bytes = write_local_av_fixture(&source_path)?;
    let source_has_video_track_marker = bytes_contain_ascii(&source_bytes, b"vide");
    let source_has_audio_track_marker = bytes_contain_ascii(&source_bytes, b"soun");

    let mut ingest_service = MediaPmService::new_fs_at(&workspace_root).await?;
    let paths = ingest_service.paths().clone();

    let (precheck_updated_tools, precheck_added_tools) = if run_sync {
        run_tools_update_precheck(&mut ingest_service, &workspace_root).await?
    } else {
        (0, 0)
    };

    let source_hash_text = {
        let cas = FileSystemCas::open(&paths.runtime_root.join("store")).await?;
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
    let auto_added_source_title =
        auto_added_source.title.clone().filter(|value| !value.trim().is_empty()).ok_or_else(
            || std::io::Error::other("demo preflight add_local_source produced empty title"),
        )?;
    let auto_added_source_description =
        auto_added_source.description.clone().filter(|value| !value.trim().is_empty()).ok_or_else(
            || std::io::Error::other("demo preflight add_local_source produced empty description"),
        )?;

    let (configured_tool_count, configured_step_count) =
        configure_document_for_local_tool_chain(&workspace_root, &source_hash_text)?;

    let mut service = {
        let store_root = workspace_root.join(".mediapm").join("store");
        let file_system_cas = FileSystemCas::open(&store_root).await?;
        let conductor = SimpleConductor::new(
            RuntimeStoragePaths::new(&workspace_root.join(".mediapm")),
            file_system_cas,
        );
        MediaPmService::new(conductor, MediaPmPaths::from_root(&workspace_root))
    };
    if run_sync {
        clear_machine_workflows(&service.paths().conductor_machine_ncl)?;
    }

    let maybe_summary = if run_sync { Some(service.sync_library(false).await?) } else { None };
    let effective_paths = service.paths().clone();
    let cas_root = service.paths().runtime_root.join("store");
    let store_size_stats = summarize_store_sizes(&cas_root).await?;
    let materialization_preference_order = DEMO_MATERIALIZATION_PREFERENCE_ORDER
        .iter()
        .map(|method| format!("{method:?}"))
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

    let lock = load_mediapm_state_document(&service.paths().mediapm_state_ncl)?;
    let (materialized_primary_hardlinked_to_cas, materialized_secondary_hardlinked_to_cas) =
        if maybe_summary.is_some() {
            let hierarchy_root = &effective_paths.hierarchy_root_dir;
            (
                assert_materialized_output_hardlinked_to_cas(
                    &cas_root,
                    hierarchy_root,
                    &lock,
                    &materialized_primary,
                )?,
                assert_materialized_output_hardlinked_to_cas(
                    &cas_root,
                    hierarchy_root,
                    &lock,
                    &materialized_secondary,
                )?,
            )
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
        lock_tool_registry_count: lock.tool_registry.len(),
        executed_instances: maybe_summary.as_ref().map_or(0, |summary| summary.executed_instances),
        cached_instances: maybe_summary.as_ref().map_or(0, |summary| summary.cached_instances),
        rematerialized_instances: maybe_summary
            .as_ref()
            .map_or(0, |summary| summary.rematerialized_instances),
        materialized_paths: maybe_summary.as_ref().map_or(0, |summary| summary.materialized_paths),
        removed_paths: maybe_summary.as_ref().map_or(0, |summary| summary.removed_paths),
        warning_count: maybe_summary.as_ref().map_or(0, |summary| summary.warnings.len()),
        profile_path: display_path(&service.paths().runtime_root.join("profile.json")),
        mediapm_ncl_path: display_path(&service.paths().mediapm_ncl),
        conductor_user_ncl_path: display_path(&service.paths().conductor_user_ncl),
        conductor_machine_ncl_path: display_path(&service.paths().conductor_machine_ncl),
        mediapm_state_ncl_path: display_path(&service.paths().mediapm_state_ncl),
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
    let run_sync = demo_run_sync_enabled();

    if ci_mode_detected() {
        eprintln!(
            "[mediapm_demo] CI environment detected; running in configuration-only mode (no sync)"
        );
    }

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
    /// Verifies demo artifact generation writes one complete import
    /// workflow manifest when runtime sync is intentionally skipped.
    #[tokio::test]
    async fn generate_demo_artifacts_writes_manifest_and_import_metadata() {
        let run = super::generate_demo_artifacts(false).await.expect("demo artifact generation");

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
            Some(false),
            "config-only demo run should not execute tools-update precheck"
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

    /// Ensures unset sync mode defaults to full sync enabled.
    #[test]
    fn demo_run_sync_defaults_to_enabled_when_env_unset() {
        let previous = std::env::var(super::DEMO_RUN_SYNC_ENV_VAR).ok();
        // SAFETY: test mutates one process env key in a controlled scope and
        // restores the previous value before exit.
        unsafe {
            std::env::remove_var(super::DEMO_RUN_SYNC_ENV_VAR);
        }

        let enabled = super::demo_run_sync_enabled();

        // SAFETY: restore previous env var value for test isolation.
        unsafe {
            if let Some(value) = previous {
                std::env::set_var(super::DEMO_RUN_SYNC_ENV_VAR, value);
            }
        }

        assert!(enabled, "demo runs should default to sync enabled when env override is unset");
    }

    /// Ensures cleanup retries can remove readonly-marked demo artifact trees
    /// created by prior sync runs on Windows hosts.
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

    /// Ensures ratio rendering stays neutral for empty/objectless stores.
    #[test]
    fn store_size_ratio_uses_neutral_value_for_zero_denominator() {
        let stats = super::StoreSizeStats { without_delta_bytes: 0, with_delta_bytes: 0 };
        assert_eq!(stats.ratio_with_delta_over_without(), 1.0);
    }
}
