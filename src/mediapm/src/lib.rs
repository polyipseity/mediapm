//! Phase 3 `mediapm` media orchestration facade.
//!
//! This crate composes:
//! - Phase 1 CAS for content identity/storage,
//! - Phase 2 Conductor for declarative workflow execution,
//! - Phase 3 policy/lock/materialization logic specialized for media libraries.
//!
//! Phase 3 state contract:
//! - desired state: `mediapm.ncl`,
//! - conductor runtime docs: `mediapm.conductor.ncl`,
//!   `mediapm.conductor.machine.ncl` (both configurable),
//! - realized state: `.mediapm/state.ncl` by default (configurable).

pub mod builtins;
mod conductor_bridge;
mod config;
mod error;
mod global;
mod http_client;
mod lockfile;
mod materializer;
mod paths;
mod tools;

use std::collections::BTreeMap;
use std::fs;
use std::future::Future;
use std::path::{Path, PathBuf};
use std::process::Command as ProcessCommand;

use mediapm_cas::{CasApi, FileSystemCas, InMemoryCas};
use mediapm_conductor::runtime_env::{ensure_runtime_env_files, load_runtime_env_files};
use mediapm_conductor::{
    ConductorApi, ConductorError, MachineNickelDocument, RunSummary, RunWorkflowOptions,
    RuntimeStoragePaths, SimpleConductor,
};
use musicbrainz_rs::entity::recording::Recording;
use musicbrainz_rs::prelude::*;
use rand::Rng as _;
use url::Url;

pub use conductor_bridge::{ConductorToolRow, ToolSyncReport};
pub use config::{
    HierarchyEntry, HierarchyEntryKind, HierarchyFolderRenameRule, HierarchyNode,
    HierarchyNodeKind, MaterializationMethod, MediaMetadataRegexTransform, MediaMetadataValue,
    MediaMetadataValueCandidate, MediaMetadataVariantBinding, MediaPmDocument, MediaPmState,
    MediaRuntimeStorage, MediaSourceSpec, MediaStep, MediaStepTool, PlatformInheritedEnvVars,
    PlaylistEntryPathMode, PlaylistFormat, PlaylistItemRef, ToolRequirement,
    ToolRequirementDependencies, TransformInputValue, flatten_hierarchy_value,
    load_mediapm_document, load_mediapm_state_document, merge_mediapm_document_with_state,
    nest_hierarchy_value, regex_variant_selector, save_mediapm_document,
    save_mediapm_state_document,
};
pub use error::MediaPmError;
pub use global::MediaPmGlobalPaths;
pub use lockfile::{
    MEDIAPM_LOCK_VERSION, ManagedFileRecord, MediaLockFile, ToolRegistryRecord, ToolRegistryStatus,
    load_lockfile, save_lockfile,
};
pub use materializer::MaterializeReport;
pub use paths::MediaPmPaths;

use crate::tools::downloader::{
    ToolCachePruneReport, ToolDownloadCache, default_global_tool_cache_root,
};

/// Media package descriptor returned by source processing.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct MediaPackage {
    /// Stable media id in `mediapm.ncl`.
    pub media_id: String,
    /// Canonical source URI that produced this package.
    pub source_uri: Url,
    /// Whether permanent transcode mode was requested.
    pub permanent: bool,
}

/// Summary of one complete `mediapm sync` execution.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct SyncSummary {
    /// Number of conductor instances executed during sync.
    pub executed_instances: usize,
    /// Number of conductor instances served from cache.
    pub cached_instances: usize,
    /// Number of conductor instances rematerialized from cache metadata.
    pub rematerialized_instances: usize,
    /// Number of hierarchy paths materialized to the resolved library root.
    pub materialized_paths: usize,
    /// Number of stale hierarchy paths removed.
    pub removed_paths: usize,
    /// Number of tools newly registered in conductor machine config.
    pub added_tools: usize,
    /// Number of tools updated/promoted in conductor machine config.
    pub updated_tools: usize,
    /// Non-fatal warnings surfaced during sync.
    pub warnings: Vec<String>,
}

/// Summary of one `mediapm tools sync` execution.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ToolsSyncSummary {
    /// Number of tool ids newly registered in conductor machine config.
    pub added_tools: usize,
    /// Number of tool ids updated/promoted to match desired version.
    pub updated_tools: usize,
    /// Number of desired tool ids already up to date.
    pub unchanged_tools: usize,
    /// Non-fatal tool-sync warnings.
    pub warnings: Vec<String>,
}

/// Preset families supported by `mediapm hierarchy add/remove`.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum MediaHierarchyPreset {
    /// Local-source hierarchy preset.
    Local,
    /// Online-source (`yt-dlp`) hierarchy preset.
    YtDlp,
}

impl MediaHierarchyPreset {
    /// Returns stable identifier text for user-facing diagnostics and ids.
    #[must_use]
    fn as_label(self) -> &'static str {
        match self {
            Self::Local => "local",
            Self::YtDlp => "yt-dlp",
        }
    }
}

/// Status of the global user cache under the `mediapm` user-cache namespace.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct GlobalToolCacheStatus {
    /// Root path of the global user cache directory.
    pub tool_cache_dir: PathBuf,
    /// CAS payload store directory (`cache/store/`).
    pub store_dir: PathBuf,
    /// Default metadata index file path (`cache/tools.jsonc`).
    pub index_jsonc: PathBuf,
    /// Number of logical cache-key rows currently tracked.
    pub entry_count: usize,
}

/// Summary of one global user-cache prune run.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Default)]
pub struct GlobalToolCachePruneSummary {
    /// Number of expired key rows removed from index metadata.
    pub removed_entries: usize,
    /// Number of unreferenced payload hashes removed from CAS store.
    pub removed_payloads: usize,
}

/// Resolves default user-scoped global paths for `mediapm`.
#[must_use]
pub fn resolve_default_global_paths() -> Option<MediaPmGlobalPaths> {
    MediaPmGlobalPaths::resolve_default()
}

/// Ensures global user-directory layout exists and returns resolved paths.
///
/// # Errors
///
/// Returns [`MediaPmError`] when global path resolution fails or required
/// directories cannot be created.
pub fn ensure_global_directory_layout() -> Result<MediaPmGlobalPaths, MediaPmError> {
    let paths = resolve_global_paths_or_error()?;

    fs::create_dir_all(&paths.root_dir).map_err(|source| MediaPmError::Io {
        operation: "creating global mediapm root directory".to_string(),
        path: paths.root_dir.clone(),
        source,
    })?;
    fs::create_dir_all(&paths.tool_cache_dir).map_err(|source| MediaPmError::Io {
        operation: "creating global user cache directory".to_string(),
        path: paths.tool_cache_dir.clone(),
        source,
    })?;

    Ok(paths)
}

/// Returns current status for the global user cache.
///
/// # Errors
///
/// Returns [`MediaPmError`] when global paths cannot be resolved or cache
/// metadata cannot be opened/read.
pub async fn global_tool_cache_status() -> Result<GlobalToolCacheStatus, MediaPmError> {
    let paths = resolve_global_paths_or_error()?;
    let cache = ToolDownloadCache::open(&paths.tool_cache_dir).await?;

    Ok(GlobalToolCacheStatus {
        tool_cache_dir: paths.tool_cache_dir.clone(),
        store_dir: paths.tool_cache_store_dir,
        index_jsonc: paths.tool_cache_index_jsonc,
        entry_count: cache.entry_count(),
    })
}

/// Prunes expired rows from the global user cache and removes stale payloads.
///
/// # Errors
///
/// Returns [`MediaPmError`] when global paths cannot be resolved, cache
/// metadata cannot be opened, or cache pruning fails.
pub async fn global_tool_cache_prune_expired() -> Result<GlobalToolCachePruneSummary, MediaPmError>
{
    let tool_cache_root = default_global_tool_cache_root().ok_or_else(|| {
        MediaPmError::Workflow(
            "global mediapm directory could not be resolved for this environment".to_string(),
        )
    })?;
    let cache = ToolDownloadCache::open(&tool_cache_root).await?;
    let ToolCachePruneReport { removed_entries, removed_payloads } =
        cache.prune_expired_entries().await?;

    Ok(GlobalToolCachePruneSummary { removed_entries, removed_payloads })
}

/// Removes all files under the global user cache directory.
///
/// # Errors
///
/// Returns [`MediaPmError`] when global paths cannot be resolved or existing
/// cache directories cannot be removed.
pub fn global_tool_cache_clear() -> Result<(), MediaPmError> {
    let paths = resolve_global_paths_or_error()?;
    if !paths.tool_cache_dir.exists() {
        return Ok(());
    }

    fs::remove_dir_all(&paths.tool_cache_dir).map_err(|source| MediaPmError::Io {
        operation: "clearing global user cache directory".to_string(),
        path: paths.tool_cache_dir,
        source,
    })
}

/// Resolves default global paths or returns one workflow-level error.
fn resolve_global_paths_or_error() -> Result<MediaPmGlobalPaths, MediaPmError> {
    resolve_default_global_paths().ok_or_else(|| {
        MediaPmError::Workflow(
            "global mediapm directory could not be resolved for this environment".to_string(),
        )
    })
}

/// Async API contract for media source processing and sync.
pub trait MediaPmApi: Send + Sync {
    /// Processes a single source URI using the configured media pipeline policy.
    fn process_source(
        &self,
        uri: Url,
        permanent: bool,
    ) -> impl Future<Output = Result<MediaPackage, MediaPmError>> + Send;

    /// Reconciles declared media/tool state to filesystem/materialization state.
    fn sync_library(&self) -> impl Future<Output = Result<SyncSummary, MediaPmError>> + Send;
}

/// Generic media service over a pluggable conductor implementation.
pub struct MediaPmService<C>
where
    C: ConductorApi,
{
    /// Conductor backend used for workflow execution and state coordination.
    conductor: C,
    /// Canonical Phase 3 path set for this service instance.
    paths: MediaPmPaths,
    /// Optional runtime-storage overrides applied after `mediapm.ncl` values.
    runtime_storage_overrides: MediaRuntimeStorage,
}

impl<C> MediaPmService<C>
where
    C: ConductorApi,
{
    /// Creates a media service with explicit workspace paths.
    #[must_use]
    pub fn new(conductor: C, paths: MediaPmPaths) -> Self {
        Self { conductor, paths, runtime_storage_overrides: MediaRuntimeStorage::default() }
    }

    /// Creates a media service with explicit workspace paths and path overrides.
    ///
    /// `runtime_storage_overrides` has higher precedence than values declared in
    /// `mediapm.ncl` and is primarily used for CLI-level path flags.
    #[must_use]
    pub fn new_with_runtime_storage_overrides(
        conductor: C,
        paths: MediaPmPaths,
        runtime_storage_overrides: MediaRuntimeStorage,
    ) -> Self {
        Self { conductor, paths, runtime_storage_overrides }
    }

    /// Returns canonical Phase 3 paths used by this service.
    #[must_use]
    pub fn paths(&self) -> &MediaPmPaths {
        &self.paths
    }

    /// Resolves effective Phase 3 paths by merging config + service overrides.
    fn resolve_effective_paths(
        &self,
        config_runtime_storage: &MediaRuntimeStorage,
    ) -> MediaPmPaths {
        let merged = self.resolve_effective_runtime_storage(config_runtime_storage);
        self.paths.with_runtime_storage(&merged)
    }

    /// Resolves effective runtime-storage policy by merging config + overrides.
    fn resolve_effective_runtime_storage(
        &self,
        config_runtime_storage: &MediaRuntimeStorage,
    ) -> MediaRuntimeStorage {
        merge_runtime_storage(config_runtime_storage, &self.runtime_storage_overrides)
    }

    /// Reconciles managed workflows for config-only edit commands.
    ///
    /// This helper keeps conductor machine workflow rows synchronized after
    /// source/hierarchy mutations even when explicit tool sync is deferred.
    ///
    /// # Errors
    ///
    /// Returns [`MediaPmError`] when conductor documents cannot be prepared,
    /// workflow reconciliation fails, or lock state cannot be persisted.
    fn reconcile_workflows_after_config_edit(
        &self,
        document: &MediaPmDocument,
    ) -> Result<(), MediaPmError> {
        let effective_paths = self.resolve_effective_paths(&document.runtime);
        conductor_bridge::ensure_conductor_documents(&effective_paths)?;
        let mut lock = load_lockfile(&effective_paths.mediapm_state_ncl)?;
        conductor_bridge::reconcile_media_workflows_for_config_edits(
            &effective_paths,
            document,
            &mut lock,
        )?;
        save_lockfile(&effective_paths.mediapm_state_ncl, &lock)
    }

    /// Adds one online media source to `mediapm.ncl`.
    ///
    /// # Errors
    ///
    /// Returns [`MediaPmError`] when source validation fails, config cannot be
    /// loaded/saved, or default source metadata cannot be synthesized.
    #[expect(
        clippy::too_many_lines,
        reason = "this item intentionally keeps end-to-end control flow together so ordering invariants remain explicit during maintenance"
    )]
    pub async fn add_media_source(
        &self,
        uri: &Url,
        recording_id: Option<&str>,
    ) -> Result<String, MediaPmError> {
        validate_source_uri(uri)?;

        if uri.scheme() == "local" {
            return Err(MediaPmError::Workflow(
                "use 'media add --preset local <path>' for local sources so CAS hash pointers are recorded"
                    .to_string(),
            ));
        }

        let mb = if let Some(rid) = recording_id {
            Some(fetch_mb_recording_metadata(rid).await?)
        } else {
            None
        };

        let mut document = ensure_and_load_mediapm_document(&self.paths.mediapm_ncl)?;
        let media_id = media_id_from_uri(uri);
        let OnlineSourceMetadata { title, artist, description } = fetch_online_source_metadata(uri);
        let source_title = mb
            .as_ref()
            .map(|m| m.title.clone())
            .or(title)
            .unwrap_or_else(|| remote_default_title(uri));
        let source_artist_literal = mb.as_ref().map(|m| m.artist.clone());
        let source_description = mb
            .as_ref()
            .map(|m| build_remote_default_description(&m.title, Some(&m.artist)))
            .or(description)
            .unwrap_or_else(|| build_remote_default_description(&source_title, artist.as_deref()));

        document.media.insert(
            media_id.clone(),
            MediaSourceSpec {
                id: None,
                description: Some(source_description),
                title: Some(source_title.clone()),
                workflow_id: None,
                metadata: Some(BTreeMap::from([
                    (
                        "title".to_string(),
                        MediaMetadataValue::Fallback(vec![
                            MediaMetadataValueCandidate::Variant(MediaMetadataVariantBinding {
                                variant: "video".to_string(),
                                metadata_key: "title".to_string(),
                                transform: None,
                            }),
                            MediaMetadataValueCandidate::Variant(MediaMetadataVariantBinding {
                                variant: "video".to_string(),
                                metadata_key: "track".to_string(),
                                transform: None,
                            }),
                            MediaMetadataValueCandidate::Variant(MediaMetadataVariantBinding {
                                variant: "infojson".to_string(),
                                metadata_key: "title".to_string(),
                                transform: None,
                            }),
                            MediaMetadataValueCandidate::Literal(source_title.clone()),
                        ]),
                    ),
                    (
                        "artist".to_string(),
                        MediaMetadataValue::Fallback(vec![
                            MediaMetadataValueCandidate::Variant(MediaMetadataVariantBinding {
                                variant: "video".to_string(),
                                metadata_key: "artist".to_string(),
                                transform: None,
                            }),
                            MediaMetadataValueCandidate::Variant(MediaMetadataVariantBinding {
                                variant: "video".to_string(),
                                metadata_key: "album_artist".to_string(),
                                transform: None,
                            }),
                            MediaMetadataValueCandidate::Variant(MediaMetadataVariantBinding {
                                variant: "infojson".to_string(),
                                metadata_key: "uploader".to_string(),
                                transform: None,
                            }),
                            MediaMetadataValueCandidate::Literal(
                                source_artist_literal.unwrap_or_else(|| "unknown".to_string()),
                            ),
                        ]),
                    ),
                    (
                        "video_id".to_string(),
                        MediaMetadataValue::Variant(MediaMetadataVariantBinding {
                            variant: "infojson".to_string(),
                            metadata_key: "id".to_string(),
                            transform: None,
                        }),
                    ),
                    (
                        "video_ext".to_string(),
                        MediaMetadataValue::Variant(MediaMetadataVariantBinding {
                            variant: "video".to_string(),
                            metadata_key: "format_name".to_string(),
                            transform: Some(MediaMetadataRegexTransform {
                                pattern: "(?i)matroska(?:,.*)?".to_string(),
                                replacement: ".mkv".to_string(),
                            }),
                        }),
                    ),
                ])),
                variant_hashes: BTreeMap::new(),
                steps: vec![
                    MediaStep {
                        tool: MediaStepTool::YtDlp,
                        input_variants: Vec::new(),
                        output_variants: BTreeMap::from([
                            (
                                "video".to_string(),
                                serde_json::json!({
                                    "kind": "primary",
                                }),
                            ),
                            (
                                "subtitles".to_string(),
                                serde_json::json!({
                                    "kind": "subtitles",
                                }),
                            ),
                            (
                                "thumbnails".to_string(),
                                serde_json::json!({
                                    "kind": "thumbnails",
                                }),
                            ),
                            (
                                "description".to_string(),
                                serde_json::json!({
                                    "kind": "description",
                                }),
                            ),
                            (
                                "infojson".to_string(),
                                serde_json::json!({
                                    "kind": "infojson",
                                }),
                            ),
                            (
                                "links".to_string(),
                                serde_json::json!({
                                    "kind": "links",
                                }),
                            ),
                            (
                                "archive".to_string(),
                                serde_json::json!({
                                    "kind": "archive",
                                }),
                            ),
                        ]),
                        options: BTreeMap::from([(
                            "uri".to_string(),
                            TransformInputValue::String(normalize_source_uri(uri).to_string()),
                        )]),
                    },
                    MediaStep {
                        tool: MediaStepTool::Ffmpeg,
                        input_variants: vec!["video".to_string()],
                        output_variants: BTreeMap::from([(
                            "video".to_string(),
                            serde_json::json!({
                                "kind": "primary",
                                "idx": 0,
                                "extension": "mkv",
                            }),
                        )]),
                        options: BTreeMap::from([
                            (
                                "codec_copy".to_string(),
                                TransformInputValue::String("true".to_string()),
                            ),
                            (
                                "container".to_string(),
                                TransformInputValue::String("matroska".to_string()),
                            ),
                        ]),
                    },
                    MediaStep {
                        tool: MediaStepTool::MediaTagger,
                        input_variants: vec!["video".to_string()],
                        output_variants: BTreeMap::from([(
                            "video".to_string(),
                            serde_json::json!({
                                "kind": "primary",
                                "extension": "mkv",
                            }),
                        )]),
                        options: BTreeMap::new(),
                    },
                    MediaStep {
                        tool: MediaStepTool::Rsgain,
                        input_variants: vec!["video".to_string()],
                        output_variants: BTreeMap::from([(
                            "video".to_string(),
                            serde_json::json!({
                                "kind": "primary",
                                "extension": "mkv",
                            }),
                        )]),
                        options: BTreeMap::new(),
                    },
                ],
            },
        );

        save_mediapm_document(&self.paths.mediapm_ncl, &document)?;
        self.reconcile_workflows_after_config_edit(&document)?;
        Ok(media_id)
    }

    /// Adds one local media source to `mediapm.ncl` as an `import`
    /// CAS-hash ingest step.
    ///
    /// # Errors
    ///
    /// Returns [`MediaPmError`] when the local source path cannot be
    /// canonicalized/read, CAS import fails, config cannot be loaded/saved, or
    /// required conductor runtime documents cannot be prepared.
    #[expect(
        clippy::too_many_lines,
        reason = "this item intentionally keeps end-to-end control flow together so ordering invariants remain explicit during maintenance"
    )]
    pub async fn add_local_source(
        &self,
        local_path: &Path,
        recording_id: Option<&str>,
    ) -> Result<String, MediaPmError> {
        let absolute = local_path.canonicalize().map_err(|source| MediaPmError::Io {
            operation: "canonicalizing local media path".to_string(),
            path: local_path.to_path_buf(),
            source,
        })?;

        let mut document = ensure_and_load_mediapm_document(&self.paths.mediapm_ncl)?;
        let effective_paths = self.resolve_effective_paths(&document.runtime);

        conductor_bridge::ensure_conductor_documents(&effective_paths)?;
        export_mediapm_nickel_config_schemas(&effective_paths)?;

        let machine =
            conductor_bridge::load_machine_document(&effective_paths.conductor_machine_ncl)?;
        let cas_root = resolve_conductor_cas_root(&effective_paths, &machine);
        let cas = FileSystemCas::open(&cas_root).await.map_err(|source| {
            MediaPmError::Workflow(format!(
                "opening conductor CAS store '{}' for local import: {source}",
                cas_root.display()
            ))
        })?;

        let bytes = tokio::fs::read(&absolute).await.map_err(|source| MediaPmError::Io {
            operation: "reading local media source for CAS import".to_string(),
            path: absolute.clone(),
            source,
        })?;
        let hash = cas.put(bytes).await.map_err(|source| {
            MediaPmError::Workflow(format!("importing local media into CAS failed: {source}"))
        })?;

        let mb = if let Some(rid) = recording_id {
            Some(fetch_mb_recording_metadata(rid).await?)
        } else {
            None
        };

        let media_id = media_id_from_local_path(&absolute);
        let LocalSourceMetadata { title, description } = fetch_local_source_metadata(&absolute);
        let source_title = mb
            .as_ref()
            .map(|m| m.title.clone())
            .or(title)
            .unwrap_or_else(|| local_default_title(&absolute));
        let source_artist_literal = mb.as_ref().map(|m| m.artist.clone());
        let source_description = mb
            .as_ref()
            .map(|m| build_remote_default_description(&m.title, Some(&m.artist)))
            .or(description)
            .unwrap_or_else(|| build_local_default_description(&absolute, &source_title));
        let source_extension_with_dot = local_extension_with_dot(&absolute);
        let hash_text = hash.to_string();

        document.media.insert(
            media_id.clone(),
            MediaSourceSpec {
                id: None,
                description: Some(source_description),
                title: Some(source_title.clone()),
                workflow_id: None,
                metadata: Some(BTreeMap::from([
                    (
                        "title".to_string(),
                        MediaMetadataValue::Fallback(vec![
                            MediaMetadataValueCandidate::Variant(MediaMetadataVariantBinding {
                                variant: "media".to_string(),
                                metadata_key: "title".to_string(),
                                transform: None,
                            }),
                            MediaMetadataValueCandidate::Variant(MediaMetadataVariantBinding {
                                variant: "media".to_string(),
                                metadata_key: "track".to_string(),
                                transform: None,
                            }),
                            MediaMetadataValueCandidate::Literal(source_title.clone()),
                        ]),
                    ),
                    (
                        "artist".to_string(),
                        MediaMetadataValue::Fallback(vec![
                            MediaMetadataValueCandidate::Variant(MediaMetadataVariantBinding {
                                variant: "media".to_string(),
                                metadata_key: "artist".to_string(),
                                transform: None,
                            }),
                            MediaMetadataValueCandidate::Variant(MediaMetadataVariantBinding {
                                variant: "media".to_string(),
                                metadata_key: "album_artist".to_string(),
                                transform: None,
                            }),
                            MediaMetadataValueCandidate::Literal(
                                source_artist_literal.unwrap_or_else(|| "unknown".to_string()),
                            ),
                        ]),
                    ),
                    (
                        "video_ext".to_string(),
                        MediaMetadataValue::Literal(source_extension_with_dot),
                    ),
                ])),
                variant_hashes: BTreeMap::new(),
                steps: local_source_default_steps(&hash_text),
            },
        );
        save_mediapm_document(&self.paths.mediapm_ncl, &document)?;
        self.reconcile_workflows_after_config_edit(&document)?;
        Ok(media_id)
    }

    /// Adds one hierarchy preset node tree for an existing media id.
    ///
    /// This operation is idempotent per `(preset, media_id, folder)` identity.
    /// Repeated invocations with the same triple will not add duplicate nodes.
    ///
    /// # Errors
    ///
    /// Returns [`MediaPmError`] when the media id is unknown, `folder` is
    /// empty/invalid, or `mediapm.ncl` cannot be loaded/saved.
    pub fn add_media_hierarchy_preset(
        &self,
        preset: MediaHierarchyPreset,
        media_id: &str,
        folder: &str,
    ) -> Result<(), MediaPmError> {
        let mut document = ensure_and_load_mediapm_document(&self.paths.mediapm_ncl)?;

        if !document.media.contains_key(media_id) {
            return Err(MediaPmError::Workflow(format!(
                "cannot add {} hierarchy preset: media id '{media_id}' does not exist",
                preset.as_label()
            )));
        }

        let normalized_folder = normalize_hierarchy_folder_root(folder)?;
        let hierarchy_id = hierarchy_preset_node_id(media_id);
        if hierarchy_contains_node_id(&document.hierarchy, &hierarchy_id) {
            return Ok(());
        }

        document.hierarchy.push(build_hierarchy_preset_node(
            preset,
            media_id,
            &normalized_folder,
            hierarchy_id,
        ));

        save_mediapm_document(&self.paths.mediapm_ncl, &document)?;
        self.reconcile_workflows_after_config_edit(&document)?;
        Ok(())
    }

    /// Removes one media source id from `mediapm.ncl`.
    ///
    /// This operation also removes any hierarchy nodes whose effective
    /// `media_id` equals the removed media id so configuration remains
    /// self-consistent after source deletion.
    ///
    /// # Errors
    ///
    /// Returns [`MediaPmError`] when the target media id is not registered or
    /// when `mediapm.ncl` cannot be loaded/saved.
    pub fn remove_media_source(&self, media_id: &str) -> Result<usize, MediaPmError> {
        let mut document = ensure_and_load_mediapm_document(&self.paths.mediapm_ncl)?;
        if document.media.remove(media_id).is_none() {
            return Err(MediaPmError::Workflow(format!(
                "cannot remove media source: media id '{media_id}' does not exist"
            )));
        }

        let removed_hierarchy_nodes =
            remove_hierarchy_nodes_by_media_id(&mut document.hierarchy, media_id);
        save_mediapm_document(&self.paths.mediapm_ncl, &document)?;
        self.reconcile_workflows_after_config_edit(&document)?;
        Ok(removed_hierarchy_nodes)
    }

    /// Removes one hierarchy preset node tree for one media id + folder root.
    ///
    /// This operation is idempotent. If the preset node does not exist,
    /// no changes are written and `0` is returned.
    ///
    /// # Errors
    ///
    /// Returns [`MediaPmError`] when `folder` is empty/invalid or
    /// `mediapm.ncl` cannot be loaded/saved.
    pub fn remove_media_hierarchy_preset(
        &self,
        _preset: MediaHierarchyPreset,
        media_id: &str,
        folder: &str,
    ) -> Result<usize, MediaPmError> {
        let mut document = ensure_and_load_mediapm_document(&self.paths.mediapm_ncl)?;
        normalize_hierarchy_folder_root(folder)?;
        let hierarchy_id = hierarchy_preset_node_id(media_id);
        let removed_nodes = remove_hierarchy_nodes_by_id(&mut document.hierarchy, &hierarchy_id);
        if removed_nodes > 0 {
            save_mediapm_document(&self.paths.mediapm_ncl, &document)?;
            self.reconcile_workflows_after_config_edit(&document)?;
        }
        Ok(removed_nodes)
    }

    /// Lists currently registered tools from conductor machine config.
    ///
    /// # Errors
    ///
    /// Returns [`MediaPmError`] when config or state document cannot be loaded or
    /// when conductor tool rows cannot be resolved.
    pub fn list_tools(&self) -> Result<Vec<ConductorToolRow>, MediaPmError> {
        let document = ensure_and_load_mediapm_document(&self.paths.mediapm_ncl)?;
        let effective_paths = self.resolve_effective_paths(&document.runtime);
        let lock = load_lockfile(&effective_paths.mediapm_state_ncl)?;
        conductor_bridge::list_tools(&effective_paths, &lock)
    }

    /// Prunes one tool binary while preserving metadata.
    ///
    /// # Errors
    ///
    /// Returns [`MediaPmError`] when config/state documents cannot be loaded,
    /// prune operations fail, or state cannot be persisted.
    pub async fn prune_tool(&self, tool_id: &str) -> Result<usize, MediaPmError> {
        let document = ensure_and_load_mediapm_document(&self.paths.mediapm_ncl)?;
        let effective_paths = self.resolve_effective_paths(&document.runtime);
        let mut lock = load_lockfile(&effective_paths.mediapm_state_ncl)?;
        let removed_hashes =
            conductor_bridge::prune_tool_binary(&effective_paths, &mut lock, tool_id).await?;
        save_lockfile(&effective_paths.mediapm_state_ncl, &lock)?;
        Ok(removed_hashes)
    }

    /// Executes one managed tool binary directly with passthrough arguments.
    ///
    /// `tool_selector` accepts either an immutable tool id or one logical tool
    /// name that resolves to exactly one active/installed managed tool.
    ///
    /// # Errors
    ///
    /// Returns [`MediaPmError`] when runtime docs/state cannot be loaded,
    /// selector resolution is ambiguous/invalid, executable materialization is
    /// missing, process launch fails, or the host does not provide an exit code.
    pub fn run_managed_tool(
        &self,
        tool_selector: &str,
        args: &[String],
    ) -> Result<i32, MediaPmError> {
        let document = ensure_and_load_mediapm_document(&self.paths.mediapm_ncl)?;
        let effective_paths = self.resolve_effective_paths(&document.runtime);
        conductor_bridge::ensure_conductor_documents(&effective_paths)?;
        let lock = load_lockfile(&effective_paths.mediapm_state_ncl)?;
        let target = conductor_bridge::resolve_managed_tool_executable_target(
            &effective_paths,
            &lock,
            tool_selector,
        )?;

        let status =
            ProcessCommand::new(&target.command_path).args(args).status().map_err(|source| {
                MediaPmError::Io {
                    operation: format!("running managed tool '{}' executable", target.tool_id),
                    path: target.command_path.clone(),
                    source,
                }
            })?;

        status.code().ok_or_else(|| {
            MediaPmError::Workflow(format!(
                "managed tool '{}' terminated without a numeric exit code",
                target.tool_id
            ))
        })
    }

    /// Refreshes mediapm-managed conductor runtime paths and dotenv files.
    ///
    /// This command updates machine-managed runtime defaults under
    /// `mediapm.conductor.machine.ncl` so moved workspaces re-materialize
    /// effective paths on next execution without running workflows.
    ///
    /// # Errors
    ///
    /// Returns [`MediaPmError`] when config loading, dotenv setup, or document
    /// normalization fails.
    pub fn refresh_runtime_configuration(&self) -> Result<(), MediaPmError> {
        let document = ensure_and_load_mediapm_document(&self.paths.mediapm_ncl)?;
        let effective_paths = self.resolve_effective_paths(&document.runtime);
        load_runtime_dotenv(&effective_paths)?;
        ensure_runtime_env_files(&effective_paths.runtime_root).map_err(MediaPmError::from)?;
        conductor_bridge::ensure_conductor_documents(&effective_paths)?;
        Ok(())
    }

    /// Reconciles only tool requirements and state/runtime metadata.
    ///
    /// This operation intentionally avoids running conductor workflows or
    /// hierarchy materialization, and is used by `mediapm tools sync`.
    ///
    /// # Errors
    ///
    /// Returns [`MediaPmError`] when runtime/config preparation fails, tool
    /// reconciliation fails, workflow reconciliation fails, or state
    /// cannot be persisted.
    pub async fn sync_tools(&self) -> Result<ToolsSyncSummary, MediaPmError> {
        self.sync_tools_with_tag_update_checks(true).await
    }

    /// Reconciles only tool requirements and lock/runtime metadata.
    ///
    /// `check_tag_updates` controls whether tag-only tool selectors (for
    /// example `tag = "latest"`) trigger remote release checks.
    ///
    /// # Errors
    ///
    /// Returns [`MediaPmError`] when runtime/config preparation fails, tool
    /// reconciliation fails, workflow reconciliation fails, or lock state
    /// cannot be persisted.
    pub async fn sync_tools_with_tag_update_checks(
        &self,
        check_tag_updates: bool,
    ) -> Result<ToolsSyncSummary, MediaPmError> {
        let document = ensure_and_load_mediapm_document(&self.paths.mediapm_ncl)?;
        let (summary, _lock) = self.sync_tools_from_document(&document, check_tag_updates).await?;
        Ok(summary)
    }

    /// Internal helper: runs tool sync from an already-loaded mediapm document.
    ///
    /// Returns the sync summary together with the saved lock state so that
    /// callers performing a full library sync can skip a redundant lock reload
    /// and a redundant mediapm-document parse.
    async fn sync_tools_from_document(
        &self,
        document: &MediaPmDocument,
        check_tag_updates: bool,
    ) -> Result<(ToolsSyncSummary, MediaLockFile), MediaPmError> {
        let effective_runtime_storage = self.resolve_effective_runtime_storage(&document.runtime);
        let effective_paths = self.paths.with_runtime_storage(&effective_runtime_storage);
        load_runtime_dotenv(&effective_paths)?;
        conductor_bridge::ensure_conductor_documents(&effective_paths)?;
        export_mediapm_nickel_config_schemas(&effective_paths)?;
        mediapm_conductor::export_nickel_config_schemas(&effective_paths.conductor_schema_dir)?;

        let mut lock = load_lockfile(&effective_paths.mediapm_state_ncl)?;
        let resolved_inherited_env_vars =
            effective_runtime_storage.inherited_env_vars_with_defaults();
        let report = conductor_bridge::reconcile_desired_tools(
            &effective_paths,
            document,
            &resolved_inherited_env_vars,
            &mut lock,
            check_tag_updates,
            effective_runtime_storage.use_user_tool_cache_enabled(),
        )
        .await?;
        conductor_bridge::reconcile_media_workflows(&effective_paths, document, &mut lock)?;
        save_lockfile(&effective_paths.mediapm_state_ncl, &lock)?;

        Ok((
            ToolsSyncSummary {
                added_tools: report.added_tool_ids.len(),
                updated_tools: report.updated_tool_ids.len(),
                unchanged_tools: report.unchanged_tool_ids.len(),
                warnings: report.warnings,
            },
            lock,
        ))
    }

    /// Reconciles full desired state with explicit tag-update-check policy.
    ///
    /// # Errors
    ///
    /// Returns [`MediaPmError`] when tool sync fails, conductor execution
    /// fails (including filesystem-CAS fallback), hierarchy materialization
    /// fails, or state cannot be persisted.
    pub async fn sync_library_with_tag_update_checks(
        &self,
        check_tag_updates: bool,
    ) -> Result<SyncSummary, MediaPmError> {
        // Load the mediapm document once and reuse it across both sync phases to
        // avoid a redundant Nickel evaluation between tool-sync and library-sync.
        let document = ensure_and_load_mediapm_document(&self.paths.mediapm_ncl)?;
        let (tool_summary, mut lock) =
            self.sync_tools_from_document(&document, check_tag_updates).await?;
        let effective_runtime_storage = self.resolve_effective_runtime_storage(&document.runtime);
        let effective_paths = self.paths.with_runtime_storage(&effective_runtime_storage);
        let machine =
            conductor_bridge::load_machine_document(&effective_paths.conductor_machine_ncl)?;
        let conductor_cas_root = resolve_conductor_cas_root(&effective_paths, &machine);
        let workflow_options =
            conductor_run_workflow_options(&effective_paths, &effective_runtime_storage);

        let conductor_summary = if should_prefer_filesystem_workflow_runner(&machine) {
            run_workflow_with_filesystem_cas(
                &conductor_cas_root,
                &effective_paths.conductor_user_ncl,
                &effective_paths.conductor_machine_ncl,
                workflow_options,
            )
            .await?
        } else {
            match self
                .conductor
                .run_workflow_with_options(
                    &effective_paths.conductor_user_ncl,
                    &effective_paths.conductor_machine_ncl,
                    workflow_options,
                )
                .await
            {
                Ok(summary) => summary,
                Err(primary_error) => {
                    if !should_retry_workflow_with_filesystem_cas(&primary_error) {
                        return Err(primary_error.into());
                    }

                    run_workflow_with_filesystem_cas(
                        &conductor_cas_root,
                        &effective_paths.conductor_user_ncl,
                        &effective_paths.conductor_machine_ncl,
                        conductor_run_workflow_options(
                            &effective_paths,
                            &effective_runtime_storage,
                        ),
                    )
                    .await
                    .map_err(|fallback_error| {
                        MediaPmError::Workflow(format!(
                            "workflow execution failed with primary conductor backend ({primary_error}); filesystem-CAS fallback also failed: {fallback_error}"
                        ))
                    })?
                }
            }
        };

        let materialize_report = materializer::sync_hierarchy(
            &effective_paths,
            &document,
            &machine,
            &conductor_cas_root,
            &mut lock,
        )
        .await?;
        let mut warnings = tool_summary.warnings.clone();
        warnings.extend(materialize_report.notices.clone());

        // Reconcile again after materialization so managed-file hashes written
        // during this sync are immediately rooted in machine external_data.
        conductor_bridge::reconcile_media_workflows(&effective_paths, &document, &mut lock)?;
        save_lockfile(&effective_paths.mediapm_state_ncl, &lock)?;

        Ok(SyncSummary {
            executed_instances: conductor_summary.executed_instances,
            cached_instances: conductor_summary.cached_instances,
            rematerialized_instances: conductor_summary.rematerialized_instances,
            materialized_paths: materialize_report.materialized_paths,
            removed_paths: materialize_report.removed_paths,
            added_tools: tool_summary.added_tools,
            updated_tools: tool_summary.updated_tools,
            warnings,
        })
    }
}

impl MediaPmService<SimpleConductor<InMemoryCas>> {
    /// Creates an in-memory conductor stack rooted at the current directory.
    #[must_use]
    pub fn new_in_memory() -> Self {
        Self::new_in_memory_at(Path::new("."))
    }

    /// Creates an in-memory conductor stack for one explicit workspace root.
    #[must_use]
    pub fn new_in_memory_at(root_dir: &Path) -> Self {
        let cas = InMemoryCas::new();
        let conductor = SimpleConductor::new(cas);
        let paths = MediaPmPaths::from_root(root_dir);
        Self::new(conductor, paths)
    }

    /// Creates an in-memory conductor stack with runtime-storage overrides.
    #[must_use]
    pub fn new_in_memory_at_with_runtime_storage_overrides(
        root_dir: &Path,
        runtime_storage_overrides: MediaRuntimeStorage,
    ) -> Self {
        let cas = InMemoryCas::new();
        let conductor = SimpleConductor::new(cas);
        let paths = MediaPmPaths::from_root(root_dir);
        Self::new_with_runtime_storage_overrides(conductor, paths, runtime_storage_overrides)
    }
}

impl<C> MediaPmApi for MediaPmService<C>
where
    C: ConductorApi,
{
    async fn process_source(
        &self,
        uri: Url,
        permanent: bool,
    ) -> Result<MediaPackage, MediaPmError> {
        validate_source_uri(&uri)?;

        Ok(MediaPackage { media_id: media_id_from_uri(&uri), source_uri: uri, permanent })
    }

    async fn sync_library(&self) -> Result<SyncSummary, MediaPmError> {
        self.sync_library_with_tag_update_checks(false).await
    }
}

/// Returns built-in tool ids that phase 3 expects to be available.
#[must_use]
pub fn registered_builtin_ids() -> [&'static str; 5] {
    mediapm_conductor::registered_builtin_ids()
}

/// Loads runtime dotenv values for one workspace root using effective path policy.
///
/// This helper is intended for CLI entrypoints that need environment-backed
/// credentials before invoking internal builtins directly.
///
/// # Errors
///
/// Returns [`MediaPmError`] when config cannot be loaded, effective runtime
/// paths cannot be resolved, or dotenv loading fails.
pub fn load_runtime_dotenv_for_root(
    root_dir: &Path,
    runtime_storage_overrides: &MediaRuntimeStorage,
) -> Result<MediaPmPaths, MediaPmError> {
    let base_paths = MediaPmPaths::from_root(root_dir);
    let document = ensure_and_load_mediapm_document(&base_paths.mediapm_ncl)?;
    let merged_runtime_storage =
        merge_runtime_storage(&document.runtime, runtime_storage_overrides);
    let effective_paths = base_paths.with_runtime_storage(&merged_runtime_storage);
    load_runtime_dotenv(&effective_paths)?;
    Ok(effective_paths)
}

/// Loads `mediapm.ncl`, writing defaults when absent.
fn ensure_and_load_mediapm_document(path: &Path) -> Result<MediaPmDocument, MediaPmError> {
    if !config::mediapm_document_exists(path) {
        save_mediapm_document(path, &MediaPmDocument::default())?;
    }

    load_mediapm_document(path)
}

/// Returns true when any hierarchy node in one recursive tree declares
/// `target_id`.
fn hierarchy_contains_node_id(nodes: &[HierarchyNode], target_id: &str) -> bool {
    nodes.iter().any(|node| {
        node.id.as_deref().is_some_and(|value| value == target_id)
            || hierarchy_contains_node_id(&node.children, target_id)
    })
}

/// Removes all nodes in one recursive hierarchy tree whose `id` matches
/// `target_id` and returns the number of removed nodes.
fn remove_hierarchy_nodes_by_id(nodes: &mut Vec<HierarchyNode>, target_id: &str) -> usize {
    let mut removed = 0;
    let mut index = 0;

    while index < nodes.len() {
        if nodes[index].id.as_deref().is_some_and(|value| value == target_id) {
            nodes.remove(index);
            removed += 1;
            continue;
        }

        removed += remove_hierarchy_nodes_by_id(&mut nodes[index].children, target_id);
        index += 1;
    }

    removed
}

/// Removes all nodes in one recursive hierarchy tree whose `media_id` matches
/// `target_media_id` and returns the number of removed nodes.
fn remove_hierarchy_nodes_by_media_id(
    nodes: &mut Vec<HierarchyNode>,
    target_media_id: &str,
) -> usize {
    let mut removed = 0;
    let mut index = 0;

    while index < nodes.len() {
        if nodes[index].media_id.as_deref().is_some_and(|value| value == target_media_id) {
            nodes.remove(index);
            removed += 1;
            continue;
        }

        removed += remove_hierarchy_nodes_by_media_id(&mut nodes[index].children, target_media_id);
        index += 1;
    }

    removed
}

/// Stable media-root folder template used by hierarchy presets.
const HIERARCHY_MEDIA_ROOT_TEMPLATE: &str = "${media.metadata.title} [${media.id}]";

/// Demo-style yt-dlp media-root folder template used by hierarchy presets.
const HIERARCHY_YT_DLP_MEDIA_ROOT_TEMPLATE: &str =
    "${media.metadata.artist} - ${media.metadata.title} [${media.id}]";

/// Stable tagged-media filename template used by hierarchy presets.
const HIERARCHY_TAGGED_MEDIA_FILE_TEMPLATE: &str =
    "${media.metadata.title} [${media.id}]${media.metadata.video_ext}";

/// Demo-style yt-dlp tagged-media filename template.
const HIERARCHY_YT_DLP_TAGGED_MEDIA_FILE_TEMPLATE: &str =
    "${media.metadata.artist} - ${media.metadata.title} [${media.id}]${media.metadata.video_ext}";

/// Demo-style yt-dlp info-json filename template.
const HIERARCHY_YT_DLP_INFOJSON_FILE_TEMPLATE: &str =
    "${media.metadata.artist} - ${media.metadata.title} [${media.id}].info.json";

/// Demo-style yt-dlp description filename template.
const HIERARCHY_YT_DLP_DESCRIPTION_FILE_TEMPLATE: &str =
    "${media.metadata.artist} - ${media.metadata.title} [${media.id}].description.txt";

/// Demo-style root-sidecar rename pattern for flattened file-family variants.
const HIERARCHY_YT_DLP_ROOT_RENAME_PATTERN: &str = "^.*\\.([^.]*)$";

/// Normalizes one hierarchy-root folder CLI value.
///
/// Returned values use slash separators and never carry surrounding slashes.
///
/// # Errors
///
/// Returns [`MediaPmError`] when the provided folder is empty after trimming.
fn normalize_hierarchy_folder_root(folder: &str) -> Result<String, MediaPmError> {
    let normalized = folder.trim().replace('\\', "/").trim_matches('/').to_string();
    if normalized.is_empty() {
        return Err(MediaPmError::Workflow(
            "hierarchy preset folder must be non-empty".to_string(),
        ));
    }

    Ok(normalized)
}

/// Builds hierarchy id for one media-root folder.
#[must_use]
fn hierarchy_preset_node_id(media_id: &str) -> String {
    media_id.to_string()
}

/// Builds one media-file hierarchy node bound to one output variant.
#[must_use]
fn hierarchy_media_file_node(path: &str, media_id: &str, variant: &str) -> HierarchyNode {
    HierarchyNode {
        path: path.to_string(),
        kind: HierarchyNodeKind::Media,
        id: None,
        media_id: Some(media_id.to_string()),
        variant: Some(variant.to_string()),
        variants: Vec::new(),
        rename_files: Vec::new(),
        format: PlaylistFormat::default(),
        ids: Vec::new(),
        children: Vec::new(),
    }
}

/// Builds one media-folder hierarchy node bound to ordered variant selectors.
#[must_use]
fn hierarchy_media_folder_node(
    path: &str,
    media_id: &str,
    variants: Vec<String>,
    rename_files: Vec<HierarchyFolderRenameRule>,
) -> HierarchyNode {
    HierarchyNode {
        path: path.to_string(),
        kind: HierarchyNodeKind::MediaFolder,
        id: None,
        media_id: Some(media_id.to_string()),
        variant: None,
        variants,
        rename_files,
        format: PlaylistFormat::default(),
        ids: Vec::new(),
        children: Vec::new(),
    }
}

/// Builds media-root children for the local hierarchy preset.
#[must_use]
fn local_hierarchy_media_children(media_id: &str) -> Vec<HierarchyNode> {
    let mut media =
        hierarchy_media_file_node(HIERARCHY_TAGGED_MEDIA_FILE_TEMPLATE, media_id, "media");
    media.id = Some(format!("{media_id}.media"));
    vec![media]
}

/// Builds media-root children for the yt-dlp hierarchy preset.
#[must_use]
fn yt_dlp_hierarchy_media_children(media_id: &str) -> Vec<HierarchyNode> {
    let mut video =
        hierarchy_media_file_node(HIERARCHY_YT_DLP_TAGGED_MEDIA_FILE_TEMPLATE, media_id, "video");
    video.id = Some(format!("{media_id}.video"));

    let mut archive = hierarchy_media_file_node(
        "${media.metadata.artist} - ${media.metadata.title} [${media.id}].archive.txt",
        media_id,
        "archive",
    );
    archive.id = Some(format!("{media_id}.archive"));

    let mut description = hierarchy_media_file_node(
        HIERARCHY_YT_DLP_DESCRIPTION_FILE_TEMPLATE,
        media_id,
        "description",
    );
    description.id = Some(format!("{media_id}.description"));

    let mut infojson =
        hierarchy_media_file_node(HIERARCHY_YT_DLP_INFOJSON_FILE_TEMPLATE, media_id, "infojson");
    infojson.id = Some(format!("{media_id}.infojson"));

    let mut subtitles = hierarchy_media_folder_node(
        "subtitles",
        media_id,
        vec!["subtitles".to_string()],
        vec![HierarchyFolderRenameRule {
            pattern: "^([^.]+)\\.([^.]*)$".to_string(),
            replacement: "${media.metadata.artist} - ${media.metadata.title} [${media.id}].$1.$2"
                .to_string(),
        }],
    );
    subtitles.id = Some(format!("{media_id}.subtitles"));

    let mut thumbnails = hierarchy_media_folder_node(
        "thumbnails",
        media_id,
        vec!["thumbnails".to_string()],
        vec![HierarchyFolderRenameRule {
            pattern: HIERARCHY_YT_DLP_ROOT_RENAME_PATTERN.to_string(),
            replacement:
                "${media.metadata.artist} - ${media.metadata.title} [${media.id}].thumbnail.$1"
                    .to_string(),
        }],
    );
    thumbnails.id = Some(format!("{media_id}.thumbnails"));

    let mut thumbnails_root = hierarchy_media_folder_node(
        "",
        media_id,
        vec!["thumbnails".to_string()],
        vec![HierarchyFolderRenameRule {
            pattern: HIERARCHY_YT_DLP_ROOT_RENAME_PATTERN.to_string(),
            replacement: "folder.$1".to_string(),
        }],
    );
    thumbnails_root.id = Some(format!("{media_id}.thumbnails.folder"));

    let mut links = hierarchy_media_folder_node(
        "links",
        media_id,
        vec!["links".to_string()],
        vec![HierarchyFolderRenameRule {
            pattern: HIERARCHY_YT_DLP_ROOT_RENAME_PATTERN.to_string(),
            replacement: "${media.metadata.artist} - ${media.metadata.title} [${media.id}].link.$1"
                .to_string(),
        }],
    );
    links.id = Some(format!("{media_id}.links"));

    vec![video, archive, description, infojson, subtitles, thumbnails, thumbnails_root, links]
}

/// Builds one hierarchy node tree for the selected preset.
#[must_use]
fn build_hierarchy_preset_node(
    preset: MediaHierarchyPreset,
    media_id: &str,
    normalized_folder: &str,
    hierarchy_id: String,
) -> HierarchyNode {
    let (media_root_template, media_children) = match preset {
        MediaHierarchyPreset::Local => {
            (HIERARCHY_MEDIA_ROOT_TEMPLATE.to_string(), local_hierarchy_media_children(media_id))
        }
        MediaHierarchyPreset::YtDlp => (
            HIERARCHY_YT_DLP_MEDIA_ROOT_TEMPLATE.to_string(),
            yt_dlp_hierarchy_media_children(media_id),
        ),
    };

    HierarchyNode {
        path: normalized_folder.to_string(),
        kind: HierarchyNodeKind::Folder,
        id: None,
        media_id: Some(media_id.to_string()),
        variant: None,
        variants: Vec::new(),
        rename_files: Vec::new(),
        format: PlaylistFormat::default(),
        ids: Vec::new(),
        children: vec![HierarchyNode {
            path: media_root_template,
            kind: HierarchyNodeKind::Folder,
            id: Some(hierarchy_id),
            media_id: Some(media_id.to_string()),
            variant: None,
            variants: Vec::new(),
            rename_files: Vec::new(),
            format: PlaylistFormat::default(),
            ids: Vec::new(),
            children: media_children,
        }],
    }
}

/// Ensures runtime dotenv files exist and loads key/value pairs into process env.
fn load_runtime_dotenv(paths: &MediaPmPaths) -> Result<(), MediaPmError> {
    load_runtime_env_files(&paths.runtime_root).map_err(|source| {
        MediaPmError::Workflow(format!(
            "loading conductor runtime dotenv files under '{}' failed: {source}",
            paths.runtime_root.display()
        ))
    })?;

    Ok(())
}

/// Exports embedded `mediapm.ncl` Nickel schemas into runtime storage.
///
/// Export policy is controlled by `runtime.mediapm_schema_dir`:
/// - omitted: writes to `<runtime.mediapm_dir>/config/mediapm`,
/// - explicit `null`: disables export,
/// - explicit string: writes to that resolved path.
fn export_mediapm_nickel_config_schemas(paths: &MediaPmPaths) -> Result<(), MediaPmError> {
    let Some(export_dir) = paths.schema_export_dir.as_ref() else {
        return Ok(());
    };

    fs::create_dir_all(export_dir).map_err(|source| MediaPmError::Io {
        operation: "creating mediapm schema export directory".to_string(),
        path: export_dir.clone(),
        source,
    })?;

    for (file_name, content) in config::versions::embedded_schema_sources() {
        let path = export_dir.join(file_name);
        fs::write(&path, content.as_bytes()).map_err(|source| MediaPmError::Io {
            operation: format!("writing exported mediapm Nickel schema '{file_name}'"),
            path,
            source,
        })?;
    }

    Ok(())
}

/// `YouTube` URLs are canonicalized to `https://www.youtube.com/watch?v={video_id}`
/// so that tracking parameters are stripped and short (`youtu.be`) links are
/// expanded.  All other URLs are returned unchanged.
fn normalize_source_uri(uri: &Url) -> Url {
    // Extract YouTube video id from www.youtube.com or youtu.be forms.
    let host = uri.host_str().unwrap_or("");
    let video_id: Option<String> = if host == "www.youtube.com" || host == "youtube.com" {
        uri.query_pairs().find(|(k, _)| k == "v").map(|(_, v)| v.into_owned())
    } else if host == "youtu.be" {
        uri.path_segments().and_then(|mut s| s.next()).map(ToOwned::to_owned)
    } else {
        None
    };

    if let Some(id) = video_id {
        Url::parse(&format!("https://www.youtube.com/watch?v={id}")).unwrap_or_else(|_| uri.clone())
    } else {
        uri.clone()
    }
}

/// Validates source URI policy (`http`, `https`, `local`).
fn validate_source_uri(uri: &Url) -> Result<(), MediaPmError> {
    match uri.scheme() {
        "http" | "https" | "local" => Ok(()),
        _ => Err(MediaPmError::InvalidSource(
            "phase-3 supports only http(s) and local:<id> schemes".to_string(),
        )),
    }
}

/// `NanoID` alphabet: URL-safe characters (`A-Za-z0-9_-`), 64 symbols.
const NANOID_ALPHABET: &[u8; 64] =
    b"0123456789ABCDEFGHIJKLMNOPQRSTUVWXYZabcdefghijklmnopqrstuvwxyz_-";

/// Generates an 8-character `NanoID` using the thread-local RNG.
///
/// The ID is drawn from the 64-symbol URL-safe alphabet and is suitable for
/// stable user-facing identifiers.  Each call produces an independent,
/// non-deterministic value; test code that requires determinism must use a
/// seeded `StdRng` with the `NANOID_ALPHABET` constant directly.
fn nanoid_8() -> String {
    let mut rng = rand::rng();
    (0..8)
        .map(|_| {
            let idx = (rng.random::<u8>() & 0x3F) as usize;
            NANOID_ALPHABET[idx] as char
        })
        .collect()
}

/// Derives a yt-dlp media id from a canonical source URI.
///
/// For `YouTube` (`www.youtube.com` / `youtube.com` / `youtu.be`), the id is
/// `youtube.<video_id>` using the `v=` query parameter so the identifier is
/// stable and human-readable.  For all other hosts the id falls back to
/// `<host_slug>.<content_hash_12>` where the hash provides collision
/// resistance.
fn media_id_from_uri(uri: &Url) -> String {
    let host = uri.host_str().unwrap_or("");
    if host == "www.youtube.com" || host == "youtube.com" {
        if let Some((_, video_id)) = uri.query_pairs().find(|(k, _)| k == "v") {
            return format!("youtube.{video_id}");
        }
    } else if host == "youtu.be"
        && let Some(video_id) = uri.path_segments().and_then(|mut s| s.next())
        && !video_id.is_empty()
    {
        return format!("youtube.{video_id}");
    }
    // Generic fallback: domain slug + 12-char content hash for stability.
    let host_slug = host.trim_start_matches("www.").replace('.', "-");
    let hash = mediapm_cas::Hash::from_content(uri.as_str().as_bytes()).to_hex();
    format!("{host_slug}.{}", &hash[..12])
}

/// Derives a stable local media id for a new local source registration.
///
/// Each call returns a fresh 8-character `NanoID` so that multiple imports of
/// different files never collide even when the file names are identical.
/// The `local.` prefix makes the id preset visible in config files.
fn media_id_from_local_path(_path: &Path) -> String {
    format!("local.{}", nanoid_8())
}

/// Merges config-declared runtime storage with service-level overrides.
///
/// Precedence order is: service override (for example CLI flag) >
/// `mediapm.ncl` value > built-in default.
#[must_use]
fn merge_runtime_storage(
    config_value: &MediaRuntimeStorage,
    override_value: &MediaRuntimeStorage,
) -> MediaRuntimeStorage {
    let merged_inherited_env_vars = merge_platform_inherited_env_var_maps(
        config_value.inherited_env_vars.as_ref(),
        override_value.inherited_env_vars.as_ref(),
    );

    MediaRuntimeStorage {
        mediapm_dir: override_value
            .mediapm_dir
            .clone()
            .or_else(|| config_value.mediapm_dir.clone()),
        hierarchy_root_dir: override_value
            .hierarchy_root_dir
            .clone()
            .or_else(|| config_value.hierarchy_root_dir.clone()),
        mediapm_tmp_dir: override_value
            .mediapm_tmp_dir
            .clone()
            .or_else(|| config_value.mediapm_tmp_dir.clone()),
        materialization_preference_order: override_value
            .materialization_preference_order
            .clone()
            .or_else(|| config_value.materialization_preference_order.clone()),
        conductor_config: override_value
            .conductor_config
            .clone()
            .or_else(|| config_value.conductor_config.clone()),
        conductor_machine_config: override_value
            .conductor_machine_config
            .clone()
            .or_else(|| config_value.conductor_machine_config.clone()),
        conductor_state_config: override_value
            .conductor_state_config
            .clone()
            .or_else(|| config_value.conductor_state_config.clone()),
        conductor_tmp_dir: override_value
            .conductor_tmp_dir
            .clone()
            .or_else(|| config_value.conductor_tmp_dir.clone()),
        conductor_schema_dir: override_value
            .conductor_schema_dir
            .clone()
            .or_else(|| config_value.conductor_schema_dir.clone()),
        inherited_env_vars: merged_inherited_env_vars,
        media_state_config: override_value
            .media_state_config
            .clone()
            .or_else(|| config_value.media_state_config.clone()),
        env_file: override_value.env_file.clone().or_else(|| config_value.env_file.clone()),
        mediapm_schema_dir: override_value
            .mediapm_schema_dir
            .clone()
            .or_else(|| config_value.mediapm_schema_dir.clone()),
        use_user_tool_cache: override_value
            .use_user_tool_cache
            .or(config_value.use_user_tool_cache),
    }
}

/// Merges optional platform-keyed inherited env-var maps with deterministic
/// order and case-insensitive de-duplication.
#[must_use]
fn merge_platform_inherited_env_var_maps(
    config_value: Option<&crate::config::PlatformInheritedEnvVars>,
    override_value: Option<&crate::config::PlatformInheritedEnvVars>,
) -> Option<crate::config::PlatformInheritedEnvVars> {
    let mut merged = BTreeMap::<String, Vec<String>>::new();

    for candidate in [config_value, override_value].into_iter().flatten() {
        for (raw_platform, names) in candidate {
            let platform = raw_platform.trim().to_ascii_lowercase();
            if platform.is_empty() {
                continue;
            }

            let entry = merged.entry(platform).or_default();
            append_unique_env_var_names(entry, names);
        }
    }

    merged.retain(|_, names| !names.is_empty());

    if merged.is_empty() { None } else { Some(merged) }
}

/// Appends trimmed environment-variable names with case-insensitive
/// de-duplication while preserving first-seen casing and order.
fn append_unique_env_var_names(target: &mut Vec<String>, source: &[String]) {
    for raw_name in source {
        let trimmed = raw_name.trim();
        if trimmed.is_empty() {
            continue;
        }
        if target.iter().any(|existing: &String| existing.eq_ignore_ascii_case(trimmed)) {
            continue;
        }
        target.push(trimmed.to_string());
    }
}

/// Builds conductor runtime options from resolved Phase 3 paths.
///
/// `mediapm` always provides grouped runtime-storage paths explicitly when it
/// invokes conductor so phase-2 runtime writes (volatile state + CAS store)
/// stay aligned with effective phase-3 path policy rather than falling back to
/// standalone conductor defaults under `.conductor/`.
#[must_use]
fn conductor_run_workflow_options(
    paths: &MediaPmPaths,
    runtime_storage: &MediaRuntimeStorage,
) -> RunWorkflowOptions {
    RunWorkflowOptions {
        runtime_storage_paths: RuntimeStoragePaths {
            conductor_dir: paths.runtime_root.clone(),
            conductor_state_config: Some(paths.conductor_state_config.clone()),
            cas_store_dir: Some(paths.runtime_root.join("store")),
            conductor_tmp_dir: Some(paths.conductor_tmp_dir.clone()),
            conductor_schema_dir: Some(paths.conductor_schema_dir.clone()),
        },
        runtime_inherited_env_vars: runtime_storage.inherited_env_vars_with_defaults(),
        ..RunWorkflowOptions::default()
    }
}

/// Derives a fallback local title from one source path.
fn local_default_title(path: &Path) -> String {
    path.file_name()
        .map_or_else(|| path.display().to_string(), |value| value.to_string_lossy().to_string())
}

/// Builds default description for one local media source.
fn build_local_default_description(path: &Path, title: &str) -> String {
    let file_name = local_default_title(path);
    let mut lines = vec![format!("file: {file_name}")];
    lines.push(format!("title: {title}"));
    lines.push("artist: unknown".to_string());
    lines.join("\n")
}

/// Resolves one local file extension value with a leading dot.
///
/// Missing extensions fall back to `.bin` so hierarchy interpolation keys can
/// remain defined for all local sources added through `media add --preset local`.
fn local_extension_with_dot(path: &Path) -> String {
    path.extension()
        .and_then(|value| value.to_str())
        .map(str::trim)
        .filter(|value| !value.is_empty())
        .map_or_else(|| ".bin".to_string(), |value| format!(".{value}"))
}

/// Builds default managed transform chain for one local-source CAS hash.
///
/// The generated chain keeps local ingest semantics aligned with
/// `media add --preset local` defaults:
/// `import -> media-tagger -> rsgain`, while reusing one stable variant key
/// across the full pipeline.
#[must_use]
fn local_source_default_steps(hash_text: &str) -> Vec<MediaStep> {
    vec![
        MediaStep {
            tool: MediaStepTool::Import,
            input_variants: Vec::new(),
            output_variants: BTreeMap::from([(
                "media".to_string(),
                serde_json::json!({
                    "kind": "primary",
                }),
            )]),
            options: BTreeMap::from([
                ("kind".to_string(), TransformInputValue::String("cas_hash".to_string())),
                ("hash".to_string(), TransformInputValue::String(hash_text.to_string())),
            ]),
        },
        MediaStep {
            tool: MediaStepTool::MediaTagger,
            input_variants: vec!["media".to_string()],
            output_variants: BTreeMap::from([(
                "media".to_string(),
                serde_json::json!({
                    "kind": "primary",
                }),
            )]),
            options: BTreeMap::new(),
        },
        MediaStep {
            tool: MediaStepTool::Rsgain,
            input_variants: vec!["media".to_string()],
            output_variants: BTreeMap::from([(
                "media".to_string(),
                serde_json::json!({
                    "kind": "primary",
                }),
            )]),
            options: BTreeMap::new(),
        },
    ]
}

/// Builds default description for one remote media source.
fn build_remote_default_description(title: &str, artist: Option<&str>) -> String {
    let artist = artist.map(str::trim).filter(|value| !value.is_empty()).unwrap_or("unknown");
    format!("title: {title}\nartist: {artist}")
}

/// Metadata resolved from a `MusicBrainz` recording ID.
struct MbRecordingMetadata {
    /// Recording title.
    title: String,
    /// Combined artist credit text (may be `"unknown"` when absent).
    artist: String,
}

/// Validates that `id` is a well-formed UUID (`xxxxxxxx-xxxx-xxxx-xxxx-xxxxxxxxxxxx`).
fn validate_recording_id_format(id: &str) -> Result<(), MediaPmError> {
    let parts: Vec<&str> = id.split('-').collect();
    let valid = parts.len() == 5
        && parts[0].len() == 8
        && parts[1].len() == 4
        && parts[2].len() == 4
        && parts[3].len() == 4
        && parts[4].len() == 12
        && id.chars().all(|c| c.is_ascii_hexdigit() || c == '-');
    if valid {
        Ok(())
    } else {
        Err(MediaPmError::Workflow(format!(
            "recording id '{id}' is not a valid UUID (expected 8-4-4-4-12 lowercase hex)"
        )))
    }
}

/// Fetches and validates a `MusicBrainz` recording, returning title and artist credit.
///
/// # Errors
///
/// Returns [`MediaPmError`] when the recording id is not a valid UUID or the
/// `MusicBrainz` API call fails (network error, unknown id, etc.).
async fn fetch_mb_recording_metadata(
    recording_id: &str,
) -> Result<MbRecordingMetadata, MediaPmError> {
    validate_recording_id_format(recording_id)?;
    let recording =
        Recording::fetch().id(recording_id).with_artists().execute_async().await.map_err(|e| {
            MediaPmError::Workflow(format!(
                "MusicBrainz lookup for recording '{recording_id}' failed: {e}"
            ))
        })?;
    let title = recording.title.clone();
    let artist = recording
        .artist_credit
        .as_deref()
        .filter(|credits| !credits.is_empty())
        .map(|credits| {
            let mut combined = String::new();
            for credit in credits {
                combined.push_str(&credit.name);
                if let Some(join_phrase) = credit.joinphrase.as_deref() {
                    combined.push_str(join_phrase);
                }
            }
            combined
        })
        .filter(|s| !s.trim().is_empty())
        .unwrap_or_else(|| "unknown".to_string());
    Ok(MbRecordingMetadata { title, artist })
}

/// Metadata tuple fetched by downloader-aware online probes.
#[derive(Debug, Clone, PartialEq, Eq, Default)]
struct OnlineSourceMetadata {
    /// Best-effort media title.
    title: Option<String>,
    /// Best-effort artist/uploader label.
    artist: Option<String>,
    /// Best-effort textual description.
    description: Option<String>,
}

/// Metadata tuple fetched by local-file probes.
#[derive(Debug, Clone, PartialEq, Eq, Default)]
struct LocalSourceMetadata {
    /// Best-effort media title.
    title: Option<String>,
    /// Best-effort textual description.
    description: Option<String>,
}

/// Resolves online metadata using downloader tools when available.
fn fetch_online_source_metadata(uri: &Url) -> OnlineSourceMetadata {
    try_fetch_online_source_metadata_with_yt_dlp(uri).unwrap_or_default()
}

/// Resolves local metadata using media-probe tooling when available.
fn fetch_local_source_metadata(path: &Path) -> LocalSourceMetadata {
    try_fetch_local_source_metadata_with_ffprobe(path).unwrap_or_default()
}

/// Fetches online metadata by invoking `yt-dlp` when present on PATH.
fn try_fetch_online_source_metadata_with_yt_dlp(uri: &Url) -> Option<OnlineSourceMetadata> {
    let output = ProcessCommand::new("yt-dlp")
        .arg("--dump-single-json")
        .arg("--skip-download")
        .arg("--no-warnings")
        .arg(uri.as_str())
        .output()
        .ok()?;

    if !output.status.success() {
        return None;
    }

    let value: serde_json::Value = serde_json::from_slice(&output.stdout).ok()?;
    let metadata = parse_online_source_metadata(&value);

    if metadata.title.is_none() && metadata.artist.is_none() && metadata.description.is_none() {
        None
    } else {
        Some(metadata)
    }
}

/// Fetches local metadata by invoking `ffprobe` when present on PATH.
fn try_fetch_local_source_metadata_with_ffprobe(path: &Path) -> Option<LocalSourceMetadata> {
    let output = ProcessCommand::new("ffprobe")
        .arg("-v")
        .arg("error")
        .arg("-print_format")
        .arg("json")
        .arg("-show_format")
        .arg(path)
        .output()
        .ok()?;

    if !output.status.success() {
        return None;
    }

    let value: serde_json::Value = serde_json::from_slice(&output.stdout).ok()?;
    let metadata = parse_local_source_metadata_from_ffprobe_json(&value);

    if metadata.title.is_none() && metadata.description.is_none() { None } else { Some(metadata) }
}

/// Parses online metadata fields from one downloader JSON payload.
fn parse_online_source_metadata(value: &serde_json::Value) -> OnlineSourceMetadata {
    let title = first_non_empty_json_string(value, &["fulltitle", "title", "track"]);
    let artist = first_non_empty_json_string(
        value,
        &["uploader", "channel", "artist", "creator", "uploader_id"],
    );
    let description = first_non_empty_json_string(value, &["description", "summary"]);

    OnlineSourceMetadata { title, artist, description }
}

/// Parses local metadata fields from one ffprobe JSON payload.
fn parse_local_source_metadata_from_ffprobe_json(value: &serde_json::Value) -> LocalSourceMetadata {
    let tags = value
        .get("format")
        .and_then(|format| format.get("tags"))
        .cloned()
        .unwrap_or(serde_json::Value::Null);

    let title = first_non_empty_json_string(&tags, &["title", "track"]);
    let description = first_non_empty_json_string(&tags, &["description", "comment", "synopsis"]);

    LocalSourceMetadata { title, description }
}

/// Returns first non-empty string value from one JSON object key list.
fn first_non_empty_json_string(value: &serde_json::Value, keys: &[&str]) -> Option<String> {
    keys.iter().find_map(|key| {
        value
            .get(*key)
            .or_else(|| {
                value.as_object().and_then(|object| {
                    object.iter().find_map(|(candidate, candidate_value)| {
                        if candidate.eq_ignore_ascii_case(key) {
                            Some(candidate_value)
                        } else {
                            None
                        }
                    })
                })
            })
            .and_then(serde_json::Value::as_str)
            .map(str::trim)
            .filter(|text| !text.is_empty())
            .map(ToString::to_string)
    })
}

/// Derives a human-readable title for one remote source URL.
fn remote_default_title(uri: &Url) -> String {
    uri.path_segments()
        .and_then(|mut segments| segments.rfind(|segment| !segment.is_empty()))
        .map(ToString::to_string)
        .filter(|title| !title.trim().is_empty())
        .unwrap_or_else(|| "unknown".to_string())
}

/// Resolves conductor CAS root from machine runtime storage with default fallback.
fn resolve_conductor_cas_root(paths: &MediaPmPaths, machine: &MachineNickelDocument) -> PathBuf {
    if let Some(raw) = machine.runtime.cas_store_dir.as_deref() {
        let candidate = PathBuf::from(raw);
        if candidate.is_absolute() { candidate } else { paths.root_dir.join(candidate) }
    } else {
        paths.runtime_root.join("store")
    }
}

/// Executes workflows with a filesystem-backed conductor rooted at one CAS
/// store path.
///
/// This path is used when workflow steps need payload hashes imported into the
/// persistent runtime CAS store during tool reconciliation.
async fn run_workflow_with_filesystem_cas(
    conductor_cas_root: &Path,
    user_ncl: &Path,
    machine_ncl: &Path,
    options: RunWorkflowOptions,
) -> Result<RunSummary, MediaPmError> {
    let cas = FileSystemCas::open(conductor_cas_root).await.map_err(|source| {
        MediaPmError::Workflow(format!(
            "opening conductor CAS store '{}' for workflow execution failed: {source}",
            conductor_cas_root.display()
        ))
    })?;
    let conductor = SimpleConductor::new(cas);
    conductor.run_workflow_with_options(user_ncl, machine_ncl, options).await.map_err(Into::into)
}

/// Returns true when workflow execution should run directly against
/// filesystem-backed CAS instead of an in-memory conductor backend.
///
/// Managed executable tools persist runtime `content_map` hashes in the
/// resolved conductor CAS store during tool reconciliation. Running workflow
/// execution with in-memory CAS in that state would force a fail-then-retry
/// fallback path and duplicate workflow progress output.
#[must_use]
fn should_prefer_filesystem_workflow_runner(machine: &MachineNickelDocument) -> bool {
    machine
        .tool_configs
        .values()
        .any(|config| config.content_map.as_ref().is_some_and(|map| !map.is_empty()))
}

/// Returns true when conductor workflow execution should retry on filesystem CAS.
///
/// The default in-memory conductor used by high-level `mediapm` service
/// constructors cannot resolve hashes imported into the persistent runtime
/// store during tool reconciliation. When that mismatch surfaces as a
/// deterministic missing-object CAS error, sync falls back to a temporary
/// filesystem-backed conductor bound to the resolved runtime store.
fn should_retry_workflow_with_filesystem_cas(error: &ConductorError) -> bool {
    let text = error.to_string();
    text.contains("cas operation failed") && text.contains("object not found")
}

/// Produces warnings for stale safety-external-data entries.
/// Returns current Unix timestamp in seconds.
#[cfg(test)]
mod tests {
    use std::collections::BTreeMap;
    use std::fs;

    use mediapm_cas::Hash;
    use mediapm_conductor::{MachineNickelDocument, ToolConfigSpec};
    use serde_json::json;

    use super::{
        HierarchyNodeKind, LocalSourceMetadata, MediaHierarchyPreset, MediaPmApi, MediaPmService,
        MediaRuntimeStorage, OnlineSourceMetadata, load_mediapm_document, merge_runtime_storage,
        parse_local_source_metadata_from_ffprobe_json, parse_online_source_metadata,
        should_prefer_filesystem_workflow_runner, validate_source_uri,
    };
    use tempfile::tempdir;
    use url::Url;

    /// Ensures scheme validation allows online and local URI inputs.
    #[test]
    fn source_scheme_validation_matches_phase3_policy() {
        let http = Url::parse("https://example.com/video.mkv").expect("url");
        let local = Url::parse("local:media-id").expect("url");

        assert!(validate_source_uri(&http).is_ok());
        assert!(validate_source_uri(&local).is_ok());
    }

    /// Ensures unsupported schemes are rejected.
    #[test]
    fn source_scheme_validation_rejects_unsupported_schemes() {
        let ftp = Url::parse("ftp://example.com/video.mkv").expect("url");
        assert!(validate_source_uri(&ftp).is_err());
    }

    /// Ensures sync bootstraps default docs and state on a fresh workspace.
    #[tokio::test]
    async fn sync_library_bootstraps_default_phase3_state_files() {
        let root = tempdir().expect("tempdir");
        let service = MediaPmService::new_in_memory_at(root.path());

        let _ = service.sync_library().await.expect("sync");

        assert!(service.paths().mediapm_ncl.exists());
        assert!(service.paths().conductor_user_ncl.exists());
        assert!(service.paths().conductor_machine_ncl.exists());
        assert!(service.paths().mediapm_state_ncl.exists());
        assert!(service.paths().runtime_root.join(".env").exists());
        assert!(service.paths().runtime_root.join(".env.generated").exists());
        assert!(service.paths().runtime_root.join(".gitignore").exists());

        let dotenv_text =
            fs::read_to_string(service.paths().runtime_root.join(".env")).expect("read .env");
        assert!(dotenv_text.contains("# conductor runtime environment variables"));
        assert!(dotenv_text.contains("# MEDIAPM_CONDUCTOR_EXECUTABLE_TIMEOUT_SECS="));
        assert!(dotenv_text.contains("# MEDIAPM_DOWNLOAD_TIMEOUT_SECONDS="));
        assert!(dotenv_text.contains("# ACOUSTID_API_KEY="));
        assert!(dotenv_text.contains("# MEDIAPM_MEDIA_TAGGER_FFMPEG_BIN="));

        let dotenv_generated_text =
            fs::read_to_string(service.paths().runtime_root.join(".env.generated"))
                .expect("read .env.generated");
        assert!(dotenv_generated_text.contains("# @generated by mediapm tool sync"));

        let gitignore_text = fs::read_to_string(service.paths().runtime_root.join(".gitignore"))
            .expect("read runtime .gitignore");
        assert!(gitignore_text.contains("/.env"));
        assert!(gitignore_text.contains("/.env.generated"));

        let schema_dir =
            service.paths().schema_export_dir.as_ref().expect("default schema export dir");
        assert!(schema_dir.join("mod.ncl").exists());
        assert!(schema_dir.join("v1.ncl").exists());

        let conductor_schema_dir = service.paths().conductor_schema_dir.clone();
        assert!(conductor_schema_dir.join("mod.ncl").exists());
        assert!(conductor_schema_dir.join("v1.ncl").exists());
    }

    /// Ensures tools-only sync bootstraps documents without running workflows.
    #[tokio::test]
    async fn sync_tools_bootstraps_default_state_files() {
        let root = tempdir().expect("tempdir");
        let service = MediaPmService::new_in_memory_at(root.path());

        let summary = service.sync_tools().await.expect("tool sync");

        assert_eq!(summary.added_tools, 0);
        assert_eq!(summary.updated_tools, 0);
        assert_eq!(summary.unchanged_tools, 0);
        assert!(service.paths().mediapm_ncl.exists());
        assert!(service.paths().conductor_user_ncl.exists());
        assert!(service.paths().conductor_machine_ncl.exists());
        assert!(service.paths().mediapm_state_ncl.exists());
        assert!(service.paths().runtime_root.join(".env").exists());
        assert!(service.paths().runtime_root.join(".env.generated").exists());
        assert!(service.paths().runtime_root.join(".gitignore").exists());

        let dotenv_text =
            fs::read_to_string(service.paths().runtime_root.join(".env")).expect("read .env");
        assert!(dotenv_text.contains("# conductor runtime environment variables"));
        assert!(dotenv_text.contains("# MEDIAPM_CONDUCTOR_EXECUTABLE_TIMEOUT_SECS="));
        assert!(dotenv_text.contains("# MEDIAPM_DOWNLOAD_TIMEOUT_SECONDS="));
        assert!(dotenv_text.contains("# ACOUSTID_API_KEY="));
        assert!(dotenv_text.contains("# MEDIAPM_MEDIA_TAGGER_FFMPEG_BIN="));

        let dotenv_generated_text =
            fs::read_to_string(service.paths().runtime_root.join(".env.generated"))
                .expect("read .env.generated");
        assert!(dotenv_generated_text.contains("# @generated by mediapm tool sync"));

        let gitignore_text = fs::read_to_string(service.paths().runtime_root.join(".gitignore"))
            .expect("read runtime .gitignore");
        assert!(gitignore_text.contains("/.env"));
        assert!(gitignore_text.contains("/.env.generated"));

        let schema_dir =
            service.paths().schema_export_dir.as_ref().expect("default schema export dir");
        assert!(schema_dir.join("mod.ncl").exists());
        assert!(schema_dir.join("v1.ncl").exists());

        let conductor_schema_dir = service.paths().conductor_schema_dir.clone();
        assert!(conductor_schema_dir.join("mod.ncl").exists());
        assert!(conductor_schema_dir.join("v1.ncl").exists());
    }

    /// Ensures explicit `runtime.mediapm_schema_dir = null` disables schema
    /// file export during sync.
    #[tokio::test]
    async fn sync_tools_skips_schema_export_when_runtime_schema_dir_is_null() {
        let root = tempdir().expect("tempdir");
        let service = MediaPmService::new_in_memory_at_with_runtime_storage_overrides(
            root.path(),
            MediaRuntimeStorage {
                mediapm_schema_dir: Some(None),
                ..MediaRuntimeStorage::default()
            },
        );

        let summary = service.sync_tools().await.expect("tool sync");

        assert_eq!(summary.added_tools, 0);
        assert_eq!(summary.updated_tools, 0);
        assert_eq!(summary.unchanged_tools, 0);
        assert!(!root.path().join(".mediapm").join("config").join("mediapm").exists());
        let conductor_schema_dir = root.path().join(".mediapm").join("config").join("conductor");
        assert!(conductor_schema_dir.join("mod.ncl").exists());
        assert!(conductor_schema_dir.join("v1.ncl").exists());
    }

    /// Ensures local hierarchy preset insertion is idempotent for one
    /// `(media, folder)` target and emits the expected folder tree.
    #[tokio::test]
    async fn add_local_hierarchy_preset_is_idempotent_for_existing_media() {
        let root = tempdir().expect("tempdir");
        let service = MediaPmService::new_in_memory_at(root.path());
        let local_file = root.path().join("local-source.txt");
        fs::write(&local_file, b"local-bytes").expect("write local source");
        let folder = "music videos";

        let media_id = service.add_local_source(&local_file, None).await.expect("add local source");

        service
            .add_media_hierarchy_preset(MediaHierarchyPreset::Local, &media_id, folder)
            .expect("first hierarchy preset insertion should succeed");
        service
            .add_media_hierarchy_preset(MediaHierarchyPreset::Local, &media_id, folder)
            .expect("second hierarchy preset insertion should remain idempotent");

        let document =
            load_mediapm_document(&service.paths().mediapm_ncl).expect("load mediapm document");

        let matching_nodes: Vec<_> = document
            .hierarchy
            .iter()
            .filter(|node| {
                node.kind == HierarchyNodeKind::Folder
                    && node.path == folder
                    && node.media_id.as_deref() == Some(media_id.as_str())
                    && node.children.len() == 1
            })
            .collect();

        assert_eq!(
            matching_nodes.len(),
            1,
            "local hierarchy preset should exist exactly once for one media id/folder"
        );
        assert!(matching_nodes[0].id.is_none(), "outer hierarchy folder should not carry an id");
        let media_root = &matching_nodes[0].children[0];
        assert_eq!(
            media_root.id.as_deref(),
            Some(media_id.as_str()),
            "inner media-root folder should use the media id"
        );
        assert_eq!(
            media_root.path, "${media.metadata.title} [${media.id}]",
            "local hierarchy preset should keep stable media-root template"
        );
        let variants: Vec<_> =
            media_root.children.iter().map(|node| node.variant.as_deref().unwrap_or("")).collect();
        assert_eq!(variants, vec!["media"]);
        assert_eq!(
            media_root.children[0].id.as_deref(),
            Some(format!("{media_id}.media").as_str())
        );
    }

    /// Ensures yt-dlp hierarchy preset adds infojson projection while keeping
    /// the same media-root style as the online demo (without sidecars folder).
    #[tokio::test]
    async fn add_yt_dlp_hierarchy_preset_includes_infojson_projection() {
        let root = tempdir().expect("tempdir");
        let service = MediaPmService::new_in_memory_at(root.path());
        let media_id = service
            .add_media_source(&Url::parse("https://example.com/video").expect("url"), None)
            .await
            .expect("add remote source");

        service
            .add_media_hierarchy_preset(MediaHierarchyPreset::YtDlp, &media_id, "music videos")
            .expect("add yt-dlp hierarchy preset");

        let document =
            load_mediapm_document(&service.paths().mediapm_ncl).expect("load mediapm document");
        let media_root = document
            .hierarchy
            .iter()
            .find(|node| {
                node.kind == HierarchyNodeKind::Folder
                    && node.path == "music videos"
                    && node.media_id.as_deref() == Some(media_id.as_str())
            })
            .and_then(|node| node.children.first())
            .expect("yt-dlp preset should create media-root child folder");

        let variants: std::collections::BTreeSet<_> = media_root
            .children
            .iter()
            .flat_map(|node| {
                let mut values = Vec::new();
                if let Some(variant) = node.variant.as_deref() {
                    values.push(variant.to_string());
                }
                values.extend(node.variants.iter().cloned());
                values
            })
            .collect();
        assert_eq!(
            variants,
            std::collections::BTreeSet::from([
                "archive".to_string(),
                "description".to_string(),
                "infojson".to_string(),
                "links".to_string(),
                "subtitles".to_string(),
                "thumbnails".to_string(),
                "video".to_string(),
            ])
        );

        let variant_ids: std::collections::BTreeSet<_> = media_root
            .children
            .iter()
            .map(|node| node.id.as_deref().unwrap_or("").to_string())
            .collect();
        assert_eq!(
            variant_ids,
            std::collections::BTreeSet::from([
                format!("{media_id}.archive"),
                format!("{media_id}.description"),
                format!("{media_id}.infojson"),
                format!("{media_id}.links"),
                format!("{media_id}.subtitles"),
                format!("{media_id}.thumbnails"),
                format!("{media_id}.thumbnails.folder"),
                format!("{media_id}.video"),
            ])
        );
    }

    /// Ensures hierarchy preset insertion fails for unknown media ids.
    #[test]
    fn add_hierarchy_preset_rejects_unknown_media_id() {
        let root = tempdir().expect("tempdir");
        let service = MediaPmService::new_in_memory_at(root.path());

        let error = service
            .add_media_hierarchy_preset(
                MediaHierarchyPreset::Local,
                "missing-media",
                "music videos",
            )
            .expect_err("unknown media id should be rejected");

        assert!(
            error.to_string().contains(
                "cannot add local hierarchy preset: media id 'missing-media' does not exist"
            ),
            "error should explain missing media id"
        );
    }

    /// Ensures hierarchy preset removal is idempotent for one media/folder.
    #[tokio::test]
    async fn remove_hierarchy_preset_is_idempotent() {
        let root = tempdir().expect("tempdir");
        let service = MediaPmService::new_in_memory_at(root.path());
        let local_file = root.path().join("local-source.txt");
        fs::write(&local_file, b"local-bytes").expect("write local source");
        let folder = "music videos";

        let media_id = service.add_local_source(&local_file, None).await.expect("add local source");
        service
            .add_media_hierarchy_preset(MediaHierarchyPreset::Local, &media_id, folder)
            .expect("add hierarchy preset");

        let removed_first = service
            .remove_media_hierarchy_preset(MediaHierarchyPreset::Local, &media_id, folder)
            .expect("first hierarchy-preset removal should succeed");
        let removed_second = service
            .remove_media_hierarchy_preset(MediaHierarchyPreset::Local, &media_id, folder)
            .expect("second hierarchy-preset removal should remain idempotent");

        assert_eq!(removed_first, 1, "first removal should remove one node");
        assert_eq!(removed_second, 0, "second removal should remove zero nodes");
    }

    /// Ensures media-source removal drops matching hierarchy nodes.
    #[tokio::test]
    async fn remove_media_source_removes_matching_hierarchy_nodes() {
        let root = tempdir().expect("tempdir");
        let service = MediaPmService::new_in_memory_at(root.path());
        let local_file = root.path().join("local-source.txt");
        fs::write(&local_file, b"local-bytes").expect("write local source");

        let media_id = service.add_local_source(&local_file, None).await.expect("add local source");
        service
            .add_media_hierarchy_preset(MediaHierarchyPreset::Local, &media_id, "music videos")
            .expect("add hierarchy preset");

        let removed_hierarchy_nodes =
            service.remove_media_source(&media_id).expect("remove media source");
        assert_eq!(
            removed_hierarchy_nodes, 1,
            "media-source removal should cascade one matching hierarchy node"
        );

        let document =
            load_mediapm_document(&service.paths().mediapm_ncl).expect("load mediapm document");
        assert!(!document.media.contains_key(&media_id), "removed media id should no longer exist");
        assert!(
            document.hierarchy.iter().all(|node| node.media_id.as_deref() != Some(&media_id)),
            "matching hierarchy nodes should also be removed"
        );
    }

    /// Ensures media-source removal rejects unknown media ids.
    #[test]
    fn remove_media_source_rejects_unknown_media_id() {
        let root = tempdir().expect("tempdir");
        let service = MediaPmService::new_in_memory_at(root.path());

        let error = service
            .remove_media_source("missing-media")
            .expect_err("unknown media id should be rejected");

        assert!(
            error
                .to_string()
                .contains("cannot remove media source: media id 'missing-media' does not exist"),
            "error should explain missing media id"
        );
    }

    /// Ensures service-level runtime overrides take precedence for cache toggle
    /// when computing effective runtime-storage policy.
    #[test]
    fn merge_runtime_storage_prefers_override_cache_toggle() {
        let config = MediaRuntimeStorage {
            env_file: Some("config.env".to_string()),
            inherited_env_vars: Some(BTreeMap::from([(
                "windows".to_string(),
                vec!["SYSTEMROOT".to_string(), "PATH".to_string()],
            )])),
            use_user_tool_cache: Some(true),
            ..MediaRuntimeStorage::default()
        };
        let override_value = MediaRuntimeStorage {
            env_file: Some("override.env".to_string()),
            inherited_env_vars: Some(BTreeMap::from([
                ("WINDOWS".to_string(), vec!["path".to_string(), "TMPDIR".to_string()]),
                ("linux".to_string(), vec!["LD_LIBRARY_PATH".to_string()]),
            ])),
            use_user_tool_cache: Some(false),
            ..MediaRuntimeStorage::default()
        };

        let merged = merge_runtime_storage(&config, &override_value);

        assert_eq!(merged.env_file.as_deref(), Some("override.env"));
        assert_eq!(merged.use_user_tool_cache, Some(false));
        assert!(!merged.use_user_tool_cache_enabled());
        assert_eq!(
            merged.inherited_env_vars,
            Some(BTreeMap::from([
                ("linux".to_string(), vec!["LD_LIBRARY_PATH".to_string()],),
                (
                    "windows".to_string(),
                    vec!["SYSTEMROOT".to_string(), "PATH".to_string(), "TMPDIR".to_string(),],
                ),
            ]))
        );
    }

    /// Ensures absent cache-toggle values keep the default shared-cache policy
    /// enabled after runtime-storage merging.
    #[test]
    fn merge_runtime_storage_defaults_cache_toggle_enabled_when_absent() {
        let merged =
            merge_runtime_storage(&MediaRuntimeStorage::default(), &MediaRuntimeStorage::default());

        assert!(merged.use_user_tool_cache_enabled());
    }

    /// Ensures online metadata parsing extracts title/artist/description when
    /// downloader JSON includes those fields.
    #[test]
    fn parse_online_metadata_reads_title_artist_and_description() {
        let payload = json!({
            "fulltitle": "Demo Song",
            "uploader": "Demo Artist",
            "description": "A short description"
        });

        let metadata = parse_online_source_metadata(&payload);
        assert_eq!(
            metadata,
            OnlineSourceMetadata {
                title: Some("Demo Song".to_string()),
                artist: Some("Demo Artist".to_string()),
                description: Some("A short description".to_string()),
            }
        );
    }

    /// Ensures local metadata parsing extracts title/description from ffprobe
    /// `format.tags` payloads with case-insensitive key matching.
    #[test]
    fn parse_local_metadata_reads_ffprobe_tags_case_insensitively() {
        let payload = json!({
            "format": {
                "tags": {
                    "TITLE": "Local Demo",
                    "Comment": "Local description"
                }
            }
        });

        let metadata = parse_local_source_metadata_from_ffprobe_json(&payload);
        assert_eq!(
            metadata,
            LocalSourceMetadata {
                title: Some("Local Demo".to_string()),
                description: Some("Local description".to_string()),
            }
        );
    }

    /// Ensures workflow execution prefers filesystem CAS when managed runtime
    /// tool configs reference persisted payload hashes.
    #[test]
    fn prefer_filesystem_workflow_runner_when_content_map_hashes_exist() {
        let machine = MachineNickelDocument {
            tool_configs: BTreeMap::from([(
                "mediapm.tools.ffmpeg+github-releases-btbn-ffmpeg-builds@latest".to_string(),
                ToolConfigSpec {
                    content_map: Some(BTreeMap::from([(
                        "./".to_string(),
                        Hash::from_content(b"payload"),
                    )])),
                    ..ToolConfigSpec::default()
                },
            )]),
            ..MachineNickelDocument::default()
        };

        assert!(should_prefer_filesystem_workflow_runner(&machine));
    }

    /// Ensures workflow execution keeps existing conductor backend when no
    /// managed runtime payload hashes are configured.
    #[test]
    fn prefer_filesystem_workflow_runner_is_false_without_content_map_hashes() {
        let machine = MachineNickelDocument {
            tool_configs: BTreeMap::from([(
                "mediapm.tools.ffmpeg+github-releases-btbn-ffmpeg-builds@latest".to_string(),
                ToolConfigSpec::default(),
            )]),
            ..MachineNickelDocument::default()
        };

        assert!(!should_prefer_filesystem_workflow_runner(&machine));
    }
}
