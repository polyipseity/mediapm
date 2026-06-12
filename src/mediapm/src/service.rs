//! `MediaPmApi` trait and `MediaPmService` generic implementation.
//!
//! # Module structure note
//!
//! This file intentionally remains as a single module despite exceeding 1 000
//! lines. Almost all logic lives inside `impl<C> MediaPmService<C>` whose
//! methods take `&self` or `&mut self`. Splitting `impl` methods across files
//! requires non-idiomatic `include!()`, so the module is kept whole.

use std::collections::BTreeMap;
use std::future::Future;
use std::path::Path;
use std::process::Command as ProcessCommand;

use mediapm_cas::{CasApi, FileSystemCas, InMemoryCas};
use mediapm_conductor::runtime_env::ensure_runtime_env_files;
use mediapm_conductor::{
    ConductorApi, SimpleConductor, StateMutationOptions, ToolKindSpec, WorkflowStepEvent,
    resolve_managed_tool_executable_with_filesystem_cas,
};
use pulsebar::{MultiProgress, ProgressBar};
use tokio::sync::mpsc;
use url::Url;

use crate::conductor_bridge::ConductorToolRow;
use crate::config::{
    MediaMetadataValue, MediaMetadataValueCandidate, MediaMetadataVariantBinding, MediaPmDocument,
    MediaSourceSpec, MediaStep, MediaStepTool, ToolRequirement, TransformInputValue,
    load_mediapm_document_without_validation, save_mediapm_document,
};
use crate::config::{MediaPmState, load_mediapm_state_document, save_mediapm_state_document};
use crate::error::MediaPmError;
use crate::hierarchy::{
    build_hierarchy_preset_node, default_hierarchy_folder_root_for_preset,
    hierarchy_contains_node_id, hierarchy_preset_node_id, insert_hierarchy_preset_node,
    normalize_hierarchy_folder_root, remove_hierarchy_nodes_by_id,
    remove_hierarchy_nodes_by_media_id,
};
use crate::paths::MediaPmPaths;
pub use crate::service_standalone::*;
use crate::source_metadata::{
    fetch_local_source_metadata, fetch_online_source_metadata, resolve_conductor_cas_root,
    resolve_online_source_metadata_for_add,
};
use crate::{
    AddInsertPosition, MediaHierarchyPreset, MediaPackage, MediaRuntimeStorage,
    MediaStepInvalidationSummary, SyncSummary, ToolsSyncSummary,
};
use crate::{
    build_local_default_description, conductor_run_workflow_options,
    export_mediapm_nickel_config_schemas, load_runtime_dotenv, local_default_title,
    local_extension_with_dot, local_source_default_steps, media_id_from_local_path,
    media_id_from_uri, merge_runtime_storage, normalize_source_uri, validate_source_uri,
};
use crate::{conductor_bridge, config, materializer, tools};

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
    /// Canonical path set for this service instance.
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

    /// Returns canonical paths used by this service.
    #[must_use]
    pub fn paths(&self) -> &MediaPmPaths {
        &self.paths
    }

    /// Resolves effective paths by merging config + service overrides.
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

    /// Returns true when one logical tool requirement likely needs explicit
    /// `mediapm tool sync` reconciliation.
    ///
    /// This check is intentionally local-only: it validates desired-vs-active
    /// selector alignment and required machine rows, but does not perform
    /// remote release lookups.
    fn logical_tool_requires_sync(
        tool_name: &str,
        requirement: &ToolRequirement,
        lock: &MediaPmState,
        machine: &mediapm_conductor::MachineNickelDocument,
    ) -> bool {
        if tools::catalog::tool_catalog_entry(tool_name).is_err() {
            return false;
        }

        let Some(active_tool_id) = lock.active_tools.get(tool_name) else {
            return true;
        };

        let Some(registry_entry) = lock.tool_registry.get(active_tool_id) else {
            return true;
        };

        if !registry_entry.name.eq_ignore_ascii_case(tool_name) {
            return true;
        }

        let Some(tool_spec) = machine.tools.get(active_tool_id) else {
            return true;
        };

        if let Some(required_version) = requirement.normalized_version()
            && config::normalize_selector_compare_value(registry_entry.version.as_str())
                != config::normalize_selector_compare_value(required_version.as_str())
        {
            return true;
        }

        if matches!(&tool_spec.kind, ToolKindSpec::Executable { .. }) {
            let has_content_map = machine
                .tool_configs
                .get(active_tool_id)
                .and_then(|tool_config| tool_config.content_map.as_ref())
                .is_some_and(|content_map| !content_map.is_empty());
            if !has_content_map {
                return true;
            }
        }

        false
    }

    /// Returns sorted logical tool names that likely need explicit
    /// `mediapm tool sync` reconciliation.
    fn collect_tools_requiring_sync(
        document: &MediaPmDocument,
        lock: &MediaPmState,
        machine: &mediapm_conductor::MachineNickelDocument,
    ) -> Vec<String> {
        let mut names = document
            .tools
            .iter()
            .filter(|(tool_name, requirement)| {
                Self::logical_tool_requires_sync(tool_name, requirement, lock, machine)
            })
            .map(|(tool_name, _requirement)| tool_name.clone())
            .collect::<Vec<_>>();
        names.sort();
        names
    }

    /// Adds one warning that hints explicit tool-sync usage when required.
    ///
    /// This warning preserves explicit-policy boundaries: `mediapm sync` does
    /// not auto-run tool reconciliation.
    fn append_tool_sync_hint_warning(warnings: &mut Vec<String>, tools_requiring_sync: &[String]) {
        if tools_requiring_sync.is_empty() {
            return;
        }

        warnings.push(format!(
            "tool state appears outdated for [{}]; run 'mediapm tool sync' to reconcile managed tool binaries/config before rerunning 'mediapm sync'",
            tools_requiring_sync.join(", ")
        ));
    }

    /// Adds one online media source to `mediapm.ncl`.
    ///
    /// `title`, `artist`, and `description` are CLI-level overrides that take
    /// precedence over metadata fetched from the remote source.
    ///
    /// # Errors
    ///
    /// Returns [`MediaPmError`] when source validation fails, config cannot be
    /// loaded/saved, or default source metadata cannot be synthesized.
    #[allow(clippy::too_many_arguments)]
    pub async fn add_media_source(
        &self,
        uri: &Url,
        title: Option<&str>,
        artist: Option<&str>,
        description: Option<&str>,
        album: Option<&str>,
        recording_mbid: Option<&str>,
        release_mbid: Option<&str>,
    ) -> Result<String, MediaPmError> {
        self.add_media_source_with_position(
            uri,
            title,
            artist,
            description,
            album,
            recording_mbid,
            release_mbid,
            AddInsertPosition::Sorted,
            false,
        )
        .await
    }

    /// Adds one online media source to `mediapm.ncl` with one insertion
    /// policy hint for CLI parity.
    ///
    /// `media` registry entries are key-addressed and persisted in sorted key
    /// order, so all insertion modes currently converge to deterministic key
    /// insertion semantics.
    ///
    /// # Errors
    ///
    /// Returns [`MediaPmError`] when source validation fails, config cannot be
    /// loaded/saved, or default source metadata cannot be synthesized.
    #[allow(clippy::too_many_arguments, clippy::too_many_lines)]
    pub async fn add_media_source_with_position(
        &self,
        uri: &Url,
        title: Option<&str>,
        artist: Option<&str>,
        description: Option<&str>,
        album: Option<&str>,
        recording_mbid: Option<&str>,
        release_mbid: Option<&str>,
        _position: AddInsertPosition,
        overwrite: bool,
    ) -> Result<String, MediaPmError> {
        validate_source_uri(uri)?;

        if uri.scheme() == "local" {
            return Err(MediaPmError::Workflow(
                "use 'media add --preset local <path>' for local sources so CAS hash pointers are recorded"
                    .to_string(),
            ));
        }

        let mut document = ensure_and_load_mediapm_document(&self.paths.mediapm_ncl)?;
        let normalized_uri = normalize_source_uri(uri);
        let media_id = media_id_from_uri(&normalized_uri);
        let effective_paths = self.resolve_effective_paths(&document.runtime);
        let yt_dlp_configured = document.tools.contains_key("yt-dlp");
        let (yt_dlp_metadata, warning) = if yt_dlp_configured {
            conductor_bridge::ensure_conductor_documents(&effective_paths)?;
            let machine =
                conductor_bridge::load_machine_document(&effective_paths.conductor_machine_ncl)?;
            let conductor_cas_root = resolve_conductor_cas_root(&effective_paths, &machine);
            match resolve_managed_tool_executable_with_filesystem_cas(
                &effective_paths.conductor_machine_ncl,
                &conductor_cas_root,
                &effective_paths.tools_dir,
                "yt-dlp",
            )
            .await
            {
                Ok(resolved_tool) => {
                    let metadata = fetch_online_source_metadata(
                        &normalized_uri,
                        &resolved_tool.executable_path,
                    );
                    let warning = if metadata.title.is_none()
                        && metadata.artist.is_none()
                        && metadata.description.is_none()
                    {
                        Some(format!(
                            "managed yt-dlp binary at '{}' returned no usable metadata for remote source '{normalized_uri}'",
                            resolved_tool.executable_path.display()
                        ))
                    } else {
                        None
                    };
                    (Some(metadata), warning)
                }
                Err(error) => {
                    let warning = Some(format!(
                        "yt-dlp managed tool is configured but unavailable for metadata fetch: {error}"
                    ));
                    (None, warning)
                }
            }
        } else {
            (
                None,
                Some(format!(
                    "yt-dlp managed tool is not configured; cannot fetch title, description, or artist metadata for remote source '{normalized_uri}'"
                )),
            )
        };
        let resolved_online_metadata =
            resolve_online_source_metadata_for_add(yt_dlp_metadata, warning);
        if let Some(warning) = resolved_online_metadata.warning.as_ref() {
            eprintln!("warning: {warning}");
        }
        let source_title = title
            .map(str::to_string)
            .or(resolved_online_metadata.title)
            .unwrap_or_else(|| "unknown".to_string());
        let source_artist_literal = artist.map(str::to_string).or(resolved_online_metadata.artist);
        let source_description = description
            .map(str::to_string)
            .or(resolved_online_metadata.description)
            .unwrap_or_else(|| {
                format!(
                    "title: {source_title}\nartist: {}",
                    source_artist_literal.as_deref().unwrap_or("unknown")
                )
            });

        // Do-not-overwrite guard: skip insert when entry exists and overwrite is not requested.
        if !overwrite && document.media.contains_key(&media_id) {
            return Ok(media_id);
        }

        document.media.insert(
            media_id.clone(),
            MediaSourceSpec {
                id: None,
                description: Some(source_description),
                title: Some(source_title.clone()),
                artist: source_artist_literal.clone(),
                workflow_id: None,
                metadata: {
                    let mut metadata_map = BTreeMap::new();
                    let mut title_candidates = Vec::new();
                    if let Some(explicit) = title {
                        title_candidates
                            .push(MediaMetadataValueCandidate::Literal(explicit.to_string()));
                    }
                    title_candidates.extend([
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
                    ]);
                    metadata_map.insert(
                        "title".to_string(),
                        MediaMetadataValue::Fallback(title_candidates),
                    );
                    let mut artist_candidates = Vec::new();
                    if let Some(explicit) = artist {
                        artist_candidates
                            .push(MediaMetadataValueCandidate::Literal(explicit.to_string()));
                    }
                    artist_candidates.extend([
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
                            source_artist_literal.as_deref().unwrap_or("unknown").to_string(),
                        ),
                    ]);
                    metadata_map.insert(
                        "artist".to_string(),
                        MediaMetadataValue::Fallback(artist_candidates),
                    );
                    if let Some(album_value) = album {
                        metadata_map.insert(
                            "album".to_string(),
                            MediaMetadataValue::Literal(album_value.to_string()),
                        );
                    }
                    metadata_map.insert(
                        "video_id".to_string(),
                        MediaMetadataValue::Variant(MediaMetadataVariantBinding {
                            variant: "infojson".to_string(),
                            metadata_key: "id".to_string(),
                            transform: None,
                        }),
                    );
                    metadata_map.insert(
                        "video_ext".to_string(),
                        MediaMetadataValue::Literal(".mkv".to_string()),
                    );
                    Some(metadata_map)
                },
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
                            TransformInputValue::String(normalized_uri.to_string()),
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
                        options: BTreeMap::new(),
                    },
                    MediaStep {
                        tool: MediaStepTool::MediaTagger,
                        input_variants: vec!["video".to_string()],
                        output_variants: BTreeMap::from([(
                            "video".to_string(),
                            serde_json::json!({
                                "kind": "primary",
                            }),
                        )]),
                        options: BTreeMap::from([
                            (
                                "recording_mbid".to_string(),
                                TransformInputValue::String(
                                    recording_mbid.unwrap_or("").to_string(),
                                ),
                            ),
                            (
                                "release_mbid".to_string(),
                                TransformInputValue::String(release_mbid.unwrap_or("").to_string()),
                            ),
                        ]),
                    },
                    MediaStep {
                        tool: MediaStepTool::Rsgain,
                        input_variants: vec!["video".to_string()],
                        output_variants: BTreeMap::from([(
                            "video".to_string(),
                            serde_json::json!({
                                "kind": "primary",
                            }),
                        )]),
                        options: BTreeMap::new(),
                    },
                ],
            },
        );

        save_mediapm_document(&self.paths.mediapm_ncl, &document)?;

        let mut lock = load_mediapm_state_document(&effective_paths.mediapm_state_ncl)?;
        conductor_bridge::reconcile_media_workflows_for_config_edits(
            &effective_paths,
            &document,
            &mut lock,
        )?;
        save_mediapm_state_document(&effective_paths.mediapm_state_ncl, &lock)?;

        Ok(media_id)
    }

    /// Adds one local media source to `mediapm.ncl` as an `import`
    /// CAS-hash ingest step.
    ///
    /// `title`, `artist`, and `description` are CLI-level overrides that take
    /// precedence over metadata fetched from the local file.
    ///
    /// # Errors
    ///
    /// Returns [`MediaPmError`] when the local source path cannot be
    /// canonicalized/read, CAS import fails, config cannot be loaded/saved, or
    /// required conductor runtime documents cannot be prepared.
    #[allow(clippy::too_many_arguments)]
    pub async fn add_local_source(
        &self,
        local_path: &Path,
        title: Option<&str>,
        artist: Option<&str>,
        description: Option<&str>,
        album: Option<&str>,
        recording_mbid: Option<&str>,
        release_mbid: Option<&str>,
    ) -> Result<String, MediaPmError> {
        self.add_local_source_with_position(
            local_path,
            title,
            artist,
            description,
            album,
            recording_mbid,
            release_mbid,
            AddInsertPosition::Sorted,
            false,
        )
        .await
    }

    /// Adds one local media source to `mediapm.ncl` with one insertion-policy
    /// hint for CLI parity.
    ///
    /// `media` registry entries are key-addressed and persisted in sorted key
    /// order, so all insertion modes currently converge to deterministic key
    /// insertion semantics.
    ///
    /// # Errors
    ///
    /// Returns [`MediaPmError`] when the local source path cannot be
    /// canonicalized/read, CAS import fails, config cannot be loaded/saved, or
    /// required conductor runtime documents cannot be prepared.
    #[allow(clippy::too_many_arguments, clippy::too_many_lines)]
    pub async fn add_local_source_with_position(
        &self,
        local_path: &Path,
        title: Option<&str>,
        artist: Option<&str>,
        description: Option<&str>,
        album: Option<&str>,
        recording_mbid: Option<&str>,
        release_mbid: Option<&str>,
        _position: AddInsertPosition,
        overwrite: bool,
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
        let cas = FileSystemCas::open_with_alpha_and_integrity(
            &cas_root,
            4,
            self.runtime_storage_overrides.to_cas_integrity_config(),
        )
        .await
        .map_err(|source| {
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

        let media_id = media_id_from_local_path(&hash);
        let mut lock = load_mediapm_state_document(&effective_paths.mediapm_state_ncl)?;
        let managed_ffprobe_path = crate::materializer::metadata::resolve_managed_ffprobe_path(
            &effective_paths,
            &machine,
            &lock,
        );
        let metadata_cache = crate::metadata_cache::MetadataCache::open(
            &effective_paths.workspace_mediapm_cache_dir(),
        )
        .map_err(|e| tracing::warn!("failed to open metadata cache: {e}"))
        .ok();
        let local_metadata = fetch_local_source_metadata(
            &absolute,
            managed_ffprobe_path.as_deref(),
            metadata_cache.as_ref(),
        );
        let source_title = title
            .map(str::to_string)
            .or(local_metadata.title)
            .unwrap_or_else(|| local_default_title(&absolute));
        let source_artist_literal = artist.map(str::to_string).or(local_metadata.artist);
        let source_description =
            description.map(str::to_string).or(local_metadata.description).unwrap_or_else(|| {
                build_local_default_description(
                    &absolute,
                    &source_title,
                    source_artist_literal.as_deref().unwrap_or("unknown"),
                )
            });
        let source_extension_with_dot = local_extension_with_dot(&absolute);
        let hash_text = hash.to_string();

        // Do-not-overwrite guard: skip insert when entry exists and overwrite is not requested.
        if !overwrite && document.media.contains_key(&media_id) {
            return Ok(media_id);
        }

        document.media.insert(
            media_id.clone(),
            MediaSourceSpec {
                id: None,
                description: Some(source_description),
                title: Some(source_title.clone()),
                artist: source_artist_literal.clone(),
                workflow_id: None,
                metadata: {
                    let mut metadata_map = BTreeMap::new();
                    let mut title_candidates = Vec::new();
                    if let Some(explicit) = title {
                        title_candidates
                            .push(MediaMetadataValueCandidate::Literal(explicit.to_string()));
                    }
                    title_candidates.extend([
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
                    ]);
                    metadata_map.insert(
                        "title".to_string(),
                        MediaMetadataValue::Fallback(title_candidates),
                    );
                    let mut artist_candidates = Vec::new();
                    if let Some(explicit) = artist {
                        artist_candidates
                            .push(MediaMetadataValueCandidate::Literal(explicit.to_string()));
                    }
                    artist_candidates.extend([
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
                            source_artist_literal.as_deref().unwrap_or("unknown").to_string(),
                        ),
                    ]);
                    metadata_map.insert(
                        "artist".to_string(),
                        MediaMetadataValue::Fallback(artist_candidates),
                    );
                    if let Some(album_value) = album {
                        metadata_map.insert(
                            "album".to_string(),
                            MediaMetadataValue::Literal(album_value.to_string()),
                        );
                    }
                    metadata_map.insert(
                        "video_ext".to_string(),
                        MediaMetadataValue::Literal(source_extension_with_dot),
                    );
                    Some(metadata_map)
                },
                variant_hashes: BTreeMap::new(),
                steps: local_source_default_steps(&hash_text, recording_mbid, release_mbid),
            },
        );
        save_mediapm_document(&self.paths.mediapm_ncl, &document)?;

        conductor_bridge::reconcile_media_workflows_for_config_edits(
            &effective_paths,
            &document,
            &mut lock,
        )?;
        save_mediapm_state_document(&effective_paths.mediapm_state_ncl, &lock)?;

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
        self.add_media_hierarchy_preset_with_position(
            preset,
            media_id,
            Some(folder),
            AddInsertPosition::Sorted,
            false,
        )
    }

    /// Adds one hierarchy preset node tree for an existing media id with one
    /// insertion policy.
    ///
    /// `folder` may be omitted to use preset-specific defaults:
    /// - local: `music videos/local`
    /// - yt-dlp: `music videos/online`
    ///
    /// This operation is idempotent by generated hierarchy id.
    ///
    /// # Errors
    ///
    /// Returns [`MediaPmError`] when the media id is unknown, effective folder
    /// root is empty/invalid, or `mediapm.ncl` cannot be loaded/saved.
    pub fn add_media_hierarchy_preset_with_position(
        &self,
        preset: MediaHierarchyPreset,
        media_id: &str,
        folder: Option<&str>,
        position: AddInsertPosition,
        overwrite: bool,
    ) -> Result<(), MediaPmError> {
        let mut document = ensure_and_load_mediapm_document(&self.paths.mediapm_ncl)?;

        if !document.media.contains_key(media_id) {
            return Err(MediaPmError::Workflow(format!(
                "cannot add {} hierarchy preset: media id '{media_id}' does not exist",
                preset.as_label()
            )));
        }

        let normalized_folder = match folder {
            Some(f) => normalize_hierarchy_folder_root(f)?,
            None => default_hierarchy_folder_root_for_preset(preset),
        };
        let hierarchy_id = hierarchy_preset_node_id(media_id);
        if hierarchy_contains_node_id(&document.hierarchy, &hierarchy_id) {
            return Ok(());
        }

        let node = build_hierarchy_preset_node(preset, media_id, &normalized_folder, hierarchy_id);
        insert_hierarchy_preset_node(
            &mut document.hierarchy,
            node,
            &normalized_folder,
            position,
            overwrite,
        );

        save_mediapm_document(&self.paths.mediapm_ncl, &document)?;
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
        Ok(removed_hierarchy_nodes)
    }

    /// Invalidates cached completed tool calls for one media step.
    ///
    /// This operation keeps media-step synthesis state unchanged and only
    /// targets conductor runtime cache rows that correspond to the selected
    /// media-step index.
    ///
    /// # Errors
    ///
    /// Returns [`MediaPmError`] when the media id/step index is invalid,
    /// conductor documents or state cannot be loaded, or cache-state mutation
    /// fails.
    pub async fn invalidate_media_step_tool_calls(
        &self,
        media_id: &str,
        step_index: usize,
    ) -> Result<MediaStepInvalidationSummary, MediaPmError> {
        self.invalidate_media_step_tool_calls_internal(media_id, step_index, false).await
    }

    /// Invalidates cached completed tool calls and forces one media step to
    /// regenerate managed workflow invocations.
    ///
    /// In addition to conductor cache invalidation, this mode clears the
    /// selected `workflow_states` refresh timestamp before workflow
    /// reconciliation so managed workflow synthesis treats the step as stale.
    ///
    /// # Errors
    ///
    /// Returns [`MediaPmError`] when the media id/step index is invalid,
    /// lock/runtime documents cannot be loaded, or conductor state mutation
    /// fails.
    pub async fn invalidate_media_step_tool_calls_and_regenerate(
        &self,
        media_id: &str,
        step_index: usize,
    ) -> Result<MediaStepInvalidationSummary, MediaPmError> {
        self.invalidate_media_step_tool_calls_internal(media_id, step_index, true).await
    }

    /// Shared implementation for media-step invalidation modes.
    async fn invalidate_media_step_tool_calls_internal(
        &self,
        media_id: &str,
        step_index: usize,
        regenerate_step: bool,
    ) -> Result<MediaStepInvalidationSummary, MediaPmError> {
        let document = ensure_and_load_mediapm_document(&self.paths.mediapm_ncl)?;
        let Some(source) = document.media.get(media_id) else {
            return Err(MediaPmError::Workflow(format!(
                "cannot invalidate media step: media id '{media_id}' does not exist"
            )));
        };
        if step_index >= source.steps.len() {
            return Err(MediaPmError::Workflow(format!(
                "cannot invalidate media step: media '{media_id}' has {} step(s), but step index {step_index} was requested",
                source.steps.len()
            )));
        }

        let effective_runtime_storage = self.resolve_effective_runtime_storage(&document.runtime);
        let effective_paths = self.paths.with_runtime_storage(&effective_runtime_storage);
        load_runtime_dotenv(&effective_paths)?;
        ensure_runtime_env_files(&effective_paths.runtime_root).map_err(MediaPmError::from)?;
        conductor_bridge::ensure_conductor_documents(&effective_paths)?;

        let mut lock = load_mediapm_state_document(&effective_paths.mediapm_state_ncl)?;
        if regenerate_step {
            mark_media_step_for_regeneration(&mut lock, media_id, step_index)?;
            conductor_bridge::reconcile_media_workflows(&effective_paths, &document, &mut lock)?;
        } else {
            conductor_bridge::reconcile_media_workflows(&effective_paths, &document, &mut lock)?;
        }

        let workflow_id = conductor_bridge::managed_workflow_id_for_media(media_id, source);
        let machine =
            conductor_bridge::load_machine_document(&effective_paths.conductor_machine_ncl)?;
        let step_targets = collect_workflow_step_targets_for_media_step(
            &machine,
            workflow_id.as_str(),
            step_index,
        )?;

        let mut state_document =
            load_or_default_conductor_state_document(&effective_paths.conductor_state_config)?;
        let (removed_impure_timestamps, impure_timestamps_by_tool, tools_without_timestamp) =
            remove_target_step_impure_timestamps(
                &mut state_document,
                workflow_id.as_str(),
                &step_targets,
            );
        save_conductor_state_document(&effective_paths.conductor_state_config, &state_document)?;

        let invalidation_rules = build_tool_invalidation_rules(
            &step_targets,
            &impure_timestamps_by_tool,
            &tools_without_timestamp,
        );

        let workflow_options =
            conductor_run_workflow_options(&effective_paths, &effective_runtime_storage);
        let state_options = StateMutationOptions {
            runtime_storage_paths: workflow_options.runtime_storage_paths.clone(),
            runtime_inherited_env_vars: workflow_options.runtime_inherited_env_vars.clone(),
        };
        let mut state = self
            .conductor
            .load_resolved_state(
                &effective_paths.conductor_user_ncl,
                &effective_paths.conductor_machine_ncl,
                state_options.clone(),
            )
            .await?;

        let mut removed_instances = 0usize;
        state.instances.retain(|_instance_key, instance| {
            let remove_instance = should_invalidate_instance(instance, &invalidation_rules);
            if remove_instance {
                removed_instances = removed_instances.saturating_add(1);
                false
            } else {
                true
            }
        });

        if removed_instances > 0 {
            self.conductor
                .replace_resolved_state(
                    &effective_paths.conductor_user_ncl,
                    &effective_paths.conductor_machine_ncl,
                    state,
                    state_options,
                )
                .await?;
        }

        save_mediapm_state_document(&effective_paths.mediapm_state_ncl, &lock)?;

        let mut targeted_step_ids =
            step_targets.into_iter().map(|target| target.step_id).collect::<Vec<_>>();
        targeted_step_ids.sort();

        Ok(MediaStepInvalidationSummary {
            workflow_id,
            targeted_step_ids,
            removed_impure_timestamps,
            removed_instances,
            regenerated_step: regenerate_step,
        })
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
        conductor_bridge::list_tools(&effective_paths)
    }

    /// Adds one tool requirement to `mediapm.ncl` by logical name.
    ///
    /// The tool name must appear in the built-in downloader catalog. If a
    /// requirement for this name already exists, the method is a no-op and
    /// returns `false`. Otherwise the entry is inserted with `tag = "latest"`
    /// and the updated document is saved before returning `true`.
    ///
    /// # Errors
    ///
    /// Returns [`MediaPmError`] when the tool name is not in the catalog,
    /// or when `mediapm.ncl` cannot be loaded or saved.
    pub fn add_tool_requirement(&self, tool_name: &str) -> Result<bool, MediaPmError> {
        // Validate against catalog before mutating config.
        tools::catalog::tool_catalog_entry(tool_name)?;

        let mut document = load_mediapm_document_without_validation(&self.paths.mediapm_ncl)?;
        if document.tools.contains_key(tool_name) {
            return Ok(false);
        }
        document.tools.insert(
            tool_name.to_string(),
            ToolRequirement {
                version: None,
                tag: Some("latest".to_string()),
                dependencies: tools::catalog::default_tool_requirement_dependencies(tool_name),
                recheck_seconds: None,
                max_input_slots: None,
                max_output_slots: None,
            },
        );
        save_mediapm_document(&self.paths.mediapm_ncl, &document)?;
        Ok(true)
    }

    /// Removes one tool requirement entry from `mediapm.ncl` by logical name.
    ///
    /// This method updates desired tool requirements only. Runtime tool
    /// registration state is reconciled by a subsequent `tools sync` or
    /// top-level `sync` execution.
    ///
    /// Returns `false` when no requirement with this name exists.
    ///
    /// # Errors
    ///
    /// Returns [`MediaPmError`] when `mediapm.ncl` cannot be loaded or saved.
    pub fn remove_tool_requirement(&self, tool_name: &str) -> Result<bool, MediaPmError> {
        let mut document = ensure_and_load_mediapm_document(&self.paths.mediapm_ncl)?;
        let removed = document.tools.remove(tool_name).is_some();
        if removed {
            save_mediapm_document(&self.paths.mediapm_ncl, &document)?;
        }
        Ok(removed)
    }

    /// Prunes one tool binary and optionally removes all associated metadata.
    ///
    /// When `remove_metadata` is `false` the operation only removes
    /// `tool_configs.<tool_id>` (binary payload) and marks the registry entry
    /// as `Pruned`, preserving historical records.
    ///
    /// When `remove_metadata` is `true` the tool spec, registry entry, and all
    /// binary content are fully erased.  This is useful for retiring a tool
    /// that will never be re-provisioned, but forces a full re-fetch if the
    /// same tool is re-added later.
    ///
    /// # Errors
    ///
    /// Returns [`MediaPmError`] when config/state documents cannot be loaded,
    /// prune operations fail, or state cannot be persisted.
    pub async fn prune_tool(
        &self,
        tool_id: &str,
        remove_metadata: bool,
    ) -> Result<usize, MediaPmError> {
        let document = ensure_and_load_mediapm_document(&self.paths.mediapm_ncl)?;
        let effective_paths = self.resolve_effective_paths(&document.runtime);
        let mut lock = load_mediapm_state_document(&effective_paths.mediapm_state_ncl)?;
        let removed_hashes = conductor_bridge::prune_tool_binary(
            &effective_paths,
            &mut lock,
            tool_id,
            remove_metadata,
        )
        .await?;
        save_mediapm_state_document(&effective_paths.mediapm_state_ncl, &lock)?;
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
        let machine =
            conductor_bridge::load_machine_document(&effective_paths.conductor_machine_ncl)?;
        let conductor_cas_root = resolve_conductor_cas_root(&effective_paths, &machine);
        let resolved_tool = if let Ok(runtime_handle) = tokio::runtime::Handle::try_current() {
            tokio::task::block_in_place(|| {
                runtime_handle.block_on(resolve_managed_tool_executable_with_filesystem_cas(
                    &effective_paths.conductor_machine_ncl,
                    &conductor_cas_root,
                    &effective_paths.tools_dir,
                    tool_selector,
                ))
            })
        } else {
            let runtime = tokio::runtime::Builder::new_current_thread()
                .enable_all()
                .build()
                .map_err(|source| {
                    MediaPmError::Workflow(format!(
                        "creating temporary runtime for managed tool execution failed: {source}"
                    ))
                })?;

            runtime.block_on(resolve_managed_tool_executable_with_filesystem_cas(
                &effective_paths.conductor_machine_ncl,
                &conductor_cas_root,
                &effective_paths.tools_dir,
                tool_selector,
            ))
        }
        .map_err(MediaPmError::from)?;

        let status = ProcessCommand::new(&resolved_tool.executable_path)
            .args(args)
            .status()
            .map_err(|source| MediaPmError::Io {
                operation: format!("running managed tool '{}' executable", resolved_tool.tool_id),
                path: resolved_tool.executable_path.clone(),
                source,
            })?;

        status.code().ok_or_else(|| {
            MediaPmError::Workflow(format!(
                "managed tool '{}' terminated without a numeric exit code",
                resolved_tool.tool_id
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
    /// hierarchy materialization, and is used by `mediapm tool sync`.
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
        _check_tag_updates: bool,
    ) -> Result<ToolsSyncSummary, MediaPmError> {
        let document = ensure_and_load_mediapm_document(&self.paths.mediapm_ncl)?;
        let (summary, lock, effective_paths) =
            self.sync_tools_from_document(&document, _check_tag_updates).await?;
        save_mediapm_state_document(&effective_paths.mediapm_state_ncl, &lock)?;
        Ok(summary)
    }

    /// Internal helper: runs tool sync from an already-loaded mediapm document.
    ///
    /// Reconciles desired tool state (`reconcile_desired_tools`) but does NOT
    /// reconcile workflows.  `sync_library_with_tag_update_checks` is responsible
    /// for workflow reconciliation, while `sync_tools_with_tag_update_checks`
    /// intentionally skips it (tool sync should only touch tools).  Callers are
    /// responsible for lock persistence regardless of path.
    async fn sync_tools_from_document(
        &self,
        document: &MediaPmDocument,
        check_tag_updates: bool,
    ) -> Result<(ToolsSyncSummary, MediaPmState, MediaPmPaths), MediaPmError> {
        let effective_runtime_storage = self.resolve_effective_runtime_storage(&document.runtime);
        let effective_paths = self.paths.with_runtime_storage(&effective_runtime_storage);
        load_runtime_dotenv(&effective_paths)?;
        conductor_bridge::ensure_conductor_documents(&effective_paths)?;
        export_mediapm_nickel_config_schemas(&effective_paths)?;
        mediapm_conductor::export_nickel_config_schemas(&effective_paths.conductor_schema_dir)?;

        let mut lock = load_mediapm_state_document(&effective_paths.mediapm_state_ncl)?;
        let resolved_inherited_env_vars =
            effective_runtime_storage.inherited_env_vars_with_defaults();
        let report = conductor_bridge::reconcile_desired_tools(
            &effective_paths,
            document,
            &resolved_inherited_env_vars,
            &mut lock,
            check_tag_updates,
        )
        .await?;

        if !report.updated_tool_ids.is_empty() {
            eprintln!("[mediapm] tool id(s) updated: {}", report.updated_tool_ids.join(", "));
        }

        Ok((
            ToolsSyncSummary {
                added_tools: report.added_tool_ids.len(),
                updated_tools: report.updated_tool_ids.len(),
                unchanged_tools: report.unchanged_tool_ids.len(),
                warnings: report.warnings,
            },
            lock,
            effective_paths,
        ))
    }

    /// Reconciles full desired state with explicit tag-update-check policy.
    ///
    /// # Errors
    ///
    /// Returns [`MediaPmError`] when config/runtime preparation fails,
    /// workflow reconciliation/execution fails (including filesystem-CAS
    /// fallback), hierarchy materialization fails, or state cannot be
    /// persisted.
    #[expect(clippy::too_many_lines)]
    pub async fn sync_library_with_tag_update_checks(
        &self,
        _check_tag_updates: bool,
        verify_materialization_override: Option<bool>,
    ) -> Result<SyncSummary, MediaPmError> {
        let document = ensure_and_load_mediapm_document(&self.paths.mediapm_ncl)?;
        let effective_runtime_storage = self.resolve_effective_runtime_storage(&document.runtime);
        let effective_paths = self.paths.with_runtime_storage(&effective_runtime_storage);
        load_runtime_dotenv(&effective_paths)?;
        ensure_runtime_env_files(&effective_paths.runtime_root).map_err(MediaPmError::from)?;
        conductor_bridge::ensure_conductor_documents(&effective_paths)?;
        export_mediapm_nickel_config_schemas(&effective_paths)?;
        mediapm_conductor::export_nickel_config_schemas(&effective_paths.conductor_schema_dir)?;

        let mut lock = load_mediapm_state_document(&effective_paths.mediapm_state_ncl)?;
        conductor_bridge::reconcile_media_workflows(&effective_paths, &document, &mut lock)?;
        // NOTE: `mediapm sync` intentionally does NOT invoke desired-tool sync
        // here.  The hint mechanism below (collect_tools_requiring_sync +
        // append_tool_sync_hint_warning) reminds users to run `mediapm tool sync`
        // when tool state looks stale.
        let machine =
            conductor_bridge::load_machine_document(&effective_paths.conductor_machine_ncl)?;
        let tools_requiring_sync = Self::collect_tools_requiring_sync(&document, &lock, &machine);
        let conductor_cas_root = resolve_conductor_cas_root(&effective_paths, &machine);
        // Conductor workflow progress bars.
        let (tx, mut rx) = mpsc::unbounded_channel::<WorkflowStepEvent>();

        let receiver_handle: tokio::task::JoinHandle<()> = tokio::spawn(async move {
            let mp = MultiProgress::new();
            let mut total_steps: usize = 0;
            let mut overall_bar: Option<ProgressBar> = None;
            let mut worker_bars: Vec<ProgressBar> = Vec::new();
            let mut per_worker_count: Vec<usize> = Vec::new();
            while let Some(event) = rx.recv().await {
                if total_steps == 0 {
                    total_steps = event.total_steps;
                    for _wi in 0..event.worker_count {
                        worker_bars.push(mp.add_bar(0).with_format("{msg}"));
                        per_worker_count.push(0);
                    }
                    overall_bar = Some(
                        mp.add_bar(total_steps as u64)
                            .with_format("{msg}  [{bar:20}]  {pos}/{total}")
                            .with_message("overall"),
                    );
                }
                if event.worker_index >= per_worker_count.len() {
                    per_worker_count.resize(event.worker_index + 1, 0);
                    worker_bars
                        .resize_with(event.worker_index + 1, || mp.add_bar(0).with_format("{msg}"));
                }
                per_worker_count[event.worker_index] =
                    per_worker_count[event.worker_index].saturating_add(1);
                if let Some(ref bar) = overall_bar {
                    bar.set_position(event.completed_steps as u64);
                    bar.set_message(&format!(
                        "completed {}/{} steps",
                        event.completed_steps, total_steps,
                    ));
                }
                if let Some(worker_bar) = worker_bars.get(event.worker_index) {
                    let wi = event.worker_index;
                    worker_bar.set_message(&format!(
                        "worker {wi}: {}: {}  ({})",
                        event.workflow_display_name, event.step_id, per_worker_count[wi],
                    ));
                }
            }
            if let Some(ref bar) = overall_bar {
                bar.set_message("all workflows complete");
                bar.set_position(total_steps as u64);
            }
            for (wi, bar) in worker_bars.iter().enumerate() {
                bar.set_message(&format!("worker {wi}: done  ({})", per_worker_count[wi]));
            }
            tokio::time::sleep(std::time::Duration::from_millis(75)).await;
            // mp dropped here → render thread joins.
        });

        let mut workflow_options =
            conductor_run_workflow_options(&effective_paths, &effective_runtime_storage);
        workflow_options.progress_sender = Some(tx.clone());

        eprintln!("[mediapm::sync] running conductor workflows...");
        let conductor_summary = match self
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
                return Err(primary_error.into());
            }
        };
        // Drop sender so receiver task can complete.
        drop(tx);
        receiver_handle.await.ok();

        eprintln!("[mediapm::sync] syncing hierarchy materialization outputs...");
        let hierarchy_start = std::time::Instant::now();
        let state_json =
            serde_json::to_vec(&lock).map_err(|e| MediaPmError::Serialization(e.to_string()))?;
        let state_hash = mediapm_cas::Hash::from_bytes(*blake3::hash(&state_json).as_bytes());
        let materialize_report = materializer::sync_hierarchy(
            &effective_paths,
            &document,
            &machine,
            &conductor_cas_root,
            &mut lock,
            Some(state_hash),
            verify_materialization_override
                .unwrap_or_else(|| document.runtime.verify_materialization()),
        )
        .await?;
        let hierarchy_elapsed = hierarchy_start.elapsed();
        eprintln!(
            "[mediapm::sync] hierarchy materialization completed in {:.1}s",
            hierarchy_elapsed.as_secs_f64()
        );
        let mut warnings = Vec::new();
        warnings.extend(materialize_report.notices.clone());
        Self::append_tool_sync_hint_warning(&mut warnings, &tools_requiring_sync);

        // Reconcile again after materialization so managed-file hashes written
        // during this sync are immediately rooted in machine external_data.
        eprintln!("[mediapm::sync] finalizing machine-state reconciliation...");
        conductor_bridge::reconcile_media_workflows(&effective_paths, &document, &mut lock)?;
        save_mediapm_state_document(&effective_paths.mediapm_state_ncl, &lock)?;

        Ok(SyncSummary {
            executed_instances: conductor_summary.executed_instances,
            cached_instances: conductor_summary.cached_instances,
            rematerialized_instances: conductor_summary.rematerialized_instances,
            materialized_paths: materialize_report.materialized_paths,
            removed_paths: materialize_report.removed_paths,
            removed_empty_dirs: materialize_report.removed_empty_dirs,
            added_tools: 0,
            updated_tools: 0,
            warnings,
        })
    }
}

impl MediaPmService<SimpleConductor<FileSystemCas>> {
    /// Creates a filesystem-backed conductor stack rooted at the given directory.
    ///
    /// This is the production constructor used by the `mediapm` CLI.
    ///
    /// # Errors
    ///
    /// Returns [`MediaPmError`] when the underlying filesystem CAS backend
    /// cannot be opened or initialized at the resolved runtime store path.
    pub async fn new_fs_at_with_runtime_storage_overrides(
        root_dir: &Path,
        runtime_storage_overrides: MediaRuntimeStorage,
    ) -> Result<Self, MediaPmError> {
        let paths = MediaPmPaths::from_root(root_dir);
        let cas_store_root = paths.runtime_root.join("store");
        let file_system_cas = FileSystemCas::open_with_alpha_and_integrity(
            &cas_store_root,
            4,
            runtime_storage_overrides.to_cas_integrity_config(),
        )
        .await
        .map_err(|error| {
            MediaPmError::Workflow(format!(
                "opening conductor CAS store '{}' for workflow execution failed: {error}",
                cas_store_root.display()
            ))
        })?;
        let conductor = SimpleConductor::new(file_system_cas);
        Ok(Self::new_with_runtime_storage_overrides(conductor, paths, runtime_storage_overrides))
    }
}

impl MediaPmService<SimpleConductor<InMemoryCas>> {
    /// Creates a media service with an in-memory conductor and paths from the
    /// current directory.
    ///
    /// Useful for lightweight testing of API behavior that does not require
    /// inspecting filesystem artifacts.
    #[must_use]
    pub fn new_in_memory() -> Self {
        let paths = MediaPmPaths::from_current_dir();
        let conductor = SimpleConductor::new(InMemoryCas::new());
        Self::new(conductor, paths)
    }

    /// Creates a media service with an in-memory conductor rooted at the given
    /// directory.
    ///
    /// The workspace root is used to derive all canonical [`MediaPmPaths`] so
    /// filesystem artifacts are available for inspection.
    #[must_use]
    pub fn new_in_memory_at(root_dir: &Path) -> Self {
        let paths = MediaPmPaths::from_root(root_dir);
        let conductor = SimpleConductor::new(InMemoryCas::new());
        Self::new(conductor, paths)
    }

    /// Creates a media service with an in-memory conductor, rooted at the given
    /// directory, with explicit runtime storage overrides.
    #[must_use]
    pub fn new_in_memory_at_with_runtime_storage_overrides(
        root_dir: &Path,
        runtime_storage_overrides: MediaRuntimeStorage,
    ) -> Self {
        let paths = MediaPmPaths::from_root(root_dir);
        let conductor = SimpleConductor::new(InMemoryCas::new());
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
        self.sync_library_with_tag_update_checks(false, None).await
    }
}
