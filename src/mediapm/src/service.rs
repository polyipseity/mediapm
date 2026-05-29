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
use mediapm_conductor::{ConductorApi, SimpleConductor};
use url::Url;

use crate::conductor_bridge::ConductorToolRow;
use crate::config::{
    MediaMetadataValue, MediaMetadataValueCandidate, MediaMetadataVariantBinding, MediaPmDocument,
    MediaSourceSpec, MediaStep, MediaStepTool, ToolRequirement, ToolRequirementDependencies,
    TransformInputValue, load_mediapm_document, save_mediapm_document,
};
use crate::error::MediaPmError;
use crate::hierarchy::{
    build_hierarchy_preset_node, default_hierarchy_folder_root_for_preset,
    hierarchy_contains_node_id, hierarchy_preset_node_id, insert_hierarchy_preset_node,
    normalize_hierarchy_folder_root, remove_hierarchy_nodes_by_id,
    remove_hierarchy_nodes_by_media_id,
};
use crate::lockfile::{MediaLockFile, load_lockfile, save_lockfile};
use crate::paths::MediaPmPaths;
use crate::source_metadata::{
    LocalSourceMetadata, fetch_local_source_metadata, fetch_mb_recording_metadata,
    fetch_online_source_metadata, resolve_conductor_cas_root,
    resolve_online_source_metadata_for_add, run_workflow_with_filesystem_cas,
    should_prefer_filesystem_workflow_runner, should_retry_workflow_with_filesystem_cas,
};
use crate::{
    AddInsertPosition, MediaHierarchyPreset, MediaPackage, MediaRuntimeStorage, SyncSummary,
    ToolsSyncSummary,
};
use crate::{
    build_local_default_description, build_remote_default_description,
    conductor_run_workflow_options, export_mediapm_nickel_config_schemas, load_runtime_dotenv,
    local_default_title, local_extension_with_dot, local_source_default_steps,
    media_id_from_local_path, media_id_from_uri, merge_runtime_storage, normalize_source_uri,
    validate_source_uri,
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
    pub async fn add_media_source(
        &self,
        uri: &Url,
        recording_id: Option<&str>,
    ) -> Result<String, MediaPmError> {
        self.add_media_source_with_position(uri, recording_id, AddInsertPosition::Sorted).await
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
    #[allow(clippy::too_many_lines)]
    pub async fn add_media_source_with_position(
        &self,
        uri: &Url,
        recording_id: Option<&str>,
        _position: AddInsertPosition,
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
        let normalized_uri = normalize_source_uri(uri);
        let media_id = media_id_from_uri(&normalized_uri);
        let effective_paths = self.resolve_effective_paths(&document.runtime);
        let yt_dlp_configured = document.tools.contains_key("yt-dlp");
        let (yt_dlp_metadata, warning) = if yt_dlp_configured {
            let lock = load_lockfile(&effective_paths.mediapm_state_ncl)?;
            match conductor_bridge::resolve_managed_tool_executable_target(
                &effective_paths,
                &lock,
                "yt-dlp",
            ) {
                Ok(target) => {
                    let metadata =
                        fetch_online_source_metadata(&normalized_uri, &target.command_path);
                    let warning = if metadata.title.is_none()
                        && metadata.artist.is_none()
                        && metadata.description.is_none()
                    {
                        Some(format!(
                            "managed yt-dlp binary at '{}' returned no usable metadata for remote source '{normalized_uri}'",
                            target.command_path.display()
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
            resolve_online_source_metadata_for_add(&normalized_uri, yt_dlp_metadata, warning);
        if let Some(warning) = resolved_online_metadata.warning.as_ref() {
            eprintln!("warning: {warning}");
        }
        let source_title = if let Some(mb) = mb.as_ref() {
            mb.title.clone()
        } else {
            resolved_online_metadata.title.clone()
        };
        let source_artist_literal = mb.as_ref().map(|m| m.artist.clone());
        let source_description = if let Some(mb) = mb.as_ref() {
            build_remote_default_description(&mb.title, Some(&mb.artist))
        } else {
            resolved_online_metadata.description.clone()
        };

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
                                source_artist_literal
                                    .or(resolved_online_metadata.artist.clone())
                                    .unwrap_or_else(|| "unknown".to_string()),
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
                    ("video_ext".to_string(), MediaMetadataValue::Literal(".mkv".to_string())),
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
                                TransformInputValue::String(String::new()),
                            ),
                            (
                                "release_mbid".to_string(),
                                TransformInputValue::String(String::new()),
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
    pub async fn add_local_source(
        &self,
        local_path: &Path,
        recording_id: Option<&str>,
    ) -> Result<String, MediaPmError> {
        self.add_local_source_with_position(local_path, recording_id, AddInsertPosition::Sorted)
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
    #[allow(clippy::too_many_lines)]
    pub async fn add_local_source_with_position(
        &self,
        local_path: &Path,
        recording_id: Option<&str>,
        _position: AddInsertPosition,
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
        self.add_media_hierarchy_preset_with_position(
            preset,
            media_id,
            Some(folder),
            AddInsertPosition::Sorted,
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
    ) -> Result<(), MediaPmError> {
        let mut document = ensure_and_load_mediapm_document(&self.paths.mediapm_ncl)?;

        if !document.media.contains_key(media_id) {
            return Err(MediaPmError::Workflow(format!(
                "cannot add {} hierarchy preset: media id '{media_id}' does not exist",
                preset.as_label()
            )));
        }

        let folder = folder.unwrap_or_else(|| default_hierarchy_folder_root_for_preset(preset));
        let normalized_folder = normalize_hierarchy_folder_root(folder)?;
        let hierarchy_id = hierarchy_preset_node_id(media_id);
        if hierarchy_contains_node_id(&document.hierarchy, &hierarchy_id) {
            return Ok(());
        }

        let node = build_hierarchy_preset_node(preset, media_id, &normalized_folder, hierarchy_id);
        insert_hierarchy_preset_node(&mut document.hierarchy, node, &normalized_folder, position);

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

        let mut document = ensure_and_load_mediapm_document(&self.paths.mediapm_ncl)?;
        if document.tools.contains_key(tool_name) {
            return Ok(false);
        }
        document.tools.insert(
            tool_name.to_string(),
            ToolRequirement {
                version: None,
                tag: Some("latest".to_string()),
                dependencies: ToolRequirementDependencies::default(),
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
        eprintln!("[mediapm::sync] reconciling managed tools and workflow configuration...");
        let (tool_summary, mut lock) =
            self.sync_tools_from_document(&document, check_tag_updates).await?;
        let effective_runtime_storage = self.resolve_effective_runtime_storage(&document.runtime);
        let effective_paths = self.paths.with_runtime_storage(&effective_runtime_storage);
        let machine =
            conductor_bridge::load_machine_document(&effective_paths.conductor_machine_ncl)?;
        let conductor_cas_root = resolve_conductor_cas_root(&effective_paths, &machine);
        let workflow_options =
            conductor_run_workflow_options(&effective_paths, &effective_runtime_storage);

        eprintln!("[mediapm::sync] running conductor workflows...");
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

        eprintln!("[mediapm::sync] syncing hierarchy materialization outputs...");
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
        eprintln!("[mediapm::sync] finalizing machine-state reconciliation...");
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

/// Returns built-in tool ids that mediapm expects to be available.
#[must_use]
pub fn registered_builtin_ids() -> [&'static str; 5] {
    mediapm_conductor::registered_builtin_ids()
}

/// Resolves effective runtime paths for one workspace root without mutating
/// workspace files.
///
/// Unlike `load_runtime_dotenv_for_root`, this helper does not bootstrap a
/// missing `mediapm.ncl` and does not load dotenv files into process state.
/// It is intended for passthrough CLI routing where the parent executable must
/// inject its resolved runtime defaults into child CLI argv without creating
/// configuration files as a side effect.
///
/// # Errors
///
/// Returns [`MediaPmError`] when an existing `mediapm.ncl` cannot be loaded or
/// when effective runtime paths cannot be derived from config plus overrides.
pub fn resolve_effective_paths_for_root(
    root_dir: &Path,
    runtime_storage_overrides: &MediaRuntimeStorage,
) -> Result<MediaPmPaths, MediaPmError> {
    let base_paths = MediaPmPaths::from_root(root_dir);
    let document = if base_paths.mediapm_ncl.exists() {
        load_mediapm_document(&base_paths.mediapm_ncl)?
    } else {
        MediaPmDocument::default()
    };

    let merged_runtime_storage =
        merge_runtime_storage(&document.runtime, runtime_storage_overrides);
    Ok(base_paths.with_runtime_storage(&merged_runtime_storage))
}

/// Loads runtime dotenv values for one workspace root using effective path policy.
///
/// This helper is intended for CLI entrypoints that need environment-backed
/// credentials before invoking internal builtins directly.
/// # Errors
///
/// Returns [`MediaPmError`] when config cannot be loaded, effective runtime
/// paths cannot be resolved, or dotenv loading fails.
pub fn load_runtime_dotenv_for_root(
    root_dir: &Path,
    runtime_storage_overrides: &MediaRuntimeStorage,
) -> Result<MediaPmPaths, MediaPmError> {
    let effective_paths = if MediaPmPaths::from_root(root_dir).mediapm_ncl.exists() {
        resolve_effective_paths_for_root(root_dir, runtime_storage_overrides)?
    } else {
        let base_paths = MediaPmPaths::from_root(root_dir);
        let document = ensure_and_load_mediapm_document(&base_paths.mediapm_ncl)?;
        let merged_runtime_storage =
            merge_runtime_storage(&document.runtime, runtime_storage_overrides);
        base_paths.with_runtime_storage(&merged_runtime_storage)
    };
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
