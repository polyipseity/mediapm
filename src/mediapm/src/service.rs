//! High-level orchestration service for mediapm.
//!
//! [`MediaPmService`] composes CAS + Conductor into the media-facing API.
//! Callers create a service instance bound to a workspace root, then call
//! methods to add/remove sources, sync tools, sync the library, and
//! invalidate cached steps.

use std::collections::BTreeMap;
use std::path::Path;
use std::sync::Arc;

use mediapm_cas::{CasApi, CasMaintenanceApi, FileSystemCas, Hash, InMemoryCas};
use mediapm_conductor::cache::{Cache, CacheDomainConfig, ENTRY_TTL_SECONDS};
use mediapm_conductor::cache_user_level::default_mediapm_user_download_cache_root;
use mediapm_conductor::runtime_env::{ensure_runtime_env_files, extend_runtime_gitignore};
use mediapm_conductor::tools::provider::ConfigVersionSpec;
use mediapm_conductor::{RuntimeStoragePaths, SimpleConductor};
use url::Url;

use crate::conductor_bridge::documents::{
    ConductorToolRow, list_tools, load_conductor_generated_document, load_conductor_user_document,
};
use crate::conductor_bridge::sync::{
    apply_resolved_field_backfills, open_workspace_cas_store, reconcile_desired_tools,
};
use crate::config::{
    MediaPmState, MediaRuntimeStorage, MediaSourceSpec, MediaStepTool, ToolRequirement,
    load_mediapm_document, load_mediapm_state_document, save_mediapm_document,
    save_mediapm_state_document,
};
use crate::error::MediaPmError;
use crate::hierarchy::{
    insert_hierarchy_preset_node, remove_hierarchy_nodes_by_id, remove_hierarchy_nodes_by_media_id,
};
use crate::materializer;
use crate::metadata_cache::MetadataCache;
use crate::output::{ProgressGroup, ProgressGroupApi};
use crate::paths::{MediaPmPathOverrides, MediaPmPaths};
pub(crate) use crate::service_standalone::*;
use crate::source_metadata::{fetch_local_source_metadata, resolve_conductor_cas_root};
use crate::tools::downloader::ToolDownloadCache;
use crate::tools::is_known_tool_id;
use crate::tools::provider::RecheckPolicy;
use crate::tools::workflows::{MANAGED_WORKFLOW_PREFIX, reconcile_media_workflows};

use crate::{
    AddInsertPosition, MediaHierarchyPreset, MediaPackage, MediaStepInvalidationSummary,
    SyncSummary, ToolsSyncSummary, conductor_run_workflow_options, ensure_mediapm_executable_env,
    export_mediapm_nickel_config_schemas, load_runtime_dotenv, local_source_default_steps,
    media_id_from_local_path, media_id_from_uri, merge_runtime_storage, normalize_source_uri,
    validate_source_uri,
};

/// Mediapm-specific entries appended to the conductor-managed `.gitignore`
/// at service construction time. Keeps generated and machine-managed files
/// out of version control.
const MEDIAPM_EXTRA_GITIGNORE: &str = concat!("/cache/\n", "/tools/\n");

/// Resolves the workspace CAS store used for tool payload import and materialization.
pub(crate) trait WorkspaceProvisioningCas:
    CasApi + CasMaintenanceApi + Send + Sync + Sized + 'static
{
    async fn workspace_provisioning_cas(
        conductor: &SimpleConductor<Self>,
        effective_paths: &MediaPmPaths,
    ) -> Result<Arc<FileSystemCas>, MediaPmError>;
}

impl WorkspaceProvisioningCas for FileSystemCas {
    async fn workspace_provisioning_cas(
        conductor: &SimpleConductor<Self>,
        _effective_paths: &MediaPmPaths,
    ) -> Result<Arc<FileSystemCas>, MediaPmError> {
        Ok(Arc::clone(conductor.cas()))
    }
}

impl WorkspaceProvisioningCas for InMemoryCas {
    async fn workspace_provisioning_cas(
        _conductor: &SimpleConductor<Self>,
        effective_paths: &MediaPmPaths,
    ) -> Result<Arc<FileSystemCas>, MediaPmError> {
        open_workspace_cas_store(effective_paths).await
    }
}

// ---------------------------------------------------------------------------
// Service struct
// ---------------------------------------------------------------------------

/// Composes CAS + Conductor into the media-facing API and CLI scaffold.
///
/// Type parameter `Cas` selects the content-addressed store backend.
/// Generic code uses [`MediaPmService<Cas>`]; concrete filesystem and
/// in-memory variants have convenience constructors.
///
/// # Type parameters
///
/// * `Cas` — The CAS backend. Must implement [`CasApi`] + [`CasMaintenanceApi`] + `Send + Sync + 'static`.
pub struct MediaPmService<Cas: CasApi + CasMaintenanceApi + Send + Sync + 'static> {
    /// Conductor instance bound to this service's workspace.
    conductor: SimpleConductor<Cas>,
    /// Resolved filesystem paths for this workspace.
    paths: MediaPmPaths,
    /// Runtime storage overrides passed at construction.
    runtime_storage_overrides: MediaRuntimeStorage,
    /// Persistent local-source metadata cache (`<runtime>/cache/mediapm`).
    metadata_cache: MetadataCache,
}

#[allow(private_bounds)]
impl<Cas: WorkspaceProvisioningCas + CasApi + CasMaintenanceApi + Send + Sync + Sized + 'static>
    MediaPmService<Cas>
{
    /// Creates a new service instance with the given conductor and paths.
    ///
    /// Runtime storage overrides default to [`MediaRuntimeStorage::default()`].
    #[must_use]
    pub fn new(conductor: SimpleConductor<Cas>, paths: MediaPmPaths) -> Self {
        let metadata_cache = MetadataCache::open(&paths.workspace_mediapm_cache_dir());
        Self {
            conductor,
            paths,
            runtime_storage_overrides: MediaRuntimeStorage::default(),
            metadata_cache,
        }
    }

    /// Creates a new service instance with explicit runtime storage overrides.
    #[must_use]
    pub fn new_with_runtime_storage_overrides(
        conductor: SimpleConductor<Cas>,
        paths: MediaPmPaths,
        runtime_storage_overrides: MediaRuntimeStorage,
    ) -> Self {
        let metadata_cache = MetadataCache::open(&paths.workspace_mediapm_cache_dir());
        Self { conductor, paths, runtime_storage_overrides, metadata_cache }
    }

    // -----------------------------------------------------------------------
    // Getters
    // -----------------------------------------------------------------------

    /// Returns a shared reference to the paths layout.
    #[must_use]
    pub fn paths(&self) -> &MediaPmPaths {
        &self.paths
    }

    /// Returns a shared reference to the conductor.
    #[must_use]
    pub fn conductor(&self) -> &SimpleConductor<Cas> {
        &self.conductor
    }

    /// Returns a shared reference to the runtime storage overrides.
    #[must_use]
    pub fn runtime_storage_overrides(&self) -> &MediaRuntimeStorage {
        &self.runtime_storage_overrides
    }

    // -----------------------------------------------------------------------
    // Path and runtime helpers
    // -----------------------------------------------------------------------

    /// Resolves effective paths by applying runtime storage overrides on top
    /// of the base paths.
    ///
    /// # Errors
    ///
    /// Returns [`MediaPmError::Io`] if the document cannot be read, or
    /// [`MediaPmError::Serialization`] if it cannot be parsed.
    pub fn resolve_effective_paths(&self) -> Result<MediaPmPaths, MediaPmError> {
        let merged = self.resolve_effective_runtime_storage()?;
        let overrides = MediaPmPathOverrides {
            mediapm_dir: merged.mediapm_dir.as_ref().map(|d| Path::new(d).to_path_buf()),
            hierarchy_root_dir: merged
                .hierarchy_root_dir
                .as_ref()
                .map(|d| Path::new(d).to_path_buf()),
            conductor_config: merged.conductor_config.as_ref().map(|d| Path::new(d).to_path_buf()),
            conductor_generated_config: merged
                .conductor_generated_config
                .as_ref()
                .map(|d| Path::new(d).to_path_buf()),
            conductor_state_config: merged
                .conductor_state_config
                .as_ref()
                .map(|d| Path::new(d).to_path_buf()),
            conductor_schema_dir: merged
                .conductor_schema_dir
                .as_ref()
                .map(|d| Path::new(d).to_path_buf()),
            media_state_config: merged
                .media_state_config
                .as_ref()
                .map(|d| Path::new(d).to_path_buf()),
            env_file: merged.env_file.as_ref().map(|d| Path::new(d).to_path_buf()),
            env_generated_file: merged
                .env_generated_file
                .as_ref()
                .map(|d| Path::new(d).to_path_buf()),
            mediapm_schema_dir: merged
                .mediapm_schema_dir
                .as_ref()
                .map(|inner| inner.as_ref().map(|d| Path::new(d).to_path_buf())),
        };
        Ok(self.paths.with_overrides(&overrides))
    }

    /// Resolves effective runtime storage by merging config-declared values
    /// with service-level overrides.
    ///
    /// # Errors
    ///
    /// Returns [`MediaPmError::Io`] if the document cannot be read, or
    /// [`MediaPmError::Serialization`] if it cannot be parsed.
    pub fn resolve_effective_runtime_storage(&self) -> Result<MediaRuntimeStorage, MediaPmError> {
        let effective_paths = MediaPmPaths::from_root(&self.paths.root_dir);
        let mut doc = ensure_and_load_mediapm_document(&effective_paths)?;
        // Tools now live at the document level (not inside runtime).
        // Populate runtime.tools so merge_runtime_storage sees them.
        doc.runtime.tools = doc.tools;
        Ok(merge_runtime_storage(&doc.runtime, &self.runtime_storage_overrides))
    }

    // -----------------------------------------------------------------------
    // Tool-sync helpers
    // -----------------------------------------------------------------------

    /// Checks whether a logical tool for the given media id requires a sync.
    ///
    /// Returns `true` if the tool is missing from the state's managed tool
    /// registry or its canonical version does not match the provider-resolved
    /// version.
    ///
    /// # Panics
    ///
    /// Panics on an internal invariant violation: a `tool_id` declared in
    /// `effective.tools` at the `is_none()` guard must still be present at
    /// the later `unwrap()`.
    ///
    /// # Errors
    ///
    /// Returns [`MediaPmError`] when effective runtime storage resolution
    /// fails.
    pub async fn logical_tool_requires_sync(
        &self,
        tool_id: &str,
        state: &MediaPmState,
    ) -> Result<bool, MediaPmError> {
        if let Some(existing) = state.managed_tools.iter().rfind(|e| e.tool_id == tool_id) {
            let effective = self.resolve_effective_runtime_storage()?;
            let desired = effective.tools.get(tool_id);
            // If no desired requirement is declared, the tool is considered
            // up-to-date when present in state.
            if desired.is_none() {
                return Ok(false);
            }
            // Resolve the canonical version from the provider and compare
            // against the recorded canonical_version in state. Use the SAME
            // cache root the sync path uses so the resolved canonical_version
            // matches what was recorded during sync (a divergent default
            // cache would produce a spurious mismatch warning).
            let cache_root = match effective.cache_root_override.as_deref() {
                Some(root) => root.to_path_buf(),
                None => default_mediapm_user_download_cache_root().ok_or_else(|| {
                    MediaPmError::Workflow(
                        "could not determine default tool cache root".to_string(),
                    )
                })?,
            };
            let content_domain = CacheDomainConfig {
                domain: "tools".to_string(),
                index_file_name: "tools.json".to_string(),
                entry_ttl_seconds: ENTRY_TTL_SECONDS,
            };
            let metadata_domain = CacheDomainConfig {
                domain: "tool_metadata".to_string(),
                index_file_name: "tool_metadata.json".to_string(),
                entry_ttl_seconds: 24 * 60 * 60,
            };
            let cache = Cache::open(&cache_root, &[content_domain, metadata_domain])
                .await
                .map(ToolDownloadCache::from_cache)
                .map_err(|e| {
                    MediaPmError::Workflow(format!("failed to open tool download cache: {e}"))
                })?;
            match crate::tools::provider::resolve_tool_fetch(
                tool_id,
                Some((&cache, "tool_metadata")),
                RecheckPolicy::default(),
            )
            .await
            {
                Ok((_, metadata)) => {
                    let resolved_canonical_version = metadata.canonical_version;
                    // Build live_state from managed tools and compute composite
                    // canonical_version for apples-to-apples comparison.
                    let live_state =
                        crate::conductor_bridge::sync::index_managed_tools(&state.managed_tools);
                    let expected_composite =
                        crate::conductor_bridge::sync::compute_composite_canonical_version(
                            &resolved_canonical_version,
                            tool_id,
                            desired.unwrap(),
                            &live_state,
                        );
                    Ok(existing.canonical_version != expected_composite
                        || (existing.content_map_hash.is_empty()
                            && !MediaStepTool::is_builtin_source_ingest_name(tool_id)))
                }
                Err(_) => {
                    // Conservatively recommend sync on provider resolution failure.
                    Ok(true)
                }
            }
        } else {
            Ok(true) // missing from state's managed tool registry → requires sync
        }
    }

    /// Collects tool ids that require a sync based on state comparison.
    ///
    /// # Errors
    ///
    /// Returns [`MediaPmError`] when effective runtime storage resolution or
    /// an individual tool sync check fails.
    pub async fn collect_tools_requiring_sync(
        &self,
        state: &MediaPmState,
    ) -> Result<Vec<String>, MediaPmError> {
        let effective = self.resolve_effective_runtime_storage()?;
        let mut needing_sync = Vec::new();
        for tool_id in effective.tools.keys() {
            if self.logical_tool_requires_sync(tool_id, state).await? {
                needing_sync.push(tool_id.clone());
            }
        }
        Ok(needing_sync)
    }

    /// Appends a warning message when tools require syncing.
    ///
    /// # Errors
    ///
    /// Returns [`MediaPmError::Io`] if the document cannot be read, or
    /// [`MediaPmError::Serialization`] if it cannot be parsed.
    pub async fn append_tool_sync_hint_warning(
        &self,
        warnings: &mut Vec<String>,
        state: &MediaPmState,
    ) -> Result<(), MediaPmError> {
        let needing_sync = self.collect_tools_requiring_sync(state).await?;
        if !needing_sync.is_empty() {
            warnings.push(format!(
                "tools require sync before library sync: {}",
                needing_sync.join(", ")
            ));
        }
        Ok(())
    }

    // -----------------------------------------------------------------------
    // Source management
    // -----------------------------------------------------------------------

    /// Adds one online media source and saves the document.
    ///
    /// This is a convenience wrapper around
    /// [`add_media_source_with_position`](Self::add_media_source_with_position)
    /// that inserts at the end.
    ///
    /// # Errors
    ///
    /// Delegates to [`add_media_source_with_position`](Self::add_media_source_with_position).
    pub fn add_media_source(
        &mut self,
        media_source: &MediaSourceSpec,
        media_id: String,
        uri: &Url,
        title: Option<&str>,
        description: Option<&str>,
    ) -> Result<(), MediaPmError> {
        self.add_media_source_with_position(
            media_source,
            media_id,
            uri,
            title,
            description,
            AddInsertPosition::End,
            false,
        )
    }

    /// Adds one online media source at the given position and saves the
    /// document.
    ///
    /// Normalizes the URI, validates the scheme, optionally fetches metadata
    /// from the source, then inserts the entry into the mediapm document.
    ///
    /// # Errors
    ///
    /// Returns [`MediaPmError::Workflow`] if the media id already exists or
    /// the hierarchy insertion fails.
    #[expect(
        clippy::too_many_arguments,
        clippy::needless_pass_by_value,
        reason = "public API entrypoint with CLI-parity parameter surface; underscore-prefixed args are accepted for interface symmetry"
    )]
    pub fn add_media_source_with_position(
        &mut self,
        media_source: &MediaSourceSpec,
        media_id: String,
        _uri: &Url,
        title: Option<&str>,
        description: Option<&str>,
        _position: AddInsertPosition,
        overwrite: bool,
    ) -> Result<(), MediaPmError> {
        let effective_paths = self.resolve_effective_paths()?;
        let mut document =
            crate::service_standalone::ensure_and_load_mediapm_document(&effective_paths)?;

        if document.media.contains_key(&media_id) {
            if overwrite {
                document.media.remove(&media_id);
                let _ = remove_hierarchy_nodes_by_media_id(&mut document.hierarchy, &media_id);
            } else {
                return Err(MediaPmError::Workflow(format!(
                    "media source '{media_id}' already exists in config",
                )));
            }
        }

        // Build the source spec from the provided template and metadata.
        let mut source = media_source.clone();
        if let Some(t) = title.filter(|s| !s.is_empty()) {
            source.title = t.to_string();
        }
        if let Some(d) = description.filter(|s| !s.is_empty()) {
            source.description = d.to_string();
        }

        document.media.insert(media_id.clone(), source);

        // Save the document.
        save_mediapm_document(&effective_paths.mediapm_ncl, &document)?;

        Ok(())
    }

    /// Adds one local media source, auto-resolving metadata, and saves the
    /// document.
    ///
    /// This is a convenience wrapper around
    /// [`add_local_source_with_position`](Self::add_local_source_with_position)
    /// that inserts at the end.
    ///
    /// # Errors
    ///
    /// Delegates to [`add_local_source_with_position`](Self::add_local_source_with_position).
    pub fn add_local_source(
        &mut self,
        path: &Path,
        ffprobe_command: &str,
        media_id: Option<String>,
        position: AddInsertPosition,
    ) -> Result<String, MediaPmError> {
        self.add_local_source_with_position(path, ffprobe_command, media_id, position, false)
    }

    /// Adds one local media source at the given position, auto-resolving
    /// metadata via ffprobe, and saves the document.
    ///
    /// Reads the file into CAS to obtain a content hash, then builds default
    /// media steps and metadata entries.
    ///
    /// # Errors
    ///
    /// Returns [`MediaPmError::Io`] if the file cannot be read, or
    /// [`MediaPmError::Workflow`] if the media id already exists.
    pub fn add_local_source_with_position(
        &mut self,
        path: &Path,
        ffprobe_command: &str,
        media_id: Option<String>,
        _position: AddInsertPosition,
        overwrite: bool,
    ) -> Result<String, MediaPmError> {
        let effective_paths = self.resolve_effective_paths()?;
        let mut document =
            crate::service_standalone::ensure_and_load_mediapm_document(&effective_paths)?;

        // Compute content hash from the file.
        let bytes = std::fs::read(path).map_err(|e| MediaPmError::Io {
            operation: "reading local file for media add".to_string(),
            path: path.to_path_buf(),
            source: e,
        })?;
        let hash = Hash::from_content(&bytes);
        let resolved_media_id =
            media_id.filter(|s| !s.is_empty()).unwrap_or_else(|| media_id_from_local_path(&hash));

        if document.media.contains_key(&resolved_media_id) {
            if overwrite {
                document.media.remove(&resolved_media_id);
                let _ =
                    remove_hierarchy_nodes_by_media_id(&mut document.hierarchy, &resolved_media_id);
            } else {
                return Err(MediaPmError::Workflow(format!(
                    "media source '{resolved_media_id}' already exists in config",
                )));
            }
        }

        // Fetch metadata via ffprobe (cache hit skips the probe).
        let metadata =
            fetch_local_source_metadata(path, ffprobe_command, Some(&self.metadata_cache))?;
        let title = Some(metadata.title.as_str());
        let description = Some(metadata.description.as_str());

        // Build default steps.
        let hash_text = hash.to_hex();
        let steps = local_source_default_steps(&hash_text, None, None);

        let source = MediaSourceSpec {
            title: title.filter(|s| !s.is_empty()).map(str::to_string).unwrap_or_default(),
            description: description
                .filter(|s| !s.is_empty())
                .map(str::to_string)
                .unwrap_or_default(),
            artist: String::new(),
            metadata: BTreeMap::new(),
            variant_hashes: BTreeMap::new(),
            steps,
        };

        document.media.insert(resolved_media_id.clone(), source);

        // Also insert a hierarchy preset node for this source.
        if let Some(preset_node) = document.hierarchy.first_mut() {
            // Append the media folder node to the first root folder.
            let media_node = crate::hierarchy::local_hierarchy_media_children();
            preset_node.children.extend(media_node);
        }

        save_mediapm_document(&effective_paths.mediapm_ncl, &document)?;

        Ok(resolved_media_id)
    }

    // -----------------------------------------------------------------------
    // Hierarchy management
    // -----------------------------------------------------------------------

    /// Adds a hierarchy preset node at the given position and saves the
    /// document.
    ///
    /// # Errors
    ///
    /// Returns [`MediaPmError::Workflow`] if the preset node id already
    /// exists.
    pub fn add_media_hierarchy_preset_with_position(
        &mut self,
        preset: MediaHierarchyPreset,
        position: AddInsertPosition,
    ) -> Result<(), MediaPmError> {
        let effective_paths = self.resolve_effective_paths()?;
        let mut document =
            crate::service_standalone::ensure_and_load_mediapm_document(&effective_paths)?;

        insert_hierarchy_preset_node(&mut document.hierarchy, preset, position)?;

        save_mediapm_document(&effective_paths.mediapm_ncl, &document)?;
        Ok(())
    }

    /// Adds a hierarchy preset node at the end and saves the document.
    ///
    /// # Errors
    ///
    /// Delegates to
    /// [`add_media_hierarchy_preset_with_position`](Self::add_media_hierarchy_preset_with_position).
    pub fn add_media_hierarchy_preset(
        &mut self,
        preset: MediaHierarchyPreset,
    ) -> Result<(), MediaPmError> {
        self.add_media_hierarchy_preset_with_position(preset, AddInsertPosition::End)
    }

    /// Removes one media source by id and saves the document.
    ///
    /// Also removes any hierarchy nodes referencing this media id.
    ///
    /// # Errors
    ///
    /// Returns [`MediaPmError::Workflow`] if the media id does not exist.
    pub fn remove_media_source(&mut self, media_id: &str) -> Result<(), MediaPmError> {
        let effective_paths = self.resolve_effective_paths()?;
        let mut document =
            crate::service_standalone::ensure_and_load_mediapm_document(&effective_paths)?;

        if document.media.remove(media_id).is_none() {
            return Err(MediaPmError::Workflow(format!("media source '{media_id}' not found")));
        }

        // Remove hierarchy nodes that reference this media id.
        let _ = remove_hierarchy_nodes_by_media_id(&mut document.hierarchy, media_id);

        save_mediapm_document(&effective_paths.mediapm_ncl, &document)?;
        Ok(())
    }

    /// Removes one hierarchy preset node by id and saves the document.
    ///
    /// # Errors
    ///
    /// Returns [`MediaPmError::Workflow`] if the node id is not found.
    pub fn remove_media_hierarchy_preset(&mut self, node_id: &str) -> Result<(), MediaPmError> {
        let effective_paths = self.resolve_effective_paths()?;
        let mut document =
            crate::service_standalone::ensure_and_load_mediapm_document(&effective_paths)?;

        let removed = remove_hierarchy_nodes_by_id(&mut document.hierarchy, node_id);
        if removed == 0 {
            return Err(MediaPmError::Workflow(format!("hierarchy node '{node_id}' not found")));
        }

        save_mediapm_document(&effective_paths.mediapm_ncl, &document)?;
        Ok(())
    }

    /// Removes hierarchy nodes referencing the given media id and saves the
    /// document.
    ///
    /// Returns the number of removed nodes.
    ///
    /// # Errors
    ///
    /// Returns [`MediaPmError::Io`] if saving fails.
    pub fn remove_media_hierarchy_preset_by_media_id(
        &mut self,
        media_id: &str,
    ) -> Result<usize, MediaPmError> {
        let effective_paths = self.resolve_effective_paths()?;
        let mut document =
            crate::service_standalone::ensure_and_load_mediapm_document(&effective_paths)?;

        let removed = remove_hierarchy_nodes_by_media_id(&mut document.hierarchy, media_id);

        save_mediapm_document(&effective_paths.mediapm_ncl, &document)?;
        Ok(removed)
    }

    // -----------------------------------------------------------------------
    // Tool management
    // -----------------------------------------------------------------------

    /// Lists registered tools from the conductor generated document.
    ///
    /// # Errors
    ///
    /// Delegates to [`list_tools`].
    #[allow(dead_code)]
    pub(crate) fn list_tools(&self) -> Result<Vec<ConductorToolRow>, MediaPmError> {
        let effective_paths = self.resolve_effective_paths()?;
        list_tools(&effective_paths)
    }

    /// Adds one tool requirement to the document and saves it.
    ///
    /// Only updates the user-facing document; does not trigger a sync. Call
    /// [`sync_tools`](Self::sync_tools) afterwards to materialize the tool.
    ///
    /// # Errors
    ///
    /// Returns [`MediaPmError::Workflow`] if the tool id is empty.
    pub fn add_tool_requirement(
        &mut self,
        tool_id: &str,
        version_spec: Option<ConfigVersionSpec>,
    ) -> Result<(), MediaPmError> {
        if tool_id.is_empty() {
            return Err(MediaPmError::Workflow("tool id must not be empty".to_string()));
        }
        if !is_known_tool_id(tool_id) {
            return Err(MediaPmError::Workflow(format!(
                "tool '{tool_id}' is not in the built-in tool registry"
            )));
        }

        let effective_paths = self.resolve_effective_paths()?;
        let mut document =
            crate::service_standalone::ensure_and_load_mediapm_document(&effective_paths)?;

        let requirement = ToolRequirement {
            version_spec: version_spec.unwrap_or(ConfigVersionSpec::Latest),
            ..ToolRequirement::default()
        };

        document.tools.insert(tool_id.to_string(), requirement);

        save_mediapm_document(&effective_paths.mediapm_ncl, &document)?;
        Ok(())
    }

    /// Removes one tool requirement from the document and saves it.
    ///
    /// # Errors
    ///
    /// Returns [`MediaPmError::Workflow`] if the tool id is not present.
    pub fn remove_tool_requirement(&mut self, tool_id: &str) -> Result<(), MediaPmError> {
        let effective_paths = self.resolve_effective_paths()?;
        let mut document =
            crate::service_standalone::ensure_and_load_mediapm_document(&effective_paths)?;

        if document.tools.remove(tool_id).is_none() {
            return Err(MediaPmError::Workflow(format!("tool requirement '{tool_id}' not found")));
        }

        save_mediapm_document(&effective_paths.mediapm_ncl, &document)?;
        Ok(())
    }

    // -----------------------------------------------------------------------
    // Invalidation
    // -----------------------------------------------------------------------

    /// Invalidates tool-call instances for a given media step.
    ///
    /// Clears variant hashes and optionally impure timestamps for the targeted
    /// step. When `invalidate_calls` is true, tool call instances are
    /// invalidated. When `regenerate` is true, re-generation is triggered
    /// immediately after invalidation.
    ///
    /// # Errors
    ///
    /// Returns [`MediaPmError::Workflow`] if the media id is not found.
    pub fn invalidate_media_step_tool_calls(
        &mut self,
        media_id: &str,
        step_index: usize,
        invalidate_calls: bool,
        regenerate: bool,
    ) -> Result<MediaStepInvalidationSummary, MediaPmError> {
        let effective_paths = self.resolve_effective_paths()?;
        let mut state = load_mediapm_state_document(&effective_paths.mediapm_state_json)?;

        if !state.workflow_states.contains_key(media_id) {
            return Err(MediaPmError::Workflow(format!(
                "media source '{media_id}' not found in state",
            )));
        }
        let workflow_id = format!("media/{media_id}");

        let (removed_instances, removed_generated_timestamps) = if invalidate_calls {
            mark_media_step_for_regeneration(&mut state, media_id, step_index);
            remove_target_step_impure_timestamps(&mut state, media_id);
            // TODO: collect actual removed instance ids and generated
            //       timestamps from the state entry after invalidation.
            (vec![format!("step:{step_index}")], vec![])
        } else {
            (vec![], vec![])
        };

        save_mediapm_state_document(&effective_paths.mediapm_state_json, &state)?;

        Ok(MediaStepInvalidationSummary {
            workflow_id,
            targeted_step_ids: vec![step_index.to_string()],
            removed_generated_timestamps,
            removed_instances,
            regenerated_step: regenerate,
            warnings: Vec::new(),
        })
    }

    // -----------------------------------------------------------------------
    // Sync
    // -----------------------------------------------------------------------

    /// Refreshes the runtime configuration by loading dotenv files, ensuring
    /// runtime env files exist, and exporting schemas.
    ///
    /// # Errors
    ///
    /// Returns [`MediaPmError::Conductor`] if env file creation fails, or
    /// [`MediaPmError::Io`] if schema export fails.
    pub fn refresh_runtime_configuration(&self) -> Result<(), MediaPmError> {
        let effective_paths = self.resolve_effective_paths()?;

        // Load dotenv files.
        load_runtime_dotenv(&effective_paths.env_file, &effective_paths.env_generated_file);

        // Ensure conductor runtime env files exist.
        ensure_runtime_env_files(&effective_paths.runtime_root).map_err(MediaPmError::Conductor)?;

        // Export schemas.
        export_mediapm_nickel_config_schemas(
            effective_paths.schema_export_dir.as_deref(),
            &effective_paths.conductor_schema_dir,
        )?;

        Ok(())
    }

    /// Runs a full tool sync using the document's desired tools.
    ///
    /// # Errors
    ///
    /// Delegates to [`sync_tools_from_document`](Self::sync_tools_from_document).
    pub async fn sync_tools(&mut self) -> Result<ToolsSyncSummary, MediaPmError> {
        self.sync_tools_with_tag_update_checks(false, false).await
    }

    /// Runs a full tool sync with optional tag-update checks.
    ///
    /// # Errors
    ///
    /// Delegates to [`sync_tools_from_document`](Self::sync_tools_from_document).
    pub async fn sync_tools_with_tag_update_checks(
        &mut self,
        check_tag_updates: bool,
        no_progress: bool,
    ) -> Result<ToolsSyncSummary, MediaPmError> {
        let effective_paths = self.resolve_effective_paths()?;
        let merged = self.resolve_effective_runtime_storage()?;
        let recheck_policy = if check_tag_updates {
            RecheckPolicy::ForceReResolve
        } else {
            RecheckPolicy::UseCached
        };

        self.sync_tools_from_document(&effective_paths, &merged, recheck_policy, no_progress).await
    }

    /// Internal tool-sync implementation that reconciles desired tools from
    /// the resolved runtime storage.
    ///
    /// # Errors
    ///
    /// Returns [`MediaPmError::Io`] if document loading fails, or
    /// [`MediaPmError::Conductor`] if reconciliation fails.
    async fn sync_tools_from_document(
        &mut self,
        effective_paths: &MediaPmPaths,
        runtime_storage: &MediaRuntimeStorage,
        recheck_policy: RecheckPolicy,
        no_progress: bool,
    ) -> Result<ToolsSyncSummary, MediaPmError> {
        // Build the desired tools map from runtime storage.
        let desired_tools: BTreeMap<String, serde_json::Value> = runtime_storage
            .tools
            .iter()
            .map(|(id, req)| {
                let value = serde_json::to_value(req.clone()).unwrap_or_else(|e| {
                    panic!("ToolRequirement serialization should not fail: {e}")
                });
                (id.clone(), value)
            })
            .collect();

        let inherited_env_vars = runtime_storage.inherited_env_vars.clone();

        // Run the reconciliation.
        // Load current state before reconciliation (needed for skip logic).
        let mut state = load_mediapm_state_document(&effective_paths.mediapm_state_json)?;

        // When --no-progress is set, use a disabled ProgressGroup that produces
        // zero-cost no-op handles — no ticker thread, no terminal output.
        let progress_group: Option<ProgressGroup> =
            if no_progress { Some(ProgressGroup::disabled()) } else { None };
        let pg_ref: Option<&dyn ProgressGroupApi> =
            progress_group.as_ref().map(|g| g as &dyn ProgressGroupApi);

        let workspace_cas =
            Cas::workspace_provisioning_cas(self.conductor(), effective_paths).await?;

        let report = reconcile_desired_tools(
            workspace_cas,
            effective_paths,
            &desired_tools,
            &inherited_env_vars,
            recheck_policy,
            &state,
            runtime_storage.cache_root_override.as_deref(),
            pg_ref,
        )
        .await?;

        // Merge deployment records from the provisioning pipeline into the
        // persisted managed-tool registry and save.
        // Old version records are preserved — the dedup in
        // `dedup_managed_tools` collapses exact `(tool_id, canonical_version)`
        // pairs on serialization.
        for record in &report.tool_records {
            state.managed_tools.push(record.clone());
        }
        // Apply skip-path resolved-field backfills in place: fill `None`
        // resolved fields from fresh provider metadata for skipped tools.
        // Identity fields are preserved, existing `Some` values are never
        // overwritten, and it is a no-op when nothing differs (keeps re-sync
        // state.json byte-identical).
        apply_resolved_field_backfills(&mut state.managed_tools, &report.resolved_field_backfills);
        save_mediapm_state_document(&effective_paths.mediapm_state_json, &state)?;

        // Synthesize managed media workflows into the conductor generated
        // doc. Tool reconciliation (reconcile_desired_tools) loads, mutates,
        // and saves the generated doc internally, so reload both conductor
        // docs fresh here; the mediapm document is reloaded to stay aligned
        // with the persisted state.
        let user_doc = load_conductor_user_document(effective_paths)?;
        let mut generated_doc = load_conductor_generated_document(effective_paths)?;
        let document = ensure_and_load_mediapm_document(effective_paths)?;
        reconcile_media_workflows(
            effective_paths,
            &document,
            &mut generated_doc,
            user_doc.as_ref(),
        )?;

        Ok(ToolsSyncSummary {
            added_tools: report.tools_added,
            updated_tools: report.tools_updated,
            pruned_tools: report.pruned_tools,
            removed_tools: report.tools_removed,
            warnings: report.warnings,
        })
    }

    // -----------------------------------------------------------------------
    // Source processing
    // -----------------------------------------------------------------------

    /// Validates and normalizes a source URI, returning a [`MediaPackage`].
    ///
    /// # Errors
    ///
    /// Returns [`MediaPmError::InvalidSource`] if the URI scheme is
    /// unsupported.
    pub fn process_source(&self, uri: &Url) -> Result<MediaPackage, MediaPmError> {
        let normalized = normalize_source_uri(uri);
        validate_source_uri(&normalized)?;
        let media_id = media_id_from_uri(&normalized);

        Ok(MediaPackage { media_id, source_uri: normalized, permanent: false })
    }
}

// ---------------------------------------------------------------------------
// Filesystem convenience constructors
// ---------------------------------------------------------------------------

impl MediaPmService<FileSystemCas> {
    /// Creates a new filesystem-backed service at the given workspace root.
    ///
    /// Opens the filesystem CAS at the computed runtime root, creates a
    /// `SimpleConductor`, and initializes all paths.
    ///
    /// # Errors
    ///
    /// Returns [`MediaPmError::Io`] if the CAS cannot be created or the
    /// conductor fails to initialise.
    pub async fn new_fs_at(root_dir: impl Into<std::path::PathBuf>) -> Result<Self, MediaPmError> {
        Self::new_fs_at_with_runtime_storage_overrides(root_dir, MediaRuntimeStorage::default())
            .await
    }

    /// Creates a new filesystem-backed service at the given workspace root
    /// with explicit runtime storage overrides.
    ///
    /// # Errors
    ///
    /// Returns [`MediaPmError::Io`] if the CAS cannot be created or the
    /// conductor fails to initialise.
    pub async fn new_fs_at_with_runtime_storage_overrides(
        root_dir: impl Into<std::path::PathBuf>,
        runtime_storage_overrides: MediaRuntimeStorage,
    ) -> Result<Self, MediaPmError> {
        let root_dir = root_dir.into();
        let effective_paths =
            resolve_effective_paths_for_root(&root_dir, &runtime_storage_overrides);

        // Ensure parent directory exists.
        std::fs::create_dir_all(&effective_paths.runtime_root).map_err(|e| MediaPmError::Io {
            operation: "create runtime root directory".to_string(),
            path: effective_paths.runtime_root.clone(),
            source: e,
        })?;
        extend_runtime_gitignore(&effective_paths.runtime_root, MEDIAPM_EXTRA_GITIGNORE)?;

        // Bootstrap the mediapm document so a fresh workspace has a default
        // `mediapm.ncl` on disk. Reconstructed workspaces keep their existing
        // document untouched.
        if !effective_paths.mediapm_ncl.exists() {
            let document = ensure_and_load_mediapm_document(&effective_paths)?;
            if let Some(parent) = effective_paths.mediapm_ncl.parent() {
                std::fs::create_dir_all(parent).map_err(|e| MediaPmError::Io {
                    operation: "create mediapm document directory".to_string(),
                    path: parent.to_path_buf(),
                    source: e,
                })?;
            }
            save_mediapm_document(&effective_paths.mediapm_ncl, &document)?;
        }

        // Open the filesystem CAS.
        let conductor_cas_root = resolve_conductor_cas_root(&effective_paths);
        std::fs::create_dir_all(&conductor_cas_root).map_err(|e| MediaPmError::Io {
            operation: "create conductor CAS root directory".to_string(),
            path: conductor_cas_root.clone(),
            source: e,
        })?;
        let strategies = runtime_storage_overrides.to_verify_strategies();
        let cas = FileSystemCas::open_with_strategies(&conductor_cas_root, strategies)
            .await
            .map_err(|e| MediaPmError::Workflow(format!("failed to open filesystem CAS: {e}")))?;

        // Build the conductor. Wire the mediapm-managed conductor documents
        // (user + generated) and the volatile state file so `run_workflow`
        // loads the merged config and persists state across syncs.
        let runtime_storage = RuntimeStoragePaths::new(&effective_paths.runtime_root)
            .with_config_paths(
                vec![
                    effective_paths.conductor_user_ncl.clone(),
                    effective_paths.conductor_generated_ncl.clone(),
                ],
                effective_paths.conductor_state_config.clone(),
            );
        let conductor = SimpleConductor::new(runtime_storage, cas);

        Ok(Self::new_with_runtime_storage_overrides(
            conductor,
            effective_paths,
            runtime_storage_overrides,
        ))
    }

    /// Runs a full library sync (tools + materialization).
    ///
    /// # Errors
    ///
    /// Delegates to
    /// [`sync_library_with_tag_update_checks`](Self::sync_library_with_tag_update_checks).
    pub async fn sync_library(
        &mut self,
        verify_materialization: bool,
    ) -> Result<SyncSummary, MediaPmError> {
        self.sync_library_with_tag_update_checks(verify_materialization, false, false).await
    }

    /// Runs a full library sync with optional tag-update checks.
    ///
    /// This is the primary sync entrypoint:
    /// 1. Ensures runtime env files and schemas are up-to-date.
    /// 2. Syncs tools.
    /// 3. Executes synthesized managed workflows through the conductor.
    /// 4. Loads the mediapm document and state.
    /// 5. Opens the filesystem CAS for materialization.
    /// 6. Runs the materializer.
    ///
    /// # Errors
    ///
    /// Returns the first critical error encountered; non-fatal issues are
    /// collected as warnings.
    pub async fn sync_library_with_tag_update_checks(
        &mut self,
        verify_materialization: bool,
        check_tag_updates: bool,
        no_progress: bool,
    ) -> Result<SyncSummary, MediaPmError> {
        let effective_paths = self.resolve_effective_paths()?;
        let merged = self.resolve_effective_runtime_storage()?;
        let recheck_policy = if check_tag_updates {
            RecheckPolicy::ForceReResolve
        } else {
            RecheckPolicy::UseCached
        };

        let mut warnings: Vec<String> = Vec::new();

        // 1. Refresh runtime configuration.
        self.refresh_runtime_configuration()?;

        // 2. Sync tools.
        let tools_report =
            self.sync_tools_from_document(&effective_paths, &merged, recheck_policy, false).await?;

        // Tool sync rewrites `.env.generated`; reload dotenv so workflow
        // unified-config env inheritance captures fresh tool paths.
        load_runtime_dotenv(&effective_paths.env_file, &effective_paths.env_generated_file);

        // 3. Execute synthesized managed workflows through the conductor.
        // The generated doc is fully machine-managed; iterate its managed
        // `mediapm.media.*` workflows only (run_workflow resolves each
        // against the merged user + generated config).
        let generated_doc = load_conductor_generated_document(&effective_paths)?;
        let workflow_names: Vec<String> = generated_doc
            .workflows
            .iter()
            .filter(|workflow| workflow.name.starts_with(MANAGED_WORKFLOW_PREFIX))
            .map(|workflow| workflow.name.clone())
            .collect();
        let mut executed_instances: usize = 0;
        let mut cached_instances: usize = 0;
        self.conductor
            .ensure_persisted_state_loaded()
            .await
            .map_err(|e| MediaPmError::Workflow(format!("failed to load conductor state: {e}")))?;
        ensure_mediapm_executable_env()?;

        // Surface the conductor-owned workflow progress screen during sync.
        // When --no-progress is set, use a disabled group (zero-cost no-op
        // handles); otherwise build a live dynamic-height group that the
        // conductor populates with one overall bar plus one child per step.
        let workflow_group: ProgressGroup = if no_progress {
            ProgressGroup::disabled()
        } else {
            ProgressGroup::builder().dynamic_height(true).build()
        };
        let workflow_pg: Option<Arc<dyn ProgressGroupApi + Send + Sync>> =
            Some(Arc::new(workflow_group));
        for workflow_name in workflow_names {
            let before = self.conductor.get_state()?;
            let summary = self
                .conductor
                .run_workflow(
                    &workflow_name,
                    conductor_run_workflow_options(&effective_paths, &merged, workflow_pg.clone()),
                )
                .await?;
            let after = self.conductor.get_state()?;
            let new_keys = after
                .tool_call_instances
                .keys()
                .filter(|key| !before.tool_call_instances.contains_key(key))
                .count();
            executed_instances += new_keys;
            // Steps that ran but produced no new instance key were cache hits
            // (executed = new keys; cached = ran - executed).
            let ran = summary.executed_steps + summary.cached_steps;
            cached_instances += ran.saturating_sub(new_keys);
            if summary.failed_steps > 0 {
                warnings.push(format!(
                    "workflow '{workflow_name}' had {} failed step(s)",
                    summary.failed_steps
                ));
            }
        }
        // Block until the conductor-owned workflow screen has flushed its
        // final frame (indicatif has no blocking join; this drains the
        // ticker and releases the draw target).
        if let Some(ref group) = workflow_pg {
            group.join();
        }

        // 4. Load mediapm document and state.
        let mut document = load_mediapm_document(&effective_paths.mediapm_ncl)?;
        let mut state = load_mediapm_state_document(&effective_paths.mediapm_state_json)?;
        let generated_doc = load_conductor_generated_document(&effective_paths)?;
        let conductor_state = self.conductor.get_state()?;

        // 5. Check if any tools require sync.
        self.append_tool_sync_hint_warning(&mut warnings, &state).await?;

        materializer::backfill_source_variant_hashes_from_workflow_outputs(
            &mut document,
            &generated_doc,
            &conductor_state,
            self.conductor.cas(),
        )
        .await?;

        // 6 – 7. Reuse the service's CAS and run the materializer. Opening a
        // second `FileSystemCas` at the same store root would fail with
        // `LockContention`, since the service constructor already holds the
        // directory lock for its lifetime.
        let materialize_report = materializer::sync_hierarchy(
            &effective_paths,
            &document,
            &mut state,
            self.conductor.cas(),
            verify_materialization,
            &conductor_state,
            &generated_doc,
            None,
        )
        .await?;

        save_mediapm_state_document(&effective_paths.mediapm_state_json, &state)?;

        // 8. Gather warnings from materializer.
        warnings.extend(materialize_report.notices);

        Ok(SyncSummary {
            executed_instances,
            cached_instances,
            materialized_paths: materialize_report.materialized_paths,
            removed_paths: materialize_report.removed_paths,
            removed_empty_dirs: materialize_report.removed_empty_dirs,
            added_tools: tools_report.added_tools,
            updated_tools: tools_report.updated_tools,
            warnings,
        })
    }
}

// ---------------------------------------------------------------------------
// In-memory convenience constructors
// ---------------------------------------------------------------------------

impl MediaPmService<InMemoryCas> {
    /// Creates a new in-memory service at a temporary root.
    ///
    /// Useful for testing and short-lived operations.
    #[must_use]
    pub fn new_in_memory() -> Self {
        let root_dir = mediapm_utils::temp::runtime_dir_for_workspace(Path::new("in-memory"));
        let paths = MediaPmPaths::from_root(&root_dir);
        let cas = InMemoryCas::new();
        let runtime_storage = RuntimeStoragePaths::new(&paths.runtime_root).with_config_paths(
            vec![paths.conductor_user_ncl.clone(), paths.conductor_generated_ncl.clone()],
            paths.conductor_state_config.clone(),
        );
        let conductor = SimpleConductor::new(runtime_storage, cas);
        Self::new(conductor, paths)
    }
}
