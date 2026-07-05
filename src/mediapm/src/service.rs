//! High-level orchestration service for mediapm.
//!
//! [`MediaPmService`] composes CAS + Conductor into the media-facing API.
//! Callers create a service instance bound to a workspace root, then call
//! methods to add/remove sources, sync tools, sync the library, and
//! invalidate cached steps.

use std::collections::BTreeMap;
use std::path::Path;

use mediapm_cas::{CasApi, CasMaintenanceApi, FileSystemCas, Hash, InMemoryCas};
use mediapm_conductor::runtime_env::ensure_runtime_env_files;
use mediapm_conductor::{RuntimeStoragePaths, SimpleConductor};
use url::Url;

use crate::conductor_bridge::documents::{ConductorToolRow, list_tools};
use crate::conductor_bridge::sync::reconcile_desired_tools;
use crate::config::{
    MediaMetadataValue, MediaPmState, MediaRuntimeStorage, MediaSourceSpec, ToolRequirement,
    load_mediapm_document, load_mediapm_state_document, save_mediapm_document,
    save_mediapm_state_document,
};
use crate::error::MediaPmError;
use crate::hierarchy::{
    insert_hierarchy_preset_node, remove_hierarchy_nodes_by_id, remove_hierarchy_nodes_by_media_id,
};
use crate::materializer;
use crate::paths::{MediaPmPathOverrides, MediaPmPaths};
pub(crate) use crate::service_standalone::*;
use crate::source_metadata::{fetch_local_source_metadata, resolve_conductor_cas_root};
use crate::tools::catalog::tool_catalog_entry;

use crate::{
    AddInsertPosition, MediaHierarchyPreset, MediaPackage, MediaStepInvalidationSummary,
    SyncSummary, ToolsSyncSummary, export_mediapm_nickel_config_schemas, load_runtime_dotenv,
    local_source_default_steps, media_id_from_local_path, media_id_from_uri, merge_runtime_storage,
    normalize_source_uri, validate_source_uri,
};

// ---------------------------------------------------------------------------
// Helpers
// ---------------------------------------------------------------------------

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
}

impl<Cas: CasApi + CasMaintenanceApi + Send + Sync + 'static> MediaPmService<Cas> {
    /// Creates a new service instance with the given conductor and paths.
    ///
    /// Runtime storage overrides default to [`MediaRuntimeStorage::default()`].
    #[must_use]
    pub fn new(conductor: SimpleConductor<Cas>, paths: MediaPmPaths) -> Self {
        Self { conductor, paths, runtime_storage_overrides: MediaRuntimeStorage::default() }
    }

    /// Creates a new service instance with explicit runtime storage overrides.
    #[must_use]
    pub fn new_with_runtime_storage_overrides(
        conductor: SimpleConductor<Cas>,
        paths: MediaPmPaths,
        runtime_storage_overrides: MediaRuntimeStorage,
    ) -> Self {
        Self { conductor, paths, runtime_storage_overrides }
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
    /// Returns `true` if the tool is missing from the state's tool table or
    /// its requirements have changed.
    pub fn logical_tool_requires_sync(
        &self,
        tool_id: &str,
        state: &MediaPmState,
    ) -> Result<bool, MediaPmError> {
        if let Some(existing) = state.tools.get(tool_id) {
            let effective = self.resolve_effective_runtime_storage()?;
            let desired = effective.tools.get(tool_id);
            // If no desired requirement is declared, the tool is considered
            // up-to-date when present in state.
            Ok(desired
                .is_some_and(|req| req.version != existing.version || req.tag != existing.tag))
        } else {
            Ok(true) // missing from state → requires sync
        }
    }

    /// Collects tool ids that require a sync based on state comparison.
    pub fn collect_tools_requiring_sync(
        &self,
        state: &MediaPmState,
    ) -> Result<Vec<String>, MediaPmError> {
        let effective = self.resolve_effective_runtime_storage()?;
        let mut needing_sync = Vec::new();
        for tool_id in effective.tools.keys() {
            if self.logical_tool_requires_sync(tool_id, state)? {
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
    pub fn append_tool_sync_hint_warning(
        &self,
        warnings: &mut Vec<String>,
        state: &MediaPmState,
    ) -> Result<(), MediaPmError> {
        let needing_sync = self.collect_tools_requiring_sync(state)?;
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
    #[allow(clippy::too_many_arguments, clippy::needless_pass_by_value)]
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

        // Fetch metadata via ffprobe.
        let metadata = fetch_local_source_metadata(path, ffprobe_command, None)?;
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
        version: Option<&str>,
        tag: Option<&str>,
    ) -> Result<(), MediaPmError> {
        if tool_id.is_empty() {
            return Err(MediaPmError::Workflow("tool id must not be empty".to_string()));
        }
        if tool_catalog_entry(tool_id).is_none() {
            return Err(MediaPmError::Workflow(format!(
                "tool '{tool_id}' is not in the built-in catalog"
            )));
        }

        let effective_paths = self.resolve_effective_paths()?;
        let mut document =
            crate::service_standalone::ensure_and_load_mediapm_document(&effective_paths)?;

        let requirement = ToolRequirement {
            version: version.map_or(MediaMetadataValue::Literal(String::new()), |v| {
                MediaMetadataValue::Literal(v.to_string())
            }),
            tag: tag.unwrap_or_default().to_string(),
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
        let mut state = load_mediapm_state_document(&effective_paths.mediapm_state_ncl)?;

        if !state.media.contains_key(media_id) {
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

        save_mediapm_state_document(&effective_paths.mediapm_state_ncl, &state)?;

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
        self.sync_tools_with_tag_update_checks(false).await
    }

    /// Runs a full tool sync with optional tag-update checks.
    ///
    /// # Errors
    ///
    /// Delegates to [`sync_tools_from_document`](Self::sync_tools_from_document).
    pub async fn sync_tools_with_tag_update_checks(
        &mut self,
        check_tag_updates: bool,
    ) -> Result<ToolsSyncSummary, MediaPmError> {
        let effective_paths = self.resolve_effective_paths()?;
        let merged = self.resolve_effective_runtime_storage()?;

        self.sync_tools_from_document(&effective_paths, &merged, check_tag_updates).await
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
        check_tag_updates: bool,
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
        let report = reconcile_desired_tools(
            &**self.conductor.cas(),
            effective_paths,
            &desired_tools,
            &inherited_env_vars,
            check_tag_updates,
        )
        .await?;

        // Load and update state with reconciled tools.
        let mut state = load_mediapm_state_document(&effective_paths.mediapm_state_ncl)?;
        for (tool_id, req) in &runtime_storage.tools {
            state.tools.insert(tool_id.clone(), req.clone());
        }
        save_mediapm_state_document(&effective_paths.mediapm_state_ncl, &state)?;

        Ok(ToolsSyncSummary {
            added_tools: report.tools_added,
            updated_tools: report.tools_updated,
            pruned_tools: 0, // stub: lifecycle prune not yet wired
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

        // Build the conductor.
        let runtime_storage = RuntimeStoragePaths::new(&effective_paths.runtime_root);
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
        self.sync_library_with_tag_update_checks(verify_materialization, false).await
    }

    /// Runs a full library sync with optional tag-update checks.
    ///
    /// This is the primary sync entrypoint:
    /// 1. Ensures runtime env files and schemas are up-to-date.
    /// 2. Syncs tools.
    /// 3. Loads the mediapm document and state.
    /// 4. Opens the filesystem CAS for materialization.
    /// 5. Runs the materializer.
    ///
    /// # Errors
    ///
    /// Returns the first critical error encountered; non-fatal issues are
    /// collected as warnings.
    pub async fn sync_library_with_tag_update_checks(
        &mut self,
        verify_materialization: bool,
        check_tag_updates: bool,
    ) -> Result<SyncSummary, MediaPmError> {
        let effective_paths = self.resolve_effective_paths()?;
        let merged = self.resolve_effective_runtime_storage()?;

        let mut warnings: Vec<String> = Vec::new();

        // 1. Refresh runtime configuration.
        self.refresh_runtime_configuration()?;

        // 2. Sync tools.
        let tools_report =
            self.sync_tools_from_document(&effective_paths, &merged, check_tag_updates).await?;

        // 3. Load mediapm document and state.
        let document = load_mediapm_document(&effective_paths.mediapm_ncl)?;
        let state = load_mediapm_state_document(&effective_paths.mediapm_state_ncl)?;

        // 4. Check if any tools require sync.
        self.append_tool_sync_hint_warning(&mut warnings, &state)?;

        // 5 – 6. Open CAS and run the materializer.
        let materialize_report = {
            let conductor_cas_root = resolve_conductor_cas_root(&effective_paths);
            let strategies = merged.to_verify_strategies();
            let cas = FileSystemCas::open_with_strategies(&conductor_cas_root, strategies)
                .await
                .map_err(|e| {
                    MediaPmError::Workflow(format!("failed to open filesystem CAS: {e}"))
                })?;
            materializer::sync_hierarchy(
                &effective_paths,
                &document,
                &state,
                &cas,
                verify_materialization,
            )
            .await?
        };

        // 7. Gather warnings from materializer.
        warnings.extend(materialize_report.notices);

        Ok(SyncSummary {
            executed_instances: 0, // stub: conductor not yet wired for full sync
            cached_instances: 0,
            rematerialized_instances: 0,
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
        let root_dir = std::env::temp_dir().join("mediapm-inmemory");
        let paths = MediaPmPaths::from_root(&root_dir);
        let cas = InMemoryCas::new();
        let runtime_storage = RuntimeStoragePaths::new(&paths.runtime_root);
        let conductor = SimpleConductor::new(runtime_storage, cas);
        Self::new(conductor, paths)
    }
}
