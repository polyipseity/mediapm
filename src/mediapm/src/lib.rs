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
use std::path::{Path, PathBuf};
use std::process::Command as ProcessCommand;

use async_trait::async_trait;
use mediapm_cas::{CasApi, FileSystemCas, InMemoryCas};
use mediapm_conductor::{
    ConductorApi, ConductorError, MachineNickelDocument, RunSummary, RunWorkflowOptions,
    RuntimeStoragePaths, SimpleConductor,
};
use url::Url;

pub use conductor_bridge::{ConductorToolRow, ToolSyncReport};
pub use config::{
    HierarchyEntry, HierarchyEntryKind, HierarchyFolderRenameRule, HierarchyNode,
    HierarchyNodeKind, MaterializationMethod, MediaMetadataRegexTransform, MediaMetadataValue,
    MediaMetadataVariantBinding, MediaPmDocument, MediaPmState, MediaRuntimeStorage,
    MediaSourceSpec, MediaStep, MediaStepTool, PlatformInheritedEnvVars, PlaylistEntryPathMode,
    PlaylistFormat, PlaylistItemRef, ToolRequirement, ToolRequirementDependencies,
    TransformInputValue, flatten_hierarchy_value, load_mediapm_document,
    load_mediapm_state_document, merge_mediapm_document_with_state, nest_hierarchy_value,
    regex_variant_selector, save_mediapm_document, save_mediapm_state_document,
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

/// Canonical generated dotenv template for runtime credential guidance.
const RUNTIME_DOTENV_TEMPLATE: &str = concat!(
    "# mediapm runtime credential file\n",
    "#\n",
    "# This file is generated and loaded automatically by mediapm.\n",
    "# Keep secrets here instead of committing them into mediapm.ncl.\n",
    "#\n",
    "# Copy/paste and replace placeholder values as needed.\n",
    "# Lines beginning with # are comments.\n",
    "# By default every environment-variable assignment is commented so\n",
    "# shell/user-level environment values remain visible without .env overrides.\n",
    "\n",
    "# AcoustID API key used by media-tagger lookups.\n",
    "# For mediapm sync workflows, add ACOUSTID_API_KEY to runtime.inherited_env_vars\n",
    "# so conductor forwards this variable to tool subprocesses.\n",
    "# ACOUSTID_API_KEY=replace-with-your-acoustid-api-key\n",
    "\n",
    "# Optional endpoint overrides for advanced/self-hosted setups.\n",
    "# ACOUSTID_ENDPOINT=https://api.acoustid.org/v2/lookup\n",
    "# MUSICBRAINZ_ENDPOINT=https://musicbrainz.org/ws/2\n",
);

/// Canonical colocated `.gitignore` content for runtime dotenv protection.
const RUNTIME_DOTENV_GITIGNORE: &str = "/.env\n";

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
#[async_trait]
pub trait MediaPmApi: Send + Sync {
    /// Processes a single source URI using the configured media pipeline policy.
    async fn process_source(&self, uri: Url, permanent: bool)
    -> Result<MediaPackage, MediaPmError>;

    /// Reconciles declared media/tool state to filesystem/materialization state.
    async fn sync_library(&self) -> Result<SyncSummary, MediaPmError>;
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
    pub fn add_media_source(&self, uri: &Url) -> Result<String, MediaPmError> {
        validate_source_uri(uri)?;

        if uri.scheme() == "local" {
            return Err(MediaPmError::Workflow(
                "use 'media add-local <path>' for local sources so CAS hash pointers are recorded"
                    .to_string(),
            ));
        }

        let mut document = ensure_and_load_mediapm_document(&self.paths.mediapm_ncl)?;
        let media_id = media_id_from_uri(uri);
        let OnlineSourceMetadata { title, author, description } = fetch_online_source_metadata(uri);
        let source_title = title.unwrap_or_else(|| remote_default_title(uri));
        let source_description = description
            .unwrap_or_else(|| build_remote_default_description(&source_title, author.as_deref()));

        document.media.insert(
            media_id.clone(),
            MediaSourceSpec {
                id: None,
                description: Some(source_description),
                title: Some(source_title),
                workflow_id: None,
                metadata: Some(BTreeMap::from([
                    (
                        "title".to_string(),
                        MediaMetadataValue::Variant(MediaMetadataVariantBinding {
                            variant: "infojson".to_string(),
                            metadata_key: "title".to_string(),
                            transform: None,
                        }),
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
                            variant: "infojson".to_string(),
                            metadata_key: "ext".to_string(),
                            transform: Some(MediaMetadataRegexTransform {
                                pattern: "(.+)".to_string(),
                                replacement: ".$1".to_string(),
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
                                "source".to_string(),
                                serde_json::json!({
                                    "kind": "primary",
                                    "save": "full",
                                }),
                            ),
                            (
                                "infojson".to_string(),
                                serde_json::json!({
                                    "kind": "infojson",
                                    "save": "full",
                                }),
                            ),
                        ]),
                        options: BTreeMap::from([(
                            "uri".to_string(),
                            TransformInputValue::String(uri.to_string()),
                        )]),
                    },
                    MediaStep {
                        tool: MediaStepTool::Rsgain,
                        input_variants: vec!["source".to_string()],
                        output_variants: BTreeMap::from([(
                            "normalized".to_string(),
                            serde_json::json!({
                                "kind": "primary",
                                "save": "full",
                            }),
                        )]),
                        options: BTreeMap::new(),
                    },
                    MediaStep {
                        tool: MediaStepTool::MediaTagger,
                        input_variants: vec!["normalized".to_string()],
                        output_variants: BTreeMap::from([(
                            "default".to_string(),
                            serde_json::json!({
                                "kind": "primary",
                                "save": "full",
                            }),
                        )]),
                        options: BTreeMap::new(),
                    },
                ],
            },
        );

        save_mediapm_document(&self.paths.mediapm_ncl, &document)?;
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
    pub async fn add_local_source(&self, local_path: &Path) -> Result<String, MediaPmError> {
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

        let media_id = media_id_from_local_path(&absolute);
        let LocalSourceMetadata { title, description } = fetch_local_source_metadata(&absolute);
        let source_title = title.unwrap_or_else(|| local_default_title(&absolute));
        let source_description = description
            .unwrap_or_else(|| build_local_default_description(&absolute, &source_title));
        let source_extension_with_dot = local_extension_with_dot(&absolute);

        document.media.insert(
            media_id.clone(),
            MediaSourceSpec {
                id: None,
                description: Some(source_description),
                title: Some(source_title),
                workflow_id: None,
                metadata: Some(BTreeMap::from([
                    (
                        "title".to_string(),
                        MediaMetadataValue::Literal(local_default_title(&absolute)),
                    ),
                    (
                        "video_ext".to_string(),
                        MediaMetadataValue::Literal(source_extension_with_dot),
                    ),
                ])),
                variant_hashes: BTreeMap::new(),
                steps: vec![
                    MediaStep {
                        tool: MediaStepTool::Import,
                        input_variants: Vec::new(),
                        output_variants: BTreeMap::from([(
                            "source".to_string(),
                            serde_json::json!({
                                "kind": "primary",
                                "save": "full",
                            }),
                        )]),
                        options: BTreeMap::from([
                            (
                                "kind".to_string(),
                                TransformInputValue::String("cas_hash".to_string()),
                            ),
                            ("hash".to_string(), TransformInputValue::String(hash.to_string())),
                        ]),
                    },
                    MediaStep {
                        tool: MediaStepTool::Rsgain,
                        input_variants: vec!["source".to_string()],
                        output_variants: BTreeMap::from([(
                            "normalized".to_string(),
                            serde_json::json!({
                                "kind": "primary",
                                "save": "full",
                            }),
                        )]),
                        options: BTreeMap::new(),
                    },
                    MediaStep {
                        tool: MediaStepTool::MediaTagger,
                        input_variants: vec!["normalized".to_string()],
                        output_variants: BTreeMap::from([(
                            "default".to_string(),
                            serde_json::json!({
                                "kind": "primary",
                                "save": "full",
                            }),
                        )]),
                        options: BTreeMap::new(),
                    },
                ],
            },
        );
        save_mediapm_document(&self.paths.mediapm_ncl, &document)?;

        Ok(media_id)
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

#[async_trait]
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

/// Ensures runtime dotenv files exist and loads key/value pairs into process env.
fn load_runtime_dotenv(paths: &MediaPmPaths) -> Result<(), MediaPmError> {
    ensure_runtime_dotenv_files(paths)?;

    dotenvy::from_path_override(&paths.env_file).map_err(|source| {
        MediaPmError::Workflow(format!(
            "loading runtime dotenv file '{}' failed: {source}",
            paths.env_file.display()
        ))
    })?;

    Ok(())
}

/// Creates runtime `.env` and colocated `.gitignore` files when absent.
fn ensure_runtime_dotenv_files(paths: &MediaPmPaths) -> Result<(), MediaPmError> {
    let dotenv_parent =
        paths.env_file.parent().map_or_else(|| paths.runtime_root.clone(), Path::to_path_buf);

    fs::create_dir_all(&dotenv_parent).map_err(|source| MediaPmError::Io {
        operation: "creating runtime dotenv parent directory".to_string(),
        path: dotenv_parent.clone(),
        source,
    })?;

    if !paths.env_file.exists() {
        fs::write(&paths.env_file, RUNTIME_DOTENV_TEMPLATE.as_bytes()).map_err(|source| {
            MediaPmError::Io {
                operation: "writing runtime dotenv template".to_string(),
                path: paths.env_file.clone(),
                source,
            }
        })?;
    }

    let gitignore_path = dotenv_parent.join(".gitignore");
    let should_write_gitignore = match fs::read_to_string(&gitignore_path) {
        Ok(current) => current != RUNTIME_DOTENV_GITIGNORE,
        Err(error) if error.kind() == std::io::ErrorKind::NotFound => true,
        Err(source) => {
            return Err(MediaPmError::Io {
                operation: "reading runtime dotenv gitignore".to_string(),
                path: gitignore_path,
                source,
            });
        }
    };

    if should_write_gitignore {
        fs::write(&gitignore_path, RUNTIME_DOTENV_GITIGNORE.as_bytes()).map_err(|source| {
            MediaPmError::Io {
                operation: "writing runtime dotenv gitignore".to_string(),
                path: gitignore_path,
                source,
            }
        })?;
    }

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

/// Validates source URI policy (`http`, `https`, `local`).
fn validate_source_uri(uri: &Url) -> Result<(), MediaPmError> {
    match uri.scheme() {
        "http" | "https" | "local" => Ok(()),
        _ => Err(MediaPmError::InvalidSource(
            "phase-3 supports only http(s) and local:<id> schemes".to_string(),
        )),
    }
}

/// Derives stable media id from canonical source URI.
fn media_id_from_uri(uri: &Url) -> String {
    let hash = mediapm_cas::Hash::from_content(uri.as_str().as_bytes()).to_hex();
    format!("media-{}", &hash[..12])
}

/// Derives stable local media id from canonical filesystem path.
fn media_id_from_local_path(path: &Path) -> String {
    let hash = mediapm_cas::Hash::from_content(path.to_string_lossy().as_bytes()).to_hex();
    format!("local-{}", &hash[..12])
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
    lines.push("author: unknown".to_string());
    lines.join("\n")
}

/// Resolves one local file extension value with a leading dot.
///
/// Missing extensions fall back to `.bin` so hierarchy interpolation keys can
/// remain defined for all local sources added through `add-local`.
fn local_extension_with_dot(path: &Path) -> String {
    path.extension()
        .and_then(|value| value.to_str())
        .map(str::trim)
        .filter(|value| !value.is_empty())
        .map_or_else(|| ".bin".to_string(), |value| format!(".{value}"))
}

/// Builds default description for one remote media source.
fn build_remote_default_description(title: &str, author: Option<&str>) -> String {
    let author = author.map(str::trim).filter(|value| !value.is_empty()).unwrap_or("unknown");
    format!("title: {title}\nauthor: {author}")
}

/// Metadata tuple fetched by downloader-aware online probes.
#[derive(Debug, Clone, PartialEq, Eq, Default)]
struct OnlineSourceMetadata {
    /// Best-effort media title.
    title: Option<String>,
    /// Best-effort author/uploader label.
    author: Option<String>,
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

    if metadata.title.is_none() && metadata.author.is_none() && metadata.description.is_none() {
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
    let author = first_non_empty_json_string(
        value,
        &["uploader", "channel", "artist", "creator", "uploader_id"],
    );
    let description = first_non_empty_json_string(value, &["description", "summary"]);

    OnlineSourceMetadata { title, author, description }
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
        LocalSourceMetadata, MediaPmApi, MediaPmService, MediaRuntimeStorage, OnlineSourceMetadata,
        merge_runtime_storage, parse_local_source_metadata_from_ffprobe_json,
        parse_online_source_metadata, should_prefer_filesystem_workflow_runner,
        validate_source_uri,
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
        assert!(service.paths().runtime_root.join(".gitignore").exists());

        let dotenv_text =
            fs::read_to_string(service.paths().runtime_root.join(".env")).expect("read .env");
        assert!(dotenv_text.contains("# ACOUSTID_API_KEY="));
        assert!(dotenv_text.contains("# ACOUSTID_ENDPOINT="));
        assert!(dotenv_text.contains("# MUSICBRAINZ_ENDPOINT="));

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
        assert!(service.paths().runtime_root.join(".gitignore").exists());

        let dotenv_text =
            fs::read_to_string(service.paths().runtime_root.join(".env")).expect("read .env");
        assert!(dotenv_text.contains("# ACOUSTID_API_KEY="));
        assert!(dotenv_text.contains("# ACOUSTID_ENDPOINT="));
        assert!(dotenv_text.contains("# MUSICBRAINZ_ENDPOINT="));

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

    /// Ensures online metadata parsing extracts title/author/description when
    /// downloader JSON includes those fields.
    #[test]
    fn parse_online_metadata_reads_title_author_and_description() {
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
                author: Some("Demo Artist".to_string()),
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
