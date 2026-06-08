//! `mediapm` media orchestration facade.
//!
//! This crate composes:
//! - CAS (`mediapm-cas`) for content identity/storage,
//! - Conductor (`mediapm-conductor`) for declarative workflow execution,
//! - `mediapm` policy/lock/materialization logic specialized for media libraries.
//!
//! State contract:
//! - desired state: `mediapm.ncl`,
//! - conductor runtime docs: `mediapm.conductor.ncl`,
//!   `mediapm.conductor.machine.ncl` (both configurable),
//! - realized state: `.mediapm/state.ncl` by default (configurable).

pub mod builtins;
mod conductor_bridge;
mod config;
mod error;
mod global;
mod hierarchy;
mod http_client;
mod materializer;
mod metadata_cache;
mod paths;
mod service;
mod source_metadata;
mod tools;

use std::collections::BTreeMap;
use std::fs;
use std::path::{Path, PathBuf};

use mediapm_conductor::runtime_env::load_runtime_env_files;
use mediapm_conductor::{RunWorkflowOptions, RuntimeStoragePaths};
use url::Url;

pub use conductor_bridge::{ConductorToolRow, ToolSyncReport};
pub use config::{
    HierarchyEntry, HierarchyEntryKind, HierarchyFolderRenameRule, HierarchyNode,
    HierarchyNodeKind, HierarchyPath, ManagedFileRecord, MaterializationMethod,
    MediaMetadataRegexTransform, MediaMetadataValue, MediaMetadataValueCandidate,
    MediaMetadataVariantBinding, MediaPmDocument, MediaPmState, MediaRuntimeStorage,
    MediaSourceSpec, MediaStep, MediaStepTool, PlatformInheritedEnvVars, PlaylistEntryPathMode,
    PlaylistFormat, PlaylistItemRef, SanitizeNamesConfig, ToolRegistryRecord, ToolRequirement,
    ToolRequirementDependencies, TransformInputValue, flatten_hierarchy_value,
    load_mediapm_document, load_mediapm_state_document, merge_mediapm_document_with_state,
    nest_hierarchy_value, regex_variant_selector, save_mediapm_document,
    save_mediapm_state_document,
};
pub use error::MediaPmError;
pub use global::MediaPmGlobalPaths;
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
    /// Number of empty parent directories removed after stale path cleanup.
    pub removed_empty_dirs: usize,
    /// Number of tools newly registered in conductor machine config.
    ///
    /// `mediapm sync` no longer reconciles tool requirements automatically,
    /// so this remains `0` unless policy changes in a future release.
    pub added_tools: usize,
    /// Number of tools updated/promoted in conductor machine config.
    ///
    /// `mediapm sync` no longer reconciles tool requirements automatically,
    /// so this remains `0` unless policy changes in a future release.
    pub updated_tools: usize,
    /// Non-fatal warnings surfaced during sync.
    pub warnings: Vec<String>,
}

/// Summary of one `mediapm tool sync` execution.
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

/// Summary of one `mediapm media invalidate` operation.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct MediaStepInvalidationSummary {
    /// Managed conductor workflow id targeted by this invalidation.
    pub workflow_id: String,
    /// Deterministic conductor step ids mapped from the requested media step.
    pub targeted_step_ids: Vec<String>,
    /// Number of volatile conductor impure-timestamp rows removed.
    pub removed_impure_timestamps: usize,
    /// Number of cached orchestration instances removed from state.
    pub removed_instances: usize,
    /// Whether mediapm step refresh state was invalidated before reconciliation.
    pub regenerated_step: bool,
}

/// Preset families supported by `mediapm hierarchy add/remove`.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum MediaHierarchyPreset {
    /// Local-source hierarchy preset.
    Local,
    /// Online-source (`yt-dlp`) hierarchy preset.
    YtDlp,
}

/// Supported insertion policies for add-command mutations.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum AddInsertPosition {
    /// Keep deterministic sorted insertion behavior (default).
    Sorted,
    /// Insert at the beginning of the affected logical group.
    Beginning,
    /// Insert at the end of the affected logical group.
    End,
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
pub use service::{
    MediaPmApi, MediaPmService, load_runtime_dotenv_for_root, registered_builtin_ids,
    resolve_effective_paths_for_root,
};

/// Ensures runtime dotenv files exist and loads key/value pairs into process env.
pub(crate) fn load_runtime_dotenv(paths: &MediaPmPaths) -> Result<(), MediaPmError> {
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
pub(crate) fn export_mediapm_nickel_config_schemas(
    paths: &MediaPmPaths,
) -> Result<(), MediaPmError> {
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
pub(crate) fn normalize_source_uri(uri: &Url) -> Url {
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
pub(crate) fn validate_source_uri(uri: &Url) -> Result<(), MediaPmError> {
    match uri.scheme() {
        "http" | "https" | "local" => Ok(()),
        _ => Err(MediaPmError::InvalidSource(
            "mediapm supports only http(s) and local:<id> schemes".to_string(),
        )),
    }
}

/// Derives a yt-dlp media id from a canonical source URI.
///
/// For `YouTube` (`www.youtube.com` / `youtube.com` / `youtu.be`), the id is
/// `youtube.<video_id>` using the `v=` query parameter so the identifier is
/// stable and human-readable.  For all other hosts the id falls back to
/// `<host_slug>.<content_hash_12>` where the hash provides collision
/// resistance.
pub(crate) fn media_id_from_uri(uri: &Url) -> String {
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

/// Derives a deterministic local media id from a CAS content hash.
///
/// Uses the first 12 hex characters of the CAS blake3 hash so the identifier
/// remains stable across repeated imports of the same file content.
/// The `local.` prefix makes the id preset visible in config files.
pub(crate) fn media_id_from_local_path(hash: &mediapm_cas::Hash) -> String {
    format!("local.{}", &hash.to_hex()[..12])
}

/// Merges config-declared runtime storage with service-level overrides.
///
/// Precedence order is: service override (for example CLI flag) >
/// `mediapm.ncl` value > built-in default.
#[must_use]
pub(crate) fn merge_runtime_storage(
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
        env_generated_file: override_value
            .env_generated_file
            .clone()
            .or_else(|| config_value.env_generated_file.clone()),
        mediapm_schema_dir: override_value
            .mediapm_schema_dir
            .clone()
            .or_else(|| config_value.mediapm_schema_dir.clone()),
        profiler_enabled: override_value.profiler_enabled.or(config_value.profiler_enabled),
        verify_materialization: override_value
            .verify_materialization
            .or(config_value.verify_materialization),
        instance_ttl_seconds: override_value
            .instance_ttl_seconds
            .or(config_value.instance_ttl_seconds),
        verify_on_read: override_value
            .verify_on_read
            .clone()
            .or_else(|| config_value.verify_on_read.clone()),
        path_sanitization: override_value
            .path_sanitization
            .clone()
            .or_else(|| config_value.path_sanitization.clone()),
        verify_on_read_sample_denominator: override_value
            .verify_on_read_sample_denominator
            .or(config_value.verify_on_read_sample_denominator),
        verify_on_read_stale_timeout_secs: override_value
            .verify_on_read_stale_timeout_secs
            .or(config_value.verify_on_read_stale_timeout_secs),
        reconstructed_bytes_cache_ttl_secs: override_value
            .reconstructed_bytes_cache_ttl_secs
            .or(config_value.reconstructed_bytes_cache_ttl_secs),
    }
}

/// Merges optional platform-keyed inherited env-var maps with deterministic
/// order and case-insensitive de-duplication.
#[must_use]
pub(crate) fn merge_platform_inherited_env_var_maps(
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
pub(crate) fn append_unique_env_var_names(target: &mut Vec<String>, source: &[String]) {
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

/// Builds conductor runtime options from resolved mediapm paths.
///
/// `mediapm` always provides grouped runtime-storage paths explicitly when it
/// invokes conductor so conductor runtime writes (volatile state + CAS store)
/// stay aligned with effective mediapm path policy rather than falling back to
/// standalone conductor defaults under `.conductor/`.
#[must_use]
pub(crate) fn conductor_run_workflow_options(
    paths: &MediaPmPaths,
    runtime_storage: &MediaRuntimeStorage,
) -> RunWorkflowOptions {
    RunWorkflowOptions {
        runtime_storage_paths: RuntimeStoragePaths {
            conductor_dir: paths.runtime_root.clone(),
            conductor_state_config: Some(paths.conductor_state_config.clone()),
            cas_store_dir: Some(paths.runtime_root.join("store")),
            conductor_schema_dir: Some(paths.conductor_schema_dir.clone()),
            conductor_tools_dir: Some(paths.tools_dir.clone()),
        },
        runtime_inherited_env_vars: runtime_storage.inherited_env_vars_with_defaults(),
        profiler_enabled: runtime_storage.profiler_enabled.unwrap_or(false),
        profile_output_path: runtime_storage
            .profiler_enabled
            .is_some_and(|enabled| enabled)
            .then(|| paths.runtime_root.join("profile.json")),
        cas_integrity_config: Some(runtime_storage.to_cas_integrity_config()),
        ..RunWorkflowOptions::default()
    }
}

/// Derives a fallback local title from one source path.
pub(crate) fn local_default_title(path: &Path) -> String {
    path.file_name()
        .map_or_else(|| path.display().to_string(), |value| value.to_string_lossy().to_string())
}

/// Builds default description for one local media source.
pub(crate) fn build_local_default_description(path: &Path, title: &str, artist: &str) -> String {
    let file_name = local_default_title(path);
    let mut lines = vec![format!("file: {file_name}")];
    lines.push(format!("title: {title}"));
    lines.push(format!("artist: {artist}"));
    lines.join("\n")
}

/// Resolves one local file extension value with a leading dot.
///
/// Missing extensions fall back to `.bin` so hierarchy interpolation keys can
/// remain defined for all local sources added through `media add --preset local`.
pub(crate) fn local_extension_with_dot(path: &Path) -> String {
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
pub(crate) fn local_source_default_steps(
    hash_text: &str,
    recording_mbid: Option<&str>,
    release_mbid: Option<&str>,
) -> Vec<MediaStep> {
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
            options: BTreeMap::from([
                (
                    "recording_mbid".to_string(),
                    TransformInputValue::String(recording_mbid.unwrap_or("").to_string()),
                ),
                (
                    "release_mbid".to_string(),
                    TransformInputValue::String(release_mbid.unwrap_or("").to_string()),
                ),
            ]),
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

#[cfg(test)]
mod tests {
    use std::collections::BTreeMap;

    use url::Url;

    use super::*;

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

    /// Ensures local preset media-tagger defaults explicitly include both
    /// optional `MusicBrainz` identifier fields as empty placeholders.
    #[test]
    fn local_preset_media_tagger_defaults_include_empty_mbids() {
        let steps = local_source_default_steps("blake3:deadbeef", None, None);
        let media_tagger_step = steps
            .iter()
            .find(|step| step.tool == MediaStepTool::MediaTagger)
            .expect("local preset should include media-tagger step");

        assert_eq!(
            media_tagger_step.options.get("recording_mbid"),
            Some(&TransformInputValue::String(String::new()))
        );
        assert_eq!(
            media_tagger_step.options.get("release_mbid"),
            Some(&TransformInputValue::String(String::new()))
        );
    }

    /// Ensures service-level runtime overrides keep precedence for retained
    /// runtime-storage fields.
    #[test]
    fn merge_runtime_storage_prefers_override_fields() {
        let config = MediaRuntimeStorage {
            env_file: Some("config.env".to_string()),
            env_generated_file: None,
            inherited_env_vars: Some(BTreeMap::from([(
                "windows".to_string(),
                vec!["SYSTEMROOT".to_string(), "PATH".to_string()],
            )])),
            ..MediaRuntimeStorage::default()
        };
        let override_value = MediaRuntimeStorage {
            env_file: Some("override.env".to_string()),
            env_generated_file: None,
            inherited_env_vars: Some(BTreeMap::from([
                ("WINDOWS".to_string(), vec!["path".to_string(), "TMPDIR".to_string()]),
                ("linux".to_string(), vec!["LD_LIBRARY_PATH".to_string()]),
            ])),
            ..MediaRuntimeStorage::default()
        };

        let merged = merge_runtime_storage(&config, &override_value);

        assert_eq!(merged.env_file.as_deref(), Some("override.env"));
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

    /// Ensures `instance_ttl_seconds` merges correctly: the override value wins
    /// when set, and `None` in the override preserves the config value.
    #[test]
    fn merge_runtime_storage_preserves_instance_ttl_override() {
        let config = MediaRuntimeStorage {
            instance_ttl_seconds: Some(3600),
            ..MediaRuntimeStorage::default()
        };
        let override_value_some = MediaRuntimeStorage {
            instance_ttl_seconds: Some(7200),
            ..MediaRuntimeStorage::default()
        };
        let override_value_none = MediaRuntimeStorage::default();

        let merged_some = merge_runtime_storage(&config, &override_value_some);
        let merged_none = merge_runtime_storage(&config, &override_value_none);

        assert_eq!(
            merged_some.instance_ttl_seconds,
            Some(7200),
            "override value should win when set"
        );
        assert_eq!(
            merged_none.instance_ttl_seconds,
            Some(3600),
            "config value should survive when override is None"
        );
    }

    /// Ensures short `YouTube` links are normalized to the canonical watch URL.
    #[test]
    fn normalize_source_uri_expands_short_youtube_links() {
        let short = Url::parse("https://youtu.be/dQw4w9WgXcQ?t=43").expect("url");

        let normalized = normalize_source_uri(&short);

        assert_eq!(normalized.as_str(), "https://www.youtube.com/watch?v=dQw4w9WgXcQ");
    }
}
