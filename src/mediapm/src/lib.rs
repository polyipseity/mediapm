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
//!   `mediapm.conductor.generated.ncl` (both configurable),
//! - realized state: `.mediapm/state.ncl` by default (configurable).

pub mod builtins;
pub(crate) mod conductor_bridge;
mod config;
mod error;
pub(crate) mod global;
pub(crate) mod hierarchy;
pub(crate) mod http_client;
pub(crate) mod materializer;
pub(crate) mod metadata_cache;
pub mod output;
pub(crate) mod paths;
pub(crate) mod service;
pub(crate) mod service_standalone;
pub(crate) mod source_metadata;
pub(crate) mod tools;
pub(crate) mod util;

use std::collections::BTreeMap;
use std::path::Path;

use url::Url;

pub use config::{
    ActiveToolInstance, DecodedOutputVariantConfig, GenericOutputVariantConfig, HierarchyEntry,
    HierarchyEntryKind, HierarchyFolderRenameRule, HierarchyNode, HierarchyNodeKind, HierarchyPath,
    MaterializationMethod, MediaMetadataRegexTransform, MediaMetadataValue,
    MediaMetadataValueCandidate, MediaMetadataVariantBinding, MediaPmDocument, MediaPmState,
    MediaRuntimeStorage, MediaSourceSpec, MediaStep, MediaStepTool, OutputCaptureKind,
    OutputSaveConfig, PlatformInheritedEnvVars, PlaylistEntryPathMode, PlaylistFormat,
    PlaylistItemRef, SanitizeNamesConfig, ToolRegistryEntry, ToolRequirement,
    ToolRequirementDependencies, TransformInputValue, YtDlpOutputKind, YtDlpOutputVariantConfig,
    flatten_hierarchy_value, load_mediapm_document, load_mediapm_state_document,
    merge_mediapm_document_with_state, nest_hierarchy_value, regex_variant_selector,
    save_mediapm_document, save_mediapm_state_document,
};
pub use error::MediaPmError;
pub use global::{
    GlobalToolCachePruneSummary, GlobalToolCacheStatus, MediaPmGlobalPaths,
    ensure_global_directory_layout, global_tool_cache_clear, global_tool_cache_prune_expired,
    global_tool_cache_status,
};
pub use materializer::MaterializeReport;
pub use paths::MediaPmPaths;
pub use service::MediaPmService;
pub use service_standalone::{registered_builtin_ids, resolve_effective_paths_for_root};

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
    pub added_tools: usize,
    /// Number of tools updated/promoted in conductor machine config.
    pub updated_tools: usize,
    /// Non-fatal warnings surfaced during sync.
    pub warnings: Vec<String>,
}

/// Summary of tool sync operations.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ToolsSyncSummary {
    /// Number of tools newly registered.
    pub added_tools: usize,
    /// Number of tools updated.
    pub updated_tools: usize,
    /// Number of tools pruned from machine config.
    pub pruned_tools: usize,
    /// Number of tools removed entirely.
    pub removed_tools: usize,
    /// Tool-specific warnings.
    pub warnings: Vec<String>,
}

/// Summary of media step invalidation operations.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct MediaStepInvalidationSummary {
    /// The workflow id that was targeted.
    pub workflow_id: String,
    /// Step ids targeted by the invalidation.
    pub targeted_step_ids: Vec<String>,
    /// Removed generated timestamps (builtins with deterministic output).
    pub removed_generated_timestamps: Vec<String>,
    /// Tool call instances removed from impure builtins.
    pub removed_instances: Vec<String>,
    /// Whether regeneration was performed after invalidation.
    pub regenerated_step: bool,
    /// Warnings during invalidation.
    pub warnings: Vec<String>,
}

/// Describes where to insert a new item relative to existing items.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum AddInsertPosition {
    /// Insert in sorted (alphabetical) position.
    Sorted,
    /// Insert at the beginning of the list.
    Beginning,
    /// Insert at the end of the list.
    End,
}

/// Predefined media hierarchy presets for `mediapm hierarchy add`.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum MediaHierarchyPreset {
    /// Local-file library (flat `Artists/Album/` sort).
    Local,
    /// `yt-dlp` channel-based library (channel → playlist → media).
    YtDlpChannel,
}

// ---------------------------------------------------------------------------
// Utility functions
// ---------------------------------------------------------------------------

/// Loads runtime dotenv files for a resolved set of mediapm paths.
///
/// Reads `env_file` first (user-provided) then `env_generated_file` second
/// (machine-generated), so generated values can override user values.
/// Missing files are silently skipped.
pub fn load_runtime_dotenv(env_file: &Path, env_generated_file: &Path) {
    let _ = dotenvy::from_path_override(env_file);
    let _ = dotenvy::from_path_override(env_generated_file);
}

/// Exports mediapm and conductor JSON schemas to disk for both the
/// mediapm-native and conductor-managed schema directories.
///
/// # Errors
///
/// Returns [`MediaPmError::Io`] if schema directory creation or file writes
/// fail.
pub fn export_mediapm_nickel_config_schemas(
    schema_export_dir: Option<&Path>,
    conductor_schema_dir: &Path,
) -> Result<(), MediaPmError> {
    use std::fs;

    // Export mediapm native schema
    if let Some(export_dir) = schema_export_dir {
        fs::create_dir_all(export_dir).map_err(|e| MediaPmError::Io {
            operation: "create mediapm schema export dir".to_string(),
            path: export_dir.to_path_buf(),
            source: e,
        })?;
        let mediapm_schema = serde_json::json!({
            "$schema": "https://json-schema.org/draft-07/schema#",
            "title": "MediaPmConfig",
            "type": "object",
            "properties": {
                "version": { "type": "integer" },
                "media": { "type": "object" },
                "hierarchy": { "type": "array" },
                "runtime": { "type": "object" }
            }
        });
        let schema_path = export_dir.join("mediapm.schema.json");
        fs::write(
            &schema_path,
            serde_json::to_string_pretty(&mediapm_schema)
                .map_err(|e| MediaPmError::Serialization(e.to_string()))?,
        )
        .map_err(|e| MediaPmError::Io {
            operation: "write mediapm schema file".to_string(),
            path: schema_path,
            source: e,
        })?;
    }

    // Export conductor schema
    fs::create_dir_all(conductor_schema_dir).map_err(|e| MediaPmError::Io {
        operation: "create conductor schema export dir".to_string(),
        path: conductor_schema_dir.to_path_buf(),
        source: e,
    })?;
    let conductor_schema = serde_json::json!({
        "$schema": "https://json-schema.org/draft-07/schema#",
        "title": "ConductorConfig",
        "type": "object"
    });
    let conductor_schema_path = conductor_schema_dir.join("conductor.schema.json");
    fs::write(
        &conductor_schema_path,
        serde_json::to_string_pretty(&conductor_schema)
            .map_err(|e| MediaPmError::Serialization(e.to_string()))?,
    )
    .map_err(|e| MediaPmError::Io {
        operation: "write conductor schema file".to_string(),
        path: conductor_schema_path,
        source: e,
    })?;

    Ok(())
}

/// Normalizes a media source URI for stable identity.
///
/// Expands short `YouTube` links (`youtu.be/...`) to the canonical
/// `www.youtube.com/watch?v=...` form. Other URIs are returned unchanged.
#[must_use]
pub fn normalize_source_uri(uri: &Url) -> Url {
    let Some(host) = uri.host_str() else {
        return uri.clone();
    };

    if host.eq_ignore_ascii_case("youtu.be")
        && let Some(v) = uri.path().strip_prefix('/')
        && let Ok(mut normalized) = Url::parse("https://www.youtube.com/watch")
    {
        normalized.query_pairs_mut().append_pair("v", v);
        // Preserve timestamp query param
        for (key, value) in uri.query_pairs() {
            if key == "t" {
                normalized.query_pairs_mut().append_pair(&key, &value);
            }
        }
        return normalized;
    }

    uri.clone()
}

/// Validates a media source URI against supported scheme policies.
///
/// Supported schemes: `https`, `http`, `local`.
///
/// # Errors
///
/// Returns [`MediaPmError::InvalidSource`] when the scheme is unsupported.
pub fn validate_source_uri(uri: &Url) -> Result<(), MediaPmError> {
    let scheme = uri.scheme();
    match scheme {
        "https" | "http" | "local" => Ok(()),
        _ => Err(MediaPmError::InvalidSource(format!(
            "unsupported URI scheme '{scheme}'; expected 'https', 'http', or 'local'"
        ))),
    }
}

/// Derives a deterministic media id from a source URI.
///
/// For `https` URIs, uses the host slug plus 12 hex chars of the content hash.
/// For `local` URIs, uses the URI path as the media id.
#[must_use]
pub fn media_id_from_uri(uri: &Url) -> String {
    if uri.scheme() == "local" {
        // Strip `local:` prefix and use the path portion.
        uri.path().to_string()
    } else {
        let host = uri.host_str().unwrap_or("unknown");
        let host_slug = host.trim_start_matches("www.").replace('.', "-");
        let hash = mediapm_cas::Hash::from_content(uri.as_str().as_bytes()).to_hex();
        format!("{host_slug}.{}", &hash[..12])
    }
}

/// Derives a deterministic local media id from a CAS content hash.
///
/// Uses the first 12 hex characters of the CAS blake3 hash so the identifier
/// remains stable across repeated imports of the same file content.
/// The `local.` prefix makes the id preset visible in config files.
#[must_use]
pub fn media_id_from_local_path(hash: &mediapm_cas::Hash) -> String {
    format!("local.{}", &hash.to_hex()[..12])
}

/// Merges config-declared runtime storage with service-level overrides.
///
/// Precedence order is: service override > `mediapm.ncl` value > built-in default.
#[must_use]
pub fn merge_runtime_storage(
    config_value: &MediaRuntimeStorage,
    override_value: &MediaRuntimeStorage,
) -> MediaRuntimeStorage {
    let merged_inherited_env_vars = merge_platform_inherited_env_var_maps(
        Some(&config_value.inherited_env_vars),
        Some(&override_value.inherited_env_vars),
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
        materialization_preference_order: override_value.materialization_preference_order.clone(),
        tools: {
            let mut m = config_value.tools.clone();
            m.extend(override_value.tools.clone());
            m
        },
        verify_on_read: override_value.verify_on_read.clone(),
        verify_on_read_sample_denominator: override_value.verify_on_read_sample_denominator,
        verify_on_read_stale_timeout_secs: override_value.verify_on_read_stale_timeout_secs,
        reconstructed_cache_ttl_seconds: override_value.reconstructed_cache_ttl_seconds,
        instance_ttl_seconds: override_value.instance_ttl_seconds,
        inherited_env_vars: merged_inherited_env_vars.unwrap_or_default(),
        media_state_config: override_value.media_state_config.clone(),
        conductor_config: override_value
            .conductor_config
            .clone()
            .or_else(|| config_value.conductor_config.clone()),
        conductor_generated_config: override_value
            .conductor_generated_config
            .clone()
            .or_else(|| config_value.conductor_generated_config.clone()),
        conductor_state_config: override_value
            .conductor_state_config
            .clone()
            .or_else(|| config_value.conductor_state_config.clone()),
        conductor_schema_dir: override_value
            .conductor_schema_dir
            .clone()
            .or_else(|| config_value.conductor_schema_dir.clone()),
        env_file: override_value.env_file.clone().or_else(|| config_value.env_file.clone()),
        env_generated_file: override_value
            .env_generated_file
            .clone()
            .or_else(|| config_value.env_generated_file.clone()),
        mediapm_schema_dir: override_value
            .mediapm_schema_dir
            .clone()
            .or_else(|| config_value.mediapm_schema_dir.clone()),
        profiler_enabled: override_value.profiler_enabled,
        verify_materialization: override_value.verify_materialization,
        retry_impure: override_value.retry_impure,
        path_sanitization: override_value.path_sanitization.clone(),
    }
}

/// Merges optional platform-keyed inherited env-var maps with deterministic
/// order and case-insensitive de-duplication.
#[must_use]
pub fn merge_platform_inherited_env_var_maps(
    config_value: Option<&BTreeMap<String, Vec<String>>>,
    override_value: Option<&BTreeMap<String, Vec<String>>>,
) -> Option<BTreeMap<String, Vec<String>>> {
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
#[must_use]
#[allow(dead_code)]
pub(crate) fn conductor_run_workflow_options(
    _paths: &MediaPmPaths,
    runtime_storage: &MediaRuntimeStorage,
) -> mediapm_conductor::RunWorkflowOptions {
    mediapm_conductor::RunWorkflowOptions {
        retry_impure: runtime_storage.retry_impure,
        ..mediapm_conductor::RunWorkflowOptions::default()
    }
}

/// Derives a fallback local title from one source path.
#[must_use]
#[allow(dead_code)]
pub(crate) fn local_default_title(path: &Path) -> String {
    path.file_name()
        .map_or_else(|| path.display().to_string(), |value| value.to_string_lossy().to_string())
}

/// Builds default description for one local media source.
#[allow(dead_code)]
#[must_use]
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
#[allow(dead_code)]
/// remain defined for all local sources added through `media add --preset local`.
#[must_use]
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
#[allow(dead_code)]
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
