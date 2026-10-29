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
//! - realized state: `.mediapm/state.json` by default (configurable).

pub mod builtins;
// The seam between the extracted CLI definition and this library. See the
// module docs for why the two conversions cannot live with the definition.
// Crate-visible only: the items are `From` impls, which apply wherever both
// types are nameable, so no caller has to name this module to reach them.
#[cfg(feature = "cli")]
pub(crate) mod cli;
pub(crate) mod conductor_bridge;
pub mod config;
/// Golden demo hierarchy layout contract (examples + tests).
#[doc(hidden)]
pub mod demo_hierarchy_spec;
mod error;
/// Example and test temp isolation (`MEDIAPM_EXAMPLE_*` env overrides, fallback roots, cleanup).
///
/// See `.agents/instructions/example-temp-isolation.instructions.md` for the full temp-directory model.
pub mod example_isolation;
pub(crate) mod global;
pub(crate) mod hierarchy;
pub(crate) mod materializer;
pub(crate) mod metadata_cache;
pub mod output;
pub(crate) mod path_component;
pub(crate) mod paths;
pub(crate) mod service;
pub(crate) mod service_standalone;
pub(crate) mod source_metadata;
pub mod state;
pub mod sync_report;
pub(crate) mod tools;
pub(crate) mod util;

use std::collections::BTreeMap;
use std::path::{Path, PathBuf};
use std::sync::Arc;

use url::Url;

pub use conductor_bridge::sync::{content_map_hash, find_active_tool_spec};
pub use config::versions::{
    apply_v1_contract, apply_v2_contract, apply_v3_contract, evaluate_mod_ncl_expression,
    validate_v1_document, validate_v2_document, validate_v3_document,
};
pub use config::{
    ConfigPayloadStaging, GenericOutputVariantConfig, HierarchyEntry, HierarchyEntryKind,
    HierarchyFolderRenameRule, HierarchyNode, HierarchyNodeKind, HierarchyPath, ManagedFileRecord,
    MaterializationMethod, MediaMetadataRegexTransform, MediaMetadataValue,
    MediaMetadataValueCandidate, MediaMetadataVariantBinding, MediaPmDocument, MediaPmState,
    MediaRuntimeStorage, MediaSourceSpec, MediaStep, MediaStepTool, OutputCaptureKind,
    OutputSaveConfig, OutputVariantValue, PayloadStaging, PlatformInheritedEnvVars,
    PlaylistEntryPathMode, PlaylistFormat, PlaylistItemRef, SanitizeNamesConfig, ToolRegistryEntry,
    ToolRequirement, TransformInputValue, VerifyStrategy, YtDlpOutputKind,
    YtDlpOutputVariantConfig, flatten_hierarchy_value, load_mediapm_document,
    load_mediapm_state_document, nest_hierarchy_value, regex_variant_selector,
    save_mediapm_document, save_mediapm_state_document,
};
pub use error::MediaPmError;
pub use global::{
    GlobalToolCachePruneSummary, GlobalToolCacheStatus, MEDIAPM_GIT_HASH, MediaPmGlobalPaths,
    ensure_global_directory_layout, global_tool_cache_clear, global_tool_cache_prune_expired,
    global_tool_cache_status,
};
pub use materializer::MaterializeReport;
pub use materializer::progress_labels::{MaterializationBarLabel, split_entry_path};
pub use mediapm_conductor::tools::provider::{ConfigVersionSpec, VersionSpec};
pub use paths::MediaPmPaths;
pub use service::MediaPmService;
pub use service_standalone::{registered_builtin_ids, resolve_effective_paths_for_root};
pub use sync_report::{
    SyncLibraryOptions, SyncPhaseObserver, SyncPhaseReport, SyncProgressOverrides,
};
pub use tools::provider::RecheckPolicy;

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
///
/// `skipped_paths` counts a clean outcome: the run wrote nothing. A record
/// already named the hash the run resolved for that path, and the file's
/// length matched the length of the CAS object that hash names. No hash is
/// computed over the target, so a same-length in-place edit still counts as a
/// skip. `missing_paths` counts the opposite, a library left short an entry
/// because the run could not produce its output, which the CLI reports as an
/// error on its summary line and exits non-zero for.
///
/// How the counters become an icon and an exit status is one rule, in
/// [`crate::output::sync_outcome`], with a short version: a missing path or a
/// failed workflow step is an error, a run that only warned is a warning, and
/// everything else is clean. No rule reads `skipped_paths`, so a skip can
/// neither raise nor lower an outcome.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct SyncSummary {
    /// Number of conductor instances executed during sync.
    pub executed_instances: usize,
    /// Number of conductor instances served from cache.
    pub cached_instances: usize,
    /// Number of hierarchy paths materialized to the resolved library root.
    pub materialized_paths: usize,
    /// Number of hierarchy paths the run left untouched because a record named
    /// the resolved hash and the file's length matched the length of the CAS
    /// object that hash names. The target's bytes are not hashed, so this is a
    /// length match, not a verification, and a same-length edit is not caught.
    pub skipped_paths: usize,
    /// Number of hierarchy paths the run left unwritten because it could not
    /// produce their output. Any count above zero makes the run an error.
    pub missing_paths: usize,
    /// Number of stale hierarchy paths removed.
    pub removed_paths: usize,
    /// Number of empty parent directories removed after stale path cleanup.
    pub removed_empty_dirs: usize,
    /// Number of tools the recorded registry held no entry for, so the pass
    /// deployed them into conductor machine config.
    pub added_tools: usize,
    /// Number of tools whose canonical version or content-map hash differs from
    /// the newest recorded entry, so the pass deployed a different artifact.
    pub updated_tools: usize,
    /// Number of tool entries pruned from the generated conductor document.
    pub pruned_tools: usize,
    /// Number of tools removed entirely.
    pub removed_tools: usize,
    /// Number of tools the pass deployed nothing new for: their canonical
    /// version was already provisioned, or the reconciled identity matched the
    /// newest recorded entry.
    pub skipped_tools: usize,
    /// Number of managed-workflow steps that failed across all workflows. Any
    /// count above zero makes the run an error: the media a failed step was to
    /// produce is not on disk.
    pub workflow_failed_steps: usize,
    /// Warnings surfaced during sync, one `Δ` line each. A run that carries
    /// warnings and nothing worse is a warning, not an error.
    pub warnings: Vec<String>,
}

/// Aggregate result of the managed-workflow phase.
#[derive(Debug, Clone, Default, PartialEq, Eq)]
pub struct WorkflowSyncSummary {
    /// Conductor instances newly executed in this sync.
    pub executed_instances: usize,
    /// Conductor instances served from cache.
    pub cached_instances: usize,
    /// Steps that failed after exhausting retries across all workflows.
    pub failed_steps: usize,
}

/// Aggregate result of the materialization phase.
///
/// A `missing_paths` count above zero means this phase left the library short
/// an entry, which is what both the screen's overall row and the phase's
/// summary line report as an error. A `skipped_paths` count is clean and is
/// not asked about anywhere.
#[derive(Debug, Clone, Default, PartialEq, Eq)]
pub struct MaterializationSyncSummary {
    /// Paths materialized (new or updated).
    pub materialized_paths: usize,
    /// Paths the run left untouched because a record named the resolved hash
    /// and the file's length matched the length of the CAS object that hash
    /// names. The target's bytes are not hashed, so a same-length edit is not
    /// caught.
    pub skipped_paths: usize,
    /// Paths the run left unwritten because it could not produce their output.
    pub missing_paths: usize,
    /// Stale paths removed.
    pub removed_paths: usize,
    /// Empty parent directories removed after stale-path cleanup.
    pub removed_empty_dirs: usize,
}

/// Summary of tool sync operations.
///
/// A non-empty [`warnings`](Self::warnings) list means at least one tool failed
/// to provision. [`crate::output::tool_sync_outcome`] reads that list, and the
/// CLI turns its answer into the warning exit status. The counts describe what
/// registered, not what failed, so they cannot stand in for the list.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ToolsSyncSummary {
    /// Number of tools the recorded registry held no entry for.
    pub added_tools: usize,
    /// Number of tools whose canonical version or content-map hash differs from
    /// the newest recorded entry.
    pub updated_tools: usize,
    /// Number of managed-tool entries whose `content_map` was cleared in the
    /// generated conductor config because a newer version superseded them.
    pub pruned_tools: usize,
    /// Number of tools removed entirely.
    pub removed_tools: usize,
    /// Number of tools the pass deployed nothing new for: their canonical
    /// version was already provisioned, or the reconciled identity matched the
    /// newest recorded entry.
    pub skipped_tools: usize,
    /// Tool-specific warnings, one per tool whose provisioning failed.
    ///
    /// Non-empty means [`crate::output::tool_sync_outcome`] answers
    /// [`ToolSyncOutcome::Warning`](crate::output::ToolSyncOutcome::Warning).
    pub warnings: Vec<String>,
}

/// Summary of one media step marker request.
///
/// The counts describe the recorded conductor instances the targeted steps
/// resolved to, which is the only place the request has an effect: invalidating
/// work a mediapm-side record could express was the destructive half this
/// replaces, and it no longer writes mediapm state at all.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct MediaStepInvalidationSummary {
    /// The mediapm workflow that owns the targeted step.
    pub workflow_id: String,
    /// Step ids in that workflow that the requested index resolves to.
    ///
    /// One media step can synthesize several conductor steps, so this is a list
    /// even when the caller named one index.
    pub targeted_step_ids: Vec<String>,
    /// Recorded instances whose marker this request set or cleared.
    pub changed_instances: usize,
    /// Recorded instances that already carried the requested marker state.
    ///
    /// Together with [`Self::changed_instances`] this distinguishes "nothing has
    /// served those step ids yet" (both zero) from "the request was already
    /// satisfied" (`changed` zero, `unchanged` not). Neither is an error: a
    /// retried request reports what it found.
    pub unchanged_instances: usize,
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

/// Loads runtime dotenv files for a resolved set of mediapm paths.
///
/// Reads `env_file` first (user-provided) then `env_generated_file` second
/// (machine-generated), so generated values can override user values.
/// Missing files are silently skipped.
pub fn load_runtime_dotenv(env_file: &Path, env_generated_file: &Path) {
    let _ = dotenvy::from_path_override(env_file);
    let _ = dotenvy::from_path_override(env_generated_file);
}

/// Writes the Nickel contract sources each document is validated against into
/// the directories a reader opens.
///
/// `schema_export_dir` receives this crate's registry and version contracts
/// (`mod.ncl`, `v1.ncl`, `v2.ncl`, `v3.ncl`) and `conductor_schema_dir` receives
/// the conductor's (`mod.ncl`, `v1.ncl`, `v2.ncl`, `v3.ncl`, `v4.ncl`). The bytes
/// written are the ones the runtime evaluates, embedded at compile time, so an exported
/// contract cannot describe a rule the process does not enforce and a schema
/// change reaches the export directory unchanged.
///
/// # Errors
///
/// Returns [`MediaPmError::Io`] when a directory or file cannot be written. A
/// `schema_export_dir` of `None` disables the mediapm half rather than failing.
pub fn export_config_contracts(
    schema_export_dir: Option<&Path>,
    conductor_schema_dir: &Path,
) -> Result<(), MediaPmError> {
    if let Some(export_dir) = schema_export_dir {
        write_contract_sources(export_dir, crate::config::versions::CONTRACT_SOURCES)?;
    }
    write_contract_sources(
        conductor_schema_dir,
        mediapm_conductor::config::versions::contract_sources(),
    )
}

/// Writes one set of contract sources into `dir`, creating it first.
///
/// Each source is written under the file name the registry imports it by, so
/// the exported set is evaluable on its own.
fn write_contract_sources(
    dir: &Path,
    sources: &[(&'static str, &'static str)],
) -> Result<(), MediaPmError> {
    std::fs::create_dir_all(dir).map_err(|error| MediaPmError::Io {
        operation: "create contract export dir".to_string(),
        path: dir.to_path_buf(),
        source: error,
    })?;
    for (name, source) in sources {
        let path = dir.join(name);
        std::fs::write(&path, source).map_err(|error| MediaPmError::Io {
            operation: "write contract source".to_string(),
            path,
            source: error,
        })?;
    }
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
        Some(&config_value.environment.inherited_env_vars),
        Some(&override_value.environment.inherited_env_vars),
    );

    MediaRuntimeStorage {
        paths: config::RuntimePathsConfig {
            mediapm_dir: pick_path(
                &override_value.paths.mediapm_dir,
                &config_value.paths.mediapm_dir,
            ),
            hierarchy_root_dir: pick_path(
                &override_value.paths.hierarchy_root_dir,
                &config_value.paths.hierarchy_root_dir,
            ),
            mediapm_state_config: override_value.paths.mediapm_state_config.clone(),
            conductor_config: pick_path(
                &override_value.paths.conductor_config,
                &config_value.paths.conductor_config,
            ),
            conductor_generated_config: pick_path(
                &override_value.paths.conductor_generated_config,
                &config_value.paths.conductor_generated_config,
            ),
            conductor_state_config: pick_path(
                &override_value.paths.conductor_state_config,
                &config_value.paths.conductor_state_config,
            ),
            conductor_schema_dir: pick_path(
                &override_value.paths.conductor_schema_dir,
                &config_value.paths.conductor_schema_dir,
            ),
            env_file: pick_path(&override_value.paths.env_file, &config_value.paths.env_file),
            env_generated_file: pick_path(
                &override_value.paths.env_generated_file,
                &config_value.paths.env_generated_file,
            ),
            mediapm_schema_dir: pick_path(
                &override_value.paths.mediapm_schema_dir,
                &config_value.paths.mediapm_schema_dir,
            ),
        },
        materialization: config::RuntimeMaterializationConfig {
            materialization_preference_order: override_value
                .materialization
                .materialization_preference_order
                .clone(),
            verify_materialization: override_value.materialization.verify_materialization,
            payload_staging: override_value.materialization.payload_staging,
        },
        verification: config::RuntimeVerificationConfig {
            verify_on_read: override_value.verification.verify_on_read.clone(),
            verify_on_read_sample_denominator: override_value
                .verification
                .verify_on_read_sample_denominator,
            verify_on_read_stale_timeout_secs: override_value
                .verification
                .verify_on_read_stale_timeout_secs,
        },
        caching: config::RuntimeCachingConfig {
            reconstructed_cache_ttl_seconds: override_value.caching.reconstructed_cache_ttl_seconds,
        },
        lifecycle: config::RuntimeLifecycleConfig {
            instance_ttl_seconds: override_value.lifecycle.instance_ttl_seconds,
        },
        environment: config::RuntimeEnvironmentConfig {
            inherited_env_vars: merged_inherited_env_vars.unwrap_or_default(),
            profiler_enabled: override_value.environment.profiler_enabled,
        },
        path_sanitization: override_value.path_sanitization.clone(),
        retry_impure: override_value.retry_impure,
        tools: {
            let mut m = config_value.tools.clone();
            m.extend(override_value.tools.clone());
            m
        },
        cache_root_override: override_value.cache_root_override.clone(),
    }
}

/// Picks the override path when it is set, otherwise falls back to the
/// config path.
///
/// `MediaRuntimeStorage` path fields are `PathBuf` (not `Option`), so an empty
/// buffer is the "unset" sentinel: the override wins only when non-empty.
#[must_use]
fn pick_path(override_value: &Path, config_value: &Path) -> PathBuf {
    if override_value.as_os_str().is_empty() {
        config_value.to_path_buf()
    } else {
        override_value.to_path_buf()
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

/// Returns true when the executable's file name is exactly `mediapm` or
/// `mediapm.exe`.
///
/// The comparison reads the file name rather than the file stem, so
/// `mediapm.sh`, `mediapm.bat` and every other single-extension spelling are
/// rejected alongside `mediapm-*` names: `mediapm-conductor` shares the prefix
/// and dispatches workflow steps, and it does not implement the `builtin`
/// subcommands the managed-tool launcher scripts invoke, so accepting it hands
/// every launcher-based tool a binary that cannot run them. The predicate
/// never stats the path, so an explicit `MEDIAPM_EXECUTABLE` override is
/// accepted on its name alone.
#[must_use]
fn is_mediapm_cli_executable(path: &std::path::Path) -> bool {
    matches!(path.file_name().and_then(|name| name.to_str()), Some("mediapm" | "mediapm.exe"))
}

/// Returns the mediapm CLI in `dir`, if one is built there.
///
/// Probes the names the CLI is built under, `mediapm` then `mediapm.exe`, in
/// that order. The `.exe` name is probed on every platform rather than under a
/// Windows gate: a Windows build produces `mediapm.exe`, and `Path::is_file`
/// performs no `PATHEXT` extension search, so an extension-less probe alone
/// never finds it. Every candidate also has to pass
/// [`is_mediapm_cli_executable`], so something else sitting under one of those
/// names is not mistaken for the CLI.
fn mediapm_cli_in(dir: &std::path::Path) -> Option<std::path::PathBuf> {
    ["mediapm", "mediapm.exe"]
        .into_iter()
        .map(|name| dir.join(name))
        .find(|candidate| is_mediapm_cli_executable(candidate) && candidate.is_file())
}

/// Resolves the mediapm CLI executable from an explicit override and the
/// current executable.
///
/// Precedence: a valid explicit override, the current executable when it is
/// the CLI, a CLI beside the current executable, then one in the parent
/// directory (the `target/debug` layout a test or example binary lives under).
/// Both neighbour positions probe the names the CLI is built under — `mediapm`
/// and `mediapm.exe` — so the `target/debug` fallback also reaches a Windows
/// build.
///
/// Returns `Ok(None)` when nothing usable exists. Returns `Err` only when an
/// explicit override was supplied and is not the mediapm CLI, because a
/// wrong override is a configuration error the caller must see.
fn resolve_mediapm_executable_path_in(
    configured: Option<&str>,
    current_exe: &std::path::Path,
) -> Result<Option<std::path::PathBuf>, MediaPmError> {
    if let Some(configured) = configured {
        let path = std::path::PathBuf::from(configured);
        if !is_mediapm_cli_executable(&path) {
            return Err(MediaPmError::Workflow(format!(
                "MEDIAPM_EXECUTABLE names '{}', which is not the mediapm CLI; set it to the mediapm binary",
                path.display()
            )));
        }
        return Ok(Some(path));
    }

    if is_mediapm_cli_executable(current_exe) {
        return Ok(Some(current_exe.to_path_buf()));
    }

    let current_dir = current_exe.parent();
    if let Some(found) = current_dir
        .and_then(mediapm_cli_in)
        .or_else(|| current_dir.and_then(std::path::Path::parent).and_then(mediapm_cli_in))
    {
        return Ok(Some(found));
    }

    Ok(None)
}

/// Resolves the mediapm CLI executable used by managed-tool launcher scripts.
///
/// Reads `MEDIAPM_EXECUTABLE` and the current executable path, then delegates
/// the precedence rules to [`resolve_mediapm_executable_path_in`]. A blank
/// `MEDIAPM_EXECUTABLE` counts as unset.
///
/// `Ok(None)` means no mediapm CLI exists on this host, a value the callers
/// interpret differently: [`ensure_mediapm_executable`] reports it as an error
/// while [`ensure_mediapm_executable_env`] only warns. The current executable
/// is never substituted for a missing CLI, because a host binary that is not
/// the CLI would then be exported as one.
///
/// # Errors
///
/// Returns [`MediaPmError::Workflow`] when the current executable path cannot
/// be read, when `MEDIAPM_EXECUTABLE` is not valid Unicode, or when the value
/// it holds is not the mediapm CLI.
pub(crate) fn resolve_mediapm_executable_path() -> Result<Option<std::path::PathBuf>, MediaPmError>
{
    let configured = match std::env::var("MEDIAPM_EXECUTABLE") {
        Ok(value) => Some(value),
        Err(std::env::VarError::NotPresent) => None,
        Err(error @ std::env::VarError::NotUnicode(_)) => {
            return Err(MediaPmError::Workflow(format!(
                "MEDIAPM_EXECUTABLE is not valid Unicode: {error}"
            )));
        }
    };
    let configured = configured.as_deref().map(str::trim).filter(|value| !value.is_empty());

    let current = std::env::current_exe().map_err(|error| {
        MediaPmError::Workflow(format!("failed to resolve current executable path: {error}"))
    })?;
    resolve_mediapm_executable_path_in(configured, &current)
}

/// Remedy stated wherever no mediapm CLI can be resolved.
///
/// Managed-tool launcher scripts dispatch through `MEDIAPM_EXECUTABLE`, so
/// every message reporting the condition has to name both ways out: build the
/// CLI, or point the variable at an existing build.
const MISSING_MEDIAPM_CLI_MESSAGE: &str = "no mediapm CLI executable found; build it with `cargo build --package mediapm` or set MEDIAPM_EXECUTABLE to the mediapm binary";

/// Exports `path` as `MEDIAPM_EXECUTABLE` for the child processes this process
/// spawns.
///
/// The variable is process-global state that conductor reads when it spawns a
/// workflow step, so this is the single side effect of resolution and it is
/// kept in one place: no caller can export a path it did not just resolve.
fn export_mediapm_executable(path: &std::path::Path) {
    // SAFETY: mediapm orchestration runs workflow steps sequentially on one
    // thread; the variable is read when conductor spawns child processes.
    unsafe {
        std::env::set_var("MEDIAPM_EXECUTABLE", path);
    }
}

/// Resolves the mediapm CLI executable and exports it as
/// `MEDIAPM_EXECUTABLE`, returning the resolved path.
///
/// Managed-tool launcher scripts dispatch through `MEDIAPM_EXECUTABLE`, so a
/// caller that runs launcher-based tools needs this to have succeeded.
///
/// # Errors
///
/// Returns [`MediaPmError::Workflow`] when no mediapm CLI can be resolved, or
/// when `MEDIAPM_EXECUTABLE` names a binary that is not the CLI.
pub fn ensure_mediapm_executable() -> Result<std::path::PathBuf, MediaPmError> {
    let resolved = resolve_mediapm_executable_path()?
        .ok_or_else(|| MediaPmError::Workflow(MISSING_MEDIAPM_CLI_MESSAGE.to_string()))?;
    export_mediapm_executable(&resolved);
    Ok(resolved)
}

/// Ensures launcher-based managed tools can resolve the mediapm executable.
///
/// Best effort by design: `sync_library` calls this before running workflows,
/// and a library embed whose tools are not launcher-based must not fail because
/// the host has no mediapm CLI built beside it. When nothing is resolvable this
/// exports nothing and warns with the remedy; it never fabricates a value and
/// never substitutes the current executable. A caller that genuinely needs the
/// variable wants [`ensure_mediapm_executable`] instead, which reports the
/// condition as an error.
///
/// # Errors
///
/// Returns [`MediaPmError::Workflow`] when the current executable path cannot
/// be read, when `MEDIAPM_EXECUTABLE` is not valid Unicode, or when the value
/// it holds is not the mediapm CLI: a configured-but-wrong override is a real
/// error and must not be swallowed.
pub(crate) fn ensure_mediapm_executable_env() -> Result<(), MediaPmError> {
    if let Some(executable) = resolve_mediapm_executable_path()? {
        export_mediapm_executable(&executable);
    } else {
        tracing::warn!("{}", MISSING_MEDIAPM_CLI_MESSAGE);
    }
    Ok(())
}

/// Builds conductor runtime options from resolved mediapm paths.
#[must_use]
pub(crate) fn conductor_run_workflow_options(
    _paths: &MediaPmPaths,
    runtime_storage: &MediaRuntimeStorage,
    progress_group: Option<Arc<dyn mediapm_utils::progress::ProgressScreenApi + Send + Sync>>,
    overall_bar: Option<Arc<dyn mediapm_utils::progress::ProgressBarApi>>,
) -> mediapm_conductor::RunWorkflowOptions {
    mediapm_conductor::RunWorkflowOptions {
        retry_impure: runtime_storage.retry_impure,
        progress_group,
        overall_bar,
        ..mediapm_conductor::RunWorkflowOptions::default()
    }
}

/// Builds default managed transform chain for one local-source CAS hash.
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
                OutputVariantValue::YtDlp(YtDlpOutputVariantConfig {
                    kind: YtDlpOutputKind::Primary,
                    ..Default::default()
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
                OutputVariantValue::YtDlp(YtDlpOutputVariantConfig {
                    kind: YtDlpOutputKind::Primary,
                    ..Default::default()
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
                OutputVariantValue::YtDlp(YtDlpOutputVariantConfig {
                    kind: YtDlpOutputKind::Primary,
                    ..Default::default()
                }),
            )]),
            options: BTreeMap::new(),
        },
    ]
}

#[cfg(test)]
mod tests {
    //! Unit tests for mediapm CLI executable resolution.
    //!
    //! Every case drives [`resolve_mediapm_executable_path_in`], which takes the
    //! override and the current executable as arguments, so no test reads or
    //! mutates process-global environment. The guarantee under test is that a
    //! host binary which is not the CLI is never resolved as one: exporting such
    //! a path as `MEDIAPM_EXECUTABLE` makes every managed-tool launcher script
    //! re-run the host binary instead of the tool it was built to dispatch.

    use super::{is_mediapm_cli_executable, resolve_mediapm_executable_path_in};
    use std::path::{Path, PathBuf};

    /// Creates the isolated artifact root a test writes its fake executables
    /// into. Fake executables are empty files: resolution checks existence only.
    fn artifact_root() -> tempfile::TempDir {
        mediapm_utils::temp::artifact_dir().expect("create artifact temp dir")
    }

    /// Returns `path` in the `&str` form the resolver takes for an override.
    ///
    /// # Panics
    ///
    /// Panics when the path is not valid Unicode, which a temp root never is.
    fn override_arg(path: &Path) -> &str {
        path.to_str().expect("temp path is valid Unicode")
    }

    /// A current executable that is not the CLI resolves to nothing, so the
    /// caller learns that no CLI exists instead of exporting the host binary.
    #[test]
    fn resolve_refuses_a_non_cli_current_executable() {
        let root = artifact_root();
        let current = root.path().join("examples").join("mediapm_demo_online");

        let resolved =
            resolve_mediapm_executable_path_in(None, &current).expect("resolution succeeds");

        assert_eq!(resolved, None);
    }

    /// An override that is not the CLI is a configuration error, so the caller
    /// sees which value is wrong instead of silently resolving something else.
    #[test]
    fn resolve_rejects_an_override_that_is_not_the_cli() {
        let root = artifact_root();
        let current = root.path().join("examples").join("mediapm_demo_online");
        let override_path = current.clone();

        let error =
            resolve_mediapm_executable_path_in(Some(override_arg(&override_path)), &current)
                .expect_err("a non-CLI override must be rejected");

        let message = error.to_string();
        assert!(message.contains("MEDIAPM_EXECUTABLE"), "message names the variable: {message}");
        assert!(
            message.contains(&override_path.display().to_string()),
            "message names the rejected path: {message}"
        );
    }

    /// An explicit override naming the CLI is the user's statement of where it
    /// lives, so it is accepted without stat'ing the path: the binary may be
    /// built later, and a build-then-run sequence must not depend on ordering.
    #[test]
    fn resolve_accepts_an_override_naming_the_cli() {
        let root = artifact_root();
        let cli = root.path().join("mediapm");
        let current = root.path().join("examples").join("mediapm_demo_online");

        let resolved = resolve_mediapm_executable_path_in(Some(override_arg(&cli)), &current)
            .expect("resolution succeeds");

        assert_eq!(resolved, Some(cli));
    }

    /// A process that already is the CLI resolves to itself, which is how a
    /// `mediapm` invocation hands its own path to the children it spawns.
    #[test]
    fn resolve_prefers_the_current_executable_over_siblings() {
        let root = artifact_root();
        let cli = root.path().join("mediapm");

        let resolved = resolve_mediapm_executable_path_in(None, &cli).expect("resolution succeeds");

        assert_eq!(resolved, Some(cli));
    }

    /// A test or example binary lives in `<target>/debug/deps`, so its direct
    /// sibling is that directory's CLI build. The neighbour wins over the
    /// target directory's own `mediapm`, which may be a different build.
    #[test]
    fn resolve_prefers_a_sibling_cli_over_the_target_dir_cli() {
        let root = artifact_root();
        let target_dir = root.path().join("debug");
        let deps_dir = target_dir.join("deps");
        std::fs::create_dir_all(&deps_dir).expect("create deps dir");
        let sibling = deps_dir.join("mediapm");
        let target_cli = target_dir.join("mediapm");
        std::fs::write(&sibling, b"").expect("write deps sibling cli stub");
        std::fs::write(&target_cli, b"").expect("write target dir cli stub");
        let current = deps_dir.join("mod-abc");

        let resolved =
            resolve_mediapm_executable_path_in(None, &current).expect("resolution succeeds");

        assert_eq!(resolved, Some(sibling));
    }

    /// With no `deps` sibling, the target directory's `mediapm` is still a CLI
    /// build of this workspace, so it is the fallback.
    #[test]
    fn resolve_falls_back_to_the_target_dir_cli() {
        let root = artifact_root();
        let target_dir = root.path().join("debug");
        let deps_dir = target_dir.join("deps");
        std::fs::create_dir_all(&deps_dir).expect("create deps dir");
        let target_cli = target_dir.join("mediapm");
        std::fs::write(&target_cli, b"").expect("write target dir cli stub");
        let current = deps_dir.join("mod-abc");

        let resolved =
            resolve_mediapm_executable_path_in(None, &current).expect("resolution succeeds");

        assert_eq!(resolved, Some(target_cli));
    }

    /// A Windows build names the CLI `mediapm.exe`, and `Path::is_file` searches
    /// no `PATHEXT` extensions, so the `deps` neighbour probe has to ask for that
    /// name explicitly.
    #[test]
    fn resolve_finds_a_windows_named_sibling_cli() {
        let root = artifact_root();
        let deps_dir = root.path().join("debug").join("deps");
        std::fs::create_dir_all(&deps_dir).expect("create deps dir");
        let sibling = deps_dir.join("mediapm.exe");
        std::fs::write(&sibling, b"").expect("write deps sibling cli stub");
        let current = deps_dir.join("mod-abc");

        let resolved =
            resolve_mediapm_executable_path_in(None, &current).expect("resolution succeeds");

        assert_eq!(resolved, Some(sibling));
    }

    /// The same Windows name has to be found by the target-directory fallback,
    /// otherwise a built CLI is reported as missing on Windows.
    #[test]
    fn resolve_finds_a_windows_named_target_dir_cli() {
        let root = artifact_root();
        let target_dir = root.path().join("debug");
        let deps_dir = target_dir.join("deps");
        std::fs::create_dir_all(&deps_dir).expect("create deps dir");
        let target_cli = target_dir.join("mediapm.exe");
        std::fs::write(&target_cli, b"").expect("write target dir cli stub");
        let current = deps_dir.join("mod-abc");

        let resolved =
            resolve_mediapm_executable_path_in(None, &current).expect("resolution succeeds");

        assert_eq!(resolved, Some(target_cli));
    }

    /// Windows ships the CLI as `mediapm.exe`, so the `.exe` suffix counts as
    /// the CLI without a platform gate.
    #[test]
    fn resolve_accepts_a_windows_cli_file_name() {
        assert!(is_mediapm_cli_executable(Path::new("mediapm.exe")));
    }

    /// Only the CLI's own names are accepted: `mediapm-conductor` and the
    /// example binaries share the `mediapm-` prefix, and exporting one of them
    /// hands the launcher a binary that does not implement its `builtin`
    /// subcommands. A single-extension spelling is a different program too, so
    /// `mediapm.bat` pins the file-name comparison against a file-stem one.
    #[test]
    fn is_mediapm_cli_executable_accepts_only_the_cli_name() {
        let accepted = [PathBuf::from("mediapm"), PathBuf::from("mediapm.exe")];
        let rejected = [
            PathBuf::from("mediapm-conductor"),
            PathBuf::from("mediapm-cas"),
            PathBuf::from("mediapm_demo_online"),
            PathBuf::from("mediapm.bat"),
            PathBuf::from("mod-abc"),
        ];

        for name in accepted {
            assert!(is_mediapm_cli_executable(&name), "accepts {name:?}");
        }
        for name in rejected {
            assert!(!is_mediapm_cli_executable(&name), "rejects {name:?}");
        }
    }
}
