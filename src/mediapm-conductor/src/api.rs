//! Public API for the conductor crate.
//!
//! [`RuntimeStoragePaths`] centralizes file-system path resolution for a
//! conductor runtime root.  [`ConductorApi`] is the main public trait,
//! implemented by [`SimpleConductor`].

use std::path::{Path, PathBuf};

use mediapm_cas::CasApi;
use serde::{Deserialize, Serialize};

use crate::defaults;
use crate::error::ConductorError;
use crate::provision::helpers::sanitize_tool_id;

// ---------------------------------------------------------------------------
// Runtime storage paths
// ---------------------------------------------------------------------------

/// Resolved filesystem paths for one conductor runtime directory.
///
/// These paths are derived from a `conductor_dir` root with a consistent
/// subdirectory layout.  Use [`RuntimeStoragePaths::default_for`] to create a
/// set for a given root.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct RuntimeStoragePaths {
    /// Root runtime directory (e.g. `~/.conductor/`).
    pub conductor_dir: PathBuf,
    /// CAS content store subdirectory.
    pub cas_store_dir: PathBuf,
    /// Temporary files subdirectory.
    pub conductor_tmp_dir: PathBuf,
    /// Schema export subdirectory.
    pub conductor_schema_dir: PathBuf,
    /// Tool materialization subdirectory.
    pub conductor_tools_dir: PathBuf,
}

impl RuntimeStoragePaths {
    /// Creates storage paths rooted at `conductor_dir` with default subdirectory names.
    #[must_use]
    pub fn new(conductor_dir: &Path) -> Self {
        Self {
            conductor_dir: conductor_dir.to_path_buf(),
            cas_store_dir: conductor_dir.join(defaults::DEFAULT_CAS_STORE_DIR_NAME),
            conductor_tmp_dir: conductor_dir.join(defaults::DEFAULT_CONDUCTOR_TMP_DIR_NAME),
            conductor_schema_dir: conductor_dir.join(defaults::DEFAULT_CONDUCTOR_SCHEMA_DIR_NAME),
            conductor_tools_dir: conductor_dir.join(defaults::DEFAULT_CONDUCTOR_TOOLS_DIR_NAME),
        }
    }

    /// Creates storage paths from a `conductor_dir` root and optional overrides.
    ///
    /// Override paths that are relative are resolved relative to `conductor_dir`.
    /// Absolute override paths are used as-is.
    #[must_use]
    pub fn resolve_for(conductor_dir: &Path, overrides: &PathOverrides) -> Self {
        let resolve = |base: &Path, override_val: &Option<PathBuf>, subdir: &str| -> PathBuf {
            match override_val {
                Some(p) if p.is_absolute() => p.clone(),
                Some(p) => base.join(p),
                None => base.join(subdir),
            }
        };
        Self {
            conductor_dir: conductor_dir.to_path_buf(),
            cas_store_dir: resolve(
                conductor_dir,
                &overrides.store,
                defaults::DEFAULT_CAS_STORE_DIR_NAME,
            ),
            conductor_tmp_dir: resolve(
                conductor_dir,
                &overrides.tmp,
                defaults::DEFAULT_CONDUCTOR_TMP_DIR_NAME,
            ),
            conductor_schema_dir: resolve(
                conductor_dir,
                &overrides.schemas,
                defaults::DEFAULT_CONDUCTOR_SCHEMA_DIR_NAME,
            ),
            conductor_tools_dir: resolve(
                conductor_dir,
                &overrides.tools,
                defaults::DEFAULT_CONDUCTOR_TOOLS_DIR_NAME,
            ),
        }
    }
}

/// Optional overrides for runtime storage paths.
///
/// All paths are resolved relative to `conductor_dir` unless absolute.
#[derive(Debug, Clone, Default)]
pub struct PathOverrides {
    /// Override for CAS store path.
    pub store: Option<PathBuf>,
    /// Override for temp directory.
    pub tmp: Option<PathBuf>,
    /// Override for schemas directory.
    pub schemas: Option<PathBuf>,
    /// Override for tools directory.
    pub tools: Option<PathBuf>,
}

// ---------------------------------------------------------------------------
// Run summary
// ---------------------------------------------------------------------------

/// Aggregated counters for one workflow run.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize, Default)]
pub struct RunSummary {
    /// Total number of steps in the workflow.
    pub total_steps: usize,
    /// Number of steps that actually executed (not cache-hits).
    pub executed_steps: usize,
    /// Number of steps that were cache-hit reuse.
    pub cached_steps: usize,
    /// Number of steps that failed.
    pub failed_steps: usize,
}

// ---------------------------------------------------------------------------
// Runtime diagnostics
// ---------------------------------------------------------------------------

/// Scheduler + worker diagnostics snapshot.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize, Default)]
pub struct RuntimeDiagnostics {
    /// Number of pending (queued) steps.
    pub pending_steps: usize,
    /// Number of steps currently being executed.
    pub executing_steps: usize,
    /// Number of completed steps.
    pub completed_steps: usize,
    /// Number of failed steps.
    pub failed_steps: usize,
}

// ---------------------------------------------------------------------------
// ConductorApi trait
// ---------------------------------------------------------------------------

/// Public API surface for the conductor runtime.
///
/// Implementations may be backed by an in-process coordinator or an actor
/// client.
pub trait ConductorApi<C: CasApi>: Send + Sync {
    /// Runs a workflow by name with the given options.
    fn run_workflow_with_options(
        &self,
        workflow_name: &str,
        options: RunWorkflowOptions,
    ) -> impl std::future::Future<Output = Result<RunSummary, ConductorError>> + Send;

    /// Runs a workflow by name with default options.
    fn run_workflow(
        &self,
        workflow_name: &str,
    ) -> impl std::future::Future<Output = Result<RunSummary, ConductorError>> + Send {
        let wf = workflow_name.to_owned();
        async move { self.run_workflow_with_options(&wf, RunWorkflowOptions::default()).await }
    }

    /// Returns a snapshot of runtime diagnostics.
    fn get_runtime_diagnostics(
        &self,
    ) -> impl std::future::Future<Output = Result<RuntimeDiagnostics, ConductorError>> + Send;
}

/// Options for a single workflow run.
#[derive(Debug, Clone, Default)]
pub struct RunWorkflowOptions {
    /// Number of retry attempts for impure steps.
    pub retry_impure: bool,
    /// Optional tool selector override (managed tool id).
    pub tool_selector: Option<String>,
}

// ---------------------------------------------------------------------------
// Managed tool resolution
// ---------------------------------------------------------------------------

/// Result of resolving a managed tool's executable from the filesystem CAS.
#[derive(Debug, Clone)]
pub struct ManagedToolExecutableResolution {
    /// Absolute path to the resolved executable.
    pub executable_path: PathBuf,
    /// The tool's content map hash that was resolved.
    pub content_hash: mediapm_cas::Hash,
}

/// Resolves a managed tool's executable path from a `FilesystemCas`.
///
/// This is used by the mediapm integration layer to resolve managed tool
/// binaries without going through the full conductor workflow execution path.
///
/// # Errors
///
/// Returns [`ConductorError::Workflow`] if the tool is not managed or has no
/// content map, or [`ConductorError::Io`] on filesystem errors.
pub async fn resolve_managed_tool_executable_with_filesystem_cas(
    tool_id: &str,
    conductor_tools_dir: &Path,
    _cas: &mediapm_cas::FileSystemCas,
) -> Result<ManagedToolExecutableResolution, ConductorError> {
    // Ensure the tool content is extracted in the tools directory.
    let tool_dir = conductor_tools_dir.join(sanitize_tool_id(tool_id));
    let metadata_path = tool_dir.join("metadata.json");

    if !metadata_path.exists() {
        return Err(ConductorError::Workflow(format!(
            "tool '{tool_id}' has no extracted content in '{}'",
            conductor_tools_dir.display(),
        )));
    }

    let metadata_bytes = tokio::fs::read(&metadata_path)
        .await
        .map_err(|e| ConductorError::io("read tool metadata", metadata_path.clone(), e))?;
    let metadata: serde_json::Value = serde_json::from_slice(&metadata_bytes)
        .map_err(|e| ConductorError::Serialization(e.to_string()))?;

    let content_hash_str =
        metadata.get("content_hash").and_then(serde_json::Value::as_str).ok_or_else(|| {
            ConductorError::Workflow(format!("tool '{tool_id}' metadata missing content_hash"))
        })?;
    let content_hash: mediapm_cas::Hash = content_hash_str.parse().map_err(|_| {
        ConductorError::Workflow(format!(
            "tool '{tool_id}' has invalid content_hash: {content_hash_str}"
        ))
    })?;

    // Find the executable in the payload directory.
    let payload_dir = tool_dir.join("payload");
    if !payload_dir.is_dir() {
        return Err(ConductorError::Workflow(format!("tool '{tool_id}' has no payload directory")));
    }

    let executable_path = payload_dir.join(if cfg!(windows) { "index.exe" } else { "index" });
    if !executable_path.exists() {
        return Err(ConductorError::Workflow(format!(
            "tool '{tool_id}' has no executable at expected path: {}",
            executable_path.display()
        )));
    }

    Ok(ManagedToolExecutableResolution { executable_path, content_hash })
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn runtime_storage_paths_default() {
        let paths = RuntimeStoragePaths::new(Path::new("/tmp/conductor"));
        assert_eq!(paths.conductor_dir, Path::new("/tmp/conductor"));
        assert_eq!(paths.cas_store_dir, Path::new("/tmp/conductor/store"));
        assert_eq!(paths.conductor_tmp_dir, Path::new("/tmp/conductor/tmp"));
        assert_eq!(paths.conductor_schema_dir, Path::new("/tmp/conductor/schemas"));
        assert_eq!(paths.conductor_tools_dir, Path::new("/tmp/conductor/tools"));
    }

    #[test]
    fn runtime_storage_paths_with_overrides() {
        let paths = RuntimeStoragePaths::resolve_for(
            Path::new("/tmp/conductor"),
            &PathOverrides {
                store: Some(PathBuf::from("custom-store")),
                tools: Some(PathBuf::from("/absolute/tools")),
                ..PathOverrides::default()
            },
        );
        assert_eq!(paths.cas_store_dir, Path::new("/tmp/conductor/custom-store"));
        assert_eq!(paths.conductor_tools_dir, Path::new("/absolute/tools"));
    }
}
