//! Minimal Phase 3 bootstrap example.
//!
//! This example demonstrates how a fresh workspace is initialized by `mediapm`.
//! It creates a temporary workspace, runs one `sync`, and prints the generated
//! state file locations.

use std::error::Error;
use std::path::{Path, PathBuf};

use mediapm::{MediaPmApi, MediaPmService};

/// Shared result type for this example.
type ExampleResult<T> = Result<T, Box<dyn Error>>;

/// Paths and summary produced by one bootstrap sync run.
#[derive(Debug, Clone)]
struct BootstrapRunResult {
    /// Workspace root where bootstrap files were written.
    workspace_root: PathBuf,
    /// Path to `mediapm.ncl`.
    mediapm_ncl: PathBuf,
    /// Path to conductor user config.
    conductor_user_ncl: PathBuf,
    /// Path to conductor machine config.
    conductor_machine_ncl: PathBuf,
    /// Path to lockfile.
    lock_jsonc: PathBuf,
    /// Executed conductor-instance count reported by sync.
    executed_instances: usize,
    /// Cache-hit instance count reported by sync.
    cached_instances: usize,
    /// Rematerialized instance count reported by sync.
    rematerialized_instances: usize,
    /// Materialized-path count reported by sync.
    materialized_paths: usize,
    /// Removed-path count reported by sync.
    removed_paths: usize,
}

/// Runs one bootstrap sync for a caller-provided workspace root.
async fn run_bootstrap_sync(workspace_root: &Path) -> ExampleResult<BootstrapRunResult> {
    let service = MediaPmService::new_in_memory_at(workspace_root);
    let summary = service.sync_library().await?;

    Ok(BootstrapRunResult {
        workspace_root: workspace_root.to_path_buf(),
        mediapm_ncl: service.paths().mediapm_ncl.clone(),
        conductor_user_ncl: service.paths().conductor_user_ncl.clone(),
        conductor_machine_ncl: service.paths().conductor_machine_ncl.clone(),
        lock_jsonc: service.paths().lock_jsonc.clone(),
        executed_instances: summary.executed_instances,
        cached_instances: summary.cached_instances,
        rematerialized_instances: summary.rematerialized_instances,
        materialized_paths: summary.materialized_paths,
        removed_paths: summary.removed_paths,
    })
}

#[tokio::main]
/// Runs one bootstrap sync in a temporary workspace.
async fn main() -> ExampleResult<()> {
    let workspace = tempfile::tempdir()?;
    let result = run_bootstrap_sync(workspace.path()).await?;

    println!("temporary workspace: {}", result.workspace_root.display());
    println!("mediapm.ncl: {}", result.mediapm_ncl.display());
    println!("conductor user document: {}", result.conductor_user_ncl.display());
    println!("conductor machine document: {}", result.conductor_machine_ncl.display());
    println!("lock file: {}", result.lock_jsonc.display());
    println!(
        "sync summary => executed={}, cached={}, rematerialized={}, materialized={}, removed={}",
        result.executed_instances,
        result.cached_instances,
        result.rematerialized_instances,
        result.materialized_paths,
        result.removed_paths,
    );

    Ok(())
}

#[cfg(test)]
mod tests {
    use std::fs;

    use mediapm_conductor::decode_machine_document;

    /// Verifies bootstrap sync creates expected state/config files.
    #[tokio::test]
    async fn bootstrap_sync_creates_default_documents() {
        let workspace = tempfile::tempdir().expect("tempdir");
        let result = super::run_bootstrap_sync(workspace.path())
            .await
            .expect("bootstrap sync should succeed");

        assert!(result.mediapm_ncl.exists(), "mediapm.ncl should be created");
        assert!(result.conductor_user_ncl.exists(), "conductor user document should be created");
        assert!(
            result.conductor_machine_ncl.exists(),
            "conductor machine document should be created"
        );
        assert!(result.lock_jsonc.exists(), "lock file should be created");
    }

    /// Verifies bootstrap registers builtins required by managed workflows.
    #[tokio::test]
    async fn bootstrap_sync_registers_all_builtins() {
        let workspace = tempfile::tempdir().expect("tempdir");
        let result = super::run_bootstrap_sync(workspace.path())
            .await
            .expect("bootstrap sync should succeed");

        let machine_text =
            fs::read_to_string(&result.conductor_machine_ncl).expect("read machine document");
        let machine = decode_machine_document(machine_text.as_bytes()).expect("decode machine");

        for tool_id in mediapm::registered_builtin_ids() {
            assert!(
                machine.tools.contains_key(tool_id),
                "expected builtin '{tool_id}' in machine tools"
            );
        }
    }
}
