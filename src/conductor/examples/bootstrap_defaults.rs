//! Minimal bootstrap example for the conductor crate.
//!
//! This example intentionally starts with no `conductor.ncl` or
//! `conductor.machine.ncl` on disk. The conductor bootstraps a placeholder
//! workflow, persists program-edited state, and prints a compact execution
//! summary.
//!
//! Unlike `demo.rs`, this example uses an ephemeral temporary directory and
//! does not keep long-lived artifacts.

use std::error::Error;
use std::fs;
use std::path::{Path, PathBuf};
use std::sync::atomic::{AtomicU64, Ordering};
use std::time::{SystemTime, UNIX_EPOCH};

use mediapm_cas::FileSystemCas;
use mediapm_conductor::{ConductorApi, SimpleConductor};

/// Convenient result type shared by this example.
type ExampleResult<T> = Result<T, Box<dyn Error>>;

/// Best-effort temporary directory guard for non-persistent examples.
///
/// The directory is removed on drop so this lightweight example does not leave
/// long-lived artifacts behind.
#[derive(Debug)]
struct EphemeralRunDir {
    /// Absolute path of the temporary directory used by one example run.
    path: PathBuf,
}

impl EphemeralRunDir {
    /// Returns the temporary directory path.
    fn path(&self) -> &Path {
        &self.path
    }
}

impl Drop for EphemeralRunDir {
    /// Removes the temporary directory tree if it still exists.
    fn drop(&mut self) {
        if self.path.exists() {
            let _ = fs::remove_dir_all(&self.path);
        }
    }
}

/// Creates a unique temporary run directory that is deleted on drop.
fn create_ephemeral_run_dir(example_name: &str) -> ExampleResult<EphemeralRunDir> {
    static SEQUENCE: AtomicU64 = AtomicU64::new(1);

    let timestamp_ns = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .map(|duration| duration.as_nanos())
        .unwrap_or(0);
    let sequence = SEQUENCE.fetch_add(1, Ordering::Relaxed);
    let process_id = std::process::id();

    let directory_name = format!("{example_name}-{process_id}-{timestamp_ns}-{sequence}");
    let path = std::env::temp_dir().join("mediapm-conductor-examples").join(directory_name);
    fs::create_dir_all(&path)?;

    Ok(EphemeralRunDir { path })
}

/// Executes the bootstrap workflow scenario and prints observed behavior.
async fn run_bootstrap_demo() -> ExampleResult<()> {
    let run_dir = create_ephemeral_run_dir("bootstrap-defaults")?;
    let root = run_dir.path();
    let cas_root = root.join("cas-store");
    let user_path = root.join("conductor.ncl");
    let machine_path = root.join("conductor.machine.ncl");

    let conductor = SimpleConductor::new(FileSystemCas::open(&cas_root).await?);
    let run_summary = conductor.run_workflow(&user_path, &machine_path).await?;
    let state = conductor.get_state().await?;
    let diagnostics = conductor.get_runtime_diagnostics().await?;

    println!("temporary run directory (auto-cleaned): {}", root.display());
    println!("conductor.machine.ncl persisted: {}", machine_path.exists());
    println!(
        "run summary => executed: {}, cached: {}, rematerialized: {}",
        run_summary.executed_instances,
        run_summary.cached_instances,
        run_summary.rematerialized_instances,
    );
    println!("state instances: {}", state.instances.len());
    println!("worker pool size: {}", diagnostics.worker_pool_size);

    Ok(())
}

#[tokio::main]
/// Executes the bootstrap example.
async fn main() -> ExampleResult<()> {
    run_bootstrap_demo().await
}
