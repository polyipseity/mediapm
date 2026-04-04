//! Manual optimizer walkthrough for a filesystem CAS.
//!
//! What this example does:
//! 1. Creates a temporary filesystem CAS root.
//! 2. Stores two similar payloads.
//! 3. Runs `optimize_once` to allow delta rewriting.
//! 4. Renders topology as Mermaid visualization markup.
//! 5. Prints rewritten object count + visualization preview.
//! 6. Cleans up the temporary root.
//!
//! Use `cas_artifact_inspection_demo.rs` when you want persistent on-disk artifacts
//! you can inspect with CLI commands later.

use bytes::Bytes;
use mediapm_cas::{CasApi, CasMaintenanceApi, FileSystemCas, OptimizeOptions};

/// Returns a unique temporary root for this optimize demonstration.
fn unique_demo_root() -> std::path::PathBuf {
    let stamp = std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .map(|d| d.as_nanos())
        .unwrap_or(0);
    std::env::temp_dir().join(format!("mediapm-cas-optimize-demo-{stamp}"))
}

/// Runs one manual optimization pass and returns `(rewrites, mermaid_markup)`.
async fn run_manual_optimize_once(
    root: &std::path::Path,
) -> Result<(usize, String), Box<dyn std::error::Error>> {
    let cas = FileSystemCas::open(root).await?;

    cas.put(Bytes::from_static(b"aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaBBBBBBBBBBBBBBBB")).await?;
    cas.put(Bytes::from_static(b"aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaCCCCCCCCCCCCCCCC")).await?;

    let report = cas.optimize_once(OptimizeOptions::default()).await?;
    let mermaid = cas.visualize_mermaid(false).await?;
    Ok((report.rewritten_objects, mermaid))
}

#[tokio::main]
/// Runs the manual optimize walkthrough and prints a Mermaid preview.
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    let root = unique_demo_root();
    let (rewritten_objects, mermaid) = run_manual_optimize_once(&root).await?;
    println!("manual optimize_once rewritten_objects={rewritten_objects}");
    println!("mermaid preview:\n{mermaid}");

    let _ = std::fs::remove_dir_all(&root);
    Ok(())
}

#[cfg(test)]
mod tests {
    #[tokio::test]
    /// Ensures the walkthrough flow completes and emits Mermaid output.
    async fn manual_optimize_runs_successfully() {
        let temp = tempfile::tempdir().expect("tempdir");
        let (rewritten, mermaid) =
            super::run_manual_optimize_once(temp.path()).await.expect("run manual optimize flow");
        assert!(rewritten <= 24, "default max_rewrites bound should apply");
        assert!(mermaid.contains("flowchart TD"));
    }
}
