//! Object-store garbage collection.
//!
//! GC roots are derived from current sidecar variant references. Unreferenced
//! object files are reported (dry-run) or removed (`apply=true`).
//!
//! This keeps storage bounded while preserving safety: by default GC is
//! inspect-first (dry-run), and only explicit apply mode performs deletion.

use std::{
    collections::HashSet,
    path::{Path, PathBuf},
};

use anyhow::Result;
use serde::Serialize;
use tokio::fs;
use walkdir::WalkDir;

use crate::infrastructure::store::{WorkspacePaths, load_all_sidecars};

/// Report produced by GC.
///
/// Candidate listing is included so users can inspect exactly what would be
/// removed before running destructive mode.
#[derive(Debug, Clone, Default, Serialize)]
pub struct GcReport {
    /// Count of object files reachable from sidecars.
    pub referenced_objects: usize,
    /// Number of unreferenced object files discovered.
    pub candidate_count: usize,
    /// Number of object files removed (only when `apply=true`).
    pub removed_count: usize,
    /// Full candidate path list.
    pub candidates: Vec<String>,
}

/// Run mark-and-sweep style object collection against sidecar roots.
///
/// "Mark" is performed by scanning sidecar variant references; "sweep" scans
/// object files and identifies entries outside that reachable set.
pub async fn gc_workspace(paths: &WorkspacePaths, apply: bool) -> Result<GcReport> {
    let sidecars = load_all_sidecars(paths).await?;

    let referenced_objects = sidecars
        .iter()
        .flat_map(|sidecar| sidecar.variants.iter())
        .map(|variant| paths.root.join(PathBuf::from(&variant.object_relpath)))
        .collect::<HashSet<_>>();

    let mut candidates = Vec::new();
    if paths.objects_dir.exists() {
        for entry in
            WalkDir::new(&paths.objects_dir).follow_links(false).into_iter().filter_map(Result::ok)
        {
            if !entry.file_type().is_file() {
                continue;
            }

            let candidate = entry.path().to_path_buf();
            if !referenced_objects.contains(&candidate) {
                candidates.push(candidate);
            }
        }
    }

    candidates.sort();

    let mut removed_count = 0_usize;
    if apply {
        for candidate in &candidates {
            if remove_path(candidate).await.is_ok() {
                removed_count += 1;
            }
        }
    }

    Ok(GcReport {
        referenced_objects: referenced_objects.len(),
        candidate_count: candidates.len(),
        removed_count,
        candidates: candidates.iter().map(|path| path.to_string_lossy().to_string()).collect(),
    })
}

async fn remove_path(path: &Path) -> Result<()> {
    let metadata = fs::symlink_metadata(path).await?;
    if metadata.is_file() || metadata.file_type().is_symlink() {
        fs::remove_file(path).await?;
    } else {
        fs::remove_dir_all(path).await?;
    }

    Ok(())
}
