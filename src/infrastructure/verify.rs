//! Workspace integrity verification.
//!
//! This module validates object availability, hash consistency, sidecar
//! invariants, and edit lineage references.
//!
//! Verification is intentionally separate from `sync`: users may want to audit
//! state without mutating anything, and CI can run integrity checks as a guard.

use std::{collections::HashSet, path::PathBuf};

use anyhow::Result;
use serde::Serialize;

use crate::infrastructure::store::{WorkspacePaths, hash_file, load_all_sidecars, object_relpath};

/// Verification report returned by `verify` command.
///
/// The report collects all issues in one pass instead of failing fast so users
/// can fix multiple problems in a single iteration.
#[derive(Debug, Default, Clone, Serialize)]
pub struct VerifyReport {
    /// Number of sidecars processed.
    pub sidecars_checked: usize,
    /// Number of variants processed.
    pub variants_checked: usize,
    /// Missing object-file references.
    pub missing_objects: Vec<String>,
    /// Hash mismatch entries where object bytes do not match sidecar hash.
    pub hash_mismatches: Vec<String>,
    /// Sidecar-level referential consistency issues.
    pub sidecar_reference_issues: Vec<String>,
    /// Edit-event reference issues.
    pub edit_reference_issues: Vec<String>,
}

impl VerifyReport {
    /// Return `true` when no integrity issues were found.
    pub fn is_clean(&self) -> bool {
        self.missing_objects.is_empty()
            && self.hash_mismatches.is_empty()
            && self.sidecar_reference_issues.is_empty()
            && self.edit_reference_issues.is_empty()
    }
}

/// Verify all sidecars and referenced objects in the workspace.
///
/// This function treats the sidecar set as source-of-truth for reachability and
/// lineage correctness, then validates object bytes against recorded hashes.
pub async fn verify_workspace(paths: &WorkspacePaths) -> Result<VerifyReport> {
    let mut report = VerifyReport::default();
    let sidecars = load_all_sidecars(paths).await?;

    report.sidecars_checked = sidecars.len();

    for sidecar in sidecars {
        let known_variants: HashSet<_> =
            sidecar.variants.iter().map(|variant| variant.variant_hash).collect();

        if !known_variants.contains(&sidecar.original.original_variant_hash) {
            report.sidecar_reference_issues.push(format!(
                "{}: original_variant_hash is missing from variants",
                sidecar.canonical_uri
            ));
        }

        for variant in &sidecar.variants {
            report.variants_checked += 1;

            let object_path = paths.root.join(PathBuf::from(&variant.object_relpath));
            if !object_path.exists() {
                report.missing_objects.push(format!(
                    "{}: {}",
                    sidecar.canonical_uri,
                    object_path.display()
                ));
                continue;
            }

            let expected_relpath = relpath_string(&object_relpath(&variant.variant_hash));
            let actual_relpath = variant.object_relpath.replace('\\', "/");
            if expected_relpath != actual_relpath {
                report.sidecar_reference_issues.push(format!(
                    "{}: object_relpath mismatch for {} (expected {}, got {})",
                    sidecar.canonical_uri, variant.variant_hash, expected_relpath, actual_relpath
                ));
            }

            let digest = hash_file(&object_path).await?;
            if digest != variant.variant_hash {
                report.hash_mismatches.push(format!(
                    "{}: object hash mismatch at {}",
                    sidecar.canonical_uri,
                    object_path.display()
                ));
            }
        }

        for edit in &sidecar.edits {
            if !known_variants.contains(&edit.from_variant_hash)
                || !known_variants.contains(&edit.to_variant_hash)
            {
                report.edit_reference_issues.push(format!(
                    "{}: invalid edit reference {} ({} -> {})",
                    sidecar.canonical_uri,
                    edit.event_id,
                    edit.from_variant_hash,
                    edit.to_variant_hash
                ));
            }
        }
    }

    Ok(report)
}

fn relpath_string(path: &std::path::Path) -> String {
    path.to_string_lossy().replace('\\', "/")
}
