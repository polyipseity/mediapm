//! Temporary sandbox directory management and tool content materialization.

use std::collections::BTreeMap;
use std::path::{Path, PathBuf};

use mediapm_cas::{CasApi, Hash};

use crate::error::ConductorError;

/// Creates a temporary sandbox directory for step execution.
pub(super) async fn create_sandbox(
    base_tmp_dir: &Path,
    instance_key: &str,
) -> Result<PathBuf, ConductorError> {
    let sandbox_root = base_tmp_dir.join("sandbox").join(sanitize_for_path(instance_key));
    tokio::fs::create_dir_all(&sandbox_root)
        .await
        .map_err(|source| ConductorError::io("create sandbox directory", &sandbox_root, source))?;
    Ok(sandbox_root)
}

/// Sanitizes a string for use as a path component.
pub(super) fn sanitize_for_path(s: &str) -> String {
    s.replace(|c: char| !c.is_ascii_alphanumeric() && c != '_' && c != '-', "_")
}

/// Materializes tool content map entries into the sandbox directory.
pub(super) async fn materialize_content_map<C: CasApi + Send + Sync>(
    cas: &C,
    content_map: &BTreeMap<String, String>,
    sandbox_dir: &Path,
) -> Result<(), ConductorError> {
    for (relative_path, value) in content_map {
        // Try to parse as hash; if it fails, treat as inline bytes.
        if let Ok(hash) = value.parse::<Hash>() {
            let data = cas.get(hash).await.map_err(ConductorError::Cas)?;
            let target_path = sandbox_dir.join(relative_path);
            if let Some(parent) = target_path.parent() {
                tokio::fs::create_dir_all(parent).await.map_err(|source| {
                    ConductorError::io("create content parent directory", parent, source)
                })?;
            }
            tokio::fs::write(&target_path, &data)
                .await
                .map_err(|source| ConductorError::io("write tool content", &target_path, source))?;
        }
        // Non-hash values are skipped (inline descriptions).
    }
    Ok(())
}
