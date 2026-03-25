//! JSON formatting/canonicalization helpers exposed as CLI functionality.
//!
//! Formatting is not cosmetic only: deterministic serialization lowers review
//! noise, helps merge behavior, and makes tool-generated changes predictable.

use std::path::Path;

use anyhow::Result;
use serde::Serialize;

use crate::{
    configuration::config::{AppConfig, load_config, save_config_pretty},
    infrastructure::store::{WorkspacePaths, load_all_sidecars, write_sidecar},
};

/// Report returned by the `fmt` command.
#[derive(Debug, Default, Clone, Serialize)]
pub struct FormatReport {
    /// Whether the config file was found and rewritten.
    pub config_written: bool,
    /// Number of sidecars rewritten in canonical key order.
    pub sidecars_rewritten: usize,
}

/// Canonicalize config and sidecar JSON files in the workspace.
///
/// This command is useful after migrations, automated updates, or any workflow
/// that may leave JSON key ordering inconsistent across files.
pub async fn format_workspace(paths: &WorkspacePaths, config_path: &Path) -> Result<FormatReport> {
    let mut report = FormatReport::default();

    if config_path.exists() {
        let config: AppConfig = load_config(config_path).await?;
        save_config_pretty(config_path, &config).await?;
        report.config_written = true;
    }

    for sidecar in load_all_sidecars(paths).await? {
        write_sidecar(paths, &sidecar).await?;
        report.sidecars_rewritten += 1;
    }

    Ok(report)
}
