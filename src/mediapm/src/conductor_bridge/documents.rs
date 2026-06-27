//! Conductor NCL document loading, saving, and builtin registration.
//!
//! This module manages the four-document model:
//! - `mediapm.ncl` — user intent (loaded via mediapm config, not here)
//! - `conductor.generated.ncl` — machine-generated tool definitions
//! - `state.ncl` — machine mediapm state (loaded via mediapm config)
//! - `state.conductor.ncl` — conductor runtime state

use mediapm_conductor::{
    NickelDocument, ToolKindSpec, ToolRuntime, ToolSpec, decode_document, encode_document,
    registered_builtin_ids,
};

use crate::error::MediaPmError;
use crate::paths::MediaPmPaths;

use super::util::write_bytes_if_changed;

// ── Document load/save ───────────────────────────────────────────────────

/// Loads a conductor NCL document from disk, returning default if missing.
fn load_conductor_document(
    path: &std::path::Path,
    label: &str,
) -> Result<NickelDocument, MediaPmError> {
    if path.exists() {
        let bytes = std::fs::read(path).map_err(|source| MediaPmError::Io {
            operation: format!("reading {label}"),
            path: path.to_path_buf(),
            source,
        })?;
        decode_document(&bytes).map_err(|e| MediaPmError::ConductorDocument {
            operation: format!("decoding {label}"),
            path: path.to_path_buf(),
            detail: e.to_string(),
        })
    } else {
        Ok(NickelDocument::default())
    }
}

/// Encodes and writes a conductor NCL document to disk.
fn save_conductor_document(
    path: &std::path::Path,
    document: &NickelDocument,
    label: &str,
) -> Result<(), MediaPmError> {
    let bytes = encode_document(document.clone()).map_err(|e| MediaPmError::ConductorDocument {
        operation: format!("encoding {label}"),
        path: path.to_path_buf(),
        detail: e.to_string(),
    })?;
    write_bytes_if_changed(path, &bytes, &format!("saving {label}"))
}

/// Loads the conductor generated document (`.ncl`) from disk.
///
/// Returns an empty [`NickelDocument`] when the file does not exist.
pub(crate) fn load_conductor_generated_document(
    paths: &MediaPmPaths,
) -> Result<NickelDocument, MediaPmError> {
    load_conductor_document(&paths.conductor_machine_ncl, "conductor generated NCL")
}

/// Saves the conductor generated document (`.ncl`) to disk (only if changed).
pub(crate) fn save_conductor_generated_document(
    paths: &MediaPmPaths,
    document: &NickelDocument,
) -> Result<(), MediaPmError> {
    save_conductor_document(&paths.conductor_machine_ncl, document, "conductor generated NCL")
}

/// Loads the conductor runtime state document (`.ncl`) from disk.
///
/// Returns an empty [`NickelDocument`] when the file does not exist.
#[allow(dead_code)]
pub(crate) fn load_conductor_state_document(
    paths: &MediaPmPaths,
) -> Result<NickelDocument, MediaPmError> {
    load_conductor_document(&paths.conductor_state_config, "conductor runtime state")
}

/// Saves the conductor runtime state document (`.ncl`) to disk.
#[allow(dead_code)]
pub(crate) fn save_conductor_state_document(
    paths: &MediaPmPaths,
    document: &NickelDocument,
) -> Result<(), MediaPmError> {
    save_conductor_document(&paths.conductor_state_config, document, "conductor runtime state")
}

// ── Tool enumeration ─────────────────────────────────────────────────────

/// One row of tool metadata for `mediapm tool list` output.
#[allow(dead_code)]
#[derive(Debug, Clone)]
pub(crate) struct ConductorToolRow {
    /// Tool name (e.g. "yt-dlp").
    pub(crate) name: String,
    /// Tool version label.
    pub(crate) version: String,
    /// Whether this tool is managed by mediapm.
    pub(crate) managed: bool,
}

/// Lists registered tool specs from the conductor generated document.
pub(crate) fn list_tools(paths: &MediaPmPaths) -> Result<Vec<ConductorToolRow>, MediaPmError> {
    let doc = load_conductor_generated_document(paths)?;
    let tools = &doc.tools;

    let mut rows: Vec<ConductorToolRow> = tools
        .keys()
        .map(|name| ConductorToolRow { name: name.clone(), version: String::new(), managed: true })
        .collect();
    rows.sort_by(|a, b| a.name.cmp(&b.name));
    Ok(rows)
}

// ── Builtin registration ─────────────────────────────────────────────────

/// Registers missing builtin tool definitions into the generated document.
pub(crate) fn register_missing_builtin_tools(document: &mut NickelDocument) {
    for builtin_id in registered_builtin_ids() {
        if !document.tools.contains_key(&builtin_id) {
            document.tools.insert(
                builtin_id.clone(),
                ToolSpec {
                    name: builtin_id.clone(),
                    version: "latest".to_string(),
                    kind: ToolKindSpec::Builtin {
                        name: builtin_id.clone(),
                        version: "latest".to_string(),
                    },
                    inputs: std::collections::BTreeMap::new(),
                    default_inputs: std::collections::BTreeMap::new(),
                    outputs: std::collections::BTreeMap::new(),
                    runtime: ToolRuntime::default(),
                },
            );
        }
    }
}

/// Registers missing builtin tool configs into the generated document.
///
/// Applies default runtime configuration (impure flag) for each builtin
/// whose [`ToolSpec`] already exists in the document but lacks runtime
/// overrides.
pub(crate) fn register_missing_builtin_tool_configs(document: &mut NickelDocument) {
    for builtin in mediapm_conductor::tools::ALL_BUILTINS {
        if let Some(spec) = document.tools.get_mut(builtin.name) {
            // Set impure flag from builtin registration data.
            if builtin.is_impure && !spec.runtime.impure {
                spec.runtime.impure = true;
            }
        }
    }
}
