//! Conductor NCL document loading, saving, and builtin registration.
//!
//! This module manages the two-file conductor document model:
//! - `mediapm.conductor.ncl` — user-owned intent (optional; manual tools,
//!   workflows, and runtime tweaks). Never rewritten by mediapm.
//! - `mediapm.conductor.generated.ncl` — machine-generated tool definitions,
//!   rewritten wholesale on every sync and stamped with the shared
//!   generated-file banner.
//!
//! Mediapm machine state (`state.json`) and conductor runtime state live in
//! mediapm config and the conductor runtime state file respectively, not here.

use mediapm_conductor::{
    NickelDocument, ToolKindSpec, ToolRuntime, ToolSpec, decode_document, encode_document, tools,
};

use crate::conductor_bridge::constants::MEDIAPM_TOOL_ID_PREFIX;
use crate::error::MediaPmError;
use crate::paths::MediaPmPaths;
use crate::tools::workflows::MANAGED_WORKFLOW_PREFIX;

use super::util::write_bytes_if_changed;

// ── Namespace validation ─────────────────────────────────────────────────

/// Validates that a user-owned conductor document does not collide with the
/// mediapm-managed namespaces (two-file model, condition 2).
///
/// The `mediapm.tools.*` tool namespace and the `mediapm.media.*` workflow
/// namespace are reserved for mediapm's own machine-managed entries. A user
/// document that declares them would be ambiguous once the generated
/// document is merged at load time, so it is rejected with MPM-E009 and the
/// user is told to remove or rename the conflicting entry.
///
/// # Errors
///
/// Returns [`MediaPmError::ConfigValidation`] (MPM-E009) when a tool key or
/// workflow name falls inside a managed namespace.
pub(crate) fn validate_user_doc_namespaces(document: &NickelDocument) -> Result<(), MediaPmError> {
    let managed_tool_prefix = format!("{MEDIAPM_TOOL_ID_PREFIX}.");
    for key in document.tools.keys() {
        if key.starts_with(&managed_tool_prefix) {
            return Err(managed_namespace_error("tool key", key, &managed_tool_prefix));
        }
    }
    for workflow in &document.workflows {
        if workflow.name.starts_with(MANAGED_WORKFLOW_PREFIX) {
            return Err(managed_namespace_error(
                "workflow name",
                &workflow.name,
                MANAGED_WORKFLOW_PREFIX,
            ));
        }
    }
    Ok(())
}

fn managed_namespace_error(what: &str, name: &str, prefix: &str) -> MediaPmError {
    MediaPmError::ConfigValidation {
        code: "MPM-E009",
        context: "validating user conductor document".to_string(),
        detail: format!("{what} '{name}' uses the reserved managed namespace '{prefix}'"),
        suggestion: "remove or rename the entry; mediapm owns the managed namespace".to_string(),
    }
}

/// Loads the user-owned conductor document (`mediapm.conductor.ncl`), if
/// present, validating that it does not collide with mediapm-managed
/// namespaces.
///
/// Returns `None` when the user document does not exist (the common case —
/// the generated document alone carries conductor state). The user document
/// is never a reconcile save target.
///
/// # Errors
///
/// Returns MPM-E009 when the user document uses a reserved managed
/// namespace, or a document I/O error when loading or decoding fails.
pub(crate) fn load_conductor_user_document(
    paths: &MediaPmPaths,
) -> Result<Option<NickelDocument>, MediaPmError> {
    let path = &paths.conductor_user_ncl;
    if !path.exists() {
        return Ok(None);
    }
    let document = load_conductor_document(path, "conductor user NCL")?;
    validate_user_doc_namespaces(&document)?;
    Ok(Some(document))
}

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
    load_conductor_document(&paths.conductor_generated_ncl, "conductor generated NCL")
}

/// Saves the conductor generated document (`.ncl`) to disk (only if changed).
pub(crate) fn save_conductor_generated_document(
    paths: &MediaPmPaths,
    document: &NickelDocument,
) -> Result<(), MediaPmError> {
    save_conductor_document(&paths.conductor_generated_ncl, document, "conductor generated NCL")
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
        .map(|key| {
            // Parse "{name}@{hash}" format. Bare keys (no '@hash') use the
            // key as the name with an empty version.
            if let Some(at_pos) = key.rfind('@') {
                let (name, version) = key.split_at(at_pos);
                ConductorToolRow {
                    name: name.to_string(),
                    version: version[1..].to_string(), // skip '@'
                    managed: true,
                }
            } else {
                ConductorToolRow { name: key.clone(), version: String::new(), managed: true }
            }
        })
        .collect();
    rows.sort_by(|a, b| a.name.cmp(&b.name));
    Ok(rows)
}

// ── Builtin registration ─────────────────────────────────────────────────

/// Registers missing builtin tool definitions into the generated document.
pub(crate) fn register_missing_builtin_tools(document: &mut NickelDocument) {
    for builtin in tools::ALL_BUILTINS {
        if !document.tools.contains_key(builtin.builtin_id) {
            document.tools.insert(
                builtin.builtin_id.to_string(),
                ToolSpec {
                    name: builtin.name.to_string(),
                    kind: ToolKindSpec::Builtin { builtin_id: builtin.builtin_id.to_string() },
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
pub(crate) fn apply_builtin_runtime_defaults(document: &mut NickelDocument) {
    for builtin in mediapm_conductor::tools::ALL_BUILTINS {
        if let Some(spec) = document.tools.get_mut(builtin.builtin_id) {
            // Set impure flag from builtin registration data.
            if builtin.is_impure && !spec.runtime.impure {
                spec.runtime.impure = true;
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use mediapm_conductor::WorkflowSpec;

    use super::*;

    fn doc_with_tool_keys(keys: &[&str]) -> NickelDocument {
        let tools = keys
            .iter()
            .map(|key| {
                (
                    (*key).to_string(),
                    ToolSpec {
                        name: key.split('@').next().unwrap_or(key).to_string(),
                        kind: ToolKindSpec::default(),
                        ..Default::default()
                    },
                )
            })
            .collect();
        NickelDocument { tools, ..Default::default() }
    }

    fn doc_with_workflow_names(names: &[&str]) -> NickelDocument {
        NickelDocument {
            workflows: names
                .iter()
                .map(|name| WorkflowSpec {
                    name: (*name).to_string(),
                    display_name: None,
                    description: None,
                    impure: false,
                    steps: vec![],
                })
                .collect(),
            ..Default::default()
        }
    }

    #[test]
    fn validate_user_doc_namespaces_accepts_clean_doc() {
        let doc = doc_with_tool_keys(&["user_script", "my-tool@v1"]);
        assert!(validate_user_doc_namespaces(&doc).is_ok());
        let doc = doc_with_workflow_names(&["transcode", "archive.manual"]);
        assert!(validate_user_doc_namespaces(&doc).is_ok());
    }

    #[test]
    fn validate_user_doc_namespaces_rejects_managed_tool_key() {
        let doc = doc_with_tool_keys(&["mediapm.tools.ffmpeg@blake3:abc"]);
        let err =
            validate_user_doc_namespaces(&doc).expect_err("managed tool key must be rejected");
        assert_eq!(err.code(), "MPM-E009");
        let msg = err.to_string();
        assert!(msg.contains("mediapm.tools."), "error must name the reserved prefix: {msg}");
    }

    #[test]
    fn validate_user_doc_namespaces_rejects_managed_workflow_name() {
        let doc = doc_with_workflow_names(&["mediapm.media.youtube"]);
        let err =
            validate_user_doc_namespaces(&doc).expect_err("managed workflow name must be rejected");
        assert_eq!(err.code(), "MPM-E009");
    }

    #[test]
    fn validate_user_doc_namespaces_accepts_non_managed_prefix_lookalikes() {
        // `mediapm.tools-extra` and `mediapm.media-extra` share the prefix
        // characters but fall outside the managed namespace (which is the
        // dotted `mediapm.tools.` / `mediapm.media.` form).
        let doc = doc_with_tool_keys(&["mediapm.tools-extra"]);
        assert!(validate_user_doc_namespaces(&doc).is_ok());
        let doc = doc_with_workflow_names(&["mediapm.media-extra"]);
        assert!(validate_user_doc_namespaces(&doc).is_ok());
    }

    #[test]
    fn load_conductor_user_document_returns_none_when_missing() {
        let tmp = mediapm_utils::temp::artifact_dir().unwrap();
        let paths = MediaPmPaths::from_root(tmp.path());
        let loaded = load_conductor_user_document(&paths).expect("missing user doc loads as None");
        assert!(loaded.is_none());
    }

    #[test]
    fn load_conductor_user_document_loads_and_validates() {
        let tmp = mediapm_utils::temp::artifact_dir().unwrap();
        let paths = MediaPmPaths::from_root(tmp.path());

        // A clean user doc round-trips through load + validate.
        let doc = doc_with_tool_keys(&["user_script"]);
        save_conductor_document(&paths.conductor_user_ncl, &doc, "test user NCL")
            .expect("save user doc");
        let loaded =
            load_conductor_user_document(&paths).expect("clean user doc loads").expect("some doc");
        assert_eq!(loaded.tools.len(), 1);

        // A doc with a managed-namespace entry fails validation with MPM-E009.
        let bad = doc_with_tool_keys(&["mediapm.tools.yt-dlp"]);
        save_conductor_document(&paths.conductor_user_ncl, &bad, "test user NCL")
            .expect("save bad user doc");
        let err = load_conductor_user_document(&paths).expect_err("managed namespace must fail");
        assert_eq!(err.code(), "MPM-E009");
    }
}
