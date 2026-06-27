//! Nickel config evaluation and rendering utilities.

use std::path::Path;

use nickel_lang_core::eval::cache::CacheImpl;
use nickel_lang_core::program::ProgramBuilder;
use serde::Serialize;
use serde::de::DeserializeOwned;
use serde_json::Value;

use crate::error::MediaPmError;

/// Evaluates one Nickel source file at `path` to its exported JSON value.
pub fn evaluate_nickel_source_to_json(path: &Path) -> Result<Value, MediaPmError> {
    let mut program = ProgramBuilder::new()
        .add_path(path)
        .build::<CacheImpl>()
        .map_err(|err| MediaPmError::Workflow(format!("failed to create Nickel program: {err}")))?;

    let nickel_value = program
        .eval_full_for_export()
        .map_err(|err| MediaPmError::Workflow(format!("failed to evaluate '{path:?}': {err:?}")))?;

    serde_json::to_value(&nickel_value)
        .map_err(|err| MediaPmError::Serialization(format!("failed to render Nickel value: {err}")))
}

/// Parses one floating-point value into `u64` when it is a non-negative
/// integer within representable range.
#[must_use]
pub fn parse_non_negative_integral_u64(value: f64) -> Option<u64> {
    if value.is_sign_negative() || value.is_nan() || value.is_infinite() {
        return None;
    }
    if value.fract() != 0.0 {
        return None;
    }
    let unsigned = value as u64;
    if unsigned as f64 != value {
        return None;
    }
    Some(unsigned)
}

/// Parses one floating-point value into `u32` when it is a non-negative
/// integer within representable range.
#[must_use]
pub fn parse_non_negative_integral_u32(value: f64) -> Option<u32> {
    let raw_u64 = parse_non_negative_integral_u64(value)?;
    u32::try_from(raw_u64).ok()
}

/// Normalizes a Nickel-version-field document-level `version` marker.
///
/// Accepts both integer and floating-point JSON encoded version markers.
#[must_use]
pub fn normalize_version_field_to_u64(value: &serde_json::Value) -> Option<u64> {
    match value {
        serde_json::Value::Number(n) => {
            n.as_u64().or_else(|| n.as_f64().and_then(|f| parse_non_negative_integral_u64(f)))
        }
        _ => None,
    }
}

/// Loads and deserializes a JSON document from a Nickel source file.
fn load_json_document<T: DeserializeOwned>(path: &Path, label: &str) -> Result<T, MediaPmError> {
    let value = evaluate_nickel_source_to_json(path)?;
    serde_json::from_value(value)
        .map_err(|err| MediaPmError::Serialization(format!("failed to deserialize {label}: {err}")))
}

/// Serializes a document and writes it as pretty-printed JSON.
fn save_json_document<T: Serialize>(path: &Path, doc: &T, label: &str) -> Result<(), MediaPmError> {
    let value = serde_json::to_value(doc).map_err(|err| {
        MediaPmError::Serialization(format!("failed to serialize {label}: {err}"))
    })?;
    let json = serde_json::to_string_pretty(&value)
        .map_err(|err| MediaPmError::Serialization(format!("failed to format JSON: {err}")))?;
    std::fs::write(path, json).map_err(|err| MediaPmError::Io {
        operation: format!("write {label}"),
        path: path.to_path_buf(),
        source: err,
    })?;
    Ok(())
}

/// Loads and parses a `mediapm.ncl` file.
pub fn load_mediapm_document(path: &Path) -> Result<crate::config::MediaPmDocument, MediaPmError> {
    load_json_document(path, "mediapm document")
}

/// Serializes and writes a `mediapm.ncl` document.
pub fn save_mediapm_document(
    path: &Path,
    doc: &crate::config::MediaPmDocument,
) -> Result<(), MediaPmError> {
    save_json_document(path, doc, "mediapm document")
}

/// Loads and parses a `state.ncl` file.
pub fn load_mediapm_state_document(
    path: &Path,
) -> Result<crate::config::MediaPmState, MediaPmError> {
    load_json_document(path, "mediapm state")
}

/// Serializes and writes a `state.ncl` document.
pub fn save_mediapm_state_document(
    path: &Path,
    state: &crate::config::MediaPmState,
) -> Result<(), MediaPmError> {
    save_json_document(path, state, "mediapm state")
}

/// Merges runtime state into a [`MediaPmDocument`].
///
/// Currently a no-op passthrough since the state document contains
/// `ManagedWorkflowStepState` entries that are structurally incompatible with
/// the document's `MediaSourceSpec` entries.
pub fn merge_mediapm_document_with_state(
    doc: &crate::config::MediaPmDocument,
    _state: &crate::config::MediaPmState,
) -> Result<crate::config::MediaPmDocument, MediaPmError> {
    Ok(doc.clone())
}
