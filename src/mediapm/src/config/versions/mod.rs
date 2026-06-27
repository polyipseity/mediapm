//! Versioned document migration and envelope dispatch.
//!
//! This module manages the version marker dispatch for serialized
//! `mediapm.ncl` and `state.ncl` documents.  The `Migrate` trait defines the
//! decode/encode contract that each supported schema version must implement.

#![allow(dead_code)]
// TODO: Stream A stubs — wired when provisioning pipeline is complete.

use serde_json::Value;

use crate::error::MediaPmError;

mod v1;

use super::MediaPmDocument;
use super::MediaPmState;

// ---------------------------------------------------------------------------
// Migrate trait
// ---------------------------------------------------------------------------

/// Version-aware migration contract for config document types.
///
/// Types that implement `Migrate` for a particular schema version can
/// decode from older-wire JSON and encode back to the same wire format.
pub trait Migrate: Sized {
    /// The numeric schema version this implementation handles.
    fn version() -> u32;

    /// Decodes one JSON value into the runtime config model for this version.
    fn decode(value: Value) -> Result<Self, MediaPmError>;

    /// Encodes the runtime config model back into the JSON wire format for
    /// this version.
    fn encode(&self) -> Result<Value, MediaPmError>;
}

// ---------------------------------------------------------------------------
// Version dispatch
// ---------------------------------------------------------------------------

/// Decodes one mediapm document JSON value into the runtime model by
/// inspecting the top-level `version` marker.
pub fn decode_mediapm_document_value(value: Value) -> Result<MediaPmDocument, MediaPmError> {
    let version = extract_version_field(&value)?;

    match version {
        1 => MediaPmDocument::decode(value),
        // Latest version is always rust-backed; versions beyond that are
        // unsupported.
        _ => Err(MediaPmError::Workflow(format!(
            "unsupported mediapm document schema version {version}",
        ))),
    }
}

/// Encodes one mediapm document to its latest stable wire format.
pub fn encode_mediapm_document_value(doc: &MediaPmDocument) -> Result<Value, MediaPmError> {
    doc.encode()
}

/// Decodes one mediapm state JSON value into the runtime state model.
pub fn decode_mediapm_state_value(value: Value) -> Result<MediaPmState, MediaPmError> {
    let version = extract_version_field(&value)?;

    match version {
        1 => MediaPmState::decode(value),
        _ => Err(MediaPmError::Workflow(format!(
            "unsupported mediapm state schema version {version}",
        ))),
    }
}

/// Encodes one mediapm state to its latest stable wire format.
pub fn encode_mediapm_state_value(state: &MediaPmState) -> Result<Value, MediaPmError> {
    state.encode()
}

// ---------------------------------------------------------------------------
// Version field extraction
// ---------------------------------------------------------------------------

/// Extracts the numeric `version` field from one JSON value.
///
/// Returns `MediaPmError::Workflow` when the version field is missing or
/// not representable as `u64`.
pub fn extract_version_field(value: &Value) -> Result<u64, MediaPmError> {
    let version_value = value
        .get("version")
        .ok_or_else(|| MediaPmError::Workflow("missing 'version' field in document".to_string()))?;

    super::nickel_io::normalize_version_field_to_u64(version_value).ok_or_else(|| {
        MediaPmError::Workflow(format!(
            "'version' field value '{version_value}' is not a non-negative integer",
        ))
    })
}
