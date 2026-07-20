//! Public serialization API for [`MediaPmState`].
//!
//! Thin delegation layer over [`super::versions`] submodules. Wire-format
//! structs, V1 migration logic, and V2 encode/decode all live in
//! `versions/v1.rs` and `versions/v2.rs`.

use serde_json::Value;

use crate::config::MediaPmState;
use crate::error::MediaPmError;

use super::versions;

// ---------------------------------------------------------------------------
// Public API
// ---------------------------------------------------------------------------

/// Decodes one [`Value`] (from JSON deserialization) into a
/// [`MediaPmState`], handling version dispatch.
///
/// # Errors
///
/// Returns [`MediaPmError::Workflow`] if the version is unsupported, or
/// [`MediaPmError::Serialization`] if deserialization fails.
pub fn from_json_value(value: Value) -> Result<MediaPmState, MediaPmError> {
    let version = versions::extract_state_version_field(&value)?;

    match version {
        1 => versions::v1::from_v1_json_value(value),
        2 => versions::v2::from_v2_json_value(value),
        v => Err(MediaPmError::Workflow(format!("unsupported mediapm state schema version {v}"))),
    }
}

/// Encodes one [`MediaPmState`] into a [`Value`] (V2 format).
///
/// Always produces V2 output regardless of input version.
///
/// # Errors
///
/// Returns [`MediaPmError::Serialization`] if serialization fails.
pub fn to_json_value(state: &MediaPmState) -> Result<Value, MediaPmError> {
    versions::v2::to_v2_json_value(state)
}

/// Migrates one [`Value`] from old Nickel format into a [`MediaPmState`].
///
/// Accepts both V1 wrapper and flat post-rewrite formats. Delegates to
/// `versions::v1::from_v1_json_value` which handles all V1 shapes.
///
/// # Errors
///
/// Returns [`MediaPmError::Workflow`] if the format is unrecognized, or
/// [`MediaPmError::Serialization`] if deserialization fails.
pub fn migrate_from_old_nickel(value: Value) -> Result<MediaPmState, MediaPmError> {
    versions::v1::from_v1_json_value(value)
}
