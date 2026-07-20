//! Version dispatch for persisted state JSON.
//!
//! Version-specific wire formats live in [`v1`] and [`v2`] submodules.
//! The public serialization API is in [`super::ser`], which delegates to
//! version dispatch functions here.

pub mod v1;
pub mod v2;

use serde_json::Value;

use crate::error::MediaPmError;

/// Extracts the numeric `version` field from a state JSON value.
///
/// Returns `MediaPmError::Workflow` when the field is missing or not
/// representable as `u64`.
pub(super) fn extract_state_version_field(value: &Value) -> Result<u64, MediaPmError> {
    value.get("version").and_then(|v| v.as_u64()).ok_or_else(|| {
        MediaPmError::Workflow("missing or invalid 'version' field in state JSON".to_string())
    })
}
