//! JSON persistence for orchestration state.
//!
//! ## Versioning
//!
//! - The state schema has a single wire format (version 2).
//! - The runtime `OrchestrationState` is the wire format; no separate
//!   envelope types are needed. This keeps the version boundary minimal.
//! - Future versions should add a version-check entry in `decode_state_json`
//!   and (if the runtime types diverge from the wire format) a `v<N>.rs`
//!   module with a `From` bridge.

use serde_json;

use crate::error::ConductorError;
use crate::state::OrchestrationState;

/// Current orchestration state wire version.
const CURRENT_STATE_VERSION: u32 = 2;

/// Decodes orchestration state from JSON bytes.
///
/// Checks the `version` field and rejects unsupported versions.
///
/// # Errors
///
/// Returns [`ConductorError::Serialization`] if the JSON is invalid, the
/// version marker is missing/non-numeric, or the version is unsupported.
pub fn decode_state_json(bytes: &[u8]) -> Result<OrchestrationState, ConductorError> {
    let value: serde_json::Value =
        serde_json::from_slice(bytes).map_err(|e| ConductorError::Serialization(e.to_string()))?;
    let version = value.get("version").and_then(serde_json::Value::as_u64).ok_or_else(|| {
        ConductorError::Serialization(
            "missing or non-numeric 'version' field in state JSON".to_string(),
        )
    })?;
    if version != u64::from(CURRENT_STATE_VERSION) {
        return Err(ConductorError::Serialization(format!(
            "unsupported orchestration state version: {version} (expected {CURRENT_STATE_VERSION})"
        )));
    }
    serde_json::from_value(value).map_err(|e| ConductorError::Serialization(e.to_string()))
}

/// Encodes orchestration state as pretty JSON with the current version
/// marker.
///
/// # Errors
///
/// Returns [`ConductorError::Serialization`] if serialization fails.
pub fn encode_state_json(state: &OrchestrationState) -> Result<Vec<u8>, ConductorError> {
    let mut value =
        serde_json::to_value(state).map_err(|e| ConductorError::Serialization(e.to_string()))?;
    // Ensure the version marker is always current (defensive against stale
    // runtime version fields).
    if let Some(obj) = value.as_object_mut() {
        obj.insert(
            "version".to_string(),
            serde_json::Value::Number(serde_json::Number::from(CURRENT_STATE_VERSION)),
        );
    }
    serde_json::to_vec_pretty(&value).map_err(|e| ConductorError::Serialization(e.to_string()))
}
