//! Public serialization API for [`MediaPmState`].
//!
//! Thin delegation layer over [`super::versions`]. Version dispatch, the
//! wire-format structs, and the V1/V2 read paths all live behind the
//! `versions` module boundary; this layer deliberately names no `vX` path so
//! that the supported version set has exactly one home.

use serde_json::Value;

use crate::config::MediaPmState;
use crate::error::MediaPmError;

use super::versions;

/// Decodes one [`Value`] (from JSON deserialization) into a
/// [`MediaPmState`], handling version dispatch.
///
/// # Errors
///
/// Returns [`MediaPmError::Workflow`] if the version is unsupported, or
/// [`MediaPmError::Serialization`] if deserialization fails.
pub fn from_json_value(value: Value) -> Result<MediaPmState, MediaPmError> {
    versions::from_json_value(value)
}

/// Encodes one [`MediaPmState`] into a [`Value`] in the current wire format.
///
/// # Errors
///
/// Returns [`MediaPmError::Serialization`] if serialization fails.
pub fn to_json_value(state: &MediaPmState) -> Result<Value, MediaPmError> {
    versions::to_json_value(state)
}

/// Migrates one legacy Nickel-format [`Value`] into a [`MediaPmState`].
///
/// # Errors
///
/// Returns [`MediaPmError::Workflow`] if the format is unrecognized, or
/// [`MediaPmError::Serialization`] if deserialization fails.
pub fn migrate_from_old_nickel(value: Value) -> Result<MediaPmState, MediaPmError> {
    versions::migrate_from_old_nickel(value)
}
