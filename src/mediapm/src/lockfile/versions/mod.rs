//! Versioned persistence envelopes for `.mediapm/lock.jsonc`.
//!
//! Runtime source-of-truth structs live in `lockfile.rs`. Versioned envelopes
//! and schema dispatch live under `lockfile/versions/`.
//!
//! ## DO NOT REMOVE: versions policy guard
//!
//! - `vX.rs` files must never import unversioned runtime structs outside
//!   `lockfile/versions/`.
//! - A `vX` file may reference only adjacent versions for migration.
//! - This `mod.rs` is the only bridge between latest version state and
//!   unversioned runtime `MediaLockFile`.
//! - Files outside `lockfile/versions/` must use only APIs in this module and
//!   never import `versions::vX` directly.
//! - Do not directly re-export `versions::vX` structs/types from this module.

use std::collections::BTreeMap;

use fp_library::brands::RcBrand;
use fp_library::types::optics::IsoPrime;
use serde_json::Value;

use crate::error::MediaPmError;
use crate::lockfile::MediaLockFile;

pub(crate) mod v1;

/// Latest-version bindings.
///
/// Keep explicit latest-version references centralized for safe schema bumps.
// BEGIN latest-version bindings
mod latest {
    use fp_library::brands::RcBrand;
    use fp_library::types::optics::IsoPrime;

    use super::v1;

    /// Latest lockfile schema marker.
    pub(super) const VERSION: u32 = v1::LOCKFILE_VERSION_V1;
    /// Latest envelope type.
    pub(super) type Envelope = v1::LockfileEnvelopeV1;
    /// Latest version-local state type.
    pub(super) type State = v1::LockfileStateV1;

    /// Returns whether `marker` matches latest lockfile marker.
    pub(super) const fn is_version(marker: u32) -> bool {
        v1::is_lockfile_version_v1(marker)
    }

    /// Latest envelope/state isomorphism.
    pub(super) fn version_iso() -> IsoPrime<'static, RcBrand, Envelope, State> {
        v1::lockfile_v1_iso()
    }
}
// END latest-version bindings

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum LockfileLayoutVersion {
    V1,
}

/// Optic-based migration trait between persistence envelope versions.
pub(crate) trait Migrate<To> {
    /// Migrates `self` to `To` via optic composition.
    fn migrate(self) -> To;
}

/// Shared migration helper using `from_iso.view` then `to_iso.review`.
pub(crate) fn migrate_with_version_state<'a, From, To, State>(
    from: From,
    from_iso: &IsoPrime<'a, RcBrand, From, State>,
    to_iso: &IsoPrime<'a, RcBrand, To, State>,
) -> To {
    let state = from_iso.from(from);
    to_iso.to(state)
}

impl Migrate<latest::Envelope> for latest::Envelope {
    fn migrate(self) -> latest::Envelope {
        migrate_with_version_state(self, &latest::version_iso(), &latest::version_iso())
    }
}

/// Decodes one lockfile envelope using one already-dispatched version marker.
fn decode_envelope_for_version(
    bytes: &[u8],
    version: u32,
) -> Result<latest::Envelope, MediaPmError> {
    match dispatch_lockfile_version(version)? {
        LockfileLayoutVersion::V1 => serde_json::from_slice(bytes).map_err(|err| {
            MediaPmError::Serialization(format!(
                "decoding .mediapm/lock.jsonc v{} envelope failed: {err}",
                latest::VERSION
            ))
        }),
    }
}

/// Migrates one parsed lockfile envelope from `from_version` to
/// `target_version`.
///
/// This function is the central migration gateway used by decode flows.
fn migrate_envelope_to_version(
    envelope: latest::Envelope,
    from_version: u32,
    target_version: u32,
) -> Result<latest::Envelope, MediaPmError> {
    let from_layout = dispatch_lockfile_version(from_version)?;
    let to_layout = dispatch_lockfile_version(target_version)?;

    match (from_layout, to_layout) {
        (LockfileLayoutVersion::V1, LockfileLayoutVersion::V1) => Ok(envelope.migrate()),
    }
}

/// Returns latest lockfile schema marker.
#[must_use]
pub(crate) const fn latest_lockfile_version() -> u32 {
    latest::VERSION
}

/// ## DO NOT REMOVE: latest-first version dispatch guard
///
/// Version checks should always start from latest.
fn dispatch_lockfile_version(marker: u32) -> Result<LockfileLayoutVersion, MediaPmError> {
    if latest::is_version(marker) {
        Ok(LockfileLayoutVersion::V1)
    } else {
        Err(MediaPmError::Workflow(format!(
            "unsupported .mediapm/lock.jsonc version {marker}; expected {}",
            latest_lockfile_version()
        )))
    }
}

/// Extracts numeric top-level `version` marker from lockfile bytes.
fn decode_version_marker(bytes: &[u8]) -> Result<u32, MediaPmError> {
    let value: Value = serde_json::from_slice(bytes).map_err(|err| {
        MediaPmError::Serialization(format!("decoding .mediapm/lock.jsonc: {err}"))
    })?;

    let marker = value.get("version").and_then(Value::as_u64).ok_or_else(|| {
        MediaPmError::Workflow(
            ".mediapm/lock.jsonc must define numeric top-level 'version'".to_string(),
        )
    })?;

    u32::try_from(marker).map_err(|_| {
        MediaPmError::Workflow(format!(
            ".mediapm/lock.jsonc version {marker} exceeds supported u32 range"
        ))
    })
}

/// Bridges latest version-local state to runtime `MediaLockFile`.
fn runtime_iso() -> IsoPrime<'static, RcBrand, latest::State, MediaLockFile> {
    IsoPrime::new(
        |versioned: latest::State| {
            let mut object = serde_json::Map::new();
            object.insert("version".to_string(), Value::from(latest::VERSION));
            for (key, value) in versioned.payload {
                object.insert(key, value);
            }

            serde_json::from_value(Value::Object(object)).expect(
                "latest lockfile version-local state must always deserialize into MediaLockFile",
            )
        },
        |runtime: MediaLockFile| {
            let value = serde_json::to_value(runtime)
                .expect("MediaLockFile must always serialize into JSON value");
            let mut object = value
                .as_object()
                .cloned()
                .expect("MediaLockFile JSON value must be a top-level object");
            let _ = object.remove("version");

            latest::State { payload: object.into_iter().collect::<BTreeMap<String, Value>>() }
        },
    )
}

/// Decodes runtime lockfile from versioned persistence bytes.
pub(crate) fn decode_lockfile_bytes(bytes: &[u8]) -> Result<MediaLockFile, MediaPmError> {
    let marker = decode_version_marker(bytes)?;

    let envelope = decode_envelope_for_version(bytes, marker)?;
    let migrated = migrate_envelope_to_version(envelope, marker, latest_lockfile_version())?;

    let state = latest::version_iso().from(migrated);
    Ok(runtime_iso().from(state))
}

/// Encodes runtime lockfile with latest versioned persistence envelope.
pub(crate) fn encode_lockfile_bytes(lockfile: MediaLockFile) -> Result<Vec<u8>, MediaPmError> {
    let _ = dispatch_lockfile_version(lockfile.version)?;

    let latest_state = runtime_iso().to(lockfile);
    let envelope = latest::version_iso().to(latest_state);
    serde_json::to_vec_pretty(&envelope)
        .map_err(|err| MediaPmError::Serialization(format!("encoding lockfile JSON: {err}")))
}

#[cfg(test)]
mod tests {
    use crate::lockfile::MediaLockFile;

    use super::{decode_lockfile_bytes, encode_lockfile_bytes, latest_lockfile_version};

    /// Verifies decode rejects unsupported lockfile schema markers.
    #[test]
    fn decode_rejects_unsupported_lockfile_version() {
        let bytes = format!("{{\"version\": {}}}", latest_lockfile_version() + 1);
        let err =
            decode_lockfile_bytes(bytes.as_bytes()).expect_err("unsupported version should fail");
        assert!(err.to_string().contains("unsupported .mediapm/lock.jsonc version"));
    }

    /// Verifies latest encode/decode round-trip preserves explicit version marker.
    #[test]
    fn encode_round_trip_preserves_latest_lockfile_version() {
        let encoded = encode_lockfile_bytes(MediaLockFile::default()).expect("encode lockfile");
        let decoded = decode_lockfile_bytes(&encoded).expect("decode lockfile");
        assert_eq!(decoded.version, latest_lockfile_version());
    }
}
