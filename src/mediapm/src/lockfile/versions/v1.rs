//! Version-1 persisted envelope for `.mediapm/lock.jsonc`.
//!
//! ## DO NOT REMOVE: versions policy guard
//!
//! - This file must not import unversioned runtime structs from outside
//!   `lockfile/versions/`.
//! - A `vX` module may reference only the immediately previous version and only
//!   for migration/isomorphism.
//! - Latest-version bridging to runtime structs is owned by
//!   `lockfile/versions/mod.rs`.

use std::collections::BTreeMap;

use fp_library::brands::RcBrand;
use fp_library::types::optics::IsoPrime;
use serde::{Deserialize, Serialize};
use serde_json::Value;

/// Version marker for the V1 lockfile envelope.
pub(crate) const LOCKFILE_VERSION_V1: u32 = 1;

/// Returns whether `marker` matches the V1 lockfile schema marker.
#[must_use]
pub(crate) const fn is_lockfile_version_v1(marker: u32) -> bool {
    marker == LOCKFILE_VERSION_V1
}

/// Version-local state for V1 lockfile payload fields.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize, Default)]
pub(crate) struct LockfileStateV1 {
    /// All top-level lockfile fields except `version`.
    #[serde(flatten)]
    pub(crate) payload: BTreeMap<String, Value>,
}

/// Top-level V1 lockfile persisted envelope.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize, Default)]
pub(crate) struct LockfileEnvelopeV1 {
    /// Explicit schema marker.
    pub(crate) version: u32,
    /// Persisted payload fields.
    #[serde(flatten)]
    pub(crate) payload: BTreeMap<String, Value>,
}

/// Isomorphism between V1 lockfile envelope and V1 local state.
pub(crate) fn lockfile_v1_iso() -> IsoPrime<'static, RcBrand, LockfileEnvelopeV1, LockfileStateV1> {
    IsoPrime::new(
        |envelope: LockfileEnvelopeV1| LockfileStateV1 { payload: envelope.payload },
        |state: LockfileStateV1| LockfileEnvelopeV1 {
            version: LOCKFILE_VERSION_V1,
            payload: state.payload,
        },
    )
}
