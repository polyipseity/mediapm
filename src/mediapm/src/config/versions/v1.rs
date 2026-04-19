//! Version-1 persisted envelope for `mediapm.ncl`.
//!
//! ## DO NOT REMOVE: versions policy guard
//!
//! - This file must not import unversioned runtime structs from outside
//!   `config/versions/`.
//! - A `vX` module may reference only the immediately previous version and only
//!   for migration/isomorphism.
//! - Latest-version bridging to runtime structs is owned by
//!   `config/versions/mod.rs`.

use std::collections::BTreeMap;

use fp_library::brands::RcBrand;
use fp_library::types::optics::IsoPrime;
use serde::{Deserialize, Serialize};
use serde_json::Value;

/// Version marker for the V1 `mediapm.ncl` envelope.
pub(crate) const MEDIAPM_NICKEL_VERSION_V1: u32 = 1;

/// Returns whether `marker` matches the V1 schema marker.
#[must_use]
pub(crate) const fn is_mediapm_nickel_version_v1(marker: u32) -> bool {
    marker == MEDIAPM_NICKEL_VERSION_V1
}

/// Version-local state for V1 persisted payload fields.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize, Default)]
pub(crate) struct MediaPmDocumentStateV1 {
    /// All top-level `mediapm.ncl` fields except `version`.
    #[serde(flatten)]
    pub(crate) payload: BTreeMap<String, Value>,
}

/// Top-level V1 persisted document envelope.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize, Default)]
pub(crate) struct MediaPmDocumentEnvelopeV1 {
    /// Explicit schema marker.
    pub(crate) version: u32,
    /// Persisted payload fields.
    #[serde(flatten)]
    pub(crate) payload: BTreeMap<String, Value>,
}

/// Isomorphism between V1 envelope and V1 local state.
pub(crate) fn mediapm_document_v1_iso()
-> IsoPrime<'static, RcBrand, MediaPmDocumentEnvelopeV1, MediaPmDocumentStateV1> {
    IsoPrime::new(
        |envelope: MediaPmDocumentEnvelopeV1| MediaPmDocumentStateV1 { payload: envelope.payload },
        |state: MediaPmDocumentStateV1| MediaPmDocumentEnvelopeV1 {
            version: MEDIAPM_NICKEL_VERSION_V1,
            payload: state.payload,
        },
    )
}
