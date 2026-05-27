//! Versioned persistence envelopes for conductor Nickel documents.
//!
//! Runtime source-of-truth configuration structs live in
//! `model/config/mod.rs`. Version modules own persisted wire/document shapes
//! and Nickel migration/validation wrappers.
//!
//! The module path is `config` to communicate intent (runtime configuration),
//! while schema contracts remain Nickel-based.
//!
//! ## DO NOT REMOVE: version file correspondence guard
//!
//! - Every supported schema version must provide exactly one `vX.ncl` file.
//! - `latest` bindings in this module must point to the highest supported `vX.ncl`.
//! - Migration/validation dispatch in this module must remain latest-first.
//! - Keep historical version structs out of this file; only `v_latest.rs` may
//!   define the Rust persisted-schema bridge.
//! - These rules are mandatory and must not be removed.

mod v_latest;

use crate::error::ConductorError;

/// Latest-version Nickel contract bindings.
///
/// Keep explicit latest pointers centralized for safe schema bumps.
// BEGIN latest-version bindings
mod latest {
    use fp_library::brands::RcBrand;
    use fp_library::types::optics::IsoPrime;

    use super::v_latest;

    /// Latest persisted Nickel schema marker.
    pub(super) const VERSION: u32 = v_latest::NICKEL_VERSION_LATEST;
    /// File name of the latest embedded Nickel contract.
    pub(super) const NCL_FILE_NAME: &str = "v1.ncl";
    /// Source of the latest embedded Nickel contract.
    pub(super) const NCL_SOURCE: &str = include_str!("v1.ncl");

    /// Rust envelope type for the latest schema bridge.
    pub(super) type Envelope = v_latest::NickelEnvelopeLatest;
    /// Rust shared-state type for the latest schema bridge.
    pub(super) type State = v_latest::NickelStateLatest;

    /// Returns whether `marker` equals the latest supported schema marker.
    #[must_use]
    pub(super) const fn is_version(marker: u32) -> bool {
        v_latest::is_nickel_version_latest(marker)
    }

    /// Isomorphism between the latest persisted document envelope and shared state.
    pub(super) fn version_iso() -> IsoPrime<'static, RcBrand, Envelope, State> {
        v_latest::nickel_latest_iso()
    }
}
// END latest-version bindings

/// Active version markers for both user and machine Nickel documents.
pub(crate) const USER_NICKEL_VERSION: u32 = latest::VERSION;
/// Active version markers for both user and machine Nickel documents.
pub(crate) const MACHINE_NICKEL_VERSION: u32 = latest::VERSION;

/// Fixed embedded migration helper module.
const MOD_NCL_SOURCE: &str = include_str!("mod.ncl");

/// Resolves one requested schema marker to the embedded Nickel contract file and source.
fn resolve_version_contract(
    requested_version: u32,
    document_kind: &str,
) -> Result<(&'static str, &'static str), ConductorError> {
    if latest::is_version(requested_version) {
        Ok((latest::NCL_FILE_NAME, latest::NCL_SOURCE))
    } else {
        Err(ConductorError::Workflow(format!(
            "unsupported {document_kind} schema version {requested_version}; expected {}",
            latest::VERSION
        )))
    }
}

mod nickel_io;

mod iso;

pub(crate) use self::iso::{
    compile_total_configuration_sources, decode_machine_document, decode_state_document,
    decode_user_document, encode_machine_document, encode_state_document, encode_user_document,
    evaluate_total_configuration_sources,
};

#[cfg(test)]
mod tests;
