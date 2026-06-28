//! Versioned persistence for Nickel configuration envelopes.
//!
//! ## Policy
//!
//! - Historical schema migration uses Nickel files (`v<N>.ncl`), not Rust.
//! - This module bridges between persisted wire formats and runtime config
//!   types. Do not import unversioned config types directly from version modules.
//! - The latest bridge is in `v_latest.rs`.

pub(crate) mod v_latest;

use crate::error::ConductorError;

use super::nickel_io::evaluate_document_source;

/// Source of the v1 Nickel contract (needed for backward migration validation).
pub(crate) const V1_NCL_SOURCE: &str = include_str!("v1.ncl");
/// Source of the v2 Nickel contract.
pub(crate) const V2_NCL_SOURCE: &str = include_str!("v2.ncl");

/// Active version marker for conductor Nickel documents.
pub(crate) const NICKEL_VERSION: u32 = v_latest::NICKEL_VERSION_LATEST;

/// Fixed embedded migration helper module.
pub(crate) const MOD_NCL_SOURCE: &str = include_str!("mod.ncl");

/// Resolves one requested schema marker to the embedded Nickel contract file name.
pub(super) fn resolve_version_contract(
    requested_version: u32,
    document_kind: &str,
) -> Result<&'static str, ConductorError> {
    match requested_version {
        1 => Ok("v1.ncl"),
        2 => Ok("v2.ncl"),
        _ => Err(ConductorError::Workflow(format!(
            "unsupported {document_kind} schema version {requested_version}; expected 1 or {}",
            v_latest::NICKEL_VERSION_LATEST
        ))),
    }
}

// ---------------------------------------------------------------------------
// Document encoding / decoding (inlined from the removed iso.rs)
// ---------------------------------------------------------------------------

/// Encodes one configuration document to Nickel source bytes.
///
/// The document is first converted to a latest-schema envelope, validated,
/// then rendered as Nickel source.
///
/// # Errors
///
/// Returns [`ConductorError`] when the document cannot be converted to the
/// latest schema, validation fails, or Nickel rendering fails.
pub fn encode_document(document: crate::config::NickelDocument) -> Result<Vec<u8>, ConductorError> {
    let envelope: v_latest::NickelEnvelopeLatest = document.into();
    mediapm_utils::nickel::render_document_as_nickel(&envelope, "configuration document")
        .map_err(ConductorError::Serialization)
}

/// Decodes bytes through the embedded Nickel migration wrapper into a runtime
/// config document.
///
/// The input bytes are interpreted as UTF-8 Nickel source, evaluated through
/// the versioned migration pipeline, and deserialized into a `NickelDocument`.
///
/// # Errors
///
/// Returns [`ConductorError`] when the bytes are not valid UTF-8, Nickel
/// evaluation fails, or the document does not match the expected schema.
pub fn decode_document(bytes: &[u8]) -> Result<crate::config::NickelDocument, ConductorError> {
    let source = std::str::from_utf8(bytes).map_err(|err| {
        ConductorError::Serialization(format!("document source is not valid UTF-8: {err}"))
    })?;
    let envelope: v_latest::NickelEnvelopeLatest =
        evaluate_document_source(source, "configuration document")?;

    Ok(envelope.into())
}

/// Evaluates one Nickel source through the full migration pipeline and returns
/// the compiled `NickelDocument`.
///
/// This is the primary configuration loading entry point: it reads the version
/// marker, applies migrations, validates against the target schema, and
/// deserializes into the runtime config type.
pub(crate) fn compile_configuration_source(
    source: &str,
) -> Result<crate::config::NickelDocument, ConductorError> {
    let envelope: v_latest::NickelEnvelopeLatest =
        evaluate_document_source(source, "configuration document")?;
    let marker = envelope.version;
    if marker != v_latest::NICKEL_VERSION_LATEST {
        return Err(ConductorError::Workflow(format!(
            "expected configuration document version {} but found {marker}",
            v_latest::NICKEL_VERSION_LATEST,
        )));
    }

    Ok(envelope.into())
}

/// Evaluates one Nickel source through the full migration pipeline for
/// validation side effects, discarding the result.
pub(crate) fn evaluate_configuration_source(source: &str) -> Result<(), ConductorError> {
    let _ = compile_configuration_source(source)?;
    Ok(())
}
