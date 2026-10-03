//! Versioned persistence for Nickel configuration envelopes.
//!
//! ## Policy
//!
//! - Historical schema migration uses Nickel files (`v<N>.ncl`), not Rust.
//! - This module bridges between persisted wire formats and runtime config
//!   types. Do not import unversioned config types directly from version modules.
//! - The latest bridge is in `v_latest.rs`.
//!
//! ## DO NOT REMOVE: versions policy guard
//!
//! - Version dispatch is Nickel: `mod.ncl` owns `current_version`,
//!   `supported_versions`, and `migrate_to`, and each `vN.ncl` owns the
//!   migration into itself. Do not re-introduce a parallel Rust dispatcher
//!   over the same versions.
//! - `v_latest.rs` owns the active `*Latest` boundary family. This module is
//!   the ONLY bridge between that family and the resolved (option-free) types
//!   in `config/mod.rs`: `from_boundary` (read direction), `into_boundary`
//!   (encode direction), and `merge_document_sources` (multi-document load +
//!   merge) all live here. `config/mod.rs` names no version path at all.
//! - This module MAY import from `v_latest`, but MUST NOT re-export any
//!   versioned symbol outside `versions/`. A re-export would let an outside
//!   caller name a `*Latest` type, which is the leak these entry points exist
//!   to close.
//! - `resolve_version_contract` is the registry: a version this build cannot
//!   name a contract file for must be an error, never a fallback to the
//!   newest contract.

pub(crate) mod v_latest;

mod merge;

use std::path::{Path, PathBuf};

use crate::error::ConductorError;

use super::nickel_io::{evaluate_document_source, migrate_document_source_to_version};

/// Source of the v1 Nickel contract (needed for backward migration validation).
pub(crate) const V1_NCL_SOURCE: &str = include_str!("v1.ncl");
/// Source of the v2 Nickel contract.
pub(crate) const V2_NCL_SOURCE: &str = include_str!("v2.ncl");

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
// Boundary entry points
//
// These are the ONLY way to cross between the `*Latest` wire family and the
// resolved runtime types. None of them names a `*Latest` type in its
// signature, so a caller outside `versions/` cannot construct one to pass in.
// ---------------------------------------------------------------------------

/// Resolves a raw Nickel document source into the runtime config document.
///
/// This is the read-direction boundary entry point: it parses (evaluating the
/// source through the versioned migration pipeline) and then resolves boundary
/// defaults in one step, so the caller never handles a `*Latest` value. The
/// `content_map` subset `external_data` invariant is enforced before the
/// resolved document is returned.
///
/// # Errors
///
/// Returns [`ConductorError::Serialization`] when the source is not valid
/// UTF-8 or Nickel evaluation/migration fails, and
/// [`ConductorError::Workflow`] when the resolved document violates the
/// `content_map` subset `external_data` invariant.
pub(crate) fn from_boundary(source: &str) -> Result<crate::config::NickelDocument, ConductorError> {
    let envelope: v_latest::NickelEnvelopeLatest =
        evaluate_document_source(source, "configuration document")?;
    let doc: crate::config::NickelDocument = envelope.into();
    doc.validate_external_data_invariant()?;
    Ok(doc)
}

/// Converts a runtime config document into the latest-schema wire envelope.
///
/// This is the encode-direction boundary entry point: the `From` bridges in
/// `v_latest` apply boundary defaults, so the conversion is driven from inside
/// `versions/` and the caller never names the `*Latest` type it receives.
///
/// The caller is responsible for validating the `content_map` subset
/// `external_data` invariant first (see [`encode_document`]).
pub(crate) fn into_boundary(
    document: crate::config::NickelDocument,
) -> v_latest::NickelEnvelopeLatest {
    document.into()
}

/// Returns the schema version marker of the active boundary family.
///
/// Replaces direct use of the `NICKEL_VERSION_LATEST` constant outside
/// `versions/`, so the marker is read through the version module rather than
/// imported from it.
pub(crate) fn latest_version() -> u32 {
    v_latest::NICKEL_VERSION_LATEST
}

/// Loads, merges, and resolves every configuration document at `paths`.
///
/// This is the multi-document entry point. Merging is presence-preserving
/// (explicit beats implicit) and therefore operates on the raw wire envelope,
/// so it lives in [`merge`]. Routing load and merge through a single call
/// keeps [`merge::SourceDocument`] — and the `*Latest` type it wraps —
/// unnameable from outside `versions/`.
///
/// # Errors
///
/// Returns [`ConductorError::Io`] when a document cannot be read,
/// [`ConductorError::Serialization`] when a document is not valid UTF-8 or
/// fails Nickel evaluation/migration, and [`ConductorError::Workflow`] when
/// the merged documents conflict or violate the `content_map` subset
/// `external_data` invariant.
pub(crate) fn merge_document_sources(
    paths: &[PathBuf],
) -> Result<crate::config::NickelDocument, ConductorError> {
    let sources = paths
        .iter()
        .map(|path| merge::load_source(path.as_path()))
        .collect::<Result<Vec<_>, _>>()?;
    merge::merge_documents(&sources)
}

/// Restores human-readable fields from the document previously written at
/// `previous_path` onto `outgoing`.
///
/// `external_data` descriptions and workflow `display_name`/`description` are
/// not derivable from the resolved document, so re-saving a rebuilt document
/// would otherwise drop them. Only fields that are currently `None` on
/// `outgoing` are filled: an explicit outgoing value always wins.
///
/// # Errors
///
/// Returns [`ConductorError::Io`] when the previous document cannot be read,
/// and [`ConductorError::Serialization`] when it is not valid UTF-8 or fails
/// Nickel evaluation/migration. Callers that treat an absent or unreadable
/// previous document as "nothing to preserve" should discard this error.
pub(crate) fn restore_readable_fields(
    outgoing: &mut crate::config::NickelDocument,
    previous_path: &Path,
) -> Result<(), ConductorError> {
    let previous = merge::load_source(previous_path)?;
    merge::preserve_readable_fields(outgoing, &previous.envelope);
    Ok(())
}

// ---------------------------------------------------------------------------
// Document encoding / decoding (inlined from the removed iso.rs)
// ---------------------------------------------------------------------------

/// Encodes one configuration document to Nickel source bytes.
///
/// The document is first validated (`content_map` ⊆ `external_data` invariant),
/// converted to a latest-schema envelope, then rendered as Nickel source.
///
/// # Errors
///
/// Returns [`ConductorError`] when the document cannot be converted to the
/// latest schema, validation fails, or Nickel rendering fails.
pub fn encode_document(document: crate::config::NickelDocument) -> Result<Vec<u8>, ConductorError> {
    // Validate the content_map ⊆ external_data invariant before encoding.
    // This catches missing external_data entries at encode time (first
    // save), preventing silent production of an invalid document that would
    // fail on the next decode.
    document.validate_external_data_invariant()?;
    let envelope = into_boundary(document);
    let bytes =
        mediapm_utils::nickel::render_document_as_nickel(&envelope, "configuration document")
            .map_err(ConductorError::Serialization)?;
    // Stamp every machine-written document with the shared generated-file
    // banner. All save paths (CLI export, standalone machine-doc save,
    // mediapm reconcile/synthesis) route through this function, so the
    // banner text is identical everywhere. `#` comment lines are ignored by
    // the Nickel evaluator, so decode is unaffected and `encode → decode →
    // encode` is byte-stable (see `encode_document_banner_round_trip_byte_stable`).
    Ok(mediapm_utils::generated::prepend_banner(&bytes))
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
    from_boundary(source)
}

/// Decodes bytes through the embedded Nickel migration wrapper into the raw
/// latest-schema wire envelope, WITHOUT applying boundary defaults.
///
/// This preserves field presence (which fields were explicitly written in the
/// source document) for callers that must distinguish explicit from implicit
/// values, notably multi-document merging.  Presence-preserving fields:
/// `external_data` descriptions, workflow `display_name`/`description`, and
/// `external_data.save` (absent = implicit `Saved`).
///
/// # Errors
///
/// Returns [`ConductorError`] when the bytes are not valid UTF-8 or Nickel
/// evaluation/migration fails.
pub(crate) fn decode_document_envelope(
    bytes: &[u8],
) -> Result<v_latest::NickelEnvelopeLatest, ConductorError> {
    let source = std::str::from_utf8(bytes).map_err(|err| {
        ConductorError::Serialization(format!("document source is not valid UTF-8: {err}"))
    })?;
    evaluate_document_source(source, "configuration document")
}

/// Evaluates one Nickel document source to schema v1 through the embedded
/// migration pipeline and applies `validate_document_v1` to the result.
///
/// This is the version-locked validation path for the legacy v1 surface. The
/// runtime decode pipeline never applies v1 contracts directly (v1 documents
/// are always migrated forward to the latest schema), so this helper exists
/// for tests that must prove the v1 contract itself: strictness rejections
/// (S-B1..S-B5), R2 known-good round-trips, and R3 v2→v1 migration output.
///
/// `migrate_to 1` is the identity on v1 inputs, so this helper doubles as a
/// direct v1 validator for already-v1 documents.
///
/// # Errors
///
/// Returns [`ConductorError`] when the source fails to evaluate or when
/// migration output violates the v1 envelope contract.
pub fn validate_v1_document(source: &str) -> Result<serde_json::Value, ConductorError> {
    migrate_document_source_to_version(source, 1, "configuration document")
}
