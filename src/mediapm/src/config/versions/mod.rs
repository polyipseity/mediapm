//! Versioned document migration and envelope dispatch.
//!
//! This module manages the version marker dispatch for serialized
//! `mediapm.ncl` documents.  The `Migrate` trait defines the decode/encode
//! contract that each supported schema version must implement.

#![allow(dead_code)]
// TODO: Stream A stubs — wired when provisioning pipeline is complete.

use serde_json::Value;

use crate::error::MediaPmError;

mod v1;

use super::MediaPmDocument;

// ---------------------------------------------------------------------------
// Migrate trait
// ---------------------------------------------------------------------------

/// Version-aware migration contract for config document types.
///
/// Types that implement `Migrate` for a particular schema version can
/// decode from older-wire JSON and encode back to the same wire format.
pub trait Migrate: Sized {
    /// The numeric schema version this implementation handles.
    fn version() -> u32;

    /// Decodes one JSON value into the runtime config model for this version.
    fn decode(value: Value) -> Result<Self, MediaPmError>;

    /// Encodes the runtime config model back into the JSON wire format for
    /// this version.
    fn encode(&self) -> Result<Value, MediaPmError>;
}

// ---------------------------------------------------------------------------
// Version dispatch
// ---------------------------------------------------------------------------

/// Decodes one mediapm document JSON value into the runtime model by
/// inspecting the top-level `version` marker.
pub fn decode_mediapm_document_value(value: Value) -> Result<MediaPmDocument, MediaPmError> {
    let version = extract_version_field(&value)?;

    match version {
        1 => MediaPmDocument::decode(value),
        // Latest version is always rust-backed; versions beyond that are
        // unsupported.
        _ => Err(MediaPmError::Workflow(format!(
            "unsupported mediapm document schema version {version}",
        ))),
    }
}

/// Encodes one mediapm document to its latest stable wire format.
pub fn encode_mediapm_document_value(doc: &MediaPmDocument) -> Result<Value, MediaPmError> {
    doc.encode()
}

// ---------------------------------------------------------------------------
// Version field extraction
// ---------------------------------------------------------------------------

/// Extracts the numeric `version` field from one JSON value.
///
/// Returns `MediaPmError::Workflow` when the version field is missing or
/// not representable as `u64`.
pub fn extract_version_field(value: &Value) -> Result<u64, MediaPmError> {
    let version_value = value.get("version").ok_or_else(|| MediaPmError::ConfigValidation {
        code: "MPM-E004",
        context: "document version extraction".to_string(),
        detail: "missing 'version' field in document".to_string(),
        suggestion: "ensure the document has a top-level 'version' field".to_string(),
    })?;

    super::nickel_io::normalize_version_field_to_u64(version_value).ok_or_else(|| {
        MediaPmError::ConfigValidation {
            code: "MPM-E004",
            context: "document version extraction".to_string(),
            detail: format!(
                "'version' field value '{version_value}' is not a non-negative integer",
            ),
            suggestion: "use a non-negative integer for the 'version' field".to_string(),
        }
    })
}

// ---------------------------------------------------------------------------
// Registry surface (mirrors `mod.ncl`)
// ---------------------------------------------------------------------------

/// Numeric schema versions supported by the mediapm registry (mirrors
/// `mod.ncl`'s `SupportedVersion` / `supported_versions`).
pub const SUPPORTED_VERSIONS: &[u32] = &[1];

/// The current (latest) supported schema version (mirrors `mod.ncl`'s
/// `current_version`).
pub const CURRENT_VERSION: u32 = 1;

/// Predicate mirroring `mod.ncl`'s `SupportedVersion`.
#[must_use]
pub fn is_supported_version(version: u32) -> bool {
    SUPPORTED_VERSIONS.contains(&version)
}

/// Identity migration stub mirroring `mod.ncl`'s `migrate_to`.
///
/// mediapm currently supports a single document version, so migration is the
/// identity when the requested version matches the document's version and any
/// other combination is an error.
pub fn migrate_to(requested_version: u32, document: Value) -> Result<Value, MediaPmError> {
    let version = extract_version_field(&document)?;
    if u64::from(requested_version) == version && is_supported_version(requested_version) {
        return Ok(document);
    }
    Err(MediaPmError::Workflow(format!(
        "no migration edge between mediapm document versions {version} and {requested_version}",
    )))
}

// ---------------------------------------------------------------------------
// Nickel schema validation helpers (used by integration tests)
// ---------------------------------------------------------------------------

/// Evaluates `document | v1.<contract_name>` for one document source against
/// the embedded `v1.ncl` module, returning the validated JSON value.
///
/// Contract failures surface as `MediaPmError::Workflow`.
///
/// # Errors
///
/// Returns [`MediaPmError`] when the temp workspace cannot be written or the
/// wrapper cannot be evaluated.
pub fn apply_v1_contract(contract_name: &str, source: &str) -> Result<Value, MediaPmError> {
    const V1_NCL_SOURCE: &str = include_str!("v1.ncl");
    const MOD_NCL_SOURCE: &str = include_str!("mod.ncl");

    let dir = tempfile::tempdir().map_err(|err| MediaPmError::Io {
        operation: "create mediapm schema validation temp dir".to_string(),
        path: std::env::temp_dir(),
        source: err,
    })?;
    let v1_path = dir.path().join("v1.ncl");
    let mod_path = dir.path().join("mod.ncl");
    let input_path = dir.path().join("document_input.ncl");
    let wrapper_path = dir.path().join("apply_contract.ncl");
    std::fs::write(&v1_path, V1_NCL_SOURCE).map_err(|err| MediaPmError::Io {
        operation: "write embedded v1.ncl".to_string(),
        path: v1_path.clone(),
        source: err,
    })?;
    // The current `v1.ncl` imports `mod.ncl` for `shared.VersionContract`; the
    // strict rewrite keeps both files importable, so write both unconditionally.
    std::fs::write(&mod_path, MOD_NCL_SOURCE).map_err(|err| MediaPmError::Io {
        operation: "write embedded mod.ncl".to_string(),
        path: mod_path.clone(),
        source: err,
    })?;
    std::fs::write(&input_path, source).map_err(|err| MediaPmError::Io {
        operation: "write document input".to_string(),
        path: input_path.clone(),
        source: err,
    })?;
    // `validate_document_v1` is a plain function (not a contract): applying
    // it with `document | v1.validate_document_v1` hits the deprecated
    // function-as-contract path, so it is invoked as a function.  Record
    // contracts like `MediaPmStateV1` are applied with `|` as usual.
    let application = if contract_name == "validate_document_v1" {
        "v1.validate_document_v1 document".to_string()
    } else {
        format!("document | v1.{contract_name}")
    };
    std::fs::write(
        &wrapper_path,
        format!(
            "let v1 = import \"v1.ncl\" in\n\
             let document = import \"document_input.ncl\" in\n\
             {application}\n",
        ),
    )
    .map_err(|err| MediaPmError::Io {
        operation: "write contract wrapper".to_string(),
        path: wrapper_path.clone(),
        source: err,
    })?;
    super::nickel_io::evaluate_nickel_source_to_json(&wrapper_path)
}

/// Validates one V1 mediapm document source against the embedded `v1.ncl`
/// `MediaPmDocumentV1` contract.
///
/// # Errors
///
/// Returns [`MediaPmError`] when the document source fails contract
/// validation or cannot be evaluated.
pub fn validate_v1_document(source: &str) -> Result<Value, MediaPmError> {
    apply_v1_contract("validate_document_v1", source)
}

/// Evaluates one expression in the scope of the embedded `mod.ncl` registry
/// module (importing both `mod.ncl` and `v1.ncl`).
///
/// # Errors
///
/// Returns [`MediaPmError`] when the temp workspace cannot be written or the
/// wrapper cannot be evaluated.
pub fn evaluate_mod_ncl_expression(expr: &str) -> Result<Value, MediaPmError> {
    const MOD_NCL_SOURCE: &str = include_str!("mod.ncl");
    const V1_NCL_SOURCE: &str = include_str!("v1.ncl");

    let dir = tempfile::tempdir().map_err(|err| MediaPmError::Io {
        operation: "create mediapm registry validation temp dir".to_string(),
        path: std::env::temp_dir(),
        source: err,
    })?;
    let mod_path = dir.path().join("mod.ncl");
    let v1_path = dir.path().join("v1.ncl");
    let wrapper_path = dir.path().join("evaluate_expr.ncl");
    std::fs::write(&mod_path, MOD_NCL_SOURCE).map_err(|err| MediaPmError::Io {
        operation: "write embedded mod.ncl".to_string(),
        path: mod_path.clone(),
        source: err,
    })?;
    std::fs::write(&v1_path, V1_NCL_SOURCE).map_err(|err| MediaPmError::Io {
        operation: "write embedded v1.ncl".to_string(),
        path: v1_path.clone(),
        source: err,
    })?;
    std::fs::write(&wrapper_path, format!("let shared = import \"mod.ncl\" in\n{expr}\n"))
        .map_err(|err| MediaPmError::Io {
            operation: "write registry expression wrapper".to_string(),
            path: wrapper_path.clone(),
            source: err,
        })?;
    super::nickel_io::evaluate_nickel_source_to_json(&wrapper_path)
}
