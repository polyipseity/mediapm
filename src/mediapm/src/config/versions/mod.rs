//! Versioned document migration and envelope dispatch.
//!
//! This module manages the version marker dispatch for serialized
//! `mediapm.ncl` documents.  The `Migrate` trait defines the decode/encode
//! contract that each supported schema version must implement.

#![allow(dead_code)]

use serde_json::Value;

use crate::error::MediaPmError;

mod v1;
mod v2;

use super::MediaPmDocument;
use v1::MediaPmDocumentEnvelopeV1;
use v2::MediaPmDocumentEnvelopeV2;

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
        1 => MediaPmDocumentEnvelopeV1::decode(value).map(MediaPmDocument::from),
        2 => MediaPmDocumentEnvelopeV2::decode(value).map(MediaPmDocument::from),
        // Latest version is always rust-backed; versions beyond that are
        // unsupported.
        _ => Err(MediaPmError::Workflow(format!(
            "unsupported mediapm document schema version {version}",
        ))),
    }
}

/// Encodes one mediapm document to its latest stable wire format.
pub fn encode_mediapm_document_value(doc: &MediaPmDocument) -> Result<Value, MediaPmError> {
    // Encode to the latest (V2) wire format.
    MediaPmDocumentEnvelopeV2::from(doc).encode()
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
pub const SUPPORTED_VERSIONS: &[u32] = &[1, 2];

/// The current (latest) supported schema version (mirrors `mod.ncl`'s
/// `current_version`).
pub const CURRENT_VERSION: u32 = 2;

/// Predicate mirroring `mod.ncl`'s `SupportedVersion`.
#[must_use]
pub fn is_supported_version(version: u32) -> bool {
    SUPPORTED_VERSIONS.contains(&version)
}

/// Migrates one mediapm document JSON value to the requested version,
/// mirroring `mod.ncl`'s `migrate_to` dispatch.
///
/// Each version envelope owns its edges, matching the placement policy in
/// the Nickel schema files: V1→V2 strips the legacy `state` payload (via the
/// unified model's `From`), and V2→V1 bumps the version marker only (V1
/// accepts stateless documents).
pub fn migrate_to(requested_version: u32, document: Value) -> Result<Value, MediaPmError> {
    let version = extract_version_field(&document)?;
    if u64::from(requested_version) == version && is_supported_version(requested_version) {
        return Ok(document);
    }
    match (version, requested_version) {
        (1, 2) => {
            let model = MediaPmDocument::from(MediaPmDocumentEnvelopeV1::decode(document)?);
            MediaPmDocumentEnvelopeV2::from(&model).encode()
        }
        (2, 1) => {
            let model = MediaPmDocument::from(MediaPmDocumentEnvelopeV2::decode(document)?);
            MediaPmDocumentEnvelopeV1::from(&model).encode()
        }
        _ => Err(MediaPmError::Workflow(format!(
            "no migration edge between mediapm document versions {version} and {requested_version}",
        ))),
    }
}

// ---------------------------------------------------------------------------
// Nickel schema validation helpers (used by integration tests)
// ---------------------------------------------------------------------------

/// Writes one versioned schema file, its sibling version modules, and a
/// `document | v{version}.<contract_name>` wrapper into a fresh temp
/// workspace, then evaluates the wrapper against the embedded
/// `{version}.ncl` module, returning the validated JSON value.
///
/// Contract failures surface as `MediaPmError::Workflow`.
///
/// # Errors
///
/// Returns [`MediaPmError`] when the temp workspace cannot be written or the
/// wrapper cannot be evaluated.
fn apply_version_contract(
    version: &str,
    contract_name: &str,
    source: &str,
) -> Result<Value, MediaPmError> {
    const V1_NCL_SOURCE: &str = include_str!("v1.ncl");
    const V2_NCL_SOURCE: &str = include_str!("v2.ncl");
    const MOD_NCL_SOURCE: &str = include_str!("mod.ncl");

    let dir = mediapm_utils::temp::artifact_dir().map_err(|err| MediaPmError::Io {
        operation: "create mediapm schema validation temp dir".to_string(),
        path: std::env::temp_dir(),
        source: err,
    })?;
    let v1_path = dir.path().join("v1.ncl");
    let v2_path = dir.path().join("v2.ncl");
    let mod_path = dir.path().join("mod.ncl");
    let input_path = dir.path().join("document_input.ncl");
    let wrapper_path = dir.path().join("apply_contract.ncl");
    std::fs::write(&v1_path, V1_NCL_SOURCE).map_err(|err| MediaPmError::Io {
        operation: "write embedded v1.ncl".to_string(),
        path: v1_path.clone(),
        source: err,
    })?;
    // `mod.ncl` imports both version files; keep every temp workspace
    // self-contained so any wrapper can import any of the three modules.
    std::fs::write(&v2_path, V2_NCL_SOURCE).map_err(|err| MediaPmError::Io {
        operation: "write embedded v2.ncl".to_string(),
        path: v2_path.clone(),
        source: err,
    })?;
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
    // `validate_document_v{version}` is a plain function (not a contract):
    // applying it with `document | v{version}.validate_document_v{version}`
    // hits the deprecated function-as-contract path, so it is invoked as a
    // function.  Record contracts like `MediaPmStateV1` are applied with `|`
    // as usual.
    let validator = format!("validate_document_{version}");
    let application = if contract_name == validator {
        format!("{version}.{validator} document")
    } else {
        format!("document | {version}.{contract_name}")
    };
    std::fs::write(
        &wrapper_path,
        format!(
            "let {version} = import \"{version}.ncl\" in\n\
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

/// Applies one V1 schema contract to a document source.
///
/// # Errors
///
/// Returns [`MediaPmError`] when the document source fails contract
/// validation or cannot be evaluated.
pub fn apply_v1_contract(contract_name: &str, source: &str) -> Result<Value, MediaPmError> {
    apply_version_contract("v1", contract_name, source)
}

/// Applies one V2 schema contract to a document source.
///
/// # Errors
///
/// Returns [`MediaPmError`] when the document source fails contract
/// validation or cannot be evaluated.
pub fn apply_v2_contract(contract_name: &str, source: &str) -> Result<Value, MediaPmError> {
    apply_version_contract("v2", contract_name, source)
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

/// Validates one V2 mediapm document source against the embedded `v2.ncl`
/// `MediaPmDocumentV2` contract.
///
/// # Errors
///
/// Returns [`MediaPmError`] when the document source fails contract
/// validation or cannot be evaluated.
pub fn validate_v2_document(source: &str) -> Result<Value, MediaPmError> {
    apply_v2_contract("validate_document_v2", source)
}

/// Evaluates one expression in the scope of the embedded `mod.ncl` registry
/// module (importing `mod.ncl`, `v1.ncl`, and `v2.ncl`).
///
/// # Errors
///
/// Returns [`MediaPmError`] when the temp workspace cannot be written or the
/// wrapper cannot be evaluated.
pub fn evaluate_mod_ncl_expression(expr: &str) -> Result<Value, MediaPmError> {
    const MOD_NCL_SOURCE: &str = include_str!("mod.ncl");
    const V1_NCL_SOURCE: &str = include_str!("v1.ncl");
    const V2_NCL_SOURCE: &str = include_str!("v2.ncl");

    let dir = mediapm_utils::temp::artifact_dir().map_err(|err| MediaPmError::Io {
        operation: "create mediapm registry validation temp dir".to_string(),
        path: std::env::temp_dir(),
        source: err,
    })?;
    let mod_path = dir.path().join("mod.ncl");
    let v1_path = dir.path().join("v1.ncl");
    let v2_path = dir.path().join("v2.ncl");
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
    std::fs::write(&v2_path, V2_NCL_SOURCE).map_err(|err| MediaPmError::Io {
        operation: "write embedded v2.ncl".to_string(),
        path: v2_path.clone(),
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
