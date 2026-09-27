//! Nickel schema registry for persisted `mediapm.ncl` documents.
//!
//! ## Where the version ladder lives
//!
//! The migration ladder for `mediapm.ncl` is **Nickel**, not Rust. The
//! versioned contracts are `v1.ncl` and `v2.ncl`; `mod.ncl` is the registry
//! that owns `current_version`, `supported_versions`, and the `migrate_to`
//! dispatch. Each version file owns the migration *into* itself
//! (`v2.ncl` exports `migrate_v1_to_v2`, `v1.ncl` exports `migrate_v2_to_v1`)
//! and `mod.ncl` only dispatches.
//!
//! This module exposes the Rust-side surface of that Nickel ladder: it writes
//! the embedded `.ncl` sources into a scratch workspace, evaluates a contract
//! or an expression against them, and hands back the resulting JSON. There is
//! deliberately no Rust `decode`/`migrate_to` mirror of the Nickel ladder —
//! two implementations of the same migrations would drift, and the Nickel one
//! is the one every `mediapm.ncl` on disk has actually passed through.
//!
//! The one Rust type that remains is the `*Latest` boundary family in
//! `v_latest.rs`, which carries `Option` on user-optional fields and is
//! resolved into the option-free `MediaRuntimeStorage` by
//! `MediaRuntimeStorage::from_boundary`.
//!
//! ## DO NOT REMOVE: versions policy guard
//!
//! - Version dispatch lives in `mod.ncl`; do not re-introduce a parallel Rust
//!   dispatcher over the same versions.
//! - `v_latest.rs` owns the active boundary types; `config/mod.rs` owns only
//!   the resolved (option-free) types and delegates through `from_boundary`.
//! - Do not directly re-export `vX` wire structs from this module; expose
//!   unversioned functions and keep versioned internals encapsulated.

pub mod v_latest;

use serde_json::Value;

use crate::error::MediaPmError;

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
