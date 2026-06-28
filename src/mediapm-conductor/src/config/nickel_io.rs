//! Low-level Nickel evaluation, rendering, and document workspace helpers.
//!
//! # Design
//!
//! These helpers manage temporary Nickel workspace directories, evaluate
//! `.ncl` files through `nickel-lang-core`, and render Rust structs as Nickel
//! source.  Results are cached by source-text hash to avoid re-evaluating
//! unchanged documents across repeated decode calls.
//!
//! # Cache discipline
//!
//! - `eval_cache`: (`source_hash`, version) → JSON value for migrated/decode results
//! - `eval_source_value_cache`: `source_hash` → JSON value for schema-agnostic metadata inspection

use std::collections::HashMap;
use std::fs;
use std::path::{Path, PathBuf};
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::{Mutex, OnceLock};

use blake3;
use nickel_lang_core::error::Error as NickelError;
use nickel_lang_core::eval::cache::CacheImpl;
use nickel_lang_core::program::{BuilderError, Program, ProgramBuilder};
use serde::Serialize;
use serde::de::DeserializeOwned;
use serde_json::Value;

use crate::error::ConductorError;

use super::versions::{
    MOD_NCL_SOURCE, V1_NCL_SOURCE, V2_NCL_SOURCE, resolve_version_contract, v_latest,
};

/// Cache key: (source hash, requested version).
type EvalCacheKey = (blake3::Hash, u32);

/// In-memory cache for `migrate_document_source_to_version` results.
fn eval_cache() -> &'static Mutex<HashMap<EvalCacheKey, Value>> {
    static CACHE: OnceLock<Mutex<HashMap<EvalCacheKey, Value>>> = OnceLock::new();
    CACHE.get_or_init(|| Mutex::new(HashMap::new()))
}

/// In-memory cache for `evaluate_document_source_value` results.
fn eval_source_value_cache() -> &'static Mutex<HashMap<blake3::Hash, Value>> {
    static CACHE: OnceLock<Mutex<HashMap<blake3::Hash, Value>>> = OnceLock::new();
    CACHE.get_or_init(|| Mutex::new(HashMap::new()))
}

/// Monotonically increasing counter for unique workspace filenames.
pub(super) static NICKEL_WORKSPACE_COUNTER: AtomicU64 = AtomicU64::new(0);

/// Returns a reference to the shared temporary Nickel workspace directory,
/// creating it on first access.
pub(super) fn nickel_workspace_dir() -> &'static Path {
    static DIR: OnceLock<PathBuf> = OnceLock::new();
    DIR.get_or_init(|| {
        let dir =
            std::env::temp_dir().join(format!("mediapm-conductor-nickel-{}", std::process::id()));
        let _ = fs::create_dir_all(&dir);
        dir
    })
}

/// Writes one Nickel source file into the temporary workspace.
pub(super) fn write_nickel_file(
    path: &Path,
    source: &str,
    operation: &str,
) -> Result<(), ConductorError> {
    fs::write(path, source).map_err(|source_err| ConductorError::Io {
        operation: operation.to_string(),
        path: path.to_path_buf(),
        source: source_err,
    })
}

/// Renders a Nickel interpreter error with file context for user-facing diagnostics.
fn nickel_eval_error(
    program: &Program<CacheImpl>,
    err: NickelError,
    context: &str,
) -> ConductorError {
    ConductorError::Workflow(format!(
        "{context}: {}",
        nickel_lang_core::error::report::report_as_str(
            &mut program.files(),
            err,
            nickel_lang_core::error::report::ColorOpt::Never,
        )
    ))
}

/// Evaluates one temporary Nickel main file and deserializes the fully exported result.
pub(super) fn evaluate_main_file_as<T>(main_file: &Path, context: &str) -> Result<T, ConductorError>
where
    T: DeserializeOwned,
{
    let mut program: Program<CacheImpl> =
        ProgramBuilder::new().add_path(main_file.as_os_str()).build().map_err(|err| match err {
            BuilderError::Io { path: _, error } => ConductorError::Io {
                operation: "constructing Nickel program".to_string(),
                path: main_file.to_path_buf(),
                source: error,
            },
            BuilderError::NoInputs => ConductorError::Workflow(
                "constructing Nickel program: no inputs provided".to_string(),
            ),
        })?;

    let value =
        program.eval_full_for_export().map_err(|err| nickel_eval_error(&program, err, context))?;

    T::deserialize(value).map_err(|err| {
        ConductorError::Serialization(format!(
            "{context}: failed deserializing exported Nickel value: {err}"
        ))
    })
}

/// Evaluates one raw Nickel document source and returns its exported value.
///
/// This helper is intentionally schema-agnostic and is used for metadata
/// inspection tasks such as top-level field/key validation.
///
/// Results are cached by source text hash to avoid re-evaluating unchanged
/// documents across repeated inspection calls.
fn evaluate_document_source_value(
    source: &str,
    document_kind: &str,
) -> Result<Value, ConductorError> {
    let cache_key = blake3::hash(source.as_bytes());
    {
        let cache = eval_source_value_cache();
        let guard = cache.lock().unwrap();
        if let Some(cached) = guard.get(&cache_key) {
            return Ok(cached.clone());
        }
    }

    let workspace_dir = nickel_workspace_dir();
    let seq = NICKEL_WORKSPACE_COUNTER.fetch_add(1, Ordering::Relaxed);

    let subdir = workspace_dir.join(format!("inspect-{seq}"));
    fs::create_dir_all(&subdir).map_err(|source| ConductorError::Io {
        operation: "creating Nickel workspace subdirectory for source value evaluation".to_string(),
        path: subdir.clone(),
        source,
    })?;

    let input_path = subdir.join("document_input.ncl");
    let wrapper_path = subdir.join("inspect_document.ncl");

    write_nickel_file(
        &input_path,
        source,
        "writing temporary Nickel input document for metadata inspection",
    )?;

    let wrapper_source = "import \"document_input.ncl\"\n".to_string();
    write_nickel_file(
        &wrapper_path,
        &wrapper_source,
        "writing temporary Nickel metadata inspection wrapper",
    )?;

    let result: Value = evaluate_main_file_as(
        &wrapper_path,
        &format!("evaluating {document_kind} source metadata"),
    )?;

    // Clean up temporary directory after evaluation completes.
    let _ = fs::remove_dir_all(&subdir);

    let cache = eval_source_value_cache();
    let mut guard = cache.lock().unwrap();
    guard.insert(cache_key, result.clone());
    drop(guard);

    Ok(result)
}

/// Parses and validates the explicit top-level `version` marker from one
/// conductor Nickel source document.
///
/// All conductor configuration documents must carry an explicit numeric
/// `version` field.
pub(super) fn read_document_version_marker(
    source: &str,
    document_kind: &str,
) -> Result<u32, ConductorError> {
    let value = evaluate_document_source_value(source, document_kind)?;
    let object = value.as_object().ok_or_else(|| {
        ConductorError::Workflow(format!(
            "{document_kind} must evaluate to one record with a top-level 'version' field"
        ))
    })?;

    let version_value = object.get("version").ok_or_else(|| {
        ConductorError::Workflow(format!(
            "{document_kind} must define a top-level numeric 'version' field"
        ))
    })?;

    let marker_u64 = if let Some(version) = version_value.as_u64() {
        version
    } else if let Some(version) = version_value.as_f64() {
        if !version.is_finite() || version.fract() != 0.0 || version < 0.0 {
            return Err(ConductorError::Workflow(format!(
                "{document_kind} top-level 'version' must be a non-negative integer"
            )));
        }

        format!("{version:.0}").parse::<u64>().map_err(|_| {
            ConductorError::Workflow(format!(
                "{document_kind} top-level 'version' value {version} exceeds supported range"
            ))
        })?
    } else {
        return Err(ConductorError::Workflow(format!(
            "{document_kind} top-level 'version' must be numeric"
        )));
    };

    let marker = u32::try_from(marker_u64).map_err(|_| {
        ConductorError::Workflow(format!(
            "{document_kind} top-level 'version' value {marker_u64} exceeds supported range"
        ))
    })?;

    resolve_version_contract(marker, document_kind)?;
    Ok(marker)
}

/// Determines one in-memory migration target by reading the document version
/// marker and selecting the latest compatible version.
pub(super) fn latest_version_for_source(
    source: &str,
    document_kind: &str,
) -> Result<u32, ConductorError> {
    let version = read_document_version_marker(source, document_kind)?;
    resolve_version_contract(version, document_kind)?;
    Ok(version)
}

/// Returns whether `key` can be emitted as a bare Nickel identifier.

/// Evaluates one document source through the embedded Nickel migration wrapper
/// into one requested persisted schema version.
///
/// Results are cached by (source text hash, requested version) to avoid
/// re-evaluating unchanged documents on repeated decode calls.
pub(crate) fn migrate_document_source_to_version<T>(
    source: &str,
    requested_version: u32,
    document_kind: &str,
) -> Result<T, ConductorError>
where
    T: DeserializeOwned + Serialize,
{
    let cache_key = (blake3::hash(source.as_bytes()), requested_version);
    {
        let cache = eval_cache();
        let guard = cache.lock().unwrap();
        if let Some(cached) = guard.get(&cache_key) {
            return serde_json::from_value(cached.clone()).map_err(|err| {
                ConductorError::Serialization(format!(
                    "failed deserializing cached {document_kind}: {err}"
                ))
            });
        }
    }

    let validator_name = format!("validate_document_v{requested_version}");
    let version_file_name = resolve_version_contract(requested_version, document_kind)?;

    let workspace_dir = nickel_workspace_dir();
    let seq = NICKEL_WORKSPACE_COUNTER.fetch_add(1, Ordering::Relaxed);

    let subdir = workspace_dir.join(format!("decode-{seq}"));
    fs::create_dir_all(&subdir).map_err(|source| ConductorError::Io {
        operation: "creating Nickel workspace subdirectory for document migration".to_string(),
        path: subdir.clone(),
        source,
    })?;

    let mod_path = subdir.join("mod.ncl");
    let v1_path = subdir.join("v1.ncl");
    let v2_path = subdir.join("v2.ncl");
    let input_path = subdir.join("document_input.ncl");
    let wrapper_path = subdir.join("decode_document.ncl");

    write_nickel_file(&mod_path, MOD_NCL_SOURCE, "writing temporary Nickel migration helper")?;
    write_nickel_file(&v1_path, V1_NCL_SOURCE, "writing temporary Nickel v1.ncl helper")?;
    write_nickel_file(&v2_path, V2_NCL_SOURCE, "writing temporary Nickel v2.ncl helper")?;
    write_nickel_file(&input_path, source, "writing temporary Nickel input document")?;

    let wrapper_source = format!(
        r#"
let migration = import "mod.ncl" in
let version = import "{version_file_name}" in
let document = import "document_input.ncl" in
version.{validator_name} (migration.migrate_to {requested_version} document)
"#
    );
    write_nickel_file(&wrapper_path, &wrapper_source, "writing temporary Nickel decode wrapper")?;

    let result: T = evaluate_main_file_as(
        &wrapper_path,
        &format!("evaluating {document_kind} via Nickel migration wrapper"),
    )?;

    // Clean up temporary directory after evaluation completes.
    let _ = fs::remove_dir_all(&subdir);

    let json_value = serde_json::to_value(&result).map_err(|err| {
        ConductorError::Serialization(format!("failed caching evaluated {document_kind}: {err}"))
    })?;
    let cache = eval_cache();
    let mut guard = cache.lock().unwrap();
    guard.insert(cache_key, json_value);
    drop(guard);

    Ok(result)
}

/// Evaluates one document source through the embedded Nickel migration wrapper
/// into the latest supported schema version.
pub(super) fn evaluate_document_source<T>(
    source: &str,
    document_kind: &str,
) -> Result<T, ConductorError>
where
    T: DeserializeOwned + Serialize,
{
    migrate_document_source_to_version(source, v_latest::NICKEL_VERSION_LATEST, document_kind)
}
