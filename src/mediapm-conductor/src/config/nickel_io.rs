//! Low-level Nickel evaluation, rendering, and document workspace helpers.
//!
//! # Design
//!
//! These helpers evaluate `.ncl` files through `nickel-lang-core` and render
//! Rust structs as Nickel source.  Results are cached by source-text hash to
//! avoid re-evaluating unchanged documents across repeated decode calls.
//!
//! Each evaluation allocates its own scratch workspace via
//! `scratch_workspace_dir()` (a `mediapm_utils::temp::artifact_dir()` by
//! default); the `TempDir` guard removes it on both success and error paths,
//! so failed decodes never leak scratch files.
//!
//! # Cache discipline
//!
//! - `eval_cache`: (`source_hash`, version) → JSON value for migrated/decode results

use std::collections::HashMap;
use std::fs;
use std::path::Path;
use std::sync::{Mutex, OnceLock};

use blake3;
use nickel_lang_core::error::Error as NickelError;
use nickel_lang_core::eval::cache::CacheImpl;
use nickel_lang_core::program::{BuilderError, Program, ProgramBuilder};
use serde::Serialize;
use serde::de::DeserializeOwned;
use serde_json::Value;

use crate::error::ConductorError;

use super::versions::MOD_NCL_SOURCE;
use super::versions::V1_NCL_SOURCE;
use super::versions::V2_NCL_SOURCE;
use super::versions::V3_NCL_SOURCE;
use super::versions::V4_NCL_SOURCE;
use super::versions::latest_version;
use super::versions::resolve_version_contract;

/// Cache key: (source hash, requested version).
type EvalCacheKey = (blake3::Hash, u32);

/// In-memory cache for `migrate_document_source_to_version` results.
fn eval_cache() -> &'static Mutex<HashMap<EvalCacheKey, Value>> {
    static CACHE: OnceLock<Mutex<HashMap<EvalCacheKey, Value>>> = OnceLock::new();
    CACHE.get_or_init(|| Mutex::new(HashMap::new()))
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

// Test-only per-thread workspace-root override consulted by
// `scratch_workspace_dir`; compiled out of non-test builds.
#[cfg(test)]
thread_local! {
    static WORKSPACE_ROOT_OVERRIDE: std::cell::RefCell<Option<std::path::PathBuf>> =
        const { std::cell::RefCell::new(None) };
}

/// Creates the per-evaluation scratch workspace directory.
///
/// Tests override the workspace root via the `WORKSPACE_ROOT_OVERRIDE`
/// thread-local to assert cleanup in a sibling-free directory: redirecting
/// the process-global `TMPDIR` is racy under plain `cargo test`, where
/// sibling lib-test threads create their own workspaces under the
/// redirected root.
fn scratch_workspace_dir() -> std::io::Result<tempfile::TempDir> {
    #[cfg(test)]
    {
        if let Some(root) = WORKSPACE_ROOT_OVERRIDE.with(|slot| slot.borrow().clone()) {
            return tempfile::Builder::new().tempdir_in(root);
        }
    }
    mediapm_utils::temp::artifact_dir()
}

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

    // Per-decode scratch workspace: the `TempDir` guard removes the
    // directory on success AND on the `?` error paths below. The previous
    // explicit cleanup ran only on success, so a failed evaluation leaked
    // the scratch files.
    let workspace = scratch_workspace_dir().map_err(|source| ConductorError::Io {
        operation: "creating Nickel workspace for document migration".to_string(),
        path: std::env::temp_dir(),
        source,
    })?;

    let mod_path = workspace.path().join("mod.ncl");
    let v1_path = workspace.path().join("v1.ncl");
    let v2_path = workspace.path().join("v2.ncl");
    let v3_path = workspace.path().join("v3.ncl");
    let v4_path = workspace.path().join("v4.ncl");
    let input_path = workspace.path().join("document_input.ncl");
    let wrapper_path = workspace.path().join("decode_document.ncl");

    write_nickel_file(&mod_path, MOD_NCL_SOURCE, "writing temporary Nickel migration helper")?;
    write_nickel_file(&v1_path, V1_NCL_SOURCE, "writing temporary Nickel v1.ncl helper")?;
    write_nickel_file(&v2_path, V2_NCL_SOURCE, "writing temporary Nickel v2.ncl helper")?;
    write_nickel_file(&v3_path, V3_NCL_SOURCE, "writing temporary Nickel v3.ncl helper")?;
    write_nickel_file(&v4_path, V4_NCL_SOURCE, "writing temporary Nickel v4.ncl helper")?;
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
    migrate_document_source_to_version(source, latest_version(), document_kind)
}

#[cfg(test)]
mod tests {
    use super::*;

    /// Test-owned scratch-workspace root for the current test, registered in
    /// [`WORKSPACE_ROOT_OVERRIDE`] for its whole lifetime and deregistered on
    /// drop (including panics). The thread-local override, unlike a
    /// process-global `TMPDIR` redirect, cannot be disturbed by sibling
    /// lib-test threads under plain `cargo test`.
    struct WorkspaceRootOverride(tempfile::TempDir);

    impl WorkspaceRootOverride {
        fn new() -> Self {
            let root = mediapm_utils::temp::artifact_dir().expect("isolated workspace root");
            WORKSPACE_ROOT_OVERRIDE
                .with(|slot| *slot.borrow_mut() = Some(root.path().to_path_buf()));
            Self(root)
        }
    }

    impl Drop for WorkspaceRootOverride {
        fn drop(&mut self) {
            WORKSPACE_ROOT_OVERRIDE.with(|slot| *slot.borrow_mut() = None);
        }
    }

    /// Counts directories directly under `root` (a leaked scratch workspace).
    fn child_dir_count(root: &tempfile::TempDir) -> usize {
        std::fs::read_dir(root.path())
            .expect("read isolated workspace root")
            .filter_map(Result::ok)
            .filter(|entry| entry.file_type().is_ok_and(|kind| kind.is_dir()))
            .count()
    }

    const MINIMAL_V2_DOC: &str = "{ version = 2 }";

    /// A v3 document that names the staging decision on one tool.
    const V3_DOC_WITH_STAGING: &str = r#"{
        version = 3,
        tools = {
            "t" = {
                kind = "executable",
                name = "t",
                command = ["true"],
                runtime = {
                    payload_root = "/tmp/payloads/t",
                    payload_staging = "in_place",
                },
            },
        },
    }"#;

    /// A v3 document that names neither a staging strategy nor a payload root.
    ///
    /// The tool carries no content map, so the document stages nothing and the
    /// cross-field rule has nothing to ask of it. Its `runtime` record is empty
    /// rather than absent, so the two fields being missing is a statement about
    /// the contract instead of about a record nothing wrote.
    const V3_DOC_WITHOUT_STAGING: &str = r#"{
        version = 3,
        tools = {
            "t" = {
                kind = "executable",
                name = "t",
                command = ["true"],
                runtime = {},
            },
        },
    }"#;

    /// A v2 document that names the same tool with a payload.
    const V2_DOC_WITH_PAYLOAD: &str = r#"{
        version = 2,
        tools = {
            "t" = {
                kind = "executable",
                name = "t",
                command = ["true"],
                runtime = { content_map = { "tool" = "inline description" } },
            },
        },
    }"#;

    /// The migration that enters v3 records the behaviour a v2 document had,
    /// and the one that leaves v3 drops a decision v2 cannot express.
    #[test]
    fn migration_between_v2_and_v3_carries_the_staging_decision() {
        let upgraded = migrate_document_source_to_version::<Value>(
            V2_DOC_WITH_PAYLOAD,
            3,
            "configuration document",
        )
        .expect("v2 document must migrate to v3");
        assert_eq!(upgraded["version"].as_f64(), Some(3.0), "the upgrade stamps v3");
        assert_eq!(upgraded["tools"]["t"]["runtime"]["payload_staging"], "copy");
        assert_eq!(upgraded["tools"]["t"]["runtime"]["payload_root"], "");

        let downgraded = migrate_document_source_to_version::<Value>(
            V3_DOC_WITH_STAGING,
            2,
            "configuration document",
        )
        .expect("v3 document must migrate back to v2");
        assert_eq!(downgraded["version"].as_f64(), Some(2.0), "the downgrade stamps v2");
        let runtime = &downgraded["tools"]["t"]["runtime"];
        assert!(
            runtime.get("payload_staging").is_none() && runtime.get("payload_root").is_none(),
            "v2 cannot express the staging decision, so the downgrade drops it: {runtime}"
        );
    }

    /// A v3 document that names its staging decision keeps the name it chose.
    #[test]
    fn a_v3_document_naming_in_place_keeps_that_strategy() {
        let decoded = migrate_document_source_to_version::<Value>(
            V3_DOC_WITH_STAGING,
            3,
            "configuration document",
        )
        .expect("v3 document must decode");
        assert_eq!(decoded["version"].as_f64(), Some(3.0), "the document keeps its version");
        assert_eq!(decoded["tools"]["t"]["runtime"]["payload_staging"], "in_place");
    }

    /// Both staging fields are optional in the contract, so a document that
    /// names neither evaluates without them.
    ///
    /// This is the half the resolved types cannot show: the contract has to
    /// accept the absence before the Rust default can apply to it. The tool
    /// carries no content map, which is the case the cross-field rule exempts.
    #[test]
    fn a_v3_document_naming_no_staging_decision_evaluates_without_one() {
        let decoded = migrate_document_source_to_version::<Value>(
            V3_DOC_WITHOUT_STAGING,
            3,
            "configuration document",
        )
        .expect("a v3 document that stages nothing must decode");
        let runtime = &decoded["tools"]["t"]["runtime"];
        assert!(
            runtime.get("payload_staging").is_none() && runtime.get("payload_root").is_none(),
            "the contract must treat both fields as optional: {runtime}"
        );
    }
    // Syntax-broken document: fails evaluation after the scratch workspace
    // exists, exercising the `?` error path that previously leaked the
    // workspace directory.
    const BROKEN_DOC: &str = "{ version = 2";

    /// Runs `body` with the scratch-workspace root isolated to a fresh,
    /// test-owned directory, asserting no workspace directory remains
    /// afterwards. Only the decode under test can create directories under
    /// the root, so the count is deterministic under every runner.
    fn assert_no_artifact_dir_leak(body: impl FnOnce()) {
        let root = WorkspaceRootOverride::new();
        assert_eq!(child_dir_count(&root.0), 0, "fresh isolated root must start empty");
        body();
        assert_eq!(
            child_dir_count(&root.0),
            0,
            "decode must remove its scratch workspace on both success and error"
        );
    }

    #[test]
    fn decode_success_leaves_no_artifact_dir() {
        assert_no_artifact_dir_leak(|| {
            let decoded = migrate_document_source_to_version::<Value>(
                MINIMAL_V2_DOC,
                2,
                "configuration document",
            );
            assert!(decoded.is_ok(), "minimal v2 doc must decode: {decoded:?}");
        });
    }

    #[test]
    fn decode_error_leaves_no_artifact_dir() {
        assert_no_artifact_dir_leak(|| {
            let decoded = migrate_document_source_to_version::<Value>(
                BROKEN_DOC,
                2,
                "configuration document",
            );
            assert!(decoded.is_err(), "syntax-broken doc must fail decode");
        });
    }
}
