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
//! `v_latest.rs`, carried on [`MediaPmDocument::runtime`](document::MediaPmDocument::runtime).
//! Because that field's type is a versioned boundary type, the document model
//! itself lives here, in `document.rs`.
//!
//! This module is the ONLY bridge between that family and the resolved
//! (option-free) types in `config/mod.rs`. Two unversioned entry points cross
//! the boundary, and neither names a `*Latest` type where a caller outside
//! `versions/` can see it:
//!
//! - [`resolve_runtime_storage`] — read direction, boundary to resolved.
//! - [`runtime_boundary`] — write direction, resolved intent to a boundary
//!   value whose every knob is spelled out.
//!
//! A caller names the type only ever on the *inside*: `runtime_boundary`
//! returns one without the caller mentioning it, and the caller feeds it
//! straight into `MediaPmDocument::runtime`.
//!
//! ## DO NOT REMOVE: versions policy guard
//!
//! - Version dispatch lives in `mod.ncl`; do not re-introduce a parallel Rust
//!   dispatcher over the same versions.
//! - `v_latest.rs` owns the active boundary types; `document.rs` owns the
//!   wire-shaped document that carries one; `config/mod.rs` owns only the
//!   resolved (option-free) types and reaches the boundary solely through the
//!   two entry points above.
//! - Do not re-export `v_latest` symbols. A `pub use` here would put the
//!   active version number into the crate's public API, so the next schema
//!   bump would break every consumer for a detail they never asked about.
//! - `config/mod.rs` re-exports [`MediaPmDocument`], which is an unversioned
//!   name and is not a boundary leak; it must never gain a `versions::v`
//!   path beside it.

mod document;
mod v_latest;

use std::collections::BTreeMap;

use serde_json::Value;

use crate::error::MediaPmError;

pub use document::MediaPmDocument;
pub use v_latest::MediaRuntimeStorageLatest;

use super::{MediaRuntimeStorage, RuntimeBasePaths, defaults};
use v_latest::{
    RuntimeCachingConfigLatest, RuntimeEnvironmentConfigLatest, RuntimeLifecycleConfigLatest,
    RuntimeMaterializationConfigLatest, RuntimePathsConfigLatest, RuntimeVerificationConfigLatest,
};

/// Resolves a boundary runtime-storage value into the option-free model.
///
/// This is the read-direction boundary entry point. Its parameter is named
/// here, inside `versions/`, precisely so a caller outside never has to name
/// it: the usual source of a boundary value is
/// [`MediaPmDocument::runtime`](document::MediaPmDocument::runtime), which the
/// caller reaches as a field of a type it may name.
///
/// Unset boundary paths resolve to an empty `PathBuf`, which the override
/// machinery reads as "no override" so the `MediaPmPaths` defaults win.
///
/// # Examples
///
/// ```
/// # use mediapm::config::{MediaPmDocument, RuntimeBasePaths, versions};
/// # fn main() -> Result<(), Box<dyn std::error::Error>> {
/// let document = MediaPmDocument::default();
/// let base = RuntimeBasePaths {
///     workspace_root: "/w".into(),
///     mediapm_dir: "/w/.mediapm".into(),
/// };
/// let resolved = versions::resolve_runtime_storage(&document.runtime, &base);
/// assert!(resolved.paths.mediapm_dir.as_os_str().is_empty());
/// # Ok(())
/// # }
/// ```
#[must_use]
pub fn resolve_runtime_storage(
    latest: &MediaRuntimeStorageLatest,
    base: &RuntimeBasePaths,
) -> MediaRuntimeStorage {
    v_latest::resolve_runtime_storage(latest, base)
}

/// The runtime knobs a document may deviate from the spelled-out defaults by.
///
/// [`runtime_boundary`] writes out every knob of the boundary value, using
/// [`defaults`] for anything left as `None` here.
/// That is deliberate: the point of the resulting document is that a reader
/// can see the whole runtime surface, so the constructor fills in the defaults
/// rather than leaving fields implicit.
///
/// Every field is a maintenance obligation, so a field earns its place only
/// when a caller genuinely overrides the spelled-out value. A new knob added
/// to the boundary type needs no field here — it is spelled out from
/// `defaults` like everything else.
///
/// [`Default`] means "deviate from nothing", which is the right base for a
/// document that wants the standard runtime surface written out in full.
#[derive(Debug, Clone, Default)]
pub struct RuntimeBoundaryDeviations {
    /// Value for `paths.hierarchy_root_dir`.
    ///
    /// The spelled-out default is the workspace root, so materializing into a
    /// subdirectory is a deviation worth recording.
    pub hierarchy_root_dir: Option<String>,

    /// Value for `paths.mediapm_state_config`.
    ///
    /// Three states, all meaningful, which is why this is an
    /// `Option<Option<String>>` and not a plain `Option`:
    ///
    /// - `None` — write the spelled-out default (`.mediapm/state.json`).
    /// - `Some(None)` — write the knob as explicitly unset, serializing to
    ///   `mediapm_state_config = null`. A document that means to demonstrate
    ///   the unset state must not silently get the default instead, so this
    ///   case is spelled out here rather than left to fall out of a
    ///   `Default` impl.
    /// - `Some(Some(path))` — write that path.
    pub mediapm_state_config: Option<Option<String>>,

    /// Value for `lifecycle.instance_ttl_seconds`.
    ///
    /// The spelled-out default is the seven-day
    /// [`DEFAULT_INSTANCE_TTL_SECONDS`](crate::config::defaults::DEFAULT_INSTANCE_TTL_SECONDS).
    pub instance_ttl_seconds: Option<u64>,

    /// Value for `environment.profiler_enabled`.
    ///
    /// The spelled-out default is
    /// [`DEFAULT_PROFILER_ENABLED`](crate::config::defaults::DEFAULT_PROFILER_ENABLED)
    /// (`false`).
    pub profiler_enabled: Option<bool>,

    /// Value for `environment.inherited_env_vars`.
    ///
    /// The spelled-out default is unset, which leaves the host's own
    /// environment in charge. A caller that wants the document to record an
    /// explicit host-env-var map supplies one here.
    pub inherited_env_vars: Option<BTreeMap<String, Vec<String>>>,
}

/// The standard workspace-relative runtime layout, spelled out in full.
///
/// [`runtime_boundary`] writes these into every path knob so the produced
/// document documents the whole path surface instead of leaving it implicit.
/// The values are the ones [`MediaPmPaths`](crate::paths::MediaPmPaths)
/// resolves when no override is supplied, expressed relative to the workspace
/// root (which is how a `mediapm.ncl` spells them).
///
/// This is a single private table on purpose. It is a second place that knows
/// the layout, so `runtime_boundary_spells_out_the_documented_path_layout`
/// pins it to `MediaPmPaths::from_root`: a layout change that misses this
/// table fails the test rather than silently emitting a document whose written
/// paths disagree with the paths the runtime actually uses.
mod spelled_path_layout {
    /// Value of every path knob when a document spells the layout out in full.
    ///
    /// `hierarchy_root_dir` is the workspace root, spelled `"."`; a caller that
    /// materializes into a subdirectory overrides it through
    /// [`RuntimeBoundaryDeviations::hierarchy_root_dir`].
    pub(super) const PATHS: [(&str, &str); 10] = [
        ("mediapm_dir", ".mediapm"),
        ("hierarchy_root_dir", "."),
        ("mediapm_state_config", ".mediapm/state.json"),
        ("conductor_config", "mediapm.conductor.ncl"),
        ("conductor_generated_config", "mediapm.conductor.generated.ncl"),
        ("conductor_state_config", ".mediapm/state.conductor.json"),
        ("conductor_schema_dir", ".mediapm/config/conductor"),
        ("mediapm_schema_dir", ".mediapm/config/mediapm"),
        ("env_file", ".mediapm/.env"),
        ("env_generated_file", ".mediapm/.env.generated"),
    ];
}

/// Looks up one spelled-out path value by knob name.
///
/// # Panics
///
/// Panics if `knob` is not one of the ten entries in [`spelled_path_layout`],
/// which can only happen if a caller in this module is edited to pass a
/// misspelled constant. The list is closed and private, so this is a
/// programming-error assertion rather than a runtime condition.
fn spelled_path(knob: &str) -> String {
    spelled_path_layout::PATHS.iter().find(|(name, _)| *name == knob).map_or_else(
        || panic!("`{knob}` is not a spelled-out runtime path knob"),
        |(_, value)| (*value).to_string(),
    )
}

/// Builds a boundary runtime-storage value with every knob spelled out.
///
/// This is the write-direction counterpart to [`resolve_runtime_storage`] and
/// the only way a caller outside `versions/` obtains a boundary value. The
/// returned type is named here, inside `versions/`, so a caller never has to
/// name it — the value goes straight into
/// [`MediaPmDocument::runtime`](document::MediaPmDocument::runtime):
///
/// ```
/// # use mediapm::config::{MediaPmDocument, versions};
/// # fn main() {
/// let mut document = MediaPmDocument::default();
/// document.runtime = versions::runtime_boundary(versions::RuntimeBoundaryDeviations {
///     hierarchy_root_dir: Some("media".to_string()),
///     profiler_enabled: Some(true),
///     ..Default::default()
/// });
/// assert_eq!(document.runtime.paths.hierarchy_root_dir.as_deref(), Some("media"));
/// # }
/// ```
///
/// Every knob absent from `deviations` is written out explicitly — the scalar
/// ones from [`defaults`], the path ones from
/// `spelled_path_layout` — so the produced document documents the whole
/// runtime surface rather than only the interesting parts.
#[must_use]
pub fn runtime_boundary(deviations: RuntimeBoundaryDeviations) -> MediaRuntimeStorageLatest {
    let RuntimeBoundaryDeviations {
        hierarchy_root_dir,
        mediapm_state_config,
        instance_ttl_seconds,
        profiler_enabled,
        inherited_env_vars,
    } = deviations;

    MediaRuntimeStorageLatest {
        paths: RuntimePathsConfigLatest {
            mediapm_dir: Some(spelled_path("mediapm_dir")),
            hierarchy_root_dir: Some(
                hierarchy_root_dir.unwrap_or_else(|| spelled_path("hierarchy_root_dir")),
            ),
            // Flat `Option<String>` in the boundary, three states in the
            // deviation: the deviation's `Some(None)` is what lands here as
            // `None`, so an explicitly-unset knob stays unset instead of
            // collapsing back to the spelled-out default.
            mediapm_state_config: match mediapm_state_config {
                Some(explicit) => explicit,
                None => Some(spelled_path("mediapm_state_config")),
            },
            conductor_config: Some(spelled_path("conductor_config")),
            conductor_generated_config: Some(spelled_path("conductor_generated_config")),
            conductor_state_config: Some(spelled_path("conductor_state_config")),
            conductor_schema_dir: Some(spelled_path("conductor_schema_dir")),
            mediapm_schema_dir: Some(spelled_path("mediapm_schema_dir")),
            env_file: Some(spelled_path("env_file")),
            env_generated_file: Some(spelled_path("env_generated_file")),
        },
        materialization: RuntimeMaterializationConfigLatest {
            materialization_preference_order: Some(
                defaults::default_materialization_preference_order(),
            ),
            verify_materialization: Some(defaults::default_verify_materialization()),
        },
        verification: RuntimeVerificationConfigLatest {
            verify_on_read: Some(defaults::default_verify_on_read()),
            verify_on_read_sample_denominator: Some(
                defaults::default_verify_on_read_sample_denominator(),
            ),
            verify_on_read_stale_timeout_secs: Some(
                defaults::default_verify_on_read_stale_timeout_secs(),
            ),
        },
        caching: RuntimeCachingConfigLatest {
            reconstructed_cache_ttl_seconds: Some(
                defaults::default_reconstructed_cache_ttl_seconds(),
            ),
        },
        lifecycle: RuntimeLifecycleConfigLatest {
            instance_ttl_seconds: Some(
                instance_ttl_seconds.unwrap_or(defaults::default_instance_ttl_seconds()),
            ),
        },
        environment: RuntimeEnvironmentConfigLatest {
            inherited_env_vars,
            profiler_enabled: Some(
                profiler_enabled.unwrap_or(defaults::default_profiler_enabled()),
            ),
        },
        path_sanitization: None,
        retry_impure: Some(defaults::default_retry_impure()),
        tools: BTreeMap::new(),
    }
}

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

#[cfg(test)]
mod tests {
    use std::path::Path;

    use super::v_latest::{MediaRuntimeStorageLatest, RuntimePathsConfigLatest};
    use super::{
        RuntimeBoundaryDeviations, resolve_runtime_storage, runtime_boundary, spelled_path,
    };
    use crate::config::{MediaRuntimeStorage, RuntimeBasePaths};
    use crate::paths::MediaPmPaths;

    /// Verifies an unset boundary path resolves to an empty `PathBuf` rather
    /// than a fabricated one.
    ///
    /// The override machinery reads an empty path as "no override" so the
    /// `MediaPmPaths` defaults win. Populating a default here instead would
    /// silently redirect saves to a different path than the one `MediaPmPaths`
    /// exposes for reads.
    #[test]
    fn resolve_runtime_storage_leaves_unset_paths_empty() {
        let boundary = runtime_boundary(RuntimeBoundaryDeviations::default());
        let resolved = resolve_runtime_storage(
            &MediaRuntimeStorageLatest { paths: RuntimePathsConfigLatest::default(), ..boundary },
            &RuntimeBasePaths { workspace_root: "/w".into(), mediapm_dir: "/m".into() },
        );

        for path in [
            &resolved.paths.mediapm_dir,
            &resolved.paths.hierarchy_root_dir,
            &resolved.paths.mediapm_state_config,
            &resolved.paths.conductor_config,
            &resolved.paths.conductor_generated_config,
            &resolved.paths.conductor_state_config,
            &resolved.paths.conductor_schema_dir,
            &resolved.paths.mediapm_schema_dir,
            &resolved.paths.env_file,
            &resolved.paths.env_generated_file,
        ] {
            assert!(path.as_os_str().is_empty(), "unset boundary path must resolve to empty");
        }
    }

    /// Verifies every scalar the boundary spells out resolves to the same
    /// value the resolved model documents.
    ///
    /// A boundary field left `None` and a field spelled out at its default must
    /// land on the same resolved value, or a document would resolve differently
    /// depending only on whether it spelled a default out.
    #[test]
    fn resolve_runtime_storage_agrees_with_defaults() {
        let resolved = resolve_runtime_storage(
            &runtime_boundary(RuntimeBoundaryDeviations::default()),
            &RuntimeBasePaths { workspace_root: "/w".into(), mediapm_dir: "/m".into() },
        );

        assert!(!resolved.retry_impure);
        assert_eq!(resolved.path_sanitization, crate::config::SanitizeNamesConfig::default());
        assert_eq!(
            resolved.materialization.materialization_preference_order,
            crate::config::defaults::default_materialization_preference_order()
        );
        assert_eq!(
            resolved.verification.verify_on_read_sample_denominator,
            crate::config::defaults::default_verify_on_read_sample_denominator()
        );
        assert_eq!(
            resolved.caching.reconstructed_cache_ttl_seconds,
            crate::config::defaults::default_reconstructed_cache_ttl_seconds()
        );
        assert_eq!(
            resolved.lifecycle.instance_ttl_seconds,
            crate::config::defaults::default_instance_ttl_seconds()
        );
        assert_eq!(
            resolved.environment.profiler_enabled,
            crate::config::defaults::default_profiler_enabled()
        );
        assert_ne!(
            resolved.paths.mediapm_dir,
            MediaRuntimeStorage::default().paths.mediapm_dir,
            "spelled-out paths must survive the round trip into the resolved model"
        );
    }

    /// Pins the spelled-out path table to the layout `MediaPmPaths` actually
    /// resolves.
    ///
    /// [`spelled_path_layout`] is the only place outside `paths.rs` that knows
    /// the runtime layout, so a layout change that updates one and not the
    /// other would emit a `mediapm.ncl` whose written paths disagree with the
    /// paths the runtime uses — a document that validates and then resolves
    /// somewhere else. Comparing the table against a real
    /// `MediaPmPaths::from_root` is what makes the second source of truth
    /// safe rather than merely tidy.
    #[test]
    fn runtime_boundary_spells_out_the_documented_path_layout() {
        let root = Path::new("/workspace");
        let paths = MediaPmPaths::from_root(root);

        assert_eq!(spelled_path("mediapm_dir"), ".mediapm");
        assert_eq!(
            spelled_path("hierarchy_root_dir"),
            ".",
            "the workspace root is the spelled-out hierarchy root"
        );
        assert_eq!(spelled_path("conductor_config"), "mediapm.conductor.ncl");
        assert_eq!(spelled_path("conductor_generated_config"), "mediapm.conductor.generated.ncl");

        for (knob, expected) in [
            ("mediapm_dir", &paths.runtime_root),
            ("mediapm_state_config", &paths.mediapm_state_json),
            ("conductor_config", &paths.conductor_user_ncl),
            ("conductor_generated_config", &paths.conductor_generated_ncl),
            ("conductor_state_config", &paths.conductor_state_config),
            ("conductor_schema_dir", &paths.conductor_schema_dir),
            (
                "mediapm_schema_dir",
                paths.schema_export_dir.as_ref().expect("schema export is enabled by default"),
            ),
            ("env_file", &paths.env_file),
            ("env_generated_file", &paths.env_generated_file),
        ] {
            let relative = expected
                .strip_prefix(root)
                .unwrap_or_else(|_| panic!("{knob} is not under the workspace root"))
                .to_string_lossy()
                .replace('\\', "/");
            assert_eq!(spelled_path(knob), relative, "spelled-out {knob} must match MediaPmPaths");
        }
    }

    /// Verifies the boundary constructor spells out every knob, so a document
    /// built through it documents the whole runtime surface.
    ///
    /// This is the property the two demos depend on: their generated
    /// `mediapm.ncl` must keep listing every field regardless of which
    /// deviations the caller supplied.
    #[test]
    fn runtime_boundary_spells_out_every_knob() {
        let boundary = runtime_boundary(RuntimeBoundaryDeviations::default());

        assert_eq!(boundary.paths.mediapm_dir.as_deref(), Some(".mediapm"));
        assert_eq!(boundary.paths.hierarchy_root_dir.as_deref(), Some("."));
        assert_eq!(boundary.paths.mediapm_state_config.as_deref(), Some(".mediapm/state.json"));
        assert!(boundary.materialization.materialization_preference_order.is_some());
        assert!(boundary.materialization.verify_materialization.is_some());
        assert!(boundary.verification.verify_on_read.is_some());
        assert!(boundary.verification.verify_on_read_sample_denominator.is_some());
        assert!(boundary.verification.verify_on_read_stale_timeout_secs.is_some());
        assert!(boundary.caching.reconstructed_cache_ttl_seconds.is_some());
        assert!(boundary.lifecycle.instance_ttl_seconds.is_some());
        assert!(boundary.environment.profiler_enabled.is_some());
        assert!(boundary.retry_impure.is_some());
    }

    /// Verifies the three-state `mediapm_state_config` deviation keeps its
    /// middle case distinct from the default.
    ///
    /// A document that means to demonstrate the unset state must serialize
    /// `mediapm_state_config = null`; collapsing `Some(None)` into the default
    /// would silently change what the document teaches.
    #[test]
    fn explicitly_unset_state_config_differs_from_the_default() {
        let unset = runtime_boundary(RuntimeBoundaryDeviations {
            mediapm_state_config: Some(None),
            ..Default::default()
        });
        let spelled = runtime_boundary(RuntimeBoundaryDeviations::default());

        assert_eq!(unset.paths.mediapm_state_config, None);
        assert_eq!(spelled.paths.mediapm_state_config.as_deref(), Some(".mediapm/state.json"));
        assert_ne!(unset.paths.mediapm_state_config, spelled.paths.mediapm_state_config);
    }
}
