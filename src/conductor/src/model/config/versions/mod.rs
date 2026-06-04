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
mod tests {
    //! Tests for latest Nickel schema and Rust bridge compatibility.
    //!
    //! ## DO NOT REMOVE: latest schema compatibility guard
    //!
    //! These tests ensure the latest embedded Nickel contract (`vX.ncl`) stays
    //! wire-compatible with `v_latest.rs` Rust structs. When the schema evolves,
    //! update these tests alongside `v_latest.rs` and the latest `vX.ncl`.

    use super::nickel_io::{
        TempNickelWorkspace, evaluate_main_file_as, migrate_document_source_to_version,
        render_document_as_nickel, write_nickel_file,
    };
    use super::{ConductorError, MOD_NCL_SOURCE};
    use super::{
        USER_NICKEL_VERSION, decode_machine_document, decode_user_document,
        encode_machine_document, encode_user_document,
    };
    use super::{latest, resolve_version_contract, v_latest};
    use crate::model::config::{
        ImpureTimestamp, InputBinding, MachineNickelDocument, OutputPolicy, ToolInputKind,
        UserNickelDocument, WorkflowSpec, WorkflowStepSpec,
    };
    use crate::model::state::OutputSaveMode;
    use serde::Deserialize;

    /// One declared one-hop migration edge from Nickel migration metadata.
    #[derive(Debug, Clone, PartialEq, Eq, Deserialize)]
    struct AtomicMigrationPair {
        /// Source schema marker.
        from: u32,
        /// Destination schema marker.
        to: u32,
    }

    /// Exposed migration metadata from `mod.ncl` used by Rust invariants tests.
    #[derive(Debug, Clone, PartialEq, Eq, Deserialize)]
    struct MigrationMetadata {
        /// Latest schema marker exposed by Nickel migration module.
        current_version: u32,
        /// Supported schema markers exposed by Nickel migration module.
        supported_versions: Vec<u32>,
        /// Declared one-hop migration edges.
        atomic_migration_pairs: Vec<AtomicMigrationPair>,
    }

    /// Reads migration metadata from `mod.ncl` through Nickel evaluation.
    fn read_migration_metadata() -> Result<MigrationMetadata, ConductorError> {
        let workspace = TempNickelWorkspace::new()?;
        write_nickel_file(
            &workspace.path().join("mod.ncl"),
            MOD_NCL_SOURCE,
            "writing temporary Nickel migration helper for metadata test",
        )?;
        write_nickel_file(
            &workspace.path().join(latest::NCL_FILE_NAME),
            latest::NCL_SOURCE,
            "writing temporary latest Nickel contract for migration metadata test",
        )?;

        let wrapper_source = r#"
    let migration = import "mod.ncl" in
    {
      current_version = migration.current_version,
      supported_versions = migration.supported_versions,
      atomic_migration_pairs = migration.atomic_migration_pairs,
    }
    "#;
        let wrapper_path = workspace.path().join("migration_metadata.ncl");
        write_nickel_file(
            &wrapper_path,
            wrapper_source,
            "writing temporary Nickel migration metadata wrapper",
        )?;

        evaluate_main_file_as(&wrapper_path, "evaluating Nickel migration metadata")
    }

    /// Evaluates one document source through exactly one declared atomic
    /// migration hop (`from_version -> to_version`).
    fn migrate_document_source_atomic<T>(
        source: &str,
        from_version: u32,
        to_version: u32,
        document_kind: &str,
    ) -> Result<T, ConductorError>
    where
        T: serde::de::DeserializeOwned,
    {
        let (_, version_contract_source) = resolve_version_contract(to_version, document_kind)?;
        let validator_name = format!("validate_document_v{to_version}");
        let workspace = TempNickelWorkspace::new()?;

        write_nickel_file(
            &workspace.path().join("mod.ncl"),
            MOD_NCL_SOURCE,
            "writing temporary Nickel migration helper for atomic migration test",
        )?;
        write_nickel_file(
            &workspace.path().join(latest::NCL_FILE_NAME),
            version_contract_source,
            "writing temporary latest Nickel contract for atomic migration test",
        )?;
        write_nickel_file(
            &workspace.path().join("document_input.ncl"),
            source,
            "writing temporary Nickel input document for atomic migration test",
        )?;

        let wrapper_source = format!(
            r#"
        let migration = import "mod.ncl" in
        let version = import "{}" in
        let document = import "document_input.ncl" in
        version.{validator_name} (migration.migrate_atomic {} {} document)
        "#,
            latest::NCL_FILE_NAME,
            from_version,
            to_version,
        );
        let wrapper_path = workspace.path().join("atomic_migrate_document.ncl");
        write_nickel_file(
            &wrapper_path,
            &wrapper_source,
            "writing temporary Nickel atomic migration wrapper",
        )?;

        evaluate_main_file_as(
            &wrapper_path,
            &format!(
                "evaluating atomic Nickel migration {from_version}->{to_version} for {document_kind}"
            ),
        )
    }

    /// Verifies that one Rust-authored latest envelope survives Nickel migration
    /// and validation unchanged.
    #[test]
    fn latest_schema_round_trips_latest_rust_bridge_envelope() {
        let envelope = v_latest::NickelEnvelopeLatest {
            version: latest::VERSION,
            ..v_latest::NickelEnvelopeLatest::default()
        };

        let source_bytes = render_document_as_nickel(&envelope, "compatibility-envelope")
            .expect("render latest envelope as Nickel source");
        let source = std::str::from_utf8(&source_bytes).expect("rendered envelope must be UTF-8");
        let decoded: v_latest::NickelEnvelopeLatest =
            migrate_document_source_to_version(source, latest::VERSION, "compatibility-envelope")
                .expect("decode latest envelope through Nickel migration wrapper");

        assert_eq!(decoded, envelope);
    }

    /// Verifies that the latest Nickel contract accepts a shape covering all
    /// struct fields in `v_latest.rs` that can be exercised without real hashes.
    #[test]
    fn latest_schema_deserializes_comprehensive_v_latest_shape() {
        let source = r#"
    {
    version = 1,
    external_data = {},
    tools = {
        "tool_builtin@1.0.0" = {
            kind = "builtin",
            name = "echo",
            version = "1.0.0",
        },
        "tool_exec@1.0.0" = {
            kind = "executable",
            is_impure = false,
            command = ["bin/tool", "--flag"],
            env_vars = {
                DEMO = "true",
            },
            success_codes = [0],
            inputs = {
                input_1 = {},
            },
            outputs = {
                out = {
                    capture = {
                        kind = "stdout",
                    },
                },
            },
        },
    },
    workflows = {
        wf = {
            name = "workflow label",
            description = "workflow description",
            steps = [
                {
                    id = "step-1",
                    tool = "tool_exec@1.0.0",
                    inputs = {
                        input_1 = "hello",
                    },
                    outputs = {
                        out = {
                            save = true,
                        },
                    },
                },
            ],
        },
    },
    tool_configs = {},
    impure_timestamps = {},
    state_pointer = null,
    }
    "#;

        let decoded: v_latest::NickelEnvelopeLatest =
            migrate_document_source_to_version(source, latest::VERSION, "compatibility-shape")
                .expect("decode comprehensive latest shape via Nickel");

        assert_eq!(decoded.version, latest::VERSION);
        assert_eq!(decoded.tools.len(), 2);
        assert_eq!(decoded.workflows.len(), 1);
        let workflow = decoded.workflows.get("wf").expect("workflow should exist");
        assert_eq!(workflow.name.as_deref(), Some("workflow label"));
        assert_eq!(workflow.description.as_deref(), Some("workflow description"));
    }

    /// Verifies that `save = false` round-trips through encode/decode without
    /// being coerced to default `save = true`.
    #[test]
    fn output_policy_unsaved_round_trips_through_latest_schema() {
        let document = UserNickelDocument {
            workflows: std::collections::BTreeMap::from([(
                "wf".to_string(),
                WorkflowSpec {
                    name: None,
                    description: None,
                    steps: vec![WorkflowStepSpec {
                        id: "step".to_string(),
                        tool: "echo@1.0.0".to_string(),
                        inputs: std::collections::BTreeMap::new(),
                        depends_on: Vec::new(),
                        outputs: std::collections::BTreeMap::from([(
                            "result".to_string(),
                            OutputPolicy { save: Some(OutputSaveMode::Unsaved) },
                        )]),
                    }],
                },
            )]),
            ..UserNickelDocument::default()
        };

        let encoded = encode_user_document(document).expect("encode user document");
        let decoded = decode_user_document(&encoded).expect("decode user document");
        let save = decoded
            .workflows
            .get("wf")
            .and_then(|workflow| workflow.steps.first())
            .and_then(|step| step.outputs.get("result"))
            .and_then(|policy| policy.save);

        assert_eq!(save, Some(OutputSaveMode::Unsaved));
    }

    /// Verifies legacy builtin-only extras are rejected by the strict v1 shape.
    #[test]
    fn latest_schema_rejects_legacy_builtin_extra_fields() {
        let source = r#"
    {
    version = 1,
    tools = {
        "echo@1.0.0" = {
            kind = "builtin",
            name = "echo",
            version = "1.0.0",
            is_impure = false,
        },
    },
    }
    "#;

        let err = migrate_document_source_to_version::<v_latest::NickelEnvelopeLatest>(
            source,
            latest::VERSION,
            "compatibility-shape",
        )
        .expect_err("legacy builtin extras must be rejected");
        assert!(err.to_string().contains("is_impure"));
    }

    /// Verifies encode helpers emit documents that can always deserialize into
    /// the latest Rust bridge envelope.
    #[test]
    fn encode_helpers_emit_latest_bridge_compatible_documents() {
        let user_bytes = encode_user_document(UserNickelDocument::default()).expect("encode user");
        let user_source = std::str::from_utf8(&user_bytes).expect("user bytes utf-8");
        let user_envelope: v_latest::NickelEnvelopeLatest =
            migrate_document_source_to_version(user_source, USER_NICKEL_VERSION, "conductor.ncl")
                .expect("decode encoded user envelope as latest bridge type");

        let machine_bytes =
            encode_machine_document(MachineNickelDocument::default()).expect("encode machine");
        let machine_source = std::str::from_utf8(&machine_bytes).expect("machine bytes utf-8");
        let machine_envelope: v_latest::NickelEnvelopeLatest = migrate_document_source_to_version(
            machine_source,
            USER_NICKEL_VERSION,
            "conductor.machine.ncl",
        )
        .expect("decode encoded machine envelope as latest bridge type");

        assert_eq!(user_envelope.version, latest::VERSION);
        assert_eq!(machine_envelope.version, latest::VERSION);
    }

    /// Verifies atomic migration edge declarations are symmetric and adjacent.
    #[test]
    fn atomic_migration_pairs_are_bidirectional_and_adjacent() {
        let metadata = read_migration_metadata().expect("read Nickel migration metadata");
        assert_eq!(metadata.current_version, latest::VERSION);

        for pair in &metadata.atomic_migration_pairs {
            let reverse_exists = metadata
                .atomic_migration_pairs
                .iter()
                .any(|candidate| candidate.from == pair.to && candidate.to == pair.from);
            assert!(
                reverse_exists,
                "missing reverse atomic migration edge for {} -> {}",
                pair.from, pair.to
            );

            let delta = pair.from.abs_diff(pair.to);
            assert_eq!(
                delta, 1,
                "atomic migration edge {} -> {} must be adjacent",
                pair.from, pair.to
            );
        }

        if metadata.supported_versions.len() <= 1 {
            assert!(
                metadata.atomic_migration_pairs.is_empty(),
                "single-version schema must not declare cross-version atomic migrations"
            );
        }
    }

    /// Verifies every declared atomic migration hop round-trips in both
    /// directions without document-shape drift.
    #[test]
    fn atomic_migrations_round_trip_both_directions() {
        let metadata = read_migration_metadata().expect("read Nickel migration metadata");

        if metadata.atomic_migration_pairs.is_empty() {
            assert!(
                metadata.supported_versions.len() <= 1,
                "empty atomic migration table is only valid for single-version schemas"
            );
            return;
        }

        let seed_latest = v_latest::NickelEnvelopeLatest {
            version: latest::VERSION,
            ..v_latest::NickelEnvelopeLatest::default()
        };
        let seed_latest_source = render_document_as_nickel(&seed_latest, "atomic-seed")
            .expect("render latest atomic seed envelope");
        let seed_latest_source = std::str::from_utf8(&seed_latest_source)
            .expect("rendered atomic seed envelope must be utf-8");

        for pair in &metadata.atomic_migration_pairs {
            let start: v_latest::NickelEnvelopeLatest =
                migrate_document_source_to_version(seed_latest_source, pair.from, "atomic-start")
                    .expect("materialize atomic start version from latest seed");
            let start_source = render_document_as_nickel(&start, "atomic-start")
                .expect("render atomic start envelope");
            let start_source =
                std::str::from_utf8(&start_source).expect("atomic start source must be utf-8");

            let forward: v_latest::NickelEnvelopeLatest =
                migrate_document_source_atomic(start_source, pair.from, pair.to, "atomic-forward")
                    .expect("evaluate atomic forward migration hop");

            let forward_source = render_document_as_nickel(&forward, "atomic-forward")
                .expect("render atomic forward envelope");
            let forward_source =
                std::str::from_utf8(&forward_source).expect("atomic forward source must be utf-8");

            let backward: v_latest::NickelEnvelopeLatest = migrate_document_source_atomic(
                forward_source,
                pair.to,
                pair.from,
                "atomic-backward",
            )
            .expect("evaluate atomic backward migration hop");

            assert_eq!(
                forward.version, pair.to,
                "atomic forward migration must end at requested target version"
            );
            assert_eq!(
                backward.version, pair.from,
                "atomic backward migration must end at requested source version"
            );
            assert_eq!(
                backward, start,
                "atomic migration {} -> {} -> {} must round-trip exactly",
                pair.from, pair.to, pair.from
            );
        }
    }

    /// Verifies that `conductor.machine.ncl` preserves the same full schema as
    /// `conductor.ncl`.
    #[test]
    fn decode_machine_document_accepts_full_schema_fields() {
        let source = r#"
    {
    version = 1,
    runtime = {
        conductor_dir = ".runtime",
        conductor_state_config = ".runtime/state.ncl",
        cas_store_dir = ".runtime/store",
    },
    external_data = {
        "blake3:0000000000000000000000000000000000000000000000000000000000000000" = {
            description = "fixture root",
            save = "full",
        },
    },
    tools = {
        "echo@1.0.0" = {
            kind = "builtin",
            name = "echo",
            version = "1.0.0",
        },
    },
    }
    "#;

        let decoded =
            decode_machine_document(source.as_bytes()).expect("machine document should decode");
        assert_eq!(decoded.runtime.conductor_dir.as_deref(), Some(".runtime"));
        assert_eq!(decoded.runtime.conductor_state_config.as_deref(), Some(".runtime/state.ncl"));
        assert_eq!(decoded.runtime.cas_store_dir.as_deref(), Some(".runtime/store"));
        assert_eq!(decoded.external_data.len(), 1);
        assert_eq!(
            decoded.external_data.values().next().and_then(|reference| reference.save),
            Some(OutputSaveMode::Full)
        );
        assert_eq!(decoded.tools.len(), 1);
    }

    /// Verifies legacy `runtime_storage` key spelling is rejected.
    #[test]
    fn decode_machine_document_rejects_legacy_runtime_storage_key() {
        let source = r#"
    {
    version = 1,
    runtime_storage = {
        conductor_dir = ".runtime",
    },
    }
    "#;

        let error = decode_machine_document(source.as_bytes())
            .expect_err("legacy runtime_storage key should be rejected");
        assert!(error.to_string().contains("runtime_storage"));
    }

    /// Verifies that `conductor.ncl` preserves the same full schema as
    /// `conductor.machine.ncl`.
    #[test]
    fn decode_user_document_accepts_full_schema_fields() {
        let source = r#"
    {
    version = 1,
    runtime = {
        conductor_dir = ".runtime",
        conductor_state_config = ".runtime/state.ncl",
        cas_store_dir = ".runtime/store",
    },
    external_data = {
        "blake3:0000000000000000000000000000000000000000000000000000000000000000" = {
            description = "tool content root",
            save = true,
        },
    },
    tool_configs = {
        "tool_a@1.0.0" = {
            max_concurrent_calls = -1,
            max_retries = 1,
            input_defaults = {
                "args" = ["--flag", "value"],
            },
            content_map = {
                "bin/tool" = "blake3:0000000000000000000000000000000000000000000000000000000000000000",
            },
        },
    },
    impure_timestamps = {
        wf = {
            step = {
                epoch_seconds = 123,
                subsec_nanos = 456,
            },
        },
    },
    state_pointer = "blake3:1111111111111111111111111111111111111111111111111111111111111111",
    }
    "#;

        let decoded = decode_user_document(source.as_bytes()).expect("user document should decode");
        assert_eq!(decoded.runtime.conductor_dir.as_deref(), Some(".runtime"));
        assert_eq!(decoded.runtime.conductor_state_config.as_deref(), Some(".runtime/state.ncl"));
        assert_eq!(decoded.runtime.cas_store_dir.as_deref(), Some(".runtime/store"));
        assert_eq!(decoded.external_data.len(), 1);
        assert_eq!(
            decoded.external_data.values().next().and_then(|reference| reference.save),
            Some(OutputSaveMode::Saved)
        );
        assert_eq!(decoded.tool_configs.len(), 1);
        assert!(decoded.tool_configs.get("tool_a@1.0.0").is_some_and(|config| {
            config.input_defaults.contains_key("args") && config.max_retries == 1
        }));
        assert_eq!(
            decoded.impure_timestamps.get("wf").and_then(|steps| steps.get("step")).copied(),
            Some(ImpureTimestamp { epoch_seconds: 123, subsec_nanos: 456 })
        );
        assert!(decoded.state_pointer.is_some());
    }

    /// Verifies external-data save policy rejects `false` in user documents.
    #[test]
    fn decode_user_document_rejects_external_data_unsaved_save_mode() {
        let source = r#"
    {
    version = 1,
    external_data = {
        "blake3:0000000000000000000000000000000000000000000000000000000000000000" = {
            description = "invalid unsaved external",
            save = false,
        },
    },
    }
    "#;

        let err = decode_user_document(source.as_bytes())
            .expect_err("external_data save=false should be rejected");
        let message = err.to_string();
        assert!(message.contains("save"));
    }

    /// Verifies tool-config content-map hashes must be rooted in `external_data`.
    #[test]
    fn decode_user_document_rejects_content_map_hash_missing_external_data_root() {
        let source = r#"
    {
    version = 1,
    tools = {
        "tool_a@1.0.0" = {
            kind = "executable",
            is_impure = false,
            command = ["bin/tool"],
            env_vars = {},
            success_codes = [0],
            inputs = {},
            outputs = {
                stdout = {
                    capture = {
                        kind = "stdout",
                    },
                },
            },
        },
    },
    tool_configs = {
        "tool_a@1.0.0" = {
            max_concurrent_calls = -1,
            content_map = {
                "bin/tool" = "blake3:0000000000000000000000000000000000000000000000000000000000000000",
            },
        },
    },
    }
    "#;

        let err = decode_user_document(source.as_bytes())
            .expect_err("content_map hash without external_data root should be rejected");
        assert!(err.to_string().contains("missing from external_data"));
    }

    /// Verifies tool-config retry policy rejects values smaller than `-1`.
    #[test]
    fn decode_user_document_rejects_invalid_max_retries() {
        let source = r#"
    {
    version = 1,
    tools = {
        "echo@1.0.0" = {
            kind = "builtin",
            name = "echo",
            version = "1.0.0",
        },
    },
    tool_configs = {
        "echo@1.0.0" = {
            max_retries = -2,
        },
    },
    }
    "#;

        let err =
            decode_user_document(source.as_bytes()).expect_err("invalid max_retries should fail");
        assert!(err.to_string().contains("max_retries must be -1 or a non-negative integer"));
    }

    /// Verifies the NCL `IntegerNumberV1` contract rejects fractional numbers
    /// for `max_concurrent_calls`.
    #[test]
    fn tool_config_ncl_rejects_non_integer_max_concurrent_calls() {
        let source = r#"
    {
    version = 1,
    tools = {
        "echo@1.0.0" = {
            kind = "builtin",
            name = "echo",
            version = "1.0.0",
        },
    },
    tool_configs = {
        "echo@1.0.0" = {
            max_concurrent_calls = 3.14,
        },
    },
    }
    "#;

        let err = decode_user_document(source.as_bytes())
            .expect_err("fractional max_concurrent_calls must be rejected by NCL IntegerNumberV1");
        assert!(err.to_string().contains("contract") || err.to_string().contains("integer"));
    }

    /// Verifies the NCL `IntegerNumberV1` contract rejects non-number values
    /// for `max_concurrent_calls`.
    #[test]
    fn tool_config_ncl_rejects_non_number_max_concurrent_calls() {
        let source = r#"
    {
    version = 1,
    tools = {
        "echo@1.0.0" = {
            kind = "builtin",
            name = "echo",
            version = "1.0.0",
        },
    },
    tool_configs = {
        "echo@1.0.0" = {
            max_concurrent_calls = "unlimited",
        },
    },
    }
    "#;

        let err = decode_user_document(source.as_bytes())
            .expect_err("string max_concurrent_calls must be rejected by NCL IntegerNumberV1");
        assert!(
            err.to_string().contains("contract") || err.to_string().contains("IntegerNumberV1")
        );
    }

    /// Verifies the NCL `IntegerNumberV1` contract accepts boundary integer
    /// values for `max_concurrent_calls`.
    #[test]
    fn tool_config_ncl_accepts_boundary_integer_max_concurrent_calls() {
        let source_pairs = [
            (-1, "negative boundary for max_concurrent_calls"),
            (1, "minimum positive for max_concurrent_calls"),
            (100, "positive value for max_concurrent_calls"),
        ];

        for (value, description) in &source_pairs {
            let source = format!(
                r#"
        {{
        version = 1,
        tools = {{
            "echo@1.0.0" = {{
                kind = "builtin",
                name = "echo",
                version = "1.0.0",
            }},
        }},
        tool_configs = {{
            "echo@1.0.0" = {{
                max_concurrent_calls = {value},
            }},
        }},
        }}
        "#
            );
            let decoded = decode_user_document(source.as_bytes()).unwrap_or_else(|_| {
                panic!("{description} should be accepted by NCL IntegerNumberV1")
            });
            assert_eq!(
                decoded.tool_configs.get("echo@1.0.0").unwrap().max_concurrent_calls,
                *value,
                "{description}",
            );
        }
    }

    /// Verifies workflow-step string bindings accept `${external_data.<hash>}`.
    #[test]
    fn decode_user_document_accepts_external_data_input_binding() {
        let source = r#"
    {
    version = 1,
    tools = {
        "echo@1.0.0" = {
            kind = "builtin",
            name = "echo",
            version = "1.0.0",
        },
    },
    workflows = {
        wf = {
            steps = [
                {
                    id = "step-1",
                    tool = "echo@1.0.0",
                    inputs = {
                        path = "${external_data.blake3:0000000000000000000000000000000000000000000000000000000000000000}",
                    },
                },
            ],
        },
    },
    }
    "#;

        let decoded = decode_user_document(source.as_bytes()).expect("user document should decode");
        let step = &decoded.workflows["wf"].steps[0];
        assert_eq!(
            step.inputs.get("path"),
            Some(&InputBinding::String(
                "${external_data.blake3:0000000000000000000000000000000000000000000000000000000000000000}"
                    .to_string()
            ))
        );
    }

    /// Verifies optional workflow metadata fields survive user-document decode.
    #[test]
    fn decode_user_document_preserves_workflow_metadata_fields() {
        let source = r#"
    {
    version = 1,
    tools = {
        "echo@1.0.0" = {
            kind = "builtin",
            name = "echo",
            version = "1.0.0",
        },
    },
    workflows = {
        wf = {
            name = "friendly workflow",
            description = "informational metadata",
            steps = [
                {
                    id = "step-1",
                    tool = "echo@1.0.0",
                    inputs = {
                        text = "hello",
                    },
                },
            ],
        },
    },
    }
    "#;

        let decoded = decode_user_document(source.as_bytes()).expect("user document should decode");
        let workflow = decoded.workflows.get("wf").expect("workflow should exist");
        assert_eq!(workflow.name.as_deref(), Some("friendly workflow"));
        assert_eq!(workflow.description.as_deref(), Some("informational metadata"));
    }

    /// Verifies workflow-step input bindings support mixed literal +
    /// interpolation segments.
    #[test]
    fn decode_user_document_accepts_interpolated_input_binding_segments() {
        let source = r#"
    {
    version = 1,
    tools = {
        "echo@1.0.0" = {
            kind = "builtin",
            name = "echo",
            version = "1.0.0",
        },
    },
    workflows = {
        wf = {
            steps = [
                {
                    id = "step-1",
                    tool = "echo@1.0.0",
                    inputs = {
                        path = "prefix-${external_data.blake3:0000000000000000000000000000000000000000000000000000000000000000}/artifact.txt",
                    },
                },
            ],
        },
    },
    }
    "#;

        let decoded = decode_user_document(source.as_bytes()).expect("user document should decode");
        let step = &decoded.workflows["wf"].steps[0];
        assert_eq!(
            step.inputs.get("path"),
            Some(&InputBinding::String(
                "prefix-${external_data.blake3:0000000000000000000000000000000000000000000000000000000000000000}/artifact.txt"
                    .to_string()
            ))
        );
    }

    /// Verifies executable input declarations default to scalar `string` kind.
    #[test]
    fn decode_user_document_defaults_input_kind_to_string() {
        let source = r#"
    {
    version = 1,
    tools = {
        "tool_exec@1.0.0" = {
            kind = "executable",
            command = ["bin/tool"],
            inputs = {
                text = {},
            },
            outputs = {
                out = { capture = { kind = "stdout" } },
            },
        },
    },
    workflows = {
        wf = {
            steps = [
                {
                    id = "step-1",
                    tool = "tool_exec@1.0.0",
                    inputs = { text = "hello" },
                },
            ],
        },
    },
    }
    "#;

        let decoded = decode_user_document(source.as_bytes()).expect("user document should decode");
        let tool = decoded.tools.get("tool_exec@1.0.0").expect("tool should exist");
        let text_input = tool.inputs.get("text").expect("input should exist");
        assert_eq!(text_input.kind, ToolInputKind::String);
    }

    /// Verifies tool-level executable input defaults are rejected and callers
    /// must use `tool_configs.<tool>.input_defaults` instead.
    #[test]
    fn decode_user_document_rejects_tool_level_input_default_field() {
        let source = r#"
    {
    version = 1,
    tools = {
        "tool_exec@1.0.0" = {
            kind = "executable",
            command = ["bin/tool"],
            inputs = {
                text = {
                    default = "fallback",
                },
            },
            outputs = {
                out = { capture = { kind = "stdout" } },
            },
        },
    },
    workflows = {
        wf = {
            steps = [
                {
                    id = "step-1",
                    tool = "tool_exec@1.0.0",
                    inputs = { text = "hello" },
                },
            ],
        },
    },
    }
    "#;

        let err = decode_user_document(source.as_bytes())
            .expect_err("tool-level input defaults should be rejected");
        assert!(err.to_string().contains("default"));
    }

    /// Verifies executable input declarations support explicit `string_list`
    /// kind and workflow steps can provide list-valued bindings.
    #[test]
    fn decode_user_document_accepts_string_list_input_declaration_and_binding() {
        let source = r#"
    {
    version = 1,
    tools = {
        "tool_exec@1.0.0" = {
            kind = "executable",
            command = ["bin/tool", "${*inputs.args}"],
            inputs = {
                args = { kind = "string_list" },
            },
            outputs = {
                out = { capture = { kind = "stdout" } },
            },
        },
    },
    workflows = {
        wf = {
            steps = [
                {
                    id = "step-1",
                    tool = "tool_exec@1.0.0",
                    inputs = {
                        args = ["--one", "--two"],
                    },
                },
            ],
        },
    },
    }
    "#;

        let decoded = decode_user_document(source.as_bytes()).expect("user document should decode");
        let tool = decoded.tools.get("tool_exec@1.0.0").expect("tool should exist");
        let args_input = tool.inputs.get("args").expect("args input should exist");
        assert_eq!(args_input.kind, ToolInputKind::StringList);

        let step = &decoded.workflows["wf"].steps[0];
        assert_eq!(
            step.inputs.get("args"),
            Some(&InputBinding::StringList(vec!["--one".to_string(), "--two".to_string()]))
        );
    }

    /// Verifies executable step input values must match the declared input
    /// kind.
    #[test]
    fn decode_user_document_rejects_executable_step_input_kind_mismatch() {
        let source = r#"
    {
    version = 1,
    tools = {
        "tool_exec@1.0.0" = {
            kind = "executable",
            command = ["bin/tool", "${*inputs.args}"],
            inputs = {
                args = { kind = "string_list" },
            },
            outputs = {
                out = { capture = { kind = "stdout" } },
            },
        },
    },
    workflows = {
        wf = {
            steps = [
                {
                    id = "step-1",
                    tool = "tool_exec@1.0.0",
                    inputs = {
                        args = "--not-a-list",
                    },
                },
            ],
        },
    },
    }
    "#;

        let err = decode_user_document(source.as_bytes())
            .expect_err("mismatched executable input kind should be rejected");
        let message = err.to_string();
        assert!(message.contains("expects kind 'string_list'"));
        assert!(message.contains("received 'string'"));
    }

    /// Verifies unsupported `${...}` workflow-step input expressions fail fast.
    #[test]
    fn decode_user_document_rejects_unsupported_input_binding_expression() {
        let source = r#"
    {
    version = 1,
    tools = {
        "echo@1.0.0" = {
            kind = "builtin",
            name = "echo",
            version = "1.0.0",
        },
    },
    workflows = {
        wf = {
            steps = [
                {
                    id = "step-1",
                    tool = "echo@1.0.0",
                    inputs = {
                        bad = "${unknown.binding}",
                    },
                },
            ],
        },
    },
    }
    "#;

        let err = decode_user_document(source.as_bytes())
            .expect_err("unsupported input binding expression should be rejected");
        assert!(err.to_string().contains("unsupported input binding expression"));
    }

    /// Verifies workflow-step input bindings reject materialization directives.
    #[test]
    fn decode_user_document_rejects_materialization_directive_in_input_binding_expression() {
        let source = r#"
    {
    version = 1,
    tools = {
        "echo@1.0.0" = {
            kind = "builtin",
            name = "echo",
            version = "1.0.0",
        },
    },
    workflows = {
        wf = {
            steps = [
                {
                    id = "step-1",
                    tool = "echo@1.0.0",
                    inputs = {
                        bad = "${step_output.seed.result:file(out.txt)}",
                    },
                },
            ],
        },
    },
    }
    "#;

        let err = decode_user_document(source.as_bytes())
            .expect_err("materialization directive in input binding should be rejected");
        let message = err.to_string();
        assert!(message.contains(":file(...)"));
        assert!(message.contains(":folder(...)"));
    }

    /// Verifies output-capture kind variants reject undeclared fields.
    #[test]
    fn latest_schema_rejects_output_capture_extra_fields() {
        let source = r#"
    {
    version = 1,
    tools = {
        "tool_exec@1.0.0" = {
            kind = "executable",
            is_impure = false,
            command = ["bin/tool"],
            env_vars = {},
            success_codes = [0],
            inputs = {},
            outputs = {
                out = {
                    capture = {
                        kind = "stdout",
                        path = "unexpected.txt",
                    },
                },
            },
        },
    },
    }
    "#;

        let err = migrate_document_source_to_version::<v_latest::NickelEnvelopeLatest>(
            source,
            latest::VERSION,
            "compatibility-shape",
        )
        .expect_err("output-capture extra field must be rejected");
        assert!(err.to_string().contains("path"));
    }

    /// Verifies impure timestamp nanosecond components stay within one second.
    #[test]
    fn decode_user_document_rejects_out_of_range_subsec_nanos() {
        let source = r"
    {
    version = 1,
    impure_timestamps = {
        wf = {
            step = {
                epoch_seconds = 123,
                subsec_nanos = 1000000000,
            },
        },
    },
    }
    ";

        let err = decode_user_document(source.as_bytes()).expect_err(
            "user document should reject impure timestamp subsec_nanos >= 1_000_000_000",
        );
        assert!(err.to_string().contains("subsec_nanos must be in range 0..999999999"));
    }

    /// Verifies state documents reject non-volatile top-level fields even when
    /// those fields are empty maps.
    #[test]
    fn decode_state_document_rejects_non_volatile_top_level_fields() {
        let source = r"
    {
    version = 1,
    impure_timestamps = {},
    state_pointer = null,
    tools = {},
    }
    ";

        let err = super::decode_state_document(source.as_bytes())
            .expect_err("state document with non-volatile fields should fail");
        assert!(
            err.to_string()
                .contains("may only define version, impure_timestamps, and state_pointer")
        );
    }

    /// Verifies encoded state documents emit only volatile keys plus explicit
    /// version marker.
    #[test]
    fn encode_state_document_emits_only_volatile_keys() {
        let encoded =
            super::encode_state_document(crate::model::config::StateNickelDocument::default())
                .expect("state encode should succeed");
        let rendered = std::str::from_utf8(&encoded).expect("state source must be utf-8");

        assert!(rendered.contains("version"));
        assert!(rendered.contains("impure_timestamps"));
        assert!(rendered.contains("state_pointer"));
        assert!(!rendered.contains("external_data"));
        assert!(!rendered.contains("tools"));
        assert!(!rendered.contains("workflows"));
        assert!(!rendered.contains("tool_configs"));
    }

    /// Verifies full configuration evaluation requires explicit top-level
    /// `version` markers in all three configuration documents.
    #[test]
    fn evaluate_total_configuration_sources_rejects_missing_version_marker() {
        let user = r"{ version = 1, workflows = {} }";
        let machine = r"{ version = 1, tools = {} }";
        let state = r"{ impure_timestamps = {}, state_pointer = null }";

        let err = super::evaluate_total_configuration_sources(user, machine, state)
            .expect_err("missing state version marker should fail");
        assert!(err.to_string().contains("top-level numeric 'version' field"));
    }
}
