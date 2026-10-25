//! Strictness tests for the mediapm Nickel schema surface (S-C1..S-C10) and
//! the Rust serde config surface (S-E1..S-E4).
//!
//! These tests evaluate the embedded `v1.ncl` / `mod.ncl` contracts through
//! the same `nickel-lang-core` evaluator that the production bridge uses, so
//! they run in CI without a Nickel CLI.  Each rejection test asserts that a
//! document which the *loose* pre-strictness schema accepted is now rejected.

use serde_json::{Value, json};

use mediapm::{
    MediaPmDocument, MediaRuntimeStorage, MediaStep, OutputVariantValue, ToolRegistryEntry,
    VerifyStrategy, apply_v1_contract, apply_v2_contract, evaluate_mod_ncl_expression,
    validate_v1_document, validate_v2_document,
};

/// Realistic v1 document exercising every closed contract surface.
const REALISTIC_V1_DOC: &str = r#"
{
  version = 1,
  media = {
    "m1" = {
      title = "Hello, World",
      steps = [
        { tool = "yt-dlp", output_variants = { primary = { kind = "primary" } } },
        { tool = "ffmpeg" },
      ],
    },
  },
  hierarchy = [
    { path = "Season 1", kind = "folder", children = [ { path = "ep1", kind = "media" } ] },
  ],
  tools = {
    ffmpeg = { version_spec = { tag = "v6" }, recheck_seconds = 3600 },
    rsgain = { version_spec = "inherit" },
  },
  runtime = {
    mediapm_dir = "/tmp/mp",
    materialization_preference_order = ["hardlink", "symlink"],
    verify_on_read = ["always"],
    instance_ttl_seconds = 604800,
    profiler_enabled = true,
    inherited_env_vars = { windows = ["PATH"], linux = ["PATH"] },
  },
}
"#;

fn expect_rejected(contract: &str, source: &str) {
    let result = apply_v1_contract(contract, source);
    assert!(
        result.is_err(),
        "expected contract '{contract}' to reject source, but it validated successfully: {result:?}"
    );
}

fn expect_rejected_v2(contract: &str, source: &str) {
    let result = apply_v2_contract(contract, source);
    assert!(
        result.is_err(),
        "expected v2 contract '{contract}' to reject source, but it validated successfully: {result:?}"
    );
}

/// Realistic v2 document: same shape as `REALISTIC_V1_DOC` but version 2 and
/// no `state` field (state is managed via `state.json`, not the v2 config
/// surface).
const REALISTIC_V2_DOC: &str = r#"
{
  version = 2,
  media = {
    "m1" = {
      title = "Hello, World",
      steps = [
        { tool = "yt-dlp", output_variants = { primary = { kind = "primary" } } },
        { tool = "ffmpeg" },
      ],
    },
  },
  hierarchy = [
    { path = "Season 1", kind = "folder", children = [ { path = "ep1", kind = "media" } ] },
  ],
  tools = {
    ffmpeg = { version_spec = { tag = "v6" }, recheck_seconds = 3600 },
    rsgain = { version_spec = "inherit" },
  },
  runtime = {
    paths = { mediapm_dir = "/tmp/mp" },
    materialization = { materialization_preference_order = ["hardlink", "symlink"] },
    verification = { verify_on_read = ["always"] },
    lifecycle = { instance_ttl_seconds = 604800 },
    environment = { profiler_enabled = true, inherited_env_vars = { windows = ["PATH"], linux = ["PATH"] } },
  },
}
"#;

// ---------------------------------------------------------------------------
// S-C1: `MediaSourceSpecV1`
// ---------------------------------------------------------------------------

/// S-C1: `media.<id>` entries are closed `MediaSourceSpecV1` records — unknown
/// fields must be rejected.
#[test]
fn strict_media_contract_rejects_unknown_media_field() {
    expect_rejected("validate_document_v1", r"{ version = 1, media = { m1 = { bogus = 1 } } }");
}

/// S-C1: step `tool` is a closed string enum — unknown tool names must be
/// rejected.
#[test]
fn strict_media_step_rejects_unknown_tool() {
    expect_rejected(
        "validate_document_v1",
        r#"{ version = 1, media = { m1 = { steps = [ { tool = "wget" } ] } } }"#,
    );
}

// ---------------------------------------------------------------------------
// S-C2: `HierarchyNodeV1`
// ---------------------------------------------------------------------------

/// S-C2: hierarchy node `kind` is a closed string enum — unknown kinds must
/// be rejected.
#[test]
fn strict_hierarchy_rejects_unknown_kind() {
    expect_rejected(
        "validate_document_v1",
        r#"{ version = 1, hierarchy = [ { kind = "video" } ] }"#,
    );
}

/// S-C2: hierarchy children are typed `HierarchyNodeV1` records (no `Dyn`) —
/// free-form child records must be rejected.
#[test]
fn strict_hierarchy_rejects_free_form_child() {
    expect_rejected(
        "validate_document_v1",
        r#"{ version = 1, hierarchy = [ { path = "a", bogus = true } ] }"#,
    );
}

/// S-C2: hierarchy node `format` is a closed string enum.
#[test]
fn strict_hierarchy_rejects_unknown_format() {
    expect_rejected(
        "validate_document_v1",
        r#"{ version = 1, hierarchy = [ { path = "a", format = "mp4" } ] }"#,
    );
}

// ---------------------------------------------------------------------------
// S-C3: `MediaRuntimeStorageV1`
// ---------------------------------------------------------------------------

/// S-C3: `runtime` is a closed record — unknown fields must be rejected.
#[test]
fn strict_runtime_rejects_unknown_field() {
    expect_rejected("validate_document_v1", r"{ version = 1, runtime = { bogus = 1 } }");
}

/// S-C3: integral runtime fields carry `IntegerNumberV1` — fractional values
/// must be rejected.
#[test]
fn strict_runtime_rejects_fractional_ttl() {
    expect_rejected(
        "validate_document_v1",
        r"{ version = 1, runtime = { instance_ttl_seconds = 2.5 } }",
    );
}

/// S-C3: runtime enum arrays carry closed string enums — unknown
/// materialization methods must be rejected.
#[test]
fn strict_runtime_rejects_unknown_materialization_method() {
    expect_rejected(
        "validate_document_v1",
        r#"{ version = 1, runtime = { materialization_preference_order = ["write"] } }"#,
    );
}

// ---------------------------------------------------------------------------
// S-C4: `ToolRequirementV1`
// ---------------------------------------------------------------------------

/// S-C4: tool requirement numeric fields carry `IntegerNumberV1` — fractional
/// recheck intervals must be rejected.
#[test]
fn strict_tool_requirement_rejects_fractional_recheck() {
    expect_rejected(
        "validate_document_v1",
        r"{ version = 1, tools = { ffmpeg = { recheck_seconds = 1.5 } } }",
    );
}

/// S-C4: `ToolRequirementV1` is closed — unknown fields must be rejected.
#[test]
fn strict_tool_requirement_rejects_unknown_field() {
    expect_rejected("validate_document_v1", r"{ version = 1, tools = { ffmpeg = { bogus = 1 } } }");
}

// ---------------------------------------------------------------------------
// S-C5: version specs
// ---------------------------------------------------------------------------

/// S-C5: exact version specs are closed records — extra fields must be
/// rejected.
#[test]
fn strict_version_spec_rejects_extra_field() {
    expect_rejected(
        "validate_document_v1",
        r#"{ version = 1, tools = { ffmpeg = { version_spec = { vcs_hash = "abc", bogus = 1 } } } }"#,
    );
}

/// S-C5: version spec strings are closed enums — unknown literals must be
/// rejected.
#[test]
fn strict_version_spec_rejects_unknown_literal() {
    expect_rejected(
        "validate_document_v1",
        r#"{ version = 1, tools = { ffmpeg = { version_spec = "inherits" } } }"#,
    );
}

// ---------------------------------------------------------------------------
// S-C6: `MediaPmStateV1`
// ---------------------------------------------------------------------------

/// S-C6: `MediaPmStateV1` is a closed record — unknown fields must be
/// rejected.
#[test]
fn strict_state_rejects_unknown_field() {
    expect_rejected(
        "MediaPmStateV1",
        r#"{ version = 3, managed_files = { "a" = { media_id = "x", variant = "v", hash = "h" } }, bogus = 2 }"#,
    );
}

// ---------------------------------------------------------------------------
// S-C8: `mod.ncl` registry surface
// ---------------------------------------------------------------------------

/// S-C8: `mod.ncl`'s `VersionContract` carries an integer guard — fractional
/// version markers must be rejected while integers are accepted.
#[test]
fn strict_mod_ncl_version_contract_rejects_float() {
    let result = evaluate_mod_ncl_expression("(1.5 | shared.VersionContract)");
    assert!(
        result.is_err(),
        "VersionContract must reject fractional version markers, got {result:?}"
    );

    let result = evaluate_mod_ncl_expression("(1 | shared.VersionContract)");
    assert!(result.is_ok(), "VersionContract must accept integral version markers, got {result:?}");
    assert_eq!(result.unwrap().as_f64(), Some(1.0));
}

/// S-C8: `mod.ncl` exports the registry surface and unversioned aliases.
#[test]
fn parity_mod_ncl_exports_registry_surface() {
    let mod_source = include_str!("../../src/config/versions/mod.ncl");
    assert!(mod_source.contains("current_version = "), "mod.ncl must export current_version");
    assert!(
        mod_source.contains("supported_versions = [1, 2]"),
        "mod.ncl must export supported_versions = [1, 2]"
    );
    assert!(mod_source.contains("migrate_to = migrate_to_fn"), "mod.ncl must export migrate_to");
    assert!(
        mod_source.contains("SupportedVersion = SupportedVersion_pred"),
        "mod.ncl must export SupportedVersion"
    );
    assert!(
        mod_source.contains("validate_document = v2_migration.validate_document_v2"),
        "mod.ncl must re-export validate_document (unversioned alias)"
    );
    assert!(
        mod_source.contains("envelope_contract = v2_migration.envelope_contract_v2"),
        "mod.ncl must re-export envelope_contract (unversioned alias)"
    );
    assert!(
        mod_source.contains("std.number.is_integer"),
        "mod.ncl VersionContract must carry an integer guard"
    );
}

// ---------------------------------------------------------------------------
// S-C7: `v1.ncl` export surface
// ---------------------------------------------------------------------------

/// S-C7: `v1.ncl` exports the version-locked validator and envelope contract.
#[test]
fn parity_v1_ncl_exports_validator_and_envelope() {
    let schema = include_str!("../../src/config/versions/v1.ncl");
    assert!(
        schema.contains("validate_document_v1 = fun document => document | _media_pm_document_v1"),
        "v1.ncl must export validate_document_v1"
    );
    assert!(
        schema.contains("envelope_contract_v1 = _media_pm_document_v1"),
        "v1.ncl must export envelope_contract_v1"
    );
}

// ---------------------------------------------------------------------------
// V2 export surface (strict version separation policy guard)
// ---------------------------------------------------------------------------

/// The v2 module exports the version-locked validator and envelope contract.
#[test]
fn parity_v2_ncl_exports_validator_and_envelope() {
    let schema = include_str!("../../src/config/versions/v2.ncl");
    assert!(
        schema.contains("validate_document_v2 = fun document => document | _media_pm_document_v2"),
        "v2.ncl must export validate_document_v2"
    );
    assert!(
        schema.contains("envelope_contract_v2 = _media_pm_document_v2"),
        "v2.ncl must export envelope_contract_v2"
    );
}

/// STRICT VERSION SEPARATION: the v2 export surface must not define any
/// `*V1` contract names — every exported contract is `*V2`-suffixed or one
/// of the unversioned entry points (`validate_document_v2`,
/// `envelope_contract_v2`, `migrate_v1_to_v2`).
#[test]
fn strict_v2_no_v1_names() {
    let schema = include_str!("../../src/config/versions/v2.ncl");
    for v1_name in [
        "MediaPmDocumentV1",
        "MediaSourceSpecV1",
        "MediaStepSpecV1",
        "HierarchyNodeV1",
        "MediaRuntimeStorageV1",
        "ToolRequirementV1",
        "ConfigVersionSpecV1",
        "VersionSpecV1",
        "OutputVariantValueV1",
        "SanitizeNamesConfigV1",
    ] {
        assert!(
            !schema.contains(v1_name),
            "v2.ncl must not define or reference {v1_name} (strict version separation)"
        );
    }
    // A contract name that only exists in v1 must not resolve in the v2
    // module (missing export -> evaluation error).
    expect_rejected_v2(
        "MediaRuntimeStorageV1",
        r#"{ instance_ttl_seconds = 3600, mediapm_dir = "/tmp/mp" }"#,
    );
}

/// The v2 module must not expose any `MediaPmState` contract (V1 or V2):
/// state is managed separately via `state.json` and is not part of the v2
/// config surface.
#[test]
fn strict_v2_media_pm_state_not_exported() {
    let schema = include_str!("../../src/config/versions/v2.ncl");
    assert!(
        !schema.contains("MediaPmStateV"),
        "v2.ncl must not define or reference any MediaPmStateV1/V2 contract"
    );
    assert!(!schema.contains("state |"), "v2.ncl must not apply any state contract");
    assert!(
        !schema.contains("ManagedFileRecord"),
        "v2.ncl must not define or reference state-domain ManagedFileRecord"
    );
    assert!(
        !schema.contains("ManagedWorkflowStepState"),
        "v2.ncl must not define or reference state-domain ManagedWorkflowStepState"
    );
    assert!(
        !schema.contains("ToolRegistryEntry"),
        "v2.ncl must not define or reference state-domain ToolRegistryEntry"
    );
    // Behavioral check: a MediaPmStateV2 contract must not resolve.
    expect_rejected_v2("MediaPmStateV2", r"{ version = 3, managed_files = {} }");
}

/// R1: the closed v2 envelope rejects a `state` field — the legacy state
/// payload is dropped from the v2 config surface.
#[test]
fn strict_v2_state_field_rejected() {
    expect_rejected_v2(
        "validate_document_v2",
        r"{ version = 2, state = { version = 3, managed_files = {} } }",
    );
}

/// R1: the closed v2 envelope rejects unknown top-level fields.
#[test]
fn strict_v2_rejects_unknown_top_level_field() {
    expect_rejected_v2("validate_document_v2", r"{ version = 2, bogus = 1 }");
}

// ---------------------------------------------------------------------------
// Parity: the strict surface must not over-reject
// ---------------------------------------------------------------------------

/// R2: the v1 envelope still accepts its optional legacy `state` field with a
/// valid `MediaPmStateV1` payload.
#[test]
fn parity_v1_ncl_accepts_state_field() {
    let result = validate_v1_document(
        r#"{
      version = 1,
      state = {
        version = 3,
        managed_files = {
          "a" = { media_id = "x", variant = "v", hash = "h" },
        },
      },
      media = {},
      hierarchy = [],
      tools = {},
      runtime = {},
    }"#,
    );
    let value = result.expect("v1 document with state field must validate");
    let obj = value.as_object().expect("validated document must be an object");
    let state = obj["state"].as_object().expect("state must be an object");
    assert_eq!(state["version"].as_f64(), Some(3.0));
    assert_eq!(state["managed_files"]["a"]["media_id"], "x");
}

/// A realistic v2 document (same shape as the v1 realistic doc minus the
/// state payload) validates cleanly against the v2 envelope.
#[test]
fn parity_v2_ncl_evaluates_cleanly() {
    let result = validate_v2_document(REALISTIC_V2_DOC);
    let value = result.expect("realistic v2 document must validate");
    let obj = value.as_object().expect("validated document must be an object");
    assert_eq!(obj["version"].as_f64(), Some(2.0));
    assert!(!obj.contains_key("state"), "validated v2 document must not carry a state field");
    assert_eq!(
        obj["media"].as_object().expect("media must be an object")["m1"]["title"],
        "Hello, World"
    );
    let hierarchy = obj["hierarchy"].as_array().expect("hierarchy must be an array");
    assert_eq!(hierarchy[0]["kind"], "folder");
    assert_eq!(hierarchy[0]["children"][0]["kind"], "media");
    assert_eq!(
        obj["runtime"]["materialization"]["materialization_preference_order"][0],
        "hardlink"
    );
}

// ---------------------------------------------------------------------------
// R3: migration round-trips through the mod.ncl dispatch
// ---------------------------------------------------------------------------

/// R3: a stateful v1 document migrates to v2 through `mod.ncl`'s
/// `migrate_to` dispatch: the legacy `state` payload is stripped, the version
/// marker becomes 2, and all other fields are preserved.  The migrated output
/// decodes into the strict Rust `MediaPmDocument` model (`deny_unknown_fields`).
#[test]
fn parity_v1_to_v2_migration_strips_state() {
    let value = evaluate_mod_ncl_expression(
        r#"(shared.migrate_to 2 {
      version = 1,
      media = { m1 = { title = "Hello", steps = [ { tool = "ffmpeg" } ] } },
      hierarchy = [],
      tools = {},
      runtime = {},
      state = { version = 3, managed_files = {} },
    })"#,
    )
    .expect("v1 -> v2 migration must evaluate");
    let obj = value.as_object().expect("migrated document must be an object");
    assert_eq!(obj["version"].as_f64(), Some(2.0));
    assert!(
        !obj.contains_key("state"),
        "migrated v2 document must not carry the legacy state field"
    );
    assert_eq!(obj["media"].as_object().expect("media must be an object")["m1"]["title"], "Hello");
    let doc: MediaPmDocument = serde_json::from_value(value)
        .expect("migrated v2 output must decode into the strict MediaPmDocument model");
    assert_eq!(doc.version, 2);
}

/// R3: a stateless v2 document migrates back to v1 with the version marker
/// dropped to 1 and no state injected.
#[test]
fn parity_v2_to_v1_migration_bumps_version() {
    let value = evaluate_mod_ncl_expression(
        r#"(shared.migrate_to 1 {
      version = 2,
      media = { m1 = { title = "Hello", steps = [ { tool = "ffmpeg" } ] } },
      hierarchy = [],
      tools = {},
      runtime = {},
    })"#,
    )
    .expect("v2 -> v1 migration must evaluate");
    let obj = value.as_object().expect("migrated document must be an object");
    assert_eq!(obj["version"].as_f64(), Some(1.0));
    assert!(!obj.contains_key("state"), "migrated v1 document must not inject a state field");
    assert_eq!(obj["media"].as_object().expect("media must be an object")["m1"]["title"], "Hello");
    let doc: MediaPmDocument = serde_json::from_value(value)
        .expect("migrated v1 output must decode into the strict MediaPmDocument model");
    assert_eq!(doc.version, 1);
}

/// S-C1..S-C3: a realistic document exercising the full closed surface still
/// validates cleanly (no accidental over-rejection).
#[test]
fn parity_v1_ncl_evaluates_cleanly() {
    let result = validate_v1_document(REALISTIC_V1_DOC);
    let value = result.expect("realistic v1 document must validate");
    let obj = value.as_object().expect("validated document must be an object");
    assert_eq!(obj["version"].as_f64(), Some(1.0));
    assert_eq!(
        obj["media"].as_object().expect("media must be an object")["m1"]["title"],
        "Hello, World"
    );
    let hierarchy = obj["hierarchy"].as_array().expect("hierarchy must be an array");
    assert_eq!(hierarchy[0]["kind"], "folder");
    assert_eq!(hierarchy[0]["children"][0]["kind"], "media");
    assert_eq!(obj["runtime"]["materialization_preference_order"][0], "hardlink");
}

/// Regression: the pre-strictness loose `media` contract accepted arbitrary
/// extra fields; it must now fail loudly.
#[test]
fn regression_v1_ncl_loose_media_no_longer_accepted() {
    expect_rejected(
        "validate_document_v1",
        r#"{ version = 1, media = { m1 = { title = "x", extra = true } } }"#,
    );
}

/// Regression: the pre-strictness open `runtime` record (`{ .. }`) silently
/// accepted unknown fields; it must now fail loudly.
#[test]
fn regression_runtime_open_record_no_longer_accepted() {
    expect_rejected(
        "validate_document_v1",
        r#"{ version = 1, runtime = { instance_ttl_seconds = 3600, mystery = "x" } }"#,
    );
}

// ---------------------------------------------------------------------------
// S-C9: the export directory holds the contracts the runtime evaluates
// ---------------------------------------------------------------------------

/// Every file `export_config_contracts` writes is byte-identical to the Nickel
/// source the runtime embeds, for both the mediapm registry and the
/// conductor's.
///
/// The guarantee is that a reader of `.mediapm/config/` sees the contract a
/// document was actually validated against. A regenerated, hand-edited, or
/// separately authored schema would fail here even if it described the same
/// rules, because the export's only job is to publish these exact bytes.
#[test]
fn strict_exported_contracts_are_the_sources_the_runtime_evaluates() {
    let dir = mediapm_utils::temp::artifact_dir().expect("create temp dir");
    let export_dir = dir.path().join("mediapm");
    let conductor_dir = dir.path().join("conductor");
    mediapm::export_config_contracts(Some(&export_dir), &conductor_dir).expect("export contracts");

    let mediapm_expected = [
        ("mod.ncl", include_str!("../../src/config/versions/mod.ncl")),
        ("v1.ncl", include_str!("../../src/config/versions/v1.ncl")),
        ("v2.ncl", include_str!("../../src/config/versions/v2.ncl")),
    ];
    for (name, source) in mediapm_expected {
        let written = std::fs::read_to_string(export_dir.join(name))
            .unwrap_or_else(|error| panic!("read exported {name}: {error}"));
        assert_eq!(written, source, "exported mediapm {name} must be the evaluated source");
        assert!(
            source.contains('{'),
            "the embedded {name} must be a Nickel module, not an empty file"
        );
    }

    let conductor_expected = [
        ("mod.ncl", include_str!("../../../mediapm-conductor/src/config/versions/mod.ncl")),
        ("v1.ncl", include_str!("../../../mediapm-conductor/src/config/versions/v1.ncl")),
        ("v2.ncl", include_str!("../../../mediapm-conductor/src/config/versions/v2.ncl")),
        ("v3.ncl", include_str!("../../../mediapm-conductor/src/config/versions/v3.ncl")),
    ];
    for (name, source) in conductor_expected {
        let written = std::fs::read_to_string(conductor_dir.join(name))
            .unwrap_or_else(|error| panic!("read exported {name}: {error}"));
        assert_eq!(written, source, "exported conductor {name} must be the evaluated source");
        assert!(
            source.contains('{'),
            "the embedded {name} must be a Nickel module, not an empty file"
        );
    }

    // The exported registry imports what sits beside it, so the set is
    // evaluable on its own only when every version file came along.
    let registry = std::fs::read_to_string(conductor_dir.join("mod.ncl")).expect("read mod.ncl");
    for version in ["v1.ncl", "v2.ncl", "v3.ncl"] {
        assert!(
            registry.contains(&format!("\"{version}\"")),
            "conductor mod.ncl must import {version}, so the export must carry it"
        );
    }
}

/// Nothing in the export directory is a JSON schema: the export publishes
/// Nickel contract sources and no second, hand-maintained description of the
/// same document survives beside them.
#[test]
fn regression_no_json_schema_is_exported() {
    let dir = mediapm_utils::temp::artifact_dir().expect("create temp dir");
    let export_dir = dir.path().join("mediapm");
    let conductor_dir = dir.path().join("conductor");
    mediapm::export_config_contracts(Some(&export_dir), &conductor_dir).expect("export contracts");

    for directory in [&export_dir, &conductor_dir] {
        let entries = std::fs::read_dir(directory).expect("read export dir");
        for entry in entries {
            let path = entry.expect("export dir entry").path();
            let name = path
                .file_name()
                .and_then(std::ffi::OsStr::to_str)
                .expect("exported file name is UTF-8")
                .to_string();
            let extension = path.extension().and_then(std::ffi::OsStr::to_str);
            assert!(
                extension.is_some_and(|extension| extension.eq_ignore_ascii_case("ncl")),
                "every exported contract is a Nickel source, found {name}"
            );
            assert!(
                !name.contains(".json"),
                "export must publish Nickel contract sources, found {name}"
            );
        }
    }
}

// ---------------------------------------------------------------------------
// S-E1..S-E4: Rust serde strictness for mediapm config types
// ---------------------------------------------------------------------------

/// Asserts that decoding `value` as `T` fails and that the error message
/// contains `needle`.
fn serde_reject<T>(value: Value, needle: &str)
where
    T: serde::de::DeserializeOwned + std::fmt::Debug,
{
    let err = serde_json::from_value::<T>(value).expect_err("serde decode must be rejected");
    let msg = format!("{err}");
    assert!(msg.contains(needle), "error must contain '{needle}': {msg}");
}

/// S-E1: `ToolRegistryEntry` must reject unknown fields (was silently accepted
/// before the strictness overhaul).
#[test]
fn strict_tool_registry_entry_rejects_unknown_field() {
    serde_reject::<ToolRegistryEntry>(
        json!({
            "tool_id": "ffmpeg@v1",
            "version": "1.0",
            "bogus_field": 1,
        }),
        "bogus_field",
    );
}

/// S-E1: `MediaRuntimeStorage` must reject unknown fields (was silently
/// accepted before the strictness overhaul).
#[test]
fn strict_media_runtime_rejects_unknown_field() {
    serde_reject::<MediaRuntimeStorage>(
        json!({
            "verification": { "verify_on_read": ["modified"] },
            "bogus_field": 1,
        }),
        "bogus_field",
    );
}

/// S-E2: free-form `output_variants` values must be rejected.  Before the
/// strictness overhaul the variant map was `BTreeMap<String, Value>` and any
/// arbitrary object was accepted.
#[test]
fn strict_output_variants_rejects_free_form_value() {
    let err = serde_json::from_value::<MediaStep>(json!({
        "tool": "import",
        "output_variants": {
            "media": { "arbitrary": 1 },
        },
    }))
    .expect_err("free-form output variant value must be rejected");
    let msg = format!("{err}");
    assert!(
        msg.contains("untagged") || msg.contains("variant"),
        "error must describe the failed variant decode: {msg}"
    );
}

/// S-E3: `verify_on_read` must reject unknown strategy names at the serde
/// boundary.  Before the strictness overhaul the field was `Vec<String>` and
/// unknown names were accepted.
#[test]
fn strict_verify_strategy_rejects_unknown_name() {
    serde_reject::<MediaRuntimeStorage>(
        json!({
            "verification": { "verify_on_read": ["bogus"] },
        }),
        "bogus",
    );
}

/// R1 regression: before S-E3, `to_verify_strategies` silently ignored unknown
/// strategy names (the match had an `_ => {}` arm).  Unknown names must now
/// fail fast at decode time instead of being dropped.
#[test]
fn regression_verify_on_read_unknown_no_longer_ignored() {
    serde_reject::<MediaRuntimeStorage>(
        json!({
            "verification": { "verify_on_read": ["always", "bogus"] },
        }),
        "bogus",
    );
}

/// S-E2: both yt-dlp- and generic-shaped variant objects must decode through
/// the typed untagged `OutputVariantValue` enum and round-trip losslessly for
/// the historical minimal wire form (`{ "kind": ... }`).
#[test]
fn strict_output_variants_accepts_ytdlp_and_generic_shapes() {
    // yt-dlp-shaped variant (untagged enum tries the YtDlp arm first).
    let yt: OutputVariantValue =
        serde_json::from_value(json!({ "kind": "primary" })).expect("yt-dlp shape must decode");
    assert!(matches!(yt, OutputVariantValue::YtDlp(_)));
    // Generic-shaped variant (kind string is not a yt-dlp kind).
    let generic: OutputVariantValue =
        serde_json::from_value(json!({ "kind": "custom" })).expect("generic shape must decode");
    assert!(matches!(generic, OutputVariantValue::Generic(_)));
    // Round-trip preserves the historical minimal wire form (defaults skipped).
    assert_eq!(
        serde_json::to_value(&yt).expect("yt-dlp variant must serialize"),
        json!({ "kind": "primary" })
    );
    assert_eq!(
        serde_json::to_value(&generic).expect("generic variant must serialize"),
        json!({ "kind": "custom" })
    );
    // The typed variants must also decode inside a full MediaStep document.
    let step: MediaStep = serde_json::from_value(json!({
        "tool": "yt-dlp",
        "output_variants": {
            "media": { "kind": "primary" },
            "raw": { "kind": "custom", "extension": "webm" },
        },
    }))
    .expect("typed output_variants must decode inside MediaStep");
    assert!(matches!(step.output_variants.get("media"), Some(OutputVariantValue::YtDlp(_))));
    assert!(matches!(step.output_variants.get("raw"), Some(OutputVariantValue::Generic(_))));
}

/// S-E3: all four CAS strategy names must decode and round-trip to the same
/// `snake_case` wire names.
///
/// The boundary type is reached through `MediaPmDocument::runtime` rather than
/// named directly: the boundary family is not publicly nameable, and a field
/// access exercises the same serde surface a real document load does.
#[test]
fn strict_verify_strategy_accepts_known_names() {
    let document: MediaPmDocument = serde_json::from_value(json!({
        "runtime": { "verification": { "verify_on_read": ["always", "modified", "sample", "stale"] } },
    }))
    .expect("all CAS strategy names must decode");
    assert_eq!(
        *document.runtime.verification.verify_on_read.as_ref().expect("verify_on_read is present"),
        vec![
            VerifyStrategy::Always,
            VerifyStrategy::Modified,
            VerifyStrategy::Sample,
            VerifyStrategy::Stale,
        ]
    );
    let back = serde_json::to_value(&document).expect("document must serialize");
    assert_eq!(
        back["runtime"]["verification"]["verify_on_read"],
        json!(["always", "modified", "sample", "stale"])
    );
}

/// S-E4: typed numeric fields reject fractional values at the serde boundary
/// (serde number-from-float validation on `u64` fields).
#[test]
fn strict_runtime_rejects_fractional_denominator() {
    serde_reject::<MediaRuntimeStorage>(
        json!({
            "verification": { "verify_on_read_sample_denominator": 1.5 },
        }),
        "invalid type",
    );
}
