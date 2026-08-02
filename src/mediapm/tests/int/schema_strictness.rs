//! Strictness tests for the mediapm Nickel schema surface (S-C1..S-C10).
//!
//! These tests evaluate the embedded `v1.ncl` / `mod.ncl` contracts through
//! the same `nickel-lang-core` evaluator that the production bridge uses, so
//! they run in CI without a Nickel CLI.  Each rejection test asserts that a
//! document which the *loose* pre-strictness schema accepted is now rejected.

use serde_json::{Value, json};

use mediapm::{apply_v1_contract, evaluate_mod_ncl_expression, validate_v1_document};

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
fn mod_ncl_version_contract_rejects_float() {
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
fn mod_ncl_exports_registry_surface() {
    let mod_source = include_str!("../../src/config/versions/mod.ncl");
    assert!(mod_source.contains("current_version = "), "mod.ncl must export current_version");
    assert!(
        mod_source.contains("supported_versions = [1]"),
        "mod.ncl must export supported_versions = [1]"
    );
    assert!(mod_source.contains("migrate_to = migrate_to_fn"), "mod.ncl must export migrate_to");
    assert!(
        mod_source.contains("SupportedVersion = SupportedVersion_pred"),
        "mod.ncl must export SupportedVersion"
    );
    assert!(
        mod_source.contains("validate_document = v1_migration.validate_document_v1"),
        "mod.ncl must re-export validate_document (unversioned alias)"
    );
    assert!(
        mod_source.contains("envelope_contract = v1_migration.envelope_contract_v1"),
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
fn v1_ncl_exports_validator_and_envelope() {
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
// Parity: the strict surface must not over-reject
// ---------------------------------------------------------------------------

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
// S-C9: strict JSON schema export
// ---------------------------------------------------------------------------

/// Recursively asserts that a JSON schema node is strict: every fixed-shape
/// object closes `additionalProperties` and declares `required`, map-typed
/// objects carry a typed `additionalProperties` schema, enums are non-empty,
/// and no stub (shape-less `"type": "object"`) nodes survive.
fn assert_strict_schema_structure(root: &Value, node: &Value, path: &str) {
    let Some(obj) = node.as_object() else {
        return;
    };

    if let Some(enum_values) = obj.get("enum") {
        assert!(
            !enum_values.as_array().expect("enum must be an array").is_empty(),
            "{path}: enum must be non-empty"
        );
    }

    if let Some(properties) = obj.get("properties") {
        let props = properties.as_object().expect("properties must be an object");
        if !props.is_empty() {
            assert_eq!(
                obj.get("additionalProperties"),
                Some(&json!(false)),
                "{path}: fixed-shape object must close additionalProperties"
            );
            assert!(
                obj.get("required").is_some(),
                "{path}: fixed-shape object must declare required"
            );
        }
        for (key, subschema) in props {
            assert_strict_schema_structure(root, subschema, &format!("{path}.{key}"));
        }
    }

    if let Some(additional) = obj.get("additionalProperties")
        && additional.is_object()
    {
        assert!(
            !obj.contains_key("properties"),
            "{path}: map-typed object must not also be fixed-shape"
        );
        assert_eq!(
            obj.get("type"),
            Some(&json!("object")),
            "{path}: map-typed object must declare \"type\": \"object\""
        );
        assert_strict_schema_structure(root, additional, &format!("{path}.<map>"));
    }

    if let Some(any_of) = obj.get("anyOf") {
        for (index, branch) in any_of.as_array().expect("anyOf must be an array").iter().enumerate()
        {
            assert_strict_schema_structure(root, branch, &format!("{path}.anyOf[{index}]"));
        }
    }

    if let Some(items) = obj.get("items") {
        assert_strict_schema_structure(root, items, &format!("{path}.items"));
    }

    if let Some(reference) = obj.get("$ref").and_then(Value::as_str) {
        // Verify the target resolves, but do not recurse into it: `$ref`
        // cycles are legal (e.g. `hierarchyNode.children.items` -> itself) and
        // every definition is already walked independently from the root.
        resolve_schema_ref(root, reference);
    }

    let has_shape = obj.contains_key("properties")
        || obj.contains_key("anyOf")
        || obj.contains_key("$ref")
        || obj.contains_key("enum")
        || obj.contains_key("const");
    if !has_shape {
        assert!(obj.get("type").is_some(), "{path}: node must declare a type (no stub objects)");
    }
}

fn resolve_schema_ref<'a>(root: &'a Value, reference: &str) -> &'a Value {
    assert!(reference.starts_with("#/definitions/"), "unexpected $ref target: {reference}");
    let key = reference.trim_start_matches("#/definitions/");
    root.get("definitions")
        .and_then(|defs| defs.get(key))
        .unwrap_or_else(|| panic!("unresolvable $ref: {reference}"))
}

/// A minimal draft-07 validator supporting exactly the keyword surface the
/// mediapm schema uses: `type`, `enum`, `const`, `properties`,
/// `additionalProperties` (bool or schema), `required`, `items`, `anyOf`, and
/// `$ref`.  Returns `Err` with a message on the first violation.
#[expect(clippy::too_many_lines, reason = "mini-validator covers the full keyword surface")]
fn validate_with_schema(
    root: &Value,
    schema: &Value,
    value: &Value,
    path: &str,
) -> Result<(), String> {
    if let Some(reference) = schema.get("$ref").and_then(Value::as_str) {
        let target = resolve_schema_ref(root, reference);
        return validate_with_schema(root, target, value, path);
    }
    if let Some(any_of) = schema.get("anyOf").and_then(Value::as_array) {
        let mut errors = Vec::new();
        let mut any_ok = false;
        for (index, branch) in any_of.iter().enumerate() {
            match validate_with_schema(root, branch, value, path) {
                Ok(()) => {
                    any_ok = true;
                    break;
                }
                Err(message) => errors.push(format!("anyOf[{index}]: {message}")),
            }
        }
        if !any_ok {
            return Err(format!("{path}: no anyOf branch matched: {}", errors.join("; ")));
        }
        return Ok(());
    }
    if let Some(const_value) = schema.get("const")
        && value != const_value
    {
        return Err(format!("{path}: expected const {const_value}, found {value}"));
    }
    if let Some(enum_values) = schema.get("enum").and_then(Value::as_array)
        && !enum_values.iter().any(|candidate| candidate == value)
    {
        return Err(format!("{path}: value {value} not in enum {enum_values:?}"));
    }
    if let Some(type_name) = schema.get("type").and_then(Value::as_str) {
        let matches = match type_name {
            "object" => value.is_object(),
            "array" => value.is_array(),
            "string" => value.is_string(),
            "boolean" => value.is_boolean(),
            "integer" => value.is_i64() || value.is_u64(),
            "number" => value.is_number(),
            other => panic!("unsupported type keyword: {other}"),
        };
        if !matches {
            return Err(format!("{path}: expected type {type_name}, found {value}"));
        }
    }
    if let (Some(properties), Some(obj)) = (schema.get("properties"), value.as_object()) {
        for (key, subschema) in properties.as_object().expect("properties must be an object") {
            if let Some(field) = obj.get(key) {
                validate_with_schema(root, subschema, field, &format!("{path}.{key}"))?;
            }
        }
        if let Some(required) = schema.get("required").and_then(Value::as_array) {
            for key in required {
                let key = key.as_str().expect("required keys must be strings");
                if !obj.contains_key(key) {
                    return Err(format!("{path}: missing required field '{key}'"));
                }
            }
        }
        match schema.get("additionalProperties") {
            Some(Value::Bool(false)) => {
                let props = properties
                    .as_object()
                    .expect("additionalProperties: false requires fixed-shape properties");
                for key in obj.keys() {
                    if !props.contains_key(key) {
                        return Err(format!(
                            "{path}: unexpected property '{key}' (additionalProperties is false)"
                        ));
                    }
                }
            }
            Some(additional_schema) if additional_schema.is_object() => {
                let props = properties
                    .as_object()
                    .expect("map-typed additionalProperties requires properties");
                for (key, field) in obj {
                    if !props.contains_key(key) {
                        validate_with_schema(
                            root,
                            additional_schema,
                            field,
                            &format!("{path}.{key}"),
                        )?;
                    }
                }
            }
            _ => {}
        }
    } else if let Some(map_schema) = schema.get("additionalProperties")
        && map_schema.is_object()
        && let Some(obj) = value.as_object()
    {
        for (key, field) in obj {
            validate_with_schema(root, map_schema, field, &format!("{path}.{key}"))?;
        }
    }
    if let (Some(items), Some(array)) = (schema.get("items"), value.as_array()) {
        for (index, element) in array.iter().enumerate() {
            validate_with_schema(root, items, element, &format!("{path}[{index}]"))?;
        }
    }
    Ok(())
}

/// S-C9: the exported `mediapm.schema.json` is strict draft-07.
#[test]
fn json_schema_export_is_strict() {
    let dir = tempfile::tempdir().expect("create temp dir");
    let export_dir = dir.path().join("schemas");
    let conductor_dir = dir.path().join("conductor");
    mediapm::export_mediapm_nickel_config_schemas(Some(&export_dir), &conductor_dir)
        .expect("export schemas");

    let schema_path = export_dir.join("mediapm.schema.json");
    let schema_text = std::fs::read_to_string(&schema_path).expect("read mediapm schema");
    let schema: Value = serde_json::from_str(&schema_text).expect("parse mediapm schema");

    // Root shape: object, closed, with required fields.
    assert_eq!(schema["type"], "object");
    assert_eq!(schema["additionalProperties"], json!(false));
    assert!(schema["required"].is_array(), "root must carry a required list");
    let root_required: Vec<&str> = schema["required"]
        .as_array()
        .expect("required must be an array")
        .iter()
        .filter_map(Value::as_str)
        .collect();
    for key in ["version", "media", "hierarchy", "tools", "runtime"] {
        assert!(root_required.contains(&key), "root must require field '{key}'");
    }
    assert!(
        schema["definitions"].as_object().expect("definitions").len() >= 10,
        "schema must define the full closed-surface definitions set"
    );

    // Full recursive strictness walk (rejects any leftover stub objects).
    assert_strict_schema_structure(&schema, &schema, "$");

    // --- rejection samples validated by the mini-validator ---
    let base: Value =
        serde_json::from_str(r#"{"version":1,"media":{},"hierarchy":[],"tools":{},"runtime":{}}"#)
            .expect("base document");

    assert!(
        validate_with_schema(&schema, &schema, &base, "$").is_ok(),
        "empty-but-valid document must pass"
    );

    let mut unknown_root = base.clone();
    unknown_root["bogus"] = json!(1);
    assert!(
        validate_with_schema(&schema, &schema, &unknown_root, "$").is_err(),
        "unknown root field must be rejected"
    );

    let mut fractional_ttl = base.clone();
    fractional_ttl["runtime"]["instance_ttl_seconds"] = json!(2.5);
    assert!(
        validate_with_schema(&schema, &schema, &fractional_ttl, "$").is_err(),
        "fractional instance_ttl_seconds must be rejected by \"type\": \"integer\""
    );

    let mut bad_kind = base.clone();
    bad_kind["hierarchy"] = json!([{ "kind": "video" }]);
    assert!(
        validate_with_schema(&schema, &schema, &bad_kind, "$").is_err(),
        "unknown hierarchy kind must be rejected by enum"
    );

    let mut bad_media = base.clone();
    bad_media["media"] = json!({ "m1": { "bogus": 1 } });
    assert!(
        validate_with_schema(&schema, &schema, &bad_media, "$").is_err(),
        "unknown media field must be rejected by additionalProperties: false"
    );

    let mut missing_media = base.clone();
    missing_media.as_object_mut().expect("base is object").remove("media");
    assert!(
        validate_with_schema(&schema, &schema, &missing_media, "$").is_err(),
        "missing required field 'media' must be rejected"
    );

    // Conductor schema remains a plain object schema.
    let conductor_path = conductor_dir.join("conductor.schema.json");
    let conductor: Value = serde_json::from_str(
        &std::fs::read_to_string(&conductor_path).expect("read conductor schema"),
    )
    .expect("parse conductor schema");
    assert_eq!(conductor["type"], "object");
}
