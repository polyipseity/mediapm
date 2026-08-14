//! Schema strictness guard module (R5).
//!
//! Re-asserts the strictness properties of the conductor Nickel schema
//! (`v1.ncl` and `v2.ncl`) and the Rust decode pipeline. A future loosening
//! of these contracts fails this suite even if it compiles.
//!
//! Naming: `strict_*` = reject previously-accepted input; `regression_*` =
//! input that was silently accepted/dropped before now errors.

use mediapm_conductor::NickelDocument;
use mediapm_conductor::config::versions::{decode_document, validate_v1_document};
use mediapm_conductor::config::{
    ConductorRuntimeConfig, OutputCaptureSpec, ToolInputSpec, ToolRuntime, ToolSpec, WorkflowSpec,
    WorkflowStepSpec,
};

// ---------------------------------------------------------------------------
// Error-extraction helpers (assertions stay at each call site)
// ---------------------------------------------------------------------------

/// Returns the formatted error from decoding a v2 document that must fail.
fn v2_decode_error(doc: &str) -> String {
    format!("{}", decode_document(doc.as_bytes()).expect_err("v2 document must be rejected"))
}

/// Returns the formatted error from validating a v1 document that must fail.
fn v1_validation_error(doc: &str) -> String {
    format!("{}", validate_v1_document(doc).expect_err("v1 document must be rejected"))
}

/// Returns the formatted error from deserializing `value` as `T`, which must
/// fail.
fn serde_reject<T: serde::de::DeserializeOwned + std::fmt::Debug>(
    value: serde_json::Value,
) -> String {
    format!("{}", serde_json::from_value::<T>(value).expect_err("value must be rejected"))
}

/// A v2 document with an extra top-level field unknown to the envelope.
const UNKNOWN_TOP_LEVEL_FIELD_DOC: &str = r#"{
    version = 2,
    bogus_field = "silently dropped before",
}"#;

/// A v2 document with an empty `external_data` hash.
const EMPTY_HASH_DOC: &str = r#"{
    version = 2,
    external_data = {
        "blake3:aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa" = {
            hash = "",
            description = "empty hash",
            save = true,
        },
    },
}"#;

/// A v2 document with an unknown platform key in `platform_inherited_env_vars`.
const UNKNOWN_PLATFORM_KEY_DOC: &str = r#"{
    version = 2,
    runtime = {
        platform_inherited_env_vars = {
            other_os = ["SOME_VAR"],
        },
    },
}"#;

/// A v2 document with a fractional `max_retries`.
const FRACTIONAL_MAX_RETRIES_DOC: &str = r#"{
    version = 2,
    tools = {
        "my-tool" = {
            kind = "executable",
            name = "my-tool",
            command = ["my-tool"],
            runtime = {
                max_retries = 2.5,
            },
        },
    },
}"#;

/// A v2 document with an empty inherited environment variable name.
const EMPTY_ENV_VAR_NAME_DOC: &str = r#"{
    version = 2,
    tools = {
        "my-tool" = {
            kind = "executable",
            name = "my-tool",
            command = ["my-tool"],
            runtime = {
                inherited_env_vars = [""],
            },
        },
    },
}"#;

/// A realistic v1 document used to prove the v1→v2 migration output satisfies
/// the tightened v2 envelope.
const REALISTIC_V1_DOC: &str = r#"{
    version = 1,
    runtime = {
        retry_impure = true,
        inherited_env_vars = {
            macos = ["PATH"],
        },
    },
    tool_configs = {
        "ffmpeg" = {
            env_vars = {
                FFMPEG_HOME = "/opt/ffmpeg",
            },
            content_map = {
                "ffmpeg" = "blake3:aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa",
            },
            max_retries = 3,
        },
    },
    tools = {
        "ffmpeg" = {
            kind = "executable",
            command = ["ffmpeg"],
        },
        "echo" = {
            kind = "builtin",
            builtin_id = "echo@v1",
        },
    },
    workflows = {
        "wf1" = {
            name = "wf1",
            description = "test workflow",
            steps = [
                {
                    id = "s1",
                    tool = "echo",
                    inputs = { text = "hello" },
                },
            ],
        },
    },
    external_data = {
        "blake3:aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa" = {
            description = "ffmpeg binary",
            save = true,
        },
    },
}"#;

/// S-A1: unknown top-level fields are rejected (closed envelope, no `..`).
#[test]
fn strict_v2_rejects_unknown_top_level_field() {
    let msg = v2_decode_error(UNKNOWN_TOP_LEVEL_FIELD_DOC);
    assert!(msg.contains("bogus_field"), "error must name the unknown field: {msg}");
}

/// S-A3: `ExternalContentRefV2.hash` must be a non-empty string.
#[test]
fn strict_v2_rejects_empty_hash() {
    let msg = v2_decode_error(EMPTY_HASH_DOC);
    assert!(
        msg.contains("hash") && msg.contains("contract"),
        "error must mention the hash contract: {msg}"
    );
}

/// S-A5: `platform_inherited_env_vars` is closed to windows/linux/macos.
#[test]
fn strict_v2_rejects_unknown_platform_key() {
    let msg = v2_decode_error(UNKNOWN_PLATFORM_KEY_DOC);
    assert!(
        msg.contains("other_os") || msg.contains("platform"),
        "error must mention the offending key or the platform contract: {msg}"
    );
}

/// S-A4/S3: integer-guarded `max_retries` rejects fractional values.
#[test]
fn strict_v2_rejects_fractional_max_retries() {
    let msg = v2_decode_error(FRACTIONAL_MAX_RETRIES_DOC);
    assert!(msg.contains("contract"), "error must mention a contract violation: {msg}");
}

/// S-A4: `inherited_env_vars` entries must be non-empty strings.
#[test]
fn strict_v2_rejects_empty_env_var_name() {
    let msg = v2_decode_error(EMPTY_ENV_VAR_NAME_DOC);
    assert!(msg.contains("contract"), "error must mention a contract violation: {msg}");
}

/// S-A2/S1: a minimal valid v2 document still decodes (R2 — no valid-doc
/// regression).
#[test]
fn strict_v2_accepts_valid_minimal_document() {
    let doc = decode_document(r"{ version = 2 }".as_bytes()).expect("minimal v2 doc must decode");
    assert_eq!(doc.tools.len(), 0);
    assert!(doc.workflows.is_empty());
}

/// S-A8: the v1→v2 migration output satisfies the tightened v2 envelope.
#[test]
fn parity_v1_to_v2_migration_output_passes_tightened_envelope() {
    let doc = decode_document(REALISTIC_V1_DOC.as_bytes())
        .expect("realistic v1 doc must migrate and satisfy tightened v2 envelope");
    assert!(doc.tools.contains_key("ffmpeg"), "ffmpeg tool migrated");
    assert!(doc.tools.contains_key("echo"), "echo tool migrated");
    assert_eq!(doc.workflows.len(), 1, "workflow array migrated");
    let ffmpeg = &doc.tools["ffmpeg"];
    assert!(
        ffmpeg.runtime.inherited_env_vars.iter().any(|s| s == "FFMPEG_HOME"),
        "tool_configs env_vars must migrate into inherited_env_vars"
    );
}

/// S-A7: `mod.ncl` exports unversioned registry aliases.
#[test]
fn parity_mod_ncl_exports_unversioned_aliases() {
    let mod_source = include_str!("../../src/config/versions/mod.ncl");
    assert!(
        mod_source.contains("validate_document = v2_migration.validate_document_v2"),
        "mod.ncl must re-export validate_document (unversioned alias)"
    );
    assert!(
        mod_source.contains("envelope_contract = v2_migration.envelope_contract_v2"),
        "mod.ncl must re-export envelope_contract (unversioned alias)"
    );
}

/// Regression: unknown top-level fields were silently dropped via the `..`
/// envelope open; they must now fail loudly.
#[test]
fn regression_v2_unknown_field_no_longer_silently_dropped() {
    let msg = v2_decode_error(UNKNOWN_TOP_LEVEL_FIELD_DOC);
    assert!(
        msg.contains("bogus_field"),
        "the unknown field must surface in the error, not be dropped: {msg}"
    );
}

// ---------------------------------------------------------------------------
// v1 legacy surface (S-B1..S-B7)
// ---------------------------------------------------------------------------

/// S-B1: a v1 runtime with a non-integer sample denominator.
const V1_STRING_SAMPLE_DENOMINATOR_DOC: &str = r#"{
    version = 1,
    runtime = {
        verify_on_read_sample_denominator = "fast",
    },
}"#;

/// S-B2: a v1 runtime with a fractional stale timeout.
const V1_FRACTIONAL_STALE_TIMEOUT_DOC: &str = r"{
    version = 1,
    runtime = {
        verify_on_read_stale_timeout_secs = 1.5,
    },
}";

/// S-B3: a v1 runtime with a fractional reconstructed-bytes TTL.
const V1_FRACTIONAL_RUNTIME_TIMEOUT_DOC: &str = r"{
    version = 1,
    runtime = {
        reconstructed_bytes_cache_ttl_secs = 1.5,
    },
}";

/// S-B4: a v1 runtime with a string-valued `retry_impure`.
const V1_STRING_RETRY_IMPURE_DOC: &str = r#"{
    version = 1,
    runtime = {
        retry_impure = "yes",
    },
}"#;

/// S-B5: a v1 tool with a free-form (non-OutputPolicyV1) output value.
const V1_FREE_FORM_OUTPUT_DOC: &str = r#"{
    version = 1,
    runtime = {},
    tools = {
        "t" = {
            kind = "executable",
            command = ["x"],
            outputs = {
                o = { arbitrary = 1 },
            },
        },
    },
}"#;

/// A realistic v2 document exercising every v1-migratable construct
/// (S-B7/R3 migration fixture).
const REALISTIC_V2_DOC: &str = r#"{
    version = 2,
    runtime = {
        retry_impure = true,
        platform_inherited_env_vars = {
            macos = ["PATH"],
        },
    },
    tools = {
        "ffmpeg" = {
            kind = "executable",
            name = "ffmpeg",
            command = ["ffmpeg"],
            env_vars = {
                FFMPEG_HOME = "/opt/ffmpeg",
            },
            success_codes = [0],
            inputs = {
                url = { kind = "string" },
                args = { kind = "string_list", required = true },
            },
            default_inputs = {
                format = "mp4",
            },
            outputs = {
                out = { name = "out", capture = "out", save = "full" },
                raw = { name = "raw", capture = "raw", save = "false" },
            },
            runtime = {
                impure = true,
                max_concurrent_calls = 2,
                max_retries = 3,
                inherited_env_vars = ["FFMPEG_HOME"],
                content_map = {
                    "ffmpeg" = "blake3:aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa",
                },
            },
        },
        "echo" = {
            kind = "builtin",
            name = "echo",
            builtin_id = "echo@v1",
            default_inputs = {
                text = "hello",
            },
        },
    },
    workflows = [
        {
            name = "wf1",
            display_name = "WF One",
            description = "test workflow",
            steps = [
                {
                    id = "s1",
                    tool = "echo",
                    inputs = { text = "hello" },
                    outputs = {
                        stdout = { name = "stdout", capture = "stdout", save = "true" },
                    },
                    depends_on = [],
                },
            ],
        },
    ],
    external_data = {
        "blake3:aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa" = {
            hash = "blake3:aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa",
            description = "ffmpeg binary",
            save = "full",
        },
    },
}"#;

/// S-B1: `verify_on_read_sample_denominator` is integer-guarded.
#[test]
fn strict_v1_rejects_string_sample_denominator() {
    let msg = v1_validation_error(V1_STRING_SAMPLE_DENOMINATOR_DOC);
    assert!(
        msg.contains("verify_on_read_sample_denominator"),
        "error must name the offending field: {msg}"
    );
}

/// S-B2: `verify_on_read_stale_timeout_secs` is integer-guarded.
#[test]
fn strict_v1_rejects_fractional_stale_timeout() {
    let msg = v1_validation_error(V1_FRACTIONAL_STALE_TIMEOUT_DOC);
    assert!(
        msg.contains("verify_on_read_stale_timeout_secs"),
        "error must name the offending field: {msg}"
    );
}

/// S-B3: `reconstructed_bytes_cache_ttl_secs` is integer-guarded.
#[test]
fn strict_v1_rejects_fractional_runtime_timeout() {
    let msg = v1_validation_error(V1_FRACTIONAL_RUNTIME_TIMEOUT_DOC);
    assert!(
        msg.contains("reconstructed_bytes_cache_ttl_secs"),
        "error must name the offending field: {msg}"
    );
}

/// S-B4: `retry_impure` is bool-guarded.
#[test]
fn strict_v1_rejects_string_retry_impure() {
    let msg = v1_validation_error(V1_STRING_RETRY_IMPURE_DOC);
    assert!(msg.contains("retry_impure"), "error must name the offending field: {msg}");
}

/// S-B5: tool-level `outputs` values are `OutputPolicyV1` records, not
/// free-form values.
#[test]
fn strict_v1_rejects_free_form_output_value() {
    let msg = v1_validation_error(V1_FREE_FORM_OUTPUT_DOC);
    assert!(msg.contains("outputs"), "error must name the offending field: {msg}");
}

/// R2: a known-good v1 document still validates after the S-B1..S-B5
/// tightening.
#[test]
fn strict_v1_accepts_valid_legacy_document() {
    let doc = validate_v1_document(REALISTIC_V1_DOC).expect("realistic v1 doc must validate");
    let obj = doc.as_object().expect("v1 doc must be an object");
    // The Rust deserialization path yields floats for integral Nickel
    // numbers (see `read_document_version_marker`'s as_u64/as_f64
    // fallback), so compare via `as_f64`.
    assert_eq!(obj["version"].as_f64(), Some(1.0));
    assert!(obj.contains_key("tool_configs"), "envelope must carry top-level tool_configs");
    let tc = obj["tool_configs"].as_object().expect("tool_configs must be a map");
    assert!(tc.contains_key("ffmpeg"), "tool_configs entry must survive validation");
}

/// S-B7/R3: the v2→v1 migration output satisfies the tightened v1 envelope,
/// preserving tool runtimes, workflows, and external data.
#[test]
fn parity_v2_to_v1_migration_output_passes_tightened_v1() {
    let doc = validate_v1_document(REALISTIC_V2_DOC)
        .expect("realistic v2 doc must migrate and satisfy the tightened v1 envelope");
    let obj = doc.as_object().expect("migrated v1 doc must be an object");

    let tools = obj["tools"].as_object().expect("tools must be a map");
    let ffmpeg = tools["ffmpeg"].as_object().expect("ffmpeg tool migrated");
    assert_eq!(ffmpeg["kind"], "executable");
    assert_eq!(ffmpeg["command"][0], "ffmpeg");
    assert_eq!(ffmpeg["is_impure"], true);
    assert_eq!(ffmpeg["outputs"]["out"]["save"], "full");
    assert_eq!(ffmpeg["outputs"]["raw"]["save"], false);
    assert_eq!(
        ffmpeg["inputs"]["args"]["kind"], "string_list",
        "v1-representable input kinds pass through"
    );
    assert!(
        !ffmpeg["inputs"]["args"].as_object().unwrap().contains_key("required"),
        "v1 ToolInputSpecV1 has no required field"
    );

    let echo = tools["echo"].as_object().expect("echo tool migrated");
    assert_eq!(echo["kind"], "builtin");
    assert_eq!(echo["builtin_id"], "echo@v1");
    assert!(!echo.contains_key("name"), "v1 builtin spec must not carry name");

    let tc = obj["tool_configs"].as_object().expect("tool_configs must be a map");
    let ffmpeg_tc = tc["ffmpeg"].as_object().expect("ffmpeg tool_configs migrated");
    assert_eq!(ffmpeg_tc["max_retries"].as_f64(), Some(3.0));
    assert_eq!(ffmpeg_tc["max_concurrent_calls"].as_f64(), Some(2.0));
    assert_eq!(ffmpeg_tc["env_vars"]["FFMPEG_HOME"], "FFMPEG_HOME");
    assert!(ffmpeg_tc.contains_key("content_map"), "content_map migrated");

    let workflows = obj["workflows"].as_object().expect("workflows must be a map");
    let wf1 = workflows["wf1"].as_object().expect("wf1 workflow migrated");
    assert_eq!(wf1["name"], "wf1");
    assert_eq!(wf1["description"], "test workflow");
    let steps = wf1["steps"].as_array().expect("steps must be an array");
    assert_eq!(steps[0]["outputs"]["stdout"]["save"], true);

    let ext = obj["external_data"].as_object().expect("external_data must be a map");
    let ext_entry = ext.values().next().expect("external_data entry migrated");
    assert_eq!(ext_entry["save"], "full");
}

/// R1: the four runtime fields that were `Dyn` are now typed; every
/// previously-accepted untyped value is rejected.
#[test]
fn regression_v1_dyn_runtime_fields_no_longer_untyped() {
    for doc in [
        V1_STRING_SAMPLE_DENOMINATOR_DOC,
        V1_FRACTIONAL_STALE_TIMEOUT_DOC,
        V1_FRACTIONAL_RUNTIME_TIMEOUT_DOC,
        V1_STRING_RETRY_IMPURE_DOC,
    ] {
        let msg = v1_validation_error(doc);
        assert!(msg.contains("contract"), "error must mention a contract violation: {msg}");
    }
}

// ---------------------------------------------------------------------------
// Rust serde strictness (S-D1..S-D4)
// ---------------------------------------------------------------------------

/// S-D1: `ToolSpec` rejects unknown fields via serde.
#[test]
fn strict_serde_tool_spec_rejects_unknown_field() {
    let msg = serde_reject::<ToolSpec>(serde_json::json!({
        "kind": { "kind": "builtin", "builtin_id": "echo@v1" },
        "name": "echo",
        "bogus_field": 1,
    }));
    assert!(msg.contains("bogus_field"), "error must name the unknown field: {msg}");
}

/// S-D1: `WorkflowSpec` rejects unknown fields via serde.
#[test]
fn strict_serde_workflow_spec_rejects_unknown_field() {
    let msg = serde_reject::<WorkflowSpec>(serde_json::json!({
        "name": "wf1",
        "bogus_field": 1,
    }));
    assert!(msg.contains("bogus_field"), "error must name the unknown field: {msg}");
}

/// S-D1: `WorkflowStepSpec` rejects unknown fields via serde.
#[test]
fn strict_serde_step_spec_rejects_unknown_field() {
    let msg = serde_reject::<WorkflowStepSpec>(serde_json::json!({
        "id": "s1",
        "tool": "echo",
        "bogus_field": 1,
    }));
    assert!(msg.contains("bogus_field"), "error must name the unknown field: {msg}");
}

/// S-D1: `ToolRuntime` rejects unknown fields via serde.
#[test]
fn strict_serde_runtime_rejects_unknown_field() {
    let msg = serde_reject::<ToolRuntime>(serde_json::json!({
        "impure": true,
        "bogus_field": 1,
    }));
    assert!(msg.contains("bogus_field"), "error must name the unknown field: {msg}");
}

/// S-D1: `OutputCaptureSpec` and `ToolInputSpec` reject unknown fields.
#[test]
fn strict_serde_sub_specs_reject_unknown_fields() {
    let msg = serde_reject::<OutputCaptureSpec>(serde_json::json!({
        "name": "out",
        "capture": "stdout",
        "bogus_field": 1,
    }));
    assert!(msg.contains("bogus_field"), "error must name the unknown field: {msg}");

    let msg = serde_reject::<ToolInputSpec>(serde_json::json!({
        "kind": "string",
        "bogus_field": 1,
    }));
    assert!(msg.contains("bogus_field"), "error must name the unknown field: {msg}");
}

/// S-D1: `NickelDocument` rejects unknown top-level fields via serde.
#[test]
fn strict_serde_document_rejects_unknown_field() {
    let msg = serde_reject::<NickelDocument>(serde_json::json!({
        "bogus_field": 1,
    }));
    assert!(msg.contains("bogus_field"), "error must name the unknown field: {msg}");
}

/// S-D3: `platform_inherited_env_vars` is closed to windows/linux/macos in
/// the Rust serde layer, not just the Nickel contract.
#[test]
fn strict_platform_env_rejects_unknown_key() {
    let msg = serde_reject::<ConductorRuntimeConfig>(serde_json::json!({
        "platform_inherited_env_vars": { "bsd": [] },
    }));
    assert!(msg.contains("bsd"), "error must name the unknown key: {msg}");
}

/// S-D3: platform env var names must be non-empty in the Rust serde layer.
#[test]
fn strict_platform_env_rejects_empty_env_name() {
    let msg = serde_reject::<ConductorRuntimeConfig>(serde_json::json!({
        "platform_inherited_env_vars": { "linux": [""] },
    }));
    assert!(msg.contains("non-empty"), "error must explain the non-empty rule: {msg}");
}

/// R2: known-good conductor documents still round-trip after the S-D1..S-D4
/// tightening (Nickel decode path).
#[test]
fn regression_valid_conductor_docs_still_round_trip() {
    let minimal =
        decode_document(r"{ version = 2 }".as_bytes()).expect("minimal v2 doc must still decode");
    assert!(minimal.tools.is_empty() && minimal.workflows.is_empty());

    // S-D3: a runtime record with platform-inherited env vars must still
    // decode through the typed `PlatformInheritedEnvVars` representation.
    let platform_doc = decode_document(
        br#"{
            version = 2,
            runtime = {
                retry_impure = true,
                platform_inherited_env_vars = { macos = ["PATH"] },
            },
        }"#,
    )
    .expect("platform-runtime doc must still decode");
    assert!(platform_doc.runtime.retry_impure, "retry_impure preserved");
    assert_eq!(
        platform_doc.runtime.platform_inherited_env_vars.macos,
        vec!["PATH".to_string()],
        "platform env vars preserved"
    );
}
