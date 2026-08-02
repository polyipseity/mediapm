//! Schema strictness guard module (R5).
//!
//! Re-asserts the strictness properties of the conductor Nickel schema
//! (`v2.ncl`) and the Rust decode pipeline. A future loosening of these
//! contracts fails this suite even if it compiles.
//!
//! Naming: `strict_*` = reject previously-accepted input; `regression_*` =
//! input that was silently accepted/dropped before now errors.

use mediapm_conductor::config::versions::decode_document;

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
            hash = "blake3:aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa",
            description = "ffmpeg binary",
            save = true,
        },
    },
}"#;

/// S-A1: unknown top-level fields are rejected (closed envelope, no `..`).
#[test]
fn strict_v2_rejects_unknown_top_level_field() {
    let err = decode_document(UNKNOWN_TOP_LEVEL_FIELD_DOC.as_bytes())
        .expect_err("unknown top-level field must be rejected");
    let msg = format!("{err}");
    assert!(msg.contains("bogus_field"), "error must name the unknown field: {msg}");
}

/// S-A3: `ExternalContentRefV2.hash` must be a non-empty string.
#[test]
fn strict_v2_rejects_empty_hash() {
    let err = decode_document(EMPTY_HASH_DOC.as_bytes()).expect_err("empty hash must be rejected");
    let msg = format!("{err}");
    assert!(
        msg.contains("hash") && msg.contains("contract"),
        "error must mention the hash contract: {msg}"
    );
}

/// S-A5: `platform_inherited_env_vars` is closed to windows/linux/macos.
#[test]
fn strict_v2_rejects_unknown_platform_key() {
    let err = decode_document(UNKNOWN_PLATFORM_KEY_DOC.as_bytes())
        .expect_err("unknown platform key must be rejected");
    let msg = format!("{err}");
    assert!(
        msg.contains("other_os") || msg.contains("platform"),
        "error must mention the offending key or the platform contract: {msg}"
    );
}

/// S-A4/S3: integer-guarded `max_retries` rejects fractional values.
#[test]
fn strict_v2_rejects_fractional_max_retries() {
    let err = decode_document(FRACTIONAL_MAX_RETRIES_DOC.as_bytes())
        .expect_err("fractional max_retries must be rejected");
    let msg = format!("{err}");
    assert!(msg.contains("contract"), "error must mention a contract violation: {msg}");
}

/// S-A4: `inherited_env_vars` entries must be non-empty strings.
#[test]
fn strict_v2_rejects_empty_env_var_name() {
    let err = decode_document(EMPTY_ENV_VAR_NAME_DOC.as_bytes())
        .expect_err("empty env var name must be rejected");
    let msg = format!("{err}");
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
fn v1_to_v2_migration_output_passes_tightened_envelope() {
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
fn mod_ncl_exports_unversioned_aliases() {
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
    let err = decode_document(UNKNOWN_TOP_LEVEL_FIELD_DOC.as_bytes())
        .expect_err("unknown field must not be silently dropped");
    let msg = format!("{err}");
    assert!(
        msg.contains("bogus_field"),
        "the unknown field must surface in the error, not be dropped: {msg}"
    );
}
