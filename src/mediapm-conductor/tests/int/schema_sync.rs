//! Sync-prevention tests for the conductor Nickel schema (`v2.ncl`).
//!
//! These tests validate that the V2 Nickel schema definition stays in sync
//! with the Rust deserialization types (`NickelDocument`).  If the Rust
//! structs gain or lose fields, the Nickel schema must be updated
//! correspondingly, and these tests force that update to be deliberate.

use mediapm_conductor::{NickelDocument, ToolKindSpec, ToolRuntime, ToolSpec};

/// Validates that the Rust struct serialization shape matches the expected
/// V2 schema contract invariants.
#[test]
fn conductor_document_serialization_invariants() {
    // Use a populated document so skip_serializing_if fields are visible.
    let mut doc = NickelDocument::default();
    doc.tools.insert(
        "test-tool".into(),
        ToolSpec {
            kind: ToolKindSpec::Builtin { builtin_id: "echo@v1".into() },
            name: "echo".into(),
            runtime: ToolRuntime::default(),
            ..Default::default()
        },
    );
    let json = serde_json::to_value(&doc).unwrap();
    let obj = json.as_object().expect("NickelDocument must serialize to a JSON object");

    // --- MUST be present at top level ---
    assert!(
        obj.contains_key("tools"),
        "tools must be a top-level field in NickelDocument (must be in V2 schema)"
    );
    assert!(
        obj.contains_key("workflows"),
        "workflows must be a top-level field in NickelDocument (must be in V2 schema)"
    );
    assert!(
        obj.contains_key("runtime"),
        "runtime must be a top-level field in NickelDocument (must be in V2 schema)"
    );

    // --- MUST be absent (NickelDocument has no version field) ---
    assert!(
        !obj.contains_key("version"),
        "version must NOT be a top-level field in NickelDocument (NickelDocument has no version field)"
    );
}

/// Validates that the V2 Nickel schema (`v2.ncl`) contains the expected
/// contracts and omits removed ones.
#[test]
fn v2_nickel_schema_structure() {
    let schema = include_str!("../../src/config/versions/v2.ncl");

    // --- runtime MUST be optional ---
    assert!(
        schema.contains("runtime | ConductorRuntimeConfigV2 | optional"),
        "v2.ncl must make runtime optional (NickelDocument has #[serde(default)] on runtime)"
    );

    // --- version MUST be required (S-A2) ---
    assert!(
        schema.contains("version | VersionTwoV2,"),
        "v2.ncl must make version required (envelope carries the version marker)"
    );
    assert!(
        !schema.contains("version | VersionTwoV2 | optional"),
        "v2.ncl must not mark version optional (version is required)"
    );

    // --- envelope MUST be closed (S-A1): no `..` open-record marker ---
    assert!(
        !schema.lines().any(|line| line.trim() == ".."),
        "v2.ncl NickelDocumentV2 must be a closed record (no `..` for forward compat)"
    );

    // --- strictness assertions (S-A3, S-A4, S-A5, S-A6) ---
    assert!(
        schema.contains("hash | NonEmptyStringV2 | optional"),
        "v2.ncl ExternalContentRefV2.hash must be a non-empty string"
    );
    assert!(
        schema.contains("inherited_env_vars | Array NonEmptyStringV2 | optional"),
        "v2.ncl ToolRuntimeV2.inherited_env_vars must be an array of non-empty strings"
    );
    assert!(
        schema.contains("platform_inherited_env_vars | PlatformInheritedEnvVarsV2 | optional"),
        "v2.ncl platform_inherited_env_vars must use the closed PlatformInheritedEnvVarsV2 contract"
    );
    assert!(
        !schema.contains("platform_inherited_env_vars | { _ : Array String }"),
        "v2.ncl must not accept arbitrary platform keys in platform_inherited_env_vars"
    );
    assert!(
        schema.contains("display_name | NonEmptyStringV2 | optional")
            && schema.contains("description | NonEmptyStringV2 | optional"),
        "v2.ncl WorkflowSpecV2 display_name/description must be non-empty strings"
    );

    // --- MUST still define key contracts ---
    assert!(
        schema.contains("let NickelDocumentV2 = {"),
        "v2.ncl must define NickelDocumentV2 contract"
    );
    assert!(
        schema.contains("let ConductorRuntimeConfigV2 = "),
        "v2.ncl must define ConductorRuntimeConfigV2 contract"
    );
    assert!(schema.contains("let ToolSpecV2 = "), "v2.ncl must define ToolSpecV2 contract");
}

/// Validates that the V1 legacy Nickel schema (`v1.ncl`) contains the
/// tightened legacy-surface contracts (S-B1..S-B5), keeps the documented
/// `state_pointer` exception (S-B6), and preserves the canonical envelope
/// shape (top-level `tool_configs`, required `runtime`).
#[test]
fn v1_nickel_schema_structure() {
    let schema = include_str!("../../src/config/versions/v1.ncl");

    // --- S-B1..S-B3: runtime numeric fields are integer-guarded ---
    for field in [
        "verify_on_read_sample_denominator",
        "verify_on_read_stale_timeout_secs",
        "reconstructed_bytes_cache_ttl_secs",
    ] {
        assert!(
            schema.contains(&format!("{field} | IntegerNumberV1 | optional")),
            "v1.ncl RuntimeStorageV1.{field} must be integer-guarded"
        );
    }

    // --- S-B4: retry_impure is bool-guarded ---
    assert!(
        schema.contains("retry_impure | Bool | optional"),
        "v1.ncl RuntimeStorageV1.retry_impure must be bool-guarded"
    );

    // --- S-B5: tool-level outputs use the OutputPolicyV1 record ---
    assert!(
        schema.contains("outputs | { _ : OutputPolicyV1 } | optional"),
        "v1.ncl ExecutableToolSpecV1.outputs must be OutputPolicyV1 records"
    );
    assert!(
        !schema.contains("outputs | { _ : Dyn }"),
        "v1.ncl must not leave tool-level outputs untyped"
    );

    // --- S-B6: state_pointer stays Dyn with the why-comment ---
    assert!(
        schema.contains("state_pointer | Dyn | optional"),
        "v1.ncl must keep state_pointer Dyn (Option<Hash> bridging)"
    );

    // --- canonical envelope: top-level tool_configs, required runtime ---
    assert!(
        schema.contains("tool_configs | { _ : ToolConfigV1 } | optional"),
        "v1.ncl NickelDocumentV1 must declare tool_configs at top level"
    );
    assert!(
        schema.contains("runtime | RuntimeStorageV1,"),
        "v1.ncl NickelDocumentV1 must keep runtime required (non-nullable schema)"
    );

    // --- RuntimeStorageV1 is closed and fully typed (no remaining Dyn) ---
    let runtime_block = schema
        .split("let RuntimeStorageV1 = {")
        .nth(1)
        .expect("RuntimeStorageV1 contract must be defined")
        .split("}\nin")
        .next()
        .expect("RuntimeStorageV1 contract must be a record");
    assert!(
        !runtime_block.contains("Dyn"),
        "v1.ncl RuntimeStorageV1 must have no remaining Dyn fields"
    );
    assert!(
        !runtime_block.lines().any(|line| line.trim() == ".."),
        "v1.ncl RuntimeStorageV1 must be a closed record"
    );
}
