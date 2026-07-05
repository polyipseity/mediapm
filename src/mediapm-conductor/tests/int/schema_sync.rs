//! Sync-prevention tests for the conductor Nickel schema (`v2.ncl`).
//!
//! These tests validate that the V2 Nickel schema definition stays in sync
//! with the Rust deserialization types (`NickelDocument`).  If the Rust
//! structs gain or lose fields, the Nickel schema must be updated
//! correspondingly, and these tests force that update to be deliberate.

use mediapm_conductor::config::documents::NickelDocument;
use mediapm_conductor::config::{ToolKindSpec, ToolRuntime, ToolSpec};

/// Validates that the Rust struct serialization shape matches the expected
/// V2 schema contract invariants.
#[test]
fn conductor_document_serialization_invariants() {
    // Use a populated document so skip_serializing_if fields are visible.
    let mut doc = NickelDocument::default();
    doc.tools.insert(
        "test-tool".into(),
        ToolSpec {
            kind: ToolKindSpec::Builtin { builtin_id: "echo@1.0.0".into() },
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

    // --- version MUST be optional ---
    assert!(
        schema.contains("version | VersionTwoV2 | optional"),
        "v2.ncl must make version optional (NickelDocument has no version field)"
    );

    // --- MUST allow extra fields (..) ---
    assert!(
        schema.contains(".."),
        "v2.ncl NickelDocumentV2 must allow extra fields via .. for forward compat"
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
