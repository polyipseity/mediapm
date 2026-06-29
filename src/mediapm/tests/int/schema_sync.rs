//! Sync-prevention tests for the mediapm Nickel schema (`v1.ncl`).
//!
//! These tests validate that the V1 Nickel schema definition stays in sync
//! with the Rust deserialization types (`MediaPmDocument`,
//! `MediaPmDocumentEnvelopeV1`).  If the Rust structs gain or lose fields,
//! the Nickel schema must be updated correspondingly, and these tests force
//! that update to be deliberate.

use mediapm::MediaPmDocument;

/// Validates that the Rust struct serialization shape matches the expected
/// V1 schema contract invariants.
#[test]
fn mediapm_document_serialization_invariants() {
    let doc = MediaPmDocument::default();
    let json = serde_json::to_value(&doc).unwrap();
    let obj = json.as_object().expect("MediaPmDocument must serialize to a JSON object");

    // --- MUST be present at top level ---
    assert!(
        obj.contains_key("version"),
        "version must be a top-level field in MediaPmDocument (must be in V1 schema)"
    );
    assert!(
        obj.contains_key("media"),
        "media must be a top-level field in MediaPmDocument (must be in V1 schema)"
    );
    assert!(
        obj.contains_key("hierarchy"),
        "hierarchy must be a top-level field in MediaPmDocument (must be in V1 schema)"
    );
    assert!(
        obj.contains_key("tools"),
        "tools must be a top-level field in MediaPmDocument (must be in V1 schema)"
    );
    assert!(
        obj.contains_key("runtime"),
        "runtime must be a top-level field in MediaPmDocument (must be in V1 schema)"
    );

    // --- MUST NOT be present at top level ---
    assert!(
        !obj.contains_key("conductor"),
        "conductor must NOT be a top-level field in MediaPmDocument (removed from V1 schema)"
    );

    // --- tools MUST NOT be inside runtime ---
    if let Some(runtime) = obj.get("runtime").and_then(|v| v.as_object()) {
        assert!(
            !runtime.contains_key("tools"),
            "tools must NOT be inside runtime in MediaPmDocument (tools moved to top level)"
        );
    }
}

/// Validates that the V1 Nickel schema (`v1.ncl`) contains the expected
/// contracts and omits removed ones.
#[test]
fn v1_nickel_schema_structure() {
    let schema = include_str!("../../src/config/versions/v1.ncl");

    // --- MUST define ToolRequirementV1 contract ---
    assert!(
        schema.contains("let ToolRequirementV1 = {"),
        "v1.ncl must define ToolRequirementV1 contract for the top-level tools field"
    );
    assert!(
        schema.contains("version | String | { .. } | optional,"),
        "ToolRequirementV1 must have version field"
    );
    assert!(schema.contains("dependencies | {"), "ToolRequirementV1 must have dependencies field");

    // --- MUST have top-level tools with ToolRequirementV1 ---
    assert!(
        schema.contains("tools | { : ToolRequirementV1 } | default = {}"),
        "v1.ncl must declare top-level tools using ToolRequirementV1"
    );

    // --- MUST NOT have conductor field ---
    assert!(
        !schema.contains("conductor | { .. } | optional"),
        "v1.ncl must NOT have a conductor field"
    );
    assert!(
        !schema.contains("conductor |"),
        "v1.ncl must NOT have any conductor field (double-check)"
    );

    // --- MUST NOT have tools inside runtime ---
    assert!(
        !schema.contains("tools | { .. } | optional"),
        "v1.ncl must NOT have tools inside runtime (tools is now top-level)"
    );
}
