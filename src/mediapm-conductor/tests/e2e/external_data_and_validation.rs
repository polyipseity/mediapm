//! End-to-end decode validation tests for external-data invariants and
//! required fields.
//!
//! Covers:
//! - COND-O.4: Missing external_data → validation error
//! - COND-O.6: Document version missing → parse error
//! - C3: Missing version test (integration level)

use mediapm_conductor::config::versions::decode_document;

/// A valid v2 document with external_data covering its content-map hashes.
const VALID_V2_DOC: &str = r#"{
    version = 2,
    tools = {
        "my-tool" = {
            kind = "executable",
            name = "my-tool",
            command = ["my-tool"],
            runtime = {
                content_map = {
                    "tool.bin" = "blake3:aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa",
                },
            },
        },
    },
    external_data = {
        "blake3:aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa" = {
            hash = "blake3:aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa",
            description = "test binary payload",
            save = true,
        },
    },
}"#;

/// A v2 document where a content-map hash is NOT declared in external_data.
const MISSING_EXTERNAL_DATA_DOC: &str = r#"{
    version = 2,
    tools = {
        "my-tool" = {
            kind = "executable",
            name = "my-tool",
            command = ["my-tool"],
            runtime = {
                content_map = {
                    "tool.bin" = "blake3:aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa",
                },
            },
        },
    },
    external_data = {},
}"#;

/// A document without the required `version` field.
const MISSING_VERSION_DOC: &str = r#"{
    tools = {
        "my-tool" = {
            kind = "executable",
            name = "my-tool",
            command = ["my-tool"],
        },
    },
}"#;

/// A document with a non-hash content-map value (inline description) and no
/// external_data — should decode successfully since inline values are exempt.
const NON_HASH_CONTENT_MAP_DOC: &str = r#"{
    version = 2,
    tools = {
        "echo" = {
            kind = "builtin",
            name = "echo",
            builtin_id = "echo@v1",
            runtime = {
                content_map = {
                    "readme.txt" = "inline-description-not-a-hash",
                },
            },
        },
    },
}"#;

/// Verifies that a valid v2 document with complete external_data decodes
/// successfully.
#[test]
fn valid_document_with_external_data_decodes() {
    let doc = decode_document(VALID_V2_DOC.as_bytes()).expect("valid doc should decode");
    assert_eq!(doc.tools.len(), 1, "should have one tool");
    assert!(doc.tools.contains_key("my-tool"));
    assert_eq!(doc.external_data.len(), 1, "should have one external_data entry");
}

/// Verifies that a document missing an external_data entry for a content-map
/// hash fails validation.
#[test]
fn missing_external_data_rejected_at_decode_time() {
    let err = decode_document(MISSING_EXTERNAL_DATA_DOC.as_bytes())
        .expect_err("missing external_data should produce error");
    let msg = format!("{err}");
    assert!(
        msg.contains("not in external_data"),
        "error should mention missing external_data: {msg}"
    );
}

/// Verifies that a document without a `version` field produces a parse error.
#[test]
fn document_without_version_field_produces_error() {
    let err = decode_document(MISSING_VERSION_DOC.as_bytes())
        .expect_err("missing version should produce error");
    let msg = format!("{err}");
    assert!(
        msg.contains("version") && msg.contains("field"),
        "error should mention missing version field: {msg}"
    );
}

/// Verifies that non-hash content-map values are exempt from the external_data
/// invariant.
#[test]
fn non_hash_content_map_values_do_not_require_external_data() {
    let doc = decode_document(NON_HASH_CONTENT_MAP_DOC.as_bytes())
        .expect("non-hash content_map should decode OK");
    assert!(doc.tools.contains_key("echo"), "echo tool should be present");
}
