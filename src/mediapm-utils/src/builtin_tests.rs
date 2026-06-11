//! Tests for shared builtin helpers.
//!
//! These tests exercise [`crate::builtin::describe`],
//! [`crate::builtin::describe_json_compact`],
//! [`crate::builtin::describe_json_compat`], and
//! [`crate::builtin::validate_only_known_keys`] directly.

use crate::StringMap;
use crate::builtin::{
    describe, describe_json_compact, describe_json_compat, validate_only_known_keys,
};

/// Verifies `describe` returns all five expected keys.
#[test]
fn describe_contains_all_keys() {
    let result = describe("tool-1", "Tool One", "1.0.0", false, "A test tool.");
    assert_eq!(result.len(), 5);
    assert!(result.contains_key("tool_id"));
    assert!(result.contains_key("tool_name"));
    assert!(result.contains_key("tool_version"));
    assert!(result.contains_key("is_impure"));
    assert!(result.contains_key("summary"));
}

/// Verifies `describe` returns correct values.
#[test]
fn describe_correct_values() {
    let result = describe("t1", "Test", "2.0", true, "Impure tool.");
    assert_eq!(result.get("tool_id"), Some(&"t1".to_string()));
    assert_eq!(result.get("tool_name"), Some(&"Test".to_string()));
    assert_eq!(result.get("tool_version"), Some(&"2.0".to_string()));
    assert_eq!(result.get("is_impure"), Some(&"true".to_string()));
    assert_eq!(result.get("summary"), Some(&"Impure tool.".to_string()));
}

/// Verifies `describe_json_compact` output contains all key-value pairs.
#[test]
fn describe_json_compact_contains_keys() {
    let json = describe_json_compact("echo", "Echo", "0.1.0", false, "Echoes input");
    assert!(json.contains(r#""tool_id": "echo""#));
    assert!(json.contains(r#""tool_name": "Echo""#));
    assert!(json.contains(r#""tool_version": "0.1.0""#));
    assert!(json.contains(r#""is_impure": "false""#));
    assert!(json.contains(r#""summary": "Echoes input""#));
}

/// Verifies `describe_json_compact` uses 2-space indentation.
#[test]
fn describe_json_compact_indentation() {
    let json = describe_json_compact("x", "X", "1", false, ".");
    // Each line (except first/last) should start with two spaces.
    for line in json.lines().skip(1) {
        if line == "}" {
            continue;
        }
        assert!(line.starts_with("  "), "line should be indented: {line:?}");
    }
}

/// Verifies `describe_json_compat` returns the same as compact.
#[test]
fn describe_json_compat_matches_compact() {
    let compat = describe_json_compat("t", "T", "1", true, "Compat test");
    let compact = describe_json_compact("t", "T", "1", true, "Compat test");
    assert_eq!(compat, compact);
}

/// Verifies `validate_only_known_keys` passes for an empty params map.
#[test]
fn validate_only_known_keys_accepts_empty() {
    let params = StringMap::new();
    let known = &["foo", "bar"];
    assert!(validate_only_known_keys(&params, known, "test").is_ok());
}

/// Verifies `validate_only_known_keys` passes when all keys are known.
#[test]
fn validate_only_known_keys_accepts_known() {
    let params = StringMap::from([
        ("foo".to_string(), "1".to_string()),
        ("bar".to_string(), "2".to_string()),
    ]);
    let known = &["foo", "bar", "baz"];
    assert!(validate_only_known_keys(&params, known, "test").is_ok());
}

/// Verifies `validate_only_known_keys` passes with a subset of known keys.
#[test]
fn validate_only_known_keys_accepts_subset() {
    let params = StringMap::from([("foo".to_string(), "1".to_string())]);
    let known = &["foo", "bar"];
    assert!(validate_only_known_keys(&params, known, "test").is_ok());
}

/// Verifies `validate_only_known_keys` rejects an unknown key.
#[test]
fn validate_only_known_keys_rejects_unknown() {
    let params = StringMap::from([
        ("foo".to_string(), "1".to_string()),
        ("unknown".to_string(), "x".to_string()),
    ]);
    let known = &["foo", "bar"];
    let err =
        validate_only_known_keys(&params, known, "test").expect_err("should reject unknown key");
    assert!(err.contains("unknown"), "error should mention unknown key: {err}");
}

/// Verifies `validate_only_known_keys` context is included in error.
#[test]
fn validate_only_known_keys_includes_context() {
    let params = StringMap::from([("bad".to_string(), "x".to_string())]);
    let err =
        validate_only_known_keys(&params, &["good"], "my_operation").expect_err("should fail");
    assert!(err.contains("my_operation"), "error should include context: {err}");
}
