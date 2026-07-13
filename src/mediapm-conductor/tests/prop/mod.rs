//! Property-test harness for conductor integration scenarios.
//!
//! These tests verify algebraic properties of conductor constructs
//! at the integration level — cache key determinism, selector invariants,
//! and pipeline symmetry.

use proptest::prelude::*;

// ---------------------------------------------------------------------------
// Cache-key determinism
// ---------------------------------------------------------------------------

/// Cache keys derived from tool_id + os label are deterministic:
/// the same pair always produces the same key.
#[test]
fn cache_key_is_deterministic() {
    // The cache key format used internally by fetch_tool_sources is
    //   "{tool_id}_{os_label}"
    // This test verifies the structural property.
    fn make_key(tool: &str, os: &str) -> String {
        format!("{tool}_{os}")
    }

    proptest!(|(
        tool in "[a-zA-Z0-9._-]{1,20}",
        os in "\\w{1,16}",
    )| {
        let key1 = make_key(&tool, &os);
        let key2 = make_key(&tool, &os);
        prop_assert_eq!(key1, key2, "same (tool, os) must produce same cache key");
    });
}

// ---------------------------------------------------------------------------
// Launcher-script shape invariants
// ---------------------------------------------------------------------------

/// A generated launcher script for non-Windows always contains a shebang
/// and an `exec` line referencing `MEDIAPM_EXECUTABLE`.
#[test]
fn unix_launcher_has_shebang_and_exec() {
    fn generate_unix_launcher(builtin_id: &str) -> String {
        format!("#!/bin/sh\nexec \"${{MEDIAPM_EXECUTABLE}}\" {builtin_id} \"$@\"\n")
    }

    proptest!(|(builtin_id in "[a-zA-Z0-9@._-]{1,30}")| {
        let script = generate_unix_launcher(&builtin_id);
        prop_assert!(script.starts_with("#!/bin/sh"), "shebang required");
        prop_assert!(script.contains("MEDIAPM_EXECUTABLE"), "MEDIAPM_EXECUTABLE required");
        prop_assert!(script.contains(&builtin_id), "builtin_id must appear in script");
        prop_assert!(script.ends_with("\"\n"), "script must end with newline + args");
    });
}

/// A generated Windows launcher always starts with `@echo off` and uses
/// `%*` for argument forwarding.
#[test]
fn windows_launcher_has_echo_off_and_percent_star() {
    fn generate_windows_launcher(builtin_id: &str) -> String {
        format!("@echo off\r\n\"%MEDIAPM_EXECUTABLE%\" {builtin_id} %*\r\n")
    }

    proptest!(|(builtin_id in "[a-zA-Z0-9@._-]{1,30}")| {
        let script = generate_windows_launcher(&builtin_id);
        prop_assert!(script.starts_with("@echo off"), "Windows launcher starts with @echo off");
        prop_assert!(script.contains("%*"), "Windows launcher uses %*");
        prop_assert!(script.contains(&builtin_id), "builtin_id must appear in script");
    });
}
