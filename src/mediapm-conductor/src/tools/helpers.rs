//! Shared helpers for tool preset and provider modules.
//!
//! This module provides two flavors of OS-conditional selector for building
//! `${context.os == ... ? ... : ...}` template expressions:
//!
//! - **[`build_os_conditional_selector`]** — for **flat binary names** (no OS
//!   directory component in the path). The function prepends the OS label to
//!   each value: `{"linux": "sd-x86_64-linux"}` → `linux/sd-x86_64-linux`.
//!   Use for: command path selectors where the binary name itself is
//!   OS-specific (e.g. `yt-dlp_macos`, `ffmpeg_macos`).
//!
//! - **[`build_raw_os_conditional_selector`]** — for **pre-qualified paths**
//!   (OS directory component already present in the path). The function uses
//!   values as-is: `{"macos": "deps/ffmpeg/macos/ffmpeg"}` →
//!   `deps/ffmpeg/macos/ffmpeg`. Use for: inlined companion dep selectors
//!   where the content map key is `deps/{tool_id}/{os}/{filename}`.
//!
//! **Do not mix these up.** Using `build_os_conditional_selector` with
//! pre-qualified paths produces double-OS-prefixed results (e.g.
//! `macos/deps/ffmpeg/macos/ffmpeg`), which silently break runtime path
//! resolution.

use std::collections::BTreeMap;

// ── build_os_conditional_selector (flat binary names) ────────────────────

/// Builds a nested `${context.os == ... ? ... : ...}` selector for multi-OS
/// executables so each fallback branch is a fully wrapped template expression.
///
/// Each value in `per_os_exec` is a (possibly relative) executable path
/// **without** the OS prefix — the function prepends the OS label. Example:
/// `{"linux": "sd-x86_64-linux", "windows": "sd.exe"}` → nested templates
/// that resolve to `linux/sd-x86_64-linux` or `windows/sd.exe` at runtime.
///
/// When only one OS is present, collapses to plain `"linux/path"`.
///
/// **Do not use with pre-qualified paths** (e.g. `deps/ffmpeg/macos/ffmpeg`)
/// — use [`build_raw_os_conditional_selector`] instead.
fn build_nested_os_selector(entries: &[(String, String)]) -> String {
    match entries {
        [] => String::new(),
        [(os, path)] => format!("{os}/{path}"),
        [head, tail @ ..] => {
            let (os, path) = &head;
            let nested = build_nested_os_selector(tail);
            format!("${{context.os == \"{os}\" ? {os}/{path} : {nested}}}")
        }
    }
}

/// Builds a `${context.os == ... ? ... : ...}` selector for multi-OS
/// executables where values are **flat binary names** (no OS in path).
///
/// Prepends the OS label to each value:
/// `{"linux": "sd-x86_64-linux", "windows": "sd.exe"}` →
/// `${context.os == "linux" ? linux/sd-x86_64-linux : windows/sd.exe}`
///
/// For pre-qualified paths that already contain the OS directory component
/// (e.g. `deps/ffmpeg/macos/ffmpeg`), use
/// [`build_raw_os_conditional_selector`] instead.
#[must_use]
pub fn build_os_conditional_selector(per_os_exec: &BTreeMap<String, String>) -> String {
    if per_os_exec.is_empty() {
        return String::new();
    }
    let entries: Vec<(String, String)> =
        per_os_exec.iter().map(|(os, path)| (os.clone(), path.clone())).collect();
    build_nested_os_selector(&entries)
}

// ── build_raw_os_conditional_selector (pre-qualified paths) ──────────────

/// Recursive nested ternary builder for pre-qualified paths.
fn build_raw_nested_os_selector(entries: &[(String, String)]) -> String {
    match entries {
        [] => String::new(),
        [(_os, path)] => path.clone(),
        [(os, path), tail @ ..] => {
            let nested = build_raw_nested_os_selector(tail);
            format!("${{context.os == \"{os}\" ? {path} : {nested}}}")
        }
    }
}

/// Builds a `${context.os == ... ? ... : ...}` selector for multi-OS paths
/// where values are **pre-qualified** (OS directory component already present).
///
/// Unlike [`build_os_conditional_selector`], this function uses path values
/// as-is without prepending the OS label. Example:
/// `{"linux": "deps/ffmpeg/linux/ffmpeg", "macos": "deps/ffmpeg/macos/ffmpeg"}` →
/// `${context.os == "linux" ? deps/ffmpeg/linux/ffmpeg : deps/ffmpeg/macos/ffmpeg}`
///
/// Use this for inlined companion dep selectors where the content map key
/// follows the `deps/{tool_id}/{os}/{filename}` convention.
#[must_use]
pub fn build_raw_os_conditional_selector(per_os: &BTreeMap<String, String>) -> String {
    if per_os.is_empty() {
        return String::new();
    }
    let entries: Vec<(String, String)> =
        per_os.iter().map(|(os, path)| (os.clone(), path.clone())).collect();
    build_raw_nested_os_selector(&entries)
}

#[cfg(test)]
mod tests {
    use super::*;

    // --- build_os_conditional_selector (existing, verify still works) ---

    #[test]
    fn os_selector_single_entry_collapses_to_literal() {
        let result = build_os_conditional_selector(&BTreeMap::from([(
            "linux".to_string(),
            "sd-x86_64-linux".to_string(),
        )]));
        assert_eq!(result, "linux/sd-x86_64-linux");
    }

    #[test]
    fn os_selector_multiple_entries_nests_with_os_prefix() {
        let result = build_os_conditional_selector(&BTreeMap::from([
            ("linux".to_string(), "sd-x86_64-linux".to_string()),
            ("macos".to_string(), "sd-x86_64-macos".to_string()),
        ]));
        assert!(result.contains("linux/sd-x86_64-linux"), "must prepend OS: {result}");
        assert!(result.contains("macos/sd-x86_64-macos"), "must prepend OS: {result}");
        assert!(result.contains("${context.os == "), "must be nested template: {result}");
    }

    #[test]
    fn os_selector_empty_returns_empty() {
        assert_eq!(build_os_conditional_selector(&BTreeMap::new()), "");
    }

    // --- build_raw_os_conditional_selector (new) ---

    #[test]
    fn raw_os_selector_single_entry_collapses_to_path() {
        let result = build_raw_os_conditional_selector(&BTreeMap::from([(
            "macos".to_string(),
            "deps/ffmpeg/macos/ffmpeg".to_string(),
        )]));
        assert_eq!(result, "deps/ffmpeg/macos/ffmpeg");
    }

    #[test]
    fn raw_os_selector_multiple_entries_use_paths_as_is() {
        let result = build_raw_os_conditional_selector(&BTreeMap::from([
            ("linux".to_string(), "deps/ffmpeg/linux/ffmpeg".to_string()),
            ("macos".to_string(), "deps/ffmpeg/macos/ffmpeg".to_string()),
            ("windows".to_string(), "deps/ffmpeg/windows/ffmpeg.exe".to_string()),
        ]));
        // Paths must appear WITHOUT any OS prefix prepended
        assert!(result.contains("deps/ffmpeg/linux/ffmpeg"), "must contain literal path: {result}");
        assert!(result.contains("deps/ffmpeg/macos/ffmpeg"), "must contain literal path: {result}");
        assert!(
            result.contains("deps/ffmpeg/windows/ffmpeg.exe"),
            "must contain literal path: {result}"
        );
        // Must NOT contain double-prefixed paths
        assert!(!result.contains("linux/deps/"), "must not double-prefix: {result}");
        assert!(!result.contains("macos/deps/"), "must not double-prefix: {result}");
        assert!(!result.contains("windows/deps/"), "must not double-prefix: {result}");
        assert!(result.contains("${context.os == "), "must be nested template: {result}");
    }

    #[test]
    fn raw_os_selector_empty_returns_empty() {
        assert_eq!(build_raw_os_conditional_selector(&BTreeMap::new()), "");
    }

    // --- shared invariant: both helpers produce valid template expressions ---

    #[test]
    fn both_selectors_produce_balanced_template_expressions() {
        let os = build_os_conditional_selector(&BTreeMap::from([
            ("linux".to_string(), "a".to_string()),
            ("macos".to_string(), "b".to_string()),
            ("windows".to_string(), "c".to_string()),
        ]));
        let raw = build_raw_os_conditional_selector(&BTreeMap::from([
            ("linux".to_string(), "a".to_string()),
            ("macos".to_string(), "b".to_string()),
            ("windows".to_string(), "c".to_string()),
        ]));
        // Both must have balanced ${} and ternary syntax
        for (label, result) in [("os", &os), ("raw", &raw)] {
            let opens = result.matches("${").count();
            let closes = result.matches('}').count();
            assert_eq!(opens, closes, "{label} selector must have balanced braces: {result}");
            assert!(
                opens <= 2,
                "{label} selector must have at most 2 nested expressions: {result}"
            );
        }
    }
}
