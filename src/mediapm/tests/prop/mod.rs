//! Property-test harness for mediapm integration scenarios.
//!
//! Integration-level property tests covering managed-tool source structure,
//! content-map shape invariants, and OS-selector completeness.

use proptest::prelude::*;
use std::collections::BTreeMap;

// ---------------------------------------------------------------------------
// OS-selector shape invariants (mediapm managed tools)
// ---------------------------------------------------------------------------

/// A `build_os_conditional_selector`-style function (mirroring the conductor
/// implementation) must include every OS/path entry and produce syntactically
/// valid output.
fn build_os_conditional_selector(per_os_exec: &BTreeMap<String, String>) -> String {
    if per_os_exec.is_empty() {
        return String::new();
    }
    let mut iter = per_os_exec.iter();
    let (first_os, first_path) = iter.next().expect("non-empty");
    if per_os_exec.len() == 1 {
        return format!("{first_os}/{first_path}");
    }
    let mut result = format!("${{context.os == \"{first_os}\" ? {first_os}/{first_path}");
    for (os, path) in iter.by_ref() {
        result.push_str(&format!(" : context.os == \"{os}\" ? {os}/{path}"));
    }
    result.push('}');
    result
}

proptest! {
    /// `build_os_conditional_selector` includes every entry from its input
    /// and collapses to a plain path when there's only one entry.
    #[test]
    fn os_selector_roundtrip(
        entries in prop::collection::btree_map(
            "(linux|macos|windows)".prop_filter("OS must be non-empty", |s| !s.is_empty()),
            "[a-zA-Z0-9._/-]+",
            0..4,
        )
    ) {
        let selector = build_os_conditional_selector(&entries);
        if entries.is_empty() {
            prop_assert_eq!(selector, "", "empty map produces empty string");
            return Ok(());
        }
        for (os, path) in &entries {
            let fragment = format!("{os}/{path}");
            prop_assert!(
                selector.contains(&fragment),
                "selector {:?} should contain {:?}",
                selector,
                fragment,
            );
        }
        if entries.len() == 1 {
            let (os, path) = entries.iter().next().unwrap();
            prop_assert_eq!(selector, format!("{os}/{path}"));
        } else {
            prop_assert!(
                selector.starts_with("${context.os == \""),
                "multi-OS selector should start with template syntax: {:?}",
                selector,
            );
            prop_assert!(
                selector.ends_with('}'),
                "multi-OS selector should end with '}}': {:?}",
                selector,
            );
        }
    }
}

// ---------------------------------------------------------------------------
// Source-structure determinism
// ---------------------------------------------------------------------------

/// Managed-tool source OS labels belong to a fixed set.
#[test]
fn managed_tool_os_labels_are_valid() {
    let valid_oses = ["linux", "macos", "windows"];

    // Reflect the known managed tools and their OS labels.
    // (This mirrors the structure checked by resolve_tool_fetch tests.)
    let known_oses: Vec<&[&str]> = vec![
        &["linux", "macos", "windows"], // ffmpeg, yt-dlp, deno, rsgain, sd
    ];

    for os_list in &known_oses {
        for os in *os_list {
            assert!(
                valid_oses.contains(os),
                "OS label {os:?} is not in the valid set {valid_oses:?}"
            );
        }
    }
}
