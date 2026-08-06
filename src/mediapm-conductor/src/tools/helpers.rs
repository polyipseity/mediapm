//! Shared helpers for tool preset and provider modules.
//!
//! This module provides utility functions used by both the provider pipeline
//! (downloading and CAS-importing tool payloads) and the preset layer
//! (building [`ToolSpec`](crate::ToolSpec) / [`ToolRuntime`](crate::ToolRuntime) contracts).

use std::collections::BTreeMap;

/// Builds a nested `${context.os == ... ? ... : ...}` selector for multi-OS
/// executables so each fallback branch is a fully wrapped template expression.
///
/// Each value in `per_os_exec` is a (possibly relative) executable path
/// without the OS prefix — the function prepends the OS label. Example:
/// `{"linux": "sd-x86_64-linux", "windows": "sd.exe"}` → nested templates
/// that resolve to `linux/sd-x86_64-linux` or `windows/sd.exe` at runtime.
///
/// When only one OS is present, collapses to plain `"linux/path"`.
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
/// executables.
///
/// # Panics
///
/// Panics if `per_os_exec` is empty (unreachable because the function returns
/// early for empty maps).
#[must_use]
pub fn build_os_conditional_selector(per_os_exec: &BTreeMap<String, String>) -> String {
    if per_os_exec.is_empty() {
        return String::new();
    }
    let entries: Vec<(String, String)> =
        per_os_exec.iter().map(|(os, path)| (os.clone(), path.clone())).collect();
    build_nested_os_selector(&entries)
}
