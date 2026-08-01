//! Shared helpers for tool preset and provider modules.
//!
//! This module provides utility functions used by both the provider pipeline
//! (downloading and CAS-importing tool payloads) and the preset layer
//! (building [`ToolSpec`](crate::ToolSpec) / [`ToolRuntime`](crate::ToolRuntime) contracts).

use std::collections::BTreeMap;
use std::fmt::Write;

/// Builds a `${context.os == "linux" ? linux/path : ...}` template expression
/// for the per-OS executable path map.
///
/// Each value in `per_os_exec` is a (possibly relative) executable path
/// without the OS prefix — the function prepends the OS label. Example:
/// `{"linux": "sd-x86_64-linux", "windows": "sd.exe"}` → produces
/// `${context.os == "linux" ? linux/sd-x86_64-linux : context.os == "windows" ? windows/sd.exe}`
///
/// When only one OS is present, collapses to plain `"linux/path"`.
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
    let mut iter = per_os_exec.iter();
    let (first_os, first_path) = iter.next().expect("non-empty per_os_exec");
    if per_os_exec.len() == 1 {
        return format!("{first_os}/{first_path}");
    }
    let mut result = format!("${{context.os == \"{first_os}\" ? {first_os}/{first_path}");
    for (os, path) in iter {
        let _ = write!(result, " : context.os == \"{os}\" ? {os}/{path}");
    }
    result.push('}');
    result
}
