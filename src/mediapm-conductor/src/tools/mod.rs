//! Tool presets, provider, builtins, and builtin registry.
//!
//! This module organizes every tool preset and builtin tool that conductor
//! can discover or invoke.
//!
//! - **Tool presets** (feature `tool-presets`) — managed executable tool
//!   preset configuration via [`preset`] and 3-phase provisioning via
//!   [`provider`].
//! - **Builtins** — always-compiled crates (`echo`, `fs`, `import`,
//!   `archive`, `export`) registered in [`ALL_BUILTINS`] and discoverable
//!   via [`registered_builtin_ids`].

pub mod helpers;
pub mod preset;
pub mod provider;

use std::collections::HashSet;

// ---------------------------------------------------------------------------
// Builtin registry
// ---------------------------------------------------------------------------

/// Static metadata for a registered builtin tool in this crate.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct BuiltinRegistration {
    /// Canonical qualified tool identifier (e.g. `"builtins.echo@v1"`).
    pub id: &'static str,
    /// Short tool name (e.g. `"echo"`).
    pub name: &'static str,
    /// Versioned builtin identifier (e.g. `"echo@v1"`).
    pub builtin_id: &'static str,
    /// Whether this tool produces side effects.
    pub is_impure: bool,
    /// Human-readable one-line description.
    pub summary: &'static str,
}

/// All builtin tools registered in this crate.
pub const ALL_BUILTINS: &[BuiltinRegistration] = &[
    BuiltinRegistration {
        id: mediapm_conductor_builtin_archive::TOOL_ID,
        name: mediapm_conductor_builtin_archive::TOOL_NAME,
        builtin_id: mediapm_conductor_builtin_archive::TOOL_BUILTIN_ID,
        is_impure: false,
        summary: "pure archive builtin runtime transforming bytes to bytes",
    },
    BuiltinRegistration {
        id: mediapm_conductor_builtin_echo::TOOL_ID,
        name: mediapm_conductor_builtin_echo::TOOL_NAME,
        builtin_id: mediapm_conductor_builtin_echo::TOOL_BUILTIN_ID,
        is_impure: false,
        summary: "echo-like builtin returning text as stdout/stderr string-map",
    },
    BuiltinRegistration {
        id: mediapm_conductor_builtin_export::TOOL_ID,
        name: mediapm_conductor_builtin_export::TOOL_NAME,
        builtin_id: mediapm_conductor_builtin_export::TOOL_BUILTIN_ID,
        is_impure: true,
        summary: "export builtin runtime that writes file/folder payloads to host paths",
    },
    BuiltinRegistration {
        id: mediapm_conductor_builtin_fs::TOOL_ID,
        name: mediapm_conductor_builtin_fs::TOOL_NAME,
        builtin_id: mediapm_conductor_builtin_fs::TOOL_BUILTIN_ID,
        is_impure: true,
        summary: "filesystem operation builtin runtime with impure side-effecting behavior",
    },
    BuiltinRegistration {
        id: mediapm_conductor_builtin_import::TOOL_ID,
        name: mediapm_conductor_builtin_import::TOOL_NAME,
        builtin_id: mediapm_conductor_builtin_import::TOOL_BUILTIN_ID,
        is_impure: true,
        summary: "import builtin that ingests file/folder/fetch/cas_hash sources into pure bytes",
    },
];

/// Returns the set of registered builtin tool IDs.
///
/// Each builtin is identified by its versioned `builtin_id` (e.g. `"echo@v1"`).
#[must_use]
pub fn registered_builtin_ids() -> HashSet<String> {
    ALL_BUILTINS.iter().map(|e| e.builtin_id.to_string()).collect()
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn all_builtins_are_listed_in_registered_ids() {
        let ids = registered_builtin_ids();
        for entry in ALL_BUILTINS {
            assert!(ids.contains(entry.builtin_id), "missing: {}", entry.builtin_id);
        }
        assert_eq!(ids.len(), ALL_BUILTINS.len());
    }
}
