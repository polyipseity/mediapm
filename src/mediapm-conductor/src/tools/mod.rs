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
//!   via [`registered_builtin_ids`]. A registration marked
//!   [`BuiltinRegistration::test_only`] is a test double: it is compiled and
//!   resolvable by name, and it never appears in a conductor document.

pub mod helpers;
pub mod preset;
pub mod provider;
#[cfg(feature = "tool-presets")]
pub mod spec;

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
    /// Whether this entry is a test double rather than a production builtin.
    ///
    /// A test-only entry is compiled into the crate so workflow tests can
    /// dispatch to it by name, and registration skips it so it never reaches a
    /// user's document. The compile gate that admits an entry and this flag
    /// answer different questions: the gate decides whether the entry exists in
    /// a given build at all (integration-test crates compile the library
    /// without `cfg(test)`, so a `cfg(test)`-only entry would be invisible to
    /// them), while the flag decides whether a build that has the entry may
    /// write it into a document.
    pub test_only: bool,
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
        test_only: false,
        summary: "pure archive builtin runtime transforming bytes to bytes",
    },
    BuiltinRegistration {
        id: mediapm_conductor_builtin_echo::TOOL_ID,
        name: mediapm_conductor_builtin_echo::TOOL_NAME,
        builtin_id: mediapm_conductor_builtin_echo::TOOL_BUILTIN_ID,
        is_impure: false,
        test_only: false,
        summary: "echo-like builtin returning text as stdout/stderr string-map",
    },
    BuiltinRegistration {
        id: mediapm_conductor_builtin_export::TOOL_ID,
        name: mediapm_conductor_builtin_export::TOOL_NAME,
        builtin_id: mediapm_conductor_builtin_export::TOOL_BUILTIN_ID,
        is_impure: true,
        test_only: false,
        summary: "export builtin runtime that writes file/folder payloads to host paths",
    },
    BuiltinRegistration {
        id: mediapm_conductor_builtin_fs::TOOL_ID,
        name: mediapm_conductor_builtin_fs::TOOL_NAME,
        builtin_id: mediapm_conductor_builtin_fs::TOOL_BUILTIN_ID,
        is_impure: true,
        test_only: false,
        summary: "filesystem operation builtin runtime with impure side-effecting behavior",
    },
    BuiltinRegistration {
        id: mediapm_conductor_builtin_import::TOOL_ID,
        name: mediapm_conductor_builtin_import::TOOL_NAME,
        builtin_id: mediapm_conductor_builtin_import::TOOL_BUILTIN_ID,
        is_impure: true,
        test_only: false,
        summary: "import builtin that ingests file/folder/fetch/cas_hash sources into pure bytes",
    },
    #[cfg(any(test, feature = "progress"))]
    BuiltinRegistration {
        id: "builtins.flaky@v1",
        name: "flaky",
        builtin_id: "flaky@v1",
        is_impure: false,
        test_only: true,
        summary: "test-only builtin that fails its first N invocations then succeeds",
    },
];

/// Returns the set of registered builtin tool IDs.
///
/// Each builtin is identified by its versioned `builtin_id` (e.g. `"echo@v1"`).
/// The set covers every entry, including a [`BuiltinRegistration::test_only`]
/// double, because it answers what this build can resolve by name.
#[must_use]
pub fn registered_builtin_ids() -> HashSet<String> {
    ALL_BUILTINS.iter().map(|e| e.builtin_id.to_string()).collect()
}

/// Returns the builtin ids a conductor document may carry, in registry order.
///
/// Conductor-document writers take their builtin list from here rather than
/// restating it, so a builtin added to [`ALL_BUILTINS`] reaches every writer at
/// once and a [`BuiltinRegistration::test_only`] double reaches none of them.
#[must_use]
pub fn registered_production_builtin_ids() -> Vec<String> {
    ALL_BUILTINS
        .iter()
        .filter(|entry| !entry.test_only)
        .map(|entry| entry.builtin_id.to_string())
        .collect()
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

    /// Production membership follows the flag, so a build that carries the
    /// `flaky` double still offers writers only the five real builtins.
    #[test]
    fn production_ids_exclude_test_only_entries() {
        let production = registered_production_builtin_ids();
        for entry in ALL_BUILTINS {
            assert_eq!(
                production.iter().any(|id| id == entry.builtin_id),
                !entry.test_only,
                "{} production membership must follow its test_only flag",
                entry.builtin_id
            );
        }
        assert!(
            ALL_BUILTINS.iter().any(|entry| entry.test_only),
            "the registry carries the flaky double, so this test would prove nothing without it"
        );
    }
}
