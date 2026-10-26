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
//!   via [`registered_builtin_ids`]. A registration gated on the
//!   `test-builtins` feature is a test double: a build that turns that feature
//!   on compiles it and resolves it by name, and it never appears in a
//!   conductor document. The compile gate on an entry and the
//!   [`BuiltinRegistration::test_only`] flag answer different questions — the
//!   gate decides whether a build contains the entry at all, the flag whether a
//!   build that has it may write or keep it — so a `cfg(test)`-only entry would
//!   be invisible to integration-test crates while a gate production builds
//!   also enable would leak the double into shipped binaries.

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
    /// A test-only entry is compiled into the crate under the `test-builtins`
    /// feature so workflow tests can dispatch to it by name, and registration
    /// skips it so it never reaches a user's document. The compile gate that
    /// admits an entry and this flag answer different questions: the gate
    /// decides whether the entry exists in a given build at all
    /// (integration-test crates compile the library without `cfg(test)`, so a
    /// `cfg(test)`-only entry would be invisible to them), while the flag
    /// decides whether a build that has the entry may write it into a document.
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
    #[cfg(any(test, feature = "test-builtins"))]
    BuiltinRegistration {
        id: "builtins.flaky@v1",
        name: "flaky",
        builtin_id: "flaky@v1",
        is_impure: false,
        test_only: true,
        summary: "test-only builtin that fails its first N invocations then succeeds",
    },
    #[cfg(any(test, feature = "test-builtins"))]
    BuiltinRegistration {
        id: "builtins.slow@v1",
        name: "slow",
        builtin_id: "slow@v1",
        is_impure: false,
        test_only: true,
        summary: "test-only builtin that records dispatch events and can hold until an awaited line appears",
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

/// Iterates the builtin registrations a conductor document may carry.
///
/// Every site that needs the production set takes it here: the writer that
/// registers builtins, the pass that applies their runtime defaults, the prune
/// that keeps their entries, and [`registered_production_builtin_ids`]. Keeping
/// the predicate here is what holds those sites in step: a copy of it elsewhere
/// can be left behind when a site is added, and that is how a `test_only` double
/// reaches a document.
pub fn production_builtins() -> impl Iterator<Item = &'static BuiltinRegistration> {
    ALL_BUILTINS.iter().filter(|entry| !entry.test_only)
}

/// Returns the builtin ids a conductor document may carry, in registry order.
///
/// Derived from [`production_builtins`], so a builtin added to [`ALL_BUILTINS`]
/// appears here without a second edit and a [`BuiltinRegistration::test_only`]
/// double never does. The writer that inserts builtin entries enumerates
/// [`production_builtins`] directly; this is the flat list for callers that need
/// ids rather than registrations.
#[must_use]
pub fn registered_production_builtin_ids() -> Vec<String> {
    production_builtins().map(|entry| entry.builtin_id.to_string()).collect()
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

    /// Production membership follows the flag, so a build that carries a
    /// double still offers writers only the real builtins.
    #[test]
    fn production_ids_exclude_test_only_entries() {
        let production: Vec<&str> = production_builtins().map(|entry| entry.builtin_id).collect();
        for entry in ALL_BUILTINS {
            assert_eq!(
                production.contains(&entry.builtin_id),
                !entry.test_only,
                "{} production membership must follow its test_only flag",
                entry.builtin_id
            );
        }
    }

    /// Verifies the document-facing registry is exactly the five real builtins
    /// in every build, whatever doubles this one carries.
    #[test]
    fn strict_the_production_registry_holds_exactly_the_five_real_builtins() {
        assert_eq!(
            registered_production_builtin_ids(),
            vec![
                "archive@v1".to_string(),
                "echo@v1".to_string(),
                "export@v1".to_string(),
                "fs@v1".to_string(),
                "import@v1".to_string(),
            ],
        );
    }

    /// Verifies a test build carries both doubles and can resolve them by
    /// name, which is what the conductor integration suites dispatch to.
    #[cfg(feature = "test-builtins")]
    #[test]
    fn a_test_build_resolves_both_doubles_by_name() {
        let doubles: Vec<&str> = ALL_BUILTINS
            .iter()
            .filter(|entry| entry.test_only)
            .map(|entry| entry.builtin_id)
            .collect();
        assert_eq!(doubles, vec!["flaky@v1", "slow@v1"]);
        let ids = registered_builtin_ids();
        assert_eq!(ids.len(), 7, "five real builtins plus two doubles: {ids:?}");
        for id in ["flaky@v1", "slow@v1"] {
            assert!(ids.contains(id), "a test build must resolve {id} by name");
        }
    }
}
