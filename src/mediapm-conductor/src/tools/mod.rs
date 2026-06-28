//! Tool presets, builtins, and builtin registry.
//!
//! This module organizes every tool preset and builtin tool that conductor
//! can discover or invoke.
//!
//! - **Tool presets** (feature `tool-presets`) — source-fetched common
//!   executables such as `sd`.
//! - **Builtins** — always-compiled crates (`echo`, `fs`, `import`,
//!   `archive`, `export`) registered in [`ALL_BUILTINS`] and discoverable
//!   via [`registered_builtin_ids`].

#[cfg(feature = "tool-presets")]
pub mod preset_sd;

#[cfg(all(feature = "cli", feature = "tool-presets"))]
use clap::ValueEnum;

#[cfg(feature = "tool-presets")]
use crate::error::ConductorError;

use std::collections::HashSet;

// ---------------------------------------------------------------------------
// Builtin registry
// ---------------------------------------------------------------------------

/// Static metadata for a registered builtin tool in this crate.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct BuiltinRegistration {
    /// Canonical qualified tool identifier (e.g. `"builtins.echo@1.0.0"`).
    pub id: &'static str,
    /// Short tool name (e.g. `"echo"`).
    pub name: &'static str,
    /// Semver version string.
    pub version: &'static str,
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
        version: mediapm_conductor_builtin_archive::TOOL_VERSION,
        is_impure: false,
        summary: "pure archive builtin runtime transforming bytes to bytes",
    },
    BuiltinRegistration {
        id: mediapm_conductor_builtin_echo::TOOL_ID,
        name: mediapm_conductor_builtin_echo::TOOL_NAME,
        version: mediapm_conductor_builtin_echo::TOOL_VERSION,
        is_impure: false,
        summary: "echo-like builtin returning text as stdout/stderr string-map",
    },
    BuiltinRegistration {
        id: mediapm_conductor_builtin_export::TOOL_ID,
        name: mediapm_conductor_builtin_export::TOOL_NAME,
        version: mediapm_conductor_builtin_export::TOOL_VERSION,
        is_impure: true,
        summary: "export builtin runtime that writes file/folder payloads to host paths",
    },
    BuiltinRegistration {
        id: mediapm_conductor_builtin_fs::TOOL_ID,
        name: mediapm_conductor_builtin_fs::TOOL_NAME,
        version: mediapm_conductor_builtin_fs::TOOL_VERSION,
        is_impure: true,
        summary: "filesystem operation builtin runtime with impure side-effecting behavior",
    },
    BuiltinRegistration {
        id: mediapm_conductor_builtin_import::TOOL_ID,
        name: mediapm_conductor_builtin_import::TOOL_NAME,
        version: mediapm_conductor_builtin_import::TOOL_VERSION,
        is_impure: true,
        summary: "import builtin that ingests file/folder/fetch/cas_hash sources into pure bytes",
    },
];

/// Returns the set of registered builtin tool IDs.
///
/// Each builtin is identified by its canonical `name` (e.g. `"echo"`).
/// The version is not part of the ID; version matching is the caller's
/// responsibility.
#[must_use]
pub fn registered_builtin_ids() -> HashSet<String> {
    ALL_BUILTINS.iter().map(|e| e.name.to_string()).collect()
}

// ---------------------------------------------------------------------------
// Common executable tool presets (feature-gated)
// ---------------------------------------------------------------------------

/// Common executable tools that conductor can source directly from upstream.
///
/// This enum starts with a minimal set (`Sd`) and can grow as additional
/// frequently-used helper tools are standardized.
#[cfg(feature = "tool-presets")]
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
#[cfg_attr(all(feature = "cli", feature = "tool-presets"), derive(ValueEnum))]
pub enum CommonExecutableTool {
    /// Stream editor fetched from official GitHub release assets.
    Sd,
}

#[cfg(feature = "tool-presets")]
impl CommonExecutableTool {
    /// Returns the canonical logical tool name used in machine config.
    #[must_use]
    pub const fn logical_tool_name(self) -> &'static str {
        match self {
            Self::Sd => preset_sd::LOGICAL_TOOL_NAME,
        }
    }

    /// Returns the expected executable file name produced by installation.
    #[must_use]
    pub fn executable_file_name(self) -> String {
        match self {
            Self::Sd => preset_sd::executable_file_name(),
        }
    }
}

/// Installs one common executable tool from source and returns binary bytes.
///
/// # Errors
///
/// Returns [`ConductorError`] when installation fails or the executable
/// payload cannot be materialized.
#[cfg(feature = "tool-presets")]
pub fn fetch_common_executable_tool_payload(
    tool: CommonExecutableTool,
) -> Result<preset_sd::CommonExecutablePayload, ConductorError> {
    match tool {
        CommonExecutableTool::Sd => preset_sd::fetch_payload(),
    }
}

#[cfg(test)]
mod tests {
    #[cfg(feature = "tool-presets")]
    use super::CommonExecutableTool;

    /// Protects stable tool-preset selector metadata for release downloads.
    #[cfg(feature = "tool-presets")]
    #[test]
    fn common_sd_tool_selector_fields_are_stable() {
        assert_eq!(CommonExecutableTool::Sd.logical_tool_name(), "mediapm-conductor.tools.sd");
        let executable_name = CommonExecutableTool::Sd.executable_file_name();
        assert!(executable_name.starts_with("sd"));
    }

    /// Every registered builtin has its name present in the ID set.
    #[test]
    fn all_builtins_are_listed_in_registered_ids() {
        let ids = super::registered_builtin_ids();
        for entry in super::ALL_BUILTINS {
            assert!(ids.contains(entry.name), "missing: {}", entry.name);
        }
        assert_eq!(ids.len(), super::ALL_BUILTINS.len());
    }
}
