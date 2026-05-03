//! Common executable tool presets and download infrastructure managed by
//! conductor.
//!
//! Each preset must live in its own module file so the preset catalog can grow
//! cleanly without turning one API file into an implementation monolith.
//!
//! The `downloader` sub-module provides the CAS-backed download cache engine
//! and default cache-root helpers shared with `mediapm`.

pub mod downloader;
pub mod sd;

use clap::ValueEnum;

use crate::error::ConductorError;

/// Common executable tools that conductor can source directly from upstream.
///
/// This enum intentionally starts with a minimal set (`sd`) and can grow as
/// additional frequently-used helper tools are standardized.
#[derive(Debug, Clone, Copy, PartialEq, Eq, ValueEnum)]
pub enum CommonExecutableTool {
    /// Stream editor fetched from official GitHub release assets.
    ///
    /// Use as the default cross-platform string-manipulation helper in
    /// conductor workflows/config rewrites.
    Sd,
}

impl CommonExecutableTool {
    /// Returns the canonical logical tool name used in machine config.
    #[must_use]
    pub const fn logical_tool_name(self) -> &'static str {
        match self {
            Self::Sd => sd::LOGICAL_TOOL_NAME,
        }
    }

    /// Returns the expected executable file name produced by installation.
    #[must_use]
    pub fn executable_file_name(self) -> String {
        match self {
            Self::Sd => sd::executable_file_name(),
        }
    }
}

/// Binary payload materialized for one source-installed common executable.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct CommonExecutablePayload {
    /// Canonical executable file name (for example `sd.exe` on Windows).
    pub executable_file_name: String,
    /// Raw executable bytes that should be written/imported as-is.
    pub executable_bytes: Vec<u8>,
}

/// Installs one common executable tool from source and returns binary bytes.
///
/// # Errors
///
/// Returns [`ConductorError`] when installation fails or the executable payload
/// cannot be materialized.
pub fn fetch_common_executable_tool_payload(
    tool: CommonExecutableTool,
) -> Result<CommonExecutablePayload, ConductorError> {
    match tool {
        CommonExecutableTool::Sd => sd::fetch_payload(),
    }
}

#[cfg(test)]
mod tests {
    use super::CommonExecutableTool;

    /// Protects stable tool-preset selector metadata for release downloads.
    #[test]
    fn common_sd_tool_selector_fields_are_stable() {
        assert_eq!(CommonExecutableTool::Sd.logical_tool_name(), "mediapm-conductor.tools.sd");
        let executable_name = CommonExecutableTool::Sd.executable_file_name();
        assert!(executable_name.starts_with("sd"));
    }
}
