//! Tool catalog entry types for managed-tool provisioning.
//!
//! These types are always compiled (no feature gate) since they are pure
//! data structures with no external dependencies.

/// Archive format constants for tool payloads.
pub const ARCHIVE_BINARY: &str = "binary";
/// ZIP archive format.
pub const ARCHIVE_ZIP: &str = "zip";
/// Gzip-compressed tar archive.
pub const ARCHIVE_TAR_GZ: &str = "tar.gz";
/// Xz-compressed tar archive.
pub const ARCHIVE_TAR_XZ: &str = "tar.xz";

/// Supported operating-system targets for tool payload selection.
#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord)]
pub enum ToolOs {
    /// Windows target.
    Windows,
    /// Linux target.
    Linux,
    /// macOS target.
    Macos,
}

#[allow(dead_code)]
impl ToolOs {
    /// Returns the canonical lower-case label for this OS value.
    #[must_use]
    pub const fn as_str(self) -> &'static str {
        match self {
            Self::Windows => "windows",
            Self::Linux => "linux",
            Self::Macos => "macos",
        }
    }

    /// Returns every supported OS in deterministic order.
    #[must_use]
    pub const fn all() -> [Self; 3] {
        [Self::Windows, Self::Linux, Self::Macos]
    }
}

/// Returns the host OS for runtime-local policy decisions.
#[must_use]
pub const fn current_tool_os() -> ToolOs {
    #[cfg(target_os = "windows")]
    {
        ToolOs::Windows
    }
    #[cfg(target_os = "macos")]
    {
        ToolOs::Macos
    }
    #[cfg(not(any(target_os = "windows", target_os = "macos")))]
    {
        ToolOs::Linux
    }
}

/// Per-platform download value with URL, architecture, optional checksum,
/// and optional per-platform archive format override.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct PlatformValue {
    /// Download URL for this platform.
    pub url: &'static str,
    /// Target architecture label (e.g. `x86_64`, `aarch64`).
    pub arch: &'static str,
    /// Optional SHA-256 checksum hex string.
    pub checksum_sha256: Option<&'static str>,
    /// Archive format for this platform's payload (`binary`, `zip`, `tar.gz`, `tar.xz`).
    pub archive_format: &'static str,
}

/// Per-OS list of platform download entries.
pub type PlatformValues = Vec<(ToolOs, Vec<PlatformValue>)>;

/// Catalog entry for one logical tool declared in `mediapm.ncl`.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ToolCatalogEntry {
    /// Logical tool name (e.g. `yt-dlp`, `ffmpeg`).
    pub id: &'static str,
    /// Human-readable description for diagnostics.
    pub description: &'static str,
    /// Upstream project homepage URL.
    pub homepage: &'static str,
    /// Default version string for the catalog track.
    pub latest: &'static str,
    /// Per-platform download entries keyed by OS.
    pub platforms: PlatformValues,
    /// Archive format for extraction (`binary`, `zip`, `tar.gz`, `tar.xz`).
    pub archive_format: &'static str,
}

/// Entry for `sd` tool (feature-gated behind `tool-presets`).
#[cfg(feature = "tool-presets")]
mod sd;

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn archive_format_valid_values() {
        let valid = [ARCHIVE_BINARY, ARCHIVE_ZIP, ARCHIVE_TAR_GZ, ARCHIVE_TAR_XZ];
        for v in &valid {
            assert!(valid.contains(v));
        }
    }

    #[test]
    fn tool_os_as_str_roundtrip() {
        assert_eq!(ToolOs::Windows.as_str(), "windows");
        assert_eq!(ToolOs::Linux.as_str(), "linux");
        assert_eq!(ToolOs::Macos.as_str(), "macos");
    }

    #[test]
    fn tool_os_all_contains_three() {
        let all = ToolOs::all();
        assert_eq!(all.len(), 3);
        assert!(all.contains(&ToolOs::Windows));
        assert!(all.contains(&ToolOs::Linux));
        assert!(all.contains(&ToolOs::Macos));
    }
}
