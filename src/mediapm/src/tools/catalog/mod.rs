//! Built-in downloader catalog for managed mediapm tool entries.
//!
//! Catalog entries are split into one Rust file per logical tool so source
//! details remain isolated and easy to review. Each entry defines per-platform
//! download URLs, archive format, and checksums.

mod deno;
mod ffmpeg;
mod media_tagger;
mod rsgain;
mod sd;
mod yt_dlp;

/// Archive format constants for tool payloads.
pub(crate) const ARCHIVE_BINARY: &str = "binary";
/// ZIP archive format.
pub(crate) const ARCHIVE_ZIP: &str = "zip";
/// Gzip-compressed tar archive.
pub(crate) const ARCHIVE_TAR_GZ: &str = "tar.gz";
/// Xz-compressed tar archive.
pub(crate) const ARCHIVE_TAR_XZ: &str = "tar.xz";

/// Supported operating-system targets for tool payload selection.
#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord)]
pub(crate) enum ToolOs {
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
pub(crate) const fn current_tool_os() -> ToolOs {
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
pub(crate) struct PlatformValue {
    /// Download URL for this platform.
    pub url: &'static str,
    /// Target architecture label (e.g. `x86_64`, `aarch64`).
    pub arch: &'static str,
    /// Optional SHA-256 checksum hex string.
    pub checksum_sha256: Option<&'static str>,
    /// Per-platform archive format override.
    ///
    /// When `Some`, this overrides the tool-level `archive_format` for this
    /// specific platform. This is needed when one platform uses a different
    /// archive format (e.g. rsgain Linux uses `.tar.xz` while other platforms
    /// use `.zip`).
    pub archive_format: Option<&'static str>,
}

/// Per-OS list of platform download entries.
pub(crate) type PlatformValues = Vec<(ToolOs, Vec<PlatformValue>)>;

/// Catalog entry for one logical tool declared in `mediapm.ncl`.
#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) struct ToolCatalogEntry {
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

/// In-memory catalog for requirement reconciliation and downloads.
pub(crate) fn tool_catalog() -> &'static [ToolCatalogEntry] {
    use std::sync::OnceLock;
    static CATALOG: OnceLock<Vec<ToolCatalogEntry>> = OnceLock::new();
    CATALOG.get_or_init(|| {
        vec![
            ffmpeg::entry(),
            yt_dlp::entry(),
            deno::entry(),
            rsgain::entry(),
            #[cfg(feature = "media-tagger")]
            media_tagger::entry(),
            sd::entry(),
        ]
    })
}

/// Resolves a catalog entry by logical tool name (case-insensitive).
#[must_use]
pub(crate) fn tool_catalog_entry(tool_name: &str) -> Option<&'static ToolCatalogEntry> {
    let normalized = tool_name.trim();
    tool_catalog().iter().find(|entry| entry.id.eq_ignore_ascii_case(normalized))
}
