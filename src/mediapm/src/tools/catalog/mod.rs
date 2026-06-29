//! Built-in downloader catalog for managed mediapm tool entries.
//!
//! Catalog entries are split into one Rust file per logical tool so source
//! details remain isolated and easy to review. Each entry defines per-platform
//! download URLs, archive format, and checksums.
//!
//! # All-platform design
//!
//! Every [`ToolCatalogEntry`] defines [`PlatformValue`] entries for each
//! supported OS (`linux`, `macos`, `windows`). The downloader and provisioner
//! fetch and CAS-import payloads for **all** platforms, not just the host OS.
//! The conductor's content-map key prefix (`./{os}/…`) and its
//! [`FOREIGN_PLATFORM_DIRS`] filtering ensure only the host-native files
//! are materialised into the sandbox at runtime.

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
    /// Archive format for this platform's payload (`binary`, `zip`, `tar.gz`, `tar.xz`).
    pub archive_format: &'static str,
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

/// Derive the expected [`ARCHIVE_*`] constant from a URL's file extension.
///
/// Returns `None` for unrecognised or absent extensions.
#[cfg(test)]
#[must_use]
fn archive_format_from_url(url: &str) -> Option<&'static str> {
    let url_path = url.split('?').next().unwrap_or(url);
    // Strip trailing / or path segments to get the filename portion
    let filename = url_path.trim_end_matches('/').split('/').next_back().unwrap_or(url_path);
    if filename.ends_with(".tar.xz") {
        Some(ARCHIVE_TAR_XZ)
    } else if filename.ends_with(".tar.gz") || filename.ends_with(".tgz") {
        Some(ARCHIVE_TAR_GZ)
    } else if filename.ends_with(".zip") || filename == "zip" {
        Some(ARCHIVE_ZIP)
    } else if !filename.contains('.') || filename.ends_with(".exe") {
        // no extension or known binary extension → binary
        Some(ARCHIVE_BINARY)
    } else {
        None
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn catalog_entries_archive_format_matches_url_extension() {
        for entry in tool_catalog() {
            for (os, platform_values) in &entry.platforms {
                for pv in platform_values {
                    let expected = archive_format_from_url(pv.url);
                    assert_eq!(
                        Some(pv.archive_format),
                        expected,
                        "{}:{}: url `{}` has extension suggesting `{}`, but archive_format is `{}`",
                        entry.id,
                        os.as_str(),
                        pv.url,
                        expected.unwrap_or("<unknown>"),
                        pv.archive_format,
                    );
                }
            }
        }
    }

    #[test]
    fn catalog_entries_have_unique_urls_per_platform() {
        for entry in tool_catalog() {
            for (os, platform_values) in &entry.platforms {
                let mut seen = std::collections::HashSet::new();
                for pv in platform_values {
                    assert!(
                        seen.insert(pv.url),
                        "{}:{}: duplicate url `{}`",
                        entry.id,
                        os.as_str(),
                        pv.url,
                    );
                }
            }
        }
    }

    #[test]
    fn catalog_entries_have_valid_archive_format() {
        let valid = [ARCHIVE_BINARY, ARCHIVE_ZIP, ARCHIVE_TAR_GZ, ARCHIVE_TAR_XZ];
        for entry in tool_catalog() {
            assert!(
                valid.contains(&entry.archive_format),
                "{}: invalid top-level archive_format `{}`",
                entry.id,
                entry.archive_format,
            );
            for (os, platform_values) in &entry.platforms {
                for pv in platform_values {
                    assert!(
                        valid.contains(&pv.archive_format),
                        "{}:{}: invalid archive_format `{}`",
                        entry.id,
                        os.as_str(),
                        pv.archive_format,
                    );
                }
            }
        }
    }
}
