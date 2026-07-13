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

pub(crate) use mediapm_conductor::tools::catalog::{
    ARCHIVE_BINARY, ARCHIVE_TAR_XZ, ARCHIVE_ZIP, PlatformValue, ToolCatalogEntry, ToolOs,
};

mod deno;
mod ffmpeg;
mod media_tagger;
mod rsgain;
mod yt_dlp;

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
    use mediapm_conductor::tools::catalog::ARCHIVE_TAR_GZ;
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
    use mediapm_conductor::tools::catalog::ARCHIVE_TAR_GZ;

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
