//! Catalog entry for `sd` managed tool provisioning.
//!
//! Feature-gated behind `tool-presets`.

#[cfg(feature = "tool-presets")]
use super::{ARCHIVE_TAR_GZ, ARCHIVE_ZIP, PlatformValue, ToolCatalogEntry};

/// Returns the catalog entry for `sd`.
///
/// Only available when the `tool-presets` feature is enabled.
#[cfg(feature = "tool-presets")]
#[must_use]
#[allow(dead_code)]
pub fn entry() -> ToolCatalogEntry {
    ToolCatalogEntry {
        id: "sd",
        description: "sd stream editor (find-and-replace for files)",
        homepage: "https://github.com/chmln/sd",
        latest: "v1.1.0",
        platforms: vec![
            (
                super::ToolOs::Windows,
                vec![PlatformValue {
                    url: "https://github.com/chmln/sd/releases/download/v1.1.0/sd-v1.1.0-x86_64-pc-windows-msvc.zip",
                    arch: "x86_64",
                    checksum_sha256: None,
                    archive_format: ARCHIVE_ZIP,
                }],
            ),
            (
                super::ToolOs::Macos,
                vec![PlatformValue {
                    url: "https://github.com/chmln/sd/releases/download/v1.1.0/sd-v1.1.0-aarch64-apple-darwin.tar.gz",
                    arch: "aarch64",
                    checksum_sha256: None,
                    archive_format: ARCHIVE_TAR_GZ,
                }],
            ),
            (
                super::ToolOs::Linux,
                vec![PlatformValue {
                    url: "https://github.com/chmln/sd/releases/download/v1.1.0/sd-v1.1.0-x86_64-unknown-linux-gnu.tar.gz",
                    arch: "x86_64",
                    checksum_sha256: None,
                    archive_format: ARCHIVE_TAR_GZ,
                }],
            ),
        ],
        archive_format: ARCHIVE_TAR_GZ,
    }
}

#[cfg(test)]
#[cfg(feature = "tool-presets")]
mod tests {
    use super::*;

    #[test]
    fn sd_entry_has_all_three_platforms() {
        let entry = entry();
        assert_eq!(entry.id, "sd");
        assert_eq!(entry.platforms.len(), 3);
    }

    #[test]
    fn sd_entry_urls_are_https() {
        let entry = entry();
        for (_os, values) in &entry.platforms {
            for pv in values {
                assert!(pv.url.starts_with("https://"), "sd url not https: {}", pv.url);
            }
        }
    }
}
