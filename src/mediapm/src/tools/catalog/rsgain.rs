//! Catalog entry for `rsgain` managed tool provisioning.

use super::{ARCHIVE_TAR_XZ, ARCHIVE_ZIP, PlatformValue, ToolCatalogEntry};

/// Declarative catalog record for `rsgain`.
pub(super) fn entry() -> ToolCatalogEntry {
    ToolCatalogEntry {
        id: "rsgain",
        description: "rsgain ReplayGain loudness analyzer",
        homepage: "https://github.com/complexlogic/rsgain",
        latest: "v3.7",
        platforms: vec![
            (
                super::ToolOs::Windows,
                vec![PlatformValue {
                    url: "https://github.com/complexlogic/rsgain/releases/download/v3.7/rsgain-3.7-win64.zip",
                    arch: "x86_64",
                    checksum_sha256: None,
                    archive_format: None,
                }],
            ),
            (
                super::ToolOs::Macos,
                vec![PlatformValue {
                    url: "https://github.com/complexlogic/rsgain/releases/download/v3.7/rsgain-3.7-macOS-x86_64.zip",
                    arch: "x86_64",
                    checksum_sha256: None,
                    archive_format: None,
                }],
            ),
            (
                super::ToolOs::Linux,
                vec![PlatformValue {
                    url: "https://github.com/complexlogic/rsgain/releases/download/v3.7/rsgain-3.7-Linux.tar.xz",
                    arch: "x86_64",
                    checksum_sha256: None,
                    archive_format: Some(ARCHIVE_TAR_XZ),
                }],
            ),
        ],
        archive_format: ARCHIVE_ZIP,
    }
}
