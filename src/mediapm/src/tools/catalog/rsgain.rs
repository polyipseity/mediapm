//! Catalog entry for `rsgain` managed tool provisioning.

use super::{ARCHIVE_ZIP, PlatformValue, ToolCatalogEntry};

/// Declarative catalog record for `rsgain`.
pub(super) fn entry() -> ToolCatalogEntry {
    ToolCatalogEntry {
        id: "rsgain",
        description: "rsgain ReplayGain loudness analyzer",
        homepage: "https://github.com/complexlogic/rsgain",
        latest: "latest",
        platforms: vec![
            (
                super::ToolOs::Windows,
                vec![PlatformValue {
                    url: "https://github.com/complexlogic/rsgain/releases/latest/download/rsgain-x86_64-pc-windows-msvc.zip",
                    arch: "x86_64",
                    checksum_sha256: None,
                }],
            ),
            (
                super::ToolOs::Macos,
                vec![PlatformValue {
                    url: "https://github.com/complexlogic/rsgain/releases/latest/download/rsgain-x86_64-apple-darwin.zip",
                    arch: "x86_64",
                    checksum_sha256: None,
                }],
            ),
            (
                super::ToolOs::Linux,
                vec![PlatformValue {
                    url: "https://github.com/complexlogic/rsgain/releases/latest/download/rsgain-x86_64-unknown-linux-gnu.zip",
                    arch: "x86_64",
                    checksum_sha256: None,
                }],
            ),
        ],
        archive_format: ARCHIVE_ZIP,
    }
}
