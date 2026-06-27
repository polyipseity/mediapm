//! Catalog entry for `sd` managed tool provisioning.

use super::{ARCHIVE_TAR_GZ, PlatformValue, ToolCatalogEntry};

/// Declarative catalog record for `sd`.
pub(super) fn entry() -> ToolCatalogEntry {
    ToolCatalogEntry {
        id: "sd",
        description: "sd stream editor (find-and-replace for files)",
        homepage: "https://github.com/chmln/sd",
        latest: "latest",
        platforms: vec![
            (
                super::ToolOs::Windows,
                vec![PlatformValue {
                    url: "https://github.com/chmln/sd/releases/latest/download/sd-x86_64-pc-windows-msvc.zip",
                    arch: "x86_64",
                    checksum_sha256: None,
                }],
            ),
            (
                super::ToolOs::Macos,
                vec![PlatformValue {
                    url: "https://github.com/chmln/sd/releases/latest/download/sd-aarch64-apple-darwin.tar.gz",
                    arch: "aarch64",
                    checksum_sha256: None,
                }],
            ),
            (
                super::ToolOs::Linux,
                vec![PlatformValue {
                    url: "https://github.com/chmln/sd/releases/latest/download/sd-x86_64-unknown-linux-gnu.tar.gz",
                    arch: "x86_64",
                    checksum_sha256: None,
                }],
            ),
        ],
        archive_format: ARCHIVE_TAR_GZ,
    }
}
