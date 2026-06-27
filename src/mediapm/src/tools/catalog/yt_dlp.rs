//! Catalog entry for `yt-dlp` managed tool provisioning.

use super::{ARCHIVE_BINARY, PlatformValue, ToolCatalogEntry};

/// Declarative catalog record for `yt-dlp`.
pub(super) fn entry() -> ToolCatalogEntry {
    ToolCatalogEntry {
        id: "yt-dlp",
        description: "yt-dlp remote media downloader",
        homepage: "https://github.com/yt-dlp/yt-dlp",
        latest: "latest",
        platforms: vec![
            (
                super::ToolOs::Windows,
                vec![PlatformValue {
                    url: "https://github.com/yt-dlp/yt-dlp/releases/latest/download/yt-dlp.exe",
                    arch: "x86_64",
                    checksum_sha256: None,
                }],
            ),
            (
                super::ToolOs::Macos,
                vec![PlatformValue {
                    url: "https://github.com/yt-dlp/yt-dlp/releases/latest/download/yt-dlp_macos",
                    arch: "x86_64",
                    checksum_sha256: None,
                }],
            ),
            (
                super::ToolOs::Linux,
                vec![PlatformValue {
                    url: "https://github.com/yt-dlp/yt-dlp/releases/latest/download/yt-dlp_linux",
                    arch: "x86_64",
                    checksum_sha256: None,
                }],
            ),
        ],
        archive_format: ARCHIVE_BINARY,
    }
}
