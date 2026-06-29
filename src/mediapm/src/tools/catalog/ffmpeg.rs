//! Catalog entry for `ffmpeg` managed tool provisioning.

use super::{ARCHIVE_ZIP, PlatformValue, ToolCatalogEntry};

/// Declarative catalog record for `ffmpeg`.
pub(super) fn entry() -> ToolCatalogEntry {
    ToolCatalogEntry {
        id: "ffmpeg",
        description: "ffmpeg media processing toolkit",
        homepage: "https://ffmpeg.org",
        latest: "latest",
        platforms: vec![
            (
                super::ToolOs::Windows,
                vec![
                    PlatformValue {
                        url: "https://github.com/BtbN/FFmpeg-Builds/releases/latest/download/ffmpeg-master-latest-win64-gpl-shared.zip",
                        arch: "x86_64",
                        checksum_sha256: None,
                        archive_format: None,
                    },
                    PlatformValue {
                        url: "https://github.com/BtbN/FFmpeg-Builds/releases/latest/download/ffmpeg-master-latest-win64-gpl.zip",
                        arch: "x86_64",
                        checksum_sha256: None,
                        archive_format: None,
                    },
                ],
            ),
            (
                super::ToolOs::Macos,
                vec![PlatformValue {
                    url: "https://evermeet.cx/ffmpeg/getrelease/zip",
                    arch: "x86_64",
                    checksum_sha256: None,
                    archive_format: None,
                }],
            ),
            (
                super::ToolOs::Linux,
                vec![
                    PlatformValue {
                        url: "https://github.com/BtbN/FFmpeg-Builds/releases/latest/download/ffmpeg-master-latest-linux64-gpl-shared.tar.xz",
                        arch: "x86_64",
                        checksum_sha256: None,
                        archive_format: None,
                    },
                    PlatformValue {
                        url: "https://github.com/BtbN/FFmpeg-Builds/releases/latest/download/ffmpeg-master-latest-linux64-gpl.tar.xz",
                        arch: "x86_64",
                        checksum_sha256: None,
                        archive_format: None,
                    },
                ],
            ),
        ],
        archive_format: ARCHIVE_ZIP,
    }
}
