//! Catalog entry for `yt-dlp` managed tool provisioning.

use super::{DownloadPayloadMode, PlatformValue, ToolCatalogEntry, ToolDownloadDescriptor};

/// Preferred direct-download URL for latest Windows `yt-dlp` binary.
const YT_DLP_WINDOWS_URLS: &[&str] =
    &["https://github.com/yt-dlp/yt-dlp/releases/latest/download/yt-dlp.exe"];

/// Preferred direct-download URL for latest Unix-like `yt-dlp` binary.
const YT_DLP_UNIX_URLS: &[&str] =
    &["https://github.com/yt-dlp/yt-dlp/releases/latest/download/yt-dlp"];

/// Declarative catalog record for `yt-dlp`.
pub(super) const ENTRY: ToolCatalogEntry = ToolCatalogEntry {
    name: "yt-dlp",
    description: "yt-dlp remote media downloader",
    registry_track: "latest",
    source_label: PlatformValue {
        windows: "GitHub Releases",
        macos: "GitHub Releases",
        linux: "GitHub Releases",
    },
    source_identifier: PlatformValue {
        windows: "github-releases-yt-dlp-yt-dlp",
        macos: "github-releases-yt-dlp-yt-dlp",
        linux: "github-releases-yt-dlp-yt-dlp",
    },
    executable_name: PlatformValue { windows: "yt-dlp.exe", macos: "yt-dlp", linux: "yt-dlp" },
    download: ToolDownloadDescriptor::StaticUrls {
        modes: PlatformValue {
            windows: DownloadPayloadMode::DirectBinary,
            macos: DownloadPayloadMode::DirectBinary,
            linux: DownloadPayloadMode::DirectBinary,
        },
        urls: PlatformValue {
            windows: YT_DLP_WINDOWS_URLS,
            macos: YT_DLP_UNIX_URLS,
            linux: YT_DLP_UNIX_URLS,
        },
        release_repo: Some("yt-dlp/yt-dlp"),
    },
};
