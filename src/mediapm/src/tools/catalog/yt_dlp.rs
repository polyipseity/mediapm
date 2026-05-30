//! Catalog entry for `yt-dlp` managed tool provisioning.

use super::{DownloadPayloadMode, PlatformValue, ToolCatalogEntry, ToolDownloadDescriptor};

/// Preferred direct-download URL for latest Windows `yt-dlp` binary.
const YT_DLP_WINDOWS_URLS: &[&str] =
    &["https://github.com/yt-dlp/yt-dlp/releases/latest/download/yt-dlp.exe"];

/// Preferred direct-download URL for latest macOS self-contained `yt-dlp` binary.
const YT_DLP_MACOS_URLS: &[&str] =
    &["https://github.com/yt-dlp/yt-dlp/releases/latest/download/yt-dlp_macos"];

/// Preferred direct-download URL for latest Linux self-contained `yt-dlp` binary.
const YT_DLP_LINUX_URLS: &[&str] =
    &["https://github.com/yt-dlp/yt-dlp/releases/latest/download/yt-dlp_linux"];

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
            macos: YT_DLP_MACOS_URLS,
            linux: YT_DLP_LINUX_URLS,
        },
        release_repo: Some("yt-dlp/yt-dlp"),
    },
    additional_download_sources: &[],
};

#[cfg(test)]
mod tests {
    use super::ENTRY;
    use crate::tools::catalog::ToolDownloadDescriptor;

    /// Protects managed dependency wiring by ensuring yt-dlp provisioning no
    /// longer bundles `deno` directly.
    #[test]
    fn entry_omits_bundled_deno_runtime_source() {
        let ToolDownloadDescriptor::StaticUrls { .. } = ENTRY.download else {
            panic!("yt-dlp catalog entry must use static URL strategy");
        };

        assert!(ENTRY.additional_download_sources.is_empty());
    }
}
