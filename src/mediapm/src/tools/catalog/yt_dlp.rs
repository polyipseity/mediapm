//! Catalog entry for `yt-dlp` managed tool provisioning.

use super::{
    DownloadPayloadMode, PlatformValue, ToolAdditionalDownloadSource, ToolCatalogEntry,
    ToolDownloadDescriptor,
};

/// Preferred direct-download URL for latest Windows `yt-dlp` binary.
const YT_DLP_WINDOWS_URLS: &[&str] =
    &["https://github.com/yt-dlp/yt-dlp/releases/latest/download/yt-dlp.exe"];

/// Preferred direct-download URL for latest macOS self-contained `yt-dlp` binary.
const YT_DLP_MACOS_URLS: &[&str] =
    &["https://github.com/yt-dlp/yt-dlp/releases/latest/download/yt-dlp_macos"];

/// Preferred direct-download URL for latest Linux self-contained `yt-dlp` binary.
const YT_DLP_LINUX_URLS: &[&str] =
    &["https://github.com/yt-dlp/yt-dlp/releases/latest/download/yt-dlp_linux"];

/// Preferred direct-download URL candidates for the bundled macOS `deno` runtime.
const DENO_MACOS_URLS: &[&str] = &[
    "https://github.com/denoland/deno/releases/latest/download/deno-aarch64-apple-darwin.zip",
    "https://github.com/denoland/deno/releases/latest/download/deno-x86_64-apple-darwin.zip",
];

/// Preferred direct-download URL candidates for the bundled Linux `deno` runtime.
const DENO_LINUX_URLS: &[&str] = &[
    "https://github.com/denoland/deno/releases/latest/download/deno-aarch64-unknown-linux-gnu.zip",
    "https://github.com/denoland/deno/releases/latest/download/deno-x86_64-unknown-linux-gnu.zip",
];

/// Preferred direct-download URL candidates for the bundled Windows `deno` runtime.
const DENO_WINDOWS_URLS: &[&str] =
    &["https://github.com/denoland/deno/releases/latest/download/deno-x86_64-pc-windows-msvc.zip"];

/// Additional `deno` source merged into the managed `yt-dlp` install root.
const YT_DLP_ADDITIONAL_SOURCES: &[ToolAdditionalDownloadSource] =
    &[ToolAdditionalDownloadSource {
        urls: PlatformValue {
            windows: DENO_WINDOWS_URLS,
            macos: DENO_MACOS_URLS,
            linux: DENO_LINUX_URLS,
        },
        mode: PlatformValue {
            windows: DownloadPayloadMode::ZipArchive,
            macos: DownloadPayloadMode::ZipArchive,
            linux: DownloadPayloadMode::ZipArchive,
        },
        expected_executable_name: PlatformValue {
            windows: "deno.exe",
            macos: "deno",
            linux: "deno",
        },
    }];

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
    additional_download_sources: YT_DLP_ADDITIONAL_SOURCES,
};

#[cfg(test)]
mod tests {
    use super::ENTRY;
    use crate::tools::catalog::{ToolAdditionalDownloadSource, ToolDownloadDescriptor};

    /// Protects the managed JS runtime bundle by ensuring yt-dlp carries a
    /// dedicated `deno` companion source in the same install root.
    #[test]
    fn entry_includes_deno_runtime_companion_source() {
        let ToolDownloadDescriptor::StaticUrls { .. } = ENTRY.download else {
            panic!("yt-dlp catalog entry must use static URL strategy");
        };

        assert_eq!(ENTRY.additional_download_sources.len(), 1);

        let ToolAdditionalDownloadSource { urls, expected_executable_name, .. } =
            ENTRY.additional_download_sources[0];

        assert!(urls.windows.iter().any(|url| url.contains("deno")));
        assert!(urls.macos.iter().any(|url| url.contains("deno")));
        assert!(urls.linux.iter().any(|url| url.contains("deno")));
        assert_eq!(expected_executable_name.windows, "deno.exe");
        assert_eq!(expected_executable_name.macos, "deno");
        assert_eq!(expected_executable_name.linux, "deno");
    }
}
