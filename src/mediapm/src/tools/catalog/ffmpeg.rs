//! Catalog entry for `ffmpeg` managed tool provisioning.

use super::{
    DownloadPayloadMode, PlatformValue, ToolAdditionalDownloadSource, ToolCatalogEntry,
    ToolDownloadDescriptor,
};

/// Windows URL candidates for ffmpeg payload downloads.
///
/// The first two candidates prefer GitHub-hosted `BtbN` archives. Gyan links are
/// retained as fallback candidates for transient GitHub/CDN failures.
const FFMPEG_WINDOWS_URLS: &[&str] = &[
    "https://github.com/BtbN/FFmpeg-Builds/releases/latest/download/ffmpeg-master-latest-win64-gpl-shared.zip",
    "https://github.com/BtbN/FFmpeg-Builds/releases/latest/download/ffmpeg-master-latest-win64-gpl.zip",
    "https://www.gyan.dev/ffmpeg/builds/ffmpeg-release-essentials.zip",
    "https://www.gyan.dev/ffmpeg/builds/ffmpeg-release-full.zip",
];

/// Linux URL candidates for ffmpeg payload downloads.
///
/// Linux provisioning must use Linux-targeted archives so recursive
/// executable discovery can resolve `ffmpeg` (without `.exe`) for the Linux
/// selector branch generated in managed tool command selectors.
const FFMPEG_LINUX_URLS: &[&str] = &[
    "https://github.com/BtbN/FFmpeg-Builds/releases/latest/download/ffmpeg-master-latest-linux64-gpl-shared.tar.xz",
    "https://github.com/BtbN/FFmpeg-Builds/releases/latest/download/ffmpeg-master-latest-linux64-gpl.tar.xz",
];

/// macOS URL candidate for ffmpeg payload downloads.
const FFMPEG_MACOS_URLS: &[&str] = &["https://evermeet.cx/ffmpeg/getrelease/zip"];

/// macOS URL candidate for ffprobe side-source binary download.
///
/// `evermeet.cx` provides `ffprobe` as a separate ZIP download alongside its
/// `ffmpeg` release. The macOS `ffmpeg` archive contains only the `ffmpeg`
/// binary, so `ffprobe` must be fetched from an additional source.
const FFPROBE_MACOS_URLS: &[&str] = &[
    "https://evermeet.cx/ffmpeg/getrelease/ffprobe/zip",
    "https://evermeet.cx/ffmpeg/get/ffprobe/zip",
];

/// Additional ffprobe source merged into the managed ffmpeg install root.
const FFMPEG_ADDITIONAL_SOURCES: &[ToolAdditionalDownloadSource] =
    &[ToolAdditionalDownloadSource {
        urls: PlatformValue {
            windows: &[], // BtbN archive already includes ffprobe.exe as a sibling binary
            macos: FFPROBE_MACOS_URLS,
            linux: &[], // BtbN archive already includes ffprobe as a sibling binary
        },
        mode: PlatformValue {
            windows: DownloadPayloadMode::ZipArchive,
            macos: DownloadPayloadMode::ZipArchive,
            linux: DownloadPayloadMode::ZipArchive,
        },
        expected_executable_name: PlatformValue { windows: "", macos: "ffprobe", linux: "" },
    }];

/// Declarative catalog record for `ffmpeg`.
pub(super) const ENTRY: ToolCatalogEntry = ToolCatalogEntry {
    name: "ffmpeg",
    description: "ffmpeg media processing toolkit",
    registry_track: "latest",
    source_label: PlatformValue {
        windows: "GitHub Releases (BtbN, with Gyan fallback)",
        macos: "Evermeet",
        linux: "GitHub Releases (BtbN)",
    },
    source_identifier: PlatformValue {
        windows: "github-releases-btbn-ffmpeg-builds",
        macos: "evermeet-ffmpeg",
        linux: "github-releases-btbn-ffmpeg-builds",
    },
    executable_name: PlatformValue { windows: "ffmpeg.exe", macos: "ffmpeg", linux: "ffmpeg" },
    download: ToolDownloadDescriptor::StaticUrls {
        modes: PlatformValue {
            windows: DownloadPayloadMode::ZipArchive,
            linux: DownloadPayloadMode::TarXzArchive,
            macos: DownloadPayloadMode::ZipArchive,
        },
        urls: PlatformValue {
            windows: FFMPEG_WINDOWS_URLS,
            macos: FFMPEG_MACOS_URLS,
            linux: FFMPEG_LINUX_URLS,
        },
        release_repo: Some("BtbN/FFmpeg-Builds"),
    },
    additional_download_sources: FFMPEG_ADDITIONAL_SOURCES,
};

#[cfg(test)]
mod tests {
    use super::ENTRY;
    use crate::tools::catalog::{DownloadPayloadMode, ToolDownloadDescriptor};

    /// Protects Linux payload correctness by enforcing Linux-targeted ffmpeg
    /// release URLs (instead of Windows-only archives).
    #[test]
    fn linux_urls_reference_linux_builds() {
        let ToolDownloadDescriptor::StaticUrls { modes, urls, .. } = ENTRY.download else {
            panic!("ffmpeg catalog entry must use static URL strategy");
        };

        assert_eq!(modes.linux, DownloadPayloadMode::TarXzArchive);

        assert!(
            urls.linux.iter().all(|url| url.contains("linux64")),
            "linux ffmpeg URL set must target linux64 assets"
        );
        assert!(
            urls.linux.iter().all(|url| url.ends_with(".tar.xz")),
            "linux ffmpeg URL set must use tar.xz archives published by BtbN"
        );
    }

    /// Protects immutable tool-id/source metadata stability for Linux ffmpeg.
    #[test]
    fn linux_source_identifier_uses_btbn() {
        assert_eq!(ENTRY.source_identifier.linux, "github-releases-btbn-ffmpeg-builds");
    }

    /// Protects macOS ffprobe companion provisioning by requiring dedicated
    /// ffprobe API endpoints (not ffmpeg aliases) in additional-source URLs.
    #[test]
    fn macos_additional_ffprobe_urls_target_ffprobe_api() {
        let additional = ENTRY.additional_download_sources;
        assert!(
            !additional.is_empty(),
            "ffmpeg catalog should include macOS ffprobe companion source"
        );

        let macos_urls = additional[0].urls.macos;
        assert!(
            macos_urls.iter().all(|url| url.contains("/ffmpeg/get") && url.contains("ffprobe")),
            "macOS ffprobe companion URLs must target ffprobe-specific evermeet API endpoints"
        );
    }
}
