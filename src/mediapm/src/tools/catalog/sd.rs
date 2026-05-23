//! Catalog entry for `sd` managed tool provisioning.
//!
//! `sd` is provisioned from upstream GitHub release assets.

use super::{DownloadPayloadMode, PlatformValue, ToolCatalogEntry, ToolDownloadDescriptor};

/// Declarative catalog record for `sd`.
pub(super) const ENTRY: ToolCatalogEntry = ToolCatalogEntry {
    name: "sd",
    description: "sd stream editor",
    registry_track: "latest",
    source_label: PlatformValue {
        windows: "GitHub releases (chmln/sd)",
        macos: "GitHub releases (chmln/sd)",
        linux: "GitHub releases (chmln/sd)",
    },
    source_identifier: PlatformValue {
        windows: "github-releases-chmln-sd",
        macos: "github-releases-chmln-sd",
        linux: "github-releases-chmln-sd",
    },
    executable_name: PlatformValue { windows: "sd.exe", macos: "sd", linux: "sd" },
    download: ToolDownloadDescriptor::StaticUrls {
        modes: PlatformValue {
            windows: DownloadPayloadMode::ZipArchive,
            macos: DownloadPayloadMode::TarGzArchive,
            linux: DownloadPayloadMode::TarGzArchive,
        },
        urls: PlatformValue {
            windows: &[
                "https://github.com/chmln/sd/releases/latest/download/sd-x86_64-pc-windows-msvc.zip",
                "https://github.com/chmln/sd/releases/latest/download/sd-x86_64-pc-windows-gnu.zip",
                "https://github.com/chmln/sd/releases/latest/download/sd-aarch64-pc-windows-msvc.zip",
            ],
            macos: &[
                "https://github.com/chmln/sd/releases/latest/download/sd-aarch64-apple-darwin.tar.gz",
                "https://github.com/chmln/sd/releases/latest/download/sd-x86_64-apple-darwin.tar.gz",
            ],
            linux: &[
                "https://github.com/chmln/sd/releases/latest/download/sd-x86_64-unknown-linux-gnu.tar.gz",
                "https://github.com/chmln/sd/releases/latest/download/sd-x86_64-unknown-linux-musl.tar.gz",
                "https://github.com/chmln/sd/releases/latest/download/sd-aarch64-unknown-linux-gnu.tar.gz",
                "https://github.com/chmln/sd/releases/latest/download/sd-aarch64-unknown-linux-musl.tar.gz",
            ],
        },
        release_repo: Some("chmln/sd"),
    },
};
