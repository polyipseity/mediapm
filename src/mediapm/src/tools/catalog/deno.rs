//! Catalog entry for `deno` managed tool provisioning.
//!
//! `deno` is provisioned from upstream GitHub release assets.

use super::{DownloadPayloadMode, PlatformValue, ToolCatalogEntry, ToolDownloadDescriptor};

/// Declarative catalog record for `deno`.
pub(super) const ENTRY: ToolCatalogEntry = ToolCatalogEntry {
    name: "deno",
    description: "deno JavaScript and TypeScript runtime",
    registry_track: "latest",
    source_label: PlatformValue {
        windows: "GitHub releases (denoland/deno)",
        macos: "GitHub releases (denoland/deno)",
        linux: "GitHub releases (denoland/deno)",
    },
    source_identifier: PlatformValue {
        windows: "github-releases-denoland-deno",
        macos: "github-releases-denoland-deno",
        linux: "github-releases-denoland-deno",
    },
    executable_name: PlatformValue { windows: "deno.exe", macos: "deno", linux: "deno" },
    download: ToolDownloadDescriptor::StaticUrls {
        modes: PlatformValue {
            windows: DownloadPayloadMode::ZipArchive,
            macos: DownloadPayloadMode::ZipArchive,
            linux: DownloadPayloadMode::ZipArchive,
        },
        urls: PlatformValue {
            windows: &[
                "https://github.com/denoland/deno/releases/latest/download/deno-x86_64-pc-windows-msvc.zip",
            ],
            macos: &[
                "https://github.com/denoland/deno/releases/latest/download/deno-aarch64-apple-darwin.zip",
                "https://github.com/denoland/deno/releases/latest/download/deno-x86_64-apple-darwin.zip",
            ],
            linux: &[
                "https://github.com/denoland/deno/releases/latest/download/deno-aarch64-unknown-linux-gnu.zip",
                "https://github.com/denoland/deno/releases/latest/download/deno-x86_64-unknown-linux-gnu.zip",
            ],
        },
        release_repo: Some("denoland/deno"),
    },
    additional_download_sources: &[],
};
