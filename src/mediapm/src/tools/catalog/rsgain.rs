//! Catalog entry for `rsgain` managed tool provisioning.

use super::{PlatformValue, ToolCatalogEntry, ToolDownloadDescriptor};

/// Marker list preferred for selecting a Windows `rsgain` ZIP asset.
const RSGAIN_WINDOWS_MARKERS: &[&str] = &["windows", "win", "msvc"];

/// Marker list preferred for selecting a macOS `rsgain` ZIP asset.
const RSGAIN_MACOS_MARKERS: &[&str] = &["macos", "darwin", "apple"];

/// Marker list preferred for selecting a Linux `rsgain` ZIP asset.
const RSGAIN_LINUX_MARKERS: &[&str] = &["linux", "musl", "gnu"];

/// Declarative catalog record for `rsgain`.
pub(super) const ENTRY: ToolCatalogEntry = ToolCatalogEntry {
    name: "rsgain",
    description: "rsgain ReplayGain loudness analyzer",
    registry_track: "latest",
    source_label: PlatformValue {
        windows: "GitHub Releases",
        macos: "GitHub Releases",
        linux: "GitHub Releases",
    },
    source_identifier: PlatformValue {
        windows: "github-releases-complexlogic-rsgain",
        macos: "github-releases-complexlogic-rsgain",
        linux: "github-releases-complexlogic-rsgain",
    },
    executable_name: PlatformValue { windows: "rsgain.exe", macos: "rsgain", linux: "rsgain" },
    download: ToolDownloadDescriptor::GitHubLatestZipAsset {
        repo: "complexlogic/rsgain",
        markers: PlatformValue {
            windows: RSGAIN_WINDOWS_MARKERS,
            macos: RSGAIN_MACOS_MARKERS,
            linux: RSGAIN_LINUX_MARKERS,
        },
    },
};
