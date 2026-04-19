//! Catalog entry for internal `media-tagger` managed tool provisioning.

use super::{PlatformValue, ToolCatalogEntry, ToolDownloadDescriptor};

/// Declarative catalog record for internal `media-tagger` launcher shims.
pub(super) const ENTRY: ToolCatalogEntry = ToolCatalogEntry {
    name: "media-tagger",
    description: "mediapm native metadata tagger (Chromaprint + AcoustID + MusicBrainz)",
    registry_track: "latest",
    source_label: PlatformValue {
        windows: "mediapm internal launcher",
        macos: "mediapm internal launcher",
        linux: "mediapm internal launcher",
    },
    source_identifier: PlatformValue {
        windows: "mediapm-internal",
        macos: "mediapm-internal",
        linux: "mediapm-internal",
    },
    executable_name: PlatformValue {
        windows: "media-tagger.cmd",
        macos: "media-tagger",
        linux: "media-tagger",
    },
    download: ToolDownloadDescriptor::InternalLauncher,
};
