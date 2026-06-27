//! Catalog entry for internal `media-tagger` managed tool provisioning.
//!
//! `media-tagger` is an internal launcher shim shipped with `mediapm` itself,
//! not an external download. The platforms list is intentionally empty to
//! signal internal-launcher provisioning.

#![allow(dead_code)]
// TODO: Stream A stubs — wired when provisioning pipeline is complete.

use super::ToolCatalogEntry;

/// Declarative catalog record for internal `media-tagger` launcher shims.
pub(super) fn entry() -> ToolCatalogEntry {
    ToolCatalogEntry {
        id: "media-tagger",
        description: "mediapm native metadata tagger (Chromaprint + AcoustID + MusicBrainz)",
        homepage: "https://github.com/mediapm/mediapm",
        latest: "latest",
        platforms: vec![],
        archive_format: "binary",
    }
}
