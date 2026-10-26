//! The unified `mediapm.ncl` document model.
//!
//! [`MediaPmDocument`] is the single type every `mediapm.ncl` on disk decodes
//! into, for every schema version: the Nickel ladder in
//! `v1.ncl`–`v3.ncl` migrates a document *up* to the active version first, and
//! only then does this type see it. It therefore carries a version marker and a
//! `runtime` field holding the active `*Latest` boundary value, which is why it
//! lives here rather than in `config/mod.rs`: naming that boundary type is
//! exactly what `config/mod.rs` is forbidden to do.
//!
//! The name itself is unversioned, so callers outside `versions/` may name
//! `MediaPmDocument` freely — through the `pub use` in `config/mod.rs`, which
//! is the crate's usual facade for its config types. What they may not do is
//! reach the boundary value hanging off [`MediaPmDocument::runtime`], which is
//! reached through the unversioned entry points in the parent module.
//!
//! ## DO NOT REMOVE: versions policy guard
//!
//! - This file declares no resolved (`option`-free) type. Everything it
//!   declares is wire-shaped, which is the placement rule the whole
//!   `versions/` directory exists to enforce.
//! - Do not add a `decode`/`migrate_to` mirror of the Nickel ladder here.
//!   Version dispatch is Nickel; two implementations of the same migrations
//!   would drift, and the Nickel one is the one every on-disk document has
//!   actually passed through.
//! - Do not re-export anything from this file. Callers reach
//!   [`MediaPmDocument`] through `super`'s `pub use`, so a second route out
//!   would only widen the boundary.

use std::collections::BTreeMap;

use serde::{Deserialize, Serialize};

use mediapm_conductor::PayloadStaging;

use super::v_latest::{MediaRuntimeStorageLatest, resolved_library_payload_staging};
use crate::config::{MediaPmState, ToolRequirement, defaults, hierarchy_types, source_types};

/// Top-level mediapm document deserialized from `mediapm.ncl`.
#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct MediaPmDocument {
    /// Schema version marker.
    #[serde(default = "defaults::default_mediapm_document_version")]
    pub version: u32,
    /// Media source entries keyed by unique id.
    #[serde(default)]
    pub media: BTreeMap<String, source_types::MediaSourceSpec>,
    /// Hierarchy declaration.
    #[serde(default)]
    pub hierarchy: Vec<hierarchy_types::HierarchyNode>,
    /// Managed tool requirement declarations keyed by tool id.
    #[serde(default)]
    pub tools: BTreeMap<String, ToolRequirement>,
    /// Runtime configuration overrides.
    #[serde(default)]
    pub runtime: MediaRuntimeStorageLatest,
    /// Legacy `state` payload accepted for V1 documents.
    ///
    /// State is managed separately via `state.json`; the V2 schema drops this
    /// field, so it is accepted on read for legacy documents and never
    /// emitted on V2 writes.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub state: Option<MediaPmState>,
}

impl Default for MediaPmDocument {
    fn default() -> Self {
        Self {
            version: defaults::MEDIAPM_DOCUMENT_VERSION,
            media: BTreeMap::new(),
            hierarchy: Vec::new(),
            tools: BTreeMap::new(),
            runtime: MediaRuntimeStorageLatest::default(),
            state: None,
        }
    }
}

impl MediaPmDocument {
    /// Trims the user-visible text fields.
    ///
    /// Load calls this before the document reaches the hierarchy, where these
    /// values become metadata substitutions and literal path components: padding
    /// that survived load time would be part of the materialized path.
    ///
    /// A tool entry survives whatever it says. A bare `latest` entry is a request
    /// for that tool at its latest version, which is what `tool add` writes, so
    /// dropping it would silently remove the user's tool.
    pub fn normalize(&mut self) {
        for source in self.media.values_mut() {
            let trimmed = source.description.trim().to_string();
            source.description = trimmed;
            let trimmed = source.title.trim().to_string();
            source.title = trimmed;
            let trimmed = source.artist.trim().to_string();
            source.artist = trimmed;
        }
    }

    /// Resolves the staging strategy one tool requirement runs with.
    ///
    /// A tool that names a strategy keeps it; one that says `inherit` takes the
    /// library-wide default, and a document that names neither runs `in_place`.
    /// Both absences are resolved here rather than at the consumer, so the
    /// materializer never has to know what an unset field meant.
    #[must_use]
    pub fn effective_payload_staging(&self, tool: &ToolRequirement) -> PayloadStaging {
        tool.payload_staging
            .resolve(resolved_library_payload_staging(&self.runtime.materialization))
    }
}
#[cfg(test)]
mod tests {
    use super::*;
    use crate::config::MediaSourceSpec;

    /// Builds a minimal source whose description, title, and artist carry
    /// surrounding whitespace, so `normalize` has something to trim.
    fn padded_source() -> MediaSourceSpec {
        MediaSourceSpec {
            description: "  a description  ".to_string(),
            title: "\tthe title\n".to_string(),
            artist: "  the artist  ".to_string(),
            ..MediaSourceSpec::default()
        }
    }

    /// Verifies `normalize` trims the user-visible text fields.
    ///
    /// Untrimmed values reach the hierarchy as metadata substitutions and as
    /// literal path components, so whitespace that survives load time becomes
    /// part of the materialized path.
    #[test]
    fn normalize_trims_source_text_fields() {
        let mut document = MediaPmDocument {
            media: BTreeMap::from([("m1".to_string(), padded_source())]),
            ..MediaPmDocument::default()
        };
        document.normalize();

        let source = &document.media["m1"];
        assert_eq!(source.description, "a description");
        assert_eq!(source.title, "the title");
        assert_eq!(source.artist, "the artist");
    }
}
